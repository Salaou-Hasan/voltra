// ============================================================================
// tenant.rs — Multi-tenancy: namespace isolation, per-tenant keys & quotas
//
// DESIGN
// ──────
// A tenant is a hard namespace boundary enforced at the data layer:
//
//   logical table  "players"  →  physical table  "tn:<tenant_id>:players"
//
// Everything tenant-scoped flows through that prefix:
//   - ReducerContext reads/writes (get_row/set_row/delete_row/counters)
//   - Subscriptions (query rewritten at subscribe time; wire frames carry the
//     LOGICAL name — the prefix is stripped once at encode time, which is safe
//     because every subscriber of a physical tenant table is in that tenant)
//   - SQL (statement table names rewritten before execution)
//
// Tenants are stored in the system table `__tenants` (key = tenant id), so
// they ride the existing WAL / snapshot / replication machinery for free.
//
// AUTH
// ────
// Each tenant has a generated API key (`ndbt_<32 hex>`). A WebSocket client
// presenting `Authorization: Bearer <tenant_key>` is authenticated AND bound
// to that tenant for the lifetime of the connection. Tenant keys work even
// when a server API key is configured — they are first-class credentials.
//
// QUOTAS
// ──────
//   max_rows           — checked at commit time (inserts only); 0 = unlimited
//   max_calls_per_sec  — token bucket refilled continuously; 0 = unlimited
// ============================================================================

use crate::error::{VoltraError, Result};
use crate::table::TableStore;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;

/// System table holding tenant definitions.
pub const TENANTS_TABLE: &str = "__tenants";

/// Physical-name prefix marker. Physical = `tn:<tenant_id>:<logical>`.
pub const TENANT_PREFIX: &str = "tn:";

/// Build the physical table name for a tenant + logical table.
pub fn physical_table(tenant_id: &str, logical: &str) -> String {
    format!("{}{}:{}", TENANT_PREFIX, tenant_id, logical)
}

/// Strip a tenant prefix from a physical table name, returning the logical
/// name. Non-tenant names pass through unchanged.
pub fn logical_table(physical: &str) -> &str {
    if let Some(rest) = physical.strip_prefix(TENANT_PREFIX) {
        if let Some(idx) = rest.find(':') {
            return &rest[idx + 1..];
        }
    }
    physical
}

/// True when `physical` belongs to the given tenant.
pub fn belongs_to_tenant(physical: &str, tenant_id: &str) -> bool {
    physical
        .strip_prefix(TENANT_PREFIX)
        .and_then(|rest| rest.strip_prefix(tenant_id))
        .map(|rest| rest.starts_with(':'))
        .unwrap_or(false)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantInfo {
    pub id: String,
    pub name: String,
    pub api_key: String,
    /// 0 = unlimited.
    #[serde(default)]
    pub max_rows: u64,
    /// 0 = unlimited.
    #[serde(default)]
    pub max_calls_per_sec: u32,
    #[serde(default)]
    pub created_at: u64,
}

/// Continuous-refill token bucket for per-tenant call rate limiting.
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

pub struct TenantRegistry {
    tables: Arc<TableStore>,
    /// api_key → tenant_id (hot path: every handshake).
    key_index: DashMap<String, String>,
    /// tenant_id → TenantInfo cache (authoritative copy lives in __tenants).
    tenants: DashMap<String, TenantInfo>,
    /// tenant_id → rate bucket.
    buckets: DashMap<String, parking_lot::Mutex<TokenBucket>>,
}

impl TenantRegistry {
    /// Create a registry and hydrate the cache from the `__tenants` table
    /// (rows restored by snapshot/WAL replay before this is called).
    pub fn load(tables: Arc<TableStore>) -> Arc<Self> {
        let reg = Arc::new(TenantRegistry {
            tables,
            key_index: DashMap::new(),
            tenants: DashMap::new(),
            buckets: DashMap::new(),
        });
        reg.rehydrate();
        reg
    }

    /// Re-scan `__tenants` into the in-memory caches. Called at startup and
    /// after any tenant mutation that bypasses `create`/`delete` (e.g. WAL
    /// replay on a replica).
    pub fn rehydrate(&self) {
        self.key_index.clear();
        self.tenants.clear();
        if let Ok(rows) = self.tables.list_rows_with_keys(TENANTS_TABLE) {
            for (_key, val) in rows {
                if let Ok(info) = serde_json::from_value::<TenantInfo>(val) {
                    self.key_index.insert(info.api_key.clone(), info.id.clone());
                    self.tenants.insert(info.id.clone(), info);
                }
            }
        }
        log::info!("[tenant] {} tenant(s) loaded", self.tenants.len());
    }

    pub fn count(&self) -> usize {
        self.tenants.len()
    }

    /// Resolve an API key (raw token, no "Bearer ") to a tenant id.
    /// Supports the `key:role` suffix convention — only the key part matters.
    pub fn resolve_key(&self, raw_token: &str) -> Option<String> {
        let key_part = raw_token.split(':').next().unwrap_or(raw_token);
        self.key_index.get(key_part).map(|e| e.value().clone())
    }

    pub fn get(&self, tenant_id: &str) -> Option<TenantInfo> {
        self.tenants.get(tenant_id).map(|e| e.value().clone())
    }

    pub fn list(&self) -> Vec<TenantInfo> {
        let mut v: Vec<TenantInfo> = self.tenants.iter().map(|e| e.value().clone()).collect();
        v.sort_by_key(|a| a.created_at);
        v
    }

    /// Create a tenant. Generates id + API key, persists the row via
    /// `TableStore::set_row` and returns the full info (including the key —
    /// shown once to the operator). The caller is responsible for journaling
    /// the returned delta (WAL append + publish) for durability.
    pub fn create(
        &self,
        name: &str,
        max_rows: u64,
        max_calls_per_sec: u32,
    ) -> Result<(TenantInfo, crate::table::RowDelta)> {
        let name = name.trim();
        if name.is_empty() {
            return Err(VoltraError::invalid_argument("Tenant name must not be empty"));
        }
        if name.contains(':') {
            return Err(VoltraError::invalid_argument("Tenant name must not contain ':'"));
        }
        if self.tenants.iter().any(|e| e.value().name == name) {
            return Err(VoltraError::invalid_argument(format!(
                "Tenant '{}' already exists", name
            )));
        }
        let id = generate_id(name);
        let api_key = format!("ndbt_{}", random_hex(32));
        let info = TenantInfo {
            id: id.clone(),
            name: name.to_string(),
            api_key: api_key.clone(),
            max_rows,
            max_calls_per_sec,
            created_at: now_secs(),
        };
        let row = serde_json::to_value(&info)
            .map_err(|e| VoltraError::SerializationError(e.to_string()))?;
        let delta = self.tables.set_row(TENANTS_TABLE.to_string(), id.clone(), row)?;
        self.key_index.insert(api_key, id.clone());
        self.tenants.insert(id, info.clone());
        Ok((info, delta))
    }

    /// Delete a tenant and ALL its data. Returns the deltas (tenant row +
    /// every data row) for the caller to journal.
    pub fn delete(&self, tenant_id: &str) -> Result<Vec<crate::table::RowDelta>> {
        let Some(info) = self.get(tenant_id) else {
            return Err(VoltraError::invalid_argument(format!(
                "Unknown tenant '{}'", tenant_id
            )));
        };
        let mut deltas = Vec::new();
        // Drop every row in every table belonging to this tenant.
        for table in self.tables.list_tables() {
            if !belongs_to_tenant(&table, tenant_id) {
                continue;
            }
            if let Ok(rows) = self.tables.list_rows_with_keys(&table) {
                for (key, _) in rows {
                    if let Ok(d) = self.tables.delete_row(&table, &key) {
                        deltas.push(d);
                    }
                }
            }
        }
        // Drop the tenant definition row itself.
        if let Ok(d) = self.tables.delete_row(TENANTS_TABLE, tenant_id) {
            deltas.push(d);
        }
        self.key_index.remove(&info.api_key);
        self.tenants.remove(tenant_id);
        self.buckets.remove(tenant_id);
        Ok(deltas)
    }

    /// Per-tenant call rate check. `true` = allowed.
    pub fn check_rate(&self, tenant_id: &str) -> bool {
        let Some(info) = self.tenants.get(tenant_id) else { return true; };
        let limit = info.max_calls_per_sec;
        drop(info);
        if limit == 0 {
            return true;
        }
        let bucket = self.buckets.entry(tenant_id.to_string()).or_insert_with(|| {
            parking_lot::Mutex::new(TokenBucket {
                tokens: limit as f64,
                last_refill: Instant::now(),
            })
        });
        let mut b = bucket.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(b.last_refill).as_secs_f64();
        b.tokens = (b.tokens + elapsed * limit as f64).min(limit as f64);
        b.last_refill = now;
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Current number of data rows owned by a tenant (excludes the tenant
    /// definition row).
    pub fn tenant_row_count(&self, tenant_id: &str) -> u64 {
        let mut total = 0u64;
        for table in self.tables.list_tables() {
            if belongs_to_tenant(&table, tenant_id) {
                total += self
                    .tables
                    .list_rows_with_keys(&table)
                    .map(|r| r.len() as u64)
                    .unwrap_or(0);
            }
        }
        total
    }

    /// Row quota for a tenant (0 = unlimited).
    pub fn row_quota(&self, tenant_id: &str) -> u64 {
        self.tenants.get(tenant_id).map(|e| e.value().max_rows).unwrap_or(0)
    }

    /// Summary JSON for the admin API (key masked except last 4 chars).
    pub fn summary_json(&self, include_keys: bool) -> Value {
        let list: Vec<Value> = self
            .list()
            .into_iter()
            .map(|t| {
                let key = if include_keys {
                    t.api_key.clone()
                } else {
                    mask_key(&t.api_key)
                };
                serde_json::json!({
                    "id": t.id,
                    "name": t.name,
                    "api_key": key,
                    "max_rows": t.max_rows,
                    "max_calls_per_sec": t.max_calls_per_sec,
                    "created_at": t.created_at,
                    "rows_used": self.tenant_row_count(&t.id),
                })
            })
            .collect();
        serde_json::json!({ "tenants": list })
    }
}

fn mask_key(k: &str) -> String {
    if k.len() <= 9 {
        "****".to_string()
    } else {
        format!("{}…{}", &k[..9], &k[k.len() - 4..])
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Tenant id = slug of the name + 6 random hex chars (stable, URL-safe,
/// no ':' so the physical-name encoding is unambiguous).
fn generate_id(name: &str) -> String {
    let slug: String = name
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .chars()
        .take(24)
        .collect();
    format!("{}-{}", if slug.is_empty() { "tenant".into() } else { slug }, random_hex(6))
}

fn random_hex(n: usize) -> String {
    use rand::RngCore;
    let mut bytes = vec![0u8; n.div_ceil(2)];
    rand::thread_rng().fill_bytes(&mut bytes);
    let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    hex[..n].to_string()
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> Arc<TenantRegistry> {
        TenantRegistry::load(Arc::new(TableStore::new()))
    }

    #[test]
    fn physical_logical_roundtrip() {
        let p = physical_table("acme-1a2b3c", "players");
        assert_eq!(p, "tn:acme-1a2b3c:players");
        assert_eq!(logical_table(&p), "players");
        assert_eq!(logical_table("players"), "players");
        assert_eq!(logical_table("tn:broken"), "tn:broken");
    }

    #[test]
    fn belongs_to_tenant_is_exact() {
        let p = physical_table("acme", "players");
        assert!(belongs_to_tenant(&p, "acme"));
        assert!(!belongs_to_tenant(&p, "acm"));
        assert!(!belongs_to_tenant(&p, "acme2"));
        assert!(!belongs_to_tenant("players", "acme"));
    }

    #[test]
    fn create_resolves_key_and_persists() {
        let reg = registry();
        let (info, _delta) = reg.create("Acme Games", 1000, 50).unwrap();
        assert!(info.api_key.starts_with("ndbt_"));
        assert_eq!(reg.resolve_key(&info.api_key), Some(info.id.clone()));
        // role-suffix form also resolves
        assert_eq!(reg.resolve_key(&format!("{}:admin", info.api_key)), Some(info.id.clone()));
        // persisted to the system table
        let row = reg.tables.get_row(TENANTS_TABLE, &info.id).unwrap();
        assert!(row.is_some());
    }

    #[test]
    fn duplicate_name_rejected() {
        let reg = registry();
        reg.create("acme", 0, 0).unwrap();
        assert!(reg.create("acme", 0, 0).is_err());
    }

    #[test]
    fn name_with_colon_rejected() {
        let reg = registry();
        assert!(reg.create("bad:name", 0, 0).is_err());
    }

    #[test]
    fn rehydrate_recovers_from_table() {
        let tables = Arc::new(TableStore::new());
        let reg1 = TenantRegistry::load(tables.clone());
        let (info, _) = reg1.create("acme", 0, 0).unwrap();
        // Fresh registry over the same store (simulates restart after replay).
        let reg2 = TenantRegistry::load(tables);
        assert_eq!(reg2.resolve_key(&info.api_key), Some(info.id));
    }

    #[test]
    fn delete_removes_tenant_and_data() {
        let reg = registry();
        let (info, _) = reg.create("acme", 0, 0).unwrap();
        let phys = physical_table(&info.id, "players");
        reg.tables.set_row(phys.clone(), "p1".into(), serde_json::json!({"hp": 1})).unwrap();
        reg.tables.set_row(phys.clone(), "p2".into(), serde_json::json!({"hp": 2})).unwrap();
        assert_eq!(reg.tenant_row_count(&info.id), 2);

        let deltas = reg.delete(&info.id).unwrap();
        assert!(deltas.len() >= 3); // 2 data rows + tenant row
        assert_eq!(reg.resolve_key(&info.api_key), None);
        assert!(reg.tables.get_row(&phys, "p1").unwrap().is_none());
        assert!(reg.tables.get_row(TENANTS_TABLE, &info.id).unwrap().is_none());
    }

    #[test]
    fn rate_limit_enforced_and_refills() {
        let reg = registry();
        let (info, _) = reg.create("acme", 0, 5).unwrap();
        let mut allowed = 0;
        for _ in 0..20 {
            if reg.check_rate(&info.id) { allowed += 1; }
        }
        assert!(allowed <= 5, "burst exceeded bucket: {allowed}");
        // Refill after ~400ms at 5/s should grant ≥1 more.
        std::thread::sleep(std::time::Duration::from_millis(400));
        assert!(reg.check_rate(&info.id));
    }

    #[test]
    fn zero_limits_mean_unlimited() {
        let reg = registry();
        let (info, _) = reg.create("acme", 0, 0).unwrap();
        for _ in 0..1000 {
            assert!(reg.check_rate(&info.id));
        }
        assert_eq!(reg.row_quota(&info.id), 0);
    }

    #[test]
    fn unknown_key_does_not_resolve() {
        let reg = registry();
        reg.create("acme", 0, 0).unwrap();
        assert_eq!(reg.resolve_key("ndbt_deadbeef"), None);
        assert_eq!(reg.resolve_key(""), None);
    }
}
