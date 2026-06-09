use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// Configuration for a single scheduled reducer invocation.
#[derive(Clone, Debug)]
pub struct ScheduledReducerConfig {
    /// Name of the reducer to call (must be registered in the ReducerRegistry).
    pub reducer: String,
    /// How often to fire, in milliseconds.
    pub interval_ms: u64,
    /// Optional JSON-encoded args to pass to the reducer.
    /// Will be MessagePack-encoded before dispatch.
    pub args_json: Option<String>,
}

/// Default policy for reducers that are NOT listed in `PermissionsConfig.rules`.
///
/// - `Open`   (default): unlisted reducers are callable by any role (fail-open).
/// - `Closed`: unlisted reducers are denied unless the caller is the scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PermissionsPolicy {
    Open,
    Closed,
}

impl Default for PermissionsPolicy {
    fn default() -> Self {
        PermissionsPolicy::Open
    }
}

/// Role-based access control configuration.
///
/// Maps reducer names to the list of roles that are allowed to call them.
/// A reducer not listed here is callable by ANY authenticated client (open) by default,
/// unless `default_policy` is set to `Closed`.
/// An empty `Vec` means NO role can call it (effectively disabled).
///
/// Example in `neondb.toml`:
/// ```toml
/// [server]
/// permissions_default_policy = "closed"
///
/// [permissions]
/// delete_player = ["admin"]
/// reset_scores  = ["admin", "moderator"]
/// increment     = ["user", "admin"]
/// ```
#[derive(Clone, Debug, Default)]
pub struct PermissionsConfig {
    /// reducer_name → allowed roles.
    pub rules: HashMap<String, Vec<String>>,
    /// Default policy for unlisted reducers.  Defaults to `Open` for backward compat.
    pub default_policy: PermissionsPolicy,
}

impl PermissionsConfig {
    /// Returns `true` if `role` is allowed to call `reducer`.
    ///
    /// Rules:
    /// - Scheduler calls (`caller_role == "scheduler"`) always bypass checks.
    /// - Reducer listed in the map → caller's role must be in the allowed list,
    ///   regardless of `default_policy`.
    /// - Reducer NOT in the map:
    ///     * `default_policy == Open`   → allowed.
    ///     * `default_policy == Closed` → denied.
    pub fn is_allowed(&self, reducer: &str, caller_role: &str) -> bool {
        // Scheduler is always trusted — it runs inside the server process.
        if caller_role == "scheduler" {
            return true;
        }
        match self.rules.get(reducer) {
            // Listed → strict role check, ignore default_policy.
            Some(roles) => roles.iter().any(|r| r == caller_role),
            // Not listed → honor default_policy.
            None => matches!(self.default_policy, PermissionsPolicy::Open),
        }
    }
}

/// TLS configuration — enables WSS (WebSocket Secure).
///
/// Example in `neondb.toml`:
/// ```toml
/// [tls]
/// enabled  = true
/// cert_path = "/etc/neondb/cert.pem"
/// key_path  = "/etc/neondb/key.pem"
/// ```
///
/// Environment variables:
/// - `NEONDB_TLS_ENABLED`    — "1" or "true"
/// - `NEONDB_TLS_CERT_PATH`  — path to PEM certificate
/// - `NEONDB_TLS_KEY_PATH`   — path to PEM private key
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_path: Option<std::path::PathBuf>,
    pub key_path: Option<std::path::PathBuf>,
}

/// Server configuration loaded from `neondb.toml`, environment variables, or defaults.
#[derive(Clone, Debug)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub wal_path: PathBuf,
    pub fsync_interval_ms: u32,
    pub wal_batch_size: usize,
    pub wal_batch_interval_ms: u32,
    pub unsafe_no_fsync: bool,
    pub shard_id: u32,
    pub shard_count: u32,
    pub log_level: String,
    pub metrics_port: u16,
    pub max_connections: usize,
    pub reducer_timeout_ms: u64,
    pub api_key: Option<String>,
    pub tune_system: bool,
    pub reuse_port: bool,
    /// Enable two-frame subscription delivery: route header + shared body per delta.
    /// Env: NEONDB_TWO_FRAME_PROTOCOL=1. Default: false (legacy one-frame mode).
    pub two_frame_protocol: bool,
    pub snapshot_interval: u64,
    pub snapshot_dir: PathBuf,
    pub scheduled_reducers: Vec<ScheduledReducerConfig>,
    /// Role-based access control rules.  Empty = no restrictions.
    pub permissions: PermissionsConfig,
    /// Maximum time (ms) a single SQL query may run before being cancelled.
    pub sql_timeout_ms: u64,
    /// Maximum size (bytes) of a single blob written through `BlobStore::store_blob`.
    /// A misbehaving reducer otherwise could stage a multi-GB inventory and balloon
    /// memory.  Default 16 MiB.  Env: `NEONDB_MAX_BLOB_SIZE`.
    pub max_blob_size_bytes: usize,
    /// Maximum linear memory (bytes) a WASM reducer instance may grow to.
    /// Enforced via Wasmtime's `ResourceLimiter`.  When exceeded, the WASM
    /// `memory.grow` instruction returns -1 and the reducer typically traps.
    /// Default 64 MiB.  Env: `NEONDB_REDUCER_MAX_MEMORY_BYTES`.
    pub reducer_max_memory_bytes: usize,
    /// Maximum size (bytes) of args bytes passed INTO a reducer and result
    /// bytes returned FROM it.  Applies to all backends.  Default 1 MiB.
    /// Env: `NEONDB_REDUCER_MAX_IO_BYTES`.
    pub reducer_max_io_bytes: usize,
    /// Rate limiter burst capacity per client (0 = disabled).
    /// Env: `NEONDB_RATE_LIMIT_CAPACITY`.  Default 100.
    pub rate_limit_capacity: u32,
    /// Rate limiter sustained calls/sec per client.
    /// Env: `NEONDB_RATE_LIMIT_RATE`.  Default 50.0.
    pub rate_limit_refill_rate: f64,
    /// Presence heartbeat timeout (ms) before marking idle. 0 = presence disabled.
    /// Env: `NEONDB_PRESENCE_HEARTBEAT_TIMEOUT_MS`.  Default 30000.
    pub presence_heartbeat_timeout_ms: u64,
    /// Presence offline timeout (ms) before removing user entirely.
    /// Env: `NEONDB_PRESENCE_OFFLINE_TIMEOUT_MS`.  Default 60000.
    pub presence_offline_timeout_ms: u64,
    /// TTL sweep interval (ms) — how often the background task checks for expired rows.
    /// Env: `NEONDB_TTL_SWEEP_INTERVAL_MS`.  Default 5000.
    pub ttl_sweep_interval_ms: u64,
    /// TLS / WSS configuration.  Disabled by default; configure via `[tls]` TOML section
    /// or `NEONDB_TLS_*` env vars.
    pub tls: TlsConfig,
}

// These structs mirror the TOML schema. Fields that are not yet wired into
// Config are kept for forward-compatibility and suppressed individually.
#[derive(Deserialize)]
struct ConfigFile {
    #[allow(dead_code)]
    project: Option<ConfigProject>,
    server: Option<ConfigServer>,
    scheduler: Option<Vec<ConfigScheduler>>,
    permissions: Option<HashMap<String, Vec<String>>>,
    #[serde(rename = "permissions_meta")]
    permissions_meta: Option<ConfigPermissionsMeta>,
    tls: Option<ConfigTls>,
}

#[derive(Deserialize)]
struct ConfigTls {
    enabled: Option<bool>,
    cert_path: Option<String>,
    key_path: Option<String>,
}

#[derive(Deserialize)]
struct ConfigPermissionsMeta {
    default_policy: Option<String>,
}

#[derive(Deserialize)]
struct ConfigProject {
    #[allow(dead_code)]
    name: Option<String>,
}

#[derive(Deserialize)]
struct ConfigServer {
    host: Option<String>,
    port: Option<u16>,
    wal_path: Option<String>,
    fsync_interval_ms: Option<u32>,
    wal_batch_size: Option<usize>,
    wal_batch_interval_ms: Option<u32>,
    unsafe_no_fsync: Option<bool>,
    shard_id: Option<u32>,
    shard_count: Option<u32>,
    log_level: Option<String>,
    metrics_port: Option<u16>,
    max_connections: Option<usize>,
    reducer_timeout_ms: Option<u64>,
    api_key: Option<String>,
    tune_system: Option<bool>,
    reuse_port: Option<bool>,
    two_frame_protocol: Option<bool>,
    snapshot_interval: Option<u64>,
    snapshot_dir: Option<String>,
    permissions_default_policy: Option<String>,
    sql_timeout_ms: Option<u64>,
    max_blob_size_bytes: Option<usize>,
    reducer_max_memory_bytes: Option<usize>,
    reducer_max_io_bytes: Option<usize>,
    rate_limit_capacity: Option<u32>,
    rate_limit_refill_rate: Option<f64>,
    presence_heartbeat_timeout_ms: Option<u64>,
    presence_offline_timeout_ms: Option<u64>,
    ttl_sweep_interval_ms: Option<u64>,
}

#[derive(Deserialize)]
struct ConfigScheduler {
    reducer: String,
    interval_ms: u64,
    args_json: Option<String>,
}

impl Config {
    /// Load configuration by searching for `neondb.toml` in current directory and parents,
    /// then merging environment variables and defaults.
    pub fn from_env() -> Self {
        let default_host = "127.0.0.1".to_string();
        let default_port = 3000u16;
        let default_wal = env::temp_dir().join("neondb.wal");
        let default_log = env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());

        let mut cfg = Config {
            host: default_host,
            port: default_port,
            wal_path: default_wal,
            fsync_interval_ms: 0,
            wal_batch_size: 100_000,
            wal_batch_interval_ms: 100,
            unsafe_no_fsync: false,
            shard_id: 0,
            shard_count: 1,
            log_level: default_log,
            metrics_port: default_port + 1,
            max_connections: 500,
            reducer_timeout_ms: 5_000,
            api_key: None,
            tune_system: false,
            reuse_port: true,
            two_frame_protocol: false,
            snapshot_interval: 1_000_000,
            snapshot_dir: env::temp_dir().join("neondb_snapshots"),
            scheduled_reducers: vec![],
            permissions: PermissionsConfig::default(),
            sql_timeout_ms: 5_000,
            max_blob_size_bytes: 16 * 1024 * 1024,
            reducer_max_memory_bytes: 64 * 1024 * 1024,
            reducer_max_io_bytes: 1 * 1024 * 1024,
            rate_limit_capacity: 100,
            rate_limit_refill_rate: 50.0,
            presence_heartbeat_timeout_ms: 30_000,
            presence_offline_timeout_ms: 60_000,
            ttl_sweep_interval_ms: 5_000,
            tls: TlsConfig::default(),
        };

        if let Some(toml_path) = find_config_in_cwd() {
            if let Ok(contents) = fs::read_to_string(&toml_path) {
                if let Ok(parsed) = toml::from_str::<ConfigFile>(&contents) {
                    apply_server_section(&mut cfg, parsed.server);
                    apply_scheduler_section(&mut cfg, parsed.scheduler);
                    apply_permissions_section(&mut cfg, parsed.permissions);
                    apply_permissions_meta(&mut cfg, parsed.permissions_meta);
                    apply_tls_section(&mut cfg, parsed.tls);
                }
            }
        }

        apply_env_overrides(&mut cfg);

        // Security warning: api_key=None on a non-loopback host means the
        // WebSocket port accepts unauthenticated connections from the network.
        if cfg.api_key.is_none() && cfg.host != "127.0.0.1" && cfg.host != "localhost" {
            log::warn!(
                "SECURITY WARNING: NeonDB is binding to '{}' with NO api_key set. \
                 Any client on the network can call reducers. \
                 Set NEONDB_API_KEY=<long-random-secret> or `[server] api_key = \"...\"` \
                 in neondb.toml before exposing this port. \
                 Use 127.0.0.1 for local-only development.",
                cfg.host
            );
        }

        cfg
    }

    pub fn load_from_path(path: &Path) -> Option<Self> {
        let contents = fs::read_to_string(path).ok()?;
        let parsed: ConfigFile = toml::from_str(&contents).ok()?;
        let mut cfg = Config::from_env();
        apply_server_section(&mut cfg, parsed.server);
        apply_scheduler_section(&mut cfg, parsed.scheduler);
        apply_permissions_section(&mut cfg, parsed.permissions);
        apply_permissions_meta(&mut cfg, parsed.permissions_meta);
        apply_tls_section(&mut cfg, parsed.tls);
        Some(cfg)
    }

    /// Apply process-wide limits derived from this Config to the global state.
    ///
    /// Currently sets the maximum blob size accepted by `BlobStore::store_blob`.
    /// The caller (typically `main.rs::run_server`) is expected to call this
    /// once at startup, after the Config has been loaded but before any
    /// reducer can run.  If never called, the table layer keeps its compile-time
    /// default (16 MiB).
    pub fn apply_global_limits(&self) {
        crate::table::set_max_blob_size(self.max_blob_size_bytes);
        crate::reducer::set_max_memory_bytes(self.reducer_max_memory_bytes);
        crate::reducer::set_max_io_bytes(self.reducer_max_io_bytes);
    }
}

fn parse_policy_str(s: &str) -> Option<PermissionsPolicy> {
    match s.trim().to_ascii_lowercase().as_str() {
        "open" => Some(PermissionsPolicy::Open),
        "closed" | "close" | "deny" | "default-deny" => Some(PermissionsPolicy::Closed),
        _ => None,
    }
}

fn apply_permissions_meta(cfg: &mut Config, meta: Option<ConfigPermissionsMeta>) {
    if let Some(m) = meta {
        if let Some(p) = m.default_policy.as_deref().and_then(parse_policy_str) {
            cfg.permissions.default_policy = p;
        }
    }
}

fn apply_server_section(cfg: &mut Config, server: Option<ConfigServer>) {
    let Some(s) = server else { return };
    if let Some(h) = s.host {
        cfg.host = h;
    }
    if let Some(p) = s.port {
        cfg.port = p;
    }
    if let Some(w) = s.wal_path {
        cfg.wal_path = PathBuf::from(w);
    }
    if let Some(f) = s.fsync_interval_ms {
        cfg.fsync_interval_ms = f;
    }
    if let Some(b) = s.wal_batch_size {
        cfg.wal_batch_size = b;
    }
    if let Some(i) = s.wal_batch_interval_ms {
        cfg.wal_batch_interval_ms = i;
    }
    if let Some(u) = s.unsafe_no_fsync {
        cfg.unsafe_no_fsync = u;
    }
    if let Some(s) = s.shard_id {
        cfg.shard_id = s;
    }
    if let Some(c) = s.shard_count {
        cfg.shard_count = c;
    }
    if let Some(l) = s.log_level {
        cfg.log_level = l;
    }
    if let Some(m) = s.metrics_port {
        cfg.metrics_port = m;
    }
    if let Some(m) = s.max_connections {
        cfg.max_connections = m;
    }
    if let Some(t) = s.reducer_timeout_ms {
        cfg.reducer_timeout_ms = t;
    }
    if let Some(k) = s.api_key {
        cfg.api_key = Some(k);
    }
    if let Some(t) = s.tune_system {
        cfg.tune_system = t;
    }
    if let Some(r) = s.reuse_port {
        cfg.reuse_port = r;
    }
    if let Some(t) = s.two_frame_protocol {
        cfg.two_frame_protocol = t;
    }
    if let Some(i) = s.snapshot_interval {
        cfg.snapshot_interval = i;
    }
    if let Some(d) = s.snapshot_dir {
        cfg.snapshot_dir = PathBuf::from(d);
    }
    if let Some(p) = s.permissions_default_policy.as_deref().and_then(parse_policy_str) {
        cfg.permissions.default_policy = p;
    }
    if let Some(t) = s.sql_timeout_ms {
        cfg.sql_timeout_ms = t;
    }
    if let Some(b) = s.max_blob_size_bytes {
        cfg.max_blob_size_bytes = b;
    }
    if let Some(m) = s.reducer_max_memory_bytes {
        cfg.reducer_max_memory_bytes = m;
    }
    if let Some(i) = s.reducer_max_io_bytes {
        cfg.reducer_max_io_bytes = i;
    }
    if let Some(c) = s.rate_limit_capacity {
        cfg.rate_limit_capacity = c;
    }
    if let Some(r) = s.rate_limit_refill_rate {
        cfg.rate_limit_refill_rate = r;
    }
    if let Some(h) = s.presence_heartbeat_timeout_ms {
        cfg.presence_heartbeat_timeout_ms = h;
    }
    if let Some(o) = s.presence_offline_timeout_ms {
        cfg.presence_offline_timeout_ms = o;
    }
    if let Some(t) = s.ttl_sweep_interval_ms {
        cfg.ttl_sweep_interval_ms = t;
    }
}

fn apply_scheduler_section(cfg: &mut Config, scheduler: Option<Vec<ConfigScheduler>>) {
    if let Some(entries) = scheduler {
        cfg.scheduled_reducers = entries
            .into_iter()
            .map(|s| ScheduledReducerConfig {
                reducer: s.reducer,
                interval_ms: s.interval_ms,
                args_json: s.args_json,
            })
            .collect();
    }
}

fn apply_permissions_section(
    cfg: &mut Config,
    permissions: Option<HashMap<String, Vec<String>>>,
) {
    if let Some(rules) = permissions {
        // Preserve any previously-applied default_policy.
        let policy = cfg.permissions.default_policy;
        cfg.permissions = PermissionsConfig { rules, default_policy: policy };
    }
}

fn apply_tls_section(cfg: &mut Config, tls: Option<ConfigTls>) {
    let Some(t) = tls else { return };
    if let Some(e) = t.enabled {
        cfg.tls.enabled = e;
    }
    if let Some(p) = t.cert_path {
        cfg.tls.cert_path = Some(PathBuf::from(p));
    }
    if let Some(p) = t.key_path {
        cfg.tls.key_path = Some(PathBuf::from(p));
    }
}

fn apply_env_overrides(cfg: &mut Config) {
    if let Ok(h) = env::var("NEONDB_HOST") {
        cfg.host = h;
    }
    if let Ok(p) =
        env::var("NEONDB_PORT").and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.port = p;
    }
    if let Ok(w) = env::var("NEONDB_WAL_PATH") {
        cfg.wal_path = PathBuf::from(w);
    }
    if let Ok(f) = env::var("NEONDB_FSYNC_INTERVAL_MS")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.fsync_interval_ms = f;
    }
    if let Ok(b) = env::var("NEONDB_WAL_BATCH_SIZE")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.wal_batch_size = b;
    }
    if let Ok(i) = env::var("NEONDB_WAL_BATCH_INTERVAL_MS")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.wal_batch_interval_ms = i;
    }
    if let Ok(v) = env::var("NEONDB_UNSAFE_NO_FSYNC") {
        cfg.unsafe_no_fsync = v == "1" || v.eq_ignore_ascii_case("true");
    }
    if let Ok(s) = env::var("NEONDB_SHARD_ID")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.shard_id = s;
    }
    if let Ok(c) = env::var("NEONDB_SHARD_COUNT")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.shard_count = c;
    }
    if let Ok(p) = env::var("NEONDB_METRICS_PORT")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.metrics_port = p;
    }
    if let Ok(c) = env::var("NEONDB_MAX_CONNECTIONS")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.max_connections = c;
    }
    if let Ok(t) = env::var("NEONDB_REDUCER_TIMEOUT_MS")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.reducer_timeout_ms = t;
    }
    if let Ok(k) = env::var("NEONDB_API_KEY") {
        cfg.api_key = Some(k);
    }
    if let Ok(l) = env::var("RUST_LOG") {
        cfg.log_level = l;
    }
    if let Ok(t) = env::var("NEONDB_TUNE_SYSTEM") {
        cfg.tune_system = t == "1" || t.eq_ignore_ascii_case("true");
    }
    if let Ok(r) = env::var("NEONDB_REUSE_PORT") {
        cfg.reuse_port = r == "1" || r.eq_ignore_ascii_case("true");
    }
    if let Ok(v) = env::var("NEONDB_TWO_FRAME_PROTOCOL") {
        cfg.two_frame_protocol = v == "1" || v.eq_ignore_ascii_case("true");
    }
    if let Ok(i) = env::var("NEONDB_SNAPSHOT_INTERVAL")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.snapshot_interval = i;
    }
    if let Ok(d) = env::var("NEONDB_SNAPSHOT_DIR") {
        cfg.snapshot_dir = PathBuf::from(d);
    }
    // NEONDB_PERMISSIONS accepts a JSON object: {"delete_player":["admin"],"increment":["user","admin"]}
    if let Ok(json) = env::var("NEONDB_PERMISSIONS") {
        if let Ok(rules) = serde_json::from_str::<HashMap<String, Vec<String>>>(&json) {
            let policy = cfg.permissions.default_policy;
            cfg.permissions = PermissionsConfig { rules, default_policy: policy };
        }
    }
    if let Ok(v) = env::var("NEONDB_PERMISSIONS_DEFAULT_POLICY") {
        if let Some(p) = parse_policy_str(&v) {
            cfg.permissions.default_policy = p;
        }
    }
    if let Ok(t) = env::var("NEONDB_SQL_TIMEOUT_MS")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.sql_timeout_ms = t;
    }
    if let Ok(b) = env::var("NEONDB_MAX_BLOB_SIZE")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.max_blob_size_bytes = b;
    }
    if let Ok(m) = env::var("NEONDB_REDUCER_MAX_MEMORY_BYTES")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.reducer_max_memory_bytes = m;
    }
    if let Ok(i) = env::var("NEONDB_REDUCER_MAX_IO_BYTES")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.reducer_max_io_bytes = i;
    }
    if let Ok(c) = env::var("NEONDB_RATE_LIMIT_CAPACITY")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.rate_limit_capacity = c;
    }
    if let Ok(r) = env::var("NEONDB_RATE_LIMIT_RATE")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.rate_limit_refill_rate = r;
    }
    if let Ok(h) = env::var("NEONDB_PRESENCE_HEARTBEAT_TIMEOUT_MS")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.presence_heartbeat_timeout_ms = h;
    }
    if let Ok(o) = env::var("NEONDB_PRESENCE_OFFLINE_TIMEOUT_MS")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.presence_offline_timeout_ms = o;
    }
    if let Ok(t) = env::var("NEONDB_TTL_SWEEP_INTERVAL_MS")
        .and_then(|v| v.parse().map_err(|_| std::env::VarError::NotPresent))
    {
        cfg.ttl_sweep_interval_ms = t;
    }
    if let Ok(v) = env::var("NEONDB_TLS_ENABLED") {
        cfg.tls.enabled = v == "1" || v.eq_ignore_ascii_case("true");
    }
    if let Ok(p) = env::var("NEONDB_TLS_CERT_PATH") {
        cfg.tls.cert_path = Some(PathBuf::from(p));
    }
    if let Ok(p) = env::var("NEONDB_TLS_KEY_PATH") {
        cfg.tls.key_path = Some(PathBuf::from(p));
    }
}

fn find_config_in_cwd() -> Option<PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        let candidate = dir.join("neondb.toml");
        if candidate.exists() {
            return Some(candidate);
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_env() {
        let config = Config::from_env();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 3000);
        assert_eq!(config.wal_batch_size, 100_000);
        assert_eq!(config.wal_batch_interval_ms, 100);
        assert!(config.scheduled_reducers.is_empty());
    }

    #[test]
    fn test_permissions_open_by_default() {
        let p = PermissionsConfig::default();
        // No rules means everything is allowed.
        assert!(p.is_allowed("any_reducer", "user"));
        assert!(p.is_allowed("any_reducer", ""));
    }

    #[test]
    fn test_permissions_role_check() {
        let mut rules = HashMap::new();
        rules.insert("delete_player".to_string(), vec!["admin".to_string()]);
        rules.insert("increment".to_string(), vec!["user".to_string(), "admin".to_string()]);
        let p = PermissionsConfig { rules, default_policy: PermissionsPolicy::Open };

        assert!(p.is_allowed("delete_player", "admin"));
        assert!(!p.is_allowed("delete_player", "user"));
        assert!(!p.is_allowed("delete_player", ""));
        assert!(p.is_allowed("increment", "user"));
        assert!(p.is_allowed("increment", "admin"));
        assert!(!p.is_allowed("increment", "guest"));
        // Unrestricted reducer.
        assert!(p.is_allowed("hello", "guest"));
    }

    #[test]
    fn test_permissions_scheduler_always_allowed() {
        let mut rules = HashMap::new();
        rules.insert("reset_scores".to_string(), vec!["admin".to_string()]);
        let p = PermissionsConfig { rules, default_policy: PermissionsPolicy::Open };
        // Scheduler bypasses all role checks.
        assert!(p.is_allowed("reset_scores", "scheduler"));
    }

    #[test]
    fn test_permissions_empty_roles_blocks_all() {
        let mut rules = HashMap::new();
        rules.insert("disabled_reducer".to_string(), vec![]);
        let p = PermissionsConfig { rules, default_policy: PermissionsPolicy::Open };
        assert!(!p.is_allowed("disabled_reducer", "admin"));
        assert!(!p.is_allowed("disabled_reducer", "user"));
        // Scheduler still gets through.
        assert!(p.is_allowed("disabled_reducer", "scheduler"));
    }

    // ── default_policy tests ─────────────────────────────────────────────────

    #[test]
    fn test_policy_open_unlisted_allowed() {
        let p = PermissionsConfig {
            rules: HashMap::new(),
            default_policy: PermissionsPolicy::Open,
        };
        assert!(p.is_allowed("anything", "user"));
        assert!(p.is_allowed("anything", ""));
    }

    #[test]
    fn test_policy_closed_unlisted_denied() {
        let p = PermissionsConfig {
            rules: HashMap::new(),
            default_policy: PermissionsPolicy::Closed,
        };
        assert!(!p.is_allowed("unlisted", "user"));
        assert!(!p.is_allowed("unlisted", "admin"));
        // Scheduler is always allowed even under closed policy.
        assert!(p.is_allowed("unlisted", "scheduler"));
    }

    #[test]
    fn test_policy_closed_listed_still_strict() {
        // When listed, role must match — closed policy does NOT auto-allow listed reducers.
        let mut rules = HashMap::new();
        rules.insert("delete_player".to_string(), vec!["admin".to_string()]);
        let p = PermissionsConfig {
            rules,
            default_policy: PermissionsPolicy::Closed,
        };
        assert!(p.is_allowed("delete_player", "admin"));
        assert!(!p.is_allowed("delete_player", "user"));
        // Unlisted reducer is denied.
        assert!(!p.is_allowed("hello", "user"));
    }

    #[test]
    fn test_policy_open_listed_still_strict() {
        // Listed rules always win, regardless of open default.
        let mut rules = HashMap::new();
        rules.insert("delete_player".to_string(), vec!["admin".to_string()]);
        let p = PermissionsConfig {
            rules,
            default_policy: PermissionsPolicy::Open,
        };
        assert!(p.is_allowed("delete_player", "admin"));
        assert!(!p.is_allowed("delete_player", "user"));
        // Unlisted is open.
        assert!(p.is_allowed("hello", "user"));
    }

    #[test]
    fn test_parse_policy_str_accepts_variants() {
        assert_eq!(parse_policy_str("open"), Some(PermissionsPolicy::Open));
        assert_eq!(parse_policy_str("OPEN"), Some(PermissionsPolicy::Open));
        assert_eq!(parse_policy_str("closed"), Some(PermissionsPolicy::Closed));
        assert_eq!(parse_policy_str("Closed"), Some(PermissionsPolicy::Closed));
        assert_eq!(parse_policy_str("deny"), Some(PermissionsPolicy::Closed));
        assert_eq!(parse_policy_str("bogus"), None);
    }

    #[test]
    fn test_config_from_env_default_policy_is_open() {
        // Default policy must remain Open for backward compatibility.
        let config = Config::from_env();
        assert_eq!(config.permissions.default_policy, PermissionsPolicy::Open);
    }

    #[test]
    fn test_config_default_sql_timeout() {
        let config = Config::from_env();
        assert_eq!(config.sql_timeout_ms, 5_000);
    }

    #[test]
    fn test_config_default_reducer_limits_sane() {
        // The defaults must be large enough to support real reducers but small
        // enough to prevent a single misbehaving module from exhausting host RAM.
        let config = Config::from_env();
        assert!(
            config.reducer_max_memory_bytes >= 1 * 1024 * 1024,
            "max_memory_bytes too small: {}",
            config.reducer_max_memory_bytes
        );
        assert!(
            config.reducer_max_io_bytes >= 64 * 1024,
            "max_io_bytes too small: {}",
            config.reducer_max_io_bytes
        );
        // Sanity caps: defaults shouldn't be absurd.
        assert!(config.reducer_max_memory_bytes <= 1 * 1024 * 1024 * 1024);
        assert!(config.reducer_max_io_bytes <= 64 * 1024 * 1024);
    }
}
