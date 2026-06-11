//! PostgreSQL catalog: table definitions persisted in the MVCC store.
//!
//! Each table definition lives as a JSON blob in `NS_PG_CATALOG` keyed by the
//! lowercase table name. Row data lives in the table's own namespace, keyed by
//! an 8-byte big-endian rowid. Rowid counters are rebuilt on boot by scanning
//! each table's keys, so they survive AOF replay without extra bookkeeping.

use super::types::ColType;
use crate::mvcc::{Datum, MvccStore, NS_PG_BASE, NS_PG_CATALOG};
use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub ctype: ColType,
    pub not_null: bool,
    pub primary_key: bool,
    /// SERIAL/BIGSERIAL columns default to the rowid.
    pub serial: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub ns: u32,
    pub columns: Vec<ColumnDef>,
    #[serde(skip)]
    pub next_rowid: AtomicU64,
}

impl TableDef {
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }
    pub fn alloc_rowid(&self) -> u64 {
        self.next_rowid.fetch_add(1, Ordering::Relaxed)
    }
}

pub fn rowid_key(rowid: u64) -> Bytes {
    Bytes::copy_from_slice(&rowid.to_be_bytes())
}

pub struct Catalog {
    tables: DashMap<String, Arc<TableDef>>,
    next_ns: AtomicU32,
}

impl Catalog {
    /// Rebuild the catalog from the store (after snapshot + AOF replay).
    pub fn load(store: &MvccStore) -> Self {
        let cat = Catalog { tables: DashMap::new(), next_ns: AtomicU32::new(NS_PG_BASE) };
        let ts = store.current_ts();
        store.for_each_visible(NS_PG_CATALOG, ts, |_key, datum| {
            if let Datum::Str(raw) = datum {
                if let Ok(def) = serde_json::from_slice::<TableDef>(raw) {
                    if def.ns >= cat.next_ns.load(Ordering::Relaxed) {
                        cat.next_ns.store(def.ns + 1, Ordering::Relaxed);
                    }
                    cat.tables.insert(def.name.clone(), Arc::new(def));
                }
            }
        });
        // Rebuild rowid counters from existing row keys.
        for entry in cat.tables.iter() {
            let def = entry.value();
            let mut max_rowid = 0u64;
            store.for_each_visible(def.ns, ts, |key, _| {
                if key.len() == 8 {
                    let mut be = [0u8; 8];
                    be.copy_from_slice(key);
                    max_rowid = max_rowid.max(u64::from_be_bytes(be));
                }
            });
            def.next_rowid.store(max_rowid + 1, Ordering::Relaxed);
        }
        cat
    }

    pub fn get(&self, name: &str) -> Option<Arc<TableDef>> {
        self.tables.get(&name.to_lowercase()).map(|t| t.clone())
    }

    pub fn list(&self) -> Vec<Arc<TableDef>> {
        let mut v: Vec<Arc<TableDef>> = self.tables.iter().map(|e| e.value().clone()).collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    /// Register a new table. Returns the def and the catalog write
    /// (the caller commits it through the MVCC store).
    pub fn create(
        &self,
        name: &str,
        columns: Vec<ColumnDef>,
    ) -> Result<(Arc<TableDef>, Bytes, Bytes), String> {
        let lname = name.to_lowercase();
        if self.tables.contains_key(&lname) {
            return Err(format!("relation \"{lname}\" already exists"));
        }
        let def = Arc::new(TableDef {
            name: lname.clone(),
            ns: self.next_ns.fetch_add(1, Ordering::Relaxed),
            columns,
            next_rowid: AtomicU64::new(1),
        });
        let blob = serde_json::to_vec(def.as_ref())
            .map_err(|e| format!("catalog encode failed: {e}"))?;
        self.tables.insert(lname.clone(), def.clone());
        Ok((def, Bytes::from(lname.into_bytes()), Bytes::from(blob)))
    }

    /// Remove a table. Returns its def for row cleanup.
    pub fn drop_table(&self, name: &str) -> Option<Arc<TableDef>> {
        self.tables.remove(&name.to_lowercase()).map(|(_, def)| def)
    }
}

// next_rowid is #[serde(skip)] — provide the Default it needs.
impl Default for TableDef {
    fn default() -> Self {
        Self { name: String::new(), ns: 0, columns: Vec::new(), next_rowid: AtomicU64::new(1) }
    }
}
