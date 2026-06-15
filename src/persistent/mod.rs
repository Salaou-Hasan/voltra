// ============================================================================
// persistent/mod.rs — SQLite-backed relational tier for NeonDB
//
// Performance contract:
//   - The game hot path (DashMap → kanal → WAL) NEVER calls into this module.
//   - SQLite is accessed only at:
//       • WebSocket handshake (auth verify + character load)
//       • Disconnect / background flush (character save)
//       • HTTP auth endpoints (/auth/register, /auth/login, etc.)
//   - Single Mutex<Connection> behind WAL journal mode gives safe concurrency.
//     At 1000 logins/sec this is well within capacity; game reducers never wait.
// ============================================================================

use crate::error::{NeonDBError, Result};
use rusqlite::{params, Connection};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

// ── PersistentStore ───────────────────────────────────────────────────────────

pub struct PersistentStore {
    conn: Mutex<Connection>,
    pub path: PathBuf,
}

impl PersistentStore {
    /// Open (or create) the SQLite database at `path`.
    /// Sets WAL journal mode and initialises the schema on first run.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .map_err(|e| NeonDBError::internal(format!("SQLite open: {e}")))?;

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous  = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA cache_size   = -16000;
             PRAGMA temp_store   = MEMORY;",
        )
        .map_err(|e| NeonDBError::internal(format!("SQLite pragmas: {e}")))?;

        let store = PersistentStore {
            conn: Mutex::new(conn),
            path: path.to_path_buf(),
        };
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS users (
                id            TEXT PRIMARY KEY,
                email         TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role          TEXT NOT NULL DEFAULT 'player',
                created_at    INTEGER NOT NULL,
                updated_at    INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

            CREATE TABLE IF NOT EXISTS characters (
                id         TEXT PRIMARY KEY,
                user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                name       TEXT NOT NULL,
                data       TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_characters_user ON characters(user_id);

            CREATE TABLE IF NOT EXISTS item_catalog (
                id         TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                item_type  TEXT NOT NULL DEFAULT 'generic',
                stats      TEXT NOT NULL DEFAULT '{}',
                price      INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                action  TEXT NOT NULL,
                data    TEXT,
                ts      INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts);",
        )
        .map_err(|e| NeonDBError::internal(format!("SQLite schema init: {e}")))?;
        Ok(())
    }

    // ── Users ─────────────────────────────────────────────────────────────────

    pub fn create_user(
        &self, id: &str, email: &str, hash: &str, role: &str, now: i64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO users (id, email, password_hash, role, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
            params![id, email, hash, role, now],
        )
        .map_err(|e| NeonDBError::internal(format!("create_user: {e}")))?;
        Ok(())
    }

    /// Returns `(id, role, password_hash, created_at)` for the given email.
    pub fn user_by_email(&self, email: &str) -> Result<Option<UserRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, email, role, password_hash, created_at
                 FROM users WHERE email = ?1",
            )
            .map_err(|e| NeonDBError::internal(format!("prepare: {e}")))?;
        let mut rows = stmt
            .query(params![email])
            .map_err(|e| NeonDBError::internal(format!("query: {e}")))?;
        if let Some(row) = rows
            .next()
            .map_err(|e| NeonDBError::internal(e.to_string()))?
        {
            Ok(Some(UserRow {
                id: row.get(0).map_err(|e| NeonDBError::internal(e.to_string()))?,
                email: row.get(1).map_err(|e| NeonDBError::internal(e.to_string()))?,
                role: row.get(2).map_err(|e| NeonDBError::internal(e.to_string()))?,
                password_hash: row.get(3).map_err(|e| NeonDBError::internal(e.to_string()))?,
                created_at: row.get(4).map_err(|e| NeonDBError::internal(e.to_string()))?,
            }))
        } else {
            Ok(None)
        }
    }

    /// Returns user row by id.
    pub fn user_by_id(&self, id: &str) -> Result<Option<UserRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, email, role, password_hash, created_at
                 FROM users WHERE id = ?1",
            )
            .map_err(|e| NeonDBError::internal(format!("prepare: {e}")))?;
        let mut rows = stmt
            .query(params![id])
            .map_err(|e| NeonDBError::internal(format!("query: {e}")))?;
        if let Some(row) = rows
            .next()
            .map_err(|e| NeonDBError::internal(e.to_string()))?
        {
            Ok(Some(UserRow {
                id: row.get(0).map_err(|e| NeonDBError::internal(e.to_string()))?,
                email: row.get(1).map_err(|e| NeonDBError::internal(e.to_string()))?,
                role: row.get(2).map_err(|e| NeonDBError::internal(e.to_string()))?,
                password_hash: row.get(3).map_err(|e| NeonDBError::internal(e.to_string()))?,
                created_at: row.get(4).map_err(|e| NeonDBError::internal(e.to_string()))?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn update_password_hash(&self, user_id: &str, hash: &str, now: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE users SET password_hash = ?1, updated_at = ?2 WHERE id = ?3",
            params![hash, now, user_id],
        )
        .map_err(|e| NeonDBError::internal(format!("update_password: {e}")))?;
        Ok(())
    }

    // ── Characters ────────────────────────────────────────────────────────────

    /// Upsert a character row.  The `data` field stores the full game-state JSON.
    pub fn save_character(
        &self, id: &str, user_id: &str, name: &str, data: &Value, now: i64,
    ) -> Result<()> {
        let data_str = data.to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO characters (id, user_id, name, data, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5)
             ON CONFLICT(id) DO UPDATE SET
               name       = excluded.name,
               data       = excluded.data,
               updated_at = excluded.updated_at",
            params![id, user_id, name, data_str, now],
        )
        .map_err(|e| NeonDBError::internal(format!("save_character: {e}")))?;
        Ok(())
    }

    pub fn load_character(&self, id: &str) -> Result<Option<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT data FROM characters WHERE id = ?1")
            .map_err(|e| NeonDBError::internal(format!("prepare: {e}")))?;
        let mut rows = stmt
            .query(params![id])
            .map_err(|e| NeonDBError::internal(format!("query: {e}")))?;
        if let Some(row) = rows
            .next()
            .map_err(|e| NeonDBError::internal(e.to_string()))?
        {
            let s: String = row.get(0).map_err(|e| NeonDBError::internal(e.to_string()))?;
            Ok(Some(serde_json::from_str(&s).unwrap_or(Value::Object(Default::default()))))
        } else {
            Ok(None)
        }
    }

    pub fn list_characters_for_user(
        &self, user_id: &str,
    ) -> Result<Vec<CharacterSummary>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, name, updated_at FROM characters
                 WHERE user_id = ?1 ORDER BY updated_at DESC",
            )
            .map_err(|e| NeonDBError::internal(format!("prepare: {e}")))?;
        let rows = stmt
            .query_map(params![user_id], |row| {
                Ok(CharacterSummary {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    updated_at: row.get(2)?,
                })
            })
            .map_err(|e| NeonDBError::internal(format!("query: {e}")))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| NeonDBError::internal(e.to_string()))?);
        }
        Ok(out)
    }

    // ── Item Catalog ──────────────────────────────────────────────────────────

    pub fn upsert_catalog_item(
        &self, id: &str, name: &str, item_type: &str, stats: &Value, price: i64, now: i64,
    ) -> Result<()> {
        let stats_str = stats.to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO item_catalog (id, name, item_type, stats, price, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
             ON CONFLICT(id) DO UPDATE SET
               name       = excluded.name,
               item_type  = excluded.item_type,
               stats      = excluded.stats,
               price      = excluded.price,
               updated_at = excluded.updated_at",
            params![id, name, item_type, stats_str, price, now],
        )
        .map_err(|e| NeonDBError::internal(format!("upsert_catalog_item: {e}")))?;
        Ok(())
    }

    pub fn get_catalog_item(&self, id: &str) -> Result<Option<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT id, name, item_type, stats, price FROM item_catalog WHERE id = ?1",
            )
            .map_err(|e| NeonDBError::internal(format!("prepare: {e}")))?;
        let mut rows = stmt
            .query(params![id])
            .map_err(|e| NeonDBError::internal(format!("query: {e}")))?;
        if let Some(row) = rows
            .next()
            .map_err(|e| NeonDBError::internal(e.to_string()))?
        {
            let stats_str: String =
                row.get(3).map_err(|e| NeonDBError::internal(e.to_string()))?;
            let stats: Value =
                serde_json::from_str(&stats_str).unwrap_or(Value::Object(Default::default()));
            let id_val: String =
                row.get(0).map_err(|e| NeonDBError::internal(e.to_string()))?;
            let name: String =
                row.get(1).map_err(|e| NeonDBError::internal(e.to_string()))?;
            let itype: String =
                row.get(2).map_err(|e| NeonDBError::internal(e.to_string()))?;
            let price: i64 =
                row.get(4).map_err(|e| NeonDBError::internal(e.to_string()))?;
            Ok(Some(serde_json::json!({
                "id": id_val, "name": name, "type": itype, "stats": stats, "price": price
            })))
        } else {
            Ok(None)
        }
    }

    pub fn list_catalog(&self, item_type: Option<&str>) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        // Fetch all rows and filter in-memory — avoids rusqlite MappedRows
        // lifetime issues when branching on query params.
        let mut stmt = conn
            .prepare(
                "SELECT id, name, item_type, stats, price FROM item_catalog ORDER BY name",
            )
            .map_err(|e| NeonDBError::internal(format!("prepare: {e}")))?;
        let raw: Vec<(String, String, String, String, i64)> =
            match stmt.query_map([], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            }) {
                Ok(mapped) => mapped.filter_map(|r| r.ok()).collect(),
                Err(e) => return Err(NeonDBError::internal(e.to_string())),
            };
        let itype_filter = item_type.map(|s| s.to_owned());
        let tuples: Vec<(String, String, String, String, i64)> = raw
            .into_iter()
            .filter(|(_, _, t, _, _)| itype_filter.as_deref().map_or(true, |f| t == f))
            .collect();
        Ok(tuples
            .into_iter()
            .map(|(id, name, itype, stats_str, price)| {
                let stats = serde_json::from_str(&stats_str)
                    .unwrap_or(Value::Object(Default::default()));
                serde_json::json!({"id": id, "name": name, "type": itype, "stats": stats, "price": price})
            })
            .collect())
    }

    // ── Audit log ─────────────────────────────────────────────────────────────

    pub fn log_audit(
        &self, user_id: Option<&str>, action: &str, data: Option<&Value>,
    ) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let data_str = data.map(|v| v.to_string());
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO audit_log (user_id, action, data, ts) VALUES (?1, ?2, ?3, ?4)",
            params![user_id, action, data_str, now],
        )
        .map_err(|e| NeonDBError::internal(format!("log_audit: {e}")))?;
        Ok(())
    }

    // ── Raw SQL (admin console only — never called from game hot path) ────────

    pub fn exec_sql(&self, sql: &str) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| NeonDBError::internal(format!("SQL prepare: {e}")))?;
        let col_names: Vec<String> = stmt
            .column_names()
            .into_iter()
            .map(|s| s.to_owned())
            .collect();
        let rows = stmt
            .query_map([], |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in col_names.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i)?;
                    let json_val = match val {
                        rusqlite::types::Value::Null => Value::Null,
                        rusqlite::types::Value::Integer(n) => Value::Number(n.into()),
                        rusqlite::types::Value::Real(f) => {
                            serde_json::Number::from_f64(f)
                                .map(Value::Number)
                                .unwrap_or(Value::Null)
                        }
                        rusqlite::types::Value::Text(s) => Value::String(s),
                        rusqlite::types::Value::Blob(b) => {
                            use base64::Engine as _;
                            Value::String(base64::engine::general_purpose::STANDARD.encode(&b))
                        }
                    };
                    obj.insert(col.clone(), json_val);
                }
                Ok(Value::Object(obj))
            })
            .map_err(|e| NeonDBError::internal(format!("SQL query: {e}")))?;
        let mut result = Vec::new();
        for r in rows {
            result.push(r.map_err(|e| NeonDBError::internal(e.to_string()))?);
        }
        Ok(result)
    }
}

// ── Value types ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct UserRow {
    pub id: String,
    pub email: String,
    pub role: String,
    pub password_hash: String,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct CharacterSummary {
    pub id: String,
    pub name: String,
    pub updated_at: i64,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn open_tmp() -> PersistentStore {
        let f = NamedTempFile::new().unwrap();
        PersistentStore::open(f.path()).unwrap()
    }

    #[test]
    fn schema_initialises_without_error() {
        let _ = open_tmp();
    }

    #[test]
    fn create_and_find_user() {
        let db = open_tmp();
        db.create_user("u1", "alice@example.com", "hash123", "player", 1_000_000)
            .unwrap();
        let row = db.user_by_email("alice@example.com").unwrap().unwrap();
        assert_eq!(row.id, "u1");
        assert_eq!(row.email, "alice@example.com");
        assert_eq!(row.role, "player");
        assert_eq!(row.password_hash, "hash123");
    }

    #[test]
    fn user_by_id_returns_correct_row() {
        let db = open_tmp();
        db.create_user("u2", "bob@example.com", "hash_bob", "admin", 2_000_000)
            .unwrap();
        let row = db.user_by_id("u2").unwrap().unwrap();
        assert_eq!(row.email, "bob@example.com");
        assert_eq!(row.role, "admin");
    }

    #[test]
    fn user_by_email_not_found() {
        let db = open_tmp();
        assert!(db.user_by_email("nobody@example.com").unwrap().is_none());
    }

    #[test]
    fn save_and_load_character() {
        let db = open_tmp();
        db.create_user("u3", "carol@example.com", "h", "player", 1)
            .unwrap();
        let data = serde_json::json!({ "hp": 100, "level": 5, "zone": "forest" });
        db.save_character("char1", "u3", "Carol", &data, 1_000_000)
            .unwrap();
        let loaded = db.load_character("char1").unwrap().unwrap();
        assert_eq!(loaded["hp"], serde_json::json!(100));
        assert_eq!(loaded["level"], serde_json::json!(5));
    }

    #[test]
    fn save_character_upserts() {
        let db = open_tmp();
        db.create_user("u4", "d@example.com", "h", "player", 1)
            .unwrap();
        let v1 = serde_json::json!({ "hp": 50 });
        db.save_character("char2", "u4", "Dave", &v1, 1).unwrap();
        let v2 = serde_json::json!({ "hp": 99 });
        db.save_character("char2", "u4", "Dave", &v2, 2).unwrap();
        let loaded = db.load_character("char2").unwrap().unwrap();
        assert_eq!(loaded["hp"], serde_json::json!(99));
    }

    #[test]
    fn catalog_upsert_and_get() {
        let db = open_tmp();
        let stats = serde_json::json!({ "atk": 10 });
        db.upsert_catalog_item("sword_01", "Iron Sword", "weapon", &stats, 100, 1)
            .unwrap();
        let item = db.get_catalog_item("sword_01").unwrap().unwrap();
        assert_eq!(item["name"], "Iron Sword");
        assert_eq!(item["price"], serde_json::json!(100));
    }

    #[test]
    fn audit_log_inserts() {
        let db = open_tmp();
        db.log_audit(Some("u1"), "login", Some(&serde_json::json!({"ip": "1.2.3.4"})))
            .unwrap();
        db.log_audit(None, "server_start", None).unwrap();
    }

    #[test]
    fn exec_sql_select() {
        let db = open_tmp();
        db.create_user("u5", "e@example.com", "h", "player", 1)
            .unwrap();
        let rows = db.exec_sql("SELECT id, email FROM users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["email"], "e@example.com");
    }
}
