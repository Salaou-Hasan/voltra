// ============================================================================
// auth_service/mod.rs — registration, login, and token management
//
// Sits on top of PersistentStore (SQLite) and IdentityIssuer (Ed25519 JWT).
// All operations are synchronous and CPU-bound; callers should spawn_blocking
// when calling from an async context.
//
// Performance note: bcrypt at cost 10 takes ~60ms per hash.  This is fine for
// login endpoints; it is NEVER called from game reducer code.
// ============================================================================

use crate::auth::IdentityIssuer;
use crate::error::{VoltraError, Result};
use crate::persistent::{PersistentStore, UserRow};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// ── Public types ─────────────────────────────────────────────────────────────

/// Public user record — no password hash exposed.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct User {
    pub id: String,
    pub email: String,
    pub role: String,
    pub created_at: i64,
}

impl From<UserRow> for User {
    fn from(r: UserRow) -> Self {
        User { id: r.id, email: r.email, role: r.role, created_at: r.created_at }
    }
}

// ── AuthService ───────────────────────────────────────────────────────────────

pub struct AuthService {
    pub store: Arc<PersistentStore>,
    pub issuer: Arc<IdentityIssuer>,
    /// JWT TTL in seconds (default 86 400 = 24 h).
    pub token_ttl_secs: u64,
}

impl AuthService {
    pub fn new(
        store: Arc<PersistentStore>,
        issuer: Arc<IdentityIssuer>,
        token_ttl_secs: u64,
    ) -> Self {
        AuthService { store, issuer, token_ttl_secs }
    }

    /// Register a new user.  Returns the created `User` (no token — call login next).
    pub fn register(&self, email: &str, password: &str, role: &str) -> Result<User> {
        // Normalise
        let email = email.trim().to_lowercase();
        if email.is_empty() || !email.contains('@') {
            return Err(VoltraError::invalid_argument("invalid email address"));
        }
        if password.len() < 8 {
            return Err(VoltraError::invalid_argument(
                "password must be at least 8 characters",
            ));
        }
        let role = if role.is_empty() { "player" } else { role };

        if self.store.user_by_email(&email)?.is_some() {
            return Err(VoltraError::invalid_argument("email already registered"));
        }

        let hash = bcrypt::hash(password, 10)
            .map_err(|e| VoltraError::internal(format!("bcrypt hash: {e}")))?;

        let id = generate_id();
        let now = now_secs();
        self.store.create_user(&id, &email, &hash, role, now)?;
        self.store.log_audit(Some(&id), "register", None)?;

        Ok(User { id, email, role: role.to_owned(), created_at: now })
    }

    /// Verify credentials and return `(User, jwt_token)`.
    pub fn login(&self, email: &str, password: &str) -> Result<(User, String)> {
        let email = email.trim().to_lowercase();
        let row = self
            .store
            .user_by_email(&email)?
            .ok_or_else(|| VoltraError::invalid_argument("invalid email or password"))?;

        let ok = bcrypt::verify(password, &row.password_hash)
            .map_err(|e| VoltraError::internal(format!("bcrypt verify: {e}")))?;
        if !ok {
            return Err(VoltraError::invalid_argument("invalid email or password"));
        }

        let token = self
            .issuer
            .issue(&row.id, vec![row.role.clone()], self.token_ttl_secs)
            .map_err(|e| VoltraError::internal(format!("JWT issue: {e}")))?;

        self.store.log_audit(Some(&row.id), "login", None)?;
        Ok((User::from(row), token))
    }

    /// Verify a JWT and return the user it belongs to.
    pub fn verify_token(&self, token: &str) -> Result<User> {
        let claims = self
            .issuer
            .verify(token)
            .map_err(|e| VoltraError::invalid_argument(format!("invalid token: {e}")))?;
        let row = self
            .store
            .user_by_id(&claims.sub)?
            .ok_or_else(|| VoltraError::invalid_argument("token references unknown user"))?;
        Ok(User::from(row))
    }

    pub fn get_user(&self, id: &str) -> Result<Option<User>> {
        Ok(self.store.user_by_id(id)?.map(User::from))
    }

    /// Change password after verifying the old one.
    pub fn change_password(
        &self, user_id: &str, old_password: &str, new_password: &str,
    ) -> Result<()> {
        if new_password.len() < 8 {
            return Err(VoltraError::invalid_argument(
                "password must be at least 8 characters",
            ));
        }
        let row = self
            .store
            .user_by_id(user_id)?
            .ok_or_else(|| VoltraError::invalid_argument("user not found"))?;
        let ok = bcrypt::verify(old_password, &row.password_hash)
            .map_err(|e| VoltraError::internal(format!("bcrypt verify: {e}")))?;
        if !ok {
            return Err(VoltraError::invalid_argument("incorrect current password"));
        }
        let hash = bcrypt::hash(new_password, 10)
            .map_err(|e| VoltraError::internal(format!("bcrypt hash: {e}")))?;
        self.store.update_password_hash(user_id, &hash, now_secs())?;
        self.store.log_audit(Some(user_id), "change_password", None)?;
        Ok(())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn generate_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos().hash(&mut h);
    std::thread::current().id().hash(&mut h);
    format!("usr_{:016x}", h.finish())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::IdentityIssuer;
    use crate::persistent::PersistentStore;
    use tempfile::NamedTempFile;

    fn setup() -> AuthService {
        let db_file = NamedTempFile::new().unwrap();
        let store = Arc::new(PersistentStore::open(db_file.path()).unwrap());
        let key_file = NamedTempFile::new().unwrap();
        let issuer = IdentityIssuer::generate();
        // Save & load to exercise full round-trip
        issuer.save_to_file(key_file.path()).unwrap();
        let issuer = Arc::new(IdentityIssuer::load_from_file(key_file.path()).unwrap());
        AuthService::new(store, issuer, 3600)
    }

    #[test]
    fn register_and_login() {
        let svc = setup();
        let user = svc.register("alice@example.com", "password123", "player").unwrap();
        assert_eq!(user.email, "alice@example.com");
        assert_eq!(user.role, "player");

        let (logged_in, token) = svc.login("alice@example.com", "password123").unwrap();
        assert_eq!(logged_in.id, user.id);
        assert!(!token.is_empty());
    }

    #[test]
    fn login_wrong_password_fails() {
        let svc = setup();
        svc.register("bob@example.com", "correct_horse", "player").unwrap();
        let err = svc.login("bob@example.com", "wrong").unwrap_err();
        assert!(err.to_string().contains("invalid email or password"));
    }

    #[test]
    fn duplicate_email_fails() {
        let svc = setup();
        svc.register("carol@example.com", "password123", "player").unwrap();
        let err = svc.register("carol@example.com", "password456", "player").unwrap_err();
        assert!(err.to_string().contains("already registered"));
    }

    #[test]
    fn verify_token_round_trip() {
        let svc = setup();
        svc.register("dave@example.com", "password999", "admin").unwrap();
        let (user, token) = svc.login("dave@example.com", "password999").unwrap();
        let verified = svc.verify_token(&token).unwrap();
        assert_eq!(verified.id, user.id);
        assert_eq!(verified.role, "admin");
    }

    #[test]
    fn change_password_works() {
        let svc = setup();
        let user = svc.register("eve@example.com", "old_password", "player").unwrap();
        svc.change_password(&user.id, "old_password", "new_password_123").unwrap();
        svc.login("eve@example.com", "new_password_123").unwrap();
        svc.login("eve@example.com", "old_password").unwrap_err();
    }

    #[test]
    fn short_password_rejected() {
        let svc = setup();
        let err = svc.register("f@example.com", "short", "player").unwrap_err();
        assert!(err.to_string().contains("8 characters"));
    }
}
