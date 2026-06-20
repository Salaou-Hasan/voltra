use ed25519_dalek::{SigningKey, pkcs8::DecodePrivateKey, pkcs8::EncodePrivateKey};
use pkcs8::LineEnding;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Claims extracted from a valid JWT.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NeonDBClaims {
    /// Subject — the user ID.
    pub sub: String,
    /// Role (e.g., "admin", "player", "spectator").
    #[serde(default)]
    pub role: String,
    /// Issued-at timestamp (Unix seconds).
    #[serde(default)]
    pub iat: u64,
    /// Expiration timestamp (Unix seconds). 0 means no expiry.
    #[serde(default)]
    pub exp: u64,
    /// Optional: custom namespace claims (game-specific metadata).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Supported authentication modes.
#[derive(Clone, Debug)]
#[derive(Default)]
pub enum AuthMode {
    /// No authentication required (dev mode).
    #[default]
    None,
    /// Static API key (existing behavior).
    ApiKey(String),
    /// JWT with HMAC signing (HS256/HS384/HS512).
    /// The String is the shared secret.
    JwtHmac { secret: String, algorithm: Algorithm },
    /// JWT with RSA public key verification (RS256/RS384/RS512).
    /// The String is the PEM-encoded public key.
    JwtRsa {
        public_key_pem: String,
        algorithm: Algorithm,
    },
}


/// Result of validating an auth token.
#[derive(Clone, Debug)]
pub enum AuthResult {
    /// Authentication succeeded. Contains extracted identity.
    Authenticated {
        user_id: String,
        role: String,
        claims: NeonDBClaims,
    },
    /// Authentication failed with reason.
    Denied(String),
    /// No auth configured — allow anonymous access.
    Anonymous,
}

/// The auth validator. Constructed once at startup, shared across all connections.
pub struct AuthValidator {
    mode: AuthMode,
}

impl AuthValidator {
    /// Create a new AuthValidator with the given mode.
    pub fn new(mode: AuthMode) -> Self {
        AuthValidator { mode }
    }

    /// Validate a Bearer token string.
    /// Input: the raw header value (e.g., "Bearer eyJ..." or "Bearer my-api-key" or "Bearer key:role")
    pub fn validate(&self, authorization_header: &str) -> AuthResult {
        // Strip "Bearer " prefix
        let token = if let Some(stripped) = authorization_header.strip_prefix("Bearer ") {
            stripped.trim()
        } else if let Some(stripped) = authorization_header.strip_prefix("bearer ") {
            stripped.trim()
        } else {
            return AuthResult::Denied("Missing 'Bearer ' prefix in Authorization header".into());
        };

        if token.is_empty() {
            return AuthResult::Denied("Empty token after Bearer prefix".into());
        }

        match &self.mode {
            AuthMode::None => AuthResult::Anonymous,

            AuthMode::ApiKey(expected_key) => {
                // Support "key:role" format
                if let Some((key_part, role_part)) = token.split_once(':') {
                    if key_part == expected_key {
                        AuthResult::Authenticated {
                            user_id: "api_key_user".into(),
                            role: role_part.to_string(),
                            claims: NeonDBClaims {
                                sub: "api_key_user".into(),
                                role: role_part.to_string(),
                                iat: 0,
                                exp: 0,
                                metadata: None,
                            },
                        }
                    } else {
                        AuthResult::Denied("Invalid API key".into())
                    }
                } else if token == expected_key {
                    AuthResult::Authenticated {
                        user_id: "api_key_user".into(),
                        role: "default".into(),
                        claims: NeonDBClaims {
                            sub: "api_key_user".into(),
                            role: "default".into(),
                            iat: 0,
                            exp: 0,
                            metadata: None,
                        },
                    }
                } else {
                    AuthResult::Denied("Invalid API key".into())
                }
            }

            AuthMode::JwtHmac { secret, algorithm } => {
                let mut validation = Validation::new(*algorithm);
                validation.validate_aud = false;
                // We handle exp validation ourselves to support exp=0 meaning no expiry
                validation.validate_exp = false;

                match decode::<NeonDBClaims>(
                    token,
                    &DecodingKey::from_secret(secret.as_bytes()),
                    &validation,
                ) {
                    Ok(token_data) => {
                        let claims = token_data.claims;
                        // Check expiration manually: exp=0 means no expiry
                        if claims.exp > 0 {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            if now > claims.exp {
                                return AuthResult::Denied(format!(
                                    "Token expired at {}, current time is {}",
                                    claims.exp, now
                                ));
                            }
                        }
                        AuthResult::Authenticated {
                            user_id: claims.sub.clone(),
                            role: claims.role.clone(),
                            claims,
                        }
                    }
                    Err(e) => AuthResult::Denied(format!("JWT validation failed: {}", e)),
                }
            }

            AuthMode::JwtRsa {
                public_key_pem,
                algorithm,
            } => {
                let mut validation = Validation::new(*algorithm);
                validation.validate_aud = false;
                validation.validate_exp = false;

                let decoding_key = match DecodingKey::from_rsa_pem(public_key_pem.as_bytes()) {
                    Ok(key) => key,
                    Err(e) => {
                        return AuthResult::Denied(format!("Invalid RSA public key: {}", e));
                    }
                };

                match decode::<NeonDBClaims>(token, &decoding_key, &validation) {
                    Ok(token_data) => {
                        let claims = token_data.claims;
                        // Check expiration manually: exp=0 means no expiry
                        if claims.exp > 0 {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            if now > claims.exp {
                                return AuthResult::Denied(format!(
                                    "Token expired at {}, current time is {}",
                                    claims.exp, now
                                ));
                            }
                        }
                        AuthResult::Authenticated {
                            user_id: claims.sub.clone(),
                            role: claims.role.clone(),
                            claims,
                        }
                    }
                    Err(e) => AuthResult::Denied(format!("JWT validation failed: {}", e)),
                }
            }
        }
    }

    /// Generate a JWT (for testing or for the server to issue tokens).
    /// Only works in JwtHmac mode.
    pub fn generate_token(
        &self,
        user_id: &str,
        role: &str,
        ttl_seconds: u64,
    ) -> Result<String, String> {
        match &self.mode {
            AuthMode::JwtHmac { secret, algorithm } => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                let exp = if ttl_seconds == 0 {
                    0
                } else {
                    now + ttl_seconds
                };

                let claims = NeonDBClaims {
                    sub: user_id.to_string(),
                    role: role.to_string(),
                    iat: now,
                    exp,
                    metadata: None,
                };

                encode(
                    &Header::new(*algorithm),
                    &claims,
                    &EncodingKey::from_secret(secret.as_bytes()),
                )
                .map_err(|e| format!("Failed to encode JWT: {}", e))
            }
            _ => Err("generate_token only works in JwtHmac mode".into()),
        }
    }

    /// Generate a JWT with custom metadata attached.
    /// Only works in JwtHmac mode.
    pub fn generate_token_with_metadata(
        &self,
        user_id: &str,
        role: &str,
        ttl_seconds: u64,
        metadata: serde_json::Value,
    ) -> Result<String, String> {
        match &self.mode {
            AuthMode::JwtHmac { secret, algorithm } => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                let exp = if ttl_seconds == 0 {
                    0
                } else {
                    now + ttl_seconds
                };

                let claims = NeonDBClaims {
                    sub: user_id.to_string(),
                    role: role.to_string(),
                    iat: now,
                    exp,
                    metadata: Some(metadata),
                };

                encode(
                    &Header::new(*algorithm),
                    &claims,
                    &EncodingKey::from_secret(secret.as_bytes()),
                )
                .map_err(|e| format!("Failed to encode JWT: {}", e))
            }
            _ => Err("generate_token_with_metadata only works in JwtHmac mode".into()),
        }
    }

    /// Determine auth mode from environment variables.
    /// Priority:
    ///   1. NEONDB_JWT_SECRET -> JwtHmac (HS256)
    ///   2. NEONDB_JWT_PUBLIC_KEY or NEONDB_JWT_PUBLIC_KEY_FILE -> JwtRsa (RS256)
    ///   3. NEONDB_API_KEY -> ApiKey
    ///   4. None -> AuthMode::None (dev mode)
    pub fn from_env() -> Self {
        // 1. Check for JWT HMAC secret
        if let Ok(secret) = std::env::var("NEONDB_JWT_SECRET") {
            if !secret.is_empty() {
                return AuthValidator {
                    mode: AuthMode::JwtHmac {
                        secret,
                        algorithm: Algorithm::HS256,
                    },
                };
            }
        }

        // 2. Check for JWT RSA public key (inline PEM)
        if let Ok(pem) = std::env::var("NEONDB_JWT_PUBLIC_KEY") {
            if !pem.is_empty() {
                return AuthValidator {
                    mode: AuthMode::JwtRsa {
                        public_key_pem: pem,
                        algorithm: Algorithm::RS256,
                    },
                };
            }
        }

        // 2b. Check for JWT RSA public key file path
        if let Ok(path) = std::env::var("NEONDB_JWT_PUBLIC_KEY_FILE") {
            if !path.is_empty() {
                match std::fs::read_to_string(&path) {
                    Ok(pem) => {
                        return AuthValidator {
                            mode: AuthMode::JwtRsa {
                                public_key_pem: pem,
                                algorithm: Algorithm::RS256,
                            },
                        };
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to read JWT public key file '{}': {}. Falling back.",
                            path,
                            e
                        );
                    }
                }
            }
        }

        // 3. Check for static API key
        if let Ok(key) = std::env::var("NEONDB_API_KEY") {
            if !key.is_empty() {
                return AuthValidator {
                    mode: AuthMode::ApiKey(key),
                };
            }
        }

        // 4. No auth configured — dev mode
        AuthValidator {
            mode: AuthMode::None,
        }
    }

    /// Returns a reference to the current auth mode.
    pub fn mode(&self) -> &AuthMode {
        &self.mode
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Ed25519 identity system
// ─────────────────────────────────────────────────────────────────────────────

/// JWT claims for the Ed25519 identity system.
///
/// Unlike `NeonDBClaims` (which is for backwards-compat HMAC/RSA tokens),
/// `NeonClaims` carries a `roles` *list* so a single token can hold multiple
/// roles simultaneously.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NeonClaims {
    /// Subject — identity / user id.
    pub sub: String,
    /// List of roles the identity holds.
    pub roles: Vec<String>,
    /// Issued-at timestamp (Unix seconds).
    pub iat: u64,
    /// Expiry timestamp (Unix seconds).
    pub exp: u64,
}

/// Signs and verifies JWTs using an Ed25519 key pair.
///
/// One `IdentityIssuer` is created at server startup.  Its public key is
/// published via `GET /auth/public-key` so clients can verify tokens
/// independently.  Tokens are signed with EdDSA (Algorithm::EdDSA) and
/// `jsonwebtoken` 9's built-in support for that algorithm.
pub struct IdentityIssuer {
    pub(crate) signing_key: SigningKey,
    /// Short hex string derived from the first 8 bytes of the public key.
    /// Useful for key rotation logging / debugging.
    pub kid: String,
}

impl IdentityIssuer {
    /// Generate a fresh Ed25519 key pair.
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let kid = Self::compute_kid(&signing_key);
        IdentityIssuer { signing_key, kid }
    }

    /// Load from a PKCS8 PEM-encoded private key string.
    pub fn from_pkcs8_pem(pem: &str) -> crate::error::Result<Self> {
        let signing_key = SigningKey::from_pkcs8_pem(pem)
            .map_err(|e| crate::error::NeonDBError::internal(format!("Ed25519 key parse error: {}", e)))?;
        let kid = Self::compute_kid(&signing_key);
        Ok(IdentityIssuer { signing_key, kid })
    }

    /// Export the public key as a PKCS8 PEM string (for distribution to clients).
    pub fn public_key_pem(&self) -> String {
        // EncodePublicKey is a trait from pkcs8::spki.
        use pkcs8::EncodePublicKey as _;
        self.signing_key
            .verifying_key()
            .to_public_key_pem(LineEnding::LF)
            .unwrap_or_default()
    }

    /// Issue a signed JWT for the given identity and roles.
    ///
    /// `ttl_secs` controls how long the token is valid (from now).
    /// A `ttl_secs` of 0 sets `exp` to `u64::MAX / 2` (effectively unlimited).
    pub fn issue(&self, identity: &str, roles: Vec<String>, ttl_secs: u64) -> crate::error::Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Year 9999 in Unix seconds: safe upper bound (fits in i64/jsonwebtoken)
        let exp = if ttl_secs == 0 { 253_402_300_799_u64 } else { now + ttl_secs };

        let claims = NeonClaims { sub: identity.to_string(), roles, iat: now, exp };

        // jsonwebtoken 9 requires the PEM bytes for EdDSA.
        let pem = self
            .signing_key
            .to_pkcs8_pem(LineEnding::LF)
            .map_err(|e| crate::error::NeonDBError::internal(format!("PKCS8 encode error: {}", e)))?;
        let key = EncodingKey::from_ed_pem(pem.as_bytes())
            .map_err(|e| crate::error::NeonDBError::internal(format!("JWT encoding key error: {}", e)))?;

        encode(&Header::new(Algorithm::EdDSA), &claims, &key)
            .map_err(|e| crate::error::NeonDBError::internal(format!("JWT sign error: {}", e)))
    }

    /// Verify a JWT, return the claims if valid.
    ///
    /// Returns an error if the token is expired, tampered, or signed by a
    /// different key.
    pub fn verify(&self, token: &str) -> crate::error::Result<NeonClaims> {
        let pub_pem = self.public_key_pem();
        let key = DecodingKey::from_ed_pem(pub_pem.as_bytes())
            .map_err(|e| crate::error::NeonDBError::internal(format!("JWT decoding key error: {}", e)))?;

        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.validate_aud = false;
        validation.validate_exp = true;
        // jsonwebtoken uses the numeric `exp` field, but we need to ensure it
        // treats the claim as seconds since UNIX_EPOCH (the default).
        // No leeway — strict expiry.
        validation.leeway = 0;

        decode::<NeonClaims>(token, &key, &validation)
            .map(|data| data.claims)
            .map_err(|e| crate::error::NeonDBError::internal(format!("JWT verify error: {}", e)))
    }

    /// Save the private key to a PKCS8 PEM file.
    pub fn save_to_file(&self, path: &std::path::Path) -> crate::error::Result<()> {
        let pem = self
            .signing_key
            .to_pkcs8_pem(LineEnding::LF)
            .map_err(|e| crate::error::NeonDBError::internal(format!("PKCS8 encode error: {}", e)))?;
        std::fs::write(path, pem.as_bytes())
            .map_err(|e| crate::error::NeonDBError::internal(format!("Write key file: {}", e)))?;
        Ok(())
    }

    /// Load from a PKCS8 PEM file.
    pub fn load_from_file(path: &std::path::Path) -> crate::error::Result<Self> {
        let pem = std::fs::read_to_string(path)
            .map_err(|e| crate::error::NeonDBError::internal(format!("Read key file: {}", e)))?;
        Self::from_pkcs8_pem(&pem)
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn compute_kid(signing_key: &SigningKey) -> String {
        let pub_bytes = signing_key.verifying_key().to_bytes();
        // Take first 8 bytes of public key as the key-id fingerprint.
        hex_encode(&pub_bytes[..8])
    }
}

/// Hex-encode a byte slice (lowercase, no separator).
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Guard to serialize tests that mutate process-wide environment variables.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn test_auth_mode_none_returns_anonymous() {
        let validator = AuthValidator::new(AuthMode::None);
        let result = validator.validate("Bearer some-token");
        match result {
            AuthResult::Anonymous => {} // expected
            other => panic!("Expected Anonymous, got {:?}", other),
        }
    }

    #[test]
    fn test_api_key_valid_returns_authenticated() {
        let validator = AuthValidator::new(AuthMode::ApiKey("my-secret-key".into()));
        let result = validator.validate("Bearer my-secret-key");
        match result {
            AuthResult::Authenticated {
                user_id,
                role,
                claims,
            } => {
                assert_eq!(user_id, "api_key_user");
                assert_eq!(role, "default");
                assert_eq!(claims.sub, "api_key_user");
                assert_eq!(claims.role, "default");
            }
            other => panic!("Expected Authenticated, got {:?}", other),
        }
    }

    #[test]
    fn test_api_key_invalid_returns_denied() {
        let validator = AuthValidator::new(AuthMode::ApiKey("correct-key".into()));
        let result = validator.validate("Bearer wrong-key");
        match result {
            AuthResult::Denied(reason) => {
                assert!(reason.contains("Invalid API key"), "Got: {}", reason);
            }
            other => panic!("Expected Denied, got {:?}", other),
        }
    }

    #[test]
    fn test_api_key_with_role_suffix() {
        let validator = AuthValidator::new(AuthMode::ApiKey("my-key".into()));
        let result = validator.validate("Bearer my-key:admin");
        match result {
            AuthResult::Authenticated {
                user_id,
                role,
                claims,
            } => {
                assert_eq!(user_id, "api_key_user");
                assert_eq!(role, "admin");
                assert_eq!(claims.role, "admin");
            }
            other => panic!("Expected Authenticated, got {:?}", other),
        }
    }

    #[test]
    fn test_jwt_hmac_valid_token() {
        let secret = "test-secret-key-256-bits-long!!!";
        let validator = AuthValidator::new(AuthMode::JwtHmac {
            secret: secret.into(),
            algorithm: Algorithm::HS256,
        });

        // Generate a valid token
        let token = validator.generate_token("player42", "player", 3600).unwrap();
        let result = validator.validate(&format!("Bearer {}", token));

        match result {
            AuthResult::Authenticated {
                user_id,
                role,
                claims,
            } => {
                assert_eq!(user_id, "player42");
                assert_eq!(role, "player");
                assert_eq!(claims.sub, "player42");
                assert!(claims.iat > 0);
                assert!(claims.exp > claims.iat);
            }
            other => panic!("Expected Authenticated, got {:?}", other),
        }
    }

    #[test]
    fn test_jwt_hmac_expired_token_denied() {
        let secret = "test-secret-key-256-bits-long!!!";
        let algorithm = Algorithm::HS256;

        // Manually create an already-expired token
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let claims = NeonDBClaims {
            sub: "expired_user".into(),
            role: "player".into(),
            iat: now - 7200,  // issued 2 hours ago
            exp: now - 3600,  // expired 1 hour ago
            metadata: None,
        };

        let token = encode(
            &Header::new(algorithm),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();

        let validator = AuthValidator::new(AuthMode::JwtHmac {
            secret: secret.into(),
            algorithm,
        });

        let result = validator.validate(&format!("Bearer {}", token));
        match result {
            AuthResult::Denied(reason) => {
                assert!(reason.contains("expired"), "Got: {}", reason);
            }
            other => panic!("Expected Denied (expired), got {:?}", other),
        }
    }

    #[test]
    fn test_jwt_hmac_wrong_secret_denied() {
        let correct_secret = "correct-secret-key!!";
        let wrong_secret = "wrong-secret-key!!!!";

        // Generate token with one secret
        let generator = AuthValidator::new(AuthMode::JwtHmac {
            secret: correct_secret.into(),
            algorithm: Algorithm::HS256,
        });
        let token = generator.generate_token("user1", "player", 3600).unwrap();

        // Validate with a different secret
        let validator = AuthValidator::new(AuthMode::JwtHmac {
            secret: wrong_secret.into(),
            algorithm: Algorithm::HS256,
        });
        let result = validator.validate(&format!("Bearer {}", token));

        match result {
            AuthResult::Denied(reason) => {
                assert!(
                    reason.contains("JWT validation failed"),
                    "Got: {}",
                    reason
                );
            }
            other => panic!("Expected Denied (wrong secret), got {:?}", other),
        }
    }

    #[test]
    fn test_jwt_hmac_malformed_token_denied() {
        let validator = AuthValidator::new(AuthMode::JwtHmac {
            secret: "some-secret".into(),
            algorithm: Algorithm::HS256,
        });

        let result = validator.validate("Bearer not.a.valid.jwt.token");
        match result {
            AuthResult::Denied(reason) => {
                assert!(
                    reason.contains("JWT validation failed"),
                    "Got: {}",
                    reason
                );
            }
            other => panic!("Expected Denied (malformed), got {:?}", other),
        }
    }

    #[test]
    fn test_generate_token_roundtrips() {
        let secret = "roundtrip-secret-key-1234567890";
        let validator = AuthValidator::new(AuthMode::JwtHmac {
            secret: secret.into(),
            algorithm: Algorithm::HS256,
        });

        let token = validator
            .generate_token("alice", "admin", 7200)
            .unwrap();
        let result = validator.validate(&format!("Bearer {}", token));

        match result {
            AuthResult::Authenticated {
                user_id,
                role,
                claims,
            } => {
                assert_eq!(user_id, "alice");
                assert_eq!(role, "admin");
                assert_eq!(claims.sub, "alice");
                assert_eq!(claims.role, "admin");
                assert!(claims.exp > 0);
                assert_eq!(claims.exp - claims.iat, 7200);
            }
            other => panic!("Expected Authenticated, got {:?}", other),
        }
    }

    #[test]
    fn test_generate_token_with_metadata() {
        let secret = "metadata-secret-key-1234567890!";
        let validator = AuthValidator::new(AuthMode::JwtHmac {
            secret: secret.into(),
            algorithm: Algorithm::HS256,
        });

        let metadata = serde_json::json!({
            "guild": "dragons",
            "level": 42,
            "premium": true
        });

        let token = validator
            .generate_token_with_metadata("bob", "player", 3600, metadata.clone())
            .unwrap();

        let result = validator.validate(&format!("Bearer {}", token));
        match result {
            AuthResult::Authenticated { claims, .. } => {
                let meta = claims.metadata.expect("metadata should be present");
                assert_eq!(meta["guild"], "dragons");
                assert_eq!(meta["level"], 42);
                assert_eq!(meta["premium"], true);
            }
            other => panic!("Expected Authenticated, got {:?}", other),
        }
    }

    #[test]
    fn test_from_env_picks_jwt_over_api_key() {
        let _guard = ENV_MUTEX.lock().unwrap();

        // Set both env vars — JWT should win
        std::env::set_var("NEONDB_JWT_SECRET", "jwt-wins-secret");
        std::env::set_var("NEONDB_API_KEY", "api-key-value");

        let validator = AuthValidator::from_env();

        // Clean up immediately
        std::env::remove_var("NEONDB_JWT_SECRET");
        std::env::remove_var("NEONDB_API_KEY");

        match validator.mode() {
            AuthMode::JwtHmac { secret, algorithm } => {
                assert_eq!(secret, "jwt-wins-secret");
                assert!(matches!(algorithm, Algorithm::HS256));
            }
            other => panic!("Expected JwtHmac, got {:?}", other),
        }
    }

    #[test]
    fn test_from_env_falls_back_to_api_key() {
        let _guard = ENV_MUTEX.lock().unwrap();

        // Only set API key, no JWT secret
        std::env::remove_var("NEONDB_JWT_SECRET");
        std::env::remove_var("NEONDB_JWT_PUBLIC_KEY");
        std::env::remove_var("NEONDB_JWT_PUBLIC_KEY_FILE");
        std::env::set_var("NEONDB_API_KEY", "my-fallback-key");

        let validator = AuthValidator::from_env();

        // Clean up
        std::env::remove_var("NEONDB_API_KEY");

        match validator.mode() {
            AuthMode::ApiKey(key) => {
                assert_eq!(key, "my-fallback-key");
            }
            other => panic!("Expected ApiKey, got {:?}", other),
        }
    }

    #[test]
    fn test_claims_deserialize_with_defaults() {
        // Only sub is truly required; everything else has defaults
        let json = r#"{"sub": "minimal_user"}"#;
        let claims: NeonDBClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.sub, "minimal_user");
        assert_eq!(claims.role, ""); // default empty string
        assert_eq!(claims.iat, 0);
        assert_eq!(claims.exp, 0);
        assert!(claims.metadata.is_none());
    }

    #[test]
    fn test_no_bearer_prefix_denied() {
        let validator = AuthValidator::new(AuthMode::JwtHmac {
            secret: "some-secret".into(),
            algorithm: Algorithm::HS256,
        });

        // No "Bearer " prefix
        let result = validator.validate("just-a-raw-token");
        match result {
            AuthResult::Denied(reason) => {
                assert!(reason.contains("Bearer"), "Got: {}", reason);
            }
            other => panic!("Expected Denied (no prefix), got {:?}", other),
        }
    }

    #[test]
    fn test_jwt_no_expiry_token_accepted() {
        let secret = "no-expiry-secret-key-12345678!";
        let validator = AuthValidator::new(AuthMode::JwtHmac {
            secret: secret.into(),
            algorithm: Algorithm::HS256,
        });

        // Generate token with ttl_seconds=0 (no expiry)
        let token = validator.generate_token("immortal", "admin", 0).unwrap();
        let result = validator.validate(&format!("Bearer {}", token));

        match result {
            AuthResult::Authenticated { user_id, claims, .. } => {
                assert_eq!(user_id, "immortal");
                assert_eq!(claims.exp, 0); // no expiry set
            }
            other => panic!("Expected Authenticated, got {:?}", other),
        }
    }

    #[test]
    fn test_generate_token_fails_in_api_key_mode() {
        let validator = AuthValidator::new(AuthMode::ApiKey("key".into()));
        let result = validator.generate_token("user", "role", 3600);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("only works in JwtHmac mode"));
    }

    // ── IdentityIssuer tests (Ed25519 / EdDSA) ───────────────────────────────

    #[test]
    fn test_identity_issuer_generate_creates_valid_issuer() {
        let issuer = super::IdentityIssuer::generate();
        // kid should be 16 hex chars (8 bytes * 2)
        assert_eq!(issuer.kid.len(), 16);
        assert!(issuer.kid.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_identity_issuer_issue_and_verify_roundtrip() {
        let issuer = super::IdentityIssuer::generate();
        let token = issuer.issue("alice", vec!["player".into()], 3600).unwrap();
        let claims = issuer.verify(&token).unwrap();
        assert_eq!(claims.sub, "alice");
        assert_eq!(claims.roles, vec!["player".to_string()]);
        assert!(claims.iat > 0);
        assert!(claims.exp > claims.iat);
    }

    #[test]
    fn test_identity_issuer_expired_token_rejected() {
        let issuer = super::IdentityIssuer::generate();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Manually craft a token with exp in the past by directly constructing
        // NeonClaims and encoding it (bypass the issuer's ttl logic).
        use jsonwebtoken::{encode, Header, Algorithm, EncodingKey};
        use ed25519_dalek::pkcs8::EncodePrivateKey;

        let pem = issuer.signing_key
            .to_pkcs8_pem(LineEnding::LF)
            .unwrap();
        let key = EncodingKey::from_ed_pem(pem.as_bytes()).unwrap();
        let expired_claims = super::NeonClaims {
            sub: "expired".into(),
            roles: vec!["player".into()],
            iat: now - 7200,
            exp: now - 3600, // already expired
        };
        let token = encode(&Header::new(Algorithm::EdDSA), &expired_claims, &key).unwrap();

        let result = issuer.verify(&token);
        assert!(result.is_err(), "Expected error for expired token");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("JWT verify error"), "Got: {}", err);
    }

    #[test]
    fn test_identity_issuer_tampered_token_rejected() {
        let issuer = super::IdentityIssuer::generate();
        let token = issuer.issue("bob", vec!["admin".into()], 3600).unwrap();

        // Flip one character in the signature segment (last dot-separated part).
        let parts: Vec<&str> = token.splitn(3, '.').collect();
        assert_eq!(parts.len(), 3, "JWT should have 3 segments");
        let sig = parts[2].to_string();
        let tampered_sig = {
            let mut chars: Vec<char> = sig.chars().collect();
            // Toggle the first character
            chars[0] = if chars[0] == 'A' { 'B' } else { 'A' };
            chars.iter().collect::<String>()
        };
        let tampered = format!("{}.{}.{}", parts[0], parts[1], tampered_sig);

        let result = issuer.verify(&tampered);
        assert!(result.is_err(), "Expected error for tampered token");
    }

    #[test]
    fn test_identity_issuer_wrong_key_rejected() {
        let issuer_a = super::IdentityIssuer::generate();
        let issuer_b = super::IdentityIssuer::generate();

        // Token issued by A should be rejected by B.
        let token = issuer_a.issue("charlie", vec!["spectator".into()], 3600).unwrap();
        let result = issuer_b.verify(&token);
        assert!(result.is_err(), "Expected error: token from different issuer");
    }

    #[test]
    fn test_identity_issuer_sub_roundtrips() {
        let issuer = super::IdentityIssuer::generate();
        let identity = "user-99999-special_chars-are-fine";
        let token = issuer.issue(identity, vec![], 3600).unwrap();
        let claims = issuer.verify(&token).unwrap();
        assert_eq!(claims.sub, identity);
    }

    #[test]
    fn test_identity_issuer_roles_roundtrip_multiple() {
        let issuer = super::IdentityIssuer::generate();
        let roles = vec!["admin".to_string(), "moderator".to_string(), "player".to_string()];
        let token = issuer.issue("dave", roles.clone(), 3600).unwrap();
        let claims = issuer.verify(&token).unwrap();
        assert_eq!(claims.roles, roles);
    }

    #[test]
    fn test_identity_issuer_save_and_load_from_file() {
        let issuer = super::IdentityIssuer::generate();
        let dir = std::env::temp_dir();
        let path = dir.join(format!("neondb_test_key_{}.pem", issuer.kid));

        issuer.save_to_file(&path).expect("save_to_file failed");
        let loaded = super::IdentityIssuer::load_from_file(&path).expect("load_from_file failed");

        // Both issuers should produce the same public key and kid.
        assert_eq!(issuer.kid, loaded.kid);
        assert_eq!(issuer.public_key_pem(), loaded.public_key_pem());

        // Token issued by original must be verifiable by loaded.
        let token = issuer.issue("eve", vec!["guest".into()], 3600).unwrap();
        let claims = loaded.verify(&token).unwrap();
        assert_eq!(claims.sub, "eve");

        let _ = std::fs::remove_file(&path); // cleanup
    }

    #[test]
    fn test_identity_issuer_public_key_pem_format() {
        let issuer = super::IdentityIssuer::generate();
        let pem = issuer.public_key_pem();
        assert!(
            pem.starts_with("-----BEGIN PUBLIC KEY-----"),
            "Expected PKCS8 SubjectPublicKeyInfo PEM, got: {}",
            &pem[..pem.len().min(50)]
        );
        assert!(pem.contains("-----END PUBLIC KEY-----"));
    }

    #[test]
    fn test_identity_issuer_from_pkcs8_pem_invalid_returns_error() {
        let result = super::IdentityIssuer::from_pkcs8_pem("not a pem at all");
        assert!(result.is_err(), "Expected error for invalid PEM");
    }
}
