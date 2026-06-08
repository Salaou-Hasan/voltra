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
pub enum AuthMode {
    /// No authentication required (dev mode).
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

impl Default for AuthMode {
    fn default() -> Self {
        AuthMode::None
    }
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
}
