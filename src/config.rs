use serde::Deserialize;
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
}

// These structs mirror the TOML schema. Fields that are not yet wired into
// Config are kept for forward-compatibility and suppressed individually.
#[derive(Deserialize)]
struct ConfigFile {
    #[allow(dead_code)]
    project: Option<ConfigProject>,
    server: Option<ConfigServer>,
    scheduler: Option<Vec<ConfigScheduler>>,
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
            max_connections: 100,
            reducer_timeout_ms: 5_000,
            api_key: None,
            tune_system: false,
            reuse_port: true,
            two_frame_protocol: false,
            snapshot_interval: 1_000_000,
            snapshot_dir: env::temp_dir().join("neondb_snapshots"),
            scheduled_reducers: vec![],
        };

        if let Some(toml_path) = find_config_in_cwd() {
            if let Ok(contents) = fs::read_to_string(&toml_path) {
                if let Ok(parsed) = toml::from_str::<ConfigFile>(&contents) {
                    apply_server_section(&mut cfg, parsed.server);
                    apply_scheduler_section(&mut cfg, parsed.scheduler);
                }
            }
        }

        apply_env_overrides(&mut cfg);
        cfg
    }

    pub fn load_from_path(path: &Path) -> Option<Self> {
        let contents = fs::read_to_string(path).ok()?;
        let parsed: ConfigFile = toml::from_str(&contents).ok()?;
        let mut cfg = Config::from_env();
        apply_server_section(&mut cfg, parsed.server);
        apply_scheduler_section(&mut cfg, parsed.scheduler);
        Some(cfg)
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
}
