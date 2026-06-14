// ============================================================================
// NeonDB main.rs — Session 32
//
// Fixes:
//   [BUG-1] cmd_seed dry-run format string produced malformed output.
//           Fixed in cli.rs: split into two separate println! calls.
//
//   [BUG-2] POST /cluster/call created a brand-new ReducerRegistry per
//           request.  Fixed by threading the startup Arc<ReducerRegistry>
//           through start_metrics_server → handle_metrics_request.
//
//   [BUG-3] E0529: `match peers { None | Some([]) => ...}` tried to use a
//           slice pattern against Option<&Vec<_>>.  Fixed by converting with
//           `.map(|p| p.as_slice())` before the match, yielding Option<&[_]>.
//
//   [FEAT]  POST /cluster/join — dynamic peer seeding.
//           A new node can POST its NodeInfo to any existing peer's
//           /cluster/join endpoint to register itself.  The peer adds
//           the caller to its live peer table and returns its full peer
//           list so the joiner can bootstrap without knowing every node
//           in advance.
// ============================================================================

// ── Hardware-level allocator ──────────────────────────────────────────────────
// mimalloc replaces the system allocator with a high-throughput allocator
// tuned for many small short-lived allocations (DashMap ops, channel messages).
// Huge pages: set MIMALLOC_LARGE_OS_PAGES=1 in the environment for 2MB pages.
// NUMA-aware: set MIMALLOC_NUMA_AWARE=1 for multi-socket servers.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
use dialoguer::{Input, Select, theme::ColorfulTheme};
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use neondb::{
    auth::{AuthValidator, IdentityIssuer},
    config::{Config, ScheduledReducerConfig},
    error::Result,
    metrics::Metrics,
    network::{start_listener, PendingCall, RateLimiterConfig, RateLimiterRegistry, ReducerResponse},
    presence::PresenceManager,
    reducer::{ReducerContext, ReducerRegistry},
    subscriptions::SubscriptionManager,
    table::TableStore,
    ttl::TtlManager,
    wal::{
        snapshot::{find_latest_snapshot, load_snapshot, save_snapshot},
        BatchedWalWriter, WalEntry, WalReader,
    },
};
use rmp_serde;
use tokio::sync::watch;

// ─────────────────────────────────────────────────────────────────────────────
// Template registry
// ─────────────────────────────────────────────────────────────────────────────

struct Template {
    name:        &'static str,
    category:    &'static str,
    description: &'static str,
}

const TEMPLATES: &[Template] = &[
    Template { name: "game/basic", category: "Game server", description: "Spawn, move, despawn, health — the minimal multiplayer foundation. Add modules with `neondb add`." },
    Template { name: "game/full",  category: "Game server", description: "All modules pre-configured: combat, inventory, economy, matchmaking, guilds, quests, leaderboard, chat, world." },
    Template { name: "game/unity", category: "Unity",       description: "Unity C# SDK + full game server. Copy unity/ into Assets/Scripts/NeonDB/, configure URL, play." },
    Template { name: "game/godot", category: "Godot 4",     description: "Godot GDScript SDK + full game server. Add godot/ as an autoload, configure URL, play." },
];

/// Available add-on modules (`neondb add <name>`).
const MODULES: &[(&str, &str)] = &[
    ("chat",        "Rooms, messages, per-room presence"),
    ("inventory",   "Items, qty stacking, equip slots"),
    ("leaderboard", "Score submit, global top-N, weekly reset"),
    ("matchmaking", "Queue, ELO-pair, match creation (scheduled)"),
    ("guilds",      "Create, invite, accept, kick"),
    ("quests",      "Accept, progress tracking, claim reward"),
    ("economy",     "Gold/gem wallets, shop buy/sell, transfers, loot boxes"),
    ("combat",      "Attack, ability system, NPC damage, respawn"),
    ("world",       "World tick, NPC spawn, session cleanup (scheduled)"),
];

// ─────────────────────────────────────────────────────────────────────────────
// CLI definition
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "neondb")]
#[command(author, version = "1", about = "NeonDB — self-hosted real-time game backend")]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Scaffold a new NeonDB multiplayer game project
    Init {
        #[arg(value_name = "NAME")]
        path: Option<PathBuf>,
        #[arg(long, help = "Template: game/basic | game/full | game/unity | game/godot")]
        template: Option<String>,
    },
    /// Add a feature module to an existing project (run inside project dir)
    Add {
        #[arg(value_name = "MODULE", help = "chat | inventory | leaderboard | matchmaking | guilds | quests | economy | combat | world")]
        module: String,
    },
    /// Check for and install updates to all NeonDB binaries
    Update {
        #[arg(long, help = "Only check — do not download")]
        check: bool,
    },
    /// List available project templates
    Templates,
    /// List available add-on modules (`neondb add <module>`)
    Modules,
    /// Compile JS reducers in modules/ to WASM (requires `javy`)
    Build {
        #[arg(short = 'm', long, default_value = "modules")]
        modules_dir: Option<PathBuf>,
    },
    /// Start the NeonDB server
    Start {
        #[arg(short = 'a', long)] host: Option<String>,
        #[arg(short = 'p', long)] port: Option<u16>,
        #[arg(short = 'd', long)] data_dir: Option<PathBuf>,
        #[arg(long = "wal-path")] wal_path: Option<PathBuf>,
        #[arg(short = 'f', long)] fsync_interval_ms: Option<u32>,
    },
    /// Show server status and metrics
    Status {
        #[arg(long, default_value = "http://127.0.0.1:3001")] metrics_url: String,
    },
    /// List all tables and their row counts
    Tables {
        #[arg(long, default_value = "http://127.0.0.1:3001")] metrics_url: String,
    },
    /// Read rows from a table
    Get {
        table: String,
        key: Option<String>,
        #[arg(long, default_value = "http://127.0.0.1:3001")] metrics_url: String,
    },
    /// Call a reducer once and print the result
    Call {
        reducer: String,
        #[arg(help = "JSON args array, e.g. '[\"alice\", 5]'")] args: Option<String>,
        #[arg(long, default_value = "ws://127.0.0.1:3000")] url: String,
        #[arg(long)] api_key: Option<String>,
    },
    /// Subscribe to a table and stream live updates (Ctrl-C to stop)
    Watch {
        query: String,
        #[arg(long, default_value = "ws://127.0.0.1:3000")] url: String,
        #[arg(long)] api_key: Option<String>,
    },
    /// Show status of all cluster peers
    ClusterStatus {
        #[arg(long, default_value = "http://127.0.0.1:3001")] metrics_url: String,
    },
    /// Bulk-seed rows into a running server from a JSON file
    Seed {
        #[arg(value_name = "FILE", help = "Path to seed JSON file")]
        file: String,
        #[arg(long, default_value = "http://127.0.0.1:3001", help = "Admin/metrics server URL")]
        metrics_url: String,
        #[arg(long, help = "Parse and preview what would be seeded without writing")]
        dry_run: bool,
    },
    /// Put the server into drain mode — stop accepting new connections while
    /// existing connections finish. Safe to hot-fix then undrain or restart.
    Drain {
        #[arg(long, default_value = "http://127.0.0.1:3001", help = "Admin/metrics server URL")]
        metrics_url: String,
    },
    /// Take the server out of drain mode — resume accepting new connections.
    Undrain {
        #[arg(long, default_value = "http://127.0.0.1:3001", help = "Admin/metrics server URL")]
        metrics_url: String,
    },
    /// Apply pending schema migrations from the migrations/ directory
    Migrate {
        #[arg(value_name = "DIR", default_value = "migrations", help = "Path to migrations directory")]
        dir: String,
        #[arg(long, default_value = "http://127.0.0.1:3001", help = "Admin/metrics server URL")]
        metrics_url: String,
        #[arg(long, help = "Preview what would be applied without writing")]
        dry_run: bool,
    },
    /// AI-generate an NPC template and cache it in the running server
    GenerateNpc {
        #[arg(value_name = "NPC_TYPE", help = "e.g. goblin, dragon, shadow_assassin")]
        npc_type: String,
        #[arg(long, help = "Extra context for the AI, e.g. 'volcanic dungeon boss'")]
        context: Option<String>,
        #[arg(long, default_value = "ws://127.0.0.1:3000")] url: String,
        #[arg(long)] api_key: Option<String>,
    },
    /// Run a WebSocket throughput benchmark against a running server
    Bench {
        #[arg(long, default_value = "ws://127.0.0.1:3000")] url: String,
        #[arg(short = 'c', long, default_value = "10")] clients: usize,
        #[arg(short = 'n', long, default_value = "500")] calls: usize,
        #[arg(long, default_value = "50")] warmup: usize,
        #[arg(long)] api_key: Option<String>,
    },
    /// Trigger an immediate backup on a running server
    Backup {
        #[arg(long, default_value = "http://127.0.0.1:3001")] metrics_url: String,
    },
    /// List backups in a backup directory
    Backups {
        #[arg(value_name = "DIR", help = "Backup directory")]
        dir: PathBuf,
    },
    /// Restore a backup into live data dirs (server must be STOPPED)
    Restore {
        #[arg(value_name = "BACKUP", help = "Path to a backup_<ts>_<seq> directory")]
        backup: PathBuf,
        #[arg(long = "wal-path", help = "Live WAL file path to restore into")]
        wal_path: PathBuf,
        #[arg(long = "snapshot-dir", help = "Live snapshot directory to restore into")]
        snapshot_dir: PathBuf,
        #[arg(long = "until-ts", help = "Point-in-time cutoff (unix NANOSECONDS); WAL entries after this are dropped")]
        until_ts: Option<u64>,
    },
    /// Promote a replica to primary (failover)
    Promote {
        #[arg(long, default_value = "http://127.0.0.1:3001")] metrics_url: String,
    },
    /// Generate typed client code from the running server's schema
    ///
    /// Examples:
    ///   neondb generate --lang typescript --out ./client/src/generated
    ///   neondb generate --lang gdscript  --out ./godot/addons/neondb/generated
    Generate {
        /// Target language: typescript, gdscript
        #[arg(long, default_value = "typescript")]
        lang: String,
        /// Output directory for generated files (created if absent)
        #[arg(long, short = 'o', default_value = ".")]
        out: PathBuf,
        /// Admin/metrics server URL to read the schema from
        #[arg(long, default_value = "http://127.0.0.1:3001")]
        metrics_url: String,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Init { path, template } => { init_project(path, template)?; Ok(()) }
        Commands::Add { module } => { cmd_add_module(&module, &std::env::current_dir()?)?; Ok(()) }
        Commands::Update { check } => { neondb::updater::cmd_update(check) }
        Commands::Templates => { cmd_list_templates(); Ok(()) }
        Commands::Modules => { cmd_list_modules(); Ok(()) }
        Commands::Build { modules_dir } => {
            build_wasm_modules(modules_dir.as_deref().unwrap_or(Path::new("modules")))
        }
        Commands::Start { host, port, data_dir, wal_path, fsync_interval_ms } => {
            // If run from inside a scaffolded game project, build + exec that binary
            let cwd = std::env::current_dir()?;
            if let Some(pkg_name) = is_game_project(&cwd) {
                return cmd_start_project(&cwd, &pkg_name).map_err(Into::into);
            }
            // Non-blocking background version hint — prints one line if behind
            std::thread::spawn(neondb::updater::check_and_hint);
            let mut config = Config::from_env();
            if let Some(h) = host { config.host = h; }
            if let Some(p) = port { config.port = p; }
            if let Some(d) = data_dir { config.wal_path = d.join("neondb.wal"); }
            if let Some(w) = wal_path { config.wal_path = w; }
            if let Some(f) = fsync_interval_ms { config.fsync_interval_ms = f; }
            run_server(config).await
        }
        Commands::Status { metrics_url } => neondb::cli::cmd_status(&metrics_url).await,
        Commands::Tables { metrics_url } => neondb::cli::cmd_tables(&metrics_url).await,
        Commands::Get { table, key, metrics_url } => neondb::cli::cmd_get(&metrics_url, &table, key.as_deref()).await,
        Commands::Call { reducer, args, url, api_key } => neondb::cli::cmd_call(&url, &reducer, args.as_deref(), api_key.as_deref()).await,
        Commands::Watch { query, url, api_key } => neondb::cli::cmd_watch(&url, &query, api_key.as_deref()).await,
        Commands::ClusterStatus { metrics_url } => cmd_cluster_status(&metrics_url).await,
        Commands::Seed { file, metrics_url, dry_run } => neondb::cli::cmd_seed(&metrics_url, &file, dry_run).await,
        Commands::Drain { metrics_url } => cmd_drain(&metrics_url, true).await,
        Commands::Undrain { metrics_url } => cmd_drain(&metrics_url, false).await,
        Commands::Migrate { dir, metrics_url, dry_run } => neondb::cli::cmd_migrate(&metrics_url, &dir, dry_run).await,
        Commands::GenerateNpc { npc_type, context, url, api_key } => neondb::cli::cmd_generate_npc(&url, &npc_type, context.as_deref(), api_key.as_deref()).await,
        Commands::Bench { url, clients, calls, warmup, api_key } => run_cli_bench(&url, clients, calls, warmup, api_key.as_deref()).await,
        Commands::Backup { metrics_url } => cmd_backup(&metrics_url).await,
        Commands::Backups { dir } => { cmd_list_backups(&dir); Ok(()) }
        Commands::Restore { backup, wal_path, snapshot_dir, until_ts } => {
            let (seq, n) = neondb::backup::restore_to_dirs(&backup, &wal_path, &snapshot_dir, until_ts)?;
            println!("Restored snapshot seq={} plus {} WAL entries.", seq, n);
            println!("Start the server with --wal-path {:?} to load the restored data.", wal_path);
            Ok(())
        }
        Commands::Promote { metrics_url } => cmd_promote(&metrics_url).await,
        Commands::Generate { lang, out, metrics_url } => cmd_generate(&metrics_url, &lang, &out).await,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// neondb cluster-status
// ─────────────────────────────────────────────────────────────────────────────

async fn cmd_drain(metrics_url: &str, enable: bool) -> Result<()> {
    let url = format!("{}/admin/api/drain", metrics_url);
    let client = reqwest::Client::new();
    let resp = if enable {
        client.post(&url).send().await
    } else {
        client.delete(&url).send().await
    }.map_err(|e| neondb::error::NeonDBError::network_error(format!("Cannot reach {}: {}", url, e)))?;

    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({}));
    let draining = body["draining"].as_bool().unwrap_or(enable);
    let conns = body["active_connections"].as_u64().unwrap_or(0);
    let msg = body["message"].as_str().unwrap_or("");

    if draining {
        println!("⚠  Server is DRAINING — {} active connection(s) still live", conns);
        println!("   {}", msg);
        println!("   Poll GET {}/admin/api/drain until active_connections=0,", metrics_url);
        println!("   then restart / apply fix, then: neondb undrain");
    } else {
        println!("✓  Drain disabled — server accepting connections normally ({} active)", conns);
        println!("   {}", msg);
    }
    Ok(())
}

async fn cmd_backup(metrics_url: &str) -> Result<()> {
    let url = format!("{}/backup", metrics_url);
    let resp = reqwest::Client::new().post(&url).send().await.map_err(|e| {
        neondb::error::NeonDBError::network_error(format!("Cannot reach {}: {}", url, e))
    })?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({}));
    if status.is_success() {
        println!("Backup written: {}", body["path"].as_str().unwrap_or("?"));
        println!("  seq:  {}", body["last_seq"]);
        println!("  rows: {}", body["row_count"]);
    } else {
        eprintln!("Backup failed (HTTP {}): {}", status, body);
        return Err(neondb::error::NeonDBError::internal("backup failed"));
    }
    Ok(())
}

fn cmd_list_backups(dir: &Path) {
    let backups = neondb::backup::list_backups(dir);
    if backups.is_empty() {
        println!("No backups found in {:?}", dir);
        return;
    }
    println!("{:<24} {:>12} {:>10}  PATH", "CREATED", "SEQ", "ROWS");
    for (path, ts, seq) in &backups {
        let rows = neondb::backup::read_meta(path).map(|m| m.row_count).unwrap_or(0);
        let dt = chrono_like_fmt(*ts);
        println!("{:<24} {:>12} {:>10}  {}", dt, seq, rows, path.display());
    }
}

/// Minimal unix-secs → "YYYY-MM-DD HH:MM:SS UTC" formatter (no chrono dep).
fn chrono_like_fmt(unix_secs: u64) -> String {
    let days_in_month = |y: u64, m: u64| -> u64 {
        match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 { 29 } else { 28 },
        }
    };
    let secs = unix_secs % 86_400;
    let mut days = unix_secs / 86_400;
    let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    let mut year = 1970u64;
    loop {
        let yd = if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 { 366 } else { 365 };
        if days < yd { break; }
        days -= yd; year += 1;
    }
    let mut month = 1u64;
    loop {
        let md = days_in_month(year, month);
        if days < md { break; }
        days -= md; month += 1;
    }
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02} UTC", year, month, days + 1, h, m, s)
}

async fn cmd_promote(metrics_url: &str) -> Result<()> {
    let url = format!("{}/replication/promote", metrics_url);
    let resp = reqwest::Client::new().post(&url).send().await.map_err(|e| {
        neondb::error::NeonDBError::network_error(format!("Cannot reach {}: {}", url, e))
    })?;
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({}));
    println!("{}", serde_json::to_string_pretty(&body).unwrap_or_default());
    Ok(())
}

async fn cmd_generate(metrics_url: &str, lang: &str, out: &Path) -> Result<()> {
    // Fetch the full schema from the running server.
    let url = format!("{}/schema", metrics_url);
    let schema: serde_json::Value = reqwest::Client::new()
        .get(&url)
        .send().await
        .map_err(|e| neondb::error::NeonDBError::network_error(format!("Cannot reach {}: {}", url, e)))?
        .json().await
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Invalid schema JSON: {}", e)))?;

    std::fs::create_dir_all(out).map_err(|e| {
        neondb::error::NeonDBError::internal(format!("Cannot create output dir: {}", e))
    })?;

    let tables = schema["tables"].as_object().cloned().unwrap_or_default();
    let reducers = schema["reducers"].as_array().cloned().unwrap_or_default();
    let version = schema["version"].as_str().unwrap_or("?");

    match lang {
        "typescript" | "ts" => {
            generate_typescript(&tables, &reducers, version, out)?;
        }
        "gdscript" | "godot" => {
            generate_gdscript(&tables, &reducers, version, out)?;
        }
        other => {
            return Err(neondb::error::NeonDBError::invalid_argument(
                format!("Unknown --lang '{}'. Supported: typescript, gdscript", other)
            ));
        }
    }
    Ok(())
}

fn col_type_to_ts(type_str: &str) -> &'static str {
    match type_str.to_lowercase().as_str() {
        "string" | "str" | "text" => "string",
        "i64" | "i32" | "int" | "integer" | "number" => "number",
        "f64" | "f32" | "float" | "double" => "number",
        "bool" | "boolean" => "boolean",
        "bytes" | "blob" => "Uint8Array",
        _ => "unknown",
    }
}

fn col_type_to_gd(type_str: &str) -> &'static str {
    match type_str.to_lowercase().as_str() {
        "string" | "str" | "text" => "String",
        "i64" | "i32" | "int" | "integer" | "number" => "int",
        "f64" | "f32" | "float" | "double" => "float",
        "bool" | "boolean" => "bool",
        "bytes" | "blob" => "PackedByteArray",
        _ => "Variant",
    }
}

fn snake_to_pascal(s: &str) -> String {
    s.split('_').map(|w| {
        let mut c = w.chars();
        match c.next() {
            None => String::new(),
            Some(f) => f.to_uppercase().to_string() + c.as_str(),
        }
    }).collect()
}

fn generate_typescript(
    tables: &serde_json::Map<String, serde_json::Value>,
    reducers: &[serde_json::Value],
    version: &str,
    out: &Path,
) -> Result<()> {
    // ── tables.ts ─────────────────────────────────────────────────────────────
    let mut tables_ts = format!(
        "// tables.ts — AUTO-GENERATED by `neondb generate` from server v{}\n// DO NOT EDIT — run `neondb generate` to regenerate\n\n",
        version
    );
    for (table_name, schema) in tables {
        let pascal = snake_to_pascal(table_name);
        tables_ts.push_str(&format!("export interface {} {{\n", pascal));
        if let Some(cols) = schema["columns"].as_array() {
            for col in cols {
                let name = col["name"].as_str().unwrap_or("_");
                let type_str = col["type"].as_str().unwrap_or("any");
                let required = col["required"].as_bool().unwrap_or(true);
                let ts_type = col_type_to_ts(type_str);
                let opt = if required { "" } else { "?" };
                tables_ts.push_str(&format!("  {}{}: {};\n", name, opt, ts_type));
            }
        } else {
            tables_ts.push_str("  [key: string]: unknown;\n");
        }
        tables_ts.push_str("}\n\n");
    }

    // ── reducers.ts ───────────────────────────────────────────────────────────
    let mut reducers_ts = format!(
        "// reducers.ts — AUTO-GENERATED by `neondb generate` from server v{}\n// DO NOT EDIT — run `neondb generate` to regenerate\n\nimport type {{ NeonDBClient }} from 'neondb-client';\n\nexport const Reducers = {{\n",
        version
    );
    for r in reducers {
        let name = match r.as_str() { Some(s) => s, None => continue };
        let camel: String = {
            let mut parts = name.split('_');
            let first = parts.next().unwrap_or("");
            let rest: String = parts.map(|w| {
                let mut c = w.chars();
                match c.next() { None => String::new(), Some(f) => f.to_uppercase().to_string() + c.as_str() }
            }).collect();
            format!("{}{}", first, rest)
        };
        reducers_ts.push_str(&format!(
            "  {}: (db: NeonDBClient, ...args: unknown[]) => db.call('{}', args),\n",
            camel, name
        ));
    }
    reducers_ts.push_str("};\n");

    write_generated(out, "tables.ts", &tables_ts)?;
    write_generated(out, "reducers.ts", &reducers_ts)?;
    println!("TypeScript: wrote {}/tables.ts and {}/reducers.ts", out.display(), out.display());
    println!("  {} table type(s), {} reducer(s)", tables.len(), reducers.len());
    Ok(())
}

fn generate_gdscript(
    tables: &serde_json::Map<String, serde_json::Value>,
    reducers: &[serde_json::Value],
    version: &str,
    out: &Path,
) -> Result<()> {
    // ── tables.gd ─────────────────────────────────────────────────────────────
    let mut tables_gd = format!(
        "# tables.gd — AUTO-GENERATED by `neondb generate` from server v{}\n# DO NOT EDIT — run `neondb generate` to regenerate\n\n",
        version
    );
    for (table_name, schema) in tables {
        let pascal = snake_to_pascal(table_name);
        tables_gd.push_str(&format!("class {}:\n", pascal));
        if let Some(cols) = schema["columns"].as_array() {
            if cols.is_empty() {
                tables_gd.push_str("\tpass\n\n");
                continue;
            }
            for col in cols {
                let name = col["name"].as_str().unwrap_or("_");
                let type_str = col["type"].as_str().unwrap_or("any");
                let gd_type = col_type_to_gd(type_str);
                tables_gd.push_str(&format!("\tvar {}: {}\n", name, gd_type));
            }
        } else {
            tables_gd.push_str("\tpass\n");
        }
        tables_gd.push('\n');
    }

    // ── reducers.gd ───────────────────────────────────────────────────────────
    let mut reducers_gd = format!(
        "# reducers.gd — AUTO-GENERATED by `neondb generate` from server v{}\n# DO NOT EDIT — run `neondb generate` to regenerate\n\nclass_name NeonDBReducers\n\n",
        version
    );
    for r in reducers {
        let name = match r.as_str() { Some(s) => s, None => continue };
        reducers_gd.push_str(&format!(
            "static func {}(db, args: Array = []):\n\treturn await db.call_reducer(\"{}\", args)\n\n",
            name, name
        ));
    }

    write_generated(out, "tables.gd", &tables_gd)?;
    write_generated(out, "reducers.gd", &reducers_gd)?;
    println!("GDScript: wrote {}/tables.gd and {}/reducers.gd", out.display(), out.display());
    println!("  {} table class(es), {} reducer(s)", tables.len(), reducers.len());
    Ok(())
}

fn write_generated(out: &Path, filename: &str, content: &str) -> Result<()> {
    let path = out.join(filename);
    std::fs::write(&path, content).map_err(|e| {
        neondb::error::NeonDBError::internal(format!("Cannot write {}: {}", path.display(), e))
    })
}

async fn cmd_cluster_status(metrics_url: &str) -> Result<()> {
    let url = format!("{}/cluster/peers", metrics_url);
    let resp = reqwest::get(&url).await.map_err(|e| {
        neondb::error::NeonDBError::network_error(format!("Cannot reach {}: {}", url, e))
    })?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eprintln!("Server returned HTTP {}: {}", status, body);
        return Err(neondb::error::NeonDBError::network_error(format!("HTTP {}", status)));
    }

    let data: serde_json::Value = resp.json().await.map_err(|e| {
        neondb::error::NeonDBError::internal(format!("Invalid JSON response: {}", e))
    })?;

    let my_shard    = data["my_shard_id"].as_u64().unwrap_or(0);
    let shard_count = data["shard_count"].as_u64().unwrap_or(1);
    let enabled     = data["cluster_enabled"].as_bool().unwrap_or(false);

    println!();
    if !enabled {
        println!("  Cluster: single-node mode");
        println!("  Shard:   {}/{}", my_shard, shard_count);
        println!();
        println!("  To enable clustering, set NEONDB_PEERS before starting:");
        println!("    NEONDB_PEERS=shard1=http://node2:3001,shard2=http://node3:3001");
        println!();
        println!("  Or dynamically join a running cluster:");
        println!("    NEONDB_SEED_NODE=http://existing-node:3001 neondb start");
        println!();
        return Ok(());
    }

    println!("  Cluster status  (queried shard {})", my_shard);
    println!("  Shard count: {}", shard_count);
    println!();

    let peers = data["peers"].as_array();
    // BUG-3 FIX: convert Option<&Vec<_>> → Option<&[_]> via .map(|p| p.as_slice())
    // so that the empty-slice pattern Some([]) compiles correctly.
    match peers.map(|p| p.as_slice()) {
        None | Some([]) => {
            println!("  No peers registered.");
        }
        Some(peers) => {
            println!("  {:<8}  {:<38}  {}", "Shard", "Metrics URL", "Health");
            println!("  {}", "─".repeat(62));
            for peer in peers {
                let shard_id   = peer["shard_id"].as_u64().unwrap_or(0);
                let url_str    = peer["metrics_url"].as_str().unwrap_or("?");
                let healthy    = peer["healthy"].as_bool().unwrap_or(false);
                let health_str = if healthy { "✓ healthy" } else { "✗ unreachable" };
                println!("  {:<8}  {:<38}  {}", shard_id, url_str, health_str);
            }
        }
    }
    println!();
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// neondb start — project-aware: if CWD is a scaffolded game project, build + run it
// ─────────────────────────────────────────────────────────────────────────────

fn is_game_project(cwd: &Path) -> Option<String> {
    let cargo_path = cwd.join("Cargo.toml");
    if !cargo_path.exists() { return None; }
    let content = std::fs::read_to_string(&cargo_path).ok()?;
    // Must have neondb as a dep but not BE neondb itself
    if !content.contains("neondb") { return None; }
    if content.contains("name = \"neondb\"") { return None; }
    // Extract package name
    content.lines()
        .find(|l| l.trim_start().starts_with("name") && l.contains('"'))
        .and_then(|l| l.split('"').nth(1))
        .map(|s| s.to_string())
}

fn cmd_start_project(cwd: &Path, pkg_name: &str) -> Result<()> {
    println!("[neondb] Building {} (release)…", pkg_name);
    let build = std::process::Command::new("cargo")
        .arg("build")
        .arg("--release")
        .current_dir(cwd)
        .status()
        .map_err(|e| neondb::error::NeonDBError::internal(format!("cargo build: {e}")))?;

    if !build.success() {
        return Err(neondb::error::NeonDBError::internal("cargo build --release failed"));
    }

    let bin_name = if cfg!(windows) {
        format!("{pkg_name}.exe")
    } else {
        pkg_name.to_string()
    };
    let bin = cwd.join("target").join("release").join(&bin_name);
    if !bin.exists() {
        return Err(neondb::error::NeonDBError::internal(
            format!("Binary not found at {}", bin.display()),
        ));
    }

    println!("[neondb] Starting {}…", pkg_name);
    let status = std::process::Command::new(&bin)
        .arg("start")
        .current_dir(cwd)
        .status()
        .map_err(|e| neondb::error::NeonDBError::internal(format!("exec {pkg_name}: {e}")))?;

    if status.success() {
        Ok(())
    } else {
        Err(neondb::error::NeonDBError::internal(format!("{pkg_name} exited with non-zero status")))
    }
}

// neondb templates
// ─────────────────────────────────────────────────────────────────────────────

fn cmd_list_templates() {
    println!();
    println!("  NeonDB Game Templates");
    println!();
    for t in TEMPLATES {
        println!("  {:14} — {}", t.name, t.description);
    }
    println!();
    println!("  Usage:");
    println!("    neondb init my-game --template game/basic");
    println!("    neondb init my-game --template game/full");
    println!("    neondb init my-game --template game/unity");
    println!("    neondb init my-game --template game/godot");
    println!();
    println!("  Add modules later:");
    println!("    cd my-game && neondb add combat");
    println!("    cd my-game && neondb add leaderboard");
    println!();
}

fn cmd_list_modules() {
    println!();
    println!("  NeonDB Add-on Modules  (run inside your project: neondb add <module>)");
    println!();
    for (name, desc) in MODULES {
        println!("  {:14} — {}", name, desc);
    }
    println!();
    println!("  Example:");
    println!("    cd my-game");
    println!("    neondb add combat       # adds attack, respawn, ability reducers + schema");
    println!("    neondb add leaderboard  # adds lb_submit, lb_reset reducers + schema");
    println!();
}

// ─────────────────────────────────────────────────────────────────────────────
// neondb init  (interactive when called with no args)
// ─────────────────────────────────────────────────────────────────────────────

fn init_project(path: Option<PathBuf>, template: Option<String>) -> Result<()> {
    let theme = ColorfulTheme::default();

    let project_name: String = match &path {
        Some(p) => p.file_name().and_then(|n| n.to_str()).unwrap_or("my-project").to_string(),
        None => Input::with_theme(&theme)
            .with_prompt("Project name")
            .default("my-project".to_string())
            .interact_text()
            .map_err(|e| neondb::error::NeonDBError::internal(format!("Prompt error: {}", e)))?,
    };

    let project_path: PathBuf = match path {
        Some(p) => p,
        None => {
            let suggested = format!("./{}", project_name);
            let input: String = Input::with_theme(&theme)
                .with_prompt("Project path")
                .default(suggested)
                .interact_text()
                .map_err(|e| neondb::error::NeonDBError::internal(format!("Prompt error: {}", e)))?;
            PathBuf::from(input)
        }
    };

    let template_name: String = match template {
        Some(t) => {
            if !TEMPLATES.iter().any(|tmpl| tmpl.name == t) {
                let names: Vec<_> = TEMPLATES.iter().map(|tmpl| tmpl.name).collect();
                eprintln!("Error: unknown template '{}'. Available: {}", t, names.join(", "));
                return Err(neondb::error::NeonDBError::invalid_argument(format!("unknown template '{}'", t)));
            }
            t
        }
        None => {
            // ── Tree selection: pick a branch (category), then a template ────
            // Branches open into their templates; "← Back" returns to the tree.
            let mut categories: Vec<&'static str> = Vec::new();
            for t in TEMPLATES {
                if !categories.contains(&t.category) {
                    categories.push(t.category);
                }
            }
            loop {
                let branch_items: Vec<String> = categories
                    .iter()
                    .map(|c| {
                        let members: Vec<&str> = TEMPLATES
                            .iter()
                            .filter(|t| t.category == *c)
                            .map(|t| t.name.rsplit('/').next().unwrap_or(t.name))
                            .collect();
                        format!("{:14} ▸ {}", c, members.join(", "))
                    })
                    .collect();
                let branch = Select::with_theme(&theme)
                    .with_prompt("Select a template category")
                    .default(0)
                    .items(&branch_items)
                    .interact()
                    .map_err(|e| neondb::error::NeonDBError::internal(format!("Prompt error: {}", e)))?;
                let category = categories[branch];

                let in_branch: Vec<&Template> =
                    TEMPLATES.iter().filter(|t| t.category == category).collect();
                let mut leaf_items: Vec<String> = in_branch
                    .iter()
                    .map(|t| format!("{:22} — {}", t.name, t.description))
                    .collect();
                leaf_items.push("← Back".to_string());
                let leaf = Select::with_theme(&theme)
                    .with_prompt(format!("{category} templates"))
                    .default(0)
                    .items(&leaf_items)
                    .interact()
                    .map_err(|e| neondb::error::NeonDBError::internal(format!("Prompt error: {}", e)))?;
                if leaf == in_branch.len() {
                    continue; // ← Back
                }
                break in_branch[leaf].name.to_string();
            }
        }
    };

    fs::create_dir_all(&project_path)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Cannot create directory: {}", e)))?;

    write_shared_files(&project_path, &project_name, &template_name)?;

    match template_name.as_str() {
        "game/basic"  => scaffold_game_basic(&project_path, &project_name)?,
        "game/full"   => scaffold_game_full(&project_path, &project_name)?,
        "game/unity"  => scaffold_game_unity(&project_path, &project_name)?,
        "game/godot"  => scaffold_game_godot(&project_path, &project_name)?,
        _ => {
            eprintln!("Unknown template '{}'. Run `neondb templates` to see options.", template_name);
            return Err(neondb::error::NeonDBError::invalid_argument(format!("unknown template '{}'", template_name)));
        }
    }
    Ok(())
}


// ─────────────────────────────────────────────────────────────────────────────
// Shared files (every template)
// ─────────────────────────────────────────────────────────────────────────────

fn write_shared_files(project_path: &Path, project_name: &str, template: &str) -> Result<()> {
    let scheduler_note = match template {
        "game/full" =>
            "\n[[scheduler]]\nreducer = \"world_tick\"\ninterval_ms = 1000\n\n[[scheduler]]\nreducer = \"session_cleanup\"\ninterval_ms = 60000\n\n[[scheduler]]\nreducer = \"mm_match\"\ninterval_ms = 5000\n",
        _ => "\n# Add scheduled reducers here after running `neondb add world` or `neondb add matchmaking`\n# [[scheduler]]\n# reducer = \"world_tick\"\n# interval_ms = 1000\n",
    };

    let permissions_example =
        "\n# [permissions]\n# Restrict reducers to specific roles:\n# guild_kick = [\"admin\", \"guild_owner\"]\n# ban_player = [\"admin\", \"moderator\"]\n";

    let toml = format!(
        "[project]\nname = \"{name}\"\nversion = \"0.1.0\"\n\
        [server]\nhost = \"127.0.0.1\"\nport = 3000\nmetrics_port = 3001\n\
        wal_path = \"./wal\"\nsnapshot_dir = \"./snapshots\"\n\
        # api_key = \"change-me\"\nfsync_interval_ms = 0\n\
        # snapshot_interval = 1000000\n\
        {scheduler}{permissions}\n",
        name = project_name,
        scheduler = scheduler_note,
        permissions = permissions_example,
    );

    fs::write(project_path.join("neondb.toml"), toml)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write neondb.toml: {}", e)))?;

    fs::create_dir_all(project_path.join("migrations"))
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Create migrations/: {}", e)))?;
    fs::write(project_path.join("migrations").join("README.md"), MIGRATIONS_README)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write migrations/README.md: {}", e)))?;

    fs::create_dir_all(project_path.join("modules"))
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Create modules/: {}", e)))?;

    fs::write(project_path.join(".gitignore"),
        "*.wal\n*.bin\nsnapshots/\n*.tmp\nnode_modules/\ndist/\n.env\n")
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write .gitignore: {}", e)))?;

    Ok(())
}

fn wf(project_path: &Path, rel: &str, content: &str) -> Result<()> {
    let full = project_path.join(rel);
    if let Some(parent) = full.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| neondb::error::NeonDBError::internal(format!("mkdir {:?}: {}", parent, e)))?;
    }
    fs::write(&full, content)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write {:?}: {}", full, e)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-template scaffolders
// ─────────────────────────────────────────────────────────────────────────────

// ── New game-focused scaffold functions ───────────────────────────────────────

/// Path to the NeonDB source on the machine that compiled this binary.
/// Used to add a [patch] section so scaffolded projects build offline.
const NEONDB_SOURCE_DIR: &str = env!("CARGO_MANIFEST_DIR");

/// Generate a Cargo.toml that embeds the NeonDB server as a library.
///
/// When the local NeonDB source is reachable on disk (the common case — `neondb`
/// was installed via `cargo install --path .`), the scaffold uses a direct
/// `path = "..."` dependency. That keeps `cargo build` fully offline:
/// no git fetch, no crates.io index refresh.
///
/// When the source is gone (user installed the prebuilt binary on a different
/// machine), fall back to the git dependency.
fn game_cargo_toml(name: &str) -> String {
    let neondb_dep = if std::path::Path::new(NEONDB_SOURCE_DIR).exists() {
        format!(
            "neondb     = {{ path = \"{}\" }}",
            NEONDB_SOURCE_DIR.replace('\\', "/")
        )
    } else {
        "neondb     = { git = \"https://github.com/Salaou-Hasan/NeonDB\", tag = \"v1.0.7\" }".to_string()
    };
    format!(
        "[workspace]\n\n\
[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n\
[dependencies]\n{neondb_dep}\n\
serde      = {{ version = \"1\", features = [\"derive\"] }}\nserde_json = \"1\"\n\
env_logger = \"0.11\"\n"
    )
}

/// Write all client SDKs + protocol docs into clients/ inside a scaffolded project.
/// Covers Rust (Bevy / CLI), Unity C#, Godot 4 GDScript, and a PROTOCOL.md
/// so anyone building a custom engine client knows exactly what to implement.
fn scaffold_all_clients(p: &Path, name: &str) -> Result<()> {
    // Rust client (Bevy, CLI tools, bots, custom engines in Rust)
    wf(p, "clients/rust/Cargo.toml",  &client_cargo_toml(name))?;
    wf(p, "clients/rust/src/main.rs", CLIENT_MAIN_RS)?;
    // Pin transitive deps so `cargo run` in clients/rust/ stays offline too.
    let src_lock = std::path::Path::new(NEONDB_SOURCE_DIR).join("Cargo.lock");
    if src_lock.exists() {
        let _ = fs::copy(&src_lock, p.join("clients/rust/Cargo.lock"));
    }

    // Unity C# client (copy clients/unity/ into Assets/Scripts/NeonDB/)
    wf(p, "clients/unity/NeonDBClient.cs",    UNITY_CLIENT_CS)?;
    wf(p, "clients/unity/NeonDBBehaviour.cs", UNITY_BEHAVIOUR_CS)?;
    wf(p, "clients/unity/NeonDBManager.cs",   UNITY_MANAGER_CS)?;

    // Godot 4 GDScript client (add as Autoload in Project Settings)
    wf(p, "clients/godot/neondb_client.gd",  GODOT_CLIENT_GD)?;
    wf(p, "clients/godot/NeonDBManager.gd",  GODOT_MANAGER_GD)?;

    // Wire protocol spec for custom engine implementations (C++, JS, Swift, etc.)
    wf(p, "clients/PROTOCOL.md", CLIENT_PROTOCOL_MD)?;

    Ok(())
}

/// Copy NeonDB's Cargo.lock into the scaffolded project when available,
/// so transitive dep versions are pinned and no crates.io index refresh runs.
fn copy_lockfile_if_available(p: &Path) -> Result<()> {
    let src_lock = std::path::Path::new(NEONDB_SOURCE_DIR).join("Cargo.lock");
    if src_lock.exists() {
        let _ = fs::copy(&src_lock, p.join("Cargo.lock"));
    }
    Ok(())
}

fn scaffold_game_basic(p: &Path, name: &str) -> Result<()> {
    wf(p, "Cargo.toml",                  &game_cargo_toml(name))?;
    copy_lockfile_if_available(p)?;
    wf(p, "rust-toolchain.toml",         RUST_TOOLCHAIN)?;
    wf(p, "src/main.rs",                 GAME_MAIN_RS)?;
    wf(p, "src/reducers/mod.rs",         R_MOD_BASIC)?;
    wf(p, "src/reducers/spawn.rs",       R_SPAWN_RS)?;
    wf(p, "src/reducers/move_player.rs", R_MOVE_RS)?;
    wf(p, "src/reducers/despawn.rs",     R_DESPAWN_RS)?;
    wf(p, "src/reducers/damage.rs",      R_DAMAGE_RS)?;
    wf(p, "src/reducers/heal.rs",        R_HEAL_RS)?;
    wf(p, "schema.toml",                 R_BASIC_SCHEMA)?;
    wf(p, "SCALING.md",                  SCALING_MD)?;
    wf(p, "README.md", &format!("# {name}\n\nNeonDB embedded game server.\n\nSee SCALING.md for the scaling guide.\n"))?;
    scaffold_all_clients(p, name)?;
    print_success(name, "game/basic", &[
        ("Cargo.toml",                        "neondb game server (run `neondb start` from this folder)"),
        ("src/reducers/spawn.rs",             "spawn(player_id, lobby, class)"),
        ("src/reducers/move_player.rs",       "move_player(player_id, x, y)"),
        ("src/reducers/despawn.rs",           "despawn(player_id)"),
        ("src/reducers/damage.rs",            "damage(target_id, amount)"),
        ("src/reducers/heal.rs",              "heal(target_id, amount)"),
        ("schema.toml",                       "players + sessions tables"),
        ("clients/rust/src/main.rs",          "Rust client (Bevy / CLI)"),
        ("clients/unity/NeonDBClient.cs",     "Unity C# client"),
        ("clients/godot/neondb_client.gd",   "Godot 4 GDScript client"),
        ("clients/PROTOCOL.md",              "wire protocol — implement your own client"),
    ]);
    println!("  Next steps:");
    println!("    cd {name}");
    println!("    neondb start");
    println!("    # Rust client (another terminal):");
    println!("    cd clients/rust && cargo run --release");
    println!("    # Unity: copy clients/unity/ into Assets/Scripts/NeonDB/");
    println!("    # Godot: add clients/godot/ files, set neondb_client.gd as Autoload");
    println!();
    println!("  Add systems:");
    println!("    neondb add combat    # attack, respawn, abilities");
    println!("    neondb add inventory # items, equip slots");
    println!("    neondb add chat      # rooms, messages");
    println!();
    Ok(())
}

fn scaffold_game_full(p: &Path, name: &str) -> Result<()> {
    // Core reducers
    scaffold_game_basic(p, name)?;
    // All 9 modules pre-installed
    add_module_files(p, "chat")?;
    add_module_files(p, "inventory")?;
    add_module_files(p, "leaderboard")?;
    add_module_files(p, "matchmaking")?;
    add_module_files(p, "guilds")?;
    add_module_files(p, "quests")?;
    add_module_files(p, "economy")?;
    add_module_files(p, "combat")?;
    add_module_files(p, "world")?;
    println!("  All 9 modules included. See src/reducers/ for the full source.");
    println!("  Add to neondb.toml for scheduled reducers:");
    println!("    [[scheduler]]");
    println!("    reducer = \"world_tick\"");
    println!("    interval_ms = 1000");
    println!();
    Ok(())
}


fn scaffold_game_unity(p: &Path, name: &str) -> Result<()> {
    scaffold_game_full(p, name)?;
    wf(p, "unity/NeonDBClient.cs",    UNITY_CLIENT_CS)?;
    wf(p, "unity/NeonDBBehaviour.cs", UNITY_BEHAVIOUR_CS)?;
    wf(p, "unity/NeonDBManager.cs",   UNITY_MANAGER_CS)?;
    wf(p, "unity/README.md",          UNITY_GAME_README)?;
    println!("  Unity C# SDK → unity/  (also in clients/unity/)");
    println!("    Copy unity/ into Assets/Scripts/NeonDB/");
    println!("    Add NeonDBManager to your scene, set Server URL, press Play.");
    println!("  Rust / Godot / custom engine clients → clients/");
    println!("    See clients/PROTOCOL.md to implement your own client.");
    Ok(())
}

fn scaffold_game_godot(p: &Path, name: &str) -> Result<()> {
    scaffold_game_full(p, name)?;
    wf(p, "godot/neondb_client.gd",   GODOT_CLIENT_GD)?;
    wf(p, "godot/NeonDBManager.gd",   GODOT_MANAGER_GD)?;
    wf(p, "godot/README.md",          GODOT_GAME_README)?;
    println!("  Godot 4 GDScript SDK → godot/  (also in clients/godot/)");
    println!("    Add godot/ files to your project, set neondb_client.gd as Autoload.");
    println!("  Rust / Unity / custom engine clients → clients/");
    println!("    See clients/PROTOCOL.md to implement your own client.");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// neondb add <module>
// ─────────────────────────────────────────────────────────────────────────────

/// Write Rust files for a module into src/reducers/<module>/ and register in mod.rs.
/// Register the module in `src/reducers/mod.rs` so its sub-modules compile.
/// Idempotent — no-op if the line is already present.
fn register_module_in_mod_rs(p: &Path, module: &str) -> Result<()> {
    let mod_rs = p.join("src/reducers/mod.rs");
    let line = format!("pub mod {module};\n");
    let existing = fs::read_to_string(&mod_rs).unwrap_or_default();
    if existing.contains(&line.trim_end()) {
        return Ok(());
    }
    let new_content = if existing.is_empty() {
        line
    } else if existing.ends_with('\n') {
        format!("{existing}{line}")
    } else {
        format!("{existing}\n{line}")
    };
    fs::write(&mod_rs, new_content)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("write mod.rs: {e}")))?;
    Ok(())
}

fn add_module_files(p: &Path, module: &str) -> Result<()> {
    register_module_in_mod_rs(p, module)?;
    match module {
        "chat" => {
            wf(p, "src/reducers/chat/mod.rs",   RM_CHAT_MOD_RS)?;
            wf(p, "src/reducers/chat/send.rs",   RM_CHAT_SEND_RS)?;
            wf(p, "src/reducers/chat/join.rs",   RM_CHAT_JOIN_RS)?;
            wf(p, "src/reducers/chat/leave.rs",  RM_CHAT_LEAVE_RS)?;
            append_schema(p, RM_CHAT_SCHEMA)?;
        }
        "inventory" => {
            wf(p, "src/reducers/inventory/mod.rs",    RM_INV_MOD_RS)?;
            wf(p, "src/reducers/inventory/add.rs",    RM_INV_ADD_RS)?;
            wf(p, "src/reducers/inventory/remove.rs", RM_INV_REMOVE_RS)?;
            wf(p, "src/reducers/inventory/equip.rs",  RM_INV_EQUIP_RS)?;
            append_schema(p, RM_INV_SCHEMA)?;
        }
        "leaderboard" => {
            wf(p, "src/reducers/leaderboard/mod.rs",    RM_LB_MOD_RS)?;
            wf(p, "src/reducers/leaderboard/submit.rs", RM_LB_SUBMIT_RS)?;
            wf(p, "src/reducers/leaderboard/reset.rs",  RM_LB_RESET_RS)?;
            append_schema(p, RM_LB_SCHEMA)?;
        }
        "matchmaking" => {
            wf(p, "src/reducers/matchmaking/mod.rs",      RM_MM_MOD_RS)?;
            wf(p, "src/reducers/matchmaking/queue.rs",    RM_MM_QUEUE_RS)?;
            wf(p, "src/reducers/matchmaking/dequeue.rs",  RM_MM_DEQUEUE_RS)?;
            wf(p, "src/reducers/matchmaking/match_players.rs", RM_MM_MATCH_RS)?;
            append_schema(p, RM_MM_SCHEMA)?;
        }
        "guilds" => {
            wf(p, "src/reducers/guilds/mod.rs",    RM_GUILD_MOD_RS)?;
            wf(p, "src/reducers/guilds/create.rs", RM_GUILD_CREATE_RS)?;
            wf(p, "src/reducers/guilds/invite.rs", RM_GUILD_INVITE_RS)?;
            wf(p, "src/reducers/guilds/accept.rs", RM_GUILD_ACCEPT_RS)?;
            wf(p, "src/reducers/guilds/kick.rs",   RM_GUILD_KICK_RS)?;
            append_schema(p, RM_GUILD_SCHEMA)?;
        }
        "quests" => {
            wf(p, "src/reducers/quests/mod.rs",      RM_QUEST_MOD_RS)?;
            wf(p, "src/reducers/quests/accept.rs",   RM_QUEST_ACCEPT_RS)?;
            wf(p, "src/reducers/quests/progress.rs", RM_QUEST_PROGRESS_RS)?;
            wf(p, "src/reducers/quests/complete.rs", RM_QUEST_COMPLETE_RS)?;
            append_schema(p, RM_QUEST_SCHEMA)?;
        }
        "economy" => {
            wf(p, "src/reducers/economy/mod.rs",      RM_ECON_MOD_RS)?;
            wf(p, "src/reducers/economy/buy.rs",      RM_ECON_BUY_RS)?;
            wf(p, "src/reducers/economy/sell.rs",     RM_ECON_SELL_RS)?;
            wf(p, "src/reducers/economy/transfer.rs", RM_ECON_TRANSFER_RS)?;
            wf(p, "src/reducers/economy/loot.rs",     RM_ECON_LOOT_RS)?;
            append_schema(p, RM_ECON_SCHEMA)?;
        }
        "combat" => {
            wf(p, "src/reducers/combat/mod.rs",      RM_COMBAT_MOD_RS)?;
            wf(p, "src/reducers/combat/attack.rs",   RM_COMBAT_ATTACK_RS)?;
            wf(p, "src/reducers/combat/respawn.rs",  RM_COMBAT_RESPAWN_RS)?;
            wf(p, "src/reducers/combat/ability.rs",  RM_COMBAT_ABILITY_RS)?;
            append_schema(p, RM_COMBAT_SCHEMA)?;
        }
        "world" => {
            wf(p, "src/reducers/world/mod.rs",       RM_WORLD_MOD_RS)?;
            wf(p, "src/reducers/world/tick.rs",      RM_WORLD_TICK_RS)?;
            wf(p, "src/reducers/world/npc_spawn.rs", RM_WORLD_NPC_RS)?;
            wf(p, "src/reducers/world/cleanup.rs",   RM_WORLD_CLEANUP_RS)?;
            append_schema(p, RM_WORLD_SCHEMA)?;
        }
        _ => {}
    }
    Ok(())
}

fn cmd_add_module(module: &str, project_path: &Path) -> Result<()> {
    if !project_path.join("schema.toml").exists() {
        eprintln!("No schema.toml found. Run `neondb add` from inside your project directory.");
        return Err(neondb::error::NeonDBError::invalid_argument("not a NeonDB project directory"));
    }
    // `add_module_files` registers the module in src/reducers/mod.rs itself.
    match module {
        "chat" | "inventory" | "leaderboard" | "matchmaking" |
        "guilds" | "quests" | "economy" | "combat" | "world" => {
            add_module_files(project_path, module)?;
            println!();
            println!("  Added {module} module → src/reducers/{module}/");
            println!("  Rebuild: cargo build --release");
            println!("  Restart: cargo run --release -- start");
        }
        other => {
            let names: Vec<&str> = MODULES.iter().map(|(n, _)| *n).collect();
            eprintln!("Unknown module '{}'. Available: {}", other, names.join(", "));
            return Err(neondb::error::NeonDBError::invalid_argument(
                format!("unknown module '{}'", other)));
        }
    }
    println!();
    Ok(())
}

/// Append new schema tables to the existing schema.toml without duplicating.
fn append_schema(project_path: &Path, extra: &str) -> Result<()> {
    let schema_path = project_path.join("schema.toml");
    let existing = fs::read_to_string(&schema_path).unwrap_or_default();
    // Extract table names from extra to skip already-present tables
    let new_content: String = extra.lines()
        .collect::<Vec<_>>()
        .split(|l: &&str| l.trim().starts_with("[[table]]"))
        .filter(|block| {
            // Find the `name = "..."` line in this block
            let block_name = block.iter()
                .find_map(|l| l.trim().strip_prefix("name = \"").and_then(|s| s.strip_suffix('"')));
            // Skip blocks whose table name is already in the schema
            block_name.map(|n| !existing.contains(&format!("name = \"{n}\"")))
                .unwrap_or(true)
        })
        .flat_map(|block| {
            std::iter::once("[[table]]").chain(block.iter().copied())
        })
        .collect::<Vec<_>>()
        .join("\n");

    if new_content.trim().is_empty() {
        println!("  (all tables already present in schema.toml — skipped)");
        return Ok(());
    }
    let mut file = fs::OpenOptions::new().append(true).open(&schema_path)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("open schema.toml: {e}")))?;
    use std::io::Write as _;
    writeln!(file, "\n{}", new_content.trim())
        .map_err(|e| neondb::error::NeonDBError::internal(format!("append schema.toml: {e}")))
}

fn print_success(project_name: &str, template: &str, files: &[(&str, &str)]) {
    println!();
    println!("  ✓ Project '{}' created  (template: {})", project_name, template);
    println!();
    for (file, desc) in files {
        if desc.is_empty() { println!("    {}", file); }
        else               { println!("    {:<40} {}", file, desc); }
    }
    println!();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Embedded template content — loaded from templates/ at compile time
// ═══════════════════════════════════════════════════════════════════════════════

const MIGRATIONS_README: &str = "# Migrations\nPlace `.toml` files here.\n";
const PERF_MD: &str           = include_str!("../templates/performance.md.txt");
const SCALING_MD: &str        = include_str!("../templates/scaling.md.txt");

// ── Rust game templates ───────────────────────────────────────────────────────
const GAME_MAIN_RS: &str         = include_str!("../templates/r_game_main.rs.txt");
const RUST_TOOLCHAIN: &str       = include_str!("../templates/rust_toolchain.toml.txt");
const R_MOD_BASIC: &str          = include_str!("../templates/r_reducers_mod_basic.rs.txt");
const R_SPAWN_RS: &str           = include_str!("../templates/r_spawn.rs.txt");
const R_MOVE_RS: &str            = include_str!("../templates/r_move.rs.txt");
const R_DESPAWN_RS: &str         = include_str!("../templates/r_despawn.rs.txt");
const R_DAMAGE_RS: &str          = include_str!("../templates/r_damage.rs.txt");
const R_HEAL_RS: &str            = include_str!("../templates/r_heal.rs.txt");
const R_BASIC_SCHEMA: &str       = include_str!("../templates/r_basic_schema.toml.txt");

// ── module reducers (neondb add <name>) ──────────────────────────────────────
const RM_CHAT_MOD_RS: &str       = include_str!("../templates/rm_chat_mod.rs.txt");
const RM_CHAT_SEND_RS: &str      = include_str!("../templates/rm_chat_send.rs.txt");
const RM_CHAT_JOIN_RS: &str      = include_str!("../templates/rm_chat_join.rs.txt");
const RM_CHAT_LEAVE_RS: &str     = include_str!("../templates/rm_chat_leave.rs.txt");
const RM_CHAT_SCHEMA: &str       = include_str!("../templates/rm_chat_schema.toml.txt");
const RM_INV_MOD_RS: &str        = include_str!("../templates/rm_inventory_mod.rs.txt");
const RM_INV_ADD_RS: &str        = include_str!("../templates/rm_inventory_add.rs.txt");
const RM_INV_REMOVE_RS: &str     = include_str!("../templates/rm_inventory_remove.rs.txt");
const RM_INV_EQUIP_RS: &str      = include_str!("../templates/rm_inventory_equip.rs.txt");
const RM_INV_SCHEMA: &str        = include_str!("../templates/rm_inventory_schema.toml.txt");
const RM_LB_MOD_RS: &str         = include_str!("../templates/rm_leaderboard_mod.rs.txt");
const RM_LB_SUBMIT_RS: &str      = include_str!("../templates/rm_leaderboard_submit.rs.txt");
const RM_LB_RESET_RS: &str       = include_str!("../templates/rm_leaderboard_reset.rs.txt");
const RM_LB_SCHEMA: &str         = include_str!("../templates/rm_leaderboard_schema.toml.txt");
const RM_MM_MOD_RS: &str         = include_str!("../templates/rm_matchmaking_mod.rs.txt");
const RM_MM_QUEUE_RS: &str       = include_str!("../templates/rm_matchmaking_queue.rs.txt");
const RM_MM_DEQUEUE_RS: &str     = include_str!("../templates/rm_matchmaking_dequeue.rs.txt");
const RM_MM_MATCH_RS: &str       = include_str!("../templates/rm_matchmaking_match.rs.txt");
const RM_MM_SCHEMA: &str         = include_str!("../templates/rm_matchmaking_schema.toml.txt");
const RM_GUILD_MOD_RS: &str      = include_str!("../templates/rm_guilds_mod.rs.txt");
const RM_GUILD_CREATE_RS: &str   = include_str!("../templates/rm_guilds_create.rs.txt");
const RM_GUILD_INVITE_RS: &str   = include_str!("../templates/rm_guilds_invite.rs.txt");
const RM_GUILD_ACCEPT_RS: &str   = include_str!("../templates/rm_guilds_accept.rs.txt");
const RM_GUILD_KICK_RS: &str     = include_str!("../templates/rm_guilds_kick.rs.txt");
const RM_GUILD_SCHEMA: &str      = include_str!("../templates/rm_guilds_schema.toml.txt");
const RM_QUEST_MOD_RS: &str      = include_str!("../templates/rm_quests_mod.rs.txt");
const RM_QUEST_ACCEPT_RS: &str   = include_str!("../templates/rm_quests_accept.rs.txt");
const RM_QUEST_PROGRESS_RS: &str = include_str!("../templates/rm_quests_progress.rs.txt");
const RM_QUEST_COMPLETE_RS: &str = include_str!("../templates/rm_quests_complete.rs.txt");
const RM_QUEST_SCHEMA: &str      = include_str!("../templates/rm_quests_schema.toml.txt");
const RM_ECON_MOD_RS: &str       = include_str!("../templates/rm_economy_mod.rs.txt");
const RM_ECON_BUY_RS: &str       = include_str!("../templates/rm_economy_buy.rs.txt");
const RM_ECON_SELL_RS: &str      = include_str!("../templates/rm_economy_sell.rs.txt");
const RM_ECON_TRANSFER_RS: &str  = include_str!("../templates/rm_economy_transfer.rs.txt");
const RM_ECON_LOOT_RS: &str      = include_str!("../templates/rm_economy_loot.rs.txt");
const RM_ECON_SCHEMA: &str       = include_str!("../templates/rm_economy_schema.toml.txt");
const RM_COMBAT_MOD_RS: &str     = include_str!("../templates/rm_combat_mod.rs.txt");
const RM_COMBAT_ATTACK_RS: &str  = include_str!("../templates/rm_combat_attack.rs.txt");
const RM_COMBAT_RESPAWN_RS: &str = include_str!("../templates/rm_combat_respawn.rs.txt");
const RM_COMBAT_ABILITY_RS: &str = include_str!("../templates/rm_combat_ability.rs.txt");
const RM_COMBAT_SCHEMA: &str     = include_str!("../templates/rm_combat_schema.toml.txt");
const RM_WORLD_MOD_RS: &str      = include_str!("../templates/rm_world_mod.rs.txt");
const RM_WORLD_TICK_RS: &str     = include_str!("../templates/rm_world_tick.rs.txt");
const RM_WORLD_NPC_RS: &str      = include_str!("../templates/rm_world_npc_spawn.rs.txt");
const RM_WORLD_CLEANUP_RS: &str  = include_str!("../templates/rm_world_cleanup.rs.txt");
const RM_WORLD_SCHEMA: &str      = include_str!("../templates/rm_world_schema.toml.txt");

// ── Rust client SDK scaffold ──────────────────────────────────────────────────

fn client_cargo_toml(name: &str) -> String {
    let client_dep = if std::path::Path::new(NEONDB_SOURCE_DIR).exists() {
        format!(
            "neondb-client = {{ path = \"{}/neondb-client-rust\", package = \"neondb-client\" }}",
            NEONDB_SOURCE_DIR.replace('\\', "/")
        )
    } else {
        "neondb-client = { git = \"https://github.com/Salaou-Hasan/NeonDB\", tag = \"v1.0.7\", package = \"neondb-client\" }".to_string()
    };
    format!(
        "[workspace]\n\n\
[package]\nname = \"{name}-client\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n\
[dependencies]\n{client_dep}\n\
tokio         = {{ version = \"1\", features = [\"full\"] }}\n\
serde_json    = \"1\"\n"
    )
}

const CLIENT_MAIN_RS: &str = r#"//! Example Rust client for a NeonDB game server.
//!
//! Run the server first:  neondb start
//! Then in another terminal: cargo run --release
use neondb_client::{NeonDBClient, ClientOptions};

#[tokio::main]
async fn main() {
    let opts = ClientOptions {
        url: "ws://127.0.0.1:3000".to_string(),
        api_key: None,
        call_timeout_ms: 5_000,
        reconnect: None,
    };

    let client = NeonDBClient::connect(opts).await
        .expect("Failed to connect — is the server running? (neondb start)");

    println!("[client] Connected to server");

    // Subscribe to live player updates
    let (mut rx, _sub_id) = client
        .subscribe("players")
        .await
        .expect("Subscribe failed");

    tokio::spawn(async move {
        while let Some(diff) = rx.recv().await {
            println!(
                "[update] {} {} {} = {:?}",
                diff.operation, diff.table_name, diff.row_key, diff.row_data
            );
        }
    });

    // Spawn a player
    let result = client
        .call("spawn", &serde_json::json!(["rust_player", "lobby_1", "warrior"]))
        .await
        .expect("Reducer call failed");
    println!("[spawn] {:?}", result);

    // Move the player
    let result = client
        .call("move_player", &serde_json::json!(["rust_player", 10.0, 20.0]))
        .await
        .expect("Reducer call failed");
    println!("[move]  {:?}", result);

    // Keep running to receive live updates
    println!("[client] Listening for updates (Ctrl+C to stop)…");
    tokio::signal::ctrl_c().await.ok();
}
"#;

const CLIENT_PROTOCOL_MD: &str = r#"# NeonDB Wire Protocol

Implement this to connect **any** game engine or language to NeonDB.

## Transport

- **WebSocket** binary frames (not text)
- **MessagePack** encoding — structs are positional arrays (rmp_serde default)
- Auth header at upgrade: `Authorization: Bearer <api_key>`
  - Optional role suffix: `Bearer <api_key>:<role>`

## Client → Server messages

All messages are a **MessagePack map with one key** → value is a positional array.

```
{ "ReducerCall": [call_id: u64, reducer_name: str, args: bin] }
{ "Subscribe":   [sub_id: str,  query: str] }
{ "Unsubscribe": [sub_id: str] }
```

- `call_id` — any u64 you choose; matched back in the response
- `args` — MessagePack-encoded array of your reducer's positional arguments
- `query` — e.g. `"players"` or `"players WHERE zone = 'north'"` or `"players WHERE zone = 'north' ORDER BY score DESC LIMIT 10"`
- `sub_id` — any string you choose; used to route live updates back to the right handler

## Server → Client messages

### ReducerResponse (bare array — no wrapper map)
```
[call_id: u64, success: bool, result: bin | nil, error: str | nil]
```
`result` is a MessagePack-encoded value returned by the reducer.

### SubscriptionAck
```
{ "SubscriptionAck": [sub_id: str, success: bool, message: str | nil] }
```

### SubscriptionDiff (one frame per row change)
```
{ "SubscriptionDiff": [sub_id: str, table: str, row_key: str, op: str, data: map | nil] }
```
- `op` — `"insert"` | `"update"` | `"delete"` | `"initial_snapshot"`
- `data` — full row as a MessagePack map, or nil for deletes

### BatchUpdate (one frame per tick — replaces many SubscriptionDiffs)
```
{ "BatchUpdate": [compressed: bool, payload: bin] }
```
- `payload` — when `compressed = false`: MessagePack array of SubscriptionDiff arrays
- `payload` — when `compressed = true`: gzip( above )
- Each element: `[sub_id, table, row_key, op, data | nil]`

**In tick mode (default 20 Hz) the server sends BatchUpdate, not SubscriptionDiff.**
Implement BatchUpdate first — it is the primary live-update path.

### Error
```
{ "Error": { "message": str } }
```

## Minimal implementation checklist

1. Open a WebSocket to `ws://<host>:<port>` with the auth header
2. Send a `ReducerCall` to invoke game logic
3. Await a bare-array `ReducerResponse` matching your `call_id`
4. Send a `Subscribe` with a query string
5. Await `SubscriptionAck` to confirm
6. On each `BatchUpdate`: gzip-decompress if `compressed`, then MsgPack-decode the
   payload as `[[sub_id, table, row_key, op, data?], ...]` and dispatch to handlers
7. Handle `SubscriptionDiff` for servers with tick mode disabled

## MessagePack notes

- Integers: use the most compact fixint/int8/int16/int32/int64 form
- Strings: fixstr / str8 / str16
- Binary: bin8 / bin16 (used for nested args and result payloads)
- Maps: fixmap / map16 (server uses string keys)
- Arrays: fixarray / array16

Any standard MessagePack library works. The server uses Rust's `rmp-serde` in
default (array/positional) mode for struct fields.
"#;

// ── Unity + Godot SDKs ────────────────────────────────────────────────────────
const UNITY_CLIENT_CS: &str    = include_str!("engine_templates/unity_NeonDBClient.cs");
const UNITY_BEHAVIOUR_CS: &str = include_str!("engine_templates/unity_NeonDBBehaviour.cs");
const UNITY_MANAGER_CS: &str   = include_str!("../templates/g_unity_Manager.cs.txt");
const UNITY_GAME_README: &str  = include_str!("../templates/g_unity_readme.md.txt");
const GODOT_CLIENT_GD: &str    = include_str!("engine_templates/godot_neondb_client.gd");
const GODOT_MANAGER_GD: &str   = include_str!("../templates/g_godot_Manager.gd.txt");
const GODOT_GAME_README: &str  = include_str!("../templates/g_godot_readme.md.txt");


// ═══════════════════════════════════════════════════════════════════════════════
// neondb build
// ═══════════════════════════════════════════════════════════════════════════════

/// Detect reducer language and invoke the appropriate compiler before the main
/// JS→WASM and AOT steps.
///
/// Priority (first match wins):
///   1. `reducers/*.csproj` → dotnet publish (C# → WASM via .NET 8 WASI)
///   2. `reducers/go.mod` + `*.go` → tinygo build (Go → WASM via TinyGo)
///
/// Both compilers output `.wasm` into `modules/`, which the remainder of
/// `build_wasm_modules` then AOT-compiles.
fn build_multi_lang_reducers(project_root: &Path, modules_dir: &Path) -> Result<()> {
    let reducers_dir = project_root.join("reducers");
    if !reducers_dir.is_dir() {
        return Ok(()); // no reducers/ directory — nothing to do
    }

    // ── C# detection ─────────────────────────────────────────────────────────
    let csproj = std::fs::read_dir(&reducers_dir).ok().and_then(|entries| {
        entries.flatten().find(|e| {
            e.path().extension().and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("csproj"))
                .unwrap_or(false)
        })
    });
    if let Some(csproj_entry) = csproj {
        let csproj_path = csproj_entry.path();
        println!("  C# project detected: {}", csproj_path.display());

        // Check that dotnet is available.
        let dotnet_ok = std::process::Command::new("dotnet")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !dotnet_ok {
            eprintln!(
                "  Warning: 'dotnet' not found on PATH. Skipping C# compilation.\n\
                 Install .NET 8 SDK: https://dotnet.microsoft.com/download\n\
                 Then install the WASI workload: dotnet workload install wasi-experimental"
            );
            return Ok(());
        }

        println!("  C# → WASM via dotnet publish (wasi-wasm) ...");
        let status = std::process::Command::new("dotnet")
            .arg("publish")
            .arg(&csproj_path)
            .arg("-c").arg("Release")
            .arg("-r").arg("wasi-wasm")
            .arg("--self-contained").arg("true")
            .arg("-o").arg(modules_dir)
            .current_dir(&reducers_dir)
            .status()
            .map_err(|e| neondb::error::NeonDBError::internal(format!("dotnet publish: {}", e)))?;
        if status.success() {
            println!("  C# compilation OK — .wasm written to {}", modules_dir.display());
        } else {
            return Err(neondb::error::NeonDBError::internal(
                format!("dotnet publish failed (exit {:?})", status.code())
            ));
        }
        return Ok(());
    }

    // ── Go / TinyGo detection ─────────────────────────────────────────────────
    let has_gomod = reducers_dir.join("go.mod").exists();
    let has_go_files = std::fs::read_dir(&reducers_dir).ok().map(|entries| {
        entries.flatten().any(|e| {
            e.path().extension().and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("go"))
                .unwrap_or(false)
        })
    }).unwrap_or(false);

    if has_gomod && has_go_files {
        println!("  Go project detected: {}", reducers_dir.display());

        let tinygo_ok = std::process::Command::new("tinygo")
            .arg("version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !tinygo_ok {
            eprintln!(
                "  Warning: 'tinygo' not found on PATH. Skipping Go compilation.\n\
                 Install TinyGo: https://tinygo.org/getting-started/install/\n\
                 Then run: tinygo build -o modules/reducers.wasm -target wasi ./reducers"
            );
            return Ok(());
        }

        // Determine the output name from the module name in go.mod, or use "reducers".
        let mod_name = std::fs::read_to_string(reducers_dir.join("go.mod"))
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.trim_start().starts_with("module "))
                    .map(|l| {
                        l.trim_start_matches("module").trim().split('/').last()
                            .unwrap_or("reducers")
                            .to_string()
                    })
            })
            .unwrap_or_else(|| "reducers".to_string());
        let out_wasm = modules_dir.join(format!("{}.wasm", mod_name));

        println!("  Go → WASM via tinygo build ...");
        let status = std::process::Command::new("tinygo")
            .arg("build")
            .arg("-o").arg(&out_wasm)
            .arg("-target").arg("wasi")
            .arg(".")
            .current_dir(&reducers_dir)
            .status()
            .map_err(|e| neondb::error::NeonDBError::internal(format!("tinygo build: {}", e)))?;
        if status.success() {
            println!("  Go compilation OK — {} written", out_wasm.display());
        } else {
            return Err(neondb::error::NeonDBError::internal(
                format!("tinygo build failed (exit {:?})", status.code())
            ));
        }
    }
    Ok(())
}

fn build_wasm_modules(modules_dir: &Path) -> Result<()> {
    // ── Step 0: compile multi-language reducers (C#, Go) if present ──────────
    let project_root = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    build_multi_lang_reducers(&project_root, modules_dir)?;

    if !modules_dir.is_dir() {
        println!("No '{}' directory found.", modules_dir.display());
        return Ok(());
    }
    let javy_ok = std::process::Command::new("javy")
        .arg("--version").output().map(|o| o.status.success()).unwrap_or(false);
    if !javy_ok {
        eprintln!("Error: 'javy' not found on PATH.\nDownload: https://github.com/bytecodealliance/javy/releases");
        return Err(neondb::error::NeonDBError::internal("javy not found on PATH"));
    }
    let mut js_files = Vec::new();
    collect_js_files(modules_dir, &mut js_files);
    if js_files.is_empty() {
        println!("No .js files found in {}.", modules_dir.display());
        return Ok(());
    }
    let mut compiled = 0usize; let mut failed = 0usize;
    let mut wasm_paths: Vec<std::path::PathBuf> = Vec::new();
    for js_path in &js_files {
        let wasm_path = js_path.with_extension("wasm");
        print!("  JS→WASM  {} ... ", js_path.display());
        match std::process::Command::new("javy").arg("build").arg(js_path).arg("-o").arg(&wasm_path).status() {
            Ok(s) if s.success() => { println!("ok"); compiled += 1; wasm_paths.push(wasm_path); }
            Ok(s) => { println!("FAILED (exit {})", s.code().unwrap_or(-1)); failed += 1; }
            Err(e) => { println!("FAILED ({})", e); failed += 1; }
        }
    }

    // Also AOT-compile any .wasm files that were NOT produced by javy above
    // (e.g. hand-written WAT compiled externally, or Rust→WASM32 reducers).
    collect_wasm_files(modules_dir, &mut wasm_paths);
    wasm_paths.sort(); wasm_paths.dedup();

    let mut aot_ok = 0usize; let mut aot_skip = 0usize;
    println!();
    println!("  AOT compilation (Cranelift → native machine code):");
    for wasm_path in &wasm_paths {
        let cwasm_path = wasm_path.with_extension("cwasm");
        let fresh = cwasm_path.exists() && {
            let t_wasm  = wasm_path.metadata().and_then(|m| m.modified()).ok();
            let t_cwasm = cwasm_path.metadata().and_then(|m| m.modified()).ok();
            matches!((t_wasm, t_cwasm), (Some(w), Some(c)) if c >= w)
        };
        if fresh { aot_skip += 1; continue; }
        print!("  WASM→AOT {} ... ", wasm_path.display());
        match neondb::reducer::wasm::aot_compile(wasm_path) {
            Ok(_) => { println!("ok"); aot_ok += 1; }
            Err(e) => { println!("FAILED ({})", e); }
        }
    }
    println!();
    if failed == 0 {
        println!("Build complete: {} JS→WASM, {} AOT compiled, {} AOT up-to-date.", compiled, aot_ok, aot_skip);
        Ok(())
    } else {
        Err(neondb::error::NeonDBError::internal(format!("{} files failed", failed)))
    }
}

fn collect_js_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() { collect_js_files(&p, out); }
            else if p.extension().and_then(|s| s.to_str()).map(|s| s.eq_ignore_ascii_case("js")).unwrap_or(false) {
                out.push(p);
            }
        }
    }
}

fn collect_wasm_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() { collect_wasm_files(&p, out); }
            else if p.extension().and_then(|s| s.to_str()).map(|s| s.eq_ignore_ascii_case("wasm")).unwrap_or(false) {
                out.push(p);
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Server bootstrap
// ═══════════════════════════════════════════════════════════════════════════════

async fn run_server(config: Config) -> Result<()> {
    let mut logger = env_logger::Builder::from_default_env();
    logger.filter_level(config.log_level.parse().unwrap_or(log::LevelFilter::Info));
    let _ = logger.try_init();

    log::info!("Starting NeonDB Server");

    // Apply global runtime limits (e.g. max blob size) before any data is written.
    config.apply_global_limits();

    let eviction_policy = match config.eviction.policy.trim().to_ascii_lowercase().as_str() {
        "lru_row_cap" => neondb::table::EvictionPolicy::LruRowCap {
            max_rows_per_table: config.eviction.max_rows_per_table.max(1),
        },
        "lru_byte_cap" => neondb::table::EvictionPolicy::LruByteCap {
            max_bytes_total: config.eviction.max_bytes_total.max(1),
        },
        _ => neondb::table::EvictionPolicy::None,
    };
    let mut ts = TableStore::with_eviction(eviction_policy);
    ts.set_shard(config.shard_id, config.shard_count);
    let tables = Arc::new(ts);

    // Build the shared ReducerRegistry ONCE at startup (BUG-2 fix).
    let registry = Arc::new(ReducerRegistry::new()?);
    log::info!("Available reducers: {:?}", registry.list_reducers());

    let mut min_wal_seq: u64 = 0;
    let mut initial_seq: u64 = 0;

    let snap_dir = config.snapshot_dir.clone();
    if let Some((snap_path, _)) = find_latest_snapshot(&snap_dir) {
        match load_snapshot(&snap_path, &tables) {
            Ok(meta) => {
                min_wal_seq = meta.last_sequence;
                initial_seq = meta.last_sequence.saturating_add(1);
                log::info!("Snapshot loaded: {} rows, replaying WAL from seq > {}", meta.row_count, meta.last_sequence);
            }
            Err(e) => log::warn!("Failed to load snapshot: {} — replaying full WAL", e),
        }
    }

    if config.wal_path.exists() {
        match recover_from_wal(&config.wal_path, &tables, min_wal_seq) {
            Ok((n, max_seq)) => {
                initial_seq = initial_seq.max(max_seq.saturating_add(1));
                log::info!("Recovered {} WAL entries (last seq={})", n, max_seq);
            }
            Err(e) => log::warn!("WAL recovery failed: {}", e),
        }
    } else { log::info!("WAL does not exist, starting fresh"); }

    let migrations_dir = PathBuf::from("migrations");
    match neondb::migrations::apply_migrations(&migrations_dir, &tables) {
        Ok(0) => {}
        Ok(n) => log::info!("Applied {} migration file(s)", n),
        Err(e) => log::warn!("Migration error: {}", e),
    }

    let schema_registry = Arc::new(
        neondb::schema::SchemaRegistry::load_from_file(Path::new("schema.toml"))
            .unwrap_or_else(|_| neondb::schema::SchemaRegistry::new())
    );

    // Tenant registry — hydrated from __tenants table (populated by WAL/snapshot replay above).
    let tenant_registry = neondb::tenant::TenantRegistry::load(tables.clone());
    log::info!("[tenant] {} tenant(s) loaded", tenant_registry.count());

    // Redis (RESP) + PostgreSQL (pgwire) protocol listeners over the MVCC engine.
    neondb::server::spawn_protocol_listeners(&config);

    let permissions = Arc::new(config.permissions.clone());

    // ── Cluster bus (horizontal scaling) ────────────────────────────────────
    // Reads NEONDB_PEERS, NEONDB_SHARD_ID, NEONDB_SHARD_COUNT from env.
    // No-op when NEONDB_PEERS is unset (single-node mode).
    let my_shard_id: u32 = std::env::var("NEONDB_SHARD_ID")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let shard_count: u32 = std::env::var("NEONDB_SHARD_COUNT")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(1);
    let cluster_cfg = neondb::cluster::ClusterConfig::from_env(my_shard_id, shard_count);
    let cluster_bus = neondb::cluster::ClusterBus::new(cluster_cfg);
    if cluster_bus.is_active() {
        log::info!(
            "[cluster] Active — shard {}/{}, {} peer(s): {}",
            my_shard_id, shard_count,
            cluster_bus.peers.len(),
            cluster_bus.healthy_peers().iter()
                .map(|p| format!("shard{}@{}", p.shard_id, p.metrics_url))
                .collect::<Vec<_>>().join(", ")
        );
    } else {
        log::info!("[neondb] single-node mode (set NEONDB_PEERS to enable clustering)");
    }

    let (reducer_tx, reducer_rx) = kanal::bounded_async::<PendingCall>(config.reducer_queue_cap);
    let queue_probe = reducer_tx.clone(); // for healthz queue-depth reporting
    let subscription_manager = Arc::new(SubscriptionManager::new_with_options(config.two_frame_protocol));
    subscription_manager.start_tick_flusher(config.sub_tick_ms);

    let active_connections = Arc::new(AtomicUsize::new(0));
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    // ── Auth validator (JWT / API key / open) ────────────────────────────────
    let auth_validator = Arc::new(AuthValidator::from_env());

    // ── Ed25519 identity issuer ───────────────────────────────────────────────
    // Persist key in <wal_dir>/identity_key.pem.  Generated on first start,
    // reloaded on subsequent starts so tokens stay valid across restarts.
    let identity_key_path = config.wal_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("identity_key.pem");
    let identity_issuer: Arc<IdentityIssuer> = if identity_key_path.exists() {
        match IdentityIssuer::load_from_file(&identity_key_path) {
            Ok(iss) => {
                log::info!("[identity] Loaded Ed25519 key (kid={})", iss.kid);
                Arc::new(iss)
            }
            Err(e) => {
                log::warn!("[identity] Failed to load key ({}), generating new key", e);
                let iss = IdentityIssuer::generate();
                if let Err(e2) = iss.save_to_file(&identity_key_path) {
                    log::warn!("[identity] Could not persist new key: {}", e2);
                }
                Arc::new(iss)
            }
        }
    } else {
        let iss = IdentityIssuer::generate();
        if let Err(e) = iss.save_to_file(&identity_key_path) {
            log::warn!("[identity] Could not persist key: {}", e);
        }
        log::info!("[identity] Generated new Ed25519 key (kid={})", iss.kid);
        Arc::new(iss)
    };
    println!("[neondb] Identity public key:\n{}", identity_issuer.public_key_pem());

    // ── Rate limiter ─────────────────────────────────────────────────────────
    let rate_limiter = Arc::new(RateLimiterRegistry::new(RateLimiterConfig {
        capacity: config.rate_limit_capacity,
        refill_rate: config.rate_limit_refill_rate,
        enabled: config.rate_limit_capacity > 0,
    }));

    // ── Presence manager ─────────────────────────────────────────────────────
    let presence = Arc::new(PresenceManager::new(
        config.presence_heartbeat_timeout_ms,
        config.presence_offline_timeout_ms,
    ));

    // ── TTL manager ──────────────────────────────────────────────────────────
    let ttl_manager = Arc::new(TtlManager::new());

    // ── Prometheus metrics ────────────────────────────────────────────────────
    let metrics = Arc::new(Metrics::new());

    // ── TLS configuration ────────────────────────────────────────────────────
    let tls_server_config: Option<std::sync::Arc<rustls::ServerConfig>> = if config.tls.enabled {
        match (config.tls.cert_path.as_deref(), config.tls.key_path.as_deref()) {
            (Some(cert), Some(key)) => {
                match neondb::network::tls::load_tls_config(cert, key) {
                    Ok(cfg) => {
                        log::info!("TLS enabled: cert={}, key={}", cert.display(), key.display());
                        Some(cfg)
                    }
                    Err(e) => {
                        log::error!("Failed to load TLS config, falling back to plaintext: {}", e);
                        None
                    }
                }
            }
            _ => {
                log::warn!("TLS enabled but cert_path/key_path not set. Falling back to plaintext.");
                None
            }
        }
    } else {
        None
    };

    let inline_registry = neondb::network::build_inline_registry();
    let drain_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let wal_writer = Arc::new(BatchedWalWriter::open(
        &config.wal_path, config.wal_batch_interval_ms, config.wal_batch_size, config.unsafe_no_fsync,
    )?);
    let worker_count = if config.workers > 0 { config.workers } else { num_cpus::get().max(1) };
    log::info!("Starting {} reducer workers", worker_count);
    let global_seq = Arc::new(std::sync::atomic::AtomicU64::new(initial_seq));

    let lobby_router = {
        let worker_deps = std::sync::Arc::new(neondb::worker_pool::WorkerDeps {
            tables: tables.clone(),
            registry: registry.clone(),
            subscription_manager: subscription_manager.clone(),
            wal_writer: wal_writer.clone(),
            global_seq: global_seq.clone(),
            schema_registry: schema_registry.clone(),
            ttl_manager: ttl_manager.clone(),
            tenant_registry: tenant_registry.clone(),
            cluster_bus: cluster_bus.clone(),
            metrics: metrics.clone(),
            timeout_ms: config.reducer_timeout_ms,
            snapshot_interval: config.snapshot_interval,
            snapshot_dir: config.snapshot_dir.clone(),
        });
        let max_lobbies = config.max_connections / 2;
        Arc::new(neondb::worker_pool::LobbyRouter::new(
            reducer_tx.clone(),
            config.reducer_queue_cap.max(256),
            max_lobbies.max(64),
            worker_deps,
            shutdown_rx.clone(),
        ))
    };

    let listener_handle = {
        let config_c = config.clone(); let tx_c = reducer_tx.clone();
        let subs_c = subscription_manager.clone(); let tables_c = tables.clone();
        let conns_c = active_connections.clone(); let rx_shutdown = shutdown_rx.clone();
        let perms_c = permissions.clone();
        let auth_c = auth_validator.clone();
        let rl_c = rate_limiter.clone();
        let pres_c = presence.clone();
        let ttl_c = ttl_manager.clone();
        let metrics_c = metrics.clone();
        let tls_cfg = tls_server_config.clone();
        let iss_c = identity_issuer.clone();
        let tenant_registry_ws = tenant_registry.clone();
        let inl_c = inline_registry.clone();
        let lr_c = lobby_router.clone();
        let df_c = drain_flag.clone();
        tokio::spawn(async move {
            if let Err(e) = start_listener(
                config_c.host, config_c.port, tx_c, subs_c, tables_c,
                config_c.max_connections, config_c.api_key.clone(),
                conns_c, perms_c, config_c.sql_timeout_ms,
                auth_c, rl_c, pres_c, ttl_c, iss_c, rx_shutdown, metrics_c, tls_cfg,
                tenant_registry_ws, inl_c, Some(lr_c), df_c,
            ).await { log::error!("Listener error: {}", e); }
        })
    };

    let timeout_ms        = config.reducer_timeout_ms;
    let snapshot_interval = config.snapshot_interval;
    let snapshot_dir_w    = config.snapshot_dir.clone();

    let startup_instant = std::time::Instant::now();

    let metrics_handle = {
        let subs_c = subscription_manager.clone(); let tables_c = tables.clone();
        let rx_shutdown = shutdown_rx.clone();
        let host_c = config.host.clone(); let mport = config.metrics_port;
        let registry_c = registry.clone();
        let wal_c = wal_writer.clone();
        let seq_c = global_seq.clone();
        let pres_m = presence.clone();
        let ttl_m = ttl_manager.clone();
        let prom_c = metrics.clone();
        let issuer_c = identity_issuer.clone();
        let qprobe_c = queue_probe.clone();

        // ── Multi-region infrastructure ──────────────────────────────────────
        // Override NEONDB_REGION / NEONDB_REGIONS via config fields so the
        // same env-var-based construction works whether started from binary
        // or from run_server().
        if !config.region.is_empty() && config.region != "default" {
            std::env::set_var("NEONDB_REGION", &config.region);
        }
        if !config.regions.is_empty() {
            std::env::set_var("NEONDB_REGIONS", &config.regions);
        }
        let region_registry = Arc::new(neondb::cluster::RegionRegistry::from_env());
        if region_registry.is_multi_region() {
            log::info!("[regions] Multi-region mode: region='{}', peers={}",
                region_registry.my_region, region_registry.peer_regions().len());
        }

        let lobby_routes = neondb::cluster::LobbyRouteRegistry::new(tables.clone());

        let leaderboard = Arc::new(neondb::leaderboard::LeaderboardEngine::new());
        // Register the default leaderboard board.
        leaderboard.create_board(neondb::leaderboard::LeaderboardConfig {
            name: config.leaderboard_board.clone(),
            sort_order: neondb::leaderboard::SortOrder::HighestFirst,
            time_window: neondb::leaderboard::TimeWindow::AllTime,
            max_entries: config.leaderboard_top_n,
        });
        // Start cross-region aggregation if multi-region.
        neondb::leaderboard::LeaderboardAggregator::new(
            leaderboard.clone(),
            region_registry.clone(),
            config.leaderboard_board.clone(),
            config.leaderboard_interval_secs,
            config.leaderboard_top_n,
        ).start(shutdown_rx.clone());

        let stat_sync = neondb::stat_sync::StatSyncQueue::new(
            tables.clone(),
            region_registry.clone(),
            config.stat_sync_flush_ms,
            shutdown_rx.clone(),
        );

        let admin_c = Arc::new(AdminState {
            wal_path: config.wal_path.clone(),
            backup_dir: config.backup_dir.clone(),
            backup_keep: config.backup_keep,
            tenant_registry: tenant_registry.clone(),
            cluster_bus: cluster_bus.clone(),
            drain_flag: drain_flag.clone(),
            active_connections: active_connections.clone(),
            region_registry: region_registry.clone(),
            lobby_routes: lobby_routes.clone(),
            leaderboard: leaderboard.clone(),
            stat_sync: stat_sync.clone(),
        });
        let schema_c = schema_registry.clone();
        tokio::spawn(async move {
            if let Err(e) = start_metrics_server(host_c, mport, subs_c, tables_c, registry_c, wal_c, seq_c, startup_instant, pres_m, ttl_m, prom_c, issuer_c, qprobe_c, admin_c, schema_c, rx_shutdown).await {
                log::error!("Metrics server error: {}", e);
            }
        })
    };

    // ── Replication: replica mode ────────────────────────────────────────────
    // A replica pulls committed WAL entries from the primary, applies them
    // locally, and rejects reducer calls until promoted (POST /replication/promote).
    if config.role.eq_ignore_ascii_case("replica") {
        match config.primary_url.clone() {
            Some(primary) => {
                neondb::replication::set_replica(true);
                // Resume from the highest locally recovered sequence.
                neondb::replication::init_replica_from_local_wal(initial_seq.saturating_sub(1));
                let tables_r = tables.clone();
                let subs_r = subscription_manager.clone();
                let wal_r = wal_writer.clone();
                let seq_r = global_seq.clone();
                let poll = config.replica_poll_ms;
                let shut_r = shutdown_rx.clone();
                tokio::spawn(async move {
                    neondb::replication::run_replica_loop(
                        primary, tables_r, subs_r, wal_r, seq_r, poll, shut_r,
                    ).await;
                });
                log::info!("[replication] Started in REPLICA mode (read-only)");
            }
            None => {
                log::error!(
                    "[replication] NEONDB_ROLE=replica but NEONDB_PRIMARY_URL is not set — \
                     starting as primary instead"
                );
            }
        }
    }

    // ── Automated backups ────────────────────────────────────────────────────
    if let (Some(backup_dir), true) = (config.backup_dir.clone(), config.backup_interval_secs > 0) {
        let tables_b = tables.clone();
        let wal_path_b = config.wal_path.clone();
        let seq_b = global_seq.clone();
        let keep = config.backup_keep;
        let interval_secs = config.backup_interval_secs;
        let mut shut_b = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(10)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await; // skip the immediate first tick
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let tbl = tables_b.clone();
                        let wal = wal_path_b.clone();
                        let dir = backup_dir.clone();
                        let seq = seq_b.load(std::sync::atomic::Ordering::Relaxed);
                        let res = tokio::task::spawn_blocking(move || {
                            let p = neondb::backup::backup_now(&tbl, &wal, &dir, seq)?;
                            let removed = neondb::backup::rotate_backups(&dir, keep)?;
                            Ok::<_, neondb::error::NeonDBError>((p, removed))
                        }).await;
                        match res {
                            Ok(Ok((path, removed))) => log::info!(
                                "[backup] Automated backup at {:?} ({} old rotated out)", path, removed
                            ),
                            Ok(Err(e)) => log::error!("[backup] Automated backup failed: {}", e),
                            Err(e)     => log::error!("[backup] Backup task panicked: {}", e),
                        }
                    }
                    _ = shut_b.changed() => break,
                }
            }
        });
        log::info!("[backup] Automated backups every {}s (keep {})", interval_secs, keep);
    }

    // ── Cluster gossip + fan-out retry tasks ─────────────────────────────────
    neondb::cluster::gossip::start_gossip(cluster_bus.clone(), shutdown_rx.clone());
    neondb::cluster::fanout::start_fanout_retry(cluster_bus.clone(), shutdown_rx.clone());

    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let rx = reducer_rx.clone(); let tables_w = tables.clone();
        let registry_w = registry.clone();
        let subs_w = subscription_manager.clone(); let wal_w = wal_writer.clone();
        let seq_w = global_seq.clone(); let snap_iv = snapshot_interval;
        let snap_dir_ww = snapshot_dir_w.clone(); let schema_w = schema_registry.clone();
        let ttl_w = ttl_manager.clone();
        let tenant_w = tenant_registry.clone();
        let cluster_w = cluster_bus.clone();
        let mut rx_shutdown_w = shutdown_rx.clone();
        let metrics_w = metrics.clone();

        worker_handles.push(tokio::spawn(async move {
            loop {
                let call = tokio::select! {
                    result = rx.recv() => match result { Ok(c) => c, Err(_) => break },
                    _ = rx_shutdown_w.changed() => break,
                };
                let call_id     = call.call_id;

                // Replicas are read-only: reject reducer calls until promoted.
                if neondb::replication::is_replica() {
                    let resp = ReducerResponse::error(
                        call_id,
                        "This node is a read-only replica. Write to the primary, or promote this node via POST /replication/promote.".to_string(),
                    );
                    if let Err(e) = call.response_tx.send(resp) { log::warn!("send response: {}", e); }
                    continue;
                }

                let caller_id    = call.caller_id.clone();
                let caller_role  = call.caller_role.clone();
                let call_tenant  = call.tenant_id.clone();
                let tables_blk   = tables_w.clone();
                let registry_blk = registry_w.clone();
                let reducer_name = call.reducer_name.clone();
                let args         = call.args.clone();
                let ts           = current_timestamp_nanos();
                let schema_blk   = schema_w.clone();
                let ttl_blk      = ttl_w.clone();
                let tenant_blk   = tenant_w.clone();
                let call_start   = std::time::Instant::now();

                // Execute + commit with OCC conflict retry: when a concurrent
                // worker committed a row this reducer read AND writes, the
                // commit aborts and we re-execute against fresh state (max 5).
                // Zero silent lost updates in read-modify-write reducers.
                enum Outcome {
                    Done(Vec<u8>, Vec<neondb::table::RowDelta>),
                    ReducerErr(String),
                    Panicked,
                    CommitErr(String),
                }

                let blk = tokio::time::timeout(
                    std::time::Duration::from_millis(timeout_ms),
                    tokio::task::spawn_blocking(move || {
                        let mut ctx = ReducerContext::new(tables_blk, ts)
                            .with_schema(schema_blk)
                            .with_ttl(ttl_blk);
                        ctx.caller_id   = caller_id;
                        ctx.caller_role = caller_role;
                        if let Some(tid) = call_tenant {
                            ctx = ctx.with_tenant(tid, tenant_blk);
                        }
                        const MAX_CONFLICT_RETRIES: usize = 64;
                        let mut attempt = 0;
                        loop {
                            attempt += 1;
                            let exec = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                                || registry_blk.execute(&reducer_name, &mut ctx, &args)
                            ));
                            break match exec {
                                Ok(Ok(result_bytes)) => match ctx.commit() {
                                    Ok(deltas) => Outcome::Done(result_bytes, deltas),
                                    Err(neondb::error::NeonDBError::TxnConflict(_))
                                        if attempt < MAX_CONFLICT_RETRIES =>
                                    {
                                        ctx.reset_for_retry();
                                        std::thread::yield_now();
                                        continue;
                                    }
                                    Err(e) => Outcome::CommitErr(e.to_string()),
                                },
                                Ok(Err(e)) => Outcome::ReducerErr(e.to_string()),
                                Err(_) => Outcome::Panicked,
                            };
                        }
                    }),
                ).await;

                let response = match blk {
                    Err(_) => {
                        log::warn!("call_id={} timed out", call_id);
                        metrics_w.reducer_errors_total.inc();
                        ReducerResponse::error(call_id, "Reducer timed out".to_string())
                    }
                    Ok(Err(e)) => {
                        log::error!("Join error: {}", e);
                        metrics_w.reducer_errors_total.inc();
                        ReducerResponse::error(call_id, "Internal task error".to_string())
                    }
                    Ok(Ok(outcome)) => match outcome {
                        Outcome::Done(result_bytes, deltas) => {
                            // ── Single-node write path (commit already applied) ──────────────
                            // Fan out to live subscribers, then append to the WAL for crash
                            // recovery. Distribution/consensus was removed in Session 44.
                            if !deltas.is_empty() {
                                subs_w.publish_deltas(&deltas);
                                // Fan out to cluster peers (fire-and-forget, no-op if single-node).
                                cluster_w.fanout_deltas(&deltas);
                            }
                            let seq_num = seq_w.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let entry = WalEntry::new(ts, seq_num, call.reducer_name.clone(), call.args.clone(), deltas.clone());
                            if let Err(e) = wal_w.append(&entry, seq_num) {
                                log::warn!("WAL append failed: {}", e);
                            } else {
                                metrics_w.wal_entries_written_total.inc();
                            }
                            // Periodic snapshot.
                            if snap_iv > 0 && (seq_num + 1) % snap_iv == 0 {
                                let tbl = tables_w.clone(); let dir = snap_dir_ww.clone(); let ts2 = current_timestamp_nanos();
                                let wal_rotate = wal_w.clone();
                                tokio::spawn(async move {
                                    match tokio::task::spawn_blocking(move || save_snapshot(&tbl, &dir, seq_num, ts2)).await {
                                        Ok(Ok(())) => {
                                            log::info!("Snapshot written at seq {}", seq_num);
                                            if let Err(e) = wal_rotate.truncate_before(seq_num) {
                                                log::error!("WAL rotation after snapshot failed: {}", e);
                                            }
                                        }
                                        Ok(Err(e)) => log::error!("Snapshot failed: {}", e),
                                        Err(e)     => log::error!("Snapshot panicked: {}", e),
                                    }
                                });
                            }
                            // Record successful reducer call + duration.
                            metrics_w.reducer_calls_total.inc();
                            metrics_w.reducer_duration_seconds.observe(call_start.elapsed().as_secs_f64());
                            ReducerResponse::success(call_id, result_bytes)
                        }
                        Outcome::CommitErr(e) => {
                            log::error!("Commit failed call_id={}: {}", call_id, e);
                            metrics_w.reducer_errors_total.inc();
                            ReducerResponse::error(call_id, format!("Commit error: {}", e))
                        }
                        Outcome::ReducerErr(e) => {
                            log::warn!("Reducer error: {}", e);
                            metrics_w.reducer_errors_total.inc();
                            ReducerResponse::error(call_id, e)
                        }
                        Outcome::Panicked => {
                            log::warn!("Reducer panicked call_id={}", call_id);
                            metrics_w.reducer_errors_total.inc();
                            ReducerResponse::error(call_id, "Reducer panicked".to_string())
                        }
                    },
                };
                if let Err(e) = call.response_tx.send(response) { log::warn!("send response: {}", e); }
            }
            log::debug!("Reducer worker {} stopped", worker_id);
        }));
    }

    // ── Presence sweep background task ─────────────────────────────────────────
    let presence_handle = {
        let pres = presence.clone();
        let mut rx_pres = shutdown_rx.clone();
        let sweep_interval = if config.presence_heartbeat_timeout_ms > 0 {
            config.presence_heartbeat_timeout_ms / 2
        } else {
            30_000 // default to 30s if disabled (task will be a no-op)
        };
        tokio::spawn(async move {
            if sweep_interval == 0 { return; }
            let mut ticker = tokio::time::interval(Duration::from_millis(sweep_interval.max(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await; // skip first immediate tick
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        let (newly_idle, removed) = pres.sweep(now_ms);
                        for uid in &newly_idle {
                            log::debug!("Presence: user '{}' is now idle", uid);
                        }
                        for uid in &removed {
                            log::debug!("Presence: user '{}' removed (offline timeout)", uid);
                        }
                    }
                    _ = rx_pres.changed() => break,
                }
            }
        })
    };

    // ── TTL sweep background task ────────────────────────────────────────────
    let ttl_handle = {
        let ttl_mgr = ttl_manager.clone();
        let tables_ttl = tables.clone();
        let subs_ttl = subscription_manager.clone();
        let mut rx_ttl = shutdown_rx.clone();
        let sweep_ms = config.ttl_sweep_interval_ms;
        tokio::spawn(async move {
            if sweep_ms == 0 { return; }
            let mut ticker = tokio::time::interval(Duration::from_millis(sweep_ms));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await; // skip first immediate tick
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        let expired = ttl_mgr.collect_expired(now_ms);
                        if expired.is_empty() { continue; }
                        let mut deltas = Vec::new();
                        for entry in &expired {
                            match tables_ttl.delete_row(&entry.table_name, &entry.row_key) {
                                Ok(delta) => deltas.push(delta),
                                Err(e) => {
                                    log::warn!("TTL delete {}.{} failed: {}", entry.table_name, entry.row_key, e);
                                }
                            }
                        }
                        if !deltas.is_empty() {
                            log::debug!("TTL sweep: deleted {} expired rows", deltas.len());
                            subs_ttl.publish_deltas(&deltas);
                        }
                    }
                    _ = rx_ttl.changed() => break,
                }
            }
        })
    };

    let mut scheduler_handles = Vec::new();
    let sched_seq = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX / 2));
    for sched in &config.scheduled_reducers {
        let sched: ScheduledReducerConfig = sched.clone();
        let tx_sched = reducer_tx.clone();
        let seq_sched = sched_seq.clone();
        let mut rx_shutdown_sched = shutdown_rx.clone();
        let args_bytes: Vec<u8> = sched.args_json.as_deref()
            .and_then(|j| serde_json::from_str::<serde_json::Value>(j).ok())
            .and_then(|v| rmp_serde::to_vec(&v).ok()).unwrap_or_default();
        log::info!("Scheduler: '{}' every {}ms", sched.reducer, sched.interval_ms);
        scheduler_handles.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(sched.interval_ms.max(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let call_id = seq_sched.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel::<ReducerResponse>();
                        let call = PendingCall {
                            call_id,
                            reducer_name: sched.reducer.clone(),
                            args: args_bytes.clone(),
                            caller_id: "scheduler".to_string(),
                            caller_role: "scheduler".to_string(),
                            tenant_id: None,
                            lobby_hint: None,
                            response_tx: resp_tx,
                        };
                        if tx_sched.send(call).await.is_ok() {
                            let name_c = sched.reducer.clone();
                            tokio::spawn(async move {
                                if let Some(resp) = resp_rx.recv().await {
                                    if !resp.success { log::warn!("Scheduler '{}' failed: {:?}", name_c, resp.error); }
                                }
                            });
                        } else { break; }
                    }
                    _ = rx_shutdown_sched.changed() => break,
                }
            }
        }));
    }

    // ── Periodic gauge-refresh task (every 5 s) ──────────────────────────────
    // Reads snapshot of current row count / subscription count / Raft state
    // and pushes them into the Prometheus gauges.  This is intentionally
    // separate from the hot path — no lock contention on the hot path.
    let gauge_handle = {
        let tables_g = tables.clone();
        let subs_g   = subscription_manager.clone();
        let prom_g   = metrics.clone();
        let mut rx_g = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await; // skip first immediate tick
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Row count
                        prom_g.rows_total.set(tables_g.total_row_count() as i64);
                        // Subscription count
                        prom_g.subscriptions_active.set(
                            subs_g.active_subscriptions() as i64
                        );
                    }
                    _ = rx_g.changed() => break,
                }
            }
        })
    };

    tokio::signal::ctrl_c().await.ok();
    eprintln!("\n[neondb] Shutdown signal — draining...");
    log::info!("Shutdown signal received");

    // 1. Stop accepting new connections and signal all background tasks.
    let _ = shutdown_tx.send(());

    // 2. Drop the sender side of the reducer channel so workers drain and exit.
    drop(reducer_tx);

    // 3. Wait for all in-flight reducer workers to finish, with a 30-second deadline.
    let drain_result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        async {
            for h in worker_handles  { let _ = h.await; }
            for h in scheduler_handles { let _ = h.await; }
        }
    ).await;
    if drain_result.is_err() {
        log::warn!("[neondb] Worker drain timed out after 30s — some in-flight reducers may be incomplete");
    }

    // 4. Flush any buffered WAL entries to disk before shutting down the writer.
    if let Err(e) = wal_writer.flush().await {
        log::error!("WAL flush failed during shutdown: {}", e);
    }
    if let Ok(writer) = Arc::try_unwrap(wal_writer) {
        if let Err(e) = writer.shutdown() { log::error!("WAL shutdown: {}", e); }
    }

    // 5. Await all remaining task handles (listener sends WebSocket Close frames).
    let _ = listener_handle.await;
    let _ = metrics_handle.await;
    let _ = presence_handle.await;
    let _ = ttl_handle.await;
    let _ = gauge_handle.await;

    eprintln!("[neondb] Shutdown complete.");
    log::info!("Shutdown complete");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Inline bench
// ─────────────────────────────────────────────────────────────────────────────

async fn run_cli_bench(ws_url: &str, num_clients: usize, calls_per_client: usize, warmup_per_client: usize, api_key: Option<&str>) -> Result<()> {
    use futures::{SinkExt, StreamExt};
    use hdrhistogram::Histogram;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;
    use tokio::task::JoinSet;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    #[derive(serde::Serialize)] struct IncrArgs { name: String, delta: i32 }
    #[derive(serde::Serialize)] struct CallW { #[serde(rename = "ReducerCall")] rc: (u64, String, Vec<u8>) }

    println!("=== NeonDB Bench ===");
    println!("  Server  : {}", ws_url);
    println!("  Clients : {}  Calls/client: {}  Warmup: {}", num_clients, calls_per_client, warmup_per_client);

    let args_bytes = rmp_serde::to_vec(&IncrArgs { name: "bench".to_string(), delta: 1 }).unwrap();
    let latencies: Arc<Mutex<Histogram<u64>>> = Arc::new(Mutex::new(Histogram::new(3).unwrap()));
    let mut join_set = JoinSet::new();
    let start = Instant::now();

    for cid in 0..num_clients {
        let url = ws_url.to_string(); let api = api_key.map(String::from);
        let args = args_bytes.clone(); let lat = latencies.clone();
        let warmup = warmup_per_client; let calls = calls_per_client;
        join_set.spawn(async move {
            let mut req = url.as_str().into_client_request().unwrap();
            if let Some(k) = &api { req.headers_mut().insert("authorization", format!("Bearer {}", k).parse().unwrap()); }
            let Ok((mut ws, _)) = tokio_tungstenite::connect_async(req).await else { return 0usize; };
            let total = warmup + calls; let mut ok = 0usize;
            for i in 0..total {
                let cw = rmp_serde::to_vec(&CallW { rc: ((cid as u64) * 1_000_000 + i as u64, "increment".to_string(), args.clone()) }).unwrap();
                let t0 = Instant::now();
                if ws.send(Message::Binary(cw)).await.is_err() { break; }
                if let Ok(Some(Ok(Message::Binary(_) | Message::Text(_)))) = tokio::time::timeout(Duration::from_secs(10), ws.next()).await {
                    if i >= warmup { let us = t0.elapsed().as_micros() as u64; if let Ok(mut h) = lat.lock() { let _ = h.record(us); } ok += 1; }
                }
            }
            let _ = ws.close(None).await; ok
        });
    }

    let mut total = 0usize;
    while let Some(r) = join_set.join_next().await { if let Ok(n) = r { total += n; } }
    let elapsed = start.elapsed();
    println!("\nResults:");
    println!("  Time       : {:.3}s", elapsed.as_secs_f64());
    println!("  Throughput : {:.0} TPS", total as f64 / elapsed.as_secs_f64());
    println!("  Success    : {}/{}", total, num_clients * calls_per_client);
    if let Ok(h) = latencies.lock() {
        println!("  Latency (µs): p50={} p95={} p99={} max={}", h.value_at_percentile(50.0), h.value_at_percentile(95.0), h.value_at_percentile(99.0), h.max());
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Metrics / admin HTTP server
// ─────────────────────────────────────────────────────────────────────────────

/// Paths + backup policy needed by the admin endpoints (backup, replication).
struct AdminState {
    wal_path: PathBuf,
    backup_dir: Option<PathBuf>,
    backup_keep: usize,
    tenant_registry: Arc<neondb::tenant::TenantRegistry>,
    cluster_bus: Arc<neondb::cluster::ClusterBus>,
    drain_flag: Arc<std::sync::atomic::AtomicBool>,
    active_connections: Arc<std::sync::atomic::AtomicUsize>,
    region_registry: Arc<neondb::cluster::RegionRegistry>,
    lobby_routes: Arc<neondb::cluster::LobbyRouteRegistry>,
    leaderboard: Arc<neondb::leaderboard::LeaderboardEngine>,
    stat_sync: Arc<neondb::stat_sync::StatSyncQueue>,
}

async fn start_metrics_server(
    host: String,
    port: u16,
    subscription_manager: Arc<SubscriptionManager>,
    tables: Arc<TableStore>,
    registry: Arc<ReducerRegistry>,
    wal_writer: Arc<BatchedWalWriter>,
    global_seq: Arc<std::sync::atomic::AtomicU64>,
    startup_instant: std::time::Instant,
    presence_manager: Arc<PresenceManager>,
    ttl_manager: Arc<TtlManager>,
    prom: Arc<Metrics>,
    identity_issuer: Arc<IdentityIssuer>,
    queue_probe: kanal::AsyncSender<PendingCall>,
    admin: Arc<AdminState>,
    schema_registry: Arc<neondb::schema::SchemaRegistry>,
    mut shutdown: watch::Receiver<()>,
) -> Result<()> {
    let addr: SocketAddr = format!("{}:{}", host, port).parse()
        .map_err(|e| neondb::error::NeonDBError::invalid_argument(format!("Invalid metrics address: {}", e)))?;

    let make_service = make_service_fn(move |_| {
        let subs  = subscription_manager.clone();
        let tbl   = tables.clone();
        let reg   = registry.clone();
        let wal   = wal_writer.clone();
        let seq   = global_seq.clone();
        let start = startup_instant;
        let pres  = presence_manager.clone();
        let ttl   = ttl_manager.clone();
        let prom_svc = prom.clone();
        let iss   = identity_issuer.clone();
        let qp    = queue_probe.clone();
        let adm   = admin.clone();
        let sch   = schema_registry.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let subs = subs.clone(); let tbl = tbl.clone();
                let reg = reg.clone();
                let wal  = wal.clone();  let seq = seq.clone();
                let pres = pres.clone(); let ttl = ttl.clone();
                let prom_r = prom_svc.clone();
                let iss_r = iss.clone();
                let qp_r = qp.clone();
                let adm_r = adm.clone();
                let sch_r = sch.clone();
                async move { handle_metrics_request(req, subs, tbl, reg, wal, seq, start, pres, ttl, prom_r, iss_r, qp_r, adm_r, sch_r).await }
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_service);
    log::info!("Admin/metrics on http://{}", addr);
    println!("  Admin console: http://{}/admin", addr);
    server.with_graceful_shutdown(async move { let _ = shutdown.changed().await; }).await
        .map_err(|e| neondb::error::NeonDBError::network_error(format!("Metrics server: {}", e)))
}

fn json_response(value: serde_json::Value) -> Response<Body> {
    let mut r = Response::new(Body::from(value.to_string()));
    r.headers_mut().insert(hyper::header::CONTENT_TYPE, hyper::header::HeaderValue::from_static("application/json"));
    r
}

/// The single-file admin console, embedded at compile time.
const ADMIN_DASHBOARD_HTML: &str = include_str!("admin_dashboard.html");

fn bad_request(msg: String) -> Response<Body> {
    let mut r = json_response(serde_json::json!({ "error": msg }));
    *r.status_mut() = StatusCode::BAD_REQUEST;
    r
}

fn server_error(msg: String) -> Response<Body> {
    let mut r = json_response(serde_json::json!({ "error": msg }));
    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    r
}

/// Minimal percent-decoding for admin query params (UTF-8, lossy on bad bytes).
fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok()
                .and_then(|h| u8::from_str_radix(h, 16).ok());
            if let Some(b) = hex { out.push(b); i += 3; continue; }
        }
        if bytes[i] == b'+' { out.push(b' '); } else { out.push(bytes[i]); }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Gate mutating admin endpoints behind the API key when one is configured.
/// With no NEONDB_API_KEY set (dev mode), all requests pass.
fn admin_auth_check(req: &Request<Body>) -> Option<Response<Body>> {
    let configured = std::env::var("NEONDB_API_KEY").unwrap_or_default();
    if configured.is_empty() { return None; }
    let provided = req.headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim_start_matches("Bearer ")
        .trim();
    if provided == configured { return None; }
    let mut r = json_response(serde_json::json!({
        "error": "Unauthorized: set your API key in the Operations tab"
    }));
    *r.status_mut() = StatusCode::UNAUTHORIZED;
    Some(r)
}

async fn handle_metrics_request(
    req: Request<Body>,
    subscription_manager: Arc<SubscriptionManager>,
    tables: Arc<TableStore>,
    registry: Arc<ReducerRegistry>,
    wal_writer: Arc<BatchedWalWriter>,
    global_seq: Arc<std::sync::atomic::AtomicU64>,
    startup_instant: std::time::Instant,
    presence_manager: Arc<PresenceManager>,
    ttl_manager: Arc<TtlManager>,
    prom: Arc<Metrics>,
    identity_issuer: Arc<IdentityIssuer>,
    queue_probe: kanal::AsyncSender<PendingCall>,
    admin: Arc<AdminState>,
    schema_registry: Arc<neondb::schema::SchemaRegistry>,
) -> Result<Response<Body>> {
    let path = req.uri().path().to_string();

    match (req.method(), path.as_str()) {
        // ── Admin dashboard ───────────────────────────────────────────────────
        //
        // GET  /admin              — embedded single-file web console
        // POST /admin/api/call     — invoke a reducer through the real queue
        // POST /admin/api/sql      — run a SQL query
        // POST /admin/api/row      — upsert a row (durable: WAL + live fan-out)
        // DELETE /admin/api/row    — delete a row (durable: WAL + live fan-out)
        (&Method::GET, "/admin") | (&Method::GET, "/admin/") => {
            let mut r = Response::new(Body::from(ADMIN_DASHBOARD_HTML));
            r.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("text/html; charset=utf-8"),
            );
            Ok(r)
        }

        // ── Drain mode ───────────────────────────────────────────────────────
        // GET    /admin/api/drain — drain status + active connection count
        // POST   /admin/api/drain — enable drain (stop new connections)
        // DELETE /admin/api/drain — disable drain (resume accepting connections)
        (&Method::GET, "/admin/api/drain") => {
            let draining = admin.drain_flag.load(std::sync::atomic::Ordering::Relaxed);
            let conns = admin.active_connections.load(std::sync::atomic::Ordering::Relaxed);
            Ok(json_response(serde_json::json!({
                "draining": draining,
                "active_connections": conns,
                "message": if draining {
                    format!("{} connections still active — new connections refused", conns)
                } else {
                    "Server accepting connections normally".to_string()
                }
            })))
        }

        (&Method::POST, "/admin/api/drain") => {
            if let Some(resp) = admin_auth_check(&req) { return Ok(resp); }
            admin.drain_flag.store(true, std::sync::atomic::Ordering::Relaxed);
            let conns = admin.active_connections.load(std::sync::atomic::Ordering::Relaxed);
            log::warn!("[drain] Drain mode ENABLED — {} active connections finishing", conns);
            Ok(json_response(serde_json::json!({
                "draining": true,
                "active_connections": conns,
                "message": "Drain enabled. New connections refused with HTTP 503. Existing connections unaffected."
            })))
        }

        (&Method::DELETE, "/admin/api/drain") => {
            if let Some(resp) = admin_auth_check(&req) { return Ok(resp); }
            admin.drain_flag.store(false, std::sync::atomic::Ordering::Relaxed);
            let conns = admin.active_connections.load(std::sync::atomic::Ordering::Relaxed);
            log::info!("[drain] Drain mode DISABLED — resuming normal operation");
            Ok(json_response(serde_json::json!({
                "draining": false,
                "active_connections": conns,
                "message": "Drain disabled. Server accepting new connections normally."
            })))
        }

        (&Method::POST, "/admin/api/call") => {
            if let Some(resp) = admin_auth_check(&req) { return Ok(resp); }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(format!("Read body: {}", e)))?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
            };
            let name = match payload.get("name").and_then(|v| v.as_str()) {
                Some(n) if !n.is_empty() => n.to_string(),
                _ => return Ok(bad_request("Missing 'name' field".into())),
            };
            let args_val = payload.get("args").cloned().unwrap_or(serde_json::json!([]));
            let args_bytes = rmp_serde::to_vec(&args_val)
                .map_err(|e| neondb::error::NeonDBError::reducer_error(format!("Args encode: {}", e)))?;

            // Dispatch through the real reducer queue so the call gets the
            // identical execution path as a WebSocket client (permissions
            // excepted — this endpoint is admin-gated above).
            let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel();
            let call = PendingCall {
                call_id: 0,
                reducer_name: name,
                args: args_bytes,
                caller_id: "admin-console".to_string(),
                caller_role: "admin".to_string(),
                tenant_id: None,
                lobby_hint: None,
                response_tx: resp_tx,
            };
            if queue_probe.send(call).await.is_err() {
                return Ok(server_error("Reducer queue closed".into()));
            }
            match tokio::time::timeout(std::time::Duration::from_secs(30), resp_rx.recv()).await {
                Ok(Some(resp)) => {
                    let result_json: serde_json::Value = resp.result.as_deref()
                        .and_then(|b| rmp_serde::from_slice(b).ok())
                        .unwrap_or(serde_json::Value::Null);
                    Ok(json_response(serde_json::json!({
                        "success": resp.success,
                        "result": result_json,
                        "error": resp.error,
                    })))
                }
                Ok(None) => Ok(server_error("Worker dropped response channel".into())),
                Err(_) => Ok(server_error("Reducer call timed out after 30s".into())),
            }
        }

        (&Method::POST, "/admin/api/sql") => {
            if let Some(resp) = admin_auth_check(&req) { return Ok(resp); }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(format!("Read body: {}", e)))?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
            };
            let query = match payload.get("query").and_then(|v| v.as_str()) {
                Some(q) if !q.trim().is_empty() => q.to_string(),
                _ => return Ok(bad_request("Missing 'query' field".into())),
            };
            let tbl = tables.clone();
            let result = tokio::task::spawn_blocking(move || -> std::result::Result<_, String> {
                let stmt = neondb::sql::parser::parse(&query).map_err(|e| format!("Parse error: {}", e))?;
                let exec = neondb::SqlExecutor::new(tbl);
                exec.execute_statement(&stmt).map_err(|e| format!("Execution error: {}", e))
            }).await;
            match result {
                Ok(Ok(res)) => {
                    let rows: Vec<serde_json::Value> =
                        res.rows.into_iter().map(serde_json::Value::Object).collect();
                    Ok(json_response(serde_json::json!({
                        "columns": res.columns,
                        "rows": rows,
                        "rows_affected": res.rows_affected,
                    })))
                }
                Ok(Err(e)) => Ok(bad_request(e)),
                Err(e) => Ok(server_error(format!("task: {}", e))),
            }
        }

        (&Method::POST, "/admin/api/row") => {
            if let Some(resp) = admin_auth_check(&req) { return Ok(resp); }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(format!("Read body: {}", e)))?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
            };
            let (table, rkey, data) = match (
                payload.get("table").and_then(|v| v.as_str()),
                payload.get("key").and_then(|v| v.as_str()),
                payload.get("data"),
            ) {
                (Some(t), Some(k), Some(d)) if !t.is_empty() && !k.is_empty() =>
                    (t.to_string(), k.to_string(), d.clone()),
                _ => return Ok(bad_request("Expected {table, key, data}".into())),
            };
            match tables.set_row(table.clone(), rkey.clone(), data) {
                Ok(delta) => {
                    // Durable + live: fan out to subscribers and journal to WAL,
                    // exactly like a reducer write (unlike /seed).
                    let deltas = vec![delta];
                    subscription_manager.publish_deltas(&deltas);
                    let seq = global_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let entry = neondb::WalEntry::new(
                        current_timestamp_nanos(), seq,
                        "__admin_set_row".to_string(), vec![], deltas,
                    );
                    if let Err(e) = wal_writer.append(&entry, seq) {
                        log::warn!("[admin] WAL append failed: {}", e);
                    }
                    Ok(json_response(serde_json::json!({ "ok": true, "table": table, "key": rkey })))
                }
                Err(e) => Ok(bad_request(e.to_string())),
            }
        }

        (&Method::DELETE, "/admin/api/row") => {
            if let Some(resp) = admin_auth_check(&req) { return Ok(resp); }
            let query = req.uri().query().unwrap_or("");
            let mut table = String::new(); let mut rkey = String::new();
            for pair in query.split('&') {
                let mut kv = pair.splitn(2, '=');
                match (kv.next(), kv.next()) {
                    (Some("table"), Some(v)) => table = url_decode(v),
                    (Some("key"),   Some(v)) => rkey = url_decode(v),
                    _ => {}
                }
            }
            if table.is_empty() || rkey.is_empty() {
                return Ok(bad_request("Expected ?table=X&key=Y".into()));
            }
            match tables.delete_row(&table, &rkey) {
                Ok(delta) => {
                    let deltas = vec![delta];
                    subscription_manager.publish_deltas(&deltas);
                    let seq = global_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let entry = neondb::WalEntry::new(
                        current_timestamp_nanos(), seq,
                        "__admin_delete_row".to_string(), vec![], deltas,
                    );
                    if let Err(e) = wal_writer.append(&entry, seq) {
                        log::warn!("[admin] WAL append failed: {}", e);
                    }
                    Ok(json_response(serde_json::json!({ "ok": true })))
                }
                Err(e) => Ok(bad_request(e.to_string())),
            }
        }

        // ── Tenant management endpoints ───────────────────────────────────────
        //
        // GET    /admin/api/tenants         — list all tenants (keys masked)
        // POST   /admin/api/tenants         — create a tenant
        // DELETE /admin/api/tenants?id=<id> — delete a tenant and ALL its data

        (&Method::GET, "/admin/api/tenants") => {
            if let Some(resp) = admin_auth_check(&req) { return Ok(resp); }
            Ok(json_response(admin.tenant_registry.summary_json(false)))
        }

        (&Method::POST, "/admin/api/tenants") => {
            if let Some(resp) = admin_auth_check(&req) { return Ok(resp); }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(format!("Read body: {}", e)))?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
            };
            let name = match payload.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => return Ok(bad_request("Missing 'name' field".into())),
            };
            let max_rows = payload.get("max_rows").and_then(|v| v.as_u64()).unwrap_or(0);
            let max_calls = payload.get("max_calls_per_sec").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

            match admin.tenant_registry.create(&name, max_rows, max_calls) {
                Ok((info, delta)) => {
                    // Durably persist: publish + WAL append.
                    let deltas = vec![delta];
                    subscription_manager.publish_deltas(&deltas);
                    let seq = global_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let entry = neondb::WalEntry::new(
                        current_timestamp_nanos(), seq,
                        "__admin_create_tenant".to_string(), vec![], deltas,
                    );
                    let _ = wal_writer.append(&entry, seq);
                    Ok(json_response(serde_json::json!({
                        "ok": true,
                        "id": info.id,
                        "api_key": info.api_key,
                        "name": info.name,
                    })))
                }
                Err(e) => Ok(bad_request(e.to_string())),
            }
        }

        (&Method::DELETE, "/admin/api/tenants") => {
            if let Some(resp) = admin_auth_check(&req) { return Ok(resp); }
            let query = req.uri().query().unwrap_or("");
            let tenant_id = query.split('&')
                .filter_map(|p| {
                    let mut kv = p.splitn(2, '=');
                    if kv.next() == Some("id") { kv.next().map(url_decode) } else { None }
                })
                .next()
                .unwrap_or_default();
            if tenant_id.is_empty() {
                return Ok(bad_request("Expected ?id=<tenant_id>".into()));
            }
            match admin.tenant_registry.delete(&tenant_id) {
                Ok(deltas) => {
                    subscription_manager.publish_deltas(&deltas);
                    let seq = global_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let entry = neondb::WalEntry::new(
                        current_timestamp_nanos(), seq,
                        "__admin_delete_tenant".to_string(), vec![], deltas,
                    );
                    let _ = wal_writer.append(&entry, seq);
                    Ok(json_response(serde_json::json!({ "ok": true })))
                }
                Err(e) => Ok(bad_request(e.to_string())),
            }
        }

        // ── Replication endpoints ─────────────────────────────────────────────
        //
        // GET  /replication/wal?from_seq=N&max=M — primary serves WAL entries
        // GET  /replication/status              — role + lag info
        // POST /replication/promote             — replica → primary failover
        (&Method::GET, "/replication/wal") => {
            let query = req.uri().query().unwrap_or("");
            let mut from_seq = 0u64;
            let mut max = 2048usize;
            for pair in query.split('&') {
                let mut kv = pair.splitn(2, '=');
                match (kv.next(), kv.next()) {
                    (Some("from_seq"), Some(v)) => from_seq = v.parse().unwrap_or(0),
                    (Some("max"), Some(v))      => max = v.parse::<usize>().unwrap_or(2048).clamp(1, 8192),
                    _ => {}
                }
            }
            let wal_path = admin.wal_path.clone();
            let result = tokio::task::spawn_blocking(move || {
                neondb::replication::serve_wal_entries(&wal_path, from_seq, max)
            }).await;
            match result {
                Ok(Ok((entries, last_seq))) => Ok(json_response(serde_json::json!({
                    "entries": neondb::replication::encode_entries(&entries),
                    "last_seq": last_seq,
                }))),
                Ok(Err(e)) => {
                    let mut r = json_response(serde_json::json!({ "error": e.to_string() }));
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR; Ok(r)
                }
                Err(e) => {
                    let mut r = json_response(serde_json::json!({ "error": format!("task: {}", e) }));
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR; Ok(r)
                }
            }
        }

        (&Method::GET, "/replication/status") => {
            Ok(json_response(neondb::replication::status_json()))
        }

        (&Method::POST, "/replication/promote") => {
            let was_replica = neondb::replication::is_replica();
            neondb::replication::set_replica(false);
            if was_replica {
                log::warn!("[replication] PROMOTED to primary via /replication/promote");
            }
            Ok(json_response(serde_json::json!({
                "promoted": was_replica,
                "role": "primary",
                "last_applied_seq": neondb::replication::last_applied_seq(),
            })))
        }

        // ── Cluster endpoints ─────────────────────────────────────────────────
        //
        // GET  /cluster/health  — liveness probe for gossip heartbeats
        // GET  /cluster/peers   — current peer list + health + config
        // POST /cluster/deltas  — receive replicated RowDeltas from a peer
        // POST /cluster/call    — execute a proxied reducer call
        // POST /cluster/join    — register a new peer dynamically
        (&Method::GET, "/cluster/health") => {
            Ok(json_response(serde_json::json!({
                "ok": true,
                "shard_id": admin.cluster_bus.config.my_shard_id,
            })))
        }

        (&Method::GET, "/cluster/peers") => {
            let bus = &admin.cluster_bus;
            Ok(json_response(serde_json::json!({
                "cluster_enabled": bus.is_active(),
                "my_shard_id":     bus.config.my_shard_id,
                "shard_count":     bus.config.shard_count,
                "peers":           bus.peers_snapshot(),
            })))
        }

        (&Method::POST, "/cluster/deltas") => {
            let secret = req.headers().get("x-neondb-cluster-secret")
                .and_then(|v| v.to_str().ok());
            if !admin.cluster_bus.validate_secret(secret) {
                let mut r = json_response(serde_json::json!({ "error": "Unauthorized" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(e.to_string()))?;
            match neondb::cluster::fanout::parse_delta_payload(&body_bytes) {
                Err(e) => Ok(bad_request(e.to_string())),
                Ok(payload) => {
                    let row_deltas = neondb::cluster::fanout::wire_to_row_deltas(payload.deltas);
                    let applied = row_deltas.len();
                    match neondb::cluster::ClusterBus::apply_peer_deltas(&row_deltas, &tables, &subscription_manager) {
                        Ok(()) => Ok(json_response(serde_json::json!({ "ok": true, "applied": applied }))),
                        Err(e) => Ok(server_error(e.to_string())),
                    }
                }
            }
        }

        (&Method::POST, "/cluster/call") => {
            let secret = req.headers().get("x-neondb-cluster-secret")
                .and_then(|v| v.to_str().ok());
            if !admin.cluster_bus.validate_secret(secret) {
                let mut r = json_response(serde_json::json!({ "error": "Unauthorized" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(e.to_string()))?;
            let pr: neondb::cluster::proxy::ProxyCallRequest = match serde_json::from_slice(&body_bytes) {
                Ok(r) => r,
                Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
            };
            use base64::Engine as _;
            let args = match base64::engine::general_purpose::STANDARD.decode(&pr.args_b64) {
                Ok(b) => b,
                Err(e) => return Ok(bad_request(format!("Bad args_b64: {}", e))),
            };
            let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel();
            let call = PendingCall {
                call_id: 0,
                reducer_name: pr.reducer_name,
                args,
                caller_id: pr.caller_id,
                caller_role: pr.caller_role,
                tenant_id: None,
                lobby_hint: None,
                response_tx: resp_tx,
            };
            if queue_probe.send(call).await.is_err() {
                return Ok(server_error("Reducer queue closed".into()));
            }
            match tokio::time::timeout(std::time::Duration::from_secs(30), resp_rx.recv()).await {
                Ok(Some(resp)) => {
                    if resp.success {
                        use base64::Engine as _;
                        let result_b64 = resp.result.as_deref()
                            .map(|b| base64::engine::general_purpose::STANDARD.encode(b))
                            .unwrap_or_default();
                        Ok(json_response(serde_json::json!({ "ok": true, "result_b64": result_b64 })))
                    } else {
                        Ok(json_response(serde_json::json!({
                            "ok": false,
                            "error": resp.error.unwrap_or_else(|| "Reducer error".to_string()),
                        })))
                    }
                }
                Ok(None) => Ok(server_error("Worker dropped response channel".into())),
                Err(_) => Ok(server_error("Proxied call timed out after 30s".into())),
            }
        }

        (&Method::POST, "/cluster/join") => {
            let secret = req.headers().get("x-neondb-cluster-secret")
                .and_then(|v| v.to_str().ok());
            if !admin.cluster_bus.validate_secret(secret) {
                let mut r = json_response(serde_json::json!({ "error": "Unauthorized" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(e.to_string()))?;
            let node: neondb::cluster::NodeInfo = match serde_json::from_slice(&body_bytes) {
                Ok(n) => n,
                Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
            };
            admin.cluster_bus.add_peer(node);
            Ok(json_response(serde_json::json!({
                "ok": true,
                "peers": admin.cluster_bus.peers_snapshot(),
            })))
        }

        // ── Region + lobby-route endpoints ────────────────────────────────────

        // GET /cluster/regions — list all known regions
        (&Method::GET, "/cluster/regions") => {
            let regions = admin.region_registry.all();
            Ok(json_response(serde_json::json!({
                "my_region": admin.region_registry.my_region,
                "regions":   regions,
                "multi_region": admin.region_registry.is_multi_region(),
            })))
        }

        // GET /cluster/lobby-route?lobby_id=42
        // Returns { region_id, ws_url } for the lobby or 404 if unknown.
        (&Method::GET, p) if p.starts_with("/cluster/lobby-route") => {
            let lobby_id = req.uri().query()
                .and_then(|q| q.split('&').find(|s| s.starts_with("lobby_id=")))
                .and_then(|s| s.strip_prefix("lobby_id="))
                .unwrap_or("");
            if lobby_id.is_empty() {
                return Ok(bad_request("Missing lobby_id query param".into()));
            }
            match admin.lobby_routes.lookup(lobby_id) {
                Some(route) => Ok(json_response(serde_json::json!({
                    "lobby_id":  route.lobby_id,
                    "region_id": route.region_id,
                    "ws_url":    route.ws_url,
                }))),
                None => {
                    // Unknown lobby — assume it lives here (single-region fallback).
                    let ws_url = admin.region_registry
                        .ws_url_for(&admin.region_registry.my_region)
                        .unwrap_or_default();
                    Ok(json_response(serde_json::json!({
                        "lobby_id":  lobby_id,
                        "region_id": admin.region_registry.my_region,
                        "ws_url":    ws_url,
                        "fallback":  true,
                    })))
                }
            }
        }

        // POST /cluster/register-lobby — { lobby_id, region_id?, ws_url? }
        // Called by game code after a lobby is created.
        (&Method::POST, "/cluster/register-lobby") => {
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(e.to_string()))?;
            let v: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
            };
            let lobby_id  = v["lobby_id"].as_str().unwrap_or("").to_string();
            if lobby_id.is_empty() {
                return Ok(bad_request("Missing lobby_id".into()));
            }
            let region_id = v["region_id"].as_str()
                .unwrap_or(&admin.region_registry.my_region)
                .to_string();
            let ws_url = v["ws_url"].as_str().map(|s| s.to_string())
                .or_else(|| admin.region_registry.get(&region_id).map(|r| r.ws_url.clone()))
                .unwrap_or_default();
            admin.lobby_routes.register(&lobby_id, &region_id, &ws_url);
            Ok(json_response(serde_json::json!({ "ok": true, "lobby_id": lobby_id, "region_id": region_id })))
        }

        // DELETE /cluster/lobby-route?lobby_id=42 — remove a lobby route
        (&Method::DELETE, p) if p.starts_with("/cluster/lobby-route") => {
            let lobby_id = req.uri().query()
                .and_then(|q| q.split('&').find(|s| s.starts_with("lobby_id=")))
                .and_then(|s| s.strip_prefix("lobby_id="))
                .unwrap_or("");
            admin.lobby_routes.unregister(lobby_id);
            Ok(json_response(serde_json::json!({ "ok": true })))
        }

        // ── Leaderboard endpoints ─────────────────────────────────────────────

        // GET /leaderboard/top?board=leaderboard&n=100
        (&Method::GET, p) if p.starts_with("/leaderboard/top") => {
            let query = req.uri().query().unwrap_or("");
            let board = query.split('&')
                .find(|s| s.starts_with("board="))
                .and_then(|s| s.strip_prefix("board="))
                .unwrap_or("leaderboard");
            let n: usize = query.split('&')
                .find(|s| s.starts_with("n="))
                .and_then(|s| s.strip_prefix("n="))
                .and_then(|s| s.parse().ok())
                .unwrap_or(100);
            let result = neondb::leaderboard::http_top_entries(&admin.leaderboard, board, n);
            Ok(json_response(result))
        }

        // ── Post-match stat-sync endpoint ─────────────────────────────────────

        // POST /cluster/stat-sync — receive stat write-back jobs from other regions
        (&Method::POST, "/cluster/stat-sync") => {
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(e.to_string()))?;
            let result = neondb::stat_sync::handle_stat_sync(&tables, &body_bytes);
            Ok(json_response(result))
        }

        // ── Backup endpoint ───────────────────────────────────────────────────
        (&Method::POST, "/backup") => {
            let Some(backup_dir) = admin.backup_dir.clone() else {
                let mut r = json_response(serde_json::json!({
                    "error": "No backup directory configured. Set NEONDB_BACKUP_DIR or [server] backup_dir."
                }));
                *r.status_mut() = StatusCode::BAD_REQUEST;
                return Ok(r);
            };
            let tbl = tables.clone();
            let wal_path = admin.wal_path.clone();
            let keep = admin.backup_keep;
            let last_seq = global_seq.load(std::sync::atomic::Ordering::Relaxed);
            let result = tokio::task::spawn_blocking(move || {
                let path = neondb::backup::backup_now(&tbl, &wal_path, &backup_dir, last_seq)?;
                let _ = neondb::backup::rotate_backups(&backup_dir, keep);
                Ok::<_, neondb::error::NeonDBError>(path)
            }).await;
            match result {
                Ok(Ok(path)) => {
                    let meta = neondb::backup::read_meta(&path);
                    Ok(json_response(serde_json::json!({
                        "path": path.to_string_lossy(),
                        "last_seq": last_seq,
                        "row_count": meta.map(|m| m.row_count).unwrap_or(0),
                    })))
                }
                Ok(Err(e)) => {
                    let mut r = json_response(serde_json::json!({ "error": e.to_string() }));
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR; Ok(r)
                }
                Err(e) => {
                    let mut r = json_response(serde_json::json!({ "error": format!("task: {}", e) }));
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR; Ok(r)
                }
            }
        }

        (&Method::GET, "/metrics") => {
            // Prometheus exposition format (text/plain; version=0.0.4)
            let body = prom.render();
            let mut r = Response::new(Body::from(body));
            r.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("text/plain; version=0.0.4"),
            );
            Ok(r)
        }

        (&Method::GET, "/healthz") => Ok(json_response(serde_json::json!({
            "status": "ok",
            "role": if neondb::replication::is_replica() { "replica" } else { "primary" },
            "replication_lag_entries": neondb::replication::replication_lag(),
            "total_rows": tables.total_row_count(),
            "active_connections": subscription_manager.active_connections(),
            "active_subscriptions": subscription_manager.active_subscriptions(),
            "wal_sequence": global_seq.load(std::sync::atomic::Ordering::Relaxed),
            "wal_file_size_bytes": wal_writer.wal_file_size_bytes(),
            "uptime_seconds": startup_instant.elapsed().as_secs(),
            "reducer_queue_depth": queue_probe.len(),
            "memory_usage_bytes": get_memory_usage_bytes(),
            "presence_tracked": presence_manager.count(),
            "ttl_active": ttl_manager.count(),
        }))),

        (&Method::GET, "/stats") => {
            let table_list: Vec<_> = tables.list_tables().into_iter().map(|name| {
                let count = tables.list_rows_with_keys(&name).map(|r| r.len()).unwrap_or(0);
                let indexes = tables.list_indexes(&name);
                serde_json::json!({ "name": name, "rows": count, "indexes": indexes })
            }).collect();
            let indexes: Vec<_> = tables.list_tables().into_iter().flat_map(|name| {
                tables.list_indexes(&name).into_iter().map(move |field| {
                    serde_json::json!({ "table": name.clone(), "field": field })
                })
            }).collect();
            Ok(json_response(serde_json::json!({
                "tables": table_list,
                "total_rows": tables.total_row_count(),
                "indexes": indexes,
                "wal_sequence": global_seq.load(std::sync::atomic::Ordering::Relaxed),
                "wal_file_size_bytes": wal_writer.wal_file_size_bytes(),
                "snapshot_last_seq": 0u64, // Not easily queryable without scanning snapshot dir
            })))
        },

        (&Method::POST, "/seed") => {
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(format!("Read body: {}", e)))?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => {
                    let mut r = json_response(serde_json::json!({ "error": format!("Invalid JSON: {}", e) }));
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };
            let row_arr = match payload.get("rows").and_then(|v| v.as_array()) {
                Some(a) => a.clone(),
                None => {
                    let mut r = json_response(serde_json::json!({ "error": "Expected {\"rows\": [...]}" }));
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };
            let mut rows_written = 0usize; let mut rows_skipped = 0usize; let mut errors = Vec::new();
            for (i, item) in row_arr.iter().enumerate() {
                let triple = match item.as_array() {
                    Some(t) if t.len() == 3 => t,
                    _ => { errors.push(format!("rows[{}]: expected [table, key, data]", i)); rows_skipped += 1; continue; }
                };
                let table = match triple[0].as_str() { Some(s) => s.to_string(), None => { errors.push(format!("rows[{}]: table must be string", i)); rows_skipped += 1; continue; } };
                let key   = match triple[1].as_str() { Some(s) => s.to_string(), None => { errors.push(format!("rows[{}]: key must be string", i)); rows_skipped += 1; continue; } };
                match tables.set_row(table.clone(), key.clone(), triple[2].clone()) {
                    Ok(_)  => rows_written += 1,
                    Err(e) => { errors.push(format!("rows[{}] ({}.{}): {}", i, table, key, e)); rows_skipped += 1; }
                }
            }
            let mut body = serde_json::json!({ "rows_written": rows_written, "rows_skipped": rows_skipped });
            if !errors.is_empty() { body["errors"] = serde_json::Value::Array(errors.into_iter().map(serde_json::Value::String).collect()); }
            let status = if rows_skipped > 0 && rows_written == 0 { StatusCode::BAD_REQUEST } else { StatusCode::OK };
            let mut r = json_response(body); *r.status_mut() = status; Ok(r)
        }

        (&Method::POST, "/migrate") => {
            // Accepts: {"migrations": [{"filename": "001_add_score.toml", "content": "<toml>"}]}
            // Applies each migration via apply_migrations_inline(); returns applied/skipped/errors.
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(format!("Read body: {}", e)))?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => {
                    let mut r = json_response(serde_json::json!({ "error": format!("Invalid JSON: {}", e) }));
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };
            let mig_arr = match payload.get("migrations").and_then(|v| v.as_array()) {
                Some(a) => a.clone(),
                None => {
                    let mut r = json_response(serde_json::json!({ "error": "Expected {\"migrations\": [...]}" }));
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };
            let mut applied = 0usize;
            let mut skipped = 0usize;
            let mut errors: Vec<String> = Vec::new();
            for entry in &mig_arr {
                let filename = match entry.get("filename").and_then(|v| v.as_str()) {
                    Some(f) => f.to_string(),
                    None => { errors.push("missing filename field".to_string()); skipped += 1; continue; }
                };
                let content = match entry.get("content").and_then(|v| v.as_str()) {
                    Some(c) => c.to_string(),
                    None => { errors.push(format!("{}: missing content field", filename)); skipped += 1; continue; }
                };
                match neondb::migrations::apply_migration_str(&filename, &content, &tables) {
                    Ok(true)  => applied += 1,
                    Ok(false) => skipped += 1,
                    Err(e)    => { errors.push(format!("{}: {}", filename, e)); skipped += 1; }
                }
            }
            let mut body = serde_json::json!({ "applied": applied, "skipped": skipped });
            if !errors.is_empty() {
                body["errors"] = serde_json::Value::Array(errors.into_iter().map(serde_json::Value::String).collect());
            }
            Ok(json_response(body))
        }

        (&Method::GET, "/schema") => {
            // Full machine-readable schema — used by `neondb generate`.
            // Tables: from SchemaRegistry (column defs) merged with live table list.
            let mut table_map = serde_json::Map::new();
            // First include all registered schemas with full column info.
            for table_name in schema_registry.list_tables() {
                if let Some(schema) = schema_registry.get(table_name) {
                    let cols: Vec<_> = schema.columns.iter().map(|c| serde_json::json!({
                        "name": c.name,
                        "type": c.type_str,
                        "required": c.required,
                        "default": c.default,
                        "key": schema.primary_key.as_deref() == Some(&c.name),
                    })).collect();
                    let rows = tables.list_rows_with_keys(table_name).map(|r| r.len()).unwrap_or(0);
                    table_map.insert(table_name.to_string(), serde_json::json!({
                        "columns": cols,
                        "primary_key": schema.primary_key,
                        "rls": format!("{:?}", schema.rls),
                        "rows": rows,
                    }));
                }
            }
            // Also include live tables that have no schema registered (open schema).
            for table_name in tables.list_tables() {
                if !table_map.contains_key(&table_name) {
                    let rows = tables.list_rows_with_keys(&table_name).map(|r| r.len()).unwrap_or(0);
                    table_map.insert(table_name, serde_json::json!({ "columns": [], "rows": rows }));
                }
            }
            let reducer_list: Vec<_> = registry.list_reducers();
            Ok(json_response(serde_json::json!({
                "tables": serde_json::Value::Object(table_map),
                "reducers": reducer_list,
                "version": env!("CARGO_PKG_VERSION"),
            })))
        }

        (&Method::GET, "/tables") => {
            let list: Vec<_> = tables.list_tables().into_iter().map(|name| {
                let count = tables.list_rows_with_keys(&name).map(|r| r.len()).unwrap_or(0);
                serde_json::json!({ "name": name, "rows": count })
            }).collect();
            Ok(json_response(serde_json::json!({ "tables": list, "total_rows": tables.total_row_count() })))
        }

        (&Method::GET, p) if p.starts_with("/tables/") => {
            let table_name = p.trim_start_matches("/tables/");
            match tables.list_rows_with_keys(table_name) {
                Ok(rows) => {
                    let row_objs: Vec<_> = rows.into_iter()
                        .map(|(key, data)| serde_json::json!({ "row_key": key, "data": data }))
                        .collect();
                    Ok(json_response(serde_json::json!({ "table": table_name, "count": row_objs.len(), "rows": row_objs })))
                }
                Err(e) => {
                    let mut r = json_response(serde_json::json!({ "error": e.to_string() }));
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR; Ok(r)
                }
            }
        }

        // ── Identity / JWT endpoints ──────────────────────────────────────────
        //
        // POST /auth/token  — issue a signed JWT (requires valid API key auth)
        // GET  /auth/public-key — return the server's Ed25519 public key PEM
        //   (no auth required — clients need this to verify tokens independently)

        (&Method::POST, "/auth/token") => {
            // Gate: require a valid API key in the Authorization header.
            // This endpoint is intentionally admin-only; the API key acts as
            // the bootstrap credential that mints user-facing JWTs.
            let auth_header = req
                .headers()
                .get(hyper::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if !auth_header.starts_with("Bearer ") {
                let mut r = json_response(serde_json::json!({ "error": "Unauthorized: missing Authorization header" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            // Accept any non-empty token as an API key; the operator controls
            // access by keeping the NEONDB_API_KEY secret.
            let provided_key = auth_header.trim_start_matches("Bearer ").trim();
            let api_key_configured = std::env::var("NEONDB_API_KEY").unwrap_or_default();
            if !api_key_configured.is_empty() && provided_key != api_key_configured {
                let mut r = json_response(serde_json::json!({ "error": "Unauthorized: invalid API key" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }

            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(format!("Read body: {}", e)))?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => {
                    let mut r = json_response(serde_json::json!({ "error": format!("Invalid JSON: {}", e) }));
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };

            let identity = match payload.get("identity").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => {
                    let mut r = json_response(serde_json::json!({ "error": "Missing or empty 'identity' field" }));
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };
            let roles: Vec<String> = payload
                .get("roles")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();
            let ttl_secs = payload
                .get("ttl_seconds")
                .and_then(|v| v.as_u64())
                .unwrap_or(3600);

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let expires_at = now + ttl_secs;

            match identity_issuer.issue(&identity, roles, ttl_secs) {
                Ok(token) => Ok(json_response(serde_json::json!({
                    "token": token,
                    "identity": identity,
                    "expires_at": expires_at,
                }))),
                Err(e) => {
                    let mut r = json_response(serde_json::json!({ "error": format!("Token issuance failed: {}", e) }));
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(r)
                }
            }
        }

        (&Method::GET, "/auth/public-key") => {
            let pem = identity_issuer.public_key_pem();
            Ok(json_response(serde_json::json!({ "public_key_pem": pem })))
        }

        _ => {
            let mut r = Response::new(Body::from("Not Found"));
            *r.status_mut() = StatusCode::NOT_FOUND; Ok(r)
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn current_timestamp_nanos() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

/// Best-effort memory usage query (WorkingSetSize on Windows, /proc/self/statm on Linux).
/// Returns 0 if the platform does not support the query or if parsing fails.
fn get_memory_usage_bytes() -> u64 {
    #[cfg(target_os = "windows")]
    {
        // Use GetProcessMemoryInfo via psapi — no child process, no wmic (deprecated Win11).
        use std::mem;
        #[allow(non_camel_case_types)]
        type HANDLE = *mut std::ffi::c_void;
        #[allow(non_camel_case_types)]
        type DWORD = u32;
        #[allow(non_camel_case_types)]
        type SIZE_T = usize;
        #[repr(C)]
        struct PROCESS_MEMORY_COUNTERS {
            cb: DWORD,
            page_fault_count: DWORD,
            peak_working_set_size: SIZE_T,
            working_set_size: SIZE_T,
            quota_peak_paged_pool_usage: SIZE_T,
            quota_paged_pool_usage: SIZE_T,
            quota_peak_non_paged_pool_usage: SIZE_T,
            quota_non_paged_pool_usage: SIZE_T,
            pagefile_usage: SIZE_T,
            peak_pagefile_usage: SIZE_T,
        }
        #[link(name = "kernel32")]
        extern "system" {
            fn GetCurrentProcess() -> HANDLE;
        }
        #[link(name = "psapi")]
        extern "system" {
            fn GetProcessMemoryInfo(
                process: HANDLE,
                ppsmemcounters: *mut PROCESS_MEMORY_COUNTERS,
                cb: DWORD,
            ) -> i32;
        }
        unsafe {
            let mut pmc: PROCESS_MEMORY_COUNTERS = mem::zeroed();
            pmc.cb = mem::size_of::<PROCESS_MEMORY_COUNTERS>() as DWORD;
            if GetProcessMemoryInfo(GetCurrentProcess(), &mut pmc, pmc.cb) != 0 {
                return pmc.working_set_size as u64;
            }
        }
        0
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(data) = std::fs::read_to_string("/proc/self/statm") {
            // statm fields are in pages; second field is resident set size
            if let Some(rss_pages) = data.split_whitespace().nth(1) {
                if let Ok(pages) = rss_pages.parse::<u64>() {
                    return pages * 4096; // Assume 4KB page size
                }
            }
        }
        0
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    {
        0
    }
}

fn recover_from_wal(wal_path: &Path, tables: &Arc<TableStore>, min_seq: u64) -> Result<(usize, u64)> {
    let mut reader = WalReader::open(wal_path)?;
    let entries = reader.read_all_entries()?;
    let mut replayed = 0usize; let mut max_seq = min_seq;
    for entry in &entries {
        max_seq = max_seq.max(entry.header.sequence_number);
        if entry.header.sequence_number <= min_seq { continue; }
        if !entry.verify_checksum() { log::warn!("WAL entry {} bad checksum, skipping", entry.header.sequence_number); continue; }
        for delta in &entry.payload.deltas { tables.apply_delta(delta)?; }
        replayed += 1;
    }
    Ok((replayed, max_seq))
}
