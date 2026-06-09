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
use openraft;

use neondb::{
    auth::{AuthValidator, IdentityIssuer},
    config::{Config, ScheduledReducerConfig},
    cluster::{ClusterBus, ClusterConfig, NodeInfo, PeerEntry},
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
    Template { name: "rust/basic",      category: "Rust server", description: "Foundation — users, sessions, inventory, role-based auth  (JS reducers → WASM-upgradable)" },
    Template { name: "rust/game-ready", category: "Rust server", description: "Game-ready engine — players, combat, economy, quests, guilds, world  (JS reducers → WASM-upgradable)" },
    Template { name: "rust/chat",       category: "Rust server", description: "Production chat — rooms, threads, reactions, presence, moderation  (JS reducers → WASM-upgradable)" },
    Template { name: "typescript",      category: "TypeScript",  description: "TypeScript-first — React hooks, full client SDK, package.json scaffolding" },
    Template { name: "native/game-ready", category: "Native Rust", description: "Rust reducers compiled to WASM — near-native throughput, no NeonDB source needed" },
];

// ─────────────────────────────────────────────────────────────────────────────
// CLI definition
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "neondb")]
#[command(author, version, about = "NeonDB — self-hosted real-time game backend")]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Scaffold a new NeonDB project (interactive when run with no args)
    Init {
        #[arg(value_name = "NAME")]
        path: Option<PathBuf>,
        #[arg(long, help = "Template: rust/basic | rust/game-ready | rust/chat | typescript")]
        template: Option<String>,
    },
    /// List available project templates
    Templates,
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
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Init { path, template } => { init_project(path, template)?; Ok(()) }
        Commands::Templates => { cmd_list_templates(); Ok(()) }
        Commands::Build { modules_dir } => {
            build_wasm_modules(modules_dir.as_deref().unwrap_or(Path::new("modules")))
        }
        Commands::Start { host, port, data_dir, wal_path, fsync_interval_ms } => {
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
        Commands::GenerateNpc { npc_type, context, url, api_key } => neondb::cli::cmd_generate_npc(&url, &npc_type, context.as_deref(), api_key.as_deref()).await,
        Commands::Bench { url, clients, calls, warmup, api_key } => run_cli_bench(&url, clients, calls, warmup, api_key.as_deref()).await,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// neondb cluster-status
// ─────────────────────────────────────────────────────────────────────────────

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
// neondb templates
// ─────────────────────────────────────────────────────────────────────────────

fn cmd_list_templates() {
    println!();
    println!("  NeonDB Project Templates");
    println!();
    let mut last_cat = "";
    for t in TEMPLATES {
        if t.category != last_cat {
            println!("  ── {} ─────────────────────────────────────────", t.category);
            last_cat = t.category;
        }
        println!("    {:22}  {}", t.name, t.description);
    }
    println!();
    println!("  Usage:");
    println!("    neondb init my-project --template rust/basic");
    println!("    neondb init my-game    --template rust/game-ready");
    println!("    neondb init my-chat    --template rust/chat");
    println!("    neondb init my-ts-app  --template typescript");
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
            let options: Vec<String> = TEMPLATES.iter()
                .map(|t| format!("{:22} — {}", t.name, t.description))
                .collect();
            let selection = Select::with_theme(&theme)
                .with_prompt("Select a template")
                .default(0)
                .items(&options)
                .interact()
                .map_err(|e| neondb::error::NeonDBError::internal(format!("Prompt error: {}", e)))?;
            TEMPLATES[selection].name.to_string()
        }
    };

    fs::create_dir_all(&project_path)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Cannot create directory: {}", e)))?;

    write_shared_files(&project_path, &project_name, &template_name)?;

    match template_name.as_str() {
        "rust/basic"        => scaffold_rust_basic(&project_path, &project_name)?,
        "rust/game-ready"   => scaffold_rust_game_ready(&project_path, &project_name)?,
        "rust/chat"         => scaffold_rust_chat(&project_path, &project_name)?,
        "typescript"        => scaffold_typescript(&project_path, &project_name)?,
        "native/game-ready" => scaffold_native_game_ready(&project_path, &project_name)?,
        _                   => unreachable!(),
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared files (every template)
// ─────────────────────────────────────────────────────────────────────────────

fn write_shared_files(project_path: &Path, project_name: &str, template: &str) -> Result<()> {
    let scheduler_note = match template {
        "rust/game-ready" =>
            "\n[[scheduler]]\nreducer = \"world_tick\"\ninterval_ms = 1000\n\n[[scheduler]]\nreducer = \"cleanup_sessions\"\ninterval_ms = 60000\n\n[[scheduler]]\nreducer = \"refresh\"\ninterval_ms = 5000\n",
        "rust/chat" =>
            "\n[[scheduler]]\nreducer = \"cleanup_expired_presence\"\ninterval_ms = 30000\n",
        _ => "\n# [[scheduler]]\n# reducer = \"cleanup_expired\"\n# interval_ms = 60000\n",
    };

    let permissions_example = match template {
        "rust/basic" | "rust/game-ready" =>
            "\n[permissions]\n# admin-only reducers\ndelete_user       = [\"admin\"]\nban_user          = [\"admin\", \"moderator\"]\ngrant_role        = [\"admin\"]\n",
        "rust/chat" =>
            "\n[permissions]\ndelete_message    = [\"admin\", \"moderator\"]\nban_user          = [\"admin\"]\ndelete_room       = [\"admin\"]\n",
        _ => "\n# [permissions]\n# delete_user = [\"admin\"]\n",
    };

    let toml = format!(
        "[project]\nname = \"{name}\"\nversion = \"0.1.0\"\n\
        [server]\nhost = \"127.0.0.1\"\nport = 3000\nmetrics_port = 3001\n\
        wal_path = \"./wal\"\n\
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

fn scaffold_rust_basic(p: &Path, name: &str) -> Result<()> {
    wf(p, "modules/auth/register.js",          BASIC_REGISTER_JS)?;
    wf(p, "modules/auth/login.js",             BASIC_LOGIN_JS)?;
    wf(p, "modules/auth/logout.js",            BASIC_LOGOUT_JS)?;
    wf(p, "modules/auth/grant_role.js",        BASIC_GRANT_ROLE_JS)?;
    wf(p, "modules/users/update_profile.js",   BASIC_UPDATE_PROFILE_JS)?;
    wf(p, "modules/users/delete_user.js",      BASIC_DELETE_USER_JS)?;
    wf(p, "modules/inventory/add_item.js",     BASIC_ADD_ITEM_JS)?;
    wf(p, "modules/inventory/remove_item.js",  BASIC_REMOVE_ITEM_JS)?;
    wf(p, "modules/subscribers/subscribe_to_player.js", BASIC_SUB_PLAYER_JS)?;
    wf(p, "client/example.ts",                BASIC_CLIENT_TS)?;
    wf(p, "schema.toml",                       BASIC_SCHEMA_TOML)?;
    wf(p, "PERFORMANCE.md",                    PERF_MD)?;
    wf(p, "README.md", &format!("# {} — Basic Template\n\n{}", name, BASIC_README))?;
    print_success(name, "rust/basic", &[
        ("modules/auth/",       "register, login, logout, grant_role"),
        ("modules/users/",      "update_profile, delete_user"),
        ("modules/inventory/",  "add_item, remove_item"),
        ("modules/subscribers/","subscribe_to_player"),
        ("client/example.ts",   "TypeScript client example"),
        ("schema.toml",         "typed column definitions"),
        ("neondb.toml",         "server config + [permissions]"),
    ]);
    println!("  Next steps:\n    cd {name}\n    neondb start\n\n  Upgrade to WASM for production throughput:\n    neondb build          # compiles JS → WASM via Javy (10–50× faster)\n\n  See PERFORMANCE.md for the full native-Rust path.");
    println!();
    Ok(())
}

fn scaffold_rust_game_ready(p: &Path, name: &str) -> Result<()> {
    wf(p, "modules/players/spawn.js",           GAME_SPAWN_JS)?;
    wf(p, "modules/players/despawn.js",         GAME_DESPAWN_JS)?;
    wf(p, "modules/players/move.js",            GAME_MOVE_JS)?;
    wf(p, "modules/players/update_stats.js",    GAME_UPDATE_STATS_JS)?;
    wf(p, "modules/combat/spawn_npc.js",        GAME_SPAWN_NPC_JS)?;
    wf(p, "modules/combat/attack.js",           GAME_ATTACK_JS)?;
    wf(p, "modules/combat/use_ability.js",      GAME_USE_ABILITY_JS)?;
    wf(p, "modules/combat/apply_damage.js",     GAME_APPLY_DAMAGE_JS)?;
    wf(p, "modules/combat/respawn.js",          GAME_RESPAWN_JS)?;
    wf(p, "modules/economy/buy_item.js",        GAME_BUY_ITEM_JS)?;
    wf(p, "modules/economy/sell_item.js",       GAME_SELL_ITEM_JS)?;
    wf(p, "modules/economy/transfer_currency.js", GAME_TRANSFER_CURRENCY_JS)?;
    wf(p, "modules/economy/open_loot_box.js",   GAME_OPEN_LOOT_BOX_JS)?;
    wf(p, "modules/quests/accept_quest.js",     GAME_ACCEPT_QUEST_JS)?;
    wf(p, "modules/quests/complete_quest.js",   GAME_COMPLETE_QUEST_JS)?;
    wf(p, "modules/quests/update_progress.js",  GAME_UPDATE_PROGRESS_JS)?;
    wf(p, "modules/matchmaking/queue.js",       GAME_QUEUE_JS)?;
    wf(p, "modules/matchmaking/dequeue.js",     GAME_DEQUEUE_JS)?;
    wf(p, "modules/matchmaking/create_match.js",GAME_CREATE_MATCH_JS)?;
    wf(p, "modules/matchmaking/refresh.js",     GAME_MATCHMAKING_REFRESH_JS)?;
    wf(p, "modules/guilds/create.js",           GAME_GUILD_CREATE_JS)?;
    wf(p, "modules/guilds/invite.js",           GAME_GUILD_INVITE_JS)?;
    wf(p, "modules/guilds/accept_invite.js",    GAME_GUILD_ACCEPT_JS)?;
    wf(p, "modules/guilds/kick.js",             GAME_GUILD_KICK_JS)?;
    wf(p, "modules/world/world_tick.js",        GAME_WORLD_TICK_JS)?;
    wf(p, "modules/world/cleanup_sessions.js",  GAME_CLEANUP_SESSIONS_JS)?;
    wf(p, "modules/leaderboard/submit_score.js",GAME_SUBMIT_SCORE_JS)?;
    wf(p, "modules/leaderboard/reset_weekly.js",GAME_RESET_WEEKLY_JS)?;
    wf(p, "client/game-client.ts",              GAME_CLIENT_TS)?;
    wf(p, "schema.toml",                        GAME_SCHEMA_TOML)?;
    wf(p, "GENRE_GUIDE.md",                     GAME_GENRE_GUIDE_MD)?;
    wf(p, "PERFORMANCE.md",                    PERF_MD)?;
    wf(p, "seed.json",                          GAME_SEED_JSON)?;
    wf(p, "README.md", &format!("# {} — Game-Ready Template\n\n{}", name, GAME_README))?;
    print_success(name, "rust/game-ready", &[
        ("modules/players/",    "spawn, despawn, move, update_stats"),
        ("modules/combat/",     "spawn_npc, attack, use_ability, apply_damage, respawn"),
        ("modules/economy/",    "buy_item, sell_item, transfer_currency, loot_box"),
        ("modules/quests/",     "accept, complete, update_progress"),
        ("modules/matchmaking/","queue, dequeue, create_match, refresh (scheduled)"),
        ("modules/guilds/",     "create, invite, accept_invite, kick"),
        ("modules/world/",      "world_tick (1s), cleanup_sessions (60s)"),
        ("modules/leaderboard/","submit_score, reset_weekly (scheduled)"),
        ("seed.json",           "neondb seed seed.json  — load sample data instantly"),
        ("GENRE_GUIDE.md",      "how to adapt this to any game genre"),
    ]);
    println!("  Next steps:\n    cd {name}\n    neondb start\n    neondb seed seed.json\n\n  Upgrade to WASM for production throughput:\n    neondb build          # compiles JS → WASM via Javy (10–50× faster)\n\n  See PERFORMANCE.md for the full native-Rust path.");
    println!();
    Ok(())
}

fn scaffold_rust_chat(p: &Path, name: &str) -> Result<()> {
    wf(p, "modules/rooms/create_room.js",       CHAT_CREATE_ROOM_JS)?;
    wf(p, "modules/rooms/join_room.js",         CHAT_JOIN_ROOM_JS)?;
    wf(p, "modules/rooms/leave_room.js",        CHAT_LEAVE_ROOM_JS)?;
    wf(p, "modules/rooms/delete_room.js",       CHAT_DELETE_ROOM_JS)?;
    wf(p, "modules/messages/send_message.js",   CHAT_SEND_MESSAGE_JS)?;
    wf(p, "modules/messages/edit_message.js",   CHAT_EDIT_MESSAGE_JS)?;
    wf(p, "modules/messages/delete_message.js", CHAT_DELETE_MESSAGE_JS)?;
    wf(p, "modules/messages/react.js",          CHAT_REACT_JS)?;
    wf(p, "modules/threads/create_thread.js",   CHAT_CREATE_THREAD_JS)?;
    wf(p, "modules/threads/reply.js",           CHAT_REPLY_JS)?;
    wf(p, "modules/presence/set_online.js",     CHAT_SET_ONLINE_JS)?;
    wf(p, "modules/presence/set_typing.js",     CHAT_SET_TYPING_JS)?;
    wf(p, "modules/presence/cleanup_presence.js", CHAT_CLEANUP_PRESENCE_JS)?;
    wf(p, "modules/moderation/ban_user.js",     CHAT_BAN_USER_JS)?;
    wf(p, "modules/moderation/unban_user.js",   CHAT_UNBAN_USER_JS)?;
    wf(p, "client/chat-client.ts",              CHAT_CLIENT_TS)?;
    wf(p, "schema.toml",                        CHAT_SCHEMA_TOML)?;
    wf(p, "PERFORMANCE.md",                     PERF_MD)?;
    wf(p, "README.md", &format!("# {} — Chat Template\n\n{}", name, CHAT_README))?;
    print_success(name, "rust/chat", &[
        ("modules/rooms/",      "create, join, leave, delete"),
        ("modules/messages/",   "send, edit, delete, react"),
        ("modules/threads/",    "create_thread, reply"),
        ("modules/presence/",   "set_online, set_typing, cleanup (scheduled 30s)"),
        ("modules/moderation/", "ban_user, unban_user"),
    ]);
    println!("  Next steps:\n    cd {name}\n    neondb start\n\n  Upgrade to WASM for production throughput:\n    neondb build          # compiles JS → WASM via Javy (10–50× faster)\n\n  See PERFORMANCE.md for the full native-Rust path.");
    println!();
    Ok(())
}

fn scaffold_typescript(p: &Path, name: &str) -> Result<()> {
    wf(p, "modules/hello.js",              TS_HELLO_JS)?;
    wf(p, "modules/set_value.js",          TS_SET_VALUE_JS)?;
    wf(p, "modules/delete_value.js",       TS_DELETE_VALUE_JS)?;
    wf(p, "client/src/client.ts",          TS_CLIENT_TS)?;
    wf(p, "client/src/hooks.tsx",          TS_HOOKS_TSX)?;
    wf(p, "client/src/example/App.tsx",    TS_APP_TSX)?;
    wf(p, "client/package.json",           &TS_PACKAGE_JSON.replace("__NAME__", name))?;
    wf(p, "client/tsconfig.json",          TS_TSCONFIG_JSON)?;
    wf(p, "README.md", &format!("# {} — TypeScript Template\n\n{}", name, TS_README))?;
    print_success(name, "typescript", &[
        ("modules/hello.js",      "basic counter reducer"),
        ("client/src/client.ts",  "NeonDBClient — connect, call, subscribe"),
        ("client/src/hooks.tsx",  "useNeonDBQuery, useNeonDBReducer, NeonDBProvider"),
        ("client/package.json",   "npm package config"),
    ]);
    println!("  Next steps:\n    cd {}\n    neondb start\n    cd client && npm install && npm run dev", name);
    println!();
    Ok(())
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

const BASIC_REGISTER_JS: &str       = include_str!("../templates/basic_register.js.txt");
const BASIC_LOGIN_JS: &str          = include_str!("../templates/basic_login.js.txt");
const BASIC_LOGOUT_JS: &str         = include_str!("../templates/basic_logout.js.txt");
const BASIC_GRANT_ROLE_JS: &str     = include_str!("../templates/basic_grant_role.js.txt");
const BASIC_UPDATE_PROFILE_JS: &str = include_str!("../templates/basic_update_profile.js.txt");
const BASIC_DELETE_USER_JS: &str    = include_str!("../templates/basic_delete_user.js.txt");
const BASIC_ADD_ITEM_JS: &str       = include_str!("../templates/basic_add_item.js.txt");
const BASIC_REMOVE_ITEM_JS: &str    = include_str!("../templates/basic_remove_item.js.txt");
const BASIC_SUB_PLAYER_JS: &str     = include_str!("../templates/basic_sub_player.js.txt");
const BASIC_CLIENT_TS: &str         = include_str!("../templates/basic_client.ts.txt");
const BASIC_SCHEMA_TOML: &str       = include_str!("../templates/basic_schema.toml.txt");
const BASIC_README: &str            = include_str!("../templates/basic_readme.md.txt");
const GAME_SPAWN_JS: &str           = include_str!("../templates/game_spawn.js.txt");
const GAME_DESPAWN_JS: &str         = include_str!("../templates/game_despawn.js.txt");
const GAME_MOVE_JS: &str            = include_str!("../templates/game_move.js.txt");
const GAME_UPDATE_STATS_JS: &str    = include_str!("../templates/game_update_stats.js.txt");
const GAME_SPAWN_NPC_JS: &str       = include_str!("../templates/game_spawn_npc.js.txt");
const GAME_ATTACK_JS: &str          = include_str!("../templates/game_attack.js.txt");
const GAME_USE_ABILITY_JS: &str     = include_str!("../templates/game_use_ability.js.txt");
const GAME_APPLY_DAMAGE_JS: &str    = include_str!("../templates/game_apply_damage.js.txt");
const GAME_RESPAWN_JS: &str         = include_str!("../templates/game_respawn.js.txt");
const GAME_BUY_ITEM_JS: &str        = include_str!("../templates/game_buy_item.js.txt");
const GAME_SELL_ITEM_JS: &str       = include_str!("../templates/game_sell_item.js.txt");
const GAME_TRANSFER_CURRENCY_JS: &str = include_str!("../templates/game_transfer_currency.js.txt");
const GAME_OPEN_LOOT_BOX_JS: &str   = include_str!("../templates/game_open_loot_box.js.txt");
const GAME_ACCEPT_QUEST_JS: &str    = include_str!("../templates/game_accept_quest.js.txt");
const GAME_COMPLETE_QUEST_JS: &str  = include_str!("../templates/game_complete_quest.js.txt");
const GAME_UPDATE_PROGRESS_JS: &str = include_str!("../templates/game_update_progress.js.txt");
const GAME_QUEUE_JS: &str           = include_str!("../templates/game_queue.js.txt");
const GAME_DEQUEUE_JS: &str         = include_str!("../templates/game_dequeue.js.txt");
const GAME_CREATE_MATCH_JS: &str    = include_str!("../templates/game_create_match.js.txt");
const GAME_MATCHMAKING_REFRESH_JS: &str = include_str!("../templates/game_matchmaking_refresh.js.txt");
const GAME_GUILD_CREATE_JS: &str    = include_str!("../templates/game_guild_create.js.txt");
const GAME_GUILD_INVITE_JS: &str    = include_str!("../templates/game_guild_invite.js.txt");
const GAME_GUILD_ACCEPT_JS: &str    = include_str!("../templates/game_guild_accept.js.txt");
const GAME_GUILD_KICK_JS: &str      = include_str!("../templates/game_guild_kick.js.txt");
const GAME_WORLD_TICK_JS: &str      = include_str!("../templates/game_world_tick.js.txt");
const GAME_CLEANUP_SESSIONS_JS: &str = include_str!("../templates/game_cleanup_sessions.js.txt");
const GAME_SUBMIT_SCORE_JS: &str    = include_str!("../templates/game_submit_score.js.txt");
const GAME_RESET_WEEKLY_JS: &str    = include_str!("../templates/game_reset_weekly.js.txt");
const GAME_CLIENT_TS: &str          = include_str!("../templates/game_client.ts.txt");
const GAME_SCHEMA_TOML: &str        = include_str!("../templates/game_schema.toml.txt");
const GAME_GENRE_GUIDE_MD: &str     = include_str!("../templates/game_genre_guide.md.txt");
const GAME_SEED_JSON: &str          = include_str!("../templates/game_seed.json.txt");
const GAME_README: &str             = include_str!("../templates/game_readme.md.txt");
const CHAT_CREATE_ROOM_JS: &str     = include_str!("../templates/chat_create_room.js.txt");
const CHAT_JOIN_ROOM_JS: &str       = include_str!("../templates/chat_join_room.js.txt");
const CHAT_LEAVE_ROOM_JS: &str      = include_str!("../templates/chat_leave_room.js.txt");
const CHAT_DELETE_ROOM_JS: &str     = include_str!("../templates/chat_delete_room.js.txt");
const CHAT_SEND_MESSAGE_JS: &str    = include_str!("../templates/chat_send_message.js.txt");
const CHAT_EDIT_MESSAGE_JS: &str    = include_str!("../templates/chat_edit_message.js.txt");
const CHAT_DELETE_MESSAGE_JS: &str  = include_str!("../templates/chat_delete_message.js.txt");
const CHAT_REACT_JS: &str           = include_str!("../templates/chat_react.js.txt");
const CHAT_CREATE_THREAD_JS: &str   = include_str!("../templates/chat_create_thread.js.txt");
const CHAT_REPLY_JS: &str           = include_str!("../templates/chat_reply.js.txt");
const CHAT_SET_ONLINE_JS: &str      = include_str!("../templates/chat_set_online.js.txt");
const CHAT_SET_TYPING_JS: &str      = include_str!("../templates/chat_set_typing.js.txt");
const CHAT_CLEANUP_PRESENCE_JS: &str = include_str!("../templates/chat_cleanup_presence.js.txt");
const CHAT_BAN_USER_JS: &str        = include_str!("../templates/chat_ban_user.js.txt");
const CHAT_UNBAN_USER_JS: &str      = include_str!("../templates/chat_unban_user.js.txt");
const CHAT_CLIENT_TS: &str          = include_str!("../templates/chat_client.ts.txt");
const CHAT_SCHEMA_TOML: &str        = include_str!("../templates/chat_schema.toml.txt");
const CHAT_README: &str             = include_str!("../templates/chat_readme.md.txt");
const TS_HELLO_JS: &str             = include_str!("../templates/ts_hello.js.txt");
const TS_SET_VALUE_JS: &str         = include_str!("../templates/ts_set_value.js.txt");
const TS_DELETE_VALUE_JS: &str      = include_str!("../templates/ts_delete_value.js.txt");
const TS_CLIENT_TS: &str            = include_str!("../templates/ts_client.ts.txt");
const TS_HOOKS_TSX: &str            = include_str!("../templates/ts_hooks.tsx.txt");
const TS_APP_TSX: &str              = include_str!("../templates/ts_app.tsx.txt");
const TS_PACKAGE_JSON: &str         = include_str!("../templates/ts_package.json.txt");
const TS_TSCONFIG_JSON: &str        = include_str!("../templates/ts_tsconfig.json.txt");
const TS_README: &str               = include_str!("../templates/ts_readme.md.txt");
const PERF_MD: &str                 = include_str!("../templates/performance.md.txt");

// native/game-ready template
const NATIVE_WORKSPACE_TOML: &str         = include_str!("../templates/native_workspace_cargo.toml.txt");
const NATIVE_HELPER_TOML: &str            = include_str!("../templates/native_neondb_reducer_cargo.toml.txt");
const NATIVE_HELPER_LIB: &str             = include_str!("../templates/native_neondb_reducer_lib.txt");
const NATIVE_SPAWN_TOML: &str             = include_str!("../templates/native_spawn_cargo.toml.txt");
const NATIVE_SPAWN_LIB: &str              = include_str!("../templates/native_spawn_lib.rs.txt");
const NATIVE_DESPAWN_TOML: &str           = include_str!("../templates/native_despawn_cargo.toml.txt");
const NATIVE_DESPAWN_LIB: &str            = include_str!("../templates/native_despawn_lib.rs.txt");
const NATIVE_MOVE_TOML: &str              = include_str!("../templates/native_move_player_cargo.toml.txt");
const NATIVE_MOVE_LIB: &str               = include_str!("../templates/native_move_player_lib.rs.txt");
const NATIVE_UPDATE_STATS_TOML: &str      = include_str!("../templates/native_update_stats_cargo.toml.txt");
const NATIVE_UPDATE_STATS_LIB: &str       = include_str!("../templates/native_update_stats_lib.rs.txt");
const NATIVE_ATTACK_TOML: &str            = include_str!("../templates/native_attack_cargo.toml.txt");
const NATIVE_ATTACK_LIB: &str             = include_str!("../templates/native_attack_lib.rs.txt");
const NATIVE_SPAWN_NPC_TOML: &str         = include_str!("../templates/native_spawn_npc_cargo.toml.txt");
const NATIVE_SPAWN_NPC_LIB: &str          = include_str!("../templates/native_spawn_npc_lib.rs.txt");
const NATIVE_BUY_ITEM_TOML: &str          = include_str!("../templates/native_buy_item_cargo.toml.txt");
const NATIVE_BUY_ITEM_LIB: &str           = include_str!("../templates/native_buy_item_lib.rs.txt");
const NATIVE_SELL_ITEM_TOML: &str         = include_str!("../templates/native_sell_item_cargo.toml.txt");
const NATIVE_SELL_ITEM_LIB: &str          = include_str!("../templates/native_sell_item_lib.rs.txt");
const NATIVE_WORLD_TICK_TOML: &str        = include_str!("../templates/native_world_tick_cargo.toml.txt");
const NATIVE_WORLD_TICK_LIB: &str         = include_str!("../templates/native_world_tick_lib.rs.txt");
const NATIVE_CLEANUP_TOML: &str           = include_str!("../templates/native_cleanup_sessions_cargo.toml.txt");
const NATIVE_CLEANUP_LIB: &str            = include_str!("../templates/native_cleanup_sessions_lib.rs.txt");
const NATIVE_SUBMIT_SCORE_TOML: &str      = include_str!("../templates/native_submit_score_cargo.toml.txt");
const NATIVE_SUBMIT_SCORE_LIB: &str       = include_str!("../templates/native_submit_score_lib.rs.txt");
const NATIVE_BUILD_PS1: &str              = include_str!("../templates/native_build_ps1.txt");
const NATIVE_BUILD_SH: &str              = include_str!("../templates/native_build_sh.txt");
const NATIVE_README: &str                 = include_str!("../templates/native_readme.md.txt");

fn scaffold_native_game_ready(p: &Path, name: &str) -> Result<()> {
    // Workspace root
    wf(p, "Cargo.toml",                              NATIVE_WORKSPACE_TOML)?;

    // Bundled helper crate (no crates.io needed — fully self-contained)
    wf(p, "neondb-reducer/Cargo.toml",               NATIVE_HELPER_TOML)?;
    wf(p, "neondb-reducer/src/lib.rs",               NATIVE_HELPER_LIB)?;

    // Reducer crates — each compiles to one .wasm file
    wf(p, "spawn/Cargo.toml",                        NATIVE_SPAWN_TOML)?;
    wf(p, "spawn/src/lib.rs",                        NATIVE_SPAWN_LIB)?;
    wf(p, "despawn/Cargo.toml",                      NATIVE_DESPAWN_TOML)?;
    wf(p, "despawn/src/lib.rs",                      NATIVE_DESPAWN_LIB)?;
    wf(p, "move_player/Cargo.toml",                  NATIVE_MOVE_TOML)?;
    wf(p, "move_player/src/lib.rs",                  NATIVE_MOVE_LIB)?;
    wf(p, "update_stats/Cargo.toml",                 NATIVE_UPDATE_STATS_TOML)?;
    wf(p, "update_stats/src/lib.rs",                 NATIVE_UPDATE_STATS_LIB)?;
    wf(p, "attack/Cargo.toml",                       NATIVE_ATTACK_TOML)?;
    wf(p, "attack/src/lib.rs",                       NATIVE_ATTACK_LIB)?;
    wf(p, "spawn_npc/Cargo.toml",                    NATIVE_SPAWN_NPC_TOML)?;
    wf(p, "spawn_npc/src/lib.rs",                    NATIVE_SPAWN_NPC_LIB)?;
    wf(p, "buy_item/Cargo.toml",                     NATIVE_BUY_ITEM_TOML)?;
    wf(p, "buy_item/src/lib.rs",                     NATIVE_BUY_ITEM_LIB)?;
    wf(p, "sell_item/Cargo.toml",                    NATIVE_SELL_ITEM_TOML)?;
    wf(p, "sell_item/src/lib.rs",                    NATIVE_SELL_ITEM_LIB)?;
    wf(p, "world_tick/Cargo.toml",                   NATIVE_WORLD_TICK_TOML)?;
    wf(p, "world_tick/src/lib.rs",                   NATIVE_WORLD_TICK_LIB)?;
    wf(p, "cleanup_sessions/Cargo.toml",             NATIVE_CLEANUP_TOML)?;
    wf(p, "cleanup_sessions/src/lib.rs",             NATIVE_CLEANUP_LIB)?;
    wf(p, "submit_score/Cargo.toml",                 NATIVE_SUBMIT_SCORE_TOML)?;
    wf(p, "submit_score/src/lib.rs",                 NATIVE_SUBMIT_SCORE_LIB)?;

    // Build scripts + docs
    wf(p, "build.ps1",                               NATIVE_BUILD_PS1)?;
    wf(p, "build.sh",                                NATIVE_BUILD_SH)?;
    wf(p, "PERFORMANCE.md",                          PERF_MD)?;
    wf(p, "README.md", &format!("# {} — Native Rust Template\n\n{}", name, NATIVE_README))?;

    // Shared server config (reuse game schema)
    wf(p, "schema.toml",                             GAME_SCHEMA_TOML)?;
    wf(p, "seed.json",                               GAME_SEED_JSON)?;

    print_success(name, "native/game-ready", &[
        ("neondb-reducer/",     "bundled Context API (no crates.io needed)"),
        ("spawn/ … submit_score/", "11 reducer crates, each → one .wasm"),
        ("build.ps1 / build.sh","cargo build --target wasm32-unknown-unknown"),
        ("modules/",            "auto-populated by build script"),
        ("PERFORMANCE.md",      "JS → WASM → native performance guide"),
    ]);
    println!("  Prerequisites:\n    rustup target add wasm32-unknown-unknown\n");
    println!("  Next steps:\n    cd {name}\n    .\\build.ps1          # Windows\n    ./build.sh           # Linux / macOS\n    neondb start");
    println!();
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════════
// neondb build
// ═══════════════════════════════════════════════════════════════════════════════

fn build_wasm_modules(modules_dir: &Path) -> Result<()> {
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

    let permissions = Arc::new(config.permissions.clone());

    // ── Cluster bus ───────────────────────────────────────────────────────────
    let cluster_config = ClusterConfig::from_env(config.shard_id, config.shard_count);
    if cluster_config.enabled {
        log::info!("[cluster] shard {}/{}, {} peer(s)", cluster_config.my_shard_id, cluster_config.shard_count, cluster_config.peers.len());
    } else {
        log::info!("[cluster] single-node mode");
    }
    let cluster_bus = ClusterBus::new(cluster_config);

    // ── Dynamic seed join ─────────────────────────────────────────────────────
    if let Ok(seed_url) = std::env::var("NEONDB_SEED_NODE") {
        if !seed_url.is_empty() {
            let my_shard_id = cluster_bus.config.my_shard_id;
            let my_metrics  = format!("http://{}:{}", config.host, config.metrics_port);
            log::info!("[cluster] Seeding from {}", seed_url);
            let bus_seed = cluster_bus.clone();
            tokio::spawn(async move {
                if let Err(e) = cluster_seed(&bus_seed, &seed_url, my_shard_id, &my_metrics).await {
                    log::warn!("[cluster] Seed join failed: {}", e);
                }
            });
        }
    }

    let (reducer_tx, reducer_rx) = kanal::unbounded_async::<PendingCall>();
    let subscription_manager = Arc::new(SubscriptionManager::new_with_options(config.two_frame_protocol));

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
        tokio::spawn(async move {
            if let Err(e) = start_listener(
                config_c.host, config_c.port, tx_c, subs_c, tables_c,
                config_c.max_connections, config_c.api_key.clone(),
                conns_c, perms_c, config_c.sql_timeout_ms,
                auth_c, rl_c, pres_c, ttl_c, iss_c, rx_shutdown, metrics_c, tls_cfg,
            ).await { log::error!("Listener error: {}", e); }
        })
    };

    let gossip_handle = neondb::cluster::gossip::start_gossip(cluster_bus.clone(), shutdown_rx.clone());
    let _fanout_retry_handle = neondb::cluster::fanout::start_fanout_retry(cluster_bus.clone(), shutdown_rx.clone());

    let wal_writer = Arc::new(BatchedWalWriter::open(
        &config.wal_path, config.wal_batch_interval_ms, config.wal_batch_size, config.unsafe_no_fsync,
    )?);
    let worker_count = num_cpus::get().max(1);
    log::info!("Starting {} reducer workers", worker_count);

    let timeout_ms        = config.reducer_timeout_ms;
    let snapshot_interval = config.snapshot_interval;
    let snapshot_dir_w    = config.snapshot_dir.clone();
    let global_seq        = Arc::new(std::sync::atomic::AtomicU64::new(initial_seq));

    let startup_instant = std::time::Instant::now();

    // ── Raft consensus engine ─────────────────────────────────────────────────
    // Initialise openraft. In single-node mode this is a no-op cluster of 1.
    // In multi-node mode other peers call POST /raft/add-learner and
    // POST /raft/change-membership to form the cluster.
    let raft_node_id: u64 = config.shard_id as u64;
    let raft_node_addr = format!("http://{}:{}", config.host, config.metrics_port);
    let raft_handle: Arc<neondb::raft::NeonRaft> = {
        use neondb::raft::{
            build_raft_config, TypeConfig,
            network::NeonNetworkFactory,
            state_machine::NeonStateMachine,
            storage::MemLogStore,
        };
        use openraft::Raft;

        let raft_config  = build_raft_config();
        let vote_path = config.wal_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("raft_vote.json");
        let persisted_vote = MemLogStore::load_persisted_vote(&vote_path);
        let mut log_store = MemLogStore::new(Some(vote_path));
        if let Some(vote) = persisted_vote {
            use openraft::storage::RaftLogStorage;
            log_store.save_vote(&vote).await.expect("Failed to restore persisted Raft vote");
            log::info!("[raft] restored persisted vote from disk (term={})", vote.leader_id.term);
        }
        let state_machine = NeonStateMachine::new(tables.clone(), subscription_manager.clone());
        let network_factory = NeonNetworkFactory::new();

        let raft = Raft::<TypeConfig>::new(
            raft_node_id,
            raft_config,
            network_factory,
            log_store,
            state_machine,
        ).await.expect("Failed to create Raft node");

        // Bootstrap: initialise as a single-node cluster unless peers are known.
        // A multi-node cluster is formed by calling /raft/change-membership after
        // all nodes are started.
        if config.shard_count <= 1 {
            let mut members = std::collections::BTreeMap::new();
            members.insert(raft_node_id, openraft::BasicNode { addr: raft_node_addr.clone() });
            match raft.initialize(members).await {
                Ok(_)  => log::info!("[raft] bootstrapped single-node cluster (id={})", raft_node_id),
                Err(e) => log::warn!("[raft] init skipped (already initialised?): {}", e),
            }
        } else {
            log::info!("[raft] multi-node mode: shard_count={}; call /raft/change-membership to form cluster", config.shard_count);
        }

        Arc::new(raft)
    };

    let metrics_handle = {
        let subs_c = subscription_manager.clone(); let tables_c = tables.clone();
        let rx_shutdown = shutdown_rx.clone();
        let host_c = config.host.clone(); let mport = config.metrics_port;
        let bus_c = cluster_bus.clone();
        let registry_c = registry.clone();
        let wal_c = wal_writer.clone();
        let seq_c = global_seq.clone();
        let pres_m = presence.clone();
        let ttl_m = ttl_manager.clone();
        let raft_c = raft_handle.clone();
        let raft_node_id_c = raft_node_id;
        let raft_node_addr_c = raft_node_addr.clone();
        let prom_c = metrics.clone();
        let issuer_c = identity_issuer.clone();
        tokio::spawn(async move {
            if let Err(e) = start_metrics_server(host_c, mport, subs_c, tables_c, bus_c, registry_c, wal_c, seq_c, startup_instant, pres_m, ttl_m, raft_c, raft_node_id_c, raft_node_addr_c, prom_c, issuer_c, rx_shutdown).await {
                log::error!("Metrics server error: {}", e);
            }
        })
    };

    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let rx = reducer_rx.clone(); let tables_w = tables.clone();
        let registry_w = registry.clone();
        let _subs_w = subscription_manager.clone(); let wal_w = wal_writer.clone();
        let seq_w = global_seq.clone(); let snap_iv = snapshot_interval;
        let snap_dir_ww = snapshot_dir_w.clone(); let schema_w = schema_registry.clone();
        let bus_w = cluster_bus.clone();
        let ttl_w = ttl_manager.clone();
        // Raft handle — every committed reducer result goes through consensus.
        let raft_w = raft_handle.clone();
        let mut rx_shutdown_w = shutdown_rx.clone();
        let metrics_w = metrics.clone();

        worker_handles.push(tokio::spawn(async move {
            loop {
                let call = tokio::select! {
                    result = rx.recv() => match result { Ok(c) => c, Err(_) => break },
                    _ = rx_shutdown_w.changed() => break,
                };
                let call_id     = call.call_id;
                let caller_id   = call.caller_id.clone();
                let caller_role = call.caller_role.clone();
                let tables_blk  = tables_w.clone();
                let registry_blk = registry_w.clone();
                let reducer_name = call.reducer_name.clone();
                let args         = call.args.clone();
                let ts           = current_timestamp_nanos();
                let schema_blk   = schema_w.clone();
                let ttl_blk      = ttl_w.clone();
                let call_start   = std::time::Instant::now();

                let blk = tokio::time::timeout(
                    std::time::Duration::from_millis(timeout_ms),
                    tokio::task::spawn_blocking(move || {
                        let mut ctx = ReducerContext::new(tables_blk, ts)
                            .with_schema(schema_blk)
                            .with_ttl(ttl_blk);
                        ctx.caller_id   = caller_id;
                        ctx.caller_role = caller_role;
                        let exec = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                            || registry_blk.execute(&reducer_name, &mut ctx, &args)
                        ));
                        (exec, ctx)
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
                    Ok(Ok((exec_result, mut ctx))) => match exec_result {
                        Ok(Ok(result_bytes)) => {
                            // ── Raft write path ──────────────────────────────────────────────
                            //
                            // Drain staged deltas WITHOUT applying them to the local TableStore.
                            // Forward to Raft consensus: the Raft state machine (NeonStateMachine)
                            // applies the deltas on EVERY node (including this one) once the entry
                            // is committed to a quorum.  This guarantees the write is durable and
                            // replicated before the client receives a success response.
                            //
                            // In single-node clusters, Raft commits immediately (no network RTT).
                            // In multi-node clusters, commit latency ≈ one heartbeat period / 2
                            // (≈125 ms with the default 250 ms heartbeat).
                            let deltas = ctx.drain_pending_deltas();

                            let now_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            let raft_req = neondb::raft::RaftRequest {
                                reducer_name: call.reducer_name.clone(),
                                args:         call.args.clone(),
                                deltas:       deltas.clone(),
                                timestamp_ms: now_ms,
                            };

                            match raft_w.client_write(raft_req).await {
                                Ok(_) => {
                                    // Raft committed — state machine has applied deltas and
                                    // published fan-out to local subscribers.
                                    // Also write a WAL entry for crash recovery between
                                    // snapshots (WAL is the fast-recovery path; Raft log is
                                    // the authoritative distributed log).
                                    let seq_num = seq_w.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    let entry = WalEntry::new(ts, seq_num, call.reducer_name.clone(), call.args.clone(), deltas.clone());
                                    if let Err(e) = wal_w.append(&entry, seq_num) {
                                        log::warn!("WAL append failed (non-fatal, Raft is durable): {}", e);
                                    } else {
                                        metrics_w.wal_entries_written_total.inc();
                                    }
                                    // Cluster fan-out to non-Raft legacy peers (if any).
                                    if !deltas.is_empty() {
                                        bus_w.fanout_deltas(&deltas);
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
                                Err(e) => {
                                    log::error!("Raft client_write failed call_id={}: {}", call_id, e);
                                    metrics_w.reducer_errors_total.inc();
                                    ReducerResponse::error(call_id, format!("Consensus error: {}", e))
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            log::warn!("Reducer error: {}", e);
                            metrics_w.reducer_errors_total.inc();
                            ReducerResponse::error(call_id, e.to_string())
                        }
                        Err(_) => {
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
        let raft_g   = raft_handle.clone();
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
                        // Raft state
                        let raft_metrics = raft_g.metrics().borrow().clone();
                        prom_g.raft_log_index.set(
                            raft_metrics.last_log_index.unwrap_or(0) as i64
                        );
                        let is_leader = raft_metrics.current_leader
                            .map(|lid| if lid == raft_metrics.id { 1i64 } else { 0i64 })
                            .unwrap_or(0);
                        prom_g.raft_is_leader.set(is_leader);
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
    let _ = gossip_handle.await;
    let _ = presence_handle.await;
    let _ = ttl_handle.await;
    let _ = gauge_handle.await;

    eprintln!("[neondb] Shutdown complete.");
    log::info!("Shutdown complete");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// cluster_seed — dynamic join via NEONDB_SEED_NODE
// ─────────────────────────────────────────────────────────────────────────────

async fn cluster_seed(
    bus: &Arc<ClusterBus>,
    seed_url: &str,
    my_shard_id: u32,
    my_metrics_url: &str,
) -> std::result::Result<(), String> {
    let url = format!("{}/cluster/join", seed_url.trim_end_matches('/'));

    let body = serde_json::json!({
        "shard_id":    my_shard_id,
        "metrics_url": my_metrics_url,
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(bus.config.http_timeout_ms))
        .build()
        .map_err(|e| format!("HTTP client: {}", e))?;

    let mut req = client.post(&url).json(&body);
    if let Some((hdr, val)) = bus.secret_header() {
        req = req.header(hdr, val);
    }

    let resp = req.send().await.map_err(|e| format!("POST {}: {}", url, e))?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }

    let data: serde_json::Value = resp.json().await.map_err(|e| format!("JSON: {}", e))?;
    let peers = data.get("peers").and_then(|p| p.as_array()).cloned().unwrap_or_default();

    let mut added = 0usize;
    for peer_val in &peers {
        let shard_id    = peer_val.get("shard_id").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let metrics_url = peer_val.get("metrics_url").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if shard_id == my_shard_id || metrics_url.is_empty() { continue; }
        if !bus.peers.contains_key(&shard_id) {
            let node = NodeInfo { shard_id, metrics_url };
            bus.peers.insert(shard_id, PeerEntry::new(node));
            added += 1;
        }
    }
    log::info!("[cluster] Joined via seed {}; learned {} peer(s)", seed_url, added);
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

async fn start_metrics_server(
    host: String,
    port: u16,
    subscription_manager: Arc<SubscriptionManager>,
    tables: Arc<TableStore>,
    cluster_bus: Arc<ClusterBus>,
    registry: Arc<ReducerRegistry>,
    wal_writer: Arc<BatchedWalWriter>,
    global_seq: Arc<std::sync::atomic::AtomicU64>,
    startup_instant: std::time::Instant,
    presence_manager: Arc<PresenceManager>,
    ttl_manager: Arc<TtlManager>,
    raft: Arc<neondb::raft::NeonRaft>,
    raft_node_id: u64,
    raft_node_addr: String,
    prom: Arc<Metrics>,
    identity_issuer: Arc<IdentityIssuer>,
    mut shutdown: watch::Receiver<()>,
) -> Result<()> {
    let addr: SocketAddr = format!("{}:{}", host, port).parse()
        .map_err(|e| neondb::error::NeonDBError::invalid_argument(format!("Invalid metrics address: {}", e)))?;

    let make_service = make_service_fn(move |_| {
        let subs  = subscription_manager.clone();
        let tbl   = tables.clone();
        let bus   = cluster_bus.clone();
        let reg   = registry.clone();
        let wal   = wal_writer.clone();
        let seq   = global_seq.clone();
        let start = startup_instant;
        let pres  = presence_manager.clone();
        let ttl   = ttl_manager.clone();
        let raft_svc = raft.clone();
        let nid   = raft_node_id;
        let naddr = raft_node_addr.clone();
        let prom_svc = prom.clone();
        let iss   = identity_issuer.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let subs = subs.clone(); let tbl = tbl.clone();
                let bus  = bus.clone();  let reg = reg.clone();
                let wal  = wal.clone();  let seq = seq.clone();
                let pres = pres.clone(); let ttl = ttl.clone();
                let raft_r = raft_svc.clone(); let nid_r = nid; let naddr_r = naddr.clone();
                let prom_r = prom_svc.clone();
                let iss_r = iss.clone();
                async move { handle_metrics_request(req, subs, tbl, bus, reg, wal, seq, start, pres, ttl, raft_r, nid_r, naddr_r, prom_r, iss_r).await }
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_service);
    log::info!("Admin/metrics on http://{}", addr);
    server.with_graceful_shutdown(async move { let _ = shutdown.changed().await; }).await
        .map_err(|e| neondb::error::NeonDBError::network_error(format!("Metrics server: {}", e)))
}

fn json_response(value: serde_json::Value) -> Response<Body> {
    let mut r = Response::new(Body::from(value.to_string()));
    r.headers_mut().insert(hyper::header::CONTENT_TYPE, hyper::header::HeaderValue::from_static("application/json"));
    r
}

async fn handle_metrics_request(
    req: Request<Body>,
    subscription_manager: Arc<SubscriptionManager>,
    tables: Arc<TableStore>,
    cluster_bus: Arc<ClusterBus>,
    registry: Arc<ReducerRegistry>,
    wal_writer: Arc<BatchedWalWriter>,
    global_seq: Arc<std::sync::atomic::AtomicU64>,
    startup_instant: std::time::Instant,
    presence_manager: Arc<PresenceManager>,
    ttl_manager: Arc<TtlManager>,
    raft: Arc<neondb::raft::NeonRaft>,
    raft_node_id: u64,
    raft_node_addr: String,
    prom: Arc<Metrics>,
    identity_issuer: Arc<IdentityIssuer>,
) -> Result<Response<Body>> {
    let path = req.uri().path().to_string();

    match (req.method(), path.as_str()) {
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
            "total_rows": tables.total_row_count(),
            "active_connections": subscription_manager.active_connections(),
            "active_subscriptions": subscription_manager.active_subscriptions(),
            "wal_sequence": global_seq.load(std::sync::atomic::Ordering::Relaxed),
            "wal_file_size_bytes": wal_writer.wal_file_size_bytes(),
            "uptime_seconds": startup_instant.elapsed().as_secs(),
            "reducer_queue_depth": 0,
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
            let peers: Vec<_> = cluster_bus.peers.iter().map(|e| {
                let p = e.value();
                serde_json::json!({ "shard_id": p.node.shard_id, "healthy": p.is_healthy() })
            }).collect();
            Ok(json_response(serde_json::json!({
                "tables": table_list,
                "total_rows": tables.total_row_count(),
                "indexes": indexes,
                "wal_sequence": global_seq.load(std::sync::atomic::Ordering::Relaxed),
                "wal_file_size_bytes": wal_writer.wal_file_size_bytes(),
                "snapshot_last_seq": 0u64, // Not easily queryable without scanning snapshot dir
                "cluster_enabled": cluster_bus.is_active(),
                "peers": peers,
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

        (&Method::GET, "/cluster/health") => {
            let secret = req.headers().get("x-neondb-cluster-secret").and_then(|v| v.to_str().ok());
            if !cluster_bus.validate_secret(secret) {
                let mut r = json_response(serde_json::json!({ "error": "Unauthorized" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            Ok(json_response(serde_json::json!({
                "ok": true,
                "shard_id": cluster_bus.config.my_shard_id,
                "shard_count": cluster_bus.config.shard_count,
                "total_rows": tables.total_row_count(),
                "active_connections": subscription_manager.active_connections(),
            })))
        }

        (&Method::GET, "/cluster/peers") => {
            let secret = req.headers().get("x-neondb-cluster-secret").and_then(|v| v.to_str().ok());
            if !cluster_bus.validate_secret(secret) {
                let mut r = json_response(serde_json::json!({ "error": "Unauthorized" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            let peers: Vec<_> = cluster_bus.peers.iter().map(|e| {
                let p = e.value();
                serde_json::json!({ "shard_id": p.node.shard_id, "metrics_url": p.node.metrics_url, "healthy": p.is_healthy() })
            }).collect();
            Ok(json_response(serde_json::json!({
                "my_shard_id": cluster_bus.config.my_shard_id,
                "shard_count": cluster_bus.config.shard_count,
                "cluster_enabled": cluster_bus.is_active(),
                "peers": peers,
            })))
        }

        (&Method::POST, "/cluster/join") => {
            let secret = req.headers().get("x-neondb-cluster-secret").and_then(|v| v.to_str().ok());
            if !cluster_bus.validate_secret(secret) {
                let mut r = json_response(serde_json::json!({ "error": "Unauthorized" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(format!("Read body: {}", e)))?;
            let join_req: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => {
                    let mut r = json_response(serde_json::json!({ "error": format!("Parse error: {}", e) }));
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };
            let new_shard_id = join_req.get("shard_id").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
            let new_url = join_req.get("metrics_url").and_then(|v| v.as_str()).unwrap_or("").to_string();

            if new_url.is_empty() {
                let mut r = json_response(serde_json::json!({ "error": "metrics_url required" }));
                *r.status_mut() = StatusCode::BAD_REQUEST;
                return Ok(r);
            }

            if !cluster_bus.peers.contains_key(&new_shard_id) {
                let node = NodeInfo { shard_id: new_shard_id, metrics_url: new_url.clone() };
                cluster_bus.peers.insert(new_shard_id, PeerEntry::new(node));
                log::info!("[cluster] New peer joined: shard{} @ {}", new_shard_id, new_url);
            }

            let peers: Vec<_> = cluster_bus.peers.iter()
                .map(|e| serde_json::json!({ "shard_id": e.value().node.shard_id, "metrics_url": e.value().node.metrics_url }))
                .collect();

            Ok(json_response(serde_json::json!({
                "ok": true,
                "my_shard_id": cluster_bus.config.my_shard_id,
                "peers": peers,
            })))
        }

        (&Method::POST, "/cluster/deltas") => {
            let secret = req.headers().get("x-neondb-cluster-secret").and_then(|v| v.to_str().ok());
            if !cluster_bus.validate_secret(secret) {
                let mut r = json_response(serde_json::json!({ "error": "Unauthorized" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(format!("Read body: {}", e)))?;
            match neondb::cluster::fanout::parse_delta_payload(&body_bytes) {
                Err(e) => {
                    let mut r = json_response(serde_json::json!({ "error": format!("Parse error: {}", e) }));
                    *r.status_mut() = StatusCode::BAD_REQUEST; Ok(r)
                }
                Ok(payload) => {
                    let deltas = neondb::cluster::fanout::wire_to_row_deltas(payload.deltas);
                    match ClusterBus::apply_peer_deltas(&deltas, &tables, &subscription_manager) {
                        Ok(()) => {
                            // Journal peer-applied deltas to the local WAL so they survive
                            // a restart on this receiver node. Without this, peer fan-outs
                            // are in-memory only.
                            if !deltas.is_empty() {
                                let seq_num = global_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let ts = current_timestamp_nanos();
                                let entry = WalEntry::new(
                                    ts,
                                    seq_num,
                                    "__cluster_replication".to_string(),
                                    Vec::new(),
                                    deltas.clone(),
                                );
                                if let Err(e) = wal_writer.append(&entry, seq_num) {
                                    log::error!("[cluster] WAL append for replicated deltas failed: {}", e);
                                }
                            }
                            Ok(json_response(serde_json::json!({ "ok": true, "applied": deltas.len() })))
                        }
                        Err(e) => {
                            let mut r = json_response(serde_json::json!({ "error": e.to_string() }));
                            *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR; Ok(r)
                        }
                    }
                }
            }
        }

        (&Method::POST, "/cluster/call") => {
            let secret = req.headers().get("x-neondb-cluster-secret").and_then(|v| v.to_str().ok());
            if !cluster_bus.validate_secret(secret) {
                let mut r = json_response(serde_json::json!({ "error": "Unauthorized" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await
                .map_err(|e| neondb::error::NeonDBError::network_error(format!("Read body: {}", e)))?;
            let proxy_req: neondb::cluster::proxy::ProxyCallRequest =
                match serde_json::from_slice(&body_bytes) {
                    Ok(r) => r,
                    Err(e) => {
                        let mut r = json_response(serde_json::json!({ "error": format!("Parse error: {}", e) }));
                        *r.status_mut() = StatusCode::BAD_REQUEST;
                        return Ok(r);
                    }
                };

            // Shard ownership validation: if the caller specified a target shard,
            // reject mis-routed requests with HTTP 421 (Misdirected) so the caller
            // can refresh its peer table and retry against the correct owner.
            if let Some(target) = proxy_req.target_shard_id {
                let me = cluster_bus.config.my_shard_id;
                if target != me {
                    log::warn!(
                        "[cluster] Misrouted /cluster/call: reducer='{}' target_shard={} but I am shard{}",
                        proxy_req.reducer_name, target, me
                    );
                    let mut r = json_response(serde_json::json!({
                        "error": "wrong_shard",
                        "owner_shard": me,
                    }));
                    *r.status_mut() = StatusCode::MISDIRECTED_REQUEST;
                    return Ok(r);
                }
            }

            use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
            let args = match B64.decode(&proxy_req.args_b64) {
                Ok(a) => a,
                Err(e) => {
                    let mut r = json_response(serde_json::json!({ "error": format!("Base64 decode: {}", e) }));
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };

            let tables_blk   = tables.clone();
            let registry_blk = registry.clone();
            let reducer_name = proxy_req.reducer_name.clone();
            let caller_id    = proxy_req.caller_id.clone();
            let caller_role  = proxy_req.caller_role.clone();
            let timestamp    = current_timestamp_nanos();

            let blk = tokio::task::spawn_blocking(move || {
                let mut ctx = neondb::reducer::ReducerContext::new(tables_blk, timestamp);
                ctx.caller_id   = caller_id;
                ctx.caller_role = caller_role;
                let exec = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || registry_blk.execute(&reducer_name, &mut ctx, &args)
                ));
                (exec, ctx)
            }).await;

            let resp_body = match blk {
                Err(e)              => neondb::cluster::proxy::ProxyCallResponse::error_response(format!("Task join error: {}", e)),
                Ok((Err(_), _))     => neondb::cluster::proxy::ProxyCallResponse::error_response("Reducer panicked"),
                Ok((Ok(Err(e)), _)) => neondb::cluster::proxy::ProxyCallResponse::error_response(e.to_string()),
                Ok((Ok(Ok(result_bytes)), mut ctx)) => match ctx.commit() {
                    Err(e)     => neondb::cluster::proxy::ProxyCallResponse::error_response(format!("Commit error: {}", e)),
                    Ok(deltas) => {
                        subscription_manager.publish_deltas(&deltas);
                        cluster_bus.fanout_deltas(&deltas);
                        neondb::cluster::proxy::ProxyCallResponse::success_response(&result_bytes)
                    }
                },
            };

            Ok(json_response(serde_json::to_value(resp_body).unwrap_or(serde_json::json!({}))))
        }

        // ── Raft RPC endpoints (called by peer nodes) ─────────────────────
        // These receive openraft RPCs from other NeonDB nodes in the cluster.

        (&Method::POST, "/raft/append") => {
            Ok(neondb::raft::http::handle_raft_append(raft, req).await)
        }

        (&Method::POST, "/raft/vote") => {
            Ok(neondb::raft::http::handle_raft_vote(raft, req).await)
        }

        (&Method::POST, "/raft/snapshot") => {
            Ok(neondb::raft::http::handle_raft_snapshot(raft, req).await)
        }

        (&Method::GET, "/raft/metrics") => {
            Ok(neondb::raft::http::handle_raft_metrics(raft).await)
        }

        (&Method::POST, "/raft/add-learner") => {
            Ok(neondb::raft::http::handle_raft_add_learner(raft, req).await)
        }

        (&Method::POST, "/raft/change-membership") => {
            Ok(neondb::raft::http::handle_raft_change_membership(raft, req).await)
        }

        (&Method::POST, "/raft/init") => {
            Ok(neondb::raft::http::handle_raft_init(raft, raft_node_id, raft_node_addr).await)
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
        let pid = std::process::id();
        if let Ok(output) = std::process::Command::new("wmic")
            .args(["process", "where", &format!("ProcessId={}", pid), "get", "WorkingSetSize"])
            .output()
        {
            let text = String::from_utf8_lossy(&output.stdout);
            for line in text.lines() {
                let trimmed = line.trim();
                if let Ok(val) = trimmed.parse::<u64>() {
                    return val; // WorkingSetSize is already in bytes
                }
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
