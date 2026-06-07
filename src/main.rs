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
use neondb::{
    config::{Config, ScheduledReducerConfig},
    cluster::{ClusterBus, ClusterConfig, NodeInfo, PeerEntry, PeerHealth},
    error::Result,
    network::{start_listener, PendingCall, ReducerResponse},
    reducer::{ReducerContext, ReducerRegistry},
    subscriptions::SubscriptionManager,
    table::TableStore,
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
    Template { name: "rust/basic",      category: "Rust",       description: "Foundation — users, sessions, subscribers, inventory, role-based auth" },
    Template { name: "rust/game-ready", category: "Rust",       description: "Game-ready engine — players, combat, economy, quests, matchmaking, guilds, world" },
    Template { name: "rust/chat",       category: "Rust",       description: "Production chat — rooms, threads, reactions, presence, moderation" },
    Template { name: "typescript",      category: "TypeScript", description: "TypeScript-first — React hooks, full client SDK, package.json scaffolding" },
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
        "rust/basic"      => scaffold_rust_basic(&project_path, &project_name)?,
        "rust/game-ready" => scaffold_rust_game_ready(&project_path, &project_name)?,
        "rust/chat"       => scaffold_rust_chat(&project_path, &project_name)?,
        "typescript"      => scaffold_typescript(&project_path, &project_name)?,
        _                 => unreachable!(),
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
    println!("  Next steps:\n    cd {}\n    neondb start", name);
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
    println!("  Next steps:\n    cd {}\n    neondb start\n    neondb seed seed.json", name);
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
    wf(p, "README.md", &format!("# {} — Chat Template\n\n{}", name, CHAT_README))?;
    print_success(name, "rust/chat", &[
        ("modules/rooms/",      "create, join, leave, delete"),
        ("modules/messages/",   "send, edit, delete, react"),
        ("modules/threads/",    "create_thread, reply"),
        ("modules/presence/",   "set_online, set_typing, cleanup (scheduled 30s)"),
        ("modules/moderation/", "ban_user, unban_user"),
    ]);
    println!("  Next steps:\n    cd {}\n    neondb start", name);
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
    for js_path in &js_files {
        let wasm_path = js_path.with_extension("wasm");
        print!("  Compiling {} ... ", js_path.display());
        match std::process::Command::new("javy").arg("compile").arg(js_path).arg("-o").arg(&wasm_path).status() {
            Ok(s) if s.success() => { println!("ok"); compiled += 1; }
            Ok(s) => { println!("FAILED (exit {})", s.code().unwrap_or(-1)); failed += 1; }
            Err(e) => { println!("FAILED ({})", e); failed += 1; }
        }
    }
    println!();
    if failed == 0 { println!("Build complete: {} compiled.", compiled); Ok(()) }
    else { Err(neondb::error::NeonDBError::internal(format!("{} files failed", failed))) }
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

// ═══════════════════════════════════════════════════════════════════════════════
// Server bootstrap
// ═══════════════════════════════════════════════════════════════════════════════

async fn run_server(config: Config) -> Result<()> {
    let mut logger = env_logger::Builder::from_default_env();
    logger.filter_level(config.log_level.parse().unwrap_or(log::LevelFilter::Info));
    let _ = logger.try_init();

    log::info!("Starting NeonDB Server");

    let mut ts = TableStore::new();
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

    let listener_handle = {
        let config_c = config.clone(); let tx_c = reducer_tx.clone();
        let subs_c = subscription_manager.clone(); let tables_c = tables.clone();
        let conns_c = active_connections.clone(); let rx_shutdown = shutdown_rx.clone();
        let perms_c = permissions.clone();
        tokio::spawn(async move {
            if let Err(e) = start_listener(
                config_c.host, config_c.port, tx_c, subs_c, tables_c,
                config_c.max_connections, config_c.api_key.clone(),
                conns_c, perms_c, rx_shutdown,
            ).await { log::error!("Listener error: {}", e); }
        })
    };

    let metrics_handle = {
        let subs_c = subscription_manager.clone(); let tables_c = tables.clone();
        let rx_shutdown = shutdown_rx.clone();
        let host_c = config.host.clone(); let mport = config.metrics_port;
        let bus_c = cluster_bus.clone();
        let registry_c = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = start_metrics_server(host_c, mport, subs_c, tables_c, bus_c, registry_c, rx_shutdown).await {
                log::error!("Metrics server error: {}", e);
            }
        })
    };

    let gossip_handle = neondb::cluster::gossip::start_gossip(cluster_bus.clone(), shutdown_rx.clone());

    let wal_writer = Arc::new(BatchedWalWriter::open(
        &config.wal_path, config.wal_batch_interval_ms, config.wal_batch_size, config.unsafe_no_fsync,
    )?);
    let worker_count = num_cpus::get().max(1);
    log::info!("Starting {} reducer workers", worker_count);

    let timeout_ms        = config.reducer_timeout_ms;
    let snapshot_interval = config.snapshot_interval;
    let snapshot_dir_w    = config.snapshot_dir.clone();
    let global_seq        = Arc::new(std::sync::atomic::AtomicU64::new(initial_seq));

    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let rx = reducer_rx.clone(); let tables_w = tables.clone();
        let registry_w = registry.clone();
        let subs_w = subscription_manager.clone(); let wal_w = wal_writer.clone();
        let seq_w = global_seq.clone(); let snap_iv = snapshot_interval;
        let snap_dir_ww = snapshot_dir_w.clone(); let schema_w = schema_registry.clone();
        let bus_w = cluster_bus.clone();

        worker_handles.push(tokio::spawn(async move {
            loop {
                let call = match rx.recv().await { Ok(c) => c, Err(_) => break };
                let call_id     = call.call_id;
                let caller_id   = call.caller_id.clone();
                let caller_role = call.caller_role.clone();
                let tables_blk  = tables_w.clone();
                let registry_blk = registry_w.clone();
                let reducer_name = call.reducer_name.clone();
                let args         = call.args.clone();
                let ts           = current_timestamp_nanos();
                let schema_blk   = schema_w.clone();

                let blk = tokio::time::timeout(
                    std::time::Duration::from_millis(timeout_ms),
                    tokio::task::spawn_blocking(move || {
                        let mut ctx = ReducerContext::new(tables_blk, ts).with_schema(schema_blk);
                        ctx.caller_id   = caller_id;
                        ctx.caller_role = caller_role;
                        let exec = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                            || registry_blk.execute(&reducer_name, &mut ctx, &args)
                        ));
                        (exec, ctx)
                    }),
                ).await;

                let response = match blk {
                    Err(_) => { log::warn!("call_id={} timed out", call_id); ReducerResponse::error(call_id, "Reducer timed out".to_string()) }
                    Ok(Err(e)) => { log::error!("Join error: {}", e); ReducerResponse::error(call_id, "Internal task error".to_string()) }
                    Ok(Ok((exec_result, mut ctx))) => match exec_result {
                        Ok(Ok(result_bytes)) => match ctx.commit() {
                            Ok(deltas) => {
                                let seq_num = seq_w.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let entry = WalEntry::new(ts, seq_num, call.reducer_name.clone(), call.args.clone(), deltas.clone());
                                match wal_w.append(&entry, seq_num) {
                                    Err(e) => { log::error!("WAL append failed: {}", e); ReducerResponse::error(call_id, e.to_string()) }
                                    Ok(_) => {
                                        subs_w.publish_deltas(&deltas);
                                        bus_w.fanout_deltas(&deltas);
                                        if snap_iv > 0 && (seq_num + 1) % snap_iv == 0 {
                                            let tbl = tables_w.clone(); let dir = snap_dir_ww.clone(); let ts2 = current_timestamp_nanos();
                                            tokio::spawn(async move {
                                                match tokio::task::spawn_blocking(move || save_snapshot(&tbl, &dir, seq_num, ts2)).await {
                                                    Ok(Ok(())) => log::info!("Snapshot written at seq {}", seq_num),
                                                    Ok(Err(e)) => log::error!("Snapshot failed: {}", e),
                                                    Err(e)     => log::error!("Snapshot panicked: {}", e),
                                                }
                                            });
                                        }
                                        ReducerResponse::success(call_id, result_bytes)
                                    }
                                }
                            }
                            Err(e) => { log::error!("Commit failed: {}", e); ReducerResponse::error(call_id, e.to_string()) }
                        },
                        Ok(Err(e)) => { log::warn!("Reducer error: {}", e); ReducerResponse::error(call_id, e.to_string()) }
                        Err(_) => { log::warn!("Reducer panicked call_id={}", call_id); ReducerResponse::error(call_id, "Reducer panicked".to_string()) }
                    },
                };
                if let Err(e) = call.response_tx.send(response) { log::warn!("send response: {}", e); }
            }
            log::debug!("Reducer worker {} stopped", worker_id);
        }));
    }

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

    tokio::signal::ctrl_c().await.ok();
    log::info!("Shutdown signal received");
    let _ = shutdown_tx.send(());
    drop(reducer_tx);
    for h in worker_handles  { let _ = h.await; }
    for h in scheduler_handles { let _ = h.await; }
    if let Ok(writer) = Arc::try_unwrap(wal_writer) {
        if let Err(e) = writer.shutdown() { log::error!("WAL shutdown: {}", e); }
    }
    let _ = listener_handle.await;
    let _ = metrics_handle.await;
    let _ = gossip_handle.await;
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
            bus.peers.insert(shard_id, PeerEntry {
                node,
                health: std::sync::Mutex::new(PeerHealth::default()),
            });
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
    mut shutdown: watch::Receiver<()>,
) -> Result<()> {
    let addr: SocketAddr = format!("{}:{}", host, port).parse()
        .map_err(|e| neondb::error::NeonDBError::invalid_argument(format!("Invalid metrics address: {}", e)))?;

    let make_service = make_service_fn(move |_| {
        let subs = subscription_manager.clone();
        let tbl  = tables.clone();
        let bus  = cluster_bus.clone();
        let reg  = registry.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let subs = subs.clone(); let tbl = tbl.clone();
                let bus  = bus.clone();  let reg = reg.clone();
                async move { handle_metrics_request(req, subs, tbl, bus, reg).await }
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
) -> Result<Response<Body>> {
    let path = req.uri().path().to_string();

    match (req.method(), path.as_str()) {
        (&Method::GET, "/metrics") => {
            let body = format!(
                "# NeonDB metrics\nactive_subscriptions {}\nactive_connections {}\ntotal_rows {}\nuptime_nanos {}\n",
                subscription_manager.active_subscriptions(),
                subscription_manager.active_connections(),
                tables.total_row_count(),
                current_timestamp_nanos(),
            );
            Ok(Response::new(Body::from(body)))
        }

        (&Method::GET, "/healthz") => Ok(json_response(serde_json::json!({
            "status": "ok",
            "total_rows": tables.total_row_count(),
            "active_connections": subscription_manager.active_connections(),
        }))),

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
                cluster_bus.peers.insert(new_shard_id, PeerEntry {
                    node,
                    health: std::sync::Mutex::new(PeerHealth::default()),
                });
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
                        Ok(()) => Ok(json_response(serde_json::json!({ "ok": true, "applied": deltas.len() }))),
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
                        neondb::cluster::proxy::ProxyCallResponse::success_response(&result_bytes)
                    }
                },
            };

            Ok(json_response(serde_json::to_value(resp_body).unwrap_or(serde_json::json!({}))))
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
