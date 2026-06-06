// ============================================================================
// NeonDB main.rs
//
// Session 28 — TODO-022: Role-based auth / permissions (complete)
//   - Pass Arc<PermissionsConfig> to start_listener ✅
//   - Set ctx.caller_role in the worker loop ✅
//   - Scheduler PendingCall caller_role = "scheduler" ✅
//
// Session 29 — Template system redesign (SpacetimeDB-style)
//   Three categories, four templates total:
//
//   RUST TEMPLATES (neondb init <name> --template rust/basic)
//   ├── rust/basic      — Users, sessions, subscribers, inventory, role auth
//   ├── rust/game-ready — Full prebuilt engine: players, combat, economy,
//   │                     quests, matchmaking, guilds, world.  Genre README
//   │                     guide tells you how to build any game on top.
//   └── rust/chat       — Production chat: rooms, threads, reactions, presence
//
//   TYPESCRIPT TEMPLATES (neondb init <name> --template typescript)
//   └── typescript      — TypeScript-first project with React hooks, full
//                         client examples, package.json scaffolding
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
        Commands::Bench { url, clients, calls, warmup, api_key } => run_cli_bench(&url, clients, calls, warmup, api_key.as_deref()).await,
    }
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

    // Project name
    let project_name: String = match &path {
        Some(p) => p.file_name().and_then(|n| n.to_str()).unwrap_or("my-project").to_string(),
        None => Input::with_theme(&theme)
            .with_prompt("Project name")
            .default("my-project".to_string())
            .interact_text()
            .map_err(|e| neondb::error::NeonDBError::internal(format!("Prompt error: {}", e)))?,
    };

    // Project path
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

    // Template selection
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

    // Write files shared by every template
    write_shared_files(&project_path, &project_name, &template_name)?;

    // Dispatch to per-template scaffolder
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
// rust/basic
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
    println!("  Next steps:");
    println!("    cd {}", name);
    println!("    neondb start");
    println!("    neondb call register '[\"alice\", \"secret123\", \"user\"]'");
    println!("    neondb call login    '[\"alice\", \"secret123\"]'");
    println!("    neondb watch \"users WHERE id = 'alice'\"");
    println!();
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// rust/game-ready
// ─────────────────────────────────────────────────────────────────────────────

fn scaffold_rust_game_ready(p: &Path, name: &str) -> Result<()> {
    // Players / world
    wf(p, "modules/players/spawn.js",           GAME_SPAWN_JS)?;
    wf(p, "modules/players/despawn.js",         GAME_DESPAWN_JS)?;
    wf(p, "modules/players/move.js",            GAME_MOVE_JS)?;
    wf(p, "modules/players/update_stats.js",    GAME_UPDATE_STATS_JS)?;
    // Combat
    wf(p, "modules/combat/attack.js",           GAME_ATTACK_JS)?;
    wf(p, "modules/combat/use_ability.js",      GAME_USE_ABILITY_JS)?;
    wf(p, "modules/combat/apply_damage.js",     GAME_APPLY_DAMAGE_JS)?;
    wf(p, "modules/combat/respawn.js",          GAME_RESPAWN_JS)?;
    // Economy
    wf(p, "modules/economy/buy_item.js",        GAME_BUY_ITEM_JS)?;
    wf(p, "modules/economy/sell_item.js",       GAME_SELL_ITEM_JS)?;
    wf(p, "modules/economy/transfer_currency.js", GAME_TRANSFER_CURRENCY_JS)?;
    wf(p, "modules/economy/open_loot_box.js",   GAME_OPEN_LOOT_BOX_JS)?;
    // Quests
    wf(p, "modules/quests/accept_quest.js",     GAME_ACCEPT_QUEST_JS)?;
    wf(p, "modules/quests/complete_quest.js",   GAME_COMPLETE_QUEST_JS)?;
    wf(p, "modules/quests/update_progress.js",  GAME_UPDATE_PROGRESS_JS)?;
    // Matchmaking
    wf(p, "modules/matchmaking/queue.js",       GAME_QUEUE_JS)?;
    wf(p, "modules/matchmaking/dequeue.js",     GAME_DEQUEUE_JS)?;
    wf(p, "modules/matchmaking/create_match.js",GAME_CREATE_MATCH_JS)?;
    wf(p, "modules/matchmaking/refresh.js",     GAME_MATCHMAKING_REFRESH_JS)?;
    // Guilds
    wf(p, "modules/guilds/create.js",           GAME_GUILD_CREATE_JS)?;
    wf(p, "modules/guilds/invite.js",           GAME_GUILD_INVITE_JS)?;
    wf(p, "modules/guilds/accept_invite.js",    GAME_GUILD_ACCEPT_JS)?;
    wf(p, "modules/guilds/kick.js",             GAME_GUILD_KICK_JS)?;
    // World ticks / scheduled
    wf(p, "modules/world/world_tick.js",        GAME_WORLD_TICK_JS)?;
    wf(p, "modules/world/cleanup_sessions.js",  GAME_CLEANUP_SESSIONS_JS)?;
    // Leaderboard
    wf(p, "modules/leaderboard/submit_score.js",GAME_SUBMIT_SCORE_JS)?;
    wf(p, "modules/leaderboard/reset_weekly.js",GAME_RESET_WEEKLY_JS)?;
    // Client & docs
    wf(p, "client/game-client.ts",              GAME_CLIENT_TS)?;
    wf(p, "schema.toml",                        GAME_SCHEMA_TOML)?;
    wf(p, "GENRE_GUIDE.md",                     GAME_GENRE_GUIDE_MD)?;
    wf(p, "README.md", &format!("# {} — Game-Ready Template\n\n{}", name, GAME_README))?;

    print_success(name, "rust/game-ready", &[
        ("modules/players/",    "spawn, despawn, move, update_stats"),
        ("modules/combat/",     "attack, use_ability, apply_damage, respawn"),
        ("modules/economy/",    "buy_item, sell_item, transfer_currency, loot_box"),
        ("modules/quests/",     "accept, complete, update_progress"),
        ("modules/matchmaking/","queue, dequeue, create_match, refresh (scheduled)"),
        ("modules/guilds/",     "create, invite, accept_invite, kick"),
        ("modules/world/",      "world_tick (1s), cleanup_sessions (60s)"),
        ("modules/leaderboard/","submit_score, reset_weekly (scheduled)"),
        ("client/game-client.ts","TypeScript client with all systems wired"),
        ("schema.toml",         "full typed schema for all tables"),
        ("GENRE_GUIDE.md",      "how to adapt this template to any game genre"),
    ]);
    println!("  Next steps:");
    println!("    cd {}", name);
    println!("    neondb start");
    println!("    neondb call spawn '[\"player1\", 0, 0, \"warrior\"]'");
    println!("    neondb watch \"players WHERE zone = 'zone_0_0'\"");
    println!("    neondb call attack '[\"player1\", \"enemy1\", \"sword\", 25]'");
    println!("    # See GENRE_GUIDE.md to adapt this to your game type.");
    println!();
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// rust/chat
// ─────────────────────────────────────────────────────────────────────────────

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
        ("client/chat-client.ts","TypeScript chat client"),
        ("schema.toml",         "typed schema for all tables"),
        ("neondb.toml",         "config with presence cleanup scheduler"),
    ]);
    println!("  Next steps:");
    println!("    cd {}", name);
    println!("    neondb start");
    println!("    neondb call create_room '[\"general\", \"alice\", \"General\"]'");
    println!("    neondb call join_room   '[\"general\", \"bob\"]'");
    println!("    neondb watch \"messages WHERE room_id = 'general'\"");
    println!("    neondb call send_message '[\"general\", \"alice\", \"Hello!\"]'");
    println!();
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// typescript
// ─────────────────────────────────────────────────────────────────────────────

fn scaffold_typescript(p: &Path, name: &str) -> Result<()> {
    wf(p, "modules/hello.js",                  TS_HELLO_JS)?;
    wf(p, "modules/set_value.js",              TS_SET_VALUE_JS)?;
    wf(p, "modules/delete_value.js",           TS_DELETE_VALUE_JS)?;
    wf(p, "client/src/client.ts",              TS_CLIENT_TS)?;
    wf(p, "client/src/hooks.tsx",              TS_HOOKS_TSX)?;
    wf(p, "client/src/example/App.tsx",        TS_APP_TSX)?;
    wf(p, "client/package.json",               &format!("{}", TS_PACKAGE_JSON.replace("__NAME__", name)))?;
    wf(p, "client/tsconfig.json",              TS_TSCONFIG_JSON)?;
    wf(p, "README.md", &format!("# {} — TypeScript Template\n\n{}", name, TS_README))?;

    print_success(name, "typescript", &[
        ("modules/hello.js",         "basic counter reducer"),
        ("modules/set_value.js",     "set arbitrary key/value"),
        ("modules/delete_value.js",  "delete a key"),
        ("client/src/client.ts",     "NeonDBClient — connect, call, subscribe"),
        ("client/src/hooks.tsx",     "useNeonDBQuery, useNeonDBReducer, NeonDBProvider"),
        ("client/src/example/App.tsx","React example app using hooks"),
        ("client/package.json",      "npm package config"),
        ("client/tsconfig.json",     "TypeScript config"),
    ]);
    println!("  Next steps:");
    println!("    cd {}", name);
    println!("    neondb start           # start backend");
    println!("    cd client");
    println!("    npm install");
    println!("    npm run dev            # start React frontend");
    println!();
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// print_success
// ─────────────────────────────────────────────────────────────────────────────

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
// Shared embedded content
// ═══════════════════════════════════════════════════════════════════════════════

const MIGRATIONS_README: &str = r#"# Migrations
Place `.toml` files here (e.g. `001_add_email.toml`). NeonDB applies them
automatically at startup in lexicographic order. Each file is idempotent.

Example:
```toml
[[steps]]
op = "add_field"
table = "users"
field = "email"
default = ""
```
"#;

// ═══════════════════════════════════════════════════════════════════════════════
// rust/basic — reducers
// ═══════════════════════════════════════════════════════════════════════════════

const BASIC_REGISTER_JS: &str = r#"// register(username, password_hash, role)
// Creates a new user.  Role is "user" by default; only admins can set "admin".
// In production: hash passwords server-side; never store plaintext.
function reducer(args) {
  var username = args[0]; var password_hash = args[1]; var role = args[2] || "user";
  if (!username || !password_hash) return { error: "username and password_hash required" };
  if (__neondb_get("users", username)) return { error: "Username already taken" };
  var now = Date.now();
  __neondb_set("users", username, {
    id: username, username: username, password_hash: password_hash,
    role: role, created_at: now, updated_at: now, active: true
  });
  __neondb_set("inventory", username, { player_id: username, items: [], currency: 0 });
  return { ok: true, user_id: username, role: role };
}
"#;

const BASIC_LOGIN_JS: &str = r#"// login(username, password_hash) → session_token
// Creates a session valid for 24 hours.
function reducer(args) {
  var username = args[0]; var password_hash = args[1];
  if (!username || !password_hash) return { error: "username and password_hash required" };
  var user = __neondb_get("users", username);
  if (!user) return { error: "User not found" };
  if (!user.active) return { error: "Account disabled" };
  if (user.password_hash !== password_hash) return { error: "Invalid password" };
  var token = username + "_" + Date.now() + "_" + Math.random().toString(36).slice(2);
  var session_id = "session:" + token;
  __neondb_set("sessions", session_id, {
    id: session_id, user_id: username, role: user.role,
    token: token, created_at: Date.now(), expires_at: Date.now() + 86400000, active: true
  });
  return { ok: true, session_token: token, user_id: username, role: user.role };
}
"#;

const BASIC_LOGOUT_JS: &str = r#"// logout(session_token)
// Invalidates a session.
function reducer(args) {
  var token = args[0];
  if (!token) return { error: "session_token required" };
  var session_id = "session:" + token;
  var session = __neondb_get("sessions", session_id);
  if (!session) return { error: "Session not found" };
  session.active = false; session.logged_out_at = Date.now();
  __neondb_set("sessions", session_id, session);
  return { ok: true };
}
"#;

const BASIC_GRANT_ROLE_JS: &str = r#"// grant_role(target_username, new_role)
// Admin-only — enforced by [permissions] in neondb.toml.
// ctx.caller_role is checked by the server before this reducer runs.
function reducer(args) {
  var target = args[0]; var new_role = args[1];
  if (!target || !new_role) return { error: "target_username and new_role required" };
  var user = __neondb_get("users", target);
  if (!user) return { error: "User not found" };
  var old_role = user.role;
  user.role = new_role; user.updated_at = Date.now();
  __neondb_set("users", target, user);
  return { ok: true, user_id: target, old_role: old_role, new_role: new_role };
}
"#;

const BASIC_UPDATE_PROFILE_JS: &str = r#"// update_profile(username, display_name, avatar_url)
function reducer(args) {
  var username = args[0]; var display_name = args[1]; var avatar_url = args[2];
  if (!username) return { error: "username required" };
  var user = __neondb_get("users", username);
  if (!user) return { error: "User not found" };
  if (display_name) user.display_name = display_name;
  if (avatar_url)   user.avatar_url   = avatar_url;
  user.updated_at = Date.now();
  __neondb_set("users", username, user);
  return { ok: true, user_id: username };
}
"#;

const BASIC_DELETE_USER_JS: &str = r#"// delete_user(username)
// Admin-only — enforced by [permissions] in neondb.toml.
function reducer(args) {
  var username = args[0];
  if (!username) return { error: "username required" };
  var user = __neondb_get("users", username);
  if (!user) return { error: "User not found" };
  // Soft-delete: mark inactive rather than removing data.
  user.active = false; user.deleted_at = Date.now(); user.updated_at = Date.now();
  __neondb_set("users", username, user);
  return { ok: true, user_id: username };
}
"#;

const BASIC_ADD_ITEM_JS: &str = r#"// add_item(player_id, item_id, quantity, item_name)
function reducer(args) {
  var player_id = args[0]; var item_id = args[1]; var qty = args[2] || 1; var name = args[3] || item_id;
  if (!player_id || !item_id) return { error: "player_id and item_id required" };
  var inv = __neondb_get("inventory", player_id) || { player_id: player_id, items: [], currency: 0 };
  var existing = inv.items.find(function(i) { return i.id === item_id; });
  if (existing) { existing.quantity += qty; }
  else { inv.items.push({ id: item_id, name: name, quantity: qty, added_at: Date.now() }); }
  inv.updated_at = Date.now();
  __neondb_set("inventory", player_id, inv);
  return { ok: true, player_id: player_id, item_id: item_id, quantity: qty };
}
"#;

const BASIC_REMOVE_ITEM_JS: &str = r#"// remove_item(player_id, item_id, quantity)
function reducer(args) {
  var player_id = args[0]; var item_id = args[1]; var qty = args[2] || 1;
  if (!player_id || !item_id) return { error: "player_id and item_id required" };
  var inv = __neondb_get("inventory", player_id);
  if (!inv) return { error: "Inventory not found" };
  var item = inv.items.find(function(i) { return i.id === item_id; });
  if (!item) return { error: "Item not in inventory" };
  if (item.quantity < qty) return { error: "Not enough quantity" };
  item.quantity -= qty;
  if (item.quantity === 0) inv.items = inv.items.filter(function(i) { return i.id !== item_id; });
  inv.updated_at = Date.now();
  __neondb_set("inventory", player_id, inv);
  return { ok: true, player_id: player_id, item_id: item_id, remaining: item.quantity };
}
"#;

const BASIC_SUB_PLAYER_JS: &str = r#"// subscribe_to_player(watcher_id, target_player_id)
// Creates a subscription record so the server knows to notify watcher
// whenever target_player changes.  The client still calls neondb.watch().
function reducer(args) {
  var watcher = args[0]; var target = args[1];
  if (!watcher || !target) return { error: "watcher_id and target_player_id required" };
  var sub_key = watcher + ":watches:" + target;
  __neondb_set("player_subscriptions", sub_key, {
    id: sub_key, watcher: watcher, target: target, since: Date.now()
  });
  return { ok: true, watching: target };
}
"#;

const BASIC_CLIENT_TS: &str = r#"/**
 * rust/basic template — TypeScript client example.
 * Run: neondb start  then  npx ts-node client/example.ts
 */
import { NeonDBClient } from "@neondb/client";

const db = new NeonDBClient({ url: "ws://localhost:3000" });

async function main() {
  await db.connect();

  // Register + login
  const reg = await db.call("register", ["alice", "hashed_pw_here", "user"]);
  console.log("Registered:", reg);

  const login = await db.call("login", ["alice", "hashed_pw_here"]);
  console.log("Session token:", login.session_token);

  // Watch this user's inventory in real-time
  const unsub = db.subscribe("inventory WHERE player_id = 'alice'", (diff) => {
    console.log("[inventory diff]", diff.operation, diff.rowData);
  });

  // Add items
  await db.call("add_item", ["alice", "sword_01", 1, "Iron Sword"]);
  await db.call("add_item", ["alice", "potion_hp", 5, "Health Potion"]);

  // Remove one potion
  await db.call("remove_item", ["alice", "potion_hp", 1]);

  unsub();
  await db.disconnect();
}

main().catch(console.error);
"#;

const BASIC_SCHEMA_TOML: &str = r#"# schema.toml — typed column definitions for rust/basic template.
# NeonDB validates all set_row calls against these schemas.
# Tables not listed here are schema-free (accept any JSON).

[users]
id           = "string"
username     = "string"
password_hash= "string"
role         = "string"
display_name = "string"
avatar_url   = "string"
active       = "bool"
created_at   = "u64"
updated_at   = "u64"

[sessions]
id         = "string"
user_id    = "string"
role       = "string"
token      = "string"
active     = "bool"
created_at = "u64"
expires_at = "u64"

[player_subscriptions]
id      = "string"
watcher = "string"
target  = "string"
since   = "u64"
"#;

const BASIC_README: &str = r#"## Tables

| Table                 | Purpose                                         |
|-----------------------|-------------------------------------------------|
| `users`               | User accounts with roles and profile data        |
| `sessions`            | Active login sessions with expiry                |
| `inventory`           | Per-player item lists and currency               |
| `player_subscriptions`| Who is watching whose profile                   |

## Reducers

| Reducer                | Args                                      | Role required |
|------------------------|-------------------------------------------|---------------|
| `register`             | username, password_hash, role             | open          |
| `login`                | username, password_hash                   | open          |
| `logout`               | session_token                             | open          |
| `grant_role`           | target_username, new_role                 | admin         |
| `update_profile`       | username, display_name, avatar_url        | open          |
| `delete_user`          | username                                  | admin         |
| `add_item`             | player_id, item_id, quantity, name        | open          |
| `remove_item`          | player_id, item_id, quantity              | open          |
| `subscribe_to_player`  | watcher_id, target_player_id             | open          |

## Quick start

```bash
neondb start
neondb call register '["alice", "hashed_pw", "user"]'
neondb call login    '["alice", "hashed_pw"]'
neondb watch "inventory WHERE player_id = 'alice'"
neondb call add_item '["alice", "sword_01", 1, "Iron Sword"]'
```

## Extending this template

- Add OAuth/JWT: store the token returned by your identity provider in the
  `sessions` table instead of a simple password hash.
- Add friends list: create a `friendships` table with `player_a`, `player_b`,
  `status` (pending/accepted) and reducers `send_friend_request` / `accept`.
- Add achievements: `achievements` table, `unlock_achievement` reducer that
  checks conditions before writing.
"#;

// ═══════════════════════════════════════════════════════════════════════════════
// rust/game-ready — reducers
// ═══════════════════════════════════════════════════════════════════════════════

const GAME_SPAWN_JS: &str = r#"// spawn(player_id, x, y, class_name)
// Creates a player entity in the world.
function reducer(args) {
  var pid = args[0]; var x = args[1] || 0; var y = args[2] || 0; var cls = args[3] || "warrior";
  if (!pid) return { error: "player_id required" };
  if (__neondb_get("players", pid)) return { error: "Player already exists — call despawn first" };
  var zone = "zone_" + Math.floor(x / 100) + "_" + Math.floor(y / 100);
  var base = { warrior: { hp: 200, mp: 50, atk: 30, def: 20 }, mage: { hp: 120, mp: 200, atk: 60, def: 10 }, rogue: { hp: 150, mp: 100, atk: 45, def: 15 } };
  var stats = base[cls] || base.warrior;
  __neondb_set("players", pid, {
    id: pid, x: x, y: y, zone: zone, class: cls,
    hp: stats.hp, max_hp: stats.hp, mp: stats.mp, max_mp: stats.mp,
    atk: stats.atk, def: stats.def, level: 1, xp: 0,
    status: "alive", spawned_at: Date.now(), last_action: Date.now()
  });
  return { ok: true, player_id: pid, zone: zone, class: cls, stats: stats };
}
"#;

const GAME_DESPAWN_JS: &str = r#"// despawn(player_id)
function reducer(args) {
  var pid = args[0];
  if (!pid) return { error: "player_id required" };
  var player = __neondb_get("players", pid);
  if (!player) return { error: "Player not found" };
  player.status = "offline"; player.last_action = Date.now();
  __neondb_set("players", pid, player);
  return { ok: true, player_id: pid };
}
"#;

const GAME_MOVE_JS: &str = r#"// move(player_id, x, y)
function reducer(args) {
  var pid = args[0]; var x = args[1]; var y = args[2];
  if (!pid || typeof x !== "number" || typeof y !== "number") return { error: "player_id, x, y required" };
  var player = __neondb_get("players", pid);
  if (!player) return { error: "Player not found — call spawn first" };
  if (player.status !== "alive") return { error: "Player is not alive" };
  var new_zone = "zone_" + Math.floor(x / 100) + "_" + Math.floor(y / 100);
  player.x = x; player.y = y; player.zone = new_zone; player.last_action = Date.now();
  __neondb_set("players", pid, player);
  return { ok: true, player_id: pid, x: x, y: y, zone: new_zone };
}
"#;

const GAME_UPDATE_STATS_JS: &str = r#"// update_stats(player_id, xp_gain, level_up)
function reducer(args) {
  var pid = args[0]; var xp_gain = args[1] || 0; var force_level = args[2] || false;
  var player = __neondb_get("players", pid);
  if (!player) return { error: "Player not found" };
  player.xp = (player.xp || 0) + xp_gain;
  var xp_for_next = player.level * 1000;
  if (force_level || player.xp >= xp_for_next) {
    player.level += 1; player.xp = 0;
    player.max_hp = Math.floor(player.max_hp * 1.1);
    player.hp = player.max_hp;
    player.atk = Math.floor(player.atk * 1.05);
  }
  player.last_action = Date.now();
  __neondb_set("players", pid, player);
  return { ok: true, player_id: pid, level: player.level, xp: player.xp };
}
"#;

const GAME_ATTACK_JS: &str = r#"// attack(attacker_id, target_id, weapon_type, damage_override)
// Validates zone, applies damage, publishes result.
function reducer(args) {
  var aid = args[0]; var tid = args[1]; var weapon = args[2] || "melee"; var dmg_override = args[3];
  var attacker = __neondb_get("players", aid);
  var target   = __neondb_get("players", tid);
  if (!attacker) return { error: "Attacker not found" };
  if (!target)   return { error: "Target not found" };
  if (attacker.zone !== target.zone) return { error: "Out of range — different zone" };
  if (target.status !== "alive")    return { error: "Target is already dead" };
  if (attacker.status !== "alive")  return { error: "Attacker is not alive" };
  var base_dmg = dmg_override || attacker.atk || 10;
  var mitigation = Math.floor((target.def || 0) * 0.5);
  var final_dmg = Math.max(1, base_dmg - mitigation);
  target.hp = Math.max(0, (target.hp || 0) - final_dmg);
  target.last_action = Date.now();
  var died = false;
  if (target.hp <= 0) { target.status = "dead"; target.died_at = Date.now(); died = true; }
  __neondb_set("players", tid, target);
  return { ok: true, attacker: aid, target: tid, damage: final_dmg, remaining_hp: target.hp, target_died: died };
}
"#;

const GAME_USE_ABILITY_JS: &str = r#"// use_ability(caster_id, target_id, ability_id)
// Costs MP, applies effect.  Add your ability definitions to the switch block.
function reducer(args) {
  var cid = args[0]; var tid = args[1]; var ability = args[2];
  var caster = __neondb_get("players", cid);
  var target  = __neondb_get("players", tid);
  if (!caster) return { error: "Caster not found" };
  if (!target)  return { error: "Target not found" };
  var effects = {
    fireball:   { mp_cost: 30, damage: 80, heal: 0 },
    heal:       { mp_cost: 20, damage: 0,  heal: 60 },
    shield:     { mp_cost: 15, damage: 0,  heal: 0, def_bonus: 20 },
    lightning:  { mp_cost: 45, damage: 120, heal: 0 }
  };
  var eff = effects[ability];
  if (!eff) return { error: "Unknown ability: " + ability };
  if ((caster.mp || 0) < eff.mp_cost) return { error: "Not enough MP" };
  caster.mp = (caster.mp || 0) - eff.mp_cost;
  if (eff.damage > 0) target.hp = Math.max(0, (target.hp || 0) - eff.damage);
  if (eff.heal   > 0) target.hp = Math.min(target.max_hp || 999, (target.hp || 0) + eff.heal);
  if (eff.def_bonus) target.def = (target.def || 0) + eff.def_bonus;
  if (target.hp <= 0) { target.status = "dead"; target.died_at = Date.now(); }
  __neondb_set("players", cid, caster);
  __neondb_set("players", tid, target);
  return { ok: true, ability: ability, caster: cid, target: tid, mp_remaining: caster.mp, target_hp: target.hp };
}
"#;

const GAME_APPLY_DAMAGE_JS: &str = r#"// apply_damage(target_id, amount, source_type)
// Generic damage entry point for environmental/DoT damage.
function reducer(args) {
  var tid = args[0]; var amount = args[1] || 0; var source = args[2] || "environment";
  var target = __neondb_get("players", tid);
  if (!target) return { error: "Target not found" };
  target.hp = Math.max(0, (target.hp || 0) - amount);
  if (target.hp <= 0) { target.status = "dead"; target.died_at = Date.now(); }
  target.last_action = Date.now();
  __neondb_set("players", tid, target);
  return { ok: true, target: tid, damage: amount, source: source, remaining_hp: target.hp };
}
"#;

const GAME_RESPAWN_JS: &str = r#"// respawn(player_id, x, y)
// Revives a dead player at given coordinates.
function reducer(args) {
  var pid = args[0]; var x = args[1] || 0; var y = args[2] || 0;
  var player = __neondb_get("players", pid);
  if (!player) return { error: "Player not found" };
  if (player.status === "alive") return { error: "Player is still alive" };
  var zone = "zone_" + Math.floor(x / 100) + "_" + Math.floor(y / 100);
  player.hp = Math.floor(player.max_hp * 0.5);
  player.status = "alive"; player.x = x; player.y = y; player.zone = zone;
  player.respawned_at = Date.now(); player.last_action = Date.now();
  __neondb_set("players", pid, player);
  return { ok: true, player_id: pid, zone: zone, hp: player.hp };
}
"#;

const GAME_BUY_ITEM_JS: &str = r#"// buy_item(player_id, item_id, item_name, price)
function reducer(args) {
  var pid = args[0]; var iid = args[1]; var iname = args[2] || iid; var price = args[3] || 0;
  var inv = __neondb_get("inventory", pid);
  if (!inv) return { error: "Inventory not found — player not registered" };
  if ((inv.currency || 0) < price) return { error: "Insufficient currency" };
  inv.currency -= price;
  var existing = (inv.items || []).find(function(i) { return i.id === iid; });
  if (existing) { existing.quantity += 1; }
  else { inv.items = (inv.items || []).concat([{ id: iid, name: iname, quantity: 1, bought_at: Date.now() }]); }
  inv.updated_at = Date.now();
  __neondb_set("inventory", pid, inv);
  return { ok: true, player_id: pid, item_id: iid, currency_remaining: inv.currency };
}
"#;

const GAME_SELL_ITEM_JS: &str = r#"// sell_item(player_id, item_id, sell_price)
function reducer(args) {
  var pid = args[0]; var iid = args[1]; var sell_price = args[2] || 0;
  var inv = __neondb_get("inventory", pid);
  if (!inv) return { error: "Inventory not found" };
  var item = (inv.items || []).find(function(i) { return i.id === iid; });
  if (!item || item.quantity < 1) return { error: "Item not in inventory" };
  item.quantity -= 1;
  if (item.quantity === 0) inv.items = inv.items.filter(function(i) { return i.id !== iid; });
  inv.currency = (inv.currency || 0) + sell_price;
  inv.updated_at = Date.now();
  __neondb_set("inventory", pid, inv);
  return { ok: true, player_id: pid, item_id: iid, currency_gained: sell_price, currency_total: inv.currency };
}
"#;

const GAME_TRANSFER_CURRENCY_JS: &str = r#"// transfer_currency(from_player, to_player, amount)
function reducer(args) {
  var from = args[0]; var to = args[1]; var amount = args[2] || 0;
  if (amount <= 0) return { error: "Amount must be positive" };
  var from_inv = __neondb_get("inventory", from);
  var to_inv   = __neondb_get("inventory", to);
  if (!from_inv) return { error: "Sender inventory not found" };
  if (!to_inv)   return { error: "Recipient inventory not found" };
  if ((from_inv.currency || 0) < amount) return { error: "Insufficient currency" };
  from_inv.currency -= amount;
  to_inv.currency    = (to_inv.currency || 0) + amount;
  from_inv.updated_at = to_inv.updated_at = Date.now();
  __neondb_set("inventory", from, from_inv);
  __neondb_set("inventory", to,   to_inv);
  return { ok: true, from: from, to: to, amount: amount };
}
"#;

const GAME_OPEN_LOOT_BOX_JS: &str = r#"// open_loot_box(player_id, box_type)
// Random loot table.  Extend loot_tables to add your items.
function reducer(args) {
  var pid = args[0]; var box_type = args[1] || "common";
  var inv = __neondb_get("inventory", pid);
  if (!inv) return { error: "Inventory not found" };
  var loot_tables = {
    common:    [{ id: "potion_hp",  name: "Health Potion",  w: 60 }, { id: "coin_10",   name: "10 Coins",    w: 30 }, { id: "gem_blue", name: "Blue Gem",    w: 10 }],
    rare:      [{ id: "sword_steel",name: "Steel Sword",    w: 40 }, { id: "armor_iron",name: "Iron Armor",  w: 35 }, { id: "gem_red",  name: "Red Gem",     w: 25 }],
    legendary: [{ id: "sword_legend",name:"Legendary Sword",w: 20 }, { id: "staff_mage",name: "Mage Staff",  w: 30 }, { id: "relic_01", name: "Ancient Relic",w: 50 }]
  };
  var table = loot_tables[box_type] || loot_tables.common;
  var roll = Math.random() * 100; var cum = 0; var chosen = table[table.length - 1];
  for (var i = 0; i < table.length; i++) { cum += table[i].w; if (roll < cum) { chosen = table[i]; break; } }
  var existing = (inv.items || []).find(function(i) { return i.id === chosen.id; });
  if (existing) { existing.quantity += 1; }
  else { inv.items = (inv.items || []).concat([{ id: chosen.id, name: chosen.name, quantity: 1, obtained_at: Date.now() }]); }
  inv.updated_at = Date.now();
  __neondb_set("inventory", pid, inv);
  return { ok: true, player_id: pid, item: chosen.id, item_name: chosen.name };
}
"#;

const GAME_ACCEPT_QUEST_JS: &str = r#"// accept_quest(player_id, quest_id, quest_name)
function reducer(args) {
  var pid = args[0]; var qid = args[1]; var qname = args[2] || qid;
  if (!pid || !qid) return { error: "player_id and quest_id required" };
  var key = pid + ":" + qid;
  if (__neondb_get("quests", key)) return { error: "Quest already accepted" };
  __neondb_set("quests", key, {
    id: key, player_id: pid, quest_id: qid, quest_name: qname,
    status: "active", progress: 0, target: 100, accepted_at: Date.now()
  });
  return { ok: true, player_id: pid, quest_id: qid };
}
"#;

const GAME_COMPLETE_QUEST_JS: &str = r#"// complete_quest(player_id, quest_id, xp_reward, currency_reward)
function reducer(args) {
  var pid = args[0]; var qid = args[1]; var xp = args[2] || 100; var gold = args[3] || 50;
  var key = pid + ":" + qid;
  var quest = __neondb_get("quests", key);
  if (!quest) return { error: "Quest not found" };
  if (quest.status !== "active") return { error: "Quest is not active" };
  if (quest.progress < quest.target) return { error: "Quest objective not complete yet" };
  quest.status = "completed"; quest.completed_at = Date.now();
  __neondb_set("quests", key, quest);
  var inv = __neondb_get("inventory", pid) || { player_id: pid, items: [], currency: 0 };
  inv.currency = (inv.currency || 0) + gold; inv.updated_at = Date.now();
  __neondb_set("inventory", pid, inv);
  return { ok: true, player_id: pid, quest_id: qid, xp_reward: xp, currency_reward: gold };
}
"#;

const GAME_UPDATE_PROGRESS_JS: &str = r#"// update_progress(player_id, quest_id, amount)
function reducer(args) {
  var pid = args[0]; var qid = args[1]; var amount = args[2] || 1;
  var key = pid + ":" + qid;
  var quest = __neondb_get("quests", key);
  if (!quest) return { error: "Quest not found" };
  if (quest.status !== "active") return { error: "Quest is not active" };
  quest.progress = Math.min(quest.target, (quest.progress || 0) + amount);
  quest.updated_at = Date.now();
  __neondb_set("quests", key, quest);
  var done = quest.progress >= quest.target;
  return { ok: true, player_id: pid, quest_id: qid, progress: quest.progress, target: quest.target, complete: done };
}
"#;

const GAME_QUEUE_JS: &str = r#"// queue(player_id, game_mode, rating)
// Adds player to matchmaking queue.
function reducer(args) {
  var pid = args[0]; var mode = args[1] || "ranked"; var rating = args[2] || 1000;
  if (!pid) return { error: "player_id required" };
  if (__neondb_get("matchmaking_queue", pid)) return { error: "Already in queue" };
  __neondb_set("matchmaking_queue", pid, {
    player_id: pid, mode: mode, rating: rating, queued_at: Date.now(), matched: false
  });
  return { ok: true, player_id: pid, mode: mode };
}
"#;

const GAME_DEQUEUE_JS: &str = r#"// dequeue(player_id)
function reducer(args) {
  var pid = args[0];
  if (!pid) return { error: "player_id required" };
  var entry = __neondb_get("matchmaking_queue", pid);
  if (!entry) return { error: "Not in queue" };
  entry.matched = false; entry.dequeued_at = Date.now();
  __neondb_set("matchmaking_queue", pid, entry);
  return { ok: true, player_id: pid };
}
"#;

const GAME_CREATE_MATCH_JS: &str = r#"// create_match(match_id, player1_id, player2_id, mode)
// Called by the matchmaking scheduler after finding two compatible players.
function reducer(args) {
  var mid = args[0]; var p1 = args[1]; var p2 = args[2]; var mode = args[3] || "ranked";
  if (!mid || !p1 || !p2) return { error: "match_id, player1_id, player2_id required" };
  __neondb_set("matches", mid, {
    id: mid, player1: p1, player2: p2, mode: mode,
    status: "active", winner: null, created_at: Date.now()
  });
  var e1 = __neondb_get("matchmaking_queue", p1); if (e1) { e1.matched = true; __neondb_set("matchmaking_queue", p1, e1); }
  var e2 = __neondb_get("matchmaking_queue", p2); if (e2) { e2.matched = true; __neondb_set("matchmaking_queue", p2, e2); }
  return { ok: true, match_id: mid, player1: p1, player2: p2 };
}
"#;

const GAME_MATCHMAKING_REFRESH_JS: &str = r#"// refresh()  — called by [[scheduler]] every 5 seconds.
// Simple rating-proximity matchmaking.  Replace with ELO or bracket logic.
function reducer(args) {
  var sentinel = __neondb_get("matchmaking_queue", "__tick__") || { tick: 0 };
  sentinel.tick = (sentinel.tick || 0) + 1; sentinel.last_run = Date.now();
  __neondb_set("matchmaking_queue", "__tick__", sentinel);
  // Real implementation: scan unmatched entries, pair by rating, call create_match.
  return { ok: true, tick: sentinel.tick };
}
"#;

const GAME_GUILD_CREATE_JS: &str = r#"// create_guild(guild_id, leader_id, guild_name)
function reducer(args) {
  var gid = args[0]; var leader = args[1]; var gname = args[2] || gid;
  if (!gid || !leader) return { error: "guild_id and leader_id required" };
  if (__neondb_get("guilds", gid)) return { error: "Guild already exists" };
  __neondb_set("guilds", gid, {
    id: gid, name: gname, leader: leader, members: [leader],
    level: 1, xp: 0, created_at: Date.now()
  });
  __neondb_set("guild_members", leader + ":" + gid, {
    player_id: leader, guild_id: gid, role: "leader", joined_at: Date.now()
  });
  return { ok: true, guild_id: gid, leader: leader };
}
"#;

const GAME_GUILD_INVITE_JS: &str = r#"// invite_to_guild(guild_id, inviter_id, invitee_id)
function reducer(args) {
  var gid = args[0]; var inviter = args[1]; var invitee = args[2];
  var guild = __neondb_get("guilds", gid);
  if (!guild) return { error: "Guild not found" };
  var inv_key = invitee + ":invite:" + gid;
  __neondb_set("guild_invites", inv_key, {
    id: inv_key, guild_id: gid, inviter: inviter, invitee: invitee,
    status: "pending", created_at: Date.now()
  });
  return { ok: true, guild_id: gid, invitee: invitee };
}
"#;

const GAME_GUILD_ACCEPT_JS: &str = r#"// accept_guild_invite(guild_id, player_id)
function reducer(args) {
  var gid = args[0]; var pid = args[1];
  var guild = __neondb_get("guilds", gid);
  if (!guild) return { error: "Guild not found" };
  var inv_key = pid + ":invite:" + gid;
  var invite = __neondb_get("guild_invites", inv_key);
  if (!invite || invite.status !== "pending") return { error: "No pending invite found" };
  invite.status = "accepted"; invite.accepted_at = Date.now();
  __neondb_set("guild_invites", inv_key, invite);
  if (!guild.members.includes(pid)) { guild.members.push(pid); __neondb_set("guilds", gid, guild); }
  __neondb_set("guild_members", pid + ":" + gid, {
    player_id: pid, guild_id: gid, role: "member", joined_at: Date.now()
  });
  return { ok: true, guild_id: gid, player_id: pid };
}
"#;

const GAME_GUILD_KICK_JS: &str = r#"// kick_from_guild(guild_id, leader_id, target_player_id)
function reducer(args) {
  var gid = args[0]; var leader = args[1]; var target = args[2];
  var guild = __neondb_get("guilds", gid);
  if (!guild) return { error: "Guild not found" };
  if (guild.leader !== leader) return { error: "Only the leader can kick members" };
  if (target === leader) return { error: "Leader cannot kick themselves" };
  guild.members = guild.members.filter(function(m) { return m !== target; });
  __neondb_set("guilds", gid, guild);
  var mem = __neondb_get("guild_members", target + ":" + gid);
  if (mem) { mem.role = "kicked"; mem.kicked_at = Date.now(); __neondb_set("guild_members", target + ":" + gid, mem); }
  return { ok: true, guild_id: gid, kicked: target };
}
"#;

const GAME_WORLD_TICK_JS: &str = r#"// world_tick()  — called by [[scheduler]] every 1000ms.
// Apply passive regen, environmental effects, timers.
function reducer(args) {
  var tick = __neondb_get("world_state", "tick") || { count: 0, started_at: Date.now() };
  tick.count += 1; tick.last_tick = Date.now();
  __neondb_set("world_state", "tick", tick);
  // TODO: iterate over active status effects, apply DoT / HoT, expire buffs.
  // TODO: update NPC AI positions.
  // TODO: trigger world events at specific tick counts.
  return { ok: true, tick: tick.count };
}
"#;

const GAME_CLEANUP_SESSIONS_JS: &str = r#"// cleanup_sessions()  — called by [[scheduler]] every 60 seconds.
// Marks players as offline if they haven't acted recently.
function reducer(args) {
  var sentinel = __neondb_get("world_state", "cleanup") || { runs: 0 };
  sentinel.runs += 1; sentinel.last_run = Date.now();
  __neondb_set("world_state", "cleanup", sentinel);
  // Real implementation: scan players WHERE last_action < (now - 5min) AND status = "alive"
  // and call despawn on each.  Requires OR/scan query support.
  return { ok: true, run: sentinel.runs };
}
"#;

const GAME_SUBMIT_SCORE_JS: &str = r#"// submit_score(player_id, score, mode)
function reducer(args) {
  var pid = args[0]; var score = args[1]; var mode = args[2] || "default";
  if (!pid || typeof score !== "number") return { error: "player_id and score required" };
  var key = pid + ":" + mode;
  var existing = __neondb_get("leaderboard", key);
  var best = existing ? Math.max(existing.score, score) : score;
  __neondb_set("leaderboard", key, { id: key, player_id: pid, mode: mode, score: best, updated_at: Date.now() });
  return { ok: true, player_id: pid, mode: mode, best_score: best };
}
"#;

const GAME_RESET_WEEKLY_JS: &str = r#"// reset_weekly()  — add to [[scheduler]] with interval_ms = 604800000 (7 days).
function reducer(args) {
  var sentinel = __neondb_get("leaderboard", "__reset__") || { count: 0 };
  sentinel.count += 1; sentinel.last_reset = Date.now();
  __neondb_set("leaderboard", "__reset__", sentinel);
  return { ok: true, reset_number: sentinel.count };
}
"#;

const GAME_CLIENT_TS: &str = r#"/**
 * rust/game-ready template — TypeScript client.
 * Demonstrates connecting to all major systems.
 */
import { NeonDBClient } from "@neondb/client";

const db = new NeonDBClient({ url: "ws://localhost:3000" });

async function main() {
  await db.connect();

  // Spawn player
  const spawn = await db.call("spawn", ["player1", 0, 0, "warrior"]);
  console.log("Spawned:", spawn);

  // Watch zone
  const unsub = db.subscribe("players WHERE zone = 'zone_0_0'", (diff) => {
    console.log("[zone update]", diff.operation, diff.rowData?.id, diff.rowData?.hp);
  });

  // Move + combat
  await db.call("move",   ["player1", 50, 50]);
  await db.call("attack", ["player1", "enemy1", "sword", 30]);

  // Economy
  await db.call("buy_item",  ["player1", "potion_hp", "Health Potion", 10]);
  await db.call("open_loot_box", ["player1", "common"]);

  // Quest
  await db.call("accept_quest",    ["player1", "q001", "Slay 10 Goblins"]);
  await db.call("update_progress", ["player1", "q001", 10]);

  // Matchmaking
  await db.call("queue", ["player1", "ranked", 1200]);

  // Guild
  await db.call("create_guild", ["guild_alpha", "player1", "Alpha Guild"]);

  unsub();
  await db.disconnect();
}

main().catch(console.error);
"#;

const GAME_SCHEMA_TOML: &str = r#"# schema.toml — rust/game-ready typed column definitions.

[players]
id          = "string"
x           = "f64"
y           = "f64"
zone        = "string"
class       = "string"
hp          = "f64"
max_hp      = "f64"
mp          = "f64"
max_mp      = "f64"
atk         = "f64"
def         = "f64"
level       = "f64"
xp          = "f64"
status      = "string"
spawned_at  = "u64"
last_action = "u64"

[matches]
id         = "string"
player1    = "string"
player2    = "string"
mode       = "string"
status     = "string"
created_at = "u64"

[guilds]
id         = "string"
name       = "string"
leader     = "string"
level      = "f64"
xp         = "f64"
created_at = "u64"

[leaderboard]
id         = "string"
player_id  = "string"
mode       = "string"
score      = "f64"
updated_at = "u64"
"#;

const GAME_GENRE_GUIDE_MD: &str = r#"# Genre Guide — How to build any game on this template

This template ships with a complete general-purpose game engine backend.
Below is a guide for the most common game genres.  In every case, the core
systems (players, combat, economy, matchmaking, guilds, world tick) stay
exactly as-is — you only need to *add* genre-specific reducers and tables.

---

## Action / Battle Royale

**What makes it different**: fast positional updates, shrinking zone, last
player standing win condition, high-frequency tick rate.

**Add these reducers:**
- `zone_shrink(tick_count, min_x, max_x, min_y, max_y)` — called by world_tick,
  updates the safe zone boundary stored in `world_state`.
- `check_zone_damage(player_id)` — called every tick: if player.x/y is outside
  safe zone, call `apply_damage`.
- `end_match(match_id, winner_id)` — sets match status = "finished".

**Tables to add:** `world_state` already exists.  Add `match_stats`
(kill count, damage dealt, placement).

**Subscriptions your client needs:**
```
players WHERE zone = 'zone_X_Y'      -- nearby players
world_state WHERE id = 'safe_zone'   -- safe zone boundary
matches WHERE id = 'my_match_id'     -- match result
```

---

## RPG / MMORPG

**What makes it different**: persistent world, NPC AI, deep quest chains,
party/raid groups, crafting, dungeons.

**Add these reducers:**
- `create_party(party_id, leader_id)` / `join_party` / `leave_party`
- `enter_dungeon(party_id, dungeon_id)` — spawns enemy entities
- `craft_item(player_id, recipe_id, ingredient_ids[])` — checks inventory,
  consumes ingredients, adds result
- `talk_to_npc(player_id, npc_id, dialogue_option)` — progresses quest dialogue

**Tables to add:** `parties`, `dungeons`, `npcs`, `recipes`, `dialogue_state`

**Tip:** use the `quests` table's `progress` field to track multi-step quest
chains.  Store `quest_data` as a JSON blob with step definitions.

---

## Turn-Based Strategy / Chess / Card Games

**What makes it different**: discrete turns, game state as a single authoritative
row, move validation is the critical path.

**Add these reducers:**
- `create_game(game_id, player1_id)` — write game state row
- `join_game(game_id, player2_id)` — mark game active
- `make_move(game_id, player_id, move_data)` — validate turn ownership,
  validate move legality, apply move, update `game.turn`.
- `resign(game_id, player_id)` — forfeit

**Tables to add:** `games` (id, player1, player2, turn, board_state, status,
move_history[]).

**Tip:** store `board_state` as a compact JSON string (FEN for chess, etc).
Keep `move_history` as an array inside the game row — NeonDB stores it as a
JSON blob so you get full history for free.

---

## Tower Defense / RTS

**What makes it different**: server is the authoritative simulation, clients
send commands, server broadcasts state each tick.

**Use `world_tick`** as your simulation step.  Each tick:
1. Move projectiles (stored in `projectiles` table).
2. Check collisions — if projectile hits enemy, call `apply_damage`.
3. Spawn waves of enemies (`enemies` table) at configured intervals.
4. Check win/lose condition.

**Add these reducers:**
- `place_tower(player_id, tower_id, x, y, tower_type)` — creates tower row
- `upgrade_tower(tower_id, upgrade_type)` — costs currency
- `sell_tower(tower_id)` — refunds partial currency

**Tables to add:** `towers`, `enemies`, `projectiles`, `waves`

**Tick rate:** set `interval_ms = 100` (10 Hz) in the world_tick scheduler
entry for smooth RTS simulation.

---

## Sports / Racing

**What makes it different**: race order matters, lap times, real-time position
leaderboard during the race.

**Add these reducers:**
- `start_race(race_id, player_ids[])` — sets all positions to start line
- `update_position(race_id, player_id, x, y, lap, checkpoint)` — called
  frequently by client; validates checkpoint order
- `finish_race(race_id, player_id)` — records finish time

**Subscriptions:**
```
players WHERE match_id = 'race_001'   -- live positions of all racers
leaderboard WHERE mode = 'lap_times'  -- best lap time board
```

---

## Social / Idle / Simulator

**What makes it different**: no real-time combat; emphasis on progression,
collection, social features.

The core template already gives you everything you need:
- `inventory` for collections
- `economy` for resource management
- `guilds` for social groups (rename to "clubs", "crews", etc.)
- `leaderboard` for progression ranking
- Scheduled reducers for offline progress (resource generation, building
  timers, etc.)

**Add these reducers:**
- `start_building(player_id, building_id, duration_ms)` — stores completion
  timestamp; scheduled reducer checks and completes it
- `collect_resources(player_id)` — calculates time-based accumulation,
  credits inventory
- `send_gift(from_id, to_id, item_id, quantity)` — uses transfer_currency
  pattern but for items

---

## General tips

- **Keep reducers small.** Each reducer should do one thing.  Chain them from
  the client rather than putting multi-step logic in one reducer.
- **Use zones.** The zone system (`zone_X_Y`) lets you filter subscriptions to
  only nearby entities.  For top-down games use 100×100 unit zones.  For FPS
  use smaller (10×10).
- **Read-your-writes.** NeonDB's ReducerContext lets a reducer read its own
  uncommitted writes.  Use this to chain logic within one reducer call.
- **Use the scheduler for cleanup.** Never rely on clients to clean up their
  own data.  Use scheduled reducers (cleanup_sessions) to handle
  disconnects, expired timers, and periodic resets.
- **Role-based auth is already wired.** Add reducer names to `[permissions]`
  in `neondb.toml` to restrict admin-only actions without touching reducer code.
"#;

const GAME_README: &str = r#"## Systems included

| System       | Reducers                                                    |
|--------------|-------------------------------------------------------------|
| Players      | spawn, despawn, move, update_stats                          |
| Combat       | attack, use_ability, apply_damage, respawn                  |
| Economy      | buy_item, sell_item, transfer_currency, open_loot_box       |
| Quests       | accept_quest, complete_quest, update_progress               |
| Matchmaking  | queue, dequeue, create_match, refresh (scheduled 5s)        |
| Guilds       | create_guild, invite, accept_invite, kick                   |
| World        | world_tick (scheduled 1s), cleanup_sessions (scheduled 60s) |
| Leaderboard  | submit_score, reset_weekly                                  |

## Quick start

```bash
neondb start
neondb call spawn '[["player1", 0, 0, "warrior"]]'
neondb watch "players WHERE zone = 'zone_0_0'"
neondb call attack '["player1", "enemy1", "sword", 25]'
neondb call buy_item '["player1", "potion_hp", "Health Potion", 10]'
```

## Where to go next

Read **GENRE_GUIDE.md** — it explains how to adapt this template to any game
genre (action, RPG, turn-based, RTS, sports, social) with minimal additions.

The systems above are intentionally generic.  You adapt them by:
1. Adding genre-specific reducers to `modules/`.
2. Tweaking stat formulas in the existing reducers.
3. Adding scheduler entries to `neondb.toml` for new timed systems.
4. Extending `schema.toml` with new table definitions.
"#;

// ═══════════════════════════════════════════════════════════════════════════════
// rust/chat — reducers
// ═══════════════════════════════════════════════════════════════════════════════

const CHAT_CREATE_ROOM_JS: &str = r#"// create_room(room_id, creator_id, display_name)
function reducer(args) {
  var rid = args[0]; var creator = args[1]; var display = args[2] || rid;
  if (!rid || !creator) return { error: "room_id and creator_id required" };
  if (__neondb_get("rooms", rid)) return { error: "Room already exists" };
  __neondb_set("rooms", rid, {
    id: rid, display_name: display, creator: creator,
    member_count: 1, created_at: Date.now(), active: true
  });
  __neondb_set("room_members", rid + ":" + creator, {
    room_id: rid, user_id: creator, role: "owner", joined_at: Date.now()
  });
  return { ok: true, room_id: rid };
}
"#;

const CHAT_JOIN_ROOM_JS: &str = r#"// join_room(room_id, user_id)
function reducer(args) {
  var rid = args[0]; var uid = args[1];
  if (!rid || !uid) return { error: "room_id and user_id required" };
  var room = __neondb_get("rooms", rid);
  if (!room || !room.active) return { error: "Room not found" };
  var member_key = rid + ":" + uid;
  if (__neondb_get("room_members", member_key)) return { ok: true, room_id: rid, already_member: true };
  __neondb_set("room_members", member_key, { room_id: rid, user_id: uid, role: "member", joined_at: Date.now() });
  room.member_count = (room.member_count || 0) + 1;
  __neondb_set("rooms", rid, room);
  return { ok: true, room_id: rid, member_count: room.member_count };
}
"#;

const CHAT_LEAVE_ROOM_JS: &str = r#"// leave_room(room_id, user_id)
function reducer(args) {
  var rid = args[0]; var uid = args[1];
  var room = __neondb_get("rooms", rid);
  if (!room) return { error: "Room not found" };
  var member_key = rid + ":" + uid;
  var member = __neondb_get("room_members", member_key);
  if (!member) return { error: "Not a member of this room" };
  member.role = "left"; member.left_at = Date.now();
  __neondb_set("room_members", member_key, member);
  room.member_count = Math.max(0, (room.member_count || 1) - 1);
  __neondb_set("rooms", rid, room);
  return { ok: true, room_id: rid };
}
"#;

const CHAT_DELETE_ROOM_JS: &str = r#"// delete_room(room_id, requester_id)
// Admin-only — enforced by [permissions] in neondb.toml.
function reducer(args) {
  var rid = args[0]; var requester = args[1];
  var room = __neondb_get("rooms", rid);
  if (!room) return { error: "Room not found" };
  room.active = false; room.deleted_at = Date.now(); room.deleted_by = requester;
  __neondb_set("rooms", rid, room);
  return { ok: true, room_id: rid };
}
"#;

const CHAT_SEND_MESSAGE_JS: &str = r#"// send_message(room_id, user_id, text)
function reducer(args) {
  var rid = args[0]; var uid = args[1]; var text = args[2];
  if (!rid || !uid || !text) return { error: "room_id, user_id, and text required" };
  var room = __neondb_get("rooms", rid);
  if (!room || !room.active) return { error: "Room not found" };
  if (!__neondb_get("room_members", rid + ":" + uid)) return { error: "Not a member of this room" };
  var msg_id = rid + ":" + Date.now() + ":" + uid;
  __neondb_set("messages", msg_id, {
    id: msg_id, room_id: rid, user_id: uid, text: text,
    sent_at: Date.now(), edited: false, deleted: false, reactions: {}
  });
  return { ok: true, message_id: msg_id };
}
"#;

const CHAT_EDIT_MESSAGE_JS: &str = r#"// edit_message(message_id, user_id, new_text)
function reducer(args) {
  var mid = args[0]; var uid = args[1]; var new_text = args[2];
  if (!mid || !uid || !new_text) return { error: "message_id, user_id, and new_text required" };
  var msg = __neondb_get("messages", mid);
  if (!msg) return { error: "Message not found" };
  if (msg.user_id !== uid) return { error: "Cannot edit another user's message" };
  if (msg.deleted) return { error: "Cannot edit a deleted message" };
  msg.text = new_text; msg.edited = true; msg.edited_at = Date.now();
  __neondb_set("messages", mid, msg);
  return { ok: true, message_id: mid };
}
"#;

const CHAT_DELETE_MESSAGE_JS: &str = r#"// delete_message(message_id, requester_id)
// Owner or moderator/admin can delete.
function reducer(args) {
  var mid = args[0]; var requester = args[1];
  var msg = __neondb_get("messages", mid);
  if (!msg) return { error: "Message not found" };
  msg.deleted = true; msg.deleted_at = Date.now(); msg.deleted_by = requester;
  msg.text = "[deleted]";
  __neondb_set("messages", mid, msg);
  return { ok: true, message_id: mid };
}
"#;

const CHAT_REACT_JS: &str = r#"// react(message_id, user_id, emoji)
// Toggles a reaction.  Emoji is a string like "👍", "❤️", "😂".
function reducer(args) {
  var mid = args[0]; var uid = args[1]; var emoji = args[2];
  if (!mid || !uid || !emoji) return { error: "message_id, user_id, and emoji required" };
  var msg = __neondb_get("messages", mid);
  if (!msg || msg.deleted) return { error: "Message not found" };
  var reactions = msg.reactions || {};
  var users = reactions[emoji] || [];
  var idx = users.indexOf(uid);
  if (idx === -1) { users.push(uid); } else { users.splice(idx, 1); }
  reactions[emoji] = users;
  msg.reactions = reactions;
  __neondb_set("messages", mid, msg);
  return { ok: true, message_id: mid, emoji: emoji, count: users.length };
}
"#;

const CHAT_CREATE_THREAD_JS: &str = r#"// create_thread(room_id, parent_message_id, user_id, text)
function reducer(args) {
  var rid = args[0]; var parent_id = args[1]; var uid = args[2]; var text = args[3];
  if (!rid || !parent_id || !uid || !text) return { error: "All args required" };
  var parent = __neondb_get("messages", parent_id);
  if (!parent || parent.deleted) return { error: "Parent message not found" };
  var thread_id = parent_id + ":thread:" + Date.now();
  __neondb_set("threads", thread_id, {
    id: thread_id, room_id: rid, parent_message_id: parent_id,
    user_id: uid, text: text, reply_count: 0, created_at: Date.now()
  });
  parent.thread_id = thread_id; parent.thread_reply_count = (parent.thread_reply_count || 0) + 1;
  __neondb_set("messages", parent_id, parent);
  return { ok: true, thread_id: thread_id };
}
"#;

const CHAT_REPLY_JS: &str = r#"// reply(thread_id, user_id, text)
function reducer(args) {
  var tid = args[0]; var uid = args[1]; var text = args[2];
  if (!tid || !uid || !text) return { error: "thread_id, user_id, and text required" };
  var thread = __neondb_get("threads", tid);
  if (!thread) return { error: "Thread not found" };
  var reply_id = tid + ":" + Date.now() + ":" + uid;
  __neondb_set("thread_replies", reply_id, {
    id: reply_id, thread_id: tid, user_id: uid, text: text, sent_at: Date.now()
  });
  thread.reply_count = (thread.reply_count || 0) + 1; thread.last_reply_at = Date.now();
  __neondb_set("threads", tid, thread);
  return { ok: true, reply_id: reply_id };
}
"#;

const CHAT_SET_ONLINE_JS: &str = r#"// set_online(user_id, status)  status: "online" | "away" | "dnd" | "offline"
function reducer(args) {
  var uid = args[0]; var status = args[1] || "online";
  if (!uid) return { error: "user_id required" };
  __neondb_set("presence", uid, { user_id: uid, status: status, updated_at: Date.now() });
  return { ok: true, user_id: uid, status: status };
}
"#;

const CHAT_SET_TYPING_JS: &str = r#"// set_typing(room_id, user_id, is_typing)
function reducer(args) {
  var rid = args[0]; var uid = args[1]; var typing = args[2] !== false;
  if (!rid || !uid) return { error: "room_id and user_id required" };
  var key = rid + ":" + uid;
  __neondb_set("typing_indicators", key, {
    room_id: rid, user_id: uid, is_typing: typing, updated_at: Date.now()
  });
  return { ok: true };
}
"#;

const CHAT_CLEANUP_PRESENCE_JS: &str = r#"// cleanup_expired_presence()  — called by [[scheduler]] every 30 seconds.
// Marks users offline if their presence hasn't been updated recently.
function reducer(args) {
  var sentinel = __neondb_get("presence", "__cleanup__") || { runs: 0 };
  sentinel.runs += 1; sentinel.last_run = Date.now();
  __neondb_set("presence", "__cleanup__", sentinel);
  // Real implementation: scan presence WHERE updated_at < (now - 60000)
  // and set status = "offline".  Requires OR/scan query support (TODO-020).
  return { ok: true, run: sentinel.runs };
}
"#;

const CHAT_BAN_USER_JS: &str = r#"// ban_user(target_user_id, moderator_id, reason, duration_ms)
// Admin/moderator-only — enforced by [permissions] in neondb.toml.
function reducer(args) {
  var target = args[0]; var mod_id = args[1]; var reason = args[2] || ""; var duration = args[3] || 0;
  if (!target) return { error: "target_user_id required" };
  var expires = duration > 0 ? Date.now() + duration : null;
  __neondb_set("bans", target, {
    user_id: target, moderator: mod_id, reason: reason,
    banned_at: Date.now(), expires_at: expires, active: true
  });
  return { ok: true, banned: target, expires_at: expires };
}
"#;

const CHAT_UNBAN_USER_JS: &str = r#"// unban_user(target_user_id, moderator_id)
// Admin/moderator-only.
function reducer(args) {
  var target = args[0]; var mod_id = args[1];
  var ban = __neondb_get("bans", target);
  if (!ban) return { error: "No ban record found" };
  ban.active = false; ban.unbanned_at = Date.now(); ban.unbanned_by = mod_id;
  __neondb_set("bans", target, ban);
  return { ok: true, unbanned: target };
}
"#;

const CHAT_CLIENT_TS: &str = r#"/**
 * rust/chat template — TypeScript client.
 * Run: neondb start  then  npx ts-node client/chat-client.ts
 */
import { NeonDBClient } from "@neondb/client";

const alice = new NeonDBClient({ url: "ws://localhost:3000" });
const bob   = new NeonDBClient({ url: "ws://localhost:3000" });

async function main() {
  await alice.connect();
  await bob.connect();

  // Create a room and join it
  await alice.call("create_room", ["general", "alice", "General"]);
  await bob.call("join_room",     ["general", "bob"]);

  // Both subscribe to the room's messages
  alice.subscribe("messages WHERE room_id = 'general'", (diff) => {
    const msg = diff.rowData as any;
    if (msg && !msg.deleted) console.log(`[${msg.user_id}] ${msg.text}`);
  });

  // Presence — show who's online
  alice.subscribe("presence", (diff) => {
    const p = diff.rowData as any;
    if (p) console.log(`[presence] ${p.user_id} is ${p.status}`);
  });

  await alice.call("set_online", ["alice", "online"]);
  await bob.call("set_online",   ["bob", "online"]);

  // Send messages
  const msg1 = await alice.call("send_message", ["general", "alice", "Hello everyone!"]);
  await bob.call("send_message", ["general", "bob", "Hey Alice!"]);

  // React to Alice's message
  await bob.call("react", [msg1.message_id, "bob", "👍"]);

  // Typing indicator
  await bob.call("set_typing", ["general", "bob", true]);
  await new Promise(r => setTimeout(r, 1000));
  await bob.call("set_typing", ["general", "bob", false]);

  await alice.disconnect();
  await bob.disconnect();
}

main().catch(console.error);
"#;

const CHAT_SCHEMA_TOML: &str = r#"# schema.toml — rust/chat typed column definitions.

[rooms]
id           = "string"
display_name = "string"
creator      = "string"
member_count = "f64"
active       = "bool"
created_at   = "u64"

[messages]
id       = "string"
room_id  = "string"
user_id  = "string"
text     = "string"
edited   = "bool"
deleted  = "bool"
sent_at  = "u64"

[presence]
user_id    = "string"
status     = "string"
updated_at = "u64"
"#;

const CHAT_README: &str = r#"## Tables

| Table             | Purpose                                    |
|-------------------|--------------------------------------------|
| `rooms`           | Room metadata, member count                |
| `room_members`    | Per-room membership with roles             |
| `messages`        | Chat messages (soft-delete, edit history)  |
| `threads`         | Thread root attached to a parent message   |
| `thread_replies`  | Replies inside a thread                    |
| `presence`        | Online/away/dnd/offline status per user    |
| `typing_indicators`| Who is currently typing in which room    |
| `bans`            | Active and historical bans                 |

## Reducers

| Reducer                  | Args                                  | Role     |
|--------------------------|---------------------------------------|----------|
| `create_room`            | room_id, creator_id, display_name     | open     |
| `join_room`              | room_id, user_id                      | open     |
| `leave_room`             | room_id, user_id                      | open     |
| `delete_room`            | room_id, requester_id                 | admin    |
| `send_message`           | room_id, user_id, text                | open     |
| `edit_message`           | message_id, user_id, new_text         | open     |
| `delete_message`         | message_id, requester_id              | open/mod |
| `react`                  | message_id, user_id, emoji            | open     |
| `create_thread`          | room_id, parent_message_id, uid, text | open     |
| `reply`                  | thread_id, user_id, text              | open     |
| `set_online`             | user_id, status                       | open     |
| `set_typing`             | room_id, user_id, is_typing           | open     |
| `cleanup_expired_presence`| (scheduler 30s)                      | scheduler|
| `ban_user`               | target_id, mod_id, reason, duration   | mod/admin|
| `unban_user`             | target_id, moderator_id               | mod/admin|

## Quick start

```bash
neondb start
neondb call create_room  '["general", "alice", "General Chat"]'
neondb call join_room    '["general", "bob"]'
neondb watch "messages WHERE room_id = 'general'"
neondb call send_message '["general", "alice", "Hello!"]'
neondb call react        '["general:1234:alice", "bob", "👍"]'
```

## Extending this template

- **Direct messages**: create a room per DM pair named `dm:user1:user2`,
  restricted to exactly those two members.
- **File attachments**: store a URL in the message row (`attachment_url` field);
  upload to S3/R2 from the client before calling `send_message`.
- **Read receipts**: `read_receipts` table with (room_id, user_id, last_read_at).
  Call a `mark_read` reducer when the user scrolls to the bottom.
- **Push notifications**: in the `send_message` reducer, look up subscribers
  who are offline in the `presence` table and push via your notification service.
"#;

// ═══════════════════════════════════════════════════════════════════════════════
// typescript — scaffolding
// ═══════════════════════════════════════════════════════════════════════════════

const TS_HELLO_JS: &str = r#"// hello(key, delta)  — basic counter reducer.
function reducer(args) {
  var key = args[0] || "default"; var delta = args[1] || 1;
  var row = __neondb_get("counters", key);
  var value = (row && typeof row.value === "number") ? row.value + delta : delta;
  __neondb_set("counters", key, { value: value, updated_at: Date.now() });
  return { ok: true, key: key, value: value };
}
"#;

const TS_SET_VALUE_JS: &str = r#"// set_value(table, key, json_value_string)
function reducer(args) {
  var table = args[0]; var key = args[1]; var json_str = args[2];
  if (!table || !key || !json_str) return { error: "table, key, and json_value required" };
  var value;
  try { value = JSON.parse(json_str); } catch(e) { return { error: "json_value is not valid JSON" }; }
  __neondb_set(table, key, value);
  return { ok: true, table: table, key: key };
}
"#;

const TS_DELETE_VALUE_JS: &str = r#"// delete_value(table, key)
function reducer(args) {
  var table = args[0]; var key = args[1];
  if (!table || !key) return { error: "table and key required" };
  if (!__neondb_get(table, key)) return { error: "Key not found" };
  __neondb_set(table, key, { __deleted__: true, deleted_at: Date.now() });
  return { ok: true, table: table, key: key };
}
"#;

const TS_CLIENT_TS: &str = r#"/**
 * NeonDB TypeScript client — connect, call, subscribe.
 * Install: npm install @neondb/client
 */
import { NeonDBClient } from "@neondb/client";

export function createClient(url = "ws://localhost:3000", apiKey?: string) {
  return new NeonDBClient({ url, apiKey });
}

// Example: one-shot counter increment
export async function incrementCounter(client: NeonDBClient, key: string, delta = 1) {
  return client.call("hello", [key, delta]);
}

// Example: subscribe to a table and get live rows
export function watchTable<T extends Record<string, unknown>>(
  client: NeonDBClient,
  table: string,
  onUpdate: (rows: Map<string, T>) => void
) {
  const rows = new Map<string, T>();
  return client.subscribe(`${table}`, (diff) => {
    if (diff.operation === "delete") {
      rows.delete(diff.rowKey);
    } else if (diff.rowData) {
      rows.set(diff.rowKey, diff.rowData as T);
    }
    onUpdate(new Map(rows));
  });
}
"#;

const TS_HOOKS_TSX: &str = r#"/**
 * NeonDB React hooks — useNeonDBQuery, useNeonDBReducer, NeonDBProvider.
 * Requires: npm install react @neondb/client
 */
import React, {
  createContext, useContext, useEffect, useRef, useState, useCallback,
} from "react";
import { NeonDBClient } from "@neondb/client";

// ── Context ───────────────────────────────────────────────────────────────────

const NeonDBContext = createContext<NeonDBClient | null>(null);

interface NeonDBProviderProps {
  url?: string;
  apiKey?: string;
  children: React.ReactNode;
}

export function NeonDBProvider({ url = "ws://localhost:3000", apiKey, children }: NeonDBProviderProps) {
  const clientRef = useRef<NeonDBClient | null>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    const client = new NeonDBClient({ url, apiKey });
    clientRef.current = client;
    client.connect().then(() => setReady(true)).catch(console.error);
    return () => { client.disconnect(); };
  }, [url, apiKey]);

  if (!ready || !clientRef.current) return null;
  return (
    <NeonDBContext.Provider value={clientRef.current}>
      {children}
    </NeonDBContext.Provider>
  );
}

export function useNeonDB() {
  const client = useContext(NeonDBContext);
  if (!client) throw new Error("useNeonDB must be used inside <NeonDBProvider>");
  return client;
}

// ── useNeonDBQuery ────────────────────────────────────────────────────────────

interface QueryResult<T> {
  rows: Map<string, T>;
  loading: boolean;
  error: string | null;
}

export function useNeonDBQuery<T extends Record<string, unknown>>(query: string): QueryResult<T> {
  const client = useNeonDB();
  const [rows, setRows]     = useState<Map<string, T>>(new Map());
  const [loading, setLoad]  = useState(true);
  const [error, setError]   = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    const localRows = new Map<string, T>();

    const unsub = client.subscribe(query, (diff) => {
      if (!live) return;
      if (diff.operation === "initial_snapshot" && diff.rowData) {
        localRows.set(diff.rowKey, diff.rowData as T);
        setRows(new Map(localRows));
        setLoad(false);
      } else if (diff.operation === "delete") {
        localRows.delete(diff.rowKey);
        setRows(new Map(localRows)); setLoad(false);
      } else if (diff.rowData) {
        localRows.set(diff.rowKey, diff.rowData as T);
        setRows(new Map(localRows)); setLoad(false);
      }
    });

    // Timeout if no snapshot arrives
    const t = setTimeout(() => { if (live) setLoad(false); }, 3000);
    return () => { live = false; clearTimeout(t); unsub(); };
  }, [client, query]);

  return { rows, loading, error };
}

// ── useNeonDBReducer ──────────────────────────────────────────────────────────

interface ReducerState {
  loading: boolean;
  error: string | null;
  lastResult: unknown;
}

export function useNeonDBReducer(reducerName: string) {
  const client = useNeonDB();
  const [state, setState] = useState<ReducerState>({ loading: false, error: null, lastResult: null });
  const mounted = useRef(true);
  useEffect(() => () => { mounted.current = false; }, []);

  const call = useCallback(async (...args: unknown[]) => {
    if (!mounted.current) return;
    setState(s => ({ ...s, loading: true, error: null }));
    try {
      const result = await client.call(reducerName, args);
      if (mounted.current) setState({ loading: false, error: null, lastResult: result });
      return result;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (mounted.current) setState({ loading: false, error: msg, lastResult: null });
      throw err;
    }
  }, [client, reducerName]);

  return [call, state] as const;
}
"#;

const TS_APP_TSX: &str = r#"/**
 * Example React app using NeonDB hooks.
 * Demonstrates a simple live counter.
 */
import React from "react";
import { NeonDBProvider, useNeonDBQuery, useNeonDBReducer } from "../hooks";

function Counter() {
  const { rows, loading } = useNeonDBQuery<{ value: number }>("counters");
  const [increment, { loading: sending }] = useNeonDBReducer("hello");

  if (loading) return <p>Loading...</p>;

  return (
    <div style={{ fontFamily: "sans-serif", padding: "2rem" }}>
      <h2>NeonDB Live Counter</h2>
      <ul>
        {Array.from(rows.entries()).map(([key, row]) => (
          <li key={key}>
            <strong>{key}</strong>: {row.value}
          </li>
        ))}
      </ul>
      <button onClick={() => increment("hits", 1)} disabled={sending}>
        {sending ? "..." : "Increment 'hits'"}
      </button>
    </div>
  );
}

export default function App() {
  return (
    <NeonDBProvider url="ws://localhost:3000">
      <Counter />
    </NeonDBProvider>
  );
}
"#;

const TS_PACKAGE_JSON: &str = r#"{
  "name": "__NAME__-client",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "dev":   "vite",
    "build": "tsc && vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "@neondb/client": "^0.1.0",
    "react":          "^18.2.0",
    "react-dom":      "^18.2.0"
  },
  "devDependencies": {
    "@types/react":     "^18.2.0",
    "@types/react-dom": "^18.2.0",
    "typescript":       "^5.3.0",
    "vite":             "^5.0.0",
    "@vitejs/plugin-react": "^4.2.0"
  }
}
"#;

const TS_TSCONFIG_JSON: &str = r#"{
  "compilerOptions": {
    "target": "ES2020",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "jsx": "react-jsx",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "outDir": "dist"
  },
  "include": ["src"]
}
"#;

const TS_README: &str = r#"## Structure

```
modules/          NeonDB backend reducers (JS, auto-loaded by server)
client/
  src/
    client.ts     NeonDBClient — connect, call, subscribe
    hooks.tsx     React hooks — useNeonDBQuery, useNeonDBReducer, NeonDBProvider
    example/
      App.tsx     Example React app with live counter
  package.json
  tsconfig.json
```

## Quick start

```bash
# Start the backend
neondb start

# In a separate terminal — install and run the React frontend
cd client
npm install
npm run dev      # opens http://localhost:5173
```

## Using the hooks

```tsx
import { NeonDBProvider, useNeonDBQuery, useNeonDBReducer } from "./hooks";

function MyComponent() {
  // Subscribe to a table — re-renders automatically on changes
  const { rows, loading } = useNeonDBQuery<{ value: number }>("counters");

  // Call a reducer
  const [increment, { loading: sending }] = useNeonDBReducer("hello");

  return (
    <button onClick={() => increment("score", 1)} disabled={sending}>
      Score: {rows.get("score")?.value ?? 0}
    </button>
  );
}

export default function App() {
  return (
    <NeonDBProvider url="ws://localhost:3000">
      <MyComponent />
    </NeonDBProvider>
  );
}
```

## API

### `useNeonDBQuery<T>(query: string)`
Returns `{ rows: Map<string, T>, loading: boolean, error: string | null }`.
Re-renders whenever the subscribed data changes.

### `useNeonDBReducer(reducerName: string)`
Returns `[call, { loading, error, lastResult }]`.
`call(...args)` fires the reducer.

### `NeonDBProvider`
Wraps your app.  All hooks must be inside it.
Props: `url` (default `"ws://localhost:3000"`), `apiKey` (optional).
"#;

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
        eprintln!("Error: 'javy' not found on PATH.\nDownload from: https://github.com/bytecodealliance/javy/releases");
        return Err(neondb::error::NeonDBError::internal("javy not found on PATH"));
    }

    // Collect .js files recursively
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
    else {
        println!("Build complete: {} compiled, {} failed.", compiled, failed);
        Err(neondb::error::NeonDBError::internal(format!("{} files failed to compile", failed)))
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

// ═══════════════════════════════════════════════════════════════════════════════
// Server bootstrap (unchanged from Session 28)
// ═══════════════════════════════════════════════════════════════════════════════

async fn run_server(config: Config) -> Result<()> {
    let mut logger = env_logger::Builder::from_default_env();
    logger.filter_level(config.log_level.parse().unwrap_or(log::LevelFilter::Info));
    let _ = logger.try_init();

    log::info!("Starting NeonDB Server");

    let mut ts = TableStore::new();
    ts.set_shard(config.shard_id, config.shard_count);
    let tables = Arc::new(ts);

    let registry = Arc::new(ReducerRegistry::new()?);
    log::info!("Available reducers: {:?}", registry.list_reducers());

    let mut min_wal_seq: u64 = 0;
    let mut initial_seq: u64 = 0;

    let snap_dir = config.snapshot_dir.clone();
    if let Some((snap_path, _snap_seq)) = find_latest_snapshot(&snap_dir) {
        log::info!("Loading snapshot: {:?}", snap_path);
        match load_snapshot(&snap_path, &tables) {
            Ok(meta) => {
                min_wal_seq = meta.last_sequence;
                initial_seq = meta.last_sequence.saturating_add(1);
                log::info!("Snapshot loaded: {} rows, replaying WAL from seq > {}", meta.row_count, meta.last_sequence);
            }
            Err(e) => log::warn!("Failed to load snapshot: {} — replaying full WAL", e),
        }
    }

    log::info!("Recovering from WAL: {:?}", config.wal_path);
    if config.wal_path.exists() {
        match recover_from_wal(&config.wal_path, &tables, min_wal_seq) {
            Ok((n, max_seq)) => {
                log::info!("Recovered {} entries from WAL (last seq={})", n, max_seq);
                initial_seq = initial_seq.max(max_seq.saturating_add(1));
            }
            Err(e) => log::warn!("Failed to recover from WAL: {}", e),
        }
    } else { log::info!("WAL file does not exist, starting fresh"); }

    let migrations_dir = std::path::PathBuf::from("migrations");
    match neondb::migrations::apply_migrations(&migrations_dir, &tables) {
        Ok(0) => log::debug!("No migrations to apply"),
        Ok(n) => log::info!("Applied {} migration file(s)", n),
        Err(e) => log::warn!("Migration error: {}", e),
    }

    let schema_registry = Arc::new(
        neondb::schema::SchemaRegistry::load_from_file(std::path::Path::new("schema.toml"))
            .unwrap_or_else(|e| {
                log::warn!("schema.toml load error: {} — running without type enforcement", e);
                neondb::schema::SchemaRegistry::new()
            })
    );
    if schema_registry.table_count() > 0 {
        log::info!("Schema: enforcing types for tables: {:?}", schema_registry.list_tables());
    }

    let permissions = Arc::new(config.permissions.clone());
    if !permissions.rules.is_empty() {
        log::info!("Permissions: {} reducer rules loaded", permissions.rules.len());
        for (reducer, roles) in &permissions.rules {
            log::info!("  {} → {:?}", reducer, roles);
        }
    } else {
        log::info!("Permissions: no restrictions (all reducers open)");
    }

    let (reducer_tx, reducer_rx) = kanal::unbounded_async::<PendingCall>();
    let subscription_manager = Arc::new(SubscriptionManager::new_with_options(config.two_frame_protocol));
    log::info!("Subscription fan-out mode: {}", if config.two_frame_protocol { "two-frame" } else { "legacy" });

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
        tokio::spawn(async move {
            if let Err(e) = start_metrics_server(host_c, mport, subs_c, tables_c, rx_shutdown).await {
                log::error!("Metrics server error: {}", e);
            }
        })
    };

    let wal_writer = Arc::new(BatchedWalWriter::open(
        &config.wal_path, config.wal_batch_interval_ms, config.wal_batch_size, config.unsafe_no_fsync,
    )?);
    let worker_count = num_cpus::get().max(1);
    log::info!("Starting {} parallel reducer workers", worker_count);

    let timeout_ms = config.reducer_timeout_ms;
    let snapshot_interval = config.snapshot_interval;
    let snapshot_dir_w = config.snapshot_dir.clone();
    let global_seq = Arc::new(std::sync::atomic::AtomicU64::new(initial_seq));

    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let rx = reducer_rx.clone(); let tables_w = tables.clone(); let registry_w = registry.clone();
        let subs_w = subscription_manager.clone(); let wal_w = wal_writer.clone();
        let seq_w = global_seq.clone(); let snap_interval_w = snapshot_interval;
        let snap_dir_ww = snapshot_dir_w.clone(); let schema_w = schema_registry.clone();

        let handle = tokio::spawn(async move {
            log::debug!("Reducer worker {} started", worker_id);
            loop {
                let call = match rx.recv().await { Ok(c) => c, Err(_) => break };
                let call_id          = call.call_id;
                let call_caller_id   = call.caller_id.clone();
                let call_caller_role = call.caller_role.clone();
                let tables_blk = tables_w.clone(); let registry_blk = registry_w.clone();
                let reducer_name = call.reducer_name.clone(); let args = call.args.clone();
                let timestamp = current_timestamp_nanos();
                let schema_for_blk = schema_w.clone();

                let blk_result = tokio::time::timeout(
                    std::time::Duration::from_millis(timeout_ms),
                    tokio::task::spawn_blocking(move || {
                        let mut ctx = ReducerContext::new(tables_blk, timestamp).with_schema(schema_for_blk);
                        ctx.caller_id   = call_caller_id;
                        ctx.caller_role = call_caller_role;
                        let exec = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                            || registry_blk.execute(&reducer_name, &mut ctx, &args)
                        ));
                        (exec, ctx)
                    }),
                ).await;

                let response = match blk_result {
                    Err(_) => { log::warn!("call_id={} timed out", call_id); ReducerResponse::error(call_id, "Reducer timed out".to_string()) }
                    Ok(Err(e)) => { log::error!("Join error: {}", e); ReducerResponse::error(call_id, "Internal task error".to_string()) }
                    Ok(Ok((exec_result, mut ctx))) => match exec_result {
                        Ok(Ok(result_bytes)) => match ctx.commit() {
                            Ok(deltas) => {
                                let seq_num = seq_w.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let entry = WalEntry::new(timestamp, seq_num, call.reducer_name.clone(), call.args.clone(), deltas.clone());
                                match wal_w.append(&entry, seq_num) {
                                    Err(e) => { log::error!("WAL append failed: {}", e); ReducerResponse::error(call_id, e.to_string()) }
                                    Ok(_) => {
                                        subs_w.publish_deltas(&deltas);
                                        if snap_interval_w > 0 && (seq_num + 1) % snap_interval_w == 0 {
                                            let tables_snap = tables_w.clone(); let dir_snap = snap_dir_ww.clone();
                                            let ts_snap = current_timestamp_nanos();
                                            tokio::spawn(async move {
                                                match tokio::task::spawn_blocking(move || save_snapshot(&tables_snap, &dir_snap, seq_num, ts_snap)).await {
                                                    Ok(Ok(())) => log::info!("Snapshot written at seq {}", seq_num),
                                                    Ok(Err(e)) => log::error!("Snapshot failed: {}", e),
                                                    Err(e) => log::error!("Snapshot panicked: {}", e),
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

                if let Err(e) = call.response_tx.send(response) { log::warn!("Failed to send response: {}", e); }
            }
            log::debug!("Reducer worker {} stopped", worker_id);
        });
        worker_handles.push(handle);
    }

    let mut scheduler_handles = Vec::new();
    let sched_seq = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX / 2));

    for sched in &config.scheduled_reducers {
        let sched: ScheduledReducerConfig = sched.clone(); let tx_sched = reducer_tx.clone();
        let seq_sched = sched_seq.clone(); let mut rx_shutdown_sched = shutdown_rx.clone();
        let args_bytes: Vec<u8> = sched.args_json.as_deref()
            .and_then(|j| serde_json::from_str::<serde_json::Value>(j).ok())
            .and_then(|v| rmp_serde::to_vec(&v).ok()).unwrap_or_default();
        log::info!("Scheduler: '{}' every {}ms", sched.reducer, sched.interval_ms);

        let handle = tokio::spawn(async move {
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
        });
        scheduler_handles.push(handle);
    }

    tokio::signal::ctrl_c().await.ok();
    log::info!("Shutdown signal received");
    let _ = shutdown_tx.send(());
    drop(reducer_tx);
    for h in worker_handles { let _ = h.await; }
    for h in scheduler_handles { let _ = h.await; }
    if let Ok(writer) = Arc::try_unwrap(wal_writer) {
        if let Err(e) = writer.shutdown() { log::error!("WAL shutdown error: {}", e); }
    }
    let _ = listener_handle.await;
    let _ = metrics_handle.await;
    log::info!("Shutdown complete");
    Ok(())
}

// ── Inline bench ──────────────────────────────────────────────────────────────

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

    println!("=== NeonDB Bench ===\n  Server  : {}\n  Clients : {}  Calls/client: {}  Warmup: {}", ws_url, num_clients, calls_per_client, warmup_per_client);

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
            let _ = ws.close(None).await;
            ok
        });
    }

    let mut total = 0usize;
    while let Some(r) = join_set.join_next().await { if let Ok(n) = r { total += n; } }
    let elapsed = start.elapsed();
    let tps = total as f64 / elapsed.as_secs_f64();
    println!("\nResults:");
    println!("  Time       : {:.3}s", elapsed.as_secs_f64());
    println!("  Throughput : {:.0} TPS", tps);
    println!("  Success    : {}/{}", total, num_clients * calls_per_client);
    if let Ok(h) = latencies.lock() {
        println!("  Latency (µs): p50={} p95={} p99={} max={}", h.value_at_percentile(50.0), h.value_at_percentile(95.0), h.value_at_percentile(99.0), h.max());
    }
    Ok(())
}

// ── Metrics server ────────────────────────────────────────────────────────────

async fn start_metrics_server(host: String, port: u16, subscription_manager: Arc<SubscriptionManager>, tables: Arc<TableStore>, mut shutdown: watch::Receiver<()>) -> Result<()> {
    let addr: SocketAddr = format!("{}:{}", host, port).parse()
        .map_err(|e| neondb::error::NeonDBError::invalid_argument(format!("Invalid metrics address: {}", e)))?;
    let make_service = make_service_fn(move |_| {
        let subs = subscription_manager.clone(); let tbl = tables.clone();
        async move { Ok::<_, hyper::Error>(service_fn(move |req| { let subs = subs.clone(); let tbl = tbl.clone(); async move { handle_metrics_request(req, subs, tbl).await } })) }
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

async fn handle_metrics_request(req: Request<Body>, subscription_manager: Arc<SubscriptionManager>, tables: Arc<TableStore>) -> Result<Response<Body>> {
    let path = req.uri().path().to_string();
    match (req.method(), path.as_str()) {
        (&Method::GET, "/metrics") => {
            let body = format!("# NeonDB metrics\nactive_subscriptions {}\nactive_connections {}\ntotal_rows {}\nuptime_nanos {}\n",
                subscription_manager.active_subscriptions(), subscription_manager.active_connections(), tables.total_row_count(), current_timestamp_nanos());
            Ok(Response::new(Body::from(body)))
        }
        (&Method::GET, "/healthz") => Ok(json_response(serde_json::json!({ "status": "ok", "total_rows": tables.total_row_count(), "active_connections": subscription_manager.active_connections() }))),
        (&Method::GET, "/tables") => {
            let mut table_list = Vec::new();
            for name in tables.list_tables() {
                let count = tables.list_rows_with_keys(&name).map(|r| r.len()).unwrap_or(0);
                table_list.push(serde_json::json!({ "name": name, "rows": count }));
            }
            Ok(json_response(serde_json::json!({ "tables": table_list, "total_rows": tables.total_row_count() })))
        }
        (&Method::GET, p) if p.starts_with("/tables/") => {
            let table_name = p.trim_start_matches("/tables/");
            match tables.list_rows_with_keys(table_name) {
                Ok(rows) => {
                    let row_objs: Vec<_> = rows.into_iter().map(|(key, data)| serde_json::json!({ "row_key": key, "data": data })).collect();
                    Ok(json_response(serde_json::json!({ "table": table_name, "count": row_objs.len(), "rows": row_objs })))
                }
                Err(e) => { let mut r = json_response(serde_json::json!({ "error": e.to_string() })); *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR; Ok(r) }
            }
        }
        _ => { let mut r = Response::new(Body::from("Not Found")); *r.status_mut() = StatusCode::NOT_FOUND; Ok(r) }
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

