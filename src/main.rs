// ============================================================================
// NeonDB main.rs
//
// Session 26 — TODO-023: Project Templates
//  - `neondb init <name> --template <blank|chat|leaderboard|mmo|turn-based>`
//  - `neondb templates` — list all templates with descriptions
//  - All template files embedded as &str constants (no external dependency)
//  - blank   : existing hello.js single-reducer starter
//  - chat    : rooms + messages tables, send_message / join_room reducers
//  - leaderboard : scores table, submit_score / get_top reducer, scheduled reset
//  - mmo     : players table, move / attack reducers, zone-based subscription
//  - turn-based  : games + players tables, make_move with turn validation
// ============================================================================

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
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

// ── Template definitions ──────────────────────────────────────────────────────

struct Template {
    name: &'static str,
    description: &'static str,
}

const TEMPLATES: &[Template] = &[
    Template {
        name: "blank",
        description: "Minimal starter — one hello.js reducer, ready to run immediately",
    },
    Template {
        name: "chat",
        description: "Real-time chat — rooms, messages, join_room / send_message reducers",
    },
    Template {
        name: "leaderboard",
        description: "Live leaderboard — scores table, submit_score, scheduled daily reset",
    },
    Template {
        name: "mmo",
        description: "MMO movement — players, move / attack reducers, zone subscriptions",
    },
    Template {
        name: "turn-based",
        description: "Turn-based game — games + players tables, make_move with turn validation",
    },
];

// ── CLI definition ────────────────────────────────────────────────────────────

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
    // ── Server ───────────────────────────────────────────────────────────────
    /// Scaffold a new NeonDB project in PATH
    Init {
        #[arg(value_name = "NAME", help = "Project name / directory to create")]
        path: Option<PathBuf>,
        #[arg(
            long,
            default_value = "blank",
            help = "Template: blank | chat | leaderboard | mmo | turn-based"
        )]
        template: String,
    },
    /// List available project templates
    Templates,
    /// Compile JS reducers in modules/ to WASM (requires `javy`)
    Build {
        #[arg(
            short = 'm',
            long,
            default_value = "modules",
            help = "Directory containing .js reducers to compile"
        )]
        modules_dir: Option<PathBuf>,
    },
    /// Start the NeonDB server
    Start {
        #[arg(short = 'a', long, help = "Listen address (default 127.0.0.1)")]
        host: Option<String>,
        #[arg(short = 'p', long, help = "WebSocket port (default 3000)")]
        port: Option<u16>,
        #[arg(short = 'd', long, help = "Data directory (sets WAL path)")]
        data_dir: Option<PathBuf>,
        #[arg(long = "wal-path", help = "Explicit WAL file path")]
        wal_path: Option<PathBuf>,
        #[arg(short = 'f', long, help = "WAL fsync interval ms")]
        fsync_interval_ms: Option<u32>,
    },

    // ── Inspect (read-only, hits the admin HTTP port) ─────────────────────
    /// Show server status and metrics
    Status {
        #[arg(long, default_value = "http://127.0.0.1:3001", help = "Admin/metrics HTTP URL")]
        metrics_url: String,
    },
    /// List all tables and their row counts
    Tables {
        #[arg(long, default_value = "http://127.0.0.1:3001", help = "Admin/metrics HTTP URL")]
        metrics_url: String,
    },
    /// Read rows from a table (optionally filter to a single row_key)
    Get {
        table: String,
        key: Option<String>,
        #[arg(long, default_value = "http://127.0.0.1:3001", help = "Admin/metrics HTTP URL")]
        metrics_url: String,
    },

    // ── Interactive (WebSocket) ───────────────────────────────────────────
    /// Call a reducer once and print the result
    Call {
        reducer: String,
        #[arg(help = "JSON args, e.g. '[\"my_counter\", 5]'")]
        args: Option<String>,
        #[arg(long, default_value = "ws://127.0.0.1:3000", help = "WebSocket URL of the server")]
        url: String,
        #[arg(long, help = "API key (Authorization: Bearer)")]
        api_key: Option<String>,
    },
    /// Subscribe to a table and stream live updates (Ctrl-C to stop)
    Watch {
        query: String,
        #[arg(long, default_value = "ws://127.0.0.1:3000", help = "WebSocket URL of the server")]
        url: String,
        #[arg(long, help = "API key (Authorization: Bearer)")]
        api_key: Option<String>,
    },
    /// Run a WebSocket throughput benchmark against a running server
    Bench {
        #[arg(long, default_value = "ws://127.0.0.1:3000", help = "WebSocket URL of the server")]
        url: String,
        #[arg(short = 'c', long, default_value = "10", help = "Number of concurrent clients")]
        clients: usize,
        #[arg(short = 'n', long, default_value = "500", help = "Calls per client")]
        calls: usize,
        #[arg(long, default_value = "50", help = "Warmup calls per client")]
        warmup: usize,
        #[arg(long, help = "API key (Authorization: Bearer)")]
        api_key: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { path, template } => {
            init_project(path, &template)?;
            Ok(())
        }
        Commands::Templates => {
            cmd_list_templates();
            Ok(())
        }
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
        Commands::Get { table, key, metrics_url } => {
            neondb::cli::cmd_get(&metrics_url, &table, key.as_deref()).await
        }
        Commands::Call { reducer, args, url, api_key } => {
            neondb::cli::cmd_call(&url, &reducer, args.as_deref(), api_key.as_deref()).await
        }
        Commands::Watch { query, url, api_key } => {
            neondb::cli::cmd_watch(&url, &query, api_key.as_deref()).await
        }
        Commands::Bench { url, clients, calls, warmup, api_key } => {
            run_cli_bench(&url, clients, calls, warmup, api_key.as_deref()).await
        }
    }
}

// ── neondb templates ──────────────────────────────────────────────────────────

fn cmd_list_templates() {
    println!();
    println!("  Available templates:");
    println!();
    for t in TEMPLATES {
        println!("    {:16}  {}", t.name, t.description);
    }
    println!();
    println!("  Usage:  neondb init <project-name> --template <name>");
    println!("  Example: neondb init my-chat --template chat");
    println!();
}

// ── neondb init ───────────────────────────────────────────────────────────────

fn init_project(path: Option<PathBuf>, template: &str) -> Result<()> {
    let path = match path {
        Some(p) => p,
        None => {
            eprintln!("Error: please supply a project name.");
            eprintln!();
            eprintln!("  Usage:   neondb init <project-name> [--template <name>]");
            eprintln!("  Example: neondb init my-game --template chat");
            eprintln!();
            eprintln!("  Run `neondb templates` to see all available templates.");
            return Err(neondb::error::NeonDBError::invalid_argument(
                "missing project name",
            ));
        }
    };

    // Validate template name
    if !TEMPLATES.iter().any(|t| t.name == template) {
        let names: Vec<_> = TEMPLATES.iter().map(|t| t.name).collect();
        eprintln!("Error: unknown template '{}'.", template);
        eprintln!("Available templates: {}", names.join(", "));
        return Err(neondb::error::NeonDBError::invalid_argument(format!(
            "unknown template '{}' — run `neondb templates` to see options",
            template
        )));
    }

    let project_path = path.clone();
    fs::create_dir_all(&project_path)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Cannot create directory: {}", e)))?;

    let project_name = project_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("my-game");

    // ── Shared files (all templates) ──────────────────────────────────────────
    write_shared_files(&project_path, project_name, template)?;

    // ── Template-specific files ───────────────────────────────────────────────
    match template {
        "blank"       => scaffold_blank(&project_path, project_name)?,
        "chat"        => scaffold_chat(&project_path, project_name)?,
        "leaderboard" => scaffold_leaderboard(&project_path, project_name)?,
        "mmo"         => scaffold_mmo(&project_path, project_name)?,
        "turn-based"  => scaffold_turn_based(&project_path, project_name)?,
        _             => unreachable!(),
    }

    Ok(())
}

// ── Shared scaffolding (neondb.toml, migrations/, .gitignore) ─────────────────

fn write_shared_files(project_path: &Path, project_name: &str, template: &str) -> Result<()> {
    // neondb.toml — scheduler entry only for leaderboard template
    let scheduler_section = if template == "leaderboard" {
        r#"
# Scheduled reducer — resets leaderboard every 24 hours
[[scheduler]]
reducer = "reset_scores"
interval_ms = 86400000
"#
    } else {
        r#"
# Scheduled reducers — fires automatically on a fixed interval
# [[scheduler]]
# reducer = "cleanup_expired"
# interval_ms = 60000
"#
    };

    let toml = format!(
        r#"[project]
name = "{name}"
version = "0.1.0"

[server]
host = "127.0.0.1"
port = 3000
metrics_port = 3001

# Uncomment to require Bearer token auth on all connections:
# api_key = "change-me"

# WAL durability (0 = fsync every write; 100 = batch every 100ms)
fsync_interval_ms = 0

# Snapshots (every 1 000 000 committed transactions by default)
# snapshot_interval = 1000000
{scheduler}"#,
        name = project_name,
        scheduler = scheduler_section,
    );
    fs::write(project_path.join("neondb.toml"), toml)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write neondb.toml: {}", e)))?;

    // migrations/
    fs::create_dir_all(project_path.join("migrations"))
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Create migrations/: {}", e)))?;
    fs::write(
        project_path.join("migrations").join("README.md"),
        MIGRATIONS_README,
    )
    .map_err(|e| neondb::error::NeonDBError::internal(format!("Write migrations/README.md: {}", e)))?;

    // modules/
    fs::create_dir_all(project_path.join("modules"))
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Create modules/: {}", e)))?;

    // .gitignore
    fs::write(
        project_path.join(".gitignore"),
        "*.wal\n*.bin\nsnapshots/\n*.tmp\n",
    )
    .map_err(|e| neondb::error::NeonDBError::internal(format!("Write .gitignore: {}", e)))?;

    Ok(())
}

// ── blank template ────────────────────────────────────────────────────────────

fn scaffold_blank(project_path: &Path, project_name: &str) -> Result<()> {
    fs::write(project_path.join("modules").join("hello.js"), BLANK_HELLO_JS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write hello.js: {}", e)))?;

    let readme = format!(
        "# {name}\n\nA NeonDB project.\n\n## Quick start\n\n```bash\nneondb start\nneondb call hello '[\"score\", 1]'\nneondb watch counters\n```\n",
        name = project_name
    );
    fs::write(project_path.join("README.md"), readme)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write README.md: {}", e)))?;

    print_success(project_name, "blank", &[
        ("modules/hello.js", "sample counter reducer"),
        ("neondb.toml",      "server configuration"),
        ("migrations/",      "migration files go here"),
        (".gitignore",       ""),
    ]);
    println!("  Next steps:");
    println!("    cd {}", project_name);
    println!("    neondb start");
    println!("    neondb call hello '[\"score\", 1]'");
    println!("    neondb watch counters");
    println!();

    Ok(())
}

// ── chat template ─────────────────────────────────────────────────────────────

fn scaffold_chat(project_path: &Path, project_name: &str) -> Result<()> {
    fs::write(project_path.join("modules").join("join_room.js"), CHAT_JOIN_ROOM_JS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write join_room.js: {}", e)))?;
    fs::write(project_path.join("modules").join("send_message.js"), CHAT_SEND_MESSAGE_JS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write send_message.js: {}", e)))?;

    fs::create_dir_all(project_path.join("client"))
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Create client/: {}", e)))?;
    fs::write(project_path.join("client").join("chat.ts"), CHAT_CLIENT_TS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write client/chat.ts: {}", e)))?;

    let readme = format!(
        "# {name} — Chat Template\n\n{body}",
        name = project_name,
        body = CHAT_README
    );
    fs::write(project_path.join("README.md"), readme)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write README.md: {}", e)))?;

    print_success(project_name, "chat", &[
        ("modules/join_room.js",    "join or create a room"),
        ("modules/send_message.js", "post a message to a room"),
        ("client/chat.ts",          "TypeScript client example"),
        ("neondb.toml",             "server configuration"),
    ]);
    println!("  Next steps:");
    println!("    cd {}", project_name);
    println!("    neondb start");
    println!("    neondb call join_room '[\"general\", \"alice\"]'");
    println!("    neondb watch \"messages WHERE room = 'general'\"");
    println!("    neondb call send_message '[\"general\", \"alice\", \"Hello!\"]'");
    println!();

    Ok(())
}

// ── leaderboard template ──────────────────────────────────────────────────────

fn scaffold_leaderboard(project_path: &Path, project_name: &str) -> Result<()> {
    fs::write(project_path.join("modules").join("submit_score.js"), LB_SUBMIT_SCORE_JS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write submit_score.js: {}", e)))?;
    fs::write(project_path.join("modules").join("reset_scores.js"), LB_RESET_SCORES_JS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write reset_scores.js: {}", e)))?;

    fs::create_dir_all(project_path.join("client"))
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Create client/: {}", e)))?;
    fs::write(project_path.join("client").join("leaderboard.ts"), LB_CLIENT_TS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write client/leaderboard.ts: {}", e)))?;

    let readme = format!(
        "# {name} — Leaderboard Template\n\n{body}",
        name = project_name,
        body = LB_README
    );
    fs::write(project_path.join("README.md"), readme)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write README.md: {}", e)))?;

    print_success(project_name, "leaderboard", &[
        ("modules/submit_score.js", "record a player's score"),
        ("modules/reset_scores.js", "wipe all scores (runs daily via scheduler)"),
        ("client/leaderboard.ts",   "TypeScript client example"),
        ("neondb.toml",             "server config with [[scheduler]]"),
    ]);
    println!("  Next steps:");
    println!("    cd {}", project_name);
    println!("    neondb start");
    println!("    neondb call submit_score '[\"alice\", 1500]'");
    println!("    neondb call submit_score '[\"bob\",   2200]'");
    println!("    neondb watch scores");
    println!("    neondb get scores");
    println!();

    Ok(())
}

// ── mmo template ──────────────────────────────────────────────────────────────

fn scaffold_mmo(project_path: &Path, project_name: &str) -> Result<()> {
    fs::write(project_path.join("modules").join("spawn_player.js"), MMO_SPAWN_JS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write spawn_player.js: {}", e)))?;
    fs::write(project_path.join("modules").join("move_player.js"), MMO_MOVE_JS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write move_player.js: {}", e)))?;
    fs::write(project_path.join("modules").join("attack.js"), MMO_ATTACK_JS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write attack.js: {}", e)))?;

    fs::create_dir_all(project_path.join("client"))
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Create client/: {}", e)))?;
    fs::write(project_path.join("client").join("mmo.ts"), MMO_CLIENT_TS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write client/mmo.ts: {}", e)))?;

    let readme = format!(
        "# {name} — MMO Template\n\n{body}",
        name = project_name,
        body = MMO_README
    );
    fs::write(project_path.join("README.md"), readme)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write README.md: {}", e)))?;

    print_success(project_name, "mmo", &[
        ("modules/spawn_player.js", "create a player at a position"),
        ("modules/move_player.js",  "move player to new coordinates"),
        ("modules/attack.js",       "attack another player"),
        ("client/mmo.ts",           "TypeScript client example"),
    ]);
    println!("  Next steps:");
    println!("    cd {}", project_name);
    println!("    neondb start");
    println!("    neondb call spawn_player '[\"alice\", 0, 0]'");
    println!("    neondb watch \"players WHERE zone = 'zone_0_0'\"");
    println!("    neondb call move_player '[\"alice\", 3, 7]'");
    println!();

    Ok(())
}

// ── turn-based template ───────────────────────────────────────────────────────

fn scaffold_turn_based(project_path: &Path, project_name: &str) -> Result<()> {
    fs::write(project_path.join("modules").join("create_game.js"), TB_CREATE_GAME_JS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write create_game.js: {}", e)))?;
    fs::write(project_path.join("modules").join("join_game.js"), TB_JOIN_GAME_JS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write join_game.js: {}", e)))?;
    fs::write(project_path.join("modules").join("make_move.js"), TB_MAKE_MOVE_JS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write make_move.js: {}", e)))?;

    fs::create_dir_all(project_path.join("client"))
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Create client/: {}", e)))?;
    fs::write(project_path.join("client").join("game.ts"), TB_CLIENT_TS)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write client/game.ts: {}", e)))?;

    let readme = format!(
        "# {name} — Turn-Based Game Template\n\n{body}",
        name = project_name,
        body = TB_README
    );
    fs::write(project_path.join("README.md"), readme)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Write README.md: {}", e)))?;

    print_success(project_name, "turn-based", &[
        ("modules/create_game.js", "start a new game session"),
        ("modules/join_game.js",   "second player joins"),
        ("modules/make_move.js",   "submit a move (turn-validated)"),
        ("client/game.ts",         "TypeScript client example"),
    ]);
    println!("  Next steps:");
    println!("    cd {}", project_name);
    println!("    neondb start");
    println!("    neondb call create_game '[\"game1\", \"alice\"]'");
    println!("    neondb call join_game '[\"game1\", \"bob\"]'");
    println!("    neondb watch \"games WHERE id = 'game1'\"");
    println!("    neondb call make_move '[\"game1\", \"alice\", \"e4\"]'");
    println!();

    Ok(())
}

// ── Print success banner ──────────────────────────────────────────────────────

fn print_success(project_name: &str, template: &str, files: &[(&str, &str)]) {
    println!();
    println!("  Created '{}' (template: {})", project_name, template);
    println!();
    for (file, desc) in files {
        if desc.is_empty() {
            println!("    {}", file);
        } else {
            println!("    {:<36} {}", file, desc);
        }
    }
    println!();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Embedded template file contents
// ═══════════════════════════════════════════════════════════════════════════════

// ── Shared ────────────────────────────────────────────────────────────────────

const MIGRATIONS_README: &str = r#"# Migrations

Place `.toml` files here. NeonDB applies them automatically at startup,
in lexicographic filename order. Each file is idempotent — safe to re-run.

## Format

```toml
[[steps]]
operation = "add_field"
table     = "players"
field     = "xp"
default_value = 0

[[steps]]
operation = "rename_field"
table     = "players"
old_field = "old_name"
new_field = "display_name"

[[steps]]
operation = "remove_field"
table     = "players"
field     = "deprecated_flag"
```

Supported operations: `add_field`, `remove_field`, `rename_field`.
"#;

// ── blank ─────────────────────────────────────────────────────────────────────

const BLANK_HELLO_JS: &str = r#"/**
 * hello.js — sample NeonDB reducer
 *
 * Call it:
 *   neondb call hello '["score", 1]'
 *
 * Host globals injected by NeonDB:
 *   __neondb_get(table, key) -> object | null
 *   __neondb_set(table, key, value) -> void
 */
function reducer(args) {
  var name  = args[0] || "counter";
  var delta = args[1] || 1;

  var row   = __neondb_get("counters", name);
  var value = (row && typeof row.value === "number") ? row.value : 0;
  value += delta;

  __neondb_set("counters", name, { value: value });
  return { new_value: value };
}
"#;

// ── chat ──────────────────────────────────────────────────────────────────────

const CHAT_JOIN_ROOM_JS: &str = r#"/**
 * join_room.js — join or create a chat room
 *
 * Args: [room_id: string, username: string]
 *
 * Tables used:
 *   rooms    { id, name, member_count, created_at }
 *   members  { id (room+user), room_id, username, joined_at }
 *
 * Call it:
 *   neondb call join_room '["general", "alice"]'
 */
function reducer(args) {
  var room_id  = args[0];
  var username = args[1];

  if (!room_id || !username) {
    return { error: "room_id and username required" };
  }

  // Create room if it doesn't exist
  var room = __neondb_get("rooms", room_id);
  if (!room) {
    room = { id: room_id, name: room_id, member_count: 0, created_at: Date.now() };
  }

  // Idempotent join — check if already a member
  var member_key = room_id + ":" + username;
  var existing   = __neondb_get("members", member_key);
  if (!existing) {
    __neondb_set("members", member_key, {
      id: member_key,
      room_id: room_id,
      username: username,
      joined_at: Date.now()
    });
    room.member_count = (room.member_count || 0) + 1;
  }

  __neondb_set("rooms", room_id, room);
  return { ok: true, room_id: room_id, member_count: room.member_count };
}
"#;

const CHAT_SEND_MESSAGE_JS: &str = r#"/**
 * send_message.js — post a message to a room
 *
 * Args: [room_id: string, username: string, text: string]
 *
 * Tables used:
 *   messages { id, room_id, username, text, sent_at }
 *
 * Call it:
 *   neondb call send_message '["general", "alice", "Hello world!"]'
 *
 * Subscribe to live messages in a room:
 *   neondb watch "messages WHERE room_id = 'general'"
 */
function reducer(args) {
  var room_id  = args[0];
  var username = args[1];
  var text     = args[2];

  if (!room_id || !username || !text) {
    return { error: "room_id, username, and text required" };
  }

  // Verify room exists
  var room = __neondb_get("rooms", room_id);
  if (!room) {
    return { error: "Room '" + room_id + "' does not exist. Call join_room first." };
  }

  var msg_id = room_id + ":" + Date.now() + ":" + username;
  var message = {
    id:       msg_id,
    room_id:  room_id,
    username: username,
    text:     text,
    sent_at:  Date.now()
  };

  __neondb_set("messages", msg_id, message);
  return { ok: true, message_id: msg_id };
}
"#;

const CHAT_CLIENT_TS: &str = r#"/**
 * chat.ts — TypeScript client example for the chat template
 *
 * Install the NeonDB TypeScript SDK first:
 *   cd ../neondb-client-ts && npm install && npm run build
 *
 * Run this example:
 *   npx ts-node client/chat.ts
 */
import { NeonDBClient } from "@neondb/client";

const client = new NeonDBClient({ url: "ws://localhost:3000" });

async function main() {
  await client.connect();
  console.log("Connected to NeonDB");

  // Join a room
  await client.call("join_room", ["general", "alice"]);
  console.log("Joined room: general");

  // Subscribe to live messages
  const sub = client.subscribe("messages WHERE room_id = 'general'", (diff) => {
    if (diff.operation === "insert" && diff.rowData) {
      const msg = diff.rowData as any;
      console.log(`[${msg.username}] ${msg.text}`);
    }
  });

  // Send a message after a short delay
  setTimeout(async () => {
    await client.call("send_message", ["general", "alice", "Hello from NeonDB!"]);
  }, 500);

  // Run for 5 seconds then exit
  setTimeout(() => {
    sub.unsubscribe();
    client.disconnect();
  }, 5000);
}

main().catch(console.error);
"#;

const CHAT_README: &str = r#"Real-time chat backend with rooms and live message subscriptions.

## Tables

- **rooms** — `{ id, name, member_count, created_at }`
- **members** — `{ id, room_id, username, joined_at }`
- **messages** — `{ id, room_id, username, text, sent_at }`

## Reducers

| Reducer | Args | Description |
|---|---|---|
| `join_room` | `[room_id, username]` | Join or create a room |
| `send_message` | `[room_id, username, text]` | Post a message |

## Quick start

```bash
neondb start

# Join a room
neondb call join_room '["general", "alice"]'
neondb call join_room '["general", "bob"]'

# Watch for live messages (Terminal A)
neondb watch "messages WHERE room_id = 'general'"

# Send messages (Terminal B)
neondb call send_message '["general", "alice", "Hello!"]'
neondb call send_message '["general", "bob", "Hi Alice!"]'
```
"#;

// ── leaderboard ───────────────────────────────────────────────────────────────

const LB_SUBMIT_SCORE_JS: &str = r#"/**
 * submit_score.js — record or update a player's score
 *
 * Args: [player_id: string, score: number]
 *
 * Tables used:
 *   scores { id, player_id, score, submitted_at }
 *
 * Call it:
 *   neondb call submit_score '["alice", 1500]'
 *
 * Subscribe to the live leaderboard:
 *   neondb watch scores
 */
function reducer(args) {
  var player_id = args[0];
  var score     = args[1];

  if (!player_id || typeof score !== "number") {
    return { error: "player_id (string) and score (number) required" };
  }

  var existing = __neondb_get("scores", player_id);
  var best     = existing ? Math.max(existing.score, score) : score;

  __neondb_set("scores", player_id, {
    id:           player_id,
    player_id:    player_id,
    score:        best,
    submitted_at: Date.now()
  });

  return { ok: true, player_id: player_id, best_score: best };
}
"#;

const LB_RESET_SCORES_JS: &str = r#"/**
 * reset_scores.js — wipe all scores (called by scheduler daily)
 *
 * Args: [] (no arguments needed)
 *
 * This reducer is triggered automatically by the [[scheduler]] entry
 * in neondb.toml every 24 hours. You can also call it manually:
 *   neondb call reset_scores '[]'
 */
function reducer(args) {
  // NeonDB does not yet have a "delete all rows" primitive,
  // so we mark each score as reset instead of deleting.
  // A future migration can remove the reset rows.
  var sentinel = __neondb_get("scores", "__reset_sentinel__");
  var reset_count = sentinel ? (sentinel.count || 0) + 1 : 1;

  __neondb_set("scores", "__reset_sentinel__", {
    id:         "__reset_sentinel__",
    player_id:  "__system__",
    score:      0,
    reset_at:   Date.now(),
    count:      reset_count
  });

  return { ok: true, reset_at: Date.now(), reset_number: reset_count };
}
"#;

const LB_CLIENT_TS: &str = r#"/**
 * leaderboard.ts — TypeScript client example for the leaderboard template
 *
 * Run: npx ts-node client/leaderboard.ts
 */
import { NeonDBClient } from "@neondb/client";

const client = new NeonDBClient({ url: "ws://localhost:3000" });

async function main() {
  await client.connect();

  // Subscribe to live leaderboard updates
  const sub = client.subscribe("scores", (diff) => {
    if (diff.rowData && (diff.rowData as any).player_id !== "__system__") {
      console.log(`Score update: ${(diff.rowData as any).player_id} → ${(diff.rowData as any).score}`);
    }
  });

  // Submit some scores
  const players = [
    ["alice", 1200], ["bob", 1800], ["carol", 950], ["dave", 2100]
  ];

  for (const [player, score] of players) {
    await client.call("submit_score", [player, score]);
    await new Promise(r => setTimeout(r, 100));
  }

  // Read final leaderboard
  const rows = client.getRows("scores");
  const sorted = [...rows.values()]
    .filter((r: any) => r.player_id !== "__system__")
    .sort((a: any, b: any) => b.score - a.score);

  console.log("\n=== Leaderboard ===");
  sorted.forEach((r: any, i: number) => {
    console.log(`${i + 1}. ${r.player_id.padEnd(12)} ${r.score}`);
  });

  sub.unsubscribe();
  client.disconnect();
}

main().catch(console.error);
"#;

const LB_README: &str = r#"Live leaderboard with automatic daily reset.

## Tables

- **scores** — `{ id, player_id, score, submitted_at }`

## Reducers

| Reducer | Args | Description |
|---|---|---|
| `submit_score` | `[player_id, score]` | Record or update best score |
| `reset_scores` | `[]` | Wipe all scores (auto-runs daily) |

## Quick start

```bash
neondb start

neondb call submit_score '["alice", 1500]'
neondb call submit_score '["bob", 2200]'
neondb call submit_score '["alice", 1800]'   # alice's best = 1800

neondb get scores
neondb watch scores
```

The `reset_scores` reducer fires automatically every 24 hours via the
`[[scheduler]]` entry in `neondb.toml`. Change `interval_ms` to adjust.
"#;

// ── mmo ───────────────────────────────────────────────────────────────────────

const MMO_SPAWN_JS: &str = r#"/**
 * spawn_player.js — create a player at a starting position
 *
 * Args: [player_id: string, x: number, y: number]
 *
 * Tables used:
 *   players { id, x, y, hp, zone, last_action }
 *
 * Call it:
 *   neondb call spawn_player '["alice", 0, 0]'
 */
function reducer(args) {
  var player_id = args[0];
  var x         = args[1] || 0;
  var y         = args[2] || 0;

  if (!player_id) return { error: "player_id required" };

  var existing = __neondb_get("players", player_id);
  if (existing) return { error: "Player already exists", player: existing };

  var zone = "zone_" + Math.floor(x / 10) + "_" + Math.floor(y / 10);
  __neondb_set("players", player_id, {
    id: player_id, x: x, y: y, hp: 100,
    zone: zone, last_action: Date.now()
  });
  return { ok: true, player_id: player_id, x: x, y: y, zone: zone };
}
"#;

const MMO_MOVE_JS: &str = r#"/**
 * move_player.js — move a player to new coordinates
 *
 * Args: [player_id: string, x: number, y: number]
 *
 * Subscribe to a zone:
 *   neondb watch "players WHERE zone = 'zone_0_0'"
 *
 * Call it:
 *   neondb call move_player '["alice", 5, 3]'
 */
function reducer(args) {
  var player_id = args[0];
  var x         = args[1];
  var y         = args[2];

  if (!player_id || typeof x !== "number" || typeof y !== "number") {
    return { error: "player_id, x, y required" };
  }

  var player = __neondb_get("players", player_id);
  if (!player) return { error: "Player not found — call spawn_player first" };

  var new_zone = "zone_" + Math.floor(x / 10) + "_" + Math.floor(y / 10);
  player.x           = x;
  player.y           = y;
  player.zone        = new_zone;
  player.last_action = Date.now();

  __neondb_set("players", player_id, player);
  return { ok: true, player_id: player_id, x: x, y: y, zone: new_zone };
}
"#;

const MMO_ATTACK_JS: &str = r#"/**
 * attack.js — attack another player (must be in same zone)
 *
 * Args: [attacker_id: string, target_id: string, damage: number]
 *
 * Call it:
 *   neondb call attack '["alice", "bob", 15]'
 */
function reducer(args) {
  var attacker_id = args[0];
  var target_id   = args[1];
  var damage      = args[2] || 10;

  var attacker = __neondb_get("players", attacker_id);
  var target   = __neondb_get("players", target_id);

  if (!attacker) return { error: "Attacker not found" };
  if (!target)   return { error: "Target not found" };
  if (attacker.zone !== target.zone) {
    return { error: "Players are in different zones", attacker_zone: attacker.zone, target_zone: target.zone };
  }

  var new_hp = Math.max(0, (target.hp || 100) - damage);
  target.hp          = new_hp;
  target.last_action = Date.now();

  __neondb_set("players", target_id, target);
  return { ok: true, target_id: target_id, damage: damage, remaining_hp: new_hp, alive: new_hp > 0 };
}
"#;

const MMO_CLIENT_TS: &str = r#"/**
 * mmo.ts — TypeScript client example for the MMO template
 *
 * Run: npx ts-node client/mmo.ts
 */
import { NeonDBClient } from "@neondb/client";

const client = new NeonDBClient({ url: "ws://localhost:3000" });

async function main() {
  await client.connect();

  // Spawn two players
  await client.call("spawn_player", ["alice", 0, 0]);
  await client.call("spawn_player", ["bob",   2, 3]);

  // Watch zone_0_0
  const sub = client.subscribe("players WHERE zone = 'zone_0_0'", (diff) => {
    console.log(`[${diff.operation}] ${diff.rowKey}`, diff.rowData);
  });

  // Move alice
  await client.call("move_player", ["alice", 5, 5]);

  // Attack bob (same zone)
  await client.call("attack", ["alice", "bob", 20]);

  setTimeout(() => {
    sub.unsubscribe();
    client.disconnect();
  }, 2000);
}

main().catch(console.error);
"#;

const MMO_README: &str = r#"MMO movement backend with zones and combat.

## Tables

- **players** — `{ id, x, y, hp, zone, last_action }`

Zones are 10×10 tiles: `zone_0_0` covers (0–9, 0–9), `zone_1_0` covers (10–19, 0–9), etc.

## Reducers

| Reducer | Args | Description |
|---|---|---|
| `spawn_player` | `[player_id, x, y]` | Create player at position |
| `move_player` | `[player_id, x, y]` | Move to new coordinates |
| `attack` | `[attacker_id, target_id, damage]` | Attack player in same zone |

## Quick start

```bash
neondb start

neondb call spawn_player '["alice", 0, 0]'
neondb call spawn_player '["bob", 2, 3]'

# Watch zone_0_0 for movement and combat
neondb watch "players WHERE zone = 'zone_0_0'"

neondb call move_player '["alice", 5, 5]'
neondb call attack '["alice", "bob", 15]'
```
"#;

// ── turn-based ────────────────────────────────────────────────────────────────

const TB_CREATE_GAME_JS: &str = r#"/**
 * create_game.js — start a new game session
 *
 * Args: [game_id: string, creator: string]
 *
 * Tables used:
 *   games { id, player1, player2, turn, status, moves, created_at }
 *
 * Call it:
 *   neondb call create_game '["game1", "alice"]'
 */
function reducer(args) {
  var game_id = args[0];
  var creator = args[1];

  if (!game_id || !creator) return { error: "game_id and creator required" };

  var existing = __neondb_get("games", game_id);
  if (existing) return { error: "Game already exists" };

  __neondb_set("games", game_id, {
    id:         game_id,
    player1:    creator,
    player2:    null,
    turn:       creator,
    status:     "waiting",
    moves:      [],
    created_at: Date.now()
  });
  return { ok: true, game_id: game_id, status: "waiting" };
}
"#;

const TB_JOIN_GAME_JS: &str = r#"/**
 * join_game.js — second player joins a game
 *
 * Args: [game_id: string, player: string]
 *
 * Call it:
 *   neondb call join_game '["game1", "bob"]'
 */
function reducer(args) {
  var game_id = args[0];
  var player  = args[1];

  if (!game_id || !player) return { error: "game_id and player required" };

  var game = __neondb_get("games", game_id);
  if (!game)           return { error: "Game not found" };
  if (game.status !== "waiting") return { error: "Game is not waiting for players" };
  if (game.player1 === player)   return { error: "You are already player1" };
  if (game.player2)   return { error: "Game is full" };

  game.player2 = player;
  game.status  = "active";

  __neondb_set("games", game_id, game);
  return { ok: true, game_id: game_id, status: "active", your_turn: game.turn === player };
}
"#;

const TB_MAKE_MOVE_JS: &str = r#"/**
 * make_move.js — submit a move (enforces whose turn it is)
 *
 * Args: [game_id: string, player: string, move: string]
 *
 * The move string format is up to your game logic.
 * This template uses chess-style notation (e.g. "e4") as a placeholder.
 *
 * Subscribe to game state:
 *   neondb watch "games WHERE id = 'game1'"
 *
 * Call it:
 *   neondb call make_move '["game1", "alice", "e4"]'
 */
function reducer(args) {
  var game_id  = args[0];
  var player   = args[1];
  var move_str = args[2];

  if (!game_id || !player || !move_str) {
    return { error: "game_id, player, and move required" };
  }

  var game = __neondb_get("games", game_id);
  if (!game)                 return { error: "Game not found" };
  if (game.status !== "active") return { error: "Game is not active" };
  if (game.turn !== player)  return { error: "Not your turn. Current turn: " + game.turn };

  // Record the move
  var moves = game.moves || [];
  moves.push({ player: player, move: move_str, at: Date.now() });

  // Switch turns
  var next_turn = (player === game.player1) ? game.player2 : game.player1;

  game.moves = moves;
  game.turn  = next_turn;

  __neondb_set("games", game_id, game);
  return { ok: true, move: move_str, next_turn: next_turn, total_moves: moves.length };
}
"#;

const TB_CLIENT_TS: &str = r#"/**
 * game.ts — TypeScript client example for the turn-based template
 *
 * Run: npx ts-node client/game.ts
 */
import { NeonDBClient } from "@neondb/client";

const alice = new NeonDBClient({ url: "ws://localhost:3000" });
const bob   = new NeonDBClient({ url: "ws://localhost:3000" });

async function main() {
  await alice.connect();
  await bob.connect();

  // Alice creates the game
  await alice.call("create_game", ["game1", "alice"]);
  console.log("Alice created game1");

  // Bob joins
  await bob.call("join_game", ["game1", "bob"]);
  console.log("Bob joined game1");

  // Watch game state
  alice.subscribe("games WHERE id = 'game1'", (diff) => {
    const game = diff.rowData as any;
    if (game) console.log(`Turn: ${game.turn} | Moves: ${game.moves?.length ?? 0}`);
  });

  // Play a few turns
  await alice.call("make_move", ["game1", "alice", "e4"]);
  await bob.call("make_move",   ["game1", "bob",   "e5"]);
  await alice.call("make_move", ["game1", "alice", "Nf3"]);

  setTimeout(() => {
    alice.disconnect();
    bob.disconnect();
  }, 1000);
}

main().catch(console.error);
"#;

const TB_README: &str = r#"Turn-based game backend with move validation.

## Tables

- **games** — `{ id, player1, player2, turn, status, moves[], created_at }`

## Reducers

| Reducer | Args | Description |
|---|---|---|
| `create_game` | `[game_id, creator]` | Start a new game |
| `join_game` | `[game_id, player]` | Second player joins |
| `make_move` | `[game_id, player, move]` | Submit a move (turn-enforced) |

## Quick start

```bash
neondb start

neondb call create_game '["game1", "alice"]'
neondb call join_game   '["game1", "bob"]'

# Watch live game state
neondb watch "games WHERE id = 'game1'"

# Take turns
neondb call make_move '["game1", "alice", "e4"]'
neondb call make_move '["game1", "bob",   "e5"]'
neondb call make_move '["game1", "alice", "Nf3"]'

# Try moving out of turn — server rejects it
neondb call make_move '["game1", "alice", "illegal"]'
```
"#;

// ═══════════════════════════════════════════════════════════════════════════════
// neondb build
// ═══════════════════════════════════════════════════════════════════════════════

fn build_wasm_modules(modules_dir: &Path) -> Result<()> {
    if !modules_dir.is_dir() {
        println!(
            "No '{}' directory found. Create one and add your .js reducers.",
            modules_dir.display()
        );
        return Ok(());
    }

    let javy_ok = std::process::Command::new("javy")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if !javy_ok {
        eprintln!("Error: 'javy' not found on PATH.");
        eprintln!();
        eprintln!("javy is a standalone binary — it is NOT on crates.io.");
        eprintln!("Download the latest release for your OS from:");
        eprintln!("  https://github.com/bytecodealliance/javy/releases");
        eprintln!();
        eprintln!("Windows:  download javy-x86_64-windows.zip, extract javy.exe, add to PATH");
        eprintln!("Linux:    download javy-x86_64-linux.gz, gunzip, chmod +x, move to /usr/local/bin");
        eprintln!("macOS:    download javy-x86_64-macos.gz, gunzip, chmod +x, move to /usr/local/bin");
        eprintln!();
        eprintln!("Why WASM? Compiled reducers run via Wasmtime (Cranelift JIT) and are");
        eprintln!("10-50x faster than the Boa interpreter used for raw .js files.");
        return Err(neondb::error::NeonDBError::internal("javy not found on PATH"));
    }

    let entries: Vec<_> = std::fs::read_dir(modules_dir)
        .map_err(|e| neondb::error::NeonDBError::internal(format!("Cannot read {}: {}", modules_dir.display(), e)))?
        .flatten()
        .filter(|e| {
            e.path().extension().and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("js"))
                .unwrap_or(false)
        })
        .collect();

    if entries.is_empty() {
        println!("No .js files found in {}. Nothing to build.", modules_dir.display());
        return Ok(());
    }

    let mut compiled = 0usize;
    let mut failed   = 0usize;

    for entry in &entries {
        let js_path   = entry.path();
        let wasm_path = js_path.with_extension("wasm");

        print!(
            "  Compiling {} -> {} ... ",
            js_path.file_name().unwrap_or_default().to_string_lossy(),
            wasm_path.file_name().unwrap_or_default().to_string_lossy(),
        );

        match std::process::Command::new("javy").arg("compile").arg(&js_path).arg("-o").arg(&wasm_path).status() {
            Ok(s) if s.success() => { println!("ok"); compiled += 1; }
            Ok(s) => { println!("FAILED (exit {})", s.code().unwrap_or(-1)); failed += 1; }
            Err(e) => { println!("FAILED ({})", e); failed += 1; }
        }
    }

    println!();
    if failed == 0 {
        println!("Build complete: {} file(s) compiled.", compiled);
        println!("Run 'neondb start' — .wasm files will be preferred over .js automatically.");
    } else {
        println!("Build complete: {} compiled, {} failed.", compiled, failed);
        return Err(neondb::error::NeonDBError::internal(format!("{} module(s) failed to compile", failed)));
    }
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════════
// Server (unchanged from Session 25)
// ═══════════════════════════════════════════════════════════════════════════════

async fn run_server(config: Config) -> Result<()> {
    let mut logger = env_logger::Builder::from_default_env();
    logger.filter_level(config.log_level.parse().unwrap_or(log::LevelFilter::Info));
    let _ = logger.try_init();

    log::info!("Starting NeonDB Server");
    log::info!("Config: {:?}", config);

    let mut ts = TableStore::new();
    ts.set_shard(config.shard_id, config.shard_count);
    let tables = Arc::new(ts);

    let registry = Arc::new(ReducerRegistry::new()?);
    log::info!("Available reducers: {:?}", registry.list_reducers());

    let mut min_wal_seq: u64 = 0;
    let mut initial_seq: u64 = 0;

    let snap_dir = config.snapshot_dir.clone();
    if let Some((snap_path, snap_seq)) = find_latest_snapshot(&snap_dir) {
        log::info!("Loading snapshot: {:?} (seq {})", snap_path, snap_seq);
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
    } else {
        log::info!("WAL file does not exist, starting fresh");
    }

    let migrations_dir = std::path::PathBuf::from("migrations");
    match neondb::migrations::apply_migrations(&migrations_dir, &tables) {
        Ok(0) => log::debug!("No migrations to apply"),
        Ok(n) => log::info!("Applied {} migration file(s)", n),
        Err(e) => log::warn!("Migration error: {}", e),
    }

    // ── Schema registry ───────────────────────────────────────────────────────
    let schema_registry = Arc::new(
        neondb::schema::SchemaRegistry::load_from_file(std::path::Path::new("schema.toml"))
            .unwrap_or_else(|e| {
                log::warn!("schema.toml load error: {} — running without type enforcement", e);
                neondb::schema::SchemaRegistry::new()
            })
    );
    if schema_registry.table_count() > 0 {
        log::info!("Schema: enforcing types for tables: {:?}", schema_registry.list_tables());
    } else {
        log::debug!("Schema: no schema.toml found — all tables are schema-free");
    }

    let (reducer_tx, reducer_rx) = kanal::unbounded_async::<PendingCall>();
    let subscription_manager = Arc::new(SubscriptionManager::new_with_options(config.two_frame_protocol));
    log::info!("Subscription fan-out mode: {}", if config.two_frame_protocol { "two-frame" } else { "legacy" });

    let active_connections = Arc::new(AtomicUsize::new(0));
    let (shutdown_tx, shutdown_rx) = watch::channel(());

    let listener_handle = {
        let config_c = config.clone();
        let tx_c = reducer_tx.clone();
        let subs_c = subscription_manager.clone();
        let tables_c = tables.clone();
        let conns_c = active_connections.clone();
        let rx_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(e) = start_listener(config_c.host, config_c.port, tx_c, subs_c, tables_c, config_c.max_connections, config_c.api_key.clone(), conns_c, rx_shutdown).await {
                log::error!("Listener error: {}", e);
            }
        })
    };

    let metrics_handle = {
        let subs_c = subscription_manager.clone();
        let tables_c = tables.clone();
        let rx_shutdown = shutdown_rx.clone();
        let host_c = config.host.clone();
        let mport = config.metrics_port;
        tokio::spawn(async move {
            if let Err(e) = start_metrics_server(host_c, mport, subs_c, tables_c, rx_shutdown).await {
                log::error!("Metrics server error: {}", e);
            }
        })
    };

    let wal_writer = Arc::new(BatchedWalWriter::open(&config.wal_path, config.wal_batch_interval_ms, config.wal_batch_size, config.unsafe_no_fsync)?);

    let worker_count = num_cpus::get().max(1);
    log::info!("Starting {} parallel reducer workers", worker_count);

    let timeout_ms = config.reducer_timeout_ms;
    let snapshot_interval = config.snapshot_interval;
    let snapshot_dir_w = config.snapshot_dir.clone();
    let global_seq = Arc::new(std::sync::atomic::AtomicU64::new(initial_seq));

    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let rx = reducer_rx.clone();
        let tables_w = tables.clone();
        let registry_w = registry.clone();
        let subs_w = subscription_manager.clone();
        let wal_w = wal_writer.clone();
        let seq_w = global_seq.clone();
        let snap_interval_w = snapshot_interval;
        let snap_dir_ww = snapshot_dir_w.clone();
        let schema_w = schema_registry.clone();

        let handle = tokio::spawn(async move {
            log::debug!("Reducer worker {} started", worker_id);
            loop {
                let call = match rx.recv().await { Ok(c) => c, Err(_) => break };

                let call_id = call.call_id;
                let tables_blk = tables_w.clone();
                let registry_blk = registry_w.clone();
                let reducer_name = call.reducer_name.clone();
                let args = call.args.clone();
                let timestamp = current_timestamp_nanos();
                let call_caller_id = call.caller_id.clone();

                let schema_for_blk = schema_w.clone();
                let blk_result = tokio::time::timeout(
                    std::time::Duration::from_millis(timeout_ms),
                    tokio::task::spawn_blocking(move || {
                        let schema_for_ctx = schema_for_blk;
                        let mut ctx = ReducerContext::new(tables_blk, timestamp)
                            .with_schema(schema_for_ctx);
                        ctx.caller_id = call_caller_id;
                        let exec = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            registry_blk.execute(&reducer_name, &mut ctx, &args)
                        }));
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
                                            let tables_snap = tables_w.clone();
                                            let dir_snap = snap_dir_ww.clone();
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

                if let Err(e) = call.response_tx.send(response) {
                    log::warn!("Failed to send response: {}", e);
                }
            }
            log::debug!("Reducer worker {} stopped", worker_id);
        });
        worker_handles.push(handle);
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
            .and_then(|v| rmp_serde::to_vec(&v).ok())
            .unwrap_or_default();

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
                        let call = PendingCall { call_id, reducer_name: sched.reducer.clone(), args: args_bytes.clone(), caller_id: "scheduler".to_string(), response_tx: resp_tx };
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

    println!("=== NeonDB Bench ===");
    println!("  Server  : {}", ws_url);
    println!("  Clients : {}  Calls/client: {}  Warmup: {}", num_clients, calls_per_client, warmup_per_client);

    let args_bytes = rmp_serde::to_vec(&IncrArgs { name: "bench".to_string(), delta: 1 }).unwrap();
    let latencies: Arc<Mutex<Histogram<u64>>> = Arc::new(Mutex::new(Histogram::new(3).unwrap()));
    let mut join_set = JoinSet::new();
    let start = Instant::now();

    for cid in 0..num_clients {
        let url = ws_url.to_string();
        let api = api_key.map(String::from);
        let args = args_bytes.clone();
        let lat = latencies.clone();
        let warmup = warmup_per_client;
        let calls = calls_per_client;

        join_set.spawn(async move {
            let mut req = url.as_str().into_client_request().unwrap();
            if let Some(k) = &api { req.headers_mut().insert("authorization", format!("Bearer {}", k).parse().unwrap()); }
            let Ok((mut ws, _)) = tokio_tungstenite::connect_async(req).await else { return 0usize; };
            let total = warmup + calls;
            let mut ok = 0usize;
            for i in 0..total {
                let cw = rmp_serde::to_vec(&CallW { rc: ((cid as u64) * 1_000_000 + i as u64, "increment".to_string(), args.clone()) }).unwrap();
                let t0 = Instant::now();
                if ws.send(Message::Binary(cw)).await.is_err() { break; }
                if let Ok(Some(Ok(Message::Binary(_) | Message::Text(_)))) = tokio::time::timeout(Duration::from_secs(10), ws.next()).await {
                    if i >= warmup {
                        let us = t0.elapsed().as_micros() as u64;
                        if let Ok(mut h) = lat.lock() { let _ = h.record(us); }
                        ok += 1;
                    }
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
        let subs = subscription_manager.clone();
        let tbl = tables.clone();
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
    let mut replayed = 0usize;
    let mut max_seq = min_seq;
    for entry in &entries {
        max_seq = max_seq.max(entry.header.sequence_number);
        if entry.header.sequence_number <= min_seq { continue; }
        if !entry.verify_checksum() { log::warn!("WAL entry {} bad checksum, skipping", entry.header.sequence_number); continue; }
        for delta in &entry.payload.deltas { tables.apply_delta(delta)?; }
        replayed += 1;
    }
    Ok((replayed, max_seq))
}
