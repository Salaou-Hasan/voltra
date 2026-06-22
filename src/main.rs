// ============================================================================
// Voltra main.rs — binary entry point
//
// This file holds only the global allocator, the clap `Cli`/`Commands`
// definitions, and `main()` dispatch. Every command implementation lives in
// the `app` module tree (src/app/), declared below.
//
// Fixes (history):
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

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use voltra::{config::Config, error::Result};

mod app;

use app::bench::run_cli_bench;
use app::bootstrap::run_server;
use app::build::{build_voltra_reducers, build_wasm_modules};
use app::cli::{
    cmd_backup, cmd_cluster_status, cmd_drain, cmd_generate, cmd_list_backups, cmd_list_modules,
    cmd_list_templates, cmd_promote, cmd_start_project, is_game_project, print_banner,
};
use app::scaffold::{cmd_add_module, init_project};

// ─────────────────────────────────────────────────────────────────────────────
// CLI definition
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "voltra")]
#[command(author, version = concat!("v", env!("CARGO_PKG_VERSION"), " · Gen 1 (Genesis)"), about = "Voltra — self-hosted real-time game backend")]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Scaffold a new Voltra multiplayer game project
    Init {
        #[arg(value_name = "NAME")]
        path: Option<PathBuf>,
        #[arg(
            long,
            help = "Template: game/basic | game/full | game/unity | game/godot"
        )]
        template: Option<String>,
    },
    /// Add a feature module to an existing project (run inside project dir)
    Add {
        #[arg(
            value_name = "MODULE",
            help = "chat | inventory | leaderboard | matchmaking | guilds | quests | economy | combat | world"
        )]
        module: String,
    },
    /// Check for and install updates to all Voltra binaries
    Update {
        #[arg(long, help = "Only check — do not download")]
        check: bool,
    },
    /// Install this binary to a stable location and add it to your PATH
    Install,
    /// List available project templates
    Templates,
    /// List available add-on modules (`voltra add <module>`)
    Modules,
    /// Compile JS reducers in modules/ to WASM (requires `javy`)
    Build {
        #[arg(short = 'm', long, default_value = "modules")]
        modules_dir: Option<PathBuf>,
    },
    /// Start the Voltra server
    Start {
        #[arg(short = 'a', long)]
        host: Option<String>,
        #[arg(short = 'p', long)]
        port: Option<u16>,
        #[arg(short = 'd', long)]
        data_dir: Option<PathBuf>,
        #[arg(long = "wal-path")]
        wal_path: Option<PathBuf>,
        #[arg(short = 'f', long)]
        fsync_interval_ms: Option<u32>,
    },
    /// Show server status and metrics
    Status {
        #[arg(long, default_value = "http://127.0.0.1:3001")]
        metrics_url: String,
    },
    /// List all tables and their row counts
    Tables {
        #[arg(long, default_value = "http://127.0.0.1:3001")]
        metrics_url: String,
    },
    /// Read rows from a table
    Get {
        table: String,
        key: Option<String>,
        #[arg(long, default_value = "http://127.0.0.1:3001")]
        metrics_url: String,
    },
    /// Call a reducer once and print the result
    Call {
        reducer: String,
        #[arg(help = "JSON args array, e.g. '[\"alice\", 5]'")]
        args: Option<String>,
        #[arg(long, default_value = "ws://127.0.0.1:3000")]
        url: String,
        #[arg(long)]
        api_key: Option<String>,
    },
    /// Subscribe to a table and stream live updates (Ctrl-C to stop)
    Watch {
        query: String,
        #[arg(long, default_value = "ws://127.0.0.1:3000")]
        url: String,
        #[arg(long)]
        api_key: Option<String>,
    },
    /// Show status of all cluster peers
    ClusterStatus {
        #[arg(long, default_value = "http://127.0.0.1:3001")]
        metrics_url: String,
    },
    /// Bulk-seed rows into a running server from a JSON file
    Seed {
        #[arg(value_name = "FILE", help = "Path to seed JSON file")]
        file: String,
        #[arg(
            long,
            default_value = "http://127.0.0.1:3001",
            help = "Admin/metrics server URL"
        )]
        metrics_url: String,
        #[arg(long, help = "Parse and preview what would be seeded without writing")]
        dry_run: bool,
    },
    /// Put the server into drain mode — stop accepting new connections while
    /// existing connections finish. Safe to hot-fix then undrain or restart.
    Drain {
        #[arg(
            long,
            default_value = "http://127.0.0.1:3001",
            help = "Admin/metrics server URL"
        )]
        metrics_url: String,
    },
    /// Take the server out of drain mode — resume accepting new connections.
    Undrain {
        #[arg(
            long,
            default_value = "http://127.0.0.1:3001",
            help = "Admin/metrics server URL"
        )]
        metrics_url: String,
    },
    /// Apply pending schema migrations from the migrations/ directory
    Migrate {
        #[arg(
            value_name = "DIR",
            default_value = "migrations",
            help = "Path to migrations directory"
        )]
        dir: String,
        #[arg(
            long,
            default_value = "http://127.0.0.1:3001",
            help = "Admin/metrics server URL"
        )]
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
        #[arg(long, default_value = "ws://127.0.0.1:3000")]
        url: String,
        #[arg(long)]
        api_key: Option<String>,
    },
    /// Run a WebSocket throughput benchmark against a running server
    Bench {
        #[arg(long, default_value = "ws://127.0.0.1:3000")]
        url: String,
        #[arg(short = 'c', long, default_value = "10")]
        clients: usize,
        #[arg(short = 'n', long, default_value = "500")]
        calls: usize,
        #[arg(long, default_value = "50")]
        warmup: usize,
        #[arg(long)]
        api_key: Option<String>,
    },
    /// Trigger an immediate backup on a running server
    Backup {
        #[arg(long, default_value = "http://127.0.0.1:3001")]
        metrics_url: String,
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
        #[arg(
            long = "snapshot-dir",
            help = "Live snapshot directory to restore into"
        )]
        snapshot_dir: PathBuf,
        #[arg(
            long = "until-ts",
            help = "Point-in-time cutoff (unix NANOSECONDS); WAL entries after this are dropped"
        )]
        until_ts: Option<u64>,
    },
    /// Promote a replica to primary (failover)
    Promote {
        #[arg(long, default_value = "http://127.0.0.1:3001")]
        metrics_url: String,
    },
    /// Generate typed client code from the running server's schema
    ///
    /// Examples:
    ///   voltra generate --lang typescript --out ./client/src/generated
    ///   voltra generate --lang gdscript  --out ./godot/addons/voltra/generated
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
    let command = match cli.command {
        Some(cmd) => cmd,
        None => {
            println!(
                "Voltra v{} · Gen {} ({}) — self-hosted real-time game backend",
                env!("CARGO_PKG_VERSION"),
                voltra::GENERATION,
                voltra::GENERATION_CODENAME
            );
            println!();
            println!("  Engine is ready.");
            println!();
            println!("  Get started:");
            println!("    voltra init       scaffold a new game project");
            println!("    voltra start      start the server");
            println!("    voltra --help     show all commands");
            println!();
            return Ok(());
        }
    };
    match command {
        Commands::Init { path, template } => {
            init_project(path, template)?;
            Ok(())
        }
        Commands::Add { module } => {
            cmd_add_module(&module, &std::env::current_dir()?)?;
            Ok(())
        }
        Commands::Update { check } => voltra::updater::cmd_update(check),
        Commands::Install => voltra::updater::cmd_install(),
        Commands::Templates => {
            cmd_list_templates();
            Ok(())
        }
        Commands::Modules => {
            cmd_list_modules();
            Ok(())
        }
        Commands::Build { modules_dir } => {
            let cwd = std::env::current_dir()?;
            // Voltra project: reducers/ directory OR reducers.vol → compile to native Rust
            if cwd.join("reducers").is_dir() || cwd.join("reducers.vol").exists() {
                return build_voltra_reducers(&cwd);
            }
            // Rust/WASM project: compile .js/.wat files in modules/
            build_wasm_modules(modules_dir.as_deref().unwrap_or(Path::new("modules")))
        }
        Commands::Start {
            host,
            port,
            data_dir,
            wal_path,
            fsync_interval_ms,
        } => {
            print_banner();
            // If run from inside a scaffolded game project, build + exec that binary
            let cwd = std::env::current_dir()?;
            if let Some(pkg_name) = is_game_project(&cwd) {
                return cmd_start_project(&cwd, &pkg_name);
            }
            // Non-blocking background version hint — prints one line if behind
            std::thread::spawn(voltra::updater::check_and_hint);
            let mut config = Config::from_env();
            if let Some(h) = host {
                config.host = h;
            }
            if let Some(p) = port {
                config.port = p;
            }
            if let Some(d) = data_dir {
                config.wal_path = d.join("voltra.wal");
            }
            if let Some(w) = wal_path {
                config.wal_path = w;
            }
            if let Some(f) = fsync_interval_ms {
                config.fsync_interval_ms = f;
            }
            run_server(config).await
        }
        Commands::Status { metrics_url } => voltra::cli::cmd_status(&metrics_url).await,
        Commands::Tables { metrics_url } => voltra::cli::cmd_tables(&metrics_url).await,
        Commands::Get {
            table,
            key,
            metrics_url,
        } => voltra::cli::cmd_get(&metrics_url, &table, key.as_deref()).await,
        Commands::Call {
            reducer,
            args,
            url,
            api_key,
        } => voltra::cli::cmd_call(&url, &reducer, args.as_deref(), api_key.as_deref()).await,
        Commands::Watch {
            query,
            url,
            api_key,
        } => voltra::cli::cmd_watch(&url, &query, api_key.as_deref()).await,
        Commands::ClusterStatus { metrics_url } => cmd_cluster_status(&metrics_url).await,
        Commands::Seed {
            file,
            metrics_url,
            dry_run,
        } => voltra::cli::cmd_seed(&metrics_url, &file, dry_run).await,
        Commands::Drain { metrics_url } => cmd_drain(&metrics_url, true).await,
        Commands::Undrain { metrics_url } => cmd_drain(&metrics_url, false).await,
        Commands::Migrate {
            dir,
            metrics_url,
            dry_run,
        } => voltra::cli::cmd_migrate(&metrics_url, &dir, dry_run).await,
        Commands::GenerateNpc {
            npc_type,
            context,
            url,
            api_key,
        } => {
            voltra::cli::cmd_generate_npc(&url, &npc_type, context.as_deref(), api_key.as_deref())
                .await
        }
        Commands::Bench {
            url,
            clients,
            calls,
            warmup,
            api_key,
        } => run_cli_bench(&url, clients, calls, warmup, api_key.as_deref()).await,
        Commands::Backup { metrics_url } => cmd_backup(&metrics_url).await,
        Commands::Backups { dir } => {
            cmd_list_backups(&dir);
            Ok(())
        }
        Commands::Restore {
            backup,
            wal_path,
            snapshot_dir,
            until_ts,
        } => {
            let (seq, n) =
                voltra::backup::restore_to_dirs(&backup, &wal_path, &snapshot_dir, until_ts)?;
            println!("Restored snapshot seq={} plus {} WAL entries.", seq, n);
            println!(
                "Start the server with --wal-path {:?} to load the restored data.",
                wal_path
            );
            Ok(())
        }
        Commands::Promote { metrics_url } => cmd_promote(&metrics_url).await,
        Commands::Generate {
            lang,
            out,
            metrics_url,
        } => cmd_generate(&metrics_url, &lang, &out).await,
    }
}
