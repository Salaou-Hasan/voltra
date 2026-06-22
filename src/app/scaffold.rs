// Project scaffolding: `voltra init` (interactive template picker + per-template
// generators) and `voltra add <module>` (Rust + Voltra-language module writers).
// All embedded file content comes from `crate::app::templates`.

use std::fs;
use std::path::{Path, PathBuf};

use dialoguer::{theme::ColorfulTheme, Input, Select};

use voltra::error::Result;

use crate::app::build::build_voltra_reducers;
use crate::app::templates::*;

pub(crate) fn init_project(path: Option<PathBuf>, template: Option<String>) -> Result<()> {
    let theme = ColorfulTheme::default();

    let project_name: String = match &path {
        Some(p) => p
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("my-project")
            .to_string(),
        None => Input::with_theme(&theme)
            .with_prompt("Project name")
            .default("my-project".to_string())
            .interact_text()
            .map_err(|e| voltra::error::VoltraError::internal(format!("Prompt error: {}", e)))?,
    };

    let project_path: PathBuf = match path {
        Some(p) => p,
        None => {
            let suggested = format!("./{}", project_name);
            let input: String = Input::with_theme(&theme)
                .with_prompt("Project path")
                .default(suggested)
                .interact_text()
                .map_err(|e| {
                    voltra::error::VoltraError::internal(format!("Prompt error: {}", e))
                })?;
            PathBuf::from(input)
        }
    };

    let template_name: String = match template {
        Some(t) => {
            if !TEMPLATES.iter().any(|tmpl| tmpl.name == t) {
                let names: Vec<_> = TEMPLATES.iter().map(|tmpl| tmpl.name).collect();
                eprintln!(
                    "Error: unknown template '{}'. Available: {}",
                    t,
                    names.join(", ")
                );
                return Err(voltra::error::VoltraError::invalid_argument(format!(
                    "unknown template '{}'",
                    t
                )));
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
                    .map_err(|e| {
                        voltra::error::VoltraError::internal(format!("Prompt error: {}", e))
                    })?;
                let category = categories[branch];

                let in_branch: Vec<&Template> = TEMPLATES
                    .iter()
                    .filter(|t| t.category == category)
                    .collect();
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
                    .map_err(|e| {
                        voltra::error::VoltraError::internal(format!("Prompt error: {}", e))
                    })?;
                if leaf == in_branch.len() {
                    continue; // ← Back
                }
                break in_branch[leaf].name.to_string();
            }
        }
    };

    fs::create_dir_all(&project_path).map_err(|e| {
        voltra::error::VoltraError::internal(format!("Cannot create directory: {}", e))
    })?;

    write_shared_files(&project_path, &project_name, &template_name)?;

    match template_name.as_str() {
        "voltra/basic" => scaffold_voltra_basic(&project_path, &project_name)?,
        "voltra/game-ready" => scaffold_voltra_game_ready(&project_path, &project_name)?,
        "voltra/chat" => scaffold_voltra_chat(&project_path, &project_name)?,
        "game/basic" => scaffold_game_basic(&project_path, &project_name, "game/basic")?,
        "game/full" => scaffold_game_full(&project_path, &project_name, "game/full")?,
        "game/unity" => scaffold_game_unity(&project_path, &project_name)?,
        "game/godot" => scaffold_game_godot(&project_path, &project_name)?,
        _ => {
            eprintln!(
                "Unknown template '{}'. Run `voltra templates` to see options.",
                template_name
            );
            return Err(voltra::error::VoltraError::invalid_argument(format!(
                "unknown template '{}'",
                template_name
            )));
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared files (every template)
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) fn write_shared_files(
    project_path: &Path,
    project_name: &str,
    template: &str,
) -> Result<()> {
    let scheduler_note = match template {
        "game/full" =>
            "\n[[scheduler]]\nreducer = \"cleanup_chat\"\ninterval_ms = 60000\n\n[[scheduler]]\nreducer = \"world_tick\"\ninterval_ms = 1000\n\n[[scheduler]]\nreducer = \"session_cleanup\"\ninterval_ms = 60000\n\n[[scheduler]]\nreducer = \"mm_match\"\ninterval_ms = 5000\n",
        // No active scheduler by default — referencing a reducer the chosen
        // template doesn't define makes the scheduler error on every tick.
        // Uncomment after adding a matching reducer (e.g. `voltra add world`).
        _ => "\n# [[scheduler]]\n# reducer = \"world_tick\"\n# interval_ms = 1000\n",
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

    fs::write(project_path.join("voltra.toml"), toml)
        .map_err(|e| voltra::error::VoltraError::internal(format!("Write voltra.toml: {}", e)))?;

    fs::create_dir_all(project_path.join("migrations"))
        .map_err(|e| voltra::error::VoltraError::internal(format!("Create migrations/: {}", e)))?;
    fs::write(
        project_path.join("migrations").join("README.md"),
        MIGRATIONS_README,
    )
    .map_err(|e| {
        voltra::error::VoltraError::internal(format!("Write migrations/README.md: {}", e))
    })?;

    fs::create_dir_all(project_path.join("modules"))
        .map_err(|e| voltra::error::VoltraError::internal(format!("Create modules/: {}", e)))?;

    fs::write(
        project_path.join(".gitignore"),
        "*.wal\n*.bin\nsnapshots/\n*.tmp\nnode_modules/\ndist/\n.env\n",
    )
    .map_err(|e| voltra::error::VoltraError::internal(format!("Write .gitignore: {}", e)))?;

    Ok(())
}

pub(crate) fn wf(project_path: &Path, rel: &str, content: &str) -> Result<()> {
    let full = project_path.join(rel);
    if let Some(parent) = full.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            voltra::error::VoltraError::internal(format!("mkdir {:?}: {}", parent, e))
        })?;
    }
    fs::write(&full, content)
        .map_err(|e| voltra::error::VoltraError::internal(format!("Write {:?}: {}", full, e)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-template scaffolders
// ─────────────────────────────────────────────────────────────────────────────

// ── New game-focused scaffold functions ───────────────────────────────────────

// `VOLTRA_SOURCE_DIR` is defined in `crate::app::templates` and brought into
// scope via the glob `use` at the top of this module.

/// Generate a Cargo.toml that embeds the Voltra server as a library.
///
/// When the local Voltra source is reachable on disk (the common case — `voltra`
/// was installed via `cargo install --path .`), the scaffold uses a direct
/// `path = "..."` dependency. That keeps `cargo build` fully offline:
/// no git fetch, no crates.io index refresh.
///
/// When the source is gone (user installed the prebuilt binary on a different
/// machine), fall back to the git dependency.
/// Sanitize a directory name into a valid Cargo package name.
/// Cargo requires: alphanumeric / `-` / `_`, must not start with a digit.
pub(crate) fn sanitize_package_name(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned
        .chars()
        .next()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(true)
    {
        format!("voltra_{}", cleaned)
    } else {
        cleaned
    }
}

pub(crate) fn game_cargo_toml(name: &str) -> String {
    let pkg_name = sanitize_package_name(name);
    let voltra_dep = if std::path::Path::new(VOLTRA_SOURCE_DIR).exists() {
        format!(
            "voltra     = {{ path = \"{}\" }}",
            VOLTRA_SOURCE_DIR.replace('\\', "/")
        )
    } else {
        format!(
            "voltra     = {{ git = \"https://github.com/Salaou-Hasan/Voltra\", tag = \"v{}\" }}",
            env!("CARGO_PKG_VERSION")
        )
    };
    format!(
        "[workspace]\n\n\
[package]\nname = \"{pkg_name}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n\
[dependencies]\n{voltra_dep}\n\
serde      = {{ version = \"1\", features = [\"derive\"] }}\nserde_json = \"1\"\n\
env_logger = \"0.11\"\n"
    )
}

/// Write all client SDKs + protocol docs into clients/ inside a scaffolded project.
/// Covers Rust (Bevy / CLI), Unity C#, Godot 4 GDScript, and a PROTOCOL.md
/// so anyone building a custom engine client knows exactly what to implement.
pub(crate) fn scaffold_all_clients(p: &Path, name: &str) -> Result<()> {
    // Rust client (Bevy, CLI tools, bots, custom engines in Rust)
    wf(p, "clients/rust/Cargo.toml", &client_cargo_toml(name))?;
    wf(p, "clients/rust/src/main.rs", CLIENT_MAIN_RS)?;
    // Pin transitive deps so `cargo run` in clients/rust/ stays offline too.
    let src_lock = std::path::Path::new(VOLTRA_SOURCE_DIR).join("Cargo.lock");
    if src_lock.exists() {
        let _ = fs::copy(&src_lock, p.join("clients/rust/Cargo.lock"));
    }

    // Unity C# client (copy clients/unity/ into Assets/Scripts/Voltra/)
    wf(p, "clients/unity/VoltraClient.cs", UNITY_CLIENT_CS)?;
    wf(p, "clients/unity/VoltraBehaviour.cs", UNITY_BEHAVIOUR_CS)?;
    wf(p, "clients/unity/VoltraManager.cs", UNITY_MANAGER_CS)?;

    // Godot 4 GDScript client (add as Autoload in Project Settings)
    wf(p, "clients/godot/voltra_client.gd", GODOT_CLIENT_GD)?;
    wf(p, "clients/godot/VoltraManager.gd", GODOT_MANAGER_GD)?;

    // Wire protocol spec for custom engine implementations (C++, JS, Swift, etc.)
    wf(p, "clients/PROTOCOL.md", CLIENT_PROTOCOL_MD)?;

    Ok(())
}

/// Copy Voltra's Cargo.lock into the scaffolded project when available,
/// so transitive dep versions are pinned and no crates.io index refresh runs.
pub(crate) fn copy_lockfile_if_available(p: &Path) -> Result<()> {
    let src_lock = std::path::Path::new(VOLTRA_SOURCE_DIR).join("Cargo.lock");
    if src_lock.exists() {
        let _ = fs::copy(&src_lock, p.join("Cargo.lock"));
    }
    Ok(())
}

pub(crate) fn scaffold_game_basic(p: &Path, name: &str, template: &str) -> Result<()> {
    wf(p, "Cargo.toml", &game_cargo_toml(name))?;
    copy_lockfile_if_available(p)?;
    wf(p, "src/main.rs", GAME_MAIN_RS)?;
    wf(p, "src/reducers/mod.rs", R_MOD_BASIC)?;
    wf(p, "src/reducers/spawn.rs", R_SPAWN_RS)?;
    wf(p, "src/reducers/move_player.rs", R_MOVE_RS)?;
    wf(p, "src/reducers/despawn.rs", R_DESPAWN_RS)?;
    wf(p, "src/reducers/damage.rs", R_DAMAGE_RS)?;
    wf(p, "src/reducers/heal.rs", R_HEAL_RS)?;
    wf(p, "schema.toml", R_BASIC_SCHEMA)?;
    wf(p, "SCALING.md", SCALING_MD)?;
    wf(
        p,
        "README.md",
        &format!(
            "# {name}\n\nVoltra embedded game server.\n\nSee SCALING.md for the scaling guide.\n"
        ),
    )?;
    scaffold_all_clients(p, name)?;
    // Chat (lobby + proximity) is built-in to every template
    add_module_files(p, "chat")?;
    print_success(
        name,
        template,
        &[
            (
                "Cargo.toml",
                "voltra game server (run `voltra start` from this folder)",
            ),
            ("src/reducers/spawn.rs", "spawn(player_id, lobby, class)"),
            (
                "src/reducers/move_player.rs",
                "move_player(player_id, x, y)",
            ),
            ("src/reducers/despawn.rs", "despawn(player_id)"),
            ("src/reducers/damage.rs", "damage(target_id, amount)"),
            ("src/reducers/heal.rs", "heal(target_id, amount)"),
            (
                "src/reducers/chat/send.rs",
                "send_message(room, player_id, name, text, type, x, z)",
            ),
            ("src/reducers/chat/join.rs", "join_room(room, player_id)"),
            ("src/reducers/chat/leave.rs", "leave_room(room, player_id)"),
            ("schema.toml", "players + sessions + chat tables"),
            ("clients/rust/src/main.rs", "Rust client (Bevy / CLI)"),
            ("clients/unity/VoltraClient.cs", "Unity C# client"),
            ("clients/godot/voltra_client.gd", "Godot 4 GDScript client"),
            (
                "clients/PROTOCOL.md",
                "wire protocol — implement your own client",
            ),
        ],
    );
    println!("  Next steps:");
    println!("    cd {name}");
    println!("    voltra start");
    println!("    # Rust client (another terminal):");
    println!("    cd clients/rust && cargo run --release");
    println!("    # Unity: copy clients/unity/ into Assets/Scripts/Voltra/");
    println!("    # Godot: add clients/godot/ files, set voltra_client.gd as Autoload");
    println!();
    println!("  Chat is built-in — call send_message(room, player_id, name, text, \"lobby\"|\"proximity\", x, z)");
    println!("  Add more systems:");
    println!("    voltra add combat    # attack, respawn, abilities");
    println!("    voltra add inventory # items, equip slots");
    println!();
    Ok(())
}

pub(crate) fn scaffold_game_full(p: &Path, name: &str, template: &str) -> Result<()> {
    // Core reducers (chat is added inside scaffold_game_basic)
    scaffold_game_basic(p, name, template)?;
    // All 9 additional modules pre-installed
    add_module_files(p, "inventory")?;
    add_module_files(p, "leaderboard")?;
    add_module_files(p, "matchmaking")?;
    add_module_files(p, "guilds")?;
    add_module_files(p, "quests")?;
    add_module_files(p, "economy")?;
    add_module_files(p, "combat")?;
    add_module_files(p, "world")?;
    println!("  All 9 modules included. See src/reducers/ for the full source.");
    println!("  Add to voltra.toml for scheduled reducers:");
    println!("    [[scheduler]]");
    println!("    reducer = \"world_tick\"");
    println!("    interval_ms = 1000");
    println!();
    Ok(())
}

pub(crate) fn scaffold_game_unity(p: &Path, name: &str) -> Result<()> {
    scaffold_game_full(p, name, "game/unity")?;
    wf(p, "unity/VoltraClient.cs", UNITY_CLIENT_CS)?;
    wf(p, "unity/VoltraBehaviour.cs", UNITY_BEHAVIOUR_CS)?;
    wf(p, "unity/VoltraManager.cs", UNITY_MANAGER_CS)?;
    wf(p, "unity/README.md", UNITY_GAME_README)?;
    println!("  Unity C# SDK → unity/  (also in clients/unity/)");
    println!("    Copy unity/ into Assets/Scripts/Voltra/");
    println!("    Add VoltraManager to your scene, set Server URL, press Play.");
    println!("  Rust / Godot / custom engine clients → clients/");
    println!("    See clients/PROTOCOL.md to implement your own client.");
    Ok(())
}

pub(crate) fn scaffold_game_godot(p: &Path, name: &str) -> Result<()> {
    scaffold_game_full(p, name, "game/godot")?;
    wf(p, "godot/voltra_client.gd", GODOT_CLIENT_GD)?;
    wf(p, "godot/VoltraManager.gd", GODOT_MANAGER_GD)?;
    wf(p, "godot/README.md", GODOT_GAME_README)?;
    println!("  Godot 4 GDScript SDK → godot/  (also in clients/godot/)");
    println!("    Add godot/ files to your project, set voltra_client.gd as Autoload.");
    println!("  Rust / Unity / custom engine clients → clients/");
    println!("    See clients/PROTOCOL.md to implement your own client.");
    Ok(())
}

pub(crate) fn compile_voltra_to_rs(voltra_source: &str) -> String {
    match voltra::dsl::compile(voltra_source, "reducers") {
        Ok(rs) => rs,
        Err(_) => {
            "// Auto-generated by voltra build. Run `voltra build` to regenerate.\n".to_owned()
        }
    }
}

pub(crate) fn scaffold_voltra_basic(p: &Path, name: &str) -> Result<()> {
    let all_voltra = concat_strs(&[
        VOLTRA_BASIC_SCHEMA,
        VOLTRA_BASIC_SPAWN,
        VOLTRA_BASIC_MOVEMENT,
        VOLTRA_BASIC_COMBAT,
        VOLTRA_BASIC_SYSTEM,
    ]);
    wf(p, "Cargo.toml", &game_cargo_toml(name))?;
    copy_lockfile_if_available(p)?;
    wf(p, "src/main.rs", GAME_MAIN_RS)?;
    // Per-file reducer layout (mirrors the Rust template structure)
    wf(p, "reducers/schema.vol", VOLTRA_BASIC_SCHEMA)?;
    wf(p, "reducers/spawn.vol", VOLTRA_BASIC_SPAWN)?;
    wf(p, "reducers/movement.vol", VOLTRA_BASIC_MOVEMENT)?;
    wf(p, "reducers/combat.vol", VOLTRA_BASIC_COMBAT)?;
    wf(p, "reducers/system.vol", VOLTRA_BASIC_SYSTEM)?;
    wf(p, "src/reducers.rs", &compile_voltra_to_rs(&all_voltra))?;
    wf(p, "schema.toml", R_BASIC_SCHEMA)?;
    wf(p, "SCALING.md", SCALING_MD)?;
    wf(p, ".vscode/settings.json", VSCODE_VOLTRA_SETTINGS)?;
    wf(p, "README.md",                   &format!("# {name}\n\nVoltra Voltra-language game server.\n\nEdit files in `reducers/`, run `voltra build`, then `voltra start`.\n\nSee `docs/voltra/README.md` for the language reference.\n"))?;
    wf(p, "docs/voltra/README.md", VOLTRA_LANG_REFERENCE)?;
    scaffold_all_clients(p, name)?;
    print_success(
        name,
        "voltra/basic",
        &[
            ("reducers/schema.vol", "table definitions"),
            ("reducers/spawn.vol", "spawn + despawn"),
            ("reducers/movement.vol", "move_player"),
            ("reducers/combat.vol", "damage + heal"),
            (
                "reducers/system.vol",
                "get_stats + cleanup_dead (scheduler)",
            ),
            ("src/reducers.rs", "auto-generated — do not edit"),
            ("docs/voltra/README.md", "Voltra language reference"),
            ("clients/rust/src/main.rs", "Rust client"),
            ("clients/unity/VoltraClient.cs", "Unity C# client"),
            ("clients/godot/voltra_client.gd", "Godot 4 client"),
        ],
    );
    println!("  Voltra workflow:");
    println!("    1. Edit any file in reducers/");
    println!("    2. voltra build    — compile .vol → native Rust");
    println!("    3. voltra start    — start the server");
    println!();
    Ok(())
}

pub(crate) fn scaffold_voltra_game_ready(p: &Path, name: &str) -> Result<()> {
    let all_voltra = concat_strs(&[
        VOLTRA_GAME_SCHEMA,
        VOLTRA_GAME_SPAWN,
        VOLTRA_GAME_MOVEMENT,
        VOLTRA_GAME_COMBAT,
        VOLTRA_GAME_PROGRESSION,
        VOLTRA_GAME_ECONOMY,
        VOLTRA_GAME_GUILDS,
        VOLTRA_GAME_LEADERBOARD,
        VOLTRA_GAME_SYSTEM,
    ]);
    wf(p, "Cargo.toml", &game_cargo_toml(name))?;
    copy_lockfile_if_available(p)?;
    wf(p, "src/main.rs", GAME_MAIN_RS)?;
    wf(p, "reducers/schema.vol", VOLTRA_GAME_SCHEMA)?;
    wf(p, "reducers/spawn.vol", VOLTRA_GAME_SPAWN)?;
    wf(p, "reducers/movement.vol", VOLTRA_GAME_MOVEMENT)?;
    wf(p, "reducers/combat.vol", VOLTRA_GAME_COMBAT)?;
    wf(p, "reducers/progression.vol", VOLTRA_GAME_PROGRESSION)?;
    wf(p, "reducers/economy.vol", VOLTRA_GAME_ECONOMY)?;
    wf(p, "reducers/guilds.vol", VOLTRA_GAME_GUILDS)?;
    wf(p, "reducers/leaderboard.vol", VOLTRA_GAME_LEADERBOARD)?;
    wf(p, "reducers/system.vol", VOLTRA_GAME_SYSTEM)?;
    wf(p, "src/reducers.rs", &compile_voltra_to_rs(&all_voltra))?;
    wf(p, "schema.toml", R_BASIC_SCHEMA)?;
    wf(p, "SCALING.md", SCALING_MD)?;
    wf(p, ".vscode/settings.json", VSCODE_VOLTRA_SETTINGS)?;
    wf(p, "README.md",                   &format!("# {name}\n\nVoltra Voltra-language game server — full game template.\n\nEdit files in `reducers/`, run `voltra build`, then `voltra start`.\n\nSee `docs/voltra/README.md` for the language reference.\n"))?;
    wf(p, "docs/voltra/README.md", VOLTRA_LANG_REFERENCE)?;
    scaffold_all_clients(p, name)?;
    print_success(
        name,
        "voltra/game-ready",
        &[
            (
                "reducers/schema.vol",
                "table definitions (players + guilds)",
            ),
            ("reducers/spawn.vol", "spawn + despawn"),
            ("reducers/movement.vol", "move_player"),
            ("reducers/combat.vol", "take_damage + heal"),
            ("reducers/progression.vol", "grant_xp + roll_loot"),
            ("reducers/economy.vol", "transfer_gold"),
            ("reducers/guilds.vol", "create_guild + join + leave"),
            ("reducers/leaderboard.vol", "leaderboard + top_killers"),
            (
                "reducers/system.vol",
                "get_stats + cleanup_dead (scheduler)",
            ),
            ("src/reducers.rs", "auto-generated — do not edit"),
            ("docs/voltra/README.md", "Voltra language reference"),
            ("clients/", "Rust, Unity, Godot client SDKs"),
        ],
    );
    println!("  Voltra workflow:");
    println!("    1. Edit any file in reducers/");
    println!("    2. voltra build");
    println!("    3. voltra start");
    println!();
    Ok(())
}

pub(crate) fn scaffold_voltra_chat(p: &Path, name: &str) -> Result<()> {
    let all_voltra = concat_strs(&[
        VOLTRA_CHAT_SCHEMA_VOLTRA,
        VOLTRA_CHAT_ROOMS,
        VOLTRA_CHAT_MESSAGES,
        VOLTRA_CHAT_SYSTEM,
    ]);
    wf(p, "Cargo.toml", &game_cargo_toml(name))?;
    copy_lockfile_if_available(p)?;
    wf(p, "src/main.rs", GAME_MAIN_RS)?;
    wf(p, "reducers/schema.vol", VOLTRA_CHAT_SCHEMA_VOLTRA)?;
    wf(p, "reducers/rooms.vol", VOLTRA_CHAT_ROOMS)?;
    wf(p, "reducers/messages.vol", VOLTRA_CHAT_MESSAGES)?;
    wf(p, "reducers/system.vol", VOLTRA_CHAT_SYSTEM)?;
    wf(p, "src/reducers.rs", &compile_voltra_to_rs(&all_voltra))?;
    wf(p, "schema.toml", VOLTRA_CHAT_SCHEMA)?;
    wf(p, ".vscode/settings.json", VSCODE_VOLTRA_SETTINGS)?;
    wf(p, "README.md",                   &format!("# {name}\n\nVoltra Voltra-language chat server.\n\nEdit files in `reducers/`, run `voltra build`, then `voltra start`.\n\nSee `docs/voltra/README.md` for the language reference.\n"))?;
    wf(p, "docs/voltra/README.md", VOLTRA_LANG_REFERENCE)?;
    scaffold_all_clients(p, name)?;
    print_success(
        name,
        "voltra/chat",
        &[
            (
                "reducers/schema.vol",
                "table definitions (rooms + messages + members)",
            ),
            ("reducers/rooms.vol", "create_room + join_room + leave_room"),
            ("reducers/messages.vol", "send_message + list_rooms"),
            (
                "reducers/system.vol",
                "online_count + room_members + kick + cleanup",
            ),
            ("src/reducers.rs", "auto-generated — do not edit"),
            ("docs/voltra/README.md", "Voltra language reference"),
        ],
    );
    println!("  Voltra workflow:");
    println!("    1. Edit any file in reducers/");
    println!("    2. voltra build");
    println!("    3. voltra start");
    println!();
    Ok(())
}

pub(crate) fn register_module_in_mod_rs(p: &Path, module: &str) -> Result<()> {
    let mod_rs = p.join("src/reducers/mod.rs");
    let line = format!("pub mod {module};\n");
    let existing = fs::read_to_string(&mod_rs).unwrap_or_default();
    if existing.contains(line.trim_end()) {
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
        .map_err(|e| voltra::error::VoltraError::internal(format!("write mod.rs: {e}")))?;
    Ok(())
}

pub(crate) fn add_module_files(p: &Path, module: &str) -> Result<()> {
    register_module_in_mod_rs(p, module)?;
    match module {
        "chat" => {
            wf(p, "src/reducers/chat/mod.rs", RM_CHAT_MOD_RS)?;
            wf(p, "src/reducers/chat/send.rs", RM_CHAT_SEND_RS)?;
            wf(p, "src/reducers/chat/join.rs", RM_CHAT_JOIN_RS)?;
            wf(p, "src/reducers/chat/leave.rs", RM_CHAT_LEAVE_RS)?;
            wf(p, "src/reducers/chat/cleanup.rs", RM_CHAT_CLEANUP_RS)?;
            append_schema(p, RM_CHAT_SCHEMA)?;
        }
        "inventory" => {
            wf(p, "src/reducers/inventory/mod.rs", RM_INV_MOD_RS)?;
            wf(p, "src/reducers/inventory/add.rs", RM_INV_ADD_RS)?;
            wf(p, "src/reducers/inventory/remove.rs", RM_INV_REMOVE_RS)?;
            wf(p, "src/reducers/inventory/equip.rs", RM_INV_EQUIP_RS)?;
            append_schema(p, RM_INV_SCHEMA)?;
        }
        "leaderboard" => {
            wf(p, "src/reducers/leaderboard/mod.rs", RM_LB_MOD_RS)?;
            wf(p, "src/reducers/leaderboard/submit.rs", RM_LB_SUBMIT_RS)?;
            wf(p, "src/reducers/leaderboard/reset.rs", RM_LB_RESET_RS)?;
            append_schema(p, RM_LB_SCHEMA)?;
        }
        "matchmaking" => {
            wf(p, "src/reducers/matchmaking/mod.rs", RM_MM_MOD_RS)?;
            wf(p, "src/reducers/matchmaking/queue.rs", RM_MM_QUEUE_RS)?;
            wf(p, "src/reducers/matchmaking/dequeue.rs", RM_MM_DEQUEUE_RS)?;
            wf(
                p,
                "src/reducers/matchmaking/match_players.rs",
                RM_MM_MATCH_RS,
            )?;
            append_schema(p, RM_MM_SCHEMA)?;
        }
        "guilds" => {
            wf(p, "src/reducers/guilds/mod.rs", RM_GUILD_MOD_RS)?;
            wf(p, "src/reducers/guilds/create.rs", RM_GUILD_CREATE_RS)?;
            wf(p, "src/reducers/guilds/invite.rs", RM_GUILD_INVITE_RS)?;
            wf(p, "src/reducers/guilds/accept.rs", RM_GUILD_ACCEPT_RS)?;
            wf(p, "src/reducers/guilds/kick.rs", RM_GUILD_KICK_RS)?;
            append_schema(p, RM_GUILD_SCHEMA)?;
        }
        "quests" => {
            wf(p, "src/reducers/quests/mod.rs", RM_QUEST_MOD_RS)?;
            wf(p, "src/reducers/quests/accept.rs", RM_QUEST_ACCEPT_RS)?;
            wf(p, "src/reducers/quests/progress.rs", RM_QUEST_PROGRESS_RS)?;
            wf(p, "src/reducers/quests/complete.rs", RM_QUEST_COMPLETE_RS)?;
            append_schema(p, RM_QUEST_SCHEMA)?;
        }
        "economy" => {
            wf(p, "src/reducers/economy/mod.rs", RM_ECON_MOD_RS)?;
            wf(p, "src/reducers/economy/buy.rs", RM_ECON_BUY_RS)?;
            wf(p, "src/reducers/economy/sell.rs", RM_ECON_SELL_RS)?;
            wf(p, "src/reducers/economy/transfer.rs", RM_ECON_TRANSFER_RS)?;
            wf(p, "src/reducers/economy/loot.rs", RM_ECON_LOOT_RS)?;
            append_schema(p, RM_ECON_SCHEMA)?;
        }
        "combat" => {
            wf(p, "src/reducers/combat/mod.rs", RM_COMBAT_MOD_RS)?;
            wf(p, "src/reducers/combat/attack.rs", RM_COMBAT_ATTACK_RS)?;
            wf(p, "src/reducers/combat/respawn.rs", RM_COMBAT_RESPAWN_RS)?;
            wf(p, "src/reducers/combat/ability.rs", RM_COMBAT_ABILITY_RS)?;
            append_schema(p, RM_COMBAT_SCHEMA)?;
        }
        "world" => {
            wf(p, "src/reducers/world/mod.rs", RM_WORLD_MOD_RS)?;
            wf(p, "src/reducers/world/tick.rs", RM_WORLD_TICK_RS)?;
            wf(p, "src/reducers/world/npc_spawn.rs", RM_WORLD_NPC_RS)?;
            wf(p, "src/reducers/world/cleanup.rs", RM_WORLD_CLEANUP_RS)?;
            append_schema(p, RM_WORLD_SCHEMA)?;
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn cmd_add_module(module: &str, project_path: &Path) -> Result<()> {
    if !project_path.join("schema.toml").exists() {
        eprintln!("No schema.toml found. Run `voltra add` from inside your project directory.");
        return Err(voltra::error::VoltraError::invalid_argument(
            "not a Voltra project directory",
        ));
    }

    // If this is a Voltra-language project, write a .vol file instead of Rust files.
    if project_path.join("reducers").is_dir() || project_path.join("reducers.vol").exists() {
        return cmd_add_module_voltra(module, project_path);
    }

    // Rust project path — write .rs files.
    match module {
        "chat" | "inventory" | "leaderboard" | "matchmaking" | "guilds" | "quests" | "economy"
        | "combat" | "world" => {
            add_module_files(project_path, module)?;
            println!();
            println!("  Added {module} module → src/reducers/{module}/");
            println!("  Rebuild: cargo build --release");
            println!("  Restart: cargo run --release -- start");
        }
        other => {
            let names: Vec<&str> = MODULES.iter().map(|(n, _)| *n).collect();
            eprintln!(
                "Unknown module '{}'. Available: {}",
                other,
                names.join(", ")
            );
            return Err(voltra::error::VoltraError::invalid_argument(format!(
                "unknown module '{}'",
                other
            )));
        }
    }
    println!();
    Ok(())
}

/// Voltra project: write a dedicated reducers/<module>.vol file, then rebuild.
pub(crate) fn cmd_add_module_voltra(module: &str, project_path: &Path) -> Result<()> {
    let voltra_snippet = match module {
        "chat" => VOLTRA_MOD_CHAT,
        "inventory" => VOLTRA_MOD_INVENTORY,
        "leaderboard" => VOLTRA_MOD_LEADERBOARD,
        "economy" => VOLTRA_MOD_ECONOMY,
        "guilds" => VOLTRA_MOD_GUILDS,
        "quests" => VOLTRA_MOD_QUESTS,
        "combat" => VOLTRA_MOD_COMBAT,
        "matchmaking" => VOLTRA_MOD_MATCHMAKING,
        "world" => VOLTRA_MOD_WORLD,
        other => {
            let names: Vec<&str> = MODULES.iter().map(|(n, _)| *n).collect();
            eprintln!(
                "Unknown module '{}'. Available: {}",
                other,
                names.join(", ")
            );
            return Err(voltra::error::VoltraError::invalid_argument(format!(
                "unknown module '{}'",
                other
            )));
        }
    };

    let reducers_dir = project_path.join("reducers");
    let target_path = if reducers_dir.is_dir() {
        // New per-file layout: write reducers/<module>.vol
        let path = reducers_dir.join(format!("{module}.vol"));
        if path.exists() {
            println!("  {module} module already exists at reducers/{module}.vol — skipped.");
            println!();
            return Ok(());
        }
        path
    } else {
        // Legacy single-file layout: append to reducers.vol
        let voltra_path = project_path.join("reducers.vol");
        let existing = fs::read_to_string(&voltra_path).unwrap_or_default();
        let marker = format!("// ── {module} module");
        if existing.contains(&marker) {
            println!("  {module} module already present in reducers.vol — skipped.");
            println!();
            return Ok(());
        }
        use std::io::Write as _;
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&voltra_path)
            .map_err(|e| voltra::error::VoltraError::internal(format!("open reducers.vol: {e}")))?;
        writeln!(file, "\n{}", voltra_snippet.trim()).map_err(|e| {
            voltra::error::VoltraError::internal(format!("append reducers.vol: {e}"))
        })?;
        println!();
        println!("  Added {module} module → reducers.vol");
        println!("  Rebuild: voltra build");
        println!("  Restart: voltra start");
        println!();
        println!("  Recompiling...");
        return build_voltra_reducers(project_path);
    };

    fs::write(&target_path, voltra_snippet.trim()).map_err(|e| {
        voltra::error::VoltraError::internal(format!("write reducers/{module}.vol: {e}"))
    })?;

    println!();
    println!("  Added {module} module → reducers/{module}.vol");
    println!("  Rebuild: voltra build");
    println!("  Restart: voltra start");
    println!();

    println!("  Recompiling...");
    build_voltra_reducers(project_path)?;
    Ok(())
}

/// Append new schema tables to the existing schema.toml without duplicating.
pub(crate) fn append_schema(project_path: &Path, extra: &str) -> Result<()> {
    let schema_path = project_path.join("schema.toml");
    let existing = fs::read_to_string(&schema_path).unwrap_or_default();
    // Extract table names from extra to skip already-present tables
    let new_content: String = extra
        .lines()
        .collect::<Vec<_>>()
        .split(|l: &&str| l.trim().starts_with("[[table]]"))
        .filter(|block| {
            // Find the `name = "..."` line in this block
            let block_name = block.iter().find_map(|l| {
                l.trim()
                    .strip_prefix("name = \"")
                    .and_then(|s| s.strip_suffix('"'))
            });
            // Skip blocks whose table name is already in the schema
            block_name
                .map(|n| !existing.contains(&format!("name = \"{n}\"")))
                .unwrap_or(true)
        })
        .flat_map(|block| std::iter::once("[[table]]").chain(block.iter().copied()))
        .collect::<Vec<_>>()
        .join("\n");

    if new_content.trim().is_empty() {
        println!("  (all tables already present in schema.toml — skipped)");
        return Ok(());
    }
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&schema_path)
        .map_err(|e| voltra::error::VoltraError::internal(format!("open schema.toml: {e}")))?;
    use std::io::Write as _;
    writeln!(file, "\n{}", new_content.trim())
        .map_err(|e| voltra::error::VoltraError::internal(format!("append schema.toml: {e}")))
}

pub(crate) fn print_success(project_name: &str, template: &str, files: &[(&str, &str)]) {
    println!();
    println!(
        "  ✓ Project '{}' created  (template: {})",
        project_name, template
    );
    println!();
    for (file, desc) in files {
        if desc.is_empty() {
            println!("    {}", file);
        } else {
            println!("    {:<40} {}", file, desc);
        }
    }
    println!();
}

// ── Rust client SDK scaffold ──────────────────────────────────────────────────

pub(crate) fn client_cargo_toml(name: &str) -> String {
    let client_dep = if std::path::Path::new(VOLTRA_SOURCE_DIR).exists() {
        format!(
            "voltra-client = {{ path = \"{}/voltra-client-rust\", package = \"voltra-client\" }}",
            VOLTRA_SOURCE_DIR.replace('\\', "/")
        )
    } else {
        format!(
            "voltra-client = {{ git = \"https://github.com/Salaou-Hasan/Voltra\", tag = \"v{}\", package = \"voltra-client\" }}",
            env!("CARGO_PKG_VERSION")
        )
    };
    format!(
        "[workspace]\n\n\
[package]\nname = \"{name}-client\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n\
[dependencies]\n{client_dep}\n\
tokio         = {{ version = \"1\", features = [\"full\"] }}\n\
serde_json    = \"1\"\n"
    )
}
