import sys

path = 'C:/Users/King/Desktop/NeonDB/src/main.rs'
src = open(path, encoding='utf-8').read()
orig_len = len(src)

def replace_once(src, old, new, label):
    if old not in src:
        print(f"ERROR: {label} not found", file=sys.stderr)
        sys.exit(1)
    result = src.replace(old, new, 1)
    print(f"  {label}: OK ({len(old)} -> {len(new)} chars)")
    return result

# ── 1. Constants block ────────────────────────────────────────────────────────
OLD_CONST_START = '// ── game/basic core ───────────────────────────────────────────────────────────\nconst BASIC_SPAWN_JS'
OLD_CONST_END   = 'const GAME_REDUCERS_RS: &str    = include_str!("../templates/game_reducers.rs.txt");'

start = src.index(OLD_CONST_START)
end   = src.index(OLD_CONST_END) + len(OLD_CONST_END)

NEW_CONSTANTS = '''// ── Rust game templates ───────────────────────────────────────────────────────
const GAME_MAIN_RS: &str         = include_str!("../templates/r_game_main.rs.txt");
const R_MOD_BASIC: &str          = include_str!("../templates/r_reducers_mod_basic.rs.txt");
const R_SPAWN_RS: &str           = include_str!("../templates/r_spawn.rs.txt");
const R_MOVE_RS: &str            = include_str!("../templates/r_move.rs.txt");
const R_DESPAWN_RS: &str         = include_str!("../templates/r_despawn.rs.txt");
const R_DAMAGE_RS: &str          = include_str!("../templates/r_damage.rs.txt");
const R_HEAL_RS: &str            = include_str!("../templates/r_heal.rs.txt");
const R_BASIC_SCHEMA: &str       = include_str!("../templates/r_basic_schema.toml.txt");

// ── module reducers (neon add <name>) ────────────────────────────────────────
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

// ── Unity + Godot SDKs ────────────────────────────────────────────────────────
const UNITY_CLIENT_CS: &str    = include_str!("engine_templates/unity_NeonDBClient.cs");
const UNITY_BEHAVIOUR_CS: &str = include_str!("engine_templates/unity_NeonDBBehaviour.cs");
const UNITY_MANAGER_CS: &str   = include_str!("../templates/g_unity_Manager.cs.txt");
const UNITY_GAME_README: &str  = include_str!("../templates/g_unity_readme.md.txt");
const GODOT_CLIENT_GD: &str    = include_str!("engine_templates/godot_neondb_client.gd");
const GODOT_MANAGER_GD: &str   = include_str!("../templates/g_godot_Manager.gd.txt");
const GODOT_GAME_README: &str  = include_str!("../templates/g_godot_readme.md.txt");'''

src = src[:start] + NEW_CONSTANTS + src[end:]
print("  constants: OK")

# ── 2. Helper functions + scaffold_game_basic ─────────────────────────────────
OLD_BASIC_START = 'fn scaffold_game_basic(p: &Path, name: &str) -> Result<()> {'
OLD_BASIC_END   = '''    println!("    neon add chat         # rooms, messages");
    println!();
    Ok(())
}

fn scaffold_game_full'''

start = src.index(OLD_BASIC_START)
end   = src.index(OLD_BASIC_END) + len(OLD_BASIC_END)

NEW_BASIC = '''/// Return the path to the neondb crate root, resolved from the running binary.
fn find_neondb_root() -> std::path::PathBuf {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(root) = exe.parent().and_then(|p| p.parent()).and_then(|p| p.parent()) {
            if root.join("Cargo.toml").exists() {
                return root.to_path_buf();
            }
        }
    }
    std::path::PathBuf::from(".")
}

/// Generate a Cargo.toml that embeds the NeonDB server as a library.
fn game_cargo_toml(name: &str) -> String {
    let path = find_neondb_root().to_string_lossy().replace('\\\\', "/");
    format!(
        "[package]\\nname = \\"{name}\\"\\nversion = \\"0.1.0\\"\\nedition = \\"2021\\"\\n\\n\
        [dependencies]\\n\
        neondb     = {{ path = \\"{path}\\" }}\\n\
        serde_json = \\"1\\"\\n"
    )
}

fn scaffold_game_basic(p: &Path, name: &str) -> Result<()> {
    wf(p, "Cargo.toml",                  &game_cargo_toml(name))?;
    wf(p, "src/main.rs",                 GAME_MAIN_RS)?;
    wf(p, "src/reducers/mod.rs",         R_MOD_BASIC)?;
    wf(p, "src/reducers/spawn.rs",       R_SPAWN_RS)?;
    wf(p, "src/reducers/move_player.rs", R_MOVE_RS)?;
    wf(p, "src/reducers/despawn.rs",     R_DESPAWN_RS)?;
    wf(p, "src/reducers/damage.rs",      R_DAMAGE_RS)?;
    wf(p, "src/reducers/heal.rs",        R_HEAL_RS)?;
    wf(p, "schema.toml",                 R_BASIC_SCHEMA)?;
    wf(p, "SCALING.md",                  SCALING_MD)?;
    wf(p, "README.md", &format!("# {name}\\n\\nNeonDB embedded game server.\\n\\nSee SCALING.md for the scaling guide.\\n"))?;
    print_success(name, "game/basic", &[
        ("Cargo.toml",                  "cargo build --release && cargo run --release -- start"),
        ("src/reducers/spawn.rs",       "spawn(player_id, lobby, class)"),
        ("src/reducers/move_player.rs", "move_player(player_id, x, y)"),
        ("src/reducers/despawn.rs",     "despawn(player_id)"),
        ("src/reducers/damage.rs",      "damage(target_id, amount)"),
        ("src/reducers/heal.rs",        "heal(target_id, amount)"),
        ("schema.toml",                 "players + sessions tables"),
    ]);
    println!("  Next steps:");
    println!("    cd {name}");
    println!("    cargo run --release -- start");
    println!("    neon call spawn '[\\"alice\\", \\"lobby_1\\", \\"warrior\\"]'");
    println!();
    println!("  Add systems:");
    println!("    neon add combat    # attack, respawn, abilities");
    println!("    neon add inventory # items, equip slots");
    println!("    neon add chat      # rooms, messages");
    println!();
    Ok(())
}

fn scaffold_game_full'''

src = src[:start] + NEW_BASIC + src[end:]
print("  scaffold_game_basic + helpers: OK")

# ── 3. scaffold_game_full ─────────────────────────────────────────────────────
# Find scaffold_game_full body and replace it
OLD_FULL_START = 'fn scaffold_game_full(p: &Path, name: &str) -> Result<()> {'
OLD_FULL_END   = '''    write_shared_files(p, name, "game/full")?;
    print_success(name, "game/full"'''

# Find the closing block after print_success
idx = src.index(OLD_FULL_START)
# Find the next fn after this one
next_fn = src.index('\nfn scaffold_game_unity', idx)

OLD_FULL = src[idx:next_fn]

NEW_FULL = '''fn scaffold_game_full(p: &Path, name: &str) -> Result<()> {
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
    println!("    reducer = \\"world_tick\\"");
    println!("    interval_ms = 1000");
    println!();
    Ok(())
}

'''

src = src[:idx] + NEW_FULL + src[next_fn:]
print("  scaffold_game_full: OK")

# ── 4. scaffold_game_unity ────────────────────────────────────────────────────
OLD_UNITY = '''fn scaffold_game_unity(p: &Path, name: &str) -> Result<()> {
    // Unity SDK
    wf(p, "unity/NeonDBClient.cs",    UNITY_CLIENT_CS)?;
    wf(p, "unity/NeonDBBehaviour.cs", UNITY_BEHAVIOUR_CS)?;
    wf(p, "unity/NeonDBManager.cs",   UNITY_MANAGER_CS)?;
    wf(p, "unity/README.md",          UNITY_GAME_README)?;
    // Server (same as game/full)
    scaffold_game_full(p, name)?;
    println!("  Unity SDK:");
    println!("    Copy unity/ into Assets/Scripts/NeonDB/");
    println!("    Add NeonDBManager to your scene → set Server URL → press Play");
    Ok(())
}'''

NEW_UNITY = '''fn scaffold_game_unity(p: &Path, name: &str) -> Result<()> {
    scaffold_game_full(p, name)?;
    wf(p, "unity/NeonDBClient.cs",    UNITY_CLIENT_CS)?;
    wf(p, "unity/NeonDBBehaviour.cs", UNITY_BEHAVIOUR_CS)?;
    wf(p, "unity/NeonDBManager.cs",   UNITY_MANAGER_CS)?;
    wf(p, "unity/README.md",          UNITY_GAME_README)?;
    println!("  Unity C# SDK → unity/");
    println!("    Copy unity/ into Assets/Scripts/NeonDB/");
    println!("    Add NeonDBManager to your scene, set Server URL, press Play.");
    Ok(())
}'''

src = replace_once(src, OLD_UNITY, NEW_UNITY, "scaffold_game_unity")

# ── 5. scaffold_game_godot ────────────────────────────────────────────────────
OLD_GODOT = '''fn scaffold_game_godot(p: &Path, name: &str) -> Result<()> {
    // Godot SDK
    wf(p, "godot/neondb_client.gd",   GODOT_CLIENT_GD)?;
    wf(p, "godot/NeonDBManager.gd",   GODOT_MANAGER_GD)?;
    wf(p, "godot/README.md",          GODOT_GAME_README)?;
    // Server (same as game/full)
    scaffold_game_full(p, name)?;
    println!("  Godot SDK:");
    println!("    Copy godot/ into your Godot project");
    println!("    Add NeonDBManager as an autoload (Project Settings → Autoload)");
    println!("    Set server_url → run the game");
    Ok(())
}'''

NEW_GODOT = '''fn scaffold_game_godot(p: &Path, name: &str) -> Result<()> {
    scaffold_game_full(p, name)?;
    wf(p, "godot/neondb_client.gd",   GODOT_CLIENT_GD)?;
    wf(p, "godot/NeonDBManager.gd",   GODOT_MANAGER_GD)?;
    wf(p, "godot/README.md",          GODOT_GAME_README)?;
    println!("  Godot 4 GDScript SDK → godot/");
    println!("    Add godot/ to your project, add NeonDBManager as an Autoload.");
    Ok(())
}'''

src = replace_once(src, OLD_GODOT, NEW_GODOT, "scaffold_game_godot")

# ── 6. cmd_add_module ─────────────────────────────────────────────────────────
OLD_CMD_ADD_START = 'fn cmd_add_module(module: &str, project_path: &Path) -> Result<()> {'
idx_start = src.index(OLD_CMD_ADD_START)

OLD_APPEND_START = '/// Append new schema tables to the existing schema.toml without duplicating.'
idx_end = src.index(OLD_APPEND_START)

OLD_CMD_ADD = src[idx_start:idx_end]

NEW_CMD_ADD = '''/// Write Rust files for a module into src/reducers/<module>/ and register in mod.rs.
fn add_module_files(p: &Path, module: &str) -> Result<()> {
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
        eprintln!("No schema.toml found. Run `neon add` from inside your project directory.");
        return Err(neondb::error::NeonDBError::invalid_argument("not a NeonDB project directory"));
    }
    // Also add `pub mod <module>;` to src/reducers/mod.rs if it exists
    let mod_rs = project_path.join("src/reducers/mod.rs");
    let mod_line = format!("pub mod {module};\\n");
    if mod_rs.exists() {
        let current = fs::read_to_string(&mod_rs).unwrap_or_default();
        if !current.contains(&mod_line) {
            let mut f = fs::OpenOptions::new().append(true).open(&mod_rs)
                .map_err(|e| neondb::error::NeonDBError::internal(format!("open mod.rs: {e}")))?;
            use std::io::Write as _;
            write!(f, "{mod_line}")
                .map_err(|e| neondb::error::NeonDBError::internal(format!("write mod.rs: {e}")))?;
        }
    }
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
            eprintln!("Unknown module \'{}\'. Available: {}", other, names.join(", "));
            return Err(neondb::error::NeonDBError::invalid_argument(
                format!("unknown module \'{}\'", other)));
        }
    }
    println!();
    Ok(())
}

'''

src = src[:idx_start] + NEW_CMD_ADD + src[idx_end:]
print("  cmd_add_module: OK")

# ── 7. Fix append_schema to use [[table]] not [[tables]] ─────────────────────
OLD_APPEND = '        .split(|l: &&str| l.trim().starts_with("[[tables]]"))'
NEW_APPEND = '        .split(|l: &&str| l.trim().starts_with("[[table]]"))'
src = replace_once(src, OLD_APPEND, NEW_APPEND, "append_schema [[table]] fix")

OLD_REJOIN = '        .flat_map(|block| {\n            std::iter::once("[[tables]]").chain(block.iter().copied())\n        })'
NEW_REJOIN = '        .flat_map(|block| {\n            std::iter::once("[[table]]").chain(block.iter().copied())\n        })'
src = replace_once(src, OLD_REJOIN, NEW_REJOIN, "append_schema rejoin fix")

open(path, 'w', encoding='utf-8').write(src)
print(f"\nDone. {orig_len} -> {len(src)} chars")
