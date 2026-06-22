// CLI command implementations that talk to a running server over HTTP/WS:
// drain/undrain, backup, promote, generate (codegen), cluster-status — plus
// the `voltra start` project-detection helpers (is_game_project, print_banner,
// cmd_start_project) and the template/module listing commands.

use std::path::Path;

use voltra::error::Result;

use crate::app::templates::{MODULES, TEMPLATES};

pub(crate) async fn cmd_drain(metrics_url: &str, enable: bool) -> Result<()> {
    let url = format!("{}/admin/api/drain", metrics_url);
    let client = reqwest::Client::new();
    let resp = if enable {
        client.post(&url).send().await
    } else {
        client.delete(&url).send().await
    }.map_err(|e| voltra::error::VoltraError::network_error(format!("Cannot reach {}: {}", url, e)))?;

    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({}));
    let draining = body["draining"].as_bool().unwrap_or(enable);
    let conns = body["active_connections"].as_u64().unwrap_or(0);
    let msg = body["message"].as_str().unwrap_or("");

    if draining {
        println!("⚠  Server is DRAINING — {} active connection(s) still live", conns);
        println!("   {}", msg);
        println!("   Poll GET {}/admin/api/drain until active_connections=0,", metrics_url);
        println!("   then restart / apply fix, then: voltra undrain");
    } else {
        println!("✓  Drain disabled — server accepting connections normally ({} active)", conns);
        println!("   {}", msg);
    }
    Ok(())
}

pub(crate) async fn cmd_backup(metrics_url: &str) -> Result<()> {
    let url = format!("{}/backup", metrics_url);
    let resp = reqwest::Client::new().post(&url).send().await.map_err(|e| {
        voltra::error::VoltraError::network_error(format!("Cannot reach {}: {}", url, e))
    })?;
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({}));
    if status.is_success() {
        println!("Backup written: {}", body["path"].as_str().unwrap_or("?"));
        println!("  seq:  {}", body["last_seq"]);
        println!("  rows: {}", body["row_count"]);
    } else {
        eprintln!("Backup failed (HTTP {}): {}", status, body);
        return Err(voltra::error::VoltraError::internal("backup failed"));
    }
    Ok(())
}

pub(crate) fn cmd_list_backups(dir: &Path) {
    let backups = voltra::backup::list_backups(dir);
    if backups.is_empty() {
        println!("No backups found in {:?}", dir);
        return;
    }
    println!("{:<24} {:>12} {:>10}  PATH", "CREATED", "SEQ", "ROWS");
    for (path, ts, seq) in &backups {
        let rows = voltra::backup::read_meta(path).map(|m| m.row_count).unwrap_or(0);
        let dt = chrono_like_fmt(*ts);
        println!("{:<24} {:>12} {:>10}  {}", dt, seq, rows, path.display());
    }
}

/// Minimal unix-secs → "YYYY-MM-DD HH:MM:SS UTC" formatter (no chrono dep).
pub(crate) fn chrono_like_fmt(unix_secs: u64) -> String {
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

pub(crate) async fn cmd_promote(metrics_url: &str) -> Result<()> {
    let url = format!("{}/replication/promote", metrics_url);
    let resp = reqwest::Client::new().post(&url).send().await.map_err(|e| {
        voltra::error::VoltraError::network_error(format!("Cannot reach {}: {}", url, e))
    })?;
    let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::json!({}));
    println!("{}", serde_json::to_string_pretty(&body).unwrap_or_default());
    Ok(())
}

pub(crate) async fn cmd_generate(metrics_url: &str, lang: &str, out: &Path) -> Result<()> {
    // Fetch the full schema from the running server.
    let url = format!("{}/schema", metrics_url);
    let schema: serde_json::Value = reqwest::Client::new()
        .get(&url)
        .send().await
        .map_err(|e| voltra::error::VoltraError::network_error(format!("Cannot reach {}: {}", url, e)))?
        .json().await
        .map_err(|e| voltra::error::VoltraError::internal(format!("Invalid schema JSON: {}", e)))?;

    std::fs::create_dir_all(out).map_err(|e| {
        voltra::error::VoltraError::internal(format!("Cannot create output dir: {}", e))
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
            return Err(voltra::error::VoltraError::invalid_argument(
                format!("Unknown --lang '{}'. Supported: typescript, gdscript", other)
            ));
        }
    }
    Ok(())
}

pub(crate) fn col_type_to_ts(type_str: &str) -> &'static str {
    match type_str.to_lowercase().as_str() {
        "string" | "str" | "text" => "string",
        "i64" | "i32" | "int" | "integer" | "number" => "number",
        "f64" | "f32" | "float" | "double" => "number",
        "bool" | "boolean" => "boolean",
        "bytes" | "blob" => "Uint8Array",
        _ => "unknown",
    }
}

pub(crate) fn col_type_to_gd(type_str: &str) -> &'static str {
    match type_str.to_lowercase().as_str() {
        "string" | "str" | "text" => "String",
        "i64" | "i32" | "int" | "integer" | "number" => "int",
        "f64" | "f32" | "float" | "double" => "float",
        "bool" | "boolean" => "bool",
        "bytes" | "blob" => "PackedByteArray",
        _ => "Variant",
    }
}

pub(crate) fn snake_to_pascal(s: &str) -> String {
    s.split('_').map(|w| {
        let mut c = w.chars();
        match c.next() {
            None => String::new(),
            Some(f) => f.to_uppercase().to_string() + c.as_str(),
        }
    }).collect()
}

pub(crate) fn generate_typescript(
    tables: &serde_json::Map<String, serde_json::Value>,
    reducers: &[serde_json::Value],
    version: &str,
    out: &Path,
) -> Result<()> {
    // ── tables.ts ─────────────────────────────────────────────────────────────
    let mut tables_ts = format!(
        "// tables.ts — AUTO-GENERATED by `voltra generate` from server v{}\n// DO NOT EDIT — run `voltra generate` to regenerate\n\n",
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
        "// reducers.ts — AUTO-GENERATED by `voltra generate` from server v{}\n// DO NOT EDIT — run `voltra generate` to regenerate\n\nimport type {{ VoltraClient }} from 'voltra-client';\n\nexport const Reducers = {{\n",
        version
    );
    for r in reducers {
        let name = match r.as_str() { Some(s) => s, None => continue };
        // camelCase = PascalCase with a lowercased first character.
        let mut camel = snake_to_pascal(name);
        if let Some(f) = camel.get_mut(0..1) { f.make_ascii_lowercase(); }
        reducers_ts.push_str(&format!(
            "  {}: (db: VoltraClient, ...args: unknown[]) => db.call('{}', args),\n",
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

pub(crate) fn generate_gdscript(
    tables: &serde_json::Map<String, serde_json::Value>,
    reducers: &[serde_json::Value],
    version: &str,
    out: &Path,
) -> Result<()> {
    // ── tables.gd ─────────────────────────────────────────────────────────────
    let mut tables_gd = format!(
        "# tables.gd — AUTO-GENERATED by `voltra generate` from server v{}\n# DO NOT EDIT — run `voltra generate` to regenerate\n\n",
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
        "# reducers.gd — AUTO-GENERATED by `voltra generate` from server v{}\n# DO NOT EDIT — run `voltra generate` to regenerate\n\nclass_name VoltraReducers\n\n",
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

pub(crate) fn write_generated(out: &Path, filename: &str, content: &str) -> Result<()> {
    let path = out.join(filename);
    std::fs::write(&path, content).map_err(|e| {
        voltra::error::VoltraError::internal(format!("Cannot write {}: {}", path.display(), e))
    })
}

pub(crate) async fn cmd_cluster_status(metrics_url: &str) -> Result<()> {
    let url = format!("{}/cluster/peers", metrics_url);
    let resp = reqwest::get(&url).await.map_err(|e| {
        voltra::error::VoltraError::network_error(format!("Cannot reach {}: {}", url, e))
    })?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eprintln!("Server returned HTTP {}: {}", status, body);
        return Err(voltra::error::VoltraError::network_error(format!("HTTP {}", status)));
    }

    let data: serde_json::Value = resp.json().await.map_err(|e| {
        voltra::error::VoltraError::internal(format!("Invalid JSON response: {}", e))
    })?;

    let my_shard    = data["my_shard_id"].as_u64().unwrap_or(0);
    let shard_count = data["shard_count"].as_u64().unwrap_or(1);
    let enabled     = data["cluster_enabled"].as_bool().unwrap_or(false);

    println!();
    if !enabled {
        println!("  Cluster: single-node mode");
        println!("  Shard:   {}/{}", my_shard, shard_count);
        println!();
        println!("  To enable clustering, set VOLTRA_PEERS before starting:");
        println!("    VOLTRA_PEERS=shard1=http://node2:3001,shard2=http://node3:3001");
        println!();
        println!("  Or dynamically join a running cluster:");
        println!("    VOLTRA_SEED_NODE=http://existing-node:3001 voltra start");
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
// voltra start — project-aware: if CWD is a scaffolded game project, build + run it
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) fn is_game_project(cwd: &Path) -> Option<String> {
    let cargo_path = cwd.join("Cargo.toml");
    if !cargo_path.exists() { return None; }
    let content = std::fs::read_to_string(&cargo_path).ok()?;
    // Must DEPEND on the `voltra` crate (dependency key exactly "voltra"),
    // not merely contain the substring — otherwise sibling crates like
    // voltra-console (which depend on wry/tao, not voltra) get misdetected.
    if content.contains("name = \"voltra\"") { return None; } // the engine itself
    let depends_on_voltra = content.lines().any(|l| {
        let t = l.trim_start();
        t.starts_with("voltra ") || t.starts_with("voltra=") || t.starts_with("voltra.")
    });
    if !depends_on_voltra { return None; }
    // Extract package name
    content.lines()
        .find(|l| l.trim_start().starts_with("name") && l.contains('"'))
        .and_then(|l| l.split('"').nth(1))
        .map(|s| s.to_string())
}

/// Print the Voltra wordmark at startup — colored on an interactive terminal,
/// skipped entirely when stdout is piped/redirected so logs stay clean.
pub(crate) fn print_banner() {
    use std::io::IsTerminal;
    if !std::io::stdout().is_terminal() {
        return;
    }
    #[cfg(windows)]
    let _ = console::Term::stdout(); // nudges Windows to enable ANSI/VT

    const ART: [&str; 6] = [
        "██╗   ██╗ ██████╗ ██╗     ████████╗██████╗  █████╗ ",
        "██║   ██║██╔═══██╗██║     ╚══██╔══╝██╔══██╗██╔══██╗",
        "██║   ██║██║   ██║██║        ██║   ██████╔╝███████║",
        "╚██╗ ██╔╝██║   ██║██║        ██║   ██╔══██╗██╔══██║",
        " ╚████╔╝ ╚██████╔╝███████╗   ██║   ██║  ██║██║  ██║",
        "  ╚═══╝   ╚═════╝ ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝",
    ];
    // cyan → blue → violet, one stop per row
    const COLORS: [(u8, u8, u8); 6] = [
        (34, 211, 238), (44, 178, 241), (54, 146, 244),
        (72, 116, 244), (98, 87, 241), (124, 58, 237),
    ];
    let color = std::env::var_os("NO_COLOR").is_none();

    println!();
    for (i, line) in ART.iter().enumerate() {
        if color {
            let (r, g, b) = COLORS[i];
            println!("   \x1b[1;38;2;{r};{g};{b}m{line}\x1b[0m");
        } else {
            println!("   {line}");
        }
    }
    let tag = format!("the in-memory database for games · v{}", env!("CARGO_PKG_VERSION"));
    if color {
        println!("   \x1b[38;2;138;147;166m{tag}\x1b[0m\n");
    } else {
        println!("   {tag}\n");
    }
}

pub(crate) fn cmd_start_project(cwd: &Path, pkg_name: &str) -> Result<()> {
    println!("[voltra] Building {} (release)…", pkg_name);
    let build = std::process::Command::new("cargo")
        .arg("build")
        .arg("--release")
        .current_dir(cwd)
        .status()
        .map_err(|e| voltra::error::VoltraError::internal(format!("cargo build: {e}")))?;

    if !build.success() {
        return Err(voltra::error::VoltraError::internal("cargo build --release failed"));
    }

    let bin_name = if cfg!(windows) {
        format!("{pkg_name}.exe")
    } else {
        pkg_name.to_string()
    };
    let bin = cwd.join("target").join("release").join(&bin_name);
    if !bin.exists() {
        return Err(voltra::error::VoltraError::internal(
            format!("Binary not found at {}", bin.display()),
        ));
    }

    println!("[voltra] Starting {}…", pkg_name);
    let status = std::process::Command::new(&bin)
        .arg("start")
        .current_dir(cwd)
        .status()
        .map_err(|e| voltra::error::VoltraError::internal(format!("exec {pkg_name}: {e}")))?;

    if status.success() {
        Ok(())
    } else {
        Err(voltra::error::VoltraError::internal(format!("{pkg_name} exited with non-zero status")))
    }
}

// voltra templates
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) fn cmd_list_templates() {
    println!();
    println!("  Voltra Game Templates");
    println!();
    for t in TEMPLATES {
        println!("  {:14} — {}", t.name, t.description);
    }
    println!();
    println!("  Usage:");
    println!("    voltra init my-game --template game/basic");
    println!("    voltra init my-game --template game/full");
    println!("    voltra init my-game --template game/unity");
    println!("    voltra init my-game --template game/godot");
    println!();
    println!("  Add modules later:");
    println!("    cd my-game && voltra add combat");
    println!("    cd my-game && voltra add leaderboard");
    println!();
}

pub(crate) fn cmd_list_modules() {
    println!();
    println!("  Voltra Add-on Modules  (run inside your project: voltra add <module>)");
    println!();
    for (name, desc) in MODULES {
        println!("  {:14} — {}", name, desc);
    }
    println!();
    println!("  Example:");
    println!("    cd my-game");
    println!("    voltra add combat       # adds attack, respawn, ability reducers + schema");
    println!("    voltra add leaderboard  # adds lb_submit, lb_reset reducers + schema");
    println!();
}
#[cfg(test)]
mod start_detection_tests {
    use super::is_game_project;
    use std::fs;

    fn proj(label: &str, cargo: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("voltra_isgame_{}_{}", label, std::process::id()));
        let _ = fs::create_dir_all(&dir);
        fs::write(dir.join("Cargo.toml"), cargo).unwrap();
        dir
    }

    #[test]
    fn console_crate_is_not_a_game_project() {
        // Regression: "voltra-console" contains "voltra" but does not depend on it.
        let d = proj("console", "[package]\nname = \"voltra-console\"\n\n[dependencies]\nwry = \"0.46\"\ntao = \"0.30\"\n");
        assert_eq!(is_game_project(&d), None);
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn game_with_voltra_dep_is_detected() {
        let d = proj("game", "[package]\nname = \"mygame\"\n\n[dependencies]\nvoltra = { path = \"../voltra\" }\n");
        assert_eq!(is_game_project(&d).as_deref(), Some("mygame"));
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn engine_itself_is_not_a_game_project() {
        let d = proj("engine", "[package]\nname = \"voltra\"\nversion = \"2.0.3\"\n");
        assert_eq!(is_game_project(&d), None);
        let _ = fs::remove_dir_all(&d);
    }
}
