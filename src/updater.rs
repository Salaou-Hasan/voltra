use std::fs;
use std::io::Write;
use std::path::PathBuf;

const RELEASES_REPO: &str = "Salaou-Hasan/neondb-releases";
const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

fn asset_name(bin: &str) -> String {
    let ext = if cfg!(windows) { ".exe" } else { "" };
    if cfg!(target_os = "macos") && cfg!(target_arch = "aarch64") {
        format!("{bin}-macos-arm64{ext}")
    } else if cfg!(target_os = "macos") {
        format!("{bin}-macos-x86_64{ext}")
    } else if cfg!(target_os = "linux") {
        format!("{bin}-linux-x86_64{ext}")
    } else {
        format!("{bin}-windows-x86_64{ext}")
    }
}

fn install_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Returns the latest release tag from the releases repo, or None on error.
fn latest_tag() -> Option<String> {
    let url = format!("https://api.github.com/repos/{RELEASES_REPO}/releases/latest");
    let resp = ureq::get(&url)
        .set("User-Agent", &format!("neondb/v{CURRENT_VERSION}"))
        .call()
        .ok()?;
    let json: serde_json::Value = resp.into_json().ok()?;
    json["tag_name"].as_str().map(|s| s.trim_start_matches('v').to_string())
}

fn version_newer(latest: &str) -> bool {
    fn parse(v: &str) -> (u64, u64, u64) {
        let parts: Vec<u64> = v.split('.').filter_map(|p| p.parse().ok()).collect();
        (parts.first().copied().unwrap_or(0),
         parts.get(1).copied().unwrap_or(0),
         parts.get(2).copied().unwrap_or(0))
    }
    parse(latest) > parse(CURRENT_VERSION)
}

fn download_and_replace(bin: &str, tag: &str) -> crate::error::Result<()> {
    let asset = asset_name(bin);
    let url   = format!("https://github.com/{RELEASES_REPO}/releases/download/v{tag}/{asset}");
    let dest  = install_dir().join(if cfg!(windows) { format!("{bin}.exe") } else { bin.to_string() });
    let tmp   = dest.with_extension("tmp");

    println!("  Downloading {asset} …");

    let resp = ureq::get(&url)
        .set("User-Agent", &format!("neondb/v{CURRENT_VERSION}"))
        .call()
        .map_err(|e| crate::error::NeonDBError::internal(format!("download {asset}: {e}")))?;

    let mut bytes = Vec::new();
    resp.into_reader()
        .read_to_end(&mut bytes)
        .map_err(|e| crate::error::NeonDBError::internal(format!("read {asset}: {e}")))?;

    let mut f = fs::File::create(&tmp)
        .map_err(|e| crate::error::NeonDBError::internal(format!("create tmp: {e}")))?;
    f.write_all(&bytes)
        .map_err(|e| crate::error::NeonDBError::internal(format!("write tmp: {e}")))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o755))
            .map_err(|e| crate::error::NeonDBError::internal(format!("chmod: {e}")))?;
    }

    // Windows self-update: the running exe cannot be overwritten directly.
    // Three-tier fallback so it always works regardless of Windows version / AV.
    #[cfg(windows)]
    windows_replace(&tmp, &dest)?;
    #[cfg(not(windows))]
    fs::rename(&tmp, &dest)
        .map_err(|e| crate::error::NeonDBError::internal(format!("replace {}: {e}", dest.display())))?;

    println!("    ✓ {}", dest.display());
    Ok(())
}

/// Windows-only: three-tier strategy to replace a potentially-running exe.
///
/// Tier 1 — rename-swap (works ~99% of the time):
///   neondb.exe  →  neondb.old.exe   (rename is allowed on running exes)
///   neondb.tmp  →  neondb.exe       (destination is now free)
///
/// Tier 2 — batch-script deferred copy (nuclear option):
///   Write a .cmd file to %TEMP% that sleeps 2 s then does `copy /y`.
///   Launch it detached, then exit this process so the lock is released.
///   The batch script completes the copy and deletes itself.
///
/// Tier 3 — tell the user to copy manually.
#[cfg(windows)]
fn windows_replace(tmp: &std::path::Path, dest: &std::path::Path) -> crate::error::Result<()> {
    // ── Tier 1: rename-swap ───────────────────────────────────────────────────
    let old = dest.with_extension("old.exe");
    let _ = fs::remove_file(&old); // clear stale .old from a previous run
    let tier1 = (|| -> std::io::Result<()> {
        if dest.exists() {
            fs::rename(dest, &old)?;   // rename running exe (allowed by Windows)
        }
        fs::rename(tmp, dest)?;        // place new binary
        let _ = fs::remove_file(&old);
        Ok(())
    })();
    if tier1.is_ok() {
        return Ok(());
    }
    // Restore if the second rename failed
    if old.exists() && !dest.exists() {
        let _ = fs::rename(&old, dest);
    }

    // ── Tier 2: deferred batch-script copy ───────────────────────────────────
    // Write a .cmd that waits 2 s (for this process to exit), copies the new
    // binary over the old one, then deletes itself.
    let batch = std::env::temp_dir().join("_neondb_update.cmd");
    let tmp_w  = tmp.to_string_lossy().replace('/', "\\");
    let dest_w = dest.to_string_lossy().replace('/', "\\");
    let bat_w  = batch.to_string_lossy().replace('/', "\\");
    let script = format!(
        "@echo off\r\n\
         timeout /t 2 /nobreak >nul\r\n\
         copy /y \"{tmp_w}\" \"{dest_w}\" >nul\r\n\
         del \"{tmp_w}\" >nul 2>&1\r\n\
         del \"{bat_w}\"\r\n"
    );
    if fs::write(&batch, script.as_bytes()).is_ok() {
        let launched = std::process::Command::new("cmd")
            .args(["/c", "start", "", "/min", "cmd", "/c", bat_w.as_ref()])
            .spawn();
        if launched.is_ok() {
            println!("    ✓ Update scheduled — completing in 2 s (background).");
            println!("      Run `neondb --version` in a new terminal to confirm.");
            // Exit immediately so Windows releases the exe lock.
            std::process::exit(0);
        }
    }

    // ── Tier 3: manual instructions ──────────────────────────────────────────
    Err(crate::error::NeonDBError::internal(format!(
        "Cannot replace the running binary automatically.\n\
         Manual update:\n  \
           1. Close all neondb processes\n  \
           2. Run: copy /y \"{}\" \"{}\"\n  \
           3. Then: del \"{}\"",
        tmp.display(),
        dest.display(),
        tmp.display(),
    )))
}

pub fn cmd_update(check_only: bool) -> crate::error::Result<()> {
    print!("Checking for updates … ");
    let _ = std::io::stdout().flush();

    let tag = match latest_tag() {
        Some(t) => t,
        None => {
            println!("could not reach GitHub — check your connection.");
            return Ok(());
        }
    };

    if !version_newer(&tag) {
        println!("already up to date (v{CURRENT_VERSION}).");
        return Ok(());
    }

    println!("v{CURRENT_VERSION} → v{tag} available!");

    if check_only {
        println!("  Run `neondb update` to install.");
        return Ok(());
    }

    println!("Installing v{tag} …");
    match download_and_replace("neondb", &tag) {
        Ok(()) => println!("\n  Done. Restart any running servers to pick up the new version."),
        Err(e) => eprintln!("  ✗ neondb: {e}"),
    }

    Ok(())
}

pub fn check_and_hint() {
    if let Some(tag) = latest_tag() {
        if version_newer(&tag) {
            eprintln!("[neondb] Update available: v{CURRENT_VERSION} → v{tag}  (run `neondb update`)");
        }
    }
}
