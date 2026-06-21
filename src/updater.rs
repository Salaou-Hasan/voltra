use std::fs;
use std::io::Write;
use std::path::PathBuf;

const RELEASES_REPO: &str = "Salaou-Hasan/voltra-releases";
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
        .set("User-Agent", &format!("voltra/v{CURRENT_VERSION}"))
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
        .set("User-Agent", &format!("voltra/v{CURRENT_VERSION}"))
        .call()
        .map_err(|e| crate::error::VoltraError::internal(format!("download {asset}: {e}")))?;

    let mut bytes = Vec::new();
    resp.into_reader()
        .read_to_end(&mut bytes)
        .map_err(|e| crate::error::VoltraError::internal(format!("read {asset}: {e}")))?;

    let mut f = fs::File::create(&tmp)
        .map_err(|e| crate::error::VoltraError::internal(format!("create tmp: {e}")))?;
    f.write_all(&bytes)
        .map_err(|e| crate::error::VoltraError::internal(format!("write tmp: {e}")))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o755))
            .map_err(|e| crate::error::VoltraError::internal(format!("chmod: {e}")))?;
    }

    // Windows self-update: the running exe cannot be overwritten directly.
    // Three-tier fallback so it always works regardless of Windows version / AV.
    #[cfg(windows)]
    windows_replace(&tmp, &dest)?;
    #[cfg(not(windows))]
    fs::rename(&tmp, &dest)
        .map_err(|e| crate::error::VoltraError::internal(format!("replace {}: {e}", dest.display())))?;

    println!("    ✓ {}", dest.display());
    Ok(())
}

/// Windows-only: three-tier strategy to replace a potentially-running exe.
///
/// Tier 1 — rename-swap (works ~99% of the time):
///   voltra.exe  →  voltra.old.exe   (rename is allowed on running exes)
///   voltra.tmp  →  voltra.exe       (destination is now free)
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
    let batch = std::env::temp_dir().join("_voltra_update.cmd");
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
            println!("      Run `voltra --version` in a new terminal to confirm.");
            // Exit immediately so Windows releases the exe lock.
            std::process::exit(0);
        }
    }

    // ── Tier 3: manual instructions ──────────────────────────────────────────
    Err(crate::error::VoltraError::internal(format!(
        "Cannot replace the running binary automatically.\n\
         Manual update:\n  \
           1. Close all voltra processes\n  \
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
        println!("  Run `voltra update` to install.");
        return Ok(());
    }

    println!("Installing v{tag} …");
    match download_and_replace("voltra", &tag) {
        Ok(()) => println!("\n  Done. Restart any running servers to pick up the new version."),
        Err(e) => eprintln!("  ✗ voltra: {e}"),
    }

    Ok(())
}

pub fn check_and_hint() {
    if let Some(tag) = latest_tag() {
        if version_newer(&tag) {
            eprintln!("[voltra] Update available: v{CURRENT_VERSION} → v{tag}  (run `voltra update`)");
        }
    }
}

/// Per-user folder we install the binary into (and put on PATH).
fn home_install_dir() -> PathBuf {
    #[cfg(windows)]
    let d = std::env::var_os("USERPROFILE").map(|h| PathBuf::from(h).join(".voltra"));
    #[cfg(not(windows))]
    let d = std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local").join("bin"));
    d.unwrap_or_else(|| PathBuf::from(".voltra"))
}

/// Copy the running binary to a stable location and add it to PATH.
pub fn cmd_install() -> crate::error::Result<()> {
    use crate::error::VoltraError;
    let exe = std::env::current_exe()
        .map_err(|e| VoltraError::internal(format!("current exe: {e}")))?;
    let dir = home_install_dir();
    fs::create_dir_all(&dir)
        .map_err(|e| VoltraError::internal(format!("create {}: {e}", dir.display())))?;
    let dest = dir.join(if cfg!(windows) { "voltra.exe" } else { "voltra" });

    if exe != dest {
        fs::copy(&exe, &dest)
            .map_err(|e| VoltraError::internal(format!("copy to {}: {e}", dest.display())))?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&dest, fs::Permissions::from_mode(0o755));
    }
    println!("  Installed: {}", dest.display());
    add_to_path(&dir);
    Ok(())
}

#[cfg(windows)]
fn add_to_path(dir: &std::path::Path) {
    // Edit the user PATH via PowerShell (idempotent — only appends if absent).
    let d = dir.to_string_lossy().replace('\'', "''");
    let script = format!(
        "$d='{d}'; $p=[Environment]::GetEnvironmentVariable('Path','User'); if ($null -eq $p) {{ $p='' }}; \
         if (($p -split ';') -notcontains $d) {{ [Environment]::SetEnvironmentVariable('Path', ($p.TrimEnd(';') + ';' + $d), 'User'); 'added' }} else {{ 'present' }}"
    );
    match std::process::Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .output()
    {
        Ok(o) if String::from_utf8_lossy(&o.stdout).trim() == "added" => {
            println!("  Added to PATH. Open a NEW terminal, then run: voltra -h");
        }
        Ok(_) => println!("  Already on PATH. Run: voltra -h"),
        Err(e) => println!("  Couldn't edit PATH ({e}). Add this folder yourself: {}", dir.display()),
    }
}

#[cfg(not(windows))]
fn add_to_path(dir: &std::path::Path) {
    let on_path = std::env::var("PATH")
        .map(|p| p.split(':').any(|s| std::path::Path::new(s) == dir))
        .unwrap_or(false);
    if on_path {
        println!("  {} is on your PATH. Run: voltra -h", dir.display());
    } else {
        println!("  Add to PATH — append to your shell rc (~/.bashrc or ~/.zshrc):");
        println!("    export PATH=\"$PATH:{}\"", dir.display());
    }
}

#[cfg(test)]
mod install_tests {
    use super::home_install_dir;
    #[test]
    fn install_dir_has_expected_suffix() {
        let s = home_install_dir().to_string_lossy().to_string();
        #[cfg(windows)]
        assert!(s.ends_with(".voltra"), "got {s}");
        #[cfg(not(windows))]
        assert!(s.ends_with("bin"), "got {s}");
    }
}
