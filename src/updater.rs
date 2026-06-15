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

    // On Windows, rename() fails with "Access is denied" when the destination
    // exe is currently in use (i.e. this very process).  Work around it by
    // renaming the old binary out of the way first, then placing the new one.
    #[cfg(windows)]
    {
        let old = dest.with_extension("old.exe");
        let _ = fs::remove_file(&old); // remove stale .old from a previous update
        if dest.exists() {
            fs::rename(&dest, &old)
                .map_err(|e| crate::error::NeonDBError::internal(format!("rename old binary: {e}")))?;
        }
        fs::rename(&tmp, &dest)
            .map_err(|e| crate::error::NeonDBError::internal(format!("replace {}: {e}", dest.display())))?;
        let _ = fs::remove_file(&old); // best-effort; may still be locked until process exits
    }
    #[cfg(not(windows))]
    fs::rename(&tmp, &dest)
        .map_err(|e| crate::error::NeonDBError::internal(format!("replace {}: {e}", dest.display())))?;

    println!("    ✓ {}", dest.display());
    Ok(())
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
