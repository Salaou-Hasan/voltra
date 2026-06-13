use std::fs;
use std::io::Write;
use std::path::PathBuf;

const RELEASES_REPO: &str = "Salaou-Hasan/neondb-releases";
const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// All managed binaries — updated together when a new release is available.
const MANAGED_BINS: &[&str] = &["neondb", "neondb-sim", "neondb-bench", "neondb-soak"];

fn asset_name(bin: &str) -> String {
    let target = std::env::consts::OS;
    let arch   = std::env::consts::ARCH;
    let ext    = if cfg!(windows) { ".exe" } else { "" };
    // e.g. neondb-x86_64-windows.exe
    format!("{bin}-{arch}-{target}{ext}")
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
        .set("User-Agent", &format!("neondb/{CURRENT_VERSION}"))
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

/// Download a single binary from the release and atomically replace it.
fn download_and_replace(bin: &str, tag: &str) -> crate::error::Result<()> {
    let asset  = asset_name(bin);
    let url    = format!("https://github.com/{RELEASES_REPO}/releases/download/v{tag}/{asset}");
    let dest   = install_dir().join(if cfg!(windows) { format!("{bin}.exe") } else { bin.to_string() });
    let tmp    = dest.with_extension("tmp");

    println!("  Downloading {asset} …");

    let resp = ureq::get(&url)
        .set("User-Agent", &format!("neondb/{CURRENT_VERSION}"))
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

    // Make executable on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o755))
            .map_err(|e| crate::error::NeonDBError::internal(format!("chmod: {e}")))?;
    }

    // Atomic replace: on Windows rename over existing file works
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
    let dir = install_dir();
    let mut ok = 0usize;
    let mut skipped = 0usize;

    for &bin in MANAGED_BINS {
        let bin_path = dir.join(if cfg!(windows) { format!("{bin}.exe") } else { bin.to_string() });
        if !bin_path.exists() {
            println!("  Skipping {bin} (not found in {})", dir.display());
            skipped += 1;
            continue;
        }
        match download_and_replace(bin, &tag) {
            Ok(()) => ok += 1,
            Err(e) => eprintln!("  ✗ {bin}: {e}"),
        }
    }

    println!();
    println!("  Updated {ok}/{} binaries.", ok + skipped);
    if ok > 0 {
        println!("  Restart any running servers to pick up the new version.");
    }
    Ok(())
}

pub fn check_and_hint() {
    // Lightweight background version hint — only prints one line, never blocks startup.
    if let Some(tag) = latest_tag() {
        if version_newer(&tag) {
            eprintln!("[neondb] Update available: v{CURRENT_VERSION} → v{tag}  (run `neondb update`)");
        }
    }
}
