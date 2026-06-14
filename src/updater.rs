use std::fs;
use std::io::Write;
use std::path::PathBuf;

const RAW_BASE: &str = "https://raw.githubusercontent.com/Salaou-Hasan/neondb-releases/main";
const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

fn platform_dir() -> &'static str {
    if cfg!(target_os = "windows") { "windows" }
    else if cfg!(target_os = "macos") { "macos" }
    else { "linux" }
}

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

/// Reads version.txt from the releases repo — single line like "1.0.12"
fn latest_version() -> Option<String> {
    let url = format!("{RAW_BASE}/version.txt");
    let resp = ureq::get(&url)
        .set("User-Agent", &format!("neondb/v{CURRENT_VERSION}"))
        .call()
        .ok()?;
    let text = resp.into_string().ok()?;
    let v = text.trim().trim_start_matches('v').to_string();
    if v.is_empty() { None } else { Some(v) }
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

fn download_and_replace(bin: &str) -> crate::error::Result<()> {
    let asset = asset_name(bin);
    let dir   = platform_dir();
    let url   = format!("{RAW_BASE}/{dir}/{asset}");
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

    fs::rename(&tmp, &dest)
        .map_err(|e| crate::error::NeonDBError::internal(format!("replace {}: {e}", dest.display())))?;

    println!("    ✓ {}", dest.display());
    Ok(())
}

pub fn cmd_update(check_only: bool) -> crate::error::Result<()> {
    print!("Checking for updates … ");
    let _ = std::io::stdout().flush();

    let latest = match latest_version() {
        Some(v) => v,
        None => {
            println!("could not reach GitHub — check your connection.");
            return Ok(());
        }
    };

    if !version_newer(&latest) {
        println!("already up to date (v{CURRENT_VERSION}).");
        return Ok(());
    }

    println!("v{CURRENT_VERSION} → v{latest} available!");

    if check_only {
        println!("  Run `neondb update` to install.");
        return Ok(());
    }

    println!("Installing v{latest} …");

    match download_and_replace("neondb") {
        Ok(()) => println!("\n  Done. Restart any running servers to pick up the new version."),
        Err(e) => eprintln!("  ✗ neondb: {e}"),
    }

    Ok(())
}

pub fn check_and_hint() {
    if let Some(v) = latest_version() {
        if version_newer(&v) {
            eprintln!("[neondb] Update available: v{CURRENT_VERSION} → v{v}  (run `neondb update`)");
        }
    }
}
