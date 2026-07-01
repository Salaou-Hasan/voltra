use std::fs;
use std::io::Write;
use std::path::PathBuf;

const RELEASES_REPO: &str = "Salaou-Hasan/voltra-releases";
const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Pure so it can be unit-tested for all 4 platforms regardless of the host
/// running the test — this exact string must match `release.yml`'s
/// `asset_name` for each build matrix entry, or `voltra update` 404s.
fn asset_name_for(bin: &str, os: &str, arch: &str) -> String {
    let ext = if os == "windows" { ".exe" } else { "" };
    if os == "macos" && arch == "aarch64" {
        format!("{bin}-macos-aarch64{ext}")
    } else if os == "macos" {
        format!("{bin}-macos-x86_64{ext}")
    } else if os == "linux" {
        format!("{bin}-linux-x86_64{ext}")
    } else {
        format!("{bin}-windows-x86_64{ext}")
    }
}

fn asset_name(bin: &str) -> String {
    asset_name_for(bin, std::env::consts::OS, std::env::consts::ARCH)
}

/// Pure so it can be unit-tested without a network call. `tag` must be used
/// verbatim (as returned by `latest_tag()`) — it already carries whatever
/// prefix letter the release actually uses ("g1.2.0.0", "v2.0.5", ...).
fn release_download_url(tag: &str, asset: &str) -> String {
    format!("https://github.com/{RELEASES_REPO}/releases/download/{tag}/{asset}")
}

fn install_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Returns the latest release tag from the releases repo verbatim (e.g.
/// "g1.2.0.0" or "v2.0.5"), or None on error. Must NOT strip the leading
/// letter — `download_and_replace` needs the exact tag to build a working
/// GitHub release download URL, and `version_newer` parses either prefix
/// itself.
fn latest_tag() -> Option<String> {
    let url = format!("https://api.github.com/repos/{RELEASES_REPO}/releases/latest");
    let resp = ureq::get(&url)
        .set("User-Agent", &format!("voltra/v{CURRENT_VERSION}"))
        .call()
        .ok()?;
    let json: serde_json::Value = resp.into_json().ok()?;
    json["tag_name"].as_str().map(|s| s.to_string())
}

/// Compare a release tag against the running build, generation-aware.
///
/// A tag is `g<gen>.<major>.<minor>.<patch>` (this generation's scheme) or a
/// legacy `v<major>.<minor>.<patch>` / bare `<major>.<minor>.<patch>` (treated
/// as generation 0). Generation is the most-significant component, so any
/// release in a newer generation supersedes an older-generation build even if
/// its numeric version is lower (the 1.0.0 reset at the start of a generation
/// must still update users on the previous line).
fn version_newer(latest: &str) -> bool {
    fn parse(v: &str) -> (u64, u64, u64, u64) {
        let v = v.trim();
        if let Some(rest) = v.strip_prefix('g') {
            let p: Vec<u64> = rest.split('.').filter_map(|x| x.parse().ok()).collect();
            (
                p.first().copied().unwrap_or(0),
                p.get(1).copied().unwrap_or(0),
                p.get(2).copied().unwrap_or(0),
                p.get(3).copied().unwrap_or(0),
            )
        } else {
            let s = v.strip_prefix('v').unwrap_or(v);
            let p: Vec<u64> = s.split('.').filter_map(|x| x.parse().ok()).collect();
            (
                0,
                p.first().copied().unwrap_or(0),
                p.get(1).copied().unwrap_or(0),
                p.get(2).copied().unwrap_or(0),
            )
        }
    }
    let cur: Vec<u64> = CURRENT_VERSION
        .split('.')
        .filter_map(|p| p.parse().ok())
        .collect();
    let current = (
        crate::GENERATION as u64,
        cur.first().copied().unwrap_or(0),
        cur.get(1).copied().unwrap_or(0),
        cur.get(2).copied().unwrap_or(0),
    );
    parse(latest) > current
}

fn download_and_replace(bin: &str, tag: &str) -> crate::error::Result<()> {
    let asset = asset_name(bin);
    let url = release_download_url(tag, &asset);
    let dest = install_dir().join(if cfg!(windows) {
        format!("{bin}.exe")
    } else {
        bin.to_string()
    });
    let tmp = dest.with_extension("tmp");

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
    fs::rename(&tmp, &dest).map_err(|e| {
        crate::error::VoltraError::internal(format!("replace {}: {e}", dest.display()))
    })?;

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
            fs::rename(dest, &old)?; // rename running exe (allowed by Windows)
        }
        fs::rename(tmp, dest)?; // place new binary
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
    let tmp_w = tmp.to_string_lossy().replace('/', "\\");
    let dest_w = dest.to_string_lossy().replace('/', "\\");
    let bat_w = batch.to_string_lossy().replace('/', "\\");
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

    println!("v{CURRENT_VERSION} → {tag} available!");

    if check_only {
        println!("  Run `voltra update` to install.");
        return Ok(());
    }

    println!("Installing {tag} …");
    match download_and_replace("voltra", &tag) {
        Ok(()) => println!("\n  Done. Restart any running servers to pick up the new version."),
        Err(e) => eprintln!("  ✗ voltra: {e}"),
    }

    Ok(())
}

pub fn check_and_hint() {
    if let Some(tag) = latest_tag() {
        if version_newer(&tag) {
            eprintln!(
                "[voltra] Update available: v{CURRENT_VERSION} → {tag}  (run `voltra update`)"
            );
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
    let exe =
        std::env::current_exe().map_err(|e| VoltraError::internal(format!("current exe: {e}")))?;
    let dir = home_install_dir();
    fs::create_dir_all(&dir)
        .map_err(|e| VoltraError::internal(format!("create {}: {e}", dir.display())))?;
    let dest = dir.join(if cfg!(windows) {
        "voltra.exe"
    } else {
        "voltra"
    });

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
        Err(e) => println!(
            "  Couldn't edit PATH ({e}). Add this folder yourself: {}",
            dir.display()
        ),
    }
}

/// Pick the shell rc file to edit, based on $SHELL — mirrors what a user's
/// interactive shell actually sources, so the PATH change takes effect in a
/// new terminal without guessing wrong and editing a file nobody reads.
#[cfg(not(windows))]
fn shell_rc_path() -> Option<PathBuf> {
    let home = std::env::var_os("HOME").map(PathBuf::from)?;
    let shell = std::env::var("SHELL").unwrap_or_default();
    let file = if shell.contains("zsh") {
        ".zshrc"
    } else if shell.contains("bash") {
        ".bashrc"
    } else {
        ".profile"
    };
    Some(home.join(file))
}

#[cfg(not(windows))]
fn add_to_path(dir: &std::path::Path) {
    let on_path = std::env::var("PATH")
        .map(|p| p.split(':').any(|s| std::path::Path::new(s) == dir))
        .unwrap_or(false);
    if on_path {
        println!("  {} is on your PATH. Run: voltra -h", dir.display());
        return;
    }

    let export_line = format!("export PATH=\"$PATH:{}\"", dir.display());
    match shell_rc_path() {
        Some(rc) => {
            let existing = fs::read_to_string(&rc).unwrap_or_default();
            if existing.contains(&export_line) {
                println!(
                    "  Already in {} — open a NEW terminal, then run: voltra -h",
                    rc.display()
                );
                return;
            }
            let mut new_content = existing;
            if !new_content.is_empty() && !new_content.ends_with('\n') {
                new_content.push('\n');
            }
            new_content.push_str("# Added by `voltra install`\n");
            new_content.push_str(&export_line);
            new_content.push('\n');
            match fs::write(&rc, new_content) {
                Ok(()) => println!(
                    "  Added to PATH via {}. Open a NEW terminal, then run: voltra -h",
                    rc.display()
                ),
                Err(e) => {
                    println!(
                        "  Couldn't edit {} ({e}). Add this line yourself:",
                        rc.display()
                    );
                    println!("    {export_line}");
                }
            }
        }
        None => {
            println!("  Couldn't determine your shell config file. Add this line yourself:");
            println!("    {export_line}");
        }
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

#[cfg(test)]
mod update_tests {
    use super::{asset_name_for, release_download_url, version_newer};

    // Regression test: these exact strings must match release.yml's
    // per-target `asset_name` values, or `voltra update` 404s on that
    // platform. macOS aarch64 previously said "arm64" instead of "aarch64".
    #[test]
    fn asset_name_matches_release_workflow_matrix() {
        assert_eq!(
            asset_name_for("voltra", "macos", "aarch64"),
            "voltra-macos-aarch64"
        );
        assert_eq!(
            asset_name_for("voltra", "macos", "x86_64"),
            "voltra-macos-x86_64"
        );
        assert_eq!(
            asset_name_for("voltra", "linux", "x86_64"),
            "voltra-linux-x86_64"
        );
        assert_eq!(
            asset_name_for("voltra", "windows", "x86_64"),
            "voltra-windows-x86_64.exe"
        );
    }

    // Regression test: the tag must be used verbatim in the download URL.
    // Previously `latest_tag()` stripped a leading 'v' and this function
    // unconditionally re-added one, so any "g1.x.x.x" tag (the current
    // generation scheme) built a URL for a release that doesn't exist.
    #[test]
    fn download_url_uses_tag_verbatim_for_generation_scheme() {
        let url = release_download_url("g1.2.0.0", "voltra-linux-x86_64");
        assert_eq!(
            url,
            "https://github.com/Salaou-Hasan/voltra-releases/releases/download/g1.2.0.0/voltra-linux-x86_64"
        );
        assert!(
            !url.contains("/vg1.2.0.0/"),
            "must not double-prefix: {url}"
        );
    }

    #[test]
    fn download_url_uses_tag_verbatim_for_legacy_scheme() {
        let url = release_download_url("v2.0.5", "voltra-windows-x86_64.exe");
        assert_eq!(
            url,
            "https://github.com/Salaou-Hasan/voltra-releases/releases/download/v2.0.5/voltra-windows-x86_64.exe"
        );
    }

    #[test]
    fn version_newer_detects_generation_bump_over_legacy_tag() {
        // A g2.x.x.x release must always beat a g1.x.x.x build, even with a
        // "lower" numeric version, since generation is the most-significant
        // component (this is what let the 1.0.0 reset at the start of Gen 1
        // still update everyone on the old v2.0.x line).
        assert!(version_newer("g2.0.0.0"));
    }

    #[test]
    fn version_newer_rejects_older_patch_in_same_generation() {
        // Cargo.toml's CURRENT_VERSION ("1.0.0") + GENERATION (1) encodes to
        // the same 4-tuple as tag "g1.1.0.0" — so a lower g1.x.x.x tag must
        // NOT be reported as newer. This is the exact case the original bug
        // report would have masked, since it never got exercised by a test.
        assert!(!version_newer("g1.0.0.0"));
    }

    #[test]
    fn version_newer_compares_within_same_generation() {
        assert!(version_newer("g1.99.0.0"));
    }
}
