use std::process::Command;

/// Embed the exact git tag/commit this binary was built from into
/// `VOLTRA_BUILD_TAG`, so `voltra -V` can show it. Without this, the version
/// string was built purely from Cargo.toml's `version` field, which stays
/// "1.0.0" for an entire generation by design (see VERSIONING.md) — every
/// g1.x.y.z release printed the identical string, making it impossible to
/// tell which release was actually installed after `voltra update`.
fn main() {
    let tag = Command::new("git")
        .args(["describe", "--tags", "--always", "--dirty"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("v{}", env!("CARGO_PKG_VERSION")));

    println!("cargo:rustc-env=VOLTRA_BUILD_TAG={tag}");
    // Only re-run when the current ref actually changes, not on every build.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs");
}
