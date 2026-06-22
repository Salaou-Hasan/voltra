// `voltra build` pipeline: compile `.vol` (Voltra language), C#, and Go
// reducers, then JS→WASM (javy) and WASM→AOT (Cranelift). Includes the cargo
// preflight check + friendly error for the native-Rust reducer template.

use std::path::{Path, PathBuf};

use voltra::error::Result;

// ═══════════════════════════════════════════════════════════════════════════════
// Voltra Language templates — native reducer build
// ═══════════════════════════════════════════════════════════════════════════════

/// Compile reducers.vol → src/reducers.rs, then run cargo build --release.
pub(crate) fn build_voltra_reducers(project_dir: &std::path::Path) -> Result<()> {
    // Prefer reducers/ directory (new per-file layout); fall back to reducers.vol.
    let reducers_dir = project_dir.join("reducers");
    let reducers_voltra = project_dir.join("reducers.vol");

    let (combined, display) = if reducers_dir.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(&reducers_dir)
            .map_err(|e| {
                voltra::error::VoltraError::internal(format!("Cannot read reducers/: {e}"))
            })?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "vol").unwrap_or(false))
            .collect();
        entries.sort_by_key(|e| e.file_name());
        if entries.is_empty() {
            return Err(voltra::error::VoltraError::internal(
                "reducers/ exists but contains no .vol files",
            ));
        }
        let mut src = String::new();
        for e in &entries {
            src.push_str(&std::fs::read_to_string(e.path()).map_err(|err| {
                voltra::error::VoltraError::internal(format!(
                    "Cannot read {}: {err}",
                    e.path().display()
                ))
            })?);
            src.push('\n');
        }
        (src, format!("reducers/ ({} files)", entries.len()))
    } else if reducers_voltra.exists() {
        let src = std::fs::read_to_string(&reducers_voltra).map_err(|e| {
            voltra::error::VoltraError::internal(format!("Cannot read reducers.vol: {e}"))
        })?;
        (src, "reducers.vol".to_string())
    } else {
        return Err(voltra::error::VoltraError::internal(
            "No reducers/ directory or reducers.vol found. Run `voltra init` to create a project.",
        ));
    };

    println!("  Compiling {}...", display);
    let rust_code = voltra::dsl::compile(&combined, "reducers").map_err(|errors| {
        for e in &errors {
            eprintln!("  error: {}", e);
        }
        voltra::error::VoltraError::internal("Voltra compilation failed")
    })?;

    let out_path = project_dir.join("src").join("reducers.rs");
    std::fs::create_dir_all(out_path.parent().unwrap())
        .map_err(|e| voltra::error::VoltraError::internal(format!("Cannot create src/: {e}")))?;
    std::fs::write(&out_path, &rust_code).map_err(|e| {
        voltra::error::VoltraError::internal(format!("Cannot write src/reducers.rs: {e}"))
    })?;
    println!("  {} → src/reducers.rs", display);

    // Preflight: the native template compiles real Rust, so it needs a Rust
    // toolchain (cargo) and a working linker. Fail with one clear message
    // instead of a wall of cargo errors.
    let cargo_ok = std::process::Command::new("cargo")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !cargo_ok {
        return Err(voltra::error::VoltraError::internal(
            "this template compiles native Rust reducers, which needs the Rust toolchain.\n\
             \n  Install it (2 min): https://rustup.rs\n\
             \n  Or skip the compiler entirely: put .js reducers in modules/ and run `voltra start` —\
             \n  JS reducers run inside the engine with no build step."
        ));
    }

    let status = std::process::Command::new("cargo")
        .args(["build", "--release"])
        .current_dir(project_dir)
        .status()
        .map_err(|e| voltra::error::VoltraError::internal(format!("cargo build failed: {e}")))?;
    if !status.success() {
        eprintln!(
            "\n  Build failed. Most common cause: no linker for the default target.\n\
             {}\
             \n  Alternatively, skip compiling: put .js reducers in modules/ and run `voltra start`.",
            if cfg!(windows) {
                "\n  On Windows, either:\n\
                 \x20   • install the GNU toolchain (bundles its own linker, no Visual Studio):\n\
                 \x20       rustup toolchain install stable-x86_64-pc-windows-gnu\n\
                 \x20       rustup default stable-x86_64-pc-windows-gnu\n\
                 \x20   • or install \"Build Tools for Visual Studio\" with the C++ workload (provides link.exe).\n"
            } else {
                "\n  Install a C toolchain: Debian/Ubuntu `apt install build-essential`, \
                 Fedora `dnf install gcc`, macOS `xcode-select --install`.\n"
            }
        );
        return Err(voltra::error::VoltraError::internal(
            "cargo build --release failed",
        ));
    }
    println!("  Native binary ready.");
    Ok(())
}

/// Detect reducer language and invoke the appropriate compiler before the main
/// JS→WASM and AOT steps.
///
/// Priority (first match wins):
///   1. `reducers/*.csproj` → dotnet publish (C# → WASM via .NET 8 WASI)
///   2. `reducers/go.mod` + `*.go` → tinygo build (Go → WASM via TinyGo)
///
/// Both compilers output `.wasm` into `modules/`, which the remainder of
/// `build_wasm_modules` then AOT-compiles.
pub(crate) fn build_multi_lang_reducers(project_root: &Path, modules_dir: &Path) -> Result<()> {
    let reducers_dir = project_root.join("reducers");
    if !reducers_dir.is_dir() {
        return Ok(()); // no reducers/ directory — nothing to do
    }

    // ── C# detection ─────────────────────────────────────────────────────────
    let csproj = std::fs::read_dir(&reducers_dir).ok().and_then(|entries| {
        entries.flatten().find(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("csproj"))
                .unwrap_or(false)
        })
    });
    if let Some(csproj_entry) = csproj {
        let csproj_path = csproj_entry.path();
        println!("  C# project detected: {}", csproj_path.display());

        // Check that dotnet is available.
        let dotnet_ok = std::process::Command::new("dotnet")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !dotnet_ok {
            eprintln!(
                "  Warning: 'dotnet' not found on PATH. Skipping C# compilation.\n\
                 Install .NET 8 SDK: https://dotnet.microsoft.com/download\n\
                 Then install the WASI workload: dotnet workload install wasi-experimental"
            );
            return Ok(());
        }

        println!("  C# → WASM via dotnet publish (wasi-wasm) ...");
        let status = std::process::Command::new("dotnet")
            .arg("publish")
            .arg(&csproj_path)
            .arg("-c")
            .arg("Release")
            .arg("-r")
            .arg("wasi-wasm")
            .arg("--self-contained")
            .arg("true")
            .arg("-o")
            .arg(modules_dir)
            .current_dir(&reducers_dir)
            .status()
            .map_err(|e| voltra::error::VoltraError::internal(format!("dotnet publish: {}", e)))?;
        if status.success() {
            println!(
                "  C# compilation OK — .wasm written to {}",
                modules_dir.display()
            );
        } else {
            return Err(voltra::error::VoltraError::internal(format!(
                "dotnet publish failed (exit {:?})",
                status.code()
            )));
        }
        return Ok(());
    }

    // ── Go / TinyGo detection ─────────────────────────────────────────────────
    let has_gomod = reducers_dir.join("go.mod").exists();
    let has_go_files = std::fs::read_dir(&reducers_dir)
        .ok()
        .map(|entries| {
            entries.flatten().any(|e| {
                e.path()
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|s| s.eq_ignore_ascii_case("go"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);

    if has_gomod && has_go_files {
        println!("  Go project detected: {}", reducers_dir.display());

        let tinygo_ok = std::process::Command::new("tinygo")
            .arg("version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !tinygo_ok {
            eprintln!(
                "  Warning: 'tinygo' not found on PATH. Skipping Go compilation.\n\
                 Install TinyGo: https://tinygo.org/getting-started/install/\n\
                 Then run: tinygo build -o modules/reducers.wasm -target wasi ./reducers"
            );
            return Ok(());
        }

        // Determine the output name from the module name in go.mod, or use "reducers".
        let mod_name = std::fs::read_to_string(reducers_dir.join("go.mod"))
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.trim_start().starts_with("module "))
                    .map(|l| {
                        l.trim_start_matches("module")
                            .trim()
                            .split('/')
                            .next_back()
                            .unwrap_or("reducers")
                            .to_string()
                    })
            })
            .unwrap_or_else(|| "reducers".to_string());
        let out_wasm = modules_dir.join(format!("{}.wasm", mod_name));

        println!("  Go → WASM via tinygo build ...");
        let status = std::process::Command::new("tinygo")
            .arg("build")
            .arg("-o")
            .arg(&out_wasm)
            .arg("-target")
            .arg("wasi")
            .arg(".")
            .current_dir(&reducers_dir)
            .status()
            .map_err(|e| voltra::error::VoltraError::internal(format!("tinygo build: {}", e)))?;
        if status.success() {
            println!("  Go compilation OK — {} written", out_wasm.display());
        } else {
            return Err(voltra::error::VoltraError::internal(format!(
                "tinygo build failed (exit {:?})",
                status.code()
            )));
        }
    }
    Ok(())
}

/// Compile every `.vol` file found in `voltra_dir` into a `<stem>.rs` file.
/// On success prints a summary line; on error prints each diagnostic and
/// returns an error so the caller aborts the build.
pub(crate) fn build_voltra_files(voltra_dir: &Path) -> Result<()> {
    let entries = match std::fs::read_dir(voltra_dir) {
        Ok(e) => e,
        Err(_) => return Ok(()), // directory doesn't exist — nothing to do
    };

    let mut voltra_files: Vec<std::path::PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("vol"))
        .collect();
    voltra_files.sort();

    if voltra_files.is_empty() {
        return Ok(());
    }

    let mut ok = 0usize;
    let mut failed = 0usize;

    println!("  .vol compiler:");
    for voltra_path in &voltra_files {
        let _stem = voltra_path
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy();
        let out_path = voltra_path.with_extension("rs");
        print!(
            "  .vol  {} → {} ... ",
            voltra_path.display(),
            out_path.display()
        );

        let source = match std::fs::read_to_string(voltra_path) {
            Ok(s) => s,
            Err(e) => {
                println!("FAILED (read: {})", e);
                failed += 1;
                continue;
            }
        };
        let filename = voltra_path.display().to_string();
        match voltra::dsl::compile(&source, &filename) {
            Ok(rust_code) => match std::fs::write(&out_path, &rust_code) {
                Ok(_) => {
                    println!("ok");
                    ok += 1;
                }
                Err(e) => {
                    println!("FAILED (write: {})", e);
                    failed += 1;
                }
            },
            Err(errors) => {
                println!(
                    "FAILED ({} error{})",
                    errors.len(),
                    if errors.len() == 1 { "" } else { "s" }
                );
                for e in &errors {
                    eprintln!(
                        "  {}:{}: error: {}",
                        voltra_path.display(),
                        e.line,
                        e.message
                    );
                }
                failed += 1;
            }
        }
    }

    println!("  .vol: {} compiled, {} failed", ok, failed);
    if failed > 0 {
        Err(voltra::error::VoltraError::internal(format!(
            "{} .vol file(s) failed to compile",
            failed
        )))
    } else {
        Ok(())
    }
}

pub(crate) fn build_wasm_modules(modules_dir: &Path) -> Result<()> {
    // ── Step 0a: compile .vol files if present ───────────────────────────────
    let project_root = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    build_voltra_files(&project_root)?;

    // ── Step 0b: compile multi-language reducers (C#, Go) if present ─────────
    build_multi_lang_reducers(&project_root, modules_dir)?;

    if !modules_dir.is_dir() {
        println!("No '{}' directory found.", modules_dir.display());
        return Ok(());
    }
    let javy_ok = std::process::Command::new("javy")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !javy_ok {
        eprintln!("Error: 'javy' not found on PATH.\nDownload: https://github.com/bytecodealliance/javy/releases");
        return Err(voltra::error::VoltraError::internal(
            "javy not found on PATH",
        ));
    }
    let mut js_files = Vec::new();
    collect_js_files(modules_dir, &mut js_files);
    if js_files.is_empty() {
        println!("No .js files found in {}.", modules_dir.display());
        return Ok(());
    }
    let mut compiled = 0usize;
    let mut failed = 0usize;
    let mut wasm_paths: Vec<std::path::PathBuf> = Vec::new();
    for js_path in &js_files {
        let wasm_path = js_path.with_extension("wasm");
        print!("  JS→WASM  {} ... ", js_path.display());
        match std::process::Command::new("javy")
            .arg("build")
            .arg(js_path)
            .arg("-o")
            .arg(&wasm_path)
            .status()
        {
            Ok(s) if s.success() => {
                println!("ok");
                compiled += 1;
                wasm_paths.push(wasm_path);
            }
            Ok(s) => {
                println!("FAILED (exit {})", s.code().unwrap_or(-1));
                failed += 1;
            }
            Err(e) => {
                println!("FAILED ({})", e);
                failed += 1;
            }
        }
    }

    // Also AOT-compile any .wasm files that were NOT produced by javy above
    // (e.g. hand-written WAT compiled externally, or Rust→WASM32 reducers).
    collect_wasm_files(modules_dir, &mut wasm_paths);
    wasm_paths.sort();
    wasm_paths.dedup();

    let mut aot_ok = 0usize;
    let mut aot_skip = 0usize;
    println!();
    println!("  AOT compilation (Cranelift → native machine code):");
    for wasm_path in &wasm_paths {
        let cwasm_path = wasm_path.with_extension("cwasm");
        let fresh = cwasm_path.exists() && {
            let t_wasm = wasm_path.metadata().and_then(|m| m.modified()).ok();
            let t_cwasm = cwasm_path.metadata().and_then(|m| m.modified()).ok();
            matches!((t_wasm, t_cwasm), (Some(w), Some(c)) if c >= w)
        };
        if fresh {
            aot_skip += 1;
            continue;
        }
        print!("  WASM→AOT {} ... ", wasm_path.display());
        match voltra::reducer::wasm::aot_compile(wasm_path) {
            Ok(_) => {
                println!("ok");
                aot_ok += 1;
            }
            Err(e) => {
                println!("FAILED ({})", e);
            }
        }
    }
    println!();
    if failed == 0 {
        println!(
            "Build complete: {} JS→WASM, {} AOT compiled, {} AOT up-to-date.",
            compiled, aot_ok, aot_skip
        );
        Ok(())
    } else {
        Err(voltra::error::VoltraError::internal(format!(
            "{} files failed",
            failed
        )))
    }
}

pub(crate) fn collect_js_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                collect_js_files(&p, out);
            } else if p
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("js"))
                .unwrap_or(false)
            {
                out.push(p);
            }
        }
    }
}

pub(crate) fn collect_wasm_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                collect_wasm_files(&p, out);
            } else if p
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("wasm"))
                .unwrap_or(false)
            {
                out.push(p);
            }
        }
    }
}
