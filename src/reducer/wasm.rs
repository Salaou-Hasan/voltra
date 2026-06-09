use crate::error::{NeonDBError, Result};
use crate::reducer::backend::ReducerBackend;
use crate::reducer::context::ReducerContext;
use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use wasmtime::{Caller, Engine, Instance, Linker, Module, PoolingAllocationConfig,
               ResourceLimiter, Store};

// ── Thread-local ReducerContext pointer ───────────────────────────────────────
//
// Host functions are registered once as 'static closures on a shared Linker.
// They need access to the per-call ReducerContext without capturing it (which
// would violate 'static).  The solution: store a raw pointer in a thread-local
// for the duration of the WASM call and clear it on exit via a Drop guard.
//
// Safety invariant: the pointer is valid for exactly the lifetime of the
// WasmCtxGuard.  WASM runs synchronously on the same thread as the caller, so
// the pointer is never accessed after the call returns.

thread_local! {
    static WASM_CTX: Cell<usize> = Cell::new(0);
}

struct WasmCtxGuard;

impl WasmCtxGuard {
    fn install(ctx: &mut ReducerContext) -> Self {
        WASM_CTX.with(|p| p.set(ctx as *mut ReducerContext as usize));
        WasmCtxGuard
    }
}

impl Drop for WasmCtxGuard {
    fn drop(&mut self) {
        WASM_CTX.with(|p| p.set(0));
    }
}

fn with_ctx<R, F: FnOnce(&mut ReducerContext) -> R>(f: F) -> Option<R> {
    let ptr = WASM_CTX.with(|p| p.get());
    if ptr == 0 {
        return None;
    }
    Some(f(unsafe { &mut *(ptr as *mut ReducerContext) }))
}

// ── Resource limiter (doubles as Store state) ─────────────────────────────────

struct WasmLimiter {
    max_memory_bytes: usize,
}

impl ResourceLimiter for WasmLimiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= self.max_memory_bytes)
    }

    fn table_growing(
        &mut self,
        _current: u32,
        _desired: u32,
        _maximum: Option<u32>,
    ) -> wasmtime::Result<bool> {
        Ok(true)
    }
}

// ── Shared Engine + Linker (built once for the process lifetime) ──────────────
//
// Engine creation is expensive (~10 ms).  Building it once and sharing it
// across all WASM backends eliminates per-call overhead.
//
// Linker registration is also done once.  All host functions use the
// thread-local context pointer so they require no captured state, making
// the Linker trivially Send + Sync + 'static.

static WASM_ENGINE: OnceLock<Engine> = OnceLock::new();
static WASM_LINKER: OnceLock<Linker<WasmLimiter>> = OnceLock::new();

// ── Engine construction ───────────────────────────────────────────────────────
//
// Wasmtime's PoolingAllocationConfig pre-allocates virtual address space for
// instance linear memories.  When an instance is created, Wasmtime reuses one
// of these pre-warmed slots — no mmap/malloc, just a data-segment memcpy.
// This cuts instantiation from ~1–5 ms down to ~10–50 µs, giving near-native
// throughput for WASM reducers without AOT compilation.
//
// Pool slots = max(num_cpus × 4, 32).  Each slot reserves `memory_pages × 64 KB`
// of *virtual* (not physical) address space.  With the default 160-page limit
// (10 MB each) and 32 slots that is 320 MB VAS — trivial on any 64-bit host.
//
// Fall-back: if the OS rejects the pooling reservation (e.g. hugepage limits
// on some hardened Linux configs) the engine transparently reverts to the
// standard on-demand allocator.  The server still runs; throughput degrades
// gracefully.

fn build_pooling_engine() -> std::result::Result<Engine, wasmtime::Error> {
    let mut config = wasmtime::Config::new();
    config.consume_fuel(true);

    let cores = num_cpus::get();
    let slots = ((cores * 4) as u32).max(32).min(512);

    let mut pool = PoolingAllocationConfig::default();
    pool.total_memories(slots);
    pool.total_tables(slots);
    pool.total_core_instances(slots);

    config.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(pool));
    Engine::new(&config)
}

fn build_standard_engine() -> Engine {
    let mut config = wasmtime::Config::new();
    config.consume_fuel(true);
    Engine::new(&config).expect("Wasmtime standard engine init failed")
}

/// Returns the process-wide Wasmtime engine.  Also used by `aot_compile`.
pub fn shared_engine() -> &'static Engine {
    WASM_ENGINE.get_or_init(|| {
        build_pooling_engine().unwrap_or_else(|e| {
            log::warn!(
                "[neondb] WASM pooling allocator unavailable ({}); \
                 falling back to on-demand allocation",
                e
            );
            build_standard_engine()
        })
    })
}

fn shared_linker() -> &'static Linker<WasmLimiter> {
    WASM_LINKER.get_or_init(|| {
        let mut linker: Linker<WasmLimiter> = Linker::new(shared_engine());
        register_host_functions(&mut linker)
            .expect("Failed to register WASM host functions");
        linker
    })
}

// ── Memory helpers ────────────────────────────────────────────────────────────

fn read_str(caller: &mut Caller<'_, WasmLimiter>, ptr: i32, len: i32) -> Option<String> {
    let mem = caller.get_export("memory")?.into_memory()?;
    let data = mem.data(caller);
    let s = ptr as usize;
    let e = s.checked_add(len as usize)?;
    if e > data.len() {
        return None;
    }
    std::str::from_utf8(&data[s..e]).ok().map(|s| s.to_owned())
}

fn read_bytes(caller: &mut Caller<'_, WasmLimiter>, ptr: i32, len: i32) -> Option<Vec<u8>> {
    let mem = caller.get_export("memory")?.into_memory()?;
    let data = mem.data(caller);
    let s = ptr as usize;
    let e = s.checked_add(len as usize)?;
    if e > data.len() {
        return None;
    }
    Some(data[s..e].to_vec())
}

fn write_bytes(caller: &mut Caller<'_, WasmLimiter>, ptr: i32, bytes: &[u8]) -> bool {
    let mem = match caller.get_export("memory").and_then(|e| e.into_memory()) {
        Some(m) => m,
        None => return false,
    };
    let data = mem.data_mut(caller);
    let s = ptr as usize;
    let e = match s.checked_add(bytes.len()) {
        Some(v) => v,
        None => return false,
    };
    if e > data.len() {
        return false;
    }
    data[s..e].copy_from_slice(bytes);
    true
}

// ── Host function registration ────────────────────────────────────────────────

fn register_host_functions(linker: &mut Linker<WasmLimiter>) -> wasmtime::Result<()> {
    // ── neondb_get_row ────────────────────────────────────────────────────────
    // Signature: (table_ptr, table_len, key_ptr, key_len, out_ptr, out_max) -> i32
    // Returns:   bytes written (>=0), -1 (not found), -2 (buf too small / error)
    linker.func_wrap(
        "env",
        "neondb_get_row",
        |mut caller: Caller<'_, WasmLimiter>,
         table_ptr: i32,
         table_len: i32,
         key_ptr: i32,
         key_len: i32,
         out_ptr: i32,
         out_max: i32|
         -> i32 {
            let table = match read_str(&mut caller, table_ptr, table_len) {
                Some(s) => s,
                None => return -2,
            };
            let key = match read_str(&mut caller, key_ptr, key_len) {
                Some(s) => s,
                None => return -2,
            };
            let row = with_ctx(|ctx| ctx.get_row(&table, &key).ok().flatten()).flatten();
            match row {
                None => -1,
                Some(val) => {
                    let json = serde_json::to_vec(&val).unwrap_or_default();
                    if json.len() > out_max as usize {
                        return -2;
                    }
                    if !write_bytes(&mut caller, out_ptr, &json) {
                        return -2;
                    }
                    json.len() as i32
                }
            }
        },
    )?;

    // ── neondb_set_row ────────────────────────────────────────────────────────
    // Signature: (table_ptr, table_len, key_ptr, key_len, val_ptr, val_len) -> i32
    // Returns:   0 (ok), -1 (error)
    linker.func_wrap(
        "env",
        "neondb_set_row",
        |mut caller: Caller<'_, WasmLimiter>,
         table_ptr: i32,
         table_len: i32,
         key_ptr: i32,
         key_len: i32,
         val_ptr: i32,
         val_len: i32|
         -> i32 {
            let table = match read_str(&mut caller, table_ptr, table_len) {
                Some(s) => s,
                None => return -1,
            };
            let key = match read_str(&mut caller, key_ptr, key_len) {
                Some(s) => s,
                None => return -1,
            };
            let json_bytes = match read_bytes(&mut caller, val_ptr, val_len) {
                Some(b) => b,
                None => return -1,
            };
            let val: serde_json::Value = match serde_json::from_slice(&json_bytes) {
                Ok(v) => v,
                Err(_) => return -1,
            };
            match with_ctx(|ctx| ctx.set_row(table, key, val)) {
                Some(Ok(_)) => 0,
                _ => -1,
            }
        },
    )?;

    // ── neondb_delete_row ─────────────────────────────────────────────────────
    // Signature: (table_ptr, table_len, key_ptr, key_len) -> i32
    // Returns:   0 (ok), -1 (error)
    linker.func_wrap(
        "env",
        "neondb_delete_row",
        |mut caller: Caller<'_, WasmLimiter>,
         table_ptr: i32,
         table_len: i32,
         key_ptr: i32,
         key_len: i32|
         -> i32 {
            let table = match read_str(&mut caller, table_ptr, table_len) {
                Some(s) => s,
                None => return -1,
            };
            let key = match read_str(&mut caller, key_ptr, key_len) {
                Some(s) => s,
                None => return -1,
            };
            match with_ctx(|ctx| ctx.delete_row(table, key)) {
                Some(Ok(_)) => 0,
                _ => -1,
            }
        },
    )?;

    // ── neondb_caller_id ──────────────────────────────────────────────────────
    // Signature: (out_ptr, out_max) -> i32
    // Returns:   bytes written (>=0), -1 (buf too small)
    linker.func_wrap(
        "env",
        "neondb_caller_id",
        |mut caller: Caller<'_, WasmLimiter>, out_ptr: i32, out_max: i32| -> i32 {
            let s = with_ctx(|ctx| ctx.caller_id.clone()).unwrap_or_default();
            let b = s.as_bytes();
            if b.len() > out_max as usize {
                return -1;
            }
            if !write_bytes(&mut caller, out_ptr, b) {
                return -1;
            }
            b.len() as i32
        },
    )?;

    // ── neondb_caller_role ────────────────────────────────────────────────────
    // Signature: (out_ptr, out_max) -> i32
    // Returns:   bytes written (>=0), -1 (buf too small)
    linker.func_wrap(
        "env",
        "neondb_caller_role",
        |mut caller: Caller<'_, WasmLimiter>, out_ptr: i32, out_max: i32| -> i32 {
            let s = with_ctx(|ctx| ctx.caller_role.clone()).unwrap_or_default();
            let b = s.as_bytes();
            if b.len() > out_max as usize {
                return -1;
            }
            if !write_bytes(&mut caller, out_ptr, b) {
                return -1;
            }
            b.len() as i32
        },
    )?;

    // ── neondb_get_counter (backward compat) ──────────────────────────────────
    linker.func_wrap(
        "env",
        "neondb_get_counter",
        |mut caller: Caller<'_, WasmLimiter>, ptr: i32, len: i32| -> i32 {
            let name = read_str(&mut caller, ptr, len).unwrap_or_default();
            with_ctx(|ctx| {
                ctx.get_counter(&name)
                    .ok()
                    .flatten()
                    .map(|c| c.value)
                    .unwrap_or(0)
            })
            .unwrap_or(0)
        },
    )?;

    // ── neondb_set_counter (backward compat) ──────────────────────────────────
    linker.func_wrap(
        "env",
        "neondb_set_counter",
        |mut caller: Caller<'_, WasmLimiter>, ptr: i32, len: i32, value: i32| {
            let name = read_str(&mut caller, ptr, len).unwrap_or_default();
            with_ctx(|ctx| {
                let _ = ctx.set_counter(name, value);
            });
        },
    )?;

    Ok(())
}

// ── Backend ───────────────────────────────────────────────────────────────────

pub struct WasmReducerBackend {
    module: Module,
    function_name: String,
}

impl WasmReducerBackend {
    pub fn from_file(path: PathBuf, function_name: &str) -> Result<Self> {
        let engine = shared_engine();

        // Prefer a pre-compiled AOT module (.cwasm) when it is at least as
        // fresh as the source .wasm.  This eliminates JIT compilation entirely:
        // the .cwasm is native machine code produced by Cranelift.
        let cwasm_path = path.with_extension("cwasm");
        let module = if cwasm_path.exists() && is_fresh(&cwasm_path, &path) {
            log::info!("Loading AOT-compiled reducer from {:?}", cwasm_path);
            // SAFETY: the .cwasm was serialized by the same engine configuration
            // (same Config, same Cranelift version).  `neondb build` always
            // produces the .cwasm alongside the .wasm in one step.
            unsafe { Module::deserialize_file(engine, &cwasm_path) }.map_err(|e| {
                NeonDBError::reducer_error(format!(
                    "AOT load {:?}: {}",
                    cwasm_path, e
                ))
            })?
        } else {
            let bytes = fs::read(&path)?;
            let wasm_bytes = if path.extension().and_then(|s| s.to_str()) == Some("wat") {
                wat::parse_bytes(&bytes)
                    .map_err(|e| {
                        NeonDBError::reducer_error(format!("WAT parse: {}", e))
                    })?
                    .into_owned()
            } else {
                bytes
            };
            Module::new(engine, &wasm_bytes).map_err(|e| {
                NeonDBError::reducer_error(format!("WASM compile {:?}: {}", path, e))
            })?
        };

        Ok(WasmReducerBackend {
            module,
            function_name: function_name.to_string(),
        })
    }

    fn call(&self, ctx: &mut ReducerContext, args: &[u8]) -> Result<Vec<u8>> {
        let max_io = crate::reducer::max_io_bytes();
        if args.len() > max_io {
            return Err(NeonDBError::reducer_error(format!(
                "Reducer args too large: {} bytes (limit {})",
                args.len(),
                max_io
            )));
        }

        // Install the context pointer in the thread-local for the duration of
        // this call.  The guard clears it on drop (even on panic).
        let _guard = WasmCtxGuard::install(ctx);

        let mut store = Store::new(
            shared_engine(),
            WasmLimiter {
                max_memory_bytes: crate::reducer::max_memory_bytes(),
            },
        );
        store.limiter(|s| s);
        store
            .set_fuel(1_000_000)
            .map_err(|e| NeonDBError::reducer_error(format!("Fuel: {}", e)))?;

        let instance = shared_linker()
            .instantiate(&mut store, &self.module)
            .map_err(|e| NeonDBError::reducer_error(format!("WASM instantiate: {}", e)))?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| NeonDBError::reducer_error("WASM module missing 'memory' export"))?;

        // Write args into WASM linear memory at the 64 KB mark.
        let args_offset: u32 = 0x10000;
        let mem_data = memory.data_mut(&mut store);
        if mem_data.len() < args_offset as usize + args.len() {
            return Err(NeonDBError::reducer_error(
                "WASM linear memory too small for args",
            ));
        }
        mem_data[args_offset as usize..args_offset as usize + args.len()]
            .copy_from_slice(args);

        let (result_ptr, result_len) =
            call_reducer_typed(&instance, &mut store, &self.function_name, args_offset as i32, args.len() as i32)
                .map_err(|e| NeonDBError::reducer_error(format!("WASM call: {}", e)))?;

        if result_len as usize > max_io {
            return Err(NeonDBError::reducer_error(format!(
                "WASM result too large: {} bytes (limit {})",
                result_len, max_io
            )));
        }

        let mem_slice = memory.data(&store);
        let start = result_ptr as usize;
        let end = start
            .checked_add(result_len as usize)
            .filter(|&e| e <= mem_slice.len())
            .ok_or_else(|| NeonDBError::reducer_error("WASM result out of bounds"))?;

        let result_bytes = mem_slice[start..end].to_vec();
        parse_wasm_result(&result_bytes)
    }
}

/// Parse WASM output: JSON text (WAT backward-compat) is converted to
/// MessagePack; raw MessagePack (from neondb-reducer compiled modules) is
/// passed through as-is.
fn parse_wasm_result(bytes: &[u8]) -> Result<Vec<u8>> {
    if let Ok(s) = std::str::from_utf8(bytes) {
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(s) {
            return Ok(rmp_serde::to_vec(&val)?);
        }
    }
    Ok(bytes.to_vec())
}

impl ReducerBackend for WasmReducerBackend {
    fn execute(&self, ctx: &mut ReducerContext, args: &[u8]) -> Result<Vec<u8>> {
        self.call(ctx, args)
    }
}

// ── AOT compilation ───────────────────────────────────────────────────────────

/// Compile a `.wasm` file to native code and save the result as `.cwasm`
/// alongside it.  Called by `neondb build` after JS→WASM compilation.
///
/// The resulting `.cwasm` is Cranelift-compiled machine code.  `from_file`
/// loads it with `Module::deserialize_file` — no JIT warmup at all.
pub fn aot_compile(wasm_path: &Path) -> Result<PathBuf> {
    let engine = shared_engine();
    let module = Module::from_file(engine, wasm_path).map_err(|e| {
        NeonDBError::reducer_error(format!("AOT read {:?}: {}", wasm_path, e))
    })?;
    let bytes = module
        .serialize()
        .map_err(|e| NeonDBError::reducer_error(format!("AOT serialize: {}", e)))?;
    let cwasm_path = wasm_path.with_extension("cwasm");
    fs::write(&cwasm_path, &bytes)
        .map_err(|e| NeonDBError::reducer_error(format!("AOT write {:?}: {}", cwasm_path, e)))?;
    Ok(cwasm_path)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns true when `newer` has a modification time >= `older`.
/// Falls back to true (prefer the .cwasm) when timestamps are unavailable.
fn is_fresh(newer: &Path, older: &Path) -> bool {
    let t_newer = newer.metadata().and_then(|m| m.modified()).ok();
    let t_older = older.metadata().and_then(|m| m.modified()).ok();
    match (t_newer, t_older) {
        (Some(n), Some(o)) => n >= o,
        _ => true,
    }
}

fn call_reducer_typed(
    instance: &Instance,
    store: &mut Store<WasmLimiter>,
    name: &str,
    args_ptr: i32,
    args_len: i32,
) -> std::result::Result<(i32, i32), Box<dyn std::error::Error>> {
    // 1. Standard multi-value return: (args_ptr, args_len) -> (result_ptr, result_len)
    //    Used by WAT/WAT modules and TinyGo (Go) reducers.
    if let Ok(f) = instance.get_typed_func::<(i32, i32), (i32, i32)>(&mut *store, name) {
        return Ok(f.call(&mut *store, (args_ptr, args_len))?);
    }
    // 2. No-args multi-value: () -> (result_ptr, result_len)
    if let Ok(f) = instance.get_typed_func::<(), (i32, i32)>(&mut *store, name) {
        return Ok(f.call(&mut *store, ())?);
    }
    // 3. i64 fat-pointer: (args_ptr, args_len) -> i64
    //    Used by C# (.NET WASI) reducers, which cannot export multi-value WASM functions
    //    from [UnmanagedCallersOnly].  The i64 packs ptr (high 32) | len (low 32).
    if let Ok(f) = instance.get_typed_func::<(i32, i32), i64>(&mut *store, name) {
        let packed = f.call(&mut *store, (args_ptr, args_len))?;
        let result_ptr = ((packed as u64) >> 32) as i32;
        let result_len = ((packed as u64) & 0xFFFF_FFFF) as i32;
        return Ok((result_ptr, result_len));
    }
    // 4. No-args i64 fat-pointer.
    if let Ok(f) = instance.get_typed_func::<(), i64>(&mut *store, name) {
        let packed = f.call(&mut *store, ())?;
        let result_ptr = ((packed as u64) >> 32) as i32;
        let result_len = ((packed as u64) & 0xFFFF_FFFF) as i32;
        return Ok((result_ptr, result_len));
    }
    Err(format!("No compatible '{}' export found in WASM module", name).into())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reducer::context::ReducerContext;
    use crate::table::TableStore;
    use std::sync::Arc;

    fn make_ctx() -> ReducerContext {
        ReducerContext::new(Arc::new(TableStore::new()), 1000)
    }

    #[test]
    fn test_wasm_smoke_test_wat() {
        let path = PathBuf::from("modules/increment_wasm.wat");
        if !path.exists() {
            eprintln!("Skipping: modules/increment_wasm.wat not found");
            return;
        }
        let backend = WasmReducerBackend::from_file(path, "reducer").unwrap();
        let mut ctx = make_ctx();
        let result = backend.execute(&mut ctx, b"").unwrap();
        let decoded: serde_json::Value = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(decoded["new_value"], 1);
    }

    #[test]
    fn test_wasm_memory_limit_denies_growth() {
        let _g = crate::reducer::SANDBOX_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        crate::reducer::set_max_memory_bytes(64 * 1024);

        let wat_src = r#"(module
  (memory (export "memory") 1)
  (func (export "reducer") (param i32 i32) (result i32 i32)
    (drop (memory.grow (i32.const 100)))
    (i32.store (i32.const 3276800) (i32.const 0xdeadbeef))
    (i32.const 0)
    (i32.const 0)
  )
)"#;
        let tmp = std::env::temp_dir().join("test_wasm_mem_limit.wat");
        std::fs::write(&tmp, wat_src).unwrap();
        let backend = WasmReducerBackend::from_file(tmp.clone(), "reducer").unwrap();
        let mut ctx = make_ctx();
        let result = backend.execute(&mut ctx, b"");
        std::fs::remove_file(&tmp).ok();
        crate::reducer::set_max_memory_bytes(64 * 1024 * 1024);
        assert!(result.is_err());
    }

    #[test]
    fn test_wasm_args_oversize_rejected() {
        let _g = crate::reducer::SANDBOX_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        crate::reducer::set_max_io_bytes(4 * 1024);

        let wat_src = r#"(module
  (memory (export "memory") 1)
  (func (export "reducer") (param i32 i32) (result i32 i32)
    (i32.const 0) (i32.const 0)
  )
)"#;
        let tmp = std::env::temp_dir().join("test_wasm_args_cap.wat");
        std::fs::write(&tmp, wat_src).unwrap();
        let backend = WasmReducerBackend::from_file(tmp.clone(), "reducer").unwrap();
        let mut ctx = make_ctx();
        let big = vec![0u8; 5 * 1024];
        let result = backend.execute(&mut ctx, &big);
        std::fs::remove_file(&tmp).ok();
        crate::reducer::set_max_io_bytes(1024 * 1024);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().to_lowercase().contains("too large"));
    }

    #[test]
    fn test_wasm_host_imports_counter_compat() {
        let wat_src = r#"(module
  (import "env" "neondb_get_counter" (func $get (param i32 i32) (result i32)))
  (import "env" "neondb_set_counter" (func $set (param i32 i32 i32)))
  (memory (export "memory") 1)
  (data (i32.const 512) "score")
  (data (i32.const 0) "{\"new_value\":3,\"timestamp\":0}")
  (func (export "reducer") (param i32 i32) (result i32 i32)
    (local $cur i32)
    (local.set $cur (call $get (i32.const 512) (i32.const 5)))
    (call $set
      (i32.const 512) (i32.const 5)
      (i32.add (local.get $cur) (i32.const 3)))
    (i32.const 0)
    (i32.const 29)
  )
)"#;
        let tmp = std::env::temp_dir().join("test_host_imports.wat");
        std::fs::write(&tmp, wat_src).unwrap();
        let backend = WasmReducerBackend::from_file(tmp.clone(), "reducer").unwrap();
        let mut ctx = make_ctx();
        let r = backend.execute(&mut ctx, b"").unwrap();
        let v: serde_json::Value = rmp_serde::from_slice(&r).unwrap();
        assert_eq!(v["new_value"], 3);
        ctx.commit().unwrap();
        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn test_wasm_get_set_row_via_host() {
        // WAT module that calls neondb_set_row then neondb_get_row and returns
        // the retrieved JSON length as new_value to prove the round-trip works.
        let wat_src = r#"(module
  (import "env" "neondb_set_row" (func $set_row (param i32 i32 i32 i32 i32 i32) (result i32)))
  (import "env" "neondb_get_row" (func $get_row (param i32 i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 2)
  (data (i32.const 0)   "players")
  (data (i32.const 100) "alice")
  (data (i32.const 200) "{\"hp\":100}")
  (data (i32.const 400) "{\"new_value\":1,\"timestamp\":0}")
  (func (export "reducer") (param i32 i32) (result i32 i32)
    (drop (call $set_row
      (i32.const 0)   (i32.const 7)
      (i32.const 100) (i32.const 5)
      (i32.const 200) (i32.const 10)))
    (drop (call $get_row
      (i32.const 0)   (i32.const 7)
      (i32.const 100) (i32.const 5)
      (i32.const 500) (i32.const 1000)))
    (i32.const 400)
    (i32.const 29)
  )
)"#;
        let tmp = std::env::temp_dir().join("test_wasm_row_api.wat");
        std::fs::write(&tmp, wat_src).unwrap();
        let backend = WasmReducerBackend::from_file(tmp.clone(), "reducer").unwrap();
        let mut ctx = make_ctx();
        let r = backend.execute(&mut ctx, b"").unwrap();
        let v: serde_json::Value = rmp_serde::from_slice(&r).unwrap();
        assert_eq!(v["new_value"], 1);
        ctx.commit().unwrap();
        // Verify the row was actually staged
        let row = ctx.get_row("players", "alice").unwrap();
        assert!(row.is_some());
        assert_eq!(row.unwrap()["hp"], 100);
        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn test_aot_compile_and_load() {
        let wat_src = r#"(module
  (memory (export "memory") 1)
  (data (i32.const 0) "{\"ok\":true}")
  (func (export "reducer") (param i32 i32) (result i32 i32)
    (i32.const 0) (i32.const 11)
  )
)"#;
        let wasm_tmp = std::env::temp_dir().join("test_aot_src.wat");
        let wasm_compiled = std::env::temp_dir().join("test_aot_src.wasm");
        std::fs::write(&wasm_tmp, wat_src).unwrap();

        // Convert WAT → WASM bytes manually so we have a real .wasm to AOT compile
        let wasm_bytes = wat::parse_file(&wasm_tmp).unwrap();
        std::fs::write(&wasm_compiled, &wasm_bytes).unwrap();

        // AOT compile → .cwasm
        let cwasm_path = aot_compile(&wasm_compiled).unwrap();
        assert!(cwasm_path.exists());

        // Load via WasmReducerBackend (should pick up .cwasm automatically)
        let backend = WasmReducerBackend::from_file(wasm_compiled.clone(), "reducer").unwrap();
        let mut ctx = make_ctx();
        let r = backend.execute(&mut ctx, b"").unwrap();
        let v: serde_json::Value = rmp_serde::from_slice(&r).unwrap();
        assert_eq!(v["ok"], true);

        std::fs::remove_file(&wasm_tmp).ok();
        std::fs::remove_file(&wasm_compiled).ok();
        std::fs::remove_file(&cwasm_path).ok();
    }
}
