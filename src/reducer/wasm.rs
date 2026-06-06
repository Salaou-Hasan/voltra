use crate::error::{NeonDBError, Result};
use crate::reducer::backend::ReducerBackend;
use crate::reducer::context::ReducerContext;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use wasmtime::{Caller, Engine, Instance, Linker, Module, Store};

// ---------------------------------------------------------------------------
// Shared mutable state for host callbacks
// ---------------------------------------------------------------------------

struct WasmHostState {
    counters: std::collections::HashMap<String, i32>,
    pending_sets: Vec<(String, i32)>,
}

impl WasmHostState {
    fn new(counters: std::collections::HashMap<String, i32>) -> Self {
        WasmHostState {
            counters,
            pending_sets: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

pub struct WasmReducerBackend {
    engine: Engine,
    module: Module,
    function_name: String,
}

impl WasmReducerBackend {
    pub fn from_file(path: PathBuf, function_name: &str) -> Result<Self> {
        let bytes = fs::read(&path)?;
        let wasm_bytes = if path.extension().and_then(|s| s.to_str()) == Some("wat") {
            wat::parse_bytes(&bytes)
                .map_err(|e| NeonDBError::reducer_error(format!("WAT parse error: {}", e)))?
                .into_owned()
        } else {
            bytes
        };

        let mut config = wasmtime::Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config)
            .map_err(|e| NeonDBError::reducer_error(format!("Wasmtime engine: {}", e)))?;
        let module = Module::new(&engine, &wasm_bytes)
            .map_err(|e| NeonDBError::reducer_error(format!("WASM compile: {}", e)))?;

        Ok(WasmReducerBackend {
            engine,
            module,
            function_name: function_name.to_string(),
        })
    }

    fn call(&self, ctx: &mut ReducerContext, args: &[u8]) -> Result<Vec<u8>> {
        // ---- 1. Snapshot counters -----------------------------------------
        let counters: std::collections::HashMap<String, i32> = ctx
            .list_counters()?
            .into_iter()
            .map(|c| (c.name, c.value))
            .collect();

        let host_state = Arc::new(Mutex::new(WasmHostState::new(counters)));
        let host_get = host_state.clone();
        let host_set = host_state.clone();

        // ---- 2. Build store + linker --------------------------------------
        let mut store = Store::new(&self.engine, ());

        // Wasmtime 21: add_fuel was renamed to set_fuel
        store
            .set_fuel(1_000_000)
            .map_err(|e| NeonDBError::reducer_error(format!("Fuel error: {}", e)))?;

        let mut linker: Linker<()> = Linker::new(&self.engine);

        // neondb_get_counter(name_ptr, name_len) -> i32
        linker
            .func_wrap(
                "env",
                "neondb_get_counter",
                move |mut caller: Caller<'_, ()>, ptr: i32, len: i32| -> i32 {
                    let name =
                        read_string_from_memory(&mut caller, ptr as u32, len as u32)
                            .unwrap_or_default();
                    let state = host_get.lock().unwrap();
                    *state.counters.get(&name).unwrap_or(&0)
                },
            )
            .map_err(|e| NeonDBError::reducer_error(format!("Linker get: {}", e)))?;

        // neondb_set_counter(name_ptr, name_len, value)
        linker
            .func_wrap(
                "env",
                "neondb_set_counter",
                move |mut caller: Caller<'_, ()>, ptr: i32, len: i32, value: i32| {
                    let name =
                        read_string_from_memory(&mut caller, ptr as u32, len as u32)
                            .unwrap_or_default();
                    let mut state = host_set.lock().unwrap();
                    state.counters.insert(name.clone(), value);
                    state.pending_sets.push((name, value));
                },
            )
            .map_err(|e| NeonDBError::reducer_error(format!("Linker set: {}", e)))?;

        // ---- 3. Instantiate and call reducer ------------------------------
        let instance = linker
            .instantiate(&mut store, &self.module)
            .map_err(|e| NeonDBError::reducer_error(format!("WASM instantiate: {}", e)))?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| NeonDBError::reducer_error("WASM module missing 'memory' export"))?;

        // Write args into WASM memory at offset 0x10000 (64 KB mark)
        let args_offset: u32 = 0x10000;
        let args_len = args.len() as u32;
        let mem_data = memory.data_mut(&mut store);
        if mem_data.len() < (args_offset as usize + args.len()) {
            return Err(NeonDBError::reducer_error(
                "WASM linear memory too small for args",
            ));
        }
        mem_data[args_offset as usize..args_offset as usize + args.len()]
            .copy_from_slice(args);

        // Fix: use &mut *store (reborrow) to avoid move-after-use errors
        let result = call_reducer_typed(
            &instance,
            &mut store,
            &self.function_name,
            args_offset as i32,
            args_len as i32,
        );

        let (result_ptr, result_len) = result.map_err(|e| {
            NeonDBError::reducer_error(format!("WASM reducer call failed: {}", e))
        })?;

        // ---- 4. Read result from WASM memory ------------------------------
        let mem_slice = memory.data(&store);
        let start = result_ptr as usize;
        let end = start + result_len as usize;
        if end > mem_slice.len() {
            return Err(NeonDBError::reducer_error(
                "WASM reducer returned out-of-bounds memory range",
            ));
        }
        let result_bytes = mem_slice[start..end].to_vec();

        let json_str = std::str::from_utf8(&result_bytes).map_err(|e| {
            NeonDBError::SerializationError(format!("WASM result not valid UTF-8: {}", e))
        })?;
        let json_value: serde_json::Value =
            serde_json::from_str(json_str).map_err(|e| {
                NeonDBError::SerializationError(format!("WASM result not valid JSON: {}", e))
            })?;

        // ---- 5. Apply pending writes to ctx -------------------------------
        let pending = {
            let state = host_state.lock().unwrap();
            state.pending_sets.clone()
        };
        for (name, value) in pending {
            ctx.set_counter(name, value)?;
        }

        Ok(rmp_serde::to_vec(&json_value)?)
    }
}

impl ReducerBackend for WasmReducerBackend {
    fn execute(&self, ctx: &mut ReducerContext, args: &[u8]) -> Result<Vec<u8>> {
        self.call(ctx, args)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn read_string_from_memory(caller: &mut Caller<'_, ()>, ptr: u32, len: u32) -> Option<String> {
    let memory = caller.get_export("memory")?.into_memory()?;
    let data = memory.data(caller);
    let start = ptr as usize;
    let end = start + len as usize;
    if end > data.len() {
        return None;
    }
    std::str::from_utf8(&data[start..end])
        .ok()
        .map(|s| s.to_owned())
}

/// Try (i32, i32) -> (i32, i32) signature, then no-arg () -> (i32, i32) fallback.
/// Uses `&mut *store` reborrows to avoid move-after-use borrow errors.
fn call_reducer_typed(
    instance: &Instance,
    store: &mut Store<()>,
    name: &str,
    args_ptr: i32,
    args_len: i32,
) -> std::result::Result<(i32, i32), Box<dyn std::error::Error>> {
    if let Ok(f) = instance.get_typed_func::<(i32, i32), (i32, i32)>(&mut *store, name) {
        let result = f.call(&mut *store, (args_ptr, args_len))?;
        return Ok(result);
    }
    if let Ok(f) = instance.get_typed_func::<(), (i32, i32)>(&mut *store, name) {
        let result = f.call(&mut *store, ())?;
        return Ok(result);
    }
    Err(format!("No compatible '{}' export found in WASM module", name).into())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
    fn test_wasm_host_imports() {
        // IMPORTANT: WebAssembly spec requires all (import ...) declarations
        // to appear BEFORE any (memory ...) or (func ...) definitions.
        // Imports after memory declarations cause a WAT parse error.
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
        let r1 = backend.execute(&mut ctx, b"").unwrap();
        let v1: serde_json::Value = rmp_serde::from_slice(&r1).unwrap();
        assert_eq!(v1["new_value"], 3);
        ctx.commit().unwrap();

        std::fs::remove_file(&tmp).ok();
    }
}
