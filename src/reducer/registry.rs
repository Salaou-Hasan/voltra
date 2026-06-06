use crate::error::{NeonDBError, Result};
use crate::reducer::backend::ReducerBackend;
use crate::reducer::native::NativeReducerBackend;
use crate::reducer::v8::V8ReducerBackend;
use crate::reducer::wasm::WasmReducerBackend;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Which runtime backs this reducer.
#[derive(Clone, Debug, PartialEq)]
pub enum ReducerRuntime {
    Native,
    Wasm,
    V8,
}

/// Metadata sidecar file (`<name>.json`) for a module.
/// `runtime` is parsed from JSON for forward-compatibility but the actual
/// runtime is inferred from the file extension for now.
#[derive(Debug, Deserialize)]
struct ModuleMetadata {
    pub name: Option<String>,
    #[allow(dead_code)]
    pub runtime: Option<String>,
    pub entrypoint: Option<String>,
    pub file: Option<String>,
    pub timeout_ms: Option<u64>,
}

pub struct ReducerDefinition {
    pub name: String,
    pub runtime: ReducerRuntime,
    pub backend: Box<dyn ReducerBackend>,
}

pub struct ReducerRegistry {
    reducers: HashMap<String, ReducerDefinition>,
}

impl ReducerRegistry {
    pub fn new() -> Result<Self> {
        let mut registry = ReducerRegistry {
            reducers: HashMap::new(),
        };

        registry.register_native(
            "increment",
            NativeReducerBackend::new(NativeReducerBackend::increment_reducer),
        );

        let modules_path = PathBuf::from("modules");
        if modules_path.is_dir() {
            if let Err(e) = registry.load_modules(&modules_path) {
                log::warn!("Failed to auto-load modules from {:?}: {}", modules_path, e);
            }
        }

        Ok(registry)
    }

    fn load_modules(&mut self, root: &Path) -> Result<()> {
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                self.load_modules(&path)?;
            } else if let Err(e) = self.register_module(&path) {
                log::debug!("Skipping {:?}: {}", path, e);
            }
        }
        Ok(())
    }

    fn register_module(&mut self, path: &Path) -> Result<()> {
        let ext = path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_lowercase();

        if ext == "json" {
            if let Some(metadata) = self.load_metadata(path)? {
                return self.register_module_from_metadata(path, metadata);
            }
            return Ok(());
        }

        if !["js", "wasm", "wat"].contains(&ext.as_str()) {
            return Ok(());
        }

        // TODO-005: WASM-first — if a pre-compiled .wasm companion exists for this
        // .js file, prefer it.  The .wasm is produced by `neondb build` via javy.
        // This transparently upgrades JS reducers to Wasmtime JIT when available,
        // with no changes needed to the reducer source code.
        let ext = if ext == "js" {
            let wasm_path = path.with_extension("wasm");
            if wasm_path.exists() {
                log::info!("Using pre-compiled WASM for JS reducer at {:?}", wasm_path);
                return self.register_module(&wasm_path);
            }
            ext
        } else {
            ext
        };

        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| NeonDBError::invalid_argument("Invalid module file name"))?
            .to_string();

        if self.reducers.contains_key(&name) {
            log::debug!(
                "Reducer '{}' already registered, skipping {}",
                name,
                path.display()
            );
            return Ok(());
        }

        let definition = self.create_definition(&name, path, &ext, None, None)?;
        log::info!(
            "Registered {} reducer '{}' from {}",
            format!("{:?}", definition.runtime).to_lowercase(),
            name,
            path.display()
        );
        self.reducers.insert(name, definition);
        Ok(())
    }

    fn load_metadata(&self, path: &Path) -> Result<Option<ModuleMetadata>> {
        let contents = std::fs::read_to_string(path)?;
        let metadata: ModuleMetadata = serde_json::from_str(&contents).map_err(|e| {
            NeonDBError::invalid_argument(format!("Invalid module metadata: {}", e))
        })?;
        Ok(Some(metadata))
    }

    fn register_module_from_metadata(
        &mut self,
        sidecar_path: &Path,
        metadata: ModuleMetadata,
    ) -> Result<()> {
        let module_file = metadata
            .file
            .ok_or_else(|| NeonDBError::invalid_argument("Module metadata missing 'file' field"))?;
        let module_path = sidecar_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(&module_file);

        let name = metadata.name.unwrap_or_else(|| {
            module_path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned()
        });

        if self.reducers.contains_key(&name) {
            log::debug!(
                "Reducer '{}' already registered, skipping metadata module",
                name
            );
            return Ok(());
        }

        let ext = module_path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_lowercase();

        let definition = self.create_definition(
            &name,
            &module_path,
            &ext,
            metadata.entrypoint.as_deref(),
            metadata.timeout_ms,
        )?;
        log::info!(
            "Registered {} reducer '{}' (via metadata) from {}",
            format!("{:?}", definition.runtime).to_lowercase(),
            name,
            module_path.display()
        );
        self.reducers.insert(name, definition);
        Ok(())
    }

    fn create_definition(
        &self,
        name: &str,
        path: &Path,
        ext: &str,
        entrypoint: Option<&str>,
        timeout_ms: Option<u64>,
    ) -> Result<ReducerDefinition> {
        let timeout = timeout_ms.unwrap_or(5_000);

        match ext {
            "js" => {
                let backend =
                    V8ReducerBackend::from_file(path.to_path_buf(), timeout).map_err(|e| {
                        NeonDBError::reducer_error(format!(
                            "Failed to load JS module '{}': {}",
                            path.display(),
                            e
                        ))
                    })?;
                Ok(ReducerDefinition {
                    name: name.to_string(),
                    runtime: ReducerRuntime::V8,
                    backend: Box::new(backend),
                })
            }
            "wasm" | "wat" => {
                let fn_name = entrypoint.unwrap_or("reducer");
                let backend =
                    WasmReducerBackend::from_file(path.to_path_buf(), fn_name).map_err(|e| {
                        NeonDBError::reducer_error(format!(
                            "Failed to load WASM module '{}': {}",
                            path.display(),
                            e
                        ))
                    })?;
                Ok(ReducerDefinition {
                    name: name.to_string(),
                    runtime: ReducerRuntime::Wasm,
                    backend: Box::new(backend),
                })
            }
            other => Err(NeonDBError::invalid_argument(format!(
                "Unsupported module extension: '{}'",
                other
            ))),
        }
    }

    pub fn register_native(&mut self, name: &str, backend: NativeReducerBackend) {
        self.reducers.insert(
            name.to_string(),
            ReducerDefinition {
                name: name.to_string(),
                runtime: ReducerRuntime::Native,
                backend: Box::new(backend),
            },
        );
    }

    pub fn execute(
        &self,
        reducer_name: &str,
        ctx: &mut crate::reducer::context::ReducerContext,
        args: &[u8],
    ) -> Result<Vec<u8>> {
        let definition = self.reducers.get(reducer_name).ok_or_else(|| {
            NeonDBError::reducer_error(format!("Unknown reducer: '{}'", reducer_name))
        })?;
        definition.backend.execute(ctx, args)
    }

    pub fn list_reducers(&self) -> Vec<String> {
        self.reducers.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reducer::context::{IncrementResult, ReducerContext};
    use crate::table::TableStore;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    fn make_ctx() -> ReducerContext {
        ReducerContext::new(Arc::new(TableStore::new()), 100)
    }

    /// Mirror of the private IncrementArgs in native.rs.
    /// Must be encoded with rmp_serde::to_vec on the concrete struct (not via
    /// serde_json::json!()) because rmp_serde 1.x serializes structs in array
    /// format (positional fields, no keys). serde_json::Value encodes as a
    /// msgpack map which cannot be deserialized back into a named struct.
    #[derive(Serialize, Deserialize)]
    struct IncrementArgs {
        name: String,
        delta: i32,
    }

    #[test]
    fn test_registry_has_native_increment() {
        let registry = ReducerRegistry::new().unwrap();
        assert!(registry.list_reducers().contains(&"increment".to_string()));
    }

    #[test]
    fn test_registry_unknown_reducer_returns_error() {
        let registry = ReducerRegistry::new().unwrap();
        let mut ctx = make_ctx();
        let err = registry
            .execute("does_not_exist", &mut ctx, b"")
            .unwrap_err();
        assert!(matches!(err, NeonDBError::ReducerError(_)));
    }

    #[test]
    fn test_registry_executes_native_increment() {
        let registry = ReducerRegistry::new().unwrap();
        let mut ctx = make_ctx();

        // Encode args as the concrete struct — rmp_serde 1.x uses array format
        // for structs, which matches what NativeReducerBackend::increment_reducer
        // expects when it calls rmp_serde::from_slice::<IncrementArgs>().
        //
        // IMPORTANT: decode the result as IncrementResult (concrete struct),
        // NOT as serde_json::Value. The result bytes are a msgpack array
        // [new_value, timestamp] — deserializing that as serde_json::Value
        // gives a JSON array [10, 100], and array["new_value"] == Null.
        // Decoding as IncrementResult gives the correctly typed struct.
        let args = rmp_serde::to_vec(&IncrementArgs {
            name: "hp".to_string(),
            delta: 10,
        })
        .unwrap();

        let result_bytes = registry.execute("increment", &mut ctx, &args).unwrap();

        // Decode as the concrete type — not as serde_json::Value.
        let result: IncrementResult = rmp_serde::from_slice(&result_bytes).unwrap();
        assert_eq!(result.new_value, 10);
    }

    #[test]
    fn test_registry_loads_js_module_if_present() {
        let dir = PathBuf::from("modules");
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("test_registry_js.js");
        std::fs::write(
            &path,
            r#"function reducer(args) {
  var v = ((__neondb_get("counters", args.name) || {}).value || 0) + args.delta;
  __neondb_set("counters", args.name, v);
  return { new_value: v, timestamp: 0 };
}"#,
        )
        .unwrap();

        let registry = ReducerRegistry::new().unwrap();
        assert!(registry
            .list_reducers()
            .contains(&"test_registry_js".to_string()));

        let mut ctx = make_ctx();
        let args = rmp_serde::to_vec(&serde_json::json!({"name": "mana", "delta": 7})).unwrap();
        let result = registry
            .execute("test_registry_js", &mut ctx, &args)
            .unwrap();
        let decoded: serde_json::Value = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(decoded["new_value"], 7);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_registry_loads_wasm_module_if_present() {
        let dir = PathBuf::from("modules");
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("test_registry_wasm.wat");
        // Imports must come before memory and func definitions (WASM spec).
        std::fs::write(
            &path,
            r#"(module
  (memory (export "memory") 1)
  (data (i32.const 0) "{\"new_value\":42,\"timestamp\":0}")
  (func (export "reducer") (param i32 i32) (result i32 i32)
    i32.const 0
    i32.const 30
  )
)"#,
        )
        .unwrap();

        let registry = ReducerRegistry::new().unwrap();
        assert!(registry
            .list_reducers()
            .contains(&"test_registry_wasm".to_string()));

        let mut ctx = make_ctx();
        let result = registry
            .execute("test_registry_wasm", &mut ctx, b"")
            .unwrap();
        let decoded: serde_json::Value = rmp_serde::from_slice(&result).unwrap();
        assert_eq!(decoded["new_value"], 42);
        std::fs::remove_file(&path).ok();
    }
}
