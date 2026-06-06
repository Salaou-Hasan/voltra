use crate::error::{NeonDBError, Result};
use crate::reducer::backend::ReducerBackend;
use crate::reducer::context::ReducerContext;
use boa_engine::{
    js_string, Context, JsNativeError, JsResult, JsValue, NativeFunction, Source,
};
use serde_json::Value;
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

// ---------------------------------------------------------------------------
// Shared state threaded through Boa host functions via Rc<RefCell<…>>
// ---------------------------------------------------------------------------

struct HostState {
    reads: std::collections::HashMap<String, std::collections::HashMap<String, Value>>,
    pending_sets: Vec<(String, String, i32)>,
}

impl HostState {
    fn new() -> Self {
        HostState {
            reads: std::collections::HashMap::new(),
            pending_sets: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

pub struct V8ReducerBackend {
    script: String,
    /// Reserved for future per-call Boa fuel/timeout enforcement.
    #[allow(dead_code)]
    timeout_ms: u64,
}

impl V8ReducerBackend {
    pub fn from_file(path: PathBuf, timeout_ms: u64) -> Result<Self> {
        let script = std::fs::read_to_string(&path)?;
        Ok(V8ReducerBackend { script, timeout_ms })
    }

    fn run(&self, ctx: &mut ReducerContext, args_json: Value) -> Result<Value> {
        // ---- 1. Pre-fetch all counters into the read cache ----------------
        let counters = ctx.list_counters()?;
        let mut host_state = HostState::new();
        let counter_map = host_state.reads.entry("counters".to_string()).or_default();
        for c in counters {
            counter_map.insert(
                c.name.clone(),
                serde_json::json!({ "id": c.id, "name": c.name, "value": c.value }),
            );
        }

        let host = Rc::new(RefCell::new(host_state));

        // ---- 2. Build Boa context ------------------------------------------
        let mut js_ctx = Context::default();

        // Inject __neondb_get(table, key) -> object | null
        {
            let host_ref = host.clone();
            // SAFETY: The closure is single-threaded (Boa runs on one thread),
            // captures only Rc<RefCell<…>> which is !Send, and does not
            // call any platform-unsafe operations.
            let get_fn = unsafe {
                NativeFunction::from_closure(move |_this, args, ctx| {
                    let table = args
                        .get(0)
                        .and_then(|v| v.as_string())
                        .and_then(|s| s.to_std_string().ok())
                        .unwrap_or_default();
                    let key = args
                        .get(1)
                        .and_then(|v| v.as_string())
                        .and_then(|s| s.to_std_string().ok())
                        .unwrap_or_default();

                    let state = host_ref.borrow();
                    let result = state
                        .reads
                        .get(table.as_str())
                        .and_then(|t| t.get(key.as_str()));

                    match result {
                        None => Ok(JsValue::null()),
                        Some(v) => json_to_js(v, ctx),
                    }
                })
            };
            js_ctx
                .register_global_callable(js_string!("__neondb_get"), 2, get_fn)
                .map_err(|e| NeonDBError::reducer_error(format!("Boa register get: {}", e)))?;
        }

        // Inject __neondb_set(table, key, value) -> void
        {
            let host_ref = host.clone();
            // SAFETY: same as above — single-threaded Rc<RefCell<…>> capture.
            let set_fn = unsafe {
                NativeFunction::from_closure(move |_this, args, _ctx| {
                    let table = args
                        .get(0)
                        .and_then(|v| v.as_string())
                        .and_then(|s| s.to_std_string().ok())
                        .unwrap_or_default();
                    let key = args
                        .get(1)
                        .and_then(|v| v.as_string())
                        .and_then(|s| s.to_std_string().ok())
                        .unwrap_or_default();
                    let value = args
                        .get(2)
                        .and_then(|v| v.as_number())
                        .unwrap_or(0.0) as i32;

                    host_ref.borrow_mut().pending_sets.push((table, key, value));
                    Ok(JsValue::undefined())
                })
            };
            js_ctx
                .register_global_callable(js_string!("__neondb_set"), 3, set_fn)
                .map_err(|e| NeonDBError::reducer_error(format!("Boa register set: {}", e)))?;
        }

        // ---- 3. Load the user script --------------------------------------
        js_ctx
            .eval(Source::from_bytes(self.script.as_bytes()))
            .map_err(|e| NeonDBError::reducer_error(format!("Script load error: {}", e)))?;

        // ---- 4. Serialize args and call reducer() -------------------------
        let args_str = serde_json::to_string(&args_json)?;
        let call_src = format!("reducer(JSON.parse({}))", js_escape(&args_str));
        let result_val = js_ctx
            .eval(Source::from_bytes(call_src.as_bytes()))
            .map_err(|e| NeonDBError::reducer_error(format!("Reducer call error: {}", e)))?;

        // ---- 5. Convert result to serde_json::Value -----------------------
        let result_json = js_to_json(&result_val, &mut js_ctx)
            .map_err(|e| NeonDBError::reducer_error(format!("Result conversion: {}", e)))?;

        // ---- 6. Apply pending writes --------------------------------------
        let pending = host.borrow().pending_sets.clone();
        for (table, key, value) in pending {
            if table == "counters" {
                ctx.set_counter(key, value)?;
            }
        }

        Ok(result_json)
    }
}

impl ReducerBackend for V8ReducerBackend {
    fn execute(&self, ctx: &mut ReducerContext, args: &[u8]) -> Result<Vec<u8>> {
        let args_json: Value = rmp_serde::from_slice(args)?;
        let result = self.run(ctx, args_json)?;
        Ok(rmp_serde::to_vec(&result)?)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn js_escape(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn json_to_js(value: &Value, ctx: &mut Context) -> JsResult<JsValue> {
    let json_str = serde_json::to_string(value)
        .map_err(|e| JsNativeError::error().with_message(format!("JSON encode: {}", e)))?;
    let src = format!("({})", json_str);
    ctx.eval(Source::from_bytes(src.as_bytes()))
        .map_err(|e| JsNativeError::error().with_message(format!("JSON to JS: {}", e)).into())
}

fn js_to_json(value: &JsValue, ctx: &mut Context) -> JsResult<Value> {
    if value.is_null() || value.is_undefined() {
        return Ok(Value::Null);
    }
    if let Some(b) = value.as_boolean() {
        return Ok(Value::Bool(b));
    }
    if let Some(n) = value.as_number() {
        return Ok(serde_json::json!(n));
    }
    if let Some(s) = value.as_string() {
        return Ok(Value::String(s.to_std_string().unwrap_or_default()));
    }

    let json_fn = ctx
        .eval(Source::from_bytes(b"JSON.stringify"))
        .map_err(|e| JsNativeError::error().with_message(format!("JSON.stringify: {}", e)))?;
    if let Some(f) = json_fn.as_callable() {
        let result = f.call(&JsValue::undefined(), &[value.clone()], ctx)?;
        if let Some(s) = result.as_string() {
            let raw = s.to_std_string().unwrap_or_default();
            return serde_json::from_str(&raw).map_err(|e| {
                JsNativeError::error()
                    .with_message(format!("JSON parse: {}", e))
                    .into()
            });
        }
    }
    Ok(Value::Null)
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
        ReducerContext::new(Arc::new(TableStore::new()), 2000)
    }

    #[test]
    fn test_v8_increment_from_zero() {
        let script = r#"
function reducer(args) {
    var current = __neondb_get("counters", args.name);
    var value = (current ? current.value : 0) + args.delta;
    __neondb_set("counters", args.name, value);
    return { new_value: value, timestamp: 0 };
}
"#;
        let path = std::env::temp_dir().join("test_v8_reducer.js");
        std::fs::write(&path, script).unwrap();

        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();

        let args = rmp_serde::to_vec(&serde_json::json!({"name": "score", "delta": 5})).unwrap();
        let result_bytes = backend.execute(&mut ctx, &args).unwrap();
        let result: Value = rmp_serde::from_slice(&result_bytes).unwrap();

        assert_eq!(result["new_value"], 5);
        ctx.commit().unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_increment_accumulates() {
        let script = r#"
function reducer(args) {
    var current = __neondb_get("counters", args.name);
    var value = (current ? current.value : 0) + args.delta;
    __neondb_set("counters", args.name, value);
    return { new_value: value, timestamp: 0 };
}
"#;
        let path = std::env::temp_dir().join("test_v8_accum.js");
        std::fs::write(&path, script).unwrap();
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();

        let a1 = rmp_serde::to_vec(&serde_json::json!({"name": "hp", "delta": 10})).unwrap();
        backend.execute(&mut ctx, &a1).unwrap();
        ctx.commit().unwrap();

        let a2 = rmp_serde::to_vec(&serde_json::json!({"name": "hp", "delta": 5})).unwrap();
        let r2 = backend.execute(&mut ctx, &a2).unwrap();
        let v: Value = rmp_serde::from_slice(&r2).unwrap();
        assert_eq!(v["new_value"], 15);

        std::fs::remove_file(&path).ok();
    }
}
