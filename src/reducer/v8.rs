// ============================================================================
// v8.rs — Boa JS reducer backend
//
// Session 33 fixes:
//   - __neondb_set writes eagerly to ReducerContext so __neondb_get in the
//     same reducer call sees the write immediately (read-your-own-writes).
//   - __neondb_delete likewise deletes eagerly.
//   - Flush loop at end only processes any remaining deletes; sets are no-op.
// ============================================================================

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

// ── Shared host state ─────────────────────────────────────────────────────────

#[allow(dead_code)]
struct PendingWrite {
    table: String,
    key: String,
    is_delete: bool,
}

struct HostState {
    pending_writes: Vec<PendingWrite>,
}

impl HostState {
    fn new() -> Self {
        HostState { pending_writes: Vec::new() }
    }
}

// ── Backend ───────────────────────────────────────────────────────────────────

pub struct V8ReducerBackend {
    script: String,
    #[allow(dead_code)]
    timeout_ms: u64,
}

impl V8ReducerBackend {
    pub fn from_file(path: PathBuf, timeout_ms: u64) -> Result<Self> {
        let script = std::fs::read_to_string(&path)?;
        Ok(V8ReducerBackend { script, timeout_ms })
    }

    fn run(&self, ctx: &mut ReducerContext, args_json: Value) -> Result<Value> {
        let host = Rc::new(RefCell::new(HostState::new()));
        let mut js_ctx = Context::default();

        // ── Expose caller identity as globals ─────────────────────────────────
        {
            let id_val = JsValue::String(boa_engine::JsString::from(ctx.caller_id.as_str()));
            js_ctx.register_global_property(
                js_string!("__neondb_caller_id"),
                id_val,
                boa_engine::property::Attribute::all(),
            ).map_err(|e| NeonDBError::reducer_error(format!("Boa global caller_id: {}", e)))?;
        }
        {
            let role_val = JsValue::String(boa_engine::JsString::from(ctx.caller_role.as_str()));
            js_ctx.register_global_property(
                js_string!("__neondb_caller_role"),
                role_val,
                boa_engine::property::Attribute::all(),
            ).map_err(|e| NeonDBError::reducer_error(format!("Boa global caller_role: {}", e)))?;
        }

        // ── __neondb_get(table, key) → object | null ──────────────────────────
        {
            let ctx_ptr = ctx as *mut ReducerContext;
            // SAFETY: Boa runs synchronously on one thread; closure lifetime is
            // bounded by js_ctx.eval() calls below; ctx outlives js_ctx.
            let get_fn = unsafe {
                NativeFunction::from_closure(move |_this, args, js_c| {
                    let table = args.get(0)
                        .and_then(|v| v.as_string())
                        .and_then(|s| s.to_std_string().ok())
                        .unwrap_or_default();
                    let key = args.get(1)
                        .and_then(|v| v.as_string())
                        .and_then(|s| s.to_std_string().ok())
                        .unwrap_or_default();
                    if table.is_empty() || key.is_empty() {
                        return Ok(JsValue::null());
                    }
                    let ctx_ref = &mut *ctx_ptr;
                    match ctx_ref.get_row(&table, &key) {
                        Ok(Some(v)) => json_to_js(&v, js_c),
                        _ => Ok(JsValue::null()),
                    }
                })
            };
            js_ctx.register_global_callable(js_string!("__neondb_get"), 2, get_fn)
                .map_err(|e| NeonDBError::reducer_error(format!("Boa register get: {}", e)))?;
        }

        // ── __neondb_get_all(table) → array ───────────────────────────────────
        {
            let ctx_ptr = ctx as *mut ReducerContext;
            // SAFETY: same as above.
            let get_all_fn = unsafe {
                NativeFunction::from_closure(move |_this, args, js_c| {
                    let table = args.get(0)
                        .and_then(|v| v.as_string())
                        .and_then(|s| s.to_std_string().ok())
                        .unwrap_or_default();
                    if table.is_empty() { return Ok(JsValue::null()); }
                    let ctx_ref = &mut *ctx_ptr;
                    match ctx_ref.tables.list_rows_with_keys(&table) {
                        Ok(rows) => {
                            let arr = Value::Array(rows.into_iter().map(|(_, v)| v).collect());
                            json_to_js(&arr, js_c)
                        }
                        Err(_) => Ok(JsValue::null()),
                    }
                })
            };
            js_ctx.register_global_callable(js_string!("__neondb_get_all"), 1, get_all_fn)
                .map_err(|e| NeonDBError::reducer_error(format!("Boa register get_all: {}", e)))?;
        }

        // ── __neondb_set(table, key, value) → void ────────────────────────────
        // Writes eagerly to ctx so that __neondb_get within the same reducer
        // call sees the newly written row (read-your-own-writes semantics).
        {
            let host_ref = host.clone();
            let ctx_ptr = ctx as *mut ReducerContext;
            // SAFETY: Boa is single-threaded; ctx outlives js_ctx.
            let set_fn = unsafe {
                NativeFunction::from_closure(move |_this, args, js_c| {
                    let table = args.get(0)
                        .and_then(|v| v.as_string())
                        .and_then(|s| s.to_std_string().ok())
                        .unwrap_or_default();
                    let key = args.get(1)
                        .and_then(|v| v.as_string())
                        .and_then(|s| s.to_std_string().ok())
                        .unwrap_or_default();
                    if table.is_empty() || key.is_empty() {
                        return Ok(JsValue::undefined());
                    }
                    let js_val = args.get(2).cloned().unwrap_or(JsValue::undefined());
                    let json_val = js_to_json(&js_val, js_c).unwrap_or(Value::Null);

                    // Apply write immediately to ReducerContext.
                    let ctx_ref = &mut *ctx_ptr;

                    if table == "counters" {
                        match &json_val {
                            Value::Number(n) => {
                                let amount = n.as_i64().unwrap_or(0) as i32;
                                ctx_ref
                                    .set_counter(key.clone(), amount)
                                    .map_err(|e| JsNativeError::error().with_message(e.to_string()))?;
                            }
                            _ => {
                                ctx_ref
                                    .set_row(table.clone(), key.clone(), json_val.clone())
                                    .map_err(|e| JsNativeError::error().with_message(e.to_string()))?;
                            }
                        }
                    } else {
                        ctx_ref
                            .set_row(table.clone(), key.clone(), json_val.clone())
                            .map_err(|e| JsNativeError::error().with_message(e.to_string()))?;
                    }

                    host_ref.borrow_mut().pending_writes.push(PendingWrite {
                        table,
                        key,
                        is_delete: false,
                    });
                    Ok(JsValue::undefined())
                })
            };
            js_ctx.register_global_callable(js_string!("__neondb_set"), 3, set_fn)
                .map_err(|e| NeonDBError::reducer_error(format!("Boa register set: {}", e)))?;
        }

        // ── __neondb_delete(table, key) → void ────────────────────────────────
        // Deletes eagerly so __neondb_get after a delete correctly returns null.
        {
            let host_ref = host.clone();
            let ctx_ptr = ctx as *mut ReducerContext;
            // SAFETY: Boa is single-threaded; ctx outlives js_ctx.
            let del_fn = unsafe {
                NativeFunction::from_closure(move |_this, args, _js_c| {
                    let table = args.get(0)
                        .and_then(|v| v.as_string())
                        .and_then(|s| s.to_std_string().ok())
                        .unwrap_or_default();
                    let key = args.get(1)
                        .and_then(|v| v.as_string())
                        .and_then(|s| s.to_std_string().ok())
                        .unwrap_or_default();
                    if !table.is_empty() && !key.is_empty() {
                        let ctx_ref = &mut *ctx_ptr;
                        let _ = ctx_ref.delete_row(table.clone(), key.clone());
                        host_ref.borrow_mut().pending_writes.push(PendingWrite {
                            table,
                            key,
                            is_delete: true,
                        });
                    }
                    Ok(JsValue::undefined())
                })
            };
            js_ctx.register_global_callable(js_string!("__neondb_delete"), 2, del_fn)
                .map_err(|e| NeonDBError::reducer_error(format!("Boa register delete: {}", e)))?;
        }

        // ── __neondb_ai_generate(prompt) → JSON string | null ─────────────────
        // Calls the Anthropic API synchronously via reqwest::blocking.
        // Requires ANTHROPIC_API_KEY environment variable.
        {
            // SAFETY: single-threaded Boa context.
            let ai_fn = unsafe {
                NativeFunction::from_closure(move |_this, args, _js_c| {
                    let prompt = args.get(0)
                        .and_then(|v| v.as_string())
                        .and_then(|s| s.to_std_string().ok())
                        .unwrap_or_default();
                    if prompt.is_empty() { return Ok(JsValue::null()); }

                    let api_key = match std::env::var("ANTHROPIC_API_KEY") {
                        Ok(k) => k,
                        Err(_) => return Ok(JsValue::null()),
                    };

                    let body = serde_json::json!({
                        "model": "claude-haiku-4-5-20251001",
                        "max_tokens": 1024,
                        "system": "You are a game NPC designer. Always respond with valid JSON only — no markdown, no explanation, just the JSON object.",
                        "messages": [{ "role": "user", "content": prompt }]
                    });

                    let result = reqwest::blocking::Client::new()
                        .post("https://api.anthropic.com/v1/messages")
                        .header("x-api-key", &api_key)
                        .header("anthropic-version", "2023-06-01")
                        .header("content-type", "application/json")
                        .json(&body)
                        .send();

                    match result {
                        Ok(resp) => match resp.json::<serde_json::Value>() {
                            Ok(json) => {
                                let text = json["content"][0]["text"]
                                    .as_str().unwrap_or("").to_string();
                                Ok(JsValue::String(boa_engine::JsString::from(text.as_str())))
                            }
                            Err(_) => Ok(JsValue::null()),
                        },
                        Err(_) => Ok(JsValue::null()),
                    }
                })
            };
            js_ctx.register_global_callable(js_string!("__neondb_ai_generate"), 1, ai_fn)
                .map_err(|e| NeonDBError::reducer_error(format!("Boa register ai_generate: {}", e)))?;
        }

        // ── Load and run script ───────────────────────────────────────────────
        js_ctx.eval(Source::from_bytes(self.script.as_bytes()))
            .map_err(|e| NeonDBError::reducer_error(format!("Script load error: {}", e)))?;

        let args_str = serde_json::to_string(&args_json)?;
        let call_src = format!("reducer(JSON.parse({}))", js_escape(&args_str));
        let result_val = js_ctx.eval(Source::from_bytes(call_src.as_bytes()))
            .map_err(|e| NeonDBError::reducer_error(format!("Reducer call error: {}", e)))?;

        let result_json = js_to_json(&result_val, &mut js_ctx)
            .map_err(|e| NeonDBError::reducer_error(format!("Result conversion: {}", e)))?;

        // All writes were applied eagerly; pending_writes is just a log now.
        // No second pass needed.
        drop(host);

        Ok(result_json)
    }
}

impl ReducerBackend for V8ReducerBackend {
    fn execute(&self, ctx: &mut ReducerContext, args: &[u8]) -> Result<Vec<u8>> {
        // Empty args bytes (scheduler with no args_json) → empty array.
        let args_json: Value = if args.is_empty() {
            Value::Array(vec![])
        } else {
            rmp_serde::from_slice(args).unwrap_or(Value::Array(vec![]))
        };
        let result = self.run(ctx, args_json)?;
        Ok(rmp_serde::to_vec(&result)?)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

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
                JsNativeError::error().with_message(format!("JSON parse: {}", e)).into()
            });
        }
    }
    Ok(Value::Null)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reducer::context::ReducerContext;
    use crate::table::TableStore;
    use std::sync::Arc;

    fn make_ctx() -> ReducerContext {
        ReducerContext::new(Arc::new(TableStore::new()), 2000)
    }

    fn write_tmp(name: &str, script: &str) -> PathBuf {
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, script).unwrap();
        path
    }

    #[test]
    fn test_v8_counter_set_numeric() {
        let path = write_tmp("test_v8_counter_numeric.js", r#"
function reducer(args) {
    var cur = __neondb_get("counters", args[0]);
    var val = (cur ? cur.value : 0) + (args[1] || 1);
    __neondb_set("counters", args[0], val);
    return { ok: true, value: val };
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();
        let args = rmp_serde::to_vec(&serde_json::json!(["score", 5])).unwrap();
        let res_bytes = backend.execute(&mut ctx, &args).unwrap();
        let res: Value = rmp_serde::from_slice(&res_bytes).unwrap();
        assert_eq!(res["value"], 5);
        ctx.commit().unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_set_and_get_json_object() {
        let path = write_tmp("test_v8_json_obj.js", r#"
function reducer(args) {
    __neondb_set("players", args[0], { hp: 200, status: "alive", x: args[1] });
    var p = __neondb_get("players", args[0]);
    return { ok: true, hp: p ? p.hp : -1, status: p ? p.status : "none" };
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();
        let args = rmp_serde::to_vec(&serde_json::json!(["alice", 42])).unwrap();
        let res_bytes = backend.execute(&mut ctx, &args).unwrap();
        let res: Value = rmp_serde::from_slice(&res_bytes).unwrap();
        assert_eq!(res["hp"], 200);
        assert_eq!(res["status"], "alive");
        ctx.commit().unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_empty_args_does_not_crash() {
        let path = write_tmp("test_v8_empty_args.js", r#"
function reducer(args) {
    var tick = __neondb_get("world_state", "tick") || { count: 0 };
    tick.count = (tick.count || 0) + 1;
    __neondb_set("world_state", "tick", tick);
    return { ok: true, tick: tick.count };
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();
        let res_bytes = backend.execute(&mut ctx, &[]).unwrap();
        let res: Value = rmp_serde::from_slice(&res_bytes).unwrap();
        assert_eq!(res["ok"], true);
        assert_eq!(res["tick"], 1);
        ctx.commit().unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_delete_row() {
        let path = write_tmp("test_v8_delete.js", r#"
function reducer(args) {
    __neondb_set("items", "sword", { name: "sword", qty: 1 });
    __neondb_delete("items", "sword");
    var after = __neondb_get("items", "sword");
    return { deleted: after === null };
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();
        let args = rmp_serde::to_vec(&serde_json::json!([])).unwrap();
        let res_bytes = backend.execute(&mut ctx, &args).unwrap();
        let res: Value = rmp_serde::from_slice(&res_bytes).unwrap();
        assert_eq!(res["deleted"], true);
        ctx.commit().unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_caller_identity_accessible() {
        let path = write_tmp("test_v8_caller.js", r#"
function reducer(args) {
    return { caller_id: __neondb_caller_id, caller_role: __neondb_caller_role };
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();
        ctx.caller_id   = "player-42".to_string();
        ctx.caller_role = "admin".to_string();
        let args = rmp_serde::to_vec(&serde_json::json!([])).unwrap();
        let res_bytes = backend.execute(&mut ctx, &args).unwrap();
        let res: Value = rmp_serde::from_slice(&res_bytes).unwrap();
        assert_eq!(res["caller_id"],   "player-42");
        assert_eq!(res["caller_role"], "admin");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_world_tick_pattern() {
        let path = write_tmp("test_v8_world_tick.js", r#"
function reducer(args) {
    var tick = __neondb_get("world_state", "tick") || { count: 0, started_at: Date.now() };
    tick.count += 1;
    tick.last_tick = Date.now();
    __neondb_set("world_state", "tick", tick);
    return { ok: true, tick: tick.count };
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();
        let res1 = backend.execute(&mut ctx, &[]).unwrap();
        ctx.commit().unwrap();
        let res2 = backend.execute(&mut ctx, &[]).unwrap();
        ctx.commit().unwrap();
        let v1: Value = rmp_serde::from_slice(&res1).unwrap();
        let v2: Value = rmp_serde::from_slice(&res2).unwrap();
        assert_eq!(v1["tick"], 1);
        assert_eq!(v2["tick"], 2);
        std::fs::remove_file(&path).ok();
    }
}
