// ============================================================================
// v8.rs — rquickjs JS reducer backend
//
// QuickJS (via rquickjs) replaces Boa.  QuickJS is a bytecode-compiled engine
// and is ~10-20× faster than Boa's AST-walking interpreter.
//
// ARCHITECTURE:
//   One Runtime per OS thread (thread-local, created once).
//   One Context per (thread, script_path) — created on first call, reused.
//   Per-call: update identity globals + call reducer(args).  No engine init.
//
// HOST API / BRIDGE PATTERN:
//   rquickjs's Value<'js> is invariant over 'js, making it impossible to
//   return from a closure registered via Func::from without explicit lifetime
//   annotation (which closures don't support).  Solution: raw host functions
//   return Option<String> (JSON) with no lifetime, and a JS preamble wraps
//   them with JSON.parse/stringify on the JS side.  This is zero-overhead for
//   QuickJS (native C JSON implementation).
//
//   Raw fns (Rust)         Wrapper (JS preamble)       User reducer sees
//   __neondb_get_raw   →   __neondb_get(t,k)           object | null
//   __neondb_get_all_raw → __neondb_get_all(t)         array  | null
//   __neondb_set_raw   →   __neondb_set(t,k,v)         void
//   __neondb_delete    →   (direct, no wrapper needed) void
//   __neondb_ai_generate→  __neondb_ai_generate(p)     string | null
//   globals: __neondb_caller_id, __neondb_caller_role  string
//
// SANDBOX:
//   Memory limit: 64 MiB per runtime (enforced by QuickJS).
//   CPU limit: wall-clock deadline enforced via QuickJS interrupt handler —
//     an infinite-loop reducer is killed after `timeout_ms` and the worker
//     thread survives to serve the next call.
//   Args/result byte caps enforced here.
// ============================================================================

use crate::error::{NeonDBError, Result};
use crate::reducer::backend::ReducerBackend;
use crate::reducer::context::ReducerContext;
use rquickjs::{context::EvalOptions, function::Func, Context, Ctx, Function, Object, Runtime};
use serde_json::Value;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

// ── JS preamble injected before every user script ─────────────────────────────
// Wraps raw string-returning host functions with JSON.parse/stringify so
// reducers receive and return real JS objects.

const JS_PREAMBLE: &str = r#"
var __neondb_get = function(t, k) {
    var r = __neondb_get_raw(t, k);
    return (r != null && r !== undefined) ? JSON.parse(r) : null;
};
var __neondb_get_all = function(t) {
    var r = __neondb_get_all_raw(t);
    return (r != null && r !== undefined) ? JSON.parse(r) : [];
};
var __neondb_set = function(t, k, v) {
    __neondb_set_raw(t, k, JSON.stringify(v));
};
var __neondb_ai_generate = function(p) {
    var r = __neondb_ai_generate_raw(p);
    if (r == null || r === undefined) return null;
    try { return JSON.parse(r); } catch(e) { return r; }
};
// Ergonomic aliases for template reducers
var db = {
    get:    function(t, k) { return __neondb_get(t, k); },
    set:    function(t, k, v) { __neondb_set(t, k, v); },
    delete: function(t, k) { __neondb_delete(t, k); },
    all:    function(t) { return __neondb_get_all(t); }
};
var caller = {
    get id()   { return __neondb_caller_id; },
    get role() { return __neondb_caller_role; }
};
"#;

// ── Thread-local state ────────────────────────────────────────────────────────

thread_local! {
    static QJS_RT: RefCell<Option<Runtime>> = const { RefCell::new(None) };

    // One warm Context per (thread, script_path).
    static QJS_CTXS: RefCell<HashMap<String, Context>> =
        RefCell::new(HashMap::new());

    // Raw pointer to the live ReducerContext — set before JS call, cleared after.
    // SAFETY: valid for the entire synchronous eval of reducer(args).
    static CURRENT_CTX: Cell<*mut ReducerContext> = const { Cell::new(std::ptr::null_mut()) };

    // Wall-clock deadline for the currently-executing JS call.  Read by the
    // QuickJS interrupt handler (which fires periodically during execution);
    // when `Instant::now()` passes the deadline the handler returns `true`
    // and QuickJS aborts the script with an "interrupted" error.
    static QJS_DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// RAII guard: sets the JS execution deadline on construction, clears on drop
/// (even if the call panics), so a stale deadline can never leak into the
/// next reducer call on this thread.
struct DeadlineGuard;

impl DeadlineGuard {
    fn arm(timeout_ms: u64) -> Self {
        let deadline = Instant::now() + std::time::Duration::from_millis(timeout_ms.max(1));
        QJS_DEADLINE.with(|d| d.set(Some(deadline)));
        DeadlineGuard
    }
    fn expired() -> bool {
        QJS_DEADLINE.with(|d| d.get().map(|dl| Instant::now() >= dl).unwrap_or(false))
    }
}

impl Drop for DeadlineGuard {
    fn drop(&mut self) {
        QJS_DEADLINE.with(|d| d.set(None));
    }
}

fn ensure_runtime() -> Result<()> {
    QJS_RT.with(|cell| {
        let mut rt = cell.borrow_mut();
        if rt.is_none() {
            let new_rt = Runtime::new()
                .map_err(|e| NeonDBError::reducer_error(format!("QJS runtime: {}", e)))?;
            new_rt.set_memory_limit(64 * 1024 * 1024);
            // CPU watchdog: QuickJS invokes this periodically mid-execution.
            // Returning `true` aborts the running script.  No deadline armed
            // (None) means never interrupt — e.g. during context build.
            new_rt.set_interrupt_handler(Some(Box::new(DeadlineGuard::expired)));
            *rt = Some(new_rt);
        }
        Ok(())
    })
}

// ── Raw host functions — return String/() to avoid Value<'js> lifetime issues ─

fn host_get_raw(_ctx: Ctx<'_>, table: String, key: String) -> rquickjs::Result<Option<String>> {
    let ptr = CURRENT_CTX.with(|c| c.get());
    if ptr.is_null() || table.is_empty() || key.is_empty() { return Ok(None); }
    let rctx = unsafe { &mut *ptr };
    match rctx.get_row(&table, &key) {
        Ok(Some(v)) => Ok(Some(serde_json::to_string(&v).unwrap_or_default())),
        _ => Ok(None),
    }
}

fn host_get_all_raw(_ctx: Ctx<'_>, table: String) -> rquickjs::Result<Option<String>> {
    let ptr = CURRENT_CTX.with(|c| c.get());
    if ptr.is_null() || table.is_empty() { return Ok(None); }
    let rctx = unsafe { &mut *ptr };
    match rctx.tables.list_rows_with_keys(&table) {
        Ok(rows) => {
            let arr = Value::Array(rows.into_iter().map(|(_, v)| v).collect());
            Ok(Some(serde_json::to_string(&arr).unwrap_or_default()))
        }
        Err(_) => Ok(None),
    }
}

fn host_set_raw(_ctx: Ctx<'_>, table: String, key: String, json_str: String) -> rquickjs::Result<()> {
    let ptr = CURRENT_CTX.with(|c| c.get());
    if ptr.is_null() || table.is_empty() || key.is_empty() { return Ok(()); }
    let rctx = unsafe { &mut *ptr };
    let json_val: Value = serde_json::from_str(&json_str).unwrap_or(Value::Null);
    if table == "counters" {
        if let Value::Number(n) = &json_val {
            let amount = n.as_i64().unwrap_or(0) as i32;
            rctx.set_counter(key, amount)
                .map_err(|e| rquickjs::Error::new_from_js_message("value", "counter", e.to_string()))?;
            return Ok(());
        }
    }
    rctx.set_row(table, key, json_val)
        .map_err(|e| rquickjs::Error::new_from_js_message("value", "row", e.to_string()))?;
    Ok(())
}

fn host_delete(_ctx: Ctx<'_>, table: String, key: String) -> rquickjs::Result<()> {
    let ptr = CURRENT_CTX.with(|c| c.get());
    if ptr.is_null() || table.is_empty() || key.is_empty() { return Ok(()); }
    let rctx = unsafe { &mut *ptr };
    let _ = rctx.delete_row(table, key);
    Ok(())
}

fn host_ai_generate_raw(_ctx: Ctx<'_>, prompt: String) -> rquickjs::Result<Option<String>> {
    if prompt.is_empty() { return Ok(None); }
    let api_key = match std::env::var("ANTHROPIC_API_KEY") {
        Ok(k) => k,
        Err(_) => return Ok(None),
    };
    let body = serde_json::json!({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 1024,
        "system": "You are a game NPC designer. Always respond with valid JSON only.",
        "messages": [{ "role": "user", "content": prompt }]
    });
    match reqwest::blocking::Client::new()
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", &api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&body)
        .send()
    {
        Ok(resp) => match resp.json::<serde_json::Value>() {
            Ok(json) => Ok(json["content"][0]["text"].as_str().map(str::to_owned)),
            Err(_) => Ok(None),
        },
        Err(_) => Ok(None),
    }
}

// ── Context initialisation ────────────────────────────────────────────────────

fn build_context(rt: &Runtime, script: &str) -> Result<Context> {
    let ctx = Context::full(rt)
        .map_err(|e| NeonDBError::reducer_error(format!("QJS context: {}", e)))?;

    ctx.with(|c| -> Result<()> {
        let globals = c.globals();

        // Register raw host functions.
        globals.set("__neondb_get_raw",        Func::from(host_get_raw))
            .map_err(|e| NeonDBError::reducer_error(format!("reg get_raw: {}", e)))?;
        globals.set("__neondb_get_all_raw",    Func::from(host_get_all_raw))
            .map_err(|e| NeonDBError::reducer_error(format!("reg get_all_raw: {}", e)))?;
        globals.set("__neondb_set_raw",        Func::from(host_set_raw))
            .map_err(|e| NeonDBError::reducer_error(format!("reg set_raw: {}", e)))?;
        globals.set("__neondb_delete",         Func::from(host_delete))
            .map_err(|e| NeonDBError::reducer_error(format!("reg delete: {}", e)))?;
        globals.set("__neondb_ai_generate_raw",Func::from(host_ai_generate_raw))
            .map_err(|e| NeonDBError::reducer_error(format!("reg ai_raw: {}", e)))?;

        // Seed identity globals.
        globals.set("__neondb_caller_id",   "")
            .map_err(|e| NeonDBError::reducer_error(format!("seed caller_id: {}", e)))?;
        globals.set("__neondb_caller_role", "")
            .map_err(|e| NeonDBError::reducer_error(format!("seed caller_role: {}", e)))?;

        // Load preamble (JSON bridge wrappers) then user script.
        let mut opts = EvalOptions::default();
        opts.global = true;
        c.eval_with_options::<(), _>(JS_PREAMBLE, opts)
            .map_err(|e| NeonDBError::reducer_error(format!("Preamble load: {}", e)))?;
        let mut opts2 = EvalOptions::default();
        opts2.global = true;
        c.eval_with_options::<(), _>(script, opts2)
            .map_err(|e| NeonDBError::reducer_error(format!("Script load: {}", e)))?;

        Ok(())
    })?;

    Ok(ctx)
}

// ── Backend ───────────────────────────────────────────────────────────────────

pub struct V8ReducerBackend {
    script_key: String,
    script:     String,
    timeout_ms: u64,
}

impl V8ReducerBackend {
    pub fn from_file(path: PathBuf, timeout_ms: u64) -> Result<Self> {
        let script_key = path.to_string_lossy().into_owned();
        let script     = std::fs::read_to_string(&path)?;
        Ok(V8ReducerBackend { script_key, script, timeout_ms })
    }

    fn run(&self, ctx: &mut ReducerContext, args_json: Value) -> Result<Value> {
        ensure_runtime()?;

        CURRENT_CTX.with(|c| c.set(ctx as *mut ReducerContext));

        let result = QJS_CTXS.with(|map_cell| -> Result<Value> {
            let mut map = map_cell.borrow_mut();

            if !map.contains_key(&self.script_key) {
                let qjs_ctx = QJS_RT.with(|rt_cell| -> Result<Context> {
                    let borrow = rt_cell.borrow();
                    let rt = borrow.as_ref()
                        .ok_or_else(|| NeonDBError::reducer_error("QJS runtime missing"))?;
                    build_context(rt, &self.script)
                })?;
                map.insert(self.script_key.clone(), qjs_ctx);
            }

            let qjs_ctx = map.get(&self.script_key).unwrap();

            qjs_ctx.with(|c| -> Result<Value> {
                // Update per-call identity globals.
                let globals = c.globals();
                globals.set("__neondb_caller_id",   ctx.caller_id.as_str())
                    .map_err(|e| NeonDBError::reducer_error(format!("set caller_id: {}", e)))?;
                globals.set("__neondb_caller_role", ctx.caller_role.as_str())
                    .map_err(|e| NeonDBError::reducer_error(format!("set caller_role: {}", e)))?;

                // Get the `reducer` function.
                let reducer_fn: Function = globals.get("reducer")
                    .map_err(|_| NeonDBError::reducer_error(
                        "No `reducer` function found — JS must define `function reducer(args) { ... }`"
                    ))?;

                // Encode args as JSON string, pass to reducer as parsed JS value.
                let args_json_str = serde_json::to_string(&args_json)?;
                let json_obj: Object = c.globals().get("JSON")?;
                let parse_fn: Function = json_obj.get("parse")?;
                let args_qjs = parse_fn.call::<_, rquickjs::Value>((args_json_str,))
                    .map_err(|e| NeonDBError::reducer_error(format!("Args parse: {}", e)))?;

                // Call reducer(args) under a wall-clock deadline.  The
                // interrupt handler kills the script if it runs past
                // timeout_ms; the guard clears the deadline on every exit
                // path (success, error, panic).
                let _deadline = DeadlineGuard::arm(self.timeout_ms);
                let result_qjs = reducer_fn.call::<_, rquickjs::Value>((args_qjs,))
                    .map_err(|e| {
                        if DeadlineGuard::expired() {
                            NeonDBError::reducer_error(format!(
                                "Reducer timeout: exceeded {} ms CPU budget", self.timeout_ms
                            ))
                        } else {
                            NeonDBError::reducer_error(format!("Reducer call: {}", e))
                        }
                    })?;
                drop(_deadline);

                // Stringify result back to JSON then parse as serde_json::Value.
                let stringify_fn: Function = json_obj.get("stringify")?;
                let result_str: rquickjs::String = stringify_fn.call((result_qjs,))
                    .map_err(|e| NeonDBError::reducer_error(format!("Result stringify: {}", e)))?;
                let raw = result_str.to_string()
                    .map_err(|e| NeonDBError::reducer_error(format!("Result to str: {}", e)))?;
                serde_json::from_str(&raw)
                    .map_err(|e| NeonDBError::reducer_error(format!("Result JSON parse: {}", e)))
            })
        });

        CURRENT_CTX.with(|c| c.set(std::ptr::null_mut()));

        // After a timeout the script was killed mid-execution — its JS global
        // state may be partially mutated.  Evict the warm context so the next
        // call rebuilds from the pristine source.  (DB state is safe either
        // way: staged deltas are discarded because the error skips commit.)
        if let Err(ref e) = result {
            if e.to_string().contains("Reducer timeout") {
                QJS_CTXS.with(|map_cell| {
                    map_cell.borrow_mut().remove(&self.script_key);
                });
            }
        }

        result
    }
}

impl ReducerBackend for V8ReducerBackend {
    fn execute(&self, ctx: &mut ReducerContext, args: &[u8]) -> Result<Vec<u8>> {
        let max_io = crate::reducer::max_io_bytes();
        if args.len() > max_io {
            return Err(NeonDBError::reducer_error(format!(
                "Reducer args too large: {} bytes (limit {})", args.len(), max_io
            )));
        }

        let args_json: Value = if args.is_empty() {
            Value::Array(vec![])
        } else {
            rmp_serde::from_slice(args).unwrap_or(Value::Array(vec![]))
        };

        let result  = self.run(ctx, args_json)?;
        let encoded = rmp_serde::to_vec(&result)?;

        if encoded.len() > max_io {
            return Err(NeonDBError::reducer_error(format!(
                "Reducer result too large: {} bytes (limit {})", encoded.len(), max_io
            )));
        }
        Ok(encoded)
    }
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
        let path = write_tmp("test_qjs_counter.js", r#"
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
        let res: Value = rmp_serde::from_slice(&backend.execute(&mut ctx, &args).unwrap()).unwrap();
        assert_eq!(res["value"], 5);
        ctx.commit().unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_set_and_get_json_object() {
        let path = write_tmp("test_qjs_obj.js", r#"
function reducer(args) {
    __neondb_set("players", args[0], { hp: 200, status: "alive" });
    var p = __neondb_get("players", args[0]);
    return { ok: true, hp: p ? p.hp : -1, status: p ? p.status : "none" };
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();
        let args = rmp_serde::to_vec(&serde_json::json!(["alice"])).unwrap();
        let res: Value = rmp_serde::from_slice(&backend.execute(&mut ctx, &args).unwrap()).unwrap();
        assert_eq!(res["hp"], 200);
        assert_eq!(res["status"], "alive");
        ctx.commit().unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_empty_args_does_not_crash() {
        let path = write_tmp("test_qjs_empty.js", r#"
function reducer(args) {
    var tick = __neondb_get("world_state", "tick") || { count: 0 };
    tick.count = (tick.count || 0) + 1;
    __neondb_set("world_state", "tick", tick);
    return { ok: true, tick: tick.count };
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();
        let res: Value = rmp_serde::from_slice(&backend.execute(&mut ctx, &[]).unwrap()).unwrap();
        assert_eq!(res["tick"], 1);
        ctx.commit().unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_delete_row() {
        let path = write_tmp("test_qjs_del.js", r#"
function reducer(args) {
    __neondb_set("items", "sword", { name: "sword" });
    __neondb_delete("items", "sword");
    var after = __neondb_get("items", "sword");
    return { deleted: after === null };
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();
        let args = rmp_serde::to_vec(&serde_json::json!([])).unwrap();
        let res: Value = rmp_serde::from_slice(&backend.execute(&mut ctx, &args).unwrap()).unwrap();
        assert_eq!(res["deleted"], true);
        ctx.commit().unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_caller_identity_accessible() {
        let path = write_tmp("test_qjs_caller.js", r#"
function reducer(args) {
    return { caller_id: __neondb_caller_id, caller_role: __neondb_caller_role };
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();
        ctx.caller_id   = "player-42".to_string();
        ctx.caller_role = "admin".to_string();
        let args = rmp_serde::to_vec(&serde_json::json!([])).unwrap();
        let res: Value = rmp_serde::from_slice(&backend.execute(&mut ctx, &args).unwrap()).unwrap();
        assert_eq!(res["caller_id"],   "player-42");
        assert_eq!(res["caller_role"], "admin");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_args_oversize_rejected() {
        let _g = crate::reducer::SANDBOX_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        crate::reducer::set_max_io_bytes(4 * 1024);
        let path = write_tmp("test_qjs_cap.js", "function reducer(args) { return {}; }");
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();
        let res = backend.execute(&mut ctx, &vec![0u8; 5 * 1024]);
        std::fs::remove_file(&path).ok();
        crate::reducer::set_max_io_bytes(1024 * 1024);
        assert!(res.unwrap_err().to_string().to_lowercase().contains("too large"));
    }

    #[test]
    fn test_v8_world_tick_pattern() {
        let path = write_tmp("test_qjs_tick.js", r#"
function reducer(args) {
    var tick = __neondb_get("world_state", "tick") || { count: 0 };
    tick.count += 1;
    __neondb_set("world_state", "tick", tick);
    return { ok: true, tick: tick.count };
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();
        let res1: Value = rmp_serde::from_slice(&backend.execute(&mut ctx, &[]).unwrap()).unwrap();
        ctx.commit().unwrap();
        let res2: Value = rmp_serde::from_slice(&backend.execute(&mut ctx, &[]).unwrap()).unwrap();
        ctx.commit().unwrap();
        assert_eq!(res1["tick"], 1);
        assert_eq!(res2["tick"], 2);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_infinite_loop_killed_by_timeout() {
        let path = write_tmp("test_qjs_infloop.js", r#"
function reducer(args) {
    while (true) { }  // never returns
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 200).unwrap();
        let mut ctx = make_ctx();
        let t0 = std::time::Instant::now();
        let res = backend.execute(&mut ctx, &[]);
        let elapsed = t0.elapsed();
        std::fs::remove_file(&path).ok();

        let err = res.unwrap_err().to_string();
        assert!(err.contains("Reducer timeout"), "expected timeout error, got: {err}");
        // Killed promptly — well under 5x the budget (generous CI margin).
        assert!(elapsed.as_millis() < 1000, "took too long to kill: {elapsed:?}");
    }

    #[test]
    fn test_v8_worker_survives_after_timeout() {
        // 1. Run an infinite-loop reducer → killed by deadline.
        let bad_path = write_tmp("test_qjs_bad.js", r#"
function reducer(args) { for(;;) {} }
"#);
        let bad = V8ReducerBackend::from_file(bad_path.clone(), 150).unwrap();
        let mut ctx1 = make_ctx();
        assert!(bad.execute(&mut ctx1, &[]).is_err());

        // 2. Same thread, same runtime: a healthy reducer must still work.
        let good_path = write_tmp("test_qjs_good_after.js", r#"
function reducer(args) {
    __neondb_set("recovery", "check", { alive: true });
    return { ok: true };
}
"#);
        let good = V8ReducerBackend::from_file(good_path.clone(), 1000).unwrap();
        let mut ctx2 = make_ctx();
        let res: Value = rmp_serde::from_slice(&good.execute(&mut ctx2, &[]).unwrap()).unwrap();
        assert_eq!(res["ok"], true);
        ctx2.commit().unwrap();

        // 3. The SAME bad script can be retried (fresh context after eviction)
        //    and is killed again rather than wedging the thread.
        let mut ctx3 = make_ctx();
        let err = bad.execute(&mut ctx3, &[]).unwrap_err().to_string();
        assert!(err.contains("Reducer timeout"), "retry not killed: {err}");

        std::fs::remove_file(&bad_path).ok();
        std::fs::remove_file(&good_path).ok();
    }

    #[test]
    fn test_v8_timeout_discards_staged_writes() {
        // A reducer that writes a row, then hangs: the write must NOT survive,
        // because the error path skips commit.
        let path = write_tmp("test_qjs_partial.js", r#"
function reducer(args) {
    __neondb_set("partial", "row1", { x: 1 });
    while (true) { }
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 150).unwrap();
        let tables = Arc::new(TableStore::new());
        let mut ctx = ReducerContext::new(tables.clone(), 2000);
        assert!(backend.execute(&mut ctx, &[]).is_err());
        // Worker would call rollback / drop ctx here — simulate by dropping.
        drop(ctx);
        assert!(tables.get_row("partial", "row1").unwrap().is_none(),
            "staged write leaked through after timeout");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_fast_reducer_unaffected_by_deadline() {
        // Sanity: normal reducers complete fine with a tight-but-fair budget.
        let path = write_tmp("test_qjs_fast.js", r#"
function reducer(args) {
    var total = 0;
    for (var i = 0; i < 1000; i++) total += i;
    return { total: total };
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();
        let mut ctx = make_ctx();
        let res: Value = rmp_serde::from_slice(&backend.execute(&mut ctx, &[]).unwrap()).unwrap();
        assert_eq!(res["total"], 499500);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_v8_caller_identity_updates_between_calls() {
        let path = write_tmp("test_qjs_caller2.js", r#"
function reducer(args) {
    return { caller_id: __neondb_caller_id, caller_role: __neondb_caller_role };
}
"#);
        let backend = V8ReducerBackend::from_file(path.clone(), 1000).unwrap();

        let mut ctx1 = make_ctx();
        ctx1.caller_id   = "alice".to_string();
        ctx1.caller_role = "admin".to_string();
        let r1: Value = rmp_serde::from_slice(&backend.execute(&mut ctx1, &[]).unwrap()).unwrap();
        assert_eq!(r1["caller_id"], "alice");

        let mut ctx2 = make_ctx();
        ctx2.caller_id   = "bob".to_string();
        ctx2.caller_role = "player".to_string();
        let r2: Value = rmp_serde::from_slice(&backend.execute(&mut ctx2, &[]).unwrap()).unwrap();
        assert_eq!(r2["caller_id"], "bob");

        std::fs::remove_file(&path).ok();
    }
}
