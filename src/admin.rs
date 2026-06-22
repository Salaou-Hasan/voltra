// Metrics / admin HTTP server: the embedded admin console plus every
// /admin/api/*, /cluster/*, /replication/*, and /backup endpoint. Backed by
// `AdminState`, which carries the paths + registries the handlers need.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use hyper::{
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use tokio::sync::watch;

use crate::{
    auth::IdentityIssuer, error::Result, metrics::Metrics, network::PendingCall,
    presence::PresenceManager, reducer::ReducerRegistry, subscriptions::SubscriptionManager,
    table::TableStore, ttl::TtlManager, wal::BatchedWalWriter,
};

// ─────────────────────────────────────────────────────────────────────────────
// Metrics / admin HTTP server
// ─────────────────────────────────────────────────────────────────────────────

/// Paths + backup policy needed by the admin endpoints (backup, replication).
pub struct AdminState {
    pub wal_path: PathBuf,
    pub backup_dir: Option<PathBuf>,
    pub backup_keep: usize,
    pub tenant_registry: Arc<crate::tenant::TenantRegistry>,
    pub cluster_bus: Arc<crate::cluster::ClusterBus>,
    pub drain_flag: Arc<std::sync::atomic::AtomicBool>,
    pub active_connections: Arc<std::sync::atomic::AtomicUsize>,
    pub region_registry: Arc<crate::cluster::RegionRegistry>,
    pub lobby_routes: Arc<crate::cluster::LobbyRouteRegistry>,
    pub leaderboard: Arc<crate::leaderboard::LeaderboardEngine>,
    // Held to keep the stat-sync queue alive for the server's lifetime; not read directly.
    #[allow(dead_code)]
    pub stat_sync: Arc<crate::stat_sync::StatSyncQueue>,
    /// Per-lobby worker router — exposes queue depths and call stats.
    pub lobby_router: Arc<crate::worker_pool::LobbyRouter>,
    /// SQLite-backed relational tier (auth users, characters, catalog).
    pub persistent: Arc<crate::persistent::PersistentStore>,
    /// Authentication service (register / login / verify token).
    pub auth_service: Arc<crate::auth_service::AuthService>,
}

pub async fn start_metrics_server(
    host: String,
    port: u16,
    subscription_manager: Arc<SubscriptionManager>,
    tables: Arc<TableStore>,
    registry: Arc<ReducerRegistry>,
    wal_writer: Arc<BatchedWalWriter>,
    global_seq: Arc<std::sync::atomic::AtomicU64>,
    startup_instant: std::time::Instant,
    presence_manager: Arc<PresenceManager>,
    ttl_manager: Arc<TtlManager>,
    prom: Arc<Metrics>,
    identity_issuer: Arc<IdentityIssuer>,
    queue_probe: kanal::AsyncSender<PendingCall>,
    admin: Arc<AdminState>,
    schema_registry: Arc<crate::schema::SchemaRegistry>,
    mut shutdown: watch::Receiver<()>,
) -> Result<()> {
    let addr: SocketAddr = format!("{}:{}", host, port).parse().map_err(|e| {
        crate::error::VoltraError::invalid_argument(format!("Invalid metrics address: {}", e))
    })?;

    let make_service = make_service_fn(move |_| {
        let subs = subscription_manager.clone();
        let tbl = tables.clone();
        let reg = registry.clone();
        let wal = wal_writer.clone();
        let seq = global_seq.clone();
        let start = startup_instant;
        let pres = presence_manager.clone();
        let ttl = ttl_manager.clone();
        let prom_svc = prom.clone();
        let iss = identity_issuer.clone();
        let qp = queue_probe.clone();
        let adm = admin.clone();
        let sch = schema_registry.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let subs = subs.clone();
                let tbl = tbl.clone();
                let reg = reg.clone();
                let wal = wal.clone();
                let seq = seq.clone();
                let pres = pres.clone();
                let ttl = ttl.clone();
                let prom_r = prom_svc.clone();
                let iss_r = iss.clone();
                let qp_r = qp.clone();
                let adm_r = adm.clone();
                let sch_r = sch.clone();
                async move {
                    handle_metrics_request(
                        req, subs, tbl, reg, wal, seq, start, pres, ttl, prom_r, iss_r, qp_r,
                        adm_r, sch_r,
                    )
                    .await
                }
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_service);
    log::info!("Admin/metrics on http://{}", addr);
    println!("  Admin console: http://{}/admin", addr);
    server
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .map_err(|e| crate::error::VoltraError::network_error(format!("Metrics server: {}", e)))
}

pub fn json_response(value: serde_json::Value) -> Response<Body> {
    let mut r = Response::new(Body::from(value.to_string()));
    r.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    r
}

/// The single-file admin console, embedded at compile time.
pub const ADMIN_DASHBOARD_HTML: &str = include_str!("admin_dashboard.html");

pub fn bad_request(msg: String) -> Response<Body> {
    let mut r = json_response(serde_json::json!({ "error": msg }));
    *r.status_mut() = StatusCode::BAD_REQUEST;
    r
}

pub fn server_error(msg: String) -> Response<Body> {
    let mut r = json_response(serde_json::json!({ "error": msg }));
    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    r
}

/// Minimal percent-decoding for admin query params (UTF-8, lossy on bad bytes).
pub fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                .ok()
                .and_then(|h| u8::from_str_radix(h, 16).ok());
            if let Some(b) = hex {
                out.push(b);
                i += 3;
                continue;
            }
        }
        if bytes[i] == b'+' {
            out.push(b' ');
        } else {
            out.push(bytes[i]);
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Gate mutating admin endpoints behind the API key when one is configured.
/// With no VOLTRA_API_KEY set (dev mode), all requests pass.
pub fn admin_auth_check(req: &Request<Body>) -> Option<Response<Body>> {
    let configured = std::env::var("VOLTRA_API_KEY").unwrap_or_default();
    if configured.is_empty() {
        return None;
    }
    let provided = req
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim_start_matches("Bearer ")
        .trim();
    if provided == configured {
        return None;
    }
    let mut r = json_response(serde_json::json!({
        "error": "Unauthorized: set your API key in the Operations tab"
    }));
    *r.status_mut() = StatusCode::UNAUTHORIZED;
    Some(r)
}

pub async fn handle_metrics_request(
    req: Request<Body>,
    subscription_manager: Arc<SubscriptionManager>,
    tables: Arc<TableStore>,
    registry: Arc<ReducerRegistry>,
    wal_writer: Arc<BatchedWalWriter>,
    global_seq: Arc<std::sync::atomic::AtomicU64>,
    startup_instant: std::time::Instant,
    presence_manager: Arc<PresenceManager>,
    ttl_manager: Arc<TtlManager>,
    prom: Arc<Metrics>,
    identity_issuer: Arc<IdentityIssuer>,
    queue_probe: kanal::AsyncSender<PendingCall>,
    admin: Arc<AdminState>,
    schema_registry: Arc<crate::schema::SchemaRegistry>,
) -> Result<Response<Body>> {
    let path = req.uri().path().to_string();

    match (req.method(), path.as_str()) {
        // ── Admin dashboard ───────────────────────────────────────────────────
        //
        // GET  /admin              — embedded single-file web console
        // POST /admin/api/call     — invoke a reducer through the real queue
        // POST /admin/api/sql      — run a SQL query
        // POST /admin/api/row      — upsert a row (durable: WAL + live fan-out)
        // DELETE /admin/api/row    — delete a row (durable: WAL + live fan-out)
        (&Method::GET, "/admin") | (&Method::GET, "/admin/") => {
            let mut r = Response::new(Body::from(ADMIN_DASHBOARD_HTML));
            r.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("text/html; charset=utf-8"),
            );
            Ok(r)
        }

        // ── Drain mode ───────────────────────────────────────────────────────
        // GET    /admin/api/drain — drain status + active connection count
        // POST   /admin/api/drain — enable drain (stop new connections)
        // DELETE /admin/api/drain — disable drain (resume accepting connections)
        (&Method::GET, "/admin/api/drain") => {
            let draining = admin.drain_flag.load(std::sync::atomic::Ordering::Relaxed);
            let conns = admin
                .active_connections
                .load(std::sync::atomic::Ordering::Relaxed);
            Ok(json_response(serde_json::json!({
                "draining": draining,
                "active_connections": conns,
                "message": if draining {
                    format!("{} connections still active — new connections refused", conns)
                } else {
                    "Server accepting connections normally".to_string()
                }
            })))
        }

        (&Method::POST, "/admin/api/drain") => {
            if let Some(resp) = admin_auth_check(&req) {
                return Ok(resp);
            }
            admin
                .drain_flag
                .store(true, std::sync::atomic::Ordering::Relaxed);
            let conns = admin
                .active_connections
                .load(std::sync::atomic::Ordering::Relaxed);
            log::warn!(
                "[drain] Drain mode ENABLED — {} active connections finishing",
                conns
            );
            Ok(json_response(serde_json::json!({
                "draining": true,
                "active_connections": conns,
                "message": "Drain enabled. New connections refused with HTTP 503. Existing connections unaffected."
            })))
        }

        (&Method::DELETE, "/admin/api/drain") => {
            if let Some(resp) = admin_auth_check(&req) {
                return Ok(resp);
            }
            admin
                .drain_flag
                .store(false, std::sync::atomic::Ordering::Relaxed);
            let conns = admin
                .active_connections
                .load(std::sync::atomic::Ordering::Relaxed);
            log::info!("[drain] Drain mode DISABLED — resuming normal operation");
            Ok(json_response(serde_json::json!({
                "draining": false,
                "active_connections": conns,
                "message": "Drain disabled. Server accepting new connections normally."
            })))
        }

        (&Method::POST, "/admin/api/call") => {
            if let Some(resp) = admin_auth_check(&req) {
                return Ok(resp);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await.map_err(|e| {
                crate::error::VoltraError::network_error(format!("Read body: {}", e))
            })?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
            };
            let name = match payload.get("name").and_then(|v| v.as_str()) {
                Some(n) if !n.is_empty() => n.to_string(),
                _ => return Ok(bad_request("Missing 'name' field".into())),
            };
            let args_val = payload
                .get("args")
                .cloned()
                .unwrap_or(serde_json::json!([]));
            let args_bytes = rmp_serde::to_vec(&args_val).map_err(|e| {
                crate::error::VoltraError::reducer_error(format!("Args encode: {}", e))
            })?;

            // Dispatch through the real reducer queue so the call gets the
            // identical execution path as a WebSocket client (permissions
            // excepted — this endpoint is admin-gated above).
            let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel();
            let call = PendingCall {
                call_id: 0,
                reducer_name: name,
                args: args_bytes,
                caller_id: "admin-console".to_string(),
                caller_role: "admin".to_string(),
                tenant_id: None,
                lobby_hint: None,
                response_tx: resp_tx,
            };
            if queue_probe.send(call).await.is_err() {
                return Ok(server_error("Reducer queue closed".into()));
            }
            match tokio::time::timeout(std::time::Duration::from_secs(30), resp_rx.recv()).await {
                Ok(Some(resp)) => {
                    let result_json: serde_json::Value = resp
                        .result
                        .as_deref()
                        .and_then(|b| rmp_serde::from_slice(b).ok())
                        .unwrap_or(serde_json::Value::Null);
                    Ok(json_response(serde_json::json!({
                        "success": resp.success,
                        "result": result_json,
                        "error": resp.error,
                    })))
                }
                Ok(None) => Ok(server_error("Worker dropped response channel".into())),
                Err(_) => Ok(server_error("Reducer call timed out after 30s".into())),
            }
        }

        (&Method::POST, "/admin/api/sql") => {
            if let Some(resp) = admin_auth_check(&req) {
                return Ok(resp);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await.map_err(|e| {
                crate::error::VoltraError::network_error(format!("Read body: {}", e))
            })?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
            };
            let query = match payload.get("query").and_then(|v| v.as_str()) {
                Some(q) if !q.trim().is_empty() => q.to_string(),
                _ => return Ok(bad_request("Missing 'query' field".into())),
            };
            let tbl = tables.clone();
            let result = tokio::task::spawn_blocking(move || -> std::result::Result<_, String> {
                let stmt =
                    crate::sql::parser::parse(&query).map_err(|e| format!("Parse error: {}", e))?;
                let exec = crate::SqlExecutor::new(tbl);
                exec.execute_statement(&stmt)
                    .map_err(|e| format!("Execution error: {}", e))
            })
            .await;
            match result {
                Ok(Ok(res)) => {
                    let rows: Vec<serde_json::Value> = res
                        .rows
                        .into_iter()
                        .map(serde_json::Value::Object)
                        .collect();
                    Ok(json_response(serde_json::json!({
                        "columns": res.columns,
                        "rows": rows,
                        "rows_affected": res.rows_affected,
                    })))
                }
                Ok(Err(e)) => Ok(bad_request(e)),
                Err(e) => Ok(server_error(format!("task: {}", e))),
            }
        }

        (&Method::POST, "/admin/api/row") => {
            if let Some(resp) = admin_auth_check(&req) {
                return Ok(resp);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await.map_err(|e| {
                crate::error::VoltraError::network_error(format!("Read body: {}", e))
            })?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
            };
            let (table, rkey, data) = match (
                payload.get("table").and_then(|v| v.as_str()),
                payload.get("key").and_then(|v| v.as_str()),
                payload.get("data"),
            ) {
                (Some(t), Some(k), Some(d)) if !t.is_empty() && !k.is_empty() => {
                    (t.to_string(), k.to_string(), d.clone())
                }
                _ => return Ok(bad_request("Expected {table, key, data}".into())),
            };
            match tables.set_row(table.clone(), rkey.clone(), data) {
                Ok(delta) => {
                    // Durable + live: fan out to subscribers and journal to WAL,
                    // exactly like a reducer write (unlike /seed).
                    let deltas = vec![delta];
                    subscription_manager.publish_deltas(&deltas);
                    let seq = global_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let entry = crate::WalEntry::new(
                        crate::now_nanos(),
                        seq,
                        "__admin_set_row".to_string(),
                        vec![],
                        deltas,
                    );
                    if let Err(e) = wal_writer.append(&entry, seq) {
                        log::warn!("[admin] WAL append failed: {}", e);
                    }
                    Ok(json_response(
                        serde_json::json!({ "ok": true, "table": table, "key": rkey }),
                    ))
                }
                Err(e) => Ok(bad_request(e.to_string())),
            }
        }

        (&Method::DELETE, "/admin/api/row") => {
            if let Some(resp) = admin_auth_check(&req) {
                return Ok(resp);
            }
            let query = req.uri().query().unwrap_or("");
            let mut table = String::new();
            let mut rkey = String::new();
            for pair in query.split('&') {
                let mut kv = pair.splitn(2, '=');
                match (kv.next(), kv.next()) {
                    (Some("table"), Some(v)) => table = url_decode(v),
                    (Some("key"), Some(v)) => rkey = url_decode(v),
                    _ => {}
                }
            }
            if table.is_empty() || rkey.is_empty() {
                return Ok(bad_request("Expected ?table=X&key=Y".into()));
            }
            match tables.delete_row(&table, &rkey) {
                Ok(delta) => {
                    let deltas = vec![delta];
                    subscription_manager.publish_deltas(&deltas);
                    let seq = global_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let entry = crate::WalEntry::new(
                        crate::now_nanos(),
                        seq,
                        "__admin_delete_row".to_string(),
                        vec![],
                        deltas,
                    );
                    if let Err(e) = wal_writer.append(&entry, seq) {
                        log::warn!("[admin] WAL append failed: {}", e);
                    }
                    Ok(json_response(serde_json::json!({ "ok": true })))
                }
                Err(e) => Ok(bad_request(e.to_string())),
            }
        }

        // ── Tenant management endpoints ───────────────────────────────────────
        //
        // GET    /admin/api/tenants         — list all tenants (keys masked)
        // POST   /admin/api/tenants         — create a tenant
        // DELETE /admin/api/tenants?id=<id> — delete a tenant and ALL its data
        (&Method::GET, "/admin/api/tenants") => {
            if let Some(resp) = admin_auth_check(&req) {
                return Ok(resp);
            }
            Ok(json_response(admin.tenant_registry.summary_json(false)))
        }

        (&Method::POST, "/admin/api/tenants") => {
            if let Some(resp) = admin_auth_check(&req) {
                return Ok(resp);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body()).await.map_err(|e| {
                crate::error::VoltraError::network_error(format!("Read body: {}", e))
            })?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
            };
            let name = match payload.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => return Ok(bad_request("Missing 'name' field".into())),
            };
            let max_rows = payload
                .get("max_rows")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let max_calls = payload
                .get("max_calls_per_sec")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32;

            match admin.tenant_registry.create(&name, max_rows, max_calls) {
                Ok((info, delta)) => {
                    // Durably persist: publish + WAL append.
                    let deltas = vec![delta];
                    subscription_manager.publish_deltas(&deltas);
                    let seq = global_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let entry = crate::WalEntry::new(
                        crate::now_nanos(),
                        seq,
                        "__admin_create_tenant".to_string(),
                        vec![],
                        deltas,
                    );
                    let _ = wal_writer.append(&entry, seq);
                    Ok(json_response(serde_json::json!({
                        "ok": true,
                        "id": info.id,
                        "api_key": info.api_key,
                        "name": info.name,
                    })))
                }
                Err(e) => Ok(bad_request(e.to_string())),
            }
        }

        (&Method::DELETE, "/admin/api/tenants") => {
            if let Some(resp) = admin_auth_check(&req) {
                return Ok(resp);
            }
            let query = req.uri().query().unwrap_or("");
            let tenant_id = query
                .split('&')
                .filter_map(|p| {
                    let mut kv = p.splitn(2, '=');
                    if kv.next() == Some("id") {
                        kv.next().map(url_decode)
                    } else {
                        None
                    }
                })
                .next()
                .unwrap_or_default();
            if tenant_id.is_empty() {
                return Ok(bad_request("Expected ?id=<tenant_id>".into()));
            }
            match admin.tenant_registry.delete(&tenant_id) {
                Ok(deltas) => {
                    subscription_manager.publish_deltas(&deltas);
                    let seq = global_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let entry = crate::WalEntry::new(
                        crate::now_nanos(),
                        seq,
                        "__admin_delete_tenant".to_string(),
                        vec![],
                        deltas,
                    );
                    let _ = wal_writer.append(&entry, seq);
                    Ok(json_response(serde_json::json!({ "ok": true })))
                }
                Err(e) => Ok(bad_request(e.to_string())),
            }
        }

        // ── Per-lobby worker stats ────────────────────────────────────────────
        //
        // GET /admin/api/lobbies — all active lobby workers with queue/call/latency stats
        (&Method::GET, "/admin/api/lobbies") => {
            if let Some(resp) = admin_auth_check(&req) {
                return Ok(resp);
            }
            let snapshots = admin.lobby_router.lobbies_snapshot();
            Ok(json_response(serde_json::json!({
                "active_lobbies": snapshots.len(),
                "lobbies": snapshots,
            })))
        }

        // ── Replication endpoints ─────────────────────────────────────────────
        //
        // GET  /replication/wal?from_seq=N&max=M — primary serves WAL entries
        // GET  /replication/status              — role + lag info
        // POST /replication/promote             — replica → primary failover
        (&Method::GET, "/replication/wal") => {
            let query = req.uri().query().unwrap_or("");
            let mut from_seq = 0u64;
            let mut max = 2048usize;
            for pair in query.split('&') {
                let mut kv = pair.splitn(2, '=');
                match (kv.next(), kv.next()) {
                    (Some("from_seq"), Some(v)) => from_seq = v.parse().unwrap_or(0),
                    (Some("max"), Some(v)) => {
                        max = v.parse::<usize>().unwrap_or(2048).clamp(1, 8192)
                    }
                    _ => {}
                }
            }
            let wal_path = admin.wal_path.clone();
            let result = tokio::task::spawn_blocking(move || {
                crate::replication::serve_wal_entries(&wal_path, from_seq, max)
            })
            .await;
            match result {
                Ok(Ok((entries, last_seq))) => Ok(json_response(serde_json::json!({
                    "entries": crate::replication::encode_entries(&entries),
                    "last_seq": last_seq,
                }))),
                Ok(Err(e)) => {
                    let mut r = json_response(serde_json::json!({ "error": e.to_string() }));
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(r)
                }
                Err(e) => {
                    let mut r =
                        json_response(serde_json::json!({ "error": format!("task: {}", e) }));
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(r)
                }
            }
        }

        (&Method::GET, "/replication/status") => {
            Ok(json_response(crate::replication::status_json()))
        }

        (&Method::POST, "/replication/promote") => {
            let was_replica = crate::replication::is_replica();
            crate::replication::set_replica(false);
            if was_replica {
                log::warn!("[replication] PROMOTED to primary via /replication/promote");
            }
            Ok(json_response(serde_json::json!({
                "promoted": was_replica,
                "role": "primary",
                "last_applied_seq": crate::replication::last_applied_seq(),
            })))
        }

        // ── Cluster endpoints ─────────────────────────────────────────────────
        //
        // GET  /cluster/health  — liveness probe for gossip heartbeats
        // GET  /cluster/peers   — current peer list + health + config
        // POST /cluster/deltas  — receive replicated RowDeltas from a peer
        // POST /cluster/call    — execute a proxied reducer call
        // POST /cluster/join    — register a new peer dynamically
        (&Method::GET, "/cluster/health") => Ok(json_response(serde_json::json!({
            "ok": true,
            "shard_id": admin.cluster_bus.config.my_shard_id,
        }))),

        (&Method::GET, "/cluster/peers") => {
            let bus = &admin.cluster_bus;
            Ok(json_response(serde_json::json!({
                "cluster_enabled": bus.is_active(),
                "my_shard_id":     bus.config.my_shard_id,
                "shard_count":     bus.config.shard_count,
                "peers":           bus.peers_snapshot(),
            })))
        }

        (&Method::POST, "/cluster/deltas") => {
            let secret = req
                .headers()
                .get("x-voltra-cluster-secret")
                .and_then(|v| v.to_str().ok());
            if !admin.cluster_bus.validate_secret(secret) {
                let mut r = json_response(serde_json::json!({ "error": "Unauthorized" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body())
                .await
                .map_err(|e| crate::error::VoltraError::network_error(e.to_string()))?;
            match crate::cluster::fanout::parse_delta_payload(&body_bytes) {
                Err(e) => Ok(bad_request(e.to_string())),
                Ok(payload) => {
                    let row_deltas = crate::cluster::fanout::wire_to_row_deltas(payload.deltas);
                    let applied = row_deltas.len();
                    match crate::cluster::ClusterBus::apply_peer_deltas(
                        &row_deltas,
                        &tables,
                        &subscription_manager,
                    ) {
                        Ok(()) => Ok(json_response(
                            serde_json::json!({ "ok": true, "applied": applied }),
                        )),
                        Err(e) => Ok(server_error(e.to_string())),
                    }
                }
            }
        }

        (&Method::POST, "/cluster/call") => {
            let secret = req
                .headers()
                .get("x-voltra-cluster-secret")
                .and_then(|v| v.to_str().ok());
            if !admin.cluster_bus.validate_secret(secret) {
                let mut r = json_response(serde_json::json!({ "error": "Unauthorized" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body())
                .await
                .map_err(|e| crate::error::VoltraError::network_error(e.to_string()))?;
            let pr: crate::cluster::proxy::ProxyCallRequest =
                match serde_json::from_slice(&body_bytes) {
                    Ok(r) => r,
                    Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
                };
            use base64::Engine as _;
            let args = match base64::engine::general_purpose::STANDARD.decode(&pr.args_b64) {
                Ok(b) => b,
                Err(e) => return Ok(bad_request(format!("Bad args_b64: {}", e))),
            };
            let (resp_tx, mut resp_rx) = tokio::sync::mpsc::unbounded_channel();
            let call = PendingCall {
                call_id: 0,
                reducer_name: pr.reducer_name,
                args,
                caller_id: pr.caller_id,
                caller_role: pr.caller_role,
                tenant_id: None,
                lobby_hint: None,
                response_tx: resp_tx,
            };
            if queue_probe.send(call).await.is_err() {
                return Ok(server_error("Reducer queue closed".into()));
            }
            match tokio::time::timeout(std::time::Duration::from_secs(30), resp_rx.recv()).await {
                Ok(Some(resp)) => {
                    if resp.success {
                        use base64::Engine as _;
                        let result_b64 = resp
                            .result
                            .as_deref()
                            .map(|b| base64::engine::general_purpose::STANDARD.encode(b))
                            .unwrap_or_default();
                        Ok(json_response(
                            serde_json::json!({ "ok": true, "result_b64": result_b64 }),
                        ))
                    } else {
                        Ok(json_response(serde_json::json!({
                            "ok": false,
                            "error": resp.error.unwrap_or_else(|| "Reducer error".to_string()),
                        })))
                    }
                }
                Ok(None) => Ok(server_error("Worker dropped response channel".into())),
                Err(_) => Ok(server_error("Proxied call timed out after 30s".into())),
            }
        }

        (&Method::POST, "/cluster/join") => {
            let secret = req
                .headers()
                .get("x-voltra-cluster-secret")
                .and_then(|v| v.to_str().ok());
            if !admin.cluster_bus.validate_secret(secret) {
                let mut r = json_response(serde_json::json!({ "error": "Unauthorized" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body())
                .await
                .map_err(|e| crate::error::VoltraError::network_error(e.to_string()))?;
            let node: crate::cluster::NodeInfo = match serde_json::from_slice(&body_bytes) {
                Ok(n) => n,
                Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
            };
            admin.cluster_bus.add_peer(node);
            Ok(json_response(serde_json::json!({
                "ok": true,
                "peers": admin.cluster_bus.peers_snapshot(),
            })))
        }

        // ── Region + lobby-route endpoints ────────────────────────────────────

        // GET /cluster/regions — list all known regions
        (&Method::GET, "/cluster/regions") => {
            let regions = admin.region_registry.all();
            Ok(json_response(serde_json::json!({
                "my_region": admin.region_registry.my_region,
                "regions":   regions,
                "multi_region": admin.region_registry.is_multi_region(),
            })))
        }

        // GET /cluster/lobby-route?lobby_id=42
        // Returns { region_id, ws_url } for the lobby or 404 if unknown.
        (&Method::GET, p) if p.starts_with("/cluster/lobby-route") => {
            let lobby_id = req
                .uri()
                .query()
                .and_then(|q| q.split('&').find(|s| s.starts_with("lobby_id=")))
                .and_then(|s| s.strip_prefix("lobby_id="))
                .unwrap_or("");
            if lobby_id.is_empty() {
                return Ok(bad_request("Missing lobby_id query param".into()));
            }
            match admin.lobby_routes.lookup(lobby_id) {
                Some(route) => Ok(json_response(serde_json::json!({
                    "lobby_id":  route.lobby_id,
                    "region_id": route.region_id,
                    "ws_url":    route.ws_url,
                }))),
                None => {
                    // Unknown lobby — assume it lives here (single-region fallback).
                    let ws_url = admin
                        .region_registry
                        .ws_url_for(&admin.region_registry.my_region)
                        .unwrap_or_default();
                    Ok(json_response(serde_json::json!({
                        "lobby_id":  lobby_id,
                        "region_id": admin.region_registry.my_region,
                        "ws_url":    ws_url,
                        "fallback":  true,
                    })))
                }
            }
        }

        // POST /cluster/register-lobby — { lobby_id, region_id?, ws_url? }
        // Called by game code after a lobby is created.
        (&Method::POST, "/cluster/register-lobby") => {
            let body_bytes = hyper::body::to_bytes(req.into_body())
                .await
                .map_err(|e| crate::error::VoltraError::network_error(e.to_string()))?;
            let v: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("Invalid JSON: {}", e))),
            };
            let lobby_id = v["lobby_id"].as_str().unwrap_or("").to_string();
            if lobby_id.is_empty() {
                return Ok(bad_request("Missing lobby_id".into()));
            }
            let region_id = v["region_id"]
                .as_str()
                .unwrap_or(&admin.region_registry.my_region)
                .to_string();
            let ws_url = v["ws_url"]
                .as_str()
                .map(|s| s.to_string())
                .or_else(|| {
                    admin
                        .region_registry
                        .get(&region_id)
                        .map(|r| r.ws_url.clone())
                })
                .unwrap_or_default();
            admin.lobby_routes.register(&lobby_id, &region_id, &ws_url);
            Ok(json_response(
                serde_json::json!({ "ok": true, "lobby_id": lobby_id, "region_id": region_id }),
            ))
        }

        // DELETE /cluster/lobby-route?lobby_id=42 — remove a lobby route
        (&Method::DELETE, p) if p.starts_with("/cluster/lobby-route") => {
            let lobby_id = req
                .uri()
                .query()
                .and_then(|q| q.split('&').find(|s| s.starts_with("lobby_id=")))
                .and_then(|s| s.strip_prefix("lobby_id="))
                .unwrap_or("");
            admin.lobby_routes.unregister(lobby_id);
            Ok(json_response(serde_json::json!({ "ok": true })))
        }

        // ── Leaderboard endpoints ─────────────────────────────────────────────

        // GET /leaderboard/top?board=leaderboard&n=100
        (&Method::GET, p) if p.starts_with("/leaderboard/top") => {
            let query = req.uri().query().unwrap_or("");
            let board = query
                .split('&')
                .find(|s| s.starts_with("board="))
                .and_then(|s| s.strip_prefix("board="))
                .unwrap_or("leaderboard");
            let n: usize = query
                .split('&')
                .find(|s| s.starts_with("n="))
                .and_then(|s| s.strip_prefix("n="))
                .and_then(|s| s.parse().ok())
                .unwrap_or(100);
            let result = crate::leaderboard::http_top_entries(&admin.leaderboard, board, n);
            Ok(json_response(result))
        }

        // ── Post-match stat-sync endpoint ─────────────────────────────────────

        // POST /cluster/stat-sync — receive stat write-back jobs from other regions
        (&Method::POST, "/cluster/stat-sync") => {
            let body_bytes = hyper::body::to_bytes(req.into_body())
                .await
                .map_err(|e| crate::error::VoltraError::network_error(e.to_string()))?;
            let result = crate::stat_sync::handle_stat_sync(&tables, &body_bytes);
            Ok(json_response(result))
        }

        // ── Backup endpoint ───────────────────────────────────────────────────
        (&Method::POST, "/backup") => {
            let Some(backup_dir) = admin.backup_dir.clone() else {
                let mut r = json_response(serde_json::json!({
                    "error": "No backup directory configured. Set VOLTRA_BACKUP_DIR or [server] backup_dir."
                }));
                *r.status_mut() = StatusCode::BAD_REQUEST;
                return Ok(r);
            };
            let tbl = tables.clone();
            let wal_path = admin.wal_path.clone();
            let keep = admin.backup_keep;
            let last_seq = global_seq.load(std::sync::atomic::Ordering::Relaxed);
            let result = tokio::task::spawn_blocking(move || {
                let path = crate::backup::backup_now(&tbl, &wal_path, &backup_dir, last_seq)?;
                let _ = crate::backup::rotate_backups(&backup_dir, keep);
                Ok::<_, crate::error::VoltraError>(path)
            })
            .await;
            match result {
                Ok(Ok(path)) => {
                    let meta = crate::backup::read_meta(&path);
                    Ok(json_response(serde_json::json!({
                        "path": path.to_string_lossy(),
                        "last_seq": last_seq,
                        "row_count": meta.map(|m| m.row_count).unwrap_or(0),
                    })))
                }
                Ok(Err(e)) => {
                    let mut r = json_response(serde_json::json!({ "error": e.to_string() }));
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(r)
                }
                Err(e) => {
                    let mut r =
                        json_response(serde_json::json!({ "error": format!("task: {}", e) }));
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(r)
                }
            }
        }

        (&Method::GET, "/metrics") => {
            // Prometheus exposition format (text/plain; version=0.0.4)
            let body = prom.render();
            let mut r = Response::new(Body::from(body));
            r.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("text/plain; version=0.0.4"),
            );
            Ok(r)
        }

        (&Method::GET, "/healthz") => Ok(json_response(serde_json::json!({
            "status": "ok",
            "role": if crate::replication::is_replica() { "replica" } else { "primary" },
            "replication_lag_entries": crate::replication::replication_lag(),
            "total_rows": tables.total_row_count(),
            "active_connections": subscription_manager.active_connections(),
            "active_subscriptions": subscription_manager.active_subscriptions(),
            "wal_sequence": global_seq.load(std::sync::atomic::Ordering::Relaxed),
            "wal_file_size_bytes": wal_writer.wal_file_size_bytes(),
            "uptime_seconds": startup_instant.elapsed().as_secs(),
            "reducer_queue_depth": queue_probe.len(),
            "memory_usage_bytes": get_memory_usage_bytes(),
            "presence_tracked": presence_manager.count(),
            "ttl_active": ttl_manager.count(),
            "slow_consumer_evictions": prom.slow_consumer_evictions_total.get(),
            "subscription_frames_dropped": prom.subscription_frames_dropped_total.get(),
        }))),

        (&Method::GET, "/stats") => {
            let table_list: Vec<_> = tables
                .list_tables()
                .into_iter()
                .map(|name| {
                    let count = tables
                        .list_rows_with_keys(&name)
                        .map(|r| r.len())
                        .unwrap_or(0);
                    let indexes = tables.list_indexes(&name);
                    serde_json::json!({ "name": name, "rows": count, "indexes": indexes })
                })
                .collect();
            let indexes: Vec<_> = tables
                .list_tables()
                .into_iter()
                .flat_map(|name| {
                    tables.list_indexes(&name).into_iter().map(
                        move |field| serde_json::json!({ "table": name.clone(), "field": field }),
                    )
                })
                .collect();
            Ok(json_response(serde_json::json!({
                "tables": table_list,
                "total_rows": tables.total_row_count(),
                "indexes": indexes,
                "wal_sequence": global_seq.load(std::sync::atomic::Ordering::Relaxed),
                "wal_file_size_bytes": wal_writer.wal_file_size_bytes(),
                "snapshot_last_seq": 0u64, // Not easily queryable without scanning snapshot dir
            })))
        }

        (&Method::POST, "/seed") => {
            let body_bytes = hyper::body::to_bytes(req.into_body()).await.map_err(|e| {
                crate::error::VoltraError::network_error(format!("Read body: {}", e))
            })?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => {
                    let mut r = json_response(
                        serde_json::json!({ "error": format!("Invalid JSON: {}", e) }),
                    );
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };
            let row_arr = match payload.get("rows").and_then(|v| v.as_array()) {
                Some(a) => a.clone(),
                None => {
                    let mut r =
                        json_response(serde_json::json!({ "error": "Expected {\"rows\": [...]}" }));
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };
            let mut rows_written = 0usize;
            let mut rows_skipped = 0usize;
            let mut errors = Vec::new();
            for (i, item) in row_arr.iter().enumerate() {
                let triple = match item.as_array() {
                    Some(t) if t.len() == 3 => t,
                    _ => {
                        errors.push(format!("rows[{}]: expected [table, key, data]", i));
                        rows_skipped += 1;
                        continue;
                    }
                };
                let table = match triple[0].as_str() {
                    Some(s) => s.to_string(),
                    None => {
                        errors.push(format!("rows[{}]: table must be string", i));
                        rows_skipped += 1;
                        continue;
                    }
                };
                let key = match triple[1].as_str() {
                    Some(s) => s.to_string(),
                    None => {
                        errors.push(format!("rows[{}]: key must be string", i));
                        rows_skipped += 1;
                        continue;
                    }
                };
                match tables.set_row(table.clone(), key.clone(), triple[2].clone()) {
                    Ok(_) => rows_written += 1,
                    Err(e) => {
                        errors.push(format!("rows[{}] ({}.{}): {}", i, table, key, e));
                        rows_skipped += 1;
                    }
                }
            }
            let mut body =
                serde_json::json!({ "rows_written": rows_written, "rows_skipped": rows_skipped });
            if !errors.is_empty() {
                body["errors"] = serde_json::Value::Array(
                    errors.into_iter().map(serde_json::Value::String).collect(),
                );
            }
            let status = if rows_skipped > 0 && rows_written == 0 {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::OK
            };
            let mut r = json_response(body);
            *r.status_mut() = status;
            Ok(r)
        }

        (&Method::POST, "/migrate") => {
            // Accepts: {"migrations": [{"filename": "001_add_score.toml", "content": "<toml>"}]}
            // Applies each migration via apply_migrations_inline(); returns applied/skipped/errors.
            let body_bytes = hyper::body::to_bytes(req.into_body()).await.map_err(|e| {
                crate::error::VoltraError::network_error(format!("Read body: {}", e))
            })?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => {
                    let mut r = json_response(
                        serde_json::json!({ "error": format!("Invalid JSON: {}", e) }),
                    );
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };
            let mig_arr = match payload.get("migrations").and_then(|v| v.as_array()) {
                Some(a) => a.clone(),
                None => {
                    let mut r = json_response(
                        serde_json::json!({ "error": "Expected {\"migrations\": [...]}" }),
                    );
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };
            let mut applied = 0usize;
            let mut skipped = 0usize;
            let mut errors: Vec<String> = Vec::new();
            for entry in &mig_arr {
                let filename = match entry.get("filename").and_then(|v| v.as_str()) {
                    Some(f) => f.to_string(),
                    None => {
                        errors.push("missing filename field".to_string());
                        skipped += 1;
                        continue;
                    }
                };
                let content = match entry.get("content").and_then(|v| v.as_str()) {
                    Some(c) => c.to_string(),
                    None => {
                        errors.push(format!("{}: missing content field", filename));
                        skipped += 1;
                        continue;
                    }
                };
                match crate::migrations::apply_migration_str(&filename, &content, &tables) {
                    Ok(true) => applied += 1,
                    Ok(false) => skipped += 1,
                    Err(e) => {
                        errors.push(format!("{}: {}", filename, e));
                        skipped += 1;
                    }
                }
            }
            let mut body = serde_json::json!({ "applied": applied, "skipped": skipped });
            if !errors.is_empty() {
                body["errors"] = serde_json::Value::Array(
                    errors.into_iter().map(serde_json::Value::String).collect(),
                );
            }
            Ok(json_response(body))
        }

        (&Method::GET, "/schema") => {
            // Full machine-readable schema — used by `voltra generate`.
            // Tables: from SchemaRegistry (column defs) merged with live table list.
            let mut table_map = serde_json::Map::new();
            // First include all registered schemas with full column info.
            for table_name in schema_registry.list_tables() {
                if let Some(schema) = schema_registry.get(table_name) {
                    let cols: Vec<_> = schema
                        .columns
                        .iter()
                        .map(|c| {
                            serde_json::json!({
                                "name": c.name,
                                "type": c.type_str,
                                "required": c.required,
                                "default": c.default,
                                "key": schema.primary_key.as_deref() == Some(&c.name),
                            })
                        })
                        .collect();
                    let rows = tables
                        .list_rows_with_keys(table_name)
                        .map(|r| r.len())
                        .unwrap_or(0);
                    table_map.insert(
                        table_name.to_string(),
                        serde_json::json!({
                            "columns": cols,
                            "primary_key": schema.primary_key,
                            "rls": format!("{:?}", schema.rls),
                            "rows": rows,
                        }),
                    );
                }
            }
            // Also include live tables that have no schema registered (open schema).
            for table_name in tables.list_tables() {
                if !table_map.contains_key(&table_name) {
                    let rows = tables
                        .list_rows_with_keys(&table_name)
                        .map(|r| r.len())
                        .unwrap_or(0);
                    table_map.insert(
                        table_name,
                        serde_json::json!({ "columns": [], "rows": rows }),
                    );
                }
            }
            let reducer_list: Vec<_> = registry.list_reducers();
            Ok(json_response(serde_json::json!({
                "tables": serde_json::Value::Object(table_map),
                "reducers": reducer_list,
                "version": env!("CARGO_PKG_VERSION"),
            })))
        }

        (&Method::GET, "/tables") => {
            let list: Vec<_> = tables
                .list_tables()
                .into_iter()
                .map(|name| {
                    let count = tables
                        .list_rows_with_keys(&name)
                        .map(|r| r.len())
                        .unwrap_or(0);
                    serde_json::json!({ "name": name, "rows": count })
                })
                .collect();
            Ok(json_response(
                serde_json::json!({ "tables": list, "total_rows": tables.total_row_count() }),
            ))
        }

        (&Method::GET, p) if p.starts_with("/tables/") => {
            let table_name = p.trim_start_matches("/tables/");
            match tables.list_rows_with_keys(table_name) {
                Ok(rows) => {
                    let row_objs: Vec<_> = rows
                        .into_iter()
                        .map(|(key, data)| serde_json::json!({ "row_key": key, "data": data }))
                        .collect();
                    Ok(json_response(
                        serde_json::json!({ "table": table_name, "count": row_objs.len(), "rows": row_objs }),
                    ))
                }
                Err(e) => {
                    let mut r = json_response(serde_json::json!({ "error": e.to_string() }));
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(r)
                }
            }
        }

        // ── Identity / JWT endpoints ──────────────────────────────────────────
        //
        // POST /auth/token  — issue a signed JWT (requires valid API key auth)
        // GET  /auth/public-key — return the server's Ed25519 public key PEM
        //   (no auth required — clients need this to verify tokens independently)
        (&Method::POST, "/auth/token") => {
            // Gate: require a valid API key in the Authorization header.
            // This endpoint is intentionally admin-only; the API key acts as
            // the bootstrap credential that mints user-facing JWTs.
            let auth_header = req
                .headers()
                .get(hyper::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if !auth_header.starts_with("Bearer ") {
                let mut r = json_response(
                    serde_json::json!({ "error": "Unauthorized: missing Authorization header" }),
                );
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            // Accept any non-empty token as an API key; the operator controls
            // access by keeping the VOLTRA_API_KEY secret.
            let provided_key = auth_header.trim_start_matches("Bearer ").trim();
            let api_key_configured = std::env::var("VOLTRA_API_KEY").unwrap_or_default();
            if !api_key_configured.is_empty() && provided_key != api_key_configured {
                let mut r =
                    json_response(serde_json::json!({ "error": "Unauthorized: invalid API key" }));
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }

            let body_bytes = hyper::body::to_bytes(req.into_body()).await.map_err(|e| {
                crate::error::VoltraError::network_error(format!("Read body: {}", e))
            })?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => {
                    let mut r = json_response(
                        serde_json::json!({ "error": format!("Invalid JSON: {}", e) }),
                    );
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };

            let identity = match payload.get("identity").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => {
                    let mut r = json_response(
                        serde_json::json!({ "error": "Missing or empty 'identity' field" }),
                    );
                    *r.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(r);
                }
            };
            let roles: Vec<String> = payload
                .get("roles")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let ttl_secs = payload
                .get("ttl_seconds")
                .and_then(|v| v.as_u64())
                .unwrap_or(3600);

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let expires_at = now + ttl_secs;

            match identity_issuer.issue(&identity, roles, ttl_secs) {
                Ok(token) => Ok(json_response(serde_json::json!({
                    "token": token,
                    "identity": identity,
                    "expires_at": expires_at,
                }))),
                Err(e) => {
                    let mut r = json_response(
                        serde_json::json!({ "error": format!("Token issuance failed: {}", e) }),
                    );
                    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(r)
                }
            }
        }

        (&Method::GET, "/auth/public-key") => {
            let pem = identity_issuer.public_key_pem();
            Ok(json_response(serde_json::json!({ "public_key_pem": pem })))
        }

        // ── User registration ─────────────────────────────────────────────────
        // POST /auth/register   { "email": "...", "password": "...", "role"?: "..." }
        (&Method::POST, "/auth/register") => {
            let body_bytes = hyper::body::to_bytes(req.into_body())
                .await
                .map_err(|e| crate::error::VoltraError::network_error(e.to_string()))?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("invalid JSON: {e}"))),
            };
            let email = payload
                .get("email")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let password = payload
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let role = payload
                .get("role")
                .and_then(|v| v.as_str())
                .unwrap_or("player")
                .to_string();
            let svc = admin.auth_service.clone();
            match tokio::task::spawn_blocking(move || svc.register(&email, &password, &role)).await
            {
                Ok(Ok(user)) => Ok(json_response(serde_json::json!({
                    "id": user.id, "email": user.email, "role": user.role
                }))),
                Ok(Err(e)) => Ok(bad_request(e.to_string())),
                Err(e) => Ok(server_error(e.to_string())),
            }
        }

        // ── Login ─────────────────────────────────────────────────────────────
        // POST /auth/login   { "email": "...", "password": "..." }
        // Returns JWT token for use in Authorization: Bearer <token>
        (&Method::POST, "/auth/login") => {
            let body_bytes = hyper::body::to_bytes(req.into_body())
                .await
                .map_err(|e| crate::error::VoltraError::network_error(e.to_string()))?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("invalid JSON: {e}"))),
            };
            let email = payload
                .get("email")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let password = payload
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let svc = admin.auth_service.clone();
            match tokio::task::spawn_blocking(move || svc.login(&email, &password)).await {
                Ok(Ok((user, token))) => Ok(json_response(serde_json::json!({
                    "id": user.id, "email": user.email, "role": user.role, "token": token
                }))),
                Ok(Err(e)) => {
                    let mut r = bad_request(e.to_string());
                    *r.status_mut() = StatusCode::UNAUTHORIZED;
                    Ok(r)
                }
                Err(e) => Ok(server_error(e.to_string())),
            }
        }

        // ── Current user ──────────────────────────────────────────────────────
        // GET /auth/me   Authorization: Bearer <jwt>
        (&Method::GET, "/auth/me") => {
            let auth_header = req
                .headers()
                .get(hyper::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .trim_start_matches("Bearer ")
                .trim()
                .to_string();
            if auth_header.is_empty() {
                let mut r = bad_request("missing Authorization: Bearer <token>".into());
                *r.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(r);
            }
            let svc = admin.auth_service.clone();
            match tokio::task::spawn_blocking(move || svc.verify_token(&auth_header)).await {
                Ok(Ok(user)) => Ok(json_response(serde_json::json!({
                    "id": user.id, "email": user.email, "role": user.role
                }))),
                Ok(Err(e)) => {
                    let mut r = bad_request(e.to_string());
                    *r.status_mut() = StatusCode::UNAUTHORIZED;
                    Ok(r)
                }
                Err(e) => Ok(server_error(e.to_string())),
            }
        }

        // ── Change password ───────────────────────────────────────────────────
        // POST /auth/change-password   { "user_id": "...", "old_password": "...", "new_password": "..." }
        (&Method::POST, "/auth/change-password") => {
            let body_bytes = hyper::body::to_bytes(req.into_body())
                .await
                .map_err(|e| crate::error::VoltraError::network_error(e.to_string()))?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("invalid JSON: {e}"))),
            };
            let user_id = payload
                .get("user_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let old_pw = payload
                .get("old_password")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let new_pw = payload
                .get("new_password")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let svc = admin.auth_service.clone();
            match tokio::task::spawn_blocking(move || {
                svc.change_password(&user_id, &old_pw, &new_pw)
            })
            .await
            {
                Ok(Ok(())) => Ok(json_response(serde_json::json!({ "ok": true }))),
                Ok(Err(e)) => Ok(bad_request(e.to_string())),
                Err(e) => Ok(server_error(e.to_string())),
            }
        }

        // ── Character save / load ─────────────────────────────────────────────
        // POST /player/save   { "character_id": "...", "user_id": "...", "name": "...", "data": {...} }
        (&Method::POST, "/player/save") => {
            let body_bytes = hyper::body::to_bytes(req.into_body())
                .await
                .map_err(|e| crate::error::VoltraError::network_error(e.to_string()))?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("invalid JSON: {e}"))),
            };
            let char_id = payload
                .get("character_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let user_id = payload
                .get("user_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = payload
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let data = payload
                .get("data")
                .cloned()
                .unwrap_or(serde_json::json!({}));
            if char_id.is_empty() || user_id.is_empty() {
                return Ok(bad_request("character_id and user_id required".into()));
            }
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let db = admin.persistent.clone();
            match tokio::task::spawn_blocking(move || {
                db.save_character(&char_id, &user_id, &name, &data, now)
            })
            .await
            {
                Ok(Ok(())) => Ok(json_response(serde_json::json!({ "ok": true }))),
                Ok(Err(e)) => Ok(bad_request(e.to_string())),
                Err(e) => Ok(server_error(e.to_string())),
            }
        }

        // GET /player/load?character_id=<id>
        (&Method::GET, path) if path.starts_with("/player/load") => {
            let char_id = req
                .uri()
                .query()
                .and_then(|q| q.split('&').find(|p| p.starts_with("character_id=")))
                .map(|p| p.trim_start_matches("character_id=").to_string())
                .unwrap_or_default();
            if char_id.is_empty() {
                return Ok(bad_request("?character_id=<id> required".into()));
            }
            let db = admin.persistent.clone();
            match tokio::task::spawn_blocking(move || db.load_character(&char_id)).await {
                Ok(Ok(Some(data))) => Ok(json_response(serde_json::json!({ "data": data }))),
                Ok(Ok(None)) => {
                    let mut r =
                        json_response(serde_json::json!({ "error": "character not found" }));
                    *r.status_mut() = StatusCode::NOT_FOUND;
                    Ok(r)
                }
                Ok(Err(e)) => Ok(server_error(e.to_string())),
                Err(e) => Ok(server_error(e.to_string())),
            }
        }

        // ── Item catalog ──────────────────────────────────────────────────────
        // GET /catalog?type=weapon
        (&Method::GET, path)
            if path.starts_with("/catalog") && !path.contains('/') || path == "/catalog" =>
        {
            let itype = req
                .uri()
                .query()
                .and_then(|q| q.split('&').find(|p| p.starts_with("type=")))
                .map(|p| p.trim_start_matches("type=").to_string());
            let db = admin.persistent.clone();
            match tokio::task::spawn_blocking(move || db.list_catalog(itype.as_deref())).await {
                Ok(Ok(items)) => Ok(json_response(serde_json::json!({ "items": items }))),
                Ok(Err(e)) => Ok(server_error(e.to_string())),
                Err(e) => Ok(server_error(e.to_string())),
            }
        }

        // POST /catalog   { "id": "...", "name": "...", "type": "...", "stats": {...}, "price": 0 }
        (&Method::POST, "/catalog") => {
            let body_bytes = hyper::body::to_bytes(req.into_body())
                .await
                .map_err(|e| crate::error::VoltraError::network_error(e.to_string()))?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("invalid JSON: {e}"))),
            };
            let id = payload
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = payload
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let itype = payload
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("generic")
                .to_string();
            let stats = payload
                .get("stats")
                .cloned()
                .unwrap_or(serde_json::json!({}));
            let price = payload.get("price").and_then(|v| v.as_i64()).unwrap_or(0);
            if id.is_empty() || name.is_empty() {
                return Ok(bad_request("id and name required".into()));
            }
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let db = admin.persistent.clone();
            match tokio::task::spawn_blocking(move || {
                db.upsert_catalog_item(&id, &name, &itype, &stats, price, now)
            })
            .await
            {
                Ok(Ok(())) => Ok(json_response(serde_json::json!({ "ok": true }))),
                Ok(Err(e)) => Ok(server_error(e.to_string())),
                Err(e) => Ok(server_error(e.to_string())),
            }
        }

        // ── Admin: raw SQL against the persistent SQLite store ────────────────
        // POST /persistent/sql   { "sql": "SELECT ..." }  (admin-auth-gated)
        (&Method::POST, "/persistent/sql") => {
            if let Some(r) = admin_auth_check(&req) {
                return Ok(r);
            }
            let body_bytes = hyper::body::to_bytes(req.into_body())
                .await
                .map_err(|e| crate::error::VoltraError::network_error(e.to_string()))?;
            let payload: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(e) => return Ok(bad_request(format!("invalid JSON: {e}"))),
            };
            let sql = match payload.get("sql").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => return Ok(bad_request("missing 'sql' field".into())),
            };
            let db = admin.persistent.clone();
            match tokio::task::spawn_blocking(move || db.exec_sql(&sql)).await {
                Ok(Ok(rows)) => Ok(json_response(serde_json::json!({ "rows": rows }))),
                Ok(Err(e)) => Ok(bad_request(e.to_string())),
                Err(e) => Ok(server_error(e.to_string())),
            }
        }

        _ => {
            let mut r = Response::new(Body::from("Not Found"));
            *r.status_mut() = StatusCode::NOT_FOUND;
            Ok(r)
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// Resident-set memory of the current process, in bytes (0 if unavailable).
// Moved here from the binary's app module — the admin console is its sole user.
fn get_memory_usage_bytes() -> u64 {
    #[cfg(target_os = "windows")]
    {
        use std::mem;
        #[allow(non_camel_case_types)]
        type HANDLE = *mut std::ffi::c_void;
        #[allow(non_camel_case_types)]
        type DWORD = u32;
        #[allow(non_camel_case_types)]
        type SIZE_T = usize;
        #[repr(C)]
        struct PROCESS_MEMORY_COUNTERS {
            cb: DWORD,
            page_fault_count: DWORD,
            peak_working_set_size: SIZE_T,
            working_set_size: SIZE_T,
            quota_peak_paged_pool_usage: SIZE_T,
            quota_paged_pool_usage: SIZE_T,
            quota_peak_non_paged_pool_usage: SIZE_T,
            quota_non_paged_pool_usage: SIZE_T,
            pagefile_usage: SIZE_T,
            peak_pagefile_usage: SIZE_T,
        }
        #[link(name = "kernel32")]
        extern "system" {
            fn GetCurrentProcess() -> HANDLE;
        }
        #[link(name = "psapi")]
        extern "system" {
            fn GetProcessMemoryInfo(
                process: HANDLE,
                ppsmemcounters: *mut PROCESS_MEMORY_COUNTERS,
                cb: DWORD,
            ) -> i32;
        }
        unsafe {
            let mut pmc: PROCESS_MEMORY_COUNTERS = mem::zeroed();
            pmc.cb = mem::size_of::<PROCESS_MEMORY_COUNTERS>() as DWORD;
            if GetProcessMemoryInfo(GetCurrentProcess(), &mut pmc, pmc.cb) != 0 {
                return pmc.working_set_size as u64;
            }
        }
        0
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(data) = std::fs::read_to_string("/proc/self/statm") {
            if let Some(rss_pages) = data.split_whitespace().nth(1) {
                if let Ok(pages) = rss_pages.parse::<u64>() {
                    return pages * 4096;
                }
            }
        }
        0
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    {
        0
    }
}
