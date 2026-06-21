//! Redis protocol server for Voltra.
//!
//! A full RESP2/RESP3 server backed by the MVCC engine:
//! * Read commands run lock-free on MVCC snapshots — parallel across all cores.
//! * Write commands execute inside the single MVCC sequencer — linearizable,
//!   zero lock contention (the architecture Redis itself proves out).
//! * MULTI/EXEC executes atomically in one sequencer batch; WATCH uses MVCC
//!   commit timestamps for conflict detection.
//! * Pub/Sub, blocking list ops, TTLs, 16 logical databases, AOF persistence.

pub mod cmd_hash_list;
pub mod cmd_set_zset;
pub mod cmd_string;
pub mod engine;
pub mod pubsub;
pub mod resp;
pub mod util;

#[cfg(test)]
mod tests;

use crate::mvcc::{MvccStore, Writer};
use bytes::{Bytes, BytesMut};
use engine::{dispatch_data, is_data_command, is_write, SnapDb};
use pubsub::{PubMsg, PubSub};
use resp::{encode, parse_command, Resp};
use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use util::{lossy, parse_f64, parse_i64, upper};

pub const REDIS_VERSION: &str = "7.4.0";

// ─────────────────────────────────────────────────────────────────────────────
// Server context
// ─────────────────────────────────────────────────────────────────────────────

pub struct RedisCtx {
    pub store: MvccStore,
    pub pubsub: Arc<PubSub>,
    pub password: Option<String>,
    pub started: Instant,
    pub connected: AtomicI64,
    pub total_conns: AtomicU64,
    pub total_cmds: AtomicU64,
    pub last_save_secs: AtomicU64,
    pub next_conn_id: AtomicU64,
}

impl RedisCtx {
    pub fn new(store: MvccStore, password: Option<String>) -> Arc<Self> {
        Arc::new(Self {
            store,
            pubsub: Arc::new(PubSub::default()),
            password,
            started: Instant::now(),
            connected: AtomicI64::new(0),
            total_conns: AtomicU64::new(0),
            total_cmds: AtomicU64::new(0),
            last_save_secs: AtomicU64::new(0),
            next_conn_id: AtomicU64::new(1),
        })
    }
}

/// Bind and serve. Returns only on listener error.
pub async fn start_redis_listener(host: String, port: u16, ctx: Arc<RedisCtx>) -> std::io::Result<()> {
    let listener = TcpListener::bind((host.as_str(), port)).await?;
    log::info!("[redis] RESP listener on {host}:{port}");
    serve(listener, ctx).await
}

/// Accept loop over an already-bound listener (used by tests for port 0).
pub async fn serve(listener: TcpListener, ctx: Arc<RedisCtx>) -> std::io::Result<()> {
    loop {
        let (sock, _peer) = listener.accept().await?;
        let _ = sock.set_nodelay(true);
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let _ = handle_conn(sock, ctx).await;
        });
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Connection state
// ─────────────────────────────────────────────────────────────────────────────

struct Conn {
    id: u64,
    db: u32,
    name: String,
    proto: u8,
    authenticated: bool,
    /// MULTI queue: Some(queued commands) while in a transaction.
    multi: Option<Vec<(String, Vec<Bytes>)>>,
    multi_error: bool,
    /// WATCHed keys and the snapshot they were watched at.
    watch: Vec<(u32, Bytes)>,
    watch_ts: u64,
    subs: HashSet<Bytes>,
    psubs: HashSet<Bytes>,
    push_tx: mpsc::UnboundedSender<PubMsg>,
}

impl Conn {
    fn in_subscribe_mode(&self) -> bool {
        !self.subs.is_empty() || !self.psubs.is_empty()
    }
}

async fn handle_conn(mut sock: TcpStream, ctx: Arc<RedisCtx>) -> std::io::Result<()> {
    ctx.connected.fetch_add(1, Ordering::Relaxed);
    ctx.total_conns.fetch_add(1, Ordering::Relaxed);

    let (push_tx, mut push_rx) = mpsc::unbounded_channel::<PubMsg>();
    let mut conn = Conn {
        id: ctx.next_conn_id.fetch_add(1, Ordering::Relaxed),
        db: 0,
        name: String::new(),
        proto: 2,
        authenticated: ctx.password.is_none(),
        multi: None,
        multi_error: false,
        watch: Vec::new(),
        watch_ts: 0,
        subs: HashSet::new(),
        psubs: HashSet::new(),
        push_tx,
    };

    let mut buf = BytesMut::with_capacity(16 * 1024);
    let mut out: Vec<u8> = Vec::with_capacity(16 * 1024);
    let result = loop {
        tokio::select! {
            read = sock.read_buf(&mut buf) => {
                match read {
                    Ok(0) => break Ok(()),
                    Ok(_) => {}
                    Err(e) => break Err(e),
                }
                // Drain every complete pipelined command before flushing.
                loop {
                    match parse_command(&buf) {
                        Ok(Some((args, used))) => {
                            let _ = buf.split_to(used);
                            if args.is_empty() {
                                continue;
                            }
                            ctx.total_cmds.fetch_add(1, Ordering::Relaxed);
                            let quit = step(&ctx, &mut conn, args, &mut out).await;
                            if quit {
                                sock.write_all(&out).await.ok();
                                let _ = sock.flush().await;
                                ctx.connected.fetch_add(-1, Ordering::Relaxed);
                                ctx.pubsub.drop_conn(conn.id);
                                return Ok(());
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            let mut frame = Vec::new();
                            encode(&mut frame, &Resp::err(format!("ERR {e}")), conn.proto);
                            sock.write_all(&frame).await.ok();
                            ctx.connected.fetch_add(-1, Ordering::Relaxed);
                            ctx.pubsub.drop_conn(conn.id);
                            return Ok(());
                        }
                    }
                }
                if !out.is_empty() {
                    sock.write_all(&out).await?;
                    out.clear();
                }
            }
            Some(msg) = push_rx.recv() => {
                let frame = match msg {
                    PubMsg::Message { channel, payload } => Resp::Push(vec![
                        Resp::bulk_str("message"),
                        Resp::Bulk(channel),
                        Resp::Bulk(payload),
                    ]),
                    PubMsg::PMessage { pattern, channel, payload } => Resp::Push(vec![
                        Resp::bulk_str("pmessage"),
                        Resp::Bulk(pattern),
                        Resp::Bulk(channel),
                        Resp::Bulk(payload),
                    ]),
                };
                let mut fb = Vec::new();
                encode(&mut fb, &frame, conn.proto);
                sock.write_all(&fb).await?;
            }
        }
    };

    ctx.connected.fetch_add(-1, Ordering::Relaxed);
    ctx.pubsub.drop_conn(conn.id);
    result
}

/// Process one command; push reply frames onto `out`. Returns true on QUIT.
async fn step(ctx: &Arc<RedisCtx>, conn: &mut Conn, args: Vec<Bytes>, out: &mut Vec<u8>) -> bool {
    let cmd = upper(&args[0]);
    let rest = &args[1..];
    let mut reply_frames: Vec<Resp> = Vec::new();
    let mut quit = false;

    // ── Authentication gate ──────────────────────────────────────────────────
    if !conn.authenticated && !matches!(cmd.as_str(), "AUTH" | "HELLO" | "QUIT" | "RESET") {
        reply_frames.push(Resp::err("NOAUTH Authentication required."));
        for f in reply_frames {
            encode(out, &f, conn.proto);
        }
        return false;
    }

    // ── RESP2 subscriber-mode command restriction ────────────────────────────
    if conn.proto == 2
        && conn.in_subscribe_mode()
        && !matches!(
            cmd.as_str(),
            "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "PING" | "QUIT" | "RESET"
        )
    {
        reply_frames.push(Resp::err(format!(
            "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
            cmd.to_lowercase()
        )));
        for f in reply_frames {
            encode(out, &f, conn.proto);
        }
        return false;
    }

    // ── MULTI queueing ───────────────────────────────────────────────────────
    if conn.multi.is_some() && !matches!(cmd.as_str(), "EXEC" | "DISCARD" | "MULTI" | "WATCH" | "RESET" | "QUIT") {
        if is_data_command(&cmd) || cmd == "PUBLISH" {
            conn.multi.as_mut().unwrap().push((cmd, rest.to_vec()));
            reply_frames.push(Resp::Simple("QUEUED".into()));
        } else {
            conn.multi_error = true;
            reply_frames.push(Resp::err(format!("ERR unknown command '{}'", cmd.to_lowercase())));
        }
        for f in reply_frames {
            encode(out, &f, conn.proto);
        }
        return false;
    }

    match cmd.as_str() {
        // ── connection ───────────────────────────────────────────────────────
        "PING" => {
            if conn.proto == 2 && conn.in_subscribe_mode() {
                reply_frames.push(Resp::Push(vec![
                    Resp::bulk_str("pong"),
                    rest.first().cloned().map(Resp::Bulk).unwrap_or(Resp::bulk_str("")),
                ]));
            } else if let Some(msg) = rest.first() {
                reply_frames.push(Resp::Bulk(msg.clone()));
            } else {
                reply_frames.push(Resp::Simple("PONG".into()));
            }
        }
        "ECHO" => {
            reply_frames.push(match rest.first() {
                Some(m) => Resp::Bulk(m.clone()),
                None => Resp::arity("echo"),
            });
        }
        "QUIT" => {
            reply_frames.push(Resp::ok());
            quit = true;
        }
        "SELECT" => {
            match rest.first().and_then(parse_i64) {
                Some(n) if (0..16).contains(&n) => {
                    conn.db = n as u32;
                    reply_frames.push(Resp::ok());
                }
                Some(_) => reply_frames.push(Resp::err("ERR DB index is out of range")),
                None => reply_frames.push(Resp::not_int()),
            }
        }
        "AUTH" => {
            let supplied = match rest.len() {
                1 => Some((None, lossy(&rest[0]))),
                2 => Some((Some(lossy(&rest[0])), lossy(&rest[1]))),
                _ => None,
            };
            match (supplied, &ctx.password) {
                (Some((user, pass)), Some(expected)) => {
                    let user_ok = user.map(|u| u == "default").unwrap_or(true);
                    if user_ok && &pass == expected {
                        conn.authenticated = true;
                        reply_frames.push(Resp::ok());
                    } else {
                        reply_frames.push(Resp::err("WRONGPASS invalid username-password pair or user is disabled."));
                    }
                }
                (Some(_), None) => reply_frames.push(Resp::err(
                    "ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?",
                )),
                (None, _) => reply_frames.push(Resp::arity("auth")),
            }
        }
        "HELLO" => {
            let mut proto = conn.proto;
            let mut i = 0;
            let mut auth_err = None;
            if let Some(p) = rest.first() {
                match parse_i64(p) {
                    Some(2) => proto = 2,
                    Some(3) => proto = 3,
                    _ => {
                        reply_frames.push(Resp::err("NOPROTO unsupported protocol version"));
                        for f in reply_frames {
                            encode(out, &f, conn.proto);
                        }
                        return false;
                    }
                }
                i = 1;
            }
            while i < rest.len() {
                match upper(&rest[i]).as_str() {
                    "AUTH" if i + 2 < rest.len() + 1 => {
                        let user = lossy(&rest[i + 1]);
                        let pass = rest.get(i + 2).map(lossy).unwrap_or_default();
                        match &ctx.password {
                            Some(expected) if user == "default" && &pass == expected => {
                                conn.authenticated = true;
                            }
                            None => {}
                            _ => auth_err = Some("WRONGPASS invalid username-password pair or user is disabled."),
                        }
                        i += 3;
                    }
                    "SETNAME" => {
                        if let Some(n) = rest.get(i + 1) {
                            conn.name = lossy(n);
                        }
                        i += 2;
                    }
                    _ => {
                        i += 1;
                    }
                }
            }
            if let Some(e) = auth_err {
                reply_frames.push(Resp::err(e));
            } else if !conn.authenticated {
                reply_frames.push(Resp::err("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time"));
            } else {
                conn.proto = proto;
                reply_frames.push(Resp::Map(vec![
                    (Resp::bulk_str("server"), Resp::bulk_str("redis")),
                    (Resp::bulk_str("version"), Resp::bulk_str(REDIS_VERSION)),
                    (Resp::bulk_str("proto"), Resp::Int(proto as i64)),
                    (Resp::bulk_str("id"), Resp::Int(conn.id as i64)),
                    (Resp::bulk_str("mode"), Resp::bulk_str("standalone")),
                    (Resp::bulk_str("role"), Resp::bulk_str("master")),
                    (Resp::bulk_str("modules"), Resp::Array(vec![])),
                ]));
            }
        }
        "RESET" => {
            conn.multi = None;
            conn.multi_error = false;
            conn.watch.clear();
            for ch in conn.subs.drain() {
                ctx.pubsub.unsubscribe(conn.id, &ch);
            }
            for p in conn.psubs.drain() {
                ctx.pubsub.punsubscribe(conn.id, &p);
            }
            conn.db = 0;
            conn.authenticated = ctx.password.is_none();
            reply_frames.push(Resp::Simple("RESET".into()));
        }
        // ── transactions ─────────────────────────────────────────────────────
        "MULTI" => {
            if conn.multi.is_some() {
                reply_frames.push(Resp::err("ERR MULTI calls can not be nested"));
            } else {
                conn.multi = Some(Vec::new());
                conn.multi_error = false;
                reply_frames.push(Resp::ok());
            }
        }
        "DISCARD" => {
            if conn.multi.take().is_some() {
                conn.multi_error = false;
                conn.watch.clear();
                reply_frames.push(Resp::ok());
            } else {
                reply_frames.push(Resp::err("ERR DISCARD without MULTI"));
            }
        }
        "WATCH" => {
            if conn.multi.is_some() {
                reply_frames.push(Resp::err("ERR WATCH inside MULTI is not allowed"));
            } else if rest.is_empty() {
                reply_frames.push(Resp::arity("watch"));
            } else {
                if conn.watch.is_empty() {
                    conn.watch_ts = ctx.store.current_ts();
                }
                for k in rest {
                    conn.watch.push((conn.db, k.clone()));
                }
                reply_frames.push(Resp::ok());
            }
        }
        "UNWATCH" => {
            conn.watch.clear();
            reply_frames.push(Resp::ok());
        }
        "EXEC" => {
            match conn.multi.take() {
                None => reply_frames.push(Resp::err("ERR EXEC without MULTI")),
                Some(_) if conn.multi_error => {
                    conn.multi_error = false;
                    conn.watch.clear();
                    reply_frames.push(Resp::err("EXECABORT Transaction discarded because of previous errors."));
                }
                Some(queued) => {
                    let watch = std::mem::take(&mut conn.watch);
                    let watch_ts = conn.watch_ts;
                    let dbi = conn.db;
                    let ps = ctx.pubsub.clone();
                    let (tx, rx) = oneshot::channel();
                    let sent = ctx
                        .store
                        .apply(move |w: &mut Writer| {
                            for (ns, key) in &watch {
                                if w.head_ts(*ns, key) > watch_ts {
                                    return Box::new(move || {
                                        let _ = tx.send(Resp::NullArray);
                                    });
                                }
                            }
                            let mut results = Vec::with_capacity(queued.len());
                            for (c, a) in queued {
                                if c == "PUBLISH" {
                                    let n = if a.len() == 2 { ps.publish(&a[0], &a[1]) } else { 0 };
                                    results.push(Resp::Int(n as i64));
                                } else {
                                    results.push(dispatch_data(w, dbi, &c, &a));
                                }
                            }
                            Box::new(move || {
                                let _ = tx.send(Resp::Array(results));
                            })
                        })
                        .await;
                    let r = match sent {
                        Ok(()) => rx.await.unwrap_or(Resp::err("ERR store closed")),
                        Err(_) => Resp::err("ERR store closed"),
                    };
                    reply_frames.push(r);
                }
            }
        }
        // ── pub/sub ──────────────────────────────────────────────────────────
        "SUBSCRIBE" | "PSUBSCRIBE" => {
            if rest.is_empty() {
                reply_frames.push(Resp::arity(&cmd));
            } else {
                let pattern_mode = cmd == "PSUBSCRIBE";
                for ch in rest {
                    if pattern_mode {
                        conn.psubs.insert(ch.clone());
                        ctx.pubsub.psubscribe(conn.id, ch.clone(), conn.push_tx.clone());
                    } else {
                        conn.subs.insert(ch.clone());
                        ctx.pubsub.subscribe(conn.id, ch.clone(), conn.push_tx.clone());
                    }
                    let total = (conn.subs.len() + conn.psubs.len()) as i64;
                    reply_frames.push(Resp::Push(vec![
                        Resp::bulk_str(if pattern_mode { "psubscribe" } else { "subscribe" }),
                        Resp::Bulk(ch.clone()),
                        Resp::Int(total),
                    ]));
                }
            }
        }
        "UNSUBSCRIBE" | "PUNSUBSCRIBE" => {
            let pattern_mode = cmd == "PUNSUBSCRIBE";
            let targets: Vec<Bytes> = if rest.is_empty() {
                if pattern_mode {
                    conn.psubs.iter().cloned().collect()
                } else {
                    conn.subs.iter().cloned().collect()
                }
            } else {
                rest.to_vec()
            };
            if targets.is_empty() {
                reply_frames.push(Resp::Push(vec![
                    Resp::bulk_str(if pattern_mode { "punsubscribe" } else { "unsubscribe" }),
                    Resp::Null,
                    Resp::Int((conn.subs.len() + conn.psubs.len()) as i64),
                ]));
            }
            for ch in targets {
                if pattern_mode {
                    conn.psubs.remove(&ch);
                    ctx.pubsub.punsubscribe(conn.id, &ch);
                } else {
                    conn.subs.remove(&ch);
                    ctx.pubsub.unsubscribe(conn.id, &ch);
                }
                let total = (conn.subs.len() + conn.psubs.len()) as i64;
                reply_frames.push(Resp::Push(vec![
                    Resp::bulk_str(if pattern_mode { "punsubscribe" } else { "unsubscribe" }),
                    Resp::Bulk(ch),
                    Resp::Int(total),
                ]));
            }
        }
        "PUBLISH" => {
            if rest.len() != 2 {
                reply_frames.push(Resp::arity("publish"));
            } else {
                let n = ctx.pubsub.publish(&rest[0], &rest[1]);
                reply_frames.push(Resp::Int(n as i64));
            }
        }
        "PUBSUB" => {
            let sub = rest.first().map(upper).unwrap_or_default();
            match sub.as_str() {
                "CHANNELS" => {
                    let chans = ctx.pubsub.channels_list(rest.get(1));
                    reply_frames.push(Resp::Array(chans.into_iter().map(Resp::Bulk).collect()));
                }
                "NUMSUB" => {
                    let mut pairs = Vec::new();
                    for ch in &rest[1..] {
                        pairs.push(Resp::Bulk(ch.clone()));
                        pairs.push(Resp::Int(ctx.pubsub.numsub(ch) as i64));
                    }
                    reply_frames.push(Resp::Array(pairs));
                }
                "NUMPAT" => reply_frames.push(Resp::Int(ctx.pubsub.numpat() as i64)),
                "SHARDCHANNELS" => reply_frames.push(Resp::Array(vec![])),
                "SHARDNUMSUB" => reply_frames.push(Resp::Array(vec![])),
                _ => reply_frames.push(Resp::err(format!("ERR Unknown PUBSUB subcommand '{sub}'"))),
            }
        }
        // ── blocking list ops ────────────────────────────────────────────────
        "BLPOP" | "BRPOP" => {
            reply_frames.push(blocking_pop(ctx, conn.db, rest, cmd == "BLPOP").await);
        }
        "BLMOVE" => {
            reply_frames.push(blocking_lmove(ctx, conn.db, rest).await);
        }
        "BRPOPLPUSH" => {
            // BRPOPLPUSH src dst timeout == BLMOVE src dst RIGHT LEFT timeout
            if rest.len() != 3 {
                reply_frames.push(Resp::arity("brpoplpush"));
            } else {
                let remapped =
                    vec![rest[0].clone(), rest[1].clone(), Bytes::from_static(b"RIGHT"), Bytes::from_static(b"LEFT"), rest[2].clone()];
                reply_frames.push(blocking_lmove(ctx, conn.db, &remapped).await);
            }
        }
        // ── server / introspection ───────────────────────────────────────────
        "INFO" => reply_frames.push(info_reply(ctx, rest)),
        "COMMAND" => {
            let sub = rest.first().map(upper).unwrap_or_default();
            match sub.as_str() {
                "COUNT" => reply_frames.push(Resp::Int(160)),
                "DOCS" | "INFO" => reply_frames.push(Resp::Map(vec![])),
                _ => reply_frames.push(Resp::Array(vec![])),
            }
        }
        "CONFIG" => reply_frames.push(config_reply(rest)),
        "CLIENT" => reply_frames.push(client_reply(conn, rest)),
        "TIME" => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            reply_frames.push(Resp::Array(vec![
                Resp::bulk_str(now.as_secs().to_string()),
                Resp::bulk_str(now.subsec_micros().to_string()),
            ]));
        }
        "DEBUG" => {
            let sub = rest.first().map(upper).unwrap_or_default();
            match sub.as_str() {
                "SLEEP" => {
                    let secs = rest.get(1).and_then(parse_f64).unwrap_or(0.0);
                    tokio::time::sleep(Duration::from_secs_f64(secs.clamp(0.0, 60.0))).await;
                    reply_frames.push(Resp::ok());
                }
                "JMAP" | "SET-ACTIVE-EXPIRE" | "QUICKLIST-PACKED-THRESHOLD" | "CHANGE-REPL-ID" => {
                    reply_frames.push(Resp::ok())
                }
                _ => reply_frames.push(Resp::err(format!("ERR DEBUG subcommand '{sub}' not supported"))),
            }
        }
        "SAVE" | "BGSAVE" | "BGREWRITEAOF" => {
            let r = ctx.store.save().await;
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            ctx.last_save_secs.store(now_secs, Ordering::Relaxed);
            reply_frames.push(match (cmd.as_str(), r) {
                ("SAVE", Ok(())) => Resp::ok(),
                ("BGSAVE", Ok(())) => Resp::Simple("Background saving started".into()),
                (_, Ok(())) => Resp::Simple("Background append only file rewriting started".into()),
                (_, Err(e)) => Resp::err(format!("ERR save failed: {e}")),
            });
        }
        "LASTSAVE" => reply_frames.push(Resp::Int(ctx.last_save_secs.load(Ordering::Relaxed) as i64)),
        "WAIT" => reply_frames.push(Resp::Int(0)),
        "MEMORY" => {
            let sub = rest.first().map(upper).unwrap_or_default();
            match sub.as_str() {
                "USAGE" => match rest.get(1) {
                    Some(key) => {
                        let snap_ts = ctx.store.current_ts();
                        match ctx.store.get_at(conn.db, key, snap_ts) {
                            Some(d) => {
                                let approx = rmp_serde::to_vec(&d).map(|v| v.len()).unwrap_or(0);
                                reply_frames.push(Resp::Int((approx + key.len() + 64) as i64));
                            }
                            None => reply_frames.push(Resp::Null),
                        }
                    }
                    None => reply_frames.push(Resp::arity("memory|usage")),
                },
                "DOCTOR" => reply_frames.push(Resp::bulk_str("Sam, I detected no memory issues.")),
                _ => reply_frames.push(Resp::err(format!("ERR MEMORY subcommand '{sub}' not supported"))),
            }
        }
        "SHUTDOWN" => {
            reply_frames.push(Resp::err("ERR SHUTDOWN is disabled — Voltra manages this process lifecycle"));
        }
        "SLOWLOG" => {
            let sub = rest.first().map(upper).unwrap_or_default();
            match sub.as_str() {
                "GET" => reply_frames.push(Resp::Array(vec![])),
                "LEN" => reply_frames.push(Resp::Int(0)),
                "RESET" => reply_frames.push(Resp::ok()),
                _ => reply_frames.push(Resp::err("ERR Unknown SLOWLOG subcommand")),
            }
        }
        "ACL" => {
            let sub = rest.first().map(upper).unwrap_or_default();
            match sub.as_str() {
                "WHOAMI" => reply_frames.push(Resp::bulk_str("default")),
                "LIST" => reply_frames.push(Resp::Array(vec![Resp::bulk_str("user default on nopass ~* &* +@all")])),
                "CAT" => reply_frames.push(Resp::Array(vec![])),
                _ => reply_frames.push(Resp::err("ERR Unknown ACL subcommand")),
            }
        }
        "REPLICAOF" | "SLAVEOF" => {
            reply_frames.push(Resp::err("ERR REPLICAOF is not supported — use Voltra replication (VOLTRA_ROLE/VOLTRA_PRIMARY_URL)"));
        }
        "SCRIPT" | "EVAL" | "EVALSHA" | "FUNCTION" | "FCALL" => {
            reply_frames.push(Resp::err("ERR Lua scripting is not supported — use Voltra reducers (JS/WASM/native) for server-side logic"));
        }
        // ── data plane ───────────────────────────────────────────────────────
        _ if is_data_command(&cmd) => {
            let r = if is_write(&cmd) {
                let c = cmd.clone();
                let a = rest.to_vec();
                let dbi = conn.db;
                let (tx, rx) = oneshot::channel();
                let sent = ctx
                    .store
                    .apply(move |w: &mut Writer| {
                        let r = dispatch_data(w, dbi, &c, &a);
                        Box::new(move || {
                            let _ = tx.send(r);
                        })
                    })
                    .await;
                match sent {
                    Ok(()) => rx.await.unwrap_or(Resp::err("ERR store closed")),
                    Err(_) => Resp::err("ERR store closed"),
                }
            } else {
                let mut snap = SnapDb { store: &ctx.store, ts: ctx.store.current_ts() };
                dispatch_data(&mut snap, conn.db, &cmd, rest)
            };
            reply_frames.push(r);
        }
        _ => {
            reply_frames.push(Resp::err(format!(
                "ERR unknown command '{}', with args beginning with: ",
                cmd.to_lowercase()
            )));
        }
    }

    for f in reply_frames {
        encode(out, &f, conn.proto);
    }
    quit
}

// ─────────────────────────────────────────────────────────────────────────────
// Blocking ops (poll the sequencer; 20ms cadence like Redis's busy-key retry)
// ─────────────────────────────────────────────────────────────────────────────

async fn blocking_pop(ctx: &Arc<RedisCtx>, dbi: u32, args: &[Bytes], left: bool) -> Resp {
    if args.len() < 2 {
        return Resp::arity(if left { "blpop" } else { "brpop" });
    }
    let Some(timeout) = parse_f64(&args[args.len() - 1]).filter(|t| *t >= 0.0) else {
        return Resp::err("ERR timeout is not a float or out of range");
    };
    let keys: Vec<Bytes> = args[..args.len() - 1].to_vec();
    let deadline = if timeout == 0.0 {
        None
    } else {
        Some(Instant::now() + Duration::from_secs_f64(timeout))
    };
    loop {
        let keys_try = keys.clone();
        let (tx, rx) = oneshot::channel();
        let sent = ctx
            .store
            .apply(move |w: &mut Writer| {
                let mut result = Resp::NullArray;
                for k in &keys_try {
                    let pop = dispatch_data(w, dbi, if left { "LPOP" } else { "RPOP" }, std::slice::from_ref(k));
                    if let Resp::Bulk(v) = pop {
                        result = Resp::Array(vec![Resp::Bulk(k.clone()), Resp::Bulk(v)]);
                        break;
                    }
                    if matches!(pop, Resp::Error(_)) {
                        result = pop;
                        break;
                    }
                }
                Box::new(move || {
                    let _ = tx.send(result);
                })
            })
            .await;
        let r = match sent {
            Ok(()) => rx.await.unwrap_or(Resp::err("ERR store closed")),
            Err(_) => return Resp::err("ERR store closed"),
        };
        if !matches!(r, Resp::NullArray) {
            return r;
        }
        if let Some(d) = deadline {
            if Instant::now() >= d {
                return Resp::NullArray;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn blocking_lmove(ctx: &Arc<RedisCtx>, dbi: u32, args: &[Bytes]) -> Resp {
    if args.len() != 5 {
        return Resp::arity("blmove");
    }
    let Some(timeout) = parse_f64(&args[4]).filter(|t| *t >= 0.0) else {
        return Resp::err("ERR timeout is not a float or out of range");
    };
    let deadline = if timeout == 0.0 {
        None
    } else {
        Some(Instant::now() + Duration::from_secs_f64(timeout))
    };
    let move_args: Vec<Bytes> = args[..4].to_vec();
    loop {
        let a = move_args.clone();
        let (tx, rx) = oneshot::channel();
        let sent = ctx
            .store
            .apply(move |w: &mut Writer| {
                let r = dispatch_data(w, dbi, "LMOVE", &a);
                Box::new(move || {
                    let _ = tx.send(r);
                })
            })
            .await;
        let r = match sent {
            Ok(()) => rx.await.unwrap_or(Resp::err("ERR store closed")),
            Err(_) => return Resp::err("ERR store closed"),
        };
        if !matches!(r, Resp::Null) {
            return r;
        }
        if let Some(d) = deadline {
            if Instant::now() >= d {
                return Resp::Null;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// INFO / CONFIG / CLIENT
// ─────────────────────────────────────────────────────────────────────────────

fn info_reply(ctx: &Arc<RedisCtx>, args: &[Bytes]) -> Resp {
    let section = args.first().map(upper).unwrap_or_default();
    let want = |s: &str| section.is_empty() || section == "ALL" || section == "EVERYTHING" || section == s;
    let uptime = ctx.started.elapsed().as_secs();
    let mut s = String::new();
    if want("SERVER") {
        s.push_str(&format!(
            "# Server\r\nredis_version:{REDIS_VERSION}\r\nredis_git_sha1:0\r\nredis_mode:standalone\r\n\
             os:Voltra\r\narch_bits:64\r\nprocess_id:{}\r\ntcp_port:6379\r\nuptime_in_seconds:{uptime}\r\n\
             uptime_in_days:{}\r\nexecutable:voltra\r\nconfig_file:\r\n\r\n",
            std::process::id(),
            uptime / 86400
        ));
    }
    if want("CLIENTS") {
        s.push_str(&format!(
            "# Clients\r\nconnected_clients:{}\r\nblocked_clients:0\r\nmaxclients:50000\r\n\r\n",
            ctx.connected.load(Ordering::Relaxed).max(0)
        ));
    }
    if want("MEMORY") {
        s.push_str("# Memory\r\nused_memory:0\r\nused_memory_human:0B\r\nmaxmemory:0\r\nmaxmemory_policy:noeviction\r\n\r\n");
    }
    if want("PERSISTENCE") {
        s.push_str(&format!(
            "# Persistence\r\nloading:0\r\nrdb_last_save_time:{}\r\nrdb_bgsave_in_progress:0\r\naof_enabled:1\r\naof_rewrite_in_progress:0\r\n\r\n",
            ctx.last_save_secs.load(Ordering::Relaxed)
        ));
    }
    if want("STATS") {
        s.push_str(&format!(
            "# Stats\r\ntotal_connections_received:{}\r\ntotal_commands_processed:{}\r\ninstantaneous_ops_per_sec:0\r\nkeyspace_hits:0\r\nkeyspace_misses:0\r\npubsub_channels:{}\r\npubsub_patterns:{}\r\n\r\n",
            ctx.total_conns.load(Ordering::Relaxed),
            ctx.total_cmds.load(Ordering::Relaxed),
            ctx.pubsub.channels_list(None).len(),
            ctx.pubsub.numpat()
        ));
    }
    if want("REPLICATION") {
        s.push_str("# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_replid:voltra000000000000000000000000000000000000\r\nmaster_repl_offset:0\r\n\r\n");
    }
    if want("CPU") {
        s.push_str("# CPU\r\nused_cpu_sys:0.0\r\nused_cpu_user:0.0\r\n\r\n");
    }
    if want("KEYSPACE") {
        s.push_str("# Keyspace\r\n");
        for db in 0..16u32 {
            let n = ctx.store.ns_len(db);
            if n > 0 {
                s.push_str(&format!("db{db}:keys={n},expires={},avg_ttl=0\r\n", ctx.store.ttl_count(db)));
            }
        }
        s.push_str("\r\n");
    }
    Resp::Verbatim("txt", Bytes::from(s))
}

fn config_reply(args: &[Bytes]) -> Resp {
    let sub = args.first().map(upper).unwrap_or_default();
    match sub.as_str() {
        "GET" => {
            let mut pairs: Vec<(Resp, Resp)> = Vec::new();
            let defaults: &[(&str, &str)] = &[
                ("maxmemory", "0"),
                ("maxmemory-policy", "noeviction"),
                ("appendonly", "yes"),
                ("save", "3600 1 300 100 60 10000"),
                ("timeout", "0"),
                ("databases", "16"),
                ("maxclients", "50000"),
                ("tcp-keepalive", "300"),
                ("proto-max-bulk-len", "536870912"),
            ];
            for pat in &args[1..] {
                for (k, v) in defaults {
                    if util::glob_match(pat, &Bytes::copy_from_slice(k.as_bytes()))
                        && !pairs.iter().any(|(pk, _)| matches!(pk, Resp::Bulk(b) if b == k.as_bytes()))
                    {
                        pairs.push((Resp::bulk_str(*k), Resp::bulk_str(*v)));
                    }
                }
            }
            Resp::Map(pairs)
        }
        "SET" => Resp::ok(),       // accepted and ignored — Voltra config governs
        "RESETSTAT" => Resp::ok(),
        "REWRITE" => Resp::ok(),
        _ => Resp::err(format!("ERR Unknown CONFIG subcommand '{sub}'")),
    }
}

fn client_reply(conn: &mut Conn, args: &[Bytes]) -> Resp {
    let sub = args.first().map(upper).unwrap_or_default();
    match sub.as_str() {
        "ID" => Resp::Int(conn.id as i64),
        "GETNAME" => Resp::bulk_str(conn.name.clone()),
        "SETNAME" => match args.get(1) {
            Some(n) if !n.contains(&b' ') => {
                conn.name = lossy(n);
                Resp::ok()
            }
            Some(_) => Resp::err("ERR Client names cannot contain spaces, newlines or special characters."),
            None => Resp::arity("client|setname"),
        },
        "SETINFO" => Resp::ok(), // lib-name / lib-ver from client SDKs
        "INFO" => Resp::bulk_str(format!(
            "id={} addr=0.0.0.0:0 name={} db={} resp={}",
            conn.id, conn.name, conn.db, conn.proto
        )),
        "LIST" => Resp::bulk_str(format!(
            "id={} addr=0.0.0.0:0 name={} db={} resp={}\n",
            conn.id, conn.name, conn.db, conn.proto
        )),
        "NO-EVICT" | "NO-TOUCH" | "UNPAUSE" => Resp::ok(),
        _ => Resp::err(format!("ERR Unknown CLIENT subcommand '{sub}'")),
    }
}
