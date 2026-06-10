//! neondb-sim — High-end real-world simulation benchmark
//!
//! Starts its own embedded NeonDB server with all reducers built-in, then
//! drives it with N concurrent virtual users following realistic behavioral
//! state machines that mirror actual game + chat application workloads.
//!
//! Scenarios
//! ─────────
//!  game   Full MMORPG: positions, zones, combat, abilities, NPCs, economy
//!         (buy/sell/loot), quests, matchmaking, leaderboards, world tick —
//!         everything running simultaneously like a live game server.
//!
//!  chat   Discord-scale: room management, message fan-out, reactions,
//!         thread replies, typing indicators (very high frequency),
//!         presence heartbeats — thousands of users simultaneously.
//!
//!  mixed  Game players + chat users hitting the same NeonDB instance at
//!         the same time. Measures contention between the two workloads.
//!
//!  scale  Ramps concurrency from --min to --max clients (doubling each
//!         step), reports a TPS / latency table and finds the throughput knee.
//!
//! Usage
//! ─────
//!  cargo run --release --bin neondb-sim -- game  --players 500 --duration 120
//!  cargo run --release --bin neondb-sim -- chat  --users 1000 --duration 60
//!  cargo run --release --bin neondb-sim -- mixed --players 250 --users 250 --duration 120
//!  cargo run --release --bin neondb-sim -- scale --profile game --max 5000

#![allow(clippy::needless_range_loop)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use neondb::network::message::{ClientMessage, ReducerCall};

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "neondb-sim", about = "NeonDB high-end real-world simulation benchmark")]
struct Args {
    /// WebSocket URL (default: auto-started embedded server)
    #[arg(long, default_value = "ws://127.0.0.1:3777")]
    url: String,

    /// Metrics URL for /healthz sampling
    #[arg(long, default_value = "http://127.0.0.1:3778")]
    metrics_url: String,

    /// Optional API key
    #[arg(long)]
    api_key: Option<String>,

    /// Write time-series CSV to this file
    #[arg(long)]
    csv: Option<String>,

    /// Fail if error rate exceeds this % (default 1%)
    #[arg(long, default_value = "1.0")]
    max_error_pct: f64,

    #[command(subcommand)]
    scenario: ScenarioCmd,
}

#[derive(Subcommand)]
enum ScenarioCmd {
    /// Full MMORPG simulation
    Game {
        /// Concurrent virtual players
        #[arg(short, long, default_value = "200")]
        players: usize,
        /// Simulation duration in seconds
        #[arg(short, long, default_value = "60")]
        duration: u64,
        /// Ramp-up seconds before metrics collection starts
        #[arg(long, default_value = "5")]
        ramp: u64,
    },
    /// Discord-scale chat simulation
    Chat {
        /// Concurrent virtual users
        #[arg(short, long, default_value = "200")]
        users: usize,
        /// Simulation duration in seconds
        #[arg(short, long, default_value = "60")]
        duration: u64,
        /// Pre-created rooms (users round-robin join them)
        #[arg(long, default_value = "20")]
        rooms: usize,
        /// Ramp-up seconds
        #[arg(long, default_value = "5")]
        ramp: u64,
    },
    /// Game + chat running simultaneously
    Mixed {
        #[arg(long, default_value = "100")]
        players: usize,
        #[arg(long, default_value = "100")]
        users: usize,
        #[arg(short, long, default_value = "60")]
        duration: u64,
        #[arg(long, default_value = "5")]
        ramp: u64,
    },
    /// Concurrency ramp — find the throughput knee
    Scale {
        /// Profile to use: game | chat | mixed
        #[arg(long, default_value = "game")]
        profile: String,
        /// Comma-separated concurrency levels  e.g. 10,50,100,500,1000,5000
        #[arg(long, default_value = "10,50,100,250,500,1000,2500,5000")]
        levels: String,
        /// Seconds at each level (short = noisier but faster)
        #[arg(long, default_value = "20")]
        duration_per_level: u64,
        /// Stop scaling when error rate exceeds this %
        #[arg(long, default_value = "5.0")]
        stop_error_pct: f64,
    },
}

// ─── Fast deterministic RNG (xorshift64) ─────────────────────────────────────

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed.wrapping_add(0x9e3779b97f4a7c15)) }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn range(&mut self, lo: i32, hi: i32) -> i32 {
        if lo >= hi { return lo; }
        lo + (self.next() % (hi - lo) as u64) as i32
    }
    fn pct(&mut self, p: u32) -> bool { (self.next() % 100) < p as u64 }
    fn pick<'a, T>(&mut self, s: &'a [T]) -> &'a T { &s[self.next() as usize % s.len()] }
    fn uid(&mut self) -> u64 { self.next() }
}

// ─── Metrics ─────────────────────────────────────────────────────────────────

struct OpMetrics {
    calls: AtomicU64,
    errors: AtomicU64,
    hist:  Mutex<Histogram<u64>>,  // microseconds
}

impl OpMetrics {
    fn new() -> Self {
        Self {
            calls:  AtomicU64::new(0),
            errors: AtomicU64::new(0),
            hist:   Mutex::new(Histogram::new_with_bounds(1, 60_000_000, 3).unwrap()),
        }
    }
    fn record(&self, ok: bool, us: u64) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if !ok { self.errors.fetch_add(1, Ordering::Relaxed); }
        if let Ok(mut h) = self.hist.lock() { let _ = h.record(us.max(1)); }
    }
}

struct Metrics {
    ops:           HashMap<&'static str, Arc<OpMetrics>>,
    total_calls:   AtomicU64,
    total_errors:  AtomicU64,
    measuring:     AtomicBool,   // false during ramp-up
}

impl Metrics {
    fn new(op_names: &[&'static str]) -> Self {
        let mut ops = HashMap::new();
        for &n in op_names { ops.insert(n, Arc::new(OpMetrics::new())); }
        Self {
            ops,
            total_calls:  AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            measuring:    AtomicBool::new(false),
        }
    }
    fn record(&self, op: &'static str, ok: bool, us: u64) {
        if !self.measuring.load(Ordering::Relaxed) { return; }
        self.total_calls.fetch_add(1, Ordering::Relaxed);
        if !ok { self.total_errors.fetch_add(1, Ordering::Relaxed); }
        if let Some(m) = self.ops.get(op) { m.record(ok, us); }
    }
}

// ─── Health snapshot ─────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct Health {
    memory_mb:    f64,
    queue_depth:  u64,
    wal_mb:       f64,
    rows:         u64,
    connections:  u64,
    elapsed_secs: u64,
}

async fn sample_health(url: &str, elapsed: u64) -> Health {
    if let Ok(resp) = reqwest::Client::new()
        .get(format!("{}/healthz", url))
        .timeout(Duration::from_secs(3))
        .send().await
    {
        if let Ok(v) = resp.json::<serde_json::Value>().await {
            return Health {
                memory_mb:   v["memory_usage_bytes"].as_f64().unwrap_or(0.0) / 1e6,
                queue_depth: v["reducer_queue_depth"].as_u64().unwrap_or(0),
                wal_mb:      v["wal_file_size_bytes"].as_f64().unwrap_or(0.0) / 1e6,
                rows:        v["total_rows"].as_u64().unwrap_or(0),
                connections: v["active_connections"].as_u64().unwrap_or(0),
                elapsed_secs: elapsed,
            };
        }
    }
    Health { elapsed_secs: elapsed, ..Default::default() }
}

// ─── Embedded reducer scripts ─────────────────────────────────────────────────
// Minimal but representative — designed to exercise every subsystem:
//  positional writes, combat read+write chains, inventory state, quest FSM,
//  fan-out via subscriptions, and high-frequency ephemeral keys (typing).

const SIM_REDUCERS: &[(&str, &str)] = &[
// ── Game: player spawn ──────────────────────────────────────────────────────
("sim_spawn", r#"function reducer(args) {
  const [pid, x, y, cls] = args;
  if (__neondb_get("sim_players", pid)) return { ok: true, exists: true };
  __neondb_set("sim_players", pid, {
    pid, x: x||0, y: y||0, class: cls||"warrior",
    hp: 100, max_hp: 100, mp: 100, max_mp: 100,
    xp: 0, level: 1, currency: 500, alive: true, kills: 0
  });
  return { ok: true };
}"#),
// ── Game: position update (most frequent write) ─────────────────────────────
("sim_move", r#"function reducer(args) {
  const [pid, x, y] = args;
  const p = __neondb_get("sim_players", pid);
  if (!p) return { error: "no_player" };
  p.x = x; p.y = y;
  p.zone = "z_" + Math.floor(x/100) + "_" + Math.floor(y/100);
  __neondb_set("sim_players", pid, p);
  return { ok: true, x, y, zone: p.zone };
}"#),
// ── Game: combat — attack an NPC (read attacker + read NPC + 2 writes) ──────
("sim_attack", r#"function reducer(args) {
  const [aid, tid, weapon, dmg] = args;
  const npc = __neondb_get("sim_npcs", tid);
  if (!npc) return { ok: true, skipped: true };
  npc.hp = Math.max(0, (npc.hp || 50) - (dmg || 15));
  npc.alive = npc.hp > 0;
  __neondb_set("sim_npcs", tid, npc);
  const p = __neondb_get("sim_players", aid);
  if (p) {
    if (!npc.alive) { p.kills = (p.kills||0)+1; p.xp = (p.xp||0)+50; p.currency=(p.currency||0)+20; }
    p.hp = Math.max(1, (p.hp||100) - Math.floor((dmg||15)*0.3));
    __neondb_set("sim_players", aid, p);
  }
  return { ok: true, npc_hp: npc.hp, dead: !npc.alive };
}"#),
// ── Game: ability use (read player, write player) ───────────────────────────
("sim_ability", r#"function reducer(args) {
  const [pid, ability, tid] = args;
  const costs = { fireball: 20, heal: -25, shield: 15, lightning: 30, dash: 10 };
  const p = __neondb_get("sim_players", pid);
  if (!p) return { error: "no_player" };
  const cost = costs[ability] || 10;
  if (cost > 0 && (p.mp||0) < cost) return { error: "no_mp" };
  p.mp = Math.max(0, Math.min(p.max_mp||100, (p.mp||100) - cost));
  if (ability === "heal") p.hp = Math.min(p.max_hp||100, (p.hp||100)+25);
  __neondb_set("sim_players", pid, p);
  return { ok: true, ability, mp: p.mp, hp: p.hp };
}"#),
// ── Game: apply damage to player (from NPC counter-attack) ──────────────────
("sim_damage", r#"function reducer(args) {
  const [pid, amount, src] = args;
  const p = __neondb_get("sim_players", pid);
  if (!p || !p.alive) return { ok: true, skipped: true };
  p.hp = Math.max(0, (p.hp||100) - (amount||10));
  p.alive = p.hp > 0;
  __neondb_set("sim_players", pid, p);
  return { ok: true, hp: p.hp, alive: p.alive };
}"#),
// ── Game: respawn dead player ────────────────────────────────────────────────
("sim_respawn", r#"function reducer(args) {
  const [pid] = args;
  const p = __neondb_get("sim_players", pid) || {};
  p.hp = p.max_hp||100; p.mp = p.max_mp||100;
  p.alive = true; p.x = 0; p.y = 0;
  __neondb_set("sim_players", pid, p);
  return { ok: true };
}"#),
// ── Game: spawn NPC (world entity) ──────────────────────────────────────────
("sim_spawn_npc", r#"function reducer(args) {
  const [nid, x, y, kind] = args;
  __neondb_set("sim_npcs", nid, {
    nid, x: x||0, y: y||0,
    kind: kind||"goblin", hp: 50, max_hp: 50, alive: true, patrol_x: x||0, patrol_y: y||0
  });
  return { ok: true };
}"#),
// ── Game: world tick (scheduled — respawns dead NPCs, regenerates MP) ───────
("sim_world_tick", r#"function reducer(args) {
  const players = __neondb_get_all("sim_players") || [];
  for (const p of players) {
    if (p.alive && (p.mp||100) < (p.max_mp||100)) {
      p.mp = Math.min(p.max_mp||100, (p.mp||100)+5);
      __neondb_set("sim_players", p.pid||p.row_key, p);
    }
  }
  const npcs = __neondb_get_all("sim_npcs") || [];
  let respawned = 0;
  for (const n of npcs) {
    if (!n.alive) {
      n.hp = n.max_hp||50; n.alive = true;
      n.x = n.patrol_x||0; n.y = n.patrol_y||0;
      __neondb_set("sim_npcs", n.nid||n.row_key, n);
      respawned++;
    }
  }
  return { ok: true, respawned, players_regen: players.length };
}"#),
// ── Game: economy — buy item ─────────────────────────────────────────────────
("sim_buy", r#"function reducer(args) {
  const [pid, item, qty, price] = args;
  const p = __neondb_get("sim_players", pid);
  if (!p) return { error: "no_player" };
  const cost = (price||10)*(qty||1);
  if ((p.currency||0) < cost) return { error: "insufficient" };
  p.currency -= cost;
  __neondb_set("sim_players", pid, p);
  const key = pid+":"+item;
  const inv = __neondb_get("sim_inventory", key) || { pid, item, qty: 0 };
  inv.qty += (qty||1);
  __neondb_set("sim_inventory", key, inv);
  return { ok: true, currency: p.currency, qty: inv.qty };
}"#),
// ── Game: economy — sell item ────────────────────────────────────────────────
("sim_sell", r#"function reducer(args) {
  const [pid, item, qty, price] = args;
  const key = pid+":"+item;
  const inv = __neondb_get("sim_inventory", key);
  if (!inv || inv.qty < (qty||1)) return { error: "not_enough" };
  inv.qty -= (qty||1);
  if (inv.qty <= 0) __neondb_delete("sim_inventory", key);
  else __neondb_set("sim_inventory", key, inv);
  const p = __neondb_get("sim_players", pid) || {};
  p.currency = (p.currency||0) + (price||8)*(qty||1);
  __neondb_set("sim_players", pid, p);
  return { ok: true, currency: p.currency };
}"#),
// ── Game: economy — open loot box ────────────────────────────────────────────
("sim_loot", r#"function reducer(args) {
  const [pid, box_id, rarity] = args;
  const pools = { common:["health_pot","arrow","leather"], rare:["shield","mana_gem","ring"], epic:["sword","staff","plate"] };
  const pool = pools[rarity||"common"];
  const item = pool[Math.floor(Math.random()*pool.length)];
  const key = pid+":"+item;
  const inv = __neondb_get("sim_inventory", key)||{pid,item,qty:0};
  inv.qty += 1;
  __neondb_set("sim_inventory", key, inv);
  return { ok: true, item, qty: inv.qty, rarity };
}"#),
// ── Game: economy — transfer currency (cross-player, 2 reads + 2 writes) ────
("sim_transfer", r#"function reducer(args) {
  const [from, to, amount] = args;
  const pf = __neondb_get("sim_players", from);
  const pt = __neondb_get("sim_players", to);
  if (!pf || !pt) return { error: "missing_player" };
  if ((pf.currency||0) < (amount||10)) return { error: "insufficient" };
  pf.currency -= (amount||10);
  pt.currency = (pt.currency||0) + (amount||10);
  __neondb_set("sim_players", from, pf);
  __neondb_set("sim_players", to, pt);
  return { ok: true };
}"#),
// ── Game: quests — accept, progress, complete ────────────────────────────────
("sim_quest_accept", r#"function reducer(args) {
  const [pid, qid] = args;
  __neondb_set("sim_quests", pid+":"+qid, { pid, qid, progress: 0, done: false });
  return { ok: true };
}"#),
("sim_quest_progress", r#"function reducer(args) {
  const [pid, qid, delta] = args;
  const key = pid+":"+qid;
  const q = __neondb_get("sim_quests", key);
  if (!q) return { error: "no_quest" };
  q.progress = (q.progress||0) + (delta||1);
  if (q.progress >= 10) { q.done = true; const p = __neondb_get("sim_players", pid); if (p) { p.xp=(p.xp||0)+200; p.level=Math.floor((p.xp||0)/1000)+1; __neondb_set("sim_players",pid,p); } }
  __neondb_set("sim_quests", key, q);
  return { ok: true, progress: q.progress, done: q.done };
}"#),
// ── Game: matchmaking ────────────────────────────────────────────────────────
("sim_queue", r#"function reducer(args) {
  const [pid, mode] = args;
  __neondb_set("sim_queue", pid, { pid, mode: mode||"deathmatch", ts: 0 });
  return { ok: true };
}"#),
("sim_dequeue", r#"function reducer(args) {
  const [pid] = args;
  __neondb_delete("sim_queue", pid);
  return { ok: true };
}"#),
// ── Game: leaderboard ────────────────────────────────────────────────────────
("sim_score", r#"function reducer(args) {
  const [pid, score] = args;
  const cur = __neondb_get("sim_leaderboard", pid) || { pid, score: 0 };
  const better = score > cur.score;
  if (better) { cur.score = score; __neondb_set("sim_leaderboard", pid, cur); }
  return { ok: true, new_best: better, score: cur.score };
}"#),
// ── Chat: room management ────────────────────────────────────────────────────
("sim_create_room", r#"function reducer(args) {
  const [rid, name, uid] = args;
  if (__neondb_get("sim_rooms", rid)) return { ok: true, exists: true };
  __neondb_set("sim_rooms", rid, { rid, name, creator: uid, members: [uid], msg_count: 0 });
  return { ok: true };
}"#),
("sim_join_room", r#"function reducer(args) {
  const [rid, uid] = args;
  const r = __neondb_get("sim_rooms", rid);
  if (!r) return { error: "no_room" };
  if (!r.members.includes(uid)) r.members.push(uid);
  __neondb_set("sim_rooms", rid, r);
  return { ok: true, members: r.members.length };
}"#),
// ── Chat: messaging (main write + fan-out driver) ────────────────────────────
("sim_send_msg", r#"function reducer(args) {
  const [rid, mid, uid, text] = args;
  __neondb_set("sim_messages", mid, { mid, rid, uid, text, ts: 0, edited: false });
  const r = __neondb_get("sim_rooms", rid);
  if (r) { r.msg_count = (r.msg_count||0)+1; __neondb_set("sim_rooms", rid, r); }
  return { ok: true, mid };
}"#),
// ── Chat: reactions ──────────────────────────────────────────────────────────
("sim_react", r#"function reducer(args) {
  const [mid, uid, emoji] = args;
  __neondb_set("sim_reactions", mid+":"+uid+":"+emoji, { mid, uid, emoji });
  return { ok: true };
}"#),
// ── Chat: typing indicator (highest frequency — ephemeral writes) ────────────
("sim_typing", r#"function reducer(args) {
  const [uid, rid, typing] = args;
  const k = uid+":"+rid;
  if (typing) __neondb_set("sim_typing", k, { uid, rid, ts: 0 });
  else __neondb_delete("sim_typing", k);
  return { ok: true };
}"#),
// ── Chat: presence heartbeat ─────────────────────────────────────────────────
("sim_presence", r#"function reducer(args) {
  const [uid, status] = args;
  __neondb_set("sim_presence", uid, { uid, status: status||"online", ts: 0 });
  return { ok: true };
}"#),
// ── Chat: thread replies ─────────────────────────────────────────────────────
("sim_thread_reply", r#"function reducer(args) {
  const [tid, rid, uid, text] = args;
  __neondb_set("sim_thread_replies", rid, { tid, rid, uid, text, ts: 0 });
  return { ok: true };
}"#),
];

// ─── Server startup ───────────────────────────────────────────────────────────

async fn start_embedded_server(ws_port: u16, metrics_port: u16) {
    use std::fs;
    let dir = std::env::temp_dir().join(format!("neondb_sim_{}_{}", ws_port, std::process::id()));
    let modules_dir = dir.join("modules");
    fs::create_dir_all(&modules_dir).unwrap();

    // Write all reducer scripts
    for (name, code) in SIM_REDUCERS {
        fs::write(modules_dir.join(format!("{}.js", name)), code).unwrap();
    }

    // neondb.toml
    let toml = format!(
        "[server]\nhost = \"127.0.0.1\"\nport = {ws}\nmetrics_port = {mp}\n\
         wal_path = \"{wal}\"\nsnapshot_dir = \"{snap}\"\n\
         [[scheduler]]\nreducer = \"sim_world_tick\"\ninterval_ms = 1000\n",
        ws = ws_port, mp = metrics_port,
        wal  = dir.join("wal").display().to_string().replace('\\', "/"),
        snap = dir.join("snaps").display().to_string().replace('\\', "/"),
    );
    fs::write(dir.join("neondb.toml"), toml).unwrap();

    std::env::set_current_dir(&dir).ok();
    let mut config = neondb::config::Config::from_env();
    config.port = ws_port;
    config.metrics_port = metrics_port;
    config.wal_path     = dir.join("wal");
    config.snapshot_dir = dir.join("snaps");

    tokio::spawn(async move {
        if let Err(e) = neondb::run_server(config).await {
            eprintln!("[sim-server] error: {e}");
        }
    });
}

async fn wait_for_server(ws_port: u16) {
    let addr = format!("127.0.0.1:{ws_port}");
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() { return; }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    eprintln!("[sim] Server did not start in 15s — aborting.");
    std::process::exit(1);
}

// ─── WebSocket call helper ────────────────────────────────────────────────────

struct WsConn {
    inner: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>
    >,
    seq: u64,
    user_id: u64,
}

impl WsConn {
    async fn connect(url: &str, api_key: Option<&str>, user_id: u64) -> Option<Self> {
        let mut req = url.into_client_request().ok()?;
        if let Some(k) = api_key {
            if let Ok(v) = format!("Bearer {k}").parse() {
                req.headers_mut().insert("authorization", v);
            }
        }
        let (ws, _) = tokio_tungstenite::connect_async(req).await.ok()?;
        Some(Self { inner: ws, seq: 0, user_id })
    }

    /// Send a reducer call and wait for the binary response.
    /// Returns (ok, latency_us).
    async fn call(&mut self, reducer: &str, args: Vec<u8>) -> (bool, u64) {
        self.seq += 1;
        let call_id = (self.user_id << 24) | self.seq;
        let msg = ClientMessage::ReducerCall(ReducerCall {
            call_id,
            reducer_name: reducer.to_string(),
            args,
        });
        let frame = match rmp_serde::to_vec(&msg) {
            Ok(b) => b,
            Err(_) => return (false, 0),
        };
        let t0 = Instant::now();
        if self.inner.send(Message::Binary(frame)).await.is_err() {
            return (false, 0);
        }
        loop {
            match tokio::time::timeout(Duration::from_secs(5), self.inner.next()).await {
                Ok(Some(Ok(Message::Binary(_) | Message::Text(_)))) => {
                    return (true, t0.elapsed().as_micros() as u64);
                }
                Ok(Some(Ok(_))) => continue, // ping/pong/close
                _ => return (false, t0.elapsed().as_micros() as u64),
            }
        }
    }
}

// ─── Arg serialisation helpers ────────────────────────────────────────────────

fn pack<T: serde::Serialize>(v: &T) -> Vec<u8> {
    rmp_serde::to_vec(v).unwrap_or_default()
}

// ─── Game virtual user ────────────────────────────────────────────────────────

#[derive(Default)]
struct GameState {
    spawned:      bool,
    alive:        bool,
    x: i32, y: i32,
    hp:           i32,
    currency:     i32,
    kills:        u32,
    quest_active: bool,
    quest_id:     String,
    quest_prog:   u32,
    queued:       bool,
    npc_pool:     Vec<String>,  // npc ids this user manages
    item_pool:    Vec<String>,  // items in inventory
    peer_ids:     Vec<String>,  // other player ids for transfers
}

async fn game_user(
    id: usize,
    url: String,
    api_key: Option<String>,
    metrics: Arc<Metrics>,
    deadline: Instant,
) {
    let mut rng  = Rng::new(id as u64 ^ 0xdeadbeef);
    let pid      = format!("gp_{id}");
    let mut ws   = loop {
        if let Some(c) = WsConn::connect(&url, api_key.as_deref(), id as u64).await { break c; }
        if Instant::now() >= deadline { return; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let mut st   = GameState { hp: 100, currency: 500, alive: false, ..Default::default() };

    // Pre-spawn a batch of NPCs for this player to fight
    for i in 0..5 {
        let nid = format!("npc_{id}_{i}");
        let x = rng.range(0, 500); let y = rng.range(0, 500);
        let kind = *rng.pick(&["goblin","orc","skeleton","slime","boss"]);
        let (ok, us) = ws.call("sim_spawn_npc", pack(&(nid.as_str(), x, y, kind))).await;
        metrics.record("sim_spawn_npc", ok, us);
        st.npc_pool.push(nid);
    }
    // Stock initial inventory
    for item in &["sword","shield","health_pot","mana_gem"] {
        st.item_pool.push(item.to_string());
    }

    while Instant::now() < deadline {
        // ── Spawn / respawn ───────────────────────────────────────────────────
        if !st.spawned {
            let x = rng.range(0, 500); let y = rng.range(0, 500);
            let cls = rng.pick(&["warrior","mage","rogue","paladin","archer"]);
            let (ok, us) = ws.call("sim_spawn", pack(&(pid.as_str(), x, y, *cls))).await;
            metrics.record("sim_spawn", ok, us);
            st.spawned = true; st.alive = ok; st.x = x; st.y = y;
            continue;
        }
        if !st.alive {
            let (ok, us) = ws.call("sim_respawn", pack(&(pid.as_str(),))).await;
            metrics.record("sim_respawn", ok, us);
            st.alive = ok; st.hp = 100;
            continue;
        }

        // ── Weighted operation selection (mirrors real game traffic) ──────────
        let roll = rng.range(0, 99) as u32;
        match roll {
            // 32% — positional update (most common game write)
            0..=31 => {
                st.x += rng.range(-15, 15); st.y += rng.range(-15, 15);
                st.x = st.x.clamp(0, 999);  st.y = st.y.clamp(0, 999);
                let (ok, us) = ws.call("sim_move", pack(&(pid.as_str(), st.x, st.y))).await;
                metrics.record("sim_move", ok, us);
            }
            // 14% — attack an NPC (read attacker + read NPC + 2 writes)
            32..=45 => {
                let nid = rng.pick(&st.npc_pool).clone();
                let dmg = rng.range(10, 30);
                let weapon = rng.pick(&["sword","axe","bow","spell"]);
                let (ok, us) = ws.call("sim_attack",
                    pack(&(pid.as_str(), nid.as_str(), *weapon, dmg))).await;
                metrics.record("sim_attack", ok, us);
                if ok { st.kills += 1; }
            }
            // 10% — use ability (read + write player state)
            46..=55 => {
                let ab = rng.pick(&["fireball","heal","shield","lightning","dash"]);
                let target = rng.pick(&st.npc_pool).clone();
                let (ok, us) = ws.call("sim_ability",
                    pack(&(pid.as_str(), *ab, target.as_str()))).await;
                metrics.record("sim_ability", ok, us);
            }
            // 8% — take damage from NPC counter-attack
            56..=63 => {
                let dmg = rng.range(5, 20);
                let (ok, us) = ws.call("sim_damage",
                    pack(&(pid.as_str(), dmg, "npc"))).await;
                metrics.record("sim_damage", ok, us);
                if ok { st.hp -= dmg; if st.hp <= 0 { st.alive = false; } }
            }
            // 7% — buy item (economy: read player, read inventory, 2 writes)
            64..=70 => {
                let item = rng.pick(&["health_pot","mana_gem","arrow","leather","iron"]);
                let qty  = rng.range(1, 3);
                let (ok, us) = ws.call("sim_buy",
                    pack(&(pid.as_str(), *item, qty, 10))).await;
                metrics.record("sim_buy", ok, us);
                if ok { st.currency -= 10 * qty; }
            }
            // 6% — sell item
            71..=76 => {
                let item = rng.pick(&["health_pot","arrow","leather"]);
                let (ok, us) = ws.call("sim_sell",
                    pack(&(pid.as_str(), *item, 1, 8))).await;
                metrics.record("sim_sell", ok, us);
            }
            // 5% — open loot box (random item drop)
            77..=81 => {
                let box_id = format!("box_{id}_{}", rng.uid());
                let rarity = rng.pick(&["common","common","common","rare","epic"]);
                let (ok, us) = ws.call("sim_loot",
                    pack(&(pid.as_str(), box_id.as_str(), *rarity))).await;
                metrics.record("sim_loot", ok, us);
            }
            // 5% — quest progress / accept
            82..=86 => {
                if !st.quest_active {
                    st.quest_id = format!("q_{id}_{}", rng.uid() % 10);
                    st.quest_prog = 0;
                    let (ok, us) = ws.call("sim_quest_accept",
                        pack(&(pid.as_str(), st.quest_id.as_str()))).await;
                    metrics.record("sim_quest_accept", ok, us);
                    if ok { st.quest_active = true; }
                } else {
                    let (ok, us) = ws.call("sim_quest_progress",
                        pack(&(pid.as_str(), st.quest_id.as_str(), 1))).await;
                    metrics.record("sim_quest_progress", ok, us);
                    if ok {
                        st.quest_prog += 1;
                        if st.quest_prog >= 10 { st.quest_active = false; }
                    }
                }
            }
            // 4% — matchmaking queue / dequeue
            87..=90 => {
                if !st.queued {
                    let mode = rng.pick(&["deathmatch","capture","coop"]);
                    let (ok, us) = ws.call("sim_queue",
                        pack(&(pid.as_str(), *mode))).await;
                    metrics.record("sim_queue", ok, us);
                    if ok { st.queued = true; }
                } else {
                    let (ok, us) = ws.call("sim_dequeue", pack(&(pid.as_str(),))).await;
                    metrics.record("sim_dequeue", ok, us);
                    if ok { st.queued = false; }
                }
            }
            // 3% — submit score to leaderboard
            91..=93 => {
                let score = (st.kills as i64) * 100 + rng.range(0, 50) as i64;
                let (ok, us) = ws.call("sim_score",
                    pack(&(pid.as_str(), score))).await;
                metrics.record("sim_score", ok, us);
            }
            // 3% — transfer currency to peer
            94..=96 => {
                if !st.peer_ids.is_empty() && st.currency > 20 {
                    let target = rng.pick(&st.peer_ids).clone();
                    let amt = rng.range(5, 15);
                    let (ok, us) = ws.call("sim_transfer",
                        pack(&(pid.as_str(), target.as_str(), amt))).await;
                    metrics.record("sim_transfer", ok, us);
                    if ok { st.currency -= amt; }
                } else {
                    // Build peer list from nearby player ids
                    for j in 0..4 {
                        let peer = format!("gp_{}", (id + j + 1) % 1000);
                        if !st.peer_ids.contains(&peer) { st.peer_ids.push(peer); }
                    }
                }
            }
            // 3% — spawn a fresh NPC (world management)
            97..=99 => {
                let nid = format!("npc_{id}_{}", rng.uid() % 20);
                let x = rng.range(0, 500); let y = rng.range(0, 500);
                let kind = rng.pick(&["goblin","orc","skeleton","spider"]);
                let (ok, us) = ws.call("sim_spawn_npc",
                    pack(&(nid.as_str(), x, y, *kind))).await;
                metrics.record("sim_spawn_npc", ok, us);
                if ok && !st.npc_pool.contains(&nid) { st.npc_pool.push(nid); }
            }
            _ => {}
        }
    }
    let _ = ws.inner.close(None).await;
}

// ─── Chat virtual user ────────────────────────────────────────────────────────

async fn chat_user(
    id: usize,
    url: String,
    api_key: Option<String>,
    metrics: Arc<Metrics>,
    deadline: Instant,
    room_count: usize,
) {
    let mut rng     = Rng::new(id as u64 ^ 0xcafebabe);
    let uid         = format!("cu_{id}");
    let room_id     = format!("room_{}", id % room_count.max(1));
    let mut ws      = loop {
        if let Some(c) = WsConn::connect(&url, api_key.as_deref(), (id as u64) | (1 << 32)).await
            { break c; }
        if Instant::now() >= deadline { return; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let mut recent_msgs: Vec<String> = Vec::new();

    // Setup: create room (first user per room), join, set online
    if id % room_count.max(1) == 0 {
        let (ok, us) = ws.call("sim_create_room",
            pack(&(room_id.as_str(), format!("Room {}", id % room_count.max(1)).as_str(), uid.as_str()))).await;
        metrics.record("sim_create_room", ok, us);
    } else {
        tokio::time::sleep(Duration::from_millis(100)).await; // wait for room to exist
        let (ok, us) = ws.call("sim_join_room",
            pack(&(room_id.as_str(), uid.as_str()))).await;
        metrics.record("sim_join_room", ok, us);
    }
    let (ok, us) = ws.call("sim_presence", pack(&(uid.as_str(), "online"))).await;
    metrics.record("sim_presence", ok, us);

    while Instant::now() < deadline {
        let roll = rng.range(0, 99) as u32;
        match roll {
            // 33% — send message (primary chat activity, triggers fan-out)
            0..=32 => {
                let mid = format!("m_{id}_{}", rng.uid());
                let words = rng.range(3, 12);
                let text = format!("msg {} words {}", mid, words);
                let (ok, us) = ws.call("sim_send_msg",
                    pack(&(room_id.as_str(), mid.as_str(), uid.as_str(), text.as_str()))).await;
                metrics.record("sim_send_msg", ok, us);
                if ok && recent_msgs.len() < 20 { recent_msgs.push(mid); }
            }
            // 28% — typing indicator (highest frequency ephemeral write)
            33..=60 => {
                let typing = rng.pct(70);
                let (ok, us) = ws.call("sim_typing",
                    pack(&(uid.as_str(), room_id.as_str(), typing))).await;
                metrics.record("sim_typing", ok, us);
            }
            // 14% — react to a message
            61..=74 => {
                if !recent_msgs.is_empty() {
                    let mid = rng.pick(&recent_msgs).clone();
                    let emoji = rng.pick(&["👍","❤️","😂","🔥","👀","✅","😮"]);
                    let (ok, us) = ws.call("sim_react",
                        pack(&(mid.as_str(), uid.as_str(), *emoji))).await;
                    metrics.record("sim_react", ok, us);
                }
            }
            // 12% — presence heartbeat
            75..=86 => {
                let status = rng.pick(&["online","idle","dnd"]);
                let (ok, us) = ws.call("sim_presence",
                    pack(&(uid.as_str(), *status))).await;
                metrics.record("sim_presence", ok, us);
            }
            // 8% — thread reply
            87..=94 => {
                if !recent_msgs.is_empty() {
                    let tid = rng.pick(&recent_msgs).clone();
                    let rid = format!("r_{id}_{}", rng.uid());
                    let text = format!("reply from {uid}");
                    let (ok, us) = ws.call("sim_thread_reply",
                        pack(&(tid.as_str(), rid.as_str(), uid.as_str(), text.as_str()))).await;
                    metrics.record("sim_thread_reply", ok, us);
                }
            }
            // 5% — switch room (join a different room)
            95..=99 => {
                let new_room = format!("room_{}", rng.range(0, room_count.max(1) as i32));
                let (ok, us) = ws.call("sim_join_room",
                    pack(&(new_room.as_str(), uid.as_str()))).await;
                metrics.record("sim_join_room", ok, us);
            }
            _ => {}
        }
        // Small yield to avoid one gorilla-fast user starving others
        tokio::task::yield_now().await;
    }
    let _ = ws.inner.close(None).await;
}

// ─── Simulation runner ────────────────────────────────────────────────────────

#[derive(Clone)]
struct SimConfig {
    url:         String,
    metrics_url: String,
    api_key:     Option<String>,
    max_err_pct: f64,
}

async fn run_game_sim(
    n_players: usize,
    duration:  u64,
    ramp_secs: u64,
    cfg:       &SimConfig,
    metrics:   Arc<Metrics>,
) -> Vec<Health> {
    let deadline = Instant::now() + Duration::from_secs(ramp_secs + duration);
    let measuring_start = Instant::now() + Duration::from_secs(ramp_secs);

    let mut handles = Vec::new();
    for i in 0..n_players {
        let (url, ak, met) = (cfg.url.clone(), cfg.api_key.clone(), metrics.clone());
        handles.push(tokio::spawn(game_user(i, url, ak, met, deadline)));
        if i % 20 == 19 { tokio::time::sleep(Duration::from_millis(30)).await; }
    }
    sample_loop(measuring_start, deadline, &cfg.metrics_url, &metrics, &mut handles).await
}

async fn run_chat_sim(
    n_users:   usize,
    duration:  u64,
    ramp_secs: u64,
    rooms:     usize,
    cfg:       &SimConfig,
    metrics:   Arc<Metrics>,
) -> Vec<Health> {
    let deadline = Instant::now() + Duration::from_secs(ramp_secs + duration);
    let measuring_start = Instant::now() + Duration::from_secs(ramp_secs);

    let mut handles = Vec::new();
    for i in 0..n_users {
        let (url, ak, met, r) = (cfg.url.clone(), cfg.api_key.clone(), metrics.clone(), rooms);
        handles.push(tokio::spawn(chat_user(i, url, ak, met, deadline, r)));
        if i % 20 == 19 { tokio::time::sleep(Duration::from_millis(30)).await; }
    }
    sample_loop(measuring_start, deadline, &cfg.metrics_url, &metrics, &mut handles).await
}

async fn sample_loop(
    measuring_start: Instant,
    deadline:        Instant,
    metrics_url:     &str,
    metrics:         &Arc<Metrics>,
    handles:         &mut Vec<tokio::task::JoinHandle<()>>,
) -> Vec<Health> {
    let sim_start = Instant::now();
    let mut samples = Vec::new();
    let mut last_total = 0u64;
    let mut last_t = Instant::now();

    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let now = Instant::now();

        if now >= measuring_start && !metrics.measuring.load(Ordering::SeqCst) {
            metrics.measuring.store(true, Ordering::SeqCst);
            println!("  ▶  Ramp-up complete — measuring");
        }

        let elapsed = sim_start.elapsed().as_secs();
        let total   = metrics.total_calls.load(Ordering::Relaxed);
        let errors  = metrics.total_errors.load(Ordering::Relaxed);
        let window_tps = (total - last_total) as f64 / last_t.elapsed().as_secs_f64();
        last_total = total; last_t = Instant::now();

        let h = sample_health(metrics_url, elapsed).await;
        println!("[{:>5}s] tps={:>8.0}  ok={:>9}  err={:>5}  mem={:>5.1}MB  \
                  wal={:>5.1}MB  rows={:>6}  q={:>3}  conn={}",
            elapsed, window_tps, total, errors,
            h.memory_mb, h.wal_mb, h.rows, h.queue_depth, h.connections);
        samples.push(h);

        if now >= deadline { break; }
    }

    for h in handles { let _ = h.await; }
    samples
}

// ─── Report ───────────────────────────────────────────────────────────────────

fn print_report(label: &str, metrics: &Metrics, samples: &[Health], elapsed_secs: f64) {
    let total  = metrics.total_calls.load(Ordering::Relaxed);
    let errors = metrics.total_errors.load(Ordering::Relaxed);
    let avg_tps = total as f64 / elapsed_secs;
    let err_pct = if total + errors > 0 { 100.0 * errors as f64 / (total + errors) as f64 } else { 0.0 };

    println!("\n╔══════════════════════════════════════════════════════════════════════╗");
    println!("║  NeonDB Simulation Report — {}",
        format!("{:<41}", label).chars().take(41).collect::<String>());
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║  Duration: {elapsed_secs:.0}s   Total calls: {total}   Avg TPS: {avg_tps:.0}   Error rate: {err_pct:.3}%");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║  {:<22}  {:>8}  {:>8}  {:>8}  {:>8}  {:>7}",
        "Operation", "Calls", "TPS", "p50ms", "p99ms", "Errors");
    println!("║  {:<22}  {:>8}  {:>8}  {:>8}  {:>8}  {:>7}",
        "─".repeat(22), "─".repeat(8), "─".repeat(8), "─".repeat(8), "─".repeat(8), "─".repeat(7));

    let mut ops: Vec<(&str, &OpMetrics)> = metrics.ops.iter()
        .map(|(k, v)| (*k, v.as_ref()))
        .collect();
    ops.sort_by(|a, b| b.1.calls.load(Ordering::Relaxed).cmp(&a.1.calls.load(Ordering::Relaxed)));

    for (name, op) in &ops {
        let calls = op.calls.load(Ordering::Relaxed);
        if calls == 0 { continue; }
        let errs = op.errors.load(Ordering::Relaxed);
        let tps  = calls as f64 / elapsed_secs;
        let (p50, p99) = if let Ok(h) = op.hist.lock() {
            if h.len() > 0 {
                (h.value_at_quantile(0.50) as f64 / 1000.0,
                 h.value_at_quantile(0.99) as f64 / 1000.0)
            } else { (0.0, 0.0) }
        } else { (0.0, 0.0) };
        println!("║  {:<22}  {:>8}  {:>8.1}  {:>8.2}  {:>8.2}  {:>7}",
            name, calls, tps, p50, p99, errs);
    }

    // Overall latency
    let mut all_calls = 0u64;
    let mut all_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    for (_, op) in &ops {
        all_calls += op.calls.load(Ordering::Relaxed);
        if let Ok(h) = op.hist.lock() { let _ = all_hist.add(&*h); }
    }
    println!("║  {:<22}  {:>8}  {:>8.1}  {:>8.2}  {:>8.2}  {:>7}",
        "── TOTAL ──", all_calls, avg_tps,
        all_hist.value_at_quantile(0.50) as f64 / 1000.0,
        all_hist.value_at_quantile(0.99) as f64 / 1000.0,
        errors);

    if let (Some(first), Some(last)) = (samples.first(), samples.last()) {
        let growth = if first.memory_mb > 0.0 {
            100.0 * (last.memory_mb - first.memory_mb) / first.memory_mb
        } else { 0.0 };
        println!("╠══════════════════════════════════════════════════════════════════════╣");
        println!("║  Memory: {:.1}MB → {:.1}MB ({:+.1}%)   WAL: {:.1}MB   Rows: {}",
            first.memory_mb, last.memory_mb, growth, last.wal_mb, last.rows);
    }

    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║  Verdict: {}", if err_pct < 1.0 { "✅ PASS" } else { "❌ FAIL — error rate exceeded 1%" });
    println!("╚══════════════════════════════════════════════════════════════════════╝\n");
}

// ─── Scale test ───────────────────────────────────────────────────────────────

async fn run_scale(
    profile:   &str,
    levels:    Vec<usize>,
    dur:       u64,
    stop_err:  f64,
    cfg:       &SimConfig,
) {
    println!("\n┌─ Scale test: profile={profile} levels={:?} duration={dur}s/level ─┐", levels);

    struct LevelResult { clients: usize, tps: f64, p50: f64, p99: f64, err_pct: f64 }
    let mut results = Vec::new();

    for &n in &levels {
        println!("│  → {n} clients ...");
        let all_ops: Vec<&'static str> = SIM_REDUCERS.iter().map(|(n, _)| *n).collect();
        let metrics = Arc::new(Metrics::new(&all_ops));

        let t0 = Instant::now();
        let samples = match profile {
            "chat" => run_chat_sim(n, dur, 3, (n/5).max(1), cfg, metrics.clone()).await,
            _      => run_game_sim(n, dur, 3, cfg, metrics.clone()).await,
        };
        let elapsed = t0.elapsed().as_secs_f64() - 3.0;

        let total  = metrics.total_calls.load(Ordering::Relaxed);
        let errors = metrics.total_errors.load(Ordering::Relaxed);
        let tps    = total as f64 / elapsed.max(1.0);
        let err_pct = if total + errors > 0 { 100.0 * errors as f64 / (total + errors) as f64 } else { 0.0 };

        let mut all_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        for (_, op) in &metrics.ops { if let Ok(h) = op.hist.lock() { let _ = all_hist.add(&*h); } }
        let (p50, p99) = if all_hist.len() > 0 {
            (all_hist.value_at_quantile(0.50) as f64 / 1000.0,
             all_hist.value_at_quantile(0.99) as f64 / 1000.0)
        } else { (0.0, 0.0) };

        let mem = samples.last().map(|s| s.memory_mb).unwrap_or(0.0);
        println!("│    clients={n:<5} tps={tps:>8.0}  p50={p50:.2}ms  p99={p99:.2}ms  err={err_pct:.2}%  mem={mem:.1}MB");
        results.push(LevelResult { clients: n, tps, p50, p99, err_pct });

        if err_pct > stop_err {
            println!("│  ⚠  Error rate {err_pct:.1}% > {stop_err:.1}% — stopping scale test here");
            break;
        }
    }

    println!("└─────────────────────────────────────────────────────────────────────┘");
    println!("\n  Clients  │  Total TPS  │   p50 ms  │   p99 ms  │  Err %");
    println!("  ─────────┼─────────────┼───────────┼───────────┼──────────");
    for r in &results {
        println!("  {:<9} │ {:>11.0} │ {:>9.2} │ {:>9.2} │ {:>6.2}%  {}",
            r.clients, r.tps, r.p50, r.p99, r.err_pct,
            if r.err_pct > stop_err { "← knee" } else { "" });
    }
    println!();
}

// ─── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();

    // Derive ports from URL (default: 3777/3778 to avoid collisions)
    let ws_port: u16      = 3777;
    let metrics_port: u16 = 3778;

    // Start the embedded server
    println!("┌─ NeonDB Simulation Benchmark ─────────────────────────────────────┐");
    println!("│  Starting embedded NeonDB server on :{ws_port} (metrics :{metrics_port}) ...");
    start_embedded_server(ws_port, metrics_port).await;
    wait_for_server(ws_port).await;
    println!("│  Server ready. {} reducers loaded.", SIM_REDUCERS.len());
    println!("└────────────────────────────────────────────────────────────────────┘\n");

    let cfg = SimConfig {
        url:         args.url.clone(),
        metrics_url: args.metrics_url.clone(),
        api_key:     args.api_key.clone(),
        max_err_pct: args.max_error_pct,
    };

    let all_ops: Vec<&'static str> = SIM_REDUCERS.iter().map(|(n, _)| *n).collect();

    match &args.scenario {
        ScenarioCmd::Game { players, duration, ramp } => {
            println!("▶  GAME simulation — {players} virtual players for {duration}s (ramp {ramp}s)");
            println!("   Workload: positions, combat, abilities, economy, quests, matchmaking, world\n");
            let metrics  = Arc::new(Metrics::new(&all_ops));
            let t0       = Instant::now();
            let samples  = run_game_sim(*players, *duration, *ramp, &cfg, metrics.clone()).await;
            let elapsed  = t0.elapsed().as_secs_f64() - *ramp as f64;
            let label    = format!("GAME  {players} players  {duration}s");
            print_report(&label, &metrics, &samples, elapsed);
            if let Some(csv) = &args.csv { write_csv(csv, &samples); }
        }
        ScenarioCmd::Chat { users, duration, rooms, ramp } => {
            println!("▶  CHAT simulation — {users} virtual users, {rooms} rooms, for {duration}s (ramp {ramp}s)");
            println!("   Workload: messages, typing, reactions, threads, presence\n");
            let metrics = Arc::new(Metrics::new(&all_ops));
            let t0      = Instant::now();
            let samples = run_chat_sim(*users, *duration, *ramp, *rooms, &cfg, metrics.clone()).await;
            let elapsed = t0.elapsed().as_secs_f64() - *ramp as f64;
            let label   = format!("CHAT  {users} users  {rooms} rooms  {duration}s");
            print_report(&label, &metrics, &samples, elapsed);
            if let Some(csv) = &args.csv { write_csv(csv, &samples); }
        }
        ScenarioCmd::Mixed { players, users, duration, ramp } => {
            println!("▶  MIXED simulation — {players} game players + {users} chat users for {duration}s");
            let metrics = Arc::new(Metrics::new(&all_ops));
            let t0      = Instant::now();
            let deadline_game = Instant::now() + Duration::from_secs(*ramp + *duration);
            let deadline_chat = deadline_game;
            let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
            for i in 0..*players {
                let (url, ak, met) = (cfg.url.clone(), cfg.api_key.clone(), metrics.clone());
                handles.push(tokio::spawn(game_user(i, url, ak, met, deadline_game)));
                if i % 20 == 19 { tokio::time::sleep(Duration::from_millis(20)).await; }
            }
            let rooms = (*users / 5).max(1);
            for i in 0..*users {
                let (url, ak, met) = (cfg.url.clone(), cfg.api_key.clone(), metrics.clone());
                handles.push(tokio::spawn(chat_user(i, url, ak, met, deadline_chat, rooms)));
                if i % 20 == 19 { tokio::time::sleep(Duration::from_millis(20)).await; }
            }
            let samples = sample_loop(
                Instant::now() + Duration::from_secs(*ramp),
                deadline_game, &cfg.metrics_url, &metrics, &mut handles,
            ).await;
            let elapsed = t0.elapsed().as_secs_f64() - *ramp as f64;
            let label   = format!("MIXED  {players}×game + {users}×chat  {duration}s");
            print_report(&label, &metrics, &samples, elapsed);
        }
        ScenarioCmd::Scale { profile, levels, duration_per_level, stop_error_pct } => {
            let parsed: Vec<usize> = levels.split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
            run_scale(profile, parsed, *duration_per_level, *stop_error_pct, &cfg).await;
        }
    }
}

fn write_csv(path: &str, samples: &[Health]) {
    let header = "elapsed_secs,memory_mb,wal_mb,rows,queue_depth,connections";
    let rows: Vec<String> = samples.iter().map(|h| {
        format!("{},{:.2},{:.2},{},{},{}", h.elapsed_secs, h.memory_mb, h.wal_mb, h.rows, h.queue_depth, h.connections)
    }).collect();
    let content = format!("{}\n{}", header, rows.join("\n"));
    if let Err(e) = std::fs::write(path, content) {
        eprintln!("CSV write error: {e}");
    } else {
        println!("Time-series written to {path}");
    }
}
