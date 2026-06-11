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
use neondb::reducer::context::ReducerContext;
use neondb::reducer::native::NativeReducerBackend;
use neondb::reducer::registry::NativeReducerItem;
use neondb::ServerHandle;
use serde_json::json;
use std::collections::VecDeque;

// ═══════════════════════════════════════════════════════════════════════════════
// Native Rust sim reducers — bypass the JS interpreter entirely.
//
// These are registered via `inventory::submit!` at link time.  The server's
// ReducerRegistry discovers them BEFORE loading JS modules from disk, so the
// native versions take priority.  Each reducer is a plain `fn` that operates
// directly on ReducerContext — no serialization bridge, no interpreter overhead.
//
// Effect: 10-50× faster per call than the JS equivalents, unlocking 15-30K CCU
// on a single node where the JS path peaks around 5K CCU.
// ═══════════════════════════════════════════════════════════════════════════════

fn ok_bytes() -> Vec<u8> {
    rmp_serde::to_vec(&json!({"ok": true})).unwrap_or_default()
}

fn args_to_vec(args: &[u8]) -> Vec<serde_json::Value> {
    rmp_serde::from_slice(args).unwrap_or_default()
}

// ── Game: player spawn ──────────────────────────────────────────────────────
fn sim_spawn(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let pid = a.first().and_then(|v| v.as_str()).unwrap_or("p");
    let x = a.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
    let y = a.get(2).and_then(|v| v.as_f64()).unwrap_or(0.0);
    let cls = a.get(3).and_then(|v| v.as_str()).unwrap_or("warrior");
    if ctx.get_row("sim_players", pid)?.is_some() {
        return Ok(rmp_serde::to_vec(&json!({"ok": true, "exists": true}))?);
    }
    // Lobby tag from the pid prefix ("l42_p123" → "l42") — lets clients
    // subscribe per instance: sim_players WHERE lobby = 'l42'.
    let lobby = pid.split('_').next().unwrap_or("");
    ctx.set_row("sim_players".into(), pid.into(), json!({
        "pid": pid, "lobby": lobby, "x": x, "y": y, "class": cls,
        "hp": 100, "max_hp": 100, "mp": 100, "max_mp": 100,
        "xp": 0, "level": 1, "currency": 500, "alive": true, "kills": 0
    }))?;
    Ok(ok_bytes())
}
inventory::submit! { NativeReducerItem { name: "sim_spawn", make: || Box::new(NativeReducerBackend::new(sim_spawn)) } }

// ── Game: position update ───────────────────────────────────────────────────
fn sim_move(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let pid = a.first().and_then(|v| v.as_str()).unwrap_or("p");
    let x = a.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
    let y = a.get(2).and_then(|v| v.as_f64()).unwrap_or(0.0);
    let mut p = match ctx.get_row("sim_players", pid)? {
        Some(v) => v, None => return Ok(rmp_serde::to_vec(&json!({"error": "no_player"}))?),
    };
    let zx = (x / 100.0).floor() as i64;
    let zy = (y / 100.0).floor() as i64;
    let zone = format!("z_{}_{}", zx, zy);
    p["x"] = json!(x); p["y"] = json!(y); p["zone"] = json!(&zone);
    ctx.set_row("sim_players".into(), pid.into(), p)?;
    Ok(rmp_serde::to_vec(&json!({"ok": true, "x": x, "y": y, "zone": zone}))?)
}
inventory::submit! { NativeReducerItem { name: "sim_move", make: || Box::new(NativeReducerBackend::new(sim_move)) } }

// ── Game: combat — attack NPC ───────────────────────────────────────────────
fn sim_attack(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let aid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let tid = a.get(1).and_then(|v| v.as_str()).unwrap_or("");
    let dmg = a.get(3).and_then(|v| v.as_i64()).unwrap_or(15);
    let mut npc = match ctx.get_row("sim_npcs", tid)? {
        Some(v) => v, None => return Ok(rmp_serde::to_vec(&json!({"ok": true, "skipped": true}))?),
    };
    let hp = (npc["hp"].as_i64().unwrap_or(50) - dmg).max(0);
    npc["hp"] = json!(hp);
    npc["alive"] = json!(hp > 0);
    ctx.set_row("sim_npcs".into(), tid.into(), npc.clone())?;
    if let Some(mut p) = ctx.get_row("sim_players", aid)? {
        if hp == 0 {
            p["kills"] = json!(p["kills"].as_i64().unwrap_or(0) + 1);
            p["xp"] = json!(p["xp"].as_i64().unwrap_or(0) + 50);
            p["currency"] = json!(p["currency"].as_i64().unwrap_or(0) + 20);
        }
        let php = (p["hp"].as_i64().unwrap_or(100) - (dmg as f64 * 0.3) as i64).max(1);
        p["hp"] = json!(php);
        ctx.set_row("sim_players".into(), aid.into(), p)?;
    }
    Ok(rmp_serde::to_vec(&json!({"ok": true, "npc_hp": hp, "dead": hp == 0}))?)
}
inventory::submit! { NativeReducerItem { name: "sim_attack", make: || Box::new(NativeReducerBackend::new(sim_attack)) } }

// ── Game: ability use ───────────────────────────────────────────────────────
fn sim_ability(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let pid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let ability = a.get(1).and_then(|v| v.as_str()).unwrap_or("fireball");
    let mut p = match ctx.get_row("sim_players", pid)? {
        Some(v) => v, None => return Ok(rmp_serde::to_vec(&json!({"error": "no_player"}))?),
    };
    let cost: i64 = match ability {
        "fireball" => 20, "heal" => -25, "shield" => 15, "lightning" => 30, "dash" => 10, _ => 10,
    };
    let mp = p["mp"].as_i64().unwrap_or(100);
    let max_mp = p["max_mp"].as_i64().unwrap_or(100);
    if cost > 0 && mp < cost { return Ok(rmp_serde::to_vec(&json!({"error": "no_mp"}))?); }
    let new_mp = (mp - cost).max(0).min(max_mp);
    p["mp"] = json!(new_mp);
    if ability == "heal" {
        let max_hp = p["max_hp"].as_i64().unwrap_or(100);
        p["hp"] = json!((p["hp"].as_i64().unwrap_or(100) + 25).min(max_hp));
    }
    ctx.set_row("sim_players".into(), pid.into(), p.clone())?;
    Ok(rmp_serde::to_vec(&json!({"ok": true, "ability": ability, "mp": new_mp, "hp": p["hp"]}))?)
}
inventory::submit! { NativeReducerItem { name: "sim_ability", make: || Box::new(NativeReducerBackend::new(sim_ability)) } }

// ── Game: apply damage ──────────────────────────────────────────────────────
fn sim_damage(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let pid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let amount = a.get(1).and_then(|v| v.as_i64()).unwrap_or(10);
    let mut p = match ctx.get_row("sim_players", pid)? {
        Some(v) => v, None => return Ok(rmp_serde::to_vec(&json!({"ok": true, "skipped": true}))?),
    };
    if !p["alive"].as_bool().unwrap_or(true) { return Ok(rmp_serde::to_vec(&json!({"ok": true, "skipped": true}))?); }
    let hp = (p["hp"].as_i64().unwrap_or(100) - amount).max(0);
    p["hp"] = json!(hp); p["alive"] = json!(hp > 0);
    ctx.set_row("sim_players".into(), pid.into(), p)?;
    Ok(rmp_serde::to_vec(&json!({"ok": true, "hp": hp, "alive": hp > 0}))?)
}
inventory::submit! { NativeReducerItem { name: "sim_damage", make: || Box::new(NativeReducerBackend::new(sim_damage)) } }

// ── Game: respawn ───────────────────────────────────────────────────────────
fn sim_respawn(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let pid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let mut p = ctx.get_row("sim_players", pid)?.unwrap_or(json!({}));
    p["hp"] = json!(p["max_hp"].as_i64().unwrap_or(100));
    p["mp"] = json!(p["max_mp"].as_i64().unwrap_or(100));
    p["alive"] = json!(true); p["x"] = json!(0); p["y"] = json!(0);
    ctx.set_row("sim_players".into(), pid.into(), p)?;
    Ok(ok_bytes())
}
inventory::submit! { NativeReducerItem { name: "sim_respawn", make: || Box::new(NativeReducerBackend::new(sim_respawn)) } }

// ── Game: spawn NPC ─────────────────────────────────────────────────────────
fn sim_spawn_npc(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let nid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let x = a.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
    let y = a.get(2).and_then(|v| v.as_f64()).unwrap_or(0.0);
    let kind = a.get(3).and_then(|v| v.as_str()).unwrap_or("goblin");
    ctx.set_row("sim_npcs".into(), nid.into(), json!({
        "nid": nid, "x": x, "y": y, "kind": kind,
        "hp": 50, "max_hp": 50, "alive": true, "patrol_x": x, "patrol_y": y
    }))?;
    Ok(ok_bytes())
}
inventory::submit! { NativeReducerItem { name: "sim_spawn_npc", make: || Box::new(NativeReducerBackend::new(sim_spawn_npc)) } }

// ── Game: world tick (scheduled) ────────────────────────────────────────────
fn sim_world_tick(ctx: &mut ReducerContext, _args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let players = ctx.tables.list_rows("sim_players").unwrap_or_default();
    for p in &players {
        if p["alive"].as_bool().unwrap_or(false) {
            let mp = p["mp"].as_i64().unwrap_or(100);
            let max_mp = p["max_mp"].as_i64().unwrap_or(100);
            if mp < max_mp {
                let pid = p["pid"].as_str().or(p["row_key"].as_str()).unwrap_or("");
                let mut pc = p.clone();
                pc["mp"] = json!((mp + 5).min(max_mp));
                ctx.set_row("sim_players".into(), pid.into(), pc)?;
            }
        }
    }
    let npcs = ctx.tables.list_rows("sim_npcs").unwrap_or_default();
    let mut respawned = 0i64;
    for n in &npcs {
        if !n["alive"].as_bool().unwrap_or(true) {
            let nid = n["nid"].as_str().or(n["row_key"].as_str()).unwrap_or("");
            let mut nc = n.clone();
            nc["hp"] = json!(nc["max_hp"].as_i64().unwrap_or(50));
            nc["alive"] = json!(true);
            nc["x"] = nc["patrol_x"].clone(); nc["y"] = nc["patrol_y"].clone();
            ctx.set_row("sim_npcs".into(), nid.into(), nc)?;
            respawned += 1;
        }
    }
    Ok(rmp_serde::to_vec(&json!({"ok": true, "respawned": respawned, "players_regen": players.len()}))?)
}
inventory::submit! { NativeReducerItem { name: "sim_world_tick", make: || Box::new(NativeReducerBackend::new(sim_world_tick)) } }

// ── Game: buy item ──────────────────────────────────────────────────────────
fn sim_buy(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let pid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let item = a.get(1).and_then(|v| v.as_str()).unwrap_or("item");
    let qty = a.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
    let price = a.get(3).and_then(|v| v.as_i64()).unwrap_or(10);
    let mut p = match ctx.get_row("sim_players", pid)? {
        Some(v) => v, None => return Ok(rmp_serde::to_vec(&json!({"error": "no_player"}))?),
    };
    let cost = price * qty;
    let currency = p["currency"].as_i64().unwrap_or(0);
    if currency < cost { return Ok(rmp_serde::to_vec(&json!({"error": "insufficient"}))?); }
    p["currency"] = json!(currency - cost);
    ctx.set_row("sim_players".into(), pid.into(), p)?;
    let key = format!("{}:{}", pid, item);
    let mut inv = ctx.get_row("sim_inventory", &key)?.unwrap_or(json!({"pid": pid, "item": item, "qty": 0}));
    inv["qty"] = json!(inv["qty"].as_i64().unwrap_or(0) + qty);
    ctx.set_row("sim_inventory".into(), key, inv.clone())?;
    Ok(rmp_serde::to_vec(&json!({"ok": true, "currency": currency - cost, "qty": inv["qty"]}))?)
}
inventory::submit! { NativeReducerItem { name: "sim_buy", make: || Box::new(NativeReducerBackend::new(sim_buy)) } }

// ── Game: sell item ─────────────────────────────────────────────────────────
fn sim_sell(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let pid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let item = a.get(1).and_then(|v| v.as_str()).unwrap_or("item");
    let qty = a.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
    let price = a.get(3).and_then(|v| v.as_i64()).unwrap_or(8);
    let key = format!("{}:{}", pid, item);
    let inv = match ctx.get_row("sim_inventory", &key)? {
        Some(v) if v["qty"].as_i64().unwrap_or(0) >= qty => v,
        _ => return Ok(rmp_serde::to_vec(&json!({"error": "not_enough"}))?),
    };
    let new_qty = inv["qty"].as_i64().unwrap_or(0) - qty;
    if new_qty <= 0 { ctx.delete_row("sim_inventory".into(), key.clone())?; }
    else {
        let mut inv2 = inv; inv2["qty"] = json!(new_qty);
        ctx.set_row("sim_inventory".into(), key, inv2)?;
    }
    let mut p = ctx.get_row("sim_players", pid)?.unwrap_or(json!({}));
    let cur = p["currency"].as_i64().unwrap_or(0) + price * qty;
    p["currency"] = json!(cur);
    ctx.set_row("sim_players".into(), pid.into(), p)?;
    Ok(rmp_serde::to_vec(&json!({"ok": true, "currency": cur}))?)
}
inventory::submit! { NativeReducerItem { name: "sim_sell", make: || Box::new(NativeReducerBackend::new(sim_sell)) } }

// ── Game: loot box ──────────────────────────────────────────────────────────
fn sim_loot(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let pid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let rarity = a.get(2).and_then(|v| v.as_str()).unwrap_or("common");
    let items = match rarity {
        "rare" => &["shield","mana_gem","ring"][..],
        "epic" => &["sword","staff","plate"][..],
        _ => &["health_pot","arrow","leather"][..],
    };
    let item = items[pid.len() % items.len()];
    let key = format!("{}:{}", pid, item);
    let mut inv = ctx.get_row("sim_inventory", &key)?.unwrap_or(json!({"pid": pid, "item": item, "qty": 0}));
    inv["qty"] = json!(inv["qty"].as_i64().unwrap_or(0) + 1);
    ctx.set_row("sim_inventory".into(), key, inv.clone())?;
    Ok(rmp_serde::to_vec(&json!({"ok": true, "item": item, "qty": inv["qty"], "rarity": rarity}))?)
}
inventory::submit! { NativeReducerItem { name: "sim_loot", make: || Box::new(NativeReducerBackend::new(sim_loot)) } }

// ── Game: transfer currency ─────────────────────────────────────────────────
fn sim_transfer(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let from = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let to = a.get(1).and_then(|v| v.as_str()).unwrap_or("");
    let amount = a.get(2).and_then(|v| v.as_i64()).unwrap_or(10);
    let mut pf = match ctx.get_row("sim_players", from)? { Some(v) => v, None => return Ok(rmp_serde::to_vec(&json!({"error":"missing_player"}))?), };
    let mut pt = match ctx.get_row("sim_players", to)? { Some(v) => v, None => return Ok(rmp_serde::to_vec(&json!({"error":"missing_player"}))?), };
    if pf["currency"].as_i64().unwrap_or(0) < amount { return Ok(rmp_serde::to_vec(&json!({"error":"insufficient"}))?); }
    pf["currency"] = json!(pf["currency"].as_i64().unwrap_or(0) - amount);
    pt["currency"] = json!(pt["currency"].as_i64().unwrap_or(0) + amount);
    ctx.set_row("sim_players".into(), from.into(), pf)?;
    ctx.set_row("sim_players".into(), to.into(), pt)?;
    Ok(ok_bytes())
}
inventory::submit! { NativeReducerItem { name: "sim_transfer", make: || Box::new(NativeReducerBackend::new(sim_transfer)) } }

// ── Game: quest accept ──────────────────────────────────────────────────────
fn sim_quest_accept(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let pid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let qid = a.get(1).and_then(|v| v.as_str()).unwrap_or("");
    ctx.set_row("sim_quests".into(), format!("{}:{}", pid, qid), json!({"pid": pid, "qid": qid, "progress": 0, "done": false}))?;
    Ok(ok_bytes())
}
inventory::submit! { NativeReducerItem { name: "sim_quest_accept", make: || Box::new(NativeReducerBackend::new(sim_quest_accept)) } }

// ── Game: quest progress ────────────────────────────────────────────────────
fn sim_quest_progress(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let pid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let qid = a.get(1).and_then(|v| v.as_str()).unwrap_or("");
    let delta = a.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
    let key = format!("{}:{}", pid, qid);
    let mut q = match ctx.get_row("sim_quests", &key)? {
        Some(v) => v, None => return Ok(rmp_serde::to_vec(&json!({"error": "no_quest"}))?),
    };
    let progress = q["progress"].as_i64().unwrap_or(0) + delta;
    q["progress"] = json!(progress);
    let done = progress >= 10;
    if done {
        q["done"] = json!(true);
        if let Some(mut p) = ctx.get_row("sim_players", pid)? {
            let xp = p["xp"].as_i64().unwrap_or(0) + 200;
            p["xp"] = json!(xp); p["level"] = json!(xp / 1000 + 1);
            ctx.set_row("sim_players".into(), pid.into(), p)?;
        }
    }
    ctx.set_row("sim_quests".into(), key, q)?;
    Ok(rmp_serde::to_vec(&json!({"ok": true, "progress": progress, "done": done}))?)
}
inventory::submit! { NativeReducerItem { name: "sim_quest_progress", make: || Box::new(NativeReducerBackend::new(sim_quest_progress)) } }

// ── Game: matchmaking queue/dequeue ─────────────────────────────────────────
fn sim_queue(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let pid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let mode = a.get(1).and_then(|v| v.as_str()).unwrap_or("deathmatch");
    ctx.set_row("sim_queue".into(), pid.into(), json!({"pid": pid, "mode": mode, "ts": 0}))?;
    Ok(ok_bytes())
}
inventory::submit! { NativeReducerItem { name: "sim_queue", make: || Box::new(NativeReducerBackend::new(sim_queue)) } }

fn sim_dequeue(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let pid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    ctx.delete_row("sim_queue".into(), pid.to_string())?;
    Ok(ok_bytes())
}
inventory::submit! { NativeReducerItem { name: "sim_dequeue", make: || Box::new(NativeReducerBackend::new(sim_dequeue)) } }

// ── Game: leaderboard ───────────────────────────────────────────────────────
fn sim_score(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let pid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let score = a.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
    let cur = ctx.get_row("sim_leaderboard", pid)?.unwrap_or(json!({"pid": pid, "score": 0}));
    let best = cur["score"].as_i64().unwrap_or(0);
    if score > best {
        ctx.set_row("sim_leaderboard".into(), pid.into(), json!({"pid": pid, "score": score}))?;
        Ok(rmp_serde::to_vec(&json!({"ok": true, "new_best": true, "score": score}))?)
    } else {
        Ok(rmp_serde::to_vec(&json!({"ok": true, "new_best": false, "score": best}))?)
    }
}
inventory::submit! { NativeReducerItem { name: "sim_score", make: || Box::new(NativeReducerBackend::new(sim_score)) } }

// ── Chat: create room ───────────────────────────────────────────────────────
fn sim_create_room(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let rid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let name = a.get(1).and_then(|v| v.as_str()).unwrap_or("");
    let uid = a.get(2).and_then(|v| v.as_str()).unwrap_or("");
    // Pure upsert — skip existence check to avoid read contention on room rows.
    ctx.set_row("sim_rooms".into(), rid.into(), json!({"rid": rid, "name": name, "creator": uid}))?;
    Ok(ok_bytes())
}
inventory::submit! { NativeReducerItem { name: "sim_create_room", make: || Box::new(NativeReducerBackend::new(sim_create_room)) } }

// ── Chat: join room ─────────────────────────────────────────────────────────
fn sim_join_room(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let rid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let uid = a.get(1).and_then(|v| v.as_str()).unwrap_or("");
    // Pure write: track membership in a separate row per (uid, rid) pair.
    // Avoids contention on a shared room members-array row.
    let member_key = format!("{}:{}", rid, uid);
    ctx.set_row("sim_members".into(), member_key, json!({"rid": rid, "uid": uid, "joined": true}))?;
    Ok(ok_bytes())
}
inventory::submit! { NativeReducerItem { name: "sim_join_room", make: || Box::new(NativeReducerBackend::new(sim_join_room)) } }

// ── Chat: send message (ring buffer) ────────────────────────────────────────
fn sim_send_msg(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let rid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let uid = a.get(2).and_then(|v| v.as_str()).unwrap_or("");
    let text = a.get(3).and_then(|v| v.as_str()).unwrap_or("");
    // Time-based ring buffer slot — no read-modify-write, no room contention.
    // Slot rotates every ~5ms across 200 slots = ~1s history per room.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
    let slot = (now_ms / 5) % 200;
    let mid = format!("{}:{}", rid, slot);
    ctx.set_row("sim_messages".into(), mid.clone(), json!({"mid": &mid, "rid": rid, "uid": uid, "text": text, "slot": slot}))?;
    Ok(rmp_serde::to_vec(&json!({"ok": true, "mid": mid}))?)
}
inventory::submit! { NativeReducerItem { name: "sim_send_msg", make: || Box::new(NativeReducerBackend::new(sim_send_msg)) } }

// ── Chat: react ─────────────────────────────────────────────────────────────
fn sim_react(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let mid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let uid = a.get(1).and_then(|v| v.as_str()).unwrap_or("");
    let emoji = a.get(2).and_then(|v| v.as_str()).unwrap_or("👍");
    let key = format!("{}:{}:{}", mid, uid, emoji);
    ctx.set_row("sim_reactions".into(), key, json!({"mid": mid, "uid": uid, "emoji": emoji}))?;
    Ok(ok_bytes())
}
inventory::submit! { NativeReducerItem { name: "sim_react", make: || Box::new(NativeReducerBackend::new(sim_react)) } }

// ── Chat: typing indicator ──────────────────────────────────────────────────
fn sim_typing(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let uid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let rid = a.get(1).and_then(|v| v.as_str()).unwrap_or("");
    let typing = a.get(2).and_then(|v| v.as_bool()).unwrap_or(true);
    let key = format!("{}:{}", uid, rid);
    if typing { ctx.set_row("sim_typing".into(), key, json!({"uid": uid, "rid": rid, "ts": 0}))?; }
    else { ctx.delete_row("sim_typing".into(), key)?; }
    Ok(ok_bytes())
}
inventory::submit! { NativeReducerItem { name: "sim_typing", make: || Box::new(NativeReducerBackend::new(sim_typing)) } }

// ── Chat: presence ──────────────────────────────────────────────────────────
fn sim_presence(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let uid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let status = a.get(1).and_then(|v| v.as_str()).unwrap_or("online");
    ctx.set_row("sim_presence".into(), uid.into(), json!({"uid": uid, "status": status, "ts": 0}))?;
    Ok(ok_bytes())
}
inventory::submit! { NativeReducerItem { name: "sim_presence", make: || Box::new(NativeReducerBackend::new(sim_presence)) } }

// ── Chat: thread reply ──────────────────────────────────────────────────────
fn sim_thread_reply(ctx: &mut ReducerContext, args: &[u8]) -> neondb::error::Result<Vec<u8>> {
    let a = args_to_vec(args);
    let tid = a.first().and_then(|v| v.as_str()).unwrap_or("");
    let uid = a.get(2).and_then(|v| v.as_str()).unwrap_or("");
    let text = a.get(3).and_then(|v| v.as_str()).unwrap_or("");
    ctx.set_row("sim_thread_replies".into(), tid.into(), json!({"tid": tid, "uid": uid, "text": text}))?;
    Ok(ok_bytes())
}

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

    /// Connect to an external `neondb-sim serve` server instead of starting
    /// an embedded one. Server stats are sampled over HTTP (--metrics-url).
    #[arg(long)]
    external: bool,

    /// Bot id offset — lets multiple client processes simulate distinct
    /// players against one server (e.g. 0 / 5000 / 10000).
    #[arg(long, default_value = "0")]
    id_offset: usize,

    /// Per-action think-time in ms for game bots (0 = fire as fast as possible).
    /// ~200 ≈ 5 actions/sec, a realistic human rate; keeps the client side light
    /// enough to sustain tens of thousands of live connections.
    #[arg(long, default_value = "0")]
    think_ms: u64,

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
        /// Players per game instance (lobby). Bots only interact within
        /// their own lobby — mirrors how real games shard players.
        #[arg(long, default_value = "75")]
        lobby_size: usize,
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
    /// Run the benchmark server only (clients connect with --external).
    /// Separating server and clients into different processes removes the
    /// shared-runtime scheduling bottleneck that caps in-process benchmarks.
    Serve {
        /// WebSocket port
        #[arg(long, default_value = "3777")]
        ws_port: u16,
        /// Health/stats HTTP port (GET /healthz)
        #[arg(long, default_value = "3778")]
        metrics_port: u16,
    },
    /// Maximum-throughput stress test using native reducers + pipelining
    ///
    /// Target: 250K-400K TPS using pipelined native writes, unsafe WAL, disabled rate limiter.
    /// Run:  neondb-sim stress
    ///       neondb-sim stress --clients 20 --pipeline 512 --reducer stress_ping
    Stress {
        /// Concurrent clients — sweet spot is 20-50 for inline reducers
        #[arg(short, long, default_value = "20")]
        clients: usize,
        /// Duration in seconds
        #[arg(short, long, default_value = "30")]
        duration: u64,
        /// Ramp-up seconds (not counted in metrics)
        #[arg(long, default_value = "3")]
        ramp: u64,
        /// Requests in-flight per client (pipeline depth) — 512 is optimal on most hardware
        #[arg(long, default_value = "512")]
        pipeline: usize,
        /// Reducer to call: "stress_ping" (inline, no write) or "stress_write" (full write path)
        #[arg(long, default_value = "stress_ping")]
        reducer: String,
        /// Extra workers beyond num_cpus (0 = use num_cpus × 2)
        #[arg(long, default_value = "0")]
        extra_workers: usize,
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
    /// Per-lobby (game instance) latency histograms — ≤ lobby_size writers each.
    lobby_hists:   dashmap::DashMap<usize, parking_lot::Mutex<Histogram<u64>>>,
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
            lobby_hists:  dashmap::DashMap::new(),
        }
    }
    /// Record a call's latency against its lobby (game instance).
    fn record_lobby(&self, lobby: usize, us: u64) {
        if !self.measuring.load(Ordering::Relaxed) { return; }
        let h = self.lobby_hists.entry(lobby).or_insert_with(|| {
            parking_lot::Mutex::new(Histogram::<u64>::new(3).expect("hdr histogram"))
        });
        let _ = h.lock().record(us.max(1));
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
    wal_mb:       f64,
    rows:         u64,
    connections:  u64,
    elapsed_secs: u64,
}

fn get_memory_bytes() -> u64 {
    #[cfg(target_os = "windows")]
    {
        use std::mem;
        type HANDLE  = *mut std::ffi::c_void;
        type DWORD   = u32;
        type SIZE_T  = usize;
        #[repr(C)]
        struct PROCESS_MEMORY_COUNTERS {
            cb: DWORD, page_fault_count: DWORD,
            peak_working_set_size: SIZE_T, working_set_size: SIZE_T,
            quota_peak_paged_pool_usage: SIZE_T, quota_paged_pool_usage: SIZE_T,
            quota_peak_non_paged_pool_usage: SIZE_T, quota_non_paged_pool_usage: SIZE_T,
            pagefile_usage: SIZE_T, peak_pagefile_usage: SIZE_T,
        }
        #[link(name = "kernel32")] extern "system" { fn GetCurrentProcess() -> HANDLE; }
        #[link(name = "psapi")] extern "system" {
            fn GetProcessMemoryInfo(p: HANDLE, m: *mut PROCESS_MEMORY_COUNTERS, cb: DWORD) -> i32;
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
            if let Some(pages) = data.split_whitespace().nth(1).and_then(|s| s.parse::<u64>().ok()) {
                return pages * 4096;
            }
        }
        0
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    { 0 }
}

fn sample_health(handle: &ServerHandle, elapsed: u64) -> Health {
    use std::sync::atomic::Ordering;
    Health {
        memory_mb:   get_memory_bytes() as f64 / 1e6,
        wal_mb:      handle.wal_file_size.load(Ordering::Relaxed) as f64 / 1e6,
        rows:        handle.tables.total_row_count() as u64,
        connections: handle.subs.active_connections() as u64,
        elapsed_secs: elapsed,
    }
}

/// Where benchmark stats come from: the in-process server handle, or an
/// external `neondb-sim serve` process sampled over HTTP.
enum StatsSource {
    Local(ServerHandle),
    Remote(String), // metrics base URL, e.g. http://127.0.0.1:3778
}

impl StatsSource {
    async fn sample(&self, elapsed: u64) -> Health {
        match self {
            StatsSource::Local(h) => sample_health(h, elapsed),
            StatsSource::Remote(base) => fetch_health(base, elapsed).await,
        }
    }
}

/// Sample an external server's /healthz (zeros on any failure — the
/// benchmark itself must never die because a stats poll did).
async fn fetch_health(base: &str, elapsed: u64) -> Health {
    let url = format!("{}/healthz", base.trim_end_matches('/'));
    // Hard 2s budget: a saturated server must never wedge the sample loop —
    // a missed stats sample beats a benchmark that hangs forever.
    let parsed: Option<serde_json::Value> = tokio::time::timeout(Duration::from_secs(2), async {
        let resp = reqwest::get(&url).await.ok()?;
        resp.json().await.ok()
    })
    .await
    .ok()
    .flatten();
    match parsed {
        Some(v) => Health {
            memory_mb: v["memory_usage_bytes"].as_u64().unwrap_or(0) as f64 / 1e6,
            wal_mb: v["wal_file_size_bytes"].as_u64().unwrap_or(0) as f64 / 1e6,
            rows: v["total_rows"].as_u64().unwrap_or(0),
            connections: v["active_connections"].as_u64().unwrap_or(0),
            elapsed_secs: elapsed,
        },
        None => Health {
            memory_mb: 0.0,
            wal_mb: 0.0,
            rows: 0,
            connections: 0,
            elapsed_secs: elapsed,
        },
    }
}

/// Minimal /healthz HTTP responder for `neondb-sim serve` mode.
fn spawn_health_server(handle: ServerHandle, port: u16) {
    use std::sync::atomic::Ordering;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("[serve] health port {port} unavailable: {e}");
                return;
            }
        };
        loop {
            let Ok((mut sock, _)) = listener.accept().await else { continue };
            let body = serde_json::json!({
                "status": "ok",
                "memory_usage_bytes": get_memory_bytes(),
                "wal_file_size_bytes": handle.wal_file_size.load(Ordering::Relaxed),
                "total_rows": handle.tables.total_row_count(),
                "active_connections": handle.subs.active_connections(),
            })
            .to_string();
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let _ = sock.read(&mut buf).await; // drain the request line
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
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
// Uses a per-room ring-buffer: key = "<rid>:<idx % MAX_MSGS>" so the table is
// permanently bounded to MAX_MSGS × num_rooms rows regardless of throughput.
("sim_send_msg", r#"function reducer(args) {
  const [rid, _mid, uid, text] = args;
  const MAX_MSGS = 200;
  const r = __neondb_get("sim_rooms", rid);
  if (!r) return { ok: false, error: "room not found" };
  const idx = (r.next_idx || 0);
  const slot = idx % MAX_MSGS;
  const mid = rid + ":" + slot;
  __neondb_set("sim_messages", mid, { mid, rid, uid, text, idx, edited: false });
  r.next_idx = idx + 1;
  r.msg_count = Math.min((r.msg_count||0) + 1, MAX_MSGS);
  __neondb_set("sim_rooms", rid, r);
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
// ── Chat: thread replies (keyed by tid so they overwrite, not accumulate) ────
("sim_thread_reply", r#"function reducer(args) {
  const [tid, _rid, uid, text] = args;
  __neondb_set("sim_thread_replies", tid, { tid, uid, text });
  return { ok: true };
}"#),
];

// ─── Server startup ───────────────────────────────────────────────────────────

async fn start_embedded_server(ws_port: u16, metrics_port: u16) -> ServerHandle {
    use std::fs;
    let dir = std::env::temp_dir().join(format!("neondb_sim_{}_{}", ws_port, std::process::id()));
    let modules_dir = dir.join("modules");
    fs::create_dir_all(&modules_dir).unwrap();

    for (name, code) in SIM_REDUCERS {
        fs::write(modules_dir.join(format!("{}.js", name)), code).unwrap();
    }

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
    config.port              = ws_port;
    config.metrics_port      = metrics_port;
    config.wal_path          = dir.join("wal");
    config.snapshot_dir      = dir.join("snaps");
    config.max_connections    = 50_000;   // allow scale test to reach real limits
    config.rate_limit_capacity = 0;       // disable per-client rate limiter for benchmarking
    config.redis_port         = 0;        // sim benchmarks exercise the WS path only
    config.pg_port            = 0;
    config.workers            = num_cpus::get() * 4;  // 4× CPU workers to saturate native reducers
    config.unsafe_no_fsync    = true;     // benchmark mode: skip fsync, measure compute ceiling
    config.wal_batch_size     = 4096;     // larger WAL batches reduce sync overhead
    // LRU cap: pure OOM safety net. Must sit far above legitimate row counts —
    // at 30K players sim_inventory alone holds ~270K rows, and a cap below the
    // working set causes eviction thrash on every insert (measured: game TPS
    // collapsed 21K → 2.2K when 7K players crossed the old 50K cap).
    config.eviction.policy             = "lru_row_cap".to_string();
    config.eviction.max_rows_per_table = 2_000_000;

    match neondb::run_server_with_handle(config).await {
        Ok((handle, server_fut)) => {
            tokio::spawn(async move {
                if let Err(e) = server_fut.await {
                    eprintln!("[sim-server] error: {e}");
                }
            });
            handle
        }
        Err(e) => {
            eprintln!("[sim-server] startup failed: {e}");
            std::process::exit(1);
        }
    }
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

    /// Subscribe to a live query. Counts snapshot/diff frames into
    /// SUB_FRAMES; returns once the server acks (or false on failure).
    async fn subscribe(&mut self, query: &str) -> bool {
        let msg = ClientMessage::Subscribe {
            subscription_id: format!("sub{}", self.user_id),
            query: query.to_string(),
        };
        let Ok(frame) = rmp_serde::to_vec(&msg) else { return false };
        if self.inner.send(Message::Binary(frame)).await.is_err() {
            return false;
        }
        loop {
            match tokio::time::timeout(Duration::from_secs(5), self.inner.next()).await {
                Ok(Some(Ok(Message::Binary(b)))) => {
                    match rmp_serde::from_slice::<neondb::network::message::ServerMessage>(&b) {
                        Ok(neondb::network::message::ServerMessage::SubscriptionAck { success, .. }) => {
                            return success;
                        }
                        Ok(
                            neondb::network::message::ServerMessage::SubscriptionDiff(_)
                            | neondb::network::message::ServerMessage::SubscriptionRoute(_)
                            | neondb::network::message::ServerMessage::SubscriptionBody(_),
                        ) => {
                            SUB_FRAMES.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {}
                    }
                }
                Ok(Some(Ok(_))) => continue,
                _ => return false,
            }
        }
    }

    /// Send a reducer call and wait for its ReducerResponse.
    /// Subscription fan-out frames arriving in between are counted, not
    /// mistaken for the response. Returns (ok, latency_us).
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
                Ok(Some(Ok(Message::Binary(b)))) => {
                    use neondb::network::message::ServerMessage as SM;
                    match rmp_serde::from_slice::<SM>(&b) {
                        Ok(SM::ReducerResponse(r)) => {
                            return (r.success, t0.elapsed().as_micros() as u64);
                        }
                        Ok(SM::SubscriptionDiff(_) | SM::SubscriptionRoute(_) | SM::SubscriptionBody(_)) => {
                            SUB_FRAMES.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        Ok(_) => continue,
                        // Unknown frame — keep the old lenient behavior.
                        Err(_) => return (true, t0.elapsed().as_micros() as u64),
                    }
                }
                Ok(Some(Ok(Message::Text(_)))) => continue,
                Ok(Some(Ok(_))) => continue, // ping/pong/close
                _ => return (false, t0.elapsed().as_micros() as u64),
            }
        }
    }
}

/// Live subscription frames received by all bots (fan-out deliveries).
static SUB_FRAMES: AtomicU64 = AtomicU64::new(0);

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
    lobby_size: usize,
) {
    let mut rng  = Rng::new(id as u64 ^ 0xdeadbeef);
    let lobby    = id / lobby_size.max(1);
    let pid      = format!("l{lobby}_p{id}");
    let mut ws   = loop {
        if let Some(c) = WsConn::connect(&url, api_key.as_deref(), id as u64).await { break c; }
        if Instant::now() >= deadline { return; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let mut st   = GameState { hp: 100, currency: 500, alive: false, ..Default::default() };

    // Subscribe to this lobby's live player state — every lobby-mate's write
    // fans out to us, exactly like a real game client.
    let lobby_query = format!("sim_players WHERE lobby = 'l{lobby}'");
    let _ = ws.subscribe(&lobby_query).await;

    // Pre-spawn a batch of NPCs for this player to fight
    for i in 0..5 {
        let nid = format!("l{lobby}_npc_{id}_{i}");
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
            metrics.record("sim_respawn", ok, us); metrics.record_lobby(lobby, us);
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
                metrics.record("sim_move", ok, us); metrics.record_lobby(lobby, us);
            }
            // 14% — attack an NPC (read attacker + read NPC + 2 writes)
            32..=45 => {
                let nid = rng.pick(&st.npc_pool).clone();
                let dmg = rng.range(10, 30);
                let weapon = rng.pick(&["sword","axe","bow","spell"]);
                let (ok, us) = ws.call("sim_attack",
                    pack(&(pid.as_str(), nid.as_str(), *weapon, dmg))).await;
                metrics.record("sim_attack", ok, us); metrics.record_lobby(lobby, us);
                if ok { st.kills += 1; }
            }
            // 10% — use ability (read + write player state)
            46..=55 => {
                let ab = rng.pick(&["fireball","heal","shield","lightning","dash"]);
                let target = rng.pick(&st.npc_pool).clone();
                let (ok, us) = ws.call("sim_ability",
                    pack(&(pid.as_str(), *ab, target.as_str()))).await;
                metrics.record("sim_ability", ok, us); metrics.record_lobby(lobby, us);
            }
            // 8% — take damage from NPC counter-attack
            56..=63 => {
                let dmg = rng.range(5, 20);
                let (ok, us) = ws.call("sim_damage",
                    pack(&(pid.as_str(), dmg, "npc"))).await;
                metrics.record("sim_damage", ok, us); metrics.record_lobby(lobby, us);
                if ok { st.hp -= dmg; if st.hp <= 0 { st.alive = false; } }
            }
            // 7% — buy item (economy: read player, read inventory, 2 writes)
            64..=70 => {
                let item = rng.pick(&["health_pot","mana_gem","arrow","leather","iron"]);
                let qty  = rng.range(1, 3);
                let (ok, us) = ws.call("sim_buy",
                    pack(&(pid.as_str(), *item, qty, 10))).await;
                metrics.record("sim_buy", ok, us); metrics.record_lobby(lobby, us);
                if ok { st.currency -= 10 * qty; }
            }
            // 6% — sell item
            71..=76 => {
                let item = rng.pick(&["health_pot","arrow","leather"]);
                let (ok, us) = ws.call("sim_sell",
                    pack(&(pid.as_str(), *item, 1, 8))).await;
                metrics.record("sim_sell", ok, us); metrics.record_lobby(lobby, us);
            }
            // 5% — open loot box (random item drop)
            77..=81 => {
                let box_id = format!("box_{id}_{}", rng.uid());
                let rarity = rng.pick(&["common","common","common","rare","epic"]);
                let (ok, us) = ws.call("sim_loot",
                    pack(&(pid.as_str(), box_id.as_str(), *rarity))).await;
                metrics.record("sim_loot", ok, us); metrics.record_lobby(lobby, us);
            }
            // 5% — quest progress / accept
            82..=86 => {
                if !st.quest_active {
                    st.quest_id = format!("q_{id}_{}", rng.uid() % 10);
                    st.quest_prog = 0;
                    let (ok, us) = ws.call("sim_quest_accept",
                        pack(&(pid.as_str(), st.quest_id.as_str()))).await;
                    metrics.record("sim_quest_accept", ok, us); metrics.record_lobby(lobby, us);
                    if ok { st.quest_active = true; }
                } else {
                    let (ok, us) = ws.call("sim_quest_progress",
                        pack(&(pid.as_str(), st.quest_id.as_str(), 1))).await;
                    metrics.record("sim_quest_progress", ok, us); metrics.record_lobby(lobby, us);
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
                    metrics.record("sim_queue", ok, us); metrics.record_lobby(lobby, us);
                    if ok { st.queued = true; }
                } else {
                    let (ok, us) = ws.call("sim_dequeue", pack(&(pid.as_str(),))).await;
                    metrics.record("sim_dequeue", ok, us); metrics.record_lobby(lobby, us);
                    if ok { st.queued = false; }
                }
            }
            // 3% — submit score to leaderboard
            91..=93 => {
                let score = (st.kills as i64) * 100 + rng.range(0, 50) as i64;
                let (ok, us) = ws.call("sim_score",
                    pack(&(pid.as_str(), score))).await;
                metrics.record("sim_score", ok, us); metrics.record_lobby(lobby, us);
            }
            // 3% — transfer currency to peer
            94..=96 => {
                if !st.peer_ids.is_empty() && st.currency > 20 {
                    let target = rng.pick(&st.peer_ids).clone();
                    let amt = rng.range(5, 15);
                    let (ok, us) = ws.call("sim_transfer",
                        pack(&(pid.as_str(), target.as_str(), amt))).await;
                    metrics.record("sim_transfer", ok, us); metrics.record_lobby(lobby, us);
                    if ok { st.currency -= amt; }
                } else {
                    // Build peer list from this lobby only — instances are isolated.
                    let base = lobby * lobby_size.max(1);
                    for j in 0..4 {
                        let peer_id = base + (id - base + j + 1) % lobby_size.max(1);
                        let peer = format!("l{lobby}_p{peer_id}");
                        if peer != pid && !st.peer_ids.contains(&peer) {
                            st.peer_ids.push(peer);
                        }
                    }
                }
            }
            // 3% — spawn a fresh NPC (world management)
            97..=99 => {
                let nid = format!("l{lobby}_npc_{id}_{}", rng.uid() % 20);
                let x = rng.range(0, 500); let y = rng.range(0, 500);
                let kind = rng.pick(&["goblin","orc","skeleton","spider"]);
                let (ok, us) = ws.call("sim_spawn_npc",
                    pack(&(nid.as_str(), x, y, *kind))).await;
                metrics.record("sim_spawn_npc", ok, us);
                if ok && !st.npc_pool.contains(&nid) { st.npc_pool.push(nid); }
            }
            _ => {}
        }
        // Think-time: real players don't fire back-to-back. With think-time the
        // client side stays light enough to sustain tens of thousands of live
        // connections — and the load profile matches actual humans (~5/s).
        let think = THINK_MS.load(Ordering::Relaxed);
        if think > 0 {
            // Jitter ±50% so all bots don't fire on the same beat.
            let jitter = rng.range(think as i32 / 2, think as i32 + think as i32 / 2).max(1) as u64;
            tokio::time::sleep(Duration::from_millis(jitter)).await;
        }
    }
    let _ = ws.inner.close(None).await;
}

/// Per-action think-time (ms) for game bots. 0 = fire as fast as possible
/// (old behavior). ~200ms ≈ 5 actions/sec, a realistic human rate.
static THINK_MS: AtomicU64 = AtomicU64::new(0);

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

// ─── Stress scenario ─────────────────────────────────────────────────────────

/// Start a lean embedded server tuned for maximum raw throughput:
/// - unsafe_no_fsync (no fsync on every batch — data not crash-safe, fine for benchmarks)
/// - 2× workers to saturate all CPU threads
/// - 65536 queue depth to avoid back-pressure at high client counts
/// - Rate limiter disabled (capacity=0)
/// - No scheduler (world tick not needed here)
async fn start_stress_server(ws_port: u16, metrics_port: u16, extra_workers: usize) -> ServerHandle {
    let dir = std::env::temp_dir().join(format!(
        "neondb_stress_{}_{}", ws_port, std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    // No scheduler, no module files — native reducers already registered.
    let mut config = neondb::config::Config::from_env();
    config.port               = ws_port;
    config.metrics_port       = metrics_port;
    config.wal_path           = dir.join("wal");
    config.snapshot_dir       = dir.join("snaps");
    config.unsafe_no_fsync     = true;
    config.reducer_queue_cap   = 65536;
    config.rate_limit_capacity = 0;           // disable per-client rate limiter
    config.max_connections     = 10_000;      // allow many concurrent stress clients
    config.scheduled_reducers  = Vec::new();  // no world tick
    config.redis_port          = 0;           // stress test exercises the WS path only
    config.pg_port             = 0;
    // workers: num_cpus is set inside server.rs; we use NEONDB_WORKERS env to override
    if extra_workers > 0 {
        // server.rs reads NEONDB_WORKERS if present
        std::env::set_var("NEONDB_WORKERS", (num_cpus::get() + extra_workers).to_string());
    } else {
        // Default: 2× num_cpus so both physical + logical cores are used
        std::env::set_var("NEONDB_WORKERS", (num_cpus::get() * 2).to_string());
    }

    match neondb::run_server_with_handle(config).await {
        Ok((handle, server_fut)) => {
            tokio::spawn(async move {
                if let Err(e) = server_fut.await { eprintln!("[stress-server] error: {e}"); }
            });
            handle
        }
        Err(e) => { eprintln!("[stress-server] startup failed: {e}"); std::process::exit(1); }
    }
}

/// One pipelined stress client.
///
/// Unlike the serial `WsConn::call()`, this client maintains `pipeline`
/// in-flight requests at all times.  Responses are ordered (TCP), so we
/// track timestamps in a `VecDeque` and pop from the front for each reply.
async fn run_stress_client(
    url:          String,
    api_key:      Option<String>,
    user_id:      u64,
    pipeline:     usize,
    reducer:      String,
    done:         Arc<AtomicBool>,
    total_calls:  Arc<AtomicU64>,
    total_errors: Arc<AtomicU64>,
    hist:         Arc<Mutex<Histogram<u64>>>,
) {
    let Some(conn) = WsConn::connect(&url, api_key.as_deref(), user_id).await else { return; };
    let (mut sink, mut stream) = conn.inner.split();

    // Pre-encode args once per client (key = "k<shard>" so 128 distinct keys spread load)
    let key = format!("k{}", user_id % 128);
    let args_bytes: Vec<u8> = if reducer == "stress_ping" {
        Vec::new()
    } else {
        rmp_serde::to_vec(&(key.as_str(),)).unwrap_or_default()
    };

    let mut seq: u64 = 0;
    let mut in_flight: VecDeque<Instant> = VecDeque::with_capacity(pipeline + 1);

    // Helper: encode and send one call frame
    macro_rules! send_next {
        () => {{
            seq = seq.wrapping_add(1);
            let call_id = (user_id << 24) | (seq & 0xFF_FFFF);
            let msg = ClientMessage::ReducerCall(ReducerCall {
                call_id,
                reducer_name: reducer.clone(),
                args: args_bytes.clone(),
            });
            match rmp_serde::to_vec(&msg) {
                Ok(frame) => {
                    let t0 = Instant::now();
                    if sink.send(Message::Binary(frame)).await.is_ok() {
                        in_flight.push_back(t0);
                        true
                    } else { false }
                }
                Err(_) => false,
            }
        }};
    }

    // Fill the pipeline
    for _ in 0..pipeline {
        if !send_next!() { return; }
    }

    // Steady state: one recv → one send
    loop {
        let timeout_ms = if done.load(Ordering::Relaxed) { 300 } else { 5000 };
        match tokio::time::timeout(Duration::from_millis(timeout_ms), stream.next()).await {
            Ok(Some(Ok(Message::Binary(_) | Message::Text(_)))) => {
                if let Some(t0) = in_flight.pop_front() {
                    let us = t0.elapsed().as_micros() as u64;
                    total_calls.fetch_add(1, Ordering::Relaxed);
                    if let Ok(mut h) = hist.lock() { let _ = h.record(us.max(1)); }
                }
                if !done.load(Ordering::Relaxed) && !send_next!() { return; }
            }
            Ok(Some(Ok(_))) => {} // ping / pong / close frame — skip
            _ => {
                // timeout or stream end: drain counts as errors if still in-flight
                total_errors.fetch_add(in_flight.len() as u64, Ordering::Relaxed);
                return;
            }
        }
        if done.load(Ordering::Relaxed) && in_flight.is_empty() { return; }
    }
}

async fn run_stress(
    clients:      usize,
    duration:     u64,
    ramp_secs:    u64,
    pipeline:     usize,
    reducer:      String,
    url:          String,
    api_key:      Option<String>,
    server:       &ServerHandle,
) {
    println!("  reducer={reducer}  clients={clients}  pipeline={pipeline}  duration={duration}s  ramp={ramp_secs}s");
    println!("  unsafe_no_fsync=true  rate_limiter=off  queue_cap=65536\n");

    let done         = Arc::new(AtomicBool::new(false));
    let total_calls  = Arc::new(AtomicU64::new(0));
    let total_errors = Arc::new(AtomicU64::new(0));
    let hist         = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap()
    ));

    let measuring = Arc::new(AtomicBool::new(false));

    // Spawn all clients
    let mut handles = Vec::with_capacity(clients);
    for i in 0..clients {
        let (url2, ak, done2, tc, te, h, r) = (
            url.clone(), api_key.clone(), done.clone(),
            total_calls.clone(), total_errors.clone(), hist.clone(), reducer.clone(),
        );
        handles.push(tokio::spawn(run_stress_client(
            url2, ak, i as u64, pipeline, r, done2, tc, te, h,
        )));
        // stagger spawns slightly to avoid thundering-herd on connect
        if i % 50 == 49 { tokio::time::sleep(Duration::from_millis(10)).await; }
    }

    let deadline       = Instant::now() + Duration::from_secs(ramp_secs + duration);
    let measure_start  = Instant::now() + Duration::from_secs(ramp_secs);
    let sim_start      = Instant::now();
    let mut last_calls = 0u64;
    let mut last_t     = Instant::now();
    let mut samples    = Vec::<Health>::new();

    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let now = Instant::now();

        if now >= measure_start && !measuring.load(Ordering::SeqCst) {
            measuring.store(true, Ordering::SeqCst);
            // Reset counters at measuring start
            total_calls.store(0, Ordering::SeqCst);
            total_errors.store(0, Ordering::SeqCst);
            if let Ok(mut h) = hist.lock() { *h = Histogram::new_with_bounds(1, 60_000_000, 3).unwrap(); }
            last_calls = 0;
            last_t = Instant::now();
            println!("  ▶  Ramp-up complete — measuring");
        }

        if measuring.load(Ordering::Relaxed) {
            let calls = total_calls.load(Ordering::Relaxed);
            let window_tps = (calls - last_calls) as f64 / last_t.elapsed().as_secs_f64();
            last_calls = calls; last_t = Instant::now();
            let h = sample_health(server, sim_start.elapsed().as_secs());
            println!("[{:>4}s] tps={:>9.0}  calls={:>9}  err={:>5}  mem={:.1}MB  rows={:>6}  conn={}",
                sim_start.elapsed().as_secs(), window_tps,
                calls, total_errors.load(Ordering::Relaxed),
                h.memory_mb, h.rows, h.connections);
            samples.push(h);
        }

        if now >= deadline { break; }
    }

    done.store(true, Ordering::SeqCst);
    for h in handles { let _ = h.await; }

    // Final report
    let elapsed      = duration as f64;
    let total        = total_calls.load(Ordering::Relaxed);
    let errors       = total_errors.load(Ordering::Relaxed);
    let avg_tps      = total as f64 / elapsed;
    let err_pct      = if total + errors > 0 { 100.0 * errors as f64 / (total + errors) as f64 } else { 0.0 };
    let (p50, p99, p999) = if let Ok(h) = hist.lock() {
        if h.len() > 0 {
            (h.value_at_quantile(0.50) as f64 / 1000.0,
             h.value_at_quantile(0.99) as f64 / 1000.0,
             h.value_at_quantile(0.999) as f64 / 1000.0)
        } else { (0.0, 0.0, 0.0) }
    } else { (0.0, 0.0, 0.0) };

    let mem_first = samples.first().map(|s| s.memory_mb).unwrap_or(0.0);
    let mem_last  = samples.last().map(|s| s.memory_mb).unwrap_or(0.0);
    let mem_growth = if mem_first > 0.0 { 100.0 * (mem_last - mem_first) / mem_first } else { 0.0 };

    println!("\n╔══════════════════════════════════════════════════════════════════════╗");
    println!("║  NeonDB STRESS REPORT — {reducer:<47}");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║  Duration: {duration}s   Clients: {clients}   Pipeline: {pipeline}   Total calls: {total}");
    println!("║  Avg TPS:  {avg_tps:>10.0}   Error rate: {err_pct:.3}%");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║  p50:  {p50:>8.3}ms");
    println!("║  p99:  {p99:>8.3}ms");
    println!("║  p999: {p999:>8.3}ms");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║  Memory: {mem_first:.1}MB → {mem_last:.1}MB ({mem_growth:+.1}%)   Rows: {}",
        samples.last().map(|s| s.rows).unwrap_or(0));
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    let target_250k = if avg_tps >= 250_000.0 { "✅ ≥ 250K TPS" } else { "❌ < 250K TPS" };
    let target_400k = if avg_tps >= 400_000.0 { "✅ ≥ 400K TPS" } else { "❌ < 400K TPS" };
    println!("║  Targets:  {target_250k}   {target_400k}");
    println!("╚══════════════════════════════════════════════════════════════════════╝\n");
}

// ─── Simulation runner ────────────────────────────────────────────────────────

#[derive(Clone)]
struct SimConfig {
    url:         String,
    metrics_url: String,
    api_key:     Option<String>,
    max_err_pct: f64,
    id_offset:   usize,
}

async fn run_game_sim(
    n_players: usize,
    duration:  u64,
    ramp_secs: u64,
    cfg:       &SimConfig,
    metrics:   Arc<Metrics>,
    stats:     &StatsSource,
    lobby_size: usize,
) -> Vec<Health> {
    let deadline = Instant::now() + Duration::from_secs(ramp_secs + duration);
    let measuring_start = Instant::now() + Duration::from_secs(ramp_secs);

    let mut handles = Vec::new();
    for i in 0..n_players {
        let (url, ak, met) = (cfg.url.clone(), cfg.api_key.clone(), metrics.clone());
        handles.push(tokio::spawn(game_user(i + cfg.id_offset, url, ak, met, deadline, lobby_size)));
        if i % 20 == 19 { tokio::time::sleep(Duration::from_millis(30)).await; }
    }
    sample_loop(measuring_start, deadline, &metrics, stats, &mut handles).await
}

async fn run_chat_sim(
    n_users:   usize,
    duration:  u64,
    ramp_secs: u64,
    rooms:     usize,
    cfg:       &SimConfig,
    metrics:   Arc<Metrics>,
    stats:     &StatsSource,
) -> Vec<Health> {
    let deadline = Instant::now() + Duration::from_secs(ramp_secs + duration);
    let measuring_start = Instant::now() + Duration::from_secs(ramp_secs);

    let mut handles = Vec::new();
    for i in 0..n_users {
        let (url, ak, met, r) = (cfg.url.clone(), cfg.api_key.clone(), metrics.clone(), rooms);
        handles.push(tokio::spawn(chat_user(i + cfg.id_offset, url, ak, met, deadline, r)));
        if i % 20 == 19 { tokio::time::sleep(Duration::from_millis(30)).await; }
    }
    sample_loop(measuring_start, deadline, &metrics, stats, &mut handles).await
}

async fn sample_loop(
    measuring_start: Instant,
    deadline:        Instant,
    metrics:         &Arc<Metrics>,
    stats:           &StatsSource,
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

        let h = stats.sample(elapsed).await;
        println!("[{:>5}s] tps={:>8.0}  ok={:>9}  err={:>5}  mem={:>5.1}MB  \
                  wal={:>5.1}MB  rows={:>6}  conn={}",
            elapsed, window_tps, total, errors,
            h.memory_mb, h.wal_mb, h.rows, h.connections);
        samples.push(h);

        if now >= deadline { break; }
    }

    for h in handles { let _ = h.await; }
    samples
}

// ─── Report ───────────────────────────────────────────────────────────────────

fn print_lobby_summary(metrics: &Metrics) {
    if metrics.lobby_hists.is_empty() { return; }
    // (lobby, p50, p99) per instance
    let mut per: Vec<(usize, f64, f64)> = metrics.lobby_hists.iter().map(|e| {
        let h = e.value().lock();
        (*e.key(), h.value_at_quantile(0.50) as f64 / 1000.0, h.value_at_quantile(0.99) as f64 / 1000.0)
    }).collect();
    per.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
    let n = per.len();
    let med_p50 = per[n / 2].1;
    let med_p99 = per[n / 2].2;
    let (best, worst) = (&per[0], &per[n - 1]);
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║  Per-instance latency ({n} lobbies)");
    println!("║    median lobby:  p50 {med_p50:>8.2}ms   p99 {med_p99:>8.2}ms");
    println!("║    best lobby  :  p50 {:>8.2}ms   p99 {:>8.2}ms   (l{})", best.1, best.2, best.0);
    println!("║    worst lobby :  p50 {:>8.2}ms   p99 {:>8.2}ms   (l{})", worst.1, worst.2, worst.0);
}

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

    print_lobby_summary(metrics);

    let sub_frames = SUB_FRAMES.load(Ordering::Relaxed);
    if sub_frames > 0 {
        println!("╠══════════════════════════════════════════════════════════════════════╣");
        println!("║  Subscription fan-out received: {} frames ({:.0}/s)",
            sub_frames, sub_frames as f64 / elapsed_secs.max(0.001));
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
    stats:     &StatsSource,
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
            "chat" => run_chat_sim(n, dur, 3, (n/5).max(1), cfg, metrics.clone(), stats).await,
            _      => run_game_sim(n, dur, 3, cfg, metrics.clone(), stats, 75).await,
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
    THINK_MS.store(args.think_ms, Ordering::Relaxed);

    // Derive ports from URL (default: 3777/3778 to avoid collisions)
    let ws_port: u16      = 3777;
    let metrics_port: u16 = 3778;

    // Stress scenario gets its own lean server config
    if let ScenarioCmd::Stress { clients, duration, ramp, pipeline, reducer, extra_workers } = &args.scenario {
        println!("┌─ NeonDB STRESS BENCHMARK ──────────────────────────────────────────┐");
        println!("│  Starting stress server on :{ws_port} (native reducers, no fsync) ...");
        let server = start_stress_server(ws_port, metrics_port, *extra_workers).await;
        wait_for_server(ws_port).await;
        println!("│  Server ready (native reducers: stress_ping, stress_write, increment).");
        println!("└────────────────────────────────────────────────────────────────────┘\n");
        println!("▶  STRESS scenario");
        run_stress(
            *clients, *duration, *ramp, *pipeline,
            reducer.clone(), args.url.clone(), args.api_key.clone(), &server,
        ).await;
        return;
    }

    // Serve mode: run the benchmark server and park (clients use --external).
    if let ScenarioCmd::Serve { ws_port, metrics_port } = &args.scenario {
        println!("┌─ NeonDB Benchmark Server ─────────────────────────────────────────┐");
        println!("│  WebSocket :{ws_port}   health http://127.0.0.1:{metrics_port}/healthz");
        let server = start_embedded_server(*ws_port, *metrics_port).await;
        wait_for_server(*ws_port).await;
        spawn_health_server(server, *metrics_port);
        println!("│  Ready. Run clients with:  neondb-sim --external game --players N");
        println!("└────────────────────────────────────────────────────────────────────┘");
        futures::future::pending::<()>().await;
        return;
    }

    // External mode: clients only — server stats sampled over HTTP.
    let stats = if args.external {
        println!("┌─ NeonDB Simulation Benchmark (external server) ───────────────────┐");
        println!("│  Connecting to {}   stats from {}", args.url, args.metrics_url);
        println!("└────────────────────────────────────────────────────────────────────┘\n");
        StatsSource::Remote(args.metrics_url.clone())
    } else {
        println!("┌─ NeonDB Simulation Benchmark ─────────────────────────────────────┐");
        println!("│  Starting embedded NeonDB server on :{ws_port} ...");
        let server = start_embedded_server(ws_port, metrics_port).await;
        wait_for_server(ws_port).await;
        println!("│  Server ready. {} reducers loaded.", SIM_REDUCERS.len());
        println!("└────────────────────────────────────────────────────────────────────┘\n");
        StatsSource::Local(server)
    };

    let cfg = SimConfig {
        url:         args.url.clone(),
        metrics_url: args.metrics_url.clone(),
        api_key:     args.api_key.clone(),
        max_err_pct: args.max_error_pct,
        id_offset:   args.id_offset,
    };

    let all_ops: Vec<&'static str> = SIM_REDUCERS.iter().map(|(n, _)| *n).collect();

    match &args.scenario {
        ScenarioCmd::Game { players, duration, ramp, lobby_size } => {
            let ls = (*lobby_size).max(1);
            let lobbies = (*players + ls - 1) / ls;
            println!("▶  GAME simulation — {players} players in {lobbies} lobbies of {ls} for {duration}s (ramp {ramp}s)");
            println!("   Workload: positions, combat, abilities, economy, quests, matchmaking, world\n");
            let metrics  = Arc::new(Metrics::new(&all_ops));
            let t0       = Instant::now();
            let samples  = run_game_sim(*players, *duration, *ramp, &cfg, metrics.clone(), &stats, ls).await;
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
            let samples = run_chat_sim(*users, *duration, *ramp, *rooms, &cfg, metrics.clone(), &stats).await;
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
                handles.push(tokio::spawn(game_user(i + cfg.id_offset, url, ak, met, deadline_game, 75)));
                if i % 20 == 19 { tokio::time::sleep(Duration::from_millis(20)).await; }
            }
            let rooms = (*users / 5).max(1);
            for i in 0..*users {
                let (url, ak, met) = (cfg.url.clone(), cfg.api_key.clone(), metrics.clone());
                handles.push(tokio::spawn(chat_user(i + cfg.id_offset, url, ak, met, deadline_chat, rooms)));
                if i % 20 == 19 { tokio::time::sleep(Duration::from_millis(20)).await; }
            }
            let samples = sample_loop(
                Instant::now() + Duration::from_secs(*ramp),
                deadline_game, &metrics, &stats, &mut handles,
            ).await;
            let elapsed = t0.elapsed().as_secs_f64() - *ramp as f64;
            let label   = format!("MIXED  {players}×game + {users}×chat  {duration}s");
            print_report(&label, &metrics, &samples, elapsed);
        }
        ScenarioCmd::Scale { profile, levels, duration_per_level, stop_error_pct } => {
            let parsed: Vec<usize> = levels.split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
            run_scale(profile, parsed, *duration_per_level, *stop_error_pct, &cfg, &stats).await;
        }
        // Handled above with early returns
        ScenarioCmd::Stress { .. } | ScenarioCmd::Serve { .. } => unreachable!(),
    }
}

fn write_csv(path: &str, samples: &[Health]) {
    let header = "elapsed_secs,memory_mb,wal_mb,rows,connections";
    let rows: Vec<String> = samples.iter().map(|h| {
        format!("{},{:.2},{:.2},{},{}", h.elapsed_secs, h.memory_mb, h.wal_mb, h.rows, h.connections)
    }).collect();
    let content = format!("{}\n{}", header, rows.join("\n"));
    if let Err(e) = std::fs::write(path, content) {
        eprintln!("CSV write error: {e}");
    } else {
        println!("Time-series written to {path}");
    }
}
