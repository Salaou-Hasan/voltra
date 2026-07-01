//! Process-global registry that makes [`LobbyRuntime`] reachable from
//! `#[reducer]` functions and from the WebSocket connection lifecycle.
//!
//! ## Why this exists
//!
//! `#[reducer]` functions only ever receive `&mut ReducerContext` — that's
//! the whole point of the macro (see `voltra-macros/src/reducer.rs`): every
//! reducer, regardless of which project generated it, has an identical
//! signature so `inventory::submit!` can auto-register it. A `LobbyRuntime`
//! (owning a live ECS `World` + `AreaOfInterest` + tick scheduler) cannot be
//! threaded through that signature without changing it for every reducer in
//! every project ever generated — including all the durable-gameplay ones
//! that have nothing to do with hot simulation.
//!
//! So hot-simulation reducers reach their `LobbyRuntime` the same way the JS
//! and WASM backends reach their engine state: through a process-global
//! registry (see `src/reducer/v8.rs`'s `QJS_CTXS` thread-local for the
//! precedent). This keeps `ReducerContext` untouched and keeps durable
//! reducers (inventory, economy, guilds, chat, quests) exactly as they are
//! today — plain `ctx.get/set` over `TableStore`.
//!
//! ## Lifecycle
//!
//! - `LOBBY_RUNTIMES` maps `lobby_id -> Arc<LobbyRuntime>`, created lazily on
//!   first use (`get_or_create_lobby`).
//! - `start_tick_driver` spawns one Tokio task per lobby that calls
//!   `LobbyRuntime::run_tick()` on an interval derived from the lobby's own
//!   `TickConfig` (so `tick_rate_hz` in the recipe genuinely drives the
//!   simulation, not a TableStore scheduler entry).
//! - `AOI_PENDING` is a short-lived handoff table: the WebSocket layer
//!   registers a connecting client's `(ClientId, Sender<OutboundFrames>)`
//!   under its `caller_id` the moment the connection is accepted; a
//!   hot-sim `join_lobby` reducer (running later, on the reducer worker
//!   thread, with only `ctx.caller_id` to go on) looks itself up by that same
//!   `caller_id`, spawns the ECS entity, and completes
//!   `LobbyRuntime::register_aoi_client`. This is the bridge between
//!   "a real WebSocket connection exists" and "a reducer decided which
//!   entity that connection is."

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::mpsc::Sender;

use super::aoi_broadcast::{ClientId, OutboundFrames};
use super::ecs::EntityId;
use super::lobby_runtime::LobbyRuntime;
use super::tick::TickConfig;

/// A pending AOI-client registration, deposited by the WebSocket layer at
/// connect time and consumed by a hot-sim reducer (e.g. `join_lobby`) once it
/// knows which lobby + entity the connection belongs to.
struct PendingAoiClient {
    client_id: ClientId,
    tx: Sender<OutboundFrames>,
}

/// Global registry of live lobby runtimes, keyed by lobby id (the same
/// numeric string used in `l{lobby}_...` TableStore keys — see
/// `table::parse_lobby_key` — so hot-sim and durable state can agree on
/// "which lobby" without a second ID scheme).
pub struct LobbyRuntimeRegistry {
    lobbies: DashMap<String, Arc<LobbyRuntimeCell>>,
    pending_aoi: DashMap<String, PendingAoiClient>,
    default_tick: TickConfig,
    view_distance: f32,
}

/// A lobby runtime plus a lock so `run_tick` (which needs `&mut self`) can be
/// driven by a dedicated background task while reducers call the read/write
/// methods that only need `&self` (spawn_player, set_velocity, etc. all take
/// `&self` on `LobbyRuntime` already — only `run_tick`/`tick_if_due` need
/// `&mut`, so the mutex is only ever held briefly once per tick).
pub struct LobbyRuntimeCell {
    inner: parking_lot::Mutex<LobbyRuntime>,
}

impl LobbyRuntimeCell {
    /// Run one tick. Short critical section — reducers calling `with_runtime`
    /// concurrently will block for at most one tick's worth of system work.
    pub fn run_tick(&self) {
        let mut guard = self.inner.lock();
        let result = guard.run_tick();
        log::trace!(
            "[lobby-runtime] lobby={} tick={} entities={} aoi_updates={} elapsed_us={}",
            guard.lobby_id,
            result.tick_number,
            result.entities_processed,
            result.aoi_updates,
            result.elapsed_ns / 1000,
        );
    }

    /// Run a closure against the live `LobbyRuntime` (spawn, move, despawn,
    /// set_velocity, register_aoi_client, ...). Held only for the closure's
    /// duration — no `.await` allowed inside `f`.
    pub fn with_runtime<R>(&self, f: impl FnOnce(&LobbyRuntime) -> R) -> R {
        let guard = self.inner.lock();
        f(&guard)
    }
}

impl LobbyRuntimeRegistry {
    fn new(default_tick: TickConfig, view_distance: f32) -> Self {
        Self {
            lobbies: DashMap::new(),
            pending_aoi: DashMap::new(),
            default_tick,
            view_distance,
        }
    }

    /// Fetch the lobby runtime for `lobby_id`, creating it (with the
    /// registry's default `TickConfig` + view distance) if this is the first
    /// time this lobby has been touched.
    pub fn get_or_create(&self, lobby_id: &str) -> Arc<LobbyRuntimeCell> {
        if let Some(existing) = self.lobbies.get(lobby_id) {
            return existing.clone();
        }
        let created = Arc::new(LobbyRuntimeCell {
            inner: parking_lot::Mutex::new(LobbyRuntime::new(
                lobby_id.to_string(),
                self.default_tick.clone(),
                self.view_distance,
            )),
        });
        self.lobbies
            .entry(lobby_id.to_string())
            .or_insert(created)
            .clone()
    }

    /// Look up an existing lobby runtime without creating one.
    pub fn get(&self, lobby_id: &str) -> Option<Arc<LobbyRuntimeCell>> {
        self.lobbies.get(lobby_id).map(|e| e.clone())
    }

    /// Number of live lobby runtimes (for diagnostics/tests).
    pub fn lobby_count(&self) -> usize {
        self.lobbies.len()
    }

    /// Deposit a connecting client's outbound channel so a reducer running
    /// later (keyed by the same `caller_id`) can complete AOI registration.
    /// Called from the WebSocket layer right after `register_client`.
    pub fn stage_aoi_client(&self, caller_id: &str, client_id: ClientId, tx: Sender<OutboundFrames>) {
        self.pending_aoi
            .insert(caller_id.to_string(), PendingAoiClient { client_id, tx });
    }

    /// Remove a staged (not yet consumed) registration — called on disconnect
    /// so a client that never called the joining reducer doesn't leak an
    /// entry forever.
    pub fn clear_staged_aoi_client(&self, caller_id: &str) {
        self.pending_aoi.remove(caller_id);
    }

    /// Consume the staged registration for `caller_id` (if any) and bind it
    /// to `player_entity` inside `lobby_id`'s AOI broadcaster. Returns
    /// `true` if a pending registration existed and was bound.
    ///
    /// Called from a hot-sim reducer (e.g. `join_lobby`) after it has spawned
    /// the ECS entity for this connection.
    pub fn bind_aoi_client(&self, lobby_id: &str, caller_id: &str, player_entity: EntityId) -> bool {
        let Some((_, pending)) = self.pending_aoi.remove(caller_id) else {
            return false;
        };
        let cell = self.get_or_create(lobby_id);
        cell.with_runtime(|rt| rt.register_aoi_client(pending.client_id, pending.tx, player_entity));
        true
    }

    /// Unregister a client from whichever lobby it joined, on disconnect.
    /// `caller_id` -> lobby mapping isn't tracked separately; callers that
    /// know the lobby (e.g. because the reducer told them) should prefer
    /// `LobbyRuntimeCell::with_runtime(|rt| rt.unregister_aoi_client(id))`
    /// directly. This helper covers the common case of "unregister from every
    /// lobby this process knows about" for simplicity at disconnect time.
    pub fn unregister_everywhere(&self, client_id: ClientId) {
        for entry in self.lobbies.iter() {
            entry.value().with_runtime(|rt| rt.unregister_aoi_client(client_id));
        }
    }

    /// Snapshot of lobby_id -> entity_count, for diagnostics/tests/admin.
    pub fn snapshot_counts(&self) -> HashMap<String, usize> {
        self.lobbies
            .iter()
            .map(|e| (e.key().clone(), e.value().with_runtime(|rt| rt.entity_count())))
            .collect()
    }
}

/// Ergonomic helpers for `#[reducer]` bodies that drive hot-simulation state.
///
/// These are the hot-sim equivalent of `ctx.get`/`ctx.set`/`ctx.delete`: instead
/// of reading/writing a TableStore row, they call straight into the process's
/// `LobbyRuntime` for the given lobby. A generated `join_lobby` / `set_velocity`
/// / `fire_weapon` reducer calls these directly — see
/// `templates/rm_lobby_runtime_*.rs.txt` for the generated call sites.
///
/// Every function takes `lobby_id: &str` explicitly rather than trying to
/// infer it from `ctx` — hot-sim reducers are expected to take the lobby id
/// as their first argument (matching the `l{lobby}_...` convention used
/// everywhere else in Voltra), so callers already have it in hand.
pub mod reducer_api {
    use super::*;

    /// Entity handle returned to reducers as a plain `u64` (the wire-friendly
    /// form of `EntityId`) so it round-trips through MessagePack args/results
    /// without exposing the internal `(index, generation)` struct.
    pub fn entity_to_wire(id: EntityId) -> u64 {
        ((id.index() as u64) << 32) | (id.generation() as u64)
    }

    /// Inverse of [`entity_to_wire`].
    pub fn entity_from_wire(wire: u64) -> EntityId {
        EntityId::new((wire >> 32) as u32, wire as u32)
    }

    /// Spawn a player entity in `lobby_id` and bind the calling connection's
    /// staged AOI registration (see `stage_aoi_client`) to it. Returns the
    /// wire-encoded entity id, or `None` if this caller has no staged
    /// connection (e.g. called twice, or called by a non-WebSocket caller
    /// such as the scheduler).
    ///
    /// This is the function a generated `join_lobby(lobby_id, session_id, name, x, y, hp)`
    /// reducer calls.
    pub fn join_lobby(
        lobby_id: &str,
        caller_id: &str,
        session_id: u64,
        name: &str,
        x: f32,
        y: f32,
        hp: i32,
    ) -> u64 {
        let reg = global();
        let cell = reg.get_or_create(lobby_id);
        let entity = cell.with_runtime(|rt| rt.spawn_player(session_id, name, x, y, hp));
        // Bind AOI regardless of whether a WebSocket staged a registration —
        // server-to-server / test callers simply get no fan-out, which is
        // correct (there is no live connection to fan out to).
        reg.bind_aoi_client(lobby_id, caller_id, entity);
        entity_to_wire(entity)
    }

    /// Spawn an NPC entity in `lobby_id` (no AOI client binding — NPCs aren't
    /// controlled by a connection). Returns the wire-encoded entity id.
    pub fn spawn_npc(lobby_id: &str, npc_type: &str, level: i32, x: f32, y: f32, hp: i32) -> u64 {
        let cell = global().get_or_create(lobby_id);
        let entity = cell.with_runtime(|rt| rt.spawn_npc(npc_type, level, x, y, hp));
        entity_to_wire(entity)
    }

    /// Set an entity's velocity — the hot-sim reducer behind a client's "move"
    /// input. The next tick's `MovementSystem` integrates position from this.
    pub fn set_velocity(lobby_id: &str, entity_wire: u64, vx: f32, vy: f32, vz: f32) -> bool {
        let Some(cell) = global().get(lobby_id) else {
            return false;
        };
        let entity = entity_from_wire(entity_wire);
        cell.with_runtime(|rt| {
            if !rt.world().read().is_alive(entity) {
                return false;
            }
            rt.set_velocity(entity, vx, vy, vz);
            true
        })
    }

    /// Directly teleport an entity (bypasses velocity integration — used for
    /// spawn placement, respawn, or authoritative corrections).
    pub fn move_entity(lobby_id: &str, entity_wire: u64, x: f32, y: f32) -> bool {
        let Some(cell) = global().get(lobby_id) else {
            return false;
        };
        let entity = entity_from_wire(entity_wire);
        cell.with_runtime(|rt| {
            if !rt.world().read().is_alive(entity) {
                return false;
            }
            // `move_entity` takes `&mut self` on LobbyRuntime — but we only
            // have `&LobbyRuntime` here via `with_runtime`. Reach through the
            // interior-mutable world/aoi locks directly (same operation
            // `LobbyRuntime::move_entity` performs, just without requiring
            // exclusive access to the whole runtime).
            {
                let mut world = rt.world().write();
                if let Some(t) = world.transforms.get_mut(entity) {
                    t.x = x;
                    t.y = y;
                }
            }
            rt.aoi().write().move_entity(entity, x, y);
            true
        })
    }

    /// Despawn an entity from both ECS storage and the AOI grid.
    pub fn despawn(lobby_id: &str, entity_wire: u64) -> bool {
        let Some(cell) = global().get(lobby_id) else {
            return false;
        };
        let entity = entity_from_wire(entity_wire);
        cell.with_runtime(|rt| {
            if !rt.world().read().is_alive(entity) {
                return false;
            }
            rt.despawn(entity);
            true
        })
    }

    /// Apply damage to an entity's `Health` component. Returns
    /// `(new_hp, alive)` or `None` if the entity has no health component.
    pub fn apply_damage(lobby_id: &str, entity_wire: u64, amount: i32) -> Option<(i32, bool)> {
        let cell = global().get(lobby_id)?;
        let entity = entity_from_wire(entity_wire);
        cell.with_runtime(|rt| {
            let mut world = rt.world().write();
            let health = world.healths.get_mut(entity)?;
            health.current = (health.current - amount).max(0);
            health.alive = health.current > 0;
            Some((health.current, health.alive))
        })
    }

    /// Read back an entity's current transform (x, y, z), if it exists.
    pub fn get_transform(lobby_id: &str, entity_wire: u64) -> Option<(f32, f32, f32)> {
        let cell = global().get(lobby_id)?;
        let entity = entity_from_wire(entity_wire);
        cell.with_runtime(|rt| {
            let world = rt.world().read();
            world.transforms.get(entity).map(|t| (t.x, t.y, t.z))
        })
    }

    /// Read back an entity's current health (current, max, alive), if it exists.
    pub fn get_health(lobby_id: &str, entity_wire: u64) -> Option<(i32, i32, bool)> {
        let cell = global().get(lobby_id)?;
        let entity = entity_from_wire(entity_wire);
        cell.with_runtime(|rt| {
            let world = rt.world().read();
            world
                .healths
                .get(entity)
                .map(|h| (h.current, h.max, h.alive))
        })
    }

    /// Current tick number for a lobby (0 if the lobby doesn't exist yet).
    pub fn tick_number(lobby_id: &str) -> u64 {
        global()
            .get(lobby_id)
            .map(|cell| cell.with_runtime(|rt| rt.tick_number()))
            .unwrap_or(0)
    }

    /// Live entity count for a lobby (0 if the lobby doesn't exist yet).
    pub fn entity_count(lobby_id: &str) -> usize {
        global()
            .get(lobby_id)
            .map(|cell| cell.with_runtime(|rt| rt.entity_count()))
            .unwrap_or(0)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn entity_wire_roundtrip() {
            let id = EntityId::new(7, 3);
            let wire = entity_to_wire(id);
            let back = entity_from_wire(wire);
            assert_eq!(id, back);
        }

        #[test]
        fn join_lobby_spawns_and_binds_aoi() {
            let (tx, _rx) = tokio::sync::mpsc::channel::<OutboundFrames>(8);
            global().stage_aoi_client("caller-x", 5, tx);

            let wire = join_lobby("99", "caller-x", 1, "alice", 1.0, 2.0, 100);
            let cell = global().get("99").unwrap();
            assert_eq!(cell.with_runtime(|rt| rt.aoi_client_count()), 1);
            assert_eq!(get_transform("99", wire), Some((1.0, 2.0, 0.0)));
        }

        #[test]
        fn set_velocity_then_tick_moves_entity() {
            let wire = join_lobby("100", "caller-y", 2, "bob", 0.0, 0.0, 100);
            assert!(set_velocity("100", wire, 3.0, 4.0, 0.0));

            global().get("100").unwrap().run_tick();

            assert_eq!(get_transform("100", wire), Some((3.0, 4.0, 0.0)));
        }

        #[test]
        fn apply_damage_reduces_hp_and_reports_alive() {
            let wire = spawn_npc("101", "goblin", 1, 0.0, 0.0, 50);
            let (hp, alive) = apply_damage("101", wire, 20).unwrap();
            assert_eq!(hp, 30);
            assert!(alive);

            let (hp2, alive2) = apply_damage("101", wire, 100).unwrap();
            assert_eq!(hp2, 0);
            assert!(!alive2);
        }

        #[test]
        fn despawn_removes_entity() {
            let wire = spawn_npc("102", "rat", 1, 0.0, 0.0, 10);
            assert_eq!(entity_count("102"), 1);
            assert!(despawn("102", wire));
            assert_eq!(entity_count("102"), 0);
            // Second despawn on the same (now-dead) handle fails cleanly.
            assert!(!despawn("102", wire));
        }

        #[test]
        fn unknown_lobby_operations_fail_soft() {
            assert_eq!(get_transform("no-such-lobby", 0), None);
            assert_eq!(get_health("no-such-lobby", 0), None);
            assert!(!set_velocity("no-such-lobby", 0, 1.0, 1.0, 0.0));
            assert!(!move_entity("no-such-lobby", 0, 1.0, 1.0));
            assert!(!despawn("no-such-lobby", 0));
            assert_eq!(tick_number("no-such-lobby"), 0);
            assert_eq!(entity_count("no-such-lobby"), 0);
        }
    }
}

static REGISTRY: OnceLock<Arc<LobbyRuntimeRegistry>> = OnceLock::new();

/// Initialize the global lobby-runtime registry. Safe to call multiple times
/// (only the first call takes effect) — a scaffolded project's `main.rs`
/// calls this once at startup before `voltra::run_server(...)`.
pub fn init(default_tick: TickConfig, view_distance: f32) -> Arc<LobbyRuntimeRegistry> {
    REGISTRY
        .get_or_init(|| Arc::new(LobbyRuntimeRegistry::new(default_tick, view_distance)))
        .clone()
}

/// Fetch the global registry, initializing it with defaults
/// (`TickConfig::default()` = 20Hz, view distance 100.0) if `init` was never
/// called explicitly. This means hot-sim reducers work even in a project that
/// forgot to call `init` — they just get the default tick rate.
pub fn global() -> Arc<LobbyRuntimeRegistry> {
    REGISTRY
        .get_or_init(|| Arc::new(LobbyRuntimeRegistry::new(TickConfig::default(), 100.0)))
        .clone()
}

/// Returns `true` if the registry has been explicitly initialized (as opposed
/// to lazily defaulted by `global()`). Used by the tick driver to decide
/// whether to log a hint about calling `init` explicitly.
pub fn is_initialized() -> bool {
    REGISTRY.get().is_some()
}

/// Spawn one Tokio task per currently-known lobby's tick loop, plus a
/// "watch for new lobbies" task that starts a tick loop for any lobby created
/// after this call (e.g. by `get_or_create` inside a `join_lobby` reducer).
///
/// `tick_hz` overrides the interval used to *drive* ticking; each lobby's own
/// `LobbyRuntime` was already constructed with its own `TickConfig`, so this
/// is purely how often we ask it "are you due for a tick" — matching it to
/// the lobby's configured rate avoids wasted wakeups (calling `run_tick`
/// faster than `tick_rate_hz` just re-ticks early; `LobbyRuntime::run_tick`
/// itself does not throttle, so the driver is what enforces cadence).
///
/// Returns a `watch::Sender<()>` the caller can use to stop the driver
/// (dropping it or sending stops all spawned tasks on the next loop check).
pub fn start_tick_driver(
    registry: Arc<LobbyRuntimeRegistry>,
    mut shutdown: tokio::sync::watch::Receiver<()>,
) {
    let tick_rate_hz = registry.default_tick.tick_rate_hz;
    let tick_interval = Duration::from_secs_f64(1.0 / tick_rate_hz.max(1) as f64);
    let driven: Arc<DashMap<String, ()>> = Arc::new(DashMap::new());

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tick_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = shutdown.changed() => {
                    log::info!("[lobby-runtime] Tick driver shutting down");
                    break;
                }
            }
            // Tick every lobby that exists right now. New lobbies created
            // between wakeups are picked up automatically on the next tick
            // (no separate "new lobby" task needed — this loop already scans
            // the live DashMap every interval).
            for entry in registry.lobbies.iter() {
                entry.value().run_tick();
                driven.insert(entry.key().clone(), ());
            }
        }
    });

    log::info!("[lobby-runtime] Tick driver started @ {:.1}Hz", tick_rate_hz);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_registry() -> LobbyRuntimeRegistry {
        LobbyRuntimeRegistry::new(TickConfig::default(), 100.0)
    }

    #[test]
    fn get_or_create_is_idempotent() {
        let reg = fresh_registry();
        let a = reg.get_or_create("0");
        let b = reg.get_or_create("0");
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(reg.lobby_count(), 1);
    }

    #[test]
    fn different_lobbies_are_isolated() {
        let reg = fresh_registry();
        let a = reg.get_or_create("0");
        let b = reg.get_or_create("1");
        assert!(!Arc::ptr_eq(&a, &b));

        a.with_runtime(|rt| {
            rt.spawn_player(1, "alice", 0.0, 0.0, 100);
        });
        assert_eq!(a.with_runtime(|rt| rt.entity_count()), 1);
        assert_eq!(b.with_runtime(|rt| rt.entity_count()), 0);
    }

    #[test]
    fn stage_and_bind_aoi_client() {
        let reg = fresh_registry();
        let (tx, _rx) = tokio::sync::mpsc::channel::<OutboundFrames>(8);
        reg.stage_aoi_client("caller-1", 42, tx);

        let cell = reg.get_or_create("0");
        let entity = cell.with_runtime(|rt| rt.spawn_player(1, "alice", 0.0, 0.0, 100));

        let bound = reg.bind_aoi_client("0", "caller-1", entity);
        assert!(bound);
        assert_eq!(cell.with_runtime(|rt| rt.aoi_client_count()), 1);

        // Second bind for the same caller_id fails — already consumed.
        assert!(!reg.bind_aoi_client("0", "caller-1", entity));
    }

    #[test]
    fn clear_staged_aoi_client_prevents_late_bind() {
        let reg = fresh_registry();
        let (tx, _rx) = tokio::sync::mpsc::channel::<OutboundFrames>(8);
        reg.stage_aoi_client("caller-2", 7, tx);
        reg.clear_staged_aoi_client("caller-2");

        let cell = reg.get_or_create("0");
        let entity = cell.with_runtime(|rt| rt.spawn_player(2, "bob", 0.0, 0.0, 100));
        assert!(!reg.bind_aoi_client("0", "caller-2", entity));
    }

    #[test]
    fn unregister_everywhere_removes_client_from_all_lobbies() {
        let reg = fresh_registry();
        let (tx, _rx) = tokio::sync::mpsc::channel::<OutboundFrames>(8);

        let cell0 = reg.get_or_create("0");
        let e0 = cell0.with_runtime(|rt| rt.spawn_player(1, "alice", 0.0, 0.0, 100));
        cell0.with_runtime(|rt| rt.register_aoi_client(99, tx.clone(), e0));
        assert_eq!(cell0.with_runtime(|rt| rt.aoi_client_count()), 1);

        reg.unregister_everywhere(99);
        assert_eq!(cell0.with_runtime(|rt| rt.aoi_client_count()), 0);
    }

    #[test]
    fn snapshot_counts_reports_all_lobbies() {
        let reg = fresh_registry();
        reg.get_or_create("0")
            .with_runtime(|rt| rt.spawn_player(1, "alice", 0.0, 0.0, 100));
        reg.get_or_create("1")
            .with_runtime(|rt| rt.spawn_npc("goblin", 1, 0.0, 0.0, 10));
        reg.get_or_create("1")
            .with_runtime(|rt| rt.spawn_npc("goblin", 1, 0.0, 0.0, 10));

        let counts = reg.snapshot_counts();
        assert_eq!(counts.get("0"), Some(&1));
        assert_eq!(counts.get("1"), Some(&2));
    }

    #[tokio::test]
    async fn tick_driver_advances_lobby_tick_number() {
        let reg = Arc::new(LobbyRuntimeRegistry::new(
            TickConfig {
                tick_rate_hz: 100,
                max_tick_time_ms: 10,
            },
            100.0,
        ));
        let cell = reg.get_or_create("0");
        cell.with_runtime(|rt| {
            rt.spawn_player(1, "alice", 0.0, 0.0, 100);
        });

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        start_tick_driver(reg.clone(), shutdown_rx);

        // 100Hz => 10ms/tick. Give the driver a generous window to run several.
        tokio::time::sleep(Duration::from_millis(150)).await;

        let ticks = cell.with_runtime(|rt| rt.tick_number());
        assert!(ticks >= 3, "expected several ticks to have run, got {ticks}");
    }
}
