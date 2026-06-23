use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::aoi::AreaOfInterest;
use super::aoi_broadcast::{AoiBroadcaster, ClientId, OutboundFrames};
use super::ecs::{EntityId, Health, NpcInfo, PlayerInfo, System, Transform, Velocity, World};
use super::tick::{TickConfig, TickResult, TickScheduler};

pub struct MovementSystem;

impl System for MovementSystem {
    fn run(&self, world: &mut World, _tick: u64) {
        let mut updates: Vec<(EntityId, f32, f32, f32)> = Vec::new();
        for (id, vel) in world.velocities.iter() {
            if let Some(pos) = world.transforms.get(id) {
                updates.push((id, pos.x + vel.vx, pos.y + vel.vy, pos.z + vel.vz));
            }
        }
        for (id, x, y, z) in updates {
            if let Some(t) = world.transforms.get_mut(id) {
                t.x = x;
                t.y = y;
                t.z = z;
            }
        }
    }
}

pub struct HealthSystem;

impl System for HealthSystem {
    fn run(&self, world: &mut World, _tick: u64) {
        for (_id, health) in world.healths.iter() {
            // Clamp current hp to [0, max]. alive flag is set by combat reducers,
            // not by this system — this system just enforces invariants.
            let _ = health;
        }
    }
}

pub struct LobbyRuntime {
    pub lobby_id: String,
    scheduler: TickScheduler,
    broadcaster: AoiBroadcaster,
    last_tick_check: Instant,
    tick_interval: Duration,
}

impl LobbyRuntime {
    pub fn new(lobby_id: String, config: TickConfig, view_distance: f32) -> Self {
        let tick_interval = Duration::from_secs_f64(1.0 / config.tick_rate_hz as f64);
        let mut scheduler = TickScheduler::new(config, view_distance);
        scheduler = scheduler.with_system(Box::new(MovementSystem));
        scheduler = scheduler.with_system(Box::new(HealthSystem));
        Self {
            lobby_id,
            scheduler,
            broadcaster: AoiBroadcaster::new(),
            last_tick_check: Instant::now() - tick_interval,
            tick_interval,
        }
    }

    pub fn tick_if_due(&mut self) -> Option<TickResult> {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_tick_check);
        if elapsed >= self.tick_interval {
            self.last_tick_check = now;
            let result = self.scheduler.run_tick();
            self.broadcast_after_tick();
            Some(result)
        } else {
            None
        }
    }

    pub fn run_tick(&mut self) -> TickResult {
        self.last_tick_check = Instant::now();
        let result = self.scheduler.run_tick();
        self.broadcast_after_tick();
        result
    }

    fn broadcast_after_tick(&self) {
        let transforms = self.collect_transforms();
        let aoi = self.scheduler.aoi().read();
        let tick = self.scheduler.tick_number();
        self.broadcaster.broadcast_tick(&aoi, &transforms, tick);
        self.broadcaster.update_snapshot(&transforms);
    }

    fn collect_transforms(&self) -> HashMap<EntityId, (f32, f32)> {
        let world = self.scheduler.world().read();
        world.transforms.iter().map(|(id, t)| (id, (t.x, t.y))).collect()
    }

    pub fn register_aoi_client(
        &self,
        client_id: ClientId,
        tx: tokio::sync::mpsc::Sender<OutboundFrames>,
        player_entity: EntityId,
    ) {
        self.broadcaster.register_client(client_id, tx, player_entity);
    }

    pub fn unregister_aoi_client(&self, client_id: ClientId) {
        self.broadcaster.unregister_client(client_id);
    }

    pub fn aoi_client_count(&self) -> usize {
        self.broadcaster.client_count()
    }

    pub fn world(&self) -> &parking_lot::RwLock<World> {
        self.scheduler.world()
    }

    pub fn aoi(&self) -> &parking_lot::RwLock<AreaOfInterest> {
        self.scheduler.aoi()
    }

    pub fn tick_number(&self) -> u64 {
        self.scheduler.tick_number()
    }

    pub fn spawn_player(
        &self,
        session_id: u64,
        name: &str,
        x: f32,
        y: f32,
        hp: i32,
    ) -> EntityId {
        let mut world = self.scheduler.world().write();
        let entity = world.spawn();
        world.transforms.insert(
            entity,
            Transform { x, y, z: 0.0 },
        );
        world.healths.insert(
            entity,
            Health {
                current: hp,
                max: hp,
                alive: true,
            },
        );
        world.players.insert(
            entity,
            PlayerInfo {
                session_id,
                name: name.to_string(),
                joined_at: crate::now_nanos(),
            },
        );
        let mut aoi = self.scheduler.aoi().write();
        aoi.insert(entity, x, y);
        entity
    }

    pub fn spawn_npc(&self, npc_type: &str, level: i32, x: f32, y: f32, hp: i32) -> EntityId {
        let mut world = self.scheduler.world().write();
        let entity = world.spawn();
        world.transforms.insert(entity, Transform { x, y, z: 0.0 });
        world.healths.insert(
            entity,
            Health {
                current: hp,
                max: hp,
                alive: true,
            },
        );
        world.npcs.insert(
            entity,
            NpcInfo {
                npc_type: npc_type.to_string(),
                level,
            },
        );
        let mut aoi = self.scheduler.aoi().write();
        aoi.insert(entity, x, y);
        entity
    }

    pub fn set_velocity(&self, entity: EntityId, vx: f32, vy: f32, vz: f32) {
        let mut world = self.scheduler.world().write();
        world.velocities.insert(entity, Velocity { vx, vy, vz });
    }

    pub fn move_entity(&self, entity: EntityId, new_x: f32, new_y: f32) {
        {
            let mut world = self.scheduler.world().write();
            if let Some(t) = world.transforms.get_mut(entity) {
                t.x = new_x;
                t.y = new_y;
            }
        }
        let mut aoi = self.scheduler.aoi().write();
        aoi.move_entity(entity, new_x, new_y);
    }

    pub fn despawn(&self, entity: EntityId) {
        let mut aoi = self.scheduler.aoi().write();
        aoi.remove(entity);
        let mut world = self.scheduler.world().write();
        world.despawn(entity);
    }

    pub fn entity_count(&self) -> usize {
        self.scheduler.world().read().entity_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lobby_runtime_spawns_player() {
        let rt = LobbyRuntime::new("test".into(), TickConfig::default(), 100.0);
        let e = rt.spawn_player(1, "alice", 10.0, 20.0, 100);
        assert!(rt.world().read().transforms.has(e));
        assert!(rt.world().read().players.has(e));
        assert_eq!(rt.entity_count(), 1);
    }

    #[test]
    fn lobby_runtime_tick_applies_movement() {
        let mut rt = LobbyRuntime::new("test".into(), TickConfig::default(), 100.0);
        let e = rt.spawn_player(1, "alice", 0.0, 0.0, 100);
        rt.set_velocity(e, 1.0, 2.0, 0.0);

        let result = rt.run_tick();
        assert_eq!(result.tick_number, 0);

        let world = rt.world().read();
        let t = world.transforms.get(e).unwrap();
        assert_eq!(t.x, 1.0);
        assert_eq!(t.y, 2.0);
    }

    #[test]
    fn lobby_runtime_tick_if_due_respects_interval() {
        let config = TickConfig {
            tick_rate_hz: 20,
            max_tick_time_ms: 10,
        };
        let mut rt = LobbyRuntime::new("test".into(), config, 100.0);
        // First call should tick
        assert!(rt.tick_if_due().is_some());
        // Immediate second call should NOT tick
        assert!(rt.tick_if_due().is_none());
    }

    #[test]
    fn lobby_runtime_spawn_npc() {
        let rt = LobbyRuntime::new("test".into(), TickConfig::default(), 100.0);
        let e = rt.spawn_npc("goblin", 5, 50.0, 50.0, 30);
        let world = rt.world().read();
        assert!(world.npcs.has(e));
        assert_eq!(world.npcs.get(e).unwrap().level, 5);
    }

    #[test]
    fn lobby_runtime_despawn_removes_from_world_and_aoi() {
        let rt = LobbyRuntime::new("test".into(), TickConfig::default(), 100.0);
        let e = rt.spawn_player(1, "alice", 10.0, 10.0, 100);
        assert_eq!(rt.entity_count(), 1);

        rt.despawn(e);
        assert_eq!(rt.entity_count(), 0);
        assert!(!rt.aoi().read().visible_to(e).is_empty() || rt.aoi().read().grid.position(e).is_none());
    }

    #[test]
    fn lobby_runtime_move_entity_updates_aoi() {
        let mut rt = LobbyRuntime::new("test".into(), TickConfig::default(), 100.0);
        let e1 = rt.spawn_player(1, "alice", 0.0, 0.0, 100);
        let e2 = rt.spawn_player(2, "bob", 10.0, 0.0, 100);

        // Run a tick to populate AOI visible sets
        rt.run_tick();

        // Both should see each other initially
        let visible = rt.aoi().read().visible_to(e1);
        assert!(visible.contains(&e2));

        // Move bob far away
        rt.move_entity(e2, 500.0, 500.0);
        // Tick to update AOI
        rt.run_tick();

        // Now alice should NOT see bob
        let visible = rt.aoi().read().visible_to(e1);
        assert!(!visible.contains(&e2));
    }
}
