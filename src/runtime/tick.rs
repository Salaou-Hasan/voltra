use std::time::{Duration, Instant};

use parking_lot::RwLock;

use super::aoi::AreaOfInterest;
use super::ecs::{System, SystemExecutor, World};

#[derive(Clone, Debug)]
pub struct TickConfig {
    pub tick_rate_hz: u32,
    pub max_tick_time_ms: u64,
}

impl Default for TickConfig {
    fn default() -> Self {
        Self {
            tick_rate_hz: 20,
            max_tick_time_ms: 10,
        }
    }
}

#[derive(Debug)]
pub struct TickResult {
    pub tick_number: u64,
    pub entities_processed: usize,
    pub aoi_updates: usize,
    pub elapsed_ns: u64,
}

pub struct TickScheduler {
    world: RwLock<World>,
    executor: SystemExecutor,
    aoi: RwLock<AreaOfInterest>,
    config: TickConfig,
    tick_number: u64,
    accumulator: Duration,
    last_frame: Instant,
}

impl TickScheduler {
    pub fn new(config: TickConfig, view_distance: f32) -> Self {
        Self {
            world: RwLock::new(World::new()),
            executor: SystemExecutor::new(),
            aoi: RwLock::new(AreaOfInterest::new(100.0, view_distance)),
            config,
            tick_number: 0,
            accumulator: Duration::ZERO,
            last_frame: Instant::now(),
        }
    }

    pub fn with_system(mut self, system: Box<dyn System>) -> Self {
        self.executor.add_system(system);
        self
    }

    pub fn world(&self) -> &RwLock<World> {
        &self.world
    }

    pub fn aoi(&self) -> &RwLock<AreaOfInterest> {
        &self.aoi
    }

    pub fn tick_number(&self) -> u64 {
        self.tick_number
    }

    pub fn run_tick(&mut self) -> TickResult {
        let start = Instant::now();
        let tick = self.tick_number;

        let entities_processed;
        {
            let mut world = self.world.write();
            self.executor.run_all(&mut world, tick);
            entities_processed = world.entity_count();
        }

        let aoi_updates;
        {
            let mut aoi = self.aoi.write();
            let events = aoi.update();
            aoi_updates = events.len();
        }

        self.tick_number += 1;
        let elapsed_ns = start.elapsed().as_nanos() as u64;

        TickResult {
            tick_number: tick,
            entities_processed,
            aoi_updates,
            elapsed_ns,
        }
    }

    pub fn run_fixed_step(&mut self) -> Vec<TickResult> {
        let now = Instant::now();
        let frame_time = now.duration_since(self.last_frame);
        self.last_frame = now;
        self.accumulator += frame_time;

        let tick_duration = Duration::from_secs_f64(1.0 / self.config.tick_rate_hz as f64);
        let max_tick = Duration::from_millis(self.config.max_tick_time_ms);

        let mut results = Vec::new();
        while self.accumulator >= tick_duration {
            let tick_start = Instant::now();
            results.push(self.run_tick());
            let tick_elapsed = tick_start.elapsed();

            if tick_elapsed >= max_tick {
                self.accumulator = Duration::ZERO;
                break;
            }
            self.accumulator -= tick_duration;
        }

        results
    }
}

pub struct LobbyTickHandle {
    lobby_id: String,
    scheduler: TickScheduler,
    running: bool,
}

impl LobbyTickHandle {
    pub fn new(lobby_id: String, config: TickConfig, view_distance: f32) -> Self {
        Self {
            lobby_id,
            scheduler: TickScheduler::new(config, view_distance),
            running: true,
        }
    }

    pub fn lobby_id(&self) -> &str {
        &self.lobby_id
    }

    pub fn scheduler(&self) -> &TickScheduler {
        &self.scheduler
    }

    pub fn scheduler_mut(&mut self) -> &mut TickScheduler {
        &mut self.scheduler
    }

    pub fn is_running(&self) -> bool {
        self.running
    }

    pub fn pause(&mut self) {
        self.running = false;
    }

    pub fn resume(&mut self) {
        self.running = true;
    }

    pub fn tick(&mut self) -> Option<TickResult> {
        if self.running {
            Some(self.scheduler.run_tick())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::ecs::{EntityId, Transform, Velocity};
    use super::*;

    struct TestSystem;

    impl System for TestSystem {
        fn run(&self, world: &mut World, _tick: u64) {
            let mut updates = Vec::new();
            for (id, vel) in world.velocities.iter() {
                if let Some(t) = world.transforms.get(id) {
                    updates.push((id, t.x + vel.vx, t.y + vel.vy, t.z + vel.vz));
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

    #[test]
    fn tick_scheduler_advances_tick_number() {
        let mut scheduler = TickScheduler::new(TickConfig::default(), 100.0);
        let result = scheduler.run_tick();
        assert_eq!(result.tick_number, 0);
        assert_eq!(scheduler.tick_number(), 1);
    }

    #[test]
    fn tick_scheduler_runs_systems() {
        let mut scheduler =
            TickScheduler::new(TickConfig::default(), 100.0).with_system(Box::new(TestSystem));

        {
            let mut world = scheduler.world().write();
            let e = world.spawn();
            world.transforms.insert(
                e,
                Transform {
                    x: 0.0,
                    y: 0.0,
                    z: 0.0,
                },
            );
            world.velocities.insert(
                e,
                Velocity {
                    vx: 1.0,
                    vy: 0.0,
                    vz: 0.0,
                },
            );
        }

        scheduler.run_tick();

        let world = scheduler.world().read();
        let e = EntityId::new(0, 0);
        let t = world.transforms.get(e).unwrap();
        assert_eq!(t.x, 1.0);
    }

    #[test]
    fn tick_result_reports_entities() {
        let mut scheduler = TickScheduler::new(TickConfig::default(), 100.0);
        let result = scheduler.run_tick();
        assert_eq!(result.entities_processed, 0);
    }

    #[test]
    fn fixed_step_drains_accumulator() {
        let config = TickConfig {
            tick_rate_hz: 20,
            max_tick_time_ms: 100,
        };
        let mut scheduler = TickScheduler::new(config, 100.0);
        scheduler.last_frame = Instant::now() - Duration::from_millis(100);

        let results = scheduler.run_fixed_step();
        assert!(!results.is_empty());
        assert!(results.len() <= 3);
    }

    #[test]
    fn lobby_tick_handle_pause_resume() {
        let mut handle = LobbyTickHandle::new("test".into(), TickConfig::default(), 100.0);
        assert!(handle.is_running());

        let r1 = handle.tick();
        assert!(r1.is_some());

        handle.pause();
        assert!(!handle.is_running());
        let r2 = handle.tick();
        assert!(r2.is_none());

        handle.resume();
        assert!(handle.is_running());
        let r3 = handle.tick();
        assert!(r3.is_some());
    }

    #[test]
    fn tick_config_default() {
        let config = TickConfig::default();
        assert_eq!(config.tick_rate_hz, 20);
        assert_eq!(config.max_tick_time_ms, 10);
    }
}
