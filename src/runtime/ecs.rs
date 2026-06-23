use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct EntityId {
    index: u32,
    generation: u32,
}

impl EntityId {
    pub fn new(index: u32, generation: u32) -> Self {
        Self { index, generation }
    }

    pub fn index(self) -> u32 {
        self.index
    }

    pub fn generation(self) -> u32 {
        self.generation
    }
}

struct EntityAllocator {
    generations: Vec<u32>,
    free_list: Vec<u32>,
}

impl EntityAllocator {
    fn new() -> Self {
        Self {
            generations: Vec::new(),
            free_list: Vec::new(),
        }
    }

    fn alloc(&mut self) -> EntityId {
        if let Some(index) = self.free_list.pop() {
            let gen = self.generations[index as usize];
            EntityId::new(index, gen)
        } else {
            let index = self.generations.len() as u32;
            self.generations.push(0);
            EntityId::new(index, 0)
        }
    }

    fn free(&mut self, id: EntityId) {
        let gen = &mut self.generations[id.index() as usize];
        *gen = gen.wrapping_add(1);
        self.free_list.push(id.index());
    }

    fn is_alive(&self, id: EntityId) -> bool {
        (id.index() as usize) < self.generations.len()
            && self.generations[id.index() as usize] == id.generation()
    }
}

pub struct ComponentMap<T> {
    dense: Vec<Option<T>>,
    sparse: HashMap<EntityId, u32>,
    len: usize,
}

impl<T> ComponentMap<T> {
    pub fn new() -> Self {
        Self {
            dense: Vec::new(),
            sparse: HashMap::new(),
            len: 0,
        }
    }

    pub fn insert(&mut self, id: EntityId, component: T) {
        if let Some(&slot) = self.sparse.get(&id) {
            self.dense[slot as usize] = Some(component);
            return;
        }
        let slot = self.dense.len() as u32;
        self.dense.push(Some(component));
        self.sparse.insert(id, slot);
        self.len += 1;
    }

    pub fn get(&self, id: EntityId) -> Option<&T> {
        let &slot = self.sparse.get(&id)?;
        self.dense[slot as usize].as_ref()
    }

    pub fn get_mut(&mut self, id: EntityId) -> Option<&mut T> {
        let &slot = self.sparse.get(&id)?;
        self.dense[slot as usize].as_mut()
    }

    pub fn remove(&mut self, id: EntityId) -> Option<T> {
        let slot = self.sparse.remove(&id)?;
        let val = self.dense[slot as usize].take();
        if val.is_some() {
            self.len -= 1;
        }
        val
    }

    pub fn has(&self, id: EntityId) -> bool {
        self.sparse.contains_key(&id)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = (EntityId, &T)> {
        self.sparse.iter().filter_map(|(&id, &slot)| {
            self.dense[slot as usize].as_ref().map(|c| (id, c))
        })
    }

    pub fn clear(&mut self) {
        for slot in self.dense.iter_mut() {
            *slot = None;
        }
        self.sparse.clear();
        self.len = 0;
    }
}

#[derive(Clone, Debug)]
pub struct Transform {
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

#[derive(Clone, Debug)]
pub struct Velocity {
    pub vx: f32,
    pub vy: f32,
    pub vz: f32,
}

#[derive(Clone, Debug)]
pub struct Health {
    pub current: i32,
    pub max: i32,
    pub alive: bool,
}

#[derive(Clone, Debug)]
pub struct PlayerInfo {
    pub session_id: u64,
    pub name: String,
    pub joined_at: u64,
}

#[derive(Clone, Debug)]
pub struct NpcInfo {
    pub npc_type: String,
    pub level: i32,
}

pub struct World {
    allocator: EntityAllocator,
    pub transforms: ComponentMap<Transform>,
    pub velocities: ComponentMap<Velocity>,
    pub healths: ComponentMap<Health>,
    pub players: ComponentMap<PlayerInfo>,
    pub npcs: ComponentMap<NpcInfo>,
}

impl World {
    pub fn new() -> Self {
        Self {
            allocator: EntityAllocator::new(),
            transforms: ComponentMap::new(),
            velocities: ComponentMap::new(),
            healths: ComponentMap::new(),
            players: ComponentMap::new(),
            npcs: ComponentMap::new(),
        }
    }

    pub fn spawn(&mut self) -> EntityId {
        self.allocator.alloc()
    }

    pub fn despawn(&mut self, id: EntityId) {
        self.transforms.remove(id);
        self.velocities.remove(id);
        self.healths.remove(id);
        self.players.remove(id);
        self.npcs.remove(id);
        self.allocator.free(id);
    }

    pub fn is_alive(&self, id: EntityId) -> bool {
        self.allocator.is_alive(id)
    }

    pub fn entity_count(&self) -> usize {
        self.allocator.generations.len() - self.allocator.free_list.len()
    }

    pub fn clear(&mut self) {
        self.allocator = EntityAllocator::new();
        self.transforms.clear();
        self.velocities.clear();
        self.healths.clear();
        self.players.clear();
        self.npcs.clear();
    }
}

pub trait System {
    fn run(&self, world: &mut World, tick: u64);
}

pub struct SystemExecutor {
    systems: Vec<Box<dyn System>>,
}

impl SystemExecutor {
    pub fn new() -> Self {
        Self {
            systems: Vec::new(),
        }
    }

    pub fn add_system(&mut self, system: Box<dyn System>) {
        self.systems.push(system);
    }

    pub fn run_all(&self, world: &mut World, tick: u64) {
        for system in &self.systems {
            system.run(world, tick);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_spawn_and_despawn() {
        let mut world = World::new();
        let e1 = world.spawn();
        let e2 = world.spawn();
        assert!(world.is_alive(e1));
        assert!(world.is_alive(e2));
        assert_eq!(world.entity_count(), 2);

        world.despawn(e1);
        assert!(!world.is_alive(e1));
        assert!(world.is_alive(e2));
        assert_eq!(world.entity_count(), 1);
    }

    #[test]
    fn entity_id_reuse_after_despawn() {
        let mut world = World::new();
        let e1 = world.spawn();
        world.despawn(e1);
        let e2 = world.spawn();
        assert_eq!(e2.index(), e1.index());
        assert_ne!(e2.generation(), e1.generation());
        assert!(!world.is_alive(e1));
        assert!(world.is_alive(e2));
    }

    #[test]
    fn component_map_insert_get_remove() {
        let mut map: ComponentMap<Transform> = ComponentMap::new();
        let id = EntityId::new(0, 0);
        map.insert(id, Transform { x: 1.0, y: 2.0, z: 3.0 });
        assert!(map.has(id));
        assert_eq!(map.len(), 1);

        let t = map.get(id).unwrap();
        assert_eq!(t.x, 1.0);
        assert_eq!(t.y, 2.0);
        assert_eq!(t.z, 3.0);

        let removed = map.remove(id).unwrap();
        assert_eq!(removed.x, 1.0);
        assert!(!map.has(id));
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn component_map_get_mut() {
        let mut map: ComponentMap<Health> = ComponentMap::new();
        let id = EntityId::new(0, 0);
        map.insert(id, Health { current: 100, max: 100, alive: true });

        map.get_mut(id).unwrap().current = 50;
        assert_eq!(map.get(id).unwrap().current, 50);
    }

    #[test]
    fn component_map_overwrite() {
        let mut map: ComponentMap<Transform> = ComponentMap::new();
        let id = EntityId::new(0, 0);
        map.insert(id, Transform { x: 0.0, y: 0.0, z: 0.0 });
        map.insert(id, Transform { x: 10.0, y: 20.0, z: 30.0 });
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(id).unwrap().x, 10.0);
    }

    #[test]
    fn component_map_iter() {
        let mut map: ComponentMap<Transform> = ComponentMap::new();
        let e1 = EntityId::new(0, 0);
        let e2 = EntityId::new(1, 0);
        map.insert(e1, Transform { x: 1.0, y: 0.0, z: 0.0 });
        map.insert(e2, Transform { x: 2.0, y: 0.0, z: 0.0 });

        let mut positions: Vec<f32> = map.iter().map(|(_, t)| t.x).collect();
        positions.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(positions, vec![1.0, 2.0]);
    }

    #[test]
    fn world_multiple_components() {
        let mut world = World::new();
        let e = world.spawn();
        world.transforms.insert(e, Transform { x: 1.0, y: 2.0, z: 3.0 });
        world.velocities.insert(e, Velocity { vx: 0.1, vy: 0.2, vz: 0.3 });
        world.healths.insert(e, Health { current: 100, max: 100, alive: true });

        assert!(world.transforms.has(e));
        assert!(world.velocities.has(e));
        assert!(world.healths.has(e));

        world.despawn(e);
        assert!(!world.transforms.has(e));
        assert!(!world.velocities.has(e));
        assert!(!world.healths.has(e));
    }

    struct MovementSystem;

    impl System for MovementSystem {
        fn run(&self, world: &mut World, _tick: u64) {
            let mut updates: Vec<(EntityId, f32, f32, f32)> = Vec::new();
            for (id, vel) in world.velocities.iter() {
                let pos = world.transforms.get(id);
                if let Some(pos) = pos {
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

    #[test]
    fn system_executor_runs_systems() {
        let mut world = World::new();
        let e = world.spawn();
        world.transforms.insert(e, Transform { x: 0.0, y: 0.0, z: 0.0 });
        world.velocities.insert(e, Velocity { vx: 1.0, vy: 2.0, vz: 3.0 });

        let mut executor = SystemExecutor::new();
        executor.add_system(Box::new(MovementSystem));
        executor.run_all(&mut world, 1);

        let t = world.transforms.get(e).unwrap();
        assert_eq!(t.x, 1.0);
        assert_eq!(t.y, 2.0);
        assert_eq!(t.z, 3.0);
    }

    #[test]
    fn entity_allocator_generations_increment() {
        let mut alloc = EntityAllocator::new();
        let e1 = alloc.alloc();
        alloc.free(e1);
        let e2 = alloc.alloc();
        assert_eq!(e1.index(), e2.index());
        assert_eq!(e2.generation(), 1);
        assert!(!alloc.is_alive(e1));
        assert!(alloc.is_alive(e2));
    }
}
