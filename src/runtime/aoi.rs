use std::collections::{HashMap, HashSet};

use super::ecs::EntityId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct GridCell {
    cx: i32,
    cy: i32,
}

impl GridCell {
    fn from_pos(x: f32, y: f32, cell_size: f32) -> Self {
        Self {
            cx: (x / cell_size).floor() as i32,
            cy: (y / cell_size).floor() as i32,
        }
    }
}

pub struct SpatialGrid {
    cell_size: f32,
    cells: HashMap<GridCell, HashSet<EntityId>>,
    positions: HashMap<EntityId, (f32, f32)>,
}

impl SpatialGrid {
    pub fn new(cell_size: f32) -> Self {
        Self {
            cell_size,
            cells: HashMap::new(),
            positions: HashMap::new(),
        }
    }

    pub fn insert(&mut self, id: EntityId, x: f32, y: f32) {
        let cell = GridCell::from_pos(x, y, self.cell_size);
        self.cells.entry(cell).or_default().insert(id);
        self.positions.insert(id, (x, y));
    }

    pub fn remove(&mut self, id: EntityId, x: f32, y: f32) {
        let cell = GridCell::from_pos(x, y, self.cell_size);
        if let Some(entities) = self.cells.get_mut(&cell) {
            entities.remove(&id);
            if entities.is_empty() {
                self.cells.remove(&cell);
            }
        }
        self.positions.remove(&id);
    }

    pub fn move_entity(&mut self, id: EntityId, old_x: f32, old_y: f32, new_x: f32, new_y: f32) {
        let old_cell = GridCell::from_pos(old_x, old_y, self.cell_size);
        let new_cell = GridCell::from_pos(new_x, new_y, self.cell_size);

        if old_cell != new_cell {
            if let Some(entities) = self.cells.get_mut(&old_cell) {
                entities.remove(&id);
                if entities.is_empty() {
                    self.cells.remove(&old_cell);
                }
            }
            self.cells.entry(new_cell).or_default().insert(id);
        }
        self.positions.insert(id, (new_x, new_y));
    }

    pub fn query_radius(&self, x: f32, y: f32, radius: f32) -> Vec<EntityId> {
        let mut result = Vec::new();
        let center = GridCell::from_pos(x, y, self.cell_size);
        let cell_radius = (radius / self.cell_size).ceil() as i32;
        let r2 = radius * radius;

        for dx in -cell_radius..=cell_radius {
            for dy in -cell_radius..=cell_radius {
                let cell = GridCell {
                    cx: center.cx + dx,
                    cy: center.cy + dy,
                };
                if let Some(entities) = self.cells.get(&cell) {
                    for &id in entities {
                        if let Some(&(ex, ey)) = self.positions.get(&id) {
                            let dist2 = (ex - x).powi(2) + (ey - y).powi(2);
                            if dist2 <= r2 {
                                result.push(id);
                            }
                        }
                    }
                }
            }
        }
        result
    }

    pub fn query_visible(&self, id: EntityId, view_distance: f32) -> Vec<EntityId> {
        let &(x, y) = match self.positions.get(&id) {
            Some(pos) => pos,
            None => return Vec::new(),
        };
        self.query_radius(x, y, view_distance)
            .into_iter()
            .filter(|&other| other != id)
            .collect()
    }

    pub fn position(&self, id: EntityId) -> Option<(f32, f32)> {
        self.positions.get(&id).copied()
    }

    pub fn len(&self) -> usize {
        self.positions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }
}

pub enum AoiEvent {
    Enter {
        viewer: EntityId,
        target: EntityId,
    },
    Exit {
        viewer: EntityId,
        target: EntityId,
    },
}

pub struct AreaOfInterest {
    pub grid: SpatialGrid,
    pub view_distance: f32,
    visible: HashMap<EntityId, HashSet<EntityId>>,
}

impl AreaOfInterest {
    pub fn new(cell_size: f32, view_distance: f32) -> Self {
        Self {
            grid: SpatialGrid::new(cell_size),
            view_distance,
            visible: HashMap::new(),
        }
    }

    pub fn insert(&mut self, id: EntityId, x: f32, y: f32) {
        self.grid.insert(id, x, y);
    }

    pub fn remove(&mut self, id: EntityId) {
        if let Some((x, y)) = self.grid.position(id) {
            self.grid.remove(id, x, y);
        }
        self.visible.remove(&id);
        for (_, seen) in self.visible.iter_mut() {
            seen.remove(&id);
        }
    }

    pub fn move_entity(&mut self, id: EntityId, new_x: f32, new_y: f32) {
        let old_pos = self.grid.position(id).unwrap_or((new_x, new_y));
        self.grid.move_entity(id, old_pos.0, old_pos.1, new_x, new_y);
    }

    pub fn update(&mut self) -> Vec<AoiEvent> {
        let mut events = Vec::new();
        let entity_ids: Vec<EntityId> = self.grid.positions.keys().copied().collect();

        for &id in &entity_ids {
            let visible_now: HashSet<EntityId> = self
                .grid
                .query_visible(id, self.view_distance)
                .into_iter()
                .collect();

            let old_visible = self.visible.entry(id).or_default();

            for &target in &visible_now {
                if !old_visible.contains(&target) {
                    events.push(AoiEvent::Enter { viewer: id, target });
                }
            }

            let exited: Vec<EntityId> = old_visible
                .iter()
                .filter(|t| !visible_now.contains(t))
                .copied()
                .collect();
            for target in exited {
                events.push(AoiEvent::Exit { viewer: id, target });
                old_visible.remove(&target);
            }

            *old_visible = visible_now;
        }

        events
    }

    pub fn visible_to(&self, id: EntityId) -> Vec<EntityId> {
        self.visible
            .get(&id)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spatial_grid_insert_and_query() {
        let mut grid = SpatialGrid::new(100.0);
        let e1 = EntityId::new(0, 0);
        let e2 = EntityId::new(1, 0);
        let e3 = EntityId::new(2, 0);

        grid.insert(e1, 10.0, 10.0);
        grid.insert(e2, 20.0, 20.0);
        grid.insert(e3, 500.0, 500.0);

        assert_eq!(grid.len(), 3);

        let nearby = grid.query_radius(15.0, 15.0, 20.0);
        assert!(nearby.contains(&e1));
        assert!(nearby.contains(&e2));
        assert!(!nearby.contains(&e3));
    }

    #[test]
    fn spatial_grid_remove() {
        let mut grid = SpatialGrid::new(100.0);
        let e1 = EntityId::new(0, 0);
        grid.insert(e1, 10.0, 10.0);
        assert_eq!(grid.len(), 1);

        grid.remove(e1, 10.0, 10.0);
        assert_eq!(grid.len(), 0);
        assert!(grid.query_radius(10.0, 10.0, 100.0).is_empty());
    }

    #[test]
    fn spatial_grid_move_crosses_cell_boundary() {
        let mut grid = SpatialGrid::new(100.0);
        let e1 = EntityId::new(0, 0);
        grid.insert(e1, 10.0, 10.0);

        grid.move_entity(e1, 10.0, 10.0, 150.0, 150.0);

        assert!(grid.query_radius(10.0, 10.0, 100.0).is_empty());
        let far = grid.query_radius(150.0, 150.0, 10.0);
        assert!(far.contains(&e1));
    }

    #[test]
    fn spatial_grid_move_within_same_cell() {
        let mut grid = SpatialGrid::new(100.0);
        let e1 = EntityId::new(0, 0);
        grid.insert(e1, 10.0, 10.0);

        grid.move_entity(e1, 10.0, 10.0, 50.0, 50.0);

        let pos = grid.position(e1).unwrap();
        assert_eq!(pos, (50.0, 50.0));
    }

    #[test]
    fn query_visible_excludes_self() {
        let mut grid = SpatialGrid::new(100.0);
        let e1 = EntityId::new(0, 0);
        let e2 = EntityId::new(1, 0);
        grid.insert(e1, 10.0, 10.0);
        grid.insert(e2, 20.0, 20.0);

        let visible = grid.query_visible(e1, 100.0);
        assert!(visible.contains(&e2));
        assert!(!visible.contains(&e1));
    }

    #[test]
    fn aoi_enter_exit_events() {
        let mut aoi = AreaOfInterest::new(100.0, 50.0);
        let e1 = EntityId::new(0, 0);
        let e2 = EntityId::new(1, 0);
        let e3 = EntityId::new(2, 0);

        aoi.insert(e1, 0.0, 0.0);
        aoi.insert(e2, 10.0, 0.0);
        aoi.insert(e3, 1000.0, 1000.0);

        let events = aoi.update();
        let enters: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, AoiEvent::Enter { .. }))
            .collect();
        assert_eq!(enters.len(), 2);

        let e1_sees = aoi.visible_to(e1);
        assert!(e1_sees.contains(&e2));
        assert!(!e1_sees.contains(&e3));
    }

    #[test]
    fn aoi_move_triggers_exit() {
        let mut aoi = AreaOfInterest::new(100.0, 50.0);
        let e1 = EntityId::new(0, 0);
        let e2 = EntityId::new(1, 0);

        aoi.insert(e1, 0.0, 0.0);
        aoi.insert(e2, 10.0, 0.0);
        aoi.update();

        aoi.move_entity(e2, 500.0, 500.0);
        let events = aoi.update();
        let exits: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, AoiEvent::Exit { .. }))
            .collect();
        assert_eq!(exits.len(), 2);
    }

    #[test]
    fn aoi_remove_entity() {
        let mut aoi = AreaOfInterest::new(100.0, 50.0);
        let e1 = EntityId::new(0, 0);
        let e2 = EntityId::new(1, 0);

        aoi.insert(e1, 0.0, 0.0);
        aoi.insert(e2, 10.0, 0.0);
        aoi.update();

        aoi.remove(e1);
        assert!(aoi.visible_to(e1).is_empty());
        assert!(!aoi.visible_to(e2).contains(&e1));
    }
}
