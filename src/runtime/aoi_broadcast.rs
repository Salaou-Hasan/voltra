use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

use super::ecs::EntityId;
use super::aoi::AreaOfInterest;

pub type ClientId = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AoiDelta {
    pub entity_id: u64,
    pub operation: String,
    pub x: f32,
    pub y: f32,
    pub data: Option<serde_json::Value>,
    /// Server tick when this delta was generated. Clients use this for
    /// interpolation — they render entity positions between the last two
    /// known ticks for smooth movement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_tick: Option<u64>,
}

pub use crate::subscriptions::OutboundFrames;

struct AoiClient {
    tx: Sender<OutboundFrames>,
    player_entity: EntityId,
}

pub struct AoiBroadcaster {
    clients: DashMap<ClientId, AoiClient>,
    previous_transforms: DashMap<EntityId, (f32, f32)>,
}

impl AoiBroadcaster {
    pub fn new() -> Self {
        Self {
            clients: DashMap::new(),
            previous_transforms: DashMap::new(),
        }
    }

    pub fn register_client(
        &self,
        client_id: ClientId,
        tx: Sender<OutboundFrames>,
        player_entity: EntityId,
    ) {
        self.clients.insert(client_id, AoiClient {
            tx,
            player_entity,
        });
    }

    pub fn unregister_client(&self, client_id: ClientId) {
        self.clients.remove(&client_id);
    }

    pub fn set_player_entity(&self, client_id: ClientId, entity: EntityId) {
        if let Some(mut c) = self.clients.get_mut(&client_id) {
            c.player_entity = entity;
        }
    }

    /// Compute deltas from transform changes and broadcast to nearby players.
    /// Call this after each tick.
    pub fn broadcast_tick(
        &self,
        aoi: &AreaOfInterest,
        current_transforms: &HashMap<EntityId, (f32, f32)>,
        tick_number: u64,
    ) {
        if self.clients.is_empty() || current_transforms.is_empty() {
            return;
        }

        let deltas = self.compute_deltas(current_transforms, tick_number);

        if deltas.is_empty() {
            return;
        }

        let mut per_client_deltas: HashMap<ClientId, Vec<&AoiDelta>> = HashMap::new();

        for client_entry in self.clients.iter() {
            let client = client_entry.value();
            let client_id = *client_entry.key();

            let visible: HashSet<EntityId> = aoi
                .visible_to(client.player_entity)
                .into_iter()
                .collect();

            for delta in &deltas {
                let eid = EntityId::new(delta.entity_id as u32, 0);
                if visible.contains(&eid) || delta.entity_id == client.player_entity.index() as u64 {
                    per_client_deltas
                        .entry(client_id)
                        .or_default()
                        .push(delta);
                }
            }
        }

        for (client_id, client_deltas) in &per_client_deltas {
            if let Some(client) = self.clients.get(client_id) {
                let frames = self.encode_deltas(client_deltas);
                if let Err(e) = client.tx.try_send(frames) {
                    log::warn!("[aoi] Send failed for client {}: {}", client_id, e);
                }
            }
        }
    }

    fn compute_deltas(&self, current: &HashMap<EntityId, (f32, f32)>, tick_number: u64) -> Vec<AoiDelta> {
        let mut deltas = Vec::new();

        for (&eid, &(x, y)) in current {
            match self.previous_transforms.get(&eid) {
                Some(prev) => {
                    let (old_x, old_y) = *prev;
                    if (old_x - x).abs() > 0.001 || (old_y - y).abs() > 0.001 {
                        deltas.push(AoiDelta {
                            entity_id: eid.index() as u64,
                            operation: "update".to_string(),
                            x,
                            y,
                            data: None,
                            server_tick: Some(tick_number),
                        });
                    }
                }
                None => {
                    deltas.push(AoiDelta {
                            entity_id: eid.index() as u64,
                            operation: "insert".to_string(),
                            x,
                            y,
                            data: None,
                            server_tick: Some(tick_number),
                        });
                }
            }
        }

        let mut removed = Vec::new();
        for prev_entry in self.previous_transforms.iter() {
            if !current.contains_key(prev_entry.key()) {
                removed.push(*prev_entry.key());
            }
        }
        for eid in removed {
            if let Some((x, y)) = self.previous_transforms.remove(&eid).map(|(_, v)| v) {
                    deltas.push(AoiDelta {
                        entity_id: eid.index() as u64,
                        operation: "delete".to_string(),
                        x,
                        y,
                        data: None,
                        server_tick: Some(tick_number),
                    });
            }
        }

        deltas
    }

    fn encode_deltas(&self, deltas: &[&AoiDelta]) -> OutboundFrames {
        if deltas.len() == 1 {
            let payload = rmp_serde::to_vec(deltas[0]).unwrap_or_default();
            OutboundFrames::One(Arc::new(Bytes::from(payload)))
        } else {
            let payload = rmp_serde::to_vec(deltas).unwrap_or_default();
            OutboundFrames::One(Arc::new(Bytes::from(payload)))
        }
    }

    /// Update the previous-transform cache after a tick.
    pub fn update_snapshot(&self, transforms: &HashMap<EntityId, (f32, f32)>) {
        self.previous_transforms.clear();
        for (&eid, &pos) in transforms {
            self.previous_transforms.insert(eid, pos);
        }
    }

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::ecs::EntityId;

    fn make_channel() -> (Sender<OutboundFrames>, tokio::sync::mpsc::Receiver<OutboundFrames>) {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        (tx, rx)
    }

    #[test]
    fn broadcaster_detects_new_entity() {
        let b = AoiBroadcaster::new();
        let mut current = HashMap::new();
        let e = EntityId::new(0, 0);
        current.insert(e, (10.0, 20.0));

        let deltas = b.compute_deltas(&current);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].operation, "insert");
        assert_eq!(deltas[0].x, 10.0);
    }

    #[test]
    fn broadcaster_detects_movement() {
        let b = AoiBroadcaster::new();
        let e = EntityId::new(0, 0);

        let mut snap1 = HashMap::new();
        snap1.insert(e, (0.0, 0.0));
        b.update_snapshot(&snap1);

        let mut snap2 = HashMap::new();
        snap2.insert(e, (5.0, 10.0));
        let deltas = b.compute_deltas(&snap2);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].operation, "update");
        assert_eq!(deltas[0].x, 5.0);
        assert_eq!(deltas[0].y, 10.0);
    }

    #[test]
    fn broadcaster_no_delta_for_stationary() {
        let b = AoiBroadcaster::new();
        let e = EntityId::new(0, 0);

        let mut snap1 = HashMap::new();
        snap1.insert(e, (5.0, 5.0));
        b.update_snapshot(&snap1);

        let mut snap2 = HashMap::new();
        snap2.insert(e, (5.0, 5.0));
        let deltas = b.compute_deltas(&snap2);
        assert!(deltas.is_empty());
    }

    #[test]
    fn broadcaster_detects_removal() {
        let b = AoiBroadcaster::new();
        let e = EntityId::new(0, 0);

        let mut snap1 = HashMap::new();
        snap1.insert(e, (10.0, 10.0));
        b.update_snapshot(&snap1);

        let snap2 = HashMap::new();
        let deltas = b.compute_deltas(&snap2);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].operation, "delete");
    }

    #[tokio::test]
    async fn broadcaster_sends_to_nearby_client_only() {
        let (tx, mut rx) = make_channel();
        let b = AoiBroadcaster::new();

        let player = EntityId::new(0, 0);
        let far_away = EntityId::new(1, 0);
        let nearby = EntityId::new(2, 0);

        b.register_client(1, tx, player);

        let mut aoi = AreaOfInterest::new(100.0, 100.0);
        aoi.insert(player, 0.0, 0.0);
        aoi.insert(nearby, 10.0, 0.0);
        aoi.insert(far_away, 500.0, 500.0);
        aoi.update();

        let mut transforms = HashMap::new();
        transforms.insert(player, (0.0, 0.0));
        transforms.insert(nearby, (10.0, 0.0));
        transforms.insert(far_away, (500.0, 500.0));
        b.update_snapshot(&transforms);

        let mut transforms2 = HashMap::new();
        transforms2.insert(player, (0.0, 0.0));
        transforms2.insert(nearby, (15.0, 0.0));
        transforms2.insert(far_away, (510.0, 510.0));

        b.broadcast_tick(&aoi, &transforms2, 1);

        let msg = rx.recv().await.expect("should receive a message");
        match msg {
            OutboundFrames::One(bytes) => {
                let delta: AoiDelta = rmp_serde::from_slice(&bytes).unwrap();
                assert_eq!(delta.entity_id, 2);
                assert_eq!(delta.operation, "update");
            }
            _ => panic!("expected One frame"),
        }
    }
}
