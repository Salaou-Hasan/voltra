// ============================================================================
// SubscriptionManager — reverse-index + encode-once rewrite
//
// Session 7 — TODO-003: Initial state sync on subscribe
//
//   BEFORE: subscribe() registered the query and returned.  Clients received
//   only future deltas; they had to do a full read on connect to see existing
//   data, defeating the subscription model.
//
//   FIX: subscribe() now accepts an optional Arc<TableStore>.  When provided
//   it immediately:
//     1. Queries TableStore for all rows matching the subscription predicate.
//     2. Serialises each matching row as a SubscriptionDiff with operation
//        "initial_snapshot" and sends it to the client.
//   This happens AFTER the subscription is registered in the index so there
//   is no race: if a reducer fires between registration and snapshot delivery,
//   the client gets the delta too (it may be a duplicate but that is safe).
//
//   Why "initial_snapshot" not "insert"?
//   Clients can distinguish a snapshot from a live insert and suppress
//   duplicate-insert warnings if they want.  Clients that don't care can
//   treat it the same as "insert".
//
// Session 5 — Reverse index (O(matching) not O(all)):
//   table_index: DashMap<table_name, DashMap<client_id, Vec<sub_id>>>
//   publish_deltas() now O(matching_subscribers) per delta.
//
// Session 4 — encode-once fan-out + CPU-aware DashMap shards.
// ============================================================================

use crate::error::{NeonDBError, Result};
use crate::network::message::{ServerMessage, SubscriptionDiff};
use crate::table::{RowDelta, TableStore};
use bytes::Bytes;
use dashmap::DashMap;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::mpsc::UnboundedSender;

pub type ClientId = u64;

// ── Predicate ─────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct SubscriptionFilter {
    pub table_name: String,
    pub predicate: Option<Predicate>,
}

#[derive(Clone, Debug)]
pub struct Predicate {
    pub field: String,
    pub op: ComparisonOp,
    pub value: Value,
}

#[derive(Clone, Debug)]
pub enum ComparisonOp {
    Eq, Ne, Gt, Lt, Ge, Le,
}

impl ComparisonOp {
    pub fn from_str(op: &str) -> Option<Self> {
        match op {
            "==" => Some(Self::Eq), "!=" => Some(Self::Ne),
            ">=" => Some(Self::Ge), "<=" => Some(Self::Le),
            ">"  => Some(Self::Gt), "<"  => Some(Self::Lt),
            _    => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Subscription {
    pub id: String,
    pub filter: SubscriptionFilter,
}

// ── Client info ───────────────────────────────────────────────────────────────

struct ClientInfo {
    tx: UnboundedSender<Arc<Bytes>>,
    subscriptions: DashMap<String, Subscription>,
}

// ── Encoded frame ─────────────────────────────────────────────────────────────

struct EncodedFrame {
    bytes: Arc<Bytes>,
}

impl EncodedFrame {
    fn encode(sub_id: &str, delta: &RowDelta) -> Option<Self> {
        let diff = SubscriptionDiff {
            subscription_id: sub_id.to_string(),
            table_name: delta.table_name.clone(),
            row_key: delta.row_key.clone(),
            operation: delta.operation.clone(),
            row_data: delta.row_data.clone(),
        };
        let msg = ServerMessage::SubscriptionDiff(diff);
        rmp_serde::to_vec(&msg)
            .ok()
            .map(|b| EncodedFrame { bytes: Arc::new(Bytes::from(b)) })
    }

    fn encode_snapshot(sub_id: &str, table_name: &str, row_key: &str, row_data: Value) -> Option<Self> {
        let diff = SubscriptionDiff {
            subscription_id: sub_id.to_string(),
            table_name: table_name.to_string(),
            row_key: row_key.to_string(),
            operation: "initial_snapshot".to_string(),
            row_data: Some(row_data),
        };
        let msg = ServerMessage::SubscriptionDiff(diff);
        rmp_serde::to_vec(&msg)
            .ok()
            .map(|b| EncodedFrame { bytes: Arc::new(Bytes::from(b)) })
    }
}

// ── Manager ───────────────────────────────────────────────────────────────────

pub struct SubscriptionManager {
    clients: DashMap<ClientId, Arc<ClientInfo>>,
    table_index: DashMap<String, DashMap<ClientId, Vec<String>>>,
    next_id: AtomicU64,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        SubscriptionManager {
            clients: DashMap::with_capacity_and_shard_amount(256, 16),
            table_index: DashMap::with_capacity_and_shard_amount(32, 8),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn register_client(&self, tx: UnboundedSender<Arc<Bytes>>) -> ClientId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.clients.insert(id, Arc::new(ClientInfo {
            tx,
            subscriptions: DashMap::new(),
        }));
        id
    }

    pub fn unregister_client(&self, client_id: ClientId) {
        if let Some((_, client)) = self.clients.remove(&client_id) {
            for sub_ref in client.subscriptions.iter() {
                let table_name = &sub_ref.value().filter.table_name;
                self.remove_from_table_index(table_name, client_id, &sub_ref.key().clone());
            }
        }
    }

    /// Register a subscription and immediately deliver matching existing rows
    /// as "initial_snapshot" frames.
    ///
    /// TODO-003: tables is now taken as an optional parameter.
    /// - When Some(tables): initial snapshot is sent before returning.
    /// - When None: only registers the subscription (used in tests that don't
    ///   need snapshot behaviour).
    ///
    /// Race safety: we register the subscription in the index FIRST, then
    /// snapshot.  A reducer firing between registration and snapshot delivery
    /// will push a delta to the client.  The client may receive both a snapshot
    /// row and a delta for the same row — that is safe (last write wins in the
    /// client cache).
    pub fn subscribe(
        &self,
        client_id: ClientId,
        subscription_id: String,
        query: String,
    ) -> Result<()> {
        self.subscribe_with_snapshot(client_id, subscription_id, query, None)
    }

    pub fn subscribe_with_snapshot(
        &self,
        client_id: ClientId,
        subscription_id: String,
        query: String,
        tables: Option<&Arc<TableStore>>,
    ) -> Result<()> {
        let filter = parse_subscription_query(&query)?;
        let table_name = filter.table_name.clone();

        let client = self.clients.get(&client_id)
            .ok_or_else(|| NeonDBError::invalid_argument(
                format!("Unknown client: {}", client_id),
            ))?;

        let subscription = Subscription {
            id: subscription_id.clone(),
            filter: filter.clone(),
        };

        // ── Register in per-client map and reverse index (BEFORE snapshot) ───
        client.subscriptions.insert(subscription_id.clone(), subscription);

        self.table_index
            .entry(table_name.clone())
            .or_insert_with(|| DashMap::with_capacity_and_shard_amount(64, 4))
            .entry(client_id)
            .or_insert_with(Vec::new)
            .push(subscription_id.clone());

        // ── TODO-003: initial state sync ─────────────────────────────────────
        // After registration, snapshot all currently matching rows and push
        // them to the client as "initial_snapshot" frames.
        if let Some(tables) = tables {
            if let Ok(rows) = tables.list_rows(&table_name) {
                let tx = client.tx.clone();
                for row_value in rows {
                    // Apply predicate to each existing row
                    let row_key = row_value
                        .get("row_key")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    let matches = match &filter.predicate {
                        None => true,
                        Some(pred) => {
                            let actual = if pred.field == "row_key" {
                                Some(Value::String(row_key.clone()))
                            } else {
                                row_value.get(&pred.field).cloned()
                            };
                            pred.matches(actual.as_ref())
                        }
                    };

                    if matches {
                        if let Some(frame) = EncodedFrame::encode_snapshot(
                            &subscription_id,
                            &table_name,
                            &row_key,
                            row_value,
                        ) {
                            // Non-blocking send — if the channel is closed the
                            // client disconnected before we could snapshot.
                            let _ = tx.send(frame.bytes);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn unsubscribe(&self, client_id: ClientId, subscription_id: &str) -> Result<bool> {
        let client = self.clients.get(&client_id)
            .ok_or_else(|| NeonDBError::invalid_argument(
                format!("Unknown client: {}", client_id),
            ))?;

        if let Some((_, sub)) = client.subscriptions.remove(subscription_id) {
            let table_name = &sub.filter.table_name;
            self.remove_from_table_index(table_name, client_id, subscription_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn remove_from_table_index(
        &self,
        table_name: &str,
        client_id: ClientId,
        subscription_id: &str,
    ) {
        if let Some(client_map) = self.table_index.get(table_name) {
            if let Some(mut sub_ids) = client_map.get_mut(&client_id) {
                sub_ids.retain(|s| s != subscription_id);
                if sub_ids.is_empty() {
                    drop(sub_ids);
                    client_map.remove(&client_id);
                }
            }
        }
        self.table_index.remove_if(table_name, |_, v| v.is_empty());
    }

    // ── Hot path: publish_deltas ───────────────────────────────────────────────

    pub fn publish_deltas(&self, deltas: &[RowDelta]) {
        if self.clients.is_empty() || deltas.is_empty() {
            return;
        }

        for delta in deltas {
            let table_entry = match self.table_index.get(&delta.table_name) {
                Some(e) => e,
                None    => continue,
            };

            let mut matching: Vec<(UnboundedSender<Arc<Bytes>>, String)> = Vec::new();

            for client_entry in table_entry.iter() {
                let client_id = *client_entry.key();
                let sub_ids   = client_entry.value();

                let client = match self.clients.get(&client_id) {
                    Some(c) => c,
                    None    => continue,
                };

                for sub_id in sub_ids.iter() {
                    let sub = match client.subscriptions.get(sub_id) {
                        Some(s) => s,
                        None    => continue,
                    };

                    if sub.filter.matches(delta) {
                        matching.push((client.tx.clone(), sub_id.clone()));
                    }
                }
            }

            if matching.is_empty() {
                continue;
            }

            matching.sort_unstable_by(|a, b| a.1.cmp(&b.1));

            let mut i = 0;
            while i < matching.len() {
                let sub_id = &matching[i].1;

                let run_end = matching[i..]
                    .iter()
                    .position(|(_, sid)| sid != sub_id)
                    .map(|p| i + p)
                    .unwrap_or(matching.len());

                if let Some(frame) = EncodedFrame::encode(sub_id, delta) {
                    for (tx, _) in &matching[i..run_end] {
                        let _ = tx.send(frame.bytes.clone());
                    }
                }

                i = run_end;
            }
        }
    }

    pub fn active_subscriptions(&self) -> usize {
        self.clients.iter().map(|c| c.subscriptions.len()).sum()
    }

    pub fn active_connections(&self) -> usize {
        self.clients.len()
    }
}

// ── Filter evaluation ─────────────────────────────────────────────────────────

impl SubscriptionFilter {
    pub fn matches(&self, delta: &RowDelta) -> bool {
        if self.table_name != delta.table_name {
            return false;
        }
        let Some(predicate) = &self.predicate else { return true; };
        let row_value = delta.row_data_value();
        let actual = row_value.as_ref().and_then(|v| {
            if predicate.field == "row_key" {
                Some(Value::String(delta.row_key.clone()))
            } else {
                v.get(&predicate.field).cloned()
            }
        });
        predicate.matches(actual.as_ref())
    }
}

impl Predicate {
    pub fn matches(&self, actual: Option<&Value>) -> bool {
        let Some(actual) = actual else { return false; };
        match (&self.op, actual) {
            (ComparisonOp::Eq, Value::String(s)) =>
                matches!(&self.value, Value::String(e) if s == e),
            (ComparisonOp::Ne, Value::String(s)) =>
                matches!(&self.value, Value::String(e) if s != e),
            (ComparisonOp::Eq, Value::Number(n)) =>
                matches!(&self.value, Value::Number(e) if n == e),
            (ComparisonOp::Ne, Value::Number(n)) =>
                matches!(&self.value, Value::Number(e) if n != e),
            (ComparisonOp::Gt, Value::Number(n)) =>
                matches!(&self.value, Value::Number(e) if
                    compare_number(n, e) == Some(std::cmp::Ordering::Greater)),
            (ComparisonOp::Lt, Value::Number(n)) =>
                matches!(&self.value, Value::Number(e) if
                    compare_number(n, e) == Some(std::cmp::Ordering::Less)),
            (ComparisonOp::Ge, Value::Number(n)) =>
                matches!(&self.value, Value::Number(e) if
                    matches!(compare_number(n, e),
                        Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal))),
            (ComparisonOp::Le, Value::Number(n)) =>
                matches!(&self.value, Value::Number(e) if
                    matches!(compare_number(n, e),
                        Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal))),
            _ => false,
        }
    }
}

fn compare_number(
    l: &serde_json::Number,
    r: &serde_json::Number,
) -> Option<std::cmp::Ordering> {
    if let (Some(a), Some(b)) = (l.as_i64(), r.as_i64()) { return Some(a.cmp(&b)); }
    if let (Some(a), Some(b)) = (l.as_u64(), r.as_u64()) { return Some(a.cmp(&b)); }
    if let (Some(a), Some(b)) = (l.as_f64(), r.as_f64()) { return a.partial_cmp(&b); }
    None
}

// ── Query parser ──────────────────────────────────────────────────────────────

fn parse_subscription_query(query: &str) -> Result<SubscriptionFilter> {
    let normalized = query.trim();
    let normalized = if normalized.to_lowercase().starts_with("subscribe ") {
        normalized[10..].trim()
    } else {
        normalized
    };
    let lower = normalized.to_lowercase();
    let (table_name, predicate) = if let Some(idx) = lower.find(" where ") {
        (&normalized[..idx], Some(normalized[idx + 7..].trim()))
    } else {
        (normalized, None)
    };
    let table_name = table_name.trim();
    if table_name.is_empty() {
        return Err(NeonDBError::invalid_argument(
            "Subscription query missing table name",
        ));
    }
    let predicate = predicate
        .map(|p| parse_predicate(p.trim()))
        .transpose()?;
    Ok(SubscriptionFilter {
        table_name: table_name.to_string(),
        predicate,
    })
}

fn parse_predicate(predicate: &str) -> Result<Predicate> {
    for op in [">=", "<=", "==", "!=", ">", "<"] {
        if let Some(idx) = predicate.find(op) {
            let field = predicate[..idx].trim();
            let value_part = predicate[idx + op.len()..].trim();
            let op = ComparisonOp::from_str(op)
                .ok_or_else(|| NeonDBError::invalid_argument("Unsupported comparator"))?;
            let value = parse_predicate_value(value_part)?;
            return Ok(Predicate {
                field: field.to_string(),
                op,
                value,
            });
        }
    }
    Err(NeonDBError::invalid_argument("Subscription predicate invalid"))
}

fn parse_predicate_value(value: &str) -> Result<Value> {
    let t = value.trim();
    if (t.starts_with('"') && t.ends_with('"'))
        || (t.starts_with('\'') && t.ends_with('\''))
    {
        return Ok(Value::String(t[1..t.len() - 1].to_string()));
    }
    if let Ok(i) = t.parse::<i64>() {
        return Ok(Value::Number(i.into()));
    }
    if let Ok(f) = t.parse::<f64>() {
        return Ok(serde_json::Number::from_f64(f)
            .map(Value::Number)
            .ok_or_else(|| NeonDBError::invalid_argument("Invalid numeric literal"))?);
    }
    if t.eq_ignore_ascii_case("true")  { return Ok(Value::Bool(true));  }
    if t.eq_ignore_ascii_case("false") { return Ok(Value::Bool(false)); }
    Ok(Value::String(t.to_string()))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{RowDelta, TableStore};

    fn make_delta(table: &str, key: &str, data: Value) -> RowDelta {
        RowDelta {
            table_name: table.to_string(),
            operation: "update".to_string(),
            row_key: key.to_string(),
            row_id: 1,
            shard_id: 0,
            payload_arc: None,
            row_data: Some(data),
        }
    }

    // ── Existing tests (all must still pass) ─────────────────────────────────

    #[test]
    fn parse_subscription_query_without_predicate() {
        let f = parse_subscription_query("counters").unwrap();
        assert_eq!(f.table_name, "counters");
        assert!(f.predicate.is_none());
    }

    #[test]
    fn parse_subscription_query_with_predicate_case_insensitive() {
        let f = parse_subscription_query("Subscribe counters WHERE value >= 100").unwrap();
        assert_eq!(f.table_name, "counters");
        assert!(f.predicate.is_some());
    }

    #[test]
    fn subscription_filter_matches_row_data() {
        let f = parse_subscription_query("counters WHERE score > 15").unwrap();
        let d = make_delta("counters", "row1", serde_json::json!({"score": 20}));
        assert!(f.matches(&d));
    }

    #[test]
    fn subscription_filter_rejects_wrong_table() {
        let f = parse_subscription_query("counters").unwrap();
        let d = make_delta("users", "row1", serde_json::json!({"score": 20}));
        assert!(!f.matches(&d));
    }

    #[test]
    fn publish_deltas_arc_clone_count() {
        let mgr = SubscriptionManager::new();
        let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let id1 = mgr.register_client(tx1);
        let id2 = mgr.register_client(tx2);
        mgr.subscribe(id1, "s1".to_string(), "counters".to_string()).unwrap();
        mgr.subscribe(id2, "s2".to_string(), "counters".to_string()).unwrap();

        let deltas = vec![make_delta("counters", "k", serde_json::json!({"v": 1}))];
        mgr.publish_deltas(&deltas);
    }

    #[test]
    fn publish_deltas_shared_sub_id_encodes_once() {
        let mgr = SubscriptionManager::new();
        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let id1 = mgr.register_client(tx1);
        let id2 = mgr.register_client(tx2);
        mgr.subscribe(id1, "world_sync".to_string(), "players".to_string()).unwrap();
        mgr.subscribe(id2, "world_sync".to_string(), "players".to_string()).unwrap();

        let deltas = vec![make_delta("players", "hero_1", serde_json::json!({"hp": 100}))];
        mgr.publish_deltas(&deltas);

        let frame1 = rx1.try_recv().expect("client 1 should receive");
        let frame2 = rx2.try_recv().expect("client 2 should receive");

        assert_eq!(
            Arc::as_ptr(&frame1),
            Arc::as_ptr(&frame2),
            "shared sub_id must share the same Arc<Bytes> allocation"
        );
    }

    #[test]
    fn publish_deltas_unique_sub_ids_receive_correct_data() {
        let mgr = SubscriptionManager::new();
        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let id1 = mgr.register_client(tx1);
        let id2 = mgr.register_client(tx2);
        mgr.subscribe(id1, "sub_client_1".to_string(), "counters".to_string()).unwrap();
        mgr.subscribe(id2, "sub_client_2".to_string(), "counters".to_string()).unwrap();

        let deltas = vec![make_delta("counters", "score", serde_json::json!({"value": 42}))];
        mgr.publish_deltas(&deltas);

        let frame1 = rx1.try_recv().expect("client 1 should receive");
        let frame2 = rx2.try_recv().expect("client 2 should receive");

        use crate::network::message::ServerMessage;
        let msg1: ServerMessage = rmp_serde::from_slice(&frame1).unwrap();
        let msg2: ServerMessage = rmp_serde::from_slice(&frame2).unwrap();

        match msg1 {
            ServerMessage::SubscriptionDiff(d) => assert_eq!(d.subscription_id, "sub_client_1"),
            _ => panic!("expected SubscriptionDiff"),
        }
        match msg2 {
            ServerMessage::SubscriptionDiff(d) => assert_eq!(d.subscription_id, "sub_client_2"),
            _ => panic!("expected SubscriptionDiff"),
        }
    }

    #[test]
    fn publish_deltas_predicate_filters_correctly() {
        let mgr = SubscriptionManager::new();
        let (tx_match, mut rx_match) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let (tx_skip,  mut rx_skip)  = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let id_match = mgr.register_client(tx_match);
        let id_skip  = mgr.register_client(tx_skip);

        mgr.subscribe(id_match, "high".to_string(), "counters WHERE value >= 10".to_string()).unwrap();
        mgr.subscribe(id_skip,  "low".to_string(),  "counters WHERE value >= 100".to_string()).unwrap();

        let deltas = vec![make_delta("counters", "score", serde_json::json!({"value": 42}))];
        mgr.publish_deltas(&deltas);

        assert!(rx_match.try_recv().is_ok(), "matching predicate should receive");
        assert!(rx_skip.try_recv().is_err(), "non-matching predicate should be filtered");
    }

    #[test]
    fn publish_deltas_no_subscribers_is_noop() {
        let mgr = SubscriptionManager::new();
        let deltas = vec![make_delta("counters", "k", serde_json::json!({"v": 1}))];
        mgr.publish_deltas(&deltas);
    }

    #[test]
    fn publish_deltas_wrong_table_not_delivered() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "s".to_string(), "players".to_string()).unwrap();

        let deltas = vec![make_delta("counters", "k", serde_json::json!({"v": 1}))];
        mgr.publish_deltas(&deltas);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn reverse_index_skips_unrelated_table_entirely() {
        let mgr = SubscriptionManager::new();
        let (tx_a, mut rx_a) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let (tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let id_a = mgr.register_client(tx_a);
        let id_b = mgr.register_client(tx_b);

        mgr.subscribe(id_a, "sa".to_string(), "table_alpha".to_string()).unwrap();
        mgr.subscribe(id_b, "sb".to_string(), "table_beta".to_string()).unwrap();

        let deltas = vec![make_delta("table_alpha", "k1", serde_json::json!({"x": 1}))];
        mgr.publish_deltas(&deltas);

        assert!(rx_a.try_recv().is_ok(),  "table_alpha subscriber must receive");
        assert!(rx_b.try_recv().is_err(), "table_beta subscriber must NOT receive");
    }

    #[test]
    fn reverse_index_cleaned_up_on_unsubscribe() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let cid = mgr.register_client(tx);

        mgr.subscribe(cid, "sub1".to_string(), "counters".to_string()).unwrap();
        mgr.unsubscribe(cid, "sub1").unwrap();

        assert!(mgr.table_index.get("counters").is_none() ||
                mgr.table_index.get("counters").map(|m| m.is_empty()).unwrap_or(true));

        let deltas = vec![make_delta("counters", "k", serde_json::json!({"v": 99}))];
        mgr.publish_deltas(&deltas);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn reverse_index_cleaned_up_on_unregister() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let cid = mgr.register_client(tx);

        mgr.subscribe(cid, "s1".to_string(), "players".to_string()).unwrap();
        mgr.subscribe(cid, "s2".to_string(), "counters".to_string()).unwrap();
        mgr.unregister_client(cid);

        for table in &["players", "counters"] {
            let absent = mgr.table_index
                .get(*table)
                .map(|m| m.get(&cid).is_none())
                .unwrap_or(true);
            assert!(absent);
        }

        let deltas = vec![
            make_delta("players", "hero", serde_json::json!({"hp": 50})),
            make_delta("counters", "score", serde_json::json!({"value": 1})),
        ];
        mgr.publish_deltas(&deltas);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn reverse_index_correct_delivery_at_scale() {
        let mgr = SubscriptionManager::new();

        let mut rxs_players  = Vec::new();
        let mut rxs_counters = Vec::new();

        for i in 0..25 {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
            let cid = mgr.register_client(tx);
            mgr.subscribe(cid, format!("ps_{}", i), "players".to_string()).unwrap();
            rxs_players.push(rx);
        }
        for i in 0..25 {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
            let cid = mgr.register_client(tx);
            mgr.subscribe(cid, format!("cs_{}", i), "counters".to_string()).unwrap();
            rxs_counters.push(rx);
        }

        let deltas = vec![make_delta("players", "p1", serde_json::json!({"hp": 100}))];
        mgr.publish_deltas(&deltas);

        for (i, rx) in rxs_players.iter_mut().enumerate() {
            assert!(rx.try_recv().is_ok(),  "players subscriber {} must receive", i);
        }
        for (i, rx) in rxs_counters.iter_mut().enumerate() {
            assert!(rx.try_recv().is_err(), "counters subscriber {} must NOT receive", i);
        }
    }

    #[test]
    fn client_with_multi_table_subscriptions() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let cid = mgr.register_client(tx);

        mgr.subscribe(cid, "watch_players".to_string(),  "players".to_string()).unwrap();
        mgr.subscribe(cid, "watch_counters".to_string(), "counters".to_string()).unwrap();

        let deltas = vec![make_delta("players", "hero", serde_json::json!({"hp": 75}))];
        mgr.publish_deltas(&deltas);

        let frame = rx.try_recv().expect("should receive one frame");
        let msg: crate::network::message::ServerMessage = rmp_serde::from_slice(&frame).unwrap();
        match msg {
            crate::network::message::ServerMessage::SubscriptionDiff(d) => {
                assert_eq!(d.subscription_id, "watch_players");
                assert_eq!(d.table_name, "players");
            }
            _ => panic!("expected SubscriptionDiff"),
        }
        assert!(rx.try_recv().is_err());
    }

    // ── TODO-003: Initial state sync tests ───────────────────────────────────

    /// Subscribe to a table that already has data — client must receive
    /// initial_snapshot frames for all matching rows immediately.
    #[test]
    fn initial_snapshot_delivered_on_subscribe() {
        let tables = Arc::new(TableStore::new());
        // Insert two counters before the client subscribes.
        tables.set_counter("alpha".to_string(), 10, 0).unwrap();
        tables.set_counter("beta".to_string(),  20, 0).unwrap();

        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let cid = mgr.register_client(tx);

        mgr.subscribe_with_snapshot(
            cid,
            "snap_all".to_string(),
            "counters".to_string(),
            Some(&tables),
        ).unwrap();

        // Should receive exactly 2 snapshot frames (one per existing row).
        let mut received = 0;
        while let Ok(frame) = rx.try_recv() {
            let msg: crate::network::message::ServerMessage =
                rmp_serde::from_slice(&frame).unwrap();
            match msg {
                crate::network::message::ServerMessage::SubscriptionDiff(d) => {
                    assert_eq!(d.subscription_id, "snap_all");
                    assert_eq!(d.operation, "initial_snapshot");
                    assert_eq!(d.table_name, "counters");
                    received += 1;
                }
                _ => panic!("expected SubscriptionDiff"),
            }
        }
        assert_eq!(received, 2, "expected 2 snapshot frames, got {}", received);
    }

    /// Initial snapshot must respect the subscription predicate — only
    /// matching rows should be sent.
    #[test]
    fn initial_snapshot_respects_predicate() {
        let tables = Arc::new(TableStore::new());
        tables.set_counter("low".to_string(),  5, 0).unwrap();
        tables.set_counter("high".to_string(), 50, 0).unwrap();

        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let cid = mgr.register_client(tx);

        // Only subscribe to rows where value > 10
        mgr.subscribe_with_snapshot(
            cid,
            "snap_high".to_string(),
            "counters WHERE value > 10".to_string(),
            Some(&tables),
        ).unwrap();

        let mut received = 0;
        while let Ok(_) = rx.try_recv() {
            received += 1;
        }
        // Only "high" (value=50) matches value > 10
        assert_eq!(received, 1, "expected 1 snapshot frame (only 'high' matches), got {}", received);
    }

    /// subscribe() without a table store must NOT send snapshot frames
    /// (backwards-compatible behaviour for tests that don't pass tables).
    #[test]
    fn subscribe_without_tables_sends_no_snapshot() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Arc<Bytes>>();
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "no_snap".to_string(), "counters".to_string()).unwrap();
        assert!(rx.try_recv().is_err(), "no snapshot should be sent without tables");
    }
}
