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
//
// Session 27 — Bug fix: predicate parser now accepts `=` (single equals) in
//   addition to `==`.  CLI users and template next-step hints naturally write
//   `WHERE room = 'general'`; the parser was rejecting it with
//   "Subscription predicate invalid".
// ============================================================================

use crate::error::{NeonDBError, Result};
use crate::network::message::{ServerMessage, SubscriptionBody, SubscriptionDiff, SubscriptionRoute};
use crate::table::{RowDelta, TableStore};
use bytes::Bytes;
use dashmap::DashMap;
use serde_json::Value;
use std::collections::HashMap;
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

/// A subscription predicate — a node in the filter expression tree.
#[derive(Clone, Debug)]
pub enum Predicate {
    /// Single-field comparison: `field op value`
    Comparison {
        field: String,
        op: ComparisonOp,
        value: Value,
    },
    /// Set-membership test: `field IN (v1, v2, ...)`
    In { field: String, values: Vec<Value> },
    /// Logical AND of two sub-predicates: `left AND right`
    And(Box<Predicate>, Box<Predicate>),
}

#[derive(Clone, Debug)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
}

impl ComparisonOp {
    pub fn from_str(op: &str) -> Option<Self> {
        match op {
            "==" | "=" => Some(Self::Eq),   // accept both = and ==
            "!=" | "<>" => Some(Self::Ne),  // accept both != and <>
            ">=" => Some(Self::Ge),
            "<=" => Some(Self::Le),
            ">" => Some(Self::Gt),
            "<" => Some(Self::Lt),
            _ => None,
        }
    }
}

impl ComparisonOp {
    /// Returns true if `actual` satisfies `self op expected`.
    pub fn compare(&self, actual: Option<&Value>, expected: &Value) -> bool {
        let Some(actual) = actual else {
            return false;
        };
        match (self, actual) {
            (Self::Eq, Value::String(s)) => matches!(expected, Value::String(e) if s == e),
            (Self::Ne, Value::String(s)) => matches!(expected, Value::String(e) if s != e),
            (Self::Eq, Value::Number(n)) => matches!(expected, Value::Number(e) if n == e),
            (Self::Ne, Value::Number(n)) => matches!(expected, Value::Number(e) if n != e),
            (Self::Gt, Value::Number(n)) => matches!(expected, Value::Number(e) if
                compare_number(n, e) == Some(std::cmp::Ordering::Greater)),
            (Self::Lt, Value::Number(n)) => matches!(expected, Value::Number(e) if
                compare_number(n, e) == Some(std::cmp::Ordering::Less)),
            (Self::Ge, Value::Number(n)) => matches!(expected, Value::Number(e) if
                matches!(compare_number(n, e),
                    Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal))),
            (Self::Le, Value::Number(n)) => matches!(expected, Value::Number(e) if
                matches!(compare_number(n, e),
                    Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal))),
            (Self::Eq, Value::Bool(b)) => matches!(expected, Value::Bool(e) if b == e),
            (Self::Ne, Value::Bool(b)) => matches!(expected, Value::Bool(e) if b != e),
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Subscription {
    pub id: String,
    pub filter: SubscriptionFilter,
}

// ── Client info ───────────────────────────────────────────────────────────────

/// One outbound "subscription write" to a client.
///
/// In the legacy protocol, the server sends exactly one frame per subscription diff.
/// In the optional two-frame protocol, the server sends two frames (route + body),
/// but they are sent as a single grouped item to prevent interleaving across threads.
#[derive(Clone, Debug)]
pub enum OutboundFrames {
    One(Arc<Bytes>),
    Two {
        first: Arc<Bytes>,
        second: Arc<Bytes>,
    },
}

struct ClientInfo {
    tx: UnboundedSender<OutboundFrames>,
    subscriptions: DashMap<String, Subscription>,
}

// ── Encoding helpers ──────────────────────────────────────────────────────────

fn encode_server(msg: &ServerMessage) -> Option<Arc<Bytes>> {
    rmp_serde::to_vec(msg)
        .ok()
        .map(|b| Arc::new(Bytes::from(b)))
}

fn encode_legacy_diff(sub_id: &str, delta: &RowDelta) -> Option<Arc<Bytes>> {
    let diff = SubscriptionDiff {
        subscription_id: sub_id.to_string(),
        table_name: delta.table_name.clone(),
        row_key: delta.row_key.clone(),
        operation: delta.operation.clone(),
        row_data: delta.row_data.clone(),
    };
    encode_server(&ServerMessage::SubscriptionDiff(diff))
}

fn encode_legacy_snapshot(
    sub_id: &str,
    table_name: &str,
    row_key: &str,
    row_data: Value,
) -> Option<Arc<Bytes>> {
    let diff = SubscriptionDiff {
        subscription_id: sub_id.to_string(),
        table_name: table_name.to_string(),
        row_key: row_key.to_string(),
        operation: "initial_snapshot".to_string(),
        row_data: Some(row_data),
    };
    encode_server(&ServerMessage::SubscriptionDiff(diff))
}

fn encode_route(subscription_ids: Vec<String>) -> Option<Arc<Bytes>> {
    let route = SubscriptionRoute { subscription_ids };
    encode_server(&ServerMessage::SubscriptionRoute(route))
}

fn encode_body(delta: &RowDelta) -> Option<Arc<Bytes>> {
    let body = SubscriptionBody {
        table_name: delta.table_name.clone(),
        row_key: delta.row_key.clone(),
        operation: delta.operation.clone(),
        row_data: delta.row_data.clone(),
    };
    encode_server(&ServerMessage::SubscriptionBody(body))
}

fn encode_snapshot_body(
    table_name: &str,
    row_key: &str,
    row_data: Value,
) -> Option<Arc<Bytes>> {
    let body = SubscriptionBody {
        table_name: table_name.to_string(),
        row_key: row_key.to_string(),
        operation: "initial_snapshot".to_string(),
        row_data: Some(row_data),
    };
    encode_server(&ServerMessage::SubscriptionBody(body))
}

// ── Manager ───────────────────────────────────────────────────────────────────

pub struct SubscriptionManager {
    clients: DashMap<ClientId, Arc<ClientInfo>>,
    table_index: DashMap<String, DashMap<ClientId, Vec<String>>>,
    next_id: AtomicU64,
    two_frame: bool,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        SubscriptionManager::new_with_options(false)
    }

    pub fn new_with_options(two_frame: bool) -> Self {
        SubscriptionManager {
            clients: DashMap::with_capacity_and_shard_amount(256, 16),
            table_index: DashMap::with_capacity_and_shard_amount(32, 8),
            next_id: AtomicU64::new(1),
            two_frame,
        }
    }

    pub fn register_client(&self, tx: UnboundedSender<OutboundFrames>) -> ClientId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.clients.insert(
            id,
            Arc::new(ClientInfo {
                tx,
                subscriptions: DashMap::new(),
            }),
        );
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

        let client = self.clients.get(&client_id).ok_or_else(|| {
            NeonDBError::invalid_argument(format!("Unknown client: {}", client_id))
        })?;

        let subscription = Subscription {
            id: subscription_id.clone(),
            filter: filter.clone(),
        };

        // ── Register in per-client map and reverse index (BEFORE snapshot) ───
        client
            .subscriptions
            .insert(subscription_id.clone(), subscription);

        self.table_index
            .entry(table_name.clone())
            .or_insert_with(|| DashMap::with_capacity_and_shard_amount(64, 4))
            .entry(client_id)
            .or_insert_with(Vec::new)
            .push(subscription_id.clone());

        // ── TODO-003: initial state sync ─────────────────────────────────────
        if let Some(tables) = tables {
            if let Ok(rows) = tables.list_rows_with_keys(&table_name) {
                let tx = client.tx.clone();
                let route_bytes = if self.two_frame {
                    encode_route(vec![subscription_id.clone()])
                } else {
                    None
                };
                for (row_key, row_value) in rows {
                    let synthetic = RowDelta {
                        table_name: table_name.clone(),
                        operation: "initial_snapshot".to_string(),
                        row_key: row_key.clone(),
                        row_id: 0,
                        shard_id: 0,
                        payload_arc: None,
                        row_data: Some(row_value.clone()),
                        counter_add_amount: 0,
                        counter_add_timestamp: 0,
                    };
                    if filter.matches(&synthetic) {
                        if self.two_frame {
                            if let (Some(route), Some(body)) = (
                                route_bytes.clone(),
                                encode_snapshot_body(&table_name, &row_key, row_value),
                            ) {
                                let _ = tx.send(OutboundFrames::Two {
                                    first: route,
                                    second: body,
                                });
                            }
                        } else if let Some(frame) = encode_legacy_snapshot(
                            &subscription_id,
                            &table_name,
                            &row_key,
                            row_value,
                        ) {
                            let _ = tx.send(OutboundFrames::One(frame));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn unsubscribe(&self, client_id: ClientId, subscription_id: &str) -> Result<bool> {
        let client = self.clients.get(&client_id).ok_or_else(|| {
            NeonDBError::invalid_argument(format!("Unknown client: {}", client_id))
        })?;

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
                None => continue,
            };

            if self.two_frame {
                let Some(body) = encode_body(delta) else {
                    continue;
                };

                let mut per_client: HashMap<ClientId, (UnboundedSender<OutboundFrames>, Vec<String>)> =
                    HashMap::new();

                for client_entry in table_entry.iter() {
                    let client_id = *client_entry.key();
                    let sub_ids = client_entry.value();

                    let client = match self.clients.get(&client_id) {
                        Some(c) => c,
                        None => continue,
                    };

                    for sub_id in sub_ids.iter() {
                        let sub = match client.subscriptions.get(sub_id) {
                            Some(s) => s,
                            None => continue,
                        };
                        if sub.filter.matches(delta) {
                            per_client
                                .entry(client_id)
                                .or_insert_with(|| (client.tx.clone(), Vec::new()))
                                .1
                                .push(sub_id.clone());
                        }
                    }
                }

                for (_cid, (tx, sub_ids)) in per_client {
                    if let Some(route) = encode_route(sub_ids) {
                        let _ = tx.send(OutboundFrames::Two {
                            first: route,
                            second: body.clone(),
                        });
                    }
                }
            } else {
                let mut matching: Vec<(UnboundedSender<OutboundFrames>, String)> = Vec::new();

                for client_entry in table_entry.iter() {
                    let client_id = *client_entry.key();
                    let sub_ids = client_entry.value();

                    let client = match self.clients.get(&client_id) {
                        Some(c) => c,
                        None => continue,
                    };

                    for sub_id in sub_ids.iter() {
                        let sub = match client.subscriptions.get(sub_id) {
                            Some(s) => s,
                            None => continue,
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

                    if let Some(frame) = encode_legacy_diff(sub_id, delta) {
                        for (tx, _) in &matching[i..run_end] {
                            let _ = tx.send(OutboundFrames::One(frame.clone()));
                        }
                    }

                    i = run_end;
                }
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
        match &self.predicate {
            None => true,
            Some(p) => p.eval(delta),
        }
    }
}

impl Predicate {
    pub fn eval(&self, delta: &RowDelta) -> bool {
        match self {
            Predicate::Comparison { field, op, value } => {
                let actual = Self::extract_field(delta, field);
                op.compare(actual.as_ref(), value)
            }
            Predicate::In { field, values } => {
                let actual = Self::extract_field(delta, field);
                match actual {
                    None => false,
                    Some(v) => values.contains(&v),
                }
            }
            Predicate::And(left, right) => left.eval(delta) && right.eval(delta),
        }
    }

    fn extract_field(delta: &RowDelta, field: &str) -> Option<Value> {
        if field == "row_key" {
            Some(Value::String(delta.row_key.clone()))
        } else {
            delta.row_data_value()?.get(field).cloned()
        }
    }
}

fn compare_number(l: &serde_json::Number, r: &serde_json::Number) -> Option<std::cmp::Ordering> {
    if let (Some(a), Some(b)) = (l.as_i64(), r.as_i64()) {
        return Some(a.cmp(&b));
    }
    if let (Some(a), Some(b)) = (l.as_u64(), r.as_u64()) {
        return Some(a.cmp(&b));
    }
    if let (Some(a), Some(b)) = (l.as_f64(), r.as_f64()) {
        return a.partial_cmp(&b);
    }
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
    let predicate = predicate.map(|p| parse_predicate(p.trim())).transpose()?;
    Ok(SubscriptionFilter {
        table_name: table_name.to_string(),
        predicate,
    })
}

/// Parse a WHERE clause into a (possibly compound) Predicate tree.
///
/// Grammar (simplified):
///   predicate  = comparison | in_expr | predicate AND predicate
///   comparison = field op value
///   in_expr    = field IN ( value, ... )
///   op         = = | == | != | <> | > | < | >= | <=
fn parse_predicate(predicate: &str) -> Result<Predicate> {
    // ── 1. Split on AND at depth 0 (parens-aware) ─────────────────────────────
    if let Some((left_str, right_str)) = split_on_and(predicate) {
        let left = parse_predicate(left_str)?;
        let right = parse_predicate(right_str)?;
        return Ok(Predicate::And(Box::new(left), Box::new(right)));
    }

    // ── 2. Detect IN operator ─────────────────────────────────────────────────
    let lower = predicate.to_lowercase();
    if let Some(in_pos) = lower.find(" in ") {
        let field = predicate[..in_pos].trim().to_string();
        let rest = predicate[in_pos + 4..].trim();
        if rest.starts_with('(') && rest.ends_with(')') {
            let inner = &rest[1..rest.len() - 1];
            let values = inner
                .split(',')
                .map(|s| parse_predicate_value(s.trim()))
                .collect::<Result<Vec<_>>>()?;
            return Ok(Predicate::In { field, values });
        }
    }

    // ── 3. Fall back to single comparison ─────────────────────────────────────
    parse_comparison(predicate)
}

fn split_on_and(predicate: &str) -> Option<(&str, &str)> {
    let bytes = predicate.as_bytes();
    let mut depth = 0usize;
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ => {
                if depth == 0 && i + 5 <= bytes.len() {
                    let window = &bytes[i..i + 5];
                    if window.eq_ignore_ascii_case(b" and ") {
                        return Some((predicate[..i].trim(), predicate[i + 5..].trim()));
                    }
                }
            }
        }
        i += 1;
    }
    None
}

/// Parse a single comparison clause (`field op value`).
///
/// Session 27 fix: operators are now tried longest-first so that `>=` is
/// matched before `>`, and `=` (single equals, SQL-style) is tried last so
/// it doesn't accidentally consume the `=` in `>=` or `<=`.
fn parse_comparison(predicate: &str) -> Result<Predicate> {
    // Try multi-char operators first (longest-match), then single `=`.
    for op in [">=", "<=", "==", "!=", "<>", ">", "<", "="] {
        if let Some(idx) = predicate.find(op) {
            // For single `=`, skip if it's actually part of `>=`, `<=`, `==`, `!=`.
            if op == "=" {
                let before = predicate.as_bytes().get(idx.wrapping_sub(1)).copied();
                let after  = predicate.as_bytes().get(idx + 1).copied();
                if matches!(before, Some(b'>' | b'<' | b'!' | b'='))
                    || matches!(after, Some(b'='))
                {
                    continue;
                }
            }
            let field = predicate[..idx].trim().to_string();
            let value_part = predicate[idx + op.len()..].trim();
            let cmp_op = ComparisonOp::from_str(op)
                .ok_or_else(|| NeonDBError::invalid_argument("Unsupported comparator"))?;
            let value = parse_predicate_value(value_part)?;
            return Ok(Predicate::Comparison { field, op: cmp_op, value });
        }
    }
    Err(NeonDBError::invalid_argument(
        "Subscription predicate invalid",
    ))
}

fn parse_predicate_value(value: &str) -> Result<Value> {
    let t = value.trim();
    if (t.starts_with('"') && t.ends_with('"')) || (t.starts_with('\'') && t.ends_with('\'')) {
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
    if t.eq_ignore_ascii_case("true") {
        return Ok(Value::Bool(true));
    }
    if t.eq_ignore_ascii_case("false") {
        return Ok(Value::Bool(false));
    }
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
            counter_add_amount: 0,
            counter_add_timestamp: 0,
        }
    }

    fn recv_one(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<OutboundFrames>,
    ) -> Arc<Bytes> {
        match rx.try_recv().expect("expected a frame") {
            OutboundFrames::One(b) => b,
            OutboundFrames::Two { .. } => panic!("expected legacy single-frame message"),
        }
    }

    // ── Existing tests ────────────────────────────────────────────────────────

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

    // ── Session 27: single `=` operator ──────────────────────────────────────

    #[test]
    fn single_equals_operator_parses_correctly() {
        // This is what template next-steps and CLI users naturally write.
        let f = parse_subscription_query("messages WHERE room_id = 'general'").unwrap();
        assert_eq!(f.table_name, "messages");
        match f.predicate.unwrap() {
            Predicate::Comparison { field, op, value } => {
                assert_eq!(field, "room_id");
                assert!(matches!(op, ComparisonOp::Eq));
                assert_eq!(value, Value::String("general".to_string()));
            }
            other => panic!("expected Comparison, got {:?}", other),
        }
    }

    #[test]
    fn single_equals_matches_string_delta() {
        let f = parse_subscription_query("messages WHERE room_id = 'general'").unwrap();
        let matching = make_delta("messages", "m1", serde_json::json!({"room_id": "general", "text": "hi"}));
        let non_matching = make_delta("messages", "m2", serde_json::json!({"room_id": "private", "text": "hi"}));
        assert!(f.matches(&matching),     "room_id='general' should match");
        assert!(!f.matches(&non_matching), "room_id='private' should not match");
    }

    #[test]
    fn single_equals_does_not_break_gte_lte() {
        // >= and <= must still work correctly after the `=` fallback was added.
        let f_gte = parse_subscription_query("scores WHERE value >= 10").unwrap();
        let f_lte = parse_subscription_query("scores WHERE value <= 5").unwrap();
        let d_10 = make_delta("scores", "s1", serde_json::json!({"value": 10}));
        let d_5  = make_delta("scores", "s2", serde_json::json!({"value": 5}));
        assert!(f_gte.matches(&d_10), ">= 10 should match value=10");
        assert!(f_lte.matches(&d_5),  "<= 5 should match value=5");
    }

    #[test]
    fn players_zone_single_equals_watch_query() {
        // Matches the template next-steps hint exactly.
        let f = parse_subscription_query("players WHERE zone = 'zone_0_0'").unwrap();
        let hit  = make_delta("players", "alice", serde_json::json!({"zone": "zone_0_0", "x": 0, "y": 0}));
        let miss = make_delta("players", "bob",   serde_json::json!({"zone": "zone_1_0", "x": 10, "y": 0}));
        assert!(f.matches(&hit),  "zone_0_0 should match");
        assert!(!f.matches(&miss), "zone_1_0 should not match");
    }

    #[test]
    fn games_id_single_equals_watch_query() {
        let f = parse_subscription_query("games WHERE id = 'game1'").unwrap();
        let hit  = make_delta("games", "game1", serde_json::json!({"id": "game1", "status": "active"}));
        let miss = make_delta("games", "game2", serde_json::json!({"id": "game2", "status": "active"}));
        assert!(f.matches(&hit));
        assert!(!f.matches(&miss));
    }

    // ── All previous tests preserved below ───────────────────────────────────

    #[test]
    fn publish_deltas_arc_clone_count() {
        let mgr = SubscriptionManager::new();
        let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
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
        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let id1 = mgr.register_client(tx1);
        let id2 = mgr.register_client(tx2);
        mgr.subscribe(id1, "world_sync".to_string(), "players".to_string()).unwrap();
        mgr.subscribe(id2, "world_sync".to_string(), "players".to_string()).unwrap();
        let deltas = vec![make_delta("players", "hero_1", serde_json::json!({"hp": 100}))];
        mgr.publish_deltas(&deltas);
        let frame1 = recv_one(&mut rx1);
        let frame2 = recv_one(&mut rx2);
        assert_eq!(Arc::as_ptr(&frame1), Arc::as_ptr(&frame2),
            "shared sub_id must share the same Arc<Bytes> allocation");
    }

    #[test]
    fn publish_deltas_unique_sub_ids_receive_correct_data() {
        let mgr = SubscriptionManager::new();
        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let id1 = mgr.register_client(tx1);
        let id2 = mgr.register_client(tx2);
        mgr.subscribe(id1, "sub_client_1".to_string(), "counters".to_string()).unwrap();
        mgr.subscribe(id2, "sub_client_2".to_string(), "counters".to_string()).unwrap();
        let deltas = vec![make_delta("counters", "score", serde_json::json!({"value": 42}))];
        mgr.publish_deltas(&deltas);
        let frame1 = recv_one(&mut rx1);
        let frame2 = recv_one(&mut rx2);
        use crate::network::message::ServerMessage;
        let msg1: ServerMessage = rmp_serde::from_slice(&frame1).unwrap();
        let msg2: ServerMessage = rmp_serde::from_slice(&frame2).unwrap();
        match msg1 { ServerMessage::SubscriptionDiff(d) => assert_eq!(d.subscription_id, "sub_client_1"), _ => panic!() }
        match msg2 { ServerMessage::SubscriptionDiff(d) => assert_eq!(d.subscription_id, "sub_client_2"), _ => panic!() }
    }

    #[test]
    fn two_frame_protocol_groups_route_and_body() {
        let mgr = SubscriptionManager::new_with_options(true);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "s".to_string(), "players".to_string()).unwrap();
        let deltas = vec![make_delta("players", "p1", serde_json::json!({"hp": 99}))];
        mgr.publish_deltas(&deltas);
        let frames = rx.try_recv().expect("expected outbound frames");
        match frames {
            OutboundFrames::Two { first, second } => {
                let route: crate::network::message::ServerMessage = rmp_serde::from_slice(&first).unwrap();
                let body: crate::network::message::ServerMessage = rmp_serde::from_slice(&second).unwrap();
                match route { crate::network::message::ServerMessage::SubscriptionRoute(r) => assert_eq!(r.subscription_ids, vec!["s".to_string()]), _ => panic!() }
                match body { crate::network::message::ServerMessage::SubscriptionBody(b) => { assert_eq!(b.table_name, "players"); assert_eq!(b.row_key, "p1"); } _ => panic!() }
            }
            other => panic!("expected two-frame outbound, got {:?}", other),
        }
    }

    #[test]
    fn publish_deltas_predicate_filters_correctly() {
        let mgr = SubscriptionManager::new();
        let (tx_match, mut rx_match) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let (tx_skip, mut rx_skip) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let id_match = mgr.register_client(tx_match);
        let id_skip = mgr.register_client(tx_skip);
        mgr.subscribe(id_match, "high".to_string(), "counters WHERE value >= 10".to_string()).unwrap();
        mgr.subscribe(id_skip, "low".to_string(), "counters WHERE value >= 100".to_string()).unwrap();
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
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "s".to_string(), "players".to_string()).unwrap();
        let deltas = vec![make_delta("counters", "k", serde_json::json!({"v": 1}))];
        mgr.publish_deltas(&deltas);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn reverse_index_skips_unrelated_table_entirely() {
        let mgr = SubscriptionManager::new();
        let (tx_a, mut rx_a) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let (tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let id_a = mgr.register_client(tx_a);
        let id_b = mgr.register_client(tx_b);
        mgr.subscribe(id_a, "sa".to_string(), "table_alpha".to_string()).unwrap();
        mgr.subscribe(id_b, "sb".to_string(), "table_beta".to_string()).unwrap();
        let deltas = vec![make_delta("table_alpha", "k1", serde_json::json!({"x": 1}))];
        mgr.publish_deltas(&deltas);
        assert!(rx_a.try_recv().is_ok(), "table_alpha subscriber must receive");
        assert!(rx_b.try_recv().is_err(), "table_beta subscriber must NOT receive");
    }

    #[test]
    fn reverse_index_cleaned_up_on_unsubscribe() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "sub1".to_string(), "counters".to_string()).unwrap();
        mgr.unsubscribe(cid, "sub1").unwrap();
        assert!(mgr.table_index.get("counters").is_none() || mgr.table_index.get("counters").map(|m| m.is_empty()).unwrap_or(true));
        let deltas = vec![make_delta("counters", "k", serde_json::json!({"v": 99}))];
        mgr.publish_deltas(&deltas);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn reverse_index_cleaned_up_on_unregister() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "s1".to_string(), "players".to_string()).unwrap();
        mgr.subscribe(cid, "s2".to_string(), "counters".to_string()).unwrap();
        mgr.unregister_client(cid);
        for table in &["players", "counters"] {
            let absent = mgr.table_index.get(*table).map(|m| m.get(&cid).is_none()).unwrap_or(true);
            assert!(absent);
        }
        let deltas = vec![make_delta("players", "hero", serde_json::json!({"hp": 50})), make_delta("counters", "score", serde_json::json!({"value": 1}))];
        mgr.publish_deltas(&deltas);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn reverse_index_correct_delivery_at_scale() {
        let mgr = SubscriptionManager::new();
        let mut rxs_players = Vec::new();
        let mut rxs_counters = Vec::new();
        for i in 0..25 {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
            let cid = mgr.register_client(tx);
            mgr.subscribe(cid, format!("ps_{}", i), "players".to_string()).unwrap();
            rxs_players.push(rx);
        }
        for i in 0..25 {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
            let cid = mgr.register_client(tx);
            mgr.subscribe(cid, format!("cs_{}", i), "counters".to_string()).unwrap();
            rxs_counters.push(rx);
        }
        let deltas = vec![make_delta("players", "p1", serde_json::json!({"hp": 100}))];
        mgr.publish_deltas(&deltas);
        for (i, rx) in rxs_players.iter_mut().enumerate() { assert!(rx.try_recv().is_ok(), "players subscriber {} must receive", i); }
        for (i, rx) in rxs_counters.iter_mut().enumerate() { assert!(rx.try_recv().is_err(), "counters subscriber {} must NOT receive", i); }
    }

    #[test]
    fn client_with_multi_table_subscriptions() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "watch_players".to_string(), "players".to_string()).unwrap();
        mgr.subscribe(cid, "watch_counters".to_string(), "counters".to_string()).unwrap();
        let deltas = vec![make_delta("players", "hero", serde_json::json!({"hp": 75}))];
        mgr.publish_deltas(&deltas);
        let frame = recv_one(&mut rx);
        let msg: crate::network::message::ServerMessage = rmp_serde::from_slice(&frame).unwrap();
        match msg { crate::network::message::ServerMessage::SubscriptionDiff(d) => { assert_eq!(d.subscription_id, "watch_players"); assert_eq!(d.table_name, "players"); } _ => panic!() }
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn initial_snapshot_delivered_on_subscribe() {
        let tables = Arc::new(TableStore::new());
        tables.set_counter("alpha".to_string(), 10, 0).unwrap();
        tables.set_counter("beta".to_string(), 20, 0).unwrap();
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let cid = mgr.register_client(tx);
        mgr.subscribe_with_snapshot(cid, "snap_all".to_string(), "counters".to_string(), Some(&tables)).unwrap();
        let mut received = 0;
        while let Ok(frames) = rx.try_recv() {
            let frame = match frames { OutboundFrames::One(b) => b, OutboundFrames::Two { .. } => panic!() };
            let msg: crate::network::message::ServerMessage = rmp_serde::from_slice(&frame).unwrap();
            match msg { crate::network::message::ServerMessage::SubscriptionDiff(d) => { assert_eq!(d.subscription_id, "snap_all"); assert_eq!(d.operation, "initial_snapshot"); assert_eq!(d.table_name, "counters"); received += 1; } _ => panic!() }
        }
        assert_eq!(received, 2, "expected 2 snapshot frames, got {}", received);
    }

    #[test]
    fn initial_snapshot_respects_predicate() {
        let tables = Arc::new(TableStore::new());
        tables.set_counter("low".to_string(), 5, 0).unwrap();
        tables.set_counter("high".to_string(), 50, 0).unwrap();
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let cid = mgr.register_client(tx);
        mgr.subscribe_with_snapshot(cid, "snap_high".to_string(), "counters WHERE value > 10".to_string(), Some(&tables)).unwrap();
        let mut received = 0;
        while let Ok(_) = rx.try_recv() { received += 1; }
        assert_eq!(received, 1, "expected 1 snapshot frame (only 'high' matches), got {}", received);
    }

    #[test]
    fn subscribe_without_tables_sends_no_snapshot() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "no_snap".to_string(), "counters".to_string()).unwrap();
        assert!(rx.try_recv().is_err(), "no snapshot should be sent without tables");
    }

    #[test]
    fn predicate_in_matches_member_value() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "s".to_string(), "players WHERE status IN ('active', 'pending')".to_string()).unwrap();
        let d = make_delta("players", "p1", serde_json::json!({"status": "active"}));
        mgr.publish_deltas(&[d]);
        assert!(rx.try_recv().is_ok(), "IN match should deliver");
        let d2 = make_delta("players", "p2", serde_json::json!({"status": "banned"}));
        mgr.publish_deltas(&[d2]);
        assert!(rx.try_recv().is_err(), "IN non-match should be filtered");
    }

    #[test]
    fn predicate_in_with_numbers() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "s".to_string(), "players WHERE level IN (1, 5, 10)".to_string()).unwrap();
        let match_delta = make_delta("players", "p1", serde_json::json!({"level": 5}));
        mgr.publish_deltas(&[match_delta]);
        assert!(rx.try_recv().is_ok(), "level=5 should match IN (1,5,10)");
        let no_match = make_delta("players", "p2", serde_json::json!({"level": 7}));
        mgr.publish_deltas(&[no_match]);
        assert!(rx.try_recv().is_err(), "level=7 should not match IN (1,5,10)");
    }

    #[test]
    fn predicate_and_both_must_match() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "s".to_string(), "players WHERE score > 100 AND level > 5".to_string()).unwrap();
        let both = make_delta("players", "p1", serde_json::json!({"score": 200, "level": 10}));
        mgr.publish_deltas(&[both]);
        assert!(rx.try_recv().is_ok(), "both conditions met should deliver");
        let only_left = make_delta("players", "p2", serde_json::json!({"score": 200, "level": 3}));
        mgr.publish_deltas(&[only_left]);
        assert!(rx.try_recv().is_err(), "only left condition should be filtered");
        let only_right = make_delta("players", "p3", serde_json::json!({"score": 50, "level": 10}));
        mgr.publish_deltas(&[only_right]);
        assert!(rx.try_recv().is_err(), "only right condition should be filtered");
    }

    #[test]
    fn predicate_in_and_comparison_combined() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<OutboundFrames>();
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "s".to_string(), "players WHERE status IN ('active', 'vip') AND score >= 50".to_string()).unwrap();
        let hit = make_delta("players", "p1", serde_json::json!({"status": "active", "score": 100}));
        mgr.publish_deltas(&[hit]);
        assert!(rx.try_recv().is_ok());
        let low_score = make_delta("players", "p2", serde_json::json!({"status": "vip", "score": 10}));
        mgr.publish_deltas(&[low_score]);
        assert!(rx.try_recv().is_err());
        let banned = make_delta("players", "p3", serde_json::json!({"status": "banned", "score": 200}));
        mgr.publish_deltas(&[banned]);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn parse_in_predicate_returns_correct_values() {
        let filter = parse_subscription_query("items WHERE rarity IN ('common', 'rare', 'epic')").unwrap();
        assert_eq!(filter.table_name, "items");
        match filter.predicate.unwrap() {
            Predicate::In { field, values } => {
                assert_eq!(field, "rarity");
                assert_eq!(values.len(), 3);
                assert!(values.contains(&Value::String("common".to_string())));
                assert!(values.contains(&Value::String("rare".to_string())));
                assert!(values.contains(&Value::String("epic".to_string())));
            }
            other => panic!("Expected In predicate, got {:?}", other),
        }
    }

    #[test]
    fn parse_and_predicate_returns_compound() {
        let filter = parse_subscription_query("players WHERE score > 100 AND level > 5").unwrap();
        assert_eq!(filter.table_name, "players");
        match filter.predicate.unwrap() {
            Predicate::And(left, right) => {
                match *left { Predicate::Comparison { field, .. } => assert_eq!(field, "score"), _ => panic!() }
                match *right { Predicate::Comparison { field, .. } => assert_eq!(field, "level"), _ => panic!() }
            }
            other => panic!("Expected And predicate, got {:?}", other),
        }
    }
}
