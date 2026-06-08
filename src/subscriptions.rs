// ============================================================================
// SubscriptionManager — reverse-index + encode-once rewrite
//
// Session 7  — TODO-003: Initial state sync on subscribe
// Session 5  — Reverse index (O(matching) not O(all))
// Session 4  — encode-once fan-out + CPU-aware DashMap shards
// Session 27 — `=` (single equals) accepted in addition to `==`
// Session 30 — TODO-020 partial: OR predicate + LIMIT N on initial snapshot
// Session 31 — TODO-020 partial: ORDER BY field ASC|DESC on initial snapshot
//
//   OR: `WHERE status = 'active' OR level > 10`
//     - Full recursive OR support at any nesting depth.
//     - OR is lower-priority than AND: `A AND B OR C` parses as `(A AND B) OR C`.
//
//   LIMIT N: `players WHERE zone = 'zone_0_0' LIMIT 100`
//     - Caps the number of rows delivered in the INITIAL SNAPSHOT only.
//     - Live diffs are never limited.
//     - Appears at the end of the query, after the ORDER BY clause if present.
//
//   ORDER BY field ASC|DESC: `players ORDER BY score DESC LIMIT 10`
//     - Sorts the INITIAL SNAPSHOT rows before delivery and before LIMIT.
//     - Direction defaults to ASC when omitted.
//     - Live diffs are never reordered (ORDER BY is a snapshot hint).
//     - ORDER BY comes after WHERE, before LIMIT (SQL-compatible placement).
//     - Numbers compared numerically; strings compared lexicographically;
//       missing field sorts last in both directions.
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
use tokio::sync::mpsc::Sender;

pub type ClientId = u64;

// ── OrderBy ───────────────────────────────────────────────────────────────────

/// Sort direction for ORDER BY.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// An ORDER BY clause: sort the initial snapshot by `field` in `direction`.
#[derive(Clone, Debug)]
pub struct OrderBy {
    pub field: String,
    pub direction: SortDirection,
}

// ── Predicate ─────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct SubscriptionFilter {
    pub table_name: String,
    pub predicate: Option<Predicate>,
    /// Optional sort order for the initial snapshot delivery.
    /// Has no effect on live delta delivery.
    pub order_by: Option<OrderBy>,
    /// Optional cap on the number of rows delivered in the initial snapshot.
    /// Applied AFTER ORDER BY sorting. Has no effect on live delta delivery.
    pub limit: Option<usize>,
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
    /// Logical OR of two sub-predicates: `left OR right`
    Or(Box<Predicate>, Box<Predicate>),
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
            "==" | "=" => Some(Self::Eq),
            "!=" | "<>" => Some(Self::Ne),
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

#[derive(Clone, Debug)]
pub enum OutboundFrames {
    One(Arc<Bytes>),
    Two {
        first: Arc<Bytes>,
        second: Arc<Bytes>,
    },
}

struct ClientInfo {
    tx: Sender<OutboundFrames>,
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

    pub fn register_client(&self, tx: Sender<OutboundFrames>) -> ClientId {
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
        let limit = filter.limit;
        let order_by = filter.order_by.clone();

        let client = self.clients.get(&client_id).ok_or_else(|| {
            NeonDBError::invalid_argument(format!("Unknown client: {}", client_id))
        })?;

        let subscription = Subscription {
            id: subscription_id.clone(),
            filter: filter.clone(),
        };

        // Register in per-client map and reverse index BEFORE snapshot.
        client
            .subscriptions
            .insert(subscription_id.clone(), subscription);

        self.table_index
            .entry(table_name.clone())
            .or_insert_with(|| DashMap::with_capacity_and_shard_amount(64, 4))
            .entry(client_id)
            .or_insert_with(Vec::new)
            .push(subscription_id.clone());

        // TODO-003 + LIMIT + ORDER BY: initial state sync.
        if let Some(tables) = tables {
            if let Ok(rows) = tables.list_rows_with_keys(&table_name) {
                let tx = client.tx.clone();

                // Collect matching rows first, so we can sort before delivery.
                let mut matching_rows: Vec<(String, Value)> = rows
                    .into_iter()
                    .filter(|(row_key, row_value)| {
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
                        filter.matches(&synthetic)
                    })
                    .collect();

                // ORDER BY: sort matching rows before LIMIT truncation.
                if let Some(ref ob) = order_by {
                    let field = ob.field.clone();
                    let desc = ob.direction == SortDirection::Desc;
                    matching_rows.sort_by(|(_, a_val), (_, b_val)| {
                        let a_field = a_val.get(&field);
                        let b_field = b_val.get(&field);
                        let ord = compare_values(a_field, b_field);
                        if desc { ord.reverse() } else { ord }
                    });
                }

                // LIMIT: truncate after sorting.
                let iter: Box<dyn Iterator<Item = (String, Value)>> = if let Some(cap) = limit {
                    Box::new(matching_rows.into_iter().take(cap))
                } else {
                    Box::new(matching_rows.into_iter())
                };

                let route_bytes = if self.two_frame {
                    encode_route(vec![subscription_id.clone()])
                } else {
                    None
                };

                for (row_key, row_value) in iter {
                    if self.two_frame {
                        if let (Some(route), Some(body)) = (
                            route_bytes.clone(),
                            encode_snapshot_body(&table_name, &row_key, row_value),
                        ) {
                            if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(OutboundFrames::Two {
                                first: route,
                                second: body,
                            }) {
                                log::warn!("Client send buffer full during snapshot delivery, truncating");
                                break;
                            }
                        }
                    } else if let Some(frame) = encode_legacy_snapshot(
                        &subscription_id,
                        &table_name,
                        &row_key,
                        row_value,
                    ) {
                        if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(OutboundFrames::One(frame)) {
                            log::warn!("Client send buffer full during snapshot delivery, truncating");
                            break;
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

                let mut per_client: HashMap<ClientId, (Sender<OutboundFrames>, Vec<String>)> =
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

                let mut clients_to_remove: Vec<ClientId> = Vec::new();
                for (cid, (tx, sub_ids)) in &per_client {
                    if let Some(route) = encode_route(sub_ids.clone()) {
                        if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(OutboundFrames::Two {
                            first: route,
                            second: body.clone(),
                        }) {
                            log::warn!("Subscription send buffer full for client {}, removing subscriptions", cid);
                            clients_to_remove.push(*cid);
                        }
                    }
                }
                for cid in clients_to_remove {
                    self.unregister_client(cid);
                }
            } else {
                let mut matching: Vec<(ClientId, Sender<OutboundFrames>, String)> = Vec::new();

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
                            matching.push((client_id, client.tx.clone(), sub_id.clone()));
                        }
                    }
                }

                if matching.is_empty() {
                    continue;
                }

                matching.sort_unstable_by(|a, b| a.2.cmp(&b.2));

                let mut clients_to_remove: Vec<ClientId> = Vec::new();
                let mut i = 0;
                while i < matching.len() {
                    let sub_id = &matching[i].2;

                    let run_end = matching[i..]
                        .iter()
                        .position(|(_, _, sid)| sid != sub_id)
                        .map(|p| i + p)
                        .unwrap_or(matching.len());

                    if let Some(frame) = encode_legacy_diff(sub_id, delta) {
                        for (cid, tx, _) in &matching[i..run_end] {
                            if let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(OutboundFrames::One(frame.clone())) {
                                log::warn!("Subscription send buffer full for client {}, removing subscriptions", cid);
                                clients_to_remove.push(*cid);
                            }
                        }
                    }

                    i = run_end;
                }
                for cid in clients_to_remove {
                    self.unregister_client(cid);
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
            Predicate::Or(left, right) => left.eval(delta) || right.eval(delta),
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

/// Compare two optional JSON values for ORDER BY sorting.
/// Numbers compared numerically; strings lexicographically; missing field sorts last.
fn compare_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Greater, // missing sorts last
        (Some(_), None) => std::cmp::Ordering::Less,
        (Some(Value::Number(an)), Some(Value::Number(bn))) => {
            compare_number(an, bn).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Some(Value::String(as_)), Some(Value::String(bs))) => as_.cmp(bs),
        (Some(Value::Bool(ab)), Some(Value::Bool(bb))) => ab.cmp(bb),
        // Mixed types: fall back to string representation.
        (Some(av), Some(bv)) => av.to_string().cmp(&bv.to_string()),
    }
}

// ── Query parser ──────────────────────────────────────────────────────────────

fn parse_subscription_query(query: &str) -> Result<SubscriptionFilter> {
    let normalized = query.trim();
    let normalized = if normalized.to_lowercase().starts_with("subscribe ") {
        normalized[10..].trim()
    } else {
        normalized
    };

    // Strip modifiers in order: LIMIT first (trailing), then ORDER BY.
    let (without_limit, limit) = extract_limit(normalized);
    let (without_order_by, order_by) = extract_order_by(without_limit);

    let lower = without_order_by.to_lowercase();
    let (table_name, predicate) = if let Some(idx) = lower.find(" where ") {
        (&without_order_by[..idx], Some(without_order_by[idx + 7..].trim()))
    } else {
        (without_order_by, None)
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
        order_by,
        limit,
    })
}

/// Detects and removes a trailing `LIMIT <n>` clause.
/// Returns `(query_without_limit, Some(n))` or `(original, None)`.
fn extract_limit(query: &str) -> (&str, Option<usize>) {
    let lower = query.to_lowercase();
    if let Some(pos) = lower.rfind(" limit ") {
        let after = query[pos + 7..].trim();
        if let Ok(n) = after.parse::<usize>() {
            return (&query[..pos], Some(n));
        }
    }
    (query, None)
}

/// Detects and removes a trailing `ORDER BY <field> [ASC|DESC]` clause.
/// Returns `(query_without_order_by, Some(OrderBy))` or `(original, None)`.
///
/// Examples:
///   `"players WHERE level > 5 ORDER BY score DESC"` → field="score", Desc
///   `"players ORDER BY name"`                       → field="name",  Asc (default)
fn extract_order_by(query: &str) -> (&str, Option<OrderBy>) {
    let lower = query.to_lowercase();
    if let Some(pos) = lower.rfind(" order by ") {
        let rest = query[pos + 10..].trim();
        // rest is `"field"` or `"field ASC"` or `"field DESC"`
        let mut parts = rest.splitn(2, char::is_whitespace);
        if let Some(field) = parts.next().map(str::trim).filter(|f| !f.is_empty()) {
            let direction = match parts
                .next()
                .map(str::trim)
                .unwrap_or("")
                .to_uppercase()
                .as_str()
            {
                "DESC" => SortDirection::Desc,
                _ => SortDirection::Asc,
            };
            return (
                &query[..pos],
                Some(OrderBy {
                    field: field.to_string(),
                    direction,
                }),
            );
        }
    }
    (query, None)
}

/// Parse a WHERE clause into a (possibly compound) Predicate tree.
///
/// Operator precedence (lowest to highest):
///   OR  →  AND  →  comparison / IN
fn parse_predicate(predicate: &str) -> Result<Predicate> {
    // ── 1. Split on OR (lowest precedence) ────────────────────────────────────
    if let Some((left_str, right_str)) = split_on_keyword(predicate, " or ") {
        let left = parse_predicate(left_str)?;
        let right = parse_predicate(right_str)?;
        return Ok(Predicate::Or(Box::new(left), Box::new(right)));
    }

    // ── 2. Split on AND ────────────────────────────────────────────────────────
    if let Some((left_str, right_str)) = split_on_and(predicate) {
        let left = parse_predicate(left_str)?;
        let right = parse_predicate(right_str)?;
        return Ok(Predicate::And(Box::new(left), Box::new(right)));
    }

    // ── 3. Detect IN operator ─────────────────────────────────────────────────
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

    // ── 4. Single comparison ──────────────────────────────────────────────────
    parse_comparison(predicate)
}

/// Split `predicate` on the first occurrence of `keyword` at paren depth 0.
fn split_on_keyword<'a>(predicate: &'a str, keyword: &str) -> Option<(&'a str, &'a str)> {
    let bytes = predicate.as_bytes();
    let klen = keyword.len();
    let mut depth = 0usize;
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ => {
                if depth == 0 && i + klen <= bytes.len() {
                    let window = &predicate[i..i + klen];
                    if window.to_lowercase() == keyword {
                        return Some((predicate[..i].trim(), predicate[i + klen..].trim()));
                    }
                }
            }
        }
        i += 1;
    }
    None
}

fn split_on_and(predicate: &str) -> Option<(&str, &str)> {
    split_on_keyword(predicate, " and ")
}

/// Parse a single comparison clause (`field op value`).
fn parse_comparison(predicate: &str) -> Result<Predicate> {
    for op in [">=", "<=", "==", "!=", "<>", ">", "<", "="] {
        if let Some(idx) = predicate.find(op) {
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
    use crate::network::websocket::CLIENT_SEND_BUFFER_CAPACITY;
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
        rx: &mut tokio::sync::mpsc::Receiver<OutboundFrames>,
    ) -> Arc<Bytes> {
        match rx.try_recv().expect("expected a frame") {
            OutboundFrames::One(b) => b,
            OutboundFrames::Two { .. } => panic!("expected legacy single-frame message"),
        }
    }

    /// Drain all frames from rx and decode each as SubscriptionDiff, returning row_keys in order.
    fn drain_snapshot_keys(rx: &mut tokio::sync::mpsc::Receiver<OutboundFrames>) -> Vec<String> {
        let mut keys = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            let bytes = match frame {
                OutboundFrames::One(b) => b,
                OutboundFrames::Two { .. } => panic!("expected single-frame"),
            };
            let msg: crate::network::message::ServerMessage = rmp_serde::from_slice(&bytes).unwrap();
            match msg {
                crate::network::message::ServerMessage::SubscriptionDiff(d) => keys.push(d.row_key),
                _ => {}
            }
        }
        keys
    }

    // ── Existing tests ────────────────────────────────────────────────────────

    #[test]
    fn parse_subscription_query_without_predicate() {
        let f = parse_subscription_query("counters").unwrap();
        assert_eq!(f.table_name, "counters");
        assert!(f.predicate.is_none());
        assert!(f.limit.is_none());
        assert!(f.order_by.is_none());
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
        let f_gte = parse_subscription_query("scores WHERE value >= 10").unwrap();
        let f_lte = parse_subscription_query("scores WHERE value <= 5").unwrap();
        let d_10 = make_delta("scores", "s1", serde_json::json!({"value": 10}));
        let d_5  = make_delta("scores", "s2", serde_json::json!({"value": 5}));
        assert!(f_gte.matches(&d_10), ">= 10 should match value=10");
        assert!(f_lte.matches(&d_5),  "<= 5 should match value=5");
    }

    #[test]
    fn players_zone_single_equals_watch_query() {
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

    // ── Session 30: OR predicate ──────────────────────────────────────────────

    #[test]
    fn or_predicate_either_side_matches() {
        let f = parse_subscription_query("players WHERE status = 'alive' OR status = 'respawning'").unwrap();
        let alive = make_delta("players", "p1", serde_json::json!({"status": "alive"}));
        let resp  = make_delta("players", "p2", serde_json::json!({"status": "respawning"}));
        let dead  = make_delta("players", "p3", serde_json::json!({"status": "dead"}));
        assert!(f.matches(&alive), "alive should match");
        assert!(f.matches(&resp),  "respawning should match");
        assert!(!f.matches(&dead), "dead should not match");
    }

    #[test]
    fn or_predicate_parses_to_or_node() {
        let f = parse_subscription_query("players WHERE level < 5 OR level > 90").unwrap();
        match f.predicate.unwrap() {
            Predicate::Or(_, _) => {}
            other => panic!("Expected Or, got {:?}", other),
        }
    }

    #[test]
    fn or_with_number_comparison() {
        let f = parse_subscription_query("scores WHERE value < 10 OR value > 100").unwrap();
        let low  = make_delta("scores", "s1", serde_json::json!({"value": 5}));
        let mid  = make_delta("scores", "s2", serde_json::json!({"value": 50}));
        let high = make_delta("scores", "s3", serde_json::json!({"value": 200}));
        assert!(f.matches(&low),   "value=5 should match OR value < 10");
        assert!(!f.matches(&mid),  "value=50 should not match");
        assert!(f.matches(&high),  "value=200 should match OR value > 100");
    }

    #[test]
    fn and_has_higher_precedence_than_or() {
        let f = parse_subscription_query(
            "players WHERE level > 5 AND level < 20 OR status = 'vip'"
        ).unwrap();
        let and_wins = make_delta("players", "p1", serde_json::json!({"level": 10, "status": "normal"}));
        let or_wins  = make_delta("players", "p2", serde_json::json!({"level": 50, "status": "vip"}));
        let neither  = make_delta("players", "p3", serde_json::json!({"level": 50, "status": "normal"}));
        assert!(f.matches(&and_wins), "AND branch should match");
        assert!(f.matches(&or_wins),  "OR right branch should match");
        assert!(!f.matches(&neither), "Neither branch should not match");
    }

    #[test]
    fn or_delivers_delta_to_subscriber() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let cid = mgr.register_client(tx);
        mgr.subscribe(
            cid,
            "s".to_string(),
            "players WHERE status = 'alive' OR status = 'idle'".to_string(),
        )
        .unwrap();
        let alive = make_delta("players", "p1", serde_json::json!({"status": "alive"}));
        let idle  = make_delta("players", "p2", serde_json::json!({"status": "idle"}));
        let dead  = make_delta("players", "p3", serde_json::json!({"status": "dead"}));
        mgr.publish_deltas(&[alive]);
        assert!(rx.try_recv().is_ok(), "alive should be delivered");
        mgr.publish_deltas(&[idle]);
        assert!(rx.try_recv().is_ok(), "idle should be delivered");
        mgr.publish_deltas(&[dead]);
        assert!(rx.try_recv().is_err(), "dead should be filtered");
    }

    // ── Session 30: LIMIT N on initial snapshot ───────────────────────────────

    #[test]
    fn limit_parses_correctly() {
        let f = parse_subscription_query("players LIMIT 10").unwrap();
        assert_eq!(f.table_name, "players");
        assert!(f.predicate.is_none());
        assert_eq!(f.limit, Some(10));
    }

    #[test]
    fn limit_with_where_parses_correctly() {
        let f = parse_subscription_query("players WHERE zone = 'z1' LIMIT 50").unwrap();
        assert_eq!(f.table_name, "players");
        assert!(f.predicate.is_some());
        assert_eq!(f.limit, Some(50));
    }

    #[test]
    fn limit_caps_initial_snapshot() {
        let tables = Arc::new(TableStore::new());
        for i in 0..10usize {
            tables.set_counter(format!("c{}", i), i as i32, 0).unwrap();
        }
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let cid = mgr.register_client(tx);
        mgr.subscribe_with_snapshot(
            cid,
            "snap_limited".to_string(),
            "counters LIMIT 3".to_string(),
            Some(&tables),
        )
        .unwrap();
        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 3, "LIMIT 3 should cap snapshot at 3 rows, got {}", count);
    }

    #[test]
    fn limit_does_not_affect_live_deltas() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let cid = mgr.register_client(tx);
        mgr.subscribe(
            cid,
            "s".to_string(),
            "counters LIMIT 1".to_string(),
        )
        .unwrap();
        for i in 0..5 {
            let d = make_delta("counters", &format!("k{}", i), serde_json::json!({"value": i}));
            mgr.publish_deltas(&[d]);
        }
        let mut received = 0;
        while rx.try_recv().is_ok() {
            received += 1;
        }
        assert_eq!(received, 5, "LIMIT should not filter live deltas; got {}", received);
    }

    #[test]
    fn limit_zero_delivers_no_snapshot_rows() {
        let tables = Arc::new(TableStore::new());
        tables.set_counter("x".to_string(), 1, 0).unwrap();
        tables.set_counter("y".to_string(), 2, 0).unwrap();
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let cid = mgr.register_client(tx);
        mgr.subscribe_with_snapshot(
            cid,
            "s".to_string(),
            "counters LIMIT 0".to_string(),
            Some(&tables),
        )
        .unwrap();
        assert!(rx.try_recv().is_err(), "LIMIT 0 should deliver no snapshot rows");
    }

    // ── Session 31: ORDER BY field ASC|DESC ──────────────────────────────────

    #[test]
    fn order_by_parses_desc() {
        let f = parse_subscription_query("players ORDER BY score DESC").unwrap();
        assert_eq!(f.table_name, "players");
        assert!(f.predicate.is_none());
        let ob = f.order_by.expect("should have ORDER BY");
        assert_eq!(ob.field, "score");
        assert_eq!(ob.direction, SortDirection::Desc);
    }

    #[test]
    fn order_by_parses_asc_default() {
        let f = parse_subscription_query("players ORDER BY name").unwrap();
        let ob = f.order_by.expect("should have ORDER BY");
        assert_eq!(ob.field, "name");
        assert_eq!(ob.direction, SortDirection::Asc);
    }

    #[test]
    fn order_by_with_where_and_limit() {
        let f = parse_subscription_query(
            "scores WHERE value > 0 ORDER BY value DESC LIMIT 5"
        ).unwrap();
        assert_eq!(f.table_name, "scores");
        assert!(f.predicate.is_some());
        assert_eq!(f.limit, Some(5));
        let ob = f.order_by.expect("should have ORDER BY");
        assert_eq!(ob.field, "value");
        assert_eq!(ob.direction, SortDirection::Desc);
    }

    #[test]
    fn order_by_desc_sorts_snapshot_numeric() {
        let tables = Arc::new(TableStore::new());
        // Insert scores: p_a=10, p_b=30, p_c=20
        tables.set_row(
            "scores".to_string(), "p_a".to_string(),
            serde_json::json!({"score": 10}),
        ).unwrap();
        tables.set_row(
            "scores".to_string(), "p_b".to_string(),
            serde_json::json!({"score": 30}),
        ).unwrap();
        tables.set_row(
            "scores".to_string(), "p_c".to_string(),
            serde_json::json!({"score": 20}),
        ).unwrap();

        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let cid = mgr.register_client(tx);
        mgr.subscribe_with_snapshot(
            cid,
            "s".to_string(),
            "scores ORDER BY score DESC".to_string(),
            Some(&tables),
        )
        .unwrap();

        let keys = drain_snapshot_keys(&mut rx);
        // Should arrive in descending score order: p_b(30), p_c(20), p_a(10)
        assert_eq!(keys, vec!["p_b", "p_c", "p_a"],
            "ORDER BY score DESC should sort highest first; got {:?}", keys);
    }

    #[test]
    fn order_by_asc_sorts_snapshot_numeric() {
        let tables = Arc::new(TableStore::new());
        tables.set_row("scores".to_string(), "p_a".to_string(), serde_json::json!({"score": 10})).unwrap();
        tables.set_row("scores".to_string(), "p_b".to_string(), serde_json::json!({"score": 30})).unwrap();
        tables.set_row("scores".to_string(), "p_c".to_string(), serde_json::json!({"score": 20})).unwrap();

        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let cid = mgr.register_client(tx);
        mgr.subscribe_with_snapshot(
            cid,
            "s".to_string(),
            "scores ORDER BY score ASC".to_string(),
            Some(&tables),
        )
        .unwrap();

        let keys = drain_snapshot_keys(&mut rx);
        assert_eq!(keys, vec!["p_a", "p_c", "p_b"],
            "ORDER BY score ASC should sort lowest first; got {:?}", keys);
    }

    #[test]
    fn order_by_desc_combined_with_limit() {
        let tables = Arc::new(TableStore::new());
        // Insert 5 rows with scores 10..50
        for i in 1..=5usize {
            tables.set_row(
                "scores".to_string(),
                format!("p{}", i),
                serde_json::json!({"score": i * 10}),
            ).unwrap();
        }

        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let cid = mgr.register_client(tx);
        // Top 3 scores descending
        mgr.subscribe_with_snapshot(
            cid,
            "s".to_string(),
            "scores ORDER BY score DESC LIMIT 3".to_string(),
            Some(&tables),
        )
        .unwrap();

        let keys = drain_snapshot_keys(&mut rx);
        assert_eq!(keys.len(), 3, "LIMIT 3 should deliver exactly 3 rows");
        // First key must be the row with highest score (p5=50)
        assert_eq!(keys[0], "p5", "First row should be p5 (score=50); got {:?}", keys);
        assert_eq!(keys[1], "p4", "Second row should be p4 (score=40); got {:?}", keys);
        assert_eq!(keys[2], "p3", "Third row should be p3 (score=30); got {:?}", keys);
    }

    #[test]
    fn order_by_does_not_affect_live_deltas() {
        // Live deltas must be delivered as they arrive, regardless of ORDER BY.
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let cid = mgr.register_client(tx);
        mgr.subscribe(
            cid,
            "s".to_string(),
            "scores ORDER BY score DESC".to_string(),
        )
        .unwrap();

        // Publish 3 deltas in insertion order; they should all arrive.
        for i in [1i64, 3, 2] {
            let d = make_delta("scores", &format!("p{}", i), serde_json::json!({"score": i * 10}));
            mgr.publish_deltas(&[d]);
        }
        let mut received = 0;
        while rx.try_recv().is_ok() { received += 1; }
        assert_eq!(received, 3, "All 3 live deltas must arrive (ORDER BY doesn't filter)");
    }

    // ── All previous tests preserved below ───────────────────────────────────

    #[test]
    fn publish_deltas_arc_clone_count() {
        let mgr = SubscriptionManager::new();
        let (tx1, _rx1) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let (tx2, _rx2) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
        let (tx1, mut rx1) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let (tx2, mut rx2) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
        let (tx1, mut rx1) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let (tx2, mut rx2) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
        let (tx_match, mut rx_match) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let (tx_skip, mut rx_skip) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "s".to_string(), "players".to_string()).unwrap();
        let deltas = vec![make_delta("counters", "k", serde_json::json!({"v": 1}))];
        mgr.publish_deltas(&deltas);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn reverse_index_skips_unrelated_table_entirely() {
        let mgr = SubscriptionManager::new();
        let (tx_a, mut rx_a) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let (tx_b, mut rx_b) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
            let (tx, rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
            let cid = mgr.register_client(tx);
            mgr.subscribe(cid, format!("ps_{}", i), "players".to_string()).unwrap();
            rxs_players.push(rx);
        }
        for i in 0..25 {
            let (tx, rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let cid = mgr.register_client(tx);
        mgr.subscribe_with_snapshot(cid, "snap_high".to_string(), "counters WHERE value > 10".to_string(), Some(&tables)).unwrap();
        let mut received = 0;
        while let Ok(_) = rx.try_recv() { received += 1; }
        assert_eq!(received, 1, "expected 1 snapshot frame (only 'high' matches), got {}", received);
    }

    #[test]
    fn subscribe_without_tables_sends_no_snapshot() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
        let cid = mgr.register_client(tx);
        mgr.subscribe(cid, "no_snap".to_string(), "counters".to_string()).unwrap();
        assert!(rx.try_recv().is_err(), "no snapshot should be sent without tables");
    }

    #[test]
    fn predicate_in_matches_member_value() {
        let mgr = SubscriptionManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrames>(CLIENT_SEND_BUFFER_CAPACITY);
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
