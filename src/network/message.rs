use serde::{Deserialize, Serialize};

/// Wire protocol message: Client requests reducer execution
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReducerCall {
    pub call_id: u64,
    pub reducer_name: String,
    pub args: Vec<u8>, // Serialized args (MessagePack)
}

/// Wire protocol message: Server responds to reducer call
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReducerResponse {
    pub call_id: u64,
    pub success: bool,
    pub result: Option<Vec<u8>>, // Serialized result (MessagePack)
    pub error: Option<String>,
}

impl ReducerResponse {
    pub fn success(call_id: u64, result: Vec<u8>) -> Self {
        ReducerResponse {
            call_id,
            success: true,
            result: Some(result),
            error: None,
        }
    }

    pub fn error(call_id: u64, error: String) -> Self {
        ReducerResponse {
            call_id,
            success: false,
            result: None,
            error: Some(error),
        }
    }
}

/// On-demand SQL query from the client.
///
/// The client sends this to run a full SQL statement (SELECT/INSERT/UPDATE/DELETE)
/// directly against the live TableStore, bypassing the reducer pipeline.
/// Results are returned as a `SqlResult` server message with the same `query_id`.
///
/// The SQL engine is fully capable: JOINs, GROUP BY, HAVING, ORDER BY,
/// LIMIT/OFFSET, DISTINCT, aggregates, subqueries, CASE, scalar functions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqlQuery {
    /// Client-assigned ID for correlating the response.
    pub query_id: u64,
    /// The raw SQL string.
    pub sql: String,
}

/// Server response to a `SqlQuery`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqlResult {
    pub query_id: u64,
    pub success: bool,
    /// Column names in the order they appear in each row.
    pub columns: Vec<String>,
    /// Rows as JSON objects (column → value).
    pub rows: Vec<serde_json::Value>,
    /// For INSERT / UPDATE / DELETE: number of rows affected.
    pub rows_affected: usize,
    /// Error message if `success` is false.
    pub error: Option<String>,
}

impl SqlResult {
    pub fn ok(
        query_id: u64,
        columns: Vec<String>,
        rows: Vec<serde_json::Value>,
        rows_affected: usize,
    ) -> Self {
        SqlResult {
            query_id,
            success: true,
            columns,
            rows,
            rows_affected,
            error: None,
        }
    }

    pub fn err(query_id: u64, error: String) -> Self {
        SqlResult {
            query_id,
            success: false,
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            error: Some(error),
        }
    }
}

/// A client command for managing subscriptions, running reducer calls,
/// or executing ad-hoc SQL.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientMessage {
    ReducerCall(ReducerCall),
    Subscribe {
        subscription_id: String,
        query: String,
    },
    Unsubscribe {
        subscription_id: String,
    },
    /// Ad-hoc SQL query — full read/write SQL against the live TableStore.
    SqlQuery(SqlQuery),
    /// Client heartbeat — keeps presence alive.
    Heartbeat,
    /// Client sets their presence status.
    SetPresence {
        status: String,
        metadata: Option<serde_json::Value>,
    },
    /// Client sets a TTL on a row.
    SetTtl {
        table_name: String,
        row_key: String,
        ttl_ms: u64,
    },
    /// Client cancels a TTL on a row.
    CancelTtl {
        table_name: String,
        row_key: String,
    },
    /// Begin a client-side transaction. While a transaction is open on this
    /// connection, subsequent `ReducerCall`s are NOT dispatched to the
    /// reducer queue individually — they are buffered by the WebSocket
    /// handler and executed as one atomic batch against a single
    /// `ReducerContext` when `CommitTransaction` arrives (or discarded on
    /// `RollbackTransaction` / disconnect).
    ///
    /// Same-node only: this is a local (single-process) transaction. There is
    /// no cross-shard/cross-cluster coordination — see docs on
    /// `ServerMessage::TransactionResult` for the actual isolation level
    /// provided.
    BeginTransaction { tx_id: u64 },
    /// Commit the currently open transaction (must match the `tx_id` from
    /// `BeginTransaction`). All buffered reducer calls are executed in the
    /// order received and committed atomically in one lock acquisition.
    CommitTransaction { tx_id: u64 },
    /// Discard the currently open transaction without executing any of its
    /// buffered reducer calls.
    RollbackTransaction { tx_id: u64 },
}

/// A diff for subscribed clients.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubscriptionDiff {
    pub subscription_id: String,
    pub table_name: String,
    pub row_key: String,
    pub operation: String,
    pub row_data: Option<serde_json::Value>,
}

/// Two-frame subscription protocol: the routing header (per client).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubscriptionRoute {
    pub subscription_ids: Vec<String>,
}

/// Two-frame subscription protocol: the shared body (encoded once per delta).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubscriptionBody {
    pub table_name: String,
    pub row_key: String,
    pub operation: String,
    pub row_data: Option<serde_json::Value>,
}

/// Server messages sent back to clients.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServerMessage {
    ReducerResponse(ReducerResponse),
    SubscriptionAck {
        subscription_id: String,
        success: bool,
        message: Option<String>,
    },
    SubscriptionDiff(SubscriptionDiff),
    SubscriptionRoute(SubscriptionRoute),
    SubscriptionBody(SubscriptionBody),
    /// Response to a `ClientMessage::SqlQuery`.
    SqlResult(SqlResult),
    Error {
        message: String,
    },
    /// One frame per tick carrying all matching row diffs for this client.
    /// `payload` is a MsgPack-encoded `Vec<SubscriptionDiff>`.
    /// When `compressed = true`, `payload` is zstd-compressed before MsgPack encoding.
    BatchUpdate {
        compressed: bool,
        payload: Vec<u8>,
    },
    /// Acknowledges a `ClientMessage::BeginTransaction`.
    TransactionBegan { tx_id: u64 },
    /// Result of a `ClientMessage::CommitTransaction` (or an early failure —
    /// e.g. a reducer inside the batch returned an error, which aborts the
    /// whole transaction before any of its writes are committed).
    ///
    /// ## Isolation level — read this before relying on this feature
    ///
    /// This is **atomicity + a single, whole-transaction optimistic-
    /// concurrency check**, not full serializable isolation:
    ///
    /// - **Atomicity**: every reducer call staged in the transaction is
    ///   applied in one `apply_delta_batch_versioned()` call, under the same
    ///   sorted per-row lock acquisition Voltra already uses for a single
    ///   reducer commit. Either all deltas land, or none do.
    /// - **Isolation actually provided: read-committed with first-writer-wins
    ///   conflict detection on rows read by the transaction**, i.e. an OCC
    ///   check equivalent to what a single reducer call already gets: the
    ///   transaction's read-set (every row any of its reducer calls read via
    ///   `ctx.get_row`) is validated against the live row versions at commit
    ///   time. If any row read during the transaction was modified by
    ///   another commit in the meantime, the whole transaction is rejected
    ///   with `VoltraError::TxnConflict` and none of its writes apply.
    /// - **What it is NOT**: reducer calls within the transaction execute
    ///   sequentially against one in-memory `ReducerContext` *before* the
    ///   commit-time lock is ever taken — there is no serializable snapshot
    ///   isolation, no predicate locking, and no protection against
    ///   write-skew anomalies (two transactions each reading disjoint rows
    ///   and writing based on a global invariant that only holds across both
    ///   reads). This matches the isolation level of Voltra's existing
    ///   single-reducer OCC (documented in CLAUDE.md Session 54) — the
    ///   transaction feature extends the SAME guarantee across multiple
    ///   reducer calls rather than introducing a stronger one.
    /// - **Same-node only**: all reducer calls in a transaction run on the
    ///   worker that receives the commit; there is no cross-shard or
    ///   cross-cluster coordination. Do not span a transaction across rows
    ///   that live on different shards.
    TransactionResult {
        tx_id: u64,
        success: bool,
        /// One response per buffered reducer call, in call order. Empty when
        /// `success` is false and the transaction was rejected before any
        /// reducer executed (e.g. unknown tx_id, empty transaction).
        responses: Vec<ReducerResponse>,
        error: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reducer_call_serialization() {
        let call = ReducerCall {
            call_id: 1,
            reducer_name: "increment".to_string(),
            args: vec![1, 2, 3],
        };

        let serialized = rmp_serde::to_vec(&call).unwrap();
        let deserialized: ReducerCall = rmp_serde::from_slice(&serialized).unwrap();

        assert_eq!(deserialized.call_id, 1);
        assert_eq!(deserialized.reducer_name, "increment");
    }

    #[test]
    fn test_response_success() {
        let response = ReducerResponse::success(1, vec![1, 2, 3]);
        assert_eq!(response.success, true);
        assert_eq!(response.result, Some(vec![1, 2, 3]));
        assert_eq!(response.error, None);
    }

    #[test]
    fn test_response_error() {
        let response = ReducerResponse::error(1, "test error".to_string());
        assert_eq!(response.success, false);
        assert_eq!(response.error, Some("test error".to_string()));
    }

    #[test]
    fn test_sql_query_roundtrip() {
        let q = ClientMessage::SqlQuery(SqlQuery {
            query_id: 42,
            sql: "SELECT * FROM players WHERE zone = 'north'".to_string(),
        });
        let bytes = rmp_serde::to_vec(&q).unwrap();
        let q2: ClientMessage = rmp_serde::from_slice(&bytes).unwrap();
        match q2 {
            ClientMessage::SqlQuery(sq) => {
                assert_eq!(sq.query_id, 42);
                assert!(sq.sql.contains("players"));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_sql_result_ok() {
        let r = SqlResult::ok(
            1,
            vec!["id".into(), "score".into()],
            vec![serde_json::json!({"id": "alice", "score": 200})],
            1,
        );
        assert!(r.success);
        assert_eq!(r.columns.len(), 2);
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn test_begin_transaction_roundtrip() {
        let msg = ClientMessage::BeginTransaction { tx_id: 7 };
        let bytes = rmp_serde::to_vec(&msg).unwrap();
        let decoded: ClientMessage = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            ClientMessage::BeginTransaction { tx_id } => assert_eq!(tx_id, 7),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_commit_transaction_roundtrip() {
        let msg = ClientMessage::CommitTransaction { tx_id: 99 };
        let bytes = rmp_serde::to_vec(&msg).unwrap();
        let decoded: ClientMessage = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            ClientMessage::CommitTransaction { tx_id } => assert_eq!(tx_id, 99),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_rollback_transaction_roundtrip() {
        let msg = ClientMessage::RollbackTransaction { tx_id: 5 };
        let bytes = rmp_serde::to_vec(&msg).unwrap();
        let decoded: ClientMessage = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            ClientMessage::RollbackTransaction { tx_id } => assert_eq!(tx_id, 5),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_transaction_result_success_roundtrip() {
        let msg = ServerMessage::TransactionResult {
            tx_id: 3,
            success: true,
            responses: vec![ReducerResponse::success(0, vec![1, 2, 3])],
            error: None,
        };
        let bytes = rmp_serde::to_vec(&msg).unwrap();
        let decoded: ServerMessage = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            ServerMessage::TransactionResult {
                tx_id,
                success,
                responses,
                error,
            } => {
                assert_eq!(tx_id, 3);
                assert!(success);
                assert_eq!(responses.len(), 1);
                assert!(error.is_none());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_transaction_result_failure_roundtrip() {
        let msg = ServerMessage::TransactionResult {
            tx_id: 4,
            success: false,
            responses: vec![],
            error: Some("reducer 'x' failed: boom".to_string()),
        };
        let bytes = rmp_serde::to_vec(&msg).unwrap();
        let decoded: ServerMessage = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            ServerMessage::TransactionResult {
                success, error, ..
            } => {
                assert!(!success);
                assert!(error.unwrap().contains("boom"));
            }
            _ => panic!("wrong variant"),
        }
    }
}
