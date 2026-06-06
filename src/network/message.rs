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
    pub fn ok(query_id: u64, columns: Vec<String>, rows: Vec<serde_json::Value>, rows_affected: usize) -> Self {
        SqlResult { query_id, success: true, columns, rows, rows_affected, error: None }
    }

    pub fn err(query_id: u64, error: String) -> Self {
        SqlResult { query_id, success: false, columns: vec![], rows: vec![], rows_affected: 0, error: Some(error) }
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
    Error { message: String },
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
}
