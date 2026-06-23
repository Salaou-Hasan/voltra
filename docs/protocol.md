# Protocol Reference

---

## Transport

Voltra speaks **WebSocket** with **MessagePack** binary framing.

- Connect to `ws://host:port` (default `ws://127.0.0.1:3000`).
- All frames are binary. Every frame is a MessagePack-encoded enum variant.
- If an API key is configured, pass it as `Authorization: Bearer <key>` in the WebSocket upgrade headers.
- To include a role: `Authorization: Bearer <key>:<role>`.

---

## Enum encoding

Both `ClientMessage` and `ServerMessage` are Rust enums serialized with `rmp_serde`. By default this produces an **array-of-2** format: `[variant_index_or_name, payload]`. Variant names are used when the enum is `#[serde(rename_all = ...)]` or the default string form.

In practice, encode messages using the provided SDKs. If you are implementing a client from scratch, encode each message as a MessagePack array whose first element is the variant name string and whose second element is the variant's payload (a map for struct variants, or the value directly for newtype variants).

---

## Client to Server Messages

### ReducerCall

Call a named reducer.

```
ClientMessage::ReducerCall({
  call_id:       u64,    // client-assigned, echoed in ReducerResponse
  reducer_name:  String,
  args:          Bytes,  // MessagePack-encoded args (array or map)
})
```

`args` must be a valid MessagePack value. Pass an empty array `[]` (MessagePack fixarray of length 0) if the reducer takes no arguments.

### Subscribe

Subscribe to a table query. The server replies with a `SubscriptionAck` and immediately streams existing matching rows as `SubscriptionDiff` frames with `operation = "initial_snapshot"`.

```
ClientMessage::Subscribe({
  subscription_id: String,  // client-assigned unique ID
  query:           String,  // subscription query (see grammar below)
})
```

### Unsubscribe

Cancel a subscription.

```
ClientMessage::Unsubscribe({
  subscription_id: String,
})
```

### SqlQuery

Execute an ad-hoc SQL statement against the live TableStore. Results are returned as `ServerMessage::SqlResult`.

```
ClientMessage::SqlQuery({
  query_id: u64,
  sql:      String,  // e.g. "SELECT * FROM players WHERE zone = 'north'"
})
```

### Heartbeat

Keeps the connection alive in environments with aggressive idle timeouts.

```
ClientMessage::Heartbeat
```

### SetPresence

Set the client's presence status (visible to other clients via the presence system).

```
ClientMessage::SetPresence({
  status:   String,
  metadata: Option<JSON>,
})
```

### SetTtl / CancelTtl

Schedule a row for automatic deletion after a timeout.

```
ClientMessage::SetTtl({
  table_name: String,
  row_key:    String,
  ttl_ms:     u64,
})

ClientMessage::CancelTtl({
  table_name: String,
  row_key:    String,
})
```

---

## Server to Client Messages

### ReducerResponse

Response to a `ReducerCall`.

```
ServerMessage::ReducerResponse({
  call_id: u64,
  success: bool,
  result:  Option<Bytes>,  // MessagePack-encoded return value, if any
  error:   Option<String>, // error message when success = false
})
```

### SubscriptionAck

Confirms that a subscription was registered (or rejected).

```
ServerMessage::SubscriptionAck({
  subscription_id: String,
  success:         bool,
  message:         Option<String>,  // error detail when success = false
})
```

### SubscriptionDiff

A row change notification for a subscribed query.

```
ServerMessage::SubscriptionDiff({
  subscription_id: String,
  table_name:      String,
  row_key:         String,
  operation:       String,  // "insert" | "update" | "delete" | "initial_snapshot"
  row_data:        Option<JSON>,  // null for deletes
})
```

`initial_snapshot` frames are sent immediately after `SubscriptionAck` to deliver the current matching rows. They are indistinguishable from normal diffs except by `operation`.

### Two-frame subscription protocol

When `two_frame_protocol = true` is set in config, a single table write that matches multiple subscriptions is delivered as two consecutive frames instead of one frame per subscription:

```
ServerMessage::SubscriptionRoute({
  subscription_ids: [String],  // IDs of the subscriptions this delta matches
})

ServerMessage::SubscriptionBody({
  table_name: String,
  row_key:    String,
  operation:  String,
  row_data:   Option<JSON>,
})
```

The client must buffer the `SubscriptionRoute` frame and apply it when the following `SubscriptionBody` arrives.

### SqlResult

Response to a `SqlQuery`.

```
ServerMessage::SqlResult({
  query_id:     u64,
  success:      bool,
  columns:      [String],
  rows:         [JSON],   // each row is a JSON object
  rows_affected: usize,   // for INSERT / UPDATE / DELETE
  error:        Option<String>,
})
```

### Error

General-purpose error frame not tied to a specific call.

```
ServerMessage::Error({
  message: String,
})
```

---

## Subscription Query Grammar

```ebnf
query        = table_name
             | table_name "WHERE" predicate
             | table_name "WHERE" predicate order_clause
             | table_name "WHERE" predicate limit_clause
             | table_name "WHERE" predicate order_clause limit_clause
             | table_name order_clause
             | table_name limit_clause
             | table_name order_clause limit_clause

predicate    = comparison
             | in_expr
             | predicate "AND" predicate
             | predicate "OR" predicate
             | "(" predicate ")"

comparison   = field op value
op           = "=" | "==" | "!=" | ">" | "<" | ">=" | "<="
in_expr      = field "IN" "(" value { "," value } ")"
value        = string_literal | number_literal | bool_literal

order_clause = "ORDER" "BY" field [ "ASC" | "DESC" ]
limit_clause = "LIMIT" integer

table_name   = identifier
field        = identifier
```

Operator precedence: `AND` binds tighter than `OR`. `(A AND B) OR C` is equivalent to `A AND B OR C`.

Examples:

```
counters
players WHERE level >= 10
players WHERE status = "active"
players WHERE status IN ("active", "vip", "moderator")
players WHERE score > 1000 AND level > 5
players WHERE zone = "north" OR zone = "south"
players ORDER BY score DESC
players WHERE level > 5 ORDER BY score DESC LIMIT 10
```

`ORDER BY` and `LIMIT` only affect the initial snapshot delivery. Live diffs arrive in commit order regardless.

---

## HTTP Admin Endpoints

The metrics/admin server runs on port 3001 by default.

| Method | Path | Description |
|---|---|---|
| GET | /health | `{"status":"ok","..."}` health check |
| GET | /metrics | Prometheus-style plaintext metrics |
| GET | /tables | List tables with row counts |
| GET | /tables/`<name>` | Dump all rows of a table as JSON |
| POST | /seed | Bulk-insert rows from JSON payload |
| GET | /raft/metrics | Raft node state (leader, term, commit index) |
| POST | /raft/init | Bootstrap a single-node Raft cluster |
| POST | /raft/add-learner | Add a new node as a learner |
| POST | /raft/change-membership | Change voter set |
| POST | /raft/append | Raft AppendEntries RPC (inter-node) |
| POST | /raft/vote | Raft RequestVote RPC (inter-node) |
| POST | /raft/snapshot | Raft InstallSnapshot RPC (inter-node) |
| GET | /cluster/peers | List known cluster peers |
| POST | /cluster/call | Forward a reducer call to this node (inter-node) |
| POST | /cluster/join | Dynamic peer registration |

### POST /seed

Request body:

```json
{
  "rows": [
    ["players", "alice", {"hp": 100, "level": 5}],
    ["counters", "score", {"value": 0}]
  ]
}
```

Response:

```json
{
  "rows_written": 2,
  "rows_skipped": 0,
  "errors": []
}
```

Seeded rows are written directly to the TableStore and bypass the WAL and reducer pipeline. They are not fan-out to live subscribers. Use this for development seeding only.
