# @voltra/client

TypeScript/JavaScript client SDK for [Voltra](../) — the self-hosted, zero-cost real-time game backend.

**100% free.** No cloud account. No fees. Voltra and Dokploy run on hardware you own.

---

## Installation

```bash
npm install @voltra/client @msgpack/msgpack
# Node.js also needs the ws library for WebSocket + auth header support:
npm install ws
```

---

## Quick Start

```typescript
import { VoltraClient } from "@voltra/client";

const client = new VoltraClient({
  url: "ws://localhost:3000",
  // apiKey: "your-api-key",  // only if VOLTRA_API_KEY is set server-side
});

await client.connect();

// ── Call a reducer ────────────────────────────────────────────────────────────

// For the built-in `increment` reducer, pass a positional array
// matching the Rust struct: IncrementArgs { name: String, delta: i32 }
const resultBytes = await client.call("increment", ["player_score", 10]);
if (resultBytes) {
  const result = client.decodeResult<{ new_value: number; timestamp: number }>(resultBytes);
  console.log("New score:", result.new_value);
}

// For JS/WASM reducers that accept an object:
await client.call("my_reducer", { key: "value", amount: 5 });

// ── Subscribe to changes ──────────────────────────────────────────────────────

const sub = client.subscribe("counters", (diff) => {
  console.log(`[${diff.operation}] ${diff.rowKey}:`, diff.rowData);
});

// With a WHERE predicate:
const sub2 = client.subscribe("players WHERE level > 5", (diff) => {
  console.log("High-level player update:", diff.rowKey);
});

// With IN operator:
const sub3 = client.subscribe("players WHERE status IN ('active', 'pending')", (diff) => {
  console.log("Active/pending player:", diff.rowKey);
});

// With AND compound predicate:
const sub4 = client.subscribe("players WHERE score > 100 AND level > 5", (diff) => {
  console.log("Elite player:", diff.rowKey);
});

// Subscription events:
//   diff.operation === "initial_snapshot"  → row existed before subscribe
//   diff.operation === "insert"            → new row inserted
//   diff.operation === "update"            → row updated
//   diff.operation === "delete"            → row deleted

// ── Read the local row cache ──────────────────────────────────────────────────

// After subscribing, the client caches all received rows locally.
const allPlayers = client.getRows("players");  // Map<rowKey, rowData>
const player = client.getRow("players", "hero_1");

// ── Cleanup ───────────────────────────────────────────────────────────────────

sub.unsubscribe();
sub2.unsubscribe();
client.disconnect();
```

---

## API Reference

### `new VoltraClient(options)`

| Option | Type | Default | Description |
|---|---|---|---|
| `url` | `string` | required | WebSocket URL, e.g. `"ws://localhost:3000"` |
| `apiKey` | `string` | `""` | API key (Node.js only — sent as `Authorization: Bearer`) |
| `reconnectInterval` | `number` | `3000` | Auto-reconnect interval in ms (0 = disabled) |
| `callTimeout` | `number` | `5000` | Reducer call timeout in ms |

### `client.connect(): Promise<void>`
Open the WebSocket connection.

### `client.disconnect(): void`
Close the connection and stop auto-reconnect.

### `client.call(reducer, args?): Promise<Uint8Array | null>`
Call a server reducer.  Throws on error or timeout.

The built-in `increment` reducer expects a **positional array** `[name, delta]` because the
server uses `rmp_serde` struct encoding (compact array format):
```typescript
await client.call("increment", ["my_counter", 1]);
```

Custom JS/WASM reducers may accept objects:
```typescript
await client.call("my_reducer", { field: "value" });
```

### `client.decodeResult<T>(bytes): T`
Decode MessagePack result bytes from a `call()` response.

### `client.subscribe(query, callback): Subscription`
Subscribe to a table with an optional `WHERE` predicate.
Returns a `Subscription` with an `.unsubscribe()` method.

### `client.getRows(tableName): Map<string, object>`
Return the local row cache for a table (populated by subscription diffs).

### `client.getRow(tableName, rowKey): object | undefined`
Return a single cached row.

---

## Wire Protocol

Voltra uses **MessagePack** (via `rmp_serde` on the server) with these conventions:

| Type | Encoding |
|---|---|
| Rust struct | MessagePack ARRAY (positional fields, no keys) |
| Rust enum variant | MessagePack MAP `{"VariantName": [fields…]}` |
| `Option<T>` | `nil` or `T` |
| `Vec<u8>` | MessagePack BIN |

Outgoing messages (client → server):
- `{ "ReducerCall": [call_id, name, args_bin] }`
- `{ "Subscribe": [subscription_id, query] }`
- `{ "Unsubscribe": [subscription_id] }`

Incoming messages (server → client):
- `[call_id, success, result_bin|nil, error|nil]` — reducer response (bare array)
- `{ "SubscriptionDiff": [sub_id, table, key, op, data|nil] }` — row change
- `{ "SubscriptionAck": [sub_id, ok, msg|nil] }` — subscription confirmation
- `{ "Error": [message] }` — server error

---

## Authentication

**Node.js** (recommended): API key sent as HTTP header during WebSocket upgrade:
```typescript
const client = new VoltraClient({ url, apiKey: "my-secret-key" });
```

**Browser**: Browsers cannot set custom HTTP headers for WebSocket connections.
Options:
1. Deploy behind a proxy that injects the header
2. Use Voltra without an API key (open access)
3. Pass the key as a URL query parameter and add server-side query-param support

---

## Auto-Reconnect

The client automatically reconnects after unexpected disconnections (default: every 3 seconds).
Set `reconnectInterval: 0` to disable.

After reconnecting, you must re-subscribe because subscription state is not persisted.
Listen to `onConnected` to re-subscribe:

```typescript
client.onConnected = () => {
  client.subscribe("players", handlePlayer);
};
```

---

## Building from Source

```bash
cd voltra-client-ts
npm install
npm run build
# Output in dist/
```
