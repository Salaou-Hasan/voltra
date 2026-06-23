# TypeScript SDK

The TypeScript SDK is located at `voltra-client-ts/`. It works in both Node.js and browser environments.

> Note: the package is not yet published to npm. Use it locally via a relative path import or by copying the `src/` directory.

---

## Installation

```bash
cd voltra-client-ts
npm install
npm run build
```

For local use in another project:

```json
{
  "dependencies": {
    "@voltra/client": "file:../voltra-client-ts"
  }
}
```

In Node.js you also need the `ws` package:

```bash
npm install ws
```

---

## Connecting

```typescript
import { VoltraClient } from "@voltra/client";

const client = new VoltraClient({
  url: "ws://localhost:3000",
  apiKey: "your-api-key",         // optional
  reconnectInterval: 3_000,        // ms between reconnect attempts (0 = disabled)
  callTimeout: 5_000,              // ms before a pending call is rejected
});

await client.connect();

// Lifecycle hooks
client.onConnected    = () => console.log("connected");
client.onDisconnected = () => console.log("disconnected");
client.onError        = (msg) => console.error("error:", msg);
```

---

## Calling Reducers

```typescript
// Call with array args
const bytes = await client.call("increment", ["score", 5]);

// Decode MessagePack result
const result = client.decodeResult<{ value: number }>(bytes!);
console.log("new value:", result.value);

// Call with object args
await client.call("deal_damage", {
  attacker_id: "player_1",
  defender_id: "enemy_42",
  weapon_id:   "sword_a",
});
```

`call()` returns `Uint8Array | null`. It returns `null` if the reducer succeeded but produced no result bytes. It throws an `Error` if the server returned an error, the call timed out, or the connection dropped.

---

## Subscriptions

```typescript
const sub = client.subscribe("players WHERE level >= 5", (diff) => {
  console.log(diff.operation); // "insert" | "update" | "delete" | "initial_snapshot"
  console.log(diff.rowKey);
  console.log(diff.rowData);   // null on delete
});

// Cancel when done
sub.unsubscribe();
```

The first batch of callbacks fires immediately with `operation = "initial_snapshot"` for all currently matching rows, then live diffs arrive as rows change.

---

## Reading the Local Cache

The SDK maintains a client-side row cache populated by subscription diffs.

```typescript
// All rows in a table (Map<rowKey, rowData>)
const players = client.getRows("players");
for (const [key, row] of players) {
  console.log(key, row);
}

// Single row
const alice = client.getRow("players", "alice");
```

The cache reflects both server-confirmed updates and any in-flight optimistic updates.

---

## Optimistic Updates

Apply a speculative state change immediately before the server confirms it. On server error or timeout, the affected rows are automatically rolled back.

```typescript
await client.call(
  "move_player",
  { x: 5, y: 3 },
  {
    optimistic: (cache) => {
      // cache is a deep clone: Map<tableName, Map<rowKey, rowData>>
      const players = new Map(cache.get("players") ?? []);
      const alice = players.get("alice") ?? {};
      players.set("alice", { ...alice, x: 5, y: 3 });
      return new Map([...cache, ["players", players]]);
    },
    onRollback: (reason, cacheAfterRollback) => {
      console.warn("rolled back:", reason);
    },
  }
);
```

The rollback is targeted: only the rows the `optimistic` callback actually modified are restored. Subscription diffs that arrived mid-flight on other rows are preserved.

---

## Disconnecting

```typescript
client.disconnect();
```

All in-flight `call()` promises are rejected. All in-flight optimistic updates are rolled back. No automatic reconnect attempts are made after an explicit `disconnect()`.

---

## Error Handling

```typescript
try {
  await client.call("some_reducer", []);
} catch (err) {
  if (err instanceof Error) {
    console.error(err.message); // "Reducer returned an error: ..."
                                // or "call timed out after 5000ms"
                                // or "Not connected"
  }
}
```

---

## Auto-Reconnect

The SDK reconnects automatically when `reconnectInterval > 0` (the default is 3 seconds). On reconnect, all active subscriptions are re-sent to the server. The local row cache is not cleared — it continues to hold whatever state it had before the disconnect.

Set `reconnectInterval: 0` to disable auto-reconnect.

```typescript
const client = new VoltraClient({
  url: "ws://localhost:3000",
  reconnectInterval: 0,  // disabled
});
```
