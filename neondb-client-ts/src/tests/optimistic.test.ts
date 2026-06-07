// ============================================================================
// Optimistic Update Race Tests
//
// Covers the targeted-rollback fix: we must roll back ONLY the rows the
// optimistic callback mutated, not the entire cache.  Specifically:
//   test_a — happy path: cache reflects new value on success.
//   test_b — RACE: error rolls back ONLY touched rows; rows changed by a
//            subscription diff mid-flight are PRESERVED.
//   test_c — no-op callback (returns identical cache) → no-op rollback.
//   test_d — delete via optimistic: row deleted speculatively, error restores
//            it; mid-flight insert to a different table is preserved.
// ============================================================================
import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";

import { WebSocketServer, WebSocket as WsWebSocket } from "ws";
import { decode, encode } from "@msgpack/msgpack";

import { NeonDBClient } from "../client.js";
import type { OptimisticCache } from "../types.js";

function randomPort(): number {
  return 30_000 + Math.floor(Math.random() * 20_000);
}

async function waitUntil(fn: () => boolean, timeoutMs = 2_000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (fn()) return;
    await sleep(10);
  }
  throw new Error("Timed out waiting for condition");
}

/**
 * Helper: build a server that holds the call until a manual gate is opened,
 * then responds with success/error.  Lets us inject a subscription diff
 * mid-flight before resolving.
 */
type ServerHandle = {
  port: number;
  wss: WebSocketServer;
  /** Open the gate to send the held ReducerResponse (success). */
  respondSuccess: () => void;
  /** Open the gate to send the held ReducerResponse (error). */
  respondError: (msg: string) => void;
  /** Push a SubscriptionDiff into the open socket. */
  pushDiff: (subId: string, table: string, key: string, op: string, data: unknown) => void;
  close: () => void;
  /** The active client socket, populated after a client connects. */
  client?: WsWebSocket;
};

function makeHoldingServer(): Promise<ServerHandle> {
  return new Promise((resolve) => {
    const port = randomPort();
    const wss = new WebSocketServer({ port });

    let heldCallId: number | null = null;
    let openSocket: WsWebSocket | null = null;
    const handle: ServerHandle = {
      port,
      wss,
      respondSuccess: () => {
        if (heldCallId == null || !openSocket) return;
        const resultBytes = encode({ ok: true });
        openSocket.send(encode([heldCallId, true, resultBytes, null]));
        heldCallId = null;
      },
      respondError: (msg: string) => {
        if (heldCallId == null || !openSocket) return;
        openSocket.send(encode([heldCallId, false, null, msg]));
        heldCallId = null;
      },
      pushDiff: (subId, table, key, op, data) => {
        if (!openSocket) return;
        openSocket.send(
          encode({ SubscriptionDiff: [subId, table, key, op, data] }),
        );
      },
      close: () => wss.close(),
    };

    wss.on("connection", (ws) => {
      openSocket = ws;
      handle.client = ws;
      ws.on("message", (data) => {
        const buf = data instanceof Buffer ? new Uint8Array(data) : (data as Uint8Array);
        const msg = decode(buf) as Record<string, unknown>;
        if ("ReducerCall" in msg) {
          const [callId] = (msg as { ReducerCall: [number, string, Uint8Array] }).ReducerCall;
          heldCallId = callId;
          // do NOT respond — wait for the test to call respondSuccess/Error.
        } else if ("Subscribe" in msg) {
          const [subId] = (msg as { Subscribe: [string, string] }).Subscribe;
          ws.send(encode({ SubscriptionAck: [subId, true, null] }));
        }
      });
    });

    wss.on("listening", () => resolve(handle));
  });
}

// ── test_a: happy path ────────────────────────────────────────────────────────

test("optimistic: success leaves speculative value in cache", async () => {
  const server = await makeHoldingServer();
  const client = new NeonDBClient({
    url: `ws://127.0.0.1:${server.port}`,
    reconnectInterval: 0,
  });
  await client.connect();

  const callPromise = client.call("move_player", { x: 5 }, {
    optimistic: (cache: OptimisticCache) => {
      const players = new Map(cache.get("players") ?? []);
      players.set("alice", { x: 5, y: 0 });
      const next: OptimisticCache = new Map(cache);
      next.set("players", players);
      return next;
    },
  });

  // Cache must reflect the speculative state immediately.
  await waitUntil(() => client.getRow("players", "alice") !== undefined);
  assert.deepEqual(client.getRow("players", "alice"), { x: 5, y: 0 });

  server.respondSuccess();
  await callPromise;

  // Still reflects the speculative value (server diffs would reconcile in a real run).
  assert.deepEqual(client.getRow("players", "alice"), { x: 5, y: 0 });

  client.disconnect();
  server.close();
});

// ── test_b: THE RACE — error rolls back only touched rows ────────────────────

test("optimistic: error preserves mid-flight subscription diffs on untouched rows", async () => {
  const server = await makeHoldingServer();
  const client = new NeonDBClient({
    url: `ws://127.0.0.1:${server.port}`,
    reconnectInterval: 0,
  });
  await client.connect();

  // 1. Subscribe to "enemies" so the server can push diffs through it.
  let gotEnemyDiff = false;
  const sub = client.subscribe("enemies", () => {
    gotEnemyDiff = true;
  });

  // 2. Wait until subscribe is ack'd, then push an initial enemy row.
  await sleep(50);
  server.pushDiff(sub.id, "enemies", "goblin1", "insert", { hp: 100 });
  await waitUntil(() => client.getRow("enemies", "goblin1") !== undefined);
  assert.deepEqual(client.getRow("enemies", "goblin1"), { hp: 100 });

  // 3. Fire an optimistic call that ONLY mutates the players table.
  const callPromise = client.call(
    "move_player",
    { x: 5 },
    {
      optimistic: (cache: OptimisticCache) => {
        const players = new Map(cache.get("players") ?? []);
        players.set("alice", { x: 5, y: 0 });
        const next: OptimisticCache = new Map(cache);
        next.set("players", players);
        return next;
      },
    },
  );

  // Speculative update visible.
  await waitUntil(() => client.getRow("players", "alice") !== undefined);
  assert.deepEqual(client.getRow("players", "alice"), { x: 5, y: 0 });

  // 4. Mid-flight: server pushes a subscription diff that updates goblin1.
  server.pushDiff(sub.id, "enemies", "goblin1", "update", { hp: 50 });
  await waitUntil(
    () => (client.getRow("enemies", "goblin1") as { hp: number } | undefined)?.hp === 50,
  );

  // 5. Server now rejects the move_player call.
  server.respondError("invalid move");

  await assert.rejects(callPromise, /invalid move/);

  // 6. CRITICAL ASSERTIONS:
  //    (a) players/alice is rolled back (did not exist before → now gone).
  assert.equal(
    client.getRow("players", "alice"),
    undefined,
    "alice should be rolled back (touched row)",
  );
  //    (b) enemies/goblin1 KEPT the mid-flight diff value of 50, not the
  //        pre-call value of 100. This is the race we are protecting against.
  assert.deepEqual(
    client.getRow("enemies", "goblin1"),
    { hp: 50 },
    "goblin1 must NOT be rolled back to pre-call value — it was changed by a subscription diff mid-flight",
  );

  assert.ok(gotEnemyDiff, "subscription callback should have fired");

  client.disconnect();
  server.close();
});

// ── test_c: no-op optimistic callback ────────────────────────────────────────

test("optimistic: callback returns identical cache → empty touched set, no-op rollback", async () => {
  const server = await makeHoldingServer();
  const client = new NeonDBClient({
    url: `ws://127.0.0.1:${server.port}`,
    reconnectInterval: 0,
  });
  await client.connect();

  // Pre-populate the cache via a subscription diff.
  const sub = client.subscribe("players", () => {});
  await sleep(50);
  server.pushDiff(sub.id, "players", "bob", "insert", { x: 0 });
  await waitUntil(() => client.getRow("players", "bob") !== undefined);

  const callPromise = client.call(
    "noop_reducer",
    [],
    {
      optimistic: (cache: OptimisticCache) => {
        // Return the cache untouched — no rows mutated.
        return cache;
      },
    },
  );

  // Cache unchanged immediately.
  assert.deepEqual(client.getRow("players", "bob"), { x: 0 });

  // Server returns an error — rollback should be a no-op.
  server.respondError("nope");
  await assert.rejects(callPromise, /nope/);

  // bob is still there with the same value (was not in touched set).
  assert.deepEqual(client.getRow("players", "bob"), { x: 0 });

  client.disconnect();
  server.close();
});

// ── test_d: optimistic delete + mid-flight insert to another table ──────────

test("optimistic: speculative delete is restored on error; other-table insert preserved", async () => {
  const server = await makeHoldingServer();
  const client = new NeonDBClient({
    url: `ws://127.0.0.1:${server.port}`,
    reconnectInterval: 0,
  });
  await client.connect();

  // Pre-populate players/alice via subscription.
  const sub = client.subscribe("players", () => {});
  await sleep(50);
  server.pushDiff(sub.id, "players", "alice", "insert", { x: 1, y: 2 });
  await waitUntil(() => client.getRow("players", "alice") !== undefined);

  // Fire an optimistic call that DELETES alice.
  const callPromise = client.call("kick_player", ["alice"], {
    optimistic: (cache: OptimisticCache) => {
      const next: OptimisticCache = new Map();
      for (const [t, rows] of cache) {
        if (t === "players") {
          // Drop alice.
          const newRows = new Map(rows);
          newRows.delete("alice");
          next.set(t, newRows);
        } else {
          next.set(t, new Map(rows));
        }
      }
      return next;
    },
  });

  // Speculative delete visible.
  await waitUntil(() => client.getRow("players", "alice") === undefined);

  // Mid-flight: server inserts an item row in an unrelated table.
  const sub2 = client.subscribe("items", () => {});
  await sleep(50);
  server.pushDiff(sub2.id, "items", "sword", "insert", { dmg: 10 });
  await waitUntil(() => client.getRow("items", "sword") !== undefined);

  // Server rejects.
  server.respondError("not allowed");
  await assert.rejects(callPromise, /not allowed/);

  // alice is restored (was a touched row, pre-call value was {x:1,y:2}).
  assert.deepEqual(client.getRow("players", "alice"), { x: 1, y: 2 });
  // items/sword preserved (untouched by the optimistic call).
  assert.deepEqual(client.getRow("items", "sword"), { dmg: 10 });

  client.disconnect();
  server.close();
});
