import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";

import { WebSocketServer } from "ws";
import { decode, encode } from "@msgpack/msgpack";

import { NeonDBClient, computeBackoffDelay } from "../client.js";

function randomPort(): number {
  // pick a random high port; if it's taken the test will fail fast.
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

test("client sends Authorization header in Node when apiKey is set", async () => {
  const port = randomPort();
  const wss = new WebSocketServer({ port });

  let seenAuth: string | undefined;

  wss.on("connection", (ws, req) => {
    seenAuth = req.headers["authorization"] as string | undefined;

    ws.on("message", (data) => {
      const buf = data instanceof Buffer ? new Uint8Array(data) : (data as Uint8Array);
      const msg = decode(buf) as Record<string, unknown>;
      if ("ReducerCall" in msg) {
        const [callId] = (msg as { ReducerCall: [number, string, Uint8Array] }).ReducerCall;
        const resultBytes = encode({ ok: true });
        const frame = encode([callId, true, resultBytes, null]);
        ws.send(frame);
      }
    });
  });

  const client = new NeonDBClient({
    url: `ws://127.0.0.1:${port}`,
    apiKey: "secret",
    reconnectInterval: 0,
  });
  await client.connect();

  const resBytes = await client.call("increment", ["score", 1]);
  assert.ok(resBytes);
  assert.deepEqual(client.decodeResult(resBytes), { ok: true });

  client.disconnect();
  wss.close();

  assert.equal(seenAuth, "Bearer secret");
});

test("subscribe receives diffs and updates row cache", async () => {
  const port = randomPort();
  const wss = new WebSocketServer({ port });

  wss.on("connection", (ws) => {
    ws.on("message", (data) => {
      const buf = data instanceof Buffer ? new Uint8Array(data) : (data as Uint8Array);
      const msg = decode(buf) as Record<string, unknown>;

      if ("Subscribe" in msg) {
        const [subId] = (msg as { Subscribe: [string, string] }).Subscribe;
        ws.send(encode({ SubscriptionAck: [subId, true, null] }));
        ws.send(encode({ SubscriptionDiff: [subId, "players", "p1", "initial_snapshot", { level: 6 }] }));
      }
    });
  });

  const client = new NeonDBClient({
    url: `ws://127.0.0.1:${port}`,
    reconnectInterval: 0,
  });
  await client.connect();

  let gotDiff = false;
  client.subscribe("players WHERE level > 5", (diff) => {
    gotDiff = true;
    assert.equal(diff.tableName, "players");
    assert.equal(diff.rowKey, "p1");
    assert.equal(diff.operation, "initial_snapshot");
    assert.equal(diff.rowData?.level, 6);
  });

  await waitUntil(() => gotDiff, 2_000);
  assert.equal(client.getRow("players", "p1")?.level, 6);

  client.disconnect();
  wss.close();
});

test("auto-reconnect re-sends subscriptions", async () => {
  const port = randomPort();
  const wss = new WebSocketServer({ port });

  let subscribeCount = 0;
  let connectionCount = 0;

  wss.on("connection", (ws) => {
    connectionCount += 1;

    ws.on("message", (data) => {
      const buf = data instanceof Buffer ? new Uint8Array(data) : (data as Uint8Array);
      const msg = decode(buf) as Record<string, unknown>;
      if ("Subscribe" in msg) {
        subscribeCount += 1;
        const [subId] = (msg as { Subscribe: [string, string] }).Subscribe;
        ws.send(encode({ SubscriptionAck: [subId, true, null] }));
      }
    });

    // Force a reconnect after the first connection is up.
    if (connectionCount === 1) {
      setTimeout(() => ws.close(), 50);
    }
  });

  const client = new NeonDBClient({
    url: `ws://127.0.0.1:${port}`,
    reconnectInterval: 50,
  });

  client.subscribe("players", () => {});
  await client.connect();

  await waitUntil(() => connectionCount >= 2, 2_000);
  // One subscribe per connection
  await waitUntil(() => subscribeCount >= 2, 2_000);

  client.disconnect();
  wss.close();
});

// ── Reconnect delay unit tests (pure math, no network) ────────────────────────

test("computeBackoffDelay: no jitter — returns exact exponential delay", () => {
  // attempt 0 → baseDelayMs * 2^0 = 1000
  assert.equal(computeBackoffDelay(0, 1_000, 30_000, false), 1_000);
  // attempt 1 → 2000
  assert.equal(computeBackoffDelay(1, 1_000, 30_000, false), 2_000);
  // attempt 2 → 4000
  assert.equal(computeBackoffDelay(2, 1_000, 30_000, false), 4_000);
  // attempt 5 → 32000, but maxDelayMs=30000 clamps it
  assert.equal(computeBackoffDelay(5, 1_000, 30_000, false), 30_000);
});

test("computeBackoffDelay: jitter stays within ±25% of capped base", () => {
  const base = 1_000;
  const max = 30_000;
  for (let attempt = 0; attempt < 6; attempt++) {
    const rawBase = Math.min(max, base * Math.pow(2, attempt));
    const low = Math.round(rawBase * 0.75);
    const high = Math.round(rawBase * 1.25);
    for (let i = 0; i < 20; i++) {
      const delay = computeBackoffDelay(attempt, base, max, true);
      assert.ok(
        delay >= low && delay <= high,
        `attempt=${attempt}: delay ${delay} not in [${low}, ${high}]`,
      );
    }
  }
});

test("computeBackoffDelay: maxDelayMs is respected even with jitter", () => {
  // With jitter the upper bound is 1.25 * base.  When base >= maxDelayMs the
  // result may exceed maxDelayMs by up to 25%.  Check that base is capped
  // BEFORE jitter is applied, so jitter is applied to the capped value.
  const base = 1_000;
  const maxDelay = 2_000;
  // At attempt 10 raw = 1024000 >> maxDelay, so capped = 2000.
  // With jitter result should be in [1500, 2500].
  for (let i = 0; i < 30; i++) {
    const d = computeBackoffDelay(10, base, maxDelay, true);
    assert.ok(d >= Math.round(maxDelay * 0.75) && d <= Math.round(maxDelay * 1.25),
      `d=${d} not in jitter-allowed range around maxDelay=${maxDelay}`);
  }
});

// ── reconnect.maxAttempts fires onReconnectFailed ─────────────────────────────

test("onReconnectFailed is called when maxAttempts is exhausted", async () => {
  // Strategy: connect to a real server; drop the individual WebSocket from the
  // server side, then close the entire server so all reconnect attempts fail.
  const port = randomPort();
  const wss = new WebSocketServer({ port });

  let failedCalled = false;
  let failedErr: Error | undefined;
  let disconnectCalled = false;

  // Track the server-side WebSocket so we can drop it.
  let serverWs: import("ws").WebSocket | undefined;
  wss.on("connection", (ws) => { serverWs = ws; });

  const client = new NeonDBClient({
    url: `ws://127.0.0.1:${port}`,
    reconnect: {
      enabled: true,
      maxAttempts: 2,
      baseDelayMs: 30,
      maxDelayMs: 30,
      jitter: false,
    },
    onDisconnect: () => {
      // Once onDisconnect fires we know the socket really closed client-side.
      // Close the server now so subsequent reconnect attempts fail.
      wss.close();
      disconnectCalled = true;
    },
    onReconnectFailed: (err) => {
      failedCalled = true;
      failedErr = err;
    },
  });

  await client.connect();

  // Drop the server-side socket — triggers client onclose → scheduleReconnect.
  serverWs?.terminate();

  // 2 attempts × 30 ms + socket round-trip + margin.
  await waitUntil(() => failedCalled, 3_000);

  assert.ok(disconnectCalled, "onDisconnect should have fired");
  assert.ok(failedCalled, "onReconnectFailed should have been called");
  assert.ok(failedErr instanceof Error);

  client.disconnect();
});

// ── pending calls queue while disconnected ────────────────────────────────────

test("call queued while disconnected is flushed after reconnect", async () => {
  const port = randomPort();

  let connectionCount = 0;
  let disconnectFired = false;
  const wss = new WebSocketServer({ port });

  wss.on("connection", (ws) => {
    connectionCount += 1;
    ws.on("message", (data) => {
      const buf = data instanceof Buffer ? new Uint8Array(data) : (data as Uint8Array);
      const msg = decode(buf) as Record<string, unknown>;
      if ("ReducerCall" in msg) {
        const [callId] = (msg as { ReducerCall: [number, string, Uint8Array] }).ReducerCall;
        const resultBytes = encode({ pong: true });
        ws.send(encode([callId, true, resultBytes, null]));
      }
    });
  });

  const client = new NeonDBClient({
    url: `ws://127.0.0.1:${port}`,
    reconnect: {
      enabled: true,
      maxAttempts: 10,
      baseDelayMs: 40,
      maxDelayMs: 40,
      jitter: false,
    },
    onDisconnect: () => { disconnectFired = true; },
  });

  await client.connect();

  // Close the server-side socket for connection 1 to force a reconnect.
  // The wss.clients iteration gives us all server-side sockets.
  for (const ws of wss.clients) {
    ws.terminate();
    break;
  }

  // Wait until the client-side disconnect fires — then call() goes to callQueue.
  await waitUntil(() => disconnectFired, 1_000);

  // Now issue the call. The socket is confirmed closed so it goes to callQueue.
  const callPromise = client.call("ping", []);

  const result = await Promise.race([
    callPromise,
    sleep(2_000).then(() => { throw new Error("Timed out waiting for queued call"); }),
  ]);

  assert.ok(result instanceof Uint8Array, "queued call should resolve with result bytes");
  assert.ok(connectionCount >= 2, "client should have reconnected");

  client.disconnect();
  wss.close();
});

// ── onReconnect callback ──────────────────────────────────────────────────────

test("onReconnect is called with the attempt number after successful reconnect", async () => {
  const port = randomPort();
  const wss = new WebSocketServer({ port });

  let connectionCount = 0;
  let reconnectAttempt: number | undefined;

  wss.on("connection", (ws) => {
    connectionCount += 1;
    // Drop the first connection to trigger a reconnect.
    if (connectionCount === 1) {
      setTimeout(() => ws.close(), 30);
    }
  });

  const client = new NeonDBClient({
    url: `ws://127.0.0.1:${port}`,
    reconnect: {
      enabled: true,
      baseDelayMs: 30,
      maxDelayMs: 30,
      jitter: false,
    },
    onReconnect: (attempt) => {
      reconnectAttempt = attempt;
    },
  });

  await client.connect();
  await waitUntil(() => reconnectAttempt !== undefined, 2_000);

  assert.ok(reconnectAttempt !== undefined, "onReconnect should have fired");
  assert.ok(reconnectAttempt >= 1, "attempt should be >= 1");

  client.disconnect();
  wss.close();
});

