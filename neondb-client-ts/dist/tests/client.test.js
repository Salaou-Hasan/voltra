import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { WebSocketServer } from "ws";
import { decode, encode } from "@msgpack/msgpack";
import { NeonDBClient } from "../client.js";
function randomPort() {
    // pick a random high port; if it's taken the test will fail fast.
    return 30_000 + Math.floor(Math.random() * 20_000);
}
async function waitUntil(fn, timeoutMs = 2_000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (fn())
            return;
        await sleep(10);
    }
    throw new Error("Timed out waiting for condition");
}
test("client sends Authorization header in Node when apiKey is set", async () => {
    const port = randomPort();
    const wss = new WebSocketServer({ port });
    let seenAuth;
    wss.on("connection", (ws, req) => {
        seenAuth = req.headers["authorization"];
        ws.on("message", (data) => {
            const buf = data instanceof Buffer ? new Uint8Array(data) : data;
            const msg = decode(buf);
            if ("ReducerCall" in msg) {
                const [callId] = msg.ReducerCall;
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
            const buf = data instanceof Buffer ? new Uint8Array(data) : data;
            const msg = decode(buf);
            if ("Subscribe" in msg) {
                const [subId] = msg.Subscribe;
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
            const buf = data instanceof Buffer ? new Uint8Array(data) : data;
            const msg = decode(buf);
            if ("Subscribe" in msg) {
                subscribeCount += 1;
                const [subId] = msg.Subscribe;
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
    client.subscribe("players", () => { });
    await client.connect();
    await waitUntil(() => connectionCount >= 2, 2_000);
    // One subscribe per connection
    await waitUntil(() => subscribeCount >= 2, 2_000);
    client.disconnect();
    wss.close();
});
//# sourceMappingURL=client.test.js.map