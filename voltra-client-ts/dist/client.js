// ============================================================================
// Voltra TypeScript Client SDK — VoltraClient
// Session 31 — TODO-021: Optimistic updates
//   call(reducer, args, { optimistic }) applies a speculative cache update
//   immediately, then rolls back on server error.
// Session (auto-reconnect) — exponential-backoff reconnect with:
//   - pending call queue (calls made while disconnected are buffered)
//   - subscription re-issue after reconnect
//   - optimistic rollback on disconnect (pitfall #21)
//   - onDisconnect / onReconnect / onReconnectFailed callbacks
// ============================================================================
import { encodeReducerCall, encodeSubscribe, encodeUnsubscribe, encodeArgs, decodeServerMessage, decodeResult, } from "./protocol.js";
// Use native WebSocket in browsers; dynamically import 'ws' in Node.js.
async function getWebSocketCtor() {
    if (typeof globalThis.WebSocket !== "undefined") {
        return globalThis.WebSocket;
    }
    try {
        const mod = await import("ws");
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return (mod.WebSocket ??
            mod.default ??
            mod);
    }
    catch {
        throw new Error("WebSocket is not available. In Node.js, install the 'ws' package: npm install ws");
    }
}
// ── Reconnect helpers ─────────────────────────────────────────────────────────
/** Resolve ReconnectOptions → concrete values with defaults applied. */
function resolveReconnect(opts) {
    if (opts.reconnect) {
        return {
            enabled: opts.reconnect.enabled ?? true,
            maxAttempts: opts.reconnect.maxAttempts ?? Infinity,
            baseDelayMs: opts.reconnect.baseDelayMs ?? 1_000,
            maxDelayMs: opts.reconnect.maxDelayMs ?? 30_000,
            jitter: opts.reconnect.jitter ?? true,
        };
    }
    // Legacy: reconnectInterval field.
    const interval = opts.reconnectInterval ?? 3_000;
    return {
        enabled: interval > 0,
        maxAttempts: Infinity,
        baseDelayMs: interval,
        maxDelayMs: interval,
        jitter: false,
    };
}
/**
 * Compute exponential-backoff delay for `attempt` (0-based).
 * Formula: min(maxDelayMs, baseDelayMs * 2^attempt) ± 25% jitter.
 */
export function computeBackoffDelay(attempt, baseDelayMs, maxDelayMs, jitter) {
    const base = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt));
    if (!jitter)
        return base;
    // ±25%: multiply by a uniform value in [0.75, 1.25].
    const factor = 0.75 + Math.random() * 0.5;
    return Math.round(base * factor);
}
export class VoltraClient {
    opts;
    reconnectCfg;
    ws = null;
    pendingCalls = new Map();
    subscriptions = new Map();
    /**
     * Server-confirmed row state — updated exclusively by subscription diffs and
     * initial snapshots received from the server.  Never mutated by optimistic calls.
     */
    serverBaseCache = new Map();
    /**
     * Ordered stack of in-flight optimistic mutations.  `rowCache` is always
     * `serverBaseCache` + each layer's mutation applied in order.  Removing a
     * layer and recomputing fixes the concurrent-update race (TODO-036): rolling
     * back call #1 automatically re-applies call #2's mutation on top of the
     * (now clean) server base.
     */
    optimisticLayers = [];
    /** Derived view: serverBaseCache + optimisticLayers applied in order. */
    rowCache = new Map();
    nextCallId = 1;
    nextSubId = 1;
    reconnectTimer = null;
    closed = false;
    pendingRoute = null;
    // ── Reconnect state ───────────────────────────────────────────────────────
    /** Number of consecutive failed reconnect attempts so far. */
    reconnectAttempts = 0;
    /**
     * Calls issued while the socket was down.  Flushed (in order) immediately
     * after the next successful reconnect.
     */
    callQueue = [];
    // ── Connection lifecycle events ───────────────────────────────────────────
    onConnected;
    onDisconnected;
    onError;
    constructor(options) {
        this.opts = {
            reconnectInterval: 3_000,
            callTimeout: 5_000,
            apiKey: "",
            reconnect: undefined,
            onDisconnect: undefined,
            onReconnect: undefined,
            onReconnectFailed: undefined,
            ...options,
        };
        this.reconnectCfg = resolveReconnect(options);
    }
    // ── Connection ────────────────────────────────────────────────────────────
    connect() {
        if (this.ws?.readyState === WebSocket.OPEN) {
            return Promise.resolve();
        }
        this.closed = false;
        return this.openSocket();
    }
    /**
     * Gracefully disconnect.  Sets `userInitiatedClose` so no reconnect fires.
     * Rejects all pending (in-flight) calls immediately.
     */
    disconnect() {
        this.closed = true;
        if (this.reconnectTimer != null) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        this.ws?.close();
        this.ws = null;
        this.rejectAllPending(new Error("Client disconnected"));
        this.drainCallQueue(new Error("Client disconnected"));
    }
    // ── Reducer calls ─────────────────────────────────────────────────────────
    /**
     * Call a reducer and return the raw result bytes.
     *
     * **Standard (non-optimistic):**
     * ```ts
     * const bytes = await client.call("increment", ["score", 1]);
     * ```
     *
     * **Optimistic update:**
     * ```ts
     * await client.call("move_player", { x: 5, y: 3 }, {
     *   optimistic: (cache) => {
     *     const players = new Map(cache.get("players") ?? []);
     *     players.set("alice", { ...players.get("alice"), x: 5, y: 3 });
     *     return new Map([...cache, ["players", players]]);
     *   },
     *   onRollback: (err, rolled) => console.warn("rolled back:", err),
     * });
     * ```
     *
     * When `optimistic` is provided the client:
     *   1. Snapshots the current cache.
     *   2. Applies your speculative cache immediately (so `getRows()` reflects
     *      the change before the server responds).
     *   3. Sends the reducer call to the server.
     *   4. On server **success**: server subscription diffs naturally reconcile.
     *   5. On server **error**: cache is rolled back to the pre-call snapshot
     *      and `onRollback` is called if supplied.
     *
     * **Disconnected behaviour**: if the socket is not currently open the call
     * is buffered and automatically flushed once the next reconnect succeeds.
     * The returned Promise resolves/rejects when the buffered call completes.
     *
     * @returns Raw result bytes, or `null` if the call succeeded with no result.
     * @throws  If the reducer returned an error or the call timed out.
     */
    call(reducerName, args = [], optimisticOpts) {
        // If disconnected, queue the call and return a deferred promise.
        if (!this.isConnected()) {
            return new Promise((resolve, reject) => {
                this.callQueue.push({ reducerName, args, optimisticOpts, resolve, reject });
            });
        }
        return this.dispatchCall(reducerName, args, optimisticOpts);
    }
    /**
     * Decode MessagePack result bytes into a JavaScript value.
     */
    decodeResult(bytes) {
        return decodeResult(bytes);
    }
    // ── Subscriptions ─────────────────────────────────────────────────────────
    /**
     * Subscribe to a Voltra table (with an optional WHERE predicate).
     *
     * ```ts
     * const sub = client.subscribe("players WHERE level > 5", (diff) => {
     *   console.log(diff.operation, diff.rowKey, diff.rowData);
     * });
     * sub.unsubscribe();
     * ```
     *
     * Supported predicates:
     *   `WHERE field op value`, `WHERE field IN (…)`, `WHERE a AND b`,
     *   `WHERE a OR b`, `ORDER BY field ASC|DESC`, `LIMIT N`
     */
    subscribe(query, callback) {
        const subId = `sub_${this.nextSubId++}_${Date.now()}`;
        this.subscriptions.set(subId, { query, callback });
        const frame = encodeSubscribe(subId, query);
        if (this.isConnected()) {
            this.send(frame);
        }
        return {
            id: subId,
            unsubscribe: () => {
                this.subscriptions.delete(subId);
                if (this.isConnected()) {
                    this.send(encodeUnsubscribe(subId));
                }
            },
        };
    }
    // ── Row cache ─────────────────────────────────────────────────────────────
    /**
     * Return the client-side row cache for a table.
     * Reflects both server-confirmed diffs and any in-flight optimistic updates.
     */
    getRows(tableName) {
        return this.rowCache.get(tableName) ?? new Map();
    }
    getRow(tableName, rowKey) {
        return this.rowCache.get(tableName)?.get(rowKey);
    }
    // ── Status ────────────────────────────────────────────────────────────────
    isConnected() {
        return this.ws?.readyState === WebSocket.OPEN;
    }
    // ── Optimistic helpers ────────────────────────────────────────────────────
    /**
     * Recompute `rowCache` = `serverBaseCache` + all `optimisticLayers` applied
     * in dispatch order.
     *
     * Called after every event that mutates either the server base or the layers
     * stack: subscription diffs, optimistic apply, optimistic rollback, disconnect.
     *
     * When no layers are pending this is a cheap shallow copy.  When layers ARE
     * pending the server base is deep-cloned once and each mutation is applied in
     * sequence — O(L × N) where L = layer count and N = touched rows per layer;
     * typically both are small (1–3 calls, 1–10 rows each).
     */
    recomputeRowCache() {
        if (this.optimisticLayers.length === 0) {
            // Fast path: no speculative changes.  rowCache mirrors serverBaseCache directly.
            this.rowCache.clear();
            for (const [table, rows] of this.serverBaseCache) {
                this.rowCache.set(table, rows);
            }
            return;
        }
        // Build speculative view: deep-clone base then apply each layer's mutation.
        let current = new Map();
        for (const [table, rows] of this.serverBaseCache) {
            current.set(table, new Map(rows));
        }
        for (const layer of this.optimisticLayers) {
            current = layer.mutation(current);
        }
        this.rowCache.clear();
        for (const [table, rows] of current) {
            this.rowCache.set(table, rows);
        }
    }
    /**
     * Deep-snapshot the current (speculative) row cache into an OptimisticCache.
     * Used for passing to the `optimistic` callback and for `onRollback` payloads.
     */
    snapshotCache() {
        const snap = new Map();
        for (const [table, rows] of this.rowCache) {
            snap.set(table, new Map(rows));
        }
        return snap;
    }
    /**
     * Remove the optimistic layer for `callId` from the stack and recompute
     * `rowCache`.  Called on reducer success (layer confirmed by server diffs),
     * reducer error, timeout, and disconnect.
     *
     * Because we replay the remaining layers on top of `serverBaseCache`, rolling
     * back call #1 automatically re-applies call #2's mutation — fixing the
     * concurrent-overlapping-call race (TODO-036).
     */
    removeOptimisticLayer(callId) {
        const idx = this.optimisticLayers.findIndex((l) => l.callId === callId);
        if (idx !== -1) {
            this.optimisticLayers.splice(idx, 1);
        }
        this.recomputeRowCache();
    }
    // ── Internal ──────────────────────────────────────────────────────────────
    /**
     * Core call dispatch — assumes the socket IS currently open.
     */
    dispatchCall(reducerName, args, optimisticOpts) {
        return new Promise((resolve, reject) => {
            const callId = this.nextCallId++;
            const encodedArgs = encodeArgs(args);
            const frame = encodeReducerCall(callId, reducerName, encodedArgs);
            // ── Optimistic: push a layer onto the stack and recompute rowCache ─────
            const isOptimistic = !!optimisticOpts?.optimistic;
            if (isOptimistic) {
                // Store the raw user-provided mutation function.  On replay (after a
                // sibling layer is rolled back), recomputeRowCache() calls it again on
                // the freshly rebuilt serverBase — this is what fixes the concurrent-
                // call race (TODO-036).
                this.optimisticLayers.push({
                    callId,
                    mutation: optimisticOpts.optimistic,
                });
                this.recomputeRowCache();
            }
            const timer = setTimeout(() => {
                this.pendingCalls.delete(callId);
                if (isOptimistic) {
                    this.removeOptimisticLayer(callId);
                    optimisticOpts?.onRollback?.(`call "${reducerName}" timed out`, this.snapshotCache());
                }
                reject(new Error(`call "${reducerName}" timed out after ${this.opts.callTimeout}ms`));
            }, this.opts.callTimeout);
            this.pendingCalls.set(callId, {
                resolve: (result) => {
                    clearTimeout(timer);
                    if (result.success) {
                        // On success the server will send subscription diffs that update
                        // serverBaseCache; remove the speculative layer so those diffs land
                        // cleanly without double-applying the optimistic change.
                        if (isOptimistic) {
                            this.removeOptimisticLayer(callId);
                        }
                        resolve(result.resultBytes);
                    }
                    else {
                        if (isOptimistic) {
                            this.removeOptimisticLayer(callId);
                            optimisticOpts?.onRollback?.(result.error ?? "Reducer returned an error", this.snapshotCache());
                        }
                        reject(new Error(result.error ?? "Reducer returned an error"));
                    }
                },
                reject: (err) => {
                    clearTimeout(timer);
                    if (isOptimistic) {
                        this.removeOptimisticLayer(callId);
                    }
                    reject(err);
                },
                timer,
                isOptimistic,
                onRollback: optimisticOpts?.onRollback,
            });
            this.send(frame);
        });
    }
    async openSocket() {
        const WS = await getWebSocketCtor();
        let opened = false;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        let ws;
        if (this.opts.apiKey) {
            try {
                ws = new WS(this.opts.url, {
                    headers: { Authorization: `Bearer ${this.opts.apiKey}` },
                });
            }
            catch {
                ws = new WS(this.opts.url);
            }
        }
        else {
            ws = new WS(this.opts.url);
        }
        ws.binaryType = "arraybuffer";
        this.ws = ws;
        return new Promise((resolve, reject) => {
            ws.onopen = () => {
                opened = true;
                // Reset backoff counter on successful connection.
                const wasReconnect = this.reconnectAttempts > 0;
                const attemptNumber = this.reconnectAttempts;
                this.reconnectAttempts = 0;
                resolve();
                this.onConnected?.();
                if (wasReconnect) {
                    this.opts.onReconnect?.(attemptNumber);
                }
                // Re-issue all active subscriptions so the server sends initial snapshots again.
                for (const [subId, entry] of this.subscriptions) {
                    this.send(encodeSubscribe(subId, entry.query));
                }
                // Flush any calls that were queued while we were disconnected.
                this.flushCallQueue();
            };
            ws.onclose = () => {
                this.onDisconnected?.();
                this.opts.onDisconnect?.();
                // Roll back and reject all in-flight calls (pitfall #21).
                this.rejectAllPending(new Error("Connection closed"));
                if (!opened) {
                    reject(new Error("Connection closed before it was established"));
                    // If this socket was created as a reconnect attempt (reconnectAttempts > 0),
                    // keep the reconnect loop alive even though the attempt failed to open.
                    if (!this.closed && this.reconnectCfg.enabled && this.reconnectAttempts > 0) {
                        this.scheduleReconnect();
                    }
                    return;
                }
                if (!this.closed && this.reconnectCfg.enabled) {
                    this.scheduleReconnect();
                }
                else if (!this.closed) {
                    // Reconnect disabled — drain the queue with an error.
                    this.drainCallQueue(new Error("Connection closed and reconnect is disabled"));
                }
            };
            ws.onerror = (_evt) => {
                if (!opened) {
                    reject(new Error("WebSocket error"));
                }
            };
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            ws.onmessage = (evt) => {
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                const data = evt?.data;
                if (data instanceof ArrayBuffer) {
                    this.handleFrame(data);
                }
                else if (ArrayBuffer.isView(data)) {
                    this.handleFrame(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
                }
            };
        });
    }
    // ── Reconnect logic ───────────────────────────────────────────────────────
    scheduleReconnect() {
        const { maxAttempts, baseDelayMs, maxDelayMs, jitter } = this.reconnectCfg;
        if (this.reconnectAttempts >= maxAttempts) {
            const err = new Error(`Reconnect exhausted after ${this.reconnectAttempts} attempt(s)`);
            this.opts.onReconnectFailed?.(err);
            this.drainCallQueue(err);
            return;
        }
        const delay = computeBackoffDelay(this.reconnectAttempts, baseDelayMs, maxDelayMs, jitter);
        this.reconnectAttempts++;
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            if (!this.closed) {
                void this.openSocket().catch(() => {
                    // openSocket will have called scheduleReconnect via the onclose handler
                    // if the connection attempt itself failed.
                });
            }
        }, delay);
    }
    /**
     * Flush the queued calls now that we are connected again.
     * Each queued item is dispatched as a fresh `dispatchCall()`.
     */
    flushCallQueue() {
        const queue = this.callQueue.splice(0);
        for (const item of queue) {
            void this.dispatchCall(item.reducerName, item.args, item.optimisticOpts)
                .then(item.resolve)
                .catch(item.reject);
        }
    }
    /**
     * Drain the call queue by rejecting every buffered call with `err`.
     */
    drainCallQueue(err) {
        const queue = this.callQueue.splice(0);
        for (const item of queue) {
            item.reject(err);
        }
    }
    // ── Frame handling ────────────────────────────────────────────────────────
    handleFrame(data) {
        const msg = decodeServerMessage(data);
        switch (msg.type) {
            case "ReducerResponse": {
                const pending = this.pendingCalls.get(msg.data.callId);
                if (pending) {
                    this.pendingCalls.delete(msg.data.callId);
                    pending.resolve(msg.data);
                }
                break;
            }
            case "SubscriptionAck":
                if (!msg.data.success) {
                    console.warn(`[Voltra] Subscription "${msg.data.subscriptionId}" failed: ${msg.data.message}`);
                }
                break;
            case "SubscriptionDiff": {
                const diff = msg.data;
                this.applyToCache(diff.tableName, diff.rowKey, diff.operation, diff.rowData);
                const entry = this.subscriptions.get(diff.subscriptionId);
                entry?.callback(diff);
                break;
            }
            case "SubscriptionRoute":
                this.pendingRoute = msg.data.subscriptionIds;
                break;
            case "SubscriptionBody": {
                const route = this.pendingRoute;
                this.pendingRoute = null;
                if (!route || route.length === 0)
                    break;
                for (const subscriptionId of route) {
                    const diff = {
                        subscriptionId,
                        tableName: msg.data.tableName,
                        rowKey: msg.data.rowKey,
                        operation: msg.data.operation,
                        rowData: msg.data.rowData,
                    };
                    this.applyToCache(diff.tableName, diff.rowKey, diff.operation, diff.rowData);
                    const entry = this.subscriptions.get(subscriptionId);
                    entry?.callback(diff);
                }
                break;
            }
            case "BatchUpdate": {
                for (const diff of msg.diffs) {
                    this.applyToCache(diff.tableName, diff.rowKey, diff.operation, diff.rowData);
                    const entry = this.subscriptions.get(diff.subscriptionId);
                    entry?.callback(diff);
                }
                break;
            }
            case "Error":
                this.onError?.(msg.message);
                break;
            case "Unknown":
                break;
        }
    }
    applyToCache(tableName, rowKey, operation, rowData) {
        // Always apply server-confirmed diffs to serverBaseCache.
        if (!this.serverBaseCache.has(tableName)) {
            this.serverBaseCache.set(tableName, new Map());
        }
        const baseTable = this.serverBaseCache.get(tableName);
        if (operation === "delete") {
            baseTable.delete(rowKey);
        }
        else if (operation === "patch" && rowData != null) {
            // Merge changed fields into existing row — preserve fields not in the patch.
            const existing = baseTable.get(rowKey) ?? {};
            baseTable.set(rowKey, { ...existing, ...rowData });
        }
        else if (rowData != null) {
            baseTable.set(rowKey, rowData);
        }
        // Recompute the speculative rowCache (serverBase + remaining layers).
        this.recomputeRowCache();
    }
    send(frame) {
        if (this.ws?.readyState === WebSocket.OPEN) {
            this.ws.send(frame);
        }
    }
    rejectAllPending(err) {
        for (const pending of this.pendingCalls.values()) {
            clearTimeout(pending.timer);
            pending.reject(err);
        }
        this.pendingCalls.clear();
        // Drop all speculative layers and recompute so rowCache reflects only
        // server-confirmed data (pitfall #21 — optimistic rollback on disconnect).
        if (this.optimisticLayers.length > 0) {
            this.optimisticLayers = [];
            this.recomputeRowCache();
        }
    }
}
//# sourceMappingURL=client.js.map