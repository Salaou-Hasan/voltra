// ============================================================================
// NeonDB TypeScript Client SDK — NeonDBClient
// Session 31 — TODO-021: Optimistic updates
//   call(reducer, args, { optimistic }) applies a speculative cache update
//   immediately, then rolls back on server error.
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
export class NeonDBClient {
    opts;
    ws = null;
    pendingCalls = new Map();
    subscriptions = new Map();
    rowCache = new Map(); // tableName → { rowKey → rowData }
    nextCallId = 1;
    nextSubId = 1;
    reconnectTimer = null;
    closed = false;
    pendingRoute = null;
    // ── Connection lifecycle events ───────────────────────────────────────────
    onConnected;
    onDisconnected;
    onError;
    constructor(options) {
        this.opts = {
            reconnectInterval: 3_000,
            callTimeout: 5_000,
            apiKey: "",
            ...options,
        };
    }
    // ── Connection ────────────────────────────────────────────────────────────
    connect() {
        if (this.ws?.readyState === WebSocket.OPEN) {
            return Promise.resolve();
        }
        this.closed = false;
        return this.openSocket();
    }
    disconnect() {
        this.closed = true;
        if (this.reconnectTimer != null) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        this.ws?.close();
        this.ws = null;
        this.rejectAllPending(new Error("Client disconnected"));
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
     * @returns Raw result bytes, or `null` if the call succeeded with no result.
     * @throws  If the reducer returned an error or the call timed out.
     */
    call(reducerName, args = [], optimisticOpts) {
        return new Promise((resolve, reject) => {
            if (!this.isConnected()) {
                reject(new Error("Not connected"));
                return;
            }
            const callId = this.nextCallId++;
            const encodedArgs = encodeArgs(args);
            const frame = encodeReducerCall(callId, reducerName, encodedArgs);
            // ── Optimistic: snapshot + apply before sending ──────────────────────
            let rollbackSnapshot = null;
            if (optimisticOpts?.optimistic) {
                // Deep-clone the current cache so rollback can restore it exactly.
                rollbackSnapshot = this.snapshotCache();
                // Apply the speculative state.
                const newCache = optimisticOpts.optimistic(rollbackSnapshot);
                this.applyOptimisticCache(newCache);
            }
            const timer = setTimeout(() => {
                this.pendingCalls.delete(callId);
                // Timeout: roll back if we made an optimistic update.
                if (rollbackSnapshot !== null) {
                    this.applyOptimisticCache(rollbackSnapshot);
                    optimisticOpts?.onRollback?.(`call "${reducerName}" timed out`, this.snapshotCache());
                }
                reject(new Error(`call "${reducerName}" timed out after ${this.opts.callTimeout}ms`));
            }, this.opts.callTimeout);
            this.pendingCalls.set(callId, {
                resolve: (result) => {
                    clearTimeout(timer);
                    if (result.success) {
                        resolve(result.resultBytes);
                    }
                    else {
                        // Server error: roll back optimistic cache if present.
                        if (rollbackSnapshot !== null) {
                            this.applyOptimisticCache(rollbackSnapshot);
                            optimisticOpts?.onRollback?.(result.error ?? "Reducer returned an error", this.snapshotCache());
                        }
                        reject(new Error(result.error ?? "Reducer returned an error"));
                    }
                },
                reject: (err) => {
                    clearTimeout(timer);
                    if (rollbackSnapshot !== null) {
                        this.applyOptimisticCache(rollbackSnapshot);
                    }
                    reject(err);
                },
                timer,
                rollbackSnapshot,
                onRollback: optimisticOpts?.onRollback,
            });
            this.send(frame);
        });
    }
    /**
     * Decode MessagePack result bytes into a JavaScript value.
     */
    decodeResult(bytes) {
        return decodeResult(bytes);
    }
    // ── Subscriptions ─────────────────────────────────────────────────────────
    /**
     * Subscribe to a NeonDB table (with an optional WHERE predicate).
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
     * Deep-snapshot the current row cache into an OptimisticCache
     * (Map<tableName, Map<rowKey, rowData>>).
     */
    snapshotCache() {
        const snap = new Map();
        for (const [table, rows] of this.rowCache) {
            snap.set(table, new Map(rows));
        }
        return snap;
    }
    /**
     * Replace the live rowCache with the contents of an OptimisticCache.
     * Used both to apply speculative states and to restore rollback snapshots.
     */
    applyOptimisticCache(cache) {
        this.rowCache.clear();
        for (const [table, rows] of cache) {
            this.rowCache.set(table, new Map(rows));
        }
    }
    // ── Internal ──────────────────────────────────────────────────────────────
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
                resolve();
                this.onConnected?.();
                for (const [subId, entry] of this.subscriptions) {
                    this.send(encodeSubscribe(subId, entry.query));
                }
            };
            ws.onclose = () => {
                this.onDisconnected?.();
                this.rejectAllPending(new Error("Connection closed"));
                if (!opened) {
                    reject(new Error("Connection closed before it was established"));
                    return;
                }
                if (!this.closed && this.opts.reconnectInterval > 0) {
                    this.reconnectTimer = setTimeout(() => {
                        void this.openSocket();
                    }, this.opts.reconnectInterval);
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
                    console.warn(`[NeonDB] Subscription "${msg.data.subscriptionId}" failed: ${msg.data.message}`);
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
            case "Error":
                this.onError?.(msg.message);
                break;
            case "Unknown":
                break;
        }
    }
    applyToCache(tableName, rowKey, operation, rowData) {
        if (!this.rowCache.has(tableName)) {
            this.rowCache.set(tableName, new Map());
        }
        const table = this.rowCache.get(tableName);
        if (operation === "delete") {
            table.delete(rowKey);
        }
        else if (rowData != null) {
            table.set(rowKey, rowData);
        }
    }
    send(frame) {
        if (this.ws?.readyState === WebSocket.OPEN) {
            this.ws.send(frame);
        }
    }
    rejectAllPending(err) {
        for (const pending of this.pendingCalls.values()) {
            clearTimeout(pending.timer);
            // Roll back any in-flight optimistic updates on disconnect.
            if (pending.rollbackSnapshot !== null) {
                this.applyOptimisticCache(pending.rollbackSnapshot);
            }
            pending.reject(err);
        }
        this.pendingCalls.clear();
    }
}
//# sourceMappingURL=client.js.map