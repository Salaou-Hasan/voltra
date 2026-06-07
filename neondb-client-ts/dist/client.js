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
const ROLLBACK_KEY_SEP = "\x00";
function rkey(table, rowKey) {
    return `${table}${ROLLBACK_KEY_SEP}${rowKey}`;
}
function unrkey(key) {
    const idx = key.indexOf(ROLLBACK_KEY_SEP);
    return [key.slice(0, idx), key.slice(idx + 1)];
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
            // ── Optimistic: targeted snapshot + apply before sending ─────────────
            //
            // The previous implementation snapshotted the ENTIRE rowCache and
            // restored it on rollback.  That has a race: any subscription diff
            // arriving between send and error would be wiped by the rollback.
            //
            // Instead, we now:
            //   1. Hand the callback a deep clone of the current cache.
            //   2. Diff the returned cache against the current cache to find the
            //      exact (table, rowKey) coordinates the callback mutated.
            //   3. Snapshot only those pre-call values into `rollbackTouched`.
            //   4. Apply the new values at those coordinates in the live cache,
            //      leaving every other row untouched.
            //   5. On rollback: restore each touched coordinate to its pre-call
            //      value; rows not in the touched set keep whatever value they
            //      hold right now (so mid-flight subscription diffs are preserved).
            let rollbackTouched = null;
            if (optimisticOpts?.optimistic) {
                const preCacheClone = this.snapshotCache();
                const proposed = optimisticOpts.optimistic(preCacheClone);
                rollbackTouched = this.applyTargetedOptimistic(proposed);
            }
            const timer = setTimeout(() => {
                this.pendingCalls.delete(callId);
                // Timeout: roll back if we made an optimistic update.
                if (rollbackTouched !== null) {
                    this.rollbackTouchedRows(rollbackTouched);
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
                        // Server error: roll back ONLY the rows we touched.
                        if (rollbackTouched !== null) {
                            this.rollbackTouchedRows(rollbackTouched);
                            optimisticOpts?.onRollback?.(result.error ?? "Reducer returned an error", this.snapshotCache());
                        }
                        reject(new Error(result.error ?? "Reducer returned an error"));
                    }
                },
                reject: (err) => {
                    clearTimeout(timer);
                    if (rollbackTouched !== null) {
                        this.rollbackTouchedRows(rollbackTouched);
                    }
                    reject(err);
                },
                timer,
                rollbackTouched,
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
     * (Map<tableName, Map<rowKey, rowData>>).  Used for handing the callback a
     * safe-to-mutate copy and for the `onRollback` payload.
     */
    snapshotCache() {
        const snap = new Map();
        for (const [table, rows] of this.rowCache) {
            snap.set(table, new Map(rows));
        }
        return snap;
    }
    /**
     * Compare `proposed` against the live `rowCache`, find the (table, rowKey)
     * coordinates that DIFFER, snapshot their pre-call values, then apply the
     * proposed value at each one.  Returns the targeted rollback snapshot.
     *
     * A coordinate is "touched" if any of:
     *   - it exists in proposed but not in liveCache (an insert)
     *   - it exists in liveCache but not in proposed (a delete)
     *   - both exist but the row data is not referentially identical AND not
     *     deeply equal (an update)
     *
     * NOTE: deep equality here is JSON-string based — fast enough for the
     * typical small game row, and avoids false-positive rollbacks when the
     * callback re-clones an unchanged row.
     */
    applyTargetedOptimistic(proposed) {
        const touched = new Map();
        // 1. Walk the proposed cache to find inserts and updates.
        for (const [table, proposedRows] of proposed) {
            const liveRows = this.rowCache.get(table);
            for (const [rowKey, newRow] of proposedRows) {
                const preValue = liveRows?.get(rowKey);
                if (!rowsEqual(preValue, newRow)) {
                    touched.set(rkey(table, rowKey), preValue);
                    // Apply the new value.
                    if (!this.rowCache.has(table)) {
                        this.rowCache.set(table, new Map());
                    }
                    this.rowCache.get(table).set(rowKey, newRow);
                }
            }
        }
        // 2. Walk the live cache to find deletes (rows present live, absent in proposed).
        for (const [table, liveRows] of this.rowCache) {
            const proposedRows = proposed.get(table);
            for (const rowKey of liveRows.keys()) {
                if (!proposedRows || !proposedRows.has(rowKey)) {
                    const preValue = liveRows.get(rowKey);
                    const k = rkey(table, rowKey);
                    // Skip if we already recorded this coordinate above (defensive).
                    if (!touched.has(k)) {
                        touched.set(k, preValue);
                    }
                }
            }
        }
        // 3. Apply deletes (do this after recording so we don't lose pre-values).
        for (const [k] of touched) {
            const [table, rowKey] = unrkey(k);
            const proposedRows = proposed.get(table);
            if (!proposedRows || !proposedRows.has(rowKey)) {
                this.rowCache.get(table)?.delete(rowKey);
            }
        }
        return touched;
    }
    /**
     * Restore every (table, rowKey) pair recorded in `touched` to its pre-call
     * value.  Rows NOT in `touched` are left at whatever value they hold right
     * now — this is what preserves subscription diffs that arrived mid-flight.
     */
    rollbackTouchedRows(touched) {
        for (const [k, preValue] of touched) {
            const [table, rowKey] = unrkey(k);
            if (preValue === undefined) {
                // Row didn't exist before the call — delete it.
                this.rowCache.get(table)?.delete(rowKey);
            }
            else {
                if (!this.rowCache.has(table)) {
                    this.rowCache.set(table, new Map());
                }
                this.rowCache.get(table).set(rowKey, preValue);
            }
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
            // Roll back any in-flight optimistic updates on disconnect — but only
            // the rows each call actually touched.
            if (pending.rollbackTouched !== null) {
                this.rollbackTouchedRows(pending.rollbackTouched);
            }
            pending.reject(err);
        }
        this.pendingCalls.clear();
    }
}
/**
 * Shallow-then-deep equality for row data.  Identical references short-circuit
 * to true; otherwise we compare via JSON.stringify with stable key ordering.
 *
 * `undefined` vs anything-defined is treated as unequal (the touched-set logic
 * uses `undefined` to mean "did not exist").
 */
function rowsEqual(a, b) {
    if (a === b)
        return true;
    if (a === undefined || b === undefined)
        return false;
    // Fast path: same number of keys and shallow identity per key.
    const aKeys = Object.keys(a);
    const bKeys = Object.keys(b);
    if (aKeys.length === bKeys.length) {
        let allShallowEqual = true;
        for (const k of aKeys) {
            if (!(k in b) || a[k] !== b[k]) {
                allShallowEqual = false;
                break;
            }
        }
        if (allShallowEqual)
            return true;
    }
    // Fallback: deep compare via stable JSON.
    return stableStringify(a) === stableStringify(b);
}
function stableStringify(value) {
    if (value === null || typeof value !== "object") {
        return JSON.stringify(value);
    }
    if (Array.isArray(value)) {
        return "[" + value.map(stableStringify).join(",") + "]";
    }
    const obj = value;
    const keys = Object.keys(obj).sort();
    return ("{" +
        keys.map((k) => JSON.stringify(k) + ":" + stableStringify(obj[k])).join(",") +
        "}");
}
//# sourceMappingURL=client.js.map