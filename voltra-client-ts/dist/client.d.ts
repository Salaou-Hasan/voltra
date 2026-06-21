import type { NeonDBClientOptions, OptimisticOptions, SubscriptionCallback, Subscription, RowCache } from "./types.js";
/**
 * Compute exponential-backoff delay for `attempt` (0-based).
 * Formula: min(maxDelayMs, baseDelayMs * 2^attempt) ± 25% jitter.
 */
export declare function computeBackoffDelay(attempt: number, baseDelayMs: number, maxDelayMs: number, jitter: boolean): number;
export declare class NeonDBClient {
    private readonly opts;
    private readonly reconnectCfg;
    private ws;
    private pendingCalls;
    private subscriptions;
    private rowCache;
    private nextCallId;
    private nextSubId;
    private reconnectTimer;
    private closed;
    private pendingRoute;
    /** Number of consecutive failed reconnect attempts so far. */
    private reconnectAttempts;
    /**
     * Calls issued while the socket was down.  Flushed (in order) immediately
     * after the next successful reconnect.
     */
    private callQueue;
    onConnected?: () => void;
    onDisconnected?: () => void;
    onError?: (message: string) => void;
    constructor(options: NeonDBClientOptions);
    connect(): Promise<void>;
    /**
     * Gracefully disconnect.  Sets `userInitiatedClose` so no reconnect fires.
     * Rejects all pending (in-flight) calls immediately.
     */
    disconnect(): void;
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
    call(reducerName: string, args?: unknown, optimisticOpts?: OptimisticOptions): Promise<Uint8Array | null>;
    /**
     * Decode MessagePack result bytes into a JavaScript value.
     */
    decodeResult<T = unknown>(bytes: Uint8Array): T;
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
    subscribe(query: string, callback: SubscriptionCallback): Subscription;
    /**
     * Return the client-side row cache for a table.
     * Reflects both server-confirmed diffs and any in-flight optimistic updates.
     */
    getRows(tableName: string): RowCache;
    getRow(tableName: string, rowKey: string): Record<string, unknown> | undefined;
    isConnected(): boolean;
    /**
     * Deep-snapshot the current row cache into an OptimisticCache
     * (Map<tableName, Map<rowKey, rowData>>).  Used for handing the callback a
     * safe-to-mutate copy and for the `onRollback` payload.
     */
    private snapshotCache;
    /**
     * Compare `proposed` against the live `rowCache`, find the (table, rowKey)
     * coordinates that DIFFER, snapshot their pre-call values, then apply the
     * proposed value at each one.  Returns the targeted rollback snapshot.
     */
    private applyTargetedOptimistic;
    /**
     * Restore every (table, rowKey) pair recorded in `touched` to its pre-call
     * value.  Rows NOT in `touched` are left at whatever value they hold right
     * now — this is what preserves subscription diffs that arrived mid-flight.
     */
    private rollbackTouchedRows;
    /**
     * Core call dispatch — assumes the socket IS currently open.
     */
    private dispatchCall;
    private openSocket;
    private scheduleReconnect;
    /**
     * Flush the queued calls now that we are connected again.
     * Each queued item is dispatched as a fresh `dispatchCall()`.
     */
    private flushCallQueue;
    /**
     * Drain the call queue by rejecting every buffered call with `err`.
     */
    private drainCallQueue;
    private handleFrame;
    private applyToCache;
    private send;
    private rejectAllPending;
}
//# sourceMappingURL=client.d.ts.map