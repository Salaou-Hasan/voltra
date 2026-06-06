import type { NeonDBClientOptions, OptimisticOptions, SubscriptionCallback, Subscription, RowCache } from "./types.js";
export declare class NeonDBClient {
    private readonly opts;
    private ws;
    private pendingCalls;
    private subscriptions;
    private rowCache;
    private nextCallId;
    private nextSubId;
    private reconnectTimer;
    private closed;
    private pendingRoute;
    onConnected?: () => void;
    onDisconnected?: () => void;
    onError?: (message: string) => void;
    constructor(options: NeonDBClientOptions);
    connect(): Promise<void>;
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
     * (Map<tableName, Map<rowKey, rowData>>).
     */
    private snapshotCache;
    /**
     * Replace the live rowCache with the contents of an OptimisticCache.
     * Used both to apply speculative states and to restore rollback snapshots.
     */
    private applyOptimisticCache;
    private openSocket;
    private handleFrame;
    private applyToCache;
    private send;
    private rejectAllPending;
}
//# sourceMappingURL=client.d.ts.map