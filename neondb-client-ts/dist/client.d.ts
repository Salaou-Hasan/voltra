import type { NeonDBClientOptions, SubscriptionCallback, Subscription, RowCache } from "./types.js";
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
    /** Fired when the WebSocket connection is opened (or re-opened). */
    onConnected?: () => void;
    /** Fired when the connection is closed. */
    onDisconnected?: () => void;
    /** Fired when an unhandled server error message arrives. */
    onError?: (message: string) => void;
    constructor(options: NeonDBClientOptions);
    /**
     * Open the WebSocket connection.
     * Resolves when the connection is ready to use.
     *
     * In Node.js the API key is sent as an HTTP header.
     * In browsers the API key cannot be sent as a header — connect to a server
     * without an API key, or route through a proxy that adds the header.
     */
    connect(): Promise<void>;
    /** Close the connection and stop auto-reconnect. */
    disconnect(): void;
    /**
     * Call a reducer and return the raw result bytes.
     *
     * For the built-in `increment` reducer, pass args as a positional array
     * matching the Rust struct field order:
     * ```ts
     * const bytes = await client.call("increment", ["score", 1]);
     * const result = client.decodeResult(bytes!); // { new_value: 5, timestamp: … }
     * ```
     *
     * For JS/WASM reducers that accept an object:
     * ```ts
     * await client.call("myReducer", { key: "value" });
     * ```
     *
     * @returns Raw result bytes (MessagePack-encoded), or `null` if the call
     *          succeeded with no result.
     * @throws if the reducer returned an error or the call timed out.
     */
    call(reducerName: string, args?: unknown): Promise<Uint8Array | null>;
    /**
     * Decode MessagePack result bytes into a JavaScript value.
     * Convenience wrapper around the protocol `decodeResult` helper.
     */
    decodeResult<T = unknown>(bytes: Uint8Array): T;
    /**
     * Subscribe to a NeonDB table (with an optional WHERE predicate).
     *
     * ```ts
     * const sub = client.subscribe("players WHERE level > 5", (diff) => {
     *   console.log(diff.operation, diff.rowKey, diff.rowData);
     * });
     *
     * // Later:
     * sub.unsubscribe();
     * ```
     *
     * Supported predicates:
     *   - Single field:  `WHERE score >= 100`
     *   - IN operator:   `WHERE status IN ('active', 'pending')`
     *   - AND compound:  `WHERE score > 100 AND level > 5`
     *
     * The `"initial_snapshot"` operation is delivered for each row that
     * already exists in the table at subscription time.
     */
    subscribe(query: string, callback: SubscriptionCallback): Subscription;
    /**
     * Return the client-side row cache for a table.
     * The cache is populated by subscription diffs (including initial snapshots).
     *
     * @returns A `Map<rowKey, rowData>` snapshot.  Returns an empty map if no
     *          subscription has delivered data for this table yet.
     */
    getRows(tableName: string): RowCache;
    /** Return a single cached row, or `undefined` if not present. */
    getRow(tableName: string, rowKey: string): Record<string, unknown> | undefined;
    isConnected(): boolean;
    private openSocket;
    private handleFrame;
    private applyToCache;
    private send;
    private rejectAllPending;
}
//# sourceMappingURL=client.d.ts.map