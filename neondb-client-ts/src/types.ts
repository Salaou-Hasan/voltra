// ============================================================================
// NeonDB TypeScript Client SDK — Type Definitions
// ============================================================================

/** Options passed to NeonDBClient constructor. */
export interface NeonDBClientOptions {
  /** WebSocket URL, e.g. "ws://localhost:3000" or "wss://db.yourgame.com" */
  url: string;
  /**
   * Optional API key.  Sent as `Authorization: Bearer <key>` in the
   * WebSocket upgrade headers (Node.js only — browsers cannot set custom
   * WebSocket upgrade headers).
   */
  apiKey?: string;
  /**
   * Milliseconds between auto-reconnect attempts.
   * Set to 0 to disable.  Default: 3000.
   */
  reconnectInterval?: number;
  /**
   * Milliseconds before a `call()` promise is rejected with a timeout.
   * Default: 5000.
   */
  callTimeout?: number;
}

// ── Incoming message types ────────────────────────────────────────────────────

export interface ReducerResult {
  callId: number;
  success: boolean;
  resultBytes: Uint8Array | null;
  error: string | null;
}

export interface SubscriptionAck {
  subscriptionId: string;
  success: boolean;
  message: string | null;
}

/**
 * A single row change delivered to a subscriber.
 * `operation` is one of: `"insert"`, `"update"`, `"delete"`, `"initial_snapshot"`.
 */
export interface RowDiff {
  subscriptionId: string;
  tableName: string;
  rowKey: string;
  operation: string;
  rowData: Record<string, unknown> | null;
}

/**
 * Two-frame protocol — route frame.
 * Lists the subscription IDs that the immediately following SubscriptionBody applies to.
 */
export interface SubscriptionRouteData {
  subscriptionIds: string[];
}

/**
 * Two-frame protocol — body frame.
 * The delta body shared across all subscribers listed in the preceding SubscriptionRoute.
 */
export interface SubscriptionBodyData {
  tableName: string;
  rowKey: string;
  operation: string;
  rowData: Record<string, unknown> | null;
}

export type SubscriptionCallback = (diff: RowDiff) => void;

export interface Subscription {
  id: string;
  unsubscribe: () => void;
}

/** Cached rows for a single table, keyed by row_key. */
export type RowCache = Map<string, Record<string, unknown>>;

// ── Optimistic updates ────────────────────────────────────────────────────────

/**
 * A snapshot of the full client-side row cache for ALL tables.
 * Passed to `optimistic` so it can produce a speculative updated state.
 */
export type OptimisticCache = Map<string, RowCache>;

/**
 * Options for optimistic call() invocations.
 *
 * Usage:
 * ```ts
 * await client.call("move_player", { x: 5, y: 3 }, {
 *   optimistic: (cache) => {
 *     const players = cache.get("players") ?? new Map();
 *     players.set("alice", { ...players.get("alice"), x: 5, y: 3 });
 *     return new Map([...cache, ["players", players]]);
 *   },
 * });
 * ```
 *
 * The callback receives the current cache and MUST return a NEW Map
 * representing the optimistically updated state.  The client immediately
 * updates its local cache with the returned value so that any UI reading
 * `getRows()` / `getRow()` sees the change before the server responds.
 *
 * On server success: the server's subscription diffs will reconcile the cache.
 * On server error: the cache is automatically rolled back to the pre-call state.
 */
export interface OptimisticOptions {
  /**
   * Pure function: receives current cache snapshot, returns speculative state.
   * Must not mutate the argument; return a new Map.
   */
  optimistic: (cache: OptimisticCache) => OptimisticCache;
  /**
   * Optional callback invoked if the server rejects the call AND the
   * optimistic update has been rolled back.
   * @param error   The server error string.
   * @param rolled  The cache state after rollback (same as before the call).
   */
  onRollback?: (error: string, rolledBack: OptimisticCache) => void;
}
