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
