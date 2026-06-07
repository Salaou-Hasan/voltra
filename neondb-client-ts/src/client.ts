// ============================================================================
// NeonDB TypeScript Client SDK — NeonDBClient
// Session 31 — TODO-021: Optimistic updates
//   call(reducer, args, { optimistic }) applies a speculative cache update
//   immediately, then rolls back on server error.
// ============================================================================

import {
  encodeReducerCall,
  encodeSubscribe,
  encodeUnsubscribe,
  encodeArgs,
  decodeServerMessage,
  decodeResult,
} from "./protocol.js";
import type {
  NeonDBClientOptions,
  OptimisticOptions,
  OptimisticCache,
  ReducerResult,
  SubscriptionCallback,
  Subscription,
  RowCache,
} from "./types.js";

// Use native WebSocket in browsers; dynamically import 'ws' in Node.js.
async function getWebSocketCtor(): Promise<typeof WebSocket> {
  if (typeof globalThis.WebSocket !== "undefined") {
    return globalThis.WebSocket;
  }
  try {
    const mod = await import("ws");
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return ((mod as any).WebSocket ??
      (mod as any).default ??
      mod) as typeof WebSocket;
  } catch {
    throw new Error(
      "WebSocket is not available. In Node.js, install the 'ws' package: npm install ws",
    );
  }
}

/**
 * Pre-call value at a single (table, rowKey) coordinate.
 * `undefined` means "this row did not exist before the optimistic call".
 */
type RowSnapshot = Record<string, unknown> | undefined;

/**
 * Targeted rollback snapshot: only the (table, rowKey) pairs the optimistic
 * callback actually modified.  Maps `"table\x00rowKey"` → pre-call value.
 *
 * Using a single flat Map (rather than nested) keeps lookups O(1) and the
 * memory footprint proportional to the number of touched rows, not the cache.
 */
type TouchedRollback = Map<string, RowSnapshot>;

const ROLLBACK_KEY_SEP = "\x00";
function rkey(table: string, rowKey: string): string {
  return `${table}${ROLLBACK_KEY_SEP}${rowKey}`;
}
function unrkey(key: string): [string, string] {
  const idx = key.indexOf(ROLLBACK_KEY_SEP);
  return [key.slice(0, idx), key.slice(idx + 1)];
}

interface PendingCall {
  resolve: (result: ReducerResult) => void;
  reject: (err: Error) => void;
  timer: ReturnType<typeof setTimeout>;
  /**
   * Targeted rollback: only the (table, rowKey) pairs the optimistic call
   * mutated, with their pre-call values.  `null` if this is not an optimistic
   * call.  Rows NOT in this map are left untouched on rollback, so any
   * subscription diffs that arrived mid-flight are preserved.
   */
  rollbackTouched: TouchedRollback | null;
  /** User-supplied rollback callback (from OptimisticOptions). */
  onRollback?: OptimisticOptions["onRollback"];
}

interface SubEntry {
  query: string;
  callback: SubscriptionCallback;
}

export class NeonDBClient {
  private readonly opts: Required<NeonDBClientOptions>;
  private ws: WebSocket | null = null;
  private pendingCalls = new Map<number, PendingCall>();
  private subscriptions = new Map<string, SubEntry>();
  private rowCache = new Map<string, RowCache>(); // tableName → { rowKey → rowData }
  private nextCallId = 1;
  private nextSubId = 1;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;
  private pendingRoute: string[] | null = null;

  // ── Connection lifecycle events ───────────────────────────────────────────
  onConnected?: () => void;
  onDisconnected?: () => void;
  onError?: (message: string) => void;

  constructor(options: NeonDBClientOptions) {
    this.opts = {
      reconnectInterval: 3_000,
      callTimeout: 5_000,
      apiKey: "",
      ...options,
    };
  }

  // ── Connection ────────────────────────────────────────────────────────────

  connect(): Promise<void> {
    if (this.ws?.readyState === WebSocket.OPEN) {
      return Promise.resolve();
    }
    this.closed = false;
    return this.openSocket();
  }

  disconnect(): void {
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
  call(
    reducerName: string,
    args: unknown = [],
    optimisticOpts?: OptimisticOptions,
  ): Promise<Uint8Array | null> {
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
      let rollbackTouched: TouchedRollback | null = null;
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
          optimisticOpts?.onRollback?.(
            `call "${reducerName}" timed out`,
            this.snapshotCache(),
          );
        }
        reject(
          new Error(
            `call "${reducerName}" timed out after ${this.opts.callTimeout}ms`,
          ),
        );
      }, this.opts.callTimeout);

      this.pendingCalls.set(callId, {
        resolve: (result) => {
          clearTimeout(timer);
          if (result.success) {
            resolve(result.resultBytes);
          } else {
            // Server error: roll back ONLY the rows we touched.
            if (rollbackTouched !== null) {
              this.rollbackTouchedRows(rollbackTouched);
              optimisticOpts?.onRollback?.(
                result.error ?? "Reducer returned an error",
                this.snapshotCache(),
              );
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
  decodeResult<T = unknown>(bytes: Uint8Array): T {
    return decodeResult<T>(bytes);
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
  subscribe(query: string, callback: SubscriptionCallback): Subscription {
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
  getRows(tableName: string): RowCache {
    return this.rowCache.get(tableName) ?? new Map();
  }

  getRow(
    tableName: string,
    rowKey: string,
  ): Record<string, unknown> | undefined {
    return this.rowCache.get(tableName)?.get(rowKey);
  }

  // ── Status ────────────────────────────────────────────────────────────────

  isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  // ── Optimistic helpers ────────────────────────────────────────────────────

  /**
   * Deep-snapshot the current row cache into an OptimisticCache
   * (Map<tableName, Map<rowKey, rowData>>).  Used for handing the callback a
   * safe-to-mutate copy and for the `onRollback` payload.
   */
  private snapshotCache(): OptimisticCache {
    const snap: OptimisticCache = new Map();
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
  private applyTargetedOptimistic(
    proposed: OptimisticCache,
  ): TouchedRollback {
    const touched: TouchedRollback = new Map();

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
          this.rowCache.get(table)!.set(rowKey, newRow);
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
  private rollbackTouchedRows(touched: TouchedRollback): void {
    for (const [k, preValue] of touched) {
      const [table, rowKey] = unrkey(k);
      if (preValue === undefined) {
        // Row didn't exist before the call — delete it.
        this.rowCache.get(table)?.delete(rowKey);
      } else {
        if (!this.rowCache.has(table)) {
          this.rowCache.set(table, new Map());
        }
        this.rowCache.get(table)!.set(rowKey, preValue);
      }
    }
  }

  // ── Internal ──────────────────────────────────────────────────────────────

  private async openSocket(): Promise<void> {
    const WS = await getWebSocketCtor();
    let opened = false;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let ws: any;
    if (this.opts.apiKey) {
      try {
        ws = new (WS as any)(this.opts.url, {
          headers: { Authorization: `Bearer ${this.opts.apiKey}` },
        });
      } catch {
        ws = new WS(this.opts.url);
      }
    } else {
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

      ws.onerror = (_evt: Event) => {
        if (!opened) {
          reject(new Error("WebSocket error"));
        }
      };

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ws.onmessage = (evt: any) => {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const data: any = evt?.data;
        if (data instanceof ArrayBuffer) {
          this.handleFrame(data);
        } else if (ArrayBuffer.isView(data)) {
          this.handleFrame(
            new Uint8Array(data.buffer, data.byteOffset, data.byteLength),
          );
        }
      };
    });
  }

  private handleFrame(data: ArrayBuffer | Uint8Array): void {
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
          console.warn(
            `[NeonDB] Subscription "${msg.data.subscriptionId}" failed: ${msg.data.message}`,
          );
        }
        break;

      case "SubscriptionDiff": {
        const diff = msg.data;
        this.applyToCache(
          diff.tableName,
          diff.rowKey,
          diff.operation,
          diff.rowData,
        );
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
        if (!route || route.length === 0) break;
        for (const subscriptionId of route) {
          const diff = {
            subscriptionId,
            tableName: msg.data.tableName,
            rowKey: msg.data.rowKey,
            operation: msg.data.operation,
            rowData: msg.data.rowData,
          };
          this.applyToCache(
            diff.tableName,
            diff.rowKey,
            diff.operation,
            diff.rowData,
          );
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

  private applyToCache(
    tableName: string,
    rowKey: string,
    operation: string,
    rowData: Record<string, unknown> | null,
  ): void {
    if (!this.rowCache.has(tableName)) {
      this.rowCache.set(tableName, new Map());
    }
    const table = this.rowCache.get(tableName)!;
    if (operation === "delete") {
      table.delete(rowKey);
    } else if (rowData != null) {
      table.set(rowKey, rowData);
    }
  }

  private send(frame: Uint8Array): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(frame);
    }
  }

  private rejectAllPending(err: Error): void {
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
function rowsEqual(
  a: Record<string, unknown> | undefined,
  b: Record<string, unknown> | undefined,
): boolean {
  if (a === b) return true;
  if (a === undefined || b === undefined) return false;
  // Fast path: same number of keys and shallow identity per key.
  const aKeys = Object.keys(a);
  const bKeys = Object.keys(b);
  if (aKeys.length === bKeys.length) {
    let allShallowEqual = true;
    for (const k of aKeys) {
      if (!(k in b) || (a as Record<string, unknown>)[k] !== (b as Record<string, unknown>)[k]) {
        allShallowEqual = false;
        break;
      }
    }
    if (allShallowEqual) return true;
  }
  // Fallback: deep compare via stable JSON.
  return stableStringify(a) === stableStringify(b);
}

function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return "[" + value.map(stableStringify).join(",") + "]";
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return (
    "{" +
    keys.map((k) => JSON.stringify(k) + ":" + stableStringify(obj[k])).join(",") +
    "}"
  );
}
