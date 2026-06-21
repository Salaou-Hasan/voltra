/**
 * Voltra React Hooks — TODO-019
 *
 * Provides type-safe React hooks for real-time data subscriptions and reducer
 * calls, built on top of the VoltraClient SDK.
 *
 * Usage:
 *
 *   // 1. Wrap your app with VoltraProvider
 *   import { VoltraProvider } from "@voltra/client/hooks";
 *
 *   <VoltraProvider url="ws://localhost:3000" apiKey="optional">
 *     <App />
 *   </VoltraProvider>
 *
 *   // 2. Subscribe to live data in any component
 *   import { useVoltraQuery, useVoltraReducer } from "@voltra/client/hooks";
 *
 *   function Leaderboard() {
 *     const { rows, loading, error } = useVoltraQuery("scores");
 *     if (loading) return <p>Loading…</p>;
 *     if (error)   return <p>Error: {error.message}</p>;
 *     return <ul>{[...rows.values()].map(r => <li key={r.player_id}>{r.player_id}: {r.score}</li>)}</ul>;
 *   }
 *
 *   // 3. Call reducers from any component
 *   function SubmitButton({ playerId }: { playerId: string }) {
 *     const [submit, { loading, error }] = useVoltraReducer("submit_score");
 *     return (
 *       <button onClick={() => submit([playerId, 100])} disabled={loading}>
 *         {loading ? "Submitting…" : "Submit Score"}
 *       </button>
 *     );
 *   }
 *
 * Compatible with React 18 (concurrent mode safe — no tearing, stable refs).
 */

// ── React dependency note ─────────────────────────────────────────────────────
//
// React is a peer dependency. Install it separately:
//   npm install react react-dom
//   npm install --save-dev @types/react @types/react-dom
//
// This file uses `import type` for React types to avoid bundling React itself.
// The actual React functions are imported at runtime from the peer dependency.

import {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  useCallback,
  type ReactNode,
  type Context,
} from "react";
import { VoltraClient } from "./client.js";
import type { VoltraClientOptions, RowCache } from "./types.js";

// ── Types ─────────────────────────────────────────────────────────────────────

export interface VoltraProviderProps extends VoltraClientOptions {
  children: ReactNode;
}

export interface QueryState<T = Record<string, unknown>> {
  /** Live row data keyed by row_key. Updates on every subscription diff. */
  rows: Map<string, T>;
  /** True while the initial snapshot is being delivered. */
  loading: boolean;
  /** Set if the subscription failed to register. */
  error: Error | null;
}

export interface ReducerCallState {
  /** True while the reducer call is in-flight. */
  loading: boolean;
  /** Set if the most recent call failed. Cleared on the next call. */
  error: Error | null;
  /** The raw result bytes from the most recent successful call. */
  lastResult: Uint8Array | null;
}

export type UseReducerReturn = [
  /** Fire the reducer. Pass args as a positional array or object. */
  call: (args?: unknown) => Promise<Uint8Array | null>,
  state: ReducerCallState,
];

// ── Context ───────────────────────────────────────────────────────────────────

// We use `unknown` for the context value type to avoid React version coupling.
// The actual value is `VoltraClient | null`.
const VoltraContext: Context<VoltraClient | null> = createContext<VoltraClient | null>(null);

// ── VoltraProvider ────────────────────────────────────────────────────────────

/**
 * Wrap your app (or a subtree) with this provider.
 * Creates one shared VoltraClient for all child hooks.
 *
 * ```tsx
 * <VoltraProvider url="ws://localhost:3000">
 *   <App />
 * </VoltraProvider>
 * ```
 */
export function VoltraProvider({ children, ...opts }: VoltraProviderProps) {
  // Stable client ref — we never recreate the client on re-render.
  const clientRef = useRef<VoltraClient | null>(null);

  if (clientRef.current === null) {
    clientRef.current = new VoltraClient(opts);
  }

  useEffect(() => {
    const client = clientRef.current!;
    let cancelled = false;

    client.connect().catch((err) => {
      if (!cancelled) {
        console.error("[Voltra] Provider failed to connect:", err);
      }
    });

    return () => {
      cancelled = true;
      client.disconnect();
    };
    // opts.url and opts.apiKey intentionally excluded — client is stable.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <VoltraContext.Provider value={clientRef.current}>
      {children}
    </VoltraContext.Provider>
  );
}

// ── useVoltra ─────────────────────────────────────────────────────────────────

/**
 * Access the shared VoltraClient.
 * Must be called inside a `<VoltraProvider>` tree.
 *
 * ```ts
 * const client = useVoltra();
 * ```
 */
export function useVoltra(): VoltraClient {
  const client = useContext(VoltraContext);
  if (client === null) {
    throw new Error(
      "useVoltra must be called inside a <VoltraProvider>. " +
      "Wrap your app with <VoltraProvider url=\"ws://…\">.",
    );
  }
  return client;
}

// ── useVoltraQuery ────────────────────────────────────────────────────────────

/**
 * Subscribe to a Voltra table query and get live-updating rows.
 *
 * ```tsx
 * const { rows, loading, error } = useVoltraQuery("scores");
 * const { rows } = useVoltraQuery("players WHERE level > 5");
 * const { rows } = useVoltraQuery<Player>("players WHERE zone = 'zone_0_0'");
 * ```
 *
 * - `rows` is a `Map<rowKey, rowData>` that updates on every diff.
 * - `loading` is true until the initial snapshot completes.
 * - `error` is set if the subscription was rejected by the server.
 * - The subscription is automatically cleaned up when the component unmounts.
 * - If `query` changes, the old subscription is cleaned up and a new one opens.
 */
export function useVoltraQuery<T = Record<string, unknown>>(
  query: string,
): QueryState<T> {
  const client = useVoltra();

  const [state, setState] = useState<QueryState<T>>({
    rows: new Map(),
    loading: true,
    error: null,
  });

  // Stable ref to accumulate the initial snapshot without triggering extra renders.
  const rowsRef = useRef<Map<string, T>>(new Map());
  const snapshotCompleteRef = useRef(false);

  useEffect(() => {
    // Reset state for new query
    rowsRef.current = new Map();
    snapshotCompleteRef.current = false;
    setState({ rows: new Map(), loading: true, error: null });

    const sub = client.subscribe(query, (diff) => {
      const { operation, rowKey, rowData } = diff;

      if (operation === "initial_snapshot") {
        // Buffer snapshot rows without re-rendering on every row.
        if (rowData != null) {
          rowsRef.current.set(rowKey, rowData as T);
        }
        return;
      }

      if (operation === "initial_snapshot_complete" || !snapshotCompleteRef.current) {
        // First non-snapshot diff signals the snapshot is done.
        snapshotCompleteRef.current = true;

        if (operation !== "initial_snapshot_complete") {
          // Apply this diff on top of the snapshot.
          applyDiff(rowsRef.current, rowKey, operation, rowData as T | null);
        }

        // Flush snapshot + first live diff together — single render.
        setState({
          rows: new Map(rowsRef.current),
          loading: false,
          error: null,
        });
        return;
      }

      // Steady-state live diff.
      applyDiff(rowsRef.current, rowKey, operation, rowData as T | null);
      setState((prev) => ({
        rows: new Map(rowsRef.current),
        loading: prev.loading,
        error: prev.error,
      }));
    });

    // Mark snapshot complete after a short timeout in case the server doesn't
    // send a "initial_snapshot_complete" message (servers that send an empty table
    // may never send a non-snapshot diff).
    const snapshotTimeout = setTimeout(() => {
      if (!snapshotCompleteRef.current) {
        snapshotCompleteRef.current = true;
        setState({
          rows: new Map(rowsRef.current),
          loading: false,
          error: null,
        });
      }
    }, 2_000);

    return () => {
      clearTimeout(snapshotTimeout);
      sub.unsubscribe();
    };
  }, [client, query]);

  return state;
}

function applyDiff<T>(
  rows: Map<string, T>,
  rowKey: string,
  operation: string,
  rowData: T | null,
): void {
  if (operation === "delete") {
    rows.delete(rowKey);
  } else if (rowData != null) {
    rows.set(rowKey, rowData);
  }
}

// ── useVoltraReducer ──────────────────────────────────────────────────────────

/**
 * Call a Voltra reducer from a component.
 *
 * ```tsx
 * const [submit, { loading, error }] = useVoltraReducer("submit_score");
 *
 * return (
 *   <button onClick={() => submit(["alice", 1500])} disabled={loading}>
 *     {loading ? "Submitting…" : "Submit"}
 *   </button>
 * );
 * ```
 *
 * - `loading` is true while the call is in-flight.
 * - `error` is set on failure and cleared on the next call.
 * - `lastResult` holds the raw MessagePack result bytes from the last success.
 * - Safe to call in concurrent mode — no setState after unmount.
 */
export function useVoltraReducer(reducerName: string): UseReducerReturn {
  const client = useVoltra();
  const mountedRef = useRef(true);

  const [state, setState] = useState<ReducerCallState>({
    loading: false,
    error: null,
    lastResult: null,
  });

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const call = useCallback(
    async (args: unknown = []): Promise<Uint8Array | null> => {
      if (!mountedRef.current) return null;

      setState({ loading: true, error: null, lastResult: null });

      try {
        const result = await client.call(reducerName, args);
        if (mountedRef.current) {
          setState({ loading: false, error: null, lastResult: result ?? null });
        }
        return result;
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err));
        if (mountedRef.current) {
          setState({ loading: false, error, lastResult: null });
        }
        throw error;
      }
    },
    [client, reducerName],
  );

  return [call, state];
}

// ── useVoltraClient (escape hatch) ────────────────────────────────────────────

/**
 * Access the raw VoltraClient for advanced use cases.
 *
 * ```ts
 * const client = useVoltraClient();
 * const result = await client.call("my_reducer", { key: "value" });
 * ```
 */
export const useVoltraClient = useVoltra;
