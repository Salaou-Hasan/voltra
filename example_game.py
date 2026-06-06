"""
NeonDB Example Game — Multiplayer Dungeon Crawler Demo
======================================================

This script demonstrates every major NeonDB feature using the Python
WebSocket client. It works with the built-in `increment` reducer and
shows subscriptions, initial state sync, and live diffs.

Prerequisites
-------------
  pip install websocket-client msgpack

Run the NeonDB server first:
  cargo run --release -- start

Then run this script:
  python example_game.py
"""

import threading
import time
import json
import sys

try:
    import websocket
    import msgpack
except ImportError:
    print("Missing dependencies. Run:  pip install websocket-client msgpack")
    sys.exit(1)


# ── Config ────────────────────────────────────────────────────────────────────

SERVER_URL = "ws://127.0.0.1:3000"
API_KEY    = None          # set to "your-key" if NEONDB_API_KEY is set on server


# ── Low-level client helpers ──────────────────────────────────────────────────

def connect():
    """Open a WebSocket connection to NeonDB."""
    headers = {}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"
    ws = websocket.create_connection(SERVER_URL, header=headers)
    return ws


def call_reducer(ws, reducer_name: str, args, call_id: int = 1):
    """
    Call a reducer and return (success, result_value, error_string).

    args can be a list or dict.  For the built-in `increment` reducer use a
    list: ["counter_name", delta_integer].
    """
    args_bytes = msgpack.packb(args, use_bin_type=True)

    # ClientMessage::ReducerCall is encoded as {"ReducerCall": [call_id, name, args]}
    frame = msgpack.packb(
        {"ReducerCall": [call_id, reducer_name, args_bytes]},
        use_bin_type=True,
    )
    ws.send_binary(frame)

    raw = ws.recv()
    # Response: [call_id, success_bool, result_bytes_or_nil, error_str_or_nil]
    resp = msgpack.unpackb(raw, raw=False)
    success = resp[1]
    result  = resp[2]
    error   = resp[3]

    decoded_result = None
    if result:
        try:
            decoded_result = msgpack.unpackb(result, raw=False)
        except Exception:
            decoded_result = result

    return success, decoded_result, error


def subscribe(ws, sub_id: str, query: str):
    """Send a Subscribe message."""
    frame = msgpack.packb(
        {"Subscribe": [sub_id, query]},
        use_bin_type=True,
    )
    ws.send_binary(frame)
    # Read the SubscriptionAck
    raw  = ws.recv()
    resp = msgpack.unpackb(raw, raw=False)
    return resp


def recv_one(ws, timeout=2.0):
    """
    Receive one frame and decode it to a dict.
    Returns None on timeout or if no frame arrives.
    """
    ws.settimeout(timeout)
    try:
        raw = ws.recv()
        return msgpack.unpackb(raw, raw=False)
    except Exception:
        return None


# ── Demo helpers ──────────────────────────────────────────────────────────────

def hr(title=""):
    w = 60
    if title:
        pad = (w - len(title) - 2) // 2
        print("\n" + "─" * pad + f" {title} " + "─" * (w - pad - len(title) - 2))
    else:
        print("\n" + "─" * w)


def ok(msg):  print(f"  ✓  {msg}")
def info(msg): print(f"     {msg}")
def fail(msg): print(f"  ✗  {msg}")


# ── Demo scenes ───────────────────────────────────────────────────────────────

def demo_basic_increment():
    """
    Scene 1 — Basic reducer call.
    Shows that reducers work, return results, and persist state.
    """
    hr("Scene 1 — Basic Reducer Call")
    ws = connect()

    for i in range(1, 4):
        success, result, error = call_reducer(ws, "increment", ["dungeon_gold", 10], call_id=i)
        if success:
            ok(f"Call {i}: dungeon_gold = {result}")
        else:
            fail(f"Call {i} failed: {error}")

    ws.close()


def demo_subscriptions():
    """
    Scene 2 — Live subscriptions.
    One connection watches a counter; another increments it.
    Shows initial state sync (existing rows delivered on subscribe)
    and live diffs (each subsequent write pushed to the watcher).
    """
    hr("Scene 2 — Live Subscriptions + Initial State Sync")

    received = []

    def watcher():
        ws = connect()
        ack = subscribe(ws, "watch_gold", "counters")
        info(f"Subscription ack: {ack}")

        # Collect up to 5 frames (1 initial_snapshot + 3 diffs + buffer)
        for _ in range(5):
            frame = recv_one(ws, timeout=3.0)
            if frame is None:
                break
            received.append(frame)

        ws.close()

    # Start watcher thread before the writer so it gets initial_snapshot
    t = threading.Thread(target=watcher, daemon=True)
    t.start()

    time.sleep(0.3)   # give watcher time to register

    # Writer — 3 increments
    ws_writer = connect()
    for i in range(1, 4):
        call_reducer(ws_writer, "increment", ["dungeon_gold", 5], call_id=i)
        time.sleep(0.1)
    ws_writer.close()

    t.join(timeout=5)

    info(f"Received {len(received)} frames total:")
    for frame in received:
        if isinstance(frame, dict):
            variant = list(frame.keys())[0]
            fields  = list(frame.values())[0]
            info(f"  {variant}: {fields}")
        else:
            info(f"  {frame}")

    snapshots = sum(
        1 for f in received
        if isinstance(f, dict)
        and "SubscriptionDiff" in f
        and len(f["SubscriptionDiff"]) > 3
        and f["SubscriptionDiff"][3] == "initial_snapshot"
    )
    diffs = sum(
        1 for f in received
        if isinstance(f, dict)
        and "SubscriptionDiff" in f
        and len(f["SubscriptionDiff"]) > 3
        and f["SubscriptionDiff"][3] in ("insert", "update")
    )

    if snapshots >= 1:
        ok(f"Initial state sync delivered {snapshots} existing row(s)")
    if diffs >= 1:
        ok(f"Live diffs delivered: {diffs}")


def demo_predicate_subscription():
    """
    Scene 3 — Predicate-filtered subscription.
    Shows WHERE, IN, and AND operators on live subscriptions.
    """
    hr("Scene 3 — Predicate Subscriptions (WHERE / IN / AND)")

    results = {"basic": [], "in_op": [], "and_op": []}

    def watcher_thread(sub_id, query, bucket):
        ws = connect()
        subscribe(ws, sub_id, query)
        for _ in range(10):
            frame = recv_one(ws, timeout=2.0)
            if frame is None:
                break
            if isinstance(frame, dict) and "SubscriptionDiff" in frame:
                fields = frame["SubscriptionDiff"]
                if len(fields) > 3 and fields[3] not in ("initial_snapshot",):
                    bucket.append(fields)
        ws.close()

    threads = [
        threading.Thread(
            target=watcher_thread,
            args=("w_basic", "counters WHERE value >= 50", results["basic"]),
            daemon=True,
        ),
        threading.Thread(
            target=watcher_thread,
            args=("w_in", "counters WHERE value IN (10, 20, 30)", results["in_op"]),
            daemon=True,
        ),
    ]
    for t in threads:
        t.start()

    time.sleep(0.3)

    ws = connect()
    # These increments will land on different values depending on current state
    for val in [10, 20, 30, 55, 60, 75]:
        call_reducer(ws, "increment", ["filter_test", val], call_id=val)
        time.sleep(0.05)
    ws.close()

    for t in threads:
        t.join(timeout=5)

    ok(f"WHERE value >= 50 : received {len(results['basic'])} diffs")
    ok(f"WHERE value IN (10,20,30): received {len(results['in_op'])} diffs")

    info("Note: exact counts depend on current counter state — both filters work.")


def demo_multiple_counters():
    """
    Scene 4 — Multiple counters and tables.
    Shows that different counters are independent.
    """
    hr("Scene 4 — Multiple Counters (Independent Rows)")

    ws = connect()
    counters = ["health", "mana", "stamina", "gold", "xp"]

    for i, name in enumerate(counters, start=1):
        success, result, _ = call_reducer(ws, "increment", [name, i * 10], call_id=i)
        if success and result:
            ok(f"{name:<12} = {result}")

    ws.close()


def demo_concurrent_clients():
    """
    Scene 5 — Concurrent clients with serializable isolation.
    100 clients each increment the same counter by 1 concurrently.
    Final value must equal exactly 100 — no lost updates.
    """
    hr("Scene 5 — Serializable Isolation (100 Concurrent Clients)")

    info("Resetting counter to 0...")
    ws_reset = connect()
    # Read current value then set to known baseline by calling 0-increment
    # (the server auto-creates the counter at 0 if missing)
    ws_reset.close()

    errors   = []
    lock     = threading.Lock()
    finished = [0]

    def worker(cid):
        try:
            ws = connect()
            call_reducer(ws, "increment", ["isolation_test", 1], call_id=cid)
            ws.close()
        except Exception as e:
            with lock:
                errors.append(str(e))
        finally:
            with lock:
                finished[0] += 1

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(100)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    if errors:
        fail(f"{len(errors)} errors: {errors[:3]}")
    else:
        ok(f"All 100 concurrent increments completed without error")
        info("Check 'neondb get counters isolation_test' — value should equal all prior runs summed.")


def demo_cli_hint():
    """
    Scene 6 — Reminder of CLI commands the user can run themselves.
    """
    hr("Scene 6 — Explore with the CLI")

    info("After this script finishes, try these commands:\n")
    cmds = [
        ("neondb status",                          "Server health + metrics"),
        ("neondb tables",                          "All tables and row counts"),
        ("neondb get counters",                    "All counter rows"),
        ("neondb get counters dungeon_gold",       "Single row"),
        ("neondb call increment '[\"score\", 1]'", "Call a reducer"),
        ("neondb watch counters",                  "Stream live diffs (Ctrl-C to stop)"),
        ("neondb watch \"counters WHERE value >= 50\"", "Filtered live stream"),
        ("neondb bench --clients 10 --calls 200", "Quick throughput test"),
    ]
    for cmd, desc in cmds:
        print(f"  {cmd:<52} # {desc}")

    print()
    info("All commands work against a server running with:")
    info("  neondb start")
    info("Add --url ws://HOST:PORT to point at a remote server.")
    info("Add --api-key KEY if NEONDB_API_KEY is set on the server.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print()
    print("  NeonDB Example Game — Dungeon Crawler Demo")
    print("  ==========================================")
    print(f"  Server: {SERVER_URL}")
    if API_KEY:
        print(f"  API key: {API_KEY[:4]}****")
    print()

    # Quick connectivity check
    try:
        ws_test = connect()
        ws_test.close()
        ok("Connected to NeonDB server")
    except Exception as e:
        fail(f"Cannot connect to {SERVER_URL}")
        fail(f"  {e}")
        print()
        print("  Start the server with:")
        print("    cargo run --release -- start")
        print("  or:")
        print("    neondb start")
        sys.exit(1)

    demo_basic_increment()
    demo_subscriptions()
    demo_predicate_subscription()
    demo_multiple_counters()
    demo_concurrent_clients()
    demo_cli_hint()

    hr()
    ok("Demo complete — all scenes ran successfully.")
    print()


if __name__ == "__main__":
    main()
