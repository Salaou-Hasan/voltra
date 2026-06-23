# Voltra + Godot 4

Single-file GDScript client for Voltra — no addons, no GDExtension.

## Setup (60 seconds)

1. Start your Voltra server: `voltra start` (templates: `voltra init --template rust/game-ready`).
2. Copy `voltra_client.gd` into your project.
3. Project Settings → **Autoload** → add `voltra_client.gd` as `Voltra`.

## Calling reducers

```gdscript
func _ready() -> void:
    Voltra.connect_to("ws://127.0.0.1:3000")
    await Voltra.connected

    var r = await Voltra.call_reducer("spawn", ["player1", 0, 0, "warrior"])
    print("spawn ok=", r.success, " data=", r.data)

func move(x: float, y: float) -> void:
    Voltra.call_reducer("move", ["player1", x, y])
```

## Live subscriptions (lobby state sync)

```gdscript
func _ready() -> void:
    Voltra.connect_to("ws://127.0.0.1:3000")
    await Voltra.connected
    Voltra.row_update.connect(_on_row)
    Voltra.subscribe("players WHERE lobby = 'l42'")

func _on_row(table: String, key: String, op: String, data) -> void:
    # op: "initial_snapshot" | "set" | "delete"
    if op == "delete":
        remove_player(key)
    else:
        update_player(key, data)  # data is a Dictionary
```

The server coalesces updates at 20Hz by default (`VOLTRA_SUB_TICK_MS`),
so one busy row costs each subscriber at most 20 frames/second.

## Notes

- **Auth**: `Voltra.connect_to(url, "your-api-key")` sends it as `Bearer`.
- **Reconnect**: listen to the `disconnected` signal, call `connect_to`
  again, and re-issue subscriptions.
- Works on all Godot 4 export targets including HTML5 (WebSocketPeer is
  built in everywhere).
