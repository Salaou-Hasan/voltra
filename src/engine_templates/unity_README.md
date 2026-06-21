# Voltra + Unity

Single-file C# client for Voltra — no packages, no DLLs.

## Setup (60 seconds)

1. Start your Voltra server: `voltra start` (templates: `voltra init --template rust/game-ready`).
2. Copy `VoltraClient.cs` and `VoltraBehaviour.cs` into `Assets/Scripts/Voltra/`.
3. Add the **VoltraBehaviour** component to a GameObject and set the URL
   (default `ws://127.0.0.1:3000`).

## Calling reducers

```csharp
using Voltra;

public class Player : MonoBehaviour
{
    public VoltraBehaviour voltra;

    async void Start()
    {
        voltra.OnReady += async () =>
        {
            var r = await voltra.Client.Call("spawn",
                new object[] { "player1", 0, 0, "warrior" });
            Debug.Log($"spawn ok={r.Success} result={r.Result}");
        };
    }

    async void Move(float x, float y)
    {
        await voltra.Client.Call("move", new object[] { "player1", x, y });
    }
}
```

## Live subscriptions (lobby state sync)

```csharp
voltra.Client.Subscribe("players WHERE lobby = 'l42'", diff =>
{
    // Runs on the Unity main thread (safe to touch GameObjects).
    // diff.Op: "initial_snapshot" | "set" | "delete"
    var row = diff.Data as Dictionary<string, object>;
    if (diff.Op == "delete") RemovePlayer(diff.RowKey);
    else UpdatePlayer(diff.RowKey, row);
});
```

The server coalesces updates at 20Hz by default (`VOLTRA_SUB_TICK_MS`),
so one busy row costs each subscriber at most 20 frames/second.

## Notes

- **Auth**: set the `apiKey` field on VoltraBehaviour (sent as `Bearer`).
- **WebGL**: `System.Net.WebSockets` is unavailable in WebGL builds; use a
  JS bridge plugin (e.g. unity-webgl-websocket) and swap the transport in
  `VoltraClient.Connect/SendRaw/ReadLoop`.
- **Reconnect**: `OnDisconnected` fires on drop; call `Connect()` again and
  re-issue your subscriptions.
