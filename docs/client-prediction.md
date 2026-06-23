# Client-Side Prediction Guide

This guide explains how to implement client-side prediction with Voltra's
sequence numbering and AOI broadcast system. Client-side prediction makes
movement feel instant by applying inputs locally before the server confirms
them, then reconciling when the authoritative state arrives.

---

## Why Client-Side Prediction?

Without prediction, every player action follows this path:

```
press key → send to server → wait for response → render new position
```

At 128Hz server tick + network latency, that's 8ms + RTT/2 of visible delay.
With prediction:

```
press key → render immediately → send to server → reconcile on response
```

The player sees their own movement instantly. Other players' movements are
interpolated between known server ticks.

---

## Protocol Fields

Voltra's protocol includes two fields for prediction support:

### `ReducerCall.sequence`

Client-assigned monotonically increasing number. Server echoes it back in
`ReducerResponse.sequence` so the client knows which input was processed.

```json
{
  "ReducerCall": {
    "call_id": 12345,
    "reducer_name": "move_player",
    "args": ["l0_p42", 5.0, 10.0],
    "sequence": 7
  }
}
```

### `ReducerResponse.server_tick`

The server's tick number when this response was generated. Clients use this
for entity interpolation — rendering positions between known ticks for smooth
movement of OTHER players.

```json
{
  "ReducerResponse": {
    "call_id": 12345,
    "success": true,
    "result": "...",
    "sequence": 7,
    "server_tick": 1024
  }
}
```

### `AoiDelta.server_tick`

Every AOI broadcast delta includes the server tick it was generated at.
Clients use this to know which interpolation window each entity is in.

---

## Implementation

### Step 1: Track Sequences

```typescript
class PredictionState {
  private nextSequence = 0;
  private pendingInputs: Map<number, Input> = new Map();
  private lastAckedSequence = 0;
  private lastServerTick = 0;

  // Called before sending a reducer call
  prepareInput(input: Input): number {
    const seq = ++this.nextSequence;
    this.pendingInputs.set(seq, input);
    return seq;
  }

  // Called when ReducerResponse arrives
  onServerAck(response: ReducerResponse): Input | null {
    if (response.sequence == null) return null;
    this.lastAckedSequence = response.sequence;
    if (response.server_tick) {
      this.lastServerTick = response.server_tick;
    }
    const input = this.pendingInputs.get(response.sequence);
    this.pendingInputs.delete(response.sequence);
    return input ?? null;
  }

  // Get all unacknowledged inputs (for re-prediction after rollback)
  getPendingInputs(): Input[] {
    const inputs: Input[] = [];
    for (const [seq, input] of this.pendingInputs) {
      inputs.push({ ...input, sequence: seq });
    }
    return inputs.sort((a, b) => a.sequence - b.sequence);
  }
}
```

### Step 2: Apply Input Locally

```typescript
// Player presses W → move forward
function onInput(direction: Vector2) {
  const seq = prediction.prepareInput({
    type: "move",
    dx: direction.x * SPEED * TICK_DT,
    dy: direction.y * SPEED * TICK_DT,
  });

  // Apply immediately — player sees movement without delay
  localPlayer.x += direction.x * SPEED * TICK_DT;
  localPlayer.y += direction.y * SPEED * TICK_DT;
  localPlayer.lastInputSeq = seq;

  // Send to server
  client.call("move_player", [localPlayer.id, localPlayer.x, localPlayer.y], {
    sequence: seq,
  });
}
```

### Step 3: Reconcile on Server Response

When the server responds, it knows the authoritative position. If the
player's prediction was correct (no collisions, no other modifiers), the
server position matches. If something changed (hit a wall, took damage),
the client must correct.

```typescript
function onServerResponse(response: ReducerResponse) {
  const input = prediction.onServerAck(response);
  if (!input) return;

  // Decode server-authoritative state from response
  const serverState = decodeResult(response.result);

  // Compare prediction vs server
  const predicted = localPlayer; // current predicted position
  const error = {
    x: predicted.x - serverState.x,
    y: predicted.y - serverState.y,
  };

  const ERROR_THRESHOLD = 0.1; // tolerance in world units

  if (Math.abs(error.x) > ERROR_THRESHOLD || Math.abs(error.y) > ERROR_THRESHOLD) {
    // Prediction was wrong — snap to server position
    localPlayer.x = serverState.x;
    localPlayer.y = serverState.y;

    // Replay all inputs that arrived AFTER this one
    for (const pendingInput of prediction.getPendingInputs()) {
      localPlayer.x += pendingInput.dx;
      localPlayer.y += pendingInput.dy;
    }
  }
  // If within threshold, keep predicted position (smooth, no correction)
}
```

### Step 4: Interpolate Other Players

Other players' positions arrive via AOI broadcast deltas at the server's
tick rate. Interpolate between the last two known positions for smooth
rendering.

```typescript
interface RemoteEntity {
  id: number;
  // Last two known positions from server
  prevPos: Vector2;
  prevTick: number;
  currPos: Vector2;
  currTick: number;
  // Interpolated render position
  renderPos: Vector2;
}

function onAoiDelta(delta: AoiDelta) {
  const entity = remoteEntities.get(delta.entity_id);
  if (!entity) return;

  // Shift current → previous, store new
  entity.prevPos = entity.currPos;
  entity.prevTick = entity.currTick;
  entity.currPos = { x: delta.x, y: delta.y };
  entity.currTick = delta.server_tick ?? 0;
}

// Called every client frame (e.g. 60Hz or 144Hz)
function interpolateRemoteEntities(clientTick: number) {
  const TICK_DURATION_MS = 1000 / SERVER_TICK_RATE; // e.g. 7.8ms at 128Hz

  for (const entity of remoteEntities.values()) {
    const elapsed = (clientTick - entity.currTick) * TICK_DURATION_MS;
    const alpha = Math.min(elapsed / TICK_DURATION_MS, 1.0);

    entity.renderPos = {
      x: lerp(entity.prevPos.x, entity.currPos.x, alpha),
      y: lerp(entity.prevPos.y, entity.currPos.y, alpha),
    };
  }
}

function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * t;
}
```

### Step 5: Full Game Loop

```typescript
// WebSocket message handler
ws.onmessage = (event) => {
  const msg = decodeServerMessage(event.data);

  if ("ReducerResponse" in msg) {
    onServerResponse(msg.ReducerResponse);
  }
  if ("AoiDelta" in msg) {
    onAoiDelta(msg.AoiDelta);
  }
};

// Render loop (requestAnimationFrame or fixed timestep)
function gameLoop(timestamp: number) {
  // 1. Process queued inputs
  processInputQueue();

  // 2. Interpolate remote entities
  interpolateRemoteEntities(timestamp);

  // 3. Render everything
  render(localPlayer, remoteEntities);

  requestAnimationFrame(gameLoop);
}
```

---

## Complete TypeScript Example

```typescript
import { VoltraClient } from "voltra-client";

const client = new VoltraClient({ url: "ws://localhost:3000" });
await client.connect();

// Subscribe to AOI updates for our lobby
client.subscribe("aoi", "__aoi WHERE lobby = 'l0'");

// Prediction state
let seq = 0;
const pending = new Map<number, { x: number; y: number }>();
let playerX = 0, playerY = 0;
const SPEED = 5; // units per tick

// Handle keyboard input
document.addEventListener("keydown", (e) => {
  let dx = 0, dy = 0;
  if (e.key === "w") dy = -SPEED;
  if (e.key === "s") dy = SPEED;
  if (e.key === "a") dx = -SPEED;
  if (e.key === "d") dx = SPEED;
  if (dx === 0 && dy === 0) return;

  // Predict locally
  playerX += dx;
  playerY += dy;

  // Track for reconciliation
  seq++;
  pending.set(seq, { x: playerX, y: playerY });

  // Send to server with sequence
  client.call("move_player", ["l0_p1", playerX, playerY]);
});

// Handle server responses
client.onMessage((msg) => {
  if (msg.type === "ReducerResponse" && msg.sequence) {
    const predicted = pending.get(msg.sequence);
    pending.delete(msg.sequence);

    if (predicted && msg.success) {
      const server = JSON.parse(msg.result);
      // If prediction was wrong, snap to server state
      if (Math.abs(predicted.x - server.x) > 0.1 ||
          Math.abs(predicted.y - server.y) > 0.1) {
        playerX = server.x;
        playerY = server.y;
      }
    }
  }

  // Handle AOI updates for other players
  if (msg.type === "AoiDelta") {
    updateRemotePlayer(msg.entity_id, msg.x, msg.y, msg.server_tick);
  }
});
```

---

## Tradeoffs

| Approach | Pros | Cons |
|----------|------|------|
| **No prediction** | Simple, always correct | Input lag = RTT/2 + tick delay |
| **Prediction + reconciliation** | Instant self-movement | Correction flicker on desync |
| **Prediction + interpolation** | Smooth for all entities | Slightly stale for others |

For competitive games, use **prediction + reconciliation** for the local
player and **interpolation** for remote entities. This is what CS2, Valorant,
and Fortnite all use.

---

## Server Tick Rate Recommendations

| Game Type | Recommended Tick Rate | Why |
|-----------|----------------------|-----|
| MMO / Social | 20Hz | Low CPU, high player count |
| Battle Royale | 30-64Hz | Balance of cost and responsiveness |
| Tactical FPS | 64-128Hz | Hit registration precision |
| Fighting game | 60Hz (lockstep) | Deterministic simulation |

Set via `VOLTRA_LOBBY_TICK_HZ` or `[server] lobby_tick_hz` in voltra.toml.
