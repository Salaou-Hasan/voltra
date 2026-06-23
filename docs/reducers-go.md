# Writing Reducers in Go (TODO-033)

Voltra supports Go reducers compiled to WebAssembly via **TinyGo** (wasm32-wasi target).
Standard `go build` will NOT produce a correct WASM module — always use TinyGo.

## Prerequisites

```sh
# Install TinyGo  https://tinygo.org/getting-started/install/
tinygo version   # verify: should print "tinygo version 0.28+" or later
```

## Quick Start

```sh
voltra init my-game --template go-reducers
cd my-game
voltra build   # tinygo build -target wasi
voltra start
voltra call attack '["player1", "enemy1", 25]'
```

## Project Structure

```
reducers/
├── go.mod              ← Go module definition
├── voltra/
│   └── voltra.go       ← host-function bindings
└── combat.go           ← your reducers
modules/
└── *.wasm              ← compiled output (written by voltra build)
```

## Writing a Reducer

```go
package main

import (
    "encoding/json"
    "math"
    "unsafe"
    "voltra"
)

//export attack
func attack(argsPtr int32, argsLen int32) (int32, int32) {
    args := (*[1 << 30]byte)(unsafe.Pointer(uintptr(argsPtr)))[:argsLen:argsLen]

    var params []json.RawMessage
    json.Unmarshal(args, &params)

    var targetID string
    var damage int
    json.Unmarshal(params[1], &targetID)
    json.Unmarshal(params[2], &damage)

    rowBytes := voltra.Get("players", targetID)
    if rowBytes == nil {
        b, _ := json.Marshal(map[string]interface{}{"error": "not found"})
        return voltra.WriteResult(b)
    }

    var row map[string]interface{}
    json.Unmarshal(rowBytes, &row)
    currentHP := int(row["hp"].(float64))
    newHP := int(math.Max(0, float64(currentHP-damage)))
    row["hp"] = newHP
    row["alive"] = newHP > 0

    updated, _ := json.Marshal(row)
    voltra.Set("players", targetID, updated)

    result, _ := json.Marshal(map[string]interface{}{"ok": true, "new_hp": newHP})
    return voltra.WriteResult(result)
}

func main() {} // required by TinyGo wasi target
```

## Return Convention

TinyGo correctly exports multi-value WASM returns. The Voltra backend
expects `(result_ptr i32, result_len i32)`. Use `voltra.WriteResult([]byte)`
which writes data to a static buffer and returns the correct (ptr, len) pair.

## Host API Reference

| Function | Signature | Description |
|----------|-----------|-------------|
| `voltra.Get(table, key string)` | `[]byte` | Returns row JSON or nil |
| `voltra.Set(table, key string, val []byte)` | — | Write row JSON |
| `voltra.Delete(table, key string)` | — | Delete row |
| `voltra.CallerID()` | `string` | Client ID |
| `voltra.CallerRole()` | `string` | Client role |
| `voltra.WriteResult([]byte)` | `(int32, int32)` | Pack result for WASM return |

## TinyGo Limitations

| Available | Not Available |
|-----------|---------------|
| `encoding/json` | `net/http` |
| `math`, `strings`, `strconv` | `database/sql` |
| `unsafe` | reflection-heavy packages |
| `sync.Mutex` | goroutines (single-threaded WASM) |

## Troubleshooting

| Error | Fix |
|-------|-----|
| `tinygo: command not found` | Install TinyGo from https://tinygo.org |
| Build output missing exported function | Check `//export funcname` comment is correct |
| `go.mod` not found | Run `voltra build` from the project root (where `reducers/` lives) |
| Segfault in `ptrToSlice` | Ensure `argsLen <= actual slice capacity` |
