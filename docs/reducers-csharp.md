# Writing Reducers in C# (TODO-032)

NeonDB supports C# reducers compiled to WebAssembly via the **.NET 8 WASI workload**.
The resulting `.wasm` is loaded and executed by the existing Wasmtime backend —
no changes to the server are needed.

## Prerequisites

```sh
# .NET 8 SDK
# https://dotnet.microsoft.com/download

# WASI experimental workload (one-time)
dotnet workload install wasi-experimental
```

## Quick Start

```sh
neondb init my-game --template csharp-reducers
cd my-game
neondb build   # dotnet publish -r wasi-wasm
neondb start
neondb call attack '["player1", "enemy1", 25]'
```

## Project structure

```
reducers/
├── Reducers.csproj     ← .NET 8 WASI project
├── NeonDB.cs           ← host-function bindings
└── Combat.cs           ← your reducers
modules/
└── *.wasm              ← compiled output (written by neondb build)
```

## Writing a Reducer

```csharp
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using NeonDB;

public static class Combat
{
    [UnmanagedCallersOnly(EntryPoint = "attack")]
    public static unsafe long Attack(int argsPtr, int argsLen)
    {
        var argsSpan = new ReadOnlySpan<byte>((void*)argsPtr, argsLen);
        using var doc = JsonDocument.Parse(argsSpan);
        var root = doc.RootElement;
        string targetId = root[1].GetString()!;
        int damage = root[2].GetInt32();

        var target = ReducerContext.Get("players", targetId);
        if (target is null)
            return ReducerContext.Return(
                JsonSerializer.SerializeToUtf8Bytes(new { error = "not found" }));

        int newHp = Math.Max(0, (target["hp"]?.GetValue<int>() ?? 0) - damage);
        target["hp"] = JsonValue.Create(newHp);
        target["alive"] = JsonValue.Create(newHp > 0);
        ReducerContext.Set("players", targetId, target);

        return ReducerContext.Return(
            JsonSerializer.SerializeToUtf8Bytes(new { ok = true, new_hp = newHp }));
    }

    public static void Main() { }   // required by .NET WASI
}
```

## Return Convention

C# `[UnmanagedCallersOnly]` cannot return multiple WASM values. NeonDB uses
an **i64 fat-pointer** encoding instead:

```
high 32 bits = pointer to result JSON in linear memory
low  32 bits = byte length of result
```

Use `ReducerContext.Return(byte[])` — it handles the encoding automatically.

## Host API Reference

| Method | Description |
|--------|-------------|
| `ReducerContext.Get(table, key)` | Returns `JsonObject?` |
| `ReducerContext.Set(table, key, row)` | Write a row |
| `ReducerContext.Delete(table, key)` | Delete a row |
| `ReducerContext.CallerID()` | Client ID string |
| `ReducerContext.CallerRole()` | Client role string |
| `ReducerContext.Return(bytes)` | Pack result for WASM return |
| `ReducerContext.ReturnOk()` | Return `{"ok":true}` |

## .csproj Recommended Settings

```xml
<PropertyGroup>
  <TargetFramework>net8.0</TargetFramework>
  <RuntimeIdentifier>wasi-wasm</RuntimeIdentifier>
  <OutputType>Exe</OutputType>
  <!-- Keep binary small -->
  <InvariantGlobalization>true</InvariantGlobalization>
  <PublishTrimmed>true</PublishTrimmed>
  <AllowUnsafeBlocks>true</AllowUnsafeBlocks>
</PropertyGroup>
```

## Troubleshooting

| Error | Fix |
|-------|-----|
| `dotnet workload install wasi-experimental` fails | Update .NET SDK to 8.x |
| `.wasm` size > 10 MB | Add `<PublishTrimmed>true</PublishTrimmed>` |
| `neondb build` skips C# step | Ensure `reducers/*.csproj` exists |
| Host function not found at runtime | Check `[DllImport("env", EntryPoint = "neondb_...")]` spelling |
