/**
 * increment_js.js — Voltra JS reducer module (Boa engine)
 *
 * Host globals injected by Voltra:
 *   __voltra_get(table, key) -> { id, name, value } | null
 *   __voltra_set(table, key, value)  -> void
 *
 * The function must be named `reducer` and accept a single args object.
 * It should return a plain JS object; Voltra re-encodes it as MessagePack.
 */
function reducer(args) {
  var name  = args.name;
  var delta = args.delta;

  // Read current counter value (default 0 if not found)
  var current = __voltra_get("counters", name);
  var value   = (current && typeof current.value === "number") ? current.value : 0;

  // Apply delta
  value += delta;

  // Persist the new value
  __voltra_set("counters", name, value);

  return {
    new_value: value,
    timestamp: 0   // Boa does not expose Date.now(); use 0 or pass timestamp in args
  };
}
