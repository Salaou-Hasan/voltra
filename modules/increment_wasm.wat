;; increment_wasm.wat — Voltra WASM reducer module
;;
;; Host imports (provided by Voltra at runtime):
;;   env.voltra_get_counter(name_ptr: i32, name_len: i32) -> i32
;;   env.voltra_set_counter(name_ptr: i32, name_len: i32, value: i32)
;;
;; Exported entry-point:
;;   reducer(args_ptr: i32, args_len: i32) -> (result_ptr: i32, result_len: i32)
;;
;; This module increments the "score" counter by 1 each call and returns
;; a JSON string {"new_value":<n>,"timestamp":0} written into linear memory.
;;
;; Memory layout:
;;   [0..28]   result JSON (written at runtime)
;;   [512..516] counter name "score" (5 bytes)
;;
;; IMPORTANT: WebAssembly spec requires ALL (import ...) declarations to
;; appear BEFORE any (memory ...), (table ...), or (func ...) definitions.
;; Violating this order causes a WAT parse error "import after memory".

(module
  ;; Host imports MUST come first — before memory and func definitions.
  (import "env" "voltra_get_counter" (func $get_counter (param i32 i32) (result i32)))
  (import "env" "voltra_set_counter" (func $set_counter (param i32 i32 i32)))

  (memory (export "memory") 1)

  ;; Counter name "score" at offset 512, length 5
  (data (i32.const 512) "score")

  ;; Static JSON result template at offset 0
  ;; {"new_value":1,"timestamp":0}  — 29 bytes
  (data (i32.const 0) "{\"new_value\":1,\"timestamp\":0}")

  (func (export "reducer") (param $args_ptr i32) (param $args_len i32)
                            (result i32 i32)
    (local $cur i32)
    (local $new_val i32)

    ;; Read current value of "score"
    (local.set $cur
      (call $get_counter (i32.const 512) (i32.const 5)))

    ;; new_val = cur + 1
    (local.set $new_val
      (i32.add (local.get $cur) (i32.const 1)))

    ;; Write new value back
    (call $set_counter
      (i32.const 512) (i32.const 5)
      (local.get $new_val))

    ;; Return pointer and length of the static result template.
    (i32.const 0)
    (i32.const 29)
  )
)
