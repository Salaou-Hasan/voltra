use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn increment(name: &str, delta: i32) -> String {
    let result = serde_json::json!({"name": name, "delta": delta});
    result.to_string()
}
