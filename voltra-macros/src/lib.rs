//! Procedural macros for Voltra.
//!
//! These are re-exported through the `voltra` crate — users should import via
//! `voltra::reducer` / `voltra::table` rather than depending on this crate
//! directly.

extern crate proc_macro;

mod reducer;
mod table;

use proc_macro::TokenStream;

/// Marks a function as a Voltra native reducer.
///
/// The **first** parameter must be the reducer context. Any type annotation on
/// it is accepted but ignored — the macro always binds it as
/// `&mut ::voltra::reducer::context::ReducerContext`.  Name it `ctx` and use
/// it freely in the body.
///
/// All **remaining** parameters are deserialized positionally from the
/// MessagePack-encoded `args` bytes that the client sends.
///
/// Use [`ret!`] to return a JSON result.  If the function body falls through
/// without calling `ret!`, the reducer returns `{"ok": true}`.
///
/// # Example
///
/// ```rust,ignore
/// use voltra::reducer;
///
/// #[reducer]
/// fn heal(ctx: Ctx, target_id: String, amount: i32) {
///     let row = ctx.get("players", &target_id)?
///         .unwrap_or_else(|| serde_json::json!({"hp": 0}));
///     let hp = row["hp"].as_i64().unwrap_or(0) as i32 + amount;
///     ctx.set("players", &target_id, serde_json::json!({ "hp": hp }))?;
///     ret!({ "ok": true, "new_hp": hp })
/// }
/// ```
#[proc_macro_attribute]
pub fn reducer(_attr: TokenStream, item: TokenStream) -> TokenStream {
    reducer::expand(item)
}

/// Marks a struct as a Voltra table row type.
///
/// Derives `Serialize + Deserialize` and generates:
/// - `fn table_name() -> &'static str`
/// - `fn from_json(v: serde_json::Value) -> Option<Self>`
/// - `fn to_json(&self) -> serde_json::Value`
///
/// # Example
///
/// ```rust,ignore
/// use voltra::table;
///
/// #[table(name = "players")]
/// pub struct Player {
///     pub hp: i32,
///     pub alive: bool,
///     pub zone: String,
/// }
/// ```
#[proc_macro_attribute]
pub fn table(attr: TokenStream, item: TokenStream) -> TokenStream {
    table::expand(attr, item)
}
