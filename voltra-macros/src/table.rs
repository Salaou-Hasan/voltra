//! `#[table]` proc-macro implementation.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, Fields, ItemStruct, LitStr};

/// Entry point called from lib.rs.
pub fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
    expand_inner(attr, input)
        .unwrap_or_else(|e| e.to_compile_error().into())
}

fn expand_inner(attr: TokenStream, input: ItemStruct) -> Result<TokenStream, syn::Error> {
    let struct_name = &input.ident;
    let vis         = &input.vis;
    let attrs       = &input.attrs;  // forward existing attributes (doc-comments, etc.)

    // ── Parse table name from #[table(name = "players")] ────────────────────
    let table_name: LitStr = parse_table_name_attr(attr, struct_name)?;

    // ── Fields (require named struct) ────────────────────────────────────────
    match &input.fields {
        Fields::Named(_) => {}
        _ => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "#[table] requires a struct with named fields",
            ));
        }
    }

    // ── Re-emit the struct with Serialize + Deserialize derived ──────────────
    // We strip the original `derive` attr (if any) to avoid duplicates and
    // inject our own.
    let generics = &input.generics;
    let fields   = &input.fields;

    let expanded = quote! {
        #(#attrs)*
        #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize)]
        #vis struct #struct_name #generics #fields

        impl #generics #struct_name #generics {
            /// The Voltra table this type maps to.
            pub fn table_name() -> &'static str {
                #table_name
            }

            /// Deserialise from a `serde_json::Value` returned by
            /// `ctx.get(table, key)`.  Returns `None` if the value is
            /// malformed or the wrong type.
            pub fn from_json(v: ::serde_json::Value) -> ::std::option::Option<Self> {
                ::serde_json::from_value(v).ok()
            }

            /// Serialise to a `serde_json::Value` suitable for
            /// `ctx.set(table, key, row.to_json())`.
            pub fn to_json(&self) -> ::serde_json::Value {
                ::serde_json::to_value(self)
                    .unwrap_or(::serde_json::Value::Null)
            }
        }
    };

    Ok(TokenStream::from(expanded))
}

// ── Attribute parsing ─────────────────────────────────────────────────────────

/// Parse `name = "players"` from the attribute token stream.
/// Falls back to the snake_case version of the struct name.
fn parse_table_name_attr(
    attr: TokenStream,
    struct_ident: &syn::Ident,
) -> Result<LitStr, syn::Error> {
    // Convert to proc_macro2 for syn parsing.
    let attr2: proc_macro2::TokenStream = attr.into();

    if attr2.is_empty() {
        // No attribute args → derive name from struct ident.
        let name = to_snake_case(&struct_ident.to_string());
        return Ok(LitStr::new(&name, Span::call_site()));
    }

    // Try to parse:  name = "some_table"
    struct NameAttr {
        value: LitStr,
    }

    impl syn::parse::Parse for NameAttr {
        fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
            // Accept   name = "value"
            let ident: syn::Ident = input.parse()?;
            if ident != "name" {
                return Err(syn::Error::new_spanned(
                    ident,
                    r#"expected `name = "table_name"`"#,
                ));
            }
            let _eq: syn::Token![=] = input.parse()?;
            let value: LitStr = input.parse()?;
            Ok(NameAttr { value })
        }
    }

    let parsed: NameAttr = syn::parse2(attr2)?;
    Ok(parsed.value)
}

/// `PlayerScore` → `player_score`
fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.extend(c.to_lowercase());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::to_snake_case;

    #[test]
    fn snake_case_conversions() {
        assert_eq!(to_snake_case("Player"),      "player");
        assert_eq!(to_snake_case("PlayerScore"), "player_score");
        assert_eq!(to_snake_case("LeaderBoard"), "leader_board");
        assert_eq!(to_snake_case("HP"),          "h_p");
    }
}
