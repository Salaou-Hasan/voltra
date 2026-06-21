//! `#[reducer]` proc-macro implementation.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, FnArg, Ident, ItemFn, Pat};

/// Entry point called from lib.rs.
pub fn expand(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    expand_inner(input)
        .unwrap_or_else(|e| e.to_compile_error().into())
}

fn expand_inner(input: ItemFn) -> Result<TokenStream, syn::Error> {
    let fn_name   = &input.sig.ident;
    // A raw identifier (e.g. `r#move`, emitted by the .vol codegen for reducers
    // named after Rust keywords) stringifies to "r#move". Strip the prefix so the
    // wire name, struct name, and inner-fn name are derived from the plain name
    // ("move") — clients call the reducer by its real Neon name, not "r#move".
    let fn_name_s = {
        let s = fn_name.to_string();
        s.strip_prefix("r#").map(|s| s.to_string()).unwrap_or(s)
    };
    let vis       = &input.vis;
    let body      = &input.block;

    // ── Struct name: snake_case → PascalCase + "Reducer" ────────────────────
    let struct_name = Ident::new(
        &(to_pascal_case(&fn_name_s) + "Reducer"),
        Span::call_site(),
    );

    // ── Inner function name ──────────────────────────────────────────────────
    let inner_fn = Ident::new(
        &format!("__voltra_reducer_{}", fn_name_s),
        Span::call_site(),
    );

    // ── Parse parameters ─────────────────────────────────────────────────────
    // The FIRST param is the context parameter (any type, any name accepted).
    // All subsequent params become deserialized reducer args.
    let mut params = input.sig.inputs.iter();

    // Consume the context param — extract its binding name.
    let ctx_name: Ident = match params.next() {
        None => {
            return Err(syn::Error::new_spanned(
                &input.sig,
                "#[reducer] function must have at least one parameter (the context)",
            ));
        }
        Some(FnArg::Typed(pt)) => match &*pt.pat {
            Pat::Ident(p) => p.ident.clone(),
            _ => Ident::new("ctx", Span::call_site()),
        },
        Some(FnArg::Receiver(_)) => Ident::new("ctx", Span::call_site()),
    };

    // Collect remaining (arg) params.
    let mut arg_names: Vec<Ident> = Vec::new();
    let mut arg_types: Vec<Box<syn::Type>> = Vec::new();

    for param in params {
        match param {
            FnArg::Typed(pt) => {
                if let Pat::Ident(p) = &*pt.pat {
                    arg_names.push(p.ident.clone());
                    arg_types.push(pt.ty.clone());
                }
            }
            FnArg::Receiver(r) => {
                return Err(syn::Error::new_spanned(
                    r,
                    "#[reducer] does not accept `self` parameters",
                ));
            }
        }
    }

    // ── Code generation ───────────────────────────────────────────────────────

    // Build the generated args-parsing block only when there are args.
    let parse_block = if arg_names.is_empty() {
        quote! {}
    } else {
        let fields = arg_names.iter().zip(arg_types.iter()).map(|(n, t)| {
            quote! { pub #n: #t, }
        });
        let bindings = arg_names.iter().map(|n| {
            quote! { let #n = __voltra_args.#n; }
        });
        let fn_name_lit = &fn_name_s;

        quote! {
            #[derive(::serde::Deserialize)]
            #[allow(non_camel_case_types)]
            struct __VoltraArgs {
                #(#fields)*
            }
            let __voltra_args: __VoltraArgs = ::voltra::rmp_serde::from_slice(args)
                .map_err(|e| ::voltra::error::VoltraError::reducer_error(
                    format!("arg parse for reducer '{}': {}", #fn_name_lit, e)
                ))?;
            #(#bindings)*
        }
    };

    let fn_name_lit = &fn_name_s;

    let expanded = quote! {
        // ── Inner implementation function ─────────────────────────────────────
        fn #inner_fn(
            #ctx_name: &mut ::voltra::reducer::context::ReducerContext,
            args: &[u8],
        ) -> ::voltra::error::Result<::std::vec::Vec<u8>> {
            #[allow(unreachable_code, unused_variables, unused_mut)]
            {
                #parse_block
                // User-supplied body — `ret!(…)` is available here.
                #body
                // Default return when the body falls through without ret!
                Ok(::voltra::rmp_serde::to_vec(&::serde_json::json!({ "ok": true }))
                    .map_err(|e| ::voltra::error::VoltraError::reducer_error(e.to_string()))?)
            }
        }

        // ── ReducerBackend implementor ────────────────────────────────────────
        #[doc(hidden)]
        #vis struct #struct_name;

        impl ::voltra::reducer::backend::ReducerBackend for #struct_name {
            fn execute(
                &self,
                ctx: &mut ::voltra::reducer::context::ReducerContext,
                args: &[u8],
            ) -> ::voltra::error::Result<::std::vec::Vec<u8>> {
                #inner_fn(ctx, args)
            }
        }

        // ── Auto-register via inventory ───────────────────────────────────────
        ::voltra::inventory::submit! {
            ::voltra::reducer::registry::NativeReducerItem {
                name: #fn_name_lit,
                make: || ::std::boxed::Box::new(#struct_name),
            }
        }
    };

    Ok(TokenStream::from(expanded))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Convert `snake_case` to `PascalCase`.
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => {
                    let upper: String = first.to_uppercase().collect();
                    upper + chars.as_str()
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::to_pascal_case;

    #[test]
    fn pascal_case_basic() {
        assert_eq!(to_pascal_case("attack"),         "Attack");
        assert_eq!(to_pascal_case("deal_damage"),    "DealDamage");
        assert_eq!(to_pascal_case("buy_item_now"),   "BuyItemNow");
        assert_eq!(to_pascal_case("increment"),      "Increment");
        assert_eq!(to_pascal_case("_leading"),       "Leading");
    }
}
