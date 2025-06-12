use proc_macro2::{Ident, TokenStream};
use quote::ToTokens;
use std::fmt;
use syn::{
    parse::{Parse, ParseStream},
    spanned::Spanned,
};

use crate::{contexts::Context, subscriptions::Subscription};

#[derive(Debug)]
pub enum MeteringMacrosError {
    SynError(syn::Error),
    // Subscription-specific errors
    InvalidSubscriptionAttribute(proc_macro2::Span),
    MissingSubscriptionId(proc_macro2::Span),
    SubscriptionMethodCount(usize, proc_macro2::Span),
    SubscriptionItemNotAMethod(proc_macro2::Span),
    SubscriptionMethodMissingSelf(proc_macro2::Span),
    SubscriptionMethodInvalidArg(proc_macro2::Span),
    // Context-specific errors
    InvalidContextAttribute(proc_macro2::Span),
    ContextMissingSubscriptions(proc_macro2::Span),
    ContextMissingHandlers(proc_macro2::Span),
    ContextMismatchedHandlers(proc_macro2::Span),
}

impl fmt::Display for MeteringMacrosError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MeteringMacrosError::SynError(e) => write!(f, "{}", e),
            // Subscription errors
            MeteringMacrosError::InvalidSubscriptionAttribute(_) => {
                write!(f, "The `subscription` attribute must be a list, like `#[subscription(id = \"...\")]`")
            }
            MeteringMacrosError::MissingSubscriptionId(_) => {
                write!(
                    f,
                    "The `subscription` attribute is missing the `id` argument"
                )
            }
            MeteringMacrosError::SubscriptionMethodCount(count, _) => {
                write!(
                    f,
                    "A subscription trait must have exactly one method, but this one has {}",
                    count
                )
            }
            MeteringMacrosError::SubscriptionItemNotAMethod(_) => {
                write!(f, "The item inside a subscription trait must be a method")
            }
            MeteringMacrosError::SubscriptionMethodMissingSelf(_) => {
                write!(
                    f,
                    "The first parameter of a subscription method must be `&self`"
                )
            }
            MeteringMacrosError::SubscriptionMethodInvalidArg(_) => {
                write!(
                    f,
                    "A subscription method argument could not be parsed correctly"
                )
            }
            // Context errors
            MeteringMacrosError::InvalidContextAttribute(_) => {
                write!(
                    f,
                    "The `context` attribute must be a list, like `#[context(...)]`"
                )
            }
            MeteringMacrosError::ContextMissingSubscriptions(_) => {
                write!(
                    f,
                    "The `context` attribute is missing the `subscriptions` argument"
                )
            }
            MeteringMacrosError::ContextMissingHandlers(_) => {
                write!(
                    f,
                    "The `context` attribute is missing the `handlers` argument"
                )
            }
            MeteringMacrosError::ContextMismatchedHandlers(_) => {
                write!(f, "The `subscriptions` and `handlers` arrays must have the same number of elements")
            }
        }
    }
}

impl MeteringMacrosError {
    /// Converts the custom error into a `compile_error!` token stream.
    pub fn to_compile_error(&self) -> TokenStream {
        let message = self.to_string();
        let span = match self {
            MeteringMacrosError::SynError(e) => return e.to_compile_error(),
            MeteringMacrosError::InvalidSubscriptionAttribute(s) => *s,
            MeteringMacrosError::MissingSubscriptionId(s) => *s,
            MeteringMacrosError::SubscriptionMethodCount(_, s) => *s,
            MeteringMacrosError::SubscriptionItemNotAMethod(s) => *s,
            MeteringMacrosError::SubscriptionMethodMissingSelf(s) => *s,
            MeteringMacrosError::SubscriptionMethodInvalidArg(s) => *s,
            MeteringMacrosError::InvalidContextAttribute(s) => *s,
            MeteringMacrosError::ContextMissingSubscriptions(s) => *s,
            MeteringMacrosError::ContextMissingHandlers(s) => *s,
            MeteringMacrosError::ContextMismatchedHandlers(s) => *s,
        };
        quote::quote_spanned! {span=>
            compile_error!(#message);
        }
    }
}

// ##################################################################
// # 2. Top-Level Parsing
// ##################################################################

/// A struct to enable parsing a stream of top-level items (structs, traits, etc.).
struct RootItems {
    items: Vec<syn::Item>,
}

impl Parse for RootItems {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut items = Vec::new();
        while !input.is_empty() {
            items.push(input.parse()?);
        }
        Ok(RootItems { items })
    }
}

/// The main entry point for parsing the macro's input token stream.
/// It iterates through all items and attempts to parse them as either
/// a Subscription or a Context.
pub fn process_token_stream(
    token_stream: &TokenStream,
) -> Result<(Vec<Subscription>, Vec<Context>), MeteringMacrosError> {
    // Parse the stream into our helper struct.
    let root_items: RootItems =
        syn::parse2(token_stream.clone()).map_err(MeteringMacrosError::SynError)?;

    let mut subscriptions = Vec::new();
    let mut contexts = Vec::new();

    // Iterate through each item defined inside the macro.
    for item in root_items.items {
        match item {
            // If the item is a trait, check if it's a subscription.
            syn::Item::Trait(item_trait) => {
                if has_attribute(&item_trait.attrs, "subscription") {
                    subscriptions.push(parse_subscription(item_trait)?);
                }
                // We ignore traits without the `#[subscription]` attribute.
            }
            // If the item is a struct, check if it's a context.
            syn::Item::Struct(item_struct) => {
                if has_attribute(&item_struct.attrs, "context") {
                    contexts.push(parse_context(item_struct)?);
                }
                // We ignore structs without the `#[context]` attribute.
            }
            // Ignore all other types of items.
            _ => (),
        }
    }

    Ok((subscriptions, contexts))
}

/// A small helper to check if an attribute with a given name exists.
fn has_attribute(attrs: &[syn::Attribute], name: &str) -> bool {
    attrs.iter().any(|attr| attr.path().is_ident(name))
}

// ##################################################################
// # 3. Subscription Parsing Logic
// ##################################################################

/// Parses a `syn::ItemTrait` into our `Subscription` model.
fn parse_subscription(item_trait: syn::ItemTrait) -> Result<Subscription, MeteringMacrosError> {
    let subscription_attr = item_trait
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("subscription"))
        .unwrap(); // This is safe due to the check in the caller.

    // --- Parse subscription ID from `#[subscription(id = "...")]` ---
    let subscription_id_string = if let syn::Meta::List(meta_list) = &subscription_attr.meta {
        let mut parsed_id: Option<String> = None;
        meta_list
            .parse_nested_meta(|meta| {
                if meta.path.is_ident("id") {
                    let value = meta.value()?;
                    if let syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(lit_str),
                        ..
                    }) = value.parse()?
                    {
                        parsed_id = Some(lit_str.value());
                        Ok(())
                    } else {
                        Err(meta.error("subscription `id` must be a string literal"))
                    }
                } else {
                    Err(meta.error(
                        "unrecognized key for subscription attribute, only `id` is supported",
                    ))
                }
            })
            .map_err(MeteringMacrosError::SynError)?;

        parsed_id.ok_or_else(|| MeteringMacrosError::MissingSubscriptionId(meta_list.span()))?
    } else {
        return Err(MeteringMacrosError::InvalidSubscriptionAttribute(
            subscription_attr.span(),
        ));
    };

    // --- Collect foreign attributes on the trait ---
    let subscription_foreign_macro_token_streams = item_trait
        .attrs
        .iter()
        .filter(|attr| !attr.path().is_ident("subscription"))
        .map(|attr| attr.to_token_stream())
        .collect();

    // --- Extract visibility modifier ---
    let maybe_subscription_visibility_modifier_token_stream =
        if let syn::Visibility::Inherited = &item_trait.vis {
            None
        } else {
            Some(item_trait.vis.to_token_stream())
        };

    // --- Validate and parse the single method ---
    if item_trait.items.len() != 1 {
        return Err(MeteringMacrosError::SubscriptionMethodCount(
            item_trait.items.len(),
            item_trait.span(),
        ));
    }
    // FIX: Trait methods are `TraitItem::Fn`, not `TraitItem::Method`
    let method = if let Some(syn::TraitItem::Fn(m)) = item_trait.items.get(0) {
        m
    } else {
        return Err(MeteringMacrosError::SubscriptionItemNotAMethod(
            item_trait.items[0].span(),
        ));
    };

    // --- Parse method parameters ---
    let mut inputs = method.sig.inputs.iter();
    match inputs.next() {
        Some(syn::FnArg::Receiver(rec)) if rec.reference.is_some() && rec.mutability.is_none() => {
            ()
        } // Correct `&self`
        _ => {
            return Err(MeteringMacrosError::SubscriptionMethodMissingSelf(
                method.sig.fn_token.span,
            ))
        }
    }

    let mut subscription_method_parameter_name_idents = Vec::new();
    let mut subscription_method_parameters_token_streams = Vec::new();

    for arg in inputs {
        if let syn::FnArg::Typed(pat_type) = arg {
            subscription_method_parameters_token_streams.push(arg.to_token_stream());
            if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                subscription_method_parameter_name_idents.push(pat_ident.ident.clone());
            } else {
                return Err(MeteringMacrosError::SubscriptionMethodInvalidArg(
                    arg.span(),
                ));
            }
        } else {
            return Err(MeteringMacrosError::SubscriptionMethodInvalidArg(
                arg.span(),
            ));
        }
    }

    Ok(Subscription {
        subscription_id_string,
        subscription_foreign_macro_token_streams,
        maybe_subscription_visibility_modifier_token_stream,
        subscription_name_ident: item_trait.ident,
        subscription_method_foreign_macro_token_streams: method
            .attrs
            .iter()
            .map(|a| a.to_token_stream())
            .collect(),
        subscription_method_name_ident: method.sig.ident.clone(),
        subscription_method_parameter_name_idents,
        subscription_method_parameters_token_streams,
    })
}

// ##################################################################
// # 4. Context Parsing Logic
// ##################################################################

/// Parses a `syn::ItemStruct` into our `Context` model.
fn parse_context(item_struct: syn::ItemStruct) -> Result<Context, MeteringMacrosError> {
    let context_attr = item_struct
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("context"))
        .unwrap(); // Safe due to caller check.

    // --- Parse `#[context(subscriptions = [...], handlers = [...])]` ---
    let (subscription_ids, handler_idents) = {
        let mut sub_ids: Option<Vec<String>> = None;
        let mut h_idents: Option<Vec<Ident>> = None;

        if let syn::Meta::List(meta_list) = &context_attr.meta {
            // FIX: This closure must return syn::Result. The `?` operator inside
            // will now work because the helper functions are changed to return
            // syn::Result. The map_err at the end converts the syn::Error into
            // our custom MeteringMacrosError::SynError.
            meta_list
                .parse_nested_meta(|meta| {
                    if meta.path.is_ident("subscriptions") {
                        sub_ids = Some(parse_string_array_from_meta(&meta)?);
                        Ok(())
                    } else if meta.path.is_ident("handlers") {
                        h_idents = Some(parse_ident_array_from_meta(&meta)?);
                        Ok(())
                    } else {
                        Err(meta.error("unrecognized key for context attribute"))
                    }
                })
                .map_err(MeteringMacrosError::SynError)?;
        } else {
            return Err(MeteringMacrosError::InvalidContextAttribute(
                context_attr.span(),
            ));
        }

        let sub_ids = sub_ids
            .ok_or_else(|| MeteringMacrosError::ContextMissingSubscriptions(context_attr.span()))?;
        let h_idents = h_idents
            .ok_or_else(|| MeteringMacrosError::ContextMissingHandlers(context_attr.span()))?;
        (sub_ids, h_idents)
    };

    if subscription_ids.len() != handler_idents.len() {
        return Err(MeteringMacrosError::ContextMismatchedHandlers(
            context_attr.span(),
        ));
    }

    let context_subscription_id_strings_to_handler_idents = subscription_ids
        .into_iter()
        .zip(handler_idents.into_iter())
        .collect();

    // --- Collect foreign attributes on the struct ---
    let context_foreign_macro_token_streams = item_struct
        .attrs
        .iter()
        .filter(|attr| !attr.path().is_ident("context"))
        .map(|attr| attr.to_token_stream())
        .collect();

    // --- Extract visibility ---
    let maybe_context_visibility_modifier_token_stream =
        if let syn::Visibility::Inherited = &item_struct.vis {
            None
        } else {
            Some(item_struct.vis.to_token_stream())
        };

    // --- Extract fields ---
    let context_field_token_streams = if let syn::Fields::Named(fields) = item_struct.fields {
        fields.named.iter().map(|f| f.to_token_stream()).collect()
    } else {
        // Your example only uses named fields. Add handling for tuple/unit structs if needed.
        vec![]
    };

    Ok(Context {
        context_foreign_macro_token_streams,
        context_subscription_id_strings_to_handler_idents,
        maybe_context_visibility_modifier_token_stream,
        context_name_ident: item_struct.ident,
        context_field_token_streams,
    })
}

/// Helper to parse an array of string literals like `key = ["a", "b"]`.
// FIX: Changed return type to syn::Result to work with `?` inside `parse_nested_meta`
fn parse_string_array_from_meta(meta: &syn::meta::ParseNestedMeta) -> syn::Result<Vec<String>> {
    let value = meta.value()?;
    if let syn::Expr::Array(expr_array) = value.parse()? {
        expr_array
            .elems
            .iter()
            .map(|expr| {
                if let syn::Expr::Lit(expr_lit) = expr {
                    if let syn::Lit::Str(lit_str) = &expr_lit.lit {
                        Ok(lit_str.value())
                    } else {
                        Err(syn::Error::new(expr.span(), "Expected a string literal"))
                    }
                } else {
                    Err(syn::Error::new(expr.span(), "Expected a literal value"))
                }
            })
            .collect()
    } else {
        Err(syn::Error::new(value.span(), "Expected an array `[...]`"))
    }
}

/// Helper to parse an array of identifiers like `key = [a, b]`.
// FIX: Changed return type to syn::Result
fn parse_ident_array_from_meta(meta: &syn::meta::ParseNestedMeta) -> syn::Result<Vec<Ident>> {
    let value = meta.value()?;
    if let syn::Expr::Array(expr_array) = value.parse()? {
        expr_array
            .elems
            .iter()
            .map(|expr| {
                if let syn::Expr::Path(expr_path) = expr {
                    // FIX: `get_ident` is on the `.path` field of `ExprPath`.
                    expr_path.path.get_ident().cloned().ok_or_else(|| {
                        syn::Error::new(expr.span(), "Expected a single identifier, not a path")
                    })
                } else {
                    Err(syn::Error::new(expr.span(), "Expected an identifier"))
                }
            })
            .collect()
    } else {
        Err(syn::Error::new(value.span(), "Expected an array `[...]`"))
    }
}
