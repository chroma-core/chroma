use proc_macro2::{Ident, TokenStream};
use quote::ToTokens;
use syn::{
    parse::{Parse, ParseStream},
    spanned::Spanned,
};

use crate::{capabilities::Capability, contexts::Context, errors::MeteringMacrosError};

/// Represents an item at the root level of the syntax tree.
struct RootItems {
    items: Vec<syn::Item>,
}

/// Implementation of [`syn::parse::Parse`] for [`RootItems`].
impl Parse for RootItems {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut items = Vec::new();
        while !input.is_empty() {
            items.push(input.parse()?);
        }
        Ok(RootItems { items })
    }
}

/// The main entrypoint of the parser that accepts the full input token stream and parses out
/// vectors of [`crate::capabilities::Capability`], [`crate::contexts::Context`], and
/// passthrough items.
pub fn process_token_stream(
    token_stream: &TokenStream,
) -> Result<(Vec<Capability>, Vec<Context>, Vec<syn::Item>), MeteringMacrosError> {
    let root_items: RootItems =
        syn::parse2(token_stream.clone()).map_err(MeteringMacrosError::SynError)?;
    let mut capabilities = Vec::new();
    let mut contexts = Vec::new();
    let mut passthroughs = Vec::new();

    for item in root_items.items {
        let mut is_processed = false;
        if let syn::Item::Trait(item_trait) = &item {
            if has_attribute(&item_trait.attrs, "capability") {
                capabilities.push(parse_capability(item_trait.clone())?);
                let mut clean_trait = item_trait.clone();
                clean_trait
                    .attrs
                    .retain(|attribute| !attribute.path().is_ident("capability"));
                passthroughs.push(syn::Item::Trait(clean_trait));
                is_processed = true;
            }
        } else if let syn::Item::Struct(item_struct) = &item {
            if has_attribute(&item_struct.attrs, "context") {
                contexts.push(parse_context(item_struct.clone())?);
                let mut clean_struct = item_struct.clone();
                clean_struct
                    .attrs
                    .retain(|attribute| !attribute.path().is_ident("context"));
                passthroughs.push(syn::Item::Struct(clean_struct));
                is_processed = true;
            }
        }
        if !is_processed {
            passthroughs.push(item);
        }
    }
    Ok((capabilities, contexts, passthroughs))
}

/// Determines whether a given item's attribute macros contains a specific `target_attribute_name`.
fn has_attribute(attributes: &[syn::Attribute], target_attribute_name: &str) -> bool {
    attributes
        .iter()
        .any(|attribute| attribute.path().is_ident(target_attribute_name))
}

/// Helper function to parse an individual capability.
fn parse_capability(item_trait: syn::ItemTrait) -> Result<Capability, MeteringMacrosError> {
    // NOTE(c-gamble): Unwrap is safe due to `has_attribute` check in caller.
    let attr = item_trait
        .attrs
        .iter()
        .find(|attribute| attribute.path().is_ident("capability"))
        .ok_or_else(|| MeteringMacrosError::InvalidCapabilityAttribute(item_trait.span()))?;

    if !matches!(attr.meta, syn::Meta::Path(_)) {
        return Err(MeteringMacrosError::InvalidCapabilityAttribute(attr.span()));
    }

    match &item_trait.vis {
        syn::Visibility::Public(_) => {}
        _ => {
            return Err(MeteringMacrosError::CapabilityNotPublic(
                item_trait.ident.span(),
            ));
        }
    }

    if item_trait.items.len() != 1 {
        return Err(MeteringMacrosError::CapabilityMethodCount(
            item_trait.items.len(),
            item_trait.ident.span(),
        ));
    }

    let method = if let Some(syn::TraitItem::Fn(m)) = item_trait.items.get(0) {
        m
    } else {
        return Err(MeteringMacrosError::CapabilityItemNotAMethod(
            item_trait.items[0].span(),
        ));
    };

    match method.sig.inputs.iter().next() {
        Some(syn::FnArg::Receiver(rec)) if rec.reference.is_some() && rec.mutability.is_none() => {
            ()
        }
        _ => {
            return Err(MeteringMacrosError::CapabilityMethodMissingSelf(
                method.sig.fn_token.span,
            ))
        }
    }

    let mut param_names = vec![];
    let mut param_tokens = vec![];
    for arg in method.sig.inputs.iter().skip(1) {
        if let syn::FnArg::Typed(pt) = arg {
            if let syn::Pat::Ident(pi) = &*pt.pat {
                param_names.push(pi.ident.clone());
            } else {
                return Err(MeteringMacrosError::CapabilityMethodInvalidArg(arg.span()));
            }
            param_tokens.push(arg.to_token_stream());
        } else {
            return Err(MeteringMacrosError::CapabilityMethodInvalidArg(arg.span()));
        }
    }

    Ok(Capability {
        capability_name_ident: item_trait.ident.clone(),
        capability_marker_method_name_ident: Ident::new(
            &format!("__marker_{}", item_trait.ident.to_string()),
            attr.span(),
        ),
        capability_method_name_ident: method.sig.ident.clone(),
        capability_method_parameter_token_streams: param_tokens,
        capability_method_parameter_name_idents: param_names,
    })
}

/// Helper function to parse an individual context.
fn parse_context(item_struct: syn::ItemStruct) -> Result<Context, MeteringMacrosError> {
    // NOTE(c-gamble): Unwrap is safe due to `has_attribute` check in caller.
    let attr = item_struct
        .attrs
        .iter()
        .find(|attribute| attribute.path().is_ident("context"))
        .ok_or_else(|| MeteringMacrosError::InvalidContextAttribute(item_struct.span()))?;

    let mut cap_idents = None;
    let mut handlers = None;
    if let syn::Meta::List(meta_list) = &attr.meta {
        meta_list.parse_nested_meta(|meta| {
                if meta.path.is_ident("capabilities") {
                     let value = meta.value()?;
                     if let syn::Expr::Array(arr) = value.parse()? {
                         let mut idents = Vec::new();
                         for elem in arr.elems {
                             if let syn::Expr::Path(p) = elem {
                                 if let Some(ident) = p.path.get_ident() {
                                     idents.push(ident.clone()); // Store the trait ident directly
                                 } else {
                                    return Err(meta.error("`capabilities` must be an array of simple trait identifiers"));
                                 }
                             } else {
                                 return Err(meta.error("`capabilities` must be an array of trait identifiers"));
                             }
                         }
                         cap_idents = Some(idents);
                     } else {
                        return Err(meta.error("`capabilities` must be an array literal `[...]`"));
                     }
                } else if meta.path.is_ident("handlers") {
                     let value = meta.value()?;
                     if let syn::Expr::Array(arr) = value.parse()? {
                         let mut h = Vec::new();
                         for elem in arr.elems {
                             if let syn::Expr::Path(p) = elem {
                                 if let Some(ident) = p.path.get_ident() {
                                     h.push(ident.clone());
                                 } else {
                                    return Err(meta.error("`handlers` must be an array of simple identifiers"));
                                 }
                             } else {
                                return Err(meta.error("`handlers` must be an array of identifiers"));
                             }
                         }
                         handlers = Some(h);
                     } else {
                        return Err(meta.error("`handlers` must be an array literal `[...]`"));
                     }
                }
                Ok(())
            }).map_err(MeteringMacrosError::SynError)?;
    } else {
        return Err(MeteringMacrosError::InvalidContextAttribute(attr.span()));
    }

    let cap_idents =
        cap_idents.ok_or_else(|| MeteringMacrosError::ContextMissingCapabilities(attr.span()))?;
    let handlers =
        handlers.ok_or_else(|| MeteringMacrosError::ContextMissingHandlers(attr.span()))?;

    if cap_idents.len() != handlers.len() {
        return Err(MeteringMacrosError::ContextMismatchedHandlers(attr.span()));
    }

    Ok(Context {
        context_name_ident: item_struct.ident,
        context_capability_name_idents_to_handler_idents: cap_idents
            .into_iter()
            .zip(handlers.into_iter())
            .collect(),
    })
}
