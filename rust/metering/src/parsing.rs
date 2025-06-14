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

/// A type alias for the result of [`crate::parsing::process_token_stream`].
pub type ParseResult = (Vec<Capability>, Vec<Context>, Vec<syn::Item>);

/// The main entrypoint of the parser that accepts the full input token stream and parses out
/// vectors of [`crate::capabilities::Capability`], [`crate::contexts::Context`], and
/// passthrough items.
pub fn process_token_stream(
    token_stream: &TokenStream,
) -> Result<ParseResult, MeteringMacrosError> {
    let root_items: RootItems =
        syn::parse2(token_stream.clone()).map_err(MeteringMacrosError::SynError)?;
    let mut capabilities = Vec::new();
    let mut contexts = Vec::new();
    let mut passthroughs = Vec::new();

    for root_item in root_items.items {
        let mut is_processed = false;
        if let syn::Item::Trait(expected_capability_trait) = &root_item {
            if has_attribute(&expected_capability_trait.attrs, "capability") {
                capabilities.push(parse_capability(expected_capability_trait.clone())?);
                let mut capability_trait = expected_capability_trait.clone();
                capability_trait
                    .attrs
                    .retain(|attribute| !attribute.path().is_ident("capability"));
                passthroughs.push(syn::Item::Trait(capability_trait));
                is_processed = true;
            }
        } else if let syn::Item::Struct(expected_context_struct) = &root_item {
            if has_attribute(&expected_context_struct.attrs, "context") {
                contexts.push(parse_context(expected_context_struct.clone())?);
                let mut context_struct = expected_context_struct.clone();
                context_struct
                    .attrs
                    .retain(|attribute| !attribute.path().is_ident("context"));
                passthroughs.push(syn::Item::Struct(context_struct));
                is_processed = true;
            }
        }
        if !is_processed {
            passthroughs.push(root_item);
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
fn parse_capability(capability_trait: syn::ItemTrait) -> Result<Capability, MeteringMacrosError> {
    // NOTE(c-gamble): Unwrap is safe due to `has_attribute` check in caller.
    let capability_attribute = capability_trait
        .attrs
        .iter()
        .find(|attribute| attribute.path().is_ident("capability"))
        .ok_or_else(|| MeteringMacrosError::InvalidCapabilityAttribute(capability_trait.span()))?;

    if !matches!(capability_attribute.meta, syn::Meta::Path(_)) {
        return Err(MeteringMacrosError::InvalidCapabilityAttribute(
            capability_attribute.span(),
        ));
    }

    match &capability_trait.vis {
        syn::Visibility::Public(_) => {}
        _ => {
            return Err(MeteringMacrosError::CapabilityNotPublic(
                capability_trait.ident.span(),
            ));
        }
    }

    if capability_trait.items.len() != 1 {
        return Err(MeteringMacrosError::CapabilityMethodCount(
            capability_trait.items.len(),
            capability_trait.ident.span(),
        ));
    }

    let capability_method = if let Some(syn::TraitItem::Fn(m)) = capability_trait.items.first() {
        m
    } else {
        return Err(MeteringMacrosError::CapabilityItemNotAMethod(
            capability_trait.items[0].span(),
        ));
    };

    match capability_method.sig.inputs.iter().next() {
        Some(syn::FnArg::Receiver(rec)) if rec.reference.is_some() && rec.mutability.is_none() => {}
        _ => {
            return Err(MeteringMacrosError::CapabilityMethodMissingSelf(
                capability_method.sig.fn_token.span,
            ))
        }
    }

    let mut capability_method_parameter_name_idents = vec![];
    let mut capability_method_parameter_token_streams = vec![];
    for capability_method_parameter in capability_method.sig.inputs.iter().skip(1) {
        if let syn::FnArg::Typed(parameter_type) = capability_method_parameter {
            if let syn::Pat::Ident(parameter_name_ident) = &*parameter_type.pat {
                capability_method_parameter_name_idents.push(parameter_name_ident.ident.clone());
            } else {
                return Err(MeteringMacrosError::CapabilityMethodInvalidArg(
                    capability_method_parameter.span(),
                ));
            }
            capability_method_parameter_token_streams
                .push(capability_method_parameter.to_token_stream());
        } else {
            return Err(MeteringMacrosError::CapabilityMethodInvalidArg(
                capability_method_parameter.span(),
            ));
        }
    }

    Ok(Capability {
        capability_name_ident: capability_trait.ident.clone(),
        capability_marker_method_name_ident: Ident::new(
            &format!("__marker_{}", capability_trait.ident),
            capability_attribute.span(),
        ),
        capability_method_name_ident: capability_method.sig.ident.clone(),
        capability_method_parameter_token_streams,
        capability_method_parameter_name_idents,
    })
}

/// Helper function to parse an individual context.
fn parse_context(context_struct: syn::ItemStruct) -> Result<Context, MeteringMacrosError> {
    // NOTE(c-gamble): Unwrap is safe due to `has_attribute` check in caller.
    let context_attribute = context_struct
        .attrs
        .iter()
        .find(|attribute| attribute.path().is_ident("context"))
        .ok_or_else(|| MeteringMacrosError::InvalidContextAttribute(context_struct.span()))?;

    let mut maybe_capability_idents = None;
    let mut maybe_handler_idents = None;
    if let syn::Meta::List(meta_list) = &context_attribute.meta {
        meta_list.parse_nested_meta(|meta| {
                if meta.path.is_ident("capabilities") {
                     let value = meta.value()?;
                     if let syn::Expr::Array(array) = value.parse()? {
                         let mut capability_idents = Vec::new();
                         for element in array.elems {
                             if let syn::Expr::Path(path) = element {
                                 if let Some(capability_ident) = path.path.get_ident() {
                                     capability_idents.push(capability_ident.clone());
                                 } else {
                                    return Err(meta.error("`capabilities` must be an array of simple trait identifiers"));
                                 }
                             } else {
                                 return Err(meta.error("`capabilities` must be an array of trait identifiers"));
                             }
                         }
                         maybe_capability_idents = Some(capability_idents);
                     } else {
                        return Err(meta.error("`capabilities` must be an array literal `[...]`"));
                     }
                } else if meta.path.is_ident("handlers") {
                     let value = meta.value()?;
                     if let syn::Expr::Array(array) = value.parse()? {
                         let mut handler_idents = Vec::new();
                         for element in array.elems {
                             if let syn::Expr::Path(path) = element {
                                 if let Some(handler_ident) = path.path.get_ident() {
                                     handler_idents.push(handler_ident.clone());
                                 } else {
                                    return Err(meta.error("`handlers` must be an array of simple identifiers"));
                                 }
                             } else {
                                return Err(meta.error("`handlers` must be an array of identifiers"));
                             }
                         }
                         maybe_handler_idents = Some(handler_idents);
                     } else {
                        return Err(meta.error("`handlers` must be an array literal `[...]`"));
                     }
                }
                Ok(())
            }).map_err(MeteringMacrosError::SynError)?;
    } else {
        return Err(MeteringMacrosError::InvalidContextAttribute(
            context_attribute.span(),
        ));
    }

    let capability_idents = maybe_capability_idents
        .ok_or_else(|| MeteringMacrosError::ContextMissingCapabilities(context_attribute.span()))?;
    let handler_idents = maybe_handler_idents
        .ok_or_else(|| MeteringMacrosError::ContextMissingHandlers(context_attribute.span()))?;

    if capability_idents.len() != handler_idents.len() {
        return Err(MeteringMacrosError::ContextMismatchedHandlers(
            context_attribute.span(),
        ));
    }

    Ok(Context {
        context_name_ident: context_struct.ident,
        context_capability_name_idents_to_handler_idents: capability_idents
            .into_iter()
            .zip(handler_idents)
            .collect(),
    })
}
