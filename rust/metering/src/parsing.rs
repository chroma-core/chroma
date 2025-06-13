use proc_macro2::{Ident, TokenStream};
use quote::ToTokens;
use std::fmt;
use syn::{
    parse::{Parse, ParseStream},
    spanned::Spanned,
};

use crate::{capabilities::Capability, contexts::Context};

#[derive(Debug)]
pub enum MeteringMacrosError {
    SynError(syn::Error),

    InvalidCapabilityAttribute(proc_macro2::Span),
    MissingCapabilityId(proc_macro2::Span),
    CapabilityMethodCount(usize, proc_macro2::Span),
    CapabilityItemNotAMethod(proc_macro2::Span),
    CapabilityMethodMissingSelf(proc_macro2::Span),
    CapabilityMethodInvalidArg(proc_macro2::Span),

    InvalidContextAttribute(proc_macro2::Span),
    ContextMissingCapabilities(proc_macro2::Span),
    ContextMissingHandlers(proc_macro2::Span),
    ContextMismatchedHandlers(proc_macro2::Span),
}

impl fmt::Display for MeteringMacrosError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MeteringMacrosError::SynError(e) => write!(f, "{}", e),

            MeteringMacrosError::InvalidCapabilityAttribute(_) => {
                write!(
                    f,
                    "The `capability` attribute must be a list, like `#[capability(id = \"...\")]`"
                )
            }
            MeteringMacrosError::MissingCapabilityId(_) => {
                write!(
                    f,
                    "The `capability` attribute is missing the `id = \"...\"` argument"
                )
            }
            MeteringMacrosError::CapabilityMethodCount(count, _) => {
                write!(
                    f,
                    "A capability trait must have exactly one method, but this one has {}",
                    count
                )
            }
            MeteringMacrosError::CapabilityItemNotAMethod(_) => {
                write!(f, "The item inside a capability trait must be a method")
            }
            MeteringMacrosError::CapabilityMethodMissingSelf(_) => {
                write!(
                    f,
                    "The first parameter of a capability method must be `&self`"
                )
            }
            MeteringMacrosError::CapabilityMethodInvalidArg(_) => {
                write!(
                    f,
                    "A capability method argument could not be parsed correctly"
                )
            }

            MeteringMacrosError::InvalidContextAttribute(_) => {
                write!(
                    f,
                    "The `context` attribute must be a list, like `#[context(...)]`"
                )
            }
            MeteringMacrosError::ContextMissingCapabilities(_) => {
                write!(
                    f,
                    "The `context` attribute is missing the `capabilities` argument"
                )
            }
            MeteringMacrosError::ContextMissingHandlers(_) => {
                write!(
                    f,
                    "The `context` attribute is missing the `handlers` argument"
                )
            }
            MeteringMacrosError::ContextMismatchedHandlers(_) => {
                write!(f, "The `capabilities` and `handlers` arrays must have the same number of elements")
            }
        }
    }
}

impl MeteringMacrosError {
    pub fn to_compile_error(&self) -> TokenStream {
        let message = self.to_string();
        let span = match self {
            MeteringMacrosError::SynError(e) => return e.to_compile_error(),
            MeteringMacrosError::InvalidCapabilityAttribute(s)
            | MeteringMacrosError::MissingCapabilityId(s)
            | MeteringMacrosError::CapabilityMethodCount(_, s)
            | MeteringMacrosError::CapabilityItemNotAMethod(s)
            | MeteringMacrosError::CapabilityMethodMissingSelf(s)
            | MeteringMacrosError::CapabilityMethodInvalidArg(s)
            | MeteringMacrosError::InvalidContextAttribute(s)
            | MeteringMacrosError::ContextMissingCapabilities(s)
            | MeteringMacrosError::ContextMissingHandlers(s)
            | MeteringMacrosError::ContextMismatchedHandlers(s) => *s,
        };
        quote::quote_spanned! {span=>
            compile_error!(#message);
        }
    }
}

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

pub fn process_token_stream(
    token_stream: &TokenStream,
) -> Result<(Vec<Capability>, Vec<Context>, Vec<syn::Item>), MeteringMacrosError> {
    let root_items: RootItems =
        syn::parse2(token_stream.clone()).map_err(MeteringMacrosError::SynError)?;
    let mut capabilities = Vec::new();
    let mut contexts = Vec::new();
    let mut passthrough_items = Vec::new();

    for item in root_items.items {
        let mut is_processed = false;
        if let syn::Item::Trait(item_trait) = &item {
            if has_attribute(&item_trait.attrs, "capability") {
                capabilities.push(parse_capability(item_trait.clone())?);
                let mut clean_trait = item_trait.clone();
                clean_trait
                    .attrs
                    .retain(|a| !a.path().is_ident("capability"));
                passthrough_items.push(syn::Item::Trait(clean_trait));
                is_processed = true;
            }
        } else if let syn::Item::Struct(item_struct) = &item {
            if has_attribute(&item_struct.attrs, "context") {
                contexts.push(parse_context(item_struct.clone())?);
                let mut clean_struct = item_struct.clone();
                clean_struct.attrs.retain(|a| !a.path().is_ident("context"));
                passthrough_items.push(syn::Item::Struct(clean_struct));
                is_processed = true;
            }
        }
        if !is_processed {
            passthrough_items.push(item);
        }
    }
    Ok((capabilities, contexts, passthrough_items))
}

fn has_attribute(attrs: &[syn::Attribute], name: &str) -> bool {
    attrs.iter().any(|attr| attr.path().is_ident(name))
}

fn parse_capability(item_trait: syn::ItemTrait) -> Result<Capability, MeteringMacrosError> {
    let attr = item_trait
        .attrs
        .iter()
        .find(|a| a.path().is_ident("capability"))
        .unwrap();

    let mut id_string = None;
    if let syn::Meta::List(meta_list) = &attr.meta {
        meta_list
            .parse_nested_meta(|meta| {
                if meta.path.is_ident("id") {
                    if let syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(s),
                        ..
                    }) = meta.value()?.parse()?
                    {
                        id_string = Some(s.value());
                        Ok(())
                    } else {
                        Err(meta.error("capability `id` must be a string literal"))
                    }
                } else {
                    Err(meta
                        .error("unrecognized key for capability attribute, only `id` is supported"))
                }
            })
            .map_err(MeteringMacrosError::SynError)?;
    } else {
        return Err(MeteringMacrosError::InvalidCapabilityAttribute(attr.span()));
    }

    let id_string =
        id_string.ok_or_else(|| MeteringMacrosError::MissingCapabilityId(attr.span()))?;

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
        capability_id_string: id_string.clone(),
        capability_name_ident: item_trait.ident,
        capability_marker_method_name_ident: Ident::new(
            &format!("__marker_{}", id_string),
            attr.span(),
        ),
        capability_method_name_ident: method.sig.ident.clone(),
        capability_method_parameters_token_streams: param_tokens,
        capability_method_parameter_name_idents: param_names,
    })
}

fn parse_context(item_struct: syn::ItemStruct) -> Result<Context, MeteringMacrosError> {
    let attr = item_struct
        .attrs
        .iter()
        .find(|a| a.path().is_ident("context"))
        .unwrap();

    let mut sub_ids = None;
    let mut handlers = None;
    if let syn::Meta::List(meta_list) = &attr.meta {
        meta_list
            .parse_nested_meta(|meta| {
                if meta.path.is_ident("capabilities") {
                    let value = meta.value()?;
                    if let syn::Expr::Array(arr) = value.parse()? {
                        let mut ids = Vec::new();
                        for elem in arr.elems {
                            if let syn::Expr::Lit(syn::ExprLit {
                                lit: syn::Lit::Str(s),
                                ..
                            }) = elem
                            {
                                ids.push(s.value());
                            } else {
                                return Err(meta
                                    .error("`capabilities` must be an array of string literals"));
                            }
                        }
                        sub_ids = Some(ids);
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
                                    return Err(meta.error(
                                        "`handlers` must be an array of simple identifiers",
                                    ));
                                }
                            } else {
                                return Err(
                                    meta.error("`handlers` must be an array of identifiers")
                                );
                            }
                        }
                        handlers = Some(h);
                    } else {
                        return Err(meta.error("`handlers` must be an array literal `[...]`"));
                    }
                }
                Ok(())
            })
            .map_err(MeteringMacrosError::SynError)?;
    } else {
        return Err(MeteringMacrosError::InvalidContextAttribute(attr.span()));
    }

    let sub_ids =
        sub_ids.ok_or_else(|| MeteringMacrosError::ContextMissingCapabilities(attr.span()))?;
    let handlers =
        handlers.ok_or_else(|| MeteringMacrosError::ContextMissingHandlers(attr.span()))?;

    if sub_ids.len() != handlers.len() {
        return Err(MeteringMacrosError::ContextMismatchedHandlers(attr.span()));
    }

    Ok(Context {
        context_name_ident: item_struct.ident,
        context_capability_id_strings_to_handler_idents: sub_ids
            .into_iter()
            .zip(handlers.into_iter())
            .collect(),
    })
}
