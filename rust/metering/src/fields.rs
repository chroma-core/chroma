use proc_macro2::{Delimiter, Ident, Spacing, TokenStream, TokenTree};
use quote::quote;
use std::iter::Peekable;

use crate::{attributes::Attribute, errors::MeteringMacrosError};

pub struct Field {
    pub foreign_macro_token_streams: Vec<TokenStream>,
    pub visibility_modifier_ident: Option<Ident>,
    pub field_name_ident: Ident,
    pub field_type_token_stream: TokenStream,
    pub attribute: Option<Attribute>,
    pub custom_mutator_name_ident: Option<Ident>,
}

pub fn process_field_definition_tokens(
    field_tokens: Vec<TokenTree>,
) -> Result<Field, MeteringMacrosError> {
    let mut iter: Peekable<_> = field_tokens.into_iter().peekable();
    let mut foreign_macro_token_streams: Vec<TokenStream> = Vec::new();
    let mut visibility_modifier_ident: Option<Ident> = None;

    loop {
        if let Some(TokenTree::Punct(punct)) = iter.peek() {
            if punct.as_char() == '#' && punct.spacing() == Spacing::Alone {
                let hash_tt = iter.next().unwrap();

                match iter.next() {
                    Some(TokenTree::Group(group)) if group.delimiter() == Delimiter::Bracket => {
                        let mut ts = TokenStream::new();
                        ts.extend(std::iter::once(hash_tt.clone()));
                        ts.extend(std::iter::once(TokenTree::Group(group.clone())));
                        foreign_macro_token_streams.push(ts);
                        continue;
                    }
                    Some(other) => {
                        return Err(MeteringMacrosError::ParseError(format!(
                            "Expected `#[...]` for foreign macro, found: {:?}",
                            other
                        )));
                    }
                    None => {
                        return Err(MeteringMacrosError::ParseError(
                            "Unexpected end after `#` when parsing foreign macro".into(),
                        ));
                    }
                }
            }
        }
        break;
    }

    if let Some(TokenTree::Ident(ident)) = iter.peek() {
        if ident == "pub" {
            if let TokenTree::Ident(i) = iter.next().unwrap() {
                visibility_modifier_ident = Some(Ident::new(&i.to_string(), i.span()));
            }

            if let Some(TokenTree::Group(group)) = iter.peek() {
                if group.delimiter() == Delimiter::Parenthesis {
                    let _ = iter.next().unwrap();
                }
            }
        }
    }

    let field_name_ident = match iter.next() {
        Some(TokenTree::Ident(ident)) => Ident::new(&ident.to_string(), ident.span()),
        other => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected field name, found: {:?}",
                other
            )));
        }
    };

    match iter.next() {
        Some(TokenTree::Punct(p)) if p.as_char() == ':' => {}
        other => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected `:` after field name, found: {:?}",
                other
            )));
        }
    }

    let mut ty_tokens: Vec<TokenTree> = Vec::new();
    while let Some(tt) = iter.next() {
        ty_tokens.push(tt.clone());
    }
    let ty_ts: TokenStream = ty_tokens.into_iter().collect();

    Ok(Field {
        foreign_macro_token_streams,
        visibility_modifier_ident,
        field_name_ident,
        field_type_token_stream: ty_ts,
        attribute: None,
        custom_mutator_name_ident: None,
    })
}

pub fn generate_field_definition_token_stream(field: &Field) -> TokenStream {
    let Field {
        foreign_macro_token_streams,
        visibility_modifier_ident,
        field_name_ident,
        field_type_token_stream,
        attribute,
        custom_mutator_name_ident: _custom_mutator_name_ident,
    } = field;

    let field_definition_token_stream = if visibility_modifier_ident.is_some() {
        if let Some(Attribute {
            foreign_macro_token_streams: _foreign_macro_token_streams,
            visibility_modifier_ident: _visibility_modifier_ident,
            attribute_type_alias_ident,
            attribute_name_string: _attribute_name_string,
            attribute_name_ident: _attribute_name_ident,
            attribute_type_string: _attribute_type_string,
            attribute_type_token_stream: _attribute_type_token_stream,
        }) = attribute
        {
            quote! {
                #( #foreign_macro_token_streams )*
                #visibility_modifier_ident #field_name_ident: #attribute_type_alias_ident,
            }
        } else {
            quote! {
                #( #foreign_macro_token_streams )*
                #visibility_modifier_ident #field_name_ident: #field_type_token_stream,
            }
        }
    } else {
        if let Some(Attribute {
            foreign_macro_token_streams: _foreign_macro_token_streams,
            visibility_modifier_ident: _visibility_modifier_ident,
            attribute_type_alias_ident,
            attribute_name_string: _attribute_name_string,
            attribute_name_ident: _attribute_name_ident,
            attribute_type_string: _attribute_type_string,
            attribute_type_token_stream: _attribute_type_token_stream,
        }) = attribute
        {
            quote! {
                #( #foreign_macro_token_streams )*
                #visibility_modifier_ident #field_name_ident: #attribute_type_alias_ident,
            }
        } else {
            quote! {
                #( #foreign_macro_token_streams )*
                #visibility_modifier_ident #field_name_ident: #field_type_token_stream,
            }
        }
    };

    return field_definition_token_stream;
}
