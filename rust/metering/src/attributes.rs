use proc_macro2::{Delimiter, Ident, TokenStream, TokenTree};
use quote::quote;

use crate::errors::MeteringMacrosError;

#[derive(Clone)]
pub struct Attribute {
    pub foreign_macro_token_streams: Vec<TokenStream>,
    pub visibility_modifier_ident: Option<Ident>,
    pub attribute_type_alias_ident: Ident,
    pub attribute_name_string: String,
    pub attribute_name_ident: Ident,
    pub attribute_type_string: String,
    pub attribute_type_token_stream: TokenStream,
}

pub fn collect_attribute_definition_tokens(
    tokens: &[TokenTree],
) -> Result<(Vec<TokenTree>, usize), MeteringMacrosError> {
    let mut def_tokens = Vec::new();
    for (j, tt) in tokens.iter().enumerate() {
        def_tokens.push(tt.clone());
        if let TokenTree::Punct(p) = tt {
            if p.as_char() == ';' {
                return Ok((def_tokens, j + 1));
            }
        }
    }
    Err(MeteringMacrosError::ParseError(
        "Unterminated attribute definition: missing `;`".into(),
    ))
}

pub fn process_attribute_definition_tokens(
    def_tokens: Vec<TokenTree>,
    attribute_name_string: String,
) -> Result<Attribute, MeteringMacrosError> {
    let mut iter = def_tokens.into_iter().peekable();
    let mut foreign_macro_token_streams: Vec<TokenStream> = Vec::new();
    let mut visibility_modifier_ident: Option<Ident> = None;

    if let Some(TokenTree::Ident(ident)) = iter.peek() {
        if ident == "pub" {
            let pub_ident = if let TokenTree::Ident(i) = iter.next().unwrap() {
                Ident::new(&i.to_string(), i.span())
            } else {
                unreachable!()
            };
            visibility_modifier_ident = Some(pub_ident);

            if let Some(TokenTree::Group(group)) = iter.peek() {
                if group.delimiter() == Delimiter::Parenthesis {
                    let _ = iter.next().unwrap();
                }
            }
        }
    }

    match iter.next() {
        Some(TokenTree::Ident(ident)) if ident == "type" => {}
        other => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected `type` keyword, found: {:?}",
                other
            )));
        }
    }

    let alias_ident = match iter.next() {
        Some(TokenTree::Ident(ident)) => Ident::new(&ident.to_string(), ident.span()),
        other => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected type alias identifier after `type`, found: {:?}",
                other
            )));
        }
    };

    match iter.next() {
        Some(TokenTree::Punct(p)) if p.as_char() == '=' => {}
        other => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected `=` after alias, found: {:?}",
                other
            )));
        }
    }

    let mut rhs_tokens = Vec::new();
    let mut found_semicolon = false;
    while let Some(tt) = iter.next() {
        if let TokenTree::Punct(p) = &tt {
            if p.as_char() == ';' {
                found_semicolon = true;
                break;
            }
        }
        rhs_tokens.push(tt.clone());
    }
    if !found_semicolon {
        return Err(MeteringMacrosError::ParseError(
            "Missing terminating `;` in attribute definition".into(),
        ));
    }

    let rhs_ts: TokenStream = rhs_tokens.iter().cloned().collect();
    let attribute_type_string = rhs_ts.to_string();

    let attribute_name_ident = Ident::new(&attribute_name_string, proc_macro2::Span::call_site());

    Ok(Attribute {
        foreign_macro_token_streams,
        visibility_modifier_ident,
        attribute_type_alias_ident: alias_ident,
        attribute_name_string,
        attribute_name_ident,
        attribute_type_string,
        attribute_type_token_stream: rhs_ts,
    })
}

pub fn generate_attribute_definition_token_stream(attribute: &Attribute) -> TokenStream {
    let Attribute {
        foreign_macro_token_streams: foreign_macro_token_streams,
        visibility_modifier_ident: visibility_modifier_ident,
        attribute_type_alias_ident: attribute_type_alias_ident,
        attribute_name_string: _attribute_name_string,
        attribute_name_ident: _attribute_name_ident,
        attribute_type_string: _attribute_type_string,
        attribute_type_token_stream: attribute_type_token_stream,
    } = attribute;

    let attribute_definition_token_stream = if visibility_modifier_ident.is_some() {
        quote! {
            #( #foreign_macro_token_streams )*
            #visibility_modifier_ident type #attribute_type_alias_ident = #attribute_type_token_stream;
        }
    } else {
        quote! {
            #( #foreign_macro_token_streams )*
            type #attribute_type_alias_ident = #attribute_type_token_stream;
        }
    };

    return attribute_definition_token_stream;
}
