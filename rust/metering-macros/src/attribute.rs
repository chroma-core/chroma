use heck::ToUpperCamelCase;
use proc_macro2::{Ident, Literal, Spacing, TokenStream, TokenTree};

use crate::errors::MeteringMacrosError;

pub struct AttributeArgsResult {
    pub attribute_name_string: String,
    pub attribute_name_literal: Literal,
}

pub fn process_attribute_args(
    token_stream: &TokenStream,
) -> Result<AttributeArgsResult, MeteringMacrosError> {
    let tokens: Vec<TokenTree> = token_stream.clone().into_iter().collect();

    match tokens.as_slice() {
        [TokenTree::Ident(ident1), TokenTree::Punct(punct1), TokenTree::Literal(literal1)] => {
            if (ident1.to_string() != "name")
                || (punct1.as_char() != '=' || punct1.spacing() != Spacing::Alone)
            {
                return Err(MeteringMacrosError::AttributeArgsError);
            }

            let attribute_name = literal1.to_string().trim_matches('"').to_string();

            Ok(AttributeArgsResult {
                attribute_name_string: attribute_name,
                attribute_name_literal: literal1.clone(),
            })
        }

        _ => Err(MeteringMacrosError::AttributeArgsError),
    }
}

pub struct AttributeBodyResult {
    pub attribute_type_name: Ident,
    pub attribute_type: TokenStream,
}

pub fn process_attribute_body(
    token_stream: &TokenStream,
    attribute_name: &str,
) -> Result<AttributeBodyResult, MeteringMacrosError> {
    let mut token_stream_iter = token_stream.clone().into_iter().peekable();

    let expected_type_keyword = token_stream_iter
        .next()
        .ok_or(MeteringMacrosError::AttributeBodyError)?;
    match expected_type_keyword {
        TokenTree::Ident(ref ident) if ident == "type" => {}
        _ => return Err(MeteringMacrosError::AttributeBodyError),
    }

    let expected_type_alias = token_stream_iter
        .next()
        .ok_or(MeteringMacrosError::AttributeBodyError)?;
    let type_name_ident = match expected_type_alias {
        TokenTree::Ident(ref ident) if ident == &attribute_name.to_upper_camel_case() => {
            ident.clone()
        }
        _ => return Err(MeteringMacrosError::AttributeBodyError),
    };

    let expected_punct_equals = token_stream_iter
        .next()
        .ok_or(MeteringMacrosError::AttributeBodyError)?;
    match expected_punct_equals {
        TokenTree::Punct(ref p) if p.as_char() == '=' && p.spacing() == Spacing::Alone => {}
        _ => return Err(MeteringMacrosError::AttributeBodyError),
    }

    let mut collected: Vec<TokenTree> = Vec::new();
    while let Some(tt) = token_stream_iter.peek() {
        match tt {
            TokenTree::Punct(p) if p.as_char() == ';' && p.spacing() == Spacing::Alone => {
                break;
            }
            // Otherwise, consume it as part of the RHS type.
            _ => {
                let next_tt = token_stream_iter.next().unwrap();
                collected.push(next_tt);
            }
        }
    }

    // 6) Now expect exactly one final semicolon
    let semicolon = token_stream_iter
        .next()
        .ok_or(MeteringMacrosError::AttributeBodyError)?;
    match semicolon {
        TokenTree::Punct(ref p) if p.as_char() == ';' && p.spacing() == Spacing::Alone => {}
        _ => return Err(MeteringMacrosError::AttributeBodyError),
    }

    // 7) There must be no more tokens after that semicolon
    if token_stream_iter.next().is_some() {
        return Err(MeteringMacrosError::AttributeBodyError);
    }

    // 8) Reconstruct a TokenStream for the collected “type” tokens
    let attribute_type = TokenStream::from_iter(collected.into_iter());

    Ok(AttributeBodyResult {
        attribute_type_name: type_name_ident,
        attribute_type,
    })
}
