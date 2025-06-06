use proc_macro2::{Delimiter, Ident, Spacing, TokenStream, TokenTree};
use quote::quote;
use std::iter::Peekable;

use crate::errors::MeteringMacrosError;

/// Represents a user-defined metering attribute.
#[derive(Clone)]
pub struct Attribute {
    pub foreign_macro_token_streams: Vec<TokenStream>,
    pub maybe_visibility_modifier_token_stream: Option<TokenStream>,
    pub attribute_type_alias_ident: Ident,
    pub attribute_name_string: String,
    pub attribute_name_ident: Ident,
    pub attribute_type_string: String,
    pub attribute_type_token_stream: TokenStream,
}

/// Accepts the tokens from [`crate::utils::process_token_stream`]'s current location
/// to the end of the input and attempts to slice out the tokens containing an attribute
/// definition, given that an attribute annotation was just processed.
pub fn collect_attribute_definition_tokens(
    tokens: &[TokenTree],
) -> Result<(Vec<TokenTree>, usize), MeteringMacrosError> {
    let mut attribute_definition_tokens = Vec::new();
    for (token_index, current_token) in tokens.iter().enumerate() {
        attribute_definition_tokens.push(current_token.clone());
        if let TokenTree::Punct(expected_semicolon_punct) = current_token {
            if expected_semicolon_punct.as_char() == ';' {
                return Ok((attribute_definition_tokens, token_index + 1));
            }
        }
    }
    Err(MeteringMacrosError::ParseError(
        "Unterminated attribute definition: missing `;`".into(),
    ))
}

/// Accepts a slice of tokens known to contain a (possibly invalid) attribute definition,
/// not including the annotation, and attempts to validate and parse out an [`crate::attributes::Attribute`].
pub fn process_attribute_definition_tokens(
    attribute_definition_tokens: Vec<TokenTree>,
    attribute_name_string: String,
) -> Result<Attribute, MeteringMacrosError> {
    let mut attribute_definition_tokens_iter: Peekable<_> =
        attribute_definition_tokens.into_iter().peekable();
    let mut foreign_macro_token_streams: Vec<TokenStream> = Vec::new();

    loop {
        if let Some(TokenTree::Punct(expected_hashtag_punct)) =
            attribute_definition_tokens_iter.peek()
        {
            if expected_hashtag_punct.as_char() == '#'
                && expected_hashtag_punct.spacing() == Spacing::Alone
            {
                let hashtag_punct = attribute_definition_tokens_iter.next().unwrap();

                match attribute_definition_tokens_iter.next() {
                    Some(TokenTree::Group(expected_foreign_macro_group))
                        if expected_foreign_macro_group.delimiter() == Delimiter::Bracket =>
                    {
                        let mut foreign_macro_token_stream = TokenStream::new();
                        foreign_macro_token_stream.extend(std::iter::once(hashtag_punct.clone()));
                        foreign_macro_token_stream.extend(std::iter::once(TokenTree::Group(
                            expected_foreign_macro_group.clone(),
                        )));
                        foreign_macro_token_streams.push(foreign_macro_token_stream);
                        continue;
                    }
                    Some(unexpected) => {
                        return Err(MeteringMacrosError::ParseError(format!(
                            "Expected `#[…]` for foreign macro, found: {:?}",
                            unexpected
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

    let mut maybe_visibility_modifier_token_stream = None;
    if let Some(TokenTree::Ident(expected_pub_ident)) = attribute_definition_tokens_iter.peek() {
        if expected_pub_ident == "pub" {
            let mut visibility_modifier_token_stream = TokenStream::new();
            if let TokenTree::Ident(expected_pub_ident) =
                attribute_definition_tokens_iter.next().unwrap()
            {
                visibility_modifier_token_stream.extend(std::iter::once(TokenTree::Ident(
                    expected_pub_ident.clone(),
                )));
            }

            if let Some(TokenTree::Group(expected_visibility_modifier_group)) =
                attribute_definition_tokens_iter.peek()
            {
                if expected_visibility_modifier_group.delimiter() == Delimiter::Parenthesis {
                    if let TokenTree::Group(expected_visibility_modifier_group) =
                        attribute_definition_tokens_iter.next().unwrap()
                    {
                        visibility_modifier_token_stream.extend(std::iter::once(TokenTree::Group(
                            expected_visibility_modifier_group.clone(),
                        )));
                    }
                }
            }

            maybe_visibility_modifier_token_stream = Some(visibility_modifier_token_stream);
        }
    }

    match attribute_definition_tokens_iter.next() {
        Some(TokenTree::Ident(expected_type_keyword_ident))
            if expected_type_keyword_ident == "type" => {}
        unexpected => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected `type` keyword, found: {:?}",
                unexpected
            )));
        }
    }

    let attribute_type_alias_ident = match attribute_definition_tokens_iter.next() {
        Some(TokenTree::Ident(expected_attribute_type_alias_ident)) => Ident::new(
            &expected_attribute_type_alias_ident.to_string(),
            expected_attribute_type_alias_ident.span(),
        ),
        unexpected => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected type alias identifier after `type`, found: {:?}",
                unexpected
            )));
        }
    };

    match attribute_definition_tokens_iter.next() {
        Some(TokenTree::Punct(expected_equals_punct)) if expected_equals_punct.as_char() == '=' => {
        }
        unexpected => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected `=` after alias, found: {:?}",
                unexpected
            )));
        }
    }

    let mut attribute_type_tokens = Vec::new();
    let mut semicolon_punct = None;
    for current_token in attribute_definition_tokens_iter {
        if let TokenTree::Punct(expected_semicolon_punct) = &current_token {
            if expected_semicolon_punct.as_char() == ';' {
                semicolon_punct = Some(expected_semicolon_punct.clone());
                break;
            }
        }
        attribute_type_tokens.push(current_token.clone());
    }
    if semicolon_punct.is_none() {
        return Err(MeteringMacrosError::ParseError(
            "Missing terminating `;` in attribute definition".into(),
        ));
    }

    let attribute_type_token_stream: TokenStream = attribute_type_tokens.iter().cloned().collect();
    let attribute_type_string = attribute_type_token_stream.to_string();

    let attribute_name_ident = Ident::new(&attribute_name_string, proc_macro2::Span::call_site());

    Ok(Attribute {
        foreign_macro_token_streams,
        maybe_visibility_modifier_token_stream,
        attribute_type_alias_ident,
        attribute_name_string,
        attribute_name_ident,
        attribute_type_string,
        attribute_type_token_stream,
    })
}

/// Generates the output token stream for a user-defined attribute.
pub fn generate_attribute_definition_token_stream(attribute: &Attribute) -> TokenStream {
    let Attribute {
        foreign_macro_token_streams,
        maybe_visibility_modifier_token_stream,
        attribute_type_alias_ident,
        attribute_name_string: _attribute_name_string,
        attribute_name_ident: _attribute_name_ident,
        attribute_type_string: _attribute_type_string,
        attribute_type_token_stream,
    } = attribute;

    if maybe_visibility_modifier_token_stream.is_some() {
        quote! {
            #( #foreign_macro_token_streams )*
            #maybe_visibility_modifier_token_stream type #attribute_type_alias_ident = #attribute_type_token_stream;
        }
    } else {
        quote! {
            #( #foreign_macro_token_streams )*
            type #attribute_type_alias_ident = #attribute_type_token_stream;
        }
    }
}
