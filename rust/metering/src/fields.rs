use proc_macro2::{Delimiter, Ident, Spacing, TokenStream, TokenTree};
use quote::quote;
use std::iter::Peekable;

use crate::{attributes::Attribute, errors::MeteringMacrosError};

/// Represents a field within a user-defined metering event.
pub struct Field {
    pub foreign_macro_token_streams: Vec<TokenStream>,
    pub maybe_visibility_modifier_token_stream: Option<TokenStream>,
    pub field_name_ident: Ident,
    pub field_type_token_stream: TokenStream,
    pub attribute: Option<Attribute>,
    pub custom_mutator_name_ident: Option<Ident>,
}

/// Accepts a slice of tokens that contains a (possibly invalid) field definition,
/// not including its annotation, and attempts to parse out a [`crate::fields::Field`].
pub fn process_field_definition_tokens(
    field_definition_tokens: Vec<TokenTree>,
) -> Result<Field, MeteringMacrosError> {
    let mut field_definition_tokens_iter: Peekable<_> =
        field_definition_tokens.into_iter().peekable();
    let mut foreign_macro_token_streams: Vec<TokenStream> = Vec::new();

    loop {
        if let Some(TokenTree::Punct(expected_hashtag_punct)) = field_definition_tokens_iter.peek()
        {
            if expected_hashtag_punct.as_char() == '#'
                && expected_hashtag_punct.spacing() == Spacing::Alone
            {
                let hashtag_punct = field_definition_tokens_iter.next().unwrap();

                match field_definition_tokens_iter.next() {
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
                            "Expected `#[...]` for foreign macro, found: {:?}",
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
    if let Some(TokenTree::Ident(expected_pub_ident)) = field_definition_tokens_iter.peek() {
        if expected_pub_ident == "pub" {
            let mut visibility_modifier_token_stream = TokenStream::new();
            if let TokenTree::Ident(expected_pub_ident) =
                field_definition_tokens_iter.next().unwrap()
            {
                visibility_modifier_token_stream.extend(std::iter::once(TokenTree::Ident(
                    expected_pub_ident.clone(),
                )));
            }

            if let Some(TokenTree::Group(expected_visibility_modifier_group)) =
                field_definition_tokens_iter.peek()
            {
                if expected_visibility_modifier_group.delimiter() == Delimiter::Parenthesis {
                    if let TokenTree::Group(expected_visibility_modifier_group) =
                        field_definition_tokens_iter.next().unwrap()
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

    let field_name_ident = match field_definition_tokens_iter.next() {
        Some(TokenTree::Ident(expected_field_name_ident)) => Ident::new(
            &expected_field_name_ident.to_string(),
            expected_field_name_ident.span(),
        ),
        unexpected => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected field name, found: {:?}",
                unexpected
            )));
        }
    };

    match field_definition_tokens_iter.next() {
        Some(TokenTree::Punct(expected_colon_punct)) if expected_colon_punct.as_char() == ':' => {}
        unexpected => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected `:` after field name, found: {:?}",
                unexpected
            )));
        }
    }

    let mut field_type_tokens: Vec<TokenTree> = Vec::new();
    while let Some(current_token) = field_definition_tokens_iter.next() {
        field_type_tokens.push(current_token.clone());
    }
    let field_type_token_stream: TokenStream = field_type_tokens.into_iter().collect();

    Ok(Field {
        foreign_macro_token_streams,
        maybe_visibility_modifier_token_stream,
        field_name_ident,
        field_type_token_stream,
        // These are populated upstream for annotated fields
        attribute: None,
        custom_mutator_name_ident: None,
    })
}

/// Generates the output tokens for an individual field within a struct that defines a metering event.
pub fn generate_field_definition_token_stream(field: &Field) -> TokenStream {
    let Field {
        foreign_macro_token_streams,
        maybe_visibility_modifier_token_stream,
        field_name_ident,
        field_type_token_stream,
        attribute,
        custom_mutator_name_ident: _custom_mutator_name_ident,
    } = field;

    let field_definition_token_stream = if maybe_visibility_modifier_token_stream.is_some() {
        if let Some(Attribute {
            foreign_macro_token_streams: _foreign_macro_token_streams,
            maybe_visibility_modifier_token_stream: _maybe_visibility_modifier_token_stream,
            attribute_type_alias_ident,
            attribute_name_string: _attribute_name_string,
            attribute_name_ident: _attribute_name_ident,
            attribute_type_string: _attribute_type_string,
            attribute_type_token_stream: _attribute_type_token_stream,
        }) = attribute
        {
            quote! {
                #( #foreign_macro_token_streams )*
                #maybe_visibility_modifier_token_stream #field_name_ident: #attribute_type_alias_ident,
            }
        } else {
            quote! {
                #( #foreign_macro_token_streams )*
                #maybe_visibility_modifier_token_stream #field_name_ident: #field_type_token_stream,
            }
        }
    } else {
        if let Some(Attribute {
            foreign_macro_token_streams: _foreign_macro_token_streams,
            maybe_visibility_modifier_token_stream: _maybe_visibility_modifier_token_stream,
            attribute_type_alias_ident,
            attribute_name_string: _attribute_name_string,
            attribute_name_ident: _attribute_name_ident,
            attribute_type_string: _attribute_type_string,
            attribute_type_token_stream: _attribute_type_token_stream,
        }) = attribute
        {
            quote! {
                #( #foreign_macro_token_streams )*
                #maybe_visibility_modifier_token_stream #field_name_ident: #attribute_type_alias_ident,
            }
        } else {
            quote! {
                #( #foreign_macro_token_streams )*
                #maybe_visibility_modifier_token_stream #field_name_ident: #field_type_token_stream,
            }
        }
    };

    return field_definition_token_stream;
}
