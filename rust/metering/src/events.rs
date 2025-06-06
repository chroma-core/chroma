use proc_macro2::{Delimiter, Ident, Spacing, TokenStream, TokenTree};
use quote::quote;
use std::{collections::HashMap, iter::Peekable};

use crate::{
    annotations::{process_secondary_annotation_tokens, SecondaryAnnotation},
    attributes::Attribute,
    errors::MeteringMacrosError,
    fields::{generate_field_definition_token_stream, process_field_definition_tokens, Field},
};

/// Represents a user-defined metering event.
pub struct Event {
    pub foreign_macro_token_streams: Vec<TokenStream>,
    pub maybe_visibility_modifier_token_stream: Option<TokenStream>,
    pub event_name_ident: Ident,
    pub fields: Vec<Field>,
}

/// Accepts tokens from the [`crate::utils::process_token_stream`]'s current token to the end
/// of the input tokens and attempts to return a slice that is known to contain an event definition,
/// given that the last-processed set of tokens was an event annotation.
pub fn collect_event_definition_tokens(
    tokens: &[TokenTree],
) -> Result<(Vec<TokenTree>, usize), MeteringMacrosError> {
    let mut event_definition_tokens: Vec<TokenTree> = Vec::new();
    let mut struct_keyword_ident = None;

    for (token_index, current_token) in tokens.iter().enumerate() {
        event_definition_tokens.push(current_token.clone());

        if struct_keyword_ident.is_none() {
            if let TokenTree::Ident(expected_struct_keyword_ident) = current_token {
                if expected_struct_keyword_ident == "struct" {
                    struct_keyword_ident = Some(expected_struct_keyword_ident.clone());
                }
            }
        } else if let TokenTree::Group(expected_braces_group) = current_token {
            if expected_braces_group.delimiter() == Delimiter::Brace {
                return Ok((event_definition_tokens, token_index + 1));
            }
        }
    }

    Err(MeteringMacrosError::ParseError(
        "Unterminated event definition: never found `{…}` after `struct`".into(),
    ))
}

/// Accepts a slice of tokens that is known to contain a (possibly invalid) [`crate::events::Event`],
/// not including its annotation, and attempts to validate and parse out a representative object.
pub fn process_event_definition_tokens(
    event_definition_tokens: Vec<TokenTree>,
    registered_attributes: &HashMap<String, Attribute>,
) -> Result<Event, MeteringMacrosError> {
    let mut event_definition_tokens_iter: Peekable<_> =
        event_definition_tokens.into_iter().peekable();
    let mut foreign_macro_token_streams: Vec<TokenStream> = Vec::new();

    loop {
        if let Some(TokenTree::Punct(expected_hashtag_punct)) = event_definition_tokens_iter.peek()
        {
            if expected_hashtag_punct.as_char() == '#'
                && expected_hashtag_punct.spacing() == Spacing::Alone
            {
                let hashtag_punct = event_definition_tokens_iter.next().unwrap();

                match event_definition_tokens_iter.next() {
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
    if let Some(TokenTree::Ident(expected_pub_ident)) = event_definition_tokens_iter.peek() {
        if expected_pub_ident == "pub" {
            let mut visibility_modifier_token_stream = TokenStream::new();
            if let TokenTree::Ident(expected_pub_ident) =
                event_definition_tokens_iter.next().unwrap()
            {
                visibility_modifier_token_stream.extend(std::iter::once(TokenTree::Ident(
                    expected_pub_ident.clone(),
                )));
            }

            if let Some(TokenTree::Group(expected_visibility_modifier_group)) =
                event_definition_tokens_iter.peek()
            {
                if expected_visibility_modifier_group.delimiter() == Delimiter::Parenthesis {
                    if let TokenTree::Group(expected_visibility_modifier_group) =
                        event_definition_tokens_iter.next().unwrap()
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

    match event_definition_tokens_iter.next() {
        Some(TokenTree::Ident(expected_struct_ident)) if expected_struct_ident == "struct" => {}
        unexpected => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected `struct` in event definition, found: {:?}",
                unexpected
            )));
        }
    }

    let event_name_ident = match event_definition_tokens_iter.next() {
        Some(TokenTree::Ident(expected_event_name_ident)) => Ident::new(
            &expected_event_name_ident.to_string(),
            expected_event_name_ident.span(),
        ),
        unexpected => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected event (struct) name, found: {:?}",
                unexpected
            )));
        }
    };

    let field_group = match event_definition_tokens_iter.next() {
        Some(TokenTree::Group(expected_field_group))
            if expected_field_group.delimiter() == Delimiter::Brace =>
        {
            expected_field_group
        }
        unexpected => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected a brace-delimited field list after event name, found: {:?}",
                unexpected
            )));
        }
    };

    let field_tokens: Vec<TokenTree> = field_group.stream().into_iter().collect();
    let mut field_tokens_iter: Peekable<_> = field_tokens.into_iter().peekable();
    let mut fields = Vec::new();

    while field_tokens_iter.peek().is_some() {
        let mut field_foreign_macro_token_streams: Vec<TokenStream> = Vec::new();
        let mut expected_field_annotation_tokens: Option<(TokenTree, TokenTree)> = None;
        let mut first_non_annotation_token: Option<TokenTree> = None;

        loop {
            match field_tokens_iter.next() {
                Some(TokenTree::Punct(expected_hashtag_punct))
                    if expected_hashtag_punct.as_char() == '#'
                        && expected_hashtag_punct.spacing() == Spacing::Alone =>
                {
                    match field_tokens_iter.next() {
                        Some(TokenTree::Group(expected_annotation_group)) => {
                            let mut annotation_tokens_iter =
                                expected_annotation_group.stream().into_iter();
                            if let Some(TokenTree::Ident(expected_field_ident)) =
                                annotation_tokens_iter.next()
                            {
                                if expected_field_ident == "field" {
                                    expected_field_annotation_tokens = Some((
                                        TokenTree::Punct(expected_hashtag_punct.clone()),
                                        TokenTree::Group(expected_annotation_group.clone()),
                                    ));
                                    break;
                                }
                            }
                            let mut annotation_token_stream = TokenStream::new();
                            annotation_token_stream.extend(std::iter::once(TokenTree::Punct(
                                expected_hashtag_punct.clone(),
                            )));
                            annotation_token_stream.extend(std::iter::once(TokenTree::Group(
                                expected_annotation_group.clone(),
                            )));
                            field_foreign_macro_token_streams.push(annotation_token_stream);
                            continue;
                        }
                        _ => {
                            return Err(MeteringMacrosError::ParseError(
                                "Expected `[...]` after `#` when collecting annotations".into(),
                            ));
                        }
                    }
                }
                Some(other_token) => {
                    first_non_annotation_token = Some(other_token.clone());
                    break;
                }
                None => {
                    break;
                }
            }
        }

        if first_non_annotation_token.is_none() && expected_field_annotation_tokens.is_some() {
            if let Some(expected_field_name_token) = field_tokens_iter.next() {
                first_non_annotation_token = Some(expected_field_name_token);
            } else {
                return Err(MeteringMacrosError::ParseError(
                    "Expected a field name after `#[field(...)]`, but found none".into(),
                ));
            }
        }

        let mut field_definition_tokens = Vec::new();
        if let Some(first_field_definition_token) = first_non_annotation_token.take() {
            field_definition_tokens.push(first_field_definition_token);
        } else {
            return Err(MeteringMacrosError::ParseError(
                "Expected a field definition (e.g. `foo: u64`), but found none".into(),
            ));
        }

        while let Some(next_token) = field_tokens_iter.peek() {
            if let TokenTree::Punct(expected_comma_punct) = next_token {
                if expected_comma_punct.as_char() == ',' {
                    break;
                }
            }
            field_definition_tokens.push(field_tokens_iter.next().unwrap());
        }

        if let Some(TokenTree::Punct(expected_comma_punct)) = field_tokens_iter.peek() {
            if expected_comma_punct.as_char() == ',' {
                field_tokens_iter.next();
            }
        }

        if let Some((expected_hashtag_punct, secondary_annotation_group)) =
            expected_field_annotation_tokens.take()
        {
            let field_annotation_tokens = vec![
                expected_hashtag_punct.clone(),
                secondary_annotation_group.clone(),
            ];
            let SecondaryAnnotation::Field {
                attribute_name_string,
                custom_mutator_name_ident,
            } = process_secondary_annotation_tokens(&field_annotation_tokens)?;

            let mut field = process_field_definition_tokens(field_definition_tokens)?;
            field.foreign_macro_token_streams = field_foreign_macro_token_streams;

            if let Some(registered_attribute) = registered_attributes.get(&attribute_name_string) {
                field.attribute = Some(registered_attribute.clone());
            } else {
                return Err(MeteringMacrosError::ParseError(format!(
                    "Unknown attribute `{}`",
                    attribute_name_string
                )));
            }
            field.custom_mutator_name_ident = Some(custom_mutator_name_ident);
            fields.push(field);
        } else {
            let mut field = process_field_definition_tokens(field_definition_tokens)?;
            field.foreign_macro_token_streams = field_foreign_macro_token_streams;
            fields.push(field);
        }
    }

    Ok(Event {
        foreign_macro_token_streams,
        maybe_visibility_modifier_token_stream,
        event_name_ident,
        fields,
    })
}

/// Generates the output tokens required for a user-defined metering event.
pub fn generate_event_definition_token_stream(event: &Event) -> TokenStream {
    let Event {
        foreign_macro_token_streams,
        maybe_visibility_modifier_token_stream,
        event_name_ident,
        fields,
    } = event;

    let field_definition_token_streams: Vec<TokenStream> = fields
        .iter()
        .map(generate_field_definition_token_stream)
        .collect();

    let event_definition_token_stream = if maybe_visibility_modifier_token_stream.is_some() {
        quote! {
            #( #foreign_macro_token_streams )*
            #maybe_visibility_modifier_token_stream struct #event_name_ident {
                #(#field_definition_token_streams )*
            }
        }
    } else {
        quote! {
            #( #foreign_macro_token_streams )*
            struct #event_name_ident {
                #( #field_definition_token_streams )*
            }
        }
    };

    let unannotated_field_name_idents: Vec<&Ident> = fields
        .iter()
        .filter(|field| field.attribute.is_none())
        .map(|field| &field.field_name_ident)
        .collect();

    let unannotated_field_type_token_streams: Vec<&TokenStream> = fields
        .iter()
        .filter(|field| field.attribute.is_none())
        .map(|field| &field.field_type_token_stream)
        .collect();

    let attribute_name_idents: Vec<&Ident> = fields
        .iter()
        .filter(|field| field.attribute.is_some())
        .map(|field| &field.attribute.as_ref().unwrap().attribute_name_ident)
        .collect();

    let attribute_type_token_streams: Vec<&TokenStream> = fields
        .iter()
        .filter(|field| field.attribute.is_some())
        .map(|field| {
            &field
                .attribute
                .as_ref()
                .unwrap()
                .attribute_type_token_stream
        })
        .collect();

    let custom_mutator_name_idents: Vec<&Ident> = fields
        .iter()
        .filter_map(|field| field.custom_mutator_name_ident.as_ref())
        .collect();

    quote! {
        #event_definition_token_stream

        impl #event_name_ident {
            pub fn new( #( #unannotated_field_name_idents: #unannotated_field_type_token_streams ),* ) -> Self {
                Self { #( #unannotated_field_name_idents ),*, ..std::default::Default::default() }
            }

            #(
                fn #attribute_name_idents(&mut self, value: #attribute_type_token_streams) {
                    #custom_mutator_name_idents(self, value);
                }
            )*
        }

        impl MeteringEvent for #event_name_ident {
            #(
                fn #attribute_name_idents(&mut self, value: #attribute_type_token_streams) {
                    #custom_mutator_name_idents(self, value);
                }
            )*
        }
    }
}
