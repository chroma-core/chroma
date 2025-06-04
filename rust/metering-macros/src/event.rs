//! This file contains logic for parsing the tokens on which the [`crate::event`]
//! is invoked.

use proc_macro2::{Delimiter, Ident, Literal, Spacing, TokenStream, TokenTree};
use std::{collections::HashMap, iter::Peekable};

use crate::errors::MeteringMacrosError;
use crate::utils::parse_field_annotation;

/// The mutability of a field in a metering event struct.
///
/// Annotated fields are considered mutable, while all other fields are treated as
/// constant by the `chroma-metering` library. Constant fields are initialized once
/// at event creation and cannot be modified by the library afterward.
#[derive(Debug, Eq, PartialEq)]
pub enum FieldMutability {
    /// A field that is constant after initialization and cannot be mutated by the library's methods.
    Constant,

    /// A field that has been annotated and can be mutated by the library's methods.
    Mutable,
}

#[derive(Debug)]
pub struct Field {
    pub field_mutability: FieldMutability,
    pub field_name_ident: Ident,
    pub attribute_name_literal: Option<Literal>,
    pub field_type_token_stream: TokenStream,
}

pub struct EventBodyParseResult {
    pub event_name_ident: Ident,
    pub fields: Vec<Field>,
    pub field_name_ident_to_mutator_name_literal: HashMap<Ident, Literal>,
}

pub fn parse_event_body(
    token_stream: &TokenStream,
) -> Result<EventBodyParseResult, MeteringMacrosError> {
    // Create an iterator for the token stream.
    let tokens_iter: Vec<TokenTree> = token_stream.clone().into_iter().collect();

    // Ignore other macro invocations before the struct definition.
    let mut current_token_index = 0;
    while current_token_index + 1 < tokens_iter.len() {
        match (
            &tokens_iter[current_token_index],
            &tokens_iter[current_token_index + 1],
        ) {
            (TokenTree::Punct(punct), TokenTree::Group(group))
                if punct.as_char() == '#' && group.delimiter() == Delimiter::Bracket =>
            {
                current_token_index += 2;
                continue;
            }
            _ => break,
        }
    }

    // If we are out of tokens after ignoring macro invocations, then
    // we do not have a valid struct.
    if current_token_index + 2 >= tokens_iter.len() {
        return Err(MeteringMacrosError::EventBodyError);
    }

    // Validate that we are working with a valid struct definition.
    let expected_struct_keyword_token_tree = &tokens_iter[current_token_index];
    let expected_event_name_token_tree = &tokens_iter[current_token_index + 1];
    let expected_struct_token_tree = &tokens_iter[current_token_index + 2];
    let (event_name_ident, struct_group) = match (
        expected_struct_keyword_token_tree,
        expected_event_name_token_tree,
        expected_struct_token_tree,
    ) {
        (
            TokenTree::Ident(expected_struct_keyword_ident),
            TokenTree::Ident(expected_event_name_ident),
            TokenTree::Group(expected_struct_group),
        ) if expected_struct_keyword_ident.to_string() == "struct"
            && expected_struct_group.delimiter() == Delimiter::Brace =>
        {
            (
                expected_event_name_ident.clone(),
                expected_struct_group.clone(),
            )
        }
        _ => return Err(MeteringMacrosError::EventBodyError),
    };

    // Collect the tokens that comprise the struct into an iterator.
    let mut struct_tokens_iter: Peekable<proc_macro2::token_stream::IntoIter> =
        struct_group.stream().into_iter().peekable();

    // Store the fields we parse while iterating through the struct's tokens.
    let mut fields = Vec::new();

    // Assemble a map of field names to mutator names so we know what functions to call
    // when implementing the mutator overrides for this event.
    let mut field_name_ident_to_mutator_name_literal: HashMap<Ident, proc_macro2::Literal> =
        HashMap::new();

    // Iterate over the struct's tokens.
    while let Some(current_token_tree) = struct_tokens_iter.next() {
        // If we see a comma, it's just a delimiter between struct fields, so ignore it.
        if let TokenTree::Punct(ref punct) = current_token_tree {
            if punct.as_char() == ',' {
                continue;
            }
        }

        // Handle field annotations.
        if let TokenTree::Punct(ref punct) = current_token_tree {
            if punct.as_char() == '#' {
                // If the `#` isn't followed by a group delimited with [...], the syntax is not valid.
                let field_annotation_group = match struct_tokens_iter.next() {
                    Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Bracket => g,
                    _ => return Err(MeteringMacrosError::EventBodyError),
                };

                // Collect the tokens that comprise the field annotation into an iterator.
                let mut field_annotation_tokens_iter =
                    field_annotation_group.stream().into_iter().peekable();

                // Check if we are looking at a field annotation. If we are not, we will treat it as
                // an annotation for another crate and ignore it.
                match field_annotation_tokens_iter.next() {
                    Some(TokenTree::Ident(expected_field_ident))
                        if expected_field_ident == "field" => {}
                    _ => continue,
                }

                // Once we have verified that we are working with a field annotation, we expect
                // to see parentheses to delimit a group containing the arguments to the
                // annotation.
                let field_annotation_args_group = match field_annotation_tokens_iter.next() {
                    Some(TokenTree::Group(group))
                        if group.delimiter() == Delimiter::Parenthesis =>
                    {
                        group
                    }
                    _ => return Err(MeteringMacrosError::EventBodyError),
                };

                // Pass the annotation's arguments to a helper function to extract the attribute
                // and mutator names
                let (attribute_name_literal, mutator_name_literal) =
                    parse_field_annotation(&field_annotation_args_group.stream())?;

                // After parsing the annotation, we expect to see a field name assigned to a type.
                let field_name_ident = match struct_tokens_iter.next() {
                    Some(TokenTree::Ident(ident)) => ident,
                    _ => return Err(MeteringMacrosError::EventBodyError),
                };

                // Validate that a colon separates the field name from its type.
                match struct_tokens_iter.next() {
                    Some(TokenTree::Punct(expected_colon_punct))
                        if expected_colon_punct.as_char() == ':'
                            && expected_colon_punct.spacing() == Spacing::Alone => {}
                    _ => return Err(MeteringMacrosError::EventBodyError),
                }

                // Collect the tokens that comprise the type assigned to the field by iterating
                // until we encounter a comma.
                let mut field_type_token_stream = TokenStream::new();
                while let Some(next_token_tree) = struct_tokens_iter.peek() {
                    match next_token_tree {
                        // A comma is our terminator since struct fields are separated by commas.
                        TokenTree::Punct(punct)
                            if punct.as_char() == ',' && punct.spacing() == Spacing::Alone =>
                        {
                            struct_tokens_iter.next();
                            break;
                        }
                        // We include all non-comma tokens in the type definition.
                        non_comma_token => {
                            field_type_token_stream.extend([non_comma_token.clone()]);
                            struct_tokens_iter.next();
                        }
                    }
                }

                // Construct a field object and push it into our final fields vector.
                fields.push(Field {
                    // We know this field is mutable because it is annotated.
                    field_mutability: FieldMutability::Mutable,
                    field_name_ident: field_name_ident.clone(),
                    attribute_name_literal: Some(attribute_name_literal),
                    field_type_token_stream: field_type_token_stream.clone(),
                });

                // Save the name of the mutator keyed by the field name to know which
                // mutators should be applied to which fields when generating code.
                field_name_ident_to_mutator_name_literal
                    .insert(field_name_ident, mutator_name_literal.clone());
                continue;
            }
        }

        // If we are not looking at an annotation field, then we expect to see a valid field name
        // for a constant (non-annotated) field.
        let field_name_ident = if let TokenTree::Ident(ident) = current_token_tree {
            ident
        } else {
            return Err(MeteringMacrosError::EventBodyError);
        };

        // Validate that the field name is separated from its type with a colon.
        match struct_tokens_iter.next() {
            Some(TokenTree::Punct(expected_colon_punct))
                if expected_colon_punct.as_char() == ':'
                    && expected_colon_punct.spacing() == Spacing::Alone => {}
            _ => return Err(MeteringMacrosError::EventBodyError),
        }

        // Collect the tokens that comprise the type assigned to the field by iterating
        // until we encounter a comma.
        let mut field_type_token_stream = TokenStream::new();
        while let Some(next_token_tree) = struct_tokens_iter.peek() {
            match next_token_tree {
                // A comma is our terminator since struct fields are separated by commas.
                TokenTree::Punct(punct)
                    if punct.as_char() == ',' && punct.spacing() == Spacing::Alone =>
                {
                    struct_tokens_iter.next();
                    break;
                }
                // We include all non-comma tokens in the type definition.
                non_comma_token => {
                    field_type_token_stream.extend([non_comma_token.clone()]);
                    struct_tokens_iter.next();
                }
            }
        }

        // Construct a field object and push it into our final fields vector.
        fields.push(Field {
            // We know the field mutability is constant because this field is not annotated.
            field_mutability: FieldMutability::Constant,
            field_name_ident: field_name_ident.clone(),
            attribute_name_literal: None,
            field_type_token_stream: field_type_token_stream.clone(),
        });
    }

    // Return the parsed event.
    Ok(EventBodyParseResult {
        event_name_ident,
        fields,
        field_name_ident_to_mutator_name_literal,
    })
}
