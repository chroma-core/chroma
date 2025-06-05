use proc_macro2::{Delimiter, Group, Ident, Literal, Punct, Spacing, Span, TokenStream, TokenTree};
use std::collections::HashMap;

use crate::{
    annotations::{
        collect_annotation_tokens, process_primary_annotation_tokens, AnnotationLevel,
        PrimaryAnnotation,
    },
    attributes::{
        collect_attribute_definition_tokens, process_attribute_definition_tokens, Attribute,
    },
    errors::MeteringMacrosError,
    events::{collect_event_definition_tokens, process_event_definition_tokens, Event},
};

/// Processes the full token stream that is passed into [`crate::initialize_metering`] by
/// attempting to parse out vectors of attributes and events.
pub fn process_token_stream(
    token_stream: &TokenStream,
) -> Result<(Vec<Attribute>, Vec<Event>), MeteringMacrosError> {
    let tokens: Vec<TokenTree> = token_stream.clone().into_iter().collect();

    let mut attributes: Vec<Attribute> = Vec::new();
    let mut events: Vec<Event> = Vec::new();
    let mut registered_attributes: HashMap<String, Attribute> = HashMap::new();

    let mut current_token_index = 0;
    while current_token_index < tokens.len() {
        match &tokens[current_token_index] {
            TokenTree::Punct(expected_hashtag_punct)
                if expected_hashtag_punct.as_char() == '#'
                    && expected_hashtag_punct.spacing() == Spacing::Alone =>
            {
                let (annotation_level, annotation_tokens, tokens_consumed_by_annotation) =
                    collect_annotation_tokens(&tokens[current_token_index..])?;

                if let AnnotationLevel::Secondary = annotation_level {
                    return Err(MeteringMacrosError::ParseError(
                        "Found `#[field(...)]` outside of an event".into(),
                    ));
                }

                let primary_annotation = process_primary_annotation_tokens(&annotation_tokens)?;
                match primary_annotation {
                    PrimaryAnnotation::Attribute {
                        attribute_name_string,
                    } => {
                        let attribute_definition_start_token_index =
                            current_token_index + tokens_consumed_by_annotation;

                        let (attribute_definition_tokens, tokens_consumed_by_attribute_definition) =
                            collect_attribute_definition_tokens(
                                &tokens[attribute_definition_start_token_index..],
                            )?;

                        let attribute = process_attribute_definition_tokens(
                            attribute_definition_tokens,
                            attribute_name_string.clone(),
                        )?;

                        registered_attributes
                            .insert(attribute_name_string.clone(), attribute.clone());
                        attributes.push(attribute);

                        current_token_index = attribute_definition_start_token_index
                            + tokens_consumed_by_attribute_definition;
                    }
                    PrimaryAnnotation::Event => {
                        let event_definition_start_token_index =
                            current_token_index + tokens_consumed_by_annotation;

                        let (event_definition_tokens, tokens_consumed_by_event_definition) =
                            collect_event_definition_tokens(
                                &tokens[event_definition_start_token_index..],
                            )?;

                        let event = process_event_definition_tokens(
                            event_definition_tokens,
                            &registered_attributes,
                        )?;
                        events.push(event);

                        current_token_index = event_definition_start_token_index
                            + tokens_consumed_by_event_definition;
                    }
                }
            }
            unexpected => {
                return Err(MeteringMacrosError::ParseError(format!(
                    "Unexpected token at top level: {:?}",
                    unexpected
                )));
            }
        }
    }

    Ok((attributes, events))
}

/// Generates a compiler error at the macro call site given a string.
pub fn generate_compile_error(error_message_string: &str) -> proc_macro::TokenStream {
    let mut compile_error_token_stream = TokenStream::new();
    compile_error_token_stream.extend([TokenTree::Ident(Ident::new(
        "compile_error",
        Span::call_site(),
    ))]);

    compile_error_token_stream.extend([TokenTree::Punct(Punct::new('!', Spacing::Alone))]);

    let error_message_literal = Literal::string(error_message_string);
    let mut compile_error_arguments_token_stream = TokenStream::new();
    compile_error_arguments_token_stream.extend([TokenTree::Literal(error_message_literal)]);
    let compile_error_arguments_group =
        Group::new(Delimiter::Parenthesis, compile_error_arguments_token_stream);
    compile_error_token_stream.extend([TokenTree::Group(compile_error_arguments_group)]);

    compile_error_token_stream.extend([TokenTree::Punct(Punct::new(';', Spacing::Alone))]);

    return proc_macro::TokenStream::from(compile_error_token_stream);
}
