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
    contexts::{collect_context_definition_tokens, process_context_definition_tokens, Context},
    errors::MeteringMacrosError,
};

/// Processes the full token stream that is passed into [`crate::initialize_metering`] by
/// attempting to parse out vectors of attributes and contexts.
pub fn process_token_stream(
    token_stream: &TokenStream,
) -> Result<(Vec<Attribute>, Vec<Context>), MeteringMacrosError> {
    let tokens: Vec<TokenTree> = token_stream.clone().into_iter().collect();

    let mut attributes: Vec<Attribute> = Vec::new();
    let mut contexts: Vec<Context> = Vec::new();
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
                        "Found `#[field(...)]` outside of an context".into(),
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
                    PrimaryAnnotation::Context => {
                        let context_definition_start_token_index =
                            current_token_index + tokens_consumed_by_annotation;

                        let (context_definition_tokens, tokens_consumed_by_context_definition) =
                            collect_context_definition_tokens(
                                &tokens[context_definition_start_token_index..],
                            )?;

                        let context = process_context_definition_tokens(
                            context_definition_tokens,
                            &registered_attributes,
                        )?;
                        contexts.push(context);

                        current_token_index = context_definition_start_token_index
                            + tokens_consumed_by_context_definition;
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

    Ok((attributes, contexts))
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

    proc_macro::TokenStream::from(compile_error_token_stream)
}
