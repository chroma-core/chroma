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

pub fn process_token_stream(
    token_stream: &TokenStream,
) -> Result<(Vec<Attribute>, Vec<Event>), MeteringMacrosError> {
    let tokens: Vec<TokenTree> = token_stream.clone().into_iter().collect();

    let mut attributes: Vec<Attribute> = Vec::new();
    let mut events: Vec<Event> = Vec::new();
    let mut registered_attributes: HashMap<String, Attribute> = HashMap::new();

    let mut i = 0;
    while i < tokens.len() {
        match &tokens[i] {
            TokenTree::Punct(punct)
                if punct.as_char() == '#' && punct.spacing() == Spacing::Alone =>
            {
                let (ann_type, ann_tokens, ann_consumed) = collect_annotation_tokens(&tokens[i..])?;

                if let AnnotationLevel::Secondary = ann_type {
                    return Err(MeteringMacrosError::ParseError(
                        "Found `#[field(...)]` outside of an event".into(),
                    ));
                }

                let primary = process_primary_annotation_tokens(&ann_tokens)?;
                match primary {
                    PrimaryAnnotation::Attribute {
                        attribute_name_string,
                    } => {
                        let start = i + ann_consumed;

                        let (def_tokens, def_consumed) =
                            collect_attribute_definition_tokens(&tokens[start..])?;

                        let attribute = process_attribute_definition_tokens(
                            def_tokens,
                            attribute_name_string.clone(),
                        )?;

                        registered_attributes
                            .insert(attribute_name_string.clone(), attribute.clone());
                        attributes.push(attribute);

                        i = start + def_consumed;
                    }
                    PrimaryAnnotation::Event => {
                        let start = i + ann_consumed;

                        let (def_tokens, def_consumed) =
                            collect_event_definition_tokens(&tokens[start..])?;

                        let event =
                            process_event_definition_tokens(def_tokens, &registered_attributes)?;
                        events.push(event);

                        i = start + def_consumed;
                    }
                }
            }
            other => {
                return Err(MeteringMacrosError::ParseError(format!(
                    "Unexpected token at top level: {:?}",
                    other
                )));
            }
        }
    }

    Ok((attributes, events))
}

pub fn generate_compile_error(error_message: &str) -> proc_macro::TokenStream {
    let mut token_stream = TokenStream::new();
    token_stream.extend([TokenTree::Ident(Ident::new(
        "compile_error",
        Span::call_site(),
    ))]);

    token_stream.extend([TokenTree::Punct(Punct::new('!', Spacing::Alone))]);

    let literal = Literal::string(error_message);
    let mut inner = TokenStream::new();
    inner.extend([TokenTree::Literal(literal)]);
    let group = Group::new(Delimiter::Parenthesis, inner);
    token_stream.extend([TokenTree::Group(group)]);

    token_stream.extend([TokenTree::Punct(Punct::new(';', Spacing::Alone))]);

    return proc_macro::TokenStream::from(token_stream);
}
