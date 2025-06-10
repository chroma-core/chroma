use proc_macro2::{Delimiter, Ident, Spacing, Span, TokenTree};

use crate::errors::MeteringMacrosError;

/// The syntactic depth level of an annotation.
pub enum AnnotationLevel {
    Primary,
    Secondary,
}

/// An annotation at the outermost syntactic layer. Must be either an context or attribute.
pub enum PrimaryAnnotation {
    Context,
    Attribute { attribute_name_string: String },
}

/// An annotation at the second layer of syntactic depth. Must be a field annotation.
pub enum SecondaryAnnotation {
    Field {
        attribute_name_string: String,
        custom_mutator_name_ident: Ident,
    },
}

/// Accepts the tokens from [`crate::utils::process_token_stream`]'s current index to the end of the input tokens
/// and attempts to find the slice in which there is a valid annotation, given that the current index contains a `#`.
pub fn collect_annotation_tokens(
    tokens: &[TokenTree],
) -> Result<(AnnotationLevel, Vec<TokenTree>, usize), MeteringMacrosError> {
    if tokens.len() < 2 {
        return Err(MeteringMacrosError::ParseError(
            "Expected annotation, found end of tokens".into(),
        ));
    }

    match &tokens[0] {
        TokenTree::Punct(expected_hashtag_punct)
            if expected_hashtag_punct.as_char() == '#'
                && expected_hashtag_punct.spacing() == Spacing::Alone => {}
        unexpected => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected `#` for annotation, found: {:?}",
                unexpected
            )));
        }
    }

    let annotation_group = match &tokens[1] {
        TokenTree::Group(expected_annotation_group)
            if expected_annotation_group.delimiter() == Delimiter::Bracket =>
        {
            expected_annotation_group
        }
        unexpected => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected bracket group after `#`, found: {:?}",
                unexpected
            )));
        }
    };

    let mut annotation_tokens_iter = annotation_group.stream().into_iter();
    let expected_annotation_name = annotation_tokens_iter
        .next()
        .ok_or_else(|| MeteringMacrosError::ParseError("Empty annotation".into()))?;

    match expected_annotation_name {
        TokenTree::Ident(expected_primary_annotation_ident)
            if expected_primary_annotation_ident == "attribute"
                || expected_primary_annotation_ident == "context" =>
        {
            Ok((AnnotationLevel::Primary, tokens[0..2].to_vec(), 2))
        }
        TokenTree::Ident(expected_secondary_annotation_ident)
            if expected_secondary_annotation_ident == "field" =>
        {
            Ok((AnnotationLevel::Secondary, tokens[0..2].to_vec(), 2))
        }
        unexpected => Err(MeteringMacrosError::ParseError(format!(
            "Unrecognized annotation identifier: {:?}",
            unexpected
        ))),
    }
}

/// Processes a slice of tokens that is known to contain a (possibly invalid) primary annotation.
pub fn process_primary_annotation_tokens(
    annotation_tokens: &[TokenTree],
) -> Result<PrimaryAnnotation, MeteringMacrosError> {
    let annotation_group = match &annotation_tokens[1] {
        TokenTree::Group(expected_annotation_group) => expected_annotation_group,
        _ => unreachable!(
            "Only tokens that are known to be a group should be passed to this function."
        ),
    };

    let mut annotation_tokens_iter = annotation_group.stream().into_iter().peekable();

    match annotation_tokens_iter.next() {
        Some(TokenTree::Ident(expected_attribute_ident))
            if expected_attribute_ident == "attribute" =>
        {
            let attribute_annotation_args_group = match annotation_tokens_iter.next() {
                Some(TokenTree::Group(expected_attribute_annotation_args_group))
                    if expected_attribute_annotation_args_group.delimiter()
                        == Delimiter::Parenthesis =>
                {
                    expected_attribute_annotation_args_group
                }
                _ => {
                    return Err(MeteringMacrosError::ParseError(
                        "Expected `(name = \"...\")` after `attribute`".into(),
                    ));
                }
            };

            let mut attribute_annotation_tokens_iter = attribute_annotation_args_group
                .stream()
                .into_iter()
                .peekable();

            match attribute_annotation_tokens_iter.next() {
                Some(TokenTree::Ident(expected_name_ident)) if expected_name_ident == "name" => {}
                _ => {
                    return Err(MeteringMacrosError::ParseError(
                        "Expected `name = \"...\"` in attribute annotation".into(),
                    ));
                }
            }

            match attribute_annotation_tokens_iter.next() {
                Some(TokenTree::Punct(expected_equals_punct))
                    if expected_equals_punct.as_char() == '=' => {}
                _ => {
                    return Err(MeteringMacrosError::ParseError(
                        "Expected `=` after `name`".into(),
                    ));
                }
            }

            let attribute_name_literal = match attribute_annotation_tokens_iter.next() {
                Some(TokenTree::Literal(expected_attribute_name_literal)) => {
                    expected_attribute_name_literal
                }
                _ => {
                    return Err(MeteringMacrosError::ParseError(
                        "Expected string literal after `name =`".into(),
                    ));
                }
            };
            let mut attribute_name_string = attribute_name_literal.to_string();
            if !(attribute_name_string.starts_with('"') && attribute_name_string.ends_with('"')) {
                return Err(MeteringMacrosError::ParseError(
                    "Attribute `name` must be a string literal".into(),
                ));
            }
            attribute_name_string =
                attribute_name_string[1..attribute_name_string.len() - 1].to_string();

            if attribute_annotation_tokens_iter.next().is_some() {
                return Err(MeteringMacrosError::ParseError(
                    "Unexpected extra tokens in `attribute(...)`".into(),
                ));
            }

            if attribute_annotation_tokens_iter.next().is_some() {
                return Err(MeteringMacrosError::ParseError(
                    "Unexpected tokens after `attribute(...)`".into(),
                ));
            }

            Ok(PrimaryAnnotation::Attribute {
                attribute_name_string,
            })
        }

        Some(TokenTree::Ident(expected_context_ident)) if expected_context_ident == "context" => {
            if annotation_tokens_iter.next().is_some() {
                return Err(MeteringMacrosError::ParseError(
                    "`context` annotation takes no arguments".into(),
                ));
            }
            Ok(PrimaryAnnotation::Context)
        }

        unexpected => Err(MeteringMacrosError::ParseError(format!(
            "Unrecognized primary annotation: {:?}",
            unexpected
        ))),
    }
}

/// Processes a slice of tokens that is known to contain a (possibly invalid) secondary annotation.
pub fn process_secondary_annotation_tokens(
    annotation_tokens: &[TokenTree],
) -> Result<SecondaryAnnotation, MeteringMacrosError> {
    let annotation_group = match &annotation_tokens[1] {
        TokenTree::Group(annotation_group) => annotation_group,
        _ => unreachable!("We already verified this is a Group"),
    };
    let mut annotation_tokens_iter = annotation_group.stream().into_iter().peekable();

    match annotation_tokens_iter.next() {
        Some(TokenTree::Ident(expected_field_ident)) if expected_field_ident == "field" => {}
        unexpected => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected `field` in secondary annotation, found: {:?}",
                unexpected
            )));
        }
    }

    let annotation_args_group = match annotation_tokens_iter.next() {
        Some(TokenTree::Group(expected_parentheses_group))
            if expected_parentheses_group.delimiter() == Delimiter::Parenthesis =>
        {
            expected_parentheses_group
        }
        _ => {
            return Err(MeteringMacrosError::ParseError(
                "Expected `( ... )` after `field`".into(),
            ));
        }
    };

    let mut field_annotation_tokens_iter = annotation_args_group.stream().into_iter().peekable();
    let mut maybe_attribute_name_string: Option<String> = None;
    let mut maybe_custom_mutator_name: Option<String> = None;

    while let Some(next_token) = field_annotation_tokens_iter.next() {
        match next_token {
            TokenTree::Ident(expected_attribute_ident)
                if expected_attribute_ident == "attribute" =>
            {
                match field_annotation_tokens_iter.next() {
                    Some(TokenTree::Punct(expected_equals_punct))
                        if expected_equals_punct.as_char() == '=' => {}
                    _ => {
                        return Err(MeteringMacrosError::ParseError(
                            "Expected `=` after `attribute`".into(),
                        ));
                    }
                }

                let attribute_name_literal = match field_annotation_tokens_iter.next() {
                    Some(TokenTree::Literal(expected_attribute_name_literal)) => {
                        expected_attribute_name_literal
                    }
                    _ => {
                        return Err(MeteringMacrosError::ParseError(
                            "Expected string literal after `attribute =`".into(),
                        ));
                    }
                };
                let mut attribute_name_string = attribute_name_literal.to_string();
                if !(attribute_name_string.starts_with('"') && attribute_name_string.ends_with('"'))
                {
                    return Err(MeteringMacrosError::ParseError(
                        "`attribute` value must be a string literal".into(),
                    ));
                }
                attribute_name_string =
                    attribute_name_string[1..attribute_name_string.len() - 1].to_string();
                maybe_attribute_name_string = Some(attribute_name_string);
            }

            TokenTree::Ident(expected_mutator_ident) if expected_mutator_ident == "mutator" => {
                match field_annotation_tokens_iter.next() {
                    Some(TokenTree::Punct(expected_equals_punct))
                        if expected_equals_punct.as_char() == '=' => {}
                    _ => {
                        return Err(MeteringMacrosError::ParseError(
                            "Expected `=` after `mutator`".into(),
                        ));
                    }
                }

                let custom_mutator_name_literal = match field_annotation_tokens_iter.next() {
                    Some(TokenTree::Literal(expected_custom_mutator_name_literal)) => {
                        expected_custom_mutator_name_literal
                    }
                    _ => {
                        return Err(MeteringMacrosError::ParseError(
                            "Expected string literal after `mutator =`".into(),
                        ));
                    }
                };
                let mut custom_mutator_name_string = custom_mutator_name_literal.to_string();
                if !(custom_mutator_name_string.starts_with('"')
                    && custom_mutator_name_string.ends_with('"'))
                {
                    return Err(MeteringMacrosError::ParseError(
                        "`mutator` value must be a string literal".into(),
                    ));
                }
                custom_mutator_name_string =
                    custom_mutator_name_string[1..custom_mutator_name_string.len() - 1].to_string();
                maybe_custom_mutator_name = Some(custom_mutator_name_string);
            }

            TokenTree::Punct(expected_comma_punct) if expected_comma_punct.as_char() == ',' => {
                continue;
            }

            unexpected => {
                return Err(MeteringMacrosError::ParseError(format!(
                    "Unexpected token in field annotation: {:?}",
                    unexpected
                )));
            }
        }
    }

    let attribute_name_string = maybe_attribute_name_string.ok_or_else(|| {
        MeteringMacrosError::ParseError("`attribute` is required in field annotation".into())
    })?;

    let custom_mutator_name_string = maybe_custom_mutator_name.ok_or_else(|| {
        MeteringMacrosError::ParseError("`mutator` is required in field annotation".into())
    })?;

    Ok(SecondaryAnnotation::Field {
        attribute_name_string,
        custom_mutator_name_ident: Ident::new(&custom_mutator_name_string, Span::call_site()),
    })
}
