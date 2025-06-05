use crate::errors::MeteringMacrosError;
use proc_macro2::{Delimiter, Ident, Spacing, TokenTree};

pub enum AnnotationLevel {
    Primary,
    Secondary,
}

pub enum PrimaryAnnotation {
    Event,
    Attribute { attribute_name_string: String },
}

pub enum SecondaryAnnotation {
    Field {
        attribute_name_string: String,
        custom_mutator_name_ident: Ident,
    },
}

pub fn collect_annotation_tokens(
    tokens: &[TokenTree],
) -> Result<(AnnotationLevel, Vec<TokenTree>, usize), MeteringMacrosError> {
    if tokens.len() < 2 {
        return Err(MeteringMacrosError::ParseError(
            "Expected annotation, found end of tokens".into(),
        ));
    }

    match &tokens[0] {
        TokenTree::Punct(p) if p.as_char() == '#' && p.spacing() == Spacing::Alone => {}
        other => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected `#` for annotation, found: {:?}",
                other
            )));
        }
    }

    let group = match &tokens[1] {
        TokenTree::Group(g) if g.delimiter() == Delimiter::Bracket => g,
        other => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected bracket group after `#`, found: {:?}",
                other
            )));
        }
    };

    let mut inner_iter = group.stream().into_iter();
    let first_inner = inner_iter
        .next()
        .ok_or_else(|| MeteringMacrosError::ParseError("Empty annotation".into()))?;

    match first_inner {
        TokenTree::Ident(ident) if ident == "attribute" || ident == "event" => {
            Ok((AnnotationLevel::Primary, tokens[0..2].to_vec(), 2))
        }
        TokenTree::Ident(ident) if ident == "field" => {
            Ok((AnnotationLevel::Secondary, tokens[0..2].to_vec(), 2))
        }
        other => Err(MeteringMacrosError::ParseError(format!(
            "Unrecognized annotation identifier: {:?}",
            other
        ))),
    }
}

pub fn process_primary_annotation_tokens(
    annotation_tokens: &[TokenTree],
) -> Result<PrimaryAnnotation, MeteringMacrosError> {
    let group = match &annotation_tokens[1] {
        TokenTree::Group(g) => g,
        _ => unreachable!("We already verified this is a Group"),
    };

    let mut iter = group.stream().into_iter().peekable();

    match iter.next() {
        Some(TokenTree::Ident(ident)) if ident == "attribute" => {
            let paren_group = match iter.next() {
                Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis => g,
                other => {
                    return Err(MeteringMacrosError::ParseError(
                        "Expected `(name = \"...\")` after `attribute`".into(),
                    ));
                }
            };

            let mut inner_iter = paren_group.stream().into_iter().peekable();

            match inner_iter.next() {
                Some(TokenTree::Ident(name_ident)) if name_ident == "name" => {}
                other => {
                    return Err(MeteringMacrosError::ParseError(
                        "Expected `name = \"...\"` in attribute annotation".into(),
                    ));
                }
            }

            match inner_iter.next() {
                Some(TokenTree::Punct(p)) if p.as_char() == '=' => {}
                other => {
                    return Err(MeteringMacrosError::ParseError(
                        "Expected `=` after `name`".into(),
                    ));
                }
            }

            let lit = match inner_iter.next() {
                Some(TokenTree::Literal(lit)) => lit,
                other => {
                    return Err(MeteringMacrosError::ParseError(
                        "Expected string literal after `name =`".into(),
                    ));
                }
            };
            let lit_str = lit.to_string();
            if !(lit_str.starts_with('"') && lit_str.ends_with('"')) {
                return Err(MeteringMacrosError::ParseError(
                    "Attribute `name` must be a string literal".into(),
                ));
            }
            let name_str = lit_str[1..lit_str.len() - 1].to_string();

            if inner_iter.next().is_some() {
                return Err(MeteringMacrosError::ParseError(
                    "Unexpected extra tokens in `attribute(...)`".into(),
                ));
            }

            if iter.next().is_some() {
                return Err(MeteringMacrosError::ParseError(
                    "Unexpected tokens after `attribute(...)`".into(),
                ));
            }

            Ok(PrimaryAnnotation::Attribute {
                attribute_name_string: name_str,
            })
        }

        Some(TokenTree::Ident(ident)) if ident == "event" => {
            if iter.next().is_some() {
                return Err(MeteringMacrosError::ParseError(
                    "`event` annotation takes no arguments".into(),
                ));
            }
            Ok(PrimaryAnnotation::Event)
        }

        other => Err(MeteringMacrosError::ParseError(format!(
            "Unrecognized primary annotation: {:?}",
            other
        ))),
    }
}

pub fn process_secondary_annotation_tokens(
    annotation_tokens: &[TokenTree],
) -> Result<SecondaryAnnotation, MeteringMacrosError> {
    let group = match &annotation_tokens[1] {
        TokenTree::Group(g) => g,
        _ => unreachable!("We already verified this is a Group"),
    };
    let mut iter = group.stream().into_iter().peekable();

    match iter.next() {
        Some(TokenTree::Ident(ident)) if ident == "field" => {}
        other => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected `field` in secondary annotation, found: {:?}",
                other
            )));
        }
    }

    let paren_group = match iter.next() {
        Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis => g,
        other => {
            return Err(MeteringMacrosError::ParseError(
                "Expected `( … )` after `field`".into(),
            ));
        }
    };

    let mut inner = paren_group.stream().into_iter().peekable();
    let mut attribute_name_opt: Option<String> = None;
    let mut mutator_name_opt: Option<String> = None;

    while let Some(tt) = inner.next() {
        match tt {
            TokenTree::Ident(ident) if ident == "attribute" => {
                match inner.next() {
                    Some(TokenTree::Punct(p)) if p.as_char() == '=' => {}
                    other => {
                        return Err(MeteringMacrosError::ParseError(
                            "Expected `=` after `attribute`".into(),
                        ));
                    }
                }

                let lit = match inner.next() {
                    Some(TokenTree::Literal(lit)) => lit,
                    other => {
                        return Err(MeteringMacrosError::ParseError(
                            "Expected string literal after `attribute =`".into(),
                        ));
                    }
                };
                let lit_str = lit.to_string();
                if !(lit_str.starts_with('"') && lit_str.ends_with('"')) {
                    return Err(MeteringMacrosError::ParseError(
                        "`attribute` value must be a string literal".into(),
                    ));
                }
                let inner_name = lit_str[1..lit_str.len() - 1].to_string();
                attribute_name_opt = Some(inner_name);
            }

            TokenTree::Ident(ident) if ident == "mutator" => {
                match inner.next() {
                    Some(TokenTree::Punct(p)) if p.as_char() == '=' => {}
                    other => {
                        return Err(MeteringMacrosError::ParseError(
                            "Expected `=` after `mutator`".into(),
                        ));
                    }
                }

                let lit = match inner.next() {
                    Some(TokenTree::Literal(lit)) => lit,
                    other => {
                        return Err(MeteringMacrosError::ParseError(
                            "Expected string literal after `mutator =`".into(),
                        ));
                    }
                };
                let lit_str = lit.to_string();
                if !(lit_str.starts_with('"') && lit_str.ends_with('"')) {
                    return Err(MeteringMacrosError::ParseError(
                        "`mutator` value must be a string literal".into(),
                    ));
                }
                let inner_name = lit_str[1..lit_str.len() - 1].to_string();
                mutator_name_opt = Some(inner_name);
            }

            TokenTree::Punct(p) if p.as_char() == ',' => {
                continue;
            }

            other => {
                return Err(MeteringMacrosError::ParseError(format!(
                    "Unexpected token in field annotation: {:?}",
                    other
                )));
            }
        }
    }

    let attribute_name_string = attribute_name_opt.ok_or_else(|| {
        MeteringMacrosError::ParseError("`attribute` is required in field annotation".into())
    })?;

    let custom_mutator_name = mutator_name_opt.ok_or_else(|| {
        MeteringMacrosError::ParseError("`mutator` is required in field annotation".into())
    })?;

    Ok(SecondaryAnnotation::Field {
        attribute_name_string,
        custom_mutator_name_ident: Ident::new(&custom_mutator_name, proc_macro2::Span::call_site()),
    })
}
