use proc_macro2::{Delimiter, Group, Ident, Literal, Punct, Spacing, Span, TokenStream, TokenTree};

use crate::errors::MeteringMacrosError;

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

pub fn literal_to_string(literal: &Literal) -> String {
    return literal.to_string().trim_matches('\"').to_string();
}

pub fn parse_field_annotation(
    token_stream: &TokenStream,
) -> Result<(Literal, Literal), MeteringMacrosError> {
    let mut tokens_iter = token_stream.clone().into_iter().peekable();

    match tokens_iter.next() {
        Some(TokenTree::Ident(id)) if id == "attribute" => {}
        _ => return Err(MeteringMacrosError::EventBodyError),
    }
    match tokens_iter.next() {
        Some(TokenTree::Punct(punct))
            if punct.as_char() == '=' && punct.spacing() == Spacing::Alone => {}
        _ => return Err(MeteringMacrosError::EventBodyError),
    }
    let field_attribute_name_literal = match tokens_iter.next() {
        Some(TokenTree::Literal(literal)) => literal,
        _ => return Err(MeteringMacrosError::EventBodyError),
    };
    match tokens_iter.next() {
        Some(TokenTree::Punct(punct))
            if punct.as_char() == ',' && punct.spacing() == Spacing::Alone => {}
        _ => return Err(MeteringMacrosError::EventBodyError),
    }
    match tokens_iter.next() {
        Some(TokenTree::Ident(id)) if id == "mutator" => {}
        _ => return Err(MeteringMacrosError::EventBodyError),
    }
    match tokens_iter.next() {
        Some(TokenTree::Punct(punct))
            if punct.as_char() == '=' && punct.spacing() == Spacing::Alone => {}
        _ => return Err(MeteringMacrosError::EventBodyError),
    }
    let field_mutator_name_literal = match tokens_iter.next() {
        Some(TokenTree::Literal(literal)) => literal,
        _ => return Err(MeteringMacrosError::EventBodyError),
    };

    if tokens_iter.next().is_some() {
        return Err(MeteringMacrosError::EventBodyError);
    }

    Ok((field_attribute_name_literal, field_mutator_name_literal))
}
