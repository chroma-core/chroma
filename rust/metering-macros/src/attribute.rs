//! This file contains logic for parsing the arguments and tokens
//! provided in an invocation of the [`crate::attribute`] procedural
//! macro.

use heck::ToUpperCamelCase;
use proc_macro2::{Ident, Spacing, TokenStream, TokenTree};

use crate::{errors::MeteringMacrosError, utils};

/// The result of parsing arguments passed to an invocation
/// of the [`crate::attribute`] procedural macro.
pub struct AttributeArgsParseResult {
    /// The name of the attribute as a `String`.
    pub attribute_name_string: String,
}

/// Parses and validates the arguments provided to the [`crate::attribute`] macro.
///
/// # Overview
/// This function expects the input token stream to match the pattern:
/// `name = "<attribute_name>"`. If valid, it returns an [`AttributeArgsParseResult`]
/// containing the parsed attribute name in both string and literal form.  
/// If invalid, it returns a [`MeteringMacrosError`] indicating an argument error.
///
/// # Arguments
/// * `token_stream` - A reference to the token stream passed to the macro invocation.
///
/// # Returns
/// * `Ok(AttributeArgsParseResult)` - When the arguments match the expected pattern.
/// * `Err(MeteringMacrosError)` - If the input is malformed or does not match expectations.
pub fn parse_attribute_args(
    token_stream: &TokenStream,
) -> Result<AttributeArgsParseResult, MeteringMacrosError> {
    // Collect the tokens as a vector
    let tokens: Vec<TokenTree> = token_stream.clone().into_iter().collect();

    // Match against the expected pattern: Ident(name), Punct(=), Literal("<attribute_name>")
    match tokens.as_slice() {
        [TokenTree::Ident(expected_name_ident), TokenTree::Punct(expected_equals_punct), TokenTree::Literal(expected_attribute_name_literal)] =>
        {
            if (expected_name_ident != "name")
                || (expected_equals_punct.as_char() != '='
                    || expected_equals_punct.spacing() != Spacing::Alone)
            {
                return Err(MeteringMacrosError::AttributeArgsError);
            }

            let attribute_name_string = utils::literal_to_string(expected_attribute_name_literal);

            Ok(AttributeArgsParseResult {
                attribute_name_string,
            })
        }
        _ => Err(MeteringMacrosError::AttributeArgsError),
    }
}

/// The result of parsing the tokens on which the [`crate::attribute`] macro was invoked.
///
/// This struct contains both the alias (`proc_macro2::Ident`) used for the attribute type and
/// the actual type representation as a `proc_macro2::TokenStream`.
pub struct AttributeBodyParseResult {
    /// The alias of the type assigned to the attribute (e.g., `MyType` in `type MyType = u8;`).
    pub attribute_type_alias_ident: Ident,

    /// The full token stream representing the type assigned to the attribute.
    pub attribute_type_token_stream: TokenStream,
}

/// Parses and validates the tokens on which the [`crate::attribute`] macro is invoked.
///
/// # Overview
/// This function expects the input token stream to represent a Rust type alias
/// (e.g., `type MyAlias = SomeType;`). If the tokens are valid and match the expected
/// structure, it extracts the alias and type and returns an [`AttributeBodyParseResult`].
/// If parsing fails or the input is malformed, it returns a [`MeteringMacrosError`].
///
/// # Arguments
/// * `token_stream` - The token stream representing the macro's target.
/// * `attribute_name` - The name of the attribute being parsed, used to validate the input.
///
/// # Returns
/// * `Ok(AttributeBodyParseResult)` - When parsing succeeds and the structure is valid.
/// * `Err(MeteringMacrosError)` - When parsing fails or validation is unsuccessful.
pub fn parse_attribute_body(
    token_stream: &TokenStream,
    attribute_name: &str,
) -> Result<AttributeBodyParseResult, MeteringMacrosError> {
    // Convert the tokens into an iterator
    let mut tokens_iter = token_stream.clone().into_iter().peekable();

    // We expect to see the `type` keyword as the first token
    let expected_type_keyword_token_tree = tokens_iter
        .next()
        .ok_or(MeteringMacrosError::AttributeBodyError)?;
    // We can throw out the ident once we've validated it.
    match expected_type_keyword_token_tree {
        TokenTree::Ident(ref expected_type_keyword_ident)
            if expected_type_keyword_ident == "type" => {}
        _ => return Err(MeteringMacrosError::AttributeBodyError),
    }

    // Next, we expect the alias type name. It must be the CamelCase equivalent of the
    // attribute name supplied to the macro's arguments.
    let expected_type_alias_token_tree = tokens_iter
        .next()
        .ok_or(MeteringMacrosError::AttributeBodyError)?;
    // We need to return the type alias so we can output it as part of the generated code.
    let attribute_type_alias_ident = match expected_type_alias_token_tree {
        TokenTree::Ident(ref expected_type_alias_ident)
            if expected_type_alias_ident == &attribute_name.to_upper_camel_case() =>
        {
            expected_type_alias_ident.clone()
        }
        _ => return Err(MeteringMacrosError::AttributeBodyError),
    };

    // After the type alias, we expect to see an equals sign.
    let expected_equals_token_tree = tokens_iter
        .next()
        .ok_or(MeteringMacrosError::AttributeBodyError)?;
    // We can throw out this token once we've validated it.
    match expected_equals_token_tree {
        TokenTree::Punct(ref expected_equals_punct)
            if expected_equals_punct.as_char() == '='
                && expected_equals_punct.spacing() == Spacing::Alone => {}
        _ => return Err(MeteringMacrosError::AttributeBodyError),
    }

    // Now, we iterate over the remaining tokens, which we expect to be a valid type definition.
    let mut attribute_type_tokens: Vec<TokenTree> = Vec::new();
    while let Some(token_tree) = tokens_iter.peek() {
        match token_tree {
            // We stop when we encounter a semicolon.
            TokenTree::Punct(p) if p.as_char() == ';' && p.spacing() == Spacing::Alone => {
                break;
            }
            // We consume all other tokens as part of the type definition.
            _ => {
                let next_token_tree = tokens_iter.next().unwrap();
                attribute_type_tokens.push(next_token_tree);
            }
        }
    }

    // Finally, we expect to see a semicolon.
    let expected_semicolon_token_tree = tokens_iter
        .next()
        .ok_or(MeteringMacrosError::AttributeBodyError)?;
    match expected_semicolon_token_tree {
        TokenTree::Punct(ref expected_semicolon_punct)
            if expected_semicolon_punct.as_char() == ';'
                && expected_semicolon_punct.spacing() == Spacing::Alone => {}
        _ => return Err(MeteringMacrosError::AttributeBodyError),
    }

    // We ensure that there are no remaining tokens after the semicolon.
    if tokens_iter.next().is_some() {
        return Err(MeteringMacrosError::AttributeBodyError);
    }

    // We reconstruct a token stream from the collected type definition tokens.
    let attribute_type_token_stream = TokenStream::from_iter(attribute_type_tokens.into_iter());

    Ok(AttributeBodyParseResult {
        attribute_type_alias_ident,
        attribute_type_token_stream,
    })
}
