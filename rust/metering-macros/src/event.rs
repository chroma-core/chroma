use std::{collections::HashMap, iter::Peekable};

use proc_macro2::{Delimiter, Ident, Literal, Spacing, TokenStream, TokenTree};

use crate::errors::MeteringMacrosError;

#[derive(Debug, Eq, PartialEq)]
pub enum FieldMutability {
    Constant,
    Mutable,
}

#[derive(Debug)]
pub struct Field {
    pub field_mutability: FieldMutability,
    pub field_name: Ident,
    pub field_attribute_name: Option<Literal>,
    pub field_type: TokenStream,
}

pub struct EventBodyResult {
    pub event_name: Ident,
    pub fields: Vec<Field>,
    pub field_to_mutator: HashMap<Ident, Literal>,
}

pub fn process_event_body(tokens: &TokenStream) -> Result<EventBodyResult, MeteringMacrosError> {
    // 1) Collect top‐level tokens into a Vec:
    let top: Vec<TokenTree> = tokens.clone().into_iter().collect();

    // 2) Skip any leading outer‐attributes (e.g. `#[derive(Debug)]` or `#[cfg(...)]`, etc.),
    //    which appear as `Punct('#')` followed by `Group(Delimiter::Bracket)`. Repeat until none remain.
    let mut idx = 0;
    while idx + 1 < top.len() {
        match (&top[idx], &top[idx + 1]) {
            (TokenTree::Punct(p), TokenTree::Group(g))
                if p.as_char() == '#' && g.delimiter() == Delimiter::Bracket =>
            {
                idx += 2;
                continue;
            }
            _ => break,
        }
    }

    // 3) Now at position `idx` we expect `Ident("struct")`, then `Ident(event_name)`, then `Group` with `{ … }`.
    if idx + 2 >= top.len() {
        return Err(MeteringMacrosError::EventBodyError);
    }
    let first = &top[idx];
    let second = &top[idx + 1];
    let third = &top[idx + 2];
    let (event_name_ident, body_group) = match (first, second, third) {
        (TokenTree::Ident(si), TokenTree::Ident(en), TokenTree::Group(gr))
            if si.to_string() == "struct" && gr.delimiter() == Delimiter::Brace =>
        {
            (en.clone(), gr.clone())
        }
        _ => return Err(MeteringMacrosError::EventBodyError),
    };

    // 4) Everything else after that (fields, etc.) is inside `body_group.stream()`.
    //    We'll iterate through the brace‐group to pull out each field.
    let mut inner_iter: Peekable<proc_macro2::token_stream::IntoIter> =
        body_group.stream().into_iter().peekable();

    let mut fields = Vec::new();
    let mut field_to_mutator: HashMap<Ident, proc_macro2::Literal> = HashMap::new();

    while let Some(tt) = inner_iter.next() {
        // a) Skip stray commas between fields
        if let TokenTree::Punct(ref p) = tt {
            if p.as_char() == ',' {
                continue;
            }
        }

        // 2a) Does this token start a “#[field(...)]” annotation?
        if let TokenTree::Punct(ref p) = tt {
            if p.as_char() == '#' {
                // Next must be a bracketed Group: #[ … ]
                let attr_group = match inner_iter.next() {
                    Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Bracket => g,
                    _ => return Err(MeteringMacrosError::EventBodyError),
                };

                // Parse the contents of `#[ … ]` now:
                // We expect exactly: Ident("field"), Group(Paren) inside it.
                let mut attr_iter = attr_group.stream().into_iter().peekable();
                //  - consume Ident("field")
                match attr_iter.next() {
                    Some(TokenTree::Ident(id)) if id == "field" => {}
                    _ => return Err(MeteringMacrosError::EventBodyError),
                }

                //  - consume the inner `( … )`
                let paren_group = match attr_iter.next() {
                    Some(TokenTree::Group(g)) if g.delimiter() == Delimiter::Parenthesis => g,
                    _ => return Err(MeteringMacrosError::EventBodyError),
                };

                // Now parse inside `field(attribute = "...", mutator = "...")`
                let (attr_name_lit, mutator_name_lit) =
                    parse_single_field_attribute(&paren_group.stream())?;

                // 2b) After the attribute, the next token must be the actual field name (Ident)
                let field_ident = match inner_iter.next() {
                    Some(TokenTree::Ident(id)) => id,
                    _ => return Err(MeteringMacrosError::EventBodyError),
                };

                // Next must be `:`
                match inner_iter.next() {
                    Some(TokenTree::Punct(colon))
                        if colon.as_char() == ':' && colon.spacing() == Spacing::Alone => {}
                    _ => return Err(MeteringMacrosError::EventBodyError),
                }

                // 2c) Collect the type tokens until we see a trailing comma (or `}`).
                let mut field_type_tokens = TokenStream::new();
                while let Some(peek_tt) = inner_iter.peek() {
                    match peek_tt {
                        TokenTree::Punct(p)
                            if p.as_char() == ',' && p.spacing() == Spacing::Alone =>
                        {
                            // consume the comma and break
                            inner_iter.next();
                            break;
                        }
                        other => {
                            field_type_tokens.extend([other.clone()]);
                            inner_iter.next();
                        }
                    }
                }

                // Add to annotated_fields
                fields.push(Field {
                    field_mutability: FieldMutability::Mutable,
                    field_name: field_ident.clone(),
                    field_attribute_name: Some(attr_name_lit),
                    field_type: field_type_tokens.clone(),
                });

                // Map the field name → mutator literal
                field_to_mutator.insert(field_ident, mutator_name_lit.clone());
                continue;
            }
        }

        // 2d) Otherwise, this is a “constant” field of the form `Ident : Type ,`
        let field_ident = if let TokenTree::Ident(id) = tt {
            id
        } else {
            return Err(MeteringMacrosError::EventBodyError);
        };

        // Next token must be `:`
        match inner_iter.next() {
            Some(TokenTree::Punct(colon))
                if colon.as_char() == ':' && colon.spacing() == Spacing::Alone => {}
            _ => return Err(MeteringMacrosError::EventBodyError),
        }

        // Now read the type until we hit a comma
        let mut field_type_tokens = TokenStream::new();
        while let Some(peek_tt) = inner_iter.peek() {
            match peek_tt {
                TokenTree::Punct(p) if p.as_char() == ',' && p.spacing() == Spacing::Alone => {
                    inner_iter.next();
                    break;
                }
                other => {
                    field_type_tokens.extend([other.clone()]);
                    inner_iter.next();
                }
            }
        }

        fields.push(Field {
            field_mutability: FieldMutability::Constant,
            field_name: field_ident.clone(),
            field_attribute_name: None,
            field_type: field_type_tokens.clone(),
        });
    }

    Ok(EventBodyResult {
        event_name: event_name_ident.clone(),
        fields,
        field_to_mutator,
    })
}

/// Helper to parse exactly `attribute = "<...>", mutator = "<...>"` inside `#[field(...)]`.
fn parse_single_field_attribute(
    ts: &TokenStream,
) -> Result<(Literal, Literal), MeteringMacrosError> {
    // We expect tokens like:
    //   Ident("attribute"), Punct('='), Literal("\"my_attr\""), Punct(','),
    //   Ident("mutator"), Punct('='), Literal("\"my_mutator\"")
    let mut iter = ts.clone().into_iter().peekable();

    // 1) Check `attribute`
    match iter.next() {
        Some(TokenTree::Ident(id)) if id == "attribute" => {}
        _ => return Err(MeteringMacrosError::EventBodyError),
    }
    // 2) Check `=`
    match iter.next() {
        Some(TokenTree::Punct(p)) if p.as_char() == '=' && p.spacing() == Spacing::Alone => {}
        _ => return Err(MeteringMacrosError::EventBodyError),
    }
    // 3) Get the attribute literal (e.g. "\"my_attribute\"")
    let attribute_lit = match iter.next() {
        Some(TokenTree::Literal(lit)) => lit,
        _ => return Err(MeteringMacrosError::EventBodyError),
    };
    // 4) Check comma
    match iter.next() {
        Some(TokenTree::Punct(p)) if p.as_char() == ',' && p.spacing() == Spacing::Alone => {}
        _ => return Err(MeteringMacrosError::EventBodyError),
    }
    // 5) Check `mutator`
    match iter.next() {
        Some(TokenTree::Ident(id)) if id == "mutator" => {}
        _ => return Err(MeteringMacrosError::EventBodyError),
    }
    // 6) Check `=`
    match iter.next() {
        Some(TokenTree::Punct(p)) if p.as_char() == '=' && p.spacing() == Spacing::Alone => {}
        _ => return Err(MeteringMacrosError::EventBodyError),
    }
    // 7) Get the mutator literal
    let mutator_lit = match iter.next() {
        Some(TokenTree::Literal(lit)) => lit,
        _ => return Err(MeteringMacrosError::EventBodyError),
    };

    // There should be nothing else
    if iter.next().is_some() {
        return Err(MeteringMacrosError::EventBodyError);
    }

    Ok((attribute_lit, mutator_lit))
}
