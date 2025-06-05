use std::{collections::HashMap, iter::Peekable};

use proc_macro2::{Delimiter, Ident, Spacing, TokenStream, TokenTree};
use quote::quote;

use crate::{
    annotations::{process_secondary_annotation_tokens, SecondaryAnnotation},
    attributes::Attribute,
    errors::MeteringMacrosError,
    fields::{generate_field_definition_token_stream, process_field_definition_tokens, Field},
};

pub struct Event {
    pub foreign_macro_token_streams: Vec<TokenStream>,
    pub visibility_modifier_ident: Option<Ident>,
    pub event_name_ident: Ident,
    pub fields: Vec<Field>,
}

pub fn collect_event_definition_tokens(
    tokens: &[TokenTree],
) -> Result<(Vec<TokenTree>, usize), MeteringMacrosError> {
    let mut def_tokens: Vec<TokenTree> = Vec::new();
    let mut seen_struct = false;

    for (j, tt) in tokens.iter().enumerate() {
        def_tokens.push(tt.clone());

        if !seen_struct {
            if let TokenTree::Ident(ident) = tt {
                if ident == "struct" {
                    seen_struct = true;
                }
            }
        } else {
            if let TokenTree::Group(g) = tt {
                if g.delimiter() == Delimiter::Brace {
                    return Ok((def_tokens, j + 1));
                }
            }
        }
    }

    Err(MeteringMacrosError::ParseError(
        "Unterminated event definition: never found `{…}` after `struct`".into(),
    ))
}

pub fn process_event_definition_tokens(
    def_tokens: Vec<TokenTree>,
    registered_attributes: &HashMap<String, Attribute>,
) -> Result<Event, MeteringMacrosError> {
    let mut iter: Peekable<_> = def_tokens.into_iter().peekable();
    let mut foreign_macro_token_streams: Vec<TokenStream> = Vec::new();
    let mut visibility_modifier_ident: Option<Ident> = None;

    loop {
        if let Some(TokenTree::Punct(punct)) = iter.peek() {
            if punct.as_char() == '#' && punct.spacing() == Spacing::Alone {
                let hash_tt = iter.next().unwrap();

                match iter.next() {
                    Some(TokenTree::Group(group)) if group.delimiter() == Delimiter::Bracket => {
                        let mut ts = TokenStream::new();
                        ts.extend(std::iter::once(hash_tt.clone()));
                        ts.extend(std::iter::once(TokenTree::Group(group.clone())));
                        foreign_macro_token_streams.push(ts);
                        continue;
                    }
                    Some(other) => {
                        return Err(MeteringMacrosError::ParseError(format!(
                            "Expected a bracket group after `#` (for a foreign macro), found: {:?}",
                            other
                        )));
                    }
                    None => {
                        return Err(MeteringMacrosError::ParseError(
                            "Unexpected end of tokens after `#` while collecting foreign macro"
                                .into(),
                        ));
                    }
                }
            }
        }
        break;
    }

    if let Some(TokenTree::Ident(ident)) = iter.peek() {
        if ident == "pub" {
            if let TokenTree::Ident(i) = iter.next().unwrap() {
                visibility_modifier_ident = Some(Ident::new(&i.to_string(), i.span()));
            }

            if let Some(TokenTree::Group(group)) = iter.peek() {
                if group.delimiter() == Delimiter::Parenthesis {
                    let _ = iter.next().unwrap();
                }
            }
        }
    }

    match iter.next() {
        Some(TokenTree::Ident(ident)) if ident == "struct" => {}
        other => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected `struct` in event definition, found: {:?}",
                other
            )));
        }
    }

    let event_name_ident = match iter.next() {
        Some(TokenTree::Ident(ident)) => Ident::new(&ident.to_string(), ident.span()),
        other => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected event (struct) name, found: {:?}",
                other
            )));
        }
    };

    let fields_group = match iter.next() {
        Some(TokenTree::Group(group)) if group.delimiter() == Delimiter::Brace => group,
        other => {
            return Err(MeteringMacrosError::ParseError(format!(
                "Expected a brace‐delimited field list after event name, found: {:?}",
                other
            )));
        }
    };

    let inner_tokens: Vec<TokenTree> = fields_group.stream().into_iter().collect();
    let mut inner_iter: Peekable<_> = inner_tokens.into_iter().peekable();
    let mut fields = Vec::new();

    while inner_iter.peek().is_some() {
        if let Some(TokenTree::Punct(p)) = inner_iter.peek() {
            if p.as_char() == ',' {
                inner_iter.next();
                continue;
            }
        }

        if let Some(TokenTree::Punct(p)) = inner_iter.peek() {
            if p.as_char() == '#' {
                let _hash = inner_iter.next().unwrap();

                let ann_group = inner_iter.next().unwrap();
                let annotation_tokens = vec![_hash.clone(), ann_group.clone()];

                let secondary = process_secondary_annotation_tokens(&annotation_tokens)?;

                let mut field_def_tokens = Vec::new();
                while let Some(next_tt) = inner_iter.peek() {
                    if let TokenTree::Punct(p2) = next_tt {
                        if p2.as_char() == ',' {
                            break;
                        }
                    }
                    field_def_tokens.push(inner_iter.next().unwrap());
                }

                let mut field = process_field_definition_tokens(field_def_tokens)?;

                if let SecondaryAnnotation::Field {
                    attribute_name_string,
                    custom_mutator_name_ident,
                } = secondary
                {
                    if !registered_attributes.contains_key(&attribute_name_string) {
                        return Err(MeteringMacrosError::ParseError(format!(
                            "Field references unknown attribute `{}`",
                            attribute_name_string
                        )));
                    }

                    let attribute = registered_attributes
                        .get(&attribute_name_string)
                        .unwrap()
                        .clone();
                    field.attribute = Some(attribute);
                    field.custom_mutator_name_ident = Some(custom_mutator_name_ident);
                    fields.push(field);
                }
            } else {
                let mut field_def_tokens = Vec::new();
                while let Some(next_tt) = inner_iter.peek() {
                    if let TokenTree::Punct(p2) = next_tt {
                        if p2.as_char() == ',' {
                            break;
                        }
                    }
                    field_def_tokens.push(inner_iter.next().unwrap());
                }
                let field = process_field_definition_tokens(field_def_tokens)?;
                fields.push(field);
            }
        } else {
            let mut field_def_tokens = Vec::new();
            while let Some(next_tt) = inner_iter.peek() {
                if let TokenTree::Punct(p2) = next_tt {
                    if p2.as_char() == ',' {
                        break;
                    }
                }
                field_def_tokens.push(inner_iter.next().unwrap());
            }
            let field = process_field_definition_tokens(field_def_tokens)?;
            fields.push(field);
        }
    }

    Ok(Event {
        foreign_macro_token_streams,
        visibility_modifier_ident,
        event_name_ident,
        fields,
    })
}

pub fn generate_event_definition_token_stream(event: &Event) -> TokenStream {
    let Event {
        foreign_macro_token_streams: foreign_macro_token_streams,
        visibility_modifier_ident: visibility_modifier_ident,
        event_name_ident: event_name_ident,
        fields: fields,
    } = event;

    let field_definition_token_streams: Vec<TokenStream> = fields
        .iter()
        .map(|field| generate_field_definition_token_stream(field))
        .collect();

    let event_definition_token_stream = if visibility_modifier_ident.is_some() {
        quote! {
            #( #foreign_macro_token_streams )*
            #visibility_modifier_ident struct #event_name_ident {
                #( #field_definition_token_streams )*
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
        .filter(|field| field.custom_mutator_name_ident.is_some())
        .map(|field| field.custom_mutator_name_ident.as_ref().unwrap())
        .collect();

    return quote! {
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
    };
}
