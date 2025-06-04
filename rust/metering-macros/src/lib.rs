extern crate proc_macro;

use attribute::{
    process_attribute_args, process_attribute_body, AttributeArgsResult, AttributeBodyResult,
};
use errors::MeteringMacrosError;
use event::{process_event_body, EventBodyResult, FieldMutability};
use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;
use utils::generate_compile_error;

mod attribute;
mod errors;
mod event;
mod registry;
mod utils;

#[proc_macro_attribute]
pub fn attribute(
    raw_args: proc_macro::TokenStream,
    raw_body: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let args = TokenStream::from(raw_args);
    let body = TokenStream::from(raw_body);

    let AttributeArgsResult {
        attribute_name_literal: _attribute_name_literal,
        attribute_name_string,
    } = match process_attribute_args(&args) {
        Ok(result) => result,
        Err(error) => {
            return proc_macro::TokenStream::from(generate_compile_error(&error.to_string()));
        }
    };

    let AttributeBodyResult {
        attribute_type_name,
        attribute_type,
    } = match process_attribute_body(&body, &attribute_name_string) {
        Ok(result) => result,
        Err(error) => {
            return proc_macro::TokenStream::from(generate_compile_error(&error.to_string()))
        }
    };

    let _attribute_registration_fn_name: Ident = Ident::new(
        &format!("__register_attribute_{}", attribute_type_name),
        Span::call_site(),
    );

    let _attribute_type_tokens_literal = Literal::string(attribute_type.to_string().as_str());

    if let Err(error) = registry::register_attribute(
        &attribute_name_string,
        attribute_type.to_string().trim_matches('\"'),
    ) {
        return proc_macro::TokenStream::from(generate_compile_error(&format!(
            "failed to register attribute: {}",
            error
        )));
    }

    /*
    The following code generates:
    ```
        type AttributeTypeName = ...attribute type>;
    ```
    */
    proc_macro::TokenStream::from(quote! {
        type #attribute_type_name = #attribute_type;
    })
}

#[proc_macro_attribute]
pub fn event(
    raw_args: proc_macro::TokenStream,
    raw_body: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let args = TokenStream::from(raw_args);
    if !args.is_empty() {
        return proc_macro::TokenStream::from(generate_compile_error(
            &MeteringMacrosError::EventArgsError.to_string(),
        ));
    }

    let body = TokenStream::from(raw_body);

    let EventBodyResult {
        event_name,
        fields,
        field_to_mutator,
    } = match process_event_body(&body) {
        Ok(result) => result,
        Err(error) => {
            return proc_macro::TokenStream::from(generate_compile_error(&error.to_string()))
        }
    };

    let mut field_names: Vec<Ident> = Vec::new();
    let mut field_types: Vec<TokenStream> = Vec::new();

    let mut constant_field_names: Vec<Ident> = Vec::new();
    let mut constant_field_types: Vec<TokenStream> = Vec::new();

    let mut annotated_fields: Vec<registry::AnnotatedField> = Vec::new();

    let mut registry_tuples: Vec<(String, String, String)> = Vec::new();

    let mut attribute_methods: Vec<Ident> = Vec::new();
    let mut mutable_types: Vec<TokenStream> = Vec::new();
    let mut mutator_idents: Vec<Ident> = Vec::new();

    for field in &fields {
        field_names.push(field.field_name.clone());
        field_types.push(field.field_type.clone());

        if field.field_mutability == FieldMutability::Constant {
            constant_field_names.push(field.field_name.clone());
            constant_field_types.push(field.field_type.clone());
        } else {
            annotated_fields.push(registry::AnnotatedField {
                field_name: field.field_name.to_string(),
                attribute_name: field
                    .field_attribute_name
                    .clone()
                    .expect("mutable fields must have an attribute name")
                    .to_string()
                    .trim_matches('\"')
                    .to_string(),
                mutator_name: field_to_mutator
                    .get(&field.field_name)
                    .expect("must have a mutator for this annotated field")
                    .to_string()
                    .trim_matches('\"')
                    .to_string(),
            });

            let attr_name_literal = field
                .field_attribute_name
                .clone()
                .expect("mutable field always has an attribute name");
            let attr_name_str = attr_name_literal.to_string().trim_matches('\"').to_string();
            let mutator_ident_str = field_to_mutator
                .get(&field.field_name)
                .expect("must have a mutator for this annotated field")
                .to_string()
                .trim_matches('\"')
                .to_string();
            let mutator_ident = Ident::new(&mutator_ident_str, Span::call_site());

            let field_ty_str: String = field.field_type.to_string();
            registry_tuples.push((
                attr_name_str.clone(),
                mutator_ident_str.clone(),
                field_ty_str.clone(),
            ));

            let method_ident = Ident::new(&attr_name_str, Span::call_site());
            attribute_methods.push(method_ident);
            mutable_types.push(field.field_type.clone());
            mutator_idents.push(mutator_ident);
        }
    }

    let event_name_string = event_name.to_string().trim_matches('\"').to_string();
    if let Err(error) = registry::register_event(&event_name_string, annotated_fields) {
        return proc_macro::TokenStream::from(generate_compile_error(&format!(
            "failed to register event: {}",
            error
        )));
    }

    let expanded = quote! {
        #[derive(std::fmt::Debug, std::default::Default, Clone)]
        struct #event_name {
            #(
                #[allow(dead_code)]
                #field_names : #field_types
            ),*
        }

        impl #event_name {
            pub fn new( #( #constant_field_names : #constant_field_types ),* ) -> Self {
                Self {
                    #(
                        #constant_field_names: #constant_field_names
                    ),*,
                    ..Default::default()
                }
            }

            #(
                fn #attribute_methods(&mut self, value: #mutable_types) {
                    #mutator_idents(self, value);
                }
            )*
        }

        impl chroma_metering::MeteringEvent for #event_name {
            #(
                fn #attribute_methods(&mut self, value: #mutable_types) {
                    #mutator_idents(self, value);
                }
            )*
        }
    };

    expanded.into()
}

#[proc_macro]
pub fn generate_base_mutators(_input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let registered_attributes = match registry::list_registered_attributes() {
        Ok(result) => result,
        Err(_) => {
            return proc_macro::TokenStream::from(generate_compile_error(
                "failed to read registered attributes from registry",
            ))
        }
    };

    let mut base_mutators: Vec<TokenStream> = Vec::new();
    for (attribute_name, attribute_type_tokens_str) in registered_attributes.into_iter() {
        let mutator_name = Ident::new(&attribute_name, Span::call_site());

        let attribute_type_tokens: TokenStream = match syn::parse_str(&attribute_type_tokens_str) {
            Ok(ts) => ts,
            Err(e) => {
                return proc_macro::TokenStream::from(generate_compile_error(&format!(
                    "registered attribute type `{}` could not parse as a Rust type: {}",
                    attribute_type_tokens_str, e
                )));
            }
        };

        base_mutators.push(quote! {
            fn #mutator_name(&mut self, _value: #attribute_type_tokens) { }
        });
    }

    let expanded = quote! {
        #(#base_mutators)*
    };
    expanded.into()
}
