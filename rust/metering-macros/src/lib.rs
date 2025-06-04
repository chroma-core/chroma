//! This crate contains the implementation of the procedural macros used in
//! Chroma's metering library. There are three primary exports:
//!     - [`crate::attribute`] is a procedural attribute macro that
//!       parses definitions for metering attributes and
//!       registers them in the application's metering registry.
//!     - [`crate::event`] is a procedural attribute macro that parses
//!       a metering event definition, registers the event, and
//!       generates the necessary trait implementations for the
//!       event.
//!     - [`crate::generate_noop_mutators`] is a procedural functional
//!       macro that is used by the `chroma-metering` crate (it is not
//!       intended for use in applications) to generate definitions
//!       of no-op mutators on the `MeteringEvent` trait.
//! NOTE(c-gamble): Throughout the comments in this crate, we use
//! `<>` to denote values that are placeholders for user-defined
//! variables.

extern crate proc_macro;

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::parse_str;
use utils::generate_compile_error;

use crate::attribute::{
    parse_attribute_args, parse_attribute_body, AttributeArgsParseResult, AttributeBodyParseResult,
};
use crate::errors::MeteringMacrosError;
use crate::event::{parse_event_body, EventBodyParseResult, FieldMutability};
use crate::utils::literal_to_string;

mod attribute;
mod errors;
mod event;
mod registry;
mod utils;

//////////////////////////////////////// `attribute` macro ////////////////////////////////////////
/// # Overview
/// This macro is used to register metering attributes. It requires `name` as an argument
/// an expects the value supplied to `name` to be a string literal. It must be invoked
/// on a single type alias definition, the identifier of which must be the CamelCase
/// equivalent of the value supplied to `name`. The type to which the identifier is
/// assigned may be of arbitrary complexity.
///
/// # Example Usage
/// ```
/// #[attribute(name = "my_attribute")]
/// type MyAttribute = Option<u8>;
/// ```
#[proc_macro_attribute]
pub fn attribute(
    raw_args: proc_macro::TokenStream,
    raw_body: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    // Convert built-in `TokenStream` to `proc_macro2::TokenStream`.
    let args = TokenStream::from(raw_args);
    let body = TokenStream::from(raw_body);

    // Parse the arguments passed to the macro. We expect only a string name.
    let AttributeArgsParseResult {
        attribute_name_string,
    } = match parse_attribute_args(&args) {
        Ok(result) => result,
        Err(error) => {
            return generate_compile_error(&error.to_string());
        }
    };

    // Parse the tokens on which the macro was invoked. We expect only a type alias definition.
    let AttributeBodyParseResult {
        attribute_type_alias_ident,
        attribute_type_token_stream,
    } = match parse_attribute_body(&body, &attribute_name_string) {
        Ok(result) => result,
        Err(error) => {
            return generate_compile_error(&error.to_string());
        }
    };

    // Register the attribute in the application's registry.
    // NOTE(c-gamble): This code is executed at compilation, not at runtime.
    if let Err(error) = registry::register_attribute(
        &attribute_name_string,
        &attribute_type_token_stream.to_string(),
    ) {
        return generate_compile_error(&format!("failed to register attribute: {}", error));
    }

    // Generate the macro's output code.
    // The following code generates:
    // ```
    // type <AttributeTypeName> = <attribute type>;
    // ```
    return proc_macro::TokenStream::from(quote! {
        type #attribute_type_alias_ident = #attribute_type_token_stream;
    });
}

//////////////////////////////////////// `event` macro ////////////////////////////////////////
/// # Overview
/// This macro is used to register metering events and generate the required trait
/// implementations required to allow a user-defined metering event to work with
/// the `chroma-metering` library. No arguments should be passed to the macro
/// invocation. The macro expects to be applied to a valid struct definition.
/// The macro may be used alongside other macros from other crates, and
/// it is agnostic to invocation order. It expects zero or more field annotations
/// on the struct's fields. Even though a field annotation looks like a macro,
/// it is not. It is parsed by the same macro as the event itself. Field annotations
/// require two string literal arguments: `attribute` and `mutator`. The value
/// provided to `attribute` must be a registered attribute. The type of the field
/// must be equivalent to the type of registered attribute. The value provided to
/// `mutator` must be the name of a user-defined function that is a valid symbol
/// within the scope of the event's definition. The user-defined function must
/// accept a mutable reference to the event in which it is used as a mutator as
/// its first argument, and it must accept a value of the same type as the
/// field which it is intended to mutate as its second argument. The field
/// annotation applies to a single struct field. Struct fields that are not
/// annotated with field annotations will be . The macro expects that all
/// types used for struct fields implement the `std::default::Default` trait.
///
/// # Example Usage
/// ```
/// #[event]
/// struct MyEvent {
///     constant_field: String,
///     #[field(attribute = "my_attribute", mutator = "my_mutator")]
///     annotated_field: u8
/// }
///
/// fn my_mutator(event: &mut MyEvent, value: u8) {
///     event.annotated_field += value;
/// }
/// ```
#[proc_macro_attribute]
pub fn event(
    raw_args: proc_macro::TokenStream,
    raw_body: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    // Convert built-in `TokenStream` to `proc_macro2::TokenStream`.
    let args = TokenStream::from(raw_args);
    let body = TokenStream::from(raw_body);

    // Verify that no arguments have been provided to the macro.
    if !args.is_empty() {
        return generate_compile_error(&MeteringMacrosError::EventArgsError.to_string());
    }

    // Parse the tokens on which the macrto was invoked. We expect a valid struct
    // definition with zero or more field annotations.
    let EventBodyParseResult {
        event_name_ident,
        fields,
        field_name_ident_to_mutator_name_literal,
    } = match parse_event_body(&body) {
        Ok(result) => result,
        Err(error) => {
            return generate_compile_error(&error.to_string());
        }
    };

    // Store all field names and type definitions to regenerate the struct definition.
    let mut field_name_idents: Vec<Ident> = Vec::new();
    let mut field_type_token_streams: Vec<TokenStream> = Vec::new();

    // Store only constant field names and type definitions.
    let mut constant_field_name_idents: Vec<Ident> = Vec::new();
    let mut constant_field_type_token_streams_idents: Vec<TokenStream> = Vec::new();

    // Store annotated fields as `AnnotatedField` objects for the registry.
    let mut annotated_fields: Vec<registry::AnnotatedField> = Vec::new();

    // Store identifiers of the names of the mutators that are implemented by `MeteringEvent`.
    let mut mutator_name_idents: Vec<Ident> = Vec::new();

    // Store the tokens that comprise the type definitions for the attributes used in this event.
    let mut attribute_type_token_streams: Vec<TokenStream> = Vec::new();

    // Store identifiers of the custom mutators called during the overrides of the no-op
    // mutators in `MeteringEvent`.
    let mut custom_mutator_name_idents: Vec<Ident> = Vec::new();

    // Iterate over the fields obtained from parsing.
    for field in &fields {
        // Store the field's information in the "all fields" vectors.
        field_name_idents.push(field.field_name_ident.clone());
        field_type_token_streams.push(field.field_type_token_stream.clone());

        // If the field is a constant field, also store it in the "constant fields" vectors.
        if field.field_mutability == FieldMutability::Constant {
            constant_field_name_idents.push(field.field_name_ident.clone());
            constant_field_type_token_streams_idents.push(field.field_type_token_stream.clone());
        } else {
            // Verify that the annotated field has an attribute.
            let Some(attribute_name_literal) = &field.attribute_name_literal else {
                return generate_compile_error(
                    &MeteringMacrosError::AnnotatedFieldMissingAttributeError(
                        field.field_name_ident.to_string(),
                    )
                    .to_string(),
                );
            };

            // Convert the attribute name to a string.
            let attribute_name_string = literal_to_string(attribute_name_literal);

            // Verify that the annotated field has a mutator.
            let Some(mutator_name_literal) =
                field_name_ident_to_mutator_name_literal.get(&field.field_name_ident)
            else {
                return generate_compile_error(
                    &MeteringMacrosError::AnnotatedFieldMissingMutatorError(
                        field.field_name_ident.to_string(),
                    )
                    .to_string(),
                );
            };

            // If the field is mutable, we know it's annotated so we push it onto the "annotated fields" vector.
            annotated_fields.push(registry::AnnotatedField {
                field_name_string: field.field_name_ident.to_string(),
                attribute_name_string: attribute_name_string.clone(),
                mutator_name_string: literal_to_string(mutator_name_literal),
            });

            // The name of the mutator implemented on the base trait is the name of the attribute.
            let mutator_name_ident = Ident::new(&attribute_name_string, Span::call_site());
            mutator_name_idents.push(mutator_name_ident);

            // Extract the attribute type that is stored in the registry.
            let (_, attribute_type_string) =
                match registry::get_registered_attribute(&attribute_name_string) {
                    Ok(result) => result,
                    Err(error) => {
                        return generate_compile_error(&error.to_string());
                    }
                };

            // Parse the attribute type as a token stream.
            let attribute_type_token_stream = match parse_str(&attribute_name_string) {
                Ok(result) => result,
                Err(error) => {
                    return generate_compile_error(&format!(
                        "Could not parse token stream from attribute type definition `{}`: {}",
                        attribute_type_string, error
                    ))
                }
            };
            attribute_type_token_streams.push(attribute_type_token_stream);

            // Extract the name of the custom mutator from the mapping of field names to mutators
            // and convert it to an identifier.
            let custom_mutator_name_ident = Ident::new(
                &literal_to_string(
                    field_name_ident_to_mutator_name_literal
                        .get(&field.field_name_ident)
                        .expect("must have a mutator for this annotated field"),
                ),
                Span::call_site(),
            );
            custom_mutator_name_idents.push(custom_mutator_name_ident);
        }
    }

    // Register the event into the registry.
    // NOTE(c-gamble): This code is executed at compilation, not runtime.
    if let Err(error) = registry::register_event(&event_name_ident.to_string(), annotated_fields) {
        return generate_compile_error(&format!(
            "Failed to register event `{}`: {}",
            event_name_ident.to_string(),
            error
        ));
    }

    // Generate the macro's output code.
    // The following code generates:
    // ```
    // #[derive(std::fmt::Debug, std::default::Default, Clone)]
    // struct <EventName> {
    //      #[allow(dead_code)] // to suppress compiler warnings (TODO(c-gamble): figure out how to get IDEs to comply without suppressing)
    //      <all fields>
    // }
    //
    // impl <EventName> {
    //      pub fn new(<constant fields>) -> Self {
    //          Self {
    //              <constant fields>
    //          }
    //      }
    //
    //      // NOTE(c-gamble): We include an inherent implementation, as well as an implementation
    //      // via the `MeteringEvent` trait for better code completion with language servers.
    //
    //      <overrides for the no-ops with custom mutators on every attribute used in the event>
    // }
    //
    // impl chroma_metering::MeteringEvent <EventName> {
    //      <overrides for the no-ops with custom mutators on every attribute used in the event>
    // }
    // ```
    return proc_macro::TokenStream::from(quote! {
        #[derive(std::fmt::Debug, std::default::Default, Clone)]
        struct #event_name_ident {
            #(
                #[allow(dead_code)]
                #field_name_idents : #field_type_token_streams
            ),*
        }

        impl #event_name_ident {
            pub fn new( #( #constant_field_name_idents : #constant_field_type_token_streams_idents ),* ) -> Self {
                Self {
                    #(
                        #constant_field_name_idents: #constant_field_name_idents
                    ),*,
                    ..Default::default()
                }
            }

            #(
                fn #mutator_name_idents(&mut self, value: #attribute_type_token_streams) {
                    #custom_mutator_name_idents(self, value);
                }
            )*
        }

        impl chroma_metering::MeteringEvent for #event_name_ident {
            #(
                fn #mutator_name_idents(&mut self, value: #attribute_type_token_streams) {
                    #custom_mutator_name_idents(self, value);
                }
            )*
        }
    });
}

//////////////////////////////////////// `generate_noop_mutators` macro ////////////////////////////////////////
/// # Overview
/// This macro is intended for internal use by the `chroma-metering` library. It is not intended for use by
/// users of the `chroma-metering` library. Its purpose is to generate no-op methods on the `MeteringEvent`
/// trait such that these methods may be overridden by custom mutators in the trait's implementation for
/// a user-defined event.
///
/// # Example Usage
/// ```
/// pub trait MeteringEvent: Debug + Any + Send + 'static {
///     generate_noop_mutators! {}
/// }
/// ```
#[proc_macro]
pub fn generate_noop_mutators(_input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // Get all of the registered attributes from the registry.
    let registered_attributes = match registry::get_registered_attributes() {
        Ok(result) => result,
        Err(error) => {
            return generate_compile_error(&format!(
                "Failed to read registered attributes from registry: {}",
                error
            ));
        }
    };

    let mut base_mutators: Vec<TokenStream> = Vec::new();
    for (attribute_name, attribute_type_tokens_str) in registered_attributes.into_iter() {
        let mutator_name = Ident::new(&attribute_name, Span::call_site());

        let attribute_type_tokens: TokenStream = match syn::parse_str(&attribute_type_tokens_str) {
            Ok(ts) => ts,
            Err(e) => {
                return generate_compile_error(&format!(
                    "registered attribute type `{}` could not parse as a Rust type: {}",
                    attribute_type_tokens_str, e
                ));
            }
        };

        base_mutators.push(quote! {
            fn #mutator_name(&mut self, _value: #attribute_type_tokens) { }
        });
    }

    return proc_macro::TokenStream::from(quote! {
        #(#base_mutators)*
    });
}
