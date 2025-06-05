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

use std::collections::{HashMap, HashSet};

use proc_macro2::{Ident, Span, TokenStream, TokenTree};
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

// //////////////////////////////////////// `attribute` macro ////////////////////////////////////////
// /// # Overview
// /// This macro is used to register metering attributes. It requires `name` as an argument
// /// an expects the value supplied to `name` to be a string literal. It must be invoked
// /// on a single type alias definition, the identifier of which must be the CamelCase
// /// equivalent of the value supplied to `name`. The type to which the identifier is
// /// assigned may be of arbitrary complexity.
// ///
// /// # Example Usage
// /// ```
// /// #[attribute(name = "my_attribute")]
// /// type MyAttribute = Option<u8>;
// /// ```
// #[proc_macro_attribute]
// pub fn attribute(
//     raw_args: proc_macro::TokenStream,
//     raw_body: proc_macro::TokenStream,
// ) -> proc_macro::TokenStream {
//     // Convert built-in `TokenStream` to `proc_macro2::TokenStream`.
//     let args = TokenStream::from(raw_args);
//     let body = TokenStream::from(raw_body);

//     // Parse the arguments passed to the macro. We expect only a string name.
//     let AttributeArgsParseResult {
//         attribute_name_string,
//     } = match parse_attribute_args(&args) {
//         Ok(result) => result,
//         Err(error) => {
//             return generate_compile_error(&error.to_string());
//         }
//     };

//     // Parse the tokens on which the macro was invoked. We expect only a type alias definition.
//     let AttributeBodyParseResult {
//         attribute_type_alias_ident,
//         attribute_type_token_stream,
//     } = match parse_attribute_body(&body, &attribute_name_string) {
//         Ok(result) => result,
//         Err(error) => {
//             return generate_compile_error(&error.to_string());
//         }
//     };

//     // Register the attribute in the application's registry.
//     // NOTE(c-gamble): This code is executed at compilation, not at runtime.
//     if let Err(error) = registry::register_attribute(
//         &attribute_name_string,
//         &attribute_type_token_stream.to_string(),
//     ) {
//         return generate_compile_error(&format!("failed to register attribute: {}", error));
//     }

//     // Generate the macro's output code.
//     // The following code generates:
//     // ```
//     // type <AttributeTypeName> = <attribute type>;
//     // ```
//     return proc_macro::TokenStream::from(quote! {
//         type #attribute_type_alias_ident = #attribute_type_token_stream;
//     });
// }

// //////////////////////////////////////// `event` macro ////////////////////////////////////////
// /// # Overview
// /// This macro is used to register metering events and generate the required trait
// /// implementations required to allow a user-defined metering event to work with
// /// the `chroma-metering` library. No arguments should be passed to the macro
// /// invocation. The macro expects to be applied to a valid struct definition.
// /// The macro may be used alongside other macros from other crates, and
// /// it is agnostic to invocation order. It expects zero or more field annotations
// /// on the struct's fields. Even though a field annotation looks like a macro,
// /// it is not. It is parsed by the same macro as the event itself. Field annotations
// /// require two string literal arguments: `attribute` and `mutator`. The value
// /// provided to `attribute` must be a registered attribute. The type of the field
// /// must be equivalent to the type of registered attribute. The value provided to
// /// `mutator` must be the name of a user-defined function that is a valid symbol
// /// within the scope of the event's definition. The user-defined function must
// /// accept a mutable reference to the event in which it is used as a mutator as
// /// its first argument, and it must accept a value of the same type as the
// /// field which it is intended to mutate as its second argument. The field
// /// annotation applies to a single struct field. Struct fields that are not
// /// annotated with field annotations will be . The macro expects that all
// /// types used for struct fields implement the `std::default::Default` trait.
// ///
// /// # Example Usage
// /// ```
// /// #[event]
// /// struct MyEvent {
// ///     constant_field: String,
// ///     #[field(attribute = "my_attribute", mutator = "my_mutator")]
// ///     annotated_field: u8
// /// }
// ///
// /// fn my_mutator(event: &mut MyEvent, value: u8) {
// ///     event.annotated_field += value;
// /// }
// /// ```
// #[proc_macro_attribute]
// pub fn event(
//     raw_args: proc_macro::TokenStream,
//     raw_body: proc_macro::TokenStream,
// ) -> proc_macro::TokenStream {
//     // Convert built-in `TokenStream` to `proc_macro2::TokenStream`.
//     let args = TokenStream::from(raw_args);
//     let body = TokenStream::from(raw_body);

//     // Verify that no arguments have been provided to the macro.
//     if !args.is_empty() {
//         return generate_compile_error(&MeteringMacrosError::EventArgsError.to_string());
//     }

//     // Parse the tokens on which the macrto was invoked. We expect a valid struct
//     // definition with zero or more field annotations.
//     let EventBodyParseResult {
//         event_name_ident,
//         fields,
//         field_name_ident_to_mutator_name_literal,
//     } = match parse_event_body(&body) {
//         Ok(result) => result,
//         Err(error) => {
//             return generate_compile_error(&error.to_string());
//         }
//     };

//     // Store all field names and type definitions to regenerate the struct definition.
//     let mut field_name_idents: Vec<Ident> = Vec::new();
//     let mut field_type_token_streams: Vec<TokenStream> = Vec::new();

//     // Store only constant field names and type definitions.
//     let mut constant_field_name_idents: Vec<Ident> = Vec::new();
//     let mut constant_field_type_token_streams_idents: Vec<TokenStream> = Vec::new();

//     // Store annotated fields as `AnnotatedField` objects for the registry.
//     let mut annotated_fields: Vec<registry::AnnotatedField> = Vec::new();

//     // Store identifiers of the names of the mutators that are implemented by `MeteringEvent`.
//     let mut mutator_name_idents: Vec<Ident> = Vec::new();

//     // Store the tokens that comprise the type definitions for the attributes used in this event.
//     let mut attribute_type_token_streams: Vec<TokenStream> = Vec::new();

//     // Store identifiers of the custom mutators called during the overrides of the no-op
//     // mutators in `MeteringEvent`.
//     let mut custom_mutator_name_idents: Vec<Ident> = Vec::new();

//     // Iterate over the fields obtained from parsing.
//     for field in &fields {
//         // Store the field's information in the "all fields" vectors.
//         field_name_idents.push(field.field_name_ident.clone());
//         field_type_token_streams.push(field.field_type_token_stream.clone());

//         // If the field is a constant field, also store it in the "constant fields" vectors.
//         if field.field_mutability == FieldMutability::Constant {
//             constant_field_name_idents.push(field.field_name_ident.clone());
//             constant_field_type_token_streams_idents.push(field.field_type_token_stream.clone());
//         } else {
//             // Verify that the annotated field has an attribute.
//             let Some(attribute_name_literal) = &field.attribute_name_literal else {
//                 return generate_compile_error(
//                     &MeteringMacrosError::AnnotatedFieldMissingAttributeError(
//                         field.field_name_ident.to_string(),
//                     )
//                     .to_string(),
//                 );
//             };

//             // Convert the attribute name to a string.
//             let attribute_name_string = literal_to_string(attribute_name_literal);

//             // Verify that the annotated field has a mutator.
//             let Some(mutator_name_literal) =
//                 field_name_ident_to_mutator_name_literal.get(&field.field_name_ident)
//             else {
//                 return generate_compile_error(
//                     &MeteringMacrosError::AnnotatedFieldMissingMutatorError(
//                         field.field_name_ident.to_string(),
//                     )
//                     .to_string(),
//                 );
//             };

//             // If the field is mutable, we know it's annotated so we push it onto the "annotated fields" vector.
//             annotated_fields.push(registry::AnnotatedField {
//                 field_name_string: field.field_name_ident.to_string(),
//                 attribute_name_string: attribute_name_string.clone(),
//                 mutator_name_string: literal_to_string(mutator_name_literal),
//             });

//             // The name of the mutator implemented on the base trait is the name of the attribute.
//             let mutator_name_ident = Ident::new(&attribute_name_string, Span::call_site());
//             mutator_name_idents.push(mutator_name_ident);

//             // Extract the attribute type that is stored in the registry.
//             let (_, attribute_type_string) =
//                 match registry::get_registered_attribute(&attribute_name_string) {
//                     Ok(result) => result,
//                     Err(error) => {
//                         return generate_compile_error(&error.to_string());
//                     }
//                 };

//             // Parse the attribute type as a token stream.
//             let attribute_type_token_stream = match parse_str(&attribute_type_string) {
//                 Ok(result) => result,
//                 Err(error) => {
//                     return generate_compile_error(&format!(
//                         "Could not parse token stream from attribute type definition `{}`: {}",
//                         attribute_type_string, error
//                     ))
//                 }
//             };
//             attribute_type_token_streams.push(attribute_type_token_stream);

//             // Extract the name of the custom mutator from the mapping of field names to mutators
//             // and convert it to an identifier.
//             let custom_mutator_name_ident = Ident::new(
//                 &literal_to_string(
//                     field_name_ident_to_mutator_name_literal
//                         .get(&field.field_name_ident)
//                         .expect("must have a mutator for this annotated field"),
//                 ),
//                 Span::call_site(),
//             );
//             custom_mutator_name_idents.push(custom_mutator_name_ident);
//         }
//     }

//     // Register the event into the registry.
//     // NOTE(c-gamble): This code is executed at compilation, not runtime.
//     if let Err(error) = registry::register_event(&event_name_ident.to_string(), annotated_fields) {
//         return generate_compile_error(&format!(
//             "Failed to register event `{}`: {}",
//             event_name_ident.to_string(),
//             error
//         ));
//     }

//     // Generate the macro's output code.
//     // The following code generates:
//     // ```
//     // #[derive(std::fmt::Debug, std::default::Default, Clone)]
//     // struct <EventName> {
//     //      #[allow(dead_code)] // to suppress compiler warnings (TODO(c-gamble): figure out how to get IDEs to comply without suppressing)
//     //      <all fields>
//     // }
//     //
//     // impl <EventName> {
//     //      pub fn new(<constant fields>) -> Self {
//     //          Self {
//     //              <constant fields>
//     //          }
//     //      }
//     //
//     //      // NOTE(c-gamble): We include an inherent implementation, as well as an implementation
//     //      // via the `MeteringEvent` trait for better code completion with language servers.
//     //
//     //      <overrides for the no-ops with custom mutators on every attribute used in the event>
//     // }
//     //
//     // impl MeteringEvent <EventName> {
//     //      <overrides for the no-ops with custom mutators on every attribute used in the event>
//     // }
//     // ```
//     return proc_macro::TokenStream::from(quote! {
//         #[derive(std::fmt::Debug, std::default::Default, Clone)]
//         struct #event_name_ident {
//             #(
//                 #[allow(dead_code)]
//                 #field_name_idents : #field_type_token_streams
//             ),*
//         }

//         impl #event_name_ident {
//             pub fn new( #( #constant_field_name_idents : #constant_field_type_token_streams_idents ),* ) -> Self {
//                 Self {
//                     #(
//                         #constant_field_name_idents: #constant_field_name_idents
//                     ),*,
//                     ..Default::default()
//                 }
//             }

//             #(
//                 fn #mutator_name_idents(&mut self, value: #attribute_type_token_streams) {
//                     #custom_mutator_name_idents(self, value);
//                 }
//             )*
//         }

//         impl MeteringEvent for #event_name_ident {
//             #(
//                 fn #mutator_name_idents(&mut self, value: #attribute_type_token_streams) {
//                     #custom_mutator_name_idents(self, value);
//                 }
//             )*
//         }
//     });
// }

// //////////////////////////////////////// `generate_noop_mutators` macro ////////////////////////////////////////
// /// # Overview
// /// This macro is intended for internal use by the `chroma-metering` library. It is not intended for use by
// /// users of the `chroma-metering` library. Its purpose is to generate no-op methods on the `MeteringEvent`
// /// trait such that these methods may be overridden by custom mutators in the trait's implementation for
// /// a user-defined event. It should be called from inside the definition of the `MeteringEvent` trait.
// ///
// /// # Example Usage
// /// ```
// /// pub trait MeteringEvent: Debug + Any + Send + 'static {
// ///     generate_noop_mutators! {}
// /// }
// /// ```
// #[proc_macro]
// pub fn generate_noop_mutators(_input: proc_macro::TokenStream) -> proc_macro::TokenStream {
//     // Get all of the registered attributes from the registry.
//     let registered_attributes = match registry::get_registered_attributes() {
//         Ok(result) => result,
//         Err(error) => {
//             return generate_compile_error(&format!(
//                 "Failed to read registered attributes from registry: {}",
//                 error
//             ));
//         }
//     };
//     eprintln!(
//         "Registered attributes in generate_noop_mutators!: {:?}",
//         registered_attributes
//     );
//     // Collect the identifiers for the no-op mutators.
//     let mut mutator_name_idents: Vec<Ident> = Vec::new();

//     // Collect the token streams for the attributes' type definitions.
//     let mut attribute_type_token_streams: Vec<TokenStream> = Vec::new();

//     // Iterate over all of the registered attributes.
//     for (attribute_name_string, attribute_type_string) in registered_attributes.into_iter() {
//         // The name of the mutator for the `MeteringEvent` trait is just the name of the attribute.
//         let mutator_name_ident = Ident::new(&attribute_name_string, Span::call_site());
//         mutator_name_idents.push(mutator_name_ident);

//         // Parse the attribute's type as a token stream.
//         let attribute_type_token_stream: TokenStream = match syn::parse_str(&attribute_type_string)
//         {
//             Ok(result) => result,
//             Err(error) => {
//                 return generate_compile_error(&format!(
//                     "Failed to parse token stream from registered attribute type `{}`: {}",
//                     attribute_type_string, error
//                 ));
//             }
//         };
//         attribute_type_token_streams.push(attribute_type_token_stream);
//     }

//     // Generate the macro's output code.
//     // The following code generates:
//     // ```
//     // <no-op mutators for the every attribute in the register>
//     // ```
//     return proc_macro::TokenStream::from(quote! {
//         #(
//             fn #mutator_name_idents(&mut self, _value: #attribute_type_token_streams) { }
//         )*
//     });
// }

struct Attribute {
    foreign_macro_token_streams: Vec<TokenStream>,
    visibility_modifier_ident: Option<Ident>,
    attribute_type_alias_ident: Ident,
    attribute_name_string: String,
    attribute_name_ident: Ident,
    attribute_type_string: String,
    attribute_type_token_stream: TokenStream,
}

fn process_attribute_definition_tokens(
    attribute_tokens: Vec<TokenTree>,
) -> Result<Attribute, MeteringMacrosError> {
}

fn process_event_definition_tokens(
    event_tokens: Vec<TokenTree>,
) -> Result<Event, MeteringMacrosError> {
}

fn process_field_definition_tokens(
    field_tokens: Vec<TokenTree>,
) -> Result<Field, MeteringMacrosError> {
}

struct Field {
    foreign_macro_token_streams: Vec<TokenStream>,
    visibility_modifier_ident: Option<Ident>,
    field_name_ident: Ident,
    field_type_token_stream: TokenStream,
    attribute: Option<Attribute>,
    custom_mutator_name_ident: Option<Ident>,
}

struct Event {
    foreign_macro_token_streams: Vec<TokenStream>,
    visibility_modifier_ident: Option<Ident>,
    event_name_ident: Ident,
    fields: Vec<Field>,
}

fn generate_noop_mutator_definition_token_stream(attribute: &Attribute) -> TokenStream {
    let Attribute {
        foreign_macro_token_streams: _foreign_macro_token_streams,
        visibility_modifier_ident: _visibility_modifier_ident,
        attribute_type_alias_ident: _attribute_type_alias_ident,
        attribute_name_string: _attribute_name_string,
        attribute_name_ident: attribute_name_ident,
        attribute_type_string: _attribute_type_string,
        attribute_type_token_stream: attribute_type_token_stream,
    } = attribute;

    let noop_mutator_definition_token_stream = quote! {
        fn #attribute_name_ident(&mut self, _: #attribute_type_token_stream) {}
    };

    return noop_mutator_definition_token_stream;
}

fn generate_attribute_definition_token_stream(attribute: &Attribute) -> TokenStream {
    let Attribute {
        foreign_macro_token_streams: foreign_macro_token_streams,
        visibility_modifier_ident: visibility_modifier_ident,
        attribute_type_alias_ident: attribute_type_alias_ident,
        attribute_name_string: _attribute_name_string,
        attribute_name_ident: _attribute_name_ident,
        attribute_type_string: _attribute_type_string,
        attribute_type_token_stream: attribute_type_token_stream,
    } = attribute;

    let attribute_definition_token_stream = if visibility_modifier_ident.is_some() {
        quote! {
            #( #foreign_macro_token_streams )*
            #visibility_modifier_ident type #attribute_type_alias_ident = #attribute_type_token_stream;
        }
    } else {
        quote! {
            #( #foreign_macro_token_streams )*
            type #attribute_type_alias_ident = #attribute_type_token_stream;
        }
    };

    return attribute_definition_token_stream;
}

fn generate_field_definition_token_stream(field: &Field) -> TokenStream {
    let Field {
        foreign_macro_token_streams: foreign_macro_token_streams,
        visibility_modifier_ident: visibility_modifier_ident,
        field_name_ident: field_name_ident,
        field_type_token_stream: field_type_token_stream,
        attribute: attribute,
        custom_mutator_name_ident: _custom_mutator_name_ident,
    } = field;

    let field_definition_token_stream = if visibility_modifier_ident.is_some() {
        if let Some(Attribute {
            foreign_macro_token_streams: _foreign_macro_token_streams,
            visibility_modifier_ident: _visibility_modifier_ident,
            attribute_type_alias_ident: attribute_type_alias_ident,
            attribute_name_string: _attribute_name_string,
            attribute_name_ident: _attribute_name_ident,
            attribute_type_string: _attribute_type_string,
            attribute_type_token_stream: _attribute_type_token_stream,
        }) = attribute
        {
            quote! {
                #( #foreign_macro_token_streams )*
                #visibility_modifier_ident #field_name_ident: #attribute_type_alias_ident,
            }
        } else {
            quote! {
                #( #foreign_macro_token_streams )*
                #visibility_modifier_ident #field_name_ident: #field_type_token_stream,
            }
        }
    } else {
        if let Some(Attribute {
            foreign_macro_token_streams: _foreign_macro_token_streams,
            visibility_modifier_ident: _visibility_modifier_ident,
            attribute_type_alias_ident: attribute_type_alias_ident,
            attribute_name_string: _attribute_name_string,
            attribute_name_ident: _attribute_name_ident,
            attribute_type_string: _attribute_type_string,
            attribute_type_token_stream: _attribute_type_token_stream,
        }) = attribute
        {
            quote! {
                #( #foreign_macro_token_streams )*
                #visibility_modifier_ident #field_name_ident: #attribute_type_alias_ident,
            }
        } else {
            quote! {
                #( #foreign_macro_token_streams )*
                #visibility_modifier_ident #field_name_ident: #field_type_token_stream,
            }
        }
    };

    return field_definition_token_stream;
}

fn generate_event_definition_token_stream(event: &Event) -> TokenStream {
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
        #[derive(std::fmt::Debug, std::default::Default, Clone)]
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

/// This handles validating the token stream we receive from the macro invocation and returning attributes/events if it is valid.
/// We expect users to _only_ include attribute and event definitions in the invocation, not any other code.
/// NOTE(c-gamble): Later, we may consider requiring mutator definitions to be in here as well, but since these aren't validated
/// by our library, we are forcing them to be outside of the invocation for now.
/* Parsing logic:
    -every group should either be a type definition (with an optional visibility modifier) or a struct definition (also with an optional visibility modifier)
    -all outermost groups must be annotated with either #[attribute(name = "<attribute_name>")] or #[event] (no args)
        -if we encounter a type definition or struct definition (or any other code) without first having seen an annotation that can be applied to it, it's invalid
    -once we know we have a valid annotation, we have to decide if it's for an attribtue or an event
        -if it's for an attribute, there must be exaclty one argument passed in as `name = "<some_literal">`. all other argument combinations are invalid
        -if it's for an event, no arguments should be supplied. if any arguments are supplied, the input is invalid.
    -after parsing our annotation (for both attributes and events), we need to be cognizant of other macro invocations. for each type (Event, Attribute, and Field (which we discuss in more depth below)), we have
    a Vec<TokenStream> called `foreign_macro_token_streams` in which we can store the tokens that we recognize as foreign macros to spit back out
    -now, let's talk about each primary type (event vs attribute) in depth

    for attributes:
        these are relatively straightforward. they are single-line type definitions in which users specify an optional visibility modifier like pub or pub(crate) or pub(super), etc. followed by the `type` keyword.
        after the type keyword, users add an alias for the type. then, they specify a type of arbitrary complexity. the type definition ends at the first semicolon we encounter. an example of an attribute annotation
        would be:
            #[attribute(name = "my_attribute")]
            type SomeTypeAlias = Option<Vec<Box<dyn Send>>>;
    for events:
        these are slightly more complex than attributes. events are structs with optional visibility modifiers and nested fields. fields within structs may be annotated or not annotated. if a field is not annotated,
        it is considered an "unannotated field" and does not have an attribute or a custom mutator. in this case, the field should just be a syntactically valid struct field definition with an optional visibility modifier.
        fields that are annotated must be annotated with #[field(attribute =  "<an attribute that exists in `registered_attributes`>", mutator = "some_fn_name")]. annotated fields may have optional visibility modifiers.

    the only semantic validations we do are:
        1. no two attributes with the same name (we'll throw an error)
        2. annotated fields must have the same type assignment as the attribute to which they are mapped (we force this to be true, no error is thrown if the user doesn't do this)
    here are the validations that we assume are handled by the compiler:
        1. no duplicate type aliases
        2. no two events with the same name
        3. the arguments passed to a mutator must be: a mutable reference to the event in which they are used, followed by a value of the same type as the attribute they are intended to modify
        4. mutators must not have return values
        5. mutators must be valid symbols within the current scope
*/

fn process_token_stream(
    token_stream: &TokenStream,
) -> Result<(Vec<Attribute>, Vec<Event>), MeteringMacrosError> {
    // Collect the tokens into a vector.
    let tokens: Vec<TokenTree> = token_stream.into_iter().collect();

    // Maintain an in-memory store of the known attributes.
    let registered_attributes: HashMap<String, String> = HashMap::new(); // attribute_name -> attribute_type
}

// NOTE(c-gamble): We skip validating that the type assigned to the field is the same as the
// type stored for the attribute in the registry to avoid false postivies because compilation
// hasn't happened yet, so we don't know which aliases may be used in the field or mutator
// definition that actually reconcile to the same type.

#[proc_macro]
pub fn initialize_metering(raw_token_stream: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let token_stream = TokenStream::from(raw_token_stream);

    let (attributes, events) = match process_token_stream(&token_stream) {
        Ok(result) => result,
        Err(error) => return generate_compile_error(&error.to_string()),
    };

    let noop_mutator_definition_token_streams: Vec<TokenStream> = attributes
        .iter()
        .map(|attribute| generate_noop_mutator_definition_token_stream(attribute))
        .collect();

    let attribute_definition_token_streams: Vec<TokenStream> = attributes
        .iter()
        .map(|attribute| generate_attribute_definition_token_stream(attribute))
        .collect();

    let event_definition_token_streams: Vec<TokenStream> = events
        .iter()
        .map(|event| generate_event_definition_token_stream(event))
        .collect();

    return proc_macro::TokenStream::from(quote! {
        pub trait MeteringEvent: std::fmt::Debug + std::any::Any + Send + 'static {
            #( #noop_mutator_definition_token_streams )*
        }

        #( #attribute_definition_token_streams )*

        #( #event_definition_token_streams )*

        /// The default receiver registered in the library.
        #[derive(Clone, std::fmt::Debug)]
        pub struct __DefaultReceiver;

        /// The default receiver simply prints out the metering events submitted to it.
        #[async_trait::async_trait]
        impl chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>
            for __DefaultReceiver
        {
            async fn send(
                &self,
                message: Box<dyn MeteringEvent>,
                tracing_context: Option<tracing::Span>,
            ) -> Result<(), chroma_system::ChannelError> {
                if let Some(span) = tracing_context {
                    println!("[metering] span={:?} event={:?}", span, message);
                } else {
                    println!("[metering] event={:?}", message);
                }
                Ok(())
            }
        }

        /// The storage slot for the registered receiver.
        static RECEIVER: once_cell::sync::Lazy<
            parking_lot::Mutex<Box<dyn chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>>>,
        > = once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(Box::new(__DefaultReceiver)));

        /// Allows library users to register their own receivers.
        pub fn register_receiver(
            receiver: Box<
                dyn chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>,
            >,
        ) {
            let mut receiver_slot = RECEIVER.lock();
            *receiver_slot = receiver;
        }

        /// A trait containing a `submit` method to send metering events to the registered receiver.
        #[async_trait::async_trait]
        pub trait SubmitExt: MeteringEvent + Sized + Send {
            async fn submit(self) {
                let maybe_current_span = Some(tracing::Span::current());

                let receiver: Box<
                    dyn chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>,
                > = {
                    let lock = RECEIVER.lock();
                    (*lock).clone()
                };

                let boxed_metering_event: Box<dyn MeteringEvent> = Box::new(self);

                if let Err(error) = receiver.send(boxed_metering_event, maybe_current_span).await {
                    tracing::error!("Unable to send meter event: {error}");
                }
            }
        }

        /// A blanket-impl of the `submit` method for all metering events.
        #[async_trait::async_trait]
        impl<T> SubmitExt for T
        where
            T: MeteringEvent + Send + 'static,
        {
            async fn submit(self) {
                let maybe_current_span = Some(tracing::Span::current());
                let receiver: Box<
                    dyn chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>,
                > = {
                    let lock = RECEIVER.lock();
                    (*lock).clone_box()
                };
                let boxed_metering_event: Box<dyn MeteringEvent> = Box::new(self);
                if let Err(error) = receiver.send(boxed_metering_event, maybe_current_span).await {
                    tracing::error!("Unable to send meter event: {error}");
                }
            }
        }

        thread_local! {
            /// The thread-local event stack in which metering events are stored.
            static EVENT_STACK: std::cell::RefCell<Vec<(std::any::TypeId, Box<dyn MeteringEvent>)>> = std::cell::RefCell::new(Vec::new());
        }

        /// A zero-sized struct used to implement RAII for metering events.
        pub struct MeteringEventGuard;

        /// We implement drop for the guard such that metering events are dropped when they fall out of scope.
        impl Drop for MeteringEventGuard {
            fn drop(&mut self) {
                if let Some(dropped_event) = EVENT_STACK.with(|event_stack| event_stack.borrow_mut().pop())
                {
                    tracing::warn!(
                        "Dropping event because it is now out of scope: {:?}",
                        dropped_event
                    );
                }
            }
        }

        /// Creates a metering event of type `E` and pushes it onto the stack.
        pub fn create<E: MeteringEvent>(metering_event: E) -> MeteringEventGuard {
            let type_id = TypeId::of::<E>();
            let boxed_metering_event: Box<dyn MeteringEvent> =
                Box::new(metering_event);
            EVENT_STACK.with(|event_stack| {
                event_stack
                    .borrow_mut()
                    .push((type_id, boxed_metering_event));
            });
            MeteringEventGuard
        }

        thread_local! {
            /// A thread-local pointer to an empty metering event such that if the stack is empty
            /// method invocations won't fail.
            static BLANK_METERING_EVENT_POINTER: *mut dyn MeteringEvent = {
                let boxed_blank_metering_event = Box::new(BlankMeteringEvent);
                Box::into_raw(boxed_blank_metering_event) as *mut dyn MeteringEvent
            };
        }

        /// A zero-sized metering event to use in case of the stack being empty.
        struct BlankMeteringEvent;

        /// We implement debug so that the metering event can be sent to the default receiver.
        impl std::fmt::Debug for BlankMeteringEvent {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "BlankMeteringEvent")
            }
        }

        /// The blank metering event has no custom mutators, so everything is a no-op.
        impl MeteringEvent for BlankMeteringEvent {}

        /// Returns a pointer to the metering event at the top of the stack.
        pub fn current() -> &'static mut dyn MeteringEvent {
            if let Some(raw_ptr) = EVENT_STACK.with(|event_stack| {
                let mut vec = event_stack.borrow_mut();
                if let Some((_, boxed_metering_event)) = vec.last_mut() {
                    let raw: *mut dyn MeteringEvent =
                        &mut **boxed_metering_event as *mut dyn MeteringEvent;
                    Some(raw)
                } else {
                    None
                }
            }) {
                unsafe { &mut *raw_ptr }
            } else {
                BLANK_METERING_EVENT_POINTER.with(|p| unsafe { &mut *(*p) })
            }
        }

        /// Checks if the top event on the stack is of type `E`. If so, the event is removed from the stack
        /// and returned to the caller. If not, `None` is returned.
        pub fn close<E: MeteringEvent>() -> Option<E> {
            EVENT_STACK.with(|event_stack| {
                let mut vec = event_stack.borrow_mut();
                if let Some((type_id, _boxed_evt)) = vec.last() {
                    if *type_id == std::any::TypeId::of::<E>() {
                        let (_type_id, boxed_any) = vec.pop().unwrap();
                        let raw_evt: *mut dyn MeteringEvent =
                            Box::into_raw(boxed_any);
                        let raw_e: *mut E = raw_evt as *mut E;
                        let boxed_e: Box<E> = unsafe { Box::from_raw(raw_e) };
                        return Some(*boxed_e);
                    }
                }
                None
            })
        }

        /// A trait that allows futures to be metered to pass events between async contexts.
        pub trait MeteredFutureExt: std::future::Future + Sized {
            fn metered(self, _metering_event_guard: MeteringEventGuard) -> MeteredFuture<Self> {
                MeteredFuture { inner: self }
            }
        }

        /// Blanket-impl of the `MeteredFutureExt` trait for futures.
        impl<F: std::future::Future> MeteredFutureExt for F {}

        /// The struct that holds the inner future for metered futures.
        pub struct MeteredFuture<F: std::future::Future> {
            inner: F,
        }

        /// Implementation of the `Future` trait for `MeteredFuture`.
        impl<F: std::future::Future> std::future::Future for MeteredFuture<F> {
            type Output = F::Output;

            fn poll(mut self: std::pin::Pin<&mut Self>, context: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                let inner_future = unsafe {
                    self.as_mut()
                        .map_unchecked_mut(|metered_future| &mut metered_future.inner)
                };
                inner_future.poll(context)
            }
        }

        /// Implementation of `Unpin` for metered future.
        impl<F: std::future::Future + Unpin> Unpin for MeteredFuture<F> {}
    });
}

//////////////////// VALID INPUT ////////////////////
inititalize_metering! {
    #[attribute(name = "my_attribute")]
    type SomeAlias = Option<Vec<Box<u8>>>;

    #[event]
    pub(super) struct MyEvent {
        my_unannotated_field: u8,
        #[field(attribute = "my_attribute", mutator = "my_mutator")]
        my_annotated_field: Option<Vec<Box<u8>>> // it's okay for the last field of a struct to have or not have a comma
    }
}

//////////////////// INVALID INPUT ////////////////////
inititalize_metering! {
    // blocks that are not properly annotated attributes/events are not allowed
    fn my_mutator(event: &mut MyEvent, value: Option<Vec<Box<u8>>>) {
        event.my_annotated_field = value;
    }

    #[attribute(name = "my_attribute")]
    type SomeAlias = Option<Vec<Box<u8>>>;

    #[event]
    pub(super) struct MyEvent {
        my_unannotated_field: u8,
        #[field(attribute = "my_attribute", mutator = "my_mutator")]
        my_annotated_field: Option<Vec<Box<u8>>>,
    }

    // not allowed
    fn some_other_random_fn() {}

    #[attribute(name = "my_other_attribute"] // invalid annotation syntax, not allowed
    pub type SomeAlias = Option<Vec<Box<u8>>>;

    #[event("myevent")] // no args can be passed to event annotation
    struct MyOtherEvent {
        my_unannotated_field: u8; // fields must be separated by commas
        #[field(attribute = "my_attribute")] // not the proper arguments passed to field
        my_annotated_field: Option<Vec<Box<u8>>>,
    }; // this is just invalid rust syntax
}
