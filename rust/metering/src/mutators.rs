use proc_macro2::TokenStream;
use quote::quote;

use crate::attributes::Attribute;

/// Generates a no-op mutator function for the `MeteringEvent` trait.
pub fn generate_noop_mutator_definition_token_stream(attribute: &Attribute) -> TokenStream {
    let Attribute {
        foreign_macro_token_streams: _foreign_macro_token_streams,
        maybe_visibility_modifier_token_stream: _maybe_maybe_visibility_modifier_token_stream,
        attribute_type_alias_ident: _attribute_type_alias_ident,
        attribute_name_string: _attribute_name_string,
        attribute_name_ident,
        attribute_type_string: _attribute_type_string,
        attribute_type_token_stream,
    } = attribute;

    let noop_mutator_definition_token_stream = quote! {
        fn #attribute_name_ident(&mut self, _: #attribute_type_token_stream) {}
    };

    noop_mutator_definition_token_stream
}
