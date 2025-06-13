use proc_macro2::{Ident, TokenStream};
use quote::quote;

/// Represents a user-defined capability for a context. A capability means that a context
/// is able to handle the receipt of a specific set of information.
#[derive(Clone, Debug)]
pub struct Capability {
    pub capability_id_string: String,
    pub capability_name_ident: Ident,
    pub capability_marker_method_name_ident: Ident,
    pub capability_method_name_ident: Ident,
    pub capability_method_parameter_token_streams: Vec<TokenStream>,
    pub capability_method_parameter_name_idents: Vec<Ident>,
}

/// Generates a marker method for a capability.
/// e.g.,   
/// ```ignore
/// fn __marker_some_capability(&self) -> Result<&dyn SomeCapability, String> {
///     Err(format!("This context does not support the capability with ID '{}'", "some_capability"))
/// }
/// ```
pub fn generate_capability_marker_method_definition(capability: &Capability) -> TokenStream {
    let Capability {
        capability_name_ident,
        capability_marker_method_name_ident,
        capability_id_string,
        ..
    } = capability;

    quote! {
        fn #capability_marker_method_name_ident(&self) -> Result<&dyn #capability_name_ident, String> {
            Err(format!("This context does not support the capability with ID '{}'", #capability_id_string))
        }
    }
}
