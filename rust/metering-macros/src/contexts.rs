use proc_macro2::{Ident, TokenStream};
use quote::quote;
use std::collections::HashMap;

use crate::capabilities::Capability;

/// Represents a user-defined context. A context is set of fields that, by way of handlers,
/// can be modified whenever a capability that is registered for the context is called.
#[derive(Debug)]
pub struct Context {
    pub context_name_ident: Ident,
    pub context_capability_name_idents_to_handler_idents: HashMap<Ident, Ident>,
}

/// Generates the implementations of a context's capabilities and the corresponding marker
/// method overrides for the `MeteringContext` implementation on that context.
/// e.g.,
/// ```ignore
/// impl SomeCapability for SomeContext {
///     fn some_capability(&self, some_arg: SomeType) {
///         some_handler(self, some_arg);
///     }
/// }
///
/// impl MeteringContext for SomeContext {
///     fn as_any(&self) -> &dyn ::std::any::Any {
///         self    
///     }
///
///     fn __marker_some_capability(&self) -> Result<&dyn SomeCapability, String> {
///         Ok(self)   
///     }
/// }
/// ```
pub fn generate_capability_implementations_for_context(
    context: &Context,
    capability_name_to_capability: &HashMap<Ident, Capability>,
) -> TokenStream {
    let context_name_ident = &context.context_name_ident;

    let capability_implementations_for_context = context
        .context_capability_name_idents_to_handler_idents
        .iter()
        .map(|(capability_name_ident, handler_ident)| {
            let Capability {
                capability_method_name_ident,
                capability_method_parameter_token_streams,
                capability_method_parameter_name_idents,
                ..
            } = capability_name_to_capability
            .get(capability_name_ident)
            .unwrap_or_else(|| panic!("No capability found with ID {}", capability_name_ident));

            quote! {
                impl #capability_name_ident for #context_name_ident {
                    fn #capability_method_name_ident(&self, #( #capability_method_parameter_token_streams ),*) {
                        #handler_ident(self, #( #capability_method_parameter_name_idents ),*);
                    }
                }
            }
        });

    let capability_marker_method_overrides_for_context = context
        .context_capability_name_idents_to_handler_idents
        .keys()
        .map(|capability_name_ident| {
            let Capability {
                capability_name_ident,
                capability_marker_method_name_ident,
                ..
            } = capability_name_to_capability
            .get(capability_name_ident)
            .unwrap_or_else(|| panic!("No capability found with ID {}", capability_name_ident));

            quote! {
                fn #capability_marker_method_name_ident(&self) -> Result<&dyn #capability_name_ident, String> {
                    Ok(self)
                }
            }
        });

    quote! {

        #( #capability_implementations_for_context )*

        impl MeteringContext for #context_name_ident {
            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }
            #( #capability_marker_method_overrides_for_context )*
        }
    }
}

/// Generates the implementation of a capability for the trait object of the base context.
/// e.g.,
/// ```ignore
/// impl SomeCapability for dyn MeteringContext {
///     fn some_capability(&self, some_arg: SomeType) {
///         if let Ok(capability_marker_for_context) = self.__marker_some_capability() {
///             capability_marker_for_context.some_capability(some_arg);
///         }
///     }
/// }
/// ```
pub fn generate_capability_implementation_for_base_context(capability: &Capability) -> TokenStream {
    let Capability {
        capability_name_ident,
        capability_method_name_ident,
        capability_marker_method_name_ident,
        capability_method_parameter_token_streams,
        capability_method_parameter_name_idents,
        ..
    } = capability;

    quote! {
        impl #capability_name_ident for dyn MeteringContext {
            fn #capability_method_name_ident(&self, #( #capability_method_parameter_token_streams ),*) {
                if let Ok(capability_marker_for_context) = self.#capability_marker_method_name_ident() {
                    capability_marker_for_context.#capability_method_name_ident(#( #capability_method_parameter_name_idents ),*);
                }
            }
        }
    }
}
