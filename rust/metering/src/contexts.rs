use proc_macro2::{Ident, TokenStream};
use quote::quote;
use std::collections::HashMap;

use crate::capabilities::Capability;

#[derive(Debug)]
pub struct Context {
    pub context_name_ident: Ident,
    pub context_capability_id_strings_to_handler_idents: HashMap<String, Ident>,
}

pub fn generate_context_impls(
    context: &Context,
    capability_id_to_capability: &HashMap<String, Capability>,
) -> TokenStream {
    let context_name_ident = &context.context_name_ident;

    let capability_impls = context
        .context_capability_id_strings_to_handler_idents
        .iter()
        .map(|(sub_id, handler_ident)| {
            let sub = capability_id_to_capability.get(sub_id).unwrap();
            let sub_trait_name = &sub.capability_name_ident;
            let sub_method_name = &sub.capability_method_name_ident;
            let sub_method_params = &sub.capability_method_parameters_token_streams;
            let sub_method_param_names = &sub.capability_method_parameter_name_idents;
            quote! {
                impl #sub_trait_name for #context_name_ident {
                    fn #sub_method_name(&self, #( #sub_method_params ),*) {
                        #handler_ident(self, #( #sub_method_param_names ),*);
                    }
                }
            }
        });

    let marker_method_overrides = context
        .context_capability_id_strings_to_handler_idents
        .keys()
        .map(|sub_id| {
            let sub = capability_id_to_capability.get(sub_id).unwrap();
            let marker_method_name = &sub.capability_marker_method_name_ident;
            let sub_trait_name = &sub.capability_name_ident;
            quote! {
                fn #marker_method_name(&self) -> Result<&dyn #sub_trait_name, String> {
                    Ok(self)
                }
            }
        });

    quote! {

        #( #capability_impls )*


        impl MeteringContext for #context_name_ident {
            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }
            #( #marker_method_overrides )*
        }
    }
}

pub fn generate_dispatch_impl(capability: &Capability) -> TokenStream {
    let Capability {
        capability_name_ident,
        capability_method_name_ident,
        capability_marker_method_name_ident,
        capability_method_parameters_token_streams,
        capability_method_parameter_name_idents,
        ..
    } = capability;

    quote! {
        impl #capability_name_ident for dyn MeteringContext {
            fn #capability_method_name_ident(&self, #( #capability_method_parameters_token_streams ),*) {
                if let Ok(sub_impl) = self.#capability_marker_method_name_ident() {
                    sub_impl.#capability_method_name_ident(#( #capability_method_parameter_name_idents ),*);
                }
            }
        }
    }
}
