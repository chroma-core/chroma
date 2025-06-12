use proc_macro2::{Ident, TokenStream};
use quote::quote;
use std::collections::HashMap;

use crate::subscriptions::Subscription;

/// Represents a user-defined metering context.
pub struct Context {
    pub context_foreign_macro_token_streams: Vec<TokenStream>,
    pub context_subscription_id_strings_to_handler_idents: HashMap<String, Ident>,
    pub maybe_context_visibility_modifier_token_stream: Option<TokenStream>,
    pub context_name_ident: Ident,
    pub context_field_token_streams: Vec<TokenStream>,
}

/// Generates the output tokens for an implementation of a given subscription on a context.
fn generate_context_subscription_implementation_token_stream(
    context_name_ident: &Ident,
    handler_ident: &Ident,
    subscription: &Subscription,
) -> TokenStream {
    let Subscription {
        subscription_id_string: _subscription_id_string,
        subscription_foreign_macro_token_streams: _subscription_foreign_macro_token_streams,
        maybe_subscription_visibility_modifier_token_stream:
            _maybe_subscription_visibility_modifier_token_stream,
        subscription_name_ident,
        subscription_method_foreign_macro_token_streams:
            _subscription_method_foreign_macro_token_streams,
        subscription_method_name_ident,
        subscription_method_parameter_name_idents,
        subscription_method_parameters_token_streams,
    } = subscription;

    quote! {
        impl #subscription_name_ident for #context_name_ident {
            fn #subscription_method_name_ident(&self, #( #subscription_method_parameters_token_streams )*) {
                #handler_ident(self, #( #subscription_method_parameter_name_idents )*);
            }
        }
    }
}

/// Generates the output tokens required for a user-defined metering context.
pub fn generate_context_definition_token_stream(
    context: &Context,
    subscription_ids_to_subscriptions: &HashMap<String, Subscription>,
) -> TokenStream {
    let Context {
        context_foreign_macro_token_streams,
        context_subscription_id_strings_to_handler_idents,
        maybe_context_visibility_modifier_token_stream,
        context_name_ident,
        context_field_token_streams,
    } = context;

    let context_definition_token_stream =
        if maybe_context_visibility_modifier_token_stream.is_some() {
            quote! {
                #( #context_foreign_macro_token_streams )*
                #maybe_context_visibility_modifier_token_stream struct #context_name_ident {
                    #( #context_field_token_streams )*
                }
            }
        } else {
            quote! {
                #( #context_foreign_macro_token_streams )*
                struct #context_name_ident {
                    #( #context_field_token_streams )*
                }
            }
        };

    let context_subscription_implementation_token_streams: Vec<TokenStream> =
        context_subscription_id_strings_to_handler_idents
            .iter()
            .map(|(subscription_id, handler_ident)| {
                generate_context_subscription_implementation_token_stream(
                    context_name_ident,
                    handler_ident,
                    subscription_ids_to_subscriptions
                        .get(subscription_id)
                        .expect(&format!(
                            "No subscription found with ID {:?}",
                            subscription_id
                        )),
                )
            })
            .collect();

    quote! {
        #context_definition_token_stream

        impl MeteringContext for #context_name_ident {
            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }
        }

        #( #context_subscription_implementation_token_streams )*
    }
}
