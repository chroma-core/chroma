use proc_macro2::{Ident, TokenStream};
use quote::quote;

/// Represents a user-defined metering subscription.
#[derive(Clone)]
pub struct Subscription {
    pub subscription_id_string: String,
    pub subscription_foreign_macro_token_streams: Vec<TokenStream>,
    pub maybe_subscription_visibility_modifier_token_stream: Option<TokenStream>,
    pub subscription_name_ident: Ident,
    // NOTE(c-gamble) These fields are intentionally *not* a sub-struct to more rigidly
    // enforce the one-method rule for subscriptions.
    pub subscription_method_foreign_macro_token_streams: Vec<TokenStream>,
    pub subscription_method_name_ident: Ident,
    pub subscription_method_parameter_name_idents: Vec<Ident>,
    pub subscription_method_parameters_token_streams: Vec<TokenStream>,
}

/// Generates the output token stream for a user-defined subscription.
pub fn generate_subscription_definition_token_stream(subscription: &Subscription) -> TokenStream {
    let Subscription {
        subscription_id_string: _subscription_id_string,
        subscription_foreign_macro_token_streams,
        maybe_subscription_visibility_modifier_token_stream,
        subscription_name_ident,
        subscription_method_foreign_macro_token_streams,
        subscription_method_name_ident,
        subscription_method_parameter_name_idents: _subscription_method_parameter_name_idents,
        subscription_method_parameters_token_streams,
    } = subscription;

    if maybe_subscription_visibility_modifier_token_stream.is_some() {
        quote! {
            #( #subscription_foreign_macro_token_streams )*
            #maybe_subscription_visibility_modifier_token_stream trait #subscription_name_ident {
                #( #subscription_method_foreign_macro_token_streams )*
                fn #subscription_method_name_ident(&self, #( #subscription_method_parameters_token_streams )*);
            }

            impl #subscription_name_ident for MeteringContext {
                fn #subscription_method_name_ident(&self, #( #subscription_method_parameters_token_streams )*) {}
            }
        }
    } else {
        quote! {
            #( #subscription_foreign_macro_token_streams )*
            trait #subscription_name_ident {
                #( #subscription_method_foreign_macro_token_streams )*
                fn #subscription_method_name_ident(&self, #( #subscription_method_parameters_token_streams )*);
            }

            impl #subscription_name_ident for MeteringContext {
                fn #subscription_method_name_ident(&self, #( #subscription_method_parameters_token_streams )*) {}
            }
        }
    }
}
