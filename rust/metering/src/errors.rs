use std::fmt;

use proc_macro2::TokenStream;

#[derive(Debug)]
pub enum MeteringMacrosError {
    SynError(syn::Error),
    // Subscription-specific errors
    InvalidSubscriptionAttribute(proc_macro2::Span),
    MissingSubscriptionId(proc_macro2::Span),
    InvalidSubscriptionIdType(proc_macro2::Span),
    SubscriptionMethodCount(usize, proc_macro2::Span),
    SubscriptionItemNotAMethod(proc_macro2::Span),
    SubscriptionMethodMissingSelf(proc_macro2::Span),
    SubscriptionMethodInvalidArg(proc_macro2::Span),
    // Context-specific errors
    InvalidContextAttribute(proc_macro2::Span),
    ContextMissingSubscriptions(proc_macro2::Span),
    ContextMissingHandlers(proc_macro2::Span),
    ContextAttributeValueNotArray(proc_macro2::Span),
    ContextAttributeInvalidValue(proc_macro2::Span),
    ContextMismatchedHandlers(proc_macro2::Span),
}

impl fmt::Display for MeteringMacrosError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MeteringMacrosError::SynError(e) => write!(f, "{}", e),
            // Subscription errors
            MeteringMacrosError::InvalidSubscriptionAttribute(_) => {
                write!(f, "The `subscription` attribute must be a list, like `#[subscription(id = \"...\")]`")
            }
            MeteringMacrosError::MissingSubscriptionId(_) => {
                write!(
                    f,
                    "The `subscription` attribute is missing the `id` argument"
                )
            }
            MeteringMacrosError::InvalidSubscriptionIdType(_) => {
                write!(
                    f,
                    "The `id` argument for a subscription must be a string literal"
                )
            }
            MeteringMacrosError::SubscriptionMethodCount(count, _) => {
                write!(
                    f,
                    "A subscription trait must have exactly one method, but this one has {}",
                    count
                )
            }
            MeteringMacrosError::SubscriptionItemNotAMethod(_) => {
                write!(f, "The item inside a subscription trait must be a method")
            }
            MeteringMacrosError::SubscriptionMethodMissingSelf(_) => {
                write!(
                    f,
                    "The first parameter of a subscription method must be `&self`"
                )
            }
            MeteringMacrosError::SubscriptionMethodInvalidArg(_) => {
                write!(
                    f,
                    "A subscription method argument could not be parsed correctly"
                )
            }
            // Context errors
            MeteringMacrosError::InvalidContextAttribute(_) => {
                write!(
                    f,
                    "The `context` attribute must be a list, like `#[context(...)]`"
                )
            }
            MeteringMacrosError::ContextMissingSubscriptions(_) => {
                write!(
                    f,
                    "The `context` attribute is missing the `subscriptions` argument"
                )
            }
            MeteringMacrosError::ContextMissingHandlers(_) => {
                write!(
                    f,
                    "The `context` attribute is missing the `handlers` argument"
                )
            }
            MeteringMacrosError::ContextAttributeValueNotArray(_) => {
                write!(
                    f,
                    "The `subscriptions` and `handlers` arguments must be arrays (`[...]`)"
                )
            }
            MeteringMacrosError::ContextAttributeInvalidValue(_) => {
                write!(f, "A value inside the `subscriptions` or `handlers` array is not a valid type (string literal or identifier)")
            }
            MeteringMacrosError::ContextMismatchedHandlers(_) => {
                write!(f, "The `subscriptions` and `handlers` arrays must have the same number of elements")
            }
        }
    }
}

impl MeteringMacrosError {
    /// Converts the custom error into a `compile_error!` token stream.
    pub fn to_compile_error(&self) -> TokenStream {
        let message = self.to_string();
        let span = match self {
            MeteringMacrosError::SynError(e) => return e.to_compile_error(),
            MeteringMacrosError::InvalidSubscriptionAttribute(s) => *s,
            MeteringMacrosError::MissingSubscriptionId(s) => *s,
            MeteringMacrosError::InvalidSubscriptionIdType(s) => *s,
            MeteringMacrosError::SubscriptionMethodCount(_, s) => *s,
            MeteringMacrosError::SubscriptionItemNotAMethod(s) => *s,
            MeteringMacrosError::SubscriptionMethodMissingSelf(s) => *s,
            MeteringMacrosError::SubscriptionMethodInvalidArg(s) => *s,
            MeteringMacrosError::InvalidContextAttribute(s) => *s,
            MeteringMacrosError::ContextMissingSubscriptions(s) => *s,
            MeteringMacrosError::ContextMissingHandlers(s) => *s,
            MeteringMacrosError::ContextAttributeValueNotArray(s) => *s,
            MeteringMacrosError::ContextAttributeInvalidValue(s) => *s,
            MeteringMacrosError::ContextMismatchedHandlers(s) => *s,
        };
        quote::quote_spanned! {span=>
            compile_error!(#message);
        }
    }
}
