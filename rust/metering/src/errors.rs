use std::fmt;

use proc_macro2::TokenStream;

/// A compile-time error generated while attempting to parse the tokens provided to the
/// [`crate::initialize_metering`] macro.
#[derive(Debug)]
pub enum MeteringMacrosError {
    SynError(syn::Error),
    // Capability-related errors
    InvalidCapabilityAttribute(proc_macro2::Span),
    CapabilityNotPublic(proc_macro2::Span),
    CapabilityMethodCount(usize, proc_macro2::Span),
    CapabilityItemNotAMethod(proc_macro2::Span),
    CapabilityMethodMissingSelf(proc_macro2::Span),
    CapabilityMethodInvalidArg(proc_macro2::Span),
    // Context-related errors
    InvalidContextAttribute(proc_macro2::Span),
    ContextMissingCapabilities(proc_macro2::Span),
    ContextMissingHandlers(proc_macro2::Span),
    ContextMismatchedHandlers(proc_macro2::Span),
}

/// Implements display for [`crate::errors::MeteringMacrosError`].
impl fmt::Display for MeteringMacrosError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MeteringMacrosError::SynError(error) => write!(formatter, "{}", error),

            MeteringMacrosError::InvalidCapabilityAttribute(_) => {
                write!(formatter, "The `capability` attribute should have no arguments, like `#[capability]`, not `#[capability(...)]`")
            }
            MeteringMacrosError::CapabilityNotPublic(_) => {
                write!(formatter, "A capability trait must be declared as `pub`")
            }
            MeteringMacrosError::CapabilityMethodCount(count, _) => {
                write!(
                    formatter,
                    "A capability trait must have exactly one method, but this one has {}",
                    count
                )
            }
            MeteringMacrosError::CapabilityItemNotAMethod(_) => {
                write!(
                    formatter,
                    "The item inside a capability trait must be a method"
                )
            }
            MeteringMacrosError::CapabilityMethodMissingSelf(_) => {
                write!(
                    formatter,
                    "The first parameter of a capability method must be `&self`"
                )
            }
            MeteringMacrosError::CapabilityMethodInvalidArg(_) => {
                write!(
                    formatter,
                    "A capability method argument could not be parsed correctly"
                )
            }

            MeteringMacrosError::InvalidContextAttribute(_) => {
                write!(
                    formatter,
                    "The `context` attribute must be a list, like `#[context(...)]`"
                )
            }
            MeteringMacrosError::ContextMissingCapabilities(_) => {
                write!(
                    formatter,
                    "The `context` attribute is missing the `capabilities` argument"
                )
            }
            MeteringMacrosError::ContextMissingHandlers(_) => {
                write!(
                    formatter,
                    "The `context` attribute is missing the `handlers` argument"
                )
            }
            MeteringMacrosError::ContextMismatchedHandlers(_) => {
                write!(formatter, "The `capabilities` and `handlers` arrays must have the same number of elements")
            }
        }
    }
}

/// Generates a compile-time error for a [`crate::errors::MeteringMacrosError`].
impl MeteringMacrosError {
    pub fn to_compile_error(&self) -> TokenStream {
        let error_message = self.to_string();
        let span = match self {
            MeteringMacrosError::SynError(error) => return error.to_compile_error(),
            MeteringMacrosError::InvalidCapabilityAttribute(span)
            | MeteringMacrosError::CapabilityNotPublic(span)
            | MeteringMacrosError::CapabilityMethodCount(_, span)
            | MeteringMacrosError::CapabilityItemNotAMethod(span)
            | MeteringMacrosError::CapabilityMethodMissingSelf(span)
            | MeteringMacrosError::CapabilityMethodInvalidArg(span)
            | MeteringMacrosError::InvalidContextAttribute(span)
            | MeteringMacrosError::ContextMissingCapabilities(span)
            | MeteringMacrosError::ContextMissingHandlers(span)
            | MeteringMacrosError::ContextMismatchedHandlers(span) => *span,
        };
        quote::quote_spanned! {span=>
            compile_error!(#error_message);
        }
    }
}
