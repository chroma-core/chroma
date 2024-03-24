use crate::errors::{ChromaError, ErrorCodes};
use num_bigint::BigInt;
use thiserror::Error;

/// A macro for easily implementing match arms for a base error type with common errors.
/// Other types can wrap it and still implement the ChromaError trait
/// without boilerplate.
macro_rules! impl_base_convert_error {
    ($err:ty, { $($variant:pat => $action:expr),* $(,)? }) => {
        impl ChromaError for $err {
            fn code(&self) -> ErrorCodes {
                match self {
                    Self::DecodeError(inner) => inner.code(),
                    // Handle custom variants
                    $( $variant => $action, )*
                }
            }
        }
    };
}

#[derive(Error, Debug)]
pub(crate) enum ConversionError {
    #[error("Error decoding protobuf message")]
    DecodeError,
}

impl ChromaError for ConversionError {
    fn code(&self) -> crate::errors::ErrorCodes {
        match self {
            ConversionError::DecodeError => ErrorCodes::Internal,
        }
    }
}

pub(crate) type SeqId = BigInt;
