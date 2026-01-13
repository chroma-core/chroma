use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;
use tonic::Status;

/// Macro to define UUID newtype wrappers with standard implementations.
/// Reduces boilerplate for UUID wrapper types.
///
/// Generates:
/// - Struct with standard derives (Copy, Clone, Debug, etc.)
/// - new() method with configurable UUID generation (v4 for random, v7 for time-ordered)
/// - FromStr implementation for parsing from strings
/// - Display implementation for formatting
///
/// # Examples
///
/// ```ignore
/// define_uuid_newtype!(
///     /// My custom UUID type.
///     MyUuid,
///     new_v4
/// );
/// ```
macro_rules! define_uuid_newtype {
    ($(#[$meta:meta])* $name:ident, $uuid_fn:ident) => {
        $(#[$meta])*
        #[derive(
            Copy, Clone, Debug, Default, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize,
        )]
        pub struct $name(pub uuid::Uuid);

        impl $name {
            pub fn new() -> Self {
                $name(uuid::Uuid::$uuid_fn())
            }
        }

        impl std::str::FromStr for $name {
            type Err = uuid::Error;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                uuid::Uuid::parse_str(s).map($name)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

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
pub enum ConversionError {
    #[error("Error decoding protobuf message")]
    DecodeError,
}

impl ChromaError for ConversionError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

impl From<ConversionError> for Status {
    fn from(value: ConversionError) -> Self {
        Status::invalid_argument(value.to_string())
    }
}

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub enum WrappedSerdeJsonError {
    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),
}

impl ChromaError for WrappedSerdeJsonError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}
