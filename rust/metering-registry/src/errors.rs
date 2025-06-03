use thiserror::Error;

#[derive(Debug, Error)]
pub enum MeteringRegistryError {
    #[error("Found attribute with identical name")]
    DuplicateAttributeError,
    #[error("Found existing event with specified type ID")]
    DuplicateEventError,
    #[error("Found existing type ID definition for specified event name")]
    DuplicateEventTypeIdDefinitionError,
    #[error("No event found with specified type ID")]
    EventNotFoundError,
    #[error("`CARGO_MANIFEST_DIR` must be set when building with cargo")]
    CargoManifestError,
    #[error("Registry not found at provided path")]
    RegistryNotFoundError,
    #[error("Error attempting to read registry from disk")]
    RegistryReadError(#[from] std::io::Error),
    #[error("Error attempting to write registry to disk")]
    RegistryWriteError, // impl From `std::io::Error` is inherited from the `RegistryReadError` variant
    #[error("Error attempting to deserialize registry from JSON")]
    RegistryDeserializationError(#[from] serde_json::Error),
}
