use thiserror::Error;

#[derive(Debug, Error)]
pub enum MeteringMacrosError {
    #[error("Attributes must be annotated as `#[attribute(name = \"<your_attribute_name>\"")]
    AttributeArgsError,
    #[error("The `attribute` macro may only be used to annotate statements of the structure: `type <YourAttributeName> = <a valid type>`")]
    AttributeBodyError,
    #[error("No attribute exists with the specified name: {0}")]
    AttributeNotFoundError(String),
    #[error("Events must not have any arguments in their annotation.")]
    EventArgsError,
    #[error("Events must ... TODO")]
    EventBodyError,
    #[error("Annotated field {0} must have an attribute.")]
    AnnotatedFieldMissingAttributeError(String),
    #[error("Annotated field {0} must have a mutator.")]
    AnnotatedFieldMissingMutatorError(String),
    // #[error("Fields must be annotated as `#[field(attribute = \"<some_registered_attribute>\", mutator = \"<some_valid_mutator_fn>\"")]
    // FieldArgsError,
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
