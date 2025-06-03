use thiserror::Error;

#[derive(Debug, Error)]
pub enum MeteringMacrosError {
    #[error("Attributes must be annotated as `#[attribute(name = \"<your_attribute_name>\"")]
    AttributeArgsError,
    #[error("The `attribute` macro may only be used to annotate statements of the structure: `type <YourAttributeName> = <a valid type>`")]
    AttributeBodyError,
    #[error("Events must not have any arguments in their annotation.")]
    EventArgsError,
    #[error("Events must ... TODO")]
    EventBodyError,
    // #[error("Fields must be annotated as `#[field(attribute = \"<some_registered_attribute>\", mutator = \"<some_valid_mutator_fn>\"")]
    // FieldArgsError,
}
