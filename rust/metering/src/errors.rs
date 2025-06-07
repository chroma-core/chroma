use thiserror::Error;

#[derive(Debug, Error)]
pub enum MeteringMacrosError {
    #[error("Failed to parse tokens provided in macro invocation: {0}")]
    ParseError(String),
}
