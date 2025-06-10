use thiserror::Error;

/// A compilation error that occurs in the metering macro invocation
#[derive(Debug, Error)]
pub enum MeteringMacrosError {
    #[error("Failed to parse tokens provided in macro invocation: {0}")]
    ParseError(String),
}
