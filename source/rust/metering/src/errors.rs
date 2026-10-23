use thiserror::Error;

#[derive(Debug, Error)]
pub enum MeteringError {
    #[error("Unable to send meter event: {0}")]
    SendError(String),
}
