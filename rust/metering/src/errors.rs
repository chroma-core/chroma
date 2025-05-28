use thiserror::Error;

/// Errors for Chroma's metering crate
#[derive(Error, Debug)]
pub enum MeteringError {
    #[error("Meter event handler is already initialized")]
    ReceiverAlreadyInitialized,

    #[error("Unable to send meter event: {0}")]
    SubmitError(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Multiple Arc clones exist; cannot unwrap MeterEvent")]
    ArcCloneError,

    #[error("Mutex poisoned when accessing MeterEvent")]
    MutexPoisonedError,
}
