use std::fmt;

/// Errors for the metering library.
#[derive(Debug)]
pub enum MeteringError {
    /// Attempted to initialize the global receiver more than once.
    ReceiverAlreadyInitialized,
    /// No global receiver configured when submitting an event.
    ReceiverNotInitialized,
    /// Failed to lock a mutex (event stack or event data).
    MutexLockError(String),
    /// Failed to unwrap an Arc containing an event (other references exist).
    ArcUnwrapError,
}

impl fmt::Display for MeteringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MeteringError::ReceiverAlreadyInitialized => {
                write!(f, "Event receiver has already been initialized")
            }
            MeteringError::ReceiverNotInitialized => write!(f, "No event receiver configured"),
            MeteringError::MutexLockError(details) => write!(f, "Mutex lock failed: {}", details),
            MeteringError::ArcUnwrapError => write!(f, "Failed to unwrap Arc<MeterEvent>"),
        }
    }
}

impl std::error::Error for MeteringError {}
