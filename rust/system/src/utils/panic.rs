use chroma_error::ChromaError;
use std::any::Any;
use thiserror::Error;

#[derive(Error)]
#[error("Panic {:?}", get_panic_message(.0))]
pub struct PanicError(Box<dyn std::any::Any + Send>);

impl std::fmt::Debug for PanicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Panic: {:?}",
            get_panic_message(&self.0)
                .unwrap_or("panic does not have displayable message".to_string())
        )
    }
}

impl PanicError {
    pub(crate) fn new(panic_value: Box<dyn std::any::Any + Send>) -> Self {
        PanicError(panic_value)
    }
}

impl ChromaError for PanicError {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::Internal
    }
}

/// Extracts the panic message from the value returned by `std::panic::catch_unwind`.
pub(crate) fn get_panic_message(value: &Box<dyn Any + Send>) -> Option<String> {
    #[allow(clippy::manual_map)]
    if let Some(s) = value.downcast_ref::<&str>() {
        Some(s.to_string())
    } else if let Some(s) = value.downcast_ref::<String>() {
        Some(s.clone())
    } else {
        None
    }
}
