use std::any::Any;

/// Extracts the panic message from the value returned by `std::panic::catch_unwind`.
pub(crate) fn get_panic_message(value: Box<dyn Any + Send>) -> Option<String> {
    #[allow(clippy::manual_map)]
    if let Some(s) = value.downcast_ref::<&str>() {
        Some(s.to_string())
    } else if let Some(s) = value.downcast_ref::<String>() {
        Some(s.clone())
    } else {
        None
    }
}
