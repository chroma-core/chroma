pub mod panic;

pub use panic::*;

pub mod guard;

pub use guard::*;

use std::time::Duration;

/// Convert a Duration to fractional milliseconds.
pub fn duration_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

const DEFAULT_THREAD_STACK_SIZE_BYTES: usize = 16 * 1024 * 1024;

/// Returns the configured stack size for Rust worker threads.
///
/// `CHROMA_THREAD_STACK_SIZE_BYTES` takes precedence. If it is unset, `RUST_MIN_STACK`
/// is used. Invalid values fall back to 16MiB.
pub fn thread_stack_size_bytes() -> usize {
    std::env::var("CHROMA_THREAD_STACK_SIZE_BYTES")
        .ok()
        .or_else(|| std::env::var("RUST_MIN_STACK").ok())
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_THREAD_STACK_SIZE_BYTES)
}

#[cfg(test)]
mod tests {
    use super::thread_stack_size_bytes;

    #[test]
    fn chroma_thread_stack_size_overrides_rust_min_stack() {
        unsafe {
            std::env::set_var("RUST_MIN_STACK", "8388608");
            std::env::set_var("CHROMA_THREAD_STACK_SIZE_BYTES", "16777216");
        }
        assert_eq!(thread_stack_size_bytes(), 16 * 1024 * 1024);
        unsafe {
            std::env::remove_var("CHROMA_THREAD_STACK_SIZE_BYTES");
            std::env::remove_var("RUST_MIN_STACK");
        }
    }

    #[test]
    fn falls_back_to_rust_min_stack() {
        unsafe {
            std::env::remove_var("CHROMA_THREAD_STACK_SIZE_BYTES");
            std::env::set_var("RUST_MIN_STACK", "4194304");
        }
        assert_eq!(thread_stack_size_bytes(), 4 * 1024 * 1024);
        unsafe {
            std::env::remove_var("RUST_MIN_STACK");
        }
    }
}
