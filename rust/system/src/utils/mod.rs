pub mod panic;

pub use panic::*;

pub mod guard;

pub use guard::*;

use std::time::Duration;

/// Convert a Duration to fractional milliseconds.
pub fn duration_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}
