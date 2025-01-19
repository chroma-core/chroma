use std::time::SystemTime;

use pyo3::{exceptions::PyOSError, prelude::*};

/// Returns the current eopch time in ns
#[pyfunction]
fn heartbeat() -> PyResult<u128> {
    let duration_since_epoch =
        match std::time::SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => duration,
            Err(_) => return Err(PyOSError::new_err("Failed to get system time")),
        };
    Ok(duration_since_epoch.as_nanos())
}

/// A Python module implemented in Rust.
#[pymodule]
fn rust_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(heartbeat, m)?)?;
    Ok(())
}
