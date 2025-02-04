mod bindings;
mod compaction_manager;

use bindings::Bindings;
use pyo3::prelude::*;

#[pymodule]
fn rust_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Bindings>()?;
    Ok(())
}
