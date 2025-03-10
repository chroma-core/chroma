mod bindings;
mod errors;

use bindings::{Bindings, PythonBindingsConfig};
use pyo3::prelude::*;

//////////////////////// Config Imports ////////////////////////
use chroma_sqlite::config::{MigrationHash, MigrationMode, SqliteDBConfig};

#[pymodule]
fn chromadb_rust_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Bindings>()?;

    // TODO: move this into a module hierarchy

    // Root config
    m.add_class::<PythonBindingsConfig>()?;

    // Sqlite config classes
    m.add_class::<SqliteDBConfig>()?;
    m.add_class::<MigrationMode>()?;
    m.add_class::<MigrationHash>()?;

    // Log config classes
    // TODO
    Ok(())
}
