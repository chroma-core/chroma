#![recursion_limit = "256"]
mod bindings;
mod errors;
use std::fs::File;

use simplelog::{CombinedLogger, Config, LevelFilter, WriteLogger};

use crate::bindings::cli;
use bindings::{Bindings, PythonBindingsConfig};
use pyo3::prelude::*;

//////////////////////// Config Imports ////////////////////////
use chroma_sqlite::config::{MigrationHash, MigrationMode, SqliteDBConfig};

#[pymodule]
fn chromadb_rust_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Get file name from PYTEST_XDIST_WORKER env var
    let worker_id = std::env::var("PYTEST_XDIST_WORKER").unwrap_or_else(|_| "unknown".to_string());
    let log_file_name = format!("chroma_rust_bindings_{}.log", worker_id);

    CombinedLogger::init(vec![
        // Only write to a file called app.log
        WriteLogger::new(
            LevelFilter::Info, // global filter
            Config::default(), // timestamp, level, target, etc.
            File::create(log_file_name).expect("Failed to create log file"),
        ),
    ])
    .unwrap();

    // pyo3_log::Logger::new(m.py(), pyo3_log::Caching::Nothing)?
    //     .filter(log::LevelFilter::Info)
    //     .install()
    //     .expect("Someone installed a logger before us :-(");

    m.add_class::<Bindings>()?;

    // TODO: move this into a module hierarchy

    // Root config
    m.add_class::<PythonBindingsConfig>()?;

    // Sqlite config classes
    m.add_class::<SqliteDBConfig>()?;
    m.add_class::<MigrationMode>()?;
    m.add_class::<MigrationHash>()?;

    m.add_function(wrap_pyfunction!(cli, m)?)?;

    // Log config classes
    // TODO
    Ok(())
}
