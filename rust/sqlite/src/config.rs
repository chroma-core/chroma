use pyo3::{pyclass, pymethods};

#[derive(Clone)]
#[pyclass]
pub struct SqliteDBConfig {
    // The SQLite database URL
    pub url: String,
    pub hash_type: MigrationHash,
    pub migration_mode: MigrationMode,
}

/// Migration mode for the database
/// - Apply: Apply the migrations
/// - Validate: Validate the applied migrations and ensure none are unappliued
#[derive(Clone, PartialEq)]
#[pyclass(eq, eq_int)]
pub enum MigrationMode {
    Apply,
    Validate,
}

/// The hash function to use for the migration files
/// - SHA256: Use SHA256 hash
/// - MD5: Use MD5 hash
#[derive(Clone, Copy, PartialEq, Debug)]
#[pyclass(eq, eq_int)]
pub enum MigrationHash {
    SHA256,
    MD5,
}

//////////////////////// PyMethods Implementation ////////////////////////

#[pymethods]
impl SqliteDBConfig {
    #[new]
    pub fn py_new(url: String, hash_type: MigrationHash, migration_mode: MigrationMode) -> Self {
        SqliteDBConfig {
            url,
            hash_type,
            migration_mode,
        }
    }
}
