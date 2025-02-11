use pyo3::{pyclass, pymethods};

#[derive(Clone)]
#[pyclass]
pub struct SqliteDBConfig {
    pub hash_type: MigrationHash,
    pub migration_mode: MigrationMode,
    // The SQLite database URL
    // If unspecified, then the database is in memory only
    pub url: Option<String>,
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
    #[pyo3(signature = (hash_type, migration_mode, url=None))]
    pub fn py_new(
        hash_type: MigrationHash,
        migration_mode: MigrationMode,
        url: Option<String>,
    ) -> Self {
        SqliteDBConfig {
            hash_type,
            migration_mode,
            url,
        }
    }
}
