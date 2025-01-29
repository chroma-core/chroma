use std::path::PathBuf;

#[derive(Clone)]
pub(crate) struct SqliteDBConfig {
    // The SQLite database URL
    pub(crate) url: String,
    // TODO: change this to something bundled with binary
    // The root directory where all migration files are stored
    // The migration files are stored in subdirectories of this directory
    pub(crate) migrations_root_dir: PathBuf,
    pub(crate) hash_type: MigrationHash,
    pub(crate) migration_mode: MigrationMode,
}

/// Migration mode for the database
/// - Apply: Apply the migrations
/// - Validate: Validate the applied migrations and ensure none are unappliued
#[derive(Clone, PartialEq)]
pub(crate) enum MigrationMode {
    Apply,
    Validate,
}

/// The hash function to use for the migration files
/// - SHA256: Use SHA256 hash
/// - MD5: Use MD5 hash
#[derive(Clone)]
pub(crate) enum MigrationHash {
    SHA256,
    MD5,
}
