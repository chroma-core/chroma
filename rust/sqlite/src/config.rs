#[derive(Clone)]
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
pub enum MigrationMode {
    Apply,
    Validate,
}

/// The hash function to use for the migration files
/// - SHA256: Use SHA256 hash
/// - MD5: Use MD5 hash
#[derive(Clone)]
pub enum MigrationHash {
    SHA256,
    MD5,
}
