use crate::db::{SqliteCreationError, SqliteDb};
use async_trait::async_trait;
use chroma_config::{
    registry::{Injectable, Registry},
    Configurable,
};
use chroma_error::ChromaError;
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};

#[derive(Serialize, Deserialize, Clone)]
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
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[pyclass(eq, eq_int)]
pub enum MigrationMode {
    Apply,
    Validate,
}

/// The hash function to use for the migration files
/// - SHA256: Use SHA256 hash
/// - MD5: Use MD5 hash
#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
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

//////////////////////// Configurable Implementation ////////////////////////

impl Injectable for SqliteDb {}

#[async_trait]
impl Configurable<SqliteDBConfig> for SqliteDb {
    async fn try_from_config(
        config: &SqliteDBConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        // TODO: copy all other pragmas from python and add basic tests
        // TODO: make this file path handling more robust
        let filename = config.url.trim_end_matches('/').to_string() + "/chroma.sqlite3";
        let options = SqliteConnectOptions::new()
            .filename(filename)
            // Due to a bug in the python code, foreign_keys is turned off
            // The python code enabled it in a transaction, however,
            // https://www.sqlite.org/pragma.html states that foreign_keys
            // is a no-op in a transaction. In order to be able to run our migrations
            // we turn it off
            .pragma("foreign_keys", "OFF")
            .pragma("case_sensitive_like", "ON")
            .create_if_missing(true);
        let conn = SqlitePool::connect_with(options)
            .await
            .map_err(|e| Box::new(SqliteCreationError::SqlxError(e)) as Box<dyn ChromaError>)?;

        let db = SqliteDb::new(conn, config.hash_type);

        db.initialize_migrations_table().await?;
        match config.migration_mode {
            MigrationMode::Apply => {
                db.apply_all_migration()
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
            }
            MigrationMode::Validate => {
                db.validate_all_migrations()
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
            }
        }

        registry.register(db.clone());
        Ok(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_utils::new_test_db_path;
    use chroma_config::registry::Registry;

    #[tokio::test]
    async fn test_sqlite_db_config_registry() {
        let config = SqliteDBConfig {
            url: new_test_db_path(),
            hash_type: MigrationHash::SHA256,
            migration_mode: MigrationMode::Apply,
        };

        let registry = Registry::new();
        let _db = SqliteDb::try_from_config(&config, &registry).await.unwrap();
        let _retrieved_db = registry
            .get::<SqliteDb>()
            .expect("To be able to retrieve the db");
    }
}
