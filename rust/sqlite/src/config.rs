use crate::db::{SqliteCreationError, SqliteDb};
use async_trait::async_trait;
use chroma_config::{
    registry::{Injectable, Registry},
    Configurable,
};
use chroma_error::ChromaError;
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

#[derive(Serialize, Deserialize, Clone, Default)]
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
#[derive(Clone, PartialEq, Serialize, Deserialize, Default)]
#[pyclass(eq, eq_int)]
pub enum MigrationMode {
    #[default]
    Apply,
    Validate,
}

/// The hash function to use for the migration files
/// - SHA256: Use SHA256 hash
/// - MD5: Use MD5 hash
#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize, Default)]
#[pyclass(eq, eq_int)]
pub enum MigrationHash {
    #[default]
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

//////////////////////// Configurable Implementation ////////////////////////

impl Injectable for SqliteDb {}

#[async_trait]
impl Configurable<SqliteDBConfig> for SqliteDb {
    async fn try_from_config(
        config: &SqliteDBConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        // TODO: copy all other pragmas from python and add basic tests
        let conn_options = SqliteConnectOptions::new()
            // Due to a bug in the python code, foreign_keys is turned off
            // The python code enabled it in a transaction, however,
            // https://www.sqlite.org/pragma.html states that foreign_keys
            // is a no-op in a transaction. In order to be able to run our migrations
            // we turn it off
            .pragma("foreign_keys", "OFF")
            .pragma("case_sensitive_like", "ON");
        let conn = if let Some(url) = &config.url {
            SqlitePoolOptions::new()
                .connect_with(conn_options.filename(url).create_if_missing(true))
                .await
                .map_err(|err| SqliteCreationError::SqlxError(err).boxed())?
        } else {
            SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(conn_options.in_memory(true).shared_cache(true))
                .await
                .map_err(|err| SqliteCreationError::SqlxError(err).boxed())?
        };

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
    use crate::db::test_utils::new_test_db_persist_path;

    use super::*;
    use chroma_config::registry::Registry;

    #[tokio::test]
    async fn test_sqlite_db_config_registry() {
        let config = SqliteDBConfig {
            url: new_test_db_persist_path(),
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
