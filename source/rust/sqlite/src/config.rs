use std::{path::Path, time::Duration};

use crate::db::{SqliteCreationError, SqliteDb};
use async_trait::async_trait;
use chroma_config::{
    registry::{Injectable, Registry},
    Configurable,
};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tokio::fs::create_dir_all;

#[cfg(feature = "pyo3")]
use pyo3::{pyclass, pymethods};

fn default_hash_type() -> MigrationHash {
    MigrationHash::MD5
}

fn default_migration_mode() -> MigrationMode {
    MigrationMode::Apply
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct SqliteDBConfig {
    #[serde(default = "default_hash_type")]
    pub hash_type: MigrationHash,
    #[serde(default = "default_migration_mode")]
    pub migration_mode: MigrationMode,
    // The SQLite database URL
    // If unspecified, then the database is in memory only
    pub url: Option<String>,
}

impl Default for SqliteDBConfig {
    fn default() -> Self {
        SqliteDBConfig {
            hash_type: default_hash_type(),
            migration_mode: default_migration_mode(),
            url: None,
        }
    }
}

/// Migration mode for the database
/// - Apply: Apply the migrations
/// - Validate: Validate the applied migrations and ensure none are unappliued
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[cfg_attr(feature = "pyo3", pyclass(eq, eq_int))]
#[serde(rename_all = "lowercase")]
pub enum MigrationMode {
    Apply,
    Validate,
}

/// The hash function to use for the migration files
/// - SHA256: Use SHA256 hash
/// - MD5: Use MD5 hash
#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "pyo3", pyclass(eq, eq_int))]
#[serde(rename_all = "lowercase")]
pub enum MigrationHash {
    SHA256,
    MD5,
}

//////////////////////// PyMethods Implementation ////////////////////////

#[cfg(feature = "pyo3")]
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
impl Configurable<SqliteDBConfig, SqliteCreationError> for SqliteDb {
    async fn try_from_config(
        config: &SqliteDBConfig,
        registry: &Registry,
    ) -> Result<Self, SqliteCreationError> {
        // TODO: copy all other pragmas from python and add basic tests
        let conn_options = SqliteConnectOptions::new()
            // Due to a bug in the python code, foreign_keys is turned off
            // The python code enabled it in a transaction, however,
            // https://www.sqlite.org/pragma.html states that foreign_keys
            // is a no-op in a transaction. In order to be able to run our migrations
            // we turn it off
            .pragma("foreign_keys", "OFF")
            .pragma("case_sensitive_like", "ON")
            .busy_timeout(Duration::from_secs(1000))
            .with_regexp();
        let conn = if let Some(url) = &config.url {
            let path = Path::new(url);
            if let Some(parent) = path.parent() {
                create_dir_all(parent).await?;
            }
            SqlitePoolOptions::new()
                .connect_with(conn_options.filename(path).create_if_missing(true))
                .await?
        } else {
            SqlitePoolOptions::new()
                .max_lifetime(None)
                .idle_timeout(None)
                .max_connections(1)
                .connect_with(conn_options.in_memory(true).shared_cache(true))
                .await?
        };

        let db = SqliteDb::new(conn, config.hash_type);

        db.initialize_migrations_table().await?;
        match config.migration_mode {
            MigrationMode::Apply => {
                db.apply_all_migration().await?;
            }
            MigrationMode::Validate => db.validate_all_migrations().await?,
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
