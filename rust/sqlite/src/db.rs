use crate::config::{MigrationMode, SqliteDBConfig};
use crate::migrations::{GetSourceMigrationsError, Migration, MigrationDir, MIGRATION_DIRS};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use sqlx::{Executor, Row};
use thiserror::Error;

// // TODO:
// // - support memory mode, add concurrency tests
struct SqliteDb {
    conn: SqlitePool,
    config: SqliteDBConfig,
}

impl SqliteDb {
    pub async fn try_from_config(config: &SqliteDBConfig) -> Result<Self, SqliteCreationError> {
        // TODO: copy all other pragmas from python and add basic tests
        let options = SqliteConnectOptions::new()
            .filename(&config.url)
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
            .map_err(|e| SqliteCreationError::SqlxError(e))?;

        let db = Self {
            conn,
            config: config.clone(),
        };

        db.initialize_migrations_table().await?;
        match config.migration_mode {
            MigrationMode::Apply => {
                let mut all_unapplied_migrations = Vec::new();
                for dir in MIGRATION_DIRS.iter() {
                    let applied_migrations = db.get_existing_migrations(dir).await;
                    let source_migrations = dir
                        .get_source_migrations(&config.hash_type)
                        .map_err(SqliteCreationError::GetSourceMigrationsError)?;
                    let unapplied = db
                        .validate_migrations_and_get_unapplied(
                            applied_migrations,
                            source_migrations,
                        )
                        .map_err(SqliteCreationError::MigrationValidationError)?;
                    all_unapplied_migrations.extend(unapplied);
                }
                db.apply_migrations(all_unapplied_migrations).await?;
            }
            MigrationMode::Validate => {
                // TODO: Test this
                if !db.has_initialized_migrations().await {
                    return Err(SqliteCreationError::MigrationsTableNotInitialized);
                }
                for dir in MIGRATION_DIRS.iter() {
                    let applied_migrations = db.get_existing_migrations(dir).await;
                    let source_migrations = dir
                        .get_source_migrations(&config.hash_type)
                        .map_err(SqliteCreationError::GetSourceMigrationsError)?;
                    let unapplied = db.validate_migrations_and_get_unapplied(
                        applied_migrations,
                        source_migrations,
                    )?;
                    if !unapplied.is_empty() {
                        return Err(SqliteCreationError::UnappliedMigrationsFound);
                    }
                }
            }
        }
        Ok(db)
    }

    //////////////////////// Migrations ////////////////////////

    /// Apply all migrations in a transaction
    /// Arguments:
    /// - migrations: Vec<Migration> - The migrations to apply
    async fn apply_migrations(&self, migrations: Vec<Migration>) -> Result<(), sqlx::Error> {
        let mut tx = self.conn.begin().await?;
        for migration in migrations {
            // Apply the migration
            // TODO(hammadb): Determine how to handle foreign keys on
            // this is copied over from the python code but it does
            // not work in a transaction
            tx.execute("PRAGMA foreign_keys = ON").await?;
            tx.execute(sqlx::query(&migration.sql)).await?;

            // Bookkeeping
            let query = r#"
                INSERT INTO migrations (dir, version, filename, sql, hash)
                VALUES ($1, $2, $3, $4, $5)
            "#;
            let query = sqlx::query(query)
                .bind(&migration.dir)
                .bind(migration.version)
                .bind(&migration.filename)
                .bind(&migration.sql)
                .bind(&migration.hash);
            tx.execute(query).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Validate migration sequence and get the migrations that need to be applied
    /// Arguments:
    /// - applied_migrations: Vec<Migration> - The migrations that have been applied, in ascending version order
    /// - source_migrations: Vec<Migration> - The migrations that are on disk, in ascending version order
    /// Returns:
    /// - Vec<Migration> - The migrations that need to be applied
    fn validate_migrations_and_get_unapplied(
        &self,
        applied_migrations: Vec<Migration>,
        source_migrations: Vec<Migration>,
    ) -> Result<Vec<Migration>, MigrationValidationError> {
        for (db_migration, source_migration) in
            applied_migrations.iter().zip(source_migrations.iter())
        {
            if db_migration.version != source_migration.version {
                return Err(MigrationValidationError::InconsistentVersion(
                    db_migration.version,
                    source_migration.version,
                ));
            }
            if db_migration.hash != source_migration.hash {
                return Err(MigrationValidationError::InconsistentHash(
                    db_migration.hash.clone(),
                    source_migration.hash.clone(),
                ));
            }
        }

        let unapplied = source_migrations[applied_migrations.len()..].to_vec();
        Ok(unapplied)
    }

    /// Initialize the migrations table
    /// Note:
    /// - This function is idempotent
    async fn initialize_migrations_table(&self) -> Result<(), sqlx::Error> {
        let query = r#"
            CREATE TABLE IF NOT EXISTS migrations (
                dir TEXT NOT NULL,
                version INTEGER NOT NULL,
                filename TEXT NOT NULL,
                sql TEXT NOT NULL,
                hash TEXT NOT NULL,
                PRIMARY KEY (dir, version)
            )
        "#;
        sqlx::query(query).execute(&self.conn).await?;
        Ok(())
    }

    /// Check if the migrations table has been initialized
    /// Returns:
    /// - bool - True if the migrations table has been initialized
    async fn has_initialized_migrations(&self) -> bool {
        let query = r#"
            SELECT name FROM sqlite_master WHERE type='table' AND name='migrations'
        "#;
        let row = sqlx::query(query)
            .fetch_one(&self.conn)
            .await
            .expect("Expect it to be fetched");
        let name: String = row.get("name");
        name == "migrations" // Sanity check
    }

    /// Get existing migrations for a given directory
    /// Arguments:
    /// - dir_name: str - The name of the directory that contains the migrations
    /// Returns:
    /// - Vec<Migration> - A list of migrations
    /// Notes
    /// - dir_name has to be held constant for a given migration directory
    /// - The migrations are sorted by version in ascending order
    /// - The dir_name is consistent with the python implementation
    async fn get_existing_migrations(&self, dir: &MigrationDir) -> Vec<Migration> {
        let query = r#"
            SELECT dir, version, filename, sql, hash
            FROM migrations
            WHERE dir = $1
            ORDER BY version ASC
        "#;
        let rows = sqlx::query(query)
            .bind(dir.as_str())
            .fetch_all(&self.conn)
            .await
            .expect("Expect it to be fetched");

        let mut migrations = Vec::new();
        for row in rows {
            let dir: String = row.get("dir");
            let version: i32 = row.get("version");
            let filename: String = row.get("filename");
            let sql: String = row.get("sql");
            let hash: String = row.get("hash");
            migrations.push(Migration::new(dir, filename, version, sql, hash));
        }
        migrations
    }
}

//////////////////////// Error Types ////////////////////////

#[derive(Error, Debug)]
pub enum SqliteCreationError {
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),
    #[error(transparent)]
    GetSourceMigrationsError(#[from] GetSourceMigrationsError),
    #[error(transparent)]
    MigrationValidationError(#[from] MigrationValidationError),
    #[error("Migrations table not initialized")]
    MigrationsTableNotInitialized,
    #[error("Unappliued migrations found")]
    UnappliedMigrationsFound,
}

#[derive(Error, Debug)]
pub enum MigrationValidationError {
    #[error("Inconsistent version: db={0}, source={1}")]
    InconsistentVersion(i32, i32),
    #[error("Inconsistent hash: db={0}, source={1}")]
    InconsistentHash(String, String),
}

//////////////////////// Tests ////////////////////////
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MigrationHash;
    use sqlx::Row;
    use std::path::PathBuf;

    //////////////////////// Test Helpers ////////////////////////

    fn test_migration_dir() -> PathBuf {
        let migration_dir = "/../migrations".to_string();
        PathBuf::from(migration_dir)
    }

    fn new_test_db_path() -> String {
        // TODO: Make tmpfile work
        // let dir = tempdir().expect("Expect it to be created");
        // let path = dir.path().join("chroma.sqlite3");
        let path = "/Users/hammad/Documents/chroma/chroma/chromaTEST.sqlite3".to_string();
        // remove the file if it exists
        std::fs::remove_file(&path).unwrap_or_default();
        path
    }

    //////////////////////// SqliteDb ////////////////////////

    #[tokio::test]
    async fn test_sqlite_db() {
        let config = SqliteDBConfig {
            url: "sqlite::memory:".to_string(),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
        };
        let db = SqliteDb::try_from_config(&config)
            .await
            .expect("Expect it to be created");

        // Check if migrations table exists
        let query = r#"
            SELECT name FROM sqlite_master WHERE type='table' AND name='migrations'
        "#;
        let row = sqlx::query(query)
            .fetch_one(&db.conn)
            .await
            .expect("Expect it to be fetched");
        let name: String = row.get("name");
        assert_eq!(name, "migrations");
    }

    // #[tokio::test]
    // async fn test_migrations_validate_on_existing_db() {
    //     let config: SqliteDBConfig = SqliteDBConfig {
    //         url: existing_test_db_path(),
    //         migrations_root_dir: test_migration_dir(),
    //         hash_type: MigrationHash::MD5,
    //         migration_mode: MigrationMode::Validate,
    //     };
    //     let db = SqliteDb::try_from_config(&config)
    //         .await
    //         .expect("Expect it to be created");

    //     // Check if migrations table exists
    //     let query = r#"
    //         SELECT name FROM sqlite_master WHERE type='table' AND name='migrations'
    //     "#;
    //     let row = sqlx::query(query)
    //         .fetch_one(&db.conn)
    //         .await
    //         .expect("Expect it to be fetched");
    //     let name: String = row.get("name");
    //     assert_eq!(name, "migrations");
    // }

    #[tokio::test]
    async fn test_migrations_get_applied_on_new_db() {
        let config = SqliteDBConfig {
            url: new_test_db_path(),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
        };
        let db = SqliteDb::try_from_config(&config)
            .await
            .expect("Expect it to be created");
        for dir in MIGRATION_DIRS.iter() {
            let migrations = db.get_existing_migrations(&dir).await;
            let on_disk_path = test_migration_dir().join(dir.as_str());
            // See how many files are in the directory
            let files = std::fs::read_dir(on_disk_path).unwrap();
            let num_files = files.count();
            assert_eq!(migrations.len(), num_files);
        }
    }

    // TODO: more tests
    // - add test migrations
    // - tamper with one and test
    // - add new migration and test
    // - reorder migrations
}
