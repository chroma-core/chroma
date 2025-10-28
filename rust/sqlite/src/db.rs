use crate::config::{MigrationHash, MigrationMode, SqliteDBConfig};
use crate::migrations::{GetSourceMigrationsError, Migration, MigrationDir, MIGRATION_DIRS};
use chroma_error::{ChromaError, ErrorCodes};
use sqlx::sqlite::SqlitePool;
use sqlx::{Executor, Row};
use thiserror::Error;
use tokio::io;

// // TODO:
// // - support memory mode, add concurrency tests
#[derive(Debug, Clone)]
/// A wrapper around a SQLite database connection that also runs migrations
/// on the database
/// ## Fields:
/// - conn: SqlitePool - The connection to the SQLite database
/// ## Notes:
/// - Clone safety: The SqlitePool is cloneable and all clones share the same connection
///     pool. The pool is Send/Sync.
pub struct SqliteDb {
    conn: SqlitePool,
    migration_hash_type: MigrationHash,
}

impl SqliteDb {
    pub(crate) fn new(conn: SqlitePool, migration_hash_type: MigrationHash) -> Self {
        Self {
            conn,
            migration_hash_type,
        }
    }

    pub fn get_conn(&self) -> &SqlitePool {
        &self.conn
    }

    pub async fn reset(&self) -> Result<(), SqliteMigrationError> {
        // TODO: Make this into a transaction
        let query = r#"
            SELECT name FROM sqlite_master
            WHERE type='table'
        "#;
        let rows = sqlx::query(query).fetch_all(&self.conn).await?;
        for row in rows {
            let name: String = row.get("name");
            let query = format!("DROP TABLE IF EXISTS {}", name);
            sqlx::query(&query).execute(&self.conn).await?;
        }

        self.initialize_migrations_table().await?;
        self.apply_all_migration().await?;

        Ok(())
    }

    //////////////////////// Migrations ////////////////////////

    pub(crate) async fn apply_all_migration(&self) -> Result<(), SqliteMigrationError> {
        let mut all_unapplied_migrations = Vec::new();
        for dir in MIGRATION_DIRS.iter() {
            let applied_migrations = self.get_existing_migrations(dir).await;
            let source_migrations = dir
                .get_source_migrations(&self.migration_hash_type)
                .map_err(SqliteMigrationError::GetSourceMigrationsError)?;
            let unapplied = self
                .validate_migrations_and_get_unapplied(applied_migrations, source_migrations)
                .map_err(SqliteMigrationError::MigrationValidationError)?;
            all_unapplied_migrations.extend(unapplied);
        }
        self.apply_migrations(all_unapplied_migrations).await?;

        Ok(())
    }

    pub(crate) async fn validate_all_migrations(&self) -> Result<(), SqliteMigrationError> {
        if !self.has_initialized_migrations().await {
            return Err(SqliteMigrationError::MigrationsTableNotInitialized);
        }
        for dir in MIGRATION_DIRS.iter() {
            let applied_migrations = self.get_existing_migrations(dir).await;
            let source_migrations = dir
                .get_source_migrations(&self.migration_hash_type)
                .map_err(SqliteMigrationError::GetSourceMigrationsError)?;
            let unapplied =
                self.validate_migrations_and_get_unapplied(applied_migrations, source_migrations)?;
            if !unapplied.is_empty() {
                return Err(SqliteMigrationError::UnappliedMigrationsFound);
            }
        }

        Ok(())
    }

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
    /// ## Arguments:
    /// - applied_migrations: Vec<Migration> - The migrations that have been applied, in ascending version order
    /// - source_migrations: Vec<Migration> - The migrations that are on disk, in ascending version order
    /// ## Returns:
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
            if db_migration.sql != source_migration.sql {
                return Err(MigrationValidationError::InconsistentHash(
                    db_migration.hash.clone(),
                    source_migration.hash.clone(),
                ));
            }
        }

        if applied_migrations.len() > source_migrations.len() {
            return Ok(vec![]);
        }

        let unapplied = source_migrations[applied_migrations.len()..].to_vec();
        Ok(unapplied)
    }

    /// Initialize the migrations table
    /// Note:
    /// - This function is idempotent
    pub(crate) async fn initialize_migrations_table(&self) -> Result<(), sqlx::Error> {
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
        // HACK(hammadb) - https://github.com/launchbadge/sqlx/issues/481#issuecomment-2224913791
        // This is really not great, and ideally we'd write out own pool, like we have
        // in python, but for now this is the best we can do
        let lock_table = r#"
            CREATE TABLE IF NOT EXISTS acquire_write (
                id INTEGER PRIMARY KEY,
                lock_status INTEGER NOT NULL
            )
        "#;
        sqlx::query(lock_table).execute(&self.conn).await?;
        let insert_lock = r#"
            INSERT INTO acquire_write (lock_status) VALUES (TRUE)
        "#;
        sqlx::query(insert_lock).execute(&self.conn).await?;
        Ok(())
    }

    pub async fn begin_immediate<'tx, C>(&self, tx: C) -> Result<(), sqlx::Error>
    where
        C: sqlx::Executor<'tx, Database = sqlx::Sqlite>,
    {
        let query = r#"
            UPDATE acquire_write SET lock_status = TRUE WHERE id = 1
        "#;
        match tx.execute(query).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
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
    /// ## Returns:
    /// - Vec<Migration> - A list of migrations
    /// ## Notes
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
pub enum SqliteMigrationError {
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),
    #[error(transparent)]
    GetSourceMigrationsError(#[from] GetSourceMigrationsError),
    #[error(transparent)]
    MigrationValidationError(#[from] MigrationValidationError),
    #[error("Migrations table not initialized")]
    MigrationsTableNotInitialized,
    #[error("Unapplied migrations found")]
    UnappliedMigrationsFound,
}

impl ChromaError for SqliteMigrationError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            SqliteMigrationError::SqlxError(_) => chroma_error::ErrorCodes::Internal,
            SqliteMigrationError::GetSourceMigrationsError(_) => chroma_error::ErrorCodes::Internal,
            SqliteMigrationError::MigrationValidationError(_) => chroma_error::ErrorCodes::Internal,
            SqliteMigrationError::MigrationsTableNotInitialized => ErrorCodes::Internal,
            SqliteMigrationError::UnappliedMigrationsFound => chroma_error::ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum SqliteCreationError {
    #[error(transparent)]
    MigrationError(#[from] SqliteMigrationError),
    #[error(transparent)]
    PathError(#[from] io::Error),
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),
}

impl ChromaError for SqliteCreationError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            SqliteCreationError::MigrationError(err) => err.code(),
            SqliteCreationError::PathError(_) => ErrorCodes::Internal,
            SqliteCreationError::SqlxError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum MigrationValidationError {
    #[error("Inconsistent version: db={0}, source={1}")]
    InconsistentVersion(i32, i32),
    #[error("Inconsistent hash: db={0}, source={1}")]
    InconsistentHash(String, String),
}

//////////////////////// Test Helpers ////////////////////////

pub mod test_utils {
    use super::*;
    use crate::config::MigrationHash;
    use chroma_config::{registry::Registry, Configurable};
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[allow(dead_code)]
    pub(crate) fn test_migration_dir() -> PathBuf {
        let migration_dir = "migrations/".to_string();
        PathBuf::from(migration_dir)
    }

    pub fn new_test_db_persist_path() -> Option<String> {
        let path = tempdir().unwrap().into_path();
        Some(path.to_str().unwrap().to_string() + "/chroma.sqlite3")
    }

    pub async fn get_new_sqlite_db() -> SqliteDb {
        let config = SqliteDBConfig {
            url: new_test_db_persist_path(),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
        };
        let registry = Registry::new();
        SqliteDb::try_from_config(&config, &registry)
            .await
            .expect("Expect it to be created")
    }
}

//////////////////////// Tests ////////////////////////
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MigrationHash;
    use crate::db::test_utils::{new_test_db_persist_path, test_migration_dir};
    use chroma_config::registry::Registry;
    use chroma_config::Configurable;
    use sqlx::Row;

    //////////////////////// SqliteDb ////////////////////////

    #[tokio::test]
    async fn test_sqlite_db() {
        let config = SqliteDBConfig {
            url: new_test_db_persist_path(),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
        };
        let registry = Registry::new();
        let db = SqliteDb::try_from_config(&config, &registry)
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

    #[tokio::test]
    async fn test_it_initializes_and_validates() {
        let config: SqliteDBConfig = SqliteDBConfig {
            url: new_test_db_persist_path(),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
        };
        let registry = Registry::new();
        let db = SqliteDb::try_from_config(&config, &registry)
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

    #[tokio::test]
    async fn test_migrations_get_applied_on_new_db() {
        let test_db_path = new_test_db_persist_path();
        let config = SqliteDBConfig {
            url: test_db_path.clone(),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
        };
        let registry = Registry::new();
        let db = SqliteDb::try_from_config(&config, &registry)
            .await
            .expect("Expect it to be created");

        // Ensure the migrations were applied by checking the count of migrations we see
        // after creating the db
        for dir in MIGRATION_DIRS.iter() {
            let migrations = db.get_existing_migrations(dir).await;
            let on_disk_path = test_migration_dir().join(dir.as_str());
            // See how many files are in the directory
            let files = std::fs::read_dir(on_disk_path).expect("Expect it to be read");
            let num_files = files.count();
            assert_eq!(migrations.len(), num_files);
        }

        // Ensure validate mode works
        let config = SqliteDBConfig {
            url: test_db_path,
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Validate,
        };
        let registry = Registry::new();
        let _ = SqliteDb::try_from_config(&config, &registry)
            .await
            .expect("Expect it to be created & validated");
    }

    #[tokio::test]
    async fn test_migrations_tampered() {
        let test_db_path = new_test_db_persist_path();
        let config = SqliteDBConfig {
            url: test_db_path.clone(),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
        };
        let registry = Registry::new();
        let db = SqliteDb::try_from_config(&config, &registry)
            .await
            .expect("Expect it to be created");

        // Tamper with a migration file in the db
        let dir = &MIGRATION_DIRS[0];
        let migrations = db.get_existing_migrations(dir).await;
        let mut tampered_migration = migrations[0].clone();
        tampered_migration.sql = "SELECT 1".to_string();
        let query = r#"
            UPDATE migrations
            SET sql = $1
            WHERE dir = $2 AND version = $3
        "#;
        let query = sqlx::query(query)
            .bind(&tampered_migration.sql)
            .bind(&tampered_migration.dir)
            .bind(tampered_migration.version);
        db.conn
            .execute(query)
            .await
            .expect("Expect it to be executed");

        // Ensure validate mode fails
        let config = SqliteDBConfig {
            url: test_db_path,
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Validate,
        };

        let result = SqliteDb::try_from_config(&config, &registry).await;
        match result {
            Ok(_) => panic!("Expect it to fail"),
            Err(e) => {
                assert!(e.to_string().contains("Inconsistent hash"))
            }
        }
    }

    #[tokio::test]
    async fn test_migrations_reorder() {
        let test_db_path = new_test_db_persist_path();
        let config = SqliteDBConfig {
            url: test_db_path.clone(),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
        };
        let db = SqliteDb::try_from_config(&config, &Registry::new())
            .await
            .expect("Expect it to be created");

        // Reorder the migrations in the db
        let dir = &MIGRATION_DIRS[0];
        let migrations = db.get_existing_migrations(dir).await;
        let mut reordered_migrations = migrations.clone();
        reordered_migrations.reverse();
        for (i, migration) in reordered_migrations.iter().enumerate() {
            let query = r#"
                UPDATE migrations
                SET version = $1
                WHERE dir = $2 AND version = $3
            "#;
            let query = sqlx::query(query)
                .bind((i + reordered_migrations.len()) as u32)
                .bind(&migration.dir)
                .bind(migration.version);
            db.conn
                .execute(query)
                .await
                .expect("Expect it to be executed");
        }

        // Ensure validate mode fails
        let config = SqliteDBConfig {
            url: test_db_path,
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Validate,
        };

        let result = SqliteDb::try_from_config(&config, &Registry::new()).await;
        match result {
            Ok(_) => panic!("Expect it to fail"),
            Err(e) => {
                assert!(e.to_string().contains("Inconsistent version"))
            }
        }
    }

    #[tokio::test]
    async fn test_reset() {
        let test_db_path = new_test_db_persist_path();
        let config = SqliteDBConfig {
            url: test_db_path.clone(),
            hash_type: MigrationHash::MD5,
            migration_mode: MigrationMode::Apply,
        };
        let db = SqliteDb::try_from_config(&config, &Registry::new())
            .await
            .expect("Expect it to be created");

        // Insert a tenant
        sqlx::query(
            r#"
            INSERT INTO tenants (id)
            VALUES ($1)
        "#,
        )
        .bind("test_tenant")
        .execute(&db.conn)
        .await
        .expect("Expect it to be executed");

        // Reset the db
        db.reset().await.expect("Expect it to reset without error");

        // Tenant should no longer exist
        let result = sqlx::query(
            r#"
            SELECT id FROM tenants WHERE id = $1
        "#,
        )
        .bind("test_tenant")
        .fetch_all(&db.conn)
        .await
        .expect("Expect it to be executed");
        assert!(result.is_empty());
    }
}
