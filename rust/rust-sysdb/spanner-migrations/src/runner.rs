//! Migration runner for Spanner schema migrations.

use google_cloud_googleapis::spanner::admin::database::v1::UpdateDatabaseDdlRequest;
use google_cloud_spanner::admin::client::Client as AdminClient;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::mutation::insert;
use google_cloud_spanner::statement::Statement;
use thiserror::Error;
use tonic::Code;

use crate::migrations::{GetSourceMigrationsError, Migration, MigrationDir, MIGRATION_DIRS};

#[derive(Error, Debug)]
pub enum MigrationError {
    #[error("Client error: {0}")]
    ClientError(String),
    #[error(transparent)]
    GetSourceMigrationsError(#[from] GetSourceMigrationsError),
    #[error("Migration validation error: {0}")]
    MigrationValidationError(String),
    #[error("Unapplied migrations found: {0}")]
    UnappliedMigrationsFound(usize),
    #[error("DDL execution failed: {0}")]
    DdlError(#[from] tonic::Status),
}

/// Runs Spanner schema migrations.
pub struct MigrationRunner {
    client: Client,
    admin_client: AdminClient,
    database_path: String,
}

impl MigrationRunner {
    pub fn new(client: Client, admin_client: AdminClient, database_path: String) -> Self {
        Self {
            client,
            admin_client,
            database_path,
        }
    }

    pub async fn apply_all_migrations(&self) -> Result<(), MigrationError> {
        for dir in MIGRATION_DIRS.iter() {
            let applied_migrations = self.get_existing_migrations(dir).await?;
            let source_migrations = dir.get_source_migrations()?;

            let unapplied = self.get_unapplied_migrations(applied_migrations, source_migrations)?;

            tracing::info!(
                "Found {} unapplied migrations for {}",
                unapplied.len(),
                dir.as_str()
            );

            for migration in unapplied {
                tracing::info!(
                    "Applying migration: {} (version {})",
                    migration.filename,
                    migration.version
                );
                self.apply_migration(&migration).await?;
            }
        }
        Ok(())
    }

    pub async fn validate_all_migrations(&self) -> Result<(), MigrationError> {
        for dir in MIGRATION_DIRS.iter() {
            let applied_migrations = self.get_existing_migrations(dir).await?;
            let source_migrations = dir.get_source_migrations()?;

            let unapplied = self.get_unapplied_migrations(applied_migrations, source_migrations)?;

            if !unapplied.is_empty() {
                return Err(MigrationError::UnappliedMigrationsFound(unapplied.len()));
            }
        }
        Ok(())
    }

    async fn apply_migration(&self, migration: &Migration) -> Result<(), MigrationError> {
        self.execute_ddl(&migration.sql).await?;
        self.record_migration(migration).await?;
        Ok(())
    }

    async fn execute_ddl(&self, sql: &str) -> Result<(), MigrationError> {
        let sql = sql.trim().trim_end_matches(';');
        tracing::info!("Executing DDL: {}", sql);

        let request = UpdateDatabaseDdlRequest {
            database: self.database_path.clone(),
            statements: vec![sql.to_string()],
            operation_id: String::new(),
            proto_descriptors: Vec::new(),
        };

        let mut operation = self
            .admin_client
            .database()
            .update_database_ddl(request, None)
            .await?;

        // Poll until the DDL operation completes
        operation.wait(None).await?;

        tracing::info!("DDL executed successfully");
        Ok(())
    }

    async fn record_migration(&self, migration: &Migration) -> Result<(), MigrationError> {
        let mutation = insert(
            "migrations",
            &["dir", "version", "filename", "sql", "checksum"],
            &[
                &migration.dir,
                &(migration.version as i64),
                &migration.filename,
                &migration.sql,
                &migration.hash,
            ],
        );

        self.client
            .apply(vec![mutation])
            .await
            .map_err(|e| MigrationError::ClientError(e.to_string()))?;

        Ok(())
    }

    fn get_unapplied_migrations(
        &self,
        applied: Vec<Migration>,
        source: Vec<Migration>,
    ) -> Result<Vec<Migration>, MigrationError> {
        if applied.len() > source.len() {
            tracing::warn!("More migrations applied than source");
            return Ok(vec![]);
        }
        for (i, (applied_m, source_m)) in applied.iter().zip(source.iter()).enumerate() {
            if applied_m.version != source_m.version {
                return Err(MigrationError::MigrationValidationError(format!(
                    "Version mismatch at index {}: applied={}, source={}",
                    i, applied_m.version, source_m.version
                )));
            }
            if applied_m.hash != source_m.hash {
                return Err(MigrationError::MigrationValidationError(format!(
                    "Hash mismatch for migration {}: applied={}, source={}",
                    applied_m.filename, applied_m.hash, source_m.hash
                )));
            }
        }
        Ok(source.into_iter().skip(applied.len()).collect())
    }

    pub async fn initialize_migrations_table(&self) -> Result<(), MigrationError> {
        let ddl = "CREATE TABLE IF NOT EXISTS migrations (dir STRING(255) NOT NULL, version INT64 NOT NULL, filename STRING(512) NOT NULL, sql STRING(MAX) NOT NULL, checksum STRING(64) NOT NULL) PRIMARY KEY (dir, version)";

        match self.execute_ddl(ddl).await {
            Ok(_) => {
                tracing::info!("Migrations table created");
                Ok(())
            }
            Err(MigrationError::DdlError(e)) if e.code() == Code::AlreadyExists => {
                tracing::info!("Migrations table already exists");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn get_existing_migrations(
        &self,
        dir: &MigrationDir,
    ) -> Result<Vec<Migration>, MigrationError> {
        let mut stmt = Statement::new(
            "SELECT dir, version, filename, sql, checksum FROM migrations WHERE dir = @dir ORDER BY version ASC",
        );
        stmt.add_param("dir", &dir.as_str());

        let mut tx = self
            .client
            .single()
            .await
            .map_err(|e| MigrationError::ClientError(e.to_string()))?;

        let mut iter = match tx.query(stmt).await {
            Ok(iter) => iter,
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("not found") || err_str.contains("does not exist") {
                    tracing::error!("Migrations table does not exist yet");
                }
                return Err(MigrationError::ClientError(e.to_string()));
            }
        };

        let mut result = Vec::new();
        while let Some(row) = iter
            .next()
            .await
            .map_err(|e| MigrationError::ClientError(e.to_string()))?
        {
            let dir: String = row
                .column_by_name("dir")
                .expect("failed to get dir from migratons table");
            let version: i64 = row
                .column_by_name("version")
                .expect("failed to get version from migratons table");
            let filename: String = row
                .column_by_name("filename")
                .expect("failed to get filename from migratons table");
            let sql: String = row
                .column_by_name("sql")
                .expect("failed to get sql from migratons table");
            let hash: String = row
                .column_by_name("checksum")
                .expect("failed to get checksum from migratons table");

            result.push(Migration::new(dir, filename, version as i32, sql, hash));
        }

        Ok(result)
    }
}
