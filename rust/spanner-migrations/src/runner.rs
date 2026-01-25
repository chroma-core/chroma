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

// TODO(tanujnay112): Remove this backwards compatibility migration once all systems are updated
#[derive(Clone, Copy)]
struct LegacyHashMapping {
    old_hash: &'static str,
    new_hash: &'static str,
}

/// Legacy hash mappings for backwards compatibility
static LEGACY_HASH_MAPPINGS: std::sync::LazyLock<
    std::collections::HashMap<&'static str, LegacyHashMapping>,
> = std::sync::LazyLock::new(|| {
    std::collections::HashMap::from([
        // Technically not needed but doesn't hurt
        (
            "0001-create_tenants.spanner.sql",
            LegacyHashMapping {
                old_hash: "87dbaa652753aba0730b9b784a2974f0b23c2017b562ec9f0b01663d9c840321",
                new_hash: "87dbaa652753aba0730b9b784a2974f0b23c2017b562ec9f0b01663d9c840321",
            },
        ),
        (
            "0002-create_databases.spanner.sql",
            LegacyHashMapping {
                old_hash: "167906c4b0c6de55f535925908bbb8130a1492741f4584b4987f97c0a4fd8818",
                new_hash: "e07d1f1b3d7c9ac2d7df289b7da4b46602c350a3b7589444cde87ece30db4fa8",
            },
        ),
    ])
});

/// Migrates old individual file hashes to new rolling hash format
fn remap_legacy_hashes(migrations: &mut [Migration]) {
    for migration in migrations.iter_mut() {
        if let Some(mapping) = LEGACY_HASH_MAPPINGS.get(migration.filename.as_str()) {
            if migration.hash == mapping.old_hash {
                migration.hash = mapping.new_hash.to_string();
            }
        }
    }
}

impl MigrationRunner {
    pub fn new(client: Client, admin_client: AdminClient, database_path: String) -> Self {
        Self {
            client,
            admin_client,
            database_path,
        }
    }

    // TODO(tanujnay112): Remove this method after all legacy hashes are migrated
    pub async fn migrate_legacy_hashes(&self) -> Result<usize, MigrationError> {
        let mut total_updated = 0;
        tracing::info!("Starting legacy hash migration process");

        for (filename, mapping) in LEGACY_HASH_MAPPINGS.iter() {
            // Use read_write_transaction to execute UPDATE and get affected rows count
            let (_, updated_rows) = self
                .client
                .read_write_transaction(|tx| Box::pin(async move {
                    let mut stmt = Statement::new(
                        "UPDATE migrations SET checksum = @new_hash WHERE filename = @filename AND checksum = @old_hash",
                    );
                    stmt.add_param("new_hash", &(mapping.new_hash.to_string()));
                    stmt.add_param("filename", &(filename.to_string()));
                    stmt.add_param("old_hash", &(mapping.old_hash.to_string()));

                    let affected_rows = tx
                        .update(stmt)
                        .await
                        .map_err(|e| tonic::Status::internal(e.to_string()))?;

                    Ok(affected_rows)
                }))
                .await
                .map_err(|e: google_cloud_spanner::client::Error| MigrationError::ClientError(e.to_string()))?;

            total_updated += updated_rows;

            if updated_rows > 0 {
                tracing::info!(
                    "Updated {} rows for migration {} from old hash {} to new hash {}",
                    updated_rows,
                    filename,
                    mapping.old_hash,
                    mapping.new_hash
                );
            }
        }

        tracing::info!(
            "Total rows updated during legacy hash migration: {}",
            total_updated
        );
        Ok(total_updated as usize)
    }

    pub async fn apply_all_migrations(&self, slug: Option<&str>) -> Result<(), MigrationError> {
        for dir in MIGRATION_DIRS.iter() {
            if let Some(slug) = slug {
                if dir.migration_slug() != slug {
                    tracing::info!("skipping {} != {}", dir.migration_slug(), slug);
                    continue;
                }
            }
            let applied_migrations = self.get_existing_migrations(dir).await?;
            let source_migrations = dir.get_source_migrations()?;

            let unapplied = self.get_unapplied_migrations(applied_migrations, source_migrations)?;

            tracing::info!(
                "Found {} unapplied migrations for {}",
                unapplied.len(),
                dir.migration_slug()
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

        // TODO(tanujnay112): Remove once legacy hash migration is no longer needed
        // Technically an error here need not error the process.
        if let Err(e) = self.migrate_legacy_hashes().await {
            tracing::warn!("Legacy hash migration failed. This may be acceptable if it has already run. Error: {}", e);
        }
        self.client.clone().close().await;
        Ok(())
    }

    pub async fn validate_all_migrations(&self, slug: Option<&str>) -> Result<(), MigrationError> {
        for dir in MIGRATION_DIRS.iter() {
            if let Some(slug) = slug {
                if dir.migration_slug() != slug {
                    tracing::info!("skipping {} != {}", dir.migration_slug(), slug);
                    continue;
                }
            }
            let applied_migrations = self.get_existing_migrations(dir).await?;
            let source_migrations = dir.get_source_migrations()?;

            let unapplied = self.get_unapplied_migrations(applied_migrations, source_migrations)?;

            if !unapplied.is_empty() {
                return Err(MigrationError::UnappliedMigrationsFound(unapplied.len()));
            }
        }
        self.client.clone().close().await;
        Ok(())
    }

    async fn apply_migration(&self, migration: &Migration) -> Result<(), MigrationError> {
        self.execute_migration_statement(&migration.sql).await?;
        self.record_migration(migration).await?;
        Ok(())
    }

    async fn execute_migration_statement(&self, sql: &str) -> Result<(), MigrationError> {
        let sql = sql.trim().trim_end_matches(';');
        if let Some(dml) = sql.strip_prefix("-- DML:") {
            self.execute_dml(dml).await?;
        } else {
            tracing::info!("Executing DDL: {}", sql);

            let request = UpdateDatabaseDdlRequest {
                database: self.database_path.clone(),
                statements: vec![sql.to_string()],
                operation_id: String::new(),
                proto_descriptors: Vec::new(),
                throughput_mode: false,
            };

            let mut operation = self
                .admin_client
                .database()
                .update_database_ddl(request, None)
                .await?;

            // Poll until the DDL operation completes
            operation.wait(None).await?;

            tracing::info!("DDL executed successfully");
        }
        Ok(())
    }

    async fn execute_dml(&self, dml: &str) -> Result<(), MigrationError> {
        tracing::info!("Executing DML: {}", dml);
        let dml = dml.to_string();
        self.client
            .read_write_transaction(|tx| {
                let dml = dml.clone();
                Box::pin(async move {
                    let stmt = Statement::new(&dml);
                    tx.update(stmt).await?;
                    Ok(())
                })
            })
            .await
            .map_err(|err: google_cloud_spanner::client::Error| {
                MigrationError::ClientError(err.to_string())
            })?;
        tracing::info!("DML executed successfully");
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

    #[allow(clippy::result_large_err)]
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

        match self.execute_migration_statement(ddl).await {
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
        stmt.add_param("dir", &dir.migration_slug());

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

        remap_legacy_hashes(&mut result);

        Ok(result)
    }
}
