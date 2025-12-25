//! Spanner backend implementation.
//!
//! This module provides the `SpannerBackend` which implements all SysDb
//! operations using Google Cloud Spanner as the underlying database.

use chroma_config::{registry::Registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use google_cloud_gax::conn::Environment;
use google_cloud_spanner::client::{Client, ClientConfig, Error as SpannerClientError};
use google_cloud_spanner::statement::Statement;
use thiserror::Error;

use crate::config::SpannerConfig;
use crate::error::SysDbError;
use crate::types::{
    CreateDatabaseRequest, CreateDatabaseResponse, CreateTenantRequest, CreateTenantResponse,
    Database, GetDatabaseRequest, GetDatabaseResponse, GetTenantRequest, GetTenantResponse,
    SetTenantResourceNameRequest, SetTenantResourceNameResponse, Tenant,
};

#[derive(Error, Debug)]
pub enum SpannerError {
    #[error("Failed to connect to Spanner database: {0}")]
    ConnectionError(String),
    #[error("Failed to configure Spanner client: {0}")]
    ConfigurationError(String),
}

impl ChromaError for SpannerError {
    fn code(&self) -> ErrorCodes {
        match self {
            SpannerError::ConnectionError(_) => ErrorCodes::Internal,
            SpannerError::ConfigurationError(_) => ErrorCodes::Internal,
        }
    }
}

/// Spanner backend implementation.
///
/// Wraps a Google Cloud Spanner client and provides methods for all
/// SysDb operations.
#[derive(Clone)]
pub struct SpannerBackend {
    client: Client,
}

impl SpannerBackend {
    /// Create a new SpannerBackend with the given client.
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Get a reference to the underlying Spanner client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    // ============================================================
    // Tenant Operations
    // ============================================================

    /// Create a new tenant.
    ///
    /// Inserts a new tenant record with the given name as the ID.
    /// Uses commit timestamps for created_at and updated_at.
    /// Sets last_compaction_time to Unix epoch (0) by default.
    /// If the tenant already exists, does nothing (insert on conflict do nothing).
    pub async fn create_tenant(
        &self,
        req: &CreateTenantRequest,
    ) -> Result<CreateTenantResponse, SysDbError> {
        // In the schema, tenant id IS the tenant name
        let tenant_id = &req.name;

        // Use a read-write transaction to atomically check and insert
        self.client
            .read_write_transaction(|tx| {
                let tenant_id = tenant_id.clone();
                Box::pin(async move {
                    // Check if tenant already exists
                    let mut check_stmt = Statement::new(
                        "SELECT id FROM tenants WHERE id = @id AND is_deleted = FALSE",
                    );
                    check_stmt.add_param("id", &tenant_id);

                    let mut iter = tx.query(check_stmt).await?;

                    // If tenant doesn't exist, insert it using DML
                    // Set last_compaction_time to Unix epoch (0) by default
                    if iter.next().await?.is_none() {
                        let mut insert_stmt = Statement::new(
                            "INSERT INTO tenants (id, is_deleted, created_at, updated_at, last_compaction_time) VALUES (@id, @is_deleted, PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP(), TIMESTAMP_SECONDS(0))",
                        );
                        insert_stmt.add_param("id", &tenant_id);
                        insert_stmt.add_param("is_deleted", &false);
                        
                        tx.update(insert_stmt).await?;
                        tracing::info!("Created tenant: {}", tenant_id);
                    } else {
                        tracing::debug!("Tenant already exists, skipping insert: {}", tenant_id);
                    }

                    Ok(())
                })
            })
            .await
            .map_err(|e: SpannerClientError| SysDbError::Spanner(e.to_string()))?;

        Ok(CreateTenantResponse {})
    }

    /// Get a tenant by name.
    ///
    /// Returns None if the tenant does not exist or is marked as deleted.
    pub async fn get_tenant(
        &self,
        req: &GetTenantRequest,
    ) -> Result<Option<GetTenantResponse>, SysDbError> {
        let tenant_id = &req.name;

        let mut stmt = Statement::new(
            "SELECT id, resource_name FROM tenants WHERE id = @id AND is_deleted = FALSE",
        );
        stmt.add_param("id", tenant_id);

        let mut tx = self
            .client
            .single()
            .await
            .map_err(|e| SysDbError::Spanner(e.to_string()))?;

        let mut iter = tx
            .query(stmt)
            .await
            .map_err(|e| SysDbError::Spanner(e.to_string()))?;

        // Get the first row if it exists
        if let Some(row) = iter
            .next()
            .await
            .map_err(|e| SysDbError::Spanner(e.to_string()))?
        {
            let id: String = row
                .column_by_name("id")
                .map_err(|e| SysDbError::Internal(format!("failed to read 'id' column: {}", e)))?;

            // resource_name can be NULL, so we handle the error as None
            let resource_name: Option<String> = row.column_by_name("resource_name").ok();

            Ok(Some(GetTenantResponse {
                tenant: Tenant {
                    id: id.clone(),
                    name: id, // In this schema, id IS the name
                    resource_name,
                },
            }))
        } else {
            Ok(None)
        }
    }

    /// Set the resource name for a tenant.
    ///
    /// Only sets if resource_name is currently NULL.
    pub async fn set_tenant_resource_name(
        &self,
        _req: &SetTenantResourceNameRequest,
    ) -> Result<SetTenantResourceNameResponse, SysDbError> {
        unimplemented!("implement set_tenant_resource_name")
    }

    // ============================================================
    // Database Operations
    // ============================================================

    /// Create a new database.
    ///
    /// Validates that the database name is not empty and that the tenant exists.
    /// Uses commit timestamps for created_at and updated_at.
    pub async fn create_database(
        &self,
        req: &CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse, SysDbError> {
        // Validate database name is not empty
        if req.name.is_empty() {
            return Err(SysDbError::InvalidArgument(
                "database name cannot be empty".to_string(),
            ));
        }

        // Check if tenant exists
        let get_tenant_req = GetTenantRequest {
            name: req.tenant.clone(),
        };
        let tenant = self.get_tenant(&get_tenant_req).await?;
        if tenant.is_none() {
            return Err(SysDbError::NotFound(format!(
                "tenant '{}' not found",
                req.tenant
            )));
        }

        // Use a read-write transaction to atomically check and insert
        self.client
            .read_write_transaction(|tx| {
                let db_id = req.id.clone();
                let db_name = req.name.clone();
                let tenant_id = req.tenant.clone();
                Box::pin(async move {
                    // Check if database already exists
                    let mut check_stmt = Statement::new(
                        "SELECT ID FROM databases WHERE ID = @id AND IS_DELETED = FALSE",
                    );
                    check_stmt.add_param("id", &db_id);

                    let mut iter = tx.query(check_stmt).await?;

                    // If database doesn't exist, insert it
                    if iter.next().await?.is_none() {
                        let mut insert_stmt = Statement::new(
                            "INSERT INTO databases (ID, NAME, TENANT_ID, IS_DELETED, CREATED_AT, UPDATED_AT) VALUES (@id, @name, @tenant_id, @is_deleted, PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP())",
                        );
                        insert_stmt.add_param("id", &db_id);
                        insert_stmt.add_param("name", &db_name);
                        insert_stmt.add_param("tenant_id", &tenant_id);
                        insert_stmt.add_param("is_deleted", &false);

                        tx.update(insert_stmt).await?;
                        tracing::info!("Created database: {} for tenant: {}", db_name, tenant_id);
                    } else {
                        tracing::debug!("Database already exists, skipping insert: {}", db_id);
                    }

                    Ok(())
                })
            })
            .await
            .map_err(|e: SpannerClientError| SysDbError::Spanner(e.to_string()))?;

        Ok(CreateDatabaseResponse {})
    }

    /// Get a database by name and tenant.
    ///
    /// Returns None if the database does not exist or is marked as deleted.
    pub async fn get_database(
        &self,
        req: &GetDatabaseRequest,
    ) -> Result<Option<GetDatabaseResponse>, SysDbError> {
        let mut stmt = Statement::new(
            "SELECT ID, NAME, TENANT_ID FROM databases WHERE NAME = @name AND TENANT_ID = @tenant_id AND IS_DELETED = FALSE",
        );
        stmt.add_param("name", &req.name);
        stmt.add_param("tenant_id", &req.tenant);

        let mut tx = self
            .client
            .single()
            .await
            .map_err(|e| SysDbError::Spanner(e.to_string()))?;

        let mut iter = tx
            .query(stmt)
            .await
            .map_err(|e| SysDbError::Spanner(e.to_string()))?;

        // Get the first row if it exists
        if let Some(row) = iter
            .next()
            .await
            .map_err(|e| SysDbError::Spanner(e.to_string()))?
        {
            let id: String = row
                .column_by_name("ID")
                .map_err(|e| SysDbError::Internal(format!("failed to read 'ID' column: {}", e)))?;
            let name: String = row
                .column_by_name("NAME")
                .map_err(|e| SysDbError::Internal(format!("failed to read 'NAME' column: {}", e)))?;
            let tenant_id: String = row
                .column_by_name("TENANT_ID")
                .map_err(|e| SysDbError::Internal(format!("failed to read 'TENANT_ID' column: {}", e)))?;

            Ok(Some(GetDatabaseResponse {
                database: Database {
                    id,
                    name,
                    tenant_id,
                },
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn list_databases(
        &self,
        _tenant: &str,
        _limit: Option<i32>,
        _offset: i32,
    ) -> Result<Vec<Database>, SysDbError> {
        todo!("implement list_databases")
    }

    pub async fn delete_database(&self, _name: &str, _tenant: &str) -> Result<(), SysDbError> {
        todo!("implement delete_database")
    }

    // ============================================================
    // Lifecycle
    // ============================================================

    pub async fn close(self) {
        self.client.close().await;
    }
}

#[async_trait::async_trait]
impl Configurable<SpannerConfig> for SpannerBackend {
    async fn try_from_config(
        config: &SpannerConfig,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let client = match config {
            SpannerConfig::Emulator(emulator) => {
                let client_config = ClientConfig {
                    environment: Environment::Emulator(emulator.grpc_endpoint()),
                    ..Default::default()
                };

                let client = Client::new(&emulator.database_path(), client_config)
                    .await
                    .map_err(|e| {
                        Box::new(SpannerError::ConnectionError(e.to_string()))
                            as Box<dyn ChromaError>
                    })?;

                tracing::info!(
                    "Connected to Spanner emulator: {}",
                    emulator.database_path()
                );

                client
            }
            SpannerConfig::Gcp(gcp) => {
                let client_config = ClientConfig::default().with_auth().await.map_err(|e| {
                    Box::new(SpannerError::ConfigurationError(e.to_string()))
                        as Box<dyn ChromaError>
                })?;

                let client = Client::new(&gcp.database_path(), client_config)
                    .await
                    .map_err(|e| {
                        Box::new(SpannerError::ConnectionError(e.to_string()))
                            as Box<dyn ChromaError>
                    })?;

                tracing::info!("Connected to Spanner GCP: {}", gcp.database_path());

                client
            }
        };

        Ok(SpannerBackend { client })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        CreateDatabaseRequest, CreateTenantRequest, GetDatabaseRequest, GetTenantRequest,
    };
    use uuid::Uuid;

    // These tests require Tilt to be running with Spanner emulator.
    // They will be skipped if the Spanner emulator is not reachable.
    // To run: cargo test --package rust-sysdb --lib spanner::tests

    async fn setup_test_backend() -> Option<SpannerBackend> {
        use chroma_config::registry::Registry;
        use chroma_config::spanner::SpannerEmulatorConfig;
        use chroma_config::Configurable;

        // Use the same config as Tilt (localhost:9010 when port-forwarded)
        let emulator = SpannerEmulatorConfig {
            host: "localhost".to_string(),
            grpc_port: 9010,
            rest_port: 9020,
            project: "local-project".to_string(),
            instance: "test-instance".to_string(),
            database: "local-database".to_string(),
        };

        let config = SpannerConfig::Emulator(emulator);
        let registry = Registry::new();

        match SpannerBackend::try_from_config(&config, &registry).await {
            Ok(backend) => Some(backend),
            Err(e) => {
                eprintln!(
                    "Failed to connect to Spanner emulator: {:?}. Is Tilt running?",
                    e
                );
                None
            }
        }
    }

    #[tokio::test]
    #[ignore] // Requires Tilt running
    async fn test_create_and_get_tenant() {
        let Some(backend) = setup_test_backend().await else {
            println!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
            return;
        };

        let tenant_name = format!(
            "test_tenant_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        // Test create_tenant
        let create_req = CreateTenantRequest {
            name: tenant_name.clone(),
        };
        let result = backend.create_tenant(&create_req).await;
        assert!(
            result.is_ok(),
            "Failed to create tenant: {:?}",
            result.err()
        );

        // Test get_tenant
        let get_req = GetTenantRequest {
            name: tenant_name.clone(),
        };
        let result = backend.get_tenant(&get_req).await;
        assert!(result.is_ok(), "Failed to get tenant: {:?}", result.err());

        let tenant_response = result.unwrap();
        assert!(
            tenant_response.is_some(),
            "Tenant should exist after creation"
        );

        let tenant = tenant_response.unwrap();
        println!("Tenant: {:?}", tenant);
        assert_eq!(tenant.tenant.name, tenant_name);
        assert_eq!(tenant.tenant.id, tenant_name);
    }

    #[tokio::test]
    #[ignore] // Requires Tilt running
    async fn test_create_tenant_idempotent() {
        let Some(backend) = setup_test_backend().await else {
            println!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
            return;
        };

        let tenant_name = format!(
            "test_tenant_idempotent_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        // Create tenant first time
        let create_req = CreateTenantRequest {
            name: tenant_name.clone(),
        };
        let result1 = backend.create_tenant(&create_req).await;
        assert!(
            result1.is_ok(),
            "Failed to create tenant first time: {:?}",
            result1.err()
        );

        // Create tenant second time (should succeed - idempotent)
        let result2 = backend.create_tenant(&create_req).await;
        assert!(
            result2.is_ok(),
            "Failed to create tenant second time (should be idempotent): {:?}",
            result2.err()
        );

        // Verify tenant exists
        let get_req = GetTenantRequest {
            name: tenant_name.clone(),
        };
        let result = backend.get_tenant(&get_req).await;
        assert!(result.is_ok(), "Failed to get tenant: {:?}", result.err());
        assert!(result.unwrap().is_some(), "Tenant should exist");
    }

    #[tokio::test]
    #[ignore] // Requires Tilt running
    async fn test_get_nonexistent_tenant() {
        let Some(backend) = setup_test_backend().await else {
            println!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
            return;
        };

        let tenant_name = format!(
            "nonexistent_tenant_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let get_req = GetTenantRequest { name: tenant_name };
        let result = backend.get_tenant(&get_req).await;
        assert!(
            result.is_ok(),
            "Getting nonexistent tenant should not error"
        );
        assert!(
            result.unwrap().is_none(),
            "Nonexistent tenant should return None"
        );
    }

    #[tokio::test]
    #[ignore] // Requires Tilt running
    async fn test_create_and_get_database() {
        let Some(backend) = setup_test_backend().await else {
            println!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
            return;
        };

        // First create a tenant
        let tenant_name = format!(
            "test_tenant_db_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let create_tenant_req = CreateTenantRequest {
            name: tenant_name.clone(),
        };
        let result = backend.create_tenant(&create_tenant_req).await;
        assert!(
            result.is_ok(),
            "Failed to create tenant: {:?}",
            result.err()
        );

        // Now create a database
        let db_id = Uuid::new_v4().to_string();
        let db_name = format!(
            "test_database_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let create_db_req = CreateDatabaseRequest {
            id: db_id.clone(),
            name: db_name.clone(),
            tenant: tenant_name.clone(),
        };
        let result = backend.create_database(&create_db_req).await;
        assert!(
            result.is_ok(),
            "Failed to create database: {:?}",
            result.err()
        );

        // Test get_database
        let get_db_req = GetDatabaseRequest {
            name: db_name.clone(),
            tenant: tenant_name.clone(),
        };
        let result = backend.get_database(&get_db_req).await;
        assert!(
            result.is_ok(),
            "Failed to get database: {:?}",
            result.err()
        );

        let db_response = result.unwrap();
        assert!(
            db_response.is_some(),
            "Database should exist after creation"
        );

        let db = db_response.unwrap();
        assert_eq!(db.database.name, db_name);
        assert_eq!(db.database.id, db_id);
        assert_eq!(db.database.tenant_id, tenant_name);
    }

    #[tokio::test]
    #[ignore] // Requires Tilt running
    async fn test_create_database_idempotent() {
        let Some(backend) = setup_test_backend().await else {
            println!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
            return;
        };

        // First create a tenant
        let tenant_name = format!(
            "test_tenant_idempotent_db_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let create_tenant_req = CreateTenantRequest {
            name: tenant_name.clone(),
        };
        let result = backend.create_tenant(&create_tenant_req).await;
        assert!(
            result.is_ok(),
            "Failed to create tenant: {:?}",
            result.err()
        );

        // Create database first time
        let db_id = Uuid::new_v4().to_string();
        let db_name = format!(
            "test_database_idempotent_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let create_db_req = CreateDatabaseRequest {
            id: db_id.clone(),
            name: db_name.clone(),
            tenant: tenant_name.clone(),
        };
        let result1 = backend.create_database(&create_db_req).await;
        assert!(
            result1.is_ok(),
            "Failed to create database first time: {:?}",
            result1.err()
        );

        // Create database second time (should succeed - idempotent)
        let result2 = backend.create_database(&create_db_req).await;
        assert!(
            result2.is_ok(),
            "Failed to create database second time (should be idempotent): {:?}",
            result2.err()
        );

        // Verify database exists
        let get_db_req = GetDatabaseRequest {
            name: db_name.clone(),
            tenant: tenant_name.clone(),
        };
        let result = backend.get_database(&get_db_req).await;
        assert!(
            result.is_ok(),
            "Failed to get database: {:?}",
            result.err()
        );
        assert!(result.unwrap().is_some(), "Database should exist");
    }

    #[tokio::test]
    #[ignore] // Requires Tilt running
    async fn test_get_nonexistent_database() {
        let Some(backend) = setup_test_backend().await else {
            println!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
            return;
        };

        // First create a tenant
        let tenant_name = format!(
            "test_tenant_nonexistent_db_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let create_tenant_req = CreateTenantRequest {
            name: tenant_name.clone(),
        };
        let result = backend.create_tenant(&create_tenant_req).await;
        assert!(
            result.is_ok(),
            "Failed to create tenant: {:?}",
            result.err()
        );

        let db_name = format!(
            "nonexistent_database_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let get_db_req = GetDatabaseRequest {
            name: db_name,
            tenant: tenant_name,
        };
        let result = backend.get_database(&get_db_req).await;
        assert!(
            result.is_ok(),
            "Getting nonexistent database should not error"
        );
        assert!(
            result.unwrap().is_none(),
            "Nonexistent database should return None"
        );
    }

    #[tokio::test]
    #[ignore] // Requires Tilt running
    async fn test_create_database_invalid_tenant() {
        let Some(backend) = setup_test_backend().await else {
            println!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
            return;
        };

        let db_id = Uuid::new_v4().to_string();
        let db_name = format!(
            "test_database_invalid_tenant_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let create_db_req = CreateDatabaseRequest {
            id: db_id,
            name: db_name,
            tenant: "nonexistent_tenant".to_string(),
        };
        let result = backend.create_database(&create_db_req).await;
        assert!(
            result.is_err(),
            "Creating database with nonexistent tenant should fail"
        );
        match result.unwrap_err() {
            SysDbError::NotFound(_) => {
                // Expected error
            }
            e => panic!("Expected NotFound error, got: {:?}", e),
        }
    }
}
