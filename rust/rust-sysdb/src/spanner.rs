//! Spanner backend implementation.
//!
//! This module provides the `SpannerBackend` which implements all SysDb
//! operations using Google Cloud Spanner as the underlying database.

use chroma_config::{registry::Registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use google_cloud_gax::conn::Environment;
use google_cloud_spanner::client::{Client, ClientConfig};
use google_cloud_spanner::statement::Statement;
use thiserror::Error;

use crate::config::SpannerConfig;
use crate::types::{
    CreateDatabaseRequest, CreateDatabaseResponse, CreateTenantRequest, CreateTenantResponse,
    GetDatabaseRequest, GetDatabaseResponse, GetTenantRequest, GetTenantResponse,
    SetTenantResourceNameRequest, SetTenantResourceNameResponse,
};
use chroma_types::sysdb_errors::SysDbError;
use chroma_types::{Database, Tenant};

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
        let tenant_id = req.id.to_string();

        // Use a read-write transaction to atomically check and insert
        self.client
            .read_write_transaction::<(), SysDbError, _>(|tx| {
                let tenant_id = tenant_id.clone();
                Box::pin(async move {
                    // Check if tenant already exists
                    let mut check_stmt = Statement::new(
                        "SELECT id FROM tenants WHERE id = @id AND is_deleted = FALSE",
                    );
                    check_stmt.add_param("id", &tenant_id);

                    let mut iter = tx.query(check_stmt).await?;

                    // If tenant doesn't exist, insert it otherwise ignore for idempotency
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
            .await?;

        Ok(CreateTenantResponse {})
    }

    /// Get a tenant by name.
    ///
    /// Returns `SysDbError::NotFound` if the tenant does not exist or is marked as deleted.
    pub async fn get_tenant(
        &self,
        req: &GetTenantRequest,
    ) -> Result<GetTenantResponse, SysDbError> {
        let tenant_id = req.id.to_string();

        let mut stmt = Statement::new(
            "SELECT id, resource_name, UNIX_SECONDS(last_compaction_time) as last_compaction_time FROM tenants WHERE id = @id AND is_deleted = FALSE",
        );
        stmt.add_param("id", &tenant_id);

        let mut tx = self.client.single().await?;

        let mut iter = tx.query(stmt).await?;

        // Get the first row if it exists
        if let Some(row) = iter.next().await? {
            Ok(GetTenantResponse {
                tenant: Tenant::try_from(row)?,
            })
        } else {
            Err(SysDbError::NotFound(format!(
                "tenant '{}' not found",
                tenant_id
            )))
        }
    }

    /// Set the resource name for a tenant.
    ///
    /// Only sets if resource_name is currently NULL.
    pub async fn set_tenant_resource_name(
        &self,
        _req: &SetTenantResourceNameRequest,
    ) -> Result<SetTenantResourceNameResponse, SysDbError> {
        todo!("implement set_tenant_resource_name")
    }

    // ============================================================
    // Database Operations
    // ============================================================

    /// Create a new database.
    ///
    /// Validates that the database name is not empty and that the tenant exists.
    /// Uses commit timestamps for created_at and updated_at.
    /// All checks and the insert are done atomically in a single transaction.
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

        // Use a read-write transaction to atomically check tenant, check database, and insert
        let tenant_id = req.tenant_id.to_string();
        let db_id = req.id.to_string();
        let db_name = req.name.clone();

        let result = self
            .client
            .read_write_transaction::<(), SysDbError, _>(|tx| {
                let tenant_id = tenant_id.clone();
                let db_id = db_id.clone();
                let db_name = db_name.clone();
                Box::pin(async move {
                    // Check if tenant exists within the same transaction
                    let mut tenant_check_stmt = Statement::new(
                        "SELECT id FROM tenants WHERE id = @id AND is_deleted = FALSE",
                    );
                    tenant_check_stmt.add_param("id", &tenant_id);

                    let mut tenant_iter = tx.query(tenant_check_stmt).await?;
                    if tenant_iter.next().await?.is_none() {
                        return Err(SysDbError::NotFound(format!(
                            "tenant '{}' not found",
                            tenant_id
                        )));
                    }

                    // Check if database with this (name, tenant_id) combination already exists
                    let mut name_check_stmt = Statement::new(
                        "SELECT id FROM databases WHERE name = @name AND tenant_id = @tenant_id AND is_deleted = FALSE",
                    );
                    name_check_stmt.add_param("name", &db_name);
                    name_check_stmt.add_param("tenant_id", &tenant_id);

                    let mut name_iter = tx.query(name_check_stmt).await?;
                    if name_iter.next().await?.is_some() {
                        return Err(SysDbError::AlreadyExists(format!(
                            "database with name '{}' already exists for tenant '{}'",
                            db_name, tenant_id
                        )));
                    }

                    // Check if database with this ID already exists
                    let mut check_stmt = Statement::new(
                        "SELECT id FROM databases WHERE id = @id AND is_deleted = FALSE",
                    );
                    check_stmt.add_param("id", &db_id);

                    let mut iter = tx.query(check_stmt).await?;
                    if iter.next().await?.is_some() {
                        return Err(SysDbError::AlreadyExists(format!(
                            "database with id '{}' already exists",
                            db_id
                        )));
                    }

                    // Insert the new database
                    let mut insert_stmt = Statement::new(
                        "INSERT INTO databases (id, name, tenant_id, is_deleted, created_at, updated_at) VALUES (@id, @name, @tenant_id, @is_deleted, PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP())",
                    );
                    insert_stmt.add_param("id", &db_id);
                    insert_stmt.add_param("name", &db_name);
                    insert_stmt.add_param("tenant_id", &tenant_id);
                    insert_stmt.add_param("is_deleted", &false);

                    tx.update(insert_stmt).await?;
                    tracing::info!("Created database: {} for tenant: {}", db_name, tenant_id);

                    Ok(())
                })
            })
            .await;

        match result {
            Ok((_, _)) => Ok(CreateDatabaseResponse {}),
            Err(e) => Err(e),
        }
    }

    /// Get a database by name and tenant.
    ///
    /// Returns `SysDbError::NotFound` if the database does not exist or is marked as deleted.
    pub async fn get_database(
        &self,
        req: &GetDatabaseRequest,
    ) -> Result<GetDatabaseResponse, SysDbError> {
        let mut stmt = Statement::new(
            "SELECT id, name, tenant_id FROM databases WHERE name = @name AND tenant_id = @tenant_id AND is_deleted = FALSE",
        );
        stmt.add_param("name", &req.name);
        stmt.add_param("tenant_id", &req.tenant_id.to_string());

        let mut tx = self.client.single().await?;

        let mut iter = tx.query(stmt).await?;

        // Get the first row if it exists
        if let Some(row) = iter.next().await? {
            Ok(GetDatabaseResponse {
                database: Database::try_from(row)?,
            })
        } else {
            Err(SysDbError::NotFound(format!(
                "database '{}' not found for tenant '{}'",
                req.name, req.tenant_id
            )))
        }
    }

    pub async fn list_databases(
        &self,
        _tenant: &str,
        _limit: Option<i32>,
        _offset: i32,
    ) -> Result<Vec<chroma_types::chroma_proto::Database>, SysDbError> {
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
    async fn test_k8s_integration_create_and_get_tenant() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let tenant_id = Uuid::new_v4();

        // Test create_tenant
        let create_req = CreateTenantRequest { id: tenant_id };
        let result = backend.create_tenant(&create_req).await;
        assert!(
            result.is_ok(),
            "Failed to create tenant: {:?}",
            result.err()
        );

        // Test get_tenant
        let get_req = GetTenantRequest { id: tenant_id };
        let result = backend.get_tenant(&get_req).await;
        assert!(result.is_ok(), "Failed to get tenant: {:?}", result.err());

        let tenant = result.unwrap();
        assert_eq!(tenant.tenant.id, tenant_id.to_string());
        assert_eq!(tenant.tenant.last_compaction_time, 0);
        assert!(tenant.tenant.resource_name.is_none());
    }

    #[tokio::test]
    async fn test_k8s_integration_create_tenant_idempotent() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let tenant_id = Uuid::new_v4();

        // Create tenant first time
        let create_req = CreateTenantRequest { id: tenant_id };
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
        let get_req = GetTenantRequest { id: tenant_id };
        let result = backend.get_tenant(&get_req).await;
        assert!(result.is_ok(), "Failed to get tenant: {:?}", result.err());
        let tenant = result.unwrap(); // Tenant should exist
        assert_eq!(tenant.tenant.id, tenant_id.to_string());
        assert_eq!(tenant.tenant.last_compaction_time, 0);
        assert!(tenant.tenant.resource_name.is_none());
    }

    #[tokio::test]
    async fn test_k8s_integration_get_nonexistent_tenant() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let tenant_id = Uuid::new_v4();
        let get_req = GetTenantRequest { id: tenant_id };
        let result = backend.get_tenant(&get_req).await;
        assert!(
            result.is_err(),
            "Getting nonexistent tenant should return error"
        );
        match result.unwrap_err() {
            SysDbError::NotFound(_) => {}
            e => panic!("Expected NotFound error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_create_and_get_database() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // First create a tenant
        let tenant_id = Uuid::new_v4();
        let create_tenant_req = CreateTenantRequest { id: tenant_id };
        let result = backend.create_tenant(&create_tenant_req).await;
        assert!(
            result.is_ok(),
            "Failed to create tenant: {:?}",
            result.err()
        );

        // Now create a database
        let db_id = Uuid::new_v4();
        let db_name = format!(
            "test_database_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let create_db_req = CreateDatabaseRequest {
            id: db_id,
            name: db_name.clone(),
            tenant_id,
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
            tenant_id,
        };
        let result = backend.get_database(&get_db_req).await;
        assert!(result.is_ok(), "Failed to get database: {:?}", result.err());

        let db = result.unwrap();
        assert_eq!(db.database.name, db_name);
        assert_eq!(db.database.id, db_id);
        assert_eq!(db.database.tenant, tenant_id.to_string());
    }

    #[tokio::test]
    async fn test_k8s_integration_create_database_conflict() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // First create a tenant
        let tenant_id = Uuid::new_v4();
        let create_tenant_req = CreateTenantRequest { id: tenant_id };
        let result = backend.create_tenant(&create_tenant_req).await;
        assert!(
            result.is_ok(),
            "Failed to create tenant: {:?}",
            result.err()
        );

        // Create database first time
        let db_id = Uuid::new_v4();
        let db_name = format!(
            "test_database_conflict_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let create_db_req = CreateDatabaseRequest {
            id: db_id,
            name: db_name.clone(),
            tenant_id,
        };
        let result1 = backend.create_database(&create_db_req).await;
        assert!(
            result1.is_ok(),
            "Failed to create database first time: {:?}",
            result1.err()
        );

        // Create database second time with same ID (should return AlreadyExists)
        let result2 = backend.create_database(&create_db_req).await;
        assert!(
            result2.is_err(),
            "Creating database with duplicate ID should return error"
        );
        match result2.unwrap_err() {
            SysDbError::AlreadyExists(_) => {
                // Expected error
            }
            e => panic!("Expected AlreadyExists error, got: {:?}", e),
        }

        // Verify database still exists
        let get_db_req = GetDatabaseRequest {
            name: db_name.clone(),
            tenant_id,
        };
        let result = backend.get_database(&get_db_req).await;
        assert!(result.is_ok(), "Failed to get database: {:?}", result.err());
        let db = result.unwrap(); // Database should exist
        assert_eq!(db.database.id, db_id);
        assert_eq!(db.database.name, db_name);
        assert_eq!(db.database.tenant, tenant_id.to_string());
    }

    #[tokio::test]
    async fn test_k8s_integration_get_nonexistent_database() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // First create a tenant
        let tenant_id = Uuid::new_v4();
        let create_tenant_req = CreateTenantRequest { id: tenant_id };
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
            tenant_id,
        };
        let result = backend.get_database(&get_db_req).await;
        assert!(
            result.is_err(),
            "Getting nonexistent database should return error"
        );
        match result.unwrap_err() {
            SysDbError::NotFound(_) => {}
            e => panic!("Expected NotFound error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_create_database_invalid_tenant() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let db_id = Uuid::new_v4();
        let db_name = format!(
            "test_database_invalid_tenant_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        let tenant_id = Uuid::new_v4();

        let create_db_req = CreateDatabaseRequest {
            id: db_id,
            name: db_name.clone(),
            tenant_id,
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

    #[tokio::test]
    async fn test_k8s_integration_create_database_empty_name() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let tenant_id = Uuid::new_v4();
        // First create a tenant
        let create_tenant_req = CreateTenantRequest { id: tenant_id };
        let result = backend.create_tenant(&create_tenant_req).await;
        assert!(
            result.is_ok(),
            "Failed to create tenant: {:?}",
            result.err()
        );

        // Try to create database with empty name
        let db_id = Uuid::new_v4();
        let create_db_req = CreateDatabaseRequest {
            id: db_id,
            name: "".to_string(), // Empty name
            tenant_id,
        };
        let result = backend.create_database(&create_db_req).await;
        assert!(
            result.is_err(),
            "Creating database with empty name should fail"
        );
        match result.unwrap_err() {
            SysDbError::InvalidArgument(_) => {
                // Expected error
            }
            e => panic!("Expected InvalidArgument error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_create_database_duplicate_name_tenant() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // First create a tenant
        let tenant_id = Uuid::new_v4();
        let create_tenant_req = CreateTenantRequest { id: tenant_id };
        let result = backend.create_tenant(&create_tenant_req).await;
        assert!(
            result.is_ok(),
            "Failed to create tenant: {:?}",
            result.err()
        );

        // Create database first time
        let db_id1 = Uuid::new_v4();
        let db_name = format!(
            "test_database_dup_name_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let create_db_req1 = CreateDatabaseRequest {
            id: db_id1,
            name: db_name.clone(),
            tenant_id,
        };
        let result1 = backend.create_database(&create_db_req1).await;
        assert!(
            result1.is_ok(),
            "Failed to create database first time: {:?}",
            result1.err()
        );

        // Try to create database second time with same (name, tenant_id) but different ID
        // (should return AlreadyExists)
        let db_id2 = Uuid::new_v4();
        let create_db_req2 = CreateDatabaseRequest {
            id: db_id2,
            name: db_name.clone(),
            tenant_id,
        };
        let result2 = backend.create_database(&create_db_req2).await;
        assert!(
            result2.is_err(),
            "Creating database with duplicate (name, tenant_id) should return error"
        );
        match result2.unwrap_err() {
            SysDbError::AlreadyExists(msg) => {
                assert!(
                    msg.contains(&db_name) && msg.contains(&tenant_id.to_string()),
                    "Error message should mention database name and tenant: {}",
                    msg
                );
            }
            e => panic!("Expected AlreadyExists error, got: {:?}", e),
        }

        // Verify original database still exists
        let get_db_req = GetDatabaseRequest {
            name: db_name.clone(),
            tenant_id,
        };
        let result = backend.get_database(&get_db_req).await;
        assert!(result.is_ok(), "Failed to get database: {:?}", result.err());
        let db = result.unwrap();
        assert_eq!(db.database.id, db_id1);
        assert_eq!(db.database.name, db_name);
        assert_eq!(db.database.tenant, tenant_id.to_string());
    }
}
