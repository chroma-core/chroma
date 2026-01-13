//! Spanner backend implementation.
//!
//! This module provides the `SpannerBackend` which implements all SysDb
//! operations using Google Cloud Spanner as the underlying database.

use chroma_config::{registry::Registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use google_cloud_gax::conn::Environment;
use google_cloud_spanner::client::{Client, ClientConfig};
use google_cloud_spanner::mutation::insert;
use google_cloud_spanner::statement::Statement;
use thiserror::Error;
use uuid::Uuid;

use crate::config::SpannerConfig;
use crate::types::{
    CreateCollectionRequest, CreateCollectionResponse, CreateDatabaseRequest,
    CreateDatabaseResponse, CreateTenantRequest, CreateTenantResponse, GetDatabaseRequest,
    GetDatabaseResponse, GetTenantRequest, GetTenantResponse, SetTenantResourceNameRequest,
    SetTenantResourceNameResponse, SpannerRow,
};
use crate::types::{SpannerRows, SysDbError};
use chroma_types::{Collection, Database, DatabaseUuid, InternalCollectionConfiguration, Tenant};

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
        req: CreateTenantRequest,
    ) -> Result<CreateTenantResponse, SysDbError> {
        // Use a read-write transaction to atomically check and insert
        self.client
            .read_write_transaction::<(), SysDbError, _>(|tx| {
                let tenant_id = req.id.clone();
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
    pub async fn get_tenant(&self, req: GetTenantRequest) -> Result<GetTenantResponse, SysDbError> {
        let mut stmt = Statement::new(
            "SELECT id, resource_name, UNIX_SECONDS(last_compaction_time) as last_compaction_time FROM tenants WHERE id = @id AND is_deleted = FALSE",
        );
        stmt.add_param("id", &req.id);

        let mut tx = self.client.single().await?;

        let mut iter = tx.query(stmt).await?;

        // Get the first row if it exists
        if let Some(row) = iter.next().await? {
            Ok(GetTenantResponse {
                tenant: Tenant::try_from(SpannerRow { row })?,
            })
        } else {
            Err(SysDbError::NotFound(format!(
                "tenant '{}' not found",
                req.id
            )))
        }
    }

    /// Set the resource name for a tenant.
    ///
    /// Only sets if resource_name is currently NULL.
    pub async fn set_tenant_resource_name(
        &self,
        _req: SetTenantResourceNameRequest,
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
        req: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse, SysDbError> {
        // Validate database name is not empty
        if req.name.is_empty() {
            return Err(SysDbError::InvalidArgument(
                "database name cannot be empty".to_string(),
            ));
        }

        // Use a read-write transaction to atomically check tenant, check database, and insert
        let result = self
            .client
            .read_write_transaction::<(), SysDbError, _>(|tx| {
                let tenant_id = req.tenant_id.clone();
                let db_id = req.id.to_string();
                let db_name = req.name.to_string();
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
        req: GetDatabaseRequest,
    ) -> Result<GetDatabaseResponse, SysDbError> {
        let mut stmt = Statement::new(
            "SELECT id, name, tenant_id FROM databases WHERE name = @name AND tenant_id = @tenant_id AND is_deleted = FALSE",
        );
        stmt.add_param("name", &req.name);
        stmt.add_param("tenant_id", &req.tenant_id);

        let mut tx = self.client.single().await?;

        let mut iter = tx.query(stmt).await?;

        // Get the first row if it exists
        if let Some(row) = iter.next().await? {
            Ok(GetDatabaseResponse {
                database: Database::try_from(SpannerRow { row })?,
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
    // Collection Operations
    // ============================================================

    pub async fn create_collection(
        &self,
        req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse, SysDbError> {
        // Validate collection name is not empty
        if req.name.is_empty() {
            return Err(SysDbError::InvalidArgument(
                "collection name cannot be empty".to_string(),
            ));
        }

        // Destructure req to take ownership of fields (avoids cloning)
        let CreateCollectionRequest {
            id,
            name: collection_name,
            dimension,
            index_schema,
            segments,
            metadata,
            get_or_create,
            tenant_id,
            database_name,
        } = req;

        // Regions for compaction cursors
        // TODO(Sanket): Extract the topo name from db name and use the config
        // to get the regions.
        const REGIONS: &[&str] = &["us", "asia", "europe"];

        // TRADEOFF: We use mutations with buffer_write instead of DML for inserts.
        //
        // Mutations (buffer_write):
        // - Pros: All writes batched and applied atomically at commit, more efficient
        // - Cons: Cannot read our own writes within the transaction (data not visible until commit)
        //
        // DML (tx.update with INSERT statements):
        // - Pros: Can read our own writes, can use PENDING_COMMIT_TIMESTAMP()
        // - Cons: Each statement is a separate round-trip, slower for many inserts
        //
        // For create_collection, we use mutations because:
        // 1. We have 13+ inserts (1 collection + 3 cursors + 9 segments + N metadata)
        // 2. We can build the response from the request data since we know exactly what we inserted
        // 3. The only difference is timestamps, which we approximate with Utc::now()
        // 4. Collection creation is infrequent, but we still prefer the simpler/faster approach
        //
        // Note: For get_or_create when collection exists, we DO read from DB (via fetch_collection_in_tx)
        // since we need the actual stored data, not what we're trying to insert.

        // Transaction returns (Collection, bool) where bool = was_created
        let result = self
            .client
            .read_write_transaction::<(Collection, bool), SysDbError, _>(|tx| {
                // Clone for the closure (needed because closure may be called multiple times for retries)
                let collection_id_uuid = id;
                let collection_id = id.0.to_string();
                let tenant_id_str = tenant_id.clone();
                let collection_name = collection_name.clone();
                let database_name = database_name.clone();
                let dimension_i64 = dimension.map(|d| d as i64);
                let segments = segments.clone();
                let metadata = metadata.clone();
                let index_schema = index_schema.clone();
                Box::pin(async move {
                    let index_schema_json = serde_json::to_string(&index_schema)?;
                    // Check if database exists and get database_id
                    let mut db_stmt = Statement::new(
                        "SELECT id FROM databases WHERE name = @name AND tenant_id = @tenant_id AND is_deleted = FALSE",
                    );
                    db_stmt.add_param("name", &database_name);
                    db_stmt.add_param("tenant_id", &tenant_id_str);

                    let mut db_iter = tx.query(db_stmt).await?;
                    let db_row = db_iter.next().await?;
                    let database_id = match db_row {
                        Some(row) => {
                            let db_id_str: String = row
                                .column_by_name("id")
                                .map_err(SysDbError::FailedToReadColumn)?;
                            db_id_str
                        }
                        None => {
                            return Err(SysDbError::NotFound(format!(
                                "database '{}' not found for tenant '{}' for collection '{}'",
                                database_name, tenant_id_str, collection_name
                            )));
                        }
                    };

                    // Check if collection with this ID already exists
                    let mut id_check_stmt = Statement::new(
                        "SELECT collection_id FROM collections WHERE collection_id = @collection_id AND is_deleted = FALSE",
                    );
                    id_check_stmt.add_param("collection_id", &collection_id);

                    let mut id_iter = tx.query(id_check_stmt).await?;
                    if id_iter.next().await?.is_some() {
                        if get_or_create {
                            // Return the existing collection
                            // TODO(Sanket): Use the local region here.
                            let fetched_collection =
                                Self::fetch_collection_in_tx(&mut *tx, &collection_id, REGIONS[0]).await?;
                            return Ok((fetched_collection, false)); // false = not created
                        } else {
                            return Err(SysDbError::AlreadyExists(format!(
                                "collection with id '{}' already exists",
                                collection_id
                            )));
                        }
                    }

                    // Check if collection with same name exists in this database
                    let mut check_stmt = Statement::new(
                        "SELECT collection_id, name, dimension, database_id, database_name, tenant_id FROM collections WHERE tenant_id = @tenant_id AND database_name = @database_name AND name = @name AND is_deleted = FALSE",
                    );
                    check_stmt.add_param("tenant_id", &tenant_id_str);
                    check_stmt.add_param("database_name", &database_name);
                    check_stmt.add_param("name", &collection_name);

                    let mut check_iter = tx.query(check_stmt).await?;
                    if let Some(existing_row) = check_iter.next().await? {
                        // Collection with same name exists
                        if get_or_create {
                            // Fetch the collection inside the transaction to avoid TOCTOU
                            let existing_collection_id: String = existing_row
                                .column_by_name("collection_id")
                                .map_err(SysDbError::FailedToReadColumn)?;
                            // TODO(Sanket): Use the local region here.
                            let fetched_collection =
                                Self::fetch_collection_in_tx(&mut *tx, &existing_collection_id, REGIONS[0]).await?;
                            return Ok((fetched_collection, false)); // false = not created
                        } else {
                            return Err(SysDbError::AlreadyExists(format!(
                                "collection with name '{}' already exists in database '{}'",
                                collection_name, database_id
                            )));
                        }
                    }

                    let now_timestamp_us = chrono::Utc::now().timestamp_micros();
                    let mut mutations = Vec::new();

                    // Insert the collection
                    mutations.push(insert(
                        "collections",
                        &[
                            "collection_id",
                            "name",
                            "dimension",
                            "database_id",
                            "database_name",
                            "tenant_id",
                            "is_deleted",
                            "created_at",
                            "updated_at",
                        ],
                        &[
                            &collection_id,
                            &collection_name,
                            &dimension_i64,
                            &database_id,
                            &database_name,
                            &tenant_id_str,
                            &false,
                            &now_timestamp_us,
                            &now_timestamp_us,
                        ],
                    ));

                    // Insert compaction cursors for each region
                    for region in REGIONS {
                        mutations.push(insert(
                            "collection_compaction_cursors",
                            &[
                                "collection_id",
                                "region",
                                "index_schema",
                                "created_at",
                                "updated_at",
                            ],
                            &[
                                &collection_id,
                                region,
                                &index_schema_json,
                                &now_timestamp_us,
                                &now_timestamp_us,
                            ],
                        ));
                    }

                    // Insert segments (3 segments × 3 regions = 9 rows)
                    for segment in &segments {
                        let segment_id_str = segment.id.0.to_string();
                        let segment_type_str: String = segment.r#type.into();
                        let segment_scope_str: String = segment.scope.clone().into();
                        let file_paths_json: Option<String> = if segment.file_path.is_empty() {
                            None
                        } else {
                            Some(
                                serde_json::to_string(&segment.file_path)
                                    .map_err(SysDbError::InvalidSchemaJson)?,
                            )
                        };

                        for region in REGIONS {
                            mutations.push(insert(
                                "collection_segments",
                                &[
                                    "collection_id",
                                    "region",
                                    "id",
                                    "type",
                                    "scope",
                                    "is_deleted",
                                    "created_at",
                                    "updated_at",
                                    "file_paths",
                                ],
                                &[
                                    &collection_id,
                                    region,
                                    &segment_id_str,
                                    &segment_type_str,
                                    &segment_scope_str,
                                    &false,
                                    &now_timestamp_us,
                                    &now_timestamp_us,
                                    &file_paths_json,
                                ],
                            ));
                        }
                    }

                    // Insert metadata if provided
                    if let Some(ref meta) = metadata {
                        for (key, value) in meta.iter() {
                            let (str_val, int_val, float_val, bool_val): (Option<&str>, Option<i64>, Option<f64>, Option<bool>) = match value {
                                chroma_types::MetadataValue::Str(s) => (Some(s.as_str()), None, None, None),
                                chroma_types::MetadataValue::Int(i) => (None, Some(*i), None, None),
                                chroma_types::MetadataValue::Float(f) => (None, None, Some(*f), None),
                                chroma_types::MetadataValue::Bool(b) => (None, None, None, Some(*b)),
                                chroma_types::MetadataValue::SparseVector(_) => continue, // Not supported
                            };

                            mutations.push(insert(
                                "collection_metadata",
                                &[
                                    "collection_id",
                                    "key",
                                    "str_value",
                                    "int_value",
                                    "float_value",
                                    "bool_value",
                                    "created_at",
                                    "updated_at",
                                ],
                                &[
                                    &collection_id,
                                    key,
                                    &str_val,
                                    &int_val,
                                    &float_val,
                                    &bool_val,
                                    &now_timestamp_us,
                                    &now_timestamp_us,
                                ],
                            ));
                        }
                    }

                    // Buffer all mutations - they will be applied atomically at commit
                    tx.buffer_write(mutations);

                    tracing::info!(
                        "Created collection: {} (id: {}) in database: {} for tenant: {}",
                        collection_name,
                        collection_id,
                        database_name,
                        tenant_id_str
                    );

                    // Build the Collection object inside the transaction
                    let collection = Collection {
                        collection_id: collection_id_uuid,
                        name: collection_name,
                        config: InternalCollectionConfiguration::default_hnsw(),
                        schema: Some(index_schema),
                        metadata,
                        dimension: dimension.map(|d| d as i32),
                        tenant: tenant_id_str,
                        database: database_name,
                        log_position: 0,
                        version: 0,
                        total_records_post_compaction: 0,
                        size_bytes_post_compaction: 0,
                        last_compaction_time_secs: 0,
                        version_file_path: None,
                        root_collection_id: None,
                        lineage_file_path: None,
                        updated_at: std::time::SystemTime::now(),
                        database_id: DatabaseUuid(
                            Uuid::parse_str(&database_id).map_err(SysDbError::InvalidUuid)?,
                        ),
                        compaction_failure_count: 0,
                    };

                    Ok((collection, true)) // true = was created
                })
            })
            .await?;

        // Result is (CommitResult, (Collection, bool))
        let (_commit_result, (collection, created)) = result;

        Ok(CreateCollectionResponse {
            collection,
            created,
        })
    }

    /// Fetch a collection from the database within a transaction.
    ///
    /// Uses a JOIN query to get collection, metadata, and compaction cursor fields.
    /// The rows are converted to Collection via TryFrom<Vec<Row>>.
    async fn fetch_collection_in_tx(
        tx: &mut google_cloud_spanner::transaction_rw::ReadWriteTransaction,
        collection_id: &str,
        region: &str,
    ) -> Result<Collection, SysDbError> {
        // 3-way LEFT JOIN to get collection, metadata, and compaction cursor fields
        let mut fetch_stmt = Statement::new(
            r#"
            SELECT 
                c.collection_id,
                c.name,
                c.dimension,
                c.database_id,
                c.database_name,
                c.tenant_id,
                c.updated_at,
                cm.key as metadata_key,
                cm.str_value as metadata_str_value,
                cm.int_value as metadata_int_value,
                cm.float_value as metadata_float_value,
                cm.bool_value as metadata_bool_value,
                cursors.last_compacted_offset,
                cursors.version,
                cursors.total_records_post_compaction,
                cursors.size_bytes_post_compaction,
                cursors.last_compaction_time_secs,
                cursors.version_file_name,
                cursors.index_schema,
                cursors.compaction_failure_count
            FROM collections c
            LEFT JOIN collection_metadata cm ON cm.collection_id = c.collection_id
            LEFT JOIN collection_compaction_cursors cursors 
                ON cursors.collection_id = c.collection_id AND cursors.region = @region
            WHERE c.collection_id = @collection_id
            "#,
        );
        fetch_stmt.add_param("collection_id", &collection_id);
        fetch_stmt.add_param("region", &region);

        let mut fetch_iter = tx.query(fetch_stmt).await?;

        // Collect all rows and convert to Collection using TryFrom<Vec<Row>>
        let mut rows = Vec::new();
        while let Some(row) = fetch_iter.next().await? {
            rows.push(row);
        }

        Collection::try_from(SpannerRows { rows })
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
        CreateCollectionRequest, CreateDatabaseRequest, CreateTenantRequest, GetDatabaseRequest,
        GetTenantRequest,
    };
    use chroma_types::{CollectionUuid, Schema, Segment, SegmentScope, SegmentType, SegmentUuid};
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
            database: "local-sysdb-database".to_string(),
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
    async fn test_k8s_mcmr_integration_create_and_get_tenant() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let tenant_id = Uuid::new_v4().to_string();

        // Test create_tenant
        let create_req = CreateTenantRequest {
            id: tenant_id.clone(),
        };
        let result = backend.create_tenant(create_req.clone()).await;
        assert!(
            result.is_ok(),
            "Failed to create tenant: {:?}",
            result.err()
        );

        // Test get_tenant
        let get_req = GetTenantRequest {
            id: tenant_id.clone(),
        };
        let result = backend.get_tenant(get_req.clone()).await;
        assert!(result.is_ok(), "Failed to get tenant: {:?}", result.err());

        let tenant = result.unwrap();
        assert_eq!(tenant.tenant.id, tenant_id);
        assert_eq!(tenant.tenant.last_compaction_time, 0);
        assert!(tenant.tenant.resource_name.is_none());
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_create_tenant_idempotent() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let tenant_id = Uuid::new_v4().to_string();

        // Create tenant first time
        let create_req = CreateTenantRequest {
            id: tenant_id.clone(),
        };
        let result1 = backend.create_tenant(create_req.clone()).await;
        assert!(
            result1.is_ok(),
            "Failed to create tenant first time: {:?}",
            result1.err()
        );

        // Create tenant second time (should succeed - idempotent)
        let result2 = backend.create_tenant(create_req.clone()).await;
        assert!(
            result2.is_ok(),
            "Failed to create tenant second time (should be idempotent): {:?}",
            result2.err()
        );

        // Verify tenant exists
        let get_req = GetTenantRequest {
            id: tenant_id.clone(),
        };
        let result = backend.get_tenant(get_req.clone()).await;
        assert!(result.is_ok(), "Failed to get tenant: {:?}", result.err());
        let tenant = result.unwrap(); // Tenant should exist
        assert_eq!(tenant.tenant.id, tenant_id);
        assert_eq!(tenant.tenant.last_compaction_time, 0);
        assert!(tenant.tenant.resource_name.is_none());
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_nonexistent_tenant() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let tenant_id = Uuid::new_v4().to_string();
        let get_req = GetTenantRequest { id: tenant_id };
        let result = backend.get_tenant(get_req.clone()).await;
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
    async fn test_k8s_mcmr_integration_create_and_get_database() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // First create a tenant
        let tenant_id = Uuid::new_v4().to_string();
        let create_tenant_req = CreateTenantRequest {
            id: tenant_id.clone(),
        };
        let result = backend.create_tenant(create_tenant_req.clone()).await;
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
            tenant_id: tenant_id.clone(),
        };
        let result = backend.create_database(create_db_req.clone()).await;
        assert!(
            result.is_ok(),
            "Failed to create database: {:?}",
            result.err()
        );

        // Test get_database
        let get_db_req = GetDatabaseRequest {
            name: db_name.clone(),
            tenant_id: tenant_id.clone(),
        };
        let result = backend.get_database(get_db_req.clone()).await;
        assert!(result.is_ok(), "Failed to get database: {:?}", result.err());

        let db = result.unwrap();
        assert_eq!(db.database.name, db_name);
        assert_eq!(db.database.id, db_id);
        assert_eq!(db.database.tenant, tenant_id);
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_create_database_conflict() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // First create a tenant
        let tenant_id = Uuid::new_v4().to_string();
        let create_tenant_req = CreateTenantRequest {
            id: tenant_id.clone(),
        };
        let result = backend.create_tenant(create_tenant_req.clone()).await;
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
            tenant_id: tenant_id.clone(),
        };
        let result1 = backend.create_database(create_db_req.clone()).await;
        assert!(
            result1.is_ok(),
            "Failed to create database first time: {:?}",
            result1.err()
        );

        // Create database second time with same ID (should return AlreadyExists)
        let result2 = backend.create_database(create_db_req.clone()).await;
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
            tenant_id: tenant_id.clone(),
        };
        let result = backend.get_database(get_db_req.clone()).await;
        assert!(result.is_ok(), "Failed to get database: {:?}", result.err());
        let db = result.unwrap(); // Database should exist
        assert_eq!(db.database.id, db_id);
        assert_eq!(db.database.name, db_name);
        assert_eq!(db.database.tenant, tenant_id);
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_nonexistent_database() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // First create a tenant
        let tenant_id = Uuid::new_v4().to_string();
        let create_tenant_req = CreateTenantRequest {
            id: tenant_id.clone(),
        };
        let result = backend.create_tenant(create_tenant_req.clone()).await;
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
            tenant_id: tenant_id.clone(),
        };
        let result = backend.get_database(get_db_req.clone()).await;
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
    async fn test_k8s_mcmr_integration_create_database_invalid_tenant() {
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
        let tenant_id = Uuid::new_v4().to_string();

        let create_db_req = CreateDatabaseRequest {
            id: db_id,
            name: db_name.clone(),
            tenant_id: tenant_id.clone(),
        };
        let result = backend.create_database(create_db_req.clone()).await;
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
    async fn test_k8s_mcmr_integration_create_database_empty_name() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let tenant_id = Uuid::new_v4().to_string();
        // First create a tenant
        let create_tenant_req = CreateTenantRequest {
            id: tenant_id.clone(),
        };
        let result = backend.create_tenant(create_tenant_req.clone()).await;
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
            tenant_id: tenant_id.clone(),
        };
        let result = backend.create_database(create_db_req.clone()).await;
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
        let tenant_id = Uuid::new_v4().to_string();
        let create_tenant_req = CreateTenantRequest {
            id: tenant_id.clone(),
        };
        let result = backend.create_tenant(create_tenant_req.clone()).await;
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
            tenant_id: tenant_id.clone(),
        };
        let result1 = backend.create_database(create_db_req1.clone()).await;
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
            tenant_id: tenant_id.clone(),
        };
        let result2 = backend.create_database(create_db_req2.clone()).await;
        assert!(
            result2.is_err(),
            "Creating database with duplicate (name, tenant_id) should return error"
        );
        match result2.unwrap_err() {
            SysDbError::AlreadyExists(msg) => {
                assert!(
                    msg.contains(&db_name) && msg.contains(&tenant_id),
                    "Error message should mention database name and tenant: {}",
                    msg
                );
            }
            e => panic!("Expected AlreadyExists error, got: {:?}", e),
        }

        // Verify original database still exists
        let get_db_req = GetDatabaseRequest {
            name: db_name.clone(),
            tenant_id: tenant_id.clone(),
        };
        let result = backend.get_database(get_db_req.clone()).await;
        assert!(result.is_ok(), "Failed to get database: {:?}", result.err());
        let db = result.unwrap();
        assert_eq!(db.database.id, db_id1);
        assert_eq!(db.database.name, db_name);
        assert_eq!(db.database.tenant, tenant_id);
    }

    // Helper to create a tenant and database for collection tests
    async fn setup_tenant_and_database(backend: &SpannerBackend) -> (String, String) {
        let tenant_id = Uuid::new_v4().to_string();
        let create_tenant_req = CreateTenantRequest {
            id: tenant_id.clone(),
        };
        backend
            .create_tenant(create_tenant_req)
            .await
            .expect("Failed to create tenant");

        let db_id = Uuid::new_v4();
        let db_name = format!(
            "test_db_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let create_db_req = CreateDatabaseRequest {
            id: db_id,
            name: db_name.clone(),
            tenant_id: tenant_id.clone(),
        };
        backend
            .create_database(create_db_req)
            .await
            .expect("Failed to create database");

        (tenant_id, db_name)
    }

    // Helper to create test segments for a collection
    fn create_test_segments(collection_id: CollectionUuid) -> Vec<Segment> {
        vec![
            Segment {
                id: SegmentUuid(Uuid::new_v4()),
                r#type: SegmentType::BlockfileMetadata,
                scope: SegmentScope::METADATA,
                collection: collection_id,
                metadata: None,
                file_path: std::collections::HashMap::new(),
            },
            Segment {
                id: SegmentUuid(Uuid::new_v4()),
                r#type: SegmentType::BlockfileRecord,
                scope: SegmentScope::RECORD,
                collection: collection_id,
                metadata: None,
                file_path: std::collections::HashMap::new(),
            },
            Segment {
                id: SegmentUuid(Uuid::new_v4()),
                r#type: SegmentType::HnswDistributed,
                scope: SegmentScope::VECTOR,
                collection: collection_id,
                metadata: None,
                file_path: std::collections::HashMap::new(),
            },
        ]
    }

    /// Helper to verify all fields of a newly created collection
    #[allow(clippy::too_many_arguments)]
    fn verify_new_collection(
        collection: &chroma_types::Collection,
        expected_id: CollectionUuid,
        expected_name: &str,
        expected_dimension: Option<i32>,
        expected_tenant: &str,
        expected_database: &str,
        expected_metadata: Option<&chroma_types::Metadata>,
        expected_schema: Option<&Schema>,
    ) {
        // Basic fields
        assert_eq!(
            collection.collection_id, expected_id,
            "collection_id mismatch"
        );
        assert_eq!(collection.name, expected_name, "name mismatch");
        assert_eq!(
            collection.dimension, expected_dimension,
            "dimension mismatch"
        );
        assert_eq!(collection.tenant, expected_tenant, "tenant mismatch");
        assert_eq!(collection.database, expected_database, "database mismatch");

        // Schema verification
        assert_eq!(
            collection.schema.as_ref(),
            expected_schema,
            "schema mismatch"
        );

        // Compaction cursor fields - should be 0/None for newly created collection
        assert_eq!(
            collection.log_position, 0,
            "log_position should be 0 for new collection"
        );
        assert_eq!(
            collection.version, 0,
            "version should be 0 for new collection"
        );
        assert_eq!(
            collection.total_records_post_compaction, 0,
            "total_records_post_compaction should be 0"
        );
        assert_eq!(
            collection.size_bytes_post_compaction, 0,
            "size_bytes_post_compaction should be 0"
        );
        assert_eq!(
            collection.last_compaction_time_secs, 0,
            "last_compaction_time_secs should be 0"
        );
        assert!(
            collection.version_file_path.is_none(),
            "version_file_path should be None for new collection"
        );
        assert_eq!(
            collection.compaction_failure_count, 0,
            "compaction_failure_count should be 0"
        );

        // Metadata verification
        match (expected_metadata, &collection.metadata) {
            (Some(expected), Some(actual)) => {
                assert_eq!(expected.len(), actual.len(), "metadata length mismatch");
                for (key, value) in expected {
                    assert_eq!(
                        actual.get(key),
                        Some(value),
                        "metadata key '{}' mismatch",
                        key
                    );
                }
            }
            (None, None) => {}
            (None, Some(actual)) if actual.is_empty() => {} // Empty is same as None
            _ => panic!(
                "metadata mismatch: expected {:?}, got {:?}",
                expected_metadata, collection.metadata
            ),
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_create_collection() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_collection_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let metadata: chroma_types::Metadata = [(
            "key1".to_string(),
            chroma_types::MetadataValue::Str("value1".to_string()),
        )]
        .into_iter()
        .collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(128),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: Some(metadata.clone()),
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result = backend.create_collection(create_req).await;
        assert!(
            result.is_ok(),
            "Failed to create collection: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert!(response.created, "Collection should be marked as created");

        verify_new_collection(
            &response.collection,
            collection_id,
            &collection_name,
            Some(128),
            &tenant_id,
            &db_name,
            Some(&metadata),
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_create_collection_duplicate_fails() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_collection_dup_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create collection first time
        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(128),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result1 = backend.create_collection(create_req).await;
        assert!(
            result1.is_ok(),
            "Failed to create collection first time: {:?}",
            result1.err()
        );

        let response1 = result1.unwrap();
        assert!(response1.created, "Collection should be created first time");
        verify_new_collection(
            &response1.collection,
            collection_id,
            &collection_name,
            Some(128),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );

        // Create collection second time with same name (should fail)
        let collection_id2 = CollectionUuid(Uuid::new_v4());
        let create_req2 = CreateCollectionRequest {
            id: collection_id2,
            name: collection_name.clone(),
            dimension: Some(128),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id2),
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result2 = backend.create_collection(create_req2).await;
        assert!(
            result2.is_err(),
            "Creating duplicate collection should fail"
        );
        match result2.unwrap_err() {
            SysDbError::AlreadyExists(_) => {}
            e => panic!("Expected AlreadyExists error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_create_collection_get_or_create() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_collection_goc_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create collection first time with get_or_create=true
        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(256),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: None,
            get_or_create: true,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result1 = backend.create_collection(create_req).await;
        assert!(
            result1.is_ok(),
            "Failed to create collection first time: {:?}",
            result1.err()
        );
        let response1 = result1.unwrap();
        assert!(response1.created, "Collection should be created first time");
        verify_new_collection(
            &response1.collection,
            collection_id,
            &collection_name,
            Some(256),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );

        let metadata: chroma_types::Metadata = [(
            "key1".to_string(),
            chroma_types::MetadataValue::Str("value1".to_string()),
        )]
        .into_iter()
        .collect();

        // Create collection second time with get_or_create=true (should return existing)
        let collection_id2 = CollectionUuid(Uuid::new_v4());
        let create_req2 = CreateCollectionRequest {
            id: collection_id2,
            name: collection_name.clone(),
            dimension: Some(512), // Different dimension
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id2),
            metadata: Some(metadata.clone()),
            get_or_create: true,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result2 = backend.create_collection(create_req2).await;
        assert!(
            result2.is_ok(),
            "get_or_create should succeed: {:?}",
            result2.err()
        );
        let response2 = result2.unwrap();
        assert!(
            !response2.created,
            "Collection should NOT be created second time"
        );
        // Should return the original collection with original values
        verify_new_collection(
            &response2.collection,
            collection_id,
            &collection_name,
            Some(256), // Original dimension, not 512
            &tenant_id,
            &db_name,
            None, // None and not some(metadata)
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_create_collection_empty_name() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let collection_id = CollectionUuid(Uuid::new_v4());

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: "".to_string(), // Empty name
            dimension: Some(128),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name,
        };

        let result = backend.create_collection(create_req).await;
        assert!(
            result.is_err(),
            "Creating collection with empty name should fail"
        );
        match result.unwrap_err() {
            SysDbError::InvalidArgument(_) => {}
            e => panic!("Expected InvalidArgument error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_create_collection_nonexistent_database() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // Create tenant but NOT database
        let tenant_id = Uuid::new_v4().to_string();
        let create_tenant_req = CreateTenantRequest {
            id: tenant_id.clone(),
        };
        backend
            .create_tenant(create_tenant_req)
            .await
            .expect("Failed to create tenant");

        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_collection_no_db_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name,
            dimension: Some(128),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: "nonexistent_database".to_string(),
        };

        let result = backend.create_collection(create_req).await;
        assert!(
            result.is_err(),
            "Creating collection in nonexistent database should fail"
        );
        match result.unwrap_err() {
            SysDbError::NotFound(_) => {}
            e => panic!("Expected NotFound error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_create_collection_with_metadata() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_collection_meta_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let metadata: chroma_types::Metadata = [
            (
                "str_key".to_string(),
                chroma_types::MetadataValue::Str("string_value".to_string()),
            ),
            ("int_key".to_string(), chroma_types::MetadataValue::Int(42)),
            (
                "float_key".to_string(),
                chroma_types::MetadataValue::Float(1.5),
            ),
            (
                "bool_key".to_string(),
                chroma_types::MetadataValue::Bool(true),
            ),
        ]
        .into_iter()
        .collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: None, // No dimension
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: Some(metadata.clone()),
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result = backend.create_collection(create_req).await;
        assert!(
            result.is_ok(),
            "Failed to create collection with metadata: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert!(response.created);

        verify_new_collection(
            &response.collection,
            collection_id,
            &collection_name,
            None,
            &tenant_id,
            &db_name,
            Some(&metadata),
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_create_collection_duplicate_id_fails() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name1 = format!(
            "test_collection_id1_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create collection first time
        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name1.clone(),
            dimension: Some(128),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result1 = backend.create_collection(create_req).await;
        assert!(
            result1.is_ok(),
            "Failed to create collection first time: {:?}",
            result1.err()
        );

        let response1 = result1.unwrap();
        assert!(response1.created, "Collection should be created first time");
        verify_new_collection(
            &response1.collection,
            collection_id,
            &collection_name1,
            Some(128),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );

        // Create collection second time with SAME ID but different name (should fail)
        let collection_name2 = format!(
            "test_collection_id2_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let create_req2 = CreateCollectionRequest {
            id: collection_id, // Same ID
            name: collection_name2,
            dimension: Some(256),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result2 = backend.create_collection(create_req2).await;
        assert!(
            result2.is_err(),
            "Creating collection with duplicate ID should fail"
        );
        match result2.unwrap_err() {
            SysDbError::AlreadyExists(_) => {}
            e => panic!("Expected AlreadyExists error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_create_collection_duplicate_id_get_or_create() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name1 = format!(
            "test_collection_goc_id1_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create collection first time with get_or_create=true
        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name1.clone(),
            dimension: Some(128),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: None,
            get_or_create: true,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result1 = backend.create_collection(create_req).await;
        assert!(
            result1.is_ok(),
            "Failed to create collection first time: {:?}",
            result1.err()
        );
        let response1 = result1.unwrap();
        assert!(response1.created, "Collection should be created first time");
        verify_new_collection(
            &response1.collection,
            collection_id,
            &collection_name1,
            Some(128),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );

        // Create collection second time with SAME ID but different name (should return existing)
        let collection_name2 = format!(
            "test_collection_goc_id2_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let create_req2 = CreateCollectionRequest {
            id: collection_id, // Same ID
            name: collection_name2,
            dimension: Some(512), // Different dimension
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: None,
            get_or_create: true,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result2 = backend.create_collection(create_req2).await;
        assert!(
            result2.is_ok(),
            "get_or_create with same ID should succeed: {:?}",
            result2.err()
        );
        let response2 = result2.unwrap();
        assert!(
            !response2.created,
            "Collection should NOT be created second time"
        );
        // Should return the original collection with original values
        verify_new_collection(
            &response2.collection,
            collection_id,
            &collection_name1, // Original name, not collection_name2
            Some(128),         // Original dimension, not 512
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_create_collection_same_name_different_databases() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // Create tenant
        let tenant_id = Uuid::new_v4().to_string();
        let create_tenant_req = CreateTenantRequest {
            id: tenant_id.clone(),
        };
        backend
            .create_tenant(create_tenant_req)
            .await
            .expect("Failed to create tenant");

        // Create two databases
        let db_name1 = format!(
            "test_db1_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let db_name2 = format!(
            "test_db2_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        backend
            .create_database(CreateDatabaseRequest {
                id: Uuid::new_v4(),
                name: db_name1.clone(),
                tenant_id: tenant_id.clone(),
            })
            .await
            .expect("Failed to create database 1");

        backend
            .create_database(CreateDatabaseRequest {
                id: Uuid::new_v4(),
                name: db_name2.clone(),
                tenant_id: tenant_id.clone(),
            })
            .await
            .expect("Failed to create database 2");

        // Same collection name for both
        let collection_name = format!(
            "shared_collection_name_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create collection in database 1
        let collection_id1 = CollectionUuid(Uuid::new_v4());
        let create_req1 = CreateCollectionRequest {
            id: collection_id1,
            name: collection_name.clone(),
            dimension: Some(128),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id1),
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name1.clone(),
        };

        let result1 = backend.create_collection(create_req1).await;
        assert!(
            result1.is_ok(),
            "Failed to create collection in db1: {:?}",
            result1.err()
        );

        let response1 = result1.unwrap();
        assert!(response1.created, "Collection should be created in db1");
        verify_new_collection(
            &response1.collection,
            collection_id1,
            &collection_name,
            Some(128),
            &tenant_id,
            &db_name1,
            None,
            Some(&Schema::default()),
        );

        // Create collection with SAME NAME in database 2 (should succeed)
        let collection_id2 = CollectionUuid(Uuid::new_v4());
        let create_req2 = CreateCollectionRequest {
            id: collection_id2,
            name: collection_name.clone(), // Same name
            dimension: Some(256),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id2),
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name2.clone(), // Different database
        };

        let result2 = backend.create_collection(create_req2).await;
        assert!(
            result2.is_ok(),
            "Creating collection with same name in different database should succeed: {:?}",
            result2.err()
        );

        let response2 = result2.unwrap();
        assert!(response2.created, "Collection should be created in db2");
        verify_new_collection(
            &response2.collection,
            collection_id2,
            &collection_name,
            Some(256),
            &tenant_id,
            &db_name2,
            None,
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_create_collection_get_or_create_with_metadata() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_collection_goc_meta_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create collection with metadata using get_or_create=true
        let original_metadata: chroma_types::Metadata = [
            (
                "str_key".to_string(),
                chroma_types::MetadataValue::Str("original_value".to_string()),
            ),
            ("int_key".to_string(), chroma_types::MetadataValue::Int(42)),
            (
                "float_key".to_string(),
                chroma_types::MetadataValue::Float(1.5),
            ),
            (
                "bool_key".to_string(),
                chroma_types::MetadataValue::Bool(true),
            ),
        ]
        .into_iter()
        .collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(256),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: Some(original_metadata.clone()),
            get_or_create: true,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result1 = backend.create_collection(create_req).await;
        assert!(
            result1.is_ok(),
            "Failed to create collection first time: {:?}",
            result1.err()
        );
        let response1 = result1.unwrap();
        assert!(response1.created, "Collection should be created first time");
        verify_new_collection(
            &response1.collection,
            collection_id,
            &collection_name,
            Some(256),
            &tenant_id,
            &db_name,
            Some(&original_metadata),
            Some(&Schema::default()),
        );

        // Call get_or_create again with different metadata - should return original
        let different_metadata: chroma_types::Metadata = [(
            "different_key".to_string(),
            chroma_types::MetadataValue::Str("different_value".to_string()),
        )]
        .into_iter()
        .collect();

        let collection_id2 = CollectionUuid(Uuid::new_v4());
        let create_req2 = CreateCollectionRequest {
            id: collection_id2,
            name: collection_name.clone(), // Same name
            dimension: Some(512),          // Different dimension
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id2),
            metadata: Some(different_metadata), // Different metadata - should be ignored
            get_or_create: true,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result2 = backend.create_collection(create_req2).await;
        assert!(
            result2.is_ok(),
            "get_or_create should succeed: {:?}",
            result2.err()
        );
        let response2 = result2.unwrap();
        assert!(
            !response2.created,
            "Collection should NOT be created second time"
        );

        // Should return the original collection with ORIGINAL metadata
        verify_new_collection(
            &response2.collection,
            collection_id,
            &collection_name,
            Some(256), // Original dimension
            &tenant_id,
            &db_name,
            Some(&original_metadata), // Original metadata, not different_metadata
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_create_collection_get_or_create_with_custom_schema() {
        use chroma_types::{
            FtsIndexConfig, FtsIndexType, StringInvertedIndexConfig, StringInvertedIndexType,
            StringValueType, ValueTypes,
        };

        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_collection_goc_schema_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create a custom schema with an additional key override
        let mut custom_schema = Schema::default();
        custom_schema.keys.insert(
            "custom_key".to_string(),
            ValueTypes {
                string: Some(StringValueType {
                    fts_index: Some(FtsIndexType {
                        enabled: true,
                        config: FtsIndexConfig {},
                    }),
                    string_inverted_index: Some(StringInvertedIndexType {
                        enabled: false,
                        config: StringInvertedIndexConfig {},
                    }),
                }),
                ..Default::default()
            },
        );

        // Create collection with custom schema
        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(256),
            index_schema: custom_schema.clone(),
            segments: create_test_segments(collection_id),
            metadata: None,
            get_or_create: true,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result1 = backend.create_collection(create_req).await;
        assert!(
            result1.is_ok(),
            "Failed to create collection first time: {:?}",
            result1.err()
        );
        let response1 = result1.unwrap();
        assert!(response1.created, "Collection should be created first time");
        verify_new_collection(
            &response1.collection,
            collection_id,
            &collection_name,
            Some(256),
            &tenant_id,
            &db_name,
            None,
            Some(&custom_schema),
        );

        // Verify the custom key is in the schema
        assert!(
            response1
                .collection
                .schema
                .as_ref()
                .unwrap()
                .keys
                .contains_key("custom_key"),
            "Schema should contain custom_key"
        );

        // Call get_or_create again with default schema - should return original custom schema
        let collection_id2 = CollectionUuid(Uuid::new_v4());
        let create_req2 = CreateCollectionRequest {
            id: collection_id2,
            name: collection_name.clone(),   // Same name
            dimension: Some(512),            // Different dimension
            index_schema: Schema::default(), // Different schema - should be ignored
            segments: create_test_segments(collection_id2),
            metadata: None,
            get_or_create: true,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        let result2 = backend.create_collection(create_req2).await;
        assert!(
            result2.is_ok(),
            "get_or_create should succeed: {:?}",
            result2.err()
        );
        let response2 = result2.unwrap();
        assert!(
            !response2.created,
            "Collection should NOT be created second time"
        );

        // Should return the original collection with ORIGINAL custom schema
        verify_new_collection(
            &response2.collection,
            collection_id,
            &collection_name,
            Some(256), // Original dimension
            &tenant_id,
            &db_name,
            None,
            Some(&custom_schema), // Original custom schema, not default
        );

        // Verify the custom key is still in the schema (proving it was read from DB)
        assert!(
            response2
                .collection
                .schema
                .as_ref()
                .unwrap()
                .keys
                .contains_key("custom_key"),
            "Schema should still contain custom_key after get_or_create"
        );
    }
}
