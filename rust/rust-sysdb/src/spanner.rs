//! Spanner backend implementation.
//!
//! This module provides the `SpannerBackend` which implements all SysDb
//! operations using Google Cloud Spanner as the underlying database.

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use chroma_config::{registry::Registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use google_cloud_gax::conn::Environment;
use google_cloud_spanner::client::{Client, ClientConfig};
use google_cloud_spanner::key::Key;
use google_cloud_spanner::mutation::{delete, insert, update};
use google_cloud_spanner::row::Row;
use google_cloud_spanner::statement::Statement;
use thiserror::Error;
use uuid::Uuid;

use crate::config::{SpannerBackendConfig, SpannerConfig};
use crate::types::{
    CreateCollectionRequest, CreateCollectionResponse, CreateDatabaseRequest,
    CreateDatabaseResponse, CreateTenantRequest, CreateTenantResponse,
    GetCollectionWithSegmentsRequest, GetCollectionWithSegmentsResponse, GetCollectionsRequest,
    GetCollectionsResponse, GetDatabaseRequest, GetDatabaseResponse, GetTenantRequest,
    GetTenantResponse, SetTenantResourceNameRequest, SetTenantResourceNameResponse, SpannerRow,
    SpannerRowRef, SpannerRows, SysDbError, UpdateCollectionRequest, UpdateCollectionResponse,
};
use chroma_types::{
    Collection, Database, DatabaseUuid, InternalCollectionConfiguration, RegionName, Segment,
    Tenant,
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
    /// All regions in this backend's topology (for multi-region writes)
    regions: Vec<RegionName>,
    /// The local region for this instance (for reads)
    local_region: RegionName,
}

impl SpannerBackend {
    /// Create a new SpannerBackend with the given client and region configuration.
    pub fn new(client: Client, regions: Vec<RegionName>, local_region: RegionName) -> Self {
        Self {
            client,
            regions,
            local_region,
        }
    }

    /// Get a reference to the underlying Spanner client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Get the regions this backend writes to.
    pub fn regions(&self) -> &[RegionName] {
        &self.regions
    }

    /// Get the local region for reads.
    pub fn local_region(&self) -> &RegionName {
        &self.local_region
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
        // Use a read-write transaction to atomically check tenant, check database, and insert
        let result = self
            .client
            .read_write_transaction::<(), SysDbError, _>(|tx| {
                let tenant_id = req.tenant_id.clone();
                let db_id = req.id.to_string();
                let db_name = req.name.clone().into_string();
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
        let db_name_str = req.name.as_ref();
        let mut stmt = Statement::new(
            "SELECT id, name, tenant_id FROM databases WHERE name = @name AND tenant_id = @tenant_id AND is_deleted = FALSE",
        );
        stmt.add_param("name", &db_name_str);
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
                db_name_str, req.tenant_id
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
        let database_name = database_name.into_string();

        // Get regions from backend configuration
        let regions = self.regions.clone();
        let local_region = self.local_region.clone();

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
                let regions = regions.clone();
                let local_region = local_region.clone();
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
                            let fetched_collection =
                                Self::fetch_collection_in_tx(&mut *tx, &collection_id, local_region.as_str()).await?;
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
                            let fetched_collection =
                                Self::fetch_collection_in_tx(&mut *tx, &existing_collection_id, local_region.as_str()).await?;
                            return Ok((fetched_collection, false)); // false = not created
                        } else {
                            return Err(SysDbError::AlreadyExists(format!(
                                "collection with name '{}' already exists in database '{}'",
                                collection_name, database_id
                            )));
                        }
                    }

                    // Use Spanner's commit timestamp sentinel string
                    // This tells Spanner to use the actual commit timestamp instead of a client-provided value
                    let commit_ts = "spanner.commit_timestamp()";
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
                            &commit_ts,
                            &commit_ts,
                        ],
                    ));

                    // Insert compaction cursors for each region
                    for region in &regions {
                        let region_str = region.to_string();
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
                                &region_str,
                                &index_schema_json,
                                &commit_ts,
                                &commit_ts,
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

                        for region in &regions {
                            let region_str = region.to_string();
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
                                    &region_str,
                                    &segment_id_str,
                                    &segment_type_str,
                                    &segment_scope_str,
                                    &false,
                                    &commit_ts,
                                    &commit_ts,
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
                                    &commit_ts,
                                    &commit_ts,
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

    /// Get collections by filter.
    ///
    /// Supports filtering by:
    /// - `ids`: One or more collection IDs
    /// - `name` + `tenant_id` + `database_name`: Collection by name within a database
    /// - `include_soft_deleted`: Whether to include soft-deleted collections (default: false)
    /// - `limit` and `offset`: Pagination
    ///
    /// Returns a list of matching collections.
    pub async fn get_collections(
        &self,
        req: GetCollectionsRequest,
    ) -> Result<GetCollectionsResponse, SysDbError> {
        let filter = req.filter;

        // Use local region for reads
        let region = self.local_region.as_str();

        // Build dynamic query based on which filters are provided
        let mut where_clauses: Vec<String> = Vec::new();

        // Filter by collection IDs
        let ids_str: Option<Vec<String>> = filter
            .ids
            .as_ref()
            .map(|ids| ids.iter().map(|id| id.0.to_string()).collect());

        if ids_str.is_some() {
            where_clauses.push("c.collection_id IN UNNEST(@collection_ids)".to_string());
        }

        // Filter by name
        if filter.name.is_some() {
            where_clauses.push("c.name = @name".to_string());
        }

        // Filter by tenant_id
        if filter.tenant_id.is_some() {
            where_clauses.push("c.tenant_id = @tenant_id".to_string());
        }

        // Filter by database_name
        if filter.database_name.is_some() {
            where_clauses.push("c.database_name = @database_name".to_string());
        }

        // Filter by soft-deleted status
        if !filter.include_soft_deleted {
            where_clauses.push("c.is_deleted = FALSE".to_string());
        }

        // AND.
        let where_clause = if where_clauses.is_empty() {
            "TRUE".to_string()
        } else {
            where_clauses.join(" AND ")
        };

        // Build LIMIT/OFFSET clause for pagination
        // Note: We apply pagination to a subquery to get collection IDs first,
        // then join with metadata to avoid limiting metadata rows instead of collections
        let pagination = match (filter.limit, filter.offset) {
            (Some(limit), Some(offset)) => format!("LIMIT {} OFFSET {}", limit, offset),
            (Some(limit), None) => format!("LIMIT {}", limit),
            (None, None) => String::new(),
            (None, Some(_)) => {
                return Err(SysDbError::InvalidArgument(
                    "offset requires limit to be specified".to_string(),
                ));
            }
        };

        let query = format!(
            r#"
            WITH filtered_collections AS (
                SELECT c.collection_id
                FROM collections c
                WHERE {where_clause}
                ORDER BY c.created_at ASC
                {pagination}
            )
            SELECT 
                c.collection_id,
                c.name,
                c.dimension,
                c.database_id,
                c.database_name,
                c.tenant_id,
                c.updated_at,
                c.created_at,
                cm.key as metadata_key,
                cm.str_value as metadata_str_value,
                cm.int_value as metadata_int_value,
                cm.float_value as metadata_float_value,
                cm.bool_value as metadata_bool_value,
                ccc.last_compacted_offset,
                ccc.version,
                ccc.total_records_post_compaction,
                ccc.size_bytes_post_compaction,
                ccc.last_compaction_time_secs,
                ccc.version_file_name,
                ccc.compaction_failure_count,
                ccc.index_schema
            FROM filtered_collections fc
            JOIN collections c ON c.collection_id = fc.collection_id
            LEFT JOIN collection_metadata cm ON cm.collection_id = c.collection_id
            LEFT JOIN collection_compaction_cursors ccc 
                ON ccc.collection_id = c.collection_id AND ccc.region = @region
            ORDER BY c.created_at ASC
            "#,
            where_clause = where_clause,
            pagination = pagination,
        );

        let mut stmt = Statement::new(&query);
        stmt.add_param("region", &region);

        // Bind parameters based on which filters are set
        if let Some(ref ids) = ids_str {
            stmt.add_param("collection_ids", ids);
        }
        if let Some(ref name) = filter.name {
            stmt.add_param("name", name);
        }
        if let Some(ref tenant_id) = filter.tenant_id {
            stmt.add_param("tenant_id", tenant_id);
        }
        if let Some(ref database_name) = filter.database_name {
            stmt.add_param("database_name", &database_name.as_ref());
        }

        let mut tx = self.client.single().await?;
        let mut result_set = tx.query(stmt).await?;

        // Collect all rows, grouped by collection_id, preserving query order (created_at ASC)
        let mut collection_order: Vec<String> = Vec::new();
        let mut rows_by_collection: std::collections::HashMap<String, Vec<Row>> =
            std::collections::HashMap::new();

        while let Some(row) = result_set.next().await? {
            let collection_id: String = row
                .column_by_name("collection_id")
                .map_err(SysDbError::FailedToReadColumn)?;

            if !rows_by_collection.contains_key(&collection_id) {
                collection_order.push(collection_id.clone());
            }
            rows_by_collection
                .entry(collection_id)
                .or_default()
                .push(row);
        }

        // Convert each group of rows to a Collection, preserving the query order
        let mut collections = Vec::new();
        for collection_id in collection_order {
            if let Some(rows) = rows_by_collection.remove(&collection_id) {
                let collection = Collection::try_from(SpannerRows { rows })?;
                collections.push(collection);
            }
        }

        Ok(GetCollectionsResponse { collections })
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

    /// Get a collection with its segments using a single 4-way JOIN query.
    ///
    /// This query joins:
    /// - collections (1 row)
    /// - collection_metadata (N rows - one per metadata key)
    /// - collection_compaction_cursors (1 row for the region)
    /// - collection_segments (3 rows - one per segment type)
    ///
    /// The result is N × 3 rows which are deduplicated in Rust.
    /// - Collection + metadata + compaction cursor: parsed via `Collection::try_from(rows)`
    /// - Segments: deduplicated using HashMap
    ///
    /// This approach fetches all data in a single network round trip.
    pub async fn get_collection_with_segments(
        &self,
        req: GetCollectionWithSegmentsRequest,
    ) -> Result<GetCollectionWithSegmentsResponse, SysDbError> {
        // Use local region for reads
        let region = self.local_region.as_str();
        let collection_id = req.id.0.to_string();

        // 4-way JOIN query: collection + metadata + compaction cursor + segments
        // Note: Segment columns are aliased with "segment_" prefix to match TryFrom<Row> for Segment
        let query = r#"
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
                ccc.last_compacted_offset,
                ccc.version,
                ccc.total_records_post_compaction,
                ccc.size_bytes_post_compaction,
                ccc.last_compaction_time_secs,
                ccc.version_file_name,
                ccc.index_schema,
                ccc.compaction_failure_count,
                cs.id as segment_id,
                cs.type as segment_type,
                cs.scope as segment_scope,
                cs.collection_id as segment_collection_id,
                cs.file_paths as segment_file_paths
            FROM collections c
            LEFT JOIN collection_metadata cm ON cm.collection_id = c.collection_id
            LEFT JOIN collection_compaction_cursors ccc 
                ON ccc.collection_id = c.collection_id AND ccc.region = @region
            LEFT JOIN collection_segments cs 
                ON cs.collection_id = c.collection_id AND cs.region = @region
            WHERE c.collection_id = @collection_id AND c.is_deleted = FALSE
        "#;

        let mut stmt = Statement::new(query);
        stmt.add_param("collection_id", &collection_id);
        stmt.add_param("region", &region);

        let mut tx = self.client.single().await?;
        let mut result_set = tx.query(stmt).await?;

        // Collect all rows
        let mut rows: Vec<Row> = Vec::new();
        while let Some(row) = result_set.next().await? {
            rows.push(row);
        }

        if rows.is_empty() {
            return Err(SysDbError::NotFound(format!(
                "Collection {} not found",
                collection_id
            )));
        }

        // Extract segments from rows (dedupe using HashMap)
        // Note: This must be done BEFORE Collection::try_from since that consumes rows
        // Uses TryFrom<&Row> for Segment which expects segment_ prefixed column names
        let mut segments_map: HashMap<String, Segment> = HashMap::new();

        for row in &rows {
            // Read the optional segment_id (NULL means no segment from LEFT JOIN)
            let segment_id_opt: Option<String> = row
                .column_by_name("segment_id")
                .map_err(SysDbError::FailedToReadColumn)?;

            if let Some(segment_id_str) = segment_id_opt {
                if let Entry::Vacant(entry) = segments_map.entry(segment_id_str) {
                    let segment = Segment::try_from(SpannerRowRef { row })?;
                    entry.insert(segment);
                }
            }
        }

        // Parse collection (including metadata and compaction cursor) using TryFrom<Vec<Row>>
        // This reuses the existing implementation that handles:
        // - Collection fields from first row
        // - Metadata aggregation from all rows
        // - Compaction cursor fields
        let collection = Collection::try_from(SpannerRows { rows })?;

        let segments: Vec<Segment> = segments_map.into_values().collect();

        Ok(GetCollectionWithSegmentsResponse {
            collection,
            segments,
        })
    }

    // ============================================================
    // Update Operations
    // ============================================================

    /// Update a collection.
    ///
    /// Supports updating:
    /// - `name`: New collection name (must be unique within database)
    /// - `dimension`: New dimension value
    /// - `metadata`: New metadata (replaces existing if provided)
    /// - `reset_metadata`: If true, deletes all existing metadata
    /// - `new_configuration`: New configuration (selective update of hnsw, spann, or embedding function)
    ///
    /// Returns the updated collection.
    pub async fn update_collection(
        &self,
        req: UpdateCollectionRequest,
    ) -> Result<UpdateCollectionResponse, SysDbError> {
        let UpdateCollectionRequest {
            id,
            name,
            dimension,
            metadata,
            reset_metadata,
            new_configuration,
            ..
        } = req;

        let collection_id = id.0.to_string();

        let result = self
            .client
            .read_write_transaction::<(), SysDbError, _>(|tx| {
                let collection_id = collection_id.clone();
                let name = name.clone();
                let metadata = metadata.clone();
                let new_configuration = new_configuration.clone();

                Box::pin(async move {
                    // First, verify the collection exists and get tenant/database info for name uniqueness check
                    let mut check_stmt = Statement::new(
                        "SELECT tenant_id, database_name FROM collections WHERE collection_id = @collection_id AND is_deleted = FALSE",
                    );
                    check_stmt.add_param("collection_id", &collection_id);

                    let mut check_iter = tx.query(check_stmt).await?;
                    let (tenant_id, database_name): (String, String) = match check_iter.next().await? {
                        Some(row) => {
                            let tenant: String = row.column_by_name("tenant_id").map_err(SysDbError::FailedToReadColumn)?;
                            let db: String = row.column_by_name("database_name").map_err(SysDbError::FailedToReadColumn)?;
                            (tenant, db)
                        }
                        None => {
                            return Err(SysDbError::NotFound(format!(
                                "collection with id '{}' not found",
                                collection_id
                            )));
                        }
                    };

                    // If name is being changed, check for uniqueness within the database
                    if let Some(ref new_name) = name {
                        let mut name_check_stmt = Statement::new(
                            "SELECT collection_id FROM collections WHERE tenant_id = @tenant_id AND database_name = @database_name AND name = @name AND collection_id != @collection_id AND is_deleted = FALSE",
                        );
                        name_check_stmt.add_param("tenant_id", &tenant_id);
                        name_check_stmt.add_param("database_name", &database_name);
                        name_check_stmt.add_param("name", new_name);
                        name_check_stmt.add_param("collection_id", &collection_id);

                        let mut name_iter = tx.query(name_check_stmt).await?;
                        if name_iter.next().await?.is_some() {
                            return Err(SysDbError::AlreadyExists(format!(
                                "collection with name '{}' already exists in database '{}'",
                                new_name, database_name
                            )));
                        }
                    }

                    // Use Spanner's commit timestamp sentinel string
                    let commit_ts = "spanner.commit_timestamp()";
                    let mut mutations = Vec::new();

                    // Determine what needs to be updated
                    let has_collection_changes = name.is_some() || dimension.is_some();
                    let has_metadata_changes = metadata.is_some() || reset_metadata;
                    let has_config_changes = new_configuration.as_ref().is_some_and(|c| {
                        c.hnsw.is_some() || c.spann.is_some() || c.embedding_function.is_some()
                    });

                    // Build collection update mutation if name or dimension changed
                    if has_collection_changes {
                        // We need the current name/dimension if not changing them
                        let mut current_stmt = Statement::new(
                            "SELECT name, dimension FROM collections WHERE collection_id = @collection_id",
                        );
                        current_stmt.add_param("collection_id", &collection_id);
                        let mut current_iter = tx.query(current_stmt).await?;
                        let (current_name, current_dimension): (String, Option<i64>) = match current_iter.next().await? {
                            Some(row) => {
                                let n: String = row.column_by_name("name").map_err(SysDbError::FailedToReadColumn)?;
                                let d: Option<i64> = row.column_by_name("dimension").ok();
                                (n, d)
                            }
                            None => return Err(SysDbError::Internal("collection disappeared".to_string())),
                        };

                        let final_name = name.as_ref().unwrap_or(&current_name);
                        let final_dimension = dimension.map(|d| d as i64).or(current_dimension);

                        mutations.push(update(
                            "collections",
                            &["collection_id", "name", "dimension", "updated_at"],
                            &[&collection_id, final_name, &final_dimension, &commit_ts],
                        ));
                    }

                    // Handle metadata changes
                    if has_metadata_changes {
                        // Query existing metadata keys to delete them
                        let mut keys_stmt = Statement::new(
                            "SELECT key FROM collection_metadata WHERE collection_id = @collection_id",
                        );
                        keys_stmt.add_param("collection_id", &collection_id);
                        let mut keys_iter = tx.query(keys_stmt).await?;
                        while let Some(row) = keys_iter.next().await? {
                            let key: String = row.column_by_name("key").map_err(SysDbError::FailedToReadColumn)?;
                            mutations.push(delete(
                                "collection_metadata",
                                Key::composite(&[&collection_id, &key]),
                            ));
                        }

                        // Insert new metadata if provided
                        if let Some(ref meta) = metadata {
                            for (key, value) in meta.iter() {
                                let (str_val, int_val, float_val, bool_val): (
                                    Option<&str>,
                                    Option<i64>,
                                    Option<f64>,
                                    Option<bool>,
                                ) = match value {
                                    chroma_types::MetadataValue::Str(s) => {
                                        (Some(s.as_str()), None, None, None)
                                    }
                                    chroma_types::MetadataValue::Int(i) => (None, Some(*i), None, None),
                                    chroma_types::MetadataValue::Float(f) => {
                                        (None, None, Some(*f), None)
                                    }
                                    chroma_types::MetadataValue::Bool(b) => {
                                        (None, None, None, Some(*b))
                                    }
                                    chroma_types::MetadataValue::SparseVector(_) => continue,
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
                                        &commit_ts,
                                        &commit_ts,
                                    ],
                                ));
                            }
                        }
                    }

                    // Handle configuration updates (spann and embedding_function only)
                    // Updates schema for ALL regions
                    if has_config_changes {
                        // Safe to unwrap: has_config_changes implies new_configuration is Some with hnsw, spann, or embedding_function
                        let config = new_configuration.as_ref().unwrap();

                        // Read current schemas from all regions
                        let mut schema_stmt = Statement::new(
                            "SELECT region, index_schema FROM collection_compaction_cursors WHERE collection_id = @collection_id",
                        );
                        schema_stmt.add_param("collection_id", &collection_id);

                        let mut schema_iter = tx.query(schema_stmt).await?;
                        let mut region_schemas: Vec<(String, String)> = Vec::new();

                        while let Some(row) = schema_iter.next().await? {
                            let region: String = row.column_by_name("region").map_err(SysDbError::FailedToReadColumn)?;
                            let schema_json: String = row.column_by_name("index_schema").map_err(SysDbError::FailedToReadColumn)?;
                            region_schemas.push((region, schema_json));
                        }

                        if region_schemas.is_empty() {
                            return Err(SysDbError::Internal("collection has no schema in any region".to_string()));
                        }

                        // Update schema for each region
                        for (region, current_schema_json) in region_schemas {
                            let mut schema: chroma_types::Schema = serde_json::from_str(&current_schema_json)
                                .map_err(|e| SysDbError::Internal(format!("failed to parse schema for region {}: {}", region, e)))?;

                            // Apply updates (errors if hnsw is set)
                            schema.apply_update_configuration(config)?;

                            let new_schema_json = serde_json::to_string(&schema)
                                .map_err(|e| SysDbError::Internal(format!("failed to serialize schema for region {}: {}", region, e)))?;

                            mutations.push(update(
                                "collection_compaction_cursors",
                                &["collection_id", "region", "index_schema", "updated_at"],
                                &[&collection_id, &region, &new_schema_json, &commit_ts],
                            ));
                        }
                    }

                    // Buffer all mutations - they will be applied atomically at commit
                    if !mutations.is_empty() {
                        tx.buffer_write(mutations);
                    }

                    tracing::info!("Updated collection: {}", collection_id);

                    Ok(())
                })
            })
            .await;

        match result {
            Ok((_, ())) => Ok(UpdateCollectionResponse {}),
            Err(e) => Err(e),
        }
    }

    // ============================================================
    // Lifecycle
    // ============================================================

    pub async fn close(self) {
        self.client.close().await;
    }
}

#[async_trait::async_trait]
impl<'a> Configurable<SpannerBackendConfig<'a>> for SpannerBackend {
    async fn try_from_config(
        config: &SpannerBackendConfig<'a>,
        _registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let client = match config.spanner {
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

        Ok(SpannerBackend {
            client,
            regions: config.regions.clone(),
            local_region: config.local_region.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        CollectionFilter, CreateCollectionRequest, CreateDatabaseRequest, CreateTenantRequest,
        GetCollectionWithSegmentsRequest, GetCollectionsRequest, GetDatabaseRequest,
        GetTenantRequest, UpdateCollectionRequest,
    };
    use chroma_types::{
        CollectionUuid, DatabaseName, Schema, Segment, SegmentScope, SegmentType, SegmentUuid,
    };
    use uuid::Uuid;

    // These tests require Tilt to be running with Spanner emulator.
    // They will be skipped if the Spanner emulator is not reachable.
    // To run: cargo test --package rust-sysdb --lib spanner::tests

    async fn setup_test_backend() -> Option<SpannerBackend> {
        use crate::config::SpannerBackendConfig;
        use chroma_config::registry::Registry;
        use chroma_config::Configurable;
        use chroma_config_spanner::SpannerEmulatorConfig;

        // Use the same config as Tilt (localhost:9010 when port-forwarded)
        let emulator = SpannerEmulatorConfig {
            host: "localhost".to_string(),
            grpc_port: 9010,
            rest_port: 9020,
            project: "local-project".to_string(),
            instance: "test-instance".to_string(),
            database: "local-sysdb-database".to_string(),
        };

        let spanner_config = SpannerConfig::Emulator(emulator);
        let registry = Registry::new();

        // Test regions matching Tilt emulator setup
        let regions = vec![
            RegionName::new("us").unwrap(),
            RegionName::new("asia").unwrap(),
            RegionName::new("europe").unwrap(),
        ];
        let local_region = RegionName::new("us").unwrap();

        let config = SpannerBackendConfig {
            spanner: &spanner_config,
            regions,
            local_region,
        };

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
        let db_name = chroma_types::DatabaseName::new(format!(
            "test_database_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ))
        .expect("test db name should be valid");

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
        let db_name = chroma_types::DatabaseName::new(format!(
            "test_database_conflict_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ))
        .expect("test db name should be valid");

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

        let db_name = chroma_types::DatabaseName::new(format!(
            "nonexistent_database_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ))
        .expect("test db name should be valid");

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
        let db_name = chroma_types::DatabaseName::new(format!(
            "test_database_invalid_tenant_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ))
        .expect("test db name should be valid");
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

        // Try to create database with empty name - should fail at type level
        assert!(
            chroma_types::DatabaseName::new("").is_none(),
            "DatabaseName::new should reject empty string"
        );
        assert!(
            chroma_types::DatabaseName::new("ab").is_none(),
            "DatabaseName::new should reject strings shorter than 3 characters"
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_create_database_duplicate_name_tenant() {
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
        let db_name = chroma_types::DatabaseName::new(format!(
            "test_database_dup_name_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ))
        .expect("test db name should be valid");

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
                    msg.contains(db_name.as_ref()) && msg.contains(&tenant_id),
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
    async fn setup_tenant_and_database(
        backend: &SpannerBackend,
    ) -> (String, chroma_types::DatabaseName) {
        let tenant_id = Uuid::new_v4().to_string();
        let create_tenant_req = CreateTenantRequest {
            id: tenant_id.clone(),
        };
        backend
            .create_tenant(create_tenant_req)
            .await
            .expect("Failed to create tenant");

        let db_id = Uuid::new_v4();
        let db_name = chroma_types::DatabaseName::new(format!(
            "test_db_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
        .expect("test db name should be valid");

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

    // Helper to create test segments for a collection (empty file paths)
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
        expected_database: &chroma_types::DatabaseName,
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
        assert_eq!(
            collection.database,
            expected_database.as_ref(),
            "database mismatch"
        );

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

    // Helper to create test segments with file paths populated
    fn create_test_segments_with_file_paths(collection_id: CollectionUuid) -> Vec<Segment> {
        vec![
            Segment {
                id: SegmentUuid(Uuid::new_v4()),
                r#type: SegmentType::BlockfileMetadata,
                scope: SegmentScope::METADATA,
                collection: collection_id,
                metadata: None,
                file_path: [(
                    "metadata_block".to_string(),
                    vec!["path/to/metadata1.bin".to_string()],
                )]
                .into_iter()
                .collect(),
            },
            Segment {
                id: SegmentUuid(Uuid::new_v4()),
                r#type: SegmentType::BlockfileRecord,
                scope: SegmentScope::RECORD,
                collection: collection_id,
                metadata: None,
                file_path: [(
                    "record_block".to_string(),
                    vec![
                        "path/to/record1.bin".to_string(),
                        "path/to/record2.bin".to_string(),
                    ],
                )]
                .into_iter()
                .collect(),
            },
            Segment {
                id: SegmentUuid(Uuid::new_v4()),
                r#type: SegmentType::HnswDistributed,
                scope: SegmentScope::VECTOR,
                collection: collection_id,
                metadata: None,
                file_path: [
                    (
                        "hnsw_index".to_string(),
                        vec!["path/to/hnsw.idx".to_string()],
                    ),
                    (
                        "vectors".to_string(),
                        vec![
                            "path/to/vectors1.bin".to_string(),
                            "path/to/vectors2.bin".to_string(),
                        ],
                    ),
                ]
                .into_iter()
                .collect(),
            },
        ]
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_create_collection() {
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
    async fn test_k8s_mcmr_integration_create_collection_duplicate_fails() {
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
    async fn test_k8s_mcmr_integration_create_collection_get_or_create() {
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
    async fn test_k8s_mcmr_integration_create_collection_empty_name() {
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
    async fn test_k8s_mcmr_integration_create_collection_nonexistent_database() {
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
            database_name: chroma_types::DatabaseName::new("nonexistent_database")
                .expect("test db name should be valid"),
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
    async fn test_k8s_mcmr_integration_create_collection_with_metadata() {
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
    async fn test_k8s_mcmr_integration_create_collection_duplicate_id_fails() {
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
    async fn test_k8s_mcmr_integration_create_collection_duplicate_id_get_or_create() {
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
    async fn test_k8s_mcmr_integration_create_collection_same_name_different_databases() {
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
        let db_name1 = chroma_types::DatabaseName::new(format!(
            "test_db1_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
        .expect("test db name should be valid");
        let db_name2 = chroma_types::DatabaseName::new(format!(
            "test_db2_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
        .expect("test db name should be valid");

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
    async fn test_k8s_mcmr_integration_create_collection_get_or_create_with_metadata() {
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
    async fn test_k8s_mcmr_integration_create_collection_get_or_create_with_custom_schema() {
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

    // ============================================================
    // get_collections tests
    // ============================================================

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_by_id() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_get_by_id_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

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

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get by ID
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(vec![collection_id]),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collection: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 1);

        verify_new_collection(
            &response.collections[0],
            collection_id,
            &collection_name,
            Some(128),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_by_name() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_get_by_name_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(256),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get by name + tenant + database
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .name(collection_name.clone())
                .tenant_id(tenant_id.clone())
                .database_name(db_name.clone()),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collection: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 1);

        verify_new_collection(
            &response.collections[0],
            collection_id,
            &collection_name,
            Some(256),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_multiple_ids() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create multiple collections with different dimensions
        // Store (id, name, dimension) for verification
        let dimensions: [i32; 3] = [128, 256, 512];
        let mut created_collections: Vec<(CollectionUuid, String, i32)> = Vec::new();

        for (i, &dim) in dimensions.iter().enumerate() {
            let collection_id = CollectionUuid(Uuid::new_v4());
            let collection_name = format!(
                "test_multi_{}_{}_{}",
                i,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                Uuid::new_v4()
            );

            created_collections.push((collection_id, collection_name.clone(), dim));

            let create_req = CreateCollectionRequest {
                id: collection_id,
                name: collection_name,
                dimension: Some(dim as u32),
                index_schema: Schema::default(),
                segments: create_test_segments(collection_id),
                metadata: None,
                get_or_create: false,
                tenant_id: tenant_id.clone(),
                database_name: db_name.clone(),
            };

            backend
                .create_collection(create_req)
                .await
                .expect("Failed to create collection");

            // Small delay to ensure different created_at timestamps for ordering
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Get by multiple IDs
        let collection_ids: Vec<_> = created_collections.iter().map(|(id, _, _)| *id).collect();
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(collection_ids),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collections: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 3);

        // Verify all fields of each collection in order (ordered by created_at ASC)
        for (i, (expected_id, expected_name, expected_dim)) in
            created_collections.iter().enumerate()
        {
            verify_new_collection(
                &response.collections[i],
                *expected_id,
                expected_name,
                Some(*expected_dim),
                &tenant_id,
                &db_name,
                None,
                Some(&Schema::default()),
            );
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_pagination() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create 5 collections with different dimensions
        // Store (id, name, dimension) for verification
        let dimensions: [i32; 5] = [128, 256, 384, 512, 640];
        let mut created_collections: Vec<(CollectionUuid, String, i32)> = Vec::new();

        for (i, &dim) in dimensions.iter().enumerate() {
            let collection_id = CollectionUuid(Uuid::new_v4());
            let collection_name = format!(
                "test_pagination_{}_{}_{}",
                i,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                Uuid::new_v4()
            );

            created_collections.push((collection_id, collection_name.clone(), dim));

            let create_req = CreateCollectionRequest {
                id: collection_id,
                name: collection_name,
                dimension: Some(dim as u32),
                index_schema: Schema::default(),
                segments: create_test_segments(collection_id),
                metadata: None,
                get_or_create: false,
                tenant_id: tenant_id.clone(),
                database_name: db_name.clone(),
            };

            backend
                .create_collection(create_req)
                .await
                .expect("Failed to create collection");

            // Small delay to ensure different created_at timestamps
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Get first 2 (should be collections 0 and 1 in order)
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .tenant_id(tenant_id.clone())
                .database_name(db_name.clone())
                .limit(2),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collections: {:?}",
            result.err()
        );
        let response = result.unwrap();
        assert_eq!(response.collections.len(), 2);

        // Verify all fields of first page (collections 0 and 1)
        for (i, (expected_id, expected_name, expected_dim)) in
            created_collections.iter().take(2).enumerate()
        {
            verify_new_collection(
                &response.collections[i],
                *expected_id,
                expected_name,
                Some(*expected_dim),
                &tenant_id,
                &db_name,
                None,
                Some(&Schema::default()),
            );
        }

        // Get next 2 with offset (should be collections 2 and 3 in order)
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .tenant_id(tenant_id.clone())
                .database_name(db_name.clone())
                .limit(2)
                .offset(2),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collections: {:?}",
            result.err()
        );
        let response = result.unwrap();
        assert_eq!(response.collections.len(), 2);

        // Verify all fields of second page (collections 2 and 3)
        for (i, (expected_id, expected_name, expected_dim)) in
            created_collections.iter().skip(2).take(2).enumerate()
        {
            verify_new_collection(
                &response.collections[i],
                *expected_id,
                expected_name,
                Some(*expected_dim),
                &tenant_id,
                &db_name,
                None,
                Some(&Schema::default()),
            );
        }

        // Get last page with offset 4 (should be collection 4 only)
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .tenant_id(tenant_id.clone())
                .database_name(db_name.clone())
                .limit(2)
                .offset(4),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collections: {:?}",
            result.err()
        );
        let response = result.unwrap();
        assert_eq!(response.collections.len(), 1);

        // Verify all fields of last page (collection 4)
        let (expected_id, expected_name, expected_dim) = &created_collections[4];
        verify_new_collection(
            &response.collections[0],
            *expected_id,
            expected_name,
            Some(*expected_dim),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_empty_result() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Get non-existent collection by ID
        let non_existent_id = CollectionUuid(Uuid::new_v4());
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(vec![non_existent_id]),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Should return Ok with empty list: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert!(
            response.collections.is_empty(),
            "Should return empty list for non-existent ID"
        );

        // Get by non-existent name
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .name("non_existent_collection_name_xyz")
                .tenant_id(tenant_id)
                .database_name(db_name),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Should return Ok with empty list: {:?}",
            result.err()
        );
        assert!(result.unwrap().collections.is_empty());
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_tenant_database_isolation() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // Create two separate tenant/database pairs
        let (tenant_id1, db_name1) = setup_tenant_and_database(&backend).await;
        let (tenant_id2, db_name2) = setup_tenant_and_database(&backend).await;

        // Create collection in first tenant/database
        let collection_id1 = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "isolated_collection_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let create_req = CreateCollectionRequest {
            id: collection_id1,
            name: collection_name.clone(),
            dimension: Some(128),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id1),
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id1.clone(),
            database_name: db_name1.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Query from second tenant/database - should not find the collection
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .name(collection_name.clone())
                .tenant_id(tenant_id2)
                .database_name(db_name2),
        };

        let result = backend.get_collections(get_req).await;
        assert!(result.is_ok());
        assert!(
            result.unwrap().collections.is_empty(),
            "Should not find collection from different tenant/database"
        );

        // Query from first tenant/database - should find the collection
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .name(collection_name.clone())
                .tenant_id(tenant_id1.clone())
                .database_name(db_name1.clone()),
        };

        let result = backend.get_collections(get_req).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.collections.len(), 1);

        verify_new_collection(
            &response.collections[0],
            collection_id1,
            &collection_name,
            Some(128),
            &tenant_id1,
            &db_name1,
            None,
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_with_metadata() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with all 4 metadata types
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_get_with_metadata_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let metadata: chroma_types::Metadata = [
            (
                "str_key".to_string(),
                chroma_types::MetadataValue::Str("test_value".to_string()),
            ),
            ("int_key".to_string(), chroma_types::MetadataValue::Int(42)),
            (
                "float_key".to_string(),
                chroma_types::MetadataValue::Float(1.23456),
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
            dimension: Some(512),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: Some(metadata.clone()),
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get collection and verify all fields including metadata
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(vec![collection_id]),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collection: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 1);

        verify_new_collection(
            &response.collections[0],
            collection_id,
            &collection_name,
            Some(512),
            &tenant_id,
            &db_name,
            Some(&metadata),
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_multiple_with_metadata() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create 3 collections with different metadata
        let metadata1: chroma_types::Metadata = [
            (
                "env".to_string(),
                chroma_types::MetadataValue::Str("production".to_string()),
            ),
            ("priority".to_string(), chroma_types::MetadataValue::Int(1)),
        ]
        .into_iter()
        .collect();

        let metadata2: chroma_types::Metadata = [
            (
                "env".to_string(),
                chroma_types::MetadataValue::Str("staging".to_string()),
            ),
            ("priority".to_string(), chroma_types::MetadataValue::Int(2)),
            ("debug".to_string(), chroma_types::MetadataValue::Bool(true)),
        ]
        .into_iter()
        .collect();

        let metadata3: chroma_types::Metadata = [(
            "score".to_string(),
            chroma_types::MetadataValue::Float(99.5),
        )]
        .into_iter()
        .collect();

        let metadatas = [metadata1.clone(), metadata2.clone(), metadata3.clone()];
        let dimensions: [i32; 3] = [128, 256, 512];
        let mut created_collections: Vec<(CollectionUuid, String, i32, chroma_types::Metadata)> =
            Vec::new();

        for (i, (&dim, metadata)) in dimensions.iter().zip(metadatas.iter()).enumerate() {
            let collection_id = CollectionUuid(Uuid::new_v4());
            let collection_name = format!(
                "test_multi_meta_{}_{}_{}",
                i,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                Uuid::new_v4()
            );

            created_collections.push((
                collection_id,
                collection_name.clone(),
                dim,
                metadata.clone(),
            ));

            let create_req = CreateCollectionRequest {
                id: collection_id,
                name: collection_name,
                dimension: Some(dim as u32),
                index_schema: Schema::default(),
                segments: create_test_segments(collection_id),
                metadata: Some(metadata.clone()),
                get_or_create: false,
                tenant_id: tenant_id.clone(),
                database_name: db_name.clone(),
            };

            backend
                .create_collection(create_req)
                .await
                .expect("Failed to create collection");

            // Small delay to ensure different created_at timestamps
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Get all collections by IDs
        let collection_ids: Vec<_> = created_collections
            .iter()
            .map(|(id, _, _, _)| *id)
            .collect();
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(collection_ids),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collections: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 3);

        // Verify all fields of each collection including their unique metadata
        for (i, (expected_id, expected_name, expected_dim, expected_metadata)) in
            created_collections.iter().enumerate()
        {
            verify_new_collection(
                &response.collections[i],
                *expected_id,
                expected_name,
                Some(*expected_dim),
                &tenant_id,
                &db_name,
                Some(expected_metadata),
                Some(&Schema::default()),
            );
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_mixed_with_and_without_metadata() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create collections: with metadata, without metadata, with metadata
        let metadata1: chroma_types::Metadata = [(
            "key1".to_string(),
            chroma_types::MetadataValue::Str("value1".to_string()),
        )]
        .into_iter()
        .collect();

        let metadata3: chroma_types::Metadata =
            [("key3".to_string(), chroma_types::MetadataValue::Int(123))]
                .into_iter()
                .collect();

        let metadatas: [Option<chroma_types::Metadata>; 3] =
            [Some(metadata1.clone()), None, Some(metadata3.clone())];
        let dimensions: [i32; 3] = [128, 256, 512];
        let mut created_collections: Vec<(
            CollectionUuid,
            String,
            i32,
            Option<chroma_types::Metadata>,
        )> = Vec::new();

        for (i, (&dim, metadata)) in dimensions.iter().zip(metadatas.iter()).enumerate() {
            let collection_id = CollectionUuid(Uuid::new_v4());
            let collection_name = format!(
                "test_mixed_meta_{}_{}_{}",
                i,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                Uuid::new_v4()
            );

            created_collections.push((
                collection_id,
                collection_name.clone(),
                dim,
                metadata.clone(),
            ));

            let create_req = CreateCollectionRequest {
                id: collection_id,
                name: collection_name,
                dimension: Some(dim as u32),
                index_schema: Schema::default(),
                segments: create_test_segments(collection_id),
                metadata: metadata.clone(),
                get_or_create: false,
                tenant_id: tenant_id.clone(),
                database_name: db_name.clone(),
            };

            backend
                .create_collection(create_req)
                .await
                .expect("Failed to create collection");

            // Small delay to ensure different created_at timestamps
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Get all collections
        let collection_ids: Vec<_> = created_collections
            .iter()
            .map(|(id, _, _, _)| *id)
            .collect();
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(collection_ids),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collections: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 3);

        // Verify all fields - particularly that collection without metadata has None
        for (i, (expected_id, expected_name, expected_dim, expected_metadata)) in
            created_collections.iter().enumerate()
        {
            verify_new_collection(
                &response.collections[i],
                *expected_id,
                expected_name,
                Some(*expected_dim),
                &tenant_id,
                &db_name,
                expected_metadata.as_ref(),
                Some(&Schema::default()),
            );
        }

        // Extra verification that the middle collection has no metadata
        assert!(
            response.collections[1].metadata.is_none()
                || response.collections[1]
                    .metadata
                    .as_ref()
                    .map(|m| m.is_empty())
                    .unwrap_or(true),
            "Middle collection should have no metadata"
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_by_name_with_metadata() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with metadata
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_by_name_meta_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let metadata: chroma_types::Metadata = [
            (
                "author".to_string(),
                chroma_types::MetadataValue::Str("test_user".to_string()),
            ),
            ("version".to_string(), chroma_types::MetadataValue::Int(42)),
            (
                "confidence".to_string(),
                chroma_types::MetadataValue::Float(0.95),
            ),
            (
                "verified".to_string(),
                chroma_types::MetadataValue::Bool(true),
            ),
        ]
        .into_iter()
        .collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(768),
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: Some(metadata.clone()),
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get collection by name and verify all fields including metadata
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .name(collection_name.clone())
                .tenant_id(tenant_id.clone())
                .database_name(db_name.clone()),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collection by name: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 1);

        verify_new_collection(
            &response.collections[0],
            collection_id,
            &collection_name,
            Some(768),
            &tenant_id,
            &db_name,
            Some(&metadata),
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_by_name_with_custom_schema() {
        use chroma_types::{
            FtsIndexConfig, FtsIndexType, StringInvertedIndexConfig, StringInvertedIndexType,
            StringValueType, ValueTypes,
        };

        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with a custom schema
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_by_name_schema_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create a custom schema with key overrides
        let mut custom_schema = Schema::default();
        custom_schema.keys.insert(
            "category".to_string(),
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

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(512),
            index_schema: custom_schema.clone(),
            segments: create_test_segments(collection_id),
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get by name and verify custom schema is returned correctly
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .name(collection_name.clone())
                .tenant_id(tenant_id.clone())
                .database_name(db_name.clone()),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collection: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 1);

        verify_new_collection(
            &response.collections[0],
            collection_id,
            &collection_name,
            Some(512),
            &tenant_id,
            &db_name,
            None,
            Some(&custom_schema),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_with_custom_schema() {
        use chroma_types::{
            FtsIndexConfig, FtsIndexType, StringInvertedIndexConfig, StringInvertedIndexType,
            StringValueType, ValueTypes,
        };

        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with a custom schema
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_custom_schema_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create a custom schema with key overrides
        let mut custom_schema = Schema::default();
        custom_schema.keys.insert(
            "title".to_string(),
            ValueTypes {
                string: Some(StringValueType {
                    fts_index: Some(FtsIndexType {
                        enabled: true,
                        config: FtsIndexConfig {},
                    }),
                    string_inverted_index: Some(StringInvertedIndexType {
                        enabled: true,
                        config: StringInvertedIndexConfig {},
                    }),
                }),
                ..Default::default()
            },
        );

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(384),
            index_schema: custom_schema.clone(),
            segments: create_test_segments(collection_id),
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get by ID and verify custom schema is returned correctly
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(vec![collection_id]),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collection: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 1);

        verify_new_collection(
            &response.collections[0],
            collection_id,
            &collection_name,
            Some(384),
            &tenant_id,
            &db_name,
            None,
            Some(&custom_schema),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_multiple_with_different_schemas() {
        use chroma_types::{
            FtsIndexConfig, FtsIndexType, StringInvertedIndexConfig, StringInvertedIndexType,
            StringValueType, ValueTypes,
        };

        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create 3 collections with different schemas
        // Schema 1: default schema (no custom keys)
        let schema1 = Schema::default();

        // Schema 2: custom schema with "title" key
        let mut schema2 = Schema::default();
        schema2.keys.insert(
            "title".to_string(),
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

        // Schema 3: custom schema with "product_name" key
        let mut schema3 = Schema::default();
        schema3.keys.insert(
            "product_name".to_string(),
            ValueTypes {
                string: Some(StringValueType {
                    fts_index: Some(FtsIndexType {
                        enabled: true,
                        config: FtsIndexConfig {},
                    }),
                    string_inverted_index: Some(StringInvertedIndexType {
                        enabled: true,
                        config: StringInvertedIndexConfig {},
                    }),
                }),
                ..Default::default()
            },
        );

        let schemas = [schema1.clone(), schema2.clone(), schema3.clone()];
        let dimensions: [i32; 3] = [128, 256, 512];
        let mut created_collections: Vec<(CollectionUuid, String, i32, Schema)> = Vec::new();

        for (i, (schema, &dim)) in schemas.iter().zip(dimensions.iter()).enumerate() {
            let collection_id = CollectionUuid(Uuid::new_v4());
            let collection_name = format!(
                "test_multi_schema_{}_{}_{}",
                i,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                Uuid::new_v4()
            );

            created_collections.push((collection_id, collection_name.clone(), dim, schema.clone()));

            let create_req = CreateCollectionRequest {
                id: collection_id,
                name: collection_name,
                dimension: Some(dim as u32),
                index_schema: schema.clone(),
                segments: create_test_segments(collection_id),
                metadata: None,
                get_or_create: false,
                tenant_id: tenant_id.clone(),
                database_name: db_name.clone(),
            };

            backend
                .create_collection(create_req)
                .await
                .expect("Failed to create collection");

            // Small delay to ensure different created_at timestamps
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Get all collections by IDs
        let collection_ids: Vec<_> = created_collections
            .iter()
            .map(|(id, _, _, _)| *id)
            .collect();
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(collection_ids),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collections: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 3);

        // Verify each collection has its correct schema
        for (i, (expected_id, expected_name, expected_dim, expected_schema)) in
            created_collections.iter().enumerate()
        {
            verify_new_collection(
                &response.collections[i],
                *expected_id,
                expected_name,
                Some(*expected_dim),
                &tenant_id,
                &db_name,
                None,
                Some(expected_schema),
            );
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_with_schema_and_metadata() {
        use chroma_types::{
            FtsIndexConfig, FtsIndexType, StringInvertedIndexConfig, StringInvertedIndexType,
            StringValueType, ValueTypes,
        };

        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with both custom schema and metadata
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_schema_and_meta_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create a custom schema with key overrides
        let mut custom_schema = Schema::default();
        custom_schema.keys.insert(
            "document_type".to_string(),
            ValueTypes {
                string: Some(StringValueType {
                    fts_index: Some(FtsIndexType {
                        enabled: true,
                        config: FtsIndexConfig {},
                    }),
                    string_inverted_index: Some(StringInvertedIndexType {
                        enabled: true,
                        config: StringInvertedIndexConfig {},
                    }),
                }),
                ..Default::default()
            },
        );

        let metadata: chroma_types::Metadata = [
            (
                "source".to_string(),
                chroma_types::MetadataValue::Str("api".to_string()),
            ),
            ("version".to_string(), chroma_types::MetadataValue::Int(2)),
            (
                "confidence".to_string(),
                chroma_types::MetadataValue::Float(0.98),
            ),
            (
                "indexed".to_string(),
                chroma_types::MetadataValue::Bool(true),
            ),
        ]
        .into_iter()
        .collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(1024),
            index_schema: custom_schema.clone(),
            segments: create_test_segments(collection_id),
            metadata: Some(metadata.clone()),
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get by ID and verify both schema and metadata
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(vec![collection_id]),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collection: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 1);

        verify_new_collection(
            &response.collections[0],
            collection_id,
            &collection_name,
            Some(1024),
            &tenant_id,
            &db_name,
            Some(&metadata),
            Some(&custom_schema),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_by_tenant_database_with_schemas() {
        use chroma_types::{
            FtsIndexConfig, FtsIndexType, StringInvertedIndexConfig, StringInvertedIndexType,
            StringValueType, ValueTypes,
        };

        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create 3 collections with different schemas using tenant/database filter
        let schema1 = Schema::default();

        let mut schema2 = Schema::default();
        schema2.keys.insert(
            "author".to_string(),
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

        let mut schema3 = Schema::default();
        schema3.keys.insert(
            "content".to_string(),
            ValueTypes {
                string: Some(StringValueType {
                    fts_index: Some(FtsIndexType {
                        enabled: true,
                        config: FtsIndexConfig {},
                    }),
                    string_inverted_index: Some(StringInvertedIndexType {
                        enabled: true,
                        config: StringInvertedIndexConfig {},
                    }),
                }),
                ..Default::default()
            },
        );

        let schemas = [schema1.clone(), schema2.clone(), schema3.clone()];
        let dimensions: [i32; 3] = [128, 256, 512];
        let mut created_collections: Vec<(CollectionUuid, String, i32, Schema)> = Vec::new();

        for (i, (schema, &dim)) in schemas.iter().zip(dimensions.iter()).enumerate() {
            let collection_id = CollectionUuid(Uuid::new_v4());
            let collection_name = format!(
                "test_tenant_db_schema_{}_{}_{}",
                i,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                Uuid::new_v4()
            );

            created_collections.push((collection_id, collection_name.clone(), dim, schema.clone()));

            let create_req = CreateCollectionRequest {
                id: collection_id,
                name: collection_name,
                dimension: Some(dim as u32),
                index_schema: schema.clone(),
                segments: create_test_segments(collection_id),
                metadata: None,
                get_or_create: false,
                tenant_id: tenant_id.clone(),
                database_name: db_name.clone(),
            };

            backend
                .create_collection(create_req)
                .await
                .expect("Failed to create collection");

            // Small delay to ensure different created_at timestamps
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Get all collections by tenant/database filter (no IDs)
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .tenant_id(tenant_id.clone())
                .database_name(db_name.clone()),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collections: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 3);

        // Verify each collection has its correct schema (ordered by created_at ASC)
        for (i, (expected_id, expected_name, expected_dim, expected_schema)) in
            created_collections.iter().enumerate()
        {
            verify_new_collection(
                &response.collections[i],
                *expected_id,
                expected_name,
                Some(*expected_dim),
                &tenant_id,
                &db_name,
                None,
                Some(expected_schema),
            );
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_by_tenant_database_with_schemas_and_metadata(
    ) {
        use chroma_types::{
            FtsIndexConfig, FtsIndexType, Metadata, MetadataValue, StringInvertedIndexConfig,
            StringInvertedIndexType, StringValueType, ValueTypes,
        };

        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create 3 collections with different schemas AND metadata
        let schema1 = Schema::default();
        let metadata1: Metadata = [(
            "type".to_string(),
            MetadataValue::Str("documents".to_string()),
        )]
        .into_iter()
        .collect();

        let mut schema2 = Schema::default();
        schema2.keys.insert(
            "author".to_string(),
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
        let metadata2: Metadata = [
            (
                "type".to_string(),
                MetadataValue::Str("articles".to_string()),
            ),
            ("priority".to_string(), MetadataValue::Int(5)),
        ]
        .into_iter()
        .collect();

        let mut schema3 = Schema::default();
        schema3.keys.insert(
            "content".to_string(),
            ValueTypes {
                string: Some(StringValueType {
                    fts_index: Some(FtsIndexType {
                        enabled: true,
                        config: FtsIndexConfig {},
                    }),
                    string_inverted_index: Some(StringInvertedIndexType {
                        enabled: true,
                        config: StringInvertedIndexConfig {},
                    }),
                }),
                ..Default::default()
            },
        );
        let metadata3: Metadata = [
            ("type".to_string(), MetadataValue::Str("notes".to_string())),
            ("active".to_string(), MetadataValue::Bool(true)),
            ("score".to_string(), MetadataValue::Float(0.95)),
        ]
        .into_iter()
        .collect();

        let schemas = [schema1.clone(), schema2.clone(), schema3.clone()];
        let metadatas = [metadata1.clone(), metadata2.clone(), metadata3.clone()];
        let dimensions: [i32; 3] = [128, 256, 512];
        let mut created_collections: Vec<(CollectionUuid, String, i32, Schema, Metadata)> =
            Vec::new();

        for (i, ((schema, metadata), &dim)) in schemas
            .iter()
            .zip(metadatas.iter())
            .zip(dimensions.iter())
            .enumerate()
        {
            let collection_id = CollectionUuid(Uuid::new_v4());
            let collection_name = format!(
                "test_tenant_db_schema_meta_{}_{}_{}",
                i,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                Uuid::new_v4()
            );

            created_collections.push((
                collection_id,
                collection_name.clone(),
                dim,
                schema.clone(),
                metadata.clone(),
            ));

            let create_req = CreateCollectionRequest {
                id: collection_id,
                name: collection_name,
                dimension: Some(dim as u32),
                index_schema: schema.clone(),
                segments: create_test_segments(collection_id),
                metadata: Some(metadata.clone()),
                get_or_create: false,
                tenant_id: tenant_id.clone(),
                database_name: db_name.clone(),
            };

            backend
                .create_collection(create_req)
                .await
                .expect("Failed to create collection");

            // Small delay to ensure different created_at timestamps
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Get all collections by tenant/database filter (no IDs)
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .tenant_id(tenant_id.clone())
                .database_name(db_name.clone()),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collections: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 3);

        // Verify each collection has its correct schema AND metadata (ordered by created_at ASC)
        for (i, (expected_id, expected_name, expected_dim, expected_schema, expected_metadata)) in
            created_collections.iter().enumerate()
        {
            verify_new_collection(
                &response.collections[i],
                *expected_id,
                expected_name,
                Some(*expected_dim),
                &tenant_id,
                &db_name,
                Some(expected_metadata),
                Some(expected_schema),
            );
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_pagination_with_schemas() {
        use chroma_types::{
            FtsIndexConfig, FtsIndexType, StringInvertedIndexConfig, StringInvertedIndexType,
            StringValueType, ValueTypes,
        };

        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create 5 collections with different schemas
        let mut schemas: Vec<Schema> = Vec::new();
        for i in 0..5 {
            let mut schema = Schema::default();
            schema.keys.insert(
                format!("key_{}", i),
                ValueTypes {
                    string: Some(StringValueType {
                        fts_index: Some(FtsIndexType {
                            enabled: i % 2 == 0, // Alternate FTS enabled
                            config: FtsIndexConfig {},
                        }),
                        string_inverted_index: Some(StringInvertedIndexType {
                            enabled: true,
                            config: StringInvertedIndexConfig {},
                        }),
                    }),
                    ..Default::default()
                },
            );
            schemas.push(schema);
        }

        let dimensions: [i32; 5] = [128, 256, 384, 512, 640];
        let mut created_collections: Vec<(CollectionUuid, String, i32, Schema)> = Vec::new();

        for (i, (schema, &dim)) in schemas.iter().zip(dimensions.iter()).enumerate() {
            let collection_id = CollectionUuid(Uuid::new_v4());
            let collection_name = format!(
                "test_pagination_schema_{}_{}_{}",
                i,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                Uuid::new_v4()
            );

            created_collections.push((collection_id, collection_name.clone(), dim, schema.clone()));

            let create_req = CreateCollectionRequest {
                id: collection_id,
                name: collection_name,
                dimension: Some(dim as u32),
                index_schema: schema.clone(),
                segments: create_test_segments(collection_id),
                metadata: None,
                get_or_create: false,
                tenant_id: tenant_id.clone(),
                database_name: db_name.clone(),
            };

            backend
                .create_collection(create_req)
                .await
                .expect("Failed to create collection");

            // Small delay to ensure different created_at timestamps
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Get first 2 collections (should be collections 0 and 1)
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .tenant_id(tenant_id.clone())
                .database_name(db_name.clone())
                .limit(2),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collections: {:?}",
            result.err()
        );
        let response = result.unwrap();
        assert_eq!(response.collections.len(), 2);

        // Verify all fields of first page including schemas
        for (i, (expected_id, expected_name, expected_dim, expected_schema)) in
            created_collections.iter().take(2).enumerate()
        {
            verify_new_collection(
                &response.collections[i],
                *expected_id,
                expected_name,
                Some(*expected_dim),
                &tenant_id,
                &db_name,
                None,
                Some(expected_schema),
            );
        }

        // Get next 2 with offset (should be collections 2 and 3)
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .tenant_id(tenant_id.clone())
                .database_name(db_name.clone())
                .limit(2)
                .offset(2),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collections: {:?}",
            result.err()
        );
        let response = result.unwrap();
        assert_eq!(response.collections.len(), 2);

        // Verify all fields of second page including schemas
        for (i, (expected_id, expected_name, expected_dim, expected_schema)) in
            created_collections.iter().skip(2).take(2).enumerate()
        {
            verify_new_collection(
                &response.collections[i],
                *expected_id,
                expected_name,
                Some(*expected_dim),
                &tenant_id,
                &db_name,
                None,
                Some(expected_schema),
            );
        }

        // Get last page (should be collection 4 only)
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .tenant_id(tenant_id.clone())
                .database_name(db_name.clone())
                .limit(2)
                .offset(4),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collections: {:?}",
            result.err()
        );
        let response = result.unwrap();
        assert_eq!(response.collections.len(), 1);

        // Verify all fields of last page including schema
        let (expected_id, expected_name, expected_dim, expected_schema) = &created_collections[4];
        verify_new_collection(
            &response.collections[0],
            *expected_id,
            expected_name,
            Some(*expected_dim),
            &tenant_id,
            &db_name,
            None,
            Some(expected_schema),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_by_name_with_schema_and_metadata() {
        use chroma_types::{
            FtsIndexConfig, FtsIndexType, StringInvertedIndexConfig, StringInvertedIndexType,
            StringValueType, ValueTypes,
        };

        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with both custom schema and metadata
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_by_name_schema_meta_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create a custom schema
        let mut custom_schema = Schema::default();
        custom_schema.keys.insert(
            "article_title".to_string(),
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

        // Create metadata with all 4 types
        let metadata: chroma_types::Metadata = [
            (
                "author".to_string(),
                chroma_types::MetadataValue::Str("John Doe".to_string()),
            ),
            (
                "word_count".to_string(),
                chroma_types::MetadataValue::Int(1500),
            ),
            (
                "rating".to_string(),
                chroma_types::MetadataValue::Float(4.5),
            ),
            (
                "published".to_string(),
                chroma_types::MetadataValue::Bool(true),
            ),
        ]
        .into_iter()
        .collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(768),
            index_schema: custom_schema.clone(),
            segments: create_test_segments(collection_id),
            metadata: Some(metadata.clone()),
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get by name and verify both schema and metadata
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .name(collection_name.clone())
                .tenant_id(tenant_id.clone())
                .database_name(db_name.clone()),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collection by name: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 1);

        verify_new_collection(
            &response.collections[0],
            collection_id,
            &collection_name,
            Some(768),
            &tenant_id,
            &db_name,
            Some(&metadata),
            Some(&custom_schema),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_ordering() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create 3 collections with small delays to ensure different created_at
        let mut created_ids = Vec::new();
        for i in 0..3 {
            let collection_id = CollectionUuid(Uuid::new_v4());
            created_ids.push(collection_id);

            let collection_name = format!(
                "test_order_{}_{}_{}",
                i,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                Uuid::new_v4()
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
                database_name: db_name.clone(),
            };

            backend
                .create_collection(create_req)
                .await
                .expect("Failed to create collection");

            // Small delay to ensure different created_at timestamps
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }

        // Get all collections in the database
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .tenant_id(tenant_id)
                .database_name(db_name),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collections: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 3);

        // Verify order matches creation order (created_at ASC)
        let returned_ids: Vec<_> = response
            .collections
            .iter()
            .map(|c| c.collection_id)
            .collect();

        assert_eq!(
            returned_ids, created_ids,
            "Collections should be returned in created_at ASC order"
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collections_null_dimension() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with NULL dimension
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_null_dim_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: None, // NULL dimension
            index_schema: Schema::default(),
            segments: create_test_segments(collection_id),
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get by ID and verify NULL dimension
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default().ids(vec![collection_id]),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collection: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(response.collections.len(), 1);

        verify_new_collection(
            &response.collections[0],
            collection_id,
            &collection_name,
            None, // Verify dimension is None
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );
    }

    // ============================================================
    // GetCollectionWithSegments Tests
    // ============================================================

    // Helper to verify all segment fields
    fn verify_segment(
        segment: &Segment,
        expected_collection_id: CollectionUuid,
        expected_scope: SegmentScope,
        expected_type: SegmentType,
        expected_file_paths: &std::collections::HashMap<String, Vec<String>>,
    ) {
        assert_eq!(
            segment.collection, expected_collection_id,
            "Segment collection ID mismatch"
        );
        assert_eq!(segment.scope, expected_scope, "Segment scope mismatch");
        assert_eq!(segment.r#type, expected_type, "Segment type mismatch");
        assert_eq!(
            &segment.file_path, expected_file_paths,
            "Segment file_path mismatch for {:?}",
            expected_scope
        );
        // Segment metadata is not stored in collection_segments table
        assert!(
            segment.metadata.is_none(),
            "Segment metadata should be None"
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collection_with_segments_empty_file_paths() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with segments (empty file paths)
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_with_segments_empty_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let segments = create_test_segments(collection_id);
        let segment_ids: Vec<SegmentUuid> = segments.iter().map(|s| s.id).collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(128),
            index_schema: Schema::default(),
            segments,
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get collection with segments
        let get_req = GetCollectionWithSegmentsRequest {
            database_name: db_name.clone(),
            id: collection_id,
        };
        let result = backend.get_collection_with_segments(get_req).await;

        assert!(
            result.is_ok(),
            "Failed to get collection with segments: {:?}",
            result.err()
        );

        let response = result.unwrap();

        // ===== Verify ALL collection fields =====
        verify_new_collection(
            &response.collection,
            collection_id,
            &collection_name,
            Some(128),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );

        // ===== Verify segments =====
        assert_eq!(response.segments.len(), 3, "Should return all 3 segments");

        // Verify all segment IDs are present
        let returned_segment_ids: std::collections::HashSet<SegmentUuid> =
            response.segments.iter().map(|s| s.id).collect();
        for expected_id in &segment_ids {
            assert!(
                returned_segment_ids.contains(expected_id),
                "Missing segment ID: {:?}",
                expected_id
            );
        }

        // Verify each segment's fields exhaustively
        let empty_file_paths: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        for segment in &response.segments {
            match segment.scope {
                SegmentScope::METADATA => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::METADATA,
                        SegmentType::BlockfileMetadata,
                        &empty_file_paths,
                    );
                }
                SegmentScope::RECORD => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::RECORD,
                        SegmentType::BlockfileRecord,
                        &empty_file_paths,
                    );
                }
                SegmentScope::VECTOR => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::VECTOR,
                        SegmentType::HnswDistributed,
                        &empty_file_paths,
                    );
                }
                _ => panic!("Unexpected segment scope: {:?}", segment.scope),
            }
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collection_with_segments_with_file_paths() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with segments that have file paths
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_with_segments_files_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let segments = create_test_segments_with_file_paths(collection_id);
        let segment_ids: Vec<SegmentUuid> = segments.iter().map(|s| s.id).collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(256),
            index_schema: Schema::default(),
            segments,
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get collection with segments
        let get_req = GetCollectionWithSegmentsRequest {
            database_name: db_name.clone(),
            id: collection_id,
        };
        let result = backend.get_collection_with_segments(get_req).await;

        assert!(
            result.is_ok(),
            "Failed to get collection with segments: {:?}",
            result.err()
        );

        let response = result.unwrap();

        // ===== Verify ALL collection fields =====
        verify_new_collection(
            &response.collection,
            collection_id,
            &collection_name,
            Some(256),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );

        // ===== Verify segments =====
        assert_eq!(response.segments.len(), 3, "Should return all 3 segments");

        // Verify all segment IDs are present
        let returned_segment_ids: std::collections::HashSet<SegmentUuid> =
            response.segments.iter().map(|s| s.id).collect();
        for expected_id in &segment_ids {
            assert!(
                returned_segment_ids.contains(expected_id),
                "Missing segment ID: {:?}",
                expected_id
            );
        }

        // Build expected file paths for each segment type
        let metadata_file_paths: std::collections::HashMap<String, Vec<String>> = [(
            "metadata_block".to_string(),
            vec!["path/to/metadata1.bin".to_string()],
        )]
        .into_iter()
        .collect();

        let record_file_paths: std::collections::HashMap<String, Vec<String>> = [(
            "record_block".to_string(),
            vec![
                "path/to/record1.bin".to_string(),
                "path/to/record2.bin".to_string(),
            ],
        )]
        .into_iter()
        .collect();

        let vector_file_paths: std::collections::HashMap<String, Vec<String>> = [
            (
                "hnsw_index".to_string(),
                vec!["path/to/hnsw.idx".to_string()],
            ),
            (
                "vectors".to_string(),
                vec![
                    "path/to/vectors1.bin".to_string(),
                    "path/to/vectors2.bin".to_string(),
                ],
            ),
        ]
        .into_iter()
        .collect();

        // Verify each segment's fields exhaustively
        for segment in &response.segments {
            match segment.scope {
                SegmentScope::METADATA => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::METADATA,
                        SegmentType::BlockfileMetadata,
                        &metadata_file_paths,
                    );
                }
                SegmentScope::RECORD => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::RECORD,
                        SegmentType::BlockfileRecord,
                        &record_file_paths,
                    );
                }
                SegmentScope::VECTOR => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::VECTOR,
                        SegmentType::HnswDistributed,
                        &vector_file_paths,
                    );
                }
                _ => panic!("Unexpected segment scope: {:?}", segment.scope),
            }
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collection_with_segments_not_found() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // Create a database name for the test
        let db_name = DatabaseName::new("test_database").unwrap();

        // Try to get a non-existent collection
        let non_existent_id = CollectionUuid(Uuid::new_v4());
        let get_req = GetCollectionWithSegmentsRequest {
            database_name: db_name,
            id: non_existent_id,
        };

        let result = backend.get_collection_with_segments(get_req).await;

        assert!(result.is_err(), "Should fail for non-existent collection");

        match result.err().unwrap() {
            SysDbError::NotFound(msg) => {
                assert!(
                    msg.contains(&non_existent_id.0.to_string()),
                    "Error should mention collection ID: {}",
                    msg
                );
            }
            e => panic!("Expected NotFound error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collection_with_segments_and_metadata() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with both metadata and segments with file paths
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_with_segments_and_metadata_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create metadata with all 4 types
        let metadata: chroma_types::Metadata = [
            (
                "str_key".to_string(),
                chroma_types::MetadataValue::Str("Test collection".to_string()),
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

        let segments = create_test_segments_with_file_paths(collection_id);
        let segment_ids: Vec<SegmentUuid> = segments.iter().map(|s| s.id).collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(512),
            index_schema: Schema::default(),
            segments,
            metadata: Some(metadata.clone()),
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get collection with segments
        let get_req = GetCollectionWithSegmentsRequest {
            database_name: db_name.clone(),
            id: collection_id,
        };
        let result = backend.get_collection_with_segments(get_req).await;

        assert!(
            result.is_ok(),
            "Failed to get collection with segments: {:?}",
            result.err()
        );

        let response = result.unwrap();

        // ===== Verify ALL collection fields =====
        verify_new_collection(
            &response.collection,
            collection_id,
            &collection_name,
            Some(512),
            &tenant_id,
            &db_name,
            Some(&metadata),
            Some(&Schema::default()),
        );

        // ===== Verify metadata exhaustively (all 4 types) =====
        let returned_metadata = response.collection.metadata.as_ref();
        assert!(returned_metadata.is_some(), "Should have metadata");

        let returned_metadata = returned_metadata.unwrap();
        assert_eq!(returned_metadata.len(), 4, "Should have 4 metadata entries");
        assert_eq!(
            returned_metadata.get("str_key"),
            Some(&chroma_types::MetadataValue::Str(
                "Test collection".to_string()
            ))
        );
        assert_eq!(
            returned_metadata.get("int_key"),
            Some(&chroma_types::MetadataValue::Int(42))
        );
        assert_eq!(
            returned_metadata.get("float_key"),
            Some(&chroma_types::MetadataValue::Float(1.5))
        );
        assert_eq!(
            returned_metadata.get("bool_key"),
            Some(&chroma_types::MetadataValue::Bool(true))
        );

        // ===== Verify segments =====
        assert_eq!(response.segments.len(), 3, "Should return all 3 segments");

        // Verify all segment IDs are present
        let returned_segment_ids: std::collections::HashSet<SegmentUuid> =
            response.segments.iter().map(|s| s.id).collect();
        for expected_id in &segment_ids {
            assert!(
                returned_segment_ids.contains(expected_id),
                "Missing segment ID: {:?}",
                expected_id
            );
        }

        // Verify each segment has correct collection reference and non-empty file paths
        for segment in &response.segments {
            assert_eq!(
                segment.collection, collection_id,
                "Segment should reference correct collection"
            );
            assert!(
                !segment.file_path.is_empty(),
                "Segment {:?} should have file paths",
                segment.scope
            );
            assert!(
                segment.metadata.is_none(),
                "Segment metadata should be None"
            );
        }

        // Verify segment types are correct
        let metadata_segment = response
            .segments
            .iter()
            .find(|s| s.scope == SegmentScope::METADATA);
        let record_segment = response
            .segments
            .iter()
            .find(|s| s.scope == SegmentScope::RECORD);
        let vector_segment = response
            .segments
            .iter()
            .find(|s| s.scope == SegmentScope::VECTOR);

        assert!(metadata_segment.is_some(), "Should have METADATA segment");
        assert!(record_segment.is_some(), "Should have RECORD segment");
        assert!(vector_segment.is_some(), "Should have VECTOR segment");

        assert_eq!(
            metadata_segment.unwrap().r#type,
            SegmentType::BlockfileMetadata
        );
        assert_eq!(record_segment.unwrap().r#type, SegmentType::BlockfileRecord);
        assert_eq!(vector_segment.unwrap().r#type, SegmentType::HnswDistributed);
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collection_with_segments_with_schema_empty_file_paths() {
        use chroma_types::{
            FtsIndexConfig, FtsIndexType, StringInvertedIndexConfig, StringInvertedIndexType,
            StringValueType, ValueTypes,
        };

        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with custom schema and segments (empty file paths)
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_with_segments_schema_empty_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create a custom schema with FTS enabled
        let mut custom_schema = Schema::default();
        custom_schema.keys.insert(
            "title".to_string(),
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

        let segments = create_test_segments(collection_id);
        let segment_ids: Vec<SegmentUuid> = segments.iter().map(|s| s.id).collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(256),
            index_schema: custom_schema.clone(),
            segments,
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get collection with segments
        let get_req = GetCollectionWithSegmentsRequest {
            database_name: db_name.clone(),
            id: collection_id,
        };
        let result = backend.get_collection_with_segments(get_req).await;

        assert!(
            result.is_ok(),
            "Failed to get collection with segments: {:?}",
            result.err()
        );

        let response = result.unwrap();

        // ===== Verify ALL collection fields including schema =====
        verify_new_collection(
            &response.collection,
            collection_id,
            &collection_name,
            Some(256),
            &tenant_id,
            &db_name,
            None,
            Some(&custom_schema),
        );

        // ===== Verify segments =====
        assert_eq!(response.segments.len(), 3, "Should return all 3 segments");

        // Verify all segment IDs are present
        let returned_segment_ids: std::collections::HashSet<SegmentUuid> =
            response.segments.iter().map(|s| s.id).collect();
        for expected_id in &segment_ids {
            assert!(
                returned_segment_ids.contains(expected_id),
                "Missing segment ID: {:?}",
                expected_id
            );
        }

        // Verify each segment's fields exhaustively
        let empty_file_paths: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        for segment in &response.segments {
            match segment.scope {
                SegmentScope::METADATA => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::METADATA,
                        SegmentType::BlockfileMetadata,
                        &empty_file_paths,
                    );
                }
                SegmentScope::RECORD => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::RECORD,
                        SegmentType::BlockfileRecord,
                        &empty_file_paths,
                    );
                }
                SegmentScope::VECTOR => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::VECTOR,
                        SegmentType::HnswDistributed,
                        &empty_file_paths,
                    );
                }
                _ => panic!("Unexpected segment scope: {:?}", segment.scope),
            }
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collection_with_segments_with_schema_with_file_paths() {
        use chroma_types::{
            FtsIndexConfig, FtsIndexType, StringInvertedIndexConfig, StringInvertedIndexType,
            StringValueType, ValueTypes,
        };

        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with custom schema and segments with file paths
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_with_segments_schema_files_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create a custom schema with both FTS and inverted index
        let mut custom_schema = Schema::default();
        custom_schema.keys.insert(
            "description".to_string(),
            ValueTypes {
                string: Some(StringValueType {
                    fts_index: Some(FtsIndexType {
                        enabled: true,
                        config: FtsIndexConfig {},
                    }),
                    string_inverted_index: Some(StringInvertedIndexType {
                        enabled: true,
                        config: StringInvertedIndexConfig {},
                    }),
                }),
                ..Default::default()
            },
        );

        let segments = create_test_segments_with_file_paths(collection_id);
        let segment_ids: Vec<SegmentUuid> = segments.iter().map(|s| s.id).collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(512),
            index_schema: custom_schema.clone(),
            segments,
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get collection with segments
        let get_req = GetCollectionWithSegmentsRequest {
            database_name: db_name.clone(),
            id: collection_id,
        };
        let result = backend.get_collection_with_segments(get_req).await;

        assert!(
            result.is_ok(),
            "Failed to get collection with segments: {:?}",
            result.err()
        );

        let response = result.unwrap();

        // ===== Verify ALL collection fields including schema =====
        verify_new_collection(
            &response.collection,
            collection_id,
            &collection_name,
            Some(512),
            &tenant_id,
            &db_name,
            None,
            Some(&custom_schema),
        );

        // ===== Verify segments =====
        assert_eq!(response.segments.len(), 3, "Should return all 3 segments");

        // Verify all segment IDs are present
        let returned_segment_ids: std::collections::HashSet<SegmentUuid> =
            response.segments.iter().map(|s| s.id).collect();
        for expected_id in &segment_ids {
            assert!(
                returned_segment_ids.contains(expected_id),
                "Missing segment ID: {:?}",
                expected_id
            );
        }

        // Build expected file paths for each segment type
        let metadata_file_paths: std::collections::HashMap<String, Vec<String>> = [(
            "metadata_block".to_string(),
            vec!["path/to/metadata1.bin".to_string()],
        )]
        .into_iter()
        .collect();

        let record_file_paths: std::collections::HashMap<String, Vec<String>> = [(
            "record_block".to_string(),
            vec![
                "path/to/record1.bin".to_string(),
                "path/to/record2.bin".to_string(),
            ],
        )]
        .into_iter()
        .collect();

        let vector_file_paths: std::collections::HashMap<String, Vec<String>> = [
            (
                "hnsw_index".to_string(),
                vec!["path/to/hnsw.idx".to_string()],
            ),
            (
                "vectors".to_string(),
                vec![
                    "path/to/vectors1.bin".to_string(),
                    "path/to/vectors2.bin".to_string(),
                ],
            ),
        ]
        .into_iter()
        .collect();

        // Verify each segment's fields exhaustively
        for segment in &response.segments {
            match segment.scope {
                SegmentScope::METADATA => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::METADATA,
                        SegmentType::BlockfileMetadata,
                        &metadata_file_paths,
                    );
                }
                SegmentScope::RECORD => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::RECORD,
                        SegmentType::BlockfileRecord,
                        &record_file_paths,
                    );
                }
                SegmentScope::VECTOR => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::VECTOR,
                        SegmentType::HnswDistributed,
                        &vector_file_paths,
                    );
                }
                _ => panic!("Unexpected segment scope: {:?}", segment.scope),
            }
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collection_with_segments_with_metadata_empty_file_paths()
    {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with metadata and segments (empty file paths)
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_with_segments_meta_empty_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create metadata with all 4 types
        let metadata: chroma_types::Metadata = [
            (
                "str_key".to_string(),
                chroma_types::MetadataValue::Str("test_value".to_string()),
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

        let segments = create_test_segments(collection_id);
        let segment_ids: Vec<SegmentUuid> = segments.iter().map(|s| s.id).collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(256),
            index_schema: Schema::default(),
            segments,
            metadata: Some(metadata.clone()),
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get collection with segments
        let get_req = GetCollectionWithSegmentsRequest {
            database_name: db_name.clone(),
            id: collection_id,
        };
        let result = backend.get_collection_with_segments(get_req).await;

        assert!(
            result.is_ok(),
            "Failed to get collection with segments: {:?}",
            result.err()
        );

        let response = result.unwrap();

        // ===== Verify ALL collection fields =====
        verify_new_collection(
            &response.collection,
            collection_id,
            &collection_name,
            Some(256),
            &tenant_id,
            &db_name,
            Some(&metadata),
            Some(&Schema::default()),
        );

        // ===== Verify segments =====
        assert_eq!(response.segments.len(), 3, "Should return all 3 segments");

        let returned_segment_ids: std::collections::HashSet<SegmentUuid> =
            response.segments.iter().map(|s| s.id).collect();
        for expected_id in &segment_ids {
            assert!(
                returned_segment_ids.contains(expected_id),
                "Missing segment ID: {:?}",
                expected_id
            );
        }

        let empty_file_paths: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        for segment in &response.segments {
            match segment.scope {
                SegmentScope::METADATA => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::METADATA,
                        SegmentType::BlockfileMetadata,
                        &empty_file_paths,
                    );
                }
                SegmentScope::RECORD => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::RECORD,
                        SegmentType::BlockfileRecord,
                        &empty_file_paths,
                    );
                }
                SegmentScope::VECTOR => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::VECTOR,
                        SegmentType::HnswDistributed,
                        &empty_file_paths,
                    );
                }
                _ => panic!("Unexpected segment scope: {:?}", segment.scope),
            }
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collection_with_segments_with_schema_and_metadata_empty_file_paths(
    ) {
        use chroma_types::{
            FtsIndexConfig, FtsIndexType, StringInvertedIndexConfig, StringInvertedIndexType,
            StringValueType, ValueTypes,
        };

        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with custom schema, metadata, and segments (empty file paths)
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_with_segments_schema_meta_empty_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create a custom schema
        let mut custom_schema = Schema::default();
        custom_schema.keys.insert(
            "title".to_string(),
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

        // Create metadata
        let metadata: chroma_types::Metadata = [
            (
                "category".to_string(),
                chroma_types::MetadataValue::Str("documents".to_string()),
            ),
            ("priority".to_string(), chroma_types::MetadataValue::Int(10)),
        ]
        .into_iter()
        .collect();

        let segments = create_test_segments(collection_id);
        let segment_ids: Vec<SegmentUuid> = segments.iter().map(|s| s.id).collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(128),
            index_schema: custom_schema.clone(),
            segments,
            metadata: Some(metadata.clone()),
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get collection with segments
        let get_req = GetCollectionWithSegmentsRequest {
            database_name: db_name.clone(),
            id: collection_id,
        };
        let result = backend.get_collection_with_segments(get_req).await;

        assert!(
            result.is_ok(),
            "Failed to get collection with segments: {:?}",
            result.err()
        );

        let response = result.unwrap();

        // ===== Verify ALL collection fields including schema and metadata =====
        verify_new_collection(
            &response.collection,
            collection_id,
            &collection_name,
            Some(128),
            &tenant_id,
            &db_name,
            Some(&metadata),
            Some(&custom_schema),
        );

        // ===== Verify segments =====
        assert_eq!(response.segments.len(), 3, "Should return all 3 segments");

        let returned_segment_ids: std::collections::HashSet<SegmentUuid> =
            response.segments.iter().map(|s| s.id).collect();
        for expected_id in &segment_ids {
            assert!(
                returned_segment_ids.contains(expected_id),
                "Missing segment ID: {:?}",
                expected_id
            );
        }

        let empty_file_paths: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        for segment in &response.segments {
            match segment.scope {
                SegmentScope::METADATA => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::METADATA,
                        SegmentType::BlockfileMetadata,
                        &empty_file_paths,
                    );
                }
                SegmentScope::RECORD => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::RECORD,
                        SegmentType::BlockfileRecord,
                        &empty_file_paths,
                    );
                }
                SegmentScope::VECTOR => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::VECTOR,
                        SegmentType::HnswDistributed,
                        &empty_file_paths,
                    );
                }
                _ => panic!("Unexpected segment scope: {:?}", segment.scope),
            }
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collection_with_segments_with_schema_and_metadata_with_file_paths(
    ) {
        use chroma_types::{
            FtsIndexConfig, FtsIndexType, StringInvertedIndexConfig, StringInvertedIndexType,
            StringValueType, ValueTypes,
        };

        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection with custom schema, metadata, and segments with file paths
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_with_segments_schema_meta_files_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create a custom schema with both FTS and inverted index
        let mut custom_schema = Schema::default();
        custom_schema.keys.insert(
            "content".to_string(),
            ValueTypes {
                string: Some(StringValueType {
                    fts_index: Some(FtsIndexType {
                        enabled: true,
                        config: FtsIndexConfig {},
                    }),
                    string_inverted_index: Some(StringInvertedIndexType {
                        enabled: true,
                        config: StringInvertedIndexConfig {},
                    }),
                }),
                ..Default::default()
            },
        );

        // Create metadata with all 4 types
        let metadata: chroma_types::Metadata = [
            (
                "type".to_string(),
                chroma_types::MetadataValue::Str("articles".to_string()),
            ),
            ("version".to_string(), chroma_types::MetadataValue::Int(2)),
            (
                "score".to_string(),
                chroma_types::MetadataValue::Float(0.95),
            ),
            (
                "active".to_string(),
                chroma_types::MetadataValue::Bool(true),
            ),
        ]
        .into_iter()
        .collect();

        let segments = create_test_segments_with_file_paths(collection_id);
        let segment_ids: Vec<SegmentUuid> = segments.iter().map(|s| s.id).collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: Some(512),
            index_schema: custom_schema.clone(),
            segments,
            metadata: Some(metadata.clone()),
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get collection with segments
        let get_req = GetCollectionWithSegmentsRequest {
            database_name: db_name.clone(),
            id: collection_id,
        };
        let result = backend.get_collection_with_segments(get_req).await;

        assert!(
            result.is_ok(),
            "Failed to get collection with segments: {:?}",
            result.err()
        );

        let response = result.unwrap();

        // ===== Verify ALL collection fields including schema and metadata =====
        verify_new_collection(
            &response.collection,
            collection_id,
            &collection_name,
            Some(512),
            &tenant_id,
            &db_name,
            Some(&metadata),
            Some(&custom_schema),
        );

        // ===== Verify segments =====
        assert_eq!(response.segments.len(), 3, "Should return all 3 segments");

        let returned_segment_ids: std::collections::HashSet<SegmentUuid> =
            response.segments.iter().map(|s| s.id).collect();
        for expected_id in &segment_ids {
            assert!(
                returned_segment_ids.contains(expected_id),
                "Missing segment ID: {:?}",
                expected_id
            );
        }

        // Build expected file paths
        let metadata_file_paths: std::collections::HashMap<String, Vec<String>> = [(
            "metadata_block".to_string(),
            vec!["path/to/metadata1.bin".to_string()],
        )]
        .into_iter()
        .collect();

        let record_file_paths: std::collections::HashMap<String, Vec<String>> = [(
            "record_block".to_string(),
            vec![
                "path/to/record1.bin".to_string(),
                "path/to/record2.bin".to_string(),
            ],
        )]
        .into_iter()
        .collect();

        let vector_file_paths: std::collections::HashMap<String, Vec<String>> = [
            (
                "hnsw_index".to_string(),
                vec!["path/to/hnsw.idx".to_string()],
            ),
            (
                "vectors".to_string(),
                vec![
                    "path/to/vectors1.bin".to_string(),
                    "path/to/vectors2.bin".to_string(),
                ],
            ),
        ]
        .into_iter()
        .collect();

        for segment in &response.segments {
            match segment.scope {
                SegmentScope::METADATA => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::METADATA,
                        SegmentType::BlockfileMetadata,
                        &metadata_file_paths,
                    );
                }
                SegmentScope::RECORD => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::RECORD,
                        SegmentType::BlockfileRecord,
                        &record_file_paths,
                    );
                }
                SegmentScope::VECTOR => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::VECTOR,
                        SegmentType::HnswDistributed,
                        &vector_file_paths,
                    );
                }
                _ => panic!("Unexpected segment scope: {:?}", segment.scope),
            }
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_get_collection_with_segments_null_dimension() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        // Create a collection without dimension
        let collection_id = CollectionUuid(Uuid::new_v4());
        let collection_name = format!(
            "test_with_segments_null_dim_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let segments = create_test_segments(collection_id);
        let segment_ids: Vec<SegmentUuid> = segments.iter().map(|s| s.id).collect();

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: collection_name.clone(),
            dimension: None, // No dimension
            index_schema: Schema::default(),
            segments,
            metadata: None,
            get_or_create: false,
            tenant_id: tenant_id.clone(),
            database_name: db_name.clone(),
        };

        backend
            .create_collection(create_req)
            .await
            .expect("Failed to create collection");

        // Get collection with segments
        let get_req = GetCollectionWithSegmentsRequest {
            database_name: db_name.clone(),
            id: collection_id,
        };
        let result = backend.get_collection_with_segments(get_req).await;

        assert!(
            result.is_ok(),
            "Failed to get collection with segments: {:?}",
            result.err()
        );

        let response = result.unwrap();

        // ===== Verify ALL collection fields with null dimension =====
        verify_new_collection(
            &response.collection,
            collection_id,
            &collection_name,
            None, // Null dimension
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );

        // ===== Verify segments field by field =====
        assert_eq!(response.segments.len(), 3, "Should return all 3 segments");

        let returned_segment_ids: std::collections::HashSet<SegmentUuid> =
            response.segments.iter().map(|s| s.id).collect();
        for expected_id in &segment_ids {
            assert!(
                returned_segment_ids.contains(expected_id),
                "Missing segment ID: {:?}",
                expected_id
            );
        }

        let empty_file_paths: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        for segment in &response.segments {
            match segment.scope {
                SegmentScope::METADATA => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::METADATA,
                        SegmentType::BlockfileMetadata,
                        &empty_file_paths,
                    );
                }
                SegmentScope::RECORD => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::RECORD,
                        SegmentType::BlockfileRecord,
                        &empty_file_paths,
                    );
                }
                SegmentScope::VECTOR => {
                    verify_segment(
                        segment,
                        collection_id,
                        SegmentScope::VECTOR,
                        SegmentType::HnswDistributed,
                        &empty_file_paths,
                    );
                }
                _ => panic!("Unexpected segment scope: {:?}", segment.scope),
            }
        }
    }

    // ============================================================
    // Update Collection Tests
    // ============================================================

    /// Helper to create a collection for update tests
    async fn create_collection_for_update(
        backend: &SpannerBackend,
        tenant_id: &str,
        db_name: &chroma_types::DatabaseName,
        name: &str,
        dimension: Option<u32>,
        metadata: Option<chroma_types::Metadata>,
    ) -> CollectionUuid {
        create_collection_for_update_with_schema(
            backend,
            tenant_id,
            db_name,
            name,
            dimension,
            metadata,
            Schema::default(),
        )
        .await
    }

    /// Helper to create a collection for update tests with custom schema
    async fn create_collection_for_update_with_schema(
        backend: &SpannerBackend,
        tenant_id: &str,
        db_name: &chroma_types::DatabaseName,
        name: &str,
        dimension: Option<u32>,
        metadata: Option<chroma_types::Metadata>,
        schema: Schema,
    ) -> CollectionUuid {
        let collection_id = CollectionUuid(Uuid::new_v4());

        let create_req = CreateCollectionRequest {
            id: collection_id,
            name: name.to_string(),
            dimension,
            index_schema: schema,
            segments: create_test_segments(collection_id),
            metadata,
            get_or_create: false,
            tenant_id: tenant_id.to_string(),
            database_name: db_name.clone(),
        };

        let result = backend.create_collection(create_req).await;
        assert!(
            result.is_ok(),
            "Failed to create collection for update test: {:?}",
            result.err()
        );

        collection_id
    }

    /// Helper to fetch a single collection by ID
    async fn fetch_collection(
        backend: &SpannerBackend,
        collection_id: CollectionUuid,
        tenant_id: &str,
        db_name: &chroma_types::DatabaseName,
    ) -> chroma_types::Collection {
        let get_req = GetCollectionsRequest {
            filter: CollectionFilter::default()
                .ids(vec![collection_id])
                .tenant_id(tenant_id.to_string())
                .database_name(db_name.clone()),
        };

        let result = backend.get_collections(get_req).await;
        assert!(
            result.is_ok(),
            "Failed to get collection: {:?}",
            result.err()
        );

        let response = result.unwrap();
        assert_eq!(
            response.collections.len(),
            1,
            "Expected exactly one collection"
        );

        response.collections[0].clone()
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_name_only() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let original_name = format!("test_coll_orig_{}", Uuid::new_v4());
        let new_name = format!("test_coll_new_{}", Uuid::new_v4());

        let collection_id = create_collection_for_update(
            &backend,
            &tenant_id,
            &db_name,
            &original_name,
            Some(128),
            None,
        )
        .await;

        // Update name only
        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: collection_id,
            name: Some(new_name.clone()),
            dimension: None,
            metadata: None,
            reset_metadata: false,
            new_configuration: None,
        };

        let result = backend.update_collection(update_req).await;
        assert!(
            result.is_ok(),
            "Failed to update collection name: {:?}",
            result.err()
        );

        // Verify via get_collections + verify_new_collection
        let collection = fetch_collection(&backend, collection_id, &tenant_id, &db_name).await;
        verify_new_collection(
            &collection,
            collection_id,
            &new_name, // name should be updated
            Some(128), // dimension should be unchanged
            &tenant_id,
            &db_name,
            None, // metadata should still be None
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_dimension_only() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let name = format!("test_coll_dim_{}", Uuid::new_v4());

        let collection_id =
            create_collection_for_update(&backend, &tenant_id, &db_name, &name, Some(128), None)
                .await;

        // Update dimension only
        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: collection_id,
            name: None,
            dimension: Some(256),
            metadata: None,
            reset_metadata: false,
            new_configuration: None,
        };

        let result = backend.update_collection(update_req).await;
        assert!(
            result.is_ok(),
            "Failed to update collection dimension: {:?}",
            result.err()
        );

        // Verify via get_collections + verify_new_collection
        let collection = fetch_collection(&backend, collection_id, &tenant_id, &db_name).await;
        verify_new_collection(
            &collection,
            collection_id,
            &name,     // name should be unchanged
            Some(256), // dimension should be updated
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_name_and_dimension() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let original_name = format!("test_coll_nd_orig_{}", Uuid::new_v4());
        let new_name = format!("test_coll_nd_new_{}", Uuid::new_v4());

        let collection_id = create_collection_for_update(
            &backend,
            &tenant_id,
            &db_name,
            &original_name,
            Some(128),
            None,
        )
        .await;

        // Update both name and dimension
        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: collection_id,
            name: Some(new_name.clone()),
            dimension: Some(512),
            metadata: None,
            reset_metadata: false,
            new_configuration: None,
        };

        let result = backend.update_collection(update_req).await;
        assert!(
            result.is_ok(),
            "Failed to update collection name and dimension: {:?}",
            result.err()
        );

        // Verify via get_collections + verify_new_collection
        let collection = fetch_collection(&backend, collection_id, &tenant_id, &db_name).await;
        verify_new_collection(
            &collection,
            collection_id,
            &new_name,
            Some(512),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_metadata_replace() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let name = format!("test_coll_meta_{}", Uuid::new_v4());

        let original_metadata: chroma_types::Metadata = [
            (
                "key1".to_string(),
                chroma_types::MetadataValue::Str("value1".to_string()),
            ),
            ("key2".to_string(), chroma_types::MetadataValue::Int(42)),
        ]
        .into_iter()
        .collect();

        let collection_id = create_collection_for_update(
            &backend,
            &tenant_id,
            &db_name,
            &name,
            Some(128),
            Some(original_metadata),
        )
        .await;

        // Update metadata (replace with new keys)
        let new_metadata: chroma_types::Metadata = [
            ("key3".to_string(), chroma_types::MetadataValue::Float(1.5)),
            ("key4".to_string(), chroma_types::MetadataValue::Bool(true)),
        ]
        .into_iter()
        .collect();

        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: collection_id,
            name: None,
            dimension: None,
            metadata: Some(new_metadata.clone()),
            reset_metadata: false,
            new_configuration: None,
        };

        let result = backend.update_collection(update_req).await;
        assert!(
            result.is_ok(),
            "Failed to update collection metadata: {:?}",
            result.err()
        );

        // Verify via get_collections - old metadata should be gone, new metadata present
        let collection = fetch_collection(&backend, collection_id, &tenant_id, &db_name).await;
        verify_new_collection(
            &collection,
            collection_id,
            &name,
            Some(128),
            &tenant_id,
            &db_name,
            Some(&new_metadata),
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_metadata_reset() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let name = format!("test_coll_reset_{}", Uuid::new_v4());

        let original_metadata: chroma_types::Metadata = [
            (
                "key1".to_string(),
                chroma_types::MetadataValue::Str("value1".to_string()),
            ),
            ("key2".to_string(), chroma_types::MetadataValue::Int(42)),
        ]
        .into_iter()
        .collect();

        let collection_id = create_collection_for_update(
            &backend,
            &tenant_id,
            &db_name,
            &name,
            Some(128),
            Some(original_metadata),
        )
        .await;

        // Reset metadata (delete all)
        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: collection_id,
            name: None,
            dimension: None,
            metadata: None,
            reset_metadata: true,
            new_configuration: None,
        };

        let result = backend.update_collection(update_req).await;
        assert!(
            result.is_ok(),
            "Failed to reset collection metadata: {:?}",
            result.err()
        );

        // Verify via get_collections - metadata should be empty/None
        let collection = fetch_collection(&backend, collection_id, &tenant_id, &db_name).await;
        verify_new_collection(
            &collection,
            collection_id,
            &name,
            Some(128),
            &tenant_id,
            &db_name,
            None, // metadata should be gone
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_name_conflict() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let name1 = format!("test_coll_conflict1_{}", Uuid::new_v4());
        let name2 = format!("test_coll_conflict2_{}", Uuid::new_v4());

        // Create two collections
        let collection_id1 =
            create_collection_for_update(&backend, &tenant_id, &db_name, &name1, Some(128), None)
                .await;

        let _collection_id2 =
            create_collection_for_update(&backend, &tenant_id, &db_name, &name2, Some(256), None)
                .await;

        // Try to rename collection1 to collection2's name (should fail)
        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: collection_id1,
            name: Some(name2.clone()),
            dimension: None,
            metadata: None,
            reset_metadata: false,
            new_configuration: None,
        };

        let result = backend.update_collection(update_req).await;
        assert!(result.is_err(), "Should fail to update to conflicting name");

        match result.unwrap_err() {
            SysDbError::AlreadyExists(msg) => {
                assert!(
                    msg.contains(&name2),
                    "Error message should mention conflicting name"
                );
            }
            e => panic!("Expected AlreadyExists error, got: {:?}", e),
        }

        // Verify original collection is unchanged
        let collection = fetch_collection(&backend, collection_id1, &tenant_id, &db_name).await;
        verify_new_collection(
            &collection,
            collection_id1,
            &name1, // should still have original name
            Some(128),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_not_found() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        // Setup tenant/database but don't create collection
        let (_tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let nonexistent_id = CollectionUuid(Uuid::new_v4());

        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: nonexistent_id,
            name: Some("new_name".to_string()),
            dimension: None,
            metadata: None,
            reset_metadata: false,
            new_configuration: None,
        };

        let result = backend.update_collection(update_req).await;
        assert!(result.is_err(), "Should fail for nonexistent collection");

        match result.unwrap_err() {
            SysDbError::NotFound(msg) => {
                assert!(
                    msg.contains(&nonexistent_id.0.to_string()),
                    "Error message should mention collection ID"
                );
            }
            e => panic!("Expected NotFound error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_no_changes() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let name = format!("test_coll_noop_{}", Uuid::new_v4());

        let metadata: chroma_types::Metadata = [(
            "key1".to_string(),
            chroma_types::MetadataValue::Str("value1".to_string()),
        )]
        .into_iter()
        .collect();

        let collection_id = create_collection_for_update(
            &backend,
            &tenant_id,
            &db_name,
            &name,
            Some(128),
            Some(metadata.clone()),
        )
        .await;

        // Update with no changes (all None/false)
        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: collection_id,
            name: None,
            dimension: None,
            metadata: None,
            reset_metadata: false,
            new_configuration: None,
        };

        let result = backend.update_collection(update_req).await;
        assert!(
            result.is_ok(),
            "No-op update should succeed: {:?}",
            result.err()
        );

        // Verify collection is unchanged
        let collection = fetch_collection(&backend, collection_id, &tenant_id, &db_name).await;
        verify_new_collection(
            &collection,
            collection_id,
            &name,
            Some(128),
            &tenant_id,
            &db_name,
            Some(&metadata),
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_all_fields() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let original_name = format!("test_coll_all_orig_{}", Uuid::new_v4());
        let new_name = format!("test_coll_all_new_{}", Uuid::new_v4());

        let original_metadata: chroma_types::Metadata = [(
            "old_key".to_string(),
            chroma_types::MetadataValue::Str("old_value".to_string()),
        )]
        .into_iter()
        .collect();

        let collection_id = create_collection_for_update(
            &backend,
            &tenant_id,
            &db_name,
            &original_name,
            Some(128),
            Some(original_metadata),
        )
        .await;

        // Update name, dimension, and metadata all at once
        let new_metadata: chroma_types::Metadata =
            [("new_key".to_string(), chroma_types::MetadataValue::Int(999))]
                .into_iter()
                .collect();

        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: collection_id,
            name: Some(new_name.clone()),
            dimension: Some(1024),
            metadata: Some(new_metadata.clone()),
            reset_metadata: false,
            new_configuration: None,
        };

        let result = backend.update_collection(update_req).await;
        assert!(
            result.is_ok(),
            "Failed to update all collection fields: {:?}",
            result.err()
        );

        // Verify all fields updated
        let collection = fetch_collection(&backend, collection_id, &tenant_id, &db_name).await;
        verify_new_collection(
            &collection,
            collection_id,
            &new_name,
            Some(1024),
            &tenant_id,
            &db_name,
            Some(&new_metadata),
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_set_dimension_from_none() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let name = format!("test_coll_null_dim_{}", Uuid::new_v4());

        // Create collection with no dimension
        let collection_id = create_collection_for_update(
            &backend, &tenant_id, &db_name, &name, None, // No dimension initially
            None,
        )
        .await;

        // Set dimension
        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: collection_id,
            name: None,
            dimension: Some(384),
            metadata: None,
            reset_metadata: false,
            new_configuration: None,
        };

        let result = backend.update_collection(update_req).await;
        assert!(
            result.is_ok(),
            "Failed to set dimension from None: {:?}",
            result.err()
        );

        // Verify dimension is now set
        let collection = fetch_collection(&backend, collection_id, &tenant_id, &db_name).await;
        verify_new_collection(
            &collection,
            collection_id,
            &name,
            Some(384),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_hnsw_config_rejected() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let name = format!("test_coll_hnsw_{}", Uuid::new_v4());

        let collection_id =
            create_collection_for_update(&backend, &tenant_id, &db_name, &name, Some(128), None)
                .await;

        // Try to update HNSW config (should be rejected)
        let hnsw_config = chroma_types::UpdateHnswConfiguration {
            ef_search: Some(100),
            max_neighbors: None,
            num_threads: None,
            resize_factor: None,
            sync_threshold: None,
            batch_size: None,
        };

        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: collection_id,
            name: None,
            dimension: None,
            metadata: None,
            reset_metadata: false,
            new_configuration: Some(chroma_types::UpdateCollectionConfiguration {
                hnsw: Some(hnsw_config),
                spann: None,
                embedding_function: None,
            }),
        };

        let result = backend.update_collection(update_req).await;
        assert!(result.is_err(), "HNSW config update should be rejected");

        match result.unwrap_err() {
            SysDbError::Schema(_) => {
                // Expected - Schema error from apply_update_configuration
            }
            e => panic!("Expected Schema error for HNSW update, got: {:?}", e),
        }

        // Verify collection is unchanged
        let collection = fetch_collection(&backend, collection_id, &tenant_id, &db_name).await;
        verify_new_collection(
            &collection,
            collection_id,
            &name,
            Some(128),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_rename_to_same_name() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let name = format!("test_coll_same_{}", Uuid::new_v4());

        let collection_id =
            create_collection_for_update(&backend, &tenant_id, &db_name, &name, Some(128), None)
                .await;

        // Update name to same name (should succeed - it's a no-op effectively)
        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: collection_id,
            name: Some(name.clone()),
            dimension: None,
            metadata: None,
            reset_metadata: false,
            new_configuration: None,
        };

        let result = backend.update_collection(update_req).await;
        assert!(
            result.is_ok(),
            "Renaming to same name should succeed: {:?}",
            result.err()
        );

        // Verify collection unchanged
        let collection = fetch_collection(&backend, collection_id, &tenant_id, &db_name).await;
        verify_new_collection(
            &collection,
            collection_id,
            &name,
            Some(128),
            &tenant_id,
            &db_name,
            None,
            Some(&Schema::default()),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_embedding_function() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let name = format!("test_coll_ef_{}", Uuid::new_v4());

        let collection_id =
            create_collection_for_update(&backend, &tenant_id, &db_name, &name, Some(128), None)
                .await;

        // Update embedding function
        let new_ef = chroma_types::EmbeddingFunctionConfiguration::Known(
            chroma_types::EmbeddingFunctionNewConfiguration {
                name: "test_embedding_function".to_string(),
                config: serde_json::json!({"model": "test-model"}),
            },
        );

        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: collection_id,
            name: None,
            dimension: None,
            metadata: None,
            reset_metadata: false,
            new_configuration: Some(chroma_types::UpdateCollectionConfiguration {
                hnsw: None,
                spann: None,
                embedding_function: Some(new_ef.clone()),
            }),
        };

        let result = backend.update_collection(update_req).await;
        assert!(
            result.is_ok(),
            "Failed to update embedding function: {:?}",
            result.err()
        );

        // Build expected schema: default schema with updated embedding function
        let mut expected_schema = Schema::default();
        expected_schema
            .apply_update_configuration(&chroma_types::UpdateCollectionConfiguration {
                hnsw: None,
                spann: None,
                embedding_function: Some(new_ef),
            })
            .expect("apply_update_configuration should succeed");

        // Verify all fields exhaustively
        let collection = fetch_collection(&backend, collection_id, &tenant_id, &db_name).await;
        verify_new_collection(
            &collection,
            collection_id,
            &name,
            Some(128),
            &tenant_id,
            &db_name,
            None,
            Some(&expected_schema),
        );
    }

    #[tokio::test]
    async fn test_k8s_mcmr_integration_update_collection_spann_config() {
        let Some(backend) = setup_test_backend().await else {
            panic!("Skipping test: Spanner emulator not reachable. Is Tilt running?");
        };

        let (tenant_id, db_name) = setup_tenant_and_database(&backend).await;

        let name = format!("test_coll_spann_{}", Uuid::new_v4());

        // Create collection with SPANN schema
        let spann_schema = Schema::new_default(chroma_types::KnnIndex::Spann);
        let collection_id = create_collection_for_update_with_schema(
            &backend,
            &tenant_id,
            &db_name,
            &name,
            Some(128),
            None,
            spann_schema.clone(),
        )
        .await;

        // Update spann config
        let spann_update = chroma_types::UpdateSpannConfiguration {
            search_nprobe: Some(20),
            ef_search: Some(200),
        };

        let update_req = UpdateCollectionRequest {
            database_name: db_name.clone(),
            id: collection_id,
            name: None,
            dimension: None,
            metadata: None,
            reset_metadata: false,
            new_configuration: Some(chroma_types::UpdateCollectionConfiguration {
                hnsw: None,
                spann: Some(spann_update.clone()),
                embedding_function: None,
            }),
        };

        let result = backend.update_collection(update_req).await;
        assert!(
            result.is_ok(),
            "Failed to update spann config: {:?}",
            result.err()
        );

        // Build expected schema: spann schema with updated config
        let mut expected_schema = spann_schema;
        expected_schema
            .apply_update_configuration(&chroma_types::UpdateCollectionConfiguration {
                hnsw: None,
                spann: Some(spann_update),
                embedding_function: None,
            })
            .expect("apply_update_configuration should succeed");

        // Verify all fields exhaustively
        let collection = fetch_collection(&backend, collection_id, &tenant_id, &db_name).await;
        verify_new_collection(
            &collection,
            collection_id,
            &name,
            Some(128),
            &tenant_id,
            &db_name,
            None,
            Some(&expected_schema),
        );
    }
}
