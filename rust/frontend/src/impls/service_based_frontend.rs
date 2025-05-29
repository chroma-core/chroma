use crate::{
    config::FrontendConfig, executor::Executor, types::errors::ValidationError,
    CollectionsWithSegmentsProvider,
};
use backon::{ExponentialBuilder, Retryable};
use chroma_config::{registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::{LocalCompactionManager, LocalCompactionManagerConfig, Log};
use chroma_segment::local_segment_manager::LocalSegmentManager;
use chroma_sqlite::db::SqliteDb;
use chroma_sysdb::SysDb;
use chroma_system::System;
use chroma_tracing::meter_event::{MeterEvent, ReadAction, WriteAction};
use chroma_types::{
    operator::{Filter, KnnBatch, KnnProjection, Limit, Projection, Scan},
    plan::{Count, Get, Knn},
    AddCollectionRecordsError, AddCollectionRecordsRequest, AddCollectionRecordsResponse,
    Collection, CollectionUuid, CountCollectionsError, CountCollectionsRequest,
    CountCollectionsResponse, CountRequest, CountResponse, CreateCollectionError,
    CreateCollectionRequest, CreateCollectionResponse, CreateDatabaseError, CreateDatabaseRequest,
    CreateDatabaseResponse, CreateTenantError, CreateTenantRequest, CreateTenantResponse,
    DeleteCollectionError, DeleteCollectionRecordsError, DeleteCollectionRecordsRequest,
    DeleteCollectionRecordsResponse, DeleteCollectionRequest, DeleteDatabaseError,
    DeleteDatabaseRequest, DeleteDatabaseResponse, ForkCollectionError, ForkCollectionRequest,
    ForkCollectionResponse, GetCollectionError, GetCollectionRequest, GetCollectionResponse,
    GetCollectionsError, GetDatabaseError, GetDatabaseRequest, GetDatabaseResponse, GetRequest,
    GetResponse, GetTenantError, GetTenantRequest, GetTenantResponse, HealthCheckResponse,
    HeartbeatError, HeartbeatResponse, Include, KnnIndex, ListCollectionsRequest,
    ListCollectionsResponse, ListDatabasesError, ListDatabasesRequest, ListDatabasesResponse,
    Operation, OperationRecord, QueryError, QueryRequest, QueryResponse, ResetError, ResetResponse,
    Segment, SegmentScope, SegmentType, SegmentUuid, UpdateCollectionError,
    UpdateCollectionRecordsError, UpdateCollectionRecordsRequest, UpdateCollectionRecordsResponse,
    UpdateCollectionRequest, UpdateCollectionResponse, UpsertCollectionRecordsError,
    UpsertCollectionRecordsRequest, UpsertCollectionRecordsResponse, VectorIndexConfiguration,
    Where,
};
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashSet, time::Duration};

use super::utils::to_records;

#[derive(Debug)]
struct Metrics {
    fork_retries_counter: Counter<u64>,
    delete_retries_counter: Counter<u64>,
    count_retries_counter: Counter<u64>,
    query_retries_counter: Counter<u64>,
    get_retries_counter: Counter<u64>,
    add_retries_counter: Counter<u64>,
    update_retries_counter: Counter<u64>,
    upsert_retries_counter: Counter<u64>,
}

#[derive(Clone, Debug)]
pub struct ServiceBasedFrontend {
    allow_reset: bool,
    executor: Executor,
    log_client: Log,
    sysdb_client: SysDb,
    collections_with_segments_provider: CollectionsWithSegmentsProvider,
    max_batch_size: u32,
    metrics: Arc<Metrics>,
    default_knn_index: KnnIndex,
    retries_builder: ExponentialBuilder,
    tenants_to_migrate_immediately: HashSet<String>,
}

impl ServiceBasedFrontend {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        allow_reset: bool,
        sysdb_client: SysDb,
        collections_with_segments_provider: CollectionsWithSegmentsProvider,
        log_client: Log,
        executor: Executor,
        max_batch_size: u32,
        default_knn_index: KnnIndex,
        tenants_to_migrate_immediately: HashSet<String>,
    ) -> Self {
        let meter = global::meter("chroma");
        let fork_retries_counter = meter.u64_counter("fork_retries").build();
        let delete_retries_counter = meter.u64_counter("delete_retries").build();
        let count_retries_counter = meter.u64_counter("count_retries").build();
        let query_retries_counter = meter.u64_counter("query_retries").build();
        let get_retries_counter = meter.u64_counter("get_retries").build();
        let add_retries_counter = meter.u64_counter("add_retries").build();
        let update_retries_counter = meter.u64_counter("update_retries").build();
        let upsert_retries_counter = meter.u64_counter("upsert_retries").build();
        let metrics = Arc::new(Metrics {
            fork_retries_counter,
            delete_retries_counter,
            count_retries_counter,
            query_retries_counter,
            get_retries_counter,
            add_retries_counter,
            update_retries_counter,
            upsert_retries_counter,
        });
        // factor: 2.0,
        // min_delay_ms: 100,
        // max_delay_ms: 5000,
        // max_attempts: 5,
        // jitter: true,
        // TODO(Sanket): Ideally config for this.
        let retries_builder = ExponentialBuilder::default()
            .with_max_times(5)
            .with_factor(2.0)
            .with_max_delay(Duration::from_millis(5000))
            .with_min_delay(Duration::from_millis(100))
            .with_jitter();
        ServiceBasedFrontend {
            allow_reset,
            executor,
            log_client,
            sysdb_client,
            collections_with_segments_provider,
            max_batch_size,
            metrics,
            default_knn_index,
            retries_builder,
            tenants_to_migrate_immediately,
        }
    }

    pub fn get_default_knn_index(&self) -> KnnIndex {
        self.default_knn_index
    }

    pub async fn heartbeat(&self) -> Result<HeartbeatResponse, HeartbeatError> {
        Ok(HeartbeatResponse {
            nanosecond_heartbeat: SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        })
    }

    pub fn get_max_batch_size(&mut self) -> u32 {
        self.max_batch_size
    }

    pub fn get_supported_segment_types(&self) -> Vec<SegmentType> {
        self.executor.get_supported_segment_types()
    }

    pub async fn get_cached_collection(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<Collection, GetCollectionError> {
        Ok(self
            .collections_with_segments_provider
            .get_collection_with_segments(collection_id)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?
            .collection)
    }

    async fn get_collection_dimension(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<Option<u32>, GetCollectionError> {
        Ok(self
            .collections_with_segments_provider
            .get_collection_with_segments(collection_id)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?
            .collection
            .dimension
            .map(|dim| dim as u32))
    }

    async fn set_collection_dimension(
        &mut self,
        collection_id: CollectionUuid,
        dimension: u32,
    ) -> Result<UpdateCollectionResponse, UpdateCollectionError> {
        self.sysdb_client
            .update_collection(collection_id, None, None, Some(dimension), None)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        // Invalidate the cache.
        self.collections_with_segments_provider
            .collections_with_segments_cache
            .remove(&collection_id)
            .await;
        Ok(UpdateCollectionResponse {})
    }

    async fn validate_embedding<Embedding, F>(
        &mut self,
        collection_id: CollectionUuid,
        option_embeddings: Option<&Vec<Embedding>>,
        update_if_not_present: bool,
        read_length: F,
    ) -> Result<(), ValidationError>
    where
        F: Fn(&Embedding) -> Option<usize>,
    {
        if let Some(embeddings) = option_embeddings {
            let emb_dims = embeddings
                .iter()
                .filter_map(read_length)
                .collect::<Vec<_>>();
            let min_dim = emb_dims.iter().min().cloned();
            let max_dim = emb_dims.iter().max().cloned();
            let emb_dim = if let (Some(low), Some(high)) = (min_dim, max_dim) {
                if low != high {
                    return Err(ValidationError::DimensionInconsistent);
                }
                low as u32
            } else {
                // No embedding to check, return
                return Ok(());
            };
            match self.get_collection_dimension(collection_id).await {
                Ok(Some(expected_dim)) => {
                    if expected_dim != emb_dim {
                        return Err(ValidationError::DimensionMismatch(expected_dim, emb_dim));
                    }

                    Ok(())
                }
                Ok(None) => {
                    if update_if_not_present {
                        self.set_collection_dimension(collection_id, emb_dim)
                            .await?;
                    }
                    Ok(())
                }
                Err(err) => Err(err.into()),
            }
        } else {
            Ok(())
        }
    }

    pub async fn reset(&mut self) -> Result<ResetResponse, ResetError> {
        if !self.allow_reset {
            return Err(ResetError::NotAllowed);
        }
        self.collections_with_segments_provider
            .collections_with_segments_cache
            .clear()
            .await
            .map_err(|err| ResetError::Cache(Box::new(err)))?;
        self.executor.reset().await.map_err(|err| err.boxed())?;
        self.sysdb_client.reset().await?;
        self.log_client.reset().await?;
        Ok(ResetResponse {})
    }

    pub async fn create_tenant(
        &mut self,
        CreateTenantRequest { name, .. }: CreateTenantRequest,
    ) -> Result<CreateTenantResponse, CreateTenantError> {
        self.sysdb_client.create_tenant(name).await
    }

    pub async fn get_tenant(
        &mut self,
        GetTenantRequest { name, .. }: GetTenantRequest,
    ) -> Result<GetTenantResponse, GetTenantError> {
        self.sysdb_client.get_tenant(name).await
    }

    pub async fn create_database(
        &mut self,
        CreateDatabaseRequest {
            database_id,
            tenant_id,
            database_name,
            ..
        }: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse, CreateDatabaseError> {
        self.sysdb_client
            .create_database(database_id, database_name, tenant_id)
            .await
    }

    pub async fn list_databases(
        &mut self,
        ListDatabasesRequest {
            tenant_id,
            limit,
            offset,
            ..
        }: ListDatabasesRequest,
    ) -> Result<ListDatabasesResponse, ListDatabasesError> {
        self.sysdb_client
            .list_databases(tenant_id, limit, offset)
            .await
    }

    pub async fn get_database(
        &mut self,
        GetDatabaseRequest {
            tenant_id,
            database_name,
            ..
        }: GetDatabaseRequest,
    ) -> Result<GetDatabaseResponse, GetDatabaseError> {
        self.sysdb_client
            .get_database(database_name, tenant_id)
            .await
    }

    pub async fn delete_database(
        &mut self,
        DeleteDatabaseRequest {
            tenant_id,
            database_name,
            ..
        }: DeleteDatabaseRequest,
    ) -> Result<DeleteDatabaseResponse, DeleteDatabaseError> {
        self.sysdb_client
            .delete_database(database_name, tenant_id)
            .await
    }

    pub async fn list_collections(
        &mut self,
        ListCollectionsRequest {
            tenant_id,
            database_name,
            limit,
            offset,
            ..
        }: ListCollectionsRequest,
    ) -> Result<ListCollectionsResponse, GetCollectionsError> {
        self.sysdb_client
            .get_collections(
                None,
                None,
                Some(tenant_id),
                Some(database_name),
                limit,
                offset,
            )
            .await
    }

    pub async fn count_collections(
        &mut self,
        CountCollectionsRequest {
            tenant_id,
            database_name,
            ..
        }: CountCollectionsRequest,
    ) -> Result<CountCollectionsResponse, CountCollectionsError> {
        self.sysdb_client
            .count_collections(tenant_id, Some(database_name))
            .await
            .map(|count| count as u32)
    }

    pub async fn get_collection(
        &mut self,
        GetCollectionRequest {
            tenant_id,
            database_name,
            collection_name,
            ..
        }: GetCollectionRequest,
    ) -> Result<GetCollectionResponse, GetCollectionError> {
        let mut collections = self
            .sysdb_client
            .get_collections(
                None,
                Some(collection_name.clone()),
                Some(tenant_id),
                Some(database_name),
                None,
                0,
            )
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        collections
            .pop()
            .ok_or(GetCollectionError::NotFound(collection_name))
    }

    pub async fn create_collection(
        &mut self,
        CreateCollectionRequest {
            tenant_id,
            database_name,
            name,
            metadata,
            configuration,
            get_or_create,
            ..
        }: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse, CreateCollectionError> {
        let collection_id = CollectionUuid::new();

        let supported_segment_types: HashSet<SegmentType> =
            self.get_supported_segment_types().into_iter().collect();

        if let Some(config) = configuration.as_ref() {
            match &config.vector_index {
                VectorIndexConfiguration::Spann { .. } => {
                    if !supported_segment_types.contains(&SegmentType::Spann) {
                        return Err(CreateCollectionError::SpannNotImplemented);
                    }
                }
                VectorIndexConfiguration::Hnsw { .. } => {
                    if !supported_segment_types.contains(&SegmentType::HnswDistributed)
                        && !supported_segment_types.contains(&SegmentType::HnswLocalMemory)
                        && !supported_segment_types.contains(&SegmentType::HnswLocalPersisted)
                    {
                        return Err(CreateCollectionError::HnswNotSupported);
                    }
                }
            }
        }

        // Check default server configuration's index type
        match self.default_knn_index {
            KnnIndex::Spann => {
                if !supported_segment_types.contains(&SegmentType::Spann) {
                    return Err(CreateCollectionError::SpannNotImplemented);
                }
            }
            KnnIndex::Hnsw => {
                if !supported_segment_types.contains(&SegmentType::HnswDistributed)
                    && !supported_segment_types.contains(&SegmentType::HnswLocalMemory)
                    && !supported_segment_types.contains(&SegmentType::HnswLocalPersisted)
                {
                    return Err(CreateCollectionError::HnswNotSupported);
                }
            }
        }

        let segments = match self.executor {
            Executor::Distributed(_) => {
                let mut vector_segment_type = SegmentType::HnswDistributed;
                if let Some(config) = configuration.as_ref() {
                    if matches!(config.vector_index, VectorIndexConfiguration::Spann(_)) {
                        vector_segment_type = SegmentType::Spann;
                    }
                }

                vec![
                    Segment {
                        id: SegmentUuid::new(),
                        r#type: vector_segment_type,
                        scope: SegmentScope::VECTOR,
                        collection: collection_id,
                        metadata: None,
                        file_path: Default::default(),
                    },
                    Segment {
                        id: SegmentUuid::new(),
                        r#type: SegmentType::BlockfileMetadata,
                        scope: SegmentScope::METADATA,
                        collection: collection_id,
                        metadata: None,
                        file_path: Default::default(),
                    },
                    Segment {
                        id: SegmentUuid::new(),
                        r#type: SegmentType::BlockfileRecord,
                        scope: SegmentScope::RECORD,
                        collection: collection_id,
                        metadata: None,
                        file_path: Default::default(),
                    },
                ]
            }
            Executor::Local(_) => {
                vec![
                    Segment {
                        id: SegmentUuid::new(),
                        r#type: SegmentType::HnswLocalPersisted,
                        scope: SegmentScope::VECTOR,
                        collection: collection_id,
                        metadata: None,
                        file_path: Default::default(),
                    },
                    Segment {
                        id: SegmentUuid::new(),
                        r#type: SegmentType::Sqlite,
                        scope: SegmentScope::METADATA,
                        collection: collection_id,
                        metadata: None,
                        file_path: Default::default(),
                    },
                ]
            }
        };

        let collection = self
            .sysdb_client
            .create_collection(
                tenant_id.clone(),
                database_name,
                collection_id,
                name,
                segments,
                configuration,
                metadata,
                None,
                get_or_create,
            )
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        self.collections_with_segments_provider
            .collections_with_segments_cache
            .remove(&collection_id)
            .await;
        if self.tenant_is_on_new_log_by_default(&tenant_id) {
            if let Err(err) = self.log_client.seal_log(&tenant_id, collection_id).await {
                tracing::error!("could not seal collection right away: {err}");
            }
        }
        Ok(collection)
    }

    fn tenant_is_on_new_log_by_default(&self, tenant_id: &str) -> bool {
        self.tenants_to_migrate_immediately.contains(tenant_id)
    }

    pub async fn update_collection(
        &mut self,
        UpdateCollectionRequest {
            collection_id,
            new_name,
            new_metadata,
            new_configuration,
            ..
        }: UpdateCollectionRequest,
    ) -> Result<UpdateCollectionResponse, UpdateCollectionError> {
        self.sysdb_client
            .update_collection(
                collection_id,
                new_name,
                new_metadata,
                None,
                new_configuration,
            )
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        // Invalidate the cache.
        self.collections_with_segments_provider
            .collections_with_segments_cache
            .remove(&collection_id)
            .await;

        Ok(UpdateCollectionResponse {})
    }

    pub async fn delete_collection(
        &mut self,
        DeleteCollectionRequest {
            tenant_id,
            database_name,
            collection_name,
            ..
        }: DeleteCollectionRequest,
    ) -> Result<DeleteCollectionRecordsResponse, DeleteCollectionError> {
        let collection = self
            .get_collection(
                GetCollectionRequest::try_new(
                    tenant_id.clone(),
                    database_name.clone(),
                    collection_name,
                )
                .map_err(DeleteCollectionError::Validation)?,
            )
            .await?;

        let segments = self
            .sysdb_client
            .get_segments(None, None, None, collection.collection_id)
            .await
            .map_err(|e| e.boxed())?;

        self.sysdb_client
            .delete_collection(
                tenant_id,
                database_name,
                collection.collection_id,
                segments.into_iter().map(|s| s.id).collect(),
            )
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        // Invalidate the cache.
        self.collections_with_segments_provider
            .collections_with_segments_cache
            .remove(&collection.collection_id)
            .await;

        Ok(DeleteCollectionRecordsResponse {})
    }

    pub async fn retryable_fork(
        &mut self,
        ForkCollectionRequest {
            tenant_id,
            database_name,
            source_collection_id,
            target_collection_name,
            ..
        }: ForkCollectionRequest,
    ) -> Result<ForkCollectionResponse, ForkCollectionError> {
        let target_collection_id = CollectionUuid::new();
        let log_offsets = self
            .log_client
            .fork_logs(&tenant_id, source_collection_id, target_collection_id)
            .await?;
        let collection_and_segments = self
            .sysdb_client
            .fork_collection(
                source_collection_id,
                log_offsets.compaction_offset,
                log_offsets.enumeration_offset,
                target_collection_id,
                target_collection_name,
            )
            .await?;
        let collection = collection_and_segments.collection.clone();
        let latest_collection_logical_size_bytes = collection_and_segments
            .collection
            .size_bytes_post_compaction;

        // Update the cache.
        self.collections_with_segments_provider
            .set_collection_with_segments(collection_and_segments)
            .await;

        // TODO: Submit event after the response is sent
        MeterEvent::CollectionFork {
            tenant: tenant_id,
            database: database_name,
            collection_id: source_collection_id.0,
            latest_collection_logical_size_bytes,
        }
        .submit()
        .await;

        Ok(collection)
    }

    pub async fn fork_collection(
        &mut self,
        request: ForkCollectionRequest,
    ) -> Result<ForkCollectionResponse, ForkCollectionError> {
        let retries = Arc::new(AtomicUsize::new(0));
        let fork_to_retry = || {
            let mut self_clone = self.clone();
            let request_clone = request.clone();
            async move { self_clone.retryable_fork(request_clone).await }
        };

        let res = fork_to_retry
            .retry(self.collections_with_segments_provider.get_retry_backoff())
            // NOTE: Transport level errors will manifest as unknown errors, and they should also be retried
            .when(|e| {
                matches!(
                    e.code(),
                    ErrorCodes::FailedPrecondition | ErrorCodes::Unknown
                )
            })
            .notify(|_, _| {
                let retried = retries.fetch_add(1, Ordering::Relaxed);
                if retried > 0 {
                    tracing::info!(
                        "Retrying fork() request for collection {}",
                        request.source_collection_id
                    );
                }
            })
            .await;
        self.metrics
            .fork_retries_counter
            .add(retries.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    pub async fn retryable_push_logs(
        &mut self,
        tenant_id: &str,
        collection_id: CollectionUuid,
        records: Vec<OperationRecord>,
    ) -> Result<(), Box<dyn ChromaError>> {
        self.log_client
            .push_logs(tenant_id, collection_id, records)
            .await
    }

    pub async fn add(
        &mut self,
        AddCollectionRecordsRequest {
            tenant_id,
            database_name,
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        }: AddCollectionRecordsRequest,
    ) -> Result<AddCollectionRecordsResponse, AddCollectionRecordsError> {
        self.validate_embedding(collection_id, embeddings.as_ref(), true, |embedding| {
            Some(embedding.len())
        })
        .await
        .map_err(|err| err.boxed())?;

        let embeddings = embeddings.map(|embeddings| embeddings.into_iter().map(Some).collect());

        let (records, log_size_bytes) =
            to_records(ids, embeddings, documents, uris, metadatas, Operation::Add)
                .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        let retries = Arc::new(AtomicUsize::new(0));
        let add_to_retry = || {
            let mut self_clone = self.clone();
            let records_clone = records.clone();
            let tenant_id_clone = tenant_id.clone();
            async move {
                self_clone
                    .retryable_push_logs(&tenant_id_clone, collection_id, records_clone)
                    .await
            }
        };
        let res = add_to_retry
            .retry(self.retries_builder)
            .when(|e| matches!(e.code(), ErrorCodes::AlreadyExists))
            .notify(|_, _| {
                let retried = retries.fetch_add(1, Ordering::Relaxed);
                if retried > 0 {
                    tracing::info!("Retrying add() request for collection {}", collection_id);
                }
            })
            .await;
        self.metrics
            .add_retries_counter
            .add(retries.load(Ordering::Relaxed) as u64, &[]);

        // TODO: Submit event after the response is sent
        MeterEvent::CollectionWrite {
            tenant: tenant_id,
            database: database_name,
            collection_id: collection_id.0,
            action: WriteAction::Add,
            log_size_bytes,
        }
        .submit()
        .await;

        match res {
            Ok(()) => Ok(AddCollectionRecordsResponse {}),
            Err(e) => {
                if e.code() == ErrorCodes::AlreadyExists {
                    Err(AddCollectionRecordsError::Backoff)
                } else {
                    Err(AddCollectionRecordsError::Other(Box::new(e) as _))
                }
            }
        }
    }

    pub async fn update(
        &mut self,
        UpdateCollectionRecordsRequest {
            tenant_id,
            database_name,
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        }: UpdateCollectionRecordsRequest,
    ) -> Result<UpdateCollectionRecordsResponse, UpdateCollectionRecordsError> {
        self.validate_embedding(collection_id, embeddings.as_ref(), true, |embedding| {
            embedding.as_ref().map(|emb| emb.len())
        })
        .await
        .map_err(|err| err.boxed())?;

        let (records, log_size_bytes) = to_records(
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            Operation::Update,
        )
        .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        let retries = Arc::new(AtomicUsize::new(0));
        let add_to_retry = || {
            let mut self_clone = self.clone();
            let records_clone = records.clone();
            let tenant_id_clone = tenant_id.clone();
            async move {
                self_clone
                    .retryable_push_logs(&tenant_id_clone, collection_id, records_clone)
                    .await
            }
        };
        let res = add_to_retry
            .retry(self.retries_builder)
            .when(|e| matches!(e.code(), ErrorCodes::AlreadyExists))
            .notify(|_, _| {
                let retried = retries.fetch_add(1, Ordering::Relaxed);
                if retried > 0 {
                    tracing::info!("Retrying update() request for collection {}", collection_id);
                }
            })
            .await;
        self.metrics
            .update_retries_counter
            .add(retries.load(Ordering::Relaxed) as u64, &[]);

        // TODO: Submit event after the response is sent
        MeterEvent::CollectionWrite {
            tenant: tenant_id,
            database: database_name,
            collection_id: collection_id.0,
            action: WriteAction::Update,
            log_size_bytes,
        }
        .submit()
        .await;

        match res {
            Ok(()) => Ok(UpdateCollectionRecordsResponse {}),
            Err(e) => {
                if e.code() == ErrorCodes::AlreadyExists {
                    Err(UpdateCollectionRecordsError::Backoff)
                } else {
                    Err(UpdateCollectionRecordsError::Other(Box::new(e) as _))
                }
            }
        }
    }

    pub async fn upsert(
        &mut self,
        UpsertCollectionRecordsRequest {
            tenant_id,
            database_name,
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        }: UpsertCollectionRecordsRequest,
    ) -> Result<UpsertCollectionRecordsResponse, UpsertCollectionRecordsError> {
        self.validate_embedding(collection_id, embeddings.as_ref(), true, |embedding| {
            Some(embedding.len())
        })
        .await
        .map_err(|err| err.boxed())?;

        let embeddings = embeddings.map(|embeddings| embeddings.into_iter().map(Some).collect());

        let (records, log_size_bytes) = to_records(
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            Operation::Upsert,
        )
        .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        let retries = Arc::new(AtomicUsize::new(0));
        let add_to_retry = || {
            let mut self_clone = self.clone();
            let records_clone = records.clone();
            let tenant_id_clone = tenant_id.clone();
            async move {
                self_clone
                    .retryable_push_logs(&tenant_id_clone, collection_id, records_clone)
                    .await
            }
        };
        let res = add_to_retry
            .retry(self.retries_builder)
            .when(|e| matches!(e.code(), ErrorCodes::AlreadyExists))
            .notify(|_, _| {
                let retried = retries.fetch_add(1, Ordering::Relaxed);
                if retried > 0 {
                    tracing::info!("Retrying upsert() request for collection {}", collection_id);
                }
            })
            .await;
        self.metrics
            .upsert_retries_counter
            .add(retries.load(Ordering::Relaxed) as u64, &[]);

        // TODO: Submit event after the response is sent
        MeterEvent::CollectionWrite {
            tenant: tenant_id,
            database: database_name,
            collection_id: collection_id.0,
            action: WriteAction::Upsert,
            log_size_bytes,
        }
        .submit()
        .await;

        match res {
            Ok(()) => Ok(UpsertCollectionRecordsResponse {}),
            Err(e) => {
                if e.code() == ErrorCodes::AlreadyExists {
                    Err(UpsertCollectionRecordsError::Backoff)
                } else {
                    Err(UpsertCollectionRecordsError::Other(Box::new(e) as _))
                }
            }
        }
    }

    pub async fn retryable_delete(
        &mut self,
        DeleteCollectionRecordsRequest {
            tenant_id,
            database_name,
            collection_id,
            ids,
            r#where,
            ..
        }: DeleteCollectionRecordsRequest,
    ) -> Result<DeleteCollectionRecordsResponse, DeleteCollectionRecordsError> {
        let mut records = Vec::new();

        let read_event = if let Some(where_clause) = r#where {
            let collection_and_segments = self
                .collections_with_segments_provider
                .get_collection_with_segments(collection_id)
                .await
                .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
            let latest_collection_logical_size_bytes = collection_and_segments
                .collection
                .size_bytes_post_compaction;
            let fts_query_length = where_clause.fts_query_length();
            let metadata_predicate_count = where_clause.metadata_predicate_count();

            let filter = Filter {
                query_ids: ids,
                where_clause: Some(where_clause),
            };

            let get_result = self
                .executor
                .get(Get {
                    scan: Scan {
                        collection_and_segments,
                    },
                    filter,
                    limit: Limit {
                        skip: 0,
                        fetch: None,
                    },
                    proj: Projection {
                        document: false,
                        embedding: false,
                        metadata: false,
                    },
                })
                .await?;

            let return_bytes = get_result.size_bytes();

            for record in get_result.result.records {
                records.push(OperationRecord {
                    id: record.id,
                    operation: Operation::Delete,
                    document: None,
                    embedding: None,
                    encoding: None,
                    metadata: None,
                });
            }
            // TODO: Submit event after the response is sent
            Some(MeterEvent::CollectionRead {
                tenant: tenant_id.clone(),
                database: database_name.clone(),
                collection_id: collection_id.0,
                action: ReadAction::GetForDelete,
                fts_query_length,
                metadata_predicate_count,
                query_embedding_count: 0,
                pulled_log_size_bytes: get_result.pulled_log_bytes,
                latest_collection_logical_size_bytes,
                return_bytes,
            })
        } else if let Some(user_ids) = ids {
            records.extend(user_ids.into_iter().map(|id| OperationRecord {
                id,
                operation: Operation::Delete,
                document: None,
                embedding: None,
                encoding: None,
                metadata: None,
            }));
            None
        } else {
            None
        };

        if records.is_empty() {
            tracing::debug!("Bailing because no records were found");
            return Ok(DeleteCollectionRecordsResponse {});
        }

        let log_size_bytes = records.iter().map(OperationRecord::size_bytes).sum();

        self.log_client
            .push_logs(&tenant_id, collection_id, records)
            .await
            .map_err(|err| {
                if err.code() == ErrorCodes::Unavailable {
                    DeleteCollectionRecordsError::Backoff
                } else {
                    DeleteCollectionRecordsError::Internal(Box::new(err) as _)
                }
            })?;

        if let Some(event) = read_event {
            event.submit().await;
        }
        // TODO: Submit event after the response is sent
        MeterEvent::CollectionWrite {
            tenant: tenant_id,
            database: database_name,
            collection_id: collection_id.0,
            action: WriteAction::Delete,
            log_size_bytes,
        }
        .submit()
        .await;

        Ok(DeleteCollectionRecordsResponse {})
    }

    pub async fn delete(
        &mut self,
        request: DeleteCollectionRecordsRequest,
    ) -> Result<DeleteCollectionRecordsResponse, DeleteCollectionRecordsError> {
        let retries = Arc::new(AtomicUsize::new(0));
        let delete_to_retry = || {
            let mut self_clone = self.clone();
            let request_clone = request.clone();
            let cache_clone = self
                .collections_with_segments_provider
                .collections_with_segments_cache
                .clone();
            async move {
                let res = self_clone.retryable_delete(request_clone).await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => {
                        if e.code() == ErrorCodes::NotFound {
                            tracing::info!(
                                "Invalidating cache for collection {}",
                                request.collection_id
                            );
                            cache_clone.remove(&request.collection_id).await;
                        }
                        Err(e)
                    }
                }
            }
        };
        let res = delete_to_retry
            .retry(self.collections_with_segments_provider.get_retry_backoff())
            // NOTE: Transport level errors will manifest as unknown errors, and they should also be retried
            .when(|e| matches!(e.code(), ErrorCodes::NotFound | ErrorCodes::Unknown))
            .notify(|_, _| {
                let retried = retries.fetch_add(1, Ordering::Relaxed);
                if retried > 0 {
                    tracing::info!(
                        "Retrying delete() request for collection {}",
                        request.collection_id
                    );
                }
            })
            .await;
        self.metrics
            .delete_retries_counter
            .add(retries.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    pub async fn retryable_count(
        &mut self,
        CountRequest {
            tenant_id,
            database_name,
            collection_id,
            ..
        }: CountRequest,
    ) -> Result<CountResponse, QueryError> {
        let collection_and_segments = self
            .collections_with_segments_provider
            .get_collection_with_segments(collection_id)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        let latest_collection_logical_size_bytes = collection_and_segments
            .collection
            .size_bytes_post_compaction;
        let count_result = self
            .executor
            .count(Count {
                scan: Scan {
                    collection_and_segments,
                },
            })
            .await?;
        let return_bytes = count_result.size_bytes();
        // TODO: Submit event after the response is sent
        MeterEvent::CollectionRead {
            tenant: tenant_id.clone(),
            database: database_name.clone(),
            collection_id: collection_id.0,
            action: ReadAction::Count,
            fts_query_length: 0,
            metadata_predicate_count: 0,
            query_embedding_count: 0,
            pulled_log_size_bytes: count_result.pulled_log_bytes,
            latest_collection_logical_size_bytes,
            return_bytes,
        }
        .submit()
        .await;
        Ok(count_result.count)
    }

    pub async fn count(&mut self, request: CountRequest) -> Result<CountResponse, QueryError> {
        let retries = Arc::new(AtomicUsize::new(0));
        let count_to_retry = || {
            let mut self_clone = self.clone();
            let request_clone = request.clone();
            let cache_clone = self
                .collections_with_segments_provider
                .collections_with_segments_cache
                .clone();
            async move {
                let res = self_clone.retryable_count(request_clone).await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => {
                        if e.code() == ErrorCodes::NotFound {
                            tracing::info!(
                                "Invalidating cache for collection {}",
                                request.collection_id
                            );
                            cache_clone.remove(&request.collection_id).await;
                        }
                        Err(e)
                    }
                }
            }
        };
        let res = count_to_retry
            .retry(self.collections_with_segments_provider.get_retry_backoff())
            // NOTE: Transport level errors will manifest as unknown errors, and they should also be retried
            .when(|e| matches!(e.code(), ErrorCodes::NotFound | ErrorCodes::Unknown))
            .notify(|_, _| {
                let retried = retries.fetch_add(1, Ordering::Relaxed);
                if retried > 0 {
                    tracing::info!(
                        "Retrying count() request for collection {}",
                        request.collection_id
                    );
                }
            })
            .await;
        self.metrics
            .count_retries_counter
            .add(retries.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    async fn retryable_get(
        &mut self,
        GetRequest {
            tenant_id,
            database_name,
            collection_id,
            ids,
            r#where,
            limit,
            offset,
            include,
            ..
        }: GetRequest,
    ) -> Result<GetResponse, QueryError> {
        let collection_and_segments = self
            .collections_with_segments_provider
            .get_collection_with_segments(collection_id)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        let latest_collection_logical_size_bytes = collection_and_segments
            .collection
            .size_bytes_post_compaction;
        let metadata_predicate_count = r#where
            .as_ref()
            .map(Where::metadata_predicate_count)
            .unwrap_or_default();
        let fts_query_length = r#where
            .as_ref()
            .map(Where::fts_query_length)
            .unwrap_or_default();
        let get_result = self
            .executor
            .get(Get {
                scan: Scan {
                    collection_and_segments,
                },
                filter: Filter {
                    query_ids: ids,
                    where_clause: r#where,
                },
                limit: Limit {
                    skip: offset,
                    fetch: limit,
                },
                proj: Projection {
                    document: include.0.contains(&Include::Document),
                    embedding: include.0.contains(&Include::Embedding),
                    // If URI is requested, metadata is also requested so we can extract the URI.
                    metadata: (include.0.contains(&Include::Metadata)
                        || include.0.contains(&Include::Uri)),
                },
            })
            .await?;
        let return_bytes = get_result.size_bytes();
        // TODO: Submit event after the response is sent
        MeterEvent::CollectionRead {
            tenant: tenant_id.clone(),
            database: database_name.clone(),
            collection_id: collection_id.0,
            action: ReadAction::Get,
            metadata_predicate_count,
            fts_query_length,
            query_embedding_count: 0,
            pulled_log_size_bytes: get_result.pulled_log_bytes,
            latest_collection_logical_size_bytes,
            return_bytes,
        }
        .submit()
        .await;
        Ok((get_result, include).into())
    }

    pub async fn get(&mut self, request: GetRequest) -> Result<GetResponse, QueryError> {
        let retries = Arc::new(AtomicUsize::new(0));
        let get_to_retry = || {
            let mut self_clone = self.clone();
            let request_clone = request.clone();
            let cache_clone = self
                .collections_with_segments_provider
                .collections_with_segments_cache
                .clone();
            async move {
                let res = self_clone.retryable_get(request_clone).await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => {
                        if e.code() == ErrorCodes::NotFound {
                            tracing::info!(
                                "Invalidating cache for collection {}",
                                request.collection_id
                            );
                            cache_clone.remove(&request.collection_id).await;
                        }
                        Err(e)
                    }
                }
            }
        };
        let res = get_to_retry
            .retry(self.collections_with_segments_provider.get_retry_backoff())
            // NOTE: Transport level errors will manifest as unknown errors, and they should also be retried
            .when(|e| matches!(e.code(), ErrorCodes::NotFound | ErrorCodes::Unknown))
            .notify(|_, _| {
                let retried = retries.fetch_add(1, Ordering::Relaxed);
                if retried > 0 {
                    tracing::info!(
                        "Retrying get() request for collection {}",
                        request.collection_id
                    );
                }
            })
            .await;
        self.metrics
            .get_retries_counter
            .add(retries.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    async fn retryable_query(
        &mut self,
        QueryRequest {
            tenant_id,
            database_name,
            collection_id,
            ids,
            r#where,
            embeddings,
            n_results,
            include,
            ..
        }: QueryRequest,
    ) -> Result<QueryResponse, QueryError> {
        let collection_and_segments = self
            .collections_with_segments_provider
            .get_collection_with_segments(collection_id)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        let latest_collection_logical_size_bytes = collection_and_segments
            .collection
            .size_bytes_post_compaction;
        let metadata_predicate_count = r#where
            .as_ref()
            .map(Where::metadata_predicate_count)
            .unwrap_or_default();
        let fts_query_length = r#where
            .as_ref()
            .map(Where::fts_query_length)
            .unwrap_or_default();
        let query_embedding_count = embeddings.len() as u64;
        let query_result = self
            .executor
            .knn(Knn {
                scan: Scan {
                    collection_and_segments,
                },
                filter: Filter {
                    query_ids: ids,
                    where_clause: r#where,
                },
                knn: KnnBatch {
                    embeddings,
                    fetch: n_results,
                },
                proj: KnnProjection {
                    projection: Projection {
                        document: include.0.contains(&Include::Document),
                        embedding: include.0.contains(&Include::Embedding),
                        // If URI is requested, metadata is also requested so we can extract the URI.
                        metadata: (include.0.contains(&Include::Metadata)
                            || include.0.contains(&Include::Uri)),
                    },
                    distance: include.0.contains(&Include::Distance),
                },
            })
            .await?;
        let return_bytes = query_result.size_bytes();
        // TODO: Submit event after the response is sent
        MeterEvent::CollectionRead {
            tenant: tenant_id.clone(),
            database: database_name.clone(),
            collection_id: collection_id.0,
            action: ReadAction::Query,
            metadata_predicate_count,
            fts_query_length,
            query_embedding_count,
            pulled_log_size_bytes: query_result.pulled_log_bytes,
            latest_collection_logical_size_bytes,
            return_bytes,
        }
        .submit()
        .await;
        Ok((query_result, include).into())
    }

    pub async fn query(&mut self, request: QueryRequest) -> Result<QueryResponse, QueryError> {
        self.validate_embedding(
            request.collection_id,
            Some(&request.embeddings),
            false,
            |embedding| Some(embedding.len()),
        )
        .await
        .map_err(|err| err.boxed())?;

        let retries = Arc::new(AtomicUsize::new(0));
        let query_to_retry = || {
            let mut self_clone = self.clone();
            let request_clone = request.clone();
            let cache_clone = self
                .collections_with_segments_provider
                .collections_with_segments_cache
                .clone();
            async move {
                let res = self_clone.retryable_query(request_clone).await;
                match res {
                    Ok(res) => Ok(res),
                    Err(e) => {
                        if e.code() == ErrorCodes::NotFound {
                            tracing::info!(
                                "Invalidating cache for collection {}",
                                request.collection_id
                            );
                            cache_clone.remove(&request.collection_id).await;
                        }
                        Err(e)
                    }
                }
            }
        };
        let res = query_to_retry
            .retry(self.collections_with_segments_provider.get_retry_backoff())
            // NOTE: Transport level errors will manifest as unknown errors, and they should also be retried
            .when(|e| matches!(e.code(), ErrorCodes::NotFound | ErrorCodes::Unknown))
            .notify(|_, _| {
                let retried = retries.fetch_add(1, Ordering::Relaxed);
                if retried > 0 {
                    tracing::info!(
                        "Retrying query() request for collection {}",
                        request.collection_id
                    );
                }
            })
            .await;
        self.metrics
            .query_retries_counter
            .add(retries.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    pub async fn healthcheck(&self) -> HealthCheckResponse {
        HealthCheckResponse {
            is_executor_ready: self.executor.is_ready().await,
        }
    }
}

#[async_trait::async_trait]
impl Configurable<(FrontendConfig, System)> for ServiceBasedFrontend {
    async fn try_from_config(
        (config, system): &(FrontendConfig, System),
        registry: &registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        // Create sqlitedb if configured
        if let Some(sqlite_conf) = &config.sqlitedb {
            SqliteDb::try_from_config(sqlite_conf, registry)
                .await
                .map_err(|e| e.boxed())?;
        };

        // Create segment manager if configured
        if let Some(segment_manager_conf) = &config.segment_manager {
            LocalSegmentManager::try_from_config(segment_manager_conf, registry).await?;
        };

        let sysdb = SysDb::try_from_config(&config.sysdb, registry).await?;
        let mut log = Log::try_from_config(&(config.log.clone(), system.clone()), registry).await?;
        let max_batch_size = log.get_max_batch_size().await?;

        // Create compation manager and pass handle to log service if configured
        if let Log::Sqlite(sqlite_log) = &log {
            let compaction_manager =
                LocalCompactionManager::try_from_config(&LocalCompactionManagerConfig {}, registry)
                    .await?;
            // TODO: Move this inside LocalCompactionManager::try_from_config, when system is stored in registry
            let handle = system.start_component(compaction_manager);
            sqlite_log
                .init_compactor_handle(handle.clone())
                .map_err(|e| e.boxed())?;
            registry.register(handle);
        }

        let collections_with_segments_provider = CollectionsWithSegmentsProvider::try_from_config(
            &config.collections_with_segments_provider.clone(),
            registry,
        )
        .await?;

        let executor =
            Executor::try_from_config(&(config.executor.clone(), system.clone()), registry).await?;

        let tenants_to_migrate_immediately = config
            .tenants_to_migrate_immediately
            .iter()
            .cloned()
            .collect::<HashSet<String>>();
        Ok(ServiceBasedFrontend::new(
            config.allow_reset,
            sysdb,
            collections_with_segments_provider,
            log,
            executor,
            max_batch_size,
            config.default_knn_index,
            tenants_to_migrate_immediately,
        ))
    }
}

#[cfg(test)]
mod tests {
    use chroma_config::registry::Registry;
    use chroma_sysdb::GrpcSysDbConfig;
    use chroma_types::Collection;
    use uuid::Uuid;

    use crate::server::CreateCollectionPayload;

    use super::*;

    #[tokio::test]
    async fn test_default_sqlite_segments() {
        // Creating a collection in SQLite should result in two segments.
        let registry = Registry::new();
        let system = System::new();
        let config = FrontendConfig::sqlite_in_memory();
        let mut frontend = ServiceBasedFrontend::try_from_config(&(config, system), &registry)
            .await
            .unwrap();

        let collection = frontend
            .create_collection(
                CreateCollectionRequest::try_new(
                    "default_tenant".to_string(),
                    "default_database".to_string(),
                    "test".to_string(),
                    None,
                    None,
                    false,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let mut sysdb: SysDb = registry.get().unwrap();
        let segments = sysdb
            .get_segments(None, None, None, collection.collection_id)
            .await
            .unwrap();

        assert_eq!(segments.len(), 2);
        assert!(segments
            .iter()
            .any(|s| s.r#type == SegmentType::Sqlite && s.scope == SegmentScope::METADATA));
        assert!(segments.iter().any(
            |s| s.r#type == SegmentType::HnswLocalPersisted && s.scope == SegmentScope::VECTOR
        ));
    }

    #[tokio::test]
    async fn test_k8s_integration_default_distributed_segments() {
        // Creating a collection in distributed should result in three segments.
        // TODO: this should use our official Rust HTTP client, once we have one
        let client = reqwest::Client::new();
        let create_response = client
            .post("http://localhost:3000/api/v2/tenants/default_tenant/databases/default_database/collections")
            .json(
                &CreateCollectionPayload { name: Uuid::new_v4().to_string(), configuration: None, metadata: None, get_or_create: false },
            )
            .send()
            .await
            .unwrap();

        assert_eq!(create_response.status(), 200);
        let collection: Collection = create_response.json().await.unwrap();

        let registry = Registry::new();
        let sysdb_config = chroma_sysdb::SysDbConfig::Grpc(GrpcSysDbConfig {
            host: "localhost".to_string(),
            port: 50051,
            ..Default::default()
        });
        let mut sysdb = SysDb::try_from_config(&sysdb_config, &registry)
            .await
            .unwrap();
        let segments = sysdb
            .get_segments(None, None, None, collection.collection_id)
            .await
            .unwrap();

        assert_eq!(segments.len(), 3);
        assert!(segments.iter().any(
            |s| s.r#type == SegmentType::BlockfileMetadata && s.scope == SegmentScope::METADATA
        ));
        assert!(
            segments.iter().any(
                |s| s.r#type == SegmentType::HnswDistributed && s.scope == SegmentScope::VECTOR
            ) || segments
                .iter()
                .any(|s| s.r#type == SegmentType::Spann && s.scope == SegmentScope::VECTOR)
        );
        assert!(segments
            .iter()
            .any(|s| s.r#type == SegmentType::BlockfileRecord && s.scope == SegmentScope::RECORD));
    }
}
