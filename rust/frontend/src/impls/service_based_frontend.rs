use crate::{
    config::FrontendConfig, executor::Executor, types::errors::ValidationError,
    CollectionsWithSegmentsProvider,
};
use backon::{ExponentialBuilder, Retryable};
use chroma_config::{registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::{LocalCompactionManager, LocalCompactionManagerConfig, Log};
use chroma_metering::{
    CollectionForkContext, CollectionReadContext, CollectionWriteContext, Enterable,
    ExternalCollectionReadContext, FinishRequest, FtsQueryLength, LatestCollectionLogicalSizeBytes,
    LogSizeBytes, MetadataPredicateCount, MeterEvent, MeteredFutureExt, PulledLogSizeBytes,
    QueryEmbeddingCount, ReturnBytes, WriteAction,
};
use chroma_segment::local_segment_manager::LocalSegmentManager;
use chroma_sqlite::db::SqliteDb;
use chroma_sysdb::{GetCollectionsOptions, SysDb};
use chroma_system::System;
use chroma_types::{
    operator::{Filter, KnnBatch, KnnProjection, Limit, Projection, Scan},
    plan::{Count, Get, Knn, Search},
    AddCollectionRecordsError, AddCollectionRecordsRequest, AddCollectionRecordsResponse,
    Collection, CollectionAndSegments, CollectionUuid, CountCollectionsError,
    CountCollectionsRequest, CountCollectionsResponse, CountRequest, CountResponse,
    CreateCollectionError, CreateCollectionRequest, CreateCollectionResponse, CreateDatabaseError,
    CreateDatabaseRequest, CreateDatabaseResponse, CreateTenantError, CreateTenantRequest,
    CreateTenantResponse, DeleteCollectionError, DeleteCollectionRecordsError,
    DeleteCollectionRecordsRequest, DeleteCollectionRecordsResponse, DeleteCollectionRequest,
    DeleteDatabaseError, DeleteDatabaseRequest, DeleteDatabaseResponse, ForkCollectionError,
    ForkCollectionRequest, ForkCollectionResponse, GetCollectionByCrnError,
    GetCollectionByCrnRequest, GetCollectionByCrnResponse, GetCollectionError,
    GetCollectionRequest, GetCollectionResponse, GetCollectionsError, GetDatabaseError,
    GetDatabaseRequest, GetDatabaseResponse, GetRequest, GetResponse, GetTenantError,
    GetTenantRequest, GetTenantResponse, HealthCheckResponse, HeartbeatError, HeartbeatResponse,
    Include, KnnIndex, ListCollectionsRequest, ListCollectionsResponse, ListDatabasesError,
    ListDatabasesRequest, ListDatabasesResponse, Operation, OperationRecord, QueryError,
    QueryRequest, QueryResponse, ResetError, ResetResponse, SearchRequest, SearchResponse, Segment,
    SegmentScope, SegmentType, SegmentUuid, UpdateCollectionError, UpdateCollectionRecordsError,
    UpdateCollectionRecordsRequest, UpdateCollectionRecordsResponse, UpdateCollectionRequest,
    UpdateCollectionResponse, UpdateTenantError, UpdateTenantRequest, UpdateTenantResponse,
    UpsertCollectionRecordsError, UpsertCollectionRecordsRequest, UpsertCollectionRecordsResponse,
    VectorIndexConfiguration, Where,
};
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

use super::utils::to_records;

#[derive(Debug)]
struct Metrics {
    fork_retries_counter: Counter<u64>,
    delete_retries_counter: Counter<u64>,
    add_retries_counter: Counter<u64>,
    update_retries_counter: Counter<u64>,
    upsert_retries_counter: Counter<u64>,
    list_db_retries_counter: Counter<u64>,
    create_db_retries_counter: Counter<u64>,
    get_db_retries_counter: Counter<u64>,
    delete_db_retries_counter: Counter<u64>,
    list_collections_retries_counter: Counter<u64>,
    count_collections_retries_counter: Counter<u64>,
    get_collection_retries_counter: Counter<u64>,
    get_collection_by_crn_retries_counter: Counter<u64>,
    create_collection_retries_counter: Counter<u64>,
    update_collection_retries_counter: Counter<u64>,
    delete_collection_retries_counter: Counter<u64>,
    get_tenant_retries_counter: Counter<u64>,
    create_tenant_retries_counter: Counter<u64>,
    update_tenant_retries_counter: Counter<u64>,
    get_collection_with_segments_counter: Counter<u64>,
    search_retries_counter: Counter<u64>,
    metering_fork_counter: Counter<u64>,
    metering_read_counter: Counter<u64>,
    metering_write_counter: Counter<u64>,
    metering_external_read_counter: Counter<u64>,
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
    retry_policy: ExponentialBuilder,
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
        retry_policy: ExponentialBuilder,
    ) -> Self {
        let meter = global::meter("chroma");
        let fork_retries_counter = meter.u64_counter("fork_retries").build();
        let delete_retries_counter = meter.u64_counter("delete_retries").build();
        let add_retries_counter = meter.u64_counter("add_retries").build();
        let update_retries_counter = meter.u64_counter("update_retries").build();
        let upsert_retries_counter = meter.u64_counter("upsert_retries").build();
        let search_retries_counter = meter.u64_counter("search_retries").build();
        let metering_fork_counter = meter.u64_counter("metering_events_sent.fork").with_description("The number of fork metering events sent by the frontend to the metering event receiver.").build();
        let metering_read_counter = meter.u64_counter("metering_events_sent.read").with_description("The number of read metering events sent by the frontend to the metering event receiver.").build();
        let metering_write_counter = meter.u64_counter("metering_events_sent.write").with_description("The number of write metering events sent by the frontend to the metering event receiver.").build();
        let metering_external_read_counter = meter.u64_counter("metering_events_sent.external_read").with_description("The number of external read metering events sent by the frontend to the metering event receiver.").build();
        let list_db_retries_counter = meter.u64_counter("list_database_retries").build();
        let create_db_retries_counter = meter.u64_counter("create_database_retries").build();
        let get_db_retries_counter = meter.u64_counter("get_database_retries").build();
        let delete_db_retries_counter = meter.u64_counter("delete_database_retries").build();
        let list_collections_retries_counter =
            meter.u64_counter("list_collections_retries").build();
        let count_collections_retries_counter =
            meter.u64_counter("count_collections_retries").build();
        let get_collection_retries_counter = meter.u64_counter("get_collection_retries").build();
        let get_collection_by_crn_retries_counter =
            meter.u64_counter("get_collection_by_crn_retries").build();
        let get_tenant_retries_counter = meter.u64_counter("get_tenant_retries").build();
        let create_collection_retries_counter =
            meter.u64_counter("create_collection_retries").build();
        let update_collection_retries_counter =
            meter.u64_counter("update_collection_retries").build();
        let delete_collection_retries_counter =
            meter.u64_counter("delete_collection_retries").build();
        let create_tenant_retries_counter = meter.u64_counter("create_tenant_retries").build();
        let update_tenant_retries_counter = meter.u64_counter("update_tenant_retries").build();
        let get_collection_with_segments_counter = meter
            .u64_counter("get_collection_with_segments_retries")
            .build();
        let metrics = Arc::new(Metrics {
            fork_retries_counter,
            delete_retries_counter,
            add_retries_counter,
            update_retries_counter,
            upsert_retries_counter,
            get_collection_with_segments_counter,
            list_db_retries_counter,
            get_db_retries_counter,
            list_collections_retries_counter,
            count_collections_retries_counter,
            get_collection_retries_counter,
            get_collection_by_crn_retries_counter,
            get_tenant_retries_counter,
            create_collection_retries_counter,
            update_collection_retries_counter,
            create_tenant_retries_counter,
            update_tenant_retries_counter,
            create_db_retries_counter,
            delete_db_retries_counter,
            delete_collection_retries_counter,
            search_retries_counter,
            metering_fork_counter,
            metering_read_counter,
            metering_write_counter,
            metering_external_read_counter,
        });
        ServiceBasedFrontend {
            allow_reset,
            executor,
            log_client,
            sysdb_client,
            collections_with_segments_provider,
            max_batch_size,
            metrics,
            default_knn_index,
            retry_policy,
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
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_create_tenant = || {
            let mut self_clone = self.clone();
            let tenant_clone = name.clone();
            async move { self_clone.sysdb_client.create_tenant(tenant_clone).await }
        };
        let res = retryable_create_tenant
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "create tenant failed with error {:?} for tenant {}. Retrying",
                    e,
                    name,
                );
            })
            .await?;
        self.metrics
            .create_tenant_retries_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);
        Ok(res)
    }

    pub async fn get_tenant(
        &mut self,
        GetTenantRequest { name, .. }: GetTenantRequest,
    ) -> Result<GetTenantResponse, GetTenantError> {
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_get_tenant = || {
            let mut self_clone = self.clone();
            let tenant_clone = name.clone();
            async move { self_clone.sysdb_client.get_tenant(tenant_clone).await }
        };
        let res = retryable_get_tenant
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "get tenant failed with error {:?} for tenant {}. Retrying",
                    e,
                    name,
                );
            })
            .await?;
        self.metrics
            .get_tenant_retries_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);
        Ok(res)
    }

    pub async fn update_tenant(
        &mut self,
        UpdateTenantRequest {
            tenant_id,
            resource_name,
            ..
        }: UpdateTenantRequest,
    ) -> Result<UpdateTenantResponse, UpdateTenantError> {
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_update_tenant = || {
            let mut self_clone = self.clone();
            let tenant_clone = tenant_id.clone();
            let resource_clone = resource_name.clone();
            async move {
                self_clone
                    .sysdb_client
                    .update_tenant(tenant_clone, resource_clone)
                    .await
            }
        };
        let res = retryable_update_tenant
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "update tenant failed with error {:?} for tenant {} for resource {}. Retrying",
                    e,
                    tenant_id,
                    resource_name
                );
            })
            .await?;
        self.metrics
            .update_tenant_retries_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);
        Ok(res)
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
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_create_db = || {
            let mut self_clone = self.clone();
            let tenant_clone = tenant_id.clone();
            let db_name_clone = database_name.clone();
            async move {
                self_clone
                    .sysdb_client
                    .create_database(database_id, db_name_clone, tenant_clone)
                    .await
            }
        };
        let res = retryable_create_db
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "create database failed with error {:?} for tenant {}, database name {}, database id {}. Retrying",
                    e,
                    tenant_id,
                    database_id,
                    database_name
                );
            })
            .await;
        self.metrics
            .create_db_retries_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);
        res
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
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_list_db = || {
            let mut self_clone = self.clone();
            let tenant_clone = tenant_id.clone();
            async move {
                self_clone
                    .sysdb_client
                    .list_databases(tenant_clone, limit, offset)
                    .await
            }
        };
        let res = retryable_list_db
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "list databases failed with error {:?} for tenant {}. Retrying",
                    e,
                    tenant_id
                );
            })
            .await;
        self.metrics
            .list_db_retries_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    pub async fn get_database(
        &mut self,
        GetDatabaseRequest {
            tenant_id,
            database_name,
            ..
        }: GetDatabaseRequest,
    ) -> Result<GetDatabaseResponse, GetDatabaseError> {
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_get_db = || {
            let mut self_clone = self.clone();
            let tenant_clone = tenant_id.clone();
            let db_name_clone = database_name.clone();
            async move {
                self_clone
                    .sysdb_client
                    .get_database(db_name_clone, tenant_clone)
                    .await
            }
        };
        let res = retryable_get_db
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "get database failed with error {:?} for tenant {} database name {}. Retrying",
                    e,
                    tenant_id,
                    database_name
                );
            })
            .await;
        self.metrics
            .get_db_retries_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    pub async fn delete_database(
        &mut self,
        DeleteDatabaseRequest {
            tenant_id,
            database_name,
            ..
        }: DeleteDatabaseRequest,
    ) -> Result<DeleteDatabaseResponse, DeleteDatabaseError> {
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_delete_db = || {
            let mut self_clone = self.clone();
            let tenant_clone = tenant_id.clone();
            let db_name_clone = database_name.clone();
            async move {
                self_clone
                    .sysdb_client
                    .delete_database(db_name_clone, tenant_clone)
                    .await
            }
        };
        let res = retryable_delete_db
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "delete database failed with error {:?} for tenant {} database name {}. Retrying",
                    e,
                    tenant_id,
                    database_name
                );
            })
            .await;
        self.metrics
            .delete_db_retries_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);
        res
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
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_list_collection = || {
            let mut self_clone = self.clone();
            let tenant_clone = tenant_id.clone();
            let db_name_clone = database_name.clone();
            async move {
                self_clone
                    .sysdb_client
                    .get_collections(GetCollectionsOptions {
                        tenant: Some(tenant_clone),
                        database: Some(db_name_clone),
                        limit,
                        offset,
                        ..Default::default()
                    })
                    .await
            }
        };
        let res = retryable_list_collection
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "list collections failed with error {:?} for tenant {} database name {}. Retrying",
                    e,
                    tenant_id,
                    database_name
                );
            })
            .await;
        self.metrics
            .list_collections_retries_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    pub async fn count_collections(
        &mut self,
        CountCollectionsRequest {
            tenant_id,
            database_name,
            ..
        }: CountCollectionsRequest,
    ) -> Result<CountCollectionsResponse, CountCollectionsError> {
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_count_collection = || {
            let mut self_clone = self.clone();
            let tenant_clone = tenant_id.clone();
            let db_name_clone = database_name.clone();
            async move {
                self_clone
                    .sysdb_client
                    .count_collections(tenant_clone, Some(db_name_clone))
                    .await
                    .map(|count| count as u32)
            }
        };
        let res = retryable_count_collection
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "count collections failed with error {:?} for tenant {} database name {}. Retrying",
                    e,
                    tenant_id,
                    database_name
                );
            })
            .await;
        self.metrics
            .count_collections_retries_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);
        res
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
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_get_collection = || {
            let mut self_clone = self.clone();
            let tenant_clone = tenant_id.clone();
            let db_name_clone = database_name.clone();
            let collection_name_clone = collection_name.clone();
            async move {
                let mut collections = self_clone
                    .sysdb_client
                    .get_collections(GetCollectionsOptions {
                        name: Some(collection_name_clone.clone()),
                        tenant: Some(tenant_clone),
                        database: Some(db_name_clone),
                        limit: Some(1),
                        ..Default::default()
                    })
                    .await
                    .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
                collections
                    .pop()
                    .ok_or(GetCollectionError::NotFound(collection_name_clone))
            }
        };
        let res = retryable_get_collection
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "get collection failed with error {:?} for tenant {} database name {}, collection name {}. Retrying",
                    e,
                    tenant_id,
                    database_name,
                    collection_name,
                );
            })
            .await;
        self.metrics
            .get_collection_retries_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    pub async fn get_collection_by_crn(
        &mut self,
        GetCollectionByCrnRequest { parsed_crn, .. }: GetCollectionByCrnRequest,
    ) -> Result<GetCollectionByCrnResponse, GetCollectionByCrnError> {
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_get_by_crn = || {
            let mut self_clone = self.clone();
            let tenant_clone = parsed_crn.tenant_resource_name.clone();
            let db_name_clone = parsed_crn.database_name.clone();
            let coll_name_clone = parsed_crn.collection_name.clone();
            async move {
                self_clone
                    .sysdb_client
                    .get_collection_by_crn(tenant_clone, db_name_clone, coll_name_clone)
                    .await
                    .map_err(|err| Box::new(err) as Box<dyn ChromaError>)
            }
        };
        let res = retryable_get_by_crn
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "get collection by crn failed with error {:?} for tenant {} database name {}, collection name {}. Retrying",
                    e,
                    parsed_crn.tenant_resource_name,
                    parsed_crn.database_name,
                    parsed_crn.collection_name,
                );
            })
            .await?;
        self.metrics
            .get_collection_by_crn_retries_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);

        Ok(res)
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

        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_create_collection = || {
            let mut self_clone = self.clone();
            let tenant_clone = tenant_id.clone();
            let db_name_clone = database_name.clone();
            let coll_name_clone = name.clone();
            let segments_clone = segments.clone();
            let config_clone = configuration.clone();
            let metadata_clone = metadata.clone();
            async move {
                self_clone
                    .sysdb_client
                    .create_collection(
                        tenant_clone,
                        db_name_clone,
                        collection_id,
                        coll_name_clone,
                        segments_clone,
                        config_clone,
                        metadata_clone,
                        None,
                        get_or_create,
                    )
                    .await
                    .map_err(|err| Box::new(err) as Box<dyn ChromaError>)
            }
        };
        let res = retryable_create_collection
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "create collection failed with error {:?} for tenant {} database name {}, collection name {}. Retrying",
                    e,
                    tenant_id,
                    database_name,
                    name,
                );
            })
            .await?;
        self.collections_with_segments_provider
            .collections_with_segments_cache
            .remove(&collection_id)
            .await;
        Ok(res)
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
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_update_collection = || {
            let mut self_clone = self.clone();
            let new_name_clone = new_name.clone();
            let new_metadata_clone = new_metadata.clone();
            let new_config_clone = new_configuration.clone();
            async move {
                self_clone
                    .sysdb_client
                    .update_collection(
                        collection_id,
                        new_name_clone,
                        new_metadata_clone,
                        None,
                        new_config_clone,
                    )
                    .await
                    .map_err(|err| Box::new(err) as Box<dyn ChromaError>)
            }
        };
        retryable_update_collection
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "Update collection failed with error {:?} for collection id {}. Retrying",
                    e,
                    collection_id,
                );
            })
            .await?;
        // Invalidate the cache.
        self.collections_with_segments_provider
            .collections_with_segments_cache
            .remove(&collection_id)
            .await;
        self.metrics
            .update_collection_retries_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);
        Ok(UpdateCollectionResponse {})
    }

    pub async fn retryable_delete_collection(
        &mut self,
        tenant_id: String,
        database_name: String,
        collection_name: String,
    ) -> Result<CollectionUuid, DeleteCollectionError> {
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

        Ok(collection.collection_id)
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
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_delete_collection = || {
            let mut self_clone = self.clone();
            let tenant_clone = tenant_id.clone();
            let db_name_clone = database_name.clone();
            let coll_name_clone = collection_name.clone();
            async move {
                self_clone
                    .retryable_delete_collection(tenant_clone, db_name_clone, coll_name_clone)
                    .await
            }
        };
        let id = retryable_delete_collection
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "delete collection failed with error {:?} for collection name {}, database name {}. Retrying",
                    e,
                    collection_name,
                    database_name,
                );
            })
            .await?;
        // Invalidate the cache.
        self.collections_with_segments_provider
            .collections_with_segments_cache
            .remove(&id)
            .await;
        self.metrics
            .delete_collection_retries_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);
        Ok(DeleteCollectionRecordsResponse {})
    }

    pub async fn retryable_fork(
        &mut self,
        ForkCollectionRequest {
            tenant_id,
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

        // Attach metadata to the metering context
        chroma_metering::with_current(|context| {
            context.latest_collection_logical_size_bytes(latest_collection_logical_size_bytes);
        });

        // TODO: Submit event after the response is sent
        match chroma_metering::close::<CollectionForkContext>() {
            Ok(collection_fork_context) => {
                if let Ok(()) = MeterEvent::CollectionFork(collection_fork_context)
                    .submit()
                    .await
                {
                    self.metrics.metering_fork_counter.add(1, &[]);
                }
            }
            Err(e) => tracing::error!("Failed to submit metering event to receiver: {:?}", e),
        }

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
            .retry(self.retry_policy)
            // NOTE: Transport level errors will manifest as unknown errors, and they should also be retried
            .when(|e| {
                matches!(
                    e.code(),
                    ErrorCodes::FailedPrecondition | ErrorCodes::Unknown | ErrorCodes::Unavailable
                )
            })
            .notify(|_, _| {
                retries.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "Retrying fork() request for collection {}",
                    request.source_collection_id
                );
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
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        }: AddCollectionRecordsRequest,
    ) -> Result<AddCollectionRecordsResponse, AddCollectionRecordsError> {
        self.validate_embedding(
            collection_id,
            Some(&embeddings),
            true,
            |embedding: &Vec<f32>| Some(embedding.len()),
        )
        .await
        .map_err(|err| err.boxed())?;

        let embeddings = Some(embeddings.into_iter().map(Some).collect());

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
        // Any error other than a backoff is not safe to retry.
        let res = add_to_retry
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
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

        // Attach metadata to the metering context
        chroma_metering::with_current(|context| {
            context.log_size_bytes(log_size_bytes);
            context.finish_request(Instant::now());
        });

        // TODO: Submit event after the response is sent
        match res {
            Ok(()) => {
                match chroma_metering::close::<CollectionWriteContext>() {
                    Ok(collection_write_context) => {
                        if let Ok(()) = MeterEvent::CollectionWrite(collection_write_context)
                            .submit()
                            .await
                        {
                            self.metrics.metering_write_counter.add(1, &[]);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to submit metering event to receiver: {:?}", e)
                    }
                }
                Ok(AddCollectionRecordsResponse {})
            }
            Err(e) => {
                if e.code() == ErrorCodes::Unavailable {
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
        // Any error other than a backoff is not safe to retry.
        let res = add_to_retry
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
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

        // Attach metadata to the metering context
        chroma_metering::with_current(|context| {
            context.log_size_bytes(log_size_bytes);
            context.finish_request(Instant::now());
        });

        // TODO: Submit event after the response is sent
        match res {
            Ok(()) => {
                match chroma_metering::close::<CollectionWriteContext>() {
                    Ok(collection_write_context) => {
                        if let Ok(()) = MeterEvent::CollectionWrite(collection_write_context)
                            .submit()
                            .await
                        {
                            self.metrics.metering_write_counter.add(1, &[]);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to submit metering event to receiver: {:?}", e)
                    }
                }
                Ok(UpdateCollectionRecordsResponse {})
            }
            Err(e) => {
                if e.code() == ErrorCodes::Unavailable {
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
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        }: UpsertCollectionRecordsRequest,
    ) -> Result<UpsertCollectionRecordsResponse, UpsertCollectionRecordsError> {
        self.validate_embedding(
            collection_id,
            Some(&embeddings),
            true,
            |embedding: &Vec<f32>| Some(embedding.len()),
        )
        .await
        .map_err(|err| err.boxed())?;

        let embeddings = Some(embeddings.into_iter().map(Some).collect());

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
        // Any error other than a backoff is not safe to retry.
        let res = add_to_retry
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
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

        // Attach metadata to the metering context
        chroma_metering::with_current(|context| {
            context.log_size_bytes(log_size_bytes);
            context.finish_request(Instant::now());
        });

        // TODO: Submit event after the response is sent
        match res {
            Ok(()) => {
                match chroma_metering::close::<CollectionWriteContext>() {
                    Ok(collection_write_context) => {
                        if let Ok(()) = MeterEvent::CollectionWrite(collection_write_context)
                            .submit()
                            .await
                        {
                            self.metrics.metering_write_counter.add(1, &[]);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to submit metering event to receiver: {:?}", e)
                    }
                }
                Ok(UpsertCollectionRecordsResponse {})
            }
            Err(e) => {
                if e.code() == ErrorCodes::Unavailable {
                    Err(UpsertCollectionRecordsError::Backoff)
                } else {
                    Err(UpsertCollectionRecordsError::Other(Box::new(e) as _))
                }
            }
        }
    }

    pub async fn retryable_push_delete_logs(
        &mut self,
        tenant_id: String,
        database_name: String,
        collection_id: CollectionUuid,
        records: Vec<OperationRecord>,
    ) -> Result<DeleteCollectionRecordsResponse, DeleteCollectionRecordsError> {
        let collection_write_context_container =
            chroma_metering::create::<CollectionWriteContext>(CollectionWriteContext::new(
                tenant_id.clone(),
                database_name.clone(),
                collection_id.0.to_string(),
                WriteAction::Delete,
            ));

        // Closure for write context operations
        (async {
            if records.is_empty() {
                tracing::debug!("Bailing because no records were found");
                return Ok::<_, DeleteCollectionRecordsError>(DeleteCollectionRecordsResponse {});
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

            // Attach metadata to the write context
            chroma_metering::with_current(|context| {
                context.log_size_bytes(log_size_bytes);
            });

            Ok(DeleteCollectionRecordsResponse {})
        })
        .meter(collection_write_context_container.clone())
        .await?;

        // Need to re-enter the write context before attempting to close
        collection_write_context_container.enter();

        // TODO: Submit event after the response is sent
        match chroma_metering::close::<CollectionWriteContext>() {
            Ok(collection_write_context) => {
                if let Ok(()) = MeterEvent::CollectionWrite(collection_write_context)
                    .submit()
                    .await
                {
                    self.metrics.metering_write_counter.add(1, &[]);
                }
            }
            Err(e) => {
                tracing::error!("Failed to submit metering event to receiver: {:?}", e)
            }
        }

        Ok(DeleteCollectionRecordsResponse {})
    }

    pub async fn retryable_get_records_to_delete(
        &mut self,
        DeleteCollectionRecordsRequest {
            collection_id,
            ids,
            r#where,
            ..
        }: DeleteCollectionRecordsRequest,
    ) -> Result<Vec<OperationRecord>, DeleteCollectionRecordsError> {
        let mut records = Vec::new();

        let read_event = if let Some(where_clause) = r#where {
            let collection_and_segments = self
                .retryable_get_collection_with_segments(collection_id)
                .await?;
            let latest_collection_logical_size_bytes = collection_and_segments
                .collection
                .size_bytes_post_compaction;
            let fts_query_length = where_clause.fts_query_length();
            let metadata_predicate_count = where_clause.metadata_predicate_count();

            let filter = Filter {
                query_ids: ids,
                where_clause: Some(where_clause),
            };

            let get_plan = Get {
                scan: Scan {
                    collection_and_segments,
                },
                filter,
                limit: Limit {
                    offset: 0,
                    limit: None,
                },
                proj: Projection {
                    document: false,
                    embedding: false,
                    metadata: false,
                },
            };

            let get_result = self
                .executor
                .get(get_plan.clone(), |code: tonic::Code| {
                    let mut provider = self.collections_with_segments_provider.clone();
                    let mut replan_get = get_plan.clone();
                    async move {
                        if code == tonic::Code::NotFound {
                            provider
                                .collections_with_segments_cache
                                .remove(&collection_id)
                                .await;
                            let collection_and_segments = provider
                                .get_collection_with_segments(collection_id)
                                .await
                                .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
                            replan_get.scan = Scan {
                                collection_and_segments,
                            };
                        }
                        Ok(replan_get)
                    }
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

            // Attach metadata to the read context
            chroma_metering::with_current(|context| {
                context.fts_query_length(fts_query_length);
                context.metadata_predicate_count(metadata_predicate_count);
                context.query_embedding_count(0);
                context.pulled_log_size_bytes(get_result.pulled_log_bytes);
                context.latest_collection_logical_size_bytes(latest_collection_logical_size_bytes);
                context.return_bytes(return_bytes);
            });

            match chroma_metering::close::<CollectionReadContext>() {
                Ok(collection_read_context) => {
                    Some(MeterEvent::CollectionRead(collection_read_context))
                }
                Err(e) => {
                    tracing::error!("Failed to submit metering event to receiver: {:?}", e);
                    None
                }
            }
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

        if let Some(event) = read_event {
            event.submit().await;
        }

        Ok(records)
    }

    pub async fn delete(
        &mut self,
        request: DeleteCollectionRecordsRequest,
    ) -> Result<DeleteCollectionRecordsResponse, DeleteCollectionRecordsError> {
        let records_to_delete = self
            .retryable_get_records_to_delete(request.clone())
            .await?;

        let retries = Arc::new(AtomicUsize::new(0));
        let retryable_push_delete_logs = || {
            let mut self_clone = self.clone();
            let tenant = request.tenant_id.clone();
            let database_name = request.database_name.clone();
            let records_to_delete = records_to_delete.clone();
            async move {
                self_clone
                    .retryable_push_delete_logs(
                        tenant,
                        database_name,
                        request.collection_id,
                        records_to_delete,
                    )
                    .await
            }
        };
        let res = retryable_push_delete_logs
            .retry(self.retry_policy)
            // NOTE: It's not safe to retry adds to the log except when it is unavailable.
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|_, _| {
                let retried = retries.fetch_add(1, Ordering::Relaxed);
                if retried > 0 {
                    tracing::info!(
                        "Retrying push delete logs request for collection {}",
                        request.collection_id
                    );
                }
            })
            .await?;

        self.metrics
            .delete_retries_counter
            .add(retries.load(Ordering::Relaxed) as u64, &[]);
        Ok(res)
    }

    pub async fn retryable_count(
        &mut self,
        CountRequest { collection_id, .. }: CountRequest,
    ) -> Result<CountResponse, QueryError> {
        let collection_and_segments = self
            .retryable_get_collection_with_segments(collection_id)
            .await?;
        let latest_collection_logical_size_bytes = collection_and_segments
            .collection
            .size_bytes_post_compaction;
        let count_plan = Count {
            scan: Scan {
                collection_and_segments,
            },
        };
        let count_result = self
            .executor
            .count(count_plan.clone(), |code: tonic::Code| {
                let mut provider = self.collections_with_segments_provider.clone();
                let mut count_replanned = count_plan.clone();
                async move {
                    if code == tonic::Code::NotFound {
                        provider
                            .collections_with_segments_cache
                            .remove(&collection_id)
                            .await;
                        let collection_and_segments = provider
                            .get_collection_with_segments(collection_id)
                            .await
                            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
                        count_replanned.scan = Scan {
                            collection_and_segments,
                        };
                    }
                    Ok(count_replanned)
                }
            })
            .await?;
        let return_bytes = count_result.size_bytes();

        // Attach metadata to the metering context
        chroma_metering::with_current(|context| {
            context.fts_query_length(0);
            context.metadata_predicate_count(0);
            context.query_embedding_count(0);
            context.pulled_log_size_bytes(count_result.pulled_log_bytes);
            context.latest_collection_logical_size_bytes(latest_collection_logical_size_bytes);
            context.return_bytes(return_bytes);
        });

        // TODO: Submit event after the response is sent
        match chroma_metering::close::<CollectionReadContext>() {
            Ok(collection_read_context) => {
                if let Ok(()) = MeterEvent::CollectionRead(collection_read_context)
                    .submit()
                    .await
                {
                    self.metrics.metering_read_counter.add(1, &[]);
                }
            }
            Err(_) => match chroma_metering::close::<ExternalCollectionReadContext>() {
                Ok(external_collection_read_context) => {
                    if let Ok(()) =
                        MeterEvent::ExternalCollectionRead(external_collection_read_context)
                            .submit()
                            .await
                    {
                        self.metrics.metering_external_read_counter.add(1, &[]);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to submit metering event to receiver: {:?}", e)
                }
            },
        }

        Ok(count_result.count)
    }

    pub async fn count(&mut self, request: CountRequest) -> Result<CountResponse, QueryError> {
        self.retryable_count(request).await
    }

    fn is_retryable(code: tonic::Code) -> bool {
        code == tonic::Code::Unavailable || code == tonic::Code::Unknown
    }

    async fn retryable_get_collection_with_segments(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<CollectionAndSegments, Box<dyn ChromaError>> {
        let retry_count = Arc::new(AtomicUsize::new(0));
        let retryable_get_collection = || {
            let mut self_clone = self.clone();
            async move {
                self_clone
                    .collections_with_segments_provider
                    .get_collection_with_segments(collection_id)
                    .await
                    .map_err(|err| Box::new(err) as Box<dyn ChromaError>)
            }
        };

        let res = retryable_get_collection
            .retry(self.retry_policy)
            .when(|e| Self::is_retryable(e.code().into()))
            .notify(|e, _| {
                retry_count.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    "get collection with segments failed with error {:?} for collection {}. Retrying",
                    e,
                    collection_id
                );
            })
            .await;
        self.metrics
            .get_collection_with_segments_counter
            .add(retry_count.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    async fn retryable_get(
        &mut self,
        GetRequest {
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
            .retryable_get_collection_with_segments(collection_id)
            .await?;
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
        let get_plan = Get {
            scan: Scan {
                collection_and_segments,
            },
            filter: Filter {
                query_ids: ids,
                where_clause: r#where,
            },
            limit: Limit { offset, limit },
            proj: Projection {
                document: include.0.contains(&Include::Document),
                embedding: include.0.contains(&Include::Embedding),
                // If URI is requested, metadata is also requested so we can extract the URI.
                metadata: (include.0.contains(&Include::Metadata)
                    || include.0.contains(&Include::Uri)),
            },
        };
        let get_result = self
            .executor
            .get(get_plan.clone(), |code: tonic::Code| {
                let mut provider = self.collections_with_segments_provider.clone();
                let mut get_replanned = get_plan.clone();
                async move {
                    if code == tonic::Code::NotFound {
                        provider
                            .collections_with_segments_cache
                            .remove(&collection_id)
                            .await;
                        let collection_and_segments = provider
                            .get_collection_with_segments(collection_id)
                            .await
                            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
                        get_replanned.scan = Scan {
                            collection_and_segments,
                        };
                    }
                    Ok(get_replanned)
                }
            })
            .await?;
        let return_bytes = get_result.size_bytes();

        // Attach metadata to the metering context
        chroma_metering::with_current(|context| {
            context.fts_query_length(fts_query_length);
            context.metadata_predicate_count(metadata_predicate_count);
            context.query_embedding_count(0);
            context.pulled_log_size_bytes(get_result.pulled_log_bytes);
            context.latest_collection_logical_size_bytes(latest_collection_logical_size_bytes);
            context.return_bytes(return_bytes);
            context.finish_request(Instant::now());
        });

        // TODO: Submit event after the response is sent
        match chroma_metering::close::<CollectionReadContext>() {
            Ok(collection_read_context) => {
                if let Ok(()) = MeterEvent::CollectionRead(collection_read_context)
                    .submit()
                    .await
                {
                    self.metrics.metering_read_counter.add(1, &[]);
                }
            }
            Err(_) => match chroma_metering::close::<ExternalCollectionReadContext>() {
                Ok(external_collection_read_context) => {
                    if let Ok(()) =
                        MeterEvent::ExternalCollectionRead(external_collection_read_context)
                            .submit()
                            .await
                    {
                        self.metrics.metering_external_read_counter.add(1, &[]);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to submit metering event to receiver: {:?}", e)
                }
            },
        }

        Ok((get_result, include).into())
    }

    pub async fn get(&mut self, request: GetRequest) -> Result<GetResponse, QueryError> {
        self.retryable_get(request).await
    }

    async fn retryable_query(
        &mut self,
        QueryRequest {
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
            .retryable_get_collection_with_segments(collection_id)
            .await?;
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
        let knn_plan = Knn {
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
        };
        let query_result = self
            .executor
            .knn(knn_plan.clone(), |code: tonic::Code| {
                let mut provider = self.collections_with_segments_provider.clone();
                let mut knn_replanned = knn_plan.clone();
                async move {
                    if code == tonic::Code::NotFound {
                        provider
                            .collections_with_segments_cache
                            .remove(&collection_id)
                            .await;
                        let collection_and_segments = provider
                            .get_collection_with_segments(collection_id)
                            .await
                            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
                        knn_replanned.scan = Scan {
                            collection_and_segments,
                        };
                    }
                    Ok(knn_replanned)
                }
            })
            .await?;
        let return_bytes = query_result.size_bytes();

        // Attach metadata to the metering context
        chroma_metering::with_current(|context| {
            context.fts_query_length(fts_query_length);
            context.metadata_predicate_count(metadata_predicate_count);
            context.query_embedding_count(query_embedding_count);
            context.pulled_log_size_bytes(query_result.pulled_log_bytes);
            context.latest_collection_logical_size_bytes(latest_collection_logical_size_bytes);
            context.return_bytes(return_bytes);
            context.finish_request(Instant::now());
        });

        // TODO: Submit event after the response is sent
        match chroma_metering::close::<CollectionReadContext>() {
            Ok(collection_read_context) => {
                if let Ok(()) = MeterEvent::CollectionRead(collection_read_context)
                    .submit()
                    .await
                {
                    self.metrics.metering_read_counter.add(1, &[]);
                }
            }
            Err(_) => match chroma_metering::close::<ExternalCollectionReadContext>() {
                Ok(external_collection_read_context) => {
                    if let Ok(()) =
                        MeterEvent::ExternalCollectionRead(external_collection_read_context)
                            .submit()
                            .await
                    {
                        self.metrics.metering_external_read_counter.add(1, &[]);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to submit metering event to receiver: {:?}", e)
                }
            },
        }

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
        self.retryable_query(request).await
    }

    pub async fn retryable_search(
        &mut self,
        request: SearchRequest,
    ) -> Result<SearchResponse, QueryError> {
        // TODO: The dispatch logic is mostly the same for count/get/query/search, we should consider unifying them
        // Get collection and segments once for all queries
        let collection_and_segments = self
            .collections_with_segments_provider
            .get_collection_with_segments(request.collection_id)
            .await
            .map_err(|err| QueryError::Other(Box::new(err) as Box<dyn ChromaError>))?;

        let latest_collection_logical_size_bytes = collection_and_segments
            .collection
            .size_bytes_post_compaction;

        // Aggregate metrics across all search payloads
        let mut total_metadata_predicate_count = 0u64;
        let mut total_fts_query_length = 0u64;
        let mut total_search_embedding_count = 0u64;

        for payload in &request.searches {
            // Count metadata predicates and FTS query length from where clause
            if let Some(ref where_clause) = payload.filter.where_clause {
                total_metadata_predicate_count += where_clause.metadata_predicate_count();
                total_fts_query_length += where_clause.fts_query_length();
            }

            // Count embeddings from the score expression
            // Each rank in the score expression contains one embedding
            total_search_embedding_count += payload.rank.knn_queries().len() as u64;
        }

        // Create a single Search plan with one scan and the payloads from the request
        // Clone the searches to use them later for aggregating select keys
        let searches_for_select = request.searches.clone();
        let search_plan = Search {
            scan: Scan {
                collection_and_segments,
            },
            payloads: request.searches,
        };

        // Execute the single search plan using the executor
        let result = self.executor.search(search_plan).await?;

        // Calculate return bytes (approximate size of the response)
        let return_bytes = result.size_bytes();

        // Attach metadata to the metering context
        chroma_metering::with_current(|context| {
            context.fts_query_length(total_fts_query_length);
            context.metadata_predicate_count(total_metadata_predicate_count);
            context.query_embedding_count(total_search_embedding_count);
            context.pulled_log_size_bytes(result.pulled_log_bytes);
            context.latest_collection_logical_size_bytes(latest_collection_logical_size_bytes);
            context.return_bytes(return_bytes);
            context.finish_request(Instant::now());
        });

        // TODO: Submit metering event after the response is sent
        match chroma_metering::close::<CollectionReadContext>() {
            Ok(collection_read_context) => {
                if let Ok(()) = MeterEvent::CollectionRead(collection_read_context)
                    .submit()
                    .await
                {
                    self.metrics.metering_read_counter.add(1, &[]);
                }
            }
            Err(_) => match chroma_metering::close::<ExternalCollectionReadContext>() {
                Ok(external_collection_read_context) => {
                    if let Ok(()) =
                        MeterEvent::ExternalCollectionRead(external_collection_read_context)
                            .submit()
                            .await
                    {
                        self.metrics.metering_external_read_counter.add(1, &[]);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to submit metering event to receiver: {:?}", e)
                }
            },
        }

        Ok((result, searches_for_select).into())
    }

    pub async fn search(&mut self, request: SearchRequest) -> Result<SearchResponse, QueryError> {
        // TODO: The retry logic is mostly the same for count/get/query/search, we should consider unifying them
        let retries = Arc::new(AtomicUsize::new(0));
        let search_to_retry = || {
            let mut self_clone = self.clone();
            let request_clone = request.clone();
            let cache_clone = self
                .collections_with_segments_provider
                .collections_with_segments_cache
                .clone();
            async move {
                let res = self_clone.retryable_search(request_clone).await;
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
        let res = search_to_retry
            .retry(self.collections_with_segments_provider.get_retry_backoff())
            // NOTE: Transport level errors will manifest as unknown errors, and they should also be retried
            .when(|e| matches!(e.code(), ErrorCodes::NotFound | ErrorCodes::Unknown))
            .notify(|_, _| {
                let retried = retries.fetch_add(1, Ordering::Relaxed);
                if retried > 0 {
                    tracing::info!(
                        "Retrying search() request for collection {}",
                        request.collection_id
                    );
                }
            })
            .await;
        self.metrics
            .search_retries_counter
            .add(retries.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    pub async fn healthcheck(&self) -> HealthCheckResponse {
        HealthCheckResponse {
            is_executor_ready: self.executor.is_ready().await,
            is_log_client_ready: self.log_client.is_ready(),
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
            sqlite_log
                .init_max_batch_size(max_batch_size)
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

        let retry_config = &config.retry;
        Ok(ServiceBasedFrontend::new(
            config.allow_reset,
            sysdb,
            collections_with_segments_provider,
            log,
            executor,
            max_batch_size,
            config.default_knn_index,
            retry_config.into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use chroma_config::registry::Registry;
    use chroma_sysdb::GrpcSysDbConfig;
    use chroma_types::Collection;
    use uuid::Uuid;

    use crate::{
        executor::config::{DistributedExecutorConfig, ExecutorConfig, RetryConfig},
        server::CreateCollectionPayload,
    };

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
            .post("http://localhost:8000/api/v2/tenants/default_tenant/databases/default_database/collections")
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

    #[test]
    fn test_crn_parsing() {
        use chroma_types::GetCollectionByCrnRequest;

        let result = GetCollectionByCrnRequest::try_new("tenant1:db1:coll1".to_string());
        assert!(result.is_ok());
        let request = result.unwrap();
        assert_eq!(request.parsed_crn.tenant_resource_name, "tenant1");
        assert_eq!(request.parsed_crn.database_name, "db1");
        assert_eq!(request.parsed_crn.collection_name, "coll1");

        assert!(GetCollectionByCrnRequest::try_new("tenant1:coll1".to_string()).is_err());
        assert!(GetCollectionByCrnRequest::try_new("tenant1".to_string()).is_err());
        assert!(GetCollectionByCrnRequest::try_new("tenant1:db1:coll1:extra".to_string()).is_err());
        assert!(GetCollectionByCrnRequest::try_new("".to_string()).is_err());
        assert!(GetCollectionByCrnRequest::try_new("tenant1:db1:".to_string()).is_err());
        assert!(GetCollectionByCrnRequest::try_new("tenant1:db1:coll1:".to_string()).is_err());
        assert!(GetCollectionByCrnRequest::try_new(":db1:coll1".to_string()).is_err());
        assert!(GetCollectionByCrnRequest::try_new(":db1:coll1:".to_string()).is_err());
        assert!(GetCollectionByCrnRequest::try_new(":db1::".to_string()).is_err());
    }
}
