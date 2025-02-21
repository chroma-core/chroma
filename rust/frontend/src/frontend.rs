use crate::{
    compaction_client::compaction_client::CompactionClient, config::FrontendConfig,
    executor::Executor, types::errors::ValidationError, CollectionsWithSegmentsProvider,
};
use backon::Retryable;
use chroma_config::{registry, Configurable};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::{LocalCompactionManager, LocalCompactionManagerConfig, Log};
use chroma_segment::local_segment_manager::LocalSegmentManager;
use chroma_sqlite::db::SqliteDb;
use chroma_sysdb::SysDb;
use chroma_system::System;
use chroma_types::{
    operator::{Filter, KnnBatch, KnnProjection, Limit, Projection, Scan},
    plan::{Count, Get, Knn},
    AddCollectionRecordsError, AddCollectionRecordsRequest, AddCollectionRecordsResponse,
    CollectionUuid, CountCollectionsError, CountCollectionsRequest, CountCollectionsResponse,
    CountRequest, CountResponse, CreateCollectionError, CreateCollectionRequest,
    CreateCollectionResponse, CreateDatabaseError, CreateDatabaseRequest, CreateDatabaseResponse,
    CreateTenantError, CreateTenantRequest, CreateTenantResponse, DeleteCollectionError,
    DeleteCollectionRecordsError, DeleteCollectionRecordsRequest, DeleteCollectionRecordsResponse,
    DeleteCollectionRequest, DeleteDatabaseError, DeleteDatabaseRequest, DeleteDatabaseResponse,
    DistributedHnswParameters, GetCollectionError, GetCollectionRequest, GetCollectionResponse,
    GetCollectionsError, GetDatabaseError, GetDatabaseRequest, GetDatabaseResponse, GetRequest,
    GetResponse, GetTenantError, GetTenantRequest, GetTenantResponse, HealthCheckResponse,
    HeartbeatError, HeartbeatResponse, Include, ListCollectionsRequest, ListCollectionsResponse,
    ListDatabasesError, ListDatabasesRequest, ListDatabasesResponse, ManualCompactionError,
    ManualCompactionRequest, ManualCompactionResponse, Metadata, Operation, OperationRecord,
    QueryError, QueryRequest, QueryResponse, ResetError, ResetResponse, ScalarEncoding, Segment,
    SegmentScope, SegmentType, SegmentUuid, SingleNodeHnswParameters, UpdateCollectionError,
    UpdateCollectionRecordsError, UpdateCollectionRecordsRequest, UpdateCollectionRecordsResponse,
    UpdateCollectionRequest, UpdateCollectionResponse, UpdateMetadata, UpdateMetadataValue,
    UpsertCollectionRecordsError, UpsertCollectionRecordsRequest, UpsertCollectionRecordsResponse,
    CHROMA_DOCUMENT_KEY, CHROMA_URI_KEY,
};
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(thiserror::Error, Debug)]
enum ToRecordsError {
    #[error("Inconsistent number of IDs, embeddings, documents, URIs and metadatas")]
    InconsistentLength,
}

impl ChromaError for ToRecordsError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::InvalidArgument
    }
}

fn to_records<
    MetadataValue: Into<UpdateMetadataValue>,
    M: IntoIterator<Item = (String, MetadataValue)>,
>(
    ids: Vec<String>,
    embeddings: Option<Vec<Option<Vec<f32>>>>,
    documents: Option<Vec<Option<String>>>,
    uris: Option<Vec<Option<String>>>,
    metadatas: Option<Vec<Option<M>>>,
    operation: Operation,
) -> Result<Vec<OperationRecord>, ToRecordsError> {
    let len = ids.len();

    // Check that every present vector has the same length as `ids`.
    if embeddings.as_ref().is_some_and(|v| v.len() != len)
        || documents.as_ref().is_some_and(|v| v.len() != len)
        || uris.as_ref().is_some_and(|v| v.len() != len)
        || metadatas.as_ref().is_some_and(|v| v.len() != len)
    {
        return Err(ToRecordsError::InconsistentLength);
    }

    let mut embeddings_iter = embeddings.into_iter().flat_map(|v| v.into_iter());
    let mut documents_iter = documents.into_iter().flat_map(|v| v.into_iter());
    let mut uris_iter = uris.into_iter().flat_map(|v| v.into_iter());
    let mut metadatas_iter = metadatas.into_iter().flat_map(|v| v.into_iter());

    let mut records = Vec::with_capacity(len);

    for id in ids {
        let embedding = embeddings_iter.next().flatten();
        let document = documents_iter.next().flatten();
        let uri = uris_iter.next().flatten();
        let metadata = metadatas_iter.next().flatten();

        let encoding = embedding.as_ref().map(|_| ScalarEncoding::FLOAT32);

        let mut metadata = metadata
            .map(|m| {
                m.into_iter()
                    .map(|(key, value)| (key, value.into()))
                    .collect::<UpdateMetadata>()
            })
            .unwrap_or_default();
        if let Some(document) = document.clone() {
            metadata.insert(
                CHROMA_DOCUMENT_KEY.to_string(),
                UpdateMetadataValue::Str(document),
            );
        }
        if let Some(uri) = uri {
            metadata.insert(CHROMA_URI_KEY.to_string(), UpdateMetadataValue::Str(uri));
        }

        records.push(OperationRecord {
            id,
            embedding,
            document,
            encoding,
            metadata: Some(metadata),
            operation,
        });
    }

    Ok(records)
}

#[derive(Debug)]
struct Metrics {
    delete_retries_counter: Counter<u64>,
    count_retries_counter: Counter<u64>,
    query_retries_counter: Counter<u64>,
    get_retries_counter: Counter<u64>,
}

#[derive(Clone, Debug)]
pub struct Frontend {
    allow_reset: bool,
    executor: Executor,
    log_client: Log,
    sysdb_client: SysDb,
    compaction_client: CompactionClient,
    collections_with_segments_provider: CollectionsWithSegmentsProvider,
    max_batch_size: u32,
    metrics: Arc<Metrics>,
}

impl Frontend {
    pub fn new(
        allow_reset: bool,
        sysdb_client: SysDb,
        compaction_client: CompactionClient,
        collections_with_segments_provider: CollectionsWithSegmentsProvider,
        log_client: Log,
        executor: Executor,
        max_batch_size: u32,
    ) -> Self {
        let meter = global::meter("chroma");
        let delete_retries_counter = meter.u64_counter("delete_retries").build();
        let count_retries_counter = meter.u64_counter("count_retries").build();
        let query_retries_counter = meter.u64_counter("query_retries").build();
        let get_retries_counter = meter.u64_counter("query_retries").build();
        let metrics = Arc::new(Metrics {
            delete_retries_counter,
            count_retries_counter,
            query_retries_counter,
            get_retries_counter,
        });
        Frontend {
            allow_reset,
            executor,
            log_client,
            sysdb_client,
            compaction_client,
            collections_with_segments_provider,
            max_batch_size,
            metrics,
        }
    }

    pub async fn heartbeat(&self) -> Result<HeartbeatResponse, HeartbeatError> {
        Ok(HeartbeatResponse {
            nanosecond_heartbeat: SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        })
    }

    pub fn get_max_batch_size(&mut self) -> u32 {
        self.max_batch_size
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
            .update_collection(collection_id, None, None, Some(dimension))
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
        self.sysdb_client.reset().await
    }

    pub async fn manually_compact(
        &mut self,
        request: ManualCompactionRequest,
    ) -> Result<ManualCompactionResponse, ManualCompactionError> {
        self.compaction_client.manually_compact(request).await
    }

    pub async fn create_tenant(
        &mut self,
        request: CreateTenantRequest,
    ) -> Result<CreateTenantResponse, CreateTenantError> {
        self.sysdb_client.create_tenant(request.name).await
    }

    pub async fn get_tenant(
        &mut self,
        request: GetTenantRequest,
    ) -> Result<GetTenantResponse, GetTenantError> {
        self.sysdb_client.get_tenant(request.name).await
    }

    pub async fn create_database(
        &mut self,
        request: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse, CreateDatabaseError> {
        self.sysdb_client
            .create_database(
                request.database_id,
                request.database_name,
                request.tenant_id,
            )
            .await
    }

    pub async fn list_databases(
        &mut self,
        request: ListDatabasesRequest,
    ) -> Result<ListDatabasesResponse, ListDatabasesError> {
        self.sysdb_client
            .list_databases(request.tenant_id, request.limit, request.offset)
            .await
    }

    pub async fn get_database(
        &mut self,
        request: GetDatabaseRequest,
    ) -> Result<GetDatabaseResponse, GetDatabaseError> {
        self.sysdb_client
            .get_database(request.database_name, request.tenant_id)
            .await
    }

    pub async fn delete_database(
        &mut self,
        request: DeleteDatabaseRequest,
    ) -> Result<DeleteDatabaseResponse, DeleteDatabaseError> {
        self.sysdb_client
            .delete_database(request.database_name, request.tenant_id)
            .await
    }

    pub async fn list_collections(
        &mut self,
        request: ListCollectionsRequest,
    ) -> Result<ListCollectionsResponse, GetCollectionsError> {
        self.sysdb_client
            .get_collections(
                None,
                None,
                Some(request.tenant_id),
                Some(request.database_name),
                request.limit,
                request.offset,
            )
            .await
    }

    pub async fn count_collections(
        &mut self,
        request: CountCollectionsRequest,
    ) -> Result<CountCollectionsResponse, CountCollectionsError> {
        self.sysdb_client
            .count_collections(request.tenant_id, Some(request.database_name))
            .await
            .map(|count| count as u32)
    }

    pub async fn get_collection(
        &mut self,
        request: GetCollectionRequest,
    ) -> Result<GetCollectionResponse, GetCollectionError> {
        let mut collections = self
            .sysdb_client
            .get_collections(
                None,
                Some(request.collection_name.clone()),
                Some(request.tenant_id),
                Some(request.database_name),
                None,
                0,
            )
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        collections
            .pop()
            .ok_or(GetCollectionError::NotFound(request.collection_name))
    }

    pub async fn create_collection(
        &mut self,
        request: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse, CreateCollectionError> {
        let collection_id = CollectionUuid::new();
        let segments = match self.executor {
            Executor::Distributed(_) => {
                let hnsw_metadata =
                    Metadata::try_from(DistributedHnswParameters::try_from(&request.metadata)?)?;

                vec![
                    Segment {
                        id: SegmentUuid::new(),
                        r#type: SegmentType::HnswDistributed,
                        scope: SegmentScope::VECTOR,
                        collection: collection_id,
                        metadata: Some(hnsw_metadata),
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
                let hnsw_metadata =
                    Metadata::try_from(SingleNodeHnswParameters::try_from(&request.metadata)?)?;

                vec![
                    Segment {
                        id: SegmentUuid::new(),
                        r#type: SegmentType::HnswLocalPersisted,
                        scope: SegmentScope::VECTOR,
                        collection: collection_id,
                        metadata: Some(hnsw_metadata),
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
                request.tenant_id,
                request.database_name,
                collection_id,
                request.name,
                segments,
                request.metadata,
                None,
                request.get_or_create,
            )
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        self.collections_with_segments_provider
            .collections_with_segments_cache
            .remove(&collection_id)
            .await;

        Ok(collection)
    }

    pub async fn update_collection(
        &mut self,
        request: UpdateCollectionRequest,
    ) -> Result<UpdateCollectionResponse, UpdateCollectionError> {
        self.sysdb_client
            .update_collection(
                request.collection_id,
                request.new_name,
                request.new_metadata,
                None,
            )
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        // Invalidate the cache.
        self.collections_with_segments_provider
            .collections_with_segments_cache
            .remove(&request.collection_id)
            .await;

        Ok(UpdateCollectionResponse {})
    }

    pub async fn delete_collection(
        &mut self,
        request: DeleteCollectionRequest,
    ) -> Result<DeleteCollectionRecordsResponse, DeleteCollectionError> {
        let collection = self
            .get_collection(
                GetCollectionRequest::try_new(
                    request.tenant_id.clone(),
                    request.database_name.clone(),
                    request.collection_name,
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
                request.tenant_id,
                request.database_name,
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

    pub async fn add(
        &mut self,
        request: AddCollectionRecordsRequest,
    ) -> Result<AddCollectionRecordsResponse, AddCollectionRecordsError> {
        let AddCollectionRecordsRequest {
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        } = request;

        self.validate_embedding(collection_id, embeddings.as_ref(), true, |embedding| {
            Some(embedding.len())
        })
        .await
        .map_err(|err| err.boxed())?;

        let embeddings = embeddings.map(|embeddings| embeddings.into_iter().map(Some).collect());

        let records = to_records(ids, embeddings, documents, uris, metadatas, Operation::Add)
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        self.log_client
            .push_logs(collection_id, records)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        Ok(AddCollectionRecordsResponse {})
    }

    pub async fn update(
        &mut self,
        request: UpdateCollectionRecordsRequest,
    ) -> Result<UpdateCollectionRecordsResponse, UpdateCollectionRecordsError> {
        let UpdateCollectionRecordsRequest {
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        }: UpdateCollectionRecordsRequest = request;

        self.validate_embedding(collection_id, embeddings.as_ref(), true, |embedding| {
            embedding.as_ref().map(|emb| emb.len())
        })
        .await
        .map_err(|err| err.boxed())?;

        let records = to_records(
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            Operation::Update,
        )
        .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        self.log_client
            .push_logs(collection_id, records)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        Ok(UpdateCollectionRecordsResponse {})
    }

    pub async fn upsert(
        &mut self,
        request: UpsertCollectionRecordsRequest,
    ) -> Result<UpsertCollectionRecordsResponse, UpsertCollectionRecordsError> {
        let UpsertCollectionRecordsRequest {
            collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        } = request;

        self.validate_embedding(collection_id, embeddings.as_ref(), true, |embedding| {
            Some(embedding.len())
        })
        .await
        .map_err(|err| err.boxed())?;

        let embeddings = embeddings.map(|embeddings| embeddings.into_iter().map(Some).collect());

        let records = to_records(
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            Operation::Upsert,
        )
        .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        self.log_client
            .push_logs(collection_id, records)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        Ok(UpsertCollectionRecordsResponse {})
    }

    pub async fn retryable_delete(
        &mut self,
        request: DeleteCollectionRecordsRequest,
    ) -> Result<DeleteCollectionRecordsResponse, DeleteCollectionRecordsError> {
        let mut records = Vec::new();

        if let Some(ids) = request.ids {
            records.extend(ids.into_iter().map(|id| OperationRecord {
                id,
                operation: Operation::Delete,
                document: None,
                embedding: None,
                encoding: None,
                metadata: None,
            }));
        }

        if let Some(where_clause) = request.r#where {
            let scan = self
                .collections_with_segments_provider
                .get_collection_with_segments(request.collection_id)
                .await
                .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

            let filter = Filter {
                query_ids: None,
                where_clause: Some(where_clause),
            };

            let get_result = self
                .executor
                .get(Get {
                    scan: Scan {
                        collection_and_segments: scan,
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

            for record in get_result.records {
                records.push(OperationRecord {
                    id: record.id,
                    operation: Operation::Delete,
                    document: None,
                    embedding: None,
                    encoding: None,
                    metadata: None,
                });
            }
        }

        if records.is_empty() {
            tracing::debug!("Bailing because no records were found");
            return Ok(DeleteCollectionRecordsResponse {});
        }

        self.log_client
            .push_logs(request.collection_id, records)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

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
            .when(|e| e.code() == ErrorCodes::NotFound)
            .notify(|_, _| {
                retries.fetch_add(1, Ordering::Relaxed);
            })
            .await;
        self.metrics
            .delete_retries_counter
            .add(retries.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    pub async fn retryable_count(
        &mut self,
        request: CountRequest,
    ) -> Result<CountResponse, QueryError> {
        tracing::info!(
            "Retrying count() request for collection {}",
            request.collection_id
        );
        let collection_and_segments = self
            .collections_with_segments_provider
            .get_collection_with_segments(request.collection_id)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        Ok(self
            .executor
            .count(Count {
                scan: Scan {
                    collection_and_segments,
                },
            })
            .await?)
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
            .when(|e| e.code() == ErrorCodes::NotFound)
            .notify(|_, _| {
                retries.fetch_add(1, Ordering::Relaxed);
            })
            .await;
        self.metrics
            .count_retries_counter
            .add(retries.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    async fn retryable_get(&mut self, request: GetRequest) -> Result<GetResponse, QueryError> {
        tracing::info!(
            "Retrying get() request for collection {}",
            request.collection_id
        );
        let collection_and_segments = self
            .collections_with_segments_provider
            .get_collection_with_segments(request.collection_id)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
        let get_result = self
            .executor
            .get(Get {
                scan: Scan {
                    collection_and_segments,
                },
                filter: Filter {
                    query_ids: request.ids,
                    where_clause: request.r#where,
                },
                limit: Limit {
                    skip: request.offset,
                    fetch: request.limit,
                },
                proj: Projection {
                    document: request.include.0.contains(&Include::Document),
                    embedding: request.include.0.contains(&Include::Embedding),
                    // If URI is requested, metadata is also requested so we can extract the URI.
                    metadata: (request.include.0.contains(&Include::Metadata)
                        || request.include.0.contains(&Include::Uri)),
                },
            })
            .await?;
        Ok((get_result, request.include).into())
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
            .when(|e| e.code() == ErrorCodes::NotFound)
            .notify(|_, _| {
                retries.fetch_add(1, Ordering::Relaxed);
            })
            .await;
        self.metrics
            .get_retries_counter
            .add(retries.load(Ordering::Relaxed) as u64, &[]);
        res
    }

    async fn retryable_query(
        &mut self,
        request: QueryRequest,
    ) -> Result<QueryResponse, QueryError> {
        tracing::info!(
            "Retrying query() request for collection {}",
            request.collection_id
        );
        let collection_and_segments = self
            .collections_with_segments_provider
            .get_collection_with_segments(request.collection_id)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        let query_result = self
            .executor
            .knn(Knn {
                scan: Scan {
                    collection_and_segments,
                },
                filter: Filter {
                    query_ids: request.ids,
                    where_clause: request.r#where,
                },
                knn: KnnBatch {
                    embeddings: request.embeddings,
                    fetch: request.n_results,
                },
                proj: KnnProjection {
                    projection: Projection {
                        document: request.include.0.contains(&Include::Document),
                        embedding: request.include.0.contains(&Include::Embedding),
                        // If URI is requested, metadata is also requested so we can extract the URI.
                        metadata: (request.include.0.contains(&Include::Metadata)
                            || request.include.0.contains(&Include::Uri)),
                    },
                    distance: request.include.0.contains(&Include::Distance),
                },
            })
            .await?;
        Ok((query_result, request.include).into())
    }

    pub async fn query(&mut self, request: QueryRequest) -> Result<QueryResponse, QueryError> {
        self.validate_embedding(
            request.collection_id,
            Some(&request.embeddings),
            true,
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
            .when(|e| e.code() == ErrorCodes::NotFound)
            .notify(|_, _| {
                retries.fetch_add(1, Ordering::Relaxed);
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
impl Configurable<(FrontendConfig, System)> for Frontend {
    async fn try_from_config(
        (config, system): &(FrontendConfig, System),
        registry: &registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        // Create sqlitedb if configured
        if let Some(sqlite_conf) = &config.sqlitedb {
            SqliteDb::try_from_config(sqlite_conf, registry).await?;
        };

        // Create segment manager if configured
        if let Some(segment_manager_conf) = &config.segment_manager {
            LocalSegmentManager::try_from_config(segment_manager_conf, registry).await?;
        };

        let sysdb = SysDb::try_from_config(&config.sysdb, registry).await?;
        let mut log = Log::try_from_config(&config.log, registry).await?;
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

        let compaction_client = CompactionClient::try_from_config(
            &(config.compaction_client.clone(), system.clone()),
            registry,
        )
        .await?;

        Ok(Frontend::new(
            config.allow_reset,
            sysdb,
            compaction_client,
            collections_with_segments_provider,
            log,
            executor,
            max_batch_size,
        ))
    }
}
