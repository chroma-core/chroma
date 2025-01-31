use crate::{
    config::{FrontendConfig, ScorecardRule},
    executor::Executor,
    types::errors::ValidationError,
    utils::{validate_name, validate_non_empty_filter},
    CollectionsWithSegmentsProvider,
};
use backon::Retryable;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::{sysdb, GetCollectionsError};
use chroma_system::System;
use chroma_types::{
    operator::{Filter, KnnBatch, KnnProjection, Limit, Projection},
    plan::{Count, Get, Knn},
    AddCollectionRecordsError, AddCollectionRecordsRequest, AddCollectionRecordsResponse,
    CollectionUuid, CountCollectionsRequest, CountCollectionsResponse, CountRequest, CountResponse,
    CreateCollectionError, CreateCollectionRequest, CreateCollectionResponse, CreateDatabaseError,
    CreateDatabaseRequest, CreateDatabaseResponse, CreateTenantError, CreateTenantRequest,
    CreateTenantResponse, DeleteCollectionError, DeleteCollectionRecordsError,
    DeleteCollectionRecordsRequest, DeleteCollectionRecordsResponse, DeleteCollectionRequest,
    DeleteDatabaseError, DeleteDatabaseRequest, DeleteDatabaseResponse, GetCollectionError,
    GetCollectionRequest, GetCollectionResponse, GetDatabaseError, GetDatabaseRequest,
    GetDatabaseResponse, GetRequest, GetResponse, GetTenantError, GetTenantRequest,
    GetTenantResponse, HeartbeatError, HeartbeatResponse, Include, ListCollectionsRequest,
    ListCollectionsResponse, ListDatabasesError, ListDatabasesRequest, ListDatabasesResponse,
    Operation, OperationRecord, QueryError, QueryRequest, QueryResponse, ResetError, ResetResponse,
    ScalarEncoding, Segment, SegmentScope, SegmentType, SegmentUuid, UpdateCollectionError,
    UpdateCollectionRecordsError, UpdateCollectionRecordsRequest, UpdateCollectionRecordsResponse,
    UpdateCollectionRequest, UpdateCollectionResponse, UpdateMetadata, UpdateMetadataValue,
    UpsertCollectionRecordsError, UpsertCollectionRecordsRequest, UpsertCollectionRecordsResponse,
    CHROMA_DOCUMENT_KEY, CHROMA_URI_KEY,
};
use mdac::{Pattern, Rule, Scorecard, ScorecardTicket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

struct ScorecardGuard {
    scorecard: Arc<Scorecard<'static>>,
    ticket: Option<ScorecardTicket>,
}

impl Drop for ScorecardGuard {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.scorecard.untrack(ticket);
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ScorecardRuleError {
    #[error("Invalid pattern: {0}")]
    InvalidPattern(String),
}

impl ChromaError for ScorecardRuleError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            ScorecardRuleError::InvalidPattern(_) => chroma_error::ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum ToRecordsError {
    #[error("Inconsistent number of IDs, embeddings, documents, URIs and metadatas")]
    InconsistentLength,
}

fn to_records<
    MetadataValue: Into<UpdateMetadataValue>,
    M: IntoIterator<Item = (String, MetadataValue)>,
>(
    ids: Vec<String>,
    embeddings: Option<Vec<Vec<f32>>>,
    documents: Option<Vec<String>>,
    uris: Option<Vec<String>>,
    metadatas: Option<Vec<M>>,
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
        let embedding = embeddings_iter.next();
        let document = documents_iter.next();
        let uri = uris_iter.next();
        let metadata = metadatas_iter.next();

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
#[derive(Clone, Debug)]
pub struct Frontend {
    allow_reset: bool,
    executor: Executor,
    log_client: Box<chroma_log::Log>,
    scorecard_enabled: Arc<AtomicBool>,
    scorecard: Arc<Scorecard<'static>>,
    sysdb_client: Box<sysdb::SysDb>,
    collections_with_segments_provider: CollectionsWithSegmentsProvider,
}

impl Frontend {
    pub fn new(
        allow_reset: bool,
        sysdb_client: Box<sysdb::SysDb>,
        collections_with_segments_provider: CollectionsWithSegmentsProvider,
        log_client: Box<chroma_log::Log>,
        executor: Executor,
        scorecard_enabled: bool,
        rules: Vec<Rule>,
    ) -> Self {
        let scorecard_enabled = Arc::new(AtomicBool::new(scorecard_enabled));
        // NOTE(rescrv):  Assume statically no more than 128 threads because we won't deploy on
        // hardware with that many threads anytime soon for frontends, if ever.
        // SAFETY(rescrv):  This is safe because 128 is non-zero.
        let scorecard = Arc::new(Scorecard::new(&(), rules, 128.try_into().unwrap()));

        Frontend {
            allow_reset,
            executor,
            log_client,
            scorecard_enabled,
            scorecard,
            sysdb_client,
            collections_with_segments_provider,
        }
    }

    fn scorecard_request(&self, tags: &[&str]) -> Option<ScorecardGuard> {
        if self.scorecard_enabled.load(Ordering::Relaxed) {
            self.scorecard.track(tags).map(|ticket| ScorecardGuard {
                scorecard: Arc::clone(&self.scorecard),
                ticket: Some(ticket),
            })
        } else {
            Some(ScorecardGuard {
                scorecard: Arc::clone(&self.scorecard),
                ticket: None,
            })
        }
    }

    pub async fn heartbeat(&self) -> Result<HeartbeatResponse, HeartbeatError> {
        Ok(HeartbeatResponse {
            nanosecond_heartbeat: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos(),
        })
    }

    async fn get_collection_dimension(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<Option<u32>, GetCollectionError> {
        Ok(self
            .collections_with_segments_provider
            .get_collection_with_segments(collection_id)
            .await
            .map_err(|err| GetCollectionError::SysDB(err.to_string()))?
            .collection_and_segments
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
            .map_err(|err| UpdateCollectionError::SysDB(err.to_string()))?;
        Ok(UpdateCollectionResponse {})
    }

    async fn validate_embedding(
        &mut self,
        collection_id: CollectionUuid,
        option_embeddings: Option<&Vec<Vec<f32>>>,
        update_if_not_present: bool,
    ) -> Result<(), ValidationError> {
        if let Some(embeddings) = option_embeddings {
            let emb_dims = embeddings.iter().map(|emb| emb.len());
            let min_dim = emb_dims.clone().min();
            let max_dim = emb_dims.max();
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
                Ok(Some(expected_dim)) if expected_dim != emb_dim => {
                    Err(ValidationError::DimensionMismatch(expected_dim, emb_dim))
                }
                Ok(None) if update_if_not_present => {
                    self.set_collection_dimension(collection_id, emb_dim)
                        .await?;
                    Ok(())
                }
                Ok(None) => Err(ValidationError::CollectionUninitialized),
                Ok(_) => Ok(()),
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
            .map_err(|_| ResetError::Cache)?;
        self.sysdb_client.reset().await
    }

    pub async fn create_tenant(
        &mut self,
        request: CreateTenantRequest,
    ) -> Result<CreateTenantResponse, CreateTenantError> {
        let tags = &["op:create_tenant"];
        let _guard = self
            .scorecard_request(tags)
            .ok_or(CreateTenantError::RateLimited)?;
        self.sysdb_client.create_tenant(request.name).await
    }

    pub async fn get_tenant(
        &mut self,
        request: GetTenantRequest,
    ) -> Result<GetTenantResponse, GetTenantError> {
        let tags = &["op:get_tenant"];
        let _guard = self
            .scorecard_request(tags)
            .ok_or(GetTenantError::RateLimited)?;
        self.sysdb_client.get_tenant(request.name).await
    }

    pub async fn create_database(
        &mut self,
        request: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse, CreateDatabaseError> {
        let tags = &[
            "op:create_database",
            &format!("tenant_id:{}", request.tenant_id),
            &format!("database_id:{}", request.database_id),
        ];
        let _guard = self
            .scorecard_request(tags)
            .ok_or(CreateDatabaseError::RateLimited)?;
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
        let tags = &[
            "op:list_database",
            &format!("tenant_id:{}", request.tenant_id),
        ];
        let _guard = self
            .scorecard_request(tags)
            .ok_or(ListDatabasesError::RateLimited)?;
        self.sysdb_client
            .list_databases(request.tenant_id, request.limit, request.offset)
            .await
    }

    pub async fn get_database(
        &mut self,
        request: GetDatabaseRequest,
    ) -> Result<GetDatabaseResponse, GetDatabaseError> {
        let tags = &[
            "op:get_database",
            &format!("tenant_id:{}", request.tenant_id),
        ];
        let _guard = self
            .scorecard_request(tags)
            .ok_or(GetDatabaseError::RateLimited)?;
        self.sysdb_client
            .get_database(request.database_name, request.tenant_id)
            .await
    }

    pub async fn delete_database(
        &mut self,
        request: DeleteDatabaseRequest,
    ) -> Result<DeleteDatabaseResponse, DeleteDatabaseError> {
        let tags = &[
            "op:delete_database",
            &format!("tenant_id:{}", request.tenant_id),
        ];
        let _guard = self
            .scorecard_request(tags)
            .ok_or(DeleteDatabaseError::RateLimited)?;
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
            )
            .await
    }

    pub async fn count_collections(
        &mut self,
        request: CountCollectionsRequest,
    ) -> Result<CountCollectionsResponse, GetCollectionsError> {
        self.sysdb_client
            .get_collections(
                None,
                None,
                Some(request.tenant_id),
                Some(request.database_name),
            )
            .await
            .map(|collections| collections.len() as u32)
    }

    pub async fn get_collection(
        &mut self,
        request: GetCollectionRequest,
    ) -> Result<GetCollectionResponse, GetCollectionError> {
        let mut collections = self
            .sysdb_client
            .get_collections(
                None,
                Some(request.collection_name),
                Some(request.tenant_id),
                Some(request.database_name),
            )
            .await
            .map_err(|err| GetCollectionError::SysDB(err.to_string()))?;
        collections.pop().ok_or(GetCollectionError::NotFound)
    }

    pub async fn create_collection(
        &mut self,
        request: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse, CreateCollectionError> {
        validate_name(&request.name)
            .map_err(|err| CreateCollectionError::Validation(err.to_string()))?;

        let collection_id = CollectionUuid::new();
        let segments = vec![
            Segment {
                id: SegmentUuid::new(),
                r#type: SegmentType::HnswDistributed,
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
        ];

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
            .map_err(|err| match err {
                sysdb::CreateCollectionError::CollectionNameExists => {
                    CreateCollectionError::CollectionNameExists
                }
                _ => CreateCollectionError::SysDB(err.to_string()),
            })?;

        Ok(collection)
    }

    pub async fn update_collection(
        &mut self,
        request: UpdateCollectionRequest,
    ) -> Result<UpdateCollectionResponse, UpdateCollectionError> {
        if let Some(name) = request.new_name.as_ref() {
            validate_name(name)
                .map_err(|err| UpdateCollectionError::Validation(err.to_string()))?;
        }
        self.sysdb_client
            .update_collection(
                request.collection_id,
                request.new_name,
                request.new_metadata,
                None,
            )
            .await
            .map_err(|err| UpdateCollectionError::SysDB(err.to_string()))?;

        Ok(UpdateCollectionResponse {})
    }

    pub async fn delete_collection(
        &mut self,
        request: DeleteCollectionRequest,
    ) -> Result<(), DeleteCollectionError> {
        let collection = self
            .sysdb_client
            .get_collections(
                None,
                Some(request.collection_name),
                Some(request.tenant_id.clone()),
                Some(request.database_name.clone()),
            )
            .await
            .map_err(|err| DeleteCollectionError::SysDB(err.to_string()))?
            .into_iter()
            .next()
            .ok_or(DeleteCollectionError::NotFound)?;

        let segments = self
            .sysdb_client
            .get_segments(None, None, None, collection.collection_id)
            .await
            .map_err(|err| DeleteCollectionError::SysDB(err.to_string()))?;

        self.sysdb_client
            .delete_collection(
                request.tenant_id,
                request.database_name,
                collection.collection_id,
                segments.into_iter().map(|s| s.id).collect(),
            )
            .await
            .map_err(|err| DeleteCollectionError::SysDB(err.to_string()))?;

        Ok(())
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

        self.validate_embedding(collection_id, embeddings.as_ref(), true)
            .await
            .map_err(|err| AddCollectionRecordsError::Validation(err.to_string()))?;

        let records = to_records(ids, embeddings, documents, uris, metadatas, Operation::Add)
            .map_err(|err| match err {
                ToRecordsError::InconsistentLength => AddCollectionRecordsError::InconsistentLength,
            })?;

        self.log_client
            .push_logs(request.collection_id, records)
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

        self.validate_embedding(collection_id, embeddings.as_ref(), true)
            .await
            .map_err(|err| UpdateCollectionRecordsError::Validation(err.to_string()))?;

        let records = to_records(
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            Operation::Update,
        )
        .map_err(|err| match err {
            ToRecordsError::InconsistentLength => UpdateCollectionRecordsError::InconsistentLength,
        })?;

        self.log_client
            .push_logs(request.collection_id, records)
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

        self.validate_embedding(collection_id, embeddings.as_ref(), true)
            .await
            .map_err(|err| UpsertCollectionRecordsError::Validation(err.to_string()))?;

        let records = to_records(
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            Operation::Upsert,
        )
        .map_err(|err| match err {
            ToRecordsError::InconsistentLength => UpsertCollectionRecordsError::InconsistentLength,
        })?;

        self.log_client
            .push_logs(request.collection_id, records)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        Ok(UpsertCollectionRecordsResponse {})
    }

    pub async fn delete(
        &mut self,
        request: DeleteCollectionRecordsRequest,
    ) -> Result<DeleteCollectionRecordsResponse, DeleteCollectionRecordsError> {
        let scan = self
            .collections_with_segments_provider
            .get_collection_with_segments(request.collection_id)
            .await?;

        let filter = Filter {
            query_ids: request.ids,
            where_clause: request.r#where,
        };

        validate_non_empty_filter(&filter)
            .map_err(|err| DeleteCollectionRecordsError::Validation(err.to_string()))?;

        let get_result = self
            .executor
            .get(Get {
                scan,
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

        if get_result.records.is_empty() {
            tracing::debug!("Bailing because no records were found");
            return Ok(DeleteCollectionRecordsResponse {});
        }

        let records = get_result
            .records
            .into_iter()
            .map(|record| OperationRecord {
                id: record.id,
                operation: Operation::Delete,
                document: None,
                embedding: None,
                encoding: None,
                metadata: None,
            })
            .collect::<Vec<_>>();

        self.log_client
            .push_logs(request.collection_id, records)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        Ok(DeleteCollectionRecordsResponse {})
    }

    pub async fn count_to_retry(
        &mut self,
        request: CountRequest,
    ) -> Result<CountResponse, QueryError> {
        tracing::info!(
            "Retrying count() request for collection {}",
            request.collection_id
        );
        let scan = self
            .collections_with_segments_provider
            .get_collection_with_segments(request.collection_id)
            .await?;
        Ok(self.executor.count(Count { scan }).await?)
    }

    pub async fn count(&mut self, request: CountRequest) -> Result<CountResponse, QueryError> {
        let count_to_retry = || {
            let mut self_clone = self.clone();
            let request_clone = request.clone();
            let cache_clone = self
                .collections_with_segments_provider
                .collections_with_segments_cache
                .clone();
            async move {
                let res = self_clone.count_to_retry(request_clone).await;
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
        count_to_retry
            .retry(self.collections_with_segments_provider.get_retry_backoff())
            .when(|e| e.code() == ErrorCodes::NotFound)
            .await
    }

    async fn get_to_retry(&mut self, request: GetRequest) -> Result<GetResponse, QueryError> {
        tracing::info!(
            "Retrying get() request for collection {}",
            request.collection_id
        );
        let scan = self
            .collections_with_segments_provider
            .get_collection_with_segments(request.collection_id)
            .await?;
        let get_result = self
            .executor
            .get(Get {
                scan,
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
                    metadata: request.include.0.contains(&Include::Metadata),
                },
            })
            .await?;
        Ok((get_result, request.include).into())
    }

    pub async fn get(&mut self, request: GetRequest) -> Result<GetResponse, QueryError> {
        let get_to_retry = || {
            let mut self_clone = self.clone();
            let request_clone = request.clone();
            let cache_clone = self
                .collections_with_segments_provider
                .collections_with_segments_cache
                .clone();
            async move {
                let res = self_clone.get_to_retry(request_clone).await;
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
        get_to_retry
            .retry(self.collections_with_segments_provider.get_retry_backoff())
            .when(|e| e.code() == ErrorCodes::NotFound)
            .await
    }

    async fn query_to_retry(&mut self, request: QueryRequest) -> Result<QueryResponse, QueryError> {
        tracing::info!(
            "Retrying query() request for collection {}",
            request.collection_id
        );
        let scan = self
            .collections_with_segments_provider
            .get_collection_with_segments(request.collection_id)
            .await?;

        self.validate_embedding(request.collection_id, Some(&request.embeddings), false)
            .await
            .map_err(|err| QueryError::Validation(err.to_string()))?;

        let query_result = self
            .executor
            .knn(Knn {
                scan,
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
                        metadata: request.include.0.contains(&Include::Metadata),
                    },
                    distance: request.include.0.contains(&Include::Distance),
                },
            })
            .await?;
        Ok((query_result, request.include).into())
    }

    pub async fn query(&mut self, request: QueryRequest) -> Result<QueryResponse, QueryError> {
        let query_to_retry = || {
            let mut self_clone = self.clone();
            let request_clone = request.clone();
            let cache_clone = self
                .collections_with_segments_provider
                .collections_with_segments_cache
                .clone();
            async move {
                let res = self_clone.query_to_retry(request_clone).await;
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
        query_to_retry
            .retry(self.collections_with_segments_provider.get_retry_backoff())
            .when(|e| e.code() == ErrorCodes::NotFound)
            .await
    }
}

#[async_trait::async_trait]
impl Configurable<(FrontendConfig, System)> for Frontend {
    async fn try_from_config(
        (config, system): &(FrontendConfig, System),
    ) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb_client = chroma_sysdb::from_config(&config.sysdb).await?;
        let log_client = chroma_log::from_config(&config.log).await?;

        let collections_with_segments_provider =
            CollectionsWithSegmentsProvider::try_from_config(&(
                config.collections_with_segments_provider.clone(),
                sysdb_client.clone(),
            ))
            .await?;

        let executor =
            Executor::try_from_config(&(config.executor.clone(), system.clone())).await?;
        fn rule_to_rule(rule: &ScorecardRule) -> Result<Rule, ScorecardRuleError> {
            let patterns = rule
                .patterns
                .iter()
                .map(|p| {
                    Pattern::new(p).ok_or_else(|| ScorecardRuleError::InvalidPattern(p.clone()))
                })
                .collect::<Result<Vec<_>, ScorecardRuleError>>()?;
            Ok(Rule {
                patterns,
                limit: rule.score as usize,
            })
        }
        let rules = config
            .scorecard
            .iter()
            .map(rule_to_rule)
            .collect::<Result<Vec<_>, ScorecardRuleError>>()
            .map_err(|x| Box::new(x) as _)?;
        Ok(Frontend::new(
            config.allow_reset,
            sysdb_client,
            collections_with_segments_provider,
            log_client,
            executor,
            config.scorecard_enabled,
            rules,
        ))
    }
}
