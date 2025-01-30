use crate::{
    config::{FrontendConfig, ScorecardRule},
    executor::Executor,
    CollectionsWithSegmentsProvider,
};
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_sysdb::{sysdb, GetCollectionsError};
use chroma_system::System;
use chroma_types::{
    operator::{Filter, KnnBatch, KnnProjection, Limit, Projection},
    plan::{Count, Get, Knn},
    AddToCollectionError, AddToCollectionRequest, AddToCollectionResponse, CollectionUuid,
    CountCollectionsRequest, CountCollectionsResponse, CountRequest, CountResponse,
    CreateCollectionError, CreateCollectionRequest, CreateCollectionResponse, CreateDatabaseError,
    CreateDatabaseRequest, CreateDatabaseResponse, CreateTenantError, CreateTenantRequest,
    CreateTenantResponse, DeleteCollectionError, DeleteCollectionRequest, DeleteDatabaseError,
    DeleteDatabaseRequest, DeleteDatabaseResponse, GetCollectionError, GetCollectionRequest,
    GetCollectionResponse, GetDatabaseError, GetDatabaseRequest, GetDatabaseResponse, GetRequest,
    GetResponse, GetTenantError, GetTenantRequest, GetTenantResponse, Include,
    ListCollectionsRequest, ListCollectionsResponse, ListDatabasesError, ListDatabasesRequest,
    ListDatabasesResponse, QueryError, QueryRequest, QueryResponse, ResetError,
    UpdateCollectionError, UpdateCollectionRequest, UpdateCollectionResponse, CHROMA_DOCUMENT_KEY,
    CHROMA_URI_KEY,
};
use chroma_types::{
    Operation, OperationRecord, ScalarEncoding, UpdateMetadata, UpdateMetadataValue,
};
use mdac::{Pattern, Rule, Scorecard, ScorecardTicket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[allow(dead_code)]
const DEFAULT_TENANT: &str = "default_tenant";
#[allow(dead_code)]
const DEFAULT_DATABASE: &str = "default_database";

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
    mut ids: Vec<String>,
    mut embeddings: Option<Vec<Vec<f32>>>,
    mut documents: Option<Vec<String>>,
    mut uris: Option<Vec<String>>,
    mut metadatas: Option<Vec<M>>,
    operation: Operation,
) -> Result<Vec<OperationRecord>, ToRecordsError> {
    let mut records: Vec<OperationRecord> = vec![];
    while let Some(id) = ids.pop() {
        let embedding = embeddings
            .as_mut()
            .map(|v| v.pop().ok_or(ToRecordsError::InconsistentLength))
            .transpose()?;
        let document = documents
            .as_mut()
            .map(|v| v.pop().ok_or(ToRecordsError::InconsistentLength))
            .transpose()?;
        let uri = uris
            .as_mut()
            .map(|v| v.pop().ok_or(ToRecordsError::InconsistentLength))
            .transpose()?;
        let metadata = metadatas
            .as_mut()
            .map(|v| v.pop().ok_or(ToRecordsError::InconsistentLength))
            .transpose()?;

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
    executor: Executor,
    log_client: Box<chroma_log::Log>,
    scorecard_enabled: Arc<AtomicBool>,
    scorecard: Arc<Scorecard<'static>>,
    sysdb_client: Box<sysdb::SysDb>,
    collections_with_segments_provider: CollectionsWithSegmentsProvider,
}

impl Frontend {
    pub fn new(
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

    #[allow(dead_code)]
    pub async fn reset(&mut self) -> Result<(), ResetError> {
        self.collections_with_segments_provider
            .collections_with_segments_cache
            .clear()
            .await
            .map_err(|_| ResetError::Cache)
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
        let collection_id = CollectionUuid::new();
        let segments = vec![
            chroma_types::Segment {
                id: chroma_types::SegmentUuid::new(),
                r#type: chroma_types::SegmentType::HnswDistributed,
                scope: chroma_types::SegmentScope::VECTOR,
                collection: collection_id,
                metadata: None,
                file_path: Default::default(),
            },
            chroma_types::Segment {
                id: chroma_types::SegmentUuid::new(),
                r#type: chroma_types::SegmentType::BlockfileMetadata,
                scope: chroma_types::SegmentScope::METADATA,
                collection: collection_id,
                metadata: None,
                file_path: Default::default(),
            },
            chroma_types::Segment {
                id: chroma_types::SegmentUuid::new(),
                r#type: chroma_types::SegmentType::BlockfileRecord,
                scope: chroma_types::SegmentScope::RECORD,
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
            .map_err(|err| CreateCollectionError::SysDB(err.to_string()))?;

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
        request: AddToCollectionRequest,
    ) -> Result<AddToCollectionResponse, AddToCollectionError> {
        let collection_id = CollectionUuid(request.collection_id);

        let chroma_types::AddToCollectionRequest {
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        } = request;

        let records = to_records(ids, embeddings, documents, uris, metadatas, Operation::Add)
            .map_err(|err| match err {
                ToRecordsError::InconsistentLength => {
                    chroma_types::AddToCollectionError::InconsistentLength
                }
            })?;
        self.log_client
            .push_logs(collection_id, records)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        Ok(chroma_types::AddToCollectionResponse {})
    }

    pub async fn update(
        &mut self,
        request: chroma_types::UpdateCollectionRecordsRequest,
    ) -> Result<
        chroma_types::UpdateCollectionRecordsResponse,
        chroma_types::UpdateCollectionRecordsError,
    > {
        let collection_id = CollectionUuid(request.collection_id);

        let chroma_types::UpdateCollectionRecordsRequest {
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        } = request;

        let records = to_records(
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            Operation::Update,
        )
        .map_err(|err| match err {
            ToRecordsError::InconsistentLength => {
                chroma_types::UpdateCollectionRecordsError::InconsistentLength
            }
        })?;

        self.log_client
            .push_logs(collection_id, records)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        Ok(chroma_types::UpdateCollectionRecordsResponse {})
    }

    pub async fn upsert(
        &mut self,
        request: chroma_types::UpsertCollectionRequest,
    ) -> Result<chroma_types::UpsertCollectionResponse, chroma_types::UpsertCollectionError> {
        let collection_id = CollectionUuid(request.collection_id);

        let chroma_types::UpsertCollectionRequest {
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        } = request;

        let records = to_records(
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            Operation::Upsert,
        )
        .map_err(|err| match err {
            ToRecordsError::InconsistentLength => {
                chroma_types::UpsertCollectionError::InconsistentLength
            }
        })?;

        self.log_client
            .push_logs(collection_id, records)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        Ok(chroma_types::UpsertCollectionResponse {})
    }

    pub async fn count(&mut self, request: CountRequest) -> Result<CountResponse, QueryError> {
        let scan = self
            .collections_with_segments_provider
            .get_collection_with_segments(request.collection_id)
            .await?;
        Ok(self.executor.count(Count { scan }).await?)
    }

    pub async fn get(&mut self, request: GetRequest) -> Result<GetResponse, QueryError> {
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

    pub async fn query(&mut self, request: QueryRequest) -> Result<QueryResponse, QueryError> {
        let scan = self
            .collections_with_segments_provider
            .get_collection_with_segments(request.collection_id)
            .await?;
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
            sysdb_client,
            collections_with_segments_provider,
            log_client,
            executor,
            config.scorecard_enabled,
            rules,
        ))
    }
}
