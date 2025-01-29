use crate::{config::FrontendConfig, executor::Executor};
use chroma_cache::Cache;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_sysdb::sysdb;
use chroma_system::System;
use chroma_types::{
    operator::{Filter, KnnBatch, KnnProjection, Limit, Projection, Scan},
    plan::{Count, Get, Knn},
    CollectionAndSegments, CollectionUuid, CountRequest, CountResponse, CreateDatabaseError,
    CreateDatabaseRequest, CreateDatabaseResponse, GetCollectionError, GetCollectionRequest,
    GetCollectionResponse, GetDatabaseError, GetDatabaseRequest, GetDatabaseResponse, GetRequest,
    GetResponse, Include, QueryError, QueryRequest, QueryResponse,
};
use chroma_types::{
    Operation, OperationRecord, ScalarEncoding, UpdateMetadata, UpdateMetadataValue,
};
use mdac::{Scorecard, ScorecardTicket};
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

#[derive(Clone, Debug)]
pub struct Frontend {
    #[allow(dead_code)]
    executor: Executor,
    sysdb_client: Box<sysdb::SysDb>,
    log_client: Box<chroma_log::Log>,
    scorecard_enabled: Arc<AtomicBool>,
    scorecard: Arc<Scorecard<'static>>,
    collections_with_segments_cache: Arc<dyn Cache<CollectionUuid, CollectionAndSegments>>,
}

impl Frontend {
    pub fn new(
        sysdb_client: Box<sysdb::SysDb>,
        collections_with_segments_cache: Arc<dyn Cache<CollectionUuid, CollectionAndSegments>>,
        log_client: Box<chroma_log::Log>,
        executor: Executor,
    ) -> Self {
        let scorecard_enabled = Arc::new(AtomicBool::new(false));
        // NOTE(rescrv):  Assume statically no more than 128 threads because we won't deploy on
        // hardware with that many threads anytime soon for frontends, if ever.
        // SAFETY(rescrv):  This is safe because 128 is non-zero.
        let scorecard = Arc::new(Scorecard::new(&(), vec![], 128.try_into().unwrap()));
        Frontend {
            executor,
            sysdb_client,
            log_client,
            scorecard_enabled,
            scorecard,
            collections_with_segments_cache,
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
        let res = self
            .sysdb_client
            .create_database(
                request.database_id,
                request.database_name,
                request.tenant_id,
            )
            .await;
        match res {
            Ok(()) => Ok(CreateDatabaseResponse {}),
            Err(e) => Err(e),
        }
    }

    pub async fn get_database(
        &mut self,
        request: GetDatabaseRequest,
    ) -> Result<GetDatabaseResponse, GetDatabaseError> {
        self.sysdb_client
            .get_database(request.database_name, request.tenant_id)
            .await
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

    pub async fn add(
        &mut self,
        request: chroma_types::AddToCollectionRequest,
    ) -> Result<chroma_types::AddToCollectionResponse, chroma_types::AddToCollectionError> {
        let collection_id = CollectionUuid(request.collection_id);

        let chroma_types::AddToCollectionRequest {
            mut ids,
            mut embeddings,
            mut documents,
            mut uri,
            mut metadatas,
            ..
        } = request;

        let mut records: Vec<OperationRecord> = vec![];
        while let Some(id) = ids.pop() {
            let embedding = embeddings
                .as_mut()
                .map(|v| {
                    v.pop()
                        .ok_or(chroma_types::AddToCollectionError::InconsistentLength)
                })
                .transpose()?;
            let document = documents
                .as_mut()
                .map(|v| {
                    v.pop()
                        .ok_or(chroma_types::AddToCollectionError::InconsistentLength)
                })
                .transpose()?;
            let uri = uri
                .as_mut()
                .map(|v| {
                    v.pop()
                        .ok_or(chroma_types::AddToCollectionError::InconsistentLength)
                })
                .transpose()?;
            let metadata = metadatas
                .as_mut()
                .map(|v| {
                    v.pop()
                        .ok_or(chroma_types::AddToCollectionError::InconsistentLength)
                })
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
                    "chroma:document".to_string(),
                    UpdateMetadataValue::Str(document),
                );
            }
            if let Some(uri) = uri {
                metadata.insert("chroma:uri".to_string(), UpdateMetadataValue::Str(uri));
            }

            records.push(OperationRecord {
                id,
                embedding,
                document,
                encoding,
                metadata: Some(metadata),
                operation: Operation::Add,
            });
        }

        self.log_client
            .push_logs(collection_id, records)
            .await
            .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;

        Ok(chroma_types::AddToCollectionResponse {})
    }

    pub async fn count(&mut self, request: CountRequest) -> Result<CountResponse, QueryError> {
        let scan = self
            .fetch_collection_snapshot(request.collection_id)
            .await?;
        Ok(self.executor.count(Count { scan }).await?)
    }

    pub async fn get(&mut self, request: GetRequest) -> Result<GetResponse, QueryError> {
        let scan = self
            .fetch_collection_snapshot(request.collection_id)
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
                    document: request.include.includes.contains(&Include::Document),
                    embedding: request.include.includes.contains(&Include::Embedding),
                    metadata: request.include.includes.contains(&Include::Metadata),
                },
            })
            .await?;
        Ok((get_result, request.include).into())
    }

    pub async fn query(&mut self, request: QueryRequest) -> Result<QueryResponse, QueryError> {
        let scan = self
            .fetch_collection_snapshot(request.collection_id)
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
                        document: request.include.includes.contains(&Include::Document),
                        embedding: request.include.includes.contains(&Include::Embedding),
                        metadata: request.include.includes.contains(&Include::Metadata),
                    },
                    distance: request.include.includes.contains(&Include::Distance),
                },
            })
            .await?;
        Ok((query_result, request.include).into())
    }

    async fn fetch_collection_snapshot(
        &mut self,
        collection_id: CollectionUuid,
    ) -> Result<Scan, QueryError> {
        let collection_and_segments = match self
            .collections_with_segments_cache
            .get(&collection_id)
            .await
            .map_err(|_| QueryError::CollectionSegments)?
        {
            Some(collection_and_segments) => collection_and_segments,
            None => {
                let collection_and_segments_sysdb = self
                    .sysdb_client
                    .get_collection_with_segments(collection_id)
                    .await
                    .map_err(|_| QueryError::CollectionSegments)?;
                // NOTE: We use a double check pattern here so that if another thread concurrently
                // inserts into the cache by the time we reach here, we keep the one that was inserted.
                // This ensures that all threads get the same reference for the cache.
                match self
                    .collections_with_segments_cache
                    .get(&collection_id)
                    .await
                    .map_err(|_| QueryError::CollectionSegments)?
                {
                    Some(collection_and_segments) => collection_and_segments,
                    None => {
                        self.collections_with_segments_cache
                            .insert(collection_id, collection_and_segments_sysdb.clone())
                            .await;
                        collection_and_segments_sysdb
                    }
                }
            }
        };
        Ok(Scan {
            collection_and_segments,
        })
    }
}

#[async_trait::async_trait]
impl Configurable<(FrontendConfig, System)> for Frontend {
    async fn try_from_config(
        (config, system): &(FrontendConfig, System),
    ) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb_client = chroma_sysdb::from_config(&config.sysdb).await?;
        let log_client = chroma_log::from_config(&config.log).await?;

        let collections_with_segments_cache = chroma_cache::from_config::<
            CollectionUuid,
            CollectionAndSegments,
        >(&config.cache_config)
        .await?;

        let executor =
            Executor::try_from_config(&(config.executor.clone(), system.clone())).await?;
        Ok(Frontend::new(
            sysdb_client,
            collections_with_segments_cache.into(),
            log_client,
            executor,
        ))
    }
}
