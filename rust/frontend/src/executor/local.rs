use super::config::LocalExecutorConfig;
use async_trait::async_trait;
use chroma_config::{registry::Registry, Configurable};
use chroma_distance::normalize;
use chroma_error::ChromaError;
use chroma_log::{BackfillMessage, LocalCompactionManager, PurgeLogsMessage};
use chroma_segment::{
    local_segment_manager::LocalSegmentManager, sqlite_metadata::SqliteMetadataReader,
};
use chroma_sqlite::db::SqliteDb;
use chroma_system::ComponentHandle;
use chroma_types::{
    operator::{
        CountResult, Filter, GetResult, KnnBatchResult, KnnProjectionOutput, KnnProjectionRecord,
        Limit, Projection, ProjectionRecord, RecordMeasure, SearchResult,
    },
    plan::{Count, Get, Knn, Search},
    CollectionAndSegments, CollectionUuid, ExecutorError, SegmentType, Space,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[derive(Clone, Debug)]
pub struct LocalExecutor {
    hnsw_manager: LocalSegmentManager,
    metadata_reader: SqliteMetadataReader,
    compactor_handle: ComponentHandle<LocalCompactionManager>,
    backfilled_collections: Arc<parking_lot::Mutex<HashSet<CollectionUuid>>>,
}

impl LocalExecutor {
    pub fn new(
        hnsw_manager: LocalSegmentManager,
        sqlite_db: SqliteDb,
        compactor_handle: ComponentHandle<LocalCompactionManager>,
    ) -> Self {
        Self {
            hnsw_manager,
            metadata_reader: SqliteMetadataReader::new(sqlite_db),
            compactor_handle,
            backfilled_collections: Arc::new(parking_lot::Mutex::new(HashSet::new())),
        }
    }

    pub fn get_supported_segment_types(&self) -> Vec<SegmentType> {
        vec![
            SegmentType::HnswLocalMemory,
            SegmentType::HnswLocalPersisted,
            SegmentType::Sqlite,
        ]
    }
}

impl LocalExecutor {
    pub async fn count(&mut self, plan: Count) -> Result<CountResult, ExecutorError> {
        self.try_backfill_collection(&plan.scan.collection_and_segments)
            .await?;
        self.metadata_reader
            .count(plan)
            .await
            .map_err(|err| ExecutorError::Internal(Box::new(err)))
    }

    // If collection has already been backfilled, this function does nothing.
    pub async fn try_backfill_collection(
        &mut self,
        collection_and_segment: &CollectionAndSegments,
    ) -> Result<(), ExecutorError> {
        {
            let backfill_guard = self.backfilled_collections.lock();
            if backfill_guard.contains(&collection_and_segment.collection.collection_id) {
                return Ok(());
            }
        }
        let backfill_msg = BackfillMessage {
            collection_id: collection_and_segment.collection.collection_id,
        };
        self.compactor_handle
            .request(backfill_msg, None)
            .await
            .map_err(|err| ExecutorError::BackfillError(Box::new(err)))?
            .map_err(|err| ExecutorError::BackfillError(Box::new(err)))?;
        let purge_log_msg = PurgeLogsMessage {
            collection_id: collection_and_segment.collection.collection_id,
        };
        self.compactor_handle
            .request(purge_log_msg, None)
            .await
            .map_err(|err| ExecutorError::BackfillError(Box::new(err)))?
            .map_err(|err| ExecutorError::BackfillError(Box::new(err)))?;
        let mut backfill_guard = self.backfilled_collections.lock();
        backfill_guard.insert(collection_and_segment.collection.collection_id);
        Ok(())
    }

    pub async fn get(&mut self, plan: Get) -> Result<GetResult, ExecutorError> {
        let collection_and_segments = plan.scan.collection_and_segments.clone();
        self.try_backfill_collection(&collection_and_segments)
            .await?;
        let load_embedding = plan.proj.embedding;
        let mut result = self
            .metadata_reader
            .get(plan)
            .await
            .map_err(|err| ExecutorError::Internal(Box::new(err)))?;
        if load_embedding {
            if let Some(dimensionality) = collection_and_segments.collection.dimension {
                let hnsw_reader = self
                    .hnsw_manager
                    .get_hnsw_reader(
                        &collection_and_segments.collection,
                        &collection_and_segments.vector_segment,
                        dimensionality as usize,
                    )
                    .await
                    .map_err(|err| ExecutorError::Internal(Box::new(err)))?;
                for record in &mut result.result.records {
                    record.embedding = Some(
                        hnsw_reader
                            .get_embedding_by_user_id(&record.id)
                            .await
                            .map_err(|err| ExecutorError::Internal(Box::new(err)))?,
                    );
                }
            }
        }
        Ok(result)
    }

    pub async fn knn(&mut self, plan: Knn) -> Result<KnnBatchResult, ExecutorError> {
        let collection_and_segments = plan.scan.collection_and_segments.clone();
        self.try_backfill_collection(&collection_and_segments)
            .await?;

        let empty_result = Ok(KnnBatchResult {
            pulled_log_bytes: 0,
            results: vec![Default::default(); plan.knn.embeddings.len()],
        });

        let dimensionality = match collection_and_segments.collection.dimension {
            Some(dim) => dim,
            None => return empty_result,
        };

        let allowed_user_ids = match plan.filter {
            Filter {
                query_ids: None,
                where_clause: None,
            } => Vec::new(),
            Filter {
                query_ids: Some(uids),
                where_clause: _,
            } if uids.is_empty() => return empty_result,
            Filter {
                query_ids: Some(uids),
                where_clause: None,
            } => uids,
            filter => {
                let filter_plan = Get {
                    scan: plan.scan.clone(),
                    filter: filter.clone(),
                    limit: Limit {
                        offset: 0,
                        limit: None,
                    },
                    proj: Default::default(),
                };

                let allowed_uids = self
                    .get(filter_plan)
                    .await?
                    .result
                    .records
                    .into_iter()
                    .map(|record| record.id)
                    .collect::<Vec<_>>();

                if allowed_uids.is_empty() {
                    return empty_result;
                }

                allowed_uids
            }
        };

        let hnsw_reader = self
            .hnsw_manager
            .get_hnsw_reader(
                &collection_and_segments.collection,
                &collection_and_segments.vector_segment,
                dimensionality as usize,
            )
            .await
            .map_err(|err| ExecutorError::Internal(Box::new(err)))?;

        let mut allowed_offset_ids = Vec::new();
        for user_id in allowed_user_ids {
            let offset_id = hnsw_reader
                .get_offset_id_by_user_id(&user_id)
                .await
                .map_err(|err| ExecutorError::Internal(Box::new(err)))?;
            allowed_offset_ids.push(offset_id);
        }

        let hnsw_config = collection_and_segments
            .collection
            .schema
            .as_ref()
            .map(|schema| {
                schema.get_internal_hnsw_config_with_legacy_fallback(
                    &plan.scan.collection_and_segments.vector_segment,
                )
            })
            .transpose()
            .map_err(|err| ExecutorError::Internal(Box::new(err)))?
            .flatten()
            .ok_or(ExecutorError::CollectionMissingHnswConfiguration)?;

        let distance_function = hnsw_config.space;

        let mut results = Vec::new();
        let mut returned_user_ids = Vec::new();
        for embedding in plan.knn.embeddings {
            let query_embedding = if let Space::Cosine = distance_function {
                normalize(&embedding)
            } else {
                embedding
            };
            let distances = hnsw_reader
                .query_embedding(
                    allowed_offset_ids.as_slice(),
                    query_embedding,
                    plan.knn.fetch,
                )
                .await
                .map_err(|err| ExecutorError::Internal(Box::new(err)))?;

            let mut records = Vec::new();
            for RecordMeasure { offset_id, measure } in distances {
                let user_id = hnsw_reader
                    .get_user_id_by_offset_id(offset_id)
                    .await
                    .map_err(|err| ExecutorError::Internal(Box::new(err)))?;
                returned_user_ids.push(user_id.clone());
                let knn_projection = KnnProjectionRecord {
                    record: ProjectionRecord {
                        id: user_id,
                        document: None,
                        embedding: plan.proj.projection.embedding.then_some(
                            hnsw_reader
                                .get_embedding_by_offset_id(offset_id)
                                .await
                                .map_err(|err| ExecutorError::Internal(Box::new(err)))?,
                        ),
                        metadata: None,
                    },
                    distance: plan.proj.distance.then_some(measure),
                };
                records.push(knn_projection);
            }

            results.push(KnnProjectionOutput { records });
        }

        if plan.proj.projection.document || plan.proj.projection.metadata {
            let projection_plan = Get {
                scan: plan.scan,
                filter: Filter {
                    query_ids: Some(returned_user_ids),
                    where_clause: None,
                },
                limit: Limit {
                    offset: 0,
                    limit: None,
                },
                proj: Projection {
                    document: plan.proj.projection.document,
                    embedding: false,
                    metadata: plan.proj.projection.metadata,
                },
            };

            let hydrated_records = self.get(projection_plan).await?;
            let mut user_id_to_document = HashMap::new();
            let mut user_id_to_metadata = HashMap::new();
            for ProjectionRecord {
                id,
                document,
                embedding: _,
                metadata,
            } in hydrated_records.result.records
            {
                user_id_to_document.insert(id.clone(), document);
                user_id_to_metadata.insert(id, metadata);
            }

            for result in &mut results {
                for record in &mut result.records {
                    record.record.document = user_id_to_document
                        .get(&record.record.id)
                        .cloned()
                        .flatten();
                    record.record.metadata = user_id_to_metadata
                        .get(&record.record.id)
                        .cloned()
                        .flatten();
                }
            }
        }

        Ok(KnnBatchResult {
            pulled_log_bytes: 0,
            results,
        })
    }

    pub async fn search(&mut self, _plan: Search) -> Result<SearchResult, ExecutorError> {
        Err(ExecutorError::NotImplemented(
            "Search operation is not implemented for local executor".to_string(),
        ))
    }

    pub async fn reset(&mut self) -> Result<(), Box<dyn ChromaError>> {
        self.hnsw_manager.reset().await.map_err(|err| err.boxed())?;
        Ok(())
    }
}

#[async_trait]
impl Configurable<LocalExecutorConfig> for LocalExecutor {
    async fn try_from_config(
        _config: &LocalExecutorConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let hnsw_manager = registry
            .get::<LocalSegmentManager>()
            .map_err(|err| err.boxed())?;
        let sqlite_db = registry.get::<SqliteDb>().map_err(|err| err.boxed())?;
        let compactor_handle = registry
            .get::<ComponentHandle<LocalCompactionManager>>()
            .map_err(|err| err.boxed())?;
        Ok(Self::new(hnsw_manager, sqlite_db, compactor_handle.clone()))
    }
}

#[cfg(test)]
mod tests {
    use chroma_config::registry::Registry;
    use chroma_config::Configurable;
    use chroma_system::System;
    use chroma_types::{
        AddCollectionRecordsRequest, CreateCollectionRequest, IncludeList, QueryRequest,
    };

    use crate::{Frontend, FrontendConfig};

    #[tokio::test]
    async fn test_query() {
        let registry = Registry::new();
        let system = System::new();

        let config_and_system = (FrontendConfig::sqlite_in_memory(), system);
        let mut frontend = Frontend::try_from_config(&config_and_system, &registry)
            .await
            .unwrap();

        // Add data
        let collection = frontend
            .create_collection(
                CreateCollectionRequest::try_new(
                    "default_tenant".to_string(),
                    "default_database".to_string(),
                    "test".to_string(),
                    None,
                    None,
                    None,
                    false,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        frontend
            .add(
                AddCollectionRecordsRequest::try_new(
                    "default_tenant".to_string(),
                    "default_database".to_string(),
                    collection.collection_id,
                    vec!["id1".to_string(), "id2".to_string()],
                    vec![vec![-1.0, -1.0], vec![1.0, 1.0]],
                    None,
                    None,
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        // Knn should work
        let result = Box::pin(
            frontend.query(
                QueryRequest::try_new(
                    "default_tenant".to_string(),
                    "default_database".to_string(),
                    collection.collection_id,
                    None,
                    None,
                    vec![vec![0.5, 0.5]],
                    10,
                    IncludeList::default_query(),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.ids[0], vec!["id2".to_string(), "id1".to_string()]);

        // An empty list of IDs should return no results
        let result = Box::pin(
            frontend.query(
                QueryRequest::try_new(
                    "default_tenant".to_string(),
                    "default_database".to_string(),
                    collection.collection_id,
                    Some(vec![]),
                    None,
                    vec![vec![0.5, 0.5]],
                    10,
                    IncludeList::default_query(),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.ids[0].len(), 0);
    }
}
