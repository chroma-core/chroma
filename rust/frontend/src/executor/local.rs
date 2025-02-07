use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chroma_distance::{normalize, DistanceFunction};
use chroma_log::{BackfillMessage, LocalCompactionManager};
use chroma_segment::{
    local_segment_manager::LocalSegmentManager, sqlite_metadata::SqliteMetadataReader,
    utils::distance_function_from_segment,
};
use chroma_sqlite::db::SqliteDb;
use chroma_system::ComponentHandle;
use chroma_types::{
    operator::{
        CountResult, Filter, GetResult, KnnBatchResult, KnnProjectionOutput, KnnProjectionRecord,
        Projection, ProjectionRecord, RecordDistance,
    },
    plan::{Count, Get, Knn},
    CollectionAndSegments, CollectionUuid, ExecutorError,
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
            collection_and_segment: collection_and_segment.clone(),
        };
        self.compactor_handle
            .request(backfill_msg, None)
            .await
            .map_err(|_| ExecutorError::BackfillError)?
            .map_err(|_| ExecutorError::BackfillError)?;
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
                        &collection_and_segments.vector_segment,
                        dimensionality as usize,
                    )
                    .await
                    .map_err(|err| ExecutorError::Internal(Box::new(err)))?;
                for record in &mut result.records {
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
        if let Some(dimensionality) = collection_and_segments.collection.dimension {
            let allowed_user_ids = if plan.filter.where_clause.is_none() {
                plan.filter.query_ids.unwrap_or_default()
            } else {
                let filter_plan = Get {
                    scan: plan.scan.clone(),
                    filter: plan.filter.clone(),
                    limit: Default::default(),
                    proj: Default::default(),
                };

                let allowed_uids = self
                    .get(filter_plan)
                    .await?
                    .records
                    .into_iter()
                    .map(|record| record.id)
                    .collect::<Vec<_>>();

                if allowed_uids.is_empty() {
                    return Ok(vec![Default::default(); plan.knn.embeddings.len()]);
                }

                allowed_uids
            };

            let hnsw_reader = self
                .hnsw_manager
                .get_hnsw_reader(
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

            let distance_function =
                distance_function_from_segment(&plan.scan.collection_and_segments.vector_segment)
                    .map_err(|err| ExecutorError::Internal(err))?;
            let mut knn_batch_results = Vec::new();
            let mut returned_user_ids = Vec::new();
            for embedding in plan.knn.embeddings {
                let query_embedding = if let DistanceFunction::Cosine = distance_function {
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
                for RecordDistance { offset_id, measure } in distances {
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

                knn_batch_results.push(KnnProjectionOutput { records });
            }

            if plan.proj.projection.document || plan.proj.projection.metadata {
                let projection_plan = Get {
                    scan: plan.scan,
                    filter: Filter {
                        query_ids: Some(returned_user_ids),
                        where_clause: None,
                    },
                    limit: Default::default(),
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
                } in hydrated_records.records
                {
                    user_id_to_document.insert(id.clone(), document);
                    user_id_to_metadata.insert(id, metadata);
                }

                for result in &mut knn_batch_results {
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

            Ok(knn_batch_results)
        } else {
            // Collection is unintialized
            Ok(vec![Default::default(); plan.knn.embeddings.len()])
        }
    }
}
