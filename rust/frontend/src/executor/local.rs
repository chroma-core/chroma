use std::collections::HashMap;

use chroma_segment::{
    local_segment_manager::LocalSegmentManager, sqlite_metadata::SqliteMetadataReader,
};
use chroma_sqlite::db::SqliteDb;
use chroma_types::{
    operator::{
        CountResult, Filter, GetResult, KnnBatchResult, KnnProjectionOutput, KnnProjectionRecord,
        Projection, ProjectionRecord, RecordDistance,
    },
    plan::{Count, Get, Knn},
    ExecutorError,
};

#[derive(Clone, Debug)]
pub struct LocalExecutor {
    hnsw_manager: LocalSegmentManager,
    metadata_reader: SqliteMetadataReader,
}

impl LocalExecutor {
    pub fn new(hnsw_manager: LocalSegmentManager, sqlite_db: SqliteDb) -> Self {
        Self {
            hnsw_manager,
            metadata_reader: SqliteMetadataReader::new(sqlite_db),
        }
    }
}

impl LocalExecutor {
    pub async fn count(&mut self, plan: Count) -> Result<CountResult, ExecutorError> {
        self.metadata_reader
            .count(plan)
            .await
            .map_err(|err| ExecutorError::Internal(Box::new(err)))
    }

    pub async fn get(&mut self, plan: Get) -> Result<GetResult, ExecutorError> {
        let mut collection_and_segments = plan.scan.collection_and_segments.clone();
        collection_and_segments.collection.dimension = Some(384);
        let load_embedding = plan.proj.embedding;
        println!("load_embedding: {load_embedding}");
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
        // println!("knn plan : {:?}", plan);
        let mut collection_and_segments = plan.scan.collection_and_segments.clone();
        collection_and_segments.collection.dimension = Some(384);
        if let Some(dimensionality) = collection_and_segments.collection.dimension {
            let filter_plan = Get {
                scan: plan.scan.clone(),
                filter: plan.filter.clone(),
                limit: Default::default(),
                proj: Default::default(),
            };

            let allowed_user_ids = self
                .get(filter_plan)
                .await?
                .records
                .into_iter()
                .map(|record| record.id)
                .collect::<Vec<_>>();

            if allowed_user_ids.is_empty() {
                return Ok(Vec::new());
            }

            let hnsw_reader = self
                .hnsw_manager
                .get_hnsw_reader(
                    &collection_and_segments.vector_segment,
                    dimensionality as usize,
                )
                .await
                .map_err(|err| ExecutorError::Internal(Box::new(err)))?;

            let mut allowed_offset_ids = Vec::new();
            let mut oid_to_uid = HashMap::new();
            let mut uid_to_oid = HashMap::new();
            for user_id in allowed_user_ids {
                let offset_id = hnsw_reader
                    .get_offset_id_by_user_id(&user_id)
                    .await
                    .map_err(|err| ExecutorError::Internal(Box::new(err)))?;
                allowed_offset_ids.push(offset_id);
                oid_to_uid.insert(offset_id, user_id.clone());
                uid_to_oid.insert(user_id, offset_id);
            }

            let mut knn_batch_results = Vec::new();
            let mut returned_user_ids = Vec::new();
            for embedding in plan.knn.embeddings {
                let distances = hnsw_reader
                    .query_embedding(allowed_offset_ids.as_slice(), embedding, plan.knn.fetch)
                    .await
                    .map_err(|err| ExecutorError::Internal(Box::new(err)))?;

                let mut records = Vec::new();
                for RecordDistance { offset_id, measure } in distances {
                    let user_id = oid_to_uid
                        .get(&offset_id)
                        .cloned()
                        .ok_or(ExecutorError::InconsistentData)?;
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
            Ok(Vec::new())
        }
    }
}
