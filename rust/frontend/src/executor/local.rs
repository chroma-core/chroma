use chroma_segment::{
    local_segment_manager::LocalSegmentManager, sqlite_metadata::SqliteMetadataReader,
};
use chroma_sqlite::db::SqliteDb;
use chroma_types::{
    operator::{CountResult, GetResult, KnnBatchResult},
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
        let collection_and_segments = plan.scan.collection_and_segments.clone();
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

    pub async fn knn(&mut self, _plan: Knn) -> Result<KnnBatchResult, ExecutorError> {
        todo!()
    }
}
