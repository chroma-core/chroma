use chroma_segment::sqlite_metadata::SqliteMetadataReader;
use chroma_sqlite::db::SqliteDb;
use chroma_types::{
    operator::{CountResult, GetResult, KnnBatchResult},
    plan::{Count, Get, Knn},
    ExecutorError,
};

#[derive(Clone, Debug)]
pub struct LocalExecutor {
    metadata_reader: SqliteMetadataReader,
}

impl LocalExecutor {
    pub fn new(sqlite_db: SqliteDb) -> Self {
        Self {
            metadata_reader: SqliteMetadataReader::new(sqlite_db),
        }
    }
}

impl LocalExecutor {
    pub async fn count(&mut self, plan: Count) -> Result<CountResult, ExecutorError> {
        self.metadata_reader
            .count(plan)
            .await
            .map_err(|err| ExecutorError::Sqlite(Box::new(err)))
    }

    pub async fn get(&mut self, plan: Get) -> Result<GetResult, ExecutorError> {
        let result = self
            .metadata_reader
            .get(plan)
            .await
            .map_err(|err| ExecutorError::Sqlite(Box::new(err)))?;
        // TODO: Fetch embeddings if required
        Ok(result)
    }

    pub async fn knn(&mut self, _plan: Knn) -> Result<KnnBatchResult, ExecutorError> {
        todo!()
    }
}
