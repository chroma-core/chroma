use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::blockfile_record::RecordSegmentReaderShard;
use chroma_storage::{PutOptions, Storage, StorageError};
use chroma_types::{AttachedFunction, Chunk, LogRecord, MaterializedLogOperation};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::execution::operators::execute_task::{AttachedFunctionExecutor, HydratedInputBatch};

const DEFAULT_PARAM_KEY: &str = "s3_path";

#[derive(Debug, Default, Deserialize, Serialize)]
struct CountState {
    count: i64,
    pulled_log_offsets: HashMap<String, i64>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StoredCountState {
    Current(CountState),
    Legacy(i64),
}

#[derive(Debug)]
pub struct CountToFileAsyncExecutor {
    path: String,
    storage: Storage,
}

#[derive(Debug, Error)]
pub enum CountToFileAsyncError {
    #[error("missing required param: {0}")]
    MissingParam(String),
    #[error("invalid params JSON: {0}")]
    InvalidParams(String),
    #[error("invalid s3 path: {0}")]
    InvalidPath(String),
    #[error("storage is required for count_to_file_async")]
    MissingStorage,
    #[error("failed to read count file: {0}")]
    Read(StorageError),
    #[error("failed to write count file: {0}")]
    Write(StorageError),
    #[error("invalid count file: {0}")]
    InvalidState(String),
    #[error("pulled log offset does not fit in i64: {0}")]
    InvalidPulledLogOffset(u64),
}

impl ChromaError for CountToFileAsyncError {
    fn code(&self) -> ErrorCodes {
        match self {
            CountToFileAsyncError::MissingParam(_)
            | CountToFileAsyncError::InvalidParams(_)
            | CountToFileAsyncError::InvalidPath(_)
            | CountToFileAsyncError::MissingStorage
            | CountToFileAsyncError::InvalidState(_)
            | CountToFileAsyncError::InvalidPulledLogOffset(_) => ErrorCodes::InvalidArgument,
            CountToFileAsyncError::Read(e) | CountToFileAsyncError::Write(e) => e.code(),
        }
    }
}

impl CountToFileAsyncExecutor {
    pub fn from_attached_function(
        af: &AttachedFunction,
        storage: Option<Storage>,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let params_json = af.params.as_deref().unwrap_or("{}");
        let params: serde_json::Value = serde_json::from_str(params_json).map_err(|e| {
            Box::new(CountToFileAsyncError::InvalidParams(e.to_string())) as Box<dyn ChromaError>
        })?;

        let path = params
            .get(DEFAULT_PARAM_KEY)
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                Box::new(CountToFileAsyncError::MissingParam(
                    DEFAULT_PARAM_KEY.to_string(),
                )) as Box<dyn ChromaError>
            })?
            .to_string();

        let storage = storage.ok_or_else(|| {
            Box::new(CountToFileAsyncError::MissingStorage) as Box<dyn ChromaError>
        })?;

        Ok(Self { path, storage })
    }

    fn parse_storage_key<'a>(
        &'a self,
        storage: &'a Storage,
    ) -> Result<&'a str, CountToFileAsyncError> {
        if let Some(without_scheme) = self.path.strip_prefix("s3://") {
            let (bucket, key) = without_scheme
                .split_once('/')
                .ok_or_else(|| CountToFileAsyncError::InvalidPath(self.path.clone()))?;
            if key.is_empty() {
                return Err(CountToFileAsyncError::InvalidPath(self.path.clone()));
            }
            if let Some(expected_bucket) = storage.bucket_name() {
                if bucket != expected_bucket {
                    return Err(CountToFileAsyncError::InvalidPath(format!(
                        "bucket mismatch: expected {expected_bucket}, got {bucket}"
                    )));
                }
            }
            return Ok(key);
        }

        if self.path.is_empty() {
            return Err(CountToFileAsyncError::InvalidPath(self.path.clone()));
        }

        Ok(&self.path)
    }

    async fn load_state(
        &self,
        storage: &Storage,
        key: &str,
    ) -> Result<CountState, CountToFileAsyncError> {
        match storage.get(key, Default::default()).await {
            Ok(bytes) => {
                let body = std::str::from_utf8(bytes.as_ref()).map_err(|_| {
                    CountToFileAsyncError::InvalidState(format!(
                        "{} was not valid utf-8",
                        self.path
                    ))
                })?;
                match serde_json::from_str::<StoredCountState>(body.trim()).map_err(|err| {
                    CountToFileAsyncError::InvalidState(format!("{}: {err}", self.path))
                })? {
                    StoredCountState::Current(state) => Ok(state),
                    StoredCountState::Legacy(count) => Ok(CountState {
                        count,
                        pulled_log_offsets: HashMap::new(),
                    }),
                }
            }
            Err(StorageError::NotFound { .. }) => Ok(CountState::default()),
            Err(err) => Err(CountToFileAsyncError::Read(err)),
        }
    }
}

#[async_trait]
impl AttachedFunctionExecutor for CountToFileAsyncExecutor {
    async fn execute(
        &self,
        input_batches: Vec<HydratedInputBatch<'_, '_>>,
        _output_reader: Option<&RecordSegmentReaderShard<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        let key = self
            .parse_storage_key(&self.storage)
            .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;

        let mut state = self
            .load_state(&self.storage, key)
            .await
            .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;

        for batch in input_batches {
            let collection_id = batch.input_collection_id.to_string();
            let stored_offset = state
                .pulled_log_offsets
                .get(&collection_id)
                .copied()
                .unwrap_or(-1);

            for (record, _) in batch.records.iter() {
                let operation_log_offset = record
                    .get_operation_log_offset()
                    .map_err(|err| Box::new(err) as Box<dyn ChromaError>)?;
                if operation_log_offset <= stored_offset {
                    continue;
                }

                match record.get_operation() {
                    MaterializedLogOperation::AddNew => state.count += 1,
                    MaterializedLogOperation::DeleteExisting => state.count -= 1,
                    _ => {}
                }
            }

            let pulled_log_offset = i64::try_from(batch.completion_offset).map_err(|_| {
                Box::new(CountToFileAsyncError::InvalidPulledLogOffset(
                    batch.completion_offset,
                )) as Box<dyn ChromaError>
            })?;
            state
                .pulled_log_offsets
                .entry(collection_id)
                .and_modify(|offset| *offset = (*offset).max(pulled_log_offset))
                .or_insert(pulled_log_offset);
        }

        let bytes = serde_json::to_vec(&state).map_err(|err| {
            Box::new(CountToFileAsyncError::InvalidState(err.to_string())) as Box<dyn ChromaError>
        })?;

        self.storage
            .put_bytes(key, bytes, PutOptions::default())
            .await
            .map_err(|err| Box::new(CountToFileAsyncError::Write(err)) as Box<dyn ChromaError>)?;

        Ok(Chunk::new(Arc::from(Vec::<LogRecord>::new())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_segment::{
        blockfile_record::RecordSegmentReaderOptions,
        types::{materialize_logs, HydratedMaterializedLogRecord, MaterializeLogsResult},
    };
    use chroma_storage::local::LocalStorage;
    use chroma_types::{CollectionUuid, Operation, OperationRecord};

    fn record(log_offset: i64, id: &str, operation: Operation) -> LogRecord {
        LogRecord {
            log_offset,
            record: OperationRecord {
                id: id.to_string(),
                embedding: Some(vec![0.0]),
                encoding: None,
                metadata: None,
                document: None,
                operation,
            },
        }
    }

    fn add_record(log_offset: i64, id: &str) -> LogRecord {
        record(log_offset, id, Operation::Add)
    }

    async fn hydrate_records<'a>(
        materialized: &'a MaterializeLogsResult,
    ) -> Vec<HydratedMaterializedLogRecord<'a, 'a>> {
        let mut records = Vec::new();
        for record in materialized.iter() {
            records.push(
                record
                    .hydrate(None)
                    .await
                    .expect("hydration should succeed"),
            );
        }
        records
    }

    async fn execute_logs(
        executor: &CountToFileAsyncExecutor,
        collection_id: CollectionUuid,
        pulled_log_offset: u64,
        logs: Vec<LogRecord>,
    ) {
        let materialized = materialize_logs(
            &None,
            Chunk::new(Arc::from(logs)),
            None,
            &RecordSegmentReaderOptions::default(),
        )
        .await
        .expect("logs should materialize");
        let records = hydrate_records(&materialized).await;

        executor
            .execute(
                vec![HydratedInputBatch {
                    input_collection_id: collection_id,
                    input_collection_name: "test-input".to_string(),
                    tenant_id: "test-tenant".to_string(),
                    database_id: "test-database".to_string(),
                    completion_offset: pulled_log_offset,
                    records: Chunk::new(Arc::from(records)),
                }],
                None,
            )
            .await
            .expect("count should succeed");
    }

    #[tokio::test]
    async fn retries_only_count_new_offsets_per_input_collection() {
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let storage = Storage::Local(LocalStorage::new(
            temp_dir.path().to_str().expect("temporary path is utf-8"),
        ));
        let executor = CountToFileAsyncExecutor {
            path: "count.json".to_string(),
            storage: storage.clone(),
        };
        let first_collection = CollectionUuid::new();
        let second_collection = CollectionUuid::new();

        execute_logs(
            &executor,
            first_collection,
            2,
            vec![add_record(1, "one"), add_record(2, "two")],
        )
        .await;
        execute_logs(
            &executor,
            first_collection,
            4,
            vec![
                add_record(1, "one"),
                add_record(2, "two"),
                record(3, "one", Operation::Update),
                add_record(4, "three"),
            ],
        )
        .await;
        execute_logs(
            &executor,
            second_collection,
            2,
            vec![add_record(1, "four"), add_record(2, "five")],
        )
        .await;
        execute_logs(
            &executor,
            second_collection,
            2,
            vec![add_record(1, "four"), add_record(2, "five")],
        )
        .await;

        let bytes = storage
            .get("count.json", Default::default())
            .await
            .expect("count state should exist");
        let state: CountState =
            serde_json::from_slice(bytes.as_ref()).expect("count state should be valid JSON");

        assert_eq!(state.count, 5);
        assert_eq!(
            state.pulled_log_offsets.get(&first_collection.to_string()),
            Some(&4)
        );
        assert_eq!(
            state.pulled_log_offsets.get(&second_collection.to_string()),
            Some(&2)
        );
    }

    #[tokio::test]
    async fn legacy_integer_count_is_upgraded_on_write() {
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let storage = Storage::Local(LocalStorage::new(
            temp_dir.path().to_str().expect("temporary path is utf-8"),
        ));
        storage
            .put_bytes("count.json", b"4".to_vec(), PutOptions::default())
            .await
            .expect("legacy count should be written");
        let executor = CountToFileAsyncExecutor {
            path: "count.json".to_string(),
            storage: storage.clone(),
        };
        let collection_id = CollectionUuid::new();

        execute_logs(&executor, collection_id, 5, vec![add_record(5, "five")]).await;

        let bytes = storage
            .get("count.json", Default::default())
            .await
            .expect("count state should exist");
        let state: CountState =
            serde_json::from_slice(bytes.as_ref()).expect("count state should be valid JSON");

        assert_eq!(state.count, 5);
        assert_eq!(
            state.pulled_log_offsets.get(&collection_id.to_string()),
            Some(&5)
        );
    }
}
