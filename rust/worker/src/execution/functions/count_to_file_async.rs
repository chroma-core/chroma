use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::blockfile_record::RecordSegmentReaderShard;
use chroma_segment::types::HydratedMaterializedLogRecord;
use chroma_storage::{PutOptions, Storage, StorageError};
use chroma_types::{AttachedFunction, Chunk, LogRecord, MaterializedLogOperation};
use thiserror::Error;

use crate::execution::operators::execute_task::AttachedFunctionExecutor;

const DEFAULT_PARAM_KEY: &str = "s3_path";

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
}

impl ChromaError for CountToFileAsyncError {
    fn code(&self) -> ErrorCodes {
        match self {
            CountToFileAsyncError::MissingParam(_)
            | CountToFileAsyncError::InvalidParams(_)
            | CountToFileAsyncError::InvalidPath(_)
            | CountToFileAsyncError::MissingStorage => ErrorCodes::InvalidArgument,
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

    async fn load_existing_count(
        &self,
        storage: &Storage,
        key: &str,
    ) -> Result<i64, CountToFileAsyncError> {
        match storage.get(key, Default::default()).await {
            Ok(bytes) => {
                let body = std::str::from_utf8(bytes.as_ref()).map_err(|_| {
                    CountToFileAsyncError::InvalidPath(format!(
                        "count file at {} was not valid utf-8",
                        self.path
                    ))
                })?;
                body.trim().parse::<i64>().map_err(|_| {
                    CountToFileAsyncError::InvalidPath(format!(
                        "count file at {} did not contain an integer",
                        self.path
                    ))
                })
            }
            Err(StorageError::NotFound { .. }) => Ok(0),
            Err(err) => Err(CountToFileAsyncError::Read(err)),
        }
    }
}

#[async_trait]
impl AttachedFunctionExecutor for CountToFileAsyncExecutor {
    async fn execute(
        &self,
        input_records: Vec<Chunk<HydratedMaterializedLogRecord<'_, '_>>>,
        _output_reader: Option<&RecordSegmentReaderShard<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        let key = self
            .parse_storage_key(&self.storage)
            .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;

        let delete_count = input_records
            .iter()
            .flat_map(|batch| batch.iter())
            .filter(|(record, _)| {
                record.get_operation() == MaterializedLogOperation::DeleteExisting
            })
            .count() as i64;

        let insert_count = input_records
            .iter()
            .flat_map(|batch| batch.iter())
            .filter(|(record, _)| record.get_operation() == MaterializedLogOperation::AddNew)
            .count() as i64;

        let existing_count = self
            .load_existing_count(&self.storage, key)
            .await
            .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
        let new_total_count = existing_count + insert_count - delete_count;

        self.storage
            .put_bytes(
                key,
                new_total_count.to_string().into_bytes(),
                PutOptions::default(),
            )
            .await
            .map_err(|err| Box::new(CountToFileAsyncError::Write(err)) as Box<dyn ChromaError>)?;

        Ok(Chunk::new(Arc::from(Vec::<LogRecord>::new())))
    }
}
