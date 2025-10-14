use std::sync::Arc;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_system::{Operator, OperatorType};
use chroma_types::{Chunk, CollectionUuid, LogRecord};
use thiserror::Error;

/// The `FetchLogOperator` fetches logs from the log service
///
/// # Parameters
/// - `log_client`: The log service reader
/// - `batch_size`: The maximum number of logs to fetch by `log_client` at a time
/// - `start_log_offset_id`: The offset id of the first log to read
/// - `maximum_fetch_count`: The maximum number of logs to fetch in total
/// - `collection_uuid`: The uuid of the collection where the fetched logs should belong
///
/// # Inputs
/// - No input is required
///
/// # Outputs
/// - The contiguous chunk of logs belong to the collection with `collection_uuid`
///   starting from `start_log_offset_id`. At most `maximum_fetch_count` number of logs
///   will be fetched
///
/// # Usage
/// It should be run at the start of an orchestrator to get the latest data of a collection
#[derive(Clone, Debug)]
pub struct FetchLogOperator {
    pub log_client: Log,
    pub batch_size: u32,
    pub start_log_offset_id: u64,
    pub maximum_fetch_count: Option<u32>,
    pub collection_uuid: CollectionUuid,
    pub tenant: String,
}

type FetchLogInput = ();

pub type FetchLogOutput = Chunk<LogRecord>;

#[derive(Error, Debug)]
pub enum FetchLogError {
    #[error("Error when pulling log: {0}")]
    PullLog(#[from] Box<dyn ChromaError>),
    #[error("Error when capturing system time: {0}")]
    SystemTime(#[from] SystemTimeError),
}

impl ChromaError for FetchLogError {
    fn code(&self) -> ErrorCodes {
        match self {
            FetchLogError::PullLog(e) => e.code(),
            FetchLogError::SystemTime(_) => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<FetchLogInput, FetchLogOutput> for FetchLogOperator {
    type Error = FetchLogError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(&self, _: &FetchLogInput) -> Result<FetchLogOutput, FetchLogError> {
        tracing::debug!(
            batch_size = self.batch_size,
            start_log_offset_id = self.start_log_offset_id,
            maximum_fetch_count = self.maximum_fetch_count,
            collection_uuid = ?self.collection_uuid.0,
            "[{}]",
            self.get_name(),
        );

        let mut log_client = self.log_client.clone();
        let mut limit_offset = log_client
            .scout_logs(&self.tenant, self.collection_uuid, self.start_log_offset_id)
            .await
            .inspect_err(|err| {
                tracing::error!("could not pull logs: {err:?}");
            })?;
        let mut fetched = Vec::new();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as i64;

        if let Some(maximum_fetch_count) = self.maximum_fetch_count {
            limit_offset = std::cmp::min(
                limit_offset,
                self.start_log_offset_id + maximum_fetch_count as u64,
            );
        }

        let window_size: usize = self.batch_size as usize;
        let ranges = (self.start_log_offset_id..limit_offset)
            .step_by(window_size)
            .map(|x| (x, std::cmp::min(x + window_size as u64, limit_offset)))
            .collect::<Vec<_>>();
        let sema = Arc::new(tokio::sync::Semaphore::new(10));
        let batch_readers = ranges
            .into_iter()
            .map(|(start, limit)| {
                let mut log_client = log_client.clone();
                let collection_uuid = self.collection_uuid;
                let num_records = (limit - start) as i32;
                let start = start as i64;
                let sema = Arc::clone(&sema);
                async move {
                    let _permit = sema.acquire().await.unwrap();
                    log_client
                        .read(
                            &self.tenant,
                            collection_uuid,
                            start,
                            num_records,
                            Some(timestamp),
                        )
                        .await
                }
            })
            .collect::<Vec<_>>();
        let batches = futures::future::join_all(batch_readers).await;
        for batch in batches {
            match batch {
                Ok(batch) => fetched.extend(batch),
                Err(err) => {
                    return Err(FetchLogError::PullLog(Box::new(err)));
                }
            }
        }
        fetched.sort_by_key(|f| f.log_offset);
        Ok(Chunk::new(fetched.into()))
    }
}

#[cfg(test)]
mod tests {
    use chroma_log::{
        in_memory_log::{InMemoryLog, InternalLogRecord},
        test::{upsert_generator, LogGenerator},
    };
    use chroma_system::Operator;
    use chroma_types::CollectionUuid;

    use crate::execution::operators::fetch_log::FetchLogOperator;

    use super::Log;

    fn setup_in_memory_log() -> (CollectionUuid, Log) {
        let collection_id = CollectionUuid::new();
        let mut in_memory_log = InMemoryLog::new();
        upsert_generator
            .generate_vec(0..10)
            .into_iter()
            .for_each(|log| {
                in_memory_log.add_log(
                    collection_id,
                    InternalLogRecord {
                        collection_id,
                        log_offset: log.log_offset,
                        log_ts: log.log_offset,
                        record: log,
                    },
                )
            });
        (collection_id, Log::InMemory(in_memory_log))
    }

    #[tokio::test]
    async fn test_pull_all() {
        let (collection_uuid, log_client) = setup_in_memory_log();

        let fetch_log_operator = FetchLogOperator {
            log_client,
            batch_size: 2,
            start_log_offset_id: 0,
            maximum_fetch_count: None,
            collection_uuid,
            tenant: "test-tenant".to_string(),
        };

        let logs = fetch_log_operator
            .run(&())
            .await
            .expect("Fetch log operator should not fail");

        assert_eq!(logs.len(), 10);
        logs.iter()
            .map(|(log, _)| log)
            .zip(0..10)
            .for_each(|(log, offset)| assert_eq!(log.log_offset, offset));
    }

    #[tokio::test]
    async fn test_pull_range() {
        let (collection_uuid, log_client) = setup_in_memory_log();

        let fetch_log_operator = FetchLogOperator {
            log_client,
            batch_size: 2,
            start_log_offset_id: 3,
            maximum_fetch_count: Some(3),
            collection_uuid,
            tenant: "test-tenant".to_string(),
        };

        let logs = fetch_log_operator
            .run(&())
            .await
            .expect("FetchLogOperator should not fail");

        assert_eq!(logs.len(), 3);
        logs.iter()
            .map(|(log, _)| log)
            .zip(3..6)
            .for_each(|(log, offset)| assert_eq!(log.log_offset, offset));
    }
}
