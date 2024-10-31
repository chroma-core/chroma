use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Chunk, CollectionUuid, LogRecord};
use thiserror::Error;
use tonic::async_trait;
use tracing::trace;

use crate::{
    execution::operator::{Operator, OperatorType},
    log::log::{Log, PullLogsError},
};

#[derive(Clone, Debug)]
pub struct FetchLogOperator {
    pub(crate) log_client: Box<Log>,
    pub batch_size: u32,
    pub start_log_offset_id: u32,
    pub maximum_fetch_count: Option<u32>,
    pub collection_uuid: CollectionUuid,
}

pub type FetchLogInput = ();

pub type FetchLogOutput = Chunk<LogRecord>;

#[derive(Error, Debug)]
pub enum FetchLogError {
    #[error("Error when pulling log: {0}")]
    PullLog(#[from] PullLogsError),
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
        trace!("[{}]: {:?}", self.get_name(), self);

        let mut fetched = Vec::new();
        let mut log_client = self.log_client.clone();
        let mut offset = self.start_log_offset_id as i64;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as i64;
        loop {
            let mut log_batch = log_client
                .read(
                    self.collection_uuid,
                    offset,
                    self.batch_size as i32,
                    Some(timestamp),
                )
                .await?;

            let retrieve_count = log_batch.len();

            if let Some(last_log) = log_batch.last() {
                offset = last_log.log_offset + 1;
                fetched.append(&mut log_batch);
                if let Some(limit) = self.maximum_fetch_count {
                    if fetched.len() >= limit as usize {
                        // Enough logs have been fetched
                        fetched.truncate(limit as usize);
                        break;
                    }
                }
            }

            if retrieve_count < self.batch_size as usize {
                // No more logs to fetch
                break;
            }
        }
        tracing::info!(name: "Fetched log records", num_records = fetched.len());
        Ok(Chunk::new(fetched.into()))
    }
}

#[cfg(test)]
mod tests {
    use chroma_types::CollectionUuid;

    use crate::{
        execution::{operator::Operator, operators::fetch_log::FetchLogOperator},
        log::{
            log::{InMemoryLog, InternalLogRecord},
            test::{add_generator_0, LogGenerator},
        },
    };

    use super::Log;

    fn in_memory_log_setup() -> (CollectionUuid, Box<Log>) {
        let collection_id = CollectionUuid::new();
        let mut in_memory_log = InMemoryLog::new();
        let generator = LogGenerator {
            generator: add_generator_0,
        };
        generator.generate_vec(0..10).into_iter().for_each(|log| {
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
        (collection_id, Box::new(Log::InMemory(in_memory_log)))
    }

    #[tokio::test]
    async fn test_pull_all() {
        let (collection_uuid, log_client) = in_memory_log_setup();

        let fetch_log_operator = FetchLogOperator {
            log_client,
            batch_size: 100,
            start_log_offset_id: 0,
            maximum_fetch_count: None,
            collection_uuid,
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
        let (collection_uuid, log_client) = in_memory_log_setup();

        let fetch_log_operator = FetchLogOperator {
            log_client,
            batch_size: 100,
            start_log_offset_id: 3,
            maximum_fetch_count: Some(3),
            collection_uuid,
        };

        let logs = fetch_log_operator
            .run(&())
            .await
            .expect("Fetch log operator should not fail");

        assert_eq!(logs.len(), 3);
        logs.iter()
            .map(|(log, _)| log)
            .zip(3..6)
            .for_each(|(log, offset)| assert_eq!(log.log_offset, offset));
    }
}
