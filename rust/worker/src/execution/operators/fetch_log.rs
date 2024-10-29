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
    pub skip: u32,
    pub fetch: Option<u32>,
    pub collection: CollectionUuid,
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
        let mut offset = self.skip as i64;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as i64;
        loop {
            if let Some(limit) = self.fetch {
                if fetched.len() >= limit as usize {
                    // Enough logs have been fetched
                    fetched.truncate(limit as usize);
                    break;
                }
            }

            let mut log_batch = log_client
                .read(
                    self.collection,
                    offset,
                    self.batch_size as i32,
                    Some(timestamp),
                )
                .await?;

            let retrieve_count = log_batch.len();

            if let Some(last_log) = log_batch.last() {
                offset = last_log.log_offset + 1;
                fetched.append(&mut log_batch);
            }

            if retrieve_count < self.batch_size as usize {
                // No more logs to fetch
                break;
            }
        }
        tracing::info!(name: "Pulled log records", num_records = fetched.len());
        Ok(Chunk::new(fetched.into()))
    }
}
