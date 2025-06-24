use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
use futures::future::try_join_all;
use thiserror::Error;
use wal3::{GarbageCollectionOptions, LogWriter, LogWriterOptions};

#[derive(Clone, Debug)]
pub struct TruncateDirtyLogOperator {
    pub storage: Storage,
}

pub type TruncateDirtyLogInput = ();

pub type TruncateDirtyLogOutput = ();

#[derive(Debug, Error)]
pub enum TruncateDirtyLogError {
    #[error("No log service found")]
    NoLogServiceFound,
    #[error(transparent)]
    Wal3(#[from] wal3::Error),
}

impl ChromaError for TruncateDirtyLogError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<TruncateDirtyLogInput, TruncateDirtyLogOutput> for TruncateDirtyLogOperator {
    type Error = TruncateDirtyLogError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        _input: &TruncateDirtyLogInput,
    ) -> Result<TruncateDirtyLogOutput, TruncateDirtyLogError> {
        let storage_arc = Arc::new(self.storage.clone());

        // NOTE: This is a hack to get the dirty log writers for all available log services
        // It tries to open the dirty log manifest sequentially until the manifest is not found
        // It assumes that all dirty log paths looks like `dirty-rust-log-service-<replica_id>`
        let mut dirty_log_writers = Vec::new();
        let mut replica_id = 0;
        loop {
            let dirty_log_prefix = format!("dirty-rust-log-service-{replica_id}");
            match LogWriter::open(
                LogWriterOptions::default(),
                storage_arc.clone(),
                &dirty_log_prefix,
                "garbage collection service",
                (),
            )
            .await
            {
                Ok(log_writer) => dirty_log_writers.push(log_writer),
                Err(wal3::Error::UninitializedLog) => break,
                Err(err) => {
                    tracing::error!("Unable to open dirty log [{dirty_log_prefix}]: {err}");
                    break;
                }
            };
            replica_id += 1;
        }
        if dirty_log_writers.is_empty() {
            tracing::error!("Unable to find any dirty log manifest. Skipping dirty log GC");
            return Err(TruncateDirtyLogError::NoLogServiceFound);
        }
        try_join_all(dirty_log_writers.into_iter().map(|writer| async move {
            match writer
                .garbage_collect(&GarbageCollectionOptions::default(), None)
                .await
            {
                Ok(()) => Ok(()),
                Err(err) => {
                    tracing::error!("Unable to garbage collect dirty log: {err}");
                    Err(TruncateDirtyLogError::Wal3(err))
                }
            }
        }))
        .await?;
        Ok(())
    }
}
