use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
use futures::future::try_join_all;
use thiserror::Error;
use wal3::{GarbageCollectionOptions, GarbageCollector, LogWriterOptions};

#[derive(Clone, Debug)]
pub struct TruncateDirtyLogOperator {
    pub storage: Storage,
    pub logs: Log,
}

pub type TruncateDirtyLogInput = ();

pub type TruncateDirtyLogOutput = ();

#[derive(Debug, Error)]
pub enum TruncateDirtyLogError {
    #[error("No log service found")]
    NoLogServiceFound,
    #[error(transparent)]
    Wal3(#[from] wal3::Error),
    #[error(transparent)]
    Gc(#[from] chroma_log::GarbageCollectError),
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
        let mut replica_id = 0u64;
        loop {
            let dirty_log_prefix = format!("dirty-rust-log-service-{replica_id}");
            match GarbageCollector::open(
                LogWriterOptions::default(),
                storage_arc.clone(),
                &dirty_log_prefix,
                "garbage collection service",
            )
            .await
            {
                Ok(log_writer) => dirty_log_writers.push((log_writer, replica_id)),
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
        try_join_all(dirty_log_writers.into_iter().map(|(writer, index)| {
            let mut logs = self.logs.clone();
            async move {
                match writer
                    .garbage_collect_phase1_compute_garbage(
                        &GarbageCollectionOptions::default(),
                        None,
                    )
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => return Ok(()),
                    Err(wal3::Error::NoSuchCursor(_)) => {
                        tracing::warn!(
                            "dirty log has no cursor; this should not happen in steady state"
                        );
                        return Ok(());
                    }
                    Err(err) => {
                        tracing::error!("Unable to garbage collect dirty log: {err}");
                        return Err(TruncateDirtyLogError::Wal3(err));
                    }
                };
                logs.garbage_collect_phase2_for_dirty_log(index)
                    .await
                    .map_err(TruncateDirtyLogError::Gc)?;
                match writer
                    .garbage_collect_phase3_delete_garbage(&GarbageCollectionOptions::default())
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(wal3::Error::NoSuchCursor(_)) => {
                        tracing::warn!(
                            "dirty log has no cursor; this should not happen in steady state"
                        );
                        Ok(())
                    }
                    Err(err) => {
                        tracing::error!("Unable to garbage collect dirty log: {err}");
                        Err(TruncateDirtyLogError::Wal3(err))
                    }
                }
            }
        }))
        .await?;
        Ok(())
    }
}
