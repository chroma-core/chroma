use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
use chroma_types::CollectionUuid;
use futures::future::try_join_all;
use thiserror::Error;
use wal3::{GarbageCollectionOptions, LogPosition, LogWriter, LogWriterOptions};

#[derive(Clone, Debug)]
pub struct DeleteUnusedLogsOperator {
    pub dry_run: bool,
    pub storage: Storage,
}

#[derive(Clone, Debug)]
pub struct DeleteUnusedLogsInput {
    pub collections_to_destroy: HashSet<CollectionUuid>,
    pub collections_to_garbage_collect: HashMap<CollectionUuid, LogPosition>,
}

pub type DeleteUnusedLogsOutput = ();

#[derive(Debug, Error)]
pub enum DeleteUnusedLogsError {
    #[error(transparent)]
    Wal3(#[from] wal3::Error),
}

impl ChromaError for DeleteUnusedLogsError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<DeleteUnusedLogsInput, DeleteUnusedLogsOutput> for DeleteUnusedLogsOperator {
    type Error = DeleteUnusedLogsError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &DeleteUnusedLogsInput,
    ) -> Result<DeleteUnusedLogsOutput, DeleteUnusedLogsError> {
        tracing::info!("Garbage collecting logs: {input:?}");
        if self.dry_run {
            tracing::info!("Skipping actual log cleanup in dry run mode");
            return Ok(());
        }
        let storage_arc = Arc::new(self.storage.clone());
        if !input.collections_to_garbage_collect.is_empty() {
            let mut log_gc_futures = Vec::with_capacity(input.collections_to_garbage_collect.len());
            for (collection_id, minimum_log_offset_to_keep) in &input.collections_to_garbage_collect
            {
                let storage_clone = storage_arc.clone();
                log_gc_futures.push(async move {
                    let writer = match LogWriter::open(
                        LogWriterOptions::default(),
                        storage_clone,
                        &collection_id.storage_prefix_for_log(),
                        "garbage collection service",
                        (),
                    )
                    .await
                    {
                        Ok(log_writer) => log_writer,
                        Err(wal3::Error::UninitializedLog) => return Ok(()),
                        Err(err) => {
                            tracing::error!("Unable to initialize log writer for collection [{collection_id}]: {err}");
                            return Err(DeleteUnusedLogsError::Wal3(err))
                        }
                    };
                    match writer.garbage_collect(&GarbageCollectionOptions::default(), Some(*minimum_log_offset_to_keep)).await {
                        Ok(()) => Ok(()),
                        Err(err) => {
                            tracing::error!("Unable to garbage collect log for collection [{collection_id}]: {err}");
                            Err(DeleteUnusedLogsError::Wal3(err))
                        },
                    }
                });
            }
            try_join_all(log_gc_futures).await?;
            tracing::info!(
                "Wal3 gc complete for collections: {:?}",
                input.collections_to_garbage_collect
            );
        }
        if !input.collections_to_destroy.is_empty() {
            let mut log_destroy_futures = Vec::with_capacity(input.collections_to_destroy.len());
            for collection_id in &input.collections_to_destroy {
                let storage_clone = storage_arc.clone();
                log_destroy_futures.push(async move {
                    match wal3::destroy(storage_clone, &collection_id.storage_prefix_for_log())
                        .await
                    {
                        Ok(()) => Ok(()),
                        Err(err) => {
                            tracing::error!(
                                "Unable to destroy log for collection [{collection_id}]: {err:?}"
                            );
                            Err(DeleteUnusedLogsError::Wal3(err))
                        }
                    }
                })
            }
            try_join_all(log_destroy_futures).await?;
            tracing::info!(
                "Wal3 destruction complete for collections: {:?}",
                input.collections_to_destroy
            );
        }

        Ok(())
    }
}
