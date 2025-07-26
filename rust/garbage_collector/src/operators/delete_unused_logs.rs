use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
use chroma_types::CollectionUuid;
use futures::future::try_join_all;
use thiserror::Error;
use wal3::{GarbageCollectionOptions, GarbageCollector, LogPosition, LogWriterOptions};

use crate::types::CleanupMode;

#[derive(Clone, Debug)]
pub struct DeleteUnusedLogsOperator {
    pub enabled: bool,
    pub mode: CleanupMode,
    pub storage: Storage,
    pub logs: Log,
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
    #[error(transparent)]
    Gc(#[from] chroma_log::GarbageCollectError),
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
        if !self.enabled {
            tracing::info!("Skipping log GC because it is not enabled for this tenant");
            return Ok(());
        }

        let storage_arc = Arc::new(self.storage.clone());
        if !input.collections_to_garbage_collect.is_empty() {
            let mut log_gc_futures = Vec::with_capacity(input.collections_to_garbage_collect.len());
            for (collection_id, minimum_log_offset_to_keep) in &input.collections_to_garbage_collect
            {
                let storage_clone = storage_arc.clone();
                let mut logs = self.logs.clone();
                log_gc_futures.push(async move {
                    let writer = match GarbageCollector::open(
                        LogWriterOptions::default(),
                        storage_clone,
                        &collection_id.storage_prefix_for_log(),
                        "garbage collection service",
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
                    // See README.md in wal3 for a description of why this happens in three phases.
                    match writer.garbage_collect_phase1_compute_garbage(&GarbageCollectionOptions::default(), Some(*minimum_log_offset_to_keep)).await {
                        Ok(true) => {},
                        Ok(false) => return Ok(()),
                        Err(err) => {
                            tracing::error!("Unable to garbage collect log for collection [{collection_id}]: {err}");
                            return Err(DeleteUnusedLogsError::Wal3(err));
                        }
                    };
                    if let Err(err) = logs.garbage_collect_phase2(*collection_id).await {
                        tracing::error!("Unable to garbage collect log for collection [{collection_id}]: {err}");
                        return Err(DeleteUnusedLogsError::Gc(err));
                    };
                    match self.mode {
                        CleanupMode::Delete | CleanupMode::DeleteV2 => {
                            if let Err(err) = writer.garbage_collect_phase3_delete_garbage(&GarbageCollectionOptions::default()).await {
                                tracing::error!("Unable to garbage collect log for collection [{collection_id}]: {err}");
                                return Err(DeleteUnusedLogsError::Wal3(err));
                            };
                        }
                        mode => {
                            tracing::info!("Skipping delete phase of log GC in {mode:?} mode");
                        }
                    }
                    Ok(())
                });
            }
            try_join_all(log_gc_futures).await?;
            tracing::info!(
                "Wal3 gc complete for collections: {:?}",
                input.collections_to_garbage_collect
            );
        }
        match self.mode {
            CleanupMode::Delete | CleanupMode::DeleteV2 => {
                if !input.collections_to_destroy.is_empty() {
                    let mut log_destroy_futures =
                        Vec::with_capacity(input.collections_to_destroy.len());
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
            }
            mode => {
                tracing::info!("Keeping logs for soft deleted collections in {mode:?} mode");
            }
        }

        Ok(())
    }
}
