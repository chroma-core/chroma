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
use tracing::Level;
use wal3::{GarbageCollectionOptions, GarbageCollector, LogPosition, LogWriterOptions};

use crate::types::CleanupMode;

#[derive(Clone, Debug)]
pub struct DeleteUnusedLogsOperator {
    pub enabled: bool,
    pub mode: CleanupMode,
    pub storage: Storage,
    pub logs: Log,
    pub enable_dangerous_option_to_ignore_min_versions_for_wal3: bool,
}

#[derive(Clone, Debug)]
pub struct DeleteUnusedLogsInput {
    pub collections_to_destroy: HashSet<CollectionUuid>,
    pub collections_to_garbage_collect: HashMap<CollectionUuid, LogPosition>,
}

pub type DeleteUnusedLogsOutput = ();

#[derive(Debug, Error)]
pub enum DeleteUnusedLogsError {
    #[error("failed to garbage collect in wal3 for {collection_id}: {err}")]
    Wal3 {
        collection_id: CollectionUuid,
        err: wal3::Error,
    },
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
                let collection_id = *collection_id;
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
                            return Err(DeleteUnusedLogsError::Wal3{ collection_id, err})
                        }
                    };
                    // NOTE(rescrv):  Once upon a time, we had a bug where we would not pass the
                    // min log offset into the wal3 garbage collect process.  For disaster
                    // recovery, we need to keep N versions per the config, but the collections
                    // (staging only) were collected up to the most recent version.  The fix is
                    // this hack to allow a corrupt garbage error to be masked iff the appropriate
                    // configuration value is set.
                    //
                    // The configuration is
                    // enable_dangerous_option_to_ignore_min_versions_for_wal3.  Its default is
                    // false.  Setting it to true enables this loop to skip the min_log_offset.
                    //
                    // To remove this hack, search for the warning below, and then make every
                    // collection that appears with that warning compact min-versions-to-keep
                    // times.
                    let mut min_log_offset = Some(*minimum_log_offset_to_keep);
                    for _ in 0..if self.enable_dangerous_option_to_ignore_min_versions_for_wal3 { 2 } else { 1 } {
                        // See README.md in wal3 for a description of why this happens in three phases.
                        match writer.garbage_collect_phase1_compute_garbage(&GarbageCollectionOptions::default(), min_log_offset).await {
                            Ok(true) => {},
                            Ok(false) => return Ok(()),
                            Err(wal3::Error::CorruptGarbage(c)) if c.starts_with("First to keep does not overlap manifest") => {
                                if self.enable_dangerous_option_to_ignore_min_versions_for_wal3 {
                                    tracing::event!(Level::WARN, name = "encountered enable_dangerous_option_to_ignore_min_versions_for_wal3 path", collection_id =? collection_id);
                                    min_log_offset.take();
                                }
                            }
                            Err(err) => {
                                tracing::error!("Unable to garbage collect log for collection [{collection_id}]: {err}");
                                return Err(DeleteUnusedLogsError::Wal3{ collection_id, err});
                            }
                        };
                    }
                    if let Err(err) = logs.garbage_collect_phase2(collection_id).await {
                        tracing::error!("Unable to garbage collect log for collection [{collection_id}]: {err}");
                        return Err(DeleteUnusedLogsError::Gc(err));
                    };
                    match self.mode {
                        CleanupMode::DeleteV2 => {
                            if let Err(err) = writer.garbage_collect_phase3_delete_garbage(&GarbageCollectionOptions::default()).await {
                                tracing::error!("Unable to garbage collect log for collection [{collection_id}]: {err}");
                                return Err(DeleteUnusedLogsError::Wal3{ collection_id, err});
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
            CleanupMode::DeleteV2 => {
                if !input.collections_to_destroy.is_empty() {
                    let mut log_destroy_futures =
                        Vec::with_capacity(input.collections_to_destroy.len());
                    for collection_id in &input.collections_to_destroy {
                        let collection_id = *collection_id;
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
                                    Err(DeleteUnusedLogsError::Wal3{ collection_id, err})
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
