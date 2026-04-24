use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
use chroma_types::{CollectionUuid, DatabaseName};
use futures::future::try_join_all;
use thiserror::Error;
use tracing::Level;
use wal3::{
    create_s3_factories, FragmentSeqNo, GarbageCollectionOptions, GarbageCollector, LogPosition,
    LogReaderOptions, LogWriterOptions, ManifestManager, S3FragmentManagerFactory,
    S3ManifestManagerFactory, SnapshotOptions, ThrottleOptions,
};

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
    pub database_name: Option<DatabaseName>,
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
                    let prefix = collection_id.storage_prefix_for_log();
                    let options = LogWriterOptions::default();
                    let (fragment_manager_factory, manifest_manager_factory) = create_s3_factories(
                        options.clone(),
                        LogReaderOptions::default(),
                        storage_clone.clone(),
                        prefix.clone(),
                        "garbage collection service".to_string(),
                        Arc::new(()),
                        Arc::new(()),
                    );
                    let writer = match GarbageCollector::<
                        (FragmentSeqNo, LogPosition),
                        S3FragmentManagerFactory,
                        S3ManifestManagerFactory,
                    >::open(
                        options,
                        fragment_manager_factory,
                        manifest_manager_factory,
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
                    let mut gc_state = wal3::GarbageCollectionState::empty();
                    for _ in 0..if self.enable_dangerous_option_to_ignore_min_versions_for_wal3 { 2 } else { 1 } {
                        // See README.md in wal3 for a description of why this happens in three phases.
                        match writer.garbage_collect_phase1_compute_garbage(&GarbageCollectionOptions::default(), min_log_offset).await {
                            Ok(Some(state)) => { gc_state = state; },
                            Ok(None) => return Ok(()),
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
                    if let Err(err) = logs.garbage_collect_phase2(input.database_name.clone(), collection_id).await {
                        tracing::error!("Unable to garbage collect log for collection [{collection_id}]: {err}");
                        return Err(DeleteUnusedLogsError::Gc(err));
                    };
                    match self.mode {
                        CleanupMode::DeleteV2 => {
                            if let Err(err) = writer.garbage_collect_phase3_delete_garbage(&GarbageCollectionOptions::default(), &gc_state).await {
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
                            let prefix = collection_id.storage_prefix_for_log();
                            let manifest_manager = match ManifestManager::new(
                                ThrottleOptions::default(),
                                SnapshotOptions::default(),
                                storage_clone.clone(),
                                prefix.clone(),
                                "destroy service".to_string(),
                                Arc::new(()),
                                Arc::new(()),
                            )
                            .await
                            {
                                Ok(mm) => mm,
                                Err(wal3::Error::UninitializedLog) => return Ok(()),
                                Err(err) => {
                                    tracing::error!(
                                        "Unable to create manifest manager for collection [{collection_id}]: {err:?}"
                                    );
                                    return Err(DeleteUnusedLogsError::Wal3 {
                                        collection_id,
                                        err,
                                    });
                                }
                            };
                            match wal3::destroy(storage_clone, &prefix, &manifest_manager).await {
                                Ok(()) => Ok(()),
                                Err(err) => {
                                    tracing::error!(
                                        "Unable to destroy log for collection [{collection_id}]: {err:?}"
                                    );
                                    Err(DeleteUnusedLogsError::Wal3 { collection_id, err })
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

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_log::{in_memory_log::InMemoryLog, Log};
    use chroma_storage::{s3_client_for_test_with_new_bucket, GetOptions};
    use chroma_system::Operator;
    use wal3::{Cursor, CursorName, CursorStore, CursorStoreOptions, LogWriter, SnapshotOptions};

    async fn seed_collection_log(storage: Storage, collection_id: CollectionUuid) -> LogPosition {
        let prefix = collection_id.storage_prefix_for_log();
        let options = LogWriterOptions {
            snapshot_manifest: SnapshotOptions {
                snapshot_rollover_threshold: 2,
                fragment_rollover_threshold: 2,
            },
            ..LogWriterOptions::default()
        };
        let storage = Arc::new(storage);
        let (fragment_factory, manifest_factory) = create_s3_factories(
            options.clone(),
            LogReaderOptions::default(),
            storage.clone(),
            prefix.clone(),
            "test-writer".to_string(),
            Arc::new(()),
            Arc::new(()),
        );
        let log = LogWriter::open_or_initialize(
            options,
            "test-writer",
            fragment_factory,
            manifest_factory,
            None,
        )
        .await
        .expect("log should initialize");

        let mut keep_position = LogPosition::default();
        for i in 0..40 {
            let position = log
                .append_many(
                    (0..5)
                        .map(|j| format!("collection:{i}:{j}").into_bytes())
                        .collect(),
                )
                .await
                .expect("append should succeed");
            if i == 20 {
                keep_position = position;
            }
        }

        let cursors = CursorStore::new(
            CursorStoreOptions::default(),
            storage,
            prefix,
            "cursor-writer".to_string(),
        );
        cursors
            .init(
                &CursorName::new("so_you_may_gc").expect("cursor name should be valid"),
                Cursor {
                    position: keep_position,
                    epoch_us: keep_position.offset(),
                    writer: "test-writer".to_string(),
                },
            )
            .await
            .expect("cursor should initialize");

        keep_position
    }

    #[tokio::test]
    async fn test_k8s_integration_delete_unused_logs_delete_mode_removes_garbage() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let collection_id = CollectionUuid::new();
        let keep_position = seed_collection_log(storage.clone(), collection_id).await;
        let prefix = collection_id.storage_prefix_for_log();
        let before = storage
            .list_prefix(&prefix, GetOptions::default())
            .await
            .expect("list should succeed");

        DeleteUnusedLogsOperator {
            enabled: true,
            mode: CleanupMode::DeleteV2,
            storage: storage.clone(),
            logs: Log::InMemory(InMemoryLog::new()),
            enable_dangerous_option_to_ignore_min_versions_for_wal3: false,
        }
        .run(&DeleteUnusedLogsInput {
            collections_to_destroy: HashSet::new(),
            collections_to_garbage_collect: HashMap::from([(collection_id, keep_position)]),
            database_name: None,
        })
        .await
        .expect("delete-mode GC should succeed");

        let after = storage
            .list_prefix(&prefix, GetOptions::default())
            .await
            .expect("list should succeed");

        assert!(
            after.len() < before.len(),
            "expected GC to delete at least one log artifact, before={} after={}",
            before.len(),
            after.len()
        );
    }

    #[tokio::test]
    #[ignore] // TODO(rescrv, gc week):  Unignore this test.
    async fn test_k8s_integration_delete_unused_logs_dry_run_keeps_files() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let collection_id = CollectionUuid::new();
        let keep_position = seed_collection_log(storage.clone(), collection_id).await;
        let prefix = collection_id.storage_prefix_for_log();
        let before = storage
            .list_prefix(&prefix, GetOptions::default())
            .await
            .expect("list should succeed")
            .into_iter()
            .collect::<HashSet<_>>();

        DeleteUnusedLogsOperator {
            enabled: true,
            mode: CleanupMode::DryRunV2,
            storage: storage.clone(),
            logs: Log::InMemory(InMemoryLog::new()),
            enable_dangerous_option_to_ignore_min_versions_for_wal3: false,
        }
        .run(&DeleteUnusedLogsInput {
            collections_to_destroy: HashSet::new(),
            collections_to_garbage_collect: HashMap::from([(collection_id, keep_position)]),
            database_name: None,
        })
        .await
        .expect("dry-run GC should succeed");

        let after = storage
            .list_prefix(&prefix, GetOptions::default())
            .await
            .expect("list should succeed")
            .into_iter()
            .collect::<HashSet<_>>();

        assert_eq!(after, before);
    }

    #[tokio::test]
    async fn test_k8s_integration_delete_unused_logs_hard_delete_destroys_collection_prefix() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let collection_id = CollectionUuid::new();
        seed_collection_log(storage.clone(), collection_id).await;
        let prefix = collection_id.storage_prefix_for_log();

        DeleteUnusedLogsOperator {
            enabled: true,
            mode: CleanupMode::DeleteV2,
            storage: storage.clone(),
            logs: Log::InMemory(InMemoryLog::new()),
            enable_dangerous_option_to_ignore_min_versions_for_wal3: false,
        }
        .run(&DeleteUnusedLogsInput {
            collections_to_destroy: HashSet::from([collection_id]),
            collections_to_garbage_collect: HashMap::new(),
            database_name: None,
        })
        .await
        .expect("hard-delete should succeed");

        let remaining = storage
            .list_prefix(&prefix, GetOptions::default())
            .await
            .expect("list should succeed");

        assert!(remaining.is_empty(), "expected log prefix to be destroyed");
    }
}
