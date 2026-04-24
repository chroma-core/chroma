use std::sync::Arc;

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
use futures::future::try_join_all;
use thiserror::Error;
use wal3::{
    create_s3_factories, FragmentSeqNo, GarbageCollectionOptions, GarbageCollector, LogPosition,
    LogReaderOptions, LogWriterOptions, S3FragmentManagerFactory, S3ManifestManagerFactory,
};

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
            let options = LogWriterOptions::default();
            let (fragment_manager_factory, manifest_manager_factory) = create_s3_factories(
                options.clone(),
                LogReaderOptions::default(),
                storage_arc.clone(),
                dirty_log_prefix.clone(),
                "garbage collection service".to_string(),
                Arc::new(()),
                Arc::new(()),
            );
            match GarbageCollector::<
                (FragmentSeqNo, LogPosition),
                S3FragmentManagerFactory,
                S3ManifestManagerFactory,
            >::open(options, fragment_manager_factory, manifest_manager_factory)
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
                let gc_state = match writer
                    .garbage_collect_phase1_compute_garbage(
                        &GarbageCollectionOptions::default(),
                        None,
                    )
                    .await
                {
                    Ok(Some(state)) => state,
                    Ok(None) => return Ok(()),
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
                    .garbage_collect_phase3_delete_garbage(
                        &GarbageCollectionOptions::default(),
                        &gc_state,
                    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_log::{in_memory_log::InMemoryLog, Log};
    use chroma_storage::{s3_client_for_test_with_new_bucket, GetOptions};
    use chroma_system::Operator;
    use wal3::{Cursor, CursorName, CursorStore, CursorStoreOptions, LogWriter, SnapshotOptions};

    async fn seed_dirty_log(storage: Storage, replica_id: u64) -> String {
        let prefix = format!("dirty-rust-log-service-{replica_id}");
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
            "dirty-log-writer".to_string(),
            Arc::new(()),
            Arc::new(()),
        );
        let log = LogWriter::open_or_initialize(
            options,
            "dirty-log-writer",
            fragment_factory,
            manifest_factory,
            None,
        )
        .await
        .expect("dirty log should initialize");

        let mut keep_position = LogPosition::default();
        for i in 0..40 {
            let position = log
                .append_many(
                    (0..5)
                        .map(|j| format!("dirty:{replica_id}:{i}:{j}").into_bytes())
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
            prefix.clone(),
            "cursor-writer".to_string(),
        );
        cursors
            .init(
                &CursorName::new("so_you_may_gc").expect("cursor name should be valid"),
                Cursor {
                    position: keep_position,
                    epoch_us: keep_position.offset(),
                    writer: "dirty-log-writer".to_string(),
                },
            )
            .await
            .expect("cursor should initialize");

        prefix
    }

    #[tokio::test]
    async fn test_k8s_integration_truncate_dirty_log_returns_no_log_service_found_when_uninitialized(
    ) {
        let storage = s3_client_for_test_with_new_bucket().await;

        let err = TruncateDirtyLogOperator {
            storage,
            logs: Log::InMemory(InMemoryLog::new()),
        }
        .run(&())
        .await
        .expect_err("missing dirty logs should fail");

        assert!(matches!(err, TruncateDirtyLogError::NoLogServiceFound));
    }

    #[tokio::test]
    async fn test_k8s_integration_truncate_dirty_log_truncates_multiple_prefixes() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let prefix0 = seed_dirty_log(storage.clone(), 0).await;
        let prefix1 = seed_dirty_log(storage.clone(), 1).await;
        let before0 = storage
            .list_prefix(&prefix0, GetOptions::default())
            .await
            .expect("list should succeed");
        let before1 = storage
            .list_prefix(&prefix1, GetOptions::default())
            .await
            .expect("list should succeed");

        TruncateDirtyLogOperator {
            storage: storage.clone(),
            logs: Log::InMemory(InMemoryLog::new()),
        }
        .run(&())
        .await
        .expect("dirty log truncation should succeed");

        let after0 = storage
            .list_prefix(&prefix0, GetOptions::default())
            .await
            .expect("list should succeed");
        let after1 = storage
            .list_prefix(&prefix1, GetOptions::default())
            .await
            .expect("list should succeed");

        assert!(
            after0.len() < before0.len(),
            "expected replica 0 dirty log to shrink, before={} after={}",
            before0.len(),
            after0.len()
        );
        assert!(
            after1.len() < before1.len(),
            "expected replica 1 dirty log to shrink, before={} after={}",
            before1.len(),
            after1.len()
        );
    }
}
