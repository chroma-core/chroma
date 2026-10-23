use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
use futures::{stream, StreamExt};
use thiserror::Error;
use wal3::{
    create_s3_factories, FragmentSeqNo, GarbageCollectionOptions, GarbageCollector, LogPosition,
    LogReaderOptions, LogWriterOptions, S3FragmentManagerFactory, S3ManifestManagerFactory,
};

const DIRTY_LOG_GC_CONCURRENCY: usize = 10;

#[derive(Clone, Debug)]
pub struct TruncateDirtyLogOperator {
    pub storage: Storage,
    pub logs: Log,
}

pub type TruncateDirtyLogInput = ();
pub type TruncateDirtyLogOutput = ();

#[derive(Debug, Error)]
pub enum TruncateDirtyLogError {
    #[error("Dirty log GC failed for {failed} of {total} members")]
    PartialFailure { failed: usize, total: usize },
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

#[derive(Debug, PartialEq)]
enum DirtyLogGcOutcome {
    Collected,
    Skipped,
}

// Drain every result before reporting failure so one unavailable member cannot
// cancel another member's in-flight GC. Bound storage work as well as RPCs.
async fn collect_dirty_logs<F, Fut>(
    members: Vec<String>,
    collect: F,
) -> Result<(), TruncateDirtyLogError>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<DirtyLogGcOutcome, TruncateDirtyLogError>>,
{
    if members.is_empty() {
        tracing::info!("No log service members available; deferring dirty log GC");
        return Ok(());
    }
    let total = members.len();
    let mut results = stream::iter(members.into_iter().map(|member_id| {
        let future = collect(member_id.clone());
        async move { (member_id, future.await) }
    }))
    .buffer_unordered(DIRTY_LOG_GC_CONCURRENCY);
    let (mut succeeded, mut skipped, mut failed) = (0, 0, 0);
    while let Some((member_id, result)) = results.next().await {
        match result {
            Ok(DirtyLogGcOutcome::Collected) => succeeded += 1,
            Ok(DirtyLogGcOutcome::Skipped) => skipped += 1,
            Err(error) => {
                failed += 1;
                tracing::error!(%member_id, %error, "Unable to garbage collect dirty log");
            }
        }
    }
    tracing::info!(total, succeeded, skipped, failed, "Finished dirty log GC");
    if failed > 0 {
        return Err(TruncateDirtyLogError::PartialFailure { failed, total });
    }
    Ok(())
}

impl TruncateDirtyLogOperator {
    async fn collect_member<Fut>(
        &self,
        member_id: &str,
        phase2: Fut,
    ) -> Result<DirtyLogGcOutcome, TruncateDirtyLogError>
    where
        Fut: Future<Output = Result<(), chroma_log::GarbageCollectError>>,
    {
        let options = LogWriterOptions::default();
        let (fragment_factory, manifest_factory) = create_s3_factories(
            options.clone(),
            LogReaderOptions::default(),
            Arc::new(self.storage.clone()),
            format!("dirty-{member_id}"),
            "garbage collection service".to_string(),
            Arc::new(()),
            Arc::new(()),
        );
        let writer = match GarbageCollector::<
            (FragmentSeqNo, LogPosition),
            S3FragmentManagerFactory,
            S3ManifestManagerFactory,
        >::open(options, fragment_factory, manifest_factory)
        .await
        {
            Ok(writer) => writer,
            // A newly joined member may not have initialized its dirty log yet.
            Err(wal3::Error::UninitializedLog) => return Ok(DirtyLogGcOutcome::Skipped),
            Err(error) => return Err(error.into()),
        };
        let options = GarbageCollectionOptions::default();
        let gc_state = match writer
            .garbage_collect_phase1_compute_garbage(&options, None)
            .await
        {
            Ok(Some(state)) => state,
            Ok(None) => return Ok(DirtyLogGcOutcome::Skipped),
            Err(wal3::Error::NoSuchCursor(_)) => {
                tracing::warn!(%member_id, "Dirty log has no cursor; skipping GC");
                return Ok(DirtyLogGcOutcome::Skipped);
            }
            Err(error) => return Err(error.into()),
        };
        // Never delete fragments unless the owner successfully updates its manifest.
        phase2.await?;
        match writer
            .garbage_collect_phase3_delete_garbage(&options, &gc_state)
            .await
        {
            Ok(()) => Ok(DirtyLogGcOutcome::Collected),
            Err(wal3::Error::NoSuchCursor(_)) => {
                tracing::warn!(%member_id, "Dirty log has no cursor; skipping GC");
                Ok(DirtyLogGcOutcome::Skipped)
            }
            Err(error) => Err(error.into()),
        }
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
        // Persisted manifests outlive pods. Only the current memberlist identifies
        // owners we can ask to perform phase two; retired logs remain untouched.
        collect_dirty_logs(self.logs.dirty_log_members(), |member_id| async move {
            let mut logs = self.logs.clone();
            self.collect_member(
                &member_id,
                logs.garbage_collect_phase2_for_dirty_log(&member_id),
            )
            .await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_log::{in_memory_log::InMemoryLog, Log};
    use chroma_storage::{s3_client_for_test_with_new_bucket, GetOptions};
    use chroma_system::Operator;
    use wal3::{Cursor, CursorName, CursorStore, CursorStoreOptions, LogWriter, SnapshotOptions};

    #[tokio::test]
    async fn dirty_log_gc_drains_failures_with_bounded_concurrency() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let completed = AtomicUsize::new(0);
        let total = DIRTY_LOG_GC_CONCURRENCY * 3;
        let result = collect_dirty_logs(
            (0..total)
                .map(|i| format!("rust-log-service-{i}"))
                .collect(),
            |member| {
                let (active, peak, completed) = (&active, &peak, &completed);
                async move {
                    let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(count, Ordering::SeqCst);
                    // Keep healthy members pending when the first member fails.
                    if member != "rust-log-service-0" {
                        tokio::task::yield_now().await;
                    }
                    active.fetch_sub(1, Ordering::SeqCst);
                    completed.fetch_add(1, Ordering::SeqCst);
                    if member == "rust-log-service-0" {
                        Err(
                            chroma_log::GarbageCollectError::Resolution("unreachable".into())
                                .into(),
                        )
                    } else {
                        Ok(DirtyLogGcOutcome::Collected)
                    }
                }
            },
        )
        .await;
        assert!(
            matches!(result, Err(TruncateDirtyLogError::PartialFailure { failed: 1, total: n }) if n == total)
        );
        assert_eq!(completed.load(Ordering::SeqCst), total);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(peak.load(Ordering::SeqCst) <= DIRTY_LOG_GC_CONCURRENCY);
        assert!(peak.load(Ordering::SeqCst) > 1);
    }

    #[tokio::test]
    async fn dirty_log_gc_empty_membership_does_no_work() {
        collect_dirty_logs(Vec::new(), |_| async {
            panic!("empty membership must not start GC");
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn dirty_log_gc_uses_exact_members_even_with_ordinal_gaps() {
        let visited = std::sync::Mutex::new(Vec::new());
        collect_dirty_logs(
            vec!["rust-log-service-0".into(), "rust-log-service-3".into()],
            |member| {
                visited.lock().unwrap().push(member);
                async { Ok(DirtyLogGcOutcome::Skipped) }
            },
        )
        .await
        .unwrap();
        assert_eq!(
            *visited.lock().unwrap(),
            vec!["rust-log-service-0", "rust-log-service-3"]
        );
    }

    #[tokio::test]
    async fn dirty_log_gc_operator_defers_without_members() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::Local(chroma_storage::local::LocalStorage::new(
            dir.path().to_str().unwrap(),
        ));
        TruncateDirtyLogOperator {
            storage,
            logs: Log::InMemory(InMemoryLog::new()),
        }
        .run(&())
        .await
        .unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn test_k8s_integration_dirty_log_gc_phase2_failure_preserves_fragments() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let (prefix, _writer) = seed_dirty_log(storage.clone(), 16).await;
        let fragment_prefix = format!("{prefix}/log/");
        let mut before = storage
            .list_prefix(&fragment_prefix, GetOptions::default())
            .await
            .unwrap();
        assert!(!before.is_empty());
        let result = TruncateDirtyLogOperator {
            storage: storage.clone(),
            logs: Log::InMemory(InMemoryLog::new()),
        }
        .collect_member("rust-log-service-16", async {
            Err(chroma_log::GarbageCollectError::Resolution(
                "unreachable".into(),
            ))
        })
        .await;
        assert!(matches!(result, Err(TruncateDirtyLogError::Gc(_))));
        let mut after = storage
            .list_prefix(&fragment_prefix, GetOptions::default())
            .await
            .unwrap();
        before.sort();
        after.sort();
        assert_eq!(
            before, after,
            "phase-three deletion must not run after phase-two failure"
        );
    }

    async fn seed_dirty_log(
        storage: Storage,
        replica_id: u64,
    ) -> (
        String,
        LogWriter<(FragmentSeqNo, LogPosition), S3FragmentManagerFactory, S3ManifestManagerFactory>,
    ) {
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

        (prefix, log)
    }

    #[tokio::test]
    async fn test_k8s_integration_truncate_dirty_log_defers_without_members() {
        let storage = s3_client_for_test_with_new_bucket().await;

        TruncateDirtyLogOperator {
            storage,
            logs: Log::InMemory(InMemoryLog::new()),
        }
        .run(&())
        .await
        .expect("missing membership should defer GC");
    }

    #[tokio::test]
    async fn test_k8s_integration_truncate_dirty_log_truncates_multiple_prefixes() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let (prefix0, writer0) = seed_dirty_log(storage.clone(), 0).await;
        let (prefix1, writer1) = seed_dirty_log(storage.clone(), 1).await;
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
        .collect_member("rust-log-service-0", async {
            writer0
                .garbage_collect_phase2_update_manifest(&GarbageCollectionOptions::default())
                .await
                .unwrap();
            Ok(())
        })
        .await
        .expect("dirty log truncation should succeed");
        TruncateDirtyLogOperator {
            storage: storage.clone(),
            logs: Log::InMemory(InMemoryLog::new()),
        }
        .collect_member("rust-log-service-1", async {
            writer1
                .garbage_collect_phase2_update_manifest(&GarbageCollectionOptions::default())
                .await
                .unwrap();
            Ok(())
        })
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
