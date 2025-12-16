use std::sync::Arc;

use setsum::Setsum;

use wal3::{
    FragmentSeqNo, Garbage, GarbageCollectionOptions, LogPosition, ManifestAndETag,
    ManifestPublisher, Snapshot, SnapshotCache, SnapshotPointer,
};

/// A mock ManifestPublisher that delegates snapshot_load to a SnapshotCache.
/// Used in tests to provide snapshot loading without needing full storage infrastructure.
pub struct MockManifestPublisher<C: SnapshotCache> {
    snapshot_cache: Arc<C>,
}

impl<C: SnapshotCache> MockManifestPublisher<C> {
    pub fn new(snapshot_cache: Arc<C>) -> Self {
        Self { snapshot_cache }
    }
}

#[async_trait::async_trait]
impl<C: SnapshotCache> ManifestPublisher<(FragmentSeqNo, LogPosition)>
    for MockManifestPublisher<C>
{
    async fn recover(&mut self) -> Result<(), wal3::Error> {
        Ok(())
    }

    async fn manifest_and_etag(&self) -> Result<ManifestAndETag, wal3::Error> {
        Err(wal3::Error::UninitializedLog)
    }

    fn assign_timestamp(&self, _record_count: usize) -> Option<(FragmentSeqNo, LogPosition)> {
        None
    }

    async fn publish_fragment(
        &self,
        _pointer: &(FragmentSeqNo, LogPosition),
        _path: &str,
        _messages_len: u64,
        _num_bytes: u64,
        _setsum: Setsum,
    ) -> Result<LogPosition, wal3::Error> {
        Err(wal3::Error::UninitializedLog)
    }

    async fn garbage_applies_cleanly(&self, _garbage: &Garbage) -> Result<bool, wal3::Error> {
        Ok(false)
    }

    async fn apply_garbage(&self, _garbage: Garbage) -> Result<(), wal3::Error> {
        Err(wal3::Error::UninitializedLog)
    }

    async fn compute_garbage(
        &self,
        _options: &GarbageCollectionOptions,
        _first_to_keep: LogPosition,
    ) -> Result<Option<Garbage>, wal3::Error> {
        Err(wal3::Error::UninitializedLog)
    }

    async fn snapshot_load(
        &self,
        pointer: &SnapshotPointer,
    ) -> Result<Option<Snapshot>, wal3::Error> {
        self.snapshot_cache.get(pointer).await
    }

    fn shutdown(&self) {}
}
