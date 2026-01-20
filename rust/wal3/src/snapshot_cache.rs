use std::sync::Arc;

use crate::{Error, Snapshot, SnapshotPointer};

/// A cache for snapshots.
#[async_trait::async_trait]
pub trait SnapshotCache: Send + Sync + 'static {
    /// Get a snapshot from the cache.
    async fn get(&self, ptr: &SnapshotPointer) -> Result<Option<Snapshot>, Error>;
    /// Put a snapshot into the cache.
    async fn put(&self, ptr: &SnapshotPointer, snapshot: &Snapshot) -> Result<(), Error>;
}

#[async_trait::async_trait]
impl SnapshotCache for () {
    async fn get(&self, _: &SnapshotPointer) -> Result<Option<Snapshot>, Error> {
        Ok(None)
    }

    async fn put(&self, _: &SnapshotPointer, _: &Snapshot) -> Result<(), Error> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl SnapshotCache for Arc<dyn SnapshotCache> {
    async fn get(&self, ptr: &SnapshotPointer) -> Result<Option<Snapshot>, Error> {
        self.as_ref().get(ptr).await
    }

    async fn put(&self, ptr: &SnapshotPointer, snapshot: &Snapshot) -> Result<(), Error> {
        self.as_ref().put(ptr, snapshot).await
    }
}
