use crate::{Error, Snapshot, SnapshotPointer};

#[async_trait::async_trait]
pub trait SnapshotCache: Send + Sync {
    async fn get(&self, ptr: &SnapshotPointer) -> Result<Option<Snapshot>, Error>;
    async fn put(&self, ptr: &SnapshotPointer, snapshot: &Snapshot) -> Result<(), Error>;
}
