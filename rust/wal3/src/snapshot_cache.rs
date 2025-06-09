use crate::{Error, Snapshot, SnapshotPointer};

#[async_trait::async_trait]
pub trait SnapshotCache: Send + Sync {
    async fn get(&self, ptr: &SnapshotPointer) -> Result<Option<Snapshot>, Error>;
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
