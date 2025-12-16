use std::sync::Arc;

use chroma_storage::Storage;

use crate::interfaces::ManifestConsumer;
use crate::Error;
use crate::FragmentSeqNo;
use crate::LogPosition;
use crate::LogReaderOptions;
use crate::Snapshot;
use crate::SnapshotCache;
use crate::SnapshotPointer;

pub struct ManifestReader {
    options: LogReaderOptions,
    storage: Arc<Storage>,
    prefix: String,
    snapshot_cache: Arc<dyn SnapshotCache>,
}

impl ManifestReader {
    pub fn new(
        options: LogReaderOptions,
        storage: Arc<Storage>,
        prefix: String,
        snapshot_cache: Arc<dyn SnapshotCache>,
    ) -> Self {
        Self {
            options,
            storage,
            prefix,
            snapshot_cache,
        }
    }
}

#[async_trait::async_trait]
impl ManifestConsumer<(FragmentSeqNo, LogPosition)> for ManifestReader {
    async fn snapshot_load(&self, pointer: &SnapshotPointer) -> Result<Option<Snapshot>, Error> {
        super::snapshot_load(
            self.options.throttle,
            &self.storage,
            &self.prefix,
            &self.snapshot_cache,
            pointer,
        )
        .await
    }
}
