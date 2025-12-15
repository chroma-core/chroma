use std::sync::Arc;

use chroma_storage::Storage;

use crate::interfaces::{
    FragmentPublisherFactory as FragmentPublisherFactoryTrait,
    ManifestPublisherFactory as ManifestPublisherFactoryTrait,
};
use crate::{Error, FragmentSeqNo, LogPosition, LogWriterOptions, MarkDirty, SnapshotCache};

pub mod batch_manager;
pub mod manifest_manager;

pub use batch_manager::{upload_parquet, BatchManager};
pub use manifest_manager::ManifestManager;

pub struct FragmentPublisherFactory {
    pub options: LogWriterOptions,
    pub storage: Arc<Storage>,
    pub prefix: String,
    pub mark_dirty: Arc<dyn MarkDirty>,
}

#[async_trait::async_trait]
impl FragmentPublisherFactoryTrait for FragmentPublisherFactory {
    type FragmentPointer = (FragmentSeqNo, LogPosition);
    type Publisher = BatchManager;

    async fn make(&self) -> Result<Self::Publisher, Error> {
        BatchManager::new(
            self.options.clone(),
            Arc::clone(&self.storage),
            self.prefix.clone(),
            Arc::clone(&self.mark_dirty),
        )
        .ok_or_else(|| Error::internal(file!(), line!()))
    }
}

pub struct ManifestPublisherFactory {
    pub options: LogWriterOptions,
    pub storage: Arc<Storage>,
    pub prefix: String,
    pub writer: String,
    pub mark_dirty: Arc<dyn MarkDirty>,
    pub snapshot_cache: Arc<dyn SnapshotCache>,
}

#[async_trait::async_trait]
impl ManifestPublisherFactoryTrait for ManifestPublisherFactory {
    type FragmentPointer = (FragmentSeqNo, LogPosition);
    type Publisher = ManifestManager;

    async fn make(&self) -> Result<Self::Publisher, Error> {
        ManifestManager::new(
            self.options.throttle_manifest,
            self.options.snapshot_manifest,
            Arc::clone(&self.storage),
            self.prefix.clone(),
            self.writer.clone(),
            Arc::clone(&self.mark_dirty),
            Arc::clone(&self.snapshot_cache),
        )
        .await
    }
}
