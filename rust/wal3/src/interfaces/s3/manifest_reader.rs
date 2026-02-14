use std::sync::Arc;

use chroma_storage::ETag;
use chroma_storage::Storage;

use crate::interfaces::s3::ManifestManager;
use crate::interfaces::{ManifestConsumer, ManifestWitness};
use crate::Error;
use crate::FragmentSeqNo;
use crate::LogPosition;
use crate::LogReaderOptions;
use crate::Manifest;
use crate::Snapshot;
use crate::SnapshotCache;
use crate::SnapshotPointer;
use crate::ThrottleOptions;

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

    /// Load the latest manifest from object storage.
    pub async fn load(
        options: &ThrottleOptions,
        storage: &Storage,
        prefix: &str,
    ) -> Result<Option<(Manifest, ETag)>, Error> {
        super::manifest_load(options, storage, prefix).await
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

    async fn manifest_head(&self, witness: &ManifestWitness) -> Result<bool, Error> {
        let ManifestWitness::ETag(e_tag) = witness else {
            return Err(Error::internal(file!(), line!()));
        };
        ManifestManager::head(&self.options.throttle, &self.storage, &self.prefix, e_tag).await
    }

    async fn manifest_load(&self) -> Result<Option<(Manifest, ManifestWitness)>, Error> {
        match ManifestManager::load(&self.options.throttle, &self.storage, &self.prefix).await {
            Ok(Some((manifest, e_tag))) => Ok(Some((manifest, ManifestWitness::ETag(e_tag)))),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }
}
