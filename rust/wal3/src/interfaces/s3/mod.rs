use std::sync::Arc;

use tracing::Level;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, GetOptions, Storage, StorageError,
};

use crate::interfaces::{FragmentManagerFactory, ManifestManagerFactory};
use crate::{
    Error, FragmentSeqNo, LogPosition, LogReaderOptions, LogWriterOptions, MarkDirty, Snapshot,
    SnapshotCache, SnapshotPointer, ThrottleOptions,
};

pub mod batch_manager;
pub mod fragment_puller;
pub mod manifest_manager;
pub mod manifest_reader;

pub use batch_manager::{upload_parquet, BatchManager};
pub use fragment_puller::FragmentPuller;
pub use manifest_manager::ManifestManager;
pub use manifest_reader::ManifestReader;

/// Creates S3 fragment and manifest manager factories.
///
/// This helper encapsulates the common factory setup logic, reducing boilerplate
/// when opening logs.
pub fn create_factories(
    write: LogWriterOptions,
    read: LogReaderOptions,
    storage: Arc<Storage>,
    prefix: String,
    writer: String,
    mark_dirty: Arc<dyn MarkDirty>,
    snapshot_cache: Arc<dyn SnapshotCache>,
) -> (S3FragmentManagerFactory, S3ManifestManagerFactory) {
    let fragment_manager_factory = S3FragmentManagerFactory {
        write: write.clone(),
        read: read.clone(),
        storage: Arc::clone(&storage),
        prefix: prefix.clone(),
        mark_dirty: Arc::clone(&mark_dirty),
    };
    let manifest_manager_factory = S3ManifestManagerFactory {
        write,
        read,
        storage,
        prefix,
        writer,
        mark_dirty,
        snapshot_cache,
    };
    (fragment_manager_factory, manifest_manager_factory)
}

pub struct S3FragmentManagerFactory {
    pub write: LogWriterOptions,
    pub read: LogReaderOptions,
    pub storage: Arc<Storage>,
    pub prefix: String,
    pub mark_dirty: Arc<dyn MarkDirty>,
}

#[async_trait::async_trait]
impl FragmentManagerFactory for S3FragmentManagerFactory {
    type FragmentPointer = (FragmentSeqNo, LogPosition);
    type Publisher = BatchManager;
    type Consumer = FragmentPuller;

    async fn make_publisher(&self) -> Result<Self::Publisher, Error> {
        BatchManager::new(
            self.write.clone(),
            Arc::clone(&self.storage),
            self.prefix.clone(),
            Arc::clone(&self.mark_dirty),
        )
        .ok_or_else(|| Error::internal(file!(), line!()))
    }

    async fn make_consumer(&self) -> Result<Self::Consumer, Error> {
        Ok(FragmentPuller::new(
            self.read.clone(),
            Arc::clone(&self.storage),
            self.prefix.clone(),
        ))
    }
}

pub struct S3ManifestManagerFactory {
    pub write: LogWriterOptions,
    pub read: LogReaderOptions,
    pub storage: Arc<Storage>,
    pub prefix: String,
    pub writer: String,
    pub mark_dirty: Arc<dyn MarkDirty>,
    pub snapshot_cache: Arc<dyn SnapshotCache>,
}

#[async_trait::async_trait]
impl ManifestManagerFactory for S3ManifestManagerFactory {
    type FragmentPointer = (FragmentSeqNo, LogPosition);
    type Publisher = ManifestManager;
    type Consumer = ManifestReader;

    async fn make_publisher(&self) -> Result<Self::Publisher, Error> {
        ManifestManager::new(
            self.write.throttle_manifest,
            self.write.snapshot_manifest,
            Arc::clone(&self.storage),
            self.prefix.clone(),
            self.writer.clone(),
            Arc::clone(&self.mark_dirty),
            Arc::clone(&self.snapshot_cache),
        )
        .await
    }

    async fn make_consumer(&self) -> Result<Self::Consumer, Error> {
        Ok(ManifestReader::new(
            self.read.clone(),
            Arc::clone(&self.storage),
            self.prefix.clone(),
            Arc::clone(&self.snapshot_cache),
        ))
    }
}

async fn snapshot_load(
    throttle: ThrottleOptions,
    storage: &Storage,
    prefix: &str,
    snapshot_cache: &dyn SnapshotCache,
    pointer: &SnapshotPointer,
) -> Result<Option<Snapshot>, Error> {
    match snapshot_cache.get(pointer).await {
        Ok(Some(snapshot)) => return Ok(Some(snapshot)),
        Ok(None) => {
            // pass
        }
        Err(err) => {
            tracing::event!(Level::ERROR, name = "cache error", error =? err);
        }
    };
    if let Some(res) = uncached_snapshot_load(throttle, storage, prefix, pointer).await? {
        Ok(Some(res))
    } else {
        Ok(None)
    }
}

async fn uncached_snapshot_load(
    throttle: ThrottleOptions,
    storage: &Storage,
    prefix: &str,
    pointer: &SnapshotPointer,
) -> Result<Option<Snapshot>, Error> {
    let exp_backoff = crate::backoff::ExponentialBackoff::new(
        throttle.throughput as f64,
        throttle.headroom as f64,
    );
    let mut retries = 0;
    let path = format!("{}/{}", prefix, pointer.path_to_snapshot);
    loop {
        match storage
            .get_with_e_tag(&path, GetOptions::new(StorageRequestPriority::P0))
            .await
            .map_err(Arc::new)
        {
            Ok((ref snapshot, _)) => {
                let snapshot: Snapshot = serde_json::from_slice(snapshot).map_err(|e| {
                    Error::CorruptManifest(format!("could not decode JSON snapshot: {e:?}"))
                })?;
                return Ok(Some(snapshot));
            }
            Err(err) => match &*err {
                StorageError::NotFound { path: _, source: _ } => return Ok(None),
                err => {
                    let backoff = exp_backoff.next();
                    tokio::time::sleep(backoff).await;
                    if retries >= 3 {
                        return Err(Error::StorageError(Arc::new(err.clone())));
                    }
                    retries += 1;
                }
            },
        }
    }
}
