use std::sync::Arc;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, ETag, GetOptions, Storage, StorageError,
};
use setsum::Setsum;
use tracing::Level;

use crate::interfaces::{FragmentManagerFactory, ManifestManagerFactory};
use crate::{
    fragment_path, parse_fragment_path, Error, Fragment, FragmentIdentifier, FragmentSeqNo,
    LogPosition, LogReaderOptions, LogWriterOptions, Manifest, MarkDirty, Snapshot, SnapshotCache,
    SnapshotPointer, ThrottleOptions,
};

pub mod batch_manager;
pub mod fragment_puller;
pub mod manifest_manager;
pub mod manifest_reader;

pub use batch_manager::{upload_parquet, BatchManager};
pub use fragment_puller::S3FragmentPuller;
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
    type Consumer = S3FragmentPuller;

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
        Ok(S3FragmentPuller::new(
            self.read.clone(),
            Arc::clone(&self.storage),
            self.prefix.clone(),
        ))
    }
}

#[derive(Clone)]
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

    async fn init_manifest(&self, manifest: &Manifest) -> Result<(), Error> {
        ManifestManager::initialize_from_manifest(
            &self.write,
            &self.storage,
            &self.prefix,
            manifest.clone(),
        )
        .await
    }

    async fn open_publisher(&self) -> Result<Self::Publisher, Error> {
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

/// Load the latest manifest from object storage.
pub async fn manifest_load(
    options: &ThrottleOptions,
    storage: &Storage,
    prefix: &str,
) -> Result<Option<(Manifest, ETag)>, Error> {
    let exp_backoff =
        crate::backoff::ExponentialBackoff::new(options.throughput as f64, options.headroom as f64);
    let mut retries = 0;
    let path = crate::manifest::manifest_path(prefix);
    loop {
        match storage
            .get_with_e_tag(
                &path,
                GetOptions::new(StorageRequestPriority::P0).with_strong_consistency(),
            )
            .await
            .map_err(Arc::new)
        {
            Ok((ref manifest, e_tag)) => {
                let Some(e_tag) = e_tag else {
                    return Err(Error::CorruptManifest(format!(
                        "no ETag for manifest at {}",
                        path
                    )));
                };
                let manifest: Manifest = serde_json::from_slice(manifest).map_err(|e| {
                    Error::CorruptManifest(format!("could not decode JSON manifest: {e:?}"))
                })?;
                return Ok(Some((manifest, e_tag)));
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

/// Reads a parquet fragment from storage and computes its setsum and records.
async fn read_parquet(
    storage: &Storage,
    prefix: &str,
    path: &str,
    starting_log_position: Option<LogPosition>,
) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64), Error> {
    let path = fragment_path(prefix, path);
    let parquet = storage
        .get(&path, GetOptions::new(StorageRequestPriority::P0))
        .await
        .map_err(Arc::new)?;
    let num_bytes = parquet.len() as u64;
    let (setsum, mut records, uses_relative_offsets) =
        super::checksum_parquet(&parquet, starting_log_position)?;
    match (starting_log_position, uses_relative_offsets) {
        (Some(starting_log_position), true) => {
            for record in records.iter_mut() {
                record.0 = LogPosition::from_offset(
                    starting_log_position
                        .offset()
                        .checked_add(record.0.offset())
                        .ok_or(Error::Overflow(format!(
                            "log position overflow: {} + {}",
                            starting_log_position.offset(),
                            record.0.offset()
                        )))?,
                );
            }
            Ok((setsum, records, num_bytes))
        }
        (None, false) => Ok((setsum, records, num_bytes)),
        (Some(_), false) => Err(Error::internal(file!(), line!())),
        (None, true) => Err(Error::internal(file!(), line!())),
    }
}

async fn read_fragment(
    storage: &Storage,
    prefix: &str,
    path: &str,
    starting_log_position: Option<LogPosition>,
) -> Result<Option<Fragment>, Error> {
    let seq_no = parse_fragment_path(path)
        .ok_or_else(|| Error::MissingFragmentSequenceNumber(path.to_string()))?;
    let FragmentIdentifier::SeqNo(_) = seq_no else {
        return Err(Error::internal(file!(), line!()));
    };
    let (setsum, data, num_bytes) =
        match read_parquet(storage, prefix, path, starting_log_position).await {
            Ok((setsum, data, num_bytes)) => (setsum, data, num_bytes),
            Err(Error::StorageError(storage)) => {
                if matches!(&*storage, StorageError::NotFound { .. }) {
                    return Ok(None);
                }
                return Err(Error::StorageError(storage));
            }
            Err(e) => return Err(e),
        };
    if data.is_empty() {
        return Err(Error::CorruptFragment(path.to_string()));
    }
    let start = LogPosition::from_offset(data.iter().map(|(p, _)| p.offset()).min().unwrap_or(0));
    let limit =
        LogPosition::from_offset(data.iter().map(|(p, _)| p.offset() + 1).max().unwrap_or(0));
    Ok(Some(Fragment {
        path: path.to_string(),
        seq_no,
        start,
        limit,
        num_bytes,
        setsum,
    }))
}
