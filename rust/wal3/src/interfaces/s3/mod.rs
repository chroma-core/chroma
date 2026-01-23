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

pub mod fragment_puller;
pub mod fragment_uploader;
pub mod manifest_manager;
pub mod manifest_reader;

pub use super::batch_manager::{upload_parquet, BatchManager};
pub use fragment_puller::S3FragmentPuller;
pub use fragment_uploader::S3FragmentUploader;
pub use manifest_manager::ManifestManager;
pub use manifest_reader::ManifestReader;

/// Creates S3 fragment and manifest manager factories.
///
/// This helper encapsulates the common factory setup logic, reducing boilerplate
/// when opening logs.
pub fn create_s3_factories(
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
        storage: Storage::clone(&*storage),
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
    pub storage: Storage,
    pub prefix: String,
    pub mark_dirty: Arc<dyn MarkDirty>,
}

#[async_trait::async_trait]
impl FragmentManagerFactory for S3FragmentManagerFactory {
    type FragmentPointer = (FragmentSeqNo, LogPosition);
    type Publisher = BatchManager<Self::FragmentPointer, S3FragmentUploader>;
    type Consumer = S3FragmentPuller;

    async fn preferred_storage(&self) -> Storage {
        self.storage.clone()
    }

    async fn make_publisher(&self) -> Result<Self::Publisher, Error> {
        let fragment_uploader = S3FragmentUploader::new(
            self.write.clone(),
            self.storage.clone(),
            self.prefix.clone(),
            Arc::clone(&self.mark_dirty),
        );
        BatchManager::new(self.write.clone(), fragment_uploader)
            .ok_or_else(|| Error::internal(file!(), line!()))
    }

    async fn make_consumer(&self) -> Result<Self::Consumer, Error> {
        Ok(S3FragmentPuller::new(
            self.read.clone(),
            Arc::new(self.storage.clone()),
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
pub async fn read_parquet(
    storage: &Storage,
    prefix: &str,
    path: &str,
    starting_log_position: Option<LogPosition>,
) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64, u64), Error> {
    let bytes = read_bytes(storage, prefix, path).await?.ok_or_else(|| {
        Arc::new(StorageError::NotFound {
            path: path.into(),
            source: Arc::new(std::io::Error::other("file not found")),
        })
    })?;
    parse_parquet(&bytes, starting_log_position).await
}

/// Reads a parquet fragment from storage and computes its setsum and records.
pub async fn read_bytes(
    storage: &Storage,
    prefix: &str,
    path: &str,
) -> Result<Option<Arc<Vec<u8>>>, Error> {
    let path = fragment_path(prefix, path);
    match storage
        .get(&path, GetOptions::new(StorageRequestPriority::P0))
        .await
        .map_err(Arc::new)
    {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) => {
            if matches!(&*err, StorageError::NotFound { .. }) {
                Ok(None)
            } else {
                Err(err.into())
            }
        }
    }
}

/// Reads a parquet fragment from storage and computes its setsum and records.
pub async fn parse_parquet(
    parquet: &[u8],
    starting_log_position: Option<LogPosition>,
) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64, u64), Error> {
    let num_bytes = parquet.len() as u64;
    let (setsum, records, uses_relative_offsets, now_micros) =
        super::checksum_parquet(parquet, starting_log_position)?;
    match (starting_log_position, uses_relative_offsets) {
        (Some(_), true) => Ok((setsum, records, num_bytes, now_micros)),
        (Some(_), false) => Err(Error::internal(file!(), line!())),
        (None, false) => Ok((setsum, records, num_bytes, now_micros)),
        (None, true) => Err(Error::internal(file!(), line!())),
    }
}

pub async fn read_fragment(
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
    let Some(bytes) = read_bytes(storage, prefix, path).await? else {
        return Ok(None);
    };
    let (setsum, data, num_bytes) = match parse_parquet(&bytes, starting_log_position).await {
        Ok((setsum, data, num_bytes, _ts)) => (setsum, data, num_bytes),
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
