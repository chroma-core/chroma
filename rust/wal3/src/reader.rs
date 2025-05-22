// NOTE(rescrv):  All caches align to storage.  For now, implement without caching.  Caching
// should/could literally be a layer over storage, so add it later once correctness without caching
// is ensured by adequate testing.

use std::sync::Arc;

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use setsum::Setsum;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, GetOptions, Storage, StorageError,
};

use crate::{
    parse_fragment_path, Error, Fragment, LogPosition, LogReaderOptions, Manifest, ScrubError,
    ScrubSuccess, Snapshot,
};

/// Limits allows encoding things like offset, timestamp, and byte size limits for the read.
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Limits {
    pub max_files: Option<u64>,
    pub max_bytes: Option<u64>,
}

/// LogReader is a reader for the log.
pub struct LogReader {
    options: LogReaderOptions,
    storage: Arc<Storage>,
    pub(crate) prefix: String,
}

impl LogReader {
    pub fn new(options: LogReaderOptions, storage: Arc<Storage>, prefix: String) -> Self {
        Self {
            options,
            storage,
            prefix,
        }
    }

    pub async fn open(
        options: LogReaderOptions,
        storage: Arc<Storage>,
        prefix: String,
    ) -> Result<Self, Error> {
        Ok(Self {
            options,
            storage,
            prefix,
        })
    }

    pub async fn manifest(&self) -> Result<Option<Manifest>, Error> {
        Ok(
            Manifest::load(&self.options.throttle, &self.storage, &self.prefix)
                .await?
                .map(|(m, _)| m),
        )
    }

    pub async fn maximum_log_position(&self) -> Result<LogPosition, Error> {
        let Some((manifest, _)) =
            Manifest::load(&self.options.throttle, &self.storage, &self.prefix).await?
        else {
            return Err(Error::UninitializedLog);
        };
        Ok(manifest.maximum_log_position())
    }

    pub async fn minimum_log_position(&self) -> Result<LogPosition, Error> {
        let Some((manifest, _)) =
            Manifest::load(&self.options.throttle, &self.storage, &self.prefix).await?
        else {
            return Err(Error::UninitializedLog);
        };
        Ok(manifest.minimum_log_position())
    }

    /// Scan up to:
    /// 1. Up to, but not including, the offset of the log position.  This makes it a half-open
    ///    interval.
    /// 2. Up to, and including, the number of files to return.
    /// 3. Up to, and including, the total number of bytes to return.
    #[tracing::instrument(skip(self))]
    pub async fn scan(&self, from: LogPosition, limits: Limits) -> Result<Vec<Fragment>, Error> {
        let Some((manifest, _)) =
            Manifest::load(&self.options.throttle, &self.storage, &self.prefix).await?
        else {
            return Err(Error::UninitializedLog);
        };
        let mut snapshots = manifest
            .snapshots
            .iter()
            .filter(|s| s.limit.offset() > from.offset())
            .cloned()
            .collect::<Vec<_>>();
        let mut fragments = manifest
            .fragments
            .iter()
            .filter(|f| f.limit.offset() > from.offset())
            .cloned()
            .collect::<Vec<_>>();
        while !snapshots.is_empty() {
            // In parallel resolve this level of the tree.
            let futures = snapshots
                .iter()
                .map(|s| {
                    let options = self.options.clone();
                    let storage = Arc::clone(&self.storage);
                    async move { Snapshot::load(&options.throttle, &storage, &self.prefix, s).await }
                })
                .collect::<Vec<_>>();
            let resolved = futures::future::try_join_all(futures).await?;
            // NOTE(rescrv):  This empties snapshots before the first loop so we can fill it
            // incrementally as we find snapshots that reference snapshots.
            for (r, s) in
                std::iter::zip(resolved.iter(), std::mem::take(&mut snapshots).into_iter())
            {
                if let Some(r) = r {
                    snapshots.extend(r.snapshots.iter().cloned());
                    fragments.extend(r.fragments.iter().cloned());
                } else {
                    return Err(Error::CorruptManifest(format!(
                        "snapshot {} is missing",
                        s.path_to_snapshot
                    )));
                }
            }
        }
        fragments.retain(|f| f.limit > from);
        fragments.sort_by_key(|f| f.start.offset());
        if let Some(max_files) = limits.max_files {
            if fragments.len() as u64 > max_files {
                tracing::info!("truncating to {} files from {}", max_files, fragments.len());
                fragments.truncate(max_files as usize);
            }
        }
        while fragments.len() > 1
            && fragments
                .iter()
                .map(|f| f.num_bytes)
                .fold(0, u64::saturating_add)
                > limits.max_bytes.unwrap_or(u64::MAX)
        {
            tracing::info!(
                "truncating to {} files because bytes restrictions",
                fragments.len() - 1
            );
            fragments.pop();
        }
        Ok(fragments)
    }

    #[tracing::instrument(skip(self))]
    pub async fn fetch(&self, fragment: &Fragment) -> Result<Arc<Vec<u8>>, Error> {
        let path = format!("{}/{}", self.prefix, fragment.path);
        Ok(self
            .storage
            .get_with_e_tag(&path, GetOptions::new(StorageRequestPriority::P0))
            .await
            .map_err(Arc::new)?
            .0)
    }

    #[tracing::instrument(skip(self))]
    #[allow(clippy::type_complexity)]
    pub async fn read_parquet(
        &self,
        fragment: &Fragment,
    ) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64), Error> {
        read_parquet(&self.storage, &self.prefix, &fragment.path).await
    }

    #[tracing::instrument(skip(self), ret)]
    pub async fn scrub(&self) -> Result<ScrubSuccess, Error> {
        let Some((manifest, _)) =
            Manifest::load(&self.options.throttle, &self.storage, &self.prefix).await?
        else {
            return Err(Error::UninitializedLog);
        };
        let manifest_scrub_success = manifest.scrub()?;
        let mut calculated_setsum = Setsum::default();
        let mut bytes_read = 0u64;
        for reference in manifest.snapshots.iter() {
            if let Some(empirical) = Snapshot::load(
                &self.options.throttle,
                &self.storage,
                &self.prefix,
                reference,
            )
            .await?
            {
                let empirical_scrub_success = empirical.scrub()?;
                if empirical_scrub_success.calculated_setsum != reference.setsum
                    || empirical_scrub_success.calculated_setsum != empirical.setsum
                {
                    return Err(Error::ScrubError(
                        ScrubError::MismatchedSnapshotSetsum {
                            reference: reference.clone(),
                            empirical,
                        }
                        .into(),
                    ));
                }
                calculated_setsum += empirical_scrub_success.calculated_setsum;
                bytes_read += empirical_scrub_success.bytes_read;
            } else {
                return Err(Error::ScrubError(
                    ScrubError::MissingSnapshot {
                        reference: reference.clone(),
                    }
                    .into(),
                ));
            }
        }
        for reference in manifest.fragments.iter() {
            if let Some(empirical) =
                read_fragment(&self.storage, &self.prefix, &reference.path).await?
            {
                calculated_setsum += empirical.setsum;
                bytes_read += empirical.num_bytes;
                if reference.path != empirical.path {
                    return Err(Error::ScrubError(
                        ScrubError::MismatchedPath {
                            reference: reference.clone(),
                            empirical,
                        }
                        .into(),
                    ));
                }
                if reference.seq_no != empirical.seq_no {
                    return Err(Error::ScrubError(
                        ScrubError::MismatchedSeqNo {
                            reference: reference.clone(),
                            empirical,
                        }
                        .into(),
                    ));
                }
                if reference.num_bytes != empirical.num_bytes {
                    return Err(Error::ScrubError(
                        ScrubError::MismatchedNumBytes {
                            reference: reference.clone(),
                            empirical,
                        }
                        .into(),
                    ));
                }
                if reference.start != empirical.start {
                    return Err(Error::ScrubError(
                        ScrubError::MismatchedStart {
                            reference: reference.clone(),
                            empirical,
                        }
                        .into(),
                    ));
                }
                if reference.limit != empirical.limit {
                    return Err(Error::ScrubError(
                        ScrubError::MismatchedLimit {
                            reference: reference.clone(),
                            empirical,
                        }
                        .into(),
                    ));
                }
                if reference.setsum != empirical.setsum {
                    return Err(Error::ScrubError(
                        ScrubError::MismatchedFragmentSetsum {
                            reference: reference.clone(),
                            empirical,
                        }
                        .into(),
                    ));
                }
            } else {
                return Err(Error::ScrubError(
                    ScrubError::MissingFragment {
                        reference: reference.clone(),
                    }
                    .into(),
                ));
            }
        }
        let observed_scrub_success = ScrubSuccess {
            calculated_setsum,
            bytes_read,
        };
        if manifest_scrub_success != observed_scrub_success {
            return Err(Error::ScrubError(
                ScrubError::OverallMismatch {
                    manifest: manifest_scrub_success,
                    observed: observed_scrub_success,
                }
                .into(),
            ));
        }
        Ok(observed_scrub_success)
    }
}

pub async fn read_parquet(
    storage: &Storage,
    prefix: &str,
    path: &str,
) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64), Error> {
    let path = format!("{prefix}/{path}");
    let parquet = storage
        .get(&path, GetOptions::new(StorageRequestPriority::P0))
        .await
        .map_err(Arc::new)?;
    let num_bytes = parquet.len() as u64;
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_owner(parquet.to_vec()))
        .map_err(Arc::new)?;
    let reader = builder.build().map_err(Arc::new)?;
    let mut setsum = Setsum::default();
    let mut records = vec![];
    for batch in reader {
        let batch = batch.map_err(|_| Error::CorruptFragment(path.to_string()))?;
        let offset = batch
            .column_by_name("offset")
            .ok_or_else(|| Error::CorruptFragment(path.to_string()))?;
        let epoch_micros = batch
            .column_by_name("timestamp_us")
            .ok_or_else(|| Error::CorruptFragment(path.to_string()))?;
        let body = batch
            .column_by_name("body")
            .ok_or_else(|| Error::CorruptFragment(path.to_string()))?;
        let offset = offset
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| Error::CorruptFragment(path.to_string()))?;
        let epoch_micros = epoch_micros
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| Error::CorruptFragment(path.to_string()))?;
        let body = body
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .ok_or_else(|| Error::CorruptFragment(path.to_string()))?;
        for i in 0..batch.num_rows() {
            let offset = offset.value(i);
            let epoch_micros = epoch_micros.value(i);
            let body = body.value(i);
            setsum.insert_vectored(&[&offset.to_be_bytes(), &epoch_micros.to_be_bytes(), body]);
            records.push((LogPosition::from_offset(offset), body.to_vec()));
        }
    }
    Ok((setsum, records, num_bytes))
}

pub async fn read_fragment(
    storage: &Storage,
    prefix: &str,
    path: &str,
) -> Result<Option<Fragment>, Error> {
    let seq_no = parse_fragment_path(path)
        .ok_or_else(|| Error::MissingFragmentSequenceNumber(path.to_string()))?;
    let (setsum, data, num_bytes) = match read_parquet(storage, prefix, path).await {
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
