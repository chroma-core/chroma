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
    parse_fragment_path, Error, Fragment, LogPosition, LogReaderOptions, Manifest, ManifestAndETag,
    ScrubError, ScrubSuccess, Snapshot, SnapshotCache,
};

fn ranges_overlap(lhs: (LogPosition, LogPosition), rhs: (LogPosition, LogPosition)) -> bool {
    lhs.0 < rhs.1 && rhs.0 < lhs.1
}

/// Limits allows encoding things like offset, timestamp, and byte size limits for the read.
#[derive(Copy, Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Limits {
    pub max_files: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_records: Option<u64>,
}

impl Limits {
    pub const UNLIMITED: Limits = Limits {
        max_files: None,
        max_bytes: None,
        max_records: None,
    };
}

/// LogReader is a reader for the log.
pub struct LogReader {
    options: LogReaderOptions,
    storage: Arc<Storage>,
    cache: Option<Arc<dyn SnapshotCache>>,
    pub(crate) prefix: String,
}

impl LogReader {
    pub fn new(options: LogReaderOptions, storage: Arc<Storage>, prefix: String) -> Self {
        let cache = None;
        Self {
            options,
            storage,
            cache,
            prefix,
        }
    }

    pub async fn open(
        options: LogReaderOptions,
        storage: Arc<Storage>,
        prefix: String,
    ) -> Result<Self, Error> {
        let cache = None;
        Ok(Self {
            options,
            storage,
            cache,
            prefix,
        })
    }

    pub fn with_cache(&mut self, cache: Arc<dyn SnapshotCache>) {
        self.cache = Some(cache);
    }

    /// Verify that the reader would read the same manifest as the one provided in
    /// manifest_and_etag, but do it in a way that doesn't load the whole manifest.
    pub async fn verify(&self, manifest_and_etag: &ManifestAndETag) -> Result<bool, Error> {
        Manifest::head(
            &self.options.throttle,
            &self.storage,
            &self.prefix,
            &manifest_and_etag.e_tag,
        )
        .await
    }

    pub async fn manifest(&self) -> Result<Option<Manifest>, Error> {
        Ok(
            Manifest::load(&self.options.throttle, &self.storage, &self.prefix)
                .await?
                .map(|(m, _)| m),
        )
    }

    pub async fn manifest_and_e_tag(&self) -> Result<Option<ManifestAndETag>, Error> {
        match Manifest::load(&self.options.throttle, &self.storage, &self.prefix).await {
            Ok(Some((manifest, e_tag))) => Ok(Some(ManifestAndETag { manifest, e_tag })),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn oldest_timestamp(&self) -> Result<LogPosition, Error> {
        let Some((manifest, _)) =
            Manifest::load(&self.options.throttle, &self.storage, &self.prefix).await?
        else {
            return Err(Error::UninitializedLog);
        };
        Ok(manifest.oldest_timestamp())
    }

    pub async fn next_write_timestamp(&self) -> Result<LogPosition, Error> {
        let Some((manifest, _)) =
            Manifest::load(&self.options.throttle, &self.storage, &self.prefix).await?
        else {
            return Err(Error::UninitializedLog);
        };
        Ok(manifest.next_write_timestamp())
    }

    /// Scan up to:
    /// 1. Up to, but not including, the offset of the log position.  This makes it a half-open
    ///    interval.
    /// 2. Up to, and including, the number of files to return.
    /// 3. Up to, and including, the total number of bytes to return.
    pub async fn scan(&self, from: LogPosition, limits: Limits) -> Result<Vec<Fragment>, Error> {
        let Some((manifest, _)) =
            Manifest::load(&self.options.throttle, &self.storage, &self.prefix).await?
        else {
            return Err(Error::UninitializedLog);
        };
        let mut short_read = false;
        self.scan_with_cache(&manifest, from, limits, &mut short_read)
            .await
    }

    /// Scan up to:
    /// 1. Up to, but not including, the offset of the log position.  This makes it a half-open
    ///    interval.
    /// 2. Up to, and including, the number of files to return.
    ///
    /// This differs from scan in that it takes a loaded manifest.
    /// This differs from scan_from_manifest because it will load snapshots.
    pub async fn scan_with_cache(
        &self,
        manifest: &Manifest,
        from: LogPosition,
        limits: Limits,
        short_read: &mut bool,
    ) -> Result<Vec<Fragment>, Error> {
        let log_position_range = if let Some(max_records) = limits.max_records {
            (from, from + max_records)
        } else {
            (from, LogPosition::MAX)
        };
        let mut snapshots = manifest
            .snapshots
            .iter()
            .filter(|s| ranges_overlap(log_position_range, (s.start, s.limit)))
            .cloned()
            .collect::<Vec<_>>();
        let mut fragments = manifest
            .fragments
            .iter()
            .filter(|f| ranges_overlap(log_position_range, (f.start, f.limit)))
            .cloned()
            .collect::<Vec<_>>();
        while !snapshots.is_empty() {
            // In parallel resolve this level of the tree.
            let futures = snapshots
                .iter()
                .map(|s| {
                    let options = self.options.clone();
                    let storage = Arc::clone(&self.storage);
                    let cache = self.cache.as_ref().map(Arc::clone);
                    async move {
                        if let Some(cache) = cache {
                            if let Some(snapshot) = cache.get(s).await? {
                                return Ok(Some(snapshot));
                            }
                            let snap = Snapshot::load(&options.throttle, &storage, &self.prefix, s)
                                .await?;
                            if let Some(snap) = snap.as_ref() {
                                cache.put(s, snap).await?;
                            }
                            Ok(snap)
                        } else {
                            Snapshot::load(&options.throttle, &storage, &self.prefix, s).await
                        }
                    }
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
        Ok(Self::post_process_fragments(
            fragments, from, limits, short_read,
        ))
    }

    /// Do a consistent stale read of the manifest.  If the read can be returned without I/O,
    /// return Some(Vec<Fragment>).  If the read would require reading from the future or
    /// snapshots, return None.  Scan is more appropriate for that.
    ///
    /// 1. Up to, but not including, the offset of the log position.  This makes it a half-open
    ///    interval.
    /// 2. Up to, and including, the number of files to return.
    /// 3. Up to, and including, the total number of bytes to return.
    pub fn scan_from_manifest(
        manifest: &Manifest,
        from: LogPosition,
        limits: Limits,
    ) -> Option<Vec<Fragment>> {
        let log_position_range = if let Some(max_records) = limits.max_records {
            if from.offset().saturating_add(max_records) == u64::MAX {
                return None;
            }
            (from, from + max_records)
        } else {
            (from, LogPosition::MAX)
        };
        // If no there is no fragment with a start earlier than the from LogPosition, that means
        // we'd need to load snapshots.  Since this is an in-memory only function, we return "None"
        // to indicate that it's not satisfiable and do no I/O.
        if !manifest
            .fragments
            .iter()
            .any(|f| f.start <= log_position_range.0)
        {
            return None;
        }
        // If no there is no fragment with a limit later than the upper-bound LogPosition, that
        // means we have a stale manifest.  Since this is an in-memory only function, we return
        // "None" to indicate that it's not satisfiable and do no I/O.
        if !manifest
            .fragments
            .iter()
            .any(|f| f.limit > log_position_range.1)
        {
            return None;
        }
        let fragments = manifest
            .fragments
            .iter()
            .filter(|f| ranges_overlap(log_position_range, (f.start, f.limit)))
            .cloned()
            .collect::<Vec<_>>();
        let mut short_read = false;
        Some(Self::post_process_fragments(
            fragments,
            from,
            limits,
            &mut short_read,
        ))
    }

    // Post process the fragments such that only records starting at from and not exceeding limits
    // will be processed.  Sets *short_read=true when the limits truncate the log.
    fn post_process_fragments(
        mut fragments: Vec<Fragment>,
        from: LogPosition,
        limits: Limits,
        short_read: &mut bool,
    ) -> Vec<Fragment> {
        fragments.sort_by_key(|f| f.start.offset());
        if let Some(max_files) = limits.max_files {
            if fragments.len() as u64 > max_files {
                *short_read = true;
                fragments.truncate(max_files as usize);
            }
        }
        while fragments.len() > 1
            // NOTE(rescrv):  We take the start of the last fragment, because if there are enough
            // records without it we can pop.
            && fragments[fragments.len() - 1].start - from
                > limits.max_records.unwrap_or(u64::MAX)
        {
            fragments.pop();
            *short_read = true;
        }
        while fragments.len() > 1
            && fragments
                .iter()
                .map(|f| f.num_bytes)
                .fold(0, u64::saturating_add)
                > limits.max_bytes.unwrap_or(u64::MAX)
        {
            fragments.pop();
            *short_read = true;
        }
        fragments
    }

    #[tracing::instrument(skip(self))]
    pub async fn fetch(&self, fragment: &Fragment) -> Result<Arc<Vec<u8>>, Error> {
        Self::stateless_fetch(&self.storage, &self.prefix, fragment).await
    }

    /// A class method to fetch data (no state from an instantiated log reader)
    #[tracing::instrument]
    pub async fn stateless_fetch(
        storage: &Storage,
        prefix: &str,
        fragment: &Fragment,
    ) -> Result<Arc<Vec<u8>>, Error> {
        let path = fragment_path(prefix, &fragment.path);
        Ok(storage
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
    pub async fn scrub(&self, limits: Limits) -> Result<ScrubSuccess, Vec<Error>> {
        let Some((manifest, _)) =
            Manifest::load(&self.options.throttle, &self.storage, &self.prefix)
                .await
                .map_err(|x| vec![x])?
        else {
            return Err(vec![Error::UninitializedLog]);
        };
        let manifest_scrub_success = manifest.scrub().map_err(|x| vec![x.into()])?;
        let from = manifest.oldest_timestamp();
        let mut short_read = false;
        let fragments = self
            .scan_with_cache(&manifest, from, limits, &mut short_read)
            .await
            .map_err(|x| vec![x])?;
        let futures = fragments
            .iter()
            .map(|reference| async {
                if let Some(empirical) = read_fragment(&self.storage, &self.prefix, &reference.path)
                    .await
                    .map_err(|x| vec![x])?
                {
                    if reference.path != empirical.path {
                        return Err(vec![Error::ScrubError(
                            ScrubError::MismatchedPath {
                                reference: reference.clone(),
                                empirical,
                            }
                            .into(),
                        )]);
                    }
                    if reference.seq_no != empirical.seq_no {
                        return Err(vec![Error::ScrubError(
                            ScrubError::MismatchedSeqNo {
                                reference: reference.clone(),
                                empirical,
                            }
                            .into(),
                        )]);
                    }
                    if reference.num_bytes != empirical.num_bytes {
                        return Err(vec![Error::ScrubError(
                            ScrubError::MismatchedNumBytes {
                                reference: reference.clone(),
                                empirical,
                            }
                            .into(),
                        )]);
                    }
                    if reference.start != empirical.start {
                        return Err(vec![Error::ScrubError(
                            ScrubError::MismatchedStart {
                                reference: reference.clone(),
                                empirical,
                            }
                            .into(),
                        )]);
                    }
                    if reference.limit != empirical.limit {
                        return Err(vec![Error::ScrubError(
                            ScrubError::MismatchedLimit {
                                reference: reference.clone(),
                                empirical,
                            }
                            .into(),
                        )]);
                    }
                    if reference.setsum != empirical.setsum {
                        return Err(vec![Error::ScrubError(
                            ScrubError::MismatchedFragmentSetsum {
                                reference: reference.clone(),
                                empirical,
                            }
                            .into(),
                        )]);
                    }
                    Ok(reference.clone())
                } else {
                    Err(vec![Error::ScrubError(
                        ScrubError::MissingFragment {
                            reference: reference.clone(),
                        }
                        .into(),
                    )])
                }
            })
            .collect::<Vec<_>>();
        if futures.is_empty() {
            return Ok(ScrubSuccess {
                calculated_setsum: manifest_scrub_success.calculated_setsum,
                bytes_read: 0,
                short_read: manifest_scrub_success.short_read,
            });
        }
        let mut calculated_setsum = Setsum::default();
        let mut bytes_read = 0u64;
        let mut errors = vec![];
        for result in futures::future::join_all(futures).await {
            match result {
                Ok(frag) => {
                    calculated_setsum += frag.setsum;
                    bytes_read += frag.num_bytes;
                }
                Err(errs) => errors.extend(errs),
            }
        }
        let observed_scrub_success = ScrubSuccess {
            calculated_setsum,
            bytes_read,
            short_read,
        };
        if !short_read && manifest_scrub_success != observed_scrub_success {
            let mut ret = vec![Error::ScrubError(
                ScrubError::OverallMismatch {
                    manifest: manifest_scrub_success,
                    observed: observed_scrub_success,
                }
                .into(),
            )];
            ret.extend(errors);
            Err(ret)
        } else if short_read {
            Err(vec![Error::Success])
        } else {
            Ok(observed_scrub_success)
        }
    }
}

pub fn fragment_path(prefix: &str, path: &str) -> String {
    format!("{prefix}/{path}")
}

pub async fn read_parquet(
    storage: &Storage,
    prefix: &str,
    path: &str,
) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64), Error> {
    let path = fragment_path(prefix, path);
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

#[cfg(test)]
mod tests {
    use setsum::Setsum;

    use crate::{Fragment, FragmentSeqNo};

    use super::*;

    #[test]
    fn post_process_fragments_uses_from_position_for_record_limits() {
        let fragments = vec![
            Fragment {
                path: "fragment1".to_string(),
                seq_no: FragmentSeqNo(1),
                start: LogPosition::from_offset(100),
                limit: LogPosition::from_offset(150),
                num_bytes: 1000,
                setsum: Setsum::default(),
            },
            Fragment {
                path: "fragment2".to_string(),
                seq_no: FragmentSeqNo(2),
                start: LogPosition::from_offset(150),
                limit: LogPosition::from_offset(200),
                num_bytes: 1000,
                setsum: Setsum::default(),
            },
            Fragment {
                path: "fragment3".to_string(),
                seq_no: FragmentSeqNo(3),
                start: LogPosition::from_offset(200),
                limit: LogPosition::from_offset(250),
                num_bytes: 1000,
                setsum: Setsum::default(),
            },
        ];

        // Test case: from position is later than the first fragment's start
        // This tests the bug fix where we use 'from' instead of fragments[0].start
        let from = LogPosition::from_offset(125);
        let limits = Limits {
            max_files: None,
            max_bytes: None,
            max_records: Some(100), // Set a limit that should trigger the record check
        };

        let mut short_read = false;
        let result =
            LogReader::post_process_fragments(fragments.clone(), from, limits, &mut short_read);

        // With the fix: last fragment start (200) - from (125) = 75 records
        // This should be under the 100 record limit, so all fragments should remain
        assert_eq!(result.len(), 3);
        assert!(!short_read);

        // Test case that would fail with the old bug:
        // If we were using fragments[0].start (100) instead of from (125),
        // then: last fragment start (200) - fragments[0].start (100) = 100 records
        // This would equal the limit, but the actual span from 'from' is only 75
        let limits_strict = Limits {
            max_files: None,
            max_bytes: None,
            max_records: Some(74), // Just under the actual span from 'from'
        };

        let mut short_read = false;
        let result_strict = LogReader::post_process_fragments(
            fragments.clone(),
            from,
            limits_strict,
            &mut short_read,
        );

        // With the fix: 200 - 125 = 75 > 74, so last fragment should be removed
        assert_eq!(result_strict.len(), 2);
        assert_eq!(result_strict[0].seq_no, FragmentSeqNo(1));
        assert_eq!(result_strict[1].seq_no, FragmentSeqNo(2));
        assert!(short_read);
    }

    #[test]
    fn records_based_pruning_bug_from_commit_message() {
        // Test the exact scenario from the commit message:
        // - Fragments with LogPosition ranges [1, 101), [101, 201).
        // - Query for 75 records at offset 50 should fetch both fragments.
        // - Prior to this change only the first fragment was fetched.

        let fragments = vec![
            Fragment {
                path: "fragment1".to_string(),
                seq_no: FragmentSeqNo(1),
                start: LogPosition::from_offset(1),
                limit: LogPosition::from_offset(101),
                num_bytes: 1000,
                setsum: Setsum::default(),
            },
            Fragment {
                path: "fragment2".to_string(),
                seq_no: FragmentSeqNo(2),
                start: LogPosition::from_offset(101),
                limit: LogPosition::from_offset(201),
                num_bytes: 1000,
                setsum: Setsum::default(),
            },
        ];

        // Query for 75 records at offset 50
        let from = LogPosition::from_offset(50);
        let limits = Limits {
            max_files: None,
            max_bytes: None,
            max_records: Some(75),
        };

        let mut short_read = false;
        let result =
            LogReader::post_process_fragments(fragments.clone(), from, limits, &mut short_read);

        // With the fix: both fragments should be returned
        // The calculation is: last fragment start (101) - from (50) = 51 records
        // Since 51 < 75, both fragments should remain
        assert_eq!(
            result.len(),
            2,
            "Both fragments should be returned for 75 records from offset 50"
        );
        assert_eq!(result[0].seq_no, FragmentSeqNo(1));
        assert_eq!(result[1].seq_no, FragmentSeqNo(2));
        assert!(!short_read);

        // Test the edge case where the old bug would have incorrectly calculated:
        // Old bug would use: fragments[1].start (101) - fragments[0].start (1) = 100 records
        // If max_records was 99, old code would incorrectly remove the second fragment
        let limits_edge_case = Limits {
            max_files: None,
            max_bytes: None,
            max_records: Some(50), // Just under the actual span from 'from' (51)
        };

        let mut short_read = false;
        let result_edge = LogReader::post_process_fragments(
            fragments.clone(),
            from,
            limits_edge_case,
            &mut short_read,
        );

        // With the fix: 101 - 50 = 51 > 50, so the second fragment should be removed
        assert_eq!(
            result_edge.len(),
            1,
            "Only first fragment should remain with 50 record limit"
        );
        assert_eq!(result_edge[0].seq_no, FragmentSeqNo(1));
        assert!(short_read);
    }

    #[test]
    fn test_ranges_overlap() {
        use crate::LogPosition;

        // Test cases that should return true (overlapping ranges)

        // Case 1: Complete overlap - one range is entirely within another
        assert!(
            ranges_overlap(
                (LogPosition::from_offset(10), LogPosition::from_offset(20)),
                (LogPosition::from_offset(12), LogPosition::from_offset(18))
            ),
            "Range (12,18) is entirely within (10,20)"
        );
        assert!(
            ranges_overlap(
                (LogPosition::from_offset(12), LogPosition::from_offset(18)),
                (LogPosition::from_offset(10), LogPosition::from_offset(20))
            ),
            "Range (12,18) is entirely within (10,20) - reversed"
        );

        // Case 2: Partial overlap - ranges overlap partially
        assert!(
            ranges_overlap(
                (LogPosition::from_offset(10), LogPosition::from_offset(20)),
                (LogPosition::from_offset(15), LogPosition::from_offset(25))
            ),
            "Ranges (10,20) and (15,25) overlap partially"
        );
        assert!(
            ranges_overlap(
                (LogPosition::from_offset(15), LogPosition::from_offset(25)),
                (LogPosition::from_offset(10), LogPosition::from_offset(20))
            ),
            "Ranges (15,25) and (10,20) overlap partially - reversed"
        );

        // Case 3: Identical ranges
        assert!(
            ranges_overlap(
                (LogPosition::from_offset(10), LogPosition::from_offset(20)),
                (LogPosition::from_offset(10), LogPosition::from_offset(20))
            ),
            "Identical ranges should overlap"
        );

        // Test cases that should return false (non-overlapping ranges)

        // Case 4: Completely separate ranges
        assert!(
            !ranges_overlap(
                (LogPosition::from_offset(10), LogPosition::from_offset(20)),
                (LogPosition::from_offset(25), LogPosition::from_offset(35))
            ),
            "Ranges (10,20) and (25,35) are completely separate"
        );
        assert!(
            !ranges_overlap(
                (LogPosition::from_offset(25), LogPosition::from_offset(35)),
                (LogPosition::from_offset(10), LogPosition::from_offset(20))
            ),
            "Ranges (25,35) and (10,20) are completely separate - reversed"
        );

        // Case 5: Adjacent but not touching ranges (gap between them)
        assert!(
            !ranges_overlap(
                (LogPosition::from_offset(10), LogPosition::from_offset(20)),
                (LogPosition::from_offset(21), LogPosition::from_offset(30))
            ),
            "Ranges (10,20) and (21,30) have a gap"
        );
        assert!(
            !ranges_overlap(
                (LogPosition::from_offset(21), LogPosition::from_offset(30)),
                (LogPosition::from_offset(10), LogPosition::from_offset(20))
            ),
            "Ranges (21,30) and (10,20) have a gap - reversed"
        );

        // Case 6: Adjacent ranges that just touch at boundaries (should NOT overlap for exclusive ranges)
        assert!(
            !ranges_overlap(
                (LogPosition::from_offset(10), LogPosition::from_offset(20)),
                (LogPosition::from_offset(20), LogPosition::from_offset(30))
            ),
            "Ranges (10,20) and (20,30) just touch - should not overlap"
        );
        assert!(
            !ranges_overlap(
                (LogPosition::from_offset(20), LogPosition::from_offset(30)),
                (LogPosition::from_offset(10), LogPosition::from_offset(20))
            ),
            "Ranges (20,30) and (10,20) just touch - should not overlap"
        );
    }

    #[test]
    fn scan_from_manifest_cached_manifest_boundary_conditions() {
        use crate::Manifest;

        // Test boundary conditions for the cached manifest bug fix
        // This tests the logic that checks if a cached manifest can satisfy a pull-logs request

        let fragments = vec![
            Fragment {
                path: "fragment1".to_string(),
                seq_no: FragmentSeqNo(1),
                start: LogPosition::from_offset(1),
                limit: LogPosition::from_offset(101),
                num_bytes: 1000,
                setsum: Setsum::default(),
            },
            Fragment {
                path: "fragment2".to_string(),
                seq_no: FragmentSeqNo(2),
                start: LogPosition::from_offset(101),
                limit: LogPosition::from_offset(201), // Manifest max is 201
                num_bytes: 1000,
                setsum: Setsum::default(),
            },
        ];

        let manifest = Manifest {
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 2000,
            writer: "test-writer".to_string(),
            snapshots: vec![],
            fragments: fragments.clone(),
            initial_offset: Some(LogPosition::from_offset(1)),
            initial_seq_no: Some(FragmentSeqNo(1)),
        };

        // Boundary case 1: Request exactly at the manifest limit
        let from = LogPosition::from_offset(100);
        let limits = Limits {
            max_files: None,
            max_bytes: None,
            max_records: Some(100), // Would need data up to exactly offset 200
        };
        let result = LogReader::scan_from_manifest(&manifest, from, limits);
        assert!(
            result.is_some(),
            "Should succeed when request stays within manifest coverage"
        );

        // Boundary case 2: Request exactly to the manifest limit
        let limits_at_limit = Limits {
            max_files: None,
            max_bytes: None,
            max_records: Some(101), // Would need data up to offset 201, manifest limit is 201
        };
        let result_at_limit = LogReader::scan_from_manifest(&manifest, from, limits_at_limit);
        assert!(
            result_at_limit.is_none(),
            "Should fail when request  exceeds limit"
        );

        // Boundary case 3: Request one beyond the manifest limit
        let limits_beyond = Limits {
            max_files: None,
            max_bytes: None,
            max_records: Some(102), // Would need data up to offset 202, beyond manifest limit of 201
        };
        let result_beyond = LogReader::scan_from_manifest(&manifest, from, limits_beyond);
        assert!(
            result_beyond.is_none(),
            "Should return None when request exceeds manifest coverage"
        );

        // Boundary case 4: Request from the very end of the manifest
        let from_end = LogPosition::from_offset(200);
        let limits_at_end = Limits {
            max_files: None,
            max_bytes: None,
            max_records: Some(1), // Would need data up to offset 201, exactly at manifest limit
        };
        let result_at_end = LogReader::scan_from_manifest(&manifest, from_end, limits_at_end);
        assert!(
            result_at_end.is_none(),
            "Should fail when reading exactly at manifest boundary"
        );

        // Boundary case 5: Request from beyond the manifest
        let from_beyond = LogPosition::from_offset(201);
        let limits_beyond = Limits {
            max_files: None,
            max_bytes: None,
            max_records: Some(1),
        };
        let result_beyond = LogReader::scan_from_manifest(&manifest, from_beyond, limits_beyond);
        assert!(
            result_beyond.is_none(),
            "Should return None when starting beyond manifest coverage"
        );

        // Boundary case 6: No max_records limit (LogPosition::MAX)
        let from_middle = LogPosition::from_offset(50);
        let limits_unlimited = Limits {
            max_files: None,
            max_bytes: None,
            max_records: None, // This creates a range to LogPosition::MAX
        };
        let result_unlimited =
            LogReader::scan_from_manifest(&manifest, from_middle, limits_unlimited);
        assert!(
            result_unlimited.is_none(),
            "Should return None when unlimited range extends beyond manifest"
        );

        // Boundary case 7: Empty manifest
        let empty_manifest = Manifest {
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 0,
            writer: "test-writer".to_string(),
            snapshots: vec![],
            fragments: vec![],
            initial_offset: None,
            initial_seq_no: None,
        };
        let result_empty =
            LogReader::scan_from_manifest(&empty_manifest, LogPosition::from_offset(0), limits);
        assert!(
            result_empty.is_none(),
            "Should return None for empty manifest"
        );

        // Boundary case 8: Integer overflow conditions (i64::MAX scenario from the bug fix)
        let from_overflow_test = LogPosition::from_offset(u64::MAX - 10);
        let limits_overflow = Limits {
            max_files: None,
            max_bytes: None,
            max_records: Some(20), // This would overflow if not handled properly
        };
        let result_overflow =
            LogReader::scan_from_manifest(&manifest, from_overflow_test, limits_overflow);
        assert!(
            result_overflow.is_none(),
            "Should handle potential overflow gracefully"
        );
    }

    #[test]
    fn obo_in_manifest_code() {
        let manifest = Manifest {
            setsum: Setsum::default(),
            collected: Setsum::default(),
            acc_bytes: 35837467,
            writer: "log writer".to_string(),
            snapshots: vec![],
            fragments: vec![
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000001.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(1),
                    start: LogPosition { offset: 1 },
                    limit: LogPosition { offset: 101 },
                    num_bytes: 140461,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000002.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(2),
                    start: LogPosition { offset: 101 },
                    limit: LogPosition { offset: 201 },
                    num_bytes: 139431,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000003.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(3),
                    start: LogPosition { offset: 201 },
                    limit: LogPosition { offset: 301 },
                    num_bytes: 152250,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000004.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(4),
                    start: LogPosition { offset: 301 },
                    limit: LogPosition { offset: 401 },
                    num_bytes: 141502,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000005.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(5),
                    start: LogPosition { offset: 401 },
                    limit: LogPosition { offset: 501 },
                    num_bytes: 139784,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000006.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(6),
                    start: LogPosition { offset: 501 },
                    limit: LogPosition { offset: 601 },
                    num_bytes: 133366,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000007.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(7),
                    start: LogPosition { offset: 601 },
                    limit: LogPosition { offset: 701 },
                    num_bytes: 135825,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000008.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(8),
                    start: LogPosition { offset: 701 },
                    limit: LogPosition { offset: 801 },
                    num_bytes: 133677,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000009.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(9),
                    start: LogPosition { offset: 801 },
                    limit: LogPosition { offset: 901 },
                    num_bytes: 131341,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000000a.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(10),
                    start: LogPosition { offset: 901 },
                    limit: LogPosition { offset: 1001 },
                    num_bytes: 139558,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000000b.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(11),
                    start: LogPosition { offset: 1001 },
                    limit: LogPosition { offset: 1101 },
                    num_bytes: 139566,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000000c.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(12),
                    start: LogPosition { offset: 1101 },
                    limit: LogPosition { offset: 1201 },
                    num_bytes: 138893,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000000d.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(13),
                    start: LogPosition { offset: 1201 },
                    limit: LogPosition { offset: 1301 },
                    num_bytes: 144141,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000000e.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(14),
                    start: LogPosition { offset: 1301 },
                    limit: LogPosition { offset: 1401 },
                    num_bytes: 136472,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000000f.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(15),
                    start: LogPosition { offset: 1401 },
                    limit: LogPosition { offset: 1501 },
                    num_bytes: 136962,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000010.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(16),
                    start: LogPosition { offset: 1501 },
                    limit: LogPosition { offset: 1601 },
                    num_bytes: 135440,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000011.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(17),
                    start: LogPosition { offset: 1601 },
                    limit: LogPosition { offset: 1701 },
                    num_bytes: 136610,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000012.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(18),
                    start: LogPosition { offset: 1701 },
                    limit: LogPosition { offset: 1801 },
                    num_bytes: 138079,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000013.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(19),
                    start: LogPosition { offset: 1801 },
                    limit: LogPosition { offset: 1901 },
                    num_bytes: 132739,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000014.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(20),
                    start: LogPosition { offset: 1901 },
                    limit: LogPosition { offset: 2001 },
                    num_bytes: 155167,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000015.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(21),
                    start: LogPosition { offset: 2001 },
                    limit: LogPosition { offset: 2101 },
                    num_bytes: 133472,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000016.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(22),
                    start: LogPosition { offset: 2101 },
                    limit: LogPosition { offset: 2201 },
                    num_bytes: 137153,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000017.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(23),
                    start: LogPosition { offset: 2201 },
                    limit: LogPosition { offset: 2301 },
                    num_bytes: 133490,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000018.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(24),
                    start: LogPosition { offset: 2301 },
                    limit: LogPosition { offset: 2401 },
                    num_bytes: 136554,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000019.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(25),
                    start: LogPosition { offset: 2401 },
                    limit: LogPosition { offset: 2501 },
                    num_bytes: 138884,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000001a.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(26),
                    start: LogPosition { offset: 2501 },
                    limit: LogPosition { offset: 2601 },
                    num_bytes: 137372,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000001b.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(27),
                    start: LogPosition { offset: 2601 },
                    limit: LogPosition { offset: 2701 },
                    num_bytes: 138278,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000001c.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(28),
                    start: LogPosition { offset: 2701 },
                    limit: LogPosition { offset: 2801 },
                    num_bytes: 134956,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000001d.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(29),
                    start: LogPosition { offset: 2801 },
                    limit: LogPosition { offset: 2901 },
                    num_bytes: 140997,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000001e.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(30),
                    start: LogPosition { offset: 2901 },
                    limit: LogPosition { offset: 3001 },
                    num_bytes: 138062,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000001f.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(31),
                    start: LogPosition { offset: 3001 },
                    limit: LogPosition { offset: 3101 },
                    num_bytes: 134711,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000020.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(32),
                    start: LogPosition { offset: 3101 },
                    limit: LogPosition { offset: 3201 },
                    num_bytes: 144809,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000021.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(33),
                    start: LogPosition { offset: 3201 },
                    limit: LogPosition { offset: 3301 },
                    num_bytes: 138345,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000022.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(34),
                    start: LogPosition { offset: 3301 },
                    limit: LogPosition { offset: 3401 },
                    num_bytes: 136250,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000023.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(35),
                    start: LogPosition { offset: 3401 },
                    limit: LogPosition { offset: 3501 },
                    num_bytes: 146369,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000024.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(36),
                    start: LogPosition { offset: 3501 },
                    limit: LogPosition { offset: 3601 },
                    num_bytes: 138827,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000025.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(37),
                    start: LogPosition { offset: 3601 },
                    limit: LogPosition { offset: 3701 },
                    num_bytes: 133829,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000026.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(38),
                    start: LogPosition { offset: 3701 },
                    limit: LogPosition { offset: 3801 },
                    num_bytes: 140918,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000027.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(39),
                    start: LogPosition { offset: 3801 },
                    limit: LogPosition { offset: 3901 },
                    num_bytes: 141103,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000028.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(40),
                    start: LogPosition { offset: 3901 },
                    limit: LogPosition { offset: 4001 },
                    num_bytes: 141949,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000029.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(41),
                    start: LogPosition { offset: 4001 },
                    limit: LogPosition { offset: 4101 },
                    num_bytes: 139094,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000002a.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(42),
                    start: LogPosition { offset: 4101 },
                    limit: LogPosition { offset: 4201 },
                    num_bytes: 139944,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000002b.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(43),
                    start: LogPosition { offset: 4201 },
                    limit: LogPosition { offset: 4301 },
                    num_bytes: 140248,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000002c.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(44),
                    start: LogPosition { offset: 4301 },
                    limit: LogPosition { offset: 4401 },
                    num_bytes: 140256,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000002d.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(45),
                    start: LogPosition { offset: 4401 },
                    limit: LogPosition { offset: 4501 },
                    num_bytes: 141742,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000002e.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(46),
                    start: LogPosition { offset: 4501 },
                    limit: LogPosition { offset: 4601 },
                    num_bytes: 142404,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000002f.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(47),
                    start: LogPosition { offset: 4601 },
                    limit: LogPosition { offset: 4701 },
                    num_bytes: 137577,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000030.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(48),
                    start: LogPosition { offset: 4701 },
                    limit: LogPosition { offset: 4801 },
                    num_bytes: 134633,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000031.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(49),
                    start: LogPosition { offset: 4801 },
                    limit: LogPosition { offset: 4901 },
                    num_bytes: 141037,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000032.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(50),
                    start: LogPosition { offset: 4901 },
                    limit: LogPosition { offset: 5001 },
                    num_bytes: 131669,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000033.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(51),
                    start: LogPosition { offset: 5001 },
                    limit: LogPosition { offset: 5101 },
                    num_bytes: 138795,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000034.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(52),
                    start: LogPosition { offset: 5101 },
                    limit: LogPosition { offset: 5201 },
                    num_bytes: 133732,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000035.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(53),
                    start: LogPosition { offset: 5201 },
                    limit: LogPosition { offset: 5301 },
                    num_bytes: 135872,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000036.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(54),
                    start: LogPosition { offset: 5301 },
                    limit: LogPosition { offset: 5401 },
                    num_bytes: 139780,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000037.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(55),
                    start: LogPosition { offset: 5401 },
                    limit: LogPosition { offset: 5501 },
                    num_bytes: 139217,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000038.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(56),
                    start: LogPosition { offset: 5501 },
                    limit: LogPosition { offset: 5601 },
                    num_bytes: 136125,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000039.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(57),
                    start: LogPosition { offset: 5601 },
                    limit: LogPosition { offset: 5701 },
                    num_bytes: 139423,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000003a.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(58),
                    start: LogPosition { offset: 5701 },
                    limit: LogPosition { offset: 5801 },
                    num_bytes: 142812,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000003b.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(59),
                    start: LogPosition { offset: 5801 },
                    limit: LogPosition { offset: 5901 },
                    num_bytes: 141047,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000003c.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(60),
                    start: LogPosition { offset: 5901 },
                    limit: LogPosition { offset: 6001 },
                    num_bytes: 142000,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000003d.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(61),
                    start: LogPosition { offset: 6001 },
                    limit: LogPosition { offset: 6101 },
                    num_bytes: 136870,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000003e.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(62),
                    start: LogPosition { offset: 6101 },
                    limit: LogPosition { offset: 6201 },
                    num_bytes: 134251,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000003f.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(63),
                    start: LogPosition { offset: 6201 },
                    limit: LogPosition { offset: 6301 },
                    num_bytes: 158023,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000040.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(64),
                    start: LogPosition { offset: 6301 },
                    limit: LogPosition { offset: 6401 },
                    num_bytes: 136371,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000041.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(65),
                    start: LogPosition { offset: 6401 },
                    limit: LogPosition { offset: 6501 },
                    num_bytes: 145348,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000042.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(66),
                    start: LogPosition { offset: 6501 },
                    limit: LogPosition { offset: 6601 },
                    num_bytes: 138702,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000043.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(67),
                    start: LogPosition { offset: 6601 },
                    limit: LogPosition { offset: 6701 },
                    num_bytes: 152525,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000044.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(68),
                    start: LogPosition { offset: 6701 },
                    limit: LogPosition { offset: 6801 },
                    num_bytes: 139994,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000045.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(69),
                    start: LogPosition { offset: 6801 },
                    limit: LogPosition { offset: 6901 },
                    num_bytes: 136266,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000046.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(70),
                    start: LogPosition { offset: 6901 },
                    limit: LogPosition { offset: 7001 },
                    num_bytes: 138243,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000047.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(71),
                    start: LogPosition { offset: 7001 },
                    limit: LogPosition { offset: 7101 },
                    num_bytes: 139202,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000048.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(72),
                    start: LogPosition { offset: 7101 },
                    limit: LogPosition { offset: 7201 },
                    num_bytes: 138727,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000049.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(73),
                    start: LogPosition { offset: 7201 },
                    limit: LogPosition { offset: 7301 },
                    num_bytes: 136865,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000004a.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(74),
                    start: LogPosition { offset: 7301 },
                    limit: LogPosition { offset: 7401 },
                    num_bytes: 138886,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000004b.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(75),
                    start: LogPosition { offset: 7401 },
                    limit: LogPosition { offset: 7501 },
                    num_bytes: 137304,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000004c.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(76),
                    start: LogPosition { offset: 7501 },
                    limit: LogPosition { offset: 7601 },
                    num_bytes: 136574,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000004d.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(77),
                    start: LogPosition { offset: 7601 },
                    limit: LogPosition { offset: 7701 },
                    num_bytes: 140747,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000004e.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(78),
                    start: LogPosition { offset: 7701 },
                    limit: LogPosition { offset: 7801 },
                    num_bytes: 144560,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000004f.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(79),
                    start: LogPosition { offset: 7801 },
                    limit: LogPosition { offset: 7901 },
                    num_bytes: 137682,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000050.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(80),
                    start: LogPosition { offset: 7901 },
                    limit: LogPosition { offset: 8001 },
                    num_bytes: 141263,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000051.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(81),
                    start: LogPosition { offset: 8001 },
                    limit: LogPosition { offset: 8101 },
                    num_bytes: 136293,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000052.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(82),
                    start: LogPosition { offset: 8101 },
                    limit: LogPosition { offset: 8201 },
                    num_bytes: 134459,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000053.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(83),
                    start: LogPosition { offset: 8201 },
                    limit: LogPosition { offset: 8301 },
                    num_bytes: 137102,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000054.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(84),
                    start: LogPosition { offset: 8301 },
                    limit: LogPosition { offset: 8401 },
                    num_bytes: 140636,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000055.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(85),
                    start: LogPosition { offset: 8401 },
                    limit: LogPosition { offset: 8501 },
                    num_bytes: 137111,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000056.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(86),
                    start: LogPosition { offset: 8501 },
                    limit: LogPosition { offset: 8601 },
                    num_bytes: 135579,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000057.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(87),
                    start: LogPosition { offset: 8601 },
                    limit: LogPosition { offset: 8701 },
                    num_bytes: 137219,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000058.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(88),
                    start: LogPosition { offset: 8701 },
                    limit: LogPosition { offset: 8801 },
                    num_bytes: 141777,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000059.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(89),
                    start: LogPosition { offset: 8801 },
                    limit: LogPosition { offset: 8901 },
                    num_bytes: 133803,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000005a.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(90),
                    start: LogPosition { offset: 8901 },
                    limit: LogPosition { offset: 9001 },
                    num_bytes: 135483,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000005b.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(91),
                    start: LogPosition { offset: 9001 },
                    limit: LogPosition { offset: 9101 },
                    num_bytes: 140399,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000005c.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(92),
                    start: LogPosition { offset: 9101 },
                    limit: LogPosition { offset: 9201 },
                    num_bytes: 143820,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000005d.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(93),
                    start: LogPosition { offset: 9201 },
                    limit: LogPosition { offset: 9301 },
                    num_bytes: 139460,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000005e.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(94),
                    start: LogPosition { offset: 9301 },
                    limit: LogPosition { offset: 9401 },
                    num_bytes: 137437,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000005f.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(95),
                    start: LogPosition { offset: 9401 },
                    limit: LogPosition { offset: 9501 },
                    num_bytes: 142969,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000060.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(96),
                    start: LogPosition { offset: 9501 },
                    limit: LogPosition { offset: 9601 },
                    num_bytes: 141351,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000061.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(97),
                    start: LogPosition { offset: 9601 },
                    limit: LogPosition { offset: 9701 },
                    num_bytes: 138392,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000062.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(98),
                    start: LogPosition { offset: 9701 },
                    limit: LogPosition { offset: 9801 },
                    num_bytes: 142135,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000063.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(99),
                    start: LogPosition { offset: 9801 },
                    limit: LogPosition { offset: 9901 },
                    num_bytes: 135380,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000064.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(100),
                    start: LogPosition { offset: 9901 },
                    limit: LogPosition { offset: 10001 },
                    num_bytes: 141166,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000065.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(101),
                    start: LogPosition { offset: 10001 },
                    limit: LogPosition { offset: 10101 },
                    num_bytes: 145075,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000066.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(102),
                    start: LogPosition { offset: 10101 },
                    limit: LogPosition { offset: 10201 },
                    num_bytes: 139179,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000067.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(103),
                    start: LogPosition { offset: 10201 },
                    limit: LogPosition { offset: 10301 },
                    num_bytes: 141121,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000068.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(104),
                    start: LogPosition { offset: 10301 },
                    limit: LogPosition { offset: 10401 },
                    num_bytes: 133021,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000069.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(105),
                    start: LogPosition { offset: 10401 },
                    limit: LogPosition { offset: 10501 },
                    num_bytes: 133919,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000006a.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(106),
                    start: LogPosition { offset: 10501 },
                    limit: LogPosition { offset: 10601 },
                    num_bytes: 145022,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000006b.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(107),
                    start: LogPosition { offset: 10601 },
                    limit: LogPosition { offset: 10701 },
                    num_bytes: 141337,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000006c.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(108),
                    start: LogPosition { offset: 10701 },
                    limit: LogPosition { offset: 10801 },
                    num_bytes: 150894,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000006d.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(109),
                    start: LogPosition { offset: 10801 },
                    limit: LogPosition { offset: 10901 },
                    num_bytes: 146528,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000006e.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(110),
                    start: LogPosition { offset: 10901 },
                    limit: LogPosition { offset: 11001 },
                    num_bytes: 136972,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000006f.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(111),
                    start: LogPosition { offset: 11001 },
                    limit: LogPosition { offset: 11101 },
                    num_bytes: 137727,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000070.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(112),
                    start: LogPosition { offset: 11101 },
                    limit: LogPosition { offset: 11201 },
                    num_bytes: 140892,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000071.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(113),
                    start: LogPosition { offset: 11201 },
                    limit: LogPosition { offset: 11301 },
                    num_bytes: 141376,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000072.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(114),
                    start: LogPosition { offset: 11301 },
                    limit: LogPosition { offset: 11401 },
                    num_bytes: 139071,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000073.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(115),
                    start: LogPosition { offset: 11401 },
                    limit: LogPosition { offset: 11501 },
                    num_bytes: 132369,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000074.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(116),
                    start: LogPosition { offset: 11501 },
                    limit: LogPosition { offset: 11601 },
                    num_bytes: 136670,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000075.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(117),
                    start: LogPosition { offset: 11601 },
                    limit: LogPosition { offset: 11701 },
                    num_bytes: 143230,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000076.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(118),
                    start: LogPosition { offset: 11701 },
                    limit: LogPosition { offset: 11801 },
                    num_bytes: 147801,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000077.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(119),
                    start: LogPosition { offset: 11801 },
                    limit: LogPosition { offset: 11901 },
                    num_bytes: 139923,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000078.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(120),
                    start: LogPosition { offset: 11901 },
                    limit: LogPosition { offset: 12001 },
                    num_bytes: 139459,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000079.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(121),
                    start: LogPosition { offset: 12001 },
                    limit: LogPosition { offset: 12101 },
                    num_bytes: 138578,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000007a.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(122),
                    start: LogPosition { offset: 12101 },
                    limit: LogPosition { offset: 12201 },
                    num_bytes: 138652,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000007b.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(123),
                    start: LogPosition { offset: 12201 },
                    limit: LogPosition { offset: 12301 },
                    num_bytes: 141800,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000007c.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(124),
                    start: LogPosition { offset: 12301 },
                    limit: LogPosition { offset: 12401 },
                    num_bytes: 137535,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000007d.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(125),
                    start: LogPosition { offset: 12401 },
                    limit: LogPosition { offset: 12501 },
                    num_bytes: 137534,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000007e.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(126),
                    start: LogPosition { offset: 12501 },
                    limit: LogPosition { offset: 12601 },
                    num_bytes: 139740,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000007f.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(127),
                    start: LogPosition { offset: 12601 },
                    limit: LogPosition { offset: 12701 },
                    num_bytes: 139313,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000080.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(128),
                    start: LogPosition { offset: 12701 },
                    limit: LogPosition { offset: 12801 },
                    num_bytes: 141420,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000081.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(129),
                    start: LogPosition { offset: 12801 },
                    limit: LogPosition { offset: 12901 },
                    num_bytes: 144742,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000082.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(130),
                    start: LogPosition { offset: 12901 },
                    limit: LogPosition { offset: 13001 },
                    num_bytes: 140023,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000083.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(131),
                    start: LogPosition { offset: 13001 },
                    limit: LogPosition { offset: 13101 },
                    num_bytes: 141135,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000084.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(132),
                    start: LogPosition { offset: 13101 },
                    limit: LogPosition { offset: 13201 },
                    num_bytes: 139778,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000085.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(133),
                    start: LogPosition { offset: 13201 },
                    limit: LogPosition { offset: 13301 },
                    num_bytes: 141698,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000086.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(134),
                    start: LogPosition { offset: 13301 },
                    limit: LogPosition { offset: 13401 },
                    num_bytes: 149539,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000087.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(135),
                    start: LogPosition { offset: 13401 },
                    limit: LogPosition { offset: 13501 },
                    num_bytes: 137223,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000088.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(136),
                    start: LogPosition { offset: 13501 },
                    limit: LogPosition { offset: 13601 },
                    num_bytes: 138479,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000089.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(137),
                    start: LogPosition { offset: 13601 },
                    limit: LogPosition { offset: 13701 },
                    num_bytes: 138107,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000008a.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(138),
                    start: LogPosition { offset: 13701 },
                    limit: LogPosition { offset: 13801 },
                    num_bytes: 132080,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000008b.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(139),
                    start: LogPosition { offset: 13801 },
                    limit: LogPosition { offset: 13901 },
                    num_bytes: 132956,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000008c.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(140),
                    start: LogPosition { offset: 13901 },
                    limit: LogPosition { offset: 14001 },
                    num_bytes: 137782,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000008d.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(141),
                    start: LogPosition { offset: 14001 },
                    limit: LogPosition { offset: 14101 },
                    num_bytes: 135937,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000008e.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(142),
                    start: LogPosition { offset: 14101 },
                    limit: LogPosition { offset: 14201 },
                    num_bytes: 135979,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000008f.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(143),
                    start: LogPosition { offset: 14201 },
                    limit: LogPosition { offset: 14301 },
                    num_bytes: 137787,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000090.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(144),
                    start: LogPosition { offset: 14301 },
                    limit: LogPosition { offset: 14401 },
                    num_bytes: 136146,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000091.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(145),
                    start: LogPosition { offset: 14401 },
                    limit: LogPosition { offset: 14501 },
                    num_bytes: 135798,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000092.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(146),
                    start: LogPosition { offset: 14501 },
                    limit: LogPosition { offset: 14601 },
                    num_bytes: 140262,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000093.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(147),
                    start: LogPosition { offset: 14601 },
                    limit: LogPosition { offset: 14701 },
                    num_bytes: 140513,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000094.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(148),
                    start: LogPosition { offset: 14701 },
                    limit: LogPosition { offset: 14801 },
                    num_bytes: 143028,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000095.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(149),
                    start: LogPosition { offset: 14801 },
                    limit: LogPosition { offset: 14901 },
                    num_bytes: 141584,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000096.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(150),
                    start: LogPosition { offset: 14901 },
                    limit: LogPosition { offset: 15001 },
                    num_bytes: 134143,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000097.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(151),
                    start: LogPosition { offset: 15001 },
                    limit: LogPosition { offset: 15101 },
                    num_bytes: 134158,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000098.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(152),
                    start: LogPosition { offset: 15101 },
                    limit: LogPosition { offset: 15201 },
                    num_bytes: 131993,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000099.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(153),
                    start: LogPosition { offset: 15201 },
                    limit: LogPosition { offset: 15301 },
                    num_bytes: 143121,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000009a.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(154),
                    start: LogPosition { offset: 15301 },
                    limit: LogPosition { offset: 15401 },
                    num_bytes: 140176,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000009b.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(155),
                    start: LogPosition { offset: 15401 },
                    limit: LogPosition { offset: 15501 },
                    num_bytes: 129247,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000009c.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(156),
                    start: LogPosition { offset: 15501 },
                    limit: LogPosition { offset: 15601 },
                    num_bytes: 135408,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000009d.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(157),
                    start: LogPosition { offset: 15601 },
                    limit: LogPosition { offset: 15701 },
                    num_bytes: 140057,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000009e.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(158),
                    start: LogPosition { offset: 15701 },
                    limit: LogPosition { offset: 15801 },
                    num_bytes: 142579,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000009f.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(159),
                    start: LogPosition { offset: 15801 },
                    limit: LogPosition { offset: 15901 },
                    num_bytes: 132968,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000a0.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(160),
                    start: LogPosition { offset: 15901 },
                    limit: LogPosition { offset: 16001 },
                    num_bytes: 144536,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000a1.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(161),
                    start: LogPosition { offset: 16001 },
                    limit: LogPosition { offset: 16101 },
                    num_bytes: 135808,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000a2.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(162),
                    start: LogPosition { offset: 16101 },
                    limit: LogPosition { offset: 16201 },
                    num_bytes: 142077,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000a3.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(163),
                    start: LogPosition { offset: 16201 },
                    limit: LogPosition { offset: 16301 },
                    num_bytes: 128320,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000a4.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(164),
                    start: LogPosition { offset: 16301 },
                    limit: LogPosition { offset: 16401 },
                    num_bytes: 141075,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000a5.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(165),
                    start: LogPosition { offset: 16401 },
                    limit: LogPosition { offset: 16501 },
                    num_bytes: 147777,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000a6.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(166),
                    start: LogPosition { offset: 16501 },
                    limit: LogPosition { offset: 16601 },
                    num_bytes: 142136,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000a7.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(167),
                    start: LogPosition { offset: 16601 },
                    limit: LogPosition { offset: 16701 },
                    num_bytes: 139917,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000a8.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(168),
                    start: LogPosition { offset: 16701 },
                    limit: LogPosition { offset: 16801 },
                    num_bytes: 135551,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000a9.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(169),
                    start: LogPosition { offset: 16801 },
                    limit: LogPosition { offset: 16901 },
                    num_bytes: 138513,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000aa.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(170),
                    start: LogPosition { offset: 16901 },
                    limit: LogPosition { offset: 16998 },
                    num_bytes: 128558,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000ab.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(171),
                    start: LogPosition { offset: 16998 },
                    limit: LogPosition { offset: 17098 },
                    num_bytes: 140852,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000ac.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(172),
                    start: LogPosition { offset: 17098 },
                    limit: LogPosition { offset: 17198 },
                    num_bytes: 137489,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000ad.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(173),
                    start: LogPosition { offset: 17198 },
                    limit: LogPosition { offset: 17230 },
                    num_bytes: 58889,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000ae.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(174),
                    start: LogPosition { offset: 17230 },
                    limit: LogPosition { offset: 17330 },
                    num_bytes: 132866,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000af.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(175),
                    start: LogPosition { offset: 17330 },
                    limit: LogPosition { offset: 17430 },
                    num_bytes: 136424,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000b0.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(176),
                    start: LogPosition { offset: 17430 },
                    limit: LogPosition { offset: 17462 },
                    num_bytes: 65028,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000b1.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(177),
                    start: LogPosition { offset: 17462 },
                    limit: LogPosition { offset: 17562 },
                    num_bytes: 143723,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000b2.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(178),
                    start: LogPosition { offset: 17562 },
                    limit: LogPosition { offset: 17662 },
                    num_bytes: 141430,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000b3.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(179),
                    start: LogPosition { offset: 17662 },
                    limit: LogPosition { offset: 17747 },
                    num_bytes: 117091,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000b4.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(180),
                    start: LogPosition { offset: 17747 },
                    limit: LogPosition { offset: 17847 },
                    num_bytes: 136364,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000b5.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(181),
                    start: LogPosition { offset: 17847 },
                    limit: LogPosition { offset: 17947 },
                    num_bytes: 143624,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000b6.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(182),
                    start: LogPosition { offset: 17947 },
                    limit: LogPosition { offset: 17960 },
                    num_bytes: 40448,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000b7.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(183),
                    start: LogPosition { offset: 17960 },
                    limit: LogPosition { offset: 18060 },
                    num_bytes: 132795,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000b8.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(184),
                    start: LogPosition { offset: 18060 },
                    limit: LogPosition { offset: 18103 },
                    num_bytes: 82080,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000b9.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(185),
                    start: LogPosition { offset: 18103 },
                    limit: LogPosition { offset: 18203 },
                    num_bytes: 135489,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000ba.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(186),
                    start: LogPosition { offset: 18203 },
                    limit: LogPosition { offset: 18281 },
                    num_bytes: 119440,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000bb.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(187),
                    start: LogPosition { offset: 18281 },
                    limit: LogPosition { offset: 18381 },
                    num_bytes: 137393,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000bc.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(188),
                    start: LogPosition { offset: 18381 },
                    limit: LogPosition { offset: 18481 },
                    num_bytes: 143793,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000bd.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(189),
                    start: LogPosition { offset: 18481 },
                    limit: LogPosition { offset: 18495 },
                    num_bytes: 40225,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000be.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(190),
                    start: LogPosition { offset: 18495 },
                    limit: LogPosition { offset: 18595 },
                    num_bytes: 135172,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000bf.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(191),
                    start: LogPosition { offset: 18595 },
                    limit: LogPosition { offset: 18673 },
                    num_bytes: 114019,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000c0.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(192),
                    start: LogPosition { offset: 18673 },
                    limit: LogPosition { offset: 18773 },
                    num_bytes: 134766,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000c1.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(193),
                    start: LogPosition { offset: 18773 },
                    limit: LogPosition { offset: 18833 },
                    num_bytes: 93267,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000c2.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(194),
                    start: LogPosition { offset: 18833 },
                    limit: LogPosition { offset: 18933 },
                    num_bytes: 135209,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000c3.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(195),
                    start: LogPosition { offset: 18933 },
                    limit: LogPosition { offset: 18958 },
                    num_bytes: 56317,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000c4.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(196),
                    start: LogPosition { offset: 18958 },
                    limit: LogPosition { offset: 19058 },
                    num_bytes: 138040,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000c5.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(197),
                    start: LogPosition { offset: 19058 },
                    limit: LogPosition { offset: 19136 },
                    num_bytes: 116094,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000c6.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(198),
                    start: LogPosition { offset: 19136 },
                    limit: LogPosition { offset: 19236 },
                    num_bytes: 146527,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000c7.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(199),
                    start: LogPosition { offset: 19236 },
                    limit: LogPosition { offset: 19336 },
                    num_bytes: 138535,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000c8.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(200),
                    start: LogPosition { offset: 19336 },
                    limit: LogPosition { offset: 19368 },
                    num_bytes: 59758,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000c9.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(201),
                    start: LogPosition { offset: 19368 },
                    limit: LogPosition { offset: 19468 },
                    num_bytes: 136268,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000ca.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(202),
                    start: LogPosition { offset: 19468 },
                    limit: LogPosition { offset: 19511 },
                    num_bytes: 74216,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000cb.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(203),
                    start: LogPosition { offset: 19511 },
                    limit: LogPosition { offset: 19600 },
                    num_bytes: 122984,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000cc.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(204),
                    start: LogPosition { offset: 19600 },
                    limit: LogPosition { offset: 19700 },
                    num_bytes: 135231,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000cd.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(205),
                    start: LogPosition { offset: 19700 },
                    limit: LogPosition { offset: 19800 },
                    num_bytes: 146693,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000ce.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(206),
                    start: LogPosition { offset: 19800 },
                    limit: LogPosition { offset: 19831 },
                    num_bytes: 62674,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000cf.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(207),
                    start: LogPosition { offset: 19831 },
                    limit: LogPosition { offset: 19931 },
                    num_bytes: 141046,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000d0.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(208),
                    start: LogPosition { offset: 19931 },
                    limit: LogPosition { offset: 20031 },
                    num_bytes: 142907,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000d1.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(209),
                    start: LogPosition { offset: 20031 },
                    limit: LogPosition { offset: 20045 },
                    num_bytes: 41411,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000d2.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(210),
                    start: LogPosition { offset: 20045 },
                    limit: LogPosition { offset: 20145 },
                    num_bytes: 144353,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000d3.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(211),
                    start: LogPosition { offset: 20145 },
                    limit: LogPosition { offset: 20223 },
                    num_bytes: 119791,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000d4.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(212),
                    start: LogPosition { offset: 20223 },
                    limit: LogPosition { offset: 20323 },
                    num_bytes: 140264,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000d5.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(213),
                    start: LogPosition { offset: 20323 },
                    limit: LogPosition { offset: 20401 },
                    num_bytes: 117603,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000d6.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(214),
                    start: LogPosition { offset: 20401 },
                    limit: LogPosition { offset: 20501 },
                    num_bytes: 137419,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000d7.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(215),
                    start: LogPosition { offset: 20501 },
                    limit: LogPosition { offset: 20601 },
                    num_bytes: 134816,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000d8.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(216),
                    start: LogPosition { offset: 20601 },
                    limit: LogPosition { offset: 20615 },
                    num_bytes: 44611,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000d9.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(217),
                    start: LogPosition { offset: 20615 },
                    limit: LogPosition { offset: 20715 },
                    num_bytes: 147000,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000da.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(218),
                    start: LogPosition { offset: 20715 },
                    limit: LogPosition { offset: 20776 },
                    num_bytes: 100711,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000db.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(219),
                    start: LogPosition { offset: 20776 },
                    limit: LogPosition { offset: 20876 },
                    num_bytes: 130467,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000dc.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(220),
                    start: LogPosition { offset: 20876 },
                    limit: LogPosition { offset: 20918 },
                    num_bytes: 78680,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000dd.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(221),
                    start: LogPosition { offset: 20918 },
                    limit: LogPosition { offset: 21018 },
                    num_bytes: 141027,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000de.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(222),
                    start: LogPosition { offset: 21018 },
                    limit: LogPosition { offset: 21118 },
                    num_bytes: 137172,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000df.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(223),
                    start: LogPosition { offset: 21118 },
                    limit: LogPosition { offset: 21120 },
                    num_bytes: 28577,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000e0.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(224),
                    start: LogPosition { offset: 21120 },
                    limit: LogPosition { offset: 21220 },
                    num_bytes: 142801,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000e1.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(225),
                    start: LogPosition { offset: 21220 },
                    limit: LogPosition { offset: 21317 },
                    num_bytes: 132718,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000e2.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(226),
                    start: LogPosition { offset: 21317 },
                    limit: LogPosition { offset: 21417 },
                    num_bytes: 141569,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000e3.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(227),
                    start: LogPosition { offset: 21417 },
                    limit: LogPosition { offset: 21517 },
                    num_bytes: 135554,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000e4.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(228),
                    start: LogPosition { offset: 21517 },
                    limit: LogPosition { offset: 21617 },
                    num_bytes: 139003,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000e5.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(229),
                    start: LogPosition { offset: 21617 },
                    limit: LogPosition { offset: 21717 },
                    num_bytes: 138216,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000e6.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(230),
                    start: LogPosition { offset: 21717 },
                    limit: LogPosition { offset: 21723 },
                    num_bytes: 37598,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000e7.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(231),
                    start: LogPosition { offset: 21723 },
                    limit: LogPosition { offset: 21823 },
                    num_bytes: 141600,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000e8.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(232),
                    start: LogPosition { offset: 21823 },
                    limit: LogPosition { offset: 21923 },
                    num_bytes: 143969,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000e9.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(233),
                    start: LogPosition { offset: 21923 },
                    limit: LogPosition { offset: 21971 },
                    num_bytes: 80795,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000ea.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(234),
                    start: LogPosition { offset: 21971 },
                    limit: LogPosition { offset: 22071 },
                    num_bytes: 137429,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000eb.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(235),
                    start: LogPosition { offset: 22071 },
                    limit: LogPosition { offset: 22171 },
                    num_bytes: 138327,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000ec.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(236),
                    start: LogPosition { offset: 22171 },
                    limit: LogPosition { offset: 22213 },
                    num_bytes: 72307,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000ed.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(237),
                    start: LogPosition { offset: 22213 },
                    limit: LogPosition { offset: 22313 },
                    num_bytes: 134711,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000ee.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(238),
                    start: LogPosition { offset: 22313 },
                    limit: LogPosition { offset: 22413 },
                    num_bytes: 143139,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000ef.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(239),
                    start: LogPosition { offset: 22413 },
                    limit: LogPosition { offset: 22432 },
                    num_bytes: 49336,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000f0.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(240),
                    start: LogPosition { offset: 22432 },
                    limit: LogPosition { offset: 22532 },
                    num_bytes: 139229,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000f1.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(241),
                    start: LogPosition { offset: 22532 },
                    limit: LogPosition { offset: 22609 },
                    num_bytes: 113924,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000f2.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(242),
                    start: LogPosition { offset: 22609 },
                    limit: LogPosition { offset: 22709 },
                    num_bytes: 142130,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000f3.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(243),
                    start: LogPosition { offset: 22709 },
                    limit: LogPosition { offset: 22809 },
                    num_bytes: 133268,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000f4.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(244),
                    start: LogPosition { offset: 22809 },
                    limit: LogPosition { offset: 22891 },
                    num_bytes: 113712,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000f5.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(245),
                    start: LogPosition { offset: 22891 },
                    limit: LogPosition { offset: 22991 },
                    num_bytes: 135405,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000f6.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(246),
                    start: LogPosition { offset: 22991 },
                    limit: LogPosition { offset: 23091 },
                    num_bytes: 134463,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000f7.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(247),
                    start: LogPosition { offset: 23091 },
                    limit: LogPosition { offset: 23146 },
                    num_bytes: 86577,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000f8.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(248),
                    start: LogPosition { offset: 23146 },
                    limit: LogPosition { offset: 23246 },
                    num_bytes: 133988,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000f9.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(249),
                    start: LogPosition { offset: 23246 },
                    limit: LogPosition { offset: 23346 },
                    num_bytes: 140277,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000fa.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(250),
                    start: LogPosition { offset: 23346 },
                    limit: LogPosition { offset: 23446 },
                    num_bytes: 136722,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000fb.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(251),
                    start: LogPosition { offset: 23446 },
                    limit: LogPosition { offset: 23475 },
                    num_bytes: 58492,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000fc.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(252),
                    start: LogPosition { offset: 23475 },
                    limit: LogPosition { offset: 23575 },
                    num_bytes: 141272,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000fd.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(253),
                    start: LogPosition { offset: 23575 },
                    limit: LogPosition { offset: 23675 },
                    num_bytes: 137722,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000fe.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(254),
                    start: LogPosition { offset: 23675 },
                    limit: LogPosition { offset: 23742 },
                    num_bytes: 100808,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000ff.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(255),
                    start: LogPosition { offset: 23742 },
                    limit: LogPosition { offset: 23842 },
                    num_bytes: 134240,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000100.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(256),
                    start: LogPosition { offset: 23842 },
                    limit: LogPosition { offset: 23942 },
                    num_bytes: 135368,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000101.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(257),
                    start: LogPosition { offset: 23942 },
                    limit: LogPosition { offset: 24029 },
                    num_bytes: 121177,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000102.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(258),
                    start: LogPosition { offset: 24029 },
                    limit: LogPosition { offset: 24129 },
                    num_bytes: 131830,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000103.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(259),
                    start: LogPosition { offset: 24129 },
                    limit: LogPosition { offset: 24229 },
                    num_bytes: 137812,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000104.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(260),
                    start: LogPosition { offset: 24229 },
                    limit: LogPosition { offset: 24301 },
                    num_bytes: 104740,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000105.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(261),
                    start: LogPosition { offset: 24301 },
                    limit: LogPosition { offset: 24401 },
                    num_bytes: 136602,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000106.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(262),
                    start: LogPosition { offset: 24401 },
                    limit: LogPosition { offset: 24485 },
                    num_bytes: 115053,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000107.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(263),
                    start: LogPosition { offset: 24485 },
                    limit: LogPosition { offset: 24585 },
                    num_bytes: 141135,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000108.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(264),
                    start: LogPosition { offset: 24585 },
                    limit: LogPosition { offset: 24685 },
                    num_bytes: 136246,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000109.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(265),
                    start: LogPosition { offset: 24685 },
                    limit: LogPosition { offset: 24785 },
                    num_bytes: 136663,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000010a.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(266),
                    start: LogPosition { offset: 24785 },
                    limit: LogPosition { offset: 24790 },
                    num_bytes: 35690,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000010b.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(267),
                    start: LogPosition { offset: 24790 },
                    limit: LogPosition { offset: 24890 },
                    num_bytes: 138674,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000010c.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(268),
                    start: LogPosition { offset: 24890 },
                    limit: LogPosition { offset: 24990 },
                    num_bytes: 140703,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000010d.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(269),
                    start: LogPosition { offset: 24990 },
                    limit: LogPosition { offset: 25045 },
                    num_bytes: 85851,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000010e.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(270),
                    start: LogPosition { offset: 25045 },
                    limit: LogPosition { offset: 25145 },
                    num_bytes: 141113,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=000000000000010f.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(271),
                    start: LogPosition { offset: 25145 },
                    limit: LogPosition { offset: 25245 },
                    num_bytes: 135896,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000110.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(272),
                    start: LogPosition { offset: 25245 },
                    limit: LogPosition { offset: 25345 },
                    num_bytes: 137036,
                    setsum: Setsum::default(),
                },
                Fragment {
                    path: "log/Bucket=0000000000000000/FragmentSeqNo=0000000000000111.parquet"
                        .to_string(),
                    seq_no: FragmentSeqNo(273),
                    start: LogPosition { offset: 25345 },
                    limit: LogPosition { offset: 25445 },
                    num_bytes: 135284,
                    setsum: Setsum::default(),
                },
            ],
            initial_offset: Some(LogPosition { offset: 1 }),
            initial_seq_no: Some(FragmentSeqNo(1)),
        };
        let Some(fragments) = LogReader::scan_from_manifest(
            &manifest,
            LogPosition::from_offset(20776),
            Limits {
                max_files: None,
                max_bytes: None,
                max_records: Some(142),
            },
        ) else {
            panic!("failed to get fragments");
        };
        eprintln!("{fragments:?}");
        assert_eq!(fragments.len(), 2);
        assert_eq!(
            fragments[0],
            Fragment {
                path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000db.parquet"
                    .to_string(),
                seq_no: FragmentSeqNo(219),
                start: LogPosition { offset: 20776 },
                limit: LogPosition { offset: 20876 },
                num_bytes: 130467,
                setsum: Setsum::default(),
            }
        );
        assert_eq!(
            fragments[1],
            Fragment {
                path: "log/Bucket=0000000000000000/FragmentSeqNo=00000000000000dc.parquet"
                    .to_string(),
                seq_no: FragmentSeqNo(220),
                start: LogPosition { offset: 20876 },
                limit: LogPosition { offset: 20918 },
                num_bytes: 78680,
                setsum: Setsum::default(),
            }
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_verify_returns_true_when_manifest_etag_matches() {
        let storage = Arc::new(chroma_storage::s3::s3_client_for_test_with_new_bucket().await);
        let prefix = "test-prefix".to_string();
        let options = LogReaderOptions::default();
        let reader = LogReader::new(options, storage.clone(), prefix.clone());

        let manifest = Manifest::new_empty("test-writer");
        Manifest::initialize_from_manifest(
            &crate::LogWriterOptions::default(),
            &storage,
            &prefix,
            manifest.clone(),
        )
        .await
        .unwrap();

        let (loaded_manifest, etag) = Manifest::load(&reader.options.throttle, &storage, &prefix)
            .await
            .unwrap()
            .unwrap();

        let manifest_and_etag = ManifestAndETag {
            manifest: loaded_manifest,
            e_tag: etag,
        };

        let result = reader.verify(&manifest_and_etag).await.unwrap();
        assert!(result, "verify should return true for matching etag");
    }

    #[tokio::test]
    async fn test_k8s_integration_verify_returns_false_when_manifest_etag_does_not_match() {
        let storage = Arc::new(chroma_storage::s3::s3_client_for_test_with_new_bucket().await);
        let prefix = "test-prefix".to_string();
        let options = LogReaderOptions::default();
        let reader = LogReader::new(options, storage.clone(), prefix.clone());

        let manifest = Manifest::new_empty("test-writer");
        Manifest::initialize_from_manifest(
            &crate::LogWriterOptions::default(),
            &storage,
            &prefix,
            manifest.clone(),
        )
        .await
        .unwrap();

        let fake_etag = chroma_storage::ETag("fake-etag-that-wont-match".to_string());
        let manifest_and_etag = ManifestAndETag {
            manifest,
            e_tag: fake_etag,
        };

        let result = reader.verify(&manifest_and_etag).await.unwrap();
        assert!(!result, "verify should return false for non-matching etag");
    }

    #[tokio::test]
    async fn test_k8s_integration_verify_handles_storage_errors_gracefully() {
        use chroma_storage::local::LocalStorage;

        let storage = Arc::new(chroma_storage::Storage::Local(LocalStorage::new(
            "./test-local",
        )));
        let prefix = "test-prefix".to_string();
        let options = LogReaderOptions::default();
        let reader = LogReader::new(options, storage, prefix);

        let manifest = Manifest::new_empty("test-writer");
        let fake_etag = chroma_storage::ETag("fake-etag".to_string());
        let manifest_and_etag = ManifestAndETag {
            manifest,
            e_tag: fake_etag,
        };

        let result = reader.verify(&manifest_and_etag).await;
        match result {
            Err(crate::Error::StorageError(storage_error)) => {
                match storage_error.as_ref() {
                    chroma_storage::StorageError::NotImplemented => {
                        // This is expected for local storage
                    }
                    _ => panic!("Unexpected storage error: {:?}", storage_error),
                }
            }
            _ => panic!("Expected storage error for local storage verify"),
        }
    }

    #[tokio::test]
    async fn test_k8s_integration_manifest_and_e_tag_returns_both_manifest_and_etag() {
        let storage = Arc::new(chroma_storage::s3::s3_client_for_test_with_new_bucket().await);
        let prefix = "test-prefix".to_string();
        let options = LogReaderOptions::default();
        let reader = LogReader::new(options, storage.clone(), prefix.clone());

        let manifest = Manifest::new_empty("test-writer");
        Manifest::initialize_from_manifest(
            &crate::LogWriterOptions::default(),
            &storage,
            &prefix,
            manifest.clone(),
        )
        .await
        .unwrap();

        let result = reader.manifest_and_e_tag().await.unwrap();
        assert!(
            result.is_some(),
            "manifest_and_e_tag should return Some when manifest exists"
        );

        let manifest_and_etag = result.unwrap();
        assert_eq!(manifest_and_etag.manifest.writer, "test-writer");
        assert!(
            !manifest_and_etag.e_tag.0.is_empty(),
            "etag should not be empty"
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_manifest_and_e_tag_returns_none_when_no_manifest() {
        let storage = Arc::new(chroma_storage::s3::s3_client_for_test_with_new_bucket().await);
        let prefix = "nonexistent-prefix".to_string();
        let options = LogReaderOptions::default();
        let reader = LogReader::new(options, storage, prefix);

        let result = reader.manifest_and_e_tag().await.unwrap();
        assert!(
            result.is_none(),
            "manifest_and_e_tag should return None when no manifest exists"
        );
    }
}
