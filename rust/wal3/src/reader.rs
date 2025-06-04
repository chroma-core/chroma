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

fn ranges_overlap(lhs: (LogPosition, LogPosition), rhs: (LogPosition, LogPosition)) -> bool {
    lhs.0 <= rhs.1 && rhs.0 <= lhs.1
}

/// Limits allows encoding things like offset, timestamp, and byte size limits for the read.
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Limits {
    pub max_files: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_records: Option<u64>,
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
        Ok(Self::post_process_fragments(fragments, from, limits))
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
        Some(Self::post_process_fragments(fragments, from, limits))
    }

    fn post_process_fragments(
        mut fragments: Vec<Fragment>,
        from: LogPosition,
        limits: Limits,
    ) -> Vec<Fragment> {
        fragments.sort_by_key(|f| f.start.offset());
        if let Some(max_files) = limits.max_files {
            if fragments.len() as u64 > max_files {
                tracing::info!("truncating to {} files from {}", max_files, fragments.len());
                fragments.truncate(max_files as usize);
            }
        }
        while fragments.len() > 1
            // NOTE(rescrv):  We take the start of the last fragment, because if there are enough
            // records without it we can pop.
            && fragments[fragments.len() - 1].start - from
                > limits.max_records.unwrap_or(u64::MAX)
        {
            tracing::info!(
                "truncating to {} files because records restrictions",
                fragments.len() - 1
            );
            fragments.pop();
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

        let result = LogReader::post_process_fragments(fragments.clone(), from, limits);

        // With the fix: last fragment start (200) - from (125) = 75 records
        // This should be under the 100 record limit, so all fragments should remain
        assert_eq!(result.len(), 3);

        // Test case that would fail with the old bug:
        // If we were using fragments[0].start (100) instead of from (125),
        // then: last fragment start (200) - fragments[0].start (100) = 100 records
        // This would equal the limit, but the actual span from 'from' is only 75
        let limits_strict = Limits {
            max_files: None,
            max_bytes: None,
            max_records: Some(74), // Just under the actual span from 'from'
        };

        let result_strict =
            LogReader::post_process_fragments(fragments.clone(), from, limits_strict);

        // With the fix: 200 - 125 = 75 > 74, so last fragment should be removed
        assert_eq!(result_strict.len(), 2);
        assert_eq!(result_strict[0].seq_no, FragmentSeqNo(1));
        assert_eq!(result_strict[1].seq_no, FragmentSeqNo(2));
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

        let result = LogReader::post_process_fragments(fragments.clone(), from, limits);

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

        // Test the edge case where the old bug would have incorrectly calculated:
        // Old bug would use: fragments[1].start (101) - fragments[0].start (1) = 100 records
        // If max_records was 99, old code would incorrectly remove the second fragment
        let limits_edge_case = Limits {
            max_files: None,
            max_bytes: None,
            max_records: Some(50), // Just under the actual span from 'from' (51)
        };

        let result_edge =
            LogReader::post_process_fragments(fragments.clone(), from, limits_edge_case);

        // With the fix: 101 - 50 = 51 > 50, so the second fragment should be removed
        assert_eq!(
            result_edge.len(),
            1,
            "Only first fragment should remain with 50 record limit"
        );
        assert_eq!(result_edge[0].seq_no, FragmentSeqNo(1));
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
            acc_bytes: 2000,
            writer: "test-writer".to_string(),
            snapshots: vec![],
            fragments: fragments.clone(),
            initial_offset: Some(LogPosition::from_offset(1)),
        };

        // Boundary case 1: Request exactly at the manifest limit
        let from = LogPosition::from_offset(100);
        let limits = Limits {
            max_files: None,
            max_bytes: None,
            max_records: Some(100), // Would need data up to exactly offset 200
        };
        let result = LogReader::scan_from_manifest(&manifest, from, limits.clone());
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
            acc_bytes: 0,
            writer: "test-writer".to_string(),
            snapshots: vec![],
            fragments: vec![],
            initial_offset: None,
        };
        let result_empty = LogReader::scan_from_manifest(
            &empty_manifest,
            LogPosition::from_offset(0),
            limits.clone(),
        );
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
}
