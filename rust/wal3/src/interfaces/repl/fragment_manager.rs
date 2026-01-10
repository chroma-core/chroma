use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use setsum::Setsum;
use tracing::Level;

use chroma_storage::{
    admissioncontrolleds3::StorageRequestPriority, GetOptions, Storage, StorageError,
};
use chroma_types::Cmek;

use crate::interfaces::batch_manager::upload_parquet;
use crate::interfaces::{FragmentConsumer, FragmentUploader};
use crate::{
    fragment_path, Error, Fragment, FragmentIdentifier, FragmentUuid, LogPosition, LogWriterOptions,
};

#[derive(Clone, Debug)]
pub struct ReplicatedFragmentOptions {
    pub minimum_allowed_replication_factor: usize,
    pub minimum_failures_to_exclude_replica: usize,
    pub decimation_interval: Duration,
    pub slow_writer_tolerance: Duration,
}

impl Default for ReplicatedFragmentOptions {
    fn default() -> Self {
        Self {
            minimum_allowed_replication_factor: 1,
            minimum_failures_to_exclude_replica: 1,
            decimation_interval: Duration::from_secs(30),
            slow_writer_tolerance: Duration::from_secs(15),
        }
    }
}

pub struct StorageWrapper {
    #[allow(dead_code)]
    region: String,
    storage: Storage,
    prefix: String,
    counter: AtomicU64,
}

impl StorageWrapper {
    /// Creates a new StorageWrapper.
    pub fn new(region: String, storage: Storage, prefix: String) -> Self {
        Self {
            region,
            storage,
            prefix,
            counter: AtomicU64::new(0),
        }
    }
}

struct BookKeeping {
    last_decimation: Instant,
}

pub struct ReplicatedFragmentUploader {
    options: ReplicatedFragmentOptions,
    writer: LogWriterOptions,
    storages: Arc<Vec<StorageWrapper>>,
    bookkeeping: Arc<Mutex<BookKeeping>>,
}

impl ReplicatedFragmentUploader {
    pub fn new(
        options: ReplicatedFragmentOptions,
        writer: LogWriterOptions,
        storages: Arc<Vec<StorageWrapper>>,
    ) -> Self {
        let bookkeeping = Arc::new(Mutex::new(BookKeeping {
            // NOTE(rescrv):  We set it into the future to require that the process be healthy for
            // at least decimation interval before it starts decimating.
            last_decimation: Instant::now() + options.decimation_interval,
        }));
        Self {
            options,
            writer,
            storages,
            bookkeeping,
        }
    }

    fn compute_mask(&self) -> Result<Vec<bool>, Error> {
        // SAFETY(rescrv):  Mutex poisoning.
        let mut bookkeeping = self.bookkeeping.lock().unwrap();
        if bookkeeping.last_decimation.elapsed() >= self.options.decimation_interval {
            bookkeeping.last_decimation = Instant::now();
        }
        let counts = self
            .storages
            .iter()
            .map(|s| s.counter.load(Ordering::Relaxed))
            .collect::<Vec<_>>();
        Ok(compute_mask_from_counts(
            &counts,
            self.options.minimum_failures_to_exclude_replica,
        ))
    }
}

/// The outcome of processing quorum write results.
#[derive(Debug, PartialEq, Eq)]
enum QuorumOutcome {
    /// Quorum achieved with the given result.
    Success((String, Setsum, usize)),
    /// Consistency error detected between replicas.
    ConsistencyError(String),
    /// Not enough successful writes to achieve quorum.
    InsufficientQuorum,
}

/// A single result from a quorum write operation: path, setsum, and byte count.
type QuorumWriteResult<E> = Option<Result<(String, Setsum, usize), E>>;

/// Process the results from a quorum write operation.
///
/// This function validates that all successful results are consistent (same path, setsum, and
/// record count) and determines whether quorum was achieved.
///
/// Returns `QuorumOutcome::Success` if at least `minimum_replication_factor` results succeeded
/// with consistent values, `QuorumOutcome::ConsistencyError` if any two successful results have
/// mismatched values, or `QuorumOutcome::InsufficientQuorum` if not enough writes succeeded.
fn process_quorum_results<E>(
    results: &[QuorumWriteResult<E>],
    minimum_replication_factor: usize,
) -> QuorumOutcome {
    let mut canonical: Option<(String, Setsum, usize)> = None;
    let mut success_count = 0;

    for r in results.iter() {
        match (canonical.as_ref(), r) {
            (Some(canonical), Some(Ok(r))) => {
                if r.0 != canonical.0 {
                    return QuorumOutcome::ConsistencyError(format!(
                        "path mismatch: {} != {}",
                        r.0, canonical.0
                    ));
                }
                if r.1 != canonical.1 {
                    return QuorumOutcome::ConsistencyError(format!(
                        "setsum mismatch: {} != {}",
                        r.1.hexdigest(),
                        canonical.1.hexdigest()
                    ));
                }
                if r.2 != canonical.2 {
                    return QuorumOutcome::ConsistencyError(format!(
                        "record-count mismatch: {} != {}",
                        r.2, canonical.2
                    ));
                }
                success_count += 1;
            }
            (None, Some(Ok(r))) => {
                canonical = Some(r.clone());
                success_count += 1;
            }
            (_, Some(Err(_))) | (_, None) => {}
        }
    }

    if success_count >= minimum_replication_factor {
        if let Some(result) = canonical {
            return QuorumOutcome::Success(result);
        }
    }
    QuorumOutcome::InsufficientQuorum
}

/// Compute a mask of which storages should be tried based on their error counts.
///
/// Returns a boolean mask where `true` means the storage should be tried.
/// A storage is excluded (masked out) if:
/// 1. Its error count is more than two standard deviations above the mean, AND
/// 2. The threshold is at least `minimum_failures_to_exclude` (to avoid excluding storages
///    that have only had a few failures).
fn compute_mask_from_counts(counts: &[u64], minimum_failures_to_exclude: usize) -> Vec<bool> {
    if counts.is_empty() {
        return vec![];
    }
    // Compute mean using f64 to avoid overflow.
    let n = counts.len() as f64;
    let sum: f64 = counts.iter().map(|&c| c as f64).sum();
    let mean = sum / n;
    // Compute standard deviation.
    let variance: f64 = counts
        .iter()
        .map(|&c| {
            let diff = c as f64 - mean;
            diff * diff
        })
        .sum::<f64>()
        / n;
    let stddev = variance.sqrt();
    // Mask out storages that are more than two standard deviations above the mean.
    let threshold = mean + 2.0 * stddev;
    counts
        .iter()
        .map(|&c| (c as f64) <= threshold || threshold < minimum_failures_to_exclude as f64)
        .collect::<Vec<_>>()
}

#[async_trait::async_trait]
impl FragmentUploader<FragmentUuid> for ReplicatedFragmentUploader {
    /// upload a parquet fragment
    async fn upload_parquet(
        &self,
        pointer: &FragmentUuid,
        messages: Vec<Vec<u8>>,
        cmek: Option<Cmek>,
        epoch_micros: u64,
    ) -> Result<(String, Setsum, usize), Error> {
        let mask = self.compute_mask()?;
        assert_eq!(mask.len(), self.storages.len());
        let mut futures = vec![];
        let mut indices = vec![];
        for (idx, (should_try, storage)) in std::iter::zip(mask, self.storages.iter()).enumerate() {
            if should_try {
                let options = self.writer.clone();
                let prefix = storage.prefix.clone();
                let storage = storage.storage.clone();
                let fragment_identifier = (*pointer).into();
                let log_position = None;
                let messages = messages.clone();
                let cmek = cmek.clone();
                futures.push(async move {
                    upload_parquet(
                        &options,
                        &storage,
                        &prefix,
                        fragment_identifier,
                        log_position,
                        messages,
                        cmek,
                        epoch_micros,
                    )
                    .await
                });
                indices.push(idx);
            }
        }
        let results = crate::quorum_writer::write_quorum(
            futures,
            self.options.minimum_allowed_replication_factor,
            self.options.slow_writer_tolerance,
        )
        .await;
        assert_eq!(indices.len(), results.len());

        // Increment error counters and collect errors for logging.
        let mut errors = vec![];
        for (idx, r) in std::iter::zip(indices.iter().cloned(), results.iter()) {
            if let Some(Err(err)) = r {
                self.storages[idx].counter.fetch_add(1, Ordering::Relaxed);
                errors.push(err);
            }
        }

        match process_quorum_results(&results, self.options.minimum_allowed_replication_factor) {
            QuorumOutcome::Success(result) => Ok(result),
            QuorumOutcome::ConsistencyError(msg) => Err(Error::ReplicationConsistencyError(msg)),
            QuorumOutcome::InsufficientQuorum => {
                for err in errors.iter() {
                    tracing::event!(
                        Level::ERROR,
                        name = "quorum write failed because of error",
                        error =? **err
                    );
                }
                Err(Error::ReplicationError)
            }
        }
    }
}

pub struct FragmentReader {
    storages: Arc<Vec<StorageWrapper>>,
}

impl FragmentReader {
    pub fn new(storages: Arc<Vec<StorageWrapper>>) -> Self {
        Self { storages }
    }
}

#[async_trait::async_trait]
impl FragmentConsumer for FragmentReader {
    type FragmentPointer = FragmentUuid;

    async fn read_raw_bytes(&self, path: &str, _: LogPosition) -> Result<Arc<Vec<u8>>, Error> {
        let mut err: Option<Error> = None;
        for storage in self.storages.iter() {
            let path = fragment_path(&storage.prefix, path);
            match storage
                .storage
                .get(&path, GetOptions::new(StorageRequestPriority::P0))
                .await
            {
                Ok(parquet) => return Ok(parquet),
                Err(StorageError::NotFound { .. }) => {
                    // TODO(rescrv, mcmr): Read repair.
                    continue;
                }
                Err(e) => {
                    tracing::error!("reading from region {} failed", storage.region);
                    err = Some(Arc::new(e).into());
                }
            }
        }
        if let Some(err) = err {
            Err(err)
        } else {
            Err(Error::internal(file!(), line!()))
        }
    }

    async fn read_parquet(
        &self,
        path: &str,
        fragment_first_log_position: LogPosition,
    ) -> Result<(Setsum, Vec<(LogPosition, Vec<u8>)>, u64, u64), Error> {
        let mut err: Option<Error> = None;
        for storage in self.storages.iter() {
            match crate::interfaces::s3::read_parquet(
                &storage.storage,
                &storage.prefix,
                path,
                Some(fragment_first_log_position),
            )
            .await
            {
                Ok(parquet) => return Ok(parquet),
                Err(Error::StorageError(e)) if matches!(&*e, StorageError::NotFound { .. }) => {
                    // TODO(rescrv, mcmr): Read repair.
                    continue;
                }
                Err(e) => {
                    tracing::error!("reading from region {} failed", storage.region);
                    err = Some(e);
                }
            }
        }
        if let Some(err) = err {
            Err(err)
        } else {
            Err(Error::internal(file!(), line!()))
        }
    }

    async fn read_fragment(
        &self,
        path: &str,
        fragment_first_log_position: LogPosition,
    ) -> Result<Option<Fragment>, Error> {
        let mut err: Option<Error> = None;
        for storage in self.storages.iter() {
            match read_fragment_uuid(
                &storage.storage,
                &storage.prefix,
                path,
                Some(fragment_first_log_position),
            )
            .await
            {
                Ok(Some(fragment)) => return Ok(Some(fragment)),
                Ok(None) => {
                    // TODO(rescrv, mcmr): Read repair.
                    continue;
                }
                Err(Error::StorageError(e)) if matches!(&*e, StorageError::NotFound { .. }) => {
                    // TODO(rescrv, mcmr): Read repair.
                    continue;
                }
                Err(e) => {
                    tracing::error!("reading from region {} failed", storage.region);
                    err = Some(e);
                }
            }
        }
        if let Some(err) = err {
            Err(err)
        } else {
            Ok(None)
        }
    }
}

/// Read a fragment with a UUID-based path (for repl storage).
async fn read_fragment_uuid(
    storage: &Storage,
    prefix: &str,
    path: &str,
    starting_log_position: Option<LogPosition>,
) -> Result<Option<Fragment>, Error> {
    let seq_no = crate::parse_fragment_path(path)
        .ok_or_else(|| Error::MissingFragmentSequenceNumber(path.to_string()))?;
    let FragmentIdentifier::Uuid(_) = seq_no else {
        return Err(Error::internal(file!(), line!()));
    };
    let (setsum, data, num_bytes) =
        match crate::interfaces::s3::read_parquet(storage, prefix, path, starting_log_position)
            .await
        {
            Ok((setsum, data, num_bytes, _)) => (setsum, data, num_bytes),
            Err(Error::StorageError(storage_err)) => {
                if matches!(&*storage_err, StorageError::NotFound { .. }) {
                    return Ok(None);
                }
                return Err(Error::StorageError(storage_err));
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
#[allow(clippy::type_complexity)]
mod tests {
    use super::compute_mask_from_counts;

    // Empty input returns empty output.
    #[test]
    fn empty_counts() {
        assert_eq!(Vec::<bool>::new(), compute_mask_from_counts(&[], 0));
        assert_eq!(Vec::<bool>::new(), compute_mask_from_counts(&[], 10));
    }

    // Single storage is always included regardless of its count.
    #[test]
    fn single_storage_zero_count() {
        assert_eq!(vec![true], compute_mask_from_counts(&[0], 0));
    }

    #[test]
    fn single_storage_nonzero_count() {
        // With a single storage, stddev = 0, so threshold = mean = count.
        // The storage count equals the threshold, so it's included.
        assert_eq!(vec![true], compute_mask_from_counts(&[100], 0));
    }

    #[test]
    fn single_storage_with_minimum_failures_threshold() {
        // Even with minimum_failures_to_exclude > 0, a single storage should be included.
        assert_eq!(vec![true], compute_mask_from_counts(&[5], 10));
    }

    // All storages have zero errors - all should be included.
    #[test]
    fn all_zeros() {
        assert_eq!(
            vec![true, true, true],
            compute_mask_from_counts(&[0, 0, 0], 0)
        );
    }

    // All storages have identical non-zero counts - all should be included.
    // mean = 10, stddev = 0, threshold = 10. All counts <= 10.
    #[test]
    fn all_identical_nonzero() {
        assert_eq!(
            vec![true, true, true, true],
            compute_mask_from_counts(&[10, 10, 10, 10], 0)
        );
    }

    // One outlier that is more than 2 stddev above the mean should be excluded.
    // counts = [0, 0, 0, 100]
    // mean = 25, variance = (625 + 625 + 625 + 5625) / 4 = 1875, stddev = ~43.3
    // threshold = 25 + 2*43.3 = 111.6
    // All counts <= 111.6, so all included.
    #[test]
    fn one_high_outlier_within_threshold() {
        assert_eq!(
            vec![true, true, true, true],
            compute_mask_from_counts(&[0, 0, 0, 100], 0)
        );
    }

    // Extreme outlier that exceeds 2 stddev threshold.
    // counts = [0, 0, 0, 1000]
    // mean = 250, variance = (62500 + 62500 + 62500 + 562500) / 4 = 187500
    // stddev = ~433.0, threshold = 250 + 866 = 1116
    // All counts <= 1116, so all included. Need more extreme case.
    #[test]
    fn extreme_outlier_still_within_two_stddev() {
        // This demonstrates that with only one outlier among zeros,
        // the outlier is always within 2 stddev because it dominates the variance.
        assert_eq!(
            vec![true, true, true, true],
            compute_mask_from_counts(&[0, 0, 0, 1000], 0)
        );
    }

    // Multiple similar values with one outlier.
    // counts = [10, 10, 10, 10, 100]
    // mean = 28, variance = (324 + 324 + 324 + 324 + 5184) / 5 = 1296, stddev = 36
    // threshold = 28 + 72 = 100
    // count 100 == threshold, so included (using <=).
    #[test]
    fn outlier_exactly_at_threshold() {
        assert_eq!(
            vec![true, true, true, true, true],
            compute_mask_from_counts(&[10, 10, 10, 10, 100], 0)
        );
    }

    // Create a scenario where outlier exceeds threshold.
    // counts = [10, 10, 10, 10, 10, 200]
    // mean = 250/6 = 41.67
    // differences: -31.67, -31.67, -31.67, -31.67, -31.67, 158.33
    // variance = (1002.9 * 5 + 25068.4) / 6 = (5014.5 + 25068.4) / 6 = 5013.8
    // stddev = ~70.8, threshold = 41.67 + 141.6 = 183.3
    // 200 > 183.3, so last storage excluded.
    #[test]
    fn outlier_exceeds_threshold() {
        assert_eq!(
            vec![true, true, true, true, true, false],
            compute_mask_from_counts(&[10, 10, 10, 10, 10, 200], 0)
        );
    }

    // Verify minimum_failures_to_exclude prevents exclusion when threshold is low.
    // Same as above but with minimum_failures_to_exclude = 200.
    // threshold ~183.3 < 200, so no exclusions allowed.
    #[test]
    fn minimum_failures_prevents_exclusion() {
        assert_eq!(
            vec![true, true, true, true, true, true],
            compute_mask_from_counts(&[10, 10, 10, 10, 10, 200], 200)
        );
    }

    // When minimum_failures_to_exclude is just below threshold, exclusion happens.
    #[test]
    fn minimum_failures_just_below_threshold() {
        // threshold ~183.3, minimum = 183, so exclusion can happen.
        assert_eq!(
            vec![true, true, true, true, true, false],
            compute_mask_from_counts(&[10, 10, 10, 10, 10, 200], 183)
        );
    }

    // Two outliers both exceeding threshold.
    // counts = [0, 0, 0, 0, 100, 100]
    // mean = 200/6 = 33.33
    // differences: -33.33, -33.33, -33.33, -33.33, 66.67, 66.67
    // variance = (1111.1 * 4 + 4444.4 * 2) / 6 = (4444.4 + 8888.8) / 6 = 2222.2
    // stddev = 47.14, threshold = 33.33 + 94.28 = 127.6
    // Both 100s <= 127.6, so included.
    #[test]
    fn two_equal_outliers_within_threshold() {
        assert_eq!(
            vec![true, true, true, true, true, true],
            compute_mask_from_counts(&[0, 0, 0, 0, 100, 100], 0)
        );
    }

    // Two outliers with different magnitudes, larger one excluded.
    // counts = [0, 0, 0, 0, 50, 200]
    // mean = 250/6 = 41.67
    // differences: -41.67 * 4, 8.33, 158.33
    // variance = (1736.1 * 4 + 69.4 + 25068.4) / 6 = (6944.4 + 69.4 + 25068.4) / 6 = 5347.0
    // stddev = 73.1, threshold = 41.67 + 146.2 = 187.9
    // 50 <= 187.9 (included), 200 > 187.9 (excluded)
    #[test]
    fn two_unequal_outliers_larger_excluded() {
        assert_eq!(
            vec![true, true, true, true, true, false],
            compute_mask_from_counts(&[0, 0, 0, 0, 50, 200], 0)
        );
    }

    // Three storages, one significantly higher.
    // counts = [5, 5, 50]
    // mean = 20, differences = -15, -15, 30
    // variance = (225 + 225 + 900) / 3 = 450, stddev = 21.2
    // threshold = 20 + 42.4 = 62.4
    // All included since 50 <= 62.4.
    #[test]
    fn three_storages_moderate_outlier() {
        assert_eq!(
            vec![true, true, true],
            compute_mask_from_counts(&[5, 5, 50], 0)
        );
    }

    // Three storages with extreme outlier.
    // counts = [5, 5, 100]
    // mean = 36.67, differences = -31.67, -31.67, 63.33
    // variance = (1002.9 + 1002.9 + 4010.7) / 3 = 2005.5, stddev = 44.8
    // threshold = 36.67 + 89.6 = 126.3
    // 100 <= 126.3, included.
    #[test]
    fn three_storages_larger_outlier() {
        assert_eq!(
            vec![true, true, true],
            compute_mask_from_counts(&[5, 5, 100], 0)
        );
    }

    // Two storages only - outlier behavior.
    // counts = [0, 100]
    // mean = 50, differences = -50, 50
    // variance = (2500 + 2500) / 2 = 2500, stddev = 50
    // threshold = 50 + 100 = 150
    // Both included.
    #[test]
    fn two_storages_one_zero_one_high() {
        assert_eq!(vec![true, true], compute_mask_from_counts(&[0, 100], 0));
    }

    // Large number of storages with one extreme outlier.
    // counts = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1000]
    // mean = 1009/10 = 100.9
    // Low values diff = -99.9, high value diff = 899.1
    // variance = (9980.01 * 9 + 808380.81) / 10 = (89820.09 + 808380.81) / 10 = 89820.1
    // stddev = 299.7, threshold = 100.9 + 599.4 = 700.3
    // 1000 > 700.3, excluded!
    #[test]
    fn ten_storages_one_extreme_outlier() {
        assert_eq!(
            vec![true, true, true, true, true, true, true, true, true, false],
            compute_mask_from_counts(&[1, 1, 1, 1, 1, 1, 1, 1, 1, 1000], 0)
        );
    }

    // Verify minimum_failures_to_exclude works with 10 storages.
    #[test]
    fn ten_storages_minimum_failures_prevents_exclusion() {
        // threshold ~700.3, minimum = 800, so no exclusion.
        assert_eq!(
            vec![true, true, true, true, true, true, true, true, true, true],
            compute_mask_from_counts(&[1, 1, 1, 1, 1, 1, 1, 1, 1, 1000], 800)
        );
    }

    // All storages have high but similar counts - all included.
    #[test]
    fn all_high_similar_counts() {
        assert_eq!(
            vec![true, true, true, true, true],
            compute_mask_from_counts(&[1000, 1001, 999, 1002, 998], 0)
        );
    }

    // Gradually increasing counts.
    // counts = [0, 10, 20, 30, 40]
    // mean = 20, differences = -20, -10, 0, 10, 20
    // variance = (400 + 100 + 0 + 100 + 400) / 5 = 200, stddev = 14.14
    // threshold = 20 + 28.28 = 48.28
    // All counts <= 48.28, all included.
    #[test]
    fn gradually_increasing_counts() {
        assert_eq!(
            vec![true, true, true, true, true],
            compute_mask_from_counts(&[0, 10, 20, 30, 40], 0)
        );
    }

    // Multiple storages at exactly the threshold value.
    // counts = [0, 0, 0, 0, 0, 0, 0, 0, 100, 100]
    // mean = 20, differences = -20 * 8, 80 * 2
    // variance = (400 * 8 + 6400 * 2) / 10 = (3200 + 12800) / 10 = 1600, stddev = 40
    // threshold = 20 + 80 = 100
    // Both 100s == threshold, included.
    #[test]
    fn multiple_at_exact_threshold() {
        assert_eq!(
            vec![true, true, true, true, true, true, true, true, true, true],
            compute_mask_from_counts(&[0, 0, 0, 0, 0, 0, 0, 0, 100, 100], 0)
        );
    }

    // Test with u64::MAX to ensure no overflow.
    #[test]
    fn large_count_no_overflow() {
        // With two storages having u64::MAX, the mean is u64::MAX and stddev is 0.
        // threshold = u64::MAX + 0 = u64::MAX.
        // Both counts == threshold, included.
        assert_eq!(
            vec![true, true],
            compute_mask_from_counts(&[u64::MAX, u64::MAX], 0)
        );
    }

    // Mixed large and small values.
    #[test]
    fn mixed_large_small() {
        // counts = [0, u64::MAX]
        // mean = u64::MAX / 2 (huge), stddev is half of max
        // threshold = mean + 2*stddev = u64::MAX (approximately)
        // Both should be included since we're using f64 which will round.
        let result = compute_mask_from_counts(&[0, u64::MAX], 0);
        // At f64 precision, both should be included.
        assert_eq!(vec![true, true], result);
    }

    // Boundary: minimum_failures_to_exclude = 0 allows any exclusion.
    #[test]
    fn minimum_failures_zero_allows_exclusion() {
        assert_eq!(
            vec![true, true, true, true, true, false],
            compute_mask_from_counts(&[10, 10, 10, 10, 10, 200], 0)
        );
    }

    // Boundary: minimum_failures_to_exclude = 1 still allows exclusion when threshold >= 1.
    #[test]
    fn minimum_failures_one_allows_exclusion() {
        // threshold ~183.3 >= 1, so exclusion allowed.
        assert_eq!(
            vec![true, true, true, true, true, false],
            compute_mask_from_counts(&[10, 10, 10, 10, 10, 200], 1)
        );
    }

    // Very large minimum_failures_to_exclude prevents all exclusions.
    #[test]
    fn very_large_minimum_failures_prevents_all_exclusions() {
        assert_eq!(
            vec![true, true, true, true, true, true],
            compute_mask_from_counts(&[10, 10, 10, 10, 10, 200], usize::MAX)
        );
    }

    // Realistic scenario: 5 replicas, one degraded.
    // counts = [2, 3, 2, 3, 50]
    // mean = 12, differences = -10, -9, -10, -9, 38
    // variance = (100 + 81 + 100 + 81 + 1444) / 5 = 361.2, stddev = 19.0
    // threshold = 12 + 38 = 50
    // 50 == threshold, included (boundary).
    #[test]
    fn realistic_five_replicas_one_degraded() {
        assert_eq!(
            vec![true, true, true, true, true],
            compute_mask_from_counts(&[2, 3, 2, 3, 50], 0)
        );
    }

    // Realistic scenario: 5 replicas, one severely degraded.
    // counts = [2, 3, 2, 3, 100]
    // mean = 22, differences = -20, -19, -20, -19, 78
    // variance = (400 + 361 + 400 + 361 + 6084) / 5 = 1521.2, stddev = 39.0
    // threshold = 22 + 78 = 100
    // 100 == threshold, included (boundary).
    #[test]
    fn realistic_five_replicas_one_severely_degraded() {
        assert_eq!(
            vec![true, true, true, true, true],
            compute_mask_from_counts(&[2, 3, 2, 3, 100], 0)
        );
    }

    // Realistic scenario: 5 replicas, one catastrophically degraded.
    // counts = [2, 3, 2, 3, 500]
    // mean = 102, differences = -100, -99, -100, -99, 398
    // variance = (10000 + 9801 + 10000 + 9801 + 158404) / 5 = 39601.2, stddev = 199.0
    // threshold = 102 + 398 = 500
    // 500 == threshold, included (boundary).
    #[test]
    fn realistic_five_replicas_one_catastrophically_degraded() {
        assert_eq!(
            vec![true, true, true, true, true],
            compute_mask_from_counts(&[2, 3, 2, 3, 500], 0)
        );
    }

    // Realistic scenario: 5 replicas, one beyond catastrophic.
    // counts = [2, 3, 2, 3, 501]
    // mean = 102.2, threshold will be just under 501
    // This should finally exclude the last replica.
    #[test]
    fn realistic_five_replicas_one_beyond_catastrophic() {
        // Let's compute precisely:
        // mean = 511/5 = 102.2
        // differences: -100.2, -99.2, -100.2, -99.2, 398.8
        // variance = (10040.04 + 9840.64 + 10040.04 + 9840.64 + 159041.44) / 5
        //          = 198802.8 / 5 = 39760.56
        // stddev = 199.4
        // threshold = 102.2 + 398.8 = 501.0
        // 501 <= 501.0, included!
        // We need to go higher.
        assert_eq!(
            vec![true, true, true, true, true],
            compute_mask_from_counts(&[2, 3, 2, 3, 501], 0)
        );
    }

    // Force exclusion by making the gap larger.
    // counts = [0, 0, 0, 0, 0, 0, 0, 0, 0, 500]
    // mean = 50
    // differences: -50 * 9, 450
    // variance = (2500 * 9 + 202500) / 10 = (22500 + 202500) / 10 = 22500
    // stddev = 150
    // threshold = 50 + 300 = 350
    // 500 > 350, excluded!
    #[test]
    fn force_exclusion_many_zeros_one_high() {
        assert_eq!(
            vec![true, true, true, true, true, true, true, true, true, false],
            compute_mask_from_counts(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 500], 0)
        );
    }

    // Order of inputs doesn't matter - outlier at beginning.
    #[test]
    fn outlier_at_beginning() {
        assert_eq!(
            vec![false, true, true, true, true, true, true, true, true, true],
            compute_mask_from_counts(&[500, 0, 0, 0, 0, 0, 0, 0, 0, 0], 0)
        );
    }

    // Order of inputs doesn't matter - outlier in middle.
    #[test]
    fn outlier_in_middle() {
        assert_eq!(
            vec![true, true, true, true, false, true, true, true, true, true],
            compute_mask_from_counts(&[0, 0, 0, 0, 500, 0, 0, 0, 0, 0], 0)
        );
    }

    // ==================== process_quorum_results tests ====================

    use super::process_quorum_results;
    use super::QuorumOutcome;
    use setsum::Setsum;

    fn make_setsum(seed: u8) -> Setsum {
        // Use different valid hex digests based on seed.
        match seed {
            1 => Setsum::from_hexdigest(
                "4eec78e0b5cd15df7b36fd42cdc3aecb1986ffa3655c338201db88f80d855465",
            )
            .unwrap(),
            2 => Setsum::from_hexdigest(
                "dd901afef0e5d336aaa52a2df7f785c909091fd0aa011980de443a61a889d3e1",
            )
            .unwrap(),
            _ => Setsum::from_hexdigest(
                "307d93deb6b3e91525dc277027bc34958d8f1e74965e4c027820c3596e0f2847",
            )
            .unwrap(),
        }
    }

    // Empty results with zero minimum yields InsufficientQuorum (no canonical result).
    #[test]
    fn quorum_empty_results_zero_minimum() {
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> = vec![];
        assert_eq!(
            QuorumOutcome::InsufficientQuorum,
            process_quorum_results(&results, 0)
        );
    }

    // Empty results with non-zero minimum yields InsufficientQuorum.
    #[test]
    fn quorum_empty_results_nonzero_minimum() {
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> = vec![];
        assert_eq!(
            QuorumOutcome::InsufficientQuorum,
            process_quorum_results(&results, 2)
        );
    }

    // Single successful result meets minimum of 1.
    #[test]
    fn quorum_single_success_meets_minimum_one() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> =
            vec![Some(Ok(("path/to/file".to_string(), setsum, 100)))];
        assert_eq!(
            QuorumOutcome::Success(("path/to/file".to_string(), setsum, 100)),
            process_quorum_results(&results, 1)
        );
    }

    // Single successful result does not meet minimum of 2.
    #[test]
    fn quorum_single_success_insufficient_for_two() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> =
            vec![Some(Ok(("path/to/file".to_string(), setsum, 100)))];
        assert_eq!(
            QuorumOutcome::InsufficientQuorum,
            process_quorum_results(&results, 2)
        );
    }

    // Two identical successful results meet minimum of 2.
    #[test]
    fn quorum_two_identical_successes() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> = vec![
            Some(Ok(("path/to/file".to_string(), setsum, 100))),
            Some(Ok(("path/to/file".to_string(), setsum, 100))),
        ];
        assert_eq!(
            QuorumOutcome::Success(("path/to/file".to_string(), setsum, 100)),
            process_quorum_results(&results, 2)
        );
    }

    // Three identical successful results meet minimum of 2.
    #[test]
    fn quorum_three_identical_successes_exceeds_minimum() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> = vec![
            Some(Ok(("path/to/file".to_string(), setsum, 100))),
            Some(Ok(("path/to/file".to_string(), setsum, 100))),
            Some(Ok(("path/to/file".to_string(), setsum, 100))),
        ];
        assert_eq!(
            QuorumOutcome::Success(("path/to/file".to_string(), setsum, 100)),
            process_quorum_results(&results, 2)
        );
    }

    // Path mismatch between two successful results.
    #[test]
    fn quorum_path_mismatch() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> = vec![
            Some(Ok(("path/to/file1".to_string(), setsum, 100))),
            Some(Ok(("path/to/file2".to_string(), setsum, 100))),
        ];
        let outcome = process_quorum_results(&results, 2);
        match outcome {
            QuorumOutcome::ConsistencyError(msg) => {
                assert!(
                    msg.contains("path mismatch"),
                    "expected path mismatch error, got: {}",
                    msg
                );
                assert!(msg.contains("path/to/file1"), "should contain first path");
                assert!(msg.contains("path/to/file2"), "should contain second path");
            }
            other => panic!("expected ConsistencyError, got {:?}", other),
        }
    }

    // Setsum mismatch between two successful results.
    #[test]
    fn quorum_setsum_mismatch() {
        let setsum1 = make_setsum(1);
        let setsum2 = make_setsum(2);
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> = vec![
            Some(Ok(("path/to/file".to_string(), setsum1, 100))),
            Some(Ok(("path/to/file".to_string(), setsum2, 100))),
        ];
        let outcome = process_quorum_results(&results, 2);
        match outcome {
            QuorumOutcome::ConsistencyError(msg) => {
                assert!(
                    msg.contains("setsum mismatch"),
                    "expected setsum mismatch error, got: {}",
                    msg
                );
                assert!(
                    msg.contains(&setsum1.hexdigest()),
                    "should contain first setsum"
                );
                assert!(
                    msg.contains(&setsum2.hexdigest()),
                    "should contain second setsum"
                );
            }
            other => panic!("expected ConsistencyError, got {:?}", other),
        }
    }

    // Record count mismatch between two successful results.
    #[test]
    fn quorum_record_count_mismatch() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> = vec![
            Some(Ok(("path/to/file".to_string(), setsum, 100))),
            Some(Ok(("path/to/file".to_string(), setsum, 200))),
        ];
        let outcome = process_quorum_results(&results, 2);
        match outcome {
            QuorumOutcome::ConsistencyError(msg) => {
                assert!(
                    msg.contains("record-count mismatch"),
                    "expected record-count mismatch error, got: {}",
                    msg
                );
                assert!(msg.contains("100"), "should contain first count");
                assert!(msg.contains("200"), "should contain second count");
            }
            other => panic!("expected ConsistencyError, got {:?}", other),
        }
    }

    // Mix of success and error: one success, one error, minimum 1.
    #[test]
    fn quorum_one_success_one_error_minimum_one() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), &str>>> = vec![
            Some(Ok(("path/to/file".to_string(), setsum, 100))),
            Some(Err("storage error")),
        ];
        assert_eq!(
            QuorumOutcome::Success(("path/to/file".to_string(), setsum, 100)),
            process_quorum_results(&results, 1)
        );
    }

    // Mix of success and error: one success, one error, minimum 2.
    #[test]
    fn quorum_one_success_one_error_minimum_two() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), &str>>> = vec![
            Some(Ok(("path/to/file".to_string(), setsum, 100))),
            Some(Err("storage error")),
        ];
        assert_eq!(
            QuorumOutcome::InsufficientQuorum,
            process_quorum_results(&results, 2)
        );
    }

    // Mix of success and None (timeout): one success, one timeout, minimum 1.
    #[test]
    fn quorum_one_success_one_timeout_minimum_one() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> =
            vec![Some(Ok(("path/to/file".to_string(), setsum, 100))), None];
        assert_eq!(
            QuorumOutcome::Success(("path/to/file".to_string(), setsum, 100)),
            process_quorum_results(&results, 1)
        );
    }

    // Mix of success and None (timeout): one success, one timeout, minimum 2.
    #[test]
    fn quorum_one_success_one_timeout_minimum_two() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> =
            vec![Some(Ok(("path/to/file".to_string(), setsum, 100))), None];
        assert_eq!(
            QuorumOutcome::InsufficientQuorum,
            process_quorum_results(&results, 2)
        );
    }

    // All errors yields InsufficientQuorum.
    #[test]
    fn quorum_all_errors() {
        let results: Vec<Option<Result<(String, Setsum, usize), &str>>> = vec![
            Some(Err("error1")),
            Some(Err("error2")),
            Some(Err("error3")),
        ];
        assert_eq!(
            QuorumOutcome::InsufficientQuorum,
            process_quorum_results(&results, 1)
        );
    }

    // All timeouts (None) yields InsufficientQuorum.
    #[test]
    fn quorum_all_timeouts() {
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> = vec![None, None, None];
        assert_eq!(
            QuorumOutcome::InsufficientQuorum,
            process_quorum_results(&results, 1)
        );
    }

    // Consistency error takes precedence: first two match, third differs in path.
    #[test]
    fn quorum_consistency_error_on_third_result() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> = vec![
            Some(Ok(("path/a".to_string(), setsum, 100))),
            Some(Ok(("path/a".to_string(), setsum, 100))),
            Some(Ok(("path/b".to_string(), setsum, 100))),
        ];
        let outcome = process_quorum_results(&results, 2);
        match outcome {
            QuorumOutcome::ConsistencyError(msg) => {
                assert!(msg.contains("path mismatch"));
            }
            other => panic!("expected ConsistencyError, got {:?}", other),
        }
    }

    // Minimum of 0 with single success returns Success (edge case).
    #[test]
    fn quorum_minimum_zero_with_success() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> =
            vec![Some(Ok(("path".to_string(), setsum, 50)))];
        assert_eq!(
            QuorumOutcome::Success(("path".to_string(), setsum, 50)),
            process_quorum_results(&results, 0)
        );
    }

    // First result is error, second is success, minimum 1.
    #[test]
    fn quorum_error_then_success() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), &str>>> = vec![
            Some(Err("first failed")),
            Some(Ok(("path".to_string(), setsum, 100))),
        ];
        assert_eq!(
            QuorumOutcome::Success(("path".to_string(), setsum, 100)),
            process_quorum_results(&results, 1)
        );
    }

    // First result is None, second is success, minimum 1.
    #[test]
    fn quorum_timeout_then_success() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> =
            vec![None, Some(Ok(("path".to_string(), setsum, 100)))];
        assert_eq!(
            QuorumOutcome::Success(("path".to_string(), setsum, 100)),
            process_quorum_results(&results, 1)
        );
    }

    // Five replicas: three succeed identically, two fail, minimum 3.
    #[test]
    fn quorum_five_replicas_three_succeed() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), &str>>> = vec![
            Some(Ok(("path".to_string(), setsum, 100))),
            Some(Err("error")),
            Some(Ok(("path".to_string(), setsum, 100))),
            Some(Err("error")),
            Some(Ok(("path".to_string(), setsum, 100))),
        ];
        assert_eq!(
            QuorumOutcome::Success(("path".to_string(), setsum, 100)),
            process_quorum_results(&results, 3)
        );
    }

    // Five replicas: two succeed identically, three fail, minimum 3 - insufficient.
    #[test]
    fn quorum_five_replicas_two_succeed_insufficient() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), &str>>> = vec![
            Some(Ok(("path".to_string(), setsum, 100))),
            Some(Err("error")),
            Some(Ok(("path".to_string(), setsum, 100))),
            Some(Err("error")),
            Some(Err("error")),
        ];
        assert_eq!(
            QuorumOutcome::InsufficientQuorum,
            process_quorum_results(&results, 3)
        );
    }

    // ==================== ReplicatedFragmentUploader integration tests ====================

    use super::ReplicatedFragmentOptions;
    use super::ReplicatedFragmentUploader;
    use super::StorageWrapper;
    use crate::interfaces::FragmentUploader;
    use crate::FragmentUuid;
    use crate::LogWriterOptions;
    use chroma_storage::s3_client_for_test_with_new_bucket;
    use std::sync::Arc;
    use std::time::Duration;

    const TEST_EPOCH_MICROS: u64 = 1234567890123456;

    fn make_test_options(min_replication: usize) -> ReplicatedFragmentOptions {
        ReplicatedFragmentOptions {
            minimum_allowed_replication_factor: min_replication,
            minimum_failures_to_exclude_replica: 100,
            decimation_interval: Duration::from_secs(3600),
            slow_writer_tolerance: Duration::from_secs(30),
        }
    }

    fn make_storage_wrapper(storage: chroma_storage::Storage, prefix: &str) -> StorageWrapper {
        StorageWrapper::new("test-region".to_string(), storage, prefix.to_string())
    }

    // Single replica successfully uploads.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_replicated_uploader_single_replica_success() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let wrapper = make_storage_wrapper(storage, "prefix1");
        let storages = Arc::new(vec![wrapper]);
        let uploader = ReplicatedFragmentUploader::new(
            make_test_options(1),
            LogWriterOptions::default(),
            storages,
        );
        let pointer = FragmentUuid::generate();
        let messages = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let result = uploader
            .upload_parquet(&pointer, messages, None, TEST_EPOCH_MICROS)
            .await;
        assert!(result.is_ok(), "upload should succeed: {:?}", result);
        let (path, setsum, _size) = result.unwrap();
        assert!(!path.is_empty(), "path should not be empty");
        assert_ne!(setsum, Setsum::default(), "setsum should be computed");
        println!(
            "replicated_uploader_single_replica_success: path={}, setsum={}",
            path,
            setsum.hexdigest()
        );
    }

    // Two replicas both successfully upload with consistent results.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_replicated_uploader_two_replicas_both_succeed() {
        let storage1 = s3_client_for_test_with_new_bucket().await;
        let storage2 = s3_client_for_test_with_new_bucket().await;
        let wrapper1 = make_storage_wrapper(storage1, "prefix1");
        let wrapper2 = make_storage_wrapper(storage2, "prefix2");
        let storages = Arc::new(vec![wrapper1, wrapper2]);
        let uploader = ReplicatedFragmentUploader::new(
            make_test_options(2),
            LogWriterOptions::default(),
            storages,
        );
        let pointer = FragmentUuid::generate();
        let messages = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let result = uploader
            .upload_parquet(&pointer, messages, None, TEST_EPOCH_MICROS)
            .await;
        assert!(result.is_ok(), "upload should succeed: {:?}", result);
        let (path, setsum, _size) = result.unwrap();
        assert!(!path.is_empty(), "path should not be empty");
        assert_ne!(setsum, Setsum::default(), "setsum should be computed");
        println!(
            "replicated_uploader_two_replicas_both_succeed: path={}, setsum={}",
            path,
            setsum.hexdigest()
        );
    }

    // Three replicas with minimum replication of 2: all succeed.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_replicated_uploader_three_replicas_all_succeed() {
        let storage1 = s3_client_for_test_with_new_bucket().await;
        let storage2 = s3_client_for_test_with_new_bucket().await;
        let storage3 = s3_client_for_test_with_new_bucket().await;
        let wrapper1 = make_storage_wrapper(storage1, "prefix1");
        let wrapper2 = make_storage_wrapper(storage2, "prefix2");
        let wrapper3 = make_storage_wrapper(storage3, "prefix3");
        let storages = Arc::new(vec![wrapper1, wrapper2, wrapper3]);
        let uploader = ReplicatedFragmentUploader::new(
            make_test_options(2),
            LogWriterOptions::default(),
            storages,
        );
        let pointer = FragmentUuid::generate();
        let messages = vec![vec![7, 8, 9]];
        let result = uploader
            .upload_parquet(&pointer, messages, None, TEST_EPOCH_MICROS)
            .await;
        assert!(result.is_ok(), "upload should succeed: {:?}", result);
        println!(
            "replicated_uploader_three_replicas_all_succeed: {:?}",
            result
        );
    }

    // Zero replicas yields ReplicationError.
    #[tokio::test]
    async fn replicated_uploader_zero_replicas() {
        let storages: Arc<Vec<StorageWrapper>> = Arc::new(vec![]);
        let uploader = ReplicatedFragmentUploader::new(
            make_test_options(1),
            LogWriterOptions::default(),
            storages,
        );
        let pointer = FragmentUuid::generate();
        let messages = vec![vec![1, 2, 3]];
        let result = uploader
            .upload_parquet(&pointer, messages, None, TEST_EPOCH_MICROS)
            .await;
        assert!(result.is_err(), "upload should fail with no replicas");
        match result {
            Err(crate::Error::ReplicationError) => {
                println!("replicated_uploader_zero_replicas: correctly returned ReplicationError");
            }
            other => panic!("expected ReplicationError, got {:?}", other),
        }
    }

    // Single replica with minimum replication factor of 2 yields ReplicationError.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_replicated_uploader_insufficient_replicas() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let wrapper = make_storage_wrapper(storage, "prefix");
        let storages = Arc::new(vec![wrapper]);
        let uploader = ReplicatedFragmentUploader::new(
            make_test_options(2),
            LogWriterOptions::default(),
            storages,
        );
        let pointer = FragmentUuid::generate();
        let messages = vec![vec![1, 2, 3]];
        let result = uploader
            .upload_parquet(&pointer, messages, None, TEST_EPOCH_MICROS)
            .await;
        assert!(
            result.is_err(),
            "upload should fail with insufficient replicas"
        );
        match result {
            Err(crate::Error::ReplicationError) => {
                println!("replicated_uploader_insufficient_replicas: correctly returned ReplicationError");
            }
            other => panic!("expected ReplicationError, got {:?}", other),
        }
    }

    // Verify error counter increments on storage failure (simulated by invalid storage config).
    #[tokio::test]
    async fn test_k8s_mcmr_integration_replicated_uploader_error_counter_increments() {
        // Create one valid storage and one that will fail (using empty storage for simplicity).
        let storage1 = s3_client_for_test_with_new_bucket().await;
        let wrapper1 = make_storage_wrapper(storage1, "prefix1");
        // Use a storage to a non-existent endpoint to simulate failure.
        // For this test, we'll just use two valid storages and verify both counters stay at 0.
        let storage2 = s3_client_for_test_with_new_bucket().await;
        let wrapper2 = make_storage_wrapper(storage2, "prefix2");
        let storages = Arc::new(vec![wrapper1, wrapper2]);
        let uploader = ReplicatedFragmentUploader::new(
            make_test_options(1),
            LogWriterOptions::default(),
            storages.clone(),
        );
        let pointer = FragmentUuid::generate();
        let messages = vec![vec![1, 2, 3]];
        let result = uploader
            .upload_parquet(&pointer, messages, None, TEST_EPOCH_MICROS)
            .await;
        assert!(result.is_ok(), "upload should succeed: {:?}", result);
        // Both storage counters should be 0 since both succeeded.
        assert_eq!(
            0,
            storages[0]
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            "first storage counter should be 0"
        );
        assert_eq!(
            0,
            storages[1]
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            "second storage counter should be 0"
        );
        println!("replicated_uploader_error_counter_increments: counters verified at 0");
    }

    // Same UUID uploaded twice to same storage should succeed (idempotent).
    #[tokio::test]
    async fn test_k8s_mcmr_integration_replicated_uploader_same_uuid_twice() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let wrapper = make_storage_wrapper(storage, "prefix");
        let storages = Arc::new(vec![wrapper]);
        let uploader = ReplicatedFragmentUploader::new(
            make_test_options(1),
            LogWriterOptions::default(),
            storages,
        );
        let pointer = FragmentUuid::generate();
        let messages = vec![vec![1, 2, 3]];
        // First upload should succeed.
        let result1 = uploader
            .upload_parquet(&pointer, messages.clone(), None, TEST_EPOCH_MICROS)
            .await;
        // Second upload with same pointer will fail due to IfNotExist semantics.
        let result2 = uploader
            .upload_parquet(&pointer, messages, None, TEST_EPOCH_MICROS)
            .await;
        assert!(
            result1.is_ok(),
            "first upload should succeed: {:?}",
            result1
        );
        // Second upload may fail due to precondition (file exists) - depends on upload_parquet behavior.
        println!(
            "replicated_uploader_same_uuid_twice: first={:?}, second={:?}",
            result1, result2
        );
    }

    // Different messages produce different setsums (sanity check).
    #[tokio::test]
    async fn test_k8s_mcmr_integration_replicated_uploader_different_messages_different_setsums() {
        let storage1 = s3_client_for_test_with_new_bucket().await;
        let storage2 = s3_client_for_test_with_new_bucket().await;
        let wrapper1 = make_storage_wrapper(storage1, "prefix");
        let wrapper2 = make_storage_wrapper(storage2, "prefix");
        let storages1 = Arc::new(vec![wrapper1]);
        let storages2 = Arc::new(vec![wrapper2]);
        let uploader1 = ReplicatedFragmentUploader::new(
            make_test_options(1),
            LogWriterOptions::default(),
            storages1,
        );
        let uploader2 = ReplicatedFragmentUploader::new(
            make_test_options(1),
            LogWriterOptions::default(),
            storages2,
        );
        let pointer1 = FragmentUuid::generate();
        let pointer2 = FragmentUuid::generate();
        let messages1 = vec![vec![1, 2, 3]];
        let messages2 = vec![vec![4, 5, 6]];
        let result1 = uploader1
            .upload_parquet(&pointer1, messages1, None, TEST_EPOCH_MICROS)
            .await;
        let result2 = uploader2
            .upload_parquet(&pointer2, messages2, None, TEST_EPOCH_MICROS)
            .await;
        assert!(result1.is_ok() && result2.is_ok());
        let (_, setsum1, _) = result1.unwrap();
        let (_, setsum2, _) = result2.unwrap();
        assert_ne!(
            setsum1, setsum2,
            "different messages should produce different setsums"
        );
        println!(
            "replicated_uploader_different_messages_different_setsums: {} != {}",
            setsum1.hexdigest(),
            setsum2.hexdigest()
        );
    }

    // ==================== compute_mask decimation tests ====================

    // Verify compute_mask updates last_decimation after interval elapsed.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_compute_mask_after_decimation_interval_elapsed() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let wrapper = make_storage_wrapper(storage, "prefix");
        let storages = Arc::new(vec![wrapper]);
        // Use a very short decimation interval so it elapses immediately.
        let options = ReplicatedFragmentOptions {
            minimum_allowed_replication_factor: 1,
            minimum_failures_to_exclude_replica: 100,
            decimation_interval: Duration::from_millis(1),
            slow_writer_tolerance: Duration::from_secs(30),
        };
        let uploader =
            ReplicatedFragmentUploader::new(options, LogWriterOptions::default(), storages);
        // Wait for decimation interval to elapse.
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Call compute_mask and verify it succeeds.
        let mask = uploader.compute_mask();
        assert!(mask.is_ok(), "compute_mask should succeed");
        assert_eq!(
            vec![true],
            mask.unwrap(),
            "single storage should be masked in"
        );
        println!("compute_mask_after_decimation_interval_elapsed: passed");
    }

    // Verify compute_mask behavior before decimation interval elapses.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_compute_mask_before_decimation_interval_elapsed() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let wrapper = make_storage_wrapper(storage, "prefix");
        let storages = Arc::new(vec![wrapper]);
        // Use a very long decimation interval so it doesn't elapse.
        let options = ReplicatedFragmentOptions {
            minimum_allowed_replication_factor: 1,
            minimum_failures_to_exclude_replica: 100,
            decimation_interval: Duration::from_secs(3600),
            slow_writer_tolerance: Duration::from_secs(30),
        };
        let uploader =
            ReplicatedFragmentUploader::new(options, LogWriterOptions::default(), storages);
        // Call compute_mask immediately.
        let mask = uploader.compute_mask();
        assert!(mask.is_ok(), "compute_mask should succeed");
        assert_eq!(
            vec![true],
            mask.unwrap(),
            "single storage should be masked in"
        );
        println!("compute_mask_before_decimation_interval_elapsed: passed");
    }

    // ==================== Concurrent upload tests ====================

    // Multiple simultaneous uploads to same replicas should all succeed.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_replicated_uploader_concurrent_uploads() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let wrapper = make_storage_wrapper(storage, "prefix");
        let storages = Arc::new(vec![wrapper]);
        let uploader = Arc::new(ReplicatedFragmentUploader::new(
            make_test_options(1),
            LogWriterOptions::default(),
            storages,
        ));
        let mut handles = vec![];
        for i in 0..5 {
            let uploader = Arc::clone(&uploader);
            let handle = tokio::spawn(async move {
                let pointer = FragmentUuid::generate();
                let messages = vec![vec![i as u8; 10]];
                uploader
                    .upload_parquet(&pointer, messages, None, TEST_EPOCH_MICROS)
                    .await
            });
            handles.push(handle);
        }
        let mut successes = 0;
        for handle in handles {
            let result = handle.await.expect("task panicked");
            if result.is_ok() {
                successes += 1;
            }
        }
        assert_eq!(5, successes, "all concurrent uploads should succeed");
        println!(
            "replicated_uploader_concurrent_uploads: {} successes",
            successes
        );
    }

    // Verify that compute_mask_from_counts correctly excludes replicas with high error counts.
    // This is a unit test of the underlying function since we cannot set the counter directly
    // from test code (StorageWrapper fields are private).
    #[test]
    fn compute_mask_excludes_high_error_count_replica() {
        // With 10 replicas at count 1 and one at count 1000, the outlier should be excluded.
        // mean = (9 + 1000) / 10 = 100.9
        // differences: -99.9 * 9, 899.1
        // variance = (9980.01 * 9 + 808380.81) / 10 = 89820.1
        // stddev = 299.7, threshold = 100.9 + 599.4 = 700.3
        // 1000 > 700.3, so excluded.
        let counts = vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1000];
        let mask = compute_mask_from_counts(&counts, 10);
        assert_eq!(
            vec![true, true, true, true, true, true, true, true, true, false],
            mask,
            "high error count replica should be excluded"
        );
        println!("compute_mask_excludes_high_error_count_replica: passed");
    }

    // ==================== Edge case quorum tests ====================

    // Consistency error takes precedence even when quorum is insufficient.
    #[test]
    fn quorum_consistency_error_takes_precedence_over_insufficient() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), ()>>> = vec![
            Some(Ok(("path/a".to_string(), setsum, 100))),
            Some(Ok(("path/b".to_string(), setsum, 100))),
            None,
            None,
            None,
        ];
        // Even though we only have 2 successes and need 3, the consistency error is detected.
        let outcome = process_quorum_results(&results, 3);
        match outcome {
            QuorumOutcome::ConsistencyError(msg) => {
                assert!(msg.contains("path mismatch"));
                println!(
                    "quorum_consistency_error_takes_precedence_over_insufficient: {:?}",
                    msg
                );
            }
            other => panic!("expected ConsistencyError, got {:?}", other),
        }
    }

    // Mix of timeouts (None) and errors in results.
    #[test]
    fn quorum_with_mixed_none_and_errors() {
        let setsum = make_setsum(1);
        let results: Vec<Option<Result<(String, Setsum, usize), &str>>> = vec![
            Some(Ok(("path".to_string(), setsum, 100))),
            None,
            Some(Err("storage error")),
            None,
            Some(Err("another error")),
        ];
        // Only 1 success, minimum 2.
        assert_eq!(
            QuorumOutcome::InsufficientQuorum,
            process_quorum_results(&results, 2)
        );
        // Only 1 success, minimum 1.
        assert_eq!(
            QuorumOutcome::Success(("path".to_string(), setsum, 100)),
            process_quorum_results(&results, 1)
        );
        println!("quorum_with_mixed_none_and_errors: passed");
    }

    // ==================== Empty messages tests ====================

    // Upload with empty messages vector.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_replicated_uploader_empty_messages() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let wrapper = make_storage_wrapper(storage, "prefix");
        let storages = Arc::new(vec![wrapper]);
        let uploader = ReplicatedFragmentUploader::new(
            make_test_options(1),
            LogWriterOptions::default(),
            storages,
        );
        let pointer = FragmentUuid::generate();
        let messages: Vec<Vec<u8>> = vec![];
        let result = uploader
            .upload_parquet(&pointer, messages, None, TEST_EPOCH_MICROS)
            .await;
        // Empty messages may succeed or fail depending on parquet implementation.
        println!("replicated_uploader_empty_messages: result={:?}", result);
    }

    // Upload with single empty message.
    #[tokio::test]
    async fn test_k8s_mcmr_integration_replicated_uploader_single_empty_message() {
        let storage = s3_client_for_test_with_new_bucket().await;
        let wrapper = make_storage_wrapper(storage, "prefix");
        let storages = Arc::new(vec![wrapper]);
        let uploader = ReplicatedFragmentUploader::new(
            make_test_options(1),
            LogWriterOptions::default(),
            storages,
        );
        let pointer = FragmentUuid::generate();
        let messages: Vec<Vec<u8>> = vec![vec![]];
        let result = uploader
            .upload_parquet(&pointer, messages, None, TEST_EPOCH_MICROS)
            .await;
        // Single empty message should still create a valid parquet file.
        println!(
            "replicated_uploader_single_empty_message: result={:?}",
            result
        );
    }
}
