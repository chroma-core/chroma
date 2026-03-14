use chroma_log::CollectionRecord;
use chroma_types::DatabaseName;
use rand::seq::SliceRandom;
use rand::thread_rng;

use crate::compactor::types::CompactionJob;

pub(crate) trait SchedulerPolicy: Send + Sync + SchedulerPolicyClone {
    fn determine(&self, collections: Vec<CollectionRecord>, number_jobs: i32)
        -> Vec<CompactionJob>;
}

pub(crate) trait SchedulerPolicyClone {
    fn clone_box(&self) -> Box<dyn SchedulerPolicy>;
}

impl<T> SchedulerPolicyClone for T
where
    T: 'static + SchedulerPolicy + Clone,
{
    fn clone_box(&self) -> Box<dyn SchedulerPolicy> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn SchedulerPolicy> {
    fn clone(&self) -> Box<dyn SchedulerPolicy> {
        self.clone_box()
    }
}

#[derive(Clone)]
pub(crate) struct LasCompactionTimeSchedulerPolicy {}

impl SchedulerPolicy for LasCompactionTimeSchedulerPolicy {
    fn determine(
        &self,
        collections: Vec<CollectionRecord>,
        number_jobs: i32,
    ) -> Vec<CompactionJob> {
        let mut collections = collections;
        collections.sort_by(|a, b| a.last_compaction_time.cmp(&b.last_compaction_time));
        let number_tasks = if number_jobs > collections.len() as i32 {
            collections.len() as i32
        } else {
            number_jobs
        };
        let mut tasks = Vec::new();
        for collection in &collections[0..number_tasks as usize] {
            let database_name = match DatabaseName::new(collection.database_name.clone()) {
                Some(db_name) => db_name,
                None => {
                    tracing::warn!(
                        "Invalid database name for collection {}: {}",
                        collection.collection_id,
                        collection.database_name
                    );
                    continue;
                }
            };
            tasks.push(CompactionJob {
                collection_id: collection.collection_id,
                database_name,
                collection_size_bytes: collection.collection_logical_size_bytes,
            });
        }
        tasks
    }
}

/// A scheduler policy that bounds the total memory usage of concurrent compaction jobs.
///
/// This policy:
/// 1. Randomly shuffles collections to provide fairness across collections
/// 2. Respects the maximum number of concurrent jobs
/// 3. Respects the maximum total size (in bytes) of collections being compacted
///
/// The size limit is enforced using `collection_logical_size_bytes` as a proxy for
/// memory usage during compaction. When both job count and size limits are set,
/// both constraints are enforced (the stricter one wins).
#[derive(Clone)]
pub(crate) struct MemoryBoundedSchedulerPolicy {
    /// Maximum total size in bytes of all collections being compacted.
    /// When 0, this limit is disabled.
    max_total_size_bytes: u64,
    /// Current total size in bytes of collections already in-flight.
    current_in_flight_size_bytes: u64,
}

impl MemoryBoundedSchedulerPolicy {
    pub(crate) fn new(max_total_size_bytes: u64, current_in_flight_size_bytes: u64) -> Self {
        Self {
            max_total_size_bytes,
            current_in_flight_size_bytes,
        }
    }
}

impl SchedulerPolicy for MemoryBoundedSchedulerPolicy {
    fn determine(
        &self,
        collections: Vec<CollectionRecord>,
        number_jobs: i32,
    ) -> Vec<CompactionJob> {
        // Shuffle collections randomly for fairness
        let mut collections = collections;
        collections.shuffle(&mut thread_rng());

        let mut tasks = Vec::new();
        let mut cumulative_size = self.current_in_flight_size_bytes;
        let size_limit_enabled = self.max_total_size_bytes > 0;
        // Starvation prevention: allow at least one job when nothing is in-flight.
        // We track skipped collections for starvation prevention fallback.
        let nothing_in_flight = self.current_in_flight_size_bytes == 0;
        let mut first_skipped_collection: Option<CollectionRecord> = None;

        for collection in collections {
            // Stop if we've reached the job limit
            if tasks.len() >= number_jobs as usize {
                break;
            }

            let collection_size = collection.collection_logical_size_bytes;

            // Check size limit if enabled
            if size_limit_enabled {
                // If adding this collection would exceed the limit, skip it
                let would_exceed = cumulative_size
                    .checked_add(collection_size)
                    .map_or(true, |total| total > self.max_total_size_bytes);

                if would_exceed {
                    // Save the first skipped collection for potential starvation prevention
                    if first_skipped_collection.is_none() {
                        first_skipped_collection = Some(collection);
                    }
                    // Continue checking other collections in case a smaller one fits
                    continue;
                }
            }

            let database_name = match DatabaseName::new(collection.database_name.clone()) {
                Some(db_name) => db_name,
                None => {
                    tracing::warn!(
                        "Invalid database name for collection {}: {}",
                        collection.collection_id,
                        collection.database_name
                    );
                    continue;
                }
            };

            cumulative_size = cumulative_size.saturating_add(collection_size);
            tasks.push(CompactionJob {
                collection_id: collection.collection_id,
                database_name,
                collection_size_bytes: collection_size,
            });
        }

        // Starvation prevention: if no tasks were selected and nothing is in-flight,
        // allow one collection even if it exceeds the limit to prevent deadlock.
        if tasks.is_empty() && nothing_in_flight {
            if let Some(collection) = first_skipped_collection {
                if let Some(database_name) = DatabaseName::new(collection.database_name.clone()) {
                    tasks.push(CompactionJob {
                        collection_id: collection.collection_id,
                        database_name,
                        collection_size_bytes: collection.collection_logical_size_bytes,
                    });
                }
            }
        }

        tasks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::CollectionUuid;
    use std::str::FromStr;

    fn make_collection(id: &str, size_bytes: u64) -> CollectionRecord {
        CollectionRecord {
            collection_id: CollectionUuid::from_str(id).unwrap(),
            database_name: "test_db".to_string(),
            tenant_id: "test".to_string(),
            last_compaction_time: 0,
            first_record_time: 0,
            offset: 0,
            collection_version: 0,
            collection_logical_size_bytes: size_bytes,
        }
    }

    #[test]
    fn test_scheduler_policy() {
        let collection_uuid_1 =
            CollectionUuid::from_str("00000000-0000-0000-0000-000000000001").unwrap();
        let collection_uuid_2 =
            CollectionUuid::from_str("00000000-0000-0000-0000-000000000002").unwrap();
        let scheduler_policy = LasCompactionTimeSchedulerPolicy {};
        let collections = vec![
            CollectionRecord {
                collection_id: collection_uuid_1,
                database_name: "test_db".to_string(),
                tenant_id: "test".to_string(),
                last_compaction_time: 1,
                first_record_time: 1,
                offset: 0,
                collection_version: 0,
                collection_logical_size_bytes: 100,
            },
            CollectionRecord {
                collection_id: collection_uuid_2,
                database_name: "test_db".to_string(),
                tenant_id: "test".to_string(),
                last_compaction_time: 0,
                first_record_time: 0,
                offset: 0,
                collection_version: 0,
                collection_logical_size_bytes: 100,
            },
        ];
        let jobs = scheduler_policy.determine(collections.clone(), 1);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].collection_id, collection_uuid_2);

        let jobs = scheduler_policy.determine(collections.clone(), 2);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].collection_id, collection_uuid_2);
        assert_eq!(jobs[1].collection_id, collection_uuid_1);
    }

    #[test]
    fn test_las_compaction_policy_includes_size() {
        let scheduler_policy = LasCompactionTimeSchedulerPolicy {};
        let collections = vec![make_collection(
            "00000000-0000-0000-0000-000000000001",
            12345,
        )];
        let jobs = scheduler_policy.determine(collections, 1);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].collection_size_bytes, 12345);
    }

    // =========================================================================
    // MemoryBoundedSchedulerPolicy Tests
    // =========================================================================

    #[test]
    fn test_memory_bounded_policy_respects_size_limit() {
        // Create collections with known sizes
        let collections = vec![
            make_collection("00000000-0000-0000-0000-000000000001", 500),
            make_collection("00000000-0000-0000-0000-000000000002", 500),
            make_collection("00000000-0000-0000-0000-000000000003", 500),
        ];

        // With a limit of 1000 bytes and no in-flight jobs, should accept at most 2 collections
        let policy = MemoryBoundedSchedulerPolicy::new(1000, 0);
        let jobs = policy.determine(collections, 10);

        // Due to random shuffling, we can't predict which collections are selected,
        // but we know the total size should not exceed 1000
        let total_size: u64 = jobs.iter().map(|j| j.collection_size_bytes).sum();
        assert!(
            total_size <= 1000,
            "Total size {} exceeds limit 1000",
            total_size
        );
        assert!(
            jobs.len() <= 2,
            "Expected at most 2 jobs, got {}",
            jobs.len()
        );
    }

    #[test]
    fn test_memory_bounded_policy_respects_job_limit() {
        let collections = vec![
            make_collection("00000000-0000-0000-0000-000000000001", 100),
            make_collection("00000000-0000-0000-0000-000000000002", 100),
            make_collection("00000000-0000-0000-0000-000000000003", 100),
        ];

        // Even with a high size limit, should respect job count limit
        let policy = MemoryBoundedSchedulerPolicy::new(10000, 0);
        let jobs = policy.determine(collections, 2);

        assert_eq!(jobs.len(), 2, "Should respect job count limit of 2");
    }

    #[test]
    fn test_memory_bounded_policy_accounts_for_in_flight_size() {
        let collections = vec![
            make_collection("00000000-0000-0000-0000-000000000001", 500),
            make_collection("00000000-0000-0000-0000-000000000002", 500),
        ];

        // With 800 bytes already in flight and a 1000 byte limit,
        // should only accept collections that fit within remaining 200 bytes
        let policy = MemoryBoundedSchedulerPolicy::new(1000, 800);
        let jobs = policy.determine(collections, 10);

        // Neither 500-byte collection should fit
        assert_eq!(
            jobs.len(),
            0,
            "No collections should fit within remaining budget"
        );
    }

    #[test]
    fn test_memory_bounded_policy_allows_at_least_one_when_empty() {
        let collections = vec![make_collection(
            "00000000-0000-0000-0000-000000000001",
            2000,
        )];

        // Even if the collection exceeds the limit, allow at least one
        // to prevent starvation when nothing is in flight
        let policy = MemoryBoundedSchedulerPolicy::new(1000, 0);
        let jobs = policy.determine(collections, 10);

        assert_eq!(
            jobs.len(),
            1,
            "Should allow at least one job to prevent starvation"
        );
    }

    #[test]
    fn test_memory_bounded_policy_disabled_when_limit_is_zero() {
        let collections = vec![
            make_collection("00000000-0000-0000-0000-000000000001", 1000000),
            make_collection("00000000-0000-0000-0000-000000000002", 1000000),
            make_collection("00000000-0000-0000-0000-000000000003", 1000000),
        ];

        // When max_total_size_bytes is 0, size limit is disabled
        let policy = MemoryBoundedSchedulerPolicy::new(0, 0);
        let jobs = policy.determine(collections, 10);

        assert_eq!(jobs.len(), 3, "Should accept all jobs when size limit is 0");
    }

    #[test]
    fn test_memory_bounded_policy_concurrent_jobs_and_size_interact() {
        // Test that both limits are enforced and the stricter one wins
        let collections = vec![
            make_collection("00000000-0000-0000-0000-000000000001", 100),
            make_collection("00000000-0000-0000-0000-000000000002", 100),
            make_collection("00000000-0000-0000-0000-000000000003", 100),
            make_collection("00000000-0000-0000-0000-000000000004", 100),
        ];

        // Job limit of 3, size limit of 250 (fits 2 collections)
        let policy = MemoryBoundedSchedulerPolicy::new(250, 0);
        let jobs = policy.determine(collections.clone(), 3);

        let total_size: u64 = jobs.iter().map(|j| j.collection_size_bytes).sum();
        assert!(
            total_size <= 250,
            "Total size {} exceeds limit 250",
            total_size
        );
        assert!(jobs.len() <= 2, "Size limit should be stricter than job limit");

        // Now flip: size limit of 500 (fits 4+), job limit of 2
        let policy = MemoryBoundedSchedulerPolicy::new(500, 0);
        let jobs = policy.determine(collections, 2);

        assert_eq!(jobs.len(), 2, "Job limit should be stricter than size limit");
    }

    #[test]
    fn test_memory_bounded_policy_skips_large_finds_smaller() {
        // Test that policy skips large collections and finds smaller ones that fit
        let collections = vec![
            make_collection("00000000-0000-0000-0000-000000000001", 800), // too big
            make_collection("00000000-0000-0000-0000-000000000002", 100), // fits
            make_collection("00000000-0000-0000-0000-000000000003", 100), // fits
        ];

        let policy = MemoryBoundedSchedulerPolicy::new(300, 0);

        // Run multiple times since shuffling is random
        let mut found_multiple = false;
        for _ in 0..50 {
            let jobs = policy.determine(collections.clone(), 10);
            // Should always respect size limit
            let total_size: u64 = jobs.iter().map(|j| j.collection_size_bytes).sum();
            assert!(
                total_size <= 300,
                "Total size {} exceeds limit 300",
                total_size
            );
            if jobs.len() >= 2 {
                found_multiple = true;
            }
        }
        // With random shuffling, we should eventually find a case where
        // both small collections are selected
        assert!(
            found_multiple,
            "Policy should be able to select multiple small collections"
        );
    }

    #[test]
    fn test_memory_bounded_policy_all_collections_selected_when_fits() {
        let collections = vec![
            make_collection("00000000-0000-0000-0000-000000000001", 100),
            make_collection("00000000-0000-0000-0000-000000000002", 100),
            make_collection("00000000-0000-0000-0000-000000000003", 100),
        ];

        // All collections fit within the limit
        let policy = MemoryBoundedSchedulerPolicy::new(500, 0);
        let jobs = policy.determine(collections, 10);

        assert_eq!(
            jobs.len(),
            3,
            "All collections should be selected when they fit"
        );
    }

    #[test]
    fn test_memory_bounded_policy_size_overflow_protection() {
        // Test that we handle potential overflow safely
        let collections = vec![
            make_collection("00000000-0000-0000-0000-000000000001", u64::MAX - 100),
            make_collection("00000000-0000-0000-0000-000000000002", 200),
        ];

        // This would overflow if not handled properly
        let policy = MemoryBoundedSchedulerPolicy::new(u64::MAX, 0);
        let jobs = policy.determine(collections, 10);

        // First collection should be selected (starvation prevention)
        // Second should be skipped due to overflow protection
        assert!(jobs.len() >= 1, "Should handle overflow gracefully");
    }
}
