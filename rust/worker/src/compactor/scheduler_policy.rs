use chroma_log::CollectionRecord;
use chroma_types::DatabaseName;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

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
            });
        }
        tasks
    }
}

#[derive(Clone)]
pub(crate) struct RandomSchedulerPolicy {}

impl SchedulerPolicy for RandomSchedulerPolicy {
    fn determine(
        &self,
        collections: Vec<CollectionRecord>,
        number_jobs: i32,
    ) -> Vec<CompactionJob> {
        let mut collections = collections;
        let mut rng = rand::thread_rng();
        collections.shuffle(&mut rng);

        let number_tasks = number_jobs.min(collections.len() as i32) as usize;
        let mut tasks = Vec::new();
        for collection in &collections[..number_tasks] {
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
            });
        }
        tasks
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SchedulerPolicyConfig {
    LeastRecentlyCompacted,
    #[default]
    Random,
}

impl From<&SchedulerPolicyConfig> for Box<dyn SchedulerPolicy> {
    fn from(config: &SchedulerPolicyConfig) -> Self {
        match config {
            SchedulerPolicyConfig::LeastRecentlyCompacted => {
                Box::new(LasCompactionTimeSchedulerPolicy {})
            }
            SchedulerPolicyConfig::Random => Box::new(RandomSchedulerPolicy {}),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::CollectionUuid;
    use std::collections::HashSet;
    use std::str::FromStr;

    fn make_record(uuid_suffix: u32, last_compaction_time: i64) -> CollectionRecord {
        CollectionRecord {
            collection_id: CollectionUuid::from_str(&format!(
                "00000000-0000-0000-0000-{:012}",
                uuid_suffix
            ))
            .unwrap(),
            database_name: "test_db".to_string(),
            tenant_id: "test".to_string(),
            last_compaction_time,
            first_record_time: 0,
            offset: 0,
            collection_version: 0,
            collection_logical_size_bytes: 100,
        }
    }

    #[test]
    fn least_recently_compacted_orders_by_compaction_time() {
        let policy = LasCompactionTimeSchedulerPolicy {};
        let collections = vec![make_record(1, 1), make_record(2, 0)];

        let jobs = policy.determine(collections.clone(), 1);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].collection_id, collections[1].collection_id);

        let jobs = policy.determine(collections.clone(), 2);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].collection_id, collections[1].collection_id);
        assert_eq!(jobs[1].collection_id, collections[0].collection_id);
    }

    #[test]
    fn random_policy_returns_correct_count() {
        let policy = RandomSchedulerPolicy {};
        let collections: Vec<_> = (1..=5).map(|i| make_record(i, 0)).collect();

        let jobs = policy.determine(collections, 3);
        assert_eq!(jobs.len(), 3);
    }

    #[test]
    fn random_policy_caps_at_collection_count() {
        let policy = RandomSchedulerPolicy {};
        let collections: Vec<_> = (1..=2).map(|i| make_record(i, 0)).collect();

        let jobs = policy.determine(collections, 10);
        assert_eq!(jobs.len(), 2);
    }

    #[test]
    fn random_policy_empty_input() {
        let policy = RandomSchedulerPolicy {};
        let jobs = policy.determine(vec![], 5);
        assert!(jobs.is_empty());
    }

    #[test]
    fn random_policy_returns_all_inputs_when_jobs_equals_len() {
        let policy = RandomSchedulerPolicy {};
        let collections: Vec<_> = (1..=4).map(|i| make_record(i, 0)).collect();
        let expected_ids: HashSet<_> = collections.iter().map(|c| c.collection_id).collect();

        let jobs = policy.determine(collections, 4);
        let actual_ids: HashSet<_> = jobs.iter().map(|j| j.collection_id).collect();
        assert_eq!(actual_ids, expected_ids);
    }

    #[test]
    fn random_policy_skips_invalid_database_name() {
        let policy = RandomSchedulerPolicy {};
        let mut record = make_record(1, 0);
        record.database_name = "".to_string();
        let collections = vec![record, make_record(2, 0)];

        let jobs = policy.determine(collections, 2);
        assert_eq!(jobs.len(), 1);
        assert_eq!(
            jobs[0].collection_id,
            CollectionUuid::from_str("00000000-0000-0000-0000-000000000002").unwrap()
        );
    }

    #[test]
    fn scheduler_policy_config_serde_round_trip() {
        let config: SchedulerPolicyConfig = serde_json::from_str("\"random\"").unwrap();
        assert_eq!(config, SchedulerPolicyConfig::Random);

        let config: SchedulerPolicyConfig =
            serde_json::from_str("\"least_recently_compacted\"").unwrap();
        assert_eq!(config, SchedulerPolicyConfig::LeastRecentlyCompacted);
    }

    #[test]
    fn scheduler_policy_config_default_is_random() {
        assert_eq!(
            SchedulerPolicyConfig::default(),
            SchedulerPolicyConfig::Random
        );
    }
}
