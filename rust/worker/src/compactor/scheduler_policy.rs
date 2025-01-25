use super::CollectionAndSegments;
use crate::compactor::types::CompactionJob;

pub(crate) trait SchedulerPolicy: Send + Sync + SchedulerPolicyClone {
    fn determine(
        &self,
        collections_and_segments: Vec<CollectionAndSegments>,
        number_jobs: i32,
    ) -> Vec<CompactionJob>;
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
        mut collections_and_segments: Vec<CollectionAndSegments>,
        number_jobs: i32,
    ) -> Vec<CompactionJob> {
        collections_and_segments.sort_by(|a, b| {
            a.collection
                .last_compaction_time
                .cmp(&b.collection.last_compaction_time)
        });
        let number_tasks = if number_jobs > collections_and_segments.len() as i32 {
            collections_and_segments.len() as i32
        } else {
            number_jobs
        };
        let mut tasks = Vec::new();
        for collection_and_segments in &collections_and_segments[0..number_tasks as usize] {
            tasks.push(CompactionJob {
                collection_id: collection_and_segments.collection.collection_id,
                tenant_id: collection_and_segments.collection.tenant_id.clone(),
                offset: collection_and_segments.collection.offset,
                collection_version: collection_and_segments.collection.collection_version,
                segments: collection_and_segments.segments.clone(),
            });
        }
        tasks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_log::log::CollectionRecord;
    use chroma_types::CollectionUuid;
    use std::str::FromStr;

    #[test]
    fn test_scheduler_policy() {
        let collection_uuid_1 =
            CollectionUuid::from_str("00000000-0000-0000-0000-000000000001").unwrap();
        let collection_uuid_2 =
            CollectionUuid::from_str("00000000-0000-0000-0000-000000000002").unwrap();
        let scheduler_policy = LasCompactionTimeSchedulerPolicy {};
        let collections = vec![
            CollectionAndSegments {
                collection: CollectionRecord {
                    collection_id: collection_uuid_1,
                    tenant_id: "test".to_string(),
                    last_compaction_time: 1,
                    first_record_time: 1,
                    offset: 0,
                    collection_version: 0,
                },
                segments: vec![],
            },
            CollectionAndSegments {
                collection: CollectionRecord {
                    collection_id: collection_uuid_2,
                    tenant_id: "test".to_string(),
                    last_compaction_time: 0,
                    first_record_time: 0,
                    offset: 0,
                    collection_version: 0,
                },
                segments: vec![],
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
}
