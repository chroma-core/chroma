use crate::compactor::types::CompactionJob;
use crate::log::log::CollectionRecord;

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
            tasks.push(CompactionJob {
                collection_id: collection.id.clone(),
                tenant_id: collection.tenant_id.clone(),
                offset: collection.offset,
            });
        }
        tasks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheduler_policy() {
        let scheduler_policy = LasCompactionTimeSchedulerPolicy {};
        let collections = vec![
            CollectionRecord {
                id: "test1".to_string(),
                tenant_id: "test".to_string(),
                last_compaction_time: 1,
                first_record_time: 1,
                offset: 0,
            },
            CollectionRecord {
                id: "test2".to_string(),
                tenant_id: "test".to_string(),
                last_compaction_time: 0,
                first_record_time: 0,
                offset: 0,
            },
        ];
        let jobs = scheduler_policy.determine(collections.clone(), 1);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].collection_id, "test2");

        let jobs = scheduler_policy.determine(collections.clone(), 2);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].collection_id, "test2");
        assert_eq!(jobs[1].collection_id, "test1");
    }
}
