use crate::compactor::log::CollectionRecord;
use crate::compactor::log::Log;
use crate::compactor::task::Task;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use super::runtime::Runnable;

#[derive(Clone)]
pub(crate) struct Scheduler {
    log: Arc<RwLock<dyn Log + Send + Sync>>,
    policy: SchedulerPolicy,
    task_queue: Arc<Mutex<Vec<Task>>>,
    max_queue_size: usize,
}

impl Scheduler {
    pub(crate) fn new(
        log: Arc<RwLock<dyn Log + Send + Sync>>,
        policy: SchedulerPolicy,
        max_queue_size: usize,
    ) -> Scheduler {
        Scheduler {
            log,
            policy,
            task_queue: Arc::new(Mutex::new(Vec::with_capacity(max_queue_size))),
            max_queue_size,
        }
    }

    pub(crate) fn schedule(&self, collections: Vec<CollectionRecord>) {
        let tasks = self
            .policy
            .determine(collections, self.max_queue_size as i32);
        {
            let mut task_queue = self.task_queue.lock().unwrap();
            task_queue.clear();
            task_queue.extend(tasks);
        }
    }

    pub(crate) fn take_task(&self) -> Option<Task> {
        let mut task_queue = self.task_queue.lock().unwrap();
        if task_queue.is_empty() {
            return None;
        }
        Some(task_queue.remove(0))
    }

    pub(crate) fn get_tasks(&self) -> Vec<Task> {
        let task_queue = self.task_queue.lock().unwrap();
        task_queue.clone()
    }
}

impl Runnable for Scheduler {
    fn run(&self) {
        let collections = self.log.read().unwrap().get_collections();
        self.schedule(collections);
    }

    fn box_clone(&self) -> Box<dyn Runnable + Send + Sync> {
        Box::new((*self).clone())
    }
}

#[derive(Clone)]
pub(crate) struct SchedulerPolicy {}

impl SchedulerPolicy {
    fn determine(&self, collections: Vec<CollectionRecord>, number_tasks: i32) -> Vec<Task> {
        let mut collections = collections;
        collections.sort_by(|a, b| a.last_compaction_time.cmp(&b.last_compaction_time));
        let number_tasks = if number_tasks > collections.len() as i32 {
            collections.len() as i32
        } else {
            number_tasks
        };
        let mut tasks = Vec::new();
        for collection in &collections[0..number_tasks as usize] {
            tasks.push(Task {
                collection_id: collection.id.clone(),
                tenant_id: collection.tenant_id.clone().unwrap(),
                cursor: collection.cursor.unwrap(),
            });
        }
        tasks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compactor::log::CollectionRecord;
    use crate::compactor::log::InMemoryLog;
    use crate::compactor::log::Log;
    use std::sync::Arc;

    #[test]
    fn test_scheduler() {
        let log: Arc<RwLock<dyn Log + Send + Sync>> = Arc::new(RwLock::new(InMemoryLog::new()));
        let scheduler_policy = SchedulerPolicy {};
        let max_queue_size = 10;
        let scheduler = Arc::new(Scheduler::new(log, scheduler_policy, max_queue_size));

        let collections = vec![
            CollectionRecord {
                id: "test1".to_string(),
                tenant_id: Some("test".to_string()),
                last_compaction_time: Some(1),
                first_record_time: Some(1),
                cursor: Some(0),
            },
            CollectionRecord {
                id: "test2".to_string(),
                tenant_id: Some("test".to_string()),
                last_compaction_time: Some(0),
                first_record_time: Some(0),
                cursor: Some(0),
            },
        ];

        scheduler.schedule(collections.clone());
        let task = scheduler.take_task().unwrap();
        assert_eq!(task.collection_id, "test2");

        let task = scheduler.take_task().unwrap();
        assert_eq!(task.collection_id, "test1");
    }

    #[test]
    fn test_scheduler_policy() {
        let scheduler_policy = SchedulerPolicy {};
        let collections = vec![
            CollectionRecord {
                id: "test1".to_string(),
                tenant_id: Some("test".to_string()),
                last_compaction_time: Some(1),
                first_record_time: Some(1),
                cursor: Some(0),
            },
            CollectionRecord {
                id: "test2".to_string(),
                tenant_id: Some("test".to_string()),
                last_compaction_time: Some(0),
                first_record_time: Some(0),
                cursor: Some(0),
            },
        ];
        let tasks = scheduler_policy.determine(collections.clone(), 1);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].collection_id, "test2");

        let tasks = scheduler_policy.determine(collections.clone(), 2);
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].collection_id, "test2");
        assert_eq!(tasks[1].collection_id, "test1");
    }
}
