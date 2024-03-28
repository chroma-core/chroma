use crate::compactor::scheduler_policy::SchedulerPolicy;
use crate::compactor::types::Task;
use crate::log::log::CollectionInfo;
use crate::log::log::CollectionRecord;
use crate::log::log::Log;
use crate::sysdb::sysdb::SysDb;
use crate::system::Component;
use crate::system::ComponentContext;
use crate::system::Handler;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Scheduler {
    log: Box<dyn Log>,
    sysdb: Box<dyn SysDb>,
    policy: Box<dyn SchedulerPolicy>,
    task_queue: Arc<Mutex<Vec<Task>>>,
    max_queue_size: usize,
    schedule_interval: Duration,
}

impl Scheduler {
    pub(crate) fn new(
        log: Box<dyn Log>,
        sysdb: Box<dyn SysDb>,
        policy: Box<dyn SchedulerPolicy>,
        max_queue_size: usize,
        schedule_interval: Duration,
    ) -> Scheduler {
        Scheduler {
            log,
            sysdb,
            policy,
            task_queue: Arc::new(Mutex::new(Vec::with_capacity(max_queue_size))),
            max_queue_size,
            schedule_interval,
        }
    }

    async fn get_collections_with_new_data(&mut self) -> Vec<CollectionInfo> {
        let collections = self.log.get_collections_with_new_data().await;
        // TODO: filter collecitons based on memberlist
        let collections = match collections {
            Ok(collections) => collections,
            Err(e) => {
                // TODO: Log error
                println!("Error: {:?}", e);
                return Vec::new();
            }
        };
        collections
    }

    async fn verify_and_enrich_collections(
        &mut self,
        collections: Vec<CollectionInfo>,
    ) -> Vec<CollectionRecord> {
        let mut collection_records = Vec::new();
        for collection_info in collections {
            let collection_id = Uuid::parse_str(collection_info.collection_id.as_str());
            if collection_id.is_err() {
                // TODO: Log error
                println!("Error: {:?}", collection_id.err());
                continue;
            }
            let collection_id = Some(collection_id.unwrap());
            // TODO: add a cache to avoid fetching the same collection multiple times
            let result = self
                .sysdb
                .get_collections(collection_id, None, None, None)
                .await;

            match result {
                Ok(collection) => {
                    if collection.is_empty() {
                        // TODO: Log error
                        println!("Collection not found: {:?}", collection_info.collection_id);
                        continue;
                    }
                    collection_records.push(CollectionRecord {
                        id: collection[0].id.to_string(),
                        tenant_id: collection[0].tenant.clone(),
                        // TODO: get the last compaction time from the sysdb
                        last_compaction_time: 0,
                        first_record_time: collection_info.first_log_ts,
                        offset: collection_info.first_log_offset,
                    });
                }
                Err(e) => {
                    // TODO: Log error
                    println!("Error: {:?}", e);
                }
            }
        }
        collection_records
    }

    pub(crate) async fn schedule_internal(&mut self, collection_records: Vec<CollectionRecord>) {
        let tasks = self
            .policy
            .determine(collection_records, self.max_queue_size as i32);
        {
            let mut task_queue = self.task_queue.lock();
            task_queue.clear();
            task_queue.extend(tasks);
        }
    }

    pub(crate) async fn schedule(&mut self) {
        let collections = self.get_collections_with_new_data().await;
        if collections.is_empty() {
            return;
        }
        let collection_records = self.verify_and_enrich_collections(collections).await;
        self.schedule_internal(collection_records).await;
    }

    pub(crate) fn take_task(&self) -> Option<Task> {
        let mut task_queue = self.task_queue.lock();
        if task_queue.is_empty() {
            return None;
        }
        Some(task_queue.remove(0))
    }

    pub(crate) fn get_tasks(&self) -> Vec<Task> {
        let task_queue = self.task_queue.lock();
        task_queue.clone()
    }
}

#[async_trait]
impl Component for Scheduler {
    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        ctx.scheduler.schedule_interval(
            ctx.sender.clone(),
            ScheduleMessage {},
            self.schedule_interval,
            None,
            ctx,
        );
    }

    fn queue_size(&self) -> usize {
        // TODO: make this comfigurable
        1000
    }
}

impl Debug for Scheduler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Scheduler")
    }
}

#[derive(Clone, Debug)]
struct ScheduleMessage {}

#[async_trait]
impl Handler<ScheduleMessage> for Scheduler {
    async fn handle(&mut self, _event: ScheduleMessage, _ctx: &ComponentContext<Scheduler>) {
        self.schedule().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compactor::scheduler_policy::LasCompactionTimeSchedulerPolicy;
    use crate::log::log::InMemoryLog;
    use crate::log::log::InternalLogRecord;
    use crate::sysdb::sysdb::GetCollectionsError;
    use crate::sysdb::sysdb::GetSegmentsError;
    use crate::types::Collection;
    use crate::types::LogRecord;
    use crate::types::Operation;
    use crate::types::OperationRecord;
    use crate::types::Segment;
    use crate::types::SegmentScope;
    use num_bigint::BigInt;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::time::Duration;
    use uuid::Uuid;

    #[derive(Clone, Debug)]
    pub(crate) struct TestSysDb {
        collections: HashMap<Uuid, Collection>,
    }

    impl TestSysDb {
        pub(crate) fn new() -> Self {
            TestSysDb {
                collections: HashMap::new(),
            }
        }

        pub(crate) fn add_collection(&mut self, collection: Collection) {
            self.collections.insert(collection.id, collection);
        }

        fn filter_collections(
            collection: &Collection,
            collection_id: Option<Uuid>,
            name: Option<String>,
            tenant: Option<String>,
            database: Option<String>,
        ) -> bool {
            if collection_id.is_some() && collection_id.unwrap() != collection.id {
                return false;
            }
            if name.is_some() && name.unwrap() != collection.name {
                return false;
            }
            if tenant.is_some() && tenant.unwrap() != collection.tenant {
                return false;
            }
            if database.is_some() && database.unwrap() != collection.database {
                return false;
            }
            true
        }
    }

    #[async_trait]
    impl SysDb for TestSysDb {
        async fn get_collections(
            &mut self,
            collection_id: Option<Uuid>,
            name: Option<String>,
            tenant: Option<String>,
            database: Option<String>,
        ) -> Result<Vec<Collection>, GetCollectionsError> {
            let mut collections = Vec::new();
            for collection in self.collections.values() {
                if !TestSysDb::filter_collections(
                    &collection,
                    collection_id,
                    name.clone(),
                    tenant.clone(),
                    database.clone(),
                ) {
                    continue;
                }
                collections.push(collection.clone());
            }
            Ok(collections)
        }

        async fn get_segments(
            &mut self,
            id: Option<Uuid>,
            r#type: Option<String>,
            scope: Option<SegmentScope>,
            collection: Option<Uuid>,
        ) -> Result<Vec<Segment>, GetSegmentsError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn test_scheduler() {
        let mut log = Box::new(InMemoryLog::new());

        let collection_uuid_1 = Uuid::from_str("00000000-0000-0000-0000-000000000001").unwrap();
        let collection_id_1 = collection_uuid_1.to_string();
        log.add_log(
            collection_id_1.clone(),
            Box::new(InternalLogRecord {
                collection_id: collection_id_1.clone(),
                log_offset: 1,
                log_ts: 1,
                record: LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: None,
                        encoding: None,
                        metadata: None,
                        operation: Operation::Add,
                    },
                },
            }),
        );

        let collection_uuid_2 = Uuid::from_str("00000000-0000-0000-0000-000000000002").unwrap();
        let collection_id_2 = collection_uuid_2.to_string();
        log.add_log(
            collection_id_2.clone(),
            Box::new(InternalLogRecord {
                collection_id: collection_id_2.clone(),
                log_offset: 2,
                log_ts: 2,
                record: LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: None,
                        encoding: None,
                        metadata: None,
                        operation: Operation::Add,
                    },
                },
            }),
        );

        let mut sysdb = Box::new(TestSysDb::new());

        let collection_1 = Collection {
            id: collection_uuid_1,
            name: "collection_1".to_string(),
            metadata: None,
            dimension: Some(1),
            tenant: "tenant_1".to_string(),
            database: "database_1".to_string(),
            log_position: 0,
            version: 0,
        };

        let collection_2 = Collection {
            id: collection_uuid_2,
            name: "collection_2".to_string(),
            metadata: None,
            dimension: Some(1),
            tenant: "tenant_2".to_string(),
            database: "database_2".to_string(),
            log_position: 0,
            version: 0,
        };
        sysdb.add_collection(collection_1);
        sysdb.add_collection(collection_2);
        let scheduler_policy = Box::new(LasCompactionTimeSchedulerPolicy {});
        let mut scheduler =
            Scheduler::new(log, sysdb, scheduler_policy, 1000, Duration::from_secs(1));

        scheduler.schedule().await;
        let tasks = scheduler.get_tasks();
        assert_eq!(tasks.len(), 2);
        // TODO: 3/9 Tasks may be out of order since we have not yet implemented SysDB Get last compaction time. Use contains instead of equal.
        let task_ids = tasks
            .iter()
            .map(|t| t.collection_id.clone())
            .collect::<Vec<String>>();
        assert!(task_ids.contains(&collection_id_1));
        assert!(task_ids.contains(&collection_id_2));
    }
}
