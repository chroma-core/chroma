use crate::compactor::scheduler_policy::SchedulerPolicy;
use crate::compactor::types::CompactionJob;
use crate::log::log::CollectionInfo;
use crate::log::log::CollectionRecord;
use crate::log::log::Log;
use crate::sysdb::sysdb::SysDb;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct Scheduler {
    log: Box<dyn Log>,
    sysdb: Box<dyn SysDb>,
    policy: Box<dyn SchedulerPolicy>,
    job_queue: Vec<CompactionJob>,
    max_concurrent_jobs: usize,
}

impl Scheduler {
    pub(crate) fn new(
        log: Box<dyn Log>,
        sysdb: Box<dyn SysDb>,
        policy: Box<dyn SchedulerPolicy>,
        max_concurrent_jobs: usize,
    ) -> Scheduler {
        Scheduler {
            log,
            sysdb,
            policy,
            job_queue: Vec::with_capacity(max_concurrent_jobs),
            max_concurrent_jobs,
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
        let jobs = self
            .policy
            .determine(collection_records, self.max_concurrent_jobs as i32);
        {
            self.job_queue.clear();
            self.job_queue.extend(jobs);
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

    pub(crate) fn get_jobs(&self) -> impl Iterator<Item = &CompactionJob> {
        self.job_queue.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compactor::scheduler_policy::LasCompactionTimeSchedulerPolicy;
    use crate::log::log::InMemoryLog;
    use crate::log::log::InternalLogRecord;
    use crate::sysdb::test_sysdb::TestSysDb;
    use crate::types::Collection;
    use crate::types::LogRecord;
    use crate::types::Operation;
    use crate::types::OperationRecord;
    use std::str::FromStr;
    use uuid::Uuid;

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
        let mut scheduler = Scheduler::new(log, sysdb, scheduler_policy, 1000);

        scheduler.schedule().await;
        let jobs = scheduler.get_jobs();

        // TODO: 3/9 Tasks may be out of order since we have not yet implemented SysDB Get last compaction time. Use contains instead of equal.
        let job_ids = jobs
            .map(|t| t.collection_id.clone())
            .collect::<Vec<String>>();
        assert_eq!(job_ids.len(), 2);
        assert!(job_ids.contains(&collection_id_1));
        assert!(job_ids.contains(&collection_id_2));
    }
}
