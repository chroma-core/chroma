use crate::compactor::collection::Collection;
use crate::compactor::log::Log;
use crate::compactor::runtime::Runnable;
use crate::compactor::scheduler::Scheduler;
use crate::compactor::task::Task;
use crate::sysdb::sysdb::SysDb;
use crate::types::EmbeddingRecord;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

use super::segment_writer::SegmentWriter;

#[derive(Clone)]
pub(crate) struct Compactor {
    log: Arc<RwLock<dyn Log + Send + Sync>>,
    sysdb: Box<dyn SysDb>,
    scheduler: Arc<Scheduler>,
    running_tasks: Arc<RwLock<HashMap<String, Task>>>,
    compaction_timeout: Duration,
    batch_size: usize,
    segment_writer: Arc<SegmentWriter>,
}

impl Compactor {
    pub(crate) fn new(
        log: Arc<RwLock<dyn Log + Send + Sync>>,
        sysdb: Box<dyn SysDb>,
        scheduler: Arc<Scheduler>,
        running_tasks: Arc<RwLock<HashMap<String, Task>>>,
        compaction_timeout: Duration,
        batch_size: usize,
        segment_writer: Arc<SegmentWriter>,
    ) -> Compactor {
        Compactor {
            log,
            sysdb,
            scheduler,
            running_tasks,
            compaction_timeout,
            batch_size,
            segment_writer,
        }
    }

    pub(crate) async fn compact(&self) {
        let log = self.log.clone();
        let task = self.scheduler.take_task();
        if task.is_none() {
            return;
        }
        let task = task.unwrap();
        if self
            .running_tasks
            .read()
            .unwrap()
            .contains_key(&task.collection_id)
        {
            return;
        }

        self.running_tasks
            .write()
            .unwrap()
            .insert(task.collection_id.clone(), task.clone());

        // TODO: make the collectio_id as uuid
        // thinking about concurrent compaction for HNSW index
        // HNSW index is trivially parallel, so we can't have concurrent compaction
        let collection_id = task.collection_id.clone();
        let mut collection = Collection::new(
            collection_id,
            self.sysdb.clone(),
            self.segment_writer.clone(),
        );
        let start = Instant::now();
        let deadline = start + self.compaction_timeout;
        loop {
            if Instant::now() - start > self.compaction_timeout {
                break;
            }
            let records = log.read().unwrap().read(
                task.collection_id.clone(),
                task.cursor as usize,
                self.batch_size,
            );
            if records.is_none() {
                break;
            }
            // TODO: make the records as Vec<Box<EmbeddingRecord>> when getting from log
            let records = records.unwrap();
            let boxed_records: Vec<Box<EmbeddingRecord>> = records
                .iter()
                .map(|record| Box::new(record.clone()))
                .collect();
            let records = self.pre_process(&boxed_records);
            collection
                .compact(task.collection_id.clone(), records, deadline)
                .await;
        }

        self.running_tasks
            .write()
            .unwrap()
            .remove(&task.collection_id);
    }

    fn pre_process(&self, records: &Vec<Box<EmbeddingRecord>>) -> Vec<Box<EmbeddingRecord>> {
        let mut result: HashMap<String, Box<EmbeddingRecord>> = HashMap::new();
        for record in records {
            // group by id
            let id = record.id.clone();
            result.insert(id, record.clone());
        }
        result.values().cloned().collect()
    }
}

impl Runnable for Compactor {
    fn run(&self) {
        self.compact();
    }

    fn box_clone(&self) -> Box<dyn Runnable + Send + Sync> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::compactor::executor::DedicatedExecutor;
    use crate::compactor::log::CollectionRecord;
    use crate::compactor::log::InMemoryLog;
    use crate::compactor::runtime::Runtime;
    use crate::compactor::scheduler::SchedulerPolicy;
    use crate::config;
    use crate::config::Configurable;
    use crate::sysdb::sysdb::GrpcSysDb;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::thread;

    #[tokio::test]
    async fn test_compactor() {
        let log: Arc<RwLock<dyn Log + Send + Sync>> = Arc::new(RwLock::new(InMemoryLog::new()));
        let scheduler_policy = SchedulerPolicy {};
        let max_queue_size = 10;
        let scheduler = Arc::new(Scheduler::new(
            log.clone(),
            scheduler_policy,
            max_queue_size,
        ));
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
        let tasks = scheduler.get_tasks();
        assert_eq!(tasks.len(), 2);

        let running_tasks: Arc<RwLock<HashMap<String, Task>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let compaction_timeout = Duration::from_secs(1);
        let batch_size = 10;
        // Create a sysdb
        let config = config::RootConfig::load();
        // TODO: Sysdb should have a dynamic resolution in sysdb
        let sysdb = GrpcSysDb::try_from_config(&config.worker).await;
        let sysdb = match sysdb {
            Ok(sysdb) => sysdb,
            Err(err) => {
                println!("Failed to create sysdb component: {:?}", err);
                return;
            }
        };
        let sysdb = Box::new(sysdb);

        // Create a segment writer
        let executor = DedicatedExecutor::new("segment_writer", NonZeroUsize::new(1).unwrap());
        let segment_writer = Arc::new(SegmentWriter::new(executor));

        let compactor = Compactor::new(
            log.clone(),
            sysdb,
            scheduler.clone(),
            running_tasks,
            compaction_timeout,
            batch_size,
            segment_writer,
        );

        compactor.compact().await;
        let tasks = scheduler.get_tasks();
        assert_eq!(tasks.len(), 1);

        compactor.compact().await;
        let tasks = scheduler.get_tasks();
        assert_eq!(tasks.len(), 0);
    }

    #[test]
    fn test_compactor_with_runtime() {
        // let log: Arc<RwLock<dyn Log + Send + Sync>> = Arc::new(RwLock::new(InMemoryLog::new()));
        // let scheduler_policy = SchedulerPolicy {};
        // let max_queue_size = 10;
        // let scheduler = Arc::new(Scheduler::new(
        //     log.clone(),
        //     scheduler_policy,
        //     max_queue_size,
        // ));
        // let running_tasks: Arc<RwLock<HashMap<String, Task>>> =
        //     Arc::new(RwLock::new(HashMap::new()));
        // let compaction_timeout = Duration::from_secs(1);
        // let batch_size = 10;
        // let compactor = Compactor::new(
        //     log.clone(),
        //     scheduler.clone(),
        //     running_tasks,
        //     compaction_timeout,
        //     batch_size,
        // );

        // let compactor_clone = compactor.box_clone();
        // let runtime = Runtime::new(compactor_clone, Some(Duration::from_secs(1)));
        // let runtime_clone = runtime.clone();
        // thread::spawn(move || {
        //     let handle = runtime.execute();
        //     handle.join_handle.join().unwrap();
        // });
        // runtime_clone.shutdown();
        // let tasks = scheduler.get_tasks();
        // assert_eq!(tasks.len(), 0);
    }
}
