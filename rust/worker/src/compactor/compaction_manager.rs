use super::scheduler::Scheduler;
use super::scheduler_policy::LasCompactionTimeSchedulerPolicy;
use crate::compactor::types::CompactionJob;
use crate::compactor::types::ScheduleMessage;
use crate::config::Configurable;
use crate::errors::ChromaError;
use crate::errors::ErrorCodes;
use crate::execution::operator::TaskMessage;
use crate::execution::orchestration::CompactOrchestrator;
use crate::execution::orchestration::CompactionResponse;
use crate::log::log::Log;
use crate::system::Component;
use crate::system::ComponentContext;
use crate::system::Handler;
use crate::system::Receiver;
use crate::system::System;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

pub(crate) struct CompactionManager {
    system: Option<System>,
    scheduler: Scheduler,
    // Dependencies
    log: Box<dyn Log>,
    // Dispatcher
    dispatcher: Option<Box<dyn Receiver<TaskMessage>>>,
    // Config
    compaction_manager_queue_size: usize,
    max_concurrent_jobs: usize,
    compaction_interval: Duration,
}

#[derive(Error, Debug)]
pub(crate) enum CompactionError {
    #[error("Failed to compact")]
    FailedToCompact,
}

impl ChromaError for CompactionError {
    fn code(&self) -> ErrorCodes {
        match self {
            CompactionError::FailedToCompact => ErrorCodes::Internal,
        }
    }
}

impl CompactionManager {
    pub(crate) fn new(
        scheduler: Scheduler,
        log: Box<dyn Log>,
        compaction_manager_queue_size: usize,
        max_concurrent_jobs: usize,
        compaction_interval: Duration,
    ) -> Self {
        CompactionManager {
            system: None,
            scheduler,
            log,
            dispatcher: None,
            compaction_manager_queue_size,
            max_concurrent_jobs,
            compaction_interval,
        }
    }

    async fn compact(
        &self,
        compaction_job: &CompactionJob,
    ) -> Result<CompactionResponse, Box<dyn ChromaError>> {
        let collection_uuid = Uuid::from_str(&compaction_job.collection_id);
        if collection_uuid.is_err() {
            // handle error properly
            println!("Failed to parse collection id");
            return Err(Box::new(CompactionError::FailedToCompact));
        }

        let dispatcher = match self.dispatcher {
            Some(ref dispatcher) => dispatcher,
            None => {
                println!("No dispatcher found");
                return Err(Box::new(CompactionError::FailedToCompact));
            }
        };

        match self.system {
            Some(ref system) => {
                let orchestrator = CompactOrchestrator::new(
                    compaction_job.clone(),
                    system.clone(),
                    collection_uuid.unwrap(),
                    self.log.clone(),
                    dispatcher.clone(),
                    None,
                );

                match orchestrator.run().await {
                    Ok(result) => {
                        println!("Compaction Job completed");
                        return Ok(result);
                    }
                    Err(e) => {
                        println!("Compaction Job failed");
                        return Err(e);
                    }
                }
            }
            None => {
                println!("No system found");
                return Err(Box::new(CompactionError::FailedToCompact));
            }
        };
    }

    // TODO: make the return type more informative
    pub(crate) async fn compact_batch(&mut self) -> (u32, u32) {
        self.scheduler.schedule().await;
        let mut jobs = FuturesUnordered::new();
        for job in self.scheduler.get_jobs() {
            jobs.push(self.compact(job));
        }
        let mut num_completed_jobs = 0;
        let mut num_failed_jobs = 0;
        while let Some(job) = jobs.next().await {
            match job {
                Ok(result) => {
                    println!("Compaction completed: {:?}", result);
                    num_completed_jobs += 1;
                }
                Err(e) => {
                    println!("Compaction failed: {:?}", e);
                    num_failed_jobs += 1;
                }
            }
        }
        (num_completed_jobs, num_failed_jobs)
    }

    pub(crate) fn set_dispatcher(&mut self, dispatcher: Box<dyn Receiver<TaskMessage>>) {
        self.dispatcher = Some(dispatcher);
    }

    pub(crate) fn set_system(&mut self, system: System) {
        self.system = Some(system);
    }
}

#[async_trait]
impl Configurable for CompactionManager {
    async fn try_from_config(
        config: &crate::config::WorkerConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb = match crate::sysdb::from_config(&config).await {
            Ok(sysdb) => sysdb,
            Err(err) => {
                return Err(err);
            }
        };
        let log = match crate::log::from_config(&config).await {
            Ok(log) => log,
            Err(err) => {
                return Err(err);
            }
        };

        let policy = Box::new(LasCompactionTimeSchedulerPolicy {});
        let scheduler = Scheduler::new(log.clone(), sysdb.clone(), policy, 1000);
        let compaction_interval_sec = config.compactor.compaction_interval_sec;
        let max_concurrent_jobs = config.compactor.max_concurrent_jobs;
        let compaction_manager_queue_size = config.compactor.compaction_manager_queue_size;
        Ok(CompactionManager::new(
            scheduler,
            log,
            compaction_manager_queue_size,
            max_concurrent_jobs,
            Duration::from_secs(compaction_interval_sec),
        ))
    }
}

// ============== Component Implementation ==============
#[async_trait]
impl Component for CompactionManager {
    fn queue_size(&self) -> usize {
        1000 // TODO: make configurable
    }

    async fn on_start(&mut self, ctx: &crate::system::ComponentContext<Self>) -> () {
        ctx.scheduler.schedule_interval(
            ctx.sender.clone(),
            ScheduleMessage {},
            self.compaction_interval,
            None,
            ctx,
        );
    }
}

impl Debug for CompactionManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CompactionManager")
    }
}

// ============== Handlers ==============
#[async_trait]
impl Handler<ScheduleMessage> for CompactionManager {
    async fn handle(
        &mut self,
        message: ScheduleMessage,
        ctx: &ComponentContext<CompactionManager>,
    ) {
        self.compact_batch().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::dispatcher::Dispatcher;
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
    async fn test_compaction_manager() {
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

        let mut manager = CompactionManager::new(
            Scheduler::new(
                log.clone(),
                sysdb.clone(),
                Box::new(LasCompactionTimeSchedulerPolicy {}),
                1000,
            ),
            log,
            1000,
            10,
            Duration::from_secs(1),
        );

        let system = System::new();

        let dispatcher = Dispatcher::new(10, 10, 10);
        let dispatcher_handle = system.start_component(dispatcher);
        manager.set_dispatcher(dispatcher_handle.receiver());
        manager.set_system(system);
        let (num_completed, number_failed) = manager.compact_batch().await;
        assert_eq!(num_completed, 2);
        assert_eq!(number_failed, 0);
    }
}
