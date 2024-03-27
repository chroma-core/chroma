use super::super::operator::{wrap, TaskMessage};
use crate::errors::ChromaError;
use crate::execution::data::data_chunk::DataChunk;
use crate::execution::operators::partition::PartitionInput;
use crate::execution::operators::partition::PartitionOperator;
use crate::execution::operators::partition::PartitionResult;
use crate::execution::operators::pull_log::PullLogsInput;
use crate::execution::operators::pull_log::PullLogsOperator;
use crate::execution::operators::pull_log::PullLogsResult;
use crate::log::log::Log;
use crate::sysdb::sysdb::SysDb;
use crate::system::Component;
use crate::system::Handler;
use crate::system::Receiver;
use crate::system::System;
use async_trait::async_trait;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use uuid::Uuid;

/**  The state of the orchestrator.
In chroma, we have a relatively fixed number of query plans that we can execute. Rather
than a flexible state machine abstraction, we just manually define the states that we
expect to encounter for a given query plan. This is a bit more rigid, but it's also simpler and easier to
understand. We can always add more abstraction later if we need it.
```plaintext

                               ┌───► Write─────-------┐
                               │                      │
  Pending ─► PullLogs ─► Group │                      ├─► Flush ─► Finished
                               │                      │
                               └───► Write ───────────┘

```
*/
#[derive(Debug)]
enum ExecutionState {
    Pending,
    PullLogs,
    Partition,
    Write,
    Flush,
    Finished,
}

#[derive(Debug)]
pub struct CompactOrchestrator {
    state: ExecutionState,
    // Component Execution
    system: System,
    segment_id: Uuid,
    // Dependencies
    log: Box<dyn Log>,
    sysdb: Box<dyn SysDb>,
    // Dispatcher
    dispatcher: Box<dyn Receiver<TaskMessage>>,
    // Result Channel
    result_channel: Option<tokio::sync::oneshot::Sender<Result<(), Box<dyn ChromaError>>>>,
}

impl CompactOrchestrator {
    pub fn new(
        system: System,
        segment_id: Uuid,
        log: Box<dyn Log>,
        sysdb: Box<dyn SysDb>,
        dispatcher: Box<dyn Receiver<TaskMessage>>,
        result_channel: Option<tokio::sync::oneshot::Sender<Result<(), Box<dyn ChromaError>>>>,
    ) -> Self {
        CompactOrchestrator {
            state: ExecutionState::Pending,
            system,
            segment_id,
            log,
            sysdb,
            dispatcher,
            result_channel,
        }
    }

    /// Get the collection id for a segment id.
    /// TODO: This can be cached
    async fn get_collection_id_for_segment_id(&mut self, segment_id: Uuid) -> Option<Uuid> {
        let segments = self
            .sysdb
            .get_segments(Some(segment_id), None, None, None)
            .await;
        match segments {
            Ok(segments) => match segments.get(0) {
                Some(segment) => segment.collection,
                None => None,
            },
            Err(e) => {
                // Log an error and return
                return None;
            }
        }
    }

    async fn pull_logs(&mut self, self_address: Box<dyn Receiver<PullLogsResult>>) {
        self.state = ExecutionState::PullLogs;
        let operator = PullLogsOperator::new(self.log.clone());
        let collection_id = match self.get_collection_id_for_segment_id(self.segment_id).await {
            Some(collection_id) => collection_id,
            None => {
                // Log an error and reply + return
                return;
            }
        };
        let end_timestamp = SystemTime::now().duration_since(UNIX_EPOCH);
        let end_timestamp = match end_timestamp {
            // TODO: change protobuf definition to use u64 instead of i64
            Ok(end_timestamp) => end_timestamp.as_secs() as i64,
            Err(e) => {
                // Log an error and reply + return
                return;
            }
        };
        let input = PullLogsInput::new(collection_id, 0, 100, None, Some(end_timestamp));
        let task = wrap(operator, input, self_address);
        match self.dispatcher.send(task).await {
            Ok(_) => (),
            Err(e) => {
                // TODO: log an error and reply to caller
            }
        }
    }

    async fn group(
        &mut self,
        records: DataChunk,
        self_address: Box<dyn Receiver<PartitionResult>>,
    ) {
        self.state = ExecutionState::Partition;
        // TODO: make this configurable
        let max_partition_size = 100;
        let operator = PartitionOperator::new();
        let input = PartitionInput::new(records, max_partition_size);
        let task = wrap(operator, input, self_address);
        match self.dispatcher.send(task).await {
            Ok(_) => (),
            Err(e) => {
                // TODO: log an error and reply to caller
            }
        }
    }

    async fn write(&mut self, records: Vec<DataChunk>) {
        self.state = ExecutionState::Write;

        for record in records {
            // TODO: implement write
        }
    }
}

// ============== Component Implementation ==============

#[async_trait]
impl Component for CompactOrchestrator {
    fn queue_size(&self) -> usize {
        1000 // TODO: make configurable
    }

    async fn on_start(&mut self, ctx: &crate::system::ComponentContext<Self>) -> () {
        self.pull_logs(ctx.sender.as_receiver()).await;
    }
}

// ============== Handlers ==============
#[async_trait]
impl Handler<PullLogsResult> for CompactOrchestrator {
    async fn handle(
        &mut self,
        message: PullLogsResult,
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        let records = match message {
            Ok(result) => result.logs(),
            Err(e) => {
                // Log an error and return
                let result_channel = match self.result_channel.take() {
                    Some(tx) => tx,
                    None => {
                        // Log an error
                        return;
                    }
                };
                let _ = result_channel.send(Err(Box::new(e)));
                return;
            }
        };
        self.group(records, ctx.sender.as_receiver()).await;
    }
}

#[async_trait]
impl Handler<PartitionResult> for CompactOrchestrator {
    async fn handle(
        &mut self,
        message: PartitionResult,
        ctx: &crate::system::ComponentContext<CompactOrchestrator>,
    ) {
        let records = match message {
            Ok(result) => result.records,
            Err(e) => {
                // Log an error and return
                let result_channel = match self.result_channel.take() {
                    Some(tx) => tx,
                    None => {
                        // Log an error
                        return;
                    }
                };
                let _ = result_channel.send(Err(Box::new(e)));
                return;
            }
        };
        // TODO: implement write records
    }
}
