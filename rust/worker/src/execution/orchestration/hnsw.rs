use super::super::operator::{wrap, TaskMessage};
use super::super::operators::pull_log::{PullLogsInput, PullLogsOperator, PullLogsOutput};
use crate::errors::ChromaError;
use crate::execution::operators::pull_log::PullLogsResult;
use crate::sysdb::sysdb::SysDb;
use crate::system::System;
use crate::types::VectorQueryResult;
use crate::{
    log::log::Log,
    system::{Component, Handler, Receiver},
};
use async_trait::async_trait;
use num_bigint::BigInt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/**  The state of the orchestrator.
In chroma, we have a relatively fixed number of query plans that we can execute. Rather
than a flexible state machine abstraction, we just manually define the states that we
expect to encounter for a given query plan. This is a bit more rigid, but it's also simpler and easier to
understand. We can always add more abstraction later if we need it.
```plaintext

                               ┌───► Brute Force ─────┐
                               │                      │
  Pending ─► PullLogs ─► Dedupe│                      ├─► MergeResults ─► Finished
                               │                      │
                               └───► HNSW ────────────┘

```
*/
#[derive(Debug)]
enum ExecutionState {
    Pending,
    PullLogs,
    Dedupe,
    QueryKnn,
    MergeResults,
    Finished,
}

#[derive(Debug)]
pub(crate) struct HnswQueryOrchestrator {
    state: ExecutionState,
    // Component Execution
    system: System,
    // Query state
    query_vectors: Vec<Vec<f32>>,
    k: i32,
    include_embeddings: bool,
    segment_id: Uuid,
    // Services
    log: Box<dyn Log>,
    sysdb: Box<dyn SysDb>,
    dispatcher: Box<dyn Receiver<TaskMessage>>,
    // Result channel
    result_channel: Option<
        tokio::sync::oneshot::Sender<Result<Vec<Vec<VectorQueryResult>>, Box<dyn ChromaError>>>,
    >,
}

impl HnswQueryOrchestrator {
    pub(crate) fn new(
        system: System,
        query_vectors: Vec<Vec<f32>>,
        k: i32,
        include_embeddings: bool,
        segment_id: Uuid,
        log: Box<dyn Log>,
        sysdb: Box<dyn SysDb>,
        dispatcher: Box<dyn Receiver<TaskMessage>>,
    ) -> Self {
        HnswQueryOrchestrator {
            state: ExecutionState::Pending,
            system,
            query_vectors,
            k,
            include_embeddings,
            segment_id,
            log,
            sysdb,
            dispatcher,
            result_channel: None,
        }
    }

    /// Get the collection id for a segment id.
    /// TODO: This can be cached
    async fn get_collection_id_for_segment_id(&mut self, segment_id: Uuid) -> Option<Uuid> {
        let segments = self
            .sysdb
            .get_segments(Some(segment_id), None, None, None, None)
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
            Ok(end_timestamp) => end_timestamp.as_nanos() as i64,
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

    ///  Run the orchestrator and return the result.
    ///  # Note
    ///  Use this over spawning the component directly. This method will start the component and
    /// wait for it to finish before returning the result.
    pub(crate) async fn run(mut self) -> Result<Vec<Vec<VectorQueryResult>>, Box<dyn ChromaError>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.result_channel = Some(tx);
        let mut handle = self.system.clone().start_component(self);
        let result = rx.await;
        handle.stop();
        result.unwrap()
    }
}

// ============== Component Implementation ==============

#[async_trait]
impl Component for HnswQueryOrchestrator {
    fn queue_size(&self) -> usize {
        1000 // TODO: make configurable
    }

    async fn on_start(&mut self, ctx: &crate::system::ComponentContext<Self>) -> () {
        self.pull_logs(ctx.sender.as_receiver()).await;
    }
}

// ============== Handlers ==============

#[async_trait]
impl Handler<PullLogsResult> for HnswQueryOrchestrator {
    async fn handle(
        &mut self,
        message: PullLogsResult,
        ctx: &crate::system::ComponentContext<HnswQueryOrchestrator>,
    ) {
        self.state = ExecutionState::Dedupe;

        // TODO: implement the remaining state transitions and operators
        // This is an example of the final state transition and result

        let result_channel = match self.result_channel.take() {
            Some(tx) => tx,
            None => {
                // Log an error
                return;
            }
        };

        match message {
            Ok(logs) => {
                // TODO: remove this after debugging
                println!("Received logs: {:?}", logs);
                let _ = result_channel.send(Ok(vec![vec![VectorQueryResult {
                    id: "abc".to_string(),
                    seq_id: BigInt::from(0),
                    distance: 0.0,
                    vector: Some(vec![0.0, 0.0, 0.0]),
                }]]));
            }
            Err(e) => {
                let _ = result_channel.send(Err(Box::new(e)));
            }
        }
    }
}
