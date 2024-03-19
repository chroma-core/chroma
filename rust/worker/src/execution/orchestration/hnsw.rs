use super::super::operator::{wrap, TaskMessage};
use super::super::operators::pull_log::{PullLogsInput, PullLogsOperator, PullLogsOutput};
use crate::sysdb::sysdb::SysDb;
use crate::{
    log::log::Log,
    system::{Component, Handler, Receiver},
};
use async_trait::async_trait;
use std::fmt::{self, Debug, Formatter};
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
struct HnswQueryOrchestrator {
    state: ExecutionState,
    // Query state
    query_vectors: Vec<Vec<f32>>,
    k: i32,
    include_embeddings: bool,
    segment_id: Uuid,
    // Services
    log: Box<dyn Log>,
    sysdb: Box<dyn SysDb>,
    dispatcher: Box<dyn Receiver<TaskMessage>>,
}

impl HnswQueryOrchestrator {
    pub fn new(
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
            query_vectors,
            k,
            include_embeddings,
            segment_id,
            log,
            sysdb,
            dispatcher,
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

    async fn pull_logs(&mut self, self_address: Box<dyn Receiver<PullLogsOutput>>) {
        self.state = ExecutionState::PullLogs;
        let operator = PullLogsOperator::new(self.log.clone());
        let collection_id = match self.get_collection_id_for_segment_id(self.segment_id).await {
            Some(collection_id) => collection_id,
            None => {
                // Log an error and reply + return
                return;
            }
        };
        let input = PullLogsInput::new(collection_id, 0, 100);
        let task = wrap(operator, input, self_address);
        match self.dispatcher.send(task).await {
            Ok(_) => (),
            Err(e) => {
                // TODO: log an error and reply to caller
            }
        }
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
impl Handler<PullLogsOutput> for HnswQueryOrchestrator {
    async fn handle(
        &mut self,
        message: PullLogsOutput,
        ctx: &crate::system::ComponentContext<HnswQueryOrchestrator>,
    ) {
        self.state = ExecutionState::Dedupe;
        // TODO: implement the remaining state transitions and operators
    }
}
