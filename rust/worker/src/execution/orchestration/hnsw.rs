use super::super::operator::{wrap, TaskMessage};
use super::super::operators::pull_log::{PullLogsInput, PullLogsOperator, PullLogsOutput};
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
    dispatcher: Box<dyn Receiver<TaskMessage>>,
}

impl HnswQueryOrchestrator {
    pub fn new(
        query_vectors: Vec<Vec<f32>>,
        k: i32,
        include_embeddings: bool,
        segment_id: Uuid,
        log: Box<dyn Log>,
        dispatcher: Box<dyn Receiver<TaskMessage>>,
    ) -> Self {
        HnswQueryOrchestrator {
            state: ExecutionState::Pending,
            query_vectors,
            k,
            include_embeddings,
            segment_id,
            log,
            dispatcher,
        }
    }
}

#[async_trait]
impl Component for HnswQueryOrchestrator {
    fn queue_size(&self) -> usize {
        1000 // TODO: make configurable
    }

    async fn on_start(&mut self, ctx: &crate::system::ComponentContext<Self>) -> () {
        self.state = ExecutionState::PullLogs;
        let operator = PullLogsOperator::new(self.log.clone());
        // TODO: segment id vs collection id
        let input = PullLogsInput::new(self.segment_id, 0, 100);
        let task = wrap(operator, input, ctx.sender.as_receiver());
        match self.dispatcher.send(task).await {
            Ok(_) => (),
            Err(e) => {
                // TODO: log an error
            }
        }
    }
}

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
