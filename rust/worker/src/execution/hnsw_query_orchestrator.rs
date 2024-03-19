use super::operator::{wrap, PullLogsInput, PullLogsOperator, PullLogsOutput, TaskMessage};
use crate::{
    log::log::Log,
    system::{Component, Handler, Receiver},
};
use async_trait::async_trait;
use std::fmt::{self, Debug, Formatter};
use uuid::Uuid;

#[derive(Debug)]
enum ExecutionState {
    Pending,
    PullLogs,
    Dedupe,
    QueryKnn,
    MergeResults,
    Finished,
}

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

impl Debug for HnswQueryOrchestrator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("HnswQueryOrchestrator")
            .field("state", &self.state)
            .field("query_vectors", &self.query_vectors)
            .field("k", &self.k)
            .field("include_embeddings", &self.include_embeddings)
            .field("segment_id", &self.segment_id)
            .finish()
    }
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
        // TODO: move box into constructor
        let operator = Box::new(PullLogsOperator::new(self.log.clone()));
        // TODO: segment id vs collection id
        let input = PullLogsInput::new(self.segment_id, 0, 100);
        let task = wrap(operator, input, ctx.sender.as_receiver());
        // self.dispatcher.send(task).await;
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_on_start() {
        let system = crate::system::System::new();
        let log = Box::new(crate::log::log::InMemoryLog::new());
        // reply_channel = chan();
        // let orchestrator = HnswQueryOrchestrator::new(
        //     vec![vec![1.0, 2.0, 3.0]],
        //     10,
        //     true,
        //     Uuid::new_v4(),
        //     log,
        //     dispatcher,
        //     reply_channel,
        // );
        // let handle = system.start_component(orchestrator);
        // let msg = StartMessage { reply_chan : reply_chan}
        // let res = handler.send(msg).await();
        // handle.cancel();
        // handle.join();
    }
}
