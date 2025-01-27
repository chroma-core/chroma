use chroma_types::{
    operator::{CountResult, GetResult, KnnBatchResult},
    plan::{Count, Get, Knn},
    ExecutorError,
};
use distributed::DistributedExecutor;

mod distributed;

#[derive(Clone)]
pub enum Executor {
    Distributed(DistributedExecutor),
}

impl Executor {
    pub async fn count(&mut self, plan: Count) -> Result<CountResult, ExecutorError> {
        match self {
            Executor::Distributed(distributed_executor) => distributed_executor.count(plan).await,
        }
    }
    pub async fn get(&mut self, plan: Get) -> Result<GetResult, ExecutorError> {
        match self {
            Executor::Distributed(distributed_executor) => distributed_executor.get(plan).await,
        }
    }
    pub async fn knn(&mut self, plan: Knn) -> Result<KnnBatchResult, ExecutorError> {
        match self {
            Executor::Distributed(distributed_executor) => distributed_executor.knn(plan).await,
        }
    }
}

// WARN: This is a placeholder impl, which should be replaced by proper initialization from config
impl Default for Executor {
    fn default() -> Self {
        Self::Distributed(DistributedExecutor::default())
    }
}
