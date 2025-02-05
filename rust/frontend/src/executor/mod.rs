use chroma_types::{
    operator::{CountResult, GetResult, KnnBatchResult},
    plan::{Count, Get, Knn},
    ExecutorError,
};
use distributed::DistributedExecutor;
use local::LocalExecutor;

//////////////////////// Exposed Modules ////////////////////////
pub(super) mod client_manager;
pub(crate) mod config;
mod distributed;
mod local;

//////////////////////// Main Types ////////////////////////
#[derive(Clone, Debug)]
pub(crate) enum Executor {
    Distributed(DistributedExecutor),
    Local(LocalExecutor),
}

impl Executor {
    pub async fn count(&mut self, plan: Count) -> Result<CountResult, ExecutorError> {
        match self {
            Executor::Distributed(distributed_executor) => distributed_executor.count(plan).await,
            Executor::Local(local_executor) => local_executor.count(plan).await,
        }
    }
    pub async fn get(&mut self, plan: Get) -> Result<GetResult, ExecutorError> {
        match self {
            Executor::Distributed(distributed_executor) => distributed_executor.get(plan).await,
            Executor::Local(local_executor) => local_executor.get(plan).await,
        }
    }
    pub async fn knn(&mut self, plan: Knn) -> Result<KnnBatchResult, ExecutorError> {
        match self {
            Executor::Distributed(distributed_executor) => distributed_executor.knn(plan).await,
            Executor::Local(local_executor) => local_executor.knn(plan).await,
        }
    }
    pub async fn is_ready(&self) -> bool {
        match self {
            Executor::Distributed(distributed_executor) => distributed_executor.is_ready().await,
            Executor::Local(local_executor) => todo!(),
        }
    }
}
