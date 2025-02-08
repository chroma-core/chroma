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
// TODO: This should be private once we fix dep injection
mod distributed;
pub mod local;

//////////////////////// Main Types ////////////////////////
#[derive(Clone, Debug)]
pub enum Executor {
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
            Executor::Local(_local_executor) => todo!(),
        }
    }
    pub async fn reset(&mut self) -> Result<(), ExecutorError> {
        match self {
            Executor::Distributed(_) => Ok(()),
            Executor::Local(local_executor) => local_executor
                .reset()
                .await
                .map_err(ExecutorError::Internal),
        }
    }
}
