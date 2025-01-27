use std::{iter::once, time::Duration};

use chroma_types::{
    chroma_proto::query_executor_client::QueryExecutorClient,
    operator::{from_proto_knn_batch_result, CountResult, GetResult, KnnBatchResult},
    plan::{Count, Get, Knn},
    ExecutorError,
};
use tonic::{
    transport::{Channel, Endpoint},
    Request,
};

#[derive(Clone)]
pub struct DistributedExecutor {
    client: QueryExecutorClient<tonic::transport::Channel>,
}

impl DistributedExecutor {
    pub async fn count(&mut self, plan: Count) -> Result<CountResult, ExecutorError> {
        Ok(self
            .client
            .count(Request::new(plan.into()))
            .await?
            .into_inner()
            .count)
    }

    pub async fn get(&mut self, plan: Get) -> Result<GetResult, ExecutorError> {
        Ok(self
            .client
            .get(Request::new(plan.try_into()?))
            .await?
            .into_inner()
            .try_into()?)
    }
    pub async fn knn(&mut self, plan: Knn) -> Result<KnnBatchResult, ExecutorError> {
        Ok(from_proto_knn_batch_result(
            self.client
                .knn(Request::new(plan.try_into()?))
                .await?
                .into_inner(),
        )?)
    }
}

// WARN: This is a placeholder impl, which should be replaced by proper initialization from memberlist
impl Default for DistributedExecutor {
    fn default() -> Self {
        let endpoint =
            Endpoint::from_shared("query-service-0.query-service.chroma.svc.cluster.local:50051")
                .expect("This should be a valid endpoint for query service")
                .connect_timeout(Duration::from_secs(6))
                .timeout(Duration::from_secs(1));
        Self {
            client: QueryExecutorClient::new(Channel::balance_list(once(endpoint).cycle().take(6))),
        }
    }
}
