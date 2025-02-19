use super::{
    grpc_compaction_client::{GrpcCompactionClient, GrpcCompactionClientConfig},
    local_compaction_client::LocalCompactionClient,
};
use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_system::System;
use chroma_types::{ManualCompactionError, ManualCompactionRequest, ManualCompactionResponse};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum CompactionClient {
    Local(LocalCompactionClient),
    Grpc(GrpcCompactionClient),
}

impl CompactionClient {
    pub async fn manually_compact(
        &mut self,
        request: ManualCompactionRequest,
    ) -> Result<ManualCompactionResponse, ManualCompactionError> {
        match self {
            CompactionClient::Local(client) => client.manually_compact(request).await,
            CompactionClient::Grpc(client) => client.manually_compact(request).await,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum CompactionClientConfig {
    Local,
    Grpc(GrpcCompactionClientConfig),
}

#[async_trait::async_trait]
impl Configurable<(CompactionClientConfig, System)> for CompactionClient {
    async fn try_from_config(
        (config, system): &(CompactionClientConfig, System),
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match config {
            CompactionClientConfig::Local => {
                let client =
                    LocalCompactionClient::try_from_config(&((), system.clone()), registry).await?;
                Ok(CompactionClient::Local(client))
            }
            CompactionClientConfig::Grpc(grpc_config) => {
                let client = GrpcCompactionClient::try_from_config(
                    &(grpc_config.clone(), system.clone()),
                    registry,
                )
                .await?;
                Ok(CompactionClient::Grpc(client))
            }
        }
    }
}
