use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_sysdb::SysDb;
use chroma_system::System;
use chroma_types::{
    chroma_proto::{self, compactor_client::CompactorClient},
    CollectionUuid, ManualCompactionError, ManualCompactionRequest, ManualCompactionResponse,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GrpcCompactionClientConfig {
    pub url: String,
}

#[derive(Debug, Clone)]
pub(super) struct GrpcCompactionClient {
    sysdb: SysDb,
    client: CompactorClient<tonic::transport::Channel>,
}

impl GrpcCompactionClient {
    pub async fn manually_compact(
        &mut self,
        request: ManualCompactionRequest,
    ) -> Result<ManualCompactionResponse, ManualCompactionError> {
        async fn get_version(sysdb: &mut SysDb, collection_id: CollectionUuid) -> i32 {
            let mut collection = sysdb
                .get_collections(Some(collection_id), None, None, None, None, 0)
                .await
                .unwrap(); // todo
            let collection = collection.pop().unwrap(); // todo
            return collection.version;
        }

        let version_before_compaction = get_version(&mut self.sysdb, request.collection_id).await;

        self.client
            .compact(chroma_proto::CompactionRequest {
                ids: Some(chroma_proto::CollectionIds {
                    ids: vec![request.collection_id.to_string()],
                }),
            })
            .await
            .unwrap();

        loop {
            let version_after_compaction =
                get_version(&mut self.sysdb, request.collection_id).await;
            if version_after_compaction > version_before_compaction {
                break;
            }
            // todo
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        Ok(ManualCompactionResponse {})
    }
}

#[derive(Debug, Error)]
pub enum FromConfigError {
    #[error("Failed to create gRPC client: {0}")]
    GrpcClient(#[from] tonic::transport::Error),
}

impl ChromaError for FromConfigError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            FromConfigError::GrpcClient(_) => chroma_error::ErrorCodes::Internal,
        }
    }
}

#[async_trait::async_trait]
impl Configurable<(GrpcCompactionClientConfig, System)> for GrpcCompactionClient {
    async fn try_from_config(
        (config, _system): &(GrpcCompactionClientConfig, System),
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let client = CompactorClient::connect(config.url.clone())
            .await
            .map_err(|err| FromConfigError::GrpcClient(err).boxed())?;
        Ok(Self {
            client,
            sysdb: registry.get().map_err(|err| err.boxed())?,
        })
    }
}
