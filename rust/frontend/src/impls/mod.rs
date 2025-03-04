pub mod in_memory_frontend;
pub mod service_based_frontend;
mod utils;

use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_system::System;
use in_memory_frontend::InMemoryFrontend;
use service_based_frontend::ServiceBasedFrontend;

use crate::FrontendConfig;

#[derive(Clone)]
pub enum Frontend {
    ServiceBased(ServiceBasedFrontend),
    InMemory(InMemoryFrontend),
}

impl Frontend {
    pub async fn reset(&mut self) -> Result<chroma_types::ResetResponse, chroma_types::ResetError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.reset().await,
            Frontend::InMemory(frontend) => frontend.reset().await,
        }
    }

    pub async fn heartbeat(
        &self,
    ) -> Result<chroma_types::HeartbeatResponse, chroma_types::HeartbeatError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.heartbeat().await,
            Frontend::InMemory(frontend) => frontend.heartbeat().await,
        }
    }

    pub fn get_max_batch_size(&mut self) -> u32 {
        match self {
            Frontend::ServiceBased(frontend) => frontend.get_max_batch_size(),
            Frontend::InMemory(frontend) => frontend.get_max_batch_size(),
        }
    }

    pub async fn create_tenant(
        &mut self,
        request: chroma_types::CreateTenantRequest,
    ) -> Result<chroma_types::CreateTenantResponse, chroma_types::CreateTenantError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.create_tenant(request).await,
            Frontend::InMemory(frontend) => frontend.create_tenant(request).await,
        }
    }

    pub async fn get_tenant(
        &mut self,
        request: chroma_types::GetTenantRequest,
    ) -> Result<chroma_types::GetTenantResponse, chroma_types::GetTenantError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.get_tenant(request).await,
            Frontend::InMemory(frontend) => frontend.get_tenant(request).await,
        }
    }

    pub async fn create_database(
        &mut self,
        request: chroma_types::CreateDatabaseRequest,
    ) -> Result<chroma_types::CreateDatabaseResponse, chroma_types::CreateDatabaseError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.create_database(request).await,
            Frontend::InMemory(frontend) => frontend.create_database(request).await,
        }
    }

    pub async fn list_databases(
        &mut self,
        request: chroma_types::ListDatabasesRequest,
    ) -> Result<chroma_types::ListDatabasesResponse, chroma_types::ListDatabasesError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.list_databases(request).await,
            Frontend::InMemory(frontend) => frontend.list_databases(request).await,
        }
    }

    pub async fn get_database(
        &mut self,
        request: chroma_types::GetDatabaseRequest,
    ) -> Result<chroma_types::GetDatabaseResponse, chroma_types::GetDatabaseError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.get_database(request).await,
            Frontend::InMemory(frontend) => frontend.get_database(request).await,
        }
    }

    pub async fn delete_database(
        &mut self,
        request: chroma_types::DeleteDatabaseRequest,
    ) -> Result<chroma_types::DeleteDatabaseResponse, chroma_types::DeleteDatabaseError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.delete_database(request).await,
            Frontend::InMemory(frontend) => frontend.delete_database(request).await,
        }
    }

    pub async fn list_collections(
        &mut self,
        request: chroma_types::ListCollectionsRequest,
    ) -> Result<chroma_types::ListCollectionsResponse, chroma_types::GetCollectionsError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.list_collections(request).await,
            Frontend::InMemory(frontend) => frontend.list_collections(request).await,
        }
    }

    pub async fn count_collections(
        &mut self,
        request: chroma_types::CountCollectionsRequest,
    ) -> Result<chroma_types::CountCollectionsResponse, chroma_types::CountCollectionsError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.count_collections(request).await,
            Frontend::InMemory(frontend) => frontend.count_collections(request).await,
        }
    }

    pub async fn get_collection(
        &mut self,
        request: chroma_types::GetCollectionRequest,
    ) -> Result<chroma_types::GetCollectionResponse, chroma_types::GetCollectionError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.get_collection(request).await,
            Frontend::InMemory(frontend) => frontend.get_collection(request).await,
        }
    }

    pub async fn create_collection(
        &mut self,
        request: chroma_types::CreateCollectionRequest,
    ) -> Result<chroma_types::CreateCollectionResponse, chroma_types::CreateCollectionError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.create_collection(request).await,
            Frontend::InMemory(frontend) => frontend.create_collection(request).await,
        }
    }

    pub async fn update_collection(
        &mut self,
        request: chroma_types::UpdateCollectionRequest,
    ) -> Result<chroma_types::UpdateCollectionResponse, chroma_types::UpdateCollectionError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.update_collection(request).await,
            Frontend::InMemory(frontend) => frontend.update_collection(request).await,
        }
    }

    pub async fn delete_collection(
        &mut self,
        request: chroma_types::DeleteCollectionRequest,
    ) -> Result<chroma_types::DeleteCollectionRecordsResponse, chroma_types::DeleteCollectionError>
    {
        match self {
            Frontend::ServiceBased(frontend) => frontend.delete_collection(request).await,
            Frontend::InMemory(frontend) => frontend.delete_collection(request).await,
        }
    }

    pub async fn add(
        &mut self,
        request: chroma_types::AddCollectionRecordsRequest,
    ) -> Result<chroma_types::AddCollectionRecordsResponse, chroma_types::AddCollectionRecordsError>
    {
        match self {
            Frontend::ServiceBased(frontend) => frontend.add(request).await,
            Frontend::InMemory(frontend) => frontend.add(request).await,
        }
    }

    pub async fn update(
        &mut self,
        request: chroma_types::UpdateCollectionRecordsRequest,
    ) -> Result<
        chroma_types::UpdateCollectionRecordsResponse,
        chroma_types::UpdateCollectionRecordsError,
    > {
        match self {
            Frontend::ServiceBased(frontend) => frontend.update(request).await,
            Frontend::InMemory(frontend) => frontend.update(request).await,
        }
    }

    pub async fn upsert(
        &mut self,
        request: chroma_types::UpsertCollectionRecordsRequest,
    ) -> Result<
        chroma_types::UpsertCollectionRecordsResponse,
        chroma_types::UpsertCollectionRecordsError,
    > {
        match self {
            Frontend::ServiceBased(frontend) => frontend.upsert(request).await,
            Frontend::InMemory(frontend) => frontend.upsert(request).await,
        }
    }

    pub async fn delete(
        &mut self,
        request: chroma_types::DeleteCollectionRecordsRequest,
    ) -> Result<
        chroma_types::DeleteCollectionRecordsResponse,
        chroma_types::DeleteCollectionRecordsError,
    > {
        match self {
            Frontend::ServiceBased(frontend) => frontend.delete(request).await,
            Frontend::InMemory(frontend) => frontend.delete(request).await,
        }
    }

    pub async fn count(
        &mut self,
        request: chroma_types::CountRequest,
    ) -> Result<chroma_types::CountResponse, chroma_types::QueryError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.count(request).await,
            Frontend::InMemory(frontend) => frontend.count(request).await,
        }
    }

    pub async fn get(
        &mut self,
        request: chroma_types::GetRequest,
    ) -> Result<chroma_types::GetResponse, chroma_types::QueryError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.get(request).await,
            Frontend::InMemory(frontend) => frontend.get(request).await,
        }
    }

    pub async fn query(
        &mut self,
        request: chroma_types::QueryRequest,
    ) -> Result<chroma_types::QueryResponse, chroma_types::QueryError> {
        match self {
            Frontend::ServiceBased(frontend) => frontend.query(request).await,
            Frontend::InMemory(frontend) => frontend.query(request).await,
        }
    }

    pub async fn healthcheck(&self) -> chroma_types::HealthCheckResponse {
        match self {
            Frontend::ServiceBased(frontend) => frontend.healthcheck().await,
            Frontend::InMemory(frontend) => frontend.healthcheck().await,
        }
    }
}
#[async_trait::async_trait]
impl Configurable<(FrontendConfig, System)> for Frontend {
    async fn try_from_config(
        config_and_system: &(FrontendConfig, System),
        registry: &chroma_config::registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        ServiceBasedFrontend::try_from_config(config_and_system, registry)
            .await
            .map(Frontend::ServiceBased)
    }
}
