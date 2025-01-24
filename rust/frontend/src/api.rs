use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_sysdb::sysdb;
use chroma_types::CreateDatabaseResponse;

use crate::config::FrontEndConfig;

#[async_trait::async_trait]
pub trait ServerApi {
    async fn create_database(
        &mut self,
        request: chroma_types::CreateDatabaseRequest,
    ) -> chroma_types::CreateDatabaseResponse;
}

#[derive(Clone)]
pub struct SegmentApi {
    sysdb_client: Box<sysdb::SysDb>,
}

impl SegmentApi {
    pub fn new(sysdb_client: Box<sysdb::SysDb>) -> Self {
        SegmentApi { sysdb_client }
    }
}

#[async_trait::async_trait]
impl Configurable<FrontEndConfig> for SegmentApi {
    async fn try_from_config(config: &FrontEndConfig) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb_client = chroma_sysdb::from_config(&config.sysdb).await?;

        Ok(SegmentApi::new(sysdb_client))
    }
}

#[async_trait::async_trait]
impl ServerApi for SegmentApi {
    async fn create_database(
        &mut self,
        request: chroma_types::CreateDatabaseRequest,
    ) -> chroma_types::CreateDatabaseResponse {
        let res = self
            .sysdb_client
            .create_database(
                request.database_id,
                request.database_name,
                request.tenant_id,
            )
            .await;
        // TODO: Error handling.
        CreateDatabaseResponse {
            success: res.is_ok(),
        }
    }
}
