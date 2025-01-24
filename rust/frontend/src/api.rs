use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_sysdb::sysdb;
use chroma_types::{CreateDatabaseError, CreateDatabaseResponse};

use crate::config::FrontEndConfig;

#[derive(Clone)]
pub struct Frontend {
    sysdb_client: Box<sysdb::SysDb>,
}

impl Frontend {
    pub fn new(sysdb_client: Box<sysdb::SysDb>) -> Self {
        Frontend { sysdb_client }
    }

    pub async fn create_database(
        &mut self,
        request: chroma_types::CreateDatabaseRequest,
    ) -> Result<chroma_types::CreateDatabaseResponse, CreateDatabaseError> {
        let res = self
            .sysdb_client
            .create_database(
                request.database_id,
                request.database_name,
                request.tenant_id,
            )
            .await;
        match res {
            Ok(_) => Ok(CreateDatabaseResponse {}),
            Err(e) => Err(e),
        }
    }
}

#[async_trait::async_trait]
impl Configurable<FrontEndConfig> for Frontend {
    async fn try_from_config(config: &FrontEndConfig) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb_client = chroma_sysdb::from_config(&config.sysdb).await?;

        Ok(Frontend::new(sysdb_client))
    }
}
