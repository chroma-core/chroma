use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_sysdb::sysdb;
use chroma_types::{CreateDatabaseError, CreateDatabaseResponse, GetDatabaseError};

use crate::config::FrontendConfig;

#[allow(dead_code)]
const DEFAULT_TENANT: &str = "default_tenant";
#[allow(dead_code)]
const DEFAULT_DATABASE: &str = "default_database";

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
            Ok(()) => Ok(CreateDatabaseResponse {}),
            Err(e) => Err(e),
        }
    }

    pub async fn get_database(
        &mut self,
        request: chroma_types::GetDatabaseRequest,
    ) -> Result<chroma_types::GetDatabaseResponse, GetDatabaseError> {
        self.sysdb_client
            .get_database(request.database_name, request.tenant_id)
            .await
    }
}

#[async_trait::async_trait]
impl Configurable<FrontendConfig> for Frontend {
    async fn try_from_config(config: &FrontendConfig) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb_client = chroma_sysdb::from_config(&config.sysdb).await?;

        Ok(Frontend::new(sysdb_client))
    }
}
