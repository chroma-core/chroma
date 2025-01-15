use chroma_config::Configurable;
use chroma_error::ChromaError;
use serde::Deserialize;

use crate::{GrpcSysDb, SysDb};

#[derive(Deserialize, Debug, Clone)]
pub struct GrpcSysDbConfig {
    pub host: String,
    pub port: u16,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
}

#[derive(Deserialize, Debug)]
pub enum SysDbConfig {
    Grpc(GrpcSysDbConfig),
}

pub async fn from_config(config: &SysDbConfig) -> Result<Box<SysDb>, Box<dyn ChromaError>> {
    match &config {
        SysDbConfig::Grpc(_) => Ok(Box::new(SysDb::Grpc(
            GrpcSysDb::try_from_config(config).await?,
        ))),
    }
}
