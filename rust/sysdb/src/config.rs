use crate::{GrpcSysDb, SysDb};
use chroma_config::Configurable;
use chroma_error::ChromaError;
use serde::{Deserialize, Serialize};

//////////////////////// GRPC SYSDB CONFIG ////////////////////////

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct GrpcSysDbConfig {
    pub host: String,
    pub port: u16,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    #[serde(default = "default_num_channels")]
    pub num_channels: usize,
}

//////////////////////// DEFAULTS ////////////////////////

fn default_num_channels() -> usize {
    5
}

//////////////////////// SYSDB CONFIG ////////////////////////

#[derive(Deserialize, Debug, Clone, Serialize)]
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
