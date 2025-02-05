use crate::{GrpcSysDb, SysDb};
use chroma_config::Configurable;
use chroma_error::ChromaError;
use serde::{Deserialize, Serialize};

//////////////////////// GRPC SYSDB CONFIG ////////////////////////

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct GrpcSysDbConfig {
    #[serde(default = "GrpcSysDbConfig::default_host")]
    pub host: String,
    #[serde(default = "GrpcSysDbConfig::default_port")]
    pub port: u16,
    #[serde(default = "GrpcSysDbConfig::default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "GrpcSysDbConfig::default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "GrpcSysDbConfig::default_num_channels")]
    pub num_channels: usize,
}

impl GrpcSysDbConfig {
    fn default_host() -> String {
        "sysdb.chroma".to_string()
    }

    fn default_port() -> u16 {
        50051
    }

    fn default_connect_timeout_ms() -> u64 {
        5000
    }

    fn default_request_timeout_ms() -> u64 {
        5000
    }

    fn default_num_channels() -> usize {
        5
    }
}

impl Default for GrpcSysDbConfig {
    fn default() -> Self {
        GrpcSysDbConfig {
            host: GrpcSysDbConfig::default_host(),
            port: GrpcSysDbConfig::default_port(),
            connect_timeout_ms: GrpcSysDbConfig::default_connect_timeout_ms(),
            request_timeout_ms: GrpcSysDbConfig::default_request_timeout_ms(),
            num_channels: GrpcSysDbConfig::default_num_channels(),
        }
    }
}

//////////////////////// SYSDB CONFIG ////////////////////////

#[derive(Deserialize, Debug, Clone, Serialize)]
pub enum SysDbConfig {
    Grpc(GrpcSysDbConfig),
}

impl Default for SysDbConfig {
    fn default() -> Self {
        SysDbConfig::Grpc(GrpcSysDbConfig::default())
    }
}

pub async fn from_config(config: &SysDbConfig) -> Result<Box<SysDb>, Box<dyn ChromaError>> {
    match &config {
        SysDbConfig::Grpc(_) => Ok(Box::new(SysDb::Grpc(
            GrpcSysDb::try_from_config(config).await?,
        ))),
    }
}
