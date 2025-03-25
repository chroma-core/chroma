use crate::{sqlite::SqliteSysDb, GrpcSysDb, SysDb};
use async_trait::async_trait;
use chroma_config::{
    registry::{Injectable, Registry},
    Configurable,
};
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

//////////////////////// SQLITE SYSDB CONFIG ////////////////////////

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct SqliteSysDbConfig {
    pub log_topic_namespace: String,
    pub log_tenant: String,
}

impl Default for SqliteSysDbConfig {
    fn default() -> Self {
        SqliteSysDbConfig {
            log_topic_namespace: "default".to_string(),
            log_tenant: "default".to_string(),
        }
    }
}

//////////////////////// SYSDB CONFIG ////////////////////////

#[derive(Deserialize, Debug, Clone, Serialize)]
pub enum SysDbConfig {
    #[serde(alias = "grpc")]
    Grpc(GrpcSysDbConfig),
    #[serde(alias = "sqlite")]
    Sqlite(SqliteSysDbConfig),
}

impl Default for SysDbConfig {
    fn default() -> Self {
        SysDbConfig::Grpc(GrpcSysDbConfig::default())
    }
}

impl Injectable for SysDb {}

#[async_trait]
impl Configurable<SysDbConfig> for SysDb {
    async fn try_from_config(
        config: &SysDbConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let out = match &config {
            SysDbConfig::Grpc(grpc_config) => {
                SysDb::Grpc(GrpcSysDb::try_from_config(grpc_config, registry).await?)
            }
            SysDbConfig::Sqlite(sqlite_config) => {
                SysDb::Sqlite(SqliteSysDb::try_from_config(sqlite_config, registry).await?)
            }
        };

        registry.register(out.clone());
        Ok(out)
    }
}
