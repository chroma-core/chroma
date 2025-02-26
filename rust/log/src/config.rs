use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct GrpcLogConfig {
    #[serde(default = "GrpcLogConfig::default_host")]
    pub host: String,
    #[serde(default = "GrpcLogConfig::default_port")]
    pub port: u16,
    #[serde(default = "GrpcLogConfig::default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "GrpcLogConfig::default_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

impl GrpcLogConfig {
    fn default_host() -> String {
        "logservice.chroma".to_string()
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
}

impl Default for GrpcLogConfig {
    fn default() -> Self {
        GrpcLogConfig {
            host: GrpcLogConfig::default_host(),
            port: GrpcLogConfig::default_port(),
            connect_timeout_ms: GrpcLogConfig::default_connect_timeout_ms(),
            request_timeout_ms: GrpcLogConfig::default_request_timeout_ms(),
        }
    }
}

#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct SqliteLogConfig {
    pub tenant_id: String,
    pub topic_namespace: String,
}

impl Default for SqliteLogConfig {
    fn default() -> Self {
        SqliteLogConfig {
            tenant_id: "default".to_string(),
            topic_namespace: "default".to_string(),
        }
    }
}

#[derive(Deserialize, Clone, Serialize, Debug)]
pub enum LogConfig {
    #[serde(alias = "grpc")]
    Grpc(GrpcLogConfig),
    #[serde(alias = "sqlite")]
    Sqlite(SqliteLogConfig),
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig::Grpc(GrpcLogConfig::default())
    }
}
