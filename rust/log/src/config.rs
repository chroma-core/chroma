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
    #[serde(default = "GrpcLogConfig::default_max_encoding_message_size")]
    pub max_encoding_message_size: usize,
    #[serde(default = "GrpcLogConfig::default_max_decoding_message_size")]
    pub max_decoding_message_size: usize,
    // NOTE(rescrv):  This is a hack to allow us to test two log services in parallel without
    // breaking the old service.  If alt host is specified, then the "use_alt_host_for_everything"
    // and "use_alt_for_collections" config parameters are consulted.  If
    // "use_alt_host_for_everything" is set (CI) or the collection being written is in the alt
    // collections vec (staging), then we can use the alt host.
    #[serde(default = "Option::default")]
    pub alt_host: Option<String>,
    #[serde(default)]
    pub use_alt_host_for_everything: bool,
    #[serde(default)]
    pub use_alt_for_tenants: Vec<String>,
    #[serde(default)]
    pub use_alt_for_collections: Vec<String>,
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

    fn default_max_encoding_message_size() -> usize {
        32_000_000
    }

    fn default_max_decoding_message_size() -> usize {
        32_000_000
    }
}

impl Default for GrpcLogConfig {
    fn default() -> Self {
        GrpcLogConfig {
            host: GrpcLogConfig::default_host(),
            port: GrpcLogConfig::default_port(),
            connect_timeout_ms: GrpcLogConfig::default_connect_timeout_ms(),
            request_timeout_ms: GrpcLogConfig::default_request_timeout_ms(),
            max_encoding_message_size: GrpcLogConfig::default_max_encoding_message_size(),
            max_decoding_message_size: GrpcLogConfig::default_max_decoding_message_size(),
            alt_host: None,
            use_alt_host_for_everything: false,
            use_alt_for_tenants: vec![],
            use_alt_for_collections: vec![],
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
