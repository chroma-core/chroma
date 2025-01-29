use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone, Serialize)]
pub struct GrpcLogConfig {
    pub host: String,
    pub port: u16,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
}

#[derive(Deserialize, Clone, Serialize)]
pub enum LogConfig {
    Grpc(GrpcLogConfig),
}
