use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct GrpcLogConfig {
    pub host: String,
    pub port: u16,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
}

#[derive(Deserialize, Clone)]
pub enum LogConfig {
    Grpc(GrpcLogConfig),
}
