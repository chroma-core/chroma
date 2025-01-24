use serde::Deserialize;

#[derive(Deserialize)]
pub struct GrpcLogConfig {
    pub host: String,
    pub port: u16,
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
}

#[derive(Deserialize)]
pub enum LogConfig {
    Grpc(GrpcLogConfig),
}
