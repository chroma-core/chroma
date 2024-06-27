use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct GrpcSysDbConfig {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) connect_timeout_ms: u64,
    pub(crate) request_timeout_ms: u64,
}

#[derive(Deserialize)]
pub(crate) enum SysDbConfig {
    Grpc(GrpcSysDbConfig),
}
