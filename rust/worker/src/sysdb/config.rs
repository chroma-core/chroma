use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct GrpcSysDbConfig {
    pub(crate) host: String,
    pub(crate) port: u16,
}

#[derive(Deserialize)]
pub(crate) enum SysDbConfig {
    Grpc(GrpcSysDbConfig),
}
