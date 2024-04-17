use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct GrpcLogConfig {
    pub(crate) host: String,
    pub(crate) port: u16,
}

#[derive(Deserialize)]
pub(crate) enum LogConfig {
    Grpc(GrpcLogConfig),
}
