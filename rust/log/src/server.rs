use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_types::chroma_proto::{
    log_service_server::LogService, GetAllCollectionInfoToCompactRequest,
    GetAllCollectionInfoToCompactResponse, PullLogsRequest, PullLogsResponse, PushLogsRequest,
    PushLogsResponse, UpdateCollectionLogOffsetRequest, UpdateCollectionLogOffsetResponse,
};
use serde::{Deserialize, Serialize};
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Request, Response, Status};

//////////////////////// Log Server ////////////////////////
#[derive(Clone)]
pub struct LogServer {
    config: LogServerConfig,
}

#[async_trait]
impl LogService for LogServer {
    async fn push_logs(
        &self,
        _request: Request<PushLogsRequest>,
    ) -> Result<Response<PushLogsResponse>, Status> {
        todo!("Implement wal3 backed push_logs here")
    }

    async fn pull_logs(
        &self,
        _request: Request<PullLogsRequest>,
    ) -> Result<Response<PullLogsResponse>, Status> {
        todo!("Implement wal3 backed pull_logs here")
    }

    async fn get_all_collection_info_to_compact(
        &self,
        _request: Request<GetAllCollectionInfoToCompactRequest>,
    ) -> Result<Response<GetAllCollectionInfoToCompactResponse>, Status> {
        todo!("Implement wal3 backed get_all_collection_info_to_compact here")
    }

    async fn update_collection_log_offset(
        &self,
        _request: Request<UpdateCollectionLogOffsetRequest>,
    ) -> Result<Response<UpdateCollectionLogOffsetResponse>, Status> {
        todo!("Implement wal3 backed update_collection_log_offset here")
    }
}

impl LogServer {
    pub(crate) async fn run(log_server: LogServer) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("[::]:{}", log_server.config.port).parse().unwrap();
        println!("Log listening on {}", addr);
        let server = Server::builder().add_service(
            chroma_types::chroma_proto::log_service_server::LogServiceServer::new(
                log_server.clone(),
            ),
        );

        let server = server.serve_with_shutdown(addr, async {
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(sigterm) => sigterm,
                Err(e) => {
                    tracing::error!("Failed to create signal handler: {:?}", e);
                    return;
                }
            };
            sigterm.recv().await;
            tracing::info!("Received SIGTERM, shutting down");
        });

        server.await?;

        Ok(())
    }
}

/////////////////////////// Config ///////////////////////////

fn default_otel_service_name() -> String {
    "rust-log-service".to_string()
}

fn default_port() -> u16 {
    50051
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct OpenTelemetryConfig {
    pub endpoint: String,
    #[serde(default = "default_otel_service_name")]
    pub service_name: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct LogServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    pub opentelemetry: Option<OpenTelemetryConfig>,
}

impl Default for LogServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            opentelemetry: None,
        }
    }
}

#[async_trait]
impl Configurable<LogServerConfig> for LogServer {
    async fn try_from_config(
        config: &LogServerConfig,
        _registry: &chroma_config::registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        Ok(Self {
            config: config.clone(),
        })
    }
}
