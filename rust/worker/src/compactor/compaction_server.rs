use async_trait::async_trait;
use chroma_system::ComponentHandle;
use chroma_types::chroma_proto::{
    compactor_server::{Compactor, CompactorServer},
    CompactionRequest, CompactionResponse,
};
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::oneshot,
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::trace_span;

use crate::compactor::{OneOffCompactionMessage, RegisterOnReadySignal};

use super::CompactionManager;

pub struct CompactionServer {
    pub manager: ComponentHandle<CompactionManager>,
    pub port: u16,
}

impl CompactionServer {
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("[::]:{}", self.port).parse().unwrap();
        tracing::info!("Compaction server listening at {addr}");

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_not_serving::<CompactorServer<Self>>()
            .await;

        // Add readiness listener
        let (on_ready_tx, on_ready_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = on_ready_rx.await;
            health_reporter.set_serving::<CompactorServer<Self>>().await;
        });

        // (Request the compactor to notify us when it's ready)
        self.manager
            .send(RegisterOnReadySignal { on_ready_tx }, None)
            .await?;

        let server = Server::builder()
            .add_service(health_service)
            .add_service(CompactorServer::new(self));
        server
            .serve_with_shutdown(addr, async {
                match signal(SignalKind::terminate()) {
                    Ok(mut sigterm) => {
                        sigterm.recv().await;
                        tracing::info!("Received SIGTERM, shutting down")
                    }
                    Err(err) => {
                        tracing::error!("Failed to create SIGTERM handler: {err}")
                    }
                }
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Compactor for CompactionServer {
    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let compaction_span = trace_span!("CompactionRequest", request = ?request);
        self.manager
            .receiver()
            .send(
                OneOffCompactionMessage::try_from(request.into_inner())
                    .map_err(|e| Status::invalid_argument(e.to_string()))?,
                Some(compaction_span),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CompactionResponse {}))
    }
}
