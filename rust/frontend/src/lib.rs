mod server;
use server::FrontendServer;

pub async fn frontend_service_entrypoint() {
    let server = FrontendServer::new();
    FrontendServer::run(server).await;
}
