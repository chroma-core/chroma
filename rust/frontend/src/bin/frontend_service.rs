use chroma_frontend::frontend_service_entrypoint;

#[tokio::main]
async fn main() {
    frontend_service_entrypoint().await;
}
