use chroma_frontend::frontend_service_entrypoint;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    frontend_service_entrypoint(Arc::new(()) as _, Arc::new(()) as _, true).await;
}
