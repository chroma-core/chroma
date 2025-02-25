use std::sync::Arc;

use chroma_frontend::frontend_service_entrypoint;

#[tokio::main]
async fn main() {
    frontend_service_entrypoint(Arc::new(()) as _, Arc::new(()) as _, ()).await;
}
