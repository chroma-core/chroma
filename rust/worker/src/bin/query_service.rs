use worker::query_service_entrypoint;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    query_service_entrypoint().await;
}
