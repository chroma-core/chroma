use worker::query_service_entrypoint;

#[tokio::main]
async fn main() {
    query_service_entrypoint().await;
}
