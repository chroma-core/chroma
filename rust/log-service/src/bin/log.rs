#[tokio::main]
async fn main() {
    chroma_log_service::log_entrypoint().await;
}
