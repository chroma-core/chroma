#[tokio::main]
async fn main() {
    worker::compaction_client_entrypoint().await;
}
