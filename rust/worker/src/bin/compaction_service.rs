use worker::compaction_service_entrypoint;

#[tokio::main]
async fn main() {
    compaction_service_entrypoint().await;
}
