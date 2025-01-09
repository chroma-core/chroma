use garbage_collector_library::garbage_collector_service_entrypoint;

#[tokio::main]
async fn main() {
    garbage_collector_service_entrypoint().await
}
