#[tokio::main]
async fn main() {
    worker::work_queue::server::service_entrypoint().await
}
