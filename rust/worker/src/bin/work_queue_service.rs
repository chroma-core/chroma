#[tokio::main]
async fn main() {
    worker::work_queue::service_entrypoint().await
}
