use worker::worker_entrypoint;

#[tokio::main]
async fn main() {
    worker_entrypoint().await;
}
