use worker::s3_test_entrypoint;

#[tokio::main]
async fn main() {
    s3_test_entrypoint().await;
}
