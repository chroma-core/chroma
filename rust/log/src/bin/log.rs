#[tokio::main]
async fn main() {
    #[cfg(feature = "server")]
    chroma_log::log_entrypoint().await;
}
