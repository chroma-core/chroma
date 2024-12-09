//! The main chroma-load service.

#[tokio::main]
async fn main() {
    chroma_load::entrypoint().await;
}
