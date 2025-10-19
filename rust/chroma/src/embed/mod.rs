#[cfg(feature = "ollama")]
pub mod ollama;

#[async_trait::async_trait]
pub trait EmbeddingFunction: Send + Sync + 'static {
    type Error: std::error::Error + std::fmt::Display;

    async fn embed(&self, batches: &[&str]) -> Result<Vec<Vec<f32>>, Self::Error>;
}
