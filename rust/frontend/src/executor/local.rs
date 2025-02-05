use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::ChromaError;

#[derive(Debug, Clone)]
pub struct LocalExecutor {
    // ...
}

#[async_trait]
impl Configurable<()> for LocalExecutor {
    async fn try_from_config(_config: &()) -> Result<Self, Box<dyn ChromaError>> {
        Ok(Self {})
    }
}
