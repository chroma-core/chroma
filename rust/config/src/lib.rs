use async_trait::async_trait;
use chroma_error::ChromaError;

/// # Description
/// A trait for configuring a struct from a config object.
/// # Notes
/// This trait is used to configure structs from the config object.
/// Components that need to be configured from the config object should implement this trait.
#[async_trait]
pub trait Configurable<T> {
    async fn try_from_config(worker_config: &T) -> Result<Self, Box<dyn ChromaError>>
    where
        Self: Sized;
}
