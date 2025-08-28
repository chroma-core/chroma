pub mod assignment;
pub mod helpers;
pub mod registry;

use async_trait::async_trait;
use chroma_error::ChromaError;
use registry::Registry;
use thiserror::Error;

/// # Description
/// A trait for configuring a struct from a config object.
/// # Notes
/// This trait is used to configure structs from the config object.
/// Components that need to be configured from the config object should implement this trait.
#[async_trait]
pub trait Configurable<T, E = Box<dyn ChromaError>> {
    async fn try_from_config(config: &T, registry: &Registry) -> Result<Self, E>
    where
        Self: Sized;
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Config error: {0}")]
    ConfigError(#[from] figment::Error),
}

impl ChromaError for ConfigError {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::Internal
    }
}
