pub mod config;
pub mod grpc_log;
pub mod in_memory_log;
#[allow(clippy::module_inception)]
mod log;
pub mod sqlite_log;
pub mod test;
pub mod types;

use chroma_config::Configurable;
use chroma_error::ChromaError;
use config::LogConfig;
pub use log::*;
pub use types::*;

pub async fn from_config(config: &LogConfig) -> Result<Box<log::Log>, Box<dyn ChromaError>> {
    match &config {
        config::LogConfig::Grpc(_) => Ok(Box::new(log::Log::Grpc(
            grpc_log::GrpcLog::try_from_config(config).await?,
        ))),
    }
}
