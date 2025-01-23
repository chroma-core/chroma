pub mod config;
#[allow(clippy::module_inception)]
pub mod log;
pub mod test;
pub mod tracing;

use chroma_config::Configurable;
use chroma_error::ChromaError;
use config::LogConfig;

pub async fn from_config(config: &LogConfig) -> Result<Box<log::Log>, Box<dyn ChromaError>> {
    match &config {
        config::LogConfig::Grpc(_) => Ok(Box::new(log::Log::Grpc(
            log::GrpcLog::try_from_config(config).await?,
        ))),
    }
}
