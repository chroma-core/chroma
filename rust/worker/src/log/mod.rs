pub(crate) mod config;
pub mod log;
use self::config::LogConfig;
use chroma_config::Configurable;
use chroma_error::ChromaError;

pub(crate) async fn from_config(config: &LogConfig) -> Result<Box<log::Log>, Box<dyn ChromaError>> {
    match &config {
        crate::log::config::LogConfig::Grpc(_) => Ok(Box::new(log::Log::Grpc(
            log::GrpcLog::try_from_config(config).await?,
        ))),
    }
}
