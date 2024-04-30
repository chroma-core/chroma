pub(crate) mod cached_log;
pub(crate) mod config;
pub(crate) mod log;

use crate::{config::Configurable, errors::ChromaError};

use self::config::LogConfig;

pub(crate) async fn from_config(
    config: &LogConfig,
) -> Result<Box<dyn log::Log>, Box<dyn ChromaError>> {
    match &config {
        crate::log::config::LogConfig::Grpc(_) => {
            Ok(Box::new(log::GrpcLog::try_from_config(config).await?))
        }
    }
}
