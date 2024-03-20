pub(crate) mod config;
pub(crate) mod log;

use crate::{
    config::{Configurable, WorkerConfig},
    errors::ChromaError,
};

pub(crate) async fn from_config(
    config: &WorkerConfig,
) -> Result<Box<dyn log::Log>, Box<dyn ChromaError>> {
    match &config.log {
        crate::log::config::LogConfig::Grpc(_) => {
            Ok(Box::new(log::GrpcLog::try_from_config(config).await?))
        }
    }
}
