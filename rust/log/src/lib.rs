pub mod config;
pub mod grpc_log;
pub mod in_memory_log;
pub mod local_compaction_manager;
#[allow(clippy::module_inception)]
mod log;
pub mod sqlite_log;
pub mod test;
pub mod types;

use chroma_config::{registry::Injectable, Configurable};
use chroma_error::ChromaError;
use chroma_system::System;
use config::LogConfig;
pub use local_compaction_manager::*;
pub use log::*;
pub use types::*;

use async_trait::async_trait;

impl Injectable for Log {}

#[async_trait]
impl Configurable<(LogConfig, System)> for Log {
    async fn try_from_config(
        config: &(LogConfig, System),
        registry: &chroma_config::registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (config, system) = config;
        let res = match &config {
            LogConfig::Grpc(grpc_log_config) => Self::Grpc(
                grpc_log::GrpcLog::try_from_config(
                    &(grpc_log_config.clone(), system.clone()),
                    registry,
                )
                .await?,
            ),
            LogConfig::Sqlite(sqlite_log_config) => Self::Sqlite(
                sqlite_log::SqliteLog::try_from_config(sqlite_log_config, registry).await?,
            ),
        };

        registry.register(res.clone());
        Ok(res)
    }
}
