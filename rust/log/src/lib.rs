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
use config::LogConfig;
pub use local_compaction_manager::*;
pub use log::*;
pub use types::*;

#[cfg(feature = "server")]
mod server;
#[cfg(feature = "server")]
mod state_hash_table;
#[cfg(feature = "server")]
use server::{LogServer, LogServerConfig};

use async_trait::async_trait;

impl Injectable for Log {}

#[async_trait]
impl Configurable<LogConfig> for Log {
    async fn try_from_config(
        config: &LogConfig,
        registry: &chroma_config::registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let res = match &config {
            LogConfig::Grpc(grpc_log_config) => {
                Self::Grpc(grpc_log::GrpcLog::try_from_config(grpc_log_config, registry).await?)
            }
            LogConfig::Sqlite(sqlite_log_config) => Self::Sqlite(
                sqlite_log::SqliteLog::try_from_config(sqlite_log_config, registry).await?,
            ),
        };

        registry.register(res.clone());
        Ok(res)
    }
}

// Entrypoint for the wal3 based log server
#[cfg(feature = "server")]
pub async fn log_entrypoint() {
    let config = LogServerConfig::default();
    let registry = chroma_config::registry::Registry::new();
    let log_server = LogServer::try_from_config(&config, &registry)
        .await
        .expect("Failed to create log server");

    let server_join_handle = tokio::spawn(async move {
        let _ = crate::server::LogServer::run(log_server).await;
    });

    match server_join_handle.await {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Error terminating server: {:?}", e);
        }
    }
}
