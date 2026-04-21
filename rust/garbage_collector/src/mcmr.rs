use std::{fmt::Debug, sync::Arc, time::Duration};

use chroma_config::{
    registry::Registry,
    spanner::{SpannerChannelConfig, SpannerConfig, SpannerSessionPoolConfig},
    Configurable,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::{config::StorageConfig, Storage};
use chroma_types::MultiCloudMultiRegionConfiguration;
use google_cloud_gax::conn::Environment;
use google_cloud_spanner::client::{
    ChannelConfig, Client as SpannerClient, ClientConfig as SpannerClientConfig,
};
use google_cloud_spanner::session::SessionConfig;
use thiserror::Error;
use tracing::Level;
use wal3::ReplicatedFragmentOptions;

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct RegionalStorageConfig {
    pub storage: StorageConfig,
}

#[derive(Clone, Debug)]
pub struct RegionalStorage {
    pub storage: Storage,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct TopologicalStorageConfig {
    pub spanner: SpannerConfig,
    pub repl: ReplicatedFragmentOptions,
}

#[derive(Clone)]
pub struct TopologicalStorage {
    pub spanner: SpannerClient,
    pub repl: ReplicatedFragmentOptions,
}

impl std::fmt::Debug for TopologicalStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopologicalStorage")
            .field("spanner", &"SpannerClient { ... }")
            .field("repl", &self.repl)
            .finish()
    }
}

pub type RegionsAndTopologiesConfig =
    MultiCloudMultiRegionConfiguration<RegionalStorageConfig, TopologicalStorageConfig>;
pub type RegionsAndTopologies =
    MultiCloudMultiRegionConfiguration<RegionalStorage, TopologicalStorage>;

#[derive(Debug, Error)]
pub enum RegionsAndTopologiesError {
    #[error("spanner error: {0}")]
    Spanner(#[from] google_cloud_spanner::client::Error),
    #[error("spanner auth error")]
    SpannerAuth(#[from] google_cloud_spanner::admin::google_cloud_auth::error::Error),
}

impl ChromaError for RegionsAndTopologiesError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

fn to_session_config(cfg: &SpannerSessionPoolConfig) -> SessionConfig {
    let mut config = SessionConfig::default();
    config.session_get_timeout = Duration::from_secs(cfg.session_get_timeout_secs);
    config.max_opened = cfg.max_opened;
    config.min_opened = cfg.min_opened;
    config
}

fn to_channel_config(cfg: &SpannerChannelConfig) -> ChannelConfig {
    ChannelConfig {
        num_channels: cfg.num_channels,
        connect_timeout: Duration::from_secs(cfg.connect_timeout_secs),
        timeout: Duration::from_secs(cfg.timeout_secs),
        http2_keep_alive_interval: Some(Duration::from_secs(cfg.http2_keep_alive_interval_secs)),
        keep_alive_timeout: Some(Duration::from_secs(cfg.keep_alive_timeout_secs)),
        keep_alive_while_idle: Some(cfg.keep_alive_while_idle),
    }
}

pub async fn instantiate_regions_and_topologies(
    config: Option<RegionsAndTopologiesConfig>,
    registry: &Registry,
) -> Result<Option<Arc<RegionsAndTopologies>>, Box<dyn ChromaError>> {
    let Some(config) = config else {
        return Ok(None);
    };

    let runtime = config
        .try_cast_async(
            |region| async move {
                Ok::<RegionalStorage, Box<dyn ChromaError>>(RegionalStorage {
                    storage: Storage::try_from_config(&region.storage, registry).await?,
                })
            },
            |topology| async move {
                let database_path = topology.spanner.database_path().clone();
                let session_config = to_session_config(topology.spanner.session_pool());
                let channel_config = to_channel_config(topology.spanner.channel());
                let config = match &topology.spanner {
                    SpannerConfig::Emulator(emulator) => SpannerClientConfig {
                        environment: Environment::Emulator(emulator.grpc_endpoint()),
                        session_config,
                        channel_config,
                        ..Default::default()
                    },
                    SpannerConfig::Gcp(_) => {
                        let mut config = SpannerClientConfig::default().with_auth().await.map_err(
                            |err| -> Box<dyn ChromaError> {
                                tracing::event!(Level::ERROR, name = "auth error", error = ?err);
                                Box::new(RegionsAndTopologiesError::from(err)) as _
                            },
                        )?;
                        config.session_config = session_config;
                        config.channel_config = channel_config;
                        config
                    }
                };
                let repl = topology.repl.clone();
                Ok::<TopologicalStorage, Box<dyn ChromaError>>(TopologicalStorage {
                    spanner: SpannerClient::new(database_path, config).await.map_err(
                        |err| -> Box<dyn ChromaError> {
                            Box::new(RegionsAndTopologiesError::from(err)) as _
                        },
                    )?,
                    repl,
                })
            },
        )
        .await?;

    Ok(Some(Arc::new(runtime)))
}
