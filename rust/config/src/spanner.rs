use std::time::Duration;

use google_cloud_spanner::client::ChannelConfig;
use google_cloud_spanner::session::SessionConfig;
use serde::{Deserialize, Serialize};

/// Session pool configuration for Spanner connections.
///
/// The default values are tuned for production workloads with higher concurrency and longer
/// timeouts than the library defaults.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SpannerSessionPoolConfig {
    /// How long to wait for a session before timing out.  Default: 30 seconds.
    #[serde(default = "SpannerSessionPoolConfig::default_session_get_timeout_secs")]
    pub session_get_timeout_secs: u64,
    /// Maximum concurrent sessions.  Default: 400.
    #[serde(default = "SpannerSessionPoolConfig::default_max_opened")]
    pub max_opened: usize,
    /// Minimum sessions to keep warm.  Default: 25.
    #[serde(default = "SpannerSessionPoolConfig::default_min_opened")]
    pub min_opened: usize,
}

impl SpannerSessionPoolConfig {
    fn default_session_get_timeout_secs() -> u64 {
        30
    }

    fn default_max_opened() -> usize {
        400
    }

    fn default_min_opened() -> usize {
        25
    }

    /// Converts this configuration to the library's `SessionConfig`.
    pub fn to_session_config(&self) -> SessionConfig {
        let mut config = SessionConfig::default();
        config.session_get_timeout = Duration::from_secs(self.session_get_timeout_secs);
        config.max_opened = self.max_opened;
        config.min_opened = self.min_opened;
        config
    }
}

impl Default for SpannerSessionPoolConfig {
    fn default() -> Self {
        Self {
            session_get_timeout_secs: Self::default_session_get_timeout_secs(),
            max_opened: Self::default_max_opened(),
            min_opened: Self::default_min_opened(),
        }
    }
}

/// Channel configuration for gRPC connections to Spanner.
///
/// Controls the number of gRPC channels and their timeouts.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SpannerChannelConfig {
    /// Number of gRPC channels.  Default: 4.
    #[serde(default = "SpannerChannelConfig::default_num_channels")]
    pub num_channels: usize,
    /// Connection timeout in seconds.  Default: 30.
    #[serde(default = "SpannerChannelConfig::default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    /// Request timeout in seconds.  Default: 30.
    #[serde(default = "SpannerChannelConfig::default_timeout_secs")]
    pub timeout_secs: u64,
}

impl SpannerChannelConfig {
    fn default_num_channels() -> usize {
        4
    }

    fn default_connect_timeout_secs() -> u64 {
        30
    }

    fn default_timeout_secs() -> u64 {
        30
    }

    /// Converts this configuration to the library's `ChannelConfig`.
    pub fn to_channel_config(&self) -> ChannelConfig {
        ChannelConfig {
            num_channels: self.num_channels,
            connect_timeout: Duration::from_secs(self.connect_timeout_secs),
            timeout: Duration::from_secs(self.timeout_secs),
        }
    }
}

impl Default for SpannerChannelConfig {
    fn default() -> Self {
        Self {
            num_channels: Self::default_num_channels(),
            connect_timeout_secs: Self::default_connect_timeout_secs(),
            timeout_secs: Self::default_timeout_secs(),
        }
    }
}

/// Configuration for connecting to a Spanner emulator (local development)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SpannerEmulatorConfig {
    #[serde(default = "SpannerEmulatorConfig::default_host")]
    pub host: String,
    #[serde(default = "SpannerEmulatorConfig::default_grpc_port")]
    pub grpc_port: u16,
    #[serde(default = "SpannerEmulatorConfig::default_rest_port")]
    pub rest_port: u16,
    #[serde(default = "SpannerEmulatorConfig::default_project")]
    pub project: String,
    #[serde(default = "SpannerEmulatorConfig::default_instance")]
    pub instance: String,
    #[serde(default = "SpannerEmulatorConfig::default_database")]
    pub database: String,
    #[serde(default)]
    pub session_pool: SpannerSessionPoolConfig,
    #[serde(default)]
    pub channel: SpannerChannelConfig,
}

impl Default for SpannerEmulatorConfig {
    fn default() -> Self {
        Self {
            host: Self::default_host(),
            grpc_port: Self::default_grpc_port(),
            rest_port: Self::default_rest_port(),
            project: Self::default_project(),
            instance: Self::default_instance(),
            database: Self::default_database(),
            session_pool: SpannerSessionPoolConfig::default(),
            channel: SpannerChannelConfig::default(),
        }
    }
}

impl SpannerEmulatorConfig {
    fn default_host() -> String {
        "spanner.chroma.svc.cluster.local".to_string()
    }
    fn default_grpc_port() -> u16 {
        9010
    }
    fn default_rest_port() -> u16 {
        9020
    }
    fn default_project() -> String {
        "local-project".to_string()
    }
    fn default_instance() -> String {
        "test-instance".to_string()
    }
    fn default_database() -> String {
        "local-database".to_string()
    }

    /// Returns the database path in the format required by the Spanner client
    pub fn database_path(&self) -> String {
        format!(
            "projects/{}/instances/{}/databases/{}",
            self.project, self.instance, self.database
        )
    }

    /// Returns the gRPC endpoint for SPANNER_EMULATOR_HOST
    pub fn grpc_endpoint(&self) -> String {
        format!("{}:{}", self.host, self.grpc_port)
    }

    /// Returns the REST endpoint for admin operations
    pub fn rest_endpoint(&self) -> String {
        format!("http://{}:{}", self.host, self.rest_port)
    }
}

/// Configuration for connecting to Google Cloud Spanner.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SpannerGcpConfig {
    #[serde(default = "SpannerGcpConfig::default_project")]
    pub project: String,
    #[serde(default = "SpannerGcpConfig::default_instance")]
    pub instance: String,
    #[serde(default = "SpannerGcpConfig::default_database")]
    pub database: String,
    #[serde(default)]
    pub session_pool: SpannerSessionPoolConfig,
    #[serde(default)]
    pub channel: SpannerChannelConfig,
}

impl SpannerGcpConfig {
    // points to staging.
    fn default_project() -> String {
        "chroma-398322".to_string()
    }

    fn default_instance() -> String {
        "sysdb-nam-eur-asia3".to_string()
    }

    fn default_database() -> String {
        "sysdb".to_string()
    }

    /// Returns the database path in the format required by the Spanner client
    pub fn database_path(&self) -> String {
        format!(
            "projects/{}/instances/{}/databases/{}",
            self.project, self.instance, self.database
        )
    }
}

impl Default for SpannerGcpConfig {
    fn default() -> Self {
        Self {
            project: Self::default_project(),
            instance: Self::default_instance(),
            database: Self::default_database(),
            session_pool: SpannerSessionPoolConfig::default(),
            channel: SpannerChannelConfig::default(),
        }
    }
}

/// Spanner configuration - either emulator or GCP (mutually exclusive)
/// Defaults to emulator with standard local settings.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum SpannerConfig {
    /// Emulator configuration for local development
    Emulator(SpannerEmulatorConfig),
    Gcp(SpannerGcpConfig),
}

impl SpannerConfig {
    /// Returns the database path in the format required by the Spanner client.
    pub fn database_path(&self) -> String {
        match self {
            Self::Emulator(e) => e.database_path(),
            Self::Gcp(g) => g.database_path(),
        }
    }

    /// Returns the session pool configuration for this Spanner instance.
    pub fn session_config(&self) -> SessionConfig {
        match self {
            Self::Emulator(e) => e.session_pool.to_session_config(),
            Self::Gcp(g) => g.session_pool.to_session_config(),
        }
    }

    /// Returns the channel configuration for gRPC connections.
    pub fn channel_config(&self) -> ChannelConfig {
        match self {
            Self::Emulator(e) => e.channel.to_channel_config(),
            Self::Gcp(g) => g.channel.to_channel_config(),
        }
    }
}

impl Default for SpannerConfig {
    fn default() -> Self {
        Self::Emulator(SpannerEmulatorConfig::default())
    }
}
