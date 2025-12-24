use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SpannerGcpConfig {
    #[serde(default = "SpannerGcpConfig::default_project")]
    pub project: String,
    #[serde(default = "SpannerGcpConfig::default_instance")]
    pub instance: String,
    #[serde(default = "SpannerGcpConfig::default_database")]
    pub database: String,
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

impl Default for SpannerConfig {
    fn default() -> Self {
        Self::Gcp(SpannerGcpConfig::default())
    }
}
