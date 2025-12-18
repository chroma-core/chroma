use chroma_storage::config::StorageConfig;
use chroma_tracing::{OtelFilter, OtelFilterLevel};
use figment::providers::{Env, Format, Yaml};
use serde::{Deserialize, Serialize};

const DEFAULT_CONFIG_PATH: &str = "./chroma_config.yaml";

/// Configuration for connecting to a Spanner emulator (local development)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SpannerEmulatorConfig {
    pub host: String,
    pub grpc_port: u16,
    pub rest_port: u16,
    pub project: String,
    pub instance: String,
    pub database: String,
}

impl SpannerEmulatorConfig {
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

/// Spanner configuration - either emulator or GCP (mutually exclusive)
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct SpannerConfig {
    /// Emulator configuration for local development
    pub emulator: Option<SpannerEmulatorConfig>,
    // TODO: Add GCP config later
    // pub gcp: Option<SpannerGcpConfig>,
}

#[derive(Serialize, Deserialize)]
pub struct SysDbServiceConfig {
    #[serde(default = "SysDbServiceConfig::default_service_name")]
    pub service_name: String,
    #[serde(default = "SysDbServiceConfig::default_otel_endpoint")]
    pub otel_endpoint: String,
    #[serde(default = "SysDbServiceConfig::default_otel_filters")]
    pub otel_filters: Vec<OtelFilter>,
    #[serde(default = "SysDbServiceConfig::default_port")]
    pub port: u16,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub spanner: SpannerConfig,
}

impl SysDbServiceConfig {
    fn default_service_name() -> String {
        "rust-sysdb-service".to_string()
    }

    fn default_otel_endpoint() -> String {
        "http://otel-collector.chroma.svc.cluster.local:4317".to_string()
    }

    fn default_otel_filters() -> Vec<OtelFilter> {
        vec![OtelFilter {
            crate_name: "rust_sysdb".to_string(),
            filter_level: OtelFilterLevel::Trace,
        }]
    }

    fn default_port() -> u16 {
        50051
    }
}

#[derive(Serialize, Deserialize)]
pub struct RootConfig {
    pub sysdb_service: SysDbServiceConfig,
}

impl RootConfig {
    pub fn load() -> Self {
        Self::load_from_path(DEFAULT_CONFIG_PATH)
    }

    pub fn load_from_path(path: &str) -> Self {
        println!("loading config from {path}");
        println!(
            r#"Full config is:
================================================================================
{}
================================================================================
"#,
            std::fs::read_to_string(path)
                .expect("should be able to open and read config to string")
        );
        // Unfortunately, figment doesn't support environment variables with underscores. So we have to map and replace them.
        // Excluding our own environment variables, which are prefixed with CHROMA_.
        let mut f = figment::Figment::from(Env::prefixed("CHROMA_").map(|k| match k {
            k if k == "my_member_id" => k.into(),
            k => k.as_str().replace("__", ".").into(),
        }));
        if std::path::Path::new(path).exists() {
            f = figment::Figment::from(Yaml::file(path)).merge(f);
        }
        // Apply defaults - this seems to be the best way to do it.
        // https://github.com/SergioBenitez/Figment/issues/77#issuecomment-1642490298
        // f = f.join(Serialized::default(
        //     "worker.num_indexing_threads",
        //     num_cpus::get(),
        // ));
        let res = f.extract();
        match res {
            Ok(config) => config,
            Err(e) => panic!("Error loading config: {}", e),
        }
    }
}
