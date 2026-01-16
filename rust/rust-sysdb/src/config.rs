use chroma_storage::config::{RegionalStorage, TopologicalStorage};
use chroma_tracing::{OtelFilter, OtelFilterLevel};
use chroma_types::{MultiCloudMultiRegionConfiguration, RegionName};
use figment::providers::{Env, Format, Yaml};
use serde::{Deserialize, Serialize};

pub use chroma_config::spanner::{SpannerConfig, SpannerEmulatorConfig};

/// Configuration for instantiating a SpannerBackend.
///
/// Bundles the Spanner connection config with region configuration.
pub struct SpannerBackendConfig<'a> {
    /// The Spanner connection configuration (emulator or GCP).
    pub spanner: &'a SpannerConfig,
    /// All regions in the topology this backend serves.
    pub regions: Vec<RegionName>,
    /// The local region for this instance (used for reads).
    pub local_region: RegionName,
}

const DEFAULT_CONFIG_PATH: &str = "./chroma_config.yaml";

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
    pub regions_and_topologies:
        MultiCloudMultiRegionConfiguration<RegionalStorage, TopologicalStorage>,
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
        } else {
            panic!("Config file {} does not exist", path);
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
