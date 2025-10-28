use chroma_config::assignment;
use chroma_config::helpers::deserialize_duration_from_seconds;
use chroma_index::config::SpannProviderConfig;
use chroma_sysdb::SysDbConfig;
use chroma_tracing::{OtelFilter, OtelFilterLevel};
use figment::providers::{Env, Format, Yaml};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, time::Duration};

const DEFAULT_CONFIG_PATH: &str = "./chroma_config.yaml";

#[derive(Deserialize, Serialize, Debug)]
/// # Description
/// The RootConfig for all chroma services this is a YAML file that
/// is shared between all services, and secondarily, fields can be
/// populated from environment variables. The environment variables
/// are prefixed with CHROMA_ and are uppercase. Values in the envionment
/// variables take precedence over values in the YAML file.
/// By default, it is read from the current working directory,
/// with the filename chroma_config.yaml.
pub struct RootConfig {
    // The root config object wraps the worker config object so that
    // we can share the same config file between multiple services.
    #[serde(default)]
    pub query_service: QueryServiceConfig,
    #[serde(default)]
    pub compaction_service: CompactionServiceConfig,
}

impl RootConfig {
    /// # Description
    /// Load the config from the default location.
    /// # Returns
    /// The config object.
    /// # Panics
    /// - If the config file cannot be read.
    /// - If the config file is not valid YAML.
    /// - If the config file does not contain the required fields.
    /// - If the config file contains invalid values.
    /// - If the environment variables contain invalid values.
    /// # Notes
    /// The default location is the current working directory, with the filename chroma_config.yaml.
    /// The environment variables are prefixed with CHROMA_ and are uppercase.
    /// Values in the envionment variables take precedence over values in the YAML file.
    pub fn load() -> Self {
        Self::load_from_path(DEFAULT_CONFIG_PATH)
    }

    /// # Description
    /// Load the config from a specific location.
    /// # Arguments
    /// - path: The path to the config file.
    /// # Returns
    /// The config object.
    /// # Panics
    /// - If the config file cannot be read.
    /// - If the config file is not valid YAML.
    /// - If the config file does not contain the required fields.
    /// - If the config file contains invalid values.
    /// - If the environment variables contain invalid values.
    /// # Notes
    /// The environment variables are prefixed with CHROMA_ and are uppercase.
    /// Values in the envionment variables take precedence over values in the YAML file.
    // NOTE:  Copied to ../load/src/config.rs.
    pub fn load_from_path(path: &str) -> Self {
        // Unfortunately, figment doesn't support environment variables with underscores. So we have to map and replace them.
        // Excluding our own environment variables, which are prefixed with CHROMA_.
        eprintln!("loading config from {path}");
        eprintln!(
            "{}",
            std::fs::read_to_string(path).unwrap_or("<ERROR>".to_string())
        );
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

impl Default for RootConfig {
    fn default() -> Self {
        Self::load()
    }
}

#[derive(Default, Deserialize, Serialize)]
/// # Description
/// The primary config for the worker service.
/// ## Description of parameters
/// - my_ip: The IP address of the worker service. Used for memberlist assignment. Must be provided.
/// - assignment_policy: The assignment policy to use. Must be provided.
/// # Notes
/// In order to set the enviroment variables, you must prefix them with CHROMA_WORKER__<FIELD_NAME>.
/// For example, to set my_ip, you would set CHROMA_WORKER__MY_IP.
/// Each submodule that needs to be configured from the config object should implement the Configurable trait and
/// have its own field in this struct for its Config struct.
#[derive(Debug)]
pub struct QueryServiceConfig {
    #[serde(default = "QueryServiceConfig::default_service_name")]
    pub service_name: String,
    #[serde(default = "QueryServiceConfig::default_otel_endpoint")]
    pub otel_endpoint: String,
    #[serde(default = "QueryServiceConfig::default_otel_filters")]
    pub otel_filters: Vec<OtelFilter>,
    #[allow(dead_code)]
    #[serde(default = "QueryServiceConfig::default_my_member_id")]
    pub my_member_id: String,
    #[serde(default = "QueryServiceConfig::default_my_port")]
    pub my_port: u16,
    #[allow(dead_code)]
    #[serde(default)]
    pub assignment_policy: assignment::config::AssignmentPolicyConfig,
    #[allow(dead_code)]
    #[serde(default)]
    pub memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig,
    #[serde(default)]
    pub sysdb: SysDbConfig,
    #[serde(default)]
    pub storage: chroma_storage::config::StorageConfig,
    #[serde(default)]
    pub log: chroma_log::config::LogConfig,
    #[serde(default)]
    pub dispatcher: chroma_system::DispatcherConfig,
    #[serde(default)]
    pub blockfile_provider: chroma_blockstore::config::BlockfileProviderConfig,
    #[serde(default)]
    pub hnsw_provider: chroma_index::config::HnswProviderConfig,
    #[serde(default = "QueryServiceConfig::default_fetch_log_batch_size")]
    pub fetch_log_batch_size: u32,
    #[serde(default)]
    pub spann_provider: SpannProviderConfig,
    #[serde(default)]
    pub jemalloc_pprof_server_port: Option<u16>,
    #[serde(
        rename = "grpc_shutdown_grace_period_seconds",
        deserialize_with = "deserialize_duration_from_seconds",
        default = "QueryServiceConfig::default_grpc_shutdown_grace_period"
    )]
    pub grpc_shutdown_grace_period: Duration,
    // TODO: This is a temporary config to enable bm25 for certain tenants.
    // This should be removed once we have collection schema ready.
    #[serde(default)]
    pub bm25_tenant: HashSet<String>,
}

impl QueryServiceConfig {
    fn default_service_name() -> String {
        "query-service".to_string()
    }

    fn default_otel_endpoint() -> String {
        "http://otel-collector:4317".to_string()
    }

    fn default_otel_filters() -> Vec<OtelFilter> {
        vec![OtelFilter {
            crate_name: "worker".to_string(),
            filter_level: OtelFilterLevel::Trace,
        }]
    }

    fn default_my_member_id() -> String {
        "query-service-0".to_string()
    }

    fn default_my_port() -> u16 {
        50051
    }

    fn default_fetch_log_batch_size() -> u32 {
        100
    }

    fn default_grpc_shutdown_grace_period() -> Duration {
        Duration::from_secs(1)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
/// # Description
/// The primary config for the compaction service.
/// ## Description of parameters
/// - my_ip: The IP address of the worker service. Used for memberlist assignment. Must be provided.
/// - assignment_policy: The assignment policy to use. Must be provided.
/// # Notes
/// In order to set the enviroment variables, you must prefix them with CHROMA_COMPACTOR__<FIELD_NAME>.
/// For example, to set my_ip, you would set CHROMA_COMPACTOR__MY_IP.
/// Each submodule that needs to be configured from the config object should implement the Configurable trait and
/// have its own field in this struct for its Config struct.
pub struct CompactionServiceConfig {
    #[serde(default = "CompactionServiceConfig::default_service_name")]
    pub service_name: String,
    #[serde(default = "CompactionServiceConfig::default_otel_endpoint")]
    pub otel_endpoint: String,
    #[serde(default = "CompactionServiceConfig::default_otel_filters")]
    pub otel_filters: Vec<OtelFilter>,
    #[serde(default = "CompactionServiceConfig::default_my_member_id")]
    pub my_member_id: String,
    #[allow(dead_code)]
    #[serde(default = "CompactionServiceConfig::default_my_port")]
    pub my_port: u16,
    #[serde(default)]
    pub assignment_policy: assignment::config::AssignmentPolicyConfig,
    #[serde(default)]
    pub memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig,
    #[serde(default)]
    pub sysdb: SysDbConfig,
    #[serde(default)]
    pub storage: chroma_storage::config::StorageConfig,
    #[serde(default)]
    pub log: chroma_log::config::LogConfig,
    #[serde(default)]
    pub heap_service: s3heap_service::client::HeapServiceConfig,
    #[serde(default)]
    pub dispatcher: chroma_system::DispatcherConfig,
    #[serde(default)]
    pub compactor: crate::compactor::config::CompactorConfig,
    #[serde(default)]
    pub task_runner: Option<crate::compactor::config::TaskRunnerConfig>,
    #[serde(default)]
    pub blockfile_provider: chroma_blockstore::config::BlockfileProviderConfig,
    #[serde(default)]
    pub hnsw_provider: chroma_index::config::HnswProviderConfig,
    #[serde(default)]
    pub spann_provider: chroma_index::config::SpannProviderConfig,
    #[serde(default)]
    pub jemalloc_pprof_server_port: Option<u16>,
}

impl CompactionServiceConfig {
    fn default_service_name() -> String {
        "compaction-service".to_string()
    }

    fn default_otel_endpoint() -> String {
        "http://otel-collector:4317".to_string()
    }

    fn default_otel_filters() -> Vec<OtelFilter> {
        vec![OtelFilter {
            crate_name: "compaction_service".to_string(),
            filter_level: OtelFilterLevel::Trace,
        }]
    }

    fn default_my_member_id() -> String {
        "compaction-service-0".to_string()
    }

    fn default_my_port() -> u16 {
        50051
    }
}
