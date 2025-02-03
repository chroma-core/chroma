use chroma_config::assignment;
use chroma_sysdb::SysDbConfig;
use figment::providers::{Env, Format, Yaml};
use serde::{Deserialize, Serialize};

const DEFAULT_CONFIG_PATH: &str = "./chroma_config.yaml";

#[derive(Deserialize, Serialize)]
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
pub struct QueryServiceConfig {
    #[serde(default = "QueryServiceConfig::default_service_name")]
    pub service_name: String,
    #[serde(default = "QueryServiceConfig::default_otel_endpoint")]
    pub otel_endpoint: String,
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
}

impl QueryServiceConfig {
    fn default_service_name() -> String {
        "query-service".to_string()
    }

    fn default_otel_endpoint() -> String {
        "http://otel-collector:4317".to_string()
    }

    fn default_my_member_id() -> String {
        "query-service-0".to_string()
    }

    fn default_my_port() -> u16 {
        50051
    }
}

#[derive(Default, Deserialize, Serialize)]
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
    pub dispatcher: chroma_system::DispatcherConfig,
    #[serde(default)]
    pub compactor: crate::compactor::config::CompactorConfig,
    #[serde(default)]
    pub blockfile_provider: chroma_blockstore::config::BlockfileProviderConfig,
    #[serde(default)]
    pub hnsw_provider: chroma_index::config::HnswProviderConfig,
}

impl CompactionServiceConfig {
    fn default_service_name() -> String {
        "compaction-service".to_string()
    }

    fn default_otel_endpoint() -> String {
        "http://otel-collector:4317".to_string()
    }

    fn default_my_member_id() -> String {
        "compaction-service-0".to_string()
    }

    fn default_my_port() -> u16 {
        50051
    }
}
