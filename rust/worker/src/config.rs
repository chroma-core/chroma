use async_trait::async_trait;
use figment::providers::{Env, Format, Serialized, Yaml};
use serde::Deserialize;

use crate::errors::ChromaError;

const DEFAULT_CONFIG_PATH: &str = "./chroma_config.yaml";
const ENV_PREFIX: &str = "CHROMA_";

#[derive(Deserialize)]
/// # Description
/// The RootConfig for all chroma services this is a YAML file that
/// is shared between all services, and secondarily, fields can be
/// populated from environment variables. The environment variables
/// are prefixed with CHROMA_ and are uppercase. Values in the envionment
/// variables take precedence over values in the YAML file.
/// By default, it is read from the current working directory,
/// with the filename chroma_config.yaml.
pub(crate) struct RootConfig {
    // The root config object wraps the worker config object so that
    // we can share the same config file between multiple services.
    pub worker: WorkerConfig,
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
    pub(crate) fn load() -> Self {
        return Self::load_from_path(DEFAULT_CONFIG_PATH);
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
    pub(crate) fn load_from_path(path: &str) -> Self {
        // Unfortunately, figment doesn't support environment variables with underscores. So we have to map and replace them.
        // Excluding our own environment variables, which are prefixed with CHROMA_.
        let mut f = figment::Figment::from(Env::prefixed("CHROMA_").map(|k| match k {
            k if k == "num_indexing_threads" => k.into(),
            k if k == "my_ip" => k.into(),
            k => k.as_str().replace("__", ".").into(),
        }));
        if std::path::Path::new(path).exists() {
            f = figment::Figment::from(Yaml::file(path)).merge(f);
        }
        // Apply defaults - this seems to be the best way to do it.
        // https://github.com/SergioBenitez/Figment/issues/77#issuecomment-1642490298
        f = f.join(Serialized::default(
            "worker.num_indexing_threads",
            num_cpus::get(),
        ));
        let res = f.extract();
        match res {
            Ok(config) => return config,
            Err(e) => panic!("Error loading config: {}", e),
        }
    }
}

#[derive(Deserialize)]
/// # Description
/// The primary config for the worker service.
/// ## Description of parameters
/// - my_ip: The IP address of the worker service. Used for memberlist assignment. Must be provided
/// - num_indexing_threads: The number of indexing threads to use. If not provided, defaults to the number of cores on the machine.
/// - pulsar_tenant: The pulsar tenant to use. Must be provided.
/// - pulsar_namespace: The pulsar namespace to use. Must be provided.
/// - assignment_policy: The assignment policy to use. Must be provided.
/// # Notes
/// In order to set the enviroment variables, you must prefix them with CHROMA_WORKER__<FIELD_NAME>.
/// For example, to set my_ip, you would set CHROMA_WORKER__MY_IP.
/// Each submodule that needs to be configured from the config object should implement the Configurable trait and
/// have its own field in this struct for its Config struct.
pub(crate) struct WorkerConfig {
    pub(crate) my_ip: String,
    pub(crate) my_port: u16,
    pub(crate) num_indexing_threads: u32,
    pub(crate) pulsar_tenant: String,
    pub(crate) pulsar_namespace: String,
    pub(crate) pulsar_url: String,
    pub(crate) kube_namespace: String,
    pub(crate) assignment_policy: crate::assignment::config::AssignmentPolicyConfig,
    pub(crate) memberlist_provider: crate::memberlist::config::MemberlistProviderConfig,
    pub(crate) ingest: crate::ingest::config::IngestConfig,
    pub(crate) sysdb: crate::sysdb::config::SysDbConfig,
    pub(crate) segment_manager: crate::segment::config::SegmentManagerConfig,
    pub(crate) storage: crate::storage::config::StorageConfig,
    pub(crate) log: crate::log::config::LogConfig,
}

/// # Description
/// A trait for configuring a struct from a config object.
/// # Notes
/// This trait is used to configure structs from the config object.
/// Components that need to be configured from the config object should implement this trait.
#[async_trait]
pub(crate) trait Configurable {
    async fn try_from_config(worker_config: &WorkerConfig) -> Result<Self, Box<dyn ChromaError>>
    where
        Self: Sized;
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::Jail;

    #[test]
    fn test_config_from_default_path() {
        Jail::expect_with(|jail| {
            let _ = jail.create_file(
                "chroma_config.yaml",
                r#"
                worker:
                    my_ip: "192.0.0.1"
                    my_port: 50051
                    num_indexing_threads: 4
                    pulsar_tenant: "public"
                    pulsar_namespace: "default"
                    pulsar_url: "pulsar://localhost:6650"
                    kube_namespace: "chroma"
                    assignment_policy:
                        RendezvousHashing:
                            hasher: Murmur3
                    memberlist_provider:
                        CustomResource:
                            memberlist_name: "worker-memberlist"
                            queue_size: 100
                    ingest:
                        queue_size: 100
                    sysdb:
                        Grpc:
                            host: "localhost"
                            port: 50051
                    segment_manager:
                        storage_path: "/tmp"
                    storage:
                        S3:
                            bucket: "chroma"
                    log:
                        Grpc:
                            host: "localhost"
                            port: 50052
                "#,
            );
            let config = RootConfig::load();
            assert_eq!(config.worker.my_ip, "192.0.0.1");
            assert_eq!(config.worker.num_indexing_threads, 4);
            assert_eq!(config.worker.pulsar_tenant, "public");
            assert_eq!(config.worker.pulsar_namespace, "default");
            assert_eq!(config.worker.kube_namespace, "chroma");
            Ok(())
        });
    }

    #[test]
    fn test_config_from_specific_path() {
        Jail::expect_with(|jail| {
            let _ = jail.create_file(
                "random_path.yaml",
                r#"
                worker:
                    my_ip: "192.0.0.1"
                    my_port: 50051
                    num_indexing_threads: 4
                    pulsar_tenant: "public"
                    pulsar_namespace: "default"
                    pulsar_url: "pulsar://localhost:6650"
                    kube_namespace: "chroma"
                    assignment_policy:
                        RendezvousHashing:
                            hasher: Murmur3
                    memberlist_provider:
                        CustomResource:
                            memberlist_name: "worker-memberlist"
                            queue_size: 100
                    ingest:
                        queue_size: 100
                    sysdb:
                        Grpc:
                            host: "localhost"
                            port: 50051
                    segment_manager:
                        storage_path: "/tmp"
                    storage:
                        S3:
                            bucket: "chroma"
                    log:
                        Grpc:
                            host: "localhost"
                            port: 50052

                "#,
            );
            let config = RootConfig::load_from_path("random_path.yaml");
            assert_eq!(config.worker.my_ip, "192.0.0.1");
            assert_eq!(config.worker.num_indexing_threads, 4);
            assert_eq!(config.worker.pulsar_tenant, "public");
            assert_eq!(config.worker.pulsar_namespace, "default");
            assert_eq!(config.worker.kube_namespace, "chroma");
            Ok(())
        });
    }

    #[test]
    #[should_panic]
    fn test_config_missing_required_field() {
        Jail::expect_with(|jail| {
            let _ = jail.create_file(
                "chroma_config.yaml",
                r#"
                worker:
                    num_indexing_threads: 4
                "#,
            );
            let _ = RootConfig::load();
            Ok(())
        });
    }

    #[test]
    fn test_missing_default_field() {
        Jail::expect_with(|jail| {
            let _ = jail.create_file(
                "chroma_config.yaml",
                r#"
                worker:
                    my_ip: "192.0.0.1"
                    my_port: 50051
                    pulsar_tenant: "public"
                    pulsar_namespace: "default"
                    kube_namespace: "chroma"
                    pulsar_url: "pulsar://localhost:6650"
                    assignment_policy:
                        RendezvousHashing:
                            hasher: Murmur3
                    memberlist_provider:
                        CustomResource:
                            memberlist_name: "worker-memberlist"
                            queue_size: 100
                    ingest:
                        queue_size: 100
                    sysdb:
                        Grpc:
                            host: "localhost"
                            port: 50051
                    segment_manager:
                        storage_path: "/tmp"
                    storage:
                        S3:
                            bucket: "chroma"
                    log:
                        Grpc:
                            host: "localhost"
                            port: 50052
                "#,
            );
            let config = RootConfig::load();
            assert_eq!(config.worker.my_ip, "192.0.0.1");
            assert_eq!(config.worker.num_indexing_threads, num_cpus::get() as u32);
            Ok(())
        });
    }

    #[test]
    fn test_config_with_env_override() {
        Jail::expect_with(|jail| {
            let _ = jail.set_env("CHROMA_WORKER__MY_IP", "192.0.0.1");
            let _ = jail.set_env("CHROMA_WORKER__MY_PORT", 50051);
            let _ = jail.set_env("CHROMA_WORKER__PULSAR_TENANT", "A");
            let _ = jail.set_env("CHROMA_WORKER__PULSAR_NAMESPACE", "B");
            let _ = jail.set_env("CHROMA_WORKER__KUBE_NAMESPACE", "C");
            let _ = jail.set_env("CHROMA_WORKER__PULSAR_URL", "pulsar://localhost:6650");
            let _ = jail.create_file(
                "chroma_config.yaml",
                r#"
                worker:
                    assignment_policy:
                        RendezvousHashing:
                            hasher: Murmur3
                    memberlist_provider:
                        CustomResource:
                            memberlist_name: "worker-memberlist"
                            queue_size: 100
                    ingest:
                        queue_size: 100
                    sysdb:
                        Grpc:
                            host: "localhost"
                            port: 50051
                    segment_manager:
                        storage_path: "/tmp"
                    storage:
                        S3:
                            bucket: "chroma"
                    log:
                        Grpc:
                            host: "localhost"
                            port: 50052
                "#,
            );
            let config = RootConfig::load();
            assert_eq!(config.worker.my_ip, "192.0.0.1");
            assert_eq!(config.worker.my_port, 50051);
            assert_eq!(config.worker.num_indexing_threads, num_cpus::get() as u32);
            assert_eq!(config.worker.pulsar_tenant, "A");
            assert_eq!(config.worker.pulsar_namespace, "B");
            assert_eq!(config.worker.kube_namespace, "C");
            Ok(())
        });
    }

    #[test]
    fn test_default_config_path() {
        // Sanity check that root config loads from default path correctly
        let config = RootConfig::load();
    }
}
