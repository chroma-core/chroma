use crate::errors::ChromaError;
use async_trait::async_trait;
use figment::providers::{Env, Format, Serialized, Yaml};
use serde::Deserialize;

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
    pub query_service: QueryServiceConfig,
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
            Ok(config) => return config,
            Err(e) => panic!("Error loading config: {}", e),
        }
    }
}

#[derive(Deserialize)]
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
pub(crate) struct QueryServiceConfig {
    pub(crate) service_name: String,
    pub(crate) otel_endpoint: String,
    pub(crate) my_member_id: String,
    pub(crate) my_port: u16,
    pub(crate) assignment_policy: crate::assignment::config::AssignmentPolicyConfig,
    pub(crate) memberlist_provider: crate::memberlist::config::MemberlistProviderConfig,
    pub(crate) sysdb: crate::sysdb::config::SysDbConfig,
    pub(crate) storage: crate::storage::config::StorageConfig,
    pub(crate) log: crate::log::config::LogConfig,
    pub(crate) dispatcher: crate::execution::config::DispatcherConfig,
    pub(crate) blockfile_provider: crate::blockstore::config::BlockfileProviderConfig,
    pub(crate) hnsw_provider: crate::index::config::HnswProviderConfig,
}

#[derive(Deserialize)]
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
pub(crate) struct CompactionServiceConfig {
    pub(crate) service_name: String,
    pub(crate) otel_endpoint: String,
    pub(crate) my_member_id: String,
    pub(crate) my_port: u16,
    pub(crate) assignment_policy: crate::assignment::config::AssignmentPolicyConfig,
    pub(crate) memberlist_provider: crate::memberlist::config::MemberlistProviderConfig,
    pub(crate) sysdb: crate::sysdb::config::SysDbConfig,
    pub(crate) storage: crate::storage::config::StorageConfig,
    pub(crate) log: crate::log::config::LogConfig,
    pub(crate) dispatcher: crate::execution::config::DispatcherConfig,
    pub(crate) compactor: crate::compactor::config::CompactorConfig,
    pub(crate) blockfile_provider: crate::blockstore::config::BlockfileProviderConfig,
    pub(crate) hnsw_provider: crate::index::config::HnswProviderConfig,
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::Jail;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_config_from_default_path() {
        Jail::expect_with(|jail| {
            let _ = jail.create_file(
                "chroma_config.yaml",
                r#"
                query_service:
                    service_name: "query-service"
                    otel_endpoint: "http://jaeger:4317"
                    my_member_id: "query-service-0"
                    my_port: 50051
                    assignment_policy:
                        RendezvousHashing:
                            hasher: Murmur3
                    memberlist_provider:
                        CustomResource:
                            kube_namespace: "chroma"
                            memberlist_name: "query-service-memberlist"
                            queue_size: 100
                    sysdb:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    storage:
                        S3:
                            bucket: "chroma"
                            credentials: Minio
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                            upload_part_size_bytes: 8388608
                    log:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    dispatcher:
                        num_worker_threads: 4
                        dispatcher_queue_size: 100
                        worker_queue_size: 100
                    blockfile_provider:
                        Arrow:
                            block_manager_config:
                                max_block_size_bytes: 16384
                                block_cache_config:
                                    lru:
                                        capacity: 1000
                            sparse_index_manager_config:
                                sparse_index_cache_config:
                                    lru:
                                        capacity: 1000
                    hnsw_provider:
                        hnsw_temporary_path: "~/tmp"
                        hnsw_cache_config:
                            lru:
                                capacity: 1000

                compaction_service:
                    service_name: "compaction-service"
                    otel_endpoint: "http://jaeger:4317"
                    my_member_id: "compaction-service-0"
                    my_port: 50051
                    assignment_policy:
                        RendezvousHashing:
                            hasher: Murmur3
                    memberlist_provider:
                        CustomResource:
                            kube_namespace: "chroma"
                            memberlist_name: "compaction-service-memberlist"
                            queue_size: 100
                    sysdb:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    storage:
                        S3:
                            bucket: "chroma"
                            credentials: Minio
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                            upload_part_size_bytes: 8388608
                    log:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    dispatcher:
                        num_worker_threads: 4
                        dispatcher_queue_size: 100
                        worker_queue_size: 100
                    compactor:
                        compaction_manager_queue_size: 1000
                        max_concurrent_jobs: 100
                        compaction_interval_sec: 60
                        min_compaction_size: 10
                    blockfile_provider:
                        Arrow:
                            block_manager_config:
                                max_block_size_bytes: 16384
                                block_cache_config:
                                    lru:
                                        capacity: 1000
                            sparse_index_manager_config:
                                sparse_index_cache_config:
                                    lru:
                                        capacity: 1000
                    hnsw_provider:
                        hnsw_temporary_path: "~/tmp"
                        hnsw_cache_config:
                            lru:
                                capacity: 1000
                "#,
            );
            let config = RootConfig::load();
            assert_eq!(config.query_service.my_member_id, "query-service-0");
            assert_eq!(config.query_service.my_port, 50051);

            assert_eq!(
                config.compaction_service.my_member_id,
                "compaction-service-0"
            );
            assert_eq!(config.compaction_service.my_port, 50051);
            Ok(())
        });
    }

    #[test]
    #[serial]
    fn test_config_from_specific_path() {
        Jail::expect_with(|jail| {
            let _ = jail.create_file(
                "random_path.yaml",
                r#"
                query_service:
                    service_name: "query-service"
                    otel_endpoint: "http://jaeger:4317"
                    my_member_id: "query-service-0"
                    my_port: 50051
                    assignment_policy:
                        RendezvousHashing:
                            hasher: Murmur3
                    memberlist_provider:
                        CustomResource:
                            kube_namespace: "chroma"
                            memberlist_name: "query-service-memberlist"
                            queue_size: 100
                    sysdb:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    storage:
                        S3:
                            bucket: "chroma"
                            credentials: Minio
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                            upload_part_size_bytes: 8388608
                    log:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    dispatcher:
                        num_worker_threads: 4
                        dispatcher_queue_size: 100
                        worker_queue_size: 100
                    blockfile_provider:
                        Arrow:
                            block_manager_config:
                                max_block_size_bytes: 16384
                                block_cache_config:
                                    lru:
                                        capacity: 1000
                            sparse_index_manager_config:
                                sparse_index_cache_config:
                                    lru:
                                        capacity: 1000
                    hnsw_provider:
                        hnsw_temporary_path: "~/tmp"
                        hnsw_cache_config:
                            lru:
                                capacity: 1000

                compaction_service:
                    service_name: "compaction-service"
                    otel_endpoint: "http://jaeger:4317"
                    my_member_id: "compaction-service-0"
                    my_port: 50051
                    assignment_policy:
                        RendezvousHashing:
                            hasher: Murmur3
                    memberlist_provider:
                        CustomResource:
                            kube_namespace: "chroma"
                            memberlist_name: "compaction-service-memberlist"
                            queue_size: 100
                    sysdb:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    storage:
                        S3:
                            bucket: "chroma"
                            credentials: Minio
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                            upload_part_size_bytes: 8388608
                    log:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    dispatcher:
                        num_worker_threads: 4
                        dispatcher_queue_size: 100
                        worker_queue_size: 100
                    compactor:
                        compaction_manager_queue_size: 1000
                        max_concurrent_jobs: 100
                        compaction_interval_sec: 60
                        min_compaction_size: 10
                    blockfile_provider:
                        Arrow:
                            block_manager_config:
                                max_block_size_bytes: 16384
                                block_cache_config:
                                    lru:
                                        capacity: 1000
                            sparse_index_manager_config:
                                sparse_index_cache_config:
                                    lru:
                                        capacity: 1000
                    hnsw_provider:
                        hnsw_temporary_path: "~/tmp"
                        hnsw_cache_config:
                            lru:
                                capacity: 1000
                "#,
            );
            let config = RootConfig::load_from_path("random_path.yaml");
            assert_eq!(config.query_service.my_member_id, "query-service-0");
            assert_eq!(config.query_service.my_port, 50051);

            assert_eq!(
                config.compaction_service.my_member_id,
                "compaction-service-0"
            );
            assert_eq!(config.compaction_service.my_port, 50051);
            Ok(())
        });
    }

    #[test]
    #[should_panic]
    #[serial]
    fn test_config_missing_required_field() {
        Jail::expect_with(|jail| {
            let _ = jail.create_file(
                "chroma_config.yaml",
                r#"
                query_service:
                    assignment_policy:
                        RendezvousHashing:
                            hasher: Murmur3
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
                query_service:
                    service_name: "query-service"
                    otel_endpoint: "http://jaeger:4317"
                    my_member_id: "query-service-0"
                    my_port: 50051
                    assignment_policy:
                        RendezvousHashing:
                            hasher: Murmur3
                    memberlist_provider:
                        CustomResource:
                            kube_namespace: "chroma"
                            memberlist_name: "query-service-memberlist"
                            queue_size: 100
                    sysdb:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    storage:
                        S3:
                            bucket: "chroma"
                            credentials: Minio
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                            upload_part_size_bytes: 8388608
                    log:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    dispatcher:
                        num_worker_threads: 4
                        dispatcher_queue_size: 100
                        worker_queue_size: 100
                    blockfile_provider:
                        Arrow:
                            block_manager_config:
                                max_block_size_bytes: 16384
                                block_cache_config:
                                    lru:
                                        capacity: 1000
                            sparse_index_manager_config:
                                sparse_index_cache_config:
                                    lru:
                                        capacity: 1000
                    hnsw_provider:
                        hnsw_temporary_path: "~/tmp"
                        hnsw_cache_config:
                            lru:
                                capacity: 1000

                compaction_service:
                    service_name: "compaction-service"
                    otel_endpoint: "http://jaeger:4317"
                    my_member_id: "compaction-service-0"
                    my_port: 50051
                    assignment_policy:
                        RendezvousHashing:
                            hasher: Murmur3
                    memberlist_provider:
                        CustomResource:
                            kube_namespace: "chroma"
                            memberlist_name: "compaction-service-memberlist"
                            queue_size: 100
                    sysdb:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    storage:
                        S3:
                            bucket: "chroma"
                            credentials: Minio
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                            upload_part_size_bytes: 8388608
                    log:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    dispatcher:
                        num_worker_threads: 4
                        dispatcher_queue_size: 100
                        worker_queue_size: 100
                    compactor:
                        compaction_manager_queue_size: 1000
                        max_concurrent_jobs: 100
                        compaction_interval_sec: 60
                        min_compaction_size: 10
                    blockfile_provider:
                        Arrow:
                            block_manager_config:
                                max_block_size_bytes: 16384
                                block_cache_config:
                                    lru:
                                        capacity: 1000
                            sparse_index_manager_config:
                                sparse_index_cache_config:
                                    lru:
                                        capacity: 1000
                    hnsw_provider:
                        hnsw_temporary_path: "~/tmp"
                        hnsw_cache_config:
                            lru:
                                capacity: 1000
                "#,
            );
            let config = RootConfig::load();
            assert_eq!(config.query_service.my_member_id, "query-service-0");
            assert_eq!(
                config.compaction_service.my_member_id,
                "compaction-service-0"
            );
            Ok(())
        });
    }

    #[test]
    #[serial]
    fn test_config_with_env_override() {
        Jail::expect_with(|jail| {
            let _ = jail.set_env("CHROMA_QUERY_SERVICE__MY_MEMBER_ID", "query-service-0");
            let _ = jail.set_env("CHROMA_QUERY_SERVICE__MY_PORT", 50051);
            let _ = jail.set_env(
                "CHROMA_COMPACTION_SERVICE__MY_MEMBER_ID",
                "compaction-service-0",
            );
            let _ = jail.set_env("CHROMA_COMPACTION_SERVICE__MY_PORT", 50051);
            let _ = jail.set_env("CHROMA_COMPACTION_SERVICE__STORAGE__S3__BUCKET", "buckets!");
            let _ = jail.set_env("CHROMA_COMPACTION_SERVICE__STORAGE__S3__CREDENTIALS", "AWS");
            let _ = jail.set_env(
                "CHROMA_COMPACTION_SERVICE__STORAGE__S3__upload_part_size_bytes",
                format!("{}", 1024 * 1024 * 8),
            );
            let _ = jail.set_env(
                "CHROMA_COMPACTION_SERVICE__STORAGE__S3__CONNECT_TIMEOUT_MS",
                5000,
            );
            let _ = jail.set_env(
                "CHROMA_COMPACTION_SERVICE__STORAGE__S3__REQUEST_TIMEOUT_MS",
                1000,
            );
            let _ = jail.create_file(
                "chroma_config.yaml",
                r#"
                query_service:
                    service_name: "query-service"
                    otel_endpoint: "http://jaeger:4317"
                    assignment_policy:
                        RendezvousHashing:
                            hasher: Murmur3
                    memberlist_provider:
                        CustomResource:
                            kube_namespace: "chroma"
                            memberlist_name: "query-service-memberlist"
                            queue_size: 100
                    sysdb:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    storage:
                        S3:
                            bucket: "chroma"
                            credentials: Minio
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                            upload_part_size_bytes: 8388608
                    log:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    dispatcher:
                        num_worker_threads: 4
                        dispatcher_queue_size: 100
                        worker_queue_size: 100
                    blockfile_provider:
                        Arrow:
                            block_manager_config:
                                max_block_size_bytes: 16384
                                block_cache_config:
                                    lru:
                                        capacity: 1000
                            sparse_index_manager_config:
                                sparse_index_cache_config:
                                    lru:
                                        capacity: 1000
                    hnsw_provider:
                        hnsw_temporary_path: "~/tmp"
                        hnsw_cache_config:
                            lru:
                                capacity: 1000

                compaction_service:
                    service_name: "compaction-service"
                    otel_endpoint: "http://jaeger:4317"
                    assignment_policy:
                        RendezvousHashing:
                            hasher: Murmur3
                    memberlist_provider:
                        CustomResource:
                            kube_namespace: "chroma"
                            memberlist_name: "compaction-service-memberlist"
                            queue_size: 100
                    sysdb:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    log:
                        Grpc:
                            host: "localhost"
                            port: 50051
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                    dispatcher:
                        num_worker_threads: 4
                        dispatcher_queue_size: 100
                        worker_queue_size: 100
                    compactor:
                        compaction_manager_queue_size: 1000
                        max_concurrent_jobs: 100
                        compaction_interval_sec: 60
                        min_compaction_size: 10
                    blockfile_provider:
                        Arrow:
                            block_manager_config:
                                max_block_size_bytes: 16384
                                block_cache_config:
                                    lru:
                                        capacity: 1000
                            sparse_index_manager_config:
                                sparse_index_cache_config:
                                    lru:
                                        capacity: 1000
                    hnsw_provider:
                        hnsw_temporary_path: "~/tmp"
                        hnsw_cache_config:
                            lru:
                                capacity: 1000
                "#,
            );
            let config = RootConfig::load();
            assert_eq!(config.query_service.my_member_id, "query-service-0");
            assert_eq!(config.query_service.my_port, 50051);
            assert_eq!(
                config.compaction_service.my_member_id,
                "compaction-service-0"
            );
            assert_eq!(config.compaction_service.my_port, 50051);
            match &config.compaction_service.storage {
                crate::storage::config::StorageConfig::S3(s) => {
                    assert_eq!(s.bucket, "buckets!");
                    assert_eq!(
                        s.credentials,
                        crate::storage::config::S3CredentialsConfig::AWS
                    );
                    assert_eq!(s.connect_timeout_ms, 5000);
                    assert_eq!(s.request_timeout_ms, 1000);
                    assert_eq!(s.upload_part_size_bytes, 1024 * 1024 * 8);
                }
                _ => panic!("Invalid storage config"),
            }
            Ok(())
        });
    }

    #[test]
    #[serial]
    fn test_default_config_path() {
        // Sanity check that root config loads from default path correctly
        let _ = RootConfig::load();
    }
}
