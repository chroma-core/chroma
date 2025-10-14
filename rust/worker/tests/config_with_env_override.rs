use figment::Jail;
use serial_test::serial;
use uuid::Uuid;
use worker::config::RootConfig;

#[test]
#[serial]
fn test_config_with_env_override() {
    Jail::expect_with(|jail| {
        jail.set_env("CHROMA_QUERY_SERVICE__MY_MEMBER_ID", "query-service-0");
        jail.set_env("CHROMA_QUERY_SERVICE__MY_PORT", 50051);
        jail.set_env("CHROMA_QUERY_SERVICE__JEMALLOC_PPROF_SERVER_PORT", 6060);
        jail.set_env(
            "CHROMA_COMPACTION_SERVICE__MY_MEMBER_ID",
            "compaction-service-0",
        );
        jail.set_env("CHROMA_COMPACTION_SERVICE__MY_PORT", 50051);
        jail.set_env(
            "CHROMA_COMPACTION_SERVICE__JEMALLOC_PPROF_SERVER_PORT",
            6060,
        );
        jail.set_env("CHROMA_COMPACTION_SERVICE__STORAGE__S3__BUCKET", "buckets!");
        jail.set_env("CHROMA_COMPACTION_SERVICE__STORAGE__S3__CREDENTIALS", "AWS");
        jail.set_env(
            "CHROMA_COMPACTION_SERVICE__STORAGE__S3__upload_part_size_bytes",
            format!("{}", 1024 * 1024 * 8),
        );
        jail.set_env(
            "CHROMA_COMPACTION_SERVICE__STORAGE__S3__download_part_size_bytes",
            format!("{}", 1024 * 1024 * 8),
        );
        jail.set_env(
            "CHROMA_COMPACTION_SERVICE__STORAGE__S3__CONNECT_TIMEOUT_MS",
            5000,
        );
        jail.set_env(
            "CHROMA_COMPACTION_SERVICE__STORAGE__S3__REQUEST_TIMEOUT_MS",
            1000,
        );
        jail.set_env(
            "CHROMA_COMPACTION_SERVICE__COMPACTOR__DISABLED_COLLECTIONS",
            "[\"74b3240e-a2b0-43d7-8adb-f55a394964a1\",\"496db4aa-fbe1-498a-b60b-81ec0fe59792\"]",
        );
        let _ = jail.create_file(
            "chroma_config.yaml",
            r#"
            query_service:
                service_name: "query-service"
                otel_endpoint: "http://jaeger:4317"
                assignment_policy:
                    rendezvous_hashing:
                        hasher: Murmur3
                memberlist_provider:
                    custom_resource:
                        kube_namespace: "chroma"
                        memberlist_name: "query-service-memberlist"
                        queue_size: 100
                sysdb:
                    grpc:
                        host: "localhost"
                        port: 50051
                        connect_timeout_ms: 5000
                        request_timeout_ms: 1000
                storage:
                    admission_controlled_s3:
                        s3_config:
                            bucket: "chroma"
                            credentials: Minio
                            connect_timeout_ms: 5000
                            request_timeout_ms: 1000
                            upload_part_size_bytes: 8388608
                            download_part_size_bytes: 8388608
                        rate_limiting_policy:
                            count_based_policy:
                                max_concurrent_requests: 15
                log:
                    grpc:
                        host: "localhost"
                        port: 50051
                        connect_timeout_ms: 5000
                        request_timeout_ms: 1000
                dispatcher:
                    num_worker_threads: 4
                    dispatcher_queue_size: 100
                    worker_queue_size: 100
                    task_queue_limit: 100
                    active_io_tasks: 1000
                blockfile_provider:
                    arrow:
                        block_manager_config:
                            max_block_size_bytes: 16384
                            block_cache_config:
                                memory:
                                    capacity: 1000
                        sparse_index_manager_config:
                            sparse_index_cache_config:
                                memory:
                                    capacity: 1000
                hnsw_provider:
                    hnsw_temporary_path: "~/tmp"
                    hnsw_cache_config:
                        memory:
                            capacity: 1073741824

            compaction_service:
                service_name: "compaction-service"
                otel_endpoint: "http://jaeger:4317"
                assignment_policy:
                    rendezvous_hashing:
                        hasher: Murmur3
                memberlist_provider:
                    custom_resource:
                        kube_namespace: "chroma"
                        memberlist_name: "compaction-service-memberlist"
                        queue_size: 100
                sysdb:
                    grpc:
                        host: "localhost"
                        port: 50051
                        connect_timeout_ms: 5000
                        request_timeout_ms: 1000
                log:
                    grpc:
                        host: "localhost"
                        port: 50051
                        connect_timeout_ms: 5000
                        request_timeout_ms: 1000
                dispatcher:
                    num_worker_threads: 4
                    dispatcher_queue_size: 100
                    worker_queue_size: 100
                    task_queue_limit: 100
                    active_io_tasks: 1000
                compactor:
                    compaction_manager_queue_size: 1000
                    max_concurrent_jobs: 100
                    compaction_interval_sec: 60
                    min_compaction_size: 10
                    max_compaction_size: 10000
                    max_partition_size: 5000
                    disabled_collections: ["c92e4d75-eb25-4295-82d8-7c53dbd33258"]
                blockfile_provider:
                    arrow:
                        block_manager_config:
                            max_block_size_bytes: 16384
                            block_cache_config:
                                memory:
                                    capacity: 1000
                        sparse_index_manager_config:
                            sparse_index_cache_config:
                                memory:
                                    capacity: 1000
                hnsw_provider:
                    hnsw_temporary_path: "~/tmp"
                    hnsw_cache_config:
                        disk:
                            capacity: 1073741824
                            eviction: lru
            "#,
        );
        let config = RootConfig::load();
        assert_eq!(config.query_service.my_member_id, "query-service-0");
        assert_eq!(config.query_service.my_port, 50051);
        assert_eq!(config.query_service.jemalloc_pprof_server_port, Some(6060));
        assert_eq!(
            config.compaction_service.my_member_id,
            "compaction-service-0"
        );
        assert_eq!(config.compaction_service.my_port, 50051);
        assert_eq!(
            config.compaction_service.jemalloc_pprof_server_port,
            Some(6060)
        );
        match &config.compaction_service.storage {
            chroma_storage::config::StorageConfig::S3(s) => {
                assert_eq!(s.bucket, "buckets!");
                assert_eq!(
                    s.credentials,
                    chroma_storage::config::S3CredentialsConfig::AWS
                );
                assert_eq!(s.connect_timeout_ms, 5000);
                assert_eq!(s.request_timeout_ms, 1000);
                assert_eq!(s.upload_part_size_bytes, 1024 * 1024 * 8);
                assert_eq!(s.download_part_size_bytes, 1024 * 1024 * 8);
            }
            _ => panic!("Invalid storage config"),
        }
        assert_eq!(
            config
                .compaction_service
                .compactor
                .disabled_collections
                .len(),
            2
        );
        assert_eq!(
            Uuid::parse_str(&config.compaction_service.compactor.disabled_collections[0]).unwrap(),
            Uuid::parse_str("74b3240e-a2b0-43d7-8adb-f55a394964a1").unwrap()
        );
        assert_eq!(
            Uuid::parse_str(&config.compaction_service.compactor.disabled_collections[1]).unwrap(),
            Uuid::parse_str("496db4aa-fbe1-498a-b60b-81ec0fe59792").unwrap()
        );
        Ok(())
    });
}
