use chroma_blockstore::config::BlockfileProviderConfig;
use chroma_index::config::{HnswGarbageCollectionPolicyConfig, PlGarbageCollectionPolicyConfig};
use figment::Jail;
use serial_test::serial;
use worker::config::RootConfig;

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
                jemalloc_pprof_server_port: 6060
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
                        disk:
                            capacity: 1073741824
                            eviction: lru

            compaction_service:
                service_name: "compaction-service"
                otel_endpoint: "http://jaeger:4317"
                my_member_id: "compaction-service-0"
                my_port: 50051
                jemalloc_pprof_server_port: 6060
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
                compactor:
                    compaction_manager_queue_size: 1000
                    max_concurrent_jobs: 100
                    compaction_interval_sec: 60
                    min_compaction_size: 10
                    max_compaction_size: 10000
                    max_partition_size: 5000
                    disabled_collections: []
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
                spann_provider:
                    pl_garbage_collection:
                        enabled: true
                        policy:
                            random_sample:
                                sample_size: 0.1
                    hnsw_garbage_collection:
                        enabled: true
                        policy:
                            delete_percentage:
                                threshold: 10.0
            "#,
        );
        let config = RootConfig::load_from_path("random_path.yaml");
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
        match config.compaction_service.blockfile_provider {
            BlockfileProviderConfig::Arrow(arrow_config) => {
                assert_eq!(
                    arrow_config
                        .block_manager_config
                        .num_concurrent_block_flushes,
                    40
                );
            }
            _ => panic!("Expected Arrow blockfile provider config"),
        }
        assert!(
            config
                .compaction_service
                .spann_provider
                .pl_garbage_collection
                .enabled
        );
        match config
            .compaction_service
            .spann_provider
            .pl_garbage_collection
            .policy
        {
            PlGarbageCollectionPolicyConfig::RandomSample(config) => {
                assert_eq!(config.sample_size, 0.1);
            }
        }
        assert!(
            config
                .compaction_service
                .spann_provider
                .hnsw_garbage_collection
                .enabled
        );
        match config
            .compaction_service
            .spann_provider
            .hnsw_garbage_collection
            .policy
        {
            HnswGarbageCollectionPolicyConfig::DeletePercentage(policy) => {
                assert_eq!(policy.threshold, 10.0);
            }
            _ => panic!("Expected DeletePercentage policy"),
        }
        Ok(())
    });
}
