use chroma_blockstore::{
    arrow::config::{ArrowBlockfileProviderConfig, BlockManagerConfig, RootManagerConfig},
    config::BlockfileProviderConfig,
    provider::BlockfileProvider,
};
use chroma_config::Configurable;
use chroma_index::{
    config::{
        HnswGarbageCollectionConfig, HnswGarbageCollectionPolicyConfig, HnswProviderConfig,
        PlGarbageCollectionConfig, PlGarbageCollectionPolicyConfig,
    },
    hnsw_provider::{self, HnswIndexProvider},
};
use chroma_segment::spann_provider::{self, SpannProvider};
use chroma_sysdb::{self, SysDb};

#[tokio::main]
async fn main() {
    let registry = Registry::new();

    // Sysdb
    let sysdb_client = SysDb::try_from_config(&chroma_sysdb::SysDbConfig::default(), &registry)
        .await
        .expect("Failed to create sysdb client");

    let collection_uuid = uuid::uuid!("ca3dc43f-24d7-4e22-a2bf-0cc052611519");
    let collection_with_segment = sysdb_client
        .get_collection_with_segments(collection_uuid)
        .await
        .unwrap();

    // Storage
    let storage_config = StorageConfig::AdmissionControlledS3(AdmissionControlledS3StorageConfig {
        s3_config: S3StorageConfig {
            bucket: "chroma-serverless-staging".to_string(),
            credentials: S3CredentialsConfig::AWS,
            connect_timeout_ms: 5000,
            request_timeout_ms: 60000,
            upload_part_size_bytes: 8 * 1024 * 1024,
            download_part_size_bytes: 8 * 1024 * 1024,
        },
        rate_limiting_policy: RateLimitingConfig::CountBasedPolicy(CountBasedPolicyConfig {
            max_concurrent_requests: 128,
            bandwidth_allocation: vec![0.7, 0.3],
        }),
    });
    let storage = AdmissionControlledS3Storage::try_from_config(&storage_config, &registry)
        .await
        .expect("Failed to create storage client");

    let hnsw_provider = HnswIndexProvider::try_from_config(
        (HnswProviderConfig {
            hnsw_temporary_path: "/tmp_test".to_string(),
            hnsw_cache_config: chroma_cache::CacheConfig::Nop,
            permitted_parallelism: 128,
        },),
        &registry,
    )
    .await
    .expect("Failed to create HNSW provider");

    let blockfile_provider = BlockfileProvider::try_from_config(
        (BlockfileProviderConfig::Arrow(Box::new(ArrowBlockfileProviderConfig {
            block_manager_config: BlockManagerConfig {
                max_block_size_bytes: 8 * 1024 * 1024,
                block_cache_config: chroma_cache::CacheConfig::Nop,
            },
            root_manager_config: RootManagerConfig {
                root_cache_config: chroma_cache::CacheConfig::Nop,
            },
        }))),
        &registry,
    )
    .await
    .expect("Failed to create blockfile provider");

    let spann_provider = SpannProvider::try_from_config(
        &(
            hnsw_provider.clone(),
            blockfile_provider.clone(),
            spann_provider::SpannProviderConfig {
                pl_garbage_collection: PlGarbageCollectionConfig {
                    enabled: false,
                    ..Default::default()
                },
                hnsw_garbage_collection: HnswGarbageCollectionConfig {
                    enabled: false,
                    ..Default::default()
                },
            },
        ),
        &registry,
    )
    .await
    .expect("Failed to create Spann provider");

    // Read
    spann_provider
        .read(
            &collection_with_segment.collection,
            &collection_with_segment.vector_segment,
            1536,
        )
        .await
        .expect("Failed to read segment");
}
