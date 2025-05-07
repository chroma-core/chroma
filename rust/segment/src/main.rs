use chroma_blockstore::{
    arrow::config::{ArrowBlockfileProviderConfig, BlockManagerConfig, RootManagerConfig},
    config::BlockfileProviderConfig,
    provider::BlockfileProvider,
};
use chroma_config::{registry::Registry, Configurable};
use chroma_index::{
    config::{HnswGarbageCollectionConfig, HnswProviderConfig, PlGarbageCollectionConfig},
    hnsw_provider::HnswIndexProvider,
};
use chroma_segment::spann_provider::SpannProvider;
use chroma_storage::{
    admissioncontrolleds3::AdmissionControlledS3Storage,
    config::{
        AdmissionControlledS3StorageConfig, CountBasedPolicyConfig, RateLimitingConfig,
        S3CredentialsConfig, S3StorageConfig, StorageConfig,
    },
};
use chroma_sysdb::{self, SysDb};
use chroma_types::CollectionUuid;

#[tokio::main]
async fn main() {
    let registry = Registry::new();

    // Sysdb
    let mut sysdb_client = SysDb::try_from_config(&chroma_sysdb::SysDbConfig::default(), &registry)
        .await
        .expect("Failed to create sysdb client");

    let collection_uuid = uuid::uuid!("ce640ae9-5f25-4cde-b0db-c1ea362b1fc9");
    let collection_with_segment = sysdb_client
        .get_collection_with_segments(CollectionUuid(collection_uuid))
        .await
        .unwrap();

    println!(
        "Collection: {:?}, Segment: {:?}",
        collection_with_segment.collection, collection_with_segment.vector_segment
    );

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
    let storage = chroma_storage::Storage::AdmissionControlledS3(storage);

    let hnsw_provider = HnswIndexProvider::try_from_config(
        &(
            HnswProviderConfig {
                hnsw_temporary_path: "/tmp_test".to_string(),
                hnsw_cache_config: chroma_cache::CacheConfig::Nop,
                permitted_parallelism: 128,
            },
            storage.clone(),
        ),
        &registry,
    )
    .await
    .expect("Failed to create HNSW provider");

    let blockfile_provider = BlockfileProvider::try_from_config(
        &(
            BlockfileProviderConfig::Arrow(Box::new(ArrowBlockfileProviderConfig {
                block_manager_config: BlockManagerConfig {
                    max_block_size_bytes: 8 * 1024 * 1024,
                    block_cache_config: chroma_cache::CacheConfig::Nop,
                },
                root_manager_config: RootManagerConfig {
                    root_cache_config: chroma_cache::CacheConfig::Nop,
                },
            })),
            storage.clone(),
        ),
        &registry,
    )
    .await
    .expect("Failed to create blockfile provider");

    let spann_provider = SpannProvider::try_from_config(
        &(
            hnsw_provider.clone(),
            blockfile_provider.clone(),
            chroma_index::config::SpannProviderConfig {
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
    let reader = spann_provider
        .read(
            &collection_with_segment.collection,
            &collection_with_segment.vector_segment,
            1536,
        )
        .await
        .expect("Failed to read segment");

    // TODO: use real data for a realistic query endpoint
    let dimension = collection_with_segment
        .collection
        .dimension
        .expect("Dimension should exist");
    let mut random_vector = vec![0.0; dimension as usize];
    for i in 0..dimension {
        random_vector[i as usize] = rand::random::<f32>();
    }

    // Search
    println!("Querying with random vector");
    let center_result = reader
        .rng_query(&random_vector)
        .await
        .expect("Query to succeed");

    let mut fetch_pl_futures = Vec::new();
    let (ids, distances, _) = center_result;

    for id in ids {
        let fetch_pl_future = reader.fetch_posting_list(id as u32);
        fetch_pl_futures.push(fetch_pl_future);
    }

    // Execute all futures in parallel
    println!("Fetching posting lists in parallel");
    let time_start = std::time::Instant::now();
    let fetch_pl_results = futures::future::join_all(fetch_pl_futures).await;
    println!(
        "Time taken to fetch posting lists: {:?}",
        time_start.elapsed()
    );
    // ensure there are no errors
    for result in fetch_pl_results {
        if let Err(err) = result {
            println!("Error fetching posting list: {:?}", err);
        }
    }
}
