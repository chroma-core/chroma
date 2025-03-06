#[cfg(test)]
mod tests {
    use chroma_config::registry::Registry;
    use chroma_config::Configurable;
    use chroma_storage::config::{
        ObjectStoreBucketConfig, ObjectStoreConfig, ObjectStoreType, StorageConfig,
    };
    use chroma_storage::Storage;
    use chroma_sysdb::{GrpcSysDbConfig, SysDb, SysDbConfig};
    use chroma_system::{Dispatcher, Orchestrator, System};
    use chroma_types::CollectionUuid;
    use garbage_collector_library::garbage_collector_orchestrator::GarbageCollectorOrchestrator;
    use garbage_collector_library::helper::ChromaGrpcClients;
    use proptest::prelude::*;
    use std::str::FromStr;
    use std::time::Duration;
    use uuid::Uuid;

    // Helper function to create random embeddings
    fn create_random_embeddings(count: usize) -> (Vec<Vec<f32>>, Vec<String>) {
        let mut embeddings = Vec::with_capacity(count);
        let mut ids = Vec::with_capacity(count);

        for i in 0..count {
            // Create a 3D embedding with random values between 0 and 1
            let embedding = vec![
                rand::random::<f32>(),
                rand::random::<f32>(),
                rand::random::<f32>(),
            ];
            embeddings.push(embedding);
            ids.push(format!("id{}", i));
        }

        (embeddings, ids)
    }

    // Helper function to run garbage collection
    async fn run_gc(
        collection_id: &str,
        version_file_path: &str,
        storage: Storage,
        sysdb: SysDb,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let system = System::new();
        let dispatcher = Dispatcher::new(chroma_system::DispatcherConfig::default());
        let dispatcher_handle = system.start_component(dispatcher);

        let mut orchestrator = GarbageCollectorOrchestrator::new(
            CollectionUuid::from_str(collection_id)?,
            version_file_path.to_string(),
            0, // immediate expiry for testing
            sysdb,
            dispatcher_handle,
            storage,
        );

        let (sender, _receiver) = tokio::sync::oneshot::channel();
        orchestrator.set_result_channel(sender);
        orchestrator.run(system).await?;

        Ok(())
    }

    async fn add_embeddings_and_wait_for_version(
        clients: &mut ChromaGrpcClients,
        collection_id: &str,
        tenant_id: &str,
        embeddings: Vec<Vec<f32>>,
        ids: Vec<String>,
        expected_version: i64,
        max_attempts: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!(
            batch_size = embeddings.len(),
            expected_version,
            "Adding embeddings batch"
        );

        // Add embeddings
        clients
            .add_embeddings(collection_id, embeddings, ids)
            .await?;

        // Wait for version to increase
        for attempt in 1..=max_attempts {
            tracing::info!(
                attempt,
                max_attempts,
                expected_version,
                "Waiting for version to increase..."
            );

            tokio::time::sleep(Duration::from_secs(2)).await;

            let versions = clients
                .list_collection_versions(collection_id, tenant_id, Some(100), None, None)
                .await?;

            // Find the highest version number
            if let Some(max_version) = versions.versions.iter().map(|v| v.version).max() {
                if max_version >= expected_version {
                    tracing::info!(
                        max_version,
                        expected_version,
                        "Version increased as expected"
                    );
                    return Ok(());
                } else {
                    tracing::info!(
                        max_version,
                        expected_version,
                        "Version has not yet increased"
                    );
                }
            }
        }

        Err("Timeout waiting for version to increase".into())
    }

    async fn verify_version_ordering(
        clients: &mut ChromaGrpcClients,
        collection_id: &str,
        tenant_id: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let versions = clients
            .list_collection_versions(collection_id, tenant_id, Some(100), None, None)
            .await?;

        // Check if versions are ordered (should be in ascending order)
        let mut prev_version = 0; // Start from 0 for ascending order
        for version in versions.versions.iter() {
            if version.version < prev_version {
                tracing::error!(
                    "Versions not in ascending order. Full version list: {:?}",
                    versions
                        .versions
                        .iter()
                        .map(|v| v.version)
                        .collect::<Vec<_>>()
                );
                return Ok(false);
            }
            prev_version = version.version;
        }

        Ok(true)
    }

    async fn verify_versions_after_cutoff(
        clients: &mut ChromaGrpcClients,
        collection_id: &str,
        tenant_id: &str,
        cutoff_time: i64,
        min_versions: usize,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let versions = clients
            .list_collection_versions(collection_id, tenant_id, Some(100), None, None)
            .await?;

        // If we have fewer versions than min_versions, all versions should be kept
        if versions.versions.len() < min_versions {
            return Ok(true);
        }

        // Check that all remaining versions are either:
        // 1. After the cutoff time, or
        // 2. Among the minimum required versions
        let mut sorted_versions: Vec<_> = versions.versions.iter().collect();
        sorted_versions.sort_by_key(|v| v.created_at_secs);

        // All other versions should be after cutoff
        let remaining_versions: Vec<_> = sorted_versions.iter().skip(min_versions).collect();

        for version in remaining_versions {
            if version.created_at_secs < cutoff_time {
                return Ok(false);
            }
        }

        Ok(true)
    }

    proptest! {
        #![proptest_config(ProptestConfig{
            cases: 10,
            max_shrink_iters: 1,
            fork: false,
            ..ProptestConfig::default()
        })]
        #[test]
        fn test_k8s_integration_record_count_preserved_after_gc(
            num_records in 40..100usize,
            num_gc_runs in 1..2usize,
            num_insert_batches in 1..4usize,
        ) {
            // Initialize tracing subscriber for logging
            let _ = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::INFO)
                .with_test_writer()
                .try_init();

            let runtime = tokio::runtime::Runtime::new().unwrap();

            runtime.block_on(async {
                // Setup test environment
                let test_uuid = Uuid::new_v4();
                let tenant_id = format!("test_tenant_{}", test_uuid);
                let database_name = format!("test_db_{}", test_uuid);
                let collection_name = format!("test_collection_{}", test_uuid);

                // Initialize clients and storage
                let mut clients = ChromaGrpcClients::new().await.unwrap();

                // Create storage config and client
                let storage_config = StorageConfig::ObjectStore(ObjectStoreConfig {
                    bucket: ObjectStoreBucketConfig {
                        name: "chroma-storage".to_string(),
                        r#type: ObjectStoreType::Minio,
                    },
                    upload_part_size_bytes: 1024 * 1024,
                    download_part_size_bytes: 1024 * 1024,
                    max_concurrent_requests: 10,
                });

                let registry = Registry::new();
                let storage = Storage::try_from_config(&storage_config, &registry).await.unwrap();

                // Create collection
                let collection_id = clients.create_database_and_collection(
                    &tenant_id,
                    &database_name,
                    &collection_name,
                ).await.unwrap();

                // Create embeddings data
                let records_per_batch = num_records / num_insert_batches;
                let (all_embeddings, all_ids) = create_random_embeddings(num_records);

                // Add embeddings in batches and wait for versions
                for i in 0..num_insert_batches {
                    let start_idx = i * records_per_batch;
                    let end_idx = if i == num_insert_batches - 1 {
                        num_records // Use all remaining records for last batch
                    } else {
                        (i + 1) * records_per_batch
                    };

                    let batch_embeddings = all_embeddings[start_idx..end_idx].to_vec();
                    let batch_ids = all_ids[start_idx..end_idx].to_vec();

                    add_embeddings_and_wait_for_version(
                        &mut clients,
                        &collection_id,
                        &tenant_id,
                        batch_embeddings,
                        batch_ids,
                        i as i64 + 1, // Versions start from 1
                        10,
                    ).await.unwrap();
                }

                // Get initial record count
                let initial_records = clients.get_records(
                    &collection_id,
                    None,
                    true,
                    false,
                    false,
                ).await.unwrap();
                println!("Initial records: {:?}", initial_records);

                // Run GC multiple times
                let sysdb_config = SysDbConfig::Grpc(GrpcSysDbConfig {
                    host: "localhost".to_string(),
                    port: 50051,
                    connect_timeout_ms: 5000,
                    request_timeout_ms: 10000,
                    num_channels: 1,
                });

                let mut sysdb = SysDb::try_from_config(&sysdb_config, &registry).await.unwrap();

                // Get collection info for GC
                let collections_to_gc = sysdb.get_collections_to_gc().await.unwrap();
                let collection_info = collections_to_gc.iter()
                    .find(|c| c.id.0.to_string() == collection_id)
                    .expect("Collection should be available for GC");

                println!("Collection Id: {:?}", collection_info.id);
                println!("Latest version: {:?}", collection_info.latest_version);
                for _ in 0..num_gc_runs {
                    tracing::info!("Running GC.. and waiting for it to complete");
                    run_gc(
                        &collection_id,
                        &collection_info.version_file_path,
                        storage.clone(),
                        sysdb.clone(),
                    ).await.unwrap();

                    // Give some time for GC to complete
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }

                // Get final record count
                let final_records = clients.get_records(
                    &collection_id,
                    None,
                    true,
                    false,
                    false,
                ).await.unwrap();
                println!("Final records: {:?}", final_records);
                // Verify record count hasn't changed
                prop_assert_eq!(
                    initial_records.ids.len(),
                    final_records.ids.len(),
                    "Record count changed after GC"
                );

                // Verify all IDs are still present
                for id in initial_records.ids.iter() {
                    prop_assert!(
                        final_records.ids.contains(id),
                        "ID {} missing after GC",
                        id
                    );
                }

                // Verify embeddings are preserved
                if let (Some(initial_embeddings), Some(final_embeddings)) =
                    (initial_records.embeddings, final_records.embeddings) {
                    prop_assert_eq!(
                        initial_embeddings.len(),
                        final_embeddings.len(),
                        "Embedding count changed after GC"
                    );

                    // Verify each embedding is preserved
                    for (i, initial_embedding) in initial_embeddings.iter().enumerate() {
                        let final_embedding = &final_embeddings[i];
                        prop_assert_eq!(
                            initial_embedding,
                            final_embedding,
                            "Embedding {} changed after GC",
                            i
                        );
                    }
                }

                Ok(())
            })?;
        }
    }

    proptest! {
        #[test]
        fn test_k8s_integration_gc_version_properties(
            num_records in 50..100usize,
            num_batches in 2..5usize,
            cutoff_hours in 0..24i64,
        ) {
            // Initialize tracing subscriber for logging
            let _ = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::INFO)
                .with_test_writer()
                .try_init();

            let runtime = tokio::runtime::Runtime::new().unwrap();

            runtime.block_on(async {
                // Setup test environment
                let test_uuid = Uuid::new_v4();
                let tenant_id = format!("test_tenant_{}", test_uuid);
                let database_name = format!("test_db_{}", test_uuid);
                let collection_name = format!("test_collection_{}", test_uuid);

                // Initialize clients and storage
                let mut clients = ChromaGrpcClients::new().await.unwrap();

                // Create storage config and client
                let storage_config = StorageConfig::ObjectStore(ObjectStoreConfig {
                    bucket: ObjectStoreBucketConfig {
                        name: "chroma-storage".to_string(),
                        r#type: ObjectStoreType::Minio,
                    },
                    upload_part_size_bytes: 1024 * 1024,
                    download_part_size_bytes: 1024 * 1024,
                    max_concurrent_requests: 10,
                });

                let registry = Registry::new();
                let storage = Storage::try_from_config(&storage_config, &registry).await.unwrap();

                // Create collection
                let collection_id = clients.create_database_and_collection(
                    &tenant_id,
                    &database_name,
                    &collection_name,
                ).await.unwrap();

                tracing::info!("Created collection with id: {}", collection_id);
                // Create and add embeddings in multiple batches with time delays
                let records_per_batch = num_records / num_batches;
                let (all_embeddings, all_ids) = create_random_embeddings(num_records);

                for (current_version, i) in (0..num_batches).enumerate() {
                    let start_idx = i * records_per_batch;
                    let end_idx = if i == num_batches - 1 {
                        num_records
                    } else {
                        (i + 1) * records_per_batch
                    };

                    let batch_embeddings = all_embeddings[start_idx..end_idx].to_vec();
                    let batch_ids = all_ids[start_idx..end_idx].to_vec();

                    // Add embeddings and wait for version to increase
                    add_embeddings_and_wait_for_version(
                        &mut clients,
                        &collection_id,
                        &tenant_id,
                        batch_embeddings,
                        batch_ids,
                        (current_version + 1) as i64,
                        10,
                    ).await.unwrap();

                    // No need to increment current_version as it's handled by enumerate

                    // Add artificial delay between batches
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                // Calculate cutoff time
                let cutoff_time = (chrono::Utc::now() - chrono::Duration::hours(cutoff_hours))
                    .timestamp();

                // Run garbage collection
                let sysdb_config = SysDbConfig::Grpc(GrpcSysDbConfig {
                    host: "localhost".to_string(),
                    port: 50051,
                    connect_timeout_ms: 5000,
                    request_timeout_ms: 10000,
                    num_channels: 1,
                });

                let mut sysdb = SysDb::try_from_config(&sysdb_config, &registry).await.unwrap();

                // Get collection info for GC
                let collections_to_gc = sysdb.get_collections_to_gc().await.unwrap();
                let collection_info = collections_to_gc.iter()
                    .find(|c| c.id.0.to_string() == collection_id)
                    .expect("Collection should be available for GC");

                // Run GC with the specified cutoff time
                run_gc(
                    &collection_id,
                    &collection_info.version_file_path,
                    storage.clone(),
                    sysdb.clone(),
                ).await.unwrap();

                // Give GC time to complete
                tokio::time::sleep(Duration::from_secs(2)).await;

                // Verify properties

                // 1. Check version ordering
                prop_assert!(
                    verify_version_ordering(&mut clients, &collection_id, &tenant_id).await.unwrap(),
                    "Versions are not properly ordered"
                );

                // 2. Check versions relative to cutoff time
                prop_assert!(
                    verify_versions_after_cutoff(
                        &mut clients,
                        &collection_id,
                        &tenant_id,
                        cutoff_time,
                        2 // minimum versions to keep
                    ).await.unwrap(),
                    "Versions don't satisfy cutoff time requirements"
                );

                // 3. Verify data integrity
                let final_records = clients.get_records(
                    &collection_id,
                    None,
                    true,
                    false,
                    false,
                ).await.unwrap();

                prop_assert_eq!(
                    final_records.ids.len(),
                    num_records,
                    "Record count changed after GC"
                );

                Ok(())
            })?;
        }
    }
}
