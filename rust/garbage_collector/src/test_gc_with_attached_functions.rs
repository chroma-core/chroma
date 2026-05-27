#[cfg(test)]
mod tests {
    use crate::garbage_collector_orchestrator_v2::GarbageCollectorOrchestrator;
    use crate::helper::{setup_tilt_log, setup_tilt_storage, ChromaGrpcClients};
    use chroma_blockstore::RootManager;
    use chroma_cache::nop::NopCache;
    use chroma_config::registry::Registry;
    use chroma_log::Log;
    use chroma_storage::GetOptions;
    use chroma_sysdb::{GetCollectionsOptions, TestSysDb};
    use chroma_system::{Dispatcher, Orchestrator, System};
    use chroma_types::{
        AttachedFunction, AttachedFunctionUuid, CollectionUuid, DatabaseName, Operation,
        OperationRecord, Segment, SegmentFlushInfo, SegmentScope, SegmentType, SegmentUuid,
    };
    use chrono::DateTime;
    use std::{collections::HashMap, sync::Arc, time::SystemTime};
    use uuid::Uuid;
    use wal3::{Cursor, CursorName, CursorStore, CursorStoreOptions, LogPosition};

    async fn push_test_logs(
        logs: &mut Log,
        collection_id: CollectionUuid,
        database_name: &DatabaseName,
        record_count: usize,
    ) {
        for i in 0..record_count {
            logs.push_logs(
                "test_tenant",
                database_name.clone(),
                collection_id,
                vec![OperationRecord {
                    id: format!("attached-fn-record-{i}"),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                }],
                None,
            )
            .await
            .expect("push_logs should succeed");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_k8s_integration_gc_preserves_logs_for_attached_function() {
        let mut clients = match ChromaGrpcClients::new().await {
            Ok(clients) => clients,
            Err(err) => {
                panic!("Skipping test: Tilt gRPC services not reachable: {err:?}. Is Tilt running?")
            }
        };

        let registry = Registry::new();
        let storage = setup_tilt_storage(&registry)
            .await
            .expect("Tilt S3 storage should be reachable");

        let collection_id = CollectionUuid::new();
        let tenant = "test_tenant".to_string();
        let database = DatabaseName::new("test_database").expect("database name should be valid");

        let system = System::new();
        let dispatcher = Dispatcher::new(Default::default());
        let dispatcher_handle = system.start_component(dispatcher);
        let root_manager = RootManager::new(storage.clone(), Box::new(NopCache));
        let grpc_log = setup_tilt_log(&system, &registry)
            .await
            .expect("Tilt log service should be reachable");

        let mut test_sysdb = TestSysDb::new();
        test_sysdb.set_storage(Some(storage.clone()));
        let mut sysdb = chroma_sysdb::SysDb::Test(test_sysdb);

        let segment_id = SegmentUuid::new();
        let segment = Segment {
            id: segment_id,
            r#type: SegmentType::BlockfileMetadata,
            scope: SegmentScope::METADATA,
            collection: collection_id,
            metadata: None,
            file_path: HashMap::new(),
        };

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
            .expect("create_collection should succeed");

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
                database_id: database.as_ref().to_string(),
                last_run: None,
                completion_offset: 50,
                min_records_for_invocation: 10,
                is_deleted: false,
                is_async: true,
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
            };

            let mut attached_functions = HashMap::new();
            attached_functions.insert(collection_id, vec![attached_fn]);
            test_sysdb.set_attached_functions(attached_functions);
        }

        let mut seed_logs = grpc_log.clone();
        push_test_logs(&mut seed_logs, collection_id, &database, 100).await;

        let state_before_compaction = clients
            .inspect_log_state(collection_id, &database)
            .await
            .expect("inspect_log_state should succeed");
        let compaction_offset = state_before_compaction
            .limit
            .checked_sub(1)
            .expect("log should contain at least one record");
        let cursor_store = CursorStore::new(
            CursorStoreOptions::default(),
            Arc::new(storage.clone()),
            collection_id.storage_prefix_for_log(),
            "test-writer".to_string(),
        );
        cursor_store
            .init(
                &CursorName::new("so_you_may_gc").expect("cursor name should be valid"),
                Cursor {
                    position: LogPosition::from_offset(compaction_offset),
                    epoch_us: compaction_offset,
                    writer: "test-writer".to_string(),
                },
            )
            .await
            .expect("cursor should initialize");

        let new_compaction_offset: i64 = 80;

        sysdb
            .flush_compaction(
                tenant.clone(),
                database.clone(),
                collection_id,
                new_compaction_offset,
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
            .expect("flush_compaction should succeed");

        let mut collections = sysdb
            .get_collections(GetCollectionsOptions {
                collection_id: Some(collection_id),
                ..Default::default()
            })
            .await
            .expect("get_collections should succeed");
        let collection = collections.pop().expect("collection should exist");

        let now = DateTime::from_timestamp(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("system time should be after epoch")
                .as_secs() as i64,
            0,
        )
        .expect("timestamp should be valid");

        let fragment_prefix = format!("{}/log/", collection_id.storage_prefix_for_log());
        let fragment_count_before_first_gc = storage
            .list_prefix(&fragment_prefix, GetOptions::default())
            .await
            .expect("list_prefix before first GC should succeed")
            .len();
        assert!(
            fragment_count_before_first_gc > 0,
            "expected WAL fragments before first GC"
        );

        let orchestrator = GarbageCollectorOrchestrator::new(
            collection_id,
            database.clone(),
            collection
                .version_file_path
                .clone()
                .expect("version file path should exist"),
            None,
            now,
            now,
            sysdb.clone(),
            dispatcher_handle.clone(),
            system.clone(),
            storage.clone(),
            grpc_log.clone(),
            None,
            root_manager.clone(),
            crate::types::CleanupMode::DeleteV2,
            1,
            true,
            false,
            10,
        );

        orchestrator
            .run(system.clone())
            .await
            .expect("first GC run should succeed");

        let state_after_first_gc = clients
            .inspect_log_state(collection_id, &database)
            .await
            .expect("inspect_log_state should succeed");
        assert_eq!(
            state_after_first_gc.start, 51,
            "first GC should keep logs starting at offset 51 while the attached function is behind"
        );
        let mut read_logs = grpc_log.clone();
        let read_from_50 = read_logs
            .read("test_tenant", database.clone(), collection_id, 50, 2, None)
            .await;
        assert!(
            read_from_50.is_err(),
            "offset 50 should no longer be readable after the first GC pass"
        );

        let read_from_51 = read_logs
            .read("test_tenant", database.clone(), collection_id, 51, 1, None)
            .await
            .expect("read from offset 51 should succeed");
        assert_eq!(
            read_from_51.first().map(|r| r.log_offset),
            Some(51),
            "offset 51 should remain readable after the first GC pass"
        );

        let fragment_count_after_first_gc = storage
            .list_prefix(&fragment_prefix, GetOptions::default())
            .await
            .expect("list_prefix after first GC should succeed")
            .len();
        assert!(
            fragment_count_after_first_gc < fragment_count_before_first_gc,
            "first GC should delete some WAL fragments: before={} after={}",
            fragment_count_before_first_gc,
            fragment_count_after_first_gc
        );

        // Attached function is ahead of compaction now.
        let new_completion_offset = new_compaction_offset + 4;

        let finish_request =
            chroma_types::chroma_proto::TryFinishAsyncAttachedFunctionInvocationRequest {
                attached_function_id: attached_fn_id.0.to_string(),
                collection_id: collection_id.to_string(),
                new_completion_offset: new_completion_offset as u64,
            };

        sysdb
            .clone()
            .try_finish_async_attached_function_invocation(finish_request)
            .await
            .expect("advancing attached function should succeed");

        let orchestrator2 = GarbageCollectorOrchestrator::new(
            collection_id,
            database.clone(),
            collection
                .version_file_path
                .expect("version file path should exist"),
            None,
            now,
            now,
            sysdb,
            dispatcher_handle,
            system.clone(),
            storage.clone(),
            grpc_log,
            None,
            root_manager,
            crate::types::CleanupMode::DeleteV2,
            1,
            true,
            false,
            10,
        );

        orchestrator2
            .run(system.clone())
            .await
            .expect("second GC run should succeed");

        let state_after_second_gc = clients
            .inspect_log_state(collection_id, &database)
            .await
            .expect("inspect_log_state should succeed");
        assert_eq!(
            state_after_second_gc.start,
            (new_compaction_offset + 1) as u64,
            "once the attached function catches up, GC should advance to the compaction boundary"
        );

        let fragment_count_after_second_gc = storage
            .list_prefix(&fragment_prefix, GetOptions::default())
            .await
            .expect("list_prefix after second GC should succeed")
            .len();
        assert!(
            fragment_count_after_second_gc < fragment_count_after_first_gc,
            "second GC should delete additional WAL fragments: first={} second={}",
            fragment_count_after_first_gc,
            fragment_count_after_second_gc
        );

        system.stop().await;
        system.join().await;
    }
}
