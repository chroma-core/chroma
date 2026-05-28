#[cfg(test)]
mod tests {
    use crate::garbage_collector_orchestrator_v2::GarbageCollectorOrchestrator;
    use crate::helper::{setup_tilt_log, setup_tilt_storage};
    use chroma_blockstore::RootManager;
    use chroma_cache::nop::NopCache;
    use chroma_config::registry::Registry;
    use chroma_log::Log;
    use chroma_storage::{GetOptions, PutOptions};
    use chroma_sysdb::{GetCollectionsOptions, TestSysDb};
    use chroma_system::{Dispatcher, Orchestrator, System};
    use chroma_types::{
        AttachedFunction, AttachedFunctionUuid, CollectionUuid, DatabaseName, Operation,
        OperationRecord, Segment, SegmentFlushInfo, SegmentScope, SegmentType, SegmentUuid,
        USER_ID_BLOOM_FILTER,
    };
    use chrono::Utc;
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

    async fn assert_paths_exist(
        storage: &chroma_storage::Storage,
        paths: &[String],
        message: &str,
    ) {
        for path in paths {
            storage
                .get(path, GetOptions::default())
                .await
                .unwrap_or_else(|_| panic!("{message}: missing {path}"));
        }
    }

    async fn assert_paths_deleted(
        storage: &chroma_storage::Storage,
        paths: &[String],
        message: &str,
    ) {
        for path in paths {
            assert!(
                storage.get(path, GetOptions::default()).await.is_err(),
                "{message}: still found {path}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_k8s_integration_gc_preserves_version_floor_for_attached_function() {
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
        push_test_logs(&mut seed_logs, collection_id, &database, 60).await;
        let first_compaction_offset = 50;
        let first_version_paths = vec![
            format!("gc-attached-functions/{collection_id}/v1/metadata.bin"),
            format!("gc-attached-functions/{collection_id}/v1/postings.bin"),
        ];
        for path in &first_version_paths {
            storage
                .put_bytes(path, vec![1, 2, 3], PutOptions::default())
                .await
                .expect("first version file path should be written");
        }

        sysdb
            .flush_compaction(
                tenant.clone(),
                database.clone(),
                collection_id,
                first_compaction_offset,
                0,
                Arc::new([SegmentFlushInfo {
                    segment_id,
                    file_paths: HashMap::from([(
                        USER_ID_BLOOM_FILTER.to_string(),
                        first_version_paths.clone(),
                    )]),
                }]),
                60,
                0,
                None,
            )
            .await
            .expect("first flush_compaction should succeed");

        push_test_logs(&mut seed_logs, collection_id, &database, 60).await;
        let second_compaction_offset = 110;
        let second_version_paths = vec![
            format!("gc-attached-functions/{collection_id}/v2/metadata.bin"),
            format!("gc-attached-functions/{collection_id}/v2/postings.bin"),
        ];
        for path in &second_version_paths {
            storage
                .put_bytes(path, vec![4, 5, 6], PutOptions::default())
                .await
                .expect("second version file path should be written");
        }

        sysdb
            .flush_compaction(
                tenant.clone(),
                database.clone(),
                collection_id,
                second_compaction_offset,
                1,
                Arc::new([SegmentFlushInfo {
                    segment_id,
                    file_paths: HashMap::from([(
                        USER_ID_BLOOM_FILTER.to_string(),
                        second_version_paths.clone(),
                    )]),
                }]),
                120,
                0,
                None,
            )
            .await
            .expect("second flush_compaction should succeed");

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
                    position: LogPosition::from_offset(second_compaction_offset as u64),
                    epoch_us: second_compaction_offset as u64,
                    writer: "test-writer".to_string(),
                },
            )
            .await
            .expect("cursor should initialize");

        let mut collections = sysdb
            .get_collections(GetCollectionsOptions {
                collection_id: Some(collection_id),
                ..Default::default()
            })
            .await
            .expect("get_collections should succeed");
        let collection = collections.pop().expect("collection should exist");

        let now = Utc::now();

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

        assert_paths_exist(
            &storage,
            &first_version_paths,
            "first GC should preserve the first compacted version while the attached function is behind",
        )
        .await;
        assert_paths_exist(
            &storage,
            &second_version_paths,
            "first GC should keep the current compacted version",
        )
        .await;

        let first_gc_versions = sysdb
            .list_collection_versions(collection_id)
            .await
            .expect("list_collection_versions after first GC should succeed");
        assert!(
            first_gc_versions.iter().any(|version| version.version == 1),
            "first GC should retain the first compacted version in version history"
        );

        let new_completion_offset = second_compaction_offset + 4;

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
            sysdb.clone(),
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

        assert_paths_deleted(
            &storage,
            &first_version_paths,
            "second GC should delete the first compacted version once the attached function catches up",
        )
        .await;
        assert_paths_exist(
            &storage,
            &second_version_paths,
            "second GC should keep the current compacted version",
        )
        .await;

        let second_gc_versions = sysdb
            .list_collection_versions(collection_id)
            .await
            .expect("list_collection_versions after second GC should succeed");
        assert!(
            second_gc_versions
                .iter()
                .all(|version| version.version != 1),
            "second GC should delete the first compacted version from version history"
        );

        system.stop().await;
        system.join().await;
    }
}
