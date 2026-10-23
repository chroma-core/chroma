//! Partial validation: unchanged production operators and actual type bridge.
//! This excludes the worker orchestrator wrapper and does not enable usearch.
mod execution {
    pub mod operators {
        pub mod finish_attached_function {
            include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../worker/src/execution/operators/finish_attached_function.rs"));
        }
        pub mod register {
            include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../worker/src/execution/operators/register.rs"));
        }
    }
}
#[path = "../../worker/src/execution/orchestration/hierarchical_register_bridge.rs"]
mod bridge;
#[cfg(test)]
mod hierarchical_registration_tests {
    use crate::execution::operators::register::{RegisterInput, RegisterOperator, RegisterError};
    use chroma_types::SegmentFlushInfo;
    use crate::bridge::{attach_descriptor, CapturedContext, HIERARCHY_BUNDLE};
    use chroma_log::{in_memory_log::InMemoryLog, Log};
    use chroma_sysdb::{GetCollectionsOptions, SysDb, TestSysDb};
    use chroma_system::Operator;
    use chroma_types::{test_segment, Collection, SegmentScope, SegmentType};

    // These real test clients establish the operator call path and error order,
    // not production atomicity or persistence of a cloned in-memory log.
    async fn exercise(fail_log_update: bool) {
        let mut collection = Collection::test_collection(4);
        collection.version = 3;
        collection.log_position = 10;
        collection.total_records_post_compaction = 16;
        let mut vector = test_segment(collection.collection_id, SegmentScope::VECTOR);
        vector.r#type = SegmentType::Spann;
        vector.file_path.insert(HIERARCHY_BUNDLE.into(), vec!["parent-descriptor".into()]);
        vector.file_path.insert("unrelated".into(), vec!["retained".into()]);
        let other = test_segment(collection.collection_id, SegmentScope::RECORD);
        let parent = vec![
            SegmentFlushInfo { segment_id: vector.id, file_paths: vector.file_path.clone() },
            SegmentFlushInfo { segment_id: other.id, file_paths: other.file_path.clone() },
        ];
        let mut test_sysdb = TestSysDb::new();
        test_sysdb.add_collection(collection.clone());
        test_sysdb.add_segment(vector.clone());
        test_sysdb.add_segment(other.clone());
        let sysdb = SysDb::Test(test_sysdb);
        let mut test_log = InMemoryLog::new();
        test_log.set_fail_update_offset(fail_log_update);
        let log = Log::InMemory(test_log);
        let expected = CapturedContext {
            collection_id: collection.collection_id,
            tenant: collection.tenant.clone(),
            database: collection.database.clone(),
            collection_version: 3,
            parent_log_position: 10,
            pulled_log_offset: 12,
            target_total_records: 18,
            target_logical_size_bytes: 288,
            vector_segment_id: vector.id.0,
            parent_vector_files: vector.file_path.clone(),
        };
        let reference = serde_json::json!({"path":"/owned/sealed-hierarchy.json","sha256":"a".repeat(64),"bytes":123}).to_string();
        collection.total_records_post_compaction = 18;
        // This target deliberately does not compile CollectionRegisterInfo or
        // CompactionContext. It compiles the unmodified production operators.
        let make_input = || {
            let outputs = attach_descriptor(&collection, collection.collection_id,
                12, 288, &vector, &parent, parent.clone(), &expected, reference.clone()).unwrap();
            let flush = chroma_types::CollectionFlushInfo {
                tenant_id: collection.tenant.clone(),
                database_name: chroma_types::DatabaseName::new(collection.database.clone()).unwrap(),
                collection_id: collection.collection_id,
                collection_version: 3,
                log_position: 12,
                segment_flush_info: outputs.into(),
                total_records_post_compaction: 18,
                size_bytes_post_compaction: 288,
                schema: None,
            };
            RegisterInput::new(flush.tenant_id, flush.database_name, flush.collection_id,
                flush.log_position, flush.collection_version, flush.segment_flush_info,
                flush.total_records_post_compaction, flush.size_bytes_post_compaction,
                sysdb.clone(), log.clone(), flush.schema)
        };
        let input = make_input();
        let result = RegisterOperator::new().run(&input).await;
        if fail_log_update {
            assert!(matches!(result, Err(RegisterError::UpdateLogOffset(_))));
        } else {
            assert!(result.is_ok());
        }
        let observed = sysdb.get_collections(GetCollectionsOptions {
            collection_id: Some(collection.collection_id), ..Default::default()
        }).await.unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].version, 4);
        assert_eq!(observed[0].log_position, 12);
        assert_eq!(observed[0].total_records_post_compaction, 18);
        assert_eq!(observed[0].size_bytes_post_compaction, 288);
        assert_eq!(observed[0].tenant, collection.tenant);
        assert_eq!(observed[0].database, collection.database);
        let segments = sysdb.get_segments(None, None, None, collection.collection_id).await.unwrap();
        assert_eq!(segments.len(), 2);
        let saved = segments.iter().find(|segment| segment.id == vector.id).unwrap();
        assert_eq!(saved.file_path[HIERARCHY_BUNDLE], vec![reference.clone()]);
        assert_eq!(saved.file_path["unrelated"], vec!["retained"]);
        assert_eq!(segments.iter().find(|segment| segment.id == other.id).unwrap().file_path, other.file_path);
        // The stored newer version rejects another use of the old context in
        // this mock. This does not establish the mock's absent future-version fence.
        let stale = make_input();
        assert!(matches!(RegisterOperator::new().run(&stale).await, Err(RegisterError::FlushCompaction(_))));
    }

    #[tokio::test]
    async fn hierarchy_input_uses_existing_register_operator() { exercise(false).await; }

    #[tokio::test]
    async fn hierarchy_log_error_preserves_observed_sysdb_commit() { exercise(true).await; }
}
