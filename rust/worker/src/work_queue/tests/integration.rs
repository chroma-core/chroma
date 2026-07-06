#[cfg(test)]
mod tests {
    use crate::work_queue::work_queue_client::WorkQueueClient;
    use chroma_config::registry::Registry;
    use chroma_config::Configurable;
    use chroma_sysdb::{DatabaseOrTopology, GetCollectionsOptions, SysDb};
    use chroma_types::chroma_proto::{
        CheckInvocationStatusRequest, InvocationCheckItem, InvocationStatus,
    };
    use chroma_types::{AttachedFunctionUuid, CollectionUuid, DatabaseName, SegmentFlushInfo};
    use std::sync::Arc;
    use uuid::Uuid;

    struct TestContext {
        work_queue_client: WorkQueueClient,
        sysdb: SysDb,
        tenant_id: String,
        database_name: String,
    }

    async fn setup_test_context() -> Result<TestContext, Box<dyn std::error::Error>> {
        // Connect to work queue service
        let work_queue_client = WorkQueueClient::new("http://localhost:50058".to_string()).await?;

        // Connect to Go sysdb (requires Tilt running) using same config as compact.rs test
        let registry = Registry::new();
        // Use port-forward to access sysdb in chroma namespace
        let sysdb_config = chroma_sysdb::SysDbConfig::Grpc(chroma_sysdb::GrpcSysDbConfig {
            host: "localhost".to_string(),
            port: 50051,
            connect_timeout_ms: 5000,
            request_timeout_ms: 10000,
            num_channels: 4,
        });
        let sysdb = SysDb::try_from_config(&(sysdb_config, None), &registry).await?;

        // Use pre-existing tenant and database
        let tenant_id = "default_tenant".to_string();
        let database_name = "default_database".to_string();

        Ok(TestContext {
            work_queue_client,
            sysdb,
            tenant_id,
            database_name,
        })
    }

    async fn with_work_queue_test<F, Fut>(test_fn: F)
    where
        F: FnOnce(TestContext) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let context = setup_test_context()
            .await
            .expect("Failed to setup test context. Is Tilt running?");
        test_fn(context).await
        // Cleanup if needed - could delete tenant/database here
    }

    async fn create_test_collection(
        sysdb: &mut SysDb,
        collection_id: CollectionUuid,
        tenant_id: &str,
        database_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use chroma_types::{Segment, SegmentScope, SegmentType, SegmentUuid};

        let collection_name = format!("test_collection_{}", Uuid::new_v4());
        let db_name = chroma_types::DatabaseName::new(database_name.to_string()).unwrap();

        // Create collection with segments like the AttachFunction does
        let segments = vec![
            Segment {
                r#type: SegmentType::BlockfileMetadata,
                scope: SegmentScope::METADATA,
                collection: collection_id,
                id: SegmentUuid(Uuid::new_v4()),
                metadata: None,
                file_path: Default::default(),
            },
            Segment {
                r#type: SegmentType::BlockfileRecord,
                scope: SegmentScope::RECORD,
                collection: collection_id,
                id: SegmentUuid(Uuid::new_v4()),
                metadata: None,
                file_path: Default::default(),
            },
            Segment {
                r#type: SegmentType::HnswDistributed,
                scope: SegmentScope::VECTOR,
                collection: collection_id,
                id: SegmentUuid(Uuid::new_v4()),
                metadata: None,
                file_path: Default::default(),
            },
        ];

        sysdb
            .create_collection(
                tenant_id.to_string(),
                db_name,
                collection_id,
                collection_name,
                segments,
                None,    // configuration json
                None,    // schema
                None,    // metadata
                Some(1), // dimension
                false,   // get_or_create
            )
            .await?;

        Ok(())
    }

    async fn create_test_attached_function(
        sysdb: &mut SysDb,
        collection_id: CollectionUuid,
    ) -> Result<AttachedFunctionUuid, Box<dyn std::error::Error>> {
        // Create a test attached function similar to compact.rs test
        let fn_name = format!("test_function_{}", Uuid::new_v4());
        let output_collection_name = format!("output_collection_{}", Uuid::new_v4());

        let (attached_function_id, _created) = sysdb
            .create_attached_function(
                fn_name,
                "dummy_async".to_string(), // Use the async function from migration
                collection_id,
                output_collection_name,
                serde_json::Value::Null,
                "default_tenant".to_string(),
                "default_database".to_string(),
                10, // min_records_for_invocation
            )
            .await?;

        // Finish creating the function with output schema
        let output_schema = serde_json::json!({
            "test": "schema"
        });
        sysdb
            .finish_create_attached_function(attached_function_id, output_schema.to_string())
            .await?;

        Ok(attached_function_id)
    }

    // Note: In a real scenario, updating collection log position would be done
    // through the log service, not sysdb. For testing work queue repair logic,
    // we rely on the sysdb methods to simulate repair conditions.

    #[tokio::test]
    async fn test_k8s_integration_work_queue_lifecycle() {
        with_work_queue_test(|mut ctx| async move {
            let coll_id = CollectionUuid::new();
            let offset = 100;

            // Create collection
            create_test_collection(&mut ctx.sysdb, coll_id, &ctx.tenant_id, &ctx.database_name)
                .await
                .expect("Failed to create collection");

            // Create async attached function
            let fn_id = create_test_attached_function(&mut ctx.sysdb, coll_id)
                .await
                .expect("Failed to create async attached function");

            // Push work
            ctx.work_queue_client
                .push_work(fn_id.to_string(), coll_id.to_string(), offset, None)
                .await
                .expect("Failed to push work");

            // Get work
            let work_items = ctx
                .work_queue_client
                .get_work("test_shard".to_string(), 10)
                .await
                .expect("Failed to get work");

            println!("Got {} work items", work_items.items.len());
            for (i, item) in work_items.items.iter().enumerate() {
                println!(
                    "  Item {}: fn_id={}, offset={}",
                    i, item.fn_id, item.completion_offset
                );
            }

            // Filter work items to only our function ID
            let our_items: Vec<_> = work_items
                .items
                .iter()
                .filter(|item| item.fn_id == fn_id.to_string())
                .collect();

            assert_eq!(our_items.len(), 1, "Expected 1 work item for our function");
            assert_eq!(our_items[0].completion_offset, offset);

            // Finish work
            ctx.work_queue_client
                .finish_work(fn_id.to_string(), coll_id.to_string(), 200)
                .await
                .expect("Failed to finish work");

            // Get work again - should not contain our function
            let work_items = ctx
                .work_queue_client
                .get_work("test_shard".to_string(), 10)
                .await
                .expect("Failed to get work after finish");

            println!("After finish: got {} work items", work_items.items.len());
            let our_items_after: Vec<_> = work_items
                .items
                .iter()
                .filter(|item| item.fn_id == fn_id.to_string())
                .collect();
            assert_eq!(
                our_items_after.len(),
                0,
                "Expected 0 work items for our function after finish"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn test_k8s_integration_work_queue_fifo_and_filtering() {
        with_work_queue_test(|mut ctx| async move {
            let mut work_items = Vec::new();

            // Create multiple collections and attached functions
            for i in 0..3 {
                let coll_id = CollectionUuid::new();

                create_test_collection(&mut ctx.sysdb, coll_id, &ctx.tenant_id, &ctx.database_name)
                    .await
                    .expect("Failed to create collection");

                let fn_id = create_test_attached_function(&mut ctx.sysdb, coll_id)
                    .await
                    .expect("Failed to create async attached function");

                ctx.work_queue_client
                    .push_work(fn_id.to_string(), coll_id.to_string(), i * 100, None)
                    .await
                    .expect("Failed to push work");

                work_items.push((fn_id, coll_id, i * 100));
            }

            // Get work - should return in FIFO order
            let retrieved = ctx
                .work_queue_client
                .get_work("test_shard".to_string(), 10)
                .await
                .expect("Failed to get work");

            println!("Got {} work items total", retrieved.items.len());

            // Filter to only our test items
            let our_fn_ids: std::collections::HashSet<String> = work_items
                .iter()
                .map(|(fn_id, _, _)| fn_id.to_string())
                .collect();

            let our_retrieved: Vec<_> = retrieved
                .items
                .iter()
                .filter(|item| our_fn_ids.contains(&item.fn_id))
                .collect();

            assert_eq!(
                our_retrieved.len(),
                3,
                "Expected 3 work items for our functions"
            );

            // Check FIFO order by completion offset (assuming same order as pushed)
            for (i, item) in our_retrieved.iter().enumerate() {
                let expected_offset = i * 100;
                assert_eq!(
                    item.completion_offset, expected_offset as i64,
                    "Expected offset {} for item {}",
                    expected_offset, i
                );
            }

            // Mark some as completed
            for i in [0, 2] {
                ctx.work_queue_client
                    .finish_work(
                        work_items[i].0.to_string(),
                        work_items[i].1.to_string(),
                        work_items[i].2 + 50,
                    )
                    .await
                    .expect("Failed to finish work");
            }

            // Get work again - should filter out completed items
            let filtered = ctx
                .work_queue_client
                .get_work("test_shard".to_string(), 10)
                .await
                .expect("Failed to get filtered work");

            println!(
                "After finishing items 0 and 2: got {} work items total",
                filtered.items.len()
            );

            // Filter to only our remaining test item
            let our_filtered: Vec<_> = filtered
                .items
                .iter()
                .filter(|item| item.fn_id == work_items[1].0.to_string())
                .collect();

            // Should see only item 1 (indices 0 and 2 were completed)
            assert_eq!(
                our_filtered.len(),
                1,
                "Expected 1 remaining work item for our function"
            );
            assert_eq!(our_filtered[0].completion_offset, work_items[1].2);
        })
        .await;
    }

    #[tokio::test]
    async fn test_k8s_integration_work_queue_repair_flow() {
        with_work_queue_test(|mut ctx| async move {
            let coll_id = CollectionUuid::new();
            let initial_offset = 100;
            let new_offset = 150;
            let advanced_log_position = 200;

            // Create collection
            create_test_collection(&mut ctx.sysdb, coll_id, &ctx.tenant_id, &ctx.database_name)
                .await
                .expect("Failed to create collection");

            // Create async attached function
            let fn_id = create_test_attached_function(&mut ctx.sysdb, coll_id)
                .await
                .expect("Failed to create async attached function");

            let database_name =
                DatabaseName::new(ctx.database_name.clone()).expect("Invalid database name");
            let collections = ctx
                .sysdb
                .get_collections(GetCollectionsOptions {
                    collection_id: Some(coll_id),
                    tenant: Some(ctx.tenant_id.clone()),
                    database_or_topology: Some(DatabaseOrTopology::Database(database_name.clone())),
                    ..Default::default()
                })
                .await
                .expect("Failed to fetch collection before advancing log position");
            assert_eq!(collections.len(), 1, "Expected exactly one collection");

            ctx.sysdb
                .flush_compaction(
                    ctx.tenant_id.clone(),
                    database_name,
                    coll_id,
                    advanced_log_position,
                    collections[0].version,
                    Arc::<[SegmentFlushInfo]>::from([]),
                    0,
                    0,
                    None,
                )
                .await
                .expect("Failed to advance collection log position");

            // Push work
            ctx.work_queue_client
                .push_work(fn_id.to_string(), coll_id.to_string(), initial_offset, None)
                .await
                .expect("Failed to push work");

            // Finish work - the queue frontier should keep the entry alive while sysdb
            // records that additional work is still needed.
            ctx.work_queue_client
                .finish_work(fn_id.to_string(), coll_id.to_string(), new_offset)
                .await
                .expect("Failed to finish work");

            // Get work - this branch still re-enqueues repair work into the queue.
            let work_items = ctx
                .work_queue_client
                .get_work("test_shard".to_string(), 10)
                .await
                .expect("Failed to get work after repair");

            println!(
                "After repair: got {} work items total",
                work_items.items.len()
            );

            // The queue still exposes a repaired item for this function on this branch.
            let our_items: Vec<_> = work_items
                .items
                .iter()
                .filter(|item| item.fn_id == fn_id.to_string())
                .collect();

            assert_eq!(
                our_items.len(),
                1,
                "Expected repair to keep a visible work item on this branch"
            );

            // Check invocation status via sysdb
            let status_response = ctx
                .sysdb
                .clone()
                .check_invocation_status(CheckInvocationStatusRequest {
                    items: vec![
                        InvocationCheckItem {
                            function_id: fn_id.to_string(),
                            input_collection_id: coll_id.to_string(),
                            completion_offset: initial_offset,
                        },
                        InvocationCheckItem {
                            function_id: fn_id.to_string(),
                            input_collection_id: coll_id.to_string(),
                            completion_offset: new_offset,
                        },
                    ],
                })
                .await
                .expect("Failed to check invocation status")
                .into_inner();

            // Verify invocation statuses
            assert_eq!(status_response.results.len(), 2);
            println!(
                "Invocation status: initial_offset={} status={:?}, new_offset={} status={:?}",
                initial_offset,
                status_response.results[0].status,
                new_offset,
                status_response.results[1].status
            );

            // The original queue offset should be marked as needing repair because sysdb's
            // completion has advanced while the collection frontier is still ahead.
            assert_eq!(
                status_response.results[0].status,
                InvocationStatus::NeedsRepair as i32,
                "Initial offset should still require repair"
            );
            assert_eq!(
                status_response.results[0].current_completion_offset, new_offset,
                "Repair status should report the latest sysdb completion offset"
            );

            // The latest completion offset is not yet done because the collection frontier is ahead.
            assert_eq!(
                status_response.results[1].status,
                InvocationStatus::NotDone as i32,
                "New offset should be marked as not done until the frontier is reached"
            );
            assert_eq!(
                status_response.results[1].current_completion_offset, new_offset,
                "The latest sysdb completion offset should be returned for the pending work"
            );
        })
        .await;
    }
}
