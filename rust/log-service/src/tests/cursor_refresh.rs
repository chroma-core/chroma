use super::*;

#[tokio::test]
async fn test_k8s_integration_stale_cursor_refreshes_after_collection_moves_hosts() {
    let storage = chroma_storage::s3_client_for_test_with_new_bucket().await;
    let region = RegionName::new("local").unwrap();
    let mut server = log_server_for_installed_writer_tests();
    server.storages = Arc::new(MultiCloudMultiRegionConfiguration {
        preferred: region.clone(),
        regions: vec![ProviderRegion {
            name: region,
            provider: "aws".to_string(),
            region: "us-east-1".to_string(),
            config: RegionalStorage {
                storage: storage.clone(),
            },
        }],
        topologies: vec![],
    });
    server.config.reinsert_threshold = 2;
    server.config.num_records_before_backpressure = 10;
    server.cache = Some(Arc::from(chroma_cache::new_cache_for_test()));
    let collection_id = CollectionUuid::new();
    let cache = server.cache.as_ref().unwrap();
    let cache_key = cache_key_for_cursor(collection_id, &INTRINSIC_CURSOR);
    let cursors = CursorStore::new(
        CursorStoreOptions::default(),
        Arc::new(storage),
        collection_id.storage_prefix_for_log(),
        "new-log-host".to_string(),
    );
    let mut committed = Cursor {
        position: LogPosition::from_offset(1),
        ..Default::default()
    };
    cursors
        .init(&INTRINSIC_CURSOR, committed.clone())
        .await
        .unwrap();
    let stale_witness = cursors.load(&INTRINSIC_CURSOR).await.unwrap().unwrap();
    cache
        .insert(
            cache_key.clone(),
            CachedBytes::new(serde_json::to_vec(&stale_witness).unwrap()),
        )
        .await;

    // Another host commits all ten records without updating this host's cache.
    committed.position = LogPosition::from_offset(11);
    cursors
        .save(&INTRINSIC_CURSOR, &committed, &stale_witness)
        .await
        .unwrap();
    let mut stale_rollup = RollupPerCollection::new(LogPosition::from_offset(1), 10, 0);
    stale_rollup.reinsert_count = 1;
    let mut stale = HashMap::from([((None, collection_id), stale_rollup)]);
    server.enrich_dirty_log(&mut stale).await.unwrap();
    assert!(stale[&(None, collection_id)].requires_backpressure(10));
    assert_eq!(stale[&(None, collection_id)].start_log_position.offset(), 1);

    let dirty_log = FakeLogWriter::new(vec![Ok(LogPosition::from_offset(1))]);
    let rollup = Rollup {
        witness: None,
        cursor: Cursor::default(),
        last_record_witnessed: LogPosition::from_offset(1),
        rollups: stale,
    };
    let (_, _, saved) = server.save_dirty_log(rollup, &*dirty_log).await.unwrap();
    assert!(
        cache.get(&cache_key).await.unwrap().is_none(),
        "crossing the reinsertion threshold must evict the former host's cursor"
    );
    assert_eq!(saved[&collection_id].reinsert_count, 2);

    let mut refreshed = saved
        .into_iter()
        .map(|(id, rollup)| ((None, id), rollup))
        .collect();
    server.enrich_dirty_log(&mut refreshed).await.unwrap();
    assert!(
        refreshed.is_empty(),
        "the current cursor removes the stale backlog"
    );
    assert_eq!(
        cursors
            .load(&INTRINSIC_CURSOR)
            .await
            .unwrap()
            .unwrap()
            .cursor
            .position
            .offset(),
        11,
        "refreshing cached state must preserve the committed cursor"
    );
}
