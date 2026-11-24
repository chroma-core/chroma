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
    server.config.compaction_cursor_refresh_interval_rollups = 100;
    server.config.num_records_before_backpressure = 100;
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

fn server_with_storage(storage: Storage) -> LogServer {
    let mut server = log_server_for_installed_writer_tests();
    let region = RegionName::new("local").unwrap();
    server.storages = Arc::new(MultiCloudMultiRegionConfiguration {
        preferred: region.clone(),
        regions: vec![ProviderRegion {
            name: region,
            provider: "aws".to_string(),
            region: "us-east-1".to_string(),
            config: RegionalStorage { storage },
        }],
        topologies: vec![],
    });
    server.cache = Some(Arc::from(chroma_cache::new_cache_for_test()));
    server.config.num_records_before_backpressure = 10;
    server
}

async fn cache_old_cursor_and_commit(
    server: &LogServer,
    id: CollectionUuid,
    position: u64,
) -> CursorStore {
    let cursors = CursorStore::new(
        CursorStoreOptions::default(),
        Arc::new(server.preferred_storage().unwrap().clone()),
        id.storage_prefix_for_log(),
        "other-host".to_string(),
    );
    write_cursor(server.preferred_storage().unwrap(), id, 1).await;
    let stale = cursors.load(&INTRINSIC_CURSOR).await.unwrap().unwrap();
    server
        .cache
        .as_ref()
        .unwrap()
        .insert(
            cache_key_for_cursor(id, &INTRINSIC_CURSOR),
            CachedBytes::new(serde_json::to_vec(&stale).unwrap()),
        )
        .await;
    write_cursor(server.preferred_storage().unwrap(), id, position).await;
    cursors
}

#[tokio::test]
async fn refreshes_pressured_rollup_before_batch_wait_expires() {
    let (_temp, storage) = chroma_storage::test_storage();
    let mut server = server_with_storage(storage);
    server.config.reinsert_threshold = 180;
    let id = CollectionUuid::new();
    cache_old_cursor_and_commit(&server, id, 11).await;
    let mut rollup = RollupPerCollection::new(LogPosition::from_offset(1), 10, 0);
    rollup.reinsert_count = 1;
    let mut rollups = HashMap::from([((None, id), rollup)]);
    server.enrich_dirty_log(&mut rollups).await.unwrap();
    assert!(rollups.is_empty());
}

#[tokio::test]
async fn refreshes_small_rollup_independently_of_batch_wait() {
    let (_temp, storage) = chroma_storage::test_storage();
    let mut server = server_with_storage(storage);
    server.config.reinsert_threshold = 180;
    let id = CollectionUuid::new();
    cache_old_cursor_and_commit(&server, id, 6).await;
    let mut rollup = RollupPerCollection::new(LogPosition::from_offset(1), 5, 0);
    rollup.reinsert_count = 5;
    let mut rollups = HashMap::from([((None, id), rollup)]);
    server.enrich_dirty_log(&mut rollups).await.unwrap();
    assert_eq!(rollups[&(None, id)].start_log_position.offset(), 1);
    rollups.get_mut(&(None, id)).unwrap().reinsert_count = 6;
    server.enrich_dirty_log(&mut rollups).await.unwrap();
    assert!(rollups.is_empty());
}

#[tokio::test]
async fn stale_pressure_allows_write_after_durable_progress() {
    let (_temp, storage) = chroma_storage::test_storage();
    let server = server_with_storage(storage);
    let id = CollectionUuid::new();
    cache_old_cursor_and_commit(&server, id, 11).await;
    server.need_to_compact_s3.lock().insert(
        id,
        RollupPerCollection::new(LogPosition::from_offset(1), 10, 0),
    );
    server.set_backpressure(&[id]);
    server.check_for_backpressure(id).await.unwrap();
    // Reuse the fresh result, but still account for writes that extend the pending range.
    server
        .need_to_compact_s3
        .lock()
        .get_mut(&id)
        .unwrap()
        .limit_log_position = LogPosition::from_offset(21);
    let err = server.check_for_backpressure(id).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert_eq!(err.metadata().get("backoff-reason").unwrap(), "compaction");
}

#[tokio::test]
async fn missing_cursor_and_replication_pressure_still_reject_writes() {
    let (_temp, storage) = chroma_storage::test_storage();
    let server = server_with_storage(storage);
    let id = CollectionUuid::new();
    let rollup = RollupPerCollection::new(LogPosition::from_offset(1), 10, 0);
    server.need_to_compact_s3.lock().insert(id, rollup);
    server.set_backpressure(&[id]);
    assert_eq!(
        server.check_for_backpressure(id).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    cache_old_cursor_and_commit(&server, id, 11).await;
    server
        .cache
        .as_ref()
        .unwrap()
        .remove(&cache_key_for_verified_cursor(id))
        .await;
    server
        .need_to_compact_repl
        .lock()
        .insert((TopologyName::new("repl").unwrap(), id), rollup);
    assert_eq!(
        server.check_for_backpressure(id).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
}

#[tokio::test]
async fn expired_verification_reads_new_committed_position() {
    let (_temp, storage) = chroma_storage::test_storage();
    let server = server_with_storage(storage);
    let id = CollectionUuid::new();
    let cursors = cache_old_cursor_and_commit(&server, id, 1).await;
    server.need_to_compact_s3.lock().insert(
        id,
        RollupPerCollection::new(LogPosition::from_offset(1), 10, 0),
    );
    server.set_backpressure(&[id]);
    assert!(server.check_for_backpressure(id).await.is_err());
    let before = cursors.load(&INTRINSIC_CURSOR).await.unwrap().unwrap();
    write_cursor(server.preferred_storage().unwrap(), id, 11).await;
    let expired = crate::cursor_refresh::VerifiedCursor {
        checked_at: SystemTime::now() - Duration::from_secs(2),
        witness: Some(before),
    };
    server
        .cache
        .as_ref()
        .unwrap()
        .insert(
            cache_key_for_verified_cursor(id),
            CachedBytes::new(serde_json::to_vec(&expired).unwrap()),
        )
        .await;
    server.check_for_backpressure(id).await.unwrap();
}

#[tokio::test]
async fn corrupt_durable_cursor_does_not_admit_write() {
    let (_temp, storage) = chroma_storage::test_storage();
    let server = server_with_storage(storage.clone());
    let id = CollectionUuid::new();
    storage
        .put_bytes(
            &format!(
                "{}/{}",
                id.storage_prefix_for_log(),
                INTRINSIC_CURSOR.path()
            ),
            b"invalid cursor".to_vec(),
            chroma_storage::PutOptions::default(),
        )
        .await
        .unwrap();
    server.need_to_compact_s3.lock().insert(
        id,
        RollupPerCollection::new(LogPosition::from_offset(1), 10, 0),
    );
    server.set_backpressure(&[id]);
    assert_eq!(
        server.check_for_backpressure(id).await.unwrap_err().code(),
        tonic::Code::Unavailable
    );
}

// Local storage supplies read ETags but does not implement conditional writes.
// These sequential fixtures write cursor files directly; production loads use CursorStore.
async fn write_cursor(storage: &Storage, id: CollectionUuid, position: u64) {
    let cursor = Cursor {
        position: LogPosition::from_offset(position),
        ..Default::default()
    };
    storage
        .put_bytes(
            &format!(
                "{}/{}",
                id.storage_prefix_for_log(),
                INTRINSIC_CURSOR.path()
            ),
            serde_json::to_vec(&cursor).unwrap(),
            chroma_storage::PutOptions::default(),
        )
        .await
        .unwrap();
}
