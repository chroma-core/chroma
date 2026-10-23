use super::*;
use arrow::array::{ArrayRef, BinaryArray, RecordBatch, UInt64Array};
use parquet::arrow::ArrowWriter;
use prost::Message;

/// Exercises the real pull handler with a warm fragment cache. No S3, Kubernetes,
/// fake delays, or mocked decoding: only the storage backend is local.
#[tokio::test]
#[ignore = "local RLS incident replay benchmark"]
async fn test_incident_repeated_tail_pulls() {
    if let Ok(path) = std::env::var("INCIDENT_CONFIG_PATH") {
        use figment::providers::{Format, Yaml};
        let config: worker::config::RootConfig =
            figment::Figment::from(Yaml::file(path)).extract().unwrap();
        assert!(!config.query_service.use_fragment_fetch);
        assert_eq!(
            config.query_service.collections_for_fragment_fetch,
            [
                "5a6c62ba-a4ec-4b49-bf4d-f517a7648bce",
                "6a298bb2-b5a7-401d-aae6-a75e937bb4d5",
            ]
        );
        assert!(config.query_service.fragment_storage.is_some());
        assert!(matches!(
            config.query_service.fragment_fetcher_cache,
            chroma_cache::CacheConfig::Memory(_)
        ));
    }
    let (_dir, storage) = chroma_storage::test_storage();
    let mut server = log_server_for_installed_writer_tests();
    let region = RegionName::new("local").unwrap();
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
    server.cache = Some(Arc::from(chroma_cache::new_cache_for_test()));
    let collection = CollectionUuid::new();
    let count = 8192u64;
    let messages = (0..count)
        .map(|i| {
            let mut record = proto_operation_record(format!("record-{i}"));
            record.vector = Some(chroma_types::chroma_proto::Vector {
                vector: vec![0; 4096],
                encoding: 0,
                dimension: 1024,
            });
            record.encode_to_vec()
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "offset",
            Arc::new(UInt64Array::from_iter_values(1..=count)) as ArrayRef,
        ),
        (
            "timestamp_us",
            Arc::new(UInt64Array::from(vec![1u64; count as usize])) as ArrayRef,
        ),
        (
            "body",
            Arc::new(BinaryArray::from_iter_values(
                messages.iter().map(Vec::as_slice),
            )) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut bytes = Vec::new();
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let fragment = wal3::Fragment {
        path: "fragment/fixture.parquet".to_string(),
        seq_no: wal3::FragmentIdentifier::SeqNo(FragmentSeqNo::BEGIN),
        start: LogPosition::from_offset(1),
        limit: LogPosition::from_offset(count + 1),
        num_bytes: bytes.len() as u64,
        setsum: wal3::Setsum::default(),
    };
    let mut manifest = wal3::Manifest::new_empty("incident-replay");
    manifest.acc_bytes = bytes.len() as u64;
    manifest.fragments.push(fragment.clone());
    storage
        .put_bytes(
            &format!("{}/manifest/MANIFEST", collection.storage_prefix_for_log()),
            serde_json::to_vec(&manifest).unwrap(),
            Default::default(),
        )
        .await
        .unwrap();
    let cache = server.cache.as_ref().unwrap();
    cache
        .insert(
            cache_key_for_manifest_and_etag(collection),
            CachedBytes::new(
                serde_json::to_vec(&wal3::ManifestAndWitness {
                    manifest,
                    witness: wal3::interfaces::ManifestWitness::ETag(chroma_storage::ETag(
                        "fixture".into(),
                    )),
                })
                .unwrap(),
            ),
        )
        .await;
    storage
        .put_bytes(
            &wal3::fragment_path(&collection.storage_prefix_for_log(), &fragment.path),
            bytes.clone(),
            Default::default(),
        )
        .await
        .unwrap();
    cache
        .insert(
            cache_key_for_fragment(collection, &fragment.path),
            CachedBytes::new(bytes),
        )
        .await;

    for compacted in [0, 0, 0, count] {
        let start = std::time::Instant::now();
        let tail = get_enum_offset_on_server(&server, "dbname", collection).await as u64;
        assert_eq!(tail, count);
        let mut requests = 0;
        let mut returned = 0;
        let mut wire_bytes = 0;
        for offset in ((compacted + 1)..=tail).step_by(625) {
            let batch_size = (tail + 1 - offset).min(625) as i32;
            let response = server
                .pull_logs(Request::new(PullLogsRequest {
                    collection_id: collection.to_string(),
                    start_from_offset: offset as i64,
                    batch_size,
                    end_timestamp: 0,
                    database_name: "dbname".to_string(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.records.len(), batch_size as usize);
            for (i, record) in response.records.iter().enumerate() {
                assert_eq!(record.log_offset, offset as i64 + i as i64);
                assert_eq!(
                    record.record.as_ref().unwrap().id,
                    format!("record-{}", offset + i as u64 - 1)
                );
            }
            wire_bytes += response.encoded_len();
            returned += response.records.len();
            requests += 1;
        }
        assert_eq!(returned as u64, count - compacted);
        eprintln!("rls replay: compacted={compacted} tail={tail} requests={requests} records={returned} wire_bytes={wire_bytes} elapsed={:?}", start.elapsed());
    }

    // Exercise the existing worker implementation, not a substitute decoder.
    use worker::execution::operators::fragment_fetch::{FragmentFetcher, FragmentPointer};
    let cache_config = figment::Figment::from(figment::providers::Serialized::defaults(
        serde_json::json!({"memory": {"capacity": 67108864}}),
    ))
    .extract()
    .unwrap();
    let fetcher = Arc::new(FragmentFetcher::new(storage, &cache_config).await.unwrap());
    for compacted in [0, 0, 0, count] {
        let start = std::time::Instant::now();
        let response = server
            .scout_log_fragments(Request::new(
                chroma_types::chroma_proto::ScoutLogFragmentsRequest {
                    collection_id: collection.to_string(),
                    database_name: "dbname".to_string(),
                    start_from_offset: compacted + 1,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        let rls_wire_bytes = response.encoded_len();
        let upper = response.first_uninserted_record_offset;
        let pointers = response
            .fragments
            .into_iter()
            .map(FragmentPointer::from)
            .collect::<Vec<_>>();
        let records = fetcher
            .fetch_records(&pointers, compacted + 1, upper, 10)
            .await
            .unwrap();
        assert_eq!(records.len() as u64, count - compacted);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.log_offset, compacted as i64 + i as i64 + 1);
            assert_eq!(record.record.id, format!("record-{}", compacted + i as u64));
            assert_eq!(
                record.record.embedding.as_ref().unwrap(),
                &vec![0.0f32; 1024]
            );
        }
        eprintln!("direct replay: compacted={compacted} fragments={} records={} rls_wire_bytes={rls_wire_bytes} elapsed={:?}", pointers.len(), records.len(), start.elapsed());
    }
}
