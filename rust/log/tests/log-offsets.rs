use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::{
    GetAllCollectionInfoToCompactRequest, MigrateLogRequest, PullLogsRequest, PushLogsRequest,
    ScoutLogsRequest, SealLogRequest, UpdateCollectionLogOffsetRequest,
};
use chroma_types::{Operation, OperationRecord};
use tonic::transport::Channel;
use uuid::Uuid;

#[tokio::test]
async fn test_k8s_integration_log_offsets_empty_log_50052() {
    const ADDRESS: &str = "http://localhost:50052";
    let collection_id = Uuid::new_v4().to_string();
    eprintln!("processing {collection_id} on {ADDRESS}");
    let logservice_channel = Channel::from_static(ADDRESS).connect().await.unwrap();
    let mut log_service = LogServiceClient::new(logservice_channel);
    // Insert one record.
    let records = vec![OperationRecord {
        id: "some-record".to_string(),
        embedding: Some(vec![0.0, 0.0, 0.0]),
        document: Some("some-document".to_string()),
        encoding: None,
        metadata: None,
        operation: Operation::Add,
    }];
    let resp = log_service
        .push_logs(PushLogsRequest {
            collection_id: collection_id.clone(),
            records: records
                .into_iter()
                .map(|r| r.try_into())
                .collect::<Result<Vec<chroma_types::chroma_proto::OperationRecord>, _>>()
                .unwrap(),
        })
        .await
        .unwrap();
    let resp = resp.into_inner();
    assert!(!resp.log_is_sealed);
    assert_eq!(1, resp.record_count);
    // Scout said record.
    let resp = log_service
        .scout_logs(ScoutLogsRequest {
            collection_id: collection_id.clone(),
        })
        .await
        .unwrap();
    let resp = resp.into_inner();
    assert_eq!(1, resp.first_uncompacted_record_offset);
    assert_eq!(2, resp.first_uninserted_record_offset);
    // Pull said record.
    let resp = log_service
        .pull_logs(PullLogsRequest {
            collection_id: collection_id.clone(),
            batch_size: 2,
            end_timestamp: i64::MAX,
            start_from_offset: 1,
        })
        .await
        .unwrap();
    let resp = resp.into_inner();
    assert_eq!(1, resp.records.len());
    assert_eq!(1, resp.records[0].log_offset);
    // "compact" said record.
    let resp = log_service
        .get_all_collection_info_to_compact(GetAllCollectionInfoToCompactRequest {
            min_compaction_size: 1,
        })
        .await
        .unwrap();
    let resp = resp.into_inner();
    let Some(coll) = resp
        .all_collection_info
        .iter()
        .find(|c| c.collection_id == collection_id)
    else {
        panic!("collection not found");
    };
    assert_eq!(1, coll.first_log_offset);
    // "finish" the compaction.
    let _resp = log_service
        .update_collection_log_offset(UpdateCollectionLogOffsetRequest {
            collection_id: collection_id.clone(),
            log_offset: 1,
        })
        .await
        .unwrap();
    // said record no longer shows in compaction.
    let resp = log_service
        .get_all_collection_info_to_compact(GetAllCollectionInfoToCompactRequest {
            min_compaction_size: 1,
        })
        .await
        .unwrap();
    let resp = resp.into_inner();
    let coll = resp
        .all_collection_info
        .iter()
        .find(|c| c.collection_id == collection_id);
    assert!(coll.is_none());
}

#[tokio::test]
async fn test_k8s_integration_log_offsets_empty_log_50054() {
    const ADDRESS1: &str = "http://localhost:50052";
    const ADDRESS2: &str = "http://localhost:50054";
    let collection_id = Uuid::new_v4().to_string();
    eprintln!("processing {collection_id} on {ADDRESS1} (go) and {ADDRESS2} (rust)");
    let go_logservice_channel = Channel::from_static(ADDRESS1).connect().await.unwrap();
    let mut go_log_service = LogServiceClient::new(go_logservice_channel);
    let rust_logservice_channel = Channel::from_static(ADDRESS2).connect().await.unwrap();
    let mut rust_log_service = LogServiceClient::new(rust_logservice_channel);
    // Insert one record.
    let records = vec![OperationRecord {
        id: "some-record".to_string(),
        embedding: Some(vec![0.0, 0.0, 0.0]),
        document: Some("some-document".to_string()),
        encoding: None,
        metadata: None,
        operation: Operation::Add,
    }];
    let resp = rust_log_service
        .push_logs(PushLogsRequest {
            collection_id: collection_id.clone(),
            records: records
                .into_iter()
                .map(|r| r.try_into())
                .collect::<Result<Vec<chroma_types::chroma_proto::OperationRecord>, _>>()
                .unwrap(),
        })
        .await
        .unwrap();
    let resp = resp.into_inner();
    assert!(!resp.log_is_sealed);
    assert_eq!(1, resp.record_count);
    // Seal on the go log.  Do this after there's a record so the log is initialized in log db.
    let _resp = go_log_service
        .seal_log(SealLogRequest {
            collection_id: collection_id.clone(),
        })
        .await
        .unwrap();
    // Migrate to the rust log
    let _resp = rust_log_service
        .migrate_log(MigrateLogRequest {
            collection_id: collection_id.clone(),
        })
        .await
        .unwrap();
    // Scout said record.
    let resp = rust_log_service
        .scout_logs(ScoutLogsRequest {
            collection_id: collection_id.clone(),
        })
        .await
        .unwrap();
    let resp = resp.into_inner();
    assert_eq!(1, resp.first_uncompacted_record_offset);
    assert_eq!(2, resp.first_uninserted_record_offset);
    // Pull said record.
    let resp = rust_log_service
        .pull_logs(PullLogsRequest {
            collection_id: collection_id.clone(),
            batch_size: 2,
            end_timestamp: i64::MAX,
            start_from_offset: 1,
        })
        .await
        .unwrap();
    let resp = resp.into_inner();
    assert_eq!(1, resp.records.len());
    assert_eq!(1, resp.records[0].log_offset);
    // "compact" said record.
    let resp = rust_log_service
        .get_all_collection_info_to_compact(GetAllCollectionInfoToCompactRequest {
            min_compaction_size: 1,
        })
        .await
        .unwrap();
    let resp = resp.into_inner();
    let Some(coll) = resp
        .all_collection_info
        .iter()
        .find(|c| c.collection_id == collection_id)
    else {
        panic!("collection not found");
    };
    assert_eq!(1, coll.first_log_offset);
    // "finish" the compaction.
    let _resp = rust_log_service
        .update_collection_log_offset(UpdateCollectionLogOffsetRequest {
            collection_id: collection_id.clone(),
            log_offset: 1,
        })
        .await
        .unwrap();
    // said record no longer shows in compaction.
    let resp = rust_log_service
        .get_all_collection_info_to_compact(GetAllCollectionInfoToCompactRequest {
            min_compaction_size: 1,
        })
        .await
        .unwrap();
    let resp = resp.into_inner();
    let coll = resp
        .all_collection_info
        .iter()
        .find(|c| c.collection_id == collection_id);
    assert!(coll.is_none());
}
