use chroma_types::chroma_proto::log_service_client::LogServiceClient;
use chroma_types::chroma_proto::PushLogsRequest;
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
        .unwrap_err();
    assert!(resp
        .to_string()
        .contains("Go log service doesn't support PushLogs"));
}
