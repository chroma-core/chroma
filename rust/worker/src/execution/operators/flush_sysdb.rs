use crate::execution::operator::Operator;
use crate::sysdb::sysdb::FlushCompactionError;
use crate::sysdb::sysdb::SysDb;
use crate::types::FlushCompactionResponse;
use crate::types::SegmentFlushInfo;
use async_trait::async_trait;
use std::sync::Arc;

/// The flush sysdb operator is responsible for flushing compaction data to the sysdb.
#[derive(Debug)]
pub struct FlushSysDbOperator {}

impl FlushSysDbOperator {
    /// Create a new flush sysdb operator.
    pub fn new() -> Box<Self> {
        Box::new(FlushSysDbOperator {})
    }
}

#[derive(Debug)]
/// The input for the flush sysdb operator.
/// This input is used to flush compaction data to the sysdb.
/// # Parameters
/// * `tenant` - The tenant id.
/// * `collection_id` - The collection id.
/// * `log_position` - The log position. Note that this is the log position for the last record that
/// was flushed to S3.
/// * `collection_version` - The collection version. This is the current collection version before
/// the flush operation. This version will be incremented by 1 after the flush operation. If the
/// collection version in sysdb is not the same as the current collection version, the flush operation
/// will fail.
/// * `segment_flush_info` - The segment flush info.
pub struct FlushSysDbInput {
    tenant: String,
    collection_id: String,
    log_position: i64,
    collection_version: i32,
    segment_flush_info: Arc<[SegmentFlushInfo]>,
    sysdb: Box<dyn SysDb>,
}

impl FlushSysDbInput {
    /// Create a new flush sysdb input.
    pub fn new(
        tenant: String,
        collection_id: String,
        log_position: i64,
        collection_version: i32,
        segment_flush_info: Arc<[SegmentFlushInfo]>,
        sysdb: Box<dyn SysDb>,
    ) -> Self {
        FlushSysDbInput {
            tenant,
            collection_id,
            log_position,
            collection_version,
            segment_flush_info,
            sysdb,
        }
    }
}

/// The output for the flush sysdb operator.
/// # Parameters
/// * `result` - The result of the flush compaction operation.
#[derive(Debug)]
pub struct FlushSysDbOutput {
    result: FlushCompactionResponse,
}

pub type FlushSysDbResult = Result<FlushSysDbOutput, FlushCompactionError>;

#[async_trait]
impl Operator<FlushSysDbInput, FlushSysDbOutput> for FlushSysDbOperator {
    type Error = FlushCompactionError;

    async fn run(&self, input: &FlushSysDbInput) -> FlushSysDbResult {
        let mut sysdb = input.sysdb.clone();
        let result = sysdb
            .flush_compaction(
                input.tenant.clone(),
                input.collection_id.clone(),
                input.log_position,
                input.collection_version,
                input.segment_flush_info.clone(),
            )
            .await;
        match result {
            Ok(response) => Ok(FlushSysDbOutput { result: response }),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sysdb::test_sysdb::TestSysDb;
    use crate::types::Collection;
    use crate::types::Segment;
    use crate::types::SegmentScope;
    use crate::types::SegmentType;
    use std::collections::HashMap;
    use std::str::FromStr;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_flush_sysdb_operator() {
        let mut sysdb = Box::new(TestSysDb::new());
        let collection_version = 0;
        let collection_uuid_1 = Uuid::from_str("00000000-0000-0000-0000-000000000001").unwrap();
        let tenant_1 = "tenant_1".to_string();
        let collection_1 = Collection {
            id: collection_uuid_1,
            name: "collection_1".to_string(),
            metadata: None,
            dimension: Some(1),
            tenant: tenant_1.clone(),
            database: "database_1".to_string(),
            log_position: 0,
            version: collection_version,
        };

        let collection_uuid_2 = Uuid::from_str("00000000-0000-0000-0000-000000000002").unwrap();
        let tenant_2 = "tenant_2".to_string();
        let collection_2 = Collection {
            id: collection_uuid_2,
            name: "collection_2".to_string(),
            metadata: None,
            dimension: Some(1),
            tenant: tenant_2.clone(),
            database: "database_2".to_string(),
            log_position: 0,
            version: collection_version,
        };
        sysdb.add_collection(collection_1);
        sysdb.add_collection(collection_2);

        let mut file_path_1 = HashMap::new();
        file_path_1.insert("hnsw".to_string(), vec!["path_1".to_string()]);
        let segment_id_1 = Uuid::from_str("00000000-0000-0000-0000-000000000003").unwrap();

        let segment_1 = Segment {
            id: segment_id_1.clone(),
            r#type: SegmentType::HnswDistributed,
            scope: SegmentScope::VECTOR,
            collection: Some(collection_uuid_1),
            metadata: None,
            file_path: file_path_1.clone(),
        };

        let mut file_path_2 = HashMap::new();
        file_path_2.insert("hnsw".to_string(), vec!["path_2".to_string()]);
        let segment_id_2 = Uuid::from_str("00000000-0000-0000-0000-000000000004").unwrap();
        let segment_2 = Segment {
            id: segment_id_2.clone(),
            r#type: SegmentType::HnswDistributed,
            scope: SegmentScope::VECTOR,
            collection: Some(collection_uuid_2),
            metadata: None,
            file_path: file_path_2.clone(),
        };
        sysdb.add_segment(segment_1);
        sysdb.add_segment(segment_2);

        let mut file_path_3 = HashMap::new();
        file_path_3.insert("hnsw".to_string(), vec!["path_3".to_string()]);

        let mut file_path_4 = HashMap::new();
        file_path_4.insert("hnsw".to_string(), vec!["path_4".to_string()]);
        let segment_flush_info = vec![
            SegmentFlushInfo {
                segment_id: segment_id_1.clone(),
                file_paths: file_path_3.clone(),
            },
            SegmentFlushInfo {
                segment_id: segment_id_2.clone(),
                file_paths: file_path_4.clone(),
            },
        ];

        let log_position = 100;
        let operator = FlushSysDbOperator::new();
        let input = FlushSysDbInput::new(
            tenant_1.clone(),
            collection_uuid_1.to_string(),
            log_position,
            collection_version,
            segment_flush_info.into(),
            sysdb.clone(),
        );

        let result = operator.run(&input).await;

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.result.collection_id, collection_uuid_1.to_string());
        assert_eq!(result.result.collection_version, collection_version + 1);

        let collections = sysdb
            .get_collections(Some(collection_uuid_1), None, None, None)
            .await;

        assert!(collections.is_ok());
        let collection = collections.unwrap();
        assert_eq!(collection.len(), 1);
        let collection = collection[0].clone();
        assert_eq!(collection.log_position, log_position);

        let segments = sysdb.get_segments(None, None, None, None).await;
        assert!(segments.is_ok());
        let segments = segments.unwrap();
        assert_eq!(segments.len(), 2);
        let segment_1 = segments.iter().find(|s| s.id == segment_id_1).unwrap();
        assert_eq!(segment_1.file_path, file_path_3);
        let segment_2 = segments.iter().find(|s| s.id == segment_id_2).unwrap();
        assert_eq!(segment_2.file_path, file_path_4);
    }
}
