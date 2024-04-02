use std::sync::Arc;

use crate::{
    execution::data::data_chunk::DataChunk,
    types::{LogRecord, Operation, OperationRecord},
};

/// A MaterializedLogRecord is a LogRecord that, where appropriate, has been reconciled
/// with the state of the system. For example, an update operation may partially mutate
/// the metadata of a record, which a LogRecord models with the `UpdateMetdata` type.
/// A MaterializedLogRecord would have the metadata fully reconciled in its materialized_metadata
/// field.
pub(super) struct MaterializedLogRecord<'a> {
    segment_offset_id: Option<u32>, // If the record is new, this is the offset id assigned to it
    record: &'a LogRecord,
    old_embedding: Option<Vec<f32>>,
    new_metadata: Option<crate::types::Metadata>,
    old_metadata: Option<crate::types::Metadata>,
    old_document: Option<String>,
}

// In order to update full text search we need to know the old document so we can remove it

pub(super) trait SegmentWriter {
    fn begin_transaction(&mut self);
    fn write_records(&mut self, records: Vec<Box<LogRecord>>, offset_ids: Vec<Option<u32>>);
    fn commit_transaction(&mut self);
    fn rollback_transaction(&mut self);
}

pub(super) trait OffsetIdAssigner: SegmentWriter {
    fn assign_offset_ids(&mut self, records: DataChunk) -> Vec<Option<u32>>;
}

pub(super) async fn data_chunk() {
    let data = vec![
        LogRecord {
            log_offset: 1,
            record: OperationRecord {
                id: "embedding_id_1".to_string(),
                embedding: None,
                encoding: None,
                metadata: None,
                operation: Operation::Add,
            },
        },
        LogRecord {
            log_offset: 2,
            record: OperationRecord {
                id: "embedding_id_2".to_string(),
                embedding: None,
                encoding: None,
                metadata: None,
                operation: Operation::Add,
            },
        },
    ];
    let arc_vec: Arc<[LogRecord]> = Arc::from(data.as_slice());
    // See if we can reference the data
    let mut stored_record_vec = Vec::new();
    for record in arc_vec.iter() {
        stored_record_vec.push(MaterializedLogRecord {
            segment_offset_id: 0,
            record: &record,
            new_embedding: None,
            new_metadata: None,
            new_document: None,
        });
    }
    for stored_record in stored_record_vec.iter() {
        println!("Record: {:?}", stored_record.record);
    }
    let arc_stored: Arc<[MaterializedLogRecord]> = stored_record_vec.into();
    test_store_fn(arc_stored).await;
}

pub(super) async fn test_store_fn(arc_stored: Arc<[MaterializedLogRecord<'_>]>) {
    for stored_record in arc_stored.iter() {
        println!("Record: {:?}", stored_record.record);
    }

    let channel = tokio::sync::mpsc::channel(1);
    let (mut tx, mut rx) = channel;
    tx.send(arc_stored).await.unwrap();
}
