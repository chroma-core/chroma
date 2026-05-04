use crate::work_queue::types::{WorkQueueError, WorkQueueRecord};
use arrow::array::{Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct QueueState {
    // FIFO queue - VecDeque maintains insertion order
    pub pending_work: VecDeque<WorkQueueRecord>,
    // Deduplication index: (fn_id, input_coll_id) -> completion_offset
    pub dedup_index: HashMap<(AttachedFunctionUuid, CollectionUuid), i64>,
    // Current ETag from storage
    pub current_etag: Option<String>,
    // Monotonic counter for FIFO ordering
    pub next_insertion_order: u64,
    // Persistence tracking
    pub dirty: bool,
}

impl QueueState {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            pending_work: VecDeque::new(),
            dedup_index: HashMap::new(),
            current_etag: None,
            next_insertion_order: 0,
            dirty: false,
        }
    }

    /// Serialize state to Parquet bytes
    #[allow(dead_code)]
    pub fn to_parquet_bytes(&self) -> Result<Bytes, WorkQueueError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("fn_id", DataType::Utf8, false),
            Field::new("input_coll_id", DataType::Utf8, false),
            Field::new("completion_offset", DataType::Int64, false),
            Field::new("insertion_order", DataType::UInt64, false),
        ]));

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), None)
            .map_err(|e| WorkQueueError::Serialization(e.to_string()))?;

        if !self.pending_work.is_empty() {
            let fn_ids: Vec<_> = self
                .pending_work
                .iter()
                .map(|r| r.fn_id.to_string())
                .collect();
            let coll_ids: Vec<_> = self
                .pending_work
                .iter()
                .map(|r| r.input_coll_id.to_string())
                .collect();
            let offsets: Vec<_> = self
                .pending_work
                .iter()
                .map(|r| r.completion_offset)
                .collect();
            let orders: Vec<_> = self
                .pending_work
                .iter()
                .map(|r| r.insertion_order)
                .collect();

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(fn_ids)),
                    Arc::new(StringArray::from(coll_ids)),
                    Arc::new(Int64Array::from(offsets)),
                    Arc::new(UInt64Array::from(orders)),
                ],
            )
            .map_err(|e| WorkQueueError::Serialization(e.to_string()))?;

            writer
                .write(&batch)
                .map_err(|e| WorkQueueError::Serialization(e.to_string()))?;
        }

        writer
            .close()
            .map_err(|e| WorkQueueError::Serialization(e.to_string()))?;

        Ok(Bytes::from(buffer))
    }

    #[allow(dead_code)]
    pub fn from_parquet_bytes(bytes: &[u8]) -> Result<Self, WorkQueueError> {
        let bytes = Bytes::copy_from_slice(bytes);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| WorkQueueError::Serialization(e.to_string()))?;

        let reader = builder
            .build()
            .map_err(|e| WorkQueueError::Serialization(e.to_string()))?;

        let mut state = QueueState::new();

        for batch_result in reader {
            let batch = batch_result.map_err(|e| WorkQueueError::Serialization(e.to_string()))?;

            // Validate schema and look up columns by name
            let schema = batch.schema();
            let fn_ids_idx = schema
                .column_with_name("fn_id")
                .ok_or_else(|| {
                    WorkQueueError::Serialization("Missing required field: fn_id".to_string())
                })?
                .0;
            let coll_ids_idx = schema
                .column_with_name("input_coll_id")
                .ok_or_else(|| {
                    WorkQueueError::Serialization(
                        "Missing required field: input_coll_id".to_string(),
                    )
                })?
                .0;
            let offsets_idx = schema
                .column_with_name("completion_offset")
                .ok_or_else(|| {
                    WorkQueueError::Serialization(
                        "Missing required field: completion_offset".to_string(),
                    )
                })?
                .0;
            let orders_idx = schema
                .column_with_name("insertion_order")
                .ok_or_else(|| {
                    WorkQueueError::Serialization(
                        "Missing required field: insertion_order".to_string(),
                    )
                })?
                .0;

            let fn_ids = batch
                .column(fn_ids_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    WorkQueueError::Serialization("Failed to downcast fn_ids".to_string())
                })?;
            let coll_ids = batch
                .column(coll_ids_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    WorkQueueError::Serialization("Failed to downcast coll_ids".to_string())
                })?;
            let offsets = batch
                .column(offsets_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    WorkQueueError::Serialization("Failed to downcast offsets".to_string())
                })?;
            let orders = batch
                .column(orders_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    WorkQueueError::Serialization("Failed to downcast orders".to_string())
                })?;

            for i in 0..batch.num_rows() {
                let fn_id = AttachedFunctionUuid::from_str(fn_ids.value(i))
                    .map_err(|e| WorkQueueError::Serialization(format!("Invalid fn_id: {}", e)))?;
                let input_coll_id = CollectionUuid::from_str(coll_ids.value(i)).map_err(|e| {
                    WorkQueueError::Serialization(format!("Invalid collection_id: {}", e))
                })?;

                let record = WorkQueueRecord {
                    fn_id,
                    input_coll_id,
                    completion_offset: offsets.value(i),
                    insertion_order: orders.value(i),
                };

                let key = (fn_id, input_coll_id);
                // Detect duplicate (fn_id, input_coll_id) pairs and log warning
                if state.dedup_index.contains_key(&key) {
                    tracing::error!(
                        key = ?key,
                        "Duplicate (fn_id, input_coll_id) pair found in Parquet file - overwriting previous entry"
                    );
                }
                state.dedup_index.insert(key, record.completion_offset);
                state.pending_work.push_back(record);
            }
        }

        // Sort by insertion_order to maintain FIFO
        let mut sorted: Vec<_> = state.pending_work.drain(..).collect();
        sorted.sort_by_key(|r| r.insertion_order);
        state.pending_work = VecDeque::from(sorted);

        // Set next_insertion_order
        state.next_insertion_order = state
            .pending_work
            .back()
            .map(|r| r.insertion_order + 1)
            .unwrap_or(0);

        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_queue_state_serialization() {
        let mut state = QueueState::new();

        let record1 = WorkQueueRecord {
            fn_id: AttachedFunctionUuid(Uuid::new_v4()),
            input_coll_id: CollectionUuid(Uuid::new_v4()),
            completion_offset: 100,
            insertion_order: 0,
        };

        let record2 = WorkQueueRecord {
            fn_id: AttachedFunctionUuid(Uuid::new_v4()),
            input_coll_id: CollectionUuid(Uuid::new_v4()),
            completion_offset: 200,
            insertion_order: 1,
        };

        let record3 = WorkQueueRecord {
            fn_id: AttachedFunctionUuid(Uuid::new_v4()),
            input_coll_id: CollectionUuid(Uuid::new_v4()),
            completion_offset: 300,
            insertion_order: 2,
        };

        state.pending_work.push_back(record1.clone());
        state
            .dedup_index
            .insert((record1.fn_id, record1.input_coll_id), 100);

        state.pending_work.push_back(record2.clone());
        state
            .dedup_index
            .insert((record2.fn_id, record2.input_coll_id), 200);

        state.pending_work.push_back(record3.clone());
        state
            .dedup_index
            .insert((record3.fn_id, record3.input_coll_id), 300);

        let bytes = state.to_parquet_bytes().expect("Failed to serialize");
        let restored = QueueState::from_parquet_bytes(&bytes).expect("Failed to deserialize");

        assert_eq!(restored.pending_work.len(), 3);
        assert_eq!(restored.pending_work[0].completion_offset, 100);
        assert_eq!(restored.pending_work[1].completion_offset, 200);
        assert_eq!(restored.pending_work[2].completion_offset, 300);
        assert_eq!(restored.dedup_index.len(), 3);
    }
}
