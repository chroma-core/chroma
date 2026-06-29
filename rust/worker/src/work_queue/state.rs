use crate::work_queue::types::{WorkQueueError, WorkQueueRecord};
use arrow::array::{Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;

use chroma_storage::ETag;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueueOffsets {
    completion_offset: i64,
    compaction_offset: Option<i64>,
}

impl QueueOffsets {
    fn dedup_frontier(self) -> i64 {
        self.compaction_offset.unwrap_or(self.completion_offset)
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct QueueState {
    // FIFO queue - VecDeque maintains insertion order
    pub pending_work: VecDeque<WorkQueueRecord>,
    // Deduplication index: (fn_id, input_coll_id) -> stored offsets
    dedup_index: HashMap<(AttachedFunctionUuid, CollectionUuid), QueueOffsets>,
    // Current ETag from storage
    pub current_etag: Option<ETag>,
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
            Field::new("compaction_offset", DataType::Int64, true),
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
            let compaction_offsets: Vec<_> = self
                .pending_work
                .iter()
                .map(|r| r.compaction_offset)
                .collect();

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(fn_ids)),
                    Arc::new(StringArray::from(coll_ids)),
                    Arc::new(Int64Array::from(offsets)),
                    Arc::new(Int64Array::from(compaction_offsets)),
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
            let compaction_offsets_idx =
                schema.column_with_name("compaction_offset").map(|(idx, _)| idx);

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
            let compaction_offsets = match compaction_offsets_idx {
                Some(idx) => Some(
                    batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            WorkQueueError::Serialization(
                                "Failed to downcast compaction offsets".to_string(),
                            )
                        })?,
                ),
                None => None,
            };

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
                    compaction_offset: compaction_offsets.and_then(|offsets| {
                        offsets.is_valid(i).then_some(offsets.value(i))
                    }),
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
                state.dedup_index.insert(
                    key,
                    QueueOffsets {
                        completion_offset: record.completion_offset,
                        compaction_offset: record.compaction_offset,
                    },
                );
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

    pub fn push_work(
        &mut self,
        fn_id: AttachedFunctionUuid,
        input_coll_id: CollectionUuid,
        completion_offset: i64,
        compaction_offset: Option<i64>,
    ) -> bool {
        let key = (fn_id, input_coll_id);

        let new_offsets = QueueOffsets {
            completion_offset,
            compaction_offset,
        };

        if let Some(&existing_offsets) = self.dedup_index.get(&key) {
            if new_offsets.dedup_frontier() <= existing_offsets.dedup_frontier() {
                return false;
            }
        }

        self.pending_work
            .retain(|r| !(r.fn_id == fn_id && r.input_coll_id == input_coll_id));

        let record = WorkQueueRecord {
            fn_id,
            input_coll_id,
            completion_offset,
            compaction_offset,
            insertion_order: self.next_insertion_order,
        };

        self.next_insertion_order += 1;
        self.dedup_index.insert(key, new_offsets);
        self.pending_work.push_back(record);
        self.dirty = true;

        true
    }

    /// Mark work as successfully completed
    /// Returns true if any work was actually removed
    pub fn finish_work_success(
        &mut self,
        fn_id: &AttachedFunctionUuid,
        input_coll_id: &CollectionUuid,
        completion_offset: i64,
    ) -> bool {
        let key = (*fn_id, *input_coll_id);

        // Check if there's an entry to remove
        if let Some(&existing_offsets) = self.dedup_index.get(&key) {
            if existing_offsets.completion_offset <= completion_offset {
                // Remove the single entry for this key
                self.pending_work
                    .retain(|r| !(r.fn_id == *fn_id && r.input_coll_id == *input_coll_id));

                // Remove from dedup index
                self.dedup_index.remove(&key);
                self.dirty = true;
                return true;
            }
        }

        false
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
            compaction_offset: Some(140),
            insertion_order: 0,
        };

        let record2 = WorkQueueRecord {
            fn_id: AttachedFunctionUuid(Uuid::new_v4()),
            input_coll_id: CollectionUuid(Uuid::new_v4()),
            completion_offset: 200,
            compaction_offset: None,
            insertion_order: 1,
        };

        let record3 = WorkQueueRecord {
            fn_id: AttachedFunctionUuid(Uuid::new_v4()),
            input_coll_id: CollectionUuid(Uuid::new_v4()),
            completion_offset: 300,
            compaction_offset: Some(360),
            insertion_order: 2,
        };

        state.pending_work.push_back(record1.clone());
        state.dedup_index.insert(
            (record1.fn_id, record1.input_coll_id),
            QueueOffsets {
                completion_offset: 100,
                compaction_offset: record1.compaction_offset,
            },
        );

        state.pending_work.push_back(record2.clone());
        state.dedup_index.insert(
            (record2.fn_id, record2.input_coll_id),
            QueueOffsets {
                completion_offset: 200,
                compaction_offset: record2.compaction_offset,
            },
        );

        state.pending_work.push_back(record3.clone());
        state.dedup_index.insert(
            (record3.fn_id, record3.input_coll_id),
            QueueOffsets {
                completion_offset: 300,
                compaction_offset: record3.compaction_offset,
            },
        );

        let bytes = state.to_parquet_bytes().expect("Failed to serialize");
        let restored = QueueState::from_parquet_bytes(&bytes).expect("Failed to deserialize");

        assert_eq!(restored.pending_work.len(), 3);
        assert_eq!(restored.pending_work[0].completion_offset, 100);
        assert_eq!(restored.pending_work[0].compaction_offset, Some(140));
        assert_eq!(restored.pending_work[1].completion_offset, 200);
        assert_eq!(restored.pending_work[1].compaction_offset, None);
        assert_eq!(restored.pending_work[2].completion_offset, 300);
        assert_eq!(restored.pending_work[2].compaction_offset, Some(360));
        assert_eq!(restored.dedup_index.len(), 3);
    }

    #[test]
    fn test_queue_state_backwards_compatible_without_compaction_offset() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("fn_id", DataType::Utf8, false),
            Field::new("input_coll_id", DataType::Utf8, false),
            Field::new("completion_offset", DataType::Int64, false),
            Field::new("insertion_order", DataType::UInt64, false),
        ]));

        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![fn_id.to_string()])),
                Arc::new(StringArray::from(vec![coll_id.to_string()])),
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(UInt64Array::from(vec![0])),
            ],
        )
        .expect("Failed to create legacy batch");

        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, schema, None).expect("Failed to create writer");
        writer.write(&batch).expect("Failed to write legacy batch");
        writer.close().expect("Failed to close legacy writer");

        let restored =
            QueueState::from_parquet_bytes(&buffer).expect("Failed to deserialize legacy bytes");

        assert_eq!(restored.pending_work.len(), 1);
        assert_eq!(restored.pending_work[0].completion_offset, 100);
        assert_eq!(restored.pending_work[0].compaction_offset, None);
    }

    #[test]
    fn test_push_work_prefers_highest_compaction_offset() {
        let mut state = QueueState::new();

        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        state.push_work(fn_id, coll_id, 20, Some(40));
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 20);
        assert_eq!(state.pending_work[0].compaction_offset, Some(40));

        state.push_work(fn_id, coll_id, 20, Some(60));
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 20);
        assert_eq!(state.pending_work[0].compaction_offset, Some(60));
    }
}
