use crate::work_queue::types::{WorkQueueError, WorkQueueRecord};
use arrow::array::{Array, Int32Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use hashlink::LinkedHashMap;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::str::FromStr;
use std::sync::Arc;

use chroma_storage::ETag;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct QueueState {
    // FIFO queue backed by a linked list and a hash index. Updating or removing
    // an entry by (fn_id, input_coll_id) is O(1), while updates preserve the
    // entry's original queue position.
    pub pending_work: LinkedHashMap<(AttachedFunctionUuid, CollectionUuid), WorkQueueRecord>,
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
            pending_work: LinkedHashMap::new(),
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
            Field::new("compaction_offset", DataType::Int64, false),
            Field::new("insertion_order", DataType::UInt64, false),
            Field::new("failure_count", DataType::Int32, false),
        ]));

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), None)
            .map_err(|e| WorkQueueError::Serialization(e.to_string()))?;

        if !self.pending_work.is_empty() {
            let fn_ids: Vec<_> = self
                .pending_work
                .iter()
                .map(|(_, r)| r.fn_id.to_string())
                .collect();
            let coll_ids: Vec<_> = self
                .pending_work
                .iter()
                .map(|(_, r)| r.input_coll_id.to_string())
                .collect();
            let orders: Vec<_> = self
                .pending_work
                .iter()
                .map(|(_, r)| r.insertion_order)
                .collect();
            let completion_offsets: Vec<_> = self
                .pending_work
                .iter()
                .map(|(_, r)| r.completion_offset)
                .collect();
            let compaction_offsets: Vec<_> = self
                .pending_work
                .iter()
                .map(|(_, r)| r.compaction_offset)
                .collect();
            let failure_counts: Vec<_> = self
                .pending_work
                .iter()
                .map(|(_, r)| r.failure_count)
                .collect();

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(fn_ids)),
                    Arc::new(StringArray::from(coll_ids)),
                    Arc::new(Int64Array::from(completion_offsets)),
                    Arc::new(Int64Array::from(compaction_offsets)),
                    Arc::new(UInt64Array::from(orders)),
                    Arc::new(Int32Array::from(failure_counts)),
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
        let mut hydrated_legacy_rows = 0usize;

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
                .map(|(idx, _)| idx);
            let orders_idx = schema
                .column_with_name("insertion_order")
                .ok_or_else(|| {
                    WorkQueueError::Serialization(
                        "Missing required field: insertion_order".to_string(),
                    )
                })?
                .0;
            let compaction_offsets_idx = schema
                .column_with_name("compaction_offset")
                .map(|(idx, _)| idx);
            let failure_counts_idx = schema.column_with_name("failure_count").map(|(idx, _)| idx);

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
            let offsets = offsets_idx
                .map(|idx| {
                    batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            WorkQueueError::Serialization("Failed to downcast offsets".to_string())
                        })
                })
                .transpose()?;
            let orders = batch
                .column(orders_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    WorkQueueError::Serialization("Failed to downcast orders".to_string())
                })?;
            let compaction_offsets = compaction_offsets_idx
                .map(|idx| {
                    batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            WorkQueueError::Serialization(
                                "Failed to downcast compaction offsets".to_string(),
                            )
                        })
                })
                .transpose()?;
            let failure_counts = failure_counts_idx
                .map(|idx| {
                    batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| {
                            WorkQueueError::Serialization(
                                "Failed to downcast failure_count".to_string(),
                            )
                        })
                })
                .transpose()?;

            for i in 0..batch.num_rows() {
                let fn_id = AttachedFunctionUuid::from_str(fn_ids.value(i))
                    .map_err(|e| WorkQueueError::Serialization(format!("Invalid fn_id: {}", e)))?;
                let input_coll_id = CollectionUuid::from_str(coll_ids.value(i)).map_err(|e| {
                    WorkQueueError::Serialization(format!("Invalid collection_id: {}", e))
                })?;

                let completion_offset = offsets.map(|offsets| offsets.value(i));
                let (compaction_offset, hydrated_from_legacy_completion_offset) =
                    match compaction_offsets {
                        Some(compaction_offsets) if compaction_offsets.is_valid(i) => {
                            (compaction_offsets.value(i), false)
                        }
                        _ => (
                            completion_offset.ok_or_else(|| {
                                WorkQueueError::Serialization(
                                    "Legacy row is missing both completion_offset and compaction_offset"
                                        .to_string(),
                                )
                            })?,
                            true,
                        ),
                    };

                if hydrated_from_legacy_completion_offset {
                    hydrated_legacy_rows += 1;
                }

                let record = WorkQueueRecord {
                    fn_id,
                    input_coll_id,
                    completion_offset: completion_offset.unwrap_or(compaction_offset),
                    compaction_offset,
                    insertion_order: orders.value(i),
                    failure_count: failure_counts
                        .map(|failure_counts| {
                            if failure_counts.is_valid(i) {
                                failure_counts.value(i)
                            } else {
                                0
                            }
                        })
                        .unwrap_or(0),
                };

                let key = (fn_id, input_coll_id);
                // Detect duplicate (fn_id, input_coll_id) pairs and log warning
                if state.pending_work.contains_key(&key) {
                    tracing::error!(
                        key = ?key,
                        "Duplicate (fn_id, input_coll_id) pair found in Parquet file - overwriting previous entry"
                    );
                }
                state.pending_work.insert(key, record);
            }
        }

        // Sort by insertion_order to maintain FIFO
        let mut sorted: Vec<_> = state.pending_work.drain().collect();
        sorted.sort_by_key(|(_, record)| record.insertion_order);
        state.pending_work.extend(sorted);

        // Set next_insertion_order
        state.next_insertion_order = state
            .pending_work
            .iter()
            .map(|(_, record)| record.insertion_order + 1)
            .max()
            .unwrap_or(0);

        if hydrated_legacy_rows > 0 {
            tracing::info!(
                "Successfully hydrated missing compaction_offset for {} legacy work queue entr{}",
                hydrated_legacy_rows,
                if hydrated_legacy_rows == 1 {
                    "y"
                } else {
                    "ies"
                }
            );
        }

        Ok(state)
    }

    pub fn push_work(
        &mut self,
        fn_id: AttachedFunctionUuid,
        input_coll_id: CollectionUuid,
        completion_offset: i64,
        compaction_offset: i64,
    ) -> bool {
        let key = (fn_id, input_coll_id);

        if let Some(existing_record) = self.pending_work.get_mut(&key) {
            if compaction_offset <= existing_record.compaction_offset {
                return false;
            }

            existing_record.completion_offset = completion_offset;
            existing_record.compaction_offset = compaction_offset;
            self.dirty = true;
            return true;
        }

        let record = WorkQueueRecord {
            fn_id,
            input_coll_id,
            completion_offset,
            compaction_offset,
            insertion_order: self.next_insertion_order,
            failure_count: 0,
        };

        self.next_insertion_order += 1;
        self.pending_work.insert(key, record);
        self.dirty = true;

        true
    }

    /// Returns up to `limit` eligible records in FIFO order.
    pub(crate) fn get_live_work(
        &self,
        limit: usize,
        max_failure_count: i32,
    ) -> Vec<WorkQueueRecord> {
        let mut work = Vec::with_capacity(limit);

        for (_, item) in self.pending_work.iter() {
            if work.len() >= limit {
                break;
            }

            if item.failure_count >= max_failure_count {
                continue;
            }

            work.push(item.clone());
        }

        work
    }

    pub fn update_failure_count(
        &mut self,
        fn_id: &AttachedFunctionUuid,
        input_coll_id: &CollectionUuid,
        failure_count: i32,
    ) -> bool {
        let Some(record) = self.pending_work.get_mut(&(*fn_id, *input_coll_id)) else {
            return false;
        };

        if record.failure_count == failure_count {
            return false;
        }

        record.failure_count = failure_count;
        self.dirty = true;
        true
    }

    /// Sets the failure count for a queued entry.
    ///
    /// `None` means the entry is absent. `Some(false)` means the entry was
    /// already at the requested value, which is still a successful,
    /// idempotent update.
    pub fn set_failure_count(
        &mut self,
        fn_id: &AttachedFunctionUuid,
        input_coll_id: &CollectionUuid,
        failure_count: i32,
    ) -> Option<bool> {
        let record = self
            .pending_work
            .iter_mut()
            .find(|record| record.fn_id == *fn_id && record.input_coll_id == *input_coll_id)?;

        if record.failure_count == failure_count {
            return Some(false);
        }

        record.failure_count = failure_count;
        self.dirty = true;
        Some(true)
    }

    /// Mark work as successfully completed.
    /// Removes the queue entry once completion reaches the queued frontier;
    /// otherwise leaves the queued entry unchanged.
    pub fn finish_work_success(
        &mut self,
        fn_id: &AttachedFunctionUuid,
        input_coll_id: &CollectionUuid,
        completion_offset: i64,
    ) {
        let key = (*fn_id, *input_coll_id);

        if let Some(compaction_offset) = self
            .pending_work
            .get(&key)
            .map(|record| record.compaction_offset)
        {
            if compaction_offset <= completion_offset {
                self.pending_work.remove(&key);
                self.dirty = true;
            } else if let Some(record) = self.pending_work.get_mut(&key) {
                if record.failure_count != 0 {
                    record.failure_count = 0;
                    self.dirty = true;
                }
            }
        }
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
            compaction_offset: 140,
            insertion_order: 0,
            failure_count: 0,
        };

        let record2 = WorkQueueRecord {
            fn_id: AttachedFunctionUuid(Uuid::new_v4()),
            input_coll_id: CollectionUuid(Uuid::new_v4()),
            completion_offset: 200,
            compaction_offset: 240,
            insertion_order: 1,
            failure_count: 5,
        };

        let record3 = WorkQueueRecord {
            fn_id: AttachedFunctionUuid(Uuid::new_v4()),
            input_coll_id: CollectionUuid(Uuid::new_v4()),
            completion_offset: 300,
            compaction_offset: 360,
            insertion_order: 2,
            failure_count: 0,
        };

        assert!(state.push_work(
            record1.fn_id,
            record1.input_coll_id,
            record1.completion_offset,
            record1.compaction_offset,
        ));
        assert!(state.push_work(
            record2.fn_id,
            record2.input_coll_id,
            record2.completion_offset,
            record2.compaction_offset,
        ));
        assert!(state.update_failure_count(&record2.fn_id, &record2.input_coll_id, 5));
        assert!(state.push_work(
            record3.fn_id,
            record3.input_coll_id,
            record3.completion_offset,
            record3.compaction_offset,
        ));

        let bytes = state.to_parquet_bytes().expect("Failed to serialize");
        let restored = QueueState::from_parquet_bytes(&bytes).expect("Failed to deserialize");

        assert_eq!(restored.pending_work.len(), 3);
        let restored_records: Vec<_> = restored.pending_work.values().collect();
        assert_eq!(restored_records[0].completion_offset, 100);
        assert_eq!(restored_records[0].compaction_offset, 140);
        assert_eq!(restored_records[1].completion_offset, 200);
        assert_eq!(restored_records[1].compaction_offset, 240);
        assert_eq!(restored_records[1].failure_count, 5);
        assert_eq!(restored_records[2].completion_offset, 300);
        assert_eq!(restored_records[2].compaction_offset, 360);
    }

    #[test]
    fn test_queue_state_deserializes_legacy_completion_offset_schema() {
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
        .expect("Failed to build legacy batch");

        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, schema, None).expect("Failed to create writer");
        writer.write(&batch).expect("Failed to write batch");
        writer.close().expect("Failed to close writer");

        let restored =
            QueueState::from_parquet_bytes(&buffer).expect("Failed to deserialize legacy schema");

        assert_eq!(restored.pending_work.len(), 1);
        let restored_record = restored.pending_work.values().next().unwrap();
        assert_eq!(restored_record.completion_offset, 100);
        assert_eq!(restored_record.compaction_offset, 100);
        assert_eq!(restored_record.failure_count, 0);
    }

    #[test]
    fn test_dlq_state_survives_a_newer_queue_frontier() {
        let mut state = QueueState::new();
        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        assert!(state.push_work(fn_id, coll_id, 10, 10));
        assert!(state.update_failure_count(&fn_id, &coll_id, 5));

        assert!(state.push_work(fn_id, coll_id, 20, 20));
        assert_eq!(state.pending_work.values().next().unwrap().failure_count, 5);
    }

    #[test]
    fn test_set_failure_count_is_idempotent_and_reports_missing_entries() {
        let mut state = QueueState::new();
        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        assert_eq!(state.set_failure_count(&fn_id, &coll_id, 0), None);

        state.push_work(fn_id, coll_id, 10, 10);
        state.dirty = false;

        assert_eq!(state.set_failure_count(&fn_id, &coll_id, 0), Some(false));
        assert!(!state.dirty);
        assert_eq!(state.set_failure_count(&fn_id, &coll_id, 3), Some(true));
        assert!(state.dirty);
        assert_eq!(state.pending_work[0].failure_count, 3);
    }

    #[test]
    fn test_success_clears_dlq_when_work_remains() {
        let mut state = QueueState::new();
        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        assert!(state.push_work(fn_id, coll_id, 10, 20));
        assert!(state.update_failure_count(&fn_id, &coll_id, 5));

        state.finish_work_success(&fn_id, &coll_id, 10);

        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work.values().next().unwrap().failure_count, 0);
    }

    #[test]
    fn test_push_work_prefers_highest_compaction_offset() {
        let mut state = QueueState::new();

        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        state.push_work(fn_id, coll_id, 20, 40);
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(
            state
                .pending_work
                .values()
                .next()
                .unwrap()
                .completion_offset,
            20
        );
        assert_eq!(
            state
                .pending_work
                .values()
                .next()
                .unwrap()
                .compaction_offset,
            40
        );

        state.push_work(fn_id, coll_id, 20, 60);
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(
            state
                .pending_work
                .values()
                .next()
                .unwrap()
                .completion_offset,
            20
        );
        assert_eq!(
            state
                .pending_work
                .values()
                .next()
                .unwrap()
                .compaction_offset,
            60
        );
    }

    #[test]
    fn test_replacing_work_preserves_its_queue_position() {
        let mut state = QueueState::new();
        let first_fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let first_coll_id = CollectionUuid(Uuid::new_v4());
        let second_fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let second_coll_id = CollectionUuid(Uuid::new_v4());

        assert!(state.push_work(first_fn_id, first_coll_id, 10, 10));
        assert!(state.push_work(second_fn_id, second_coll_id, 20, 20));
        assert!(state.push_work(first_fn_id, first_coll_id, 30, 30));

        let records: Vec<_> = state.pending_work.values().collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].fn_id, first_fn_id);
        assert_eq!(records[0].completion_offset, 30);
        assert_eq!(records[1].fn_id, second_fn_id);
    }

    #[test]
    fn test_finish_work_waits_for_compaction_offset() {
        let mut state = QueueState::new();

        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        state.push_work(fn_id, coll_id, 20, 60);
        assert_eq!(state.pending_work.len(), 1);

        state.finish_work_success(&fn_id, &coll_id, 40);
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(
            state
                .pending_work
                .values()
                .next()
                .unwrap()
                .completion_offset,
            20
        );
        assert_eq!(
            state
                .pending_work
                .values()
                .next()
                .unwrap()
                .compaction_offset,
            60
        );

        state.finish_work_success(&fn_id, &coll_id, 60);
        assert_eq!(state.pending_work.len(), 0);
    }
}
