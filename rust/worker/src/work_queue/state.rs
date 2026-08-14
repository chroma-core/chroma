use crate::work_queue::types::{WorkQueueError, WorkQueueRecord};
use arrow::array::{Array, Int32Array, Int64Array, StringArray, UInt64Array};
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
    compaction_offset: i64,
}

impl QueueOffsets {
    fn dedup_frontier(self) -> i64 {
        self.compaction_offset
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
                .map(|r| r.fn_id.to_string())
                .collect();
            let coll_ids: Vec<_> = self
                .pending_work
                .iter()
                .map(|r| r.input_coll_id.to_string())
                .collect();
            let orders: Vec<_> = self
                .pending_work
                .iter()
                .map(|r| r.insertion_order)
                .collect();
            let completion_offsets: Vec<_> = self
                .pending_work
                .iter()
                .map(|r| r.completion_offset)
                .collect();
            let compaction_offsets: Vec<_> = self
                .pending_work
                .iter()
                .map(|r| r.compaction_offset)
                .collect();
            let failure_counts: Vec<_> =
                self.pending_work.iter().map(|r| r.failure_count).collect();

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
                if state.dedup_index.contains_key(&key) {
                    tracing::error!(
                        key = ?key,
                        "Duplicate (fn_id, input_coll_id) pair found in Parquet file - overwriting previous entry"
                    );
                }
                state.dedup_index.insert(
                    key,
                    QueueOffsets {
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

        let new_offsets = QueueOffsets { compaction_offset };

        if let Some(&existing_offsets) = self.dedup_index.get(&key) {
            if new_offsets.dedup_frontier() <= existing_offsets.dedup_frontier() {
                return false;
            }
        }

        let failure_count = self
            .pending_work
            .iter()
            .find(|record| record.fn_id == fn_id && record.input_coll_id == input_coll_id)
            .map_or(0, |record| record.failure_count);

        // We eagerly drop the older queue row here for simplicity; we could
        // remove this retain later if get_work learns to skip stale rows lazily.
        self.pending_work
            .retain(|r| !(r.fn_id == fn_id && r.input_coll_id == input_coll_id));

        let record = WorkQueueRecord {
            fn_id,
            input_coll_id,
            completion_offset,
            compaction_offset,
            insertion_order: self.next_insertion_order,
            failure_count,
        };

        self.next_insertion_order += 1;
        self.dedup_index.insert(key, new_offsets);
        self.pending_work.push_back(record);
        self.dirty = true;

        true
    }

    pub fn add_work(
        &mut self,
        fn_id: AttachedFunctionUuid,
        input_coll_id: CollectionUuid,
        compaction_offset: i64,
        failure_count: i32,
    ) -> bool {
        let key = (fn_id, input_coll_id);
        if self.dedup_index.contains_key(&key) {
            return false;
        }

        let record = WorkQueueRecord {
            fn_id,
            input_coll_id,
            completion_offset: compaction_offset,
            compaction_offset,
            insertion_order: self.next_insertion_order,
            failure_count,
        };
        self.next_insertion_order += 1;
        self.dedup_index.insert(
            key,
            QueueOffsets {
                compaction_offset: record.compaction_offset,
            },
        );
        self.pending_work.push_back(record);
        self.dirty = true;
        true
    }

    pub(crate) fn contains_entry(
        &self,
        fn_id: &AttachedFunctionUuid,
        input_coll_id: &CollectionUuid,
    ) -> bool {
        self.dedup_index.contains_key(&(*fn_id, *input_coll_id))
    }

    pub(crate) fn get_live_work(
        &self,
        limit: usize,
        max_failure_count: i32,
    ) -> (Vec<WorkQueueRecord>, usize) {
        let mut work = Vec::with_capacity(limit);
        let mut failure_count_filtered = 0;

        for item in self
            .pending_work
            .iter()
            .filter(|item| self.contains_entry(&item.fn_id, &item.input_coll_id))
        {
            if item.failure_count >= max_failure_count {
                failure_count_filtered += 1;
            } else if work.len() < limit {
                work.push(item.clone());
            }
        }

        (work, failure_count_filtered)
    }

    pub fn update_failure_count(
        &mut self,
        fn_id: &AttachedFunctionUuid,
        input_coll_id: &CollectionUuid,
        failure_count: i32,
    ) -> bool {
        let Some(record) = self
            .pending_work
            .iter_mut()
            .find(|record| record.fn_id == *fn_id && record.input_coll_id == *input_coll_id)
        else {
            return false;
        };

        if record.failure_count == failure_count {
            return false;
        }

        record.failure_count = failure_count;
        self.dirty = true;
        true
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

        if let Some(existing_offsets) = self.dedup_index.get_mut(&key) {
            if existing_offsets.dedup_frontier() <= completion_offset {
                // Remove the single entry for this key
                self.pending_work
                    .retain(|r| !(r.fn_id == *fn_id && r.input_coll_id == *input_coll_id));

                // Remove from dedup index
                self.dedup_index.remove(&key);
                self.dirty = true;
            } else if let Some(record) = self
                .pending_work
                .iter_mut()
                .find(|record| record.fn_id == *fn_id && record.input_coll_id == *input_coll_id)
            {
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

        state.pending_work.push_back(record1.clone());
        state.dedup_index.insert(
            (record1.fn_id, record1.input_coll_id),
            QueueOffsets {
                compaction_offset: record1.compaction_offset,
            },
        );

        state.pending_work.push_back(record2.clone());
        state.dedup_index.insert(
            (record2.fn_id, record2.input_coll_id),
            QueueOffsets {
                compaction_offset: record2.compaction_offset,
            },
        );

        state.pending_work.push_back(record3.clone());
        state.dedup_index.insert(
            (record3.fn_id, record3.input_coll_id),
            QueueOffsets {
                compaction_offset: record3.compaction_offset,
            },
        );

        let bytes = state.to_parquet_bytes().expect("Failed to serialize");
        let restored = QueueState::from_parquet_bytes(&bytes).expect("Failed to deserialize");

        assert_eq!(restored.pending_work.len(), 3);
        assert_eq!(restored.pending_work[0].completion_offset, 100);
        assert_eq!(restored.pending_work[0].compaction_offset, 140);
        assert_eq!(restored.pending_work[1].completion_offset, 200);
        assert_eq!(restored.pending_work[1].compaction_offset, 240);
        assert_eq!(restored.pending_work[1].failure_count, 5);
        assert_eq!(restored.pending_work[2].completion_offset, 300);
        assert_eq!(restored.pending_work[2].compaction_offset, 360);
        assert_eq!(restored.dedup_index.len(), 3);
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
        assert_eq!(restored.pending_work[0].completion_offset, 100);
        assert_eq!(restored.pending_work[0].compaction_offset, 100);
        assert_eq!(restored.pending_work[0].failure_count, 0);
    }

    #[test]
    fn test_dlq_state_survives_a_newer_queue_frontier() {
        let mut state = QueueState::new();
        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        assert!(state.push_work(fn_id, coll_id, 10, 10));
        assert!(state.update_failure_count(&fn_id, &coll_id, 5));

        assert!(state.push_work(fn_id, coll_id, 20, 20));
        assert_eq!(state.pending_work[0].failure_count, 5);
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
        assert_eq!(state.pending_work[0].failure_count, 0);
    }

    #[test]
    fn test_push_work_prefers_highest_compaction_offset() {
        let mut state = QueueState::new();

        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        state.push_work(fn_id, coll_id, 20, 40);
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 20);
        assert_eq!(state.pending_work[0].compaction_offset, 40);

        state.push_work(fn_id, coll_id, 20, 60);
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 20);
        assert_eq!(state.pending_work[0].compaction_offset, 60);
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
        assert_eq!(state.pending_work[0].completion_offset, 20);
        assert_eq!(state.pending_work[0].compaction_offset, 60);

        state.finish_work_success(&fn_id, &coll_id, 60);
        assert_eq!(state.pending_work.len(), 0);
    }

    #[test]
    fn test_add_work_persists_explicit_failure_count() {
        let mut state = QueueState::new();
        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        assert!(state.add_work(fn_id, coll_id, 120, 4));
        assert!(!state.add_work(fn_id, coll_id, 220, 0));

        let reloaded = QueueState::from_parquet_bytes(&state.to_parquet_bytes().unwrap()).unwrap();
        assert_eq!(reloaded.pending_work.len(), 1);
        assert_eq!(reloaded.pending_work[0].completion_offset, 120);
        assert_eq!(reloaded.pending_work[0].compaction_offset, 120);
        assert_eq!(reloaded.pending_work[0].failure_count, 4);
    }
}
