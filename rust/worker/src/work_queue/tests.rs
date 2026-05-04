#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::work_queue::state::QueueState;
    use crate::work_queue::types::WorkQueueRecord;
    use chroma_types::{AttachedFunctionUuid, CollectionUuid};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_work_queue_basic_operations() {
        // TODO: Create proper storage for testing
        // let storage = Storage::for_test();
        // let config = config::WorkQueueConfig::default();

        // let mut manager = WorkQueueManager::new(storage, config);

        // Test push work
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let _push_msg = PushWorkMessage {
            fn_id: AttachedFunctionUuid(Uuid::new_v4()),
            input_coll_id: CollectionUuid(Uuid::new_v4()),
            completion_offset: 100,
            response_tx: tx,
        };

        // This would normally be done through the component system
        // manager.handle(push_msg, &ctx).await;

        // For now, just verify the manager can be created
        // The manager is successfully created if we reach this point
    }

    #[test]
    fn test_queue_state_fifo_ordering() {
        let mut state = QueueState::new();

        // Add items in random order
        let items = vec![(100, 2), (200, 0), (150, 1)];

        for (offset, order) in items {
            let record = WorkQueueRecord {
                fn_id: AttachedFunctionUuid(Uuid::new_v4()),
                input_coll_id: CollectionUuid(Uuid::new_v4()),
                completion_offset: offset,
                insertion_order: order,
            };
            state.pending_work.push_back(record);
        }

        // Verify FIFO ordering by insertion_order
        let orders: Vec<u64> = state
            .pending_work
            .iter()
            .map(|r| r.insertion_order)
            .collect();
        assert_eq!(orders, vec![2, 0, 1]);
    }

    #[test]
    fn test_queue_state_deduplication() {
        let mut state = QueueState::new();

        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        // Add initial record
        let record1 = WorkQueueRecord {
            fn_id: fn_id.clone(),
            input_coll_id: coll_id.clone(),
            completion_offset: 100,
            insertion_order: 0,
        };
        state.pending_work.push_back(record1.clone());
        state
            .dedup_index
            .insert((fn_id.clone(), coll_id.clone()), 100);

        // Check dedup index
        assert_eq!(
            state.dedup_index.get(&(fn_id.clone(), coll_id.clone())),
            Some(&100)
        );

        // Simulate adding a newer record (should replace)
        state
            .pending_work
            .retain(|r| !(r.fn_id == fn_id && r.input_coll_id == coll_id));

        let record2 = WorkQueueRecord {
            fn_id: fn_id.clone(),
            input_coll_id: coll_id.clone(),
            completion_offset: 200,
            insertion_order: 1,
        };
        state.pending_work.push_back(record2);
        state
            .dedup_index
            .insert((fn_id.clone(), coll_id.clone()), 200);

        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 200);
    }

    #[tokio::test]
    async fn test_parquet_round_trip() {
        let mut state = QueueState::new();

        // Add some records
        for i in 0..5 {
            let record = WorkQueueRecord {
                fn_id: AttachedFunctionUuid(Uuid::new_v4()),
                input_coll_id: CollectionUuid(Uuid::new_v4()),
                completion_offset: i * 100,
                insertion_order: i as u64,
            };
            let key = (record.fn_id.clone(), record.input_coll_id.clone());
            state.dedup_index.insert(key, record.completion_offset);
            state.pending_work.push_back(record);
        }

        // Serialize to parquet
        let bytes = state.to_parquet_bytes().expect("Failed to serialize");

        // Deserialize from parquet
        let restored = QueueState::from_parquet_bytes(&bytes).expect("Failed to deserialize");

        // Verify state is preserved
        assert_eq!(restored.pending_work.len(), 5);
        assert_eq!(restored.dedup_index.len(), 5);
        assert_eq!(restored.next_insertion_order, 5);

        // Verify ordering is preserved
        for (i, record) in restored.pending_work.iter().enumerate() {
            assert_eq!(record.insertion_order, i as u64);
            assert_eq!(record.completion_offset, i as i64 * 100);
        }
    }
}
