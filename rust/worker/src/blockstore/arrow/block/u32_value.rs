use super::{
    delta::BlockDelta,
    delta_storage::{BlockStorage, StringValueStorage, UInt32Storage},
};
use crate::blockstore::{
    arrow::types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    key::{CompositeKey, KeyWrapper},
};
use arrow::{
    array::{Array, StringArray, UInt32Array},
    util::bit_util,
};
use std::sync::Arc;

impl ArrowWriteableValue for u32 {
    type ReadableValue<'referred_data> = &'referred_data str;

    fn offset_size(item_count: usize) -> usize {
        0
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::UInt32(builder) => {
                let mut storage = builder.storage.write();
                storage.insert(
                    CompositeKey {
                        prefix: prefix.to_string(),
                        key,
                    },
                    value,
                );
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::UInt32(builder) => {
                let mut storage = builder.storage.write();
                storage.remove(&CompositeKey {
                    prefix: prefix.to_string(),
                    key,
                });
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn get_delta_builder() -> BlockStorage {
        BlockStorage::UInt32(UInt32Storage::new())
    }
}

impl ArrowReadableValue<'_> for u32 {
    fn get(array: &Arc<dyn Array>, index: usize) -> u32 {
        let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
        array.value(index)
    }
    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        delta: &mut BlockDelta,
    ) {
        delta.add(prefix, key, value);
    }
}
