use super::delta::{int32::Int32ArrayStorage, BlockDelta, BlockStorage};
use crate::{
    arrow::types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    key::{CompositeKey, KeyWrapper},
};
use arrow::{
    array::{Array, Int32Array, ListArray},
    util::bit_util,
};
use std::sync::Arc;

impl ArrowWriteableValue for &Int32Array {
    type ReadableValue<'referred_data> = Int32Array;

    fn offset_size(item_count: usize) -> usize {
        bit_util::round_upto_multiple_of_64((item_count + 1) * 4)
    }

    fn validity_size(item_count: usize) -> usize {
        0 // We don't support None values for Int32Array
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::Int32Array(builder) => {
                let mut builder = builder.storage.write();
                // We have to clone the value in this odd way here because when reading out of a block we get the entire array
                let mut new_vec = Vec::with_capacity(value.len());
                for i in 0..value.len() {
                    new_vec.push(value.value(i));
                }
                let new_arr = Int32Array::from(new_vec);
                builder.insert(
                    CompositeKey {
                        prefix: prefix.to_string(),
                        key,
                    },
                    new_arr,
                );
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::Int32Array(builder) => {
                let mut builder = builder.storage.write();
                builder.remove(&CompositeKey {
                    prefix: prefix.to_string(),
                    key,
                });
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn get_delta_builder() -> BlockStorage {
        BlockStorage::Int32Array(Int32ArrayStorage::new())
    }
}

impl ArrowReadableValue<'_> for Int32Array {
    fn get(array: &Arc<dyn Array>, index: usize) -> Self {
        let arr = array
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(index);
        // Cloning an arrow array is cheap, since they are immutable and backed by Arc'ed data
        arr.as_any().downcast_ref::<Int32Array>().unwrap().clone()
    }

    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        delta: &mut BlockDelta,
    ) {
        delta.add(prefix, key, &value);
    }
}
