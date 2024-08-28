use crate::{
    arrow::{
        block::delta::{single_column_storage::SingleColumnStorage, BlockDelta, BlockStorage},
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::KeyWrapper,
};
use arrow::{
    array::{Array, Int32Array, ListArray},
    util::bit_util,
};
use std::sync::Arc;

impl ArrowWriteableValue for Vec<i32> {
    type ReadableValue<'referred_data> = &'referred_data [i32];

    fn offset_size(item_count: usize) -> usize {
        bit_util::round_upto_multiple_of_64((item_count + 1) * 4)
    }

    fn validity_size(_item_count: usize) -> usize {
        0 // We don't support None values for Int32Array
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::Int32Array(builder) => {
                builder.add(prefix, key, value);
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::Int32Array(builder) => {
                builder.delete(prefix, key);
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn get_delta_builder() -> BlockStorage {
        BlockStorage::Int32Array(SingleColumnStorage::new())
    }
}

impl<'referred_data> ArrowReadableValue<'referred_data> for &'referred_data [i32] {
    fn get(array: &'referred_data Arc<dyn Array>, index: usize) -> Self {
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
        let start = list_array.value_offsets()[index] as usize;
        let end = list_array.value_offsets()[index + 1] as usize;
        let i32array = list_array
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        &i32array.values()[start..end]
    }

    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        delta: &mut BlockDelta,
    ) {
        delta.add(prefix, key, value.to_vec());
    }
}
