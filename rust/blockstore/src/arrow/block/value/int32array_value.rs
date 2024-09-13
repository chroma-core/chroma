use crate::{
    arrow::{
        block::delta::{single_column_storage::SingleColumnStorage, BlockDelta, BlockStorage},
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::KeyWrapper,
};
use arrow::{
    array::{Array, ListArray, UInt32Array},
    util::bit_util,
};
use std::{mem::size_of, sync::Arc};

impl ArrowWriteableValue for Vec<u32> {
    type ReadableValue<'referred_data> = &'referred_data [u32];

    fn offset_size(item_count: usize) -> usize {
        bit_util::round_upto_multiple_of_64((item_count + 1) * size_of::<u32>())
    }

    fn validity_size(_item_count: usize) -> usize {
        0 // We don't support None values for Int32Array
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::VecUInt32(builder) => {
                builder.add(prefix, key, value);
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::VecUInt32(builder) => {
                builder.delete(prefix, key);
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn get_delta_builder() -> BlockStorage {
        BlockStorage::VecUInt32(SingleColumnStorage::new())
    }
}

impl<'referred_data> ArrowReadableValue<'referred_data> for &'referred_data [u32] {
    fn get(array: &'referred_data Arc<dyn Array>, index: usize) -> Self {
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
        let start = list_array.value_offsets()[index] as usize;
        let end = list_array.value_offsets()[index + 1] as usize;
        let u32array = list_array
            .values()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        &u32array.values()[start..end]
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
