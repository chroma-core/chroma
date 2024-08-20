use crate::{
    arrow::{
        block::delta::{single_column_storage::SingleColumnStorage, BlockDelta, BlockStorage},
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::KeyWrapper,
};
use arrow::{
    array::{Array, BinaryArray},
    util::bit_util,
};
use roaring::RoaringBitmap;

impl ArrowWriteableValue for RoaringBitmap {
    type ReadableValue<'referred_data> = RoaringBitmap;

    fn offset_size(item_count: usize) -> usize {
        bit_util::round_upto_multiple_of_64((item_count + 1) * 4)
    }

    fn validity_size(_item_count: usize) -> usize {
        0 // We don't support None values for RoaringBitmap
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::RoaringBitmap(builder) => {
                builder.add(prefix, key, value);
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::RoaringBitmap(builder) => {
                builder.delete(prefix, key);
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn get_delta_builder() -> BlockStorage {
        BlockStorage::RoaringBitmap(SingleColumnStorage::new())
    }
}

impl ArrowReadableValue<'_> for RoaringBitmap {
    fn get(array: &std::sync::Arc<dyn Array>, index: usize) -> Self {
        let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
        let bytes = arr.value(index);
        // TODO: proper error handling
        RoaringBitmap::deserialize_from(bytes).unwrap()
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
