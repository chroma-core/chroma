use crate::{
    arrow::{
        block::delta::{
            single_column_size_tracker::SingleColumnSizeTracker,
            single_column_storage::SingleColumnStorage, BlockDelta, BlockStorage,
        },
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::KeyWrapper,
};
use arrow::{
    array::{Array, UInt32Array, UInt32Builder},
    datatypes::Field,
};
use std::sync::Arc;

impl ArrowWriteableValue for u32 {
    type ReadableValue<'referred_data> = u32;
    type ArrowBuilder = UInt32Builder;
    type SizeTracker = SingleColumnSizeTracker;
    type PreparedValue = u32;

    fn offset_size(_item_count: usize) -> usize {
        0
    }

    fn validity_size(_item_count: usize) -> usize {
        0 // We don't support None values for UInt32Array
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::UInt32(builder) => builder.add(prefix, key, value),
            _ => panic!("Invalid builder type: {:?}", &delta.builder),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::UInt32(builder) => builder.delete(prefix, key),
            _ => panic!("Invalid builder type: {:?}", &delta.builder),
        }
    }

    fn get_delta_builder() -> BlockStorage {
        BlockStorage::UInt32(SingleColumnStorage::new())
    }

    fn get_arrow_builder(size_tracker: Self::SizeTracker) -> Self::ArrowBuilder {
        UInt32Builder::with_capacity(size_tracker.get_num_items())
    }

    fn prepare(value: Self) -> Self::PreparedValue {
        value
    }

    fn append(value: Self::PreparedValue, builder: &mut Self::ArrowBuilder) {
        builder.append_value(value);
    }

    fn finish(mut builder: Self::ArrowBuilder) -> (arrow::datatypes::Field, Arc<dyn Array>) {
        let value_field = Field::new("value", arrow::datatypes::DataType::UInt32, false);
        let value_arr = builder.finish();
        let value_arr = (&value_arr as &dyn Array).slice(0, value_arr.len());
        (value_field, value_arr)
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
