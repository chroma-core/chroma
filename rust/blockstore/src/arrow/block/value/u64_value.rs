use crate::{
    arrow::{
        block::delta::{
            single_column_size_tracker::SingleColumnSizeTracker,
            single_column_storage::SingleColumnStorage, BlockStorage, UnorderedBlockDelta,
        },
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::KeyWrapper,
    BlockfileWriterMutationOrdering,
};
use arrow::{
    array::{Array, AsArray, UInt64Array, UInt64Builder},
    datatypes::{Field, UInt64Type},
};
use std::sync::Arc;

impl ArrowWriteableValue for u64 {
    type ReadableValue<'referred_data> = u64;
    type ArrowBuilder = UInt64Builder;
    type SizeTracker = SingleColumnSizeTracker;
    type PreparedValue = u64;

    fn offset_size(_item_count: usize) -> usize {
        0
    }

    fn validity_size(_item_count: usize) -> usize {
        0 // We don't support None values for UInt64Array
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockStorage) {
        match &delta {
            BlockStorage::UInt64(builder) => builder.add(prefix, key, value),
            _ => panic!("Invalid builder type: {:?}", &delta),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &UnorderedBlockDelta) {
        match &delta.builder {
            BlockStorage::UInt64(builder) => builder.delete(prefix, key),
            _ => panic!("Invalid builder type: {:?}", &delta.builder),
        }
    }

    fn get_delta_builder(mutation_ordering_hint: BlockfileWriterMutationOrdering) -> BlockStorage {
        BlockStorage::UInt64(SingleColumnStorage::new(mutation_ordering_hint))
    }

    fn get_arrow_builder(size_tracker: Self::SizeTracker) -> Self::ArrowBuilder {
        UInt64Builder::with_capacity(size_tracker.get_num_items())
    }

    fn prepare(value: Self) -> Self::PreparedValue {
        value
    }

    fn append(value: Self::PreparedValue, builder: &mut Self::ArrowBuilder) {
        builder.append_value(value);
    }

    fn finish(
        mut builder: Self::ArrowBuilder,
        _: &Self::SizeTracker,
    ) -> (arrow::datatypes::Field, Arc<dyn Array>) {
        let value_field = Field::new("value", arrow::datatypes::DataType::UInt64, false);
        let value_arr = builder.finish();
        let value_arr = (&value_arr as &dyn Array).slice(0, value_arr.len());
        (value_field, value_arr)
    }

    fn get_owned_value_from_delta(
        prefix: &str,
        key: KeyWrapper,
        delta: &UnorderedBlockDelta,
    ) -> Option<Self::PreparedValue> {
        match &delta.builder {
            BlockStorage::UInt64(builder) => builder.get_owned_value(prefix, key),
            _ => panic!("Invalid builder type: {:?}", &delta.builder),
        }
    }
}

impl ArrowReadableValue<'_> for u64 {
    fn get(array: &Arc<dyn Array>, index: usize) -> u64 {
        let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
        array.value(index)
    }

    fn get_range(array: &Arc<dyn Array>, offset: usize, length: usize) -> Vec<Self> {
        array
            .as_primitive::<UInt64Type>()
            .slice(offset, length)
            .values()
            .to_vec()
    }

    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        storage: &mut BlockStorage,
    ) {
        u64::add(prefix, key.into(), value, storage);
    }
}
