use std::sync::Arc;

use arrow::{
    array::{Array, AsArray, Float32Array, Float32Builder},
    datatypes::{DataType, Field, Float32Type},
};

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

impl ArrowWriteableValue for f32 {
    type ReadableValue<'referred_data> = f32;
    type ArrowBuilder = Float32Builder;
    type SizeTracker = SingleColumnSizeTracker;
    type PreparedValue = f32;

    fn offset_size(_item_count: usize) -> usize {
        0
    }

    fn validity_size(_item_count: usize) -> usize {
        0 // We don't support None values for Float32Array
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockStorage) {
        match &delta {
            BlockStorage::Float32(builder) => builder.add(prefix, key, value),
            _ => panic!("Invalid builder type: {:?}", &delta),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &UnorderedBlockDelta) {
        match &delta.builder {
            BlockStorage::Float32(builder) => builder.delete(prefix, key),
            _ => panic!("Invalid builder type: {:?}", &delta.builder),
        }
    }

    fn get_delta_builder(mutation_ordering_hint: BlockfileWriterMutationOrdering) -> BlockStorage {
        BlockStorage::Float32(SingleColumnStorage::new(mutation_ordering_hint))
    }

    fn get_arrow_builder(size_tracker: Self::SizeTracker) -> Self::ArrowBuilder {
        Float32Builder::with_capacity(size_tracker.get_num_items())
    }

    fn prepare(value: Self) -> Self::PreparedValue {
        value
    }

    fn append(value: Self::PreparedValue, builder: &mut Self::ArrowBuilder) {
        builder.append_value(value);
    }

    fn finish(mut builder: Self::ArrowBuilder, _: &Self::SizeTracker) -> (Field, Arc<dyn Array>) {
        let value_field = Field::new("value", DataType::Float32, false);
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
            BlockStorage::Float32(builder) => builder.get_owned_value(prefix, key),
            _ => panic!("Invalid builder type: {:?}", &delta.builder),
        }
    }
}

impl ArrowReadableValue<'_> for f32 {
    fn get(array: &Arc<dyn Array>, index: usize) -> f32 {
        let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
        array.value(index)
    }

    fn get_range(array: &Arc<dyn Array>, offset: usize, length: usize) -> Vec<Self> {
        array
            .as_primitive::<Float32Type>()
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
        f32::add(prefix, key.into(), value, storage);
    }
}
