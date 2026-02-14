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
    array::{Array, AsArray, Float32Builder, ListBuilder},
    datatypes::{Field, Float32Type},
    util::bit_util,
};
use std::{mem::size_of, sync::Arc};

impl ArrowWriteableValue for Vec<f32> {
    type ReadableValue<'referred_data> = &'referred_data [f32];
    type ArrowBuilder = ListBuilder<Float32Builder>;
    type SizeTracker = SingleColumnSizeTracker;
    type PreparedValue = Vec<f32>;

    fn offset_size(item_count: usize) -> usize {
        bit_util::round_upto_multiple_of_64((item_count + 1) * size_of::<i32>())
    }

    fn validity_size(_item_count: usize) -> usize {
        0
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockStorage) {
        match delta {
            BlockStorage::VecFloat32(builder) => builder.add(prefix, key, value),
            _ => panic!("Invalid builder type"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &UnorderedBlockDelta) {
        match &delta.builder {
            BlockStorage::VecFloat32(builder) => builder.delete(prefix, key),
            _ => panic!("Invalid builder type"),
        }
    }

    fn get_delta_builder(mutation_ordering_hint: BlockfileWriterMutationOrdering) -> BlockStorage {
        BlockStorage::VecFloat32(SingleColumnStorage::new(mutation_ordering_hint))
    }

    fn get_arrow_builder(size_tracker: Self::SizeTracker) -> Self::ArrowBuilder {
        let total_value_count = size_tracker.get_value_size() / size_of::<f32>();
        ListBuilder::with_capacity(
            Float32Builder::with_capacity(total_value_count),
            size_tracker.get_num_items(),
        )
    }

    fn prepare(value: Self) -> Self::PreparedValue {
        value
    }

    fn append(value: Self::PreparedValue, builder: &mut Self::ArrowBuilder) {
        builder.values().append_slice(&value);
        builder.append(true);
    }

    fn finish(mut builder: Self::ArrowBuilder, _: &Self::SizeTracker) -> (Field, Arc<dyn Array>) {
        let value_field = Field::new(
            "value",
            arrow::datatypes::DataType::List(Arc::new(Field::new(
                "item",
                arrow::datatypes::DataType::Float32,
                true,
            ))),
            true,
        );
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
            BlockStorage::VecFloat32(builder) => builder.get_owned_value(prefix, key),
            _ => panic!("Invalid builder type"),
        }
    }
}

impl<'referred_data> ArrowReadableValue<'referred_data> for &'referred_data [f32] {
    fn get(array: &'referred_data Arc<dyn Array>, index: usize) -> Self {
        let list_array = array.as_list::<i32>();
        let start = list_array.value_offsets()[index] as usize;
        let end = list_array.value_offsets()[index + 1] as usize;
        let values = list_array.values().as_primitive::<Float32Type>();
        &values.values()[start..end]
    }

    fn get_range(array: &'referred_data Arc<dyn Array>, offset: usize, length: usize) -> Vec<Self> {
        let list_array = array.as_list::<i32>();
        let offsets = list_array.offsets().slice(offset, length);
        let values = list_array.values().as_primitive::<Float32Type>();
        offsets
            .iter()
            .zip(offsets.iter().skip(1))
            .map(|(&start, &end)| &values.values()[start as usize..end as usize])
            .collect()
    }

    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        storage: &mut BlockStorage,
    ) {
        <Vec<f32>>::add(prefix, key.into(), value.to_vec(), storage);
    }
}
