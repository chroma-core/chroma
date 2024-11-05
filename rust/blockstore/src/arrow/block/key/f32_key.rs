use crate::arrow::{
    block::delta::{BlockKeyArrowBuilder, BlockStorage},
    types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey},
};
use arrow::array::{Array, Float32Array, Float32Builder, StringBuilder};
use std::sync::Arc;

impl ArrowWriteableKey for f32 {
    type ReadableKey<'referred_data> = f32;

    fn offset_size(_: usize) -> usize {
        0
    }
    fn get_arrow_builder(
        item_count: usize,
        prefix_capacity: usize,
        _: usize,
    ) -> BlockKeyArrowBuilder {
        let prefix_builder = StringBuilder::with_capacity(item_count, prefix_capacity);
        let key_builder = Float32Builder::with_capacity(item_count);
        BlockKeyArrowBuilder::Float32((prefix_builder, key_builder))
    }
}

impl ArrowReadableKey<'_> for f32 {
    fn get(array: &Arc<dyn Array>, index: usize) -> Self {
        array
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(index)
    }

    fn add_to_delta<'external, V: ArrowReadableValue<'external>>(
        prefix: &str,
        key: Self,
        value: V,
        storage: &mut BlockStorage,
    ) {
        V::add_to_delta(prefix, key, value, storage);
    }
}
