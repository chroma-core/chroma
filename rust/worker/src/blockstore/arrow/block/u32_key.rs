use super::delta_storage::BlockKeyArrowBuilder;
use crate::blockstore::arrow::types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey};
use arrow::array::{Array, Float32Builder, StringBuilder, UInt32Array, UInt32Builder};
use std::sync::Arc;

impl ArrowWriteableKey for u32 {
    type ReadableKey<'referred_data> = u32;

    fn offset_size(_: usize) -> usize {
        0
    }
    fn get_arrow_builder(
        item_count: usize,
        prefix_capacity: usize,
        _: usize,
    ) -> BlockKeyArrowBuilder {
        let prefix_builder = StringBuilder::with_capacity(item_count, prefix_capacity);
        let key_builder = UInt32Builder::with_capacity(item_count);
        BlockKeyArrowBuilder::UInt32((prefix_builder, key_builder))
    }
}

impl ArrowReadableKey<'_> for u32 {
    fn get(array: &Arc<dyn Array>, index: usize) -> Self {
        array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(index)
    }

    fn add_to_delta<'external, V: ArrowReadableValue<'external>>(
        prefix: &str,
        key: Self,
        value: V,
        delta: &mut super::delta::BlockDelta,
    ) {
        V::add_to_delta(prefix, key, value, delta);
    }
}
