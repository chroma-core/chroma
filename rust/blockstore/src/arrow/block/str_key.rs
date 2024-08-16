use std::sync::Arc;

use super::{delta::BlockDelta, delta::BlockKeyArrowBuilder};
use crate::arrow::types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey};
use arrow::{
    array::{Array, StringArray, StringBuilder},
    util::bit_util,
};

impl ArrowWriteableKey for &str {
    type ReadableKey<'referred_data> = &'referred_data str;

    fn offset_size(item_count: usize) -> usize {
        bit_util::round_upto_multiple_of_64((item_count + 1) * 4)
    }
    fn get_arrow_builder(
        item_count: usize,
        prefix_capacity: usize,
        capacity: usize,
    ) -> BlockKeyArrowBuilder {
        let prefix_builder = StringBuilder::with_capacity(item_count, prefix_capacity);
        let key_builder = StringBuilder::with_capacity(item_count, capacity);
        BlockKeyArrowBuilder::String((prefix_builder, key_builder))
    }
}

impl<'referred_data> ArrowReadableKey<'referred_data> for &'referred_data str {
    fn get(array: &'referred_data Arc<dyn Array>, index: usize) -> &'referred_data str {
        array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(index)
    }
    fn add_to_delta<'external, V: ArrowReadableValue<'external>>(
        prefix: &str,
        key: Self,
        value: V,
        delta: &mut BlockDelta,
    ) {
        // We could probably enclose this somehow to make it more ergonomic
        V::add_to_delta(prefix, key, value, delta);
    }
}
