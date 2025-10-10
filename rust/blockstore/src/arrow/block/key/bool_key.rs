use crate::arrow::{
    block::delta::{BlockKeyArrowBuilder, BlockStorage},
    types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey},
};
use arrow::array::{Array, AsArray, BooleanArray, BooleanBuilder, StringBuilder};
use std::sync::Arc;

impl ArrowWriteableKey for bool {
    type ReadableKey<'referred_data> = bool;

    fn offset_size(_: usize) -> usize {
        0
    }
    fn get_arrow_builder(
        item_count: usize,
        prefix_capacity: usize,
        _: usize,
    ) -> BlockKeyArrowBuilder {
        let prefix_builder = StringBuilder::with_capacity(item_count, prefix_capacity);
        let key_builder: BooleanBuilder = BooleanBuilder::with_capacity(item_count);
        BlockKeyArrowBuilder::Boolean((prefix_builder, key_builder))
    }
}

impl ArrowReadableKey<'_> for bool {
    fn get(array: &Arc<dyn Array>, index: usize) -> Self {
        array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(index)
    }

    fn get_range(array: &Arc<dyn Array>, offset: usize, length: usize) -> Vec<Self> {
        array
            .as_boolean()
            .slice(offset, length)
            .iter()
            .map(Option::unwrap_or_default)
            .collect()
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
