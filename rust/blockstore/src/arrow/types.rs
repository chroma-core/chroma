use super::block::{
    delta::BlockDelta,
    delta::{BlockKeyArrowBuilder, BlockStorage},
};
use crate::{key::KeyWrapper, Key, Value};
use arrow::array::Array;
use std::sync::Arc;

pub trait ArrowWriteableKey: Key + Default {
    type ReadableKey<'referred_data>: ArrowReadableKey<'referred_data>;

    fn offset_size(item_count: usize) -> usize;
    fn get_arrow_builder(
        item_count: usize,
        prefix_capacity: usize,
        key_capacity: usize,
    ) -> BlockKeyArrowBuilder;
}

pub trait ArrowWriteableValue: Value {
    type ReadableValue<'referred_data>: ArrowReadableValue<'referred_data>;

    fn offset_size(item_count: usize) -> usize;
    fn validity_size(item_count: usize) -> usize;
    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockDelta);
    fn delete(prefix: &str, key: KeyWrapper, delta: &BlockDelta);
    fn get_delta_builder() -> BlockStorage;
}

pub trait ArrowReadableKey<'referred_data>: Key + PartialOrd {
    fn get(array: &'referred_data Arc<dyn Array>, index: usize) -> Self;
    fn add_to_delta<'external, V: ArrowReadableValue<'external>>(
        prefix: &str,
        key: Self,
        value: V,
        delta: &mut BlockDelta,
    );
}

pub trait ArrowReadableValue<'referred_data>: Sized {
    fn get(array: &'referred_data Arc<dyn Array>, index: usize) -> Self;
    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        delta: &mut BlockDelta,
    );
}
