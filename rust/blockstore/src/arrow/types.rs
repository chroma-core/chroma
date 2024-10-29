use super::block::delta::{BlockKeyArrowBuilder, BlockStorage, UnorderedBlockDelta};
use crate::{key::KeyWrapper, Key, Value};
use arrow::{array::Array, datatypes::Field};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuilderMutationOrderHint {
    Unordered,
    Ordered,
}

pub trait ArrowWriteableValue: Value {
    type ArrowBuilder;
    type SizeTracker;
    type ReadableValue<'referred_data>: ArrowReadableValue<'referred_data>;
    type PreparedValue;

    fn offset_size(item_count: usize) -> usize;
    fn validity_size(item_count: usize) -> usize;
    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockStorage);
    fn delete(prefix: &str, key: KeyWrapper, delta: &UnorderedBlockDelta);
    fn get_arrow_builder(size_tracker: Self::SizeTracker) -> Self::ArrowBuilder;
    fn get_delta_builder(mutation_ordering_hint: BuilderMutationOrderHint) -> BlockStorage;
    fn prepare(value: Self) -> Self::PreparedValue;
    fn append(value: Self::PreparedValue, builder: &mut Self::ArrowBuilder);
    fn finish(builder: Self::ArrowBuilder) -> (Field, Arc<dyn Array>);
}

pub trait ArrowReadableKey<'referred_data>: Key + PartialOrd {
    fn get(array: &'referred_data Arc<dyn Array>, index: usize) -> Self;
    fn add_to_delta<'external, V: ArrowReadableValue<'external>>(
        prefix: &str,
        key: Self,
        value: V,
        delta: &mut BlockStorage,
    );
}

pub trait ArrowReadableValue<'referred_data>: Sized {
    fn get(array: &'referred_data Arc<dyn Array>, index: usize) -> Self;
    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        delta: &mut BlockStorage,
    );
}
