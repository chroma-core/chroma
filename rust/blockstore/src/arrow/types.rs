use super::block::delta::{BlockKeyArrowBuilder, BlockStorage, UnorderedBlockDelta};
use crate::{key::KeyWrapper, BlockfileWriterMutationOrdering, Key, Value};
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

pub trait ArrowWriteableValue: Value {
    /// The type of the Arrow builder (e.g. `StringBuilder` for strings)
    type ArrowBuilder;
    /// The type of the size tracker, `SingleColumnSizeTracker` for most values. This helps keep track of what the serialized size of the Arrow array will be.
    type SizeTracker;
    /// Every writable value has a corresponding readable value type. For example, the readable value type for a `String` is `&str`.
    type ReadableValue<'referred_data>: ArrowReadableValue<'referred_data>;
    /// Some values are a reference type and need to be converted to an owned type or need to be prepared (e.g. serializing a RoaringBitmap) before they can be stored in a delta or Arrow array.
    type PreparedValue;

    /// Some values use an offsets array. This returns the size of the offsets array given the number of items in the array.
    fn offset_size(item_count: usize) -> usize;
    /// Some values use a validity array. This returns the size of the validity array given the number of items in the array.
    fn validity_size(item_count: usize) -> usize;
    /// Add a K/V pair to a delta. This is called when a new K/V pair is added to a blockfile.
    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockStorage);
    /// Delete a K/V pair from a delta. This is called when a K/V pair is deleted from a blockfile.
    fn delete(prefix: &str, key: KeyWrapper, delta: &UnorderedBlockDelta);
    /// Returns an appropriate `BlockStorage` instance for the value type. This is called when creating a new delta.
    fn get_delta_builder(mutation_ordering_hint: BlockfileWriterMutationOrdering) -> BlockStorage;
    /// Constructs a new Arrow builder for `Self::ArrowBuilder` given the final size of the delta. This is called when a delta is done receiving updates and is ready to be committed.
    fn get_arrow_builder(size_tracker: Self::SizeTracker) -> Self::ArrowBuilder;
    /// Prepare a value for storage in delta or Arrow array.
    fn prepare(value: Self) -> Self::PreparedValue;
    /// Given only a prepared value (not a K/V pair), append it to an Arrow builder. This is called during delta serialization when it's being turned into an Arrow array.
    fn append(value: Self::PreparedValue, builder: &mut Self::ArrowBuilder);
    /// Finish an Arrow builder and return the Arrow array and its corresponding field.
    fn finish(
        builder: Self::ArrowBuilder,
        size_tracker: &Self::SizeTracker,
    ) -> (Field, Arc<dyn Array>);
}

pub trait ArrowReadableKey<'referred_data>: Key + PartialOrd {
    fn get(array: &'referred_data Arc<dyn Array>, index: usize) -> Self;
    fn add_to_delta<'external, V: ArrowReadableValue<'external>>(
        prefix: &str,
        key: Self,
        value: V,
        storage: &mut BlockStorage,
    );
}

pub trait ArrowReadableValue<'referred_data>: Sized {
    type OwnedReadableValue;

    fn get(array: &'referred_data Arc<dyn Array>, index: usize) -> Self;
    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        storage: &mut BlockStorage,
    );
    fn to_owned(self) -> Self::OwnedReadableValue;
}
