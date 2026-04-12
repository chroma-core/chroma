use std::sync::Arc;

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
    array::{Array, AsArray, BinaryArray, BinaryBuilder},
    datatypes::Field,
    util::bit_util,
};
use chroma_types::SparsePostingBlock;

impl ArrowWriteableValue for SparsePostingBlock {
    type ReadableValue<'referred_data> = SparsePostingBlock;
    type ArrowBuilder = BinaryBuilder;
    type SizeTracker = SingleColumnSizeTracker;
    type PreparedValue = Vec<u8>;

    fn offset_size(item_count: usize) -> usize {
        bit_util::round_upto_multiple_of_64((item_count + 1) * 4)
    }

    fn validity_size(_item_count: usize) -> usize {
        0
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockStorage) {
        match delta {
            BlockStorage::SparsePostingBlock(builder) => {
                builder.add(prefix, key, value);
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &UnorderedBlockDelta) {
        match &delta.builder {
            BlockStorage::SparsePostingBlock(builder) => {
                builder.delete(prefix, key);
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn get_delta_builder(mutation_ordering_hint: BlockfileWriterMutationOrdering) -> BlockStorage {
        BlockStorage::SparsePostingBlock(SingleColumnStorage::new(mutation_ordering_hint))
    }

    fn get_arrow_builder(size_tracker: Self::SizeTracker) -> Self::ArrowBuilder {
        BinaryBuilder::with_capacity(size_tracker.get_num_items(), size_tracker.get_value_size())
    }

    fn prepare(value: Self) -> Self::PreparedValue {
        value.serialize()
    }

    fn append(value: Self::PreparedValue, builder: &mut Self::ArrowBuilder) {
        builder.append_value(value);
    }

    fn finish(mut builder: Self::ArrowBuilder, _: &Self::SizeTracker) -> (Field, Arc<dyn Array>) {
        let value_field = Field::new("value", arrow::datatypes::DataType::Binary, true);
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
            BlockStorage::SparsePostingBlock(builder) => builder.get_owned_value(prefix, key),
            _ => panic!("Invalid builder type"),
        }
    }
}

impl ArrowReadableValue<'_> for SparsePostingBlock {
    fn get(array: &Arc<dyn Array>, index: usize) -> Self {
        let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
        let bytes = arr.value(index);
        SparsePostingBlock::deserialize(bytes)
            .expect("SparsePostingBlock: corrupt or truncated block bytes in blockfile")
    }

    fn get_range(array: &Arc<dyn Array>, offset: usize, length: usize) -> Vec<Self> {
        array
            .as_binary::<i32>()
            .slice(offset, length)
            .iter()
            .map(|data| {
                SparsePostingBlock::deserialize(
                    data.expect("SparsePostingBlock value should not be null"),
                )
                .expect("SparsePostingBlock: corrupt or truncated block bytes in blockfile")
            })
            .collect()
    }

    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        storage: &mut BlockStorage,
    ) {
        SparsePostingBlock::add(prefix, key.into(), value, storage);
    }
}
