use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BinaryArray, BinaryBuilder, StructArray, UInt32Array, UInt32Builder},
    datatypes::{DataType, Field, Fields},
};
use chroma_types::hierarchical_spann::{HierarchicalLeafNode, HierarchicalLeafNodeOwned};

use crate::{
    arrow::{
        block::delta::{
            hierarchical_leaf_node_delta::HierarchicalLeafNodeDelta, BlockStorage,
            UnorderedBlockDelta,
        },
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::KeyWrapper,
    BlockfileWriterMutationOrdering,
};

const PARENT_COLUMN: usize = 0;
const LENGTH_COLUMN: usize = 1;
const CENTROID_CODE_COLUMN: usize = 2;

#[derive(Clone)]
pub struct HierarchicalLeafNodeSizeTracker {
    pub node_count: usize,
    pub total_code_bytes: usize,
}

pub struct HierarchicalLeafNodeArrowBuilder {
    parent: UInt32Builder,
    length: UInt32Builder,
    centroid_code: BinaryBuilder,
}

impl ArrowWriteableValue for HierarchicalLeafNode<'_> {
    type ReadableValue<'data> = HierarchicalLeafNode<'data>;
    type PreparedValue = HierarchicalLeafNodeOwned;
    type SizeTracker = HierarchicalLeafNodeSizeTracker;
    type ArrowBuilder = HierarchicalLeafNodeArrowBuilder;

    fn offset_size(_: usize) -> usize {
        unimplemented!("not used for custom delta storage")
    }

    fn validity_size(_: usize) -> usize {
        unimplemented!("not used for custom delta storage")
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockStorage) {
        match delta {
            BlockStorage::HierarchicalLeafNodeDelta(d) => d.add(prefix, key, value),
            _ => unreachable!("Invalid delta type for HierarchicalLeafNode"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &UnorderedBlockDelta) {
        match &delta.builder {
            BlockStorage::HierarchicalLeafNodeDelta(d) => d.delete(prefix, key),
            _ => unreachable!("Invalid delta type for HierarchicalLeafNode"),
        }
    }

    fn get_delta_builder(_: BlockfileWriterMutationOrdering) -> BlockStorage {
        BlockStorage::HierarchicalLeafNodeDelta(HierarchicalLeafNodeDelta::new())
    }

    fn get_arrow_builder(tracker: Self::SizeTracker) -> Self::ArrowBuilder {
        HierarchicalLeafNodeArrowBuilder {
            parent: UInt32Builder::with_capacity(tracker.node_count),
            length: UInt32Builder::with_capacity(tracker.node_count),
            centroid_code: BinaryBuilder::with_capacity(
                tracker.node_count,
                tracker.total_code_bytes,
            ),
        }
    }

    fn prepare(value: Self) -> Self::PreparedValue {
        HierarchicalLeafNodeOwned::from(value)
    }

    fn append(value: Self::PreparedValue, builder: &mut Self::ArrowBuilder) {
        builder.parent.append_value(value.parent);
        builder.length.append_value(value.length);
        builder.centroid_code.append_value(&value.centroid_code);
    }

    fn finish(
        mut builder: Self::ArrowBuilder,
        _size_tracker: &Self::SizeTracker,
    ) -> (Field, Arc<dyn Array>) {
        let parent_field = Field::new("parent", DataType::UInt32, false);
        let length_field = Field::new("length", DataType::UInt32, false);
        let centroid_code_field = Field::new("centroid_code", DataType::Binary, true);

        let parent_array = builder.parent.finish();
        let length_array = builder.length.finish();
        let centroid_code_array = builder.centroid_code.finish();

        let struct_array = StructArray::from(vec![
            (
                Arc::new(parent_field.clone()),
                Arc::new(parent_array) as ArrayRef,
            ),
            (
                Arc::new(length_field.clone()),
                Arc::new(length_array) as ArrayRef,
            ),
            (
                Arc::new(centroid_code_field.clone()),
                Arc::new(centroid_code_array) as ArrayRef,
            ),
        ]);

        let struct_fields = Fields::from(vec![parent_field, length_field, centroid_code_field]);
        let value_field = Field::new("value", DataType::Struct(struct_fields), true);
        let value_arr = (&struct_array as &dyn Array).slice(0, struct_array.len());

        (value_field, value_arr)
    }

    fn get_owned_value_from_delta(
        prefix: &str,
        key: KeyWrapper,
        delta: &UnorderedBlockDelta,
    ) -> Option<Self::PreparedValue> {
        match &delta.builder {
            BlockStorage::HierarchicalLeafNodeDelta(d) => d.get_owned_value(prefix, key),
            _ => unreachable!("Invalid delta type for HierarchicalLeafNode"),
        }
    }
}

impl<'data> ArrowReadableValue<'data> for HierarchicalLeafNode<'data> {
    fn get(array: &'data Arc<dyn Array>, index: usize) -> Self {
        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("expected struct array");

        let parent = struct_array
            .column(PARENT_COLUMN)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("expected uint32 array for parent")
            .value(index);

        let length = struct_array
            .column(LENGTH_COLUMN)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("expected uint32 array for length")
            .value(index);

        let centroid_code = struct_array
            .column(CENTROID_CODE_COLUMN)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("expected binary array for centroid_code")
            .value(index);

        HierarchicalLeafNode {
            parent,
            length,
            centroid_code,
        }
    }

    fn get_range(array: &'data Arc<dyn Array>, offset: usize, length: usize) -> Vec<Self> {
        (offset..offset + length)
            .map(|i| Self::get(array, i))
            .collect()
    }

    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        storage: &mut BlockStorage,
    ) {
        <HierarchicalLeafNode as ArrowWriteableValue>::add(prefix, key.into(), value, storage);
    }
}
