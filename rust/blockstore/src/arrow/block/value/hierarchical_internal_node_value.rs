use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BinaryBuilder, ListArray, ListBuilder, PrimitiveArray,
        StructArray, UInt32Array, UInt32Builder,
    },
    datatypes::{DataType, Field, Fields, UInt32Type},
};
use chroma_types::hierarchical_spann::{HierarchicalInternalNode, HierarchicalInternalNodeOwned};

use crate::{
    arrow::{
        block::delta::{
            hierarchical_internal_node_delta::HierarchicalInternalNodeDelta, BlockStorage,
            UnorderedBlockDelta,
        },
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::KeyWrapper,
    BlockfileWriterMutationOrdering,
};

const PARENT_COLUMN: usize = 0;
const CENTROID_CODE_COLUMN: usize = 1;
const CHILDREN_COLUMN: usize = 2;

#[derive(Clone)]
pub struct HierarchicalInternalNodeSizeTracker {
    pub node_count: usize,
    pub total_code_bytes: usize,
    pub total_children: usize,
}

pub struct HierarchicalInternalNodeArrowBuilder {
    parent: UInt32Builder,
    centroid_code: BinaryBuilder,
    children: ListBuilder<UInt32Builder>,
}

impl ArrowWriteableValue for HierarchicalInternalNode<'_> {
    type ReadableValue<'data> = HierarchicalInternalNode<'data>;
    type PreparedValue = HierarchicalInternalNodeOwned;
    type SizeTracker = HierarchicalInternalNodeSizeTracker;
    type ArrowBuilder = HierarchicalInternalNodeArrowBuilder;

    fn offset_size(_: usize) -> usize {
        unimplemented!("not used for custom delta storage")
    }

    fn validity_size(_: usize) -> usize {
        unimplemented!("not used for custom delta storage")
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockStorage) {
        match delta {
            BlockStorage::HierarchicalInternalNodeDelta(d) => d.add(prefix, key, value),
            _ => unreachable!("Invalid delta type for HierarchicalInternalNode"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &UnorderedBlockDelta) {
        match &delta.builder {
            BlockStorage::HierarchicalInternalNodeDelta(d) => d.delete(prefix, key),
            _ => unreachable!("Invalid delta type for HierarchicalInternalNode"),
        }
    }

    fn get_delta_builder(_: BlockfileWriterMutationOrdering) -> BlockStorage {
        BlockStorage::HierarchicalInternalNodeDelta(HierarchicalInternalNodeDelta::new())
    }

    fn get_arrow_builder(tracker: Self::SizeTracker) -> Self::ArrowBuilder {
        HierarchicalInternalNodeArrowBuilder {
            parent: UInt32Builder::with_capacity(tracker.node_count),
            centroid_code: BinaryBuilder::with_capacity(
                tracker.node_count,
                tracker.total_code_bytes,
            ),
            children: ListBuilder::with_capacity(
                UInt32Builder::with_capacity(tracker.total_children),
                tracker.node_count,
            ),
        }
    }

    fn prepare(value: Self) -> Self::PreparedValue {
        HierarchicalInternalNodeOwned::from(value)
    }

    fn append(value: Self::PreparedValue, builder: &mut Self::ArrowBuilder) {
        builder.parent.append_value(value.parent);
        builder.centroid_code.append_value(&value.centroid_code);
        builder.children.values().append_slice(&value.children);
        builder.children.append(true);
    }

    fn finish(
        mut builder: Self::ArrowBuilder,
        _size_tracker: &Self::SizeTracker,
    ) -> (Field, Arc<dyn Array>) {
        let parent_field = Field::new("parent", DataType::UInt32, false);
        let centroid_code_field = Field::new("centroid_code", DataType::Binary, true);
        let children_field = Field::new(
            "children",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
            true,
        );

        let parent_array = builder.parent.finish();
        let centroid_code_array = builder.centroid_code.finish();
        let children_array = builder.children.finish();

        let struct_array = StructArray::from(vec![
            (
                Arc::new(parent_field.clone()),
                Arc::new(parent_array) as ArrayRef,
            ),
            (
                Arc::new(centroid_code_field.clone()),
                Arc::new(centroid_code_array) as ArrayRef,
            ),
            (
                Arc::new(children_field.clone()),
                Arc::new(children_array) as ArrayRef,
            ),
        ]);

        let struct_fields = Fields::from(vec![parent_field, centroid_code_field, children_field]);
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
            BlockStorage::HierarchicalInternalNodeDelta(d) => d.get_owned_value(prefix, key),
            _ => unreachable!("Invalid delta type for HierarchicalInternalNode"),
        }
    }
}

impl<'data> ArrowReadableValue<'data> for HierarchicalInternalNode<'data> {
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

        let centroid_code = struct_array
            .column(CENTROID_CODE_COLUMN)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("expected binary array for centroid_code")
            .value(index);

        let children_arr = struct_array
            .column(CHILDREN_COLUMN)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("expected list array for children");
        let children_start = children_arr.value_offsets()[index] as usize;
        let children_end = children_arr.value_offsets()[index + 1] as usize;
        let children = &children_arr
            .values()
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt32Type>>()
            .expect("expected uint32 array for children values")
            .values()[children_start..children_end];

        HierarchicalInternalNode {
            parent,
            centroid_code,
            children,
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
        <HierarchicalInternalNode as ArrowWriteableValue>::add(
            prefix,
            key.into(),
            value,
            storage,
        );
    }
}
