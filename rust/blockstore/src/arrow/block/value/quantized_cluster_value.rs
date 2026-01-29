use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, FixedSizeListArray, FixedSizeListBuilder, Float32Array, Float32Builder,
        ListArray, ListBuilder, PrimitiveArray, StructArray, UInt64Builder, UInt8Array,
        UInt8Builder,
    },
    datatypes::{ArrowPrimitiveType, DataType, Field, Fields, UInt64Type},
};
use chroma_types::{QuantizedCluster, QuantizedClusterOwned};

use crate::{
    arrow::{
        block::delta::{
            quantized_cluster_delta::QuantizedClusterDelta, BlockStorage, UnorderedBlockDelta,
        },
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::KeyWrapper,
    BlockfileWriterMutationOrdering,
};

const CENTER_COLUMN: usize = 0;
const CODES_COLUMN: usize = 1;
const IDS_COLUMN: usize = 2;
const VERSIONS_COLUMN: usize = 3;

fn get_list_slice<T: ArrowPrimitiveType>(list_arr: &ListArray, index: usize) -> &[T::Native] {
    let start = list_arr.value_offsets()[index] as usize;
    let end = list_arr.value_offsets()[index + 1] as usize;
    let values = list_arr
        .values()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("expected primitive array");
    &values.values()[start..end]
}

fn get_fixed_size_list_array(struct_array: &StructArray, column: usize) -> &FixedSizeListArray {
    struct_array
        .column(column)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("expected fixed size list array")
}

fn get_list_array(struct_array: &StructArray, column: usize) -> &ListArray {
    struct_array
        .column(column)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("expected list array")
}

#[derive(Clone)]
pub struct QuantizedClusterSizeTracker {
    pub cluster_count: usize,
    pub code_length: usize,
    pub dimension: usize,
    pub vector_count: usize,
}

pub struct QuantizedClusterArrowBuilder {
    center: FixedSizeListBuilder<Float32Builder>,
    codes: ListBuilder<FixedSizeListBuilder<UInt8Builder>>,
    ids: ListBuilder<UInt64Builder>,
    versions: ListBuilder<UInt64Builder>,
}

impl ArrowWriteableValue for QuantizedCluster<'_> {
    type ReadableValue<'data> = QuantizedCluster<'data>;
    type PreparedValue = QuantizedClusterOwned;
    type SizeTracker = QuantizedClusterSizeTracker;
    type ArrowBuilder = QuantizedClusterArrowBuilder;

    fn offset_size(_: usize) -> usize {
        unimplemented!("not used for custom delta storage")
    }

    fn validity_size(_: usize) -> usize {
        unimplemented!("not used for custom delta storage")
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockStorage) {
        match delta {
            BlockStorage::QuantizedClusterDelta(delta) => delta.add(prefix, key, value),
            _ => unreachable!("Invalid delta type for QuantizedCluster"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &UnorderedBlockDelta) {
        match &delta.builder {
            BlockStorage::QuantizedClusterDelta(delta) => delta.delete(prefix, key),
            _ => unreachable!("Invalid delta type for QuantizedCluster"),
        }
    }

    fn get_delta_builder(_: BlockfileWriterMutationOrdering) -> BlockStorage {
        BlockStorage::QuantizedClusterDelta(QuantizedClusterDelta::new())
    }

    fn get_arrow_builder(tracker: Self::SizeTracker) -> Self::ArrowBuilder {
        QuantizedClusterArrowBuilder {
            center: FixedSizeListBuilder::with_capacity(
                Float32Builder::with_capacity(tracker.cluster_count * tracker.dimension),
                tracker.dimension as i32,
                tracker.cluster_count,
            ),
            codes: ListBuilder::with_capacity(
                FixedSizeListBuilder::with_capacity(
                    UInt8Builder::with_capacity(tracker.vector_count * tracker.code_length),
                    tracker.code_length as i32,
                    tracker.vector_count,
                ),
                tracker.cluster_count,
            ),
            ids: ListBuilder::with_capacity(
                UInt64Builder::with_capacity(tracker.vector_count),
                tracker.cluster_count,
            ),
            versions: ListBuilder::with_capacity(
                UInt64Builder::with_capacity(tracker.vector_count),
                tracker.cluster_count,
            ),
        }
    }

    fn prepare(value: Self) -> Self::PreparedValue {
        QuantizedClusterOwned::from(value)
    }

    fn append(value: Self::PreparedValue, builder: &mut Self::ArrowBuilder) {
        builder.center.values().append_slice(&value.center);
        builder.center.append(true);

        let code_length = value.codes.len() / value.ids.len().max(1);
        let inner_codes = builder.codes.values();
        for chunk in value.codes.chunks(code_length) {
            inner_codes.values().append_slice(chunk);
            inner_codes.append(true);
        }
        builder.codes.append(true);

        builder.ids.values().append_slice(&value.ids);
        builder.ids.append(true);

        builder.versions.values().append_slice(&value.versions);
        builder.versions.append(true);
    }

    fn finish(
        mut builder: Self::ArrowBuilder,
        size_tracker: &Self::SizeTracker,
    ) -> (Field, Arc<dyn Array>) {
        let center_field = Field::new(
            "center",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                size_tracker.dimension as i32,
            ),
            true,
        );
        let codes_field = Field::new(
            "codes",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::UInt8, true)),
                    size_tracker.code_length as i32,
                ),
                true,
            ))),
            true,
        );
        let ids_field = Field::new(
            "ids",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            true,
        );
        let versions_field = Field::new(
            "versions",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            true,
        );

        let center_array = builder.center.finish();
        let codes_array = builder.codes.finish();
        let ids_array = builder.ids.finish();
        let versions_array = builder.versions.finish();

        let struct_array = StructArray::from(vec![
            (
                Arc::new(center_field.clone()),
                Arc::new(center_array) as ArrayRef,
            ),
            (
                Arc::new(codes_field.clone()),
                Arc::new(codes_array) as ArrayRef,
            ),
            (Arc::new(ids_field.clone()), Arc::new(ids_array) as ArrayRef),
            (
                Arc::new(versions_field.clone()),
                Arc::new(versions_array) as ArrayRef,
            ),
        ]);

        let struct_fields =
            Fields::from(vec![center_field, codes_field, ids_field, versions_field]);
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
            BlockStorage::QuantizedClusterDelta(delta) => delta.get_owned_value(prefix, key),
            _ => unreachable!("Invalid delta type for QuantizedCluster"),
        }
    }
}

impl<'data> ArrowReadableValue<'data> for QuantizedCluster<'data> {
    fn get(array: &'data Arc<dyn Array>, index: usize) -> Self {
        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("expected struct array");

        let center_arr = get_fixed_size_list_array(struct_array, CENTER_COLUMN);
        let codes_arr = get_list_array(struct_array, CODES_COLUMN);
        let ids_arr = get_list_array(struct_array, IDS_COLUMN);
        let versions_arr = get_list_array(struct_array, VERSIONS_COLUMN);

        // center: FixedSizeList<Float32>
        let center_start = center_arr.value_offset(index) as usize;
        let center_end = center_arr.value_offset(index + 1) as usize;
        let center_values = center_arr
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("expected float32 array");
        let center = &center_values.values()[center_start..center_end];

        // codes: List<FixedSizeList<UInt8>>
        let codes_outer_start = codes_arr.value_offsets()[index] as usize;
        let codes_outer_end = codes_arr.value_offsets()[index + 1] as usize;
        let codes_inner = codes_arr
            .values()
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("expected fixed size list array");
        let codes_start = codes_inner.value_offset(codes_outer_start) as usize;
        let codes_end = codes_inner.value_offset(codes_outer_end) as usize;
        let codes_values = codes_inner
            .values()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("expected uint8 array");
        let codes = &codes_values.values()[codes_start..codes_end];

        QuantizedCluster {
            center,
            codes,
            ids: get_list_slice::<UInt64Type>(ids_arr, index),
            versions: get_list_slice::<UInt64Type>(versions_arr, index),
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
        <QuantizedCluster as ArrowWriteableValue>::add(prefix, key.into(), value, storage);
    }
}
