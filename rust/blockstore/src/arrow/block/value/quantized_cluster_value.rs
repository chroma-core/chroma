use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, Float32Builder, ListArray, ListBuilder, PrimitiveArray, StructArray,
        UInt64Builder, UInt8Builder,
    },
    datatypes::{ArrowPrimitiveType, DataType, Field, Fields, Float32Type, UInt64Type, UInt8Type},
};
use chroma_types::{QuantizedCluster, QuantizedClusterOwned};

use crate::{
    arrow::{
        block::delta::{BlockStorage, UnorderedBlockDelta},
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::KeyWrapper,
    BlockfileWriterMutationOrdering,
};

const CENTER_COLUMN: usize = 0;
const CODES_COLUMN: usize = 1;
const IDS_COLUMN: usize = 2;
const VERSIONS_COLUMN: usize = 3;

fn get_list_slice<'data, T: ArrowPrimitiveType>(
    list_arr: &'data ListArray,
    index: usize,
) -> &'data [T::Native] {
    let start = list_arr.value_offsets()[index] as usize;
    let end = list_arr.value_offsets()[index + 1] as usize;
    let values = list_arr
        .values()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("expected primitive array");
    &values.values()[start..end]
}

fn get_list_array(struct_array: &StructArray, column: usize) -> &ListArray {
    struct_array
        .column(column)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("expected list array")
}

pub struct QuantizedClusterSizeTracker {
    pub cluster_count: usize,
    pub code_length: usize,
    pub dimension: usize,
    pub vector_count: usize,
}

pub struct QuantizedClusterArrowBuilder {
    center: ListBuilder<Float32Builder>,
    codes: ListBuilder<UInt8Builder>,
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

    fn add(_prefix: &str, _key: KeyWrapper, _value: Self, _delta: &BlockStorage) {
        todo!("Implement after QuantizedClusterDelta")
    }

    fn delete(_prefix: &str, _key: KeyWrapper, _delta: &UnorderedBlockDelta) {
        todo!("Implement after QuantizedClusterDelta")
    }

    fn get_delta_builder(_: BlockfileWriterMutationOrdering) -> BlockStorage {
        todo!("Implement after QuantizedClusterDelta")
    }

    fn get_arrow_builder(tracker: Self::SizeTracker) -> Self::ArrowBuilder {
        QuantizedClusterArrowBuilder {
            center: ListBuilder::with_capacity(
                Float32Builder::with_capacity(tracker.cluster_count * tracker.dimension),
                tracker.cluster_count,
            ),
            codes: ListBuilder::with_capacity(
                UInt8Builder::with_capacity(tracker.vector_count * tracker.code_length),
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

        builder.codes.values().append_slice(&value.codes);
        builder.codes.append(true);

        builder.ids.values().append_slice(&value.ids);
        builder.ids.append(true);

        builder.versions.values().append_slice(&value.versions);
        builder.versions.append(true);
    }

    fn finish(
        mut builder: Self::ArrowBuilder,
        _size_tracker: &Self::SizeTracker,
    ) -> (Field, Arc<dyn Array>) {
        let center_field = Field::new(
            "center",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        );
        let codes_field = Field::new(
            "codes",
            DataType::List(Arc::new(Field::new("item", DataType::UInt8, true))),
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
        _prefix: &str,
        _key: KeyWrapper,
        _delta: &UnorderedBlockDelta,
    ) -> Option<Self::PreparedValue> {
        todo!("Implement after QuantizedClusterDelta")
    }
}

impl<'data> ArrowReadableValue<'data> for QuantizedCluster<'data> {
    fn get(array: &'data Arc<dyn Array>, index: usize) -> Self {
        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("expected struct array");

        let center_arr = get_list_array(struct_array, CENTER_COLUMN);
        let codes_arr = get_list_array(struct_array, CODES_COLUMN);
        let ids_arr = get_list_array(struct_array, IDS_COLUMN);
        let versions_arr = get_list_array(struct_array, VERSIONS_COLUMN);

        QuantizedCluster {
            center: get_list_slice::<Float32Type>(center_arr, index),
            codes: get_list_slice::<UInt8Type>(codes_arr, index),
            ids: get_list_slice::<UInt64Type>(ids_arr, index),
            versions: get_list_slice::<UInt64Type>(versions_arr, index),
        }
    }

    fn get_range(array: &'data Arc<dyn Array>, offset: usize, length: usize) -> Vec<Self> {
        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("expected struct array");

        let center_arr = get_list_array(struct_array, CENTER_COLUMN);
        let codes_arr = get_list_array(struct_array, CODES_COLUMN);
        let ids_arr = get_list_array(struct_array, IDS_COLUMN);
        let versions_arr = get_list_array(struct_array, VERSIONS_COLUMN);

        (offset..offset + length)
            .map(|i| QuantizedCluster {
                center: get_list_slice::<Float32Type>(center_arr, i),
                codes: get_list_slice::<UInt8Type>(codes_arr, i),
                ids: get_list_slice::<UInt64Type>(ids_arr, i),
                versions: get_list_slice::<UInt64Type>(versions_arr, i),
            })
            .collect()
    }

    fn add_to_delta<K: ArrowWriteableKey>(
        _prefix: &str,
        _key: K,
        _value: Self,
        _storage: &mut BlockStorage,
    ) {
        todo!("Implement after QuantizedClusterDelta")
    }
}
