use std::sync::Arc;

use arrow::{
    array::{Array, ListArray, PrimitiveArray, StructArray},
    datatypes::{ArrowPrimitiveType, Float32Type, UInt64Type, UInt8Type},
};
use chroma_types::QuantizedCluster;

use crate::arrow::{
    block::delta::BlockStorage,
    types::{ArrowReadableValue, ArrowWriteableKey},
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
        todo!("Implement after ArrowWriteableValue")
    }
}
