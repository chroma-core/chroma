use crate::{
    arrow::{
        block::delta::{single_column_storage::SingleColumnStorage, BlockDelta, BlockStorage},
        types::{ArrowReadableValue, ArrowWriteableKey, ArrowWriteableValue},
    },
    key::KeyWrapper,
};
use arrow::{
    array::{Array, Int32Array, ListArray},
    util::bit_util,
};
use std::sync::Arc;

// impl ArrowWriteableValue for Int32Array {
//     type ReadableValue<'referred_data> = Int32Array;

//     fn offset_size(item_count: usize) -> usize {
//         bit_util::round_upto_multiple_of_64((item_count + 1) * 4)
//     }

//     fn validity_size(_item_count: usize) -> usize {
//         0 // We don't support None values for Int32Array
//     }

//     fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockDelta) {
//         match &delta.builder {
//             BlockStorage::Int32Array(builder) => {
//                 // We have to clone the value in this odd way here because when reading out of a block we get the entire array
//                 let mut new_vec = Vec::with_capacity(value.len());
//                 for i in 0..value.len() {
//                     new_vec.push(value.value(i));
//                 }
//                 let new_arr = Int32Array::from(new_vec);
//                 builder.add(prefix, key, new_arr);
//             }
//             _ => panic!("Invalid builder type"),
//         }
//     }

//     fn delete(prefix: &str, key: KeyWrapper, delta: &BlockDelta) {
//         match &delta.builder {
//             BlockStorage::Int32Array(builder) => {
//                 builder.delete(prefix, key);
//             }
//             _ => panic!("Invalid builder type"),
//         }
//     }

//     fn get_delta_builder() -> BlockStorage {
//         BlockStorage::Int32Array(SingleColumnStorage::new())
//     }
// }

// impl ArrowReadableValue<'_> for Int32Array {
//     fn get(array: &Arc<dyn Array>, index: usize) -> Self {
//         let arr = array
//             .as_any()
//             .downcast_ref::<ListArray>()
//             .unwrap()
//             .value(index);
//         // Cloning an arrow array is cheap, since they are immutable and backed by Arc'ed data
//         arr.as_any().downcast_ref::<Int32Array>().unwrap().clone()
//     }

//     fn add_to_delta<K: ArrowWriteableKey>(
//         prefix: &str,
//         key: K,
//         value: Self,
//         delta: &mut BlockDelta,
//     ) {
//         // delta.add(prefix, key, value.clone());
//     }
// }

impl ArrowWriteableValue for Vec<i32> {
    type ReadableValue<'referred_data> = Vec<i32>;

    fn offset_size(item_count: usize) -> usize {
        bit_util::round_upto_multiple_of_64((item_count + 1) * 4)
    }

    fn validity_size(_item_count: usize) -> usize {
        0 // We don't support None values for Int32Array
    }

    fn add(prefix: &str, key: KeyWrapper, value: Self, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::Int32Array(builder) => {
                // We have to clone the value in this odd way here because when reading out of a block we get the entire array
                // let mut new_vec = Vec::with_capacity(value.len());
                // for i in 0..value.len() {
                //     new_vec.push(value.value(i));
                // }
                // let new_arr = Int32Array::from(new_vec);
                builder.add(prefix, key, value);
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn delete(prefix: &str, key: KeyWrapper, delta: &BlockDelta) {
        match &delta.builder {
            BlockStorage::Int32Array(builder) => {
                builder.delete(prefix, key);
            }
            _ => panic!("Invalid builder type"),
        }
    }

    fn get_delta_builder() -> BlockStorage {
        BlockStorage::Int32Array(SingleColumnStorage::new())
    }
}

impl ArrowReadableValue<'_> for Vec<i32> {
    fn get(array: &Arc<dyn Array>, index: usize) -> Self {
        let arr = array
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(index);
        let arr_i32array = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        let mut value_arr: Vec<i32> = Vec::with_capacity(arr.len());
        for i in 0..arr.len() {
            value_arr.push(arr_i32array.value(i));
        }
        value_arr
    }

    fn add_to_delta<K: ArrowWriteableKey>(
        prefix: &str,
        key: K,
        value: Self,
        delta: &mut BlockDelta,
    ) {
        delta.add(prefix, key, value.clone());
    }
}
