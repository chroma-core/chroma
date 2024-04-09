// use crate::blockstore::types::{BlockfileKey, Key, KeyType, Value, ValueType};
use super::delta::{BlockDeltaKey, BlockDeltaValue};
use crate::blockstore::key::{CompositeKey, KeyWrapper};
use crate::errors::{ChromaError, ErrorCodes};
use arrow::array::{
    ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Float32Array,
    Float32Builder, UInt32Array, UInt32Builder,
};
use arrow::{
    array::{Array, Int32Array, Int32Builder, ListArray, ListBuilder, StringArray, StringBuilder},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use uuid::Uuid;
// use parking_lot::RwLock;
use std::io::Error;
use std::sync::Arc;
// use std::sync::Arc;
use thiserror::Error;
// use uuid::Uuid;

// use super::delta::BlockDelta;
// use super::iterator::BlockIterator;

// /// BlockState represents the state of a block in the blockstore. Conceptually, a block is immutable once the broarder system
// /// has been made aware of its existence. New blocks may exist locally but are not considered part of the blockstore until they
// /// are registered.
// /// ## State transitions
// /// The state of a block is as follows:
// /// - Uninitialized: The block has been created but no data has been added
// /// - Initialized: Data has been added to the block but it has not been committed
// /// - Commited: The block has been committed and is ready to be registered. At this point the block is immutable
// /// - Registered: The block has been registered and is now part of the blockstore
// #[derive(Clone, Copy)]
// pub enum BlockState {
//     Uninitialized,
//     Initialized,
//     Commited,
//     Registered,
// }

/// A block in a blockfile. A block is a sorted collection of data that is immutable once it has been committed.
/// Blocks are the fundamental unit of storage in the blockstore and are used to store data in the form of (key, value) pairs.
/// These pairs are stored in an Arrow record batch with the schema (prefix, key, value).
/// Blocks are created in an uninitialized state and are transitioned to an initialized state once data has been added. Once
/// committed, a block is immutable and cannot be modified. Blocks are registered with the blockstore once they have been
/// flushed.
///
/// ### BlockData Notes
/// A Block holds BlockData via its Inner. Conceptually, the BlockData being loaded into memory is an optimization. The Block interface
/// could also support out of core operations where the BlockData is loaded from disk on demand. Currently we force operations to be in-core
/// but could expand to out-of-core in the future.
#[derive(Clone)]
pub struct Block {
    // The data is stored in an Arrow record batch with the column schema (prefix, key, value).
    // These are stored in sorted order by prefix and key for efficient lookups.
    pub(super) data: RecordBatch,
    pub id: Uuid,
}

impl Block {
    pub fn from_record_batch(id: Uuid, data: RecordBatch) -> Self {
        Self { id, data }
    }

    // pub fn get(&self, query_key: &BlockfileKey) -> Option<Value> {
    //     match &self.inner.read().data {
    //         Some(data) => {
    //             let prefix = data.data.column(0);
    //             let key = data.data.column(1);
    //             let value = data.data.column(2);
    //             // TODO: This should be binary search
    //             for i in 0..prefix.len() {
    //                 if prefix
    //                     .as_any()
    //                     .downcast_ref::<StringArray>()
    //                     .unwrap()
    //                     .value(i)
    //                     == query_key.prefix
    //                 {
    //                     let key_matches = match &query_key.key {
    //                         Key::String(inner_key) => {
    //                             inner_key
    //                                 == key.as_any().downcast_ref::<StringArray>().unwrap().value(i)
    //                         }
    //                         Key::Float(inner_key) => {
    //                             *inner_key
    //                                 == key
    //                                     .as_any()
    //                                     .downcast_ref::<Float32Array>()
    //                                     .unwrap()
    //                                     .value(i)
    //                         }
    //                         Key::Bool(inner_key) => {
    //                             *inner_key
    //                                 == key
    //                                     .as_any()
    //                                     .downcast_ref::<BooleanArray>()
    //                                     .unwrap()
    //                                     .value(i)
    //                         }
    //                         Key::Uint(inner_key) => {
    //                             *inner_key
    //                                 == key.as_any().downcast_ref::<UInt32Array>().unwrap().value(i)
    //                                     as u32
    //                         }
    //                     };
    //                     if key_matches {
    //                         match self.get_value_type() {
    //                             ValueType::Int32Array => {
    //                                 return Some(Value::Int32ArrayValue(
    //                                     value
    //                                         .as_any()
    //                                         .downcast_ref::<ListArray>()
    //                                         .unwrap()
    //                                         .value(i)
    //                                         .as_any()
    //                                         .downcast_ref::<Int32Array>()
    //                                         .unwrap()
    //                                         .clone(),
    //                                 ))
    //                             }
    //                             ValueType::String => {
    //                                 return Some(Value::StringValue(
    //                                     value
    //                                         .as_any()
    //                                         .downcast_ref::<StringArray>()
    //                                         .unwrap()
    //                                         .value(i)
    //                                         .to_string(),
    //                                 ))
    //                             }
    //                             ValueType::RoaringBitmap => {
    //                                 let bytes = value
    //                                     .as_any()
    //                                     .downcast_ref::<BinaryArray>()
    //                                     .unwrap()
    //                                     .value(i);
    //                                 let bitmap = roaring::RoaringBitmap::deserialize_from(bytes);
    //                                 match bitmap {
    //                                     Ok(bitmap) => {
    //                                         return Some(Value::RoaringBitmapValue(bitmap))
    //                                     }
    //                                     // TODO: log error
    //                                     Err(_) => return None,
    //                                 }
    //                             }
    //                             ValueType::Uint => {
    //                                 return Some(Value::UintValue(
    //                                     value
    //                                         .as_any()
    //                                         .downcast_ref::<UInt32Array>()
    //                                         .unwrap()
    //                                         .value(i),
    //                                 ))
    //                             }
    //                             // TODO: Add support for other types
    //                             _ => unimplemented!(),
    //                         }
    //                     }
    //                 }
    //             }
    //             None
    //         }
    //         None => None,
    //     }
    // }

    /// Returns the size of the block in bytes
    pub(crate) fn get_size(&self) -> usize {
        let mut total_size = 0;
        for column in self.data.columns() {
            total_size += column.get_buffer_memory_size();
        }
        total_size
    }

    /// Returns the number of items in the block
    pub fn len(&self) -> usize {
        self.data.num_rows()
    }
}

// #[derive(Error, Debug)]
// pub enum FinishError {
//     #[error("Arrow error")]
//     ArrowError(#[from] arrow::error::ArrowError),
// }

// impl ChromaError for FinishError {
//     fn code(&self) -> ErrorCodes {
//         match self {
//             FinishError::ArrowError(_) => ErrorCodes::Internal,
//         }
//     }
// }

// // #[cfg(test)]
// // mod test {
// //     use super::*;
// //     use crate::blockstore::types::Key;
// //     use arrow::array::Int32Array;

// //     #[test]
// //     fn test_block_builder_can_add() {
// //         let num_entries = 1000;

// //         let mut keys = Vec::new();
// //         let mut key_bytes = 0;
// //         for i in 0..num_entries {
// //             keys.push(Key::String(format!("{:04}", i)));
// //             key_bytes += i.to_string().len();
// //         }

// //         let prefix = "key".to_string();
// //         let prefix_bytes = prefix.len() * num_entries;
// //         let mut block_builder = BlockDataBuilder::new(
// //             KeyType::String,
// //             ValueType::Int32Array,
// //             Some(BlockBuilderOptions::new(
// //                 num_entries,
// //                 prefix_bytes,
// //                 key_bytes,
// //                 num_entries,         // 2 int32s per entry
// //                 num_entries * 2 * 4, // 2 int32s per entry
// //             )),
// //         );

// //         for i in 0..num_entries {
// //             block_builder
// //                 .add(
// //                     BlockfileKey::new(prefix.clone(), keys[i].clone()),
// //                     Value::Int32ArrayValue(Int32Array::from(vec![i as i32, (i + 1) as i32])),
// //                 )
// //                 .unwrap();
// //         }

// //         // Basic sanity check
// //         let block_data = block_builder.build().unwrap();
// //         assert_eq!(block_data.data.column(0).len(), num_entries);
// //         assert_eq!(block_data.data.column(1).len(), num_entries);
// //         assert_eq!(block_data.data.column(2).len(), num_entries);
// //     }

// //     #[test]
// //     fn test_out_of_order_key_fails() {
// //         let mut block_builder = BlockDataBuilder::new(
// //             KeyType::String,
// //             ValueType::Int32Array,
// //             Some(BlockBuilderOptions::default()),
// //         );

// //         block_builder
// //             .add(
// //                 BlockfileKey::new("key".to_string(), Key::String("b".to_string())),
// //                 Value::Int32ArrayValue(Int32Array::from(vec![1, 2])),
// //             )
// //             .unwrap();

// //         let result = block_builder.add(
// //             BlockfileKey::new("key".to_string(), Key::String("a".to_string())),
// //             Value::Int32ArrayValue(Int32Array::from(vec![1, 2])),
// //         );

// //         match result {
// //             Ok(_) => panic!("Expected error"),
// //             Err(e) => {
// //                 assert_eq!(e.code(), ErrorCodes::InvalidArgument);
// //             }
// //         }
// //     }
// // }
