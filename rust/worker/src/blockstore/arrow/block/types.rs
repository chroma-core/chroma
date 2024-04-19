// use crate::blockstore::types::{BlockfileKey, Key, KeyType, Value, ValueType};
use super::delta::BlockDelta;
use crate::blockstore::arrow::types::{ArrowReadableKey, ArrowReadableValue, ArrowWriteableKey};
use crate::blockstore::key::{CompositeKey, KeyWrapper};
use crate::blockstore::Key;
use crate::errors::{ChromaError, ErrorCodes};
use arrow::array::{
    ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Float32Array,
    Float32Builder, StructArray, UInt32Array, UInt32Builder,
};
use arrow::{
    array::{Array, Int32Array, Int32Builder, ListArray, ListBuilder, StringArray, StringBuilder},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
// use proptest::bits::usize;
use uuid::Uuid;
// use parking_lot::RwLock;
use std::io::Error;
use std::sync::Arc;
// use std::sync::Arc;
use thiserror::Error;
// use uuid::Uuid;

// use super::delta::BlockDelta;
// use super::iterator::BlockIterator;

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
    pub data: RecordBatch,
    pub id: Uuid,
}

impl Block {
    pub fn from_record_batch(id: Uuid, data: RecordBatch) -> Self {
        Self { id, data }
    }

    pub fn to_block_delta<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me>>(
        &'me self,
        mut delta: BlockDelta,
    ) -> BlockDelta {
        let prefix_arr = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..self.data.num_rows() {
            let prefix = prefix_arr.value(i);
            let key = K::get(self.data.column(1), i);
            let value = V::get(self.data.column(2), i);

            K::add_to_delta(prefix, key, value, &mut delta);
        }
        delta
    }

    pub fn get<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me>>(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Option<V> {
        let prefix_arr = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..self.data.num_rows() {
            let curr_prefix = prefix_arr.value(i);
            let curr_key = K::get(self.data.column(1), i);
            if curr_prefix == prefix && curr_key == key {
                return Some(V::get(self.data.column(2), i));
            }
        }
        None
    }

    /// Returns the size of the block in bytes
    pub(crate) fn get_size(&self) -> usize {
        let mut total_size = 0;
        let mut i = 0;
        for column in self.data.columns() {
            total_size += column.get_buffer_memory_size();
            i += 1;
        }

        // debug
        let value = self
            .data
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        i = 0;
        for column in value.columns() {
            println!(
                "Struct Column {}, size: {:?}",
                i,
                column.get_buffer_memory_size()
            );
            column.nulls().map_or((), |nulls| {
                println!("Nulls size: {:?}", nulls.buffer().capacity());
            });
            let mut j = 0;
            for buffer in column.to_data().buffers() {
                println!(
                    "Struct Column {}, buffer {}, size: {:?}",
                    i,
                    j,
                    buffer.capacity(),
                );
                j += 1;
            }
            i += 1;
        }

        total_size
    }

    /// Returns the number of items in the block
    pub fn len(&self) -> usize {
        self.data.num_rows()
    }

    pub fn save(&self, path: &str) -> Result<(), Box<dyn ChromaError>> {
        let file = std::fs::File::create(path);
        let mut file = match file {
            Ok(file) => file,
            Err(e) => {
                // TODO: Return a proper error
                panic!("Error creating file: {:?}", e)
            }
        };
        let mut writer = std::io::BufWriter::new(file);
        let writer = arrow::ipc::writer::FileWriter::try_new(&mut writer, &self.data.schema());
        let mut writer = match writer {
            Ok(writer) => writer,
            Err(e) => {
                // TODO: Return a proper error
                panic!("Error creating writer: {:?}", e)
            }
        };
        match writer.write(&self.data) {
            Ok(_) => match writer.finish() {
                Ok(_) => return Ok(()),
                Err(e) => {
                    panic!("Error finishing writer: {:?}", e);
                }
            },
            Err(e) => {
                panic!("Error writing data: {:?}", e);
            }
        }
    }

    pub fn load(path: &str) -> Result<Self, Box<dyn ChromaError>> {
        let file = std::fs::File::open(path);
        let file = match file {
            Ok(file) => file,
            Err(e) => {
                // TODO: Return a proper error
                panic!("Error opening file: {:?}", e)
            }
        };
        let mut reader = std::io::BufReader::new(file);
        let reader = arrow::ipc::reader::FileReader::try_new(&mut reader, None);
        let mut reader = match reader {
            Ok(reader) => reader,
            Err(e) => {
                // TODO: Return a proper error
                panic!("Error creating reader: {:?}", e)
            }
        };
        let batch = reader.next().unwrap();
        // TODO: how to store / hydrate id?
        match batch {
            Ok(batch) => Ok(Self::from_record_batch(Uuid::new_v4(), batch)),
            Err(e) => {
                panic!("Error reading batch: {:?}", e);
            }
        }
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
