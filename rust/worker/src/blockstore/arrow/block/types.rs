use super::delta::BlockDelta;
use crate::blockstore::arrow::types::{ArrowReadableKey, ArrowReadableValue};
use crate::errors::{ChromaError, ErrorCodes};
use arrow::array::ArrayData;
use arrow::buffer::Buffer;
use arrow::ipc::reader::read_footer_length;
use arrow::ipc::{root_as_footer, root_as_message, MessageHeader, MetadataVersion};
use arrow::util::bit_util;
use arrow::{
    array::{Array, StringArray},
    record_batch::RecordBatch,
};
use std::io::SeekFrom;
use thiserror::Error;
use uuid::Uuid;

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
#[derive(Clone, Debug)]
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

    pub fn get_prefix<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me>>(
        &'me self,
        prefix: &str,
    ) -> Option<Vec<(&str, K, V)>> {
        let prefix_array = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut res: Vec<(&str, K, V)> = vec![];
        for i in 0..self.data.num_rows() {
            let curr_prefix = prefix_array.value(i);
            if curr_prefix == prefix {
                res.push((
                    curr_prefix,
                    K::get(self.data.column(1), i),
                    V::get(self.data.column(2), i),
                ));
            }
        }
        return Some(res);
    }

    pub fn get_gt<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me>>(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Option<Vec<(&str, K, V)>> {
        let prefix_array = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut res: Vec<(&str, K, V)> = vec![];
        for i in 0..self.data.num_rows() {
            let curr_prefix = prefix_array.value(i);
            let curr_key = K::get(self.data.column(1), i);
            if curr_prefix == prefix && curr_key > key {
                res.push((curr_prefix, curr_key, V::get(self.data.column(2), i)));
            }
        }
        return Some(res);
    }

    pub fn get_lt<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me>>(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Option<Vec<(&str, K, V)>> {
        let prefix_array = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut res: Vec<(&str, K, V)> = vec![];
        for i in 0..self.data.num_rows() {
            let curr_prefix = prefix_array.value(i);
            let curr_key = K::get(self.data.column(1), i);
            if curr_prefix == prefix && curr_key < key {
                res.push((curr_prefix, curr_key, V::get(self.data.column(2), i)));
            }
        }
        return Some(res);
    }

    pub fn get_lte<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me>>(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Option<Vec<(&str, K, V)>> {
        let prefix_array = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut res: Vec<(&str, K, V)> = vec![];
        for i in 0..self.data.num_rows() {
            let curr_prefix = prefix_array.value(i);
            let curr_key = K::get(self.data.column(1), i);
            if curr_prefix == prefix && curr_key <= key {
                res.push((curr_prefix, curr_key, V::get(self.data.column(2), i)));
            }
        }
        return Some(res);
    }

    pub fn get_gte<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me>>(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Option<Vec<(&str, K, V)>> {
        let prefix_array = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut res: Vec<(&str, K, V)> = vec![];
        for i in 0..self.data.num_rows() {
            let curr_prefix = prefix_array.value(i);
            let curr_key = K::get(self.data.column(1), i);
            if curr_prefix == prefix && curr_key >= key {
                res.push((curr_prefix, curr_key, V::get(self.data.column(2), i)));
            }
        }
        return Some(res);
    }

    pub fn get_at_index<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me>>(
        &'me self,
        index: usize,
    ) -> Option<(&str, K, V)> {
        if index >= self.data.num_rows() {
            return None;
        }
        let prefix_arr = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let prefix = prefix_arr.value(index);
        let key = K::get(self.data.column(1), index);
        let value = V::get(self.data.column(2), index);
        Some((prefix, key, value))
    }

    /// Returns the size of the block in bytes
    pub(crate) fn get_size(&self) -> usize {
        let mut total_size = 0;
        let mut alt_size = 0;
        for column in self.data.columns() {
            let array_data = column.to_data();
            total_size += get_size_of_array_data(&array_data);

            let column_buffer_size = column.get_buffer_memory_size();
            let alt_column_size = get_size_of_array_data(&array_data);
            alt_size += column_buffer_size;
            let alt_column_size = get_size_of_array_data(&array_data);
            println!(
                "Column buffer size: {} vs {}",
                column_buffer_size, alt_column_size
            );
        }
        println!("Total size: {} vs {}", total_size, alt_size);
        return total_size;
    }

    /// Returns the number of items in the block
    pub fn len(&self) -> usize {
        self.data.num_rows()
    }

    pub fn save(&self, path: &str) -> Result<(), Box<dyn ChromaError>> {
        let file = match std::fs::File::create(path) {
            Ok(file) => file,
            Err(e) => {
                // TODO: Return a proper error
                panic!("Error creating file: {:?}", e)
            }
        };
        let mut writer = std::io::BufWriter::new(file);
        let options =
            match arrow::ipc::writer::IpcWriteOptions::try_new(64, false, MetadataVersion::V5) {
                Ok(options) => options,
                Err(e) => {
                    panic!("Error creating options: {:?}", e);
                }
            };

        let writer = arrow::ipc::writer::FileWriter::try_new_with_options(
            &mut writer,
            &self.data.schema(),
            options,
        );
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

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        // Scope the writer so that it is dropped before we return the bytes
        {
            let mut writer =
                arrow::ipc::writer::FileWriter::try_new(&mut bytes, &self.data.schema())
                    .expect("Error creating writer");
            writer.write(&self.data).expect("Error writing data");
            writer.finish().expect("Error finishing writer");
        }
        bytes
    }

    pub fn from_bytes(bytes: &[u8], id: Uuid) -> Result<Self, Box<dyn ChromaError>> {
        return Self::from_bytes_internal(bytes, id, false);
    }

    pub fn from_bytes_with_validation(
        bytes: &[u8],
        id: Uuid,
    ) -> Result<Self, Box<dyn ChromaError>> {
        return Self::from_bytes_internal(bytes, id, true);
    }

    fn from_bytes_internal(
        bytes: &[u8],
        id: Uuid,
        validate: bool,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let cursor = std::io::Cursor::new(bytes);
        return Self::load_with_reader(cursor, id, validate);
    }

    pub fn load_with_validation(path: &str, id: Uuid) -> Result<Self, Box<dyn ChromaError>> {
        return Self::load_internal(path, id, true);
    }

    pub fn load(path: &str, id: Uuid) -> Result<Self, Box<dyn ChromaError>> {
        return Self::load_internal(path, id, false);
    }

    fn load_internal(path: &str, id: Uuid, validate: bool) -> Result<Self, Box<dyn ChromaError>> {
        let file = std::fs::File::open(path);
        let file = match file {
            Ok(file) => file,
            Err(e) => {
                // TODO: Return a proper error
                panic!("Error opening file: {:?}", e)
            }
        };
        let reader = std::io::BufReader::new(file);
        return Self::load_with_reader(reader, id, validate);
    }

    fn load_with_reader<R>(
        mut reader: R,
        id: Uuid,
        validate: bool,
    ) -> Result<Self, Box<dyn ChromaError>>
    where
        R: std::io::Read + std::io::Seek,
    {
        if validate {
            let res = verify_buffers_layout(&mut reader);
            match res {
                Ok(_) => {}
                Err(e) => {
                    return Err(Box::new(e));
                }
            }
        }

        let mut arrow_reader = arrow::ipc::reader::FileReader::try_new(&mut reader, None)
            .expect("Error creating reader");

        let batch = arrow_reader.next().unwrap();
        // TODO: how to store / hydrate id?
        match batch {
            Ok(batch) => Ok(Self::from_record_batch(id, batch)),
            Err(e) => {
                panic!("Error reading batch: {:?}", e);
            }
        }
    }
}

fn get_size_of_array_data(array_data: &ArrayData) -> usize {
    let mut total_size = 0;
    for buffer in array_data.buffers() {
        // SYSTEM ASSUMPTION: ALL BUFFERS ARE PADDED TO 64 bytes
        // We maintain this invariant in two places
        // 1. In the to_arrow methods of delta storage, we allocate
        // padded buffers
        // 2. In block load() we validate that the buffers are of size 64
        // Why do we do this instead of using get_buffer_memory_size()
        // or using the buffers capacity? TODO: answer
        let size = bit_util::round_upto_multiple_of_64(buffer.len());
        total_size += size;
    }
    // List and Struct arrays have child arrays
    for child in array_data.child_data() {
        total_size += get_size_of_array_data(child);
    }
    // Some data types have null buffers
    if let Some(buffer) = array_data.nulls() {
        let size = bit_util::round_upto_multiple_of_64(buffer.len());
        total_size += size;
    }
    return total_size;
}

#[derive(Error, Debug)]
pub enum ArrowLayoutVerificationError {
    #[error("Buffer length is not 64 byte aligned")]
    BufferLengthNotAligned,
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    InvalidFlatbuffer(#[from] flatbuffers::InvalidFlatbuffer),
    #[error("No schema in footer")]
    NoSchema,
    #[error("No record batches in footer")]
    NoRecordBatches,
    #[error("More than one record batch in IPC file")]
    MultipleRecordBatches,
    #[error("Invalid message type")]
    InvalidMessageType,
    #[error("Error decoding record batch message as record batch")]
    RecordBatchDecodeError,
    #[error("Record batch has no buffer blocks")]
    NoBufferBlocks,
}

impl ChromaError for ArrowLayoutVerificationError {
    fn code(&self) -> ErrorCodes {
        match self {
            ArrowLayoutVerificationError::BufferLengthNotAligned => ErrorCodes::Internal,
            ArrowLayoutVerificationError::IOError(_) => ErrorCodes::Internal,
            ArrowLayoutVerificationError::ArrowError(_) => ErrorCodes::Internal,
            ArrowLayoutVerificationError::InvalidFlatbuffer(_) => ErrorCodes::Internal,
            ArrowLayoutVerificationError::NoSchema => ErrorCodes::Internal,
            ArrowLayoutVerificationError::NoRecordBatches => ErrorCodes::Internal,
            ArrowLayoutVerificationError::MultipleRecordBatches => ErrorCodes::Internal,
            ArrowLayoutVerificationError::InvalidMessageType => ErrorCodes::Internal,
            ArrowLayoutVerificationError::RecordBatchDecodeError => ErrorCodes::Internal,
            ArrowLayoutVerificationError::NoBufferBlocks => ErrorCodes::Internal,
        }
    }
}

fn verify_buffers_layout<R>(mut reader: R) -> Result<(), ArrowLayoutVerificationError>
where
    R: std::io::Read + std::io::Seek,
{
    // Read the IPC file and verify that the buffers are 64 byte aligned
    // by inspecting the offsets, this is required since our
    // size calculation assumes that the buffers are 64 byte aligned
    // Space for ARROW_MAGIC (6 bytes) and length (4 bytes)
    let mut footer_buffer = [0; 10];
    match reader.seek(SeekFrom::End(-10)) {
        Ok(_) => {}
        Err(e) => {
            return Err(ArrowLayoutVerificationError::IOError(e));
        }
    }

    match reader.read_exact(&mut footer_buffer) {
        Ok(_) => {}
        Err(e) => {
            return Err(ArrowLayoutVerificationError::IOError(e));
        }
    }

    let footer_len = read_footer_length(footer_buffer);
    let footer_len = match footer_len {
        Ok(footer_len) => footer_len,
        Err(e) => {
            return Err(ArrowLayoutVerificationError::ArrowError(e));
        }
    };

    // read footer
    let mut footer_data = vec![0; footer_len];
    match reader.seek(SeekFrom::End(-10 - footer_len as i64)) {
        Ok(_) => {}
        Err(e) => {
            return Err(ArrowLayoutVerificationError::IOError(e));
        }
    }
    match reader.read_exact(&mut footer_data) {
        Ok(_) => {}
        Err(e) => {
            return Err(ArrowLayoutVerificationError::IOError(e));
        }
    }

    let footer = match root_as_footer(&footer_data) {
        Ok(footer) => footer,
        Err(e) => {
            return Err(ArrowLayoutVerificationError::InvalidFlatbuffer(e));
        }
    };

    // Read the record batch
    let record_batch_definitions = match footer.recordBatches() {
        Some(record_batch_definitions) => record_batch_definitions,
        None => {
            return Err(ArrowLayoutVerificationError::NoRecordBatches);
        }
    };

    // Ensure there is only ONE record batch, which is how we store data
    if record_batch_definitions.len() != 1 {
        return Err(ArrowLayoutVerificationError::MultipleRecordBatches);
    }

    let record_batch_definition = record_batch_definitions.get(0);
    let record_batch_len = record_batch_definition.bodyLength() as usize
        + record_batch_definition.metaDataLength() as usize;
    let record_batch_body_len = record_batch_definition.bodyLength() as usize;

    // Read the actual record batch
    let mut file_buffer = vec![0; record_batch_len];
    match reader.seek(SeekFrom::Start(record_batch_definition.offset() as u64)) {
        Ok(_) => {}
        Err(e) => {
            return Err(ArrowLayoutVerificationError::IOError(e));
        }
    }
    match reader.read_exact(&mut file_buffer) {
        Ok(_) => {}
        Err(e) => {
            return Err(ArrowLayoutVerificationError::IOError(e));
        }
    }
    let buffer = Buffer::from(file_buffer);

    // This is borrowed from arrow-ipc parse_message.rs
    // https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
    let buf = match buffer[..4] == [0xff; 4] {
        true => &buffer[8..],
        false => &buffer[4..],
    };
    let message = match root_as_message(buf) {
        Ok(message) => message,
        Err(e) => {
            return Err(ArrowLayoutVerificationError::InvalidFlatbuffer(e));
        }
    };

    match message.header_type() {
        MessageHeader::RecordBatch => {
            let record_batch = match message.header_as_record_batch() {
                Some(record_batch) => record_batch,
                None => {
                    return Err(ArrowLayoutVerificationError::RecordBatchDecodeError);
                }
            };
            // Loop over offsets and ensure the lengths of each buffer are 64 byte aligned
            let blocks = match record_batch.buffers() {
                Some(blocks) => blocks,
                None => {
                    return Err(ArrowLayoutVerificationError::RecordBatchDecodeError);
                }
            };

            let mut prev_offset = blocks.get(0).offset();
            for block in blocks.iter().skip(1) {
                let curr_offset = block.offset();
                let len = curr_offset - prev_offset;
                if len % 64 != 0 {
                    return Err(ArrowLayoutVerificationError::BufferLengthNotAligned);
                }
                prev_offset = curr_offset;
            }
            // Check the remaining buffer length based on the body length
            let last_buffer_len = record_batch_body_len - prev_offset as usize;
            if last_buffer_len % 64 != 0 {
                return Err(ArrowLayoutVerificationError::BufferLengthNotAligned);
            }
        }
        _ => {
            return Err(ArrowLayoutVerificationError::InvalidMessageType);
        }
    }

    Ok(())
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

// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::blockstore::types::Key;
//     use arrow::array::Int32Array;

//     #[test]
//     fn test_block_builder_can_add() {
//         let num_entries = 1000;

//         let mut keys = Vec::new();
//         let mut key_bytes = 0;
//         for i in 0..num_entries {
//             keys.push(Key::String(format!("{:04}", i)));
//             key_bytes += i.to_string().len();
//         }

//         let prefix = "key".to_string();
//         let prefix_bytes = prefix.len() * num_entries;
//         let mut block_builder = BlockDataBuilder::new(
//             KeyType::String,
//             ValueType::Int32Array,
//             Some(BlockBuilderOptions::new(
//                 num_entries,
//                 prefix_bytes,
//                 key_bytes,
//                 num_entries,         // 2 int32s per entry
//                 num_entries * 2 * 4, // 2 int32s per entry
//             )),
//         );

//         for i in 0..num_entries {
//             block_builder
//                 .add(
//                     BlockfileKey::new(prefix.clone(), keys[i].clone()),
//                     Value::Int32ArrayValue(Int32Array::from(vec![i as i32, (i + 1) as i32])),
//                 )
//                 .unwrap();
//         }

//         // Basic sanity check
//         let block_data = block_builder.build().unwrap();
//         assert_eq!(block_data.data.column(0).len(), num_entries);
//         assert_eq!(block_data.data.column(1).len(), num_entries);
//         assert_eq!(block_data.data.column(2).len(), num_entries);
//     }

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
