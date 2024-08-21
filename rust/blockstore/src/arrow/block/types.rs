use super::delta::BlockDelta;
use crate::arrow::types::{ArrowReadableKey, ArrowReadableValue};
use arrow::array::ArrayData;
use arrow::buffer::Buffer;
use arrow::ipc::reader::read_footer_length;
use arrow::ipc::{root_as_footer, root_as_message, MessageHeader, MetadataVersion};
use arrow::util::bit_util;
use arrow::{
    array::{Array, StringArray},
    record_batch::RecordBatch,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::Value;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::io::SeekFrom;
use thiserror::Error;
use uuid::Uuid;

const ARROW_ALIGNMENT: usize = 64;

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

impl Value for Block {
    fn size(&self) -> usize {
        self.get_size()
    }
}

impl Block {
    /// Create a concrete block from an id and the underlying record batch of data
    pub fn from_record_batch(id: Uuid, data: RecordBatch) -> Self {
        Self { id, data }
    }

    /// Converts the block to a block delta for writing to a new block
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

    fn binary_search<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me>>(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Option<V> {
        // Copied from std lib https://doc.rust-lang.org/src/core/slice/mod.rs.html#2786
        // and modified for two nested level comparisons.
        let mut size = self.len();
        let mut left = 0;
        let mut right = size;
        let prefix_arr = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        while left < right {
            let mid = left + size / 2;

            let prefix_cmp = prefix_arr.value(mid).cmp(prefix);
            // This control flow produces conditional moves, which results in
            // fewer branches and instructions than if/else or matching on
            // cmp::Ordering.
            // This is x86 asm for u8: https://rust.godbolt.org/z/698eYffTx.
            left = if prefix_cmp == Less { mid + 1 } else { left };
            right = if prefix_cmp == Greater { mid } else { right };
            if prefix_cmp == Equal {
                let key_cmp = K::get(self.data.column(1), mid)
                    .partial_cmp(&key)
                    .expect("NaN not expected"); // NaN not expected
                left = if key_cmp == Less { mid + 1 } else { left };
                right = if key_cmp == Greater { mid } else { right };
                if key_cmp == Equal {
                    return Some(V::get(self.data.column(2), mid));
                }
            }

            size = right - left;
        }
        None
    }

    fn binary_search_prefix<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me>>(
        &'me self,
        prefix: &str,
    ) -> Option<Vec<(&str, K, V)>> {
        let prefix_arr = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut size = prefix_arr.len();
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mid = left + size / 2;

            let predicate = prefix_arr.value(mid) < prefix;

            // This control flow produces conditional moves, which results in
            // fewer branches and instructions than if/else or matching on
            // boolean.
            // This is x86 asm for u8: https://rust.godbolt.org/z/698eYffTx.
            left = if predicate == true { mid + 1 } else { left };
            right = if predicate == false { mid } else { right };

            size = right - left;
        }

        let mut start_idx = left;
        let mut res = vec![];
        while start_idx < prefix_arr.len() && prefix_arr.value(start_idx) == prefix {
            res.push((
                prefix_arr.value(start_idx),
                K::get(self.data.column(1), start_idx),
                V::get(self.data.column(2), start_idx),
            ));
            start_idx += 1;
        }

        Some(res)
    }

    /*
        ===== Block Queries =====
    */

    /// Get the value for a given key in the block
    /// ### Panics
    /// - If the underlying data types are not the same as the types specified in the function signature
    pub fn get<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me>>(
        &'me self,
        prefix: &str,
        key: K,
    ) -> Option<V> {
        self.binary_search(prefix, key)
    }

    /// Get all the values for a given prefix in the block
    /// ### Panics
    /// - If the underlying data types are not the same as the types specified in the function signature
    pub fn get_prefix<'me, K: ArrowReadableKey<'me>, V: ArrowReadableValue<'me>>(
        &'me self,
        prefix: &str,
    ) -> Option<Vec<(&str, K, V)>> {
        self.binary_search_prefix(prefix)
    }

    /// Get all the values for a given prefix in the block where the key is greater than the given key
    /// ### Panics
    /// - If the underlying data types are not the same as the types specified in the function signature
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

    /// Get all the values for a given prefix in the block where the key is less than the given key
    /// ### Panics
    /// - If the underlying data types are not the same as the types specified in the function signature
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

    /// Get all the values for a given prefix in the block where the key is less than or equal to the given key
    /// ### Panics
    /// - If the underlying data types are not the same as the types specified in the function signature
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

    /// Get all the values for a given prefix in the block where the key is greater than or equal to the given key
    /// ### Panics
    /// - If the underlying data types are not the same as the types specified in the function signature
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

    /// Get all the values for a given prefix in the block where the key is between the given keys
    /// ### Notes
    /// - Returns a tuple of (prefix, key, value)
    /// - Returns None if the requested index is out of bounds
    /// ### Panics
    /// - If the underlying data types are not the same as the types specified in the function signature
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

    /*
        ===== Block Metadata =====
    */

    /// Returns the size of the block in bytes
    pub(crate) fn get_size(&self) -> usize {
        let mut total_size = 0;
        for column in self.data.columns() {
            let array_data = column.to_data();
            total_size += get_size_of_array_data(&array_data);
        }
        return total_size;
    }

    /// Returns the number of items in the block
    pub fn len(&self) -> usize {
        self.data.num_rows()
    }

    /*
        ===== Block Serialization =====
    */

    /// Save the block in Arrow IPC format to the given path
    pub fn save(&self, path: &str) -> Result<(), BlockSaveError> {
        let file = match std::fs::File::create(path) {
            Ok(file) => file,
            Err(e) => {
                return Err(BlockSaveError::IOError(e));
            }
        };

        // We force the block to be written with 64 byte alignment
        // this is the default, but we are just being defensive
        let mut writer = std::io::BufWriter::new(file);
        let options = match arrow::ipc::writer::IpcWriteOptions::try_new(
            ARROW_ALIGNMENT,
            false,
            MetadataVersion::V5,
        ) {
            Ok(options) => options,
            Err(e) => {
                return Err(BlockSaveError::ArrowError(e));
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
                return Err(BlockSaveError::ArrowError(e));
            }
        };
        match writer.write(&self.data) {
            Ok(_) => match writer.finish() {
                Ok(_) => return Ok(()),
                Err(e) => {
                    return Err(BlockSaveError::ArrowError(e));
                }
            },
            Err(e) => {
                return Err(BlockSaveError::ArrowError(e));
            }
        }
    }

    /// Convert the block to bytes in Arrow IPC format
    pub fn to_bytes(&self) -> Result<Vec<u8>, BlockToBytesError> {
        let mut bytes = Vec::new();
        // Scope the writer so that it is dropped before we return the bytes
        {
            let mut writer =
                match arrow::ipc::writer::FileWriter::try_new(&mut bytes, &self.data.schema()) {
                    Ok(writer) => writer,
                    Err(e) => {
                        return Err(BlockToBytesError::ArrowError(e));
                    }
                };
            match writer.write(&self.data) {
                Ok(_) => {}
                Err(e) => {
                    return Err(BlockToBytesError::ArrowError(e));
                }
            }
            match writer.finish() {
                Ok(_) => {}
                Err(e) => {
                    return Err(BlockToBytesError::ArrowError(e));
                }
            }
        }
        Ok(bytes)
    }

    /// Load a block from bytes in Arrow IPC format with the given id
    pub fn from_bytes(bytes: &[u8], id: Uuid) -> Result<Self, BlockLoadError> {
        return Self::from_bytes_internal(bytes, id, false);
    }

    /// Load a block from bytes in Arrow IPC format with the given id and validate the layout
    /// ### Notes
    /// - This method should be used in tests to ensure that the layout of the IPC file is as expected
    /// - The validation is not performant and should not be used in production code
    pub fn from_bytes_with_validation(bytes: &[u8], id: Uuid) -> Result<Self, BlockLoadError> {
        return Self::from_bytes_internal(bytes, id, true);
    }

    fn from_bytes_internal(bytes: &[u8], id: Uuid, validate: bool) -> Result<Self, BlockLoadError> {
        let cursor = std::io::Cursor::new(bytes);
        return Self::load_with_reader(cursor, id, validate);
    }

    /// Load a block from the given path with the given id and validate the layout
    /// ### Notes
    /// - This method should be used in tests to ensure that the layout of the IPC file is as expected
    /// - The validation is not performant and should not be used in production code
    pub fn load_with_validation(path: &str, id: Uuid) -> Result<Self, BlockLoadError> {
        return Self::load_internal(path, id, true);
    }

    /// Load a block from the given path with the given id
    pub fn load(path: &str, id: Uuid) -> Result<Self, BlockLoadError> {
        return Self::load_internal(path, id, false);
    }

    fn load_internal(path: &str, id: Uuid, validate: bool) -> Result<Self, BlockLoadError> {
        let file = std::fs::File::open(path);
        let file = match file {
            Ok(file) => file,
            Err(e) => {
                return Err(BlockLoadError::IOError(e));
            }
        };
        let reader = std::io::BufReader::new(file);
        return Self::load_with_reader(reader, id, validate);
    }

    fn load_with_reader<R>(mut reader: R, id: Uuid, validate: bool) -> Result<Self, BlockLoadError>
    where
        R: std::io::Read + std::io::Seek,
    {
        if validate {
            let res = verify_buffers_layout(&mut reader);
            match res {
                Ok(_) => {}
                Err(e) => {
                    return Err(BlockLoadError::ArrowLayoutVerificationError(e));
                }
            }
        }

        let mut arrow_reader = match arrow::ipc::reader::FileReader::try_new(&mut reader, None) {
            Ok(arrow_reader) => arrow_reader,
            Err(e) => {
                return Err(BlockLoadError::ArrowError(e));
            }
        };

        let batch = match arrow_reader.next() {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => {
                return Err(BlockLoadError::ArrowError(e));
            }
            None => {
                return Err(BlockLoadError::NoRecordBatches);
            }
        };

        // TODO: how to store / hydrate id?
        Ok(Self::from_record_batch(id, batch))
    }
}

fn get_size_of_array_data(array_data: &ArrayData) -> usize {
    let mut total_size = 0;
    for buffer in array_data.buffers() {
        // SYSTEM ASSUMPTION: ALL BUFFERS ARE PADDED TO 64 bytes
        // We maintain this invariant in three places
        // 1. In the to_arrow methods of delta storage, we allocate
        // padded buffers
        // 2. In calls to load() in tests we validate that the buffers are of size 64
        // 3. In writing to the IPC block file we use an option ensure 64 byte alignment
        // which makes the arrow writer add padding to the buffers
        // Why do we do this instead of using get_buffer_memory_size()
        // or using the buffers capacity?
        // The reason is that arrow can dramatically overreport the size of buffers
        // if the underlying buffers are shared. If we use something like get_buffer_memory_size()
        // or capacity. This is because the buffer may be shared with other arrays.
        // In the case of Arrow IPC data, all the data is one buffer
        // so get_buffer_memory_size() would overreport the size of the buffer
        // by the number of columns and also by the number of validity, and offset buffers.
        // This is why we use the buffer.len() method which gives us the actual size of the buffer
        // however len() excludes the capacity of the buffer which is why we round up to the nearest
        // multiple of 64 bytes. We ensure, both when we construct the buffer and when we write it to disk
        // that the buffer is also block.len() + padding of 64 bytes exactly.
        // (As an added note, arrow throws away explicit knowledge of this padding,
        // see verify_buffers_layout() for how we infer the padding based on
        // the offsets of each buffer)
        let size = bit_util::round_upto_multiple_of_64(buffer.len());
        total_size += size;
    }
    // List and Struct arrays have child arrays
    for child in array_data.child_data() {
        total_size += get_size_of_array_data(child);
    }
    // Some data types (like our data record) have null buffers
    if let Some(buffer) = array_data.nulls() {
        let size = bit_util::round_upto_multiple_of_64(buffer.len());
        total_size += size;
    }
    return total_size;
}

/*
===== ErrorTypes =====
*/

#[derive(Error, Debug)]
pub enum BlockSaveError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),
}

impl ChromaError for BlockSaveError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockSaveError::IOError(_) => ErrorCodes::Internal,
            BlockSaveError::ArrowError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum BlockToBytesError {
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),
}

impl ChromaError for BlockToBytesError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockToBytesError::ArrowError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum BlockLoadError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    ArrowLayoutVerificationError(#[from] ArrowLayoutVerificationError),
    #[error("No record batches in IPC file")]
    NoRecordBatches,
}

/*
===== Layout Verification =====
*/

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
    #[error("No record batches in footer")]
    NoRecordBatches,
    #[error("More than one record batch in IPC file")]
    MultipleRecordBatches,
    #[error("Invalid message type")]
    InvalidMessageType,
    #[error("Error decoding record batch message as record batch")]
    RecordBatchDecodeError,
}

impl ChromaError for ArrowLayoutVerificationError {
    fn code(&self) -> ErrorCodes {
        match self {
            // All errors are internal for this error type
            _ => ErrorCodes::Internal,
        }
    }
}

/// Verifies that the buffers in the IPC file are 64 byte aligned
/// and stored in Arrow in the way we expect.
/// All non-benchmark test code should use this by loading the block
/// with verification enabled.
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
                let len = (curr_offset - prev_offset) as usize;
                if len % ARROW_ALIGNMENT != 0 {
                    return Err(ArrowLayoutVerificationError::BufferLengthNotAligned);
                }
                prev_offset = curr_offset;
            }
            // Check the remaining buffer length based on the body length
            let last_buffer_len = record_batch_body_len - prev_offset as usize;
            if last_buffer_len % ARROW_ALIGNMENT != 0 {
                return Err(ArrowLayoutVerificationError::BufferLengthNotAligned);
            }
        }
        _ => {
            return Err(ArrowLayoutVerificationError::InvalidMessageType);
        }
    }

    Ok(())
}
