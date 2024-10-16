use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::ops::{Bound, RangeBounds};

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
use serde::de::Error as DeError;
use serde::ser::Error as SerError;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use super::delta::BlockDelta;

const ARROW_ALIGNMENT: usize = 64;

/// A RecordBatchWrapper looks like a record batch, but also implements serde's Serialize and
/// Deserialize.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct RecordBatchWrapper(pub RecordBatch);

impl std::ops::Deref for RecordBatchWrapper {
    type Target = RecordBatch;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for RecordBatchWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<RecordBatch> for RecordBatchWrapper {
    fn from(rb: RecordBatch) -> Self {
        Self(rb)
    }
}

impl Serialize for RecordBatchWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let data = Block::record_batch_to_bytes(self).map_err(S::Error::custom)?;
        serializer.serialize_bytes(&data)
    }
}

impl<'de> Deserialize<'de> for RecordBatchWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = Vec::<u8>::deserialize(deserializer)?;
        let reader = std::io::Cursor::new(data);
        let rb = Block::load_record_batch(reader, false).map_err(D::Error::custom)?;
        Ok(RecordBatchWrapper(rb))
    }
}

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
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Block {
    // The data is stored in an Arrow record batch with the column schema (prefix, key, value).
    // These are stored in sorted order by prefix and key for efficient lookups.
    pub data: RecordBatchWrapper,
    pub id: Uuid,
}

impl Block {
    /// Create a concrete block from an id and the underlying record batch of data
    pub fn from_record_batch(id: Uuid, data: RecordBatch) -> Self {
        let data = data.into();
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

            K::add_to_delta(prefix, key, value, &mut delta.builder);
        }
        delta
    }

    /// Binary searches this slice with a comparator function.
    ///
    /// The comparator function should return an order code that indicates
    /// whether its argument is `Less`, `Equal` or `Greater` the desired
    /// target.
    /// If the slice is not sorted or if the comparator function does not
    /// implement an order consistent with the sort order of the underlying
    /// slice, the returned result is unspecified and meaningless.
    ///
    /// If the value is found then [`Result::Ok`] is returned, containing the
    /// index of the matching element. If there are multiple matches, then any
    /// one of the matches could be returned. The index is chosen
    /// deterministically, but is subject to change in future versions of Rust.
    /// If the value is not found then [`Result::Err`] is returned, containing
    /// the index where a matching element could be inserted while maintaining
    /// sorted order.
    ///
    /// Based on std::slice::binary_search_by with minimal modifications (https://doc.rust-lang.org/src/core/slice/mod.rs.html#2770).
    #[inline]
    fn binary_search_by<'me, K: ArrowReadableKey<'me>, F>(
        &'me self,
        mut f: F,
    ) -> Result<usize, usize>
    where
        F: FnMut((&'me str, K)) -> Ordering,
    {
        let mut size = self.len();
        if size == 0 {
            return Err(0);
        }

        let prefix_array = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut base = 0;

        // This loop intentionally doesn't have an early exit if the comparison
        // returns Equal. We want the number of loop iterations to depend *only*
        // on the size of the input slice so that the CPU can reliably predict
        // the loop count.
        while size > 1 {
            let half = size / 2;
            let mid = base + half;

            // SAFETY: the call is made safe by the following inconstants:
            // - `mid >= 0`: by definition
            // - `mid < size`: `mid = size / 2 + size / 4 + size / 8 ...`
            let prefix = prefix_array.value(mid);
            let key = K::get(self.data.column(1), mid);
            let cmp = f((prefix, key));

            base = if cmp == Ordering::Greater { base } else { mid };

            // This is imprecise in the case where `size` is odd and the
            // comparison returns Greater: the mid element still gets included
            // by `size` even though it's known to be larger than the element
            // being searched for.
            //
            // This is fine though: we gain more performance by keeping the
            // loop iteration count invariant (and thus predictable) than we
            // lose from considering one additional element.
            size -= half;
        }

        // SAFETY: base is always in [0, size) because base <= mid.
        let prefix = prefix_array.value(base);
        let key = K::get(self.data.column(1), base);
        let cmp = f((prefix, key));
        if cmp == Ordering::Equal {
            Ok(base)
        } else {
            let result = base + (cmp == Ordering::Less) as usize;
            Err(result)
        }
    }

    /// Returns the largest index where `prefixes[index] == prefix` or None if the provided prefix does not exist in the block.
    #[inline]
    fn find_largest_index_of_prefix<'me, K: ArrowReadableKey<'me>>(
        &'me self,
        prefix: &str,
    ) -> Option<usize> {
        // By design, will never find an exact match (comparator never evaluates to Equal). This finds the index of the first element that is greater than the prefix. If no element is greater, it returns the length of the array.
        let result = self
            .binary_search_by::<K, _>(|(p, _)| match p.cmp(prefix) {
                Ordering::Less => Ordering::Less,
                Ordering::Equal => Ordering::Less,
                Ordering::Greater => Ordering::Greater,
            })
            .expect_err("Never returns Ok because the comparator never evaluates to Equal.");

        if result == 0 {
            // The first element is greater than the target prefix, so the target prefix does not exist in the block. (Or the block is empty.)
            return None;
        }

        let prefix_array = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // `result` is the first index where the prefix is larger than the input (or the length of the array) so we want one element before this.
        match prefix_array.value(result - 1).cmp(prefix) {
            // We're at the end of the array, so the prefix does not exist in the block (all values are less than the prefix)
            Ordering::Less => None,
            // The prefix exists
            Ordering::Equal => Some(result - 1),
            // This is impossible
            Ordering::Greater => None,
        }
    }

    /// Returns the smallest index where `prefixes[index] == prefix` or None if the provided prefix does not exist in the block.
    #[inline]
    fn find_smallest_index_of_prefix<'me, K: ArrowReadableKey<'me>>(
        &'me self,
        prefix: &str,
    ) -> Option<usize> {
        let result = self
            .binary_search_by::<K, _>(|(p, _)| match p.cmp(prefix) {
                Ordering::Less => Ordering::Less,
                Ordering::Equal => Ordering::Greater,
                Ordering::Greater => Ordering::Greater,
            })
            .expect_err("Never returns Ok because the comparator never evaluates to Equal.");

        let prefix_array = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        if result == self.len() {
            // The target prefix is greater than all elements in the block.
            return None;
        }

        match prefix_array.value(result).cmp(prefix) {
            Ordering::Greater => None,
            Ordering::Less => None,
            Ordering::Equal => Some(result),
        }
    }

    /// Finds the partition point of the prefix and key.
    /// Returns the index of the first element that matches the target prefix and key. If no element matches, returns the index at which the target prefix and key could be inserted to maintain sorted order.
    #[inline]
    fn binary_search_prefix_key<'me, K: ArrowReadableKey<'me>>(
        &'me self,
        prefix: &str,
        key: &K,
    ) -> usize {
        // By design, will never find an exact match (comparator never evaluates to Equal). This finds the index of the first element that matches the target prefix and key. If no element matches, it returns the index at which the target prefix and key could be inserted to maintain sorted order.
        self.binary_search_by::<K, _>(|(p, k)| {
            match p.cmp(prefix).then_with(|| {
                k.partial_cmp(key)
                    // The key type does not have a total order because of floating point values.
                    // But in our case NaN is not allowed, so we should always have total order.
                    .expect("Array values should be comparable.")
            }) {
                Ordering::Less => Ordering::Less,
                Ordering::Equal => Ordering::Greater,
                Ordering::Greater => Ordering::Greater,
            }
        })
        .expect_err("Never returns Ok because the comparator never evaluates to Equal.")
    }

    #[inline]
    fn match_prefix_key_at_index<'me, K: ArrowReadableKey<'me>>(
        &'me self,
        prefix: &str,
        key: &K,
        index: usize,
    ) -> bool {
        let prefix_array = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        index < self.len()
            && matches!(
                (
                    prefix_array.value(index).cmp(prefix),
                    K::get(self.data.column(1), index).partial_cmp(key),
                ),
                (Ordering::Equal, Some(Ordering::Equal))
            )
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
        match self.binary_search_by::<K, _>(|(p, k)| {
            p.cmp(prefix).then_with(|| {
                k.partial_cmp(&key)
                    // The key type does not have a total order because of floating point values.
                    // But in our case NaN is not allowed, so we should always have total order.
                    .expect("Array values should be comparable.")
            })
        }) {
            Ok(index) => Some(V::get(self.data.column(2), index)),
            Err(_) => None,
        }
    }

    /// Get all the values for a given prefix & key range in the block
    /// ### Panics
    /// - If the underlying data types are not the same as the types specified in the function signature
    /// - If at least one end of the prefix range is excluded (currently unsupported)
    pub fn get_range<
        'prefix,
        'me,
        K: ArrowReadableKey<'me>,
        V: ArrowReadableValue<'me>,
        PrefixRange,
        KeyRange,
    >(
        &'me self,
        prefix_range: PrefixRange,
        key_range: KeyRange,
    ) -> impl Iterator<Item = (K, V)> + 'me
    where
        PrefixRange: RangeBounds<&'prefix str>,
        KeyRange: RangeBounds<K>,
    {
        let start_index = match prefix_range.start_bound() {
            Bound::Included(prefix) => match key_range.start_bound() {
                Bound::Included(key) => self.binary_search_prefix_key(prefix, key),
                Bound::Excluded(key) => {
                    let index = self.binary_search_prefix_key(prefix, key);
                    if self.match_prefix_key_at_index(prefix, key, index) {
                        index + 1
                    } else {
                        index
                    }
                }
                Bound::Unbounded => match self.find_smallest_index_of_prefix::<K>(prefix) {
                    Some(first_index_of_prefix) => first_index_of_prefix,
                    None => self.len(), // prefix does not exist in the block so we shouldn't return anything
                },
            },
            Bound::Excluded(_) => {
                unimplemented!("Excluded prefix range is not currently supported")
            }
            Bound::Unbounded => 0,
        };

        let end_index = match prefix_range.end_bound() {
            Bound::Included(prefix) => match key_range.end_bound() {
                Bound::Included(key) => {
                    let index = self.binary_search_prefix_key(prefix, key);
                    if self.match_prefix_key_at_index(prefix, key, index) {
                        index + 1
                    } else {
                        index
                    }
                }
                Bound::Excluded(key) => self.binary_search_prefix_key::<K>(prefix, key),
                Bound::Unbounded => match self.find_largest_index_of_prefix::<K>(prefix) {
                    Some(last_index_of_prefix) => last_index_of_prefix + 1, // (add 1 because end_index is exclusive below)
                    None => start_index, // prefix does not exist in the block so we shouldn't return anything
                },
            },
            Bound::Excluded(_) => {
                unimplemented!("Excluded prefix range is not currently supported")
            }
            Bound::Unbounded => self.len(),
        };

        (start_index..end_index).map(move |index| {
            (
                K::get(self.data.column(1), index),
                V::get(self.data.column(2), index),
            )
        })
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
    ) -> Option<(&'me str, K, V)> {
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
    pub fn get_size(&self) -> usize {
        let mut total_size = 0;
        for column in self.data.columns() {
            let array_data = column.to_data();
            total_size += get_size_of_array_data(&array_data);
        }
        total_size
    }

    /// Returns the number of items in the block
    pub(crate) fn len(&self) -> usize {
        self.data.num_rows()
    }

    /// Returns a reference to metadata of the block if any is present
    /// ### Notes
    /// - The metadata is stored in the Arrow RB schema as custom metadata
    #[allow(dead_code)]
    pub(crate) fn metadata(&self) -> &HashMap<String, String> {
        let schema = self.data.schema_ref();
        schema.metadata()
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
                Ok(_) => Ok(()),
                Err(e) => Err(BlockSaveError::ArrowError(e)),
            },
            Err(e) => Err(BlockSaveError::ArrowError(e)),
        }
    }

    /// Convert the block to bytes in Arrow IPC format
    pub fn to_bytes(&self) -> Result<Vec<u8>, BlockToBytesError> {
        Self::record_batch_to_bytes(&self.data)
    }

    /// Convert the record batch to bytes in Arrow IPC format
    fn record_batch_to_bytes(rb: &RecordBatch) -> Result<Vec<u8>, BlockToBytesError> {
        let mut bytes = Vec::new();
        // Scope the writer so that it is dropped before we return the bytes
        {
            let mut writer = match arrow::ipc::writer::FileWriter::try_new(&mut bytes, &rb.schema())
            {
                Ok(writer) => writer,
                Err(e) => {
                    return Err(BlockToBytesError::ArrowError(e));
                }
            };
            match writer.write(rb) {
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
        Self::from_bytes_internal(bytes, id, false)
    }

    /// Load a block from bytes in Arrow IPC format with the given id and validate the layout
    /// ### Notes
    /// - This method should be used in tests to ensure that the layout of the IPC file is as expected
    /// - The validation is not performant and should not be used in production code
    pub fn from_bytes_with_validation(bytes: &[u8], id: Uuid) -> Result<Self, BlockLoadError> {
        Self::from_bytes_internal(bytes, id, true)
    }

    fn from_bytes_internal(bytes: &[u8], id: Uuid, validate: bool) -> Result<Self, BlockLoadError> {
        let cursor = std::io::Cursor::new(bytes);
        Self::load_with_reader(cursor, id, validate)
    }

    /// Load a block from the given path with the given id and validate the layout
    /// ### Notes
    /// - This method should be used in tests to ensure that the layout of the IPC file is as expected
    /// - The validation is not performant and should not be used in production code
    pub fn load_with_validation(path: &str, id: Uuid) -> Result<Self, BlockLoadError> {
        Self::load_internal(path, id, true)
    }

    /// Load a block from the given path with the given id
    pub fn load(path: &str, id: Uuid) -> Result<Self, BlockLoadError> {
        Self::load_internal(path, id, false)
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
        Self::load_with_reader(reader, id, validate)
    }

    fn load_with_reader<R>(reader: R, id: Uuid, validate: bool) -> Result<Self, BlockLoadError>
    where
        R: std::io::Read + std::io::Seek,
    {
        let batch = Self::load_record_batch(reader, validate)?;
        // TODO: how to store / hydrate id?
        Ok(Self::from_record_batch(id, batch))
    }

    fn load_record_batch<R>(mut reader: R, validate: bool) -> Result<RecordBatch, BlockLoadError>
    where
        R: std::io::Read + std::io::Seek,
    {
        if validate {
            verify_buffers_layout(&mut reader)
                .map_err(BlockLoadError::ArrowLayoutVerificationError)?;
        }

        let mut arrow_reader = arrow::ipc::reader::FileReader::try_new(&mut reader, None)
            .map_err(BlockLoadError::ArrowError)?;

        let batch = match arrow_reader.next() {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => {
                return Err(BlockLoadError::ArrowError(e));
            }
            None => {
                return Err(BlockLoadError::NoRecordBatches);
            }
        };
        Ok(batch)
    }
}

impl chroma_cache::Weighted for Block {
    fn weight(&self) -> usize {
        1
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
    total_size
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
    #[error(transparent)]
    BlockToBytesError(#[from] crate::arrow::block::types::BlockToBytesError),
    #[error(transparent)]
    CacheError(#[from] chroma_cache::CacheError),
}

impl ChromaError for BlockLoadError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockLoadError::IOError(_) => ErrorCodes::Internal,
            BlockLoadError::ArrowError(_) => ErrorCodes::Internal,
            BlockLoadError::ArrowLayoutVerificationError(_) => ErrorCodes::Internal,
            BlockLoadError::NoRecordBatches => ErrorCodes::Internal,
            BlockLoadError::BlockToBytesError(_) => ErrorCodes::Internal,
            BlockLoadError::CacheError(_) => ErrorCodes::Internal,
        }
    }
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
        // All errors are internal for this error type
        ErrorCodes::Internal
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
    reader
        .seek(SeekFrom::End(-10))
        .map_err(ArrowLayoutVerificationError::IOError)?;
    reader
        .read_exact(&mut footer_buffer)
        .map_err(ArrowLayoutVerificationError::IOError)?;

    let footer_len = read_footer_length(footer_buffer);
    let footer_len = footer_len.map_err(ArrowLayoutVerificationError::ArrowError)?;

    // read footer
    let mut footer_data = vec![0; footer_len];
    reader
        .seek(SeekFrom::End(-10 - footer_len as i64))
        .map_err(ArrowLayoutVerificationError::IOError)?;
    reader
        .read_exact(&mut footer_data)
        .map_err(ArrowLayoutVerificationError::IOError)?;
    let footer =
        root_as_footer(&footer_data).map_err(ArrowLayoutVerificationError::InvalidFlatbuffer)?;

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
