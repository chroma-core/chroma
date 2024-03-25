use crate::blockstore::types::{BlockfileKey, Key, KeyType, Value, ValueType};
use crate::chroma_proto;
use crate::errors::{ChromaError, ErrorCodes};
use crate::types::{EmbeddingRecord, MetadataValue, UpdateMetadataValue};
use arrow::array::{
    ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Float32Array,
    Float32Builder, GenericByteBuilder, StructArray, StructBuilder, UInt32Array, UInt32Builder,
    UInt8Builder,
};
use arrow::datatypes::Fields;
use arrow::{
    array::{Array, Int32Array, Int32Builder, ListArray, ListBuilder, StringArray, StringBuilder},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use num_bigint::BigInt;
use parking_lot::RwLock;
use prost::Message;
use std::io::Error;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

use super::delta::BlockDelta;
use super::iterator::BlockIterator;

/// BlockState represents the state of a block in the blockstore. Conceptually, a block is immutable once the broarder system
/// has been made aware of its existence. New blocks may exist locally but are not considered part of the blockstore until they
/// are registered.
/// ## State transitions
/// The state of a block is as follows:
/// - Uninitialized: The block has been created but no data has been added
/// - Initialized: Data has been added to the block but it has not been committed
/// - Commited: The block has been committed and is ready to be registered. At this point the block is immutable
/// - Registered: The block has been registered and is now part of the blockstore
#[derive(Clone, Copy)]
pub enum BlockState {
    Uninitialized,
    Initialized,
    Commited,
    Registered,
}

pub(super) struct Inner {
    pub(super) id: Uuid,
    pub(super) data: Option<BlockData>,
    pub(super) state: BlockState,
    pub(super) key_type: KeyType,
    pub(super) value_type: ValueType,
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
#[derive(Clone)]
pub struct Block {
    pub(super) inner: Arc<RwLock<Inner>>,
}

#[derive(Error, Debug)]
pub enum BlockError {
    #[error("Invalid state transition")]
    InvalidStateTransition,
    #[error("Block data error")]
    BlockDataError(#[from] BlockDataBuildError),
}

impl ChromaError for BlockError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockError::InvalidStateTransition => ErrorCodes::Internal,
            BlockError::BlockDataError(e) => e.code(),
        }
    }
}

impl Block {
    pub fn new(id: Uuid, key_type: KeyType, value_type: ValueType) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                id,
                data: None,
                state: BlockState::Uninitialized,
                key_type,
                value_type,
            })),
        }
    }

    pub fn get(&self, query_key: &BlockfileKey) -> Option<Value> {
        match &self.inner.read().data {
            Some(data) => {
                let prefix = data.data.column(0);
                let key = data.data.column(1);
                let value = data.data.column(2);
                // TODO: This should be binary search
                for i in 0..prefix.len() {
                    if prefix
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(i)
                        == query_key.prefix
                    {
                        let key_matches = match &query_key.key {
                            Key::String(inner_key) => {
                                inner_key
                                    == key.as_any().downcast_ref::<StringArray>().unwrap().value(i)
                            }
                            Key::Float(inner_key) => {
                                *inner_key
                                    == key
                                        .as_any()
                                        .downcast_ref::<Float32Array>()
                                        .unwrap()
                                        .value(i)
                            }
                            Key::Bool(inner_key) => {
                                *inner_key
                                    == key
                                        .as_any()
                                        .downcast_ref::<BooleanArray>()
                                        .unwrap()
                                        .value(i)
                            }
                            Key::Uint(inner_key) => {
                                *inner_key
                                    == key.as_any().downcast_ref::<UInt32Array>().unwrap().value(i)
                                        as u32
                            }
                        };
                        if key_matches {
                            match self.get_value_type() {
                                ValueType::Int32Array => {
                                    return Some(Value::Int32ArrayValue(
                                        value
                                            .as_any()
                                            .downcast_ref::<ListArray>()
                                            .unwrap()
                                            .value(i)
                                            .as_any()
                                            .downcast_ref::<Int32Array>()
                                            .unwrap()
                                            .clone(),
                                    ))
                                }
                                ValueType::String => {
                                    return Some(Value::StringValue(
                                        value
                                            .as_any()
                                            .downcast_ref::<StringArray>()
                                            .unwrap()
                                            .value(i)
                                            .to_string(),
                                    ))
                                }
                                ValueType::RoaringBitmap => {
                                    let bytes = value
                                        .as_any()
                                        .downcast_ref::<BinaryArray>()
                                        .unwrap()
                                        .value(i);
                                    let bitmap = roaring::RoaringBitmap::deserialize_from(bytes);
                                    match bitmap {
                                        Ok(bitmap) => {
                                            return Some(Value::RoaringBitmapValue(bitmap))
                                        }
                                        // TODO: log error
                                        Err(_) => return None,
                                    }
                                }
                                ValueType::Uint => {
                                    return Some(Value::UintValue(
                                        value
                                            .as_any()
                                            .downcast_ref::<UInt32Array>()
                                            .unwrap()
                                            .value(i),
                                    ))
                                }
                                ValueType::EmbeddingRecord => {
                                    let records =
                                        value.as_any().downcast_ref::<StructArray>().unwrap();
                                    let id = records
                                        .column(0)
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                        .unwrap()
                                        .value(i)
                                        .to_string();
                                    let document = match records
                                        .column(1)
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                    {
                                        Some(document) => Some(document.value(i).to_string()),
                                        None => None,
                                    };
                                    let metadata = match records
                                        .column(2)
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                    {
                                        Some(metadata) => Some(metadata.value(i).to_string()),
                                        None => None,
                                    };
                                    return Some(Value::EmbeddingRecordValue(EmbeddingRecord {
                                        id,
                                        seq_id: BigInt::from(0), // TODO: THIS IS WRONG, WE NEED A NEW TYPE
                                        operation: crate::types::Operation::Add, // TODO: THIS IS WRONG, WE NEED A NEW TYPE
                                        embedding: Some(vec![1.0, 2.0, 3.0]), // TODO: populate this
                                        encoding: None,                       // TODO: populate this
                                        metadata: None,
                                        collection_id: Uuid::parse_str(
                                            "00000000-0000-0000-0000-000000000000",
                                        )
                                        .unwrap(),
                                    }));
                                }
                                // TODO: Add support for other types
                                _ => unimplemented!(),
                            }
                        }
                    }
                }
                None
            }
            None => None,
        }
    }

    /// Returns the size of the block in bytes
    pub fn get_size(&self) -> usize {
        match &self.inner.read().data {
            Some(data) => data.get_size(),
            None => 0,
        }
    }

    /// Returns the number of items in the block
    pub fn len(&self) -> usize {
        match &self.inner.read().data {
            Some(data) => data.data.column(0).len(),
            None => 0,
        }
    }

    pub fn get_id(&self) -> Uuid {
        self.inner.read().id
    }

    pub fn get_key_type(&self) -> KeyType {
        self.inner.read().key_type
    }

    pub fn get_value_type(&self) -> ValueType {
        self.inner.read().value_type
    }

    pub fn get_state(&self) -> BlockState {
        self.inner.read().state
    }

    /// Marks a block as commited. A commited block is immutable and is eligbile to be flushed and registered.
    pub fn commit(&self) -> Result<(), Box<BlockError>> {
        let mut inner = self.inner.write();
        match inner.state {
            BlockState::Uninitialized => Ok(()),
            BlockState::Initialized => {
                inner.state = BlockState::Commited;
                Ok(())
            }
            BlockState::Commited | BlockState::Registered => {
                Err(Box::new(BlockError::InvalidStateTransition))
            }
        }
    }

    pub fn apply_delta(&self, delta: &BlockDelta) -> Result<(), Box<BlockError>> {
        let data = match BlockData::try_from(delta) {
            Ok(data) => data,
            Err(e) => return Err(Box::new(BlockError::BlockDataError(e))),
        };
        let mut inner = self.inner.write();
        match inner.state {
            BlockState::Uninitialized => {
                inner.data = Some(data);
                inner.state = BlockState::Initialized;
                Ok(())
            }
            BlockState::Initialized => {
                inner.data = Some(data);
                inner.state = BlockState::Initialized;
                Ok(())
            }
            BlockState::Commited | BlockState::Registered => {
                Err(Box::new(BlockError::InvalidStateTransition))
            }
        }
    }

    pub(super) fn iter(&self) -> BlockIterator {
        BlockIterator::new(
            self.clone(),
            self.inner.read().key_type,
            self.inner.read().value_type,
        )
    }
}

/// BlockData represents the data in a block. The data is stored in an Arrow record batch with the column schema (prefix, key, value).
/// These are stored in sorted order by prefix and key for efficient lookups.
#[derive(Clone)]
pub(super) struct BlockData {
    pub(super) data: RecordBatch,
}

impl BlockData {
    pub(crate) fn new(data: RecordBatch) -> Self {
        Self { data }
    }

    /// Returns the size of the block in bytes
    pub(crate) fn get_size(&self) -> usize {
        let mut total_size = 0;
        for column in self.data.columns() {
            total_size += column.get_buffer_memory_size();
        }
        total_size
    }
}

// ============== BlockDataBuilder ==============

enum KeyBuilder {
    StringBuilder(StringBuilder),
    FloatBuilder(Float32Builder),
    BoolBuilder(BooleanBuilder),
    UintBuilder(UInt32Builder),
}

enum ValueBuilder {
    Int32ArrayValueBuilder(ListBuilder<Int32Builder>),
    StringValueBuilder(StringBuilder),
    RoaringBitmapBuilder(BinaryBuilder),
    UintValueBuilder(UInt32Builder),
    EmbeddingRecordBuilder(EmbeddingRecordValueBuilder),
}

/// A struct used to build the embedding record value
/// We use this as opposed to StructBuilder because StructBuilder does not
/// give us the ability to manage the size/capacity of the buffers we are using
struct EmbeddingRecordValueBuilder {
    user_id_builder: StringBuilder,
    document_builder: StringBuilder,
    metadata_builder: BinaryBuilder,
}

/// BlockDataBuilder is used to build a block. It is used to add data to a block and then build the BlockData once all data has been added.
/// It is only used internally to an arrow_blockfile.
pub(super) struct BlockDataBuilder {
    prefix_builder: StringBuilder,
    key_builder: KeyBuilder,
    value_builder: ValueBuilder,
    last_key: Option<BlockfileKey>,
}

/// ## Options for the BlockDataBuilder
/// - item_count: The number of items in the block
/// - prefix_data_capacity: The required capacity for the prefix data. This will be rounded to the nearest 64 byte alignment by arrow.
/// - key_data_capacity: The required capacity for the key data. This will be rounded to the nearest 64 byte alignment by arrow.
/// - total_value_count: The total number of values in the block
/// - total_value_capacity: The required capacity for the value data. This will be rounded to the nearest 64 byte alignment by arrow.
/// # Note
/// The capacities are the size of the initially allocated buffers. The builder will not enforce these limits and will grow the buffers as needed.
/// When in use in a blockfile, maintaining the block size is accomplished at the blockfile level.
pub(super) struct BlockBuilderOptions {
    pub(super) item_count: usize,
    pub(super) prefix_data_capacity: usize,
    pub(super) key_data_capacity: usize,
    pub(super) total_value_count: usize,
    pub(super) value_options: BlockBuilderValueOptions,
}

pub(super) enum BlockBuilderValueOptions {
    FlatValue(BlockBuilderFlatValueOptions),
    EmbeddingValue(BlockBuilderEmbeddingValueOptions),
}

pub(super) struct BlockBuilderFlatValueOptions {
    pub(super) total_value_capacity: usize,
}

pub(super) struct BlockBuilderEmbeddingValueOptions {
    pub(super) total_user_id_capacity: usize,
    pub(super) total_document_capacity: usize,
    pub(super) total_metadata_capacity: usize,
}

impl BlockBuilderOptions {
    pub(super) fn new_flat_value(
        item_count: usize,
        prefix_data_capacity: usize,
        key_data_capacity: usize,
        total_value_count: usize,
        total_value_capacity: usize,
    ) -> Self {
        Self {
            item_count,
            prefix_data_capacity,
            key_data_capacity,
            total_value_count,
            value_options: BlockBuilderValueOptions::FlatValue(BlockBuilderFlatValueOptions {
                total_value_capacity,
            }),
        }
    }

    pub(super) fn new_embedding_value(
        item_count: usize,
        prefix_data_capacity: usize,
        key_data_capacity: usize,
        total_user_id_capacity: usize,
        total_document_capacity: usize,
        total_metadata_capacity: usize,
    ) -> Self {
        Self {
            item_count,
            prefix_data_capacity,
            key_data_capacity,
            total_value_count: 0,
            value_options: BlockBuilderValueOptions::EmbeddingValue(
                BlockBuilderEmbeddingValueOptions {
                    total_user_id_capacity,
                    total_document_capacity,
                    total_metadata_capacity,
                },
            ),
        }
    }
}

impl BlockDataBuilder {
    /// Creates a new BlockDataBuilder
    /// ## Panics
    /// Panics if the key and value types are not compatible with the options.
    pub(super) fn new(
        key_type: KeyType,
        value_type: ValueType,
        options: BlockBuilderOptions,
    ) -> Self {
        // Ensure key and value types are compatible with the options. Downstream code can
        // then unwrap the options and use them without further checks.
        match &options.value_options {
            BlockBuilderValueOptions::FlatValue(_) => match value_type {
                ValueType::Int32Array
                | ValueType::String
                | ValueType::Uint
                | ValueType::RoaringBitmap => {}
                _ => panic!("Invalid value type for flat value options"),
            },
            BlockBuilderValueOptions::EmbeddingValue(_) => match value_type {
                ValueType::EmbeddingRecord => {}
                _ => panic!("Invalid value type for embedding value options"),
            },
        }

        let prefix_builder =
            StringBuilder::with_capacity(options.item_count, options.prefix_data_capacity);
        let key_builder = match key_type {
            KeyType::String => KeyBuilder::StringBuilder(StringBuilder::with_capacity(
                options.item_count,
                options.key_data_capacity,
            )),
            KeyType::Float => {
                KeyBuilder::FloatBuilder(Float32Builder::with_capacity(options.item_count))
            }
            KeyType::Bool => {
                KeyBuilder::BoolBuilder(BooleanBuilder::with_capacity(options.item_count))
            }
            KeyType::Uint => {
                KeyBuilder::UintBuilder(UInt32Builder::with_capacity(options.item_count))
            }
        };
        let value_builder = match value_type {
            ValueType::Int32Array => {
                ValueBuilder::Int32ArrayValueBuilder(ListBuilder::with_capacity(
                    Int32Builder::with_capacity(options.total_value_count),
                    options.item_count,
                ))
            }
            ValueType::String => {
                let value_options = match &options.value_options {
                    BlockBuilderValueOptions::FlatValue(options) => options,
                    _ => panic!("Invalid value options for string value"),
                };
                ValueBuilder::StringValueBuilder(StringBuilder::with_capacity(
                    options.item_count,
                    value_options.total_value_capacity,
                ))
            }
            ValueType::Uint => {
                ValueBuilder::UintValueBuilder(UInt32Builder::with_capacity(options.item_count))
            }
            ValueType::RoaringBitmap => {
                let value_options = match &options.value_options {
                    BlockBuilderValueOptions::FlatValue(options) => options,
                    _ => panic!("Invalid value options for roaring bitmap value"),
                };
                ValueBuilder::RoaringBitmapBuilder(BinaryBuilder::with_capacity(
                    options.item_count,
                    value_options.total_value_capacity,
                ))
            }
            ValueType::EmbeddingRecord => {
                let value_options = match &options.value_options {
                    BlockBuilderValueOptions::EmbeddingValue(options) => options,
                    _ => panic!("Invalid value options for embedding record value"),
                };
                let user_id_builder = StringBuilder::with_capacity(
                    options.item_count,
                    value_options.total_user_id_capacity,
                );
                let document_builder = StringBuilder::with_capacity(
                    options.item_count,
                    value_options.total_document_capacity,
                );
                let metadata_builder = BinaryBuilder::with_capacity(
                    options.item_count,
                    value_options.total_metadata_capacity,
                );
                ValueBuilder::EmbeddingRecordBuilder(EmbeddingRecordValueBuilder {
                    user_id_builder,
                    document_builder,
                    metadata_builder,
                })
            }
            // TODO: Implement the other value types
            _ => unimplemented!(),
        };
        Self {
            prefix_builder,
            key_builder,
            value_builder,
            last_key: None,
        }
    }

    /// Adds a key, value pair to the block. The key must be greater than the last key added to the block otherwise an error is returned.
    /// TOOD: value builders should likely not take ownership of the value, and instead clone them into the arrow datastruct since that
    /// must occur anyway.
    pub(super) fn add(
        &mut self,
        key: BlockfileKey,
        value: Value,
    ) -> Result<(), Box<BlockDataAddError>> {
        match &self.last_key {
            Some(last_key) => {
                if key < *last_key {
                    return Err(Box::new(BlockDataAddError::KeyNotInOrder));
                }
            }
            None => {}
        }
        self.last_key = Some(key.clone());
        self.prefix_builder.append_value(key.prefix);
        match self.key_builder {
            KeyBuilder::StringBuilder(ref mut builder) => match key.key {
                Key::String(key) => {
                    builder.append_value(key);
                }
                _ => unreachable!("Invalid key type for block"),
            },
            KeyBuilder::FloatBuilder(ref mut builder) => match key.key {
                Key::Float(key) => {
                    builder.append_value(key);
                }
                _ => unreachable!("Invalid key type for block"),
            },
            KeyBuilder::BoolBuilder(ref mut builder) => match key.key {
                Key::Bool(key) => {
                    builder.append_value(key);
                }
                _ => unreachable!("Invalid key type for block"),
            },
            KeyBuilder::UintBuilder(ref mut builder) => match key.key {
                Key::Uint(key) => {
                    builder.append_value(key);
                }
                _ => unreachable!("Invalid key type for block"),
            },
        }

        match self.value_builder {
            ValueBuilder::Int32ArrayValueBuilder(ref mut builder) => match value {
                Value::Int32ArrayValue(array) => {
                    builder.append_value(&array);
                }
                _ => unreachable!("Invalid value type for block"),
            },
            ValueBuilder::StringValueBuilder(ref mut builder) => match value {
                Value::StringValue(string) => {
                    builder.append_value(string);
                }
                _ => unreachable!("Invalid value type for block"),
            },
            ValueBuilder::UintValueBuilder(ref mut builder) => match value {
                Value::UintValue(uint) => {
                    builder.append_value(uint);
                }
                _ => unreachable!("Invalid value type for block"),
            },
            ValueBuilder::RoaringBitmapBuilder(ref mut builder) => match value {
                Value::RoaringBitmapValue(bitmap) => {
                    let mut bytes = Vec::with_capacity(bitmap.serialized_size());
                    match bitmap.serialize_into(&mut bytes) {
                        Ok(_) => builder.append_value(&bytes),
                        Err(e) => {
                            return Err(Box::new(BlockDataAddError::RoaringBitmapError(e)));
                        }
                    }
                }
                _ => unreachable!("Invalid value type for block"),
            },
            ValueBuilder::EmbeddingRecordBuilder(ref mut builder) => match value {
                Value::EmbeddingRecordValue(embedding_record) => {
                    builder
                        .user_id_builder
                        .append_value(embedding_record.id.clone());
                    builder
                        .document_builder
                        .append_option(embedding_record.get_document());
                    match embedding_record.metadata {
                        Some(metadata) => {
                            let proto: chroma_proto::UpdateMetadata = metadata.into();
                            let bytes = proto.encode_to_vec();
                            builder.metadata_builder.append_value(bytes);
                        }
                        None => {
                            builder.metadata_builder.append_null();
                        }
                    }
                }
                _ => unreachable!("Invalid value type for block"),
            },
        }

        Ok(())
    }

    pub(super) fn build(&mut self) -> Result<BlockData, BlockDataBuildError> {
        let prefix = self.prefix_builder.finish();
        let prefix_field = Field::new("prefix", DataType::Utf8, true);
        // TODO: figure out how to get rid of nullable, the builders turn it on by default but we don't want it
        let key_field;
        let key = match self.key_builder {
            KeyBuilder::StringBuilder(ref mut builder) => {
                key_field = Field::new("key", DataType::Utf8, true);
                let arr = builder.finish();
                (&arr as &dyn Array).slice(0, arr.len())
            }
            KeyBuilder::FloatBuilder(ref mut builder) => {
                key_field = Field::new("key", DataType::Float32, true);
                let arr = builder.finish();
                (&arr as &dyn Array).slice(0, arr.len())
            }
            KeyBuilder::BoolBuilder(ref mut builder) => {
                key_field = Field::new("key", DataType::Boolean, true);
                let arr = builder.finish();
                (&arr as &dyn Array).slice(0, arr.len())
            }
            KeyBuilder::UintBuilder(ref mut builder) => {
                key_field = Field::new("key", DataType::UInt32, true);
                let arr = builder.finish();
                (&arr as &dyn Array).slice(0, arr.len())
            }
        };

        let value_field;
        let value = match self.value_builder {
            ValueBuilder::Int32ArrayValueBuilder(ref mut builder) => {
                value_field = Field::new(
                    "value",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                    true,
                );
                let arr = builder.finish();
                (&arr as &dyn Array).slice(0, arr.len())
            }
            ValueBuilder::StringValueBuilder(ref mut builder) => {
                value_field = Field::new("value", DataType::Utf8, true);
                let arr = builder.finish();
                (&arr as &dyn Array).slice(0, arr.len())
            }
            ValueBuilder::UintValueBuilder(ref mut builder) => {
                value_field = Field::new("value", DataType::UInt32, true);
                let arr = builder.finish();
                (&arr as &dyn Array).slice(0, arr.len())
            }
            ValueBuilder::RoaringBitmapBuilder(ref mut builder) => {
                value_field = Field::new("value", DataType::Binary, true);
                let arr = builder.finish();
                (&arr as &dyn Array).slice(0, arr.len())
            }
            ValueBuilder::EmbeddingRecordBuilder(ref mut builder) => {
                value_field = Field::new(
                    "value",
                    DataType::Struct(Fields::from(vec![
                        Field::new("user_id", DataType::Utf8, true),
                        Field::new("document", DataType::Utf8, true),
                        Field::new("metadata", DataType::Binary, true),
                    ])),
                    true,
                );
                let user_id_arr = builder.user_id_builder.finish();
                let document_arr = builder.document_builder.finish();
                let metadata_arr = builder.metadata_builder.finish();
                let arr = StructArray::from(vec![
                    (
                        Arc::new(Field::new("user_id", DataType::Utf8, true)),
                        Arc::new(user_id_arr) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("document", DataType::Utf8, true)),
                        Arc::new(document_arr) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("metadata", DataType::Binary, true)),
                        Arc::new(metadata_arr) as ArrayRef,
                    ),
                ]);
                (&arr as &dyn Array).slice(0, arr.len())
            }
        };

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            prefix_field,
            key_field,
            value_field,
        ]));
        let record_batch =
            RecordBatch::try_new(schema, vec![Arc::new(prefix), Arc::new(key), value]);
        match record_batch {
            Ok(record_batch) => Ok(BlockData::new(record_batch)),
            Err(e) => Err(BlockDataBuildError::ArrowError(e)),
        }
    }
}

#[derive(Error, Debug)]
pub enum BlockDataAddError {
    #[error("Blockfile key not in order")]
    KeyNotInOrder,
    #[error("Roaring bitmap error")]
    RoaringBitmapError(#[from] Error),
}

impl ChromaError for BlockDataAddError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockDataAddError::KeyNotInOrder => ErrorCodes::InvalidArgument,
            BlockDataAddError::RoaringBitmapError(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Error, Debug)]
pub enum BlockDataBuildError {
    #[error("Arrow error")]
    ArrowError(#[from] arrow::error::ArrowError),
}

impl ChromaError for BlockDataBuildError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockDataBuildError::ArrowError(_) => ErrorCodes::Internal,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::blockstore::types::Key;
    use arrow::array::Int32Array;

    #[test]
    fn test_block_builder_can_add() {
        let num_entries = 1000;

        let mut keys = Vec::new();
        let mut key_bytes = 0;
        for i in 0..num_entries {
            keys.push(Key::String(format!("{:04}", i)));
            key_bytes += i.to_string().len();
        }

        let prefix = "key".to_string();
        let prefix_bytes = prefix.len() * num_entries;
        let mut block_builder = BlockDataBuilder::new(
            KeyType::String,
            ValueType::Int32Array,
            BlockBuilderOptions::new_flat_value(
                num_entries,
                prefix_bytes,
                key_bytes,
                num_entries,         // 2 int32s per entry
                num_entries * 2 * 4, // 2 int32s per entry
            ),
        );

        for i in 0..num_entries {
            block_builder
                .add(
                    BlockfileKey::new(prefix.clone(), keys[i].clone()),
                    Value::Int32ArrayValue(Int32Array::from(vec![i as i32, (i + 1) as i32])),
                )
                .unwrap();
        }

        // Basic sanity check
        let block_data = block_builder.build().unwrap();
        assert_eq!(block_data.data.column(0).len(), num_entries);
        assert_eq!(block_data.data.column(1).len(), num_entries);
        assert_eq!(block_data.data.column(2).len(), num_entries);
    }

    #[test]
    fn test_out_of_order_key_fails() {
        let mut block_builder = BlockDataBuilder::new(
            KeyType::String,
            ValueType::Int32Array,
            BlockBuilderOptions::new_flat_value(1024, 1024, 1024, 1024, 1024),
        );

        block_builder
            .add(
                BlockfileKey::new("key".to_string(), Key::String("b".to_string())),
                Value::Int32ArrayValue(Int32Array::from(vec![1, 2])),
            )
            .unwrap();

        let result = block_builder.add(
            BlockfileKey::new("key".to_string(), Key::String("a".to_string())),
            Value::Int32ArrayValue(Int32Array::from(vec![1, 2])),
        );

        match result {
            Ok(_) => panic!("Expected error"),
            Err(e) => {
                assert_eq!(e.code(), ErrorCodes::InvalidArgument);
            }
        }
    }
}
