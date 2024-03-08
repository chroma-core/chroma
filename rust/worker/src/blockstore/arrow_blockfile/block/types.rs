use crate::blockstore::types::{BlockfileKey, Key, KeyType, Value, ValueType};
use crate::errors::{ChromaError, ErrorCodes};
use arrow::array::{BooleanArray, BooleanBuilder, Float32Array, Float32Builder};
use arrow::{
    array::{Array, Int32Array, Int32Builder, ListArray, ListBuilder, StringArray, StringBuilder},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use parking_lot::RwLock;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

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
}

impl ChromaError for BlockError {
    fn code(&self) -> ErrorCodes {
        match self {
            BlockError::InvalidStateTransition => ErrorCodes::Internal,
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
