use std::collections::HashMap;

use arrow::array::RecordBatch;
use uuid::Uuid;

use crate::arrow::{
    block::Block,
    types::{ArrowWriteableKey, ArrowWriteableValue},
};

/// A block delta tracks a source block and represents the new state of a block. Blocks are
/// immutable, so when a write is made to a block, a new block is created with the new state.
/// A block delta is a temporary representation of the new state of a block. A block
/// can be converted to a block data, which is then used to create a new block. A block data
/// can be converted into a block delta for new writes.
pub(crate) trait Delta {
    /// Creates a new block delta from a block.
    /// # Arguments
    /// - id: the id of the block delta.
    fn new<K: ArrowWriteableKey, V: ArrowWriteableValue>(id: Uuid) -> Self;
    /// Creates a new block delta from a block.
    /// # Arguments
    /// - id: the id of the block delta.
    /// - block: the block to fork.
    fn fork_block<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        new_id: Uuid,
        old_block: &Block,
    ) -> Self;
    fn id(&self) -> Uuid;
    /// Finishes the block delta and converts it into a record batch.
    /// # Arguments
    /// - metadata: the metadata to attach to the record batch.
    /// # Returns
    /// A record batch with the key value pairs in the block delta.
    fn finish<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        self,
        metadata: Option<HashMap<String, String>>,
    ) -> RecordBatch;
}
