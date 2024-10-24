use std::collections::HashMap;

use arrow::array::RecordBatch;
use uuid::Uuid;

use crate::arrow::{
    block::Block,
    types::{ArrowWriteableKey, ArrowWriteableValue},
};

pub(crate) trait DeltaCommon {
    /// Creates a new block delta from a block.
    /// # Arguments
    /// - id: the id of the block delta.
    fn new<K: ArrowWriteableKey, V: ArrowWriteableValue>(id: Uuid) -> Self;
    /// Creates a new block delta from a block.
    /// # Arguments
    /// - id: the id of the block delta.
    /// - block: the block to fork.
    fn fork_block<K: ArrowWriteableKey, V: ArrowWriteableValue>(id: Uuid, block: &Block) -> Self;
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
