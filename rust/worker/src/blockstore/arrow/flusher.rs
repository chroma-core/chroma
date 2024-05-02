use crate::errors::ChromaError;

use super::{
    provider::{BlockManager, SparseIndexManager},
    sparse_index::SparseIndex,
    types::{ArrowWriteableKey, ArrowWriteableValue},
};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub(crate) struct ArrowBlockfileFlusher {
    block_manager: BlockManager,
    sparse_index_manager: SparseIndexManager,
    modified_delta_ids: HashSet<Uuid>,
    sparse_index: SparseIndex,
    id: Uuid,
}

impl ArrowBlockfileFlusher {
    pub(crate) fn new(
        block_manager: BlockManager,
        sparse_index_manager: SparseIndexManager,
        modified_delta_ids: HashSet<Uuid>,
        sparse_index: SparseIndex,
        id: Uuid,
    ) -> Self {
        // let sparse_index = sparse_index_manager.get(&id).unwrap();
        Self {
            block_manager,
            sparse_index_manager,
            modified_delta_ids,
            sparse_index,
            id,
        }
    }

    pub(crate) async fn flush<K: ArrowWriteableKey, V: ArrowWriteableValue>(
        self,
    ) -> Result<(), Box<dyn ChromaError>> {
        for delta_id in self.modified_delta_ids {
            self.block_manager.flush(&delta_id).await;
        }
        // TODO: catch errors from the flush
        let res = self
            .sparse_index_manager
            .flush::<K>(&self.sparse_index.id)
            .await;
        Ok(())
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }
}
