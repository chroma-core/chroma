use crate::errors::ChromaError;

use super::{
    provider::{BlockManager, SparseIndexManager},
    sparse_index::SparseIndex,
    types::{ArrowWriteableKey, ArrowWriteableValue},
};
use std::collections::HashSet;
use uuid::Uuid;

pub(crate) struct ArrowBlockfileFlusher<K: ArrowWriteableKey, V: ArrowWriteableValue> {
    block_manager: BlockManager,
    sparse_index_manager: SparseIndexManager,
    modified_delta_ids: HashSet<Uuid>,
    sparse_index: SparseIndex,
    marker: std::marker::PhantomData<(K, V)>,
    id: Uuid,
}

impl<K: ArrowWriteableKey, V: ArrowWriteableValue> ArrowBlockfileFlusher<K, V> {
    pub(crate) fn new(
        block_manager: BlockManager,
        sparse_index_manager: SparseIndexManager,
        modified_delta_ids: HashSet<Uuid>,
        sparse_index: SparseIndex,
        id: Uuid,
    ) -> Self {
        let sparse_index = sparse_index_manager.get(&id).unwrap();
        Self {
            block_manager,
            sparse_index_manager,
            modified_delta_ids,
            sparse_index,
            marker: std::marker::PhantomData,
            id,
        }
    }

    pub(crate) async fn flush(self) -> Result<(), Box<dyn ChromaError>> {
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
}
