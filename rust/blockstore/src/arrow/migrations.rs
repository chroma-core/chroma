use std::collections::HashSet;

use super::{
    provider::BlockManager,
    root::{RootWriter, Version},
    sparse_index::SetCountError,
};
use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum MigrationError {
    #[error("Block not found")]
    BlockNotFound,
    #[error("Block could not be fetched")]
    BlockFetchError,
    #[error("Error setting count")]
    SetCountError(#[from] SetCountError),
}

impl ChromaError for MigrationError {
    fn code(&self) -> ErrorCodes {
        match self {
            MigrationError::BlockNotFound => ErrorCodes::Internal,
            MigrationError::BlockFetchError => ErrorCodes::Internal,
            MigrationError::SetCountError(e) => e.code(),
        }
    }
}

async fn migrate_v1_to_v1_1(
    root: &mut RootWriter,
    block_manager: &BlockManager,
    new_block_ids: &HashSet<Uuid>,
) -> Result<(), MigrationError> {
    // MIGRATION(10/15/2024 @hammadb) Get all the blocks and manually update the sparse index
    if root.version == Version::V1 {
        root.version = Version::V1_1;
        let block_ids;
        // Guard the sparse index data access with a lock
        // otherwise we have to hold the lock across an await
        {
            let sparse_index_data = root.sparse_index.data.lock();
            block_ids = sparse_index_data
                .forward
                .values()
                .filter(|block_id| !new_block_ids.contains(block_id))
                .copied()
                .collect::<Vec<Uuid>>();
        }
        for block_id in block_ids.iter() {
            let block = block_manager.get(block_id).await;
            match block {
                Ok(Some(block)) => {
                    match root.sparse_index.set_count(*block_id, block.len() as u32) {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(MigrationError::SetCountError(e));
                        }
                    }
                }
                Ok(None) => {
                    return Err(MigrationError::BlockNotFound);
                }
                Err(_) => {
                    return Err(MigrationError::BlockFetchError);
                }
            }
        }
    }

    Ok(())
}

pub async fn apply_migrations_to_blockfile(
    root: &mut RootWriter,
    block_manager: &BlockManager,
    new_block_ids: &HashSet<Uuid>,
) -> Result<(), MigrationError> {
    migrate_v1_to_v1_1(root, block_manager, new_block_ids).await
}
