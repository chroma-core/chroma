use crate::arrow::root::{RootWriter, Version};
use crate::arrow::sparse_index::SparseIndexWriter;
use crate::arrow::sparse_index::{AddError, SetCountError};
use crate::key::CompositeKey;
use crate::RootManager;
use chroma_cache::nop::NopCache;
use chroma_error::ChromaError;
use chroma_storage::Storage;
use uuid::Uuid;

/// Creates a sparse index file for ** TESTING PURPOSES ** ONLY.
/// This module is not gated as [#cfg(test)] in lib.rs because it is used in crates external to blockstore.
/// This function allows creating sparse index files with block IDs that may not actually exist in storage.
///
/// # Arguments
/// * `storage` - The storage backend to use
/// * `block_ids` - Vector of block IDs to include in the sparse index
/// * `prefix` - Optional prefix for composite keys. If None, defaults to "test"
///
/// # Returns
/// * `Result<Uuid, Box<dyn ChromaError>>` - The UUID of the created sparse index file
pub async fn create_test_sparse_index(
    storage: &Storage,
    root_id: Uuid,
    block_ids: Vec<Uuid>,
    prefix: Option<String>,
    prefix_path: String,
) -> Result<Uuid, Box<dyn ChromaError>> {
    if block_ids.is_empty() {
        return Err(Box::new(TestSparseIndexError::EmptyBlockIds));
    }

    // Initialize sparse index with first block ID
    let sparse_index = SparseIndexWriter::new(block_ids[0]);
    let prefix = prefix.unwrap_or_else(|| "test".to_string());

    // Add remaining block IDs to the sparse index
    for (i, block_id) in block_ids.iter().enumerate().skip(1) {
        let key = CompositeKey::new(prefix.clone(), format!("block_{}", i).as_str());
        sparse_index.add_block(key, *block_id)?;
        sparse_index.set_count(*block_id, 1)?;
    }

    // Set count for the first block
    sparse_index.set_count(block_ids[0], 1)?;

    let max_block_size_bytes = 8 * 1024 * 1024; // 8 MB

    // Create and save the sparse index file
    let root_writer = RootWriter::new(
        Version::V1_2,
        root_id,
        sparse_index,
        prefix_path,
        max_block_size_bytes,
    );
    let root_manager = RootManager::new(storage.clone(), Box::new(NopCache));
    root_manager.flush::<&str>(&root_writer).await?;

    Ok(root_id)
}

#[derive(Debug, thiserror::Error)]
pub enum TestSparseIndexError {
    #[error("Cannot create sparse index with empty block IDs")]
    EmptyBlockIds,
}

impl ChromaError for TestSparseIndexError {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::InvalidArgument
    }
}

impl From<AddError> for Box<dyn ChromaError> {
    fn from(error: AddError) -> Self {
        Box::new(error)
    }
}

impl From<SetCountError> for Box<dyn ChromaError> {
    fn from(error: SetCountError) -> Self {
        Box::new(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_storage::local::LocalStorage;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_sparse_index() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));

        let prefix_path = "";
        let block_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let result = create_test_sparse_index(
            &storage,
            Uuid::new_v4(),
            block_ids.clone(),
            None,
            prefix_path.to_string(),
        )
        .await;
        assert!(result.is_ok());

        // Verify the sparse index was created by trying to read it
        let root_id = result.unwrap();
        let root_manager = RootManager::new(storage.clone(), Box::new(NopCache));

        // Verify all block IDs are present in the sparse index
        let stored_block_ids = root_manager
            .get_all_block_ids(&root_id, prefix_path)
            .await
            .unwrap();
        assert_eq!(stored_block_ids.len(), block_ids.len());
        for block_id in block_ids {
            assert!(stored_block_ids.contains(&block_id));
        }
    }

    #[tokio::test]
    async fn test_create_sparse_index_with_empty_blocks() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));

        let result =
            create_test_sparse_index(&storage, Uuid::new_v4(), vec![], None, "".to_string()).await;
        assert!(matches!(
            result,
            Err(e) if e.to_string().contains("Cannot create sparse index with empty block IDs")
        ));
    }

    #[tokio::test]
    async fn test_create_sparse_index_with_custom_prefix() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));

        let block_ids = vec![Uuid::new_v4(), Uuid::new_v4()];
        let prefix = Some("custom".to_string());
        let result =
            create_test_sparse_index(&storage, Uuid::new_v4(), block_ids, prefix, "".to_string())
                .await;
        assert!(result.is_ok());
    }
}
