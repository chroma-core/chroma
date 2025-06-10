use async_trait::async_trait;
use chroma_blockstore::{
    arrow::provider::{BlockManager, RootManagerError},
    RootManager,
};
use chroma_index::{hnsw_provider::HnswIndexProvider, IndexUuid};
use chroma_storage::StorageError;
use chroma_system::{Operator, OperatorType};
use chroma_types::{chroma_proto::CollectionVersionFile, CollectionUuid, Segment, HNSW_PATH};
use std::{collections::HashSet, str::FromStr};
use thiserror::Error;
use tokio::task::{JoinError, JoinSet};
use uuid::Uuid;

#[derive(Debug)]
pub struct ListFilesAtVersionInput {
    root_manager: RootManager,
    version_file: CollectionVersionFile,
    version: i64,
}

impl ListFilesAtVersionInput {
    pub fn new(
        root_manager: RootManager,
        version_file: CollectionVersionFile,
        version: i64,
    ) -> Self {
        Self {
            root_manager,
            version_file,
            version,
        }
    }
}

#[derive(Debug)]
pub struct ListFilesAtVersionOutput {
    pub collection_id: CollectionUuid,
    pub version: i64,
    pub file_paths: HashSet<String>,
}

#[derive(Debug, Error)]
pub enum ListFilesAtVersionError {
    #[error("Version history field missing")]
    VersionHistoryMissing,
    #[error("Version {0} not found")]
    VersionNotFound(i64),
    #[error("Invalid UUID: {0}")]
    InvalidUuid(uuid::Error),
    #[error("Sparse index fetch task failed: {0}")]
    SparseIndexTaskFailed(JoinError),
    #[error("Error fetching block IDs for sparse index: {0}")]
    FetchBlockIdsError(#[from] RootManagerError),
    #[error("Version file missing collection ID")]
    VersionFileMissingCollectionId,
}

#[derive(Clone, Debug)]
pub struct ListFilesAtVersionsOperator {}

#[async_trait]
impl Operator<ListFilesAtVersionInput, ListFilesAtVersionOutput> for ListFilesAtVersionsOperator {
    type Error = ListFilesAtVersionError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &ListFilesAtVersionInput,
    ) -> Result<ListFilesAtVersionOutput, Self::Error> {
        let collection_id = CollectionUuid::from_str(
            &input
                .version_file
                .collection_info_immutable
                .as_ref()
                .ok_or_else(|| ListFilesAtVersionError::VersionFileMissingCollectionId)?
                .collection_id,
        )
        .map_err(ListFilesAtVersionError::InvalidUuid)?;

        let version_history = input
            .version_file
            .version_history
            .as_ref()
            .ok_or_else(|| ListFilesAtVersionError::VersionHistoryMissing)?;

        let mut file_paths = HashSet::new();
        let mut sparse_index_ids = HashSet::new();

        let version = version_history
            .versions
            .iter()
            .find(|v| v.version == input.version)
            .ok_or_else(|| ListFilesAtVersionError::VersionNotFound(input.version))?;

        tracing::debug!(
            "Listing files at version {} for collection {}.",
            version.version,
            collection_id,
        );
        tracing::trace!(
            "Processing version {:#?} for collection {}",
            version,
            collection_id
        );

        let mut file_prefix = String::from("");
        if let Some(segment_info) = &version.segment_info {
            for segment in &segment_info.segment_compaction_info {
                for (file_type, segment_paths) in &segment.file_paths {
                    if file_type == "hnsw_index" || file_type == HNSW_PATH {
                        for path in &segment_paths.paths {
                            let (prefix, hnsw_index_id) = Segment::extract_prefix_and_id(path);
                            file_prefix = prefix.to_string();
                            for hnsw_file in [
                                "header.bin",
                                "data_level0.bin",
                                "length.bin",
                                "link_lists.bin",
                            ] {
                                let hnsw_index_uuid = IndexUuid(
                                    Uuid::parse_str(hnsw_index_id)
                                        .map_err(ListFilesAtVersionError::InvalidUuid)?,
                                );
                                let s3_key = HnswIndexProvider::format_key(
                                    prefix,
                                    &hnsw_index_uuid,
                                    hnsw_file,
                                );
                                file_paths.insert(s3_key);
                            }
                        }
                    } else {
                        // Must be a sparse index
                        for path in &segment_paths.paths {
                            let (prefix, sparse_index_id) = Segment::extract_prefix_and_id(path);
                            let sparse_index_uuid = Uuid::parse_str(sparse_index_id)
                                .map_err(ListFilesAtVersionError::InvalidUuid)?;
                            file_prefix = prefix.to_string();
                            let file_path =
                                RootManager::get_storage_key(prefix, &sparse_index_uuid);

                            file_paths.insert(file_path);
                            sparse_index_ids.insert(sparse_index_uuid);
                        }
                    }
                }
            }
        }

        let mut block_id_tasks = JoinSet::new();
        for sparse_index_id in sparse_index_ids {
            let root_manager = input.root_manager.clone();
            let file_prefix_clone = file_prefix.clone();
            block_id_tasks.spawn(async move {
                match root_manager.get_all_block_ids(&sparse_index_id, &file_prefix_clone).await {
                    Ok(block_ids) => Ok(block_ids),
                    Err(RootManagerError::StorageGetError(StorageError::NotFound { .. })) => {
                        tracing::debug!(
                            "Sparse index {} not found in storage. Assuming it was previously deleted.",
                            sparse_index_id
                        );
                        Ok(vec![])
                    }
                    Err(e) => Err(e),
                }
            });
        }

        while let Some(res) = block_id_tasks.join_next().await {
            let block_ids = res
                .map_err(ListFilesAtVersionError::SparseIndexTaskFailed)?
                .map_err(ListFilesAtVersionError::FetchBlockIdsError)?;

            for block_id in block_ids {
                let s3_key = BlockManager::format_key(&file_prefix, &block_id);
                file_paths.insert(s3_key);
            }
        }

        Ok(ListFilesAtVersionOutput {
            collection_id,
            version: input.version,
            file_paths,
        })
    }
}
