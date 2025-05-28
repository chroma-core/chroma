use async_trait::async_trait;
use chroma_blockstore::{arrow::provider::RootManagerError, RootManager};
use chroma_storage::StorageError;
use chroma_system::{Operator, OperatorType};
use chroma_types::{chroma_proto::CollectionVersionFile, CollectionUuid, HNSW_PATH};
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

        if let Some(segment_info) = &version.segment_info {
            for segment in &segment_info.segment_compaction_info {
                for (file_type, segment_paths) in &segment.file_paths {
                    if file_type == "hnsw_index" || file_type == HNSW_PATH {
                        for path in &segment_paths.paths {
                            for hnsw_file in [
                                "header.bin",
                                "data_level0.bin",
                                "length.bin",
                                "link_lists.bin",
                            ] {
                                // Path construction here will need to be updated after the upcoming collection file prefix changes.
                                file_paths.insert(format!("hnsw/{}/{}", path, hnsw_file));
                            }
                        }
                    } else {
                        // Must be a sparse index
                        for path in &segment_paths.paths {
                            // Path construction here will need to be updated after the upcoming collection file prefix changes.
                            file_paths.insert(format!("sparse_index/{}", path));

                            let sparse_index_id = Uuid::parse_str(path)
                                .map_err(ListFilesAtVersionError::InvalidUuid)?;

                            sparse_index_ids.insert(sparse_index_id);
                        }
                    }
                }
            }
        }

        let mut block_id_tasks = JoinSet::new();
        for sparse_index_id in sparse_index_ids {
            let root_manager = input.root_manager.clone();
            block_id_tasks.spawn(async move {
                match root_manager.get_all_block_ids(&sparse_index_id).await {
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
                // Path construction here will need to be updated after the upcoming collection file prefix changes.
                file_paths.insert(format!("block/{}", block_id));
            }
        }

        Ok(ListFilesAtVersionOutput {
            collection_id,
            version: input.version,
            file_paths,
        })
    }
}
