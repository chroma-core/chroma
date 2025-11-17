use crate::types::FilePathSet;
use async_trait::async_trait;
use chroma_blockstore::{arrow::provider::RootManagerError, RootManager};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{
    hnsw_provider::{HnswIndexProvider, FILES},
    IndexUuid,
};
use chroma_storage::StorageError;
use chroma_system::{Operator, OperatorType};
use chroma_types::{chroma_proto::CollectionVersionFile, CollectionUuid, Segment, HNSW_PATH};
use futures::stream::StreamExt;
use std::{collections::HashMap, str::FromStr, sync::Arc};
use thiserror::Error;

#[derive(Debug)]
pub struct ListFilesAtVersionInput {
    root_manager: RootManager,
    version_file: Arc<CollectionVersionFile>,
    version: i64,
}

impl ListFilesAtVersionInput {
    pub fn new(
        root_manager: RootManager,
        version_file: Arc<CollectionVersionFile>,
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
    pub file_paths: FilePathSet,
}

#[derive(Debug, Error)]
pub enum ListFilesAtVersionError {
    #[error("Version history field missing")]
    VersionHistoryMissing,
    #[error("Version {0} not found")]
    VersionNotFound(i64),
    #[error("Invalid UUID: {0}")]
    InvalidUuid(uuid::Error),
    #[error("Error fetching block IDs for sparse index: {0}")]
    FetchBlockIdsError(#[from] RootManagerError),
    #[error("Version file missing collection ID")]
    VersionFileMissingCollectionId,
}

impl ChromaError for ListFilesAtVersionError {
    fn code(&self) -> ErrorCodes {
        match self {
            ListFilesAtVersionError::VersionHistoryMissing => ErrorCodes::NotFound,
            ListFilesAtVersionError::VersionNotFound(_) => ErrorCodes::NotFound,
            ListFilesAtVersionError::InvalidUuid(_) => ErrorCodes::InvalidArgument,
            ListFilesAtVersionError::FetchBlockIdsError(e) => e.code(),
            ListFilesAtVersionError::VersionFileMissingCollectionId => ErrorCodes::InvalidArgument,
        }
    }

    fn should_trace_error(&self) -> bool {
        self.code() != ErrorCodes::NotFound
    }
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

        let mut file_paths = FilePathSet::new();
        let mut sparse_index_ids = HashMap::new();

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
                            let (prefix, hnsw_index_uuid) = Segment::extract_prefix_and_id(path)
                                .map_err(ListFilesAtVersionError::InvalidUuid)?;
                            for hnsw_file in FILES {
                                let s3_key = HnswIndexProvider::format_key(
                                    prefix,
                                    &IndexUuid(hnsw_index_uuid),
                                    hnsw_file,
                                );
                                file_paths.insert_path(s3_key);
                            }
                        }
                    } else {
                        // Must be a sparse index
                        for path in &segment_paths.paths {
                            let (prefix, sparse_index_uuid) = Segment::extract_prefix_and_id(path)
                                .map_err(ListFilesAtVersionError::InvalidUuid)?;
                            let file_path =
                                RootManager::get_storage_key(prefix, &sparse_index_uuid);

                            file_paths.insert_path(file_path);
                            sparse_index_ids.insert(sparse_index_uuid, prefix.to_string());
                        }
                    }
                }
            }
        }

        if !sparse_index_ids.is_empty() {
            let mut get_block_ids_stream = futures::stream::iter(sparse_index_ids)
                .map(|(sparse_index_id, file_prefix)|
                    async move {
                        match input.root_manager.get_all_block_ids(&sparse_index_id, &file_prefix).await {
                            Ok(block_ids) => Ok((block_ids, file_prefix)),
                            Err(RootManagerError::StorageGetError(StorageError::NotFound { .. })) => {
                                tracing::debug!(
                                    "Sparse index {} not found in storage. Assuming it was previously deleted.",
                                    sparse_index_id
                                );
                                Ok((vec![], file_prefix))
                            }
                            Err(e) => Err(e),
                        }
                }).buffer_unordered(100);

            while let Some(res) = get_block_ids_stream.next().await {
                let block_ids = res.map_err(ListFilesAtVersionError::FetchBlockIdsError)?;

                for block_id in block_ids.0 {
                    file_paths.insert_block(&block_ids.1, block_id);
                }
            }
        }

        Ok(ListFilesAtVersionOutput {
            collection_id,
            version: input.version,
            file_paths,
        })
    }
}
