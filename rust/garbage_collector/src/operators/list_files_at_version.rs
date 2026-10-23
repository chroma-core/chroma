use crate::types::FilePathSet;
use async_trait::async_trait;
use chroma_blockstore::{arrow::provider::RootManagerError, RootManager};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{
    hnsw_provider::{HnswIndexProvider, FILES},
    usearch::USearchIndex,
    IndexUuid,
};
use chroma_storage::StorageError;
use chroma_system::{Operator, OperatorType};
use chroma_types::{
    chroma_proto::CollectionVersionFile, CollectionUuid, Segment, HNSW_PATH,
    QUANTIZED_SPANN_QUANTIZED_CENTROID, QUANTIZED_SPANN_RAW_CENTROID, USER_ID_BLOOM_FILTER,
};
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
                    } else if file_type == QUANTIZED_SPANN_RAW_CENTROID
                        || file_type == QUANTIZED_SPANN_QUANTIZED_CENTROID
                    {
                        let quantized = file_type == QUANTIZED_SPANN_QUANTIZED_CENTROID;
                        for path in &segment_paths.paths {
                            let (prefix, id) = Segment::extract_prefix_and_id(path)
                                .map_err(ListFilesAtVersionError::InvalidUuid)?;
                            file_paths.insert_path(USearchIndex::format_storage_key(
                                prefix,
                                IndexUuid(id),
                                quantized,
                            ));
                        }
                    } else if file_type == USER_ID_BLOOM_FILTER {
                        for path in &segment_paths.paths {
                            file_paths.insert_path(path.clone());
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

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_blockstore::test_utils::sparse_index_test_utils;
    use chroma_cache::nop::NopCache;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_system::Operator;
    use chroma_types::chroma_proto::{
        CollectionInfoImmutable, CollectionSegmentInfo, CollectionVersionHistory,
        CollectionVersionInfo, FilePaths, FlushSegmentCompactionInfo,
    };
    use std::collections::HashMap;
    use tempfile::TempDir;
    use uuid::Uuid;

    fn create_version_file(
        collection_id: &str,
        versions: Vec<CollectionVersionInfo>,
    ) -> Arc<CollectionVersionFile> {
        Arc::new(CollectionVersionFile {
            collection_info_immutable: Some(CollectionInfoImmutable {
                collection_id: collection_id.to_string(),
                tenant_id: "test_tenant".to_string(),
                database_id: "test_db".to_string(),
                dimension: 0,
                ..Default::default()
            }),
            version_history: Some(CollectionVersionHistory { versions }),
        })
    }

    fn create_segment_info(file_paths: Vec<(String, Vec<String>)>) -> CollectionSegmentInfo {
        let mut file_path_map = HashMap::new();
        for (file_type, paths) in file_paths {
            file_path_map.insert(file_type, FilePaths { paths });
        }
        CollectionSegmentInfo {
            segment_compaction_info: vec![FlushSegmentCompactionInfo {
                segment_id: "test_segment".to_string(),
                file_paths: file_path_map,
            }],
        }
    }

    #[tokio::test]
    async fn test_bloom_filter_listed_as_direct_path() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
        let root_manager = RootManager::new(storage.clone(), Box::new(NopCache));

        let bf_path = format!("bloom_filter/{}", Uuid::new_v4());
        let collection_id = Uuid::new_v4().to_string();

        // Create a sparse index so we can verify it's handled separately
        let sparse_uuid = sparse_index_test_utils::create_test_sparse_index(
            &storage,
            Uuid::new_v4(),
            vec![Uuid::new_v4(), Uuid::new_v4()],
            None,
            "".to_string(),
        )
        .await
        .unwrap();

        let version_file = create_version_file(
            &collection_id,
            vec![CollectionVersionInfo {
                version: 1,
                segment_info: Some(create_segment_info(vec![
                    ("data".to_string(), vec![sparse_uuid.to_string()]),
                    (USER_ID_BLOOM_FILTER.to_string(), vec![bf_path.clone()]),
                ])),
                collection_info_mutable: None,
                created_at_secs: 0,
                version_change_reason: 0,
                version_file_name: String::new(),
                marked_for_deletion: false,
            }],
        );

        let operator = ListFilesAtVersionsOperator {};
        let input = ListFilesAtVersionInput::new(root_manager, version_file, 1);
        let output = operator.run(&input).await.unwrap();

        // The bloom filter path should appear in the output as-is
        let all_paths: std::collections::HashSet<String> = output.file_paths.iter().collect();
        assert!(
            all_paths.contains(&bf_path),
            "Bloom filter path should be listed directly in file_paths"
        );

        // The sparse index root + blocks should also be present (sanity check)
        assert!(
            all_paths.len() > 1,
            "Should also contain sparse index root and block files"
        );
    }

    #[tokio::test]
    async fn test_bloom_filter_not_treated_as_sparse_index() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
        let root_manager = RootManager::new(storage.clone(), Box::new(NopCache));

        let bf_path = format!("bloom_filter/{}", Uuid::new_v4());
        let collection_id = Uuid::new_v4().to_string();

        // Version with ONLY a bloom filter — no sparse indices.
        // If the bloom filter were treated as a sparse index, the operator would
        // try to fetch block IDs from storage and fail.
        let version_file = create_version_file(
            &collection_id,
            vec![CollectionVersionInfo {
                version: 1,
                segment_info: Some(create_segment_info(vec![(
                    USER_ID_BLOOM_FILTER.to_string(),
                    vec![bf_path.clone()],
                )])),
                collection_info_mutable: None,
                created_at_secs: 0,
                version_change_reason: 0,
                version_file_name: String::new(),
                marked_for_deletion: false,
            }],
        );

        let operator = ListFilesAtVersionsOperator {};
        let input = ListFilesAtVersionInput::new(root_manager, version_file, 1);

        // This would fail if bloom filter were routed to the sparse index branch
        let output = operator.run(&input).await.unwrap();

        let all_paths: std::collections::HashSet<String> = output.file_paths.iter().collect();
        assert_eq!(all_paths.len(), 1);
        assert!(all_paths.contains(&bf_path));
    }

    #[tokio::test]
    async fn test_hnsw_index_paths_are_expanded() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
        let root_manager = RootManager::new(storage, Box::new(NopCache));

        let index_id = Uuid::new_v4();
        let prefix = "hnsw-index";
        let collection_id = Uuid::new_v4().to_string();
        let version_file = create_version_file(
            &collection_id,
            vec![CollectionVersionInfo {
                version: 1,
                segment_info: Some(create_segment_info(vec![(
                    HNSW_PATH.to_string(),
                    vec![format!("{prefix}/{index_id}")],
                )])),
                collection_info_mutable: None,
                created_at_secs: 0,
                version_change_reason: 0,
                version_file_name: String::new(),
                marked_for_deletion: false,
            }],
        );

        let output = ListFilesAtVersionsOperator {}
            .run(&ListFilesAtVersionInput::new(root_manager, version_file, 1))
            .await
            .unwrap();

        let all_paths: std::collections::HashSet<String> = output.file_paths.iter().collect();
        let expected_paths: std::collections::HashSet<String> = FILES
            .iter()
            .map(|file| HnswIndexProvider::format_key(prefix, &IndexUuid(index_id), file))
            .collect();

        assert_eq!(all_paths, expected_paths);
    }

    #[tokio::test]
    async fn test_quantized_spann_paths_are_expanded() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
        let root_manager = RootManager::new(storage, Box::new(NopCache));

        let raw_id = Uuid::new_v4();
        let quantized_id = Uuid::new_v4();
        let collection_id = Uuid::new_v4().to_string();
        let version_file = create_version_file(
            &collection_id,
            vec![CollectionVersionInfo {
                version: 1,
                segment_info: Some(create_segment_info(vec![
                    (
                        QUANTIZED_SPANN_RAW_CENTROID.to_string(),
                        vec![format!("spann-raw/{raw_id}")],
                    ),
                    (
                        QUANTIZED_SPANN_QUANTIZED_CENTROID.to_string(),
                        vec![format!("spann-quantized/{quantized_id}")],
                    ),
                ])),
                collection_info_mutable: None,
                created_at_secs: 0,
                version_change_reason: 0,
                version_file_name: String::new(),
                marked_for_deletion: false,
            }],
        );

        let output = ListFilesAtVersionsOperator {}
            .run(&ListFilesAtVersionInput::new(root_manager, version_file, 1))
            .await
            .unwrap();

        let all_paths: std::collections::HashSet<String> = output.file_paths.iter().collect();
        let expected_paths = std::collections::HashSet::from([
            USearchIndex::format_storage_key("spann-raw", IndexUuid(raw_id), false),
            USearchIndex::format_storage_key("spann-quantized", IndexUuid(quantized_id), true),
        ]);

        assert_eq!(all_paths, expected_paths);
    }

    #[tokio::test]
    async fn test_sparse_index_not_found_keeps_root_path_only() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
        let root_manager = RootManager::new(storage.clone(), Box::new(NopCache));

        let sparse_index_id = Uuid::new_v4();
        let prefix = "sparse-index";
        let collection_id = Uuid::new_v4().to_string();
        let version_file = create_version_file(
            &collection_id,
            vec![CollectionVersionInfo {
                version: 1,
                segment_info: Some(create_segment_info(vec![(
                    "data".to_string(),
                    vec![format!("{prefix}/{sparse_index_id}")],
                )])),
                collection_info_mutable: None,
                created_at_secs: 0,
                version_change_reason: 0,
                version_file_name: String::new(),
                marked_for_deletion: false,
            }],
        );

        let output = ListFilesAtVersionsOperator {}
            .run(&ListFilesAtVersionInput::new(root_manager, version_file, 1))
            .await
            .unwrap();

        let all_paths: std::collections::HashSet<String> = output.file_paths.iter().collect();
        assert_eq!(
            all_paths,
            std::collections::HashSet::from([RootManager::get_storage_key(
                prefix,
                &sparse_index_id
            )])
        );
    }

    #[tokio::test]
    async fn test_missing_version_history_returns_error() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
        let root_manager = RootManager::new(storage, Box::new(NopCache));
        let version_file = Arc::new(CollectionVersionFile {
            collection_info_immutable: Some(CollectionInfoImmutable {
                collection_id: Uuid::new_v4().to_string(),
                tenant_id: "test_tenant".to_string(),
                database_id: "test_db".to_string(),
                dimension: 0,
                ..Default::default()
            }),
            version_history: None,
        });

        let err = ListFilesAtVersionsOperator {}
            .run(&ListFilesAtVersionInput::new(root_manager, version_file, 1))
            .await
            .expect_err("missing version history should fail");

        assert!(matches!(
            err,
            ListFilesAtVersionError::VersionHistoryMissing
        ));
    }

    #[tokio::test]
    async fn test_missing_collection_id_returns_error() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
        let root_manager = RootManager::new(storage, Box::new(NopCache));
        let version_file = Arc::new(CollectionVersionFile {
            collection_info_immutable: None,
            version_history: Some(CollectionVersionHistory { versions: vec![] }),
        });

        let err = ListFilesAtVersionsOperator {}
            .run(&ListFilesAtVersionInput::new(root_manager, version_file, 1))
            .await
            .expect_err("missing collection id should fail");

        assert!(matches!(
            err,
            ListFilesAtVersionError::VersionFileMissingCollectionId
        ));
    }

    #[tokio::test]
    async fn test_invalid_collection_uuid_returns_error() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
        let root_manager = RootManager::new(storage, Box::new(NopCache));
        let version_file = Arc::new(CollectionVersionFile {
            collection_info_immutable: Some(CollectionInfoImmutable {
                collection_id: "not-a-uuid".to_string(),
                tenant_id: "test_tenant".to_string(),
                database_id: "test_db".to_string(),
                dimension: 0,
                ..Default::default()
            }),
            version_history: Some(CollectionVersionHistory {
                versions: vec![CollectionVersionInfo {
                    version: 1,
                    segment_info: None,
                    collection_info_mutable: None,
                    created_at_secs: 0,
                    version_change_reason: 0,
                    version_file_name: String::new(),
                    marked_for_deletion: false,
                }],
            }),
        });

        let err = ListFilesAtVersionsOperator {}
            .run(&ListFilesAtVersionInput::new(root_manager, version_file, 1))
            .await
            .expect_err("invalid collection id should fail");

        assert!(matches!(err, ListFilesAtVersionError::InvalidUuid(_)));
    }

    #[tokio::test]
    async fn test_missing_requested_version_returns_error() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
        let root_manager = RootManager::new(storage, Box::new(NopCache));
        let version_file = create_version_file(
            &Uuid::new_v4().to_string(),
            vec![CollectionVersionInfo {
                version: 1,
                segment_info: None,
                collection_info_mutable: None,
                created_at_secs: 0,
                version_change_reason: 0,
                version_file_name: String::new(),
                marked_for_deletion: false,
            }],
        );

        let err = ListFilesAtVersionsOperator {}
            .run(&ListFilesAtVersionInput::new(root_manager, version_file, 2))
            .await
            .expect_err("missing version should fail");

        assert!(matches!(err, ListFilesAtVersionError::VersionNotFound(2)));
    }
}
