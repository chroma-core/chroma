use async_trait::async_trait;
use chroma_blockstore::{
    arrow::provider::{BlockManager, RootManagerError},
    RootManager,
};
use chroma_cache::nop::NopCache;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::{
    hnsw_provider::{HnswIndexProvider, FILES},
    IndexUuid,
};
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
use chroma_types::{
    chroma_proto::{CollectionSegmentInfo, CollectionVersionFile, VersionListForCollection},
    Segment, HNSW_PATH,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use thiserror::Error;

#[derive(Clone)]
pub struct ComputeUnusedFilesOperator {
    pub collection_id: String,
    root_manager: RootManager,
    min_versions_to_keep: u64,
}

impl std::fmt::Debug for ComputeUnusedFilesOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComputeUnusedFilesOperator")
            .field("collection_id", &self.collection_id)
            .field("root_manager", &"<RootManager>") // Skip detailed debug for RootManager
            .field("min_versions_to_keep", &self.min_versions_to_keep)
            .finish()
    }
}

impl ComputeUnusedFilesOperator {
    pub fn new(collection_id: String, storage: Storage, min_versions_to_keep: u64) -> Self {
        Self {
            collection_id,
            root_manager: RootManager::new(storage, Box::new(NopCache)),
            min_versions_to_keep,
        }
    }

    /// Compute unused files between two successive versions.
    /// Returns a tuple of (unused_block_ids, unused_hnsw_prefixes).
    async fn compute_unused_between_successive_versions(
        &self,
        older_version: i64,
        newer_version: i64,
        version_to_segment_info: HashMap<i64, CollectionSegmentInfo>,
    ) -> Result<(Vec<String>, Vec<String>), ComputeUnusedFilesError> {
        let mut unused_s3_files = Vec::new();
        let mut unused_hnsw_prefixes = Vec::new();

        let older_segment_info = version_to_segment_info.get(&older_version).ok_or(
            ComputeUnusedFilesError::VersionFileMissingContent(older_version),
        )?;
        let newer_segment_info = version_to_segment_info.get(&newer_version).ok_or(
            ComputeUnusedFilesError::VersionFileMissingContent(newer_version),
        )?;

        // Print the older and newer segment info
        tracing::debug!(
            line = line!(),
            "ComputeUnusedFilesOperator: older_version: {}, older_segment_info: \n{:?}",
            older_version,
            older_segment_info
        );
        tracing::debug!(
            line = line!(),
            "ComputeUnusedFilesOperator: newer_version: {}, newer_segment_info: \n{:?}",
            newer_version,
            newer_segment_info
        );

        let mut older_si_ids = Vec::new();
        for segment_compaction_info in older_segment_info.segment_compaction_info.iter() {
            for (file_type, file_paths) in &segment_compaction_info.file_paths {
                // For hnsw_index files, just add it without comparing with newer version.
                if file_type == "hnsw_index" || file_type == HNSW_PATH {
                    for file_path in file_paths.paths.iter() {
                        let (prefix, hnsw_uuid) = Segment::extract_prefix_and_id(file_path)
                            .map_err(|e| {
                                tracing::error!(error = %e, "Failed to extract prefix and ID");
                                ComputeUnusedFilesError::InvalidUuid(e, file_path.to_string())
                            })?;
                        for file in FILES.iter() {
                            let hnsw_prefix =
                                HnswIndexProvider::format_key(prefix, &IndexUuid(hnsw_uuid), file);
                            tracing::debug!(
                                line = line!(),
                                "ComputeUnusedFilesOperator: unused_hnsw_prefix: {:?}",
                                hnsw_prefix
                            );
                            unused_hnsw_prefixes.push(hnsw_prefix);
                        }
                    }
                    continue;
                }
                for file_path in &file_paths.paths {
                    tracing::debug!(
                        line = line!(),
                        "ComputeUnusedFilesOperator: file_type: {:?}, file_path: {:?}",
                        file_type,
                        file_path
                    );
                    older_si_ids.push(file_path.clone());
                }
            }
        }

        let mut newer_si_ids = Vec::new();
        for segment_compaction_info in newer_segment_info.segment_compaction_info.iter() {
            for (file_type, file_paths) in &segment_compaction_info.file_paths {
                if file_type == "hnsw_index" || file_type == HNSW_PATH {
                    continue;
                }

                for file_path in &file_paths.paths {
                    newer_si_ids.push(file_path.clone());
                }
            }
        }

        let unused = self
            .compute_unused_files(older_si_ids, newer_si_ids)
            .await?;
        unused_s3_files.extend(unused);

        Ok((unused_s3_files, unused_hnsw_prefixes))
    }

    async fn compute_unused_files(
        &self,
        older_si_ids: Vec<String>,
        newer_si_ids: Vec<String>,
    ) -> Result<Vec<String>, ComputeUnusedFilesError> {
        let s3_files_older_version = self.s3_files_in_version(older_si_ids).await?;
        let s3_files_newer_version = self.s3_files_in_version(newer_si_ids).await?;

        let older_set: HashSet<_> = s3_files_older_version.into_iter().collect();
        let newer_set: HashSet<_> = s3_files_newer_version.into_iter().collect();

        tracing::debug!(
            line = line!(),
            "ComputeUnusedFilesOperator: older_set: \n{:?}\nnewer_set: \n{:?}",
            older_set,
            newer_set
        );
        let unused = older_set.difference(&newer_set).cloned().collect();
        tracing::debug!(
            line = line!(),
            "ComputeUnusedFilesOperator: unused: \n{:?}",
            unused
        );
        Ok(unused)
    }

    async fn s3_files_in_version(
        &self,
        si_ids: Vec<String>,
    ) -> Result<Vec<String>, ComputeUnusedFilesError> {
        let mut s3_files = Vec::new();

        for si_path in si_ids {
            let (prefix, si_id) = Segment::extract_prefix_and_id(&si_path).map_err(|e| {
                tracing::error!(error = %e, "Failed to parse UUID");
                ComputeUnusedFilesError::InvalidUuid(e, si_path.to_string())
            })?;

            let block_ids = match self.root_manager.get_all_block_ids(&si_id, prefix).await {
                Ok(ids) => ids,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to get block IDs");
                    return Err(ComputeUnusedFilesError::FailedToFetchBlockIDs(e));
                }
            };
            tracing::debug!(
                line = line!(),
                "ComputeUnusedFilesOperator: s3_files_in_version: si_id: {:?}, block_ids: {:?}",
                si_id,
                block_ids
            );
            s3_files.extend(
                block_ids
                    .iter()
                    .map(|id| BlockManager::format_key(prefix, id)),
            );
        }
        Ok(s3_files)
    }
}

#[derive(Clone)]
pub struct ComputeUnusedFilesInput {
    pub version_file: Arc<CollectionVersionFile>,
    pub versions_to_delete: VersionListForCollection,
    pub oldest_version_to_keep: i64,
}

impl std::fmt::Debug for ComputeUnusedFilesInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComputeUnusedFilesInput")
            .field("version_file", &self.version_file)
            .field("versions_to_delete", &self.versions_to_delete)
            .field("oldest_version_to_keep", &self.oldest_version_to_keep)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct ComputeUnusedFilesOutput {
    pub unused_block_ids: Vec<String>,
    pub unused_hnsw_prefixes: Vec<String>,
}

#[derive(Error, Debug)]
pub enum ComputeUnusedFilesError {
    #[error("Error parsing UUID: {0} from {1}")]
    InvalidUuid(uuid::Error, String),
    #[error("Failed to fetch block IDs: {0}")]
    FailedToFetchBlockIDs(RootManagerError),
    #[error("Version file has missing content")]
    VersionFileMissingContent(i64),
    #[error("Version history is missing")]
    MissingVersionHistory,
    #[error("Cannot delete versions: would leave fewer than minimum required versions ({0})")]
    InsufficientVersionsRemaining(u64),
    #[error("Invalid input to the operator.")]
    InvalidInputToOperator(String),
}

impl ChromaError for ComputeUnusedFilesError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<ComputeUnusedFilesInput, ComputeUnusedFilesOutput> for ComputeUnusedFilesOperator {
    type Error = ComputeUnusedFilesError;

    fn get_type(&self) -> OperatorType {
        OperatorType::Other
    }

    async fn run(
        &self,
        input: &ComputeUnusedFilesInput,
    ) -> Result<ComputeUnusedFilesOutput, Self::Error> {
        let version_file = input.version_file.clone();

        let mut output = ComputeUnusedFilesOutput {
            unused_block_ids: Vec::new(),
            unused_hnsw_prefixes: Vec::new(),
        };

        let mut versions = input.versions_to_delete.versions.clone();
        versions.sort_unstable(); // sort ascending

        // Build a map to version to segment_info
        let mut version_to_segment_info = HashMap::new();
        let version_history = version_file
            .as_ref()
            .version_history
            .as_ref()
            .ok_or(ComputeUnusedFilesError::MissingVersionHistory)?;

        // Check if version history is empty
        if version_history.versions.is_empty() {
            return Err(ComputeUnusedFilesError::MissingVersionHistory);
        }

        for v in version_history.versions.iter() {
            // A version may appear multiple times in version_file.version_history
            // But each entry will contain the same segment_info, so its safe to
            // add multiple times.
            if let Some(segment_info) = &v.segment_info {
                version_to_segment_info.insert(v.version, segment_info.clone());
            }
        }

        // Count unique versions in version history using a HashSet
        let total_versions = version_history
            .versions
            .iter()
            .map(|v| v.version)
            .collect::<HashSet<_>>()
            .len() as u64;
        let versions_to_delete = input.versions_to_delete.versions.len() as u64;

        // Add check to prevent underflow
        if versions_to_delete > total_versions {
            return Err(ComputeUnusedFilesError::InvalidInputToOperator(
                "versions to delete are greater than total versions".to_string(),
            ));
        }

        let versions_remaining = total_versions - versions_to_delete;

        if versions_remaining < self.min_versions_to_keep {
            return Err(ComputeUnusedFilesError::InsufficientVersionsRemaining(
                self.min_versions_to_keep,
            ));
        }

        // Compare each successive version pair. The last version is not compared after this loop.
        for version_window in versions.windows(2) {
            let older_version = version_window[0];
            let newer_version = version_window[1];

            let (unused_s3_files, unused_hnsw_prefixes) = self
                .compute_unused_between_successive_versions(
                    older_version,
                    newer_version,
                    version_to_segment_info.clone(),
                )
                .await?;
            output.unused_block_ids.extend(unused_s3_files);
            output.unused_hnsw_prefixes.extend(unused_hnsw_prefixes);
        }

        // Special case: Compare last version to be deleted with the next higher version.
        // Note that the next higher version need not be the oldest version to keep.
        // Eg: Oldest version to keep (due to min_versions_to_keep) is 4. Versions to delete are 1 and 2.
        let (unused_s3_files, unused_hnsw_prefixes) = self
            .compute_unused_between_successive_versions(
                *versions.last().unwrap(),
                *versions.last().unwrap() + 1,
                version_to_segment_info.clone(),
            )
            .await?;
        output.unused_block_ids.extend(unused_s3_files);
        output.unused_hnsw_prefixes.extend(unused_hnsw_prefixes);

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_blockstore::test_utils::sparse_index_test_utils;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::chroma_proto::{
        self, CollectionSegmentInfo, CollectionVersionHistory, CollectionVersionInfo, FilePaths,
        FlushSegmentCompactionInfo,
    };
    use std::collections::HashMap;
    use tempfile::TempDir;
    use uuid::Uuid;

    // Helper function to create test segment info
    fn create_test_segment_info(file_paths: Vec<(String, Vec<String>)>) -> CollectionSegmentInfo {
        let mut file_path_map = HashMap::new();
        for (file_type, paths) in file_paths {
            file_path_map.insert(file_type, FilePaths { paths });
        }

        CollectionSegmentInfo {
            segment_compaction_info: vec![FlushSegmentCompactionInfo {
                segment_id: "test".to_string(),
                file_paths: file_path_map,
            }],
        }
    }

    async fn create_sparse_index_file(
        storage: &Storage,
        block_ids: Vec<Uuid>,
    ) -> Result<Uuid, Box<dyn ChromaError>> {
        // Use the test utility function to create a sparse index
        let root_id = sparse_index_test_utils::create_test_sparse_index(
            storage,
            Uuid::new_v4(),
            block_ids,
            None, // Use default "test" prefix
            "".to_string(),
        )
        .await?;

        Ok(root_id)
    }

    /// Generates a vector of random UUIDs with length between 5 and 30
    fn generate_random_block_ids() -> Vec<Uuid> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let count = rng.gen_range(5..=50);
        (0..count).map(|_| Uuid::new_v4()).collect()
    }

    #[tokio::test]
    async fn test_compute_unused_between_successive_versions() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));

        // Replace the manual block_ids creation with the helper
        let block_ids1 = generate_random_block_ids();
        let block_ids2 = generate_random_block_ids();
        let block_ids3 = generate_random_block_ids();

        let uuid1 = create_sparse_index_file(&storage, block_ids1.clone())
            .await
            .unwrap();
        let uuid2 = create_sparse_index_file(&storage, block_ids2.clone())
            .await
            .unwrap();
        let uuid3 = create_sparse_index_file(&storage, block_ids3.clone())
            .await
            .unwrap();

        // Create version_to_segment_info map
        let mut version_to_segment_info = HashMap::new();
        let hnsw_id = IndexUuid(uuid::Uuid::new_v4());
        let prefix_path = "";
        // Older version has files 1 and 2
        version_to_segment_info.insert(
            1,
            create_test_segment_info(vec![
                (
                    "data".to_string(),
                    vec![uuid1.to_string(), uuid2.to_string()],
                ),
                ("hnsw_index".to_string(), vec![hnsw_id.to_string()]),
            ]),
        );

        // Newer version has files 2 and 3
        version_to_segment_info.insert(
            2,
            create_test_segment_info(vec![(
                "data".to_string(),
                vec![uuid2.to_string(), uuid3.to_string()],
            )]),
        );

        let operator = ComputeUnusedFilesOperator::new(
            "test_collection".to_string(),
            storage.clone(),
            1, // Add minimum versions parameter
        );

        let (unused_files, unused_hnsw_prefixes) = operator
            .compute_unused_between_successive_versions(1, 2, version_to_segment_info)
            .await
            .unwrap();

        // Check that file1's blocks are marked as unused
        for block_id in block_ids1 {
            let s3_key = BlockManager::format_key(prefix_path, &block_id);
            assert!(unused_files.contains(&s3_key));
        }
        // Check that file2's blocks are not marked as unused
        for block_id in block_ids2 {
            let s3_key = BlockManager::format_key(prefix_path, &block_id);
            assert!(!unused_files.contains(&s3_key));
        }
        // Check that file3's blocks are not marked as unused
        for block_id in block_ids3 {
            let s3_key = BlockManager::format_key(prefix_path, &block_id);
            assert!(!unused_files.contains(&s3_key));
        }
        // Check that hnsw is marked as unused
        for file in FILES.iter() {
            let s3_key = HnswIndexProvider::format_key(prefix_path, &hnsw_id, file);
            assert!(unused_hnsw_prefixes.contains(&s3_key));
        }
    }

    #[tokio::test]
    async fn test_run_operator() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));

        // Create sparse index files with block IDs
        let block_ids1 = vec![Uuid::new_v4(), Uuid::new_v4()];
        let block_ids2 = vec![Uuid::new_v4(), Uuid::new_v4()];
        let block_ids3 = vec![Uuid::new_v4(), Uuid::new_v4()];
        let block_ids4 = vec![Uuid::new_v4(), Uuid::new_v4()]; // Added fourth set of block IDs

        let uuid1 = create_sparse_index_file(&storage, block_ids1.clone())
            .await
            .unwrap();
        let uuid2 = create_sparse_index_file(&storage, block_ids2.clone())
            .await
            .unwrap();
        let uuid3 = create_sparse_index_file(&storage, block_ids3.clone())
            .await
            .unwrap();
        let uuid4 = create_sparse_index_file(&storage, block_ids4.clone()) // Added fourth UUID
            .await
            .unwrap();

        let operator =
            ComputeUnusedFilesOperator::new("test_collection".to_string(), storage.clone(), 2);

        let hnsw_id = IndexUuid(uuid::Uuid::new_v4());

        let input = ComputeUnusedFilesInput {
            oldest_version_to_keep: 3,
            version_file: Arc::new(chroma_proto::CollectionVersionFile {
                collection_info_immutable: None,
                version_history: Some(CollectionVersionHistory {
                    versions: vec![
                        CollectionVersionInfo {
                            version: 1,
                            segment_info: Some(create_test_segment_info(vec![
                                ("data".to_string(), vec![uuid1.to_string()]),
                                ("hnsw_index".to_string(), vec![hnsw_id.to_string()]),
                            ])),
                            collection_info_mutable: None,
                            created_at_secs: 0,
                            version_change_reason: 0,
                            version_file_name: String::new(),
                            marked_for_deletion: false,
                        },
                        CollectionVersionInfo {
                            version: 2,
                            segment_info: Some(create_test_segment_info(vec![(
                                "data".to_string(),
                                vec![uuid2.to_string()],
                            )])),
                            collection_info_mutable: None,
                            created_at_secs: 0,
                            version_change_reason: 0,
                            version_file_name: String::new(),
                            marked_for_deletion: false,
                        },
                        CollectionVersionInfo {
                            version: 3,
                            segment_info: Some(create_test_segment_info(vec![(
                                "data".to_string(),
                                vec![uuid3.to_string()],
                            )])),
                            collection_info_mutable: None,
                            created_at_secs: 0,
                            version_change_reason: 0,
                            version_file_name: String::new(),
                            marked_for_deletion: false,
                        },
                        CollectionVersionInfo {
                            // Added fourth version
                            version: 4,
                            segment_info: Some(create_test_segment_info(vec![(
                                "data".to_string(),
                                vec![uuid4.to_string()],
                            )])),
                            collection_info_mutable: None,
                            created_at_secs: 0,
                            version_change_reason: 0,
                            version_file_name: String::new(),
                            marked_for_deletion: false,
                        },
                    ],
                }),
            }),
            versions_to_delete: chroma_proto::VersionListForCollection {
                versions: vec![1, 2],
                collection_id: "test_collection".to_string(),
                database_id: "test_db".to_string(),
                tenant_id: "test_tenant".to_string(),
            },
        };

        let result = operator.run(&input).await.unwrap();

        let prefix_path = "";
        // Verify results - check all block IDs from uuid1 and uuid2 are marked unused
        for block_id in block_ids1 {
            let s3_key = BlockManager::format_key(prefix_path, &block_id);
            assert!(result.unused_block_ids.contains(&s3_key));
        }
        for block_id in block_ids2 {
            let s3_key = BlockManager::format_key(prefix_path, &block_id);
            assert!(result.unused_block_ids.contains(&s3_key));
        }
        // Check uuid3's blocks are not marked as unused
        for block_id in block_ids3 {
            let s3_key = BlockManager::format_key(prefix_path, &block_id);
            assert!(!result.unused_block_ids.contains(&s3_key));
        }
        // Check uuid4's blocks are not marked as unused
        for block_id in block_ids4 {
            let s3_key = BlockManager::format_key(prefix_path, &block_id);
            assert!(!result.unused_block_ids.contains(&s3_key));
        }
        // Check that hnsw is marked as unused
        for file in FILES.iter() {
            let s3_key = HnswIndexProvider::format_key(prefix_path, &hnsw_id, file);
            assert!(result.unused_hnsw_prefixes.contains(&s3_key));
        }
    }

    #[tokio::test]
    async fn test_error_handling() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));

        let operator = ComputeUnusedFilesOperator::new(
            "test_collection".to_string(),
            storage.clone(),
            1, // Add minimum versions parameter
        );

        // Create input with missing version info
        let input = ComputeUnusedFilesInput {
            version_file: Arc::new(chroma_proto::CollectionVersionFile {
                collection_info_immutable: None,
                version_history: Some(CollectionVersionHistory {
                    versions: vec![], // Empty version history wrapped in Some
                }),
            }),
            versions_to_delete: chroma_proto::VersionListForCollection {
                versions: vec![1, 2], // Versions that don't exist in history
                collection_id: "test_collection".to_string(),
                database_id: "test_db".to_string(),
                tenant_id: "test_tenant".to_string(),
            },
            oldest_version_to_keep: 3,
        };

        let result = operator.run(&input).await;

        // Verify we get the expected error
        assert!(matches!(
            result,
            Err(ComputeUnusedFilesError::MissingVersionHistory)
        ));
    }

    #[tokio::test]
    async fn test_minimum_versions_check() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));

        let operator = ComputeUnusedFilesOperator::new(
            "test_collection".to_string(),
            storage.clone(),
            3, // Require at least 3 versions to be kept
        );

        let input = ComputeUnusedFilesInput {
            version_file: Arc::new(chroma_proto::CollectionVersionFile {
                collection_info_immutable: None,
                version_history: Some(CollectionVersionHistory {
                    versions: vec![
                        CollectionVersionInfo {
                            version: 1,
                            segment_info: None,
                            collection_info_mutable: None,
                            created_at_secs: 0,
                            version_change_reason: 0,
                            version_file_name: String::new(),
                            marked_for_deletion: false,
                        },
                        CollectionVersionInfo {
                            version: 2,
                            segment_info: None,
                            collection_info_mutable: None,
                            created_at_secs: 0,
                            version_change_reason: 0,
                            version_file_name: String::new(),
                            marked_for_deletion: false,
                        },
                        CollectionVersionInfo {
                            version: 3,
                            segment_info: None,
                            collection_info_mutable: None,
                            created_at_secs: 0,
                            version_change_reason: 0,
                            version_file_name: String::new(),
                            marked_for_deletion: false,
                        },
                    ],
                }),
            }),
            versions_to_delete: chroma_proto::VersionListForCollection {
                versions: vec![1, 2], // Try to delete 2 versions
                collection_id: "test_collection".to_string(),
                database_id: "test_db".to_string(),
                tenant_id: "test_tenant".to_string(),
            },
            oldest_version_to_keep: 3,
        };

        let result = operator.run(&input).await;
        assert!(matches!(
            result,
            Err(ComputeUnusedFilesError::InsufficientVersionsRemaining(3))
        ));
    }
}
