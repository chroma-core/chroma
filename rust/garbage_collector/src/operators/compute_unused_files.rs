use async_trait::async_trait;
use chroma_blockstore::RootManager;
use chroma_cache::nop::NopCache;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_system::{Operator, OperatorType};
use chroma_types::chroma_proto::CollectionVersionHistory;
use chroma_types::chroma_proto::{
    CollectionSegmentInfo, CollectionVersionFile, CollectionVersionInfo,
    FlushSegmentCompactionInfo, VersionListForCollection,
};
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct ComputeUnusedFilesOperator {
    pub collection_id: String,
}

impl ComputeUnusedFilesOperator {
    pub fn new(collection_id: String) -> Self {
        Self { collection_id }
    }

    /// Compute unused files between two successive versions.
    /// Returns a tuple of (unused_files, unused_hnsw_prefixes).
    async fn compute_unused_between_successive_versions(
        &self,
        older_version: i64,
        newer_version: i64,
        version_to_segment_info: HashMap<i64, CollectionSegmentInfo>,
        storage: Storage,
    ) -> Result<(Vec<String>, Vec<String>), ComputeUnusedFilesError> {
        let mut unused_s3_files = Vec::new();
        let mut unused_hnsw_prefixes = Vec::new();

        let older_segment_info = version_to_segment_info.get(&older_version).ok_or(
            ComputeUnusedFilesError::VersionFileMissingContent(older_version),
        )?;
        let newer_segment_info = version_to_segment_info.get(&newer_version).ok_or(
            ComputeUnusedFilesError::VersionFileMissingContent(newer_version),
        )?;

        let mut older_si_ids = Vec::new();
        for (_idx, segment_compaction_info) in older_segment_info
            .segment_compaction_info
            .iter()
            .enumerate()
        {
            for (file_type, file_paths) in &segment_compaction_info.file_paths {
                // For hnsw_index files, just add it without comparing with newer version.
                if file_type == "hnsw_index" {
                    unused_hnsw_prefixes.extend(file_paths.paths.clone());
                    continue;
                }
                for file_path in &file_paths.paths {
                    older_si_ids.push(file_path.clone());
                }
            }
        }

        let mut newer_si_ids = Vec::new();
        for (_idx, segment_compaction_info) in newer_segment_info
            .segment_compaction_info
            .iter()
            .enumerate()
        {
            for (_file_type, file_paths) in &segment_compaction_info.file_paths {
                for file_path in &file_paths.paths {
                    newer_si_ids.push(file_path.clone());
                }
            }
        }

        let unused = self
            .compute_unused_files(older_si_ids, newer_si_ids, storage)
            .await?;
        unused_s3_files.extend(unused);

        Ok((unused_s3_files, unused_hnsw_prefixes))
    }

    async fn compute_unused_files(
        &self,
        older_si_ids: Vec<String>,
        newer_si_ids: Vec<String>,
        storage: Storage,
    ) -> Result<Vec<String>, ComputeUnusedFilesError> {
        let s3_files_older_version = self
            .s3_files_in_version(older_si_ids, storage.clone())
            .await?;
        let s3_files_newer_version = self.s3_files_in_version(newer_si_ids, storage).await?;

        let older_set: HashSet<_> = s3_files_older_version.into_iter().collect();
        let newer_set: HashSet<_> = s3_files_newer_version.into_iter().collect();

        let unused = older_set.difference(&newer_set).cloned().collect();
        Ok(unused)
    }

    async fn s3_files_in_version(
        &self,
        si_ids: Vec<String>,
        storage: Storage,
    ) -> Result<Vec<String>, ComputeUnusedFilesError> {
        let mut s3_files = Vec::new();
        let root_manager = RootManager::new(storage.clone(), Box::new(NopCache));

        for si_id in si_ids {
            let uuid = Uuid::parse_str(&si_id).map_err(|e| {
                tracing::error!(error = %e, "Failed to parse UUID");
                ComputeUnusedFilesError::ParseError(si_id.clone(), e.to_string())
            })?;

            let block_ids = match root_manager.get_all_block_ids(&uuid).await {
                Ok(ids) => ids,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to get block IDs");
                    return Err(ComputeUnusedFilesError::ParseError(si_id, e.to_string()));
                }
            };
            s3_files.extend(block_ids.iter().map(|id| format!("block/{}", id)));
        }
        Ok(s3_files)
    }
}

#[derive(Clone)]
pub struct ComputeUnusedFilesInput {
    pub version_file: CollectionVersionFile,
    pub storage: Storage,
    pub versions_to_delete: VersionListForCollection,
    pub oldest_version_to_keep: i64,
}

impl std::fmt::Debug for ComputeUnusedFilesInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComputeUnusedFilesInput")
            .field("version_file", &self.version_file)
            .field("storage", &"<Storage>") // Skip detailed debug for Storage
            .field("versions_to_delete", &self.versions_to_delete)
            .field("oldest_version_to_keep", &self.oldest_version_to_keep)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct ComputeUnusedFilesOutput {
    pub unused_files: Vec<String>,
    pub unused_hnsw_prefixes: Vec<String>,
}

#[derive(Error, Debug)]
pub enum ComputeUnusedFilesError {
    #[error("Error parsing sparse index file: {0}")]
    ParseError(String, String),
    #[error("Version file has missing content")]
    VersionFileMissingContent(i64),
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
        tracing::debug!(
            collection_id = self.collection_id,
            "Computing unused files for collection"
        );

        let version_file = input.version_file.clone();
        let storage = input.storage.clone();

        let mut output = ComputeUnusedFilesOutput {
            unused_files: Vec::new(),
            unused_hnsw_prefixes: Vec::new(),
        };

        let mut versions = input.versions_to_delete.versions.clone();
        versions.sort_unstable(); // sort ascending

        // Build a map to version to segment_info
        let mut version_to_segment_info = HashMap::new();
        let version_history = version_file.version_history.unwrap_or_default();

        for v in version_history.versions.iter() {
            // A version may appear multiple times in version_file.version_history
            // But each entry will contain the same segment_info, so its safe to
            // add multiple times.
            if let Some(segment_info) = &v.segment_info {
                version_to_segment_info.insert(v.version, segment_info.clone());
            }
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
                    storage.clone(),
                )
                .await?;
            output.unused_files.extend(unused_s3_files);
            output.unused_hnsw_prefixes.extend(unused_hnsw_prefixes);
        }

        // Special case: Compare last version to be deleted with oldest version to keep
        let (unused_s3_files, unused_hnsw_prefixes) = self
            .compute_unused_between_successive_versions(
                *versions.last().unwrap(),
                input.oldest_version_to_keep,
                version_to_segment_info.clone(),
                storage.clone(),
            )
            .await?;
        output.unused_files.extend(unused_s3_files);
        output.unused_hnsw_prefixes.extend(unused_hnsw_prefixes);

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, BlockfileWriterOptions};
    use chroma_cache::UnboundedCacheConfig;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_types::chroma_proto::{
        self, CollectionSegmentInfo, CollectionVersionInfo, FilePaths, FlushSegmentCompactionInfo,
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

    // Helper function to create a test blockfile and return its UUID
    async fn create_test_blockfile(storage: &Storage, data: Vec<(&str, u32)>) -> Uuid {
        let block_cache = Box::new(UnboundedCacheConfig {}.build());
        let sparse_index_cache = Box::new(UnboundedCacheConfig {}.build());
        let provider = ArrowBlockfileProvider::new(
            storage.clone(),
            1024 * 1024,
            block_cache,
            sparse_index_cache,
        );

        let writer = provider
            .write::<&str, u32>(BlockfileWriterOptions::new().ordered_mutations())
            .await
            .unwrap();

        for (key, value) in data {
            writer.set(key, value, 1).await.unwrap();
        }

        let writer_id = writer.id();
        let flusher = writer.commit::<&str, u32>().await.unwrap();
        flusher.flush::<&str, u32>().await.unwrap();
        writer_id
    }

    #[tokio::test]
    async fn test_compute_unused_between_successive_versions() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));

        // Create test blockfiles with real UUIDs
        let uuid1 = create_test_blockfile(&storage, vec![("key1", 1)]).await;
        let uuid2 = create_test_blockfile(&storage, vec![("key2", 2)]).await;
        let uuid3 = create_test_blockfile(&storage, vec![("key3", 3)]).await;

        // Create version_to_segment_info map
        let mut version_to_segment_info = HashMap::new();

        // Older version has files 1 and 2
        version_to_segment_info.insert(
            1,
            create_test_segment_info(vec![
                (
                    "data".to_string(),
                    vec![uuid1.to_string(), uuid2.to_string()],
                ),
                ("hnsw_index".to_string(), vec!["hnsw1".to_string()]),
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

        let operator = ComputeUnusedFilesOperator::new("test_collection".to_string());

        let (unused_files, unused_hnsw_prefixes) = operator
            .compute_unused_between_successive_versions(1, 2, version_to_segment_info, storage)
            .await
            .unwrap();

        // Check that file1 is marked as unused
        assert!(unused_files.contains(&format!("block/{}", uuid1)));
        // Check that file2 is not marked as unused
        assert!(!unused_files.contains(&format!("block/{}", uuid2)));
        // Check that file3 is not marked as unused
        assert!(!unused_files.contains(&format!("block/{}", uuid3)));
        // Check that hnsw1 is marked as unused
        assert!(unused_hnsw_prefixes.contains(&"hnsw1".to_string()));
    }

    #[tokio::test]
    async fn test_run_operator() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));

        // Create actual blockfiles with real UUIDs
        let uuid1 = create_test_blockfile(&storage, vec![("key1", 1)]).await;
        let uuid2 = create_test_blockfile(&storage, vec![("key2", 2)]).await;
        let uuid3 = create_test_blockfile(&storage, vec![("key3", 3)]).await;

        let operator = ComputeUnusedFilesOperator::new("test_collection".to_string());

        let input = ComputeUnusedFilesInput {
            oldest_version_to_keep: 3,
            version_file: chroma_proto::CollectionVersionFile {
                collection_info_immutable: None,
                version_history: Some(CollectionVersionHistory {
                    versions: vec![
                        CollectionVersionInfo {
                            version: 1,
                            segment_info: Some(create_test_segment_info(vec![
                                ("data".to_string(), vec![uuid1.to_string()]),
                                ("hnsw_index".to_string(), vec!["hnsw1".to_string()]),
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
                    ],
                }),
            },
            storage,
            versions_to_delete: chroma_proto::VersionListForCollection {
                versions: vec![1, 2],
                collection_id: "test_collection".to_string(),
                database_id: "test_db".to_string(),
                tenant_id: "test_tenant".to_string(),
            },
        };

        let result = operator.run(&input).await.unwrap();

        // Verify results
        assert!(result.unused_files.contains(&format!("block/{}", uuid1)));
        assert!(result.unused_files.contains(&format!("block/{}", uuid2)));
        assert!(!result.unused_files.contains(&format!("block/{}", uuid3)));
        assert!(result.unused_hnsw_prefixes.contains(&"hnsw1".to_string()));
    }

    #[tokio::test]
    async fn test_error_handling() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));

        let operator = ComputeUnusedFilesOperator::new("test_collection".to_string());

        // Create input with missing version info
        let input = ComputeUnusedFilesInput {
            version_file: chroma_proto::CollectionVersionFile {
                collection_info_immutable: None,
                version_history: Some(CollectionVersionHistory {
                    versions: vec![], // Empty version history wrapped in Some
                }),
            },
            storage,
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
            Err(ComputeUnusedFilesError::VersionFileMissingContent(_))
        ));
    }
}
