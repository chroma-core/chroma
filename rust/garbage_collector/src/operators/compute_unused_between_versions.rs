use async_trait::async_trait;
use chroma_blockstore::RootManager;
use chroma_cache::nop::NopCache;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::{
    chroma_proto::{CollectionVersionFile, VersionListForCollection},
    Segment,
};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Clone)]
pub struct ComputeUnusedBetweenVersionsOperator {
    storage: Storage,
}

impl ComputeUnusedBetweenVersionsOperator {
    pub fn new(storage: Storage) -> Self {
        tracing::debug!("Creating new ComputeUnusedBetweenVersionsOperator");
        Self { storage }
    }

    /// Extract S3 file references from all sparse index files for a version
    async fn extract_s3_files_from_version(
        &self,
        version_files: &HashMap<String, Vec<u8>>,
    ) -> Result<HashSet<String>, String> {
        tracing::info!(
            num_files = version_files.len(),
            files = ?version_files.keys().collect::<Vec<_>>(),
            "Starting to extract S3 files from version"
        );

        let mut all_s3_files = HashSet::new();
        let root_manager = RootManager::new(self.storage.clone(), Box::new(NopCache));

        for file_path in version_files.keys() {
            tracing::info!(file_path = %file_path, "Processing sparse index file");

            let (prefix, id) = match Segment::extract_prefix_and_id(file_path) {
                Ok(id) => {
                    tracing::debug!(uuid = %id.1, "Successfully parsed UUID from file path");
                    id
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        file_path = %file_path,
                        "Failed to parse UUID from file path"
                    );
                    return Err(format!(
                        "Failed to parse UUID from file path {}: {}",
                        file_path, e
                    ));
                }
            };

            // Use RootManager to get block IDs
            let block_ids = match root_manager.get_all_block_ids(&id, prefix).await {
                Ok(ids) => {
                    tracing::debug!(
                        uuid = %id,
                        num_blocks = ids.len(),
                        block_ids = ?ids,
                        "Successfully retrieved block IDs"
                    );
                    ids
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        uuid = %id,
                        "Failed to get block IDs"
                    );
                    return Err(format!("Failed to get block IDs for {}: {}", file_path, e));
                }
            };

            // Convert block IDs to S3 paths and add them to the set
            let s3_paths: Vec<String> = block_ids
                .into_iter()
                .map(|id| RootManager::get_storage_key(prefix, &id))
                .collect();

            tracing::info!(
                file_path = %file_path,
                num_paths = s3_paths.len(),
                "Found S3 paths for sparse index file"
            );
            all_s3_files.extend(s3_paths);
        }

        tracing::info!(
            total_s3_files = all_s3_files.len(),
            "Completed extracting all S3 files from version"
        );
        Ok(all_s3_files)
    }

    /// Compare two versions and return files that are in older_version but not in newer_version
    fn compute_unused_files(
        older_files: &HashSet<String>,
        newer_files: &HashSet<String>,
    ) -> HashSet<String> {
        tracing::info!(
            older_count = older_files.len(),
            newer_count = newer_files.len(),
            "Computing unused files between versions"
        );

        tracing::info!(
            older_files = ?older_files,
            "Files in older version"
        );
        tracing::info!(
            newer_files = ?newer_files,
            "Files in newer version"
        );

        let unused = older_files
            .difference(newer_files)
            .cloned()
            .collect::<HashSet<_>>();

        tracing::info!(
            unused_count = unused.len(),
            "Found unused files between versions"
        );
        unused
    }
}

impl std::fmt::Debug for ComputeUnusedBetweenVersionsOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComputeUnusedBetweenVersionsOperator")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct ComputeUnusedBetweenVersionsInput {
    pub version_file: CollectionVersionFile,
    pub epoch_id: i64,
    pub sysdb_client: SysDb,
    pub versions_to_delete: VersionListForCollection,
    pub version_to_content: HashMap<i64, HashMap<String, Vec<u8>>>,
    pub oldest_version_to_keep: i64,
}

#[derive(Debug)]
pub struct ComputeUnusedBetweenVersionsOutput {
    pub version_file: CollectionVersionFile,
    pub epoch_id: i64,
    pub sysdb_client: SysDb,
    pub versions_to_delete: VersionListForCollection,
    pub unused_s3_files: HashSet<String>,
    pub oldest_version_to_keep: i64,
}

#[derive(Error, Debug)]
pub enum ComputeUnusedBetweenVersionsError {
    #[error("Error parsing sparse index file for version {0}: {1}")]
    ParseError(i64, String),
    #[error("Missing content for version: {0}")]
    MissingContent(i64),
}

impl ChromaError for ComputeUnusedBetweenVersionsError {
    fn code(&self) -> ErrorCodes {
        match self {
            ComputeUnusedBetweenVersionsError::ParseError(_, _) => ErrorCodes::Internal,
            ComputeUnusedBetweenVersionsError::MissingContent(_) => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<ComputeUnusedBetweenVersionsInput, ComputeUnusedBetweenVersionsOutput>
    for ComputeUnusedBetweenVersionsOperator
{
    type Error = ComputeUnusedBetweenVersionsError;

    fn get_type(&self) -> OperatorType {
        OperatorType::Other
    }

    async fn run(
        &self,
        input: &ComputeUnusedBetweenVersionsInput,
    ) -> Result<ComputeUnusedBetweenVersionsOutput, ComputeUnusedBetweenVersionsError> {
        tracing::info!(
            num_versions = input.versions_to_delete.versions.len(),
            oldest_to_keep = input.oldest_version_to_keep,
            "Starting to compute unused files between versions"
        );

        let mut unused_s3_files = HashSet::new();
        let mut versions = input.versions_to_delete.versions.clone();
        versions.sort_unstable();

        tracing::info!(
            versions = ?versions,
            "Processing versions in order"
        );

        // Process each pair of consecutive versions
        for versions_window in versions.windows(2) {
            let older_version = versions_window[0];
            let newer_version = versions_window[1];

            tracing::info!(
                older = older_version,
                newer = newer_version,
                "Comparing consecutive versions"
            );

            // Get content for both versions
            let older_files = input
                .version_to_content
                .get(&older_version)
                .ok_or_else(|| {
                    tracing::error!(version = older_version, "Missing content for older version");
                    ComputeUnusedBetweenVersionsError::MissingContent(older_version)
                })?;
            let newer_files = input
                .version_to_content
                .get(&newer_version)
                .ok_or_else(|| {
                    tracing::error!(version = newer_version, "Missing content for newer version");
                    ComputeUnusedBetweenVersionsError::MissingContent(newer_version)
                })?;

            // Extract S3 files from both versions
            let older_s3_files = self
                .extract_s3_files_from_version(older_files)
                .await
                .map_err(|e| {
                    tracing::error!(
                        error = %e,
                        version = older_version,
                        "Failed to extract S3 files from older version"
                    );
                    ComputeUnusedBetweenVersionsError::ParseError(older_version, e)
                })?;
            let newer_s3_files = self
                .extract_s3_files_from_version(newer_files)
                .await
                .map_err(|e| {
                    tracing::error!(
                        error = %e,
                        version = newer_version,
                        "Failed to extract S3 files from newer version"
                    );
                    ComputeUnusedBetweenVersionsError::ParseError(newer_version, e)
                })?;

            // Find files that are in older version but not in newer version
            let unused_in_this_pair = Self::compute_unused_files(&older_s3_files, &newer_s3_files);
            unused_s3_files.extend(unused_in_this_pair);
        }

        // Special case: Compare last version to be deleted with oldest version to keep
        if let Some(last_version) = versions.last() {
            tracing::info!(
                last_version,
                oldest_to_keep = input.oldest_version_to_keep,
                "Comparing last version to delete with oldest version to keep"
            );

            let last_files = input.version_to_content.get(last_version).ok_or_else(|| {
                tracing::error!(version = last_version, "Missing content for last version");
                ComputeUnusedBetweenVersionsError::MissingContent(*last_version)
            })?;
            let keep_files = input
                .version_to_content
                .get(&input.oldest_version_to_keep)
                .ok_or_else(|| {
                    tracing::error!(
                        version = input.oldest_version_to_keep,
                        "Missing content for version to keep"
                    );
                    ComputeUnusedBetweenVersionsError::MissingContent(input.oldest_version_to_keep)
                })?;

            let last_s3_files = self
                .extract_s3_files_from_version(last_files)
                .await
                .map_err(|e| {
                    tracing::error!(
                        error = %e,
                        version = last_version,
                        "Failed to extract S3 files from last version"
                    );
                    ComputeUnusedBetweenVersionsError::ParseError(*last_version, e)
                })?;
            let keep_s3_files = self
                .extract_s3_files_from_version(keep_files)
                .await
                .map_err(|e| {
                    tracing::error!(
                        error = %e,
                        version = input.oldest_version_to_keep,
                        "Failed to extract S3 files from version to keep"
                    );
                    ComputeUnusedBetweenVersionsError::ParseError(input.oldest_version_to_keep, e)
                })?;

            let unused_in_last = Self::compute_unused_files(&last_s3_files, &keep_s3_files);
            unused_s3_files.extend(unused_in_last);
        }

        tracing::info!(
            total_unused = unused_s3_files.len(),
            "Completed computing all unused files"
        );

        Ok(ComputeUnusedBetweenVersionsOutput {
            version_file: input.version_file.clone(),
            epoch_id: input.epoch_id,
            sysdb_client: input.sysdb_client.clone(),
            versions_to_delete: input.versions_to_delete.clone(),
            unused_s3_files,
            oldest_version_to_keep: input.oldest_version_to_keep,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_blockstore::{
        arrow::{config::BlockManagerConfig, provider::ArrowBlockfileProvider},
        BlockfileWriterOptions,
    };
    use chroma_cache::UnboundedCacheConfig;
    use chroma_storage::{local::LocalStorage, Storage};
    use chroma_sysdb::{SysDb, TestSysDb};
    use uuid::Uuid;

    async fn _create_sparse_index(storage: &Storage, keys: Vec<String>) -> Uuid {
        let block_cache = Box::new(UnboundedCacheConfig {}.build());
        let sparse_index_cache = Box::new(UnboundedCacheConfig {}.build());
        let provider = ArrowBlockfileProvider::new(
            storage.clone(),
            1024 * 1024,
            block_cache,
            sparse_index_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");

        // Create a new blockfile
        let writer = provider
            .write::<u32, u32>(BlockfileWriterOptions::new(prefix_path).ordered_mutations())
            .await
            .unwrap();

        // Write some test data
        for (i, key) in keys.iter().enumerate() {
            writer.set(key, i as u32, 1).await.unwrap();
        }

        let writer_id = writer.id();
        let flusher = writer.commit::<u32, u32>().await.unwrap();
        flusher.flush::<u32, u32>().await.unwrap();
        writer_id
    }

    #[tokio::test]
    async fn test_run_with_missing_content() {
        let storage = Storage::Local(LocalStorage::new("/tmp")); // Path doesn't matter for this test
        let operator = ComputeUnusedBetweenVersionsOperator::new(storage);

        let input = ComputeUnusedBetweenVersionsInput {
            version_file: CollectionVersionFile::default(),
            epoch_id: 1,
            sysdb_client: SysDb::Test(TestSysDb::new()),
            versions_to_delete: VersionListForCollection {
                versions: vec![1, 2],
                collection_id: "test_collection".to_string(),
                database_id: "test_database".to_string(),
                tenant_id: "test_tenant".to_string(),
            },
            version_to_content: HashMap::new(), // Empty content map
            oldest_version_to_keep: 1,
        };

        let result = operator.run(&input).await;
        assert!(matches!(
            result,
            Err(ComputeUnusedBetweenVersionsError::MissingContent(1))
        ));
    }

    // Keep the existing test_compute_unused_files test
    #[test]
    fn test_compute_unused_files() {
        let older_files: HashSet<String> = vec![
            "file1.bin".to_string(),
            "file2.bin".to_string(),
            "file3.bin".to_string(),
        ]
        .into_iter()
        .collect();

        let newer_files: HashSet<String> = vec![
            "file2.bin".to_string(),
            "file3.bin".to_string(),
            "file4.bin".to_string(),
        ]
        .into_iter()
        .collect();

        let unused =
            ComputeUnusedBetweenVersionsOperator::compute_unused_files(&older_files, &newer_files);

        assert_eq!(unused.len(), 1);
        assert!(unused.contains("file1.bin"));
    }
}
