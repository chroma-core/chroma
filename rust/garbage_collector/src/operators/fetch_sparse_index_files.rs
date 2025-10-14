use async_trait::async_trait;
use chroma_blockstore::RootManager;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::{admissioncontrolleds3::StorageRequestPriority, GetOptions, Storage};
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::{
    chroma_proto::{CollectionVersionFile, VersionListForCollection},
    Segment, HNSW_PATH,
};
use std::collections::HashMap;
use thiserror::Error;

impl std::fmt::Debug for FetchSparseIndexFilesOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchSparseIndexFilesOperator").finish()
    }
}

pub struct FetchSparseIndexFilesOperator {
    pub storage: Storage,
}

#[derive(Debug)]
pub struct FetchSparseIndexFilesInput {
    pub version_file: CollectionVersionFile,
    pub epoch_id: i64,
    pub sysdb_client: SysDb,
    pub versions_to_delete: VersionListForCollection,
    pub oldest_version_to_keep: i64,
}

#[derive(Debug)]
pub struct FetchSparseIndexFilesOutput {
    pub version_file: CollectionVersionFile,
    pub epoch_id: i64,
    pub sysdb_client: SysDb,
    pub versions_to_delete: VersionListForCollection,
    pub version_to_content: HashMap<i64, HashMap<String, Vec<u8>>>,
    pub oldest_version_to_keep: i64,
    pub hnsw_prefixes_for_deletion: Vec<String>,
}

#[derive(Error, Debug)]
pub enum FetchSparseIndexFilesError {
    #[error("Error fetching file from S3: {0}")]
    S3Error(String),
    #[error("File not found for version: {0}")]
    FileNotFound(i64),
    #[error("Error parsing id")]
    ParsingIdFailed,
}

impl ChromaError for FetchSparseIndexFilesError {
    fn code(&self) -> ErrorCodes {
        match self {
            FetchSparseIndexFilesError::S3Error(_) => ErrorCodes::Internal,
            FetchSparseIndexFilesError::FileNotFound(_) => ErrorCodes::NotFound,
            FetchSparseIndexFilesError::ParsingIdFailed => ErrorCodes::Internal,
        }
    }

    fn should_trace_error(&self) -> bool {
        self.code() != ErrorCodes::NotFound
    }
}

#[async_trait]
impl Operator<FetchSparseIndexFilesInput, FetchSparseIndexFilesOutput>
    for FetchSparseIndexFilesOperator
{
    type Error = FetchSparseIndexFilesError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &FetchSparseIndexFilesInput,
    ) -> Result<FetchSparseIndexFilesOutput, FetchSparseIndexFilesError> {
        let mut version_to_content = HashMap::new();

        tracing::info!(
            num_versions = input.versions_to_delete.versions.len(),
            oldest_to_keep = input.oldest_version_to_keep,
            "Starting to fetch files for versions to delete plus oldest to keep"
        );

        // Combine versions to delete with the oldest version to keep
        let mut versions_to_fetch = input.versions_to_delete.versions.clone();
        versions_to_fetch.push(input.oldest_version_to_keep);

        let mut hnsw_prefixes_for_deletion = Vec::new();
        tracing::info!(
            num_versions = versions_to_fetch.len(),
            "Starting to fetch files for {} versions to delete plus oldest to keep",
            versions_to_fetch.len()
        );

        // Extract file paths from CollectionVersionFile for the versions we want to delete
        for version in &versions_to_fetch {
            tracing::info!(version = *version, "Processing version {}", version);
            if let Some(version_info) = input
                .version_file
                .version_history
                .as_ref()
                .and_then(|history| history.versions.iter().find(|v| v.version == *version))
            {
                // Get segment info from the version
                if let Some(segment_info) = &version_info.segment_info {
                    let mut version_files = HashMap::new();
                    let mut total_files_fetched = 0;
                    let mut total_bytes_fetched = 0;

                    for (idx, segment_compaction_info) in
                        segment_info.segment_compaction_info.iter().enumerate()
                    {
                        tracing::info!(segment = idx, "Processing Segment at index {}", idx);
                        // Iterate through file paths for each segment
                        for (file_type, file_paths) in &segment_compaction_info.file_paths {
                            tracing::info!(
                                file_type = file_type,
                                "Processing file type {}",
                                file_type
                            );
                            // Skip hnsw_index files
                            if file_type == "hnsw_index" || file_type == HNSW_PATH {
                                if *version == input.oldest_version_to_keep {
                                    continue;
                                }
                                // Add the hnsw_index files to the hnsw_prefixes_for_deletion vector
                                hnsw_prefixes_for_deletion.extend(file_paths.paths.clone());
                                continue;
                            }
                            // Attempt to fetch each file
                            for file_path in &file_paths.paths {
                                let (prefix, file_uuid) = Segment::extract_prefix_and_id(file_path)
                                    .map_err(|_| FetchSparseIndexFilesError::ParsingIdFailed)?;
                                let s3_key = RootManager::get_storage_key(prefix, &file_uuid);
                                match self
                                    .storage
                                    .get(&s3_key, GetOptions::new(StorageRequestPriority::P0))
                                    .await
                                {
                                    Ok(content) => {
                                        total_files_fetched += 1;
                                        total_bytes_fetched += content.len();
                                        tracing::info!(
                                            "      ✓ Fetched:  {} ({} bytes)",
                                            s3_key,
                                            content.len()
                                        );
                                        version_files
                                            .insert(file_path.clone(), (*content).to_vec());
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "Failed to fetch file {} for version {}: {}",
                                            s3_key,
                                            version,
                                            e
                                        );
                                        return Err(FetchSparseIndexFilesError::S3Error(format!(
                                            "Failed to fetch file {} for version {}: {}",
                                            s3_key, version, e
                                        )));
                                    }
                                }
                            }
                        }
                    }

                    // Summary for this version
                    tracing::info!(
                        total_files_fetched = total_files_fetched,
                        total_bytes_fetched = total_bytes_fetched,
                        "Version {} Summary: Total files fetched: {}, Total bytes fetched: {} bytes",
                        version,
                        total_files_fetched,
                        total_bytes_fetched
                    );

                    // Only insert if we found any files
                    if !version_files.is_empty() {
                        version_to_content.insert(*version, version_files);
                    }
                } else {
                    tracing::error!("No segment info found for version {}", version);
                    return Err(FetchSparseIndexFilesError::FileNotFound(*version));
                }
            } else {
                tracing::error!("Version {} not found in version history", version);
                return Err(FetchSparseIndexFilesError::FileNotFound(*version));
            }
        }

        Ok(FetchSparseIndexFilesOutput {
            version_file: input.version_file.clone(),
            epoch_id: input.epoch_id,
            sysdb_client: input.sysdb_client.clone(),
            versions_to_delete: input.versions_to_delete.clone(),
            version_to_content,
            oldest_version_to_keep: input.oldest_version_to_keep,
            hnsw_prefixes_for_deletion,
        })
    }
}

#[cfg(test)]
mod tests {
    // Add tests here
    #[tokio::test]
    async fn test_fetch_files_success() {
        // Implement test
    }

    #[tokio::test]
    async fn test_fetch_files_not_found() {
        // Implement test
    }
}
