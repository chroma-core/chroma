use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::chroma_proto::{CollectionVersionFile, VersionListForCollection};
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
    pub sysdb_client: Box<SysDb>,
    pub versions_to_delete: VersionListForCollection,
}

#[derive(Debug)]
pub struct FetchSparseIndexFilesOutput {
    pub version_file: CollectionVersionFile,
    pub epoch_id: i64,
    pub sysdb_client: Box<SysDb>,
    pub versions_to_delete: VersionListForCollection,
    pub version_to_content: HashMap<i64, Vec<u8>>,
}

#[derive(Error, Debug)]
pub enum FetchSparseIndexFilesError {
    #[error("Error fetching file from S3: {0}")]
    S3Error(String),
    #[error("File not found for version: {0}")]
    FileNotFound(i64),
}

impl ChromaError for FetchSparseIndexFilesError {
    fn code(&self) -> ErrorCodes {
        match self {
            FetchSparseIndexFilesError::S3Error(_) => ErrorCodes::Internal,
            FetchSparseIndexFilesError::FileNotFound(_) => ErrorCodes::NotFound,
        }
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

        // Extract file paths from CollectionVersionFile for the versions we want to delete
        for version in &input.versions_to_delete.versions {
            if let Some(version_info) = input
                .version_file
                .version_history
                .as_ref()
                .and_then(|history| history.versions.iter().find(|v| v.version == *version))
            {
                // Get segment info from the version
                if let Some(segment_info) = &version_info.segment_info {
                    for segment_compaction_info in &segment_info.segment_compaction_info {
                        // Iterate through file paths for each segment
                        for file_paths in segment_compaction_info.file_paths.values() {
                            // Attempt to fetch each file
                            for file_path in &file_paths.paths {
                                match self.storage.get(&file_path).await {
                                    Ok(content) => {
                                        version_to_content.insert(*version, (*content).to_vec());
                                    }
                                    Err(e) => {
                                        return Err(FetchSparseIndexFilesError::S3Error(format!(
                                            "Failed to fetch file for version {}: {}",
                                            version, e
                                        )));
                                    }
                                }
                            }
                        }
                    }
                } else {
                    return Err(FetchSparseIndexFilesError::FileNotFound(*version));
                }
            } else {
                return Err(FetchSparseIndexFilesError::FileNotFound(*version));
            }
        }

        Ok(FetchSparseIndexFilesOutput {
            version_file: input.version_file.clone(),
            epoch_id: input.epoch_id,
            sysdb_client: input.sysdb_client.clone(),
            versions_to_delete: input.versions_to_delete.clone(),
            version_to_content,
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
