use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::chroma_proto::{CollectionVersionFile, VersionListForCollection};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct ComputeUnusedBetweenVersionsOperator {}

#[derive(Debug)]
pub struct ComputeUnusedBetweenVersionsInput {
    pub version_file: CollectionVersionFile,
    pub epoch_id: i64,
    pub sysdb_client: Box<SysDb>,
    pub versions_to_delete: VersionListForCollection,
    pub version_to_content: HashMap<i64, Vec<u8>>,
}

#[derive(Debug)]
pub struct ComputeUnusedBetweenVersionsOutput {
    pub version_file: CollectionVersionFile,
    pub epoch_id: i64,
    pub sysdb_client: Box<SysDb>,
    pub versions_to_delete: VersionListForCollection,
    pub unused_s3_files: HashSet<String>,
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

impl ComputeUnusedBetweenVersionsOperator {
    /// Extract S3 file references from a sparse index file content
    fn extract_s3_files(content: &[u8]) -> Result<HashSet<String>, String> {
        // TODO: Implement parsing of sparse index file to extract S3 file references
        // This will depend on the format of your sparse index files
        Ok(HashSet::new())
    }

    /// Compare two versions and return files that are in older_version but not in newer_version
    fn compute_unused_files(
        older_files: &HashSet<String>,
        newer_files: &HashSet<String>,
    ) -> HashSet<String> {
        older_files
            .difference(newer_files)
            .cloned()
            .collect::<HashSet<_>>()
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
        let mut unused_s3_files = HashSet::new();
        let mut versions = input.versions_to_delete.versions.clone();
        versions.sort_unstable(); // Sort versions to ensure we compare them in order

        // Process each pair of consecutive versions
        for versions_window in versions.windows(2) {
            let older_version = versions_window[0];
            let newer_version = versions_window[1];

            // Get content for both versions
            let older_content = input
                .version_to_content
                .get(&older_version)
                .ok_or_else(|| ComputeUnusedBetweenVersionsError::MissingContent(older_version))?;
            let newer_content = input
                .version_to_content
                .get(&newer_version)
                .ok_or_else(|| ComputeUnusedBetweenVersionsError::MissingContent(newer_version))?;

            // Extract S3 files from both versions
            let older_files = Self::extract_s3_files(older_content)
                .map_err(|e| ComputeUnusedBetweenVersionsError::ParseError(older_version, e))?;
            let newer_files = Self::extract_s3_files(newer_content)
                .map_err(|e| ComputeUnusedBetweenVersionsError::ParseError(newer_version, e))?;

            // Find files that are in older version but not in newer version
            let unused_in_this_pair = Self::compute_unused_files(&older_files, &newer_files);
            unused_s3_files.extend(unused_in_this_pair);
        }

        Ok(ComputeUnusedBetweenVersionsOutput {
            version_file: input.version_file.clone(),
            epoch_id: input.epoch_id,
            sysdb_client: input.sysdb_client.clone(),
            versions_to_delete: input.versions_to_delete.clone(),
            unused_s3_files,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[tokio::test]
    async fn test_run_with_multiple_versions() {
        // TODO: Implement test with multiple versions
        // This will require creating mock sparse index file contents
    }

    #[tokio::test]
    async fn test_run_with_missing_content() {
        // TODO: Implement test for missing content error case
    }
}
