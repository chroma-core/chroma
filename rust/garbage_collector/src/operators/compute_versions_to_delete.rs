use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{Operator, OperatorType};
use chroma_types::chroma_proto::{CollectionVersionFile, VersionListForCollection};
use chrono::{DateTime, Utc};
use humantime::format_duration;
use std::{sync::Arc, time::Duration};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct ComputeVersionsToDeleteOperator {}

#[derive(Debug)]
pub struct ComputeVersionsToDeleteInput {
    pub version_file: Arc<CollectionVersionFile>,
    pub cutoff_time: DateTime<Utc>,
    pub min_versions_to_keep: u32,
}

#[derive(Debug)]
pub struct ComputeVersionsToDeleteOutput {
    pub version_file: Arc<CollectionVersionFile>,
    pub versions_to_delete: VersionListForCollection,
    pub oldest_version_to_keep: i64,
}

#[derive(Error, Debug)]
pub enum ComputeVersionsToDeleteError {
    #[error("Error computing versions to delete: {0}")]
    ComputeError(String),
    #[error("Invalid timestamp in version file")]
    InvalidTimestamp,
    #[error("Error parsing version file: {0}")]
    ParseError(#[from] prost::DecodeError),
}

impl ChromaError for ComputeVersionsToDeleteError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<ComputeVersionsToDeleteInput, ComputeVersionsToDeleteOutput>
    for ComputeVersionsToDeleteOperator
{
    type Error = ComputeVersionsToDeleteError;

    fn get_type(&self) -> OperatorType {
        OperatorType::Other
    }

    async fn run(
        &self,
        input: &ComputeVersionsToDeleteInput,
    ) -> Result<ComputeVersionsToDeleteOutput, ComputeVersionsToDeleteError> {
        let version_file = input.version_file.clone();
        let collection_info = version_file
            .collection_info_immutable
            .as_ref()
            .ok_or_else(|| {
                tracing::error!("Missing collection info in version file");
                ComputeVersionsToDeleteError::ComputeError("Missing collection info".to_string())
            })?;

        tracing::info!(
            tenant = %collection_info.tenant_id,
            database = %collection_info.database_id,
            collection = %collection_info.collection_id,
            "Processing collection to compute versions to delete"
        );

        let mut marked_versions = Vec::new();
        let mut oldest_version_to_keep = 0;

        let mut version_file = version_file.as_ref().clone();
        if let Some(ref mut version_history) = version_file.version_history {
            let mut unique_versions_seen = 0;
            let mut last_version = None;

            // First pass: find the oldest version that must be kept
            for version in version_history.versions.iter().rev() {
                if last_version != Some(version.version) {
                    unique_versions_seen += 1;
                    oldest_version_to_keep = version.version;
                    if unique_versions_seen == input.min_versions_to_keep {
                        break;
                    }
                    last_version = Some(version.version);
                }
            }

            tracing::info!(
                "Oldest version to keep: {}, min versions to keep: {}, cutoff time: {}, total versions: {}",
                oldest_version_to_keep,
                input.min_versions_to_keep,
                input.cutoff_time,
                version_history.versions.len()
            );

            // Second pass: mark for deletion if older than oldest_version_to_keep AND before cutoff
            for version in version_history.versions.iter_mut() {
                if version.version == 0 {
                    tracing::info!("Skipping version 0");
                    continue;
                }

                if version.version >= oldest_version_to_keep {
                    tracing::info!(
                        "Keeping version {} (created at {}) because it's greater than or equal to {}",
                        version.version,
                        version.created_at_secs,
                        oldest_version_to_keep
                    );
                    continue;
                }

                if version.created_at_secs >= input.cutoff_time.timestamp() {
                    tracing::debug!(
                        "Keeping version {} (created at {}) because it's {} newer than cutoff time ({})",
                        version.version,
                        version.created_at_secs,
                        format_duration(Duration::from_secs(
                            (input.cutoff_time.timestamp() - version.created_at_secs) as u64,
                        )),
                        input.cutoff_time
                    );
                    continue;
                }

                version.marked_for_deletion = true;
                marked_versions.push(version.version);
            }
        } else {
            tracing::warn!("No version history found in version file");
        }

        let versions_to_delete = VersionListForCollection {
            tenant_id: collection_info.tenant_id.clone(),
            database_id: collection_info.database_id.clone(),
            collection_id: collection_info.collection_id.clone(),
            versions: marked_versions,
        };

        tracing::info!(
            "For collection: {}, Computed versions to delete: {:?}, oldest version to keep: {}",
            collection_info.collection_id,
            versions_to_delete,
            oldest_version_to_keep
        );

        Ok(ComputeVersionsToDeleteOutput {
            version_file: Arc::new(version_file),
            versions_to_delete,
            oldest_version_to_keep,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::chroma_proto::{
        CollectionInfoImmutable, CollectionVersionFile, CollectionVersionHistory,
        CollectionVersionInfo,
    };
    use chrono::{Duration, Utc};

    #[tokio::test]
    async fn test_compute_versions_to_delete() {
        let now = Utc::now();

        let version_history = CollectionVersionHistory {
            versions: vec![
                CollectionVersionInfo {
                    version: 1,
                    created_at_secs: (now - Duration::hours(24)).timestamp(),
                    marked_for_deletion: false,
                    ..Default::default()
                },
                CollectionVersionInfo {
                    version: 1,
                    created_at_secs: (now - Duration::hours(24)).timestamp(),
                    marked_for_deletion: false,
                    ..Default::default()
                },
                CollectionVersionInfo {
                    version: 2,
                    created_at_secs: now.timestamp(),
                    marked_for_deletion: false,
                    ..Default::default()
                },
                CollectionVersionInfo {
                    version: 3,
                    created_at_secs: (now - Duration::hours(1)).timestamp(),
                    marked_for_deletion: false,
                    ..Default::default()
                },
            ],
        };

        let version_file = Arc::new(CollectionVersionFile {
            version_history: Some(version_history),
            collection_info_immutable: Some(CollectionInfoImmutable {
                tenant_id: "test_tenant".to_string(),
                database_id: "test_db".to_string(),
                collection_id: "test_collection".to_string(),
                dimension: 0,
                ..Default::default()
            }),
        });

        let input = ComputeVersionsToDeleteInput {
            version_file,
            cutoff_time: now - Duration::hours(20),
            min_versions_to_keep: 2,
        };

        let result = ComputeVersionsToDeleteOperator {}
            .run(&input)
            .await
            .unwrap();

        // Verify the results.
        let versions = &result
            .version_file
            .version_history
            .as_ref()
            .unwrap()
            .versions;
        assert!(versions[0].marked_for_deletion);
        assert!(versions[1].marked_for_deletion);
        assert!(!versions[2].marked_for_deletion); // Version 2 should be kept.
        assert!(!versions[3].marked_for_deletion); // Version 3 should be kept.
    }
}
