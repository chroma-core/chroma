use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{Operator, OperatorType};
use chroma_types::chroma_proto::{CollectionVersionFile, VersionListForCollection};
use chrono::{DateTime, Utc};
use rand::Rng;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct ComputeVersionsToDeleteOperator {}

#[derive(Debug)]
pub struct ComputeVersionsToDeleteInput {
    pub version_file: CollectionVersionFile,
    pub cutoff_time: DateTime<Utc>,
    pub min_versions_to_keep: u32,
}

#[derive(Debug)]
pub struct ComputeVersionsToDeleteOutput {
    pub version_file: CollectionVersionFile,
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
        let mut version_file = input.version_file.clone();
        let collection_info = version_file
            .collection_info_immutable
            .as_ref()
            .ok_or_else(|| {
                tracing::error!("Missing collection info in version file");
                ComputeVersionsToDeleteError::ComputeError("Missing collection info".to_string())
            })?;

        tracing::debug!(
            tenant = %collection_info.tenant_id,
            database = %collection_info.database_id,
            collection = %collection_info.collection_id,
            "Processing collection to compute versions to delete"
        );

        let mut marked_versions = Vec::new();
        let mut oldest_version_to_keep = 0;

        if let Some(ref mut version_history) = version_file.version_history {
            tracing::debug!(
                "Processing {} versions in history",
                version_history.versions.len()
            );

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

            // Second pass: mark for deletion if older than oldest_kept AND before cutoff
            for version in version_history.versions.iter_mut() {
                if version.version != 0
                    && version.version < oldest_version_to_keep
                    && version.created_at_secs < input.cutoff_time.timestamp()
                {
                    tracing::info!(
                        "Marking version {} for deletion (created at {})",
                        version.version,
                        version.created_at_secs
                    );
                    version.marked_for_deletion = true;
                    marked_versions.push(version.version);
                }
            }
        } else {
            tracing::warn!("No version history found in version file");
        }

        tracing::info!("Marked {} versions for deletion", marked_versions.len());

        let versions_to_delete = VersionListForCollection {
            tenant_id: collection_info.tenant_id.clone(),
            database_id: collection_info.database_id.clone(),
            collection_id: collection_info.collection_id.clone(),
            versions: marked_versions,
        };

        println!(
            "versions to delete: {:?}, oldest version to keep: {}",
            versions_to_delete, oldest_version_to_keep
        );
        Ok(ComputeVersionsToDeleteOutput {
            version_file,
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
    use proptest::collection::vec;
    use proptest::prelude::*;

    #[tokio::test]
    async fn test_compute_versions_to_delete() {
        let now = Utc::now();
        let operator = ComputeVersionsToDeleteOperator {};

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

        let version_file = CollectionVersionFile {
            version_history: Some(version_history),
            collection_info_immutable: Some(CollectionInfoImmutable {
                tenant_id: "test_tenant".to_string(),
                database_id: "test_db".to_string(),
                collection_id: "test_collection".to_string(),
                dimension: 0,
                ..Default::default()
            }),
        };

        let input = ComputeVersionsToDeleteInput {
            version_file,
            cutoff_time: now - Duration::hours(20),
            min_versions_to_keep: 2,
        };

        let result = operator.run(&input).await.unwrap();

        // Verify the results
        let versions = &result.version_file.version_history.unwrap().versions;
        assert!(versions[0].marked_for_deletion);
        assert!(versions[1].marked_for_deletion);
        assert!(!versions[2].marked_for_deletion); // Version 2 should be kept
        assert!(!versions[3].marked_for_deletion); // Version 3 should be kept
    }

    /// Helper function to create a version file with given versions
    fn create_version_file(versions: Vec<CollectionVersionInfo>) -> CollectionVersionFile {
        CollectionVersionFile {
            version_history: Some(CollectionVersionHistory { versions }),
            collection_info_immutable: Some(CollectionInfoImmutable {
                tenant_id: "test_tenant".to_string(),
                database_id: "test_db".to_string(),
                collection_id: "test_collection".to_string(),
                dimension: 0,
                ..Default::default()
            }),
        }
    }

    /// Helper function to create version info with guaranteed ordering
    fn create_ordered_version_infos(
        versions: Vec<i64>,
        base_time: DateTime<Utc>,
    ) -> Vec<CollectionVersionInfo> {
        let mut rng = rand::thread_rng();

        // First sort by version number
        let mut version_infos = versions;
        version_infos.sort();

        // Start from the most recent version (highest version number)
        // and work backwards, ensuring each previous version has an earlier timestamp
        let mut timestamps: Vec<DateTime<Utc>> = Vec::with_capacity(version_infos.len());
        let mut current_time = base_time;

        for _ in version_infos.iter().rev() {
            timestamps.push(current_time);
            // Generate a random time difference between 1 minute and 1 hour
            let minutes_diff = rng.gen_range(1..=60);
            current_time = current_time - Duration::minutes(minutes_diff);
        }
        timestamps.reverse(); // Reverse to match version order

        // Create the final version infos
        version_infos
            .into_iter()
            .zip(timestamps)
            .map(|(version, timestamp)| CollectionVersionInfo {
                version,
                created_at_secs: timestamp.timestamp(),
                marked_for_deletion: false,
                ..Default::default()
            })
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        #[test]
        fn prop_always_keeps_minimum_versions(
            min_versions_to_keep in 1u32..10u32,
            additional_versions in 0u32..90u32,
            cutoff_hours in 1i64..168i64
        ) {
            let total_versions = min_versions_to_keep + additional_versions;
            let versions: Vec<i64> = (1..=total_versions as i64).collect();

            let now = Utc::now();
            let version_infos = create_ordered_version_infos(versions, now);
            let version_file = create_version_file(version_infos);
            let input = ComputeVersionsToDeleteInput {
                version_file,
                cutoff_time: now - Duration::hours(cutoff_hours),
                min_versions_to_keep,
            };

            let operator = ComputeVersionsToDeleteOperator {};
            let result = tokio_test::block_on(operator.run(&input)).unwrap();

            // Count unique versions that are not marked for deletion
            let versions = &result.version_file.version_history.unwrap().versions;
            let mut unique_kept_versions = versions.iter()
                .filter(|v| !v.marked_for_deletion)
                .map(|v| v.version)
                .collect::<Vec<_>>();
            unique_kept_versions.sort();
            unique_kept_versions.dedup();

            prop_assert!(
                unique_kept_versions.len() >= min_versions_to_keep as usize,
                "Expected at least {} versions to be kept, but only {} were kept",
                min_versions_to_keep,
                unique_kept_versions.len()
            );
        }

        #[test]
        fn prop_respects_cutoff_time(
            min_versions_to_keep in 1u32..10u32,
            additional_versions in 0u32..90u32,
            cutoff_hours in 1i64..168i64
        ) {
            let total_versions = min_versions_to_keep + additional_versions;
            let versions: Vec<i64> = (1..=total_versions as i64).collect();

            let now = Utc::now();
            let version_infos = create_ordered_version_infos(versions, now);
            let version_file = create_version_file(version_infos);
            let cutoff_time = now - Duration::hours(cutoff_hours);
            let input = ComputeVersionsToDeleteInput {
                version_file,
                cutoff_time,
                min_versions_to_keep,
            };

            let operator = ComputeVersionsToDeleteOperator {};
            let result = tokio_test::block_on(operator.run(&input)).unwrap();

            // Verify no versions newer than cutoff_time are marked for deletion
            let versions = &result.version_file.version_history.unwrap().versions;
            for version in versions {
                if version.created_at_secs >= cutoff_time.timestamp() {
                    prop_assert!(!version.marked_for_deletion,
                        "Version {} created at {} should not be marked for deletion as it's newer than cutoff {}",
                        version.version,
                        version.created_at_secs,
                        cutoff_time.timestamp()
                    );
                }
            }
        }

        #[test]
        fn prop_version_zero_never_deleted(
            min_versions_to_keep in 1u32..10u32,
            additional_versions in 0u32..90u32,
            cutoff_hours in 1i64..168i64
        ) {
            let total_versions = min_versions_to_keep + additional_versions;
            // Start from 0 for this test to include version 0
            let versions: Vec<i64> = (0..=total_versions as i64).collect();

            let now = Utc::now();
            let version_infos = create_ordered_version_infos(versions, now);
            let version_file = create_version_file(version_infos);
            let input = ComputeVersionsToDeleteInput {
                version_file,
                cutoff_time: now - Duration::hours(cutoff_hours),
                min_versions_to_keep,
            };

            let operator = ComputeVersionsToDeleteOperator {};
            let result = tokio_test::block_on(operator.run(&input)).unwrap();

            // Verify version 0 is never marked for deletion
            let versions = &result.version_file.version_history.unwrap().versions;
            for version in versions {
                if version.version == 0 {
                    prop_assert!(!version.marked_for_deletion,
                        "Version 0 should never be marked for deletion"
                    );
                }
            }
        }

        #[test]
        fn prop_versions_are_chronologically_ordered(
            min_versions_to_keep in 1u32..10u32,
            additional_versions in 0u32..90u32,
            cutoff_hours in 1i64..168i64
        ) {
            let total_versions = min_versions_to_keep + additional_versions;
            let versions: Vec<i64> = (1..=total_versions as i64).collect();

            let now = Utc::now();
            let version_infos = create_ordered_version_infos(versions, now);

            // Verify that higher version numbers have later timestamps
            let version_file = create_version_file(version_infos);
            let versions = &version_file.version_history.as_ref().unwrap().versions;

            for window in versions.windows(2) {
                if window[0].version < window[1].version {
                    prop_assert!(
                        window[0].created_at_secs <= window[1].created_at_secs,
                        "Version {} (timestamp {}) should have earlier or equal timestamp than version {} (timestamp {})",
                        window[0].version,
                        window[0].created_at_secs,
                        window[1].version,
                        window[1].created_at_secs
                    );
                }
            }

            // Run the operator to ensure it works correctly with ordered versions
            let input = ComputeVersionsToDeleteInput {
                version_file,
                cutoff_time: now - Duration::hours(cutoff_hours),
                min_versions_to_keep,
            };

            let operator = ComputeVersionsToDeleteOperator {};
            let _ = tokio_test::block_on(operator.run(&input)).unwrap();
        }
    }
}
