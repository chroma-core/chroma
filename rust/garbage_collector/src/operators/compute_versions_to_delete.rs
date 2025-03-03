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

            // Second pass: mark for deletion if older than oldest_version_to_keep AND before cutoff
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
    use rand::Rng;

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

        let result = ComputeVersionsToDeleteOperator {}
            .run(&input)
            .await
            .unwrap();

        // Verify the results of the basic test.
        let versions = &result.version_file.version_history.unwrap().versions;
        assert!(versions[0].marked_for_deletion);
        assert!(versions[1].marked_for_deletion);
        assert!(!versions[2].marked_for_deletion); // Version 2 should be kept.
        assert!(!versions[3].marked_for_deletion); // Version 3 should be kept.
    }

    /// Helper: create a version file from a vector of version infos.
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

    /// Helper: create version infos with ordering so that higher version numbers get later timestamps.
    fn create_ordered_version_infos(
        versions: Vec<i64>,
        base_time: DateTime<Utc>,
    ) -> Vec<CollectionVersionInfo> {
        let mut rng = rand::thread_rng();

        // Ensure versions are in ascending order.
        let mut sorted_versions = versions.clone();
        sorted_versions.sort();

        // Generate timestamps such that each version has an earlier timestamp than the next.
        let mut timestamps: Vec<DateTime<Utc>> = Vec::with_capacity(sorted_versions.len());
        let mut current_time = base_time;
        for _ in sorted_versions.iter() {
            timestamps.push(current_time);
            // Decrement time by a random amount (1 to 60 minutes) for the next version.
            let minutes_diff = rng.gen_range(1..=60);
            current_time = current_time - Duration::minutes(minutes_diff);
        }

        sorted_versions
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

    // --- Regular Property Tests using Proptest ---
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        // Property 1: After cleanup, at least min_versions_to_keep unique non-zero versions are retained.
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

            // Count unique non-zero versions not marked for deletion.
            let versions = &result.version_file.version_history.unwrap().versions;
            let mut unique_kept: Vec<i64> = versions.iter()
                .filter(|v| !v.marked_for_deletion && v.version != 0)
                .map(|v| v.version)
                .collect();
            unique_kept.sort();
            unique_kept.dedup();

            prop_assert!(
                unique_kept.len() >= min_versions_to_keep as usize,
                "Expected at least {} unique kept versions, but got {}: {:?}",
                min_versions_to_keep,
                unique_kept.len(),
                unique_kept
            );
        }

        // Property 2: Versions created before the cutoff time respect deletion invariants.
        #[test]
        fn prop_respects_cutoff_time_and_invariants(
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

            let versions = &result.version_file.version_history.unwrap().versions;
            let cutoff_ts = cutoff_time.timestamp();
            let oldest = result.oldest_version_to_keep;

            // For each non-zero version created before the cutoff:
            // - If version < oldest_version_to_keep, it must be marked for deletion.
            // - If version >= oldest_version_to_keep, it must not be marked.
            for info in versions {
                if info.version != 0 && info.created_at_secs < cutoff_ts {
                    if info.version < oldest {
                        prop_assert!(info.marked_for_deletion,
                            "Version {} (created at {}) should be marked for deletion as it's below oldest_version_to_keep ({})",
                            info.version, info.created_at_secs, oldest
                        );
                    } else {
                        prop_assert!(!info.marked_for_deletion,
                            "Version {} (created at {}) should NOT be marked for deletion as it's at or above oldest_version_to_keep ({})",
                            info.version, info.created_at_secs, oldest
                        );
                    }
                }
                // Any version marked for deletion must be created before the cutoff.
                if info.marked_for_deletion {
                    prop_assert!(info.created_at_secs < cutoff_ts,
                        "Version {} is marked for deletion but its creation time {} is not before cutoff {}",
                        info.version, info.created_at_secs, cutoff_ts
                    );
                }
            }
        }

        // Property 3: Version 0 is never marked for deletion.
        #[test]
        fn prop_version_zero_never_deleted(
            min_versions_to_keep in 1u32..10u32,
            additional_versions in 0u32..90u32,
            cutoff_hours in 1i64..168i64
        ) {
            let total_versions = min_versions_to_keep + additional_versions;
            // Include version 0.
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

            let versions = &result.version_file.version_history.unwrap().versions;
            for info in versions {
                if info.version == 0 {
                    prop_assert!(!info.marked_for_deletion,
                        "Version 0 should never be marked for deletion"
                    );
                }
            }
        }

        // Property 4: Version infos are chronologically ordered relative to their version numbers.
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

            let version_file = create_version_file(version_infos);
            let versions = &version_file.version_history.as_ref().unwrap().versions;

            for window in versions.windows(2) {
                if window[0].version < window[1].version {
                    prop_assert!(
                        window[0].created_at_secs <= window[1].created_at_secs,
                        "Version {} (timestamp {}) should have an earlier or equal timestamp than version {} (timestamp {})",
                        window[0].version,
                        window[0].created_at_secs,
                        window[1].version,
                        window[1].created_at_secs
                    );
                }
            }

            // Also run the operator to verify it functions correctly on ordered versions.
            let input = ComputeVersionsToDeleteInput {
                version_file,
                cutoff_time: now - Duration::hours(cutoff_hours),
                min_versions_to_keep,
            };

            let operator = ComputeVersionsToDeleteOperator {};
            let _ = tokio_test::block_on(operator.run(&input)).unwrap();
        }
    }

    // --- State Machine Tests using proptest-state-machine ---
    mod state_machine_tests {
        use super::*;
        use chroma_types::chroma_proto::{
            CollectionInfoImmutable, CollectionVersionFile, CollectionVersionHistory,
            CollectionVersionInfo,
        };
        use chrono::{DateTime, Duration, Utc};
        use futures::executor::block_on;
        use proptest::prelude::*;
        use proptest_state_machine::prelude::*;

        // Our reference model holds the version history and cleanup parameters.
        #[derive(Clone, Debug)]
        struct Model {
            version_history: Vec<CollectionVersionInfo>,
            cutoff: DateTime<Utc>,
            min_versions: u32,
        }

        impl Model {
            fn new() -> Self {
                Self {
                    version_history: Vec::new(),
                    cutoff: Utc::now(),
                    min_versions: 1,
                }
            }
        }

        #[derive(Clone, Debug)]
        enum Command {
            AddVersion { version: i64, created_at_secs: i64 },
            SetCutoff { cutoff: DateTime<Utc> },
            SetMinVersionsToKeep { min_versions: u32 },
            ComputeCleanup,
        }

        struct CleanupSM;

        impl ReferenceStateMachine for CleanupSM {
            type State = Model;
            type Transition = Command;

            fn init_state() -> BoxedStrategy<Self::State> {
                Just(Model::new()).boxed()
            }

            fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
                prop_oneof![
                    (
                        1i64..10i64,
                        (state.cutoff.timestamp() - 1000)..(state.cutoff.timestamp() + 1000)
                    )
                        .prop_map(|(version, created_at_secs)| {
                            Command::AddVersion {
                                version,
                                created_at_secs,
                            }
                        }),
                    ((state.cutoff.timestamp() - 1000)..(state.cutoff.timestamp() + 1000))
                        .prop_map(|ts| Command::SetCutoff {
                            cutoff: DateTime::<Utc>::from_utc(
                                chrono::NaiveDateTime::from_timestamp(ts, 0),
                                Utc,
                            )
                        }),
                    (1u32..5u32)
                        .prop_map(|min_versions| Command::SetMinVersionsToKeep { min_versions }),
                    Just(Command::ComputeCleanup),
                ]
                .boxed()
            }

            fn apply(mut state: Self::State, command: &Self::Transition) -> Self::State {
                match command {
                    Command::AddVersion {
                        version,
                        created_at_secs,
                    } => {
                        let info = CollectionVersionInfo {
                            version: *version,
                            created_at_secs: *created_at_secs,
                            marked_for_deletion: false,
                            ..Default::default()
                        };
                        state.version_history.push(info);
                    }
                    Command::SetCutoff { cutoff } => {
                        state.cutoff = *cutoff;
                    }
                    Command::SetMinVersionsToKeep { min_versions } => {
                        state.min_versions = *min_versions;
                    }
                    Command::ComputeCleanup => { /* no change to model */ }
                }
                state
            }
        }

        struct CleanupSMTest;

        impl StateMachineTest for CleanupSMTest {
            type SystemUnderTest = CollectionVersionFile;
            type Reference = CleanupSM;

            fn init_test(
                ref_state: &<Self::Reference as ReferenceStateMachine>::State,
            ) -> Self::SystemUnderTest {
                CollectionVersionFile {
                    version_history: Some(CollectionVersionHistory {
                        versions: ref_state.version_history.clone(),
                    }),
                    collection_info_immutable: Some(CollectionInfoImmutable {
                        tenant_id: "tenant".to_string(),
                        database_id: "db".to_string(),
                        collection_id: "collection".to_string(),
                        dimension: 0,
                        ..Default::default()
                    }),
                }
            }

            fn apply(
                mut sut: Self::SystemUnderTest,
                ref_state: &<Self::Reference as ReferenceStateMachine>::State,
                command: <Self::Reference as ReferenceStateMachine>::Transition,
            ) -> Self::SystemUnderTest {
                match command {
                    Command::AddVersion {
                        version,
                        created_at_secs,
                    } => {
                        if let Some(ref mut history) = sut.version_history {
                            let info = CollectionVersionInfo {
                                version,
                                created_at_secs,
                                marked_for_deletion: false,
                                ..Default::default()
                            };
                            history.versions.push(info);
                        }
                    }
                    Command::SetCutoff { cutoff: _ } => { /* SUT does not hold cutoff */ }
                    Command::SetMinVersionsToKeep { min_versions: _ } => { /* SUT does not hold min versions */
                    }
                    Command::ComputeCleanup => {
                        let input = ComputeVersionsToDeleteInput {
                            version_file: sut.clone(),
                            cutoff_time: ref_state.cutoff,
                            min_versions_to_keep: ref_state.min_versions,
                        };
                        let operator = ComputeVersionsToDeleteOperator {};
                        let result =
                            block_on(operator.run(&input)).expect("Operator should not fail");
                        sut = result.version_file;

                        // Invariant checks.
                        if let Some(ref history) = sut.version_history {
                            let cutoff_ts = ref_state.cutoff.timestamp();
                            let oldest = result.oldest_version_to_keep;
                            for info in history.versions.iter() {
                                if info.version != 0 && info.created_at_secs < cutoff_ts {
                                    if info.version < oldest {
                                        assert!(
                                            info.marked_for_deletion,
                                            "Version {} should be marked",
                                            info.version
                                        );
                                    } else {
                                        assert!(
                                            !info.marked_for_deletion,
                                            "Version {} should not be marked",
                                            info.version
                                        );
                                    }
                                }
                                if info.marked_for_deletion {
                                    assert!(
                                        info.created_at_secs < cutoff_ts,
                                        "Marked version {} must be before cutoff",
                                        info.version
                                    );
                                }
                            }
                            use std::collections::BTreeSet;
                            let kept: BTreeSet<_> = history
                                .versions
                                .iter()
                                .filter(|v| !v.marked_for_deletion && v.version != 0)
                                .map(|v| v.version)
                                .collect();
                            let all: BTreeSet<_> = history
                                .versions
                                .iter()
                                .filter(|v| v.version != 0)
                                .map(|v| v.version)
                                .collect();
                            let expected =
                                std::cmp::min(ref_state.min_versions as usize, all.len());
                            assert!(
                                kept.len() >= expected,
                                "Kept versions {:?} are fewer than required {}",
                                kept,
                                expected
                            );
                        }
                    }
                }
                sut
            }

            fn check_invariants(
                sut: &Self::SystemUnderTest,
                ref_state: &<Self::Reference as ReferenceStateMachine>::State,
            ) {
                if let Some(ref history) = sut.version_history {
                    let cutoff_ts = ref_state.cutoff.timestamp();
                    for info in history.versions.iter() {
                        if info.marked_for_deletion {
                            assert!(
                                info.created_at_secs < cutoff_ts,
                                "Invariant: marked version {} should be before cutoff",
                                info.version
                            );
                        }
                    }
                }
            }
        }

        proptest_state_machine! {
            #[test]
            fn state_machine_cleanup_test(sequential 1..20 => CleanupSMTest);
        }
    }
}
