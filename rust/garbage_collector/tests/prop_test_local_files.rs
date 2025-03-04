use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::new_cache_for_test;
use chroma_segment::{
    blockfile_record::{
        RecordSegmentReader, RecordSegmentReaderCreationError, RecordSegmentWriter,
    },
    types::materialize_logs,
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::chroma_proto::{
    CollectionInfoImmutable, CollectionVersionFile, CollectionVersionHistory,
    CollectionVersionInfo, VersionChangeReason,
};
use chroma_types::{Chunk, CollectionUuid, LogRecord, Operation, OperationRecord, SegmentUuid};
use chrono::{DateTime, Utc};
use futures::executor::block_on;
use garbage_collector_library::operators::compute_versions_to_delete::{
    ComputeVersionsToDeleteInput, ComputeVersionsToDeleteOperator,
};
use proptest::{collection, prelude::*};
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct Model {
    collection_id: CollectionUuid,
    record_segment_id: SegmentUuid,
    // Current version of the collection.
    current_version: i64,
    // Version history of the collection.
    version_history: Vec<CollectionVersionInfo>,
    // Cutoff time for the collection.
    cutoff: DateTime<Utc>,
    // Minimum number of versions to keep.
    min_versions: u32,
    // Next version number to use.
    next_version: i64, // Track the next version number to use
    // Local directory object store.
    local_dir_object_store: Arc<tempfile::TempDir>,
    // Map of version to segment.
    version_to_segment_map: HashMap<i64, SegmentUuid>,
    // Map of version to file path.
    // For creating the block files for the next version, we need to know
    // the file paths of the previous version.
    version_to_file_path_map: HashMap<i64, HashMap<String, Vec<String>>>,
}

impl Model {
    fn new() -> Self {
        Self {
            collection_id: CollectionUuid::new(),
            record_segment_id: SegmentUuid::new(),
            current_version: 0,
            version_history: Vec::new(),
            cutoff: Utc::now(),
            min_versions: 1,
            next_version: 1, // Start with version 1
            local_dir_object_store: Arc::new(tempfile::tempdir().unwrap()),
            version_to_segment_map: HashMap::new(),
            version_to_file_path_map: HashMap::new(),
        }
    }

    fn get_latest_version(&self) -> i64 {
        self.version_history
            .iter()
            .map(|v| v.version)
            .max()
            .unwrap_or(0)
    }

    // Purpose of this function:
    // In order to test garbage collection logic, we need to create block files
    // when records are added to a collection. This function simulates the
    // addition of records by creating block files and storing them in the
    // local_dir_object_store.
    async fn simulate_add_records(&mut self) {
        tracing::debug!("Simulating add records");
        tracing::debug!("Current version: {}", self.current_version);
        let storage = Storage::Local(LocalStorage::new(
            self.local_dir_object_store.path().to_str().unwrap(),
        ));
        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let arrow_blockfile_provider =
            ArrowBlockfileProvider::new(storage, 1000, block_cache, sparse_index_cache);
        let blockfile_provider =
            BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
        let mut segment_file_path = HashMap::new();
        // If there are previous versions, use the file paths of the previous version.
        if let Some(previous_version) = self.version_history.last() {
            segment_file_path = self.version_to_file_path_map[&previous_version.version].clone();
        }
        let mut record_segment = chroma_types::Segment {
            id: self.record_segment_id,
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: self.collection_id,
            metadata: None,
            file_path: segment_file_path,
        };
        {
            let segment_writer =
                RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
                    .await
                    .expect("Error creating segment writer");
            let data = vec![
                LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: None,
                        document: Some(String::from("This is a document about cats.")),
                        operation: Operation::Add,
                    },
                },
                LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: None,
                        document: Some(String::from("This is a document about dogs.")),
                        operation: Operation::Add,
                    },
                },
            ];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            let record_segment_reader: Option<RecordSegmentReader> =
                match RecordSegmentReader::from_segment(&record_segment, &blockfile_provider).await
                {
                    Ok(reader) => Some(reader),
                    Err(e) => {
                        match *e {
                            // Uninitialized segment is fine and means that the record
                            // segment is not yet initialized in storage.
                            RecordSegmentReaderCreationError::UninitializedSegment => None,
                            RecordSegmentReaderCreationError::BlockfileOpenError(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderCreationError::InvalidNumberOfFiles => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderCreationError::DataRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                            RecordSegmentReaderCreationError::UserRecordNotFound(_) => {
                                panic!("Error creating record segment reader");
                            }
                        }
                    }
                };
            let mat_records = materialize_logs(&record_segment_reader, data, None)
                .await
                .expect("Log materialization failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log to record segment failed");
            let record_flusher = segment_writer
                .commit()
                .await
                .expect("Commit for segment writer failed");
            record_segment.file_path = record_flusher
                .flush()
                .await
                .expect("Flush record segment writer failed");
        }

        // Update the various internal state variables.
        self.current_version += 1;
        self.version_to_segment_map
            .insert(self.current_version, self.record_segment_id);
        self.version_to_file_path_map
            .insert(self.current_version, record_segment.file_path);
    }

    // Update sysdb such that the version history is updated with the new version.
    // The new version should have the correct file paths for the segment.
    async fn update_sysdb_with_new_version(&mut self) {
        let mut version_history = Vec::new();
        for version in self.version_history.iter() {
            version_history.push(version.clone());
        }
        // Create new version info
        let mut version_info = CollectionVersionInfo::default();
        version_info.version = self.current_version;
        version_info.created_at_secs = Utc::now().timestamp();
        version_info.version_change_reason = VersionChangeReason::DataCompaction as i32;
        version_history.push(version_info);
        self.version_history = version_history;
    }

    // Add data to the collection and increment the version.
    async fn add_data_and_incr_version(&mut self) {
        self.simulate_add_records().await;
        let mut rng = rand::thread_rng();
        self.add_version(&mut rng);
    }

    // Add a new version to the version history.
    fn add_version(&mut self, rng: &mut impl Rng) {
        // Get the latest timestamp or use cutoff - 1000 as base
        let latest_timestamp = self
            .version_history
            .iter()
            .map(|v| v.created_at_secs)
            .max()
            .unwrap_or(self.cutoff.timestamp() - 1000);

        // Create timestamp that's 1-60 seconds after the latest
        let created_at = latest_timestamp + rng.gen_range(1..=60);

        let info = CollectionVersionInfo {
            version: self.next_version,
            created_at_secs: created_at,
            marked_for_deletion: false,
            ..Default::default()
        };
        self.version_history.push(info);
        // Sort versions by creation time (newest first)
        self.version_history.sort_by_key(|v| -v.created_at_secs);
        self.next_version += 1;
    }
}

impl Default for Model {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
enum Command {
    AddVersion,
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
            Just(Command::AddVersion),
            ((state.cutoff.timestamp() - 1000)..(state.cutoff.timestamp() + 1000)).prop_map(|ts| {
                Command::SetCutoff {
                    cutoff: DateTime::<Utc>::from_utc(
                        chrono::NaiveDateTime::from_timestamp(ts, 0),
                        Utc,
                    ),
                }
            }),
            (1u32..5u32).prop_map(|min_versions| Command::SetMinVersionsToKeep { min_versions }),
            Just(Command::ComputeCleanup),
        ]
        .boxed()
    }

    fn apply(mut state: Self::State, command: &Self::Transition) -> Self::State {
        match command {
            Command::AddVersion => {
                let mut rng = rand::thread_rng();
                state.add_version(&mut rng);
            }
            Command::SetCutoff { cutoff } => {
                state.cutoff = *cutoff;
            }
            Command::SetMinVersionsToKeep { min_versions } => {
                state.min_versions = *min_versions;
            }
            Command::ComputeCleanup => {}
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
                collection_id: ref_state.collection_id,
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
            Command::AddVersion => {
                if let Some(ref mut history) = sut.version_history {
                    let latest = ref_state.version_history.first().unwrap();
                    history.versions.push(latest.clone());
                    history.versions.sort_by_key(|v| -v.created_at_secs);
                }
            }
            Command::SetCutoff { cutoff: _ } => {}
            Command::SetMinVersionsToKeep { min_versions: _ } => {}
            Command::ComputeCleanup => {
                let input = ComputeVersionsToDeleteInput {
                    version_file: sut.clone(),
                    cutoff_time: ref_state.cutoff,
                    min_versions_to_keep: ref_state.min_versions,
                };
                let operator = ComputeVersionsToDeleteOperator {};
                let result = block_on(operator.run(&input)).expect("Operator should not fail");
                sut = result.version_file;

                // Invariant checks
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
                    let expected = std::cmp::min(ref_state.min_versions as usize, all.len());
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
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        if let Some(ref history) = sut.version_history {
            // Check versions are properly ordered (newest first)
            for window in history.versions.windows(2) {
                assert!(
                    window[0].created_at_secs >= window[1].created_at_secs,
                    "Versions should be ordered by timestamp (newest first)"
                );
            }

            // Check version numbers are monotonically increasing
            let mut prev_version = 0;
            for info in history.versions.iter() {
                assert!(
                    info.version > prev_version,
                    "Version numbers should be strictly increasing"
                );
                prev_version = info.version;
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]
    #[test]
    fn state_machine_cleanup_test(cmds in prop_state_machine::commands(1..20)) {
        prop_state_machine::run_commands::<CleanupSMTest>(cmds)?;
    }
}
