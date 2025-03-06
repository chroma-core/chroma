use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::new_cache_for_test;
use chroma_segment::{
    blockfile_record::{
        RecordSegmentReader, RecordSegmentReaderCreationError, RecordSegmentWriter,
    },
    types::materialize_logs,
};
use chroma_storage::{local::LocalStorage, Storage};
use chroma_system::Orchestrator;
use chroma_types::chroma_proto::{CollectionVersionFile, CollectionVersionHistory};
use chroma_types::{Chunk, CollectionUuid, LogRecord, Operation, OperationRecord, SegmentUuid};
use chrono::{DateTime, Utc};
use futures::executor::block_on;
use garbage_collector_library::garbage_collector_orchestrator::GarbageCollectorOrchestrator;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
use prost::message::Message;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone, Debug)] // Add Arbitrary derive
enum Command {
    // Create a new collection.
    CreateCollection { id: String },
    // Add version to a specific collection.
    // id is the name of the collection.
    AddVersion { id: String }, // TODO: Add n number of versions.
    // Cleanup versions from a specific collection.
    CleanupVersions { id: String },
}

#[derive(Clone, Debug)] // Add Arbitrary derive
enum Transition {
    // Create a new collection.
    CreateCollection { id: String },
    // Add version to a specific collection.
    // id is the name of the collection.
    AddVersion { id: String }, // TODO: Add n number of versions.
    // Cleanup versions from a specific collection.
    CleanupVersions { id: String },
}

type VersionToFilesMap = HashMap<i64, Vec<String>>;
type VersionToSegmentInfosMap = HashMap<i64, Vec<SegmentInfo>>;

struct RefState {
    // Keep track of collections.
    collections: HashSet<String>,
    // Keep track of version files for each collection.
    coll_to_files_map: HashMap<String, VersionToFilesMap>,
    // Keep track of segment infos added for each new version.
    coll_to_segment_infos_map: HashMap<String, VersionToSegmentInfosMap>,
}

impl RefState {
    fn new() -> Self {
        Self {
            collections: HashSet::new(),
            coll_to_files_map: HashMap::new(),
            coll_to_segment_infos_map: HashMap::new(),
        }
    }

    fn add_version(self, id: String) -> Self {
        self.collections.insert(id);
        self
    }

    fn create_collection(self, id: String) -> Self {
        self.collections.insert(id);
        self
    }

    fn cleanup_versions(self, id: String) -> Self {
        // TODO:Use the cutoff time to compute which versions to delete.
        // Use the segment infos to figure out which files to delete.
        self
    }

    // Update the segment infos for a version.
    fn update_segment_infos_for_version() {}
}

impl ReferenceStateMachine for RefState {
    type Transition = Transition;
    type State = RefState;

    fn init_state() -> BoxedStrategy<Self> {
        Just(Self {
            collections: HashSet::new(),
            coll_to_files_map: HashMap::new(),
            coll_to_segment_infos_map: HashMap::new(),
        })
        .boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        let new_collection_id_strategy = "[a-zA-Z0-9]{3}".prop_map(|id| id.to_string());
        let existing_collection_ids: Vec<String> =
            state.collections.iter().map(|id| id.clone()).collect();
        prop_oneof![
            new_collection_id_strategy
                .clone()
                .prop_map(|id| Transition::CreateCollection { id }),
            existing_collection_ids.prop_map(|id| Transition::AddVersion { id }),
            existing_collection_ids.prop_map(|id| Transition::CleanupVersions { id }),
        ]
        .boxed()
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        match transition {
            Transition::AddVersion { id } => state.collections.contains(id),
            Transition::CleanupVersions { id } => state.collections.contains(id),
            Transition::CreateCollection { id } => !state.collections.contains(id),
        }
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self {
        match transition {
            Transition::AddVersion { id } => {
                // Only add version if collection exists
                if state.collections.contains(&id) {
                    state.add_version(id)
                } else {
                    // If collection doesn't exist, return unchanged state
                    state
                }
                // TODO: Add new version to the collection.
            }
            Command::CleanupVersions { id } => {
                // Only cleanup versions if collection exists
                if state.collections.contains(&id) {
                    state.cleanup_versions(id)
                } else {
                    // If collection doesn't exist, return unchanged state
                    state
                }
            }
            Command::CreateCollection { id } => {
                // Only create collection if it doesn't exist
                if !state.collections.contains(&id) {
                    state.create_collection(id)
                } else {
                    // If collection already exists, return unchanged state
                    state
                }
            }
        }
    }
}

#[derive(Default)]
struct GcTest {
    db_ops: MockVectorDbOperations,
}

impl GcTest {
    fn add_version(self, id: String) -> Self {
        // TODO: Call SysDb add version.
        self.db_ops.add_data_and_incr_version(
            self.collection_id,
            self.current_version,
            self.sysdb.clone(),
            self.storage.clone(),
        );
        self
    }

    fn create_collection(self, id: String) -> Self {
        // TODO: Call SysDb create collection.
        self
    }

    fn cleanup_versions(self, id: String) -> Self {
        // TODO:Use the cutoff time to compute which versions to delete.
        self.db_ops.run_gc(
            self.collection_id,
            self.current_version_file_name.clone(),
            self.sysdb.clone(),
            self.storage.clone(),
        );
        self
    }
}

impl StateMachineTest for GcTest {
    type SystemUnderTest = Self;
    type Reference = RefState;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        Self::default()
    }

    fn apply(
        state: &mut Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        match transition {
            Transition::AddVersion { id } => {
                state.add_version(id);
            }
            Transition::CreateCollection { id } => {
                state.create_collection(id);
            }
            Transition::CleanupVersions { id } => {
                state.cleanup_versions(id);
            }
        }
        state
    }

    fn check_invariants(state: &Self::SystemUnderTest) {
        // TODO: Check invariants.
    }
}

prop_state_machine! {
    #![proptest_config(ProptestConfig::with_cases(20))]
    // #[test]
    fn run_gc_test(
        sequential
        1..20
        =>
        GcTest
    );
}

// ================================
// ================================
// ================================
// ================================
// ================================
// ================================

type FilePath = HashMap<String, Vec<String>>;

#[derive(Clone, Debug)]
struct SegmentInfo {
    segment_type: chroma_types::SegmentType,
    file_paths: FilePath,
    segment_id: SegmentUuid,
}

#[derive(Clone, Debug)]
struct MockVectorDbOperations {}

impl MockVectorDbOperations {
    fn new() -> Self {
        Self {}
    }

    pub fn run_gc(
        &mut self,
        collection_id: CollectionUuid,
        version_file_name: String,
        sysdb: chroma_sysdb::SysDb,
        storage: Storage,
    ) {
        block_on(async {
            let system = chroma_system::System::new();
            let dispatcher =
                chroma_system::Dispatcher::new(chroma_system::DispatcherConfig::default());
            let dispatcher_handle = system.start_component(dispatcher);

            let orchestrator = GarbageCollectorOrchestrator::new(
                collection_id,
                version_file_name,
                0,
                sysdb,
                dispatcher_handle,
                storage,
            );
            orchestrator.run(system).await.unwrap();
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        });
    }

    // TODO(rohit): Return all the blockfiles that are part of the new version.
    fn add_records_by_creating_block_files(
        &mut self,
        collection_id: CollectionUuid,
        current_version: i64,
        storage: Storage,
        existing_segment_infos: Vec<SegmentInfo>,
    ) -> (Vec<SegmentInfo>, Vec<String>) {
        let mut segment_infos = Vec::new();
        // Wrap the async function in block_on
        block_on(async {
            tracing::debug!("Simulating add records");
            tracing::debug!("Current version: {}", current_version);
            let block_cache = new_cache_for_test();
            let sparse_index_cache = new_cache_for_test();
            let arrow_blockfile_provider =
                ArrowBlockfileProvider::new(storage, 1000, block_cache, sparse_index_cache);
            let blockfile_provider =
                BlockfileProvider::ArrowBlockfileProvider(arrow_blockfile_provider);
            let mut segment_file_path = HashMap::new();
            if !existing_segment_infos.is_empty() {
                segment_file_path = existing_segment_infos
                    .iter()
                    .find(|info| info.segment_type == chroma_types::SegmentType::BlockfileRecord)
                    .unwrap()
                    .file_paths
                    .clone();
            }
            let segment_id = SegmentUuid::new();
            let mut record_segment = chroma_types::Segment {
                id: segment_id, // TODO: Don't change this each time.
                r#type: chroma_types::SegmentType::BlockfileRecord,
                scope: chroma_types::SegmentScope::RECORD,
                collection: collection_id,
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
                    match RecordSegmentReader::from_segment(&record_segment, &blockfile_provider)
                        .await
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
                segment_infos.push(SegmentInfo {
                    segment_type: record_segment.r#type,
                    file_paths: record_segment.file_path,
                    segment_id,
                });
            }
        });

        // TODO(rohit): Return all the blockfiles that are part of the new version.
        (segment_infos, vec![])
    }

    // Add data to the collection and increment the version.
    pub fn add_data_and_incr_version(
        &mut self,
        collection_id: CollectionUuid,
        current_version: i64,
        mut sysdb: chroma_sysdb::SysDb,
        storage: Storage,
    ) -> (Vec<SegmentInfo>, Vec<String>, String) {
        let mut segment_infos = Vec::new();
        let mut blockfiles = Vec::new();
        let mut version_file_name = String::new();
        // Wrap the async function in block_on
        block_on(async {
            (segment_infos, blockfiles) = self.add_records_by_creating_block_files(
                collection_id,
                current_version,
                storage,
                vec![],
            );
            // Update sysdb with the new version by calling flush_compaction.
            let mut segment_flush_info_vec = vec![];
            segment_infos.iter().for_each(|info| {
                segment_flush_info_vec.push(chroma_types::SegmentFlushInfo {
                    segment_id: info.segment_id,
                    file_paths: info.file_paths.clone(),
                });
            });
            let segment_flush_info: Arc<[chroma_types::SegmentFlushInfo]> =
                Arc::from(segment_flush_info_vec.into_boxed_slice());

            sysdb.flush_compaction(
                "tenant".to_string(),
                collection_id,
                0,
                current_version as i32,
                segment_flush_info,
                0,
            );
            let collections = sysdb
                .get_collections(Some(collection_id), None, None, None, None, None)
                .await
                .unwrap();
            let collection = collections.first().unwrap();
            version_file_name = collection.version_file_name.clone();
            // Write the version file to storage since TestSysDb is not writing it.
            let version_history = sysdb.get_version_history(collection_id).await.unwrap();
            let version_file = CollectionVersionFile {
                version_history: Some(version_history.versions),
                collection_info_immutable: Some(collection.collection_info_immutable.unwrap()),
            };
            let version_file_bytes = version_file.encode_to_vec();
            storage
                .write_all(version_file_name.as_bytes(), &version_file_bytes)
                .await
                .unwrap();
        });
        (segment_infos, blockfiles, version_file_name)
    }
}

// TODO(rohit): Add support for multiple collections.
struct CloudDbSM {
    collection_id: CollectionUuid,
    mock_vector_db_operations: MockVectorDbOperations,
    // Value of cutoff that is passed to the garbage collector.
    // If GC is not run, then this will be Utc::now(). So any version created after this time will not be deleted.
    cutoff_for_last_gc: DateTime<Utc>,
    // Value of min_versions that is passed to the garbage collector.
    min_versions_to_keep: u32,
    // Keep track of blockfiles added for each new version.
    version_to_files_map: HashMap<i64, Vec<String>>,
    // Keep track of segment infos added for each new version.
    version_to_segment_infos_map: HashMap<i64, Vec<SegmentInfo>>,
    // SysDb. This is passed to the garbage collector, and is also used for checking invariants.
    sysdb: chroma_sysdb::SysDb,
    // Storage. This is passed to the garbage collector, and is also used for checking invariants.
    storage: Storage,
    // Current version of the collection.
    current_version: i64,
    // Current version file name.
    current_version_file_name: String,
    // Local directory object store.
    local_dir_object_store: Arc<tempfile::TempDir>,
}

impl CloudDbSM {
    fn new() -> Self {
        let temp_dir = Arc::new(tempfile::tempdir().unwrap());
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));

        Self {
            collection_id: CollectionUuid::new(),
            cutoff_for_last_gc: Utc::now(),
            min_versions_to_keep: 1,
            version_to_files_map: HashMap::new(),
            sysdb: chroma_sysdb::SysDb::Test(chroma_sysdb::TestSysDb::new()),
            storage,
            current_version: 0,
            local_dir_object_store: temp_dir,
            mock_vector_db_operations: MockVectorDbOperations::new(),
            version_to_segment_infos_map: HashMap::new(),
            current_version_file_name: String::new(),
        }

        // TODO: Should we initialize the collection with a sysdb create_collection call?
    }

    fn apply(&mut self, cmd: Command) {
        match cmd {
            Command::AddVersion => {
                // TODO: Add n number of versions instead of just 1.
                let (segment_infos, blockfiles, version_file_name) =
                    self.mock_vector_db_operations.add_data_and_incr_version(
                        self.collection_id,
                        self.current_version,
                        self.sysdb.clone(),
                        self.storage.clone(),
                    );
                self.version_to_segment_infos_map
                    .insert(self.current_version, segment_infos);
                self.version_to_files_map
                    .insert(self.current_version, blockfiles);
                self.current_version_file_name = version_file_name;
                self.current_version += 1;
            }
            Command::IncreaseCutoff { by } => {
                // This will be no-op for now.
            }
            Command::SetMinVersionsToKeep { min_versions } => {
                // TODO: Set min versions to keep.
            }
            Command::CleanupVersions => {
                self.mock_vector_db_operations.run_gc(
                    self.collection_id,
                    self.current_version_file_name.clone(),
                    self.sysdb.clone(),
                    self.storage.clone(),
                );
            }
        }
    }

    fn check_invariant_correct_version_history(&self, version_history: &CollectionVersionHistory) {
        // Check that version history is properly ordered and has right version numbers.
        // Go through each version and check that there are atleast min_versions_to_keep versions.

        // Get unique versions from the version history.
        let unique_versions = version_history
            .versions
            .iter()
            .map(|v| v.version)
            .collect::<std::collections::HashSet<_>>();
        assert!(unique_versions.len() >= self.min_versions_to_keep as usize);

        // Check that the version history is properly ordered.
        for window in version_history.versions.windows(2) {
            assert!(window[0].created_at_secs <= window[1].created_at_secs);
        }

        // Check that the version history is properly ordered in ascending order.
        for window in version_history.versions.windows(2) {
            assert!(window[0].version <= window[1].version);
        }

        // Remove the number of unique versions equal to min_versions_to_keep, and then check that the rest of the versions meet the cutoff.
        let versions_to_check = version_history.versions.clone();
        // Get the youngest version to keep as per min_versions_to_keep policy.
        let youngest_version_to_keep = unique_versions
            .into_iter()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .nth(self.min_versions_to_keep as usize - 1)
            .unwrap();
        // Check that all versions in the version history are either older than or equal to the youngest version to keep, OR meet the cutoff.
        for version in versions_to_check {
            assert!(
                version.version >= youngest_version_to_keep
                    || version.created_at_secs <= self.cutoff_for_last_gc.timestamp()
            );
        }
    }

    fn check_invariant_active_files_not_deleted(&self, version_history: &CollectionVersionHistory) {
        // - ** Invariant **: Check that active files have not been deleted.
        // - We need to know which files are part of a version.
        //   This can come from reading the sparse index of a version. We will keep this mapping separately.
        //   Then we read the sparse index file to get the blockfiles that are part of a version.
        //   So, then we build the active set of files for a Collection.
        // - We need to know all the files present in the local object store.
        // - Check that the active set is a subset of the files in the local object store.

        // Get the unique versions from the version history.
        let unique_versions = version_history
            .versions
            .iter()
            .map(|v| v.version)
            .collect::<std::collections::HashSet<_>>();

        // Get the active set of files for the collection.
        let active_files = self
            .version_to_files_map
            .iter()
            .filter(|(version, _)| unique_versions.contains(version))
            .map(|(_, files)| files)
            .flatten()
            .collect::<std::collections::HashSet<_>>();

        block_on(async {
            // Get all the files in the local object store.
            let all_files = self.storage.list_prefix("").await.unwrap();

            let all_files_set = all_files.iter().collect::<std::collections::HashSet<_>>();
            // Check that the active set is a subset of the files in the local object store.
            assert!(active_files.is_subset(&all_files_set));
        });
    }

    fn check_invariants(&mut self) {
        block_on(async {
            let version_info_list = self
                .sysdb
                .list_collection_versions(self.collection_id)
                .await
                .unwrap();
            let version_history = CollectionVersionHistory {
                versions: version_info_list,
            };
            self.check_invariant_correct_version_history(&version_history);
            self.check_invariant_active_files_not_deleted(&version_history);
        });
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]
    #[test]
    fn cloud_db_state_test(cmds in proptest::collection::vec(any::<Command>(), 1..20)) {
        let mut state = CloudDbSM::new();
        for cmd in cmds {
            state.apply(cmd);
        }
        state.check_invariants();
    }
}
