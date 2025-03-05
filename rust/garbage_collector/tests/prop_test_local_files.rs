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
use chroma_types::chroma_proto::{
    collection_version_info::VersionChangeReason, CollectionInfoImmutable, CollectionVersionFile,
    CollectionVersionHistory, CollectionVersionInfo,
};
use chroma_types::{Chunk, CollectionUuid, LogRecord, Operation, OperationRecord, SegmentUuid};
use chrono::{DateTime, Utc};
use futures::executor::block_on;
use garbage_collector_library::garbage_collector_orchestrator::GarbageCollectorOrchestrator;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
use std::collections::HashMap;
use std::fmt::Debug;
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
    // Sysdb.
    sysdb: chroma_sysdb::SysDb,
    // Log position of the collection.
    log_position: i64,
    // Keep a local storage object.
    storage: Storage,
}

impl Model {
    fn new() -> Self {
        let temp_dir = Arc::new(tempfile::tempdir().unwrap());
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));

        Self {
            collection_id: CollectionUuid::new(),
            record_segment_id: SegmentUuid::new(),
            current_version: 0,
            version_history: Vec::new(),
            cutoff: Utc::now(),
            min_versions: 1,
            next_version: 1, // Start with version 1
            local_dir_object_store: temp_dir,
            version_to_segment_map: HashMap::new(),
            version_to_file_path_map: HashMap::new(),
            sysdb: chroma_sysdb::SysDb::Test(chroma_sysdb::TestSysDb::new()),
            log_position: 0,
            storage,
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
    fn add_records_by_creating_block_files(&mut self) {
        // Wrap the async function in block_on
        block_on(async {
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
                segment_file_path =
                    self.version_to_file_path_map[&previous_version.version].clone();
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
            }

            // Update the various internal state variables.
            self.current_version += 1;
            self.version_to_segment_map
                .insert(self.current_version, self.record_segment_id);
            self.version_to_file_path_map
                .insert(self.current_version, record_segment.file_path);
        });
    }

    // Update sysdb such that the version history is updated with the new version.
    // The new version should have the correct file paths for the segment.
    fn update_sysdb_with_new_version(&mut self) {
        // Wrap the async function in block_on
        block_on(async {
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
        });
    }

    // Add data to the collection and increment the version.
    fn add_data_and_incr_version(&mut self) {
        // Wrap the async function in block_on
        block_on(async {
            let current_version = self.get_latest_version();
            self.add_records_by_creating_block_files();
            // Update sysdb with the new version by calling flush_compaction.
            let segment_flush_infos = Arc::new(vec![chroma_types::SegmentFlushInfo {
                segment_id: self.record_segment_id,
                file_paths: self.version_to_file_path_map[&current_version].clone(),
            }]);
            let segment_flush_info: Arc<[chroma_types::SegmentFlushInfo]> =
                Arc::from(segment_flush_infos.to_vec().into_boxed_slice());
            self.log_position += 1;
            self.sysdb.flush_compaction(
                "tenant".to_string(),
                self.collection_id,
                0,
                current_version as i32,
                segment_flush_info,
                0,
            );
        });
    }

    // Add a new version to the version history.
    fn add_version(&mut self) {
        self.add_data_and_incr_version();
        // // Get the latest timestamp or use cutoff - 1000 as base
        // let latest_timestamp = self
        //     .version_history
        //     .iter()
        //     .map(|v| v.created_at_secs)
        //     .max()
        //     .unwrap_or(self.cutoff.timestamp() - 1000);

        // // Create timestamp that's 1-60 seconds after the latest
        // let created_at = latest_timestamp + rng.gen_range(1..=60);

        // let info = CollectionVersionInfo {
        //     version: self.next_version,
        //     created_at_secs: created_at,
        //     marked_for_deletion: false,
        //     ..Default::default()
        // };
        // self.version_history.push(info);
        // // Sort versions by creation time (newest first)
        // self.version_history.sort_by_key(|v| -v.created_at_secs);
        // self.next_version += 1;
    }

    // Add compute_cleanup method
    fn compute_cleanup(&mut self) {
        block_on(async {
            let system = chroma_system::System::new();
            let dispatcher =
                chroma_system::Dispatcher::new(chroma_system::DispatcherConfig::default());
            let dispatcher_handle = system.start_component(dispatcher);

            let mut orchestrator = GarbageCollectorOrchestrator::new(
                self.collection_id,
                self.version_history
                    .first()
                    .unwrap()
                    .version_file_name
                    .clone(),
                0,
                self.sysdb.clone(), // Clone sysdb
                dispatcher_handle,
                self.storage.clone(), // Clone storage
            );
            orchestrator.run(system).await.unwrap();
        });
    }
}

impl Default for Model {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)] // Add Arbitrary derive
enum Command {
    AddVersion, // TODO: Add n number of versions.
    IncreaseCutoff { by: i64 },
    SetMinVersionsToKeep { min_versions: u32 },
    ComputeCleanup,
}

impl Arbitrary for Command {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(Command::AddVersion),
            (1..100i64).prop_map(|by| Command::IncreaseCutoff { by }),
            (1u32..5u32).prop_map(|min_versions| Command::SetMinVersionsToKeep { min_versions }),
            Just(Command::ComputeCleanup)
        ]
        .boxed()
    }
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
            ((state.cutoff.timestamp() - 1000)..(state.cutoff.timestamp() + 1000))
                .prop_map(|ts| { Command::IncreaseCutoff { by: ts } }),
            (1u32..5u32).prop_map(|min_versions| Command::SetMinVersionsToKeep { min_versions }),
            Just(Command::ComputeCleanup),
        ]
        .boxed()
    }

    fn apply(mut state: Self::State, command: &Self::Transition) -> Self::State {
        match command {
            Command::AddVersion => {
                state.add_version();
            }
            Command::IncreaseCutoff { by } => {
                state.cutoff = state.cutoff + tokio::time::Duration::from_secs(*by as u64);
            }
            Command::SetMinVersionsToKeep { min_versions } => {
                state.min_versions = *min_versions;
            }
            Command::ComputeCleanup => {
                state.compute_cleanup();
            }
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
                collection_id: ref_state.collection_id.to_string(),
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
            Command::IncreaseCutoff { by } => {
                sut.cutoff = sut.cutoff + tokio::time::Duration::from_secs(*by as u64);
            }
            Command::SetMinVersionsToKeep { min_versions } => {
                sut.min_versions = *min_versions;
            }
            Command::ComputeCleanup => {
                // Wrap the async operations in block_on
                block_on(async {
                    let system = chroma_system::System::new();
                    let dispatcher =
                        chroma_system::Dispatcher::new(chroma_system::DispatcherConfig::default());
                    let dispatcher_handle = system.start_component(dispatcher);

                    let mut orchestrator = GarbageCollectorOrchestrator::new(
                        ref_state.collection_id,
                        ref_state
                            .version_history
                            .first()
                            .unwrap()
                            .version_file_name
                            .clone(),
                        0,
                        ref_state.sysdb,
                        dispatcher_handle,
                        ref_state.storage,
                    );
                    orchestrator.run(system).await.unwrap();
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                });
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
            let mut record_segment = chroma_types::Segment {
                id: SegmentUuid::new(), // TODO: Don't change this each time.
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
            let version_file = CollectionVersionFile {
                version_history: Some(collection.version_history.unwrap()),
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
            Command::ComputeCleanup => {
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
    fn state_machine_cleanup_test(cmds in proptest::collection::vec(any::<Command>(), 1..20)) {
        let mut state = Model::new();
        let mut sut = CleanupSMTest::init_test(&state);

        for cmd in cmds {
            state = CleanupSM::apply(state, &cmd);
            sut = CleanupSMTest::apply(sut, &state, cmd);
            CleanupSMTest::check_invariants(&sut, &state);
        }
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
