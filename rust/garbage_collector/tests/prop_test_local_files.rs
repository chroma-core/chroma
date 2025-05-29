// Property Tests for Garbage Collection service.
// RefState and SUT based implementation using TestSysDb and LocalStorage.
//
// SUT uses TestSysDb and LocalStorage.
// Main transitions are:
// 1. Create a new collection.
// 2. Add a new version to a collection.
// 3. Cleanup versions from a collection.
//
// For AddVersion,
// SUT creates a new version file and adds the segment block ids to it.
// SparseIndices are created on LocalStorage to mimic compaction of new data.
// While SparseIndex files are created on disk, there is no creation of actual Block files on disk.
// A mapping of collection to segment to block ids is maintained in RefState.
// This allows each AddVersion to work off the previous version's segment to block id mapping.
// RefState is updated with the new version's segment to block id mapping.
// So, RefState does not use TestSysDb or LocalStorage.
//
// For CleanupVersions,
// SUT runs the actual Garbage Collection orchestrator.
// RefState does its independent computation of versions to delete, and the block ids to delete.
//
// Note on Time manipulation -
// Using mock of Tokio time can create issues which are hard to debug due to
// implementation of the mock when runtime has no jobs to do.
// Time is maintained as a u64 monotonic counter.
// Each time a transition happens, this counter (RefState::highest_registered_time) is increased by 1.
// This allows a very deterministic run (& re-run) of the state machine.
// All transitions that need time, use this counter, whose starting value it 100.
// Eg:
//  A collection is created at t=100.
//  New Version is added at t=101
//  Another Version is added at t=102
//  CleanUp versions can be called at t=103, with a cutoff of 1 seconds.

// TODO(rohitcp):
// Min versions to keep is 2. Make this configurable, and randomize it per collection.

use chroma_blockstore::test_utils::sparse_index_test_utils::create_test_sparse_index;
use chroma_storage::local::LocalStorage;
use chroma_storage::Storage;
use chroma_sysdb::TestSysDb;
use chroma_system::Orchestrator;
use chroma_types::chroma_proto::FilePaths;
use chroma_types::chroma_proto::FlushSegmentCompactionInfo;
use chroma_types::Segment;
use chroma_types::SegmentFlushInfo;
use chroma_types::SegmentScope;
use chroma_types::SegmentType;
use chroma_types::{CollectionUuid, SegmentUuid};
use chrono::DateTime;
use futures::executor::block_on;
use garbage_collector_library::garbage_collector_orchestrator::GarbageCollectorOrchestrator;
use garbage_collector_library::types::CleanupMode;
use garbage_collector_library::types::GarbageCollectorResponse;
use itertools::Itertools;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;
use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
use rand::prelude::SliceRandom;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use uuid::Uuid;

// SegmentBlockIdInfo is used to keep track of the segment block ids for a version.
// A vector of SegmentBlockIdInfo is enough to get all block ids associated with a version.
#[derive(Clone, Debug)]
struct SegmentBlockIdInfo {
    segment_id: SegmentUuid,
    block_ids: Vec<Uuid>,
    segment_type: SegmentType,
}

// Transitions for the State Machine.
#[derive(Clone, Debug)]
enum Transition {
    // Create a new collection.
    CreateCollection {
        id: String,
        creation_time_secs: u64,
        segments: Vec<Segment>,
    },
    // Add version to a specific collection.
    // id is the name of the collection.
    AddVersion {
        id: String,
        segment_block_ids: Vec<SegmentBlockIdInfo>,
        to_remove_block_ids: Vec<Uuid>,
        creation_time_secs: u64,
    },
    // Cleanup versions from a specific collection.
    CleanupVersions {
        id: String,
        cutoff_time: u64,
    },
}

type VersionToSegmentBlockIdsMap = HashMap<u64, Vec<SegmentBlockIdInfo>>;
type VersionToFilesMap = HashMap<u64, Vec<Uuid>>;
type VersionToCreationTimeMap = HashMap<u64, u64>;

#[derive(Clone, Debug)]
struct RefState {
    // Keep track of collections.
    // Used in pre-conditions to ensure AddVersion is only called on existing collections.
    collections: HashSet<String>,
    // Keep track of creation time for each version.
    coll_to_creation_time_map: HashMap<String, VersionToCreationTimeMap>,
    // Keep track of the segment and corresponding block ids for each version.
    // i.e. collection_uuid -> version -> Vec<SegmentBlockIdInfo>
    coll_to_segment_block_ids_map: HashMap<String, VersionToSegmentBlockIdsMap>,
    // Keep track of dropped block ids for each version.
    // This info can be used to compute the block ids to delete.
    // Using this info minimizes the change of making a mistake in cleanup computation.
    coll_to_dropped_block_ids_map: HashMap<String, VersionToFilesMap>,
    // Min versions to keep for all collections.
    min_versions_to_keep: u64,
    // Keep track of the highest registered time for all collections.
    // Helps to mock the time.
    highest_registered_time: u64, // TODO: Suffix with _secs to make it consistent with cutoff_secs.
    // Keep track of the files that were deleted in the last cleanup.
    last_cleanup_files: Vec<String>,
    last_cleanup_collection_id: String,
}

impl RefState {
    // TODO(rohitcp): Remove this if not needed.
    fn _new(min_versions_to_keep: u64) -> Self {
        Self {
            collections: HashSet::new(),
            coll_to_segment_block_ids_map: HashMap::new(),
            coll_to_creation_time_map: HashMap::new(),
            coll_to_dropped_block_ids_map: HashMap::new(),
            min_versions_to_keep,
            highest_registered_time: 100,
            last_cleanup_files: Vec::new(),
            last_cleanup_collection_id: String::new(),
        }
    }

    // Gets the block ids for a version. Uses to create mock block ids for next version.
    // Keep track of this enables the next version to re-use block ids and mimic actual prod behavior.
    pub fn get_segment_block_ids_for_version(
        &self,
        collection_id: String,
        version: u64,
    ) -> Vec<SegmentBlockIdInfo> {
        self.coll_to_segment_block_ids_map
            .get(&collection_id)
            .and_then(|versions| versions.get(&version))
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_current_version(&self, collection_id: String) -> u64 {
        self.coll_to_creation_time_map
            .get(&collection_id)
            .and_then(|versions| versions.keys().max().copied())
            .unwrap_or(0) // Return 0 if no versions exist
    }

    // Update the mapping of which block ids are present for a version.
    // The RefState should only keep track of version to files mapping, and nothing else.
    fn add_version(
        mut self,
        id: String,
        version: u64,
        creation_time_secs: u64,
        segment_block_ids: Vec<SegmentBlockIdInfo>,
        dropped_block_ids: Vec<Uuid>,
    ) -> Self {
        // Only proceed if collection exists
        if !self.collections.contains(&id) {
            return self;
        }

        // Initialize maps for new collections if they don't exist
        self.coll_to_creation_time_map
            .entry(id.clone())
            .or_default();
        self.coll_to_dropped_block_ids_map
            .entry(id.clone())
            .or_default();
        self.coll_to_segment_block_ids_map
            .entry(id.clone())
            .or_default();

        // Assert that the creation time is greater than the highest registered time.
        assert!(creation_time_secs > self.highest_registered_time);
        self.highest_registered_time = creation_time_secs;

        // Update the mappings
        self.coll_to_creation_time_map
            .get_mut(&id)
            .unwrap()
            .insert(version, creation_time_secs);
        self.coll_to_dropped_block_ids_map
            .get_mut(&id)
            .unwrap()
            .insert(version, dropped_block_ids);
        self.coll_to_segment_block_ids_map
            .get_mut(&id)
            .unwrap()
            .insert(version, segment_block_ids);

        self
    }

    fn create_collection(
        mut self,
        id: String,
        segments: Vec<Segment>,
        creation_time_secs: u64,
    ) -> Self {
        assert!(
            !self.collections.contains(&id),
            "RSM: create_collection: collection already exists: {}",
            id
        );

        self.collections.insert(id.clone());
        // Initialize empty maps for the new collection
        self.coll_to_dropped_block_ids_map
            .insert(id.clone(), HashMap::new());

        // Put the segment block ids for the collection.
        // AddVersion calls will need to use this to find the segment block ids for the collection.
        self.coll_to_segment_block_ids_map
            .insert(id.clone(), HashMap::new());
        let segment_block_ids = segments
            .iter()
            .map(|s| SegmentBlockIdInfo {
                segment_id: s.id,
                block_ids: vec![],
                segment_type: s.r#type,
            })
            .collect();
        self.coll_to_segment_block_ids_map
            .get_mut(&id)
            .unwrap()
            .insert(0, segment_block_ids);

        // Insert the initial version to creation time mapping.
        // This is used to find current version for the collection, and the creation time for the initial version.
        let mut initial_version_to_creation_time = HashMap::new();
        initial_version_to_creation_time.insert(0, creation_time_secs);
        self.coll_to_creation_time_map
            .insert(id.clone(), initial_version_to_creation_time);
        self
    }

    fn cleanup_versions(mut self, collection_id: String, cutoff_time: u64) -> Self {
        assert!(
            self.collections.contains(&collection_id),
            "RSM: cleanup_versions: collection does not exist: {}",
            collection_id
        );

        // For debugging purposes, keep track of the collection id for which cleanup is being done.
        self.last_cleanup_collection_id = collection_id.clone();

        // First get all versions present for the collection
        let versions_present: Vec<u64> = self
            .coll_to_creation_time_map
            .get(&collection_id)
            .unwrap()
            .iter()
            .sorted_by_key(|(version, _)| *version)
            .rev()
            .map(|(version, _)| *version)
            .collect();

        // Then get the oldest version to keep
        let oldest_version_to_keep = versions_present
            .get(self.min_versions_to_keep as usize - 1) // -1 since its 0-indexed.
            .unwrap();

        let mut versions_to_delete = self
            .coll_to_creation_time_map
            .get(&collection_id)
            .unwrap()
            .iter()
            .filter(|(version, creation_time)| {
                **creation_time < cutoff_time && version < &oldest_version_to_keep && **version > 0
            })
            .map(|(version, _)| *version)
            .collect::<Vec<_>>();
        versions_to_delete.sort();

        tracing::info!(
            line = line!(),
            "RefState: cleanup_versions:  versions to creation time: {:?}",
            self.coll_to_creation_time_map
        );
        tracing::info!(
            line = line!(),
            "RSM: cleanup_versions: cutoff_time: {:?}, versions_present: {:?}, oldest_to_keep: {:?}, to_delete: {:?}   ",
            cutoff_time,
            versions_present,
            oldest_version_to_keep,
            versions_to_delete
        );

        // Method 1: Using segment block IDs
        let mut files_to_delete_method1 = HashSet::new();
        for version in versions_to_delete.clone() {
            let next_version = version + 1; // Since the min_versions_to_keep is always > 1, we can be sure next_version exists in the hashamp.
            let current_segments = &self.coll_to_segment_block_ids_map[&collection_id][&version];
            let next_segments = &self.coll_to_segment_block_ids_map[&collection_id][&next_version];

            for current_segment in current_segments {
                if let Some(next_segment) = next_segments
                    .iter()
                    .find(|s| s.segment_id == current_segment.segment_id)
                {
                    for block_id in &current_segment.block_ids {
                        // If the block id is not present in the next version, add it to the files to delete.
                        if !next_segment.block_ids.contains(block_id) {
                            files_to_delete_method1.insert(*block_id);
                        }
                    }
                } else {
                    panic!(
                        "RSM: cleanup_versions: segment not found in next version: {:?}",
                        current_segment.segment_id
                    );
                }
            }
        }

        // Method 2: Using dropped block IDs map
        let mut files_to_delete_method2 = HashSet::new();
        for version in versions_to_delete.clone() {
            let next_version = version + 1;
            if let Some(dropped_blocks) =
                self.coll_to_dropped_block_ids_map[&collection_id].get(&next_version)
            {
                files_to_delete_method2.extend(dropped_blocks.iter().cloned());
            }
        }

        // Verify both methods give the same result
        assert_eq!(
            files_to_delete_method1,
            files_to_delete_method2,
            "RSM: cleanup_versions: different results from two methods. Method1: {:?}, Method2: {:?}",
            files_to_delete_method1,
            files_to_delete_method2
        );

        // Update last_cleanup_files with the files to delete (can use either method since they're equal)
        self.last_cleanup_files = files_to_delete_method1
            .iter()
            .map(|uuid| uuid.to_string())
            .collect();

        // Print the entire version to segment block id mapping for the collection.
        tracing::info!(
            line = line!(),
            "************\nRSM: cleanup_versions: version to segment block id mapping for collection: {:?}\n************",
            self.coll_to_segment_block_ids_map[&collection_id]
        );
        // Print the files to delete.
        tracing::info!(
            line = line!(),
            "************\nRSM: cleanup_versions: files to delete for collection: {:?}\n************",
            self.last_cleanup_files
        );

        self
    }
}

impl ReferenceStateMachine for RefState {
    type Transition = Transition;
    type State = RefState;

    fn init_state() -> BoxedStrategy<Self> {
        Just(Self {
            collections: HashSet::new(),
            coll_to_creation_time_map: HashMap::new(),
            coll_to_dropped_block_ids_map: HashMap::new(),
            coll_to_segment_block_ids_map: HashMap::new(),
            min_versions_to_keep: 2,
            highest_registered_time: 100,
            last_cleanup_files: Vec::new(),
            last_cleanup_collection_id: String::new(),
        })
        .boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        let new_collection_id_strategy = Just(()).prop_map(|_| Uuid::new_v4().to_string());
        let existing_collection_ids: Vec<String> = state.collections.iter().cloned().collect();
        let state_clone = state.clone();
        // Create a random cutoff window between 1 and 10.
        let cutoff_window_secs = 3;
        // Compute the cutoff time.
        let cutoff_time = state_clone.highest_registered_time - cutoff_window_secs;

        if existing_collection_ids.is_empty() {
            // If no collections exist, only generate CreateCollection transitions
            new_collection_id_strategy
                .prop_map(move |id| {
                    let next_time = state_clone.highest_registered_time + 1;
                    Transition::CreateCollection {
                        id: id.clone(),
                        creation_time_secs: next_time,
                        segments: generate_segments_for_collection(
                            CollectionUuid::from_str(&id.clone()).unwrap(),
                        ),
                    }
                })
                .boxed()
        } else {
            // Otherwise generate all types of transitions
            prop_oneof![
                // Weight the strategies to make CreateCollection less frequent
                1 => new_collection_id_strategy
                    .prop_map(move |id| {
                        let next_time = state_clone.highest_registered_time + 1;
                        Transition::CreateCollection {
                            id: id.clone(),
                            creation_time_secs: next_time,
                            segments: generate_segments_for_collection(CollectionUuid::from_str(&id.clone()).unwrap()),
                        }
                    }),
                4 => prop::sample::select(existing_collection_ids.clone()).prop_map(move |id| {
                    let segment_block_ids = state_clone.get_segment_block_ids_for_version(
                        id.clone(),
                        state_clone.get_current_version(id.clone()),
                    );
                    // tracing::info!(
                    //     line = line!(),
                    //     "RSM: transitions: segment_block_ids for existing collection: {:?}",
                    //     segment_block_ids
                    // );
                    let (segment_block_ids_new_version, dropped_block_ids) = segment_block_ids_for_next_version(segment_block_ids);
                    Transition::AddVersion {
                        id: id.clone(),
                        to_remove_block_ids: dropped_block_ids,
                        creation_time_secs: state_clone.highest_registered_time + 1,
                        segment_block_ids: segment_block_ids_new_version,
                    }
                }),
                2 => prop::sample::select(existing_collection_ids).prop_map(move |id| {
                    Transition::CleanupVersions {
                        id: id.clone(),
                        cutoff_time,
                    }
                }),
            ]
            .boxed()
        }
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        match transition {
            Transition::AddVersion {
                id,
                to_remove_block_ids: _,
                creation_time_secs: _,
                segment_block_ids: _,
            } => state.collections.contains(id),
            Transition::CleanupVersions { id, cutoff_time } => {
                state.collections.contains(id) &&
                *cutoff_time <= state.highest_registered_time &&
                // Check if we have enough versions to perform cleanup
                state.coll_to_creation_time_map
                    .get(id)
                    .map(|versions| versions.len() > state.min_versions_to_keep as usize)
                    .unwrap_or(false)
            }
            Transition::CreateCollection {
                id,
                segments: _,
                creation_time_secs: _,
            } => !state.collections.contains(id),
        }
    }

    fn apply(state: Self::State, transition: &Self::Transition) -> Self {
        // tracing::info!(
        //     line = line!(),
        //     "Applying transition: {:?} to RefState",
        //     transition
        // );
        match transition {
            Transition::AddVersion {
                id,
                to_remove_block_ids,
                creation_time_secs,
                segment_block_ids,
            } => state.clone().add_version(
                id.clone(),
                state.clone().get_current_version(id.clone()) + 1,
                *creation_time_secs,
                segment_block_ids.clone(),
                to_remove_block_ids.clone(),
            ),
            Transition::CleanupVersions { id, cutoff_time } => {
                state.clone().cleanup_versions(id.clone(), *cutoff_time)
            }
            Transition::CreateCollection {
                id,
                segments,
                creation_time_secs,
            } => state
                .clone()
                .create_collection(id.clone(), segments.clone(), *creation_time_secs),
        }
    }
}

// Add this at the top level of the file
static INVARIANT_CHECK_COUNT: AtomicUsize = AtomicUsize::new(0);

struct GcTest {
    storage: Storage,
    sysdb: chroma_sysdb::SysDb,
    last_cleanup_files: Vec<String>,
}

impl Default for GcTest {
    fn default() -> Self {
        // Create local storage for testing
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage_dir = tmp_dir.path().to_str().unwrap();
        tracing::info!(line = line!(), "GcTest: storage_dir: {:?}", storage_dir);
        let storage = Storage::Local(LocalStorage::new(storage_dir));

        // Create test sysdb instance
        let mut sysdb = chroma_sysdb::SysDb::Test(TestSysDb::new());

        // Set storage using block_on since set_storage is async
        if let chroma_sysdb::SysDb::Test(test_sysdb) = &mut sysdb {
            test_sysdb.set_storage(Some(storage.clone()));
        }

        Self {
            storage,
            sysdb,
            last_cleanup_files: Vec::new(),
        }
    }
}

fn get_version_file_name(sysdb: &chroma_sysdb::SysDb, id: String) -> String {
    let collection_id = CollectionUuid::from_str(&id).unwrap();
    match sysdb {
        chroma_sysdb::SysDb::Test(test_sysdb) => test_sysdb.get_version_file_name(collection_id),
        _ => panic!("get_version_file_name only supported for TestSysDb"),
    }
}

impl GcTest {
    // Logic:
    // 1. Get version file name from sysdb.
    // 2. Prepare to call flush compaction.
    // 3. Call FlushCompaction on TestSysDb.
    // 4. Update the version file in storage since SysDb does not do this.
    async fn add_version(
        mut self,
        id: String,
        _version: u64,
        creation_time_secs: u64,
        segment_block_ids: Vec<SegmentBlockIdInfo>,
    ) -> Self {
        // Set the mock time before calling flush_compaction
        if let chroma_sysdb::SysDb::Test(test_sysdb) = &mut self.sysdb {
            test_sysdb.set_mock_time(creation_time_secs);
        }

        // 1. Get version file name and current version from sysdb
        let collection_id = CollectionUuid::from_str(&id).unwrap();
        let collections = self
            .sysdb
            .get_collections(Some(collection_id), None, None, None, None, 0)
            .await
            .unwrap();

        let collection = match collections.first() {
            Some(c) => c,
            None => return self,
        };
        let current_version = collection.version;

        // ----- Prepare to call flush compaction.  ---------
        // Create the new record segment.
        let record_segment_info = segment_block_ids
            .iter()
            .find(|sbi| sbi.segment_type == SegmentType::BlockfileRecord)
            .unwrap()
            .clone();
        // Create sparse index for record segment
        let sparse_index_id = create_test_sparse_index(
            &self.storage,
            Uuid::new_v4(),
            record_segment_info.block_ids.clone(),
            Some("test_si_rec_".to_string()),
        )
        .await
        .unwrap();
        // Create segment info for this version
        let record_segment_id = record_segment_info.segment_id;
        let mut file_paths = HashMap::new();
        file_paths.insert(
            "rec_blockfile_1".to_string(),
            FilePaths {
                paths: vec![sparse_index_id.to_string()],
            },
        );
        let record_segment_info = FlushSegmentCompactionInfo {
            segment_id: record_segment_id.to_string(),
            file_paths,
        };

        // Create sparse index for metadata segment
        let metadata_segment_info = segment_block_ids
            .iter()
            .find(|sbi| sbi.segment_type == SegmentType::BlockfileMetadata)
            .unwrap()
            .clone();
        let sparse_index_id_metadata = create_test_sparse_index(
            &self.storage,
            Uuid::new_v4(),
            metadata_segment_info.block_ids.clone(),
            Some("test_si_meta_".to_string()),
        )
        .await
        .unwrap();
        // Create segment info for this version
        let metadata_segment_id = metadata_segment_info.segment_id;
        let mut file_paths_metadata = HashMap::new();
        file_paths_metadata.insert(
            "metadata_blockfile_1".to_string(),
            FilePaths {
                paths: vec![sparse_index_id_metadata.to_string()],
            },
        );
        let metadata_segment_info = FlushSegmentCompactionInfo {
            segment_id: metadata_segment_id.to_string(),
            file_paths: file_paths_metadata,
        };

        let record_segment_id = SegmentUuid::from_str(&record_segment_info.segment_id).unwrap();
        let metadata_segment_id = SegmentUuid::from_str(&metadata_segment_info.segment_id).unwrap();

        // Handle flush_compaction errors
        match self
            .sysdb
            .clone()
            .flush_compaction(
                "tenant".to_string(),
                collection_id,
                0, // log_position
                current_version,
                Arc::new([
                    SegmentFlushInfo {
                        segment_id: record_segment_id,
                        file_paths: record_segment_info
                            .file_paths
                            .into_iter()
                            .map(|(k, v)| (k, v.paths))
                            .collect(),
                    },
                    SegmentFlushInfo {
                        segment_id: metadata_segment_id,
                        file_paths: metadata_segment_info
                            .file_paths
                            .into_iter()
                            .map(|(k, v)| (k, v.paths))
                            .collect(),
                    },
                ]),
                0, // total_records_post_compaction
                0, // size_bytes_post_compaction
            )
            .await
        {
            Ok(_) => (),
            Err(e) => {
                panic!("Failed to flush compaction: {:?}", e);
            }
        }

        self
    }

    async fn create_collection(
        mut self,
        id: String,
        segments: Vec<Segment>,
        creation_time_secs: u64,
    ) -> Self {
        // Set the mock time before creating collection
        if let chroma_sysdb::SysDb::Test(test_sysdb) = &mut self.sysdb {
            test_sysdb.set_mock_time(creation_time_secs);
        }

        let collection_id = CollectionUuid::from_str(&id).unwrap();
        let result = self
            .sysdb
            .create_collection(
                "tenant".to_string(),
                "database".to_string(),
                collection_id,
                "collection".to_string(),
                segments,
                None,
                None,
                None,
                false,
            )
            .await;
        assert!(
            result.is_ok(),
            "Failed to create collection: {:?}",
            result.err()
        );
        self
    }

    async fn cleanup_versions(mut self, id: String, cutoff_time: u64) -> Self {
        let mut sysdb = self.sysdb.clone();
        let storage = self.storage.clone();

        // Do the actual Garbage Collection.
        let system = chroma_system::System::new();
        let dispatcher = chroma_system::Dispatcher::new(chroma_system::DispatcherConfig::default());
        let mut dispatcher_handle = system.start_component(dispatcher);

        let collection_id = Uuid::parse_str(&id).unwrap();
        let collections = sysdb
            .get_collections(
                Some(CollectionUuid(collection_id)),
                None,
                None,
                None,
                None,
                0,
            )
            .await
            .unwrap();

        // Return early if no collection is found
        let collection = match collections.first() {
            Some(c) => c,
            None => {
                panic!(
                    "Collection not found during cleanup: {}. Check preconditions logic.",
                    id
                );
            }
        };

        let version_file_name = get_version_file_name(&self.sysdb, id);
        let orchestrator = GarbageCollectorOrchestrator::new(
            collection.collection_id,
            version_file_name,
            DateTime::from_timestamp(cutoff_time as i64, 0).unwrap(),
            sysdb,
            dispatcher_handle.clone(),
            storage,
            CleanupMode::Delete,
        );

        self.last_cleanup_files = Vec::new();
        match orchestrator.run(system.clone()).await {
            #[expect(deprecated)]
            Ok(GarbageCollectorResponse { deletion_list, .. }) => {
                self.last_cleanup_files = deletion_list;

                tracing::info!(
                    line = line!(),
                    "GcTest: cleanup_versions: last_cleanup_files: {:?}",
                    self.last_cleanup_files
                );
            }
            Err(e) => {
                tracing::error!(line = line!(), "Error during garbage collection: {:?}", e);
            }
        }

        // Print the files to delete.
        tracing::debug!(
            line = line!(),
            "==========\nGcTest: cleanup_versions: last_cleanup_files: {:?}\n==========",
            self.last_cleanup_files
        );

        system.stop().await;
        system.join().await;
        dispatcher_handle.stop();
        dispatcher_handle.join().await.unwrap();

        self
    }
}

impl StateMachineTest for GcTest {
    type SystemUnderTest = Self;
    type Reference = RefState;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        tracing::info!(line = line!(), "Initializing new test instance");
        Self::default()
    }

    fn apply(
        state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        tracing::debug!(
            line = line!(),
            "Applying transition: {:?} to SUT",
            transition
        );
        match transition {
            Transition::AddVersion {
                id,
                to_remove_block_ids: _,
                creation_time_secs,
                segment_block_ids,
            } => block_on(state.add_version(
                id.clone(),
                ref_state.get_current_version(id.clone()) + 1,
                creation_time_secs,
                segment_block_ids.clone(),
            )),
            Transition::CreateCollection {
                id,
                segments,
                creation_time_secs,
            } => block_on(state.create_collection(id, segments, creation_time_secs)),
            Transition::CleanupVersions { id, cutoff_time } => {
                block_on(state.cleanup_versions(id, cutoff_time))
            }
        }
    }

    fn check_invariants(
        state: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        INVARIANT_CHECK_COUNT.fetch_add(1, Ordering::SeqCst);
        tracing::debug!(
            line = line!(),
            "Checking invariants (count: {})",
            INVARIANT_CHECK_COUNT.load(Ordering::SeqCst)
        );

        // Remove the block/ prefix and sort
        let mut state_last_cleanup_files: Vec<String> = state
            .last_cleanup_files
            .iter()
            .map(|file| file.replace("block/", ""))
            .collect();
        state_last_cleanup_files.sort();

        // Sort reference state files
        let mut ref_last_cleanup_files = ref_state.last_cleanup_files.clone();
        ref_last_cleanup_files.sort();

        assert_eq!(
            state_last_cleanup_files,
            ref_last_cleanup_files,
            "Cleanup files mismatch for collection: {:?} after sorting - SUT: {:?}, Reference: {:?}",
            ref_state.last_cleanup_collection_id,
            state_last_cleanup_files,
            ref_last_cleanup_files
        );
    }
}

fn generate_segments_for_collection(collection_id: CollectionUuid) -> Vec<Segment> {
    let record_segment = Segment {
        id: SegmentUuid::new(),
        r#type: SegmentType::BlockfileRecord,
        scope: SegmentScope::RECORD,
        collection: collection_id,
        metadata: None,
        file_path: HashMap::new(),
    };
    let metadata_segment = Segment {
        id: SegmentUuid::new(),
        r#type: SegmentType::BlockfileMetadata,
        scope: SegmentScope::METADATA,
        collection: collection_id,
        metadata: None,
        file_path: HashMap::new(),
    };

    vec![record_segment, metadata_segment]
}

fn segment_block_ids_for_next_version(
    existing_segment_block_ids: Vec<SegmentBlockIdInfo>,
) -> (Vec<SegmentBlockIdInfo>, Vec<Uuid>) {
    let mut new_segment_block_ids = Vec::new();
    let mut dropped_block_ids = Vec::new();
    for segment in existing_segment_block_ids {
        let block_ids = segment.block_ids.clone();
        let (new_block_ids, dropped_ids) = blocks_ids_for_next_version(block_ids);
        new_segment_block_ids.push(SegmentBlockIdInfo {
            segment_id: segment.segment_id,
            block_ids: new_block_ids,
            segment_type: segment.segment_type,
        });
        // Add dropped block ids to the list of dropped block ids
        dropped_block_ids.extend(dropped_ids);
    }

    (new_segment_block_ids, dropped_block_ids)
}

fn blocks_ids_for_next_version(block_ids: Vec<Uuid>) -> (Vec<Uuid>, Vec<Uuid>) {
    let mut rng = rand::thread_rng();

    // If there are no existing block IDs, just generate new ones
    if block_ids.is_empty() {
        let num_new_blocks = rng.gen_range(1..=10);
        let new_block_ids: Vec<Uuid> = (0..num_new_blocks).map(|_| Uuid::new_v4()).collect();
        // tracing::info!(
        //     line = line!(),
        //     "RSM: new blocks_ids_for_next_version: new_block_ids: {:?}",
        //     new_block_ids
        // );
        return (new_block_ids, Vec::new());
    }

    let keep_percentage = rng.gen_range(30..=90) as f64 / 100.0;
    let num_to_keep = (block_ids.len() as f64 * keep_percentage).ceil() as usize;
    let mut kept_block_ids: Vec<Uuid> = block_ids
        .choose_multiple(&mut rng, num_to_keep)
        .cloned()
        .collect();
    let num_new_blocks = rng.gen_range(0..=10);
    let new_block_ids: Vec<Uuid> = (0..num_new_blocks).map(|_| Uuid::new_v4()).collect();
    let dropped_block_ids = block_ids
        .into_iter()
        .filter(|id| !kept_block_ids.contains(id))
        .collect();

    kept_block_ids.extend(new_block_ids);
    (kept_block_ids, dropped_block_ids)
}

prop_state_machine! {
    fn run_gc_test(
        sequential
        1..50
        =>
        GcTest
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn run_gc_test_ext() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    INVARIANT_CHECK_COUNT.store(0, Ordering::SeqCst);
    run_gc_test();
    let checks = INVARIANT_CHECK_COUNT.load(Ordering::SeqCst);
    assert!(
        checks > 0,
        "check_invariants was never called! Count: {}",
        checks
    );
}
