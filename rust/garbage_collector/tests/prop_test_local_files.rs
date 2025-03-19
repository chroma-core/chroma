use chroma_blockstore::test_utils::sparse_index_test_utils::create_test_sparse_index;
use chroma_storage::local::LocalStorage;
use chroma_storage::Storage;
use chroma_sysdb::TestSysDb;
use chroma_system::Orchestrator;
use chroma_types::chroma_proto::FilePaths;
use chroma_types::chroma_proto::FlushSegmentCompactionInfo;
use chroma_types::SegmentFlushInfo;
use chroma_types::{CollectionUuid, SegmentUuid};
use futures::executor::block_on;
use garbage_collector_library::garbage_collector_orchestrator::GarbageCollectorOrchestrator;
use itertools::Itertools;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;
use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
use rand::prelude::SliceRandom;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;
use std::sync::atomic::{AtomicUsize, Ordering};
use chroma_types::Segment;
use chroma_types::SegmentType;
use chroma_types::SegmentScope;

#[derive(Clone, Debug)]
struct SegmentBlockIdInfo {
    segment_id: SegmentUuid,
    block_ids: Vec<Uuid>,
    segment_type: SegmentType,
}

#[derive(Clone, Debug)] // Add Arbitrary derive
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
        version_block_ids: Vec<Uuid>,
        to_remove_block_ids: Vec<Uuid>,
        creation_time_secs: u64,
    },
    // Cleanup versions from a specific collection.
    CleanupVersions {
        id: String,
        cutoff_window_secs: u64,
    },
}

type VersionToSegmentBlockIdsMap = HashMap<u64, Vec<SegmentBlockIdInfo>>;
type VersionToFilesMap = HashMap<u64, Vec<Uuid>>;
type VersionToCreationTimeMap = HashMap<u64, u64>;

#[derive(Clone, Debug)]
struct RefState {
    // Keep track of collections.
    collections: HashSet<String>,
    // Keep track of version files for each collection.
    coll_to_files_map: HashMap<String, VersionToFilesMap>,
    // Keep track of creation time for each version.
    coll_to_creation_time_map: HashMap<String, VersionToCreationTimeMap>,
    // Keep track of dropped block ids for each version.
    coll_to_dropped_block_ids_map: HashMap<String, VersionToFilesMap>,
    // Min versions to keep for all collections.
    min_versions_to_keep: u64,
    // Keep track of the highest registered time for all collections.
    highest_registered_time: u64,
    // Keep track of the files that were deleted in the last cleanup.
    last_cleanup_files: Vec<String>,
    coll_to_segment_ids: HashMap<String, Vec<Segment>>,
    coll_to_segment_block_ids_map: HashMap<String, VersionToSegmentBlockIdsMap>,
}

impl RefState {
    fn _new(min_versions_to_keep: u64) -> Self {
        Self {
            collections: HashSet::new(),
            coll_to_files_map: HashMap::new(),
            coll_to_segment_block_ids_map: HashMap::new(),
            coll_to_creation_time_map: HashMap::new(),
            coll_to_dropped_block_ids_map: HashMap::new(),
            min_versions_to_keep,
            highest_registered_time: 100,
            last_cleanup_files: Vec::new(),
            coll_to_segment_ids: HashMap::new(),
        }
    }

    pub fn get_segment_block_ids_for_version(&self, collection_id: String, version: u64) -> Vec<SegmentBlockIdInfo> {
        self.coll_to_segment_block_ids_map
            .get(&collection_id)
            .and_then(|versions| versions.get(&version))
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_block_ids_for_version(&self, collection_id: String, version: u64) -> Vec<Uuid> {
        self.coll_to_files_map
            .get(&collection_id)
            .and_then(|versions| versions.get(&version))
            .cloned()
            .unwrap_or_default()  // Return empty Vec if collection or version doesn't exist
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
        block_ids: Vec<Uuid>,
        dropped_block_ids: Vec<Uuid>,
    ) -> Self {
        // Only proceed if collection exists
        if !self.collections.contains(&id) {
            return self;
        }

        // Initialize maps for new collections if they don't exist
        self.coll_to_files_map.entry(id.clone()).or_default();
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
        self.coll_to_files_map
            .get_mut(&id)
            .unwrap()
            .insert(version, block_ids);
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

    fn create_collection(mut self, id: String, segments: Vec<Segment>) -> Self {
        if self.collections.contains(&id) {
            tracing::debug!("RSM: create_collection: Collection already exists: {}", id);
            return self;
        }
        self.collections.insert(id.clone());
        // Initialize empty maps for the new collection
        self.coll_to_files_map.insert(id.clone(), HashMap::new());
        self.coll_to_creation_time_map
            .insert(id.clone(), HashMap::new());
        self.coll_to_dropped_block_ids_map
            .insert(id.clone(), HashMap::new());
        self.coll_to_segment_block_ids_map
            .insert(id.clone(), HashMap::new());
        self.coll_to_segment_ids.insert(id.clone(), segments);
        self
    }

    fn cleanup_versions(mut self, collection_id: String, cutoff_window_secs: u64) -> Self {
        let cutoff_time = self.highest_registered_time - cutoff_window_secs;
        // We need to maintain atlest a min number of versions for the collection.
        // So the versions to check are all versions >= oldest_version_to_keep.
        // If min_versions_to_keep is 3, then oldest_version_to_keep is found by sorting the versions and picking the 3rd largest one.
        let oldest_version_to_keep = self
            .coll_to_creation_time_map
            .get(&collection_id)
            .unwrap()
            .iter()
            .sorted_by_key(|(version, _)| *version)
            .rev()
            .nth(self.min_versions_to_keep as usize)
            .unwrap();

        let versions_to_delete = self
            .coll_to_creation_time_map
            .get(&collection_id)
            .unwrap()
            .iter()
            .filter(|(version, creation_time)| {
                **creation_time < cutoff_time && version < &oldest_version_to_keep.0
            })
            .map(|(version, _)| *version)
            .collect::<Vec<_>>();

        // Get all versions sorted in ascending order
        let mut all_versions = self
            .coll_to_files_map
            .get(&collection_id)
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        all_versions.sort();

        // For each version to delete, identify files that can be safely removed
        let mut files_to_delete = HashSet::new();
        for version in versions_to_delete.clone() {
            let next_version = version + 1;
            // Get files for current version and next version
            let current_files = &self.coll_to_files_map[&collection_id][&version];
            let next_files = &self.coll_to_files_map[&collection_id][&next_version];

            // Files that can be deleted are those in current_files that aren't in next_files
            for file in current_files {
                if !next_files.contains(file) {
                    files_to_delete.insert(file.clone());
                }
            }
        }

        // Remove the version entries from our maps
        for version in versions_to_delete {
            self.coll_to_files_map
                .get_mut(&collection_id)
                .unwrap()
                .remove(&version);
            self.coll_to_creation_time_map
                .get_mut(&collection_id)
                .unwrap()
                .remove(&version);
            self.coll_to_segment_block_ids_map
                .get_mut(&collection_id)
                .unwrap()
                .remove(&version);
        }

        // Convert the files to strings.
        let files_to_delete: Vec<String> = files_to_delete
            .into_iter()
            .map(|file| format!("block/{}", file))
            .collect();
        tracing::info!("RSM: cleanup_versions: files_to_delete: {:?}", files_to_delete);
        self.last_cleanup_files = files_to_delete;

        self
    }
}

impl ReferenceStateMachine for RefState {
    type Transition = Transition;
    type State = RefState;

    fn init_state() -> BoxedStrategy<Self> {
        Just(Self {
            collections: HashSet::new(),
            coll_to_files_map: HashMap::new(),
            coll_to_creation_time_map: HashMap::new(),
            coll_to_dropped_block_ids_map: HashMap::new(),
            coll_to_segment_block_ids_map: HashMap::new(),
            min_versions_to_keep: 3,
            highest_registered_time: 100,
            last_cleanup_files: Vec::new(),
            coll_to_segment_ids: HashMap::new(),
        })
        .boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        let new_collection_id_strategy = Just(()).prop_map(|_| Uuid::new_v4().to_string());
        let existing_collection_ids: Vec<String> = 
            state.collections.iter().cloned().collect();
        let state_clone = state.clone();
        // Create a random cutoff window between 1 and 10.
        let cutoff_window_secs = 3;

        if existing_collection_ids.is_empty() {
            // If no collections exist, only generate CreateCollection transitions
            new_collection_id_strategy
                .prop_map(move |id| {
                    let next_time = state_clone.highest_registered_time + 1;
                    Transition::CreateCollection {
                        id: id.clone(),
                        creation_time_secs: next_time,
                        segments: generate_segments_for_collection(CollectionUuid::from_str(&id.clone()).unwrap()),
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
                    // let (block_ids_new_version, block_ids_dropped) =
                    //     blocks_ids_for_next_version(state_clone.get_block_ids_for_version(
                    //         id.clone(),
                    //         state_clone.get_current_version(id.clone()),
                    //     ));
                    let segment_block_ids = state_clone.get_segment_block_ids_for_version(
                        id.clone(),
                        state_clone.get_current_version(id.clone()),
                    );
                    let (segment_block_ids_new_version, dropped_block_ids) = segment_block_ids_for_next_version(segment_block_ids);
                    Transition::AddVersion {
                        id: id.clone(),
                        version_block_ids: Vec::new(), // block_ids_new_version,
                        to_remove_block_ids: dropped_block_ids,
                        creation_time_secs: state_clone.highest_registered_time + 1,
                        segment_block_ids: segment_block_ids_new_version,
                    }
                }),
                2 => prop::sample::select(existing_collection_ids).prop_map(move |id| {
                    Transition::CleanupVersions {
                        id: id.clone(),
                        cutoff_window_secs: cutoff_window_secs,
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
                version_block_ids: _,
                to_remove_block_ids: _,
                creation_time_secs: _,
                segment_block_ids: _,
            } => state.collections.contains(id),
            Transition::CleanupVersions {
                id,
                cutoff_window_secs,
            } => {
                state.collections.contains(id) && 
                *cutoff_window_secs <= state.highest_registered_time &&
                // Check if we have enough versions to perform cleanup
                state.coll_to_creation_time_map
                    .get(id)
                    .map(|versions| versions.len() > state.min_versions_to_keep as usize)
                    .unwrap_or(false)
            },
            Transition::CreateCollection {
                id,
                segments: _,
                creation_time_secs: _,
            } => !state.collections.contains(id),
        }
    }

    fn apply(state: Self::State, transition: &Self::Transition) -> Self {
        tracing::info!("Applying transition: {:?} to RefState", transition);
        match transition {
            Transition::AddVersion {
                id,
                version_block_ids,
                to_remove_block_ids,
                creation_time_secs,
                segment_block_ids,
            } => state.clone().add_version(
                id.clone(),
                state.clone().get_current_version(id.clone()) + 1,
                *creation_time_secs,
                segment_block_ids.clone(),
                version_block_ids.clone(),
                to_remove_block_ids.clone(),
            ),
            Transition::CleanupVersions {
                id,
                cutoff_window_secs,
            } => state
                .clone()
                .cleanup_versions(id.clone(), *cutoff_window_secs),
            Transition::CreateCollection {
                id,
                segments,
                creation_time_secs: _,
            } => state.clone().create_collection(id.clone(), segments.clone()),
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
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));

        // Create test sysdb instance
        let sysdb = chroma_sysdb::SysDb::Test(TestSysDb::new());

        Self {
            storage,
            sysdb,
            last_cleanup_files: Vec::new(),
        }
    }
}

impl GcTest {
    // Logic:
    // 1. Get version file name from sysdb.
    // 2. Prepare to call flush compaction.
    // 3. Call FlushCompaction on TestSysDb.
    // 4. Update the version file in storage since SysDb does not do this.
    fn add_version(
        mut self,
        id: String,
        _version: u64,
        _creation_time_secs: u64,
        segment_block_ids: Vec<SegmentBlockIdInfo>,
        version_block_ids: Vec<Uuid>,
    ) -> Self {
        // 1. Get version file name and current version from sysdb
        let collection_id = CollectionUuid::from_str(&id).unwrap();
        let collections =
            block_on(
                self.sysdb
                    .get_collections(Some(collection_id), None, None, None, None, 0),
            )
            .unwrap();

        let collection = match collections.first() {
            Some(c) => c,
            None => return self,
        };
        let current_version = collection.version;
        let _version_file_name = collection.version_file_name.clone();

        // ----- Prepare to call flush compaction.  ---------
        // Use half of version_block_ids as the block ids for the new version.
        // Create the new record segment.
        let record_segment_info = segment_block_ids.iter()
            .find(|sbi| sbi.segment_type == SegmentType::BlockfileRecord)
            .unwrap()
            .clone();
        // Create sparse index for record segment
        let sparse_index_id = block_on(create_test_sparse_index(
            &self.storage,
            record_segment_info.block_ids.clone(),
            Some("test_si_".to_string()),
        ))
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
        let metadata_segment_info = segment_block_ids.iter()
            .find(|sbi| sbi.segment_type == SegmentType::BlockfileMetadata)
            .unwrap()
            .clone();
        let sparse_index_id = block_on(create_test_sparse_index(
            &self.storage,
            metadata_segment_info.block_ids.clone(),
            Some("test_si_".to_string()),
        ))
        .unwrap();
        // Create segment info for this version
        let metadata_segment_id = metadata_segment_info.segment_id;
        let mut file_paths = HashMap::new();
        file_paths.insert(
            "metadata_blockfile_1".to_string(),
            FilePaths {
                paths: vec![sparse_index_id.to_string()],
            },
        );
        let metadata_segment_info = FlushSegmentCompactionInfo {
            segment_id: metadata_segment_id.to_string(),
            file_paths,
        };

        let record_segment_id = SegmentUuid::from_str(&record_segment_info.segment_id).unwrap();
        let metadata_segment_id = SegmentUuid::from_str(&metadata_segment_info.segment_id).unwrap();

        // Handle flush_compaction errors
        match block_on(
            self.sysdb.clone().flush_compaction(
                "tenant".to_string(),
                collection_id,
                0, // log_position
                current_version as i32,
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
            ),
        ) {
            Ok(_) => (),
            Err(e) => {
                panic!("Failed to flush compaction: {:?}", e);
            }
        }

        self
    }

    fn create_collection(mut self, id: String, segments: Vec<Segment>, _creation_time_secs: u64) -> Self {
        let collection_id = CollectionUuid::from_str(&id).unwrap();
        let result = block_on(self.sysdb.create_collection(
            "tenant".to_string(),
            "database".to_string(),
            collection_id,
            "collection".to_string(),
            segments,
            None,
            None,
            false,
        ));
        assert!(result.is_ok(), "Failed to create collection: {:?}", result.err());
        self
    }

    async fn cleanup_versions_async(mut self, id: String, _cutoff_window_secs: u64) -> Self {
        let mut sysdb = self.sysdb.clone();
        let storage = self.storage.clone();

        // Do the actual Garbage Collection.
        let system = chroma_system::System::new();
        let dispatcher = chroma_system::Dispatcher::new(chroma_system::DispatcherConfig::default());
        let dispatcher_handle = system.start_component(dispatcher);

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
                tracing::warn!("Collection not found during cleanup: {}", id);
                return self;
            }
        };

        let orchestrator = GarbageCollectorOrchestrator::new(
            collection.collection_id,
            collection.version_file_name.clone(),
            0,
            sysdb,
            dispatcher_handle,
            storage,
        );

        match orchestrator.run(system).await {
            Ok(response) => {
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                self.last_cleanup_files = response.deletion_list.clone();
                tracing::info!("RSM: cleanup_versions: last_cleanup_files: {:?}", self.last_cleanup_files);
            }
            Err(e) => {
                tracing::error!("Error during garbage collection: {:?}", e);
            }
        }

        self
    }

    fn cleanup_versions(self, id: String, cutoff_window_secs: u64) -> Self {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(self.cleanup_versions_async(id, cutoff_window_secs))
    }
}

impl StateMachineTest for GcTest {
    type SystemUnderTest = Self;
    type Reference = RefState;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        tracing::info!("Initializing new test instance");
        Self::default()
    }

    fn apply(
        state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        tracing::info!("Applying transition: {:?} to SUT", transition);
        let state = match transition {
            Transition::AddVersion {
                id,
                version_block_ids,
                to_remove_block_ids: _,
                creation_time_secs,
                segment_block_ids,
            } => state.add_version(
                id.clone(),
                ref_state.get_current_version(id.clone()) + 1,
                creation_time_secs,
                segment_block_ids.clone(),
                version_block_ids.clone(),
            ),
            Transition::CreateCollection {
                id,
                segments,
                creation_time_secs,
            } => state.create_collection(id, segments, creation_time_secs),
            Transition::CleanupVersions {
                id,
                cutoff_window_secs,
            } => state.cleanup_versions(id, cutoff_window_secs),
        };
        state
    }

    fn check_invariants(
        state: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        INVARIANT_CHECK_COUNT.fetch_add(1, Ordering::SeqCst);
        tracing::info!(
            "Checking invariants (count: {})",
            INVARIANT_CHECK_COUNT.load(Ordering::SeqCst)
        );
        
        assert_eq!(state.last_cleanup_files, ref_state.last_cleanup_files);
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

fn segment_block_ids_for_next_version(existing_segment_block_ids: Vec<SegmentBlockIdInfo>) -> (Vec<SegmentBlockIdInfo>, Vec<Uuid>) {
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
    #![proptest_config(ProptestConfig::with_cases(20))]
    // #[test]
    fn run_gc_test(
        sequential
        1..20
        =>
        GcTest
    );
}

#[test]
fn run_gc_test_ext() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    INVARIANT_CHECK_COUNT.store(0, Ordering::SeqCst);
    run_gc_test();
    let checks = INVARIANT_CHECK_COUNT.load(Ordering::SeqCst);
    assert!(checks > 0, "check_invariants was never called! Count: {}", checks);
}
