use chroma_blockstore::RootManager;
use chroma_cache::nop::NopCache;
use chroma_storage::{test_storage, Storage};
use chroma_sysdb::{SysDb, TestSysDb};
use chroma_system::{ComponentHandle, Dispatcher, DispatcherConfig, System};
use chroma_types::CollectionUuid;
use chrono::DateTime;
use futures::executor::block_on;
use garbage_collector_library::garbage_collector_orchestrator_v2::GarbageCollectorOrchestrator;
use garbage_collector_library::types::CleanupMode;
use petgraph::dot::Dot;
use petgraph::graph::DiGraph;
use petgraph::visit::EdgeRef;
use petgraph::visit::Topo;
use proptest::prelude::any;
use proptest::strategy::Strategy;
use proptest::{prelude::Just, prop_oneof};
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[derive(Clone, Debug)]
enum Transition {
    GarbageCollect {
        tick_advance: u64,
        collection_id: CollectionUuid,
        min_versions_to_keep: usize,
    },
    DeleteCollection(CollectionUuid),
    Noop, // todo: why needed?
}

#[derive(Clone)]
struct CollectionVersionGraphNode {
    collection_id: CollectionUuid,
    version: u64,
    file_references: Vec<String>,
    is_deleted: bool,
    created_at_timestamp: u64,
}

impl std::fmt::Debug for CollectionVersionGraphNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CollectionVersionGraphNode")
            .field("collection_id", &self.collection_id)
            .field("version", &self.version)
            .field("file_references", &self.file_references.len())
            .field("is_deleted", &self.is_deleted)
            .field("created_at_timestamp", &self.created_at_timestamp)
            .finish()
    }
}

/// Tiny helper that yields monotonically-increasing UUIDs.
struct UuidGenerator(u128);
impl UuidGenerator {
    fn new() -> Self {
        Self(1)
    }

    fn next(&mut self) -> Uuid {
        let id = self.0;
        self.0 += 1;
        Uuid::from_u128(id)
    }
}

fn generate_file_paths(
    parent_file_paths: &[String],
    percent_files_to_inherit_from_parent: usize,
    num_files_to_create: usize,
) -> Vec<String> {
    let mut new_file_paths = vec![];
    let num_files_to_inherit_from_parent =
        (parent_file_paths.len() * percent_files_to_inherit_from_parent) / 100;

    // Inherit some files from the parent
    for i in 0..num_files_to_inherit_from_parent {
        new_file_paths.push(parent_file_paths[i % parent_file_paths.len()].clone());
    }

    // Create new files
    for i in 0..num_files_to_create {
        new_file_paths.push(format!("file_{}_{}", i, Uuid::new_v4()));
    }

    new_file_paths
}

#[derive(Debug, Clone)]
struct ReferenceState {
    current_timestamp: u64,
    deleted_collection_ids: HashSet<CollectionUuid>,
    version_graph: DiGraph<CollectionVersionGraphNode, ()>,
}

struct ReferenceGarbageCollector {}

impl ReferenceStateMachine for ReferenceGarbageCollector {
    type State = ReferenceState;
    type Transition = Transition;

    // todo: simplify graph generation
    fn init_state() -> proptest::prelude::BoxedStrategy<Self::State> {
        #[derive(Clone, Debug, PartialEq)]
        enum NodeKind {
            IncrementParentVersion,
            Fork,
        }
        #[derive(Clone, Debug)]
        struct Node {
            kind: NodeKind,
            versions: usize,
            percent_files_to_inherit_from_parent: usize,
            num_files_to_create: usize,
            created_seconds_after_parent: u64,
            children: Vec<Node>,
        }

        let node_kind_strategy = prop_oneof![
            2 => Just(NodeKind::IncrementParentVersion),
            1 => Just(NodeKind::Fork),
        ];

        let leaf_strategy = (
            1..4usize,
            0..=100usize,
            0..=10usize,
            0..=100u64,
            node_kind_strategy.clone(),
        )
            .prop_map(
                |(
                    versions,
                    percent_files_to_inherit_from_parent,
                    num_files_to_create,
                    created_seconds_after_parent,
                    node_kind,
                )| {
                    Node {
                        kind: node_kind,
                        versions,
                        percent_files_to_inherit_from_parent,
                        num_files_to_create,
                        created_seconds_after_parent,
                        children: vec![],
                    }
                },
            );

        let tree = leaf_strategy.prop_recursive(64, 128, 2, {
            let node_kind_strategy = node_kind_strategy.clone();

            move |element| {
                proptest::collection::vec(element.clone(), 0..4)
                    .prop_filter(
                        "only one parent version increment per sibling group",
                        |elements| {
                            let num_version_increment = elements
                                .iter()
                                .filter(|e| matches!(e.kind, NodeKind::IncrementParentVersion))
                                .count();
                            num_version_increment <= 1
                        },
                    )
                    .prop_flat_map({
                        let node_kind_strategy = node_kind_strategy.clone();

                        move |elements| {
                            (
                                1..4usize,
                                0..=100usize,
                                0..=10usize,
                                0..=100u64,
                                node_kind_strategy.clone(),
                            )
                                .prop_map(
                                    move |(
                                        versions,
                                        percent_files_to_inherit_from_parent,
                                        num_files_to_create,
                                        created_seconds_after_parent,
                                        node_kind,
                                    )| {
                                        Node {
                                            kind: node_kind,
                                            versions,
                                            percent_files_to_inherit_from_parent,
                                            num_files_to_create,
                                            created_seconds_after_parent,
                                            children: elements.clone(),
                                        }
                                    },
                                )
                        }
                    })
            }
        });

        tree.prop_map(|tree| {
            let mut uuid_gen = UuidGenerator::new();
            let mut graph: DiGraph<CollectionVersionGraphNode, ()> = DiGraph::new();
            let root = graph.add_node(CollectionVersionGraphNode {
                collection_id: CollectionUuid(uuid_gen.next()),
                version: 0,
                file_references: generate_file_paths(&[], 0, 4),
                is_deleted: false,
                created_at_timestamp: 1,
            });

            let mut stack = vec![(tree.clone(), root)];

            while let Some((node, parent_index)) = stack.pop() {
                let mut last_node_index = parent_index;
                for i in 0..node.versions {
                    let parent_node = &graph[last_node_index];

                    let new_node = if i == 0 && node.kind == NodeKind::Fork {
                        CollectionVersionGraphNode {
                            collection_id: CollectionUuid(uuid_gen.next()),
                            version: 0,
                            file_references: generate_file_paths(
                                &parent_node.file_references,
                                node.percent_files_to_inherit_from_parent,
                                node.num_files_to_create,
                            ),
                            is_deleted: false,
                            created_at_timestamp: parent_node.created_at_timestamp
                                + node.created_seconds_after_parent,
                        }
                    } else {
                        CollectionVersionGraphNode {
                            collection_id: parent_node.collection_id,
                            version: parent_node.version + 1,
                            file_references: generate_file_paths(
                                &parent_node.file_references,
                                node.percent_files_to_inherit_from_parent,
                                node.num_files_to_create,
                            ),
                            is_deleted: false,
                            created_at_timestamp: parent_node.created_at_timestamp
                                + node.created_seconds_after_parent,
                        }
                    };
                    let new_node = graph.add_node(new_node);
                    graph.add_edge(last_node_index, new_node, ());
                    last_node_index = new_node;
                }

                for child in node.children {
                    stack.push((child, last_node_index));
                }
            }

            // let final_graph = Dot::with_config(&graph, &[petgraph::dot::Config::EdgeNoLabel]);
            // println!("Final graph:\n {:?}", final_graph);

            ReferenceState {
                current_timestamp: 0,
                deleted_collection_ids: HashSet::new(),
                version_graph: graph,
            }
        })
        .boxed()
    }

    fn transitions(state: &Self::State) -> proptest::prelude::BoxedStrategy<Self::Transition> {
        let collection_ids = state
            .version_graph
            .node_indices()
            .map(|idx| state.version_graph[idx].collection_id)
            .collect::<HashSet<_>>();

        let alive_collection_ids = collection_ids
            .difference(&state.deleted_collection_ids)
            .cloned()
            .collect::<Vec<_>>();

        if alive_collection_ids.is_empty() {
            return Just(Transition::Noop).boxed();
        }

        prop_oneof![
            4 => (0..100u64, any::<proptest::sample::Index>()).prop_map({
              let alive_collection_ids = alive_collection_ids.clone();

              move |(tick_advance, collection_id_index)| Transition::GarbageCollect {
                tick_advance,
                min_versions_to_keep: 1,
                collection_id: alive_collection_ids[collection_id_index.index(alive_collection_ids.len())],
            }
          }),
            1 => any::<proptest::sample::Index>()
                .prop_map(move |collection_id_index| {
                  Transition::DeleteCollection(  alive_collection_ids[collection_id_index.index(alive_collection_ids.len())])
                }),
        ]
        .boxed()
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        println!("Applying transition: {:#?}", transition);
        match transition {
            Self::Transition::GarbageCollect {
                tick_advance,
                min_versions_to_keep,
                ..
            } => {
                state.current_timestamp += *tick_advance;

                for collection_id in ReferenceGarbageCollector::get_collection_ids(&state) {
                    let mut versions_for_collection = vec![];
                    for node in state.version_graph.node_indices() {
                        let node_data = &state.version_graph[node];
                        if node_data.collection_id == collection_id {
                            versions_for_collection.push(node_data);
                        }
                    }
                    versions_for_collection.sort_by_key(|n| n.version);

                    let versions_to_delete = versions_for_collection
                        .into_iter()
                        .rev()
                        .skip(*min_versions_to_keep)
                        // todo: use offset?
                        .filter(|v| {
                            v.version != 0 && v.created_at_timestamp < state.current_timestamp
                        })
                        .map(|v| v.version)
                        .collect::<HashSet<_>>();

                    // Mark nodes as deleted
                    for node in state.version_graph.node_indices() {
                        let node_data = &mut state.version_graph[node];
                        if versions_to_delete.contains(&node_data.version)
                            && node_data.collection_id == collection_id
                        {
                            node_data.is_deleted = true;
                        }
                    }
                }
            }
            Self::Transition::DeleteCollection(collection_id) => {
                state.deleted_collection_ids.insert(*collection_id);

                // Mark all nodes in graph for collection as deleted
                for node in state.version_graph.node_indices() {
                    let node_data = &mut state.version_graph[node];
                    if node_data.collection_id == *collection_id {
                        node_data.is_deleted = true;
                    }
                }
            }
            Self::Transition::Noop => {
                // No operation
                return state;
            }
        }

        let final_graph =
            Dot::with_config(&state.version_graph, &[petgraph::dot::Config::EdgeNoLabel]);
        println!("Graph after transition:\n {:?}", final_graph);

        state
    }
}

impl ReferenceGarbageCollector {
    fn get_collection_ids(state: &ReferenceState) -> HashSet<CollectionUuid> {
        let mut collection_ids = HashSet::new();
        // Iterate over the nodes in the graph
        for node in state.version_graph.node_indices() {
            let node_data = &state.version_graph[node];
            if !node_data.is_deleted {
                collection_ids.insert(node_data.collection_id);
            }
        }
        collection_ids
    }

    fn get_file_ref_counts(state: &ReferenceState) -> HashMap<String, usize> {
        let mut file_ref_counts: HashMap<String, usize> = HashMap::new();
        // Iterate over the nodes in the graph
        for node in state.version_graph.node_indices() {
            let node_data = &state.version_graph[node];
            // Iterate over the file references in the node
            for file_ref in &node_data.file_references {
                if node_data.is_deleted {
                    (*file_ref_counts.entry(file_ref.clone()).or_insert(0)).saturating_sub(1);
                // todo
                } else {
                    *file_ref_counts.entry(file_ref.clone()).or_insert(0) += 1;
                }
            }
        }
        file_ref_counts
    }

    fn check_invariants(state: &ReferenceState) {
        // If all collections are deleted, all file ref counts should be 0
        let all_collections_deleted = state
            .version_graph
            .node_indices()
            .all(|node| state.version_graph[node].is_deleted);
        if !all_collections_deleted {
            return;
        }

        let file_ref_counts = Self::get_file_ref_counts(state);
        for (file_ref, count) in file_ref_counts {
            if count != 0 {
                panic!(
                    "Invariant violation: file reference {} has a non-zero count {}",
                    file_ref, count
                );
            }
        }
    }
}

struct GarbageCollectorUnderTest {
    tenant: String,
    database: String,
    system: System,
    sysdb: SysDb,
    storage: Storage,
    root_manager: RootManager,
    dispatcher_handle: ComponentHandle<Dispatcher>,
}

impl StateMachineTest for GarbageCollectorUnderTest {
    type SystemUnderTest = Self;
    type Reference = ReferenceGarbageCollector;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        println!("Starting test-----------------------------------------------");
        let storage = test_storage();

        let mut sysdb = chroma_sysdb::SysDb::Test(TestSysDb::new());
        if let chroma_sysdb::SysDb::Test(test_sysdb) = &mut sysdb {
            test_sysdb.set_storage(Some(storage.clone()));
        }

        let root_manager = RootManager::new(storage.clone(), Box::new(NopCache));
        let system = System::new();
        let dispatcher = Dispatcher::new(DispatcherConfig::default());
        let dispatcher_handle = system.start_component(dispatcher);
        let tenant = "test_tenant".to_string();
        let database = "test_database".to_string();

        block_on(async {
            // Create collections
            let mut topo = Topo::new(&ref_state.version_graph);
            while let Some(node_index) = topo.next(&ref_state.version_graph) {
                let node_data = &ref_state.version_graph[node_index];
                if node_data.is_deleted {
                    continue;
                }

                if node_data.version == 0 {
                    if let Some(parent_index) = ref_state
                        .version_graph
                        .neighbors_directed(node_index, petgraph::Direction::Incoming)
                        .next()
                    {
                        let parent_node = &ref_state.version_graph[parent_index];
                        sysdb
                            .fork_collection(
                                parent_node.collection_id,
                                0,
                                0,
                                node_data.collection_id,
                                format!("Collection {}", node_data.collection_id),
                            )
                            .await;
                    } else {
                        // Root collection
                        sysdb
                            .create_collection(
                                tenant.clone(),
                                database.clone(),
                                node_data.collection_id,
                                format!("Collection {}", node_data.collection_id),
                                vec![], // todo
                                None,
                                None,
                                None,
                                false,
                            )
                            .await;
                    }
                }
            }

            Self {
                tenant,
                database,
                system,
                sysdb,
                storage,
                root_manager,
                dispatcher_handle,
            }
        })
    }

    fn apply(
        mut state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        match transition {
            Transition::GarbageCollect {
                collection_id,
                min_versions_to_keep,
                ..
            } => {
                let orchestrator = GarbageCollectorOrchestrator::new(
                    collection_id,
                    // todo
                    "".to_string(),
                    DateTime::from_timestamp_nanos(0),
                    state.sysdb.clone(),
                    state.dispatcher_handle.clone(),
                    state.system.clone(),
                    state.storage.clone(),
                    state.root_manager.clone(),
                    CleanupMode::Delete,
                );
            }
            Transition::DeleteCollection(collection_id) => {
                block_on(state.sysdb.delete_collection(
                    state.tenant.clone(),
                    state.database.clone(),
                    collection_id,
                    vec![], // todo
                ))
                .unwrap();
            }
            Transition::Noop => {
                // No operation
                return state;
            }
        }

        state
    }

    fn check_invariants(
        state: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        // Check invariants in the reference state
        ReferenceGarbageCollector::check_invariants(ref_state);
    }
}

#[cfg(test)]
mod tests {
    // todo
    use crate::*;
    use proptest_state_machine::prop_state_machine;
    use tracing_test::traced_test;

    prop_state_machine! {
        fn run_gc_test(
            sequential
            1..50
            =>
          GarbageCollectorUnderTest
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[traced_test]
    async fn gc_test_new() {
        // INVARIANT_CHECK_COUNT.store(0, Ordering::SeqCst);
        run_gc_test();
        // let checks = INVARIANT_CHECK_COUNT.load(Ordering::SeqCst);
        // assert!(
        //     checks > 0,
        //     "check_invariants was never called! Count: {}",
        //     checks
        // );
    }
}
