/**
 * Contains the reference implementation of the garbage collector state machine.
 *
 * The reference implementation keeps track of all files created and a graph of the fork tree.
 * The main invariant that this provides hooks for is that the set of files on disk match the expected set of files (accounting for files that should have been pruned by the garbage collector).
 *
 * Generated transitions will:
 *  - extend a single fork tree
 *  - run garbage collection on the tree
 *  - delete collections
 */
use super::proptest_types::Transition;
use super::segment_file_strategies::SegmentGroup;
use chroma_types::{CollectionUuid, DatabaseUuid};
use petgraph::graph::{DiGraph, NodeIndex};
use proptest::prelude::{any, any_with, BoxedStrategy};
use proptest::strategy::Strategy;
use proptest::{prelude::Just, prop_oneof};
use proptest_state_machine::ReferenceStateMachine;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CollectionStatus {
    Alive,
    SoftDeleted,
    Deleted,
}

#[derive(Clone)]
struct CollectionVersionGraphNode {
    collection_id: CollectionUuid,
    version: u64,
    segments: SegmentGroup,
    is_deleted: bool,
}

impl std::fmt::Debug for CollectionVersionGraphNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CollectionVersionGraphNode")
            .field("collection_id", &self.collection_id)
            .field("version", &self.version)
            .field("is_deleted", &self.is_deleted)
            .finish()
    }
}

impl CollectionVersionGraphNode {
    fn next_version_strategy(&self) -> BoxedStrategy<Self> {
        let next_hnsw_segment = self.segments.vector.next_version_strategy();
        let next_metadata_segment = self.segments.metadata.next_version_strategy();
        let next_record_segment = self.segments.record.next_version_strategy();

        (
            Just(self.clone()),
            next_hnsw_segment,
            next_metadata_segment,
            next_record_segment,
        )
            .prop_map(|(mut new_node, hnsw, metadata, record)| {
                new_node.segments.vector = hnsw;
                new_node.segments.metadata = metadata;
                new_node.segments.record = record;
                new_node.version += 1;

                new_node
            })
            .boxed()
    }
}

#[derive(Debug, Clone)]
pub struct ReferenceState {
    pub runtime: Arc<tokio::runtime::Runtime>,
    pub collection_status: HashMap<CollectionUuid, CollectionStatus>,
    pub tenant: String,
    pub db_name: String,
    pub db_id: DatabaseUuid,
    version_graph: DiGraph<CollectionVersionGraphNode, ()>,
    root_collection_id: Option<CollectionUuid>,
    latest_node_by_collection: HashMap<CollectionUuid, NodeIndex>,
    versions_by_collection: HashMap<CollectionUuid, Vec<NodeIndex>>,
    alive_collection_ids: HashSet<CollectionUuid>,
    node_depth: HashMap<NodeIndex, usize>,
    graph_depth: usize,
}

impl ReferenceState {
    pub fn get_graphviz_of_graph(&self) -> String {
        let final_graph = petgraph::dot::Dot::with_config(
            &self.version_graph,
            &[petgraph::dot::Config::EdgeNoLabel],
        );
        format!("{:?}", final_graph)
    }

    pub fn get_file_ref_counts(&self) -> HashMap<String, HashSet<(CollectionUuid, u64)>> {
        let mut file_ref_counts: HashMap<String, HashSet<(CollectionUuid, u64)>> = HashMap::new();
        // Iterate over the nodes in the graph
        for node in self.version_graph.node_indices() {
            let node_data = &self.version_graph[node];
            // Iterate over the file references in the node
            for file_ref in &node_data.segments.get_all_file_paths() {
                let entry = file_ref_counts.entry(file_ref.clone()).or_default();
                if !node_data.is_deleted
                    && self.collection_status[&node_data.collection_id] != CollectionStatus::Deleted
                {
                    entry.insert((node_data.collection_id, node_data.version));
                }
            }
        }
        file_ref_counts
    }

    pub fn check_invariants(&self) {
        // If all collections are deleted, all file ref counts should be 0
        let all_collections_deleted = self
            .collection_status
            .values()
            .all(|status| *status == CollectionStatus::Deleted);
        if !all_collections_deleted {
            return;
        }

        let file_ref_counts = self.get_file_ref_counts();
        for (file_path, refs) in file_ref_counts {
            if !refs.is_empty() {
                panic!(
                    "Invariant violation: file reference {} has a non-zero count {}",
                    file_path,
                    refs.len()
                );
            }
        }
    }

    pub fn max_version_for_collection(&self, collection_id: CollectionUuid) -> Option<u64> {
        self.latest_live_node_for_collection(collection_id)
            .map(|node| node.version)
    }

    pub fn expected_versions_by_collection(&self) -> HashMap<CollectionUuid, Vec<u64>> {
        let mut expected_alive_collection_versions = HashMap::new();

        for collection_id in self.alive_collection_ids.iter() {
            if let Some(version_nodes) = self.versions_by_collection.get(collection_id) {
                let versions = expected_alive_collection_versions
                    .entry(*collection_id)
                    .or_insert_with(Vec::new);
                versions.extend(version_nodes.iter().filter_map(|node_index| {
                    let node = &self.version_graph[*node_index];
                    if node.is_deleted {
                        None
                    } else {
                        Some(node.version)
                    }
                }));
            }
        }

        expected_alive_collection_versions
    }

    pub fn get_graph_depth(&self) -> usize {
        self.graph_depth
    }

    fn get_collection_ids(&self) -> HashSet<CollectionUuid> {
        self.alive_collection_ids.clone()
    }

    fn insert_version_node(
        &mut self,
        collection_id: CollectionUuid,
        node_index: NodeIndex,
        depth: usize,
    ) {
        self.latest_node_by_collection
            .insert(collection_id, node_index);
        self.versions_by_collection
            .entry(collection_id)
            .or_default()
            .push(node_index);
        self.node_depth.insert(node_index, depth);
        self.graph_depth = self.graph_depth.max(depth);
    }

    fn latest_live_node_index_for_collection(
        &self,
        collection_id: CollectionUuid,
    ) -> Option<NodeIndex> {
        if let Some(node_index) = self.latest_node_by_collection.get(&collection_id) {
            if !self.version_graph[*node_index].is_deleted {
                return Some(*node_index);
            }
        }

        self.versions_by_collection
            .get(&collection_id)?
            .iter()
            .rev()
            .copied()
            .find(|node_index| !self.version_graph[*node_index].is_deleted)
    }

    fn latest_live_node_for_collection(
        &self,
        collection_id: CollectionUuid,
    ) -> Option<&CollectionVersionGraphNode> {
        self.latest_live_node_index_for_collection(collection_id)
            .map(|node_index| &self.version_graph[node_index])
    }
}

static RUNTIME_ONCE: OnceLock<Arc<tokio::runtime::Runtime>> = OnceLock::new();

pub struct ReferenceGarbageCollector {}

impl ReferenceStateMachine for ReferenceGarbageCollector {
    type State = ReferenceState;
    type Transition = Transition;

    fn init_state() -> proptest::prelude::BoxedStrategy<Self::State> {
        let runtime = RUNTIME_ONCE
            .get_or_init(|| Arc::new(tokio::runtime::Runtime::new().unwrap()))
            .clone();

        let tenant_id = Uuid::new_v4();
        let tenant_name = format!("test_tenant_{}", tenant_id);
        let database_id = Uuid::new_v4();
        let database_name = format!("test_database_{}", database_id);

        Just(ReferenceState {
            runtime,
            version_graph: DiGraph::new(),
            tenant: tenant_name,
            db_name: database_name,
            db_id: DatabaseUuid(database_id),
            collection_status: HashMap::new(),
            root_collection_id: None,
            latest_node_by_collection: HashMap::new(),
            versions_by_collection: HashMap::new(),
            alive_collection_ids: HashSet::new(),
            node_depth: HashMap::new(),
            graph_depth: 0,
        })
        .boxed()
    }

    fn transitions(state: &Self::State) -> proptest::prelude::BoxedStrategy<Self::Transition> {
        let alive_collection_ids = state
            .alive_collection_ids
            .iter()
            .copied()
            .collect::<Vec<_>>();

        let alive_collection_id_strategy = any::<proptest::sample::Index>().prop_map({
            let alive_collection_ids = alive_collection_ids.clone();
            move |collection_id_index| {
                alive_collection_ids[collection_id_index.index(alive_collection_ids.len())]
            }
        });

        let create_collection_id = CollectionUuid::new();
        let create_collection_transition =
            any_with::<SegmentGroup>((state.tenant.clone(), state.db_id, create_collection_id))
                .prop_map(move |segment_group| Transition::CreateCollection {
                    collection_id: create_collection_id,
                    segments: segment_group,
                });

        let _delete_collection_transition = alive_collection_id_strategy
            .clone()
            .prop_map(Transition::DeleteCollection);

        let fork_collection_transition =
            alive_collection_id_strategy
                .clone()
                .prop_map(move |source_collection_id| Transition::ForkCollection {
                    source_collection_id,
                    new_collection_id: CollectionUuid::new(),
                });

        let increment_collection_version_transition = alive_collection_id_strategy
            .clone()
            .prop_flat_map({
                let latest_live_nodes = alive_collection_ids
                    .iter()
                    .filter_map(|collection_id| {
                        state
                            .latest_live_node_for_collection(*collection_id)
                            .map(|node| (*collection_id, node.clone()))
                    })
                    .collect::<HashMap<_, _>>();

                move |collection_id| {
                    let parent = latest_live_nodes.get(&collection_id).cloned().unwrap();

                    parent.next_version_strategy()
                }
            })
            .prop_map({
                move |next_version| Transition::IncrementCollectionVersion {
                    collection_id: next_version.collection_id,
                    next_segments: next_version.segments.clone(),
                }
            });

        let delete_collection_transition =
            alive_collection_id_strategy.prop_map(Transition::DeleteCollection);

        if alive_collection_ids.is_empty() {
            if state.root_collection_id.is_some() {
                // If all collections are deleted, we cannot create a new collection, so there is nothing further to do
                return Just(Transition::NoOp).boxed();
            }

            return prop_oneof![create_collection_transition,].boxed();
        }

        // While the garbage collector can technically run on any collection in a fork tree, we always run it on the root collection as the test fixture will call `ListCollectionsToGc()` which only returns the root collection.
        if let Some(root_collection_id) = state.root_collection_id {
            let garbage_collect_transition =
                (1..=2usize).prop_map(move |min_versions_to_keep| Transition::GarbageCollect {
                    collection_id: root_collection_id,
                    min_versions_to_keep,
                });

            return prop_oneof![
                2 => fork_collection_transition,
                3 => increment_collection_version_transition,
                2 => garbage_collect_transition,
                1 => delete_collection_transition,
            ]
            .boxed();
        }

        create_collection_transition.boxed()
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        match transition {
            Transition::CreateCollection { .. } => true,
            Transition::IncrementCollectionVersion {
                collection_id,
                next_segments,
            } => {
                // Check if the collection exists and if the new version has at least 1 file path
                state.collection_status.get(collection_id) == Some(&CollectionStatus::Alive)
                    && !next_segments.get_all_file_paths().is_empty()
            }
            Transition::ForkCollection {
                source_collection_id,
                ..
            } => {
                state.collection_status.get(source_collection_id) == Some(&CollectionStatus::Alive)
            }
            Transition::DeleteCollection(collection_id) => {
                state.collection_status.get(collection_id) == Some(&CollectionStatus::Alive)
            }
            Transition::GarbageCollect { collection_id, .. } => {
                matches!(
                    state.collection_status.get(collection_id),
                    Some(CollectionStatus::Alive) | Some(CollectionStatus::SoftDeleted)
                )
            }
            Transition::NoOp => true,
        }
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            Self::Transition::CreateCollection {
                collection_id,
                segments,
                ..
            } => {
                let new_node = CollectionVersionGraphNode {
                    collection_id: *collection_id,
                    version: 0,
                    segments: segments.clone(),
                    is_deleted: false,
                };
                let node_index = state.version_graph.add_node(new_node);
                state.insert_version_node(*collection_id, node_index, 0);
                state.root_collection_id = Some(*collection_id);
                state
                    .collection_status
                    .insert(*collection_id, CollectionStatus::Alive);
                state.alive_collection_ids.insert(*collection_id);
            }
            Self::Transition::IncrementCollectionVersion {
                collection_id,
                next_segments,
            } => {
                let parent_node_index = state
                    .latest_live_node_index_for_collection(*collection_id)
                    .unwrap();

                let parent_node = &state.version_graph[parent_node_index];
                let parent_version = parent_node.version;
                let new_node = CollectionVersionGraphNode {
                    collection_id: *collection_id,
                    version: parent_version + 1,
                    segments: next_segments.clone(),
                    is_deleted: false,
                };
                let new_node_index = state.version_graph.add_node(new_node);
                state
                    .version_graph
                    .add_edge(parent_node_index, new_node_index, ());
                let parent_depth = state.node_depth[&parent_node_index];
                state.insert_version_node(*collection_id, new_node_index, parent_depth + 1);
            }
            Self::Transition::ForkCollection {
                source_collection_id,
                new_collection_id,
            } => {
                let parent_node_index = state
                    .latest_live_node_index_for_collection(*source_collection_id)
                    .unwrap();
                let parent_node = &state.version_graph[parent_node_index];

                let new_node = CollectionVersionGraphNode {
                    collection_id: *new_collection_id,
                    version: 0,
                    segments: parent_node.segments.clone(),
                    is_deleted: false,
                };
                let new_node_index = state.version_graph.add_node(new_node);
                state
                    .version_graph
                    .add_edge(parent_node_index, new_node_index, ());
                let parent_depth = state.node_depth[&parent_node_index];
                state.insert_version_node(*new_collection_id, new_node_index, parent_depth + 1);
                state
                    .collection_status
                    .insert(*new_collection_id, CollectionStatus::Alive);
                state.alive_collection_ids.insert(*new_collection_id);
            }
            Self::Transition::GarbageCollect {
                min_versions_to_keep,
                ..
            } => {
                // Transition soft deleted collections to deleted
                let mut collection_ids_to_delete = HashSet::new();
                for (collection_id, status) in state.collection_status.iter() {
                    if *status == CollectionStatus::SoftDeleted {
                        let first_collection_node = state
                            .versions_by_collection
                            .get(collection_id)
                            .and_then(|versions| versions.first().copied())
                            .expect("collection should have at least one version node");

                        let mut dfs =
                            petgraph::visit::Dfs::new(&state.version_graph, first_collection_node);
                        let mut seen_collection_ids: HashSet<CollectionUuid> = HashSet::new();

                        while let Some(nx) = dfs.next(&state.version_graph) {
                            let node = state
                                .version_graph
                                .node_weight(nx)
                                .expect("Node should exist");
                            seen_collection_ids.insert(node.collection_id);
                        }

                        let are_all_children_in_fork_tree_also_dead =
                            seen_collection_ids.iter().all(|collection_id| {
                                state.collection_status.get(collection_id)
                                    != Some(&CollectionStatus::Alive)
                            });

                        if are_all_children_in_fork_tree_also_dead {
                            // Can now transition to hard deleted state
                            collection_ids_to_delete.insert(*collection_id);
                        }
                    }
                }
                for collection_id in collection_ids_to_delete {
                    state
                        .collection_status
                        .insert(collection_id, CollectionStatus::Deleted);
                }

                // Mark all versions of soft deleted collections as deleted
                let soft_deleted_collections = state
                    .collection_status
                    .iter()
                    .filter_map(|(collection_id, status)| {
                        if *status == CollectionStatus::SoftDeleted {
                            Some(*collection_id)
                        } else {
                            None
                        }
                    })
                    .collect::<HashSet<_>>();
                for node in state.version_graph.node_weights_mut() {
                    if soft_deleted_collections.contains(&node.collection_id) {
                        node.is_deleted = true;
                    }
                }

                for collection_id in state.get_collection_ids() {
                    let versions_to_delete = state.versions_by_collection[&collection_id]
                        .iter()
                        .map(|node_index| &state.version_graph[*node_index])
                        .rev()
                        .skip(*min_versions_to_keep)
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
                state
                    .collection_status
                    .insert(*collection_id, CollectionStatus::SoftDeleted);
                state.alive_collection_ids.remove(collection_id);
            }
            Self::Transition::NoOp => {}
        }

        state
    }
}
