use chroma_types::CollectionUuid;
use chrono::DateTime;
use petgraph::{graph::DiGraph, prelude::DiGraphMap};

// GC will use it to rename a S3 file to a new name.
pub(crate) const RENAMED_FILE_PREFIX: &str = "gc/renamed/";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CleanupMode {
    /// Only list files that would be affected without making changes
    #[default]
    DryRun,
    /// Move files to a deletion directory instead of removing them
    Rename,
    /// Permanently delete files
    Delete,
    DryRunV2,
    DeleteV2,
}

impl CleanupMode {
    pub fn is_v2(&self) -> bool {
        matches!(self, CleanupMode::DryRunV2 | CleanupMode::DeleteV2)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum VersionStatus {
    #[allow(dead_code)]
    Alive {
        created_at: DateTime<chrono::Utc>,
    },
    Deleted,
}

#[derive(Debug, Clone)]
pub struct VersionGraphNode {
    pub collection_id: CollectionUuid,
    pub version: i64,
    #[allow(dead_code)]
    pub status: VersionStatus,
}

pub type VersionGraph = DiGraph<VersionGraphNode, ()>;

pub fn version_graph_to_collection_dependency_graph(
    graph: &VersionGraph,
) -> DiGraphMap<CollectionUuid, ()> {
    let mut collection_graph = DiGraphMap::new();

    for node in graph.node_indices() {
        let collection_id = graph[node].collection_id;
        collection_graph.add_node(collection_id);
    }

    for edge in graph.edge_indices() {
        let (source, target) = graph.edge_endpoints(edge).unwrap();
        let source_collection = graph[source].collection_id;
        let target_collection = graph[target].collection_id;

        // Don't add edges for versions within the same collection
        if source_collection == target_collection {
            continue;
        }

        collection_graph.add_edge(source_collection, target_collection, ());
    }

    collection_graph
}

#[derive(Debug, Default)]
pub struct GarbageCollectorResponse {
    pub collection_id: CollectionUuid,
    pub num_versions_deleted: u32,
    pub num_files_deleted: u32,
    #[deprecated = "only used by gc v1"]
    pub deletion_list: Vec<String>,
}
