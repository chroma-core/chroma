use std::collections::{HashMap, HashSet};

use chroma_blockstore::arrow::provider::BlockManager;
use chroma_types::CollectionUuid;
use chrono::DateTime;
use petgraph::{graph::DiGraph, prelude::DiGraphMap};
use uuid::Uuid;

// GC will use it to rename a S3 file to a new name.
pub(crate) const RENAMED_FILE_PREFIX: &str = "gc/renamed/";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CleanupMode {
    /// Move files to a deletion directory instead of removing them
    Rename, // todo: remove:?
    /// Only list files that would be affected without making changes
    #[default]
    DryRunV2,
    /// Permanently delete files
    DeleteV2,
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

#[derive(Debug, Clone)]
pub struct FilePathSet {
    // prefix -> set of block UUIDs
    blocks: HashMap<String, HashSet<Uuid>>,
    // any non-block path, e.g. sparse index files, HNSW files, etc.
    other: HashSet<String>,
}

impl Default for FilePathSet {
    fn default() -> Self {
        Self::new()
    }
}

impl FilePathSet {
    pub fn new() -> Self {
        Self {
            blocks: HashMap::new(),
            other: HashSet::new(),
        }
    }

    pub fn insert_block(&mut self, prefix: &str, block_id: Uuid) {
        self.blocks
            .entry(prefix.to_string())
            .or_default()
            .insert(block_id);
    }

    pub fn insert_path(&mut self, path: String) {
        self.other.insert(path);
    }

    pub fn len(&self) -> usize {
        let block_count: usize = self.blocks.values().map(|set| set.len()).sum();
        block_count + self.other.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty() && self.other.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = String> + '_ {
        let block_paths = self.blocks.iter().flat_map(move |(prefix, block_ids)| {
            block_ids
                .iter()
                .map(move |block_id| BlockManager::format_key(prefix, block_id))
        });

        let other_paths = self.other.iter().cloned();

        block_paths.chain(other_paths)
    }
}

#[derive(Debug)]
pub struct FilePathRefCountSet {
    // prefix -> block UUID -> reference count
    blocks: HashMap<String, HashMap<Uuid, usize>>,
    other: HashMap<String, usize>,
}

impl Default for FilePathRefCountSet {
    fn default() -> Self {
        Self::new()
    }
}

impl FilePathRefCountSet {
    pub fn new() -> Self {
        Self {
            blocks: HashMap::new(),
            other: HashMap::new(),
        }
    }

    pub fn from_set(set: FilePathSet, init_count: usize) -> Self {
        let mut ref_count_set = Self::new();

        for (prefix, block_ids) in set.blocks {
            let entry = ref_count_set.blocks.entry(prefix).or_default();
            for block_id in block_ids {
                entry.insert(block_id, init_count);
            }
        }

        for path in set.other {
            ref_count_set.other.insert(path, init_count);
        }

        ref_count_set
    }

    pub fn merge(&mut self, other: Self) {
        for (prefix, block_id_counts) in other.blocks {
            let entry = self.blocks.entry(prefix).or_default();
            for (block_id, count) in block_id_counts {
                *entry.entry(block_id).or_insert(0) += count;
            }
        }

        for (path, count) in other.other {
            *self.other.entry(path).or_insert(0) += count;
        }
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty() && self.other.is_empty()
    }

    pub fn len(&self) -> usize {
        let block_count: usize = self.blocks.values().map(|map| map.len()).sum();
        block_count + self.other.len()
    }

    pub fn as_set(&self, max_ref_count: usize) -> FilePathSet {
        let mut set = FilePathSet::new();

        for (prefix, block_counts) in &self.blocks {
            for (block_id, count) in block_counts {
                if *count <= max_ref_count {
                    set.insert_block(prefix, *block_id);
                }
            }
        }

        for (path, count) in &self.other {
            if *count <= max_ref_count {
                set.insert_path(path.clone());
            }
        }

        set
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_path_set_basics() {
        let mut set = FilePathSet::new();
        let block_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        set.insert_block("prefix1", block_id);
        set.insert_path("some/other/path".to_string());

        assert_eq!(set.blocks.len(), 1);
        assert_eq!(set.blocks.get("prefix1").unwrap().len(), 1);
        assert!(set.blocks.get("prefix1").unwrap().contains(&block_id));
        assert_eq!(set.other.len(), 1);
        assert!(set.other.contains("some/other/path"));
    }

    #[test]
    fn test_file_path_set_multiple_blocks_same_prefix() {
        let mut set = FilePathSet::new();
        let block_id1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let block_id2 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();

        set.insert_block("prefix1", block_id1);
        set.insert_block("prefix1", block_id2);

        assert_eq!(set.blocks.len(), 1);
        assert_eq!(set.blocks.get("prefix1").unwrap().len(), 2);
        assert!(set.blocks.get("prefix1").unwrap().contains(&block_id1));
        assert!(set.blocks.get("prefix1").unwrap().contains(&block_id2));
    }

    #[test]
    fn test_file_path_set_multiple_prefixes() {
        let mut set = FilePathSet::new();
        let block_id1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let block_id2 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();

        set.insert_block("prefix1", block_id1);
        set.insert_block("prefix2", block_id2);

        assert_eq!(set.blocks.len(), 2);
        assert!(set.blocks.get("prefix1").unwrap().contains(&block_id1));
        assert!(set.blocks.get("prefix2").unwrap().contains(&block_id2));
    }

    #[test]
    fn test_from_set() {
        let mut set = FilePathSet::new();
        let block_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();

        set.insert_block("prefix1", block_id);
        set.insert_path("some/path".to_string());

        let ref_count_set = FilePathRefCountSet::from_set(set, 5);

        assert_eq!(
            ref_count_set.blocks.get("prefix1").unwrap().get(&block_id),
            Some(&5)
        );
        assert_eq!(ref_count_set.other.get("some/path"), Some(&5));
    }

    #[test]
    fn test_merge_disjoint() {
        let mut set1 = FilePathSet::new();
        let block_id1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        set1.insert_block("prefix1", block_id1);

        let mut set2 = FilePathSet::new();
        let block_id2 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();
        set2.insert_block("prefix2", block_id2);

        let mut ref_count1 = FilePathRefCountSet::from_set(set1, 2);
        let ref_count2 = FilePathRefCountSet::from_set(set2, 3);

        ref_count1.merge(ref_count2);

        assert_eq!(
            ref_count1.blocks.get("prefix1").unwrap().get(&block_id1),
            Some(&2)
        );
        assert_eq!(
            ref_count1.blocks.get("prefix2").unwrap().get(&block_id2),
            Some(&3)
        );
    }

    #[test]
    fn test_merge_overlapping() {
        let mut set1 = FilePathSet::new();
        let block_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        set1.insert_block("prefix1", block_id);
        set1.insert_path("shared/path".to_string());

        let mut set2 = FilePathSet::new();
        set2.insert_block("prefix1", block_id);
        set2.insert_path("shared/path".to_string());

        let mut ref_count1 = FilePathRefCountSet::from_set(set1, 2);
        let ref_count2 = FilePathRefCountSet::from_set(set2, 3);

        ref_count1.merge(ref_count2);

        assert_eq!(
            ref_count1.blocks.get("prefix1").unwrap().get(&block_id),
            Some(&5)
        );
        assert_eq!(ref_count1.other.get("shared/path"), Some(&5));
    }

    #[test]
    fn test_as_set() {
        let mut ref_count_set = FilePathRefCountSet::new();
        let block_id1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let block_id2 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();

        ref_count_set
            .blocks
            .entry("prefix1".to_string())
            .or_default()
            .insert(block_id1, 2);
        ref_count_set
            .blocks
            .entry("prefix1".to_string())
            .or_default()
            .insert(block_id2, 5);
        ref_count_set.other.insert("path/one".to_string(), 1);
        ref_count_set.other.insert("path/two".to_string(), 4);

        let filtered_set = ref_count_set.as_set(3);
        let filtered_paths = filtered_set.iter().collect::<HashSet<_>>();

        assert_eq!(filtered_paths.len(), 2);
        assert_eq!(
            filtered_paths,
            HashSet::from([
                BlockManager::format_key("prefix1", &block_id1),
                "path/one".to_string()
            ])
        );
    }
}
