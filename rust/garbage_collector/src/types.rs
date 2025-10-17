use chroma_blockstore::arrow::provider::BlockManager;
use chroma_types::CollectionUuid;
use chrono::DateTime;
use fst::{IntoStreamer, Streamer};
use petgraph::{graph::DiGraph, prelude::DiGraphMap};
use std::collections::HashSet;
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

pub struct FilePathRefCountMap {
    map: fst::Map<Vec<u8>>,
}

impl std::fmt::Debug for FilePathRefCountMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FilePathRefCountMap(len={})", self.map.len())
    }
}

impl FilePathRefCountMap {
    pub fn empty() -> Self {
        let builder = fst::MapBuilder::memory();
        let map = builder.into_map();
        FilePathRefCountMap { map }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn add_set(&mut self, other: FilePathSet, default_count: u64) -> Result<(), fst::Error> {
        let combined_map = {
            let mut new_map = fst::MapBuilder::memory();
            let mut stream = other.paths.stream();
            while let Some(path) = stream.next() {
                new_map.insert(path, default_count)?;
            }
            let new_map = new_map.into_map();

            let mut union = fst::map::OpBuilder::new();
            union = union.add(&self.map);
            union = union.add(&new_map);
            let mut union = union.union();

            let mut combined = fst::MapBuilder::memory();

            while let Some((key, indices)) = union.next() {
                let total_count = indices.iter().map(|idx| idx.value).sum();
                combined.insert(key, total_count)?;
            }

            combined.into_map()
        };

        self.map = combined_map;
        Ok(())
    }

    pub fn filter_by_count(&self, exact_count: u64) -> Result<FilePathSet, fst::Error> {
        let mut builder = fst::SetBuilder::memory();
        let mut stream = self.map.stream();
        while let Some((path, count)) = stream.next() {
            if count == exact_count {
                builder.insert(path)?;
            }
        }

        Ok(FilePathSet {
            paths: builder.into_set(),
        })
    }
}

#[derive(Clone)]
pub struct FilePathSet {
    paths: fst::Set<Vec<u8>>,
}

impl std::fmt::Debug for FilePathSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FilePathSet(len={})", self.paths.len())
    }
}

impl TryFrom<Vec<String>> for FilePathSet {
    type Error = fst::Error;

    fn try_from(mut paths: Vec<String>) -> Result<Self, Self::Error> {
        // Must insert into the fst in sorted order
        paths.sort_unstable();

        let paths = fst::Set::from_iter(paths)?;
        Ok(FilePathSet { paths })
    }
}

impl TryFrom<HashSet<String>> for FilePathSet {
    type Error = fst::Error;

    fn try_from(mut paths: HashSet<String>) -> Result<Self, Self::Error> {
        // Must insert into the fst in sorted order
        let mut paths: Vec<String> = paths.drain().collect();
        paths.sort_unstable();

        let paths = fst::Set::from_iter(paths)?;
        Ok(FilePathSet { paths })
    }
}

impl FilePathSet {
    pub fn empty() -> Self {
        let builder = fst::SetBuilder::memory();
        let paths = builder.into_set();
        FilePathSet { paths }
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    pub fn len(&self) -> usize {
        self.paths.len()
    }

    pub fn from_block_ids(mut block_ids: Vec<Uuid>, prefix: String) -> Result<Self, fst::Error> {
        // Must insert into the fst in sorted order
        block_ids.sort_unstable();

        let paths = fst::Set::from_iter(
            block_ids
                .into_iter()
                .map(|id| BlockManager::format_key(&prefix, &id)),
        )?;

        Ok(FilePathSet { paths })
    }

    pub fn concat(sets: &[FilePathSet]) -> Result<Self, fst::Error> {
        let mut union = fst::set::OpBuilder::new();
        for set in sets {
            union = union.add(&set.paths);
        }
        let union = union.union();
        let mut builder = fst::SetBuilder::memory();
        builder.extend_stream(union)?;
        Ok(FilePathSet {
            paths: builder.into_set(),
        })
    }

    // todo: used?
    pub fn into_refcount_map(self, initial_count: u64) -> Result<FilePathRefCountMap, fst::Error> {
        let mut builder = fst::MapBuilder::memory();
        let mut stream = self.paths.stream();
        while let Some(path) = stream.next() {
            builder.insert(path, initial_count)?;
        }

        Ok(FilePathRefCountMap {
            map: builder.into_map(),
        })
    }

    pub fn into_stream<'a>(&'a self) -> fst::set::Stream<'a> {
        self.paths.into_stream()
    }
}
