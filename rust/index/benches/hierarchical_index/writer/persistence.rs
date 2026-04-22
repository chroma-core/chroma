#![allow(dead_code)]

use std::cell::RefCell;
use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tokio::runtime::{Builder, Handle, Runtime};

use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider, BlockfileFlusher,
    BlockfileWriterOptions,
};
use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;
use chroma_index::quantization::Code;
use chroma_types::QuantizedCluster;
use dashmap::{DashMap, DashSet};
use parking_lot::ReentrantMutex;
use uuid::Uuid;

use super::{
    HierarchicalSpannConfig, HierarchicalSpannWriter, InternalNode, LeafNode, NodeId, TreeNode,
    WriterStats,
};

thread_local! {
    /// Used from sync writer paths on threads with no Tokio runtime (Rayon / scoped workers).
    static BLOCKFILE_SYNC_RT: RefCell<Option<Runtime>> = const { RefCell::new(None) };
}

/// Run a short async blockfile read from sync code (`add` / `balance` may run off the main runtime).
fn block_on_for_sync_writer<F>(future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    match Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => BLOCKFILE_SYNC_RT.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                *slot = Some(
                    Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("tokio runtime for blockfile sync paths"),
                );
            }
            slot.as_ref().expect("initialized").block_on(future)
        }),
    }
}

// Blockfile prefix constants
pub const PREFIX_ROOT: &str = "root";
pub const PREFIX_NEXT_NODE: &str = "next_node";
pub const PREFIX_DIM: &str = "dim";
pub const PREFIX_NODE_TYPE: &str = "node_type";
pub const PREFIX_PARENT: &str = "parent";
pub const PREFIX_LENGTH: &str = "length";
pub const PREFIX_VERSION: &str = "version";
pub const PREFIX_CENTROID: &str = "centroid";
pub const PREFIX_EMBEDDING: &str = "embedding";
pub const PREFIX_CHILDREN: &str = "children";
pub const PREFIX_CENTROID_CODE: &str = "centroid_code";

pub const SINGLETON_KEY: u32 = 0;
pub const NODE_TYPE_LEAF: u32 = 0;
pub const NODE_TYPE_INTERNAL: u32 = 1;
pub const NO_PARENT: u32 = u32::MAX;

pub fn pack_bytes_to_u32s(bytes: &[u8]) -> Vec<u32> {
    bytes
        .chunks(4)
        .map(|chunk| {
            let mut buf = [0u8; 4];
            buf[..chunk.len()].copy_from_slice(chunk);
            u32::from_le_bytes(buf)
        })
        .collect()
}

pub fn unpack_u32s_to_bytes(packed: &[u32], byte_len: usize) -> Vec<u8> {
    let mut bytes: Vec<u8> = packed.iter().flat_map(|w| w.to_le_bytes()).collect();
    bytes.truncate(byte_len);
    bytes
}

#[derive(Clone, Debug)]
pub struct HierarchicalSpannIds {
    pub posting_list_id: Uuid,
    pub scalar_metadata_id: Uuid,
    pub vector_data_id: Uuid,
    pub list_data_id: Uuid,
}

pub struct HierarchicalSpannFlusher {
    posting_list_flusher: BlockfileFlusher,
    scalar_metadata_flusher: BlockfileFlusher,
    vector_data_flusher: BlockfileFlusher,
    list_data_flusher: BlockfileFlusher,
}

impl HierarchicalSpannFlusher {
    pub async fn flush(self) -> Result<HierarchicalSpannIds, Box<dyn ChromaError>> {
        let posting_list_id = self.posting_list_flusher.id();
        let scalar_metadata_id = self.scalar_metadata_flusher.id();
        let vector_data_id = self.vector_data_flusher.id();
        let list_data_id = self.list_data_flusher.id();

        self.posting_list_flusher
            .flush::<u32, QuantizedCluster<'_>>()
            .await?;
        self.scalar_metadata_flusher.flush::<u32, u32>().await?;
        self.vector_data_flusher.flush::<u32, Vec<f32>>().await?;
        self.list_data_flusher.flush::<u32, Vec<u32>>().await?;

        Ok(HierarchicalSpannIds {
            posting_list_id,
            scalar_metadata_id,
            vector_data_id,
            list_data_id,
        })
    }
}

impl HierarchicalSpannWriter {
    /// Commit all in-memory state to blockfiles and return a flusher.
    ///
    /// When `fork_from` is `Some`, each blockfile writer is forked from the
    /// previous committed blockfile.  Untouched keys (e.g. postings of leaves
    /// that were never materialized in memory since the last `open()`) are
    /// inherited verbatim from the parent; only mutations made since the last
    /// commit are applied.  This is required for correctness after a lazy
    /// resume — otherwise unloaded leaves would be overwritten with empty
    /// clusters.
    ///
    /// When `fork_from` is `None`, fresh blockfiles are created (the first
    /// commit of a new run).
    ///
    /// After a successful commit, tombstones are cleared.
    pub async fn commit(
        &self,
        blockfile_provider: &BlockfileProvider,
        fork_from: Option<&HierarchicalSpannIds>,
    ) -> Result<HierarchicalSpannFlusher, Box<dyn ChromaError>> {
        let mut pl_options = BlockfileWriterOptions::new("".to_string()).ordered_mutations();
        let mut sm_options = BlockfileWriterOptions::new("".to_string()).ordered_mutations();
        let mut vd_options = BlockfileWriterOptions::new("".to_string()).ordered_mutations();
        let mut ld_options = BlockfileWriterOptions::new("".to_string()).ordered_mutations();
        if let Some(ids) = fork_from {
            pl_options = pl_options.fork(ids.posting_list_id);
            sm_options = sm_options.fork(ids.scalar_metadata_id);
            vd_options = vd_options.fork(ids.vector_data_id);
            ld_options = ld_options.fork(ids.list_data_id);
        }

        let posting_list_writer = blockfile_provider
            .write::<u32, QuantizedCluster<'_>>(pl_options)
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;
        let scalar_metadata_writer = blockfile_provider
            .write::<u32, u32>(sm_options)
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;
        let vector_data_writer = blockfile_provider
            .write::<u32, Vec<f32>>(vd_options)
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;
        let list_data_writer = blockfile_provider
            .write::<u32, Vec<u32>>(ld_options)
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        // Build the sorted merged id list: ids in self.nodes (live) + ids in
        // self.tombstones (removed since last commit). Deduplicated with live
        // preference (a node should never be in both by construction, but be
        // defensive). Deletes only emitted when fork_from is Some.
        let live_ids: std::collections::BTreeSet<NodeId> =
            self.nodes.iter().map(|e| *e.key()).collect();
        let tomb_ids: std::collections::BTreeSet<NodeId> = if fork_from.is_some() {
            self.tombstones
                .iter()
                .map(|e| *e)
                .filter(|id| !live_ids.contains(id))
                .collect()
        } else {
            std::collections::BTreeSet::new()
        };

        // Dirty-aware iteration: only re-write per-node metadata for nodes
        // mutated since the last commit. Lazy shells inherited from the
        // forked parent are skipped, which makes the per-checkpoint commit
        // memory cost proportional to the *mutation rate* rather than to the
        // total tree size. See `docs/README.md` -> "Commit-time memory".
        //
        // For the very first commit (no parent fork), dirty_nodes already
        // contains every node touched since `new()` constructed an empty
        // writer (root marked dirty in `new()`, every `nodes.insert` site
        // calls `mark_node_dirty`), so the dirty filter is equivalent to
        // "all live nodes" in that case.
        let dirty_live_ids: std::collections::BTreeSet<NodeId> = self
            .dirty_nodes
            .iter()
            .map(|e| *e)
            .filter(|id| live_ids.contains(id))
            .collect();
        // Sorted union of (dirty live) ∪ tombstones. These are the only ids
        // requiring blockfile writes.
        let changed_ids: std::collections::BTreeSet<NodeId> =
            dirty_live_ids.union(&tomb_ids).copied().collect();

        // Helper enum for interleaving set/delete per prefix.
        enum Action {
            Set,
            Delete,
        }
        let action_for = |id: &NodeId| -> Action {
            if live_ids.contains(id) {
                Action::Set
            } else {
                Action::Delete
            }
        };

        // All writes within each blockfile writer MUST be in lexicographic
        // (prefix, key) order. Prefix order for each writer:
        //   scalar_metadata_writer: dim < length < next_node < node_type < parent < root < version
        //   vector_data_writer:     centroid < embedding
        //   list_data_writer:       centroid_code < children
        //   posting_list_writer:    "" (empty)

        // =========================================================
        // scalar_metadata_writer
        // =========================================================

        // -- "dim" (singleton) --
        scalar_metadata_writer
            .set(PREFIX_DIM, SINGLETON_KEY, self.dim as u32)
            .await?;

        // -- "length" (leaf nodes only; delete tombstones unconditionally) --
        for &id in &changed_ids {
            match action_for(&id) {
                Action::Set => {
                    let node_ref = self.nodes.get(&id);
                    if let Some(n) = node_ref {
                        if let TreeNode::Leaf(leaf) = n.value() {
                            // For a materialized leaf use the live count (register_in_leaf
                            // and scrub mutate ids without touching `length`). For a lazy
                            // shell (ids empty but length>0) keep the persisted length so
                            // the inherited posting list is still discoverable on reopen.
                            let len = if leaf.ids.is_empty() {
                                leaf.length
                            } else {
                                leaf.ids.len()
                            };
                            scalar_metadata_writer
                                .set(PREFIX_LENGTH, id, len as u32)
                                .await?;
                        }
                    }
                }
                Action::Delete => {
                    scalar_metadata_writer
                        .delete::<_, u32>(PREFIX_LENGTH, id)
                        .await?;
                }
            }
        }

        // -- "next_node" (singleton) --
        scalar_metadata_writer
            .set(
                PREFIX_NEXT_NODE,
                SINGLETON_KEY,
                self.next_node_id.load(Ordering::Relaxed),
            )
            .await?;

        // -- "node_type" (all nodes; critical: phantom nodes would resurface
        //    at open() if stale node_type entries remain after tombstoning) --
        for &id in &changed_ids {
            match action_for(&id) {
                Action::Set => {
                    if let Some(node_ref) = self.nodes.get(&id) {
                        let type_val = match node_ref.value() {
                            TreeNode::Leaf(_) => NODE_TYPE_LEAF,
                            TreeNode::Internal(_) => NODE_TYPE_INTERNAL,
                        };
                        scalar_metadata_writer
                            .set(PREFIX_NODE_TYPE, id, type_val)
                            .await?;
                    }
                }
                Action::Delete => {
                    scalar_metadata_writer
                        .delete::<_, u32>(PREFIX_NODE_TYPE, id)
                        .await?;
                }
            }
        }

        // -- "parent" (all nodes) --
        for &id in &changed_ids {
            match action_for(&id) {
                Action::Set => {
                    if let Some(node_ref) = self.nodes.get(&id) {
                        let parent = match node_ref.value() {
                            TreeNode::Leaf(l) => l.parent_id.unwrap_or(NO_PARENT),
                            TreeNode::Internal(i) => i.parent_id.unwrap_or(NO_PARENT),
                        };
                        scalar_metadata_writer
                            .set(PREFIX_PARENT, id, parent)
                            .await?;
                    }
                }
                Action::Delete => {
                    scalar_metadata_writer
                        .delete::<_, u32>(PREFIX_PARENT, id)
                        .await?;
                }
            }
        }

        // -- "root" (singleton) --
        scalar_metadata_writer
            .set(
                PREFIX_ROOT,
                SINGLETON_KEY,
                self.root_id.load(Ordering::Relaxed),
            )
            .await?;

        // -- "version" -- versions are only ever upserted (never deleted per id);
        //    forked parent carries historical entries for ids we haven't touched.
        //    Only re-write versions whose `versions` entry was bumped since
        //    the last commit (`dirty_versions`).
        let mut version_entries: Vec<(u32, u32)> = self
            .dirty_versions
            .iter()
            .filter_map(|e| {
                let id = *e;
                self.versions.get(&id).map(|v| (id, *v as u32))
            })
            .collect();
        version_entries.sort_unstable_by_key(|(k, _)| *k);
        for (data_id, version) in version_entries {
            scalar_metadata_writer
                .set(PREFIX_VERSION, data_id, version)
                .await?;
        }

        // =========================================================
        // vector_data_writer
        // =========================================================

        // -- "centroid" (all nodes) --
        for &id in &changed_ids {
            match action_for(&id) {
                Action::Set => {
                    if let Some(node_ref) = self.nodes.get(&id) {
                        vector_data_writer
                            .set(PREFIX_CENTROID, id, node_ref.centroid().to_vec())
                            .await?;
                    }
                }
                Action::Delete => {
                    vector_data_writer
                        .delete::<_, Vec<f32>>(PREFIX_CENTROID, id)
                        .await?;
                }
            }
        }

        // -- "embedding" (upsert only; vector ids are never tombstoned) --
        //    Only re-write embeddings inserted since the last commit
        //    (`dirty_embeddings`); previously persisted embeddings are
        //    inherited verbatim from the forked parent.
        let mut embedding_entries: Vec<(u32, Arc<[f32]>)> = self
            .dirty_embeddings
            .iter()
            .filter_map(|e| {
                let id = *e;
                self.embeddings
                    .get(&id)
                    .map(|emb| (id, emb.value().clone()))
            })
            .collect();
        embedding_entries.sort_unstable_by_key(|(k, _)| *k);
        for (data_id, embedding) in embedding_entries {
            vector_data_writer
                .set(PREFIX_EMBEDDING, data_id, embedding.to_vec())
                .await?;
        }

        // =========================================================
        // list_data_writer
        // =========================================================

        // -- "centroid_code" (all nodes) --
        for &id in &changed_ids {
            match action_for(&id) {
                Action::Set => {
                    if let Some(node_ref) = self.nodes.get(&id) {
                        let code = node_ref.centroid_code();
                        if !code.is_empty() {
                            list_data_writer
                                .set(PREFIX_CENTROID_CODE, id, pack_bytes_to_u32s(code))
                                .await?;
                        }
                    }
                }
                Action::Delete => {
                    list_data_writer
                        .delete::<_, Vec<u32>>(PREFIX_CENTROID_CODE, id)
                        .await?;
                }
            }
        }

        // -- "children" (internal nodes only) --
        for &id in &changed_ids {
            match action_for(&id) {
                Action::Set => {
                    if let Some(node_ref) = self.nodes.get(&id) {
                        if let TreeNode::Internal(internal) = node_ref.value() {
                            list_data_writer
                                .set(PREFIX_CHILDREN, id, internal.children.clone())
                                .await?;
                        }
                    }
                }
                Action::Delete => {
                    list_data_writer
                        .delete::<_, Vec<u32>>(PREFIX_CHILDREN, id)
                        .await?;
                }
            }
        }

        // =========================================================
        // posting_list_writer: "" (leaf nodes only)
        //
        // Persist any leaf that has materialized data (`!ids.is_empty()`).
        // Lazy shells (ids empty but length>0) are inherited from the
        // forked parent — writing them would clobber disk postings with
        // empty clusters. Tombstoned leaves are deleted.
        // =========================================================
        for &id in &changed_ids {
            match action_for(&id) {
                Action::Set => {
                    let node_ref = self.nodes.get(&id);
                    if let Some(n) = node_ref {
                        if let TreeNode::Leaf(leaf) = n.value() {
                            if !leaf.ids.is_empty() {
                                let wide_versions: Vec<u32> =
                                    leaf.versions.iter().map(|&v| v as u32).collect();
                                let cluster = QuantizedCluster {
                                    center: &leaf.centroid,
                                    codes: &leaf.codes,
                                    ids: &leaf.ids,
                                    versions: &wide_versions,
                                };
                                posting_list_writer.set("", id, cluster).await?;
                            }
                        }
                    }
                }
                Action::Delete => {
                    posting_list_writer
                        .delete::<_, QuantizedCluster<'_>>("", id)
                        .await?;
                }
            }
        }

        // --- Commit all writers ---
        let posting_list_flusher = posting_list_writer
            .commit::<u32, QuantizedCluster<'_>>()
            .await?;
        let scalar_metadata_flusher = scalar_metadata_writer.commit::<u32, u32>().await?;
        let vector_data_flusher = vector_data_writer.commit::<u32, Vec<f32>>().await?;
        let list_data_flusher = list_data_writer.commit::<u32, Vec<u32>>().await?;

        // Tombstones and dirty sets have been flushed; clear for next
        // checkpoint. Even though the writer is typically dropped and
        // re-opened after commit, clearing keeps things correct if the
        // caller chooses to reuse the writer.
        self.tombstones.clear();
        self.dirty_nodes.clear();
        self.dirty_versions.clear();
        self.dirty_embeddings.clear();

        Ok(HierarchicalSpannFlusher {
            posting_list_flusher,
            scalar_metadata_flusher,
            vector_data_flusher,
            list_data_flusher,
        })
    }

    /// Open a persisted index from blockfiles with lazy leaf loading.
    pub async fn open(
        blockfile_provider: &BlockfileProvider,
        ids: HierarchicalSpannIds,
        distance_fn: DistanceFunction,
        config: HierarchicalSpannConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let sm_reader = blockfile_provider
            .read::<u32, u32>(BlockfileReaderOptions::new(
                ids.scalar_metadata_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        let root_id = sm_reader
            .get(PREFIX_ROOT, SINGLETON_KEY)
            .await?
            .expect("missing root_id");
        let next_node_id = sm_reader
            .get(PREFIX_NEXT_NODE, SINGLETON_KEY)
            .await?
            .expect("missing next_node_id");
        let dim = sm_reader
            .get(PREFIX_DIM, SINGLETON_KEY)
            .await?
            .expect("missing dim") as usize;

        let mut node_types: Vec<(u32, u32)> = Vec::new();
        for (_prefix, key, value) in sm_reader
            .get_range(PREFIX_NODE_TYPE..=PREFIX_NODE_TYPE, ..)
            .await?
        {
            node_types.push((key, value));
        }

        let mut parent_ids: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
        for (_prefix, key, value) in sm_reader
            .get_range(PREFIX_PARENT..=PREFIX_PARENT, ..)
            .await?
        {
            parent_ids.insert(key, value);
        }

        let mut lengths: std::collections::HashMap<u32, usize> = std::collections::HashMap::new();
        for (_prefix, key, value) in sm_reader
            .get_range(PREFIX_LENGTH..=PREFIX_LENGTH, ..)
            .await?
        {
            lengths.insert(key, value as usize);
        }

        // Versions populated lazily as posting lists are loaded.
        let versions: DashMap<u32, u8> = DashMap::new();

        let vd_reader = blockfile_provider
            .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(
                ids.vector_data_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        let mut centroids: std::collections::HashMap<u32, Vec<f32>> =
            std::collections::HashMap::new();
        for (_prefix, key, value) in vd_reader
            .get_range(PREFIX_CENTROID..=PREFIX_CENTROID, ..)
            .await?
        {
            centroids.insert(key, value.to_vec());
        }

        let ld_reader = blockfile_provider
            .read::<u32, &'static [u32]>(BlockfileReaderOptions::new(
                ids.list_data_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        let mut children_map: std::collections::HashMap<u32, Vec<u32>> =
            std::collections::HashMap::new();
        for (_prefix, key, value) in ld_reader
            .get_range(PREFIX_CHILDREN..=PREFIX_CHILDREN, ..)
            .await?
        {
            children_map.insert(key, value.to_vec());
        }

        let nodes: DashMap<NodeId, TreeNode> = DashMap::new();

        for &(node_id, ntype) in &node_types {
            let parent_id = parent_ids
                .get(&node_id)
                .copied()
                .map(|p| if p == NO_PARENT { None } else { Some(p) })
                .unwrap_or(None);
            let centroid = centroids.remove(&node_id).unwrap_or_else(|| vec![0.0; dim]);

            if ntype == NODE_TYPE_LEAF {
                let length = lengths.get(&node_id).copied().unwrap_or(0);
                nodes.insert(
                    node_id,
                    TreeNode::Leaf(LeafNode {
                        centroid,
                        centroid_code: Vec::new(),
                        ids: Vec::new(),
                        versions: Vec::new(),
                        codes: Vec::new(),
                        parent_id,
                        length,
                    }),
                );
            } else {
                let children = children_map.remove(&node_id).unwrap_or_default();
                nodes.insert(
                    node_id,
                    TreeNode::Internal(InternalNode {
                        centroid,
                        centroid_code: Vec::new(),
                        children,
                        parent_id,
                    }),
                );
            }
        }

        let code_byte_len = Code::<1, Vec<u8>>::size(dim);
        for (_prefix, key, value) in ld_reader
            .get_range(PREFIX_CENTROID_CODE..=PREFIX_CENTROID_CODE, ..)
            .await?
        {
            let code_bytes = unpack_u32s_to_bytes(&value, code_byte_len);
            if let Some(mut node_ref) = nodes.get_mut(&key) {
                match node_ref.value_mut() {
                    TreeNode::Leaf(leaf) => {
                        leaf.centroid_code = code_bytes;
                    }
                    TreeNode::Internal(internal) => {
                        internal.centroid_code = code_bytes;
                    }
                }
            }
        }

        // Fallback: recompute codes for any nodes missing persisted codes
        // (e.g., loading a checkpoint written before centroid_code persistence).
        let missing_code_ids: Vec<NodeId> = nodes
            .iter()
            .filter_map(|e| {
                if e.value().centroid_code().is_empty() {
                    Some(*e.key())
                } else {
                    None
                }
            })
            .collect();
        let zero_centroid = vec![0.0f32; dim];
        // Recomputed codes need to be persisted on the next commit, so mark
        // them dirty.
        let dirty_nodes_init: DashSet<NodeId> = DashSet::new();
        for &nid in &missing_code_ids {
            if let Some(mut node_ref) = nodes.get_mut(&nid) {
                let centroid = node_ref.centroid().to_vec();
                let code = Code::<1>::quantize(&centroid, &zero_centroid);
                let code_bytes = code.as_ref().to_vec();
                match node_ref.value_mut() {
                    TreeNode::Leaf(leaf) => leaf.centroid_code = code_bytes,
                    TreeNode::Internal(internal) => internal.centroid_code = code_bytes,
                }
                drop(node_ref);
                dirty_nodes_init.insert(nid);
            }
        }

        let posting_list_reader = Some(
            blockfile_provider
                .read::<u32, QuantizedCluster<'static>>(BlockfileReaderOptions::new(
                    ids.posting_list_id,
                    "".to_string(),
                ))
                .await
                .map_err(|e| e as Box<dyn ChromaError>)?,
        );
        let vector_data_reader = Some(vd_reader);

        Ok(Self {
            dim,
            distance_fn,
            config,
            nodes,
            balancing: DashSet::new(),
            tombstones: DashSet::new(),
            dirty_nodes: dirty_nodes_init,
            dirty_versions: DashSet::new(),
            dirty_embeddings: DashSet::new(),
            tree_lock: ReentrantMutex::new(()),
            root_id: AtomicU32::new(root_id),
            next_node_id: AtomicU32::new(next_node_id),
            embeddings: DashMap::new(),
            versions,
            stats: WriterStats::default(),
            zero_centroid: vec![0.0f32; dim],
            posting_list_reader,
            vector_data_reader,
        })
    }

    /// Eagerly load all leaf posting data from the persisted blockfile.
    /// Call after `open()` to fully materialize the index in memory before
    /// continuing to add vectors.
    pub async fn load_all_postings(&self) -> Result<(), Box<dyn ChromaError>> {
        let leaf_ids: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|e| matches!(e.value(), TreeNode::Leaf(_)))
            .map(|e| *e.key())
            .collect();
        for id in leaf_ids {
            self.load(id).await?;
        }
        Ok(())
    }

    /// Lazily load a leaf node's posting data (ids, codes, versions) from the
    /// persisted blockfile.
    pub async fn load(&self, node_id: NodeId) -> Result<(), Box<dyn ChromaError>> {
        let Some(reader) = &self.posting_list_reader else {
            return Ok(());
        };

        {
            let node_ref = self.nodes.get(&node_id);
            match node_ref.as_ref().map(|r| r.value()) {
                Some(TreeNode::Leaf(leaf)) if leaf.ids.len() >= leaf.length => return Ok(()),
                Some(TreeNode::Leaf(_)) => {}
                _ => return Ok(()),
            }
        }

        let Some(cluster) = reader.get("", node_id).await? else {
            return Ok(());
        };

        // I/O accounting: count this as one posting load, plus the number of
        // entries fetched. Bytes ≈ entries * (4 + code_size + 1) where
        // code_size = dim/8 for 1-bit codes.
        self.stats.posting_loads.fetch_add(1, Ordering::Relaxed);
        self.stats
            .posting_load_entries
            .fetch_add(cluster.ids.len() as u64, Ordering::Relaxed);

        let narrow_versions: Vec<u8> = cluster.versions.iter().map(|&v| v as u8).collect();
        let loaded_ids = cluster.ids.to_vec();

        for (&id, &ver) in loaded_ids.iter().zip(narrow_versions.iter()) {
            self.versions.entry(id).or_insert(ver);
        }

        if let Some(mut node_ref) = self.nodes.get_mut(&node_id) {
            if let TreeNode::Leaf(leaf) = node_ref.value_mut() {
                if leaf.ids.len() < leaf.length {
                    leaf.ids = loaded_ids;
                    leaf.versions = narrow_versions;
                    leaf.codes = cluster.codes.to_vec();
                    leaf.length = leaf.ids.len();
                }
            }
        }

        Ok(())
    }

    pub fn load_embeddings_sync(&self, ids: &[u32]) {
        if self.vector_data_reader.is_none() {
            return;
        }
        let missing: Vec<u32> = ids
            .iter()
            .copied()
            .filter(|id| !self.embeddings.contains_key(id))
            .collect();
        if missing.is_empty() {
            return;
        }
        let _ = block_on_for_sync_writer(self.load_raw(&missing));
    }

    pub fn load_posting_sync(&self, node_id: NodeId) {
        if self.posting_list_reader.is_none() {
            return;
        }
        let _ = block_on_for_sync_writer(self.load(node_id));
    }

    /// Lazily load raw f32 embeddings from the persisted blockfile.
    pub async fn load_raw(&self, ids: &[u32]) -> Result<(), Box<dyn ChromaError>> {
        let Some(reader) = &self.vector_data_reader else {
            return Ok(());
        };

        let missing_ids: Vec<u32> = ids
            .iter()
            .copied()
            .filter(|id| !self.embeddings.contains_key(id))
            .collect();

        for id in missing_ids {
            if let Some(embedding) = reader.get(PREFIX_EMBEDDING, id).await? {
                self.embeddings.insert(id, Arc::from(embedding));
                // I/O accounting: bytes = dim * 4.
                self.stats.embedding_loads.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    /// `(posting (count, bytes), vector (count, bytes))` for the per-reader
    /// `loaded_blocks` pin sets. See `BlockfileReader::loaded_blocks_stats`.
    /// Use to attribute the unaccounted RSS that the writer's
    /// `memory_usage()` cannot see (the pinned-block payload lives inside
    /// the chroma_blockstore reader, not in writer-owned containers).
    pub fn reader_block_pin_stats(&self) -> ((usize, u64), (usize, u64)) {
        let posting = self
            .posting_list_reader
            .as_ref()
            .map(|r| r.loaded_blocks_stats())
            .unwrap_or((0, 0));
        let vector = self
            .vector_data_reader
            .as_ref()
            .map(|r| r.loaded_blocks_stats())
            .unwrap_or((0, 0));
        (posting, vector)
    }

    /// Drop every block currently pinned by the writer's two
    /// `BlockfileReader`s. Releases potentially many GB of heap that the
    /// foyer block cache cannot bound (the per-reader `loaded_blocks`
    /// HashMap is independent of the foyer cache and grows monotonically
    /// across `get`/`get_range` calls until the reader is dropped).
    ///
    /// **Safety contract**: caller must guarantee no value previously
    /// returned from either reader is still borrowed. The writer's own
    /// `load`/`load_raw` paths copy data via `to_vec()` and drop the
    /// returned `V` before returning, so calling this between checkpoint
    /// phases (after `add` + `balance_index_parallel`, before `commit`)
    /// from the main thread, with no concurrent writer activity, is
    /// sound. See bench `docs/README.md` -> "Reader-side block pinning"
    /// for the full discussion and the upstream fix.
    pub fn clear_reader_block_pins(&self) {
        if let Some(r) = self.posting_list_reader.as_ref() {
            r.clear_loaded_blocks();
        }
        if let Some(r) = self.vector_data_reader.as_ref() {
            r.clear_loaded_blocks();
        }
    }
}
