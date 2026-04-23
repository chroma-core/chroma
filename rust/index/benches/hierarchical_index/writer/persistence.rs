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
use chroma_types::hierarchical_spann::{
    HierarchicalInternalNode, HierarchicalLeafNode, HierarchicalSpannPostingList,
};
use dashmap::{DashMap, DashSet};
use parking_lot::ReentrantMutex;
use uuid::Uuid;

use super::super::common::{InternalNode, LeafNode, NodeId, TreeNode};
use super::super::persistance::{
    NO_PARENT, PREFIX_CENTROID, PREFIX_DIM, PREFIX_EMBEDDING, PREFIX_NEXT_NODE, PREFIX_ROOT,
    PREFIX_VERSION, SINGLETON_KEY,
};
use super::super::writer::DELETED_BIT;
use super::{HierarchicalSpannConfig, HierarchicalSpannWriter, WriterStats};

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
    pub leaf_node_id: Uuid,
    pub internal_node_id: Uuid,
}

pub struct HierarchicalSpannFlusher {
    posting_list_flusher: BlockfileFlusher,
    scalar_metadata_flusher: BlockfileFlusher,
    vector_data_flusher: BlockfileFlusher,
    leaf_node_flusher: BlockfileFlusher,
    internal_node_flusher: BlockfileFlusher,
}

impl HierarchicalSpannFlusher {
    pub async fn flush(self) -> Result<HierarchicalSpannIds, Box<dyn ChromaError>> {
        let posting_list_id = self.posting_list_flusher.id();
        let scalar_metadata_id = self.scalar_metadata_flusher.id();
        let vector_data_id = self.vector_data_flusher.id();
        let leaf_node_id = self.leaf_node_flusher.id();
        let internal_node_id = self.internal_node_flusher.id();

        self.posting_list_flusher
            .flush::<u32, HierarchicalSpannPostingList<'_>>()
            .await?;
        self.scalar_metadata_flusher.flush::<u32, u32>().await?;
        self.vector_data_flusher.flush::<u32, Vec<f32>>().await?;
        self.leaf_node_flusher
            .flush::<u32, HierarchicalLeafNode<'_>>()
            .await?;
        self.internal_node_flusher
            .flush::<u32, HierarchicalInternalNode<'_>>()
            .await?;

        Ok(HierarchicalSpannIds {
            posting_list_id,
            scalar_metadata_id,
            vector_data_id,
            leaf_node_id,
            internal_node_id,
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
        let mut ln_options = BlockfileWriterOptions::new("".to_string()).ordered_mutations();
        let mut in_options = BlockfileWriterOptions::new("".to_string()).ordered_mutations();
        if let Some(ids) = fork_from {
            pl_options = pl_options.fork(ids.posting_list_id);
            sm_options = sm_options.fork(ids.scalar_metadata_id);
            vd_options = vd_options.fork(ids.vector_data_id);
            ln_options = ln_options.fork(ids.leaf_node_id);
            in_options = in_options.fork(ids.internal_node_id);
        }

        let posting_list_writer = blockfile_provider
            .write::<u32, HierarchicalSpannPostingList<'_>>(pl_options)
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
        let leaf_node_writer = blockfile_provider
            .write::<u32, HierarchicalLeafNode<'_>>(ln_options)
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;
        let internal_node_writer = blockfile_provider
            .write::<u32, HierarchicalInternalNode<'_>>(in_options)
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
        //   scalar_metadata_writer: dim < next_node < root < version
        //   vector_data_writer:     centroid < embedding
        //   leaf_node_writer:       "" (empty prefix, key = node_id)
        //   internal_node_writer:   "" (empty prefix, key = node_id)
        //   posting_list_writer:    "" (empty)

        // =========================================================
        // scalar_metadata_writer
        // =========================================================

        // -- "dim" (singleton) --
        scalar_metadata_writer
            .set(PREFIX_DIM, SINGLETON_KEY, self.dim as u32)
            .await?;

        // -- "next_node" (singleton) --
        scalar_metadata_writer
            .set(
                PREFIX_NEXT_NODE,
                SINGLETON_KEY,
                self.next_node_id.load(Ordering::Relaxed),
            )
            .await?;

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

        // -- "centroid" (per-node f32 centroid; writer-only data) --
        //    Reader does not open this prefix; only the writer's `open()`
        //    enumerates it via `get_range`. We write a centroid for every
        //    dirty live node and delete for every tombstoned node so the
        //    forked parent stays in sync with the leaf/internal node
        //    blockfile. Iterate `changed_ids` in sorted order.
        for &id in &changed_ids {
            match action_for(&id) {
                Action::Set => {
                    if let Some(node_ref) = self.nodes.get(&id) {
                        let centroid = match node_ref.value() {
                            TreeNode::Leaf(leaf) => leaf.centroid.clone(),
                            TreeNode::Internal(internal) => internal.centroid.clone(),
                        };
                        vector_data_writer
                            .set(PREFIX_CENTROID, id, centroid)
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

        // -- "embedding" deletes --
        //    Ids tombstoned via `delete()` since the last commit. The
        //    version blockfile (above) already carries DELETED_BIT for these
        //    ids; here we erase the actual f32 data so it can't be served by
        //    rerank/recall and so disk space is reclaimed.
        let mut deleted_emb_ids: Vec<u32> =
            self.dirty_deleted_embeddings.iter().map(|e| *e).collect();
        deleted_emb_ids.sort_unstable();
        let n_deleted = deleted_emb_ids.len() as u64;
        for data_id in deleted_emb_ids {
            vector_data_writer
                .delete::<_, Vec<f32>>(PREFIX_EMBEDDING, data_id)
                .await?;
        }
        self.stats
            .embedding_deletes_committed
            .fetch_add(n_deleted, std::sync::atomic::Ordering::Relaxed);

        // =========================================================
        // leaf_node_writer / internal_node_writer
        //
        // Each dirty or tombstoned node is written to exactly one typed
        // writer. For tombstones we delete from both writers: only one
        // will have the key; the other delete is a harmless no-op against
        // the forked parent.
        // =========================================================

        for &id in &changed_ids {
            match action_for(&id) {
                Action::Set => {
                    if let Some(node_ref) = self.nodes.get(&id) {
                        match node_ref.value() {
                            TreeNode::Leaf(leaf) => {
                                let length = if leaf.ids.is_empty() {
                                    leaf.length
                                } else {
                                    leaf.ids.len()
                                };
                                let node = HierarchicalLeafNode {
                                    parent: leaf.parent_id.unwrap_or(NO_PARENT),
                                    length: length as u32,
                                    centroid_code: &leaf.centroid_code,
                                };
                                leaf_node_writer.set("", id, node).await?;
                            }
                            TreeNode::Internal(internal) => {
                                let node = HierarchicalInternalNode {
                                    parent: internal.parent_id.unwrap_or(NO_PARENT),
                                    centroid_code: &internal.centroid_code,
                                    children: &internal.children,
                                };
                                internal_node_writer.set("", id, node).await?;
                            }
                        }
                    }
                }
                Action::Delete => {
                    leaf_node_writer
                        .delete::<_, HierarchicalLeafNode<'_>>("", id)
                        .await?;
                    internal_node_writer
                        .delete::<_, HierarchicalInternalNode<'_>>("", id)
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
                                let posting = HierarchicalSpannPostingList {
                                    codes: &leaf.codes,
                                    ids: &leaf.ids,
                                    versions: &leaf.versions,
                                };
                                posting_list_writer.set("", id, posting).await?;
                            }
                        }
                    }
                }
                Action::Delete => {
                    posting_list_writer
                        .delete::<_, HierarchicalSpannPostingList<'_>>("", id)
                        .await?;
                }
            }
        }

        // --- Commit all writers ---
        let posting_list_flusher = posting_list_writer
            .commit::<u32, HierarchicalSpannPostingList<'_>>()
            .await?;
        let scalar_metadata_flusher = scalar_metadata_writer.commit::<u32, u32>().await?;
        let vector_data_flusher = vector_data_writer.commit::<u32, Vec<f32>>().await?;
        let leaf_node_flusher = leaf_node_writer
            .commit::<u32, HierarchicalLeafNode<'_>>()
            .await?;
        let internal_node_flusher = internal_node_writer
            .commit::<u32, HierarchicalInternalNode<'_>>()
            .await?;

        // Tombstones and dirty sets have been flushed; clear for next
        // checkpoint. Even though the writer is typically dropped and
        // re-opened after commit, clearing keeps things correct if the
        // caller chooses to reuse the writer.
        self.tombstones.clear();
        self.dirty_nodes.clear();
        self.dirty_versions.clear();
        self.dirty_embeddings.clear();
        self.dirty_deleted_embeddings.clear();

        Ok(HierarchicalSpannFlusher {
            posting_list_flusher,
            scalar_metadata_flusher,
            vector_data_flusher,
            leaf_node_flusher,
            internal_node_flusher,
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

        // Live versions are populated lazily as posting lists are loaded.
        // Tombstoned versions (DELETED_BIT set) MUST be loaded eagerly:
        // otherwise `load_posting_sync` would `or_insert` the stale low-7-bit
        // version from the posting list and the deleted id would resurrect
        // (no DELETED_BIT in the global version => `is_valid` would pass).
        let versions: DashMap<u32, u8> = DashMap::new();
        for (_prefix, id, ver) in sm_reader
            .get_range(PREFIX_VERSION..=PREFIX_VERSION, ..)
            .await?
        {
            let v = ver as u8;
            if v & DELETED_BIT != 0 {
                versions.insert(id, v);
            }
        }

        let vd_reader = blockfile_provider
            .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(
                ids.vector_data_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        let ln_reader = blockfile_provider
            .read::<u32, HierarchicalLeafNode<'static>>(BlockfileReaderOptions::new(
                ids.leaf_node_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        let in_reader = blockfile_provider
            .read::<u32, HierarchicalInternalNode<'static>>(BlockfileReaderOptions::new(
                ids.internal_node_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        // Eagerly load per-node f32 centroids from vector_data under
        // PREFIX_CENTROID. Writer needs full-precision centroids for
        // distance computations during balancing/splits; they are not
        // embedded in the leaf/internal node blockfiles (so the reader
        // can skip them entirely).
        let centroid_map: std::collections::HashMap<NodeId, Vec<f32>> = vd_reader
            .get_range(PREFIX_CENTROID..=PREFIX_CENTROID, ..)
            .await?
            .into_iter()
            .map(|(_p, k, v)| (k, v.to_vec()))
            .collect();

        let nodes: DashMap<NodeId, TreeNode> = DashMap::new();

        for (_prefix, node_id, leaf) in ln_reader.get_range(""..="", ..).await? {
            let parent_id = if leaf.parent == NO_PARENT {
                None
            } else {
                Some(leaf.parent)
            };
            let centroid = centroid_map.get(&node_id).cloned().unwrap_or_default();
            nodes.insert(
                node_id,
                TreeNode::Leaf(LeafNode {
                    centroid,
                    centroid_code: leaf.centroid_code.to_vec(),
                    ids: Vec::new(),
                    versions: Vec::new(),
                    codes: Vec::new(),
                    parent_id,
                    length: leaf.length as usize,
                }),
            );
        }

        for (_prefix, node_id, internal) in in_reader.get_range(""..="", ..).await? {
            let parent_id = if internal.parent == NO_PARENT {
                None
            } else {
                Some(internal.parent)
            };
            let centroid = centroid_map.get(&node_id).cloned().unwrap_or_default();
            nodes.insert(
                node_id,
                TreeNode::Internal(InternalNode {
                    centroid,
                    centroid_code: internal.centroid_code.to_vec(),
                    children: internal.children.to_vec(),
                    parent_id,
                }),
            );
        }

        let dirty_nodes_init: DashSet<NodeId> = DashSet::new();

        let posting_list_reader = Some(
            blockfile_provider
                .read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
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
            dirty_deleted_embeddings: DashSet::new(),
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

        let Some(posting) = reader.get("", node_id).await? else {
            return Ok(());
        };

        // I/O accounting: count this as one posting load, plus the number of
        // entries fetched. Bytes ≈ entries * (4 + code_size + 1) where
        // code_size = dim/8 for 1-bit codes.
        self.stats.posting_loads.fetch_add(1, Ordering::Relaxed);
        self.stats
            .posting_load_entries
            .fetch_add(posting.ids.len() as u64, Ordering::Relaxed);

        let loaded_ids = posting.ids.to_vec();
        let loaded_versions: Vec<u8> = posting.versions.to_vec();

        for (&id, &ver) in loaded_ids.iter().zip(loaded_versions.iter()) {
            self.versions.entry(id).or_insert(ver);
        }

        if let Some(mut node_ref) = self.nodes.get_mut(&node_id) {
            if let TreeNode::Leaf(leaf) = node_ref.value_mut() {
                if leaf.ids.len() < leaf.length {
                    leaf.ids = loaded_ids;
                    leaf.versions = loaded_versions;
                    leaf.codes = posting.codes.to_vec();
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
