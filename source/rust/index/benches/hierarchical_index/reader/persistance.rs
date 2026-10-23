#![allow(dead_code)]

use std::sync::Arc;

use chroma_blockstore::{arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider};
use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;
use chroma_types::hierarchical_spann::{
    HierarchicalInternalNode, HierarchicalLeafNode, HierarchicalSpannPostingList,
};
use dashmap::DashMap;

use super::super::common::{InternalNode, LeafNode, NodeId, TreeNode};
use super::super::config::HierarchicalSpannConfig;
use super::super::persistance::{
    NO_PARENT, PREFIX_CENTROID, PREFIX_DIM, PREFIX_EMBEDDING, PREFIX_ROOT, PREFIX_VERSION,
    SINGLETON_KEY,
};

use super::super::writer::{persistence::HierarchicalSpannIds, WriterStats};

use super::super::common::code_slice;
use super::super::writer::DELETED_BIT;
use super::{HierarchicalSpannReader, ReaderIntegrityError};
use chroma_index::quantization::Code;
use futures::stream::{self, StreamExt, TryStreamExt};

impl HierarchicalSpannReader {
    /// Open a persisted index for read-only search. Loads centroid_codes and
    /// tree structure; f32 centroids and posting data are lazy-loaded.
    pub async fn open(
        blockfile_provider: &BlockfileProvider,
        ids: HierarchicalSpannIds,
        distance_fn: DistanceFunction,
        config: HierarchicalSpannConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        // --- Step 1: Read global scalars ---
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
        let dim = sm_reader
            .get(PREFIX_DIM, SINGLETON_KEY)
            .await?
            .expect("missing dim") as usize;

        // --- Step 2: Build the node tree from typed blockfiles ---
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

        let nodes: DashMap<NodeId, TreeNode> = DashMap::new();
        let mut persisted_empty_leaves=Vec::new();

        for (_prefix, node_id, leaf) in ln_reader.get_range(""..="", ..).await? {
            if leaf.length==0 { persisted_empty_leaves.push(node_id); }
            let parent_id = if leaf.parent == NO_PARENT {
                None
            } else {
                Some(leaf.parent)
            };
            nodes.insert(
                node_id,
                TreeNode::Leaf(LeafNode {
                    centroid: Vec::new(), // f32 centroid loaded lazily from posting list
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
            nodes.insert(
                node_id,
                TreeNode::Internal(InternalNode {
                    centroid: Vec::new(), // f32 centroid not needed by reader
                    centroid_code: internal.centroid_code.to_vec(),
                    children: internal.children.to_vec(),
                    parent_id,
                }),
            );
        }

        // --- Step 3: Open blockfile readers for lazy loading ---
        let posting_list_reader = blockfile_provider
            .read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
                ids.posting_list_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        let vector_data_reader = blockfile_provider
            .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(
                ids.vector_data_id,
                "".to_string(),
            ))
            .await
            .map_err(|e| e as Box<dyn ChromaError>)?;

        persisted_empty_leaves.sort_unstable();
        Ok(Self {
            navigation_identity:std::sync::Arc::new(()),
            dim,
            distance_fn,
            config,
            nodes,
            root_id,
            persisted_empty_leaves,
            embeddings: DashMap::new(),
            stats: WriterStats::default(),
            scalar_metadata_reader: sm_reader,
            version_reader_lock: tokio::sync::Mutex::new(()),
            posting_list_reader,
            vector_data_reader,
        })
    }

    // =========================================================================
    // Lazy loading
    // =========================================================================

    /// Lazily load a leaf's posting data (ids, codes, versions) and centroid.
    pub async fn load_node_posting_list(
        &self,
        node_id: NodeId,
    ) -> Result<(), Box<dyn ChromaError>> {
        self.load_node_posting_list_observed(node_id,None).await
    }

    pub(super) async fn load_node_posting_list_observed(&self,node_id:NodeId,observation:Option<Arc<super::preparation::Preparation>>)->Result<(),Box<dyn ChromaError>> {
        use super::preparation::nanos;
        use std::time::Instant;
        let mut observed=observation.as_ref().map(|p|p.leaf(node_id));
        macro_rules! mark {($body:expr)=>{if let Some(o)=observed.as_mut() {($body)(&mut o.value);}};}
        {
            let node_ref = self.nodes.get(&node_id);
            match node_ref.as_ref().map(|r| r.value()) {
                Some(TreeNode::Leaf(leaf)) if leaf.ids.len() >= leaf.length => {mark!(|v:&mut super::preparation::LeafPreparation| {v.cached=true;v.completed=true;});return Ok(());}
                Some(TreeNode::Leaf(_)) => {}
                _ => return Ok(()),
            }
        }

        let posting_started=observed.as_ref().map(|_|Instant::now());
        let Some(posting) = self.posting_list_reader.get("", node_id).await? else {
            return Ok(());
        };

        if let Some(t)=posting_started {mark!(|v:&mut super::preparation::LeafPreparation| {v.posting_ns=nanos(t);v.physical_rows=posting.ids.len();v.physical_code_bytes=posting.codes.len();});}
        // Physical postings retain old copies after reassignment and deletion.
        // Keep only the version published in scalar metadata. Read selected
        // postings in bounded batches; never load the collection's version map.
        let code_size = Code::<1, Vec<u8>>::size(self.dim);
        if posting.ids.len() != posting.versions.len()
            || posting.codes.len() != posting.ids.len() * code_size
        {
            return Err(Box::new(ReaderIntegrityError(format!(
                "malformed posting list {node_id}"
            ))));
        }
        let allocation_started=observed.as_ref().map(|_|Instant::now());
        let mut valid_ids = Vec::with_capacity(posting.ids.len());
        let mut valid_versions = Vec::with_capacity(posting.ids.len());
        let mut valid_codes = Vec::with_capacity(posting.codes.len());
        let posting_ids = posting.ids;
        if let Some(t)=allocation_started {mark!(|v:&mut super::preparation::LeafPreparation| {v.allocation_ns=nanos(t);v.ids_capacity=valid_ids.capacity();v.versions_capacity=valid_versions.capacity();v.codes_capacity=valid_codes.capacity();});}
        if let Some(p)=observation.as_ref() {mark!(|v:&mut super::preparation::LeafPreparation|v.lock_enqueue_ns=nanos(p.start));}
        let guard = self.version_reader_lock.lock().await;
        if let Some(p)=observation.as_ref() {mark!(|v:&mut super::preparation::LeafPreparation|v.lock_acquired_ns=nanos(p.start));}
        let loop_started=observed.as_ref().map(|_|Instant::now());
        for start in (0..posting.ids.len()).step_by(64) {
            let end = (start + 64).min(posting.ids.len());
            if observed.is_some() {self.scalar_metadata_reader.reset_scalar_preparation_batch();mark!(|v:&mut super::preparation::LeafPreparation|{v.batches+=1;v.scheduled_point_gets+=end-start;});}
            let lookup_started=observed.as_ref().map(|_|Instant::now());
            let versions = stream::iter(start..end)
                .map(|i| async move {
                    let id = posting_ids[i];
                    self.scalar_metadata_reader
                        .get(PREFIX_VERSION, id)
                        .await
                        .map(|version| (i, version))
                })
                .buffer_unordered(16)
                .try_collect::<Vec<_>>()
                .await?;
            if let Some(t)=lookup_started {mark!(|v:&mut super::preparation::LeafPreparation|v.lookup_ns+=nanos(t));if let Some(snapshot)=self.scalar_metadata_reader.loaded_block_observation() {let n=snapshot.batch_point_blocks.len();if n>64 {if let Some(p)=observation.as_ref(){*p.overflow.lock().unwrap()=true;}}mark!(|v:&mut super::preparation::LeafPreparation|{*v.batch_distinct_blocks.entry(n).or_default()+=1;});}}
            // These values are owned integers, so scalar block pins can be released.
            self.scalar_metadata_reader.clear_loaded_blocks();
            let filter_started=observed.as_ref().map(|_|Instant::now());
            for (i, current) in versions {
                if current.is_none() {mark!(|v:&mut super::preparation::LeafPreparation|v.missing_rows+=1);}
                let current = current.ok_or_else(|| {
                    Box::new(ReaderIntegrityError(format!(
                        "missing published version for vector {}",
                        posting_ids[i]
                    ))) as Box<dyn ChromaError>
                })?;
                if current > u8::MAX as u32 {
                    mark!(|v:&mut super::preparation::LeafPreparation|v.invalid_versions+=1);
                    return Err(Box::new(ReaderIntegrityError(format!(
                        "invalid published version for vector {}",
                        posting_ids[i]
                    ))));
                }
                let current = current as u8;
                mark!(|v:&mut super::preparation::LeafPreparation|{if current & DELETED_BIT !=0 {v.deleted_rows+=1;} else if posting.versions[i]!=current {v.stale_rows+=1;}else{v.current_rows+=1;v.copied_code_bytes+=code_size;}});
                if current & DELETED_BIT == 0 && posting.versions[i] == current {
                    valid_ids.push(posting_ids[i]);
                    valid_versions.push(current);
                    valid_codes.extend_from_slice(code_slice(posting.codes, i, code_size));
                }
            }
            if let Some(t)=filter_started {mark!(|v:&mut super::preparation::LeafPreparation|v.filter_copy_ns+=nanos(t));}
        }
        if let Some(t)=loop_started {mark!(|v:&mut super::preparation::LeafPreparation|v.version_loop_ns=nanos(t));}
        drop(guard);
        if let Some(p)=observation.as_ref() {mark!(|v:&mut super::preparation::LeafPreparation|v.lock_released_ns=nanos(p.start));}

        // Load the per-leaf f32 centroid — required by score_leaves for the
        // residual query (r_q = query - centroid), c_norm and c_dot_q.
        // Stored in vector_data under PREFIX_CENTROID by the writer.
        let centroid_started=observed.as_ref().map(|_|Instant::now());
        let centroid = self
            .vector_data_reader
            .get(PREFIX_CENTROID, node_id)
            .await?
            .map(|s| s.to_vec())
            .unwrap_or_default();

        if let Some(t)=centroid_started {mark!(|v:&mut super::preparation::LeafPreparation|v.centroid_ns=nanos(t));}
        let install_started=observed.as_ref().map(|_|Instant::now());
        if let Some(mut node_ref) = self.nodes.get_mut(&node_id) {
            if let TreeNode::Leaf(leaf) = node_ref.value_mut() {
                if leaf.ids.len() < leaf.length {
                    leaf.ids = valid_ids;
                    leaf.versions = valid_versions;
                    leaf.codes = valid_codes;
                    leaf.length = leaf.ids.len();
                    leaf.centroid = centroid;
                    mark!(|v:&mut super::preparation::LeafPreparation|v.installed=true);
                }
            }
        }

        if let Some(t)=install_started {mark!(|v:&mut super::preparation::LeafPreparation|{v.install_ns=nanos(t);v.completed=true;});}
        Ok(())
    }

    /// Lazily load raw f32 embeddings for vector reranking.
    pub async fn load_embeddings(&self, ids: &[u32]) -> Result<(), Box<dyn ChromaError>> {
        let missing_ids: Vec<u32> = ids
            .iter()
            .copied()
            .filter(|id| !self.embeddings.contains_key(id))
            .collect();

        for id in missing_ids {
            if let Some(embedding) = self.vector_data_reader.get(PREFIX_EMBEDDING, id).await? {
                self.embeddings.insert(id, Arc::from(embedding));
            }
        }

        Ok(())
    }

    /// Eagerly load all leaf posting data from the blockfile.
    pub async fn load_all_postings(&self) -> Result<(), Box<dyn ChromaError>> {
        let leaf_ids: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|e| matches!(e.value(), TreeNode::Leaf(_)))
            .map(|e| *e.key())
            .collect();
        for id in leaf_ids {
            self.load_node_posting_list(id).await?;
        }
        Ok(())
    }

    /// Eagerly load all raw f32 embeddings from the blockfile.
    pub async fn load_all_embeddings(&self) -> Result<(), Box<dyn ChromaError>> {
        let all_ids: Vec<u32> = self
            .nodes
            .iter()
            .filter_map(|e| {
                if let TreeNode::Leaf(leaf) = e.value() {
                    Some(leaf.ids.clone())
                } else {
                    None
                }
            })
            .flatten()
            .collect();
        self.load_embeddings(&all_ids).await
    }

    // =========================================================================
    // Block-pin management
    // =========================================================================

    /// Aggregate block-pin stats across both internal blockfile readers.
    /// Returns ((posting_blocks, posting_bytes), (vector_data_blocks, vector_data_bytes)).
    /// See `docs/README.md` ("Reader-side block pinning") for why this matters.
    pub fn loaded_blocks_stats(&self) -> ((usize, u64), (usize, u64)) {
        (
            self.posting_list_reader.loaded_blocks_stats(),
            self.vector_data_reader.loaded_blocks_stats(),
        )
    }

    /// Clear block pins in the posting, vector, and version readers. Safe to call
    /// after any data a caller needed has been copied out (e.g. after
    /// `load_all_postings` / `load_all_embeddings`, or between tau rounds
    /// in lazy recall mode). Subsequent reads will refault through the
    /// provider's Foyer cache.
    pub fn clear_loaded_blocks(&self) {
        self.scalar_metadata_reader.clear_loaded_blocks();
        self.posting_list_reader.clear_loaded_blocks();
        self.vector_data_reader.clear_loaded_blocks();
    }

    /// Count of leaves whose posting data (ids/codes/versions/centroid) is
    /// currently materialized in `self.nodes`, and the owned-bytes those
    /// fields contribute. The `centroid_code` field is loaded once at
    /// `open()` and is NOT included here; only fields that `load_node` /
    /// `clear_loaded_postings` touch.
    pub fn loaded_postings_stats(&self) -> (usize, u64) {
        let mut leaves_loaded = 0usize;
        let mut bytes = 0u64;
        for entry in self.nodes.iter() {
            if let TreeNode::Leaf(leaf) = entry.value() {
                if !leaf.ids.is_empty() {
                    leaves_loaded += 1;
                    bytes += (leaf.ids.len() * std::mem::size_of::<u32>()) as u64;
                    bytes += leaf.codes.len() as u64;
                    bytes += leaf.versions.len() as u64;
                    bytes += (leaf.centroid.len() * std::mem::size_of::<f32>()) as u64;
                }
            }
        }
        (leaves_loaded, bytes)
    }

    /// Drop posting data (ids/codes/versions/centroid) from every leaf in
    /// `self.nodes`, so subsequent `load_node` calls re-fetch from the
    /// posting blockfile. The `length` field is preserved so `load_node`'s
    /// `ids.len() < length` check still triggers a refetch. Tree topology
    /// and quantized centroid_codes are untouched. Returns
    /// `(leaves_cleared, bytes_freed)`.
    pub fn clear_loaded_postings(&self) -> (usize, u64) {
        let mut leaves_cleared = 0usize;
        let mut bytes_freed = 0u64;
        for mut entry in self.nodes.iter_mut() {
            if let TreeNode::Leaf(leaf) = entry.value_mut() {
                if !leaf.ids.is_empty() {
                    bytes_freed += (leaf.ids.len() * std::mem::size_of::<u32>()) as u64;
                    bytes_freed += leaf.codes.len() as u64;
                    bytes_freed += leaf.versions.len() as u64;
                    bytes_freed += (leaf.centroid.len() * std::mem::size_of::<f32>()) as u64;
                    leaf.ids = Vec::new();
                    leaf.codes = Vec::new();
                    leaf.versions = Vec::new();
                    leaf.centroid = Vec::new();
                    // Keep `length` intact so load_node detects the gap.
                    leaves_cleared += 1;
                }
            }
        }
        (leaves_cleared, bytes_freed)
    }
}
