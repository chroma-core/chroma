#![allow(dead_code)]

use std::sync::Arc;

use chroma_blockstore::{arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider};
use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;
use chroma_index::quantization::PackedFourBitCentroid;
use chroma_types::hierarchical_spann::{
    HierarchicalInternalNode, HierarchicalLeafNode, HierarchicalSpannPostingList,
};
use dashmap::DashMap;

use super::super::common::{InternalNode, LeafNode, NodeId, TreeNode};
use super::super::config::HierarchicalSpannConfig;
use super::super::persistance::{
    validate_navigation_format, NO_PARENT, PREFIX_CENTROID, PREFIX_DIM, PREFIX_EMBEDDING,
    PREFIX_NAVIGATION_FORMAT, PREFIX_ROOT, SINGLETON_KEY,
};

use super::super::writer::{persistence::HierarchicalSpannIds, WriterStats};

use super::HierarchicalSpannReader;

impl HierarchicalSpannReader {
    /// Open tree metadata and ready-to-score four-bit centroid codes.
    /// Legacy checkpoints prepare codes from original centroids once. Posting
    /// data and original vectors remain lazy in both formats.
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

        super::super::writer::upgrade::require_version_free(&sm_reader).await?;

        let root_id = sm_reader
            .get(PREFIX_ROOT, SINGLETON_KEY)
            .await?
            .expect("missing root_id");
        let dim = sm_reader
            .get(PREFIX_DIM, SINGLETON_KEY)
            .await?
            .expect("missing dim") as usize;

        let navigation_format = sm_reader
            .get(PREFIX_NAVIGATION_FORMAT, SINGLETON_KEY)
            .await?;
        validate_navigation_format(navigation_format)?;

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
        let mut navigation_codes = std::collections::HashMap::new();

        for (_prefix, node_id, leaf) in ln_reader.get_range(""..="", ..).await? {
            if navigation_format.is_some() {
                navigation_codes.insert(
                    node_id,
                    PackedFourBitCentroid::from_bytes(leaf.centroid_code, dim)
                        .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?,
                );
            }
            let parent_id = if leaf.parent == NO_PARENT {
                None
            } else {
                Some(leaf.parent)
            };
            nodes.insert(
                node_id,
                TreeNode::Leaf(LeafNode {
                    navigation: std::sync::Arc::new(super::super::common::NavigationNode::new(
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                        parent_id,
                    )),
                    ids: Vec::new(),
                    codes: Vec::new(),
                    length: leaf.length as usize,
                }),
            );
        }

        for (_prefix, node_id, internal) in in_reader.get_range(""..="", ..).await? {
            if navigation_format.is_some() {
                navigation_codes.insert(
                    node_id,
                    PackedFourBitCentroid::from_bytes(internal.centroid_code, dim)
                        .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?,
                );
            }
            let parent_id = if internal.parent == NO_PARENT {
                None
            } else {
                Some(internal.parent)
            };
            nodes.insert(
                node_id,
                TreeNode::Internal(InternalNode {
                    navigation: std::sync::Arc::new(super::super::common::NavigationNode::new(
                        Vec::new(),
                        Vec::new(),
                        internal.children.to_vec(),
                        parent_id,
                    )),
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

        let mut reader = Self {
            dim,
            distance_fn,
            config,
            nodes,
            root_id,
            navigation_children: std::collections::HashMap::new(),
            navigation_scorer: chroma_index::quantization::PackedFourBitScorer::new(),
            embeddings: DashMap::new(),
            stats: WriterStats::default(),
            posting_list_reader,
            vector_data_reader,
        };
        if navigation_format.is_some() {
            reader.install_navigation_centroids(navigation_codes)?;
        } else {
            reader.prepare_navigation_centroids().await?;
        }
        Ok(reader)
    }

    // =========================================================================
    // Lazy loading
    // =========================================================================

    /// Lazily load a leaf's posting data (IDs and codes) and centroid.
    pub async fn load_node_posting_list(
        &self,
        node_id: NodeId,
    ) -> Result<(), Box<dyn ChromaError>> {
        {
            let node_ref = self.nodes.get(&node_id);
            match node_ref.as_ref().map(|r| r.value()) {
                Some(TreeNode::Leaf(leaf)) if leaf.ids.len() >= leaf.length => return Ok(()),
                Some(TreeNode::Leaf(_)) => {}
                _ => return Ok(()),
            }
        }

        let Some(posting) = self.posting_list_reader.get("", node_id).await? else {
            return Ok(());
        };

        // Load the per-leaf f32 centroid — required by score_leaves for the
        // residual query (r_q = query - centroid), c_norm and c_dot_q.
        // Stored in vector_data under PREFIX_CENTROID by the writer.
        let centroid = self
            .vector_data_reader
            .get(PREFIX_CENTROID, node_id)
            .await?
            .map(|s| s.to_vec())
            .unwrap_or_default();

        if let Some(mut node_ref) = self.nodes.get_mut(&node_id) {
            if let TreeNode::Leaf(leaf) = node_ref.value_mut() {
                if leaf.ids.len() < leaf.length {
                    leaf.ids = posting.ids.to_vec();
                    leaf.codes = posting.codes.to_vec();
                    leaf.length = leaf.ids.len();
                    leaf.centroid = centroid;
                }
            }
        }

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

    /// Clear block pins in both internal blockfile readers. Safe to call
    /// after any data a caller needed has been copied out (e.g. after
    /// `load_all_postings` / `load_all_embeddings`, or between tau rounds
    /// in lazy recall mode). Subsequent reads will refault through the
    /// provider's Foyer cache.
    pub fn clear_loaded_blocks(&self) {
        self.posting_list_reader.clear_loaded_blocks();
        self.vector_data_reader.clear_loaded_blocks();
    }

    /// Count of leaves whose posting data (IDs/codes/centroid) is
    /// currently materialized in `self.nodes`, and the owned-bytes those
    /// fields contribute. Immutable four-bit navigation payloads are excluded;
    /// this counts only fields changed by posting loading and clearing.
    pub fn loaded_postings_stats(&self) -> (usize, u64) {
        let mut leaves_loaded = 0usize;
        let mut bytes = 0u64;
        for entry in self.nodes.iter() {
            if let TreeNode::Leaf(leaf) = entry.value() {
                if !leaf.ids.is_empty() {
                    leaves_loaded += 1;
                    bytes += (leaf.ids.len() * std::mem::size_of::<u32>()) as u64;
                    bytes += leaf.codes.len() as u64;
                    bytes += (leaf.centroid.len() * std::mem::size_of::<f32>()) as u64;
                }
            }
        }
        (leaves_loaded, bytes)
    }

    /// Drop posting data (IDs/codes/centroid) from every leaf in
    /// `self.nodes`, so subsequent `load_node` calls re-fetch from the
    /// posting blockfile. The `length` field is preserved so `load_node`'s
    /// `ids.len() < length` check still triggers a refetch. Tree topology
    /// and four-bit navigation payloads are untouched. Returns
    /// `(leaves_cleared, bytes_freed)`.
    pub fn clear_loaded_postings(&self) -> (usize, u64) {
        let mut leaves_cleared = 0usize;
        let mut bytes_freed = 0u64;
        for mut entry in self.nodes.iter_mut() {
            if let TreeNode::Leaf(leaf) = entry.value_mut() {
                if !leaf.ids.is_empty() {
                    bytes_freed += (leaf.ids.len() * std::mem::size_of::<u32>()) as u64;
                    bytes_freed += leaf.codes.len() as u64;
                    bytes_freed += (leaf.centroid.len() * std::mem::size_of::<f32>()) as u64;
                    leaf.ids = Vec::new();
                    leaf.codes = Vec::new();
                    leaf.centroid = Vec::new();
                    // Keep `length` intact so load_node detects the gap.
                    leaves_cleared += 1;
                }
            }
        }
        (leaves_cleared, bytes_freed)
    }
}
