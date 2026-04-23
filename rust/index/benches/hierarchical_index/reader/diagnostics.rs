use super::super::common::{NodeId, TreeNode};
use super::HierarchicalSpannReader;
use chroma_index::quantization::Code;

impl HierarchicalSpannReader {
    /// Estimated in-memory footprint of the reader's owned data structures.
    /// Only counts data the reader explicitly holds (tree nodes, posting
    /// data, and reranking embeddings). Excludes blockfile reader caches
    /// (which are managed separately by the blockfile provider).
    pub fn memory_usage(&self) -> ReaderMemoryUsage {
        let dim = self.dim;
        let code_byte_len = Code::<1, Vec<u8>>::size(dim) as u64;
        let f32_centroid_bytes = (dim as u64) * 4;

        let mut leaf_count: u64 = 0;
        let mut internal_count: u64 = 0;
        let mut tree_bytes: u64 = 0;
        let mut centroid_bytes: u64 = 0;
        let mut posting_entries: u64 = 0;
        let mut posting_bytes: u64 = 0;

        for entry in self.nodes.iter() {
            match entry.value() {
                TreeNode::Leaf(leaf) => {
                    leaf_count += 1;
                    tree_bytes += leaf.centroid_code.len() as u64;
                    if !leaf.centroid.is_empty() {
                        centroid_bytes += f32_centroid_bytes;
                    }
                    let n = leaf.ids.len() as u64;
                    posting_entries += n;
                    // ids (u32) + per-vector codes + versions (u8)
                    posting_bytes += n.saturating_mul(4 + code_byte_len + 1);
                }
                TreeNode::Internal(internal) => {
                    internal_count += 1;
                    tree_bytes += internal.centroid_code.len() as u64;
                    if !internal.centroid.is_empty() {
                        centroid_bytes += f32_centroid_bytes;
                    }
                    // children Vec<u32>
                    tree_bytes += (internal.children.len() as u64).saturating_mul(4);
                }
            }
        }

        let embedding_count = self.embeddings.len() as u64;
        let embedding_bytes = embedding_count.saturating_mul(f32_centroid_bytes);

        ReaderMemoryUsage {
            dim,
            leaf_count,
            internal_count,
            tree_bytes,
            centroid_bytes,
            posting_entries,
            posting_bytes,
            embedding_count,
            embedding_bytes,
        }
    }

    pub fn level_node_counts(&self) -> Vec<usize> {
        let root = self.root_id;
        let depth = self.depth_of(root);
        let mut counts = vec![0usize; depth];
        let mut queue: Vec<(NodeId, usize)> = vec![(root, 0)];
        while let Some((node_id, level)) = queue.pop() {
            if level >= depth {
                continue;
            }
            counts[level] += 1;
            if let Some(node_ref) = self.nodes.get(&node_id) {
                if let TreeNode::Internal(internal) = node_ref.value() {
                    let children: Vec<NodeId> = internal.children.clone();
                    drop(node_ref);
                    for child_id in children {
                        queue.push((child_id, level + 1));
                    }
                }
            }
        }
        counts
    }

    fn depth_of(&self, node_id: NodeId) -> usize {
        let Some(node_ref) = self.nodes.get(&node_id) else {
            return 0;
        };
        match node_ref.value() {
            TreeNode::Leaf(_) => 1,
            TreeNode::Internal(internal) => {
                let children: Vec<NodeId> = internal.children.clone();
                drop(node_ref);
                1 + children
                    .iter()
                    .map(|&c| self.depth_of(c))
                    .max()
                    .unwrap_or(0)
            }
        }
    }

    pub fn depth(&self) -> usize {
        self.depth_of(self.root_id)
    }
}

/// In-memory footprint breakdown of a `HierarchicalSpannReader`.
/// All byte counts are estimates of the *payload* size of the owned
/// `Vec`/`DashMap` contents and exclude per-allocation overhead.
#[derive(Debug, Clone, Copy)]
pub struct ReaderMemoryUsage {
    pub dim: usize,
    pub leaf_count: u64,
    pub internal_count: u64,
    /// `centroid_code` (1-bit RaBitQ) for every node, plus `children` Vec
    /// payloads on internal nodes.
    pub tree_bytes: u64,
    /// f32 centroids loaded onto leaves/internals (only populated for
    /// leaves whose posting data has been materialized via `load_node`).
    pub centroid_bytes: u64,
    /// Sum of `leaf.ids.len()` across all materialized leaves.
    pub posting_entries: u64,
    /// `posting_entries * (4 [id] + code_size + 1 [version])`.
    pub posting_bytes: u64,
    /// Number of full-precision embeddings in the rerank cache.
    pub embedding_count: u64,
    /// `embedding_count * dim * 4`.
    pub embedding_bytes: u64,
}

impl ReaderMemoryUsage {
    pub fn total_bytes(&self) -> u64 {
        self.tree_bytes
            .saturating_add(self.centroid_bytes)
            .saturating_add(self.posting_bytes)
            .saturating_add(self.embedding_bytes)
    }
}
