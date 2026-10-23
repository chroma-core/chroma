//! Full vectors stored with their owner in the index's own storage, as an
//! Arrow blockfile. Each vector is keyed by its leaf (the blockfile prefix)
//! and its position in that leaf's saved posting list (the u32 key), so the
//! key space is unbounded by corpus size and a leaf's vectors share blocks.
use serde::{Deserialize, Serialize};

/// Locates one owner's full-vector blockfile in a saved generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorStoreRef {
    /// Layout identifier; readers reject layouts they do not know.
    pub kind: String,
    /// The blockfile's root ID in the blockfile provider.
    pub blockfile_id: String,
    /// Storage path prefix the blockfile's blocks and root live under.
    pub prefix_path: String,
    pub owner: u64,
    pub rows: u64,
    pub dimensions: usize,
    pub max_block_size_bytes: usize,
}

pub const LEAF_ORDINAL_LAYOUT: &str = "arrow-blockfile-leaf-ordinal-v1";

/// The blockfile prefix for a leaf: fixed-width lowercase hex, so string order
/// equals numeric order and an ordered writer can append leaves in ID order.
pub fn leaf_prefix(leaf: u128) -> String {
    format!("{leaf:032x}")
}
