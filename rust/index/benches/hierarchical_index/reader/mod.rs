#![allow(dead_code)]

use std::sync::Arc;

use chroma_blockstore::BlockfileReader;
use chroma_distance::DistanceFunction;
use chroma_types::hierarchical_spann::HierarchicalSpannPostingList;
use dashmap::DashMap;

use super::common::{NodeId, TreeNode};
use super::config::HierarchicalSpannConfig;
use super::writer::WriterStats;

mod diagnostics;
mod navigation_precision;
pub use navigation_precision::NavigationPrecision;
mod persistance;
// The implementation module shares the public component name.
#[allow(clippy::module_inception)]
mod reader;

pub struct HierarchicalSpannReader {
    nodes: DashMap<NodeId, TreeNode>,
    navigation_precision: NavigationPrecision,
    four_bit_centroids: std::collections::HashMap<NodeId, Vec<u8>>,
    prepared_four_bit_centroids:
        std::collections::HashMap<NodeId, chroma_index::quantization::PreparedFourBit>,
    four_bit_kernels: chroma_index::quantization::FourBitKernels,
    root_id: u32,
    embeddings: DashMap<u32, Arc<[f32]>>,
    dim: usize,
    distance_fn: DistanceFunction,
    config: HierarchicalSpannConfig,
    pub stats: WriterStats,
    posting_list_reader: BlockfileReader<'static, u32, HierarchicalSpannPostingList<'static>>,
    vector_data_reader: BlockfileReader<'static, u32, &'static [f32]>,
}
