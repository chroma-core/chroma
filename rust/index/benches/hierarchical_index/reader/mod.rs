#![allow(dead_code)]

use std::sync::Arc;

use chroma_blockstore::BlockfileReader;
use chroma_distance::DistanceFunction;
use chroma_types::hierarchical_spann::HierarchicalSpannPostingList;
use dashmap::DashMap;

use super::writer::{HierarchicalSpannConfig, NodeId, TreeNode, WriterStats};

mod persistance;
mod reader;
mod diagnostics;

pub struct HierarchicalSpannReader {
    nodes: DashMap<NodeId, TreeNode>,
    root_id: u32,
    embeddings: DashMap<u32, Arc<[f32]>>,
    dim: usize,
    distance_fn: DistanceFunction,
    config: HierarchicalSpannConfig,
    pub stats: WriterStats,
    posting_list_reader: BlockfileReader<'static, u32, HierarchicalSpannPostingList<'static>>,
    vector_data_reader: BlockfileReader<'static, u32, &'static [f32]>,
}
