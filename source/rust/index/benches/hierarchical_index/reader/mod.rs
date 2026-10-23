#![allow(dead_code)]

use std::sync::Arc;

use chroma_blockstore::BlockfileReader;
use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;
use chroma_types::hierarchical_spann::HierarchicalSpannPostingList;
use dashmap::DashMap;

use super::common::{NodeId, TreeNode};
use super::config::HierarchicalSpannConfig;
use super::writer::WriterStats;

mod audit;
pub use audit::{AuditLimits, CoverageAudit};
mod diagnostics;
mod persistance;
mod reader;
mod remote;
mod streaming;
pub use streaming::{StreamingLimits, StreamingMetrics};
pub use remote::{ChildScore, RootDescriptor};

pub struct HierarchicalSpannReader {
    nodes: DashMap<NodeId, TreeNode>,
    root_id: u32,
    embeddings: DashMap<u32, Arc<[f32]>>,
    dim: usize,
    distance_fn: DistanceFunction,
    config: HierarchicalSpannConfig,
    pub stats: WriterStats,
    posting_list_reader: BlockfileReader<'static, u32, HierarchicalSpannPostingList<'static>>,
    scalar_metadata_reader: BlockfileReader<'static, u32, u32>,
    version_reader_lock: tokio::sync::Mutex<()>,
    vector_data_reader: BlockfileReader<'static, u32, &'static [f32]>,
}

#[derive(Debug, thiserror::Error)]
#[error("hierarchical index integrity error: {0}")]
struct ReaderIntegrityError(String);
impl ChromaError for ReaderIntegrityError {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::Internal
    }
}
