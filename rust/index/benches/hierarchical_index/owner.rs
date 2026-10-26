//! A local owner holds the complete tree and executes work through its engine.
//! Remote operations fail explicitly until their coordination protocols exist.

use std::sync::Arc;

use chroma_blockstore::provider::BlockfileProvider;
use chroma_distance::DistanceFunction;
use chroma_error::ChromaError;

use super::common::NodeId;
use super::config::HierarchicalSpannConfig;
use super::writer::persistence::HierarchicalSpannFlusher;
use super::writer::{BufferedAddTiming, HierarchicalSpannIds, HierarchicalSpannWriter};

/// Placement applies to a whole subtree, never to individual vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnedSubtree {
    pub owner_id: u64,
    pub root_id: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum UnsupportedRemoteOperation {
    #[error("cross-owner membership transfer requires an explicit transfer protocol")]
    MembershipTransfer,
    #[error("subtree handoff requires a placement and publication protocol")]
    SubtreeHandoff,
    #[error("shared upper-tree publication requires a routing authority protocol")]
    SharedTreePublication,
    #[error("distributed navigation requires coordinated global candidate selection")]
    DistributedNavigation,
}

/// One machine owns the entire tree, including its current root and postings.
///
/// Phase methods require exclusive access. The engine supplies parallel workers
/// within each phase, without dispatch through a sequential owner thread.
/// The current root is read from the engine, so root replacement cannot leave
/// a second, stale ownership-root record behind.
pub struct LocalSubtreeOwner {
    owner_id: u64,
    engine: HierarchicalSpannWriter,
}

impl LocalSubtreeOwner {
    pub fn new(
        owner_id: u64,
        dim: usize,
        metric: DistanceFunction,
        config: HierarchicalSpannConfig,
    ) -> Self {
        Self {
            owner_id,
            engine: HierarchicalSpannWriter::new(dim, metric, config),
        }
    }

    pub fn ownership(&self) -> OwnedSubtree {
        OwnedSubtree {
            owner_id: self.owner_id,
            root_id: self.engine.current_root_id(),
        }
    }

    /// Expose diagnostics without allowing callers to bypass phase boundaries.
    pub fn engine(&self) -> &HierarchicalSpannWriter {
        &self.engine
    }

    pub fn add_batch_buffered<F: Fn() + Sync>(
        &mut self,
        vectors: &[(u32, Arc<[f32]>)],
        workers: usize,
        on_processed: F,
    ) -> Result<BufferedAddTiming, String> {
        if workers == 0 {
            return Err("an owner requires at least one insertion worker".into());
        }
        self.engine
            .add_batch_buffered(vectors, workers, on_processed)
    }

    pub fn balance_index_parallel(&mut self, workers: usize) -> Result<(), String> {
        if workers == 0 {
            return Err("an owner requires at least one balancing worker".into());
        }
        self.engine.balance_index_parallel(workers)
    }

    pub async fn commit(
        &mut self,
        provider: &BlockfileProvider,
        parent: Option<&HierarchicalSpannIds>,
    ) -> Result<HierarchicalSpannFlusher<'_>, Box<dyn ChromaError>> {
        self.engine.commit(provider, parent).await
    }

    pub async fn open(
        owner_id: u64,
        provider: &BlockfileProvider,
        ids: HierarchicalSpannIds,
        metric: DistanceFunction,
        config: HierarchicalSpannConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        Ok(Self {
            owner_id,
            engine: HierarchicalSpannWriter::open(provider, ids, metric, config).await?,
        })
    }

    pub fn transfer_membership(
        &mut self,
        _vector: u32,
        _destination_owner: u64,
    ) -> Result<(), UnsupportedRemoteOperation> {
        Err(UnsupportedRemoteOperation::MembershipTransfer)
    }

    pub fn handoff_subtree(
        &mut self,
        _root: NodeId,
        _destination_owner: u64,
    ) -> Result<(), UnsupportedRemoteOperation> {
        Err(UnsupportedRemoteOperation::SubtreeHandoff)
    }

    pub fn publish_shared_tree(&mut self) -> Result<(), UnsupportedRemoteOperation> {
        Err(UnsupportedRemoteOperation::SharedTreePublication)
    }

    pub fn navigate_remote(
        &self,
        _query: &[f32],
    ) -> Result<Vec<NodeId>, UnsupportedRemoteOperation> {
        Err(UnsupportedRemoteOperation::DistributedNavigation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ownership_follows_root_replacement_and_remote_operations_preserve_local_state() {
        let config = HierarchicalSpannConfig {
            branching_factor: 4,
            split_threshold: 8,
            merge_threshold: 1,
            ..Default::default()
        };
        let mut owner = LocalSubtreeOwner::new(17, 8, DistanceFunction::Euclidean, config);
        let initial_root = owner.ownership().root_id;
        let inputs: Vec<_> = (0..32_u32)
            .map(|id| {
                (
                    id,
                    Arc::<[f32]>::from(
                        (0..8)
                            .map(|d| (id * 17 + d * 13) as f32)
                            .collect::<Vec<_>>(),
                    ),
                )
            })
            .collect();
        owner.add_batch_buffered(&inputs, 4, || {}).unwrap();
        owner.balance_index_parallel(4).unwrap();
        assert_eq!(owner.ownership().owner_id, 17);
        assert_eq!(owner.ownership().root_id, owner.engine().current_root_id());
        assert_ne!(owner.ownership().root_id, initial_root);
        let before = owner.engine().root_reachable_valid_ids().unwrap();
        assert_eq!(before.len(), inputs.len());
        assert_eq!(
            owner.transfer_membership(0, 18),
            Err(UnsupportedRemoteOperation::MembershipTransfer)
        );
        assert_eq!(
            owner.handoff_subtree(owner.ownership().root_id, 18),
            Err(UnsupportedRemoteOperation::SubtreeHandoff)
        );
        assert_eq!(
            owner.publish_shared_tree(),
            Err(UnsupportedRemoteOperation::SharedTreePublication)
        );
        assert_eq!(
            owner.navigate_remote(&[0.0; 8]),
            Err(UnsupportedRemoteOperation::DistributedNavigation)
        );
        assert_eq!(owner.engine().root_reachable_valid_ids().unwrap(), before);
        assert!(owner.add_batch_buffered(&[], 0, || {}).is_err());
        assert!(owner.balance_index_parallel(0).is_err());
    }
}
