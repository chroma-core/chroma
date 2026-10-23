//! Full append-only coverage validation without retaining postings or raw embeddings.
use super::super::{common::TreeNode, persistance::PREFIX_VERSION, writer::DELETED_BIT};
use super::{HierarchicalSpannReader, ReaderIntegrityError};
use chroma_error::ChromaError;
use chroma_index::quantization::Code;
use futures::stream::{self, StreamExt, TryStreamExt};
use serde::Serialize;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct AuditLimits {
    pub max_bitmap_bytes: u64,
    pub max_nodes: usize,
    pub max_leaf_entries: usize,
}
impl Default for AuditLimits {
    fn default() -> Self {
        Self {
            max_bitmap_bytes: 256 * 1024 * 1024,
            max_nodes: 1_000_000,
            max_leaf_entries: 262_144,
        }
    }
}
#[derive(Debug, Serialize)]
pub struct CoverageAudit {
    pub expected_rows: u64,
    pub unique_live_rows: u64,
    pub physical_entries: u64,
    pub duplicate_current_entries: u64,
    pub stale_entries: u64,
    pub version_reads: u64,
    pub stored_nodes: usize,
    pub reachable_nodes: usize,
    pub unreachable_nodes: usize,
    pub reachable_leaves: usize,
    pub peak_leaf_entries: usize,
    pub bitmap_bytes: u64,
    pub raw_embedding_reads: u64,
    pub limits: AuditLimits,
}
fn invalid(message: impl Into<String>) -> Box<dyn ChromaError> {
    Box::new(ReaderIntegrityError(message.into()))
}
impl HierarchicalSpannReader {
    /// Every expected local row must be reachable at its authoritative version.
    /// This append-only contract expects dense IDs 0..expected_rows, all live.
    /// One leaf's ID/version arrays and a bounded batch are retained at a time.
    pub async fn audit_reachable_ids(
        &self,
        expected_rows: u64,
        limits: AuditLimits,
    ) -> Result<CoverageAudit, Box<dyn ChromaError>> {
        // Use a dedicated immutable reader; posting pins cannot be cleared
        // while another operation on this reader still borrows a posting.
        let result = self.audit_reachable_ids_inner(expected_rows, limits).await;
        self.posting_list_reader.clear_loaded_blocks();
        self.scalar_metadata_reader.clear_loaded_blocks();
        result
    }
    async fn audit_reachable_ids_inner(
        &self,
        expected_rows: u64,
        limits: AuditLimits,
    ) -> Result<CoverageAudit, Box<dyn ChromaError>> {
        if expected_rows > u64::from(u32::MAX) + 1 {
            return Err(invalid("audit row count exceeds local u32 ID range"));
        }
        if self.nodes.len() > limits.max_nodes {
            return Err(invalid("audit metadata node limit exceeded"));
        }
        let bitmap_bytes = expected_rows
            .checked_add(7)
            .ok_or_else(|| invalid("audit bitmap size overflow"))?
            / 8;
        if bitmap_bytes > limits.max_bitmap_bytes {
            return Err(invalid("audit bitmap byte limit exceeded"));
        }
        let size = usize::try_from(bitmap_bytes)
            .map_err(|_| invalid("audit bitmap does not fit address space"))?;
        let mut bitmap = Vec::new();
        bitmap
            .try_reserve_exact(size)
            .map_err(|_| invalid("cannot allocate audit bitmap within requested limits"))?;
        bitmap.resize(size, 0u8);
        let mut discovered = HashSet::new();
        let mut pending = Vec::new();
        if limits.max_nodes == 0 {
            return Err(invalid("audit metadata node limit must permit the root"));
        }
        discovered.insert(self.root_id);
        pending.push((self.root_id, None));
        let mut report = CoverageAudit {
            expected_rows,
            unique_live_rows: 0,
            physical_entries: 0,
            duplicate_current_entries: 0,
            stale_entries: 0,
            version_reads: 0,
            stored_nodes: self.nodes.len(),
            reachable_nodes: 0,
            unreachable_nodes: 0,
            reachable_leaves: 0,
            peak_leaf_entries: 0,
            bitmap_bytes,
            raw_embedding_reads: 0,
            limits,
        };
        let _version_guard = self.version_reader_lock.lock().await;
        while let Some((node_id, expected_parent)) = pending.pop() {
            let (children, declared_entries) = {
                let node = self
                    .nodes
                    .get(&node_id)
                    .ok_or_else(|| invalid(format!("audit missing reachable node {node_id}")))?;
                match node.value() {
                    TreeNode::Internal(internal) => {
                        if internal.parent_id != expected_parent {
                            return Err(invalid(format!(
                                "audit parent pointer mismatch at node {node_id}"
                            )));
                        }
                        if internal.children.is_empty() {
                            return Err(invalid(format!("audit empty internal node {node_id}")));
                        }
                        if internal.children.len() > limits.max_nodes.saturating_sub(1) {
                            return Err(invalid("audit child-reference metadata limit exceeded"));
                        }
                        (Some(internal.children.clone()), 0)
                    }
                    TreeNode::Leaf(leaf) => {
                        if leaf.parent_id != expected_parent {
                            return Err(invalid(format!(
                                "audit parent pointer mismatch at leaf {node_id}"
                            )));
                        }
                        (None, leaf.length)
                    }
                }
            };
            if let Some(children) = children {
                for child in children {
                    if !discovered.insert(child) {
                        return Err(invalid(format!("audit repeated reachable child {child}")));
                    }
                    if discovered.len() > limits.max_nodes {
                        return Err(invalid("audit reachable metadata node limit exceeded"));
                    }
                    pending.push((child, Some(node_id)));
                }
                continue;
            }
            report.reachable_leaves += 1;
            if declared_entries > limits.max_leaf_entries {
                return Err(invalid(format!(
                    "audit leaf {node_id} exceeds declared entry limit"
                )));
            }
            // Copy only IDs and versions. The code payload is validated by length,
            // then released before scalar reads; no raw vector data is fetched.
            let owned = async {
                let Some(posting) = self.posting_list_reader.get("", node_id).await? else {
                    if declared_entries == 0 {
                        return Ok((Vec::new(), Vec::new()));
                    }
                    return Err(invalid(format!("audit missing posting list {node_id}")));
                };
                let n = posting.ids.len();
                if n > limits.max_leaf_entries {
                    return Err(invalid(format!(
                        "audit leaf {node_id} exceeds physical entry limit"
                    )));
                }
                if posting.versions.len() != n
                    || posting.codes.len()
                        != n.checked_mul(Code::<1, Vec<u8>>::size(self.dim))
                            .ok_or_else(|| invalid("audit code length overflow"))?
                {
                    return Err(invalid(format!("audit malformed posting list {node_id}")));
                }
                Ok::<_, Box<dyn ChromaError>>((posting.ids.to_vec(), posting.versions.to_vec()))
            }
            .await;
            self.posting_list_reader.clear_loaded_blocks();
            let (ids, versions) = owned?;
            report.peak_leaf_entries = report.peak_leaf_entries.max(ids.len());
            report.physical_entries += ids.len() as u64;
            for start in (0..ids.len()).step_by(64) {
                let end = (start + 64).min(ids.len());
                for &id in &ids[start..end] {
                    if u64::from(id) >= expected_rows {
                        return Err(invalid(format!(
                            "audit local ID {id} is outside expected row range"
                        )));
                    }
                }
                let values = stream::iter(start..end)
                    .map(|i| {
                        let ids = &ids;
                        async move {
                            self.scalar_metadata_reader
                                .get(PREFIX_VERSION, ids[i])
                                .await
                                .map(|version| (i, version))
                        }
                    })
                    .buffer_unordered(16)
                    .try_collect::<Vec<_>>()
                    .await;
                self.scalar_metadata_reader.clear_loaded_blocks();
                for (i, current) in values? {
                    report.version_reads += 1;
                    let current = current.ok_or_else(|| {
                        invalid(format!(
                            "audit missing authoritative version for {}",
                            ids[i]
                        ))
                    })?;
                    if current > u8::MAX as u32 {
                        return Err(invalid("audit authoritative version is outside u8 range"));
                    }
                    let current = current as u8;
                    if current & DELETED_BIT != 0 || current != versions[i] {
                        report.stale_entries += 1;
                        continue;
                    }
                    let index = ids[i] as usize;
                    let bit = 1u8 << (index % 8);
                    if bitmap[index / 8] & bit != 0 {
                        report.duplicate_current_entries += 1;
                    } else {
                        bitmap[index / 8] |= bit;
                        report.unique_live_rows += 1;
                    }
                }
            }
        }
        report.reachable_nodes = discovered.len();
        report.unreachable_nodes = report.stored_nodes.saturating_sub(report.reachable_nodes);
        if report.unique_live_rows != expected_rows {
            let missing = (0..expected_rows)
                .find(|&id| bitmap[id as usize / 8] & (1u8 << (id as usize % 8)) == 0)
                .unwrap();
            return Err(invalid(format!(
                "audit missing live local ID {missing}; found {} of {expected_rows} expected rows",
                report.unique_live_rows
            )));
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::{config::HierarchicalSpannConfig, writer::HierarchicalSpannWriter};
    use super::*;
    use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
    use chroma_cache::new_cache_for_test;
    use chroma_distance::DistanceFunction;
    use chroma_storage::{local::LocalStorage, Storage};
    use std::sync::atomic::Ordering;
    fn provider(path: &std::path::Path) -> BlockfileProvider {
        BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
            Storage::Local(LocalStorage::new(path.to_str().unwrap())),
            3 * 1024 * 1024,
            new_cache_for_test(),
            new_cache_for_test(),
            16,
        ))
    }
    async fn open(
        writer: &HierarchicalSpannWriter,
        p: &BlockfileProvider,
        config: HierarchicalSpannConfig,
    ) -> HierarchicalSpannReader {
        let ids = writer.commit(p, None).await.unwrap().flush().await.unwrap();
        HierarchicalSpannReader::open(p, ids, DistanceFunction::Euclidean, config)
            .await
            .unwrap()
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn audit_covers_more_than_100k_without_materializing_postings_or_embeddings() {
        let directory = tempfile::tempdir().unwrap();
        let p = provider(directory.path());
        let config = HierarchicalSpannConfig {
            split_threshold: 100_010,
            merge_threshold: 0,
            ..Default::default()
        };
        let writer = HierarchicalSpannWriter::new(4, DistanceFunction::Euclidean, config.clone());
        for id in 0..100_001 {
            writer.add(id, &[(id % 17) as f32, 1.0, 0.0, 0.0]);
        }
        let reader = open(&writer, &p, config).await;
        let report = reader
            .audit_reachable_ids(100_001, AuditLimits::default())
            .await
            .unwrap();
        assert_eq!(report.unique_live_rows, 100_001);
        assert_eq!(report.bitmap_bytes, 12_501);
        assert_eq!(report.version_reads, 100_001);
        assert_eq!(report.raw_embedding_reads, 0);
        assert_eq!(report.peak_leaf_entries, 100_001);
        assert_eq!(reader.loaded_postings_stats(), (0, 0));
        assert_eq!(reader.posting_list_reader.loaded_blocks_stats().0, 0);
        assert_eq!(reader.scalar_metadata_reader.loaded_blocks_stats().0, 0);
        assert_eq!(reader.vector_data_reader.loaded_blocks_stats().0, 0);
        assert!(reader.embeddings.is_empty());
        assert!(reader
            .audit_reachable_ids(
                100_001,
                AuditLimits {
                    max_bitmap_bytes: 12_500,
                    ..Default::default()
                }
            )
            .await
            .is_err());
        assert!(reader
            .audit_reachable_ids(
                100_001,
                AuditLimits {
                    max_nodes: 0,
                    ..Default::default()
                }
            )
            .await
            .is_err());
        assert!(reader
            .audit_reachable_ids(
                100_001,
                AuditLimits {
                    max_leaf_entries: 100_000,
                    ..Default::default()
                }
            )
            .await
            .is_err());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn audit_accepts_duplicate_current_copies_but_rejects_missing_stale_and_out_of_range_ids()
    {
        for scenario in ["duplicate", "missing", "stale", "deleted", "range"] {
            let directory = tempfile::tempdir().unwrap();
            let p = provider(directory.path());
            let config = HierarchicalSpannConfig::default();
            let writer =
                HierarchicalSpannWriter::new(4, DistanceFunction::Euclidean, config.clone());
            writer.add(0, &[1.0, 0.0, 0.0, 0.0]);
            let expected = match scenario {
                "duplicate" => {
                    let root = writer.root_id.load(Ordering::Relaxed);
                    let mut node = writer.nodes.get_mut(&root).unwrap();
                    let TreeNode::Leaf(leaf) = node.value_mut() else {
                        panic!("expected root leaf")
                    };
                    let code = leaf.codes.clone();
                    leaf.ids.push(0);
                    leaf.versions.push(1);
                    leaf.codes.extend_from_slice(&code);
                    leaf.length = 2;
                    1
                }
                "missing" => {
                    writer.add(2, &[2.0, 0.0, 0.0, 0.0]);
                    3
                }
                "range" => {
                    writer.add(2, &[2.0, 0.0, 0.0, 0.0]);
                    2
                }
                "stale" => {
                    writer.add(1, &[2.0, 0.0, 0.0, 0.0]);
                    writer.versions.insert(1, 2);
                    writer.dirty_versions.insert(1);
                    2
                }
                "deleted" => {
                    writer.add(1, &[2.0, 0.0, 0.0, 0.0]);
                    writer.delete(1);
                    2
                }
                _ => unreachable!(),
            };
            let reader = open(&writer, &p, config).await;
            let audit = reader
                .audit_reachable_ids(expected, AuditLimits::default())
                .await;
            if scenario == "duplicate" {
                let report = audit.unwrap();
                assert_eq!(report.unique_live_rows, 1);
                assert_eq!(report.duplicate_current_entries, 1);
                assert_eq!(report.physical_entries, 2);
            } else {
                assert!(audit.is_err(), "scenario {scenario} must fail");
            }
            assert_eq!(reader.loaded_postings_stats(), (0, 0));
            assert_eq!(reader.vector_data_reader.loaded_blocks_stats().0, 0);
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn audit_rejects_missing_repeated_children_and_inconsistent_parent_pointers() {
        use super::super::super::common::{InternalNode, LeafNode};
        for scenario in ["missing", "repeated", "parent"] {
            let directory = tempfile::tempdir().unwrap();
            let p = provider(directory.path());
            let config = HierarchicalSpannConfig::default();
            let writer =
                HierarchicalSpannWriter::new(4, DistanceFunction::Euclidean, config.clone());
            writer.add(0, &[1.0, 0.0, 0.0, 0.0]);
            let reader = open(&writer, &p, config).await;
            let children = if scenario == "repeated" {
                vec![1, 1]
            } else {
                vec![1]
            };
            reader.nodes.insert(
                reader.root_id,
                TreeNode::Internal(InternalNode {
                    centroid: Vec::new(),
                    centroid_code: Vec::new(),
                    children,
                    parent_id: None,
                }),
            );
            if scenario != "missing" {
                reader.nodes.insert(
                    1,
                    TreeNode::Leaf(LeafNode {
                        centroid: Vec::new(),
                        centroid_code: Vec::new(),
                        ids: Vec::new(),
                        versions: Vec::new(),
                        codes: Vec::new(),
                        parent_id: if scenario == "parent" {
                            None
                        } else {
                            Some(reader.root_id)
                        },
                        length: 0,
                    }),
                );
            }
            assert!(
                reader
                    .audit_reachable_ids(1, AuditLimits::default())
                    .await
                    .is_err(),
                "scenario {scenario} must fail"
            );
            assert_eq!(reader.vector_data_reader.loaded_blocks_stats().0, 0);
        }
    }
}
