//! Saved ground-truth memberships and row-cost oracles, without raw embeddings.
use super::super::{
    common::{NodeId, TreeNode},
    persistance::PREFIX_VERSION,
    writer::DELETED_BIT,
};
use super::{HierarchicalSpannReader, ReaderIntegrityError};
use chroma_error::ChromaError;
use chroma_index::quantization::Code;
use futures::stream::{self, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

type Error = Box<dyn ChromaError>;
fn invalid(s: impl Into<String>) -> Error {
    Box::new(ReaderIntegrityError(s.into()))
}
pub const MAX_GT: usize = 1600;
#[derive(Clone, Copy, Debug, Serialize)]
pub struct QualityLimits {
    pub max_nodes: usize,
    pub max_leaf_entries: usize,
    pub max_depth: usize,
    pub max_report_leaves: usize,
}
impl Default for QualityLimits {
    fn default() -> Self {
        Self {
            max_nodes: 1_000_000,
            max_leaf_entries: 262_144,
            max_depth: 128,
            max_report_leaves: 4096,
        }
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct GroundTruthLeaf {
    pub leaf_id: u32,
    pub path: Vec<u32>,
    pub physical_rows: u64,
    pub live_rows: u64,
    pub physical_code_bytes: u64,
    pub live_code_bytes: u64,
    pub gt_ids: Vec<u64>,
}
#[derive(Clone, Debug, Serialize)]
pub struct OwnerGroundTruth {
    pub owner_id: String,
    pub declared_local_rows: u64,
    pub reachable_nodes: usize,
    pub stored_nodes: usize,
    pub reachable_leaves: usize,
    pub physical_rows: u64,
    pub live_rows: u64,
    pub stale_rows: u64,
    pub deleted_rows: u64,
    pub version_reads: u64,
    pub peak_posting_rows: usize,
    pub expected_gt: usize,
    pub leaves: Vec<GroundTruthLeaf>,
    pub raw_embedding_reads: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroundTruthQuery {
    pub query_id: String,
    pub expected: usize,
    pub ids: Vec<u64>,
}

pub fn checked_gt(queries: &[GroundTruthQuery], global_rows: u64) -> Result<BTreeSet<u64>, String> {
    if queries.is_empty() || queries.len() > 16 || global_rows == 0 {
        return Err("quality query/count limit".into());
    }
    let mut names = BTreeSet::new();
    let mut union = BTreeSet::new();
    for q in queries {
        if q.query_id.is_empty()
            || q.query_id.len() > 256
            || !names.insert(&q.query_id)
            || q.expected == 0
            || q.expected > 100
            || q.ids.len() != q.expected
        {
            return Err("expected unique ground-truth cardinality/query identity differs".into());
        }
        let mut ids = BTreeSet::new();
        for &id in &q.ids {
            if id >= global_rows || !ids.insert(id) {
                return Err("ground-truth ID duplicate/outside corpus".into());
            }
            union.insert(id);
        }
    }
    if union.len() > MAX_GT {
        return Err("ground-truth union limit".into());
    }
    Ok(union)
}

impl HierarchicalSpannReader {
    /// Materialize memberships from a dedicated saved reader. Only GT IDs persist
    /// in the result; one posting borrow and 64 owned versions are live at a time.
    /// This proves current-copy uniqueness for GT, not for every other corpus ID.
    pub async fn ground_truth_memberships(
        &mut self,
        owner: &str,
        local_rows: u64,
        targets: &BTreeMap<u32, u64>,
        limits: QualityLimits,
    ) -> Result<OwnerGroundTruth, Error> {
        if !self.posting_list_reader.has_exclusive_loaded_blocks()
            || !self.scalar_metadata_reader.has_exclusive_loaded_blocks()
        {
            return Err(invalid("quality requires unique dedicated saved readers"));
        }
        let result = self
            .ground_truth_memberships_inner(owner, local_rows, targets, limits)
            .await;
        // The inner future and its posting/scalar borrows have ended.
        self.posting_list_reader.clear_loaded_blocks();
        self.scalar_metadata_reader.clear_loaded_blocks();
        result
    }
    async fn ground_truth_memberships_inner(
        &self,
        owner: &str,
        local_rows: u64,
        targets: &BTreeMap<u32, u64>,
        limits: QualityLimits,
    ) -> Result<OwnerGroundTruth, Error> {
        if owner.is_empty()
            || owner.len() > 256
            || local_rows == 0
            || local_rows > u64::from(u32::MAX) + 1
            || targets.len() > MAX_GT
            || targets.keys().any(|k| u64::from(*k) >= local_rows)
            || targets.values().collect::<BTreeSet<_>>().len() != targets.len()
        {
            return Err(invalid("quality owner/GT map admission differs"));
        }
        if limits.max_nodes == 0
            || limits.max_nodes > 1_000_000
            || self.nodes.len() > limits.max_nodes
            || limits.max_leaf_entries == 0
            || limits.max_leaf_entries > 262_144
            || limits.max_depth == 0
            || limits.max_depth > 128
            || limits.max_report_leaves == 0
            || limits.max_report_leaves > 4096
        {
            return Err(invalid("quality metadata/posting/depth cap"));
        }
        let mut report = OwnerGroundTruth {
            owner_id: owner.into(),
            declared_local_rows: local_rows,
            reachable_nodes: 0,
            stored_nodes: self.nodes.len(),
            reachable_leaves: 0,
            physical_rows: 0,
            live_rows: 0,
            stale_rows: 0,
            deleted_rows: 0,
            version_reads: 0,
            peak_posting_rows: 0,
            expected_gt: targets.len(),
            leaves: Vec::new(),
            raw_embedding_reads: 0,
        };
        let mut discovered = BTreeSet::new();
        discovered.insert(self.root_id);
        let mut pending = vec![(self.root_id, None, 1usize)];
        let mut found = BTreeSet::new();
        let _guard = self.version_reader_lock.lock().await;
        while let Some((node_id, parent, depth)) = pending.pop() {
            let (children, declared) = {
                let node = self
                    .nodes
                    .get(&node_id)
                    .ok_or_else(|| invalid("quality reachable node missing"))?;
                match node.value() {
                    TreeNode::Internal(n) => {
                        if n.parent_id != parent
                            || n.children.is_empty()
                            || n.children.len() > limits.max_nodes
                        {
                            return Err(invalid("quality internal parent/children differ"));
                        }
                        (Some(n.children.clone()), 0)
                    }
                    TreeNode::Leaf(n) => {
                        if n.parent_id != parent {
                            return Err(invalid("quality leaf parent differs"));
                        }
                        (None, n.length)
                    }
                }
            };
            if let Some(children) = children {
                if depth >= limits.max_depth {
                    return Err(invalid("quality depth cap"));
                }
                for child in children {
                    if !discovered.insert(child) || discovered.len() > limits.max_nodes {
                        return Err(invalid("quality repeated child/metadata cap"));
                    }
                    pending.push((child, Some(node_id), depth + 1));
                }
                continue;
            }
            report.reachable_leaves += 1;
            if report.reachable_leaves > limits.max_report_leaves {
                return Err(invalid("quality numeric leaf report cap"));
            }
            if declared > limits.max_leaf_entries {
                return Err(invalid("quality declared leaf cap"));
            }
            let leaf = async {
                let Some(posting) = self.posting_list_reader.get("", node_id).await? else {
                    if self.persisted_empty_leaves.binary_search(&node_id).is_ok() {
                        return Ok(Some(GroundTruthLeaf {
                            leaf_id: node_id,
                            path: Vec::new(),
                            physical_rows: 0,
                            live_rows: 0,
                            physical_code_bytes: 0,
                            live_code_bytes: 0,
                            gt_ids: Vec::new(),
                        }));
                    }
                    return Err(invalid("quality missing saved posting"));
                };
                let n = posting.ids.len();
                let code_size = Code::<1>::size(self.dim);
                if n > limits.max_leaf_entries
                    || posting.versions.len() != n
                    || n.checked_mul(code_size) != Some(posting.codes.len())
                {
                    return Err(invalid("quality malformed/oversized saved posting"));
                }
                report.peak_posting_rows = report.peak_posting_rows.max(n);
                let mut row = GroundTruthLeaf {
                    leaf_id: node_id,
                    path: Vec::new(),
                    physical_rows: n as u64,
                    live_rows: 0,
                    physical_code_bytes: posting.codes.len() as u64,
                    live_code_bytes: 0,
                    gt_ids: Vec::new(),
                };
                report.physical_rows = report
                    .physical_rows
                    .checked_add(n as u64)
                    .ok_or_else(|| invalid("quality physical count overflow"))?;
                for start in (0..n).step_by(64) {
                    let end = (start + 64).min(n);
                    if posting.ids[start..end]
                        .iter()
                        .any(|id| u64::from(*id) >= local_rows)
                    {
                        return Err(invalid("quality posting ID outside local map"));
                    }
                    let posting_ids = posting.ids;
                    let values = stream::iter(start..end)
                        .map(|i| async move {
                            self.scalar_metadata_reader
                                .get(PREFIX_VERSION, posting_ids[i])
                                .await
                                .map(|v| (i, v))
                        })
                        .buffer_unordered(16)
                        .try_collect::<Vec<_>>()
                        .await;
                    self.scalar_metadata_reader.clear_loaded_blocks();
                    for (i, current) in values? {
                        report.version_reads += 1;
                        let current = current
                            .ok_or_else(|| invalid("quality missing authoritative version"))?;
                        let current = u8::try_from(current)
                            .map_err(|_| invalid("quality authoritative version outside u8"))?;
                        if current & DELETED_BIT != 0 {
                            report.deleted_rows += 1;
                            if targets.contains_key(&posting.ids[i]) {
                                return Err(invalid("quality ground-truth ID is deleted"));
                            }
                            continue;
                        }
                        if current != posting.versions[i] {
                            report.stale_rows += 1;
                            continue;
                        }
                        row.live_rows += 1;
                        report.live_rows += 1;
                        if let Some(&global) = targets.get(&posting.ids[i]) {
                            if !found.insert(global) {
                                return Err(invalid(
                                    "quality duplicate current ground-truth membership",
                                ));
                            }
                            row.gt_ids.push(global);
                        }
                    }
                }
                row.live_code_bytes = row
                    .live_rows
                    .checked_mul(code_size as u64)
                    .ok_or_else(|| invalid("quality code count overflow"))?;
                Ok::<_, Error>(Some(row))
            }
            .await;
            self.posting_list_reader.clear_loaded_blocks();
            if let Some(mut row) = leaf? {
                if !row.gt_ids.is_empty() {
                    row.gt_ids.sort_unstable();
                    let mut id = Some(node_id);
                    while let Some(current) = id {
                        if row.path.len() >= limits.max_depth {
                            return Err(invalid("quality GT path cap"));
                        }
                        row.path.push(current);
                        let node = self
                            .nodes
                            .get(&current)
                            .ok_or_else(|| invalid("quality path missing"))?;
                        id = match node.value() {
                            TreeNode::Leaf(n) => n.parent_id,
                            TreeNode::Internal(n) => n.parent_id,
                        };
                    }
                    row.path.reverse();
                }
                report.leaves.push(row);
            }
        }
        report.reachable_nodes = discovered.len();
        if found.len() != targets.len() {
            return Err(invalid("quality missing live ground-truth coverage"));
        }
        report.leaves.sort_by_key(|l| l.leaf_id);
        Ok(report)
    }
}

#[derive(Debug, Serialize)]
pub struct CoverageOracle {
    pub target_hits: usize,
    pub minimum_leaves: usize,
    pub minimum_live_rows: u64,
}
#[derive(Debug, Serialize)]
pub struct QueryQuality {
    pub query_id: String,
    pub expected: usize,
    pub covered: usize,
    pub gt_owner_count: usize,
    pub gt_leaf_count: usize,
    pub oracles: Vec<CoverageOracle>,
}
/// Exact only for complete, unique GT memberships. Costs are current copies to
/// score, not physical block I/O, owner latency or executable navigation work.
pub fn quality_oracles(
    queries: &[GroundTruthQuery],
    owners: &[OwnerGroundTruth],
    global_rows: u64,
) -> Result<Vec<QueryQuality>, String> {
    let union = checked_gt(queries, global_rows)?;
    let mut seen = BTreeSet::new();
    let mut names = BTreeSet::new();
    if owners.is_empty() || owners.len() > 128 {
        return Err("quality owner count cap".into());
    }
    let mut report_leaves = 0usize;
    for owner in owners {
        report_leaves = report_leaves
            .checked_add(owner.leaves.len())
            .ok_or("quality total leaf cap")?;
        if report_leaves > 16384 || !names.insert(&owner.owner_id) || owner.leaves.len() > 4096 {
            return Err("quality duplicate owner/leaf cap".into());
        }
        let mut leaf_ids = BTreeSet::new();
        let mut hits = 0;
        for leaf in &owner.leaves {
            if !leaf_ids.insert(leaf.leaf_id)
                || leaf.gt_ids.len() as u64 > leaf.live_rows
                || leaf.live_rows > leaf.physical_rows
            {
                return Err("quality leaf cost/membership shape".into());
            }
            for &id in &leaf.gt_ids {
                if !union.contains(&id) || !seen.insert(id) {
                    return Err("quality unexpected/duplicate global GT membership".into());
                }
                hits += 1;
            }
        }
        if hits != owner.expected_gt {
            return Err("quality owner expected GT differs".into());
        }
    }
    if seen != union {
        return Err("quality incomplete expected GT union".into());
    }
    let mut results = Vec::new();
    for q in queries {
        let expected: BTreeSet<_> = q.ids.iter().copied().collect();
        let mut weighted = Vec::new();
        let mut owner_set = BTreeSet::new();
        for owner in owners {
            for leaf in &owner.leaves {
                let hits = leaf
                    .gt_ids
                    .iter()
                    .filter(|id| expected.contains(id))
                    .count();
                if hits > 0 {
                    weighted.push((hits, leaf.live_rows));
                    owner_set.insert(&owner.owner_id);
                }
            }
        }
        let mut occupancies: Vec<_> = weighted.iter().map(|p| p.0).collect();
        occupancies.sort_unstable_by(|a, b| b.cmp(a));
        let mut dp = vec![None::<u64>; q.expected + 1];
        dp[0] = Some(0);
        for &(hits, cost) in &weighted {
            for covered in (0..=q.expected).rev() {
                if let Some(old) = dp[covered] {
                    let next = (covered + hits).min(q.expected);
                    let value = old
                        .checked_add(cost)
                        .ok_or("quality weighted row overflow")?;
                    if dp[next].is_none_or(|x| value < x) {
                        dp[next] = Some(value);
                    }
                }
            }
        }
        let mut oracles = Vec::new();
        for percent in [90usize, 95, 100] {
            let target = (q.expected * percent + 99) / 100;
            let mut covered = 0;
            let mut count = 0;
            for hits in &occupancies {
                if covered >= target {
                    break;
                }
                covered += hits;
                count += 1;
            }
            let cost = dp[target..]
                .iter()
                .flatten()
                .copied()
                .min()
                .ok_or("quality weighted coverage missing")?;
            oracles.push(CoverageOracle {
                target_hits: target,
                minimum_leaves: count,
                minimum_live_rows: cost,
            });
        }
        results.push(QueryQuality {
            query_id: q.query_id.clone(),
            expected: q.expected,
            covered: q.expected,
            gt_owner_count: owner_set.len(),
            gt_leaf_count: weighted.len(),
            oracles,
        });
    }
    Ok(results)
}

#[cfg(test)]
impl HierarchicalSpannReader {
    /// Serial saved-set oracle and in-memory edge omission for the retained98 fixture only.
    pub(crate) async fn quality_serial_fixture_oracle(
        &mut self,
        mapping: &[u64],
    ) -> (Vec<(u32, BTreeSet<u64>, u64, u64)>, String) {
        assert!(mapping.len() <= 98);
        let mut rows = Vec::new();
        let mut queue = std::collections::VecDeque::from([self.root_id]);
        let mut seen = BTreeSet::new();
        while let Some(id) = queue.pop_front() {
            assert!(seen.insert(id));
            let children = {
                let n = self.nodes.get(&id).unwrap();
                match n.value() {
                    TreeNode::Internal(n) => Some(n.children.clone()),
                    _ => None,
                }
            };
            if let Some(children) = children {
                queue.extend(children);
                continue;
            }
            let (ids, versions) = {
                let row = self.posting_list_reader.get("", id).await.unwrap().unwrap();
                (row.ids.to_vec(), row.versions.to_vec())
            };
            self.posting_list_reader.clear_loaded_blocks();
            let mut valid = BTreeSet::new();
            let mut live = 0;
            for (&local, &saved) in ids.iter().zip(&versions) {
                let now = self
                    .scalar_metadata_reader
                    .get(PREFIX_VERSION, local)
                    .await
                    .unwrap()
                    .unwrap();
                self.scalar_metadata_reader.clear_loaded_blocks();
                assert!(now <= 255);
                if now & u32::from(DELETED_BIT) == 0 && now == u32::from(saved) {
                    live += 1;
                    assert!(valid.insert(mapping[local as usize]));
                }
            }
            rows.push((id, valid, live, ids.len() as u64));
        }
        assert_eq!(self.vector_data_reader.loaded_blocks_stats().0, 0);
        assert!(self.embeddings.is_empty());
        let detached = self
            .nodes
            .iter()
            .find_map(|entry| {
                if !seen.contains(entry.key()) {
                    return None;
                }
                let TreeNode::Leaf(leaf) = entry.value() else {
                    return None;
                };
                let parent = leaf.parent_id?;
                let p = self.nodes.get(&parent)?;
                matches!(p.value(),TreeNode::Internal(n) if n.children.len()>1)
                    .then_some((*entry.key(), parent))
            })
            .expect("saved fixture has detachable reachable leaf");
        {
            let mut p = self.nodes.get_mut(&detached.1).unwrap();
            let TreeNode::Internal(n) = p.value_mut() else {
                unreachable!()
            };
            n.children.retain(|&id| id != detached.0);
        }
        let targets = mapping
            .iter()
            .enumerate()
            .map(|(i, &g)| (i as u32, g))
            .collect();
        let error = self
            .ground_truth_memberships(
                "fixture",
                mapping.len() as u64,
                &targets,
                QualityLimits::default(),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("missing live ground-truth"), "{error}");
        (rows, error)
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::{config::HierarchicalSpannConfig, writer::HierarchicalSpannWriter};
    use super::*;
    use chroma_blockstore::{
        arrow::provider::{ArrowBlockfileProvider, BlockfileReaderOptions},
        provider::BlockfileProvider,
        BlockfileWriterOptions,
    };
    use chroma_cache::new_cache_for_test;
    use chroma_distance::DistanceFunction;
    use chroma_storage::{local::LocalStorage, Storage};
    use sha2::{Digest, Sha256};
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::Ordering,
    };
    fn hash(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }
    fn output(name: &str) -> PathBuf {
        let root = PathBuf::from(
            std::env::var("SPANN_QUALITY_TEST_OUTPUT")
                .expect("explicit fresh evidence root required"),
        );
        let p = root.join(name);
        fs::create_dir(&p).unwrap();
        p
    }
    fn fixture() -> (serde_json::Value, Vec<Vec<f32>>) {
        let path = PathBuf::from(
            std::env::var("SPANN_QUALITY_BUNDLE").expect("retained98 bundle required"),
        );
        let bytes = fs::read(path).unwrap();
        assert_eq!(
            hash(&bytes),
            std::env::var("SPANN_QUALITY_BUNDLE_SHA256").unwrap()
        );
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let a = &v["deployment"]["canonical_vectors"];
        let bytes = fs::read(a["path"].as_str().unwrap()).unwrap();
        assert_eq!(hash(&bytes), a["sha256"].as_str().unwrap());
        assert_eq!(bytes.len(), 98 * 32 * 4);
        let vectors = bytes
            .chunks_exact(32 * 4)
            .map(|row| {
                row.chunks_exact(4)
                    .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
                    .collect()
            })
            .collect();
        (v, vectors)
    }
    fn provider(path: &Path) -> BlockfileProvider {
        BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
            Storage::Local(LocalStorage::new(path.to_str().unwrap())),
            3 * 1024 * 1024,
            new_cache_for_test(),
            new_cache_for_test(),
            2,
        ))
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quality_saved_semantic_rejections_and_stale_costs() {
        let dir = output("saved-invalid");
        let (_, vectors) = fixture();
        for scenario in [
            "missing",
            "duplicate",
            "deleted",
            "stale",
            "range",
            "missing-version",
            "invalid-version",
            "valid-stale-copy",
        ] {
            let case = dir.join(scenario);
            fs::create_dir(&case).unwrap();
            let p = provider(&case);
            let config = HierarchicalSpannConfig::default();
            let writer =
                HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, config.clone());
            writer.add(0, &vectors[0]);
            writer.add(1, &vectors[1]);
            let root = writer.root_id.load(Ordering::Relaxed);
            {
                let mut n = writer.nodes.get_mut(&root).unwrap();
                let TreeNode::Leaf(leaf) = n.value_mut() else {
                    panic!("two-row root leaf")
                };
                let size = Code::<1>::size(32);
                match scenario {
                    "missing" => {
                        leaf.ids.pop();
                        leaf.versions.pop();
                        leaf.codes.truncate(size);
                        leaf.length = 1;
                    }
                    "duplicate" | "valid-stale-copy" => {
                        leaf.ids.push(0);
                        leaf.versions
                            .push(if scenario == "duplicate" { 1 } else { 0 });
                        leaf.codes.extend_from_slice(&leaf.codes[..size].to_vec());
                        leaf.length += 1;
                    }
                    "range" => {
                        leaf.ids[1] = 2;
                    }
                    _ => {}
                }
            }
            if scenario == "stale" {
                writer.versions.insert(1, 2);
            }
            if scenario == "deleted" {
                writer.delete(1);
            }
            let saved = writer
                .commit(&p, None)
                .await
                .unwrap()
                .flush()
                .await
                .unwrap();
            let mut reader =
                HierarchicalSpannReader::open(&p, saved, DistanceFunction::Euclidean, config)
                    .await
                    .unwrap();
            if scenario == "missing-version" || scenario == "invalid-version" {
                let w = p
                    .write::<u32, u32>(BlockfileWriterOptions::new("".into()))
                    .await
                    .unwrap();
                let id = w.id();
                w.set(PREFIX_VERSION, 0, 1).await.unwrap();
                if scenario == "invalid-version" {
                    w.set(PREFIX_VERSION, 1, 256).await.unwrap();
                }
                let f = w.commit::<u32, u32>().await.unwrap();
                f.flush::<u32, u32>().await.unwrap();
                reader.scalar_metadata_reader = p
                    .read::<u32, u32>(BlockfileReaderOptions::new(id, "".into()))
                    .await
                    .unwrap();
            }
            let targets = BTreeMap::from([(0, 0), (1, 1)]);
            let result = reader
                .ground_truth_memberships("owner", 2, &targets, QualityLimits::default())
                .await;
            if scenario == "valid-stale-copy" {
                let r = result.unwrap();
                assert_eq!((r.physical_rows, r.live_rows, r.stale_rows), (3, 2, 1));
                let limited = QualityLimits {
                    max_leaf_entries: 2,
                    ..Default::default()
                };
                assert!(reader
                    .ground_truth_memberships("owner", 2, &targets, limited)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("declared leaf cap"));
                let cloned = reader.scalar_metadata_reader.clone();
                assert!(reader
                    .ground_truth_memberships("owner", 2, &targets, QualityLimits::default())
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("unique dedicated"));
                drop(cloned);
                fs::write(case.join("report.json"), serde_json::to_vec(&r).unwrap()).unwrap();
            } else {
                let message = result.unwrap_err().to_string();
                let expected = match scenario {
                    "missing" | "stale" => "missing live ground-truth",
                    "duplicate" => "duplicate current ground-truth",
                    "deleted" => "ground-truth ID is deleted",
                    "range" => "outside local map",
                    "missing-version" => "missing authoritative version",
                    _ => "outside u8",
                };
                assert!(message.contains(expected), "{scenario}: {message}");
                fs::write(case.join("expected-error.txt"), message).unwrap();
            }
            assert_eq!(reader.posting_list_reader.loaded_blocks_stats().0, 0);
            assert_eq!(reader.scalar_metadata_reader.loaded_blocks_stats().0, 0);
            assert_eq!(reader.vector_data_reader.loaded_blocks_stats().0, 0);
            assert!(reader.embeddings.is_empty());
        }
    }

    #[test]
    fn quality_bounds_and_nonweighted_count_counterexample() {
        let queries = vec![GroundTruthQuery {
            query_id: "q".into(),
            expected: 100,
            ids: (0..100).collect(),
        }];
        let mut leaves = Vec::new();
        leaves.push(GroundTruthLeaf {
            leaf_id: 0,
            path: vec![0],
            physical_rows: 1000,
            live_rows: 1000,
            physical_code_bytes: 1000,
            live_code_bytes: 1000,
            gt_ids: (0..10).collect(),
        });
        for i in 0..10 {
            leaves.push(GroundTruthLeaf {
                leaf_id: i + 1,
                path: vec![i + 1],
                physical_rows: 9,
                live_rows: 9,
                physical_code_bytes: 9,
                live_code_bytes: 9,
                gt_ids: (10 + u64::from(i) * 9..19 + u64::from(i) * 9).collect(),
            });
        }
        let owner = OwnerGroundTruth {
            owner_id: "x".into(),
            declared_local_rows: 1090,
            reachable_nodes: 11,
            stored_nodes: 11,
            reachable_leaves: 11,
            physical_rows: 1090,
            live_rows: 1090,
            stale_rows: 0,
            deleted_rows: 0,
            version_reads: 1090,
            peak_posting_rows: 1000,
            expected_gt: 100,
            leaves,
            raw_embedding_reads: 0,
        };
        let result = quality_oracles(&queries, &[owner.clone()], 1090).unwrap();
        assert_eq!(
            (
                result[0].oracles[0].minimum_leaves,
                result[0].oracles[0].minimum_live_rows
            ),
            (10, 90)
        );
        assert_eq!(result[0].oracles[2].minimum_live_rows, 1090);
        let mut duplicate = queries.clone();
        duplicate[0].ids[9] = 0;
        assert!(checked_gt(&duplicate, 1090).is_err());
        let mut absent = queries.clone();
        absent[0].expected = 9;
        assert!(checked_gt(&absent, 1090).is_err());
        assert!(checked_gt(&vec![queries[0].clone(); 17], 1090).is_err());
        assert!(quality_oracles(&queries, &vec![owner; 129], 1090).is_err());
    }
}
