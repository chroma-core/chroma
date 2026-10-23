//! Local placement of branches in one reference tree. Construction remains
//! centralized: owner records do not implement remote posting scans or mutation.
use super::super::{
    common::{NodeId, TreeNode},
    config::HierarchicalSpannConfig,
};
use super::{HierarchicalSpannWriter, DELETED_BIT};
use anyhow::{ensure, Context, Result};
use chroma_distance::DistanceFunction;
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodeImage {
    pub parent: Option<NodeId>,
    pub centroid_bits: Vec<u32>,
    pub centroid_code: Vec<u8>,
    pub children: Option<Vec<NodeId>>,
    pub ids: Vec<u32>,
    pub versions: Vec<u8>,
    pub codes: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TreeImage {
    pub root: NodeId,
    pub nodes: BTreeMap<NodeId, NodeImage>,
    pub live_memberships: BTreeMap<u32, NodeId>,
}
impl TreeImage {
    fn capture(w: &HierarchicalSpannWriter) -> Result<Self> {
        let mut nodes = BTreeMap::new();
        let mut live = BTreeMap::new();
        let mut pending = vec![(w.root_id(), None)];
        while let Some((id, parent)) = pending.pop() {
            ensure!(
                !nodes.contains_key(&id) && nodes.len() < 100_000,
                "cycle or node cap"
            );
            let n = w.nodes.get(&id).context("missing reachable node")?;
            let (actual_parent, children, ids, versions, codes) = match n.value() {
                TreeNode::Internal(n) => (
                    n.parent_id,
                    Some(n.children.clone()),
                    vec![],
                    vec![],
                    vec![],
                ),
                TreeNode::Leaf(n) => {
                    ensure!(
                        n.length == n.ids.len()
                            && n.versions.len() == n.ids.len()
                            && n.codes.len() == n.ids.len() * w.code_size(),
                        "requires fully materialized reference writer"
                    );
                    for (&vector, &version) in n.ids.iter().zip(&n.versions) {
                        let current = w
                            .current_version_sync(vector)
                            .context("missing current version")?;
                        if current == version && current & DELETED_BIT == 0 {
                            ensure!(
                                live.insert(vector, id).is_none(),
                                "multiple current memberships"
                            );
                        }
                    }
                    (
                        n.parent_id,
                        None,
                        n.ids.clone(),
                        n.versions.clone(),
                        n.codes.clone(),
                    )
                }
            };
            ensure!(actual_parent == parent, "parent edge mismatch");
            if let Some(children) = &children {
                for &child in children {
                    pending.push((child, Some(id)));
                }
            }
            nodes.insert(
                id,
                NodeImage {
                    parent,
                    centroid_bits: n.centroid().iter().map(|v| v.to_bits()).collect(),
                    centroid_code: n.centroid_code().to_vec(),
                    children,
                    ids,
                    versions,
                    codes,
                },
            );
        }
        Ok(Self {
            root: w.root_id(),
            nodes,
            live_memberships: live,
        })
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct CapacityPolicy {
    pub move_out_above: usize,
    pub settle_at: usize,
    pub max_owners: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct Transfer {
    pub branch: NodeId,
    pub from: usize,
    pub to: usize,
    pub live_vectors: usize,
    /// Logical serialized state size, not observed wire traffic or heap usage.
    pub mutable_payload_bytes: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct PlacementReport {
    pub epoch: u64,
    pub loads: Vec<usize>,
    pub transfers: Vec<Transfer>,
    pub unresolved_overload: Vec<usize>,
    pub scanned_nodes: usize,
    pub scanned_physical_rows: usize,
    pub directory_entries: usize,
    pub execution: &'static str,
}
/// A local owner holds immutable branch-state handles. These are actual local
/// payload handles, but mutable construction still executes in the reference writer.
#[derive(Default)]
struct LocalOwner {
    branches: BTreeMap<NodeId, Arc<NodeImage>>,
}
pub struct DynamicGlobalTree {
    reference: HierarchicalSpannWriter,
    policy: CapacityPolicy,
    owners: Vec<LocalOwner>,
    directory: BTreeMap<NodeId, usize>,
    image: Arc<TreeImage>,
    epoch: u64,
}
impl DynamicGlobalTree {
    pub fn new(
        dim: usize,
        metric: DistanceFunction,
        config: HierarchicalSpannConfig,
        policy: CapacityPolicy,
    ) -> Result<Self> {
        ensure!(
            dim > 0 && dim <= 4096 && config.max_replicas == 1,
            "bounded dimension and no replication required"
        );
        ensure!(
            policy.settle_at > 0
                && policy.settle_at < policy.move_out_above
                && policy.max_owners > 0
                && policy.max_owners <= 128,
            "invalid capacity policy"
        );
        let reference = HierarchicalSpannWriter::new(dim, metric, config);
        let image = Arc::new(TreeImage::capture(&reference)?);
        let mut this = Self {
            reference,
            policy,
            owners: vec![LocalOwner::default()],
            directory: BTreeMap::from([(0, 0)]),
            image,
            epoch: 0,
        };
        this.install_handles()?;
        Ok(this)
    }
    /// Enable an exclusive sequential POC session. Capacity movement is deferred;
    /// logical splits inherit their source owner until this hook is disabled.
    pub fn enable_live_owner_scans(
        &mut self,
        mode: super::live_owner::Mode,
    ) -> Result<Arc<super::live_owner::LiveOwnerHook>> {
        ensure!(
            self.reference.live_owner_hook.lock().is_none(),
            "live hook already enabled"
        );
        self.validate_cover()?;
        let hook = Arc::new(super::live_owner::LiveOwnerHook::new(
            mode,
            self.directory.clone(),
            self.owners.len(),
            self.reference.dim,
            self.reference.distance_fn.clone(),
        )?);
        *self.reference.live_owner_hook.lock() = Some(hook.clone());
        Ok(hook)
    }
    pub fn disable_live_owner_scans(&mut self) {
        self.reference.live_owner_hook.lock().take();
    }
    pub fn snapshot(&self) -> Arc<TreeImage> {
        self.image.clone()
    }
    pub fn branch_owners(&self) -> &BTreeMap<NodeId, usize> {
        &self.directory
    }
    pub fn owner_count(&self) -> usize {
        self.owners.len()
    }
    /// The ordinary reference add/balance schedule runs before placement. New
    /// branches inherit the largest overlapping prior owner; no routing is local.
    pub fn add_batch(&mut self, rows: &[(u32, Vec<f32>)]) -> Result<PlacementReport> {
        ensure!(
            rows.len() <= 4096
                && self
                    .image
                    .live_memberships
                    .len()
                    .checked_add(rows.len())
                    .is_some_and(|n| n <= 100_000),
            "batch/corpus cap"
        );
        let mut ids = BTreeSet::new();
        for (id, v) in rows {
            ensure!(
                ids.insert(*id)
                    && self.reference.current_version_sync(*id).is_none()
                    && v.len() == self.reference.dim
                    && v.iter().all(|v| v.is_finite()),
                "invalid new row"
            );
        }
        let old = self.image.clone();
        let old_directory = self.directory.clone();
        for (id, v) in rows {
            self.reference.add(*id, v);
            self.reference.balance_index();
        }
        let next = Arc::new(TreeImage::capture(&self.reference)?);
        ensure!(
            next.live_memberships.len() == old.live_memberships.len() + rows.len(),
            "reference membership coverage differs"
        );
        let mut directory = BTreeMap::new();
        for (&leaf, node) in &next.nodes {
            if node.children.is_none() {
                let owner = if let Some(owner) = old_directory.get(&leaf) {
                    *owner
                } else {
                    let mut votes = BTreeMap::<usize, usize>::new();
                    for (&id, &where_now) in &next.live_memberships {
                        if where_now == leaf {
                            if let Some(previous) = old.live_memberships.get(&id) {
                                *votes.entry(old_directory[previous]).or_default() += 1;
                            }
                        }
                    }
                    votes
                        .into_iter()
                        .max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)))
                        .map(|v| v.0)
                        .unwrap_or(0)
                };
                directory.insert(leaf, owner);
            }
        }
        let live = self.reference.live_owner_hook.lock().clone();
        if let Some(hook) = &live {
            hook.refresh(&self.reference);
            directory = hook.directory();
        }
        self.directory = directory;
        self.image = next;
        self.install_handles()?;
        if live.is_some() {
            self.epoch = self
                .epoch
                .checked_add(1)
                .context("placement epoch overflow")?;
            let loads = self.loads();
            return Ok(PlacementReport { epoch:self.epoch, unresolved_overload:loads.iter().enumerate().filter(|(_,n)|**n>self.policy.move_out_above).map(|(i,_)|i).collect(), loads, transfers:vec![], scanned_nodes:self.image.nodes.len(), scanned_physical_rows:self.image.nodes.values().map(|n|n.ids.len()).sum(), directory_entries:self.directory.len(), execution:"live local owner eligibility; centralized reference mutation; placement deferred while enabled" });
        }
        self.rebalance()
    }
    fn install_handles(&mut self) -> Result<()> {
        for o in &mut self.owners {
            o.branches.clear();
        }
        for (&branch, &owner) in &self.directory {
            let n = self.image.nodes.get(&branch).context("unknown branch")?;
            ensure!(n.children.is_none(), "branch cover must consist of leaves");
            self.owners
                .get_mut(owner)
                .context("unknown owner")?
                .branches
                .insert(branch, Arc::new(n.clone()));
        }
        self.validate_cover()
    }
    fn validate_cover(&self) -> Result<()> {
        let leaves: BTreeSet<_> = self
            .image
            .nodes
            .iter()
            .filter(|(_, n)| n.children.is_none())
            .map(|(id, _)| *id)
            .collect();
        ensure!(
            leaves == self.directory.keys().copied().collect(),
            "branch cover differs"
        );
        let mut seen = BTreeSet::new();
        for (owner, state) in self.owners.iter().enumerate() {
            for (&id, payload) in &state.branches {
                ensure!(
                    seen.insert(id)
                        && self.directory.get(&id) == Some(&owner)
                        && payload.as_ref() == &self.image.nodes[&id],
                    "local branch payload differs"
                );
            }
        }
        ensure!(seen == leaves, "owner branch cover differs");
        Ok(())
    }
    fn loads(&self) -> Vec<usize> {
        let mut loads = vec![0; self.owners.len()];
        for leaf in self.image.live_memberships.values() {
            loads[self.directory[leaf]] += 1;
        }
        loads
    }
    /// Move existing leaves only. An indivisible oversized leaf or exhausted
    /// owner budget is reported, never repaired by changing clustering here.
    pub fn rebalance(&mut self) -> Result<PlacementReport> {
        ensure!(
            self.reference.live_owner_hook.lock().is_none(),
            "disable live hook before capacity transfer"
        );
        let before = TreeImage::capture(&self.reference)?;
        ensure!(before == *self.image, "reference changed outside batch");
        let mut transfers = vec![];
        let mut loads = self.loads();
        for source in 0..self.owners.len() {
            if loads[source] <= self.policy.move_out_above {
                continue;
            }
            let mut candidates: Vec<_> = self
                .directory
                .iter()
                .filter(|(_, o)| **o == source)
                .map(|(&leaf, _)| {
                    (
                        leaf,
                        self.image
                            .live_memberships
                            .values()
                            .filter(|&&v| v == leaf)
                            .count(),
                    )
                })
                .collect();
            candidates.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            for (leaf, count) in candidates {
                if loads[source] <= self.policy.settle_at {
                    break;
                }
                if count == 0 || count > self.policy.move_out_above {
                    continue;
                }
                let mut dest = (0..loads.len())
                    .filter(|&i| {
                        i != source
                            && loads[i]
                                .checked_add(count)
                                .is_some_and(|n| n <= self.policy.settle_at)
                    })
                    .min_by_key(|&i| (loads[i], i));
                if dest.is_none() && self.owners.len() < self.policy.max_owners {
                    self.owners.push(LocalOwner::default());
                    loads.push(0);
                    dest = Some(loads.len() - 1);
                }
                if let Some(to) = dest {
                    let payload = self.owners[source]
                        .branches
                        .remove(&leaf)
                        .context("missing source branch")?;
                    let bytes = serde_json::to_vec(payload.as_ref())?.len();
                    ensure!(
                        self.owners[to].branches.insert(leaf, payload).is_none(),
                        "duplicate destination branch"
                    );
                    self.directory.insert(leaf, to);
                    loads[source] -= count;
                    loads[to] += count;
                    transfers.push(Transfer {
                        branch: leaf,
                        from: source,
                        to,
                        live_vectors: count,
                        mutable_payload_bytes: bytes,
                    });
                }
            }
        }
        self.epoch = self
            .epoch
            .checked_add(1)
            .context("placement epoch overflow")?;
        self.validate_cover()?;
        ensure!(
            before == TreeImage::capture(&self.reference)?,
            "placement changed tree"
        );
        Ok(PlacementReport {
            epoch: self.epoch,
            unresolved_overload: loads
                .iter()
                .enumerate()
                .filter(|(_, n)| **n > self.policy.move_out_above)
                .map(|(i, _)| i)
                .collect(),
            loads,
            transfers,
            scanned_nodes: before.nodes.len(),
            scanned_physical_rows: before.nodes.values().map(|n| n.ids.len()).sum(),
            directory_entries: self.directory.len(),
            execution:
                "centralized-reference-construction/local-branch-handoff; not distributed mutation",
        })
    }
}

#[cfg(test)]
impl DynamicGlobalTree {
    pub(super) fn test_reference(&self) -> &HierarchicalSpannWriter {
        &self.reference
    }
    pub(super) fn test_from_reference(
        reference: HierarchicalSpannWriter,
        directory: BTreeMap<NodeId, usize>,
        policy: CapacityPolicy,
    ) -> Result<Self> {
        let count = directory.values().max().copied().unwrap_or(0) + 1;
        let image = Arc::new(TreeImage::capture(&reference)?);
        let mut t = Self {
            reference,
            policy,
            owners: (0..count).map(|_| LocalOwner::default()).collect(),
            directory,
            image,
            epoch: 0,
        };
        t.install_handles()?;
        Ok(t)
    }
}

#[derive(Debug, Serialize)]
pub struct NeighborScanReport {
    pub epoch: u64,
    pub discovered: Vec<(NodeId, u32)>,
    pub dispatched: Vec<(super::owner_scan::ScanRequest, super::owner_scan::ScanReply)>,
    pub eligible_in_order: Vec<(NodeId, u32)>,
    pub bootstrap_copied_payload_bytes: usize,
    pub bootstrap_serialized_source_bytes: usize,
    pub execution: &'static str,
}
impl DynamicGlobalTree {
    fn owner_sources(&self) -> Result<BTreeMap<usize, Vec<super::owner_scan::SourceLeaf>>> {
        ensure!(
            *self.image == TreeImage::capture(&self.reference)?,
            "source tree no longer matches directory"
        );
        self.validate_cover()?;
        let mut sources: BTreeMap<_, Vec<_>> =
            (0..self.owners.len()).map(|o| (o, vec![])).collect();
        for (&leaf, &owner) in &self.directory {
            let node = &self.image.nodes[&leaf];
            let vectors = node
                .ids
                .iter()
                .map(|id| self.reference.embeddings.get(id).map(|v| v.to_vec()))
                .collect();
            sources
                .get_mut(&owner)
                .context("source owner missing")?
                .push(super::owner_scan::SourceLeaf {
                    leaf,
                    centroid: node
                        .centroid_bits
                        .iter()
                        .map(|&b| f32::from_bits(b))
                        .collect(),
                    ids: node.ids.clone(),
                    versions: node.versions.clone(),
                    codes: node.codes.clone(),
                    current: node
                        .ids
                        .iter()
                        .map(|&id| self.reference.current_version_sync(id))
                        .collect(),
                    vectors,
                });
        }
        Ok(sources)
    }
    /// A stable batch-boundary scan. Global discovery is the actual reference
    /// navigation; local actors own source payloads and run the shared predicates.
    /// No reassignment is performed between rows, so this does not claim parity
    /// with a complete mutation-interleaved split/reassignment transaction.
    pub fn scan_global_neighbors(
        &self,
        old_leaf: NodeId,
        left_leaf: NodeId,
        right_leaf: NodeId,
        old: &[f32],
        left: &[f32],
        right: &[f32],
    ) -> Result<NeighborScanReport> {
        ensure!(
            [old, left, right]
                .iter()
                .all(|v| v.len() == self.reference.dim && v.iter().all(|x| x.is_finite())),
            "invalid eligibility centers"
        );
        let sources = self.owner_sources()?;
        let mut fleet = super::owner_scan::OwnerFleet::new(
            sources,
            self.epoch,
            self.reference.dim,
            self.reference.distance_fn.clone(),
        )?;
        let c = &self.reference.config;
        let policy = super::super::common::ReadBeamPolicy::with_level_overrides(
            Some(c.write_beam_tau),
            c.write_beam_min,
            c.write_beam_max,
            c.write_level_taus.clone(),
            c.write_level_min_pcts.clone(),
            self.reference.level_node_counts(),
        );
        let neighbors = self
            .reference
            .navigate_with_policy(old, c.write_navigation, &policy);
        let mut report=NeighborScanReport{epoch:self.epoch,discovered:neighbors.iter().map(|&(id,d)|(id,d.to_bits())).collect(),dispatched:vec![],eligible_in_order:vec![],bootstrap_copied_payload_bytes:fleet.copied_source_payload_bytes,bootstrap_serialized_source_bytes:fleet.bootstrap_serialized_source_bytes,execution:"actual local owner-thread eligibility; centralized global discovery; no destination mutation"};
        let mut evaluated = std::collections::HashSet::new();
        let mut ledger_bytes = 0usize;
        for &(leaf, _) in neighbors.iter().take(c.reassign_neighbor_count) {
            if leaf == old_leaf || leaf == left_leaf || leaf == right_leaf {
                continue;
            }
            let owner = *self
                .directory
                .get(&leaf)
                .context("globally discovered leaf has no owner")?;
            let (request, reply) =
                fleet.request(owner, leaf, old, left, right, c.fp_npa, &evaluated)?;
            for &id in &reply.inserted_evaluated {
                ensure!(
                    evaluated.insert(id),
                    "owner returned duplicate evaluated ID"
                );
            }
            report
                .eligible_in_order
                .extend(reply.eligible_ids.iter().map(|&id| (leaf, id)));
            ledger_bytes = ledger_bytes
                .checked_add(serde_json::to_vec(&(&request, &reply))?.len())
                .context("scan ledger bytes overflow")?;
            ensure!(ledger_bytes <= 8 * 1024 * 1024, "scan ledger byte cap");
            report.dispatched.push((request, reply));
        }
        ensure!(
            *self.image == TreeImage::capture(&self.reference)?,
            "eligibility mutated reference"
        );
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::{common::ReadBeamPolicy, reader::HierarchicalSpannReader};
    use super::*;
    use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage, Storage};
    use sha2::{Digest, Sha256};
    use std::path::Path;
    fn config() -> HierarchicalSpannConfig {
        HierarchicalSpannConfig {
            branching_factor: 2,
            split_threshold: 8,
            merge_threshold: 2,
            ..Default::default()
        }
    }
    fn policy() -> CapacityPolicy {
        CapacityPolicy {
            move_out_above: 1000,
            settle_at: 900,
            max_owners: 8,
        }
    }
    fn rows(start: u32, end: u32) -> Vec<(u32, Vec<f32>)> {
        (start..end)
            .map(|id| (id, vec![id as f32, 1., -0., (id % 3) as f32]))
            .collect()
    }
    fn provider(path: &Path) -> BlockfileProvider {
        BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
            Storage::Local(LocalStorage::new(path.to_str().unwrap())),
            1024,
            new_cache_for_test(),
            new_cache_for_test(),
            2,
        ))
    }
    fn inventory(path: &Path) -> BTreeMap<String, String> {
        fn visit(root: &Path, p: &Path, out: &mut BTreeMap<String, String>) {
            for entry in std::fs::read_dir(p).unwrap() {
                let p = entry.unwrap().path();
                if p.is_dir() {
                    visit(root, &p, out)
                } else {
                    out.insert(
                        p.strip_prefix(root).unwrap().to_str().unwrap().into(),
                        format!("{:x}", Sha256::digest(std::fs::read(p).unwrap())),
                    );
                }
            }
        }
        let mut out = BTreeMap::new();
        visit(path, path, &mut out);
        out
    }
    fn routes(tree: &DynamicGlobalTree) -> Vec<Vec<(u32, u32)>> {
        let c = &tree.reference.config;
        let p = ReadBeamPolicy::with_level_overrides(
            Some(c.write_beam_tau),
            c.write_beam_min,
            c.write_beam_max,
            c.write_level_taus.clone(),
            c.write_level_min_pcts.clone(),
            tree.reference.level_node_counts(),
        );
        [0., 15.5, 31.5, 63.]
            .iter()
            .map(|&x| {
                tree.reference
                    .navigate_with_policy(&[x, 1., -0., 0.], c.write_navigation, &p)
                    .into_iter()
                    .map(|(i, d)| (i, d.to_bits()))
                    .collect()
            })
            .collect()
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dynamic_global_saved_reader_and_reference_routes_survive_branch_handoff() {
        let store = tempfile::tempdir().unwrap();
        let p = provider(store.path());
        let mut t =
            DynamicGlobalTree::new(4, DistanceFunction::Euclidean, config(), policy()).unwrap();
        assert_eq!(t.owner_count(), 1);
        assert_eq!(t.snapshot().nodes.len(), 1);
        t.add_batch(&rows(0, 64)).unwrap();
        assert_eq!(t.owner_count(), 1);
        let pinned_image = t.snapshot();
        assert!(pinned_image.nodes.len() > 1);
        assert_ne!(pinned_image.root, 0);
        let ids = t
            .reference
            .commit(&p, None)
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
        let pinned =
            HierarchicalSpannReader::open(&p, ids.clone(), DistanceFunction::Euclidean, config())
                .await
                .unwrap();
        let query = [22.5, 1., -0., 0.];
        let old_ids: Vec<_> = (0..64).collect();
        let expected = pinned.rerank_selected(&query, &old_ids).await.unwrap();
        let saved_before = inventory(store.path());
        let before_routes = routes(&t);
        t.policy = CapacityPolicy {
            move_out_above: 20,
            settle_at: 14,
            max_owners: 8,
        };
        let report = t.rebalance().unwrap();
        assert!(!report.transfers.is_empty());
        assert!(t.owner_count() > 1);
        assert!(t.owners.iter().any(|o| o.branches.len() > 1));
        assert!(report.unresolved_overload.is_empty());
        assert_eq!(*pinned_image, *t.snapshot());
        assert_eq!(before_routes, routes(&t));
        assert_eq!(saved_before, inventory(store.path()));
        assert_eq!(
            expected,
            pinned.rerank_selected(&query, &old_ids).await.unwrap()
        );
        let reopened = HierarchicalSpannReader::open(
            &provider(store.path()),
            ids.clone(),
            DistanceFunction::Euclidean,
            config(),
        )
        .await
        .unwrap();
        assert_eq!(
            expected,
            reopened.rerank_selected(&query, &old_ids).await.unwrap()
        );
        let next = t.add_batch(&rows(64, 80)).unwrap();
        assert_eq!(next.loads.iter().sum::<usize>(), 80);
        assert_eq!(
            t.snapshot()
                .live_memberships
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            (0..80).collect::<Vec<_>>()
        );
        assert_eq!(pinned_image.live_memberships.len(), 64);
        assert_eq!(
            expected,
            pinned.rerank_selected(&query, &old_ids).await.unwrap()
        );
        let new_ids = t
            .reference
            .commit(&p, Some(&ids))
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
        let new_reader = HierarchicalSpannReader::open(
            &provider(store.path()),
            new_ids,
            DistanceFunction::Euclidean,
            config(),
        )
        .await
        .unwrap();
        let all: Vec<_> = (0..80).collect();
        let actual = new_reader.rerank_selected(&query, &all).await.unwrap();
        assert_eq!(actual.len(), 80);
        for (id, d) in actual {
            let v = rows(id, id + 1).pop().unwrap().1;
            let independent: f32 = query.iter().zip(v).map(|(a, b)| (a - b) * (a - b)).sum();
            assert_eq!(d.to_bits(), independent.to_bits());
        }
        let saved_after = inventory(store.path());
        for (key, hash) in saved_before {
            assert_eq!(saved_after.get(&key), Some(&hash));
        }
        let root =
            std::env::var("SPANN_DYNAMIC_GLOBAL_OUTPUT").expect("explicit evidence directory");
        std::fs::write(Path::new(&root).join("placement.json"),serde_json::to_vec_pretty(&serde_json::json!({"initial_transfer":report,"continued_batch":next,"before":pinned_image,"after":t.snapshot()})).unwrap()).unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owner_scan_global_discovery_crosses_owners_and_matches_one_owner_control() {
        let mut cfg = config();
        cfg.write_beam_min = 128;
        cfg.write_beam_max = 128;
        cfg.reassign_neighbor_count = 128;
        let mut t = DynamicGlobalTree::new(4, DistanceFunction::Euclidean, cfg, policy()).unwrap();
        t.add_batch(&rows(0, 64)).unwrap();
        t.policy = CapacityPolicy {
            move_out_above: 20,
            settle_at: 14,
            max_owners: 8,
        };
        t.rebalance().unwrap();
        let sources = t.owner_sources().unwrap();
        let mut picked = vec![];
        for (&owner, leaves) in &sources {
            if let Some((leaf, v)) = leaves.iter().find_map(|l| {
                l.vectors
                    .iter()
                    .flatten()
                    .find(|v| {
                        v.iter().map(|x| x.to_bits()).collect::<Vec<_>>()
                            != l.centroid.iter().map(|x| x.to_bits()).collect::<Vec<_>>()
                    })
                    .map(|v| (l.leaf, v.clone()))
            }) {
                picked.push((owner, leaf, v));
            }
        }
        assert!(picked.len() >= 2);
        let left = picked[0].2.clone();
        let right = picked[1].2.clone();
        let old = vec![1000., 1., 0., 0.];
        let image = t.snapshot();
        let cross = t
            .scan_global_neighbors(u32::MAX, u32::MAX - 1, u32::MAX - 2, &old, &left, &right)
            .unwrap();
        assert!(!cross.eligible_in_order.is_empty());
        let actual_owners: BTreeSet<_> = cross
            .eligible_in_order
            .iter()
            .map(|(leaf, _)| t.directory[leaf])
            .collect();
        assert!(
            actual_owners.len() >= 2,
            "deliberate crossowner affected vectors required"
        );
        let threads: BTreeSet<_> = cross
            .dispatched
            .iter()
            .map(|(_, r)| r.thread.clone())
            .collect();
        assert!(threads.len() >= 2);
        assert_eq!(cross.dispatched.len(), t.directory.len());
        let directory = t.directory.clone();
        let owners = std::mem::replace(&mut t.owners, vec![LocalOwner::default()]);
        for owner in t.directory.values_mut() {
            *owner = 0;
        }
        t.install_handles().unwrap();
        let control = t
            .scan_global_neighbors(u32::MAX, u32::MAX - 1, u32::MAX - 2, &old, &left, &right)
            .unwrap();
        assert_eq!(cross.discovered, control.discovered);
        assert_eq!(cross.eligible_in_order, control.eligible_in_order);
        assert_eq!(
            cross
                .dispatched
                .iter()
                .map(|(_, r)| (&r.inserted_evaluated, &r.eligible_ids, r.evaluated))
                .collect::<Vec<_>>(),
            control
                .dispatched
                .iter()
                .map(|(_, r)| (&r.inserted_evaluated, &r.eligible_ids, r.evaluated))
                .collect::<Vec<_>>()
        );
        t.directory = directory;
        t.owners = owners;
        t.install_handles().unwrap();
        assert_eq!(*image, *t.snapshot());
        let output = std::env::var("SPANN_DYNAMIC_GLOBAL_OUTPUT").expect("fresh evidence output");
        std::fs::write(
            Path::new(&output).join("owner-scans.json"),
            serde_json::to_vec_pretty(
                &serde_json::json!({"cross_owner":cross,"one_owner":control,"picked":picked}),
            )
            .unwrap(),
        )
        .unwrap();
    }
    #[test]
    fn dynamic_global_unsplittable_capacity_and_input_admission_are_explicit() {
        let mut cfg = config();
        cfg.split_threshold = 100;
        cfg.merge_threshold = 2;
        let mut t = DynamicGlobalTree::new(
            4,
            DistanceFunction::Euclidean,
            cfg,
            CapacityPolicy {
                move_out_above: 4,
                settle_at: 2,
                max_owners: 2,
            },
        )
        .unwrap();
        let report = t.add_batch(&rows(0, 8)).unwrap();
        assert_eq!(report.unresolved_overload, vec![0]);
        assert!(report.transfers.is_empty());
        assert_eq!(t.owner_count(), 1);
        let before = t.snapshot();
        assert!(t.add_batch(&[(8, vec![f32::NAN; 4])]).is_err());
        assert!(t.add_batch(&rows(0, 1)).is_err());
        assert!(t.add_batch(&[(8, vec![0.; 4]), (8, vec![0.; 4])]).is_err());
        assert_eq!(*before, *t.snapshot());
        assert!(DynamicGlobalTree::new(
            4,
            DistanceFunction::Euclidean,
            config(),
            CapacityPolicy {
                move_out_above: 2,
                settle_at: 2,
                max_owners: 2
            }
        )
        .is_err());
    }
}
