//! Exclusive private-build operations for one globally coordinated write route.
//!
//! A session borrows its writer exclusively, so balancing cannot change the
//! topology between expansion and registration. The caller owns deployment
//! fencing, global ID ownership, global level widths, and final publication.
use super::super::common::{effective_beam, NodeId, ReadBeamPolicy, TreeNode};
use super::HierarchicalSpannWriter;
use std::collections::HashSet;
use std::sync::{atomic::Ordering, Arc};

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct WriteChild {
    pub parent_id: NodeId,
    pub node_id: NodeId,
    pub is_leaf: bool,
    pub distance: f32,
}

pub struct WriteSession<'a> {
    writer: &'a mut HierarchicalSpannWriter,
    authorization: String,
}

impl HierarchicalSpannWriter {
    /// Bind a private writer to an externally verified parent/ownership fence.
    /// This does not itself acquire a distributed lease or publish a snapshot.
    pub fn write_session(&mut self, authorization: String) -> Result<WriteSession<'_>, String> {
        if authorization.is_empty() || self.config.max_replicas != 1 {
            return Err("nonempty authorization and single-owner writer required".into());
        }
        Ok(WriteSession {
            writer: self,
            authorization,
        })
    }
}

impl WriteSession<'_> {
    fn authorized(&self, authorization: &str) -> Result<(), String> {
        if authorization != self.authorization {
            return Err("stale write authorization".into());
        }
        Ok(())
    }
    fn vector(&self, vector: &[f32]) -> Result<(), String> {
        if vector.len() != self.writer.dim || vector.iter().any(|v| !v.is_finite()) {
            return Err("invalid write vector".into());
        }
        Ok(())
    }
    pub fn root(&self) -> NodeId {
        self.writer.root_id()
    }

    pub fn score_root(&self, authorization: &str, vector: &[f32]) -> Result<WriteChild, String> {
        self.authorized(authorization)?;
        self.vector(vector)?;
        let id = self.root();
        let root = self.writer.nodes.get(&id).ok_or("missing write root")?;
        let distance = self.writer.dist(vector, root.centroid());
        if !distance.is_finite() {
            return Err("nonfinite root distance".into());
        }
        Ok(WriteChild {
            parent_id: id,
            node_id: id,
            is_leaf: matches!(root.value(), TreeNode::Leaf(_)),
            distance,
        })
    }

    /// Score the actual lower root through its committed upper-boundary alias.
    /// The caller must obtain this centroid from the authorization's routing model.
    pub fn score_boundary(
        &self,
        authorization: &str,
        vector: &[f32],
        centroid: &[f32],
    ) -> Result<WriteChild, String> {
        self.authorized(authorization)?;
        self.vector(vector)?;
        self.vector(centroid)?;
        let id = self.root();
        let root = self.writer.nodes.get(&id).ok_or("missing write root")?;
        let distance = self.writer.dist(vector, centroid);
        if !distance.is_finite() {
            return Err("nonfinite boundary distance".into());
        }
        Ok(WriteChild {
            parent_id: id,
            node_id: id,
            is_leaf: matches!(root.value(), TreeNode::Leaf(_)),
            distance,
        })
    }

    /// Use the writer's configured policy with complete global level widths.
    /// Widths include upper children at index zero, not only this lower tree.
    pub fn policy(&self, global_level_widths: Vec<usize>) -> ReadBeamPolicy {
        let config = &self.writer.config;
        ReadBeamPolicy::with_level_overrides(
            Some(config.write_beam_tau),
            config.write_beam_min,
            config.write_beam_max,
            config.write_level_taus.clone(),
            config.write_level_min_pcts.clone(),
            global_level_widths,
        )
    }

    /// Expose the actual writer policy for agreement across a captured owner set.
    /// Widths remain an explicit global input to `policy`, not per-owner limits.
    pub fn policy_settings(&self) -> serde_json::Value {
        let c = &self.writer.config;
        serde_json::json!({"tau":c.write_beam_tau,"min":c.write_beam_min,
            "max":c.write_beam_max,"level_taus":c.write_level_taus,
            "level_min_pcts":c.write_level_min_pcts})
    }

    /// Return every child of every requested parent, without a local beam cap.
    pub fn expand(
        &self,
        authorization: &str,
        parents: &[NodeId],
        vector: &[f32],
    ) -> Result<Vec<WriteChild>, String> {
        self.authorized(authorization)?;
        self.vector(vector)?;
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for &parent in parents {
            if !seen.insert(parent) {
                return Err("duplicate write parent".into());
            }
            let node = self
                .writer
                .nodes
                .get(&parent)
                .ok_or("missing write parent")?;
            let children = match node.value() {
                TreeNode::Internal(internal) => internal.children.clone(),
                TreeNode::Leaf(_) => return Err("cannot expand a leaf".into()),
            };
            drop(node);
            for child_id in children {
                let child = self
                    .writer
                    .nodes
                    .get(&child_id)
                    .ok_or("missing write child")?;
                let distance = self.writer.dist(vector, child.centroid());
                if !distance.is_finite() {
                    return Err("nonfinite write distance".into());
                }
                result.push(WriteChild {
                    parent_id: parent,
                    node_id: child_id,
                    is_leaf: matches!(child.value(), TreeNode::Leaf(_)),
                    distance,
                });
            }
        }
        Ok(result)
    }

    /// Register a new local ID at the chosen leaf; never navigate or balance.
    /// Release all routing sessions before balancing, then begin a fresh route.
    pub fn register_new(
        &mut self,
        authorization: &str,
        leaf: NodeId,
        id: u32,
        vector: &[f32],
    ) -> Result<(), String> {
        self.authorized(authorization)?;
        self.vector(vector)?;
        if self.writer.current_version_sync(id).is_some() {
            return Err("document already exists; updates need a separate transaction".into());
        }
        if !self
            .writer
            .nodes
            .get(&leaf)
            .is_some_and(|node| matches!(node.value(), TreeNode::Leaf(_)))
        {
            return Err("selected leaf unavailable; retry the complete global route".into());
        }
        // Exclusive borrow keeps the target present. Publish version only after
        // successful posting registration; a failed target never consumes an ID.
        if !self.writer.register_in_leaf(leaf, id, 1, vector) {
            return Err("selected leaf unavailable; retry the complete global route".into());
        }
        self.writer.embeddings.insert(id, Arc::from(vector));
        self.writer.versions.insert(id, 1);
        self.writer.mark_version_dirty(id);
        self.writer.mark_embedding_dirty(id);
        self.writer
            .stats
            .embeddings_added
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Apply the exact Rust write-beam arithmetic once to a globally sorted level.
/// Qualified global node identities must establish ties before calling this.
pub fn global_write_count(
    sorted_distances: &[f32],
    policy: &ReadBeamPolicy,
    level: usize,
) -> Result<usize, String> {
    if level == 0
        || sorted_distances.iter().any(|d| !d.is_finite())
        || sorted_distances.windows(2).any(|v| v[0] > v[1])
    {
        return Err("finite globally sorted distances and one-based level required".into());
    }
    let params = policy.level_params(level);
    let rows: Vec<_> = sorted_distances.iter().map(|&d| (0, d)).collect();
    Ok(effective_beam(
        &rows,
        params.tau,
        params.beam_min,
        params.beam_max,
    ))
}

/// Typed tasks for the existing compactor's transport; no new listener/service.
/// The transport must pin one private session and enforce frame/deadline limits.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRequest {
    pub request_id: String,
    pub authorization: String,
    pub command: WriteCommand,
}

#[derive(serde::Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteCommand {
    Root {
        vector: Vec<f32>,
        #[serde(default)]
        boundary_centroid: Option<Vec<f32>>,
    },
    Expand {
        parents: Vec<NodeId>,
        vector: Vec<f32>,
    },
    RegisterNew {
        leaf: NodeId,
        local_id: u32,
        vector: Vec<f32>,
    },
}

#[derive(Debug, serde::Serialize)]
pub struct WriteReply {
    pub request_id: String,
    pub result: WriteResult,
}

#[derive(Debug, serde::Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WriteResult {
    Root {
        root: WriteChild,
        local_level_widths: Vec<usize>,
    },
    Expand {
        children: Vec<WriteChild>,
    },
    Registered {
        leaf: NodeId,
        local_id: u32,
    },
}

impl WriteSession<'_> {
    /// Decode separately, then execute against exactly the pinned private writer.
    /// A registration request is not a global ownership grant or commit receipt.
    pub fn execute(&mut self, request: WriteRequest) -> Result<WriteReply, String> {
        if request.request_id.is_empty() || request.request_id.len() > 256 {
            return Err("bounded nonempty request identity required".into());
        }
        self.authorized(&request.authorization)?;
        let result = match request.command {
            WriteCommand::Root {
                vector,
                boundary_centroid,
            } => WriteResult::Root {
                root: match boundary_centroid {
                    Some(centroid) => {
                        self.score_boundary(&request.authorization, &vector, &centroid)?
                    }
                    None => self.score_root(&request.authorization, &vector)?,
                },
                local_level_widths: self.writer.level_node_counts(),
            },
            WriteCommand::Expand { parents, vector } => WriteResult::Expand {
                children: self.expand(&request.authorization, &parents, &vector)?,
            },
            WriteCommand::RegisterNew {
                leaf,
                local_id,
                vector,
            } => {
                self.register_new(&request.authorization, leaf, local_id, &vector)?;
                WriteResult::Registered { leaf, local_id }
            }
        };
        Ok(WriteReply {
            request_id: request.request_id,
            result,
        })
    }
}

/// A stable logical-owner position and its selected local leaf.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteDestination {
    pub owner: usize,
    pub leaf: NodeId,
    pub distance: f32,
}

/// Execute one global write route over exclusively borrowed private lower trees.
/// Owner positions must have a stable logical order independent of placement.
/// This virtual root uses each actual lower root centroid; a frozen external
/// boundary centroid requires an explicit root-score adapter, not substitution.
pub fn route_new_global(
    sessions: &[WriteSession<'_>],
    authorization: &str,
    vector: &[f32],
    global_level_widths: Vec<usize>,
) -> Result<WriteDestination, String> {
    route_new_global_with_boundaries(sessions, authorization, vector, global_level_widths, None)
}

/// Use the committed alias centroids at the ownership boundary when supplied.
pub fn route_new_global_with_boundaries(
    sessions: &[WriteSession<'_>],
    authorization: &str,
    vector: &[f32],
    global_level_widths: Vec<usize>,
    boundary_centroids: Option<&[Vec<f32>]>,
) -> Result<WriteDestination, String> {
    if boundary_centroids.is_some_and(|rows| rows.len() != sessions.len()) {
        return Err("boundary count differs from logical owner count".into());
    }
    if sessions.is_empty() {
        return Err("global route requires owners".into());
    }
    let first = &sessions[0].writer;
    let mut actual_widths = Vec::<usize>::new();
    for session in sessions {
        let writer = &session.writer;
        let a = &first.config;
        let b = &writer.config;
        if first.dim != writer.dim
            || first.distance_fn != writer.distance_fn
            || a.write_beam_tau != b.write_beam_tau
            || a.write_beam_min != b.write_beam_min
            || a.write_beam_max != b.write_beam_max
            || a.write_level_taus != b.write_level_taus
            || a.write_level_min_pcts != b.write_level_min_pcts
        {
            return Err("owners disagree on metric or write policy".into());
        }
        for (level, width) in writer.level_node_counts().into_iter().enumerate() {
            if level >= actual_widths.len() {
                actual_widths.resize(level + 1, 0);
            }
            actual_widths[level] = actual_widths[level]
                .checked_add(width)
                .ok_or("global level width overflow")?;
        }
    }
    if actual_widths != global_level_widths {
        return Err("global level widths differ from pinned topology".into());
    }
    let policies: Vec<_> = sessions
        .iter()
        .map(|s| s.policy(global_level_widths.clone()))
        .collect();
    let mut frontier = Vec::new();
    for (owner, session) in sessions.iter().enumerate() {
        let root = match boundary_centroids {
            Some(rows) => session.score_boundary(authorization, vector, &rows[owner])?,
            None => session.score_root(authorization, vector)?,
        };
        frontier.push((owner, root));
    }
    let mut leaves: Vec<WriteDestination> = Vec::new();
    let mut visited = HashSet::new();
    for level in 1..=128 {
        frontier.sort_by(|a, b| {
            a.1.distance
                .total_cmp(&b.1.distance)
                .then(a.0.cmp(&b.0))
                .then(a.1.node_id.cmp(&b.1.node_id))
        });
        let distances: Vec<_> = frontier.iter().map(|(_, n)| n.distance).collect();
        let count = global_write_count(&distances, &policies[0], level)?;
        for policy in &policies[1..] {
            if global_write_count(&distances, policy, level)? != count {
                return Err("owners disagree on global write policy".into());
            }
        }
        if count == 0 {
            return Err("empty global write frontier".into());
        }
        let mut parents = vec![Vec::new(); sessions.len()];
        for (owner, node) in frontier.into_iter().take(count) {
            if !visited.insert((owner, node.node_id)) {
                return Err("cycle in global write route".into());
            }
            if node.is_leaf {
                leaves.push(WriteDestination {
                    owner,
                    leaf: node.node_id,
                    distance: node.distance,
                });
            } else {
                parents[owner].push(node.node_id);
            }
        }
        if parents.iter().all(|p| p.is_empty()) {
            return leaves
                .into_iter()
                .min_by(|a, b| {
                    a.distance
                        .total_cmp(&b.distance)
                        .then(a.owner.cmp(&b.owner))
                        .then(a.leaf.cmp(&b.leaf))
                })
                .ok_or("global route found no leaf".into());
        }
        frontier = Vec::new();
        for (owner, ids) in parents.iter().enumerate() {
            if !ids.is_empty() {
                for child in sessions[owner].expand(authorization, ids, vector)? {
                    frontier.push((owner, child));
                }
            }
        }
    }
    Err("global write route exceeds depth bound".into())
}

/// Bounded ownership plan for one dense-prefix private compaction transaction.
/// Publication still fences the complete parent deployment; this is not a
/// cross-process lock or a live-serving ownership service.
pub struct WriteOwnership {
    maps: Vec<Vec<u64>>,
    next_global_id: u64,
    max_rows: usize,
    pub old_version_probes: u64,
}
impl WriteOwnership {
    pub fn new(maps: Vec<Vec<u64>>, max_rows: usize) -> Result<Self, String> {
        if maps.is_empty() || max_rows == 0 || max_rows > 100_000 {
            return Err("explicit bounded ownership scope required".into());
        }
        let count = maps
            .iter()
            .try_fold(0usize, |sum, map| sum.checked_add(map.len()))
            .ok_or("ownership count overflow")?;
        if count > max_rows {
            return Err("ownership row budget exceeded".into());
        }
        let mut seen = HashSet::new();
        for &id in maps.iter().flatten() {
            if id >= count as u64 || !seen.insert(id) {
                return Err("existing ownership must be one disjoint dense global prefix".into());
            }
        }
        Ok(Self {
            maps,
            old_version_probes: 0,
            next_global_id: count as u64,
            max_rows,
        })
    }
    pub fn maps(&self) -> &[Vec<u64>] {
        &self.maps
    }
}

/// Route and register one new global ID, then extend only its sole owner's map.
/// All lower trees must already pass full live-ID/map coverage admission.
pub fn append_owned_global(
    sessions: &mut [WriteSession<'_>],
    authorization: &str,
    ownership: &mut WriteOwnership,
    global_id: u64,
    vector: &[f32],
    boundaries: Option<&[Vec<f32>]>,
) -> Result<WriteDestination, String> {
    if global_id != ownership.next_global_id
        || global_id >= ownership.max_rows as u64
        || sessions.len() != ownership.maps.len()
    {
        return Err("new global ID, owner set or row budget differs from transaction".into());
    }
    let mut widths = Vec::<usize>::new();
    for (owner, session) in sessions.iter().enumerate() {
        session.authorized(authorization)?;
        for local in 0..ownership.maps[owner].len() {
            ownership.old_version_probes += 1;
            if !session
                .writer
                .current_version_sync(local as u32)
                .is_some_and(|v| v & super::DELETED_BIT == 0)
            {
                return Err("ownership map contains a missing or deleted local ID".into());
            }
        }
        for (level, width) in session.writer.level_node_counts().into_iter().enumerate() {
            if widths.len() <= level {
                widths.resize(level + 1, 0);
            }
            widths[level] = widths[level]
                .checked_add(width)
                .ok_or("global width overflow")?;
        }
    }
    let selected =
        route_new_global_with_boundaries(sessions, authorization, vector, widths, boundaries)?;
    let local_id =
        u32::try_from(ownership.maps[selected.owner].len()).map_err(|_| "local ID overflow")?;
    sessions[selected.owner].register_new(authorization, selected.leaf, local_id, vector)?;
    ownership.maps[selected.owner].push(global_id);
    ownership.next_global_id += 1;
    Ok(selected)
}

/// Counts and new assignments for an explicitly pre-audited immutable parent.
/// Counts do not prove the parent's disjoint ownership or live-version coverage.
/// The experiment must establish those properties with a separate full audit.
pub struct CountedWriteOwnership {
    initial_counts: Vec<usize>,
    additions: Vec<Vec<u64>>,
    next_global_id: u64,
    max_rows: usize,
}
impl CountedWriteOwnership {
    pub fn from_audited_counts(counts: Vec<usize>, max_rows: usize) -> Result<Self, String> {
        if counts.is_empty() || counts.len() > 128 || max_rows == 0 || max_rows > 100_000 {
            return Err("bounded audited parent counts required".into());
        }
        let total = counts.iter().try_fold(0usize, |n, &v| n.checked_add(v))
            .ok_or("ownership count overflow")?;
        if total > max_rows || counts.iter().any(|&n| u32::try_from(n).is_err()) {
            return Err("ownership count exceeds row or local namespace budget".into());
        }
        Ok(Self { additions: vec![Vec::new(); counts.len()], initial_counts: counts,
            next_global_id: total as u64, max_rows })
    }
    pub fn count(&self, owner: usize) -> usize {
        self.initial_counts[owner] + self.additions[owner].len()
    }
    pub fn additions(&self, owner: usize) -> &[u64] { &self.additions[owner] }
    pub fn next_global_id(&self) -> u64 { self.next_global_id }
}

/// Register using exactly the existing global route, without rechecking OLD IDs.
/// Actual selected-leaf registration still checks the NEW local ID and leaf.
/// No balancing occurs here; topology width validation remains per request.
pub fn append_counted_global(
    sessions: &mut [WriteSession<'_>], authorization: &str,
    ownership: &mut CountedWriteOwnership, global_id: u64, vector: &[f32],
    boundaries: Option<&[Vec<f32>]>,
) -> Result<WriteDestination, String> {
    if global_id != ownership.next_global_id || global_id >= ownership.max_rows as u64
        || sessions.len() != ownership.initial_counts.len() {
        return Err("new global ID, owner set or row budget differs from transaction".into());
    }
    let mut widths = Vec::<usize>::new();
    for session in sessions.iter() {
        session.authorized(authorization)?;
        for (level, width) in session.writer.level_node_counts().into_iter().enumerate() {
            if widths.len() <= level { widths.resize(level + 1, 0); }
            widths[level] = widths[level].checked_add(width).ok_or("global width overflow")?;
        }
    }
    let selected = route_new_global_with_boundaries(sessions, authorization, vector, widths, boundaries)?;
    let local_id = u32::try_from(ownership.count(selected.owner)).map_err(|_| "local ID overflow")?;
    sessions[selected.owner].register_new(authorization, selected.leaf, local_id, vector)?;
    ownership.additions[selected.owner].push(global_id);
    ownership.next_global_id += 1;
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::super::super::{
        common::{InternalNode, LeafNode},
        config::HierarchicalSpannConfig,
    };
    use super::*;
    use chroma_distance::DistanceFunction;

    fn fixture() -> HierarchicalSpannWriter {
        let mut config = HierarchicalSpannConfig::default();
        config.max_replicas = 1;
        let writer = HierarchicalSpannWriter::new(1, DistanceFunction::Euclidean, config);
        writer.nodes.clear();
        for (id, centroid, children, parent_id) in [
            (0, 0.0, vec![1, 2], None),
            (1, 1.0, vec![3], Some(0)),
            (2, 2.0, vec![4], Some(0)),
        ] {
            writer.nodes.insert(
                id,
                TreeNode::Internal(InternalNode {
                    centroid: vec![centroid],
                    centroid_code: vec![],
                    children,
                    parent_id,
                }),
            );
        }
        for (id, centroid, parent) in [(3, 3.0, 1), (4, 0.125, 2)] {
            writer.nodes.insert(
                id,
                TreeNode::Leaf(LeafNode {
                    centroid: vec![centroid],
                    centroid_code: vec![],
                    ids: vec![],
                    versions: vec![],
                    codes: vec![],
                    length: 0,
                    parent_id: Some(parent),
                }),
            );
        }
        writer
    }

    #[test]
    fn counted_parent_matches_full_prefix_after_every_append() {
        let mut full_writers = [fixture(), fixture()];
        let mut counted_writers = [fixture(), fixture()];
        let mut full = WriteOwnership::new(vec![vec![],vec![]], 32).unwrap();
        let mut counted = CountedWriteOwnership::from_audited_counts(vec![0,0],32).unwrap();
        let boundaries = vec![vec![0.0],vec![1.0]];
        let initial_widths:Vec<_> = counted_writers.iter().map(|w|w.level_node_counts()).collect();
        for (id,x) in [0.0,1.0,-0.0,3.0,0.125,2.0,1.0,0.0].into_iter().enumerate() {
            let mut a:Vec<_> = full_writers.iter_mut().map(|w|w.write_session("f".into()).unwrap()).collect();
            let mut b:Vec<_> = counted_writers.iter_mut().map(|w|w.write_session("f".into()).unwrap()).collect();
            let expected=append_owned_global(&mut a,"f",&mut full,id as u64,&[x],Some(&boundaries)).unwrap();
            let actual=append_counted_global(&mut b,"f",&mut counted,id as u64,&[x],Some(&boundaries)).unwrap();
            assert_eq!((actual.owner,actual.leaf,actual.distance.to_bits()),(expected.owner,expected.leaf,expected.distance.to_bits()));
            drop(a);drop(b);
            for owner in 0..2 {
                assert_eq!(full.maps()[owner],counted.additions(owner));
                assert_eq!(full.maps()[owner].len(),counted.count(owner));
                assert_eq!(initial_widths[owner],counted_writers[owner].level_node_counts());
                assert_eq!(full_writers[owner].current_version_sync((counted.count(owner).saturating_sub(1)) as u32),counted_writers[owner].current_version_sync((counted.count(owner).saturating_sub(1)) as u32));
            }
        }
        assert_eq!(full.old_version_probes,28);
    }

    #[test]
    fn counted_parent_rejects_invalid_range_and_new_local_conflict() {
        assert!(CountedWriteOwnership::from_audited_counts(vec![usize::MAX,1],32).is_err());
        assert!(CountedWriteOwnership::from_audited_counts(vec![33],32).is_err());
        assert!(CountedWriteOwnership::from_audited_counts(vec![0;129],32).is_err());
        let mut writer=fixture();
        let mut counted=CountedWriteOwnership::from_audited_counts(vec![0],2).unwrap();
        let mut sessions=vec![writer.write_session("f".into()).unwrap()];
        assert!(append_counted_global(&mut sessions,"f",&mut counted,1,&[0.0],None).is_err());
        assert!(append_counted_global(&mut sessions,"wrong",&mut counted,0,&[0.0],None).is_err());
        sessions[0].register_new("f",4,0,&[0.0]).unwrap();
        assert!(append_counted_global(&mut sessions,"f",&mut counted,0,&[0.0],None).is_err());
        assert_eq!(counted.next_global_id(),0);assert!(counted.additions(0).is_empty());
    }

    #[test]
    fn global_leaf_selection_matches_direct_writer_and_registers_only_chosen_leaf() {
        let mut writer = fixture();
        let policy = ReadBeamPolicy::uniform(Some(1000.0), 2, 2);
        let direct = writer.navigate_f32(&[0.0], &policy);
        assert_eq!(direct[0].0, 4);
        {
            let mut session = writer
                .write_session("deployment+owner-fence".into())
                .unwrap();
            let first = session
                .expand("deployment+owner-fence", &[0], &[0.0])
                .unwrap();
            assert_eq!(first.len(), 2);
            assert_eq!(first[0].distance, 1.0);
            assert_eq!(first[1].distance, 4.0);
            let second = session
                .expand("deployment+owner-fence", &[1, 2], &[0.0])
                .unwrap();
            let chosen = second
                .iter()
                .min_by(|a, b| {
                    a.distance
                        .total_cmp(&b.distance)
                        .then(a.node_id.cmp(&b.node_id))
                })
                .unwrap();
            assert_eq!(chosen.node_id, direct[0].0);
            session
                .register_new("deployment+owner-fence", chosen.node_id, 0, &[0.0])
                .unwrap();
            assert!(session
                .register_new("deployment+owner-fence", 3, 0, &[0.0])
                .is_err());
        }
        assert_eq!(writer.current_version_sync(0), Some(1));
        for (node, expected) in [(3, vec![]), (4, vec![0])] {
            let leaf = writer.nodes.get(&node).unwrap();
            match leaf.value() {
                TreeNode::Leaf(leaf) => assert_eq!(leaf.ids, expected),
                _ => panic!("expected leaf"),
            }
        }
    }

    #[test]
    fn invalid_target_or_fence_never_consumes_a_document_id() {
        let mut writer = fixture();
        {
            let mut session = writer.write_session("pin".into()).unwrap();
            assert!(session.register_new("stale", 4, 0, &[0.0]).is_err());
            assert!(session.register_new("pin", 999, 0, &[0.0]).is_err());
            assert!(session.register_new("pin", 1, 0, &[0.0]).is_err());
            assert!(session.register_new("pin", 4, 0, &[f32::NAN]).is_err());
            assert!(session.expand("pin", &[1, 1], &[0.0]).is_err());
        }
        assert_eq!(writer.current_version_sync(0), None);
        assert!(writer.embeddings.is_empty());
    }

    #[test]
    fn global_policy_uses_global_level_widths_and_original_tau_arithmetic() {
        let policy =
            ReadBeamPolicy::with_level_overrides(Some(2.0), 1, 8, vec![], vec![50.0], vec![10]);
        assert_eq!(
            global_write_count(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0], &policy, 1).unwrap(),
            5
        );
        assert!(global_write_count(&[2.0, 1.0], &policy, 1).is_err());
        assert!(global_write_count(&[1.0], &policy, 0).is_err());
    }
    #[test]
    fn global_route_crosses_owner_boundary_and_uses_one_destination() {
        let mut a = fixture();
        let mut b = fixture();
        a.root_id.store(1, Ordering::Relaxed);
        b.root_id.store(2, Ordering::Relaxed);
        let mut sessions = vec![
            a.write_session("pin".into()).unwrap(),
            b.write_session("pin".into()).unwrap(),
        ];
        let selected = route_new_global(&sessions, "pin", &[0.0], vec![2, 2]).unwrap();
        assert_eq!(
            selected,
            WriteDestination {
                owner: 1,
                leaf: 4,
                distance: 0.015625
            }
        );
        sessions[selected.owner]
            .register_new("pin", selected.leaf, 0, &[0.0])
            .unwrap();
        drop(sessions);
        assert_eq!(a.current_version_sync(0), None);
        assert_eq!(b.current_version_sync(0), Some(1));
    }

    #[test]
    fn mixed_depth_and_root_leaf_keep_accumulated_global_candidates() {
        let mut a = fixture();
        let mut b = fixture();
        a.root_id.store(4, Ordering::Relaxed);
        b.root_id.store(1, Ordering::Relaxed);
        let sessions = vec![
            a.write_session("pin".into()).unwrap(),
            b.write_session("pin".into()).unwrap(),
        ];
        assert_eq!(
            route_new_global(&sessions, "pin", &[0.0], vec![2, 1])
                .unwrap()
                .owner,
            0
        );
    }

    #[test]
    fn equal_local_node_ids_use_stable_logical_owner_order() {
        let mut a = fixture();
        let mut b = fixture();
        a.root_id.store(4, Ordering::Relaxed);
        b.root_id.store(4, Ordering::Relaxed);
        let sessions = vec![
            a.write_session("pin".into()).unwrap(),
            b.write_session("pin".into()).unwrap(),
        ];
        let selected = route_new_global(&sessions, "pin", &[0.0], vec![2]).unwrap();
        assert_eq!((selected.owner, selected.leaf), (0, 4));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn globally_registered_vector_survives_flush_reopen_without_changing_old_snapshot() {
        use super::super::super::reader::HierarchicalSpannReader;
        use chroma_blockstore::{
            arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider,
        };
        use chroma_cache::new_cache_for_test;
        use chroma_storage::{local::LocalStorage, Storage};
        let directory = tempfile::tempdir().unwrap();
        let provider = BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
            Storage::Local(LocalStorage::new(directory.path().to_str().unwrap())),
            3 * 1024 * 1024,
            new_cache_for_test(),
            new_cache_for_test(),
            16,
        ));
        let mut config = HierarchicalSpannConfig::default();
        config.max_replicas = 1;
        let mut a = HierarchicalSpannWriter::new(4, DistanceFunction::Euclidean, config.clone());
        let mut b = HierarchicalSpannWriter::new(4, DistanceFunction::Euclidean, config.clone());
        a.add(0, &[2.0, 0.0, 0.0, 0.0]);
        b.add(0, &[4.0, 0.0, 0.0, 0.0]);
        let old_a = a
            .commit(&provider, None)
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
        let old_b = b
            .commit(&provider, None)
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
        let old_reader = HierarchicalSpannReader::open(
            &provider,
            old_a.clone(),
            DistanceFunction::Euclidean,
            config.clone(),
        )
        .await
        .unwrap();
        let selected = {
            let mut sessions = vec![
                a.write_session("parent-fence".into()).unwrap(),
                b.write_session("parent-fence".into()).unwrap(),
            ];
            let selected =
                route_new_global(&sessions, "parent-fence", &[1.0, 0.0, 0.0, 0.0], vec![2])
                    .unwrap();
            sessions[selected.owner]
                .register_new("parent-fence", selected.leaf, 1, &[1.0, 0.0, 0.0, 0.0])
                .unwrap();
            selected
        };
        assert_eq!(selected.owner, 0);
        let new_a = a
            .commit(&provider, Some(&old_a))
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
        let reader_a = HierarchicalSpannReader::open(
            &provider,
            new_a,
            DistanceFunction::Euclidean,
            config.clone(),
        )
        .await
        .unwrap();
        let reader_b =
            HierarchicalSpannReader::open(&provider, old_b, DistanceFunction::Euclidean, config)
                .await
                .unwrap();
        assert_eq!(
            old_reader.rerank_selected(&[0.0; 4], &[0]).await.unwrap(),
            vec![(0, 4.0)]
        );
        assert!(old_reader.rerank_selected(&[0.0; 4], &[1]).await.is_err());
        let mut global = Vec::new();
        // Explicit stable global ownership: owner a maps [0,2], b maps [1].
        for (reader, map) in [(&reader_a, vec![0u64, 2]), (&reader_b, vec![1u64])] {
            let ids: Vec<_> = (0..map.len() as u32).collect();
            for (id, distance) in reader.rerank_selected(&[0.0; 4], &ids).await.unwrap() {
                global.push((map[id as usize], distance));
            }
        }
        global.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        assert_eq!(global, vec![(2, 1.0), (0, 4.0), (1, 16.0)]);
    }
    #[test]
    fn compactor_tasks_round_trip_and_register_without_private_rerouting() {
        let mut writer = fixture();
        let mut session = writer
            .write_session("parent+owner-revision".into())
            .unwrap();
        let request:WriteRequest=serde_json::from_value(serde_json::json!({"request_id":"route-1","authorization":"parent+owner-revision","command":{"op":"expand","parents":[1,2],"vector":[0.0]}})).unwrap();
        let reply = session.execute(request).unwrap();
        let encoded = serde_json::to_value(reply).unwrap();
        assert_eq!(encoded["request_id"], "route-1");
        assert_eq!(encoded["result"]["children"].as_array().unwrap().len(), 2);
        let register:WriteRequest=serde_json::from_value(serde_json::json!({"request_id":"register-1","authorization":"parent+owner-revision","command":{"op":"register_new","leaf":4,"local_id":0,"vector":[0.0]}})).unwrap();
        assert!(matches!(
            session.execute(register).unwrap().result,
            WriteResult::Registered {
                leaf: 4,
                local_id: 0
            }
        ));
        assert!(serde_json::from_value::<WriteRequest>(serde_json::json!({"request_id":"unknown","authorization":"pin","command":{"op":"register_new","leaf":4,"local_id":1,"vector":[0.0],"reroute":true}})).is_err());
        let stale:WriteRequest=serde_json::from_value(serde_json::json!({"request_id":"stale","authorization":"old","command":{"op":"register_new","leaf":3,"local_id":1,"vector":[0.0]}})).unwrap();
        assert!(session.execute(stale).is_err());
        drop(session);
        assert_eq!(writer.current_version_sync(1), None);
    }
    #[test]
    fn committed_boundary_alias_controls_first_level_without_an_extra_level() {
        let mut a = fixture();
        let mut b = fixture();
        a.root_id.store(1, Ordering::Relaxed);
        b.root_id.store(2, Ordering::Relaxed);
        for writer in [&mut a, &mut b] {
            writer.config.write_beam_min = 1;
            writer.config.write_beam_max = 1;
        }
        let sessions = vec![
            a.write_session("pin".into()).unwrap(),
            b.write_session("pin".into()).unwrap(),
        ];
        assert_eq!(
            route_new_global(&sessions, "pin", &[0.0], vec![2, 2])
                .unwrap()
                .owner,
            0
        );
        let boundaries = vec![vec![100.0], vec![0.0]];
        let selected = route_new_global_with_boundaries(
            &sessions,
            "pin",
            &[0.0],
            vec![2, 2],
            Some(&boundaries),
        )
        .unwrap();
        assert_eq!((selected.owner, selected.leaf), (1, 4));
        assert!(route_new_global_with_boundaries(
            &sessions,
            "pin",
            &[0.0],
            vec![2, 2],
            Some(&boundaries[..1])
        )
        .is_err());
    }
    #[test]
    fn global_ownership_changes_only_after_successful_single_destination_registration() {
        let mut a = fixture();
        let mut b = fixture();
        a.root_id.store(1, Ordering::Relaxed);
        b.root_id.store(2, Ordering::Relaxed);
        let mut sessions = vec![
            a.write_session("pin".into()).unwrap(),
            b.write_session("pin".into()).unwrap(),
        ];
        let mut ownership = WriteOwnership::new(vec![vec![], vec![]], 1).unwrap();
        assert!(
            append_owned_global(&mut sessions, "stale", &mut ownership, 0, &[0.0], None).is_err()
        );
        assert_eq!(ownership.maps(), &[Vec::<u64>::new(), Vec::<u64>::new()]);
        let selected =
            append_owned_global(&mut sessions, "pin", &mut ownership, 0, &[0.0], None).unwrap();
        assert_eq!(selected.owner, 1);
        assert_eq!(ownership.maps(), &[vec![], vec![0]]);
        assert!(
            append_owned_global(&mut sessions, "pin", &mut ownership, 0, &[0.0], None).is_err()
        );
        assert!(
            append_owned_global(&mut sessions, "pin", &mut ownership, 1, &[0.0], None).is_err()
        );
        drop(sessions);
        assert_eq!(a.current_version_sync(0), None);
        assert_eq!(b.current_version_sync(0), Some(1));
    }

    #[test]
    fn duplicate_missing_or_unbounded_ownership_is_rejected() {
        assert!(WriteOwnership::new(vec![vec![0], vec![0]], 10).is_err());
        assert!(WriteOwnership::new(vec![vec![1]], 10).is_err());
        assert!(WriteOwnership::new(vec![vec![]], 100_001).is_err());
        let mut writer = fixture();
        let mut sessions = vec![writer.write_session("pin".into()).unwrap()];
        let mut ownership = WriteOwnership::new(vec![vec![0]], 10).unwrap();
        assert!(
            append_owned_global(&mut sessions, "pin", &mut ownership, 1, &[0.0], None).is_err()
        );
    }
    #[test]
    fn finite_alias_does_not_score_an_overflowing_actual_root_centroid() {
        let mut writer = fixture();
        writer.root_id.store(1, Ordering::Relaxed);
        if let TreeNode::Internal(root) = writer.nodes.get_mut(&1).unwrap().value_mut() {
            root.centroid = vec![-1e20];
        }
        let session = writer.write_session("pin".into()).unwrap();
        assert!(session.score_root("pin", &[1e20]).is_err());
        assert_eq!(
            session
                .score_boundary("pin", &[1e20], &[1e20])
                .unwrap()
                .distance,
            0.0
        );
    }
}
