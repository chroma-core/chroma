//! Exclusive private-build operations for one globally coordinated write route.
//!
//! A session borrows its writer exclusively, so balancing cannot change the
//! topology between expansion and registration. The caller owns deployment
//! fencing, global ID ownership, global level widths, and final publication.
use super::super::common::{effective_beam, NodeId, ReadBeamPolicy, TreeNode};
use super::HierarchicalSpannWriter;
use std::collections::HashSet;
use std::sync::{atomic::Ordering, Arc};

#[derive(Debug, Clone, PartialEq)]
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
        let root = session.score_root(authorization, vector)?;
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
}
