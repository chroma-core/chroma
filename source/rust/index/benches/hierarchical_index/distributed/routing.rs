//! A navigation driver owns one global beam. Owners score one expansion step;
//! their replies never apply independent beam truncation or contain postings.
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::super::common::ReadBeamPolicy;
use super::observability::{Counter, Scope, WorkProfile};
use super::protocol::{AuthorityId, NodeId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeRef {
    pub id: NodeId,
    pub authority: AuthorityId,
    /// Changes with ownership, independently of routing-record revisions.
    pub placement: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Child {
    pub target: NodeRef,
    pub centroid: Vec<f32>,
    pub leaf: bool,
}

/// Immutable routing image. Posting bytes live in a separate owner record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RoutingNode {
    pub target: NodeRef,
    pub parent: Option<NodeRef>,
    pub centroid: Vec<f32>,
    pub centroid_code: Vec<u8>,
    pub children: Vec<Child>,
    pub leaf: bool,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExpandRequest {
    pub navigation: u128,
    pub step: u64,
    pub query_slot: usize,
    pub query: Vec<f32>,
    /// The order is the reference's selected internal-node order.
    pub nodes: Vec<NodeRef>,
    pub run: u128,
    pub epoch: u64,
    pub shared_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScoredChild {
    pub parent: NodeId,
    pub ordinal: usize,
    pub target: NodeRef,
    pub leaf: bool,
    pub distance: f32,
    pub observed_revision: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ExpansionStatus {
    Complete,
    Stale,
    Publishing,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExpandReply {
    pub authority: AuthorityId,
    pub run: u128,
    pub epoch: u64,
    pub shared_sequence: u64,
    /// Exactly one count for every expanded node; an owner must return all its
    /// immediate child rows. Splitting a batch by parent is supported, while
    /// child chunks for the same parent require a separate assembly protocol.
    pub child_counts: Vec<(NodeId, usize)>,
    pub navigation: u128,
    pub step: u64,
    pub query_slot: usize,
    pub nodes: Vec<NodeRef>,
    pub status: ExpansionStatus,
    pub children: Vec<ScoredChild>,
}

/// The backend may issue parallel owner requests or direct calls. A returned
/// vector is a complete response to this batch, never a partial-success prefix.
pub trait ExpansionBackend: Sync {
    fn observer(&self) -> Option<&WorkProfile> {
        None
    }
    fn expand(
        &self,
        authority: AuthorityId,
        requests: Vec<ExpandRequest>,
    ) -> Result<Vec<ExpandReply>, String>;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NavigationWork {
    pub expansions: u64,
    pub distances: u64,
    pub owner_requests: u64,
    pub levels: u64,
}

struct Search {
    attempt: u128,
    frontier: Vec<NodeRef>,
    leaves: Vec<(NodeRef, f32)>,
}

/// Execute fixed, disjoint queries together to amortize transport framing while
/// preserving each query's independent global candidate selection. Root-leaf
/// distances are supplied by the coherent shared/root image at admission.
#[allow(clippy::too_many_arguments)]
pub fn navigate_batch<B: ExpansionBackend>(
    backend: &B,
    root: &RoutingNode,
    queries: &[Vec<f32>],
    policy: &ReadBeamPolicy,
    run: u128,
    epoch: u64,
    shared_sequence: u64,
    first_attempt: u128,
    excluded: Option<NodeId>,
) -> Result<(Vec<Vec<(NodeRef, f32)>>, NavigationWork), String> {
    let _observed = backend
        .observer()
        .map(|p| p.operation(Scope::NavigationBatch));
    if queries
        .iter()
        .any(|q| q.len() != root.centroid.len() || q.iter().any(|x| !x.is_finite()))
    {
        return Err("navigation requires finite vectors with the index dimension".into());
    }
    if root.leaf {
        // The caller scores a root leaf using its configured metric; zero is
        // sufficient for construction destination choice but not query output.
        return Err("a root leaf must be handled by the metric-aware owner entry point".into());
    }
    let mut searches = queries
        .iter()
        .enumerate()
        .map(|(slot, _)| {
            Ok(Search {
                attempt: first_attempt
                    .checked_add(slot as u128)
                    .ok_or("navigation attempt exhausted")?,
                frontier: vec![root.target],
                leaves: vec![],
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut work = NavigationWork::default();
    for level in 1..=128_u64 {
        let mut copy_work = backend
            .observer()
            .map(|p| p.operation(Scope::NavigationQueryCopy));
        let mut groups: BTreeMap<AuthorityId, Vec<ExpandRequest>> = BTreeMap::new();
        for (slot, search) in searches.iter().enumerate() {
            let mut by_owner: BTreeMap<AuthorityId, Vec<NodeRef>> = BTreeMap::new();
            for node in &search.frontier {
                by_owner.entry(node.authority).or_default().push(*node);
            }
            for (owner, nodes) in by_owner {
                if let Some(w) = copy_work.as_mut() {
                    w.add(
                        Counter::NavigationQueryBytesCopied,
                        queries[slot].len() as u64 * 4,
                    );
                }
                groups.entry(owner).or_default().push(ExpandRequest {
                    navigation: search.attempt,
                    step: level,
                    query_slot: slot,
                    query: queries[slot].clone(),
                    nodes,
                    run,
                    epoch,
                    shared_sequence,
                });
            }
        }
        drop(copy_work);
        if groups.is_empty() {
            for search in &mut searches {
                reference_sort(&mut search.leaves, |row| row.1)?;
            }
            return Ok((searches.into_iter().map(|s| s.leaves).collect(), work));
        }
        work.owner_requests += groups.len() as u64;
        // This scope joins all required replies before any query selects its
        // next beam, preserving reference ordering despite response order.
        let replies = if groups.len() == 1 {
            let (owner, batch) = groups.into_iter().next().unwrap();
            vec![(owner, backend.expand(owner, batch)?)]
        } else {
            std::thread::scope(|scope| {
                let jobs = groups
                    .into_iter()
                    .map(|(owner, batch)| {
                        scope.spawn(move || {
                            backend.expand(owner, batch).map(|replies| (owner, replies))
                        })
                    })
                    .collect::<Vec<_>>();
                jobs.into_iter()
                    .map(|job| {
                        job.join()
                            .map_err(|_| "owner expansion worker panicked".to_string())?
                    })
                    .collect::<Result<Vec<_>, String>>()
            })?
        };
        let mut validation = backend
            .observer()
            .map(|p| p.operation(Scope::NavigationReplyValidation));
        let mut candidates: Vec<Vec<ScoredChild>> = vec![vec![]; searches.len()];
        let mut received: Vec<BTreeSet<NodeId>> = vec![BTreeSet::new(); searches.len()];
        let mut seen = BTreeMap::new();
        for (authority, owner_replies) in replies {
            for reply in owner_replies {
                if let Some(w) = validation.as_mut() {
                    w.add(Counter::NavigationRepliesValidated, 1);
                    w.add(
                        Counter::NavigationChildRowsValidated,
                        reply.children.len() as u64,
                    );
                }
                let slot = reply.query_slot;
                let search = searches
                    .get(slot)
                    .ok_or("expansion returned an unknown query slot")?;
                if reply.authority != authority
                    || reply.run != run
                    || reply.epoch != epoch
                    || reply.shared_sequence != shared_sequence
                {
                    return Err(
                        "expansion reply belongs to another owner or construction view".into(),
                    );
                }
                if reply.navigation != search.attempt || reply.step != level {
                    return Err("late or incompatible expansion reply".into());
                }
                if reply.status != ExpansionStatus::Complete {
                    return Err(
                        "navigation route changed; discard the complete attempt and retry".into(),
                    );
                }
                if reply.nodes.is_empty() {
                    return Err("empty expansion reply".into());
                }
                if let Some(w) = validation.as_mut() {
                    w.add(
                        Counter::NavigationReplyBytesCloned,
                        (reply.nodes.len() * std::mem::size_of::<NodeRef>()) as u64,
                    );
                }
                let key = (slot, reply.nodes.clone());
                if let Some(previous) = seen.get(&key) {
                    if previous != &reply {
                        return Err("conflicting duplicate expansion reply".into());
                    }
                    continue;
                }
                if let Some(w) = validation.as_mut() {
                    w.add(
                        Counter::NavigationReplyBytesCloned,
                        (reply.nodes.len() * std::mem::size_of::<NodeRef>()
                            + reply.children.len() * std::mem::size_of::<ScoredChild>()
                            + reply.child_counts.len() * std::mem::size_of::<(NodeId, usize)>())
                            as u64,
                    );
                }
                seen.insert(key, reply.clone());
                let counts = reply
                    .child_counts
                    .iter()
                    .copied()
                    .collect::<BTreeMap<_, _>>();
                if counts.len() != reply.child_counts.len()
                    || counts.len() != reply.nodes.len()
                    || reply
                        .nodes
                        .iter()
                        .any(|node| node.authority != authority || !counts.contains_key(&node.id))
                {
                    return Err("invalid expansion child counts or owner".into());
                }
                for node in &reply.nodes {
                    if !search.frontier.contains(node) || !received[slot].insert(node.id) {
                        return Err("conflicting or overlapping expansion reply".into());
                    }
                }
                for child in &reply.children {
                    if !reply.nodes.iter().any(|n| n.id == child.parent)
                        || !child.distance.is_finite()
                    {
                        return Err("invalid expansion child".into());
                    }
                }
                for node in &reply.nodes {
                    let children = reply
                        .children
                        .iter()
                        .filter(|c| c.parent == node.id)
                        .collect::<Vec<_>>();
                    let count = counts[&node.id];
                    let ordinals = children.iter().map(|c| c.ordinal).collect::<BTreeSet<_>>();
                    if children.len() != count
                        || ordinals.len() != count
                        || ordinals.iter().copied().ne(0..count)
                    {
                        return Err("partial or overlapping expansion child rows".into());
                    }
                    if let Some(first) = children.first() {
                        if children
                            .iter()
                            .any(|c| c.observed_revision != first.observed_revision)
                        {
                            return Err("mixed parent revisions in expansion".into());
                        }
                    }
                }
                candidates[slot].extend(reply.children);
            }
        }
        drop(validation);
        let mut candidate_work = backend
            .observer()
            .map(|p| p.operation(Scope::NavigationCandidates));
        for (slot, search) in searches.iter_mut().enumerate() {
            if search
                .frontier
                .iter()
                .any(|n| !received[slot].contains(&n.id))
            {
                return Err(
                    "missing expansion reply; partial global selection is forbidden".into(),
                );
            }
            let order = search
                .frontier
                .iter()
                .enumerate()
                .map(|(i, n)| (n.id, i))
                .collect::<BTreeMap<_, _>>();
            let rows = &mut candidates[slot];
            if let Some(w) = candidate_work.as_mut() {
                w.add(Counter::NavigationCandidateRows, rows.len() as u64);
            }
            rows.sort_unstable_by_key(|c| (order[&c.parent], c.ordinal));
            let mut ordinals = BTreeSet::new();
            if rows.iter().any(|c| !ordinals.insert((c.parent, c.ordinal))) {
                return Err("duplicate child ordinal in expansion".into());
            }
            work.expansions += search.frontier.len() as u64;
            work.distances += rows.len() as u64;
            rows.retain(|c| Some(c.target.id) != excluded);
            // Equal scores intentionally use the same unstable comparator as
            // the reference, after reconstructing the same input order.
            reference_sort(rows, |row| row.distance)?;
            let params = policy.level_params(level as usize);
            let limit = if params.tau.is_some() {
                params.beam_max
            } else {
                params.beam_min
            };
            let floor = params.beam_min.min(limit);
            let best = rows.first().map(|r| r.distance.max(1e-10));
            search.frontier.clear();
            for (selected, row) in rows.iter().enumerate() {
                if selected == limit {
                    break;
                }
                if selected >= floor
                    && params.tau.is_some_and(|tau| {
                        !matches!(
                            row.distance.partial_cmp(&(best.unwrap() * tau as f32)),
                            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                        )
                    })
                {
                    break;
                }
                if row.leaf {
                    search.leaves.push((row.target, row.distance));
                } else {
                    search.frontier.push(row.target);
                }
            }
        }
        work.levels += 1;
    }
    if searches.iter().all(|s| s.frontier.is_empty()) {
        for search in &mut searches {
            reference_sort(&mut search.leaves, |row| row.1)?;
        }
        return Ok((searches.into_iter().map(|s| s.leaves).collect(), work));
    }
    Err("navigation exceeded the depth/progress bound".into())
}

/// Rust's unstable sort can select different algorithms for differently sized
/// records. Sort the same eight-byte (u32,f32) representation as the reference,
/// then gather wide-ID rows in that order, preserving ties exactly.
fn reference_sort<T>(rows: &mut Vec<T>, distance: impl Fn(&T) -> f32) -> Result<(), String> {
    let mut order = rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            Ok((
                u32::try_from(i).map_err(|_| "navigation candidate count exhausted")?,
                distance(row),
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    order.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    let mut original = std::mem::take(rows)
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();
    rows.extend(
        order
            .into_iter()
            .map(|(i, _)| original[i as usize].take().unwrap()),
    );
    Ok(())
}

#[cfg(test)]
#[path = "routing_tests.rs"]
mod tests;
