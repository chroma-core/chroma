//! A navigation driver owns one global beam. Owners score one expansion step;
//! their replies never apply independent beam truncation or contain postings.
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::super::common::ReadBeamPolicy;
use super::observability::{Counter, Scope, WorkProfile};
use super::protocol::{AuthorityId, NodeId};

#[path = "expansion_codec.rs"]
pub(super) mod expansion_codec;

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
#[derive(Clone, Debug, Serialize)]
pub struct RoutingNode {
    pub target: NodeRef,
    pub parent: Option<NodeRef>,
    pub centroid: Vec<f32>,
    pub centroid_code: Vec<u8>,
    pub children: Vec<Child>,
    pub leaf: bool,
    pub revision: u64,
    /// Prepared at publication or deserialization, never while scoring a query.
    /// This cache is not part of the durable/wire representation or identity.
    #[serde(skip)]
    pub navigation: Arc<PackedChildNavigation>,
}

/// Child identities and row-major full-precision centroids are one immutable
/// scoring payload. Its private fields prevent pairing rows from two revisions.
#[derive(Clone, Debug, Default)]
pub struct PackedChildNavigation {
    dimension: usize,
    rows: Box<[(NodeRef, bool)]>,
    centroids: Box<[f32]>,
}
impl RoutingNode {
    pub fn prepare_navigation(&mut self) -> Result<(), String> {
        let dimension = self.centroid.len();
        if !self.children.is_empty() && dimension == 0 {
            return Err("child navigation requires a positive dimension".into());
        }
        let mut rows = Vec::with_capacity(self.children.len());
        let capacity = self
            .children
            .len()
            .checked_mul(dimension)
            .ok_or("packed centroid size overflow")?;
        let mut centroids = Vec::with_capacity(capacity);
        for child in &self.children {
            if child.centroid.len() != dimension || child.centroid.iter().any(|x| !x.is_finite()) {
                return Err("invalid packed child centroid".into());
            }
            rows.push((child.target, child.leaf));
            centroids.extend_from_slice(&child.centroid);
        }
        self.navigation = Arc::new(PackedChildNavigation {
            dimension,
            rows: rows.into_boxed_slice(),
            centroids: centroids.into_boxed_slice(),
        });
        Ok(())
    }
    /// An immutable published parent supplies both metadata and matching rows.
    pub fn scored_children(
        &self,
    ) -> Result<impl Iterator<Item = (usize, NodeRef, bool, &[f32])>, String> {
        if self.navigation.rows.len() != self.children.len()
            || self.navigation.dimension != self.centroid.len()
        {
            return Err("routing child navigation was not prepared for publication".into());
        }
        Ok(self
            .navigation
            .rows
            .iter()
            .copied()
            .zip(
                self.navigation
                    .centroids
                    .chunks_exact(self.navigation.dimension.max(1)),
            )
            .enumerate()
            .map(|(ordinal, ((target, leaf), centroid))| (ordinal, target, leaf, centroid)))
    }
}
impl PartialEq for RoutingNode {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target
            && self.parent == other.parent
            && self.centroid == other.centroid
            && self.centroid_code == other.centroid_code
            && self.children == other.children
            && self.leaf == other.leaf
            && self.revision == other.revision
    }
}
// Preserve the existing serialized field order and rebuild only the derived
// scoring buffer on receipt. Old saved/RPC payloads require no format migration.
impl<'de> Deserialize<'de> for RoutingNode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            target: NodeRef,
            parent: Option<NodeRef>,
            centroid: Vec<f32>,
            centroid_code: Vec<u8>,
            children: Vec<Child>,
            leaf: bool,
            revision: u64,
        }
        let fields = Fields::deserialize(deserializer)?;
        let mut node = Self {
            target: fields.target,
            parent: fields.parent,
            centroid: fields.centroid,
            centroid_code: fields.centroid_code,
            children: fields.children,
            leaf: fields.leaf,
            revision: fields.revision,
            navigation: Default::default(),
        };
        node.prepare_navigation()
            .map_err(serde::de::Error::custom)?;
        Ok(node)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpandRequest {
    pub navigation: u128,
    pub step: u64,
    pub query_slot: usize,
    // Required versioned keys reject mixed endpoint binaries rather than
    // treating a legacy numeric array as the new lossless representation.
    #[serde(rename = "query_f32le_v1_base64", with = "expansion_codec::query")]
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
#[serde(deny_unknown_fields)]
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
    #[serde(rename = "children_le_v1_base64", with = "expansion_codec::children")]
    pub children: Vec<ScoredChild>,
}

/// The backend may issue parallel owner requests or direct calls. A returned
/// vector is a complete response to this batch, never a partial-success prefix.
pub trait ExpansionBackend: Sync {
    fn observer(&self) -> Option<&WorkProfile> {
        None
    }
    /// Trusted in-process images append in parent/child order with a borrowed
    /// query. Nodes not resident here remain subject to the remote protocol.
    /// A stale image fails the complete attempt rather than selecting a prefix.
    fn expand_local_nodes(
        &self,
        nodes: &[NodeRef],
        _query: &[f32],
        _run: u128,
        _epoch: u64,
        _shared_sequence: u64,
        _children: &mut Vec<ScoredChild>,
        remote: &mut Vec<NodeRef>,
    ) -> Result<(), String> {
        remote.extend_from_slice(nodes);
        Ok(())
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
    /// Actual child visits in remote reply validation; excludes candidate sorting.
    pub reply_validation_row_visits: u64,
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
    let cache_parent = cache_parent_lookups();
    navigate_batch_with_assembly(
        backend,
        root,
        queries,
        policy,
        run,
        epoch,
        shared_sequence,
        first_attempt,
        excluded,
        |frontier, rows| {
            canonicalize_candidates_with_cache(frontier, rows, cache_parent).map(|_| ())
        },
    )
}

// The assembler seam lets tests substitute the retained ordering oracle. The
// production call is monomorphized with the canonical scatter above.
#[allow(clippy::too_many_arguments)]
fn navigate_batch_with_assembly<B: ExpansionBackend>(
    backend: &B,
    root: &RoutingNode,
    queries: &[Vec<f32>],
    policy: &ReadBeamPolicy,
    run: u128,
    epoch: u64,
    shared_sequence: u64,
    first_attempt: u128,
    excluded: Option<NodeId>,
    mut assemble: impl FnMut(&[NodeRef], &mut Vec<ScoredChild>) -> Result<(), String>,
) -> Result<(Vec<Vec<(NodeRef, f32)>>, NavigationWork), String> {
    let cache_parent = cache_parent_lookups();
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
        if searches.iter().all(|search| search.frontier.is_empty()) {
            for search in &mut searches {
                reference_sort(&mut search.leaves, |row| row.1, backend.observer())?;
            }
            return Ok((searches.into_iter().map(|s| s.leaves).collect(), work));
        }
        let mut candidates: Vec<Vec<ScoredChild>> = vec![vec![]; searches.len()];
        let mut remote_frontiers: Vec<Vec<NodeRef>> = vec![vec![]; searches.len()];
        let mut groups: BTreeMap<AuthorityId, Vec<ExpandRequest>> = BTreeMap::new();
        for (slot, search) in searches.iter().enumerate() {
            let mut by_owner: BTreeMap<AuthorityId, Vec<NodeRef>> = BTreeMap::new();
            backend.expand_local_nodes(
                &search.frontier,
                &queries[slot],
                run,
                epoch,
                shared_sequence,
                &mut candidates[slot],
                &mut remote_frontiers[slot],
            )?;
            for node in &remote_frontiers[slot] {
                by_owner.entry(node.authority).or_default().push(*node);
            }
            let mut copy_work = (!by_owner.is_empty())
                .then(|| {
                    backend
                        .observer()
                        .map(|p| p.operation(Scope::NavigationQueryCopy))
                })
                .flatten();
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
        work.owner_requests += groups.len() as u64;
        // This scope joins all required replies before any query selects its
        // next beam, preserving reference ordering despite response order.
        let replies = if groups.is_empty() {
            vec![]
        } else if groups.len() == 1 {
            let (owner, batch) = groups.into_iter().next().unwrap();
            vec![(owner, backend.expand(owner, batch)?)]
        } else {
            std::thread::scope(|scope| {
                let jobs = groups
                    .into_iter()
                    .map(|(owner, batch)| {
                        let context = super::observability::current_context();
                        scope.spawn(move || {
                            let _context = super::observability::install_context(context);
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
        let mut validation = (!replies.is_empty())
            .then(|| {
                backend
                    .observer()
                    .map(|p| p.operation(Scope::NavigationReplyValidation))
            })
            .flatten();
        // Keep returned bodies stable until every duplicate is compared. The
        // index borrows their ordered parent keys and bodies; no child vector
        // is cloned merely to remember an earlier reply.
        let expected = remote_frontiers
            .iter()
            .map(|nodes| {
                nodes
                    .iter()
                    .map(|node| (node.id, *node))
                    .collect::<HashMap<_, _>>()
            })
            .collect::<Vec<_>>();
        let mut received: Vec<BTreeSet<NodeId>> = vec![BTreeSet::new(); searches.len()];
        let mut seen = BTreeMap::new();
        let mut accepted = Vec::with_capacity(replies.len());
        for (authority, owner_replies) in &replies {
            let mut owner_accepted = Vec::with_capacity(owner_replies.len());
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
                if reply.authority != *authority
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
                let key = (slot, reply.nodes.as_slice());
                if let Some(previous) = seen.get(&key) {
                    if *previous != reply {
                        return Err("conflicting duplicate expansion reply".into());
                    }
                    owner_accepted.push(false);
                    continue;
                }
                seen.insert(key, reply);
                validate_parent_coverage_with_cache(
                    reply,
                    *authority,
                    &expected[slot],
                    &mut received[slot],
                    &mut work.reply_validation_row_visits,
                    cache_parent,
                )?;
                owner_accepted.push(true);
            }
            accepted.push(owner_accepted);
        }
        drop(seen);
        // All borrowed duplicate records are gone. Move each accepted body's
        // original rows into the candidates, retaining canonical assembly and
        // global selection below. A later invalid reply never exposes a prefix.
        for ((_, owner_replies), owner_accepted) in replies.into_iter().zip(accepted) {
            for (reply, accepted) in owner_replies.into_iter().zip(owner_accepted) {
                if accepted {
                    candidates[reply.query_slot].extend(reply.children);
                }
            }
        }
        drop(validation);
        let mut candidate_work = backend
            .observer()
            .map(|p| p.operation(Scope::NavigationCandidates));
        for (slot, search) in searches.iter_mut().enumerate() {
            if remote_frontiers[slot]
                .iter()
                .any(|n| !received[slot].contains(&n.id))
            {
                return Err(
                    "missing expansion reply; partial global selection is forbidden".into(),
                );
            }
            let rows = &mut candidates[slot];
            if let Some(w) = candidate_work.as_mut() {
                w.add(Counter::NavigationCandidateRows, rows.len() as u64);
            }
            if !remote_frontiers[slot].is_empty() {
                let _canonical = backend
                    .observer()
                    .filter(|_| super::observability::detailed())
                    .map(|p| p.operation(Scope::NavigationCanonicalOrder));
                assemble(&search.frontier, rows)?;
            }
            work.expansions += search.frontier.len() as u64;
            work.distances += rows.len() as u64;
            rows.retain(|c| Some(c.target.id) != excluded);
            // Equal scores intentionally use the same unstable comparator as
            // the reference, after reconstructing the same input order.
            reference_sort(rows, |row| row.distance, backend.observer())?;
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
            reference_sort(&mut search.leaves, |row| row.1, backend.observer())?;
        }
        return Ok((searches.into_iter().map(|s| s.leaves).collect(), work));
    }
    Err("navigation exceeded the depth/progress bound".into())
}

// Freeze this experimental switch once per process so control and candidate
// runs use the same executable without consulting the environment per row.
fn cache_parent_lookups() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("SPANN_NAVIGATION_PARENT_CACHE").as_deref() != Ok("0"))
}

#[cfg(test)]
fn validate_parent_coverage(
    reply: &ExpandReply,
    authority: AuthorityId,
    expected: &HashMap<NodeId, NodeRef>,
    received: &mut BTreeSet<NodeId>,
    row_visits: &mut u64,
) -> Result<(), String> {
    validate_parent_coverage_with_cache(reply, authority, expected, received, row_visits, true)
}

/// Check complete parent replies without rescanning all children per parent.
/// Ordinal storage is bounded by the actual received row count, never by an
/// unchecked declared count. Parent keys retain full placement identity.
fn validate_parent_coverage_with_cache(
    reply: &ExpandReply,
    authority: AuthorityId,
    expected: &HashMap<NodeId, NodeRef>,
    received: &mut BTreeSet<NodeId>,
    row_visits: &mut u64,
    cache_parent: bool,
) -> Result<(), String> {
    struct Coverage {
        expected: usize,
        seen: usize,
        revision: Option<u64>,
        ordinals: Vec<bool>,
    }
    let mut parents = HashMap::with_capacity(reply.child_counts.len());
    let mut total = 0usize;
    for &(parent, count) in &reply.child_counts {
        total = total
            .checked_add(count)
            .ok_or("expansion child count overflow")?;
        if parents.insert(parent, (count, 0usize)).is_some() {
            return Err("invalid expansion child counts or owner".into());
        }
    }
    if parents.len() != reply.nodes.len() {
        return Err("invalid expansion child counts or owner".into());
    }
    if total != reply.children.len() {
        return Err("partial or overlapping expansion child rows".into());
    }
    let mut coverage = Vec::with_capacity(reply.nodes.len());
    for node in &reply.nodes {
        if node.authority != authority {
            return Err("invalid expansion child counts or owner".into());
        }
        let (count, index) = parents
            .get_mut(&node.id)
            .ok_or("invalid expansion child counts or owner")?;
        if expected.get(&node.id) != Some(node) || !received.insert(node.id) {
            return Err("conflicting or overlapping expansion reply".into());
        }
        *index = coverage.len();
        coverage.push(Coverage {
            expected: *count,
            seen: 0,
            revision: None,
            ordinals: vec![false; *count],
        });
    }
    // Owners emit consecutive children of each parent. Cache that lookup while
    // still accepting interleaved rows and checking every child's contents.
    let mut last_parent = None;
    for child in &reply.children {
        *row_visits += 1;
        let index = match last_parent {
            Some((id, index)) if cache_parent && id == child.parent => index,
            _ => {
                let &(_, index) = parents
                    .get(&child.parent)
                    .ok_or("invalid expansion child")?;
                last_parent = Some((child.parent, index));
                index
            }
        };
        let parent = &mut coverage[index];
        if !child.distance.is_finite() {
            return Err("invalid expansion child".into());
        }
        let occupied = parent
            .ordinals
            .get_mut(child.ordinal)
            .ok_or("partial or overlapping expansion child rows")?;
        if *occupied {
            return Err("partial or overlapping expansion child rows".into());
        }
        if parent
            .revision
            .is_some_and(|revision| revision != child.observed_revision)
        {
            return Err("mixed parent revisions in expansion".into());
        }
        *occupied = true;
        parent.revision = Some(child.observed_revision);
        parent.seen += 1;
    }
    if coverage.iter().any(|parent| parent.seen != parent.expected) {
        return Err("partial or overlapping expansion child rows".into());
    }
    Ok(())
}

/// Place scored rows in frontier-parent order and then child-ordinal order.
/// Counts come from actual rows, never untrusted declared child counts. Remote
/// validation has already proved complete parent replies; occupied slots also
/// reject duplicates across local and remote rows. Return the number of parent
/// lookups so tests can compare reconstruction work without timing thresholds.
#[cfg(test)]
fn canonicalize_candidates(
    frontier: &[NodeRef],
    rows: &mut Vec<ScoredChild>,
) -> Result<usize, String> {
    canonicalize_candidates_with_cache(frontier, rows, true)
}

fn canonicalize_candidates_with_cache(
    frontier: &[NodeRef],
    rows: &mut Vec<ScoredChild>,
    cache_parent: bool,
) -> Result<usize, String> {
    if rows.len() > u32::MAX as usize {
        return Err("navigation candidate count exhausted".into());
    }
    let mut parents = HashMap::new();
    parents
        .try_reserve(frontier.len())
        .map_err(|_| "navigation parent allocation failed")?;
    for (position, parent) in frontier.iter().enumerate() {
        if parents.insert(parent.id, position).is_some() {
            return Err("duplicate frontier parent in expansion".into());
        }
    }
    let mut counts = Vec::new();
    counts
        .try_reserve_exact(frontier.len())
        .map_err(|_| "navigation count allocation failed")?;
    counts.resize(frontier.len(), 0usize);
    let mut positions = Vec::new();
    positions
        .try_reserve_exact(rows.len())
        .map_err(|_| "navigation position allocation failed")?;
    let mut last_parent = None;
    let mut lookups = 0;
    for child in rows.iter() {
        let position = match last_parent {
            Some((id, position)) if cache_parent && id == child.parent => position,
            _ => {
                let position = *parents
                    .get(&child.parent)
                    .ok_or("unknown parent in expansion child rows")?;
                last_parent = Some((child.parent, position));
                lookups += 1;
                position
            }
        };
        counts[position] += 1;
        positions.push(position);
    }
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(counts.len())
        .map_err(|_| "navigation offset allocation failed")?;
    let mut next = 0usize;
    for count in &counts {
        offsets.push(next);
        next = next
            .checked_add(*count)
            .ok_or("navigation candidate count exhausted")?;
    }
    let mut ordered = Vec::new();
    ordered
        .try_reserve_exact(rows.len())
        .map_err(|_| "navigation candidate allocation failed")?;
    ordered.resize(rows.len(), u32::MAX);
    for (source, (child, position)) in rows.iter().zip(positions).enumerate() {
        if child.ordinal >= counts[position] {
            return Err("partial or out-of-range expansion child ordinal".into());
        }
        let slot = &mut ordered[offsets[position] + child.ordinal];
        if *slot != u32::MAX {
            return Err("duplicate child ordinal in expansion".into());
        }
        *slot = source as u32;
    }
    if ordered.contains(&u32::MAX) {
        return Err("incomplete expansion child ordinals".into());
    }
    reorder_rows(rows, ordered.into_iter())?;
    Ok(lookups)
}

/// Reorder existing row storage using compact source-to-destination indices.
/// The complete permutation is checked before any row moves. Each swap places
/// at least one row at its final destination, so movement is linear in row count.
fn reorder_rows<T>(rows: &mut [T], order: impl IntoIterator<Item = u32>) -> Result<(), String> {
    if rows.len() > u32::MAX as usize {
        return Err("navigation candidate count exhausted".into());
    }
    let mut destinations = Vec::new();
    destinations
        .try_reserve_exact(rows.len())
        .map_err(|_| "navigation permutation allocation failed")?;
    destinations.resize(rows.len(), u32::MAX);
    let mut count = 0usize;
    for (destination, source) in order.into_iter().enumerate() {
        if destination >= rows.len()
            || source as usize >= rows.len()
            || destinations[source as usize] != u32::MAX
        {
            return Err("invalid navigation permutation".into());
        }
        destinations[source as usize] = destination as u32;
        count += 1;
    }
    if count != rows.len() {
        return Err("incomplete navigation permutation".into());
    }
    for source in 0..rows.len() {
        while destinations[source] as usize != source {
            let destination = destinations[source] as usize;
            rows.swap(source, destination);
            destinations.swap(source, destination);
        }
    }
    Ok(())
}

/// Rust's unstable sort can select different algorithms for differently sized
/// records. Sort the same eight-byte (u32,f32) representation as the reference,
/// then gather wide-ID rows in that order, preserving ties exactly.
fn reference_sort<T>(
    rows: &mut Vec<T>,
    distance: impl Fn(&T) -> f32,
    profile: Option<&super::observability::WorkProfile>,
) -> Result<(), String> {
    let profile = profile.filter(|_| super::observability::detailed());
    let allocation = profile.map(|p| p.operation(Scope::NavigationAllocationCopy));
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
    drop(allocation);
    let sort = profile.map(|p| p.operation(Scope::NavigationSort));
    order.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    drop(sort);
    let _gather = profile.map(|p| p.operation(Scope::NavigationGather));
    reorder_rows(rows, order.into_iter().map(|(source, _)| source))?;
    Ok(())
}

#[cfg(test)]
#[path = "routing_tests.rs"]
mod tests;

#[cfg(test)]
mod compact_gather_tests {
    use super::*;
    #[test]
    fn compact_gather_matches_wide_gather_with_ties_and_noncopy_rows() {
        for length in [0, 1, 2, 7, 31, 256, 1024] {
            for seed in 0..16usize {
                let original = (0..length)
                    .map(|i| (format!("row-{i}"), ((i * 104729 + seed * 31) % 17) as f32))
                    .collect::<Vec<_>>();
                let mut order = original
                    .iter()
                    .enumerate()
                    .map(|(i, row)| (i as u32, row.1))
                    .collect::<Vec<_>>();
                order.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                let expected = order
                    .iter()
                    .map(|(i, _)| original[*i as usize].clone())
                    .collect::<Vec<_>>();
                let mut actual = original;
                reference_sort(&mut actual, |row| row.1, None).unwrap();
                assert_eq!(actual, expected);
            }
        }
    }
    #[test]
    fn compact_gather_rejects_incomplete_duplicate_and_out_of_range_before_moving() {
        for order in [vec![0], vec![0, 0, 2], vec![0, 1, 3], vec![0, 1, 2, 3]] {
            let mut rows = vec!["a".to_string(), "b".to_string(), "c".to_string()];
            let before = rows.clone();
            assert!(reorder_rows(&mut rows, order).is_err());
            assert_eq!(rows, before);
        }
        let mut rows = vec![0, 1, 2, 3];
        reorder_rows(&mut rows, [3, 0, 1, 2]).unwrap();
        assert_eq!(rows, vec![3, 0, 1, 2]);
    }
}
