//! Shared routing changes prepare owner-root parent edits, publish one compatible
//! upper-tree/directory view, then acknowledge installation before releasing it.
use super::construction::{BalanceJob, BoundaryView};
use super::engine::{Cluster, EngineRequest, EngineResponse, OwnerService};
use super::observability::{detailed, Counter, Scope};
use super::placement_runtime::{PlacementRequest, PlacementResponse};
use super::protocol::envelope::Envelope;
use super::protocol::publication::{PublicationProposal, PublicationStatus, PublishedView};
use super::protocol::reservation::{RegionManager, RegionRequest, ReservationStatus};
use super::protocol::{AttemptId, AuthorityId, NodeId, ProtocolError};
use super::routing::{Child, NodeRef, RoutingNode};
use chroma_index::{
    quantization::Code,
    spann::utils::{self, EmbeddingPoint},
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Bounded diagnostic samples; elapsed timestamps share this process's monotonic origin.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CentroidRefreshDiagnostic {
    pub owner: AuthorityId,
    pub root_id: String,
    pub root_owner: AuthorityId,
    pub root_placement: u64,
    pub attempt: AttemptId,
    pub start_nanos: u64,
    pub start_unix_nanos: u64,
    #[serde(default)]
    pub gate_wait_nanos: u64,
    pub turn_wait_nanos: u64,
    pub broadcast_nanos: u64,
    pub elapsed_nanos: u64,
    pub attempts: u64,
    pub stale_prepare_retries: u64,
    pub changed_retries: u64,
    pub published: bool,
}
static REFRESH_ORIGIN: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
static REFRESH_LONGEST: std::sync::OnceLock<Mutex<Vec<CentroidRefreshDiagnostic>>> =
    std::sync::OnceLock::new();
pub(super) fn centroid_refresh_diagnostics(owner: AuthorityId) -> Vec<CentroidRefreshDiagnostic> {
    REFRESH_LONGEST.get().map_or_else(Vec::new, |records| {
        records
            .lock()
            .iter()
            .filter(|r| r.owner == owner)
            .cloned()
            .collect()
    })
}
struct RefreshDiagnosticGuard {
    started: Instant,
    record: CentroidRefreshDiagnostic,
}
impl RefreshDiagnosticGuard {
    fn new(owner: AuthorityId, root: NodeRef, attempt: AttemptId) -> Self {
        let started = Instant::now();
        let origin = *REFRESH_ORIGIN.get_or_init(|| started);
        Self {
            started,
            record: CentroidRefreshDiagnostic {
                owner,
                root_id: root.id.to_string(),
                root_owner: root.authority,
                root_placement: root.placement,
                attempt,
                start_nanos: started
                    .duration_since(origin)
                    .as_nanos()
                    .min(u64::MAX as u128) as u64,
                start_unix_nanos: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
                    .min(u64::MAX as u128) as u64,
                gate_wait_nanos: 0,
                turn_wait_nanos: 0,
                broadcast_nanos: 0,
                elapsed_nanos: 0,
                attempts: 0,
                stale_prepare_retries: 0,
                changed_retries: 0,
                published: false,
            },
        }
    }
}
impl Drop for RefreshDiagnosticGuard {
    fn drop(&mut self) {
        self.record.elapsed_nanos = self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let mut records = REFRESH_LONGEST
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock();
        records.push(self.record.clone());
        records.sort_unstable_by_key(|record| std::cmp::Reverse(record.elapsed_nanos));
        records.truncate(16);
    }
}

// An early turn has no externally prepared owner work. Release it on any local
// failure before publication. Once published, failed broadcast must retain the
// turn so another publisher cannot overtake incomplete installation.
struct UnpublishedRefreshTurn<'a> {
    publication: &'a super::protocol::publication::Publication<BoundaryView>,
    attempt: AttemptId,
}
impl Drop for UnpublishedRefreshTurn<'_> {
    fn drop(&mut self) {
        if matches!(
            self.publication.status(self.attempt),
            Ok(PublicationStatus::NotPublished)
        ) {
            if self.publication.abort(self.attempt).is_ok() {
                let _ = self.publication.release_turn(self.attempt);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParentUpdate {
    pub root: NodeRef,
    pub expected_revision: u64,
    pub expected_parent: Option<NodeRef>,
    pub new_parent: Option<NodeRef>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SharedRequest {
    RefreshCentroid {
        envelope: Envelope,
        root: NodeRef,
    },
    Prepare {
        envelope: Envelope,
        updates: Vec<ParentUpdate>,
    },
    Finish {
        envelope: Envelope,
        updates: Vec<ParentUpdate>,
    },
    Status {
        envelope: Envelope,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SharedResponse {
    Refreshed { sequence: u64 },
    Ready,
    Applied { sequence: u64 },
    Aborted,
}
struct RefreshRecord {
    envelope: Envelope,
    root: NodeRef,
    result: Option<Result<u64, String>>,
}
struct SharedRecord {
    envelope: Envelope,
    updates: Vec<ParentUpdate>,
    images: Vec<RoutingNode>,
    manager: Arc<RegionManager>,
    state: SharedResponse,
}
#[derive(Default)]
pub(super) struct SharedOwnerState {
    execution: BTreeMap<AttemptId, Arc<Mutex<()>>>,
    records: BTreeMap<AttemptId, SharedRecord>,
    proposals: BTreeMap<AttemptId, (Envelope, Vec<ParentUpdate>)>,
    rejections: BTreeMap<AttemptId, String>,
    refreshes: BTreeMap<AttemptId, RefreshRecord>,
    refresh_gates: BTreeMap<NodeId, std::sync::Weak<Mutex<()>>>,
}
impl SharedOwnerState {
    fn refresh_gate(&mut self, root: NodeId, limit: usize) -> Result<Arc<Mutex<()>>, String> {
        // Upgrade while holding the map mutex so one root cannot acquire two gates.
        self.refresh_gates
            .retain(|_, gate| gate.strong_count() != 0);
        if let Some(gate) = self
            .refresh_gates
            .get(&root)
            .and_then(std::sync::Weak::upgrade)
        {
            return Ok(gate);
        }
        if self.refresh_gates.len() >= limit {
            return Err("boundary refresh gate capacity exceeded".into());
        }
        let gate = Arc::new(Mutex::new(()));
        self.refresh_gates.insert(root, Arc::downgrade(&gate));
        Ok(gate)
    }

    pub(super) fn ensure_quiescent(&self) -> Result<(), String> {
        if self
            .refreshes
            .values()
            .any(|r| !matches!(r.result, Some(Ok(_))))
            || self
                .records
                .values()
                .any(|r| r.state == SharedResponse::Ready)
        {
            Err("unfinished shared parent installation prevents save".into())
        } else {
            Ok(())
        }
    }
}
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn revision(node: &mut RoutingNode) -> Result<(), String> {
    node.revision = node
        .revision
        .checked_add(1)
        .ok_or("shared node revision exhausted")?;
    Ok(())
}
fn child(node: &RoutingNode) -> Child {
    Child {
        target: node.target,
        centroid: node.centroid.clone(),
        leaf: node.leaf,
    }
}
fn center(node: &mut RoutingNode, dim: usize) -> Result<(), String> {
    if node.children.is_empty() {
        return Ok(());
    }
    let mut centroid = vec![0.; dim];
    for c in &node.children {
        if c.centroid.len() != dim {
            return Err("shared centroid dimension mismatch".into());
        }
        for (sum, x) in centroid.iter_mut().zip(&c.centroid) {
            *sum += x;
        }
    }
    for x in &mut centroid {
        *x /= node.children.len() as f32;
    }
    node.centroid_code = Code::<4>::quantize(&centroid, &vec![0.; dim])
        .as_ref()
        .to_vec();
    node.centroid = centroid;
    Ok(())
}
impl OwnerService {
    pub(super) fn handle_shared(&self, request: SharedRequest) -> Result<SharedResponse, String> {
        let e = match &request {
            SharedRequest::Prepare { envelope, .. }
            | SharedRequest::Finish { envelope, .. }
            | SharedRequest::Status { envelope }
            | SharedRequest::RefreshCentroid { envelope, .. } => envelope,
        };
        self.check(e)?;
        if matches!(request, SharedRequest::Status { .. }) {
            let s = self.shared_state.lock();
            let record = s
                .records
                .get(&e.attempt)
                .ok_or("unknown shared parent attempt")?;
            if record.envelope != *e {
                return Err("conflicting shared status envelope".into());
            }
            return Ok(record.state);
        }
        let execution = {
            let mut s = self.shared_state.lock();
            if s.execution.len() >= 100_000 && !s.execution.contains_key(&e.attempt) {
                return Err("shared operation record bound reached".into());
            }
            s.execution
                .entry(e.attempt)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _execution = execution.lock();
        self.check(e)?;
        match request {
            SharedRequest::RefreshCentroid { envelope: e, root } => {
                if self.owner != self.cluster()?.coordinator {
                    return Err("centroid refresh requires coordinator".into());
                }
                {
                    let s = self.shared_state.lock();
                    if let Some(record) = s.refreshes.get(&e.attempt) {
                        if record.envelope != e || record.root != root {
                            return Err("conflicting centroid refresh duplicate".into());
                        }
                        if let Some(result) = &record.result {
                            return result
                                .clone()
                                .map(|sequence| SharedResponse::Refreshed { sequence });
                        }
                    }
                }
                self.admit_placement(&e, bincode::serialize(&root).map_err(err)?)?;
                self.shared_state.lock().refreshes.insert(
                    e.attempt,
                    RefreshRecord {
                        envelope: e.clone(),
                        root,
                        result: None,
                    },
                );
                let result = self.cluster()?.refresh_boundary_centroid_inner(root, &e);
                self.shared_state
                    .lock()
                    .refreshes
                    .get_mut(&e.attempt)
                    .unwrap()
                    .result = Some(result.clone());
                if result.is_ok() {
                    self.terminal_placement(&e)?;
                }
                result.map(|sequence| SharedResponse::Refreshed { sequence })
            }
            SharedRequest::Prepare {
                envelope: e,
                mut updates,
            } => {
                updates.sort_unstable_by_key(|u| u.root.id);
                if updates.is_empty()
                    || updates.windows(2).any(|w| w[0].root.id == w[1].root.id)
                    || updates.iter().any(|u| u.root.authority != self.owner)
                {
                    return Err("invalid shared parent update bundle".into());
                }
                {
                    let mut state = self.shared_state.lock();
                    if let Some((old, body)) = state.proposals.get(&e.attempt) {
                        if old != &e || body != &updates {
                            return Err("conflicting shared parent proposal identity".into());
                        }
                    } else {
                        state
                            .proposals
                            .insert(e.attempt, (e.clone(), updates.clone()));
                    }
                    if let Some(error) = state.rejections.get(&e.attempt) {
                        return Err(error.clone());
                    }
                    if let Some(record) = state.records.get(&e.attempt) {
                        if record.envelope != e || record.updates != updates {
                            return Err("conflicting shared Prepare duplicate".into());
                        }
                        return Ok(record.state);
                    }
                }
                // Admission precedes every reservation/node wait. Closing and
                // sealing cannot mistake an in-flight Prepare for a drained phase.
                self.admit_placement(&e, bincode::serialize(&updates).map_err(err)?)
                    .map_err(|error| format!("shared parent phase admission: {error}"))?;
                match self.prepare_shared_parent(&e, updates) {
                    Ok(record) => {
                        self.shared_state.lock().records.insert(e.attempt, record);
                        Ok(SharedResponse::Ready)
                    }
                    Err(error) => {
                        self.shared_state
                            .lock()
                            .rejections
                            .insert(e.attempt, error.clone());
                        self.terminal_placement(&e)?;
                        Err(error)
                    }
                }
            }
            SharedRequest::Finish {
                envelope: e,
                mut updates,
            } => {
                updates.sort_unstable_by_key(|u| u.root.id);
                let (images, manager, state) = {
                    let s = self.shared_state.lock();
                    let r = s
                        .records
                        .get(&e.attempt)
                        .ok_or("unknown shared parent completion")?;
                    if r.envelope != e || r.updates != updates {
                        return Err("conflicting shared Finish duplicate".into());
                    }
                    (r.images.clone(), r.manager.clone(), r.state)
                };
                if state != SharedResponse::Ready {
                    return Ok(state);
                }
                let cluster = self.cluster()?;
                let publication = match cluster.placement_request(
                    cluster.coordinator,
                    PlacementRequest::PublicationStatus {
                        run: self.run,
                        attempt: e.attempt,
                    },
                )? {
                    PlacementResponse::Publication(outcome) => outcome,
                    _ => return Err("invalid shared publication status".into()),
                };
                let sequence = match publication {
                    PublicationStatus::Published { sequence } => Some(sequence),
                    PublicationStatus::Aborted => None,
                    PublicationStatus::NotPublished => {
                        return Err("shared parent publication unresolved".into())
                    }
                };
                if let Some(sequence) = sequence {
                    let view = cluster.current_view();
                    if view.sequence != sequence || view.attempt != Some(e.attempt) {
                        return Err("shared parent Finish lacks installed published view".into());
                    }
                    for update in &updates {
                        if let Some(parent) = update.new_parent {
                            let p = view
                                .value
                                .shared
                                .get(&parent.id)
                                .ok_or("published shared parent absent")?;
                            if p.target != parent
                                || !p.children.iter().any(|c| c.target == update.root)
                            {
                                return Err("published shared parent linkage mismatch".into());
                            }
                        } else if view.value.root.target != update.root {
                            return Err("unparented owned root is not published global root".into());
                        }
                    }
                }
                let handles = updates
                    .iter()
                    .map(|u| self.resident(u.root))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut guards = handles.iter().map(|h| h.write()).collect::<Vec<_>>();
                for (u, n) in updates.iter().zip(&guards) {
                    if n.reserved != Some(e.attempt)
                        || n.routing.revision != u.expected_revision
                        || n.routing.parent != u.expected_parent
                    {
                        return Err("shared parent protection changed before completion".into());
                    }
                }
                if sequence.is_some() {
                    for (n, image) in guards.iter_mut().zip(&images) {
                        n.routing = Arc::new(image.clone());
                    }
                    self.publish_images(images, &[])?;
                }
                for n in &mut guards {
                    n.reserved = None;
                }
                drop(guards);
                drop(handles);
                manager.release(e.attempt).map_err(err)?;
                let result = sequence
                    .map(|sequence| SharedResponse::Applied { sequence })
                    .unwrap_or(SharedResponse::Aborted);
                self.shared_state
                    .lock()
                    .records
                    .get_mut(&e.attempt)
                    .unwrap()
                    .state = result;
                self.terminal_placement(&e)?;
                Ok(result)
            }
            SharedRequest::Status { .. } => unreachable!(),
        }
    }
    fn prepare_shared_parent(
        &self,
        e: &Envelope,
        updates: Vec<ParentUpdate>,
    ) -> Result<SharedRecord, String> {
        let manager = self.regions.read().clone();
        let request = RegionRequest {
            roots: updates.iter().map(|u| u.root.id).collect(),
            placements: updates
                .iter()
                .map(|u| (u.root.id, u.root.placement))
                .collect(),
            topology_sequence: manager.sequence(),
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut status = manager
            .request(e.attempt, request)
            .map_err(|error| format!("shared parent region request: {error}"))?;
        while status == ReservationStatus::Queued {
            if Instant::now() >= deadline {
                match manager.cancel(e.attempt) {
                    Ok(_) => {}
                    Err(super::protocol::ProtocolError::Prerequisite) => {
                        manager.release(e.attempt).map_err(err)?
                    }
                    Err(error) => return Err(err(error)),
                }
                return Err("shared parent region acquisition deadline".into());
            }
            std::thread::sleep(Duration::from_millis(1));
            status = manager.status(e.attempt).map_err(err)?;
        }
        if status != ReservationStatus::Granted {
            return Err("shared parent dependencies changed; replan".into());
        }
        let prepared = (|| {
            let handles = updates
                .iter()
                .map(|u| {
                    self.resident(u.root)
                        .map_err(|error| format!("shared parent dependencies changed: {error}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut guards = handles.iter().map(|h| h.write()).collect::<Vec<_>>();
            let mut images = vec![];
            for (update, node) in updates.iter().zip(&guards) {
                if node.reserved.is_some()
                    || node.routing.revision != update.expected_revision
                    || node.routing.parent != update.expected_parent
                {
                    return Err("shared parent dependencies changed; replan".into());
                }
                if update
                    .new_parent
                    .is_some_and(|parent| self.nodes.contains_key(&parent.id))
                {
                    return Err("shared parent target must be outside the owned forest".into());
                }
                let mut image = (*node.routing).clone();
                image.parent = update.new_parent;
                revision(&mut image)?;
                images.push(image);
            }
            for node in &mut guards {
                node.reserved = Some(e.attempt);
            }
            Ok(SharedRecord {
                envelope: e.clone(),
                updates,
                images,
                manager: manager.clone(),
                state: SharedResponse::Ready,
            })
        })();
        if prepared.is_err() {
            manager.release(e.attempt).map_err(err)?;
        }
        prepared
    }
}

fn owned_parents(view: &BoundaryView) -> Result<BTreeMap<NodeId, Option<NodeRef>>, String> {
    let mut parents = BTreeMap::new();
    if view.units.contains_key(&view.root.target.id) {
        parents.insert(view.root.target.id, None);
    }
    for n in view.shared.values() {
        for c in &n.children {
            if view.units.contains_key(&c.target.id)
                && parents.insert(c.target.id, Some(n.target)).is_some()
            {
                return Err("ownership root has multiple shared parents".into());
            }
        }
    }
    if parents.len() != view.units.len() {
        return Err("shared view omits an ownership root".into());
    }
    Ok(parents)
}
fn validate_upper(view: &BoundaryView) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    let mut todo = vec![view.root.target];
    while let Some(reference) = todo.pop() {
        if !seen.insert(reference.id) {
            return Err("shared view contains a cycle or duplicate child".into());
        }
        if let Some(n) = view.shared.get(&reference.id) {
            if n.target != reference || n.leaf {
                return Err("invalid shared routing image".into());
            }
            todo.extend(n.children.iter().map(|c| c.target));
        } else if view.units.get(&reference.id) != Some(&reference) {
            return Err("shared view has a dangling ownership reference".into());
        }
    }
    if seen.len() != view.shared.len() + view.units.len() {
        return Err("shared view contains unreachable routing or ownership records".into());
    }
    Ok(())
}
impl Cluster {
    fn fresh_shared(
        &self,
        centroid: Vec<f32>,
        children: Vec<Child>,
        parent: Option<NodeRef>,
    ) -> Result<RoutingNode, String> {
        let id = self.local.attempt()?;
        Ok(RoutingNode {
            navigation: Default::default(),
            target: NodeRef {
                id: ((id.authority as u128) << 64) | id.sequence as u128,
                authority: self.coordinator,
                placement: 0,
            },
            parent,
            centroid_code: Code::<4>::quantize(&centroid, &vec![0.; self.local.dim])
                .as_ref()
                .to_vec(),
            centroid,
            children,
            leaf: false,
            revision: 0,
        })
    }
    fn shared_request(
        &self,
        owner: AuthorityId,
        request: SharedRequest,
    ) -> Result<SharedResponse, String> {
        let mut error = String::new();
        for _ in 0..3 {
            match self.request(owner, EngineRequest::Shared(request.clone())) {
                Ok(EngineResponse::Shared(reply)) => return Ok(reply),
                Ok(_) => return Err("invalid shared parent reply".into()),
                Err(e) => error = e,
            }
        }
        Err(error)
    }
    /// One deferred job edits one shared routing node. New overflows, empty
    /// ancestors, and a unary global root become separate immediately eligible jobs.
    pub(super) fn balance_shared_internal(&self, target: NodeRef) -> Result<(), String> {
        if self.local.owner != self.coordinator {
            return Err("shared balancing requires coordinator authority".into());
        }
        if self.local.config.branching_factor < 2 {
            return Err("shared branching factor must be at least two".into());
        }
        'replan: for _ in 0..32 {
            let before = self.publication.view();
            let Some(old) = before
                .value
                .shared
                .get(&target.id)
                .filter(|n| n.target == target)
                .cloned()
            else {
                return Ok(());
            };
            let overflow = old.children.len() > self.local.config.branching_factor;
            let collapse = old.parent.is_none() && old.children.len() == 1;
            let prune = old.parent.is_some() && old.children.is_empty();
            if !overflow && !collapse && !prune {
                return Ok(());
            }
            if old.children.is_empty() && old.parent.is_none() {
                return Err("cannot remove the sole empty shared global root".into());
            }
            let mut next = before.value.clone();
            let mut replacements = vec![];
            if overflow {
                if old.children.len() > u32::MAX as usize {
                    return Err("shared split exceeds clustering slot capacity".into());
                }
                let input: Vec<EmbeddingPoint> = old
                    .children
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (i as u32, 0, Arc::from(c.centroid.as_slice())))
                    .collect();
                let (a, left, b, right) = utils::split(input, &self.local.metric);
                for (center, group) in [(a, left), (b, right)] {
                    replacements.push(
                        self.fresh_shared(
                            center.to_vec(),
                            group
                                .into_iter()
                                .map(|(slot, _, _)| old.children[slot as usize].clone())
                                .collect(),
                            old.parent,
                        )?,
                    );
                }
            } else if collapse {
                let child = old.children[0].target;
                if let Some(image) = next.shared.get(&child.id) {
                    let mut image = image.clone();
                    image.parent = None;
                    revision(&mut image)?;
                    replacements.push(image);
                } else {
                    let mut image = match self.node(child) {
                        Ok(image) => image,
                        Err(error) if error.contains("stale") || error.contains("retired") => {
                            continue 'replan;
                        }
                        Err(error) => {
                            return Err(format!("shared root collapse child lookup: {error}"));
                        }
                    };
                    image.parent = None;
                    revision(&mut image)?;
                    replacements.push(image);
                }
            }
            next.shared.remove(&old.target.id);
            let mut changed_parent = None;
            if let Some(parent) = old.parent {
                let p = next
                    .shared
                    .get_mut(&parent.id)
                    .ok_or("shared node parent is missing")?;
                if !p.children.iter().any(|c| c.target == old.target) {
                    return Err("shared node no longer linked from parent".into());
                }
                p.children.retain(|c| c.target != old.target);
                p.children.extend(replacements.iter().map(child));
                revision(p)?;
                center(p, self.local.dim)?;
                changed_parent = Some(p.target);
                for n in &replacements {
                    next.shared.insert(n.target.id, n.clone());
                }
            } else if collapse {
                next.root = replacements[0].clone();
                if next.shared.contains_key(&next.root.target.id) {
                    next.shared.insert(next.root.target.id, next.root.clone());
                }
            } else {
                let mut root = self.fresh_shared(vec![0.; self.local.dim], vec![], None)?;
                for node in &mut replacements {
                    node.parent = Some(root.target);
                }
                root.children = replacements.iter().map(child).collect();
                center(&mut root, self.local.dim)?;
                for node in &replacements {
                    next.shared.insert(node.target.id, node.clone());
                }
                next.root = root.clone();
                next.shared.insert(root.target.id, root);
            }
            // Shared descendants change parent inside the same immutable view; owned
            // roots receive their matching edit through the owner protocol below.
            for n in &replacements {
                for c in &n.children {
                    if let Some(image) = next.shared.get_mut(&c.target.id) {
                        if image.parent != Some(n.target) {
                            image.parent = Some(n.target);
                            revision(image)?;
                        }
                    }
                }
            }
            while let Some(changed) = changed_parent {
                let image = next
                    .shared
                    .get(&changed.id)
                    .ok_or("changed shared ancestor vanished")?
                    .clone();
                if let Some(parent) = image.parent {
                    let p = next
                        .shared
                        .get_mut(&parent.id)
                        .ok_or("shared ancestor parent missing")?;
                    let row = p
                        .children
                        .iter_mut()
                        .find(|c| c.target == changed)
                        .ok_or("shared ancestor child missing")?;
                    row.centroid = image.centroid.clone();
                    revision(p)?;
                    center(p, self.local.dim)?;
                    changed_parent = Some(parent);
                } else {
                    next.root = image;
                    changed_parent = None;
                }
            }
            if let Some(root) = next.shared.get(&next.root.target.id) {
                next.root = root.clone();
            }
            validate_upper(&next)?;
            let old_parents = owned_parents(&before.value)?;
            let new_parents = owned_parents(&next)?;
            let mut updates: BTreeMap<AuthorityId, Vec<ParentUpdate>> = BTreeMap::new();
            for (id, parent) in new_parents {
                if old_parents.get(&id) != Some(&parent) {
                    let root = next.units[&id];
                    let image = match self.node(root) {
                        Ok(image) => image,
                        Err(error) if error.contains("stale") || error.contains("retired") => {
                            continue 'replan;
                        }
                        Err(error) => {
                            return Err(format!("shared parent update root lookup: {error}"));
                        }
                    };
                    // The row and the protected dependency come from one owner
                    // image. Copying the older shared row could publish stale
                    // scoring data even when the parent edit validates revision.
                    if let Some(parent) = parent {
                        let row = next
                            .shared
                            .get_mut(&parent.id)
                            .ok_or("new owned-root shared parent missing")?
                            .children
                            .iter_mut()
                            .find(|c| c.target == root)
                            .ok_or("new owned-root shared child missing")?;
                        *row = child(&image);
                        let mut changed = parent;
                        loop {
                            let node = next
                                .shared
                                .get_mut(&changed.id)
                                .ok_or("updated shared ancestor missing")?;
                            center(node, self.local.dim)?;
                            revision(node)?;
                            let current = node.clone();
                            if let Some(ancestor) = current.parent {
                                let row = next
                                    .shared
                                    .get_mut(&ancestor.id)
                                    .ok_or("updated shared ancestor parent missing")?
                                    .children
                                    .iter_mut()
                                    .find(|c| c.target == changed)
                                    .ok_or("updated shared ancestor child missing")?;
                                *row = child(&current);
                                changed = ancestor;
                            } else {
                                next.root = current;
                                break;
                            }
                        }
                    } else if next.root.target == root {
                        let mut current = image.clone();
                        current.parent = None;
                        revision(&mut current)?;
                        next.root = current;
                    }
                    updates
                        .entry(root.authority)
                        .or_default()
                        .push(ParentUpdate {
                            root,
                            expected_revision: image.revision,
                            expected_parent: image.parent,
                            new_parent: parent,
                        });
                }
            }
            validate_upper(&next)?;
            let e = self.local.envelope(
                "shared-internal-balance",
                updates
                    .values()
                    .flatten()
                    .map(|u| (u.root.id, u.root.placement))
                    .collect(),
            )?;
            match self.publication.prepare(
                e.attempt,
                PublicationProposal {
                    expected_sequence: before.sequence,
                    required_owners: updates.keys().copied().collect(),
                    value: next.prepared()?,
                },
            ) {
                Ok(_) => {}
                // No owner has been prepared and no publication turn is held.
                // Another completed view makes this proposal safe to rebuild.
                Err(ProtocolError::StalePlacement) => continue,
                Err(error) => return Err(err(error)),
            }
            let deadline = Instant::now() + Duration::from_secs(30);
            while !self.publication.acquire_turn(e.attempt).map_err(err)? {
                if Instant::now() >= deadline {
                    self.publication.abort(e.attempt).map_err(err)?;
                    return Err("shared balancing publication turn deadline".into());
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            if self.publication.view().sequence != before.sequence {
                self.publication.abort(e.attempt).map_err(err)?;
                self.publication.release_turn(e.attempt).map_err(err)?;
                continue;
            }
            let mut accepted = vec![];
            let mut changed = false;
            for (owner, body) in &updates {
                match self.shared_request(
                    *owner,
                    SharedRequest::Prepare {
                        envelope: e.clone(),
                        updates: body.clone(),
                    },
                ) {
                    Ok(SharedResponse::Ready) => {
                        accepted.push(*owner);
                        self.publication.ready(e.attempt, *owner).map_err(err)?;
                    }
                    Err(error) if error.contains("dependencies changed") => {
                        changed = true;
                        break;
                    }
                    Err(error) => {
                        return Err(format!("unresolved shared parent preparation: {error}"))
                    }
                    Ok(_) => return Err("unexpected shared parent preparation state".into()),
                }
            }
            if changed {
                self.publication.abort(e.attempt).map_err(err)?;
                for owner in accepted {
                    match self.shared_request(
                        owner,
                        SharedRequest::Finish {
                            envelope: e.clone(),
                            updates: updates[&owner].clone(),
                        },
                    )? {
                        SharedResponse::Aborted => {}
                        _ => return Err("shared parent abort was not acknowledged".into()),
                    }
                }
                self.publication.release_turn(e.attempt).map_err(err)?;
                continue;
            }
            let outcome = self.publication.publish(e.attempt).map_err(err)?;
            let sequence = match outcome {
                PublicationStatus::Published { sequence } => sequence,
                _ => return Err("shared balancing publication aborted".into()),
            };
            let published = self.publication.view();
            self.broadcast_view(&published)?;
            for (owner, body) in updates {
                match self.shared_request(
                    owner,
                    SharedRequest::Finish {
                        envelope: e.clone(),
                        updates: body,
                    },
                )? {
                    SharedResponse::Applied { sequence: observed } if observed == sequence => {}
                    _ => return Err("shared parent completion lacks matching publication".into()),
                }
            }
            self.publication.release_turn(e.attempt).map_err(err)?;
            for node in published.value.shared.values() {
                if node.children.len() > self.local.config.branching_factor
                    || node.children.is_empty()
                    || (node.parent.is_none() && node.children.len() == 1)
                {
                    self.request(
                        self.coordinator,
                        EngineRequest::Enqueue(BalanceJob {
                            envelope: self.local.envelope(
                                "balance-job",
                                vec![(node.target.id, node.target.placement)],
                            )?,
                            node: node.target,
                            depth: 0,
                            internal: true,
                        }),
                    )?;
                }
            }
            return Ok(());
        }
        Err("shared balancing exceeded its dependency replan budget".into())
    }
}

#[cfg(test)]
#[path = "shared_runtime_tests.rs"]
mod tests;

impl Cluster {
    /// Called after the owned mutation releases its region and node protection.
    /// The owner job does not complete until the shared scoring copy is refreshed.
    pub(super) fn refresh_boundary_centroid(&self, root: NodeRef) -> Result<(), String> {
        let envelope = self
            .local
            .envelope("refresh-boundary-centroid", vec![(root.id, root.placement)])?;
        match self.shared_request(
            self.coordinator,
            SharedRequest::RefreshCentroid { envelope, root },
        )? {
            SharedResponse::Refreshed { .. } => Ok(()),
            _ => Err("invalid centroid refresh completion".into()),
        }
    }
    fn refresh_boundary_centroid_inner(
        &self,
        root: NodeRef,
        envelope: &Envelope,
    ) -> Result<u64, String> {
        static SERIALIZE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        let serialize = *SERIALIZE.get_or_init(|| {
            std::env::var("SPANN_SERIALIZE_BOUNDARY_REFRESH").is_ok_and(|value| value == "1")
        });
        self.refresh_boundary_centroid_with_gate(root, envelope, serialize)
    }
    fn refresh_boundary_centroid_with_gate(
        &self,
        root: NodeRef,
        envelope: &Envelope,
        serialize: bool,
    ) -> Result<u64, String> {
        static PREPARE_UNDER_TURN: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        let early = *PREPARE_UNDER_TURN.get_or_init(|| {
            std::env::var("SPANN_REFRESH_PREPARE_UNDER_TURN").is_ok_and(|value| value == "1")
        });
        self.refresh_boundary_centroid_options(root, envelope, serialize, early)
    }
    fn refresh_boundary_centroid_options(
        &self,
        root: NodeRef,
        envelope: &Envelope,
        serialize: bool,
        early: bool,
    ) -> Result<u64, String> {
        self.refresh_boundary_centroid_until(
            root,
            envelope,
            serialize,
            early,
            Instant::now() + Duration::from_secs(30),
        )
    }
    fn refresh_boundary_centroid_until(
        &self,
        root: NodeRef,
        envelope: &Envelope,
        serialize: bool,
        early: bool,
        refresh_deadline: Instant,
    ) -> Result<u64, String> {
        let attempt = envelope.attempt;
        let enabled = detailed();
        let mut total = enabled.then(|| {
            self.local
                .observability
                .operation(Scope::BoundaryCentroidRefresh)
        });
        let mut diagnostic =
            enabled.then(|| RefreshDiagnosticGuard::new(self.local.owner, root, attempt));
        // Concurrent owner edits and shared publications can invalidate many
        // snapshots quickly. Bound elapsed waiting, rather than treating a
        // finite number of legitimate dependency changes as a failed mutation.
        // Owner edits release node and region protection before this call. Never
        // hold shared_state while waiting for the per-root gate. Node lookups,
        // parent installation and broadcasts do not acquire refresh gates, so
        // the publication turn cannot wait on a gate held by its own waiter.
        let gate = if serialize {
            Some(
                self.local
                    .shared_state
                    .lock()
                    .refresh_gate(root.id, 100_000)?,
            )
        } else {
            None
        };
        let gate_timing = (enabled && serialize).then(|| {
            self.local
                .observability
                .operation(Scope::BoundaryCentroidGateWait)
        });
        let gate_started = (enabled && serialize).then(Instant::now);
        let _gate_guard = match gate.as_ref() {
            Some(gate) => Some(
                gate.try_lock_until(refresh_deadline)
                    .ok_or("boundary refresh gate deadline")?,
            ),
            None => None,
        };
        drop(gate_timing);
        if let (Some(record), Some(start)) = (diagnostic.as_mut(), gate_started) {
            record.record.gate_wait_nanos = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        }
        if serialize {
            self.local.check(envelope)?;
        }
        // Every waiter takes a fresh snapshot and validates its own completion;
        // a predecessor's result cannot cover a subsequent owner revision.
        let mut retry = false;
        while Instant::now() < refresh_deadline {
            if let Some(work) = total.as_mut() {
                work.add(Counter::BoundaryCentroidAttempts, 1);
            }
            if let Some(record) = diagnostic.as_mut() {
                record.record.attempts += 1;
            }
            if retry {
                std::thread::sleep(Duration::from_millis(1));
            }
            retry = true;
            let e = self.local.envelope(
                "shared-centroid-publication",
                vec![(root.id, root.placement)],
            )?;
            let early_turn = if early {
                let started = Instant::now();
                let timing = enabled.then(|| {
                    self.local
                        .observability
                        .operation(Scope::BoundaryCentroidTurnWait)
                });
                while !self
                    .publication
                    .acquire_unprepared_turn(e.attempt)
                    .map_err(err)?
                {
                    if Instant::now() >= refresh_deadline {
                        return Err("shared centroid publication turn deadline".into());
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                drop(timing);
                if let Some(record) = diagnostic.as_mut() {
                    record.record.turn_wait_nanos +=
                        started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                }
                Some(UnpublishedRefreshTurn {
                    publication: &self.publication,
                    attempt: e.attempt,
                })
            } else {
                None
            };
            let mut held = (enabled && early).then(|| {
                self.local
                    .observability
                    .operation(Scope::BoundaryCentroidTurnHeld)
            });
            // Only the logical permission is held, never the publication mutex.
            // Owner Node requests do not wait on publication or refresh gates.
            let rebuild = enabled.then(|| {
                self.local
                    .observability
                    .operation(Scope::BoundaryCentroidRebuild)
            });
            let before = self.publication.view();
            if before.value.units.get(&root.id) != Some(&root) {
                return Ok(before.sequence);
            }
            let image = match self.node(root) {
                Ok(image) => image,
                Err(error) if error.contains("stale") || error.contains("retired") => continue,
                Err(error) => return Err(format!("shared centroid root lookup: {error}")),
            };
            // Publication installs shared links before owners finish changing
            // their parent fields. Resolve the parent from this pinned view;
            // then wait for the publication turn before declaring any no-op.
            let parent_image = image
                .parent
                .and_then(|p| before.value.shared.get(&p.id))
                .filter(|p| p.children.iter().any(|c| c.target == root))
                .or_else(|| {
                    before
                        .value
                        .shared
                        .values()
                        .find(|p| p.children.iter().any(|c| c.target == root))
                });
            if parent_image.is_none() && before.value.root.target != root {
                return Err("current owned unit has no published shared parent".into());
            }
            let parent = parent_image.map(|p| p.target);
            let row = parent_image.and_then(|p| p.children.iter().find(|c| c.target == root));
            let needs_update =
                row.is_some_and(|row| row.centroid != image.centroid || row.leaf != image.leaf);
            let mut next = before.value.clone();
            if needs_update {
                let parent = parent.expect("a changed row has a shared parent");
                let p = next.shared.get_mut(&parent.id).unwrap();
                let c = p.children.iter_mut().find(|c| c.target == root).unwrap();
                c.centroid = image.centroid.clone();
                c.leaf = image.leaf;
                revision(p)?;
                center(p, self.local.dim)?;
                let mut changed = parent;
                loop {
                    let child_image = next.shared[&changed.id].clone();
                    if let Some(ancestor) = child_image.parent {
                        let p = next
                            .shared
                            .get_mut(&ancestor.id)
                            .ok_or("centroid refresh shared ancestor missing")?;
                        let c = p
                            .children
                            .iter_mut()
                            .find(|c| c.target == changed)
                            .ok_or("centroid refresh ancestor linkage missing")?;
                        c.centroid = child_image.centroid.clone();
                        revision(p)?;
                        center(p, self.local.dim)?;
                        changed = ancestor;
                    } else {
                        next.root = child_image;
                        break;
                    }
                }
            }
            match self.publication.prepare(
                e.attempt,
                PublicationProposal {
                    expected_sequence: before.sequence,
                    required_owners: BTreeSet::new(),
                    value: next.prepared()?,
                },
            ) {
                Ok(_) => {}
                // No owner has been prepared. The early-turn guard releases
                // any unpublished permission before rebuilding this proposal.
                Err(ProtocolError::StalePlacement) => {
                    if let Some(work) = total.as_mut() {
                        work.add(Counter::BoundaryCentroidStalePrepareRetries, 1);
                    }
                    if let Some(record) = diagnostic.as_mut() {
                        record.record.stale_prepare_retries += 1;
                    }
                    continue;
                }
                Err(error) => return Err(err(error)),
            }
            if let Some(work) = total.as_mut() {
                work.add(Counter::BoundaryCentroidPrepared, 1);
            }
            drop(rebuild);
            let turn_started = (enabled && !early).then(Instant::now);
            let turn_wait = (enabled && !early).then(|| {
                self.local
                    .observability
                    .operation(Scope::BoundaryCentroidTurnWait)
            });
            let deadline = refresh_deadline;
            while !early && !self.publication.acquire_turn(e.attempt).map_err(err)? {
                if Instant::now() >= deadline {
                    self.publication.abort(e.attempt).map_err(err)?;
                    return Err("shared centroid publication turn deadline".into());
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            drop(turn_wait);
            if let (Some(record), Some(start)) = (diagnostic.as_mut(), turn_started) {
                record.record.turn_wait_nanos +=
                    start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            }
            if enabled && !early {
                held = Some(
                    self.local
                        .observability
                        .operation(Scope::BoundaryCentroidTurnHeld),
                );
            }
            let _held = held;
            let _early_turn = early_turn;
            let current = self.node(root);
            let unchanged = current.as_ref().is_ok_and(|n| {
                n.revision == image.revision
                    && n.parent == image.parent
                    && n.centroid == image.centroid
                    && n.leaf == image.leaf
            });
            if self.publication.view().sequence != before.sequence || !unchanged {
                if let Some(work) = total.as_mut() {
                    work.add(Counter::BoundaryCentroidChangedRetries, 1);
                }
                if let Some(record) = diagnostic.as_mut() {
                    record.record.changed_retries += 1;
                }
                self.publication.abort(e.attempt).map_err(err)?;
                self.publication.release_turn(e.attempt).map_err(err)?;
                continue;
            }
            if !needs_update {
                if let Some(work) = total.as_mut() {
                    work.add(Counter::BoundaryCentroidNoop, 1);
                }
                // Even a matching cached row must wait for earlier publishers.
                // Otherwise an older validated response can overwrite it after
                // this owner job incorrectly reports its refresh complete.
                self.publication.abort(e.attempt).map_err(err)?;
                self.publication.release_turn(e.attempt).map_err(err)?;
                return Ok(before.sequence);
            }
            let sequence = match self.publication.publish(e.attempt).map_err(err)? {
                PublicationStatus::Published { sequence } => sequence,
                _ => return Err("centroid publication was cancelled".into()),
            };
            let broadcast_started = enabled.then(Instant::now);
            let broadcast = enabled.then(|| {
                self.local
                    .observability
                    .operation(Scope::BoundaryCentroidBroadcast)
            });
            self.broadcast_view(&self.publication.view())?;
            drop(broadcast);
            if let (Some(record), Some(start)) = (diagnostic.as_mut(), broadcast_started) {
                record.record.broadcast_nanos +=
                    start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            }
            if let Some(work) = total.as_mut() {
                work.add(Counter::BoundaryCentroidPublished, 1);
            }
            if let Some(record) = diagnostic.as_mut() {
                record.record.published = true;
            }
            self.publication.release_turn(e.attempt).map_err(err)?;
            return Ok(sequence);
        }
        Err("centroid refresh exceeded its dependency replan deadline".into())
    }
}
