//! Whole-branch placement in a drained construction phase. The source sends
//! current payloads directly to the destination in bounded chunks. The
//! coordinator publishes routing and assignment together, after source fencing.
use super::construction::BoundaryView;
use super::engine::{Cluster, EngineRequest, EngineResponse, OwnerService, Resident, StoredNode};
use super::placement::{self, OwnerBudget, OwnershipUnit, PlacementPlan};
use super::protocol::envelope::Envelope;
use super::protocol::handoff::{
    Capacity, CapacityLedger, HandoffPayload, HandoffRecord, HandoffState,
};
use super::protocol::publication::{PublicationProposal, PublicationStatus, PublishedView};
use super::protocol::reservation::{RegionManager, RegionRequest, ReservationStatus};
use super::protocol::{AttemptId, AuthorityId, NodeId};
use super::routing::{NodeRef, RoutingNode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{atomic::Ordering, Arc};

#[path = "view_codec.rs"]
mod view_codec;

const CHUNK_BYTES: usize = 4 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PlacementRequest {
    Statistics {
        run: u128,
    },
    Configure {
        run: u128,
        vector_capacity: u64,
        byte_capacity: u64,
    },
    Count {
        root: NodeRef,
    },
    PublicationStatus {
        run: u128,
        attempt: AttemptId,
    },
    Export {
        envelope: Envelope,
        root: NodeRef,
        destination: AuthorityId,
        new_placement: u64,
    },
    Stage {
        envelope: Envelope,
        payload: HandoffPayload,
        chunks: usize,
    },
    Chunk {
        envelope: Envelope,
        payload: HandoffPayload,
        index: usize,
        nodes: Vec<StoredNode>,
    },
    Ready {
        envelope: Envelope,
        payload: HandoffPayload,
    },
    Fence {
        envelope: Envelope,
        payload: HandoffPayload,
    },
    Activate {
        envelope: Envelope,
        payload: HandoffPayload,
        sequence: u64,
    },
    Cleanup {
        envelope: Envelope,
        payload: HandoffPayload,
        sequence: u64,
    },
    Complete {
        envelope: Envelope,
        payload: HandoffPayload,
    },
    Abort {
        envelope: Envelope,
        payload: HandoffPayload,
        outcome: PublicationStatus,
    },
    Status {
        envelope: Envelope,
        payload: HandoffPayload,
    },
    InstallView {
        run: u128,
        #[serde(with = "view_codec")]
        view: PublishedView<BoundaryView>,
    },
    /// Refinement happens only after all construction workers drain.
    ProtectPromotion {
        envelope: Envelope,
        nodes: Vec<NodeId>,
    },
    Promote {
        envelope: Envelope,
        nodes: Vec<NodeId>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PlacementResponse {
    Statistics(PlacementStatistics),
    Ack,
    Count(u64),
    Publication(PublicationStatus),
    Exported(HandoffPayload),
    State(HandoffState),
}
/// Completed source transfers count each retained attempt once. Payload bytes
/// describe serialized branch records, excluding RPC envelopes and framing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlacementStatistics {
    pub completed_outgoing_attempts: u64,
    pub completed_outgoing_vectors: u64,
    pub completed_outgoing_payload_bytes: u64,
    pub assigned_capacity: Option<Capacity>,
    pub pending_capacity: Option<Capacity>,
}
struct Endpoint {
    record: HandoffRecord,
    source: bool,
    old_root: NodeRef,
    node_ids: Vec<NodeId>,
    incoming: BTreeMap<NodeId, StoredNode>,
    chunks: BTreeMap<usize, [u8; 32]>,
    chunk_count: usize,
    received_bytes: u64,
    region: Option<Arc<RegionManager>>,
}
#[derive(Default)]
pub(super) struct OwnerPlacementState {
    failed_exports: BTreeMap<AttemptId, String>,
    records: BTreeMap<AttemptId, Endpoint>,
    execution: BTreeMap<AttemptId, Arc<parking_lot::Mutex<()>>>,
    rejected: BTreeMap<AttemptId, (HandoffPayload, usize, String)>,
    promotions: BTreeMap<AttemptId, (Vec<NodeId>, Arc<RegionManager>)>,
    completed_promotions: BTreeMap<AttemptId, Vec<NodeId>>,
    capacity: Option<CapacityLedger>,
    view: Option<PublishedView<BoundaryView>>,
}
impl OwnerPlacementState {
    pub(super) fn ensure_save_quiescent(&self) -> Result<(), String> {
        if !self.promotions.is_empty()
            || self.records.values().any(|r| {
                !matches!(
                    r.record.state(),
                    HandoffState::Complete | HandoffState::Aborted
                )
            })
            || self
                .capacity
                .as_ref()
                .is_some_and(|c| c.pending().vectors != 0 || c.pending().bytes != 0)
        {
            return Err("unresolved placement prevents save".into());
        }
        Ok(())
    }
}
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn identity(root: NodeRef, destination: AuthorityId, new_placement: u64) -> Vec<u8> {
    bincode::serialize(&(root, destination, new_placement)).expect("fixed placement identity")
}
fn payload_identity(payload: &HandoffPayload) -> Vec<u8> {
    identity(
        NodeRef {
            id: payload.root,
            authority: payload.source,
            placement: payload.old_placement,
        },
        payload.destination,
        payload.new_placement,
    )
}
fn digest(nodes: &[StoredNode]) -> Result<([u8; 32], u64), String> {
    let bytes = bincode::serialize(nodes).map_err(err)?;
    Ok((Sha256::digest(&bytes).into(), bytes.len() as u64))
}
fn chunk_nodes(nodes: Vec<StoredNode>) -> Result<Vec<Vec<StoredNode>>, String> {
    let mut chunks = vec![];
    let mut current = vec![];
    let mut bytes = 8;
    for node in nodes {
        let size = bincode::serialized_size(&node).map_err(err)? as usize;
        if size > CHUNK_BYTES {
            return Err(
                "one posting record exceeds bounded handoff chunk; split before placement".into(),
            );
        }
        if bytes + size > CHUNK_BYTES && !current.is_empty() {
            chunks.push(std::mem::take(&mut current));
            bytes = 8;
        }
        bytes += size;
        current.push(node);
    }
    if !current.is_empty() {
        chunks.push(current)
    }
    Ok(chunks)
}
// Before an endpoint exists, a rejected export has made no ownership change.
// Release its unmutated region and retain the failed attempt for replay.
struct UncommittedExport<'a> {
    owner: &'a OwnerService,
    envelope: &'a Envelope,
    manager: Option<Arc<RegionManager>>,
    committed: bool,
}
impl Drop for UncommittedExport<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let cleanup = (|| -> Result<(), String> {
            if let Some(m) = &self.manager {
                match m.status(self.envelope.attempt) {
                    Ok(ReservationStatus::Granted) => {
                        m.release(self.envelope.attempt).map_err(err)?
                    }
                    Ok(ReservationStatus::Queued) => {
                        if m.cancel(self.envelope.attempt).is_err()
                            && m.status(self.envelope.attempt) == Ok(ReservationStatus::Granted)
                        {
                            m.release(self.envelope.attempt).map_err(err)?;
                        }
                    }
                    _ => {}
                }
            }
            self.owner.placement.lock().failed_exports.insert(
                self.envelope.attempt,
                "export preparation rejected before staging".into(),
            );
            self.owner.terminal_placement(self.envelope)
        })();
        if let Err(error) = cleanup {
            self.owner
                .fail_unresolved(format!("unresolved export preparation cleanup: {error}"));
        }
    }
}
impl OwnerService {
    pub fn placement_statistics(&self) -> PlacementStatistics {
        let state = self.placement.lock();
        let mut statistics = PlacementStatistics {
            completed_outgoing_attempts: 0,
            completed_outgoing_vectors: 0,
            completed_outgoing_payload_bytes: 0,
            assigned_capacity: state.capacity.as_ref().map(CapacityLedger::assigned),
            pending_capacity: state.capacity.as_ref().map(CapacityLedger::pending),
        };
        for endpoint in state.records.values() {
            if endpoint.source && endpoint.record.state() == HandoffState::Complete {
                statistics.completed_outgoing_attempts += 1;
                statistics.completed_outgoing_vectors += endpoint.record.payload.vectors;
                statistics.completed_outgoing_payload_bytes += endpoint.record.payload.bytes;
            }
        }
        statistics
    }
    fn placement_record(&self, e: &Envelope, p: &HandoffPayload) -> Result<(), String> {
        self.check(e)?;
        let s = self.placement.lock();
        let r = s.records.get(&e.attempt).ok_or("unknown handoff outcome")?;
        if r.record.payload != *p {
            return Err("conflicting handoff payload".into());
        }
        Ok(())
    }
    fn placement_state(&self, e: &Envelope, p: &HandoffPayload) -> Result<HandoffState, String> {
        self.placement_record(e, p)?;
        Ok(self.placement.lock().records[&e.attempt].record.state())
    }
    pub(super) fn handle_placement(
        &self,
        request: PlacementRequest,
    ) -> Result<PlacementResponse, String> {
        let envelope = match &request {
            PlacementRequest::Export { envelope, .. }
            | PlacementRequest::Stage { envelope, .. }
            | PlacementRequest::Chunk { envelope, .. }
            | PlacementRequest::Ready { envelope, .. }
            | PlacementRequest::Fence { envelope, .. }
            | PlacementRequest::Activate { envelope, .. }
            | PlacementRequest::Cleanup { envelope, .. }
            | PlacementRequest::Complete { envelope, .. }
            | PlacementRequest::Abort { envelope, .. }
            | PlacementRequest::ProtectPromotion { envelope, .. }
            | PlacementRequest::Promote { envelope, .. } => Some(envelope),
            _ => None,
        };
        if let Some(e) = envelope {
            // Fence before lookup, including the retained execution-control map.
            self.check(e)?;
            let execution = {
                let mut state = self.placement.lock();
                if !state.execution.contains_key(&e.attempt) && state.execution.len() >= 100_000 {
                    return Err("placement execution record bound reached".into());
                }
                state
                    .execution
                    .entry(e.attempt)
                    .or_insert_with(|| Arc::new(parking_lot::Mutex::new(())))
                    .clone()
            };
            let _execution = execution.lock();
            self.check(e)?;
            self.handle_placement_serial(request)
        } else {
            self.handle_placement_serial(request)
        }
    }
    fn handle_placement_serial(
        &self,
        request: PlacementRequest,
    ) -> Result<PlacementResponse, String> {
        match request {
            PlacementRequest::Statistics { run } => {
                if run != self.run {
                    return Err("wrong placement statistics writer run".into());
                }
                Ok(PlacementResponse::Statistics(self.placement_statistics()))
            }
            PlacementRequest::Configure {
                run,
                vector_capacity,
                byte_capacity,
            } => {
                if run != self.run {
                    return Err("wrong capacity writer run".into());
                }
                let mut vectors = 0u64;
                let mut bytes = 0u64;
                for entry in self.nodes.iter() {
                    let n = entry.read();
                    if n.reserved.is_some() {
                        return Err("capacity reset requires drained placement".into());
                    }
                    if let Some(p) = &n.postings {
                        vectors = vectors
                            .checked_add(p.ids.len() as u64)
                            .ok_or("capacity overflow")?;
                    }
                    bytes = bytes
                        .checked_add(
                            bincode::serialized_size(&StoredNode {
                                routing: (*n.routing).clone(),
                                postings: n.postings.clone(),
                            })
                            .map_err(err)?,
                        )
                        .ok_or("capacity overflow")?;
                }
                let mut s = self.placement.lock();
                s.ensure_save_quiescent()?;
                s.capacity = Some(
                    CapacityLedger::new(
                        Capacity {
                            vectors: vector_capacity.max(vectors),
                            bytes: byte_capacity,
                        },
                        Capacity { vectors, bytes },
                    )
                    .map_err(err)?,
                );
                Ok(PlacementResponse::Ack)
            }
            PlacementRequest::PublicationStatus { run, attempt } => {
                if run != self.run || self.owner != self.cluster()?.coordinator {
                    return Err("invalid publication status authority".into());
                }
                Ok(PlacementResponse::Publication(
                    self.cluster()?.publication.status(attempt).map_err(err)?,
                ))
            }
            PlacementRequest::Count { root } => {
                Ok(PlacementResponse::Count(self.subtree_count(root)?))
            }
            PlacementRequest::InstallView { run, view } => {
                if run != self.run {
                    return Err("stale installed writer run".into());
                }
                let mut s = self.placement.lock();
                if s.view
                    .as_ref()
                    .is_some_and(|old| old.sequence > view.sequence)
                {
                    return Err("stale installed routing view".into());
                }
                if let Some(old) = &s.view {
                    if old.sequence == view.sequence && old != &view {
                        return Err("conflicting installed routing view".into());
                    }
                }
                self.cluster()?.install_view(&view)?;
                s.view = Some(view);
                Ok(PlacementResponse::Ack)
            }
            PlacementRequest::Export {
                envelope: e,
                root,
                destination,
                new_placement,
            } => {
                self.check(&e)?;
                self.admit_placement(&e, identity(root, destination, new_placement))?;
                if let Some(error) = self.placement.lock().failed_exports.get(&e.attempt) {
                    return Err(error.clone());
                }
                if let Some(record) = self.placement.lock().records.get(&e.attempt) {
                    if !record.source
                        || record.old_root != root
                        || record.record.payload.destination != destination
                        || record.record.payload.new_placement != new_placement
                    {
                        return Err("conflicting export duplicate".into());
                    }
                    if matches!(
                        record.record.state(),
                        HandoffState::Reserved | HandoffState::Aborted
                    ) {
                        return Err(
                            "export has no completed destination staging acknowledgement".into(),
                        );
                    }
                    return Ok(PlacementResponse::Exported(record.record.payload.clone()));
                }
                let mut preparation = UncommittedExport {
                    owner: self,
                    envelope: &e,
                    manager: None,
                    committed: false,
                };
                if root.authority != self.owner
                    || destination == self.owner
                    || root.placement.checked_add(1) != Some(new_placement)
                {
                    return Err("invalid whole-subtree transfer".into());
                }
                if self.placement.lock().records.len() >= 100_000 {
                    return Err("retained placement record bound reached".into());
                }
                let manager = Arc::clone(&self.regions.read());
                preparation.manager = Some(manager.clone());
                let status = manager
                    .request(
                        e.attempt,
                        RegionRequest {
                            roots: vec![root.id],
                            topology_sequence: manager.sequence(),
                            placements: vec![(root.id, root.placement)],
                        },
                    )
                    .map_err(err)?;
                if status != ReservationStatus::Granted {
                    return Err("handoff requires a drained unconflicted source region".into());
                }
                let snapshot = self.subtree_snapshot(root)?;
                let mut ids = snapshot
                    .iter()
                    .map(|n| n.routing.target.id)
                    .collect::<Vec<_>>();
                ids.sort_unstable();
                let handles = ids
                    .iter()
                    .map(|id| {
                        self.nodes
                            .get(id)
                            .map(|r| Arc::clone(r.value()))
                            .ok_or("source subtree retired before protection")
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut guards = handles.iter().map(|h| h.write()).collect::<Vec<_>>();
                if guards.iter().any(|g| g.reserved.is_some()) {
                    return Err("source subtree has an undrained operation".into());
                }
                let mut current = guards
                    .iter()
                    .map(|g| StoredNode {
                        routing: (*g.routing).clone(),
                        postings: g.postings.clone(),
                    })
                    .collect::<Vec<_>>();
                let set = ids.iter().copied().collect::<BTreeSet<_>>();
                if current.iter().any(|n| {
                    n.routing
                        .children
                        .iter()
                        .any(|c| !set.contains(&c.target.id))
                }) {
                    return Err("export ownership unit contains a remote or missing child".into());
                }
                for node in &mut current {
                    node.routing.target.authority = destination;
                    node.routing.target.placement = new_placement;
                    if let Some(parent) = &mut node.routing.parent {
                        if set.contains(&parent.id) {
                            parent.authority = destination;
                            parent.placement = new_placement;
                        }
                    }
                    for child in &mut node.routing.children {
                        child.target.authority = destination;
                        child.target.placement = new_placement;
                    }
                }
                current.sort_unstable_by_key(|n| n.routing.target.id);
                let vectors = current
                    .iter()
                    .filter_map(|n| n.postings.as_ref())
                    .map(|p| p.ids.len() as u64)
                    .sum();
                let (hash, bytes) = digest(&current)?;
                let payload = HandoffPayload {
                    root: root.id,
                    source: self.owner,
                    destination,
                    old_placement: root.placement,
                    new_placement,
                    vectors,
                    bytes,
                    manifest_digest: hash,
                };
                let record = HandoffRecord::new(payload.clone()).map_err(err)?;
                let chunks = chunk_nodes(current)?;
                {
                    let mut s = self.placement.lock();
                    s.capacity
                        .as_mut()
                        .ok_or("source capacity is not configured")?
                        .reserve_outgoing(
                            e.attempt,
                            root.id,
                            Capacity {
                                vectors,
                                bytes: bytes.saturating_sub(8),
                            },
                        )
                        .map_err(err)?;
                    s.records.insert(
                        e.attempt,
                        Endpoint {
                            record,
                            source: true,
                            old_root: root,
                            node_ids: ids,
                            incoming: BTreeMap::new(),
                            chunks: BTreeMap::new(),
                            chunk_count: chunks.len(),
                            received_bytes: 8,
                            region: Some(manager),
                        },
                    );
                }
                for g in &mut guards {
                    g.reserved = Some(e.attempt);
                }
                preparation.committed = true;
                drop(guards);
                drop(handles);
                let cluster = self.cluster()?;
                cluster.placement_request(
                    destination,
                    PlacementRequest::Stage {
                        envelope: e.clone(),
                        payload: payload.clone(),
                        chunks: chunks.len(),
                    },
                )?;
                for (index, nodes) in chunks.into_iter().enumerate() {
                    cluster.placement_request(
                        destination,
                        PlacementRequest::Chunk {
                            envelope: e.clone(),
                            payload: payload.clone(),
                            index,
                            nodes,
                        },
                    )?;
                }
                cluster.placement_request(
                    destination,
                    PlacementRequest::Ready {
                        envelope: e.clone(),
                        payload: payload.clone(),
                    },
                )?;
                self.placement
                    .lock()
                    .records
                    .get_mut(&e.attempt)
                    .unwrap()
                    .record
                    .ready(hash, vectors, bytes)
                    .map_err(err)?;
                Ok(PlacementResponse::Exported(payload))
            }
            PlacementRequest::Stage {
                envelope: e,
                payload: p,
                chunks,
            } => {
                self.check(&e)?;
                if p.destination != self.owner || chunks == 0 {
                    return Err("invalid destination staging".into());
                }
                let record = HandoffRecord::new(p.clone()).map_err(err)?;
                self.admit_placement(&e, payload_identity(&p))?;
                let mut s = self.placement.lock();
                if let Some((old, count, error)) = s.rejected.get(&e.attempt) {
                    return if old == &p && *count == chunks {
                        Err(error.clone())
                    } else {
                        Err("conflicting rejected staging duplicate".into())
                    };
                }
                if let Some(old) = s.records.get(&e.attempt) {
                    if old.record.payload != p || old.chunk_count != chunks || old.source {
                        return Err("conflicting staging duplicate".into());
                    }
                    return Ok(PlacementResponse::State(old.record.state()));
                }
                let reservation = if s.records.len() + s.rejected.len() >= 100_000 {
                    Err("retained placement record bound reached".to_string())
                } else {
                    match s.capacity.as_mut() {
                        Some(capacity) => capacity
                            .reserve_incoming(
                                e.attempt,
                                p.root,
                                Capacity {
                                    vectors: p.vectors,
                                    bytes: p.bytes,
                                },
                            )
                            .map_err(err),
                        None => Err("destination capacity not configured".into()),
                    }
                };
                if let Err(error) = reservation {
                    if s.rejected.len() < 100_000 {
                        s.rejected.insert(e.attempt, (p, chunks, error.clone()));
                    }
                    drop(s);
                    self.terminal_placement(&e)?;
                    return Err(error);
                }
                let root = NodeRef {
                    id: p.root,
                    authority: p.source,
                    placement: p.old_placement,
                };
                s.records.insert(
                    e.attempt,
                    Endpoint {
                        record,
                        source: false,
                        old_root: root,
                        node_ids: vec![],
                        incoming: BTreeMap::new(),
                        chunks: BTreeMap::new(),
                        chunk_count: chunks,
                        received_bytes: 8,
                        region: None,
                    },
                );
                Ok(PlacementResponse::Ack)
            }
            PlacementRequest::Chunk {
                envelope: e,
                payload: p,
                index,
                nodes,
            } => {
                self.placement_record(&e, &p)?;
                let (bytes_digest, bytes) = digest(&nodes)?;
                if bytes as usize > CHUNK_BYTES + 8 {
                    return Err("handoff chunk exceeds bound".into());
                }
                let mut s = self.placement.lock();
                let r = s.records.get_mut(&e.attempt).unwrap();
                if r.source || index >= r.chunk_count {
                    return Err("invalid staging chunk index".into());
                }
                if let Some(old) = r.chunks.get(&index) {
                    return if old == &bytes_digest {
                        Ok(PlacementResponse::State(r.record.state()))
                    } else {
                        Err("conflicting duplicate staging chunk".into())
                    };
                }
                if r.record.state() != HandoffState::Reserved {
                    return Err("new chunk after staging closed".into());
                }
                for n in &nodes {
                    if n.routing.target.authority != self.owner
                        || n.routing.target.placement != p.new_placement
                        || r.incoming.contains_key(&n.routing.target.id)
                        || self.nodes.contains_key(&n.routing.target.id)
                    {
                        return Err("invalid staged node identity".into());
                    }
                }
                let accumulated = r
                    .received_bytes
                    .checked_add(bytes.saturating_sub(8))
                    .ok_or("staging byte count overflow")?;
                if accumulated > p.bytes {
                    return Err("staging exceeds reserved manifest bytes".into());
                }
                r.received_bytes = accumulated;
                for n in nodes {
                    r.incoming.insert(n.routing.target.id, n);
                }
                r.chunks.insert(index, bytes_digest);
                Ok(PlacementResponse::Ack)
            }
            PlacementRequest::Ready {
                envelope: e,
                payload: p,
            } => {
                self.placement_record(&e, &p)?;
                let mut s = self.placement.lock();
                let r = s.records.get_mut(&e.attempt).unwrap();
                if r.source {
                    return Err("wrong staging endpoint".into());
                }
                if r.record.state() != HandoffState::Reserved {
                    return Ok(PlacementResponse::State(r.record.state()));
                }
                if r.chunks.len() != r.chunk_count {
                    return Err("missing staging chunks".into());
                }
                let nodes = r.incoming.values().cloned().collect::<Vec<_>>();
                let (hash, bytes) = digest(&nodes)?;
                let vectors = nodes
                    .iter()
                    .filter_map(|n| n.postings.as_ref())
                    .map(|p| p.ids.len() as u64)
                    .sum();
                let ids = nodes
                    .iter()
                    .map(|n| n.routing.target.id)
                    .collect::<BTreeSet<_>>();
                if !ids.contains(&p.root)
                    || nodes.iter().any(|n| {
                        n.routing
                            .children
                            .iter()
                            .any(|c| !ids.contains(&c.target.id))
                    })
                {
                    return Err("incomplete staged subtree topology".into());
                }
                for n in &nodes {
                    match (&n.postings, n.routing.leaf) {
                        (Some(postings), true)
                            if postings.codes.len() == postings.ids.len() * self.code_size() => {}
                        (None, false) => {}
                        _ => return Err("invalid staged postings".into()),
                    }
                }
                r.record.ready(hash, vectors, bytes).map_err(err)?;
                r.node_ids = ids.into_iter().collect();
                Ok(PlacementResponse::State(r.record.state()))
            }
            PlacementRequest::Fence {
                envelope: e,
                payload: p,
            } => {
                self.placement_record(&e, &p)?;
                let ids = {
                    let s = self.placement.lock();
                    let r = &s.records[&e.attempt];
                    if !r.source {
                        return Err("fence requires source".into());
                    }
                    r.node_ids.clone()
                };
                for id in ids {
                    let h = self
                        .nodes
                        .get(&id)
                        .map(|n| Arc::clone(n.value()))
                        .ok_or("fenced source record missing")?;
                    let n = h.write();
                    if n.reserved != Some(e.attempt) {
                        return Err("source fence is not exclusive".into());
                    }
                }
                let mut s = self.placement.lock();
                s.records
                    .get_mut(&e.attempt)
                    .unwrap()
                    .record
                    .fenced(true)
                    .map_err(err)?;
                Ok(PlacementResponse::State(
                    s.records[&e.attempt].record.state(),
                ))
            }
            PlacementRequest::Activate {
                envelope: e,
                payload: p,
                sequence,
            } => {
                self.placement_record(&e, &p)?;
                let (nodes, next_record) = {
                    let s = self.placement.lock();
                    let r = &s.records[&e.attempt];
                    if r.source {
                        return Err("activation requires destination".into());
                    }
                    if matches!(
                        r.record.state(),
                        HandoffState::DestinationActive
                            | HandoffState::SourceCleaned
                            | HandoffState::Complete
                    ) {
                        return Ok(PlacementResponse::State(r.record.state()));
                    }
                    let view = s.view.as_ref().ok_or("destination lacks published view")?;
                    if view.sequence != sequence
                        || view.attempt != Some(e.attempt)
                        || view.value.units.get(&p.root)
                            != Some(&NodeRef {
                                id: p.root,
                                authority: self.owner,
                                placement: p.new_placement,
                            })
                    {
                        return Err("activation lacks published ownership".into());
                    }
                    let mut next_record = r.record.clone();
                    next_record.fenced(true).map_err(err)?;
                    next_record
                        .published(PublicationStatus::Published { sequence })
                        .map_err(err)?;
                    next_record
                        .active(self.owner, p.new_placement, sequence)
                        .map_err(err)?;
                    (
                        r.incoming.values().cloned().collect::<Vec<_>>(),
                        next_record,
                    )
                };
                if nodes
                    .iter()
                    .any(|n| self.nodes.contains_key(&n.routing.target.id))
                {
                    return Err("activation would overwrite current state".into());
                }
                let images = nodes.iter().map(|n| n.routing.clone()).collect();
                for n in nodes {
                    self.nodes.insert(
                        n.routing.target.id,
                        Arc::new(parking_lot::RwLock::new(Resident {
                            routing: Arc::new(n.routing),
                            postings: n.postings,
                            reserved: None,
                            preparing_split: false,
                            transfer_source: None,
                            claim_producer: None,
                        })),
                    );
                }
                self.publish_images(images, &[])?;
                self.rebuild_regions_drained()?;
                let mut s = self.placement.lock();
                let r = s.records.get_mut(&e.attempt).unwrap();
                r.record = next_record;
                r.incoming.clear();
                Ok(PlacementResponse::State(r.record.state()))
            }
            PlacementRequest::Cleanup {
                envelope: e,
                payload: p,
                sequence,
            } => {
                self.placement_record(&e, &p)?;
                let (ids, manager) = {
                    let s = self.placement.lock();
                    let r = &s.records[&e.attempt];
                    if !r.source {
                        return Err("cleanup requires source".into());
                    }
                    if r.record.state() == HandoffState::Complete {
                        return Ok(PlacementResponse::State(HandoffState::Complete));
                    }
                    (r.node_ids.clone(), r.region.clone())
                };
                {
                    let s = self.placement.lock();
                    let r = &s.records[&e.attempt];
                    if !matches!(
                        r.record.state(),
                        HandoffState::SourceFenced
                            | HandoffState::Published { .. }
                            | HandoffState::DestinationActive
                    ) {
                        return Err("cleanup before source fencing".into());
                    }
                    let view = s.view.as_ref().ok_or("source lacks published view")?;
                    if view.sequence != sequence
                        || view.attempt != Some(e.attempt)
                        || view.value.units.get(&p.root)
                            != Some(&NodeRef {
                                id: p.root,
                                authority: p.destination,
                                placement: p.new_placement,
                            })
                    {
                        return Err("cleanup lacks published ownership".into());
                    }
                }
                match self.cluster()?.placement_request(
                    p.destination,
                    PlacementRequest::Status {
                        envelope: e.clone(),
                        payload: p.clone(),
                    },
                )? {
                    PlacementResponse::State(HandoffState::DestinationActive)
                    | PlacementResponse::State(HandoffState::Complete) => {}
                    _ => return Err("cleanup before destination activation".into()),
                }
                // Destination Active acknowledgement precedes this request in the driver.
                self.publish_images(vec![], &ids)?;
                for id in &ids {
                    self.nodes.remove(id);
                }
                if let Some(manager) = manager {
                    manager.release(e.attempt).map_err(err)?;
                }
                self.rebuild_regions_drained()?;
                let mut s = self.placement.lock();
                s.capacity
                    .as_mut()
                    .unwrap()
                    .cleanup_outgoing(e.attempt)
                    .map_err(err)?;
                let r = s.records.get_mut(&e.attempt).unwrap();
                r.record
                    .published(PublicationStatus::Published { sequence })
                    .map_err(err)?;
                r.record
                    .active(p.destination, p.new_placement, sequence)
                    .map_err(err)?;
                r.record.cleaned(true).map_err(err)?;
                r.record.complete().map_err(err)?;
                drop(s);
                self.terminal_placement(&e)?;
                Ok(PlacementResponse::State(HandoffState::Complete))
            }
            PlacementRequest::Complete {
                envelope: e,
                payload: p,
            } => {
                self.placement_record(&e, &p)?;
                if self.placement_state(&e, &p)? != HandoffState::Complete {
                    match self.cluster()?.placement_request(
                        p.source,
                        PlacementRequest::Status {
                            envelope: e.clone(),
                            payload: p.clone(),
                        },
                    )? {
                        PlacementResponse::State(HandoffState::Complete) => {}
                        _ => return Err("destination completion precedes source cleanup".into()),
                    }
                }
                let mut s = self.placement.lock();
                let r = s.records.get_mut(&e.attempt).unwrap();
                if r.record.state() != HandoffState::Complete {
                    if r.source {
                        return Err("source cleanup not acknowledged".into());
                    }
                    r.record.cleaned(true).map_err(err)?;
                    r.record.complete().map_err(err)?;
                    s.capacity
                        .as_mut()
                        .unwrap()
                        .activate_incoming(e.attempt)
                        .map_err(err)?;
                }
                drop(s);
                self.terminal_placement(&e)?;
                Ok(PlacementResponse::State(HandoffState::Complete))
            }
            PlacementRequest::Abort {
                envelope: e,
                payload: p,
                outcome,
            } => {
                self.placement_record(&e, &p)?;
                let cluster = self.cluster()?;
                match cluster.placement_request(
                    cluster.coordinator,
                    PlacementRequest::PublicationStatus {
                        run: self.run,
                        attempt: e.attempt,
                    },
                )? {
                    PlacementResponse::Publication(actual)
                        if actual == outcome && actual == PublicationStatus::Aborted => {}
                    _ => return Err("abort lacks coordinator cancellation proof".into()),
                }
                let (source, ids, manager) = {
                    let mut s = self.placement.lock();
                    let r = s.records.get_mut(&e.attempt).unwrap();
                    r.record.abort(outcome).map_err(err)?;
                    r.incoming.clear();
                    (r.source, r.node_ids.clone(), r.region.clone())
                };
                if source {
                    for id in ids {
                        if let Some(n) = self.nodes.get(&id) {
                            let mut n = n.write();
                            if n.reserved == Some(e.attempt) {
                                n.reserved = None
                            }
                        }
                    }
                    if let Some(m) = manager {
                        m.release(e.attempt).map_err(err)?;
                    }
                    self.placement
                        .lock()
                        .capacity
                        .as_mut()
                        .unwrap()
                        .abort_outgoing(e.attempt)
                        .map_err(err)?;
                } else {
                    self.placement
                        .lock()
                        .capacity
                        .as_mut()
                        .unwrap()
                        .abort_incoming(e.attempt)
                        .map_err(err)?;
                }
                self.terminal_placement(&e)?;
                Ok(PlacementResponse::State(HandoffState::Aborted))
            }
            PlacementRequest::Status {
                envelope: e,
                payload: p,
            } => Ok(PlacementResponse::State(self.placement_state(&e, &p)?)),
            PlacementRequest::ProtectPromotion { envelope: e, nodes } => {
                self.check(&e)?;
                self.admit_placement(&e, bincode::serialize(&nodes).map_err(err)?)?;
                if let Some(old) = self.placement.lock().completed_promotions.get(&e.attempt) {
                    return if old == &nodes {
                        Ok(PlacementResponse::Ack)
                    } else {
                        Err("conflicting completed promotion".into())
                    };
                }
                if let Some((old, _)) = self.placement.lock().promotions.get(&e.attempt) {
                    return if old == &nodes {
                        Ok(PlacementResponse::Ack)
                    } else {
                        Err("conflicting promotion duplicate".into())
                    };
                }
                let mut placements = vec![];
                for id in &nodes {
                    let h = self
                        .nodes
                        .get(id)
                        .map(|n| Arc::clone(n.value()))
                        .ok_or("missing promotion node")?;
                    let n = h.read();
                    if n.postings.is_some() || n.reserved.is_some() {
                        return Err("invalid or busy shared promotion source".into());
                    }
                    placements.push((*id, n.routing.target.placement));
                }
                let manager = Arc::clone(&self.regions.read());
                let status = manager
                    .request(
                        e.attempt,
                        RegionRequest {
                            roots: nodes.clone(),
                            topology_sequence: manager.sequence(),
                            placements,
                        },
                    )
                    .map_err(err)?;
                if status != ReservationStatus::Granted {
                    return Err("promotion requires drained regions".into());
                }
                for id in &nodes {
                    let h = self.nodes.get(id).unwrap();
                    h.write().reserved = Some(e.attempt);
                }
                self.placement
                    .lock()
                    .promotions
                    .insert(e.attempt, (nodes, manager));
                Ok(PlacementResponse::Ack)
            }
            PlacementRequest::Promote { envelope: e, nodes } => {
                self.check(&e)?;
                self.admit_placement(&e, bincode::serialize(&nodes).map_err(err)?)?;
                {
                    let s = self.placement.lock();
                    if let Some(old) = s.completed_promotions.get(&e.attempt) {
                        return if old == &nodes {
                            Ok(PlacementResponse::Ack)
                        } else {
                            Err("conflicting completed promotion".into())
                        };
                    }
                    let (protected, _) = s
                        .promotions
                        .get(&e.attempt)
                        .ok_or("promotion was not protected")?;
                    if protected != &nodes {
                        return Err("conflicting protected promotion".into());
                    }
                    let view = s
                        .view
                        .as_ref()
                        .ok_or("promotion lacks installed shared view")?;
                    if view.attempt != Some(e.attempt)
                        || nodes.iter().any(|id| !view.value.shared.contains_key(id))
                    {
                        return Err("promotion requires published shared records".into());
                    }
                }
                let set = nodes.iter().copied().collect::<BTreeSet<_>>();
                for id in &nodes {
                    if let Some(n) = self.nodes.get(id) {
                        if n.read().postings.is_some() {
                            return Err("a posting leaf cannot become a shared routing node".into());
                        }
                    }
                }
                self.publish_images(vec![], &nodes)?;
                for id in &nodes {
                    self.nodes.remove(id);
                }
                let mut images = vec![];
                for entry in self.nodes.iter() {
                    let mut n = entry.write();
                    if n.routing.parent.is_some_and(|p| set.contains(&p.id)) {
                        let r = Arc::make_mut(&mut n.routing);
                        r.parent.as_mut().unwrap().authority = self.cluster()?.coordinator;
                        r.revision = r.revision.checked_add(1).ok_or("revision exhausted")?;
                        images.push((*n.routing).clone());
                    }
                }
                self.publish_images(images, &[])?;
                if let Some((_, manager)) = self.placement.lock().promotions.remove(&e.attempt) {
                    manager.release(e.attempt).map_err(err)?;
                }
                self.rebuild_regions_drained()?;
                self.placement
                    .lock()
                    .completed_promotions
                    .insert(e.attempt, nodes);
                self.terminal_placement(&e)?;
                Ok(PlacementResponse::Ack)
            }
        }
    }
}

impl Cluster {
    pub(super) fn placement_request(
        &self,
        owner: AuthorityId,
        request: PlacementRequest,
    ) -> Result<PlacementResponse, String> {
        // Same serialized command and attempt on retry. A failed bounded retry leaves
        // records unresolved; the caller must stop rather than reopen admission.
        let mut error = String::new();
        for _ in 0..3 {
            match self.request(owner, EngineRequest::Placement(request.clone())) {
                Ok(EngineResponse::Placement(response)) => return Ok(response),
                Ok(_) => return Err("wrong placement reply".into()),
                Err(e) => error = e,
            }
        }
        Err(error)
    }
    pub(super) fn broadcast_view(&self, view: &PublishedView<BoundaryView>) -> Result<(), String> {
        let mut owners = self.peers.keys().copied().collect::<BTreeSet<_>>();
        owners.insert(self.local.owner);
        for owner in owners {
            self.placement_request(
                owner,
                PlacementRequest::InstallView {
                    run: self.local.run,
                    view: view.clone(),
                },
            )?;
        }
        Ok(())
    }
    pub fn handoff(
        &self,
        envelope: Envelope,
        root: NodeRef,
        destination: AuthorityId,
        new_placement: u64,
    ) -> Result<(), String> {
        if self.local.owner != self.coordinator {
            self.request(
                self.coordinator,
                EngineRequest::Handoff {
                    envelope,
                    root,
                    destination,
                    new_placement,
                },
            )?;
            return Ok(());
        }
        let before = self.publication.view();
        if before.value.units.get(&root.id) != Some(&root) {
            return Err("handoff source is not a current ownership unit".into());
        }
        let mut next = before.value.clone();
        let new_root = NodeRef {
            id: root.id,
            authority: destination,
            placement: new_placement,
        };
        next.units.insert(root.id, new_root);
        for node in next.shared.values_mut() {
            for child in &mut node.children {
                if child.target == root {
                    child.target = new_root;
                }
            }
        }
        if next.root.target == root {
            // A whole-tree move preserves the current owned root image, whose
            // children may differ from the last shared publication snapshot.
            next.root = self.node(root)?;
            next.root.target = new_root;
            for child in &mut next.root.children {
                child.target.authority = destination;
                child.target.placement = new_placement;
            }
        }
        if let Some(shared_root) = next.shared.get(&next.root.target.id) {
            next.root = shared_root.clone();
        }
        self.publication
            .prepare(
                envelope.attempt,
                PublicationProposal {
                    expected_sequence: before.sequence,
                    required_owners: [root.authority, destination].into_iter().collect(),
                    value: next.prepared()?,
                },
            )
            .map_err(err)?;
        if !self
            .publication
            .acquire_turn(envelope.attempt)
            .map_err(err)?
        {
            return Err("placement must run after shared publications drain".into());
        }
        let payload = match self.placement_request(
            root.authority,
            PlacementRequest::Export {
                envelope: envelope.clone(),
                root,
                destination,
                new_placement,
            },
        )? {
            PlacementResponse::Exported(p) => p,
            _ => return Err("source did not export a handoff manifest".into()),
        };
        let mut record = HandoffRecord::new(payload.clone()).map_err(err)?;
        record
            .ready(payload.manifest_digest, payload.vectors, payload.bytes)
            .map_err(err)?;
        match self.placement_request(
            root.authority,
            PlacementRequest::Fence {
                envelope: envelope.clone(),
                payload: payload.clone(),
            },
        )? {
            PlacementResponse::State(HandoffState::SourceFenced) => {}
            _ => return Err("source fencing acknowledgement missing".into()),
        }
        record.fenced(true).map_err(err)?;
        self.publication
            .ready(envelope.attempt, root.authority)
            .map_err(err)?;
        self.publication
            .ready(envelope.attempt, destination)
            .map_err(err)?;
        let outcome = self.publication.publish(envelope.attempt).map_err(err)?;
        record.published(outcome).map_err(err)?;
        let sequence = match outcome {
            PublicationStatus::Published { sequence } => sequence,
            _ => return Err("handoff publication was cancelled".into()),
        };
        let published = self.publication.view();
        self.broadcast_view(&published)?;
        match self.placement_request(
            destination,
            PlacementRequest::Activate {
                envelope: envelope.clone(),
                payload: payload.clone(),
                sequence,
            },
        )? {
            PlacementResponse::State(HandoffState::DestinationActive)
            | PlacementResponse::State(HandoffState::Complete) => {}
            _ => return Err("destination activation acknowledgement missing".into()),
        }
        record
            .active(destination, new_placement, sequence)
            .map_err(err)?;
        match self.placement_request(
            root.authority,
            PlacementRequest::Cleanup {
                envelope: envelope.clone(),
                payload: payload.clone(),
                sequence,
            },
        )? {
            PlacementResponse::State(HandoffState::Complete) => {}
            _ => return Err("source cleanup acknowledgement missing".into()),
        }
        record.cleaned(true).map_err(err)?;
        self.placement_request(
            destination,
            PlacementRequest::Complete {
                envelope: envelope.clone(),
                payload,
            },
        )?;
        record.complete().map_err(err)?;
        self.publication
            .release_turn(envelope.attempt)
            .map_err(err)?;
        Ok(())
    }
    /// Rebalance current whole branches using existing child boundaries. This is
    /// called between construction phases, after all add/balance workers drain.
    pub fn redistribute(
        &self,
        capacities: BTreeMap<AuthorityId, u64>,
    ) -> Result<PlacementPlan, String> {
        if self.local.owner != self.coordinator {
            return Err("only the coordinator plans drained placement".into());
        }
        if capacities.is_empty() || capacities.values().any(|c| *c == 0) {
            return Err("placement requires positive capacities".into());
        }
        let view = self.publication.view();
        let mut units = vec![];
        for root in view.value.units.values() {
            let vectors = match self
                .placement_request(root.authority, PlacementRequest::Count { root: *root })?
            {
                PlacementResponse::Count(n) => n,
                _ => return Err("invalid branch count".into()),
            };
            units.push(OwnershipUnit {
                root: *root,
                vectors,
            });
        }
        let budgets = capacities
            .iter()
            .map(|(owner, capacity)| OwnerBudget {
                owner: *owner,
                vector_capacity: *capacity,
                next_interval_reserve: 0,
            })
            .collect::<Vec<_>>();
        let mut locations = units
            .iter()
            .map(|u| (u.root.id, u.root))
            .collect::<BTreeMap<_, _>>();
        let plan = placement::plan(units, &budgets, |id| {
            let root = *locations.get(&id).ok_or("missing refinement owner")?;
            match self.request(root.authority, EngineRequest::Summary { root })? {
                EngineResponse::Summary(children) => {
                    for c in &children {
                        locations.insert(c.root.id, c.root);
                    }
                    Ok(children)
                }
                _ => Err("invalid refinement summary".into()),
            }
        })?;
        // The byte cap reserves full-precision construction headroom in addition to
        // posting codes and routing metadata. It is separate from the placement
        // policy's vector trigger; the source may already exceed that trigger.
        for (owner, capacity) in &capacities {
            let byte_capacity = capacity
                .checked_mul(self.local.dim as u64 * 4 + self.local.code_size() as u64 + 4096)
                .ok_or("capacity byte bound exhausted")?;
            self.placement_request(
                *owner,
                PlacementRequest::Configure {
                    run: self.local.run,
                    vector_capacity: *capacity,
                    byte_capacity,
                },
            )?;
        }
        if !plan.promoted_shared.is_empty() {
            let promoted = plan
                .promoted_shared
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            let mut next = view.value.clone();
            let mut by_owner: BTreeMap<AuthorityId, Vec<NodeId>> = BTreeMap::new();
            for id in &plan.promoted_shared {
                let old = *locations.get(id).ok_or("missing promotion authority")?;
                let mut image = self.node(old)?;
                if image.leaf {
                    return Err("placement cannot promote a leaf".into());
                }
                by_owner.entry(old.authority).or_default().push(*id);
                image.target.authority = self.coordinator;
                next.shared.insert(*id, image);
                next.units.remove(id);
            }
            // plan.units includes final destinations; publish only original owners here.
            for unit in &plan.units {
                let original = *locations.get(&unit.root.id).unwrap_or(&unit.root);
                next.units.insert(original.id, original);
            }
            for image in next.shared.values_mut() {
                if let Some(parent) = &mut image.parent {
                    if promoted.contains(&parent.id) {
                        parent.authority = self.coordinator;
                    }
                }
                for child in &mut image.children {
                    if promoted.contains(&child.target.id) {
                        child.target.authority = self.coordinator;
                    }
                }
            }
            if let Some(root) = next.shared.get(&next.root.target.id) {
                next.root = root.clone();
            }
            let envelope = self.local.envelope("placement-refinement", vec![])?;
            let required = by_owner.keys().copied().collect();
            self.publication
                .prepare(
                    envelope.attempt,
                    PublicationProposal {
                        expected_sequence: view.sequence,
                        required_owners: required,
                        value: next.prepared()?,
                    },
                )
                .map_err(err)?;
            if !self
                .publication
                .acquire_turn(envelope.attempt)
                .map_err(err)?
            {
                return Err("refinement requires drained shared publication".into());
            }
            for (owner, nodes) in &by_owner {
                self.placement_request(
                    *owner,
                    PlacementRequest::ProtectPromotion {
                        envelope: envelope.clone(),
                        nodes: nodes.clone(),
                    },
                )?;
                self.publication
                    .ready(envelope.attempt, *owner)
                    .map_err(err)?;
            }
            self.publication.publish(envelope.attempt).map_err(err)?;
            self.broadcast_view(&self.publication.view())?;
            for (owner, nodes) in by_owner {
                self.placement_request(
                    owner,
                    PlacementRequest::Promote {
                        envelope: envelope.clone(),
                        nodes,
                    },
                )?;
            }
            self.publication
                .release_turn(envelope.attempt)
                .map_err(err)?;
        }
        for movement in &plan.moves {
            let e = self.local.envelope(
                "whole-subtree-handoff",
                vec![(movement.root.id, movement.root.placement)],
            )?;
            self.handoff(
                e,
                movement.root,
                movement.destination,
                movement.new_placement,
            )?;
        }
        Ok(plan)
    }
}

#[cfg(test)]
#[path = "placement_runtime_tests.rs"]
mod tests;
