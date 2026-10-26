//! One source and one peer owner share the existing two-role commit barriers.
//! Multiple target leaves form one destination role; routing remains readable.
use super::engine::{
    centroid_digest, sha256_bytes, validate_transfer_ack, Cluster, EngineRequest, EngineResponse,
    OwnerService, PostingList, Resident,
};
use super::observability::{Counter, Scope};
use super::protocol::envelope::Envelope;
use super::protocol::reservation::{RegionManager, RegionRequest, ReservationStatus};
use super::protocol::transfer::{
    DriverDecision, EndpointTransitions, Role, TransferDriver, TransferProof, TransferState,
    TransferStep,
};
use super::protocol::{AttemptId, AuthorityId, NodeId, ProtocolError};
use super::routing::NodeRef;
use chroma_index::quantization::Code;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{atomic::Ordering, Arc};
use std::time::{Duration, Instant};
// These are payload limits, not a cap on routing destinations. Remaining groups
// continue in deterministic, fully completed chunks.
const MAX_TARGETS: usize = 128;
const MAX_IDS: usize = 32768;
const MAX_CODES: usize = 512 * 1024;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Destination {
    pub target: NodeRef,
    pub revision: u64,
    pub centroid: [u8; 32],
    pub ids: Vec<u64>,
    pub codes: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchPayload {
    pub source: NodeRef,
    pub source_revision: u64,
    pub source_centroid: [u8; 32],
    pub destination_owner: AuthorityId,
    pub destinations: Vec<Destination>,
    pub producer: Option<AttemptId>,
    pub excluded: Option<NodeId>,
    pub digest: [u8; 32],
}
impl BatchPayload {
    fn content_digest(&self) -> Result<[u8; 32], String> {
        bincode::serialize(&(
            self.source,
            self.source_revision,
            self.source_centroid,
            self.destination_owner,
            &self.destinations,
            self.producer,
            self.excluded,
        ))
        .map(|b| sha256_bytes(&b))
        .map_err(|e| e.to_string())
    }
    fn validate(&self, code_size: usize) -> Result<(), String> {
        if self.source.authority == self.destination_owner
            || self.destinations.is_empty()
            || self.destinations.len() > MAX_TARGETS
        {
            return Err("invalid owner transfer authorities or target bound".into());
        }
        let mut unique = BTreeSet::new();
        let mut bytes = 0usize;
        let mut previous = None;
        for d in &self.destinations {
            if d.target.authority != self.destination_owner
                || d.target.id == self.source.id
                || previous.is_some_and(|p: NodeRef| p.id >= d.target.id)
                || d.ids.is_empty()
            {
                return Err("invalid owner transfer target order".into());
            }
            previous = Some(d.target);
            if d.codes.len()
                != d.ids
                    .len()
                    .checked_mul(code_size)
                    .ok_or("batch code overflow")?
            {
                return Err("invalid owner transfer code payload".into());
            }
            bytes = bytes
                .checked_add(d.codes.len())
                .ok_or("batch code overflow")?;
            for id in &d.ids {
                if !unique.insert(*id) {
                    return Err("duplicate owner transfer vector ID".into());
                }
            }
        }
        if unique.len() > MAX_IDS || bytes > MAX_CODES || self.content_digest()? != self.digest {
            return Err("invalid owner transfer digest or payload bound".into());
        }
        Ok(())
    }
    fn targets(&self, role: Role) -> Vec<NodeRef> {
        if role == Role::Source {
            vec![self.source]
        } else {
            self.destinations.iter().map(|d| d.target).collect()
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BatchRequest {
    Prepare {
        envelope: Envelope,
        payload: BatchPayload,
        role: Role,
    },
    Step {
        envelope: Envelope,
        role: Role,
        step: TransferStep,
        proof: TransferProof,
    },
    Status {
        envelope: Envelope,
        role: Role,
    },
}
pub(super) struct BatchEndpoint {
    envelope: Envelope,
    payload: BatchPayload,
    role: Role,
    prepared: bool,
    failure: Option<String>,
    transitions: EndpointTransitions,
    results: BTreeMap<TransferStep, (TransferProof, Result<TransferState, String>)>,
    manager: Option<Arc<RegionManager>>,
    handles: Vec<(NodeRef, Arc<RwLock<Resident>>)>,
    source_replacement: Option<PostingList>,
}
struct Unresolved<'a> {
    owner: &'a OwnerService,
    resolved: bool,
}
impl Drop for Unresolved<'_> {
    fn drop(&mut self) {
        if !self.resolved {
            self.owner
                .fail_unresolved("owner transfer exited with an unresolved mutation".into());
        }
    }
}
impl OwnerService {
    pub(super) fn ensure_batches_quiescent(&self) -> Result<(), String> {
        let entries = self
            .batch_transfers
            .lock()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries {
            let e = entry.lock();
            if e.failure.is_none()
                && (!e.prepared
                    || !matches!(
                        e.transitions.state(),
                        TransferState::Released | TransferState::Aborted
                    ))
            {
                return Err("owner transfer is not drained".into());
            }
        }
        Ok(())
    }
    pub(super) fn handle_owner_transfer(
        &self,
        request: BatchRequest,
    ) -> Result<TransferState, String> {
        match request {
            BatchRequest::Prepare {
                envelope,
                payload,
                role,
            } => self.prepare_owner_transfer(envelope, payload, role),
            BatchRequest::Status { envelope, role } => {
                self.check(&envelope)?;
                let entry = self
                    .batch_transfers
                    .lock()
                    .get(&envelope.attempt)
                    .cloned()
                    .ok_or("unknown owner transfer")?;
                let e = entry.lock();
                self.check(&envelope)?;
                if e.envelope != envelope || e.role != role {
                    return Err("conflicting owner transfer status".into());
                }
                if let Some(error) = &e.failure {
                    return Err(error.clone());
                }
                Ok(e.transitions.state())
            }
            BatchRequest::Step {
                envelope,
                role,
                step,
                proof,
            } => self.step_owner_transfer(envelope, role, step, proof),
        }
    }
    fn prepare_owner_transfer(
        &self,
        envelope: Envelope,
        payload: BatchPayload,
        role: Role,
    ) -> Result<TransferState, String> {
        let mut observed = self.observability.operation(Scope::TransferPrepare);
        self.check(&envelope)?;
        payload.validate(self.code_size())?;
        if envelope.kind != "owner-transfer-v1"
            || (if role == Role::Source {
                payload.source.authority
            } else {
                payload.destination_owner
            }) != self.owner
        {
            return Err("owner transfer endpoint mismatch".into());
        }
        self.admit_placement(
            &envelope,
            bincode::serialize(&("owner-transfer-v1", role, &payload))
                .map_err(|e| e.to_string())?,
        )?;
        let mut inserted = false;
        let record = {
            let mut all = self.batch_transfers.lock();
            if all.len() >= 1_000_000 && !all.contains_key(&envelope.attempt) {
                self.fail_unresolved(
                    "owner transfer record capacity exceeded after admission".into(),
                );
                return Err("owner transfer capacity exceeded".into());
            }
            all.entry(envelope.attempt)
                .or_insert_with(|| {
                    inserted = true;
                    Arc::new(Mutex::new(BatchEndpoint {
                        envelope: envelope.clone(),
                        payload: payload.clone(),
                        role,
                        prepared: false,
                        failure: None,
                        transitions: EndpointTransitions::new(role),
                        results: BTreeMap::new(),
                        manager: None,
                        handles: vec![],
                        source_replacement: None,
                    }))
                })
                .clone()
        };
        if inserted {
            observed.add(
                Counter::TransferPayloadIdBytesCloned,
                (payload
                    .destinations
                    .iter()
                    .map(|d| d.ids.len())
                    .sum::<usize>()
                    * 8) as u64,
            );
            observed.add(
                Counter::TransferPayloadCodeBytesCloned,
                payload
                    .destinations
                    .iter()
                    .map(|d| d.codes.len() as u64)
                    .sum(),
            );
        }
        let mut e = record.lock();
        self.check(&envelope)?;
        if e.envelope != envelope || e.payload != payload || e.role != role {
            return Err("conflicting owner transfer preparation".into());
        }
        if let Some(error) = &e.failure {
            return Err(error.clone());
        }
        if e.prepared {
            return Ok(TransferState::Prepared);
        }
        let result = (|| -> Result<(), String> {
            let manager = self.regions.read().clone();
            let targets = payload.targets(role);
            let placements = targets.iter().map(|n| (n.id, n.placement)).collect();
            let mut status = manager
                .request_current(
                    envelope.attempt,
                    RegionRequest {
                        roots: targets.iter().map(|n| n.id).collect(),
                        placements,
                        topology_sequence: manager.sequence(),
                    },
                )
                .map_err(|x| x.to_string())?;
            e.manager = Some(manager.clone());
            let deadline = Instant::now() + Duration::from_secs(60);
            while status == ReservationStatus::Queued {
                if self.ensure_healthy().is_err() || Instant::now() >= deadline {
                    if manager.cancel(envelope.attempt).is_err()
                        && manager.status(envelope.attempt).ok() == Some(ReservationStatus::Granted)
                    {
                        manager
                            .release(envelope.attempt)
                            .map_err(|x| x.to_string())?;
                    }
                    e.manager = None;
                    return Err("owner transfer region wait deadline or unhealthy owner".into());
                }
                std::thread::sleep(Duration::from_millis(1));
                status = manager
                    .status(envelope.attempt)
                    .map_err(|x| x.to_string())?;
            }
            if status != ReservationStatus::Granted {
                e.manager = None;
                return Err("owner transfer region changed before protection".into());
            }
            let handles = targets
                .iter()
                .map(|n| self.resident(*n).map(|h| (*n, h)))
                .collect::<Result<Vec<_>, _>>()?;
            let mut guards = handles
                .iter()
                .map(|(_, h)| h.try_write().ok_or("owner transfer node is protected"))
                .collect::<Result<Vec<_>, _>>()?;
            for ((target, _), node) in handles.iter().zip(&guards) {
                if node.routing.target != *target || node.postings.is_none() {
                    return Err("owner transfer placement or type changed".into());
                }
                if node.reserved.is_some() {
                    return Err(super::construction::append_claim_producers(
                        "owner transfer leaf is protected".into(),
                        node.claim_producer,
                    ));
                }
                let digest = if role == Role::Source {
                    payload.source_centroid
                } else {
                    payload
                        .destinations
                        .iter()
                        .find(|d| d.target == *target)
                        .unwrap()
                        .centroid
                };
                if centroid_digest(&node.routing.centroid) != digest {
                    return Err("owner transfer centroid dependency changed".into());
                }
            }
            let ids = payload
                .destinations
                .iter()
                .flat_map(|d| d.ids.iter().copied())
                .collect::<BTreeSet<_>>();
            if role == Role::Source {
                let p = guards[0].postings.as_ref().unwrap();
                let current = p.ids.iter().copied().collect::<BTreeSet<_>>();
                if current.len() != p.ids.len() || !ids.is_subset(&current) {
                    return Err("owner transfer source membership changed".into());
                }
                let mut kept = PostingList::default();
                kept.ids
                    .try_reserve(p.ids.len() - ids.len())
                    .map_err(|x| x.to_string())?;
                kept.codes
                    .try_reserve(p.codes.len())
                    .map_err(|x| x.to_string())?;
                if p.codes.len() != p.ids.len() * self.code_size() {
                    return Err("invalid source posting codes".into());
                }
                for (slot, id) in p.ids.iter().enumerate() {
                    if !ids.contains(id) {
                        kept.ids.push(*id);
                        kept.codes.extend_from_slice(
                            &p.codes[slot * self.code_size()..(slot + 1) * self.code_size()],
                        );
                    }
                }
                observed.add(Counter::TransferSourceEntriesScanned, p.ids.len() as u64);
                e.source_replacement = Some(kept);
            } else {
                for (node, d) in guards.iter_mut().zip(&payload.destinations) {
                    let p = node.postings.as_mut().unwrap();
                    if d.ids.iter().any(|id| p.ids.contains(id)) {
                        return Err("owner transfer destination membership changed".into());
                    }
                    p.ids.try_reserve(d.ids.len()).map_err(|x| x.to_string())?;
                    p.codes
                        .try_reserve(d.codes.len())
                        .map_err(|x| x.to_string())?;
                }
            }
            #[cfg(test)]
            injected(self, 1).map_err(|x| x.to_string())?;
            for node in &mut guards {
                node.reserved = Some(envelope.attempt);
                node.transfer_source = Some(payload.source.id);
                node.claim_producer = payload.producer;
            }
            drop(guards);
            e.handles = handles;
            e.prepared = true;
            Ok(())
        })();
        if let Err(error) = result {
            if let Some(manager) = e.manager.take() {
                if let Err(failed) = manager.release(envelope.attempt) {
                    self.fail_unresolved(format!(
                        "owner transfer failed preparation release: {failed}"
                    ));
                    return Err(failed.to_string());
                }
            }
            self.terminal_placement(&envelope)?;
            e.source_replacement = None;
            e.failure = Some(error.clone());
            return Err(error);
        }
        Ok(TransferState::Prepared)
    }
    fn step_owner_transfer(
        &self,
        envelope: Envelope,
        role: Role,
        step: TransferStep,
        proof: TransferProof,
    ) -> Result<TransferState, String> {
        let mut observed = self.observability.operation(Scope::TransferStep);
        self.check(&envelope)?;
        let record = self
            .batch_transfers
            .lock()
            .get(&envelope.attempt)
            .cloned()
            .ok_or("unknown owner transfer")?;
        let mut e = record.lock();
        self.check(&envelope)?;
        if e.envelope != envelope || e.role != role {
            return Err("conflicting owner transfer step".into());
        }
        if let Some(error) = &e.failure {
            return Err(error.clone());
        }
        if let Some((old, result)) = e.results.get(&step) {
            if *old != proof {
                return Err("conflicting owner transfer step proof".into());
            }
            return result.clone();
        }
        if !e.prepared {
            return Err("owner transfer was not prepared".into());
        }
        let result = (|| -> Result<TransferState, String> {
            let handles = e.handles.clone();
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut guards = loop {
                let locked = handles
                    .iter()
                    .map(|(_, h)| h.try_write())
                    .collect::<Option<Vec<_>>>();
                if let Some(guards) = locked {
                    break guards;
                }
                self.ensure_healthy()?;
                if Instant::now() >= deadline {
                    return Err("owner transfer node protection deadline".into());
                }
                std::thread::yield_now();
            };
            let terminal = matches!(
                e.transitions.state(),
                TransferState::Released | TransferState::Aborted
            );
            for ((target, h), node) in handles.iter().zip(&guards) {
                let current = self
                    .nodes
                    .get(&target.id)
                    .map(|p| Arc::ptr_eq(p.value(), h))
                    .unwrap_or(false);
                if !current
                    || node.routing.target != *target
                    || (!terminal && node.reserved != Some(envelope.attempt))
                {
                    return Err("owner transfer protection or placement was lost".into());
                }
            }
            let images = if matches!(
                step,
                TransferStep::RemoveSource | TransferStep::ActivateDestination
            ) {
                guards
                    .iter()
                    .map(|node| {
                        let mut image = (*node.routing).clone();
                        image.revision = image
                            .revision
                            .checked_add(1)
                            .ok_or("owner transfer revision exhausted")?;
                        Ok(Arc::new(image))
                    })
                    .collect::<Result<Vec<_>, String>>()?
            } else {
                vec![]
            };
            if step == TransferStep::ActivateDestination {
                for (node, d) in guards.iter().zip(&e.payload.destinations) {
                    let p = node
                        .postings
                        .as_ref()
                        .ok_or("owner transfer target is not a leaf")?;
                    if p.ids.capacity() - p.ids.len() < d.ids.len()
                        || p.codes.capacity() - p.codes.len() < d.codes.len()
                    {
                        return Err("owner transfer reserved capacity was lost".into());
                    }
                }
            }
            let BatchEndpoint {
                transitions,
                source_replacement,
                payload,
                ..
            } = &mut *e;
            let state = transitions
                .apply(step, &proof, || {
                    let mut flight = Unresolved {
                        owner: self,
                        resolved: true,
                    };
                    match step {
                        TransferStep::RemoveSource => {
                            let kept = source_replacement
                                .take()
                                .ok_or(ProtocolError::Prerequisite)?;
                            observed.add(Counter::TransferCompactions, 1);
                            observed
                                .add(Counter::TransferCopiedIdBytes, (kept.ids.len() * 8) as u64);
                            observed.add(Counter::TransferCopiedCodeBytes, kept.codes.len() as u64);
                            flight.resolved = false;
                            guards[0].postings = Some(kept);
                            #[cfg(test)]
                            injected(self, 2)?;
                        }
                        TransferStep::ActivateDestination => {
                            for (node, d) in guards.iter_mut().zip(&payload.destinations) {
                                flight.resolved = false;
                                let p = node.postings.as_mut().unwrap();
                                p.ids.extend_from_slice(&d.ids);
                                p.codes.extend_from_slice(&d.codes);
                                #[cfg(test)]
                                injected(self, 3)?;
                                observed
                                    .add(Counter::TransferCopiedIdBytes, (d.ids.len() * 8) as u64);
                                observed
                                    .add(Counter::TransferCopiedCodeBytes, d.codes.len() as u64);
                            }
                        }
                        TransferStep::Abort | TransferStep::Release => {
                            for node in &mut guards {
                                if node.reserved == Some(envelope.attempt) {
                                    node.reserved = None;
                                    node.transfer_source = None;
                                    node.claim_producer = None;
                                }
                            }
                        }
                        _ => {}
                    }
                    if !images.is_empty() {
                        let _gate = self.routing_gate.write();
                        let mut published = self.routing_images.write();
                        for (((target, _), node), image) in
                            handles.iter().zip(&mut guards).zip(&images)
                        {
                            node.routing = image.clone();
                            published.insert(target.id, image.clone());
                        }
                    }
                    flight.resolved = true;
                    Ok(())
                })
                .map_err(|x| x.to_string())?;
            drop(guards);
            if matches!(state, TransferState::Released | TransferState::Aborted) {
                if let Some(manager) = e.manager.take() {
                    manager.release(envelope.attempt).map_err(|x| {
                        self.fail_unresolved(format!("owner transfer release failed: {x}"));
                        x.to_string()
                    })?;
                }
                self.terminal_placement(&envelope)?;
                // Replay needs the immutable body/outcomes, not resident tree
                // handles or a discarded prepared source buffer.
                e.handles.clear();
                e.source_replacement = None;
            }
            Ok(state)
        })();
        e.results.insert(step, (proof, result.clone()));
        result
    }
}
impl Cluster {
    pub(super) fn transfer_owner_batch_for_job(
        &self,
        source: NodeRef,
        groups: BTreeMap<NodeRef, Vec<u64>>,
        producer: Option<AttemptId>,
        excluded: Option<NodeId>,
    ) -> Result<(), String> {
        self.transfer_owner_chunks(source, groups, producer, excluded, false)
    }

    pub(super) fn transfer_owner_batch_for_construction(
        &self,
        source: NodeRef,
        groups: BTreeMap<NodeRef, Vec<u64>>,
        producer: Option<AttemptId>,
        excluded: Option<NodeId>,
    ) -> Result<(), String> {
        self.transfer_owner_chunks(source, groups, producer, excluded, true)
    }

    fn transfer_owner_chunks(
        &self,
        source: NodeRef,
        groups: BTreeMap<NodeRef, Vec<u64>>,
        producer: Option<AttemptId>,
        excluded: Option<NodeId>,
        schedule_committed: bool,
    ) -> Result<(), String> {
        if groups.is_empty() {
            return Ok(());
        }
        let peer = groups.keys().next().unwrap().authority;
        if source.authority != self.local.owner
            || peer == source.authority
            || groups.keys().any(|t| t.authority != peer)
        {
            return Err("owner batch requires one source and one peer".into());
        }
        for (index, chunk) in bounded_groups(groups, self.local.code_size())
            .into_iter()
            .enumerate()
        {
            let targets = chunk.keys().copied().collect::<Vec<_>>();
            self.run_owner_batch(source, chunk, producer, excluded)?;
            if schedule_committed {
                // A later chunk can reject cleanly. Its retry filters out IDs
                // already moved here, so retain balancing obligations now.
                let depth = super::construction::follow_up_depth();
                self.schedule(source, depth)?;
                for target in targets {
                    self.schedule(target, depth)?;
                }
            }
            #[cfg(test)]
            if let Some(hook) = self.local.batch_chunk_hook.lock().clone() {
                hook(index + 1);
            }
            #[cfg(not(test))]
            let _ = index;
        }
        Ok(())
    }
    fn run_owner_batch(
        &self,
        source: NodeRef,
        groups: BTreeMap<NodeRef, Vec<u64>>,
        producer: Option<AttemptId>,
        excluded: Option<NodeId>,
    ) -> Result<(), String> {
        let mut observed = self.local.observability.operation(Scope::TransferDriver);
        observed.add(Counter::OwnerTransferBatches, 1);
        observed.add(Counter::OwnerTransferTargetGroups, groups.len() as u64);
        observed.add(Counter::TransferGroups, groups.len() as u64);
        let source_image = self.node(source)?;
        let mut destinations = vec![];
        let mut rows = 0;
        for (target, ids) in groups {
            let image = self.node(target)?;
            if !image.leaf {
                return Err("owner transfer target is internal".into());
            }
            let _quantize = self.local.observability.operation(Scope::TransferQuantize);
            let mut codes = vec![];
            for id in &ids {
                codes.extend_from_slice(
                    Code::<1>::quantize(&self.local.vector(*id)?, &image.centroid).as_ref(),
                );
            }
            rows += ids.len();
            destinations.push(Destination {
                target,
                revision: image.revision,
                centroid: centroid_digest(&image.centroid),
                ids,
                codes,
            });
        }
        observed.add(Counter::TransferProposedRows, rows as u64);
        let mut payload = BatchPayload {
            source,
            source_revision: source_image.revision,
            source_centroid: centroid_digest(&source_image.centroid),
            destination_owner: destinations[0].target.authority,
            destinations,
            producer,
            excluded,
            digest: [0; 32],
        };
        payload.digest = payload.content_digest()?;
        payload.validate(self.local.code_size())?;
        let envelope = self.local.envelope(
            "owner-transfer-v1",
            std::iter::once((source.id, source.placement))
                .chain(
                    payload
                        .destinations
                        .iter()
                        .map(|d| (d.target.id, d.target.placement)),
                )
                .collect(),
        )?;
        let mut endpoints = [
            (source.authority, Role::Source),
            (payload.destination_owner, Role::Destination),
        ];
        endpoints.sort_by_key(|p| p.0);
        let mut flight = Unresolved {
            owner: &self.local,
            resolved: false,
        };
        let mut accepted = vec![];
        for (authority, role) in endpoints {
            observed.add(Counter::TransferPayloadIdBytesCloned, (rows * 8) as u64);
            observed.add(
                Counter::TransferPayloadCodeBytesCloned,
                payload
                    .destinations
                    .iter()
                    .map(|d| d.codes.len() as u64)
                    .sum(),
            );
            match self.request(
                authority,
                EngineRequest::OwnerTransfer(BatchRequest::Prepare {
                    envelope: envelope.clone(),
                    payload: payload.clone(),
                    role,
                }),
            ) {
                Ok(EngineResponse::Transfer(TransferState::Prepared)) => {
                    accepted.push((authority, role))
                }
                outcome => {
                    let known = matches!(&outcome,Err(s) if !s.starts_with("transport ") && !s.contains("unresolved") && !s.contains("poison"));
                    let error = match outcome {
                        Err(e) => e,
                        _ => "invalid owner transfer prepare acknowledgement".into(),
                    };
                    for (owner, role) in accepted.into_iter().rev() {
                        let result = self.request(
                            owner,
                            EngineRequest::OwnerTransfer(BatchRequest::Step {
                                envelope: envelope.clone(),
                                role,
                                step: TransferStep::Abort,
                                proof: TransferProof {
                                    abort_decided: true,
                                    ..Default::default()
                                },
                            }),
                        );
                        if !matches!(result, Ok(EngineResponse::Transfer(TransferState::Aborted))) {
                            return Err(format!("unresolved owner transfer abort: {error}"));
                        }
                    }
                    if known {
                        flight.resolved = true;
                    }
                    observed.add(
                        Counter::TransferRejectedGroups,
                        payload.destinations.len() as u64,
                    );
                    return Err(error);
                }
            }
        }
        let mut driver = TransferDriver::default();
        let perform =
            |role: Role, step: TransferStep, d: &mut TransferDriver| -> Result<(), String> {
                let authority = if role == Role::Source {
                    source.authority
                } else {
                    payload.destination_owner
                };
                let mut last = String::new();
                for retry in 0..3 {
                    if retry > 0 {
                        observed_retry(&self.local);
                    }
                    match self.request(
                        authority,
                        EngineRequest::OwnerTransfer(BatchRequest::Step {
                            envelope: envelope.clone(),
                            role,
                            step,
                            proof: d.proof(),
                        }),
                    ) {
                        Ok(EngineResponse::Transfer(state)) => {
                            validate_transfer_ack(role, step, state)?;
                            return d.acknowledge(role, step).map_err(|x| x.to_string());
                        }
                        Ok(_) => return Err("invalid owner transfer acknowledgement".into()),
                        Err(e) => {
                            let retry = e.starts_with("transport ");
                            last = e;
                            if !retry {
                                break;
                            }
                        }
                    }
                }
                Err(format!("unresolved owner transfer {step:?}: {last}"))
            };
        perform(Role::Destination, TransferStep::Stage, &mut driver)?;
        driver
            .decide(DriverDecision::Complete)
            .map_err(|x| x.to_string())?;
        for role in [Role::Source, Role::Destination] {
            perform(role, TransferStep::BeginCompletion, &mut driver)?;
        }
        perform(Role::Source, TransferStep::RemoveSource, &mut driver)?;
        perform(
            Role::Destination,
            TransferStep::ActivateDestination,
            &mut driver,
        )?;
        for role in [Role::Source, Role::Destination] {
            perform(role, TransferStep::Complete, &mut driver)?;
        }
        for (_, role) in endpoints.into_iter().rev() {
            perform(role, TransferStep::Release, &mut driver)?;
        }
        if !driver.success() {
            return Err("unresolved owner transfer completion".into());
        }
        flight.resolved = true;
        observed.add(
            Counter::TransferCompletedGroups,
            payload.destinations.len() as u64,
        );
        self.local
            .transfers
            .fetch_add(rows as u64, Ordering::Relaxed);
        Ok(())
    }
}
fn observed_retry(owner: &OwnerService) {
    let mut o = owner.observability.operation(Scope::TransferRetry);
    o.add(Counter::TransferRetryCalls, 1);
}
#[cfg(test)]
#[path = "batched_transfer_tests.rs"]
mod tests;

#[cfg(test)]
fn injected(owner: &OwnerService, point: u64) -> Result<(), ProtocolError> {
    if owner
        .batch_fault
        .compare_exchange(point, 0, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        Err(ProtocolError::Conflict)
    } else {
        Ok(())
    }
}

fn bounded_groups(
    groups: BTreeMap<NodeRef, Vec<u64>>,
    code_size: usize,
) -> Vec<BTreeMap<NodeRef, Vec<u64>>> {
    let max_rows = (MAX_CODES / code_size.max(1)).min(MAX_IDS).max(1);
    let mut seen = BTreeSet::new();
    let mut chunk = BTreeMap::<NodeRef, Vec<u64>>::new();
    let mut count = 0;
    let mut chunks = vec![];
    for (target, ids) in groups {
        for id in ids {
            if !seen.insert(id) {
                continue;
            }
            if count == max_rows || (!chunk.contains_key(&target) && chunk.len() == MAX_TARGETS) {
                chunks.push(std::mem::take(&mut chunk));
                count = 0;
            }
            chunk.entry(target).or_default().push(id);
            count += 1;
        }
    }
    if !chunk.is_empty() {
        chunks.push(chunk);
    }
    chunks
}
