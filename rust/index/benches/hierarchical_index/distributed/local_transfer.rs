//! Same-owner membership changes claim leaves without a distributed transaction.
//! Claims hide intermediate posting edits from writers and saved snapshots while
//! immutable routing remains readable. Every accepted target shares one source scan.
use super::engine::{Cluster, OwnerService, PostingList, Resident};
use super::observability::{Counter, Scope};
use super::protocol::envelope::Envelope;
use super::protocol::NodeId;
use super::routing::NodeRef;
use chroma_index::quantization::Code;
use parking_lot::RwLock;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{atomic::Ordering, Arc},
};

#[derive(Default, Debug)]
pub(super) struct LocalTransferResult {
    pub moved: usize,
    pub destinations: Vec<NodeRef>,
    pub retries: BTreeMap<NodeRef, Vec<u64>>,
    pub blocked: BTreeSet<NodeId>,
    pub blocked_jobs: BTreeSet<super::protocol::AttemptId>,
}
struct Claims<'a> {
    owner: &'a OwnerService,
    envelope: Envelope,
    handles: Vec<Arc<RwLock<Resident>>>,
    mutated: bool,
    complete: bool,
}
impl Drop for Claims<'_> {
    fn drop(&mut self) {
        let _cleanup = self
            .owner
            .observability
            .operation(Scope::LocalTransferCleanup);
        if self.mutated && !self.complete {
            self.owner.fail_unresolved(
                "local transfer exited after a membership edit without completion".into(),
            );
            return;
        }
        for handle in &self.handles {
            let mut node = handle.write();
            if node.reserved == Some(self.envelope.attempt) {
                node.reserved = None;
                node.transfer_source = None;
                node.claim_producer = None;
            }
        }
        let _terminal = self
            .owner
            .observability
            .operation(Scope::LocalTransferTerminal);
        if let Err(error) = self.owner.terminal_placement(&self.envelope) {
            self.owner
                .fail_unresolved(format!("unresolved local transfer admission: {error}"));
        }
    }
}
struct Target {
    target: NodeRef,
    centroid: Vec<f32>,
    existing: HashSet<u64>,
    ids: Vec<u64>,
    codes: Vec<u8>,
}
#[cfg(test)]
thread_local! {static FAIL_AFTER_APPEND:std::cell::Cell<bool>=const{std::cell::Cell::new(false)};}

#[cfg(test)]
thread_local! {static BEFORE_PUBLICATION:std::cell::RefCell<Option<Box<dyn FnOnce()>>>=std::cell::RefCell::new(None);}

impl OwnerService {
    /// The source and all available local destinations receive one token while
    /// their short locks are held together. Contended destinations stay pending.
    pub(super) fn transfer_local_many(
        &self,
        source: NodeRef,
        groups: BTreeMap<NodeRef, Vec<u64>>,
        evacuate: bool,
    ) -> Result<LocalTransferResult, String> {
        self.transfer_local_many_for_job(source, groups, evacuate, None)
    }
    pub(super) fn transfer_local_many_for_job(
        &self,
        source: NodeRef,
        groups: BTreeMap<NodeRef, Vec<u64>>,
        evacuate: bool,
        producer: Option<super::protocol::AttemptId>,
    ) -> Result<LocalTransferResult, String> {
        self.ensure_healthy()?;
        if source.authority != self.owner || groups.keys().any(|n| n.authority != self.owner) {
            return Err("local transfer requires one authority".into());
        }
        let mut observed = self.observability.operation(Scope::LocalTransfer);
        observed.add(Counter::LocalTransferBatches, 1);
        observed.add(Counter::LocalTransferTargetGroups, groups.len() as u64);
        let admission = self.observability.operation(Scope::LocalTransferAdmission);
        let envelope = self.envelope("local-transfer", vec![(source.id, source.placement)])?;
        self.admit_placement(
            &envelope,
            bincode::serialize(&(source, &groups, evacuate)).map_err(|e| e.to_string())?,
        )?;
        drop(admission);
        let mut claims = Claims {
            owner: self,
            envelope,
            handles: vec![],
            mutated: false,
            complete: false,
        };
        let mut result = LocalTransferResult::default();
        // A deterministic first destination owns every proposed ID, including
        // an unavailable one. Duplicate candidates cannot move twice.
        let mut proposed = HashMap::new();
        let mut ordered = BTreeSet::new();
        ordered.insert(source);
        for (target, ids) in &groups {
            if *target != source {
                ordered.insert(*target);
                for id in ids {
                    proposed.entry(*id).or_insert(*target);
                }
            }
        }
        if proposed.is_empty() {
            return Ok(result);
        }
        let handles = ordered
            .into_iter()
            .filter_map(|target| {
                self.nodes
                    .get(&target.id)
                    .map(|h| (target, Arc::clone(h.value())))
            })
            .collect::<Vec<_>>();
        let Some(source_slot) = handles.iter().position(|(target, _)| *target == source) else {
            return Ok(result);
        };
        let mut locks = handles
            .iter()
            .map(|(_, h)| h.try_write())
            .collect::<Vec<_>>();
        let source_free = locks[source_slot].as_ref().is_some_and(|n| {
            n.reserved.is_none() && n.routing.target == source && n.postings.is_some()
        });
        if !source_free {
            result
                .blocked_jobs
                .extend(locks[source_slot].as_ref().and_then(|n| n.claim_producer));
            let current = locks[source_slot]
                .as_ref()
                .map(|n| {
                    n.postings
                        .as_ref()
                        .map(|p| p.ids.iter().copied().collect::<HashSet<_>>())
                })
                .flatten();
            result.blocked.insert(
                locks[source_slot]
                    .as_ref()
                    .and_then(|n| n.transfer_source)
                    .unwrap_or(source.id),
            );
            for (id, target) in proposed {
                if current.as_ref().is_none_or(|ids| ids.contains(&id)) {
                    result.retries.entry(target).or_default().push(id)
                }
            }
            return Ok(result);
        }
        claims
            .handles
            .try_reserve(handles.len())
            .map_err(|e| e.to_string())?;
        let mut available = BTreeSet::new();
        for ((target, handle), slot) in handles.iter().zip(&mut locks) {
            if let Some(node) = slot.as_mut() {
                if node.reserved.is_none()
                    && node.routing.target == *target
                    && node.postings.is_some()
                {
                    node.reserved = Some(claims.envelope.attempt);
                    node.transfer_source = Some(source.id);
                    node.claim_producer = producer;
                    available.insert(*target);
                    claims.handles.push(handle.clone());
                } else if *target != source {
                    result.blocked_jobs.extend(node.claim_producer);
                    result
                        .blocked
                        .insert(node.transfer_source.unwrap_or(target.id));
                }
            } else {
                result.blocked.insert(target.id);
            }
        }
        drop(locks);
        let _prepare = self
            .observability
            .operation(Scope::LocalTransferPreparation);
        let source_handle = &handles[source_slot].1;
        let (source_centroid, source_postings) = {
            let node = source_handle.read();
            (
                node.routing.centroid.clone(),
                node.postings.as_ref().unwrap().clone(),
            )
        };
        observed.add(
            Counter::TransferPayloadIdBytesCloned,
            (source_postings.ids.len() * 8) as u64,
        );
        observed.add(
            Counter::TransferPayloadCodeBytesCloned,
            source_postings.codes.len() as u64,
        );
        let size = self.code_size();
        if source_postings.codes.len()
            != source_postings
                .ids
                .len()
                .checked_mul(size)
                .ok_or("posting byte overflow")?
        {
            return Err("invalid local source posting codes".into());
        }
        let mut targets = BTreeMap::new();
        for target in groups
            .keys()
            .filter(|n| **n != source && available.contains(n))
        {
            let handle = &handles.iter().find(|(n, _)| n == target).unwrap().1;
            let node = handle.read();
            let posting = node.postings.as_ref().unwrap();
            if posting.codes.len()
                != posting
                    .ids
                    .len()
                    .checked_mul(size)
                    .ok_or("posting byte overflow")?
            {
                return Err("invalid local destination posting codes".into());
            }
            targets.insert(
                *target,
                Target {
                    target: *target,
                    centroid: node.routing.centroid.clone(),
                    existing: posting.ids.iter().copied().collect(),
                    ids: vec![],
                    codes: vec![],
                },
            );
        }
        let mut remaining = PostingList::default();
        remaining
            .ids
            .try_reserve(source_postings.ids.len())
            .map_err(|e| e.to_string())?;
        remaining
            .codes
            .try_reserve(source_postings.codes.len())
            .map_err(|e| e.to_string())?;
        let mut unique = HashSet::new();
        unique
            .try_reserve(source_postings.ids.len())
            .map_err(|e| e.to_string())?;
        for (slot, id) in source_postings.ids.iter().copied().enumerate() {
            if !unique.insert(id) {
                return Err("duplicate current source membership in local transfer".into());
            }
            let mut accepted = false;
            if let Some(target) = proposed.get(&id) {
                if let Some(prepared) = targets.get_mut(target) {
                    if prepared.existing.contains(&id) {
                        return Err(
                            "duplicate current destination membership in local transfer".into()
                        );
                    }
                    let vector = self.vector(id)?;
                    if evacuate
                        || self.metric.distance(&vector, &prepared.centroid)
                            < self.metric.distance(&vector, &source_centroid)
                    {
                        prepared.ids.try_reserve(1).map_err(|e| e.to_string())?;
                        prepared
                            .codes
                            .try_reserve(size)
                            .map_err(|e| e.to_string())?;
                        prepared.ids.push(id);
                        prepared.codes.extend_from_slice(
                            Code::<1>::quantize(&vector, &prepared.centroid).as_ref(),
                        );
                        accepted = true;
                        result.moved += 1;
                    }
                } else {
                    result.retries.entry(*target).or_default().push(id);
                }
            }
            if !accepted {
                remaining.ids.push(id);
                remaining
                    .codes
                    .extend_from_slice(&source_postings.codes[slot * size..(slot + 1) * size]);
            }
        }
        observed.add(
            Counter::LocalTransferMembershipProbes,
            source_postings.ids.len() as u64,
        );
        observed.add(
            Counter::TransferSourceEntriesScanned,
            source_postings.ids.len() as u64,
        );
        observed.add(
            Counter::TransferCopiedIdBytes,
            (remaining.ids.len() * 8) as u64,
        );
        observed.add(
            Counter::TransferCopiedCodeBytes,
            remaining.codes.len() as u64,
        );
        drop(_prepare);
        if result.moved == 0 {
            return Ok(result);
        }
        let _commit = self.observability.operation(Scope::LocalTransferCommit);
        // Never hold one resident lock while waiting for another. An ancestor
        // can briefly rewrite parent links while the membership claim is held.
        let mut commit = handles
            .iter()
            .map(|(n, h)| {
                if available.contains(n) {
                    h.try_write()
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if handles
            .iter()
            .zip(&commit)
            .any(|((n, _), guard)| available.contains(n) && guard.is_none())
        {
            result.moved = 0;
            for target in targets.values() {
                result
                    .retries
                    .entry(target.target)
                    .or_default()
                    .extend(&target.ids)
            }
            return Ok(result);
        }
        for ((target, _), guard) in handles.iter().zip(&mut commit) {
            if !available.contains(target) {
                continue;
            }
            let node = guard.as_mut().unwrap();
            if node.reserved != Some(claims.envelope.attempt)
                || node.routing.target != *target
                || node.postings.is_none()
            {
                return Err("local transfer claim dependency changed".into());
            }
            let expected = if *target == source {
                &source_centroid
            } else {
                &targets[target].centroid
            };
            if &node.routing.centroid != expected {
                return Err("local transfer centroid dependency changed".into());
            }
            let amount = targets.get(target).map_or(0, |t| t.ids.len());
            if *target == source || amount > 0 {
                node.routing
                    .revision
                    .checked_add(1)
                    .ok_or("local transfer routing revision exhausted")?;
            }
            if amount > 0 {
                let posting = node.postings.as_mut().unwrap();
                posting.ids.try_reserve(amount).map_err(|e| e.to_string())?;
                posting
                    .codes
                    .try_reserve(targets[target].codes.len())
                    .map_err(|e| e.to_string())?;
            }
        }
        let mut updates = Vec::new();
        updates
            .try_reserve(targets.len() + 1)
            .map_err(|e| e.to_string())?;
        for ((target, _), guard) in handles.iter().zip(&mut commit) {
            if *target == source {
                continue;
            }
            let Some(prepared) = targets.get(target).filter(|t| !t.ids.is_empty()) else {
                continue;
            };
            let node = guard.as_mut().unwrap();
            claims.mutated = true;
            let posting = node.postings.as_mut().unwrap();
            posting.ids.extend_from_slice(&prepared.ids);
            posting.codes.extend_from_slice(&prepared.codes);
            Arc::make_mut(&mut node.routing).revision += 1;
            updates.push((*node.routing).clone());
            result.destinations.push(*target);
            observed.add(
                Counter::TransferCopiedIdBytes,
                (prepared.ids.len() * 8) as u64,
            );
            observed.add(
                Counter::TransferCopiedCodeBytes,
                prepared.codes.len() as u64,
            );
            #[cfg(test)]
            if FAIL_AFTER_APPEND.with(|f| f.replace(false)) {
                panic!("injected local transfer failure after append")
            }
        }
        let source_node = commit[source_slot].as_mut().unwrap();
        source_node.postings = Some(remaining);
        Arc::make_mut(&mut source_node.routing).revision += 1;
        updates.push((*source_node.routing).clone());
        observed.add(Counter::TransferCompactions, 1);
        #[cfg(test)]
        BEFORE_PUBLICATION.with(|slot| {
            let action = slot.borrow_mut().take();
            if let Some(action) = action {
                action()
            }
        });
        // Parent rewrites also hold these leaf locks through publication. Keep
        // our newer membership revisions ordered with their routing images.
        self.publish_images(updates, &[])?;
        drop(commit);
        self.transfers
            .fetch_add(result.moved as u64, Ordering::Relaxed);
        claims.complete = true;
        Ok(result)
    }
}
impl Cluster {
    pub(super) fn transfer_local_exact(
        &self,
        source: NodeRef,
        destination: NodeRef,
        ids: Vec<u64>,
    ) -> Result<(), String> {
        self.transfer_local_exact_for_job(source, destination, ids, None)
    }
    pub(super) fn transfer_local_exact_for_job(
        &self,
        source: NodeRef,
        destination: NodeRef,
        ids: Vec<u64>,
        producer: Option<super::protocol::AttemptId>,
    ) -> Result<(), String> {
        let count = ids.len();
        let result = self.local.transfer_local_many_for_job(
            source,
            BTreeMap::from([(destination, ids)]),
            true,
            producer,
        )?;
        if !result.retries.is_empty() {
            return Err(super::construction::append_claim_producers(
                if self
                    .local
                    .resident(source)
                    .ok()
                    .is_some_and(|h| h.read().preparing_split)
                    || self
                        .local
                        .resident(destination)
                        .ok()
                        .is_some_and(|h| h.read().preparing_split)
                {
                    "transfer leaf is protected by a split preparation claim".into()
                } else {
                    format!(
                        "transfer leaf is protected by a local transfer claim source={}",
                        result.blocked.first().copied().unwrap_or(source.id)
                    )
                },
                result.blocked_jobs,
            ));
        }
        if result.moved != count {
            return Err("source membership changed during local transfer".into());
        }
        Ok(())
    }
}
#[cfg(test)]
#[path = "local_transfer_tests.rs"]
pub(crate) mod tests;
