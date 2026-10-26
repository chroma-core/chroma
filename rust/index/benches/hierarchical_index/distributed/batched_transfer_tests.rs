use super::super::local_transfer::tests::{
    fixed_transfer_fixture, fixed_work_delta, verify_fixed_transfer, FixedTransferFixture,
};
use super::super::transport::{RpcClient, RpcServer, TransportConfig};
use super::*;
fn vectors() -> Arc<BTreeMap<u64, Arc<[f32]>>> {
    let mut v = BTreeMap::new();
    for id in 0..2048u64 {
        v.insert(
            id,
            vec![if id < 24 { (id / 3) as f32 } else { 10. }; 768].into(),
        );
    }
    for group in 0..8u64 {
        v.insert(3000 + group, vec![group as f32; 768].into());
    }
    for id in 4000..4002u64 {
        v.insert(id, vec![20.; 768].into());
    }
    Arc::new(v)
}
fn connected() -> (FixedTransferFixture, RpcServer) {
    let mut f = fixed_transfer_fixture(true, vectors(), BTreeMap::new());
    let peer = f.owners[1].clone();
    let server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "owner-batch".into(),
        TransportConfig::default(),
        Arc::new(move |r: EngineRequest| peer.handle(r)),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "owner-batch".into(),
        TransportConfig::default(),
    )
    .unwrap();
    let mut raw = Cluster::new(
        f.owners[0].clone(),
        BTreeMap::from([(2, client)]),
        f.cluster.current_view().value.root.clone(),
    );
    raw.publication = super::super::protocol::publication::Publication::new(
        f.cluster.current_view().value.clone(),
        1_000_000,
    );
    f.cluster = Arc::new(raw);
    f.owners[0].bind_cluster(&f.cluster);
    (f, server)
}
#[test]
fn owner_batch_fixed_twenty_four_moves_matches_pair_and_local_bulk_once() {
    let (f, mut server) = connected();
    let local = fixed_transfer_fixture(false, vectors(), BTreeMap::new());
    let before = f.owners[0].observability.snapshot();
    let wire = f.cluster.peers[&2].counters();
    f.cluster
        .transfer_owner_batch_for_job(f.source, f.routes.clone(), None, None)
        .unwrap();
    let work = fixed_work_delta(&before, &f.owners[0].observability.snapshot());
    let after = f.cluster.peers[&2].counters();
    assert_eq!(work["counters"]["TransferProtocolCalls"], 11);
    assert_eq!(work["counters"]["TransferCompactions"], 1);
    assert_eq!(work["counters"]["RemoteEngineCalls"], 14);
    assert_eq!(work["counters"]["LocalEngineCalls"], 6);
    assert_eq!(work["counters"]["TransferSourceEntriesScanned"], 2048);
    assert_eq!(after.messages_sent - wire.messages_sent, 14);
    assert_eq!(
        local.owners[0]
            .transfer_local_many(local.source, local.routes.clone(), false)
            .unwrap()
            .moved,
        24
    );
    assert_eq!(verify_fixed_transfer(&f), verify_fixed_transfer(&local));
    eprintln!(
        "OWNER_BATCH_FIXED {}",
        serde_json::json!({"work":work,"framed_bytes":after.bytes_sent-wire.bytes_sent+after.bytes_received-wire.bytes_received})
    );
    server.shutdown();
}
fn payload(f: &FixedTransferFixture) -> (Envelope, BatchPayload) {
    let s = f.cluster.node(f.source).unwrap();
    let destinations = f
        .routes
        .iter()
        .map(|(target, ids)| {
            let image = f.cluster.node(*target).unwrap();
            Destination {
                target: *target,
                revision: image.revision,
                centroid: centroid_digest(&image.centroid),
                ids: ids.clone(),
                codes: ids
                    .iter()
                    .flat_map(|id| {
                        Code::<1>::quantize(&f.vectors[id], &image.centroid)
                            .as_ref()
                            .to_vec()
                    })
                    .collect(),
            }
        })
        .collect();
    let mut p = BatchPayload {
        source: f.source,
        source_revision: s.revision,
        source_centroid: centroid_digest(&s.centroid),
        destination_owner: 2,
        destinations,
        producer: None,
        excluded: None,
        digest: [0; 32],
    };
    p.digest = p.content_digest().unwrap();
    let e = f.owners[0]
        .envelope(
            "owner-transfer-v1",
            std::iter::once((f.source.id, 0))
                .chain(f.routes.keys().map(|n| (n.id, 0)))
                .collect(),
        )
        .unwrap();
    (e, p)
}
fn call(
    f: &FixedTransferFixture,
    e: &Envelope,
    role: Role,
    step: TransferStep,
    proof: TransferProof,
) -> Result<TransferState, String> {
    let owner = if role == Role::Source { 1 } else { 2 };
    match f.cluster.request(
        owner,
        EngineRequest::OwnerTransfer(BatchRequest::Step {
            envelope: e.clone(),
            role,
            step,
            proof,
        }),
    )? {
        EngineResponse::Transfer(s) => Ok(s),
        _ => Err("invalid fixture response".into()),
    }
}
fn prepare(f: &FixedTransferFixture, e: &Envelope, p: &BatchPayload) {
    for (owner, role) in [(1, Role::Source), (2, Role::Destination)] {
        assert!(matches!(
            f.cluster.request(
                owner,
                EngineRequest::OwnerTransfer(BatchRequest::Prepare {
                    envelope: e.clone(),
                    payload: p.clone(),
                    role
                })
            ),
            Ok(EngineResponse::Transfer(TransferState::Prepared))
        ));
    }
}
fn advance(
    f: &FixedTransferFixture,
    e: &Envelope,
    role: Role,
    step: TransferStep,
    d: &mut TransferDriver,
) {
    call(f, e, role, step, d.proof()).unwrap();
    d.acknowledge(role, step).unwrap();
}
fn finish(f: &FixedTransferFixture, e: &Envelope, d: &mut TransferDriver) {
    advance(f, e, Role::Source, TransferStep::RemoveSource, d);
    advance(
        f,
        e,
        Role::Destination,
        TransferStep::ActivateDestination,
        d,
    );
    for role in [Role::Source, Role::Destination] {
        advance(f, e, role, TransferStep::Complete, d);
    }
    for role in [Role::Destination, Role::Source] {
        advance(f, e, role, TransferStep::Release, d);
    }
    assert!(d.success());
}
#[test]
fn owner_batch_step_replay_identity_staging_and_phase_drain() {
    let (f, mut server) = connected();
    let (e, p) = payload(&f);
    prepare(&f, &e, &p);
    for owner in &f.owners {
        owner
            .handle(EngineRequest::CloseEpoch { run: 27, epoch: 0 })
            .unwrap();
        assert!(owner
            .handle(EngineRequest::SealEpoch { run: 27, epoch: 0 })
            .is_err());
        assert!(owner.ensure_save_quiescent(27, 0).is_err());
    }
    let mut changed = p.clone();
    changed.producer = Some(AttemptId {
        authority: 99,
        sequence: 3,
    });
    changed.digest = changed.content_digest().unwrap();
    assert!(f.owners[0]
        .handle(EngineRequest::OwnerTransfer(BatchRequest::Prepare {
            envelope: e.clone(),
            payload: changed,
            role: Role::Source
        }))
        .is_err());
    let mut d = TransferDriver::default();
    let stage_proof = d.proof();
    advance(&f, &e, Role::Destination, TransferStep::Stage, &mut d);
    assert_eq!(
        f.owners[0]
            .resident(f.source)
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids
            .len(),
        2048
    );
    for target in f.routes.keys() {
        assert_eq!(
            f.owners[1]
                .resident(*target)
                .unwrap()
                .read()
                .postings
                .as_ref()
                .unwrap()
                .ids
                .len(),
            1
        );
    }
    assert!(call(
        &f,
        &e,
        Role::Destination,
        TransferStep::ActivateDestination,
        TransferProof::default()
    )
    .is_err());
    // A distinct failed step is a fixed misuse outcome; use a fresh attempt
    // after abort to run the valid state sequence below.
    for role in [Role::Destination, Role::Source] {
        call(
            &f,
            &e,
            role,
            TransferStep::Abort,
            TransferProof {
                abort_decided: true,
                ..Default::default()
            },
        )
        .unwrap();
    }
    for owner in &f.owners {
        owner
            .handle(EngineRequest::SealEpoch { run: 27, epoch: 0 })
            .unwrap();
        assert!(owner.ensure_save_quiescent(27, 0).is_ok());
        owner
            .handle(EngineRequest::OpenEpoch { run: 27, epoch: 1 })
            .unwrap();
    }
    assert!(call(&f, &e, Role::Destination, TransferStep::Stage, stage_proof).is_err());
    let (e, p) = payload(&f);
    prepare(&f, &e, &p);
    let mut d = TransferDriver::default();
    let stage_proof = d.proof();
    advance(&f, &e, Role::Destination, TransferStep::Stage, &mut d);
    d.decide(DriverDecision::Complete).unwrap();
    for role in [Role::Source, Role::Destination] {
        advance(&f, &e, role, TransferStep::BeginCompletion, &mut d);
    }
    finish(&f, &e, &mut d);
    assert_eq!(
        call(&f, &e, Role::Destination, TransferStep::Stage, stage_proof).unwrap(),
        TransferState::Staged,
        "replay returns retained step outcome after release"
    );
    assert!(call(&f, &e, Role::Destination, TransferStep::Stage, d.proof()).is_err());
    verify_fixed_transfer(&f);
    for o in &f.owners {
        for entry in o.batch_transfers.lock().values() {
            let e = entry.lock();
            assert!(e.handles.is_empty());
            assert!(e.source_replacement.is_none());
        }
    }
    server.shutdown();
}
#[test]
fn owner_batch_predecision_failure_aborts_all_and_actual_split_claim_defers_without_drop() {
    let (f, mut server) = connected();
    f.owners[1].batch_fault.store(1, Ordering::SeqCst);
    assert!(f
        .cluster
        .transfer_owner_batch_for_job(f.source, f.routes.clone(), None, None)
        .is_err());
    for owner in &f.owners {
        owner.ensure_healthy().unwrap();
        for n in owner.nodes.iter() {
            assert!(n.value().read().reserved.is_none());
        }
    }
    assert_eq!(
        f.owners[0]
            .resident(f.source)
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids
            .len(),
        2048
    );
    let target = *f.routes.keys().next().unwrap();
    let producer = AttemptId {
        authority: 2,
        sequence: 777,
    };
    let held = f.owners[1]
        .claim_source_leaf_for_job(target, Some(producer))
        .unwrap()
        .unwrap();
    // Actual claim leaves immutable navigation readable and carries exact producer identity.
    f.owners[1].node(target).unwrap();
    let error = f
        .cluster
        .transfer_owner_batch_for_job(f.source, f.routes.clone(), None, None)
        .unwrap_err();
    assert!(error.contains("producer_jobs=2:777"), "{error}");
    assert_eq!(
        f.owners[0]
            .resident(f.source)
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids
            .len(),
        2048
    );
    assert!(f.owners[0]
        .resident(f.source)
        .unwrap()
        .read()
        .reserved
        .is_none());
    drop(held);
    f.cluster
        .transfer_owner_batch_for_job(f.source, f.routes.clone(), None, None)
        .unwrap();
    verify_fixed_transfer(&f);
    server.shutdown();
}
#[test]
fn owner_batch_partial_source_or_destination_edit_fails_closed_and_blocks_saved_state() {
    for point in [2, 3] {
        let (f, mut server) = connected();
        let owner = if point == 2 { 0 } else { 1 };
        f.owners[owner].batch_fault.store(point, Ordering::SeqCst);
        assert!(f
            .cluster
            .transfer_owner_batch_for_job(f.source, f.routes.clone(), None, None)
            .is_err());
        assert!(f.owners[0].ensure_healthy().is_err());
        assert!(f.owners[owner].ensure_healthy().is_err());
        for o in &f.owners {
            o.handle(EngineRequest::CloseEpoch { run: 27, epoch: 0 })
                .unwrap();
            assert!(o
                .handle(EngineRequest::SealEpoch { run: 27, epoch: 0 })
                .is_err());
            assert!(o.ensure_save_quiescent(27, 0).is_err());
        }
        assert_eq!(
            f.owners[0]
                .resident(f.source)
                .unwrap()
                .read()
                .postings
                .as_ref()
                .unwrap()
                .ids
                .len(),
            2024
        );
        if point == 3 {
            let total = f
                .routes
                .keys()
                .map(|n| {
                    f.owners[1]
                        .resident(*n)
                        .unwrap()
                        .read()
                        .postings
                        .as_ref()
                        .unwrap()
                        .ids
                        .len()
                })
                .sum::<usize>();
            assert_eq!(total, 11, "one destination appends before injected failure");
        }
        server.shutdown();
    }
}
struct DropAck {
    response: EngineResponse,
    drop: bool,
}
impl Serialize for DropAck {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.drop {
            Err(serde::ser::Error::custom(
                "injected lost successful acknowledgement",
            ))
        } else {
            self.response.serialize(serializer)
        }
    }
}
#[test]
fn owner_batch_actual_driver_retries_lost_successful_ack_without_reapplying() {
    for lost in [
        TransferStep::Stage,
        TransferStep::BeginCompletion,
        TransferStep::ActivateDestination,
        TransferStep::Complete,
        TransferStep::Release,
    ] {
        let mut f = fixed_transfer_fixture(true, vectors(), BTreeMap::new());
        let peer = f.owners[1].clone();
        let once = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let armed = once.clone();
        let mut server=RpcServer::bind("127.0.0.1:0".parse().unwrap(),"127.0.0.1:0".parse().unwrap(),"lost-owner-batch".into(),TransportConfig::default(),Arc::new(move|request:EngineRequest|{
            let drop=matches!(&request,EngineRequest::OwnerTransfer(BatchRequest::Step{role:Role::Destination,step,..}) if *step==lost)&&armed.swap(false,Ordering::SeqCst);
            peer.handle(request).map(|response|DropAck{response,drop})
        })).unwrap();
        let client = RpcClient::new(
            server.data_addr(),
            server.control_addr(),
            "lost-owner-batch".into(),
            TransportConfig::default(),
        )
        .unwrap();
        let mut raw = Cluster::new(
            f.owners[0].clone(),
            BTreeMap::from([(2, client)]),
            f.cluster.current_view().value.root.clone(),
        );
        raw.publication = super::super::protocol::publication::Publication::new(
            f.cluster.current_view().value.clone(),
            1_000_000,
        );
        f.cluster = Arc::new(raw);
        f.owners[0].bind_cluster(&f.cluster);
        f.cluster
            .transfer_owner_batch_for_job(f.source, f.routes.clone(), None, None)
            .unwrap();
        assert!(!once.load(Ordering::SeqCst));
        verify_fixed_transfer(&f);
        let work = f.owners[0].observability.snapshot();
        assert_eq!(work.counters["TransferCompactions"], 1);
        assert_eq!(work.counters["TransferRetryCalls"], 1);
        assert_eq!(work.counters["TransferProtocolCalls"], 12);
        for o in &f.owners {
            o.ensure_healthy().unwrap();
            assert_eq!(o.regions.read().state_snapshot()["active_grants"], 0);
        }
        server.shutdown();
    }
}
#[test]
fn owner_batch_dependency_rejection_preserves_data_and_full_body_identity() {
    for fault in 0..5 {
        let (f, mut server) = connected();
        let (e, p) = payload(&f);
        let target = p.destinations[0].target;
        let h = f.owners[if fault == 3 { 0 } else { 1 }]
            .resident(if fault == 3 { f.source } else { target })
            .unwrap();
        {
            let mut node = h.write();
            match fault {
                0 => Arc::make_mut(&mut node.routing).centroid[0] += 1.,
                1 => Arc::make_mut(&mut node.routing).target.placement += 1,
                2 => node.postings = None,
                3 => {
                    let p = node.postings.as_mut().unwrap();
                    p.ids.remove(0);
                    p.codes.drain(..f.owners[0].code_size());
                }
                _ => {
                    let p = node.postings.as_mut().unwrap();
                    p.ids.push(0);
                    p.codes
                        .extend_from_slice(&p.codes.clone()[..f.owners[1].code_size()]);
                }
            }
        }
        let role = if fault == 3 {
            Role::Source
        } else {
            Role::Destination
        };
        let owner = if role == Role::Source { 0 } else { 1 };
        let request = BatchRequest::Prepare {
            envelope: e.clone(),
            payload: p.clone(),
            role,
        };
        assert!(f.owners[owner]
            .handle_owner_transfer(request.clone())
            .is_err());
        assert!(
            f.owners[owner].handle_owner_transfer(request).is_err(),
            "rejection is retained"
        );
        assert_eq!(
            f.owners[owner].regions.read().state_snapshot()["active_grants"],
            0
        );
        f.owners[owner].ensure_healthy().unwrap();
        server.shutdown();
    }
    let (f, mut server) = connected();
    let (e, p) = payload(&f);
    prepare(&f, &e, &p);
    for variant in 0..3 {
        let mut changed = p.clone();
        match variant {
            0 => changed.destinations[0].codes[0] ^= 1,
            1 => changed.destinations[0].ids[0] += 99,
            _ => changed.destinations[0].target.placement += 1,
        };
        changed.digest = changed.content_digest().unwrap();
        assert!(f.owners[1]
            .handle_owner_transfer(BatchRequest::Prepare {
                envelope: e.clone(),
                payload: changed,
                role: Role::Destination
            })
            .is_err());
    }
    for role in [Role::Destination, Role::Source] {
        call(
            &f,
            &e,
            role,
            TransferStep::Abort,
            TransferProof {
                abort_decided: true,
                ..Default::default()
            },
        )
        .unwrap();
    }
    server.shutdown();
}
#[test]
fn owner_batch_payload_partition_preserves_all_targets_and_ids() {
    let groups = (0..129u128)
        .map(|i| {
            (
                NodeRef {
                    id: (1 << 90) + i,
                    authority: 2,
                    placement: 9,
                },
                vec![i as u64],
            )
        })
        .collect::<BTreeMap<_, _>>();
    let chunks = bounded_groups(groups.clone(), 112);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].len(), 128);
    assert_eq!(chunks[1].len(), 1);
    assert_eq!(
        chunks.into_iter().flatten().collect::<BTreeMap<_, _>>(),
        groups
    );
    let (f, mut server) = large_source_fixture();
    let target = *f.routes.keys().next().unwrap();
    let ids = (0..3).chain(10000..16000).collect::<Vec<u64>>();
    f.cluster
        .transfer_owner_batch_for_job(f.source, BTreeMap::from([(target, ids)]), None, None)
        .unwrap();
    let work = f.owners[0].observability.snapshot();
    assert_eq!(work.counters["OwnerTransferBatches"], 2);
    assert_eq!(work.counters["TransferCompactions"], 2);
    assert_eq!(work.counters["TransferProtocolCalls"], 22);
    let mut all = BTreeSet::new();
    for o in &f.owners {
        let EngineResponse::Snapshot(nodes) = o.handle(EngineRequest::Snapshot).unwrap() else {
            panic!()
        };
        for n in nodes {
            let p = n.postings.unwrap();
            for id in &p.ids {
                assert!(all.insert(*id));
            }
            assert_eq!(
                p.codes,
                p.ids
                    .iter()
                    .flat_map(
                        |id| Code::<1>::quantize(&f.vectors[id], &n.routing.centroid)
                            .as_ref()
                            .to_vec()
                    )
                    .collect::<Vec<_>>()
            );
        }
    }
    assert_eq!(all, f.vectors.keys().copied().collect());
    assert_eq!(
        f.owners[0]
            .resident(f.source)
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids,
        (3..2048).collect::<Vec<u64>>()
    );
    assert_eq!(
        f.owners[1]
            .resident(target)
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids
            .len(),
        6004
    );
    server.shutdown();
}
#[test]
fn owner_batch_reciprocal_drivers_acquire_owners_in_one_order() {
    let (f, mut server2) = connected();
    let owner1 = f.owners[0].clone();
    let mut server1 = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "reciprocal-owner-batch".into(),
        TransportConfig::default(),
        Arc::new(move |r: EngineRequest| owner1.handle(r)),
    )
    .unwrap();
    let client = RpcClient::new(
        server1.data_addr(),
        server1.control_addr(),
        "reciprocal-owner-batch".into(),
        TransportConfig::default(),
    )
    .unwrap();
    let reverse = Arc::new(Cluster::new(
        f.owners[1].clone(),
        BTreeMap::from([(1, client)]),
        f.cluster.current_view().value.root.clone(),
    ));
    reverse.install_view(&f.cluster.current_view()).unwrap();
    f.owners[1].bind_cluster(&reverse);
    let peer_source = *f.routes.keys().next().unwrap();
    let reverse_routes = BTreeMap::from([(f.source, vec![3000])]);
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let a = f.cluster.clone();
    let b = barrier.clone();
    let source = f.source;
    let forward = f.routes.clone();
    let t1 = std::thread::spawn(move || {
        b.wait();
        a.transfer_owner_batch_for_job(source, forward, None, None)
    });
    let a = reverse.clone();
    let b = barrier.clone();
    let routes = reverse_routes.clone();
    let t2 = std::thread::spawn(move || {
        b.wait();
        a.transfer_owner_batch_for_job(peer_source, routes, None, Some(peer_source.id))
    });
    barrier.wait();
    if t1.join().unwrap().is_err() {
        f.cluster
            .transfer_owner_batch_for_job(f.source, f.routes.clone(), None, None)
            .unwrap();
    }
    if t2.join().unwrap().is_err() {
        reverse
            .transfer_owner_batch_for_job(peer_source, reverse_routes, None, Some(peer_source.id))
            .unwrap();
    }
    let mut all = BTreeSet::new();
    for o in &f.owners {
        o.ensure_healthy().unwrap();
        assert_eq!(o.regions.read().state_snapshot()["active_grants"], 0);
        let EngineResponse::Snapshot(nodes) = o.handle(EngineRequest::Snapshot).unwrap() else {
            panic!()
        };
        for n in nodes {
            let p = n.postings.unwrap();
            for id in &p.ids {
                assert!(all.insert(*id));
            }
            assert_eq!(
                p.codes,
                p.ids
                    .iter()
                    .flat_map(
                        |id| Code::<1>::quantize(&f.vectors[id], &n.routing.centroid)
                            .as_ref()
                            .to_vec()
                    )
                    .collect::<Vec<_>>()
            );
        }
    }
    assert_eq!(all, f.vectors.keys().copied().collect());
    assert!(f.owners[0]
        .resident(f.source)
        .unwrap()
        .read()
        .postings
        .as_ref()
        .unwrap()
        .ids
        .contains(&3000));
    server1.shutdown();
    server2.shutdown();
}
#[test]
fn owner_batch_review_epoch_open_cannot_erase_fresh_admitted_prepare() {
    let (f, mut server) = connected();
    let (mut e, p) = payload(&f);
    e.epoch = 1;
    let owner = f.owners[0].clone();
    owner
        .handle(EngineRequest::CloseEpoch { run: 27, epoch: 0 })
        .unwrap();
    owner
        .handle(EngineRequest::SealEpoch { run: 27, epoch: 0 })
        .unwrap();
    let (visible_tx, visible_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let resume = Mutex::new(resume_rx);
    *owner.epoch_open_hook.lock() = Some(Arc::new(move |phase_available| {
        visible_tx.send(phase_available).unwrap();
        resume.lock().recv_timeout(Duration::from_secs(2)).unwrap();
    }));
    let opening = owner.clone();
    let open =
        std::thread::spawn(move || opening.handle(EngineRequest::OpenEpoch { run: 27, epoch: 1 }));
    let available = visible_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (accepted_tx, accepted_rx) = std::sync::mpsc::channel();
    let admitting = owner.clone();
    let request = BatchRequest::Prepare {
        envelope: e.clone(),
        payload: p,
        role: Role::Source,
    };
    let prepare = std::thread::spawn(move || {
        let r = admitting.handle_owner_transfer(request);
        accepted_tx.send(r.clone()).unwrap();
        r
    });
    // The unsafe order admits the new record before clearing; force that exact
    // interleaving when phase admission is exposed. The fixed order holds phase
    // admission until the old map is gone and the new epoch is published.
    let early = if available {
        Some(accepted_rx.recv_timeout(Duration::from_secs(2)).unwrap())
    } else {
        None
    };
    resume_tx.send(()).unwrap();
    open.join().unwrap().unwrap();
    let outcome =
        early.unwrap_or_else(|| accepted_rx.recv_timeout(Duration::from_secs(2)).unwrap());
    assert_eq!(outcome.unwrap(), TransferState::Prepared);
    prepare.join().unwrap().unwrap();
    assert_eq!(
        owner
            .handle_owner_transfer(BatchRequest::Status {
                envelope: e.clone(),
                role: Role::Source
            })
            .unwrap(),
        TransferState::Prepared,
        "opening must not erase a freshly admitted endpoint"
    );
    owner
        .handle_owner_transfer(BatchRequest::Step {
            envelope: e,
            role: Role::Source,
            step: TransferStep::Abort,
            proof: TransferProof {
                abort_decided: true,
                ..Default::default()
            },
        })
        .unwrap();
    *owner.epoch_open_hook.lock() = None;
    owner
        .handle(EngineRequest::CloseEpoch { run: 27, epoch: 1 })
        .unwrap();
    owner
        .handle(EngineRequest::SealEpoch { run: 27, epoch: 1 })
        .unwrap();
    owner.ensure_save_quiescent(27, 1).unwrap();
    server.shutdown();
}

fn large_source_fixture() -> (FixedTransferFixture, RpcServer) {
    let mut data = (*vectors()).clone();
    for id in 10000..16000u64 {
        data.insert(id, vec![0.; 768].into());
    }
    let data = Arc::new(data);
    let mut f = fixed_transfer_fixture(true, data, BTreeMap::new());
    let peer = f.owners[1].clone();
    let server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "chunk-owner-batch".into(),
        TransportConfig::default(),
        Arc::new(move |r: EngineRequest| peer.handle(r)),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "chunk-owner-batch".into(),
        TransportConfig::default(),
    )
    .unwrap();
    let mut raw = Cluster::new(
        f.owners[0].clone(),
        BTreeMap::from([(2, client)]),
        f.cluster.current_view().value.root.clone(),
    );
    raw.publication = super::super::protocol::publication::Publication::new(
        f.cluster.current_view().value.clone(),
        1_000_000,
    );
    f.cluster = Arc::new(raw);
    f.owners[0].bind_cluster(&f.cluster);
    {
        let h = f.owners[0].resident(f.source).unwrap();
        let mut n = h.write();
        let centroid = n.routing.centroid.clone();
        let p = n.postings.as_mut().unwrap();
        for id in 10000..16000u64 {
            p.ids.push(id);
            p.codes
                .extend_from_slice(Code::<1>::quantize(&f.vectors[&id], &centroid).as_ref());
        }
    }

    (f, server)
}

#[test]
fn owner_batch_review_later_chunk_rejection_keeps_committed_balance_obligations() {
    let (f, mut server2) = large_source_fixture();
    let owner1 = f.owners[0].clone();
    let mut server1 = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "chunk-coordinator".into(),
        TransportConfig::default(),
        Arc::new(move |r: EngineRequest| owner1.handle(r)),
    )
    .unwrap();
    let client = RpcClient::new(
        server1.data_addr(),
        server1.control_addr(),
        "chunk-coordinator".into(),
        TransportConfig::default(),
    )
    .unwrap();
    let reverse = Arc::new(Cluster::new(
        f.owners[1].clone(),
        BTreeMap::from([(1, client)]),
        f.cluster.current_view().value.root.clone(),
    ));
    reverse.install_view(&f.cluster.current_view()).unwrap();
    f.owners[1].bind_cluster(&reverse);
    let targets = f.routes.keys().copied().take(2).collect::<Vec<_>>();
    let first = targets[0];
    let second = targets[1];
    let all_ids = (0..3).chain(10000..16000).collect::<Vec<u64>>();
    let cut = MAX_CODES / f.owners[0].code_size();
    let routes = BTreeMap::from([
        (first, all_ids[..cut].to_vec()),
        (second, all_ids[cut..].to_vec()),
    ]);
    let held = Arc::new(Mutex::new(None));
    let hold = held.clone();
    let peer = f.owners[1].clone();
    *f.owners[0].batch_chunk_hook.lock() = Some(Arc::new(move |completed| {
        if completed == 1 {
            *hold.lock() = peer.claim_source_leaf_for_job(second, None).unwrap();
            assert!(hold.lock().is_some());
        }
    }));
    let error = f
        .cluster
        .transfer_owner_batch_for_construction(f.source, routes, None, None)
        .unwrap_err();
    assert!(error.contains("protected"), "{error}");
    *f.owners[0].batch_chunk_hook.lock() = None;
    let queued = f.cluster.jobs.lock().queued_balance_nodes();
    assert!(
        queued.contains(&first),
        "committed destination must retain its balancing job before a later chunk rejects"
    );
    assert!(queued.contains(&f.source));
    assert_eq!(
        f.owners[1]
            .resident(first)
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids
            .len(),
        cut + 1
    );
    drop(held.lock().take());
    f.cluster
        .transfer_owner_batch_for_construction(
            f.source,
            BTreeMap::from([(second, all_ids[cut..].to_vec())]),
            None,
            None,
        )
        .unwrap();
    f.cluster.balance(4).unwrap();
    assert!(f.cluster.jobs.lock().is_drained());
    let mut all = BTreeSet::new();
    for o in &f.owners {
        o.ensure_healthy().unwrap();
        let EngineResponse::Snapshot(nodes) = o.handle(EngineRequest::Snapshot).unwrap() else {
            panic!()
        };
        for n in nodes {
            if let Some(p) = n.postings {
                assert!(p.ids.len() <= o.config.split_threshold);
                for id in &p.ids {
                    assert!(all.insert(*id));
                }
                assert_eq!(
                    p.codes,
                    p.ids
                        .iter()
                        .flat_map(
                            |id| Code::<1>::quantize(&f.vectors[id], &n.routing.centroid)
                                .as_ref()
                                .to_vec()
                        )
                        .collect::<Vec<_>>()
                );
            }
        }
    }
    assert_eq!(all, f.vectors.keys().copied().collect());
    server1.shutdown();
    server2.shutdown();
}

#[test]
fn owner_batch_lost_successful_prepare_ack_fails_closed_without_guessing_rejection() {
    let mut f = fixed_transfer_fixture(true, vectors(), BTreeMap::new());
    let peer = f.owners[1].clone();
    let once = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let armed = once.clone();
    let mut server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "lost-prepare-batch".into(),
        TransportConfig::default(),
        Arc::new(move |r: EngineRequest| {
            let drop = matches!(
                &r,
                EngineRequest::OwnerTransfer(BatchRequest::Prepare {
                    role: Role::Destination,
                    ..
                })
            ) && armed.swap(false, Ordering::SeqCst);
            peer.handle(r).map(|response| DropAck { response, drop })
        }),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "lost-prepare-batch".into(),
        TransportConfig::default(),
    )
    .unwrap();
    let mut raw = Cluster::new(
        f.owners[0].clone(),
        BTreeMap::from([(2, client)]),
        f.cluster.current_view().value.root.clone(),
    );
    raw.publication = super::super::protocol::publication::Publication::new(
        f.cluster.current_view().value.clone(),
        1_000_000,
    );
    f.cluster = Arc::new(raw);
    f.owners[0].bind_cluster(&f.cluster);
    let error = f
        .cluster
        .transfer_owner_batch_for_job(f.source, f.routes.clone(), None, None)
        .unwrap_err();
    assert!(error.starts_with("transport "), "{error}");
    assert!(!once.load(Ordering::SeqCst));
    assert!(f.owners[0].ensure_healthy().is_err());
    assert_eq!(
        f.owners[0]
            .resident(f.source)
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids
            .len(),
        2048
    );
    assert!(f.owners[0]
        .resident(f.source)
        .unwrap()
        .read()
        .reserved
        .is_none());
    assert!(f.owners[1]
        .resident(*f.routes.keys().next().unwrap())
        .unwrap()
        .read()
        .reserved
        .is_some());
    assert_eq!(
        f.owners[0]
            .observability
            .snapshot()
            .counters
            .get("TransferCompactions")
            .copied()
            .unwrap_or(0),
        0
    );
    f.owners[1]
        .handle(EngineRequest::CloseEpoch { run: 27, epoch: 0 })
        .unwrap();
    assert!(f.owners[1]
        .handle(EngineRequest::SealEpoch { run: 27, epoch: 0 })
        .is_err());
    assert!(f.owners[1].ensure_save_quiescent(27, 0).is_err());
    server.shutdown();
}

#[test]
fn owner_batch_late_epoch_opener_cannot_regress_or_clear_current_records() {
    let (f, mut server) = connected();
    let owner = f.owners[0].clone();
    owner
        .handle(EngineRequest::CloseEpoch { run: 27, epoch: 0 })
        .unwrap();
    owner
        .handle(EngineRequest::SealEpoch { run: 27, epoch: 0 })
        .unwrap();
    let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let resume = Mutex::new(resume_rx);
    *owner.epoch_open_wait_hook.lock() = Some(Arc::new(move || {
        waiting_tx.send(()).unwrap();
        resume.lock().recv_timeout(Duration::from_secs(2)).unwrap();
    }));
    let delayed = owner.clone();
    let old =
        std::thread::spawn(move || delayed.handle(EngineRequest::OpenEpoch { run: 27, epoch: 1 }));
    waiting_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    *owner.epoch_open_wait_hook.lock() = None;
    owner
        .handle(EngineRequest::OpenEpoch { run: 27, epoch: 2 })
        .unwrap();
    let (mut e, p) = payload(&f);
    e.epoch = 2;
    owner
        .handle_owner_transfer(BatchRequest::Prepare {
            envelope: e.clone(),
            payload: p,
            role: Role::Source,
        })
        .unwrap();
    owner
        .handle_owner_transfer(BatchRequest::Step {
            envelope: e.clone(),
            role: Role::Source,
            step: TransferStep::Abort,
            proof: TransferProof {
                abort_decided: true,
                ..Default::default()
            },
        })
        .unwrap();
    owner
        .handle(EngineRequest::CloseEpoch { run: 27, epoch: 2 })
        .unwrap();
    owner
        .handle(EngineRequest::SealEpoch { run: 27, epoch: 2 })
        .unwrap();
    resume_tx.send(()).unwrap();
    assert!(old
        .join()
        .unwrap()
        .unwrap_err()
        .contains("stale phase opening"));
    assert!(owner.batch_transfers.lock().contains_key(&e.attempt));
    // An opener above the current epoch remains valid, proving monotonicity.
    owner
        .handle(EngineRequest::OpenEpoch { run: 27, epoch: 3 })
        .unwrap();
    server.shutdown();
}
