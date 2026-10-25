//! Endpoint regressions exercise actual owner membership and routing state.
use super::*;
use std::sync::mpsc;

fn service() -> Arc<OwnerService> {
    Arc::new(OwnerService::new(
        1,
        77,
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
        Arc::new(|id: u64| -> Result<Arc<[f32]>, String> { Ok(vec![id as f32; 8].into()) }),
    ))
}
fn leaf(service: &OwnerService, id: u128, ids: Vec<u64>, revision: u64) -> StoredNode {
    let centroid = vec![0.; 8];
    let mut codes = vec![];
    for id in &ids {
        codes.extend_from_slice(
            Code::<1>::quantize(&service.vectors.get(*id).unwrap(), &centroid).as_ref(),
        );
    }
    StoredNode {
        routing: RoutingNode {
            navigation: Default::default(),
            target: NodeRef {
                id,
                authority: 1,
                placement: 0,
            },
            parent: None,
            centroid_code: Code::<4>::quantize(&centroid, &centroid).as_ref().to_vec(),
            centroid,
            children: vec![],
            leaf: true,
            revision,
        },
        postings: Some(PostingList { ids, codes }),
    }
}
#[test]
fn append_duplicate_reports_only_committed_membership() {
    let owner = service();
    let node = leaf(&owner, 10, vec![], 0);
    let target = node.routing.target;
    owner.install_initial(vec![node]).unwrap();
    let envelope = owner.envelope("append", vec![(target.id, 0)]).unwrap();
    assert!(matches!(
        owner.append(envelope.clone(), target, 0, vec![7]).unwrap(),
        EngineResponse::Ack
    ));
    assert!(matches!(
        owner.append(envelope, target, 0, vec![7]).unwrap(),
        EngineResponse::Ack
    ));
    let resident = owner.nodes.get(&10).unwrap().value().clone();
    assert_eq!(resident.read().postings.as_ref().unwrap().ids, vec![7]);
    assert_eq!(owner.node(target).unwrap().revision, 1);
}
#[test]
fn rejected_append_never_turns_into_duplicate_success_or_mutates_on_overflow() {
    for revision in [0, u64::MAX] {
        let owner = service();
        let node = leaf(&owner, 10, vec![7], revision);
        let target = node.routing.target;
        owner.install_initial(vec![node]).unwrap();
        let envelope = owner.envelope("append", vec![(target.id, 0)]).unwrap();
        let ids = if revision == 0 { vec![7] } else { vec![8] };
        assert!(owner
            .append(envelope.clone(), target, revision, ids.clone())
            .is_err());
        assert!(owner
            .append(envelope.clone(), target, revision, ids)
            .is_err());
        assert_eq!(
            owner.operations.lock().phase.lookup(&envelope),
            Err(protocol::ProtocolError::Unknown)
        );
        let resident = owner.nodes.get(&10).unwrap().value().clone();
        assert_eq!(resident.read().postings.as_ref().unwrap().ids, vec![7]);
        assert_eq!(owner.node(target).unwrap().revision, revision);
    }
}
#[test]
fn transfer_prepare_retry_observes_applied_and_released_state() {
    let owner = service();
    let source = leaf(&owner, 10, vec![7], 0);
    let dest = leaf(&owner, 20, vec![], 0);
    let src = source.routing.target;
    let dst = dest.routing.target;
    let codes = Code::<1>::quantize(&owner.vectors.get(7).unwrap(), &dest.routing.centroid)
        .as_ref()
        .to_vec();
    owner.install_initial(vec![source, dest]).unwrap();
    let envelope = owner
        .envelope("posting-transfer", vec![(src.id, 0), (dst.id, 0)])
        .unwrap();
    let payload = TransferPayload {
        source_owner: 1,
        destination_owner: 1,
        source_leaf: src.id,
        destination_leaf: dst.id,
        source_placement: 0,
        destination_placement: 0,
        source_revision: 0,
        destination_revision: 0,
        source_centroid_digest: centroid_digest(&[0.; 8]),
        destination_centroid_digest: centroid_digest(&[0.; 8]),
        vectors: vec![7],
        content_digest: [0; 32],
    };
    owner
        .transfer_prepare(envelope.clone(), payload.clone(), Role::Source, vec![])
        .unwrap();
    owner
        .transfer_prepare(
            envelope.clone(),
            payload.clone(),
            Role::Destination,
            codes.clone(),
        )
        .unwrap();
    let mut driver = TransferDriver::default();
    let perform = |role, step, driver: &mut TransferDriver| {
        owner
            .transfer_step(envelope.clone(), role, step, driver.proof())
            .unwrap();
        driver.acknowledge(role, step).unwrap();
    };
    perform(Role::Destination, TransferStep::Stage, &mut driver);
    driver.decide(DriverDecision::Complete).unwrap();
    for role in [Role::Source, Role::Destination] {
        perform(role, TransferStep::BeginCompletion, &mut driver);
    }
    perform(Role::Source, TransferStep::RemoveSource, &mut driver);
    assert!(matches!(
        owner
            .transfer_prepare(envelope.clone(), payload.clone(), Role::Source, vec![])
            .unwrap(),
        EngineResponse::Transfer(TransferState::Applied)
    ));
    perform(
        Role::Destination,
        TransferStep::ActivateDestination,
        &mut driver,
    );
    for role in [Role::Source, Role::Destination] {
        perform(role, TransferStep::Complete, &mut driver);
    }
    for role in [Role::Destination, Role::Source] {
        perform(role, TransferStep::Release, &mut driver);
    }
    assert!(matches!(
        owner
            .transfer_prepare(envelope.clone(), payload, Role::Source, vec![])
            .unwrap(),
        EngineResponse::Transfer(TransferState::Released)
    ));
    assert_eq!(
        owner.regions.read().status(envelope.attempt).unwrap(),
        super::super::protocol::reservation::ReservationStatus::Released
    );
    assert_eq!(owner.node(src).unwrap().revision, 1);
    assert_eq!(owner.node(dst).unwrap().revision, 1);
    let work = owner.observability.snapshot();
    assert_eq!(work.counters["TransferCompactions"], 1);
    assert_eq!(work.counters["TransferSourceEntriesScanned"], 1);
    assert_eq!(work.counters["TransferCopiedIdBytes"], 8);
    assert_eq!(
        work.counters["TransferCopiedCodeBytes"],
        owner.code_size() as u64
    );
    assert!(work.counters["TransferMembershipProbes"] > 0);
    assert!(
        work.counters["TransferMembershipComparisonUpperBound"]
            >= work.counters["TransferMembershipProbes"] - 2
    );
}
#[test]
fn routing_remains_readable_while_posting_records_are_exclusively_locked() {
    let owner = service();
    let mut branch = leaf(&owner, 10, vec![], 0);
    let mut posting = leaf(&owner, 20, vec![7], 0);
    posting.routing.parent = Some(branch.routing.target);
    branch.routing.leaf = false;
    branch.routing.children = vec![Child {
        target: posting.routing.target,
        centroid: posting.routing.centroid.clone(),
        leaf: true,
    }];
    branch.postings = None;
    let root = branch.routing.target;
    let leaf = posting.routing.target;
    owner.install_initial(vec![branch, posting]).unwrap();
    let root_record = owner.nodes.get(&root.id).unwrap().value().clone();
    let leaf_record = owner.nodes.get(&leaf.id).unwrap().value().clone();
    let root_guard = root_record.write();
    let leaf_guard = leaf_record.write();
    let (sender, receiver) = mpsc::channel();
    let remote = owner.clone();
    let worker = std::thread::spawn(move || {
        let image = remote.node(leaf);
        let expansion = remote.expand(ExpandRequest {
            navigation: 1,
            step: 0,
            query_slot: 0,
            query: vec![7.; 8],
            nodes: vec![root],
            run: 77,
            epoch: 0,
            shared_sequence: 0,
        });
        sender.send((image, expansion)).unwrap();
    });
    let result = receiver.recv_timeout(Duration::from_secs(2));
    drop(leaf_guard);
    drop(root_guard);
    worker.join().unwrap();
    let (image, expansion) = result.expect("routing waited for a posting lock");
    assert_eq!(image.unwrap().target, leaf);
    let expansion = expansion.unwrap();
    assert_eq!(expansion.status, ExpansionStatus::Complete);
    assert_eq!(expansion.children.len(), 1);
}

#[test]
fn stale_transfer_centroid_dependency_cannot_stage_or_turn_into_success() {
    let owner = service();
    let source = leaf(&owner, 10, vec![7], 0);
    let mut destination = leaf(&owner, 20, vec![], 1);
    destination.routing.centroid = vec![1.; 8];
    destination.routing.centroid_code =
        Code::<4>::quantize(&destination.routing.centroid, &vec![0.; 8])
            .as_ref()
            .to_vec();
    let codes = Code::<1>::quantize(
        &owner.vectors.get(7).unwrap(),
        &destination.routing.centroid,
    )
    .as_ref()
    .to_vec();
    owner.install_initial(vec![source, destination]).unwrap();
    let envelope = owner
        .envelope("posting-transfer", vec![(10, 0), (20, 0)])
        .unwrap();
    let payload = TransferPayload {
        source_owner: 1,
        destination_owner: 1,
        source_leaf: 10,
        destination_leaf: 20,
        source_placement: 0,
        destination_placement: 0,
        source_revision: 0,
        destination_revision: 0,
        source_centroid_digest: centroid_digest(&[0.; 8]),
        destination_centroid_digest: centroid_digest(&[0.; 8]),
        vectors: vec![7],
        content_digest: [0; 32],
    };
    assert!(owner
        .transfer_prepare(
            envelope.clone(),
            payload.clone(),
            Role::Destination,
            codes.clone()
        )
        .is_err());
    assert!(owner
        .transfer_prepare(envelope.clone(), payload, Role::Destination, codes)
        .is_err());
    assert_eq!(
        owner.operations.lock().phase.lookup(&envelope),
        Err(protocol::ProtocolError::Unknown)
    );
    assert_eq!(
        owner.regions.read().status(envelope.attempt).unwrap(),
        super::super::protocol::reservation::ReservationStatus::Released
    );
    assert!(owner.nodes.get(&20).unwrap().read().reserved.is_none());
}

#[test]
fn navigation_pins_published_root_and_sequence_instead_of_separate_mirrors() {
    let owner = service();
    let mut root = leaf(&owner, 10, vec![], 0);
    let mut posting = leaf(&owner, 20, vec![7], 0);
    posting.routing.parent = Some(root.routing.target);
    root.routing.leaf = false;
    root.postings = None;
    root.routing.children = vec![Child {
        target: posting.routing.target,
        centroid: posting.routing.centroid.clone(),
        leaf: true,
    }];
    let published = root.routing.clone();
    let expected = posting.routing.target;
    owner.install_initial(vec![root, posting]).unwrap();
    let cluster = Cluster::new(owner, BTreeMap::new(), published);
    // These deliberately incompatible mirrors cannot influence routing. A real
    // installer updates them separately after swapping its compatible view.
    cluster.root.write().target.id = 999;
    cluster.root.write().leaf = true;
    cluster.shared_sequence.store(999, Ordering::Release);
    let selected = cluster
        .navigate(&[vec![7.; 8]], &ReadBeamPolicy::uniform(None, 1, 1), None)
        .unwrap();
    assert_eq!(selected[0][0].0, expected);
}

#[test]
fn installed_view_rejects_equal_sequence_conflicts_and_never_regresses() {
    let owner = service();
    let root = leaf(&owner, 10, vec![], 0).routing;
    let mut cluster = Cluster::new(owner, BTreeMap::new(), root);
    cluster.coordinator = 2;
    let mut first = (*cluster.publication.view()).clone();
    first.sequence = 1;
    cluster.install_view(&first).unwrap();
    let mut conflicting = first.clone();
    conflicting.value.root.centroid[0] = 1.;
    assert!(cluster.install_view(&conflicting).is_err());
    assert_eq!(*cluster.current_view(), first);
    let cluster = Arc::new(cluster);
    for top in 2..18 {
        let mut newer = first.clone();
        newer.sequence = top;
        let mut older = first.clone();
        older.sequence = top - 1;
        let start = Arc::new(std::sync::Barrier::new(3));
        let a = cluster.clone();
        let a_start = start.clone();
        let b = cluster.clone();
        let b_start = start.clone();
        let writer_a = std::thread::spawn(move || {
            a_start.wait();
            a.install_view(&newer)
        });
        let writer_b = std::thread::spawn(move || {
            b_start.wait();
            b.install_view(&older)
        });
        start.wait();
        writer_a.join().unwrap().unwrap();
        let _ = writer_b.join().unwrap();
        assert_eq!(cluster.current_view().sequence, top);
    }
}

#[test]
fn an_admitted_construction_job_blocks_phase_close_and_terminal_replay_runs_once() {
    let owner = service();
    let root = owner.initial_root();
    owner.install_initial(vec![root.clone()]).unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), root.routing));
    owner.bind_cluster(&cluster);
    let e = owner
        .envelope("controlled-construction-job", vec![])
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let running = owner.clone();
    let job = e.clone();
    let task = std::thread::spawn(move || {
        running.execute_job(&job, vec![1], || {
            started_tx.send(()).unwrap();
            finish_rx.recv().unwrap();
            Ok(())
        })
    });
    started_rx.recv().unwrap();
    assert!(owner
        .handle(EngineRequest::CloseEpoch { run: 77, epoch: 0 })
        .is_err());
    assert!(owner
        .handle(EngineRequest::SealEpoch { run: 77, epoch: 0 })
        .is_err());
    finish_tx.send(()).unwrap();
    task.join().unwrap().unwrap();
    owner
        .execute_job(&e, vec![1], || {
            panic!("terminal duplicate executes only once")
        })
        .unwrap();
    assert!(owner.execute_job(&e, vec![2], || Ok(())).is_err());
    owner
        .handle(EngineRequest::CloseEpoch { run: 77, epoch: 0 })
        .unwrap();
    owner
        .handle(EngineRequest::SealEpoch { run: 77, epoch: 0 })
        .unwrap();
    assert!(owner.execute_job(&e, vec![1], || Ok(())).is_err());
}

#[test]
fn a_real_balance_job_fences_phase_close_while_clustering_reads_vectors() {
    use super::super::construction::BalanceJob;
    let gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = gate.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Mutex::new(release_rx);
    let vectors: Arc<dyn VectorSource> = Arc::new(move |id: u64| -> Result<Arc<[f32]>, String> {
        if flag.swap(false, Ordering::AcqRel) {
            started_tx.send(()).unwrap();
            release.lock().recv().unwrap();
        }
        Ok(vec![id as f32; 8].into())
    });
    let owner = Arc::new(OwnerService::new(
        1,
        77,
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig {
            split_threshold: 8,
            merge_threshold: 2,
            branching_factor: 4,
            ..Default::default()
        },
        vectors,
    ));
    let root = leaf(&owner, 1u128 << 80, (0..11).collect(), 0);
    let target = root.routing.target;
    let image = root.routing.clone();
    owner.install_initial(vec![root]).unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), image));
    owner.bind_cluster(&cluster);
    let job = BalanceJob {
        envelope: owner
            .envelope("balance-job", vec![(target.id, target.placement)])
            .unwrap(),
        node: target,
        depth: 0,
        internal: false,
    };
    gate.store(true, Ordering::Release);
    let service = owner.clone();
    let active = job.clone();
    let task = std::thread::spawn(move || service.handle(EngineRequest::BalanceJob(active)));
    started_rx.recv().unwrap();
    assert!(owner
        .handle(EngineRequest::CloseEpoch { run: 77, epoch: 0 })
        .is_err());
    assert!(owner
        .handle(EngineRequest::SealEpoch { run: 77, epoch: 0 })
        .is_err());
    release_tx.send(()).unwrap();
    task.join().unwrap().unwrap();
    assert_ne!(
        cluster.current_view().value.root.target,
        target,
        "the actual balance operation replaces the root"
    );
    owner
        .handle(EngineRequest::BalanceJob(job.clone()))
        .unwrap();
    let mut changed = job;
    changed.depth = 1;
    assert!(owner.handle(EngineRequest::BalanceJob(changed)).is_err());
    cluster.balance(1).unwrap();
    owner
        .handle(EngineRequest::CloseEpoch { run: 77, epoch: 0 })
        .unwrap();
    owner
        .handle(EngineRequest::SealEpoch { run: 77, epoch: 0 })
        .unwrap();
}

#[test]
fn unrelated_posting_appends_do_not_invalidate_centroid_bound_transfer_codes() {
    let owner = service();
    let source = leaf(&owner, 10, vec![7], 0);
    let destination = leaf(&owner, 20, vec![], 0);
    let src = source.routing.target;
    let dst = destination.routing.target;
    let codes = Code::<1>::quantize(
        &owner.vectors.get(7).unwrap(),
        &destination.routing.centroid,
    )
    .as_ref()
    .to_vec();
    let payload = TransferPayload {
        source_owner: 1,
        destination_owner: 1,
        source_leaf: 10,
        destination_leaf: 20,
        source_placement: 0,
        destination_placement: 0,
        source_revision: 0,
        destination_revision: 0,
        source_centroid_digest: centroid_digest(&[0.; 8]),
        destination_centroid_digest: centroid_digest(&[0.; 8]),
        vectors: vec![7],
        content_digest: [0; 32],
    };
    owner.install_initial(vec![source, destination]).unwrap();
    for (leaf, id) in [(src, 8), (dst, 9)] {
        let envelope = owner.envelope("append", vec![(leaf.id, 0)]).unwrap();
        owner.append(envelope, leaf, 0, vec![id]).unwrap();
    }
    assert_eq!(owner.node(src).unwrap().revision, 1);
    assert_eq!(owner.node(dst).unwrap().revision, 1);
    let e = owner
        .envelope("posting-transfer", vec![(src.id, 0), (dst.id, 0)])
        .unwrap();
    owner
        .transfer_prepare(e.clone(), payload.clone(), Role::Source, vec![])
        .expect("an unrelated source posting leaves the centroid dependency unchanged");
    owner
        .transfer_prepare(e.clone(), payload, Role::Destination, codes)
        .expect("an unrelated destination posting leaves the prepared codes valid");
    for role in [Role::Destination, Role::Source] {
        owner
            .transfer_step(
                e.clone(),
                role,
                TransferStep::Abort,
                TransferProof {
                    abort_decided: true,
                    ..Default::default()
                },
            )
            .unwrap();
    }
    assert_eq!(
        owner
            .resident(src)
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids,
        vec![7, 8]
    );
    assert_eq!(
        owner
            .resident(dst)
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids,
        vec![9]
    );
}

#[test]
fn a_waiting_transfer_cancels_unedited_protection_when_the_owner_fails() {
    use super::super::protocol::reservation::{RegionRequest, ReservationStatus};
    let owner = service();
    owner
        .install_initial(vec![
            leaf(&owner, 10, vec![7], 0),
            leaf(&owner, 20, vec![], 0),
        ])
        .unwrap();
    let manager = owner.regions.read().clone();
    let held = owner.attempt().unwrap();
    assert_eq!(
        manager
            .request(
                held,
                RegionRequest {
                    roots: vec![10, 20],
                    topology_sequence: manager.sequence(),
                    placements: vec![(10, 0), (20, 0)]
                }
            )
            .unwrap(),
        ReservationStatus::Granted
    );
    let payload = TransferPayload {
        source_owner: 1,
        destination_owner: 1,
        source_leaf: 10,
        destination_leaf: 20,
        source_placement: 0,
        destination_placement: 0,
        source_revision: 0,
        destination_revision: 0,
        source_centroid_digest: centroid_digest(&[0.; 8]),
        destination_centroid_digest: centroid_digest(&[0.; 8]),
        vectors: vec![7],
        content_digest: [0; 32],
    };
    let envelope = owner
        .envelope("posting-transfer", vec![(10, 0), (20, 0)])
        .unwrap();
    let attempt = envelope.attempt;
    let service = owner.clone();
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send(service.transfer_prepare(envelope, payload, Role::Source, vec![]))
            .unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    while manager.status(attempt).ok() != Some(ReservationStatus::Queued) {
        assert!(
            Instant::now() < deadline,
            "the transfer must reach its actual reservation wait"
        );
        std::thread::yield_now();
    }
    *owner.poison_reason.lock() =
        Some("injected construction failure after unrelated dependency changed".into());
    owner.poisoned.store(true, Ordering::Release);
    let stopped = rx.recv_timeout(Duration::from_secs(2));
    manager.release(held).unwrap();
    worker.join().unwrap();
    assert!(stopped
        .expect("failed owner must not wait for the 60-second reservation deadline")
        .unwrap_err()
        .contains("unresolved"));
    assert_eq!(
        manager.status(attempt).unwrap(),
        ReservationStatus::Cancelled
    );
    assert!(owner
        .resident(NodeRef {
            id: 10,
            authority: 1,
            placement: 0
        })
        .unwrap()
        .read()
        .reserved
        .is_none());
}

#[test]
fn centroid_bound_transfers_reject_changed_centroids_placements_kinds_and_membership() {
    for role in [Role::Source, Role::Destination] {
        for changed in ["centroid", "placement", "kind", "membership"] {
            let owner = service();
            let mut source = leaf(&owner, 10, vec![7], 0);
            let mut destination = leaf(&owner, 20, vec![], 0);
            let target = if role == Role::Source {
                &mut source
            } else {
                &mut destination
            };
            match changed {
                "centroid" => {
                    target.routing.centroid = vec![1.; 8];
                    target.routing.centroid_code =
                        Code::<4>::quantize(&target.routing.centroid, &vec![0.; 8])
                            .as_ref()
                            .to_vec();
                    if let Some(p) = target.postings.as_mut() {
                        p.codes.clear();
                        for id in &p.ids {
                            p.codes.extend_from_slice(
                                Code::<1>::quantize(
                                    &owner.vector(*id).unwrap(),
                                    &target.routing.centroid,
                                )
                                .as_ref(),
                            );
                        }
                    }
                }
                "placement" => target.routing.target.placement = 1,
                "kind" => {
                    target.routing.leaf = false;
                    target.postings = None;
                }
                "membership" => {
                    let ids = if role == Role::Source {
                        vec![8]
                    } else {
                        vec![7]
                    };
                    *target = leaf(&owner, target.routing.target.id, ids, 0);
                }
                _ => unreachable!(),
            }
            let expected = vec![source.clone(), destination.clone()];
            owner.install_initial(vec![source, destination]).unwrap();
            let payload = TransferPayload {
                source_owner: 1,
                destination_owner: 1,
                source_leaf: 10,
                destination_leaf: 20,
                source_placement: 0,
                destination_placement: 0,
                source_revision: 0,
                destination_revision: 0,
                source_centroid_digest: centroid_digest(&[0.; 8]),
                destination_centroid_digest: centroid_digest(&[0.; 8]),
                vectors: vec![7],
                content_digest: [0; 32],
            };
            let e = owner
                .envelope("posting-transfer", vec![(10, 0), (20, 0)])
                .unwrap();
            let codes = if role == Role::Destination {
                Code::<1>::quantize(&owner.vector(7).unwrap(), &vec![0.; 8])
                    .as_ref()
                    .to_vec()
            } else {
                vec![]
            };
            assert!(
                owner.transfer_prepare(e, payload, role, codes).is_err(),
                "changed {changed} at {role:?} must invalidate preparation"
            );
            for stored in &expected {
                let current = owner
                    .nodes
                    .get(&stored.routing.target.id)
                    .unwrap()
                    .value()
                    .clone();
                let current = current.read();
                assert!(current.reserved.is_none());
                assert_eq!(*current.routing, stored.routing);
                assert_eq!(current.postings, stored.postings);
            }
        }
    }
}

#[test]
fn packed_expansion_publishes_coherent_rows_and_aggregates_exact_prefix_counts() {
    let owner = service();
    let mut left = leaf(&owner, 10, vec![], 0);
    let mut right = leaf(&owner, 20, vec![], 0);
    right.routing.centroid.fill(2.);
    let root_ref = NodeRef {
        id: 1,
        authority: 1,
        placement: 0,
    };
    left.routing.parent = Some(root_ref);
    right.routing.parent = Some(root_ref);
    let root = RoutingNode {
        navigation: Default::default(),
        target: root_ref,
        parent: None,
        centroid: vec![1.; 8],
        centroid_code: vec![],
        children: vec![
            super::super::routing::Child {
                target: left.routing.target,
                centroid: left.routing.centroid.clone(),
                leaf: true,
            },
            super::super::routing::Child {
                target: right.routing.target,
                centroid: right.routing.centroid.clone(),
                leaf: true,
            },
        ],
        leaf: false,
        revision: 0,
    };
    let left_ref = left.routing.target;
    owner
        .install_initial(vec![
            StoredNode {
                routing: root,
                postings: None,
            },
            left,
            right,
        ])
        .unwrap();
    let request = |nodes| ExpandRequest {
        navigation: 1,
        step: 0,
        query_slot: 0,
        query: vec![0.; 8],
        nodes,
        run: owner.run,
        epoch: owner.epoch.load(Ordering::Acquire),
        shared_sequence: 0,
    };
    let first = owner.expand(request(vec![root_ref])).unwrap();
    assert_eq!(
        first
            .children
            .iter()
            .map(|c| (c.ordinal, c.target.id, c.distance))
            .collect::<Vec<_>>(),
        vec![(0, 10, 0.), (1, 20, 32.)]
    );
    assert_eq!(owner.distances.load(Ordering::Relaxed), 2);
    let old_image = owner.routing_images.read()[&root_ref.id].clone();
    let mut replacement = owner.node(root_ref).unwrap();
    replacement.children.swap(0, 1);
    replacement.children[0].centroid.fill(3.);
    replacement.revision += 1;
    owner.publish_images(vec![replacement], &[]).unwrap();
    let next = owner.expand(request(vec![root_ref])).unwrap();
    assert_eq!(
        next.children
            .iter()
            .map(|c| (c.ordinal, c.target.id, c.distance, c.observed_revision))
            .collect::<Vec<_>>(),
        vec![(0, 20, 72., 1), (1, 10, 0., 1)]
    );
    assert_eq!(owner.distances.load(Ordering::Relaxed), 4);
    assert_eq!(
        old_image
            .scored_children()
            .unwrap()
            .map(|(_, t, _, row)| (t.id, row[0]))
            .collect::<Vec<_>>(),
        vec![(10, 0.), (20, 2.)]
    );
    // A malformed batch can fail after a valid prefix. Preserve the previous
    // accounting convention while flushing successful comparisons only once.
    assert!(owner.expand(request(vec![root_ref, left_ref])).is_err());
    assert_eq!(owner.distances.load(Ordering::Relaxed), 6);
    let stale = owner
        .expand(request(vec![NodeRef {
            placement: 99,
            ..root_ref
        }]))
        .unwrap();
    assert_eq!(stale.status, ExpansionStatus::Stale);
    assert_eq!(owner.distances.load(Ordering::Relaxed), 6);
}

fn scheduler_fixture() -> (Arc<OwnerService>, Arc<Cluster>, NodeRef) {
    let owner = service();
    let root = leaf(&owner, 10, vec![], 0);
    let target = root.routing.target;
    let image = root.routing.clone();
    owner.install_initial(vec![root]).unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), image));
    owner.bind_cluster(&cluster);
    (owner, cluster, target)
}
fn scheduler_enqueue(owner: &OwnerService, node: NodeRef, kind: usize) -> EngineRequest {
    use super::super::construction::{BalanceJob, NeighborJob, ReassignJob};
    let envelope = owner
        .envelope(
            "scheduler-endpoint-fixture",
            vec![(node.id, node.placement)],
        )
        .unwrap();
    match kind {
        0 => EngineRequest::Enqueue(BalanceJob {
            envelope,
            node,
            depth: 0,
            internal: false,
        }),
        1 => EngineRequest::EnqueueNeighbor(NeighborJob {
            envelope,
            node,
            old: vec![0.; 8],
            left: vec![0.; 8],
            right: vec![0.; 8],
            depth: 0,
        }),
        2 => EngineRequest::EnqueueReassign(ReassignJob {
            envelope,
            source: node,
            ids: vec![7],
            excluded: None,
            blocked: vec![],
            blocked_jobs: vec![],
            depth: 0,
        }),
        _ => panic!("enqueue kind"),
    }
}

#[test]
fn scheduler_queue_wait_never_holds_unrelated_mutation_admission_or_terminal() {
    let mut progress = vec![];
    for kind in 0..4 {
        let (owner, cluster, target) = scheduler_fixture();
        let request = if kind == 3 {
            EngineRequest::CloseEpoch {
                run: owner.run,
                epoch: 0,
            }
        } else {
            scheduler_enqueue(&owner, target, kind)
        };
        let (at_queue_tx, at_queue_rx) = mpsc::channel();
        *owner.scheduler_queue_wait_hook.lock() = Some(Arc::new(move || {
            at_queue_tx.send(()).unwrap();
        }));
        let queue = cluster.jobs.lock();
        let endpoint = owner.clone();
        let worker = std::thread::spawn(move || endpoint.handle(request));
        at_queue_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("handler reaches its queue acquisition");
        // The queue stays held, so the handler cannot finish. This checks lock
        // ownership at a controlled point rather than inferring it from latency.
        let operations_free = owner.operations.try_lock().is_some();
        if operations_free {
            let mutation = owner.envelope("independent-mutation", vec![]).unwrap();
            assert!(owner.admit_placement(&mutation, vec![3, 7]).unwrap());
            owner.terminal_placement(&mutation).unwrap();
        }
        drop(queue);
        assert!(matches!(
            worker.join().unwrap().unwrap(),
            EngineResponse::Ack
        ));
        progress.push((kind, operations_free));
    }
    assert!(progress.iter().all(|(_, free)| *free),
        "queue wait blocks independent admission/terminal; endpoint kinds (balance, neighbor, reassignment, close): {progress:?}");
}

fn pause_first_scheduler_wait(owner: &OwnerService) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
    let first = std::sync::atomic::AtomicBool::new(true);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Mutex::new(release_rx);
    *owner.scheduler_queue_wait_hook.lock() = Some(Arc::new(move || {
        if first.swap(false, Ordering::SeqCst) {
            entered_tx.send(()).unwrap();
            release
                .lock()
                .recv_timeout(Duration::from_secs(5))
                .expect("controlled queue waiter resumes");
        }
    }));
    (entered_rx, release_tx)
}

#[test]
fn scheduler_enqueue_and_close_preserve_both_controlled_orders() {
    for kind in 0..3 {
        // Close pauses immediately before queue acquisition; enqueue wins and
        // is acknowledged before close can inspect the pending work.
        let (owner, cluster, target) = scheduler_fixture();
        let (entered, release) = pause_first_scheduler_wait(&owner);
        let closing = owner.clone();
        let close = std::thread::spawn(move || {
            closing.handle(EngineRequest::CloseEpoch { run: 77, epoch: 0 })
        });
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let request = scheduler_enqueue(&owner, target, kind);
        assert!(matches!(
            owner.handle(request.clone()).unwrap(),
            EngineResponse::Ack
        ));
        assert!(matches!(
            owner.handle(request.clone()).unwrap(),
            EngineResponse::Ack
        ));
        release.send(()).unwrap();
        assert!(close
            .join()
            .unwrap()
            .unwrap_err()
            .contains("queue is not drained"));
        assert_eq!(
            owner.operations.lock().phase.state(),
            protocol::envelope::PhaseState::Open
        );
        // Accepted work remains executable. Only after workers drain may close
        // and seal succeed; replay cannot inject new work into either state.
        cluster.balance(1).unwrap();
        assert!(cluster.jobs.lock().is_drained());
        owner
            .handle(EngineRequest::CloseEpoch { run: 77, epoch: 0 })
            .unwrap();
        assert!(owner
            .handle(scheduler_enqueue(&owner, target, kind))
            .is_err());
        assert!(cluster.jobs.lock().is_drained());
        owner
            .handle(EngineRequest::SealEpoch { run: 77, epoch: 0 })
            .unwrap();
        assert!(owner.handle(request).is_err());

        // Enqueue pauses before acquiring the queue; close wins, so the waiter
        // must reject rather than acknowledge an insertion into a closed phase.
        let (owner, cluster, target) = scheduler_fixture();
        let (entered, release) = pause_first_scheduler_wait(&owner);
        let enqueueing = owner.clone();
        let request = scheduler_enqueue(&owner, target, kind);
        let enqueue = std::thread::spawn(move || enqueueing.handle(request));
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        owner
            .handle(EngineRequest::CloseEpoch { run: 77, epoch: 0 })
            .unwrap();
        release.send(()).unwrap();
        assert!(enqueue
            .join()
            .unwrap()
            .unwrap_err()
            .contains("closed phase"));
        assert!(cluster.jobs.lock().is_drained());
    }
}

#[test]
fn scheduler_waiters_revalidate_remote_epoch_after_queue_wait() {
    for kind in 0..4 {
        let (owner, cluster, target) = scheduler_fixture();
        let (entered, release) = pause_first_scheduler_wait(&owner);
        let mut request = if kind == 3 {
            EngineRequest::CloseEpoch { run: 77, epoch: 0 }
        } else {
            scheduler_enqueue(&owner, target, kind)
        };
        // An enqueue can arrive from another owner's authenticated transport.
        // Its original run/epoch remains binding regardless of attempt authority.
        match &mut request {
            EngineRequest::Enqueue(j) => j.envelope.attempt.authority = 2,
            EngineRequest::EnqueueNeighbor(j) => j.envelope.attempt.authority = 2,
            EngineRequest::EnqueueReassign(j) => j.envelope.attempt.authority = 2,
            _ => {}
        }
        // Exercise the actual wire representation without requiring a socket.
        let request: EngineRequest =
            serde_json::from_slice(&serde_json::to_vec(&request).unwrap()).unwrap();
        let waiting = owner.clone();
        let worker = std::thread::spawn(move || waiting.handle(request));
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        owner
            .handle(EngineRequest::CloseEpoch { run: 77, epoch: 0 })
            .unwrap();
        owner
            .handle(EngineRequest::SealEpoch { run: 77, epoch: 0 })
            .unwrap();
        owner
            .handle(EngineRequest::OpenEpoch { run: 77, epoch: 1 })
            .unwrap();
        release.send(()).unwrap();
        let error = worker.join().unwrap().unwrap_err();
        assert!(
            error.contains("WrongEpoch") || error.contains("stale phase close"),
            "{error}"
        );
        assert!(cluster.jobs.lock().is_drained());
        assert_eq!(
            owner.operations.lock().phase.state(),
            protocol::envelope::PhaseState::Open
        );
        assert_eq!(owner.epoch.load(Ordering::Acquire), 1);
    }
}

// A shared routing level followed by an owned routing level is the smallest
// tree in which a compatible publication can interrupt a multi-owner walk.
fn navigation_snapshot_fixture() -> (Arc<OwnerService>, Arc<Cluster>, NodeRef) {
    use super::super::construction::BoundaryView;
    use super::super::protocol::publication::Publication;
    let owner = service();
    let mut left = leaf(&owner, 11, vec![7], 0);
    let mut right = leaf(&owner, 12, vec![8], 0);
    right.routing.centroid.fill(2.);
    let internal_ref = NodeRef {
        id: 10,
        authority: 1,
        placement: 0,
    };
    let shared_ref = NodeRef {
        id: 1,
        authority: 1,
        placement: 0,
    };
    left.routing.parent = Some(internal_ref);
    right.routing.parent = Some(internal_ref);
    let internal = RoutingNode {
        navigation: Default::default(),
        target: internal_ref,
        parent: Some(shared_ref),
        centroid: vec![1.; 8],
        centroid_code: vec![],
        children: vec![
            Child {
                target: left.routing.target,
                centroid: left.routing.centroid.clone(),
                leaf: true,
            },
            Child {
                target: right.routing.target,
                centroid: right.routing.centroid.clone(),
                leaf: true,
            },
        ],
        leaf: false,
        revision: 0,
    };
    let shared = RoutingNode {
        navigation: Default::default(),
        target: shared_ref,
        parent: None,
        centroid: vec![1.; 8],
        centroid_code: vec![],
        children: vec![Child {
            target: internal_ref,
            centroid: internal.centroid.clone(),
            leaf: false,
        }],
        leaf: false,
        revision: 0,
    };
    owner
        .install_initial(vec![
            StoredNode {
                routing: shared.clone(),
                postings: None,
            },
            StoredNode {
                routing: internal,
                postings: None,
            },
            left,
            right,
        ])
        .unwrap();
    let mut cluster = Cluster::new(owner.clone(), BTreeMap::new(), shared.clone());
    cluster.publication = Publication::new(
        BoundaryView {
            navigation_topology: None,
            root: shared.clone(),
            shared: BTreeMap::from([(shared_ref.id, shared)]),
            units: BTreeMap::from([(internal_ref.id, internal_ref)]),
        }
        .prepared()
        .unwrap(),
        1000,
    );
    let cluster = Arc::new(cluster);
    owner.bind_cluster(&cluster);
    (owner, cluster, internal_ref)
}
fn publish_navigation_fixture_view(
    cluster: &Cluster,
    change: impl FnOnce(&mut super::super::construction::BoundaryView),
) {
    use super::super::protocol::publication::PublicationProposal;
    let before = cluster.current_view();
    let mut next = before.value.clone();
    change(&mut next);
    let envelope = cluster
        .local
        .envelope("navigation-fixture-publication", vec![])
        .unwrap();
    cluster
        .publication
        .prepare(
            envelope.attempt,
            PublicationProposal {
                expected_sequence: before.sequence,
                required_owners: BTreeSet::new(),
                value: next.prepared().unwrap(),
            },
        )
        .unwrap();
    assert!(cluster.publication.acquire_turn(envelope.attempt).unwrap());
    cluster.publication.publish(envelope.attempt).unwrap();
    cluster.publication.release_turn(envelope.attempt).unwrap();
}
#[test]
fn navigation_centroid_publications_do_not_exhaust_whole_attempt_budget() {
    let (owner, cluster, _) = navigation_snapshot_fixture();
    let weak = Arc::downgrade(&cluster);
    let publications = Arc::new(AtomicU64::new(0));
    let count = publications.clone();
    *cluster.navigation_snapshot_hook.lock() = Some(Arc::new(move || {
        let cluster = weak.upgrade().unwrap();
        count.fetch_add(1, Ordering::Relaxed);
        publish_navigation_fixture_view(&cluster, |view| {
            let shared = view.shared.get_mut(&view.root.target.id).unwrap();
            shared.children[0].centroid[0] += 0.25;
            shared.revision += 1;
            view.root = shared.clone();
        });
    }));
    let result = cluster.navigate(&[vec![0.; 8]], &ReadBeamPolicy::uniform(None, 4, 4), None);
    assert!(result.is_ok(), "compatible centroid publication must not discard a complete attempt: {result:?}; publications={}", publications.load(Ordering::Relaxed));
    let rows = result.unwrap();
    assert_eq!(
        rows[0].iter().map(|(r, _)| r.id).collect::<Vec<_>>(),
        vec![11, 12]
    );
    assert_eq!(publications.load(Ordering::Relaxed), 1);
    assert_eq!(
        owner.observability.snapshot().counters["NavigationAttempts"],
        1
    );
}

#[test]
fn navigation_topology_identity_covers_structure_placement_and_eligibility() {
    let (_, cluster, _) = navigation_snapshot_fixture();
    let original = cluster.current_view().value.clone();
    let mut centroids = original.clone();
    centroids.root.centroid.fill(3.);
    centroids.root.centroid_code = vec![99];
    centroids.root.revision += 1;
    for image in centroids.shared.values_mut() {
        image.centroid.fill(4.);
        image.revision += 1;
        for child in &mut image.children {
            child.centroid.fill(5.);
        }
    }
    assert_eq!(
        original.navigation_topology,
        centroids.prepared().unwrap().navigation_topology
    );
    let changes: Vec<Box<dyn Fn(&mut super::super::construction::BoundaryView)>> = vec![
        Box::new(|v| v.root.target.id += 100),
        Box::new(|v| v.root.target.authority += 1),
        Box::new(|v| v.root.target.placement += 1),
        Box::new(|v| v.root.leaf = true),
        Box::new(|v| v.root.parent = Some(v.root.children[0].target)),
        Box::new(|v| v.shared.get_mut(&1).unwrap().children[0].target.id += 100),
        Box::new(|v| v.shared.get_mut(&1).unwrap().children[0].target.authority += 1),
        Box::new(|v| v.shared.get_mut(&1).unwrap().children[0].target.placement += 1),
        Box::new(|v| v.shared.get_mut(&1).unwrap().children[0].leaf = true),
        Box::new(|v| {
            v.shared.remove(&1);
        }),
        Box::new(|v| {
            v.units.get_mut(&10).unwrap().placement += 1;
        }),
        Box::new(|v| {
            v.units.remove(&10);
        }),
        Box::new(|v| {
            let shared = v.shared.get_mut(&1).unwrap();
            shared.children.push(Child {
                target: NodeRef {
                    id: 90,
                    authority: 2,
                    placement: 0,
                },
                centroid: vec![0.; 8],
                leaf: false,
            });
        }),
    ];
    for (case, change) in changes.into_iter().enumerate() {
        let mut next = original.clone();
        change(&mut next);
        assert_ne!(
            original.navigation_topology,
            next.prepared().unwrap().navigation_topology,
            "dependency case {case}"
        );
    }
    // The cache has no wire representation and cannot change saved view identity.
    let decoded: super::super::construction::BoundaryView =
        serde_json::from_slice(&serde_json::to_vec(&original).unwrap()).unwrap();
    assert_eq!(original, decoded);
    assert_eq!(
        original.navigation_topology,
        decoded.prepared().unwrap().navigation_topology
    );
}

#[test]
fn navigation_structural_publication_restarts_and_uses_replacement_boundary() {
    let (owner, cluster, old) = navigation_snapshot_fixture();
    let fresh = NodeRef { id: 20, ..old };
    let weak = Arc::downgrade(&cluster);
    let count = Arc::new(AtomicU64::new(0));
    let calls = count.clone();
    *cluster.navigation_snapshot_hook.lock() = Some(Arc::new(move || {
        if calls.fetch_add(1, Ordering::Relaxed) != 0 {
            return;
        }
        let cluster = weak.upgrade().unwrap();
        let mut replacement = cluster.local.node(old).unwrap();
        replacement.target = fresh;
        cluster
            .local
            .publish_images(vec![replacement], &[old.id])
            .unwrap();
        publish_navigation_fixture_view(&cluster, |view| {
            view.units.remove(&old.id);
            view.units.insert(fresh.id, fresh);
            view.shared.get_mut(&1).unwrap().children[0].target = fresh;
            view.root = view.shared[&1].clone();
        });
    }));
    let result = cluster
        .navigate(&[vec![0.; 8]], &ReadBeamPolicy::uniform(None, 4, 4), None)
        .unwrap();
    assert_eq!(
        result[0]
            .iter()
            .map(|(node, _)| node.id)
            .collect::<Vec<_>>(),
        vec![11, 12]
    );
    assert_eq!(count.load(Ordering::Relaxed), 2);
    assert_eq!(
        owner.observability.snapshot().counters["NavigationAttempts"],
        2
    );
    assert!(owner.node(old).is_err());
    assert_eq!(owner.node(fresh).unwrap().target, fresh);
}

#[test]
fn navigation_pinned_view_rejects_retired_and_relocated_owned_images() {
    for relocate in [false, true] {
        let (owner, cluster, old) = navigation_snapshot_fixture();
        let view = cluster.current_view();
        let attempt = NavigationAttempt {
            cluster: &cluster,
            view: view.clone(),
        };
        if relocate {
            let mut image = owner.node(old).unwrap();
            image.target.placement += 1;
            owner.publish_images(vec![image], &[]).unwrap();
        } else {
            owner.publish_images(vec![], &[old.id]).unwrap();
        }
        let mut children = vec![];
        let mut remote = vec![];
        assert!(attempt
            .expand_local_nodes(
                &[old],
                &[0.; 8],
                owner.run,
                0,
                view.sequence,
                &mut children,
                &mut remote
            )
            .unwrap_err()
            .contains("stale local image"));
        let reply = owner
            .expand(ExpandRequest {
                navigation: 123,
                step: 1,
                query_slot: 0,
                query: vec![0.; 8],
                nodes: vec![old],
                run: owner.run,
                epoch: 0,
                shared_sequence: view.sequence,
            })
            .unwrap();
        assert_eq!(reply.status, ExpansionStatus::Stale);
        assert!(reply.children.is_empty());
    }
}

#[test]
fn navigation_structural_churn_retains_finite_retry_bound_and_reason() {
    let (_, cluster, _) = navigation_snapshot_fixture();
    let weak = Arc::downgrade(&cluster);
    *cluster.navigation_snapshot_hook.lock() = Some(Arc::new(move || {
        let cluster = weak.upgrade().unwrap();
        publish_navigation_fixture_view(&cluster, |view| {
            // Every attempt observes a distinct ownership placement, so none can
            // safely continue against its older boundary identity.
            view.units.get_mut(&10).unwrap().placement += 1;
        });
    }));
    let error = cluster
        .navigate(&[vec![0.; 8]], &ReadBeamPolicy::uniform(None, 4, 4), None)
        .unwrap_err();
    assert!(
        error.contains("whole-attempt retry budget; attempts=8"),
        "{error}"
    );
    assert!(
        error.contains("last_rejection=navigation route changed: shared topology or placement"),
        "{error}"
    );
    assert_eq!(cluster.current_view().sequence, 8);
}

#[test]
fn navigation_compatible_publication_keeps_remote_owner_expansion_and_global_beam() {
    use super::super::protocol::publication::Publication;
    use super::super::transport::{RpcServer, TransportConfig};
    let (one, old_cluster, _) = navigation_snapshot_fixture();
    let two = Arc::new(OwnerService::new(
        2,
        one.run,
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
        one.vectors.clone(),
    ));
    let old = old_cluster.current_view().value.clone();
    let mut remote_nodes = vec![];
    for id in [10, 11, 12] {
        let resident = one.nodes.get(&id).unwrap().value().clone();
        let node = resident.read();
        let mut routing = (*node.routing).clone();
        routing.target.authority = 2;
        routing.target.id += 100;
        for coordinate in &mut routing.centroid {
            *coordinate += 4.;
        }
        if let Some(parent) = routing.parent.as_mut() {
            if parent.id != 1 {
                parent.authority = 2;
                parent.id += 100;
            }
        }
        for child in &mut routing.children {
            child.target.authority = 2;
            child.target.id += 100;
            for coordinate in &mut child.centroid {
                *coordinate += 4.;
            }
        }
        let postings = node.postings.as_ref().map(|p| {
            let ids = p.ids.iter().map(|id| id + 2).collect::<Vec<_>>();
            let codes = ids
                .iter()
                .flat_map(|id| {
                    Code::<1>::quantize(&two.vectors.get(*id).unwrap(), &routing.centroid)
                        .as_ref()
                        .to_vec()
                })
                .collect();
            PostingList { ids, codes }
        });
        remote_nodes.push(StoredNode { routing, postings });
    }
    two.install_initial(remote_nodes).unwrap();
    let endpoint = two.clone();
    let mut server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "nav-snapshot-test".into(),
        TransportConfig::default(),
        Arc::new(move |q: EngineRequest| endpoint.handle(q)),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "nav-snapshot-test".into(),
        TransportConfig::default(),
    )
    .unwrap();
    let mut view = old;
    let remote = NodeRef {
        id: 110,
        authority: 2,
        placement: 0,
    };
    view.shared.get_mut(&1).unwrap().children.push(Child {
        target: remote,
        centroid: vec![5.; 8],
        leaf: false,
    });
    view.root = view.shared[&1].clone();
    view.units.insert(110, remote);
    let mut cluster = Cluster::new(
        one.clone(),
        BTreeMap::from([(2, client)]),
        view.root.clone(),
    );
    cluster.publication = Publication::new(view.prepared().unwrap(), 1000);
    let cluster = Arc::new(cluster);
    one.bind_cluster(&cluster);
    let weak = Arc::downgrade(&cluster);
    *cluster.navigation_snapshot_hook.lock() = Some(Arc::new(move || {
        let cluster = weak.upgrade().unwrap();
        publish_navigation_fixture_view(&cluster, |view| {
            view.shared.get_mut(&1).unwrap().revision += 1;
        });
    }));
    let rows = cluster
        .navigate(
            &[vec![0.; 8], vec![3.; 8], vec![6.; 8]],
            &ReadBeamPolicy::uniform(None, 2, 2),
            None,
        )
        .unwrap();
    assert_eq!(
        rows[0]
            .iter()
            .map(|(r, d)| (r.authority, r.id, *d))
            .collect::<Vec<_>>(),
        vec![(1, 11, 0.), (1, 12, 32.)]
    );
    assert_eq!(
        rows[1]
            .iter()
            .map(|(r, d)| (r.authority, r.id, *d))
            .collect::<Vec<_>>(),
        vec![(1, 12, 8.), (2, 111, 8.)]
    );
    assert_eq!(
        rows[2]
            .iter()
            .map(|(r, d)| (r.authority, r.id, *d))
            .collect::<Vec<_>>(),
        vec![(2, 112, 0.), (2, 111, 32.)]
    );
    assert_eq!(two.expansions.load(Ordering::Relaxed), 3);
    assert_eq!(
        one.observability.snapshot().counters["NavigationAttempts"],
        1
    );
    server.shutdown();
}

// Reproduce the admitted epoch23 array representation independently of the
// separately frozen field codec. Handler scoring and TCP framing are real.
#[derive(Serialize, Deserialize)]
struct LegacyExpansionReply {
    authority: AuthorityId,
    run: u128,
    epoch: u64,
    shared_sequence: u64,
    child_counts: Vec<(NodeId, usize)>,
    navigation: u128,
    step: u64,
    query_slot: usize,
    nodes: Vec<NodeRef>,
    status: ExpansionStatus,
    children: Vec<ScoredChild>,
}
impl From<ExpandReply> for LegacyExpansionReply {
    fn from(r: ExpandReply) -> Self {
        Self {
            authority: r.authority,
            run: r.run,
            epoch: r.epoch,
            shared_sequence: r.shared_sequence,
            child_counts: r.child_counts,
            navigation: r.navigation,
            step: r.step,
            query_slot: r.query_slot,
            nodes: r.nodes,
            status: r.status,
            children: r.children,
        }
    }
}
#[derive(Serialize, Deserialize)]
enum LegacyExpansionResponse {
    Expanded(Vec<LegacyExpansionReply>),
}
fn remote_expansion_owner() -> (Arc<OwnerService>, RoutingNode) {
    let owner = Arc::new(OwnerService::new(
        2,
        77,
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
        Arc::new(|id: u64| -> Result<Arc<[f32]>, String> { Ok(vec![id as f32; 8].into()) }),
    ));
    let root_ref = NodeRef {
        id: (2u128 << 64) + 10,
        authority: 2,
        placement: 1,
    };
    let mut children = vec![];
    let mut records = vec![];
    for i in 0..100 {
        let node = RoutingNode {
            navigation: Default::default(),
            target: NodeRef {
                id: root_ref.id + i + 1,
                ..root_ref
            },
            parent: Some(root_ref),
            centroid: vec![0.; 8],
            centroid_code: vec![],
            children: vec![],
            leaf: true,
            revision: 1,
        };
        children.push(Child {
            target: node.target,
            centroid: node.centroid.clone(),
            leaf: true,
        });
        records.push(StoredNode {
            routing: node,
            postings: Some(PostingList::default()),
        });
    }
    let root = RoutingNode {
        navigation: Default::default(),
        target: root_ref,
        parent: None,
        centroid: vec![0.; 8],
        centroid_code: vec![],
        children,
        leaf: false,
        revision: 1,
    };
    records.push(StoredNode {
        routing: root.clone(),
        postings: None,
    });
    owner.install_initial(records).unwrap();
    (owner, root)
}
fn expansion_requests(root: NodeRef, count: usize) -> Vec<ExpandRequest> {
    (0..count)
        .map(|slot| ExpandRequest {
            navigation: (1u128 << 64) + slot as u128,
            step: 2,
            query_slot: slot,
            query: vec![0.; 8],
            nodes: vec![root],
            run: 77,
            epoch: 0,
            shared_sequence: 3,
        })
        .collect()
}
#[test]
fn oversized_epoch23_array_expansion_reply_reports_size_instead_of_ambiguous_eof() {
    use super::super::transport::{RpcServer, TransportConfig};
    let (owner, root) = remote_expansion_owner();
    let request = expansion_requests(root.target, 1100);
    let EngineResponse::Expanded(rows) = owner
        .handle(EngineRequest::Expand(request.clone()))
        .unwrap()
    else {
        panic!("expand")
    };
    let reply = LegacyExpansionResponse::Expanded(rows.into_iter().map(Into::into).collect());
    let encoded = serde_json::to_vec(&Ok::<_, String>(reply)).unwrap();
    assert!(encoded.len() > 16 * 1024 * 1024);
    eprintln!("controlled legacy reply bytes={}", encoded.len());
    let endpoint = owner.clone();
    let mut server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "frame-diagnostic".into(),
        TransportConfig::default(),
        Arc::new(
            move |request: EngineRequest| match endpoint.handle(request)? {
                EngineResponse::Expanded(rows) => Ok(LegacyExpansionResponse::Expanded(
                    rows.into_iter().map(Into::into).collect(),
                )),
                _ => Err("expected expansion".into()),
            },
        ),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "frame-diagnostic".into(),
        TransportConfig::default(),
    )
    .unwrap();
    let result = client.call::<_, LegacyExpansionResponse>(
        &EngineRequest::Expand(request),
        RequestClass::Control,
        Instant::now() + Duration::from_secs(10),
    );
    let error = result
        .err()
        .expect("the oversized response cannot be a successful partial reply");
    assert!(
        error.to_string().contains("reply exceeds configured limit"),
        "a known size failure must retain its cause instead of becoming EOF: {error}"
    );
    server.shutdown();
    assert_eq!(server.counters().messages_received, 1);
}

fn expansion_test_cluster(
    root: RoutingNode,
    config: super::super::transport::TransportConfig,
    handler: impl Fn(EngineRequest) -> Result<EngineResponse, String> + Send + Sync + 'static,
) -> (Arc<Cluster>, super::super::transport::RpcServer) {
    use super::super::transport::RpcServer;
    let server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "bounded-expansion-test".into(),
        config.clone(),
        Arc::new(handler),
    )
    .unwrap();
    let peer = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "bounded-expansion-test".into(),
        config,
    )
    .unwrap();
    let local = service();
    let cluster = Arc::new(Cluster::new(
        local.clone(),
        BTreeMap::from([(2, peer)]),
        root,
    ));
    local.bind_cluster(&cluster);
    (cluster, server)
}
#[test]
fn bounded_expansion_large_query_batch_preserves_complete_global_tie_order() {
    use super::super::transport::TransportConfig;
    let (owner, root) = remote_expansion_owner();
    let requests = expansion_requests(root.target, 2200);
    let EngineResponse::Expanded(expected) = owner
        .handle(EngineRequest::Expand(requests.clone()))
        .unwrap()
    else {
        panic!("expand")
    };
    let aggregate_error =
        serde_json::to_vec(&Ok::<_, String>(EngineResponse::Expanded(expected.clone())))
            .unwrap_err();
    assert!(
        aggregate_error
            .to_string()
            .contains("expansion payload exceeds decoded byte limit"),
        "compressed metadata must not admit an oversized decoded aggregate: {aggregate_error}"
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = seen.clone();
    let endpoint = owner.clone();
    let (cluster, mut server) =
        expansion_test_cluster(root, TransportConfig::default(), move |q| {
            if let EngineRequest::Expand(r) = &q {
                observed.lock().push(r.len());
            }
            endpoint.handle(q)
        });
    let actual = cluster.expand_remote_bounded(2, requests).unwrap();
    assert_eq!(
        actual, expected,
        "all identities, scores, revisions, slots and canonical row order must survive grouping"
    );
    assert_eq!(seen.lock().iter().sum::<usize>(), 2200);
    assert!(seen.lock().iter().all(|n| *n <= 64));
    // Exercise the real global selector too, including the equal-score order.
    let queries = vec![vec![0.; 8]; 130];
    let policy = ReadBeamPolicy::uniform(None, 16, 16);
    let first = cluster.navigate(&queries, &policy, None).unwrap();
    assert_eq!(first.len(), 130);
    assert!(first.windows(2).all(|p| p[0] == p[1]));
    assert!(first.iter().all(
        |rows| rows.len() == 16 && rows.iter().all(|(node, d)| node.authority == 2 && *d == 0.)
    ));
    assert!(server.counters().messages_received > 1);
    server.shutdown();
}
#[test]
fn bounded_expansion_small_frame_bisects_queries_without_dropping_rows() {
    use super::super::transport::TransportConfig;
    let (owner, root) = remote_expansion_owner();
    let requests = expansion_requests(root.target, 2);
    let EngineResponse::Expanded(expected) = owner
        .handle(EngineRequest::Expand(requests.clone()))
        .unwrap()
    else {
        panic!("expand")
    };
    let single_bytes = serde_json::to_vec(&Ok::<_, String>(EngineResponse::Expanded(
        expected[..1].to_vec(),
    )))
    .unwrap()
    .len();
    let pair_bytes =
        serde_json::to_vec(&Ok::<_, String>(EngineResponse::Expanded(expected.clone())))
            .unwrap()
            .len();
    assert!(pair_bytes > single_bytes);
    let frame_bytes = (single_bytes + pair_bytes) / 2;
    let endpoint = owner.clone();
    let (cluster, mut server) = expansion_test_cluster(
        root,
        TransportConfig {
            max_frame_bytes: frame_bytes,
            ..Default::default()
        },
        move |q| endpoint.handle(q),
    );
    assert_eq!(
        cluster.expand_remote_bounded(2, requests).unwrap(),
        expected
    );
    assert_eq!(
        server.counters().messages_received,
        3,
        "one explicit size rejection and two complete replies"
    );
    assert_eq!(cluster.peers[&2].counters().failed_connections, 1);
    server.shutdown();
}
#[test]
fn bounded_expansion_single_query_splits_only_complete_parent_sets() {
    use super::super::transport::TransportConfig;
    let (owner, root) = remote_expansion_owner();
    let prototype = owner
        .expand(expansion_requests(root.target, 1).remove(0))
        .unwrap();
    let second = NodeRef {
        id: root.target.id + 1000,
        ..root.target
    };
    let mut request = expansion_requests(root.target, 1).remove(0);
    request.nodes.push(second);
    let mut combined = prototype.clone();
    combined.nodes.push(second);
    combined
        .child_counts
        .push((second.id, prototype.children.len()));
    combined
        .children
        .extend(prototype.children.iter().cloned().map(|mut child| {
            child.parent = second.id;
            child
        }));
    let single_bytes = serde_json::to_vec(&Ok::<_, String>(EngineResponse::Expanded(vec![
        prototype.clone(),
    ])))
    .unwrap()
    .len();
    let combined_bytes =
        serde_json::to_vec(&Ok::<_, String>(EngineResponse::Expanded(vec![combined])))
            .unwrap()
            .len();
    assert!(combined_bytes > single_bytes);
    let frame_bytes = (single_bytes + combined_bytes) / 2;
    let endpoint = prototype.clone();
    let (cluster, mut server) = expansion_test_cluster(
        root,
        TransportConfig {
            max_frame_bytes: frame_bytes,
            ..Default::default()
        },
        move |q| {
            let EngineRequest::Expand(requests) = q else {
                return Err("expand only".into());
            };
            let mut replies = vec![];
            for r in requests {
                let mut reply = endpoint.clone();
                reply.navigation = r.navigation;
                reply.step = r.step;
                reply.query_slot = r.query_slot;
                reply.shared_sequence = r.shared_sequence;
                reply.nodes = r.nodes;
                reply.children.clear();
                reply.child_counts.clear();
                for node in &reply.nodes {
                    reply.child_counts.push((node.id, 100));
                    for mut child in endpoint.children.clone() {
                        child.parent = node.id;
                        reply.children.push(child);
                    }
                }
                replies.push(reply);
            }
            Ok(EngineResponse::Expanded(replies))
        },
    );
    let rows = cluster.expand_remote_bounded(2, vec![request]).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.iter().map(|r| r.children.len()).sum::<usize>(), 200);
    assert_eq!(rows[0].nodes, vec![prototype.nodes[0]]);
    assert_eq!(rows[1].nodes, vec![second]);
    for r in rows {
        assert_eq!(r.child_counts, vec![(r.nodes[0].id, 100)]);
        assert!(r.children.iter().map(|c| c.ordinal).eq(0..100));
    }
    assert_eq!(server.counters().messages_received, 3);
    server.shutdown();
}
#[test]
fn bounded_expansion_single_parent_oversize_is_fatal_and_never_loops() {
    use super::super::transport::TransportConfig;
    let (owner, root) = remote_expansion_owner();
    let requests = expansion_requests(root.target, 1);
    let endpoint = owner.clone();
    let (cluster, mut server) = expansion_test_cluster(
        root,
        TransportConfig {
            max_frame_bytes: 4096,
            ..Default::default()
        },
        move |q| endpoint.handle(q),
    );
    let error = cluster.expand_remote_bounded(2, requests).unwrap_err();
    assert!(
        error.contains("one complete parent expansion exceeds"),
        "{error}"
    );
    server.shutdown();
    assert_eq!(server.counters().messages_received, 1);
}
#[test]
fn bounded_expansion_codec_field_limit_has_explicit_size_cause() {
    use super::super::transport::TransportConfig;
    let (owner, root) = remote_expansion_owner();
    let prototype = owner
        .expand(expansion_requests(root.target, 1).remove(0))
        .unwrap();
    let mut oversized = prototype;
    oversized.children = vec![oversized.children[0].clone(); 190_000];
    // The field limit is reached before the outer frame writer fills. This
    // specific signal must not be confused with arbitrary codec/data errors.
    let (cluster, mut server) =
        expansion_test_cluster(root.clone(), TransportConfig::default(), move |_| {
            Ok(EngineResponse::Expanded(vec![oversized.clone()]))
        });
    let error = cluster
        .expand_remote_bounded(2, expansion_requests(root.target, 1))
        .unwrap_err();
    assert!(
        error.contains("one complete parent expansion exceeds"),
        "{error}"
    );
    assert!(!error.contains("end of file"));
    server.shutdown();
    assert_eq!(server.counters().messages_received, 1);
}
#[test]
fn bounded_expansion_late_failure_or_ambiguous_codec_eof_never_returns_a_prefix_or_retries() {
    use super::super::transport::TransportConfig;
    for invalid_codec in [false, true] {
        let (owner, root) = remote_expansion_owner();
        let requests = expansion_requests(root.target, 65);
        let count = Arc::new(AtomicU64::new(0));
        let seen = count.clone();
        let endpoint = owner.clone();
        let (cluster, mut server) =
            expansion_test_cluster(root, TransportConfig::default(), move |q| {
                let call = seen.fetch_add(1, Ordering::Relaxed);
                if call == 0 {
                    return endpoint.handle(q);
                }
                if !invalid_codec {
                    return Err("controlled late operation rejection".into());
                }
                let EngineResponse::Expanded(mut rows) = endpoint.handle(q)? else {
                    unreachable!()
                };
                rows[0].children[0].distance = f32::NAN;
                Ok(EngineResponse::Expanded(rows))
            });
        let error = cluster.expand_remote_bounded(2, requests).unwrap_err();
        assert!(
            error.contains(if invalid_codec {
                "end of file"
            } else {
                "controlled late operation rejection"
            }),
            "{error}"
        );
        assert_eq!(
            count.load(Ordering::Relaxed),
            2,
            "the successful prefix must neither leak nor permit an arbitrary error retry"
        );
        server.shutdown();
    }
}
#[test]
fn bounded_expansion_all_subcalls_keep_one_absolute_deadline() {
    use super::super::transport::TransportConfig;
    let (owner, root) = remote_expansion_owner();
    let requests = expansion_requests(root.target, 129);
    let endpoint = owner.clone();
    let calls = Arc::new(AtomicU64::new(0));
    let seen = calls.clone();
    let (cluster, mut server) =
        expansion_test_cluster(root, TransportConfig::default(), move |q| {
            seen.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(300));
            endpoint.handle(q)
        });
    let original = Instant::now() + Duration::from_millis(500);
    let error = cluster
        .expand_remote_until(2, requests, original)
        .unwrap_err();
    assert!(
        error.contains("deadline") || error.contains("end of file"),
        "{error}"
    );
    assert!(calls.load(Ordering::Relaxed) <= 3);
    server.shutdown();
}

#[test]
fn bounded_expansion_preserves_one_rpc_for_a_fitting_64_query_16_parent_level() {
    use super::super::transport::TransportConfig;
    let (template, _) = remote_expansion_owner();
    let owner = Arc::new(OwnerService::new(
        2,
        77,
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
        template.vectors.clone(),
    ));
    let root_ref = NodeRef {
        id: (2u128 << 64) + 1,
        authority: 2,
        placement: 1,
    };
    let mut records = vec![];
    let mut root_children = vec![];
    for index in 0..16u128 {
        let parent = NodeRef {
            id: root_ref.id + 100 + index * 10,
            ..root_ref
        };
        let mut children = vec![];
        for ordinal in 0..3u128 {
            let target = NodeRef {
                id: parent.id + ordinal + 1,
                ..parent
            };
            children.push(Child {
                target,
                centroid: vec![0.; 8],
                leaf: true,
            });
            records.push(StoredNode {
                routing: RoutingNode {
                    navigation: Default::default(),
                    target,
                    parent: Some(parent),
                    centroid: vec![0.; 8],
                    centroid_code: vec![],
                    children: vec![],
                    leaf: true,
                    revision: 1,
                },
                postings: Some(PostingList::default()),
            });
        }
        root_children.push(Child {
            target: parent,
            centroid: vec![0.; 8],
            leaf: false,
        });
        records.push(StoredNode {
            routing: RoutingNode {
                navigation: Default::default(),
                target: parent,
                parent: Some(root_ref),
                centroid: vec![0.; 8],
                centroid_code: vec![],
                children,
                leaf: false,
                revision: 1,
            },
            postings: None,
        });
    }
    let root = RoutingNode {
        navigation: Default::default(),
        target: root_ref,
        parent: None,
        centroid: vec![0.; 8],
        centroid_code: vec![],
        children: root_children,
        leaf: false,
        revision: 1,
    };
    records.push(StoredNode {
        routing: root.clone(),
        postings: None,
    });
    owner.install_initial(records).unwrap();
    struct DirectOwner(Arc<OwnerService>);
    impl ExpansionBackend for DirectOwner {
        fn expand(
            &self,
            _: AuthorityId,
            requests: Vec<ExpandRequest>,
        ) -> Result<Vec<ExpandReply>, String> {
            match self.0.handle(EngineRequest::Expand(requests))? {
                EngineResponse::Expanded(r) => Ok(r),
                _ => Err("expand reply".into()),
            }
        }
    }
    let queries = vec![vec![0.; 8]; 64];
    let policy = ReadBeamPolicy::uniform(None, 16, 16);
    let expected = routing::navigate_batch(
        &DirectOwner(owner.clone()),
        &root,
        &queries,
        &policy,
        77,
        0,
        0,
        1000,
        None,
    )
    .unwrap()
    .0;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let capture = seen.clone();
    let endpoint = owner.clone();
    let (cluster, mut server) =
        expansion_test_cluster(root, TransportConfig::default(), move |q| {
            if let EngineRequest::Expand(r) = &q {
                capture
                    .lock()
                    .push((r.len(), r.iter().map(|r| r.nodes.len()).sum::<usize>()));
            }
            endpoint.handle(q)
        });
    assert_eq!(cluster.navigate(&queries, &policy, None).unwrap(), expected);
    assert_eq!(
        *seen.lock(),
        vec![(64, 64), (64, 1024)],
        "batching must count queries rather than parents"
    );
    assert_eq!(server.counters().messages_received, 2);
    assert_eq!(
        cluster.local.observability.snapshot().counters["RemoteEngineCalls"],
        2
    );
    server.shutdown();
}

#[test]
fn bounded_expansion_two_workers_overlap_preserve_order_and_bound_active_calls() {
    use super::super::transport::TransportConfig;
    let (owner, root) = remote_expansion_owner();
    let requests = expansion_requests(root.target, 256);
    let EngineResponse::Expanded(expected) = owner
        .handle(EngineRequest::Expand(requests.clone()))
        .unwrap()
    else {
        panic!("expand")
    };
    let active = Arc::new(AtomicU64::new(0));
    let peak = Arc::new(AtomicU64::new(0));
    let state = Arc::new((
        Mutex::new((vec![false; 4], Vec::new())),
        parking_lot::Condvar::new(),
    ));
    let (a, p, s) = (active.clone(), peak.clone(), state.clone());
    let (cluster, mut server) =
        expansion_test_cluster(root, TransportConfig::default(), move |request| {
            let EngineRequest::Expand(rows) = &request else {
                return Err("expand required".into());
            };
            let group = rows[0].query_slot as usize / 64;
            let now = a.fetch_add(1, Ordering::SeqCst) + 1;
            p.fetch_max(now, Ordering::SeqCst);
            let mut state = s.0.lock();
            state.0[group] = true;
            s.1.notify_all();
            let partner = group ^ 1;
            // The even group cannot complete until the odd group has completed its
            // handler. This proves overlap and reverses group completion without a
            // timing assertion. The timeout only bounds a broken implementation.
            while !state.0[partner] || (group % 2 == 0 && !state.1.contains(&partner)) {
                if s.1.wait_for(&mut state, Duration::from_secs(5)).timed_out() {
                    a.fetch_sub(1, Ordering::SeqCst);
                    return Err("paired expansion did not overlap".into());
                }
            }
            drop(state);
            let reply = owner.handle(request);
            let mut state = s.0.lock();
            state.1.push(group);
            s.1.notify_all();
            a.fetch_sub(1, Ordering::SeqCst);
            reply
        });
    let job = cluster.local.observability.job(
        super::super::observability::JobKind::Balance,
        1,
        123,
        AttemptId {
            authority: 1,
            sequence: 17,
        },
    );
    let actual = cluster
        .expand_remote_with_concurrency(2, requests, Instant::now() + Duration::from_secs(20), 2)
        .unwrap();
    drop(job);
    if super::super::observability::detailed() {
        let profile = cluster.local.observability.snapshot();
        let job = profile
            .longest_jobs
            .iter()
            .find(|job| job.node_id == "123")
            .unwrap();
        assert_eq!(
            job.work.remote_requests["Expand"].calls, 4,
            "both caller and child lane must retain the initiating job context"
        );
    }
    assert_eq!(actual, expected);
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    let completed = &state.0.lock().1;
    assert!(completed.iter().position(|g| *g == 1) < completed.iter().position(|g| *g == 0));
    assert!(completed.iter().position(|g| *g == 3) < completed.iter().position(|g| *g == 2));
    server.shutdown();
}

#[test]
fn bounded_expansion_concurrent_error_joins_other_lane_without_retry_or_prefix() {
    use super::super::transport::TransportConfig;
    let (owner, root) = remote_expansion_owner();
    let state = Arc::new((Mutex::new((false, false)), parking_lot::Condvar::new()));
    let shared = state.clone();
    let calls = Arc::new(AtomicU64::new(0));
    let seen = calls.clone();
    let (cluster, mut server) =
        expansion_test_cluster(root.clone(), TransportConfig::default(), move |request| {
            seen.fetch_add(1, Ordering::SeqCst);
            let EngineRequest::Expand(rows) = &request else {
                unreachable!()
            };
            if rows[0].query_slot == 0 {
                let mut state = shared.0.lock();
                while !state.0 {
                    if shared
                        .1
                        .wait_for(&mut state, Duration::from_secs(5))
                        .timed_out()
                    {
                        return Err("missing paired request".into());
                    }
                }
                return Err("controlled group rejection".into());
            }
            let reply = owner.handle(request);
            let mut state = shared.0.lock();
            state.0 = true;
            state.1 = true;
            shared.1.notify_all();
            reply
        });
    let error = cluster
        .expand_remote_with_concurrency(
            2,
            expansion_requests(root.target, 128),
            Instant::now() + Duration::from_secs(20),
            2,
        )
        .unwrap_err();
    assert!(error.contains("controlled group rejection"), "{error}");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(state.0.lock().1);
    server.shutdown();
}

#[test]
fn bounded_expansion_concurrent_expired_deadline_sends_nothing() {
    use super::super::transport::TransportConfig;
    let (_, root) = remote_expansion_owner();
    let calls = Arc::new(AtomicU64::new(0));
    let seen = calls.clone();
    let (cluster, mut server) =
        expansion_test_cluster(root.clone(), TransportConfig::default(), move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
            Err("unexpected expired call".into())
        });
    let error = cluster
        .expand_remote_with_concurrency(
            2,
            expansion_requests(root.target, 256),
            Instant::now() - Duration::from_millis(1),
            2,
        )
        .unwrap_err();
    assert!(error.contains("deadline"), "{error}");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    server.shutdown();
}

#[test]
fn bounded_expansion_concurrent_size_splits_preserve_original_group_order() {
    use super::super::transport::TransportConfig;
    for concurrency in [1, 2] {
        let (owner, root) = remote_expansion_owner();
        let requests = expansion_requests(root.target, 128);
        let EngineResponse::Expanded(expected) = owner
            .handle(EngineRequest::Expand(requests.clone()))
            .unwrap()
        else {
            panic!("expand")
        };
        let bytes = |n| {
            serde_json::to_vec(&Ok::<_, String>(EngineResponse::Expanded(
                expected[..n].to_vec(),
            )))
            .unwrap()
            .len()
        };
        let frame_bytes = (bytes(32) + bytes(64)) / 2;
        let (cluster, mut server) = expansion_test_cluster(
            root,
            TransportConfig {
                max_frame_bytes: frame_bytes,
                ..Default::default()
            },
            move |q| owner.handle(q),
        );
        let actual = cluster
            .expand_remote_with_concurrency(
                2,
                requests,
                Instant::now() + Duration::from_secs(20),
                concurrency,
            )
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(
            server.counters().messages_received,
            6,
            "two explicit size rejections and four complete replies"
        );
        assert_eq!(cluster.peers[&2].counters().failed_connections, 2);
        server.shutdown();
    }
}
