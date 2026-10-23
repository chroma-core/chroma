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
        }),
        2 => EngineRequest::EnqueueReassign(ReassignJob {
            envelope,
            source: node,
            ids: vec![7],
            excluded: None,
            blocked: vec![],
            blocked_jobs: vec![],
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
