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
fn stale_transfer_centroid_revision_cannot_stage_or_turn_into_success() {
    let owner = service();
    let source = leaf(&owner, 10, vec![7], 0);
    let destination = leaf(&owner, 20, vec![], 1);
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
