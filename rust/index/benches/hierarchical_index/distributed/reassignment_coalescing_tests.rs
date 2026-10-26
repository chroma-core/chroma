//! Fixed queue decisions establish what coalescing removes without relying on
//! randomized construction timing. Execution tests use actual reassignment.
use super::super::super::config::HierarchicalSpannConfig;
use super::*;
use chroma_distance::DistanceFunction;
const BASE: NodeId = 1 << 92;
fn point(id: u64) -> Arc<[f32]> {
    vec![if id == 1 { 10. } else { 0. }; 8].into()
}
fn fixture() -> (Arc<OwnerService>, Arc<Cluster>, NodeRef) {
    let owner = Arc::new(OwnerService::new(
        1,
        2029,
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig {
            split_threshold: 100,
            merge_threshold: 0,
            branching_factor: 8,
            write_beam_min: 8,
            write_beam_max: 8,
            ..Default::default()
        },
        Arc::new(|id: u64| -> Result<Arc<[f32]>, String> { Ok(point(id)) }),
    ));
    let mut root = owner.initial_root();
    root.routing.target = NodeRef {
        id: BASE,
        authority: 1,
        placement: 0,
    };
    let mut source = owner
        .fresh_leaf(
            NodeRef {
                id: BASE + 1,
                authority: 1,
                placement: 0,
            },
            vec![0.; 8],
            vec![0, 1, 2],
        )
        .unwrap();
    let mut target = owner
        .fresh_leaf(
            NodeRef {
                id: BASE + 2,
                authority: 1,
                placement: 0,
            },
            vec![10.; 8],
            vec![],
        )
        .unwrap();
    source.routing.parent = Some(root.routing.target);
    target.routing.parent = Some(root.routing.target);
    root.routing.children = vec![child(&source.routing), child(&target.routing)];
    root.routing.leaf = false;
    root.postings = None;
    let source_ref = source.routing.target;
    let root_image = root.routing.clone();
    owner.install_initial(vec![root, source, target]).unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), root_image));
    owner.bind_cluster(&cluster);
    (owner, cluster, source_ref)
}
fn job(owner: &OwnerService, source: NodeRef, ids: Vec<u64>) -> ReassignJob {
    ReassignJob {
        envelope: owner
            .envelope("reassign-job", vec![(source.id, source.placement)])
            .unwrap(),
        source,
        ids,
        excluded: None,
        blocked: vec![],
        blocked_jobs: vec![],
        depth: 0,
    }
}
fn enqueue(owner: &OwnerService, request: &ReassignJob) {
    let request = EngineRequest::EnqueueReassign(request.clone());
    // This is the same representation decoded at a remote endpoint.
    let request = serde_json::from_slice(&serde_json::to_vec(&request).unwrap()).unwrap();
    assert!(matches!(
        owner.handle(request).unwrap(),
        EngineResponse::Ack
    ));
}
fn execute(owner: &OwnerService, cluster: &Cluster, work: &WorkJob) {
    let result = owner.handle(EngineRequest::ScheduledJob(work.clone()));
    cluster
        .jobs
        .lock()
        .finish(work, result.as_ref().map(|_| ()).map_err(Clone::clone));
    result.unwrap();
}
fn memberships(owner: &OwnerService) -> BTreeMap<NodeId, Vec<u64>> {
    let EngineResponse::Snapshot(nodes) = owner.handle(EngineRequest::Snapshot).unwrap() else {
        panic!("snapshot")
    };
    let mut all = vec![];
    let mut result = BTreeMap::new();
    for node in nodes {
        if let Some(posting) = node.postings {
            let expected = posting
                .ids
                .iter()
                .flat_map(|id| {
                    Code::<1>::quantize(&point(*id), &node.routing.centroid)
                        .as_ref()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            assert_eq!(posting.codes, expected);
            all.extend_from_slice(&posting.ids);
            result.insert(node.routing.target.id, posting.ids);
        }
    }
    all.sort_unstable();
    assert_eq!(all, vec![0, 1, 2]);
    result
}

#[test]
fn fixed_decisions_preserve_membership_and_codes_with_fewer_actual_query_rows() {
    let (ordinary, base, source) = fixture();
    for ids in [vec![0, 1], vec![0, 2], vec![1, 2]] {
        base.reassign(source, ids, None).unwrap();
    }
    let expected = memberships(&ordinary);
    let base_work = ordinary.observability.snapshot();
    let (owner, cluster, source) = fixture();
    for ids in [vec![0, 1], vec![0, 2], vec![1, 2]] {
        enqueue(&owner, &job(&owner, source, ids));
    }
    let batch = cluster.jobs.lock().next(1).unwrap();
    let WorkJob::Reassign(merged) = &batch else {
        panic!("retry batch")
    };
    assert_eq!(merged.ids, vec![0, 1, 2]);
    execute(&owner, &cluster, &batch);
    assert!(cluster.jobs.lock().is_drained());
    assert_eq!(memberships(&owner), expected);
    let work = owner.observability.snapshot();
    assert_eq!(
        base_work.counters["ReassignmentQueryBytesCopied"],
        5 * 8 * 4
    );
    assert_eq!(work.counters["ReassignmentQueryBytesCopied"], 3 * 8 * 4);
    assert_eq!(base_work.counters["ReassignmentRounds"], 3);
    assert_eq!(work.counters["ReassignmentRounds"], 1);
    let stats = cluster.reassignment_coalescing();
    assert_eq!(
        (
            stats.dispatches,
            stats.merged_jobs,
            stats.duplicate_pending_ids
        ),
        (1, 2, 3)
    );
}

#[test]
fn active_and_blocked_work_never_enter_a_ready_batch() {
    let (owner, cluster, source) = fixture();
    let producer = BalanceJob {
        envelope: owner.envelope("balance-job", vec![(BASE + 90, 0)]).unwrap(),
        node: NodeRef {
            id: BASE + 90,
            authority: 2,
            placement: 0,
        },
        depth: 0,
        internal: false,
    };
    let active = {
        let mut q = cluster.jobs.lock();
        q.enqueue(producer);
        q.next(2).unwrap()
    };
    enqueue(&owner, &job(&owner, source, vec![0]));
    let mut blocked = job(&owner, source, vec![1]);
    blocked.blocked_jobs = vec![active.attempt()];
    enqueue(&owner, &blocked);
    enqueue(&owner, &job(&owner, source, vec![2]));
    let selected = cluster.jobs.lock().next(1).unwrap();
    let WorkJob::Reassign(selected_batch) = &selected else {
        panic!("retry")
    };
    assert_eq!(selected_batch.ids, vec![0, 2]);
    execute(&owner, &cluster, &selected);
    assert!(cluster.jobs.lock().next(1).is_none());
    cluster.jobs.lock().finish(&active, Ok(()));
    let released = cluster.jobs.lock().next(1).unwrap();
    let WorkJob::Reassign(released_batch) = &released else {
        panic!("retry")
    };
    assert_eq!(released_batch.ids, vec![1]);
    execute(&owner, &cluster, &released);
    assert_eq!(memberships(&owner)[&source.id], vec![0, 2]);
}

#[test]
fn same_source_structural_and_neighbor_jobs_remain_queue_barriers() {
    for neighbor in [false, true] {
        let (owner, _, source) = fixture();
        let mut q = JobQueue::default();
        q.enqueue_reassign(job(&owner, source, vec![0]), &owner)
            .unwrap();
        let envelope = owner.envelope("barrier", vec![(source.id, 0)]).unwrap();
        if neighbor {
            q.enqueue_neighbor(NeighborJob {
                envelope,
                node: source,
                old: vec![0.; 8],
                left: vec![0.; 8],
                right: vec![1.; 8],
                depth: 0,
            });
        } else {
            q.enqueue(BalanceJob {
                envelope,
                node: source,
                depth: 0,
                internal: false,
            });
        }
        q.enqueue_reassign(job(&owner, source, vec![1]), &owner)
            .unwrap();
        let first = q.next(1).unwrap();
        let WorkJob::Reassign(a) = &first else {
            panic!("first retry")
        };
        assert_eq!(a.ids, vec![0]);
        q.finish(&first, Ok(()));
        let barrier = q.next(1).unwrap();
        assert!(!matches!(barrier, WorkJob::Reassign(_)));
        q.finish(&barrier, Ok(()));
        let last = q.next(1).unwrap();
        let WorkJob::Reassign(b) = &last else {
            panic!("last retry")
        };
        assert_eq!(b.ids, vec![1]);
        q.finish(&last, Ok(()));
        assert!(q.is_drained());
        assert_eq!(q.coalescing.merged_jobs, 0);
    }
}

#[test]
fn exclusion_owner_placement_and_epoch_keep_distinct_decisions() {
    let (owner, _, source) = fixture();
    let mut q = JobQueue::default();
    q.enqueue_reassign(job(&owner, source, vec![0]), &owner)
        .unwrap();
    let mut excluded = job(&owner, source, vec![1]);
    excluded.excluded = Some(source.id);
    q.enqueue_reassign(excluded, &owner).unwrap();
    let moved = NodeRef {
        authority: 2,
        placement: 1,
        ..source
    };
    q.enqueue_reassign(job(&owner, moved, vec![2]), &owner)
        .unwrap();
    let a = q.next(1).unwrap();
    let WorkJob::Reassign(first) = &a else {
        panic!("retry")
    };
    assert_eq!(first.ids, vec![0]);
    q.finish(&a, Ok(()));
    let b = q.next(1).unwrap();
    let WorkJob::Reassign(second) = &b else {
        panic!("retry")
    };
    assert_eq!(second.excluded, Some(source.id));
    q.finish(&b, Ok(()));
    let c = q.next(2).unwrap();
    let WorkJob::Reassign(third) = &c else {
        panic!("retry")
    };
    assert_eq!(third.source, moved);
    q.finish(&c, Ok(()));
    for wrong_epoch in [false, true] {
        let mut invalid = job(&owner, source, vec![0]);
        if wrong_epoch {
            invalid.envelope.epoch = 1
        } else {
            invalid.envelope.run += 1
        };
        assert!(q
            .enqueue_reassign(invalid, &owner)
            .unwrap_err()
            .contains("epoch differs"));
    }
    assert!(q.is_drained());
    assert_eq!(q.coalescing.merged_jobs, 0);
}

#[test]
fn replay_is_immutable_and_fresh_arrivals_after_dispatch_form_new_execution() {
    let (owner, cluster, source) = fixture();
    let mut first = job(&owner, source, vec![0]);
    first.envelope.attempt.authority = 2;
    enqueue(&owner, &first);
    enqueue(&owner, &job(&owner, source, vec![1]));
    let active = cluster.jobs.lock().next(1).unwrap();
    assert_eq!(active.attempt().authority, 1);
    assert_ne!(active.attempt(), first.envelope.attempt);
    let probe = owner.attempt().unwrap();
    enqueue(&owner, &first);
    assert_eq!(
        owner.attempt().unwrap().sequence,
        probe.sequence + 1,
        "replay allocates no execution identity"
    );
    assert!(cluster.jobs.lock().ready.is_empty());
    let mut changed = first.clone();
    changed.ids.push(2);
    assert!(owner
        .handle(EngineRequest::EnqueueReassign(changed))
        .unwrap_err()
        .contains("conflicting"));
    for _ in 0..5 {
        enqueue(&owner, &job(&owner, source, vec![0, 2]));
    }
    assert!(
        cluster.jobs.lock().next(1).is_none(),
        "active source cannot absorb or execute fresh arrivals"
    );
    execute(&owner, &cluster, &active);
    let fresh = cluster.jobs.lock().next(1).unwrap();
    assert_ne!(fresh.attempt(), active.attempt());
    let WorkJob::Reassign(batch) = &fresh else {
        panic!("fresh retry")
    };
    assert_eq!(batch.ids, vec![0, 2]);
    // The dispatched envelope/body is fixed for actual execution replay.
    owner
        .handle(EngineRequest::ScheduledJob(fresh.clone()))
        .unwrap();
    owner
        .handle(EngineRequest::ScheduledJob(fresh.clone()))
        .unwrap();
    let mut conflicting = fresh.clone();
    let WorkJob::Reassign(j) = &mut conflicting else {
        panic!("retry")
    };
    j.ids.push(1);
    assert!(owner
        .handle(EngineRequest::ScheduledJob(conflicting))
        .unwrap_err()
        .contains("Conflict"));
    cluster.jobs.lock().finish(&fresh, Ok(()));
    assert!(cluster.jobs.lock().is_drained());
    memberships(&owner);
    assert_eq!(
        owner.observability.snapshot().counters["ReassignmentQueryBytesCopied"],
        4 * 8 * 4
    );
    assert_eq!(cluster.reassignment_coalescing().merged_jobs, 5);
}

#[test]
fn union_payload_and_retained_ingress_records_remain_bounded() {
    let (owner, _, source) = fixture();
    let mut q = JobQueue::default();
    for ids in [(0..32768).collect(), (32768..65536).collect(), vec![0]] {
        q.enqueue_reassign(job(&owner, source, ids), &owner)
            .unwrap();
    }
    let first = q.next(1).unwrap();
    let WorkJob::Reassign(a) = &first else {
        panic!("retry")
    };
    assert_eq!(a.ids.len(), 32768);
    assert_eq!(a.ids[0], 0);
    q.finish(&first, Ok(()));
    let second = q.next(1).unwrap();
    let WorkJob::Reassign(b) = &second else {
        panic!("retry")
    };
    assert_eq!(b.ids.len(), 32768);
    assert_eq!(b.ids[0], 32768);
    q.finish(&second, Ok(()));
    assert!(q.is_drained());
    let mut bounded = JobQueue::default();
    let original = job(&owner, source, vec![0]);
    bounded
        .enqueue_reassign_with_limit(original.clone(), &owner, 2)
        .unwrap();
    bounded
        .enqueue_reassign_with_limit(job(&owner, source, vec![1]), &owner, 2)
        .unwrap();
    bounded
        .enqueue_reassign_with_limit(original.clone(), &owner, 2)
        .unwrap();
    assert!(bounded
        .enqueue_reassign_with_limit(job(&owner, source, vec![2]), &owner, 2)
        .unwrap_err()
        .contains("bound reached"));
    assert_eq!(bounded.reassign_seen.len(), 2);
    assert_eq!(bounded.ready.len(), 2);
}
