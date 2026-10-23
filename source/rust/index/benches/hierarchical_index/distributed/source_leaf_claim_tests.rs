//! Pause real split preparation after its membership claim, then exercise other
//! mutation paths without relying on scheduler timing or a large corpus.
use super::super::engine::VectorSource;
use super::super::placement_runtime::PlacementRequest;
use super::*;
use chroma_distance::DistanceFunction;
use std::sync::{atomic::AtomicBool, mpsc};
const BASE: u128 = 1 << 88;
fn source_value(id: u64) -> Arc<[f32]> {
    let value = if id < 8 {
        id as f32 / 8.
    } else if id < 16 {
        10. + (id - 8) as f32 / 8.
    } else {
        -10.
    };
    vec![value; 8].into()
}
fn make_fixture(
    vectors: Arc<dyn VectorSource>,
) -> (
    Arc<OwnerService>,
    Arc<Cluster>,
    NodeRef,
    NodeRef,
    NodeRef,
    NodeRef,
) {
    let owner = Arc::new(OwnerService::new(
        1,
        1908,
        8,
        DistanceFunction::Euclidean,
        super::super::super::config::HierarchicalSpannConfig {
            split_threshold: 4,
            merge_threshold: 0,
            branching_factor: 3,
            ..Default::default()
        },
        vectors,
    ));
    let root = NodeRef {
        id: BASE,
        authority: 1,
        placement: 0,
    };
    let mut leaves = vec![];
    for (offset, center, ids) in [
        (1, 0.5, (0..8).collect()),
        (2, 10.5, (8..16).collect()),
        (3, -10., vec![16]),
    ] {
        let target = NodeRef {
            id: BASE + offset,
            authority: 1,
            placement: 0,
        };
        let mut node = owner.fresh_leaf(target, vec![center; 8], ids).unwrap();
        node.routing.parent = Some(root);
        leaves.push(node)
    }
    let refs = leaves.iter().map(|n| n.routing.target).collect::<Vec<_>>();
    let mut parent = owner.initial_root();
    parent.routing.target = root;
    parent.routing.leaf = false;
    parent.routing.children = leaves.iter().map(|n| child(&n.routing)).collect();
    set_center(&mut parent.routing, 8).unwrap();
    parent.postings = None;
    let image = parent.routing.clone();
    leaves.push(parent);
    owner.install_initial(leaves).unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), image));
    owner.bind_cluster(&cluster);
    (owner, cluster, root, refs[0], refs[1], refs[2])
}
fn run_split(cluster: &Cluster, node: NodeRef) -> Result<EngineResponse, String> {
    cluster.request(
        1,
        EngineRequest::BalanceJob(BalanceJob {
            envelope: cluster
                .local
                .envelope("balance-job", vec![(node.id, node.placement)])?,
            node,
            depth: 4,
            internal: false,
        }),
    )
}
fn exact(owner: &OwnerService) {
    let EngineResponse::Snapshot(nodes) = owner.handle(EngineRequest::Snapshot).unwrap() else {
        panic!("snapshot")
    };
    let mut all = vec![];
    for n in nodes {
        if let Some(p) = n.postings {
            let mut codes = vec![];
            for id in &p.ids {
                codes.extend_from_slice(
                    Code::<1>::quantize(&source_value(*id), &n.routing.centroid).as_ref(),
                )
            }
            assert_eq!(p.codes, codes);
            all.extend(p.ids)
        }
    }
    all.sort_unstable();
    assert_eq!(all, (0..17).collect::<Vec<_>>())
}
#[test]
fn claimed_preparation_keeps_navigation_and_independent_reparenting_live() {
    let armed = Arc::new(AtomicBool::new(false));
    let (paused_tx, paused_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let resume = parking_lot::Mutex::new(resume_rx);
    let gate = armed.clone();
    let vectors: Arc<dyn VectorSource> = Arc::new(move |id: u64| -> Result<Arc<[f32]>, String> {
        if id == 0 && gate.swap(false, Ordering::AcqRel) {
            paused_tx.send(()).unwrap();
            resume
                .lock()
                .recv_timeout(Duration::from_secs(20))
                .map_err(|e| e.to_string())?;
        }
        Ok(source_value(id))
    });
    let (owner, cluster, root, source, other, donor) = make_fixture(vectors);
    armed.store(true, Ordering::Release);
    let running = cluster.clone();
    let worker = std::thread::spawn(move || run_split(&running, source));
    paused_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("preparation reaches vector access after claiming membership");
    let token = owner
        .resident(source)
        .unwrap()
        .read()
        .reserved
        .expect("source claim");
    let active = owner
        .handle(EngineRequest::CloseEpoch {
            run: 1908,
            epoch: 0,
        })
        .unwrap_err();
    assert!(active.contains("jobs are still active"));
    assert!(owner.ensure_save_quiescent(1908, 0).is_err());
    let nav = cluster
        .navigate(&[source_value(0).to_vec()], &cluster.write_policy(), None)
        .unwrap();
    assert_eq!(nav[0][0].0, source);
    assert!(cluster
        .transfer(source, donor, vec![0])
        .unwrap_err()
        .contains("protected"));
    assert!(cluster
        .transfer(donor, source, vec![16])
        .unwrap_err()
        .contains("protected"));
    let deferred = cluster.reassign(donor, vec![16], Some(donor.id));
    assert!(deferred.is_ok(),"a live split claim must defer useful work, not consume its no-progress budget: {deferred:?}");
    assert_eq!(
        owner.observability.snapshot().counters["ReassignmentRounds"],
        1
    );
    assert!(!cluster.jobs.lock().is_drained());
    std::thread::sleep(Duration::from_millis(5100));
    owner.ensure_healthy().unwrap();
    assert_eq!(
        owner.observability.snapshot().counters["ReassignmentRounds"],
        1
    );
    let append = owner
        .envelope("append", vec![(source.id, source.placement)])
        .unwrap();
    assert!(owner
        .handle(EngineRequest::Append {
            envelope: append,
            leaf: source,
            expected_revision: owner.node(source).unwrap().revision,
            ids: vec![17]
        })
        .is_err());
    cluster.merge_leaf(source).unwrap();
    let export = owner
        .envelope("handoff", vec![(root.id, root.placement)])
        .unwrap();
    let request = PlacementRequest::Export {
        envelope: export.clone(),
        root,
        destination: 2,
        new_placement: 1,
    };
    assert!(owner
        .handle_placement(request.clone())
        .unwrap_err()
        .contains("drained"));
    assert_eq!(
        owner.regions.read().status(export.attempt).unwrap(),
        ReservationStatus::Released
    );
    assert!(owner.handle_placement(request).is_err());
    assert_eq!(owner.regions.read().state_snapshot()["active_grants"], 0);
    run_split(&cluster, other).unwrap();
    assert_eq!(owner.resident(source).unwrap().read().reserved, Some(token));
    assert_eq!(owner.node(root).unwrap().children.len(), 4);
    cluster.split_internal(root).unwrap();
    let rebound = owner.node(source).unwrap();
    assert_ne!(rebound.parent, Some(root));
    assert!(rebound.revision > 0);
    assert_eq!(owner.resident(source).unwrap().read().reserved, Some(token));
    resume_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    let work = owner.observability.snapshot();
    assert_eq!(work.counters["SplitAttempts"], 2);
    assert_eq!(work.counters["SplitPrepared"], 2);
    assert_eq!(work.counters["SplitCommitted"], 2);
    assert_eq!(work.counters["SplitDiscarded"], 0);
    assert!(owner.node(source).is_err());
    assert_eq!(owner.regions.read().state_snapshot()["active_grants"], 0);
    exact(&owner);
    // A protected proposal stays pending instead of being dropped; after the
    // source is replaced it navigates to a current destination and completes.
    assert_eq!(
        owner
            .resident(donor)
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids,
        vec![16]
    );
    cluster.balance(4).unwrap();
    if let Ok(resident) = owner.resident(donor) {
        assert!(
            resident.read().postings.as_ref().unwrap().ids.is_empty(),
            "the retained ID must leave its source"
        );
    }
    exact(&owner);
}
#[test]
fn a_failed_real_split_releases_its_claim_and_keeps_job_failure_visible() {
    let fail = Arc::new(AtomicBool::new(false));
    let gate = fail.clone();
    let vectors: Arc<dyn VectorSource> = Arc::new(move |id: u64| -> Result<Arc<[f32]>, String> {
        if id == 0 && gate.load(Ordering::Acquire) {
            return Err("injected immutable-vector read failure".into());
        }
        Ok(source_value(id))
    });
    let (owner, cluster, _, source, _, _) = make_fixture(vectors);
    fail.store(true, Ordering::Release);
    let error = run_split(&cluster, source).unwrap_err();
    assert!(error.contains("injected immutable-vector"));
    let resident = owner.resident(source).unwrap();
    let node = resident.read();
    assert_eq!(node.reserved, None);
    assert_eq!(
        node.postings.as_ref().unwrap().ids,
        (0..8).collect::<Vec<_>>()
    );
    drop(node);
    assert!(owner.ensure_healthy().is_err());
    assert_eq!(owner.observability.snapshot().counters["SplitPrepared"], 0);
}
#[test]
fn claim_release_is_nonblocking_atomic_and_token_specific() {
    let (owner, _, _, source, other, _) =
        make_fixture(Arc::new(|id: u64| -> Result<Arc<[f32]>, String> {
            Ok(source_value(id))
        }));
    let first = owner.claim_source_leaf(source).unwrap().unwrap();
    assert!(owner.claim_source_leaf(source).unwrap().is_none());
    let independent = owner.claim_source_leaf(other).unwrap().unwrap();
    drop(independent);
    assert_eq!(owner.resident(other).unwrap().read().reserved, None);
    let replacement = owner.attempt().unwrap();
    first.resident.write().reserved = Some(replacement);
    drop(first);
    assert_eq!(
        owner.resident(source).unwrap().read().reserved,
        Some(replacement)
    );
    owner.resident(source).unwrap().write().reserved = None;
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _claim = owner.claim_source_leaf(source).unwrap().unwrap();
        panic!("injected preparation unwind")
    }));
    assert!(unwind.is_err());
    assert_eq!(owner.resident(source).unwrap().read().reserved, None);
    exact(&owner);
}

#[test]
fn deferred_cross_owner_dependency_preserves_ids_without_starving_other_jobs() {
    let (owner, _, _, source, other, donor) =
        make_fixture(Arc::new(|id: u64| -> Result<Arc<[f32]>, String> {
            Ok(source_value(id))
        }));
    let peer = NodeRef {
        authority: 2,
        ..source
    };
    let mut queue = JobQueue::default();
    let balance = |node: NodeRef| BalanceJob {
        envelope: owner.envelope("balance-job", vec![(node.id, 0)]).unwrap(),
        node,
        depth: 0,
        internal: false,
    };
    queue.enqueue(balance(peer));
    let held = queue.next(2).unwrap();
    let deferred = ReassignJob {
        envelope: owner.envelope("reassign-job", vec![(donor.id, 0)]).unwrap(),
        source: donor,
        ids: vec![16],
        excluded: Some(donor.id),
        blocked: vec![peer.id],
    };
    queue.enqueue_reassign(deferred.clone()).unwrap();
    queue.enqueue_reassign(deferred.clone()).unwrap();
    let mut changed = deferred;
    changed.ids = vec![15];
    assert!(queue
        .enqueue_reassign(changed)
        .unwrap_err()
        .contains("conflicting"));
    queue.enqueue(balance(other));
    let independent = queue.next(1).unwrap();
    assert_eq!(independent.node(), other);
    queue.finish(&independent, Ok(()));
    assert!(queue.next(1).is_none());
    assert!(!queue.is_drained());
    queue.finish(&held, Ok(()));
    let next = queue.next(1).unwrap();
    let WorkJob::Reassign(job) = &next else {
        panic!("retained reassignment")
    };
    assert_eq!(job.ids, vec![16]);
    queue.finish(&next, Ok(()));
    assert!(queue.is_drained());
}
