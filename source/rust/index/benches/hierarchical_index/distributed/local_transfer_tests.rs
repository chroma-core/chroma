use super::super::super::config::HierarchicalSpannConfig;
use super::super::engine::{EngineRequest, EngineResponse, StoredNode, VectorSource};
use super::*;
use chroma_distance::DistanceFunction;
const BASE: u128 = 1 << 90;
fn point(id: u64) -> Arc<[f32]> {
    vec![
        match id {
            0 => 1.,
            1 => 9.,
            2 => 0.,
            3 => 10.,
            _ => 5.,
        };
        8
    ]
    .into()
}
fn fixture(vectors: Arc<dyn VectorSource>) -> (Arc<OwnerService>, Arc<Cluster>, Vec<NodeRef>) {
    let owner = Arc::new(OwnerService::new(
        1,
        708,
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig {
            split_threshold: 100,
            branching_factor: 2,
            merge_threshold: 0,
            write_beam_min: 8,
            write_beam_max: 8,
            ..Default::default()
        },
        vectors,
    ));
    let mut root = owner.initial_root();
    root.routing.target = NodeRef {
        id: BASE,
        authority: 1,
        placement: 0,
    };
    let parent = root.routing.target;
    let mut nodes = vec![];
    let mut refs = vec![];
    for (slot, centroid, ids) in [(1, 5., vec![0, 1, 2, 3]), (2, 0., vec![]), (3, 10., vec![])] {
        let target = NodeRef {
            id: BASE + slot,
            authority: 1,
            placement: 0,
        };
        let ids: Vec<u64> = ids;
        let mut codes = vec![];
        for id in &ids {
            codes.extend_from_slice(Code::<1>::quantize(&point(*id), &vec![centroid; 8]).as_ref());
        }
        let mut image = root.routing.clone();
        image.target = target;
        image.parent = Some(parent);
        image.centroid = vec![centroid; 8];
        image.centroid_code = Code::<4>::quantize(&image.centroid, &vec![0.; 8])
            .as_ref()
            .to_vec();
        image.leaf = true;
        image.children.clear();
        nodes.push(StoredNode {
            routing: image,
            postings: Some(PostingList { ids, codes }),
        });
        refs.push(target);
    }
    root.routing.leaf = false;
    root.routing.children = nodes
        .iter()
        .map(|n| super::super::routing::Child {
            target: n.routing.target,
            centroid: n.routing.centroid.clone(),
            leaf: true,
        })
        .collect();
    root.postings = None;
    let image = root.routing.clone();
    nodes.push(root);
    owner.install_initial(nodes).unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), image));
    owner.bind_cluster(&cluster);
    (owner, cluster, refs)
}
fn standard() -> (Arc<OwnerService>, Arc<Cluster>, Vec<NodeRef>) {
    fixture(Arc::new(|id: u64| -> Result<Arc<[f32]>, String> {
        Ok(point(id))
    }))
}
fn exact(owner: &OwnerService) {
    let EngineResponse::Snapshot(nodes) = owner.handle(EngineRequest::Snapshot).unwrap() else {
        panic!("snapshot")
    };
    let mut ids = vec![];
    for n in nodes {
        if let Some(p) = n.postings {
            let expected = p
                .ids
                .iter()
                .flat_map(|id| {
                    Code::<1>::quantize(&point(*id), &n.routing.centroid)
                        .as_ref()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            assert_eq!(p.codes, expected);
            ids.extend(p.ids)
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2, 3]);
}
#[test]
fn many_local_targets_deduplicate_and_compact_source_once_without_protocol() {
    let (owner, _, r) = standard();
    let before = owner.observability.snapshot();
    let result = owner
        .transfer_local_many(
            r[0],
            BTreeMap::from([(r[1], vec![0, 2, 0]), (r[2], vec![0, 1, 3])]),
            false,
        )
        .unwrap();
    assert_eq!(result.moved, 4);
    assert!(result.retries.is_empty());
    exact(&owner);
    let after = owner.observability.snapshot();
    assert_eq!(
        after.counters["TransferCompactions"] - before.counters["TransferCompactions"],
        1
    );
    assert_eq!(
        after.counters["TransferSourceEntriesScanned"]
            - before.counters["TransferSourceEntriesScanned"],
        4
    );
    assert_eq!(
        after.counters["TransferProtocolCalls"],
        before.counters["TransferProtocolCalls"]
    );
    assert_eq!(
        owner
            .resident(r[1])
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids,
        vec![0, 2]
    );
}
#[test]
fn current_centroids_revalidate_eligibility_before_forced_evacuation() {
    let (owner, _, r) = standard();
    let handle = owner.resident(r[1]).unwrap();
    {
        let mut n = handle.write();
        Arc::make_mut(&mut n.routing).centroid = vec![100.; 8];
    }
    let result = owner
        .transfer_local_many(r[0], BTreeMap::from([(r[1], vec![0])]), false)
        .unwrap();
    assert_eq!(result.moved, 0);
    assert!(result.retries.is_empty());
    assert_eq!(
        owner
            .resident(r[0])
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids,
        vec![0, 1, 2, 3]
    );
    let result = owner
        .transfer_local_many(r[0], BTreeMap::from([(r[1], vec![0])]), true)
        .unwrap();
    assert_eq!(result.moved, 1);
    exact(&owner);
}
#[test]
fn split_claimed_target_retries_current_ids_and_keeps_other_targets_live() {
    let (owner, _, r) = standard();
    let handle = owner.resident(r[1]).unwrap();
    let token = owner.attempt().unwrap();
    {
        let mut n = handle.write();
        n.reserved = Some(token);
        n.preparing_split = true;
    }
    let result = owner
        .transfer_local_many(
            r[0],
            BTreeMap::from([(r[1], vec![0, 999]), (r[2], vec![1, 3])]),
            false,
        )
        .unwrap();
    assert_eq!(result.moved, 2);
    assert_eq!(result.retries[&r[1]], vec![0]);
    assert!(result.blocked.contains(&r[1].id));
    assert_eq!(handle.read().reserved, Some(token));
    {
        let mut n = handle.write();
        n.reserved = None;
        n.preparing_split = false;
    }
    owner
        .transfer_local_many(r[0], result.retries, false)
        .unwrap();
    exact(&owner);
}
#[test]
fn vector_failure_before_mutation_releases_all_local_claims() {
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gate = armed.clone();
    let (owner, _, r) = fixture(Arc::new(move |id: u64| -> Result<Arc<[f32]>, String> {
        if gate.load(Ordering::Acquire) {
            Err("injected vector failure".into())
        } else {
            Ok(point(id))
        }
    }));
    armed.store(true, Ordering::Release);
    assert!(owner
        .transfer_local_many(r[0], BTreeMap::from([(r[1], vec![0])]), true)
        .is_err());
    owner.ensure_healthy().unwrap();
    for n in r {
        let h = owner.resident(n).unwrap();
        assert_eq!(h.read().reserved, None);
    }
    exact(&owner);
}
#[test]
fn failure_after_destination_append_poison_fences_save() {
    let (owner, _, r) = standard();
    FAIL_AFTER_APPEND.with(|v| v.set(true));
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        owner.transfer_local_many(r[0], BTreeMap::from([(r[1], vec![0])]), true)
    }));
    assert!(failed.is_err());
    assert!(owner.ensure_healthy().is_err());
    owner
        .handle(EngineRequest::CloseEpoch { run: 708, epoch: 0 })
        .unwrap();
    assert!(owner
        .handle(EngineRequest::SealEpoch { run: 708, epoch: 0 })
        .is_err());
    assert!(owner.ensure_save_quiescent(708, 0).is_err());
    assert!(owner.resident(r[0]).unwrap().read().reserved.is_some());
}
#[test]
fn reciprocal_local_callers_never_wait_with_one_leaf_locked() {
    let (owner, cluster, r) = standard();
    cluster.transfer(r[0], r[1], vec![0, 2]).unwrap();
    cluster.transfer(r[0], r[2], vec![1, 3]).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut workers = vec![];
    for (source, dest, id) in [(r[1], r[2], 0), (r[2], r[1], 1)] {
        let owner = owner.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..128 {
                let result = owner
                    .transfer_local_many(source, BTreeMap::from([(dest, vec![id])]), true)
                    .unwrap();
                if result.retries.is_empty() {
                    return;
                }
                std::thread::yield_now();
            }
            panic!("local reciprocal progress")
        }));
    }
    barrier.wait();
    for w in workers {
        w.join().unwrap();
    }
    exact(&owner);
}

#[test]
fn grouped_reassignment_uses_one_local_batch_for_two_destinations() {
    let (owner, cluster, r) = standard();
    let before = owner.observability.snapshot();
    cluster.reassign(r[0], vec![0, 1, 2, 3], None).unwrap();
    let after = owner.observability.snapshot();
    assert_eq!(
        after.counters["LocalTransferBatches"] - before.counters["LocalTransferBatches"],
        1
    );
    assert_eq!(
        after.counters["LocalTransferTargetGroups"] - before.counters["LocalTransferTargetGroups"],
        2
    );
    assert_eq!(
        after.counters["TransferCompactions"] - before.counters["TransferCompactions"],
        1
    );
    assert_eq!(
        after.counters["TransferProtocolCalls"],
        before.counters["TransferProtocolCalls"]
    );
    assert!(owner
        .resident(r[0])
        .unwrap()
        .read()
        .postings
        .as_ref()
        .unwrap()
        .ids
        .is_empty());
    exact(&owner);
}
#[test]
fn parent_replacement_during_local_preparation_preserves_claimed_membership() {
    use std::sync::mpsc;
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gate = armed.clone();
    let (paused_tx, paused_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let resume = parking_lot::Mutex::new(resume_rx);
    let (owner, cluster, r) = fixture(Arc::new(move |id: u64| -> Result<Arc<[f32]>, String> {
        if id == 0 && gate.swap(false, Ordering::AcqRel) {
            paused_tx.send(()).unwrap();
            resume
                .lock()
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
        }
        Ok(point(id))
    }));
    let root = owner.node(r[0]).unwrap().parent.unwrap();
    armed.store(true, Ordering::Release);
    let source = r[0];
    let groups = BTreeMap::from([(r[1], vec![0, 2]), (r[2], vec![1, 3])]);
    let running = owner.clone();
    let worker = std::thread::spawn(move || running.transfer_local_many(source, groups, false));
    paused_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    let token = owner.resident(source).unwrap().read().reserved.unwrap();
    let job = super::super::construction::BalanceJob {
        envelope: owner
            .envelope("balance-job", vec![(root.id, root.placement)])
            .unwrap(),
        node: root,
        depth: 0,
        internal: true,
    };
    cluster.request(1, EngineRequest::BalanceJob(job)).unwrap();
    assert_ne!(owner.node(source).unwrap().parent, Some(root));
    assert_eq!(owner.resident(source).unwrap().read().reserved, Some(token));
    resume_tx.send(()).unwrap();
    assert_eq!(worker.join().unwrap().unwrap().moved, 4);
    exact(&owner);
    assert!(owner.ensure_healthy().is_ok());
}

#[test]
fn commit_publication_precedes_a_concurrent_parent_replacement() {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    let (owner, cluster, r) = standard();
    let root = owner.node(r[0]).unwrap().parent.unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let running = owner.clone();
    let source = r[0];
    let groups = BTreeMap::from([(r[1], vec![0, 2]), (r[2], vec![1, 3])]);
    let transfer = std::thread::spawn(move || {
        BEFORE_PUBLICATION.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                ready_tx.send(()).unwrap();
                resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }))
        });
        running.transfer_local_many(source, groups, false)
    });
    ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (other_tx, other_rx) = mpsc::channel();
    let run = cluster.clone();
    let job = super::super::construction::BalanceJob {
        envelope: owner
            .envelope("balance-job", vec![(root.id, root.placement)])
            .unwrap(),
        node: root,
        depth: 0,
        internal: true,
    };
    let parent = std::thread::spawn(move || {
        let result = run.request(1, EngineRequest::BalanceJob(job));
        other_tx.send(result).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        assert!(
            other_rx.try_recv().is_err(),
            "parent replacement must wait for the claimed commit publication"
        );
        if owner.regions.read().state_snapshot()["active_grants"] > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "parent replacement reaches its structural region"
        );
        std::thread::yield_now();
    }
    assert_eq!(owner.node(source).unwrap().parent, Some(root));
    resume_tx.send(()).unwrap();
    assert_eq!(transfer.join().unwrap().unwrap().moved, 4);
    other_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    parent.join().unwrap();
    for node in r {
        let h = owner.resident(node).unwrap();
        let resident = h.read();
        let image = owner.node(node).unwrap();
        assert_eq!(resident.routing.parent, image.parent);
        assert_eq!(resident.routing.revision, image.revision);
        assert_ne!(image.parent, Some(root));
    }
    exact(&owner);
}
