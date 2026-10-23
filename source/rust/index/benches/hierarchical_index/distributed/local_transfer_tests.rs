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

// A fixed endpoint fixture isolates membership movement from navigation,
// clustering and input loading. All full vectors are resident before timing.
pub(crate) struct FixedTransferFixture {
    pub(crate) owners: Vec<Arc<OwnerService>>,
    pub(crate) cluster: Arc<Cluster>,
    pub(crate) source: NodeRef,
    pub(crate) routes: BTreeMap<NodeRef, Vec<u64>>,
    pub(crate) vectors: Arc<BTreeMap<u64, Arc<[f32]>>>,
    pub(crate) untouched: BTreeMap<NodeId, PostingList>,
}
pub(crate) fn fixed_transfer_fixture(
    remote: bool,
    vectors: Arc<BTreeMap<u64, Arc<[f32]>>>,
    peers: BTreeMap<u64, super::super::transport::RpcClient>,
) -> FixedTransferFixture {
    let owners = (1..=if remote { 2 } else { 1 })
        .map(|owner| {
            let data = vectors.clone();
            Arc::new(OwnerService::new(
                owner,
                27,
                768,
                DistanceFunction::Euclidean,
                HierarchicalSpannConfig::default(),
                Arc::new(move |id: u64| -> Result<Arc<[f32]>, String> {
                    data.get(&id)
                        .cloned()
                        .ok_or_else(|| format!("missing frozen vector {id}"))
                }),
            ))
        })
        .collect::<Vec<_>>();
    let mut root = owners[0].initial_root().routing;
    root.target.id = BASE;
    root.leaf = false;
    let source = NodeRef {
        id: BASE + 1,
        authority: 1,
        placement: 0,
    };
    let mut routes = BTreeMap::new();
    let mut leaves = vec![];
    for slot in 0..11 {
        let authority = if remote && (1..9).contains(&slot) || remote && slot == 10 {
            2
        } else {
            1
        };
        let (target, centroid, ids) = if slot == 0 {
            (source, vec![10.; 768], (0..2048).collect::<Vec<u64>>())
        } else if slot <= 8 {
            let target = NodeRef {
                id: BASE + 1 + slot,
                authority,
                placement: 0,
            };
            let group = slot as u64 - 1;
            routes.insert(target, (group * 3..group * 3 + 3).collect());
            (target, vec![group as f32; 768], vec![3000 + group])
        } else {
            (
                NodeRef {
                    id: BASE + 1 + slot,
                    authority,
                    placement: 0,
                },
                vec![20.; 768],
                vec![4000 + slot as u64 - 9],
            )
        };
        let mut routing = owners[0].initial_root().routing;
        routing.target = target;
        // Ownership-unit roots have no parent inside their owner's region
        // forest. Their shared routing parent is attached after initialization.
        routing.parent = None;
        routing.centroid = centroid;
        routing.centroid_code = Code::<4>::quantize(&routing.centroid, &vec![0.; 768])
            .as_ref()
            .to_vec();
        let codes = ids
            .iter()
            .flat_map(|id| {
                Code::<1>::quantize(&vectors[id], &routing.centroid)
                    .as_ref()
                    .to_vec()
            })
            .collect();
        root.children.push(super::super::routing::Child {
            target,
            centroid: routing.centroid.clone(),
            leaf: true,
        });
        leaves.push(StoredNode {
            routing,
            postings: Some(PostingList { ids, codes }),
        });
    }
    let untouched = leaves
        .iter()
        .filter(|n| n.routing.target.id >= BASE + 10)
        .map(|n| (n.routing.target.id, n.postings.clone().unwrap()))
        .collect();
    for owner in &owners {
        owner
            .install_initial(
                leaves
                    .iter()
                    .filter(|n| n.routing.target.authority == owner.owner)
                    .cloned()
                    .collect(),
            )
            .unwrap();
        let mut published = vec![];
        for leaf in leaves
            .iter()
            .filter(|node| node.routing.target.authority == owner.owner)
        {
            let handle = owner.resident(leaf.routing.target).unwrap();
            let mut node = handle.write();
            Arc::make_mut(&mut node.routing).parent = Some(root.target);
            published.push((*node.routing).clone());
        }
        owner.publish_images(published, &[]).unwrap();
    }
    let mut raw = Cluster::new(owners[0].clone(), peers, root.clone());
    // The shared root and leaf-sized ownership units are already fixed; this
    // fixture never navigates or changes their placement.
    raw.publication = super::super::protocol::publication::Publication::new(
        super::super::construction::BoundaryView {
            navigation_topology: None,
            root: root.clone(),
            shared: BTreeMap::from([(root.target.id, root.clone())]),
            units: leaves
                .iter()
                .map(|n| (n.routing.target.id, n.routing.target))
                .collect(),
        }
        .prepared()
        .unwrap(),
        1_000_000,
    );
    raw.deadline = std::time::Duration::from_secs(2);
    let cluster = Arc::new(raw);
    owners[0].bind_cluster(&cluster);
    FixedTransferFixture {
        owners,
        cluster,
        source,
        routes,
        vectors,
        untouched,
    }
}
pub(crate) fn verify_fixed_transfer(f: &FixedTransferFixture) -> BTreeMap<NodeId, PostingList> {
    let mut postings = BTreeMap::new();
    let mut all = BTreeSet::new();
    for owner in &f.owners {
        let EngineResponse::Snapshot(nodes) = owner.handle(EngineRequest::Snapshot).unwrap() else {
            panic!("snapshot")
        };
        for node in nodes {
            let p = node.postings.unwrap();
            for id in &p.ids {
                assert!(all.insert(*id), "duplicate current membership {id}");
            }
            let codes = p
                .ids
                .iter()
                .flat_map(|id| {
                    Code::<1>::quantize(&f.vectors[id], &node.routing.centroid)
                        .as_ref()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            assert_eq!(p.codes, codes);
            postings.insert(node.routing.target.id, p);
        }
    }
    assert_eq!(all, f.vectors.keys().copied().collect());
    assert_eq!(postings[&f.source.id].ids, (24..2048).collect::<Vec<u64>>());
    for (target, ids) in &f.routes {
        let group = (target.id - BASE - 2) as u64;
        let mut expected = vec![3000 + group];
        expected.extend(ids);
        assert_eq!(postings[&target.id].ids, expected);
    }
    for (id, before) in &f.untouched {
        assert_eq!(postings[id], *before);
    }
    postings
}
pub(crate) fn fixed_work_delta(
    before: &super::super::observability::WorkSnapshot,
    after: &super::super::observability::WorkSnapshot,
) -> serde_json::Value {
    let counters = after
        .counters
        .iter()
        .map(|(k, v)| (k.clone(), v - before.counters[k]))
        .collect::<BTreeMap<_, _>>();
    let scopes = after.inclusive_scopes.iter().filter_map(|(k,v)| {
        let old = &before.inclusive_scopes[k];
        let calls = v.calls - old.calls;
        (calls > 0).then(|| (k.clone(), serde_json::json!({
            "calls": calls,
            "inclusive_wall_seconds": (v.wall_nanos - old.wall_nanos) as f64 / 1e9,
            "inclusive_thread_cpu_seconds": ((v.thread_cpu_supported_calls - old.thread_cpu_supported_calls) > 0)
                .then_some((v.thread_cpu_nanos - old.thread_cpu_nanos) as f64 / 1e9),
            "thread_cpu_supported_calls": v.thread_cpu_supported_calls - old.thread_cpu_supported_calls,
        })))
    }).collect::<BTreeMap<_,_>>();
    serde_json::json!({"counters":counters,"inclusive_scopes":scopes})
}
#[test]
fn fixed_transfer_eight_remote_groups_isolates_protocol_from_local_bulk() {
    use super::super::transport::{RpcClient, RpcServer, TransportConfig};
    let mut vectors = BTreeMap::new();
    for id in 0..2048u64 {
        vectors.insert(
            id,
            vec![if id < 24 { (id / 3) as f32 } else { 10. }; 768].into(),
        );
    }
    for group in 0..8u64 {
        vectors.insert(3000 + group, vec![group as f32; 768].into());
    }
    for id in 4000..4002u64 {
        vectors.insert(id, vec![20.; 768].into());
    }
    let vectors = Arc::new(vectors);
    // Build both complete inputs and codes before opening either measured section.
    let mut staged = fixed_transfer_fixture(true, vectors.clone(), BTreeMap::new());
    let local = fixed_transfer_fixture(false, vectors, BTreeMap::new());
    let peer = staged.owners[1].clone();
    let kinds = Arc::new(parking_lot::Mutex::new(BTreeMap::<String, u64>::new()));
    let recorded = kinds.clone();
    let config = TransportConfig {
        max_operation_time: std::time::Duration::from_secs(2),
        ..Default::default()
    };
    let mut server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "fixed-transfer".into(),
        config.clone(),
        Arc::new(move |request: EngineRequest| {
            let kind = match &request {
                EngineRequest::Node { .. } => "Node",
                EngineRequest::PrepareTransfer { .. } => "PrepareTransfer",
                EngineRequest::TransferStep { .. } => "TransferStep",
                _ => "Other",
            };
            *recorded.lock().entry(kind.into()).or_default() += 1;
            peer.handle(request)
        }),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "fixed-transfer".into(),
        config,
    )
    .unwrap();
    let mut connected = Cluster::new(
        staged.owners[0].clone(),
        BTreeMap::from([(2, client)]),
        staged.cluster.current_view().value.root.clone(),
    );
    connected.publication = super::super::protocol::publication::Publication::new(
        staged.cluster.current_view().value.clone(),
        1_000_000,
    );
    connected.deadline = std::time::Duration::from_secs(2);
    staged.cluster = Arc::new(connected);
    staged.owners[0].bind_cluster(&staged.cluster);
    // Fixed eligibility is checked here, not by timed navigation or a cache load.
    for (target, ids) in &staged.routes {
        let dest = staged.owners[1].node(*target).unwrap();
        for id in ids {
            assert!(
                DistanceFunction::Euclidean.distance(&staged.vectors[id], &dest.centroid)
                    < DistanceFunction::Euclidean.distance(&staged.vectors[id], &vec![10.; 768])
            );
        }
    }
    let before = staged
        .owners
        .iter()
        .map(|o| o.observability.snapshot())
        .collect::<Vec<_>>();
    let wire_before = staged.cluster.peers[&2].counters();
    let started = std::time::Instant::now();
    for (target, ids) in &staged.routes {
        staged
            .cluster
            .transfer(staged.source, *target, ids.clone())
            .unwrap();
    }
    let staged_seconds = started.elapsed().as_secs_f64();
    let wire_after = staged.cluster.peers[&2].counters();
    let after = staged
        .owners
        .iter()
        .map(|o| o.observability.snapshot())
        .collect::<Vec<_>>();
    let source_work = fixed_work_delta(&before[0], &after[0]);
    let peer_work = fixed_work_delta(&before[1], &after[1]);
    assert_eq!(source_work["counters"]["TransferProtocolCalls"], 88);
    assert_eq!(source_work["counters"]["TransferCompactions"], 8);
    assert_eq!(source_work["counters"]["RemoteEngineCalls"], 56);
    assert_eq!(source_work["counters"]["NavigationCalls"], 0);
    assert_eq!(
        *kinds.lock(),
        BTreeMap::from([
            ("Node".into(), 8),
            ("PrepareTransfer".into(), 8),
            ("TransferStep".into(), 40)
        ])
    );
    assert_eq!(wire_after.messages_sent - wire_before.messages_sent, 56);
    assert_eq!(
        wire_after.messages_received - wire_before.messages_received,
        56
    );
    assert_eq!(
        wire_after.failed_connections - wire_before.failed_connections,
        0
    );
    let expected = verify_fixed_transfer(&staged);
    let before_local = local.owners[0].observability.snapshot();
    let started = std::time::Instant::now();
    let moved = local.owners[0]
        .transfer_local_many(local.source, local.routes.clone(), false)
        .unwrap();
    let local_seconds = started.elapsed().as_secs_f64();
    let after_local = local.owners[0].observability.snapshot();
    let local_work = fixed_work_delta(&before_local, &after_local);
    assert_eq!(moved.moved, 24);
    assert!(moved.retries.is_empty());
    assert_eq!(local_work["counters"]["TransferProtocolCalls"], 0);
    assert_eq!(local_work["counters"]["TransferCompactions"], 1);
    assert_eq!(local_work["counters"]["NavigationCalls"], 0);
    assert_eq!(verify_fixed_transfer(&local), expected);
    let report = serde_json::json!({
        "fixture": {"source_rows":2048,"destinations":8,"rows_per_destination":3,"dimensions":768,"moved_rows":24,"untouched_leaves":2,"absolute_runner_bound_seconds":30},
        "staged": {"wall_seconds":staged_seconds,"source":source_work,"peer":peer_work,
            "remote_request_kinds":*kinds.lock(),
            "client_framed_bytes_sent":wire_after.bytes_sent-wire_before.bytes_sent,
            "client_framed_bytes_received":wire_after.bytes_received-wire_before.bytes_received,
            "client_framed_bytes_total":wire_after.bytes_sent-wire_before.bytes_sent+wire_after.bytes_received-wire_before.bytes_received,
            "remote_protocol_calls":48,"remote_node_lookups":8,"remote_calls_total":56},
        "local_bulk": {"wall_seconds":local_seconds,"work":local_work,"framed_bytes":0},
        "validation":"all2058IDs exactly once; complete codes recomputed; source remaining IDs and existing destination prefixes checked; two nonparticipant leaves unchanged; endpoints drain before snapshots; local/staged posting maps equal",
        "scope":"one fixed native loopback fixture; setup/fullvector resident loading/initial quantization/eligibility checks/exact verification outside timed sections; no navigation, splits, caching or size sweep; inclusive CPU unavailable is null, not zero; transport CPU is not isolated; no physicalnetwork/endtoend speedup claim"
    });
    eprintln!(
        "FIXED_TRANSFER_DIAGNOSTIC {}",
        serde_json::to_string(&report).unwrap()
    );
    if let Ok(output) = std::env::var("SPANN_FIXED_TRANSFER_DIAGNOSTIC_OUTPUT") {
        std::fs::write(output, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
    server.shutdown();
}
