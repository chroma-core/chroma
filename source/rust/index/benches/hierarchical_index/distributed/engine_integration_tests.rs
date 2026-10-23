//! These tests use actual TCP handlers and owner-local payloads. They establish
//! process-local integration only; the campaign separately requires two hosts.
use super::super::{common::ReadBeamPolicy, config::HierarchicalSpannConfig};
use super::engine::*;
use super::routing::*;
use super::transport::*;
use chroma_distance::DistanceFunction;
use chroma_index::quantization::Code;
use std::{collections::BTreeMap, sync::Arc};

fn reference(id: u128, owner: u64) -> NodeRef {
    NodeRef {
        id,
        authority: owner,
        placement: 0,
    }
}
fn image(
    id: u128,
    owner: u64,
    parent: Option<NodeRef>,
    leaf: bool,
    children: Vec<Child>,
    value: f32,
) -> RoutingNode {
    let centroid = vec![value; 8];
    RoutingNode {
        navigation: Default::default(),
        target: reference(id, owner),
        parent,
        centroid_code: Code::<1>::quantize(&centroid, &[0.; 8]).as_ref().to_vec(),
        centroid,
        children,
        leaf,
        revision: 0,
    }
}
fn child(node: &RoutingNode) -> Child {
    Child {
        target: node.target,
        centroid: node.centroid.clone(),
        leaf: node.leaf,
    }
}
fn leaf(node: RoutingNode, ids: Vec<u64>, vectors: &dyn VectorSource) -> StoredNode {
    let mut codes = vec![];
    for id in &ids {
        codes.extend_from_slice(
            Code::<1>::quantize(&vectors.get(*id).unwrap(), &node.centroid).as_ref(),
        );
    }
    StoredNode {
        routing: node,
        postings: Some(PostingList { ids, codes }),
    }
}
#[test]
fn tcp_global_expansion_and_cross_owner_transfer_preserve_membership_and_codes() {
    // Wide IDs expose accidental u32/u64 wire truncation. The fixture has two
    // internal owner branches so a level expansion must contact both authorities.
    let base = 1u128 << 80;
    let root_ref = reference(base, 1);
    let a_ref = reference(base + 1, 1);
    let b_ref = reference(base + 2, 2);
    let a_leaf = image(base + 3, 1, Some(a_ref), true, vec![], 0.);
    let b_leaf = image(base + 4, 2, Some(b_ref), true, vec![], 10.);
    let a = image(base + 1, 1, Some(root_ref), false, vec![child(&a_leaf)], 0.);
    let b = image(
        base + 2,
        2,
        Some(root_ref),
        false,
        vec![child(&b_leaf)],
        10.,
    );
    let root = image(base, 1, None, false, vec![child(&a), child(&b)], 5.);
    let vectors: Arc<dyn VectorSource> = Arc::new(|id: u64| -> Result<Arc<[f32]>, String> {
        match id {
            0 => Ok(vec![1.; 8].into()),
            1 => Ok(vec![9.; 8].into()),
            _ => Err("unknown immutable vector".into()),
        }
    });
    let one = Arc::new(OwnerService::new(
        1,
        99,
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
        vectors.clone(),
    ));
    let two = Arc::new(OwnerService::new(
        2,
        99,
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
        vectors.clone(),
    ));
    one.install_initial(vec![
        StoredNode {
            routing: root.clone(),
            postings: None,
        },
        StoredNode {
            routing: a,
            postings: None,
        },
        leaf(a_leaf.clone(), vec![0], vectors.as_ref()),
    ])
    .unwrap();
    two.install_initial(vec![
        StoredNode {
            routing: b,
            postings: None,
        },
        leaf(b_leaf.clone(), vec![1], vectors.as_ref()),
    ])
    .unwrap();
    let endpoint = two.clone();
    let mut server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "integration-run".into(),
        TransportConfig::default(),
        Arc::new(move |request: EngineRequest| endpoint.handle(request)),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "integration-run".into(),
        TransportConfig::default(),
    )
    .unwrap();
    let cluster = Cluster::new(one.clone(), BTreeMap::from([(2, client.clone())]), root);
    let queries = vec![vec![1.; 8], vec![9.; 8]];
    let selected = cluster
        .navigate(&queries, &ReadBeamPolicy::uniform(None, 2, 2), None)
        .unwrap();
    assert_eq!(selected[0][0].0, a_leaf.target);
    assert_eq!(selected[1][0].0, b_leaf.target);
    assert!(client.counters().messages_sent > 0);
    cluster
        .transfer(a_leaf.target, b_leaf.target, vec![0])
        .unwrap();
    let mut all = vec![];
    for service in [&one, &two] {
        match service.handle(EngineRequest::Snapshot).unwrap() {
            EngineResponse::Snapshot(nodes) => {
                for n in nodes {
                    if let Some(p) = n.postings {
                        let mut expected = vec![];
                        for id in &p.ids {
                            expected.extend_from_slice(
                                Code::<1>::quantize(
                                    &vectors.get(*id).unwrap(),
                                    &n.routing.centroid,
                                )
                                .as_ref(),
                            );
                        }
                        assert_eq!(p.codes, expected);
                        all.extend(p.ids);
                    }
                }
            }
            _ => panic!("wrong snapshot response"),
        }
    }
    all.sort_unstable();
    assert_eq!(all, vec![0, 1]);
    let EngineResponse::Membership(one_members) = one.handle(EngineRequest::Membership).unwrap()
    else {
        panic!()
    };
    assert_eq!(
        one_members.iter().map(|(_, ids)| ids.len()).sum::<usize>(),
        0
    );
    let EngineResponse::Membership(two_members) = two.handle(EngineRequest::Membership).unwrap()
    else {
        panic!()
    };
    assert_eq!(
        two_members.iter().map(|(_, ids)| ids.len()).sum::<usize>(),
        2
    );
    server.shutdown();
}

#[test]
fn tcp_construction_refines_hands_off_and_continues_on_both_owners() {
    construction_fixture(4);
}
#[test]
fn tcp_construction_with_32_workers_per_owner_preserves_membership_under_repeated_scheduling() {
    for iteration in 0..10 {
        eprintln!("32-worker construction repetition {iteration}");
        construction_fixture(32);
    }
}
fn construction_fixture(workers: usize) {
    // A low split threshold and fanout force multiple internal levels with only
    // 100 vectors. The 80-vector owner budget forces refinement before handoff.
    // This fixture tests structural transitions, not performance at this size.
    let vectors: Arc<dyn VectorSource> = Arc::new(|id: u64| -> Result<Arc<[f32]>, String> {
        Ok((0..8)
            .map(|d| (((id + 1) * (d + 13) * 7919) % 1009) as f32 / 1009.)
            .collect::<Vec<_>>()
            .into())
    });
    let config = HierarchicalSpannConfig {
        split_threshold: 8,
        merge_threshold: 2,
        branching_factor: 4,
        ..Default::default()
    };
    let one = Arc::new(OwnerService::new(
        1,
        101,
        8,
        DistanceFunction::Euclidean,
        config.clone(),
        vectors.clone(),
    ));
    let two = Arc::new(OwnerService::new(
        2,
        101,
        8,
        DistanceFunction::Euclidean,
        config,
        vectors.clone(),
    ));
    let root = one.initial_root();
    one.install_initial(vec![root.clone()]).unwrap();
    let mut servers = Vec::new();
    let mut clients = Vec::new();
    for owner in [&one, &two] {
        let endpoint = owner.clone();
        let server = RpcServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
            "construction-fixture".into(),
            TransportConfig {
                max_control_handlers: 128,
                ..Default::default()
            },
            Arc::new(move |request: EngineRequest| endpoint.handle(request)),
        )
        .unwrap();
        clients.push(
            RpcClient::new(
                server.data_addr(),
                server.control_addr(),
                "construction-fixture".into(),
                TransportConfig::default(),
            )
            .unwrap(),
        );
        servers.push(server);
    }
    let cluster = Arc::new(Cluster::new(
        one.clone(),
        BTreeMap::from([(2, clients[1].clone())]),
        root.routing.clone(),
    ));
    let peer = Arc::new(Cluster::new(
        two.clone(),
        BTreeMap::from([(1, clients[0].clone())]),
        root.routing,
    ));
    one.bind_cluster(&cluster);
    two.bind_cluster(&peer);
    cluster
        .add_batch(&(0..100).collect::<Vec<_>>(), workers)
        .expect("first add");
    if let Err(error) = cluster.balance(workers) {
        panic!(
            "first balance: {error}; server counters {:?}",
            servers.iter().map(|s| s.counters()).collect::<Vec<_>>()
        )
    }
    for owner in [&one, &two] {
        let EngineResponse::Snapshot(nodes) = owner.handle(EngineRequest::Snapshot).unwrap() else {
            panic!()
        };
        for node in nodes {
            if let Some(postings) = node.postings {
                assert!(
                    postings.ids.len() <= 8,
                    "first drained balance leaves oversized leaf {} with {} rows",
                    node.routing.target.id,
                    postings.ids.len()
                );
            } else {
                assert!(
                    node.routing.children.len() <= 4,
                    "first drained balance leaves oversized internal node {}",
                    node.routing.target.id
                );
            }
        }
    }
    let placement = cluster
        .redistribute(BTreeMap::from([(1, 80), (2, 80)]))
        .unwrap_or_else(|error| {
            let EngineResponse::Snapshot(nodes) = one.handle(EngineRequest::Snapshot).unwrap()
            else {
                panic!()
            };
            let shape = nodes
                .iter()
                .map(|n| {
                    (
                        n.routing.target.id,
                        n.routing.parent,
                        n.routing.children.len(),
                        n.postings.as_ref().map(|p| p.ids.len()),
                    )
                })
                .collect::<Vec<_>>();
            panic!("handoff: {error}; actual nodes {shape:?}")
        });
    assert!(
        !placement.moves.is_empty(),
        "the fixture must exercise real handoff"
    );
    assert!(placement.final_vectors.values().all(|n| *n > 0));
    if let Err(error) = cluster.add_batch(&(100..125).collect::<Vec<_>>(), workers) {
        panic!(
            "second add: {error}; server counters {:?}",
            servers.iter().map(|s| s.counters()).collect::<Vec<_>>()
        )
    }
    if let Err(error) = cluster.balance(workers) {
        panic!(
            "second balance: {error}; server counters {:?}",
            servers.iter().map(|s| s.counters()).collect::<Vec<_>>()
        )
    }
    assert!(cluster
        .add_batch(&[0], workers)
        .unwrap_err()
        .contains("duplicate"));
    let mut all = Vec::new();
    let view = cluster.current_view();
    let mut routing = view.value.shared.clone();
    for owner in [&one, &two] {
        let EngineResponse::Snapshot(nodes) = owner.handle(EngineRequest::Snapshot).unwrap() else {
            panic!("snapshot response")
        };
        let mut owned = 0;
        for node in nodes {
            assert!(
                node.postings
                    .as_ref()
                    .map_or(node.routing.children.len() <= 4, |p| p.ids.len() <= 8),
                "drained balance cannot lose eligible work at {}",
                node.routing.target.id
            );
            assert!(
                routing
                    .insert(node.routing.target.id, node.routing.clone())
                    .is_none(),
                "one owner stores each owned record"
            );
            if let Some(postings) = node.postings {
                let mut expected = Vec::new();
                for id in &postings.ids {
                    expected.extend_from_slice(
                        Code::<1>::quantize(&vectors.get(*id).unwrap(), &node.routing.centroid)
                            .as_ref(),
                    );
                }
                assert_eq!(postings.codes, expected);
                owned += postings.ids.len();
                all.extend(postings.ids);
            }
        }
        assert!(owned > 0, "both owners must retain real postings");
    }
    let mut reached = std::collections::BTreeSet::new();
    let mut pending = vec![view.value.root.target];
    while let Some(target) = pending.pop() {
        assert!(
            reached.insert(target.id),
            "tree contains a cycle or duplicate child"
        );
        let node = &routing[&target.id];
        assert_eq!(node.target, target);
        for child in &node.children {
            let actual = &routing[&child.target.id];
            assert_eq!(actual.parent, Some(target));
            if child.centroid != actual.centroid {
                let stamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let artifact = std::env::temp_dir().join(format!(
                    "spann-centroid-visibility-{}-{stamp}.json",
                    std::process::id()
                ));
                #[derive(serde::Serialize)]
                struct VisibilityArtifact<'a> {
                    workers_per_owner: usize,
                    published_view: &'a super::protocol::publication::PublishedView<
                        super::construction::BoundaryView,
                    >,
                    routing: &'a BTreeMap<u128, RoutingNode>,
                    parent: &'a RoutingNode,
                    cached_child: &'a Child,
                    current_child: &'a RoutingNode,
                }
                let captured = VisibilityArtifact {
                    workers_per_owner: workers,
                    published_view: &view,
                    routing: &routing,
                    parent: node,
                    cached_child: child,
                    current_child: actual,
                };
                // Direct typed serialization supports u128 node IDs. Converting
                // through Value here would hide the original visibility failure.
                std::fs::write(&artifact, serde_json::to_vec_pretty(&captured).unwrap()).unwrap();
                eprintln!(
                    "centroid visibility failure artifact: {}",
                    artifact.display()
                );
            }
            assert_eq!(
                child.centroid, actual.centroid,
                "cached scoring centroid must follow current child; parent={:?}, parent_shared={}, parent_revision={}, child={:?}, child_shared={}, child_revision={}, child_parent={:?}",
                node.target, view.value.shared.contains_key(&node.target.id), node.revision,
                child.target, view.value.shared.contains_key(&child.target.id), actual.revision, actual.parent
            );
            pending.push(child.target);
        }
    }
    assert_eq!(
        reached.len(),
        routing.len(),
        "all owned records are reachable from the logical root"
    );
    all.sort_unstable();
    assert_eq!(all, (0..125).collect::<Vec<_>>());
    for server in &mut servers {
        server.shutdown();
    }
}

/// The first destination activation commits but loses its TCP reply. The actual
/// driver must replay that exact step and finish both endpoint release barriers.
#[test]
fn tcp_transfer_driver_recovers_a_lost_activation_reply() {
    transfer_driver_fixture(true, false);
}
#[test]
fn tcp_opposite_transfer_callers_acquire_owner_bundles_in_one_order() {
    transfer_driver_fixture(false, true);
}
fn transfer_driver_fixture(drop_activation_reply: bool, reciprocal: bool) {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Barrier,
    };
    let vectors: Arc<dyn VectorSource> =
        Arc::new(|id: u64| -> Result<Arc<[f32]>, String> { Ok(vec![id as f32; 8].into()) });
    let one = Arc::new(OwnerService::new(
        1,
        202,
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
        vectors.clone(),
    ));
    let two = Arc::new(OwnerService::new(
        2,
        202,
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
        vectors.clone(),
    ));
    let a = image(1u128 << 80, 1, None, true, vec![], 0.);
    let b = image((1u128 << 80) + 1, 2, None, true, vec![], 1.);
    one.install_initial(vec![leaf(a.clone(), vec![0], vectors.as_ref())])
        .unwrap();
    two.install_initial(vec![leaf(b.clone(), vec![1], vectors.as_ref())])
        .unwrap();
    let config = TransportConfig {
        max_control_handlers: 128,
        ..Default::default()
    };
    let endpoint = one.clone();
    let mut first = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "driver-test".into(),
        config.clone(),
        Arc::new(move |q: EngineRequest| endpoint.handle(q)),
    )
    .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = dropped.clone();
    let late_packet = Arc::new(parking_lot::Mutex::new(None));
    let late = late_packet.clone();
    let endpoint = two.clone();
    let mut second = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "driver-test".into(),
        config.clone(),
        Arc::new(move |q: EngineRequest| {
            let drop_reply = drop_activation_reply
                && matches!(
                    &q,
                    EngineRequest::TransferStep {
                        role: super::protocol::transfer::Role::Destination,
                        step: super::protocol::transfer::TransferStep::ActivateDestination,
                        ..
                    }
                );
            let prepared = if drop_activation_reply {
                match &q {
                    EngineRequest::PrepareTransfer { envelope, .. } => Some(envelope.clone()),
                    _ => None,
                }
            } else {
                None
            };
            let result = endpoint.handle(q);
            if let Some(envelope) = prepared.filter(|_| result.is_ok()) {
                *late.lock() = Some(envelope);
                // Close prevents new operations while the existing transfer is
                // admitted. Seal must wait for its endpoint release barrier.
                endpoint
                    .handle(EngineRequest::CloseEpoch { run: 202, epoch: 0 })
                    .unwrap();
                assert!(endpoint
                    .handle(EngineRequest::SealEpoch { run: 202, epoch: 0 })
                    .is_err());
            }
            if drop_reply && result.is_ok() && !flag.swap(true, Ordering::AcqRel) {
                panic!("injected loss of an already committed activation acknowledgement");
            }
            result
        }),
    )
    .unwrap();
    let to_one = RpcClient::new(
        first.data_addr(),
        first.control_addr(),
        "driver-test".into(),
        config.clone(),
    )
    .unwrap();
    let to_two = RpcClient::new(
        second.data_addr(),
        second.control_addr(),
        "driver-test".into(),
        config,
    )
    .unwrap();
    let c1 = Arc::new(Cluster::new(
        one.clone(),
        BTreeMap::from([(2, to_two)]),
        a.clone(),
    ));
    let c2 = Arc::new(Cluster::new(
        two.clone(),
        BTreeMap::from([(1, to_one)]),
        a.clone(),
    ));
    one.bind_cluster(&c1);
    two.bind_cluster(&c2);
    if reciprocal {
        let start = Arc::new(Barrier::new(3));
        let ca = c1.clone();
        let gate = start.clone();
        let aa = a.target;
        let bb = b.target;
        let forward = std::thread::spawn(move || {
            gate.wait();
            ca.transfer(aa, bb, vec![0])
        });
        let cb = c2.clone();
        let gate = start.clone();
        let backward = std::thread::spawn(move || {
            gate.wait();
            cb.transfer(bb, aa, vec![1])
        });
        start.wait();
        let first_result = forward.join().unwrap();
        let second_result = backward.join().unwrap();
        // A competing transfer may invalidate the other caller's captured revision;
        // that caller receives a known rejection and replans, rather than deadlocking.
        assert!(
            first_result.is_ok() || second_result.is_ok(),
            "both transfers failed: {first_result:?}, {second_result:?}"
        );
        if first_result.is_err() {
            c1.transfer(a.target, b.target, vec![0]).unwrap();
        }
        if second_result.is_err() {
            c2.transfer(b.target, a.target, vec![1]).unwrap();
        }
    } else {
        c1.transfer(a.target, b.target, vec![0]).unwrap();
        assert!(dropped.load(Ordering::Acquire));
    }
    let mut ids = vec![];
    for owner in [&one, &two] {
        let EngineResponse::Membership(members) = owner.handle(EngineRequest::Membership).unwrap()
        else {
            panic!()
        };
        ids.extend(members.into_iter().flat_map(|(_, ids)| ids));
        owner
            .handle(EngineRequest::CloseEpoch { run: 202, epoch: 0 })
            .unwrap();
        owner
            .handle(EngineRequest::SealEpoch { run: 202, epoch: 0 })
            .unwrap();
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1]);
    if let Some(envelope) = late_packet.lock().clone() {
        assert!(two
            .handle(EngineRequest::TransferStep {
                envelope,
                role: super::protocol::transfer::Role::Destination,
                step: super::protocol::transfer::TransferStep::ActivateDestination,
                proof: Default::default()
            })
            .is_err());
    }
    first.shutdown();
    second.shutdown();
}

#[test]
fn tcp_large_posting_transfer_chunks_raw_codes_and_preserves_every_row() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dim = 4096;
    let vectors: Arc<dyn VectorSource> = Arc::new(move |id: u64| -> Result<Arc<[f32]>, String> {
        Ok((0..dim)
            .map(|d| if (d as u64 + id) % 2 == 0 { 1. } else { -1. })
            .collect::<Vec<_>>()
            .into())
    });
    let one = Arc::new(OwnerService::new(
        1,
        303,
        dim,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
        vectors.clone(),
    ));
    let two = Arc::new(OwnerService::new(
        2,
        303,
        dim,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
        vectors.clone(),
    ));
    let code_size = Code::<1>::quantize(&vectors.get(0).unwrap(), &vec![0.; dim])
        .as_ref()
        .len();
    let rows = 2 * (512 * 1024 / code_size) + 1;
    let mut a = one.initial_root();
    let mut b = two.initial_root();
    a.postings = leaf(
        a.routing.clone(),
        (0..rows as u64).collect(),
        vectors.as_ref(),
    )
    .postings;
    b.postings = leaf(b.routing.clone(), vec![rows as u64], vectors.as_ref()).postings;
    let source = a.routing.target;
    let destination = b.routing.target;
    let root = a.routing.clone();
    one.install_initial(vec![a]).unwrap();
    two.install_initial(vec![b]).unwrap();
    let endpoint = two.clone();
    let prepares = Arc::new(AtomicUsize::new(0));
    let count = prepares.clone();
    let max_frame = 3 * 1024 * 1024;
    let config = TransportConfig {
        max_frame_bytes: max_frame,
        ..Default::default()
    };
    let mut server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "chunk-test".into(),
        config.clone(),
        Arc::new(move |q: EngineRequest| {
            if matches!(&q, EngineRequest::PrepareTransfer { .. }) {
                count.fetch_add(1, Ordering::Relaxed);
                assert!(serde_json::to_vec(&q).unwrap().len() < max_frame);
            }
            endpoint.handle(q)
        }),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "chunk-test".into(),
        config,
    )
    .unwrap();
    let cluster = Cluster::new(one.clone(), BTreeMap::from([(2, client)]), root);
    cluster
        .transfer(source, destination, (0..rows as u64).collect())
        .unwrap();
    assert_eq!(prepares.load(Ordering::Relaxed), 3);
    let EngineResponse::Membership(source_rows) = one.handle(EngineRequest::Membership).unwrap()
    else {
        panic!()
    };
    assert!(source_rows.iter().all(|(_, rows)| rows.is_empty()));
    let EngineResponse::Snapshot(nodes) = two.handle(EngineRequest::Snapshot).unwrap() else {
        panic!()
    };
    let p = nodes[0].postings.as_ref().unwrap();
    let mut ids = p.ids.clone();
    ids.sort_unstable();
    assert_eq!(ids, (0..=rows as u64).collect::<Vec<_>>());
    let mut expected = vec![];
    for id in &p.ids {
        expected.extend_from_slice(
            Code::<1>::quantize(&vectors.get(*id).unwrap(), &nodes[0].routing.centroid).as_ref(),
        );
    }
    assert_eq!(p.codes, expected);
    server.shutdown();
}
