//! Real TCP handler integration on loopback. These tests do not claim separate
//! machines; remote benchmark admission additionally requires the two-host run.
use super::super::super::config::HierarchicalSpannConfig;
use super::super::engine::{PostingList, VectorSource};
use super::super::routing::Child;
use super::super::transport::{RpcClient, RpcServer, TransportConfig};
use super::*;
use chroma_distance::DistanceFunction;
use chroma_index::quantization::Code;

struct Fixture {
    one: Arc<OwnerService>,
    two: Arc<OwnerService>,
    cluster: Arc<Cluster>,
    servers: Vec<RpcServer>,
    _remote: Arc<Cluster>,
    root: NodeRef,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for server in &mut self.servers {
            server.shutdown();
        }
    }
}
fn fixture() -> Fixture {
    let vectors: Arc<dyn VectorSource> = Arc::new(|id: u64| -> Result<Arc<[f32]>, String> {
        Ok(vec![if id < 55 { 0.0 } else { 10.0 }; 8].into())
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
    let base = 1u128 << 80;
    let target = |id| NodeRef {
        id: base + id,
        authority: 1,
        placement: 0,
    };
    let root = target(1);
    let leaf = |id, center: f32, ids: Vec<u64>| {
        let centroid = vec![center; 8];
        let mut codes = vec![];
        for id in &ids {
            codes.extend_from_slice(
                Code::<1>::quantize(&vectors.get(*id).unwrap(), &centroid).as_ref(),
            );
        }
        StoredNode {
            routing: RoutingNode {
                navigation: Default::default(),
                target: target(id),
                parent: Some(root),
                centroid: centroid.clone(),
                centroid_code: Code::<4>::quantize(&centroid, &[0.; 8]).as_ref().to_vec(),
                children: vec![],
                leaf: true,
                revision: 3,
            },
            postings: Some(PostingList { ids, codes }),
        }
    };
    let a = leaf(2, 0.0, (0..55).collect());
    let b = leaf(3, 10.0, (55..100).collect());
    let image = RoutingNode {
        navigation: Default::default(),
        target: root,
        parent: None,
        centroid: vec![5.; 8],
        centroid_code: Code::<4>::quantize(&[5.; 8], &[0.; 8]).as_ref().to_vec(),
        children: vec![
            Child {
                target: a.routing.target,
                centroid: a.routing.centroid.clone(),
                leaf: true,
            },
            Child {
                target: b.routing.target,
                centroid: b.routing.centroid.clone(),
                leaf: true,
            },
        ],
        leaf: false,
        revision: 2,
    };
    one.install_initial(vec![
        StoredNode {
            routing: image.clone(),
            postings: None,
        },
        a,
        b,
    ])
    .unwrap();
    let mut servers = vec![];
    for owner in [&one, &two] {
        let service = owner.clone();
        servers.push(
            RpcServer::bind(
                "127.0.0.1:0".parse().unwrap(),
                "127.0.0.1:0".parse().unwrap(),
                "placement-fixture".into(),
                TransportConfig::default(),
                Arc::new(move |request: EngineRequest| service.handle(request)),
            )
            .unwrap(),
        );
    }
    let clients = servers
        .iter()
        .map(|server| {
            RpcClient::new(
                server.data_addr(),
                server.control_addr(),
                "placement-fixture".into(),
                TransportConfig::default(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let cluster = Arc::new(Cluster::new(
        one.clone(),
        BTreeMap::from([(2, clients[1].clone())]),
        image.clone(),
    ));
    one.bind_cluster(&cluster);
    let remote = Arc::new(Cluster::new(
        two.clone(),
        BTreeMap::from([(1, clients[0].clone())]),
        image,
    ));
    two.bind_cluster(&remote);
    // The remote runtime must outlive its weak owner binding.

    Fixture {
        one,
        two,
        cluster,
        servers,
        _remote: remote,
        root,
    }
}

fn membership(f: &Fixture) -> Vec<u64> {
    let mut all = vec![];
    for owner in [&f.one, &f.two] {
        let EngineResponse::Membership(leaves) = owner.handle(EngineRequest::Membership).unwrap()
        else {
            panic!()
        };
        for (_, ids) in leaves {
            all.extend(ids)
        }
    }
    all.sort_unstable();
    all
}
#[test]
fn initially_one_owner_refines_then_moves_a_real_branch_over_tcp() {
    let f = fixture();
    let before = membership(&f);
    let plan = f
        .cluster
        .redistribute(BTreeMap::from([(1, 80), (2, 80)]))
        .unwrap();
    assert_eq!(plan.promoted_shared, vec![f.root.id]);
    assert_eq!(plan.moves.len(), 1);
    assert_eq!(plan.final_vectors.values().sum::<u64>(), 100);
    assert!(plan.final_vectors.values().all(|n| *n >= 30));
    assert_eq!(membership(&f), before);
    assert_eq!(before, (0..100).collect::<Vec<_>>());
    let view = f.cluster.publication.view();
    assert_eq!(view.value.units.len(), 2);
    assert!(view.value.shared.contains_key(&f.root.id));
    assert!(!f.one.nodes.contains_key(&f.root.id));
    let moved = &plan.moves[0];
    assert!(!f.one.nodes.contains_key(&moved.root.id));
    let moved_ref = NodeRef {
        authority: 2,
        placement: moved.new_placement,
        ..moved.root
    };
    assert_eq!(f.two.node(moved_ref).unwrap().target, moved_ref);
    assert!(f.one.node(moved.root).is_err());
    assert!(f.two.node(moved.root).is_err());
    let PlacementResponse::Statistics(source_statistics) = f
        .cluster
        .placement_request(1, PlacementRequest::Statistics { run: 99 })
        .unwrap()
    else {
        panic!("expected source placement statistics")
    };
    assert_eq!(source_statistics.completed_outgoing_attempts, 1);
    assert_eq!(source_statistics.completed_outgoing_vectors, moved.vectors);
    assert!(source_statistics.completed_outgoing_payload_bytes > 0);
    assert_eq!(source_statistics.pending_capacity.unwrap().vectors, 0);
    assert_eq!(
        source_statistics.assigned_capacity.unwrap().vectors,
        plan.final_vectors[&1]
    );
    let destination_statistics = f.two.placement_statistics();
    assert_eq!(destination_statistics.completed_outgoing_attempts, 0);
    assert_eq!(
        destination_statistics.assigned_capacity.unwrap().vectors,
        plan.final_vectors[&2]
    );
    assert!(f
        .cluster
        .placement_request(1, PlacementRequest::Statistics { run: 100 })
        .is_err());
    for owner in [&f.one, &f.two] {
        owner.placement.lock().ensure_save_quiescent().unwrap();
        let EngineResponse::Snapshot(nodes) = owner.handle(EngineRequest::Snapshot).unwrap() else {
            panic!()
        };
        for node in nodes {
            if let Some(p) = node.postings {
                let mut expected = vec![];
                for id in p.ids {
                    expected.extend_from_slice(
                        Code::<1>::quantize(
                            &owner.vectors.get(id).unwrap(),
                            &node.routing.centroid,
                        )
                        .as_ref(),
                    );
                }
                assert_eq!(p.codes, expected);
            }
        }
    }
}
#[test]
fn staged_transfer_rejects_early_activation_cleanup_and_conflicting_chunks_then_aborts_safely() {
    let f = fixture();
    for owner in [1, 2] {
        f.cluster
            .placement_request(
                owner,
                PlacementRequest::Configure {
                    run: 99,
                    vector_capacity: 200,
                    byte_capacity: 1_000_000,
                },
            )
            .unwrap();
    }
    let envelope = f
        .one
        .envelope("manual-handoff", vec![(f.root.id, 0)])
        .unwrap();
    let PlacementResponse::Exported(payload) = f
        .cluster
        .placement_request(
            1,
            PlacementRequest::Export {
                envelope: envelope.clone(),
                root: f.root,
                destination: 2,
                new_placement: 1,
            },
        )
        .unwrap()
    else {
        panic!()
    };
    assert!(f
        .two
        .handle_placement(PlacementRequest::Activate {
            envelope: envelope.clone(),
            payload: payload.clone(),
            sequence: 1
        })
        .is_err());
    assert!(f.two.nodes.is_empty());
    assert!(f
        .one
        .handle_placement(PlacementRequest::Cleanup {
            envelope: envelope.clone(),
            payload: payload.clone(),
            sequence: 1
        })
        .is_err());
    assert_eq!(f.one.nodes.len(), 3);
    let nodes = f.two.placement.lock().records[&envelope.attempt]
        .incoming
        .values()
        .cloned()
        .collect::<Vec<_>>();
    f.two
        .handle_placement(PlacementRequest::Chunk {
            envelope: envelope.clone(),
            payload: payload.clone(),
            index: 0,
            nodes: nodes.clone(),
        })
        .unwrap();
    let mut changed = nodes;
    changed[0].routing.revision += 1;
    assert!(f
        .two
        .handle_placement(PlacementRequest::Chunk {
            envelope: envelope.clone(),
            payload: payload.clone(),
            index: 0,
            nodes: changed
        })
        .is_err());
    // Caller-supplied cancellation is insufficient until the atomic coordinator
    // arbitration records it. No timeout guesses that publication did not happen.
    assert!(f
        .one
        .handle_placement(PlacementRequest::Abort {
            envelope: envelope.clone(),
            payload: payload.clone(),
            outcome: PublicationStatus::Aborted
        })
        .is_err());
    f.cluster.publication.abort(envelope.attempt).unwrap();
    for owner in [1, 2] {
        f.cluster
            .placement_request(
                owner,
                PlacementRequest::Abort {
                    envelope: envelope.clone(),
                    payload: payload.clone(),
                    outcome: PublicationStatus::Aborted,
                },
            )
            .unwrap();
    }
    assert_eq!(membership(&f), (0..100).collect::<Vec<_>>());
    assert!(f.two.nodes.is_empty());
    for owner in [&f.one, &f.two] {
        owner.placement.lock().ensure_save_quiescent().unwrap();
        owner
            .handle(EngineRequest::CloseEpoch { run: 99, epoch: 0 })
            .unwrap();
        owner
            .handle(EngineRequest::SealEpoch { run: 99, epoch: 0 })
            .unwrap();
    }
    assert!(f
        .two
        .handle_placement(PlacementRequest::Stage {
            envelope,
            payload,
            chunks: 1
        })
        .is_err());
}

#[test]
fn rejected_capacity_is_terminal_and_duplicate_rejection_remains_fenced() {
    let f = fixture();
    f.cluster
        .placement_request(
            2,
            PlacementRequest::Configure {
                run: 99,
                vector_capacity: 1,
                byte_capacity: 100,
            },
        )
        .unwrap();
    let e = f
        .one
        .envelope("capacity-rejection", vec![(f.root.id, 0)])
        .unwrap();
    let payload = HandoffPayload {
        root: f.root.id,
        source: 1,
        destination: 2,
        old_placement: 0,
        new_placement: 1,
        vectors: 100,
        bytes: 1000,
        manifest_digest: [5; 32],
    };
    let command = PlacementRequest::Stage {
        envelope: e.clone(),
        payload: payload.clone(),
        chunks: 1,
    };
    let first = f.two.handle_placement(command.clone()).unwrap_err();
    assert_eq!(f.two.handle_placement(command).unwrap_err(), first);
    assert!(f.two.nodes.is_empty());
    f.two.placement.lock().ensure_save_quiescent().unwrap();
    f.two
        .handle(EngineRequest::CloseEpoch { run: 99, epoch: 0 })
        .unwrap();
    f.two
        .handle(EngineRequest::SealEpoch { run: 99, epoch: 0 })
        .unwrap();
    assert!(f
        .two
        .handle_placement(PlacementRequest::Stage {
            envelope: e,
            payload,
            chunks: 1
        })
        .is_err());
}

#[test]
fn whole_tree_move_updates_root_children_and_late_activation_cannot_erase_new_adds() {
    let f = fixture();
    for owner in [1, 2] {
        f.cluster
            .placement_request(
                owner,
                PlacementRequest::Configure {
                    run: 99,
                    vector_capacity: 200,
                    byte_capacity: 1_000_000,
                },
            )
            .unwrap();
    }
    let e = f.one.envelope("whole-tree", vec![(f.root.id, 0)]).unwrap();
    f.cluster.handoff(e.clone(), f.root, 2, 1).unwrap();
    let payload = f.one.placement.lock().records[&e.attempt]
        .record
        .payload
        .clone();
    let view = f.cluster.publication.view();
    assert_eq!(view.value.root.target.authority, 2);
    assert!(view
        .value
        .root
        .children
        .iter()
        .all(|c| c.target.authority == 2 && c.target.placement == 1));
    let destination = view.value.root.children[0].target;
    let image = f.two.node(destination).unwrap();
    let append = f
        .two
        .envelope("new-add-after-handoff", vec![(destination.id, 1)])
        .unwrap();
    f.two
        .handle(EngineRequest::Append {
            envelope: append,
            leaf: destination,
            expected_revision: image.revision,
            ids: vec![100],
        })
        .unwrap();
    let published_sequence = view.sequence;
    std::thread::scope(|scope| {
        let mut jobs = vec![];
        for _ in 0..8 {
            let endpoint = &f.two;
            let envelope = e.clone();
            let payload = payload.clone();
            jobs.push(scope.spawn(move || {
                endpoint.handle_placement(PlacementRequest::Activate {
                    envelope,
                    payload,
                    sequence: published_sequence,
                })
            }));
        }
        for job in jobs {
            assert!(matches!(
                job.join().unwrap().unwrap(),
                PlacementResponse::State(HandoffState::Complete)
            ));
        }
    });
    let EngineResponse::Membership(leaves) = f.two.handle(EngineRequest::Membership).unwrap()
    else {
        panic!()
    };
    assert_eq!(
        leaves
            .iter()
            .flat_map(|(_, ids)| ids)
            .filter(|id| **id == 100)
            .count(),
        1
    );
    let selected = f
        .cluster
        .navigate(
            &[vec![0.; 8]],
            &super::super::super::common::ReadBeamPolicy::uniform(None, 1, 1),
            None,
        )
        .unwrap();
    assert_eq!(selected[0][0].0, destination);
}

#[test]
fn concurrent_initial_activation_serializes_one_install_before_completion() {
    let f = fixture();
    for owner in [1, 2] {
        f.cluster
            .placement_request(
                owner,
                PlacementRequest::Configure {
                    run: 99,
                    vector_capacity: 200,
                    byte_capacity: 1_000_000,
                },
            )
            .unwrap();
    }
    let e = f
        .one
        .envelope("concurrent-activate", vec![(f.root.id, 0)])
        .unwrap();
    let before = f.cluster.publication.view();
    let mut next = before.value.clone();
    next.root = f.one.node(f.root).unwrap();
    next.root.target.authority = 2;
    next.root.target.placement = 1;
    for child in &mut next.root.children {
        child.target.authority = 2;
        child.target.placement = 1;
    }
    next.units.insert(f.root.id, next.root.target);
    f.cluster
        .publication
        .prepare(
            e.attempt,
            PublicationProposal {
                expected_sequence: before.sequence,
                required_owners: [1, 2].into_iter().collect(),
                value: next,
            },
        )
        .unwrap();
    f.cluster.publication.acquire_turn(e.attempt).unwrap();
    let PlacementResponse::Exported(payload) = f
        .cluster
        .placement_request(
            1,
            PlacementRequest::Export {
                envelope: e.clone(),
                root: f.root,
                destination: 2,
                new_placement: 1,
            },
        )
        .unwrap()
    else {
        panic!()
    };
    f.cluster
        .placement_request(
            1,
            PlacementRequest::Fence {
                envelope: e.clone(),
                payload: payload.clone(),
            },
        )
        .unwrap();
    for owner in [1, 2] {
        f.cluster.publication.ready(e.attempt, owner).unwrap();
    }
    f.cluster.publication.publish(e.attempt).unwrap();
    let published = f.cluster.publication.view();
    f.cluster.broadcast_view(&published).unwrap();
    let sequence = published.sequence;
    let barrier = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        let mut jobs = vec![];
        for _ in 0..8 {
            let barrier = &barrier;
            let cluster = &f.cluster;
            let envelope = e.clone();
            let payload = payload.clone();
            jobs.push(scope.spawn(move || {
                barrier.wait();
                cluster.placement_request(
                    2,
                    PlacementRequest::Activate {
                        envelope,
                        payload,
                        sequence,
                    },
                )
            }));
        }
        for job in jobs {
            assert!(matches!(
                job.join().unwrap().unwrap(),
                PlacementResponse::State(HandoffState::DestinationActive)
            ));
        }
    });
    assert_eq!(f.two.nodes.len(), 3);
    f.cluster
        .placement_request(
            1,
            PlacementRequest::Cleanup {
                envelope: e.clone(),
                payload: payload.clone(),
                sequence,
            },
        )
        .unwrap();
    f.cluster
        .placement_request(
            2,
            PlacementRequest::Complete {
                envelope: e.clone(),
                payload: payload.clone(),
            },
        )
        .unwrap();
    f.cluster.publication.release_turn(e.attempt).unwrap();
    let destination = published.value.root.children[0].target;
    let image = f.two.node(destination).unwrap();
    let append = f
        .two
        .envelope(
            "append-after-concurrent-activation",
            vec![(destination.id, 1)],
        )
        .unwrap();
    f.two
        .handle(EngineRequest::Append {
            envelope: append,
            leaf: destination,
            expected_revision: image.revision,
            ids: vec![100],
        })
        .unwrap();
    assert!(matches!(
        f.cluster
            .placement_request(
                2,
                PlacementRequest::Activate {
                    envelope: e,
                    payload,
                    sequence
                }
            )
            .unwrap(),
        PlacementResponse::State(HandoffState::Complete)
    ));
    let EngineResponse::Membership(leaves) = f.two.handle(EngineRequest::Membership).unwrap()
    else {
        panic!()
    };
    assert_eq!(
        leaves
            .iter()
            .flat_map(|(_, ids)| ids)
            .filter(|id| **id == 100)
            .count(),
        1
    );
}
