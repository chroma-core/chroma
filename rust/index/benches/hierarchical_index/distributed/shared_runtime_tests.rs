//! Fixed-tree tests run real owner RPC handlers on loopback. Separate-machine
//! execution remains a campaign gate; these assertions cover shared edit logic.
use super::super::super::config::HierarchicalSpannConfig;
use super::super::engine::{PostingList, StoredNode, VectorSource};
use super::super::transport::{RpcClient, RpcServer, TransportConfig};
use super::*;
use chroma_distance::DistanceFunction;
struct Fixture {
    one: Arc<OwnerService>,
    two: Arc<OwnerService>,
    cluster: Arc<Cluster>,
    _remote: Arc<Cluster>,
    servers: Vec<RpcServer>,
    root: NodeRef,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for server in &mut self.servers {
            server.shutdown();
        }
    }
}
type ReplyHook = Arc<dyn Fn(&EngineRequest) + Send + Sync>;
fn fixture(leaves: u64) -> Fixture {
    fixture_with_reply_hook(leaves, None)
}
fn fixture_with_reply_hook(leaves: u64, reply_hook: Option<ReplyHook>) -> Fixture {
    fixture_with_hooks(leaves, reply_hook, None)
}
fn fixture_with_hooks(
    leaves: u64,
    reply_hook: Option<ReplyHook>,
    request_hook: Option<ReplyHook>,
) -> Fixture {
    let vectors: Arc<dyn VectorSource> =
        Arc::new(|id: u64| -> Result<Arc<[f32]>, String> { Ok(vec![id as f32; 8].into()) });
    let config = HierarchicalSpannConfig {
        branching_factor: 2,
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
    let base = 1u128 << 81;
    let root = NodeRef {
        id: base + 1,
        authority: 1,
        placement: 0,
    };
    let mut nodes = vec![];
    let mut children = vec![];
    for id in 0..leaves {
        let centroid = vec![id as f32; 8];
        let target = NodeRef {
            id: base + 2 + id as u128,
            authority: 1,
            placement: 0,
        };
        let image = RoutingNode {
            navigation: Default::default(),
            target,
            parent: Some(root),
            centroid: centroid.clone(),
            centroid_code: Code::<4>::quantize(&centroid, &[0.; 8]).as_ref().to_vec(),
            children: vec![],
            leaf: true,
            revision: 0,
        };
        children.push(child(&image));
        nodes.push(StoredNode {
            routing: image,
            postings: Some(PostingList {
                ids: vec![id],
                codes: Code::<1>::quantize(&vectors.get(id).unwrap(), &centroid)
                    .as_ref()
                    .to_vec(),
            }),
        });
    }
    let mut image = RoutingNode {
        navigation: Default::default(),
        target: root,
        parent: None,
        centroid: vec![0.; 8],
        centroid_code: vec![],
        children,
        leaf: false,
        revision: 0,
    };
    center(&mut image, 8).unwrap();
    nodes.push(StoredNode {
        routing: image.clone(),
        postings: None,
    });
    one.install_initial(nodes).unwrap();
    let mut servers = vec![];
    for owner in [&one, &two] {
        let service = owner.clone();
        let reply_hook = reply_hook.clone();
        let request_hook = request_hook.clone();
        servers.push(
            RpcServer::bind(
                "127.0.0.1:0".parse().unwrap(),
                "127.0.0.1:0".parse().unwrap(),
                "shared-fixture".into(),
                TransportConfig::default(),
                Arc::new(move |r: EngineRequest| {
                    let captured = r.clone();
                    if service.owner == 2 {
                        if let Some(hook) = &request_hook {
                            hook(&captured);
                        }
                    }
                    let result = service.handle(r);
                    if service.owner == 2 {
                        if let Some(hook) = &reply_hook {
                            hook(&captured);
                        }
                    }
                    result
                }),
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
                "shared-fixture".into(),
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
    Fixture {
        one,
        two,
        cluster,
        _remote: remote,
        servers,
        root,
    }
}
fn promote_root(f: &Fixture) {
    let before = f.cluster.publication.view();
    let image = f.one.node(f.root).unwrap();
    let mut next = before.value.clone();
    next.root = image.clone();
    next.shared.insert(image.target.id, image.clone());
    next.units = image
        .children
        .iter()
        .map(|c| (c.target.id, c.target))
        .collect();
    let e = f.one.envelope("fixture-promotion", vec![]).unwrap();
    f.cluster
        .publication
        .prepare(
            e.attempt,
            PublicationProposal {
                expected_sequence: before.sequence,
                required_owners: [1].into_iter().collect(),
                value: next.prepared().unwrap(),
            },
        )
        .unwrap();
    f.cluster.publication.acquire_turn(e.attempt).unwrap();
    f.cluster
        .placement_request(
            1,
            PlacementRequest::ProtectPromotion {
                envelope: e.clone(),
                nodes: vec![f.root.id],
            },
        )
        .unwrap();
    f.cluster.publication.ready(e.attempt, 1).unwrap();
    f.cluster.publication.publish(e.attempt).unwrap();
    f.cluster
        .broadcast_view(&f.cluster.publication.view())
        .unwrap();
    f.cluster
        .placement_request(
            1,
            PlacementRequest::Promote {
                envelope: e.clone(),
                nodes: vec![f.root.id],
            },
        )
        .unwrap();
    f.cluster.publication.release_turn(e.attempt).unwrap();
}
fn membership(f: &Fixture) -> Vec<u64> {
    let mut ids = vec![];
    for owner in [&f.one, &f.two] {
        let EngineResponse::Membership(leaves) = owner.handle(EngineRequest::Membership).unwrap()
        else {
            panic!()
        };
        for (_, part) in leaves {
            ids.extend(part);
        }
    }
    ids.sort_unstable();
    ids
}
#[test]
fn shared_overflow_adds_levels_preserving_two_owner_postings_and_parent_links() {
    let f = fixture(8);
    let plan = f
        .cluster
        .redistribute(BTreeMap::from([(1, 6), (2, 6)]))
        .unwrap();
    assert_eq!(plan.final_vectors[&1], 4);
    assert_eq!(plan.final_vectors[&2], 4);
    let before = f.cluster.publication.view();
    let units = before.value.units.clone();
    let original = f.root;
    f.cluster.balance_shared_internal(original).unwrap();
    let after = f.cluster.publication.view();
    assert_ne!(after.value.root.target, original);
    assert!(!after.value.shared.contains_key(&original.id));
    assert_eq!(after.value.units, units);
    assert!(after.value.shared.len() >= 3);
    for _ in 0..32 {
        let target = f
            .cluster
            .publication
            .view()
            .value
            .shared
            .values()
            .find(|n| n.children.len() > 2)
            .map(|n| n.target);
        let Some(target) = target else { break };
        f.cluster.balance_shared_internal(target).unwrap();
    }
    let after = f.cluster.publication.view();
    assert!(after.value.shared.values().all(|n| n.children.len() <= 2));
    validate_upper(&after.value).unwrap();
    assert_eq!(membership(&f), (0..8).collect::<Vec<_>>());
    for unit in after.value.units.values() {
        let actual = f.cluster.node(*unit).unwrap();
        let parent = actual
            .parent
            .expect("owned root must link to shared parent");
        assert!(after.value.shared[&parent.id]
            .children
            .iter()
            .any(|c| c.target == *unit));
    }
    for owner in [&f.one, &f.two] {
        owner.shared_state.lock().ensure_quiescent().unwrap();
    }
}
#[test]
fn unary_shared_root_collapses_to_owned_root_with_same_identity_and_postings() {
    let f = fixture(1);
    promote_root(&f);
    let old = f.cluster.publication.view();
    let owned = *old.value.units.values().next().unwrap();
    let previous = f.one.node(owned).unwrap();
    assert_eq!(previous.parent, Some(f.root));
    f.cluster.balance_shared_internal(f.root).unwrap();
    let view = f.cluster.publication.view();
    assert_eq!(view.value.root.target, owned);
    assert!(view.value.shared.is_empty());
    assert_eq!(view.value.units, old.value.units);
    let current = f.one.node(owned).unwrap();
    assert_eq!(current.parent, None);
    assert_eq!(current.revision, previous.revision + 1);
    assert_eq!(membership(&f), vec![0]);
}
#[test]
fn parent_prepare_blocks_edits_and_only_actual_abort_releases_it_then_epoch_fences_replay() {
    let f = fixture(2);
    promote_root(&f);
    let unit = *f
        .cluster
        .publication
        .view()
        .value
        .units
        .values()
        .next()
        .unwrap();
    let image = f.one.node(unit).unwrap();
    let e = f
        .one
        .envelope("shared-abort-test", vec![(unit.id, unit.placement)])
        .unwrap();
    let updates = vec![ParentUpdate {
        root: unit,
        expected_revision: image.revision,
        expected_parent: image.parent,
        new_parent: Some(NodeRef {
            id: 99,
            authority: 1,
            placement: 0,
        }),
    }];
    let barrier = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        let mut jobs = vec![];
        for _ in 0..4 {
            let barrier = &barrier;
            let endpoint = &f.one;
            let envelope = e.clone();
            let updates = updates.clone();
            jobs.push(scope.spawn(move || {
                barrier.wait();
                endpoint.handle_shared(SharedRequest::Prepare { envelope, updates })
            }));
        }
        for job in jobs {
            assert_eq!(job.join().unwrap().unwrap(), SharedResponse::Ready);
        }
    });
    assert!(f.one.shared_state.lock().ensure_quiescent().is_err());
    assert!(f
        .one
        .handle_shared(SharedRequest::Finish {
            envelope: e.clone(),
            updates: updates.clone()
        })
        .is_err());
    assert_eq!(f.one.node(unit).unwrap().parent, image.parent);
    f.cluster.publication.abort(e.attempt).unwrap();
    assert_eq!(
        f.one
            .handle_shared(SharedRequest::Finish {
                envelope: e.clone(),
                updates: updates.clone()
            })
            .unwrap(),
        SharedResponse::Aborted
    );
    assert_eq!(f.one.node(unit).unwrap().revision, image.revision);
    f.one.shared_state.lock().ensure_quiescent().unwrap();
    f.one
        .handle(EngineRequest::CloseEpoch { run: 101, epoch: 0 })
        .unwrap();
    f.one
        .handle(EngineRequest::SealEpoch { run: 101, epoch: 0 })
        .unwrap();
    assert!(f
        .one
        .handle_shared(SharedRequest::Prepare {
            envelope: e,
            updates
        })
        .is_err());
}

#[test]
fn completed_owned_centroid_edits_refresh_shared_scoring_and_duplicate_requests_keep_their_result()
{
    let f = fixture(8);
    f.cluster
        .redistribute(BTreeMap::from([(1, 6), (2, 6)]))
        .unwrap();
    let before = f.cluster.publication.view();
    let unit = *before
        .value
        .units
        .values()
        .find(|u| u.authority == 2)
        .unwrap();
    let edit = |value: f32| {
        let manager = f.two.regions.read().clone();
        let attempt = f.two.attempt().unwrap();
        let request = RegionRequest {
            roots: vec![unit.id],
            placements: vec![(unit.id, unit.placement)],
            topology_sequence: manager.sequence(),
        };
        assert_eq!(
            manager.request(attempt, request).unwrap(),
            ReservationStatus::Granted
        );
        let handle = f.two.resident(unit).unwrap();
        let mut record = handle.write();
        let mut image = (*record.routing).clone();
        image.centroid = vec![value; 8];
        image.centroid_code = Code::<4>::quantize(&image.centroid, &[0.; 8])
            .as_ref()
            .to_vec();
        revision(&mut image).unwrap();
        let posting = record.postings.as_mut().unwrap();
        posting.codes.clear();
        for id in &posting.ids {
            posting.codes.extend_from_slice(
                Code::<1>::quantize(&f.two.vectors.get(*id).unwrap(), &image.centroid).as_ref(),
            );
        }
        record.routing = Arc::new(image.clone());
        f.two.publish_images(vec![image], &[]).unwrap();
        drop(record);
        manager.release(attempt).unwrap();
    };
    edit(17.);
    let e = f
        .two
        .envelope("explicit-centroid-refresh", vec![(unit.id, unit.placement)])
        .unwrap();
    let first = f
        .cluster
        .shared_request(
            1,
            SharedRequest::RefreshCentroid {
                envelope: e.clone(),
                root: unit,
            },
        )
        .unwrap();
    let SharedResponse::Refreshed { sequence } = first else {
        panic!()
    };
    assert!(sequence > before.sequence);
    let published = f.cluster.publication.view();
    let parent = f.two.node(unit).unwrap().parent.unwrap();
    assert_eq!(
        published.value.shared[&parent.id]
            .children
            .iter()
            .find(|c| c.target == unit)
            .unwrap()
            .centroid,
        vec![17.; 8]
    );
    edit(18.);
    assert_eq!(
        f.cluster
            .shared_request(
                1,
                SharedRequest::RefreshCentroid {
                    envelope: e,
                    root: unit
                }
            )
            .unwrap(),
        first
    );
    assert_eq!(f.cluster.publication.view().sequence, sequence);
    f.cluster.refresh_boundary_centroid(unit).unwrap();
    let latest = f.cluster.publication.view();
    assert!(latest.sequence > sequence);
    assert_eq!(
        latest.value.shared[&parent.id]
            .children
            .iter()
            .find(|c| c.target == unit)
            .unwrap()
            .centroid,
        vec![18.; 8]
    );
    assert_eq!(membership(&f), (0..8).collect::<Vec<_>>());
}

#[test]
fn rejected_parent_prepare_keeps_its_outcome_after_topology_advances_and_can_seal() {
    let f = fixture(2);
    promote_root(&f);
    let unit = *f
        .cluster
        .publication
        .view()
        .value
        .units
        .values()
        .next()
        .unwrap();
    let image = f.one.node(unit).unwrap();
    let e = f
        .one
        .envelope("stale-parent-prepare", vec![(unit.id, unit.placement)])
        .unwrap();
    let updates = vec![ParentUpdate {
        root: unit,
        expected_revision: image.revision + 1,
        expected_parent: image.parent,
        new_parent: None,
    }];
    let command = SharedRequest::Prepare {
        envelope: e.clone(),
        updates,
    };
    let error = f.one.handle_shared(command.clone()).unwrap_err();
    assert!(error.contains("dependencies changed"));
    let manager = f.one.regions.read().clone();
    let other = f.one.attempt().unwrap();
    let request = RegionRequest {
        roots: vec![unit.id],
        placements: vec![(unit.id, unit.placement)],
        topology_sequence: manager.sequence(),
    };
    assert_eq!(
        manager.request(other, request).unwrap(),
        ReservationStatus::Granted
    );
    let mut control = manager.snapshot(unit.id).unwrap().0;
    control.revision += 1;
    manager.update(other, unit.id, control).unwrap();
    manager.release(other).unwrap();
    assert_eq!(f.one.handle_shared(command).unwrap_err(), error);
    assert!(f.one.resident(unit).unwrap().read().reserved.is_none());
    f.one.shared_state.lock().ensure_quiescent().unwrap();
    f.one
        .handle(EngineRequest::CloseEpoch { run: 101, epoch: 0 })
        .unwrap();
    f.one
        .handle(EngineRequest::SealEpoch { run: 101, epoch: 0 })
        .unwrap();
}

#[test]
fn duplicate_waiting_on_execution_rechecks_sealed_epoch_before_cached_refresh() {
    let f = fixture(2);
    let e = f
        .one
        .envelope("waiting-refresh-duplicate", vec![(f.root.id, 0)])
        .unwrap();
    let command = SharedRequest::RefreshCentroid {
        envelope: e.clone(),
        root: f.root,
    };
    assert!(matches!(
        f.one.handle_shared(command.clone()).unwrap(),
        SharedResponse::Refreshed { .. }
    ));
    let execution = f.one.shared_state.lock().execution[&e.attempt].clone();
    let held = execution.lock();
    std::thread::scope(|scope| {
        let owner = &f.one;
        let waiter = scope.spawn(move || owner.handle_shared(command));
        let deadline = Instant::now() + Duration::from_secs(2);
        // The handler's extra Arc proves it passed the first phase check, looked up
        // this retained control, and is waiting for the held execution mutex.
        while Arc::strong_count(&execution) < 3 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let waiting = Arc::strong_count(&execution) >= 3;
        if waiting {
            f.one
                .handle(EngineRequest::CloseEpoch { run: 101, epoch: 0 })
                .unwrap();
            f.one
                .handle(EngineRequest::SealEpoch { run: 101, epoch: 0 })
                .unwrap();
        }
        drop(held);
        let result = waiter.join().unwrap();
        assert!(waiting, "duplicate never reached the execution wait");
        let error = result.unwrap_err();
        assert!(error.contains("Sealed"), "{error}");
    });
}

#[test]
fn centroid_returning_to_cached_value_waits_for_inflight_refresh_before_noop() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    let armed = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicUsize::new(0));
    let (captured_tx, captured_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let resume_rx = Arc::new(Mutex::new(resume_rx));
    let hook = {
        let armed = armed.clone();
        let reads = reads.clone();
        Arc::new(move |request: &EngineRequest| {
            if matches!(request, EngineRequest::Node { .. })
                && armed.load(Ordering::Acquire)
                && reads.fetch_add(1, Ordering::AcqRel) == 1
            {
                // The response already contains B. Keep the publication turn
                // occupied while a later owner edit restores A.
                captured_tx.send(()).unwrap();
                resume_rx
                    .lock()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
        }) as ReplyHook
    };
    let f = fixture_with_reply_hook(8, Some(hook));
    f.cluster
        .redistribute(BTreeMap::from([(1, 6), (2, 6)]))
        .unwrap();
    let unit = *f
        .cluster
        .publication
        .view()
        .value
        .units
        .values()
        .find(|u| u.authority == 2)
        .unwrap();
    let original = f.two.node(unit).unwrap();
    // This fixed-tree seam edits an owned leaf image directly. It isolates the
    // publication ordering race; native balancing stress covers internal-root
    // centroid edits and must establish attribution for an observed campaign.
    let edit = |centroid: Vec<f32>| {
        let handle = f.two.resident(unit).unwrap();
        let mut resident = handle.write();
        let mut image = (*resident.routing).clone();
        image.centroid = centroid;
        image.centroid_code = Code::<4>::quantize(&image.centroid, &[0.; 8])
            .as_ref()
            .to_vec();
        revision(&mut image).unwrap();
        let posting = resident.postings.as_mut().unwrap();
        posting.codes.clear();
        for id in &posting.ids {
            posting.codes.extend_from_slice(
                Code::<1>::quantize(&f.two.vectors.get(*id).unwrap(), &image.centroid).as_ref(),
            );
        }
        resident.routing = Arc::new(image.clone());
        f.two.publish_images(vec![image], &[]).unwrap();
    };
    edit(vec![17.; 8]);
    armed.store(true, Ordering::Release);
    std::thread::scope(|scope| {
        let first = scope.spawn(|| f.cluster.refresh_boundary_centroid(unit));
        captured_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        edit(original.centroid.clone());
        let (done_tx, done_rx) = mpsc::channel();
        let cluster = &f.cluster;
        let second = scope.spawn(move || {
            let result = cluster.refresh_boundary_centroid(unit);
            done_tx.send(()).unwrap();
            result
        });
        let completed_while_first_held_turn =
            done_rx.recv_timeout(Duration::from_millis(100)).is_ok();
        resume_tx.send(()).unwrap();
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        assert!(
            !completed_while_first_held_turn,
            "a matching cached centroid bypassed the in-flight publication turn"
        );
    });
    let latest = f.cluster.publication.view();
    let parent = f.two.node(unit).unwrap().parent.unwrap();
    assert_eq!(
        latest.value.shared[&parent.id]
            .children
            .iter()
            .find(|c| c.target == unit)
            .unwrap()
            .centroid,
        original.centroid
    );
}

// A local structural edit releases its region before refreshing the shared row.
// Shared reparenting must bind its new row to the same current image it protects.
#[test]
fn shared_reparent_copies_current_owned_centroid_instead_of_stale_shared_row() {
    let f = fixture(8);
    f.cluster
        .redistribute(BTreeMap::from([(1, 6), (2, 6)]))
        .unwrap();
    let unit = *f
        .cluster
        .current_view()
        .value
        .units
        .values()
        .find(|u| u.authority == 2)
        .unwrap();
    let handle = f.two.resident(unit).unwrap();
    let mut resident = handle.write();
    let mut image = (*resident.routing).clone();
    image.centroid = vec![73.; 8];
    image.centroid_code = Code::<4>::quantize(&image.centroid, &[0.; 8])
        .as_ref()
        .to_vec();
    revision(&mut image).unwrap();
    let posting = resident.postings.as_mut().unwrap();
    posting.codes = posting
        .ids
        .iter()
        .flat_map(|id| {
            Code::<1>::quantize(&f.two.vectors.get(*id).unwrap(), &image.centroid)
                .as_ref()
                .to_vec()
        })
        .collect();
    resident.routing = Arc::new(image.clone());
    f.two.publish_images(vec![image.clone()], &[]).unwrap();
    drop(resident);
    // Invoke the real shared split while the owner's post-edit refresh is pending.
    f.cluster.balance_shared_internal(f.root).unwrap();
    let view = f.cluster.current_view();
    let current = f.two.node(unit).unwrap();
    assert_ne!(current.parent, Some(f.root));
    let row = view.value.shared[&current.parent.unwrap().id]
        .children
        .iter()
        .find(|c| c.target == unit)
        .unwrap();
    assert_eq!(
        row.centroid, image.centroid,
        "new shared parent published a stale owned-root centroid"
    );
    assert_eq!(membership(&f), (0..8).collect::<Vec<_>>());
}

#[test]
fn boundary_refresh_waits_for_published_reparent_to_finish() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    let armed = Arc::new(AtomicBool::new(false));
    let (captured_tx, captured_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let resume_rx = Arc::new(Mutex::new(resume_rx));
    let hook = {
        let armed = armed.clone();
        Arc::new(move |request: &EngineRequest| {
            if matches!(request, EngineRequest::Shared(SharedRequest::Finish { .. }))
                && armed.swap(false, Ordering::AcqRel)
            {
                captured_tx.send(()).unwrap();
                resume_rx
                    .lock()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
        }) as ReplyHook
    };
    let f = fixture_with_hooks(8, None, Some(hook));
    f.cluster
        .redistribute(BTreeMap::from([(1, 6), (2, 6)]))
        .unwrap();
    let unit = *f
        .cluster
        .current_view()
        .value
        .units
        .values()
        .find(|u| u.authority == 2)
        .unwrap();
    armed.store(true, Ordering::Release);
    std::thread::scope(|scope| {
        let reparent = scope.spawn(|| f.cluster.balance_shared_internal(f.root));
        captured_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let before_finish = f.two.node(unit).unwrap();
        assert!(!f
            .cluster
            .current_view()
            .value
            .shared
            .contains_key(&before_finish.parent.unwrap().id));
        let (done_tx, done_rx) = mpsc::channel();
        let cluster = &f.cluster;
        let refresh = scope.spawn(move || {
            let result = cluster.refresh_boundary_centroid(unit);
            done_tx.send(()).unwrap();
            result
        });
        let finished_early = done_rx.recv_timeout(Duration::from_millis(100)).is_ok();
        resume_tx.send(()).unwrap();
        reparent.join().unwrap().unwrap();
        refresh.join().unwrap().unwrap();
        assert!(
            !finished_early,
            "refresh completed while the published parent and owner parent were incompatible"
        );
    });
    let view = f.cluster.current_view();
    let current = f.two.node(unit).unwrap();
    assert_eq!(
        view.value.shared[&current.parent.unwrap().id]
            .children
            .iter()
            .find(|c| c.target == unit)
            .unwrap()
            .centroid,
        current.centroid
    );
}

// Diagnostic for epoch28's bounded refresh failure. Finite unrelated revision
// changes must not permanently prevent a boundary centroid from being refreshed.
#[test]
fn boundary_refresh_survives_finite_owned_revision_churn() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let edits = Arc::new(AtomicUsize::new(0));
    let target: Arc<Mutex<Option<(Arc<OwnerService>, NodeRef)>>> = Arc::new(Mutex::new(None));
    let hook = {
        let target = target.clone();
        let edits = edits.clone();
        Arc::new(move |request: &EngineRequest| {
            if matches!(request, EngineRequest::Node { .. }) {
                let state = target.lock().clone();
                if let Some((owner, unit)) = state {
                    if edits.fetch_add(1, Ordering::AcqRel) < 80 {
                        let handle = owner.resident(unit).unwrap();
                        let mut resident = handle.write();
                        let mut image = (*resident.routing).clone();
                        revision(&mut image).unwrap();
                        resident.routing = Arc::new(image.clone());
                        owner.publish_images(vec![image], &[]).unwrap();
                    }
                }
            }
        }) as ReplyHook
    };
    let f = fixture_with_reply_hook(8, Some(hook));
    f.cluster
        .redistribute(BTreeMap::from([(1, 6), (2, 6)]))
        .unwrap();
    let unit = *f
        .cluster
        .current_view()
        .value
        .units
        .values()
        .find(|u| u.authority == 2)
        .unwrap();
    *target.lock() = Some((f.two.clone(), unit));
    let result = f.cluster.refresh_boundary_centroid(unit);
    *target.lock() = None;
    assert!(
        result.is_ok(),
        "finite owned revision churn must complete a refresh: {result:?}"
    );
}

#[test]
fn boundary_refresh_gates_serialize_same_root_and_allow_distinct_roots() {
    let mut state = SharedOwnerState::default();
    let first = state.refresh_gate(1, 2).unwrap();
    let same = state.refresh_gate(1, 2).unwrap();
    let different = state.refresh_gate(2, 2).unwrap();
    assert!(Arc::ptr_eq(&first, &same));
    let held = first.lock();
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                assert!(same.try_lock().is_none());
                assert!(different.try_lock().is_some());
            })
            .join()
            .unwrap();
    });
    drop(held);
    assert!(same.try_lock().is_some());
}

#[test]
fn boundary_refresh_gate_entries_are_bounded_and_reclaimed_after_failure() {
    let mut state = SharedOwnerState::default();
    let first = state.refresh_gate(1, 1).unwrap();
    assert!(state.refresh_gate(2, 1).is_err());
    let failure = || -> Result<(), String> {
        let _guard = first.lock();
        Err("injected refresh failure".into())
    };
    assert!(failure().is_err());
    assert!(first.try_lock().is_some());
    drop(first);
    let second = state.refresh_gate(2, 1).unwrap();
    assert_eq!(state.refresh_gates.len(), 1);
    assert!(!state.refresh_gates.contains_key(&1));
    drop(second);
}

#[test]
fn boundary_refresh_gate_rechecks_fencing_and_releases_after_failure() {
    let f = fixture(8);
    f.cluster
        .redistribute(BTreeMap::from([(1, 6), (2, 6)]))
        .unwrap();
    let unit = *f
        .cluster
        .publication
        .view()
        .value
        .units
        .values()
        .find(|u| u.authority == 2)
        .unwrap();
    let valid = f
        .one
        .envelope("refresh-gate-test", vec![(unit.id, unit.placement)])
        .unwrap();
    let mut invalid = valid.clone();
    invalid.epoch += 1;
    assert!(f
        .cluster
        .refresh_boundary_centroid_with_gate(unit, &invalid, true)
        .is_err());
    f.cluster
        .refresh_boundary_centroid_with_gate(unit, &valid, true)
        .unwrap();
    assert!(f.one.shared_state.lock().refresh_gates[&unit.id]
        .upgrade()
        .is_none());
}

#[test]
fn early_refresh_waits_before_owner_snapshot_and_releases_noop_turn() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    let armed = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicUsize::new(0));
    let hook = {
        let armed = armed.clone();
        let reads = reads.clone();
        Arc::new(move |request: &EngineRequest| {
            if armed.load(Ordering::Acquire) && matches!(request, EngineRequest::Node { .. }) {
                reads.fetch_add(1, Ordering::AcqRel);
            }
        }) as ReplyHook
    };
    let f = fixture_with_reply_hook(8, Some(hook));
    f.cluster
        .redistribute(BTreeMap::from([(1, 6), (2, 6)]))
        .unwrap();
    let before = f.cluster.publication.view();
    let unit = *before
        .value
        .units
        .values()
        .find(|u| u.authority == 2)
        .unwrap();
    let blocker = f.one.envelope("test-block-publication", vec![]).unwrap();
    assert!(f
        .cluster
        .publication
        .acquire_unprepared_turn(blocker.attempt)
        .unwrap());
    let e = f
        .one
        .envelope("test-early-refresh", vec![(unit.id, unit.placement)])
        .unwrap();
    armed.store(true, Ordering::Release);
    std::thread::scope(|scope| {
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let cluster = &f.cluster;
        let task = scope.spawn(move || {
            started_tx.send(()).unwrap();
            let result = cluster.refresh_boundary_centroid_options(unit, &e, true, true);
            done_tx.send(()).unwrap();
            result
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());
        assert_eq!(
            reads.load(Ordering::Acquire),
            0,
            "snapshot lookup precedes publication permission"
        );
        f.cluster.publication.abort(blocker.attempt).unwrap();
        f.cluster.publication.release_turn(blocker.attempt).unwrap();
        assert_eq!(task.join().unwrap().unwrap(), before.sequence);
    });
    assert_eq!(f.cluster.publication.view().sequence, before.sequence);
    let next = f.one.envelope("test-after-noop", vec![]).unwrap();
    assert!(f
        .cluster
        .publication
        .acquire_unprepared_turn(next.attempt)
        .unwrap());
    f.cluster.publication.abort(next.attempt).unwrap();
    f.cluster.publication.release_turn(next.attempt).unwrap();
}

#[test]
fn early_refresh_guard_releases_failed_preparation_but_retains_published_turn() {
    use super::super::protocol::publication::Publication;
    let f = fixture(8);
    let view = f.cluster.publication.view();
    let p = Publication::new(view.value.clone(), 10);
    let first = AttemptId {
        authority: 1,
        sequence: 9001,
    };
    let second = AttemptId {
        authority: 1,
        sequence: 9002,
    };
    assert!(p.acquire_unprepared_turn(first).unwrap());
    {
        let _failure_cleanup = UnpublishedRefreshTurn {
            publication: &p,
            attempt: first,
        };
        assert_eq!(
            p.prepare(
                first,
                PublicationProposal {
                    expected_sequence: 1,
                    required_owners: BTreeSet::new(),
                    value: view.value.clone()
                }
            ),
            Err(ProtocolError::StalePlacement)
        );
    }
    assert_eq!(p.status(first).unwrap(), PublicationStatus::Aborted);
    assert!(p.acquire_unprepared_turn(second).unwrap());
    p.prepare(
        second,
        PublicationProposal {
            expected_sequence: 0,
            required_owners: BTreeSet::new(),
            value: view.value.clone(),
        },
    )
    .unwrap();
    {
        let _broadcast_failure = UnpublishedRefreshTurn {
            publication: &p,
            attempt: second,
        };
        assert_eq!(
            p.publish(second).unwrap(),
            PublicationStatus::Published { sequence: 1 }
        );
    }
    assert!(!p
        .acquire_unprepared_turn(AttemptId {
            authority: 1,
            sequence: 9003
        })
        .unwrap());
    assert_eq!(p.view().sequence, 1);
    p.release_turn(second).unwrap();
}

#[test]
fn early_refresh_turn_timeout_preserves_blocker_and_releases_root_gate() {
    let f = fixture(8);
    f.cluster
        .redistribute(BTreeMap::from([(1, 6), (2, 6)]))
        .unwrap();
    let view = f.cluster.publication.view();
    let unit = *view
        .value
        .units
        .values()
        .find(|u| u.authority == 2)
        .unwrap();
    let blocker = f.one.envelope("test-timeout-blocker", vec![]).unwrap();
    assert!(f
        .cluster
        .publication
        .acquire_unprepared_turn(blocker.attempt)
        .unwrap());
    let e = f
        .one
        .envelope("test-timeout-refresh", vec![(unit.id, unit.placement)])
        .unwrap();
    let error = f
        .cluster
        .refresh_boundary_centroid_until(
            unit,
            &e,
            true,
            true,
            Instant::now() + Duration::from_millis(20),
        )
        .unwrap_err();
    assert!(error.contains("publication turn deadline"), "{error}");
    assert!(f
        .cluster
        .publication
        .acquire_unprepared_turn(blocker.attempt)
        .unwrap());
    assert_eq!(f.cluster.publication.view().sequence, view.sequence);
    f.cluster.publication.abort(blocker.attempt).unwrap();
    f.cluster.publication.release_turn(blocker.attempt).unwrap();
    assert_eq!(
        f.cluster
            .refresh_boundary_centroid_options(unit, &e, true, true)
            .unwrap(),
        view.sequence
    );
    f.cluster.publication.reclaim_sealed().unwrap();
}
