//! Exercise the actual owner preparation, coordinator publication and scheduler.
//! These are bounded in-process correctness fixtures, not distributed timings.
use super::super::super::config::HierarchicalSpannConfig;
use super::super::engine::VectorSource;
use super::*;
use chroma_distance::DistanceFunction;

const BASE: u128 = 1u128 << 80;
fn routing(id: u128, parent: Option<NodeRef>, children: Vec<Child>) -> RoutingNode {
    let centroid = vec![0.5; 17];
    RoutingNode {
        target: NodeRef {
            id,
            authority: 1,
            placement: 0,
        },
        parent,
        centroid_code: Code::<4>::quantize(&centroid, &vec![0.; 17])
            .as_ref()
            .to_vec(),
        centroid,
        leaf: children.is_empty(),
        children,
        revision: 0,
    }
}
fn postings(source: &dyn VectorSource, centroid: &[f32]) -> PostingList {
    let ids = vec![0, 1];
    let mut codes = vec![];
    for id in &ids {
        codes.extend_from_slice(Code::<1>::quantize(&source.get(*id).unwrap(), centroid).as_ref())
    }
    PostingList { ids, codes }
}
fn fixture(unary: bool) -> (Arc<OwnerService>, Arc<Cluster>) {
    let source: Arc<dyn VectorSource> = Arc::new(|id: u64| -> Result<Arc<[f32]>, String> {
        match id {
            0 => Ok(vec![0.2; 17].into()),
            1 => Ok(vec![0.8; 17].into()),
            _ => Err("unknown fixture vector".into()),
        }
    });
    let config = HierarchicalSpannConfig {
        split_threshold: 8,
        merge_threshold: 2,
        branching_factor: 4,
        ..Default::default()
    };
    let owner = Arc::new(OwnerService::new(
        1,
        908,
        17,
        DistanceFunction::Euclidean,
        config,
        source,
    ));
    let mut root = routing(BASE, None, vec![]);
    let nodes = if unary {
        let leaf = routing(BASE + 1, Some(root.target), vec![]);
        root.children = vec![child(&leaf)];
        root.leaf = false;
        vec![
            StoredNode {
                routing: root.clone(),
                postings: None,
            },
            StoredNode {
                postings: Some(postings(&*owner.vectors, &leaf.centroid)),
                routing: leaf,
            },
        ]
    } else {
        vec![StoredNode {
            postings: Some(postings(&*owner.vectors, &root.centroid)),
            routing: root.clone(),
        }]
    };
    owner.install_initial(nodes).unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), root));
    owner.bind_cluster(&cluster);
    (owner, cluster)
}
fn prepare(
    owner: &OwnerService,
    old: RoutingNode,
    id: u128,
) -> (Envelope, StructuralEdit, BoundaryPreparation) {
    let replacement = routing(id, None, vec![]);
    let envelope = owner
        .envelope(
            "boundary-split",
            vec![(old.target.id, old.target.placement)],
        )
        .unwrap();
    let request = BoundaryPreparation {
        envelope: envelope.clone(),
        old: old.clone(),
        replacements: vec![replacement.clone()],
        dependencies: vec![],
    };
    owner
        .admit_placement(&envelope, bincode::serialize(&request).unwrap())
        .unwrap();
    owner.boundaries.lock().insert(
        envelope.attempt,
        PendingBoundary {
            prepared: vec![StoredNode {
                postings: Some(postings(&*owner.vectors, &replacement.centroid)),
                routing: replacement.clone(),
            }],
            request: request.clone(),
            execution: Arc::new(parking_lot::Mutex::new(())),
            preparing: false,
            guard: None,
            ready: false,
            finished: false,
        },
    );
    let edit = StructuralEdit {
        envelope: envelope.clone(),
        source: old.target,
        expected_revision: old.revision,
        replacements: vec![replacement],
        dependencies: vec![],
    };
    (envelope, edit, request)
}
fn exact_membership(owner: &OwnerService) -> Vec<u64> {
    let response = owner.handle(EngineRequest::Snapshot).unwrap();
    let EngineResponse::Snapshot(nodes) = response else {
        panic!("snapshot reply")
    };
    let mut ids = vec![];
    for n in nodes {
        if let Some(p) = n.postings {
            let mut expected = vec![];
            for id in &p.ids {
                expected.extend_from_slice(
                    Code::<1>::quantize(&owner.vectors.get(*id).unwrap(), &n.routing.centroid)
                        .as_ref(),
                )
            }
            assert_eq!(p.codes, expected);
            ids.extend(p.ids)
        }
    }
    ids.sort_unstable();
    ids
}
#[test]
fn boundary_finish_requires_actual_publication_and_preserves_protected_source() {
    let (owner, cluster) = fixture(false);
    let old = cluster.refresh_root().unwrap();
    let (envelope, edit, request) = prepare(&owner, old.clone(), BASE + 2);
    let mut next = cluster.publication.view().value.clone();
    next.root = edit.replacements[0].clone();
    next.units = BTreeMap::from([(next.root.target.id, next.root.target)]);
    cluster
        .publication
        .prepare(
            envelope.attempt,
            super::super::protocol::publication::PublicationProposal {
                expected_sequence: 0,
                required_owners: BTreeSet::from([1]),
                value: next,
            },
        )
        .unwrap();
    assert!(cluster.publication.acquire_turn(envelope.attempt).unwrap());
    owner.boundary_ready(request).unwrap();
    cluster.publication.ready(envelope.attempt, 1).unwrap();
    let error = owner
        .handle(EngineRequest::BoundaryFinish {
            envelope: envelope.clone(),
            published: true,
        })
        .unwrap_err();
    assert!(error.contains("confirmed published"), "{error}");
    assert!(owner.nodes.contains_key(&old.target.id));
    assert_eq!(
        owner.nodes.get(&old.target.id).unwrap().read().reserved,
        Some(envelope.attempt)
    );
    {
        let pending = owner.boundaries.lock();
        let record = &pending[&envelope.attempt];
        assert!(record.ready);
        assert!(!record.finished);
        assert!(record.guard.is_some());
    }
    cluster.publication.publish(envelope.attempt).unwrap();
    cluster.install_view(&cluster.publication.view()).unwrap();
    owner
        .handle(EngineRequest::BoundaryFinish {
            envelope: envelope.clone(),
            published: true,
        })
        .unwrap();
    cluster.publication.release_turn(envelope.attempt).unwrap();
    assert!(!owner.nodes.contains_key(&old.target.id));
    assert_eq!(exact_membership(&owner), vec![0, 1]);
    let pending = owner.boundaries.lock();
    assert!(pending[&envelope.attempt].finished);
    assert!(pending[&envelope.attempt].guard.is_none());
}
#[test]
fn structural_replay_returns_original_outcome_after_later_publication() {
    let (owner, cluster) = fixture(false);
    let (_, first, _) = prepare(&owner, cluster.refresh_root().unwrap(), BASE + 2);
    let first_result = match cluster
        .request(1, EngineRequest::Structure(first.clone()))
        .unwrap()
    {
        EngineResponse::Structure(r) => r,
        _ => panic!("structure reply"),
    };
    let (_, second, _) = prepare(&owner, cluster.refresh_root().unwrap(), BASE + 3);
    let second_result = match cluster
        .request(1, EngineRequest::Structure(second))
        .unwrap()
    {
        EngineResponse::Structure(r) => r,
        _ => panic!("structure reply"),
    };
    assert!(second_result.sequence > first_result.sequence);
    let replay = match cluster
        .request(1, EngineRequest::Structure(first.clone()))
        .unwrap()
    {
        EngineResponse::Structure(r) => r,
        _ => panic!("structure reply"),
    };
    assert_eq!(replay.sequence, first_result.sequence);
    assert_eq!(replay.root, first_result.root);
    assert_eq!(cluster.publication.view().value.root, second_result.root);
    assert_eq!(cluster.current_view().value.root, second_result.root);
    let mut changed = first;
    changed.expected_revision += 1;
    let error = cluster
        .request(1, EngineRequest::Structure(changed))
        .unwrap_err();
    assert!(error.contains("conflicting structural attempt"), "{error}");
    assert_eq!(cluster.publication.view().value.root, second_result.root);
    assert_eq!(exact_membership(&owner), vec![0, 1]);
}
#[test]
fn unary_owned_global_root_collapses_without_replacing_leaf_payload() {
    let (owner, cluster) = fixture(true);
    let old = cluster.refresh_root().unwrap();
    let target = old.children[0].target;
    let before = owner.resident(target).unwrap();
    let before_postings = before.read().postings.clone().unwrap();
    cluster.balance(1).unwrap();
    let root = cluster.refresh_root().unwrap();
    assert_eq!(root.target, target);
    assert!(root.leaf);
    assert_eq!(root.parent, None);
    assert!(!owner.nodes.contains_key(&old.target.id));
    let after = owner.resident(target).unwrap();
    assert!(Arc::ptr_eq(&before, &after));
    assert_eq!(after.read().postings.as_ref().unwrap(), &before_postings);
    assert_eq!(
        cluster.publication.view().value.units,
        BTreeMap::from([(target.id, target)])
    );
    assert_eq!(owner.nodes.len(), 1);
    assert_eq!(exact_membership(&owner), vec![0, 1]);
}

#[test]
fn failed_owner_save_preserves_previous_generation_for_fresh_reader() {
    use super::super::{
        generation::GenerationStore,
        saved_reader::{SavedReader, VectorFileDescriptor},
    };
    const CHILD: &str = "SPANN_FAILED_SAVE_PREVIOUS_READER";
    if let Some(path) = std::env::var_os(CHILD) {
        let store = GenerationStore::open(path).unwrap();
        let descriptor = store.latest().unwrap().unwrap();
        assert_eq!(descriptor.generation, "complete");
        let reader = SavedReader::open(store, &descriptor).unwrap();
        assert_eq!(
            reader.validate_membership().unwrap(),
            [0, 1].into_iter().collect()
        );
        let vectors = reader.open_vector_source().unwrap().preload().unwrap();
        assert_eq!(reader.validate_codes(&vectors).unwrap(), (1, 2));
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let (owner, cluster) = fixture(false);
    let mut bytes = vec![];
    for value in [0.2f32, 0.8] {
        for _ in 0..17 {
            bytes.extend_from_slice(&value.to_le_bytes())
        }
    }
    let corpus = dir.path().join("canonical.f32");
    std::fs::write(&corpus, &bytes).unwrap();
    use sha2::{Digest, Sha256};
    let vectors = VectorFileDescriptor {
        path: corpus.to_str().unwrap().into(),
        rows: 2,
        dimensions: 17,
        stride_bytes: 17 * 4,
        offset_bytes: 0,
        first_id: 0,
        file_bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        immutable_locators: vec!["fixture:immutable-two-vectors".into()],
    };
    let saved = dir.path().join("saved");
    let descriptor = cluster
        .save_completed(&saved, vectors.clone(), "complete", "complete-save")
        .unwrap();
    let descriptor_bytes = std::fs::read(saved.join("descriptor.json")).unwrap();
    // The independently copied complete generation remains intact. Preserve the
    // owner's original files, then inject a real filesystem failure in its next
    // SaveOwner endpoint, before the coordinator can publish a new descriptor.
    std::fs::rename(saved.join("owner-1"), saved.join("owner-1-preserved")).unwrap();
    std::fs::write(saved.join("owner-1"), b"intentional failed save target").unwrap();
    assert!(cluster
        .save_completed(&saved, vectors, "failed-generation", "failed-save")
        .is_err());
    let store = GenerationStore::open(&saved).unwrap();
    assert_eq!(store.latest().unwrap(), Some(descriptor));
    assert_eq!(
        std::fs::read(saved.join("descriptor.json")).unwrap(),
        descriptor_bytes
    );
    assert!(!saved.join("generations/failed-generation.json").exists());
    drop(cluster);
    drop(owner);
    drop(store);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("failed_owner_save_preserves_previous_generation_for_fresh_reader")
        .arg("--nocapture")
        .env(CHILD, &saved)
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn dense_shallow_construction_does_not_spin_on_posting_revision_churn() {
    // Thirty-two eventual leaf groups fit below branching100. Every split's
    // 32-neighbor search can therefore revisit almost all resident leaf peers.
    // The bounded source catches repeated navigation/quantization without progress.
    use std::sync::atomic::AtomicUsize;
    const ROWS: usize = 4096;
    const DIM: usize = 32;
    let fetches = Arc::new(AtomicUsize::new(0));
    let count = fetches.clone();
    let data = (0..ROWS)
        .map(|id| {
            let mut state = (id as u64 + 13) * 0x9e3779b1;
            let row = (0..DIM)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    0.5 + (state % 10000) as f32 / 10000.
                })
                .collect::<Vec<_>>();
            Arc::<[f32]>::from(row)
        })
        .collect::<Vec<_>>();
    let data = Arc::new(data);
    let source: Arc<dyn VectorSource> = Arc::new(move |id: u64| -> Result<Arc<[f32]>, String> {
        let requests = count.fetch_add(1, Ordering::Relaxed) + 1;
        if requests > 1_000_000 {
            return Err("dense fixture vector-access progress bound".into());
        }
        data.get(id as usize)
            .cloned()
            .ok_or("unknown fixture ID".into())
    });
    let config = HierarchicalSpannConfig {
        split_threshold: 128,
        merge_threshold: 32,
        branching_factor: 100,
        ..Default::default()
    };
    let owner = Arc::new(OwnerService::new(
        1,
        990,
        DIM,
        DistanceFunction::Euclidean,
        config,
        source,
    ));
    let root = owner.initial_root();
    let image = root.routing.clone();
    owner.install_initial(vec![root]).unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), image));
    owner.bind_cluster(&cluster);
    cluster
        .add_batch(&(0..ROWS as u64).collect::<Vec<_>>(), 32)
        .unwrap();
    let result = cluster.balance(32);
    let counts = match owner.handle(EngineRequest::Counts).unwrap() {
        EngineResponse::Counts(c) => c,
        _ => panic!(),
    };
    eprintln!(
        "dense shallow fixture: result={result:?}, fetches={}, counts={counts:?}",
        fetches.load(Ordering::Relaxed)
    );
    assert!(
        result.is_ok(),
        "dense construction must resolve actual membership work: {result:?}"
    );
    assert_eq!(counts.vectors, ROWS as u64);
    assert!(
        counts.splits < ROWS as u64,
        "construction cannot indefinitely recluster the same shallow leaf population"
    );
}

#[test]
fn merge_preserves_a_transfer_arriving_after_its_evacuation_snapshot() {
    merge_interleaving_fixture(false, false);
}
#[test]
fn merge_revalidates_preferred_neighbor_membership_changed_before_protection() {
    merge_interleaving_fixture(true, false);
}
#[test]
fn merge_preserves_arrivals_before_forced_evacuation_starts() {
    merge_interleaving_fixture(false, true);
}
fn merge_interleaving_fixture(stale_preferred: bool, early_arrival: bool) {
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize},
        mpsc,
    };
    let armed = Arc::new(AtomicBool::new(false));
    let gate = armed.clone();
    let reads = Arc::new(AtomicUsize::new(0));
    let calls = reads.clone();
    let (paused_tx, paused_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let resume = parking_lot::Mutex::new(resume_rx);
    let source: Arc<dyn VectorSource> = Arc::new(move |id: u64| -> Result<Arc<[f32]>, String> {
        if id == 0
            && gate.load(Ordering::Acquire)
            && calls.fetch_add(1, Ordering::AcqRel)
                == if stale_preferred || early_arrival {
                    0
                } else {
                    1
                }
        {
            paused_tx.send(()).unwrap();
            resume.lock().recv().unwrap();
        }
        match id {
            0 => Ok(vec![if stale_preferred { 10. } else { 0. }; 8].into()),
            1 => Ok(vec![-20.; 8].into()),
            _ => Err("unknown fixture ID".into()),
        }
    });
    let owner = Arc::new(OwnerService::new(
        1,
        991,
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig {
            split_threshold: 8,
            merge_threshold: 2,
            branching_factor: 4,
            ..Default::default()
        },
        source,
    ));
    let root_ref = NodeRef {
        id: BASE,
        authority: 1,
        placement: 0,
    };
    let make = |id: u128, center: f32, ids: Vec<u64>| {
        let mut node = owner
            .fresh_leaf(
                NodeRef {
                    id,
                    authority: 1,
                    placement: 0,
                },
                vec![center; 8],
                ids,
            )
            .unwrap();
        node.routing.parent = Some(root_ref);
        node
    };
    let from = make(BASE + 1, 0., vec![0]);
    let dest = make(BASE + 2, 10., vec![]);
    let donor = make(BASE + 3, -20., vec![1]);
    let from_ref = from.routing.target;
    let donor_ref = donor.routing.target;
    let dest_ref = dest.routing.target;
    let mut root = owner.initial_root();
    root.routing.target = root_ref;
    root.routing.leaf = false;
    root.routing.children = vec![
        child(&from.routing),
        child(&dest.routing),
        child(&donor.routing),
    ];
    root.postings = None;
    let image = root.routing.clone();
    owner
        .install_initial(vec![root, from, dest, donor])
        .unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), image));
    owner.bind_cluster(&cluster);
    armed.store(true, Ordering::Release);
    let running = cluster.clone();
    let task = std::thread::spawn(move || running.balance_leaf_once_for_correctness(from_ref));
    paused_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("merge reaches forced evacuation after snapshotting ID0");
    if stale_preferred {
        cluster.transfer(from_ref, dest_ref, vec![0]).unwrap();
    } else {
        cluster.transfer(donor_ref, from_ref, vec![1]).unwrap();
    }
    resume_tx.send(()).unwrap();
    let result = task.join().unwrap();
    assert!(
        result.is_ok(),
        "a concurrent arrival is current membership, not corruption: {result:?}"
    );
    assert!(owner.ensure_healthy().is_ok());
    if stale_preferred {
        assert!(
            owner.resident(from_ref).is_err(),
            "an empty merge source is retired after current validation"
        );
    } else {
        let now = owner.resident(from_ref).unwrap();
        assert_eq!(now.read().postings.as_ref().unwrap().ids, vec![1]);
    }
    cluster.balance(1).unwrap();
    let EngineResponse::Membership(membership) = owner.handle(EngineRequest::Membership).unwrap()
    else {
        panic!()
    };
    let mut ids = membership
        .into_iter()
        .flat_map(|(_, ids)| ids)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1]);
}

#[test]
fn empty_leaf_retirement_revalidates_arrivals_and_coalesces_follow_up_work() {
    let vectors: Arc<dyn VectorSource> =
        Arc::new(|id: u64| -> Result<Arc<[f32]>, String> { Ok(vec![id as f32; 17].into()) });
    let owner = Arc::new(OwnerService::new(
        1,
        992,
        17,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig {
            split_threshold: 8,
            merge_threshold: 2,
            branching_factor: 4,
            ..Default::default()
        },
        vectors,
    ));
    let root_ref = NodeRef {
        id: BASE,
        authority: 1,
        placement: 0,
    };
    let receiver = routing(BASE + 1, Some(root_ref), vec![]);
    let donor = routing(BASE + 2, Some(root_ref), vec![]);
    let root = routing(BASE, None, vec![child(&receiver), child(&donor)]);
    let receiving = receiver.target;
    let donating = donor.target;
    owner
        .install_initial(vec![
            StoredNode {
                routing: root.clone(),
                postings: None,
            },
            StoredNode {
                routing: receiver.clone(),
                postings: Some(PostingList::default()),
            },
            StoredNode {
                routing: donor.clone(),
                postings: Some(postings(&*owner.vectors, &donor.centroid)),
            },
        ])
        .unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), root));
    owner.bind_cluster(&cluster);
    // This is the actual empty-retirement commit path's dependency snapshot.
    // An incoming protected transfer wins before its structural reservation.
    let observed_empty = owner.node(receiving).unwrap();
    cluster.transfer(donating, receiving, vec![0]).unwrap();
    assert!(
        !cluster.install_split(&observed_empty, vec![]).unwrap(),
        "retirement must reject a membership revision changed after observed emptiness"
    );
    assert_eq!(
        owner
            .resident(receiving)
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids,
        vec![0]
    );
    cluster.retire_empty_leaf(receiving).unwrap();
    cluster.retire_empty_leaf(receiving).unwrap();
    assert_eq!(
        cluster.jobs.lock().ready.len(),
        1,
        "renewed membership creates only one pending follow-up per leaf"
    );
    cluster.balance(1).unwrap();
    let EngineResponse::Snapshot(nodes) = owner.handle(EngineRequest::Snapshot).unwrap() else {
        panic!()
    };
    let mut ids = vec![];
    for node in nodes {
        if let Some(p) = node.postings {
            let mut codes = vec![];
            for id in &p.ids {
                codes.extend_from_slice(
                    Code::<1>::quantize(&owner.vector(*id).unwrap(), &node.routing.centroid)
                        .as_ref(),
                );
            }
            assert_eq!(p.codes, codes);
            ids.extend(p.ids);
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1]);
    assert!(cluster.jobs.lock().is_drained());
}

#[test]
fn unknown_and_remote_unresolved_transfer_outcomes_are_not_retryable_dependencies() {
    for error in ["remote operation: owner has an unresolved mutation and cannot continue: dependency changed", "transport I/O while acquiring region", "transfer region acquisition deadline expired", "remote poisoned owner: centroid changed"] {
        assert!(!retryable_transfer_dependency(error),"unresolved outcome must stop instead of replan: {error}");
    }
    for error in [
        "source membership changed before transfer protection",
        "transfer centroid dependency changed",
        "transfer region is not granted",
        "stale or retired node",
        "transfer leaf is already protected",
    ] {
        assert!(retryable_transfer_dependency(error));
    }
}
