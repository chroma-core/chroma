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
        navigation: Default::default(),
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
        source_claim: None,
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
        source_claim: None,
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
                value: next.prepared().unwrap(),
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
    // Obstruct only the next owner's frozen manifest name. Previous immutable
    // objects remain readable while the actual SaveOwner endpoint fails.
    let failed_manifest = saved.join(format!(
        "owners/{}-{}-failed-save-1.json",
        owner.run.to_string().len(),
        owner.run
    ));
    std::fs::create_dir(&failed_manifest).unwrap();
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

#[test]
fn queued_transfer_survives_a_disjoint_owned_subtree_split() {
    transfer_split_interleaving(false);
}
#[test]
fn captured_transfer_survives_a_disjoint_split_before_initial_admission() {
    transfer_split_interleaving(true);
}
fn transfer_split_interleaving(capture: bool) {
    use super::super::protocol::transfer::{
        DriverDecision, Role, TransferDriver, TransferPayload, TransferState, TransferStep,
    };
    use sha2::Digest;
    let vectors: Arc<dyn VectorSource> =
        Arc::new(|id: u64| -> Result<Arc<[f32]>, String> { Ok(vec![id as f32; 17].into()) });
    let owner = Arc::new(OwnerService::new(
        1,
        996,
        17,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig {
            split_threshold: 8,
            merge_threshold: 0,
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
    let left_ref = NodeRef {
        id: BASE + 10,
        ..root_ref
    };
    let right_ref = NodeRef {
        id: BASE + 20,
        ..root_ref
    };
    let middle_ref = NodeRef {
        id: BASE + 21,
        ..root_ref
    };
    let source = routing(BASE + 11, Some(left_ref), vec![]);
    let destination = routing(BASE + 12, Some(left_ref), vec![]);
    let old = routing(BASE + 22, Some(middle_ref), vec![]);
    let middle = routing(middle_ref.id, Some(right_ref), vec![child(&old)]);
    let left = routing(
        left_ref.id,
        Some(root_ref),
        vec![child(&source), child(&destination)],
    );
    let right = routing(right_ref.id, Some(root_ref), vec![child(&middle)]);
    let root = routing(BASE, None, vec![child(&left), child(&right)]);
    let posting = |image: &RoutingNode, ids: Vec<u64>| PostingList {
        codes: ids
            .iter()
            .flat_map(|id| {
                Code::<1>::quantize(&owner.vectors.get(*id).unwrap(), &image.centroid)
                    .as_ref()
                    .to_vec()
            })
            .collect(),
        ids,
    };
    owner
        .install_initial(vec![
            StoredNode {
                routing: root.clone(),
                postings: None,
            },
            StoredNode {
                routing: left,
                postings: None,
            },
            StoredNode {
                routing: right,
                postings: None,
            },
            StoredNode {
                routing: middle,
                postings: None,
            },
            StoredNode {
                routing: source.clone(),
                postings: Some(posting(&source, vec![0])),
            },
            StoredNode {
                routing: destination.clone(),
                postings: Some(posting(&destination, vec![])),
            },
            StoredNode {
                routing: old.clone(),
                postings: Some(posting(&old, vec![1, 2])),
            },
        ])
        .unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), root));
    owner.bind_cluster(&cluster);
    let held = if capture {
        None
    } else {
        Some(owner.reserve(vec![left_ref]).unwrap())
    };
    let (captured_tx, captured_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    if capture {
        let resume_rx = parking_lot::Mutex::new(resume_rx);
        let first = std::sync::atomic::AtomicBool::new(true);
        *owner.transfer_capture_hook.lock() = Some(Arc::new(move |_| {
            if first.swap(false, Ordering::AcqRel) {
                captured_tx.send(()).unwrap();
                resume_rx
                    .lock()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
        }));
    }
    let manager = owner.regions.read().clone();
    let envelope = owner
        .envelope(
            "posting-transfer",
            vec![(source.target.id, 0), (destination.target.id, 0)],
        )
        .unwrap();
    let mut hash = sha2::Sha256::new();
    hash.update(17u64.to_le_bytes());
    for x in &source.centroid {
        hash.update(x.to_bits().to_le_bytes());
    }
    let digest: [u8; 32] = hash.finalize().into();
    let payload = TransferPayload {
        source_owner: 1,
        destination_owner: 1,
        source_leaf: source.target.id,
        destination_leaf: destination.target.id,
        source_placement: 0,
        destination_placement: 0,
        source_revision: 0,
        destination_revision: 0,
        source_centroid_digest: digest,
        destination_centroid_digest: digest,
        vectors: vec![0],
        content_digest: [0; 32],
    };
    let prepared = std::thread::scope(|scope| {
        let owner = &owner;
        let envelope = envelope.clone();
        let attempt = envelope.attempt;
        let payload = payload.clone();
        let task = scope.spawn(move || {
            owner.handle(EngineRequest::PrepareTransfer {
                envelope,
                payload,
                role: Role::Source,
                codes: vec![],
            })
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        if capture {
            captured_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        } else {
            while manager.status(attempt).ok() != Some(ReservationStatus::Queued) {
                assert!(
                    Instant::now() < deadline,
                    "transfer did not queue behind left subtree"
                );
                std::thread::yield_now();
            }
        }
        let a = routing(BASE + 23, old.parent, vec![]);
        let b = routing(BASE + 24, old.parent, vec![]);
        assert!(cluster
            .install_split(
                &old,
                vec![
                    StoredNode {
                        postings: Some(posting(&a, vec![1])),
                        routing: a
                    },
                    StoredNode {
                        postings: Some(posting(&b, vec![2])),
                        routing: b
                    }
                ]
            )
            .unwrap());
        drop(held);
        if capture {
            resume_tx.send(()).unwrap();
        }
        task.join().unwrap()
    });
    assert!(matches!(prepared,Ok(EngineResponse::Transfer(TransferState::Prepared))),"an unrelated structural edit must not invalidate an intact queued endpoint bundle: {prepared:?}");
    let mut driver = TransferDriver::default();
    driver.decide(DriverDecision::Abort).unwrap();
    owner
        .handle(EngineRequest::TransferStep {
            envelope,
            role: Role::Source,
            step: TransferStep::Abort,
            proof: driver.proof(),
        })
        .unwrap();
    cluster
        .transfer(source.target, destination.target, vec![0])
        .unwrap();
    let EngineResponse::Membership(rows) = owner.handle(EngineRequest::Membership).unwrap() else {
        panic!()
    };
    let mut ids = rows
        .into_iter()
        .flat_map(|(_, ids)| ids)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2]);
}

#[test]
fn two_batch_construction_crosses_owned_internal_branching_without_transfer_starvation() {
    // The first2048 vectors fit under one internal level;4096 require
    // more than32 leaves and an actual owned internal split.32 workers
    // expose queued transfers while structural regions publish.
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
        if requests > 2_000_000 {
            return Err("dense fixture vector-access progress bound".into());
        }
        data.get(id as usize)
            .cloned()
            .ok_or("unknown fixture ID".into())
    });
    let config = HierarchicalSpannConfig {
        split_threshold: 128,
        merge_threshold: 32,
        branching_factor: 32,
        ..Default::default()
    };
    let owner = Arc::new(OwnerService::new(
        1,
        997,
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
        .add_batch(&(0..ROWS as u64 / 2).collect::<Vec<_>>(), 32)
        .unwrap();
    cluster.balance(32).unwrap();
    let EngineResponse::Counts(first) = owner.handle(EngineRequest::Counts).unwrap() else {
        panic!()
    };
    assert!(
        first.leaves <= 32,
        "first batch must fit below the internal branching threshold: {first:?}"
    );
    cluster
        .add_batch(&(ROWS as u64 / 2..ROWS as u64).collect::<Vec<_>>(), 32)
        .unwrap();
    let result = cluster.balance(32);
    let counts = match owner.handle(EngineRequest::Counts).unwrap() {
        EngineResponse::Counts(c) => c,
        _ => panic!(),
    };
    eprintln!(
        "two-batch multilevel fixture: first={first:?}, result={result:?}, fetches={}, counts={counts:?}",
        fetches.load(Ordering::Relaxed)
    );
    assert!(
        result.is_ok(),
        "dense construction must resolve actual membership work: {result:?}"
    );
    assert_eq!(counts.vectors, ROWS as u64);
    assert!(
        counts.leaves > 32 && counts.nodes - counts.leaves > 1,
        "fixture must exercise actual internal splitting: {counts:?}"
    );
    assert_eq!(counts.jobs_started, counts.jobs_completed);
    assert!(cluster.jobs.lock().is_drained());
    let EngineResponse::Snapshot(nodes) = owner.handle(EngineRequest::Snapshot).unwrap() else {
        panic!()
    };
    let by_id = nodes
        .iter()
        .map(|n| (n.routing.target.id, n))
        .collect::<BTreeMap<_, _>>();
    let mut actual = vec![];
    for node in &nodes {
        if let Some(p) = &node.postings {
            assert!(p.ids.len() <= 128);
            let codes = p
                .ids
                .iter()
                .flat_map(|id| {
                    Code::<1>::quantize(&owner.vectors.get(*id).unwrap(), &node.routing.centroid)
                        .as_ref()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            assert_eq!(codes, p.codes);
            actual.extend_from_slice(&p.ids);
        } else {
            assert!(node.routing.children.len() <= 32);
            for c in &node.routing.children {
                let current = &by_id[&c.target.id].routing;
                assert_eq!(current.parent, Some(node.routing.target));
                assert_eq!(c.centroid, current.centroid);
            }
        }
    }
    actual.sort_unstable();
    assert_eq!(actual, (0..ROWS as u64).collect::<Vec<_>>());
    assert!(
        counts.splits < ROWS as u64,
        "construction cannot indefinitely recluster the same shallow leaf population"
    );
}

#[test]
fn reassignment_revalidates_after_slow_successful_transfer_work() {
    reassignment_progress_fixture(false);
}
#[test]
fn reassignment_without_membership_progress_still_stops_within_its_existing_budget() {
    reassignment_progress_fixture(true);
}
fn reassignment_progress_fixture(stalled: bool) {
    use std::sync::atomic::AtomicUsize;
    let vectors: Arc<dyn VectorSource> =
        Arc::new(|id: u64| -> Result<Arc<[f32]>, String> { Ok(vec![id as f32 + 1.; 17].into()) });
    let owner = Arc::new(OwnerService::new(
        1,
        998,
        17,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig {
            split_threshold: 8,
            merge_threshold: 0,
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
    let mut source = routing(BASE + 1, Some(root_ref), vec![]);
    let mut a = routing(BASE + 2, Some(root_ref), vec![]);
    let mut b = routing(BASE + 3, Some(root_ref), vec![]);
    for (node, value) in [(&mut source, 0.), (&mut a, 1.), (&mut b, 2.)] {
        node.centroid = vec![value; 17];
        node.centroid_code = Code::<4>::quantize(&node.centroid, &[0.; 17])
            .as_ref()
            .to_vec();
    }
    let root = routing(BASE, None, vec![child(&source), child(&a), child(&b)]);
    let source_ref = source.target;
    let b_ref = b.target;
    let a_ref = a.target;
    let codes = (0..2)
        .flat_map(|id| {
            Code::<1>::quantize(&owner.vectors.get(id).unwrap(), &source.centroid)
                .as_ref()
                .to_vec()
        })
        .collect();
    owner
        .install_initial(vec![
            StoredNode {
                routing: root.clone(),
                postings: None,
            },
            StoredNode {
                routing: source,
                postings: Some(PostingList {
                    ids: vec![0, 1],
                    codes,
                }),
            },
            StoredNode {
                routing: a,
                postings: Some(PostingList::default()),
            },
            StoredNode {
                routing: b,
                postings: Some(PostingList::default()),
            },
        ])
        .unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), root));
    owner.bind_cluster(&cluster);
    let seen = parking_lot::Mutex::new(BTreeSet::new());
    let attempts = AtomicUsize::new(0);
    let weak = Arc::downgrade(&owner);
    *owner.transfer_capture_hook.lock() = Some(Arc::new(move |attempt| {
        if !seen.lock().insert(attempt) {
            return;
        }
        let ordinal = attempts.fetch_add(1, Ordering::AcqRel);
        if stalled {
            let owner = weak.upgrade().unwrap();
            for (target, base) in [(a_ref, 1.), (b_ref, 2.)] {
                let handle = owner.resident(target).unwrap();
                let mut node = handle.write();
                let mut image = (*node.routing).clone();
                image.centroid = vec![base + if ordinal % 2 == 0 { 0.001 } else { 0. }; 17];
                image.centroid_code = Code::<4>::quantize(&image.centroid, &[0.; 17])
                    .as_ref()
                    .to_vec();
                image.revision += 1;
                node.routing = Arc::new(image.clone());
                owner.publish_images(vec![image], &[]).unwrap();
            }
            return;
        }
        match ordinal {
            0 => std::thread::sleep(Duration::from_millis(5100)),
            1 => {
                let owner = weak.upgrade().unwrap();
                let handle = owner.resident(b_ref).unwrap();
                let mut node = handle.write();
                let mut image = (*node.routing).clone();
                image.centroid = vec![2.125; 17];
                image.centroid_code = Code::<4>::quantize(&image.centroid, &[0.; 17])
                    .as_ref()
                    .to_vec();
                image.revision += 1;
                node.routing = Arc::new(image.clone());
                owner.publish_images(vec![image], &[]).unwrap();
            }
            _ => {}
        }
    }));
    let started = Instant::now();
    let result = cluster.reassign_path::<false>(source_ref, vec![0, 1], None);
    if stalled {
        let error = result.unwrap_err();
        assert!(
            error.contains("completed_vectors=0") && error.contains("pending_ids=2"),
            "{error}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(6),
            "known rejected attempts must not restart the stall budget"
        );
        assert_eq!(
            owner
                .resident(source_ref)
                .unwrap()
                .read()
                .postings
                .as_ref()
                .unwrap()
                .ids,
            vec![0, 1]
        );
        return;
    }
    assert!(result.is_ok(),"completed transfer work must not exhaust the budget for a later dependency revalidation: {result:?}");
    let EngineResponse::Membership(rows) = owner.handle(EngineRequest::Membership).unwrap() else {
        panic!()
    };
    let mut ids = rows
        .into_iter()
        .flat_map(|(_, ids)| ids)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1]);
    assert!(owner
        .resident(source_ref)
        .unwrap()
        .read()
        .postings
        .as_ref()
        .unwrap()
        .ids
        .is_empty());
}

#[test]
fn construction_crosses_actual_branch100_split2048_with_32_workers() {
    const DIM: usize = 8;
    let rows = 99 * 512 + 2049;
    let data = (0..rows)
        .map(|id| {
            let (group, slot) = if id < 2049 {
                (0, id)
            } else {
                (1 + (id - 2049) / 512, (id - 2049) % 512)
            };
            let offset = if slot % 2 == 0 { -0.2 } else { 0.2 };
            Arc::<[f32]>::from(vec![group as f32 * 10. + offset; DIM])
        })
        .collect::<Vec<_>>();
    let data = Arc::new(data);
    let vectors: Arc<dyn VectorSource> = Arc::new(move |id: u64| -> Result<Arc<[f32]>, String> {
        data.get(id as usize)
            .cloned()
            .ok_or("unknown fixture ID".into())
    });
    let owner = Arc::new(OwnerService::new(
        1,
        999,
        DIM,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig {
            split_threshold: 2048,
            merge_threshold: 512,
            branching_factor: 100,
            ..Default::default()
        },
        vectors,
    ));
    let root_ref = NodeRef {
        id: BASE,
        authority: 1,
        placement: 0,
    };
    let mut children = vec![];
    let mut nodes = vec![];
    for group in 0..100usize {
        let ids = if group == 0 {
            (0..2049).collect::<Vec<u64>>()
        } else {
            let begin = 2049 + (group - 1) * 512;
            (begin as u64..(begin + 512) as u64).collect::<Vec<_>>()
        };
        let center = vec![group as f32 * 10.; DIM];
        let image = RoutingNode {
            navigation: Default::default(),
            target: NodeRef {
                id: BASE + 1 + group as u128,
                ..root_ref
            },
            parent: Some(root_ref),
            centroid_code: Code::<4>::quantize(&center, &[0.; DIM]).as_ref().to_vec(),
            centroid: center,
            children: vec![],
            leaf: true,
            revision: 0,
        };
        let codes = ids
            .iter()
            .flat_map(|id| {
                Code::<1>::quantize(&owner.vectors.get(*id).unwrap(), &image.centroid)
                    .as_ref()
                    .to_vec()
            })
            .collect();
        children.push(child(&image));
        nodes.push(StoredNode {
            routing: image,
            postings: Some(PostingList { ids, codes }),
        });
    }
    let root = RoutingNode {
        navigation: Default::default(),
        target: root_ref,
        parent: None,
        centroid: vec![495.; DIM],
        centroid_code: Code::<4>::quantize(&vec![495.; DIM], &[0.; DIM])
            .as_ref()
            .to_vec(),
        children,
        leaf: false,
        revision: 0,
    };
    nodes.push(StoredNode {
        routing: root.clone(),
        postings: None,
    });
    owner.install_initial(nodes).unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), root));
    owner.bind_cluster(&cluster);
    let result = cluster.balance(32);
    let EngineResponse::Counts(counts) = owner.handle(EngineRequest::Counts).unwrap() else {
        panic!()
    };
    eprintln!("actual-threshold fixture: result={result:?}, counts={counts:?}");
    assert!(
        result.is_ok(),
        "branch100/split2048 construction must finish: {result:?}"
    );
    assert!(
        counts.leaves > 100 && counts.nodes - counts.leaves > 1,
        "fixture must split the owned internal root: {counts:?}"
    );
    assert_eq!(counts.jobs_started, counts.jobs_completed);
    assert!(cluster.jobs.lock().is_drained());
    let EngineResponse::Snapshot(nodes) = owner.handle(EngineRequest::Snapshot).unwrap() else {
        panic!()
    };
    let by_id = nodes
        .iter()
        .map(|n| (n.routing.target.id, n))
        .collect::<BTreeMap<_, _>>();
    let mut ids = vec![];
    for node in &nodes {
        if let Some(p) = &node.postings {
            assert!(p.ids.len() <= 2048);
            let codes = p
                .ids
                .iter()
                .flat_map(|id| {
                    Code::<1>::quantize(&owner.vectors.get(*id).unwrap(), &node.routing.centroid)
                        .as_ref()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            assert_eq!(codes, p.codes);
            ids.extend_from_slice(&p.ids);
        } else {
            assert!(node.routing.children.len() <= 100);
            for c in &node.routing.children {
                let current = &by_id[&c.target.id].routing;
                assert_eq!(current.parent, Some(node.routing.target));
                assert_eq!(current.centroid, c.centroid);
            }
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, (0..rows as u64).collect::<Vec<_>>());
}

/// A leaf split synchronously reassigns a neighbor's postings. Its actual job
/// identity remains the retired split source, not the neighbor it now claims.
#[test]
fn deferred_reassignment_waits_for_claim_when_producer_job_has_another_node_id() {
    use std::sync::{OnceLock, Weak};
    type Context = (Weak<OwnerService>, Weak<Cluster>, Vec<NodeRef>);
    let context = Arc::new(OnceLock::<Context>::new());
    let observed = Arc::new(parking_lot::Mutex::new(None));
    let value = |id: u64| -> Arc<[f32]> {
        vec![
            match id {
                0..=7 | 16 => 0.,
                8..=15 => 10.,
                17 => 5.,
                _ => panic!("fixture ID"),
            };
            17
        ]
        .into()
    };
    let vectors: Arc<dyn VectorSource> = Arc::new({
        let context = context.clone();
        let observed = observed.clone();
        move |id: u64| -> Result<Arc<[f32]>, String> {
            if id == 16 {
                if let Some((owner, cluster, nodes)) = context.get() {
                    let owner = owner.upgrade().unwrap();
                    let cluster = cluster.upgrade().unwrap();
                    let neighbor = nodes[1];
                    let handle = owner.resident(neighbor).unwrap();
                    let producer = {
                        let node = handle.read();
                        if node.transfer_source == Some(neighbor.id) {
                            node.claim_producer
                        } else {
                            None
                        }
                    };
                    if let Some(producer) = producer {
                        assert!(observed.lock().is_none(), "one captured neighbor transfer");
                        assert!(
                            !owner.nodes.contains_key(&nodes[0].id),
                            "the producer split source is already retired"
                        );
                        assert!(cluster.jobs.lock().active_attempts.contains(&producer));
                        // Actual navigation selects the claimed neighbor, then
                        // the local transfer result supplies its producer attempt.
                        cluster.reassign(nodes[2], vec![17], None).unwrap();
                        let mut jobs = cluster.jobs.lock();
                        let queued = jobs
                            .ready
                            .iter()
                            .find_map(|j| match j {
                                WorkJob::Reassign(j) if j.source == nodes[2] => Some(j),
                                _ => None,
                            })
                            .expect("real blocked reassignment was retained");
                        assert_eq!(queued.ids, vec![17]);
                        assert_eq!(queued.blocked, vec![neighbor.id]);
                        assert_eq!(queued.blocked_jobs, vec![producer]);
                        jobs.enqueue(BalanceJob {
                            envelope: owner
                                .envelope("balance-job", vec![(nodes[3].id, 0)])
                                .unwrap(),
                            node: nodes[3],
                            depth: 1,
                            internal: false,
                        });
                        let independent = jobs
                            .next(owner.owner)
                            .expect("independent work stays runnable");
                        let prematurely_dispatched = matches!(&independent, WorkJob::Reassign(_));
                        if !prematurely_dispatched {
                            assert_eq!(independent.node(), nodes[3]);
                        }
                        jobs.finish(&independent, Ok(()));
                        *observed.lock() = Some(prematurely_dispatched);
                    }
                }
            }
            Ok(value(id))
        }
    });
    let owner = Arc::new(OwnerService::new(
        1,
        914,
        17,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig {
            split_threshold: 12,
            merge_threshold: 0,
            branching_factor: 8,
            write_beam_min: 8,
            write_beam_max: 8,
            reassign_neighbor_count: 8,
            ..Default::default()
        },
        vectors,
    ));
    let mut root = routing(BASE, None, vec![]);
    let mut nodes = vec![];
    let mut refs = vec![];
    for (slot, center, ids) in [
        (1, 5., (0..16).collect::<Vec<_>>()),
        (2, 5., vec![16]),
        (3, 20., vec![17]),
        (4, 30., vec![]),
    ] {
        let target = NodeRef {
            id: BASE + slot,
            authority: 1,
            placement: 0,
        };
        let mut leaf = owner.fresh_leaf(target, vec![center; 17], ids).unwrap();
        leaf.routing.parent = Some(root.target);
        refs.push(target);
        nodes.push(leaf);
    }
    root.children = nodes.iter().map(|n| child(&n.routing)).collect();
    root.leaf = false;
    nodes.push(StoredNode {
        routing: root.clone(),
        postings: None,
    });
    owner.install_initial(nodes).unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), root));
    owner.bind_cluster(&cluster);
    context
        .set((
            Arc::downgrade(&owner),
            Arc::downgrade(&cluster),
            refs.clone(),
        ))
        .unwrap();
    let job = BalanceJob {
        envelope: owner
            .envelope("balance-job", vec![(refs[0].id, 0)])
            .unwrap(),
        node: refs[0],
        depth: 0,
        internal: false,
    };
    let active = {
        let mut q = cluster.jobs.lock();
        q.enqueue(job.clone());
        q.next(1).unwrap()
    };
    // Real dispatch threads the executing attempt through split -> neighbor
    // reassignment -> local claim; no test assigns resident producer metadata.
    owner
        .handle(EngineRequest::ScheduledJob(WorkJob::Balance(job)))
        .unwrap();
    assert_eq!(exact_membership(&owner), (0..18).collect::<Vec<_>>());
    assert!(owner.resident(refs[1]).unwrap().read().reserved.is_none());
    let prematurely_dispatched = observed
        .lock()
        .expect("neighbor transfer reached its real preparation");
    assert!(
        !prematurely_dispatched,
        "scheduler dispatched a retry while its actual producer's neighbor claim was held"
    );
    let next = {
        let mut q = cluster.jobs.lock();
        assert!(
            q.next(1).is_none(),
            "the producer remains active until its owner reply is acknowledged"
        );
        assert!(!q.is_drained());
        q.finish(&active, Ok(()));
        q.next(1)
            .expect("claim cleanup plus producer completion releases the retained retry")
    };
    let WorkJob::Reassign(retry) = &next else {
        panic!("retained retry")
    };
    owner
        .handle(EngineRequest::ScheduledJob(WorkJob::Reassign(
            retry.clone(),
        )))
        .unwrap();
    cluster.jobs.lock().finish(&next, Ok(()));
    assert_eq!(exact_membership(&owner), (0..18).collect::<Vec<_>>());
}

fn owned_route_churn_fixture() -> (Arc<OwnerService>, Arc<Cluster>, NodeRef) {
    owned_route_churn_fixture_with_merge(0)
}
fn owned_route_churn_fixture_with_merge(
    merge_threshold: usize,
) -> (Arc<OwnerService>, Arc<Cluster>, NodeRef) {
    let source: Arc<dyn VectorSource> =
        Arc::new(|id: u64| -> Result<Arc<[f32]>, String> { Ok(vec![id as f32; 17].into()) });
    let owner = Arc::new(OwnerService::new(
        1,
        1522,
        17,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig {
            split_threshold: 8,
            merge_threshold,
            branching_factor: 4,
            ..Default::default()
        },
        source,
    ));
    let mut root = routing(BASE, None, vec![]);
    let mut branch = routing(BASE + 1, Some(root.target), vec![]);
    let mut source = routing(BASE + 2, Some(branch.target), vec![]);
    source.centroid.fill(10.);
    let destination = routing(BASE + 3, Some(branch.target), vec![]);
    branch.children = vec![child(&source), child(&destination)];
    branch.leaf = false;
    root.children = vec![child(&branch)];
    root.leaf = false;
    let nodes = vec![
        StoredNode {
            routing: root.clone(),
            postings: None,
        },
        StoredNode {
            routing: branch,
            postings: None,
        },
        StoredNode {
            postings: Some(postings(&*owner.vectors, &source.centroid)),
            routing: source.clone(),
        },
        StoredNode {
            routing: destination,
            postings: Some(PostingList::default()),
        },
    ];
    owner.install_initial(nodes).unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), root));
    owner.bind_cluster(&cluster);
    (owner, cluster, source.target)
}
fn churn_owned_child_after_parent_capture(
    cluster: &Arc<Cluster>,
    queries_per_batch: u64,
    skip_root_captures: u64,
) -> Arc<std::sync::atomic::AtomicU64> {
    let weak = Arc::downgrade(cluster);
    let edits = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let count = edits.clone();
    let captures = std::sync::atomic::AtomicU64::new(0);
    *cluster.navigation_images_hook.lock() = Some(Arc::new(move |nodes| {
        let captured = nodes
            .iter()
            .any(|n| n.id == BASE)
            .then(|| captures.fetch_add(1, Ordering::Relaxed));
        if captured.is_some_and(|n| {
            n >= skip_root_captures && (n - skip_root_captures) % queries_per_batch == 0
        }) {
            let cluster = weak.upgrade().unwrap();
            let root = cluster
                .local
                .node(NodeRef {
                    id: BASE,
                    authority: 1,
                    placement: 0,
                })
                .unwrap();
            let old = cluster.local.node(root.children[0].target).unwrap();
            let mut replacement = old.clone();
            replacement.target = cluster.local.alloc_node().unwrap();
            assert!(cluster
                .install_replacements(
                    &old,
                    vec![StoredNode {
                        routing: replacement,
                        postings: None
                    }],
                    false
                )
                .unwrap());
            count.fetch_add(1, Ordering::Relaxed);
        }
    }));
    edits
}
#[test]
fn local_navigation_rejects_real_retirement_from_pinned_parent_without_corrupting_published_tree() {
    let (owner, cluster, source) = owned_route_churn_fixture();
    let edits = churn_owned_child_after_parent_capture(&cluster, 7, 0);
    let error = cluster
        .navigate(
            &vec![vec![0.; 17]; 7],
            &ReadBeamPolicy::uniform(None, 4, 4),
            None,
        )
        .unwrap_err();
    assert!(
        error.contains("whole-attempt retry budget; attempts=8"),
        "{error}"
    );
    assert!(error.contains("stale local image"), "{error}");
    assert!(
        error.contains("pinned_sequence=0 current_sequence=0"),
        "{error}"
    );
    // One valid parent replacement per attempt forces only its first query to
    // restart, but the driver discards the other six valid query frontiers. The authoritative current parent has no obsolete reference.
    assert_eq!(edits.load(Ordering::Relaxed), 8);
    *cluster.navigation_images_hook.lock() = None;
    let routes = cluster
        .navigate(
            &vec![vec![0.; 17]; 7],
            &ReadBeamPolicy::uniform(None, 4, 4),
            None,
        )
        .unwrap();
    assert_eq!(routes.len(), 7);
    assert!(routes
        .iter()
        .all(|r| r.iter().any(|(n, _)| n.id == source.id)));
    assert_eq!(exact_membership(&owner), vec![0, 1]);
}

#[test]
fn installed_split_navigation_churn_retains_pending_work_until_other_producer_finishes() {
    let (owner, cluster, old_source) = owned_route_churn_fixture();
    let branch = owner.node(old_source).unwrap().parent.unwrap();
    let caller = WorkJob::Balance(BalanceJob {
        envelope: owner
            .envelope("controlled-postinstall-split", vec![(old_source.id, 0)])
            .unwrap(),
        node: old_source,
        depth: 0,
        internal: false,
    });
    let other = WorkJob::Balance(BalanceJob {
        envelope: owner
            .envelope("controlled-other-structural-producer", vec![(branch.id, 0)])
            .unwrap(),
        node: branch,
        depth: 0,
        internal: true,
    });
    {
        let mut q = cluster.jobs.lock();
        if let WorkJob::Balance(j) = &caller {
            q.enqueue(j.clone());
        }
        if let WorkJob::Balance(j) = &other {
            q.enqueue(j.clone());
        }
        assert_eq!(q.next(1).unwrap().attempt(), caller.attempt());
        assert_eq!(q.next(1).unwrap().attempt(), other.attempt());
    }
    // Use the actual protected installation, then run the exact fallible
    // post-install reassignment stage under its admitted producer context.
    let old = owner.node(old_source).unwrap();
    let fresh = NodeRef {
        id: BASE + 5,
        ..old_source
    };
    let empty = NodeRef {
        id: BASE + 6,
        ..old_source
    };
    let replacements = vec![
        owner
            .fresh_leaf(fresh, old.centroid.clone(), vec![0, 1])
            .unwrap(),
        owner.fresh_leaf(empty, vec![20.; 17], vec![]).unwrap(),
    ];
    assert!(cluster
        .install_replacements(&old, replacements, false)
        .unwrap());
    assert!(owner.resident(old_source).is_err());
    let edits = churn_owned_child_after_parent_capture(&cluster, 2, 0);
    let envelope = match &caller {
        WorkJob::Balance(j) => j.envelope.clone(),
        _ => unreachable!(),
    };
    let result = owner.execute_job(
        &envelope,
        b"controlled-real-postinstall-stage".to_vec(),
        || cluster.reassign_for_job(fresh, vec![0, 1], None, Some(caller.attempt())),
    );
    assert!(
        result.is_ok(),
        "a completed split must retain deferred discovery without poisoning its owner: {result:?}"
    );
    assert_eq!(edits.load(Ordering::Relaxed), 8);
    owner.ensure_healthy().unwrap();
    assert_eq!(
        owner.observability.snapshot().counters["NavigationDeferredJobs"],
        1
    );
    assert_eq!(exact_membership(&owner), vec![0, 1]);
    assert!(owner.nodes.iter().all(|n| n.read().reserved.is_none()));
    {
        let mut q = cluster.jobs.lock();
        q.finish(&caller, Ok(()));
        let retry = q
            .ready
            .iter()
            .find_map(|w| {
                if let WorkJob::Reassign(j) = w {
                    Some(j)
                } else {
                    None
                }
            })
            .expect("pending membership obligation");
        assert_eq!(retry.ids, vec![0, 1]);
        assert!(retry.blocked_jobs.contains(&other.attempt()));
        assert!(!q.is_drained());
    }
    // Queued obligation prevents save/close from claiming that construction has
    // drained, even though the original admitted split job returned safely.
    assert!(owner
        .handle(EngineRequest::CloseEpoch {
            run: owner.run,
            epoch: 0
        })
        .is_err());
    *cluster.navigation_images_hook.lock() = None;
    {
        let mut q = cluster.jobs.lock();
        q.finish(&other, Ok(()));
    }
    cluster.balance(2).unwrap();
    assert!(cluster.jobs.lock().is_drained());
    owner.ensure_healthy().unwrap();
    assert_eq!(exact_membership(&owner), vec![0, 1]);
    assert!(owner
        .resident(fresh)
        .unwrap()
        .read()
        .postings
        .as_ref()
        .unwrap()
        .ids
        .is_empty());
}

#[test]
fn navigation_retry_keeps_stable_broken_route_fatal_without_another_structural_producer() {
    for provisional in [false, true] {
        let (owner, cluster, source) = owned_route_churn_fixture();
        let branch = owner.node(source).unwrap().parent.unwrap();
        let other = WorkJob::Balance(BalanceJob {
            envelope: owner
                .envelope("unrelated-structural-job", vec![(branch.id, 0)])
                .unwrap(),
            node: branch,
            depth: 0,
            internal: true,
        });
        if provisional {
            let mut q = cluster.jobs.lock();
            if let WorkJob::Balance(j) = &other {
                q.enqueue(j.clone());
            }
            assert_eq!(q.next(1).unwrap().attempt(), other.attempt());
        }
        // Deliberately leave a broken edge in the current parent. This differs
        // from legitimate capture-before-retirement: stopping work cannot fix it.
        owner.publish_images(vec![], &[branch.id]).unwrap();
        if provisional {
            cluster.reassign(source, vec![0, 1], None).unwrap();
            assert_eq!(cluster.jobs.lock().ready.len(), 1);
            cluster.jobs.lock().finish(&other, Ok(()));
            let retry = cluster.jobs.lock().next(1).unwrap();
            let error = owner.execute_scheduled_job(retry.clone()).unwrap_err();
            assert!(error.contains("stale local image"), "{error}");
            cluster.jobs.lock().finish(&retry, Err(error));
        } else {
            let envelope = owner
                .envelope("broken-route-job", vec![(source.id, 0)])
                .unwrap();
            let error = owner
                .execute_job(&envelope, vec![1], || {
                    cluster.reassign(source, vec![0, 1], None)
                })
                .unwrap_err();
            assert!(error.contains("whole-attempt retry budget"), "{error}");
        }
        assert!(owner.ensure_healthy().is_err());
        assert!(
            cluster.jobs.lock().ready.is_empty(),
            "reassignment-only activity cannot keep inventing progress"
        );
        assert_eq!(
            owner
                .resident(source)
                .unwrap()
                .read()
                .postings
                .as_ref()
                .unwrap()
                .ids,
            vec![0, 1]
        );
    }
}

#[test]
fn navigation_retry_snapshot_excludes_self_preserves_replay_and_releases_reciprocal_waiters() {
    let (owner, cluster, source) = owned_route_churn_fixture();
    let other_node = NodeRef {
        id: BASE + 3,
        ..source
    };
    let a = WorkJob::Balance(BalanceJob {
        envelope: owner.envelope("a", vec![(source.id, 0)]).unwrap(),
        node: source,
        depth: 0,
        internal: false,
    });
    let mut b = WorkJob::Balance(BalanceJob {
        envelope: owner.envelope("b", vec![(other_node.id, 0)]).unwrap(),
        node: other_node,
        depth: 0,
        internal: false,
    });
    if let WorkJob::Balance(job) = &mut b {
        job.envelope.attempt.authority = 2;
    }
    let retry = |node: NodeRef, id: u64| ReassignJob {
        envelope: owner
            .envelope("controlled-nav-retry", vec![(node.id, node.placement)])
            .unwrap(),
        source: node,
        ids: vec![id],
        excluded: None,
        blocked: vec![],
        blocked_jobs: vec![],
        depth: 0,
    };
    let ra = retry(source, 0);
    let rb = retry(other_node, 1);
    let mut q = cluster.jobs.lock();
    if let WorkJob::Balance(j) = &a {
        q.enqueue(j.clone());
    }
    if let WorkJob::Balance(j) = &b {
        q.enqueue(j.clone());
    }
    assert_eq!(q.next(1).unwrap().attempt(), a.attempt());
    assert_eq!(q.next(1).unwrap().attempt(), b.attempt());
    q.enqueue_navigation_retry(ra.clone(), Some(a.attempt()), &owner)
        .unwrap();
    q.enqueue_navigation_retry(rb.clone(), Some(b.attempt()), &owner)
        .unwrap();
    for job in &q.ready {
        let WorkJob::Reassign(job) = job else {
            panic!("only retained retries")
        };
        assert_eq!(
            job.blocked_jobs,
            if job.source == source {
                vec![b.attempt()]
            } else {
                vec![a.attempt()]
            }
        );
    }
    q.finish(&a, Ok(()));
    assert!(q.next(1).is_none());
    q.finish(&b, Ok(()));
    // An accepted original request is replayable after its blocker disappears.
    let ready = q.ready.len();
    q.enqueue_navigation_retry(ra.clone(), Some(a.attempt()), &owner)
        .unwrap();
    assert_eq!(q.ready.len(), ready);
    assert!(q
        .enqueue_navigation_retry(ra.clone(), Some(b.attempt()), &owner)
        .is_err());
    assert!(q.enqueue_reassign(ra, &owner).is_err());
    let first = q.next(1).unwrap();
    let second = q.next(1).unwrap();
    q.finish(&first, Ok(()));
    q.finish(&second, Ok(()));
    assert!(q.is_drained());
    assert!(q
        .enqueue_navigation_retry(retry(source, 0), None, &owner)
        .unwrap_err()
        .contains("no other active structural producer"));
}

#[test]
fn deferred_merge_evacuation_keeps_retirement_until_pending_membership_drains() {
    let (owner, cluster, source) = owned_route_churn_fixture_with_merge(3);
    let handle = owner.resident(source).unwrap();
    let mut source_image = (*handle.read().routing).clone();
    source_image.centroid.fill(0.5);
    {
        let mut node = handle.write();
        node.routing = Arc::new(source_image.clone());
        node.postings = Some(postings(&*owner.vectors, &source_image.centroid));
    }
    let branch = source_image.parent.unwrap();
    let branch_handle = owner.resident(branch).unwrap();
    let mut parent = (*branch_handle.read().routing).clone();
    parent
        .children
        .iter_mut()
        .find(|c| c.target == source)
        .unwrap()
        .centroid = source_image.centroid.clone();
    branch_handle.write().routing = Arc::new(parent.clone());
    owner
        .publish_images(vec![source_image, parent], &[])
        .unwrap();
    let caller = WorkJob::Balance(BalanceJob {
        envelope: owner
            .envelope("merge-producer", vec![(source.id, 0)])
            .unwrap(),
        node: source,
        depth: 0,
        internal: false,
    });
    let other = WorkJob::Balance(BalanceJob {
        envelope: owner
            .envelope("retiring-internal-producer", vec![(branch.id, 0)])
            .unwrap(),
        node: branch,
        depth: 0,
        internal: true,
    });
    {
        let mut q = cluster.jobs.lock();
        if let WorkJob::Balance(j) = &caller {
            q.enqueue(j.clone());
        }
        if let WorkJob::Balance(j) = &other {
            q.enqueue(j.clone());
        }
        assert_eq!(q.next(1).unwrap().attempt(), caller.attempt());
        assert_eq!(q.next(1).unwrap().attempt(), other.attempt());
    }
    // Initial neighbor selection is stable. Force churn only during the two-row
    // evacuation, after the preferred-neighbor step has moved no equal-score row.
    let edits = churn_owned_child_after_parent_capture(&cluster, 2, 1);
    cluster
        .merge_leaf_for_job(source, Some(caller.attempt()))
        .unwrap();
    assert_eq!(edits.load(Ordering::Relaxed), 8);
    assert_eq!(
        owner
            .resident(source)
            .unwrap()
            .read()
            .postings
            .as_ref()
            .unwrap()
            .ids,
        vec![0, 1]
    );
    {
        let mut q = cluster.jobs.lock();
        q.finish(&caller, Ok(()));
        assert!(q.ready.iter().any(
            |w| matches!(w,WorkJob::Reassign(j) if j.source==source&&j.excluded==Some(source.id))
        ));
        q.finish(&other, Ok(()));
    }
    *cluster.navigation_images_hook.lock() = None;
    cluster.balance(2).unwrap();
    owner.ensure_healthy().unwrap();
    assert!(cluster.jobs.lock().is_drained());
    assert_eq!(exact_membership(&owner), vec![0, 1]);
    assert!(
        owner.resident(source).is_err(),
        "the deferred merge's empty source must be retired before the queue drains"
    );
    let images = owner.routing_images.read();
    assert!(images
        .values()
        .all(|image| image.children.iter().all(|c| c.target != source)));
}

#[test]
fn insertion_navigation_window_is_bounded_and_explicit() {
    assert_eq!(Cluster::insertion_navigation_window(None).unwrap(), 64);
    for window in [1, 64, 128, 256] {
        assert_eq!(
            Cluster::insertion_navigation_window(Some(&window.to_string())).unwrap(),
            window
        );
    }
    for invalid in [
        "",
        "0",
        "257",
        "-1",
        "1.5",
        "abc",
        "9999999999999999999999999999",
    ] {
        assert!(
            Cluster::insertion_navigation_window(Some(invalid)).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn insertion_worker_override_is_bounded_and_preserves_default() {
    for default in [1, 2, 32, 64, 128] {
        assert_eq!(Cluster::insertion_workers(None, default).unwrap(), default);
    }
    for workers in [1, 32, 64] {
        assert_eq!(
            Cluster::insertion_workers(Some(&workers.to_string()), 32).unwrap(),
            workers
        );
    }
    for invalid in [
        "",
        "0",
        "65",
        "-1",
        "1.5",
        "abc",
        "9999999999999999999999999999",
    ] {
        assert!(
            Cluster::insertion_workers(Some(invalid), 32).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn scheduler_diagnostics_partition_real_dispatch_blockers_without_mutation() {
    let (owner, _) = fixture(false);
    let make = |id, authority| BalanceJob {
        envelope: owner.envelope("diagnostic-job", vec![(id, 0)]).unwrap(),
        node: NodeRef {
            id,
            authority,
            placement: 0,
        },
        depth: 0,
        internal: false,
    };
    let mut q = JobQueue::default();
    q.enqueue(make(BASE + 1, 1));
    let active = q.next(1).unwrap();
    q.enqueue(make(BASE + 1, 1));
    q.enqueue(make(BASE + 2, 2));
    let make_retry = |id| ReassignJob {
        envelope: owner.envelope("diagnostic-retry", vec![(id, 0)]).unwrap(),
        source: NodeRef {
            id,
            authority: 1,
            placement: 0,
        },
        ids: vec![0],
        excluded: None,
        blocked: vec![],
        blocked_jobs: vec![active.attempt()],
        depth: 0,
    };
    // Add already-normalized execution jobs directly to isolate eligibility.
    q.ready.push_back(WorkJob::Reassign(make_retry(BASE + 3)));
    q.ready.push_back(WorkJob::Reassign(make_retry(BASE + 1)));
    let before = bincode::serialize(&q.ready).unwrap();
    let counts = q.diagnostic_counts();
    assert_eq!(counts["ready"], 1);
    assert_eq!(counts["blocked"], 3);
    assert_eq!(counts["owner_2_dispatch_eligible"], 1);
    assert_eq!(counts["owner_1_blocked_node_only"], 1);
    assert_eq!(counts["owner_1_blocked_producer_only"], 1);
    assert_eq!(counts["owner_1_blocked_node_and_producer"], 1);
    assert_eq!(bincode::serialize(&q.ready).unwrap(), before);
    assert!(q.next(1).is_none());
    assert_eq!(q.next(2).unwrap().node().id, BASE + 2);
    q.finish(&active, Ok(()));
    assert_eq!(q.queue_eligibility()["ready"], 3);
}

#[test]
fn scheduler_scan_diagnostics_count_visits_without_changing_order() {
    let (owner, _) = fixture(false);
    let mut q = JobQueue::default();
    q.diagnostics = Some(SchedulerDiagnostics::default());
    for (id, authority) in [(BASE + 1, 2), (BASE + 2, 1)] {
        q.enqueue(BalanceJob {
            envelope: owner.envelope("diagnostic-order", vec![(id, 0)]).unwrap(),
            node: NodeRef {
                id,
                authority,
                placement: 0,
            },
            depth: 0,
            internal: false,
        });
    }
    let selected = q.next(1).unwrap();
    assert_eq!(selected.node().id, BASE + 2);
    let counts = q.diagnostic_counts();
    assert_eq!(counts["owner_1_queue_visits"], 2);
    assert_eq!(counts["owner_1_wrong_owner_visits"], 1);
    assert_eq!(counts["owner_1_dispatches"], 1);
    assert_eq!(
        q.diagnostics.as_ref().unwrap().active_owners[&selected.attempt()],
        1
    );
    q.finish(&selected, Ok(()));
    assert!(q.diagnostics.as_ref().unwrap().active_owners.is_empty());
    assert_eq!(q.next(2).unwrap().node().id, BASE + 1);
}

#[test]
fn balance_depth_follows_the_running_job() {
    assert_eq!((current_depth(), follow_up_depth()), (0, 0));
    {
        let _job = DepthScope::enter(2);
        assert_eq!((current_depth(), follow_up_depth()), (2, 3));
        {
            let _nested = DepthScope::enter(MAX_BALANCE_DEPTH);
            assert!(follow_up_depth() > MAX_BALANCE_DEPTH);
        }
        assert_eq!(current_depth(), 2);
    }
    assert_eq!((current_depth(), follow_up_depth()), (0, 0));
}
