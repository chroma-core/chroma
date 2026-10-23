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
