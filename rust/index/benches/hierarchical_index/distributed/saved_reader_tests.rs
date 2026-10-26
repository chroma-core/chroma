use super::super::generation::RootCoverage;
use super::super::routing::Child;
use super::*;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
fn node(
    id: u128,
    owner: u64,
    parent: Option<u128>,
    centroid: Vec<f32>,
    children: Vec<Child>,
) -> RoutingNode {
    RoutingNode {
        navigation: Default::default(),
        target: NodeRef {
            id,
            authority: owner,
            placement: 9,
        },
        parent: parent.map(|id| NodeRef {
            id,
            authority: 0,
            placement: 9,
        }),
        centroid,
        centroid_code: vec![],
        leaf: children.is_empty(),
        children,
        revision: 123,
    }
}
fn posting_bytes(layout: SavedLayout, p: &SavedPostingList) -> Vec<u8> {
    match layout {
        SavedLayout::V1 => bincode::serialize(&vec![SavedPostingRowV1 {
            leaf: p.leaf,
            ids: Cow::Borrowed(&p.ids),
            codes: Cow::Borrowed(&p.codes),
        }]),
        SavedLayout::V2 => bincode::serialize(&SavedPostingObjectV2 {
            version: 2,
            leaf: p.leaf,
            centroid: Cow::Borrowed(&p.centroid),
            ids: Cow::Borrowed(&p.ids),
            codes: Cow::Borrowed(&p.codes),
        }),
    }
    .unwrap()
}
fn setup(path: &std::path::Path) -> GenerationDescriptor {
    setup_as(path, SavedLayout::V2)
}
/// Root (shared) -> one leaf per owner, saved in the requested layout.
fn setup_as(path: &std::path::Path, layout: SavedLayout) -> GenerationDescriptor {
    let store = GenerationStore::open(path).unwrap();
    let dim = 17;
    let v1 = vec![0.2f32; dim];
    let v2 = vec![0.8f32; dim];
    let mut bytes = vec![];
    for v in [&v1, &v2] {
        for x in v {
            bytes.extend_from_slice(&x.to_le_bytes())
        }
    }
    let corpus = path.join("vectors.f32");
    std::fs::write(&corpus, &bytes).unwrap();
    let descriptor = VectorFileDescriptor {
        path: corpus.to_str().unwrap().into(),
        rows: 2,
        dimensions: dim,
        stride_bytes: dim as u64 * 4,
        offset_bytes: 0,
        first_id: 1,
        file_bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        immutable_locators: vec!["fixture:immutable-source-v1".into()],
    };
    let vectors = store
        .put_object(
            "vector-descriptor",
            &serde_json::to_vec(&descriptor).unwrap(),
        )
        .unwrap();
    let format = FormatContract {
        version: 1,
        dimensions: dim,
        metric: "l2".into(),
        centroid_bits: 4,
        posting_bits: 1,
        vector_bits: 32,
    };
    let mut owners = vec![];
    let mut children = vec![];
    for (id, owner, v, centroid) in [
        (1u64, 10, v1, vec![0.1; dim]),
        (2u64, 20, v2, vec![0.9; dim]),
    ] {
        let leaf = id as u128 + 1;
        let n = node(leaf, owner, Some(1), centroid.clone(), vec![]);
        let code = Code::<1>::quantize(&v, &centroid);
        owners.push(
            save_owner_snapshot_as(
                layout,
                &store,
                "run",
                "save",
                "gen",
                owner,
                vec![leaf],
                format.clone(),
                &[n.clone()],
                &[SavedPostingList {
                    leaf,
                    ids: vec![id],
                    codes: code.as_ref().to_vec(),
                    centroid: vec![],
                }],
                vec![vectors.clone()],
                None,
                &BTreeMap::new(),
            )
            .unwrap()
            .0,
        );
        children.push(Child {
            target: n.target,
            centroid,
            leaf: true,
        });
    }
    let root = node(1, 0, None, vec![0.5; dim], children);
    let shared = save_shared_routing_as(layout, &store, "shared", &[root]).unwrap();
    let manifest = GenerationManifest {
        run: "run".into(),
        save_attempt: "save".into(),
        generation: "gen".into(),
        format,
        expected_roots: vec![
            RootCoverage { root: 2, owner: 10 },
            RootCoverage { root: 3, owner: 20 },
        ],
        owners,
        shared_routing: shared,
        full_vectors: vec![vectors],
        total_vectors: 2,
    };
    let d = store.save_generation(&manifest).unwrap();
    store.publish_latest(&d, None).unwrap();
    d
}
#[test]
fn fresh_process_reader_scores_persisted_four_and_one_bit_codes() {
    const ENV: &str = "SPANN_PACKED_READER_CHILD";
    if let Some(path) = std::env::var_os(ENV) {
        let store = GenerationStore::open(path).unwrap();
        let descriptor = store.latest().unwrap().unwrap();
        let reader = SavedReader::open(store.clone(), &descriptor).unwrap();
        assert_eq!(
            reader.validate_membership().unwrap(),
            [1, 2].into_iter().collect()
        );
        for id in [2, 3] {
            let p = reader.posting_list(id).unwrap();
            assert_eq!(p.codes.len(), Code::<1>::size(17));
            assert_eq!(p.centroid, vec![if id == 2 { 0.1 } else { 0.9 }; 17]);
        }
        assert_eq!(reader.navigation[&reader.root].codes.len(), 2);
        assert_eq!(reader.centroid_code_bytes(), 16 + 17usize.div_ceil(16) * 8);
        let vector_descriptor: VectorFileDescriptor =
            serde_json::from_slice(&store.read_object(&reader.manifest.full_vectors[0]).unwrap())
                .unwrap();
        let source = CanonicalVectorSource::open(vector_descriptor).unwrap();
        let query = source.get(1).unwrap();
        let (results, work) = reader
            .query(
                &query,
                1,
                &ReadBeamPolicy::uniform(None, 2, 2),
                2,
                Some(&source),
            )
            .unwrap();
        assert_eq!(results[0].0, 1);
        assert_eq!(results[0].1, 0.0);
        assert_eq!(work.vectors, 2);
        assert_eq!(work.navigation.distances, 2);
        assert_eq!(work.reranked, 2);
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    setup(dir.path());
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("fresh_process_reader_scores_persisted_four_and_one_bit_codes")
        .arg("--nocapture")
        .env(ENV, dir.path())
        .status()
        .unwrap();
    assert!(status.success());
}
#[test]
fn fullvector_identity_and_offsets_are_checked() {
    let dir = tempfile::tempdir().unwrap();
    let d = setup(dir.path());
    let store = GenerationStore::open(dir.path()).unwrap();
    let reader = SavedReader::open(store.clone(), &d).unwrap();
    let mut descriptor: VectorFileDescriptor =
        serde_json::from_slice(&store.read_object(&reader.manifest.full_vectors[0]).unwrap())
            .unwrap();
    let source = CanonicalVectorSource::open(descriptor.clone()).unwrap();
    assert!(source.get(0).is_err());
    assert!(source.get(3).is_err());
    assert_eq!(source.get(2).unwrap().len(), 17);
    let resident = CanonicalVectorSource::open(descriptor.clone())
        .unwrap()
        .preload()
        .unwrap();
    let first = resident.get(1).unwrap();
    let again = resident.get(1).unwrap();
    assert!(std::sync::Arc::ptr_eq(&first, &again));
    assert_eq!(&*resident.get(2).unwrap(), &*source.get(2).unwrap());
    descriptor.sha256 = "0".repeat(64);
    assert!(CanonicalVectorSource::open(descriptor).is_err());
}

#[test]
fn reader_rejects_orphan_internal_and_misowned_posting_rows() {
    for layout in [SavedLayout::V1, SavedLayout::V2] {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = setup_as(dir.path(), layout);
        let store = GenerationStore::open(dir.path()).unwrap();
        let original = store.reopen(&descriptor).unwrap();
        let reader = SavedReader::open(store.clone(), &descriptor).unwrap();
        for (case, leaf) in [
            ("orphan", 999u128),
            ("internal", 1u128),
            ("misowned", 3u128),
        ] {
            let mut generation = original.clone();
            generation.run = case.into();
            generation.save_attempt = case.into();
            generation.generation = case.into();
            let mut owners = vec![];
            for r in &original.owners {
                let mut owner: OwnerManifest =
                    serde_json::from_slice(&store.read_object(r).unwrap()).unwrap();
                owner.run = case.into();
                owner.save_attempt = case.into();
                owner.generation = case.into();
                if owner.owner == 10 {
                    let mut row = reader.posting_list(2).unwrap();
                    row.leaf = leaf;
                    owner.postings = vec![store
                        .put_object(&format!("bad-{case}"), &posting_bytes(layout, &row))
                        .unwrap()];
                }
                owners.push(store.save_owner(&owner).unwrap());
            }
            generation.owners = owners;
            let descriptor = store.save_generation(&generation).unwrap();
            assert!(
                SavedReader::open(store.clone(), &descriptor).is_err(),
                "{case}"
            );
        }
    }
}

#[test]
fn reader_open_leaves_posting_payloads_on_disk_and_checks_them_on_first_use() {
    for layout in [SavedLayout::V1, SavedLayout::V2] {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = setup_as(dir.path(), layout);
        let store = GenerationStore::open(dir.path()).unwrap();
        let manifest = store.reopen(&descriptor).unwrap();
        let owner: OwnerManifest =
            serde_json::from_slice(&store.read_object(&manifest.owners[0]).unwrap()).unwrap();
        let posting = dir.path().join(&owner.postings[0].path);
        let retained = dir.path().join("retained-posting");
        std::fs::rename(&posting, &retained).unwrap();
        let reader =
            SavedReader::open(store, &descriptor).expect("opening metadata fetched posting bytes");
        assert_eq!(reader.nodes.len(), 3);
        assert!(reader.validate_membership().is_err());
        std::fs::rename(retained, posting).unwrap();
        assert_eq!(
            reader.validate_membership().unwrap(),
            [1, 2].into_iter().collect()
        );
    }
}

#[test]
fn indexed_payload_with_wrong_leaf_fails_when_loaded_without_panicking() {
    let dir = tempfile::tempdir().unwrap();
    // Rewrites a v1 routing object; v1 generations stay readable.
    let descriptor = setup_as(dir.path(), SavedLayout::V1);
    let store = GenerationStore::open(dir.path()).unwrap();
    let mut generation = store.reopen(&descriptor).unwrap();
    generation.run = "wrongpayload".into();
    generation.save_attempt = "wrongpayload".into();
    generation.generation = "wrongpayload".into();
    let mut owners = vec![];
    for r in &generation.owners {
        let mut owner: OwnerManifest =
            serde_json::from_slice(&store.read_object(r).unwrap()).unwrap();
        owner.run = generation.run.clone();
        owner.save_attempt = generation.save_attempt.clone();
        owner.generation = generation.generation.clone();
        if owner.owner == 10 {
            let mut rows: Vec<SavedPostingRowV1> =
                bincode::deserialize(&store.read_object(&owner.postings[0]).unwrap()).unwrap();
            rows[0].leaf = 999;
            let bad = store
                .put_object("wrongpayload-posting", &bincode::serialize(&rows).unwrap())
                .unwrap();
            let mut routing: SavedOwnerRouting =
                serde_json::from_slice(&store.read_object(&owner.nodes[0]).unwrap()).unwrap();
            routing.postings[0].object = bad.clone();
            owner.postings = vec![bad];
            owner.nodes = vec![store
                .put_object(
                    "wrongpayload-routing",
                    &serde_json::to_vec(&routing).unwrap(),
                )
                .unwrap()];
        }
        owners.push(store.save_owner(&owner).unwrap());
    }
    generation.owners = owners;
    let descriptor = store.save_generation(&generation).unwrap();
    let reader = SavedReader::open(store, &descriptor).unwrap();
    assert!(reader.posting_list(2).is_err());
    assert!(reader.validate_membership().is_err());
}

#[test]
fn resident_vector_cache_budget_rejects_before_allocation() {
    let dir = tempfile::tempdir().unwrap();
    let descriptor = setup(dir.path());
    let reader =
        SavedReader::open(GenerationStore::open(dir.path()).unwrap(), &descriptor).unwrap();
    let source = reader.open_vector_source().unwrap();
    let estimate = source.estimated_resident_bytes().unwrap();
    assert!(source.preload_with_limit(estimate - 1).is_err());
    let source = reader
        .open_vector_source()
        .unwrap()
        .preload_with_limit(estimate)
        .unwrap();
    assert_eq!(source.get(1).unwrap().len(), 17);
}

#[test]
fn bounded_reader_preserves_membership_and_code_validation() {
    for layout in [SavedLayout::V1, SavedLayout::V2] {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = setup_as(dir.path(), layout);
        let store = GenerationStore::open(dir.path()).unwrap();
        let mut reader = SavedReader::open(store.clone(), &descriptor).unwrap();
        let vectors = reader.open_vector_source().unwrap().preload().unwrap();
        assert_eq!(reader.validate_codes(&vectors).unwrap(), (3, 2));
        assert_eq!(
            reader.validate_membership().unwrap(),
            [1, 2].into_iter().collect()
        );
        let mut posting = reader.posting_list(2).unwrap();
        posting.codes[0] ^= 1;
        let altered = store
            .put_object("altered-code", &posting_bytes(layout, &posting))
            .unwrap();
        reader.posting_objects.insert(2, altered);
        assert!(reader
            .validate_codes(&vectors)
            .unwrap_err()
            .contains("persisted one-bit posting differs"));
        posting.ids[0] = 2;
        let duplicate = store
            .put_object("duplicate-membership", &posting_bytes(layout, &posting))
            .unwrap();
        reader.posting_objects.insert(2, duplicate);
        assert!(reader
            .validate_membership()
            .unwrap_err()
            .contains("duplicate saved vector membership"));
    }
}

#[test]
fn reader_worker_count_is_bounded() {
    assert_eq!(reader_worker_count(None).unwrap(), 32);
    assert_eq!(reader_worker_count(Some("1")).unwrap(), 1);
    assert_eq!(reader_worker_count(Some("32")).unwrap(), 32);
    for invalid in ["", "0", "33", "-1", "many"] {
        assert!(reader_worker_count(Some(invalid)).is_err());
    }
}

#[test]
fn reader_batches_overlap_join_and_preserve_order() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Barrier,
    };
    let active = AtomicUsize::new(0);
    let peak = AtomicUsize::new(0);
    let barrier = Barrier::new(8);
    for fail in [false, true] {
        let mut visited = Vec::new();
        let result = visit_reader_batches(
            &(0..32).collect::<Vec<_>>(),
            8,
            |id| {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                barrier.wait();
                active.fetch_sub(1, Ordering::SeqCst);
                if fail && *id == 3 {
                    Err("injected immutable read failure".into())
                } else {
                    Ok(*id)
                }
            },
            |id, value| {
                assert_eq!(*id, value);
                visited.push(value);
                Ok(())
            },
        );
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(peak.load(Ordering::SeqCst), 8);
        if fail {
            assert!(result
                .unwrap_err()
                .contains("injected immutable read failure"));
            assert_eq!(visited, vec![0, 1, 2]);
        } else {
            result.unwrap();
            assert_eq!(visited, (0..32).collect::<Vec<_>>());
        }
    }
}

#[test]
fn packed_saved_children_preserve_individual_scores_and_order() {
    for dim in [1usize, 17, 256, 768] {
        let mut nodes = BTreeMap::new();
        let mut children = Vec::new();
        // Child order deliberately differs from the node map's key order.
        for (id, value) in [(9, 0.2), (3, 0.8), (7, 0.0)] {
            let child = node(id, 10, Some(1), vec![value; dim], vec![]);
            children.push(Child {
                target: child.target,
                centroid: child.centroid.clone(),
                leaf: true,
            });
            nodes.insert(id, SavedNode::from_routing(&child).unwrap());
        }
        let parent = node(1, 0, None, vec![0.4; dim], children);
        nodes.insert(1, SavedNode::from_routing(&parent).unwrap());
        let codes = || {
            nodes
                .iter()
                .map(|(id, n)| {
                    (
                        *id,
                        PackedFourBitCentroid::from_bytes(&n.centroid_code, dim).unwrap(),
                    )
                })
                .collect()
        };
        let navigation = pack_saved_children(&nodes, codes(), dim).unwrap();
        assert_eq!(navigation.len(), 1);
        let packed = &navigation[&1];
        assert_eq!(
            packed
                .children
                .iter()
                .map(|(n, _)| n.id)
                .collect::<Vec<_>>(),
            vec![9, 3, 7]
        );
        let scorer = PackedFourBitScorer::new();
        for metric in [
            DistanceFunction::Euclidean,
            DistanceFunction::Cosine,
            DistanceFunction::InnerProduct,
        ] {
            for query in [
                vec![0.0; dim],
                (0..dim).map(|i| (i as f32 - 2.0) / dim as f32).collect(),
            ] {
                let prepared = scorer.prepare_query(&query);
                let actual = packed
                    .codes
                    .distances(&scorer, &prepared, &metric)
                    .collect::<Vec<_>>();
                let expected = nodes[&1]
                    .children
                    .iter()
                    .map(|child| {
                        let code =
                            PackedFourBitCentroid::from_bytes(&nodes[&child.id].centroid_code, dim)
                                .unwrap();
                        scorer.distance(&code, &prepared, &metric)
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                    expected.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
                );
            }
        }
        let mut missing = nodes.clone();
        missing.remove(&9);
        assert!(pack_saved_children(&missing, codes(), dim).is_err());
        let mut duplicate = nodes.clone();
        duplicate
            .get_mut(&1)
            .unwrap()
            .children
            .push(nodes[&1].children[0].clone());
        assert!(pack_saved_children(&duplicate, codes(), dim).is_err());
    }
}

#[test]
fn saved_reader_open_installs_sibling_buffers_without_changing_expansion() {
    let dir = tempfile::tempdir().unwrap();
    let descriptor = setup_as(dir.path(), SavedLayout::V1);
    let reader =
        SavedReader::open(GenerationStore::open(dir.path()).unwrap(), &descriptor).unwrap();
    assert_eq!(reader.navigation.len(), 1);
    let n = &reader.nodes[&reader.root];
    for slot in 0..4 {
        let query = vec![slot as f32 / 4.0; 17];
        let prepared = reader.scorer.prepare_query(&query);
        let expected = n
            .children
            .iter()
            .enumerate()
            .map(|(ordinal, c)| {
                let code =
                    PackedFourBitCentroid::from_bytes(&reader.nodes[&c.id].centroid_code, 17)
                        .unwrap();
                ScoredChild {
                    parent: n.id,
                    ordinal,
                    target: reader.nodes[&c.id].reference(),
                    leaf: c.leaf,
                    distance: reader.scorer.distance(&code, &prepared, &reader.metric),
                    observed_revision: 0,
                }
            })
            .collect::<Vec<_>>();
        let request = ExpandRequest {
            navigation: 1,
            step: 2,
            query_slot: slot,
            query,
            nodes: vec![n.reference()],
            run: 3,
            epoch: 4,
            shared_sequence: 5,
        };
        let reply = reader.expand(n.owner, vec![request]).unwrap().remove(0);
        assert_eq!(reply.children, expected);
        assert_eq!(reply.child_counts, vec![(n.id, n.children.len())]);
    }
}

fn ramp(base: f32, dim: usize) -> Vec<f32> {
    (0..dim).map(|i| base + 0.01 * i as f32).collect()
}
/// Root (shared) -> owner 10's internal unit root 2 -> leaves 4 and 5, and
/// owner 20's leaf unit root 3. Vectors 1-2 sit in leaf 4, 3 in 5, 4 in 3.
fn setup_deep(
    path: &std::path::Path,
    layout: SavedLayout,
    vector_store: Option<VectorStoreRef>,
) -> GenerationDescriptor {
    let store = GenerationStore::open(path).unwrap();
    let dim = 17;
    let rows: Vec<Vec<f32>> = [0.2, 0.3, 0.6, 0.9].iter().map(|b| ramp(*b, dim)).collect();
    let bytes: Vec<u8> = rows
        .iter()
        .flatten()
        .flat_map(|x| x.to_le_bytes())
        .collect();
    let corpus = path.join("vectors.f32");
    std::fs::write(&corpus, &bytes).unwrap();
    let descriptor = VectorFileDescriptor {
        path: corpus.to_str().unwrap().into(),
        rows: 4,
        dimensions: dim,
        stride_bytes: dim as u64 * 4,
        offset_bytes: 0,
        first_id: 1,
        file_bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        immutable_locators: vec!["fixture:deep".into()],
    };
    let vectors = store
        .put_object(
            "vector-descriptor",
            &serde_json::to_vec(&descriptor).unwrap(),
        )
        .unwrap();
    let format = FormatContract {
        version: 1,
        dimensions: dim,
        metric: "l2".into(),
        centroid_bits: 4,
        posting_bits: 1,
        vector_bits: 32,
    };
    let posting = |leaf: u128, ids: &[u64], centroid: &[f32]| SavedPostingList {
        leaf,
        ids: ids.to_vec(),
        codes: ids
            .iter()
            .flat_map(|id| {
                Code::<1>::quantize(&rows[*id as usize - 1], centroid)
                    .as_ref()
                    .to_vec()
            })
            .collect(),
        centroid: vec![],
    };
    let child = |n: &RoutingNode| Child {
        target: n.target,
        centroid: n.centroid.clone(),
        leaf: n.leaf,
    };
    let (c4, c5, c2, c3) = (
        ramp(0.25, dim),
        ramp(0.6, dim),
        ramp(0.4, dim),
        ramp(0.9, dim),
    );
    let l4 = node(4, 10, Some(2), c4.clone(), vec![]);
    let l5 = node(5, 10, Some(2), c5.clone(), vec![]);
    let n2 = node(2, 10, Some(1), c2, vec![child(&l4), child(&l5)]);
    let l3 = node(3, 20, Some(1), c3.clone(), vec![]);
    let save = |owner, nodes: &[RoutingNode], postings: &[SavedPostingList], vector_store| {
        save_owner_snapshot_as(
            layout,
            &store,
            "run",
            "save",
            "gen",
            owner,
            vec![nodes[0].target.id],
            format.clone(),
            nodes,
            postings,
            vec![vectors.clone()],
            vector_store,
            &BTreeMap::new(),
        )
        .unwrap()
        .0
    };
    let owners = vec![
        save(
            10,
            &[n2.clone(), l4, l5],
            &[posting(4, &[1, 2], &c4), posting(5, &[3], &c5)],
            vector_store,
        ),
        save(20, &[l3.clone()], &[posting(3, &[4], &c3)], None),
    ];
    let root = node(1, 0, None, ramp(0.5, dim), vec![child(&n2), child(&l3)]);
    let shared = save_shared_routing_as(layout, &store, "shared", &[root]).unwrap();
    let d = store
        .save_generation(&GenerationManifest {
            run: "run".into(),
            save_attempt: "save".into(),
            generation: "gen".into(),
            format,
            expected_roots: vec![
                RootCoverage { root: 2, owner: 10 },
                RootCoverage { root: 3, owner: 20 },
            ],
            owners,
            shared_routing: shared,
            full_vectors: vec![vectors],
            total_vectors: 4,
        })
        .unwrap();
    store.publish_latest(&d, None).unwrap();
    d
}
fn open_deep(layout: SavedLayout) -> (tempfile::TempDir, SavedReader, CanonicalVectorSource) {
    let dir = tempfile::tempdir().unwrap();
    let d = setup_deep(dir.path(), layout, None);
    let reader = SavedReader::open(GenerationStore::open(dir.path()).unwrap(), &d).unwrap();
    let source = reader.open_vector_source().unwrap();
    (dir, reader, source)
}

#[test]
fn v2_persisted_sibling_arrays_score_exactly_like_v1_regrouped_codes() {
    let (_one, v1, source) = open_deep(SavedLayout::V1);
    let (_two, v2, _) = open_deep(SavedLayout::V2);
    assert_eq!(v1.layout, SavedLayout::V1);
    assert_eq!(v2.layout, SavedLayout::V2);
    assert_eq!(
        v1.navigation.keys().collect::<Vec<_>>(),
        v2.navigation.keys().collect::<Vec<_>>()
    );
    for (parent, packed) in &v1.navigation {
        assert_eq!(packed.children, v2.navigation[parent].children);
        let (h1, p1) = packed.codes.to_parts();
        let (h2, p2) = v2.navigation[parent].codes.to_parts();
        assert_eq!(p1, p2);
        assert_eq!(
            h1.iter().flatten().map(|v| v.to_bits()).collect::<Vec<_>>(),
            h2.iter().flatten().map(|v| v.to_bits()).collect::<Vec<_>>()
        );
    }
    // v2 routing holds no exact centroid; postings carry the leaves'.
    for (id, n) in &v2.nodes {
        assert!(n.centroid.is_empty() && n.centroid_code.is_empty());
        assert!(v2.node_centroid(*id).is_none());
        assert_eq!(n.children, v1.nodes[id].children);
    }
    for leaf in [3, 4, 5] {
        assert_eq!(
            v2.posting_list(leaf).unwrap().centroid,
            v1.node_centroid(leaf).unwrap()
        );
    }
    for slot in 0..6 {
        let query = ramp(slot as f32 / 5.0, 17);
        for (parent, owner) in [(1, 0), (2, 10)] {
            let request = ExpandRequest {
                navigation: 1,
                step: 2,
                query_slot: slot,
                query: query.clone(),
                nodes: vec![v1.nodes[&parent].reference()],
                run: 3,
                epoch: 4,
                shared_sequence: 5,
            };
            let a = v1.expand(owner, vec![request.clone()]).unwrap();
            let b = v2.expand(owner, vec![request]).unwrap();
            assert_eq!(a[0].children, b[0].children);
            assert_eq!(a[0].child_counts, b[0].child_counts);
        }
        for beam in [1, 2] {
            let policy = ReadBeamPolicy::uniform(None, beam, beam);
            let (a, wa) = v1.navigate_local(&query, &policy).unwrap();
            let (b, wb) = v2.navigate_local(&query, &policy).unwrap();
            assert_eq!(a, b);
            assert_eq!(wa.distances, wb.distances);
            for rerank in [0, 2] {
                let (a, wa) = v1.query(&query, 3, &policy, rerank, Some(&source)).unwrap();
                let (b, wb) = v2.query(&query, 3, &policy, rerank, Some(&source)).unwrap();
                assert_eq!(
                    a.iter()
                        .map(|(id, d)| (*id, d.to_bits()))
                        .collect::<Vec<_>>(),
                    b.iter()
                        .map(|(id, d)| (*id, d.to_bits()))
                        .collect::<Vec<_>>()
                );
                assert_eq!(wa.navigation.distances, wb.navigation.distances);
                assert_eq!(wa.vectors, wb.vectors);
            }
        }
    }
    assert_eq!(v1.validate_codes(&source).unwrap(), (5, 4));
    assert_eq!(v2.validate_codes(&source).unwrap(), (5, 4));
    assert_eq!(v1.centroid_code_bytes(), v2.centroid_code_bytes());
}

#[test]
fn v2_validation_detects_corrupt_packed_codes_and_exact_centroids() {
    let (dir, reader, source) = open_deep(SavedLayout::V2);
    let store = GenerationStore::open(dir.path()).unwrap();
    let four_bit = "persisted four-bit centroid differs";
    // A flipped nibble in a persisted sibling array.
    for parent in [1u128, 2] {
        let mut corrupt =
            SavedReader::open(store.clone(), &store.latest().unwrap().unwrap()).unwrap();
        let (headers, mut packed) = corrupt.navigation[&parent].codes.to_parts();
        packed[1] ^= 0x11;
        corrupt.navigation.get_mut(&parent).unwrap().codes =
            PackedFourBitCentroidBatch::from_parts(headers, packed, 17).unwrap();
        assert!(corrupt
            .validate_codes(&source)
            .unwrap_err()
            .contains(four_bit));
    }
    // An exact internal centroid that no longer matches its packed code.
    let mut corrupt = SavedReader::open(store.clone(), &store.latest().unwrap().unwrap()).unwrap();
    for r in corrupt.validation.iter_mut() {
        let mut saved: SavedValidationV2 =
            bincode::deserialize(&store.read_object(r).unwrap()).unwrap();
        if let Some((_, c)) = saved.centroids.iter_mut().find(|(id, _)| *id == 2) {
            c.iter_mut().for_each(|x| *x = -*x);
            *r = store
                .put_object("corrupt-validation", &bincode::serialize(&saved).unwrap())
                .unwrap();
        }
    }
    assert!(corrupt
        .validate_codes(&source)
        .unwrap_err()
        .contains(four_bit));
    // An exact leaf centroid in a posting object, with its codes re-derived so
    // only the four-bit check can notice.
    let mut corrupt = SavedReader::open(store.clone(), &store.latest().unwrap().unwrap()).unwrap();
    let mut posting = corrupt.posting_list(5).unwrap();
    posting.centroid = ramp(-0.6, 17);
    posting.codes = Code::<1>::quantize(&source.get(3).unwrap(), &posting.centroid)
        .as_ref()
        .to_vec();
    let moved = store
        .put_object(
            "corrupt-leaf-centroid",
            &posting_bytes(SavedLayout::V2, &posting),
        )
        .unwrap();
    corrupt.posting_objects.insert(5, moved);
    assert!(corrupt
        .validate_codes(&source)
        .unwrap_err()
        .contains(four_bit));
    // A missing internal centroid fails coverage.
    let mut corrupt = SavedReader::open(store.clone(), &store.latest().unwrap().unwrap()).unwrap();
    corrupt.validation.retain(|r| {
        let saved: SavedValidationV2 =
            bincode::deserialize(&store.read_object(r).unwrap()).unwrap();
        !saved.centroids.iter().any(|(id, _)| *id == 2)
    });
    assert!(corrupt
        .validate_codes(&source)
        .unwrap_err()
        .contains("lacks an exact centroid"));
    assert_eq!(reader.validate_codes(&source).unwrap(), (5, 4));
}

#[test]
fn v2_open_never_reads_validation_objects() {
    let dir = tempfile::tempdir().unwrap();
    let d = setup_deep(dir.path(), SavedLayout::V2, None);
    let store = GenerationStore::open(dir.path()).unwrap();
    let reader = SavedReader::open(store.clone(), &d).unwrap();
    assert_eq!(reader.validation.len(), 3);
    for r in &reader.validation {
        std::fs::write(dir.path().join(&r.path), b"corrupt").unwrap();
    }
    let reader = SavedReader::open(store, &d).expect("open must not read validation objects");
    let source = reader.open_vector_source().unwrap();
    let (results, _) = reader
        .query(
            &ramp(0.6, 17),
            1,
            &ReadBeamPolicy::uniform(None, 2, 2),
            0,
            None,
        )
        .unwrap();
    assert_eq!(results[0].0, 3);
    assert!(reader
        .validate_codes(&source)
        .unwrap_err()
        .contains("integrity mismatch"));
    for r in &reader.validation {
        std::fs::remove_file(dir.path().join(&r.path)).unwrap();
    }
    assert!(SavedReader::open(GenerationStore::open(dir.path()).unwrap(), &d).is_ok());
    assert!(reader.validate_codes(&source).is_err());
}

#[test]
fn v2_owner_routing_carries_an_optional_vector_store() {
    let reference = |owner| VectorStoreRef {
        kind: super::super::vector_store::LEAF_ORDINAL_LAYOUT.into(),
        blockfile_id: "blockfile".into(),
        prefix_path: "prefix".into(),
        owner,
        rows: 3,
        dimensions: 17,
        max_block_size_bytes: 1 << 20,
        segments: vec![],
    };
    let dir = tempfile::tempdir().unwrap();
    let d = setup_deep(dir.path(), SavedLayout::V2, Some(reference(10)));
    let reader = SavedReader::open(GenerationStore::open(dir.path()).unwrap(), &d).unwrap();
    assert_eq!(
        reader.owner_vector_stores(),
        BTreeMap::from([(10, reference(10))])
    );
    let (_dir, plain, _) = open_deep(SavedLayout::V2);
    assert!(plain.owner_vector_stores().is_empty());
    let (_dir, v1, _) = open_deep(SavedLayout::V1);
    assert!(v1.owner_vector_stores().is_empty());
    // A store reference must name the saving owner and dimensions, and v1 has none.
    let store = GenerationStore::open(dir.path()).unwrap();
    let leaf = node(9, 10, None, vec![0.5; 17], vec![]);
    let posting = SavedPostingList {
        leaf: 9,
        ids: vec![1],
        codes: Code::<1>::quantize(&[0.5; 17], &[0.5; 17])
            .as_ref()
            .to_vec(),
        centroid: vec![],
    };
    for (layout, vectors) in [
        (SavedLayout::V2, reference(20)),
        (SavedLayout::V1, reference(10)),
    ] {
        assert!(save_owner_snapshot_as(
            layout,
            &store,
            "run",
            "other",
            "other",
            10,
            vec![9],
            reader.manifest.format.clone(),
            &[leaf.clone()],
            &[posting.clone()],
            vec![],
            Some(vectors),
            &BTreeMap::new(),
        )
        .is_err());
    }
}

#[test]
fn parallel_child_scoring_matches_sequential_order_and_scores() {
    let dir = tempfile::tempdir().unwrap();
    let descriptor = setup(dir.path());
    let reader =
        SavedReader::open(GenerationStore::open(dir.path()).unwrap(), &descriptor).unwrap();
    // Repeat the internal nodes so the list splits into several runs.
    let internal: Vec<NodeRef> = reader
        .nodes
        .values()
        .filter(|n| !n.leaf)
        .map(|n| n.reference())
        .collect();
    let nodes: Vec<NodeRef> = internal
        .iter()
        .cycle()
        .take(internal.len() * 40)
        .copied()
        .collect();
    let query = vec![0.3f32; reader.manifest.format.dimensions];
    let mut sequential = vec![];
    reader
        .score_children(&nodes, &query, None, &mut sequential)
        .unwrap();
    for run in [1, 3, 16, nodes.len() + 1] {
        let mut parallel = vec![];
        reader
            .score_children_parallel(&nodes, &query, run, &mut parallel)
            .unwrap();
        assert_eq!(parallel, sequential, "run of {run} nodes");
    }
}
