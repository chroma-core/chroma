use super::super::generation::{FormatContract, GenerationManifest, GenerationStore, RootCoverage};
use super::super::routing::{self, Child, NodeRef, RoutingNode};
use super::super::saved_reader::{save_owner_snapshot, save_shared_routing, VectorFileDescriptor};
use super::super::transport::{RpcServer, TransportConfig};
use super::*;
use sha2::{Digest, Sha256};

const DIM: usize = 17;
const TOKEN: &str = "owner-scan-test-token";

fn node(id: u128, owner: u64, parent: Option<u128>, centroid: Vec<f32>, children: Vec<Child>) -> RoutingNode {
    RoutingNode {
        navigation: Default::default(),
        target: NodeRef { id, authority: owner, placement: 9 },
        parent: parent.map(|id| NodeRef { id, authority: 0, placement: 9 }),
        centroid,
        centroid_code: vec![],
        leaf: children.is_empty(),
        children,
        revision: 1,
    }
}

fn mean(rows: &[&Vec<f32>]) -> Vec<f32> {
    (0..DIM).map(|d| rows.iter().map(|r| r[d]).sum::<f32>() / rows.len() as f32).collect()
}

/// Root -> one internal node per owner -> three leaves each -> four vectors per
/// leaf. Vector IDs start at 1; owner 10 holds IDs 1-12 and owner 20 holds 13-24.
fn setup(path: &std::path::Path) -> (Arc<SavedReader>, Arc<MappedVectorSource>) {
    let store = GenerationStore::open(path).unwrap();
    let mut seed = 0x9e3779b97f4a7c15u64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % 1000) as f32 / 1000.0
    };
    let vectors: Vec<Vec<f32>> = (0..24)
        .map(|i| (0..DIM).map(|_| next() + if i < 12 { 0.0 } else { 1.0 }).collect())
        .collect();
    let bytes: Vec<u8> = vectors.iter().flatten().flat_map(|x| x.to_le_bytes()).collect();
    let corpus = path.join("vectors.f32");
    std::fs::write(&corpus, &bytes).unwrap();
    let descriptor = VectorFileDescriptor {
        path: corpus.to_str().unwrap().into(),
        rows: 24,
        dimensions: DIM,
        stride_bytes: DIM as u64 * 4,
        offset_bytes: 0,
        first_id: 1,
        file_bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        immutable_locators: vec!["fixture:owner-scan".into()],
    };
    let vector_ref = store.put_object("vector-descriptor", &serde_json::to_vec(&descriptor).unwrap()).unwrap();
    let format = FormatContract {
        version: 1,
        dimensions: DIM,
        metric: "l2".into(),
        centroid_bits: 4,
        posting_bits: 1,
        vector_bits: 32,
    };
    let mut owners = vec![];
    let mut top = vec![];
    for (o, owner) in [10u64, 20].into_iter().enumerate() {
        let internal = 100 + o as u128;
        let mut nodes = vec![];
        let mut children = vec![];
        let mut postings = vec![];
        let members: Vec<&Vec<f32>> = vectors[o * 12..o * 12 + 12].iter().collect();
        for l in 0..3 {
            let leaf = 1000 + (o * 3 + l) as u128;
            let first = o * 12 + l * 4;
            let rows: Vec<&Vec<f32>> = vectors[first..first + 4].iter().collect();
            let centroid = mean(&rows);
            let mut codes = vec![];
            for v in &rows {
                codes.extend_from_slice(Code::<1>::quantize(v.as_slice(), &centroid).as_ref());
            }
            postings.push(SavedPostingList {
                leaf,
                ids: (first as u64 + 1..first as u64 + 5).collect(),
                codes,
            });
            let n = node(leaf, owner, Some(internal), centroid.clone(), vec![]);
            children.push(Child { target: n.target, centroid, leaf: true });
            nodes.push(n);
        }
        let centroid = mean(&members);
        let n = node(internal, owner, Some(1), centroid.clone(), children);
        top.push(Child { target: n.target, centroid, leaf: false });
        nodes.insert(0, n);
        owners.push(
            save_owner_snapshot(&store, "run", "save", "gen", owner, vec![internal], format.clone(), &nodes,
                &postings, vec![vector_ref.clone()])
            .unwrap(),
        );
    }
    let root = node(1, 0, None, mean(&vectors.iter().collect::<Vec<_>>()), top);
    let shared = save_shared_routing(&store, "shared", &[root]).unwrap();
    let manifest = GenerationManifest {
        run: "run".into(),
        save_attempt: "save".into(),
        generation: "gen".into(),
        format,
        expected_roots: vec![RootCoverage { root: 100, owner: 10 }, RootCoverage { root: 101, owner: 20 }],
        owners,
        shared_routing: shared,
        full_vectors: vec![vector_ref],
        total_vectors: 24,
    };
    let d = store.save_generation(&manifest).unwrap();
    let reader = SavedReader::open(store, &d).unwrap();
    let (source, _) = MappedVectorSource::open(descriptor, Some(path)).unwrap();
    (Arc::new(reader), Arc::new(source))
}

fn queries() -> Vec<Vec<f32>> {
    (0..6).map(|i| (0..DIM).map(|d| 0.1 * i as f32 + 0.03 * d as f32).collect()).collect()
}

#[test]
fn local_navigation_reaches_the_same_leaves_as_remote_expansion() {
    let dir = tempfile::tempdir().unwrap();
    let (reader, _) = setup(dir.path());
    for beam in [1, 2, 3, 6] {
        let policy = ReadBeamPolicy::uniform(None, beam, beam);
        for q in queries() {
            let (local, local_work) = reader.navigate_local(&q, &policy).unwrap();
            let (expected, work) = routing::navigate_batch(
                reader.as_ref(),
                &reader.root_routing(),
                &[q.clone()],
                &policy,
                0,
                0,
                0,
                0,
                None,
            )
            .unwrap();
            assert_eq!(local, expected[0]);
            assert_eq!(local_work.distances, work.distances);
            assert_eq!(local_work.owner_requests, 0);
        }
    }
}

fn all_local(reader: &Arc<SavedReader>, vectors: &Arc<MappedVectorSource>) -> OwnerQuery {
    let owners: BTreeSet<u64> = [10, 20].into();
    OwnerQuery {
        reader: reader.clone(),
        local: Arc::new(ScanEngine::new(reader.clone(), &owners, Some(vectors.clone()), 4, 3, u64::MAX).unwrap()),
        local_owners: owners,
        remotes: vec![],
        deadline: Duration::from_secs(10),
    }
}

#[test]
fn per_owner_scans_match_the_single_process_reader_when_rerank_covers_every_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let (reader, vectors) = setup(dir.path());
    let query = all_local(&reader, &vectors);
    let policy = ReadBeamPolicy::uniform(None, 6, 6);
    for q in queries() {
        let (expected, _) = reader.query(&q, 5, &policy, 10, Some(vectors.as_ref())).unwrap();
        let (got, record) = query.query(&q, 5, &policy, 10).unwrap();
        assert_eq!(got, expected);
        assert_eq!(record.total.leaves, 6);
        assert_eq!(record.parts.len(), 2);
        assert!(record.parts.iter().all(|p| !p.remote));
    }
}

#[test]
fn cached_postings_are_fetched_once() {
    let dir = tempfile::tempdir().unwrap();
    let (reader, vectors) = setup(dir.path());
    let owners: BTreeSet<u64> = [10, 20].into();
    let engine = ScanEngine::new(reader.clone(), &owners, Some(vectors), 4, 3, u64::MAX).unwrap();
    let leaves: Vec<u128> = reader.leaves().collect();
    let q = &queries()[2];
    let (first, cold, _) = engine.scan(q, &leaves, 3, 2).unwrap();
    assert_eq!((cold.posting_fetches, cold.cache_hits), (6, 0));
    assert_eq!(cold.posting_bytes_fetched, cold.posting_object_bytes);
    let (second, warm, _) = engine.scan(q, &leaves, 3, 2).unwrap();
    assert_eq!((warm.posting_fetches, warm.cache_hits, warm.posting_bytes_fetched), (0, 6, 0));
    assert_eq!(first, second);
    assert_eq!(engine.cached_bytes(), cold.posting_object_bytes);
    let small = ScanEngine::new(reader.clone(), &owners, None, 1, 1, 1).unwrap();
    let (_, w, _) = small.scan(q, &leaves, 3, 0).unwrap();
    assert_eq!(small.cached_bytes(), 0, "past the budget nothing is kept");
    assert_eq!(w.posting_fetches, 6);
    assert!(small.scan(q, &leaves, 3, 2).is_err(), "rerank needs vectors");
}

#[test]
fn remote_owner_over_rpc_returns_the_same_results_as_local_scans() {
    let dir = tempfile::tempdir().unwrap();
    let (reader, vectors) = setup(dir.path());
    let remote_engine =
        Arc::new(ScanEngine::new(reader.clone(), &[20].into(), Some(vectors.clone()), 4, 3, u64::MAX).unwrap());
    let server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        TOKEN.into(),
        TransportConfig::default(),
        Arc::new(move |request: ScanRequest| remote_engine.handle(request)),
    )
    .unwrap();
    let client =
        RpcClient::new(server.data_addr(), server.control_addr(), TOKEN.into(), TransportConfig::default()).unwrap();
    let split = OwnerQuery {
        reader: reader.clone(),
        local: Arc::new(ScanEngine::new(reader.clone(), &[10].into(), Some(vectors.clone()), 4, 3, u64::MAX).unwrap()),
        local_owners: [10].into(),
        remotes: vec![RemoteOwner { owner: 20, client }],
        deadline: Duration::from_secs(10),
    };
    let together = all_local(&reader, &vectors);
    for rerank in [0, 1, 3] {
        let policy = ReadBeamPolicy::uniform(None, 6, 6);
        for q in queries() {
            let (expected, _) = together.query(&q, 4, &policy, rerank).unwrap();
            let (got, record) = split.query(&q, 4, &policy, rerank).unwrap();
            assert_eq!(got, expected);
            let remote = record.parts.iter().find(|p| p.remote).unwrap();
            assert_eq!(remote.owner, 20);
            assert!(remote.transport_bytes_sent > 0 && remote.transport_bytes_received > 0);
        }
    }
    let lonely = OwnerQuery {
        reader: reader.clone(),
        local: Arc::new(ScanEngine::new(reader.clone(), &[10].into(), None, 1, 1, u64::MAX).unwrap()),
        local_owners: [10].into(),
        remotes: vec![],
        deadline: Duration::from_secs(1),
    };
    assert!(lonely.query(&queries()[0], 4, &ReadBeamPolicy::uniform(None, 6, 6), 0).is_err());
}

#[test]
fn parallel_leaf_scans_return_the_same_results_as_one_thread() {
    let dir = tempfile::tempdir().unwrap();
    let (reader, vectors) = setup(dir.path());
    let owners: BTreeSet<u64> = [10, 20].into();
    let leaves: Vec<u128> = reader.leaves().collect();
    let one = ScanEngine::new(reader.clone(), &owners, Some(vectors.clone()), 2, 1, u64::MAX).unwrap();
    let many = ScanEngine::new(reader.clone(), &owners, Some(vectors.clone()), 2, 5, u64::MAX).unwrap();
    for q in queries() {
        for (k, rerank) in [(1, 0), (3, 0), (3, 1), (2, 3), (5, 4), (24, 1)] {
            let (a, wa, _) = one.scan(&q, &leaves, k, rerank).unwrap();
            let (b, wb, _) = many.scan(&q, &leaves, k, rerank).unwrap();
            assert_eq!(a, b, "k={k} rerank={rerank}");
            assert_eq!((wa.leaves, wa.vectors, wa.reranked), (wb.leaves, wb.vectors, wb.reranked));
        }
    }
}
