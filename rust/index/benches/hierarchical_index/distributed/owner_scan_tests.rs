use super::super::generation::{FormatContract, GenerationManifest, GenerationStore, RootCoverage};
use super::super::routing::{self, Child, NodeRef, RoutingNode};
use super::super::saved_reader::{
    save_owner_snapshot, save_owner_snapshot_as, save_shared_routing, save_shared_routing_as,
    SavedLayout, VectorFileDescriptor,
};
use super::super::transport::{RpcServer, TransportConfig};
use super::*;
use sha2::{Digest, Sha256};

const DIM: usize = 17;
const TOKEN: &str = "owner-scan-test-token";

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
        revision: 1,
    }
}

fn mean(rows: &[&Vec<f32>]) -> Vec<f32> {
    (0..DIM)
        .map(|d| rows.iter().map(|r| r[d]).sum::<f32>() / rows.len() as f32)
        .collect()
}

/// Root -> one internal node per owner -> three leaves each -> four vectors per
/// leaf. Vector IDs start at 1; owner 10 holds IDs 1-12 and owner 20 holds 13-24.
fn setup(path: &std::path::Path) -> (Arc<SavedReader>, Arc<MappedVectorSource>) {
    setup_as(path, SavedLayout::V2)
}

fn setup_as(
    path: &std::path::Path,
    layout: SavedLayout,
) -> (Arc<SavedReader>, Arc<MappedVectorSource>) {
    let store = GenerationStore::open(path).unwrap();
    let mut seed = 0x9e3779b97f4a7c15u64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % 1000) as f32 / 1000.0
    };
    let vectors: Vec<Vec<f32>> = (0..24)
        .map(|i| {
            (0..DIM)
                .map(|_| next() + if i < 12 { 0.0 } else { 1.0 })
                .collect()
        })
        .collect();
    let bytes: Vec<u8> = vectors
        .iter()
        .flatten()
        .flat_map(|x| x.to_le_bytes())
        .collect();
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
    let vector_ref = store
        .put_object(
            "vector-descriptor",
            &serde_json::to_vec(&descriptor).unwrap(),
        )
        .unwrap();
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
                centroid: vec![],
            });
            let n = node(leaf, owner, Some(internal), centroid.clone(), vec![]);
            children.push(Child {
                target: n.target,
                centroid,
                leaf: true,
            });
            nodes.push(n);
        }
        let centroid = mean(&members);
        let n = node(internal, owner, Some(1), centroid.clone(), children);
        top.push(Child {
            target: n.target,
            centroid,
            leaf: false,
        });
        nodes.insert(0, n);
        owners.push(match layout {
            // The public writer saves v2.
            SavedLayout::V2 => save_owner_snapshot(
                &store,
                "run",
                "save",
                "gen",
                owner,
                vec![internal],
                format.clone(),
                &nodes,
                &postings,
                vec![vector_ref.clone()],
                None,
            )
            .unwrap(),
            SavedLayout::V1 => {
                save_owner_snapshot_as(
                    SavedLayout::V1,
                    &store,
                    "run",
                    "save",
                    "gen",
                    owner,
                    vec![internal],
                    format.clone(),
                    &nodes,
                    &postings,
                    vec![vector_ref.clone()],
                    None,
                    &Default::default(),
                )
                .unwrap()
                .0
            }
        });
    }
    let root = node(1, 0, None, mean(&vectors.iter().collect::<Vec<_>>()), top);
    let shared = match layout {
        SavedLayout::V2 => save_shared_routing(&store, "shared", &[root]).unwrap(),
        SavedLayout::V1 => {
            save_shared_routing_as(SavedLayout::V1, &store, "shared", &[root]).unwrap()
        }
    };
    let manifest = GenerationManifest {
        run: "run".into(),
        save_attempt: "save".into(),
        generation: "gen".into(),
        format,
        expected_roots: vec![
            RootCoverage {
                root: 100,
                owner: 10,
            },
            RootCoverage {
                root: 101,
                owner: 20,
            },
        ],
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
    (0..6)
        .map(|i| (0..DIM).map(|d| 0.1 * i as f32 + 0.03 * d as f32).collect())
        .collect()
}

#[test]
fn local_navigation_reaches_the_same_leaves_as_remote_expansion() {
    let dir = tempfile::tempdir().unwrap();
    let (reader, _) = setup(dir.path());
    for beam in [1, 2, 3, 6] {
        let policy = ReadBeamPolicy::uniform(None, beam, beam);
        for q in queries() {
            let (local, local_work) = reader.navigate_local(&q, &policy).unwrap();
            let (sequential, sequential_work) =
                reader.navigate_local_with(&q, &policy, false).unwrap();
            assert_eq!(sequential, local);
            assert_eq!(sequential_work.distances, local_work.distances);
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
        local: Arc::new(
            ScanEngine::new(
                reader.clone(),
                &owners,
                Some(FullVectors::Mapped(vectors.clone())),
                4,
                3,
                u64::MAX,
            )
            .unwrap(),
        ),
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
        let (expected, _) = reader
            .query(&q, 5, &policy, 10, Some(vectors.as_ref()))
            .unwrap();
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
    let engine = ScanEngine::new(
        reader.clone(),
        &owners,
        Some(FullVectors::Mapped(vectors)),
        4,
        3,
        u64::MAX,
    )
    .unwrap();
    let leaves: Vec<u128> = reader.leaves().collect();
    let q = &queries()[2];
    let (first, cold, _) = engine.scan(q, &leaves, 3, 2).unwrap();
    assert_eq!((cold.posting_fetches, cold.cache_hits), (6, 0));
    assert_eq!(cold.posting_bytes_fetched, cold.posting_object_bytes);
    let (second, warm, _) = engine.scan(q, &leaves, 3, 2).unwrap();
    assert_eq!(
        (
            warm.posting_fetches,
            warm.cache_hits,
            warm.posting_bytes_fetched
        ),
        (0, 6, 0)
    );
    assert_eq!(first, second);
    assert_eq!(engine.cached_bytes(), cold.posting_object_bytes);
    let small = ScanEngine::new(reader.clone(), &owners, None, 1, 1, 1).unwrap();
    let (_, w, _) = small.scan(q, &leaves, 3, 0).unwrap();
    assert_eq!(small.cached_bytes(), 0, "past the budget nothing is kept");
    assert_eq!(w.posting_fetches, 6);
    assert!(
        small.scan(q, &leaves, 3, 2).is_err(),
        "rerank needs vectors"
    );
}

#[test]
fn remote_owner_over_rpc_returns_the_same_results_as_local_scans() {
    let dir = tempfile::tempdir().unwrap();
    let (reader, vectors) = setup(dir.path());
    let remote_engine = Arc::new(
        ScanEngine::new(
            reader.clone(),
            &[20].into(),
            Some(FullVectors::Mapped(vectors.clone())),
            4,
            3,
            u64::MAX,
        )
        .unwrap(),
    );
    let server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        TOKEN.into(),
        TransportConfig::default(),
        Arc::new(move |request: ScanRequest| remote_engine.handle(request)),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        TOKEN.into(),
        TransportConfig::default(),
    )
    .unwrap();
    let split = OwnerQuery {
        reader: reader.clone(),
        local: Arc::new(
            ScanEngine::new(
                reader.clone(),
                &[10].into(),
                Some(FullVectors::Mapped(vectors.clone())),
                4,
                3,
                u64::MAX,
            )
            .unwrap(),
        ),
        local_owners: [10].into(),
        remotes: vec![RemoteOwner::new(20, client, 2)],
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
        local: Arc::new(
            ScanEngine::new(reader.clone(), &[10].into(), None, 1, 1, u64::MAX).unwrap(),
        ),
        local_owners: [10].into(),
        remotes: vec![],
        deadline: Duration::from_secs(1),
    };
    assert!(lonely
        .query(&queries()[0], 4, &ReadBeamPolicy::uniform(None, 6, 6), 0)
        .is_err());
}

#[test]
fn parallel_leaf_scans_return_the_same_results_as_one_thread() {
    let dir = tempfile::tempdir().unwrap();
    let (reader, vectors) = setup(dir.path());
    let owners: BTreeSet<u64> = [10, 20].into();
    let leaves: Vec<u128> = reader.leaves().collect();
    let one = ScanEngine::new(
        reader.clone(),
        &owners,
        Some(FullVectors::Mapped(vectors.clone())),
        2,
        1,
        u64::MAX,
    )
    .unwrap();
    let many = ScanEngine::new(
        reader.clone(),
        &owners,
        Some(FullVectors::Mapped(vectors.clone())),
        2,
        5,
        u64::MAX,
    )
    .unwrap();
    for q in queries() {
        for (k, rerank) in [(1, 0), (3, 0), (3, 1), (2, 3), (5, 4), (24, 1)] {
            let (a, wa, _) = one.scan(&q, &leaves, k, rerank).unwrap();
            let (b, wb, _) = many.scan(&q, &leaves, k, rerank).unwrap();
            assert_eq!(a, b, "k={k} rerank={rerank}");
            assert_eq!(
                (wa.leaves, wa.vectors, wa.reranked),
                (wb.leaves, wb.vectors, wb.reranked)
            );
        }
    }
}

#[test]
fn reranking_from_the_vector_store_matches_the_mapped_corpus() {
    use super::super::vector_store::{write_owner_vectors, VectorStoreLocation, VectorStoreReader};
    let dir = tempfile::tempdir().unwrap();
    let (reader, vectors) = setup(dir.path());
    let location = VectorStoreLocation::Local(dir.path().join("store"));
    let mut stores = BTreeMap::new();
    for owner in [10u64, 20] {
        let mut leaves: Vec<u128> = reader
            .leaves()
            .filter(|l| reader.node_owner(*l) == Some(owner))
            .collect();
        leaves.sort();
        let rows = leaves.iter().map(|leaf| {
            let p = reader.posting_list(*leaf).unwrap();
            (
                *leaf,
                p.ids
                    .iter()
                    .map(|id| Arc::<[f32]>::from(vectors.row(*id).unwrap()))
                    .collect::<Vec<_>>(),
            )
        });
        let r = write_owner_vectors(&location, owner, DIM, 16 * 1024, rows).unwrap();
        stores.insert(
            owner,
            Arc::new(VectorStoreReader::open(&location, &r, 1 << 20).unwrap()),
        );
    }
    let owners: BTreeSet<u64> = [10, 20].into();
    let leaves: Vec<u128> = reader.leaves().collect();
    let mapped = ScanEngine::new(
        reader.clone(),
        &owners,
        Some(FullVectors::Mapped(vectors.clone())),
        2,
        3,
        u64::MAX,
    )
    .unwrap();
    let stored = ScanEngine::new(
        reader.clone(),
        &owners,
        Some(FullVectors::Store(stores)),
        2,
        3,
        u64::MAX,
    )
    .unwrap();
    for q in queries() {
        for (k, rerank) in [(3, 1), (4, 3), (10, 2)] {
            let (a, _, _) = mapped.scan(&q, &leaves, k, rerank).unwrap();
            let (b, w, _) = stored.scan(&q, &leaves, k, rerank).unwrap();
            assert_eq!(a, b, "k={k} rerank={rerank}");
            assert_eq!(w.rerank_row_bytes, w.reranked * DIM as u64 * 4);
        }
    }
    let (_, warm, _) = stored.scan(&queries()[0], &leaves, 4, 3).unwrap();
    assert_eq!(
        warm.rerank_blocks_loaded, 0,
        "repeat reranks hit the block cache"
    );
}

#[test]
fn v2_scans_use_posting_centroids_and_match_v1_scores_exactly() {
    let (one, two) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (v1, vectors) = setup_as(one.path(), SavedLayout::V1);
    let (v2, _) = setup_as(two.path(), SavedLayout::V2);
    let owners: BTreeSet<u64> = [10, 20].into();
    let leaves: Vec<u128> = v2.leaves().collect();
    for leaf in &leaves {
        // v2 routing holds no exact centroid; scanning must read the posting's.
        assert!(v2.node_centroid(*leaf).is_none());
        assert_eq!(
            v2.posting_list(*leaf).unwrap().centroid,
            v1.node_centroid(*leaf).unwrap()
        );
        assert!(v1.posting_list(*leaf).unwrap().centroid.is_empty());
    }
    let first = ScanEngine::new(
        v1.clone(),
        &owners,
        Some(FullVectors::Mapped(vectors.clone())),
        4,
        3,
        u64::MAX,
    )
    .unwrap();
    let second = ScanEngine::new(
        v2.clone(),
        &owners,
        Some(FullVectors::Mapped(vectors.clone())),
        4,
        3,
        u64::MAX,
    )
    .unwrap();
    let policy = ReadBeamPolicy::uniform(None, 3, 3);
    for q in queries() {
        let bits = |r: Vec<(u64, f32)>| {
            r.into_iter()
                .map(|(id, d)| (id, d.to_bits()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            bits(first.scan(&q, &leaves, 24, 0).unwrap().0),
            bits(second.scan(&q, &leaves, 24, 0).unwrap().0)
        );
        let (a, wa) = v1.navigate_local(&q, &policy).unwrap();
        let (b, wb) = v2.navigate_local(&q, &policy).unwrap();
        assert_eq!(a, b);
        assert_eq!(wa.distances, wb.distances);
        for rerank in [0, 2] {
            let (a, _) = v1
                .query(&q, 5, &policy, rerank, Some(vectors.as_ref()))
                .unwrap();
            let (b, _) = v2
                .query(&q, 5, &policy, rerank, Some(vectors.as_ref()))
                .unwrap();
            assert_eq!(bits(a), bits(b));
        }
    }
    assert_eq!(v1.validate_codes(vectors.as_ref()).unwrap(), (9, 24));
    assert_eq!(v2.validate_codes(vectors.as_ref()).unwrap(), (9, 24));
}

#[test]
fn traced_navigation_matches_local_navigation_and_records_each_level() {
    let dir = tempfile::tempdir().unwrap();
    let (reader, _) = setup(dir.path());
    for beam in [1, 2, 6] {
        let policy = ReadBeamPolicy::uniform(None, beam, beam);
        for q in queries() {
            let (local, _) = reader.navigate_local(&q, &policy).unwrap();
            let (traced, frontiers) = reader.navigate_traced(&q, &policy).unwrap();
            assert_eq!(traced, local);
            // Root, then the internal nodes kept at level 1.
            assert_eq!(frontiers.len(), 2);
            assert_eq!(frontiers[0], vec![1]);
            assert_eq!(frontiers[1].len(), beam.min(2));
        }
    }
}

/// The leaf scan as it stood before the shortlist cutoff: per-leaf
/// allocations, every vector stored as a candidate and fixed chunks. It is
/// the reference the fast scan must match and the baseline it is timed
/// against.
fn reference_scan(
    pool: &rayon::ThreadPool,
    q: &ScanQuery,
    leaves: &[LeafInput],
) -> (Vec<Candidate>, ChunkWork) {
    let dim = q.query.len();
    let keep = q.keep;
    let threads = pool.current_num_threads();
    let chunk = leaves.len().div_ceil(threads * 2).max(1);
    let slots: Vec<usize> = (0..leaves.len()).collect();
    let scored: Vec<Result<(Vec<Candidate>, ChunkWork)>> = pool.install(|| {
        slots
            .par_chunks(chunk)
            .map(|chunk_slots| {
                let mut out = Vec::with_capacity(keep * 2);
                let mut cw = ChunkWork::default();
                let mut distances = vec![];
                for &slot in chunk_slots {
                    let p = &leaves[slot];
                    let centroid = p.centroid;
                    let residual: Vec<_> =
                        q.query.iter().zip(centroid).map(|(q, c)| q - c).collect();
                    let norm = centroid.iter().map(|x| x * x).sum::<f32>().sqrt();
                    let dot = centroid.iter().zip(q.query).map(|(c, q)| c * q).sum();
                    let qq = QuantizedQuery::new(
                        &residual,
                        Code::<1>::packed_len(dim),
                        norm,
                        dot,
                        q.q_norm,
                    );
                    distances.clear();
                    distances.resize(p.ids.len(), 0.0);
                    Code::<1>::distances_quantized_query(p.codes, q.metric, &qq, &mut distances);
                    if distances.iter().any(|d| !d.is_finite()) {
                        return Err("nonfinite saved posting score".to_string());
                    }
                    cw.leaves += 1;
                    cw.vectors += p.ids.len() as u64;
                    cw.posting_bytes_scanned += p.codes.len() as u64 + p.ids.len() as u64 * 8;
                    out.extend(p.ids.iter().zip(&distances).enumerate().map(
                        |(ordinal, (id, d))| Candidate {
                            id: *id,
                            distance: *d,
                            slot: p.slot,
                            ordinal: ordinal as u32,
                        },
                    ));
                    if out.len() >= keep * 2 {
                        trim(&mut out, keep);
                    }
                }
                trim(&mut out, keep);
                Ok((out, cw))
            })
            .collect()
    });
    let mut shortlist = vec![];
    let mut work = ChunkWork::default();
    for result in scored {
        let (part, cw) = result.unwrap();
        work.add(&cw);
        shortlist.extend(part);
    }
    trim(&mut shortlist, keep);
    (shortlist, work)
}

/// Candidates as comparable keys, best first. Slot and ordinal are left out:
/// equal keys from different leaves may legally swap which copy survives.
fn keys(mut c: Vec<Candidate>) -> Vec<(u32, u64)> {
    c.sort_unstable_by(candidate_order);
    c.into_iter()
        .map(|c| (c.distance.to_bits(), c.id))
        .collect()
}

struct Synthetic {
    dim: usize,
    centroids: Vec<Vec<f32>>,
    ids: Vec<Vec<u64>>,
    codes: Vec<Vec<u8>>,
    norms: Vec<f32>,
}
impl Synthetic {
    /// `leaves` leaves of `per_leaf` vectors each. With `ties`, every vector in
    /// a leaf is the same point and IDs repeat across leaves, so distances tie
    /// in long runs and equal (distance, ID) keys appear more than once.
    fn new(leaves: usize, per_leaf: usize, dim: usize, ties: bool, seed: u64) -> Self {
        let mut s = seed | 1;
        let mut next = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 11) as f32 / (1u64 << 53) as f32 * 2.0 - 1.0
        };
        let mut out = Synthetic {
            dim,
            centroids: vec![],
            ids: vec![],
            codes: vec![],
            norms: vec![],
        };
        for l in 0..leaves {
            let centroid: Vec<f32> = (0..dim).map(|_| next()).collect();
            let mut codes = Vec::with_capacity(per_leaf * Code::<1>::size(dim));
            let shared: Vec<f32> = centroid.iter().map(|c| c + 0.3 * next()).collect();
            for _ in 0..per_leaf {
                let v: Vec<f32> = if ties {
                    shared.clone()
                } else {
                    centroid.iter().map(|c| c + 0.3 * next()).collect()
                };
                codes.extend_from_slice(Code::<1>::quantize(&v, &centroid).as_ref());
            }
            let ids = if ties {
                (0..per_leaf as u64).map(|i| i % 7).collect()
            } else {
                (0..per_leaf as u64)
                    .map(|i| (i * leaves as u64 + l as u64).wrapping_mul(0x9e37_79b9) % (1 << 40))
                    .collect()
            };
            out.norms.push(norm(&centroid));
            out.centroids.push(centroid);
            out.ids.push(ids);
            out.codes.push(codes);
        }
        out
    }
    fn inputs(&self) -> Vec<LeafInput<'_>> {
        (0..self.centroids.len())
            .map(|i| LeafInput {
                slot: i as u32,
                ids: &self.ids[i],
                codes: &self.codes[i],
                centroid: &self.centroids[i],
                centroid_norm: self.norms[i],
            })
            .collect()
    }
    fn query(&self, seed: u64) -> Vec<f32> {
        let c = &self.centroids[seed as usize % self.centroids.len()];
        c.iter()
            .enumerate()
            .map(|(d, x)| x + 0.01 * ((d as u64 * 31 + seed) % 17) as f32)
            .collect()
    }
}

fn pool(threads: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap()
}

#[test]
fn shortlist_cutoff_matches_a_full_sort_on_ties() {
    // Few distinct distances and IDs, so most candidates tie with the cutoff.
    let mut s = 7u64;
    let mut next = move |m: u64| {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s % m
    };
    for keep in [1, 2, 3, 10, 100] {
        for (distinct_d, distinct_id) in [(1, 1), (1, 50), (3, 3), (5, 1000), (1000, 5)] {
            let all: Vec<Candidate> = (0..5000)
                .map(|o| Candidate {
                    id: next(distinct_id),
                    distance: [0.0f32, -0.0, 1.0, 2.5, -3.0][next(5) as usize]
                        * next(distinct_d) as f32,
                    slot: 0,
                    ordinal: o,
                })
                .collect();
            let mut list = Shortlist::new(keep);
            let mut other = Shortlist::new(keep);
            for (i, c) in all.iter().enumerate() {
                let into = if i % 3 == 0 { &mut other } else { &mut list };
                if into.admits(c.distance, c.id) {
                    into.push(*c);
                }
            }
            list.merge(other);
            let mut full = all.clone();
            full.sort_unstable_by(candidate_order);
            full.truncate(keep);
            assert_eq!(
                keys(list.finish()),
                keys(full),
                "keep={keep} d={distinct_d} id={distinct_id}"
            );
        }
    }
    // Leaves of identical points, where every score in a leaf ties and IDs
    // repeat, and leaves of distinct points. Both are large enough to split
    // across the scan pool.
    let metric = chroma_distance::DistanceFunction::Euclidean;
    for ties in [true, false] {
        let data = Synthetic::new(40, 600, 64, ties, 3);
        let inputs = data.inputs();
        for keep in [1, 5, 13, 100, 2000] {
            let query = data.query(keep as u64);
            let q = ScanQuery {
                query: &query,
                q_norm: norm(&query),
                metric: &metric,
                keep,
            };
            let (expected, ew) = reference_scan(&pool(1), &q, &inputs);
            for threads in [1, 3, 8] {
                let (got, gw) = score_leaves(&pool(threads), &q, &inputs).unwrap();
                assert_eq!(
                    keys(got),
                    keys(expected.clone()),
                    "keep={keep} threads={threads}"
                );
                assert_eq!(gw, ew);
            }
        }
    }
}

/// Times the leaf scan on a production-shaped request: 300 leaves of 1,450
/// 768-dimension 1-bit codes, k = 10 and rerank 10. Run with
/// `--release -- --ignored --nocapture`.
#[test]
#[ignore]
fn scan_microbenchmark() {
    let (leaves, per_leaf, dim, keep) = (300, 1450, 768, 100);
    let data = Synthetic::new(leaves, per_leaf, dim, false, 11);
    let inputs = data.inputs();
    let metric = chroma_distance::DistanceFunction::Euclidean;
    let vectors = (leaves * per_leaf) as f64;
    let queries: Vec<Vec<f32>> = (0..40).map(|i| data.query(i * 7 + 1)).collect();
    // Rounds alternate the two scans so load from other processes hits both
    // alike; each reports its fastest round.
    for threads in [1, 8, 16] {
        let p = pool(threads);
        let mut best = [(f64::MAX, f64::MAX); 2];
        for _round in 0..7 {
            for (v, fast) in [false, true].into_iter().enumerate() {
                let run = |query: &Vec<f32>| {
                    let q = ScanQuery {
                        query,
                        q_norm: norm(query),
                        metric: &metric,
                        keep,
                    };
                    if fast {
                        score_leaves(&p, &q, &inputs).unwrap()
                    } else {
                        reference_scan(&p, &q, &inputs)
                    }
                };
                for query in queries.iter().take(2) {
                    run(query);
                }
                let cpu = super::super::reader_bench::process_cpu_seconds();
                let t = Instant::now();
                for query in &queries {
                    std::hint::black_box(run(query));
                }
                let wall = t.elapsed().as_secs_f64();
                let cpu = super::super::reader_bench::process_cpu_seconds() - cpu;
                best[v] = (best[v].0.min(wall), best[v].1.min(cpu));
            }
        }
        let n = queries.len() as f64 * vectors;
        for (name, (wall, cpu)) in ["reference", "shortlist"].into_iter().zip(best) {
            println!(
                "threads={threads:2} {name:9} wall {:6.2} ns/vector  cpu {:6.2} ns/vector  scan {:7.3} ms",
                wall * 1e9 / n,
                cpu * 1e9 / n,
                wall * 1e3 / queries.len() as f64
            );
        }
        for query in &queries {
            let q = ScanQuery {
                query,
                q_norm: norm(query),
                metric: &metric,
                keep,
            };
            let (a, wa) = reference_scan(&p, &q, &inputs);
            let (b, wb) = score_leaves(&p, &q, &inputs).unwrap();
            assert_eq!(keys(a), keys(b));
            assert_eq!(wa, wb);
        }
    }
}

/// Loopback cost of one scan RPC with a realistic payload (768-dim query, 300
/// leaves, 10 results) and a trivial handler: the transport's fixed cost.
/// Run with `cargo test --release ... scan_rpc_loopback -- --ignored --nocapture`.
#[test]
#[ignore]
fn scan_rpc_loopback_cost() {
    use super::super::transport::{current_server_request, last_call_timing, RequestClass};
    let server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        TOKEN.into(),
        TransportConfig::default(),
        Arc::new(move |request: ScanRequest| -> Result<ScanReply> {
            let begin = Instant::now();
            let server = current_server_request().unwrap_or_default();
            let query: Vec<f32> = request.query.iter().map(|b| f32::from_bits(*b)).collect();
            let results = (0..request.k as u64)
                .map(|i| {
                    (
                        i * 7 + request.leaves[0] as u64,
                        query[i as usize].to_bits(),
                    )
                })
                .collect();
            Ok(ScanReply {
                results,
                work: ScanWork::default(),
                timing: ScanTiming {
                    total: 1,
                    ..Default::default()
                },
                cpu_seconds: 0.0,
                heap_peak_bytes: 0,
                server,
                server_handler_ns: begin.elapsed().as_nanos() as u64,
            })
        }),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        TOKEN.into(),
        TransportConfig::default(),
    )
    .unwrap();
    let mut seed = 0x2545f4914f6cdd1du64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let request = ScanRequest {
        query: (0..768)
            .map(|_| ((next() % 2000) as f32 / 1000.0 - 1.0).to_bits())
            .collect(),
        leaves: (0..300)
            .map(|_| (next() as u128) << 64 | next() as u128)
            .collect(),
        k: 10,
        rerank: 4,
    };
    let mut rows = vec![];
    for i in 0..3000 {
        let before = client.counters();
        let t = Instant::now();
        let reply: ScanReply = client
            .call(
                &request,
                RequestClass::Data,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap();
        let total = t.elapsed().as_nanos() as u64;
        assert_eq!(reply.results.len(), 10);
        assert_eq!(reply.results[3].1, request.query[3]);
        let call = last_call_timing();
        let (sent, received) = delta(&client.counters(), &before);
        if i >= 500 {
            rows.push([
                total,
                call.connect_ns,
                call.encode_ns,
                call.write_ns,
                call.first_byte_wait_ns,
                call.read_ns,
                call.decode_ns,
                reply.server.before_handler_ns,
                reply.server.read_ns,
                reply.server.decode_queue_ns,
                reply.server.decode_ns,
                reply.server.dispatch_ns,
                reply.server.worker_wake_ns,
                reply.server_handler_ns,
                sent,
                received,
            ]);
        }
    }
    let names = [
        "round_trip",
        "connect",
        "encode",
        "write",
        "first_byte_wait",
        "read",
        "decode",
        "server_before_handler",
        "  server_read",
        "  server_decode_queue",
        "  server_decode",
        "  server_dispatch",
        "  server_worker_wake",
        "server_handler",
        "bytes_sent",
        "bytes_received",
    ];
    for (c, name) in names.iter().enumerate() {
        let mut v: Vec<u64> = rows.iter().map(|r| r[c]).collect();
        v.sort_unstable();
        let pct = |p: f64| v[((v.len() - 1) as f64 * p) as usize];
        let bytes = c + 2 >= names.len();
        let unit = if bytes { 1.0 } else { 1e3 };
        println!(
            "{name:>22}: p50 {:>8.1} p90 {:>8.1} p99 {:>8.1} {}",
            pct(0.5) as f64 / unit,
            pct(0.9) as f64 / unit,
            pct(0.99) as f64 / unit,
            if bytes { "B" } else { "us" }
        );
    }
}

#[test]
fn scan_payloads_round_trip_exact_bits_as_base64_rows() {
    let request = ScanRequest {
        query: vec![
            0,
            1,
            0x7f7f_ffff,
            0x8000_0000,
            0xffff_ffff,
            1.5f32.to_bits(),
        ],
        leaves: vec![0, 1, u128::MAX, (7u128 << 100) | 3],
        k: 3,
        rerank: 2,
    };
    let json = serde_json::to_value(&request).unwrap();
    assert!(
        json["query_u32le_v1_base64"].is_string() && json["leaves_u128le_v1_base64"].is_string()
    );
    let back: ScanRequest = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(
        (back.query, back.leaves, back.k, back.rerank),
        (request.query, request.leaves, 3, 2)
    );
    let reply = ScanReply {
        results: vec![(u64::MAX, (-0.0f32).to_bits()), (9, 2.25f32.to_bits())],
        work: ScanWork::default(),
        timing: ScanTiming::default(),
        cpu_seconds: 0.0,
        heap_peak_bytes: 0,
        server: Default::default(),
        server_handler_ns: 0,
    };
    let back: ScanReply = serde_json::from_slice(&serde_json::to_vec(&reply).unwrap()).unwrap();
    assert_eq!(back.results, reply.results);
    // A payload that is not whole rows, or the old decimal field, is refused.
    let mut torn = json.clone();
    torn["leaves_u128le_v1_base64"] = "AAAA".into();
    assert!(serde_json::from_value::<ScanRequest>(torn).is_err());
    let old = serde_json::json!({"query": [1, 2], "leaves": [3], "k": 1, "rerank": 0});
    assert!(serde_json::from_value::<ScanRequest>(old).is_err());
}

fn placed(reader: &Arc<SavedReader>, vectors: &Arc<MappedVectorSource>, parts: u64) -> OwnerQuery {
    let placement = Arc::new(Placement::subtrees(reader, parts).unwrap());
    let all: BTreeSet<u64> = placement.leaves_by_part().into_keys().collect();
    OwnerQuery {
        reader: reader.clone(),
        local: Arc::new(
            ScanEngine::with_placement(
                reader.clone(),
                placement,
                &all,
                Some(FullVectors::Mapped(vectors.clone())),
                4,
                3,
                u64::MAX,
            )
            .unwrap(),
        ),
        local_owners: all,
        remotes: vec![],
        deadline: Duration::from_secs(10),
    }
}

#[test]
fn subtree_placement_covers_every_leaf_in_consecutive_runs() {
    let dir = tempfile::tempdir().unwrap();
    let (reader, _) = setup(dir.path());
    let order = reader.leaves_depth_first();
    assert_eq!(order.len(), 6);
    for parts in 1..=6 {
        let placement = Placement::subtrees(&reader, parts).unwrap();
        let labels: Vec<u64> = order.iter().map(|l| placement.part(*l).unwrap()).collect();
        assert!(
            labels.windows(2).all(|w| w[0] <= w[1]),
            "parts {parts}: {labels:?}"
        );
        assert_eq!(
            placement.leaves_by_part().len() as u64,
            parts,
            "parts {parts}: {labels:?}"
        );
    }
    assert!(Placement::subtrees(&reader, 7).is_err());
}

#[test]
fn read_time_parts_return_the_same_one_bit_results_for_any_part_count() {
    // Without rerank, each part returns its best k by 1-bit score and the
    // merge keeps the overall best k, so the part count cannot change results.
    let dir = tempfile::tempdir().unwrap();
    let (reader, vectors) = setup(dir.path());
    let policy = ReadBeamPolicy::uniform(None, 6, 6);
    for q in queries() {
        let (one, _) = placed(&reader, &vectors, 1)
            .query(&q, 5, &policy, 0)
            .unwrap();
        for parts in [2, 3, 6] {
            let (got, record) = placed(&reader, &vectors, parts)
                .query(&q, 5, &policy, 0)
                .unwrap();
            assert_eq!(got, one, "parts {parts}");
            assert_eq!(record.total.leaves, 6);
        }
    }
}
