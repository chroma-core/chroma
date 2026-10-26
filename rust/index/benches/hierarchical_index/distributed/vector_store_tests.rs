use super::*;

const DIM: usize = 768;
/// Small blocks (about 5 vectors) so a few leaves span many blocks.
const SMALL_BLOCK: usize = MIN_BLOCK_SIZE_BYTES;

fn vector(leaf: u128, position: usize) -> Arc<[f32]> {
    (0..DIM)
        .map(|d| (leaf as f32) * 1000.0 + position as f32 + d as f32 / 1024.0)
        .collect::<Vec<_>>()
        .into()
}

fn leaves() -> Vec<(u128, Vec<Arc<[f32]>>)> {
    [
        (3u128, 7usize),
        (10, 20),
        (11, 0),
        (1u128 << 100, 5),
        (u128::MAX, 9),
    ]
    .into_iter()
    .map(|(leaf, n)| (leaf, (0..n).map(|i| vector(leaf, i)).collect()))
    .collect()
}

fn all_keys() -> Vec<(u128, u32)> {
    leaves()
        .iter()
        .flat_map(|(leaf, vs)| (0..vs.len() as u32).map(move |i| (*leaf, i)))
        .collect()
}

/// A deterministic permutation, so reads arrive out of storage order.
fn shuffled(mut keys: Vec<(u128, u32)>, seed: usize) -> Vec<(u128, u32)> {
    let n = keys.len();
    for i in 0..n {
        let j = (i * 7919 + seed * 104729 + 13) % n;
        keys.swap(i, j);
    }
    keys
}

fn write(dir: &std::path::Path, chunk_bytes: usize) -> (VectorStoreLocation, VectorStoreRef) {
    let location = VectorStoreLocation::Local(dir.to_path_buf());
    let reference =
        write_owner_vectors_chunked(&location, 7, DIM, SMALL_BLOCK, chunk_bytes, leaves()).unwrap();
    (location, reference)
}

fn assert_exact(reader: &VectorStoreReader, keys: &[(u128, u32)]) {
    let got = reader.get_many(keys).unwrap();
    assert_eq!(got.len(), keys.len());
    for (&(leaf, position), v) in keys.iter().zip(&got) {
        assert_eq!(&**v, &*vector(leaf, position as usize), "{leaf}/{position}");
    }
}

#[test]
fn round_trip_in_arbitrary_order() {
    // usize::MAX never flushes early; 40 KB flushes and forks every few leaves.
    for chunk_bytes in [usize::MAX, 40 * 1024, 1] {
        let dir = tempfile::tempdir().unwrap();
        let (location, reference) = write(dir.path(), chunk_bytes);
        assert_eq!(reference.kind, LEAF_ORDINAL_LAYOUT);
        assert_eq!(reference.prefix_path, "vectors/owner-7");
        assert_eq!(reference.rows, 41);
        assert_eq!(reference.dimensions, DIM);
        assert_eq!(reference.max_block_size_bytes, SMALL_BLOCK);
        let root = dir
            .path()
            .join("vectors/owner-7/root")
            .join(&reference.blockfile_id);
        assert!(root.exists(), "missing root {}", root.display());
        let blocks = std::fs::read_dir(dir.path().join("vectors/owner-7/block"))
            .unwrap()
            .count();
        assert!(blocks >= 8, "expected many small blocks, got {blocks}");

        let reader = VectorStoreReader::open(&location, &reference, 64 * 1024 * 1024).unwrap();
        assert_eq!(reader.rows(), 41);
        assert_exact(&reader, &shuffled(all_keys(), 1));
        // Repeated keys and a single key both come back in input order.
        assert_exact(&reader, &[(10, 19), (3, 0), (10, 19), (u128::MAX, 8)]);
        assert_exact(&reader, &[(1u128 << 100, 4)]);
        assert!(reader.get_many(&[]).unwrap().is_empty());
    }
}

#[test]
fn with_rows_visits_each_key_once() {
    let dir = tempfile::tempdir().unwrap();
    let (location, reference) = write(dir.path(), 40 * 1024);
    let reader = VectorStoreReader::open(&location, &reference, 64 * 1024 * 1024).unwrap();
    // Small blocks hold several leaves, so some keys miss their row hint and
    // fall back to a search; repeats must each be visited.
    let mut keys = shuffled(all_keys(), 3);
    keys.extend([(10u128, 19u32), (3, 0), (10, 19)]);
    for _ in 0..2 {
        let mut seen = vec![0usize; keys.len()];
        reader
            .with_rows(&keys, |i, row| {
                seen[i] += 1;
                let (leaf, position) = keys[i];
                assert_eq!(row, &*vector(leaf, position as usize), "{leaf}/{position}");
            })
            .unwrap();
        assert!(seen.iter().all(|&n| n == 1), "{seen:?}");
    }
    let mut called = false;
    let err = reader
        .with_rows(&[(3, 1), (10, 20)], |_, _| called = true)
        .unwrap_err();
    assert!(err.contains("missing vector"), "{err}");
    assert!(!called, "visitor ran before the missing key was reported");
}

#[test]
fn missing_keys_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    let (location, reference) = write(dir.path(), usize::MAX);
    let reader = VectorStoreReader::open(&location, &reference, 1 << 20).unwrap();
    for missing in [(10u128, 20u32), (4, 0), (11, 0), (0, 0), (u128::MAX, 9)] {
        let err = reader.get_many(&[(3, 1), missing]).unwrap_err();
        assert!(err.contains("missing vector"), "{missing:?}: {err}");
    }
}

#[test]
fn leaves_must_ascend() {
    let dir = tempfile::tempdir().unwrap();
    let location = VectorStoreLocation::Local(dir.path().to_path_buf());
    let one = |leaf| (leaf, vec![vector(leaf, 0)]);
    for order in [vec![one(5), one(3)], vec![one(5), one(5)]] {
        let err = write_owner_vectors(&location, 1, DIM, SMALL_BLOCK, order).unwrap_err();
        assert!(err.contains("strictly ascending"), "{err}");
    }
    let short: Arc<[f32]> = vec![0.0; DIM - 1].into();
    let err =
        write_owner_vectors(&location, 1, DIM, SMALL_BLOCK, vec![(1, vec![short])]).unwrap_err();
    assert!(err.contains("dimensions"), "{err}");
}

#[test]
fn open_validates_reference() {
    let dir = tempfile::tempdir().unwrap();
    let (location, reference) = write(dir.path(), usize::MAX);
    let mut bad = reference.clone();
    bad.kind = "something-else".into();
    assert!(VectorStoreReader::open(&location, &bad, 1 << 20).is_err());
    let mut bad = reference.clone();
    bad.rows += 1;
    let err = VectorStoreReader::open(&location, &bad, 1 << 20)
        .err()
        .unwrap();
    assert!(err.contains("rows"), "{err}");
    let mut bad = reference.clone();
    bad.owner = 8;
    assert!(VectorStoreReader::open(&location, &bad, 1 << 20).is_err());
    let mut bad = reference;
    bad.blockfile_id = Uuid::new_v4().to_string();
    assert!(VectorStoreReader::open(&location, &bad, 1 << 20).is_err());
}

/// Writes `leaves()` with `writers` parallel writers, reading vectors by ID.
/// ID `leaf_index * 1000 + position` names `vector(leaf, position)`.
fn write_parallel(
    dir: &std::path::Path,
    writers: usize,
    chunk_bytes: usize,
) -> (VectorStoreLocation, VectorStoreRef) {
    let location = VectorStoreLocation::Local(dir.to_path_buf());
    let all = leaves();
    let ids: Vec<Vec<u64>> = all
        .iter()
        .enumerate()
        .map(|(l, (_, vs))| (0..vs.len() as u64).map(|i| l as u64 * 1000 + i).collect())
        .collect();
    let input: Vec<(u128, &[u64])> = all
        .iter()
        .zip(&ids)
        .map(|((leaf, _), ids)| (*leaf, &ids[..]))
        .collect();
    let vectors = |id: u64| -> Result<Arc<[f32]>, String> {
        let (leaf, _) = all[(id / 1000) as usize];
        Ok(vector(leaf, (id % 1000) as usize))
    };
    let reference = write_owner_vectors_parallel_chunked(
        &location,
        7,
        DIM,
        SMALL_BLOCK,
        chunk_bytes,
        writers,
        &input,
        &vectors,
    )
    .unwrap();
    (location, reference)
}

#[test]
fn parallel_writers_round_trip() {
    // Leaves hold 7, 20, 0, 5 and 9 vectors. Ranges cut only before a
    // non-empty leaf once they reach 41 / writers rows, so 2 and 3 writers
    // both cut once (after the 27 rows of the first three leaves) and 64
    // writers give each non-empty leaf its own segment.
    for (writers, segments) in [(1, 0), (2, 2), (3, 2), (64, 4)] {
        for chunk_bytes in [usize::MAX, 40 * 1024] {
            let dir = tempfile::tempdir().unwrap();
            let (location, reference) = write_parallel(dir.path(), writers, chunk_bytes);
            assert_eq!(reference.rows, 41);
            assert_eq!(reference.segments.len(), segments, "{writers} writers");
            let kind = if segments == 0 {
                LEAF_ORDINAL_LAYOUT
            } else {
                SEGMENTED_LEAF_ORDINAL_LAYOUT
            };
            assert_eq!(reference.kind, kind);
            if segments > 0 {
                assert_eq!(reference.segments.iter().map(|s| s.rows).sum::<u64>(), 41);
                assert!(reference
                    .segments
                    .windows(2)
                    .all(|w| w[0].first_leaf < w[1].first_leaf));
            }
            let json = serde_json::to_string(&reference).unwrap();
            assert_eq!(
                serde_json::from_str::<VectorStoreRef>(&json).unwrap(),
                reference
            );
            let reader = VectorStoreReader::open(&location, &reference, 64 * 1024 * 1024).unwrap();
            assert_exact(&reader, &shuffled(all_keys(), writers));
        }
    }
}

#[test]
fn segmented_reader_restricts_to_leaves() {
    let dir = tempfile::tempdir().unwrap();
    let (location, reference) = write_parallel(dir.path(), 64, usize::MAX);
    let mut reader = VectorStoreReader::open(&location, &reference, 64 * 1024 * 1024).unwrap();
    let keep: BTreeSet<u128> = [10u128, u128::MAX].into_iter().collect();
    let (kept, before) = reader.restrict_to_leaves(&keep);
    assert!(kept < before, "{kept} of {before}");
    let keys: Vec<_> = all_keys()
        .into_iter()
        .filter(|(l, _)| keep.contains(l))
        .collect();
    assert_exact(&reader, &keys);
}

#[test]
fn segmented_open_validates_reference() {
    let dir = tempfile::tempdir().unwrap();
    let (location, reference) = write_parallel(dir.path(), 3, usize::MAX);
    let mut bad = reference.clone();
    bad.segments[1].rows += 1;
    let err = VectorStoreReader::open(&location, &bad, 1 << 20)
        .err()
        .unwrap();
    assert!(err.contains("rows"), "{err}");
    let mut bad = reference.clone();
    bad.segments.clear();
    assert!(VectorStoreReader::open(&location, &bad, 1 << 20).is_err());
    let mut bad = reference.clone();
    bad.segments.swap(0, 1);
    assert!(VectorStoreReader::open(&location, &bad, 1 << 20).is_err());
    let mut bad = reference;
    bad.kind = LEAF_ORDINAL_LAYOUT.into();
    assert!(VectorStoreReader::open(&location, &bad, 1 << 20).is_err());
}

#[test]
fn parallel_writers_reject_unordered_leaves() {
    let dir = tempfile::tempdir().unwrap();
    let location = VectorStoreLocation::Local(dir.path().to_path_buf());
    let ids = [1u64];
    let vectors = |_| -> Result<Arc<[f32]>, String> { Ok(vector(1, 0)) };
    let err = write_owner_vectors_parallel(
        &location,
        1,
        DIM,
        SMALL_BLOCK,
        4,
        &[(5, &ids[..]), (3, &ids[..])],
        &vectors,
    )
    .unwrap_err();
    assert!(err.contains("strictly ascending"), "{err}");
}

#[test]
fn empty_owner_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let location = VectorStoreLocation::Local(dir.path().to_path_buf());
    let reference = write_owner_vectors(&location, 2, DIM, SMALL_BLOCK, Vec::new()).unwrap();
    assert_eq!(reference.rows, 0);
    let reader = VectorStoreReader::open(&location, &reference, 1 << 20).unwrap();
    assert!(reader.get_many(&[(1, 0)]).is_err());
}

#[test]
fn second_read_hits_cache() {
    let dir = tempfile::tempdir().unwrap();
    let (location, reference) = write(dir.path(), usize::MAX);
    let reader = VectorStoreReader::open(&location, &reference, 64 * 1024 * 1024).unwrap();
    assert_eq!(reader.counters(), VectorStoreCounters::default());
    let keys = vec![(10u128, 0u32), (10, 1), (10, 19), (3, 2)];
    assert_exact(&reader, &keys);
    let cold = reader.counters();
    assert!(cold.blocks_loaded >= 2, "{cold:?}");
    assert!(cold.blocks_loaded <= keys.len() as u64, "{cold:?}");
    assert_eq!(cold.cache_misses, cold.blocks_loaded, "{cold:?}");
    assert!(
        cold.bytes_loaded >= cold.blocks_loaded * (DIM as u64 * 4),
        "{cold:?}"
    );
    assert!(
        cold.bytes_loaded <= cold.blocks_loaded * SMALL_BLOCK as u64 * 2,
        "{cold:?}"
    );

    assert_exact(&reader, &keys);
    let warm = reader.counters();
    assert_eq!(warm.blocks_loaded, cold.blocks_loaded, "{warm:?}");
    assert_eq!(warm.bytes_loaded, cold.bytes_loaded, "{warm:?}");
    assert_eq!(
        warm.cache_hits - cold.cache_hits,
        cold.blocks_loaded,
        "{warm:?}"
    );

    reader.clear_cache().unwrap();
    assert_exact(&reader, &keys);
    let reloaded = reader.counters();
    assert_eq!(
        reloaded.blocks_loaded,
        2 * cold.blocks_loaded,
        "{reloaded:?}"
    );
}

#[test]
fn concurrent_get_many() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<VectorStoreReader>();

    let dir = tempfile::tempdir().unwrap();
    let (location, reference) = write(dir.path(), 40 * 1024);
    // A cache smaller than the blockfile forces evictions under contention.
    let reader = VectorStoreReader::open(&location, &reference, 4 * SMALL_BLOCK as u64).unwrap();
    std::thread::scope(|scope| {
        for seed in 0..8 {
            let reader = &reader;
            scope.spawn(move || {
                for round in 0..5 {
                    let keys = shuffled(all_keys(), seed * 5 + round);
                    let keys = &keys[..10 + (seed + round) % 30];
                    assert_exact(reader, keys);
                }
            });
        }
    });
    let counters = reader.counters();
    assert!(counters.blocks_loaded > 0, "{counters:?}");
}

#[test]
fn cache_capacity_counts_blocks() {
    assert_eq!(block_cache_capacity(1 << 30, 256 * 1024), 4096);
    assert_eq!(block_cache_capacity(1 << 30, 1 << 20), 1024);
    assert_eq!(block_cache_capacity(1 << 30, 3 << 20), 1024);
    assert_eq!(block_cache_capacity(0, 256 * 1024), 1);
}

/// Production-shaped warm and cold rerank timings. Run with
/// `cargo test --release ... vector_store::tests::rerank_microbench -- --ignored --nocapture`.
/// Env: `BENCH_LEAVES` (default 300), `BENCH_LEAF_VECTORS` (1450).
#[test]
#[ignore]
fn rerank_microbench() {
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use std::time::Instant;
    let env = |name: &str, default: usize| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let n_leaves = env("BENCH_LEAVES", 300);
    let per_leaf = env("BENCH_LEAF_VECTORS", 1450);
    let dir = tempfile::tempdir_in("/private/tmp").unwrap();
    let location = VectorStoreLocation::Local(dir.path().to_path_buf());
    let t = Instant::now();
    let reference = write_owner_vectors(
        &location,
        1,
        DIM,
        DEFAULT_VECTOR_BLOCK_SIZE_BYTES,
        (0..n_leaves as u128).map(|leaf| {
            let leaf = leaf * 7919 + 3;
            let vs = (0..per_leaf).map(|i| vector(leaf, i)).collect();
            (leaf, vs)
        }),
    )
    .unwrap();
    let blocks = std::fs::read_dir(dir.path().join("vectors/owner-1/block"))
        .unwrap()
        .count();
    eprintln!(
        "wrote {} rows, {blocks} block objects in {:.1}s",
        reference.rows,
        t.elapsed().as_secs_f64()
    );
    let reader = VectorStoreReader::open(&location, &reference, 32 << 30).unwrap();
    // 200 candidates in 30 leaves at random ordinals, as a rerank shortlist.
    let mut rng = StdRng::seed_from_u64(7);
    let queries: Vec<Vec<(u128, u32)>> = (0..50)
        .map(|_| {
            let leaves: Vec<u128> = (0..30)
                .map(|_| rng.gen_range(0..n_leaves as u128) * 7919 + 3)
                .collect();
            (0..200)
                .map(|_| {
                    (
                        leaves[rng.gen_range(0..leaves.len())],
                        rng.gen_range(0..per_leaf as u32),
                    )
                })
                .collect()
        })
        .collect();
    // Best of several passes over all queries: the machine may be shared.
    let time = |label: &str, f: &mut dyn FnMut(&[(u128, u32)])| {
        let passes = if label.starts_with("cold") { 1 } else { 7 };
        let mut best = f64::MAX;
        for _ in 0..passes {
            let t = Instant::now();
            for keys in &queries {
                f(keys);
            }
            best = best.min(t.elapsed().as_secs_f64() * 1e6 / queries.len() as f64);
        }
        eprintln!("{label:<40} {best:8.1} us/call");
    };
    let query: Vec<f32> = (0..DIM).map(|d| d as f32 / DIM as f32).collect();
    let metric = chroma_distance::DistanceFunction::Euclidean;
    let check: Vec<Vec<f32>> = queries
        .iter()
        .map(|keys| {
            keys.iter()
                .map(|&(l, p)| {
                    let v = vector(l, p as usize);
                    v.iter().zip(&query).map(|(a, b)| (a - b) * (a - b)).sum()
                })
                .collect()
        })
        .collect();
    let before = reader.counters();
    time("cold get_many (local fs)", &mut |keys| {
        reader.get_many(keys).unwrap();
    });
    let cold = reader.counters();
    eprintln!(
        "cold: {} blocks loaded for {} calls ({:.1}/call)",
        cold.blocks_loaded - before.blocks_loaded,
        queries.len(),
        (cold.cache_misses + cold.cache_hits - before.cache_misses - before.cache_hits) as f64
            / queries.len() as f64
    );
    time("warm get_many", &mut |keys| {
        reader.get_many(keys).unwrap();
    });
    time("warm with_rows + simd distances", &mut |keys| {
        let mut d = 0f32;
        reader
            .with_rows(keys, |_, r| d += metric.distance(&query, r))
            .unwrap();
        std::hint::black_box(d);
    });
    time("warm get_many + simd distances", &mut |keys| {
        let rows = reader.get_many(keys).unwrap();
        let d: f32 = rows.iter().map(|r| metric.distance(&query, r)).sum();
        std::hint::black_box(d);
    });
    time("warm plan + load_blocks only", &mut |keys| {
        let mut plan = reader.plan(keys);
        reader.load_blocks(&mut plan).unwrap();
        std::hint::black_box(plan);
    });
    time("warm with_rows, no-op visitor", &mut |keys| {
        reader
            .with_rows(keys, |_, r| {
                std::hint::black_box(r);
            })
            .unwrap();
    });
    time("200 BlockIndex::locate", &mut |keys| {
        for &k in keys {
            std::hint::black_box(reader.index.locate(k));
        }
    });
    time("200 leaf_prefix", &mut |keys| {
        for &(l, _) in keys {
            std::hint::black_box(leaf_prefix(l));
        }
    });
    {
        let mut plan = reader.plan(&queries[0]);
        reader.load_blocks(&mut plan).unwrap();
        let plan = plan;
        time("fetch distinct blocks (cached, async)", &mut |_| {
            runtime().block_on(async {
                let r = reader.blockfile().await.unwrap();
                for id in &plan.block_ids {
                    std::hint::black_box(r.fetch_block(*id).await.unwrap());
                }
            });
        });
        eprintln!("  ({} distinct blocks)", plan.blocks.len());
        time("200 Block::get on loaded plan", &mut |_| {
            for &(i, p, b, _) in &plan.keys {
                std::hint::black_box(
                    plan.blocks[b]
                        .as_ref()
                        .unwrap()
                        .get::<u32, &[f32]>(&plan.prefixes[p], queries[0][i].1),
                );
            }
        });
    }
    {
        // Per-phase costs across all queries, so CPU caches are as cold as
        // in a real query stream (the single-plan timings above are hot).
        let plans: Vec<RowPlan> = queries
            .iter()
            .map(|keys| {
                let mut plan = reader.plan(keys);
                reader.load_blocks(&mut plan).unwrap();
                plan
            })
            .collect();
        let mut q = 0;
        time("phase: sort + locate + prefixes", &mut |keys| {
            let mut order: Vec<usize> = (0..keys.len()).collect();
            order.sort_unstable_by_key(|&i| keys[i]);
            for &i in &order {
                std::hint::black_box(reader.index.locate(keys[i]));
            }
            for _ in 0..30 {
                std::hint::black_box(leaf_prefix(keys[0].0));
            }
        });
        time("phase: sync cache probes", &mut |_| {
            let plan = &plans[q % plans.len()];
            q += 1;
            let mut fresh = RowPlan {
                keys: Vec::new(),
                prefixes: Vec::new(),
                block_ids: plan.block_ids.clone(),
                blocks: Vec::new(),
            };
            reader.load_blocks(&mut fresh).unwrap();
            std::hint::black_box(fresh);
        });
        time("phase: hinted row lookups", &mut |_| {
            let plan = &plans[q % plans.len()];
            q += 1;
            for &(_, p, b, hint) in &plan.keys {
                std::hint::black_box(
                    plan.blocks[b]
                        .as_ref()
                        .unwrap()
                        .get_with_hint::<u32, &[f32]>(&plan.prefixes[p], 0, hint),
                );
            }
        });
        time("phase: hinted lookups + simd distance", &mut |keys| {
            let plan = &plans[q % plans.len()];
            q += 1;
            let mut d = 0f32;
            for &(i, p, b, hint) in &plan.keys {
                let row = plan.blocks[b]
                    .as_ref()
                    .unwrap()
                    .get_with_hint::<u32, &[f32]>(&plan.prefixes[p], keys[i].1, hint)
                    .unwrap_or(&[]);
                d += metric.distance(&query, &row[..query.len().min(row.len())]);
            }
            std::hint::black_box(d);
        });
        let hinted: usize = plans
            .iter()
            .map(|p| p.keys.iter().filter(|k| k.3 != usize::MAX).count())
            .sum();
        eprintln!(
            "  {:.0}% of keys have a row hint",
            100.0 * hinted as f64 / (plans.len() * 200) as f64
        );
    }
    reader.clear_cache().unwrap();
    time("cold with_rows after clear_cache", &mut |keys| {
        reader
            .with_rows(keys, |_, r| {
                std::hint::black_box(r);
            })
            .unwrap();
    });
    time("block_on(empty)", &mut |_| {
        runtime().block_on(async {});
    });
    time("block_on(open reader)", &mut |_| {
        runtime().block_on(async {
            std::hint::black_box(reader.blockfile().await.unwrap());
        });
    });
    time("open + sequential get", &mut |keys| {
        runtime().block_on(async {
            let r = reader.blockfile().await.unwrap();
            for &(l, p) in keys {
                std::hint::black_box(r.get(&leaf_prefix(l), p).await.unwrap());
            }
        });
    });
    time("open + get same key x200", &mut |keys| {
        runtime().block_on(async {
            let r = reader.blockfile().await.unwrap();
            let (l, p) = keys[0];
            let prefix = leaf_prefix(l);
            for _ in keys {
                std::hint::black_box(r.get(&prefix, p).await.unwrap());
            }
        });
    });
    // Results stay exact.
    for (keys, want) in queries.iter().zip(&check) {
        let rows = reader.get_many(keys).unwrap();
        for (row, w) in rows.iter().zip(want) {
            let d: f32 = row.iter().zip(&query).map(|(a, b)| (a - b) * (a - b)).sum();
            assert_eq!(d.to_bits(), w.to_bits());
        }
    }
}

/// Write throughput of 1 writer against several, on local disk, or in S3
/// when `SPANN_VECTOR_BENCH_BUCKET` and `SPANN_VECTOR_BENCH_PREFIX` are set.
/// `SPANN_VECTOR_BENCH_LEAVES` sets the size (50 vectors per leaf; default
/// 4,000 leaves, 614 MB) and `SPANN_VECTOR_BENCH_WRITERS` the writer counts.
/// `cargo test --release ... parallel_write_throughput -- --ignored --nocapture`
#[test]
#[ignore]
fn parallel_write_throughput() {
    let env = |k: &str| std::env::var(k).ok();
    let leaves_n: u64 = env("SPANN_VECTOR_BENCH_LEAVES").map_or(4_000, |v| v.parse().unwrap());
    let counts: Vec<usize> = env("SPANN_VECTOR_BENCH_WRITERS").map_or(vec![1, 4, 16, 32], |v| {
        v.split(',').map(|n| n.parse().unwrap()).collect()
    });
    let per_leaf = 50u64;
    let ids: Vec<Vec<u64>> = (0..leaves_n)
        .map(|l| (0..per_leaf).map(|i| l * per_leaf + i).collect())
        .collect();
    let input: Vec<(u128, &[u64])> = ids
        .iter()
        .enumerate()
        .map(|(l, ids)| (l as u128, &ids[..]))
        .collect();
    let vectors = |id: u64| -> Result<Arc<[f32]>, String> { Ok(vec![id as f32; DIM].into()) };
    for writers in counts {
        let dir = tempfile::tempdir().unwrap();
        let location = match (
            env("SPANN_VECTOR_BENCH_BUCKET"),
            env("SPANN_VECTOR_BENCH_PREFIX"),
        ) {
            (Some(bucket), Some(prefix)) => VectorStoreLocation::Durable(DurableStoreConfig {
                bucket,
                prefix: format!("{prefix}/writers-{writers}"),
            }),
            _ => VectorStoreLocation::Local(dir.path().to_path_buf()),
        };
        let t = std::time::Instant::now();
        let r = write_owner_vectors_parallel(
            &location,
            1,
            DIM,
            DEFAULT_VECTOR_BLOCK_SIZE_BYTES,
            writers,
            &input,
            &vectors,
        )
        .unwrap();
        let s = t.elapsed().as_secs_f64();
        let mb = (r.rows as usize * DIM * 4) as f64 / 1e6;
        println!(
            "writers {writers}: {} rows, {mb:.0} MB in {s:.2} s = {:.0} MB/s",
            r.rows,
            mb / s
        );
    }
}
