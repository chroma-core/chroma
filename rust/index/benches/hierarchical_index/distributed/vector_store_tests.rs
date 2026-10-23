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
