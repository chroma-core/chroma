mod common;

use common::{build_index, count_blocks, get_all_entries};

const BLOCK_SIZE: usize = 1024;

#[tokio::test]
async fn test_ms_02_single_document() {
    let vectors = vec![(0u32, vec![(1u32, 0.5f32), (2, 0.8)])];
    let (_dir, _provider, reader) = build_index(vectors).await;

    let entries_1 = get_all_entries(&reader, 1).await;
    assert_eq!(entries_1.len(), 1);
    assert_eq!(entries_1[0].0, 0);

    let entries_2 = get_all_entries(&reader, 2).await;
    assert_eq!(entries_2.len(), 1);
    assert_eq!(entries_2[0].0, 0);
}

#[tokio::test]
async fn test_ms_02_1000_documents() {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    let mut rng = StdRng::seed_from_u64(42);
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..1000)
        .map(|i| {
            let dims: Vec<(u32, f32)> = (0..50)
                .map(|_| (rng.gen_range(0..200), rng.gen_range(0.01..1.0)))
                .collect();
            (i, dims)
        })
        .collect();

    let (_dir, _provider, reader) = build_index(vectors).await;

    // Check several dimensions
    for dim in [0u32, 50, 100, 150, 199] {
        let entries = get_all_entries(&reader, dim).await;
        // Entries should be sorted by offset
        for w in entries.windows(2) {
            assert!(w[0].0 < w[1].0, "offsets must be sorted");
        }
        // Blocks should be properly sized
        let blocks = count_blocks(&reader, dim).await;
        if !entries.is_empty() {
            let expected_blocks = entries.len().div_ceil(BLOCK_SIZE);
            assert_eq!(blocks, expected_blocks);
        }
    }
}

#[tokio::test]
async fn test_ms_02_block_boundary_exact() {
    let bs = BLOCK_SIZE as u32;

    // Exactly BLOCK_SIZE docs all sharing dim 0 → exactly 1 block
    let vectors: Vec<(u32, Vec<(u32, f32)>)> =
        (0..bs).map(|i| (i, vec![(0u32, 0.5)])).collect();
    let (_dir, _provider, reader) = build_index(vectors).await;
    assert_eq!(count_blocks(&reader, 0).await, 1);

    // BLOCK_SIZE + 1 docs → 2 blocks
    let vectors: Vec<(u32, Vec<(u32, f32)>)> =
        (0..bs + 1).map(|i| (i, vec![(0u32, 0.5)])).collect();
    let (_dir2, _provider2, reader2) = build_index(vectors).await;
    assert_eq!(count_blocks(&reader2, 0).await, 2);
}

#[tokio::test]
async fn test_ms_02_large_dimension() {
    let n = 10_000usize;
    let vectors: Vec<(u32, Vec<(u32, f32)>)> =
        (0..n as u32).map(|i| (i, vec![(0u32, 0.3)])).collect();
    let (_dir, _provider, reader) = build_index(vectors).await;

    let blocks = count_blocks(&reader, 0).await;
    assert_eq!(blocks, n.div_ceil(BLOCK_SIZE));

    let entries = get_all_entries(&reader, 0).await;
    assert_eq!(entries.len(), n);

    let last_block = reader
        .get_posting_blocks(&chroma_index::sparse::types::encode_u32(0))
        .await
        .unwrap();
    let expected_last = if n % BLOCK_SIZE == 0 { BLOCK_SIZE } else { n % BLOCK_SIZE };
    assert_eq!(last_block.last().unwrap().offsets().len(), expected_last);
}
