mod common;

use chroma_index::sparse::maxscore::SparsePostingBlock;
use common::sequential_entries;

#[test]
fn test_ms_11_vectorized_matches_scalar() {
    let entries = sequential_entries(0, 1, 256, 0.7);
    let block = SparsePostingBlock::from_sorted_entries(&entries);
    let query_weight = 1.5f32;

    let mut scalar_scores = vec![0.0f32; 256];
    for (i, &v) in block.values().iter().enumerate() {
        scalar_scores[i] += v * query_weight;
    }

    let mut vec_scores = vec![0.0f32; 256];
    block.score_block_into(query_weight, &mut vec_scores);

    for i in 0..256 {
        assert!(
            (scalar_scores[i] - vec_scores[i]).abs() < 1e-5,
            "entry {i}: scalar={}, vec={}",
            scalar_scores[i],
            vec_scores[i]
        );
    }
}

#[test]
fn test_ms_11_full_query_vectorized() {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    let mut rng = StdRng::seed_from_u64(555);

    let blocks: Vec<SparsePostingBlock> = (0..5)
        .map(|b| {
            let entries: Vec<(u32, f32)> = (0..256)
                .map(|i| (b * 256 + i, rng.gen_range(0.01..1.0)))
                .collect();
            SparsePostingBlock::from_sorted_entries(&entries)
        })
        .collect();

    let query_weight = 0.8;

    for block in &blocks {
        let mut scalar_scores = vec![0.0f32; block.offsets().len()];
        for (i, &v) in block.values().iter().enumerate() {
            scalar_scores[i] += v * query_weight;
        }

        let mut vec_scores = vec![0.0f32; block.offsets().len()];
        block.score_block_into(query_weight, &mut vec_scores);

        for i in 0..block.offsets().len() {
            assert!(
                (scalar_scores[i] - vec_scores[i]).abs() < 1e-5,
                "block min_offset={}, entry {i}: scalar={}, vec={}",
                block.min_offset,
                scalar_scores[i],
                vec_scores[i]
            );
        }
    }
}

#[test]
fn test_ms_11_partial_block() {
    let entries = sequential_entries(0, 1, 100, 0.5);
    let block = SparsePostingBlock::from_sorted_entries(&entries);
    let query_weight = 2.0;

    let mut vec_scores = vec![0.0f32; 100];
    block.score_block_into(query_weight, &mut vec_scores);

    for (i, &v) in block.values().iter().enumerate() {
        let expected = v * query_weight;
        assert!(
            (vec_scores[i] - expected).abs() < 1e-5,
            "entry {i}: expected {expected}, got {}",
            vec_scores[i]
        );
    }
}

#[test]
fn test_ms_11_zero_weights() {
    let entries: Vec<(u32, f32)> = (0..256).map(|i| (i, 0.0001)).collect();
    let block = SparsePostingBlock::from_sorted_entries(&entries);

    let mut vec_scores = vec![0.0f32; 256];
    block.score_block_into(1.0, &mut vec_scores);

    for w in vec_scores.windows(2) {
        assert!((w[0] - w[1]).abs() < 1e-5);
    }
}

#[test]
fn test_ms_11_max_weights() {
    let entries: Vec<(u32, f32)> = (0..256).map(|i| (i, 1.0)).collect();
    let block = SparsePostingBlock::from_sorted_entries(&entries);

    let mut vec_scores = vec![0.0f32; 256];
    block.score_block_into(1.0, &mut vec_scores);

    for (i, &s) in vec_scores.iter().enumerate() {
        assert!(
            (s - 1.0).abs() < 1e-3,
            "entry {i}: expected ~1.0, got {s}"
        );
    }
}

#[test]
fn test_ms_11_accumulation() {
    let entries = sequential_entries(0, 1, 256, 0.5);
    let block = SparsePostingBlock::from_sorted_entries(&entries);

    let mut scores = vec![0.0f32; 256];
    block.score_block_into(1.0, &mut scores);
    block.score_block_into(2.0, &mut scores);

    let mut single_scores = vec![0.0f32; 256];
    block.score_block_into(3.0, &mut single_scores);

    for i in 0..256 {
        assert!(
            (scores[i] - single_scores[i]).abs() < 1e-4,
            "entry {i}: accumulated={}, 3x={}",
            scores[i],
            single_scores[i]
        );
    }
}
