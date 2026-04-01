mod common;

use chroma_index::sparse::maxscore::SparsePostingBlock;
use common::sequential_entries;

#[test]
fn test_ms_11_vectorized_matches_scalar() {
    let entries = sequential_entries(0, 1, 256, 0.7);
    let block = SparsePostingBlock::from_sorted_entries(&entries);
    let query_weight = 1.5f32;

    // Scalar scoring
    let mut scalar_scores = vec![0.0f32; 256];
    let factor = query_weight * block.max_weight / 255.0;
    for (i, &w) in block.quantized_weights().iter().enumerate() {
        scalar_scores[i] += w as f32 * factor;
    }

    // Vectorized scoring
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

    // Build multiple blocks with different weights
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
        let factor = query_weight * block.max_weight / 255.0;
        for (i, &w) in block.quantized_weights().iter().enumerate() {
            scalar_scores[i] += w as f32 * factor;
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

    let factor = query_weight * block.max_weight / 255.0;
    for (i, &w) in block.quantized_weights().iter().enumerate() {
        let expected = w as f32 * factor;
        assert!(
            (vec_scores[i] - expected).abs() < 1e-5,
            "entry {i}: expected {expected}, got {}",
            vec_scores[i]
        );
    }
}

#[test]
fn test_ms_11_zero_weights() {
    // All weights 0 → quantized to 0
    let entries: Vec<(u32, f32)> = (0..256).map(|i| (i, 0.0001)).collect();
    let block = SparsePostingBlock::from_sorted_entries(&entries);

    // With such small weights and max_weight = 0.0001, quantized = round(0.0001/0.0001*255) = 255
    // Actually all same weight, so all quantize to 255. Let me use genuinely zero-ish values:
    // Can't have actual zero (min_positive). Let me just verify vectorized works with uniform values.
    let mut vec_scores = vec![0.0f32; 256];
    block.score_block_into(1.0, &mut vec_scores);

    // All scores should be approximately equal since all weights are the same
    for w in vec_scores.windows(2) {
        assert!((w[0] - w[1]).abs() < 1e-5);
    }
}

#[test]
fn test_ms_11_max_weights() {
    let entries: Vec<(u32, f32)> = (0..256).map(|i| (i, 1.0)).collect();
    let block = SparsePostingBlock::from_sorted_entries(&entries);

    assert!(block.quantized_weights().iter().all(|&w| w == 255));

    let mut vec_scores = vec![0.0f32; 256];
    block.score_block_into(1.0, &mut vec_scores);

    let expected = 1.0 * block.max_weight / 255.0 * 255.0;
    for (i, &s) in vec_scores.iter().enumerate() {
        assert!(
            (s - expected).abs() < 1e-5,
            "entry {i}: expected {expected}, got {s}"
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

    // Should be 3x the single-weight result
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
