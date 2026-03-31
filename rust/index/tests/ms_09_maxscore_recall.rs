mod common;

use chroma_types::SignedRoaringBitmap;
use common::{brute_force_topk, build_index};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

fn recall(results: &[chroma_index::sparse::maxscore::Score], expected: &[(u32, f32)]) -> f32 {
    let result_offsets: std::collections::HashSet<u32> =
        results.iter().map(|s| s.offset).collect();
    let expected_offsets: std::collections::HashSet<u32> =
        expected.iter().map(|(o, _)| *o).collect();
    let overlap = result_offsets.intersection(&expected_offsets).count();
    if expected.is_empty() {
        return 1.0;
    }
    overlap as f32 / expected.len() as f32
}

#[tokio::test]
async fn test_ms_09_recall_1k_docs() {
    let mut rng = StdRng::seed_from_u64(300);
    let all_dims: Vec<u32> = (0..100).collect();
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..1000)
        .map(|i| {
            let ndims = rng.gen_range(10..50);
            let mut chosen: Vec<u32> = all_dims.choose_multiple(&mut rng, ndims).copied().collect();
            chosen.sort();
            let dims: Vec<(u32, f32)> = chosen
                .into_iter()
                .map(|d| (d, rng.gen_range(0.01..1.0)))
                .collect();
            (i, dims)
        })
        .collect();

    let (_dir, _p, reader) = build_index(vectors.clone()).await;
    let mask = SignedRoaringBitmap::full();

    let mut total_recall = 0.0;
    let num_queries = 100;
    for _ in 0..num_queries {
        let ndims = rng.gen_range(5..20);
        let mut chosen: Vec<u32> = all_dims.choose_multiple(&mut rng, ndims).copied().collect();
        chosen.sort();
        let query: Vec<(u32, f32)> = chosen
            .into_iter()
            .map(|d| (d, rng.gen_range(0.1..1.0)))
            .collect();

        let results = reader.query(query.clone(), 10, mask.clone()).await.unwrap();
        let expected = brute_force_topk(&vectors, &query, 10, &mask);
        total_recall += recall(&results, &expected);
    }

    let avg_recall = total_recall / num_queries as f32;
    assert!(
        avg_recall >= 0.95,
        "average recall {avg_recall:.3} < 0.95 threshold"
    );
}

#[tokio::test]
async fn test_ms_09_recall_varying_k() {
    let mut rng = StdRng::seed_from_u64(400);
    let all_dims2: Vec<u32> = (0..80).collect();
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..1000)
        .map(|i| {
            let ndims = rng.gen_range(10..30);
            let mut chosen: Vec<u32> = all_dims2.choose_multiple(&mut rng, ndims).copied().collect();
            chosen.sort();
            let dims: Vec<(u32, f32)> = chosen
                .into_iter()
                .map(|d| (d, rng.gen_range(0.01..1.0)))
                .collect();
            (i, dims)
        })
        .collect();

    let (_dir, _p, reader) = build_index(vectors.clone()).await;
    let mask = SignedRoaringBitmap::full();

    for k in [1u32, 5, 10, 50] {
        let mut total_recall = 0.0;
        let num_queries = 50;
        for _ in 0..num_queries {
            let ndims = rng.gen_range(3..15);
            let mut chosen: Vec<u32> = all_dims2.choose_multiple(&mut rng, ndims).copied().collect();
            chosen.sort();
            let query: Vec<(u32, f32)> = chosen
                .into_iter()
                .map(|d| (d, rng.gen_range(0.1..1.0)))
                .collect();

            let results = reader.query(query.clone(), k, mask.clone()).await.unwrap();
            let expected = brute_force_topk(&vectors, &query, k as usize, &mask);
            total_recall += recall(&results, &expected);
        }

        let avg_recall = total_recall / num_queries as f32;
        assert!(
            avg_recall >= 0.90,
            "k={k}: average recall {avg_recall:.3} < 0.90 threshold"
        );
    }
}
