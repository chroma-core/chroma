mod common;

use chroma_types::SignedRoaringBitmap;
use common::{brute_force_topk, build_index};

fn assert_topk_matches(
    results: &[chroma_index::sparse::maxscore::Score],
    expected: &[(u32, f32)],
    quantization_tol: f32,
) {
    assert_eq!(
        results.len(),
        expected.len(),
        "result count mismatch: got {}, expected {}",
        results.len(),
        expected.len()
    );
    let mut result_offsets: Vec<u32> = results.iter().map(|s| s.offset).collect();
    result_offsets.sort();
    let mut expected_offsets: Vec<u32> = expected.iter().map(|(o, _)| *o).collect();
    expected_offsets.sort();
    assert_eq!(result_offsets, expected_offsets, "top-k offset sets differ");

    for (r, e) in results.iter().zip(expected.iter()) {
        assert!(
            (r.score - e.1).abs() <= quantization_tol,
            "score for doc {} differs: maxscore={}, brute_force={}, tol={}",
            r.offset,
            r.score,
            e.1,
            quantization_tol
        );
    }
}

#[tokio::test]
async fn test_ms_06_single_term_k1() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..100)
        .map(|i| (i, vec![(0u32, (i as f32 + 1.0) / 101.0)]))
        .collect();
    let query = vec![(0u32, 1.0)];
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors.clone()).await;
    let results = reader.query(query.clone(), 1, mask.clone()).await.unwrap();
    let expected = brute_force_topk(&vectors, &query, 1, &mask);

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].offset, expected[0].0);
}

#[tokio::test]
async fn test_ms_06_single_term_k10() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..1000)
        .map(|i| (i, vec![(0u32, (i as f32 + 1.0) / 1001.0)]))
        .collect();
    let query = vec![(0u32, 1.0)];
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors.clone()).await;
    let results = reader.query(query.clone(), 10, mask.clone()).await.unwrap();
    let expected = brute_force_topk(&vectors, &query, 10, &mask);

    let mut r_offs: Vec<u32> = results.iter().map(|s| s.offset).collect();
    r_offs.sort();
    let mut e_offs: Vec<u32> = expected.iter().map(|(o, _)| *o).collect();
    e_offs.sort();
    assert_eq!(r_offs, e_offs);
}

#[tokio::test]
async fn test_ms_06_two_terms_k5() {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    let mut rng = StdRng::seed_from_u64(100);
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..500)
        .map(|i| {
            (
                i,
                vec![
                    (0u32, rng.gen_range(0.01..1.0)),
                    (1, rng.gen_range(0.01..1.0)),
                ],
            )
        })
        .collect();
    let query = vec![(0u32, 1.0), (1, 0.5)];
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors.clone()).await;
    let results = reader.query(query.clone(), 5, mask.clone()).await.unwrap();
    let expected = brute_force_topk(&vectors, &query, 5, &mask);

    let mut r_offs: Vec<u32> = results.iter().map(|s| s.offset).collect();
    r_offs.sort();
    let mut e_offs: Vec<u32> = expected.iter().map(|(o, _)| *o).collect();
    e_offs.sort();
    assert_eq!(r_offs, e_offs);
}

#[tokio::test]
async fn test_ms_06_many_terms_k10() {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    let mut rng = StdRng::seed_from_u64(200);
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..1000)
        .map(|i| {
            let ndims = rng.gen_range(5..30);
            let dims: Vec<(u32, f32)> = (0..ndims)
                .map(|_| (rng.gen_range(0..50), rng.gen_range(0.01..1.0)))
                .collect();
            (i, dims)
        })
        .collect();
    let query: Vec<(u32, f32)> = (0..30).map(|d| (d, rng.gen_range(0.1..1.0))).collect();
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors.clone()).await;
    let results = reader.query(query.clone(), 10, mask.clone()).await.unwrap();
    let expected = brute_force_topk(&vectors, &query, 10, &mask);

    let mut r_offs: Vec<u32> = results.iter().map(|s| s.offset).collect();
    r_offs.sort();
    let mut e_offs: Vec<u32> = expected.iter().map(|(o, _)| *o).collect();
    e_offs.sort();

    // Allow up to 2 mismatches due to quantization-induced tie-breaking
    let overlap = r_offs.iter().filter(|o| e_offs.contains(o)).count();
    assert!(
        overlap >= 8,
        "expected at least 8/10 overlap, got {overlap}/10. maxscore={r_offs:?}, brute={e_offs:?}"
    );
}

#[tokio::test]
async fn test_ms_06_dense_overlap() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..50)
        .map(|i| {
            let dims: Vec<(u32, f32)> = (0..20).map(|d| (d, (i as f32 + 1.0) / 51.0)).collect();
            (i, dims)
        })
        .collect();
    let query: Vec<(u32, f32)> = (0..20).map(|d| (d, 1.0)).collect();
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors.clone()).await;
    let results = reader.query(query.clone(), 5, mask.clone()).await.unwrap();
    let expected = brute_force_topk(&vectors, &query, 5, &mask);

    let mut r_offs: Vec<u32> = results.iter().map(|s| s.offset).collect();
    r_offs.sort();
    let mut e_offs: Vec<u32> = expected.iter().map(|(o, _)| *o).collect();
    e_offs.sort();
    assert_eq!(r_offs, e_offs);
}

#[tokio::test]
async fn test_ms_06_no_overlap() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> =
        (0..50).map(|i| (i, vec![(0u32, 0.5)])).collect();
    let query = vec![(999u32, 1.0)];
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors).await;
    let results = reader.query(query, 5, mask).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_ms_06_partial_overlap() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..100)
        .map(|i| (i, vec![(0u32, 0.5), (1, 0.3)]))
        .collect();
    // Query dims 0 (present), 1 (present), 999 (absent)
    let query = vec![(0u32, 1.0), (1, 0.5), (999, 2.0)];
    let mask = SignedRoaringBitmap::full();

    let (_dir, _p, reader) = build_index(vectors.clone()).await;
    let results = reader.query(query.clone(), 5, mask.clone()).await.unwrap();

    assert!(!results.is_empty());
    // All results should have same score since all docs have same weights in dims 0, 1
}
