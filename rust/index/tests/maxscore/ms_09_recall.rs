use crate::common;
use chroma_types::SignedRoaringBitmap;

fn all_mask() -> SignedRoaringBitmap {
    SignedRoaringBitmap::Exclude(Default::default())
}

#[tokio::test]
async fn recall_500_docs_10_dims() {
    let docs: Vec<(u32, Vec<(u32, f32)>)> = (0..500)
        .map(|i| {
            let dims: Vec<(u32, f32)> = (0..10)
                .filter(|d| (i + d) % 3 != 0)
                .map(|d| (d, 0.01 * ((i * 13 + d * 7) % 100) as f32))
                .collect();
            (i, dims)
        })
        .collect();

    let query: Vec<(u32, f32)> = (0..10).map(|d| (d, 1.0)).collect();
    let mask = all_mask();
    let k = 10;

    let (_dir, _provider, reader) = common::build_index(docs.clone()).await;
    let results = reader.query(query.clone(), k, mask.clone()).await.unwrap();
    let brute = common::brute_force_topk(&docs, &query, k as usize, &mask);

    let result_offsets: std::collections::HashSet<u32> =
        results.iter().map(|r| r.offset).collect();
    let brute_offsets: std::collections::HashSet<u32> =
        brute.iter().map(|(o, _)| *o).collect();

    let overlap = result_offsets.intersection(&brute_offsets).count();
    let recall = overlap as f64 / k as f64;

    assert!(
        recall >= 0.9,
        "recall {recall} too low (expected >= 0.9), maxscore={result_offsets:?}, brute={brute_offsets:?}"
    );
}

#[tokio::test]
async fn recall_varied_query_weights() {
    let docs: Vec<(u32, Vec<(u32, f32)>)> = (0..300)
        .map(|i| {
            let dims: Vec<(u32, f32)> = (0..5)
                .map(|d| (d, 0.01 * ((i * 11 + d * 3) % 100) as f32))
                .collect();
            (i, dims)
        })
        .collect();

    let query = vec![(0u32, 2.0), (1, 0.5), (2, 1.5), (3, 0.1), (4, 3.0)];
    let mask = all_mask();
    let k = 5;

    let (_dir, _provider, reader) = common::build_index(docs.clone()).await;
    let results = reader.query(query.clone(), k, mask.clone()).await.unwrap();
    let brute = common::brute_force_topk(&docs, &query, k as usize, &mask);

    let result_offsets: std::collections::HashSet<u32> =
        results.iter().map(|r| r.offset).collect();
    let brute_offsets: std::collections::HashSet<u32> =
        brute.iter().map(|(o, _)| *o).collect();

    let overlap = result_offsets.intersection(&brute_offsets).count();
    let recall = overlap as f64 / k as f64;

    assert!(
        recall >= 0.8,
        "recall {recall} too low with varied weights"
    );
}
