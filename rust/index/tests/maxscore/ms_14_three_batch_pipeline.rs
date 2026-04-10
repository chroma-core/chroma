use crate::common;
use chroma_types::SignedRoaringBitmap;

fn all_mask() -> SignedRoaringBitmap {
    SignedRoaringBitmap::Exclude(Default::default())
}

#[tokio::test]
async fn three_batch_basic_correctness() {
    let vectors = vec![
        (0u32, vec![(1u32, 0.5f32), (2, 0.3)]),
        (1, vec![(1, 0.1), (2, 0.9)]),
        (2, vec![(1, 0.8), (3, 0.4)]),
        (3, vec![(2, 0.7), (3, 0.2)]),
    ];

    let (_tmp, _prov, reader) = common::build_index(vectors.clone()).await;
    let query = vec![(1u32, 1.0f32), (2, 0.5)];

    let results = reader.query(query.clone(), 2, all_mask()).await.unwrap();
    let brute = common::brute_force_topk(&vectors, &query, 2, &all_mask());

    assert_eq!(results.len(), brute.len());
    for (r, b) in results.iter().zip(brute.iter()) {
        assert_eq!(r.offset, b.0, "offset mismatch");
        common::assert_approx(r.score, b.1, 0.02);
    }
}

#[tokio::test]
async fn three_batch_single_dimension() {
    let vectors = vec![
        (0u32, vec![(1u32, 0.9f32)]),
        (1, vec![(1, 0.1)]),
        (2, vec![(1, 0.5)]),
    ];

    let (_tmp, _prov, reader) = common::build_index(vectors.clone()).await;
    let query = vec![(1u32, 1.0f32)];

    let results = reader.query(query.clone(), 3, all_mask()).await.unwrap();
    let brute = common::brute_force_topk(&vectors, &query, 3, &all_mask());

    assert_eq!(results.len(), brute.len());
    for (r, b) in results.iter().zip(brute.iter()) {
        assert_eq!(r.offset, b.0);
        common::assert_approx(r.score, b.1, 0.02);
    }
}

#[tokio::test]
async fn three_batch_k_larger_than_results() {
    let vectors = vec![(0u32, vec![(1u32, 0.5f32)]), (1, vec![(1, 0.3)])];

    let (_tmp, _prov, reader) = common::build_index(vectors.clone()).await;
    let query = vec![(1u32, 1.0f32)];

    let results = reader.query(query.clone(), 100, all_mask()).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].offset, 0);
    assert_eq!(results[1].offset, 1);
}

#[tokio::test]
async fn three_batch_empty_query() {
    let vectors = vec![(0u32, vec![(1u32, 0.5f32)])];
    let (_tmp, _prov, reader) = common::build_index(vectors).await;

    let results = reader
        .query(Vec::<(u32, f32)>::new(), 10, all_mask())
        .await
        .unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn three_batch_k_zero() {
    let vectors = vec![(0u32, vec![(1u32, 0.5f32)])];
    let (_tmp, _prov, reader) = common::build_index(vectors).await;

    let results = reader
        .query(vec![(1u32, 1.0f32)], 0, all_mask())
        .await
        .unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn three_batch_no_matching_dimension() {
    let vectors = vec![(0u32, vec![(1u32, 0.5f32)]), (1, vec![(2, 0.3)])];

    let (_tmp, _prov, reader) = common::build_index(vectors).await;
    let query = vec![(999u32, 1.0f32)];

    let results = reader.query(query, 10, all_mask()).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn three_batch_with_mask() {
    let vectors = vec![
        (0u32, vec![(1u32, 0.9f32)]),
        (1, vec![(1, 0.1)]),
        (2, vec![(1, 0.5)]),
    ];

    let (_tmp, _prov, reader) = common::build_index(vectors.clone()).await;
    let query = vec![(1u32, 1.0f32)];

    let mut rbm = roaring::RoaringBitmap::new();
    rbm.insert(0);
    rbm.insert(2);
    let mask = SignedRoaringBitmap::Include(rbm);

    let results = reader.query(query.clone(), 10, mask.clone()).await.unwrap();
    let brute = common::brute_force_topk(&vectors, &query, 10, &mask);

    assert_eq!(results.len(), brute.len());
    for (r, b) in results.iter().zip(brute.iter()) {
        assert_eq!(r.offset, b.0);
        common::assert_approx(r.score, b.1, 0.02);
    }
}

#[tokio::test]
async fn three_batch_multi_block_per_dim() {
    let mut vectors = Vec::new();
    for i in 0..200u32 {
        vectors.push((
            i,
            vec![(1u32, 0.01 * (i as f32 + 1.0)), (2, 0.005 * i as f32)],
        ));
    }

    let (_tmp, _prov, reader) =
        common::build_index_with_block_size(vectors.clone(), Some(32)).await;
    let query = vec![(1u32, 1.0f32), (2, 0.5)];

    let results = reader.query(query.clone(), 5, all_mask()).await.unwrap();
    let brute = common::brute_force_topk(&vectors, &query, 5, &all_mask());

    assert_eq!(results.len(), brute.len());
    for (r, b) in results.iter().zip(brute.iter()) {
        assert_eq!(r.offset, b.0, "offset mismatch at rank");
        common::assert_approx(r.score, b.1, 0.05);
    }
}
