use crate::common;
use chroma_types::SignedRoaringBitmap;

fn all_mask() -> SignedRoaringBitmap {
    SignedRoaringBitmap::Exclude(Default::default())
}

#[tokio::test]
async fn query_no_matching_dims() {
    let docs = vec![(0u32, vec![(1u32, 0.5)])];
    let (_dir, _provider, reader) = common::build_index(docs).await;
    let results = reader
        .query(vec![(999u32, 1.0)], 10, all_mask())
        .await
        .unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn all_docs_masked_out() {
    let docs = vec![(0u32, vec![(1u32, 0.5)]), (1, vec![(1, 0.3)])];
    let (_dir, _provider, reader) = common::build_index(docs).await;

    let rbm = roaring::RoaringBitmap::new();
    let mask = SignedRoaringBitmap::Include(rbm);

    let results = reader.query(vec![(1u32, 1.0)], 10, mask).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn single_doc_single_dim() {
    let docs = vec![(42u32, vec![(7u32, 0.99)])];
    let (_dir, _provider, reader) = common::build_index(docs).await;

    let results = reader
        .query(vec![(7u32, 1.0)], 1, all_mask())
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].offset, 42);
    common::assert_approx(results[0].score, 0.99, 2e-3);
}

#[tokio::test]
async fn results_sorted_by_score_desc() {
    let docs = vec![
        (0u32, vec![(1u32, 0.1)]),
        (1, vec![(1, 0.5)]),
        (2, vec![(1, 0.3)]),
        (3, vec![(1, 0.9)]),
    ];

    let (_dir, _provider, reader) = common::build_index(docs).await;
    let results = reader
        .query(vec![(1u32, 1.0)], 4, all_mask())
        .await
        .unwrap();

    for w in results.windows(2) {
        assert!(
            w[0].score >= w[1].score,
            "not sorted: {} >= {}",
            w[0].score,
            w[1].score
        );
    }
}
