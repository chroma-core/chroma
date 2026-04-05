use crate::common;
use chroma_types::SignedRoaringBitmap;

fn all_mask() -> SignedRoaringBitmap {
    SignedRoaringBitmap::Exclude(Default::default())
}

#[tokio::test]
async fn query_after_incremental_update() {
    let docs = vec![
        (0u32, vec![(1u32, 0.5)]),
        (1, vec![(1, 0.3)]),
    ];
    let (_dir, provider, reader) = common::build_index(docs).await;

    let writer = common::fork_writer(&provider, &reader).await;
    writer.set(2, vec![(1u32, 0.99)]).await;
    let reader2 = common::commit_writer(&provider, writer).await;

    let results = reader2.query(vec![(1u32, 1.0)], 1, all_mask()).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].offset, 2);
    common::assert_approx(results[0].score, 0.99, 2e-3);
}
