use crate::common;
use chroma_types::SignedRoaringBitmap;

#[tokio::test]
async fn maxscore_include_mask() {
    let docs = vec![
        (0u32, vec![(1u32, 0.5)]),
        (1, vec![(1, 0.8)]),
        (2, vec![(1, 0.3)]),
        (3, vec![(1, 0.9)]),
    ];

    let (_dir, _provider, reader) = common::build_index(docs).await;

    let mut rbm = roaring::RoaringBitmap::new();
    rbm.insert(1);
    rbm.insert(3);
    let mask = SignedRoaringBitmap::Include(rbm);

    let results = reader.query(vec![(1u32, 1.0)], 10, mask).await.unwrap();
    let offsets: Vec<u32> = results.iter().map(|r| r.offset).collect();
    assert!(offsets.contains(&1));
    assert!(offsets.contains(&3));
    assert!(!offsets.contains(&0));
    assert!(!offsets.contains(&2));
}

#[tokio::test]
async fn maxscore_exclude_mask() {
    let docs = vec![
        (0u32, vec![(1u32, 0.5)]),
        (1, vec![(1, 0.8)]),
        (2, vec![(1, 0.3)]),
        (3, vec![(1, 0.9)]),
    ];

    let (_dir, _provider, reader) = common::build_index(docs).await;

    let mut rbm = roaring::RoaringBitmap::new();
    rbm.insert(1);
    rbm.insert(3);
    let mask = SignedRoaringBitmap::Exclude(rbm);

    let results = reader.query(vec![(1u32, 1.0)], 10, mask).await.unwrap();
    let offsets: Vec<u32> = results.iter().map(|r| r.offset).collect();
    assert!(offsets.contains(&0));
    assert!(offsets.contains(&2));
    assert!(!offsets.contains(&1));
    assert!(!offsets.contains(&3));
}
