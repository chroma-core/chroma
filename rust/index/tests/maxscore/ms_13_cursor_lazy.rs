use crate::common;
use chroma_index::sparse::maxscore::PostingCursor;
use chroma_index::sparse::types::encode_u32;
use chroma_types::SignedRoaringBitmap;

fn all_mask() -> SignedRoaringBitmap {
    SignedRoaringBitmap::Exclude(Default::default())
}

#[tokio::test]
async fn lazy_cursor_advance_matches_eager() {
    let vectors = vec![
        (0u32, vec![(1u32, 0.5f32)]),
        (5, vec![(1, 0.3)]),
        (10, vec![(1, 0.9)]),
        (20, vec![(1, 0.1)]),
    ];

    let (_tmp, _prov, reader) = common::build_index(vectors).await;
    let encoded_dim = encode_u32(1);

    let (dir, _) = reader
        .get_directory(&encoded_dim)
        .await
        .unwrap()
        .expect("directory should exist");
    let dir_max_offsets = dir.max_offsets().to_vec();
    let dir_max_weights = dir.max_weights().to_vec();

    // Load data blocks into cache
    reader
        .posting_reader()
        .load_blocks_for_prefixes([encoded_dim.as_str()])
        .await;

    let mut lazy_cursor =
        PostingCursor::open_lazy(dir_max_offsets.clone(), dir_max_weights.clone());
    let all_indices: Vec<usize> = (0..dir_max_offsets.len()).collect();
    lazy_cursor.populate_from_cache(reader.posting_reader(), &encoded_dim, &all_indices);

    let blocks = reader.get_posting_blocks(&encoded_dim).await.unwrap();
    let mut eager_cursor = PostingCursor::from_blocks(blocks);

    let mask = all_mask();
    for target in [0, 5, 7, 10, 15, 20, 25] {
        let e = eager_cursor.advance(target, &mask);
        let l = lazy_cursor.advance(target, &mask);
        match (e, l) {
            (Some((eo, ev)), Some((lo, lv))) => {
                assert_eq!(eo, lo, "advance({target}) offset mismatch");
                common::assert_approx(lv, ev, 1e-3);
            }
            (None, None) => {}
            _ => panic!("advance({target}) mismatch: eager={e:?} lazy={l:?}"),
        }
        if e.is_some() {
            eager_cursor.next();
            lazy_cursor.next();
        }
    }
}

#[tokio::test]
async fn lazy_cursor_get_value() {
    let vectors = vec![
        (0u32, vec![(1u32, 0.1f32)]),
        (5, vec![(1, 0.2)]),
        (10, vec![(1, 0.3)]),
    ];

    let (_tmp, _prov, reader) = common::build_index(vectors).await;
    let encoded_dim = encode_u32(1);

    let (dir, _) = reader
        .get_directory(&encoded_dim)
        .await
        .unwrap()
        .expect("directory should exist");
    let offsets = dir.max_offsets().to_vec();
    let weights = dir.max_weights().to_vec();

    reader
        .posting_reader()
        .load_blocks_for_prefixes([encoded_dim.as_str()])
        .await;

    let mut cursor = PostingCursor::open_lazy(offsets.clone(), weights);
    let all_indices: Vec<usize> = (0..offsets.len()).collect();
    cursor.populate_from_cache(reader.posting_reader(), &encoded_dim, &all_indices);

    common::assert_approx(cursor.get_value(0).unwrap(), 0.1, 1e-3);
    common::assert_approx(cursor.get_value(5).unwrap(), 0.2, 1e-3);
    common::assert_approx(cursor.get_value(10).unwrap(), 0.3, 1e-3);
    assert_eq!(cursor.get_value(7), None);
    assert_eq!(cursor.get_value(99), None);
}

#[tokio::test]
async fn lazy_cursor_drain_essential() {
    let vectors = vec![
        (0u32, vec![(1u32, 0.5f32)]),
        (1, vec![(1, 0.25)]),
        (2, vec![(1, 0.75)]),
    ];

    let (_tmp, _prov, reader) = common::build_index(vectors).await;
    let encoded_dim = encode_u32(1);

    let (dir, _) = reader
        .get_directory(&encoded_dim)
        .await
        .unwrap()
        .expect("directory should exist");
    let offsets = dir.max_offsets().to_vec();
    let weights = dir.max_weights().to_vec();

    reader
        .posting_reader()
        .load_blocks_for_prefixes([encoded_dim.as_str()])
        .await;

    let mut cursor = PostingCursor::open_lazy(offsets.clone(), weights);
    let all_indices: Vec<usize> = (0..offsets.len()).collect();
    cursor.populate_from_cache(reader.posting_reader(), &encoded_dim, &all_indices);

    let mask = all_mask();
    let mut accum = vec![0.0f32; 4096];
    let mut bitmap = [0u64; 64];

    cursor.drain_essential(0, 2, 2.0, &mut accum, &mut bitmap, &mask);

    common::assert_approx(accum[0], 0.5 * 2.0, 1e-3);
    common::assert_approx(accum[1], 0.25 * 2.0, 1e-3);
    common::assert_approx(accum[2], 0.75 * 2.0, 1e-3);
    assert!(bitmap[0] & 0b111 == 0b111);
}
