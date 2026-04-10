use crate::common;
use chroma_index::sparse::maxscore::PostingCursor;
use chroma_index::sparse::types::encode_u32;
use chroma_types::SignedRoaringBitmap;

fn all_mask() -> SignedRoaringBitmap {
    SignedRoaringBitmap::Exclude(Default::default())
}

/// Lazy cursor with only some blocks loaded. Unloaded blocks should
/// be transparently skipped during advance.
#[tokio::test]
async fn lazy_partial_load_advance_skips_unloaded() {
    // Use small block_size to force multiple blocks per dimension.
    // With block_size=2, offsets [0,1,2,3,4,5] produce 3 blocks:
    //   block 0: [0, 1], block 1: [2, 3], block 2: [4, 5]
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..6)
        .map(|i| (i, vec![(1u32, 0.1 * (i as f32 + 1.0))]))
        .collect();

    let (_tmp, _prov, reader) = common::build_index_with_block_size(vectors, Some(2)).await;
    let encoded_dim = encode_u32(1);

    let dir = reader
        .get_directory(&encoded_dim)
        .await
        .unwrap()
        .expect("directory should exist");
    let dir_max_offsets = dir.max_offsets().to_vec();
    let dir_max_weights = dir.max_weights().to_vec();

    assert_eq!(dir_max_offsets.len(), 3, "expected 3 blocks");

    // Load only block 0 and block 2, leave block 1 unloaded.
    reader
        .posting_reader()
        .load_blocks_for_prefixes([encoded_dim.as_str()])
        .await;

    let mut cursor = PostingCursor::open_lazy(dir_max_offsets.clone(), dir_max_weights.clone());
    // Only populate blocks 0 and 2
    cursor.populate_from_cache(reader.posting_reader(), &encoded_dim, &[0, 2]);

    let mask = all_mask();

    // advance(0) should find doc 0 in block 0
    let r = cursor.advance(0, &mask);
    assert_eq!(r.map(|(o, _)| o), Some(0));
    cursor.next();

    // advance(1) should find doc 1 in block 0
    let r = cursor.advance(1, &mask);
    assert_eq!(r.map(|(o, _)| o), Some(1));
    cursor.next();

    // advance(2) should skip block 1 (unloaded) and find doc 4 in block 2
    let r = cursor.advance(2, &mask);
    assert_eq!(r.map(|(o, _)| o), Some(4));
    cursor.next();

    // advance(5) should find doc 5 in block 2
    let r = cursor.advance(5, &mask);
    assert_eq!(r.map(|(o, _)| o), Some(5));
    cursor.next();

    // Exhausted
    let r = cursor.advance(6, &mask);
    assert_eq!(r, None);
}

/// Lazy cursor with partially loaded blocks used with drain_essential.
/// Entries from unloaded blocks should not appear in the accumulator.
#[tokio::test]
async fn lazy_partial_load_drain_skips_unloaded() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..6)
        .map(|i| (i, vec![(1u32, 0.1 * (i as f32 + 1.0))]))
        .collect();

    let (_tmp, _prov, reader) = common::build_index_with_block_size(vectors, Some(2)).await;
    let encoded_dim = encode_u32(1);

    let dir = reader
        .get_directory(&encoded_dim)
        .await
        .unwrap()
        .expect("directory should exist");
    let dir_max_offsets = dir.max_offsets().to_vec();
    let dir_max_weights = dir.max_weights().to_vec();

    reader
        .posting_reader()
        .load_blocks_for_prefixes([encoded_dim.as_str()])
        .await;

    let mut cursor = PostingCursor::open_lazy(dir_max_offsets, dir_max_weights);
    // Only populate block 0 and block 2 (skip block 1 with offsets [2,3])
    cursor.populate_from_cache(reader.posting_reader(), &encoded_dim, &[0, 2]);

    let mask = all_mask();
    let mut accum = vec![0.0f32; 4096];
    let mut bitmap = [0u64; 64];

    cursor.drain_essential(0, 5, 1.0, &mut accum, &mut bitmap, &mask);

    // Block 0 entries (offsets 0, 1) should be accumulated
    assert!(accum[0] > 0.0, "offset 0 should be in accum");
    assert!(accum[1] > 0.0, "offset 1 should be in accum");

    // Block 1 entries (offsets 2, 3) should NOT be accumulated (block unloaded)
    assert_eq!(
        accum[2], 0.0,
        "offset 2 should NOT be in accum (block unloaded)"
    );
    assert_eq!(
        accum[3], 0.0,
        "offset 3 should NOT be in accum (block unloaded)"
    );

    // Block 2 entries (offsets 4, 5) should be accumulated
    assert!(accum[4] > 0.0, "offset 4 should be in accum");
    assert!(accum[5] > 0.0, "offset 5 should be in accum");

    // Bitmap should reflect only the loaded entries
    let touched_bits = bitmap[0];
    assert!(touched_bits & (1 << 0) != 0, "bit 0 set");
    assert!(touched_bits & (1 << 1) != 0, "bit 1 set");
    assert!(touched_bits & (1 << 2) == 0, "bit 2 not set");
    assert!(touched_bits & (1 << 3) == 0, "bit 3 not set");
    assert!(touched_bits & (1 << 4) != 0, "bit 4 set");
    assert!(touched_bits & (1 << 5) != 0, "bit 5 set");
}

/// Lazy cursor with partially loaded blocks used with score_candidates.
/// Candidates matching unloaded blocks should not get scores.
#[tokio::test]
async fn lazy_partial_load_score_candidates_skips_unloaded() {
    let vectors: Vec<(u32, Vec<(u32, f32)>)> = (0..6)
        .map(|i| (i, vec![(1u32, 0.1 * (i as f32 + 1.0))]))
        .collect();

    let (_tmp, _prov, reader) = common::build_index_with_block_size(vectors, Some(2)).await;
    let encoded_dim = encode_u32(1);

    let dir = reader
        .get_directory(&encoded_dim)
        .await
        .unwrap()
        .expect("directory should exist");
    let dir_max_offsets = dir.max_offsets().to_vec();
    let dir_max_weights = dir.max_weights().to_vec();

    reader
        .posting_reader()
        .load_blocks_for_prefixes([encoded_dim.as_str()])
        .await;

    let mut cursor = PostingCursor::open_lazy(dir_max_offsets, dir_max_weights);
    // Only populate block 0 and block 2
    cursor.populate_from_cache(reader.posting_reader(), &encoded_dim, &[0, 2]);

    // Candidates from all three blocks
    let cand_docs = vec![0u32, 1, 2, 3, 4, 5];
    let mut cand_scores = vec![0.0f32; 6];

    cursor.score_candidates(0, 5, 1.0, &cand_docs, &mut cand_scores);

    // Candidates from loaded blocks get scores
    assert!(
        cand_scores[0] > 0.0,
        "doc 0 should be scored (block 0 loaded)"
    );
    assert!(
        cand_scores[1] > 0.0,
        "doc 1 should be scored (block 0 loaded)"
    );

    // Candidates from unloaded block 1 get no scores
    assert_eq!(
        cand_scores[2], 0.0,
        "doc 2 should not be scored (block 1 unloaded)"
    );
    assert_eq!(
        cand_scores[3], 0.0,
        "doc 3 should not be scored (block 1 unloaded)"
    );

    // Candidates from loaded block 2 get scores
    assert!(
        cand_scores[4] > 0.0,
        "doc 4 should be scored (block 2 loaded)"
    );
    assert!(
        cand_scores[5] > 0.0,
        "doc 5 should be scored (block 2 loaded)"
    );
}
