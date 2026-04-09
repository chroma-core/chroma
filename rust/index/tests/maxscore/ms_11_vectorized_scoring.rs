use crate::common;
use chroma_index::sparse::maxscore::PostingCursor;
use chroma_types::{SignedRoaringBitmap, SparsePostingBlock};

fn all_mask() -> SignedRoaringBitmap {
    SignedRoaringBitmap::Exclude(Default::default())
}

#[test]
fn drain_essential_multi_block() {
    let b1 = SparsePostingBlock::from_sorted_entries(&[(0, 0.5), (1, 0.25), (2, 0.75)]).unwrap();
    let b2 = SparsePostingBlock::from_sorted_entries(&[(3, 0.1), (4, 0.9)]).unwrap();

    let mut cursor = PostingCursor::from_blocks(vec![b1, b2]);
    let mask = all_mask();

    let mut accum = vec![0.0f32; 4096];
    let mut bitmap = [0u64; 64];

    cursor.drain_essential(0, 4, 1.0, &mut accum, &mut bitmap, &mask);

    common::assert_approx(accum[0], 0.5, 1e-3);
    common::assert_approx(accum[1], 0.25, 1e-3);
    common::assert_approx(accum[2], 0.75, 1e-3);
    common::assert_approx(accum[3], 0.1, 1e-3);
    common::assert_approx(accum[4], 0.9, 1e-3);
}

#[test]
fn drain_essential_window_bounds() {
    let block =
        SparsePostingBlock::from_sorted_entries(&[(0, 0.1), (5, 0.5), (10, 0.9), (15, 0.3)])
            .unwrap();
    let mut cursor = PostingCursor::from_blocks(vec![block]);
    let mask = all_mask();

    let mut accum = vec![0.0f32; 4096];
    let mut bitmap = [0u64; 64];

    cursor.drain_essential(5, 10, 1.0, &mut accum, &mut bitmap, &mask);

    assert_eq!(accum[0], 0.5); // doc 5, idx = 5 - 5 = 0
    assert_eq!(accum[5], 0.9); // doc 10, idx = 10 - 5 = 5
    assert_eq!(accum[10], 0.0); // doc 15 is outside window
}

#[test]
fn score_candidates_partial_match() {
    let block = SparsePostingBlock::from_sorted_entries(&[
        (0, 0.5),
        (2, 0.3),
        (4, 0.7),
        (6, 0.1),
        (8, 0.9),
    ])
    .unwrap();
    let mut cursor = PostingCursor::from_blocks(vec![block]);

    let cand_docs = vec![2, 6, 8];
    let mut cand_scores = vec![0.0; 3];

    cursor.score_candidates(0, 10, 2.0, &cand_docs, &mut cand_scores);

    common::assert_approx(cand_scores[0], 0.6, 1e-3); // 0.3 * 2.0
    common::assert_approx(cand_scores[1], 0.2, 1e-3); // 0.1 * 2.0
    common::assert_approx(cand_scores[2], 1.8, 1e-3); // 0.9 * 2.0
}

#[test]
fn score_candidates_no_matches() {
    let block = SparsePostingBlock::from_sorted_entries(&[(0, 0.5), (2, 0.3)]).unwrap();
    let mut cursor = PostingCursor::from_blocks(vec![block]);

    let cand_docs = vec![1, 3, 5];
    let mut cand_scores = vec![0.0; 3];

    cursor.score_candidates(0, 10, 1.0, &cand_docs, &mut cand_scores);

    assert_eq!(cand_scores, vec![0.0, 0.0, 0.0]);
}

#[test]
fn multiple_terms_accumulate() {
    let b_dim1 = SparsePostingBlock::from_sorted_entries(&[(0, 0.5), (1, 0.3)]).unwrap();
    let b_dim2 = SparsePostingBlock::from_sorted_entries(&[(0, 0.2), (1, 0.7)]).unwrap();

    let mut cursor1 = PostingCursor::from_blocks(vec![b_dim1]);
    let mut cursor2 = PostingCursor::from_blocks(vec![b_dim2]);
    let mask = all_mask();

    let mut accum = vec![0.0f32; 4096];
    let mut bitmap = [0u64; 64];

    cursor1.drain_essential(0, 1, 1.0, &mut accum, &mut bitmap, &mask);
    cursor2.drain_essential(0, 1, 1.0, &mut accum, &mut bitmap, &mask);

    common::assert_approx(accum[0], 0.7, 1e-3); // 0.5 + 0.2
    common::assert_approx(accum[1], 1.0, 1e-3); // 0.3 + 0.7
}
