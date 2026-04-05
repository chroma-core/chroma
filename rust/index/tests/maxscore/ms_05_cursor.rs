use crate::common;
use chroma_index::sparse::maxscore::PostingCursor;
use chroma_types::{SignedRoaringBitmap, SparsePostingBlock};

fn all_mask() -> SignedRoaringBitmap {
    SignedRoaringBitmap::Exclude(Default::default())
}

#[test]
fn cursor_sequential_advance() {
    let block = SparsePostingBlock::from_sorted_entries(&[
        (0, 0.1), (1, 0.2), (2, 0.3), (3, 0.4),
    ]).unwrap();
    let mut cursor = PostingCursor::from_blocks(vec![block]);

    assert_eq!(cursor.advance(0, &all_mask()), Some((0, 0.1)));
    cursor.next();
    assert_eq!(cursor.advance(2, &all_mask()), Some((2, 0.3)));
    cursor.next();
    assert_eq!(cursor.advance(3, &all_mask()), Some((3, 0.4)));
    cursor.next();
    assert_eq!(cursor.advance(4, &all_mask()), None);
}

#[test]
fn cursor_multi_block_advance() {
    let b1 = SparsePostingBlock::from_sorted_entries(&[(0, 0.1), (5, 0.2)]).unwrap();
    let b2 = SparsePostingBlock::from_sorted_entries(&[(10, 0.3), (15, 0.4)]).unwrap();
    let mut cursor = PostingCursor::from_blocks(vec![b1, b2]);

    assert_eq!(cursor.advance(0, &all_mask()), Some((0, 0.1)));
    cursor.next();
    let r = cursor.advance(10, &all_mask());
    assert_eq!(r, Some((10, 0.3)));
}

#[test]
fn cursor_advance_with_include_mask() {
    let block = SparsePostingBlock::from_sorted_entries(&[
        (0, 0.1), (1, 0.2), (2, 0.3), (3, 0.4),
    ]).unwrap();
    let mut rbm = roaring::RoaringBitmap::new();
    rbm.insert(1);
    rbm.insert(3);
    let mask = SignedRoaringBitmap::Include(rbm);

    let mut cursor = PostingCursor::from_blocks(vec![block]);
    assert_eq!(cursor.advance(0, &mask), Some((1, 0.2)));
    cursor.next();
    assert_eq!(cursor.advance(2, &mask), Some((3, 0.4)));
}

#[test]
fn cursor_advance_with_exclude_mask() {
    let block = SparsePostingBlock::from_sorted_entries(&[
        (0, 0.1), (1, 0.2), (2, 0.3), (3, 0.4),
    ]).unwrap();
    let mut rbm = roaring::RoaringBitmap::new();
    rbm.insert(0);
    rbm.insert(2);
    let mask = SignedRoaringBitmap::Exclude(rbm);

    let mut cursor = PostingCursor::from_blocks(vec![block]);
    assert_eq!(cursor.advance(0, &mask), Some((1, 0.2)));
}

#[test]
fn cursor_window_upper_bound() {
    let b1 = SparsePostingBlock::from_sorted_entries(&[(0, 0.1), (5, 0.9)]).unwrap();
    let b2 = SparsePostingBlock::from_sorted_entries(&[(10, 0.3), (15, 0.4)]).unwrap();
    let cursor = PostingCursor::from_blocks(vec![b1, b2]);

    assert_eq!(cursor.window_upper_bound(0, 5), 0.9);
    assert_eq!(cursor.window_upper_bound(10, 15), 0.4);
    let ub = cursor.window_upper_bound(0, 15);
    assert!(ub >= 0.9);
}

#[test]
fn cursor_get_value_across_blocks() {
    let b1 = SparsePostingBlock::from_sorted_entries(&[(0, 0.1), (5, 0.2)]).unwrap();
    let b2 = SparsePostingBlock::from_sorted_entries(&[(10, 0.3), (15, 0.4)]).unwrap();
    let mut cursor = PostingCursor::from_blocks(vec![b1, b2]);

    assert_eq!(cursor.get_value(0), Some(0.1));
    assert_eq!(cursor.get_value(5), Some(0.2));
    assert_eq!(cursor.get_value(10), Some(0.3));
    assert_eq!(cursor.get_value(15), Some(0.4));
    assert_eq!(cursor.get_value(7), None);
    assert_eq!(cursor.get_value(100), None);
}

#[test]
fn cursor_drain_essential_basic() {
    let block = SparsePostingBlock::from_sorted_entries(&[
        (0, 0.5), (1, 0.25), (2, 0.75),
    ]).unwrap();
    let mut cursor = PostingCursor::from_blocks(vec![block]);
    let mask = all_mask();

    let mut accum = vec![0.0f32; 4096];
    let mut bitmap = [0u64; 64];

    cursor.drain_essential(0, 2, 2.0, &mut accum, &mut bitmap, &mask);

    common::assert_approx(accum[0], 0.5 * 2.0, 1e-3);
    common::assert_approx(accum[1], 0.25 * 2.0, 1e-3);
    common::assert_approx(accum[2], 0.75 * 2.0, 1e-3);
    assert!(bitmap[0] & 0b111 == 0b111);
}

#[test]
fn cursor_score_candidates_basic() {
    let block = SparsePostingBlock::from_sorted_entries(&[
        (0, 0.5), (1, 0.25), (2, 0.75), (5, 0.1),
    ]).unwrap();
    let mut cursor = PostingCursor::from_blocks(vec![block]);

    let cand_docs = vec![0, 2, 5];
    let mut cand_scores = vec![0.0; 3];
    cursor.score_candidates(0, 10, 1.0, &cand_docs, &mut cand_scores);

    common::assert_approx(cand_scores[0], 0.5, 1e-3);
    common::assert_approx(cand_scores[1], 0.75, 1e-3);
    common::assert_approx(cand_scores[2], 0.1, 1e-3);
}
