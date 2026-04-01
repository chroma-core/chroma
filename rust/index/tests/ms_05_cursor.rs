mod common;

use chroma_index::sparse::maxscore::{PostingCursor, SparsePostingBlock};
use chroma_types::SignedRoaringBitmap;
use roaring::RoaringBitmap;

fn make_cursor(ranges: &[(u32, u32, f32)]) -> PostingCursor<'static> {
    let blocks: Vec<SparsePostingBlock> = ranges
        .iter()
        .map(|&(start, count, weight)| {
            let entries: Vec<(u32, f32)> =
                (start..start + count).map(|o| (o, weight)).collect();
            SparsePostingBlock::from_sorted_entries(&entries)
        })
        .collect();
    PostingCursor::from_blocks(blocks)
}

fn full_mask() -> SignedRoaringBitmap {
    SignedRoaringBitmap::full()
}

#[test]
fn test_ms_05_advance_basic() {
    let entries: Vec<(u32, f32)> = vec![(0, 0.5), (5, 0.6), (10, 0.7), (15, 0.8), (20, 0.9)];
    let block = SparsePostingBlock::from_sorted_entries(&entries);
    let mut cursor = PostingCursor::from_blocks(vec![block]);
    let mask = full_mask();

    let r = cursor.advance(0, &mask);
    assert_eq!(r.unwrap().0, 0);

    let r = cursor.advance(6, &mask);
    assert_eq!(r.unwrap().0, 10);

    let r = cursor.advance(20, &mask);
    assert_eq!(r.unwrap().0, 20);

    let r = cursor.advance(21, &mask);
    assert!(r.is_none());
}

#[test]
fn test_ms_05_advance_cross_block() {
    let mut cursor = make_cursor(&[(0, 256, 0.5), (256, 256, 0.7)]);
    let mask = full_mask();

    let r = cursor.advance(300, &mask);
    assert_eq!(r.unwrap().0, 300);
}

#[test]
fn test_ms_05_advance_skip_blocks() {
    let mut cursor = make_cursor(&[(0, 256, 0.5), (256, 256, 0.6), (512, 256, 0.7)]);
    let mask = full_mask();

    let r = cursor.advance(600, &mask);
    assert_eq!(r.unwrap().0, 600);
}

#[test]
fn test_ms_05_advance_include_mask() {
    let entries: Vec<(u32, f32)> = (0..5).map(|i| (i, 0.5)).collect();
    let block = SparsePostingBlock::from_sorted_entries(&entries);
    let mut cursor = PostingCursor::from_blocks(vec![block]);

    let mut rbm = RoaringBitmap::new();
    rbm.insert(1);
    rbm.insert(3);
    let mask = SignedRoaringBitmap::Include(rbm);

    let r = cursor.advance(0, &mask);
    assert_eq!(r.unwrap().0, 1);

    let r = cursor.advance(2, &mask);
    assert_eq!(r.unwrap().0, 3);

    let r = cursor.advance(4, &mask);
    assert!(r.is_none());
}

#[test]
fn test_ms_05_advance_exclude_mask() {
    let entries: Vec<(u32, f32)> = (0..5).map(|i| (i, 0.5)).collect();
    let block = SparsePostingBlock::from_sorted_entries(&entries);
    let mut cursor = PostingCursor::from_blocks(vec![block]);

    let mut rbm = RoaringBitmap::new();
    rbm.insert(1);
    rbm.insert(3);
    let mask = SignedRoaringBitmap::Exclude(rbm);

    let r = cursor.advance(0, &mask);
    assert_eq!(r.unwrap().0, 0);

    let r = cursor.advance(1, &mask);
    assert_eq!(r.unwrap().0, 2);
}

#[test]
fn test_ms_05_advance_full_mask() {
    let mut cursor = make_cursor(&[(0, 10, 0.5)]);
    let mask = full_mask();

    for i in 0..10u32 {
        let r = cursor.advance(i, &mask);
        assert_eq!(r.unwrap().0, i);
    }
    assert!(cursor.advance(10, &mask).is_none());
}

#[test]
fn test_ms_05_get_value_present() {
    let entries: Vec<(u32, f32)> = vec![(0, 0.3), (5, 0.6), (10, 0.9)];
    let block = SparsePostingBlock::from_sorted_entries(&entries);
    let mut cursor = PostingCursor::from_blocks(vec![block]);

    let v = cursor.get_value(5);
    assert!(v.is_some());
    let tol = 0.9 / 255.0 + 1e-6;
    assert!((v.unwrap() - 0.6).abs() <= tol);
}

#[test]
fn test_ms_05_get_value_absent() {
    let entries: Vec<(u32, f32)> = vec![(0, 0.3), (5, 0.6), (10, 0.9)];
    let block = SparsePostingBlock::from_sorted_entries(&entries);
    let mut cursor = PostingCursor::from_blocks(vec![block]);

    assert!(cursor.get_value(7).is_none());
}

#[test]
fn test_ms_05_get_value_cross_block() {
    let mut cursor = make_cursor(&[(0, 256, 0.5), (256, 256, 0.7)]);

    let v = cursor.get_value(300);
    assert!(v.is_some());
    let tol = 0.7 / 255.0 + 1e-6;
    assert!((v.unwrap() - 0.7).abs() <= tol);

    let v0 = cursor.get_value(0);
    assert!(v0.is_some());
}

#[test]
fn test_ms_05_get_value_beyond_end() {
    let mut cursor = make_cursor(&[(0, 256, 0.5)]);
    assert!(cursor.get_value(999999).is_none());
}

#[test]
fn test_ms_05_dimension_max() {
    let cursor = make_cursor(&[(0, 10, 0.5), (10, 10, 0.9), (20, 10, 0.3)]);
    let tol = 0.9 / 255.0 + 1e-6;
    assert!((cursor.dimension_max() - 0.9).abs() <= tol);
}

#[test]
fn test_ms_05_current_block_max() {
    let mut cursor = make_cursor(&[(0, 256, 0.5), (256, 256, 0.7)]);
    let mask = full_mask();

    let tol = 0.7 / 255.0 + 1e-6;
    assert!((cursor.current_block_max() - 0.5).abs() <= tol);

    cursor.advance(256, &mask);
    assert!((cursor.current_block_max() - 0.7).abs() <= tol);
}
