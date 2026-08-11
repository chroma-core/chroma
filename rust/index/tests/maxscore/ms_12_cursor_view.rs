use crate::common;
use chroma_index::sparse::maxscore::PostingCursor;
use chroma_types::{SignedRoaringBitmap, SparsePostingBlock};

fn all_mask() -> SignedRoaringBitmap {
    SignedRoaringBitmap::Exclude(Default::default())
}

/// Build a view cursor from entry slices. Returns the owned raw bytes
/// (must stay alive) and the cursor. Values go through f16 quantization,
/// so use approximate comparisons.
fn make_view_cursor(entries_per_block: &[&[(u32, f32)]]) -> (Vec<Vec<u8>>, PostingCursor<'static>) {
    let blocks: Vec<SparsePostingBlock> = entries_per_block
        .iter()
        .map(|e| SparsePostingBlock::from_sorted_entries(e).unwrap())
        .collect();

    let dir_max_offsets: Vec<u32> = blocks.iter().map(|b| b.header.max_offset).collect();
    let dir_max_weights: Vec<f32> = blocks.iter().map(|b| b.header.max_weight).collect();

    let raw_bytes: Vec<Vec<u8>> = blocks.iter().map(|b| b.serialize()).collect();

    let raw_refs: Vec<&[u8]> = raw_bytes.iter().map(|v| v.as_slice()).collect();
    let cursor = PostingCursor::open(raw_refs, dir_max_offsets, dir_max_weights);

    // SAFETY: The cursor borrows from raw_bytes which is returned alongside
    // it. The caller must keep raw_bytes alive for the cursor's lifetime.
    let cursor: PostingCursor<'static> =
        unsafe { std::mem::transmute::<PostingCursor<'_>, PostingCursor<'static>>(cursor) };
    (raw_bytes, cursor)
}

fn assert_opt_approx(a: Option<(u32, f32)>, b: Option<(u32, f32)>, tol: f32) {
    match (a, b) {
        (Some((ao, av)), Some((bo, bv))) => {
            assert_eq!(ao, bo, "offset mismatch");
            common::assert_approx(av, bv, tol);
        }
        (None, None) => {}
        _ => panic!("mismatch: {a:?} vs {b:?}"),
    }
}

#[test]
fn view_cursor_advance_matches_eager() {
    let entries = &[(0u32, 0.1f32), (5, 0.2), (10, 0.3), (20, 0.4)];

    let block = SparsePostingBlock::from_sorted_entries(entries).unwrap();
    let mut eager = PostingCursor::from_blocks(vec![block]);

    let (_raw, mut view) = make_view_cursor(&[entries]);

    let mask = all_mask();

    for target in [0, 5, 7, 10, 15, 20, 25] {
        let e = eager.advance(target, &mask);
        let v = view.advance(target, &mask);
        assert_opt_approx(v, e, 1e-3);
        if e.is_some() {
            eager.next();
            view.next();
        }
    }
}

#[test]
fn view_cursor_multi_block_advance() {
    let b1_entries: &[(u32, f32)] = &[(0, 0.1), (5, 0.2)];
    let b2_entries: &[(u32, f32)] = &[(10, 0.3), (15, 0.4)];

    let (_raw, mut cursor) = make_view_cursor(&[b1_entries, b2_entries]);
    let mask = all_mask();

    assert_opt_approx(cursor.advance(0, &mask), Some((0, 0.1)), 1e-3);
    cursor.next();
    assert_opt_approx(cursor.advance(10, &mask), Some((10, 0.3)), 1e-3);
}

#[test]
fn view_cursor_drain_essential() {
    let entries: &[(u32, f32)] = &[(0, 0.5), (1, 0.25), (2, 0.75)];
    let (_raw, mut cursor) = make_view_cursor(&[entries]);
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
fn view_cursor_score_candidates() {
    let entries: &[(u32, f32)] = &[(0, 0.5), (1, 0.25), (2, 0.75), (5, 0.1)];
    let (_raw, mut cursor) = make_view_cursor(&[entries]);

    let cand_docs = vec![0, 2, 5];
    let mut cand_scores = vec![0.0; 3];
    cursor.score_candidates(0, 10, 1.0, &cand_docs, &mut cand_scores);

    common::assert_approx(cand_scores[0], 0.5, 1e-3);
    common::assert_approx(cand_scores[1], 0.75, 1e-3);
    common::assert_approx(cand_scores[2], 0.1, 1e-3);
}

#[test]
fn view_cursor_get_value() {
    let b1: &[(u32, f32)] = &[(0, 0.1), (5, 0.2)];
    let b2: &[(u32, f32)] = &[(10, 0.3), (15, 0.4)];
    let (_raw, mut cursor) = make_view_cursor(&[b1, b2]);

    common::assert_approx(cursor.get_value(0).unwrap(), 0.1, 1e-3);
    common::assert_approx(cursor.get_value(5).unwrap(), 0.2, 1e-3);
    common::assert_approx(cursor.get_value(10).unwrap(), 0.3, 1e-3);
    common::assert_approx(cursor.get_value(15).unwrap(), 0.4, 1e-3);
    assert_eq!(cursor.get_value(7), None);
    assert_eq!(cursor.get_value(100), None);
}

#[test]
fn view_cursor_window_upper_bound() {
    let b1: &[(u32, f32)] = &[(0, 0.1), (5, 0.9)];
    let b2: &[(u32, f32)] = &[(10, 0.3), (15, 0.4)];
    let (_raw, cursor) = make_view_cursor(&[b1, b2]);

    assert_eq!(cursor.window_upper_bound(0, 5), 0.9);
    assert_eq!(cursor.window_upper_bound(10, 15), 0.4);
    let ub = cursor.window_upper_bound(0, 15);
    assert!(ub >= 0.9);
}

#[test]
fn view_cursor_current() {
    let entries: &[(u32, f32)] = &[(3, 0.5), (7, 0.9)];
    let (_raw, mut cursor) = make_view_cursor(&[entries]);

    assert_opt_approx(cursor.current(), Some((3, 0.5)), 1e-3);
    cursor.next();
    assert_opt_approx(cursor.current(), Some((7, 0.9)), 1e-3);
    cursor.next();
    assert_eq!(cursor.current(), None);
}
