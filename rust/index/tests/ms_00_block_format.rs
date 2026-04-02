mod common;

use chroma_index::sparse::maxscore::SparsePostingBlock;
use common::{assert_approx, make_block, sequential_entries};

fn f16_tolerance(_max_weight: f32) -> f32 {
    1e-3
}

fn assert_roundtrip_offsets_exact(entries: &[(u32, f32)]) {
    let block = make_block(entries);
    let bytes = block.serialize();
    let restored = SparsePostingBlock::deserialize(&bytes);
    assert_eq!(
        restored.offsets(), block.offsets(),
        "offsets must round-trip exactly"
    );
}

fn assert_roundtrip_values_bounded(entries: &[(u32, f32)]) {
    let block = make_block(entries);
    let tol = f16_tolerance(block.max_weight);
    let bytes = block.serialize();
    let restored = SparsePostingBlock::deserialize(&bytes);
    for (i, (&orig, &restored_v)) in block.values().iter().zip(restored.values().iter()).enumerate() {
        assert_approx(
            restored_v,
            orig,
            tol,
        );
        let _ = i; // suppress unused warning
    }
}

#[test]
fn test_ms_00_roundtrip_small() {
    let entries = vec![(0, 1.0), (5, 0.5), (100, 0.8)];
    assert_roundtrip_offsets_exact(&entries);
    assert_roundtrip_values_bounded(&entries);
}

#[test]
fn test_ms_00_roundtrip_256() {
    let entries = sequential_entries(0, 1, 256, 0.7);
    assert_roundtrip_offsets_exact(&entries);
    assert_roundtrip_values_bounded(&entries);
}

#[test]
fn test_ms_00_roundtrip_1_entry() {
    let entries = vec![(42, 0.9)];
    assert_roundtrip_offsets_exact(&entries);
    assert_roundtrip_values_bounded(&entries);
}

#[test]
fn test_ms_00_roundtrip_128() {
    let entries = sequential_entries(10, 3, 128, 0.5);
    assert_roundtrip_offsets_exact(&entries);
    assert_roundtrip_values_bounded(&entries);
}

#[test]
fn test_ms_00_roundtrip_129() {
    let entries = sequential_entries(10, 3, 129, 0.5);
    assert_roundtrip_offsets_exact(&entries);
    assert_roundtrip_values_bounded(&entries);
}

#[test]
fn test_ms_00_roundtrip_255() {
    let entries = sequential_entries(0, 2, 255, 0.6);
    assert_roundtrip_offsets_exact(&entries);
    assert_roundtrip_values_bounded(&entries);
}

#[test]
fn test_ms_00_roundtrip_512() {
    let entries = sequential_entries(0, 1, 512, 0.4);
    assert_roundtrip_offsets_exact(&entries);
    assert_roundtrip_values_bounded(&entries);
}

#[test]
fn test_ms_00_large_deltas() {
    let entries = vec![(0, 0.5), (1_000_000, 0.8), (2_000_000, 0.3)];
    assert_roundtrip_offsets_exact(&entries);
    assert_roundtrip_values_bounded(&entries);
}

#[test]
fn test_ms_00_consecutive_offsets() {
    let entries: Vec<(u32, f32)> = (0..256).map(|i| (i as u32, 0.5)).collect();
    assert_roundtrip_offsets_exact(&entries);
    assert_roundtrip_values_bounded(&entries);
}

#[test]
fn test_ms_00_uniform_weights() {
    let entries: Vec<(u32, f32)> = (0..256).map(|i| (i as u32 * 10, 0.5)).collect();
    let block = make_block(&entries);
    let bytes = block.serialize();
    let restored = SparsePostingBlock::deserialize(&bytes);
    let tol = f16_tolerance(block.max_weight);
    for &v in restored.values() {
        assert_approx(v, 0.5, tol);
    }
}

#[test]
fn test_ms_00_tiny_weights() {
    let entries = vec![(0, 0.001), (1, 1.0)];
    let block = make_block(&entries);
    let bytes = block.serialize();
    let restored = SparsePostingBlock::deserialize(&bytes);
    assert_eq!(restored.offsets(), block.offsets());
    let tol = f16_tolerance(block.max_weight);
    assert_approx(restored.values()[1], 1.0, tol);
    assert!(restored.values()[0] < 0.01);
}

#[test]
fn test_ms_00_quantization_precision() {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    let mut rng = StdRng::seed_from_u64(12345);
    let entries: Vec<(u32, f32)> = (0..256)
        .map(|i| (i as u32 * 7, rng.gen_range(0.01..1.0)))
        .collect();

    let block = make_block(&entries);
    let tol = f16_tolerance(block.max_weight);
    let bytes = block.serialize();
    let restored = SparsePostingBlock::deserialize(&bytes);

    for (i, (&orig, &rest)) in block.values().iter().zip(restored.values().iter()).enumerate() {
        assert!(
            (rest - orig).abs() <= tol,
            "entry {i}: expected {orig} ± {tol}, got {rest}"
        );
    }
}

#[test]
fn test_ms_00_header_fields() {
    let entries = vec![(10, 0.5), (20, 0.9), (30, 0.2)];
    let block = make_block(&entries);
    let bytes = block.serialize();
    let restored = SparsePostingBlock::deserialize(&bytes);
    assert_eq!(restored.min_offset, 10);
    assert_eq!(restored.max_offset, 30);
    assert_eq!(restored.max_weight, 0.9);
    assert_eq!(restored.offsets().len(), 3);
}

#[test]
fn test_ms_00_serialized_size() {
    let entries = sequential_entries(0, 1, 256, 0.5);
    let block = make_block(&entries);
    let bytes = block.serialize();

    let n = 256usize;
    let full_groups = n / 128;
    let remainder = n % 128;
    let packed_group_bytes = 128 * (bytes[2] as usize) / 8;
    let expected = 16 + full_groups * packed_group_bytes + remainder * 4 + n * 2;
    assert_eq!(
        bytes.len(),
        expected,
        "serialized size must match formula: header(16) + packed({}) + remainder({}) + f16_weights({})",
        full_groups * packed_group_bytes,
        remainder * 4,
        n * 2
    );
}
