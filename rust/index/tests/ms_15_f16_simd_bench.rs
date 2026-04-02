mod common;

use chroma_index::sparse::maxscore::SparsePostingBlock;
use chroma_types::{convert_f16_to_f32_scalar, convert_f16_to_f32_simd};
use half::f16;
use std::time::Instant;

fn make_f16_bytes(n: usize) -> Vec<u8> {
    let mut rng_state = 12345u64;
    let mut bytes = Vec::with_capacity(n * 2);
    for _ in 0..n {
        rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let val = (rng_state >> 33) as f32 / (u32::MAX >> 1) as f32; // 0..1
        bytes.extend_from_slice(&f16::from_f32(val).to_le_bytes());
    }
    bytes
}

#[test]
fn bench_f16_to_f32_conversion() {
    let sizes = [256, 1024, 4096];
    let iters = 100_000;

    println!("\n  f16→f32 conversion benchmark ({iters} iterations)");
    println!("  {:>6}  {:>12}  {:>12}  {:>8}", "N", "Scalar", "SIMD", "Speedup");
    println!("  {}", "-".repeat(48));

    for &n in &sizes {
        let f16_bytes = make_f16_bytes(n);
        let mut out = vec![0.0f32; n];

        // Warm up
        for _ in 0..1000 {
            convert_f16_to_f32_scalar(&f16_bytes, &mut out);
        }

        let start = Instant::now();
        for _ in 0..iters {
            convert_f16_to_f32_scalar(&f16_bytes, &mut out);
            std::hint::black_box(&out);
        }
        let scalar_ns = start.elapsed().as_nanos() as f64 / iters as f64;

        // Warm up
        for _ in 0..1000 {
            convert_f16_to_f32_simd(&f16_bytes, &mut out);
        }

        let start = Instant::now();
        for _ in 0..iters {
            convert_f16_to_f32_simd(&f16_bytes, &mut out);
            std::hint::black_box(&out);
        }
        let simd_ns = start.elapsed().as_nanos() as f64 / iters as f64;

        let speedup = scalar_ns / simd_ns;
        println!(
            "  {:>6}  {:>9.0} ns  {:>9.0} ns  {:>6.2}x",
            n, scalar_ns, simd_ns, speedup
        );
    }
}

#[test]
fn bench_block_decompress_and_score() {
    let n = 1024;
    let iters = 50_000;

    let entries: Vec<(u32, f32)> = (0..n as u32)
        .map(|i| {
            let v = ((i as f32 * 0.618).fract() * 0.9) + 0.1;
            (i * 3, v)
        })
        .collect();
    let block = SparsePostingBlock::from_sorted_entries(&entries);
    let serialized = block.serialize();

    println!("\n  Block operations benchmark (n={n}, {iters} iterations)");

    // Deserialize + access values (triggers lazy decompression)
    let start = Instant::now();
    for _ in 0..iters {
        let b = SparsePostingBlock::deserialize(&serialized);
        std::hint::black_box(b.values());
    }
    let decompress_ns = start.elapsed().as_nanos() as f64 / iters as f64;
    println!("  Deserialize+values:  {decompress_ns:.0} ns");

    // Score block (already decompressed f32 * query_weight)
    let block = SparsePostingBlock::deserialize(&serialized);
    let _ = block.values(); // force decompress
    let mut scores = vec![0.0f32; n];
    let start = Instant::now();
    for _ in 0..iters {
        scores.fill(0.0);
        block.score_block_into(1.5, &mut scores);
        std::hint::black_box(&scores);
    }
    let score_ns = start.elapsed().as_nanos() as f64 / iters as f64;
    println!("  score_block_into:    {score_ns:.0} ns");
}
