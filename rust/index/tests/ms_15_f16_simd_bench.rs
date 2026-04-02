mod common;

use half::f16;
use std::hint::black_box;
use std::time::Instant;

fn make_f16_bytes(n: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(n * 2);
    for i in 0..n {
        let val = ((i as f32 * 0.618).fract() * 0.9) + 0.1;
        bytes.extend_from_slice(&f16::from_f32(val).to_le_bytes());
    }
    bytes
}

fn make_u8_weights(n: usize, max_weight: f32) -> Vec<u8> {
    let scale = 255.0 / max_weight;
    (0..n)
        .map(|i| {
            let val = ((i as f32 * 0.618).fract() * 0.9) + 0.1;
            (val * scale).round().clamp(0.0, 255.0) as u8
        })
        .collect()
}

#[inline(always)]
fn fast_f16_to_f32(f16_bytes: &[u8], idx: usize) -> f32 {
    let bp = idx * 2;
    let h = u16::from_le_bytes([f16_bytes[bp], f16_bytes[bp + 1]]) as u32;
    let sign = (h & 0x8000) << 16;
    let nosign = (h & 0x7FFF) << 13;
    f32::from_bits(sign | (nosign + 0x3800_0000))
}

#[inline(always)]
fn half_crate_f16(f16_bytes: &[u8], idx: usize) -> f32 {
    let bp = idx * 2;
    f16::from_le_bytes([f16_bytes[bp], f16_bytes[bp + 1]]).to_f32()
}

#[test]
fn bench_per_element_drain_pattern() {
    let n = 1024;
    let iters = 200_000;
    let max_weight = 1.0f32;

    let f16_bytes = make_f16_bytes(n);
    let u8_weights = make_u8_weights(n, max_weight);
    let factor = 1.5f32 * max_weight / 255.0;
    let query_weight = 1.5f32;

    // Pre-decompressed f32 values (simulates bulk decompress path)
    let f32_values: Vec<f32> = (0..n)
        .map(|i| f16::from_f32(((i as f32 * 0.618).fract() * 0.9) + 0.1).to_f32())
        .collect();

    // Simulate drain_essential: iterate all entries, accumulate ~70% of them
    // (simulating mask hits). This matches the real access pattern.
    let mask: Vec<bool> = (0..n).map(|i| i % 10 < 7).collect();
    let mut accum = vec![0.0f32; n];

    // ── Approach 1: old u8 path ──
    for _ in 0..1000 {
        // warmup
        accum.fill(0.0);
        for pos in 0..n {
            if mask[pos] {
                accum[pos] += u8_weights[pos] as f32 * factor;
            }
        }
    }
    let start = Instant::now();
    for _ in 0..iters {
        accum.fill(0.0);
        for pos in 0..n {
            if mask[pos] {
                accum[pos] += u8_weights[pos] as f32 * factor;
            }
        }
        black_box(&accum);
    }
    let u8_ns = start.elapsed().as_nanos() as f64 / iters as f64;

    // ── Approach 2: half crate to_f32() ──
    for _ in 0..1000 {
        accum.fill(0.0);
        for pos in 0..n {
            if mask[pos] {
                accum[pos] += half_crate_f16(&f16_bytes, pos) * query_weight;
            }
        }
    }
    let start = Instant::now();
    for _ in 0..iters {
        accum.fill(0.0);
        for pos in 0..n {
            if mask[pos] {
                accum[pos] += half_crate_f16(&f16_bytes, pos) * query_weight;
            }
        }
        black_box(&accum);
    }
    let half_ns = start.elapsed().as_nanos() as f64 / iters as f64;

    // ── Approach 3: inline bit-trick ──
    for _ in 0..1000 {
        accum.fill(0.0);
        for pos in 0..n {
            if mask[pos] {
                accum[pos] += fast_f16_to_f32(&f16_bytes, pos) * query_weight;
            }
        }
    }
    let start = Instant::now();
    for _ in 0..iters {
        accum.fill(0.0);
        for pos in 0..n {
            if mask[pos] {
                accum[pos] += fast_f16_to_f32(&f16_bytes, pos) * query_weight;
            }
        }
        black_box(&accum);
    }
    let bittrick_ns = start.elapsed().as_nanos() as f64 / iters as f64;

    // ── Approach 4: pre-decompressed f32 buffer (bulk SIMD then read) ──
    for _ in 0..1000 {
        accum.fill(0.0);
        for pos in 0..n {
            if mask[pos] {
                accum[pos] += f32_values[pos] * query_weight;
            }
        }
    }
    let start = Instant::now();
    for _ in 0..iters {
        accum.fill(0.0);
        for pos in 0..n {
            if mask[pos] {
                accum[pos] += f32_values[pos] * query_weight;
            }
        }
        black_box(&accum);
    }
    let f32buf_ns = start.elapsed().as_nanos() as f64 / iters as f64;

    // ── Approach 5: bulk SIMD convert + iterate f32 (full cost) ──
    let mut val_buf = vec![0.0f32; n];
    for _ in 0..1000 {
        accum.fill(0.0);
        chroma_types::convert_f16_to_f32_simd(&f16_bytes, &mut val_buf);
        for pos in 0..n {
            if mask[pos] {
                accum[pos] += val_buf[pos] * query_weight;
            }
        }
    }
    let start = Instant::now();
    for _ in 0..iters {
        accum.fill(0.0);
        chroma_types::convert_f16_to_f32_simd(&f16_bytes, &mut val_buf);
        for pos in 0..n {
            if mask[pos] {
                accum[pos] += val_buf[pos] * query_weight;
            }
        }
        black_box(&accum);
    }
    let bulk_ns = start.elapsed().as_nanos() as f64 / iters as f64;

    println!("\n  drain_essential per-block cost (n={n}, {iters} iters, 70% mask hit)");
    println!("  {:<32} {:>9}  {:>8}", "Approach", "Time", "vs u8");
    println!("  {}", "-".repeat(55));
    println!(
        "  {:<32} {:>6.0} ns  {:>6.2}x",
        "1. u8 + factor (baseline)", u8_ns, 1.0
    );
    println!(
        "  {:<32} {:>6.0} ns  {:>6.2}x",
        "2. half crate to_f32()",
        half_ns,
        half_ns / u8_ns
    );
    println!(
        "  {:<32} {:>6.0} ns  {:>6.2}x",
        "3. inline bit-trick",
        bittrick_ns,
        bittrick_ns / u8_ns
    );
    println!(
        "  {:<32} {:>6.0} ns  {:>6.2}x",
        "4. pre-decompressed f32 (read)",
        f32buf_ns,
        f32buf_ns / u8_ns
    );
    println!(
        "  {:<32} {:>6.0} ns  {:>6.2}x",
        "5. bulk SIMD + iterate f32",
        bulk_ns,
        bulk_ns / u8_ns
    );
}
