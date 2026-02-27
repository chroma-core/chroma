//! Error analysis for RaBitQ quantization performance characteristics.
//!
//! Measures relative and absolute error of the distance estimator for 4-bit float,
//! 1-bit float, and 1-bit bitwise (QuantizedQuery) methods.
//!
//! Run:
//!   cargo bench -p chroma-index --bench quantization_error
//!   cargo bench -p chroma-index --bench quantization_error -- --distance cosine
//!   cargo bench -p chroma-index --bench quantization_error -- --distance ip
//!
//! CLI flags:
//!   --distance D      distance function: euclidean (default), cosine, ip
use chroma_distance::DistanceFunction;
use chroma_index::quantization::{Code1Bit, Code4Bit, QuantizedQuery, RabitqCode};
use criterion::{criterion_group, criterion_main, Criterion};
use faer::{
    col::ColRef,
    stats::{
        prelude::{Distribution, StandardNormal, ThreadRng},
        UnitaryMat,
    },
    Mat,
};
use rand::{rngs::StdRng, Rng, SeedableRng};

fn make_rng() -> StdRng {
    StdRng::seed_from_u64(0xdeadbeef)
}

fn random_vec(rng: &mut impl Rng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.gen_range(-1.0_f32..1.0)).collect()
}

fn random_rotation(dim: usize) -> Mat<f32> {
    let dist = UnitaryMat {
        dim,
        standard_normal: StandardNormal,
    };
    dist.sample(&mut ThreadRng::default())
}

fn rotate(rotation: &Mat<f32>, v: &[f32]) -> Vec<f32> {
    let result = rotation * ColRef::from_slice(v);
    result.iter().copied().collect()
}

fn c_norm(centroid: &[f32]) -> f32 {
    centroid.iter().map(|x| x * x).sum::<f32>().sqrt()
}

fn c_dot_q(centroid: &[f32], r_q: &[f32]) -> f32 {
    centroid.iter().zip(r_q).map(|(c, r)| c * (r + c)).sum()
}

fn q_norm(centroid: &[f32], r_q: &[f32]) -> f32 {
    r_q.iter()
        .zip(centroid)
        .map(|(r, c)| (r + c) * (r + c))
        .sum::<f32>()
        .sqrt()
}

// ── Error distribution analysis ───────────────────────────────────────────
//
// Not a timing benchmark.  Measures how accurately each implementation
// estimates the true squared Euclidean distance.
//
// A random orthogonal rotation P is applied to all vectors (centroid,
// embeddings, queries) before quantization, matching production behavior
// (quantized_spann.rs::rotate).  The rotation is what makes the RaBitQ
// estimator unbiased (Theorem 3.2): it decorrelates the sign quantization
// g = sign(Pr) from any fixed direction, so E_P[⟨g, r_q_perp⟩] = 0.
// d_true is computed from original (unrotated) vectors since P preserves L2.
//
// Queries are generated as centroid + noise so that query residuals r_q are
// zero-mean, matching the paper's unbiasedness conditions.  Without this,
// E[r_q] = -centroid creates a systematic bias that the rotation cannot fix.
//
// For every (embedding, query) pair we compute two metrics:
//
//   relative_error = (d_est − d_true) / d_true   [dimensionless, scale-free]
//   absolute_error = d_est − d_true               [in units of squared distance]
//
// where d_true = Σ(eᵢ − qᵢ)² (true squared L2 from original floats) and
// d_est comes from each quantized estimator.  Comparing the methods
// isolates two distinct error sources:
//
//   data quantization alone : 4bit_float  vs  1bit_bitwise (1-bit data + 4-bit quantized query)
//
// WHY THE RELATIVE-ERROR MEAN IS NON-ZERO (even for an unbiased estimator)
// ─────────────────────────────────────────────────────────────────────────
// Relative error ε/d_true has a strictly positive mean even for an unbiased
// estimator, due to Jensen's inequality: E[1/X] > 1/E[X] when X > 0.
//
//   • (d_est − d_true) / d_true ≥ −1  (hard floor: d_est ≥ 0)
//   • no corresponding upper bound
//
// For near pairs (small d_true), even a modest absolute overestimate
// produces a large positive relative error.  The net result is a positive
// mean relative error even when the absolute error is zero on average.
//
// Results are printed as a relative-error stats table, an absolute-error
// summary, and per-method histograms (shared x-axis, directly comparable).

fn print_error_analysis() {
    const DIM: usize = 1024;
    const N: usize = 2048; // codes per cluster
    const N_QUERIES: usize = 64; // queries to average over
    const N_BINS: usize = 20; // histogram bins
    const BAR_W: usize = 48; // max histogram bar width in chars

    let mut rng = make_rng();
    let p = random_rotation(DIM);

    let centroid_raw = random_vec(&mut rng, DIM);
    let centroid = rotate(&p, &centroid_raw);
    let df = DistanceFunction::Euclidean;
    let cn = c_norm(&centroid);
    let padded_bytes = Code1Bit::packed_len(DIM);

    // Generate embeddings. Keep originals for d_true (rotation preserves L2).
    let embeddings_raw: Vec<Vec<f32>> = (0..N).map(|_| random_vec(&mut rng, DIM)).collect();
    let embeddings: Vec<Vec<f32>> = embeddings_raw.iter().map(|e| rotate(&p, e)).collect();
    let codes_1: Vec<Vec<u8>> = embeddings
        .iter()
        .map(|emb| Code1Bit::quantize(emb, &centroid).as_ref().to_vec())
        .collect();
    let codes_4: Vec<Vec<u8>> = embeddings
        .iter()
        .map(|emb| Code4Bit::quantize(emb, &centroid).as_ref().to_vec())
        .collect();

    let total = N * N_QUERIES;
    let mut err_4bit = Vec::with_capacity(total);
    let mut err_1bitf = Vec::with_capacity(total);
    let mut err_1bitw = Vec::with_capacity(total);
    // distance_code: both the data vector and the query are quantized codes.
    // This stacks the error from quantizing both sides, isolating the combined
    // code-vs-code estimation error vs. the one-sided code-vs-query methods.
    let mut err_code4 = Vec::with_capacity(total);
    let mut err_code1 = Vec::with_capacity(total);

    // Absolute errors collected in parallel; E[abs] ≈ 0 per the paper's
    // unbiasedness claim.  Comparing against the relative-error means above
    // shows that the non-zero relative mean is a metric artefact, not a bug.
    let mut abs_4bit = Vec::with_capacity(total);
    let mut abs_1bitf = Vec::with_capacity(total);
    let mut abs_1bitw = Vec::with_capacity(total);
    let mut abs_code4 = Vec::with_capacity(total);
    let mut abs_code1 = Vec::with_capacity(total);

    // Unit-vector inner product errors: Δ(⟨n, n_q⟩) = -(d_est - d_true) / (2·‖r‖·‖r_q‖)
    // The paper (Theorem 3.2, Eq. 15) bounds this as O(1/√D) with high probability.
    // Extracting this from the Euclidean distance error isolates the RaBitQ estimator
    // error from the norm scaling, letting us directly compare against the bound.
    let mut ip_err_1bitf = Vec::with_capacity(total);
    let mut ip_err_1bitw = Vec::with_capacity(total);

    for _ in 0..N_QUERIES {
        // Generate query as centroid + noise so that r_q is zero-mean.
        let noise = random_vec(&mut rng, DIM);
        let query_raw: Vec<f32> = centroid_raw
            .iter()
            .zip(&noise)
            .map(|(c, n)| c + n)
            .collect();
        let query = rotate(&p, &query_raw);
        let r_q: Vec<f32> = query.iter().zip(&centroid).map(|(q, c)| q - c).collect();
        let cdq = c_dot_q(&centroid, &r_q);
        let qn = q_norm(&centroid, &r_q);
        // QuantizedQuery built once per query, amortized over all N codes.
        let qq = QuantizedQuery::new(&r_q, 4, padded_bytes, cn, cdq, qn);
        let r_q_norm = r_q.iter().map(|x| x * x).sum::<f32>().sqrt();
        // Quantize the query itself so distance_code can treat it as another data code.
        let cq1_bytes = Code1Bit::quantize(&query, &centroid).as_ref().to_vec();
        let cq4_bytes = Code4Bit::quantize(&query, &centroid).as_ref().to_vec();
        let cq1 = Code1Bit::new(cq1_bytes.as_slice());
        let cq4 = Code4Bit::new(cq4_bytes.as_slice());

        for i in 0..N {
            // True squared Euclidean distance from original unquantized vectors.
            // Rotation preserves L2 distance, so we use the originals.
            let d_true: f32 = embeddings_raw[i]
                .iter()
                .zip(&query_raw)
                .map(|(e, q)| (e - q) * (e - q))
                .sum();
            if d_true < f32::EPSILON {
                continue;
            }

            let c1 = Code1Bit::new(codes_1[i].as_slice());
            let c4 = Code4Bit::new(codes_4[i].as_slice());

            let d4 = c4.distance_query(&df, &r_q, cn, cdq, qn);
            let d1f = c1.distance_query(&df, &r_q, cn, cdq, qn);
            let db = c1.distance_4bit_query(&df, &qq);
            // distance_code: both vectors quantized; error comes from both sides.
            let dc4 = c4.distance_code(&df, &cq4, cn, DIM);
            let dc1 = c1.distance_code(&df, &cq1, cn, DIM);

            // Relative error: positive = overestimate, negative = underestimate.
            err_4bit.push((d4 - d_true) / d_true);
            err_1bitf.push((d1f - d_true) / d_true);
            err_1bitw.push((db - d_true) / d_true);
            err_code4.push((dc4 - d_true) / d_true);
            err_code1.push((dc1 - d_true) / d_true);

            abs_4bit.push(d4 - d_true);
            abs_1bitf.push(d1f - d_true);
            abs_1bitw.push(db - d_true);
            abs_code4.push(dc4 - d_true);
            abs_code1.push(dc1 - d_true);

            // Extract unit-vector inner product error from the Euclidean distance error.
            // d_est - d_true = -2·(⟨r, r_q⟩_est - ⟨r, r_q⟩_true)
            // Δ(⟨n, n_q⟩) = -(d_est - d_true) / (2·‖r‖·‖r_q‖)
            let denom = (2.0 * c1.norm() * r_q_norm).max(f32::EPSILON);
            ip_err_1bitf.push(-(d1f - d_true) / denom);
            ip_err_1bitw.push(-(db - d_true) / denom);
        }
    }

    // ── Descriptive statistics ────────────────────────────────────────────────
    struct Stats {
        mean: f32,
        std: f32,
        rmse: f32,
        p5: f32,
        p25: f32,
        p50: f32,
        p75: f32,
        p95: f32,
    }

    let compute_stats = |v: &mut Vec<f32>| -> Stats {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = v.len() as f32;
        let mean = v.iter().sum::<f32>() / n;
        let var = v.iter().map(|x| (x - mean) * (x - mean)).sum::<f32>() / n;
        let rmse = (v.iter().map(|x| x * x).sum::<f32>() / n).sqrt();
        let pct = |p: f32| v[((p * n) as usize).min(v.len() - 1)];
        Stats {
            mean,
            std: var.sqrt(),
            rmse,
            p5: pct(0.05),
            p25: pct(0.25),
            p50: pct(0.50),
            p75: pct(0.75),
            p95: pct(0.95),
        }
    };

    let s4 = compute_stats(&mut err_4bit);
    let s1f = compute_stats(&mut err_1bitf);
    let sb = compute_stats(&mut err_1bitw);
    let sc4 = compute_stats(&mut err_code4);
    let sc1 = compute_stats(&mut err_code1);

    let hr = "═".repeat(92);
    let sep = "─".repeat(92);

    // Per-method descriptions printed in the header for quick reference.
    let methods_desc: &[(&str, &str)] = &[
        (
            "4bit_data_full_query",
            "distance_query, 4-bit data code, raw f32 query (most accurate)",
        ),
        (
            "1bit_data_full_query",
            "distance_query_full_precision, 1-bit data code, raw f32 query",
        ),
        (
            "1bit_data_4bit_query",
            "distance_query_bitwise, 1-bit data + 4-bit quantized query (QuantizedQuery)",
        ),
        (
            "4bit_data_4bit_query",
            "distance_code, 4-bit data code vs 4-bit query code (both sides quantized)",
        ),
        (
            "1bit_data_1bit_query",
            "distance_code, 1-bit data code vs 1-bit query code (both sides quantized)",
        ),
    ];

    println!("\n{hr}");
    println!("  Relative_error = (d_est − d_true) / d_true");
    println!(
        "  dim={DIM}, N={N} codes, {N_QUERIES} queries, {} samples/method",
        N * N_QUERIES
    );
    println!("  d_true = true squared L2 between original embedding and query");
    println!("{sep}");
    println!("  Methods:");
    for (name, desc) in methods_desc {
        println!("    {:<20} {}", name, desc);
    }
    println!("{sep}");
    println!(
        "  {:<20} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "method", "mean", "std", "RMSE", "p5", "p25", "p50", "p75", "p95"
    );
    println!("{sep}");

    let row = |name: &str, s: &Stats| {
        println!(
            "  {:<20} {:>+8.5} {:>8.5} {:>8.5} {:>+8.5} {:>+8.5} {:>+8.5} {:>+8.5} {:>+8.5}",
            name, s.mean, s.std, s.rmse, s.p5, s.p25, s.p50, s.p75, s.p95
        );
    };
    row("4bit_data_full_query", &s4);
    row("1bit_data_full_query", &s1f);
    row("1bit_data_4bit_query", &sb);
    println!("{sep}");
    row("4bit_data_4bit_query", &sc4);
    row("1bit_data_1bit_query", &sc1);

    // ── Absolute error summary ────────────────────────────────────────────────
    // See the block comment above the function for a full explanation of why
    // this mean may be non-zero in our random-query test setup.  In practice
    // (fixed queries, real data) the bias is not present.
    println!("\n{hr}");
    println!("  Absolute error (d_est − d_true)  [see comment re: test-setup bias]");
    println!("{sep}");
    println!("  {:<20} {:>12} {:>12}", "method", "mean", "std");
    println!("{sep}");

    let abs_summary = |v: &[f32]| -> (f32, f32) {
        let n = v.len() as f32;
        let mean = v.iter().sum::<f32>() / n;
        let std = (v.iter().map(|x| (x - mean) * (x - mean)).sum::<f32>() / n).sqrt();
        (mean, std)
    };

    let abs_row = |name: &str, v: &[f32]| {
        let (mean, std) = abs_summary(v);
        println!("  {:<20} {:>+12.4} {:>12.4}", name, mean, std);
    };
    abs_row("4bit_data_full_query", &abs_4bit);
    abs_row("1bit_data_full_query", &abs_1bitf);
    abs_row("1bit_data_4bit_query", &abs_1bitw);
    abs_row("4bit_data_4bit_query", &abs_code4);
    abs_row("1bit_data_1bit_query", &abs_code1);

    // ── Unit-vector inner product error (paper's Theorem 3.2) ──────────────
    //
    // The paper bounds |⟨ō,q⟩/⟨ō,o⟩ - ⟨o,q⟩| = O(1/√D).
    // In our variables: |⟨n, n_q⟩_est - ⟨n, n_q⟩_true| = O(1/√D)
    // where n = r/‖r‖, n_q = r_q/‖r_q‖.
    //
    // We extract this from: Δ(⟨n, n_q⟩) = -(d_est - d_true) / (2·‖r‖·‖r_q‖)
    // This works because all other terms in the Euclidean distance formula
    // are exact; only ⟨r, r_q⟩ is estimated.
    println!("\n{hr}");
    println!(
        "  Unit-vector inner product error: |⟨n,n_q⟩_est - ⟨n,n_q⟩_true|  (paper: O(1/√D) = O({:.4}))",
        1.0 / (DIM as f32).sqrt()
    );
    println!("{sep}");
    println!(
        "  {:<20} {:>12} {:>12} {:>12} {:>12}",
        "method", "mean", "std", "p95(|err|)", "p99(|err|)"
    );
    println!("{sep}");
    let ip_summary = |name: &str, v: &mut Vec<f32>| {
        let n = v.len() as f32;
        let mean = v.iter().sum::<f32>() / n;
        let std = (v.iter().map(|x| (x - mean) * (x - mean)).sum::<f32>() / n).sqrt();
        let mut abs_v: Vec<f32> = v.iter().map(|x| x.abs()).collect();
        abs_v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p95 = abs_v[((0.95 * n) as usize).min(abs_v.len() - 1)];
        let p99 = abs_v[((0.99 * n) as usize).min(abs_v.len() - 1)];
        println!(
            "  {:<20} {:>+12.6} {:>12.6} {:>12.6} {:>12.6}",
            name, mean, std, p95, p99
        );
    };
    ip_summary("1bit_data_full_query", &mut ip_err_1bitf);
    ip_summary("1bit_data_4bit_query", &mut ip_err_1bitw);

    // ── Histograms ────────────────────────────────────────────────────────────
    // All methods share the same bin edges and the same bar scale
    // (global_max across all bins and methods), so bar lengths are directly
    // comparable across histograms.
    //
    // The range is auto-detected from the data: we take the 99th-percentile
    // absolute relative-error across all methods combined, then round up to
    // the nearest 0.01.  This zooms the x-axis to where the data actually
    // lives instead of wasting bins on an empty ±100% range.
    let range = {
        let p99_abs = |v: &[f32]| -> f32 {
            let mut abs: Vec<f32> = v.iter().map(|x| x.abs()).collect();
            abs.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let idx = ((abs.len() as f32 * 0.99) as usize).min(abs.len() - 1);
            abs[idx]
        };
        let max_p99 = p99_abs(&err_4bit)
            .max(p99_abs(&err_1bitf))
            .max(p99_abs(&err_1bitw))
            .max(p99_abs(&err_code4))
            .max(p99_abs(&err_code1));
        // Round up to nearest 0.01 so bin edges are clean numbers.
        ((max_p99 / 0.01).ceil() * 0.01).clamp(0.01, 1.0)
    };

    println!("\n{hr}");
    println!(
        "  Histograms  (±{:.0}% range, {N_BINS} bins, bars scaled to global max)",
        range * 100.0
    );
    println!("  Range auto-detected from p99 of |relative_error| across all methods.");
    println!(
        "  Values outside ±{:.0}% are counted in the ≤ / + edge bins.",
        range * 100.0
    );
    println!("{sep}");

    let bin_w = 2.0 * range / N_BINS as f32;

    let make_hist = |v: &[f32]| -> Vec<usize> {
        let mut counts = vec![0usize; N_BINS];
        for &e in v {
            let idx = ((e + range) / bin_w) as isize;
            counts[idx.clamp(0, (N_BINS - 1) as isize) as usize] += 1;
        }
        counts
    };

    let h4 = make_hist(&err_4bit);
    let h1f = make_hist(&err_1bitf);
    let hb = make_hist(&err_1bitw);
    let hc4 = make_hist(&err_code4);
    let hc1 = make_hist(&err_code1);

    let global_max = h4
        .iter()
        .chain(&h1f)
        .chain(&hb)
        .chain(&hc4)
        .chain(&hc1)
        .copied()
        .max()
        .unwrap_or(1);

    let bar = |count: usize| -> String {
        // Use eighth-block characters for sub-character precision.
        let eighths = count * BAR_W * 8 / global_max;
        let full = eighths / 8;
        let frac = eighths % 8;
        let frac_ch = [' ', '▏', '▎', '▍', '▌', '▋', '▊', '▉'][frac];
        format!(
            "{}{}",
            "█".repeat(full),
            if frac > 0 {
                frac_ch.to_string()
            } else {
                String::new()
            }
        )
    };

    let methods: &[(&str, &[usize])] = &[
        ("4bit_data_full_query", &h4),
        ("1bit_data_full_query", &h1f),
        ("1bit_data_4bit_query", &hb),
        ("4bit_data_4bit_query", &hc4),
        ("1bit_data_1bit_query", &hc1),
    ];

    for (name, hist) in methods {
        println!("\n  {name}:");
        for (i, &count) in hist.iter().enumerate() {
            let lo = -range + i as f32 * bin_w;
            let hi = lo + bin_w;
            let lo_mark = if i == 0 { "≤" } else { " " };
            let hi_mark = if i == N_BINS - 1 { "+" } else { " " };
            println!(
                "  {lo_mark}[{lo:+.3},{hi:+.3}){hi_mark} {:7} | {}",
                count,
                bar(count)
            );
        }
    }

    println!("\n{hr}");
}

fn bench_error_analysis(c: &mut Criterion) {
    let _ = c;
    print_error_analysis();
}

criterion_group!(benches, bench_error_analysis);
criterion_main!(benches);
