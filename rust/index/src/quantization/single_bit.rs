//! 1-bit RaBitQ quantization and associated query structures.
//!
//! This module contains:
//! - [`Code::<1>`]: 1-bit quantized code with precomputed `signed_sum`.
//! - [`QuantizedQuery`]: Pre-computed query quantization for the bitwise distance path.
//! - [`BatchQueryLuts`]: Pre-computed lookup tables for batch distance estimation.

use std::mem::size_of;

use bytemuck::{Pod, Zeroable};
use chroma_distance::DistanceFunction;
use simsimd::{BinarySimilarity, SpatialSimilarity};

use super::{rabitq_distance_code, rabitq_distance_query};
use super::Code;

const B_Q: u8 = 4;

// ── Header ────────────────────────────────────────────────────────────────────

/// Header for 1-bit codes. Extends the 4-bit layout with `signed_sum`
/// (2·popcount(packed) − dim), precomputed at index time for zero-cost query scoring.
/// 16 bytes.
/// Field order must start with the common fields to maintain compatibility with
/// the generic Code functions: correction, norm, radial.
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct CodeHeader1 {
    correction: f32,
    norm: f32,
    radial: f32,
    signed_sum: i32,
}

// ── Code<1, T> ────────────────────────────────────────────────────────────────

impl<T: AsRef<[u8]>> Code<1, T> {
    /// Correction factor `⟨g, n⟩`.
    pub fn correction(&self) -> f32 {
        bytemuck::pod_read_unaligned::<f32>(&self.0.as_ref()[0..4])
    }
    /// Data residual norm `‖r‖`.
    pub fn norm(&self) -> f32 {
        bytemuck::pod_read_unaligned::<f32>(&self.0.as_ref()[4..8])
    }
    /// Radial component `⟨r, c⟩`.
    pub fn radial(&self) -> f32 {
        bytemuck::pod_read_unaligned::<f32>(&self.0.as_ref()[8..12])
    }

    /// Packed quantization codes (excluding the header).
    pub fn packed(&self) -> &[u8] {
        &self.0.as_ref()[size_of::<CodeHeader1>()..]
    }

    /// Estimates distance from data vector `d` to full-precision query `q`.
    ///
    /// Computes `⟨g, r_q⟩ = 0.5 · signed_dot(packed, r_q)`: each bit contributes
    /// `+r_q[i]` (bit=1) or `−r_q[i]` (bit=0).
    ///
    /// # Derivation
    ///
    /// For BITS=1, `g[i]` is `+0.5` when bit=1 and `−0.5` when bit=0. So:
    /// ```text
    /// ⟨g, r_q⟩ = 0.5 · Σ g[i] · r_q[i]
    ///          = 0.5 · Σ sign(g[i]) · r_q[i]
    ///          = 0.5 · (Σ_{bit=1} r_q[i] − Σ_{bit=0} r_q[i])
    ///          = 0.5 · (Σ_{g[i]=+0.5} r_q[i] − Σ_{g[i]=−0.5} r_q[i])
    /// ```
    pub fn distance_query(
        &self,
        distance_fn: &DistanceFunction,
        r_q: &[f32],
        c_norm: f32,
        c_dot_q: f32,
        q_norm: f32,
    ) -> f32 {
        let g_dot_r_q = 0.5 * signed_dot(self.packed(), r_q);
        rabitq_distance_query(
            g_dot_r_q,
            self.correction(),
            self.norm(),
            self.radial(),
            c_norm,
            c_dot_q,
            q_norm,
            distance_fn,
        )
    }

    /// Estimates distance between two original data vectors `d_a` and `d_b`.
    ///
    /// For 1-bit codes, computes `⟨g_a, g_b⟩` via Hamming distance:
    /// ```text
    /// ⟨g_a, g_b⟩ = 0.25 · (dim − 2·hamming(a, b))
    /// ```
    ///
    /// # Derivation
    ///
    /// Each `g[i] ∈ {−0.5, +0.5}` (bit=1 → +0.5, bit=0 → −0.5). So each term
    /// `g_a[i]·g_b[i]` is:
    /// - **+0.25** when the bits agree (both 1 or both 0)
    /// - **−0.25** when they disagree
    ///
    /// Let `agree` = number of agreeing positions, `disagree` = hamming(a,b).
    /// Then `dim = agree + disagree` and:
    /// ```text
    /// ⟨g_a, g_b⟩ = agree·0.25 + disagree·(−0.25)
    ///            = 0.25·(agree − disagree)
    ///            = 0.25·((dim − hamming) − hamming)
    ///            = 0.25·(dim − 2·hamming(a, b))
    /// ```
    pub fn distance_code(
        &self,
        other: &Code<1, impl AsRef<[u8]>>,
        distance_fn: &DistanceFunction,
        c_norm: f32,
        dim: usize,
    ) -> f32 {
        let hamming = hamming_distance(self.packed(), other.packed());
        let g_a_dot_g_b = 0.25 * (dim as f32 - 2.0 * hamming as f32);
        rabitq_distance_code(
            g_a_dot_g_b,
            self.correction(),
            self.norm(),
            self.radial(),
            other.correction(),
            other.norm(),
            other.radial(),
            c_norm,
            distance_fn,
        )
    }

    /// Precomputed `signed_sum = 2·popcount(packed) − dim`, stored in the header.
    pub fn signed_sum(&self) -> i32 {
        bytemuck::pod_read_unaligned::<i32>(&self.0.as_ref()[12..16])
    }

    // ── Bitwise query path ───────────────────────────────

    /// Estimates distance from a stored data code to a quantized query.
    ///
    /// # Paper equation 22 (Section 3.3.2)
    ///
    /// The paper estimates `⟨x̄, q̄⟩` using `B_q` rounds of AND + popcount
    /// on packed D-bit strings. The key identity is:
    ///
    /// ```text
    /// ⟨x_b, q_u⟩ = Σ_j  2^j · popcount(x_b AND q_u^(j))
    /// ```
    ///
    /// where `q_u^(j)` is the `j`-th bit plane of the quantized query (the
    /// `j`-th bit of each `q_u[i]` packed into a D-bit string), and `x_b` is
    /// the 1-bit data code (the packed sign bits).
    ///
    /// The full estimator (Equation 20) recovers `⟨x̄, q̄⟩` from `⟨x_b, q_u⟩`:
    ///
    /// ```text
    /// ⟨x̄, q̄⟩ = (2Δ/√D)·⟨x_b, q_u⟩ + (2v_l/√D)·Σ x_b[i] - (Δ/√D)·Σ q_u[i] - √D·v_l
    /// ```
    ///
    /// # Our derivation of Equation 20
    ///
    /// We adapt to our conventions — residuals (`r_q = q − c`) and
    /// `g[i] = ±0.5` — through four algebraic steps:
    ///
    /// **Step 1 — Paper Equation 20** (unit vectors, ō[i] = ±1/√D):
    /// ```text
    /// ⟨x̄, q̄⟩ = (2Δ/√D)·⟨x_b, q_u⟩ + (2v_l/√D)·Σ x_b[i] - (Δ/√D)·Σ q_u[i] - √D·v_l
    /// ```
    ///
    /// **Step 2 — Our scaling** (residuals, g[i] = ±0.5):
    /// Replace 1/√D with 0.5 and √D with dim. We want `⟨g, r_q⟩` where
    /// `r_q[i] ≈ delta·q_u[i] + v_l`:
    /// ```text
    /// ⟨g, r_q⟩ = 0.5·(2·delta·⟨packed, q_u⟩ + 2·v_l·popcount(packed) - delta·Σ q_u[i] - dim·v_l)
    /// ```
    ///
    /// **Step 3 — Factor** (group delta terms and v_l terms):
    /// ```text
    /// ⟨g, r_q⟩ = 0.5·(delta·(2·⟨packed, q_u⟩ - Σ q_u[i]) + v_l·(2·popcount(packed) - dim))
    /// ```
    ///
    /// **Step 4 — Substitute** sign[i] = 2·bit[i] − 1:
    /// ```text
    /// signed_dot_qu = Σ sign[i]·q_u[i] = 2·⟨packed, q_u⟩ − Σ q_u[i]
    /// signed_sum    = Σ sign[i]        = 2·popcount(packed) − dim
    /// ⟨g, r_q⟩      = 0.5·(delta·signed_dot_qu + v_l·signed_sum)
    /// ```
    ///
    /// `signed_sum` is precomputed at index time and stored in the code header,
    /// so this function only needs to compute `signed_dot_qu` via the
    /// AND+popcount bit-plane expansion.
    ///
    /// # Notation
    ///
    /// - `v_l` = min(r_q[i]), `v_r` = max(r_q[i])
    /// - `delta` = (v_r − v_l) / (2^B_q − 1)
    /// - `packed` = 1-bit data code (sign bits), `g[i] = +0.5` when bit=1 else `−0.5`
    /// - `q_u` = quantized query, `r_q[i] ≈ delta·q_u[i] + v_l`
    /// - `q_u^(j)` = bit plane j of q_u (packed into dim/8 bytes)
    ///
    /// # Naive pseudocode
    ///
    /// The straightforward implementation iterates over each bit plane separately,
    /// re-reading `packed` once per plane:
    ///
    /// ```text
    /// packed_dot_qu = 0
    /// for j in 0..B_q:
    ///     plane_pop = 0
    ///     for i in 0..packed_len step 8:
    ///         packed_word = u64(packed[i..i+8])
    ///         q_word      = u64(q_u^(j)[i..i+8])
    ///         plane_pop  += popcount(packed_word AND q_word)
    ///     packed_dot_qu += plane_pop << j     // weight plane j by 2^j
    ///
    /// signed_sum    = self.signed_sum()   // precomputed: 2·popcount(packed) − dim
    /// signed_dot_qu = 2·packed_dot_qu − sum_q_u
    /// g_dot_r_q     = 0.5 · (delta · signed_dot_qu + v_l · signed_sum)
    /// ```
    ///
    /// # Optimized implementation
    ///
    /// The production code applies two optimizations over the naive approach:
    ///
    /// **[B1] Interleaved planes**: instead of looping over planes in the outer
    /// loop and re-reading `packed` once per plane (4x for `B_q=4`), we read
    /// each `packed` word exactly once, ANDing it with all four plane words in
    /// the same inner iteration. Four independent accumulators (`pop0..pop3`)
    /// are summed with their weights at the end:
    ///
    /// ```text
    /// for each 8-byte chunk (x, q0, q1, q2, q3):
    ///     x    = u64(packed chunk)
    ///     pop0 += popcount(x AND u64(q0 chunk))
    ///     pop1 += popcount(x AND u64(q1 chunk))
    ///     pop2 += popcount(x AND u64(q2 chunk))
    ///     pop3 += popcount(x AND u64(q3 chunk))
    ///
    /// packed_dot_qu = pop0 + (pop1 << 1) + (pop2 << 2) + (pop3 << 3)
    /// ```
    ///
    /// **[B2] `chunks_exact(8)` instead of `step_by(8)+index`**: exposes the
    /// stride as a type-level invariant, lets LLVM eliminate bounds checks and
    /// auto-vectorize the body to NEON/AVX-512.
    ///
    /// The `bit_planes` field is stored as a flat `Vec<u8>` (plane `j` at
    /// `[j*pb .. (j+1)*pb]`) rather than `Vec<Vec<u8>>`, so all four plane
    /// slices are contiguous and extracted with cheap slice indexing before
    /// the loop.
    pub fn distance_4bit_query(&self, distance_fn: &DistanceFunction, qq: &QuantizedQuery) -> f32 {
        let packed = self.packed();

        // Compute ⟨packed, q_u⟩ (the binary versions of g and r_q) via bit planes.
        // ⟨packed, q_u⟩ = Σ_j 2^j · popcount(packed AND q_u^(j))
        let pb = qq.padded_bytes;
        let packed_dot_qu: u32 = {
            let p0 = &qq.bit_planes[0 * pb..1 * pb];
            let p1 = &qq.bit_planes[1 * pb..2 * pb];
            let p2 = &qq.bit_planes[2 * pb..3 * pb];
            let p3 = &qq.bit_planes[3 * pb..4 * pb];
            let (mut pop0, mut pop1, mut pop2, mut pop3) = (0u32, 0u32, 0u32, 0u32);
            for (x_chunk, (((q0, q1), q2), q3)) in packed.chunks_exact(8).zip(
                p0.chunks_exact(8)
                    .zip(p1.chunks_exact(8))
                    .zip(p2.chunks_exact(8))
                    .zip(p3.chunks_exact(8)),
            ) {
                let x = u64::from_le_bytes(x_chunk.try_into().unwrap());
                pop0 += (x & u64::from_le_bytes(q0.try_into().unwrap())).count_ones();
                pop1 += (x & u64::from_le_bytes(q1.try_into().unwrap())).count_ones();
                pop2 += (x & u64::from_le_bytes(q2.try_into().unwrap())).count_ones();
                pop3 += (x & u64::from_le_bytes(q3.try_into().unwrap())).count_ones();
            }
            pop0 + (pop1 << 1) + (pop2 << 2) + (pop3 << 3)
        };

        // signed_sum = 2·popcount(packed) − dim, precomputed at index time
        let signed_sum = self.signed_sum() as f32;
        let signed_dot_qu = 2.0 * packed_dot_qu as f32 - qq.sum_q_u as f32;
        // ⟨g, r_q⟩ = 0.5·(delta·signed_dot_qu + v_l·signed_sum)
        let g_dot_r_q = 0.5 * (qq.delta * signed_dot_qu + qq.v_l * signed_sum);

        rabitq_distance_query(
            g_dot_r_q,
            self.correction(),
            self.norm(),
            self.radial(),
            qq.c_norm,
            qq.c_dot_q,
            qq.q_norm,
            distance_fn,
        )
    }
}

impl<T> Code<1, T> {
    /// Wraps existing bytes as a 1-bit code.
    pub fn new(bytes: T) -> Self {
        Self(bytes)
    }

    /// Padded byte length for a given dimension.
    pub fn packed_len(dim: usize) -> usize {
        padded_dim_1bit(dim) / 8
    }

    /// Total byte size of the code buffer for a given dimension.
    pub fn size(dim: usize) -> usize {
        size_of::<CodeHeader1>() + Self::packed_len(dim)
    }
}



impl Code<1, Vec<u8>> {
    const GRID_OFFSET: f32 = 0.5;

    /// Quantizes a data vector `d` relative to its cluster centroid `c` (1-bit path).
    ///
    /// # Paper equation (Section 3.1.3)
    ///
    /// RaBitQ represents a data vector `o` with a 1-bit code `x_b` and a
    /// quantized vector `ō`: `x_b[i] = 1 if o[i] >= 0`, `ō[i] = (2·x_b[i]−1)/√D`.
    ///
    /// The correction factor `⟨ō, o/‖o‖⟩` is stored so that the distance
    /// estimator can recover an unbiased estimate of the true distance.
    ///
    /// # Our derivation
    ///
    /// We quantize the residual `r = d − c` (the SPANN index stores
    /// embeddings relative to their cluster centroid). The grid point `g`
    /// uses ±0.5 scaling instead of the paper's ±1/√D (a fixed rescaling
    /// by √D/2 that cancels uniformly across the distance formula):
    ///
    /// ```text
    /// g[i] = +0.5   if r[i] >= 0   (bit = 1)
    ///        -0.5   if r[i] <  0   (bit = 0)
    ///     i.e. g[i] = (2·bit[i] − 1) · 0.5
    /// ```
    ///
    /// Because `g[i]` always has the **same sign** as `r[i]`, we can simplify
    /// each term of `⟨g, r⟩`:
    ///
    /// ```text
    /// g[i] · r[i] = |g[i]| · |r[i]| = 0.5 · |r[i]|
    /// ```
    ///
    /// so:
    ///
    /// ```text
    /// correction = ⟨g, n⟩ = ⟨g, r⟩ / ‖r‖
    ///           = (Σ g[i]·r[i]) / ‖r‖
    ///           = (Σ 0.5·|r[i]|) / ‖r‖
    ///           = 0.5 · Σ|r[i]| / ‖r‖
    ///           = GRID_OFFSET · sum_abs / norm
    /// ```
    ///
    /// We also precompute `signed_sum = 2·popcount(packed) − dim`, which equals
    /// `Σ (2·bit[i] − 1)` — the sum of the ±1 signs over all dimensions.
    /// This is used by `distance_4bit_query` to recover
    /// `⟨g, r_q⟩` from a bitwise AND+popcount without expanding `g` to floats.
    ///
    /// # Naive pseudocode
    ///
    /// The straightforward multi-pass implementation makes 5 passes over the data:
    ///
    /// ```text
    /// r        = embedding − centroid       // pass 1: 4 KB alloc + subtraction
    /// norm     = sqrt(dot(r, r))            // pass 2: simsimd dot
    /// radial   = dot(r, centroid)           // pass 3: simsimd dot
    /// abs_sum  = sum(|r[i]|)               // pass 4: abs + accumulate
    /// x_b      = pack_sign_bits(r)         // pass 5: sign extraction + byte pack
    /// popcount = popcount(x_b)
    ///
    /// correction = GRID_OFFSET * abs_sum / norm
    /// signed_sum = 2 * popcount - dim
    /// return CodeHeader1{correction, norm, radial, signed_sum} ++ x_b
    /// ```
    ///
    /// # Optimized implementation
    ///
    /// 1. Branchless sign extraction -- (val.to_bits() >> 31) ^ 1 reads the
    ///    IEEE 754 sign bit directly, avoiding a conditional branch per element.
    ///    For IEEE 754, the sign bit is bit 31: 0 for non-negative, 1 for negative.
    ///    XOR with 1 inverts it so that non-negative -> 1 (bit set) and negative -> 0.
    ///
    /// 2. Single allocation -- Output buffer allocated once; packed bytes written
    ///    directly into their final position, header filled last. No temporary Vec or memcpy.
    ///
    /// 3. Fuses all five passes into **one loop** over `(embedding, centroid)`,
    ///    processing 16 elements (2 output bytes) per outer iteration.
    ///    No intermediate `r` vector is allocated (saves 4 KB for dim=1024).
    ///
    /// Two key optimizations over a naive fused loop:
    ///
    /// 4. `chunks_exact(16)` — 16 f32s = 64 bytes = one AVX-512 cache line
    ///    read. Guaranteeing the exact chunk length to LLVM enables bounds-check
    ///    elimination and wider code generation (see note below).
    ///
    /// 5. Dual accumulators — each reduction (`abs_sum`, `norm_sq`,
    ///    `radial`) is split into two independent chains (e.g. elements
    ///    0..3 into `_a`, 4..7 into `_b`), breaking the FP dependency
    ///    chain and allowing OoO cores to pipeline the sequential additions.
    ///
    /// **Note**: `chunks_exact(16)` enables wider code gen: LLVM can statically
    /// prove 16 elements and emit 4x f32x4 NEON or 1x f32x16 AVX-512. Without
    /// chunks_exact, the loop body includes a length check that inhibits
    /// vectorisation. In benchmarks, chunks_exact(16) is 3.7x faster than
    /// `for (i in 0..chunk.len())` on M-series due to this.
    pub fn quantize(embedding: &[f32], centroid: &[f32]) -> Self {
        let dim = embedding.len();
        let header_len = std::mem::size_of::<CodeHeader1>();
        let mut bytes = vec![0u8; Self::size(dim)];
        let packed = &mut bytes[header_len..];

        // Dual accumulators: chain_a and chain_b are independent FP chains
        // that get merged at the end, halving the dependency depth.
        let mut sum_abs_a = 0.0f32;
        let mut sum_sq_a = 0.0f32;
        let mut dot_rc_a = 0.0f32;
        let mut sum_abs_b = 0.0f32;
        let mut sum_sq_b = 0.0f32;
        let mut dot_rc_b = 0.0f32;
        let mut ones = 0u32;

        // 16 elements = 64 bytes = one cache line = four NEON / one AVX-512 load.
        // 4 inner loops of 4: alternate between chains (a, b, a, b)
        // so each chain's additions are independent and the OoO core can pipeline them.
        for (out_pair, (emb_chunk, cen_chunk)) in packed
            .chunks_exact_mut(2)
            .zip(embedding.chunks_exact(16).zip(centroid.chunks_exact(16)))
        {
            let mut byte_lo = 0u8;
            let mut byte_hi = 0u8;

            // Dependency structure:
            //   Loops 1 and 2 can overlap (different accumulator chains).
            //   Loop 3 depends on loop 1 (chain_a). Loop 4 depends on loop 2 (chain_b).
            //   Within each loop, bit-packing and residual computation are independent
            //   across iterations, but each accumulator is a 4-deep chain
            //   (FP addition is not associative, so the compiler preserves eval order).
            for j in 0..4 {
                let residual = emb_chunk[j] - cen_chunk[j];
                byte_lo |= ((residual.to_bits() >> 31) as u8 ^ 1) << j;
                sum_abs_a += residual.abs();
                sum_sq_a += residual * residual;
                dot_rc_a += residual * cen_chunk[j];
            }
            for j in 4..8 {
                let residual = emb_chunk[j] - cen_chunk[j];
                byte_lo |= ((residual.to_bits() >> 31) as u8 ^ 1) << j;
                sum_abs_b += residual.abs();
                sum_sq_b += residual * residual;
                dot_rc_b += residual * cen_chunk[j];
            }
            for j in 0..4 {
                let residual = emb_chunk[j + 8] - cen_chunk[j + 8];
                byte_hi |= ((residual.to_bits() >> 31) as u8 ^ 1) << j;
                sum_abs_a += residual.abs();
                sum_sq_a += residual * residual;
                dot_rc_a += residual * cen_chunk[j + 8];
            }
            for j in 4..8 {
                let residual = emb_chunk[j + 8] - cen_chunk[j + 8];
                byte_hi |= ((residual.to_bits() >> 31) as u8 ^ 1) << j;
                sum_abs_b += residual.abs();
                sum_sq_b += residual * residual;
                dot_rc_b += residual * cen_chunk[j + 8];
            }
            ones += byte_lo.count_ones() + byte_hi.count_ones();
            out_pair[0] = byte_lo;
            out_pair[1] = byte_hi;
        }

        // Remainder: handle dims that aren't multiples of 16 but are multiples of 8,
        // plus any sub-8 tail.
        let processed = (dim / 16) * 16;
        let tail_emb = &embedding[processed..];
        let tail_cen = &centroid[processed..];
        let tail_packed = &mut packed[(processed / 8)..];
        for (byte_ref, (emb_tail, cen_tail)) in tail_packed
            .iter_mut()
            .zip(tail_emb.chunks(8).zip(tail_cen.chunks(8)))
        {
            let mut byte = 0u8;
            for (j, (&e, &c)) in emb_tail.iter().zip(cen_tail).enumerate() {
                let residual = e - c;
                byte |= ((residual.to_bits() >> 31) as u8 ^ 1) << j;
                sum_abs_b += residual.abs();
                sum_sq_b += residual * residual;
                dot_rc_b += residual * c;
            }
            ones += byte.count_ones();
            *byte_ref = byte;
        }

        let sum_abs = sum_abs_a + sum_abs_b;
        let norm = (sum_sq_a + sum_sq_b).sqrt();
        let radial = dot_rc_a + dot_rc_b;
        let correction = if dim == 0 || norm < f32::EPSILON {
            1.0
        } else {
            Self::GRID_OFFSET * sum_abs / norm
        };
        let signed_sum = 2 * ones as i32 - dim as i32;

        bytes[..header_len].copy_from_slice(bytemuck::bytes_of(&CodeHeader1 {
            correction,
            norm,
            radial,
            signed_sum,
        }));
        Self(bytes)
    }
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Padded dimension for 1-bit codes (multiple of 64 for u64 popcount alignment).
fn padded_dim_1bit(dim: usize) -> usize {
    dim.div_ceil(64) * 64
}

/// Computes hamming distance between two packed bit vectors.
///
/// Both slices must have the same length and that length must be a multiple of
/// 8 (guaranteed when `padded_dim` is a multiple of 64).
///
/// Uses `simsimd::BinarySimilarity::hamming` which dispatches at runtime to:
///   - AVX-512 VPOPCNTDQ on x86_64 (8 × u64 lanes per instruction)
///   - NEON CNT on ARM (byte-level popcount, vectorised over 16 bytes)
///
/// Falls back to scalar u64 XOR + POPCNT if simsimd returns None (e.g. on
/// unsupported targets or in tests without the CPU feature).
fn hamming_distance(a: &[u8], b: &[u8]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len() % 8, 0);
    if let Some(bits) = <u8 as BinarySimilarity>::hamming(a, b) {
        bits as u32
    } else {
        a.chunks_exact(8)
            .zip(b.chunks_exact(8))
            .map(|(lhs, rhs)| {
                let lhs = u64::from_le_bytes(lhs.try_into().unwrap());
                let rhs = u64::from_le_bytes(rhs.try_into().unwrap());
                (lhs ^ rhs).count_ones()
            })
            .sum()
    }
}

/// Precomputed sign expansion: for each byte value 0..256, the 8 f32 signs
/// (+1.0 or −1.0) for bits 0..7. Bit j = 1 → +1.0, bit j = 0 → −1.0.
const fn sign_table_entry(byte: u8) -> [f32; 8] {
    let b = byte as u32;
    [
        f32::from_bits(0x3F800000 | (((b >> 0) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((b >> 1) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((b >> 2) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((b >> 3) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((b >> 4) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((b >> 5) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((b >> 6) & 1) ^ 1) << 31),
        f32::from_bits(0x3F800000 | (((b >> 7) & 1) ^ 1) << 31),
    ]
}

static SIGN_TABLE: [[f32; 8]; 256] = {
    let mut table = [[0.0f32; 8]; 256];
    let mut i = 0u8;
    while i < 255 {
        table[i as usize] = sign_table_entry(i);
        i += 1;
    }
    table[255] = sign_table_entry(255);
    table
};

/// Computes `Σ sign[i] · values[i]` where sign[i] = +1.0 if bit i is set
/// in `packed`, −1.0 otherwise.
///
/// This is the hot kernel for the 1-bit `distance_query` path.  The caller
/// multiplies the result by 0.5 to recover `⟨g, r_q⟩`.
///
/// # SIMD strategy
///
/// **Step 1 — sign expansion (lookup table).**
/// A precomputed `SIGN_TABLE[256][8]` maps each byte to its 8 f32 signs
/// (+1.0 or −1.0). One table lookup + `copy_from_slice` replaces 8
/// `f32::from_bits` calls per byte. The 8 KB table stays in L1.
///
/// **Step 2 — dot product (simsimd).**
/// The sign array and the value chunk are passed to `f32::dot`, which
/// dispatches to the platform's best FMA kernel (AVX2, AVX-512, etc.).
fn signed_dot(packed: &[u8], values: &[f32]) -> f32 {
    let mut signs = [0.0f32; 64];
    let mut sum = 0.0f32;
    // 64 bits of packed per outer loop iteration.
    for (packed_chunk, val_chunk) in packed.chunks(8).zip(values.chunks(64)) {
        let n = val_chunk.len();
        // 8 bits of packed per inner loop iteration.
        for (i, &byte) in packed_chunk.iter().enumerate() {
            signs[i * 8..(i + 1) * 8].copy_from_slice(&SIGN_TABLE[byte as usize]);
        }
        sum += f32::dot(&signs[..n], val_chunk).unwrap_or(0.0) as f32;
    }
    sum
}

// ── Bitwise distance estimation (paper Section 3.3) ──────────────────────────
//
// The paper's efficient estimator quantizes the query residual r_q into B_q-bit
// unsigned integers, then computes ⟨packed, q_u⟩ using B_q rounds of
// bitwise AND + popcount on dim-bit strings. This eliminates all float
// arithmetic from the per-code inner product.
//
// Notation mapping (paper -> our code):
//   o, q       -> n (normalized residual), r_q/‖r_q‖
//   x_bar_b    -> self.packed() (the stored dim-bit quantization code)
//   q'         -> r_q (already P^-1-rotated before reaching us)
//   q_bar_u    -> quantized query (computed once per cluster scan)
//   ⟨o_bar, o⟩ -> correction (= ⟨g, n⟩, stored in the header)
//   ⟨o_bar, q⟩ -> g_dot_r_q (what we estimate per code)

/// Pre-computed query quantization for the bitwise distance path.
///
/// Computed once per query-cluster pair and reused across all codes in the
/// cluster. For BITS=1 with B_q=4, this stores:
///   - 4 bit planes of the quantized query (`r_q`) in a flat contiguous buffer
///   - `v_l`, `delta`, `sum_q_u`: scalar factors for Equation 20
pub struct QuantizedQuery {
    /// Flat bit-plane buffer: plane j occupies bytes [j*padded_bytes .. (j+1)*padded_bytes].
    /// bit_planes[j*padded_bytes + i] holds the j-th bit of q_u[i*8 .. i*8+8], packed LSB-first.
    /// One contiguous allocation replaces the prior b_q separate Vec<u8> allocations.
    pub bit_planes: Vec<u8>,
    /// Byte length of one bit plane (= packed data code length = ceil(dim/64)*8).
    pub padded_bytes: usize,
    /// Lower bound of query values: v_l = min(r_q[i])
    pub v_l: f32,
    /// Quantization step size: delta = (v_r - v_l) / (2^B_q - 1)
    pub delta: f32,
    /// Sum of quantized query values: Σ q_u[i]
    pub sum_q_u: u32,
    /// Precomputed query-level scalars
    pub c_norm: f32,
    pub c_dot_q: f32,
    pub q_norm: f32,
}

impl QuantizedQuery {
    /// Quantize a query residual `r_q` into B_q-bit unsigned integers and
    /// decompose into bit planes for AND+popcount inner products.
    ///
    /// `padded_bytes` is the byte length of the packed data codes (for alignment).
    pub fn new(
        r_q: &[f32],
        padded_bytes: usize,
        c_norm: f32,
        c_dot_q: f32,
        q_norm: f32,
    ) -> Self {
        let max_val = (1u32 << B_Q) - 1;

        // Two separate folds — each auto-vectorises to a SIMD reduction
        // (FMINV/FMAXV on ARM NEON, VMINPS horizontal on x86).
        // A combined tuple fold `(min, max)` breaks this vectorisation (scalar
        // pair dependency), measured 3.77× slower on Apple M-series.
        let v_l = r_q.iter().copied().fold(f32::INFINITY, f32::min);
        let v_r = r_q.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let range = v_r - v_l;
        let delta = if range > f32::EPSILON {
            range / max_val as f32
        } else {
            1.0
        };

        // Single fused pass: quantize each element, accumulate sum, and scatter
        // bits into a flat bit-plane buffer via chunks_exact(8). The exact-chunk
        // guarantee lets LLVM eliminate bounds checks and generate tighter code
        // (44% faster than chunks(8) on Apple M-series).
        //
        // Layout: plane j occupies bit_planes[j*padded_bytes .. (j+1)*padded_bytes].
        let inv_delta = 1.0 / delta;
        let mut bit_planes = vec![0u8; B_Q as usize * padded_bytes];
        let mut sum_q_u = 0u32;
        for (byte_idx, chunk) in r_q.chunks_exact(8).enumerate() {
            let (mut b0, mut b1, mut b2, mut b3) = (0u8, 0u8, 0u8, 0u8);
            for (bit, &v) in chunk.iter().enumerate() {
                let qu = (((v - v_l) * inv_delta).round() as u32).min(max_val);
                sum_q_u += qu;
                b0 |= (((qu >> 0) & 1) as u8) << bit;
                b1 |= (((qu >> 1) & 1) as u8) << bit;
                b2 |= (((qu >> 2) & 1) as u8) << bit;
                b3 |= (((qu >> 3) & 1) as u8) << bit;
            }
            bit_planes[0 * padded_bytes + byte_idx] = b0;
            bit_planes[1 * padded_bytes + byte_idx] = b1;
            bit_planes[2 * padded_bytes + byte_idx] = b2;
            bit_planes[3 * padded_bytes + byte_idx] = b3;
        }
        // Handle remainder for dim not divisible by 8.
        let rem = r_q.chunks_exact(8).remainder();
        if !rem.is_empty() {
            let byte_idx = r_q.len() / 8;
            let (mut b0, mut b1, mut b2, mut b3) = (0u8, 0u8, 0u8, 0u8);
            for (bit, &v) in rem.iter().enumerate() {
                let qu = (((v - v_l) * inv_delta).round() as u32).min(max_val);
                sum_q_u += qu;
                b0 |= (((qu >> 0) & 1) as u8) << bit;
                b1 |= (((qu >> 1) & 1) as u8) << bit;
                b2 |= (((qu >> 2) & 1) as u8) << bit;
                b3 |= (((qu >> 3) & 1) as u8) << bit;
            }
            bit_planes[0 * padded_bytes + byte_idx] = b0;
            bit_planes[1 * padded_bytes + byte_idx] = b1;
            bit_planes[2 * padded_bytes + byte_idx] = b2;
            bit_planes[3 * padded_bytes + byte_idx] = b3;
        }

        Self {
            bit_planes,
            padded_bytes,
            v_l,
            delta,
            sum_q_u,
            c_norm,
            c_dot_q,
            q_norm,
        }
    }
}

/// Pre-computed lookup tables for batch distance estimation (paper Section 3.3.2).
///
/// Intuition:
/// - BatchQueryLuts precomputes all possible _partial_ inner products and saves
///   them in lookup tables:
/// - For each group of 4 dimensions, a 16-entry table gives the partial
///   `⟨packed, q_u⟩` for every possible 4-bit chunk (nibble) of the data code
/// - At query time you only do nibble extraction and table lookups.
/// - Results: Large table (8 KB for dim=1024), but less compute per code.
///
/// Specifically:
/// Splits the packed data code into dim/4 nibbles. For each nibble position,
/// precomputes a 16-entry LUT: the partial inner product between the nibble
/// of `packed` and the corresponding 4 elements of the quantized query.
///
/// At scan time, each code's distance requires dim/4 LUT lookups + accumulation
/// (no float expansion, no AND+popcount).
///
/// Why `distance_4bit_query` beats `BatchQueryLuts::distance_query`:
// The working set sizes explain the gap:
//   - Bitwise: 4 bit planes x 128 bytes = 512 bytes of query data (fits in L1),
//     plus 128 bytes per code. The inner loop is 4 rounds of 16 AND+popcount
//     operations on u64 words -- 64 iterations of 3-instruction sequences.
//   - LUT: 256 nibble positions x 32 bytes per LUT entry = 8 KB of LUT data,
//     plus 128 bytes per code. The inner loop is 256 iterations of nibble extraction
//     + array indexing + accumulation -- more iterations, more cache pressure, and
//     indirect addressing (table lookup) prevents pipelining.
// The bitwise approach reads less data, does fewer iterations, and each iteration
// is a simpler instruction sequence (AND, POPCNT, ADD) that modern CPUs pipeline
// perfectly.
pub struct BatchQueryLuts {
    /// luts[nibble_idx][nibble_value] = partial `⟨packed, q_u⟩` contribution.
    /// nibble_idx ranges over 0..dim/4 (padded to byte boundary).
    pub luts: Vec<[u16; 16]>,
    pub v_l: f32,
    pub delta: f32,
    pub sum_q_u: u32,
    pub c_norm: f32,
    pub c_dot_q: f32,
    pub q_norm: f32,
    pub dim: usize,
}

impl BatchQueryLuts {
    /// Build Lookup Tables (LUTs) from a query residual `r_q` for 1-bit codes.
    ///
    /// Each nibble of the packed data code covers 4 bits (i.e., 4 dimensions).
    /// For each of the 16 possible nibble values, we precompute the partial
    /// sum of q_u[i] for the bits that are set.
    pub fn new(r_q: &[f32], c_norm: f32, c_dot_q: f32, q_norm: f32) -> Self {
        let dim = r_q.len();
        let max_val = 15u32; // B_q = 4

        // Quantize r_q.
        let v_l = r_q.iter().copied().fold(f32::INFINITY, f32::min);
        let v_r = r_q.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let range = v_r - v_l;
        let delta = if range > f32::EPSILON {
            range / max_val as f32
        } else {
            1.0
        };
        let q_u: Vec<u32> = r_q
            .iter()
            .map(|&v| (((v - v_l) / delta).round() as u32).min(max_val))
            .collect();

        // Sum of quantized query values (Σ q_u[i]) for future distance computation.
        let sum_q_u: u32 = q_u.iter().sum();

        // Number of nibbles (each nibble = 4 bits = 4 dimensions).
        let padded_dim = (dim + 63) / 64 * 64;
        let n_nibbles = padded_dim / 4;

        let mut luts = vec![[0u16; 16]; n_nibbles];

        for (nib_idx, lut) in luts.iter_mut().enumerate() {
            let base = nib_idx * 4;
            // For each of the 16 possible nibble values, sum q_u for set bits.
            for nibble_val in 0u8..16 {
                let mut partial = 0u32;
                for bit in 0..4 {
                    if (nibble_val >> bit) & 1 == 1 {
                        let elem_idx = base + bit;
                        if elem_idx < dim {
                            partial += q_u[elem_idx];
                        }
                    }
                }
                lut[nibble_val as usize] = partial as u16;
            }
        }

        Self {
            luts,
            v_l,
            delta,
            sum_q_u,
            c_norm,
            c_dot_q,
            q_norm,
            dim,
        }
    }

    /// Score a single 1-bit code using the precomputed LUTs.
    ///
    /// For each nibble of the packed data code, look up the partial inner
    /// product from the LUT and accumulate.  Then recover the full distance.
    pub fn distance_query(&self, code: &Code<1, &[u8]>, distance_fn: &DistanceFunction) -> f32 {
        let packed = code.packed();

        // ⟨packed, q_u⟩ via LUT: iterate over nibbles of packed data.
        let mut packed_dot_qu = 0u32;
        for (nib_idx, lut) in self.luts.iter().enumerate() {
            let byte_idx = nib_idx / 2;
            let byte = if byte_idx < packed.len() {
                packed[byte_idx]
            } else {
                0
            };
            let nibble = if nib_idx % 2 == 0 {
                byte & 0x0F
            } else {
                byte >> 4
            };
            packed_dot_qu += lut[nibble as usize] as u32;
        }

        let signed_dot_qu = 2.0 * packed_dot_qu as f32 - self.sum_q_u as f32;
        let signed_sum = code.signed_sum() as f32;
        let g_dot_r_q = 0.5 * (self.delta * signed_dot_qu + self.v_l * signed_sum);

        rabitq_distance_query(
            g_dot_r_q,
            code.correction(),
            code.norm(),
            code.radial(),
            self.c_norm,
            self.c_dot_q,
            self.q_norm,
            distance_fn,
        )
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use simsimd::SpatialSimilarity;

    use super::*;
    use crate::quantization::Code;

    #[test]
    fn test_1bit_attributes() {
        let embedding = (0..300).map(|i| i as f32).collect::<Vec<_>>();
        let centroid = (0..300).map(|i| i as f32 * 0.5).collect::<Vec<_>>();

        let code = Code::<1>::quantize(&embedding, &centroid);

        // Verify accessors return finite values
        assert!(code.correction().is_finite());
        assert!(code.norm().is_finite());
        assert!(code.radial().is_finite());

        // Verify norm is ‖r‖
        let r = embedding
            .iter()
            .zip(&centroid)
            .map(|(e, c)| e - c)
            .collect::<Vec<_>>();
        let expected_norm = (f32::dot(&r, &r).unwrap_or(0.0) as f32).sqrt();
        assert!(
            (code.norm() - expected_norm).abs() / expected_norm < 1e-6,
            "norm: got {}, expected {}",
            code.norm(),
            expected_norm
        );

        // Verify radial is ⟨r, c⟩
        let expected_radial = f32::dot(&r, &centroid).unwrap_or(0.0) as f32;
        assert!(
            (code.radial() - expected_radial).abs() / expected_radial.abs().max(1.0) < 1e-6,
            "radial: got {}, expected {}",
            code.radial(),
            expected_radial
        );

        // Verify correction = 0.5 * Σ|r[i]| / ‖r‖
        let abs_sum: f32 = r.iter().map(|x| x.abs()).sum();
        let expected_correction = 0.5 * abs_sum / expected_norm;
        assert!(
            (code.correction() - expected_correction).abs() < 1e-5,
            "correction: got {}, expected {}",
            code.correction(),
            expected_correction
        );

        // Verify buffer size
        assert_eq!(code.as_ref().len(), Code::<1>::size(embedding.len()));
    }

    #[test]
    fn test_1bit_size() {
        // 64-aligned (256 dims)
        assert_eq!(Code::<1>::packed_len(256), 256 / 8); // 32 bytes
        assert_eq!(Code::<1>::size(256), 16 + 32); // CodeHeader1 (16 bytes) + packed

        // Non-aligned (300) - should pad to 320 (5 * 64)
        assert_eq!(Code::<1>::packed_len(300), 320 / 8); // 40 bytes
        assert_eq!(Code::<1>::size(300), 16 + 40);

        // 1024 dims
        assert_eq!(Code::<1>::packed_len(1024), 128);
        assert_eq!(Code::<1>::size(1024), 16 + 128);

        // 4096 dims
        assert_eq!(Code::<1>::packed_len(4096), 512);
        assert_eq!(Code::<1>::size(4096), 16 + 512);
    }

    #[test]
    fn test_1bit_zero_residual() {
        let embedding = (0..300).map(|i| i as f32).collect::<Vec<_>>();

        // Exactly zero residual
        let code = Code::<1>::quantize(&embedding, &embedding);
        assert_eq!(code.correction(), 1.0);
        assert!(code.norm() < f32::EPSILON);

        // Near-zero residual
        let centroid = embedding.iter().map(|x| x + 1e-10).collect::<Vec<_>>();
        let code = Code::<1>::quantize(&embedding, &centroid);
        assert_eq!(code.correction(), 1.0);
        assert!(code.norm() < f32::EPSILON);
    }

    /// Reads bit `i` from packed 1-bit codes and returns the grid value (±0.5).
    fn read_1bit_grid(code: &Code<1>, dim: usize) -> Vec<f32> {
        let packed = code.packed();
        (0..dim)
            .map(|i| {
                let bit = (packed[i / 8] >> (i % 8)) & 1;
                bit as f32 - 0.5
            })
            .collect()
    }

    /// Verify each bit matches the sign of the residual.
    #[test]
    fn test_1bit_quantize_signs() {
        let embedding = vec![3.0, -1.0, 0.5, -2.0, 0.0, 1.0, -0.1, 0.1];
        let centroid = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        // residual: [2.0, -2.0, -0.5, -3.0, -1.0, 0.0, -1.1, -0.9]
        // expected bits: [1, 0, 0, 0, 0, 1, 0, 0] (bit 5 is 1 because r=0.0 >= 0)

        let code = Code::<1>::quantize(&embedding, &centroid);
        let grid = read_1bit_grid(&code, 8);

        let r: Vec<f32> = embedding
            .iter()
            .zip(&centroid)
            .map(|(e, c)| e - c)
            .collect();
        for i in 0..8 {
            let expected_sign = if r[i] >= 0.0 { 0.5 } else { -0.5 };
            assert_eq!(
                grid[i], expected_sign,
                "dim {}: r={}, grid={}, expected={}",
                i, r[i], grid[i], expected_sign
            );
        }
    }

    /// Spot-check that original and new quantize agree on the per-element
    /// sign bit before packing.  Both reduce to code[i] = 1 if r[i] >= 0,
    /// 0 otherwise for BITS=1; the difference is only in how they pack those
    /// bits into bytes (BitPacker8x vs LSB-first).
    #[test]
    fn test_quantize_lyon_matches_quantize() {
        let mut rng = StdRng::seed_from_u64(42);
        for &dim in &[64, 300, 1024] {
            for _ in 0..10 {
                let embedding: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
                let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
                let r: Vec<f32> = embedding
                    .iter()
                    .zip(&centroid)
                    .map(|(e, c)| e - c)
                    .collect();

                // quantize_lyon logic: sign bit = (IEEE sign bit) XOR 1
                let signs_lyon: Vec<u8> =
                    r.iter().map(|&v| (v.to_bits() >> 31) as u8 ^ 1).collect();

                // quantize logic: for BITS=1, CEIL=1, ray-walk collapses to
                //   code[i] = 1 if r[i] >= 0, else 0
                let signs_quantize: Vec<u8> =
                    r.iter().map(|&v| if v >= 0.0 { 1 } else { 0 }).collect();

                assert_eq!(signs_lyon, signs_quantize, "sign mismatch at dim={dim}");
            }
        }
    }

    /// Tests that 1-bit grid points quantize exactly using distance_query.
    #[test]
    fn test_1bit_grid_points() {
        let centroid = vec![0.0; 8];
        let c_norm = 0.0;

        // 2 grid values for BITS=1: -0.5, +0.5
        let grid: Vec<f32> = vec![-0.5, 0.5];

        // Test all 2^8=256 combinations for 8 dimensions
        for bits in 0u8..=255 {
            let embedding: Vec<f32> = (0..8).map(|i| grid[((bits >> i) & 1) as usize]).collect();
            let embedding_norm = (f32::dot(&embedding, &embedding).unwrap_or(0.0) as f32).sqrt();

            if embedding_norm < f32::EPSILON {
                continue;
            }

            let code = Code::<1>::quantize(&embedding, &centroid);
            let dist = code.distance_query(
                &DistanceFunction::Cosine,
                &embedding,
                c_norm,
                0.0,
                embedding_norm,
            );
            assert!(
                dist.abs() < 4.0 * f32::EPSILON,
                "1-bit grid {:08b} should have zero cosine self-distance, got {}",
                bits,
                dist
            );
        }
    }

    #[test]
    fn test_hamming_distance() {
        // Identical → hamming = 0
        let a = vec![0xFF, 0x00, 0xAA, 0x55, 0xFF, 0x00, 0xAA, 0x55];
        assert_eq!(hamming_distance(&a, &a), 0);

        // All different → hamming = 64 (8 bytes * 8 bits)
        let b = vec![0x00, 0xFF, 0x55, 0xAA, 0x00, 0xFF, 0x55, 0xAA];
        assert_eq!(hamming_distance(&a, &b), 64);

        // One bit different
        let mut c = a.clone();
        c[0] = 0xFE; // flip bit 0
        assert_eq!(hamming_distance(&a, &c), 1);
    }

    /// Validates that distance_query_bitwise and BatchQueryLuts produce results
    /// close to the float-based distance_query (within query quantization error).
    #[test]
    fn test_bitwise_distance_matches_float() {
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 1024;
        let centroid: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
        let c_norm = (f32::dot(&centroid, &centroid).unwrap_or(0.0) as f32).sqrt();

        let query: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
        let r_q: Vec<f32> = query.iter().zip(&centroid).map(|(q, c)| q - c).collect();
        let c_dot_q = f32::dot(&centroid, &query).unwrap_or(0.0) as f32;
        let q_norm = (f32::dot(&query, &query).unwrap_or(0.0) as f32).sqrt();

        let padded_bytes = Code::<1>::packed_len(dim);
        let qq = QuantizedQuery::new(&r_q, padded_bytes, c_norm, c_dot_q, q_norm);
        let luts = BatchQueryLuts::new(&r_q, c_norm, c_dot_q, q_norm);
        let df = DistanceFunction::Euclidean;

        for _ in 0..100 {
            let emb: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
            let code_owned = Code::<1>::quantize(&emb, &centroid);
            let code = Code::<1, _>::new(code_owned.as_ref());

            let float_dist = code.distance_query(&df, &r_q, c_norm, c_dot_q, q_norm);
            let bitwise_dist = code.distance_4bit_query(&df, &qq);
            let lut_dist = luts.distance_query(&code, &df);

            let tol = float_dist.abs() * 0.05 + 1.0;
            assert!(
                (float_dist - bitwise_dist).abs() < tol,
                "bitwise mismatch: float={float_dist}, bitwise={bitwise_dist}"
            );
            assert!(
                (float_dist - lut_dist).abs() < tol,
                "lut mismatch: float={float_dist}, lut={lut_dist}"
            );
            // bitwise and lut should agree exactly (same quantization)
            assert!(
                (bitwise_dist - lut_dist).abs() < f32::EPSILON * 100.0,
                "bitwise vs lut: bitwise={bitwise_dist}, lut={lut_dist}"
            );
        }
    }

    /// BITS=1: P95 relative error bound 8.0%, observed ~5% (code), ~3.5% (query)
    #[test]
    fn test_error_bound_bits_1() {
        for k in [1.0f32, 2.0, 4.0] {
            assert_error_bound_1bit(1024, k, 128);
        }
    }

    fn assert_error_bound_1bit(dim: usize, k: f32, n_vectors: usize) {
        let mut rng = StdRng::seed_from_u64(42);
        let centroid = (0..dim).map(|_| rng.gen_range(-k..k)).collect::<Vec<_>>();
        let c_norm = (f32::dot(&centroid, &centroid).unwrap_or(0.0) as f32).sqrt();
        let vectors = (0..n_vectors)
            .map(|_| {
                centroid
                    .iter()
                    .map(|c| c + rng.gen_range(-k..k))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let codes = vectors
            .iter()
            .map(|v| Code::<1>::quantize(v, &centroid))
            .collect::<Vec<_>>();

        let max_p95_rel_error = 0.16 / 2.0;
        let n_vectors = vectors.len();

        for distance_fn in [
            DistanceFunction::Cosine,
            DistanceFunction::Euclidean,
            DistanceFunction::InnerProduct,
        ] {
            let mut rel_errors_code = Vec::new();
            let mut rel_errors_query = Vec::new();

            for i in 0..n_vectors {
                for j in (i + 1)..n_vectors {
                    let exact = match distance_fn {
                        DistanceFunction::Cosine => {
                            SpatialSimilarity::cos(&vectors[i], &vectors[j]).unwrap_or(0.0) as f32
                        }
                        DistanceFunction::Euclidean => {
                            SpatialSimilarity::l2sq(&vectors[i], &vectors[j]).unwrap_or(0.0) as f32
                        }
                        DistanceFunction::InnerProduct => {
                            1.0 - SpatialSimilarity::dot(&vectors[i], &vectors[j]).unwrap_or(0.0)
                                as f32
                        }
                    };

                    let estimated_code =
                        codes[i].distance_code(&codes[j], &distance_fn, c_norm, dim);
                    rel_errors_code
                        .push((exact - estimated_code).abs() / exact.abs().max(f32::EPSILON));

                    let q = &vectors[j];
                    let q_norm = (f32::dot(q, q).unwrap_or(0.0) as f32).sqrt();
                    let c_dot_q = f32::dot(&centroid, q).unwrap_or(0.0) as f32;
                    let r_q: Vec<f32> = centroid.iter().zip(q).map(|(c, q)| q - c).collect();
                    let estimated_query =
                        codes[i].distance_query(&distance_fn, &r_q, c_norm, c_dot_q, q_norm);
                    rel_errors_query
                        .push((exact - estimated_query).abs() / exact.abs().max(f32::EPSILON));
                }
            }

            rel_errors_code.sort_by(|a, b| a.total_cmp(b));
            rel_errors_query.sort_by(|a, b| a.total_cmp(b));
            let p95_code = rel_errors_code[rel_errors_code.len() * 95 / 100];
            let p95_query = rel_errors_query[rel_errors_query.len() * 95 / 100];

            assert!(
                p95_code < max_p95_rel_error,
                "{:?}: distance_code P95 rel error {:.4} exceeds bound {:.4}",
                distance_fn,
                p95_code,
                max_p95_rel_error
            );
            assert!(
                p95_query < max_p95_rel_error,
                "{:?}: distance_query P95 rel error {:.4} exceeds bound {:.4}",
                distance_fn,
                p95_query,
                max_p95_rel_error
            );
        }
    }
}
