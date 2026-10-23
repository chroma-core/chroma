//! Score four-bit centroids against full-precision queries with a prepared layout.
//!
//! The SIMD loops port `ip16_fxu4_avx2` and `ip16_fxu4_avx512` from
//! VectorDB-NTU/RaBitQ-Library, commit 2f6b57e4907659a79fd039cc826164e7d320f83d.
//! Copyright VectorDB-NTU contributors. Licensed under Apache-2.0; see
//! licenses/RaBitQ-Library-LICENSE. The centroid layout is converted once:
//! each eight-byte block holds sixteen coordinates in low and high nibbles.

use super::multi_bit::CodeHeader4Bit;
use super::{rabitq_distance_query, Code};
use bitpacking::{BitPacker, BitPacker8x};
use chroma_distance::DistanceFunction;
use simsimd::SpatialSimilarity;
use std::borrow::Cow;

/// A persisted scoring code has an invalid dimension, length, or correction.
#[derive(Debug, thiserror::Error)]
#[error("invalid packed four-bit centroid: {0}")]
pub struct PackedCentroidError(&'static str);

impl chroma_error::ChromaError for PackedCentroidError {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::DataLoss
    }
}

/// A centroid's four-bit code arranged for repeated SIMD distance estimates.
pub struct PackedFourBitCentroid {
    header: CodeHeader4Bit,
    packed: Vec<u8>,
    dim: usize,
}

impl PackedFourBitCentroid {
    /// Prepare a code quantized relative to the zero vector for scoring.
    /// `dim` must be the original embedding dimension used to create `code`.
    /// Codes relative to a nonzero cluster centroid require additional distance
    /// corrections and cannot be scored by this origin-only representation.
    pub fn from_origin_code(code: &Code<4, impl AsRef<[u8]>>, dim: usize) -> Self {
        let mut values = vec![0u32; dim.div_ceil(256) * 256];
        let packer = BitPacker8x::new();
        for (block, values) in values.chunks_mut(256).enumerate() {
            packer.decompress(&code.packed()[block * 128..], values, 4);
        }
        let mut packed = vec![0; dim.div_ceil(16) * 8];
        for (i, &value) in values[..dim].iter().enumerate() {
            packed[i / 16 * 8 + i % 8] |= (value as u8) << (i % 16 / 8 * 4);
        }
        Self {
            header: code.header(),
            packed,
            dim,
        }
    }

    /// Store the dimension, three correction floats, and SIMD nibble layout.
    /// Integers and floats use little-endian bytes; this is not `Code<4>`'s
    /// bitpacker layout. The checkpoint format must identify this representation.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16 + self.packed.len());
        bytes.extend_from_slice(&(self.dim as u32).to_le_bytes());
        for value in [self.header.correction, self.header.norm, self.header.radial] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes.extend_from_slice(&self.packed);
        bytes
    }

    /// Load a ready-to-score origin code without quantizing or unpacking it.
    pub fn from_bytes(bytes: &[u8], dim: usize) -> Result<Self, PackedCentroidError> {
        if dim == 0 || bytes.len() < 16 {
            return Err(PackedCentroidError("dimensions or header are missing"));
        }
        let stored_dim = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        if stored_dim != dim {
            return Err(PackedCentroidError("dimensions differ from the checkpoint"));
        }
        let size = dim
            .div_ceil(16)
            .checked_mul(8)
            .and_then(|n| n.checked_add(16));
        if size != Some(bytes.len()) {
            return Err(PackedCentroidError(
                "code length differs from its dimensions",
            ));
        }
        let read_float = |offset| f32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let header = CodeHeader4Bit {
            correction: read_float(4),
            norm: read_float(8),
            radial: read_float(12),
        };
        if !header.correction.is_finite()
            || header.correction <= 0.0
            || !header.norm.is_finite()
            || header.norm < 0.0
            || header.radial != 0.0
        {
            return Err(PackedCentroidError("invalid origin correction values"));
        }
        Ok(Self {
            header,
            packed: bytes[16..].to_vec(),
            dim,
        })
    }

    /// Numeric payload, excluding the vector's allocation bookkeeping.
    pub fn payload_bytes(&self) -> usize {
        12 + self.packed.len()
    }
}

/// Sibling centroid codes in one allocation, in the caller's child order.
/// Headers are also contiguous. Construction consumes the individual codes so
/// their separate allocations can be released after reader preparation.
pub struct PackedFourBitCentroidBatch {
    headers: Vec<CodeHeader4Bit>,
    packed: Vec<u8>,
    dim: usize,
}

impl PackedFourBitCentroidBatch {
    pub fn new(centroids: Vec<PackedFourBitCentroid>, dim: usize) -> Self {
        let stride = dim.div_ceil(16) * 8;
        let mut headers = Vec::with_capacity(centroids.len());
        let mut packed = Vec::with_capacity(centroids.len() * stride);
        for centroid in centroids {
            assert_eq!(centroid.dim, dim);
            headers.push(centroid.header);
            packed.extend_from_slice(&centroid.packed);
        }
        Self {
            headers,
            packed,
            dim,
        }
    }

    /// Rebuild a persisted batch without per-child allocations. Each header is
    /// `[correction, norm, radial]`, checked as in `PackedFourBitCentroid::from_bytes`;
    /// `packed` holds every child's nibble block back to back, in child order.
    pub fn from_parts(
        headers: Vec<[f32; 3]>,
        packed: Vec<u8>,
        dim: usize,
    ) -> Result<Self, PackedCentroidError> {
        if dim == 0 {
            return Err(PackedCentroidError("dimensions are missing"));
        }
        if headers.len().checked_mul(dim.div_ceil(16) * 8) != Some(packed.len()) {
            return Err(PackedCentroidError(
                "packed sibling length differs from its count and dimensions",
            ));
        }
        let headers = headers
            .into_iter()
            .map(|[correction, norm, radial]| {
                if !correction.is_finite()
                    || correction <= 0.0
                    || !norm.is_finite()
                    || norm < 0.0
                    || radial != 0.0
                {
                    return Err(PackedCentroidError("invalid origin correction values"));
                }
                Ok(CodeHeader4Bit {
                    correction,
                    norm,
                    radial,
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            headers,
            packed,
            dim,
        })
    }

    /// The persisted form `from_parts` accepts: headers, then contiguous nibbles.
    pub fn to_parts(&self) -> (Vec<[f32; 3]>, Vec<u8>) {
        (
            self.headers
                .iter()
                .map(|h| [h.correction, h.norm, h.radial])
                .collect(),
            self.packed.clone(),
        )
    }

    pub fn len(&self) -> usize {
        self.headers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    /// Copy one sibling out, e.g. to compare it with a freshly quantized code.
    pub fn get(&self, index: usize) -> Option<PackedFourBitCentroid> {
        let stride = self.dim.div_ceil(16) * 8;
        Some(PackedFourBitCentroid {
            header: *self.headers.get(index)?,
            packed: self.packed[index * stride..(index + 1) * stride].to_vec(),
            dim: self.dim,
        })
    }

    /// Numeric payload, excluding allocation and container bookkeeping.
    pub fn payload_bytes(&self) -> usize {
        self.headers.len() * 12 + self.packed.len()
    }

    /// Score siblings in their original order without per-child allocations.
    pub fn distances<'a>(
        &'a self,
        scorer: &'a PackedFourBitScorer,
        query: &'a PackedCentroidQuery<'_>,
        metric: &'a DistanceFunction,
    ) -> impl Iterator<Item = f32> + 'a {
        assert_eq!(self.dim, query.dim);
        let stride = self.dim.div_ceil(16) * 8;
        self.headers.iter().enumerate().map(move |(i, header)| {
            scorer.distance_parts(
                header,
                &self.packed[i * stride..(i + 1) * stride],
                query,
                metric,
            )
        })
    }
}

type PackedDot = fn(&[u8], &[f32]) -> f32;

/// Select the CPU kernel once when opening a reader, then reuse it for queries.
pub struct PackedFourBitScorer {
    dot: PackedDot,
    label: &'static str,
}

impl Default for PackedFourBitScorer {
    fn default() -> Self {
        Self::new()
    }
}

impl PackedFourBitScorer {
    pub fn new() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx512f")
                && is_x86_feature_detected!("avx512bw")
                && is_x86_feature_detected!("avx512vl")
                && is_x86_feature_detected!("fma")
            {
                return Self {
                    dot: x86::packed_avx512_checked,
                    label: "avx512",
                };
            }
            if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                return Self {
                    dot: x86::packed_avx2_checked,
                    label: "avx2-fma",
                };
            }
        }
        Self {
            dot: packed_dot_scalar,
            label: "scalar",
        }
    }

    pub fn kernel_label(&self) -> &'static str {
        self.label
    }

    /// Prepare query padding and correction terms once before scoring children.
    pub fn prepare_query<'a>(&self, query: &'a [f32]) -> PackedCentroidQuery<'a> {
        let values = if query.len().is_multiple_of(16) {
            Cow::Borrowed(query)
        } else {
            let mut padded = query.to_vec();
            padded.resize(query.len().div_ceil(16) * 16, 0.0);
            Cow::Owned(padded)
        };
        PackedCentroidQuery {
            values,
            dim: query.len(),
            sum: query.iter().sum(),
            norm: (f32::dot(query, query).unwrap_or(0.0) as f32).sqrt(),
        }
    }

    /// Estimate distance using the centroid's RaBitQ corrections. Centroids are
    /// encoded relative to zero, so no additional cluster correction is needed.
    pub fn distance(
        &self,
        centroid: &PackedFourBitCentroid,
        query: &PackedCentroidQuery<'_>,
        metric: &DistanceFunction,
    ) -> f32 {
        assert_eq!(centroid.dim, query.dim);
        self.distance_parts(&centroid.header, &centroid.packed, query, metric)
    }

    fn distance_parts(
        &self,
        header: &CodeHeader4Bit,
        packed: &[u8],
        query: &PackedCentroidQuery<'_>,
        metric: &DistanceFunction,
    ) -> f32 {
        let dot = (self.dot)(packed, &query.values) - 7.5 * query.sum;
        rabitq_distance_query(
            dot,
            header.correction,
            header.norm,
            header.radial,
            0.0,
            0.0,
            query.norm,
            metric,
        )
    }
}

/// Query coordinates and global terms shared by every centroid comparison.
pub struct PackedCentroidQuery<'a> {
    values: Cow<'a, [f32]>,
    dim: usize,
    sum: f32,
    norm: f32,
}

fn packed_dot_scalar(packed: &[u8], query: &[f32]) -> f32 {
    assert_eq!(packed.len() * 2, query.len());
    query
        .iter()
        .enumerate()
        .map(|(i, &q)| {
            let code = (packed[i / 16 * 8 + i % 8] >> (i % 16 / 8 * 4)) & 15;
            q * f32::from(code)
        })
        .sum()
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    pub(super) fn packed_avx2_checked(p: &[u8], q: &[f32]) -> f32 {
        assert_eq!(p.len() * 2, q.len());
        assert_eq!(q.len() % 16, 0);
        // SAFETY: Detection establishes AVX2/FMA. Lengths are equal, and query
        // preparation pads to sixteen coordinates per eight-byte code block.
        unsafe { packed_avx2(p, q) }
    }
    pub(super) fn packed_avx512_checked(p: &[u8], q: &[f32]) -> f32 {
        assert_eq!(p.len() * 2, q.len());
        assert_eq!(q.len() % 16, 0);
        // SAFETY: Detection establishes the required AVX-512/FMA features.
        // Every load is bounded by the equally padded code and query lengths.
        unsafe { packed_avx512(p, q) }
    }

    #[target_feature(enable = "avx2,fma")]
    unsafe fn packed_avx2(p: &[u8], q: &[f32]) -> f32 {
        let mut sum0 = _mm256_setzero_ps();
        let mut sum1 = sum0;
        for i in (0..q.len()).step_by(16) {
            let compact = u64::from_le_bytes(p[i / 2..i / 2 + 8].try_into().unwrap());
            let mask = 0x0f0f0f0f0f0f0f0fu64;
            let code = _mm_set_epi64x(((compact >> 4) & mask) as i64, (compact & mask) as i64);
            let lo = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(code));
            let hi = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(_mm_srli_si128(code, 8)));
            sum0 = _mm256_fmadd_ps(_mm256_loadu_ps(q.as_ptr().add(i)), lo, sum0);
            sum1 = _mm256_fmadd_ps(_mm256_loadu_ps(q.as_ptr().add(i + 8)), hi, sum1);
        }
        let sum = _mm256_add_ps(sum0, sum1);
        let mut h = _mm_add_ps(_mm256_castps256_ps128(sum), _mm256_extractf128_ps(sum, 1));
        h = _mm_add_ps(h, _mm_movehl_ps(h, h));
        _mm_cvtss_f32(_mm_add_ss(h, _mm_movehdup_ps(h)))
    }

    #[target_feature(enable = "avx512f,avx512bw,avx512vl,fma")]
    unsafe fn packed_avx512(p: &[u8], q: &[f32]) -> f32 {
        let mut sums = [_mm512_setzero_ps(); 4];
        let mut i = 0;
        while i + 64 <= q.len() {
            for (lane, sum) in sums.iter_mut().enumerate() {
                let k = i + lane * 16;
                let bytes = _mm_loadl_epi64(p.as_ptr().add(k / 2).cast());
                let mask = _mm_set1_epi8(15);
                let lo = _mm_and_si128(bytes, mask);
                let hi = _mm_and_si128(_mm_srli_epi16(bytes, 4), mask);
                let expanded = _mm512_cvtepi32_ps(_mm512_cvtepu8_epi32(_mm_unpacklo_epi64(lo, hi)));
                *sum = _mm512_fmadd_ps(expanded, _mm512_loadu_ps(q.as_ptr().add(k)), *sum);
            }
            i += 64;
        }
        while i < q.len() {
            let bytes = _mm_loadl_epi64(p.as_ptr().add(i / 2).cast());
            let mask = _mm_set1_epi8(15);
            let lo = _mm_and_si128(bytes, mask);
            let hi = _mm_and_si128(_mm_srli_epi16(bytes, 4), mask);
            let expanded = _mm512_cvtepi32_ps(_mm512_cvtepu8_epi32(_mm_unpacklo_epi64(lo, hi)));
            sums[0] = _mm512_fmadd_ps(expanded, _mm512_loadu_ps(q.as_ptr().add(i)), sums[0]);
            i += 16;
        }
        _mm512_reduce_add_ps(_mm512_add_ps(
            _mm512_add_ps(sums[0], sums[1]),
            _mm512_add_ps(sums[2], sums[3]),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_codes_preserve_scoring_and_reject_malformed_data() {
        let scorer = PackedFourBitScorer::new();
        for dim in [1, 7, 16, 17, 255, 256, 257, 1024] {
            for centroid in [
                vec![0.0; dim],
                (0..dim).map(|i| (i as f32 * 0.13).sin()).collect(),
            ] {
                let code = Code::<4>::quantize(&centroid, &vec![0.0; dim]);
                let prepared = PackedFourBitCentroid::from_origin_code(&code, dim);
                let bytes = prepared.to_bytes();
                let restored = PackedFourBitCentroid::from_bytes(&bytes, dim).unwrap();
                assert_eq!(restored.to_bytes(), bytes);
                let query: Vec<_> = (0..dim).map(|i| (i as f32 * 0.2).cos()).collect();
                let query = scorer.prepare_query(&query);
                for metric in [
                    DistanceFunction::Euclidean,
                    DistanceFunction::Cosine,
                    DistanceFunction::InnerProduct,
                ] {
                    assert_eq!(
                        scorer.distance(&restored, &query, &metric),
                        scorer.distance(&prepared, &query, &metric)
                    );
                }
                assert!(PackedFourBitCentroid::from_bytes(&bytes, dim + 1).is_err());
                assert!(PackedFourBitCentroid::from_bytes(&bytes[..bytes.len() - 1], dim).is_err());
                let mut extended = bytes.clone();
                extended.push(0);
                assert!(PackedFourBitCentroid::from_bytes(&extended, dim).is_err());
                for (offset, value) in [(4, 0.0), (4, f32::NAN), (8, -1.0), (12, 1.0)] {
                    let mut malformed = bytes.clone();
                    malformed[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
                    assert!(PackedFourBitCentroid::from_bytes(&malformed, dim).is_err());
                }
            }
        }
    }

    #[test]
    fn contiguous_siblings_preserve_distances_and_order() {
        let scorer = PackedFourBitScorer::new();
        for dim in [1usize, 17, 256, 1024] {
            for count in [0usize, 1, 9] {
                let centroids: Vec<_> = (0..count)
                    .map(|child| {
                        let vector: Vec<_> = (0..dim)
                            .map(|i| (i as f32 * 0.31 + child as f32).sin())
                            .collect();
                        PackedFourBitCentroid::from_origin_code(
                            &Code::<4>::quantize(&vector, &vec![0.0; dim]),
                            dim,
                        )
                    })
                    .collect();
                let q: Vec<_> = (0..dim).map(|i| (i as f32 * 0.27).cos()).collect();
                let query = scorer.prepare_query(&q);
                let metrics = [
                    DistanceFunction::Euclidean,
                    DistanceFunction::Cosine,
                    DistanceFunction::InnerProduct,
                ];
                let bytes: Vec<_> = centroids.iter().map(|c| c.to_bytes()).collect();
                let expected: Vec<Vec<_>> = metrics
                    .iter()
                    .map(|metric| {
                        centroids
                            .iter()
                            .map(|c| scorer.distance(c, &query, metric))
                            .collect()
                    })
                    .collect();
                let batch = PackedFourBitCentroidBatch::new(centroids, dim);
                assert_eq!(batch.payload_bytes(), count * (12 + dim.div_ceil(16) * 8));
                assert_eq!(batch.packed.len(), count * dim.div_ceil(16) * 8);
                let (headers, packed) = batch.to_parts();
                let restored =
                    PackedFourBitCentroidBatch::from_parts(headers.clone(), packed.clone(), dim)
                        .unwrap();
                assert_eq!(restored.len(), count);
                for (i, bytes) in bytes.iter().enumerate() {
                    assert_eq!(&restored.get(i).unwrap().to_bytes(), bytes);
                }
                assert!(restored.get(count).is_none());
                for (metric, expected) in metrics.iter().zip(expected) {
                    assert_eq!(
                        batch.distances(&scorer, &query, metric).collect::<Vec<_>>(),
                        expected
                    );
                    assert_eq!(
                        restored
                            .distances(&scorer, &query, metric)
                            .collect::<Vec<_>>(),
                        expected
                    );
                }
                let mut short = packed.clone();
                short.push(0);
                assert!(
                    PackedFourBitCentroidBatch::from_parts(headers.clone(), short, dim).is_err()
                );
                assert!(
                    PackedFourBitCentroidBatch::from_parts(
                        headers.clone(),
                        packed.clone(),
                        dim + 16
                    )
                    .is_err()
                        || count == 0
                );
                if count > 0 {
                    let mut bad = headers.clone();
                    bad[0][2] = 1.0;
                    assert!(PackedFourBitCentroidBatch::from_parts(bad, packed, dim).is_err());
                }
            }
        }
    }

    #[test]
    fn packed_centroids_match_existing_fp_distance_for_tails_and_metrics() {
        let scorer = PackedFourBitScorer::new();
        for dim in [1usize, 7, 16, 63, 64, 255, 256, 300, 1024] {
            for query_kind in 0..4 {
                let vector: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.31).sin()).collect();
                let q: Vec<f32> = (0..dim)
                    .map(|i| {
                        match query_kind {
                            0 => (i as f32 * 0.27).cos(),
                            1 => 0.0,
                            2 => 0.25,
                            // Opposing terms exercise cancellation in the dot product.
                            _ => {
                                if i % 2 == 0 {
                                    1.0
                                } else {
                                    -0.9999
                                }
                            }
                        }
                    })
                    .collect();
                let code = Code::<4>::quantize(&vector, &vec![0.0; dim]);
                let centroid = PackedFourBitCentroid::from_origin_code(&code, dim);
                let query = scorer.prepare_query(&q);
                assert_eq!(matches!(query.values, Cow::Borrowed(_)), dim % 16 == 0);
                // Cancellation makes a tolerance based on the result's
                // magnitude unreliable. Check each reduction independently
                // against f64, using gamma_n * sum(abs(products)) to account
                // for the different scalar, AVX2 and AVX-512 addition orders.
                let products: Vec<f64> = query
                    .values
                    .iter()
                    .enumerate()
                    .map(|(i, &q)| {
                        let code = (centroid.packed[i / 16 * 8 + i % 8] >> (i % 16 / 8 * 4)) & 15;
                        f64::from(code) * f64::from(q)
                    })
                    .collect();
                let expected_dot: f64 = products.iter().sum();
                let absolute_terms: f64 = products.iter().map(|v| v.abs()).sum();
                let n_eps = (query.values.len() + 1) as f64 * f64::from(f32::EPSILON);
                let bound = n_eps / (1.0 - n_eps) * absolute_terms;
                let check_dot = |label: &str, actual: f32| {
                    assert!(
                        (f64::from(actual) - expected_dot).abs() <= bound,
                        "kernel={label} dim={dim} query_kind={query_kind} expected={expected_dot} actual={actual} bound={bound}"
                    );
                };
                check_dot("scalar", packed_dot_scalar(&centroid.packed, &query.values));
                check_dot(
                    scorer.kernel_label(),
                    (scorer.dot)(&centroid.packed, &query.values),
                );
                #[cfg(target_arch = "x86_64")]
                {
                    if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                        check_dot(
                            "avx2-fma",
                            x86::packed_avx2_checked(&centroid.packed, &query.values),
                        );
                    }
                    if is_x86_feature_detected!("avx512f")
                        && is_x86_feature_detected!("avx512bw")
                        && is_x86_feature_detected!("avx512vl")
                        && is_x86_feature_detected!("fma")
                    {
                        check_dot(
                            "avx512",
                            x86::packed_avx512_checked(&centroid.packed, &query.values),
                        );
                    }
                }
                for metric in [
                    DistanceFunction::Euclidean,
                    DistanceFunction::Cosine,
                    DistanceFunction::InnerProduct,
                ] {
                    let expected = code.distance_query(&metric, &q, 0.0, 0.0, query.norm);
                    let actual = scorer.distance(&centroid, &query, &metric);
                    assert!(
                        (actual - expected).abs() < 1e-4 * expected.abs().max(1.0),
                        "dim={dim} expected={expected} actual={actual}"
                    );
                }
            }
        }
    }

    #[test]
    fn cancellation_with_large_queries_stays_within_float_accumulation_error() {
        let scorer = PackedFourBitScorer::new();
        for dim in [256usize, 1024] {
            let vector: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.31).sin()).collect();
            let q: Vec<f32> = (0..dim)
                .map(|i| if i % 2 == 0 { 100.0 } else { -99.99 })
                .collect();
            let code = Code::<4>::quantize(&vector, &vec![0.0; dim]);
            let centroid = PackedFourBitCentroid::from_origin_code(&code, dim);
            let query = scorer.prepare_query(&q);
            let mut centered = 0.0f64;
            let mut absolute_terms = 0.0f64;
            for (i, &q) in q.iter().enumerate() {
                let code = (centroid.packed[i / 16 * 8 + i % 8] >> (i % 16 / 8 * 4)) & 15;
                centered += (f64::from(code) - 7.5) * f64::from(q);
                absolute_terms += (f64::from(code) + 7.5) * f64::from(q).abs();
            }
            let actual = (scorer.dot)(&centroid.packed, &query.values) - 7.5 * query.sum;
            // Bound accumulated rounding by gamma_n * sum(abs(terms)).
            // Include both dot-product and query-sum accumulation, followed
            // by subtraction. The f64 reference centers each coordinate first.
            let n = (2 * dim + 2) as f64;
            let epsilon = f64::from(f32::EPSILON);
            let bound = n * epsilon / (1.0 - n * epsilon) * absolute_terms;
            assert!((f64::from(actual) - centered).abs() <= bound);
        }
    }

    #[test]
    fn packed_kernels_cover_all_nibbles_and_large_dot_products() {
        let scorer = PackedFourBitScorer::new();
        for dim in [16usize, 64, 1024] {
            for max in [false, true] {
                let values: Vec<u8> = (0..dim)
                    .map(|i| if max { 15 } else { (i % 16) as u8 })
                    .collect();
                let mut packed = vec![0; dim / 2];
                for (i, &v) in values.iter().enumerate() {
                    packed[i / 16 * 8 + i % 8] |= v << (i % 16 / 8 * 4);
                }
                let q: Vec<f32> = values.iter().map(|&v| f32::from(v)).collect();
                let expected: u32 = values.iter().map(|&v| u32::from(v).pow(2)).sum();
                assert_eq!((scorer.dot)(&packed, &q), expected as f32);
            }
        }
    }
}
