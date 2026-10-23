//! Prepare four-bit centroids once so repeated distance estimates avoid layout conversion.
//!
//! The packed AVX2 and AVX-512 kernels are Rust ports of `ip16_fxu4_*` from
//! VectorDB-NTU/RaBitQ-Library, commit 2f6b57e4907659a79fd039cc826164e7d320f83d.
//! Copyright VectorDB-NTU contributors. Licensed under Apache-2.0; see
//! licenses/RaBitQ-Library-LICENSE. Each block stores eight low nibbles followed
//! by eight high nibbles in eight bytes, covering sixteen dimensions.

use bitpacking::{BitPacker, BitPacker8x};
use chroma_distance::DistanceFunction;

use super::multi_bit::CodeHeader4Bit;
use super::{rabitq_distance_query, Code, QuantizedQuery};

pub enum FourBitLayout {
    BitPlanes,
    Packed,
}

enum Payload {
    Planes(Vec<u64>),
    Packed(Vec<u8>),
}

/// Immutable scoring payload derived from a stored centroid's four-bit code.
/// The coordinate sum is prepared once for the query's scale/offset correction.
pub struct PreparedFourBit {
    header: CodeHeader4Bit,
    payload: Payload,
    code_sum: u32,
    dim: usize,
}

impl PreparedFourBit {
    pub fn new(code: &Code<4, impl AsRef<[u8]>>, dim: usize, layout: FourBitLayout) -> Self {
        let mut values = vec![0u32; dim.div_ceil(256) * 256];
        let packer = BitPacker8x::new();
        for (block, values) in values.chunks_mut(256).enumerate() {
            packer.decompress(&code.packed()[block * 128..], values, 4);
        }
        let code_sum = values[..dim].iter().sum();
        let payload = match layout {
            FourBitLayout::BitPlanes => {
                let words = dim.div_ceil(64);
                let mut planes = vec![0; words * 4];
                for (i, &value) in values[..dim].iter().enumerate() {
                    for bit in 0..4 {
                        planes[bit * words + i / 64] |= u64::from((value >> bit) & 1) << (i % 64);
                    }
                }
                Payload::Planes(planes)
            }
            FourBitLayout::Packed => {
                let mut packed = vec![0; dim.div_ceil(16) * 8];
                for (i, &value) in values[..dim].iter().enumerate() {
                    packed[i / 16 * 8 + i % 8] |= (value as u8) << (i % 16 / 8 * 4);
                }
                Payload::Packed(packed)
            }
        };
        Self {
            header: code.header(),
            payload,
            code_sum,
            dim,
        }
    }

    /// Owned numeric payload, including the header and cached coordinate sum.
    pub fn bytes(&self) -> usize {
        24 + match &self.payload {
            Payload::Planes(p) => p.len() * 8,
            Payload::Packed(p) => p.len(),
        }
    }

    /// Heap buffer capacity; the reader accounts for the inline map value.
    pub fn heap_bytes(&self) -> usize {
        match &self.payload {
            Payload::Planes(p) => p.capacity() * 8,
            Payload::Packed(p) => p.capacity(),
        }
    }

    pub fn distance_quantized(
        &self,
        query: &QuantizedQuery,
        values: &[f32],
        kernels: &FourBitKernels,
        metric: &DistanceFunction,
    ) -> f32 {
        let dot = match &self.payload {
            Payload::Planes(p) => (kernels.planes)(p, query, self.dim) as f32,
            Payload::Packed(p) => (kernels.packed)(p, values),
        };
        let grid_sum = self.code_sum as f32 - 7.5 * self.dim as f32;
        let grid_dot = query.v_l * grid_sum + query.delta * (dot - 7.5 * query.sum_q_u as f32);
        self.distance(grid_dot, query.c_norm, query.c_dot_q, query.q_norm, metric)
    }

    pub fn distance_fp(
        &self,
        values: &[f32],
        sum: f32,
        norm: f32,
        kernels: &FourBitKernels,
        metric: &DistanceFunction,
    ) -> f32 {
        let Payload::Packed(p) = &self.payload else {
            panic!("FP scoring requires packed centroids")
        };
        self.distance(
            (kernels.packed)(p, values) - 7.5 * sum,
            0.0,
            0.0,
            norm,
            metric,
        )
    }

    fn distance(
        &self,
        dot: f32,
        c_norm: f32,
        c_dot_q: f32,
        q_norm: f32,
        metric: &DistanceFunction,
    ) -> f32 {
        rabitq_distance_query(
            dot,
            self.header.correction,
            self.header.norm,
            self.header.radial,
            c_norm,
            c_dot_q,
            q_norm,
            metric,
        )
    }
}

/// Four-bit query integers represented exactly as floats for the packed kernel.
/// Conversion occurs once per query; padding contributes zero to the dot product.
pub fn four_bit_query_values(query: &QuantizedQuery, dim: usize) -> Vec<f32> {
    let mut values = vec![0.0; dim.div_ceil(16) * 16];
    for (i, value) in values[..dim].iter_mut().enumerate() {
        let integer = (0..4).fold(0u8, |sum, bit| {
            sum | (((query.bit_planes[bit * query.padded_bytes + i / 8] >> (i % 8)) & 1) << bit)
        });
        *value = f32::from(integer);
    }
    values
}

type PlaneDot = fn(&[u64], &QuantizedQuery, usize) -> u32;
type PackedDot = fn(&[u8], &[f32]) -> f32;

/// Instruction selection occurs once when the reader prepares its scoring state.
pub struct FourBitKernels {
    planes: PlaneDot,
    packed: PackedDot,
    pub plane_label: &'static str,
    pub packed_label: &'static str,
}

impl Default for FourBitKernels {
    fn default() -> Self {
        Self::detect()
    }
}

impl FourBitKernels {
    pub fn detect() -> Self {
        #[allow(unused_mut)] // x86 feature detection replaces the portable defaults.
        let mut kernels = Self {
            planes: plane_dot,
            packed: packed_dot_scalar,
            plane_label: "scalar",
            packed_label: "scalar",
        };
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("popcnt") {
                kernels.planes = x86::plane_popcnt_checked;
                kernels.plane_label = "popcnt";
            }
            if is_x86_feature_detected!("avx512f")
                && is_x86_feature_detected!("avx512bw")
                && is_x86_feature_detected!("avx512vl")
                && is_x86_feature_detected!("fma")
            {
                kernels.packed = x86::packed_avx512_checked;
                kernels.packed_label = "avx512";
            } else if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                kernels.packed = x86::packed_avx2_checked;
                kernels.packed_label = "avx2-fma";
            }
        }
        kernels
    }
}

#[inline(always)]
fn plane_dot(planes: &[u64], query: &QuantizedQuery, dim: usize) -> u32 {
    let words = dim.div_ceil(64);
    let mut dot = 0u32;
    for word in 0..words {
        for q_bit in 0..4 {
            let start = q_bit * query.padded_bytes + word * 8;
            let q = u64::from_le_bytes(query.bit_planes[start..start + 8].try_into().unwrap());
            for c_bit in 0..4 {
                dot += (q & planes[c_bit * words + word]).count_ones() << (q_bit + c_bit);
            }
        }
    }
    dot
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
    use super::*;
    use std::arch::x86_64::*;

    pub(super) fn plane_popcnt_checked(p: &[u64], q: &QuantizedQuery, dim: usize) -> u32 {
        // SAFETY: This function is selected only after detecting POPCNT.
        unsafe { plane_popcnt(p, q, dim) }
    }
    #[target_feature(enable = "popcnt")]
    unsafe fn plane_popcnt(p: &[u64], q: &QuantizedQuery, dim: usize) -> u32 {
        plane_dot(p, q, dim)
    }

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
    fn prepared_layouts_match_integer_reference_and_existing_distances() {
        for dim in [1usize, 7, 16, 63, 64, 255, 256, 300, 1024] {
            for constant in [false, true] {
                let vector: Vec<f32> = (0..dim).map(|i| (i as f32 * 0.31).sin()).collect();
                let q: Vec<f32> = (0..dim)
                    .map(|i| {
                        if constant {
                            0.0
                        } else {
                            (i as f32 * 0.27).cos()
                        }
                    })
                    .collect();
                let code = Code::<4>::quantize(&vector, &vec![0.0; dim]);
                let query = QuantizedQuery::new(&q, dim.div_ceil(64) * 8, 0.3, 0.7, 1.5);
                let values = four_bit_query_values(&query, dim);
                let kernels = FourBitKernels::detect();
                let planes = PreparedFourBit::new(&code, dim, FourBitLayout::BitPlanes);
                let packed = PreparedFourBit::new(&code, dim, FourBitLayout::Packed);
                for metric in [
                    DistanceFunction::Euclidean,
                    DistanceFunction::Cosine,
                    DistanceFunction::InnerProduct,
                ] {
                    let expected = code.distance_quantized_query(&metric, &query, dim);
                    assert_eq!(
                        planes.distance_quantized(&query, &values, &kernels, &metric),
                        expected
                    );
                    assert_eq!(
                        packed.distance_quantized(&query, &values, &kernels, &metric),
                        expected
                    );
                }
                let Payload::Packed(p) = &packed.payload else {
                    unreachable!()
                };
                let scalar = packed_dot_scalar(p, &values);
                assert_eq!((kernels.packed)(p, &values), scalar);
                #[cfg(target_arch = "x86_64")]
                if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                    assert_eq!(x86::packed_avx2_checked(p, &values), scalar);
                }
                let mut padded_q = q.clone();
                padded_q.resize(dim.div_ceil(16) * 16, 0.0);
                let estimated = packed.distance_fp(
                    &padded_q,
                    q.iter().sum(),
                    1.5,
                    &kernels,
                    &DistanceFunction::Euclidean,
                );
                let expected = code.distance_query(&DistanceFunction::Euclidean, &q, 0.0, 0.0, 1.5);
                assert!((estimated - expected).abs() < 1e-4 * expected.abs().max(1.0));
            }
        }
    }

    #[test]
    fn packed_kernels_cover_all_nibbles_and_large_integer_sums() {
        let kernels = FourBitKernels::detect();
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
                assert_eq!((kernels.packed)(&packed, &q), expected as f32);
            }
        }
    }
}
