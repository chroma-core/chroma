#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(all(target_feature = "avx", target_feature = "fma"))]
pub unsafe fn hsum256_ps_avx(x: __m256) -> f32 {
    let x128: __m128 = _mm_add_ps(_mm256_extractf128_ps(x, 1), _mm256_castps256_ps128(x));
    let x64: __m128 = _mm_add_ps(x128, _mm_movehl_ps(x128, x128));
    let x32: __m128 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    _mm_cvtss_f32(x32)
}

#[cfg(all(target_feature = "avx", target_feature = "fma"))]
pub unsafe fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let m = n - (n % 32);
    let mut ptr1: *const f32 = a.as_ptr();
    let mut ptr2: *const f32 = b.as_ptr();
    let mut sum256_1: __m256 = _mm256_setzero_ps();
    let mut sum256_2: __m256 = _mm256_setzero_ps();
    let mut sum256_3: __m256 = _mm256_setzero_ps();
    let mut sum256_4: __m256 = _mm256_setzero_ps();
    let mut i: usize = 0;
    while i < m {
        sum256_1 = _mm256_fmadd_ps(_mm256_loadu_ps(ptr1), _mm256_loadu_ps(ptr2), sum256_1);
        sum256_2 = _mm256_fmadd_ps(
            _mm256_loadu_ps(ptr1.add(8)),
            _mm256_loadu_ps(ptr2.add(8)),
            sum256_2,
        );
        sum256_3 = _mm256_fmadd_ps(
            _mm256_loadu_ps(ptr1.add(16)),
            _mm256_loadu_ps(ptr2.add(16)),
            sum256_3,
        );
        sum256_4 = _mm256_fmadd_ps(
            _mm256_loadu_ps(ptr1.add(24)),
            _mm256_loadu_ps(ptr2.add(24)),
            sum256_4,
        );

        ptr1 = ptr1.add(32);
        ptr2 = ptr2.add(32);
        i += 32;
    }

    let mut result = hsum256_ps_avx(sum256_1)
        + hsum256_ps_avx(sum256_2)
        + hsum256_ps_avx(sum256_3)
        + hsum256_ps_avx(sum256_4);

    for i in 0..n - m {
        result += (*ptr1.add(i)) * (*ptr2.add(i));
    }
    1.0_f32 - result
}

#[cfg(all(target_feature = "avx", target_feature = "fma"))]
pub unsafe fn inner_product(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let m = n - (n % 32);
    let mut ptr1: *const f32 = a.as_ptr();
    let mut ptr2: *const f32 = b.as_ptr();
    let mut sum256_1: __m256 = _mm256_setzero_ps();
    let mut sum256_2: __m256 = _mm256_setzero_ps();
    let mut sum256_3: __m256 = _mm256_setzero_ps();
    let mut sum256_4: __m256 = _mm256_setzero_ps();
    let mut i: usize = 0;
    while i < m {
        sum256_1 = _mm256_fmadd_ps(_mm256_loadu_ps(ptr1), _mm256_loadu_ps(ptr2), sum256_1);
        sum256_2 = _mm256_fmadd_ps(
            _mm256_loadu_ps(ptr1.add(8)),
            _mm256_loadu_ps(ptr2.add(8)),
            sum256_2,
        );
        sum256_3 = _mm256_fmadd_ps(
            _mm256_loadu_ps(ptr1.add(16)),
            _mm256_loadu_ps(ptr2.add(16)),
            sum256_3,
        );
        sum256_4 = _mm256_fmadd_ps(
            _mm256_loadu_ps(ptr1.add(24)),
            _mm256_loadu_ps(ptr2.add(24)),
            sum256_4,
        );

        ptr1 = ptr1.add(32);
        ptr2 = ptr2.add(32);
        i += 32;
    }

    let mut result = hsum256_ps_avx(sum256_1)
        + hsum256_ps_avx(sum256_2)
        + hsum256_ps_avx(sum256_3)
        + hsum256_ps_avx(sum256_4);

    for i in 0..n - m {
        result += (*ptr1.add(i)) * (*ptr2.add(i));
    }
    1.0_f32 - result
}

#[cfg(all(target_feature = "avx", target_feature = "fma"))]
pub unsafe fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let m = n - (n % 32);
    let mut ptr1: *const f32 = a.as_ptr();
    let mut ptr2: *const f32 = b.as_ptr();
    let mut sum256_1: __m256 = _mm256_setzero_ps();
    let mut sum256_2: __m256 = _mm256_setzero_ps();
    let mut sum256_3: __m256 = _mm256_setzero_ps();
    let mut sum256_4: __m256 = _mm256_setzero_ps();
    let mut i: usize = 0;
    while i < m {
        let sub256_1: __m256 =
            _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(0)), _mm256_loadu_ps(ptr2.add(0)));
        sum256_1 = _mm256_fmadd_ps(sub256_1, sub256_1, sum256_1);

        let sub256_2: __m256 =
            _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(8)), _mm256_loadu_ps(ptr2.add(8)));
        sum256_2 = _mm256_fmadd_ps(sub256_2, sub256_2, sum256_2);

        let sub256_3: __m256 =
            _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(16)), _mm256_loadu_ps(ptr2.add(16)));
        sum256_3 = _mm256_fmadd_ps(sub256_3, sub256_3, sum256_3);

        let sub256_4: __m256 =
            _mm256_sub_ps(_mm256_loadu_ps(ptr1.add(24)), _mm256_loadu_ps(ptr2.add(24)));
        sum256_4 = _mm256_fmadd_ps(sub256_4, sub256_4, sum256_4);

        ptr1 = ptr1.add(32);
        ptr2 = ptr2.add(32);
        i += 32;
    }

    let mut result = hsum256_ps_avx(sum256_1)
        + hsum256_ps_avx(sum256_2)
        + hsum256_ps_avx(sum256_3)
        + hsum256_ps_avx(sum256_4);
    for i in 0..n - m {
        result += (*ptr1.add(i) - *ptr2.add(i)).powi(2);
    }
    result
}
