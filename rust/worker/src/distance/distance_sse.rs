#[cfg(target_arch = "x86")]
use std::arch::x86::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(target_feature = "sse")]
pub unsafe fn hsum128_ps_sse(x: __m128) -> f32 {
    let x64: __m128 = _mm_add_ps(x, _mm_movehl_ps(x, x));
    let x32: __m128 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    _mm_cvtss_f32(x32)
}

#[cfg(target_feature = "sse")]
pub unsafe fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = a.as_ptr();
    let mut ptr2: *const f32 = b.as_ptr();
    let mut sum128_1: __m128 = _mm_setzero_ps();
    let mut sum128_2: __m128 = _mm_setzero_ps();
    let mut sum128_3: __m128 = _mm_setzero_ps();
    let mut sum128_4: __m128 = _mm_setzero_ps();

    let mut i: usize = 0;
    while i < m {
        sum128_1 = _mm_add_ps(_mm_mul_ps(_mm_loadu_ps(ptr1), _mm_loadu_ps(ptr2)), sum128_1);

        sum128_2 = _mm_add_ps(
            _mm_mul_ps(_mm_loadu_ps(ptr1.add(4)), _mm_loadu_ps(ptr2.add(4))),
            sum128_2,
        );

        sum128_3 = _mm_add_ps(
            _mm_mul_ps(_mm_loadu_ps(ptr1.add(8)), _mm_loadu_ps(ptr2.add(8))),
            sum128_3,
        );

        sum128_4 = _mm_add_ps(
            _mm_mul_ps(_mm_loadu_ps(ptr1.add(12)), _mm_loadu_ps(ptr2.add(12))),
            sum128_4,
        );

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }

    let mut result = hsum128_ps_sse(sum128_1)
        + hsum128_ps_sse(sum128_2)
        + hsum128_ps_sse(sum128_3)
        + hsum128_ps_sse(sum128_4);
    for i in 0..n - m {
        result += (*ptr1.add(i)) * (*ptr2.add(i));
    }
    1.0_f32 - result
}

#[cfg(target_feature = "sse")]
pub unsafe fn inner_product(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = a.as_ptr();
    let mut ptr2: *const f32 = b.as_ptr();
    let mut sum128_1: __m128 = _mm_setzero_ps();
    let mut sum128_2: __m128 = _mm_setzero_ps();
    let mut sum128_3: __m128 = _mm_setzero_ps();
    let mut sum128_4: __m128 = _mm_setzero_ps();

    let mut i: usize = 0;
    while i < m {
        sum128_1 = _mm_add_ps(_mm_mul_ps(_mm_loadu_ps(ptr1), _mm_loadu_ps(ptr2)), sum128_1);

        sum128_2 = _mm_add_ps(
            _mm_mul_ps(_mm_loadu_ps(ptr1.add(4)), _mm_loadu_ps(ptr2.add(4))),
            sum128_2,
        );

        sum128_3 = _mm_add_ps(
            _mm_mul_ps(_mm_loadu_ps(ptr1.add(8)), _mm_loadu_ps(ptr2.add(8))),
            sum128_3,
        );

        sum128_4 = _mm_add_ps(
            _mm_mul_ps(_mm_loadu_ps(ptr1.add(12)), _mm_loadu_ps(ptr2.add(12))),
            sum128_4,
        );

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }

    let mut result = hsum128_ps_sse(sum128_1)
        + hsum128_ps_sse(sum128_2)
        + hsum128_ps_sse(sum128_3)
        + hsum128_ps_sse(sum128_4);
    for i in 0..n - m {
        result += (*ptr1.add(i)) * (*ptr2.add(i));
    }
    1.0_f32 - result
}

#[cfg(target_feature = "sse")]
pub unsafe fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = a.as_ptr();
    let mut ptr2: *const f32 = b.as_ptr();
    let mut sum128_1: __m128 = _mm_setzero_ps();
    let mut sum128_2: __m128 = _mm_setzero_ps();
    let mut sum128_3: __m128 = _mm_setzero_ps();
    let mut sum128_4: __m128 = _mm_setzero_ps();
    let mut i: usize = 0;
    while i < m {
        let sub128_1 = _mm_sub_ps(_mm_loadu_ps(ptr1), _mm_loadu_ps(ptr2));
        sum128_1 = _mm_add_ps(_mm_mul_ps(sub128_1, sub128_1), sum128_1);

        let sub128_2 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(4)), _mm_loadu_ps(ptr2.add(4)));
        sum128_2 = _mm_add_ps(_mm_mul_ps(sub128_2, sub128_2), sum128_2);

        let sub128_3 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(8)), _mm_loadu_ps(ptr2.add(8)));
        sum128_3 = _mm_add_ps(_mm_mul_ps(sub128_3, sub128_3), sum128_3);

        let sub128_4 = _mm_sub_ps(_mm_loadu_ps(ptr1.add(12)), _mm_loadu_ps(ptr2.add(12)));
        sum128_4 = _mm_add_ps(_mm_mul_ps(sub128_4, sub128_4), sum128_4);

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }

    let mut result = hsum128_ps_sse(sum128_1)
        + hsum128_ps_sse(sum128_2)
        + hsum128_ps_sse(sum128_3)
        + hsum128_ps_sse(sum128_4);
    for i in 0..n - m {
        result += (*ptr1.add(i) - *ptr2.add(i)).powi(2);
    }
    result
}
