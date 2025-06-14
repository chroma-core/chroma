// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines aggregations over Arrow arrays.

use arrow_array::cast::*;
use arrow_array::iterator::ArrayIter;
use arrow_array::*;
use arrow_buffer::{ArrowNativeType, NullBuffer};
use arrow_data::bit_iterator::try_for_each_valid_idx;
use arrow_schema::*;
use std::borrow::BorrowMut;
use std::cmp::{self, Ordering};
use std::ops::{BitAnd, BitOr, BitXor};
use types::ByteViewType;

/// An accumulator for primitive numeric values.
trait NumericAccumulator<T: ArrowNativeTypeOp>: Copy + Default {
    /// Accumulate a non-null value.
    fn accumulate(&mut self, value: T);
    /// Accumulate a nullable values.
    /// If `valid` is false the `value` should not affect the accumulator state.
    fn accumulate_nullable(&mut self, value: T, valid: bool);
    /// Merge another accumulator into this accumulator
    fn merge(&mut self, other: Self);
    /// Return the aggregated value.
    fn finish(&mut self) -> T;
}

/// Helper for branchlessly selecting either `a` or `b` based on the boolean `m`.
/// After verifying the generated assembly this can be a simple `if`.
#[inline(always)]
fn select<T: Copy>(m: bool, a: T, b: T) -> T {
    if m {
        a
    } else {
        b
    }
}

#[derive(Clone, Copy)]
struct SumAccumulator<T: ArrowNativeTypeOp> {
    sum: T,
}

impl<T: ArrowNativeTypeOp> Default for SumAccumulator<T> {
    fn default() -> Self {
        Self { sum: T::ZERO }
    }
}

impl<T: ArrowNativeTypeOp> NumericAccumulator<T> for SumAccumulator<T> {
    fn accumulate(&mut self, value: T) {
        self.sum = self.sum.add_wrapping(value);
    }

    fn accumulate_nullable(&mut self, value: T, valid: bool) {
        let sum = self.sum;
        self.sum = select(valid, sum.add_wrapping(value), sum)
    }

    fn merge(&mut self, other: Self) {
        self.sum = self.sum.add_wrapping(other.sum);
    }

    fn finish(&mut self) -> T {
        self.sum
    }
}

#[derive(Clone, Copy)]
struct MinAccumulator<T: ArrowNativeTypeOp> {
    min: T,
}

impl<T: ArrowNativeTypeOp> Default for MinAccumulator<T> {
    fn default() -> Self {
        Self {
            min: T::MAX_TOTAL_ORDER,
        }
    }
}

impl<T: ArrowNativeTypeOp> NumericAccumulator<T> for MinAccumulator<T> {
    fn accumulate(&mut self, value: T) {
        let min = self.min;
        self.min = select(value.is_lt(min), value, min);
    }

    fn accumulate_nullable(&mut self, value: T, valid: bool) {
        let min = self.min;
        let is_lt = valid & value.is_lt(min);
        self.min = select(is_lt, value, min);
    }

    fn merge(&mut self, other: Self) {
        self.accumulate(other.min)
    }

    fn finish(&mut self) -> T {
        self.min
    }
}

#[derive(Clone, Copy)]
struct MaxAccumulator<T: ArrowNativeTypeOp> {
    max: T,
}

impl<T: ArrowNativeTypeOp> Default for MaxAccumulator<T> {
    fn default() -> Self {
        Self {
            max: T::MIN_TOTAL_ORDER,
        }
    }
}

impl<T: ArrowNativeTypeOp> NumericAccumulator<T> for MaxAccumulator<T> {
    fn accumulate(&mut self, value: T) {
        let max = self.max;
        self.max = select(value.is_gt(max), value, max);
    }

    fn accumulate_nullable(&mut self, value: T, valid: bool) {
        let max = self.max;
        let is_gt = value.is_gt(max) & valid;
        self.max = select(is_gt, value, max);
    }

    fn merge(&mut self, other: Self) {
        self.accumulate(other.max)
    }

    fn finish(&mut self) -> T {
        self.max
    }
}

fn reduce_accumulators<T: ArrowNativeTypeOp, A: NumericAccumulator<T>, const LANES: usize>(
    mut acc: [A; LANES],
) -> A {
    assert!(LANES > 0 && LANES.is_power_of_two());
    let mut len = LANES;

    // attempt at tree reduction, unfortunately llvm does not fully recognize this pattern,
    // but the generated code is still a little faster than purely sequential reduction for floats.
    while len >= 2 {
        let mid = len / 2;
        let (h, t) = acc[..len].split_at_mut(mid);

        for i in 0..mid {
            h[i].merge(t[i]);
        }
        len /= 2;
    }
    acc[0]
}

#[inline(always)]
fn aggregate_nonnull_chunk<T: ArrowNativeTypeOp, A: NumericAccumulator<T>, const LANES: usize>(
    acc: &mut [A; LANES],
    values: &[T; LANES],
) {
    for i in 0..LANES {
        acc[i].accumulate(values[i]);
    }
}

#[inline(always)]
fn aggregate_nullable_chunk<T: ArrowNativeTypeOp, A: NumericAccumulator<T>, const LANES: usize>(
    acc: &mut [A; LANES],
    values: &[T; LANES],
    validity: u64,
) {
    let mut bit = 1;
    for i in 0..LANES {
        acc[i].accumulate_nullable(values[i], (validity & bit) != 0);
        bit <<= 1;
    }
}

fn aggregate_nonnull_simple<T: ArrowNativeTypeOp, A: NumericAccumulator<T>>(values: &[T]) -> T {
    return values
        .iter()
        .copied()
        .fold(A::default(), |mut a, b| {
            a.accumulate(b);
            a
        })
        .finish();
}

#[inline(never)]
fn aggregate_nonnull_lanes<T: ArrowNativeTypeOp, A: NumericAccumulator<T>, const LANES: usize>(
    values: &[T],
) -> T {
    // aggregating into multiple independent accumulators allows the compiler to use vector registers
    // with a single accumulator the compiler would not be allowed to reorder floating point addition
    let mut acc = [A::default(); LANES];
    let mut chunks = values.chunks_exact(LANES);
    chunks.borrow_mut().for_each(|chunk| {
        aggregate_nonnull_chunk(&mut acc, chunk[..LANES].try_into().unwrap());
    });

    let remainder = chunks.remainder();
    for i in 0..remainder.len() {
        acc[i].accumulate(remainder[i]);
    }

    reduce_accumulators(acc).finish()
}

#[inline(never)]
fn aggregate_nullable_lanes<T: ArrowNativeTypeOp, A: NumericAccumulator<T>, const LANES: usize>(
    values: &[T],
    validity: &NullBuffer,
) -> T {
    assert!(LANES > 0 && 64 % LANES == 0);
    assert_eq!(values.len(), validity.len());

    // aggregating into multiple independent accumulators allows the compiler to use vector registers
    let mut acc = [A::default(); LANES];
    // we process 64 bits of validity at a time
    let mut values_chunks = values.chunks_exact(64);
    let validity_chunks = validity.inner().bit_chunks();
    let mut validity_chunks_iter = validity_chunks.iter();

    values_chunks.borrow_mut().for_each(|chunk| {
        // Safety: we asserted that values and validity have the same length and trust the iterator impl
        let mut validity = unsafe { validity_chunks_iter.next().unwrap_unchecked() };
        // chunk further based on the number of vector lanes
        chunk.chunks_exact(LANES).for_each(|chunk| {
            aggregate_nullable_chunk(&mut acc, chunk[..LANES].try_into().unwrap(), validity);
            validity >>= LANES;
        });
    });

    let remainder = values_chunks.remainder();
    if !remainder.is_empty() {
        let mut validity = validity_chunks.remainder_bits();

        let mut remainder_chunks = remainder.chunks_exact(LANES);
        remainder_chunks.borrow_mut().for_each(|chunk| {
            aggregate_nullable_chunk(&mut acc, chunk[..LANES].try_into().unwrap(), validity);
            validity >>= LANES;
        });

        let remainder = remainder_chunks.remainder();
        if !remainder.is_empty() {
            let mut bit = 1;
            for i in 0..remainder.len() {
                acc[i].accumulate_nullable(remainder[i], (validity & bit) != 0);
                bit <<= 1;
            }
        }
    }

    reduce_accumulators(acc).finish()
}

/// The preferred vector size in bytes for the target platform.
/// Note that the avx512 target feature is still unstable and this also means it is not detected on stable rust.
const PREFERRED_VECTOR_SIZE: usize =
    if cfg!(all(target_arch = "x86_64", target_feature = "avx512f")) {
        64
    } else if cfg!(all(target_arch = "x86_64", target_feature = "avx")) {
        32
    } else {
        16
    };

/// non-nullable aggregation requires fewer temporary registers so we can use more of them for accumulators
const PREFERRED_VECTOR_SIZE_NON_NULL: usize = PREFERRED_VECTOR_SIZE * 2;

/// Generic aggregation for any primitive type.
/// Returns None if there are no non-null values in `array`.
fn aggregate<T: ArrowNativeTypeOp, P: ArrowPrimitiveType<Native = T>, A: NumericAccumulator<T>>(
    array: &PrimitiveArray<P>,
) -> Option<T> {
    let null_count = array.null_count();
    if null_count == array.len() {
        return None;
    }
    let values = array.values().as_ref();
    match array.nulls() {
        Some(nulls) if null_count > 0 => {
            // const generics depending on a generic type parameter are not supported
            // so we have to match and call aggregate with the corresponding constant
            match PREFERRED_VECTOR_SIZE / std::mem::size_of::<T>() {
                64 => Some(aggregate_nullable_lanes::<T, A, 64>(values, nulls)),
                32 => Some(aggregate_nullable_lanes::<T, A, 32>(values, nulls)),
                16 => Some(aggregate_nullable_lanes::<T, A, 16>(values, nulls)),
                8 => Some(aggregate_nullable_lanes::<T, A, 8>(values, nulls)),
                4 => Some(aggregate_nullable_lanes::<T, A, 4>(values, nulls)),
                2 => Some(aggregate_nullable_lanes::<T, A, 2>(values, nulls)),
                _ => Some(aggregate_nullable_lanes::<T, A, 1>(values, nulls)),
            }
        }
        _ => {
            let is_float = matches!(
                array.data_type(),
                DataType::Float16 | DataType::Float32 | DataType::Float64
            );
            if is_float {
                match PREFERRED_VECTOR_SIZE_NON_NULL / std::mem::size_of::<T>() {
                    64 => Some(aggregate_nonnull_lanes::<T, A, 64>(values)),
                    32 => Some(aggregate_nonnull_lanes::<T, A, 32>(values)),
                    16 => Some(aggregate_nonnull_lanes::<T, A, 16>(values)),
                    8 => Some(aggregate_nonnull_lanes::<T, A, 8>(values)),
                    4 => Some(aggregate_nonnull_lanes::<T, A, 4>(values)),
                    2 => Some(aggregate_nonnull_lanes::<T, A, 2>(values)),
                    _ => Some(aggregate_nonnull_simple::<T, A>(values)),
                }
            } else {
                // for non-null integers its better to not chunk ourselves and instead
                // let llvm fully handle loop unrolling and vectorization
                Some(aggregate_nonnull_simple::<T, A>(values))
            }
        }
    }
}

/// Returns the minimum value in the boolean array.
///
/// ```
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::aggregate::min_boolean;
///
/// let a = BooleanArray::from(vec![Some(true), None, Some(false)]);
/// assert_eq!(min_boolean(&a), Some(false))
/// ```
pub fn min_boolean(array: &BooleanArray) -> Option<bool> {
    // short circuit if all nulls / zero length array
    if array.null_count() == array.len() {
        return None;
    }

    // Note the min bool is false (0), so short circuit as soon as we see it
    array
        .iter()
        .find(|&b| b == Some(false))
        .flatten()
        .or(Some(true))
}

/// Returns the maximum value in the boolean array
///
/// ```
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::aggregate::max_boolean;
///
/// let a = BooleanArray::from(vec![Some(true), None, Some(false)]);
/// assert_eq!(max_boolean(&a), Some(true))
/// ```
pub fn max_boolean(array: &BooleanArray) -> Option<bool> {
    // short circuit if all nulls / zero length array
    if array.null_count() == array.len() {
        return None;
    }

    // Note the max bool is true (1), so short circuit as soon as we see it
    match array.nulls() {
        None => array
            .values()
            .bit_chunks()
            .iter_padded()
            // We found a true if any bit is set
            .map(|x| x != 0)
            .find(|b| *b)
            .or(Some(false)),
        Some(nulls) => {
            let validity_chunks = nulls.inner().bit_chunks().iter_padded();
            let value_chunks = array.values().bit_chunks().iter_padded();
            value_chunks
                .zip(validity_chunks)
                // We found a true if the value bit is 1, AND the validity bit is 1 for any bits in the chunk
                .map(|(value_bits, validity_bits)| (value_bits & validity_bits) != 0)
                .find(|b| *b)
                .or(Some(false))
        }
    }
}

/// Helper to compute min/max of [`ArrayAccessor`].
fn min_max_helper<T, A: ArrayAccessor<Item = T>, F>(array: A, cmp: F) -> Option<T>
where
    F: Fn(&T, &T) -> bool,
{
    let null_count = array.null_count();
    if null_count == array.len() {
        None
    } else if null_count == 0 {
        // JUSTIFICATION
        //  Benefit:  ~8% speedup
        //  Soundness: `i` is always within the array bounds
        (0..array.len())
            .map(|i| unsafe { array.value_unchecked(i) })
            .reduce(|acc, item| if cmp(&acc, &item) { item } else { acc })
    } else {
        let nulls = array.nulls().unwrap();
        unsafe {
            let idx = nulls.valid_indices().reduce(|acc_idx, idx| {
                let acc = array.value_unchecked(acc_idx);
                let item = array.value_unchecked(idx);
                if cmp(&acc, &item) {
                    idx
                } else {
                    acc_idx
                }
            });
            idx.map(|idx| array.value_unchecked(idx))
        }
    }
}

/// Helper to compute min/max of [`GenericByteViewArray<T>`].
/// The specialized min/max leverages the inlined values to compare the byte views.
/// `swap_cond` is the condition to swap current min/max with the new value.
/// For example, `Ordering::Greater` for max and `Ordering::Less` for min.
fn min_max_view_helper<T: ByteViewType>(
    array: &GenericByteViewArray<T>,
    swap_cond: cmp::Ordering,
) -> Option<&T::Native> {
    let null_count = array.null_count();
    if null_count == array.len() {
        None
    } else if null_count == 0 {
        let target_idx = (0..array.len()).reduce(|acc, item| {
            // SAFETY:  array's length is correct so item is within bounds
            let cmp = unsafe { GenericByteViewArray::compare_unchecked(array, item, array, acc) };
            if cmp == swap_cond {
                item
            } else {
                acc
            }
        });
        // SAFETY: idx came from valid range `0..array.len()`
        unsafe { target_idx.map(|idx| array.value_unchecked(idx)) }
    } else {
        let nulls = array.nulls().unwrap();

        let target_idx = nulls.valid_indices().reduce(|acc_idx, idx| {
            let cmp =
                unsafe { GenericByteViewArray::compare_unchecked(array, idx, array, acc_idx) };
            if cmp == swap_cond {
                idx
            } else {
                acc_idx
            }
        });

        // SAFETY: idx came from valid range `0..array.len()`
        unsafe { target_idx.map(|idx| array.value_unchecked(idx)) }
    }
}

/// Returns the maximum value in the binary array, according to the natural order.
pub fn max_binary<T: OffsetSizeTrait>(array: &GenericBinaryArray<T>) -> Option<&[u8]> {
    min_max_helper::<&[u8], _, _>(array, |a, b| *a < *b)
}

/// Returns the maximum value in the binary view array, according to the natural order.
pub fn max_binary_view(array: &BinaryViewArray) -> Option<&[u8]> {
    min_max_view_helper(array, Ordering::Greater)
}

/// Returns the minimum value in the binary array, according to the natural order.
pub fn min_binary<T: OffsetSizeTrait>(array: &GenericBinaryArray<T>) -> Option<&[u8]> {
    min_max_helper::<&[u8], _, _>(array, |a, b| *a > *b)
}

/// Returns the minimum value in the binary view array, according to the natural order.
pub fn min_binary_view(array: &BinaryViewArray) -> Option<&[u8]> {
    min_max_view_helper(array, Ordering::Less)
}

/// Returns the maximum value in the string array, according to the natural order.
pub fn max_string<T: OffsetSizeTrait>(array: &GenericStringArray<T>) -> Option<&str> {
    min_max_helper::<&str, _, _>(array, |a, b| *a < *b)
}

/// Returns the maximum value in the string view array, according to the natural order.
pub fn max_string_view(array: &StringViewArray) -> Option<&str> {
    min_max_view_helper(array, Ordering::Greater)
}

/// Returns the minimum value in the string array, according to the natural order.
pub fn min_string<T: OffsetSizeTrait>(array: &GenericStringArray<T>) -> Option<&str> {
    min_max_helper::<&str, _, _>(array, |a, b| *a > *b)
}

/// Returns the minimum value in the string view array, according to the natural order.
pub fn min_string_view(array: &StringViewArray) -> Option<&str> {
    min_max_view_helper(array, Ordering::Less)
}

/// Returns the sum of values in the array.
///
/// This doesn't detect overflow. Once overflowing, the result will wrap around.
/// For an overflow-checking variant, use `sum_array_checked` instead.
pub fn sum_array<T, A: ArrayAccessor<Item = T::Native>>(array: A) -> Option<T::Native>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    match array.data_type() {
        DataType::Dictionary(_, _) => {
            let null_count = array.null_count();

            if null_count == array.len() {
                return None;
            }

            let iter = ArrayIter::new(array);
            let sum = iter
                .into_iter()
                .fold(T::default_value(), |accumulator, value| {
                    if let Some(value) = value {
                        accumulator.add_wrapping(value)
                    } else {
                        accumulator
                    }
                });

            Some(sum)
        }
        _ => sum::<T>(as_primitive_array(&array)),
    }
}

/// Returns the sum of values in the array.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `sum_array` instead.
pub fn sum_array_checked<T, A: ArrayAccessor<Item = T::Native>>(
    array: A,
) -> Result<Option<T::Native>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    match array.data_type() {
        DataType::Dictionary(_, _) => {
            let null_count = array.null_count();

            if null_count == array.len() {
                return Ok(None);
            }

            let iter = ArrayIter::new(array);
            let sum = iter
                .into_iter()
                .try_fold(T::default_value(), |accumulator, value| {
                    if let Some(value) = value {
                        accumulator.add_checked(value)
                    } else {
                        Ok(accumulator)
                    }
                })?;

            Ok(Some(sum))
        }
        _ => sum_checked::<T>(as_primitive_array(&array)),
    }
}

/// Returns the min of values in the array of `ArrowNumericType` type, or dictionary
/// array with value of `ArrowNumericType` type.
pub fn min_array<T, A: ArrayAccessor<Item = T::Native>>(array: A) -> Option<T::Native>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeType,
{
    min_max_array_helper::<T, A, _, _>(array, |a, b| a.is_gt(*b), min)
}

/// Returns the max of values in the array of `ArrowNumericType` type, or dictionary
/// array with value of `ArrowNumericType` type.
pub fn max_array<T, A: ArrayAccessor<Item = T::Native>>(array: A) -> Option<T::Native>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    min_max_array_helper::<T, A, _, _>(array, |a, b| a.is_lt(*b), max)
}

fn min_max_array_helper<T, A: ArrayAccessor<Item = T::Native>, F, M>(
    array: A,
    cmp: F,
    m: M,
) -> Option<T::Native>
where
    T: ArrowNumericType,
    F: Fn(&T::Native, &T::Native) -> bool,
    M: Fn(&PrimitiveArray<T>) -> Option<T::Native>,
{
    match array.data_type() {
        DataType::Dictionary(_, _) => min_max_helper::<T::Native, _, _>(array, cmp),
        _ => m(as_primitive_array(&array)),
    }
}

macro_rules! bit_operation {
    ($NAME:ident, $OP:ident, $NATIVE:ident, $DEFAULT:expr, $DOC:expr) => {
        #[doc = $DOC]
        ///
        /// Returns `None` if the array is empty or only contains null values.
        pub fn $NAME<T>(array: &PrimitiveArray<T>) -> Option<T::Native>
        where
            T: ArrowNumericType,
            T::Native: $NATIVE<Output = T::Native> + ArrowNativeTypeOp,
        {
            let default;
            if $DEFAULT == -1 {
                default = T::Native::ONE.neg_wrapping();
            } else {
                default = T::default_value();
            }

            let null_count = array.null_count();

            if null_count == array.len() {
                return None;
            }

            let data: &[T::Native] = array.values();

            match array.nulls() {
                None => {
                    let result = data
                        .iter()
                        .fold(default, |accumulator, value| accumulator.$OP(*value));

                    Some(result)
                }
                Some(nulls) => {
                    let mut result = default;
                    let data_chunks = data.chunks_exact(64);
                    let remainder = data_chunks.remainder();

                    let bit_chunks = nulls.inner().bit_chunks();
                    data_chunks
                        .zip(bit_chunks.iter())
                        .for_each(|(chunk, mask)| {
                            // index_mask has value 1 << i in the loop
                            let mut index_mask = 1;
                            chunk.iter().for_each(|value| {
                                if (mask & index_mask) != 0 {
                                    result = result.$OP(*value);
                                }
                                index_mask <<= 1;
                            });
                        });

                    let remainder_bits = bit_chunks.remainder_bits();

                    remainder.iter().enumerate().for_each(|(i, value)| {
                        if remainder_bits & (1 << i) != 0 {
                            result = result.$OP(*value);
                        }
                    });

                    Some(result)
                }
            }
        }
    };
}

bit_operation!(
    bit_and,
    bitand,
    BitAnd,
    -1,
    "Returns the bitwise and of all non-null input values."
);
bit_operation!(
    bit_or,
    bitor,
    BitOr,
    0,
    "Returns the bitwise or of all non-null input values."
);
bit_operation!(
    bit_xor,
    bitxor,
    BitXor,
    0,
    "Returns the bitwise xor of all non-null input values."
);

/// Returns true if all non-null input values are true, otherwise false.
///
/// Returns `None` if the array is empty or only contains null values.
pub fn bool_and(array: &BooleanArray) -> Option<bool> {
    if array.null_count() == array.len() {
        return None;
    }
    Some(array.false_count() == 0)
}

/// Returns true if any non-null input value is true, otherwise false.
///
/// Returns `None` if the array is empty or only contains null values.
pub fn bool_or(array: &BooleanArray) -> Option<bool> {
    max_boolean(array)
}

/// Returns the sum of values in the primitive array.
///
/// Returns `Ok(None)` if the array is empty or only contains null values.
///
/// This detects overflow and returns an `Err` for that. For an non-overflow-checking variant,
/// use `sum` instead.
pub fn sum_checked<T>(array: &PrimitiveArray<T>) -> Result<Option<T::Native>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    let null_count = array.null_count();

    if null_count == array.len() {
        return Ok(None);
    }

    let data: &[T::Native] = array.values();

    match array.nulls() {
        None => {
            let sum = data
                .iter()
                .try_fold(T::default_value(), |accumulator, value| {
                    accumulator.add_checked(*value)
                })?;

            Ok(Some(sum))
        }
        Some(nulls) => {
            let mut sum = T::default_value();

            try_for_each_valid_idx(
                nulls.len(),
                nulls.offset(),
                nulls.null_count(),
                Some(nulls.validity()),
                |idx| {
                    unsafe { sum = sum.add_checked(array.value_unchecked(idx))? };
                    Ok::<_, ArrowError>(())
                },
            )?;

            Ok(Some(sum))
        }
    }
}

/// Returns the sum of values in the primitive array.
///
/// Returns `None` if the array is empty or only contains null values.
///
/// This doesn't detect overflow in release mode by default. Once overflowing, the result will
/// wrap around. For an overflow-checking variant, use `sum_checked` instead.
pub fn sum<T: ArrowNumericType>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T::Native: ArrowNativeTypeOp,
{
    aggregate::<T::Native, T, SumAccumulator<T::Native>>(array)
}

/// Returns the minimum value in the array, according to the natural order.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
pub fn min<T: ArrowNumericType>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T::Native: PartialOrd,
{
    aggregate::<T::Native, T, MinAccumulator<T::Native>>(array)
}

/// Returns the maximum value in the array, according to the natural order.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
pub fn max<T: ArrowNumericType>(array: &PrimitiveArray<T>) -> Option<T::Native>
where
    T::Native: PartialOrd,
{
    aggregate::<T::Native, T, MaxAccumulator<T::Native>>(array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::*;
    use builder::BooleanBuilder;
    use std::sync::Arc;

    #[test]
    fn test_primitive_array_sum() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        assert_eq!(15, sum(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_float_sum() {
        let a = Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]);
        assert_eq!(16.5, sum(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_sum_with_nulls() {
        let a = Int32Array::from(vec![None, Some(2), Some(3), None, Some(5)]);
        assert_eq!(10, sum(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_sum_all_nulls() {
        let a = Int32Array::from(vec![None, None, None]);
        assert_eq!(None, sum(&a));
    }

    #[test]
    fn test_primitive_array_sum_large_float_64() {
        let c = Float64Array::new((1..=100).map(|x| x as f64).collect(), None);
        assert_eq!(Some((1..=100).sum::<i64>() as f64), sum(&c));

        // create an array that actually has non-zero values at the invalid indices
        let validity = NullBuffer::new((1..=100).map(|x| x % 3 == 0).collect());
        let c = Float64Array::new((1..=100).map(|x| x as f64).collect(), Some(validity));

        assert_eq!(
            Some((1..=100).filter(|i| i % 3 == 0).sum::<i64>() as f64),
            sum(&c)
        );
    }

    #[test]
    fn test_primitive_array_sum_large_float_32() {
        let c = Float32Array::new((1..=100).map(|x| x as f32).collect(), None);
        assert_eq!(Some((1..=100).sum::<i64>() as f32), sum(&c));

        // create an array that actually has non-zero values at the invalid indices
        let validity = NullBuffer::new((1..=100).map(|x| x % 3 == 0).collect());
        let c = Float32Array::new((1..=100).map(|x| x as f32).collect(), Some(validity));

        assert_eq!(
            Some((1..=100).filter(|i| i % 3 == 0).sum::<i64>() as f32),
            sum(&c)
        );
    }

    #[test]
    fn test_primitive_array_sum_large_64() {
        let c = Int64Array::new((1..=100).collect(), None);
        assert_eq!(Some((1..=100).sum()), sum(&c));

        // create an array that actually has non-zero values at the invalid indices
        let validity = NullBuffer::new((1..=100).map(|x| x % 3 == 0).collect());
        let c = Int64Array::new((1..=100).collect(), Some(validity));

        assert_eq!(Some((1..=100).filter(|i| i % 3 == 0).sum()), sum(&c));
    }

    #[test]
    fn test_primitive_array_sum_large_32() {
        let c = Int32Array::new((1..=100).collect(), None);
        assert_eq!(Some((1..=100).sum()), sum(&c));

        // create an array that actually has non-zero values at the invalid indices
        let validity = NullBuffer::new((1..=100).map(|x| x % 3 == 0).collect());
        let c = Int32Array::new((1..=100).collect(), Some(validity));
        assert_eq!(Some((1..=100).filter(|i| i % 3 == 0).sum()), sum(&c));
    }

    #[test]
    fn test_primitive_array_sum_large_16() {
        let c = Int16Array::new((1..=100).collect(), None);
        assert_eq!(Some((1..=100).sum()), sum(&c));

        // create an array that actually has non-zero values at the invalid indices
        let validity = NullBuffer::new((1..=100).map(|x| x % 3 == 0).collect());
        let c = Int16Array::new((1..=100).collect(), Some(validity));
        assert_eq!(Some((1..=100).filter(|i| i % 3 == 0).sum()), sum(&c));
    }

    #[test]
    fn test_primitive_array_sum_large_8() {
        let c = UInt8Array::new((1..=100).collect(), None);
        assert_eq!(
            Some((1..=100).fold(0_u8, |a, x| a.wrapping_add(x))),
            sum(&c)
        );

        // create an array that actually has non-zero values at the invalid indices
        let validity = NullBuffer::new((1..=100).map(|x| x % 3 == 0).collect());
        let c = UInt8Array::new((1..=100).collect(), Some(validity));
        assert_eq!(
            Some(
                (1..=100)
                    .filter(|i| i % 3 == 0)
                    .fold(0_u8, |a, x| a.wrapping_add(x))
            ),
            sum(&c)
        );
    }

    #[test]
    fn test_primitive_array_bit_and() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        assert_eq!(0, bit_and(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_bit_and_with_nulls() {
        let a = Int32Array::from(vec![None, Some(2), Some(3), None, None]);
        assert_eq!(2, bit_and(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_bit_and_all_nulls() {
        let a = Int32Array::from(vec![None, None, None]);
        assert_eq!(None, bit_and(&a));
    }

    #[test]
    fn test_primitive_array_bit_or() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        assert_eq!(7, bit_or(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_bit_or_with_nulls() {
        let a = Int32Array::from(vec![None, Some(2), Some(3), None, Some(5)]);
        assert_eq!(7, bit_or(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_bit_or_all_nulls() {
        let a = Int32Array::from(vec![None, None, None]);
        assert_eq!(None, bit_or(&a));
    }

    #[test]
    fn test_primitive_array_bit_xor() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        assert_eq!(1, bit_xor(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_bit_xor_with_nulls() {
        let a = Int32Array::from(vec![None, Some(2), Some(3), None, Some(5)]);
        assert_eq!(4, bit_xor(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_bit_xor_all_nulls() {
        let a = Int32Array::from(vec![None, None, None]);
        assert_eq!(None, bit_xor(&a));
    }

    #[test]
    fn test_primitive_array_bool_and() {
        let a = BooleanArray::from(vec![true, false, true, false, true]);
        assert!(!bool_and(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_bool_and_with_nulls() {
        let a = BooleanArray::from(vec![None, Some(true), Some(true), None, Some(true)]);
        assert!(bool_and(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_bool_and_all_nulls() {
        let a = BooleanArray::from(vec![None, None, None]);
        assert_eq!(None, bool_and(&a));
    }

    #[test]
    fn test_primitive_array_bool_or() {
        let a = BooleanArray::from(vec![true, false, true, false, true]);
        assert!(bool_or(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_bool_or_with_nulls() {
        let a = BooleanArray::from(vec![None, Some(false), Some(false), None, Some(false)]);
        assert!(!bool_or(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_bool_or_all_nulls() {
        let a = BooleanArray::from(vec![None, None, None]);
        assert_eq!(None, bool_or(&a));
    }

    #[test]
    fn test_primitive_array_min_max() {
        let a = Int32Array::from(vec![5, 6, 7, 8, 9]);
        assert_eq!(5, min(&a).unwrap());
        assert_eq!(9, max(&a).unwrap());
    }

    #[test]
    fn test_primitive_array_min_max_with_nulls() {
        let a = Int32Array::from(vec![Some(5), None, None, Some(8), Some(9)]);
        assert_eq!(5, min(&a).unwrap());
        assert_eq!(9, max(&a).unwrap());
    }

    #[test]
    fn test_primitive_min_max_1() {
        let a = Int32Array::from(vec![None, None, Some(5), Some(2)]);
        assert_eq!(Some(2), min(&a));
        assert_eq!(Some(5), max(&a));
    }

    #[test]
    fn test_primitive_min_max_float_large_nonnull_array() {
        let a: Float64Array = (0..256).map(|i| Some((i + 1) as f64)).collect();
        // min/max are on boundaries of chunked data
        assert_eq!(Some(1.0), min(&a));
        assert_eq!(Some(256.0), max(&a));

        // max is last value in remainder after chunking
        let a: Float64Array = (0..255).map(|i| Some((i + 1) as f64)).collect();
        assert_eq!(Some(255.0), max(&a));

        // max is first value in remainder after chunking
        let a: Float64Array = (0..257).map(|i| Some((i + 1) as f64)).collect();
        assert_eq!(Some(257.0), max(&a));
    }

    #[test]
    fn test_primitive_min_max_float_large_nullable_array() {
        let a: Float64Array = (0..256)
            .map(|i| {
                if (i + 1) % 3 == 0 {
                    None
                } else {
                    Some((i + 1) as f64)
                }
            })
            .collect();
        // min/max are on boundaries of chunked data
        assert_eq!(Some(1.0), min(&a));
        assert_eq!(Some(256.0), max(&a));

        let a: Float64Array = (0..256)
            .map(|i| {
                if i == 0 || i == 255 {
                    None
                } else {
                    Some((i + 1) as f64)
                }
            })
            .collect();
        // boundaries of chunked data are null
        assert_eq!(Some(2.0), min(&a));
        assert_eq!(Some(255.0), max(&a));

        let a: Float64Array = (0..256)
            .map(|i| if i != 100 { None } else { Some((i) as f64) })
            .collect();
        // a single non-null value somewhere in the middle
        assert_eq!(Some(100.0), min(&a));
        assert_eq!(Some(100.0), max(&a));

        // max is last value in remainder after chunking
        let a: Float64Array = (0..255).map(|i| Some((i + 1) as f64)).collect();
        assert_eq!(Some(255.0), max(&a));

        // max is first value in remainder after chunking
        let a: Float64Array = (0..257).map(|i| Some((i + 1) as f64)).collect();
        assert_eq!(Some(257.0), max(&a));
    }

    #[test]
    fn test_primitive_min_max_float_edge_cases() {
        let a: Float64Array = (0..100).map(|_| Some(f64::NEG_INFINITY)).collect();
        assert_eq!(Some(f64::NEG_INFINITY), min(&a));
        assert_eq!(Some(f64::NEG_INFINITY), max(&a));

        let a: Float64Array = (0..100).map(|_| Some(f64::MIN)).collect();
        assert_eq!(Some(f64::MIN), min(&a));
        assert_eq!(Some(f64::MIN), max(&a));

        let a: Float64Array = (0..100).map(|_| Some(f64::MAX)).collect();
        assert_eq!(Some(f64::MAX), min(&a));
        assert_eq!(Some(f64::MAX), max(&a));

        let a: Float64Array = (0..100).map(|_| Some(f64::INFINITY)).collect();
        assert_eq!(Some(f64::INFINITY), min(&a));
        assert_eq!(Some(f64::INFINITY), max(&a));
    }

    #[test]
    fn test_primitive_min_max_float_all_nans_non_null() {
        let a: Float64Array = (0..100).map(|_| Some(f64::NAN)).collect();
        assert!(max(&a).unwrap().is_nan());
        assert!(min(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_negative_nan() {
        let a: Float64Array =
            Float64Array::from(vec![f64::NEG_INFINITY, f64::NAN, f64::INFINITY, -f64::NAN]);
        let max = max(&a).unwrap();
        let min = min(&a).unwrap();
        assert!(max.is_nan());
        assert!(max.is_sign_positive());

        assert!(min.is_nan());
        assert!(min.is_sign_negative());
    }

    #[test]
    fn test_primitive_min_max_float_first_nan_nonnull() {
        let a: Float64Array = (0..100)
            .map(|i| {
                if i == 0 {
                    Some(f64::NAN)
                } else {
                    Some(i as f64)
                }
            })
            .collect();
        assert_eq!(Some(1.0), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_last_nan_nonnull() {
        let a: Float64Array = (0..100)
            .map(|i| {
                if i == 99 {
                    Some(f64::NAN)
                } else {
                    Some((i + 1) as f64)
                }
            })
            .collect();
        assert_eq!(Some(1.0), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_first_nan_nullable() {
        let a: Float64Array = (0..100)
            .map(|i| {
                if i == 0 {
                    Some(f64::NAN)
                } else if i % 2 == 0 {
                    None
                } else {
                    Some(i as f64)
                }
            })
            .collect();
        assert_eq!(Some(1.0), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_last_nan_nullable() {
        let a: Float64Array = (0..100)
            .map(|i| {
                if i == 99 {
                    Some(f64::NAN)
                } else if i % 2 == 0 {
                    None
                } else {
                    Some(i as f64)
                }
            })
            .collect();
        assert_eq!(Some(1.0), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    #[test]
    fn test_primitive_min_max_float_inf_and_nans() {
        let a: Float64Array = (0..100)
            .map(|i| {
                let x = match i % 10 {
                    0 => f64::NEG_INFINITY,
                    1 => f64::MIN,
                    2 => f64::MAX,
                    4 => f64::INFINITY,
                    5 => f64::NAN,
                    _ => i as f64,
                };
                Some(x)
            })
            .collect();
        assert_eq!(Some(f64::NEG_INFINITY), min(&a));
        assert!(max(&a).unwrap().is_nan());
    }

    macro_rules! test_binary {
        ($NAME:ident, $ARRAY:expr, $EXPECTED_MIN:expr, $EXPECTED_MAX: expr) => {
            #[test]
            fn $NAME() {
                let binary = BinaryArray::from($ARRAY);
                assert_eq!($EXPECTED_MIN, min_binary(&binary));
                assert_eq!($EXPECTED_MAX, max_binary(&binary));

                let large_binary = LargeBinaryArray::from($ARRAY);
                assert_eq!($EXPECTED_MIN, min_binary(&large_binary));
                assert_eq!($EXPECTED_MAX, max_binary(&large_binary));

                let binary_view = BinaryViewArray::from($ARRAY);
                assert_eq!($EXPECTED_MIN, min_binary_view(&binary_view));
                assert_eq!($EXPECTED_MAX, max_binary_view(&binary_view));
            }
        };
    }

    test_binary!(
        test_binary_min_max_with_nulls,
        vec![
            Some("b01234567890123".as_bytes()), // long bytes
            None,
            None,
            Some(b"a"),
            Some(b"c"),
            Some(b"abcdedfg0123456"),
        ],
        Some("a".as_bytes()),
        Some("c".as_bytes())
    );

    test_binary!(
        test_binary_min_max_no_null,
        vec![
            Some("b".as_bytes()),
            Some(b"abcdefghijklmnopqrst"), // long bytes
            Some(b"c"),
            Some(b"b01234567890123"), // long bytes for view types
        ],
        Some("abcdefghijklmnopqrst".as_bytes()),
        Some("c".as_bytes())
    );

    test_binary!(test_binary_min_max_all_nulls, vec![None, None], None, None);

    test_binary!(
        test_binary_min_max_1,
        vec![
            None,
            Some("b01234567890123435".as_bytes()), // long bytes for view types
            None,
            Some(b"b0123xxxxxxxxxxx"),
            Some(b"a")
        ],
        Some("a".as_bytes()),
        Some("b0123xxxxxxxxxxx".as_bytes())
    );

    macro_rules! test_string {
        ($NAME:ident, $ARRAY:expr, $EXPECTED_MIN:expr, $EXPECTED_MAX: expr) => {
            #[test]
            fn $NAME() {
                let string = StringArray::from($ARRAY);
                assert_eq!($EXPECTED_MIN, min_string(&string));
                assert_eq!($EXPECTED_MAX, max_string(&string));

                let large_string = LargeStringArray::from($ARRAY);
                assert_eq!($EXPECTED_MIN, min_string(&large_string));
                assert_eq!($EXPECTED_MAX, max_string(&large_string));

                let string_view = StringViewArray::from($ARRAY);
                assert_eq!($EXPECTED_MIN, min_string_view(&string_view));
                assert_eq!($EXPECTED_MAX, max_string_view(&string_view));
            }
        };
    }

    test_string!(
        test_string_min_max_with_nulls,
        vec![
            Some("b012345678901234"), // long bytes for view types
            None,
            None,
            Some("a"),
            Some("c"),
            Some("b0123xxxxxxxxxxx")
        ],
        Some("a"),
        Some("c")
    );

    test_string!(
        test_string_min_max_no_null,
        vec![
            Some("b"),
            Some("b012345678901234"), // long bytes for view types
            Some("a"),
            Some("b012xxxxxxxxxxxx")
        ],
        Some("a"),
        Some("b012xxxxxxxxxxxx")
    );

    test_string!(
        test_string_min_max_all_nulls,
        Vec::<Option<&str>>::from_iter([None, None]),
        None,
        None
    );

    test_string!(
        test_string_min_max_1,
        vec![
            None,
            Some("c12345678901234"), // long bytes for view types
            None,
            Some("b"),
            Some("c1234xxxxxxxxxx")
        ],
        Some("b"),
        Some("c1234xxxxxxxxxx")
    );

    test_string!(
        test_string_min_max_empty,
        Vec::<Option<&str>>::new(),
        None,
        None
    );

    #[test]
    fn test_boolean_min_max_empty() {
        let a = BooleanArray::from(vec![] as Vec<Option<bool>>);
        assert_eq!(None, min_boolean(&a));
        assert_eq!(None, max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max_all_null() {
        let a = BooleanArray::from(vec![None, None]);
        assert_eq!(None, min_boolean(&a));
        assert_eq!(None, max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max_no_null() {
        let a = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max() {
        let a = BooleanArray::from(vec![Some(true), Some(true), None, Some(false), None]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a = BooleanArray::from(vec![None, Some(true), None, Some(false), None]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a = BooleanArray::from(vec![Some(false), Some(true), None, Some(false), None]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a = BooleanArray::from(vec![Some(true), None]);
        assert_eq!(Some(true), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a = BooleanArray::from(vec![Some(false), None]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(false), max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max_smaller() {
        let a = BooleanArray::from(vec![Some(false)]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(false), max_boolean(&a));

        let a = BooleanArray::from(vec![None, Some(false)]);
        assert_eq!(Some(false), min_boolean(&a));
        assert_eq!(Some(false), max_boolean(&a));

        let a = BooleanArray::from(vec![None, Some(true)]);
        assert_eq!(Some(true), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));

        let a = BooleanArray::from(vec![Some(true)]);
        assert_eq!(Some(true), min_boolean(&a));
        assert_eq!(Some(true), max_boolean(&a));
    }

    #[test]
    fn test_boolean_min_max_64_true_64_false() {
        let mut no_nulls = BooleanBuilder::new();
        no_nulls.append_slice(&[true; 64]);
        no_nulls.append_slice(&[false; 64]);
        let no_nulls = no_nulls.finish();

        assert_eq!(Some(false), min_boolean(&no_nulls));
        assert_eq!(Some(true), max_boolean(&no_nulls));

        let mut with_nulls = BooleanBuilder::new();
        with_nulls.append_slice(&[true; 31]);
        with_nulls.append_null();
        with_nulls.append_slice(&[true; 32]);
        with_nulls.append_slice(&[false; 1]);
        with_nulls.append_nulls(63);
        let with_nulls = with_nulls.finish();

        assert_eq!(Some(false), min_boolean(&with_nulls));
        assert_eq!(Some(true), max_boolean(&with_nulls));
    }

    #[test]
    fn test_boolean_min_max_64_false_64_true() {
        let mut no_nulls = BooleanBuilder::new();
        no_nulls.append_slice(&[false; 64]);
        no_nulls.append_slice(&[true; 64]);
        let no_nulls = no_nulls.finish();

        assert_eq!(Some(false), min_boolean(&no_nulls));
        assert_eq!(Some(true), max_boolean(&no_nulls));

        let mut with_nulls = BooleanBuilder::new();
        with_nulls.append_slice(&[false; 31]);
        with_nulls.append_null();
        with_nulls.append_slice(&[false; 32]);
        with_nulls.append_slice(&[true; 1]);
        with_nulls.append_nulls(63);
        let with_nulls = with_nulls.finish();

        assert_eq!(Some(false), min_boolean(&with_nulls));
        assert_eq!(Some(true), max_boolean(&with_nulls));
    }

    #[test]
    fn test_boolean_min_max_96_true() {
        let mut no_nulls = BooleanBuilder::new();
        no_nulls.append_slice(&[true; 96]);
        let no_nulls = no_nulls.finish();

        assert_eq!(Some(true), min_boolean(&no_nulls));
        assert_eq!(Some(true), max_boolean(&no_nulls));

        let mut with_nulls = BooleanBuilder::new();
        with_nulls.append_slice(&[true; 31]);
        with_nulls.append_null();
        with_nulls.append_slice(&[true; 32]);
        with_nulls.append_slice(&[true; 31]);
        with_nulls.append_null();
        let with_nulls = with_nulls.finish();

        assert_eq!(Some(true), min_boolean(&with_nulls));
        assert_eq!(Some(true), max_boolean(&with_nulls));
    }

    #[test]
    fn test_boolean_min_max_96_false() {
        let mut no_nulls = BooleanBuilder::new();
        no_nulls.append_slice(&[false; 96]);
        let no_nulls = no_nulls.finish();

        assert_eq!(Some(false), min_boolean(&no_nulls));
        assert_eq!(Some(false), max_boolean(&no_nulls));

        let mut with_nulls = BooleanBuilder::new();
        with_nulls.append_slice(&[false; 31]);
        with_nulls.append_null();
        with_nulls.append_slice(&[false; 32]);
        with_nulls.append_slice(&[false; 31]);
        with_nulls.append_null();
        let with_nulls = with_nulls.finish();

        assert_eq!(Some(false), min_boolean(&with_nulls));
        assert_eq!(Some(false), max_boolean(&with_nulls));
    }

    #[test]
    fn test_sum_dyn() {
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let values = Arc::new(values) as ArrayRef;
        let keys = Int8Array::from_iter_values([2_i8, 3, 4]);

        let dict_array = DictionaryArray::new(keys, values.clone());
        let array = dict_array.downcast_dict::<Int8Array>().unwrap();
        assert_eq!(39, sum_array::<Int8Type, _>(array).unwrap());

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        assert_eq!(15, sum_array::<Int32Type, _>(&a).unwrap());

        let keys = Int8Array::from(vec![Some(2_i8), None, Some(4)]);
        let dict_array = DictionaryArray::new(keys, values.clone());
        let array = dict_array.downcast_dict::<Int8Array>().unwrap();
        assert_eq!(26, sum_array::<Int8Type, _>(array).unwrap());

        let keys = Int8Array::from(vec![None, None, None]);
        let dict_array = DictionaryArray::new(keys, values.clone());
        let array = dict_array.downcast_dict::<Int8Array>().unwrap();
        assert!(sum_array::<Int8Type, _>(array).is_none());
    }

    #[test]
    fn test_max_min_dyn() {
        let values = Int8Array::from_iter_values([10_i8, 11, 12, 13, 14, 15, 16, 17]);
        let keys = Int8Array::from_iter_values([2_i8, 3, 4]);
        let values = Arc::new(values) as ArrayRef;

        let dict_array = DictionaryArray::new(keys, values.clone());
        let array = dict_array.downcast_dict::<Int8Array>().unwrap();
        assert_eq!(14, max_array::<Int8Type, _>(array).unwrap());

        let array = dict_array.downcast_dict::<Int8Array>().unwrap();
        assert_eq!(12, min_array::<Int8Type, _>(array).unwrap());

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        assert_eq!(5, max_array::<Int32Type, _>(&a).unwrap());
        assert_eq!(1, min_array::<Int32Type, _>(&a).unwrap());

        let keys = Int8Array::from(vec![Some(2_i8), None, Some(7)]);
        let dict_array = DictionaryArray::new(keys, values.clone());
        let array = dict_array.downcast_dict::<Int8Array>().unwrap();
        assert_eq!(17, max_array::<Int8Type, _>(array).unwrap());
        let array = dict_array.downcast_dict::<Int8Array>().unwrap();
        assert_eq!(12, min_array::<Int8Type, _>(array).unwrap());

        let keys = Int8Array::from(vec![None, None, None]);
        let dict_array = DictionaryArray::new(keys, values.clone());
        let array = dict_array.downcast_dict::<Int8Array>().unwrap();
        assert!(max_array::<Int8Type, _>(array).is_none());
        let array = dict_array.downcast_dict::<Int8Array>().unwrap();
        assert!(min_array::<Int8Type, _>(array).is_none());
    }

    #[test]
    fn test_max_min_dyn_nan() {
        let values = Float32Array::from(vec![5.0_f32, 2.0_f32, f32::NAN]);
        let keys = Int8Array::from_iter_values([0_i8, 1, 2]);

        let dict_array = DictionaryArray::new(keys, Arc::new(values));
        let array = dict_array.downcast_dict::<Float32Array>().unwrap();
        assert!(max_array::<Float32Type, _>(array).unwrap().is_nan());

        let array = dict_array.downcast_dict::<Float32Array>().unwrap();
        assert_eq!(2.0_f32, min_array::<Float32Type, _>(array).unwrap());
    }

    #[test]
    fn test_min_max_sliced_primitive() {
        let expected = Some(4.0);
        let input: Float64Array = vec![None, Some(4.0)].into_iter().collect();
        let actual = min(&input);
        assert_eq!(actual, expected);
        let actual = max(&input);
        assert_eq!(actual, expected);

        let sliced_input: Float64Array = vec![None, None, None, None, None, Some(4.0)]
            .into_iter()
            .collect();
        let sliced_input = sliced_input.slice(4, 2);

        assert_eq!(&sliced_input, &input);

        let actual = min(&sliced_input);
        assert_eq!(actual, expected);
        let actual = max(&sliced_input);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_min_max_sliced_boolean() {
        let expected = Some(true);
        let input: BooleanArray = vec![None, Some(true)].into_iter().collect();
        let actual = min_boolean(&input);
        assert_eq!(actual, expected);
        let actual = max_boolean(&input);
        assert_eq!(actual, expected);

        let sliced_input: BooleanArray = vec![None, None, None, None, None, Some(true)]
            .into_iter()
            .collect();
        let sliced_input = sliced_input.slice(4, 2);

        assert_eq!(sliced_input, input);

        let actual = min_boolean(&sliced_input);
        assert_eq!(actual, expected);
        let actual = max_boolean(&sliced_input);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_min_max_sliced_string() {
        let expected = Some("foo");
        let input: StringArray = vec![None, Some("foo")].into_iter().collect();
        let actual = min_string(&input);
        assert_eq!(actual, expected);
        let actual = max_string(&input);
        assert_eq!(actual, expected);

        let sliced_input: StringArray = vec![None, None, None, None, None, Some("foo")]
            .into_iter()
            .collect();
        let sliced_input = sliced_input.slice(4, 2);

        assert_eq!(&sliced_input, &input);

        let actual = min_string(&sliced_input);
        assert_eq!(actual, expected);
        let actual = max_string(&sliced_input);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_min_max_sliced_binary() {
        let expected: Option<&[u8]> = Some(&[5]);
        let input: BinaryArray = vec![None, Some(&[5])].into_iter().collect();
        let actual = min_binary(&input);
        assert_eq!(actual, expected);
        let actual = max_binary(&input);
        assert_eq!(actual, expected);

        let sliced_input: BinaryArray = vec![None, None, None, None, None, Some(&[5])]
            .into_iter()
            .collect();
        let sliced_input = sliced_input.slice(4, 2);

        assert_eq!(&sliced_input, &input);

        let actual = min_binary(&sliced_input);
        assert_eq!(actual, expected);
        let actual = max_binary(&sliced_input);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_sum_overflow() {
        let a = Int32Array::from(vec![i32::MAX, 1]);

        assert_eq!(sum(&a).unwrap(), -2147483648);
        assert_eq!(sum_array::<Int32Type, _>(&a).unwrap(), -2147483648);
    }

    #[test]
    fn test_sum_checked_overflow() {
        let a = Int32Array::from(vec![i32::MAX, 1]);

        sum_checked(&a).expect_err("overflow should be detected");
        sum_array_checked::<Int32Type, _>(&a).expect_err("overflow should be detected");
    }
}
