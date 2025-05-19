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

//! Defines boolean kernels on Arrow `BooleanArray`'s, e.g. `AND`, `OR` and `NOT`.
//!
//! These kernels can leverage SIMD if available on your system.  Currently no runtime
//! detection is provided, you should enable the specific SIMD intrinsics using
//! `RUSTFLAGS="-C target-feature=+avx2"` for example.  See the documentation
//! [here](https://doc.rust-lang.org/stable/core/arch/) for more information.

use arrow_array::*;
use arrow_buffer::buffer::{bitwise_bin_op_helper, bitwise_quaternary_op_helper};
use arrow_buffer::{buffer_bin_and_not, BooleanBuffer, NullBuffer};
use arrow_schema::ArrowError;

/// Logical 'and' boolean values with Kleene logic
///
/// # Behavior
///
/// This function behaves as follows with nulls:
///
/// * `true` and `null` = `null`
/// * `null` and `true` = `null`
/// * `false` and `null` = `false`
/// * `null` and `false` = `false`
/// * `null` and `null` = `null`
///
/// In other words, in this context a null value really means \"unknown\",
/// and an unknown value 'and' false is always false.
/// For a different null behavior, see function \"and\".
///
/// # Example
///
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::and_kleene;
/// let a = BooleanArray::from(vec![Some(true), Some(false), None]);
/// let b = BooleanArray::from(vec![None, None, None]);
/// let and_ab = and_kleene(&a, &b).unwrap();
/// assert_eq!(and_ab, BooleanArray::from(vec![None, Some(false), None]));
/// ```
///
/// # Fails
///
/// If the operands have different lengths
pub fn and_kleene(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }

    let left_values = left.values();
    let right_values = right.values();

    let buffer = match (left.nulls(), right.nulls()) {
        (None, None) => None,
        (Some(left_null_buffer), None) => {
            // The right side has no null values.
            // The final null bit is set only if:
            // 1. left null bit is set, or
            // 2. right data bit is false (because null AND false = false).
            Some(bitwise_bin_op_helper(
                left_null_buffer.buffer(),
                left_null_buffer.offset(),
                right_values.inner(),
                right_values.offset(),
                left.len(),
                |a, b| a | !b,
            ))
        }
        (None, Some(right_null_buffer)) => {
            // Same as above
            Some(bitwise_bin_op_helper(
                right_null_buffer.buffer(),
                right_null_buffer.offset(),
                left_values.inner(),
                left_values.offset(),
                left.len(),
                |a, b| a | !b,
            ))
        }
        (Some(left_null_buffer), Some(right_null_buffer)) => {
            // Follow the same logic above. Both sides have null values.
            // Assume a is left null bits, b is left data bits, c is right null bits,
            // d is right data bits.
            // The final null bits are:
            // (a | (c & !d)) & (c | (a & !b))
            Some(bitwise_quaternary_op_helper(
                [
                    left_null_buffer.buffer(),
                    left_values.inner(),
                    right_null_buffer.buffer(),
                    right_values.inner(),
                ],
                [
                    left_null_buffer.offset(),
                    left_values.offset(),
                    right_null_buffer.offset(),
                    right_values.offset(),
                ],
                left.len(),
                |a, b, c, d| (a | (c & !d)) & (c | (a & !b)),
            ))
        }
    };
    let nulls = buffer.map(|b| NullBuffer::new(BooleanBuffer::new(b, 0, left.len())));
    Ok(BooleanArray::new(left_values & right_values, nulls))
}

/// Logical 'or' boolean values with Kleene logic
///
/// # Behavior
///
/// This function behaves as follows with nulls:
///
/// * `true` or `null` = `true`
/// * `null` or `true` = `true`
/// * `false` or `null` = `null`
/// * `null` or `false` = `null`
/// * `null` or `null` = `null`
///
/// In other words, in this context a null value really means \"unknown\",
/// and an unknown value 'or' true is always true.
/// For a different null behavior, see function \"or\".
///
/// # Example
///
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::or_kleene;
/// let a = BooleanArray::from(vec![Some(true), Some(false), None]);
/// let b = BooleanArray::from(vec![None, None, None]);
/// let or_ab = or_kleene(&a, &b).unwrap();
/// assert_eq!(or_ab, BooleanArray::from(vec![Some(true), None, None]));
/// ```
///
/// # Fails
///
/// If the operands have different lengths
pub fn or_kleene(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }

    let left_values = left.values();
    let right_values = right.values();

    let buffer = match (left.nulls(), right.nulls()) {
        (None, None) => None,
        (Some(left_nulls), None) => {
            // The right side has no null values.
            // The final null bit is set only if:
            // 1. left null bit is set, or
            // 2. right data bit is true (because null OR true = true).
            Some(bitwise_bin_op_helper(
                left_nulls.buffer(),
                left_nulls.offset(),
                right_values.inner(),
                right_values.offset(),
                left.len(),
                |a, b| a | b,
            ))
        }
        (None, Some(right_nulls)) => {
            // Same as above
            Some(bitwise_bin_op_helper(
                right_nulls.buffer(),
                right_nulls.offset(),
                left_values.inner(),
                left_values.offset(),
                left.len(),
                |a, b| a | b,
            ))
        }
        (Some(left_nulls), Some(right_nulls)) => {
            // Follow the same logic above. Both sides have null values.
            // Assume a is left null bits, b is left data bits, c is right null bits,
            // d is right data bits.
            // The final null bits are:
            // (a | (c & d)) & (c | (a & b))
            Some(bitwise_quaternary_op_helper(
                [
                    left_nulls.buffer(),
                    left_values.inner(),
                    right_nulls.buffer(),
                    right_values.inner(),
                ],
                [
                    left_nulls.offset(),
                    left_values.offset(),
                    right_nulls.offset(),
                    right_values.offset(),
                ],
                left.len(),
                |a, b, c, d| (a | (c & d)) & (c | (a & b)),
            ))
        }
    };

    let nulls = buffer.map(|b| NullBuffer::new(BooleanBuffer::new(b, 0, left.len())));
    Ok(BooleanArray::new(left_values | right_values, nulls))
}

/// Helper function to implement binary kernels
pub(crate) fn binary_boolean_kernel<F>(
    left: &BooleanArray,
    right: &BooleanArray,
    op: F,
) -> Result<BooleanArray, ArrowError>
where
    F: Fn(&BooleanBuffer, &BooleanBuffer) -> BooleanBuffer,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform bitwise operation on arrays of different length".to_string(),
        ));
    }

    let nulls = NullBuffer::union(left.nulls(), right.nulls());
    let values = op(left.values(), right.values());
    Ok(BooleanArray::new(values, nulls))
}

/// Performs `AND` operation on two arrays. If either left or right value is null then the
/// result is also null.
/// # Error
/// This function errors when the arrays have different lengths.
/// # Example
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::and;
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let b = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);
/// let and_ab = and(&a, &b).unwrap();
/// assert_eq!(and_ab, BooleanArray::from(vec![Some(false), Some(true), None]));
/// ```
pub fn and(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    binary_boolean_kernel(left, right, |a, b| a & b)
}

/// Performs `OR` operation on two arrays. If either left or right value is null then the
/// result is also null.
/// # Error
/// This function errors when the arrays have different lengths.
/// # Example
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::or;
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let b = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);
/// let or_ab = or(&a, &b).unwrap();
/// assert_eq!(or_ab, BooleanArray::from(vec![Some(true), Some(true), None]));
/// ```
pub fn or(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    binary_boolean_kernel(left, right, |a, b| a | b)
}

/// Performs `AND_NOT` operation on two arrays. If either left or right value is null then the
/// result is also null.
/// # Error
/// This function errors when the arrays have different lengths.
/// # Example
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::{and, not, and_not};
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let b = BooleanArray::from(vec![Some(true), Some(true), Some(false)]);
/// let andn_ab = and_not(&a, &b).unwrap();
/// assert_eq!(andn_ab, BooleanArray::from(vec![Some(false), Some(false), None]));
/// // It's equal to and(left, not(right))
/// assert_eq!(andn_ab, and(&a, &not(&b).unwrap()).unwrap());
pub fn and_not(left: &BooleanArray, right: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    binary_boolean_kernel(left, right, |a, b| {
        let buffer = buffer_bin_and_not(a.inner(), b.offset(), b.inner(), a.offset(), a.len());
        BooleanBuffer::new(buffer, left.offset(), left.len())
    })
}

/// Performs unary `NOT` operation on an arrays. If value is null then the result is also
/// null.
/// # Error
/// This function never errors. It returns an error for consistency.
/// # Example
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::not;
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let not_a = not(&a).unwrap();
/// assert_eq!(not_a, BooleanArray::from(vec![Some(true), Some(false), None]));
/// ```
pub fn not(left: &BooleanArray) -> Result<BooleanArray, ArrowError> {
    let nulls = left.nulls().cloned();
    let values = !left.values();
    Ok(BooleanArray::new(values, nulls))
}

/// Returns a non-null [BooleanArray] with whether each value of the array is null.
/// # Error
/// This function never errors.
/// # Example
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::is_null;
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let a_is_null = is_null(&a).unwrap();
/// assert_eq!(a_is_null, BooleanArray::from(vec![false, false, true]));
/// ```
pub fn is_null(input: &dyn Array) -> Result<BooleanArray, ArrowError> {
    let values = match input.logical_nulls() {
        None => BooleanBuffer::new_unset(input.len()),
        Some(nulls) => !nulls.inner(),
    };

    Ok(BooleanArray::new(values, None))
}

/// Returns a non-null [BooleanArray] with whether each value of the array is not null.
/// # Error
/// This function never errors.
/// # Example
/// ```rust
/// # use arrow_array::BooleanArray;
/// # use arrow_arith::boolean::is_not_null;
/// let a = BooleanArray::from(vec![Some(false), Some(true), None]);
/// let a_is_not_null = is_not_null(&a).unwrap();
/// assert_eq!(a_is_not_null, BooleanArray::from(vec![true, true, false]));
/// ```
pub fn is_not_null(input: &dyn Array) -> Result<BooleanArray, ArrowError> {
    let values = match input.logical_nulls() {
        None => BooleanBuffer::new_set(input.len()),
        Some(n) => n.inner().clone(),
    };
    Ok(BooleanArray::new(values, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_bool_array_and() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = or(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![false, true, true, true]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_and_not() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![false, true, false, true]);
        let c = and_not(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![false, false, true, false]);

        assert_eq!(c, expected);
        assert_eq!(c, and(&a, &not(&b).unwrap()).unwrap());
    }

    #[test]
    fn test_bool_array_or_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = or(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            None,
            None,
            None,
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_boolean_array_kleene_no_remainder() {
        let n = 1024;
        let a = BooleanArray::from(vec![true; n]);
        let b = BooleanArray::from(vec![None; n]);
        let result = or_kleene(&a, &b).unwrap();

        assert_eq!(result, a);
    }

    #[test]
    fn test_bool_array_and_kleene_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = and_kleene(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            Some(false),
            None,
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or_kleene_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = or_kleene(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            None,
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or_kleene_right_sided_nulls() {
        let a = BooleanArray::from(vec![false, false, false, true, true, true]);

        // ensure null bitmap of a is absent
        assert!(a.nulls().is_none());

        let b = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);

        // ensure null bitmap of b is present
        assert!(b.nulls().is_some());

        let c = or_kleene(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_or_kleene_left_sided_nulls() {
        let a = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ]);

        // ensure null bitmap of b is absent
        assert!(a.nulls().is_some());

        let b = BooleanArray::from(vec![false, false, false, true, true, true]);

        // ensure null bitmap of a is present
        assert!(b.nulls().is_none());

        let c = or_kleene(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(true),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_not() {
        let a = BooleanArray::from(vec![false, true]);
        let c = not(&a).unwrap();

        let expected = BooleanArray::from(vec![true, false]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_not_sliced() {
        let a = BooleanArray::from(vec![None, Some(true), Some(false), None, Some(true)]);
        let a = a.slice(1, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();
        let c = not(a).unwrap();

        let expected = BooleanArray::from(vec![Some(false), Some(true), None, Some(false)]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_and_nulls() {
        let a = BooleanArray::from(vec![
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);
        let b = BooleanArray::from(vec![
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
        ]);
        let c = and(&a, &b).unwrap();

        let expected = BooleanArray::from(vec![
            None,
            None,
            None,
            None,
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(true),
        ]);

        assert_eq!(c, expected);
    }

    #[test]
    fn test_bool_array_and_sliced_same_offset() {
        let a = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, false, true, true,
        ]);
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, true, false, true,
        ]);

        let a = a.slice(8, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();
        let b = b.slice(8, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(a, b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_sliced_same_offset_mod8() {
        let a = BooleanArray::from(vec![
            false, false, true, true, false, false, false, false, false, false, false, false,
        ]);
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, true, false, true,
        ]);

        let a = a.slice(0, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();
        let b = b.slice(8, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(a, b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_sliced_offset1() {
        let a = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, false, true, true,
        ]);
        let b = BooleanArray::from(vec![false, true, false, true]);

        let a = a.slice(8, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(a, &b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_sliced_offset2() {
        let a = BooleanArray::from(vec![false, false, true, true]);
        let b = BooleanArray::from(vec![
            false, false, false, false, false, false, false, false, false, true, false, true,
        ]);

        let b = b.slice(8, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(&a, b).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, true]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_bool_array_and_nulls_offset() {
        let a = BooleanArray::from(vec![None, Some(false), Some(true), None, Some(true)]);
        let a = a.slice(1, 4);
        let a = a.as_any().downcast_ref::<BooleanArray>().unwrap();

        let b = BooleanArray::from(vec![
            None,
            None,
            Some(true),
            Some(false),
            Some(true),
            Some(true),
        ]);

        let b = b.slice(2, 4);
        let b = b.as_any().downcast_ref::<BooleanArray>().unwrap();

        let c = and(a, b).unwrap();

        let expected = BooleanArray::from(vec![Some(false), Some(false), None, Some(true)]);

        assert_eq!(expected, c);
    }

    #[test]
    fn test_nonnull_array_is_null() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));

        let res = is_null(a.as_ref()).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, false]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nonnull_array_with_offset_is_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1]);
        let a = a.slice(8, 4);

        let res = is_null(&a).unwrap();

        let expected = BooleanArray::from(vec![false, false, false, false]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nonnull_array_is_not_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4]);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, true, true, true]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nonnull_array_with_offset_is_not_null() {
        let a = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 7, 6, 5, 4, 3, 2, 1]);
        let a = a.slice(8, 4);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, true, true, true]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nullable_array_is_null() {
        let a = Int32Array::from(vec![Some(1), None, Some(3), None]);

        let res = is_null(&a).unwrap();

        let expected = BooleanArray::from(vec![false, true, false, true]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nullable_array_with_offset_is_null() {
        let a = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            // offset 8, previous None values are skipped by the slice
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
            None,
            None,
        ]);
        let a = a.slice(8, 4);

        let res = is_null(&a).unwrap();

        let expected = BooleanArray::from(vec![false, true, false, true]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nullable_array_is_not_null() {
        let a = Int32Array::from(vec![Some(1), None, Some(3), None]);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, false, true, false]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_nullable_array_with_offset_is_not_null() {
        let a = Int32Array::from(vec![
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            // offset 8, previous None values are skipped by the slice
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
            None,
            None,
        ]);
        let a = a.slice(8, 4);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, false, true, false]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_null_array_is_null() {
        let a = NullArray::new(3);

        let res = is_null(&a).unwrap();

        let expected = BooleanArray::from(vec![true, true, true]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }

    #[test]
    fn test_null_array_is_not_null() {
        let a = NullArray::new(3);

        let res = is_not_null(&a).unwrap();

        let expected = BooleanArray::from(vec![false, false, false]);

        assert_eq!(expected, res);
        assert!(res.nulls().is_none());
    }
}
