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

use crate::arity::{binary, unary};
use arrow_array::*;
use arrow_buffer::ArrowNativeType;
use arrow_schema::ArrowError;
use num::traits::{WrappingShl, WrappingShr};
use std::ops::{BitAnd, BitOr, BitXor, Not};

/// The helper function for bitwise operation with two array
fn bitwise_op<T, F>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
    op: F,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    binary(left, right, op)
}

/// Perform `left & right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn bitwise_and<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: BitAnd<Output = T::Native>,
{
    bitwise_op(left, right, |a, b| a & b)
}

/// Perform `left | right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn bitwise_or<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: BitOr<Output = T::Native>,
{
    bitwise_op(left, right, |a, b| a | b)
}

/// Perform `left ^ right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn bitwise_xor<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: BitXor<Output = T::Native>,
{
    bitwise_op(left, right, |a, b| a ^ b)
}

/// Perform bitwise `left << right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn bitwise_shift_left<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: WrappingShl<Output = T::Native>,
{
    bitwise_op(left, right, |a, b| {
        let b = b.as_usize();
        a.wrapping_shl(b as u32)
    })
}

/// Perform bitwise `left >> right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn bitwise_shift_right<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: WrappingShr<Output = T::Native>,
{
    bitwise_op(left, right, |a, b| {
        let b = b.as_usize();
        a.wrapping_shr(b as u32)
    })
}

/// Perform `!array` operation on array. If array value is null
/// then the result is also null.
pub fn bitwise_not<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: Not<Output = T::Native>,
{
    Ok(unary(array, |value| !value))
}

/// Perform `left & !right` operation on two arrays. If either left or right value is null
/// then the result is also null.
pub fn bitwise_and_not<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: BitAnd<Output = T::Native>,
    T::Native: Not<Output = T::Native>,
{
    bitwise_op(left, right, |a, b| a & !b)
}

/// Perform bitwise `and` every value in an array with the scalar. If any value in the array is null then the
/// result is also null.
pub fn bitwise_and_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: BitAnd<Output = T::Native>,
{
    Ok(unary(array, |value| value & scalar))
}

/// Perform bitwise `or` every value in an array with the scalar. If any value in the array is null then the
/// result is also null.
pub fn bitwise_or_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: BitOr<Output = T::Native>,
{
    Ok(unary(array, |value| value | scalar))
}

/// Perform bitwise `xor` every value in an array with the scalar. If any value in the array is null then the
/// result is also null.
pub fn bitwise_xor_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: BitXor<Output = T::Native>,
{
    Ok(unary(array, |value| value ^ scalar))
}

/// Perform bitwise `left << right` every value in an array with the scalar. If any value in the array is null then the
/// result is also null.
pub fn bitwise_shift_left_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: WrappingShl<Output = T::Native>,
{
    Ok(unary(array, |value| {
        let scalar = scalar.as_usize();
        value.wrapping_shl(scalar as u32)
    }))
}

/// Perform bitwise `left >> right` every value in an array with the scalar. If any value in the array is null then the
/// result is also null.
pub fn bitwise_shift_right_scalar<T>(
    array: &PrimitiveArray<T>,
    scalar: T::Native,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowNumericType,
    T::Native: WrappingShr<Output = T::Native>,
{
    Ok(unary(array, |value| {
        let scalar = scalar.as_usize();
        value.wrapping_shr(scalar as u32)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitwise_and_array() -> Result<(), ArrowError> {
        // unsigned value
        let left = UInt64Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let right = UInt64Array::from(vec![Some(5), Some(10), Some(8), Some(12)]);
        let expected = UInt64Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let result = bitwise_and(&left, &right)?;
        assert_eq!(expected, result);

        // signed value
        let left = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let right = Int32Array::from(vec![Some(5), Some(-10), Some(8), Some(12)]);
        let expected = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let result = bitwise_and(&left, &right)?;
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_bitwise_shift_left() {
        let left = UInt64Array::from(vec![Some(1), Some(2), None, Some(4), Some(8)]);
        let right = UInt64Array::from(vec![Some(5), Some(10), Some(8), Some(12), Some(u64::MAX)]);
        let expected = UInt64Array::from(vec![Some(32), Some(2048), None, Some(16384), Some(0)]);
        let result = bitwise_shift_left(&left, &right).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_bitwise_shift_left_scalar() {
        let left = UInt64Array::from(vec![Some(1), Some(2), None, Some(4), Some(8)]);
        let scalar = 2;
        let expected = UInt64Array::from(vec![Some(4), Some(8), None, Some(16), Some(32)]);
        let result = bitwise_shift_left_scalar(&left, scalar).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_bitwise_shift_right() {
        let left = UInt64Array::from(vec![Some(32), Some(2048), None, Some(16384), Some(3)]);
        let right = UInt64Array::from(vec![Some(5), Some(10), Some(8), Some(12), Some(65)]);
        let expected = UInt64Array::from(vec![Some(1), Some(2), None, Some(4), Some(1)]);
        let result = bitwise_shift_right(&left, &right).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_bitwise_shift_right_scalar() {
        let left = UInt64Array::from(vec![Some(32), Some(2048), None, Some(16384), Some(3)]);
        let scalar = 2;
        let expected = UInt64Array::from(vec![Some(8), Some(512), None, Some(4096), Some(0)]);
        let result = bitwise_shift_right_scalar(&left, scalar).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_bitwise_and_array_scalar() {
        // unsigned value
        let left = UInt64Array::from(vec![Some(15), Some(2), None, Some(4)]);
        let scalar = 7;
        let expected = UInt64Array::from(vec![Some(7), Some(2), None, Some(4)]);
        let result = bitwise_and_scalar(&left, scalar).unwrap();
        assert_eq!(expected, result);

        // signed value
        let left = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let scalar = -20;
        let expected = Int32Array::from(vec![Some(0), Some(0), None, Some(4)]);
        let result = bitwise_and_scalar(&left, scalar).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_bitwise_or_array() {
        // unsigned value
        let left = UInt64Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let right = UInt64Array::from(vec![Some(7), Some(5), Some(8), Some(13)]);
        let expected = UInt64Array::from(vec![Some(7), Some(7), None, Some(13)]);
        let result = bitwise_or(&left, &right).unwrap();
        assert_eq!(expected, result);

        // signed value
        let left = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let right = Int32Array::from(vec![Some(-7), Some(-5), Some(8), Some(13)]);
        let expected = Int32Array::from(vec![Some(-7), Some(-5), None, Some(13)]);
        let result = bitwise_or(&left, &right).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_bitwise_not_array() {
        // unsigned value
        let array = UInt64Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let expected = UInt64Array::from(vec![
            Some(18446744073709551614),
            Some(18446744073709551613),
            None,
            Some(18446744073709551611),
        ]);
        let result = bitwise_not(&array).unwrap();
        assert_eq!(expected, result);
        // signed value
        let array = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let expected = Int32Array::from(vec![Some(-2), Some(-3), None, Some(-5)]);
        let result = bitwise_not(&array).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_bitwise_and_not_array() {
        // unsigned value
        let left = UInt64Array::from(vec![Some(8), Some(2), None, Some(4)]);
        let right = UInt64Array::from(vec![Some(7), Some(5), Some(8), Some(13)]);
        let expected = UInt64Array::from(vec![Some(8), Some(2), None, Some(0)]);
        let result = bitwise_and_not(&left, &right).unwrap();
        assert_eq!(expected, result);
        assert_eq!(
            bitwise_and(&left, &bitwise_not(&right).unwrap()).unwrap(),
            result
        );

        // signed value
        let left = Int32Array::from(vec![Some(2), Some(1), None, Some(3)]);
        let right = Int32Array::from(vec![Some(-7), Some(-5), Some(8), Some(13)]);
        let expected = Int32Array::from(vec![Some(2), Some(0), None, Some(2)]);
        let result = bitwise_and_not(&left, &right).unwrap();
        assert_eq!(expected, result);
        assert_eq!(
            bitwise_and(&left, &bitwise_not(&right).unwrap()).unwrap(),
            result
        );
    }

    #[test]
    fn test_bitwise_or_array_scalar() {
        // unsigned value
        let left = UInt64Array::from(vec![Some(15), Some(2), None, Some(4)]);
        let scalar = 7;
        let expected = UInt64Array::from(vec![Some(15), Some(7), None, Some(7)]);
        let result = bitwise_or_scalar(&left, scalar).unwrap();
        assert_eq!(expected, result);

        // signed value
        let left = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let scalar = 20;
        let expected = Int32Array::from(vec![Some(21), Some(22), None, Some(20)]);
        let result = bitwise_or_scalar(&left, scalar).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_bitwise_xor_array() {
        // unsigned value
        let left = UInt64Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let right = UInt64Array::from(vec![Some(7), Some(5), Some(8), Some(13)]);
        let expected = UInt64Array::from(vec![Some(6), Some(7), None, Some(9)]);
        let result = bitwise_xor(&left, &right).unwrap();
        assert_eq!(expected, result);

        // signed value
        let left = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let right = Int32Array::from(vec![Some(-7), Some(5), Some(8), Some(-13)]);
        let expected = Int32Array::from(vec![Some(-8), Some(7), None, Some(-9)]);
        let result = bitwise_xor(&left, &right).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_bitwise_xor_array_scalar() {
        // unsigned value
        let left = UInt64Array::from(vec![Some(15), Some(2), None, Some(4)]);
        let scalar = 7;
        let expected = UInt64Array::from(vec![Some(8), Some(5), None, Some(3)]);
        let result = bitwise_xor_scalar(&left, scalar).unwrap();
        assert_eq!(expected, result);

        // signed value
        let left = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let scalar = -20;
        let expected = Int32Array::from(vec![Some(-19), Some(-18), None, Some(-24)]);
        let result = bitwise_xor_scalar(&left, scalar).unwrap();
        assert_eq!(expected, result);
    }
}
