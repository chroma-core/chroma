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

//! Defines numeric arithmetic kernels on [`PrimitiveArray`], such as [`add`]

use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::timezone::Tz;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::{ArrowNativeType, IntervalDayTime, IntervalMonthDayNano};
use arrow_schema::{ArrowError, DataType, IntervalUnit, TimeUnit};

use crate::arity::{binary, try_binary};

/// Perform `lhs + rhs`, returning an error on overflow
pub fn add(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::Add, lhs, rhs)
}

/// Perform `lhs + rhs`, wrapping on overflow for [`DataType::is_integer`]
pub fn add_wrapping(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::AddWrapping, lhs, rhs)
}

/// Perform `lhs - rhs`, returning an error on overflow
pub fn sub(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::Sub, lhs, rhs)
}

/// Perform `lhs - rhs`, wrapping on overflow for [`DataType::is_integer`]
pub fn sub_wrapping(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::SubWrapping, lhs, rhs)
}

/// Perform `lhs * rhs`, returning an error on overflow
pub fn mul(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::Mul, lhs, rhs)
}

/// Perform `lhs * rhs`, wrapping on overflow for [`DataType::is_integer`]
pub fn mul_wrapping(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::MulWrapping, lhs, rhs)
}

/// Perform `lhs / rhs`
///
/// Overflow or division by zero will result in an error, with exception to
/// floating point numbers, which instead follow the IEEE 754 rules
pub fn div(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::Div, lhs, rhs)
}

/// Perform `lhs % rhs`
///
/// Overflow or division by zero will result in an error, with exception to
/// floating point numbers, which instead follow the IEEE 754 rules
pub fn rem(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    arithmetic_op(Op::Rem, lhs, rhs)
}

macro_rules! neg_checked {
    ($t:ty, $a:ident) => {{
        let array = $a
            .as_primitive::<$t>()
            .try_unary::<_, $t, _>(|x| x.neg_checked())?;
        Ok(Arc::new(array))
    }};
}

macro_rules! neg_wrapping {
    ($t:ty, $a:ident) => {{
        let array = $a.as_primitive::<$t>().unary::<_, $t>(|x| x.neg_wrapping());
        Ok(Arc::new(array))
    }};
}

/// Negates each element of  `array`, returning an error on overflow
///
/// Note: negation of unsigned arrays is not supported and will return in an error,
/// for wrapping unsigned negation consider using [`neg_wrapping`][neg_wrapping()]
pub fn neg(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    use IntervalUnit::*;
    use TimeUnit::*;

    match array.data_type() {
        Int8 => neg_checked!(Int8Type, array),
        Int16 => neg_checked!(Int16Type, array),
        Int32 => neg_checked!(Int32Type, array),
        Int64 => neg_checked!(Int64Type, array),
        Float16 => neg_wrapping!(Float16Type, array),
        Float32 => neg_wrapping!(Float32Type, array),
        Float64 => neg_wrapping!(Float64Type, array),
        Decimal128(p, s) => {
            let a = array
                .as_primitive::<Decimal128Type>()
                .try_unary::<_, Decimal128Type, _>(|x| x.neg_checked())?;

            Ok(Arc::new(a.with_precision_and_scale(*p, *s)?))
        }
        Decimal256(p, s) => {
            let a = array
                .as_primitive::<Decimal256Type>()
                .try_unary::<_, Decimal256Type, _>(|x| x.neg_checked())?;

            Ok(Arc::new(a.with_precision_and_scale(*p, *s)?))
        }
        Duration(Second) => neg_checked!(DurationSecondType, array),
        Duration(Millisecond) => neg_checked!(DurationMillisecondType, array),
        Duration(Microsecond) => neg_checked!(DurationMicrosecondType, array),
        Duration(Nanosecond) => neg_checked!(DurationNanosecondType, array),
        Interval(YearMonth) => neg_checked!(IntervalYearMonthType, array),
        Interval(DayTime) => {
            let a = array
                .as_primitive::<IntervalDayTimeType>()
                .try_unary::<_, IntervalDayTimeType, ArrowError>(|x| {
                    let (days, ms) = IntervalDayTimeType::to_parts(x);
                    Ok(IntervalDayTimeType::make_value(
                        days.neg_checked()?,
                        ms.neg_checked()?,
                    ))
                })?;
            Ok(Arc::new(a))
        }
        Interval(MonthDayNano) => {
            let a = array
                .as_primitive::<IntervalMonthDayNanoType>()
                .try_unary::<_, IntervalMonthDayNanoType, ArrowError>(|x| {
                    let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(x);
                    Ok(IntervalMonthDayNanoType::make_value(
                        months.neg_checked()?,
                        days.neg_checked()?,
                        nanos.neg_checked()?,
                    ))
                })?;
            Ok(Arc::new(a))
        }
        t => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid arithmetic operation: !{t}"
        ))),
    }
}

/// Negates each element of  `array`, wrapping on overflow for [`DataType::is_integer`]
pub fn neg_wrapping(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    downcast_integer! {
        array.data_type() => (neg_wrapping, array),
        _ => neg(array),
    }
}

/// An enumeration of arithmetic operations
///
/// This allows sharing the type dispatch logic across the various kernels
#[derive(Debug, Copy, Clone)]
enum Op {
    AddWrapping,
    Add,
    SubWrapping,
    Sub,
    MulWrapping,
    Mul,
    Div,
    Rem,
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::AddWrapping | Op::Add => write!(f, "+"),
            Op::SubWrapping | Op::Sub => write!(f, "-"),
            Op::MulWrapping | Op::Mul => write!(f, "*"),
            Op::Div => write!(f, "/"),
            Op::Rem => write!(f, "%"),
        }
    }
}

impl Op {
    fn commutative(&self) -> bool {
        matches!(self, Self::Add | Self::AddWrapping)
    }
}

/// Dispatch the given `op` to the appropriate specialized kernel
fn arithmetic_op(op: Op, lhs: &dyn Datum, rhs: &dyn Datum) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    use IntervalUnit::*;
    use TimeUnit::*;

    macro_rules! integer_helper {
        ($t:ty, $op:ident, $l:ident, $l_scalar:ident, $r:ident, $r_scalar:ident) => {
            integer_op::<$t>($op, $l, $l_scalar, $r, $r_scalar)
        };
    }

    let (l, l_scalar) = lhs.get();
    let (r, r_scalar) = rhs.get();
    downcast_integer! {
        l.data_type(), r.data_type() => (integer_helper, op, l, l_scalar, r, r_scalar),
        (Float16, Float16) => float_op::<Float16Type>(op, l, l_scalar, r, r_scalar),
        (Float32, Float32) => float_op::<Float32Type>(op, l, l_scalar, r, r_scalar),
        (Float64, Float64) => float_op::<Float64Type>(op, l, l_scalar, r, r_scalar),
        (Timestamp(Second, _), _) => timestamp_op::<TimestampSecondType>(op, l, l_scalar, r, r_scalar),
        (Timestamp(Millisecond, _), _) => timestamp_op::<TimestampMillisecondType>(op, l, l_scalar, r, r_scalar),
        (Timestamp(Microsecond, _), _) => timestamp_op::<TimestampMicrosecondType>(op, l, l_scalar, r, r_scalar),
        (Timestamp(Nanosecond, _), _) => timestamp_op::<TimestampNanosecondType>(op, l, l_scalar, r, r_scalar),
        (Duration(Second), Duration(Second)) => duration_op::<DurationSecondType>(op, l, l_scalar, r, r_scalar),
        (Duration(Millisecond), Duration(Millisecond)) => duration_op::<DurationMillisecondType>(op, l, l_scalar, r, r_scalar),
        (Duration(Microsecond), Duration(Microsecond)) => duration_op::<DurationMicrosecondType>(op, l, l_scalar, r, r_scalar),
        (Duration(Nanosecond), Duration(Nanosecond)) => duration_op::<DurationNanosecondType>(op, l, l_scalar, r, r_scalar),
        (Interval(YearMonth), Interval(YearMonth)) => interval_op::<IntervalYearMonthType>(op, l, l_scalar, r, r_scalar),
        (Interval(DayTime), Interval(DayTime)) => interval_op::<IntervalDayTimeType>(op, l, l_scalar, r, r_scalar),
        (Interval(MonthDayNano), Interval(MonthDayNano)) => interval_op::<IntervalMonthDayNanoType>(op, l, l_scalar, r, r_scalar),
        (Date32, _) => date_op::<Date32Type>(op, l, l_scalar, r, r_scalar),
        (Date64, _) => date_op::<Date64Type>(op, l, l_scalar, r, r_scalar),
        (Decimal128(_, _), Decimal128(_, _)) => decimal_op::<Decimal128Type>(op, l, l_scalar, r, r_scalar),
        (Decimal256(_, _), Decimal256(_, _)) => decimal_op::<Decimal256Type>(op, l, l_scalar, r, r_scalar),
        (l_t, r_t) => match (l_t, r_t) {
            (Duration(_) | Interval(_), Date32 | Date64 | Timestamp(_, _)) if op.commutative() => {
                arithmetic_op(op, rhs, lhs)
            }
            _ => Err(ArrowError::InvalidArgumentError(
              format!("Invalid arithmetic operation: {l_t} {op} {r_t}")
            ))
        }
    }
}

/// Perform an infallible binary operation on potentially scalar inputs
macro_rules! op {
    ($l:ident, $l_s:expr, $r:ident, $r_s:expr, $op:expr) => {
        match ($l_s, $r_s) {
            (true, true) | (false, false) => binary($l, $r, |$l, $r| $op)?,
            (true, false) => match ($l.null_count() == 0).then(|| $l.value(0)) {
                None => PrimitiveArray::new_null($r.len()),
                Some($l) => $r.unary(|$r| $op),
            },
            (false, true) => match ($r.null_count() == 0).then(|| $r.value(0)) {
                None => PrimitiveArray::new_null($l.len()),
                Some($r) => $l.unary(|$l| $op),
            },
        }
    };
}

/// Same as `op` but with a type hint for the returned array
macro_rules! op_ref {
    ($t:ty, $l:ident, $l_s:expr, $r:ident, $r_s:expr, $op:expr) => {{
        let array: PrimitiveArray<$t> = op!($l, $l_s, $r, $r_s, $op);
        Arc::new(array)
    }};
}

/// Perform a fallible binary operation on potentially scalar inputs
macro_rules! try_op {
    ($l:ident, $l_s:expr, $r:ident, $r_s:expr, $op:expr) => {
        match ($l_s, $r_s) {
            (true, true) | (false, false) => try_binary($l, $r, |$l, $r| $op)?,
            (true, false) => match ($l.null_count() == 0).then(|| $l.value(0)) {
                None => PrimitiveArray::new_null($r.len()),
                Some($l) => $r.try_unary(|$r| $op)?,
            },
            (false, true) => match ($r.null_count() == 0).then(|| $r.value(0)) {
                None => PrimitiveArray::new_null($l.len()),
                Some($r) => $l.try_unary(|$l| $op)?,
            },
        }
    };
}

/// Same as `try_op` but with a type hint for the returned array
macro_rules! try_op_ref {
    ($t:ty, $l:ident, $l_s:expr, $r:ident, $r_s:expr, $op:expr) => {{
        let array: PrimitiveArray<$t> = try_op!($l, $l_s, $r, $r_s, $op);
        Arc::new(array)
    }};
}

/// Perform an arithmetic operation on integers
fn integer_op<T: ArrowPrimitiveType>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    let l = l.as_primitive::<T>();
    let r = r.as_primitive::<T>();
    let array: PrimitiveArray<T> = match op {
        Op::AddWrapping => op!(l, l_s, r, r_s, l.add_wrapping(r)),
        Op::Add => try_op!(l, l_s, r, r_s, l.add_checked(r)),
        Op::SubWrapping => op!(l, l_s, r, r_s, l.sub_wrapping(r)),
        Op::Sub => try_op!(l, l_s, r, r_s, l.sub_checked(r)),
        Op::MulWrapping => op!(l, l_s, r, r_s, l.mul_wrapping(r)),
        Op::Mul => try_op!(l, l_s, r, r_s, l.mul_checked(r)),
        Op::Div => try_op!(l, l_s, r, r_s, l.div_checked(r)),
        Op::Rem => try_op!(l, l_s, r, r_s, l.mod_checked(r)),
    };
    Ok(Arc::new(array))
}

/// Perform an arithmetic operation on floats
fn float_op<T: ArrowPrimitiveType>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    let l = l.as_primitive::<T>();
    let r = r.as_primitive::<T>();
    let array: PrimitiveArray<T> = match op {
        Op::AddWrapping | Op::Add => op!(l, l_s, r, r_s, l.add_wrapping(r)),
        Op::SubWrapping | Op::Sub => op!(l, l_s, r, r_s, l.sub_wrapping(r)),
        Op::MulWrapping | Op::Mul => op!(l, l_s, r, r_s, l.mul_wrapping(r)),
        Op::Div => op!(l, l_s, r, r_s, l.div_wrapping(r)),
        Op::Rem => op!(l, l_s, r, r_s, l.mod_wrapping(r)),
    };
    Ok(Arc::new(array))
}

/// Arithmetic trait for timestamp arrays
trait TimestampOp: ArrowTimestampType {
    type Duration: ArrowPrimitiveType<Native = i64>;

    fn add_year_month(timestamp: i64, delta: i32, tz: Tz) -> Option<i64>;
    fn add_day_time(timestamp: i64, delta: IntervalDayTime, tz: Tz) -> Option<i64>;
    fn add_month_day_nano(timestamp: i64, delta: IntervalMonthDayNano, tz: Tz) -> Option<i64>;

    fn sub_year_month(timestamp: i64, delta: i32, tz: Tz) -> Option<i64>;
    fn sub_day_time(timestamp: i64, delta: IntervalDayTime, tz: Tz) -> Option<i64>;
    fn sub_month_day_nano(timestamp: i64, delta: IntervalMonthDayNano, tz: Tz) -> Option<i64>;
}

macro_rules! timestamp {
    ($t:ty, $d:ty) => {
        impl TimestampOp for $t {
            type Duration = $d;

            fn add_year_month(left: i64, right: i32, tz: Tz) -> Option<i64> {
                Self::add_year_months(left, right, tz)
            }

            fn add_day_time(left: i64, right: IntervalDayTime, tz: Tz) -> Option<i64> {
                Self::add_day_time(left, right, tz)
            }

            fn add_month_day_nano(left: i64, right: IntervalMonthDayNano, tz: Tz) -> Option<i64> {
                Self::add_month_day_nano(left, right, tz)
            }

            fn sub_year_month(left: i64, right: i32, tz: Tz) -> Option<i64> {
                Self::subtract_year_months(left, right, tz)
            }

            fn sub_day_time(left: i64, right: IntervalDayTime, tz: Tz) -> Option<i64> {
                Self::subtract_day_time(left, right, tz)
            }

            fn sub_month_day_nano(left: i64, right: IntervalMonthDayNano, tz: Tz) -> Option<i64> {
                Self::subtract_month_day_nano(left, right, tz)
            }
        }
    };
}
timestamp!(TimestampSecondType, DurationSecondType);
timestamp!(TimestampMillisecondType, DurationMillisecondType);
timestamp!(TimestampMicrosecondType, DurationMicrosecondType);
timestamp!(TimestampNanosecondType, DurationNanosecondType);

/// Perform arithmetic operation on a timestamp array
fn timestamp_op<T: TimestampOp>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    use IntervalUnit::*;

    let l = l.as_primitive::<T>();
    let l_tz: Tz = l.timezone().unwrap_or("+00:00").parse()?;

    let array: PrimitiveArray<T> = match (op, r.data_type()) {
        (Op::Sub | Op::SubWrapping, Timestamp(unit, _)) if unit == &T::UNIT => {
            let r = r.as_primitive::<T>();
            return Ok(try_op_ref!(T::Duration, l, l_s, r, r_s, l.sub_checked(r)));
        }

        (Op::Add | Op::AddWrapping, Duration(unit)) if unit == &T::UNIT => {
            let r = r.as_primitive::<T::Duration>();
            try_op!(l, l_s, r, r_s, l.add_checked(r))
        }
        (Op::Sub | Op::SubWrapping, Duration(unit)) if unit == &T::UNIT => {
            let r = r.as_primitive::<T::Duration>();
            try_op!(l, l_s, r, r_s, l.sub_checked(r))
        }

        (Op::Add | Op::AddWrapping, Interval(YearMonth)) => {
            let r = r.as_primitive::<IntervalYearMonthType>();
            try_op!(
                l,
                l_s,
                r,
                r_s,
                T::add_year_month(l, r, l_tz).ok_or(ArrowError::ComputeError(
                    "Timestamp out of range".to_string()
                ))
            )
        }
        (Op::Sub | Op::SubWrapping, Interval(YearMonth)) => {
            let r = r.as_primitive::<IntervalYearMonthType>();
            try_op!(
                l,
                l_s,
                r,
                r_s,
                T::sub_year_month(l, r, l_tz).ok_or(ArrowError::ComputeError(
                    "Timestamp out of range".to_string()
                ))
            )
        }

        (Op::Add | Op::AddWrapping, Interval(DayTime)) => {
            let r = r.as_primitive::<IntervalDayTimeType>();
            try_op!(
                l,
                l_s,
                r,
                r_s,
                T::add_day_time(l, r, l_tz).ok_or(ArrowError::ComputeError(
                    "Timestamp out of range".to_string()
                ))
            )
        }
        (Op::Sub | Op::SubWrapping, Interval(DayTime)) => {
            let r = r.as_primitive::<IntervalDayTimeType>();
            try_op!(
                l,
                l_s,
                r,
                r_s,
                T::sub_day_time(l, r, l_tz).ok_or(ArrowError::ComputeError(
                    "Timestamp out of range".to_string()
                ))
            )
        }

        (Op::Add | Op::AddWrapping, Interval(MonthDayNano)) => {
            let r = r.as_primitive::<IntervalMonthDayNanoType>();
            try_op!(
                l,
                l_s,
                r,
                r_s,
                T::add_month_day_nano(l, r, l_tz).ok_or(ArrowError::ComputeError(
                    "Timestamp out of range".to_string()
                ))
            )
        }
        (Op::Sub | Op::SubWrapping, Interval(MonthDayNano)) => {
            let r = r.as_primitive::<IntervalMonthDayNanoType>();
            try_op!(
                l,
                l_s,
                r,
                r_s,
                T::sub_month_day_nano(l, r, l_tz).ok_or(ArrowError::ComputeError(
                    "Timestamp out of range".to_string()
                ))
            )
        }
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Invalid timestamp arithmetic operation: {} {op} {}",
                l.data_type(),
                r.data_type()
            )))
        }
    };
    Ok(Arc::new(array.with_timezone_opt(l.timezone())))
}

/// Arithmetic trait for date arrays
///
/// Note: these should be fallible (#4456)
trait DateOp: ArrowTemporalType {
    fn add_year_month(timestamp: Self::Native, delta: i32) -> Self::Native;
    fn add_day_time(timestamp: Self::Native, delta: IntervalDayTime) -> Self::Native;
    fn add_month_day_nano(timestamp: Self::Native, delta: IntervalMonthDayNano) -> Self::Native;

    fn sub_year_month(timestamp: Self::Native, delta: i32) -> Self::Native;
    fn sub_day_time(timestamp: Self::Native, delta: IntervalDayTime) -> Self::Native;
    fn sub_month_day_nano(timestamp: Self::Native, delta: IntervalMonthDayNano) -> Self::Native;
}

macro_rules! date {
    ($t:ty) => {
        impl DateOp for $t {
            fn add_year_month(left: Self::Native, right: i32) -> Self::Native {
                Self::add_year_months(left, right)
            }

            fn add_day_time(left: Self::Native, right: IntervalDayTime) -> Self::Native {
                Self::add_day_time(left, right)
            }

            fn add_month_day_nano(left: Self::Native, right: IntervalMonthDayNano) -> Self::Native {
                Self::add_month_day_nano(left, right)
            }

            fn sub_year_month(left: Self::Native, right: i32) -> Self::Native {
                Self::subtract_year_months(left, right)
            }

            fn sub_day_time(left: Self::Native, right: IntervalDayTime) -> Self::Native {
                Self::subtract_day_time(left, right)
            }

            fn sub_month_day_nano(left: Self::Native, right: IntervalMonthDayNano) -> Self::Native {
                Self::subtract_month_day_nano(left, right)
            }
        }
    };
}
date!(Date32Type);
date!(Date64Type);

/// Arithmetic trait for interval arrays
trait IntervalOp: ArrowPrimitiveType {
    fn add(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError>;
    fn sub(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError>;
}

impl IntervalOp for IntervalYearMonthType {
    fn add(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        left.add_checked(right)
    }

    fn sub(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        left.sub_checked(right)
    }
}

impl IntervalOp for IntervalDayTimeType {
    fn add(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        let (l_days, l_ms) = Self::to_parts(left);
        let (r_days, r_ms) = Self::to_parts(right);
        let days = l_days.add_checked(r_days)?;
        let ms = l_ms.add_checked(r_ms)?;
        Ok(Self::make_value(days, ms))
    }

    fn sub(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        let (l_days, l_ms) = Self::to_parts(left);
        let (r_days, r_ms) = Self::to_parts(right);
        let days = l_days.sub_checked(r_days)?;
        let ms = l_ms.sub_checked(r_ms)?;
        Ok(Self::make_value(days, ms))
    }
}

impl IntervalOp for IntervalMonthDayNanoType {
    fn add(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        let (l_months, l_days, l_nanos) = Self::to_parts(left);
        let (r_months, r_days, r_nanos) = Self::to_parts(right);
        let months = l_months.add_checked(r_months)?;
        let days = l_days.add_checked(r_days)?;
        let nanos = l_nanos.add_checked(r_nanos)?;
        Ok(Self::make_value(months, days, nanos))
    }

    fn sub(left: Self::Native, right: Self::Native) -> Result<Self::Native, ArrowError> {
        let (l_months, l_days, l_nanos) = Self::to_parts(left);
        let (r_months, r_days, r_nanos) = Self::to_parts(right);
        let months = l_months.sub_checked(r_months)?;
        let days = l_days.sub_checked(r_days)?;
        let nanos = l_nanos.sub_checked(r_nanos)?;
        Ok(Self::make_value(months, days, nanos))
    }
}

/// Perform arithmetic operation on an interval array
fn interval_op<T: IntervalOp>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    let l = l.as_primitive::<T>();
    let r = r.as_primitive::<T>();
    match op {
        Op::Add | Op::AddWrapping => Ok(try_op_ref!(T, l, l_s, r, r_s, T::add(l, r))),
        Op::Sub | Op::SubWrapping => Ok(try_op_ref!(T, l, l_s, r, r_s, T::sub(l, r))),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid interval arithmetic operation: {} {op} {}",
            l.data_type(),
            r.data_type()
        ))),
    }
}

fn duration_op<T: ArrowPrimitiveType>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    let l = l.as_primitive::<T>();
    let r = r.as_primitive::<T>();
    match op {
        Op::Add | Op::AddWrapping => Ok(try_op_ref!(T, l, l_s, r, r_s, l.add_checked(r))),
        Op::Sub | Op::SubWrapping => Ok(try_op_ref!(T, l, l_s, r, r_s, l.sub_checked(r))),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid duration arithmetic operation: {} {op} {}",
            l.data_type(),
            r.data_type()
        ))),
    }
}

/// Perform arithmetic operation on a date array
fn date_op<T: DateOp>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    use IntervalUnit::*;

    const NUM_SECONDS_IN_DAY: i64 = 60 * 60 * 24;

    let r_t = r.data_type();
    match (T::DATA_TYPE, op, r_t) {
        (Date32, Op::Sub | Op::SubWrapping, Date32) => {
            let l = l.as_primitive::<Date32Type>();
            let r = r.as_primitive::<Date32Type>();
            return Ok(op_ref!(
                DurationSecondType,
                l,
                l_s,
                r,
                r_s,
                ((l as i64) - (r as i64)) * NUM_SECONDS_IN_DAY
            ));
        }
        (Date64, Op::Sub | Op::SubWrapping, Date64) => {
            let l = l.as_primitive::<Date64Type>();
            let r = r.as_primitive::<Date64Type>();
            let result = try_op_ref!(DurationMillisecondType, l, l_s, r, r_s, l.sub_checked(r));
            return Ok(result);
        }
        _ => {}
    }

    let l = l.as_primitive::<T>();
    match (op, r_t) {
        (Op::Add | Op::AddWrapping, Interval(YearMonth)) => {
            let r = r.as_primitive::<IntervalYearMonthType>();
            Ok(op_ref!(T, l, l_s, r, r_s, T::add_year_month(l, r)))
        }
        (Op::Sub | Op::SubWrapping, Interval(YearMonth)) => {
            let r = r.as_primitive::<IntervalYearMonthType>();
            Ok(op_ref!(T, l, l_s, r, r_s, T::sub_year_month(l, r)))
        }

        (Op::Add | Op::AddWrapping, Interval(DayTime)) => {
            let r = r.as_primitive::<IntervalDayTimeType>();
            Ok(op_ref!(T, l, l_s, r, r_s, T::add_day_time(l, r)))
        }
        (Op::Sub | Op::SubWrapping, Interval(DayTime)) => {
            let r = r.as_primitive::<IntervalDayTimeType>();
            Ok(op_ref!(T, l, l_s, r, r_s, T::sub_day_time(l, r)))
        }

        (Op::Add | Op::AddWrapping, Interval(MonthDayNano)) => {
            let r = r.as_primitive::<IntervalMonthDayNanoType>();
            Ok(op_ref!(T, l, l_s, r, r_s, T::add_month_day_nano(l, r)))
        }
        (Op::Sub | Op::SubWrapping, Interval(MonthDayNano)) => {
            let r = r.as_primitive::<IntervalMonthDayNanoType>();
            Ok(op_ref!(T, l, l_s, r, r_s, T::sub_month_day_nano(l, r)))
        }

        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Invalid date arithmetic operation: {} {op} {}",
            l.data_type(),
            r.data_type()
        ))),
    }
}

/// Perform arithmetic operation on decimal arrays
fn decimal_op<T: DecimalType>(
    op: Op,
    l: &dyn Array,
    l_s: bool,
    r: &dyn Array,
    r_s: bool,
) -> Result<ArrayRef, ArrowError> {
    let l = l.as_primitive::<T>();
    let r = r.as_primitive::<T>();

    let (p1, s1, p2, s2) = match (l.data_type(), r.data_type()) {
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => (p1, s1, p2, s2),
        (DataType::Decimal256(p1, s1), DataType::Decimal256(p2, s2)) => (p1, s1, p2, s2),
        _ => unreachable!(),
    };

    // Follow the Hive decimal arithmetic rules
    // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
    let array: PrimitiveArray<T> = match op {
        Op::Add | Op::AddWrapping | Op::Sub | Op::SubWrapping => {
            // max(s1, s2)
            let result_scale = *s1.max(s2);

            // max(s1, s2) + max(p1-s1, p2-s2) + 1
            let result_precision =
                (result_scale.saturating_add((*p1 as i8 - s1).max(*p2 as i8 - s2)) as u8)
                    .saturating_add(1)
                    .min(T::MAX_PRECISION);

            let l_mul = T::Native::usize_as(10).pow_checked((result_scale - s1) as _)?;
            let r_mul = T::Native::usize_as(10).pow_checked((result_scale - s2) as _)?;

            match op {
                Op::Add | Op::AddWrapping => {
                    try_op!(
                        l,
                        l_s,
                        r,
                        r_s,
                        l.mul_checked(l_mul)?.add_checked(r.mul_checked(r_mul)?)
                    )
                }
                Op::Sub | Op::SubWrapping => {
                    try_op!(
                        l,
                        l_s,
                        r,
                        r_s,
                        l.mul_checked(l_mul)?.sub_checked(r.mul_checked(r_mul)?)
                    )
                }
                _ => unreachable!(),
            }
            .with_precision_and_scale(result_precision, result_scale)?
        }
        Op::Mul | Op::MulWrapping => {
            let result_precision = p1.saturating_add(p2 + 1).min(T::MAX_PRECISION);
            let result_scale = s1.saturating_add(*s2);
            if result_scale > T::MAX_SCALE {
                // SQL standard says that if the resulting scale of a multiply operation goes
                // beyond the maximum, rounding is not acceptable and thus an error occurs
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Output scale of {} {op} {} would exceed max scale of {}",
                    l.data_type(),
                    r.data_type(),
                    T::MAX_SCALE
                )));
            }

            try_op!(l, l_s, r, r_s, l.mul_checked(r))
                .with_precision_and_scale(result_precision, result_scale)?
        }

        Op::Div => {
            // Follow postgres and MySQL adding a fixed scale increment of 4
            // s1 + 4
            let result_scale = s1.saturating_add(4).min(T::MAX_SCALE);
            let mul_pow = result_scale - s1 + s2;

            // p1 - s1 + s2 + result_scale
            let result_precision = (mul_pow.saturating_add(*p1 as i8) as u8).min(T::MAX_PRECISION);

            let (l_mul, r_mul) = match mul_pow.cmp(&0) {
                Ordering::Greater => (
                    T::Native::usize_as(10).pow_checked(mul_pow as _)?,
                    T::Native::ONE,
                ),
                Ordering::Equal => (T::Native::ONE, T::Native::ONE),
                Ordering::Less => (
                    T::Native::ONE,
                    T::Native::usize_as(10).pow_checked(mul_pow.neg_wrapping() as _)?,
                ),
            };

            try_op!(
                l,
                l_s,
                r,
                r_s,
                l.mul_checked(l_mul)?.div_checked(r.mul_checked(r_mul)?)
            )
            .with_precision_and_scale(result_precision, result_scale)?
        }

        Op::Rem => {
            // max(s1, s2)
            let result_scale = *s1.max(s2);
            // min(p1-s1, p2 -s2) + max( s1,s2 )
            let result_precision =
                (result_scale.saturating_add((*p1 as i8 - s1).min(*p2 as i8 - s2)) as u8)
                    .min(T::MAX_PRECISION);

            let l_mul = T::Native::usize_as(10).pow_wrapping((result_scale - s1) as _);
            let r_mul = T::Native::usize_as(10).pow_wrapping((result_scale - s2) as _);

            try_op!(
                l,
                l_s,
                r,
                r_s,
                l.mul_checked(l_mul)?.mod_checked(r.mul_checked(r_mul)?)
            )
            .with_precision_and_scale(result_precision, result_scale)?
        }
    };

    Ok(Arc::new(array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::temporal_conversions::{as_date, as_datetime};
    use arrow_buffer::{i256, ScalarBuffer};
    use chrono::{DateTime, NaiveDate};

    fn test_neg_primitive<T: ArrowPrimitiveType>(
        input: &[T::Native],
        out: Result<&[T::Native], &str>,
    ) {
        let a = PrimitiveArray::<T>::new(ScalarBuffer::from(input.to_vec()), None);
        match out {
            Ok(expected) => {
                let result = neg(&a).unwrap();
                assert_eq!(result.as_primitive::<T>().values(), expected);
            }
            Err(e) => {
                let err = neg(&a).unwrap_err().to_string();
                assert_eq!(e, err);
            }
        }
    }

    #[test]
    fn test_neg() {
        let input = &[1, -5, 2, 693, 3929];
        let output = &[-1, 5, -2, -693, -3929];
        test_neg_primitive::<Int32Type>(input, Ok(output));

        let input = &[1, -5, 2, 693, 3929];
        let output = &[-1, 5, -2, -693, -3929];
        test_neg_primitive::<Int64Type>(input, Ok(output));
        test_neg_primitive::<DurationSecondType>(input, Ok(output));
        test_neg_primitive::<DurationMillisecondType>(input, Ok(output));
        test_neg_primitive::<DurationMicrosecondType>(input, Ok(output));
        test_neg_primitive::<DurationNanosecondType>(input, Ok(output));

        let input = &[f32::MAX, f32::MIN, f32::INFINITY, 1.3, 0.5];
        let output = &[f32::MIN, f32::MAX, f32::NEG_INFINITY, -1.3, -0.5];
        test_neg_primitive::<Float32Type>(input, Ok(output));

        test_neg_primitive::<Int32Type>(
            &[i32::MIN],
            Err("Compute error: Overflow happened on: - -2147483648"),
        );
        test_neg_primitive::<Int64Type>(
            &[i64::MIN],
            Err("Compute error: Overflow happened on: - -9223372036854775808"),
        );
        test_neg_primitive::<DurationSecondType>(
            &[i64::MIN],
            Err("Compute error: Overflow happened on: - -9223372036854775808"),
        );

        let r = neg_wrapping(&Int32Array::from(vec![i32::MIN])).unwrap();
        assert_eq!(r.as_primitive::<Int32Type>().value(0), i32::MIN);

        let r = neg_wrapping(&Int64Array::from(vec![i64::MIN])).unwrap();
        assert_eq!(r.as_primitive::<Int64Type>().value(0), i64::MIN);

        let err = neg_wrapping(&DurationSecondArray::from(vec![i64::MIN]))
            .unwrap_err()
            .to_string();

        assert_eq!(
            err,
            "Compute error: Overflow happened on: - -9223372036854775808"
        );

        let a = Decimal128Array::from(vec![1, 3, -44, 2, 4])
            .with_precision_and_scale(9, 6)
            .unwrap();

        let r = neg(&a).unwrap();
        assert_eq!(r.data_type(), a.data_type());
        assert_eq!(
            r.as_primitive::<Decimal128Type>().values(),
            &[-1, -3, 44, -2, -4]
        );

        let a = Decimal256Array::from(vec![
            i256::from_i128(342),
            i256::from_i128(-4949),
            i256::from_i128(3),
        ])
        .with_precision_and_scale(9, 6)
        .unwrap();

        let r = neg(&a).unwrap();
        assert_eq!(r.data_type(), a.data_type());
        assert_eq!(
            r.as_primitive::<Decimal256Type>().values(),
            &[
                i256::from_i128(-342),
                i256::from_i128(4949),
                i256::from_i128(-3),
            ]
        );

        let a = IntervalYearMonthArray::from(vec![
            IntervalYearMonthType::make_value(2, 4),
            IntervalYearMonthType::make_value(2, -4),
            IntervalYearMonthType::make_value(-3, -5),
        ]);
        let r = neg(&a).unwrap();
        assert_eq!(
            r.as_primitive::<IntervalYearMonthType>().values(),
            &[
                IntervalYearMonthType::make_value(-2, -4),
                IntervalYearMonthType::make_value(-2, 4),
                IntervalYearMonthType::make_value(3, 5),
            ]
        );

        let a = IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(2, 4),
            IntervalDayTimeType::make_value(2, -4),
            IntervalDayTimeType::make_value(-3, -5),
        ]);
        let r = neg(&a).unwrap();
        assert_eq!(
            r.as_primitive::<IntervalDayTimeType>().values(),
            &[
                IntervalDayTimeType::make_value(-2, -4),
                IntervalDayTimeType::make_value(-2, 4),
                IntervalDayTimeType::make_value(3, 5),
            ]
        );

        let a = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(2, 4, 5953394),
            IntervalMonthDayNanoType::make_value(2, -4, -45839),
            IntervalMonthDayNanoType::make_value(-3, -5, 6944),
        ]);
        let r = neg(&a).unwrap();
        assert_eq!(
            r.as_primitive::<IntervalMonthDayNanoType>().values(),
            &[
                IntervalMonthDayNanoType::make_value(-2, -4, -5953394),
                IntervalMonthDayNanoType::make_value(-2, 4, 45839),
                IntervalMonthDayNanoType::make_value(3, 5, -6944),
            ]
        );
    }

    #[test]
    fn test_integer() {
        let a = Int32Array::from(vec![4, 3, 5, -6, 100]);
        let b = Int32Array::from(vec![6, 2, 5, -7, 3]);
        let result = add(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &Int32Array::from(vec![10, 5, 10, -13, 103])
        );
        let result = sub(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &Int32Array::from(vec![-2, 1, 0, 1, 97]));
        let result = div(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &Int32Array::from(vec![0, 1, 1, 0, 33]));
        let result = mul(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &Int32Array::from(vec![24, 6, 25, 42, 300]));
        let result = rem(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &Int32Array::from(vec![4, 1, 0, -6, 1]));

        let a = Int8Array::from(vec![Some(2), None, Some(45)]);
        let b = Int8Array::from(vec![Some(5), Some(3), None]);
        let result = add(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &Int8Array::from(vec![Some(7), None, None]));

        let a = UInt8Array::from(vec![56, 5, 3]);
        let b = UInt8Array::from(vec![200, 2, 5]);
        let err = add(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Compute error: Overflow happened on: 56 + 200");
        let result = add_wrapping(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &UInt8Array::from(vec![0, 7, 8]));

        let a = UInt8Array::from(vec![34, 5, 3]);
        let b = UInt8Array::from(vec![200, 2, 5]);
        let err = sub(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Compute error: Overflow happened on: 34 - 200");
        let result = sub_wrapping(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &UInt8Array::from(vec![90, 3, 254]));

        let a = UInt8Array::from(vec![34, 5, 3]);
        let b = UInt8Array::from(vec![200, 2, 5]);
        let err = mul(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Compute error: Overflow happened on: 34 * 200");
        let result = mul_wrapping(&a, &b).unwrap();
        assert_eq!(result.as_ref(), &UInt8Array::from(vec![144, 10, 15]));

        let a = Int16Array::from(vec![i16::MIN]);
        let b = Int16Array::from(vec![-1]);
        let err = div(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Compute error: Overflow happened on: -32768 / -1");

        let a = Int16Array::from(vec![21]);
        let b = Int16Array::from(vec![0]);
        let err = div(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Divide by zero error");

        let a = Int16Array::from(vec![21]);
        let b = Int16Array::from(vec![0]);
        let err = rem(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Divide by zero error");
    }

    #[test]
    fn test_float() {
        let a = Float32Array::from(vec![1., f32::MAX, 6., -4., -1., 0.]);
        let b = Float32Array::from(vec![1., f32::MAX, f32::MAX, -3., 45., 0.]);
        let result = add(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &Float32Array::from(vec![2., f32::INFINITY, f32::MAX, -7., 44.0, 0.])
        );

        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &Float32Array::from(vec![0., 0., f32::MIN, -1., -46., 0.])
        );

        let result = mul(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &Float32Array::from(vec![1., f32::INFINITY, f32::INFINITY, 12., -45., 0.])
        );

        let result = div(&a, &b).unwrap();
        let r = result.as_primitive::<Float32Type>();
        assert_eq!(r.value(0), 1.);
        assert_eq!(r.value(1), 1.);
        assert!(r.value(2) < f32::EPSILON);
        assert_eq!(r.value(3), -4. / -3.);
        assert!(r.value(5).is_nan());

        let result = rem(&a, &b).unwrap();
        let r = result.as_primitive::<Float32Type>();
        assert_eq!(&r.values()[..5], &[0., 0., 6., -1., -1.]);
        assert!(r.value(5).is_nan());
    }

    #[test]
    fn test_decimal() {
        // 0.015 7.842 -0.577 0.334 -0.078 0.003
        let a = Decimal128Array::from(vec![15, 0, -577, 334, -78, 3])
            .with_precision_and_scale(12, 3)
            .unwrap();

        // 5.4 0 -35.6 0.3 0.6 7.45
        let b = Decimal128Array::from(vec![54, 34, -356, 3, 6, 745])
            .with_precision_and_scale(12, 1)
            .unwrap();

        let result = add(&a, &b).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(15, 3));
        assert_eq!(
            result.as_primitive::<Decimal128Type>().values(),
            &[5415, 3400, -36177, 634, 522, 74503]
        );

        let result = sub(&a, &b).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(15, 3));
        assert_eq!(
            result.as_primitive::<Decimal128Type>().values(),
            &[-5385, -3400, 35023, 34, -678, -74497]
        );

        let result = mul(&a, &b).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(25, 4));
        assert_eq!(
            result.as_primitive::<Decimal128Type>().values(),
            &[810, 0, 205412, 1002, -468, 2235]
        );

        let result = div(&a, &b).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(17, 7));
        assert_eq!(
            result.as_primitive::<Decimal128Type>().values(),
            &[27777, 0, 162078, 11133333, -1300000, 402]
        );

        let result = rem(&a, &b).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(12, 3));
        assert_eq!(
            result.as_primitive::<Decimal128Type>().values(),
            &[15, 0, -577, 34, -78, 3]
        );

        let a = Decimal128Array::from(vec![1])
            .with_precision_and_scale(3, 3)
            .unwrap();
        let b = Decimal128Array::from(vec![1])
            .with_precision_and_scale(37, 37)
            .unwrap();
        let err = mul(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Invalid argument error: Output scale of Decimal128(3, 3) * Decimal128(37, 37) would exceed max scale of 38");

        let a = Decimal128Array::from(vec![1])
            .with_precision_and_scale(3, -2)
            .unwrap();
        let err = add(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Compute error: Overflow happened on: 10 ^ 39");

        let a = Decimal128Array::from(vec![10])
            .with_precision_and_scale(3, -1)
            .unwrap();
        let err = add(&a, &b).unwrap_err().to_string();
        assert_eq!(
            err,
            "Compute error: Overflow happened on: 10 * 100000000000000000000000000000000000000"
        );

        let b = Decimal128Array::from(vec![0])
            .with_precision_and_scale(1, 1)
            .unwrap();
        let err = div(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Divide by zero error");
        let err = rem(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Divide by zero error");
    }

    fn test_timestamp_impl<T: TimestampOp>() {
        let a = PrimitiveArray::<T>::new(vec![2000000, 434030324, 53943340].into(), None);
        let b = PrimitiveArray::<T>::new(vec![329593, 59349, 694994].into(), None);

        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_primitive::<T::Duration>().values(),
            &[1670407, 433970975, 53248346]
        );

        let r2 = add(&b, &result.as_ref()).unwrap();
        assert_eq!(r2.as_ref(), &a);

        let r3 = add(&result.as_ref(), &b).unwrap();
        assert_eq!(r3.as_ref(), &a);

        let format_array = |x: &dyn Array| -> Vec<String> {
            x.as_primitive::<T>()
                .values()
                .into_iter()
                .map(|x| as_datetime::<T>(*x).unwrap().to_string())
                .collect()
        };

        let values = vec![
            "1970-01-01T00:00:00Z",
            "2010-04-01T04:00:20Z",
            "1960-01-30T04:23:20Z",
        ]
        .into_iter()
        .map(|x| T::make_value(DateTime::parse_from_rfc3339(x).unwrap().naive_utc()).unwrap())
        .collect();

        let a = PrimitiveArray::<T>::new(values, None);
        let b = IntervalYearMonthArray::from(vec![
            IntervalYearMonthType::make_value(5, 34),
            IntervalYearMonthType::make_value(-2, 4),
            IntervalYearMonthType::make_value(7, -4),
        ]);
        let r4 = add(&a, &b).unwrap();
        assert_eq!(
            &format_array(r4.as_ref()),
            &[
                "1977-11-01 00:00:00".to_string(),
                "2008-08-01 04:00:20".to_string(),
                "1966-09-30 04:23:20".to_string()
            ]
        );

        let r5 = sub(&r4, &b).unwrap();
        assert_eq!(r5.as_ref(), &a);

        let b = IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(5, 454000),
            IntervalDayTimeType::make_value(-34, 0),
            IntervalDayTimeType::make_value(7, -4000),
        ]);
        let r6 = add(&a, &b).unwrap();
        assert_eq!(
            &format_array(r6.as_ref()),
            &[
                "1970-01-06 00:07:34".to_string(),
                "2010-02-26 04:00:20".to_string(),
                "1960-02-06 04:23:16".to_string()
            ]
        );

        let r7 = sub(&r6, &b).unwrap();
        assert_eq!(r7.as_ref(), &a);

        let b = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(344, 34, -43_000_000_000),
            IntervalMonthDayNanoType::make_value(-593, -33, 13_000_000_000),
            IntervalMonthDayNanoType::make_value(5, 2, 493_000_000_000),
        ]);
        let r8 = add(&a, &b).unwrap();
        assert_eq!(
            &format_array(r8.as_ref()),
            &[
                "1998-10-04 23:59:17".to_string(),
                "1960-09-29 04:00:33".to_string(),
                "1960-07-02 04:31:33".to_string()
            ]
        );

        let r9 = sub(&r8, &b).unwrap();
        // Note: subtraction is not the inverse of addition for intervals
        assert_eq!(
            &format_array(r9.as_ref()),
            &[
                "1970-01-02 00:00:00".to_string(),
                "2010-04-02 04:00:20".to_string(),
                "1960-01-31 04:23:20".to_string()
            ]
        );
    }

    #[test]
    fn test_timestamp() {
        test_timestamp_impl::<TimestampSecondType>();
        test_timestamp_impl::<TimestampMillisecondType>();
        test_timestamp_impl::<TimestampMicrosecondType>();
        test_timestamp_impl::<TimestampNanosecondType>();
    }

    #[test]
    fn test_interval() {
        let a = IntervalYearMonthArray::from(vec![
            IntervalYearMonthType::make_value(32, 4),
            IntervalYearMonthType::make_value(32, 4),
        ]);
        let b = IntervalYearMonthArray::from(vec![
            IntervalYearMonthType::make_value(-4, 6),
            IntervalYearMonthType::make_value(-3, 23),
        ]);
        let result = add(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &IntervalYearMonthArray::from(vec![
                IntervalYearMonthType::make_value(28, 10),
                IntervalYearMonthType::make_value(29, 27)
            ])
        );
        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &IntervalYearMonthArray::from(vec![
                IntervalYearMonthType::make_value(36, -2),
                IntervalYearMonthType::make_value(35, -19)
            ])
        );

        let a = IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(32, 4),
            IntervalDayTimeType::make_value(32, 4),
        ]);
        let b = IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(-4, 6),
            IntervalDayTimeType::make_value(-3, 23),
        ]);
        let result = add(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &IntervalDayTimeArray::from(vec![
                IntervalDayTimeType::make_value(28, 10),
                IntervalDayTimeType::make_value(29, 27)
            ])
        );
        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &IntervalDayTimeArray::from(vec![
                IntervalDayTimeType::make_value(36, -2),
                IntervalDayTimeType::make_value(35, -19)
            ])
        );
        let a = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(32, 4, 4000000000000),
            IntervalMonthDayNanoType::make_value(32, 4, 45463000000000000),
        ]);
        let b = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(-4, 6, 46000000000000),
            IntervalMonthDayNanoType::make_value(-3, 23, 3564000000000000),
        ]);
        let result = add(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNanoType::make_value(28, 10, 50000000000000),
                IntervalMonthDayNanoType::make_value(29, 27, 49027000000000000)
            ])
        );
        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_ref(),
            &IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNanoType::make_value(36, -2, -42000000000000),
                IntervalMonthDayNanoType::make_value(35, -19, 41899000000000000)
            ])
        );
        let a = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::MAX]);
        let b = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::ONE]);
        let err = add(&a, &b).unwrap_err().to_string();
        assert_eq!(err, "Compute error: Overflow happened on: 2147483647 + 1");
    }

    fn test_duration_impl<T: ArrowPrimitiveType<Native = i64>>() {
        let a = PrimitiveArray::<T>::new(vec![1000, 4394, -3944].into(), None);
        let b = PrimitiveArray::<T>::new(vec![4, -5, -243].into(), None);

        let result = add(&a, &b).unwrap();
        assert_eq!(result.as_primitive::<T>().values(), &[1004, 4389, -4187]);
        let result = sub(&a, &b).unwrap();
        assert_eq!(result.as_primitive::<T>().values(), &[996, 4399, -3701]);

        let err = mul(&a, &b).unwrap_err().to_string();
        assert!(
            err.contains("Invalid duration arithmetic operation"),
            "{err}"
        );

        let err = div(&a, &b).unwrap_err().to_string();
        assert!(
            err.contains("Invalid duration arithmetic operation"),
            "{err}"
        );

        let err = rem(&a, &b).unwrap_err().to_string();
        assert!(
            err.contains("Invalid duration arithmetic operation"),
            "{err}"
        );

        let a = PrimitiveArray::<T>::new(vec![i64::MAX].into(), None);
        let b = PrimitiveArray::<T>::new(vec![1].into(), None);
        let err = add(&a, &b).unwrap_err().to_string();
        assert_eq!(
            err,
            "Compute error: Overflow happened on: 9223372036854775807 + 1"
        );
    }

    #[test]
    fn test_duration() {
        test_duration_impl::<DurationSecondType>();
        test_duration_impl::<DurationMillisecondType>();
        test_duration_impl::<DurationMicrosecondType>();
        test_duration_impl::<DurationNanosecondType>();
    }

    fn test_date_impl<T: ArrowPrimitiveType, F>(f: F)
    where
        F: Fn(NaiveDate) -> T::Native,
        T::Native: TryInto<i64>,
    {
        let a = PrimitiveArray::<T>::new(
            vec![
                f(NaiveDate::from_ymd_opt(1979, 1, 30).unwrap()),
                f(NaiveDate::from_ymd_opt(2010, 4, 3).unwrap()),
                f(NaiveDate::from_ymd_opt(2008, 2, 29).unwrap()),
            ]
            .into(),
            None,
        );

        let b = IntervalYearMonthArray::from(vec![
            IntervalYearMonthType::make_value(34, 2),
            IntervalYearMonthType::make_value(3, -3),
            IntervalYearMonthType::make_value(-12, 4),
        ]);

        let format_array = |x: &dyn Array| -> Vec<String> {
            x.as_primitive::<T>()
                .values()
                .into_iter()
                .map(|x| {
                    as_date::<T>((*x).try_into().ok().unwrap())
                        .unwrap()
                        .to_string()
                })
                .collect()
        };

        let result = add(&a, &b).unwrap();
        assert_eq!(
            &format_array(result.as_ref()),
            &[
                "2013-03-30".to_string(),
                "2013-01-03".to_string(),
                "1996-06-29".to_string(),
            ]
        );
        let result = sub(&result, &b).unwrap();
        assert_eq!(result.as_ref(), &a);

        let b = IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(34, 2),
            IntervalDayTimeType::make_value(3, -3),
            IntervalDayTimeType::make_value(-12, 4),
        ]);

        let result = add(&a, &b).unwrap();
        assert_eq!(
            &format_array(result.as_ref()),
            &[
                "1979-03-05".to_string(),
                "2010-04-06".to_string(),
                "2008-02-17".to_string(),
            ]
        );
        let result = sub(&result, &b).unwrap();
        assert_eq!(result.as_ref(), &a);

        let b = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(34, 2, -34353534),
            IntervalMonthDayNanoType::make_value(3, -3, 2443),
            IntervalMonthDayNanoType::make_value(-12, 4, 2323242423232),
        ]);

        let result = add(&a, &b).unwrap();
        assert_eq!(
            &format_array(result.as_ref()),
            &[
                "1981-12-02".to_string(),
                "2010-06-30".to_string(),
                "2007-03-04".to_string(),
            ]
        );
        let result = sub(&result, &b).unwrap();
        assert_eq!(
            &format_array(result.as_ref()),
            &[
                "1979-01-31".to_string(),
                "2010-04-02".to_string(),
                "2008-02-29".to_string(),
            ]
        );
    }

    #[test]
    fn test_date() {
        test_date_impl::<Date32Type, _>(Date32Type::from_naive_date);
        test_date_impl::<Date64Type, _>(Date64Type::from_naive_date);

        let a = Date32Array::from(vec![i32::MIN, i32::MAX, 23, 7684]);
        let b = Date32Array::from(vec![i32::MIN, i32::MIN, -2, 45]);
        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_primitive::<DurationSecondType>().values(),
            &[0, 371085174288000, 2160000, 660009600]
        );

        let a = Date64Array::from(vec![4343, 76676, 3434]);
        let b = Date64Array::from(vec![3, -5, 5]);
        let result = sub(&a, &b).unwrap();
        assert_eq!(
            result.as_primitive::<DurationMillisecondType>().values(),
            &[4340, 76681, 3429]
        );

        let a = Date64Array::from(vec![i64::MAX]);
        let b = Date64Array::from(vec![-1]);
        let err = sub(&a, &b).unwrap_err().to_string();
        assert_eq!(
            err,
            "Compute error: Overflow happened on: 9223372036854775807 - -1"
        );
    }
}
