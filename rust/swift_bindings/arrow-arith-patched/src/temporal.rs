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

//! Defines temporal kernels for time and date related functions.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use chrono::{Datelike, NaiveDateTime, Offset, TimeZone, Timelike, Utc};

use arrow_array::temporal_conversions::{
    date32_to_datetime, date64_to_datetime, timestamp_ms_to_datetime, timestamp_ns_to_datetime,
    timestamp_s_to_datetime, timestamp_us_to_datetime, MICROSECONDS, MICROSECONDS_IN_DAY,
    MILLISECONDS, MILLISECONDS_IN_DAY, NANOSECONDS, NANOSECONDS_IN_DAY, SECONDS_IN_DAY,
};
use arrow_array::timezone::Tz;
use arrow_array::types::*;
use arrow_array::*;
use arrow_buffer::ArrowNativeType;
use arrow_schema::{ArrowError, DataType};

/// Valid parts to extract from date/time/timestamp arrays.
///
/// See [`date_part`].
///
/// Marked as non-exhaustive as may expand to support more types of
/// date parts in the future.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DatePart {
    /// Quarter of the year, in range `1..=4`
    Quarter,
    /// Calendar year
    Year,
    /// Month in the year, in range `1..=12`
    Month,
    /// ISO week of the year, in range `1..=53`
    Week,
    /// Day of the month, in range `1..=31`
    Day,
    /// Day of the week, in range `0..=6`, where Sunday is `0`
    DayOfWeekSunday0,
    /// Day of the week, in range `0..=6`, where Monday is `0`
    DayOfWeekMonday0,
    /// Day of year, in range `1..=366`
    DayOfYear,
    /// Hour of the day, in range `0..=23`
    Hour,
    /// Minute of the hour, in range `0..=59`
    Minute,
    /// Second of the minute, in range `0..=59`
    Second,
    /// Millisecond of the second
    Millisecond,
    /// Microsecond of the second
    Microsecond,
    /// Nanosecond of the second
    Nanosecond,
}

impl std::fmt::Display for DatePart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Returns function to extract relevant [`DatePart`] from types like a
/// [`NaiveDateTime`] or [`DateTime`].
///
/// [`DateTime`]: chrono::DateTime
fn get_date_time_part_extract_fn<T>(part: DatePart) -> fn(T) -> i32
where
    T: ChronoDateExt + Datelike + Timelike,
{
    match part {
        DatePart::Quarter => |d| d.quarter() as i32,
        DatePart::Year => |d| d.year(),
        DatePart::Month => |d| d.month() as i32,
        DatePart::Week => |d| d.iso_week().week() as i32,
        DatePart::Day => |d| d.day() as i32,
        DatePart::DayOfWeekSunday0 => |d| d.num_days_from_sunday(),
        DatePart::DayOfWeekMonday0 => |d| d.num_days_from_monday(),
        DatePart::DayOfYear => |d| d.ordinal() as i32,
        DatePart::Hour => |d| d.hour() as i32,
        DatePart::Minute => |d| d.minute() as i32,
        DatePart::Second => |d| d.second() as i32,
        DatePart::Millisecond => |d| (d.nanosecond() / 1_000_000) as i32,
        DatePart::Microsecond => |d| (d.nanosecond() / 1_000) as i32,
        DatePart::Nanosecond => |d| (d.nanosecond()) as i32,
    }
}

/// Given an array, return a new array with the extracted [`DatePart`] as signed 32-bit
/// integer values.
///
/// Currently only supports temporal types:
///   - Date32/Date64
///   - Time32/Time64
///   - Timestamp
///
/// Returns an [`Int32Array`] unless input was a dictionary type, in which case returns
/// the dictionary but with this function applied onto its values.
///
/// If array passed in is not of the above listed types (or is a dictionary array where the
/// values array isn't of the above listed types), then this function will return an error.
///
/// # Examples
///
/// ```
/// # use arrow_array::{Int32Array, TimestampMicrosecondArray};
/// # use arrow_arith::temporal::{DatePart, date_part};
/// let input: TimestampMicrosecondArray =
///     vec![Some(1612025847000000), None, Some(1722015847000000)].into();
///
/// let actual = date_part(&input, DatePart::Week).unwrap();
/// let expected: Int32Array = vec![Some(4), None, Some(30)].into();
/// assert_eq!(actual.as_ref(), &expected);
/// ```
pub fn date_part(array: &dyn Array, part: DatePart) -> Result<ArrayRef, ArrowError> {
    downcast_temporal_array!(
        array => {
            let array = array.date_part(part)?;
            let array = Arc::new(array) as ArrayRef;
            Ok(array)
        }
        // TODO: support interval
        // DataType::Interval(_) => {
        //     todo!();
        // }
        DataType::Dictionary(_, _) => {
            let array = array.as_any_dictionary();
            let values = date_part(array.values(), part)?;
            let values = Arc::new(values) as ArrayRef;
            let new_array = array.with_values(values);
            Ok(new_array)
        }
        t => return_compute_error_with!(format!("{part} does not support"), t),
    )
}

/// Used to integrate new [`date_part()`] method with deprecated shims such as
/// [`hour()`] and [`week()`].
fn date_part_primitive<T: ArrowTemporalType>(
    array: &PrimitiveArray<T>,
    part: DatePart,
) -> Result<Int32Array, ArrowError> {
    let array = date_part(array, part)?;
    Ok(array.as_primitive::<Int32Type>().to_owned())
}

/// Extract optional [`Tz`] from timestamp data types, returning error
/// if called with a non-timestamp type.
fn get_tz(dt: &DataType) -> Result<Option<Tz>, ArrowError> {
    match dt {
        DataType::Timestamp(_, Some(tz)) => Ok(Some(tz.parse::<Tz>()?)),
        DataType::Timestamp(_, None) => Ok(None),
        _ => Err(ArrowError::CastError(format!("Not a timestamp type: {dt}"))),
    }
}

/// Implement the specialized functions for extracting date part from temporal arrays.
trait ExtractDatePartExt {
    fn date_part(&self, part: DatePart) -> Result<Int32Array, ArrowError>;
}

impl ExtractDatePartExt for PrimitiveArray<Time32SecondType> {
    fn date_part(&self, part: DatePart) -> Result<Int32Array, ArrowError> {
        #[inline]
        fn range_check(s: i32) -> bool {
            (0..SECONDS_IN_DAY as i32).contains(&s)
        }
        match part {
            DatePart::Hour => Ok(self.unary_opt(|s| range_check(s).then_some(s / 3_600))),
            DatePart::Minute => Ok(self.unary_opt(|s| range_check(s).then_some((s / 60) % 60))),
            DatePart::Second => Ok(self.unary_opt(|s| range_check(s).then_some(s % 60))),
            // Time32Second only encodes number of seconds, so these will always be 0 (if in valid range)
            DatePart::Millisecond | DatePart::Microsecond | DatePart::Nanosecond => {
                Ok(self.unary_opt(|s| range_check(s).then_some(0)))
            }
            _ => return_compute_error_with!(format!("{part} does not support"), self.data_type()),
        }
    }
}

impl ExtractDatePartExt for PrimitiveArray<Time32MillisecondType> {
    fn date_part(&self, part: DatePart) -> Result<Int32Array, ArrowError> {
        #[inline]
        fn range_check(ms: i32) -> bool {
            (0..MILLISECONDS_IN_DAY as i32).contains(&ms)
        }
        let milliseconds = MILLISECONDS as i32;
        match part {
            DatePart::Hour => {
                Ok(self.unary_opt(|ms| range_check(ms).then_some(ms / 3_600 / milliseconds)))
            }
            DatePart::Minute => {
                Ok(self.unary_opt(|ms| range_check(ms).then_some((ms / 60 / milliseconds) % 60)))
            }
            DatePart::Second => {
                Ok(self.unary_opt(|ms| range_check(ms).then_some((ms / milliseconds) % 60)))
            }
            DatePart::Millisecond => {
                Ok(self.unary_opt(|ms| range_check(ms).then_some(ms % milliseconds)))
            }
            DatePart::Microsecond => {
                Ok(self.unary_opt(|ms| range_check(ms).then_some((ms % milliseconds) * 1_000)))
            }
            DatePart::Nanosecond => {
                Ok(self.unary_opt(|ms| range_check(ms).then_some((ms % milliseconds) * 1_000_000)))
            }
            _ => return_compute_error_with!(format!("{part} does not support"), self.data_type()),
        }
    }
}

impl ExtractDatePartExt for PrimitiveArray<Time64MicrosecondType> {
    fn date_part(&self, part: DatePart) -> Result<Int32Array, ArrowError> {
        #[inline]
        fn range_check(us: i64) -> bool {
            (0..MICROSECONDS_IN_DAY).contains(&us)
        }
        match part {
            DatePart::Hour => {
                Ok(self
                    .unary_opt(|us| range_check(us).then_some((us / 3_600 / MICROSECONDS) as i32)))
            }
            DatePart::Minute => Ok(self
                .unary_opt(|us| range_check(us).then_some(((us / 60 / MICROSECONDS) % 60) as i32))),
            DatePart::Second => {
                Ok(self
                    .unary_opt(|us| range_check(us).then_some(((us / MICROSECONDS) % 60) as i32)))
            }
            DatePart::Millisecond => Ok(self
                .unary_opt(|us| range_check(us).then_some(((us % MICROSECONDS) / 1_000) as i32))),
            DatePart::Microsecond => {
                Ok(self.unary_opt(|us| range_check(us).then_some((us % MICROSECONDS) as i32)))
            }
            DatePart::Nanosecond => Ok(self
                .unary_opt(|us| range_check(us).then_some(((us % MICROSECONDS) * 1_000) as i32))),
            _ => return_compute_error_with!(format!("{part} does not support"), self.data_type()),
        }
    }
}

impl ExtractDatePartExt for PrimitiveArray<Time64NanosecondType> {
    fn date_part(&self, part: DatePart) -> Result<Int32Array, ArrowError> {
        #[inline]
        fn range_check(ns: i64) -> bool {
            (0..NANOSECONDS_IN_DAY).contains(&ns)
        }
        match part {
            DatePart::Hour => {
                Ok(self
                    .unary_opt(|ns| range_check(ns).then_some((ns / 3_600 / NANOSECONDS) as i32)))
            }
            DatePart::Minute => Ok(self
                .unary_opt(|ns| range_check(ns).then_some(((ns / 60 / NANOSECONDS) % 60) as i32))),
            DatePart::Second => Ok(
                self.unary_opt(|ns| range_check(ns).then_some(((ns / NANOSECONDS) % 60) as i32))
            ),
            DatePart::Millisecond => Ok(self.unary_opt(|ns| {
                range_check(ns).then_some(((ns % NANOSECONDS) / 1_000_000) as i32)
            })),
            DatePart::Microsecond => {
                Ok(self
                    .unary_opt(|ns| range_check(ns).then_some(((ns % NANOSECONDS) / 1_000) as i32)))
            }
            DatePart::Nanosecond => {
                Ok(self.unary_opt(|ns| range_check(ns).then_some((ns % NANOSECONDS) as i32)))
            }
            _ => return_compute_error_with!(format!("{part} does not support"), self.data_type()),
        }
    }
}

impl ExtractDatePartExt for PrimitiveArray<Date32Type> {
    fn date_part(&self, part: DatePart) -> Result<Int32Array, ArrowError> {
        // Date32 only encodes number of days, so these will always be 0
        if let DatePart::Hour
        | DatePart::Minute
        | DatePart::Second
        | DatePart::Millisecond
        | DatePart::Microsecond
        | DatePart::Nanosecond = part
        {
            Ok(Int32Array::new(
                vec![0; self.len()].into(),
                self.nulls().cloned(),
            ))
        } else {
            let map_func = get_date_time_part_extract_fn(part);
            Ok(self.unary_opt(|d| date32_to_datetime(d).map(map_func)))
        }
    }
}

impl ExtractDatePartExt for PrimitiveArray<Date64Type> {
    fn date_part(&self, part: DatePart) -> Result<Int32Array, ArrowError> {
        let map_func = get_date_time_part_extract_fn(part);
        Ok(self.unary_opt(|d| date64_to_datetime(d).map(map_func)))
    }
}

impl ExtractDatePartExt for PrimitiveArray<TimestampSecondType> {
    fn date_part(&self, part: DatePart) -> Result<Int32Array, ArrowError> {
        // TimestampSecond only encodes number of seconds, so these will always be 0
        let array =
            if let DatePart::Millisecond | DatePart::Microsecond | DatePart::Nanosecond = part {
                Int32Array::new(vec![0; self.len()].into(), self.nulls().cloned())
            } else if let Some(tz) = get_tz(self.data_type())? {
                let map_func = get_date_time_part_extract_fn(part);
                self.unary_opt(|d| {
                    timestamp_s_to_datetime(d)
                        .map(|c| Utc.from_utc_datetime(&c).with_timezone(&tz))
                        .map(map_func)
                })
            } else {
                let map_func = get_date_time_part_extract_fn(part);
                self.unary_opt(|d| timestamp_s_to_datetime(d).map(map_func))
            };
        Ok(array)
    }
}

impl ExtractDatePartExt for PrimitiveArray<TimestampMillisecondType> {
    fn date_part(&self, part: DatePart) -> Result<Int32Array, ArrowError> {
        let array = if let Some(tz) = get_tz(self.data_type())? {
            let map_func = get_date_time_part_extract_fn(part);
            self.unary_opt(|d| {
                timestamp_ms_to_datetime(d)
                    .map(|c| Utc.from_utc_datetime(&c).with_timezone(&tz))
                    .map(map_func)
            })
        } else {
            let map_func = get_date_time_part_extract_fn(part);
            self.unary_opt(|d| timestamp_ms_to_datetime(d).map(map_func))
        };
        Ok(array)
    }
}

impl ExtractDatePartExt for PrimitiveArray<TimestampMicrosecondType> {
    fn date_part(&self, part: DatePart) -> Result<Int32Array, ArrowError> {
        let array = if let Some(tz) = get_tz(self.data_type())? {
            let map_func = get_date_time_part_extract_fn(part);
            self.unary_opt(|d| {
                timestamp_us_to_datetime(d)
                    .map(|c| Utc.from_utc_datetime(&c).with_timezone(&tz))
                    .map(map_func)
            })
        } else {
            let map_func = get_date_time_part_extract_fn(part);
            self.unary_opt(|d| timestamp_us_to_datetime(d).map(map_func))
        };
        Ok(array)
    }
}

impl ExtractDatePartExt for PrimitiveArray<TimestampNanosecondType> {
    fn date_part(&self, part: DatePart) -> Result<Int32Array, ArrowError> {
        let array = if let Some(tz) = get_tz(self.data_type())? {
            let map_func = get_date_time_part_extract_fn(part);
            self.unary_opt(|d| {
                timestamp_ns_to_datetime(d)
                    .map(|c| Utc.from_utc_datetime(&c).with_timezone(&tz))
                    .map(map_func)
            })
        } else {
            let map_func = get_date_time_part_extract_fn(part);
            self.unary_opt(|d| timestamp_ns_to_datetime(d).map(map_func))
        };
        Ok(array)
    }
}

macro_rules! return_compute_error_with {
    ($msg:expr, $param:expr) => {
        return { Err(ArrowError::ComputeError(format!("{}: {:?}", $msg, $param))) }
    };
}

pub(crate) use return_compute_error_with;

// Internal trait, which is used for mapping values from DateLike structures
trait ChronoDateExt {
    /// Returns a value in range `1..=4` indicating the quarter this date falls into
    fn quarter(&self) -> u32;

    /// Returns a value in range `0..=3` indicating the quarter (zero-based) this date falls into
    fn quarter0(&self) -> u32;

    /// Returns the day of week; Monday is encoded as `0`, Tuesday as `1`, etc.
    fn num_days_from_monday(&self) -> i32;

    /// Returns the day of week; Sunday is encoded as `0`, Monday as `1`, etc.
    fn num_days_from_sunday(&self) -> i32;
}

impl<T: ?Sized + Datelike> ChronoDateExt for T {
    fn quarter(&self) -> u32 {
        self.quarter0() + 1
    }

    fn quarter0(&self) -> u32 {
        self.month0() / 3
    }

    fn num_days_from_monday(&self) -> i32 {
        self.weekday().num_days_from_monday() as i32
    }

    fn num_days_from_sunday(&self) -> i32 {
        self.weekday().num_days_from_sunday() as i32
    }
}

/// Parse the given string into a string representing fixed-offset that is correct as of the given
/// UTC NaiveDateTime.
/// Note that the offset is function of time and can vary depending on whether daylight savings is
/// in effect or not. e.g. Australia/Sydney is +10:00 or +11:00 depending on DST.
#[deprecated(note = "Use arrow_array::timezone::Tz instead")]
pub fn using_chrono_tz_and_utc_naive_date_time(
    tz: &str,
    utc: NaiveDateTime,
) -> Option<chrono::offset::FixedOffset> {
    let tz: Tz = tz.parse().ok()?;
    Some(tz.offset_from_utc_datetime(&utc).fix())
}

/// Extracts the hours of a given array as an array of integers within
/// the range of [0, 23]. If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn hour_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Hour)
}

/// Extracts the hours of a given temporal primitive array as an array of integers within
/// the range of [0, 23].
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn hour<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::Hour)
}

/// Extracts the years of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn year_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Year)
}

/// Extracts the years of a given temporal primitive array as an array of integers
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn year<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::Year)
}

/// Extracts the quarter of a given temporal array as an array of integersa within
/// the range of [1, 4]. If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn quarter_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Quarter)
}

/// Extracts the quarter of a given temporal primitive array as an array of integers within
/// the range of [1, 4].
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn quarter<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::Quarter)
}

/// Extracts the month of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn month_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Month)
}

/// Extracts the month of a given temporal primitive array as an array of integers within
/// the range of [1, 12].
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn month<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::Month)
}

/// Extracts the day of week of a given temporal array as an array of
/// integers.
///
/// Monday is encoded as `0`, Tuesday as `1`, etc.
///
/// See also [`num_days_from_sunday`] which starts at Sunday.
///
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn num_days_from_monday_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::DayOfWeekMonday0)
}

/// Extracts the day of week of a given temporal primitive array as an array of
/// integers.
///
/// Monday is encoded as `0`, Tuesday as `1`, etc.
///
/// See also [`num_days_from_sunday`] which starts at Sunday.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn num_days_from_monday<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::DayOfWeekMonday0)
}

/// Extracts the day of week of a given temporal array as an array of
/// integers, starting at Sunday.
///
/// Sunday is encoded as `0`, Monday as `1`, etc.
///
/// See also [`num_days_from_monday`] which starts at Monday.
///
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn num_days_from_sunday_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::DayOfWeekSunday0)
}

/// Extracts the day of week of a given temporal primitive array as an array of
/// integers, starting at Sunday.
///
/// Sunday is encoded as `0`, Monday as `1`, etc.
///
/// See also [`num_days_from_monday`] which starts at Monday.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn num_days_from_sunday<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::DayOfWeekSunday0)
}

/// Extracts the day of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn day_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Day)
}

/// Extracts the day of a given temporal primitive array as an array of integers
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn day<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::Day)
}

/// Extracts the day of year of a given temporal array as an array of integers
/// The day of year that ranges from 1 to 366.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn doy_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::DayOfYear)
}

/// Extracts the day of year of a given temporal primitive array as an array of integers
/// The day of year that ranges from 1 to 366
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn doy<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    T::Native: ArrowNativeType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::DayOfYear)
}

/// Extracts the minutes of a given temporal primitive array as an array of integers
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn minute<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::Minute)
}

/// Extracts the week of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn week_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Week)
}

/// Extracts the week of a given temporal primitive array as an array of integers
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn week<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::Week)
}

/// Extracts the seconds of a given temporal primitive array as an array of integers
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn second<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::Second)
}

/// Extracts the nanoseconds of a given temporal primitive array as an array of integers
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn nanosecond<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::Nanosecond)
}

/// Extracts the nanoseconds of a given temporal primitive array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn nanosecond_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Nanosecond)
}

/// Extracts the microseconds of a given temporal primitive array as an array of integers
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn microsecond<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::Microsecond)
}

/// Extracts the microseconds of a given temporal primitive array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn microsecond_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Microsecond)
}

/// Extracts the milliseconds of a given temporal primitive array as an array of integers
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn millisecond<T>(array: &PrimitiveArray<T>) -> Result<Int32Array, ArrowError>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    date_part_primitive(array, DatePart::Millisecond)
}

/// Extracts the milliseconds of a given temporal primitive array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn millisecond_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Millisecond)
}

/// Extracts the minutes of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn minute_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Minute)
}

/// Extracts the seconds of a given temporal array as an array of integers.
/// If the given array isn't temporal primitive or dictionary array,
/// an `Err` will be returned.
#[deprecated(since = "51.0.0", note = "Use `date_part` instead")]
pub fn second_dyn(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    date_part(array, DatePart::Second)
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;

    #[test]
    fn test_temporal_array_date64_hour() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = hour(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(4, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_hour() {
        let a: PrimitiveArray<Date32Type> = vec![Some(15147), None, Some(15148)].into();

        let b = hour(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(0, b.value(2));
    }

    #[test]
    fn test_temporal_array_time32_second_hour() {
        let a: PrimitiveArray<Time32SecondType> = vec![37800, 86339].into();

        let b = hour(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(23, b.value(1));
    }

    #[test]
    fn test_temporal_array_time64_micro_hour() {
        let a: PrimitiveArray<Time64MicrosecondType> = vec![37800000000, 86339000000].into();

        let b = hour(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(23, b.value(1));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_hour() {
        let a: TimestampMicrosecondArray = vec![37800000000, 86339000000].into();

        let b = hour(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(23, b.value(1));
    }

    #[test]
    fn test_temporal_array_date64_year() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = year(&a).unwrap();
        assert_eq!(2018, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2019, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_year() {
        let a: PrimitiveArray<Date32Type> = vec![Some(15147), None, Some(15448)].into();

        let b = year(&a).unwrap();
        assert_eq!(2011, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2012, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_quarter() {
        //1514764800000 -> 2018-01-01
        //1566275025000 -> 2019-08-20
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1566275025000)].into();

        let b = quarter(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(3, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_quarter() {
        let a: PrimitiveArray<Date32Type> = vec![Some(1), None, Some(300)].into();

        let b = quarter(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(4, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_quarter_with_timezone() {
        // 24 * 60 * 60 = 86400
        let a = TimestampSecondArray::from(vec![86400 * 90]).with_timezone("+00:00".to_string());
        let b = quarter(&a).unwrap();
        assert_eq!(2, b.value(0));
        let a = TimestampSecondArray::from(vec![86400 * 90]).with_timezone("-10:00".to_string());
        let b = quarter(&a).unwrap();
        assert_eq!(1, b.value(0));
    }

    #[test]
    fn test_temporal_array_date64_month() {
        //1514764800000 -> 2018-01-01
        //1550636625000 -> 2019-02-20
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = month(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_month() {
        let a: PrimitiveArray<Date32Type> = vec![Some(1), None, Some(31)].into();

        let b = month(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_month_with_timezone() {
        // 24 * 60 * 60 = 86400
        let a = TimestampSecondArray::from(vec![86400 * 31]).with_timezone("+00:00".to_string());
        let b = month(&a).unwrap();
        assert_eq!(2, b.value(0));
        let a = TimestampSecondArray::from(vec![86400 * 31]).with_timezone("-10:00".to_string());
        let b = month(&a).unwrap();
        assert_eq!(1, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_day_with_timezone() {
        // 24 * 60 * 60 = 86400
        let a = TimestampSecondArray::from(vec![86400]).with_timezone("+00:00".to_string());
        let b = day(&a).unwrap();
        assert_eq!(2, b.value(0));
        let a = TimestampSecondArray::from(vec![86400]).with_timezone("-10:00".to_string());
        let b = day(&a).unwrap();
        assert_eq!(1, b.value(0));
    }

    #[test]
    fn test_temporal_array_date64_weekday() {
        //1514764800000 -> 2018-01-01 (Monday)
        //1550636625000 -> 2019-02-20 (Wednesday)
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = num_days_from_monday(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_weekday0() {
        //1483228800000 -> 2017-01-01 (Sunday)
        //1514764800000 -> 2018-01-01 (Monday)
        //1550636625000 -> 2019-02-20 (Wednesday)
        let a: PrimitiveArray<Date64Type> = vec![
            Some(1483228800000),
            None,
            Some(1514764800000),
            Some(1550636625000),
        ]
        .into();

        let b = num_days_from_sunday(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(1, b.value(2));
        assert_eq!(3, b.value(3));
    }

    #[test]
    fn test_temporal_array_date64_day() {
        //1514764800000 -> 2018-01-01
        //1550636625000 -> 2019-02-20
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = day(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(20, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_day() {
        let a: PrimitiveArray<Date32Type> = vec![Some(0), None, Some(31)].into();

        let b = day(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(1, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_doy() {
        //1483228800000 -> 2017-01-01 (Sunday)
        //1514764800000 -> 2018-01-01
        //1550636625000 -> 2019-02-20
        let a: PrimitiveArray<Date64Type> = vec![
            Some(1483228800000),
            Some(1514764800000),
            None,
            Some(1550636625000),
        ]
        .into();

        let b = doy(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert_eq!(1, b.value(1));
        assert!(!b.is_valid(2));
        assert_eq!(51, b.value(3));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_year() {
        let a: TimestampMicrosecondArray =
            vec![Some(1612025847000000), None, Some(1722015847000000)].into();

        let b = year(&a).unwrap();
        assert_eq!(2021, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2024, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_minute() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = minute(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(23, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_minute() {
        let a: TimestampMicrosecondArray =
            vec![Some(1612025847000000), None, Some(1722015847000000)].into();

        let b = minute(&a).unwrap();
        assert_eq!(57, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(44, b.value(2));
    }

    #[test]
    fn test_temporal_array_date32_week() {
        let a: PrimitiveArray<Date32Type> = vec![Some(0), None, Some(7)].into();

        let b = week(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(2, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_week() {
        // 1646116175000 -> 2022.03.01 , 1641171600000 -> 2022.01.03
        // 1640998800000 -> 2022.01.01
        let a: PrimitiveArray<Date64Type> = vec![
            Some(1646116175000),
            None,
            Some(1641171600000),
            Some(1640998800000),
        ]
        .into();

        let b = week(&a).unwrap();
        assert_eq!(9, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(1, b.value(2));
        assert_eq!(52, b.value(3));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_week() {
        //1612025847000000 -> 2021.1.30
        //1722015847000000 -> 2024.7.27
        let a: TimestampMicrosecondArray =
            vec![Some(1612025847000000), None, Some(1722015847000000)].into();

        let b = week(&a).unwrap();
        assert_eq!(4, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(30, b.value(2));
    }

    #[test]
    fn test_temporal_array_date64_second() {
        let a: PrimitiveArray<Date64Type> =
            vec![Some(1514764800000), None, Some(1550636625000)].into();

        let b = second(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(45, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_micro_second() {
        let a: TimestampMicrosecondArray =
            vec![Some(1612025847000000), None, Some(1722015847000000)].into();

        let b = second(&a).unwrap();
        assert_eq!(27, b.value(0));
        assert!(!b.is_valid(1));
        assert_eq!(7, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_second_with_timezone() {
        let a = TimestampSecondArray::from(vec![10, 20]).with_timezone("+00:00".to_string());
        let b = second(&a).unwrap();
        assert_eq!(10, b.value(0));
        assert_eq!(20, b.value(1));
    }

    #[test]
    fn test_temporal_array_timestamp_minute_with_timezone() {
        let a = TimestampSecondArray::from(vec![0, 60]).with_timezone("+00:50".to_string());
        let b = minute(&a).unwrap();
        assert_eq!(50, b.value(0));
        assert_eq!(51, b.value(1));
    }

    #[test]
    fn test_temporal_array_timestamp_minute_with_negative_timezone() {
        let a = TimestampSecondArray::from(vec![60 * 55]).with_timezone("-00:50".to_string());
        let b = minute(&a).unwrap();
        assert_eq!(5, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10]).with_timezone("+01:00".to_string());
        let b = hour(&a).unwrap();
        assert_eq!(11, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_without_colon() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10]).with_timezone("+0100".to_string());
        let b = hour(&a).unwrap();
        assert_eq!(11, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_without_minutes() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10]).with_timezone("+01".to_string());
        let b = hour(&a).unwrap();
        assert_eq!(11, b.value(0));
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_without_initial_sign() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10]).with_timezone("0100".to_string());
        let err = hour(&a).unwrap_err().to_string();
        assert!(err.contains("Invalid timezone"), "{}", err);
    }

    #[test]
    fn test_temporal_array_timestamp_hour_with_timezone_with_only_colon() {
        let a = TimestampSecondArray::from(vec![60 * 60 * 10]).with_timezone("01:00".to_string());
        let err = hour(&a).unwrap_err().to_string();
        assert!(err.contains("Invalid timezone"), "{}", err);
    }

    #[test]
    fn test_temporal_array_timestamp_week_without_timezone() {
        // 1970-01-01T00:00:00                     -> 1970-01-01T00:00:00 Thursday (week 1)
        // 1970-01-01T00:00:00 + 4 days            -> 1970-01-05T00:00:00 Monday   (week 2)
        // 1970-01-01T00:00:00 + 4 days - 1 second -> 1970-01-04T23:59:59 Sunday   (week 1)
        let a = TimestampSecondArray::from(vec![0, 86400 * 4, 86400 * 4 - 1]);
        let b = week(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert_eq!(2, b.value(1));
        assert_eq!(1, b.value(2));
    }

    #[test]
    fn test_temporal_array_timestamp_week_with_timezone() {
        // 1970-01-01T01:00:00+01:00                     -> 1970-01-01T01:00:00+01:00 Thursday (week 1)
        // 1970-01-01T01:00:00+01:00 + 4 days            -> 1970-01-05T01:00:00+01:00 Monday   (week 2)
        // 1970-01-01T01:00:00+01:00 + 4 days - 1 second -> 1970-01-05T00:59:59+01:00 Monday   (week 2)
        let a = TimestampSecondArray::from(vec![0, 86400 * 4, 86400 * 4 - 1])
            .with_timezone("+01:00".to_string());
        let b = week(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert_eq!(2, b.value(1));
        assert_eq!(2, b.value(2));
    }

    #[test]
    fn test_hour_minute_second_dictionary_array() {
        let a = TimestampSecondArray::from(vec![
            60 * 60 * 10 + 61,
            60 * 60 * 20 + 122,
            60 * 60 * 30 + 183,
        ])
        .with_timezone("+01:00".to_string());

        let keys = Int8Array::from_iter_values([0_i8, 0, 1, 2, 1]);
        let dict = DictionaryArray::try_new(keys.clone(), Arc::new(a)).unwrap();

        let b = hour_dyn(&dict).unwrap();

        let expected_dict =
            DictionaryArray::new(keys.clone(), Arc::new(Int32Array::from(vec![11, 21, 7])));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);

        let b = date_part(&dict, DatePart::Minute).unwrap();

        let b_old = minute_dyn(&dict).unwrap();

        let expected_dict =
            DictionaryArray::new(keys.clone(), Arc::new(Int32Array::from(vec![1, 2, 3])));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
        assert_eq!(&expected, &b_old);

        let b = date_part(&dict, DatePart::Second).unwrap();

        let b_old = second_dyn(&dict).unwrap();

        let expected_dict =
            DictionaryArray::new(keys.clone(), Arc::new(Int32Array::from(vec![1, 2, 3])));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
        assert_eq!(&expected, &b_old);

        let b = date_part(&dict, DatePart::Nanosecond).unwrap();

        let expected_dict =
            DictionaryArray::new(keys, Arc::new(Int32Array::from(vec![0, 0, 0, 0, 0])));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }

    #[test]
    fn test_year_dictionary_array() {
        let a: PrimitiveArray<Date64Type> = vec![Some(1514764800000), Some(1550636625000)].into();

        let keys = Int8Array::from_iter_values([0_i8, 1, 1, 0]);
        let dict = DictionaryArray::new(keys.clone(), Arc::new(a));

        let b = year_dyn(&dict).unwrap();

        let expected_dict = DictionaryArray::new(
            keys,
            Arc::new(Int32Array::from(vec![2018, 2019, 2019, 2018])),
        );
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }

    #[test]
    fn test_quarter_month_dictionary_array() {
        //1514764800000 -> 2018-01-01
        //1566275025000 -> 2019-08-20
        let a: PrimitiveArray<Date64Type> = vec![Some(1514764800000), Some(1566275025000)].into();

        let keys = Int8Array::from_iter_values([0_i8, 1, 1, 0]);
        let dict = DictionaryArray::new(keys.clone(), Arc::new(a));

        let b = quarter_dyn(&dict).unwrap();

        let expected =
            DictionaryArray::new(keys.clone(), Arc::new(Int32Array::from(vec![1, 3, 3, 1])));
        assert_eq!(b.as_ref(), &expected);

        let b = month_dyn(&dict).unwrap();

        let expected = DictionaryArray::new(keys, Arc::new(Int32Array::from(vec![1, 8, 8, 1])));
        assert_eq!(b.as_ref(), &expected);
    }

    #[test]
    fn test_num_days_from_monday_sunday_day_doy_week_dictionary_array() {
        //1514764800000 -> 2018-01-01 (Monday)
        //1550636625000 -> 2019-02-20 (Wednesday)
        let a: PrimitiveArray<Date64Type> = vec![Some(1514764800000), Some(1550636625000)].into();

        let keys = Int8Array::from(vec![Some(0_i8), Some(1), Some(1), Some(0), None]);
        let dict = DictionaryArray::new(keys.clone(), Arc::new(a));

        let b = num_days_from_monday_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![Some(0), Some(2), Some(2), Some(0), None]);
        let expected = DictionaryArray::new(keys.clone(), Arc::new(a));
        assert_eq!(b.as_ref(), &expected);

        let b = num_days_from_sunday_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![Some(1), Some(3), Some(3), Some(1), None]);
        let expected = DictionaryArray::new(keys.clone(), Arc::new(a));
        assert_eq!(b.as_ref(), &expected);

        let b = day_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![Some(1), Some(20), Some(20), Some(1), None]);
        let expected = DictionaryArray::new(keys.clone(), Arc::new(a));
        assert_eq!(b.as_ref(), &expected);

        let b = doy_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![Some(1), Some(51), Some(51), Some(1), None]);
        let expected = DictionaryArray::new(keys.clone(), Arc::new(a));
        assert_eq!(b.as_ref(), &expected);

        let b = week_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![Some(1), Some(8), Some(8), Some(1), None]);
        let expected = DictionaryArray::new(keys, Arc::new(a));
        assert_eq!(b.as_ref(), &expected);
    }

    #[test]
    fn test_temporal_array_date64_nanosecond() {
        // new Date(1667328721453)
        // Tue Nov 01 2022 11:52:01 GMT-0700 (Pacific Daylight Time)
        //
        // new Date(1667328721453).getMilliseconds()
        // 453

        let a: PrimitiveArray<Date64Type> = vec![None, Some(1667328721453)].into();

        let b = nanosecond(&a).unwrap();
        assert!(!b.is_valid(0));
        assert_eq!(453_000_000, b.value(1));

        let keys = Int8Array::from(vec![Some(0_i8), Some(1), Some(1)]);
        let dict = DictionaryArray::new(keys.clone(), Arc::new(a));
        let b = nanosecond_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![None, Some(453_000_000)]);
        let expected_dict = DictionaryArray::new(keys, Arc::new(a));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }

    #[test]
    fn test_temporal_array_date64_microsecond() {
        let a: PrimitiveArray<Date64Type> = vec![None, Some(1667328721453)].into();

        let b = microsecond(&a).unwrap();
        assert!(!b.is_valid(0));
        assert_eq!(453_000, b.value(1));

        let keys = Int8Array::from(vec![Some(0_i8), Some(1), Some(1)]);
        let dict = DictionaryArray::new(keys.clone(), Arc::new(a));
        let b = microsecond_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![None, Some(453_000)]);
        let expected_dict = DictionaryArray::new(keys, Arc::new(a));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }

    #[test]
    fn test_temporal_array_date64_millisecond() {
        let a: PrimitiveArray<Date64Type> = vec![None, Some(1667328721453)].into();

        let b = millisecond(&a).unwrap();
        assert!(!b.is_valid(0));
        assert_eq!(453, b.value(1));

        let keys = Int8Array::from(vec![Some(0_i8), Some(1), Some(1)]);
        let dict = DictionaryArray::new(keys.clone(), Arc::new(a));
        let b = millisecond_dyn(&dict).unwrap();

        let a = Int32Array::from(vec![None, Some(453)]);
        let expected_dict = DictionaryArray::new(keys, Arc::new(a));
        let expected = Arc::new(expected_dict) as ArrayRef;
        assert_eq!(&expected, &b);
    }

    #[test]
    fn test_temporal_array_time64_nanoseconds() {
        // 23:32:50.123456789
        let input: Time64NanosecondArray = vec![Some(84_770_123_456_789)].into();

        let actual = date_part(&input, DatePart::Hour).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(23, actual.value(0));

        let actual = date_part(&input, DatePart::Minute).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(32, actual.value(0));

        let actual = date_part(&input, DatePart::Second).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(50, actual.value(0));

        let actual = date_part(&input, DatePart::Millisecond).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(123, actual.value(0));

        let actual = date_part(&input, DatePart::Microsecond).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(123_456, actual.value(0));

        let actual = date_part(&input, DatePart::Nanosecond).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(123_456_789, actual.value(0));

        // invalid values should turn into null
        let input: Time64NanosecondArray = vec![
            Some(-1),
            Some(86_400_000_000_000),
            Some(86_401_000_000_000),
            None,
        ]
        .into();
        let actual = date_part(&input, DatePart::Hour).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        let expected: Int32Array = vec![None, None, None, None].into();
        assert_eq!(&expected, actual);
    }

    #[test]
    fn test_temporal_array_time64_microseconds() {
        // 23:32:50.123456
        let input: Time64MicrosecondArray = vec![Some(84_770_123_456)].into();

        let actual = date_part(&input, DatePart::Hour).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(23, actual.value(0));

        let actual = date_part(&input, DatePart::Minute).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(32, actual.value(0));

        let actual = date_part(&input, DatePart::Second).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(50, actual.value(0));

        let actual = date_part(&input, DatePart::Millisecond).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(123, actual.value(0));

        let actual = date_part(&input, DatePart::Microsecond).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(123_456, actual.value(0));

        let actual = date_part(&input, DatePart::Nanosecond).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(123_456_000, actual.value(0));

        // invalid values should turn into null
        let input: Time64MicrosecondArray =
            vec![Some(-1), Some(86_400_000_000), Some(86_401_000_000), None].into();
        let actual = date_part(&input, DatePart::Hour).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        let expected: Int32Array = vec![None, None, None, None].into();
        assert_eq!(&expected, actual);
    }

    #[test]
    fn test_temporal_array_time32_milliseconds() {
        // 23:32:50.123
        let input: Time32MillisecondArray = vec![Some(84_770_123)].into();

        let actual = date_part(&input, DatePart::Hour).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(23, actual.value(0));

        let actual = date_part(&input, DatePart::Minute).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(32, actual.value(0));

        let actual = date_part(&input, DatePart::Second).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(50, actual.value(0));

        let actual = date_part(&input, DatePart::Millisecond).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(123, actual.value(0));

        let actual = date_part(&input, DatePart::Microsecond).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(123_000, actual.value(0));

        let actual = date_part(&input, DatePart::Nanosecond).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(123_000_000, actual.value(0));

        // invalid values should turn into null
        let input: Time32MillisecondArray =
            vec![Some(-1), Some(86_400_000), Some(86_401_000), None].into();
        let actual = date_part(&input, DatePart::Hour).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        let expected: Int32Array = vec![None, None, None, None].into();
        assert_eq!(&expected, actual);
    }

    #[test]
    fn test_temporal_array_time32_seconds() {
        // 23:32:50
        let input: Time32SecondArray = vec![84_770].into();

        let actual = date_part(&input, DatePart::Hour).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(23, actual.value(0));

        let actual = date_part(&input, DatePart::Minute).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(32, actual.value(0));

        let actual = date_part(&input, DatePart::Second).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(50, actual.value(0));

        let actual = date_part(&input, DatePart::Millisecond).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(0, actual.value(0));

        let actual = date_part(&input, DatePart::Microsecond).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(0, actual.value(0));

        let actual = date_part(&input, DatePart::Nanosecond).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        assert_eq!(0, actual.value(0));

        // invalid values should turn into null
        let input: Time32SecondArray = vec![Some(-1), Some(86_400), Some(86_401), None].into();
        let actual = date_part(&input, DatePart::Hour).unwrap();
        let actual = actual.as_primitive::<Int32Type>();
        let expected: Int32Array = vec![None, None, None, None].into();
        assert_eq!(&expected, actual);
    }

    #[test]
    fn test_temporal_array_time_invalid_parts() {
        fn ensure_returns_error(array: &dyn Array) {
            let invalid_parts = [
                DatePart::Quarter,
                DatePart::Year,
                DatePart::Month,
                DatePart::Week,
                DatePart::Day,
                DatePart::DayOfWeekSunday0,
                DatePart::DayOfWeekMonday0,
                DatePart::DayOfYear,
            ];

            for part in invalid_parts {
                let err = date_part(array, part).unwrap_err();
                let expected = format!(
                    "Compute error: {part} does not support: {}",
                    array.data_type()
                );
                assert_eq!(expected, err.to_string());
            }
        }

        ensure_returns_error(&Time32SecondArray::from(vec![0]));
        ensure_returns_error(&Time32MillisecondArray::from(vec![0]));
        ensure_returns_error(&Time64MicrosecondArray::from(vec![0]));
        ensure_returns_error(&Time64NanosecondArray::from(vec![0]));
    }
}
