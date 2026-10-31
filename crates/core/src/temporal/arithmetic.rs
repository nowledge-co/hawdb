// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Temporal arithmetic, `duration.between` and truncation.

use chrono::{
    Datelike, FixedOffset, Months, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Timelike,
};

use super::construct::{attach, kind_name, DurationBuilder, Number};
use super::{
    delta_nanos, is_temporal, Duration, TemporalError, TemporalKind, TemporalResult, NANOS_PER_DAY,
};
use crate::Value;

/// The arithmetic operators that accept temporal operands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
}

impl TemporalArithmeticOp {
    const fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Remainder => "%",
        }
    }
}

fn overflow() -> TemporalError {
    TemporalError::new("temporal arithmetic result is out of range")
}

const fn is_instant(value: &Value) -> bool {
    matches!(
        value,
        Value::Date(_) | Value::LocalTime(_) | Value::LocalDateTime(_) | Value::DateTime(_)
    )
}

/// Evaluates `left op right` when either operand is temporal; returns
/// `None` for non-temporal operands so numeric arithmetic stays unchanged.
/// NULL operands must be handled by the caller.
///
/// Supported: instant ± duration, duration + instant, duration ± duration,
/// duration × number, number × duration and duration ÷ number.
pub fn arithmetic(
    left: &Value,
    op: TemporalArithmeticOp,
    right: &Value,
) -> Option<TemporalResult<Value>> {
    if !is_temporal(left) && !is_temporal(right) {
        return None;
    }
    use TemporalArithmeticOp as Op;
    Some(match (op, left, right) {
        (Op::Add, Value::Duration(left), Value::Duration(right)) => left
            .checked_add(**right)
            .map(Value::duration)
            .ok_or_else(overflow),
        (Op::Subtract, Value::Duration(left), Value::Duration(right)) => left
            .checked_sub(**right)
            .map(Value::duration)
            .ok_or_else(overflow),
        (Op::Add, instant, Value::Duration(duration))
        | (Op::Add, Value::Duration(duration), instant)
            if is_instant(instant) =>
        {
            add_duration(instant, duration)
        }
        (Op::Subtract, instant, Value::Duration(duration)) if is_instant(instant) => duration
            .checked_neg()
            .ok_or_else(overflow)
            .and_then(|duration| add_duration(instant, &duration)),
        (Op::Multiply, Value::Duration(duration), factor)
        | (Op::Multiply, factor, Value::Duration(duration))
            if matches!(factor, Value::Int(_) | Value::Float(_)) =>
        {
            scale(duration, factor, false)
        }
        (Op::Divide, Value::Duration(duration), divisor)
            if matches!(divisor, Value::Int(_) | Value::Float(_)) =>
        {
            scale(duration, divisor, true)
        }
        _ => Err(TemporalError::new(format!(
            "unsupported temporal arithmetic: {} {} {}",
            kind_name(left),
            op.symbol(),
            kind_name(right)
        ))),
    })
}

fn add_months(date: NaiveDate, months: i64) -> Option<NaiveDate> {
    let magnitude = Months::new(u32::try_from(months.unsigned_abs()).ok()?);
    if months >= 0 {
        date.checked_add_months(magnitude)
    } else {
        date.checked_sub_months(magnitude)
    }
}

fn add_to_local_datetime(value: NaiveDateTime, duration: &Duration) -> Option<NaiveDateTime> {
    add_months(value.date(), duration.months)?
        .and_time(value.time())
        .checked_add_signed(TimeDelta::try_days(duration.days)?)?
        .checked_add_signed(duration.time)
}

/// Adds a duration: months first (clamping the day of month), then days,
/// then the exact time. Dates ignore the sub-day remainder of the time part
/// and local times ignore months and days, as in openCypher.
fn add_duration(instant: &Value, duration: &Duration) -> TemporalResult<Value> {
    let result = match instant {
        Value::Date(date) => {
            let whole_days = i64::try_from(duration.time_nanos() / NANOS_PER_DAY).ok();
            whole_days
                .and_then(|whole_days| duration.days.checked_add(whole_days))
                .and_then(TimeDelta::try_days)
                .and_then(|days| add_months(*date, duration.months)?.checked_add_signed(days))
                .map(Value::Date)
        }
        Value::LocalTime(time) => Some(Value::LocalTime(
            time.overflowing_add_signed(duration.time).0,
        )),
        Value::LocalDateTime(value) => {
            add_to_local_datetime(*value, duration).map(Value::LocalDateTime)
        }
        Value::DateTime(value) => add_to_local_datetime(value.naive_local(), duration)
            .and_then(|local| attach(local, *value.offset()))
            .map(Value::DateTime),
        _ => unreachable!("callers pass temporal instants"),
    };
    result.ok_or_else(overflow)
}

/// `duration * number` and `duration / number`.
///
/// Multiplying by an integer is exact. Dividing by an integer is exact when
/// it divides every component (`duration({hours: 10000}) / 2`). Otherwise
/// the components are scaled in `f64` and fractions cascade into smaller
/// units, which is approximate: an exact time part beyond 2^53 nanoseconds
/// (about 104 days) loses nanosecond precision.
fn scale(duration: &Duration, factor: &Value, divide: bool) -> TemporalResult<Value> {
    if let (Value::Int(factor), false) = (factor, divide) {
        let factor = *factor;
        return duration
            .months
            .checked_mul(factor)
            .zip(duration.days.checked_mul(factor))
            .zip(duration.time_nanos().checked_mul(i128::from(factor)))
            .and_then(|((months, days), nanos)| Duration::from_parts(months, days, nanos))
            .map(Value::duration)
            .ok_or_else(overflow);
    }
    if let (Value::Int(divisor), true) = (factor, divide) {
        let divisor = *divisor;
        if divisor == 0 {
            return Err(TemporalError::new("division by zero"));
        }
        let nanos = duration.time_nanos();
        if duration.months.checked_rem(divisor) == Some(0)
            && duration.days.checked_rem(divisor) == Some(0)
            && nanos.checked_rem(i128::from(divisor)) == Some(0)
        {
            return duration
                .months
                .checked_div(divisor)
                .zip(duration.days.checked_div(divisor))
                .zip(nanos.checked_div(i128::from(divisor)))
                .and_then(|((months, days), nanos)| Duration::from_parts(months, days, nanos))
                .map(Value::duration)
                .ok_or_else(overflow);
        }
    }
    let factor = match factor {
        Value::Int(value) => *value as f64,
        Value::Float(value) => *value,
        _ => unreachable!("callers pass numeric factors"),
    };
    if divide && factor == 0.0 {
        return Err(TemporalError::new("division by zero"));
    }
    let factor = if divide { 1.0 / factor } else { factor };
    if !factor.is_finite() {
        return Err(overflow());
    }
    let mut builder = DurationBuilder::default();
    builder.add("months", Number::Float(duration.months as f64 * factor))?;
    builder.add("days", Number::Float(duration.days as f64 * factor))?;
    builder.add(
        "nanoseconds",
        Number::Float(duration.time_nanos() as f64 * factor),
    )?;
    builder.finish().map(Value::duration)
}

/// Which part of `duration.between` a function returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BetweenMode {
    /// `duration.between`: months, then days, then the exact remainder.
    Full,
    /// `duration.inMonths`: whole months only.
    Months,
    /// `duration.inDays`: whole days only.
    Days,
    /// `duration.inSeconds`: the exact time difference only.
    Seconds,
}

struct Parts {
    date: Option<NaiveDate>,
    time: Option<NaiveTime>,
    offset: Option<FixedOffset>,
}

fn parts(value: &Value) -> Option<Parts> {
    Some(match value {
        Value::Date(date) => Parts {
            date: Some(*date),
            time: None,
            offset: None,
        },
        Value::LocalTime(time) => Parts {
            date: None,
            time: Some(*time),
            offset: None,
        },
        Value::LocalDateTime(value) => Parts {
            date: Some(value.date()),
            time: Some(value.time()),
            offset: None,
        },
        Value::DateTime(value) => Parts {
            date: Some(value.date_naive()),
            time: Some(value.time()),
            offset: Some(*value.offset()),
        },
        _ => return None,
    })
}

fn month_difference(start: NaiveDateTime, end: NaiveDateTime) -> Option<i64> {
    let index = |value: NaiveDateTime| i64::from(value.year()) * 12 + i64::from(value.month0());
    let mut months = index(end) - index(start);
    let shifted = |months: i64| -> Option<NaiveDateTime> {
        Some(add_months(start.date(), months)?.and_time(start.time()))
    };
    if months > 0 && shifted(months)? > end {
        months -= 1;
    } else if months < 0 && shifted(months)? < end {
        months += 1;
    }
    Some(months)
}

/// `duration.between(a, b)` and the `duration.in*` variants. Zoned values
/// are compared on one timeline (b is re-expressed in a's offset); a local
/// value paired with a zoned one is read in the zoned value's offset. When
/// either value has no date, only the times are compared.
pub(crate) fn between(left: &Value, right: &Value, mode: BetweenMode) -> TemporalResult<Value> {
    let (Some(start), Some(mut end)) = (parts(left), parts(right)) else {
        return Err(TemporalError::new(format!(
            "duration.between() expects two temporal instants, got {} and {}",
            kind_name(left),
            kind_name(right)
        )));
    };
    if let (Some(start_offset), Some(end_offset)) = (start.offset, end.offset) {
        let local = end
            .date
            .expect("zoned values have a date")
            .and_time(end.time.expect("zoned values have a time"));
        let shifted = local
            .checked_sub_signed(TimeDelta::seconds(i64::from(end_offset.local_minus_utc())))
            .and_then(|utc| {
                utc.checked_add_signed(TimeDelta::seconds(i64::from(
                    start_offset.local_minus_utc(),
                )))
            })
            .ok_or_else(overflow)?;
        end.date = Some(shifted.date());
        end.time = Some(shifted.time());
    }
    let zero = Duration::default();
    let (Some(start_date), Some(end_date)) = (start.date, end.date) else {
        let start_time = start.time.unwrap_or(NaiveTime::MIN);
        let end_time = end.time.unwrap_or(NaiveTime::MIN);
        let nanos = delta_nanos(end_time.signed_duration_since(start_time));
        return Ok(Value::duration(match mode {
            BetweenMode::Full | BetweenMode::Seconds => {
                Duration::from_parts(0, 0, nanos).ok_or_else(overflow)?
            }
            BetweenMode::Months | BetweenMode::Days => zero,
        }));
    };
    let start = start_date.and_time(start.time.unwrap_or(NaiveTime::MIN));
    let end = end_date.and_time(end.time.unwrap_or(NaiveTime::MIN));
    let exact = delta_nanos(end.signed_duration_since(start));
    let duration = match mode {
        BetweenMode::Seconds => Duration::from_parts(0, 0, exact),
        BetweenMode::Days => Duration::from_parts(0, (exact / NANOS_PER_DAY) as i64, 0),
        BetweenMode::Months | BetweenMode::Full => {
            let months = month_difference(start, end).ok_or_else(overflow)?;
            if mode == BetweenMode::Months {
                Duration::from_parts(months, 0, 0)
            } else {
                let middle = add_months(start.date(), months)
                    .ok_or_else(overflow)?
                    .and_time(start.time());
                let remainder = delta_nanos(end.signed_duration_since(middle));
                let days = remainder / NANOS_PER_DAY;
                Duration::from_parts(months, days as i64, remainder - days * NANOS_PER_DAY)
            }
        }
    };
    duration.map(Value::duration).ok_or_else(overflow)
}

fn truncate_date(date: NaiveDate, unit: &str) -> Option<NaiveDate> {
    let year_start = |year: i32| NaiveDate::from_ymd_opt(year, 1, 1);
    match unit {
        "millennium" => year_start(date.year() - date.year().rem_euclid(1000)),
        "century" => year_start(date.year() - date.year().rem_euclid(100)),
        "decade" => year_start(date.year() - date.year().rem_euclid(10)),
        "year" => year_start(date.year()),
        "weekyear" => NaiveDate::from_isoywd_opt(date.iso_week().year(), 1, chrono::Weekday::Mon),
        "quarter" => NaiveDate::from_ymd_opt(date.year(), date.month0() / 3 * 3 + 1, 1),
        "month" => NaiveDate::from_ymd_opt(date.year(), date.month(), 1),
        "week" => date.checked_sub_days(chrono::Days::new(u64::from(
            date.weekday().num_days_from_monday(),
        ))),
        "day" => Some(date),
        _ => None,
    }
}

fn truncate_time(time: NaiveTime, unit: &str) -> Option<NaiveTime> {
    let nanos = time.nanosecond();
    match unit {
        "hour" => NaiveTime::from_hms_opt(time.hour(), 0, 0),
        "minute" => NaiveTime::from_hms_opt(time.hour(), time.minute(), 0),
        "second" => NaiveTime::from_hms_opt(time.hour(), time.minute(), time.second()),
        "millisecond" => NaiveTime::from_hms_nano_opt(
            time.hour(),
            time.minute(),
            time.second(),
            nanos - nanos % 1_000_000,
        ),
        "microsecond" => NaiveTime::from_hms_nano_opt(
            time.hour(),
            time.minute(),
            time.second(),
            nanos - nanos % 1_000,
        ),
        _ => None,
    }
}

const DATE_UNITS: [&str; 9] = [
    "millennium",
    "century",
    "decade",
    "year",
    "weekyear",
    "quarter",
    "month",
    "week",
    "day",
];
const TIME_UNITS: [&str; 5] = ["hour", "minute", "second", "millisecond", "microsecond"];

/// `date.truncate(unit, value)` and its `localtime`, `localdatetime` and
/// `datetime` siblings. Zoned results keep the input offset (UTC for local
/// inputs).
pub fn truncate(kind: TemporalKind, unit: &str, value: &Value) -> TemporalResult<Value> {
    let function = format!("{}.truncate", kind.function_name());
    let unit_lower = unit.to_ascii_lowercase();
    let Some(source) = parts(value) else {
        return Err(TemporalError::new(format!(
            "{function}() expects a temporal instant, got {}",
            kind_name(value)
        )));
    };
    if kind.has_date() && source.date.is_none() {
        return Err(TemporalError::new(format!(
            "{function}() cannot truncate a {} value",
            kind_name(value)
        )));
    }
    let (date, time) = if DATE_UNITS.contains(&unit_lower.as_str()) {
        if kind == TemporalKind::LocalTime && unit_lower != "day" {
            return Err(TemporalError::new(format!(
                "{function}() does not support unit '{unit}'"
            )));
        }
        (
            source
                .date
                .map(|date| truncate_date(date, &unit_lower).ok_or_else(overflow))
                .transpose()?,
            NaiveTime::MIN,
        )
    } else if TIME_UNITS.contains(&unit_lower.as_str()) && kind.has_time() {
        (
            source.date,
            truncate_time(source.time.unwrap_or(NaiveTime::MIN), &unit_lower)
                .ok_or_else(overflow)?,
        )
    } else {
        return Err(TemporalError::new(format!(
            "{function}() does not support unit '{unit}'"
        )));
    };
    Ok(match kind {
        TemporalKind::Date => Value::Date(date.expect("date kinds have a date")),
        TemporalKind::LocalTime => Value::LocalTime(time),
        TemporalKind::LocalDateTime => {
            Value::LocalDateTime(date.expect("date kinds have a date").and_time(time))
        }
        TemporalKind::DateTime => attach(
            date.expect("date kinds have a date").and_time(time),
            source
                .offset
                .unwrap_or_else(|| FixedOffset::east_opt(0).expect("UTC offset")),
        )
        .map(Value::DateTime)
        .ok_or_else(overflow)?,
    })
}
