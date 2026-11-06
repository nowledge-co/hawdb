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

//! SQL `TIMESTAMP` and `TIMESTAMPTZ` scalars.
//!
//! Both types store a signed count of microseconds since 1970-01-01 00:00:00.
//! `TIMESTAMP` is a local date and time without a zone; `TIMESTAMPTZ` is a UTC
//! instant (an input offset is applied and then discarded, as in PostgreSQL).
//! The representable range is chrono's `NaiveDateTime` range truncated to
//! whole microseconds, so every value converts to [`Value::LocalDateTime`] or
//! [`Value::DateTime`] without loss.

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, Timelike, Utc};

use crate::{HawDBError, Result, Value};

const MICROS_PER_SECOND: i64 = 1_000_000;
/// PostgreSQL's timestamp epoch, 2000-01-01, in Unix microseconds. Precision
/// rounding is symmetric around this instant, matching PostgreSQL.
const POSTGRES_EPOCH_MICROS: i64 = 946_684_800 * MICROS_PER_SECOND;

/// A validated SQL fractional-second precision `p` in `TIMESTAMP(p)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampPrecision(u8);

impl TimestampPrecision {
    pub const MAX: u8 = 6;
    pub const DEFAULT: Self = Self(Self::MAX);

    pub const fn new(precision: u8) -> Option<Self> {
        if precision <= Self::MAX {
            Some(Self(precision))
        } else {
            None
        }
    }

    pub const fn get(self) -> u8 {
        self.0
    }

    const fn scale(self) -> i64 {
        let mut scale = 1;
        let mut digits = Self::MAX - self.0;
        while digits > 0 {
            scale *= 10;
            digits -= 1;
        }
        scale
    }

    /// Rounds to this precision. Halfway values round away from PostgreSQL's
    /// 2000-01-01 epoch, as `AdjustTimestampForTypmod` does.
    pub fn round(self, micros: i64) -> Result<i64> {
        let scale = self.scale();
        if scale == 1 {
            return Ok(micros);
        }
        let relative = i128::from(micros) - i128::from(POSTGRES_EPOCH_MICROS);
        let (scale, offset) = (i128::from(scale), i128::from(scale / 2));
        let rounded = if relative >= 0 {
            (relative + offset) / scale * scale
        } else {
            -((-relative + offset) / scale * scale)
        };
        i64::try_from(rounded + i128::from(POSTGRES_EPOCH_MICROS))
            .ok()
            .filter(|value| in_range(*value))
            .ok_or_else(out_of_range)
    }

    /// Whether `micros` is already representable at this precision.
    pub fn accepts(self, micros: i64) -> bool {
        in_range(micros) && self.round(micros).is_ok_and(|rounded| rounded == micros)
    }
}

impl Default for TimestampPrecision {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The earliest representable microsecond (chrono's minimum datetime).
pub fn min_micros() -> i64 {
    NaiveDateTime::MIN.and_utc().timestamp_micros()
}

/// The latest representable microsecond (chrono's maximum datetime,
/// truncated to whole microseconds).
pub fn max_micros() -> i64 {
    NaiveDateTime::MAX.and_utc().timestamp_micros()
}

pub fn in_range(micros: i64) -> bool {
    (min_micros()..=max_micros()).contains(&micros)
}

fn out_of_range() -> HawDBError {
    HawDBError::Semantic("timestamp out of range".into())
}

/// Validates a stored microsecond count.
pub fn checked(micros: i64) -> Result<i64> {
    in_range(micros).then_some(micros).ok_or_else(out_of_range)
}

/// Converts a local datetime, rounding sub-microsecond digits half up.
pub fn from_naive(value: NaiveDateTime) -> Result<i64> {
    let utc = value.and_utc();
    let micros = i128::from(utc.timestamp()) * i128::from(MICROS_PER_SECOND)
        + i128::from((utc.timestamp_subsec_nanos() + 500) / 1_000);
    i64::try_from(micros)
        .ok()
        .filter(|value| in_range(*value))
        .ok_or_else(out_of_range)
}

/// Converts a zoned datetime to its UTC instant.
pub fn from_datetime(value: DateTime<FixedOffset>) -> Result<i64> {
    from_naive(value.naive_utc())
}

pub fn to_naive(micros: i64) -> Result<NaiveDateTime> {
    DateTime::<Utc>::from_timestamp_micros(checked(micros)?)
        .map(|value| value.naive_utc())
        .ok_or_else(out_of_range)
}

pub fn to_datetime(micros: i64) -> Result<DateTime<FixedOffset>> {
    Ok(to_naive(micros)?.and_utc().fixed_offset())
}

/// `TIMESTAMP` as a [`Value::LocalDateTime`].
pub fn local_value(micros: i64) -> Result<Value> {
    to_naive(micros).map(Value::LocalDateTime)
}

/// `TIMESTAMPTZ` as a UTC [`Value::DateTime`].
pub fn zoned_value(micros: i64) -> Result<Value> {
    to_datetime(micros).map(Value::DateTime)
}

/// Parses `TIMESTAMP` text: `YYYY-MM-DD`, optionally followed by a space or
/// `T` and `HH:MM[:SS[.fraction]]`. A time zone offset is rejected.
pub fn parse_timestamp(input: &str) -> Result<i64> {
    let (local, offset) = parse(input)?;
    if offset.is_some() {
        return Err(invalid(
            input,
            "TIMESTAMP values must not carry a time zone offset; use TIMESTAMPTZ",
        ));
    }
    from_naive(local)
}

/// Parses `TIMESTAMPTZ` text. An offset (`Z`, `±HH`, `±HHMM`, `±HH:MM`,
/// `±HH:MM:SS`) is applied; text without an offset is UTC.
pub fn parse_timestamptz(input: &str) -> Result<i64> {
    let (local, offset) = parse(input)?;
    let offset = offset.unwrap_or(0);
    let utc = local
        .checked_sub_signed(chrono::TimeDelta::seconds(i64::from(offset)))
        .ok_or_else(out_of_range)?;
    from_naive(utc)
}

/// PostgreSQL ISO output: `YYYY-MM-DD HH:MM:SS[.ffffff]` with trailing
/// fractional zeros removed. Years outside 0000..=9999 carry a sign.
pub fn format_timestamp(micros: i64) -> Result<String> {
    to_naive(micros).map(|value| format_naive(&value))
}

/// [`format_timestamp`] in UTC followed by `+00`.
pub fn format_timestamptz(micros: i64) -> Result<String> {
    format_timestamp(micros).map(|text| text + "+00")
}

fn format_naive(value: &NaiveDateTime) -> String {
    let year = value.year();
    let mut text = if (0..=9999).contains(&year) {
        format!("{year:04}")
    } else {
        format!("{year:+05}")
    };
    text.push_str(&format!(
        "-{:02}-{:02} {:02}:{:02}:{:02}",
        value.month(),
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    ));
    let micros = value.nanosecond() / 1_000;
    if micros != 0 {
        let fraction = format!("{micros:06}");
        text.push('.');
        text.push_str(fraction.trim_end_matches('0'));
    }
    text
}

/// The SQL type of a timestamp scalar: `TIMESTAMP(p)` or `TIMESTAMPTZ(p)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampType {
    pub zoned: bool,
    pub precision: TimestampPrecision,
}

impl TimestampType {
    pub const fn name(self) -> &'static str {
        if self.zoned {
            "TIMESTAMPTZ"
        } else {
            "TIMESTAMP"
        }
    }

    /// Converts a value assigned to or cast to this type: text is parsed,
    /// local and zoned datetimes convert through UTC (HawDB has no session
    /// time zone), and the result is rounded to the declared precision.
    /// `NULL` yields `None`.
    pub fn convert(self, value: &Value) -> Result<Option<i64>> {
        let micros = match value {
            Value::Null => return Ok(None),
            Value::String(text) if self.zoned => parse_timestamptz(text)?,
            Value::String(text) => parse_timestamp(text)?,
            Value::LocalDateTime(value) => from_naive(*value)?,
            Value::DateTime(value) => from_datetime(*value)?,
            other => {
                return Err(HawDBError::Semantic(format!(
                    "cannot convert {} to {}",
                    describe(other),
                    self.name()
                )))
            }
        };
        self.precision.round(micros).map(Some)
    }

    /// The host [`Value`] of a stored microsecond count.
    pub fn value(self, micros: i64) -> Result<Value> {
        if self.zoned {
            zoned_value(micros)
        } else {
            local_value(micros)
        }
    }
}

/// An explicit `CAST` between TEXT and the SQL timestamp types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampCast {
    Text,
    Timestamp(TimestampType),
}

impl TimestampCast {
    /// Applies the cast to a host value. Timestamps are represented as
    /// [`Value::LocalDateTime`] (`TIMESTAMP`) and UTC [`Value::DateTime`]
    /// (`TIMESTAMPTZ`); TEXT output uses PostgreSQL's ISO style.
    pub fn apply(self, value: &Value) -> Result<Value> {
        match self {
            Self::Timestamp(kind) => match kind.convert(value)? {
                None => Ok(Value::Null),
                Some(micros) => kind.value(micros),
            },
            Self::Text => match value {
                Value::Null => Ok(Value::Null),
                Value::String(text) => Ok(Value::String(text.clone())),
                Value::LocalDateTime(value) => {
                    format_timestamp(from_naive(*value)?).map(Value::String)
                }
                Value::DateTime(value) => {
                    format_timestamptz(from_datetime(*value)?).map(Value::String)
                }
                other => Err(HawDBError::Semantic(format!(
                    "CAST to TEXT supports TEXT, TIMESTAMP and TIMESTAMPTZ operands, not {}",
                    describe(other)
                ))),
            },
        }
    }
}

impl std::fmt::Display for TimestampCast {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Text => formatter.write_str("TEXT"),
            Self::Timestamp(kind) => write!(formatter, "{}({})", kind.name(), kind.precision.get()),
        }
    }
}

fn describe(value: &Value) -> String {
    value
        .logical_type()
        .map_or_else(|| "NULL".to_string(), |kind| format!("{kind:?}"))
}

fn invalid(input: &str, reason: &str) -> HawDBError {
    HawDBError::Semantic(format!("invalid timestamp {input:?}: {reason}"))
}

struct Cursor<'a> {
    input: &'a str,
    text: &'a str,
    bytes: &'a [u8],
    position: usize,
}

impl Cursor<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.position).copied()
    }

    fn eat(&mut self, byte: u8) -> bool {
        if self.peek() == Some(byte) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn digits(&mut self, min: usize, max: usize) -> Result<(u64, usize)> {
        let start = self.position;
        while self.position - start < max && self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.position += 1;
        }
        let count = self.position - start;
        if count < min {
            return Err(invalid(self.input, "expected digits"));
        }
        let value = self.text[start..self.position]
            .parse::<u64>()
            .map_err(|_| invalid(self.input, "field value out of range"))?;
        Ok((value, count))
    }

    fn fixed(&mut self, width: usize) -> Result<u32> {
        let (value, _) = self.digits(width, width)?;
        u32::try_from(value).map_err(|_| invalid(self.input, "field value out of range"))
    }
}

fn parse(input: &str) -> Result<(NaiveDateTime, Option<i32>)> {
    let trimmed = input.trim_matches(|c: char| c.is_ascii_whitespace());
    let mut cursor = Cursor {
        input,
        text: trimmed,
        bytes: trimmed.as_bytes(),
        position: 0,
    };
    let negative = if cursor.eat(b'-') {
        true
    } else {
        cursor.eat(b'+');
        false
    };
    let (year, _) = cursor.digits(4, 6)?;
    let year = i32::try_from(year).map_err(|_| out_of_range())?;
    let year = if negative { -year } else { year };
    if !cursor.eat(b'-') {
        return Err(invalid(input, "expected YYYY-MM-DD"));
    }
    let month = cursor.fixed(2)?;
    if !cursor.eat(b'-') {
        return Err(invalid(input, "expected YYYY-MM-DD"));
    }
    let day = cursor.fixed(2)?;
    let date = NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| invalid(input, "date field value out of range"))?;
    let (mut hour, mut minute, mut second, mut nanos) = (0, 0, 0, 0u32);
    let mut round_up = false;
    if cursor.eat(b' ') || cursor.eat(b'T') || cursor.eat(b't') {
        hour = cursor.fixed(2)?;
        if !cursor.eat(b':') {
            return Err(invalid(input, "expected HH:MM"));
        }
        minute = cursor.fixed(2)?;
        if cursor.eat(b':') {
            second = cursor.fixed(2)?;
            if cursor.eat(b'.') {
                let start = cursor.position;
                while cursor.peek().is_some_and(|b| b.is_ascii_digit()) {
                    cursor.position += 1;
                }
                let fraction = &trimmed[start..cursor.position];
                if fraction.is_empty() {
                    return Err(invalid(input, "expected fractional seconds"));
                }
                let mut micros = 0u32;
                for (index, digit) in fraction.bytes().enumerate() {
                    let digit = u32::from(digit - b'0');
                    match index {
                        0..=5 => micros = micros * 10 + digit,
                        6 => round_up = digit >= 5,
                        _ => {}
                    }
                }
                micros *= 10u32.pow(6u32.saturating_sub(fraction.len() as u32));
                nanos = micros * 1_000;
            }
        }
    }
    let time = chrono::NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)
        .ok_or_else(|| invalid(input, "time field value out of range"))?;
    let mut local = NaiveDateTime::new(date, time);
    if round_up {
        local = local
            .checked_add_signed(chrono::TimeDelta::microseconds(1))
            .ok_or_else(out_of_range)?;
    }
    let _ = cursor.eat(b' ');
    let offset = match cursor.peek() {
        None => None,
        Some(b'Z' | b'z') => {
            cursor.position += 1;
            Some(0)
        }
        Some(sign @ (b'+' | b'-')) => {
            cursor.position += 1;
            let hours = cursor.fixed(2)?;
            let colon = cursor.eat(b':');
            let mut minutes = 0;
            let mut seconds = 0;
            if colon || cursor.peek().is_some_and(|b| b.is_ascii_digit()) {
                minutes = cursor.fixed(2)?;
                if colon && cursor.eat(b':') {
                    seconds = cursor.fixed(2)?;
                }
            }
            if hours > 15 || minutes > 59 || seconds > 59 {
                return Err(invalid(input, "time zone offset out of range"));
            }
            let total =
                i32::try_from(hours * 3600 + minutes * 60 + seconds).map_err(|_| out_of_range())?;
            Some(if sign == b'-' { -total } else { total })
        }
        Some(_) => return Err(invalid(input, "unexpected trailing text")),
    };
    if cursor.position != trimmed.len() {
        return Err(invalid(input, "unexpected trailing text"));
    }
    Ok((local, offset))
}

#[cfg(test)]
mod tests;
