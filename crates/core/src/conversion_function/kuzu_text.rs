// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Kùzu 0.11 `string(x)` text rendering.
//!
//! Nowledge Mem stores text produced by Kùzu's cast (thread version stamps),
//! so this must reproduce Kùzu's bytes, not openCypher's `toString`. Each
//! format below was measured against Kùzu 0.11.3; see
//! `docs/CYPHER_LOAD_CSV.md` (type conversion functions) for the table.

use std::fmt::Write;

use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};

use crate::temporal::Duration;
use crate::{HawDBError, Result, ValueRef};

const MICROS_PER_SECOND: u128 = 1_000_000;
const MICROS_PER_MINUTE: u128 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: u128 = 60 * MICROS_PER_MINUTE;

/// The exact byte length of the Kùzu text of a non-NULL value, computed
/// without allocating, so callers can charge a budget before [`render`].
/// Types Kùzu has no faithful counterpart for are errors.
pub(super) fn rendered_len(value: ValueRef<'_>) -> Result<usize> {
    let mut counter = ByteCounter(0);
    write_value(&mut counter, value)?;
    Ok(counter.0)
}

/// Renders a non-NULL value as Kùzu's `string(x)` does into a buffer of
/// exactly `len` bytes, the result of [`rendered_len`] for the same value.
pub(super) fn render(value: ValueRef<'_>, len: usize) -> Result<String> {
    let mut out = String::with_capacity(len);
    write_value(&mut out, value)?;
    debug_assert_eq!(out.len(), len, "rendered_len must be exact");
    Ok(out)
}

/// Counts written bytes; never allocates.
struct ByteCounter(usize);

impl Write for ByteCounter {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
        Ok(())
    }
}

fn put<W: Write>(out: &mut W, text: &str) -> Result<()> {
    out.write_str(text).map_err(text_too_long)
}

fn put_fmt<W: Write>(out: &mut W, arguments: std::fmt::Arguments<'_>) -> Result<()> {
    out.write_fmt(arguments).map_err(text_too_long)
}

fn text_too_long(_: std::fmt::Error) -> HawDBError {
    HawDBError::Execution("string() result length overflows usize".to_owned())
}

fn write_value<W: Write>(out: &mut W, value: ValueRef<'_>) -> Result<()> {
    match value {
        // Only reachable inside lists and maps: Kùzu writes nothing.
        ValueRef::Null => {}
        ValueRef::Bool(value) => put(out, if value { "True" } else { "False" })?,
        ValueRef::Int(value) => put_fmt(out, format_args!("{value}"))?,
        ValueRef::Float(value) => write_float(out, value)?,
        ValueRef::String(value) => put(out, value)?,
        ValueRef::Binary(bytes) => write_blob(out, bytes)?,
        ValueRef::Uuid(value) => put_fmt(out, format_args!("{}", value.hyphenated()))?,
        ValueRef::Vector(vector) => {
            // Kùzu FLOAT[n] arrays print each coordinate as a float.
            put(out, "[")?;
            for (index, coordinate) in vector.coordinates().enumerate() {
                if index > 0 {
                    put(out, ",")?;
                }
                write_float(out, f64::from(coordinate))?;
            }
            put(out, "]")?;
        }
        ValueRef::List(values) => {
            put(out, "[")?;
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    put(out, ",")?;
                }
                write_value(out, value.as_ref())?;
            }
            put(out, "]")?;
        }
        // HawDB map literals correspond to Kùzu STRUCT literals, which Kùzu
        // renders as `{key: value, ...}`.
        ValueRef::Map(entries) => {
            put(out, "{")?;
            for (index, (key, value)) in entries.iter().enumerate() {
                if index > 0 {
                    put(out, ", ")?;
                }
                put(out, key)?;
                put(out, ": ")?;
                write_value(out, value.as_ref())?;
            }
            put(out, "}")?;
        }
        ValueRef::Date(date) => write_date(out, date)?,
        ValueRef::LocalDateTime(value) => write_timestamp(out, value)?,
        // Kùzu TIMESTAMP stores the UTC instant and prints it without a zone.
        ValueRef::DateTime(value) => write_timestamp(out, value.naive_utc())?,
        ValueRef::Duration(value) => write_interval(out, value)?,
        ValueRef::Decimal(_) | ValueRef::LocalTime(_) => {
            return Err(HawDBError::Execution(format!(
                "string() has no Kùzu-compatible text for a {} value",
                super::type_name(value)
            )))
        }
    }
    Ok(())
}

/// C `%f`: six decimals, exact rounding, `inf`/`nan` spelled in lower case
/// with the sign bit shown.
fn write_float<W: Write>(out: &mut W, value: f64) -> Result<()> {
    if value.is_nan() {
        put(
            out,
            if value.is_sign_negative() {
                "-nan"
            } else {
                "nan"
            },
        )
    } else if value.is_infinite() {
        put(out, if value > 0.0 { "inf" } else { "-inf" })
    } else {
        put_fmt(out, format_args!("{value:.6}"))
    }
}

/// Kùzu BLOB text: printable ASCII except `\`, `'` and `"` as is, every other
/// byte as `\xHH`.
fn write_blob<W: Write>(out: &mut W, bytes: &[u8]) -> Result<()> {
    for &byte in bytes {
        if (0x20..=0x7e).contains(&byte) && !matches!(byte, b'\\' | b'\'' | b'"') {
            out.write_char(char::from(byte)).map_err(text_too_long)?;
        } else {
            put_fmt(out, format_args!("\\x{byte:02X}"))?;
        }
    }
    Ok(())
}

/// `YYYY-MM-DD`; years before 1 AD print as the BC year with ` (BC)`.
fn write_date<W: Write>(out: &mut W, date: NaiveDate) -> Result<()> {
    let year = date.year();
    let (year, era) = if year <= 0 {
        (1 - i64::from(year), " (BC)")
    } else {
        (i64::from(year), "")
    };
    put_fmt(
        out,
        format_args!("{year:04}-{:02}-{:02}{era}", date.month(), date.day()),
    )
}

/// `YYYY-MM-DD hh:mm:ss[.ffffff]` with trailing fractional zeros removed.
/// Kùzu TIMESTAMP holds microseconds, so finer digits are truncated.
fn write_timestamp<W: Write>(out: &mut W, value: NaiveDateTime) -> Result<()> {
    write_date(out, value.date())?;
    put_fmt(
        out,
        format_args!(
            " {:02}:{:02}:{:02}",
            value.hour(),
            value.minute(),
            value.second()
        ),
    )?;
    // A leap second carries 1_000_000_000.. nanoseconds; keep the fraction.
    write_micros_fraction(out, (value.nanosecond() % 1_000_000_000) / 1_000)
}

/// `.ffffff` without trailing zeros, or nothing for zero.
fn write_micros_fraction<W: Write>(out: &mut W, micros: u32) -> Result<()> {
    if micros == 0 {
        return Ok(());
    }
    let (mut digits, mut width) = (micros, 6);
    while digits % 10 == 0 {
        digits /= 10;
        width -= 1;
    }
    put_fmt(out, format_args!(".{digits:0width$}"))
}

/// Kùzu INTERVAL text: `N year[s] N month[s] N day[s] [-]hh:mm:ss[.ffffff]`,
/// omitting zero parts, or `00:00:00` when everything is zero. Months are not
/// folded into days and hours are not folded into days.
fn write_interval<W: Write>(out: &mut W, duration: &Duration) -> Result<()> {
    // Parts are separated within this value only, so `out` (which may hold
    // an enclosing list prefix) is never inspected.
    let mut first = true;
    let mut separate = |out: &mut W| {
        let separator = if first { "" } else { " " };
        first = false;
        put(out, separator)
    };
    // Truncating division, as Kùzu's C++ does: -13 months is -1 year -1 month.
    for (count, unit) in [
        (duration.months / 12, "year"),
        (duration.months % 12, "month"),
        (duration.days, "day"),
    ] {
        if count != 0 {
            separate(out)?;
            let plural = if count == 1 { "" } else { "s" };
            put_fmt(out, format_args!("{count} {unit}{plural}"))?;
        }
    }
    // Kùzu INTERVAL holds microseconds; finer digits are truncated.
    let micros = duration.time_nanos() / 1_000;
    if micros != 0 {
        separate(out)?;
        if micros < 0 {
            put(out, "-")?;
        }
        let micros = micros.unsigned_abs();
        let hours = micros / MICROS_PER_HOUR;
        let minutes = micros % MICROS_PER_HOUR / MICROS_PER_MINUTE;
        let seconds = micros % MICROS_PER_MINUTE / MICROS_PER_SECOND;
        put_fmt(out, format_args!("{hours:02}:{minutes:02}:{seconds:02}"))?;
        write_micros_fraction(
            out,
            u32::try_from(micros % MICROS_PER_SECOND).expect("below one second"),
        )?;
    } else if first {
        put(out, "00:00:00")?;
    }
    Ok(())
}
