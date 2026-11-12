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

/// Renders a non-NULL value as Kùzu's `string(x)` does. Types Kùzu has no
/// faithful counterpart for are errors.
pub(super) fn render(value: ValueRef<'_>) -> Result<String> {
    let mut out = String::new();
    write_value(&mut out, value)?;
    Ok(out)
}

fn write_value(out: &mut String, value: ValueRef<'_>) -> Result<()> {
    match value {
        // Only reachable inside lists and maps: Kùzu writes nothing.
        ValueRef::Null => {}
        ValueRef::Bool(value) => out.push_str(if value { "True" } else { "False" }),
        ValueRef::Int(value) => write_infallible(out, format_args!("{value}")),
        ValueRef::Float(value) => write_float(out, value),
        ValueRef::String(value) => out.push_str(value),
        ValueRef::Binary(bytes) => write_blob(out, bytes),
        ValueRef::Uuid(value) => write_infallible(out, format_args!("{}", value.hyphenated())),
        ValueRef::Vector(vector) => {
            // Kùzu FLOAT[n] arrays print each coordinate as a float.
            out.push('[');
            for (index, coordinate) in vector.coordinates().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_float(out, f64::from(coordinate));
            }
            out.push(']');
        }
        ValueRef::List(values) => {
            out.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_value(out, value.as_ref())?;
            }
            out.push(']');
        }
        // HawDB map literals correspond to Kùzu STRUCT literals, which Kùzu
        // renders as `{key: value, ...}`.
        ValueRef::Map(entries) => {
            out.push('{');
            for (index, (key, value)) in entries.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                out.push_str(key);
                out.push_str(": ");
                write_value(out, value.as_ref())?;
            }
            out.push('}');
        }
        ValueRef::Date(date) => write_date(out, date),
        ValueRef::LocalDateTime(value) => write_timestamp(out, value),
        // Kùzu TIMESTAMP stores the UTC instant and prints it without a zone.
        ValueRef::DateTime(value) => write_timestamp(out, value.naive_utc()),
        ValueRef::Duration(value) => write_interval(out, value),
        ValueRef::Decimal(_) | ValueRef::LocalTime(_) => {
            return Err(HawDBError::Execution(format!(
                "string() has no Kùzu-compatible text for a {} value",
                super::type_name(value)
            )))
        }
    }
    Ok(())
}

fn write_infallible(out: &mut String, arguments: std::fmt::Arguments<'_>) {
    out.write_fmt(arguments)
        .expect("writing to a string succeeds");
}

/// C `%f`: six decimals, exact rounding, `inf`/`nan` spelled in lower case
/// with the sign bit shown.
fn write_float(out: &mut String, value: f64) {
    if value.is_nan() {
        out.push_str(if value.is_sign_negative() {
            "-nan"
        } else {
            "nan"
        });
    } else if value.is_infinite() {
        out.push_str(if value > 0.0 { "inf" } else { "-inf" });
    } else {
        write_infallible(out, format_args!("{value:.6}"));
    }
}

/// Kùzu BLOB text: printable ASCII except `\`, `'` and `"` as is, every other
/// byte as `\xHH`.
fn write_blob(out: &mut String, bytes: &[u8]) {
    for &byte in bytes {
        if (0x20..=0x7e).contains(&byte) && !matches!(byte, b'\\' | b'\'' | b'"') {
            out.push(char::from(byte));
        } else {
            write_infallible(out, format_args!("\\x{byte:02X}"));
        }
    }
}

/// `YYYY-MM-DD`; years before 1 AD print as the BC year with ` (BC)`.
fn write_date(out: &mut String, date: NaiveDate) {
    let year = date.year();
    let (year, era) = if year <= 0 {
        (1 - i64::from(year), " (BC)")
    } else {
        (i64::from(year), "")
    };
    write_infallible(
        out,
        format_args!("{year:04}-{:02}-{:02}{era}", date.month(), date.day()),
    );
}

/// `YYYY-MM-DD hh:mm:ss[.ffffff]` with trailing fractional zeros removed.
/// Kùzu TIMESTAMP holds microseconds, so finer digits are truncated.
fn write_timestamp(out: &mut String, value: NaiveDateTime) {
    write_date(out, value.date());
    write_infallible(
        out,
        format_args!(
            " {:02}:{:02}:{:02}",
            value.hour(),
            value.minute(),
            value.second()
        ),
    );
    // A leap second carries 1_000_000_000.. nanoseconds; keep the fraction.
    write_micros_fraction(out, (value.nanosecond() % 1_000_000_000) / 1_000);
}

fn write_micros_fraction(out: &mut String, micros: u32) {
    if micros != 0 {
        let digits = format!("{micros:06}");
        out.push('.');
        out.push_str(digits.trim_end_matches('0'));
    }
}

/// Kùzu INTERVAL text: `N year[s] N month[s] N day[s] [-]hh:mm:ss[.ffffff]`,
/// omitting zero parts, or `00:00:00` when everything is zero. Months are not
/// folded into days and hours are not folded into days.
fn write_interval(out: &mut String, duration: &Duration) {
    fn part(out: &mut String, count: i64, unit: &str) {
        if count == 0 {
            return;
        }
        if !out.is_empty() {
            out.push(' ');
        }
        let plural = if count == 1 { "" } else { "s" };
        write_infallible(out, format_args!("{count} {unit}{plural}"));
    }
    // Parts are separated within this value only; a list prefix in `out`
    // must not count as a preceding part.
    let mut text = String::new();
    // Truncating division, as Kùzu's C++ does: -13 months is -1 year -1 month.
    part(&mut text, duration.months / 12, "year");
    part(&mut text, duration.months % 12, "month");
    part(&mut text, duration.days, "day");
    // Kùzu INTERVAL holds microseconds; finer digits are truncated.
    let micros = duration.time_nanos() / 1_000;
    if micros != 0 {
        if !text.is_empty() {
            text.push(' ');
        }
        if micros < 0 {
            text.push('-');
        }
        let micros = micros.unsigned_abs();
        let hours = micros / MICROS_PER_HOUR;
        let minutes = micros % MICROS_PER_HOUR / MICROS_PER_MINUTE;
        let seconds = micros % MICROS_PER_MINUTE / MICROS_PER_SECOND;
        write_infallible(
            &mut text,
            format_args!("{hours:02}:{minutes:02}:{seconds:02}"),
        );
        write_micros_fraction(
            &mut text,
            u32::try_from(micros % MICROS_PER_SECOND).expect("below one second"),
        );
    } else if text.is_empty() {
        text.push_str("00:00:00");
    }
    out.push_str(&text);
}
