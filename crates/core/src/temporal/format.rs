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

//! ISO 8601 rendering of temporal values, as used by `Display`, string
//! conversion and the tagged JSON bindings.

use std::fmt::{Formatter, Write};

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use super::Duration;

fn write_year(out: &mut impl Write, year: i32) -> std::fmt::Result {
    if (0..=9999).contains(&year) {
        write!(out, "{year:04}")
    } else if year < 0 {
        write!(out, "-{:04}", year.unsigned_abs())
    } else {
        write!(out, "+{year}")
    }
}

pub(super) fn write_date(out: &mut impl Write, date: NaiveDate) -> std::fmt::Result {
    write_year(out, date.year())?;
    write!(out, "-{:02}-{:02}", date.month(), date.day())
}

/// Writes `.fff`, `.ffffff` or `.fffffffff`, whichever is the shortest exact
/// form, or nothing for a whole second.
fn write_fraction(out: &mut impl Write, nanos: u32) -> std::fmt::Result {
    if nanos == 0 {
        Ok(())
    } else if nanos.is_multiple_of(1_000_000) {
        write!(out, ".{:03}", nanos / 1_000_000)
    } else if nanos.is_multiple_of(1_000) {
        write!(out, ".{:06}", nanos / 1_000)
    } else {
        write!(out, ".{nanos:09}")
    }
}

pub(super) fn write_time(out: &mut impl Write, time: NaiveTime) -> std::fmt::Result {
    write!(
        out,
        "{:02}:{:02}:{:02}",
        time.hour(),
        time.minute(),
        time.second()
    )?;
    write_fraction(out, time.nanosecond())
}

pub(super) fn write_offset(out: &mut impl Write, offset: FixedOffset) -> std::fmt::Result {
    let seconds = offset.local_minus_utc();
    if seconds == 0 {
        return out.write_char('Z');
    }
    let sign = if seconds < 0 { '-' } else { '+' };
    let seconds = seconds.unsigned_abs();
    write!(out, "{sign}{:02}:{:02}", seconds / 3_600, seconds / 60 % 60)?;
    if !seconds.is_multiple_of(60) {
        write!(out, ":{:02}", seconds % 60)?;
    }
    Ok(())
}

pub(super) fn offset_string(offset: FixedOffset) -> String {
    let mut out = String::new();
    write_offset(&mut out, offset).expect("writing to a string succeeds");
    if out == "Z" {
        "+00:00".to_string()
    } else {
        out
    }
}

pub fn format_date(date: NaiveDate) -> String {
    let mut out = String::new();
    write_date(&mut out, date).expect("writing to a string succeeds");
    out
}

pub fn format_local_time(time: NaiveTime) -> String {
    let mut out = String::new();
    write_time(&mut out, time).expect("writing to a string succeeds");
    out
}

pub fn format_local_datetime(value: NaiveDateTime) -> String {
    let mut out = String::new();
    write_date(&mut out, value.date()).expect("writing to a string succeeds");
    out.push('T');
    write_time(&mut out, value.time()).expect("writing to a string succeeds");
    out
}

pub fn format_datetime(value: DateTime<FixedOffset>) -> String {
    let mut out = format_local_datetime(value.naive_local());
    write_offset(&mut out, *value.offset()).expect("writing to a string succeeds");
    out
}

/// Writes `P[nY][nM][nD][T[nH][nM][n.fS]]`, or `PT0S` for zero. Components
/// keep their own sign, as openCypher prints them.
pub(super) fn write_duration(f: &mut Formatter<'_>, duration: &Duration) -> std::fmt::Result {
    let years = duration.months / 12;
    let months = duration.months % 12;
    let nanos = duration.time_nanos();
    let sign = if nanos < 0 { "-" } else { "" };
    let magnitude = nanos.unsigned_abs();
    let hours = magnitude / 3_600_000_000_000;
    let minutes = magnitude / 60_000_000_000 % 60;
    let seconds = magnitude / 1_000_000_000 % 60;
    let fraction = (magnitude % 1_000_000_000) as u32;
    f.write_char('P')?;
    if years != 0 {
        write!(f, "{years}Y")?;
    }
    if months != 0 {
        write!(f, "{months}M")?;
    }
    if duration.days != 0 {
        write!(f, "{}D", duration.days)?;
    }
    if magnitude != 0 {
        f.write_char('T')?;
        if hours != 0 {
            write!(f, "{sign}{hours}H")?;
        }
        if minutes != 0 {
            write!(f, "{sign}{minutes}M")?;
        }
        if seconds != 0 || fraction != 0 {
            write!(f, "{sign}{seconds}")?;
            if fraction != 0 {
                let digits = format!("{fraction:09}");
                write!(f, ".{}", digits.trim_end_matches('0'))?;
            }
            f.write_char('S')?;
        }
    } else if duration.months == 0 && duration.days == 0 {
        f.write_str("T0S")?;
    }
    Ok(())
}
