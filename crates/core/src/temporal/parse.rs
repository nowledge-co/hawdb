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

//! ISO 8601 parsing for the openCypher temporal string forms.

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Weekday};

use super::construct::DurationBuilder;
use super::{Duration, TemporalError, TemporalResult};

fn invalid(kind: &str, input: &str) -> TemporalError {
    TemporalError::new(format!("invalid {kind} string: '{input}'"))
}

fn all_digits(input: &str) -> bool {
    !input.is_empty() && input.bytes().all(|byte| byte.is_ascii_digit())
}

fn number<T: std::str::FromStr>(input: &str, kind: &str, full: &str) -> TemporalResult<T> {
    if !all_digits(input) {
        return Err(invalid(kind, full));
    }
    input.parse().map_err(|_| invalid(kind, full))
}

/// Parses `YYYY-MM-DD`, `YYYYMMDD`, `YYYY-MM`, `YYYY`, `YYYY-Www[-D]`,
/// `YYYYWww[D]`, `YYYY-DDD` and `YYYYDDD`. Years outside `0000..=9999`
/// need an explicit sign.
pub fn parse_date(input: &str) -> TemporalResult<NaiveDate> {
    let text = input.trim();
    let (negative, unsigned) = match text.as_bytes().first() {
        Some(b'+') => (false, &text[1..]),
        Some(b'-') => (true, &text[1..]),
        _ => (false, text),
    };
    let signed = unsigned.len() != text.len();
    let year_len = if signed {
        unsigned
            .bytes()
            .take_while(u8::is_ascii_digit)
            .count()
            .max(4)
    } else {
        4
    };
    if unsigned.len() < year_len {
        return Err(invalid("date", input));
    }
    let (year_text, rest) = unsigned.split_at(year_len);
    let mut year: i32 = number(year_text, "date", input)?;
    if negative {
        year = -year;
    }
    let date = if rest.is_empty() {
        NaiveDate::from_ymd_opt(year, 1, 1)
    } else if let Some(rest) = rest.strip_prefix('-') {
        parse_date_rest(year, rest, true, input)?
    } else {
        parse_date_rest(year, rest, false, input)?
    };
    date.ok_or_else(|| invalid("date", input))
}

fn parse_date_rest(
    year: i32,
    rest: &str,
    extended: bool,
    input: &str,
) -> TemporalResult<Option<NaiveDate>> {
    if let Some(week) = rest.strip_prefix(['W', 'w']) {
        let (week_text, day_text) = if extended {
            match week.split_once('-') {
                Some((week, day)) => (week, Some(day)),
                None => (week, None),
            }
        } else if week.len() == 3 {
            (&week[..2], Some(&week[2..]))
        } else {
            (week, None)
        };
        if week_text.len() != 2 || day_text.is_some_and(|day| day.len() != 1) {
            return Err(invalid("date", input));
        }
        let week: u32 = number(week_text, "date", input)?;
        let day: u32 = day_text
            .map(|day| number(day, "date", input))
            .transpose()?
            .unwrap_or(1);
        let weekday = weekday_from_number(day).ok_or_else(|| invalid("date", input))?;
        return Ok(NaiveDate::from_isoywd_opt(year, week, weekday));
    }
    if extended {
        return match rest.split_once('-') {
            Some((month, day)) if month.len() == 2 && day.len() == 2 => {
                Ok(NaiveDate::from_ymd_opt(
                    year,
                    number(month, "date", input)?,
                    number(day, "date", input)?,
                ))
            }
            None if rest.len() == 2 => Ok(NaiveDate::from_ymd_opt(
                year,
                number(rest, "date", input)?,
                1,
            )),
            None if rest.len() == 3 => {
                Ok(NaiveDate::from_yo_opt(year, number(rest, "date", input)?))
            }
            _ => Err(invalid("date", input)),
        };
    }
    match rest.len() {
        4 => Ok(NaiveDate::from_ymd_opt(
            year,
            number(&rest[..2], "date", input)?,
            number(&rest[2..], "date", input)?,
        )),
        3 => Ok(NaiveDate::from_yo_opt(year, number(rest, "date", input)?)),
        _ => Err(invalid("date", input)),
    }
}

pub(super) fn weekday_from_number(day: u32) -> Option<Weekday> {
    Some(match day {
        1 => Weekday::Mon,
        2 => Weekday::Tue,
        3 => Weekday::Wed,
        4 => Weekday::Thu,
        5 => Weekday::Fri,
        6 => Weekday::Sat,
        7 => Weekday::Sun,
        _ => return None,
    })
}

/// Parses `HH:MM:SS.fffffffff`, `HH:MM:SS`, `HH:MM`, `HH` and the basic
/// forms `HHMMSS[.f]` and `HHMM`. A comma is accepted as decimal mark.
pub fn parse_local_time(input: &str) -> TemporalResult<NaiveTime> {
    parse_time_text(input.trim(), input)
}

fn parse_time_text(text: &str, input: &str) -> TemporalResult<NaiveTime> {
    let (clock, fraction) = match text.find(['.', ',']) {
        Some(index) => (&text[..index], Some(&text[index + 1..])),
        None => (text, None),
    };
    let fields: Vec<&str> = if clock.contains(':') {
        clock.split(':').collect()
    } else if clock.len() % 2 == 0 && clock.len() <= 6 {
        (0..clock.len())
            .step_by(2)
            .map(|index| &clock[index..index + 2])
            .collect()
    } else {
        return Err(invalid("time", input));
    };
    if fields.is_empty() || fields.len() > 3 || fields.iter().any(|field| field.len() != 2) {
        return Err(invalid("time", input));
    }
    if fraction.is_some() && fields.len() != 3 {
        return Err(invalid("time", input));
    }
    let hour: u32 = number(fields[0], "time", input)?;
    let minute: u32 = fields
        .get(1)
        .map(|field| number(field, "time", input))
        .transpose()?
        .unwrap_or(0);
    let second: u32 = fields
        .get(2)
        .map(|field| number(field, "time", input))
        .transpose()?
        .unwrap_or(0);
    let nanos = fraction
        .map(|fraction| parse_fraction(fraction, input))
        .transpose()?
        .unwrap_or(0);
    if second > 59 {
        return Err(invalid("time", input));
    }
    NaiveTime::from_hms_nano_opt(hour, minute, second, nanos).ok_or_else(|| invalid("time", input))
}

fn parse_fraction(fraction: &str, input: &str) -> TemporalResult<u32> {
    if fraction.is_empty() || fraction.len() > 9 || !all_digits(fraction) {
        return Err(invalid("time", input));
    }
    let mut nanos: u32 = number(fraction, "time", input)?;
    for _ in fraction.len()..9 {
        nanos *= 10;
    }
    Ok(nanos)
}

fn split_date_time<'a>(
    text: &'a str,
    input: &str,
    kind: &str,
) -> TemporalResult<(&'a str, Option<&'a str>)> {
    match text.find(['T', 't']) {
        Some(index) => {
            let time = &text[index + 1..];
            if time.is_empty() {
                return Err(invalid(kind, input));
            }
            Ok((&text[..index], Some(time)))
        }
        None => Ok((text, None)),
    }
}

/// Parses `<date>[T<time>]`. A missing time is midnight.
pub fn parse_local_datetime(input: &str) -> TemporalResult<NaiveDateTime> {
    let text = input.trim();
    let (date, time) = split_date_time(text, input, "local datetime")?;
    let date = parse_date(date).map_err(|_| invalid("local datetime", input))?;
    let time = time
        .map(|time| parse_time_text(time, input).map_err(|_| invalid("local datetime", input)))
        .transpose()?
        .unwrap_or(NaiveTime::MIN);
    Ok(date.and_time(time))
}

/// Parses `<date>[T<time>[<offset>]]`, where the offset is `Z`, `±HH`,
/// `±HHMM`, `±HH:MM` or `±HH:MM:SS`. A missing offset means UTC. Named
/// time-zone regions (`[Europe/Stockholm]`) are refused.
pub fn parse_datetime(input: &str) -> TemporalResult<DateTime<FixedOffset>> {
    let text = input.trim();
    if text.contains('[') {
        return Err(TemporalError::new(format!(
            "named time zones are not supported, use a UTC offset: '{input}'"
        )));
    }
    let (date, time) = split_date_time(text, input, "datetime")?;
    let date = parse_date(date).map_err(|_| invalid("datetime", input))?;
    let (time, offset) = match time {
        None => (
            NaiveTime::MIN,
            FixedOffset::east_opt(0).expect("UTC offset"),
        ),
        Some(time) => {
            let (clock, offset) = split_offset(time);
            let offset = match offset {
                Some(offset) => parse_offset(offset).map_err(|_| invalid("datetime", input))?,
                None => FixedOffset::east_opt(0).expect("UTC offset"),
            };
            (
                parse_time_text(clock, input).map_err(|_| invalid("datetime", input))?,
                offset,
            )
        }
    };
    date.and_time(time)
        .and_local_timezone(offset)
        .single()
        .ok_or_else(|| invalid("datetime", input))
}

fn split_offset(time: &str) -> (&str, Option<&str>) {
    match time.find(['Z', 'z', '+', '-']) {
        Some(index) => (&time[..index], Some(&time[index..])),
        None => (time, None),
    }
}

/// Parses a UTC offset: `Z`, `UTC`, `±HH`, `±HHMM`, `±HH:MM` or `±HH:MM:SS`.
pub(crate) fn parse_offset(input: &str) -> TemporalResult<FixedOffset> {
    let text = input.trim();
    if text.eq_ignore_ascii_case("Z") || text.eq_ignore_ascii_case("UTC") {
        return Ok(FixedOffset::east_opt(0).expect("UTC offset"));
    }
    let sign = match text.as_bytes().first() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return Err(invalid_offset(input)),
    };
    let body = &text[1..];
    let fields: Vec<&str> = if body.contains(':') {
        body.split(':').collect()
    } else if body.len() == 2 || body.len() == 4 || body.len() == 6 {
        (0..body.len())
            .step_by(2)
            .map(|index| &body[index..index + 2])
            .collect()
    } else {
        return Err(invalid_offset(input));
    };
    if fields.is_empty() || fields.len() > 3 || fields.iter().any(|field| field.len() != 2) {
        return Err(invalid_offset(input));
    }
    let mut seconds = 0i32;
    for (field, scale) in fields.iter().zip([3_600, 60, 1]) {
        let value: i32 = number(field, "offset", input)?;
        if (scale != 3_600 && value > 59) || (scale == 3_600 && value > 18) {
            return Err(invalid_offset(input));
        }
        seconds += value * scale;
    }
    if seconds > 18 * 3_600 {
        return Err(invalid_offset(input));
    }
    FixedOffset::east_opt(sign * seconds).ok_or_else(|| invalid_offset(input))
}

fn invalid_offset(input: &str) -> TemporalError {
    if input.contains('/') {
        TemporalError::new(format!(
            "named time zones are not supported, use a UTC offset: '{input}'"
        ))
    } else {
        invalid("time zone offset", input)
    }
}

/// Parses an ISO 8601 duration `[-]P[nY][nM][nW][nD][T[nH][nM][nS]]`.
/// Components may be negative or fractional; fractions cascade into smaller
/// units (a fractional month uses the average month of 30.436875 days).
pub fn parse_duration(input: &str) -> TemporalResult<Duration> {
    let text = input.trim();
    let (negate, text) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let Some(body) = text.strip_prefix(['P', 'p']) else {
        return Err(invalid("duration", input));
    };
    let (date_part, time_part) = match body.find(['T', 't']) {
        Some(index) => (&body[..index], Some(&body[index + 1..])),
        None => (body, None),
    };
    if body.is_empty() || time_part.is_some_and(str::is_empty) {
        return Err(invalid("duration", input));
    }
    let mut builder = DurationBuilder::default();
    parse_duration_section(date_part, &["Y", "M", "W", "D"], &mut builder, input)?;
    if let Some(time_part) = time_part {
        parse_duration_section(time_part, &["H", "M", "S"], &mut builder, input)?;
    }
    let duration = builder.finish()?;
    if negate {
        duration
            .checked_neg()
            .ok_or_else(|| TemporalError::new("duration is out of range"))
    } else {
        Ok(duration)
    }
}

fn parse_duration_section(
    section: &str,
    units: &[&str],
    builder: &mut DurationBuilder,
    input: &str,
) -> TemporalResult<()> {
    let time = units.len() == 3;
    let mut rest = section;
    let mut next_unit = 0usize;
    while !rest.is_empty() {
        let end = rest
            .find(|ch: char| ch.is_ascii_alphabetic())
            .ok_or_else(|| invalid("duration", input))?;
        let (number_text, tail) = rest.split_at(end);
        let unit = tail[..1].to_ascii_uppercase();
        rest = &tail[1..];
        let position = units[next_unit..]
            .iter()
            .position(|candidate| *candidate == unit)
            .ok_or_else(|| invalid("duration", input))?;
        next_unit += position + 1;
        let value = parse_duration_number(number_text, input)?;
        let key = match (time, unit.as_str()) {
            (false, "Y") => "years",
            (false, "M") => "months",
            (false, "W") => "weeks",
            (false, "D") => "days",
            (true, "H") => "hours",
            (true, "M") => "minutes",
            (true, "S") => "seconds",
            _ => return Err(invalid("duration", input)),
        };
        builder.add(key, value)?;
    }
    Ok(())
}

fn parse_duration_number(text: &str, input: &str) -> TemporalResult<super::construct::Number> {
    use super::construct::Number;
    let normalized = text.replace(',', ".");
    let (negative, unsigned) = match normalized.as_bytes().first() {
        Some(b'-') => (true, &normalized[1..]),
        Some(b'+') => (false, &normalized[1..]),
        _ => (false, normalized.as_str()),
    };
    let (whole, fraction) = match unsigned.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (unsigned, None),
    };
    if !all_digits(whole) || fraction.is_some_and(|fraction| !all_digits(fraction)) {
        return Err(invalid("duration", input));
    }
    let Some(fraction) = fraction else {
        return normalized
            .parse::<i64>()
            .map(Number::Int)
            .map_err(|_| invalid("duration", input));
    };
    let fraction = fraction.trim_end_matches('0');
    let fraction = if fraction.is_empty() { "0" } else { fraction };
    if fraction.len() > 18 {
        return Err(invalid("duration", input));
    }
    Ok(Number::Decimal {
        negative,
        whole: whole.parse().map_err(|_| invalid("duration", input))?,
        fraction: fraction.parse().map_err(|_| invalid("duration", input))?,
        digits: fraction.len() as u32,
    })
}

/// The ISO day number of `date` (Monday is 1).
pub(super) fn iso_weekday(date: NaiveDate) -> i64 {
    i64::from(date.weekday().number_from_monday())
}
