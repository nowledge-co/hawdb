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

//! Constructors: strings, maps, conversions between temporal kinds, the
//! statement clock and epoch values.

use std::collections::BTreeMap;

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};

use super::parse::{parse_offset, weekday_from_number};
use super::{
    parse_date, parse_datetime, parse_duration, parse_local_datetime, parse_local_time, Duration,
    TemporalError, TemporalKind, TemporalResult, AVERAGE_DAYS_PER_MONTH, NANOS_PER_DAY,
    NANOS_PER_SECOND,
};
use crate::Value;

fn utc() -> FixedOffset {
    FixedOffset::east_opt(0).expect("UTC offset")
}

/// A numeric duration component.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Number {
    Int(i64),
    Float(f64),
    /// An exact decimal from ISO text: `±whole.fraction` with `digits`
    /// fractional digits (at most 18).
    Decimal {
        negative: bool,
        whole: u64,
        fraction: u64,
        digits: u32,
    },
}

impl Number {
    fn as_f64(self) -> f64 {
        match self {
            Self::Int(value) => value as f64,
            Self::Float(value) => value,
            Self::Decimal {
                negative,
                whole,
                fraction,
                digits,
            } => {
                let magnitude = whole as f64 + fraction as f64 / 10f64.powi(digits as i32);
                if negative {
                    -magnitude
                } else {
                    magnitude
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum DurationUnit {
    Months(i128),
    Days(i128),
    Nanos(i128),
}

fn duration_unit(key: &str) -> Option<DurationUnit> {
    Some(match key.to_ascii_lowercase().as_str() {
        "years" => DurationUnit::Months(12),
        "quarters" => DurationUnit::Months(3),
        "months" => DurationUnit::Months(1),
        "weeks" => DurationUnit::Days(7),
        "days" => DurationUnit::Days(1),
        "hours" => DurationUnit::Nanos(3_600 * NANOS_PER_SECOND),
        "minutes" => DurationUnit::Nanos(60 * NANOS_PER_SECOND),
        "seconds" => DurationUnit::Nanos(NANOS_PER_SECOND),
        "milliseconds" => DurationUnit::Nanos(1_000_000),
        "microseconds" => DurationUnit::Nanos(1_000),
        "nanoseconds" => DurationUnit::Nanos(1),
        _ => return None,
    })
}

fn out_of_range() -> TemporalError {
    TemporalError::new("duration is out of range")
}

/// Accumulates duration components exactly for integers. Fractional values
/// cascade downward: fractional months become days through the average
/// month, fractional days become nanoseconds.
#[derive(Debug, Default)]
pub(crate) struct DurationBuilder {
    months: i128,
    days: i128,
    nanos: i128,
    fractional_months: f64,
    fractional_days: f64,
    fractional_nanos: f64,
}

impl DurationBuilder {
    pub(crate) fn add(&mut self, key: &str, value: Number) -> TemporalResult<()> {
        let unit = duration_unit(key)
            .ok_or_else(|| TemporalError::new(format!("unknown duration component '{key}'")))?;
        let (target, fraction, factor) = match unit {
            DurationUnit::Months(factor) => (&mut self.months, &mut self.fractional_months, factor),
            DurationUnit::Days(factor) => (&mut self.days, &mut self.fractional_days, factor),
            DurationUnit::Nanos(factor) => (&mut self.nanos, &mut self.fractional_nanos, factor),
        };
        match value {
            Number::Int(value) => {
                *target = i128::from(value)
                    .checked_mul(factor)
                    .and_then(|value| target.checked_add(value))
                    .ok_or_else(out_of_range)?;
            }
            Number::Decimal {
                negative,
                whole,
                fraction: digits_value,
                digits,
            } if matches!(unit, DurationUnit::Nanos(_)) => {
                // Exact: ISO seconds keep all nine fractional digits.
                let scale = 10i128.pow(digits);
                let magnitude = i128::from(whole)
                    .checked_mul(factor)
                    .and_then(|value| value.checked_add(i128::from(digits_value) * factor / scale))
                    .ok_or_else(out_of_range)?;
                let signed = if negative { -magnitude } else { magnitude };
                *target = target.checked_add(signed).ok_or_else(out_of_range)?;
            }
            value @ (Number::Float(_) | Number::Decimal { .. }) => {
                let total = value.as_f64() * factor as f64;
                if !total.is_finite() || total.abs() >= 1e36 {
                    return Err(out_of_range());
                }
                let whole = total.trunc();
                *target = target.checked_add(whole as i128).ok_or_else(out_of_range)?;
                *fraction += total - whole;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> TemporalResult<Duration> {
        let days_from_months = self.fractional_months * AVERAGE_DAYS_PER_MONTH;
        let whole_days = days_from_months.trunc();
        let fractional_days = self.fractional_days + (days_from_months - whole_days);
        let nanos_from_days = fractional_days * NANOS_PER_DAY as f64 + self.fractional_nanos;
        let days = self
            .days
            .checked_add(whole_days as i128)
            .ok_or_else(out_of_range)?;
        let nanos = self
            .nanos
            .checked_add(nanos_from_days.round() as i128)
            .ok_or_else(out_of_range)?;
        Duration::from_parts(
            i64::try_from(self.months).map_err(|_| out_of_range())?,
            i64::try_from(days).map_err(|_| out_of_range())?,
            nanos,
        )
        .ok_or_else(out_of_range)
    }
}

/// `duration(string | map | duration)`.
pub(crate) fn duration(value: &Value) -> TemporalResult<Value> {
    match value {
        Value::Duration(duration) => Ok(Value::duration(**duration)),
        Value::String(text) => parse_duration(text).map(Value::duration),
        Value::Map(map) => {
            let mut builder = DurationBuilder::default();
            for (key, value) in map {
                let number = match value {
                    Value::Null => continue,
                    Value::Int(value) => Number::Int(*value),
                    Value::Float(value) => Number::Float(*value),
                    _ => {
                        return Err(TemporalError::new(format!(
                            "duration component '{key}' must be a number"
                        )))
                    }
                };
                builder.add(key, number)?;
            }
            builder.finish().map(Value::duration)
        }
        other => Err(TemporalError::new(format!(
            "duration() expects a string or map, got {}",
            kind_name(other)
        ))),
    }
}

pub(crate) fn kind_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::Bool(_) => "BOOLEAN",
        Value::Int(_) => "INTEGER",
        Value::Float(_) => "FLOAT",
        Value::String(_) => "STRING",
        Value::Binary(_) => "BINARY",
        Value::Vector(_) => "VECTOR",
        Value::Uuid(_) => "UUID",
        Value::List(_) => "LIST",
        Value::Map(_) => "MAP",
        Value::Date(_) => "DATE",
        Value::LocalTime(_) => "LOCAL TIME",
        Value::LocalDateTime(_) => "LOCAL DATETIME",
        Value::DateTime(_) => "ZONED DATETIME",
        Value::Duration(_) => "DURATION",
    }
}

/// Builds a temporal instant of `kind` from no argument (the clock), a
/// string, a map or another temporal value.
pub fn construct(
    kind: TemporalKind,
    argument: Option<&Value>,
    now: DateTime<Utc>,
) -> TemporalResult<Value> {
    match argument {
        None => Ok(from_datetime(kind, now.fixed_offset())),
        Some(Value::Null) => Ok(Value::Null),
        Some(Value::String(text)) => match kind {
            TemporalKind::Date => parse_date(text).map(Value::Date),
            TemporalKind::LocalTime => parse_local_time(text).map(Value::LocalTime),
            TemporalKind::LocalDateTime => parse_local_datetime(text).map(Value::LocalDateTime),
            TemporalKind::DateTime => parse_datetime(text).map(Value::DateTime),
        },
        Some(Value::Map(map)) => from_map(kind, map, now),
        Some(value) => convert(kind, value),
    }
}

fn from_datetime(kind: TemporalKind, value: DateTime<FixedOffset>) -> Value {
    match kind {
        TemporalKind::Date => Value::Date(value.date_naive()),
        TemporalKind::LocalTime => Value::LocalTime(value.time()),
        TemporalKind::LocalDateTime => Value::LocalDateTime(value.naive_local()),
        TemporalKind::DateTime => Value::DateTime(value),
    }
}

/// Converts one temporal value to another kind, keeping its local fields.
/// Local values become zoned datetimes in UTC.
pub(crate) fn convert(kind: TemporalKind, value: &Value) -> TemporalResult<Value> {
    let converted = match (kind, value) {
        (TemporalKind::Date, Value::Date(date)) => Some(Value::Date(*date)),
        (TemporalKind::Date, Value::LocalDateTime(value)) => Some(Value::Date(value.date())),
        (TemporalKind::Date, Value::DateTime(value)) => Some(Value::Date(value.date_naive())),
        (TemporalKind::LocalTime, Value::LocalTime(time)) => Some(Value::LocalTime(*time)),
        (TemporalKind::LocalTime, Value::LocalDateTime(value)) => {
            Some(Value::LocalTime(value.time()))
        }
        (TemporalKind::LocalTime, Value::DateTime(value)) => Some(Value::LocalTime(value.time())),
        (TemporalKind::LocalDateTime, Value::LocalDateTime(value)) => {
            Some(Value::LocalDateTime(*value))
        }
        (TemporalKind::LocalDateTime, Value::DateTime(value)) => {
            Some(Value::LocalDateTime(value.naive_local()))
        }
        (TemporalKind::LocalDateTime, Value::Date(date)) => {
            Some(Value::LocalDateTime(date.and_time(NaiveTime::MIN)))
        }
        (TemporalKind::DateTime, Value::DateTime(value)) => Some(Value::DateTime(*value)),
        (TemporalKind::DateTime, Value::LocalDateTime(value)) => {
            attach(*value, utc()).map(Value::DateTime)
        }
        (TemporalKind::DateTime, Value::Date(date)) => {
            attach(date.and_time(NaiveTime::MIN), utc()).map(Value::DateTime)
        }
        _ => None,
    };
    converted.ok_or_else(|| {
        TemporalError::new(format!(
            "{}() cannot convert a {} value",
            kind.function_name(),
            kind_name(value)
        ))
    })
}

pub(crate) fn attach(value: NaiveDateTime, offset: FixedOffset) -> Option<DateTime<FixedOffset>> {
    value.and_local_timezone(offset).single()
}

/// Case-insensitive view of a constructor map. NULL entries are absent.
struct Fields<'a> {
    function: &'static str,
    values: BTreeMap<String, &'a Value>,
}

impl<'a> Fields<'a> {
    fn new(
        function: &'static str,
        map: &'a BTreeMap<String, Value>,
        allowed: &[&str],
    ) -> TemporalResult<Self> {
        let mut values = BTreeMap::new();
        for (key, value) in map {
            let normalized = key.to_ascii_lowercase();
            if !allowed
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(key))
            {
                return Err(TemporalError::new(format!(
                    "{function}() does not accept the map key '{key}'"
                )));
            }
            if matches!(value, Value::Null) {
                continue;
            }
            if values.insert(normalized, value).is_some() {
                return Err(TemporalError::new(format!(
                    "{function}() map key '{key}' is given more than once"
                )));
            }
        }
        Ok(Self { function, values })
    }

    fn has(&self, key: &str) -> bool {
        self.values.contains_key(&key.to_ascii_lowercase())
    }

    fn value(&self, key: &str) -> Option<&'a Value> {
        self.values.get(&key.to_ascii_lowercase()).copied()
    }

    fn int(&self, key: &str) -> TemporalResult<Option<i64>> {
        match self.value(key) {
            None => Ok(None),
            Some(Value::Int(value)) => Ok(Some(*value)),
            Some(_) => Err(TemporalError::new(format!(
                "{}() map key '{key}' must be an integer",
                self.function
            ))),
        }
    }

    fn small(&self, key: &str) -> TemporalResult<Option<u32>> {
        self.int(key)?
            .map(|value| {
                u32::try_from(value).map_err(|_| {
                    TemporalError::new(format!(
                        "{}() map key '{key}' is out of range: {value}",
                        self.function
                    ))
                })
            })
            .transpose()
    }

    fn invalid(&self) -> TemporalError {
        TemporalError::new(format!(
            "{}() map does not describe a valid value",
            self.function
        ))
    }
}

const DATE_KEYS: [&str; 8] = [
    "year",
    "month",
    "day",
    "week",
    "dayOfWeek",
    "ordinalDay",
    "quarter",
    "dayOfQuarter",
];
const TIME_KEYS: [&str; 6] = [
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
    "nanosecond",
];

fn date_fields(fields: &Fields<'_>, base: Option<NaiveDate>) -> TemporalResult<Option<NaiveDate>> {
    if !DATE_KEYS.iter().any(|key| fields.has(key)) {
        return Ok(base);
    }
    let year = fields.int("year")?;
    let year = match year {
        Some(year) => Some(i32::try_from(year).map_err(|_| fields.invalid())?),
        None => None,
    };
    let month = fields.small("month")?;
    let day = fields.small("day")?;
    let week = fields.small("week")?;
    let day_of_week = fields.small("dayOfWeek")?;
    let ordinal = fields.small("ordinalDay")?;
    let quarter = fields.small("quarter")?;
    let day_of_quarter = fields.small("dayOfQuarter")?;
    let groups = [
        month.is_some() || day.is_some(),
        week.is_some() || day_of_week.is_some(),
        ordinal.is_some(),
        quarter.is_some() || day_of_quarter.is_some(),
    ]
    .into_iter()
    .filter(|group| *group)
    .count();
    if groups > 1 {
        return Err(TemporalError::new(format!(
            "{}() map mixes calendar, week, ordinal and quarter fields",
            fields.function
        )));
    }
    if let Some(base) = base {
        if week.is_some() || day_of_week.is_some() || ordinal.is_some() || quarter.is_some() {
            return Err(TemporalError::new(format!(
                "{}() supports only year, month and day overrides of a base date",
                fields.function
            )));
        }
        return NaiveDate::from_ymd_opt(
            year.unwrap_or(base.year()),
            month.unwrap_or(base.month()),
            day.unwrap_or(base.day()),
        )
        .map(Some)
        .ok_or_else(|| fields.invalid());
    }
    let Some(year) = year else {
        return Err(TemporalError::new(format!(
            "{}() map requires 'year'",
            fields.function
        )));
    };
    let date = if week.is_some() || day_of_week.is_some() {
        let week = week.ok_or_else(|| {
            TemporalError::new(format!("{}() 'dayOfWeek' requires 'week'", fields.function))
        })?;
        weekday_from_number(day_of_week.unwrap_or(1))
            .and_then(|weekday| NaiveDate::from_isoywd_opt(year, week, weekday))
    } else if let Some(ordinal) = ordinal {
        NaiveDate::from_yo_opt(year, ordinal)
    } else if quarter.is_some() || day_of_quarter.is_some() {
        let quarter = quarter.ok_or_else(|| {
            TemporalError::new(format!(
                "{}() 'dayOfQuarter' requires 'quarter'",
                fields.function
            ))
        })?;
        if !(1..=4).contains(&quarter) {
            return Err(fields.invalid());
        }
        let start = NaiveDate::from_ymd_opt(year, (quarter - 1) * 3 + 1, 1);
        let offset = day_of_quarter.unwrap_or(1);
        start
            .filter(|_| offset >= 1)
            .and_then(|start| start.checked_add_days(chrono::Days::new(u64::from(offset - 1))))
            .filter(|date| (date.month() - 1) / 3 + 1 == quarter && date.year() == year)
    } else {
        if day.is_some() && month.is_none() {
            return Err(TemporalError::new(format!(
                "{}() 'day' requires 'month'",
                fields.function
            )));
        }
        NaiveDate::from_ymd_opt(year, month.unwrap_or(1), day.unwrap_or(1))
    };
    date.map(Some).ok_or_else(|| fields.invalid())
}

fn time_fields(fields: &Fields<'_>, base: Option<NaiveTime>) -> TemporalResult<Option<NaiveTime>> {
    use chrono::Timelike;
    if !TIME_KEYS.iter().any(|key| fields.has(key)) {
        return Ok(base);
    }
    let hour = fields.small("hour")?;
    let minute = fields.small("minute")?;
    let second = fields.small("second")?;
    let millisecond = fields.small("millisecond")?;
    let microsecond = fields.small("microsecond")?;
    let nanosecond = fields.small("nanosecond")?;
    let has_fraction = millisecond.is_some() || microsecond.is_some() || nanosecond.is_some();
    if millisecond.is_some_and(|value| value >= 1_000)
        || microsecond.is_some_and(|value| value >= 1_000_000)
        || nanosecond.is_some_and(|value| value >= 1_000_000_000)
    {
        return Err(fields.invalid());
    }
    let fraction = u64::from(millisecond.unwrap_or(0)) * 1_000_000
        + u64::from(microsecond.unwrap_or(0)) * 1_000
        + u64::from(nanosecond.unwrap_or(0));
    if fraction >= 1_000_000_000 {
        return Err(fields.invalid());
    }
    let (hour, minute, second, nanos) = match base {
        Some(base) => (
            hour.unwrap_or(base.hour()),
            minute.unwrap_or(base.minute()),
            second.unwrap_or(base.second()),
            if has_fraction {
                fraction as u32
            } else {
                base.nanosecond()
            },
        ),
        None => {
            let Some(hour) = hour else {
                return Err(TemporalError::new(format!(
                    "{}() map requires 'hour' for a time",
                    fields.function
                )));
            };
            if (second.is_some() && minute.is_none()) || (has_fraction && second.is_none()) {
                return Err(TemporalError::new(format!(
                    "{}() time fields must not skip a larger unit",
                    fields.function
                )));
            }
            (
                hour,
                minute.unwrap_or(0),
                second.unwrap_or(0),
                fraction as u32,
            )
        }
    };
    if second > 59 || nanos >= 1_000_000_000 {
        return Err(fields.invalid());
    }
    NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)
        .map(Some)
        .ok_or_else(|| fields.invalid())
}

fn base_date(fields: &Fields<'_>) -> TemporalResult<Option<NaiveDate>> {
    match fields.value("date") {
        None => Ok(None),
        Some(Value::Date(date)) => Ok(Some(*date)),
        Some(Value::LocalDateTime(value)) => Ok(Some(value.date())),
        Some(Value::DateTime(value)) => Ok(Some(value.date_naive())),
        Some(value) => Err(TemporalError::new(format!(
            "{}() map key 'date' must be a temporal value with a date, got {}",
            fields.function,
            kind_name(value)
        ))),
    }
}

fn base_time(fields: &Fields<'_>) -> TemporalResult<(Option<NaiveTime>, Option<FixedOffset>)> {
    match fields.value("time") {
        None => Ok((None, None)),
        Some(Value::LocalTime(time)) => Ok((Some(*time), None)),
        Some(Value::LocalDateTime(value)) => Ok((Some(value.time()), None)),
        Some(Value::DateTime(value)) => Ok((Some(value.time()), Some(*value.offset()))),
        Some(value) => Err(TemporalError::new(format!(
            "{}() map key 'time' must be a temporal value with a time, got {}",
            fields.function,
            kind_name(value)
        ))),
    }
}

fn base_datetime(
    fields: &Fields<'_>,
) -> TemporalResult<Option<(NaiveDateTime, Option<FixedOffset>)>> {
    match fields.value("datetime") {
        None => Ok(None),
        Some(Value::LocalDateTime(value)) => Ok(Some((*value, None))),
        Some(Value::DateTime(value)) => Ok(Some((value.naive_local(), Some(*value.offset())))),
        Some(Value::Date(date)) => Ok(Some((date.and_time(NaiveTime::MIN), None))),
        Some(value) => Err(TemporalError::new(format!(
            "{}() map key 'datetime' must be a temporal value with a date, got {}",
            fields.function,
            kind_name(value)
        ))),
    }
}

fn timezone(fields: &Fields<'_>) -> TemporalResult<Option<FixedOffset>> {
    match fields.value("timezone") {
        None => Ok(None),
        Some(Value::String(text)) => parse_offset(text).map(Some),
        Some(_) => Err(TemporalError::new(format!(
            "{}() map key 'timezone' must be a string offset",
            fields.function
        ))),
    }
}

fn from_map(
    kind: TemporalKind,
    map: &BTreeMap<String, Value>,
    now: DateTime<Utc>,
) -> TemporalResult<Value> {
    let function = kind.function_name();
    let mut allowed: Vec<&str> = vec!["timezone"];
    if kind.has_date() {
        allowed.extend(DATE_KEYS);
        allowed.push("date");
    }
    if kind.has_time() {
        allowed.extend(TIME_KEYS);
        allowed.push("time");
    }
    if matches!(kind, TemporalKind::LocalDateTime | TemporalKind::DateTime) {
        allowed.push("datetime");
    }
    if kind == TemporalKind::DateTime {
        allowed.extend(["epochMillis", "epochSeconds"]);
    }
    let fields = Fields::new(function, map, &allowed)?;
    let zone = timezone(&fields)?;
    if fields.values.keys().all(|key| key == "timezone") {
        // `date({timezone: '+01:00'})`: the statement clock in that offset.
        return Ok(from_datetime(
            kind,
            now.with_timezone(&zone.unwrap_or_else(utc)),
        ));
    }
    if kind == TemporalKind::DateTime && (fields.has("epochMillis") || fields.has("epochSeconds")) {
        return epoch_from_map(&fields, zone);
    }
    if !matches!(kind, TemporalKind::DateTime) && zone.is_some() {
        return Err(TemporalError::new(format!(
            "{function}() accepts 'timezone' only to read the current clock"
        )));
    }
    match kind {
        TemporalKind::Date => {
            let base = base_date(&fields)?;
            date_fields(&fields, base)?
                .map(Value::Date)
                .ok_or_else(|| fields.invalid())
        }
        TemporalKind::LocalTime => {
            let (base, _) = base_time(&fields)?;
            time_fields(&fields, base)?
                .map(Value::LocalTime)
                .ok_or_else(|| fields.invalid())
        }
        TemporalKind::LocalDateTime | TemporalKind::DateTime => {
            let datetime_base = base_datetime(&fields)?;
            if datetime_base.is_some() && (fields.has("date") || fields.has("time")) {
                return Err(TemporalError::new(format!(
                    "{function}() map cannot combine 'datetime' with 'date' or 'time'"
                )));
            }
            let (time_base, time_offset) = base_time(&fields)?;
            let date_base = match datetime_base {
                Some((value, _)) => Some(value.date()),
                None => base_date(&fields)?,
            };
            let time_base = match datetime_base {
                Some((value, _)) => Some(value.time()),
                None => time_base,
            };
            let date = date_fields(&fields, date_base)?
                .ok_or_else(|| TemporalError::new(format!("{function}() map requires a date")))?;
            let time = time_fields(&fields, time_base)?.unwrap_or(NaiveTime::MIN);
            let local = date.and_time(time);
            if kind == TemporalKind::LocalDateTime {
                return Ok(Value::LocalDateTime(local));
            }
            let base_offset = datetime_base.and_then(|(_, offset)| offset).or(time_offset);
            match (zone, base_offset, datetime_base) {
                // A zoned base re-expressed in another offset keeps its instant
                // when no local field was overridden.
                (Some(zone), Some(offset), Some(_))
                    if !DATE_KEYS
                        .iter()
                        .chain(TIME_KEYS.iter())
                        .any(|key| fields.has(key)) =>
                {
                    attach(local, offset)
                        .map(|value| Value::DateTime(value.with_timezone(&zone)))
                        .ok_or_else(|| fields.invalid())
                }
                _ => attach(local, zone.or(base_offset).unwrap_or_else(utc))
                    .map(Value::DateTime)
                    .ok_or_else(|| fields.invalid()),
            }
        }
    }
}

fn epoch_from_map(fields: &Fields<'_>, zone: Option<FixedOffset>) -> TemporalResult<Value> {
    if fields.has("epochMillis") && fields.has("epochSeconds") {
        return Err(TemporalError::new(
            "datetime() map cannot combine 'epochMillis' and 'epochSeconds'",
        ));
    }
    if DATE_KEYS
        .iter()
        .chain(["hour", "minute", "second", "date", "time", "datetime"].iter())
        .any(|key| fields.has(key))
    {
        return Err(TemporalError::new(
            "datetime() epoch maps accept only sub-second fields and 'timezone'",
        ));
    }
    let base = match (fields.int("epochMillis")?, fields.int("epochSeconds")?) {
        (Some(millis), None) => i128::from(millis) * 1_000_000,
        (None, Some(seconds)) => i128::from(seconds) * NANOS_PER_SECOND,
        _ => unreachable!("epoch key presence was checked"),
    };
    let fraction = i128::from(fields.int("millisecond")?.unwrap_or(0)) * 1_000_000
        + i128::from(fields.int("microsecond")?.unwrap_or(0)) * 1_000
        + i128::from(fields.int("nanosecond")?.unwrap_or(0));
    let value = epoch_nanos(base + fraction)?;
    Ok(Value::DateTime(
        value.with_timezone(&zone.unwrap_or_else(utc)),
    ))
}

fn epoch_nanos(nanos: i128) -> TemporalResult<DateTime<FixedOffset>> {
    let seconds = i64::try_from(nanos.div_euclid(NANOS_PER_SECOND));
    let subsecond = nanos.rem_euclid(NANOS_PER_SECOND) as u32;
    seconds
        .ok()
        .and_then(|seconds| DateTime::from_timestamp(seconds, subsecond))
        .map(|value| value.fixed_offset())
        .ok_or_else(|| TemporalError::new("epoch value is out of the supported datetime range"))
}

/// `datetime.fromepoch(seconds, nanoseconds)` in UTC.
pub fn from_epoch(seconds: i64, nanos: i64) -> TemporalResult<Value> {
    epoch_nanos(i128::from(seconds) * NANOS_PER_SECOND + i128::from(nanos)).map(Value::DateTime)
}

/// `datetime.fromepochmillis(milliseconds)` in UTC.
pub fn from_epoch_millis(millis: i64) -> TemporalResult<Value> {
    epoch_nanos(i128::from(millis) * 1_000_000).map(Value::DateTime)
}
