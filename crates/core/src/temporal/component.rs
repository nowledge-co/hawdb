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

//! Component access (`d.year`, `dt.epochMillis`, `duration.minutes`, ...).

use chrono::{Datelike, FixedOffset, NaiveDate, NaiveTime, Timelike};

use super::construct::kind_name;
use super::format::offset_string;
use super::parse::iso_weekday;
use super::{Duration, TemporalError, TemporalResult};
use crate::Value;

fn date_component(date: NaiveDate, key: &str) -> Option<i64> {
    Some(match key {
        "year" => i64::from(date.year()),
        "quarter" => i64::from((date.month() - 1) / 3 + 1),
        "month" => i64::from(date.month()),
        "week" => i64::from(date.iso_week().week()),
        "weekyear" => i64::from(date.iso_week().year()),
        "dayofquarter" => {
            let start = NaiveDate::from_ymd_opt(date.year(), (date.month() - 1) / 3 * 3 + 1, 1)?;
            (date - start).num_days() + 1
        }
        "day" => i64::from(date.day()),
        "ordinalday" | "dayofyear" => i64::from(date.ordinal()),
        "dayofweek" | "weekday" => iso_weekday(date),
        _ => return None,
    })
}

fn time_component(time: NaiveTime, key: &str) -> Option<i64> {
    Some(match key {
        "hour" => i64::from(time.hour()),
        "minute" => i64::from(time.minute()),
        "second" => i64::from(time.second()),
        "millisecond" => i64::from(time.nanosecond() / 1_000_000),
        "microsecond" => i64::from(time.nanosecond() / 1_000),
        "nanosecond" => i64::from(time.nanosecond()),
        _ => return None,
    })
}

fn offset_component(offset: FixedOffset, key: &str) -> Option<Value> {
    let seconds = offset.local_minus_utc();
    Some(match key {
        "timezone" | "offset" => Value::String(offset_string(offset)),
        "offsetminutes" => Value::Int(i64::from(seconds / 60)),
        "offsetseconds" => Value::Int(i64::from(seconds)),
        _ => return None,
    })
}

fn duration_component(duration: &Duration, key: &str) -> Option<i64> {
    let seconds = duration.seconds();
    let nanos = duration.nanos_of_second();
    Some(match key {
        "years" => duration.months / 12,
        "quarters" => duration.months / 3,
        "months" => duration.months,
        "weeks" => duration.days / 7,
        "days" => duration.days,
        "hours" => seconds / 3_600,
        "minutes" => seconds / 60,
        "seconds" => seconds,
        "milliseconds" => seconds.checked_mul(1_000)?.checked_add(nanos / 1_000_000)?,
        "microseconds" => seconds.checked_mul(1_000_000)?.checked_add(nanos / 1_000)?,
        "nanoseconds" => seconds.checked_mul(1_000_000_000)?.checked_add(nanos)?,
        "quartersofyear" => duration.months / 3 % 4,
        "monthsofquarter" => duration.months % 3,
        "monthsofyear" => duration.months % 12,
        "daysofweek" => duration.days % 7,
        "minutesofhour" => seconds / 60 % 60,
        "secondsofminute" => seconds % 60,
        "millisecondsofsecond" => nanos / 1_000_000,
        "microsecondsofsecond" => nanos / 1_000,
        "nanosecondsofsecond" => nanos,
        _ => return None,
    })
}

/// Reads the component `key` (case-insensitive) of a temporal value. Unknown
/// components are errors; `epochMillis`/`epochSeconds` and the offset
/// fields exist only on zoned datetimes, as in openCypher.
pub fn component(value: &Value, key: &str) -> TemporalResult<Value> {
    let normalized = key.to_ascii_lowercase();
    let key_lower = normalized.as_str();
    let result = match value {
        Value::Date(date) => date_component(*date, key_lower).map(Value::Int),
        Value::LocalTime(time) => time_component(*time, key_lower).map(Value::Int),
        Value::LocalDateTime(value) => date_component(value.date(), key_lower)
            .or_else(|| time_component(value.time(), key_lower))
            .map(Value::Int),
        Value::DateTime(value) => date_component(value.date_naive(), key_lower)
            .or_else(|| time_component(value.time(), key_lower))
            .map(Value::Int)
            .or_else(|| offset_component(*value.offset(), key_lower))
            .or_else(|| match key_lower {
                "epochseconds" => Some(Value::Int(value.timestamp())),
                "epochmillis" => Some(Value::Int(value.timestamp_millis())),
                _ => None,
            }),
        Value::Duration(duration) => duration_component(duration, key_lower).map(Value::Int),
        other => {
            return Err(TemporalError::new(format!(
                "cannot read component '{key}' of a {} value",
                kind_name(other)
            )))
        }
    };
    result.ok_or_else(|| {
        TemporalError::new(format!(
            "{} value has no component '{key}'",
            kind_name(value)
        ))
    })
}
