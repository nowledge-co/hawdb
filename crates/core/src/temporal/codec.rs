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

//! The fixed-width binary payload shared by every persistent temporal
//! encoding (WAL, canonical checkpoint, executor spill).
//!
//! All integers are little-endian. Payloads are validated completely on
//! decode: out-of-range days, sub-day nanoseconds, offsets or duration
//! nanoseconds are corruption, never clamped.
//!
//! | kind            | bytes | layout                                              |
//! |-----------------|-------|-----------------------------------------------------|
//! | date            | 4     | `i32` days since 0001-01-01 (day 1)                 |
//! | local time      | 8     | `u64` nanoseconds since midnight                    |
//! | local datetime  | 12    | date + local time                                   |
//! | zoned datetime  | 16    | UTC date + UTC time + `i32` offset seconds east     |
//! | duration        | 28    | `i64` months, `i64` days, `i64` seconds, `u32` nanos|

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use super::{Duration, TemporalError, TemporalResult};
use crate::Value;

/// The persistent kind of a temporal payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TemporalValueKind {
    Date,
    LocalTime,
    LocalDateTime,
    DateTime,
    Duration,
}

impl TemporalValueKind {
    pub const ALL: [Self; 5] = [
        Self::Date,
        Self::LocalTime,
        Self::LocalDateTime,
        Self::DateTime,
        Self::Duration,
    ];

    /// The exact payload length of this kind.
    pub const fn payload_len(self) -> usize {
        match self {
            Self::Date => 4,
            Self::LocalTime => 8,
            Self::LocalDateTime => 12,
            Self::DateTime => 16,
            Self::Duration => 28,
        }
    }

    /// Heap bytes owned by an in-memory value of this kind: durations are
    /// boxed so that `Value` stays 32 bytes; every other kind is inline.
    pub const fn heap_bytes(self) -> usize {
        match self {
            Self::Duration => std::mem::size_of::<Duration>(),
            Self::Date | Self::LocalTime | Self::LocalDateTime | Self::DateTime => 0,
        }
    }

    pub const fn of(value: &Value) -> Option<Self> {
        Some(match value {
            Value::Date(_) => Self::Date,
            Value::LocalTime(_) => Self::LocalTime,
            Value::LocalDateTime(_) => Self::LocalDateTime,
            Value::DateTime(_) => Self::DateTime,
            Value::Duration(_) => Self::Duration,
            _ => return None,
        })
    }
}

fn corrupt(kind: TemporalValueKind, detail: &str) -> TemporalError {
    TemporalError::new(format!("corrupt {kind:?} temporal payload: {detail}"))
}

fn date_days(date: NaiveDate) -> i32 {
    date.num_days_from_ce()
}

fn time_nanos(time: NaiveTime) -> u64 {
    u64::from(time.num_seconds_from_midnight()) * 1_000_000_000 + u64::from(time.nanosecond())
}

fn write_date(date: NaiveDate, out: &mut Vec<u8>) {
    out.extend_from_slice(&date_days(date).to_le_bytes());
}

fn write_time(time: NaiveTime, out: &mut Vec<u8>) {
    out.extend_from_slice(&time_nanos(time).to_le_bytes());
}

/// Appends the payload of a temporal value and returns its kind, or returns
/// `None` (writing nothing) for any other value.
pub fn encode_payload(value: &Value, out: &mut Vec<u8>) -> Option<TemporalValueKind> {
    match value {
        Value::Date(date) => write_date(*date, out),
        Value::LocalTime(time) => write_time(*time, out),
        Value::LocalDateTime(value) => {
            write_date(value.date(), out);
            write_time(value.time(), out);
        }
        Value::DateTime(value) => {
            let utc = value.naive_utc();
            write_date(utc.date(), out);
            write_time(utc.time(), out);
            out.extend_from_slice(&value.offset().local_minus_utc().to_le_bytes());
        }
        Value::Duration(duration) => {
            out.extend_from_slice(&duration.months.to_le_bytes());
            out.extend_from_slice(&duration.days.to_le_bytes());
            out.extend_from_slice(&duration.seconds().to_le_bytes());
            out.extend_from_slice(&(duration.nanos_of_second() as u32).to_le_bytes());
        }
        _ => return None,
    }
    TemporalValueKind::of(value)
}

/// The payload of a temporal value as a new vector.
pub fn payload(value: &Value) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(28);
    encode_payload(value, &mut out).map(|_| out)
}

fn read_array<const N: usize>(bytes: &[u8], at: usize) -> [u8; N] {
    bytes[at..at + N]
        .try_into()
        .expect("payload length checked")
}

fn read_date(kind: TemporalValueKind, bytes: &[u8], at: usize) -> TemporalResult<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(i32::from_le_bytes(read_array(bytes, at)))
        .ok_or_else(|| corrupt(kind, "day number is out of range"))
}

fn read_time(kind: TemporalValueKind, bytes: &[u8], at: usize) -> TemporalResult<NaiveTime> {
    let nanos = u64::from_le_bytes(read_array(bytes, at));
    if nanos >= 86_400 * 1_000_000_000 {
        return Err(corrupt(kind, "time of day is out of range"));
    }
    NaiveTime::from_num_seconds_from_midnight_opt(
        (nanos / 1_000_000_000) as u32,
        (nanos % 1_000_000_000) as u32,
    )
    .ok_or_else(|| corrupt(kind, "time of day is out of range"))
}

/// Decodes a complete payload of `kind`. The slice must have exactly the
/// kind's payload length.
pub fn decode_payload(kind: TemporalValueKind, bytes: &[u8]) -> TemporalResult<Value> {
    if bytes.len() != kind.payload_len() {
        return Err(corrupt(
            kind,
            &format!(
                "expected {} bytes, found {}",
                kind.payload_len(),
                bytes.len()
            ),
        ));
    }
    Ok(match kind {
        TemporalValueKind::Date => Value::Date(read_date(kind, bytes, 0)?),
        TemporalValueKind::LocalTime => Value::LocalTime(read_time(kind, bytes, 0)?),
        TemporalValueKind::LocalDateTime => Value::LocalDateTime(NaiveDateTime::new(
            read_date(kind, bytes, 0)?,
            read_time(kind, bytes, 4)?,
        )),
        TemporalValueKind::DateTime => {
            let utc = NaiveDateTime::new(read_date(kind, bytes, 0)?, read_time(kind, bytes, 4)?);
            let offset = FixedOffset::east_opt(i32::from_le_bytes(read_array(bytes, 12)))
                .ok_or_else(|| corrupt(kind, "offset is out of range"))?;
            // Reject instants whose local representation leaves chrono's range.
            utc.checked_add_offset(offset)
                .ok_or_else(|| corrupt(kind, "local datetime is out of range"))?;
            Value::DateTime(DateTime::from_naive_utc_and_offset(utc, offset))
        }
        TemporalValueKind::Duration => {
            let months = i64::from_le_bytes(read_array(bytes, 0));
            let days = i64::from_le_bytes(read_array(bytes, 8));
            let seconds = i64::from_le_bytes(read_array(bytes, 16));
            let nanos = u32::from_le_bytes(read_array(bytes, 24));
            if nanos >= 1_000_000_000 {
                return Err(corrupt(kind, "nanoseconds are out of range"));
            }
            let time = chrono::TimeDelta::new(seconds, nanos)
                .ok_or_else(|| corrupt(kind, "time part is out of range"))?;
            Value::duration(Duration::new(months, days, time))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temporal::{parse_date, parse_datetime, parse_duration, parse_local_datetime};

    fn samples() -> Vec<Value> {
        vec![
            Value::Date(parse_date("2026-10-30").unwrap()),
            Value::Date(NaiveDate::MIN),
            Value::Date(NaiveDate::MAX),
            Value::LocalTime(NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_999).unwrap()),
            Value::LocalDateTime(parse_local_datetime("1970-01-01T00:00:00.000000001").unwrap()),
            Value::DateTime(parse_datetime("2026-10-30T12:00:00+02:00").unwrap()),
            Value::DateTime(parse_datetime("2026-10-30T12:00:00-00:30:15").unwrap()),
            Value::duration(parse_duration("P1Y2M3DT4H5M6.000000007S").unwrap()),
            Value::duration(parse_duration("-P1Y2M3DT4H5M6.5S").unwrap()),
        ]
    }

    #[test]
    fn payloads_round_trip_exactly() {
        for value in samples() {
            let mut bytes = Vec::new();
            let kind = encode_payload(&value, &mut bytes).unwrap();
            assert_eq!(bytes.len(), kind.payload_len());
            assert_eq!(decode_payload(kind, &bytes).unwrap(), value, "{value}");
        }
        assert_eq!(encode_payload(&Value::Int(1), &mut Vec::new()), None);
    }

    #[test]
    fn payload_bytes_are_golden() {
        // 2026-10-30 is day 739_919 counting 0001-01-01 as day 1.
        let date = Value::Date(parse_date("2026-10-30").unwrap());
        assert_eq!(hex(&payload(&date).unwrap()), "4f4a0b00");
        let datetime = Value::DateTime(parse_datetime("2026-10-30T12:00:00+02:00").unwrap());
        // UTC 2026-10-30, 10:00 = 36_000e9 ns, offset +7200 s.
        assert_eq!(
            hex(&payload(&datetime).unwrap()),
            "4f4a0b00004036e7bd200000201c0000"
        );
        let duration = Value::duration(parse_duration("P1M2DT-0.5S").unwrap());
        assert_eq!(
            hex(&payload(&duration).unwrap()),
            "01000000000000000200000000000000ffffffffffffffff0065cd1d"
        );
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn corrupt_payloads_fail_closed() {
        let kinds = TemporalValueKind::ALL;
        for kind in kinds {
            assert!(decode_payload(kind, &vec![0; kind.payload_len() + 1]).is_err());
            assert!(decode_payload(kind, &[]).is_err());
        }
        assert!(decode_payload(TemporalValueKind::Date, &i32::MAX.to_le_bytes()).is_err());
        assert!(decode_payload(
            TemporalValueKind::LocalTime,
            &(86_400u64 * 1_000_000_000).to_le_bytes()
        )
        .is_err());
        let mut datetime = payload(&Value::DateTime(
            parse_datetime("2026-10-30T12:00:00Z").unwrap(),
        ))
        .unwrap();
        datetime[12..].copy_from_slice(&86_400i32.to_le_bytes());
        assert!(decode_payload(TemporalValueKind::DateTime, &datetime).is_err());
        let mut duration = vec![0u8; 28];
        duration[24..].copy_from_slice(&1_000_000_000u32.to_le_bytes());
        assert!(decode_payload(TemporalValueKind::Duration, &duration).is_err());
    }
}
