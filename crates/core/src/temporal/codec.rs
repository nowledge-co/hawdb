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

use super::types::*;

use crate::temporal::{Date, OffsetDateTime, PlainDateTime, Time};

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

fn date_days(date: Date) -> i32 {
    date.to_julian_day() - 1_721_425
}

fn time_nanos(time: Time) -> u64 {
    (u64::from(time.hour()) * 3_600 + u64::from(time.minute()) * 60 + u64::from(time.second()))
        * 1_000_000_000
        + u64::from(time.nanosecond())
}

fn write_date(date: Date, out: &mut Vec<u8>) {
    out.extend_from_slice(&date_days(date).to_le_bytes());
}

fn write_time(time: Time, out: &mut Vec<u8>) {
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
            // OffsetDateTime guarantees the local date's range. Its UTC date
            // may be just outside that range at an extreme fixed offset.
            let nanos = value.unix_timestamp_nanos();
            let day = (nanos.div_euclid(super::NANOS_PER_DAY) + 719_163) as i32;
            out.extend_from_slice(&day.to_le_bytes());
            out.extend_from_slice(&(nanos.rem_euclid(super::NANOS_PER_DAY) as u64).to_le_bytes());
            out.extend_from_slice(&value.offset().whole_seconds().to_le_bytes());
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

fn read_date(kind: TemporalValueKind, bytes: &[u8], at: usize) -> TemporalResult<Date> {
    i32::from_le_bytes(read_array(bytes, at))
        .checked_add(1_721_425)
        .and_then(|day| Date::from_julian_day(day).ok())
        .ok_or_else(|| corrupt(kind, "day number is out of range"))
}

fn read_time(kind: TemporalValueKind, bytes: &[u8], at: usize) -> TemporalResult<Time> {
    let nanos = u64::from_le_bytes(read_array(bytes, at));
    if nanos >= 86_400 * 1_000_000_000 {
        return Err(corrupt(kind, "time of day is out of range"));
    }
    time_from_hms_nano(
        (nanos / 3_600_000_000_000) as u32,
        (nanos / 60_000_000_000 % 60) as u32,
        (nanos / 1_000_000_000 % 60) as u32,
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
        TemporalValueKind::LocalDateTime => Value::LocalDateTime(PlainDateTime::new(
            read_date(kind, bytes, 0)?,
            read_time(kind, bytes, 4)?,
        )),
        TemporalValueKind::DateTime => {
            let days = i32::from_le_bytes(read_array(bytes, 0));
            let time = read_time(kind, bytes, 4)?;
            let offset = offset_from_seconds(i32::from_le_bytes(read_array(bytes, 12)))
                .ok_or_else(|| corrupt(kind, "offset is out of range"))?;
            let local_nanos = (i128::from(days) - 719_163) * super::NANOS_PER_DAY
                + i128::from(time_nanos(time))
                + i128::from(offset.whole_seconds()) * super::NANOS_PER_SECOND;
            let local = OffsetDateTime::from_unix_timestamp_nanos(local_nanos)
                .ok()
                .map(local_datetime)
                .ok_or_else(|| corrupt(kind, "local datetime is out of range"))?;
            Value::DateTime(local.assume_offset(offset))
        }
        TemporalValueKind::Duration => {
            let months = i64::from_le_bytes(read_array(bytes, 0));
            let days = i64::from_le_bytes(read_array(bytes, 8));
            let seconds = i64::from_le_bytes(read_array(bytes, 16));
            let nanos = u32::from_le_bytes(read_array(bytes, 24));
            if nanos >= 1_000_000_000 {
                return Err(corrupt(kind, "nanoseconds are out of range"));
            }
            let time =
                super::delta_from_nanos(i128::from(seconds) * 1_000_000_000 + i128::from(nanos))
                    .ok_or_else(|| corrupt(kind, "time part is out of range"))?;
            Value::duration(
                Duration::new(months, days, time)
                    .ok_or_else(|| corrupt(kind, "time part is out of range"))?,
            )
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
            Value::Date(Date::MIN),
            Value::Date(Date::MAX),
            Value::LocalTime(time_from_hms_nano(23, 59, 59, 999_999_999).unwrap()),
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
    fn native_host_offsets_round_trip_at_calendar_edges() {
        use super::super::UtcOffset;
        use std::hash::{Hash, Hasher};
        for (local, seconds) in [
            (PlainDateTime::MIN, 25 * 3_600),
            (PlainDateTime::MAX, -25 * 3_600),
        ] {
            let value = Value::DateTime(
                local.assume_offset(UtcOffset::from_whole_seconds(seconds).unwrap()),
            );
            let bytes = payload(&value).unwrap();
            assert_eq!(
                decode_payload(TemporalValueKind::DateTime, &bytes).unwrap(),
                value
            );
            let mut hash = std::collections::hash_map::DefaultHasher::new();
            value.hash(&mut hash);
            let _ = hash.finish();
        }
        assert!(Duration::new(0, 0, super::super::SignedDuration::MAX).is_none());
        assert!(Duration::new(0, 0, super::super::SignedDuration::MIN).is_none());
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
        datetime[12..].copy_from_slice(&(26 * 3_600i32).to_le_bytes());
        assert!(decode_payload(TemporalValueKind::DateTime, &datetime).is_err());
        let mut duration = vec![0u8; 28];
        duration[24..].copy_from_slice(&1_000_000_000u32.to_le_bytes());
        assert!(decode_payload(TemporalValueKind::Duration, &duration).is_err());
    }
}
