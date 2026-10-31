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

//! Canonical checkpoint encoding of temporal values: append-only tags,
//! golden bytes, exact length accounting and fail-closed decoding.

use super::*;
use hawdb_core::temporal;

fn samples() -> Vec<Value> {
    vec![
        Value::Date(temporal::parse_date("2026-10-30").unwrap()),
        Value::Date(temporal::NaiveDate::MAX),
        Value::LocalTime(temporal::parse_local_time("00:00:00.000000001").unwrap()),
        Value::LocalDateTime(temporal::parse_local_datetime("-0001-12-31T23:59:59").unwrap()),
        Value::DateTime(temporal::parse_datetime("2026-10-30T12:00:00+02:00").unwrap()),
        Value::DateTime(temporal::parse_datetime("1969-12-31T23:59:59.999999999-14:00").unwrap()),
        Value::duration(temporal::parse_duration("P1Y2M3DT4H5M6.000000007S").unwrap()),
        Value::duration(temporal::parse_duration("-PT0.5S").unwrap()),
    ]
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn temporal_canonical_tags_have_golden_bytes() {
    let golden = [
        // tag 11 + i32 days since 0001-01-01 (day 1)
        (
            Value::Date(temporal::parse_date("2026-10-30").unwrap()),
            "0b4f4a0b00",
        ),
        // tag 12 + u64 nanoseconds of day (12:00 = 43_200e9)
        (
            Value::LocalTime(temporal::parse_local_time("12:00").unwrap()),
            "0c0080a7484a270000",
        ),
        // tag 13 + date + time
        (
            Value::LocalDateTime(temporal::parse_local_datetime("2026-10-30T12:00").unwrap()),
            "0d4f4a0b000080a7484a270000",
        ),
        // tag 14 + UTC date + UTC time + i32 offset seconds
        (
            Value::DateTime(temporal::parse_datetime("2026-10-30T12:00:00+02:00").unwrap()),
            "0e4f4a0b00004036e7bd200000201c0000",
        ),
        // tag 15 + i64 months + i64 days + i64 seconds + u32 nanoseconds
        (
            Value::duration(temporal::parse_duration("P1M2DT-0.5S").unwrap()),
            "0f01000000000000000200000000000000ffffffffffffffff0065cd1d",
        ),
    ];
    for (value, expected) in golden {
        let encoded = encode_standalone_value(&value).unwrap();
        assert_eq!(hex(&encoded), expected, "{value}");
        assert_eq!(decode_standalone_value(&encoded).unwrap(), value);
    }
}

#[test]
fn temporal_values_round_trip_through_every_canonical_path() {
    let values = samples();
    for value in values.iter().cloned().chain([
        Value::List(values.clone()),
        Value::Map(BTreeMap::from([(
            "nested".to_string(),
            Value::List(values.clone()),
        )])),
    ]) {
        let encoded = encode_standalone_value(&value).unwrap();
        assert_eq!(decode_standalone_value(&encoded).unwrap(), value);
        // Length precomputation, streaming writer and validation agree.
        assert_eq!(
            encoded_value_len_with_work(&value, 0, None).unwrap(),
            encoded.len() as u64
        );
        let mut streamed = Vec::new();
        write_value_streaming(&mut streamed, &value, 0).unwrap();
        assert_eq!(streamed, encoded);
        let mut cursor = SliceCursor::new(&encoded);
        validate_encoded_value(&mut cursor, 0, None).unwrap();
        assert!(cursor.is_empty());
        standalone_value_allocation_bytes(&encoded).unwrap();
    }
}

#[test]
fn temporal_canonical_payloads_fail_closed() {
    let date =
        encode_standalone_value(&Value::Date(temporal::parse_date("2026-10-30").unwrap())).unwrap();
    // Truncated payload, out-of-range day, bad sub-day nanoseconds, bad
    // offset and bad duration nanoseconds are corruption on every path.
    let mut corrupt = vec![date[..4].to_vec()];
    corrupt.push([&[11u8][..], &i32::MAX.to_le_bytes()].concat());
    corrupt.push([&[12u8][..], &(86_400u64 * 1_000_000_000).to_le_bytes()].concat());
    let mut zoned = encode_standalone_value(&Value::DateTime(
        temporal::parse_datetime("2026-10-30T12:00:00Z").unwrap(),
    ))
    .unwrap();
    zoned[13..].copy_from_slice(&90_000i32.to_le_bytes());
    corrupt.push(zoned);
    let mut duration = vec![15u8];
    duration.extend_from_slice(&[0; 24]);
    duration.extend_from_slice(&1_000_000_000u32.to_le_bytes());
    corrupt.push(duration);
    for bytes in corrupt {
        assert!(decode_standalone_value(&bytes).is_err(), "{}", hex(&bytes));
        assert!(
            standalone_value_allocation_bytes(&bytes).is_err(),
            "{}",
            hex(&bytes)
        );
        assert!(validate_encoded_value(&mut SliceCursor::new(&bytes), 0, None).is_err());
    }
    // The next unassigned tag is unknown: a binary that predates a value tag
    // rejects it instead of misreading the record, as binaries older than
    // these temporal tags reject tags 11..=15.
    for unknown in [16u8, 255] {
        let bytes = [unknown, 0, 0, 0, 0];
        let error = decode_standalone_value(&bytes).unwrap_err().to_string();
        assert!(error.contains("unknown canonical value tag"), "{error}");
        assert!(validate_encoded_value(&mut SliceCursor::new(&bytes), 0, None).is_err());
    }
}
