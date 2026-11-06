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

use super::*;

fn ts(text: &str) -> i64 {
    parse_timestamp(text).unwrap()
}

#[test]
fn parses_postgres_and_iso_forms() {
    let expected = 1_793_880_000_000_000; // 2026-11-05 12:00:00
    assert_eq!(ts("2026-11-05 12:00:00"), expected);
    assert_eq!(ts("2026-11-05T12:00:00"), expected);
    assert_eq!(ts("  2026-11-05 12:00  "), expected);
    assert_eq!(ts("2026-11-05"), expected - 12 * 3600 * MICROS_PER_SECOND);
    assert_eq!(ts("2026-11-05 12:00:00.25"), expected + 250_000);
    assert_eq!(ts("1969-12-31 23:59:59.999999"), -1);
    assert_eq!(ts("1970-01-01"), 0);
    assert_eq!(ts("-0001-01-01"), ts("-0001-01-01 00:00:00"));
}

#[test]
fn rejects_offsets_and_malformed_timestamp_text() {
    for text in [
        "2026-11-05 12:00:00+02",
        "2026-11-05T12:00:00Z",
        "2026-11-05 12:00:00 -05:00",
        "2026-11-05 12",
        "2026-13-01",
        "2026-02-30",
        "2026-11-05 24:00:00",
        "2026-11-05 12:00:00.",
        "26-11-05",
        "20261105",
        "now",
        "infinity",
        "2026-11-05 12:00:00 BC",
        "",
    ] {
        assert!(parse_timestamp(text).is_err(), "{text}");
    }
}

#[test]
fn timestamptz_applies_offsets_and_defaults_to_utc() {
    let utc = parse_timestamptz("2026-11-05 12:00:00").unwrap();
    assert_eq!(parse_timestamptz("2026-11-05T12:00:00Z").unwrap(), utc);
    assert_eq!(parse_timestamptz("2026-11-05 14:00:00+02").unwrap(), utc);
    assert_eq!(
        parse_timestamptz("2026-11-05 14:30:00 +02:30").unwrap(),
        utc
    );
    assert_eq!(parse_timestamptz("2026-11-05 07:00:00-0500").unwrap(), utc);
    assert!(parse_timestamptz("2026-11-05 12:00:00+16").is_err());
    assert!(parse_timestamptz("2026-11-05 12:00:00+02x").is_err());
    assert_eq!(format_timestamptz(utc).unwrap(), "2026-11-05 12:00:00+00");
}

#[test]
fn sub_microsecond_input_rounds_half_up() {
    let base = ts("2026-11-05 12:00:00");
    assert_eq!(ts("2026-11-05 12:00:00.0000004"), base);
    assert_eq!(ts("2026-11-05 12:00:00.0000005"), base + 1);
    assert_eq!(ts("2026-11-05 12:00:00.9999995"), base + MICROS_PER_SECOND);
    assert_eq!(ts("1969-12-31 23:59:59.9999995"), 0);
    let naive = NaiveDateTime::parse_from_str("2026-11-05 12:00:00.000000500", "%F %T%.f").unwrap();
    assert_eq!(from_naive(naive).unwrap(), base + 1);
}

#[test]
fn precision_rounds_halfway_away_from_postgres_epoch() {
    let p0 = TimestampPrecision::new(0).unwrap();
    let p3 = TimestampPrecision::new(3).unwrap();
    let base = ts("2026-11-05 12:00:00");
    assert_eq!(p0.round(base + 499_999).unwrap(), base);
    assert_eq!(p0.round(base + 500_000).unwrap(), base + MICROS_PER_SECOND);
    assert_eq!(p3.round(base + 1_500).unwrap(), base + 2_000);
    assert_eq!(p3.round(base + 1_499).unwrap(), base + 1_000);
    // Before 2000-01-01 halfway values round toward the earlier instant,
    // also on both sides of the Unix epoch.
    let early = ts("1999-12-31 23:59:59");
    assert_eq!(p0.round(early + 500_000).unwrap(), early);
    assert_eq!(p0.round(-500_000).unwrap(), -MICROS_PER_SECOND);
    assert_eq!(p0.round(500_000).unwrap(), 0);
    assert_eq!(p3.round(-1_500).unwrap(), -2_000);
    assert!(p3.accepts(base + 2_000));
    assert!(!p3.accepts(base + 2_001));
    assert!(TimestampPrecision::new(7).is_none());
    assert_eq!(
        TimestampPrecision::DEFAULT.round(base + 1).unwrap(),
        base + 1
    );
}

#[test]
fn range_edges_and_overflow() {
    let (min, max) = (min_micros(), max_micros());
    assert!(min < 0 && max > 0);
    assert_eq!(checked(min).unwrap(), min);
    assert_eq!(checked(max).unwrap(), max);
    assert!(checked(min - 1).is_err());
    assert!(checked(max + 1).is_err());
    assert!(checked(i64::MAX).is_err());
    assert!(to_naive(i64::MIN).is_err());
    let max_text = format_timestamp(max).unwrap();
    assert_eq!(parse_timestamp(&max_text).unwrap(), max);
    let min_text = format_timestamp(min).unwrap();
    assert_eq!(parse_timestamp(&min_text).unwrap(), min);
    assert!(TimestampPrecision::new(0).unwrap().round(max).is_err());
    assert!(parse_timestamp(&format!("{max_text}5")).is_err());
    let beyond = format!("+{}-01-01", NaiveDateTime::MAX.year() + 1);
    assert!(parse_timestamp(&beyond).is_err());
}

#[test]
fn formats_postgres_iso_text_and_converts_values() {
    assert_eq!(format_timestamp(0).unwrap(), "1970-01-01 00:00:00");
    assert_eq!(format_timestamp(-1).unwrap(), "1969-12-31 23:59:59.999999");
    assert_eq!(
        format_timestamp(ts("2026-11-05 12:00:00.5")).unwrap(),
        "2026-11-05 12:00:00.5"
    );
    assert_eq!(
        format_timestamp(ts("-0044-03-15")).unwrap(),
        "-0044-03-15 00:00:00"
    );
    assert_eq!(
        format_timestamp(ts("+12345-01-01")).unwrap(),
        "+12345-01-01 00:00:00"
    );
    let micros = ts("2026-11-05 12:00:00.123456");
    let Value::LocalDateTime(local) = local_value(micros).unwrap() else {
        panic!("local datetime");
    };
    assert_eq!(from_naive(local).unwrap(), micros);
    let Value::DateTime(zoned) = zoned_value(micros).unwrap() else {
        panic!("datetime");
    };
    assert_eq!(zoned.offset().local_minus_utc(), 0);
    assert_eq!(from_datetime(zoned).unwrap(), micros);
}

#[test]
fn casts_and_conversions_between_text_and_timestamp_types() {
    let local = TimestampType {
        zoned: false,
        precision: TimestampPrecision::DEFAULT,
    };
    let zoned = TimestampType {
        zoned: true,
        precision: TimestampPrecision::new(0).unwrap(),
    };
    let text = Value::String("2026-11-05 12:00:00.5".into());
    let micros = ts("2026-11-05 12:00:00.5");
    assert_eq!(local.convert(&text).unwrap(), Some(micros));
    assert_eq!(local.convert(&Value::Null).unwrap(), None);
    // TIMESTAMPTZ(0) rounds .5 away from the PostgreSQL epoch.
    assert_eq!(zoned.convert(&text).unwrap(), Some(micros + 500_000));
    let cast = TimestampCast::Timestamp(local).apply(&text).unwrap();
    assert!(matches!(cast, Value::LocalDateTime(_)));
    assert_eq!(
        TimestampCast::Text.apply(&cast).unwrap(),
        Value::String("2026-11-05 12:00:00.5".into())
    );
    let zoned_value = TimestampCast::Timestamp(zoned)
        .apply(&Value::String("2026-11-05 14:00:00+02".into()))
        .unwrap();
    assert_eq!(
        TimestampCast::Text.apply(&zoned_value).unwrap(),
        Value::String("2026-11-05 12:00:00+00".into())
    );
    // Local and zoned values convert through UTC.
    assert_eq!(
        local.convert(&zoned_value).unwrap(),
        Some(ts("2026-11-05 12:00:00"))
    );
    assert!(local.convert(&Value::Int(1)).is_err());
    assert!(TimestampCast::Text.apply(&Value::Int(1)).is_err());
    assert_eq!(
        TimestampCast::Text.apply(&Value::Null).unwrap(),
        Value::Null
    );
    assert_eq!(TimestampCast::Timestamp(local).to_string(), "TIMESTAMP(6)");
}

#[test]
fn documented_range_matches_chrono() {
    assert_eq!(
        format_timestamp(min_micros()).unwrap(),
        "-262143-01-01 00:00:00"
    );
    assert_eq!(
        format_timestamp(max_micros()).unwrap(),
        "+262142-12-31 23:59:59.999999"
    );
}
