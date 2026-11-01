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

use std::collections::BTreeMap;

use super::*;

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp(1_793_361_600, 0).unwrap() // 2026-10-30T12:00:00Z
}

fn call(function: TemporalFunction, arguments: &[Value]) -> TemporalResult<Value> {
    evaluate(function, arguments, now())
}

fn map(entries: &[(&str, Value)]) -> Value {
    Value::Map(
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect::<BTreeMap<_, _>>(),
    )
}

fn date(text: &str) -> Value {
    Value::Date(parse_date(text).unwrap())
}

fn datetime(text: &str) -> Value {
    Value::DateTime(parse_datetime(text).unwrap())
}

fn local_datetime(text: &str) -> Value {
    Value::LocalDateTime(parse_local_datetime(text).unwrap())
}

fn duration(text: &str) -> Value {
    Value::duration(parse_duration(text).unwrap())
}

const DATE: TemporalFunction = TemporalFunction::Construct(TemporalKind::Date);
const LOCAL_TIME: TemporalFunction = TemporalFunction::Construct(TemporalKind::LocalTime);
const LOCAL_DATETIME: TemporalFunction = TemporalFunction::Construct(TemporalKind::LocalDateTime);
const DATETIME: TemporalFunction = TemporalFunction::Construct(TemporalKind::DateTime);

#[test]
fn iso_strings_round_trip_through_display() {
    for text in [
        "2026-10-30",
        "0001-01-01",
        "-0044-03-15",
        "+12345-06-07",
        "9999-12-31",
    ] {
        assert_eq!(date(text).to_string(), text);
    }
    for text in [
        "00:00:00",
        "23:59:59.999999999",
        "12:30:00.5",
        "12:30:00.000001",
    ] {
        let value = Value::LocalTime(parse_local_time(text).unwrap());
        let rendered = value.to_string();
        assert_eq!(
            Value::LocalTime(parse_local_time(&rendered).unwrap()),
            value
        );
    }
    assert_eq!(
        Value::LocalTime(parse_local_time("12:30:00.5").unwrap()).to_string(),
        "12:30:00.500"
    );
    for text in [
        "2026-10-30T12:00:00+02:00",
        "2026-10-30T12:00:00Z",
        "2026-10-30T12:00:00.123-05:30",
        "1969-12-31T23:59:59.999999999+14:00",
    ] {
        assert_eq!(datetime(text).to_string(), text);
    }
    for text in [
        "PT0S",
        "P1Y2M3DT4H5M6.5S",
        "P14D",
        "PT-1H-30M",
        "P-1M",
        "PT0.000000001S",
        "PT-0.5S",
        "PT123456789.123456789S",
    ] {
        let value = duration(text);
        assert_eq!(duration(&value.to_string()), value, "{text}");
    }
    assert_eq!(duration("P1Y2M3DT4H5M6.5S").to_string(), "P1Y2M3DT4H5M6.5S");
    assert_eq!(duration("P2W").to_string(), "P14D");
}

#[test]
fn alternative_iso_date_and_time_forms_are_accepted() {
    assert_eq!(date("20261030"), date("2026-10-30"));
    assert_eq!(date("2026-10"), date("2026-10-01"));
    assert_eq!(date("2026"), date("2026-01-01"));
    assert_eq!(date("2026-W44-5"), date("2026-10-30"));
    assert_eq!(date("2026W445"), date("2026-10-30"));
    assert_eq!(date("2026-303"), date("2026-10-30"));
    assert_eq!(
        parse_local_time("1230").unwrap(),
        parse_local_time("12:30:00").unwrap()
    );
    assert_eq!(
        parse_datetime("2026-10-30T12:00+0200").unwrap(),
        parse_datetime("2026-10-30T12:00:00+02:00").unwrap()
    );
    for invalid in ["2026-13-01", "2026-02-30", "26-01-01", "2026-1-1", "x"] {
        assert!(parse_date(invalid).is_err(), "{invalid}");
    }
    for invalid in ["24:00", "12:60", "12:00:60", "12:00:00.1234567890"] {
        assert!(parse_local_time(invalid).is_err(), "{invalid}");
    }
    let named = parse_datetime("2026-10-30T12:00[Europe/Stockholm]").unwrap_err();
    assert!(named.message().contains("named time zones"));
    assert!(parse_duration("P").is_err());
    assert!(parse_duration("PT").is_err());
    assert!(parse_duration("P1H").is_err());
    assert!(parse_duration("PT1D").is_err());
    assert!(parse_duration("P1D2Y").is_err());
}

#[test]
fn constructors_accept_strings_maps_conversions_and_the_clock() {
    assert_eq!(call(DATE, &[]).unwrap(), date("2026-10-30"));
    assert_eq!(
        call(DATETIME, &[]).unwrap(),
        datetime("2026-10-30T12:00:00Z")
    );
    assert_eq!(
        call(LOCAL_TIME, &[]).unwrap(),
        Value::LocalTime(parse_local_time("12:00").unwrap())
    );
    assert_eq!(
        call(
            DATE,
            &[map(&[("timezone", Value::String("+13:00".into()))])]
        )
        .unwrap(),
        date("2026-10-31")
    );
    assert_eq!(
        call(
            DATE,
            &[map(&[
                ("year", Value::Int(2026)),
                ("month", Value::Int(10)),
                ("day", Value::Int(30)),
            ])]
        )
        .unwrap(),
        date("2026-10-30")
    );
    assert_eq!(
        call(
            DATE,
            &[map(&[
                ("year", Value::Int(2026)),
                ("week", Value::Int(44)),
                ("dayOfWeek", Value::Int(5)),
            ])]
        )
        .unwrap(),
        date("2026-10-30")
    );
    assert_eq!(
        call(
            DATE,
            &[map(&[
                ("year", Value::Int(2026)),
                ("quarter", Value::Int(4)),
                ("dayOfQuarter", Value::Int(30)),
            ])]
        )
        .unwrap(),
        date("2026-10-30")
    );
    assert_eq!(
        call(
            DATETIME,
            &[map(&[
                ("year", Value::Int(2026)),
                ("month", Value::Int(10)),
                ("day", Value::Int(30)),
                ("hour", Value::Int(12)),
                ("timezone", Value::String("+02:00".into())),
            ])]
        )
        .unwrap(),
        datetime("2026-10-30T12:00:00+02:00")
    );
    assert_eq!(
        call(
            DATETIME,
            &[map(&[("epochMillis", Value::Int(1_793_361_600_123))])]
        )
        .unwrap(),
        datetime("2026-10-30T12:00:00.123Z")
    );
    assert_eq!(
        call(
            DATETIME,
            &[map(&[
                ("epochSeconds", Value::Int(1_793_361_600)),
                ("nanosecond", Value::Int(7)),
            ])]
        )
        .unwrap(),
        datetime("2026-10-30T12:00:00.000000007Z")
    );
    assert_eq!(
        call(DATE, &[datetime("2026-10-30T23:30:00-05:00")]).unwrap(),
        date("2026-10-30")
    );
    assert_eq!(
        call(LOCAL_DATETIME, &[date("2026-10-30")]).unwrap(),
        local_datetime("2026-10-30T00:00:00")
    );
    assert_eq!(
        call(DATETIME, &[local_datetime("2026-10-30T01:02:03")]).unwrap(),
        datetime("2026-10-30T01:02:03Z")
    );
    assert_eq!(
        call(
            LOCAL_DATETIME,
            &[map(&[
                ("date", date("2026-10-30")),
                ("hour", Value::Int(7)),
                ("day", Value::Int(1)),
            ])]
        )
        .unwrap(),
        local_datetime("2026-10-01T07:00:00")
    );
    assert_eq!(call(DATE, &[Value::Null]).unwrap(), Value::Null);
    for invalid in [
        map(&[("month", Value::Int(1))]),
        map(&[("year", Value::Int(2026)), ("day", Value::Int(3))]),
        map(&[("year", Value::Int(2026)), ("hour", Value::Int(3))]),
        map(&[("year", Value::String("2026".into()))]),
        map(&[
            ("year", Value::Int(2026)),
            ("month", Value::Int(2)),
            ("day", Value::Int(30)),
        ]),
        Value::Int(7),
        Value::LocalTime(parse_local_time("12:00").unwrap()),
    ] {
        assert!(
            call(DATE, std::slice::from_ref(&invalid)).is_err(),
            "{invalid:?}"
        );
    }
    assert!(call(DATE, &[Value::Null, Value::Null]).is_err());
    assert_eq!(
        call(TemporalFunction::DateTimeFromEpochMillis, &[Value::Int(-1)]).unwrap(),
        datetime("1969-12-31T23:59:59.999Z")
    );
    assert_eq!(
        call(
            TemporalFunction::DateTimeFromEpoch,
            &[Value::Int(1), Value::Int(5)]
        )
        .unwrap(),
        datetime("1970-01-01T00:00:01.000000005Z")
    );
}

#[test]
fn duration_maps_cascade_fractions_like_opencypher() {
    let from_map = |entries: &[(&str, Value)]| call(TemporalFunction::Duration, &[map(entries)]);
    assert_eq!(
        from_map(&[("days", Value::Int(3)), ("hours", Value::Int(2))]).unwrap(),
        duration("P3DT2H")
    );
    assert_eq!(
        from_map(&[("months", Value::Float(0.75))]).unwrap(),
        duration("P22DT19H51M49.5S")
    );
    assert_eq!(
        from_map(&[("days", Value::Float(1.5))]).unwrap(),
        duration("P1DT12H")
    );
    assert_eq!(
        from_map(&[("weeks", Value::Int(1)), ("minutes", Value::Int(-90))]).unwrap(),
        duration("P7DT-1H-30M")
    );
    assert_eq!(
        from_map(&[("hours", Value::Int(25))]).unwrap(),
        duration("PT25H"),
        "time does not carry into days"
    );
    assert!(from_map(&[("fortnights", Value::Int(1))]).is_err());
    assert!(from_map(&[("days", Value::String("1".into()))]).is_err());
}

#[test]
fn components_follow_opencypher_names() {
    let value = datetime("2026-10-30T12:34:56.789012345+02:00");
    let read = |key| component(&value, key).unwrap();
    assert_eq!(read("year"), Value::Int(2026));
    assert_eq!(read("quarter"), Value::Int(4));
    assert_eq!(read("month"), Value::Int(10));
    assert_eq!(read("week"), Value::Int(44));
    assert_eq!(read("weekYear"), Value::Int(2026));
    assert_eq!(read("day"), Value::Int(30));
    assert_eq!(read("ordinalDay"), Value::Int(303));
    assert_eq!(read("dayOfYear"), Value::Int(303));
    assert_eq!(read("dayOfQuarter"), Value::Int(30));
    assert_eq!(read("dayOfWeek"), Value::Int(5));
    assert_eq!(read("hour"), Value::Int(12));
    assert_eq!(read("minute"), Value::Int(34));
    assert_eq!(read("second"), Value::Int(56));
    assert_eq!(read("millisecond"), Value::Int(789));
    assert_eq!(read("microsecond"), Value::Int(789_012));
    assert_eq!(read("nanosecond"), Value::Int(789_012_345));
    assert_eq!(read("offset"), Value::String("+02:00".into()));
    assert_eq!(read("offsetMinutes"), Value::Int(120));
    assert_eq!(read("epochSeconds"), Value::Int(1_793_356_496));
    assert_eq!(read("epochMillis"), Value::Int(1_793_356_496_789));
    assert!(component(&local_datetime("2026-10-30T00:00:00"), "epochMillis").is_err());
    assert!(component(&date("2026-10-30"), "hour").is_err());
    assert!(component(&Value::Int(3), "year").is_err());

    let value = duration("P1Y5M10DT25H61M1.5S");
    let read = |key| component(&value, key).unwrap();
    assert_eq!(read("years"), Value::Int(1));
    assert_eq!(read("months"), Value::Int(17));
    assert_eq!(read("monthsOfYear"), Value::Int(5));
    assert_eq!(read("quarters"), Value::Int(5));
    assert_eq!(read("days"), Value::Int(10));
    assert_eq!(read("weeks"), Value::Int(1));
    assert_eq!(read("hours"), Value::Int(26));
    assert_eq!(read("minutes"), Value::Int(1_561));
    assert_eq!(read("minutesOfHour"), Value::Int(1));
    assert_eq!(read("seconds"), Value::Int(93_661));
    assert_eq!(read("milliseconds"), Value::Int(93_661_500));
    assert_eq!(read("nanosecondsOfSecond"), Value::Int(500_000_000));
    let negative = duration("PT-0.5S");
    assert_eq!(component(&negative, "seconds").unwrap(), Value::Int(-1));
    assert_eq!(
        component(&negative, "milliseconds").unwrap(),
        Value::Int(-500)
    );
}

#[test]
fn arithmetic_applies_months_days_then_time() {
    let add = |left: &Value, right: &Value| {
        arithmetic(left, TemporalArithmeticOp::Add, right)
            .unwrap()
            .unwrap()
    };
    let subtract = |left: &Value, right: &Value| {
        arithmetic(left, TemporalArithmeticOp::Subtract, right)
            .unwrap()
            .unwrap()
    };
    assert_eq!(
        add(&date("2026-01-31"), &duration("P1M")),
        date("2026-02-28")
    );
    assert_eq!(
        add(&date("2026-01-01"), &duration("PT47H")),
        date("2026-01-02")
    );
    assert_eq!(
        add(&duration("P1D"), &date("2026-01-01")),
        date("2026-01-02")
    );
    assert_eq!(
        add(
            &datetime("2026-10-30T23:00:00+02:00"),
            &duration("P1M1DT2H")
        ),
        datetime("2026-12-02T01:00:00+02:00")
    );
    assert_eq!(
        subtract(&local_datetime("2026-03-31T00:00:00"), &duration("P1M")),
        local_datetime("2026-02-28T00:00:00")
    );
    assert_eq!(
        add(
            &Value::LocalTime(parse_local_time("23:00").unwrap()),
            &duration("P1DT2H")
        ),
        Value::LocalTime(parse_local_time("01:00").unwrap())
    );
    assert_eq!(
        add(&duration("P1M"), &duration("P1DT1S")),
        duration("P1M1DT1S")
    );
    assert_eq!(
        subtract(&duration("P1M"), &duration("P1DT1S")),
        duration("P1M-1DT-1S")
    );
    let multiply = |left: &Value, right: &Value| {
        arithmetic(left, TemporalArithmeticOp::Multiply, right)
            .unwrap()
            .unwrap()
    };
    assert_eq!(
        multiply(&duration("P1M2DT3S"), &Value::Int(2)),
        duration("P2M4DT6S")
    );
    assert_eq!(
        multiply(&Value::Float(0.5), &duration("P1D")),
        duration("PT12H")
    );
    assert_eq!(
        arithmetic(
            &duration("P1D"),
            TemporalArithmeticOp::Divide,
            &Value::Int(2)
        )
        .unwrap()
        .unwrap(),
        duration("PT12H")
    );
    // Integer divisors that divide every component are exact beyond 2^53 ns,
    // where the f64 path would round to a multiple of 4 ns.
    let divide = |left: &Value, divisor: i64| {
        arithmetic(left, TemporalArithmeticOp::Divide, &Value::Int(divisor))
            .unwrap()
            .unwrap()
    };
    assert_eq!(divide(&duration("PT10000H"), 2), duration("PT5000H"));
    assert_eq!(
        divide(&duration("PT10000H0.000000002S"), 2),
        duration("PT5000H0.000000001S")
    );
    assert_eq!(
        divide(&duration("P2M4DT-10000H0.000000003S"), -1),
        duration("P-2M-4DT10000H-0.000000003S")
    );
    assert_eq!(
        multiply(&duration("PT5000H0.000000001S"), &Value::Int(2)),
        duration("PT10000H0.000000002S")
    );
    assert!(arithmetic(&Value::Int(1), TemporalArithmeticOp::Add, &Value::Int(2)).is_none());
    for (left, op, right) in [
        (
            date("2026-01-01"),
            TemporalArithmeticOp::Add,
            date("2026-01-01"),
        ),
        (
            date("2026-01-01"),
            TemporalArithmeticOp::Subtract,
            date("2026-01-01"),
        ),
        (duration("P1D"), TemporalArithmeticOp::Add, Value::Int(1)),
        (duration("P1D"), TemporalArithmeticOp::Divide, Value::Int(0)),
        (
            Value::Int(1),
            TemporalArithmeticOp::Subtract,
            duration("P1D"),
        ),
    ] {
        assert!(arithmetic(&left, op, &right).unwrap().is_err());
    }
}

#[test]
fn between_matches_opencypher_examples() {
    let between = |function, left: Value, right: Value| call(function, &[left, right]).unwrap();
    assert_eq!(
        between(
            TemporalFunction::DurationBetween,
            date("1984-10-11"),
            date("2015-06-24")
        ),
        duration("P30Y8M13D")
    );
    assert_eq!(
        between(
            TemporalFunction::DurationBetween,
            local_datetime("2015-07-21T21:40:32.142"),
            local_datetime("2016-07-21T21:45:22.142")
        ),
        duration("P1YT4M50S")
    );
    assert_eq!(
        between(
            TemporalFunction::DurationBetween,
            local_datetime("2015-01-02T11:00:00"),
            local_datetime("2015-01-01T12:00:00")
        ),
        duration("PT-23H")
    );
    assert_eq!(
        between(
            TemporalFunction::DurationBetween,
            datetime("2026-10-30T12:00:00+02:00"),
            datetime("2026-10-30T12:00:00Z")
        ),
        duration("PT2H")
    );
    assert_eq!(
        between(
            TemporalFunction::DurationInMonths,
            date("1984-10-11"),
            date("2015-06-24")
        ),
        duration("P30Y8M")
    );
    assert_eq!(
        between(
            TemporalFunction::DurationInDays,
            date("2026-01-01"),
            local_datetime("2026-01-03T23:00:00")
        ),
        duration("P2D")
    );
    assert_eq!(
        between(
            TemporalFunction::DurationInSeconds,
            date("2026-01-01"),
            date("2026-01-02")
        ),
        duration("PT24H")
    );
    assert_eq!(
        between(
            TemporalFunction::DurationBetween,
            Value::LocalTime(parse_local_time("12:00").unwrap()),
            local_datetime("2026-01-03T13:30:00")
        ),
        duration("PT1H30M")
    );
    let minutes = component(
        &between(
            TemporalFunction::DurationBetween,
            datetime("2026-10-30T12:00:00Z"),
            datetime("2026-10-30T12:42:59Z"),
        ),
        "minutes",
    )
    .unwrap();
    assert_eq!(minutes, Value::Int(42));
    assert!(call(
        TemporalFunction::DurationBetween,
        &[duration("P1D"), date("2026-01-01")]
    )
    .is_err());
}

#[test]
fn truncation_covers_date_and_time_units() {
    let value = datetime("2026-10-30T12:34:56.789+02:00");
    let truncate = |kind, unit: &str, value: &Value| {
        call(
            TemporalFunction::Truncate(kind),
            &[Value::String(unit.into()), value.clone()],
        )
    };
    assert_eq!(
        truncate(TemporalKind::Date, "month", &value).unwrap(),
        date("2026-10-01")
    );
    assert_eq!(
        truncate(TemporalKind::Date, "week", &value).unwrap(),
        date("2026-10-26")
    );
    assert_eq!(
        truncate(TemporalKind::Date, "quarter", &value).unwrap(),
        date("2026-10-01")
    );
    assert_eq!(
        truncate(TemporalKind::Date, "millennium", &value).unwrap(),
        date("2000-01-01")
    );
    assert_eq!(
        truncate(TemporalKind::Date, "weekYear", &date("2027-01-01")).unwrap(),
        date("2025-12-29"),
        "2027-01-01 belongs to ISO week-year 2026"
    );
    assert_eq!(
        truncate(TemporalKind::DateTime, "day", &value).unwrap(),
        datetime("2026-10-30T00:00:00+02:00")
    );
    assert_eq!(
        truncate(TemporalKind::DateTime, "millisecond", &value).unwrap(),
        value
    );
    assert_eq!(
        truncate(TemporalKind::LocalDateTime, "hour", &value).unwrap(),
        local_datetime("2026-10-30T12:00:00")
    );
    assert_eq!(
        truncate(TemporalKind::LocalTime, "minute", &value).unwrap(),
        Value::LocalTime(parse_local_time("12:34").unwrap())
    );
    assert!(truncate(TemporalKind::Date, "hour", &value).is_err());
    assert!(truncate(TemporalKind::Date, "fortnight", &value).is_err());
    assert!(truncate(
        TemporalKind::Date,
        "day",
        &Value::LocalTime(parse_local_time("12:00").unwrap())
    )
    .is_err());
}

#[test]
fn comparable_ordering_is_same_kind_and_matches_total_order() {
    let values = [
        date("2026-01-01"),
        date("2026-01-02"),
        datetime("2026-01-01T12:00:00+02:00"),
        datetime("2026-01-01T10:00:00Z"),
        datetime("2026-01-01T11:00:00Z"),
        local_datetime("2026-01-01T00:00:00"),
        Value::LocalTime(parse_local_time("10:00").unwrap()),
        duration("P1D"),
        Value::Int(1),
    ];
    for left in &values {
        for right in &values {
            match comparable_ordering(left, right) {
                Some(ordering) => assert_eq!(ordering, left.cmp(right), "{left} {right}"),
                None => assert!(
                    std::mem::discriminant(left) != std::mem::discriminant(right)
                        || matches!(left, Value::Duration(_))
                        || matches!(left, Value::Int(_))
                ),
            }
        }
    }
    // Same instant, different offset: ordered by offset, never equal.
    let east = datetime("2026-01-01T12:00:00+02:00");
    let utc = datetime("2026-01-01T10:00:00Z");
    assert_eq!(comparable_ordering(&utc, &east), Some(Ordering::Less));
    assert_ne!(east, utc);
    assert_eq!(
        comparable_ordering(&duration("P1D"), &duration("P2D")),
        None
    );
}

#[test]
fn statement_clock_is_pinned_and_restored() {
    let pinned = DateTime::from_timestamp(0, 0).unwrap();
    with_statement_clock(pinned, || {
        assert_eq!(statement_now(), pinned);
        with_stable_statement_clock(|| assert_eq!(statement_now(), pinned));
    });
    with_stable_statement_clock(|| {
        let first = statement_now();
        assert_eq!(statement_now(), first);
    });
    assert_ne!(statement_now(), pinned);
    assert_eq!(pinned_statement_clock(), None);
    with_statement_clock(pinned, || {
        let carried = pinned_statement_clock();
        assert_eq!(carried, Some(pinned));
        // A worker thread re-pins the carried instant.
        let observed =
            std::thread::spawn(move || with_optional_statement_clock(carried, statement_now))
                .join()
                .unwrap();
        assert_eq!(observed, pinned);
    });
}

#[test]
fn function_names_resolve_case_insensitively() {
    for function in TemporalFunction::ALL {
        let name = function.function_name().to_uppercase();
        let (namespace, member) = match name.split_once('.') {
            Some((namespace, member)) => (namespace, Some(member)),
            None => (name.as_str(), None),
        };
        assert!(TemporalFunction::is_namespace(namespace));
        assert_eq!(
            TemporalFunction::from_name(namespace, member),
            Some(function)
        );
    }
    assert_eq!(TemporalFunction::from_name("duration", Some("since")), None);
    assert!(!TemporalFunction::is_namespace("time"));
    assert!(DATE.reads_clock(0));
    assert!(!DATE.reads_clock(1));
    assert!(!TemporalFunction::Duration.reads_clock(0));
}
