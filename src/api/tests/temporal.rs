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

//! openCypher temporal values through the embedded `Database::query` API:
//! constructors, components, arithmetic, comparison and ordering, native
//! persistence (WAL replay, checkpoint, out-of-core reopen), range indexes,
//! the statement clock, and the LDBC SNB Interactive query shapes.

use super::*;
use hawdb_core::temporal;

fn date(text: &str) -> Value {
    Value::Date(temporal::parse_date(text).unwrap())
}

fn local_time(text: &str) -> Value {
    Value::LocalTime(temporal::parse_local_time(text).unwrap())
}

fn local_datetime(text: &str) -> Value {
    Value::LocalDateTime(temporal::parse_local_datetime(text).unwrap())
}

fn datetime(text: &str) -> Value {
    Value::DateTime(temporal::parse_datetime(text).unwrap())
}

fn duration(text: &str) -> Value {
    Value::duration(temporal::parse_duration(text).unwrap())
}

fn params(entries: &[(&str, Value)]) -> BTreeMap<String, Value> {
    entries
        .iter()
        .map(|(name, value)| ((*name).to_string(), value.clone()))
        .collect()
}

fn single_row(db: &mut Database, query: &str) -> crate::QueryRow {
    single_row_with(db, query, &BTreeMap::new())
}

/// A database with one `:One` node that hosts standalone projections: HawDB
/// read statements start with MATCH.
fn database() -> Database {
    let mut db = Database::new();
    db.query("CREATE (:One {id: 0})").unwrap();
    db
}

fn single_row_with(
    db: &mut Database,
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> crate::QueryRow {
    let query = if query.starts_with("RETURN") || query.starts_with("WITH") {
        format!("MATCH (_one:One) {query}")
    } else {
        query.to_string()
    };
    let query = query.as_str();
    let output = db.query_with_params(query, parameters).unwrap();
    assert_eq!(output.rows.len(), 1, "{query}");
    output.rows[0].clone()
}

fn column(output: &QueryOutput, name: &str) -> Vec<Value> {
    output.rows.iter().map(|row| row[name].clone()).collect()
}

#[test]
fn constructors_parse_strings_maps_and_conversions() {
    let mut db = database();
    let row = single_row(
        &mut db,
        "RETURN date('2026-10-30') AS d, localtime('12:30:15.5') AS t, \
         localdatetime('2026-10-30T12:30') AS ldt, \
         datetime('2026-10-30T12:00:00+02:00') AS dt, \
         duration('P1Y2M3DT4H') AS dur, \
         date({year: 2026, month: 10, day: 30}) AS from_map, \
         datetime({year: 2026, month: 10, day: 30, hour: 12, timezone: '+02:00'}) AS zoned, \
         datetime({epochMillis: 1793361600123}) AS from_millis, \
         datetime({epochSeconds: 1793361600}) AS from_seconds, \
         date(datetime('2026-10-30T23:30:00-05:00')) AS converted, \
         localdatetime(date('2026-10-30')) AS midnight, \
         duration({days: 3, hours: 2}) AS from_duration_map, \
         datetime.fromepochmillis(0) AS epoch",
    );
    assert_eq!(row["d"], date("2026-10-30"));
    assert_eq!(row["t"], local_time("12:30:15.5"));
    assert_eq!(row["ldt"], local_datetime("2026-10-30T12:30:00"));
    assert_eq!(row["dt"], datetime("2026-10-30T12:00:00+02:00"));
    assert_eq!(row["dur"], duration("P1Y2M3DT4H"));
    assert_eq!(row["from_map"], date("2026-10-30"));
    assert_eq!(row["zoned"], datetime("2026-10-30T12:00:00+02:00"));
    assert_eq!(row["from_millis"], datetime("2026-10-30T12:00:00.123Z"));
    assert_eq!(row["from_seconds"], datetime("2026-10-30T12:00:00Z"));
    assert_eq!(row["converted"], date("2026-10-30"));
    assert_eq!(row["midnight"], local_datetime("2026-10-30T00:00:00"));
    assert_eq!(row["from_duration_map"], duration("P3DT2H"));
    assert_eq!(row["epoch"], datetime("1970-01-01T00:00:00Z"));
    assert_eq!(row["dt"].to_string(), "2026-10-30T12:00:00+02:00");
    assert_eq!(row["dur"].to_string(), "P1Y2M3DT4H");

    let nulls = single_row(
        &mut db,
        "RETURN date(null) AS d, duration.between(null, date()) AS b, \
         date('2026-01-01') + null AS sum",
    );
    for name in ["d", "b", "sum"] {
        assert_eq!(nulls[name], Value::Null, "{name}");
    }
    for invalid in [
        "MATCH (x:One) RETURN date('2026-13-01') AS d",
        "MATCH (x:One) RETURN date({month: 1}) AS d",
        "MATCH (x:One) RETURN datetime('2026-10-30T12:00[Europe/Stockholm]') AS d",
        "MATCH (x:One) RETURN duration('P1H') AS d",
        "MATCH (x:One) RETURN date('2026-01-01') + date('2026-01-01') AS d",
        "MATCH (x:One) RETURN date('2026-01-01').hour AS h",
        "MATCH (x:One) RETURN date.truncate('hour', date('2026-01-01')) AS d",
        "CREATE (:Bad {on: date('2026-02-30')})",
    ] {
        assert!(db.query(invalid).is_err(), "{invalid}");
    }
    assert!(matches!(
        db.query("MATCH (x:One) RETURN date(1, 2) AS d"),
        Err(HawDBError::Parse(_))
    ));
    assert!(matches!(
        db.query("MATCH (x:One) RETURN duration.since(date(), date()) AS d"),
        Err(HawDBError::Parse(_))
    ));
}

#[test]
fn components_arithmetic_between_and_truncation() {
    let mut db = database();
    let row = single_row(
        &mut db,
        "WITH datetime('2026-10-30T12:34:56.789+02:00') AS dt, \
              duration('P1Y5M10DT25H61M1.5S') AS d \
         RETURN dt.year AS year, dt.month AS month, dt.day AS day, dt.hour AS hour, \
                dt.minute AS minute, dt.second AS second, dt.millisecond AS ms, \
                dt.epochMillis AS epoch_millis, dt.epochSeconds AS epoch_seconds, \
                dt.dayOfWeek AS dow, dt.dayOfYear AS doy, dt.ordinalDay AS ordinal, \
                dt.offset AS offset, \
                d.years AS years, d.months AS months, d.days AS days, d.hours AS hours, \
                d.minutes AS minutes, d.seconds AS seconds, d.milliseconds AS millis",
    );
    let expected = [
        ("year", Value::Int(2026)),
        ("month", Value::Int(10)),
        ("day", Value::Int(30)),
        ("hour", Value::Int(12)),
        ("minute", Value::Int(34)),
        ("second", Value::Int(56)),
        ("ms", Value::Int(789)),
        ("epoch_millis", Value::Int(1_793_356_496_789)),
        ("epoch_seconds", Value::Int(1_793_356_496)),
        ("dow", Value::Int(5)),
        ("doy", Value::Int(303)),
        ("ordinal", Value::Int(303)),
        ("offset", Value::String("+02:00".into())),
        ("years", Value::Int(1)),
        ("months", Value::Int(17)),
        ("days", Value::Int(10)),
        ("hours", Value::Int(26)),
        ("minutes", Value::Int(1_561)),
        ("seconds", Value::Int(93_661)),
        ("millis", Value::Int(93_661_500)),
    ];
    for (name, value) in expected {
        assert_eq!(row[name], value, "{name}");
    }

    let row = single_row(
        &mut db,
        "RETURN date('2026-01-31') + duration('P1M') AS clamped, \
                datetime('2026-10-30T23:00:00+02:00') + duration({days: 1, hours: 2}) AS later, \
                localdatetime('2026-03-31T00:00') - duration('P1M') AS earlier, \
                duration('P1D') + duration('PT1H') AS sum, \
                duration('P1M1D') * 2 AS doubled, duration('P1D') / 2 AS halved, \
                duration.between(date('1984-10-11'), date('2015-06-24')) AS between, \
                duration.inMonths(date('1984-10-11'), date('2015-06-24')) AS in_months, \
                duration.inDays(date('2026-01-01'), date('2026-03-01')) AS in_days, \
                duration.inSeconds(datetime('2026-01-01T00:00Z'), \
                                   datetime('2026-01-01T01:00+00:30')) AS in_seconds, \
                date.truncate('month', date('2026-10-30')) AS month_start, \
                datetime.truncate('day', datetime('2026-10-30T12:34+02:00')) AS day_start, \
                localdatetime.truncate('hour', localdatetime('2026-10-30T12:34')) AS hour_start, \
                localtime.truncate('minute', localtime('12:34:56')) AS minute_start",
    );
    let expected = [
        ("clamped", date("2026-02-28")),
        ("later", datetime("2026-11-01T01:00:00+02:00")),
        ("earlier", local_datetime("2026-02-28T00:00:00")),
        ("sum", duration("P1DT1H")),
        ("doubled", duration("P2M2D")),
        ("halved", duration("PT12H")),
        ("between", duration("P30Y8M13D")),
        ("in_months", duration("P30Y8M")),
        ("in_days", duration("P59D")),
        ("in_seconds", duration("PT30M")),
        ("month_start", date("2026-10-01")),
        ("day_start", datetime("2026-10-30T00:00:00+02:00")),
        ("hour_start", local_datetime("2026-10-30T12:00:00")),
        ("minute_start", local_time("12:34:00")),
    ];
    for (name, value) in expected {
        assert_eq!(row[name], value, "{name}");
    }
}

#[test]
fn comparisons_ordering_and_aggregates_over_temporal_properties() {
    let mut db = database();
    for (id, birthday) in [(1, "1990-05-01"), (2, "1985-12-24"), (3, "2001-01-31")] {
        db.query_with_params(
            "CREATE (:Person {id: $id, birthday: $birthday, joined: datetime($joined)})",
            &params(&[
                ("id", Value::Int(id)),
                ("birthday", date(birthday)),
                (
                    "joined",
                    Value::String(format!("2020-0{id}-01T00:00:00+0{id}:00")),
                ),
            ]),
        )
        .unwrap();
    }
    db.query("CREATE (:Person {id: 4})").unwrap();

    let ordered = db
        .query("MATCH (p:Person) WHERE p.birthday IS NOT NULL RETURN p.id AS id ORDER BY p.birthday DESC")
        .unwrap();
    assert_eq!(
        column(&ordered, "id"),
        vec![Value::Int(3), Value::Int(1), Value::Int(2)]
    );
    let by_month = db
        .query("MATCH (p:Person) WHERE p.birthday IS NOT NULL RETURN p.id AS id ORDER BY p.birthday.month, p.id")
        .unwrap();
    assert_eq!(
        column(&by_month, "id"),
        vec![Value::Int(3), Value::Int(1), Value::Int(2)]
    );
    let row = single_row(
        &mut db,
        "MATCH (p:Person) RETURN min(p.birthday) AS first, max(p.birthday) AS last, \
         count(p.birthday) AS present, collect(p.birthday.year) AS years",
    );
    assert_eq!(row["first"], date("1985-12-24"));
    assert_eq!(row["last"], date("2001-01-31"));
    assert_eq!(row["present"], Value::Int(3));
    let Value::List(mut years) = row["years"].clone() else {
        panic!("expected list");
    };
    years.sort();
    assert_eq!(
        years,
        vec![Value::Int(1985), Value::Int(1990), Value::Int(2001)]
    );

    let cutoff = params(&[("cutoff", date("1990-01-01"))]);
    let after = db
        .query_with_params(
            "MATCH (p:Person) WHERE p.birthday >= $cutoff RETURN p.id AS id ORDER BY id",
            &cutoff,
        )
        .unwrap();
    assert_eq!(column(&after, "id"), vec![Value::Int(1), Value::Int(3)]);
    let before = db
        .query("MATCH (p:Person) WHERE p.birthday < date('1990-01-01') RETURN p.id AS id")
        .unwrap();
    assert_eq!(column(&before, "id"), vec![Value::Int(2)]);
    let equal = db
        .query("MATCH (p:Person) WHERE p.birthday = date({year: 2001, month: 1, day: 31}) RETURN p.id AS id")
        .unwrap();
    assert_eq!(column(&equal, "id"), vec![Value::Int(3)]);
    // Different temporal kinds and strings never compare.
    for query in [
        "MATCH (p:Person) WHERE p.birthday < datetime('2100-01-01T00:00Z') RETURN p.id AS id",
        "MATCH (p:Person) WHERE p.birthday < '2100-01-01' RETURN p.id AS id",
        "MATCH (p:Person) WHERE p.birthday = localdatetime('1990-05-01T00:00') RETURN p.id AS id",
    ] {
        assert!(db.query(query).unwrap().rows.is_empty(), "{query}");
    }
    // Zoned datetimes compare by instant: 2020-02-01T00:00+02:00 is earlier
    // than 2020-01-31T23:00Z.
    let instants = db
        .query(
            "MATCH (p:Person) WHERE p.joined < datetime('2020-01-31T23:00:00Z') \
             RETURN p.id AS id ORDER BY id",
        )
        .unwrap();
    assert_eq!(column(&instants, "id"), vec![Value::Int(1), Value::Int(2)]);
    // Durations are equal component-wise but not ordered by `<`.
    let truth = |condition: &str| {
        format!(
            "CASE WHEN ({condition}) IS NULL THEN 'null' \
                  WHEN {condition} THEN 'true' ELSE 'false' END"
        )
    };
    let row = single_row(
        &mut db,
        &format!(
            "RETURN {} AS day_is_not_24h, {} AS same, {} AS ordered, {} AS dates, \
             {} AS same_instant",
            truth("duration('P1D') = duration({hours: 24})"),
            truth("duration('PT24H') = duration({hours: 24})"),
            truth("duration('P1D') < duration('P2D')"),
            truth("date('2026-01-01') < date('2026-01-02')"),
            truth("datetime('2026-01-01T12:00+02:00') = datetime('2026-01-01T10:00Z')"),
        ),
    );
    assert_eq!(row["day_is_not_24h"], Value::String("false".into()));
    assert_eq!(row["same"], Value::String("true".into()));
    assert_eq!(row["ordered"], Value::String("null".into()));
    assert_eq!(row["dates"], Value::String("true".into()));
    assert_eq!(row["same_instant"], Value::String("false".into()));
}

#[test]
fn temporal_properties_are_written_by_create_set_and_merge() {
    let mut db = database();
    db.query("CREATE (:Event {id: 1, on: date('2026-10-30'), at: datetime('2026-10-30T12:00:00+02:00'), took: duration('PT90M')})")
        .unwrap();
    db.query("MATCH (e:Event {id: 1}) SET e.local = localdatetime('2026-10-30T12:00'), e.clock = localtime('08:15')")
        .unwrap();
    db.query("MERGE (e:Event {id: 2}) ON CREATE SET e.on = date({year: 2027, month: 1, day: 2})")
        .unwrap();
    db.query_with_params(
        "MATCH (e:Event {id: 2}) SET e.at = $at",
        &params(&[("at", datetime("2027-01-02T00:00:00Z"))]),
    )
    .unwrap();
    let row = single_row(
        &mut db,
        "MATCH (e:Event {id: 1}) RETURN e.on AS on, e.at AS at, e.took AS took, \
         e.local AS local, e.clock AS clock, e.took.minutes AS minutes",
    );
    assert_eq!(row["on"], date("2026-10-30"));
    assert_eq!(row["at"], datetime("2026-10-30T12:00:00+02:00"));
    assert_eq!(row["took"], duration("PT1H30M"));
    assert_eq!(row["local"], local_datetime("2026-10-30T12:00:00"));
    assert_eq!(row["clock"], local_time("08:15:00"));
    assert_eq!(row["minutes"], Value::Int(90));
    let row = single_row(
        &mut db,
        "MATCH (e:Event {id: 2}) RETURN e.on AS on, e.at AS at",
    );
    assert_eq!(row["on"], date("2027-01-02"));
    assert_eq!(row["at"], datetime("2027-01-02T00:00:00Z"));
}

fn temporal_fixture(db: &mut Database) {
    db.query(
        "CREATE (:Event {id: 1, on: date('2026-10-30'), at: datetime('2026-10-30T12:00:00+02:00'), \
         local: localdatetime('2026-10-30T12:00'), clock: localtime('08:15:00.000000001'), \
         took: duration('P1M2DT3.5S'), many: [date('2026-01-01'), duration('PT1S')]})",
    )
    .unwrap();
    db.query_with_params(
        "CREATE (:Event {id: 2, on: $on, at: $at, took: $took})",
        &params(&[
            ("on", date("-0044-03-15")),
            ("at", datetime("1969-12-31T23:59:59.999999999-00:30:15")),
            ("took", duration("-P1Y2M3DT4H5M6.000000007S")),
        ]),
    )
    .unwrap();
}

fn assert_temporal_fixture(db: &mut Database) {
    let output = db
        .query(
            "MATCH (e:Event) RETURN e.id AS id, e.on AS on, e.at AS at, e.local AS local, \
             e.clock AS clock, e.took AS took, e.many AS many ORDER BY id",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    let first = &output.rows[0];
    assert_eq!(first["on"], date("2026-10-30"));
    assert_eq!(first["at"], datetime("2026-10-30T12:00:00+02:00"));
    assert_eq!(first["local"], local_datetime("2026-10-30T12:00:00"));
    assert_eq!(first["clock"], local_time("08:15:00.000000001"));
    assert_eq!(first["took"], duration("P1M2DT3.5S"));
    assert_eq!(
        first["many"],
        Value::List(vec![date("2026-01-01"), duration("PT1S")])
    );
    let second = &output.rows[1];
    assert_eq!(second["on"], date("-0044-03-15"));
    assert_eq!(
        second["at"],
        datetime("1969-12-31T23:59:59.999999999-00:30:15")
    );
    assert_eq!(second["took"], duration("-P1Y2M3DT4H5M6.000000007S"));
    let range = db
        .query("MATCH (e:Event) WHERE e.at >= datetime('2000-01-01T00:00Z') RETURN e.id AS id")
        .unwrap();
    assert_eq!(column(&range, "id"), vec![Value::Int(1)]);
}

#[test]
fn temporal_properties_survive_wal_replay() {
    let path = unique_test_dir("temporal_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        temporal_fixture(&mut db);
    }
    {
        let mut db = Database::open(&path).unwrap();
        assert_temporal_fixture(&mut db);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn temporal_properties_survive_checkpoint_in_both_residency_modes() {
    let path = unique_test_dir("temporal_checkpoint_reopen");
    {
        let mut db = Database::open(&path).unwrap();
        temporal_fixture(&mut db);
        db.checkpoint().unwrap();
        assert_temporal_fixture(&mut db);
    }
    for mode in [
        hawdb_storage::config::StorageResidencyMode::Materialized,
        hawdb_storage::config::StorageResidencyMode::OutOfCore,
    ] {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: mode,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        if mode == hawdb_storage::config::StorageResidencyMode::OutOfCore {
            assert!(db.storage_residency_report().unwrap().out_of_core);
        }
        assert_temporal_fixture(&mut db);
        // Post-checkpoint deltas over the out-of-core base also round-trip.
        db.query("MATCH (e:Event {id: 1}) SET e.later = datetime('2026-10-31T12:00:00+02:00')")
            .unwrap();
        let row = single_row(&mut db, "MATCH (e:Event {id: 1}) RETURN e.later AS later");
        assert_eq!(row["later"], datetime("2026-10-31T12:00:00+02:00"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

fn ldbc_messages(db: &mut Database) {
    // Older messages make the window selective enough for an index seek.
    for id in 100..160 {
        db.query_with_params(
            "CREATE (:Message {id: $id, creationDate: $created})",
            &params(&[
                ("id", Value::Int(id)),
                (
                    "created",
                    datetime(&format!("2010-01-01T00:{:02}:00Z", id - 100)),
                ),
            ]),
        )
        .unwrap();
    }
    for (id, created) in [
        (1, "2012-07-31T23:59:59.999Z"),
        (2, "2012-08-01T00:00:00Z"),
        (3, "2012-08-15T08:30:00+02:00"),
        (4, "2012-08-31T23:59:59Z"),
        (5, "2012-09-01T00:00:00Z"),
        (6, "2012-09-01T01:59:59+02:00"),
    ] {
        db.query_with_params(
            "CREATE (:Message {id: $id, creationDate: $created, createdMs: $ms})",
            &params(&[
                ("id", Value::Int(id)),
                ("created", datetime(created)),
                (
                    "ms",
                    Value::Int(
                        temporal::parse_datetime(created)
                            .unwrap()
                            .timestamp_millis(),
                    ),
                ),
            ]),
        )
        .unwrap();
    }
}

const IC3_WINDOW: &str = "MATCH (m:Message) \
     WHERE m.creationDate >= datetime($start) \
       AND m.creationDate < datetime($start) + duration({days: $durationDays}) \
     RETURN count(m) AS messages";

#[test]
fn ldbc_ic3_ic4_datetime_window_counts_with_and_without_a_range_index() {
    let mut db = database();
    ldbc_messages(&mut db);
    let window = params(&[
        ("start", Value::String("2012-08-01T00:00:00Z".into())),
        ("durationDays", Value::Int(31)),
    ]);
    let row = single_row_with(&mut db, IC3_WINDOW, &window);
    assert_eq!(row["messages"], Value::Int(4), "ids 2, 3, 4 and 6");

    // The constant window bounds fold to native property comparisons.
    let plan = format!(
        "{:?}",
        db.explain_query_with_params(IC3_WINDOW, &window)
            .unwrap()
            .physical_plan
    );
    assert!(!plan.contains("TemporalFunction"), "{plan}");
    assert!(plan.contains("PropertyCompare"), "{plan}");

    db.query("CREATE RANGE INDEX ON :Message(creationDate)")
        .unwrap();
    let explained = db.explain_query_with_params(IC3_WINDOW, &window).unwrap();
    assert!(
        explained.trace.selected_plan.contains("IndexNodeRangeSeek"),
        "{}",
        explained.trace.selected_plan
    );
    let row = single_row_with(&mut db, IC3_WINDOW, &window);
    assert_eq!(row["messages"], Value::Int(4));
    let narrower = params(&[
        ("start", Value::String("2012-08-15T06:30:00Z".into())),
        ("durationDays", Value::Int(1)),
    ]);
    let row = single_row_with(&mut db, IC3_WINDOW, &narrower);
    assert_eq!(
        row["messages"],
        Value::Int(1),
        "the +02:00 message at 06:30Z"
    );
}

#[test]
fn ldbc_range_index_seek_survives_checkpoint_and_out_of_core_reopen() {
    let path = unique_test_dir("temporal_range_index_reopen");
    let window = params(&[
        ("start", Value::String("2012-08-01T00:00:00Z".into())),
        ("durationDays", Value::Int(31)),
    ]);
    {
        let mut db = Database::open(&path).unwrap();
        ldbc_messages(&mut db);
        db.query("CREATE RANGE INDEX ON :Message(creationDate)")
            .unwrap();
        db.checkpoint().unwrap();
    }
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let row = single_row_with(&mut db, IC3_WINDOW, &window);
    assert_eq!(row["messages"], Value::Int(4));
    db.query_with_params(
        "CREATE (:Message {id: 7, creationDate: $created})",
        &params(&[("created", datetime("2012-08-20T00:00:00Z"))]),
    )
    .unwrap();
    let row = single_row_with(&mut db, IC3_WINDOW, &window);
    assert_eq!(row["messages"], Value::Int(5));
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn ldbc_ic10_birthday_month_and_day_components() {
    let mut db = database();
    for (id, birthday) in [
        (1, "1990-05-21"),
        (2, "1985-06-21"),
        (3, "1999-06-01"),
        (4, "1980-05-20"),
        (5, "1970-12-25"),
        (6, "1971-01-10"),
    ] {
        db.query_with_params(
            "CREATE (:Person {id: $id, birthday: $birthday})",
            &params(&[("id", Value::Int(id)), ("birthday", date(birthday))]),
        )
        .unwrap();
    }
    let query = "MATCH (p:Person) \
         WHERE (p.birthday.month = $month AND p.birthday.day >= 21) \
            OR (p.birthday.month = ($month % 12) + 1 AND p.birthday.day < 22) \
         RETURN p.id AS id ORDER BY id";
    let may = db
        .query_with_params(query, &params(&[("month", Value::Int(5))]))
        .unwrap();
    assert_eq!(
        column(&may, "id"),
        vec![Value::Int(1), Value::Int(2), Value::Int(3)]
    );
    let december = db
        .query_with_params(query, &params(&[("month", Value::Int(12))]))
        .unwrap();
    assert_eq!(column(&december, "id"), vec![Value::Int(5), Value::Int(6)]);
}

#[test]
fn ldbc_ic7_latency_minutes_and_epoch_millis_construction() {
    let mut db = database();
    ldbc_messages(&mut db);
    let row = single_row(
        &mut db,
        "MATCH (a:Message {id: 2}), (b:Message {id: 3}) \
         RETURN duration.between(a.creationDate, b.creationDate).minutes AS minutes, \
                duration.inSeconds(a.creationDate, b.creationDate).minutes AS total_minutes, \
                CASE WHEN datetime({epochMillis: b.createdMs}) = b.creationDate \
                     THEN true ELSE false END AS same_instant, \
                CASE WHEN datetime({epochMillis: b.createdMs}).epochMillis = b.createdMs \
                     THEN true ELSE false END AS round_trip",
    );
    // between() keeps 14 whole days separate from the 6h30m remainder, as in
    // Neo4j; inSeconds() measures the exact elapsed time.
    assert_eq!(row["minutes"], Value::Int(390));
    assert_eq!(row["total_minutes"], Value::Int(14 * 24 * 60 + 390));
    // The epoch constructor yields UTC, the stored value is +02:00.
    assert_eq!(row["same_instant"], Value::Bool(false));
    assert_eq!(row["round_trip"], Value::Bool(true));
    let output = db
        .query(
            "MATCH (m:Message) WHERE datetime({epochMillis: m.createdMs}) < datetime('2012-08-01T00:00Z') \
             RETURN m.id AS id",
        )
        .unwrap();
    assert_eq!(column(&output, "id"), vec![Value::Int(1)]);
}

#[test]
fn clock_functions_share_one_pinned_statement_instant_and_bypass_the_plan_cache() {
    let mut db = database();
    let pinned = temporal::DateTime::from_timestamp(1_793_361_600, 5).unwrap();
    let row = temporal::with_statement_clock(pinned, || {
        single_row(
            &mut db,
            "RETURN date() AS d, localtime() AS t, localdatetime() AS ldt, \
             datetime() AS dt, CASE WHEN true THEN CURRENT_TIMESTAMP() END AS ts, \
             CASE WHEN datetime() = datetime() THEN true ELSE false END AS stable",
        )
    });
    assert_eq!(row["d"], date("2026-10-30"));
    assert_eq!(row["t"], local_time("12:00:00.000000005"));
    assert_eq!(row["ldt"], local_datetime("2026-10-30T12:00:00.000000005"));
    assert_eq!(row["dt"], datetime("2026-10-30T12:00:00.000000005Z"));
    assert_eq!(row["ts"], Value::Int(1_793_361_600_000_000_005));
    assert_eq!(row["stable"], Value::Bool(true));
    // Statically bound clock reads in a mutation share the same instant.
    temporal::with_statement_clock(pinned, || {
        db.query("MATCH (o:One) SET o.ts = CURRENT_TIMESTAMP(), o.at = datetime(), o.on = date()")
            .unwrap()
    });
    let row = single_row(
        &mut db,
        "MATCH (o:One) RETURN o.ts AS ts, o.at AS at, o.on AS on",
    );
    assert_eq!(row["ts"], Value::Int(1_793_361_600_000_000_005));
    assert_eq!(row["at"], datetime("2026-10-30T12:00:00.000000005Z"));
    assert_eq!(row["on"], date("2026-10-30"));
    let later = temporal::DateTime::from_timestamp(1_793_448_000, 0).unwrap();
    let row = temporal::with_statement_clock(later, || {
        single_row(&mut db, "RETURN date() AS d, datetime() AS dt")
    });
    assert_eq!(
        row["d"],
        date("2026-10-31"),
        "a cached plan would keep 10-30"
    );
    // Unpinned, every clock read in one statement still observes one instant.
    let row = single_row(
        &mut db,
        "RETURN CASE WHEN datetime() = datetime() THEN true ELSE false END AS stable, \
                CASE WHEN date(datetime()) = date() THEN true ELSE false END AS same_day",
    );
    assert_eq!(row["stable"], Value::Bool(true));
    assert_eq!(row["same_day"], Value::Bool(true));
}

#[test]
fn temporal_parameters_are_cached_by_shape_and_rebound() {
    let mut db = database();
    db.query("CREATE (:Event {id: 1, on: date('2026-10-30')})")
        .unwrap();
    db.query("CREATE (:Event {id: 2, on: date('2026-11-30')})")
        .unwrap();
    let query = "MATCH (e:Event) WHERE e.on >= $from RETURN e.id AS id ORDER BY id";
    let first = db
        .query_with_params(query, &params(&[("from", date("2026-11-01"))]))
        .unwrap();
    assert_eq!(column(&first, "id"), vec![Value::Int(2)]);
    let second = db
        .query_with_params(query, &params(&[("from", date("2026-01-01"))]))
        .unwrap();
    assert_eq!(column(&second, "id"), vec![Value::Int(1), Value::Int(2)]);
    // Another temporal kind in the same slot is a different plan shape.
    let mixed = db
        .query_with_params(query, &params(&[("from", datetime("2026-01-01T00:00Z"))]))
        .unwrap();
    assert!(mixed.rows.is_empty());
    let row = single_row_with(
        &mut db,
        "RETURN $d.year AS year, $d + duration('P1D') AS next",
        &params(&[("d", date("2026-12-31"))]),
    );
    assert_eq!(row["year"], Value::Int(2026));
    assert_eq!(row["next"], date("2027-01-01"));
}

#[test]
fn temporal_case_order_keys_rebind_hidden_inputs_and_cached_parameters() {
    let mut db = Database::new();
    for statement in [
        "CREATE (:TemporalOrder {id: 1, raw: '2026-11-02', enabled: true})",
        "CREATE (:TemporalOrder {id: 2, raw: '2026-11-01', enabled: true})",
        "CREATE (:TemporalOrder {id: 3, raw: '2026-11-03', enabled: false})",
    ] {
        db.query(statement).unwrap();
    }
    let query = "MATCH (n:TemporalOrder) RETURN n.id AS id \
                 ORDER BY CASE WHEN n.enabled = true \
                   THEN date(n.raw).day + $shift ELSE 0 END DESC, id LIMIT 2";
    for (shift, expected) in [(0, [1, 2]), (-3, [3, 1]), (0, [1, 2]), (-3, [3, 1])] {
        let parameters = params(&[("shift", Value::Int(shift))]);
        let output = db.query_with_params(query, &parameters).unwrap();
        assert_eq!(output.rows.schema().columns(), &["id"]);
        assert_eq!(column(&output, "id"), expected.map(Value::Int));
    }
    for shift in [0, -3] {
        assert_eq!(
            db.explain_query_with_params(query, &params(&[("shift", Value::Int(shift))]))
                .unwrap()
                .plan_cache_lookup,
            PlanCacheLookup::Hit
        );
    }
}

#[test]
fn temporal_values_are_refused_by_relational_sql_parameters() {
    let mut db = database();
    db.query_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, at BIGINT)")
        .unwrap();
    assert!(db
        .query_sql_with_params(
            "INSERT INTO t (id, at) VALUES (1, $1)",
            &[date("2026-10-30")]
        )
        .is_err());
}

fn ids(db: &mut Database, query: &str, parameters: &BTreeMap<String, Value>) -> Vec<Value> {
    let output = db.query_with_params(query, parameters).unwrap();
    let mut ids = column(&output, "id");
    ids.sort();
    ids
}

#[test]
fn duration_ordering_is_null_whether_or_not_the_operand_folds() {
    let mut db = database();
    for (id, span) in [(1, "P1D"), (2, "P3D")] {
        db.query_with_params(
            "CREATE (:Span {id: $id, d: $d, x: 2})",
            &params(&[("id", Value::Int(id)), ("d", duration(span))]),
        )
        .unwrap();
    }
    db.query("CREATE (:Span {id: 3, x: 2})").unwrap();
    let parameters = params(&[("limit", duration("P2D")), ("days", Value::Int(2))]);

    // A duration parameter is a native property comparison; a folded
    // duration literal stays an expression comparison against a literal (the
    // planner keeps unordered kinds out of `PropertyCompare`); and
    // `duration({days: n.x})` stays a per-row temporal call.
    let plan = |db: &mut Database, query: &str| {
        format!(
            "{:?}",
            db.explain_query_with_params(query, &parameters)
                .unwrap()
                .physical_plan
        )
    };
    for operand in ["$limit", "duration({days: $days})", "duration('P2D')"] {
        for negation in ["", "NOT "] {
            let query = format!("MATCH (n:Span) WHERE {negation}n.d < {operand} RETURN n.id AS id");
            let explained = plan(&mut db, &query);
            if operand == "$limit" {
                assert!(
                    explained.contains("PropertyCompare"),
                    "{query}: {explained}"
                );
            }
            assert!(
                !explained.contains("TemporalFunction"),
                "{query}: {explained}"
            );
        }
    }
    let unfolded = "MATCH (n:Span) WHERE NOT n.d < duration({days: n.x}) RETURN n.id AS id";
    assert!(plan(&mut db, unfolded).contains("TemporalFunction"));

    // `d < limit` is NULL for two durations, so neither it nor its negation
    // selects a row, in every operand form (Neo4j: `NOT null` is null).
    for operand in [
        "$limit",
        "duration({days: $days})",
        "duration('P2D')",
        "duration({days: n.x})",
    ] {
        for condition in [
            format!("n.d < {operand}"),
            format!("NOT n.d < {operand}"),
            format!("NOT n.d >= {operand}"),
            format!("NOT (n.d <= {operand} OR n.id = 99)"),
        ] {
            let query = format!("MATCH (n:Span) WHERE {condition} RETURN n.id AS id");
            assert_eq!(
                ids(&mut db, &query, &parameters),
                Vec::<Value>::new(),
                "{query}"
            );
        }
        // Kleene logic: NULL AND false is false, so NOT selects ids 2 and 3.
        let query =
            format!("MATCH (n:Span) WHERE NOT (n.d > {operand} AND n.id = 1) RETURN n.id AS id");
        assert_eq!(
            ids(&mut db, &query, &parameters),
            vec![Value::Int(2), Value::Int(3)],
            "{query}"
        );
        // Equality between durations is defined.
        let query = format!("MATCH (n:Span) WHERE NOT n.d = {operand} RETURN n.id AS id");
        assert_eq!(
            ids(&mut db, &query, &parameters),
            vec![Value::Int(1), Value::Int(2)],
            "{query}"
        );
    }

    // A filtered mutation agrees with the read.
    db.query_with_params(
        "MATCH (n:Span) WHERE NOT n.d < $limit SET n.flagged = true",
        &parameters,
    )
    .unwrap();
    assert_eq!(
        ids(
            &mut db,
            "MATCH (n:Span) WHERE n.flagged = true RETURN n.id AS id",
            &parameters
        ),
        Vec::<Value>::new()
    );
}

#[test]
fn clock_reads_evaluated_per_row_observe_the_planning_instant() {
    let mut db = database();
    let rows = (0..2_000)
        .map(|id| {
            Value::Map(BTreeMap::from([
                ("id".to_string(), Value::Int(id)),
                ("tz".to_string(), Value::String("+01:00".into())),
            ]))
        })
        .collect();
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Tick {id: r.id, tz: r.tz})",
        &params(&[("rows", Value::List(rows))]),
    )
    .unwrap();
    let query = "MATCH (t:Tick) RETURN datetime({timezone: t.tz}) AS row_clock, \
                 datetime({timezone: '+01:00'}) AS map_clock, datetime() AS planned";
    let instants = |output: &QueryOutput| {
        output
            .rows
            .iter()
            .flat_map(|row| {
                [
                    row["row_clock"].clone(),
                    row["map_clock"].clone(),
                    row["planned"].clone(),
                ]
            })
            .map(|value| match value {
                Value::DateTime(value) => value.naive_utc(),
                other => panic!("expected a zoned datetime, got {other:?}"),
            })
            .collect::<BTreeSet<_>>()
    };
    // Unpinned: `datetime()` is bound while planning and the per-row reads
    // run later, yet all 6,000 reads observe one instant.
    let output = db.query(query).unwrap();
    assert_eq!(output.rows.len(), 2_000);
    assert_eq!(instants(&output).len(), 1, "one statement, one instant");
    assert_eq!(
        output.rows[0]["row_clock"], output.rows[0]["map_clock"],
        "both in +01:00"
    );
    // Pinned, and not replayed from the plan cache by the next statement.
    for seconds in [1_793_361_600, 1_793_448_000] {
        let pinned = temporal::DateTime::from_timestamp(seconds, 7).unwrap();
        let output = temporal::with_statement_clock(pinned, || db.query(query).unwrap());
        assert_eq!(
            instants(&output),
            BTreeSet::from([pinned.naive_utc()]),
            "{pinned}"
        );
    }
}

#[test]
fn clock_reading_timezone_maps_are_not_replayed_from_the_plan_cache() {
    let mut db = database();
    db.query("CREATE (:Event {id: 1, at: datetime('2026-10-30T13:00:00Z')})")
        .unwrap();
    let before = temporal::DateTime::from_timestamp(1_793_361_600, 0).unwrap(); // 12:00Z
    let after = temporal::DateTime::from_timestamp(1_793_448_000, 0).unwrap(); // next day
    let tz = params(&[(
        "tz",
        Value::Map(BTreeMap::from([(
            "timezone".to_string(),
            Value::String("+01:00".into()),
        )])),
    )]);
    for query in [
        "MATCH (e:Event) WHERE e.at < datetime($tz) RETURN e.id AS id",
        "MATCH (e:Event) WHERE e.at < datetime({timezone: '+01:00'}) RETURN e.id AS id",
        "MATCH (e:Event) WHERE e.at < datetime({timezone: '+01:00'}) + duration('PT0S') \
         RETURN e.id AS id",
    ] {
        for (instant, expected) in [
            (before, vec![]),
            (after, vec![Value::Int(1)]),
            (before, vec![]),
        ] {
            let found = temporal::with_statement_clock(instant, || ids(&mut db, query, &tz));
            assert_eq!(found, expected, "{query} at {instant}");
        }
    }
    let row = temporal::with_statement_clock(after, || {
        single_row_with(&mut db, "RETURN datetime($tz) AS dt", &tz)
    });
    assert_eq!(row["dt"], datetime("2026-10-31T13:00:00+01:00"));
}
