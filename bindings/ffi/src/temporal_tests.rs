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

#[test]
fn temporal_json_values_round_trip_as_tagged_iso_strings() {
    for (value, expected) in [
        (
            json!({"$date": "2026-10-30"}),
            Value::Date(hawdb::temporal::parse_date("2026-10-30").unwrap()),
        ),
        (
            json!({"$localtime": "12:30:00.500"}),
            Value::LocalTime(hawdb::temporal::parse_local_time("12:30:00.5").unwrap()),
        ),
        (
            json!({"$localdatetime": "2026-10-30T12:30:00"}),
            Value::LocalDateTime(
                hawdb::temporal::parse_local_datetime("2026-10-30T12:30").unwrap(),
            ),
        ),
        (
            json!({"$datetime": "2026-10-30T12:00:00+02:00"}),
            Value::DateTime(hawdb::temporal::parse_datetime("2026-10-30T12:00:00+02:00").unwrap()),
        ),
        (
            json!({"$duration": "P1M2DT3.5S"}),
            Value::duration(hawdb::temporal::parse_duration("P1M2DT3.5S").unwrap()),
        ),
    ] {
        assert_eq!(json_to_value(&value).unwrap(), expected, "{value}");
        assert_eq!(value_to_json(&expected), value);
    }
    for invalid in [
        json!({"$date": "2026-02-30"}),
        json!({"$datetime": 7}),
        json!({"$duration": "P"}),
        json!({"$localtime": "25:00"}),
    ] {
        assert!(json_to_value(&invalid).is_err(), "{invalid}");
    }
    // Tags only apply to single-key objects.
    assert!(matches!(
        json_to_value(&json!({"$date": "2026-10-30", "other": 1})).unwrap(),
        Value::Map(_)
    ));
}

#[test]
fn temporal_query_results_and_parameters_use_tagged_json() {
    let mut db = hawdb::Database::new();
    db.query_with_params(
        "CREATE (:Event {on: $on, took: duration('PT90M')})",
        &BTreeMap::from([(
            "on".to_string(),
            json_to_value(&json!({"$date": "2026-10-30"})).unwrap(),
        )]),
    )
    .unwrap();
    let output = db
        .query("MATCH (e:Event) RETURN e.on AS on, e.took AS took, e.on.year AS year")
        .unwrap();
    assert_eq!(
        output_to_json(&output),
        json!({
            "columns": ["on", "took", "year"],
            "rows": [[{"$date": "2026-10-30"}, {"$duration": "PT1H30M"}, 2026]],
        })
    );
}
