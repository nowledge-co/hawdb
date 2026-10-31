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
use hawdb_core::temporal;

fn plan_query(query: &str, parameters: &[(&str, Value)]) -> LogicalPlan {
    let statement = hawdb_cypher::parse(query).unwrap();
    plan_with_params(
        &statement,
        &parameters
            .iter()
            .map(|(name, value)| ((*name).to_string(), value.clone()))
            .collect(),
    )
    .unwrap()
}

fn rendered(plan: &LogicalPlan) -> String {
    format!("{plan:?}")
}

#[test]
fn constant_temporal_window_bounds_fold_to_native_property_comparisons() {
    let plan = plan_query(
        "MATCH (m:Message) WHERE m.creationDate >= datetime($start) \
         AND m.creationDate < datetime($start) + duration({days: $days}) RETURN m.id AS id",
        &[
            ("start", Value::String("2012-08-01T00:00:00Z".into())),
            ("days", Value::Int(31)),
        ],
    );
    let text = rendered(&plan);
    assert!(!text.contains("TemporalFunction"), "{text}");
    assert!(
        text.contains("PropertyCompare { variable: \"m\", property: \"creationDate\", op: Lt, value: DateTime(2012-09-01T00:00:00+00:00)"),
        "{text}"
    );
    // A reversed comparison flips its operator.
    let text = rendered(&plan_query(
        "MATCH (m:Message) WHERE date('2026-01-01') <= m.on RETURN m.id AS id",
        &[],
    ));
    assert!(text.contains("op: Gte, value: Date(2026-01-01)"), "{text}");
    // Equality folds as well.
    let text = rendered(&plan_query(
        "MATCH (m:Message) WHERE m.on = date({year: 2026, month: 1, day: 2}) RETURN m.id AS id",
        &[],
    ));
    assert!(text.contains("PropertyEq"), "{text}");
}

#[test]
fn non_constant_invalid_and_duration_comparisons_stay_expressions() {
    for query in [
        // A row-dependent argument is evaluated per row.
        "MATCH (m:Message) WHERE m.creationDate >= datetime({epochMillis: m.ms}) RETURN m.id AS id",
        // Invalid constants report their error at run time instead of planning.
        "MATCH (m:Message) WHERE m.on >= date('2026-02-30') RETURN m.id AS id",
        // Durations have no order: `<` must remain three-valued.
        "MATCH (m:Message) WHERE m.took < duration('P1D') RETURN m.id AS id",
    ] {
        let text = rendered(&plan_query(query, &[]));
        assert!(text.contains("ExpressionCompare"), "{query}: {text}");
    }
}

#[test]
fn clock_reads_bind_once_per_statement_to_the_pinned_clock() {
    let pinned = temporal::DateTime::from_timestamp(86_400, 0).unwrap();
    let plan = temporal::with_statement_clock(pinned, || {
        plan_query(
            "MATCH (n:N) RETURN date() AS d, datetime() AS dt, localtime() AS t",
            &[],
        )
    });
    let text = rendered(&plan);
    assert!(text.contains("Literal(Date(1970-01-02))"), "{text}");
    assert!(
        text.contains("Literal(DateTime(1970-01-02T00:00:00+00:00))"),
        "{text}"
    );
    assert!(text.contains("Literal(LocalTime(00:00:00))"), "{text}");
}

#[test]
fn projections_plan_temporal_calls_property_access_and_maps() {
    let text = rendered(&plan_query(
        "MATCH (p:P) RETURN p.birthday.month AS month, \
         duration.between(p.a, p.b).minutes AS minutes, {k: p.a} AS map",
        &[],
    ));
    assert!(text.contains("PropertyAccess"), "{text}");
    assert!(
        text.contains("TemporalFunction { function: DurationBetween"),
        "{text}"
    );
    assert!(text.contains("MapLiteral"), "{text}");
    let text = rendered(&plan_query(
        "MATCH (p:P) RETURN p.birthday.month, date.truncate('month', p.on)",
        &[],
    ));
    assert!(text.contains("name: \"p.birthday.month\""), "{text}");
    assert!(text.contains("name: \"date.truncate\""), "{text}");
}
