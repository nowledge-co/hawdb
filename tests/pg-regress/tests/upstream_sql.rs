// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Execute unchanged upstream queries against HawDB, using checked-in PostgreSQL
//! expected output. No reference server, libpq, environment or subprocess is used.

use std::fs;
use std::path::Path;

use hawdb::{Database, HawDBError, Value};
use serde::Deserialize;

#[path = "../src/suite.rs"]
mod suite;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expectations {
    cases: Vec<Expectation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expectation {
    id: String,
    source: suite::Range,
    #[serde(default)]
    kinds: Vec<Kind>,
    #[serde(default)]
    fixture_key: bool,
    #[serde(default)]
    error: Option<ExpectedError>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedError {
    upstream: String,
    hawdb: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Int,
    NullableInt,
    Text,
}

// Only the bounded integer/ASCII-word fixtures selected below use this decoder.
// Aligned psql output cannot distinguish blank strings from NULL, or preserve
// arbitrary surrounding text whitespace. Do not extend this decoder to those
// text cases. Only an explicitly nullable integer can decode a blank cell.
fn expected_table(text: &str, kinds: &[Kind]) -> (Vec<String>, Vec<Vec<Value>>) {
    let mut lines = text.lines();
    let columns = lines
        .next()
        .expect("expected header")
        .split('|')
        .map(|name| name.trim().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(columns.len(), kinds.len());
    assert!(columns.iter().all(|name| !name.is_empty()));
    let separator = lines.next().expect("expected table separator");
    assert!(separator.chars().all(|c| matches!(c, '-' | '+')));
    assert_eq!(separator.split('+').count(), columns.len());
    let mut body = lines.collect::<Vec<_>>();
    let footer = body.pop().expect("expected row count");
    let rows = body
        .iter()
        .map(|line| {
            let cells = line.split('|').map(str::trim).collect::<Vec<_>>();
            assert_eq!(cells.len(), kinds.len());
            cells
                .iter()
                .zip(kinds)
                .map(|(cell, kind)| match kind {
                    Kind::NullableInt if cell.is_empty() => Value::Null,
                    Kind::Int | Kind::NullableInt => {
                        Value::Int(cell.parse().expect("expected integer cell"))
                    }
                    Kind::Text => {
                        assert!(!cell.is_empty() && cell.bytes().all(|c| c.is_ascii_alphabetic()));
                        Value::String((*cell).to_owned())
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let suffix = if rows.len() == 1 { "row" } else { "rows" };
    assert_eq!(footer, format!("({} {suffix})", rows.len()));
    (columns, rows)
}

fn upstream_case(id: &str) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    // Validate every upstream checksum and exact SQL source range before use.
    let suite = suite::Suite::load(root).unwrap();
    let expectations: Expectations =
        serde_json::from_slice(&fs::read(root.join("data/hawdb-expectations.json")).unwrap())
            .unwrap();
    assert_eq!(expectations.cases.len(), 29);
    let expected = expectations
        .cases
        .iter()
        .find(|case| case.id == id)
        .unwrap();
    let selected = suite.select(&root.join("data/schedule"), id).unwrap();
    let case = selected.iter().find(|case| case.id == id).unwrap();
    assert!(case.unsupported.is_none());
    let (query, setup) = case.steps.split_last().unwrap();
    assert!(
        query.execute_sql.is_none(),
        "upstream query must be unchanged"
    );
    assert!(suite.sources.iter().any(|s| s.file == expected.source.file));
    let output = fs::read_to_string(root.join(&expected.source.file)).unwrap();
    let lines = output.split_inclusive('\n').collect::<Vec<_>>();
    let block = lines[expected.source.first_line - 1..expected.source.last_line].concat();
    let result = block
        .strip_prefix(&query.sql)
        .expect("expected output must echo this exact upstream query")
        .strip_prefix('\n')
        .unwrap();

    let mut database = Database::new();
    for step in setup {
        database
            .query_sql(step.execute_sql.as_deref().unwrap_or(&step.sql))
            .unwrap_or_else(|error| panic!("{id}: fixture failed: {error}"));
    }
    let actual = database.query_sql(&query.sql);
    if let Some(error) = &expected.error {
        assert_eq!(result.lines().next().unwrap(), error.upstream);
        assert!(expected.kinds.is_empty() && !expected.fixture_key);
        let HawDBError::Semantic(message) = actual.unwrap_err() else {
            panic!("{id}: expected a typed ungrouped-column semantic error");
        };
        assert_eq!(message, error.hawdb, "{id}");
        return;
    }
    let actual = actual.unwrap_or_else(|error| panic!("{id}: {error}"));
    let (columns, mut expected_rows) = expected_table(result, &expected.kinds);
    let mut actual_columns = actual.schema().columns().to_vec();
    let mut actual_rows = actual.value_rows().map(<[_]>::to_vec).collect::<Vec<_>>();
    if expected.fixture_key {
        // The additional mandatory key is a documented fixture adaptation.
        // Remove only that exact leading column; retain all upstream values,
        // row multiplicity and any unexpected schema differences.
        assert_eq!(actual_columns.remove(0), "_test_id");
        for row in &mut actual_rows {
            assert!(matches!(row.remove(0), Value::Int(_)));
        }
    }
    assert_eq!(actual_columns, columns, "{id}: result columns");
    if !query.ordered {
        actual_rows.sort_by_cached_key(|row| format!("{row:?}"));
        expected_rows.sort_by_cached_key(|row| format!("{row:?}"));
    }
    assert_eq!(actual_rows, expected_rows, "{id}: upstream expected rows");
}

macro_rules! regression {
    ($name:ident, $id:literal) => {
        #[test]
        fn $name() {
            upstream_case($id);
        }
    };
}

regression!(int8_eq_int8, "adapted/int8/comparison-0");
regression!(int8_ne_int8, "adapted/int8/comparison-1");
regression!(int8_lt_int8, "adapted/int8/comparison-2");
regression!(int8_gt_int8, "adapted/int8/comparison-3");
regression!(int8_le_int8, "adapted/int8/comparison-4");
regression!(int8_ge_int8, "adapted/int8/comparison-5");
regression!(int8_eq_int4, "adapted/int8/comparison-6");
regression!(int8_ne_int4, "adapted/int8/comparison-7");
regression!(int8_lt_int4, "adapted/int8/comparison-8");
regression!(int8_gt_int4, "adapted/int8/comparison-9");
regression!(int8_le_int4, "adapted/int8/comparison-10");
regression!(int8_ge_int4, "adapted/int8/comparison-11");
regression!(having_min_max_equal, "adapted/select_having/min-max-equal");
regression!(having_min_max_less, "adapted/select_having/min-max-less");
regression!(
    having_ungrouped_projection,
    "adapted/select_having/ungrouped-projection-error"
);
regression!(
    having_ungrouped_predicate,
    "adapted/select_having/ungrouped-having-error"
);
regression!(
    having_constant_false,
    "adapted/select_having/constant-false"
);
regression!(having_constant_true, "adapted/select_having/constant-true");
regression!(
    non_projected_order_key,
    "adapted/select_implicit/non-projected-order-key"
);

regression!(fromless_in_match, "subselect/fromless-in-match");
regression!(fromless_not_in_match, "subselect/fromless-not-in-match");
regression!(fromless_in_miss, "subselect/fromless-in-miss");
regression!(subselect_constant, "adapted/subselect/constant-membership");
regression!(
    subselect_uncorrelated,
    "adapted/subselect/uncorrelated-membership"
);
regression!(subselect_nested, "adapted/subselect/nested-membership");
regression!(
    subselect_correlated,
    "adapted/subselect/correlated-membership"
);
regression!(recursive_with_sum, "with/recursive-sum");
regression!(subselect_row_null, "adapted/subselect/row-not-in-null");
regression!(subselect_empty_null, "adapted/subselect/empty-not-in-null");
