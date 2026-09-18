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

fn cross_join_fixture(left_rows: i64, right_rows: i64) -> Database {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE cross_left (lid BIGINT PRIMARY KEY, lvalue TEXT)")
        .unwrap();
    database
        .query_sql("CREATE TABLE cross_right (rid BIGINT PRIMARY KEY, rvalue TEXT)")
        .unwrap();
    for id in 0..left_rows {
        database
            .query_sql_with_params(
                "INSERT INTO cross_left (lid, lvalue) VALUES ($1, $2)",
                &[Value::Int(id), Value::Null],
            )
            .unwrap();
    }
    for id in 0..right_rows {
        database
            .query_sql_with_params(
                "INSERT INTO cross_right (rid, rvalue) VALUES ($1, $2)",
                &[Value::Int(id), Value::String("repeated".into())],
            )
            .unwrap();
    }
    database
}

#[test]
fn relational_cross_join_cartesian_matrix_preserves_empty_inputs_nulls_and_duplicates() {
    for left_rows in 0..4 {
        for right_rows in 0..4 {
            let mut database = cross_join_fixture(left_rows, right_rows);
            let output = database
                .query_sql("SELECT * FROM cross_left CROSS JOIN cross_right ORDER BY lid, rid")
                .unwrap();
            let expected = (0..left_rows)
                .flat_map(|left| {
                    (0..right_rows).map(move |right| {
                        BTreeMap::from([
                            ("lid".into(), Value::Int(left)),
                            ("lvalue".into(), Value::Null),
                            ("rid".into(), Value::Int(right)),
                            ("rvalue".into(), Value::String("repeated".into())),
                        ])
                    })
                })
                .collect::<Vec<_>>();
            assert_eq!(
                output.rows.iter().collect::<Vec<_>>(),
                expected,
                "{left_rows} x {right_rows}"
            );
            let projected = database
                .query_sql("SELECT lvalue, rvalue FROM cross_left CROSS JOIN cross_right")
                .unwrap();
            assert_eq!(projected.rows.len(), expected.len());
            let count = database
                .query_sql("SELECT COUNT(*) AS total FROM cross_left CROSS JOIN cross_right")
                .unwrap();
            assert_eq!(count.rows[0]["total"], Value::Int(left_rows * right_rows));
            let distinct = database
                .query_sql("SELECT DISTINCT lvalue, rvalue FROM cross_left CROSS JOIN cross_right")
                .unwrap();
            assert_eq!(distinct.rows.len(), usize::from(!expected.is_empty()));
        }
    }
}

#[test]
fn relational_cross_join_keeps_left_join_nesting_and_prior_on_bindings() {
    let mut database = cross_join_fixture(2, 2);
    database
        .query_sql("CREATE TABLE cross_tags (tid BIGINT PRIMARY KEY)")
        .unwrap();
    database
        .query_sql("INSERT INTO cross_tags (tid) VALUES (1)")
        .unwrap();
    let output = database
        .query_sql(
            "SELECT l.lid, r.rid, t.tid FROM cross_left l CROSS JOIN cross_right r \
         LEFT JOIN cross_tags t ON l.lid = t.tid ORDER BY l.lid, r.rid",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 4);
    for row in output.rows.iter() {
        assert_eq!(
            row["tid"],
            if row["lid"] == Value::Int(1) {
                Value::Int(1)
            } else {
                Value::Null
            }
        );
    }
    let reversed = database
        .query_sql(
            "SELECT l.lid, r.rid, t.tid FROM cross_left l \
         LEFT JOIN cross_tags t ON l.lid = t.tid CROSS JOIN cross_right r \
         ORDER BY l.lid, r.rid",
        )
        .unwrap();
    assert_eq!(output.rows, reversed.rows);
    let inner = database
        .query_sql(
            "SELECT l.lid, r.rid FROM cross_left l CROSS JOIN cross_right r \
         INNER JOIN cross_tags t ON l.lid = t.tid ORDER BY r.rid",
        )
        .unwrap();
    assert_eq!(inner.rows.len(), 2);
    assert!(inner.rows.iter().all(|row| row["lid"] == Value::Int(1)));
    database
        .query_sql("DELETE FROM cross_right WHERE rid >= 0")
        .unwrap();
    assert!(database
        .query_sql(
            "SELECT l.lid FROM cross_left l LEFT JOIN cross_tags t ON l.lid = t.tid \
         CROSS JOIN cross_right r",
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn relational_cross_join_cache_rebinds_parameters_and_replans_current_data() {
    let mut database = cross_join_fixture(3, 2);
    let sql = "SELECT l.lid, r.rid FROM cross_left l CROSS JOIN cross_right r \
               WHERE l.lid >= $1 AND r.rid = $2 ORDER BY l.lid LIMIT $3 OFFSET $4";
    let before = database.relational_plan_template_cache_stats();
    for (left, right, expected) in [(0, 0, 1), (1, 1, 2), (0, 1, 1)] {
        let output = database
            .query_sql_with_params(
                sql,
                &[
                    Value::Int(left),
                    Value::Int(right),
                    Value::Int(1),
                    Value::Int(1),
                ],
            )
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["lid"], Value::Int(expected));
        assert_eq!(output.rows[0]["rid"], Value::Int(right));
    }
    assert!(database
        .query_sql_with_params(
            sql,
            &[Value::Int(0), Value::Null, Value::Int(1), Value::Int(0)],
        )
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        database.relational_plan_template_cache_stats().hits,
        before.hits + 3
    );
    database
        .query_sql("DELETE FROM cross_right WHERE rid = 1")
        .unwrap();
    assert!(database
        .query_sql_with_params(
            sql,
            &[Value::Int(0), Value::Int(1), Value::Int(1), Value::Int(0)],
        )
        .unwrap()
        .rows
        .is_empty());
    assert!(database
        .query_sql_with_params(sql, &[Value::Int(0)])
        .is_err());
}

#[test]
fn relational_cross_join_fails_when_result_budgets_are_exceeded() {
    let mut database = cross_join_fixture(2, 2);
    let sql = "SELECT l.lid, r.rid FROM cross_left l CROSS JOIN cross_right r";
    for (max_rows, max_payload_bytes, expected) in [
        (Some(3), None, "max_output_rows 3"),
        (Some(4), Some(1), "max_output_payload_bytes 1"),
    ] {
        let error = database
            .query_sql_with_params_options(
                sql,
                &[],
                QueryStreamOptions {
                    max_rows,
                    max_payload_bytes,
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
    assert_eq!(database.query_sql(sql).unwrap().rows.len(), 4);
}
