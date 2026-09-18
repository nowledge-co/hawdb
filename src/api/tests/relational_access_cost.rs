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
use hawdb_optimizer::RelationalOperatorKind;

#[test]
fn relational_cost_selection_preserves_results_and_reports_actual_access() {
    let mut database = Database::new();
    database.query_sql("CREATE TABLE cost_rows (id BIGINT PRIMARY KEY, bucket BIGINT NOT NULL, body TEXT NOT NULL)").unwrap();
    database
        .query_sql("CREATE INDEX by_bucket ON cost_rows (bucket)")
        .unwrap();
    for id in 0..100 {
        let bucket = if id < 90 {
            0
        } else if id < 99 {
            1
        } else {
            2
        };
        database
            .query_sql_with_params(
                "INSERT INTO cost_rows (id, bucket, body) VALUES ($1, $2, $3)",
                &[
                    Value::Int(id),
                    Value::Int(bucket),
                    Value::String(format!("body-{id}")),
                ],
            )
            .unwrap();
    }
    let read = database.begin_read_transaction();
    for (column, value, payload, expected, operator) in [
        (
            "bucket",
            0,
            true,
            (0..90).collect::<Vec<_>>(),
            RelationalOperatorKind::TableFullScan,
        ),
        (
            "bucket",
            2,
            true,
            vec![99],
            RelationalOperatorKind::IndexRangeScan,
        ),
        (
            "bucket",
            0,
            false,
            (0..90).collect(),
            RelationalOperatorKind::IndexRangeScan,
        ),
        (
            "id",
            99,
            true,
            vec![99],
            RelationalOperatorKind::TablePointGet,
        ),
    ] {
        let projection = if payload { "id, body" } else { "id" };
        let sql = format!("SELECT {projection} FROM cost_rows WHERE {column} = $1");
        let result = read
            .query_sql_with_params_options_profiled(
                &sql,
                &[Value::Int(value)],
                QueryStreamOptions::default(),
            )
            .unwrap();
        let access = &result.profile.operator_cardinality_profiles[0];
        assert_eq!(access.operator, operator, "{sql}");
        assert!(access.fully_consumed);
        assert_eq!(
            access.actual_rows,
            Some(if operator == RelationalOperatorKind::TableFullScan {
                100
            } else {
                expected.len()
            })
        );
        if operator == RelationalOperatorKind::IndexRangeScan {
            assert_eq!(access.access_path.covering, !payload);
            assert_eq!(access.access_path.requires_row_fetch, payload);
        }
        let mut ids = Vec::new();
        for row in &result.output.rows {
            let Value::Int(id) = row["id"] else {
                panic!("integer id")
            };
            ids.push(id);
            if payload {
                assert_eq!(row["body"], Value::String(format!("body-{id}")));
            }
        }
        ids.sort_unstable();
        assert_eq!(ids, expected);
    }
}
