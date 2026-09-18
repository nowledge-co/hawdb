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

fn from_scope_fixture() -> Database {
    let mut database = Database::new();
    for sql in [
        "CREATE TABLE scope_a (aid BIGINT PRIMARY KEY, join_key BIGINT)",
        "CREATE TABLE scope_b (bid BIGINT PRIMARY KEY, join_key BIGINT)",
        "CREATE TABLE scope_c (cid BIGINT PRIMARY KEY, owner BIGINT)",
        "INSERT INTO scope_a (aid, join_key) VALUES (1, 101), (2, 102)",
        "INSERT INTO scope_b (bid, join_key) VALUES (10, 101), (20, 999)",
        "INSERT INTO scope_c (cid, owner) VALUES (100, 101)",
    ] {
        database.query_sql(sql).unwrap();
    }
    database
}

#[test]
fn relational_comma_from_binds_unqualified_on_columns_in_its_own_item() {
    let mut database = from_scope_fixture();
    let output = database
        .query_sql(
            "SELECT a.aid, b.bid, c.cid FROM scope_a a, scope_b b \
         LEFT JOIN scope_c c ON join_key = c.owner ORDER BY a.aid, b.bid",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 4);
    for row in output.rows.iter() {
        assert_eq!(
            row["cid"],
            if row["bid"] == Value::Int(10) {
                Value::Int(100)
            } else {
                Value::Null
            }
        );
    }
    let cross = database
        .query_sql(
            "SELECT a.aid, b.bid, c.cid FROM scope_a a CROSS JOIN scope_b b \
         LEFT JOIN scope_c c ON a.join_key = c.owner ORDER BY a.aid, b.bid",
        )
        .unwrap();
    assert_eq!(cross.rows.len(), 4);
    for row in cross.rows.iter() {
        assert_eq!(
            row["cid"],
            if row["aid"] == Value::Int(1) {
                Value::Int(100)
            } else {
                Value::Null
            }
        );
    }
    let joined = database
        .query_sql_with_params(
            "SELECT a.aid, b.bid FROM scope_a a, scope_b b \
         WHERE a.join_key = b.join_key AND a.aid = $1",
            &[Value::Int(1)],
        )
        .unwrap();
    assert_eq!(joined.rows.len(), 1);
    assert_eq!(joined.rows[0]["bid"], Value::Int(10));
}

#[test]
fn relational_comma_from_rejects_out_of_scope_names_before_empty_scans() {
    let mut database = from_scope_fixture();
    for empty in [false, true] {
        if empty {
            database
                .query_sql("DELETE FROM scope_a WHERE aid >= 0")
                .unwrap();
        }
        for sql in [
            "SELECT a.aid FROM scope_a a, scope_b b JOIN scope_c c ON a.join_key = c.owner",
            "SELECT a.aid FROM scope_a a, scope_b b JOIN scope_c c ON aid = c.owner",
            "SELECT a.aid FROM scope_a a, scope_b b JOIN scope_c c ON scope_b.join_key = c.owner",
            "SELECT a.aid FROM scope_a a, scope_b b JOIN scope_c c ON d.owner = b.join_key JOIN scope_c d ON c.cid = d.cid",
            "SELECT a.aid FROM scope_a a, scope_b b WHERE join_key = 101",
            "SELECT a.aid FROM scope_a a, scope_b a",
        ] {
            assert!(database.query_sql(sql).is_err(), "accepted {sql}; empty={empty}");
        }
    }
}

#[test]
fn relational_comma_from_preserves_outer_join_groups_and_template_rebinding() {
    let mut database = from_scope_fixture();
    let sql = "SELECT a.aid, c.cid, b.bid FROM scope_a a \
               LEFT JOIN scope_c c ON a.join_key = c.owner, scope_b b \
               WHERE b.bid = $1 ORDER BY a.aid";
    for bid in [10, 20, 10] {
        let output = database
            .query_sql_with_params(sql, &[Value::Int(bid)])
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0]["cid"], Value::Int(100));
        assert_eq!(output.rows[1]["cid"], Value::Null);
        assert!(output.rows.iter().all(|row| row["bid"] == Value::Int(bid)));
    }
    database
        .query_sql("DELETE FROM scope_b WHERE bid >= 0")
        .unwrap();
    assert!(database
        .query_sql_with_params(sql, &[Value::Int(10)])
        .unwrap()
        .rows
        .is_empty());
}
