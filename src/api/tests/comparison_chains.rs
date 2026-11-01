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

/// Items 0..12 with `y = 12 - x`, plus one item without `x`.
fn items() -> Database {
    let mut db = Database::new();
    for id in 0..12 {
        db.query(&format!(
            "CREATE (:Item {{id: {id}, x: {id}, y: {}}})",
            12 - id
        ))
        .unwrap();
    }
    db.query("CREATE (:Item {id: 99, y: 3})").unwrap();
    db
}

fn params() -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("lo".to_string(), Value::Int(3)),
        ("hi".to_string(), Value::Int(7)),
        ("missing".to_string(), Value::Null),
    ])
}

fn ids(db: &mut Database, predicate: &str) -> Vec<i64> {
    let query = format!("MATCH (n:Item) WHERE {predicate} RETURN n.id AS id ORDER BY id");
    db.query_with_params(&query, &params())
        .unwrap_or_else(|error| panic!("{query}: {error}"))
        .rows
        .iter()
        .map(|row| match row["id"] {
            Value::Int(id) => id,
            ref value => panic!("unexpected id {value:?}"),
        })
        .collect()
}

#[test]
fn where_comparison_chains_match_explicit_conjunctions() {
    let mut db = items();
    for (chain, conjunction, expected) in [
        (
            "$lo <= n.x < $hi",
            "n.x >= $lo AND n.x < $hi",
            vec![3, 4, 5, 6],
        ),
        ("$hi > n.x", "n.x < $hi", vec![0, 1, 2, 3, 4, 5, 6]),
        ("9 < n.x", "n.x > 9", vec![10, 11]),
        (
            "$hi > n.x >= $lo",
            "n.x < $hi AND n.x >= $lo",
            vec![3, 4, 5, 6],
        ),
        (
            "1 < n.x < n.y <= 10",
            "1 < n.x AND n.x < n.y AND n.y <= 10",
            vec![2, 3, 4, 5],
        ),
        ("n.x = n.y = 6", "n.x = 6 AND n.y = 6", vec![6]),
        (
            "2 < n.x <> 5",
            "n.x > 2 AND n.x <> 5",
            vec![3, 4, 6, 7, 8, 9, 10, 11],
        ),
        (
            "0 <= n.x < 2 OR 10 < n.x",
            "(n.x >= 0 AND n.x < 2) OR n.x > 10",
            vec![0, 1, 11],
        ),
        (
            "NOT 2 <= n.x <= 9 AND n.y > 1",
            "NOT (n.x >= 2 AND n.x <= 9) AND n.y > 1",
            vec![0, 1, 10],
        ),
    ] {
        let actual = ids(&mut db, chain);
        assert_eq!(actual, expected, "{chain}");
        assert_eq!(ids(&mut db, conjunction), expected, "{conjunction}");
    }
}

#[test]
fn where_comparison_chains_use_three_valued_logic_for_nulls() {
    let mut db = items();
    // `x >= 3 AND null` is unknown where x >= 3 holds and false below it.
    assert_eq!(ids(&mut db, "$lo <= n.x < $missing"), Vec::<i64>::new());
    assert_eq!(ids(&mut db, "NOT $lo <= n.x < $missing"), vec![0, 1, 2]);
    // The item without `x` makes both links unknown.
    assert_eq!(ids(&mut db, "NOT 0 <= n.x < 100"), Vec::<i64>::new());
    assert_eq!(ids(&mut db, "n.y = 3 AND NOT 0 <= n.x < 9"), vec![9]);
}

#[test]
fn case_conditions_accept_parameters_on_left_and_comparison_chains() {
    let mut db = items();
    let output = db
        .query_with_params(
            "MATCH (n:Item) \
             WITH CASE WHEN $hi > n.x >= $lo THEN 1 ELSE 0 END AS valid, \
                  CASE WHEN $lo > n.x THEN 1 ELSE 0 END AS early, \
                  CASE WHEN NOT $lo <= n.x < $missing THEN 1 ELSE 0 END AS below \
             RETURN sum(valid) AS valid, sum(early) AS early, sum(below) AS below",
            &params(),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    let row = &output.rows[0];
    assert_eq!(row["valid"], Value::Int(4));
    assert_eq!(row["early"], Value::Int(3));
    assert_eq!(row["below"], Value::Int(3));
}

#[test]
fn ldbc_interactive_complex_4_comparison_shapes_execute() {
    let mut db = Database::new();
    for query in [
        "CREATE (:Person {id: 1})",
        "CREATE (:Person {id: 2})",
        "CREATE (:Tag {name: 'old'})",
        "CREATE (:Tag {name: 'new'})",
        "CREATE (:Tag {name: 'mixed'})",
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:KNOWS]->(b)",
    ] {
        db.query(query).unwrap();
    }
    for (id, date, tag) in [
        (10, 5, "old"),
        (11, 15, "new"),
        (12, 16, "new"),
        (13, 12, "mixed"),
        (14, 2, "mixed"),
        (15, 30, "new"),
    ] {
        db.query(&format!(
            "CREATE (:Post {{id: {id}, creationDate: {date}}})"
        ))
        .unwrap();
        db.query(&format!(
            "MATCH (f:Person {{id: 2}}), (p:Post {{id: {id}}}) CREATE (p)-[:HAS_CREATOR]->(f)"
        ))
        .unwrap();
        db.query(&format!(
            "MATCH (p:Post {{id: {id}}}), (t:Tag {{name: '{tag}'}}) CREATE (p)-[:HAS_TAG]->(t)"
        ))
        .unwrap();
    }
    let params = BTreeMap::from([
        ("personId".to_string(), Value::Int(1)),
        ("startDate".to_string(), Value::Int(10)),
        ("endDate".to_string(), Value::Int(20)),
    ]);
    let output = db
        .query_with_params(
            "MATCH (person:Person {id: $personId})-[:KNOWS]-(friend:Person), \
                   (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag) \
             WITH DISTINCT tag, post \
             WITH tag, \
                  CASE WHEN $endDate > post.creationDate >= $startDate THEN 1 ELSE 0 END AS valid, \
                  CASE WHEN $startDate > post.creationDate THEN 1 ELSE 0 END AS inValid \
             WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount \
             WHERE postCount > 0 AND inValidPostCount = 0 \
             RETURN tag.name AS tagName, postCount \
             ORDER BY postCount DESC, tagName ASC \
             LIMIT 10",
            &params,
        )
        .unwrap();
    let rows: Vec<_> = output
        .rows
        .iter()
        .map(|row| (row["tagName"].clone(), row["postCount"].clone()))
        .collect();
    assert_eq!(rows, vec![(Value::String("new".into()), Value::Int(2))]);
}

#[test]
fn comparison_chains_keep_property_range_index_seeks() {
    // A selective range: only ids 3..=6 fall in [3, 7); fillers are negative.
    let mut db = Database::new();
    for id in 0..32 {
        let x = if (3..=6).contains(&id) { id } else { -id };
        db.query(&format!("CREATE (:Item {{id: {id}, x: {x}}})"))
            .unwrap();
    }
    db.query("CREATE RANGE INDEX ON :Item(x)").unwrap();
    for (chain, conjunction) in [
        ("$lo <= n.x < $hi", "n.x >= $lo AND n.x < $hi"),
        ("$hi > n.x >= $lo", "n.x < $hi AND n.x >= $lo"),
        ("5 < n.x", "n.x > 5"),
    ] {
        let explain = |db: &mut Database, predicate: &str| {
            db.explain_query_with_params(
                &format!("MATCH (n:Item) WHERE {predicate} RETURN n.id AS id"),
                &params(),
            )
            .unwrap()
            .physical_plan
            .explain(0)
        };
        let plan = explain(&mut db, chain);
        assert!(plan.contains("IndexNodeRangeSeek"), "{chain}: {plan}");
        assert_eq!(plan, explain(&mut db, conjunction), "{chain}");
    }
    assert_eq!(ids(&mut db, "$lo <= n.x < $hi"), vec![3, 4, 5, 6]);
    assert_eq!(ids(&mut db, "5 < n.x"), vec![6]);
}
