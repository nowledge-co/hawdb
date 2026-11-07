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

//! A variable unwound from a list of nodes is a bound node in a later MATCH
//! (#1486), as in LDBC SNB IC6 and IC9. The MATCH checks each element at
//! runtime: a NULL element matches nothing and a non-node value fails.

use super::*;

/// Persons 1..4 with `1-2`, `1-3`, `2-4` KNOWS, a non-Person friend 9 of 1,
/// and messages 10..14 created by 2, 2, 3, 4 and 9.
fn social() -> Database {
    let mut db = Database::new();
    for id in 1..=4 {
        db.query(&format!("CREATE (:Person {{id: {id}}})")).unwrap();
    }
    db.query("CREATE (:Robot {id: 9})").unwrap();
    for (left, right) in [(1, 2), (1, 3), (2, 4)] {
        db.query(&format!(
            "MATCH (a:Person {{id: {left}}}), (b:Person {{id: {right}}}) CREATE (a)-[:KNOWS]->(b)"
        ))
        .unwrap();
    }
    db.query("MATCH (a:Robot {id: 9}), (b:Person {id: 1}) CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    for (message, creator, label) in [
        (10, 2, "Person"),
        (11, 2, "Person"),
        (12, 3, "Person"),
        (13, 4, "Person"),
        (14, 9, "Robot"),
    ] {
        db.query(&format!("CREATE (:Message {{id: {message}}})"))
            .unwrap();
        db.query(&format!(
            "MATCH (m:Message {{id: {message}}}), (c:{label} {{id: {creator}}}) \
             CREATE (m)-[:HAS_CREATOR]->(c)"
        ))
        .unwrap();
    }
    db
}

fn rows(db: &mut Database, query: &str) -> Vec<String> {
    let parameters = BTreeMap::from([("id".to_string(), Value::Int(1))]);
    let mut rows = db
        .query_with_params(query, &parameters)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
        .rows
        .iter()
        .map(|row| format!("{row:?}"))
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn error(db: &mut Database, query: &str) -> String {
    match db.query(query) {
        Ok(output) => panic!("{query}: expected an error, got {:?}", output.rows),
        Err(error) => error.to_string(),
    }
}

fn counts(db: &mut Database, query: &str) -> Vec<(i64, i64)> {
    let mut counts = db
        .query_with_params(query, &BTreeMap::from([("id".to_string(), Value::Int(1))]))
        .unwrap_or_else(|error| panic!("{query}: {error}"))
        .rows
        .iter()
        .map(|row| match (&row["id"], &row["messages"]) {
            (Value::Int(id), Value::Int(messages)) => (*id, *messages),
            other => panic!("{query}: {other:?}"),
        })
        .collect::<Vec<_>>();
    counts.sort_unstable();
    counts
}

#[test]
fn ic6_shape_matches_from_collected_friends() {
    let mut db = social();
    let unwound = "MATCH (p:Person {id: $id})-[:KNOWS]-(friend) \
         WITH collect(friend) AS friends UNWIND friends AS f \
         MATCH (f)<-[:HAS_CREATOR]-(m:Message) RETURN f.id AS id, count(m) AS messages";
    assert_eq!(counts(&mut db, unwound), vec![(2, 2), (3, 1), (9, 1)]);
    let direct = "MATCH (p:Person {id: $id})-[:KNOWS]-(f) \
         MATCH (f)<-[:HAS_CREATOR]-(m:Message) RETURN f.id AS id, count(m) AS messages";
    assert_eq!(rows(&mut db, unwound), rows(&mut db, direct));
}

#[test]
fn label_constraint_on_unwound_node_filters_elements() {
    let mut db = social();
    let unwound = "MATCH (p:Person {id: $id})-[:KNOWS]-(friend) \
         WITH collect(friend) AS friends UNWIND friends AS f \
         MATCH (f:Person)<-[:HAS_CREATOR]-(m) RETURN f.id AS id, count(m) AS messages";
    assert_eq!(counts(&mut db, unwound), vec![(2, 2), (3, 1)]);
    let direct = "MATCH (p:Person {id: $id})-[:KNOWS]-(f) \
         MATCH (f:Person)<-[:HAS_CREATOR]-(m) RETURN f.id AS id, count(m) AS messages";
    assert_eq!(rows(&mut db, unwound), rows(&mut db, direct));
}

#[test]
fn ic9_shape_matches_from_distinct_two_hop_friends() {
    let mut db = social();
    let unwound = "MATCH (p:Person {id: $id})-[:KNOWS*1..2]-(friend:Person) \
         WHERE friend.id <> $id \
         WITH collect(DISTINCT friend) AS friends UNWIND friends AS f \
         MATCH (f)<-[:HAS_CREATOR]-(m:Message) RETURN f.id AS id, m.id AS message";
    let direct = "MATCH (p:Person {id: $id})-[:KNOWS*1..2]-(f:Person) \
         WHERE f.id <> $id WITH DISTINCT f \
         MATCH (f)<-[:HAS_CREATOR]-(m:Message) RETURN f.id AS id, m.id AS message";
    let result = rows(&mut db, unwound);
    assert_eq!(result.len(), 4, "{result:?}");
    assert_eq!(result, rows(&mut db, direct));
}

#[test]
fn distinct_unwound_nodes_deduplicate_before_match() {
    let mut db = social();
    // Person 2 created two messages, so it is collected twice.
    let collected = "MATCH (:Message)-[:HAS_CREATOR]->(friend:Person) \
         WITH collect(friend) AS friends UNWIND friends AS f";
    let distinct = format!(
        "{collected} WITH DISTINCT f \
         MATCH (f)<-[:HAS_CREATOR]-(m) RETURN f.id AS id, count(m) AS messages"
    );
    assert_eq!(counts(&mut db, &distinct), vec![(2, 2), (3, 1), (4, 1)]);
    let repeated = format!(
        "{collected} MATCH (f)<-[:HAS_CREATOR]-(m) RETURN f.id AS id, count(m) AS messages"
    );
    assert_eq!(counts(&mut db, &repeated), vec![(2, 4), (3, 1), (4, 1)]);
}

#[test]
fn nested_unwind_levels_keep_node_elements() {
    let mut db = social();
    let unwound = "MATCH (p:Person {id: $id})-[:KNOWS]-(friend) \
         WITH collect(friend) AS friends WITH collect(friends) AS groups \
         UNWIND groups AS g UNWIND g AS f \
         MATCH (f)<-[:HAS_CREATOR]-(m:Message) RETURN f.id AS id, count(m) AS messages";
    assert_eq!(counts(&mut db, unwound), vec![(2, 2), (3, 1), (9, 1)]);
}

#[test]
fn null_elements_match_nothing() {
    let mut db = social();
    let unwound = "MATCH (p:Person {id: $id})-[:KNOWS]-(friend:Person) \
         UNWIND [friend, null] AS f \
         MATCH (f)<-[:HAS_CREATOR]-(m:Message) RETURN f.id AS id, count(m) AS messages";
    assert_eq!(counts(&mut db, unwound), vec![(2, 2), (3, 1)]);
    assert!(rows(
        &mut db,
        "UNWIND [null] AS f MATCH (f)-[:KNOWS]->() RETURN f"
    )
    .is_empty());
    let optional = db
        .query("UNWIND [null] AS f OPTIONAL MATCH (f)-[:KNOWS]->(g) RETURN g")
        .unwrap();
    assert_eq!(optional.rows.len(), 1);
    assert_eq!(optional.rows[0]["g"], Value::Null);
}

#[test]
fn non_node_elements_fail_clearly() {
    let mut db = social();
    let message = error(
        &mut db,
        "UNWIND [1, 2] AS f MATCH (f)-[:KNOWS]->() RETURN f",
    );
    assert!(
        message.contains("MATCH variable 'f' must be bound to a node, found an integer"),
        "{message}"
    );
    let message = error(
        &mut db,
        "MATCH (p:Person {id: 1}) WITH collect(p.id) AS ids UNWIND ids AS f \
         MATCH (f)-[:KNOWS]->() RETURN f",
    );
    assert!(
        message.contains("MATCH variable 'f' must be bound to a node, found an integer"),
        "{message}"
    );
    let message = error(
        &mut db,
        "UNWIND [{id: 1}] AS f MATCH (f)-[:KNOWS]->() RETURN f",
    );
    assert!(
        message.contains("MATCH variable 'f' must be bound to a node, found a map"),
        "{message}"
    );
    // A parameter list is only checked when the MATCH reads it.
    let message = db
        .query_with_params(
            "UNWIND $names AS f MATCH (f)-[:KNOWS]->() RETURN f",
            &BTreeMap::from([(
                "names".to_string(),
                Value::List(vec![Value::String("ada".to_string())]),
            )]),
        )
        .unwrap_err()
        .to_string();
    assert!(
        message.contains("MATCH variable 'f' must be bound to a node, found a string"),
        "{message}"
    );
    // A relationship value is not a node either.
    let message = error(
        &mut db,
        "MATCH ()-[r:KNOWS]->() WITH collect(r) AS rels UNWIND rels AS f \
         MATCH (f)-[:KNOWS]->() RETURN f",
    );
    assert!(
        message.contains("MATCH variable 'f' must be bound to a node, found a map"),
        "{message}"
    );
}

#[test]
fn empty_lists_produce_no_rows() {
    let mut db = social();
    assert!(rows(&mut db, "UNWIND [] AS f MATCH (f)-[:KNOWS]->() RETURN f").is_empty());
    assert!(rows(
        &mut db,
        "MATCH (p:Person {id: 99})-[:KNOWS]-(friend) \
         WITH collect(friend) AS friends UNWIND friends AS f \
         MATCH (f)<-[:HAS_CREATOR]-(m) RETURN f.id AS id, m.id AS message"
    )
    .is_empty());
}

/// A hand-written map is not trusted as a node: the import re-reads `_id`
/// from the snapshot, so the stored labels decide the pattern and an id that
/// is not visible fails instead of matching.
#[test]
fn hand_written_node_maps_use_the_stored_node() {
    let mut db = social();
    let id = match db
        .query("MATCH (p:Person {id: 2}) RETURN id(p) AS n")
        .unwrap()
        .rows[0]["n"]
    {
        Value::Int(id) => id,
        ref other => panic!("{other:?}"),
    };
    // The map claims `Robot`; the stored node is a Person with messages 10, 11.
    let forged = format!("UNWIND [{{_id: {id}, labels: ['Robot']}}] AS f");
    assert_eq!(
        rows(
            &mut db,
            &format!("{forged} MATCH (f:Person)<-[:HAS_CREATOR]-(m) RETURN m.id AS id")
        ),
        rows(
            &mut db,
            "MATCH (f:Person {id: 2})<-[:HAS_CREATOR]-(m) RETURN m.id AS id"
        )
    );
    // A label the stored node does not have filters it out.
    assert!(rows(
        &mut db,
        &format!("{forged} MATCH (f:Robot)<-[:HAS_CREATOR]-(m) RETURN m.id AS id")
    )
    .is_empty());
    // An id that is not in the snapshot fails rather than matching.
    let message = error(
        &mut db,
        "UNWIND [{_id: 987654321, labels: ['Person']}] AS f MATCH (f)<-[:HAS_CREATOR]-(m) RETURN m.id AS id",
    );
    assert!(message.contains("disappeared"), "{message}");
}

/// The plan below `WITH collect(..) AS xs UNWIND xs AS x` is still rewritten:
/// the start person is sought through its id index rather than found by
/// expanding every person and filtering afterwards (#1606, LDBC IC9). The
/// rows are those of the same query with the list materialized first.
#[test]
fn unwound_collect_keeps_the_start_index_seek() {
    let mut db = social();
    // Enough unrelated persons that the index seek is the cheaper start.
    let people = (100..400)
        .map(|id| Value::Map(BTreeMap::from([("id".to_string(), Value::Int(id))])))
        .collect::<Vec<_>>();
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Person {id: r.id})",
        &BTreeMap::from([("rows".to_string(), Value::List(people))]),
    )
    .unwrap();
    db.query("CREATE INDEX ON :Person(id)").unwrap();
    let unwound = "MATCH (root:Person {id: $id})-[:KNOWS*1..2]-(friend:Person) \
                   WHERE NOT friend = root \
                   WITH collect(DISTINCT friend) AS friends UNWIND friends AS friend \
                   MATCH (friend)<-[:HAS_CREATOR]-(message:Message) \
                   RETURN friend.id AS person, message.id AS message";
    let parameters = BTreeMap::from([("id".to_string(), Value::Int(1))]);
    let physical_plan = db
        .explain_query_with_params(unwound, &parameters)
        .unwrap()
        .physical_plan
        .explain(0);
    assert!(physical_plan.contains("IndexNodeSeek"), "{physical_plan}");
    assert!(!physical_plan.contains("SeqNodeScan"), "{physical_plan}");
    let direct = "MATCH (root:Person {id: $id})-[:KNOWS*1..2]-(friend:Person) \
                  WHERE NOT friend = root WITH DISTINCT friend \
                  MATCH (friend)<-[:HAS_CREATOR]-(message:Message) \
                  RETURN friend.id AS person, message.id AS message";
    let expected = rows(&mut db, direct);
    assert_eq!(expected.len(), 4, "{expected:?}");
    assert_eq!(rows(&mut db, unwound), expected);
}
