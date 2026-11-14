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

//! Relationship mutations whose pattern leaves an endpoint unlabeled must
//! write what the same pattern reads (#1552): an unlabeled pattern node
//! matches any node, while a label the catalog does not know matches none.

use super::*;

fn count(db: &mut Database, query: &str) -> Value {
    db.query(query).unwrap().rows[0]["n"].clone()
}

/// `(:A {id: 1})-[:R {k: 4}]->(:B {id: 2})`.
fn graph() -> Database {
    let mut db = Database::new();
    db.query("CREATE (:A {id: 1})").unwrap();
    db.query("CREATE (:B {id: 2})").unwrap();
    db.query("MATCH (a:A {id: 1}), (b:B {id: 2}) CREATE (a)-[:R {k: 4}]->(b)")
        .unwrap();
    db
}

/// Runs `write` in autocommit and in an explicit transaction, each on a fresh
/// graph, then evaluates `check` after the commit.
fn assert_write(write: &str, check: &str, expected: i64) {
    for in_transaction in [false, true] {
        let mut db = graph();
        if in_transaction {
            let mut transaction = db.begin_transaction().unwrap();
            transaction.query(write).unwrap();
            transaction.commit().unwrap();
        } else {
            db.query(write).unwrap();
        }
        assert_eq!(
            count(&mut db, check),
            Value::Int(expected),
            "{write} (transaction: {in_transaction})"
        );
    }
}

const RELATIONSHIPS: &str = "MATCH ()-[r:R]->() RETURN count(r) AS n";
const NODES: &str = "MATCH (n) RETURN count(n) AS n";
const COPIES: &str = "MATCH ()-[r:S]->() RETURN count(r) AS n";

#[test]
fn relationship_delete_matches_unlabeled_endpoints() {
    for write in [
        "MATCH ()-[r:R {k: 4}]->() DELETE r",
        "MATCH (:A)-[r:R {k: 4}]->() DELETE r",
        "MATCH ()-[r:R {k: 4}]->(:B) DELETE r",
        "MATCH (a {id: 1})-[r:R]->() DELETE r",
        "MATCH ()-[r:R]->(b {id: 2}) DELETE r",
        "MATCH (:A)-[r:R {k: 4}]->(:B) DELETE r",
    ] {
        assert_write(write, RELATIONSHIPS, 0);
    }
    // A named label that does not exist, or a filter that matches no node,
    // still matches nothing.
    for write in [
        "MATCH (:Missing)-[r:R]->() DELETE r",
        "MATCH ()-[r:R]->(:Missing) DELETE r",
        "MATCH (a {id: 3})-[r:R]->() DELETE r",
        "MATCH ()-[r:R {k: 5}]->() DELETE r",
    ] {
        assert_write(write, RELATIONSHIPS, 1);
    }
}

#[test]
fn relationship_set_matches_unlabeled_endpoints() {
    let check = "MATCH ()-[r:R {k: 9}]->() RETURN count(r) AS n";
    for write in [
        "MATCH ()-[r:R]->() SET r.k = 9",
        "MATCH (:A)-[r:R]->() SET r.k = 9",
        "MATCH ()-[r:R]->(:B) SET r.k = 9",
        "MATCH ()-[r:R]->() SET r.k = 9, r.seen = true",
    ] {
        assert_write(write, check, 1);
    }
    assert_write("MATCH (:Missing)-[r:R]->() SET r.k = 9", check, 0);
}

#[test]
fn detach_delete_of_relationship_targets_matches_unlabeled_endpoints() {
    for write in [
        "MATCH ()-[:R]->(t) DETACH DELETE t",
        "MATCH (:A)-[:R]->(t) DETACH DELETE t",
        "MATCH (s {id: 1})-[:R]->(t) DETACH DELETE t",
    ] {
        assert_write(write, NODES, 1);
    }
    assert_write("MATCH (:Missing)-[:R]->(t) DETACH DELETE t", NODES, 2);
}

#[test]
fn relationship_retarget_merge_matches_unlabeled_endpoints() {
    for write in [
        // The old target, the new target or the source left unlabeled.
        "MATCH (s:A)-[:R]->(o), (n:B {id: 2}) MERGE (s)-[:S]->(n)",
        "MATCH (s:A)-[:R]->(o:B), (n {id: 2}) MERGE (s)-[:S]->(n)",
        "MATCH (s)-[:R]->(o:B), (n:B {id: 2}) MERGE (s)-[:S]->(n)",
        // From the matched target: the old source or the old target unlabeled.
        "MATCH (o)-[:R]->(t:B), (n:A {id: 1}) MERGE (n)-[:S]->(t)",
        "MATCH (o:A)-[:R]->(t), (n:A {id: 1}) MERGE (n)-[:S]->(t)",
    ] {
        assert_write(write, COPIES, 1);
    }
    assert_write(
        "MATCH (s:A)-[:R]->(o:Missing), (n {id: 2}) MERGE (s)-[:S]->(n)",
        COPIES,
        0,
    );
}

/// Relationships created earlier in the same transaction are matched too.
#[test]
fn same_transaction_relationships_match_unlabeled_endpoints() {
    let mut db = graph();
    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .query("MATCH (a:A {id: 1}), (b:B {id: 2}) CREATE (a)-[:R {k: 6}]->(b)")
        .unwrap();
    transaction
        .query("MATCH ()-[r:R {k: 6}]->() SET r.k = 60")
        .unwrap();
    transaction
        .query("MATCH ()-[r:R {k: 4}]->() DELETE r")
        .unwrap();
    transaction.commit().unwrap();
    assert_eq!(count(&mut db, RELATIONSHIPS), Value::Int(1));
    assert_eq!(
        count(&mut db, "MATCH ()-[r:R {k: 60}]->() RETURN count(r) AS n"),
        Value::Int(1)
    );

    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .query("MATCH ()-[r:R {k: 60}]->() DELETE r")
        .unwrap();
    transaction.commit().unwrap();
    assert_eq!(count(&mut db, RELATIONSHIPS), Value::Int(0));
}

/// An unconstrained source is not collected as a node set, so nodes that are
/// never mutated are not charged to `max_mutation_affected_rows`.
#[test]
fn unconstrained_source_is_not_charged_to_the_affected_row_limit() {
    let mut config = DatabaseConfig::default();
    config.mutation_limits.max_affected_rows = std::num::NonZeroUsize::new(2).unwrap();
    let mut db = Database::new_with_config(config);
    for id in 0..8 {
        db.query(&format!("CREATE (:A {{id: {id}}})")).unwrap();
    }
    db.query("MATCH (a:A {id: 0}), (b:A {id: 1}) CREATE (a)-[:R {k: 4}]->(b)")
        .unwrap();
    db.query("MATCH ()-[r:R {k: 4}]->() SET r.k = 5").unwrap();
    db.query("MATCH ()-[r:R {k: 5}]->() DELETE r").unwrap();
    assert_eq!(count(&mut db, RELATIONSHIPS), Value::Int(0));
}
