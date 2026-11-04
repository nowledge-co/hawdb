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

//! Unbounded variable-length patterns (`*`, `*n..`) enumerate every trail
//! (#1489). No trail repeats a relationship, so cycles terminate; a trail
//! longer than the executor's depth limit is refused, never truncated.

use super::*;

fn ids(db: &mut Database, query: &str) -> Vec<i64> {
    let mut ids = db
        .query(query)
        .unwrap()
        .rows
        .iter()
        .map(|row| match row["id"] {
            Value::Int(id) => id,
            ref other => panic!("{query}: {other:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

/// `(:N {id: 0})-[:R]->(:N {id: 1})-> ... -> (:N {id: length})`.
fn chain(length: i64) -> Database {
    let mut db = Database::new();
    for id in 0..=length {
        db.query(&format!("CREATE (:N {{id: {id}}})")).unwrap();
    }
    for id in 0..length {
        db.query(&format!(
            "MATCH (a:N {{id: {id}}}), (b:N {{id: {}}}) CREATE (a)-[:R]->(b)",
            id + 1
        ))
        .unwrap();
    }
    db
}

#[test]
fn unbounded_patterns_reach_every_hop_of_a_chain() {
    let mut db = chain(5);
    assert_eq!(
        ids(&mut db, "MATCH (:N {id: 0})-[:R*]->(b) RETURN b.id AS id"),
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(
        ids(
            &mut db,
            "MATCH (:N {id: 0})-[:R*0..]->(b) RETURN b.id AS id"
        ),
        vec![0, 1, 2, 3, 4, 5]
    );
    assert_eq!(
        ids(
            &mut db,
            "MATCH (:N {id: 0})-[:R*3..]->(b) RETURN b.id AS id"
        ),
        vec![3, 4, 5]
    );
    assert_eq!(
        ids(&mut db, "MATCH (:N {id: 5})<-[:R*]-(b) RETURN b.id AS id"),
        vec![0, 1, 2, 3, 4]
    );
    assert_eq!(
        ids(&mut db, "MATCH (:N {id: 2})-[:R*]-(b) RETURN b.id AS id"),
        vec![0, 1, 3, 4, 5]
    );
    // The same as the bounded pattern that covers every relationship.
    assert_eq!(
        ids(&mut db, "MATCH (a:N)-[:R*]->(b) RETURN b.id AS id"),
        ids(&mut db, "MATCH (a:N)-[:R*1..5]->(b) RETURN b.id AS id")
    );
}

#[test]
fn unbounded_trails_terminate_on_cycles_and_self_loops() {
    let mut db = chain(2);
    db.query("MATCH (a:N {id: 2}), (b:N {id: 0}) CREATE (a)-[:R]->(b)")
        .unwrap();
    db.query("MATCH (a:N {id: 1}), (b:N {id: 1}) CREATE (a)-[:R]->(b)")
        .unwrap();
    // Every trail from 0 over the cycle 0 -> 1 -> 2 -> 0 and the loop on 1.
    let unbounded = ids(&mut db, "MATCH (:N {id: 0})-[:R*]->(b) RETURN b.id AS id");
    let covering = ids(
        &mut db,
        "MATCH (:N {id: 0})-[:R*1..4]->(b) RETURN b.id AS id",
    );
    assert_eq!(unbounded, covering);
    assert_eq!(unbounded, vec![0, 0, 1, 1, 2, 2]);
    // Undirected trails over the same cycle also terminate.
    let undirected = ids(&mut db, "MATCH (:N {id: 0})-[:R*]-(b) RETURN b.id AS id");
    assert_eq!(
        undirected,
        ids(
            &mut db,
            "MATCH (:N {id: 0})-[:R*1..4]-(b) RETURN b.id AS id"
        )
    );
}

#[test]
fn a_trail_beyond_the_depth_limit_is_refused_not_truncated() {
    // 256 relationships: the longest trail is exactly at the limit.
    let mut db = chain(256);
    assert_eq!(
        ids(&mut db, "MATCH (:N {id: 0})-[:R*]->(b) RETURN b.id AS id").len(),
        256
    );
    // One more relationship makes a 257-hop trail.
    db.query("CREATE (:N {id: 257})").unwrap();
    db.query("MATCH (a:N {id: 256}), (b:N {id: 257}) CREATE (a)-[:R]->(b)")
        .unwrap();
    let error = db
        .query("MATCH (:N {id: 0})-[:R*]->(b) RETURN b.id AS id")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("trail longer than 256 relationships"),
        "{error}"
    );
    // A bounded pattern within the limit is unaffected.
    assert_eq!(
        ids(
            &mut db,
            "MATCH (:N {id: 0})-[:R*1..3]->(b) RETURN b.id AS id"
        ),
        vec![1, 2, 3]
    );
}

#[test]
fn explain_prints_an_open_upper_bound() {
    let mut db = chain(2);
    let plan = db
        .query("EXPLAIN MATCH (a:N {id: 0})-[:R*2..]->(b) RETURN b.id AS id")
        .unwrap();
    let text = format!("{plan:?}");
    assert!(text.contains("hops=2.. "), "{text}");
}

#[test]
fn unbounded_patterns_join_a_second_pattern() {
    let mut db = chain(4);
    // A second pattern sharing an endpoint.
    assert_eq!(
        ids(
            &mut db,
            "MATCH (:N {id: 0})-[:R*]->(b), (b)-[:R]->(c) RETURN c.id AS id"
        ),
        vec![2, 3, 4]
    );
}
