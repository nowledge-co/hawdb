// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Pattern predicates, pattern comprehensions and list comprehension WHERE
//! end to end (#1488), with LDBC SNB IC7, IC10 and IC14 fragments.

use super::*;
use std::num::NonZeroUsize;

/// A small LDBC-SNB-shaped social graph.
///
/// Persons 1-5; KNOWS 1-2, 2-3, 1-4, 3-5. Tags 100 (`t100`), 101, 102.
/// Interests: 1 -> 100, 2 -> 101, 3 -> 100, 3 -> 102.
/// Posts 10 (by 2, tags 100, 101), 11 (by 2, tag 102), 12 (by 3, no tags),
/// 13 (by 4, tag 100). Comments 20 (by 1, replies to 10), 21 (by 3,
/// replies to 10), 22 (by 2, replies to 20).
/// Likes: 2 -> 13, 3 -> 10, 5 -> 10, 4 -> 12.
fn graph() -> Database {
    let mut db = Database::new();
    for id in 1..=5 {
        db.query(&format!(
            "CREATE (:Person {{id: {id}, firstName: 'p{id}'}})"
        ))
        .unwrap();
    }
    for id in [100, 101, 102] {
        db.query(&format!("CREATE (:Tag {{id: {id}, name: 't{id}'}})"))
            .unwrap();
    }
    for (a, b) in [(1, 2), (2, 3), (1, 4), (3, 5)] {
        db.query(&format!(
            "MATCH (a:Person {{id: {a}}}), (b:Person {{id: {b}}}) CREATE (a)-[:KNOWS]->(b)"
        ))
        .unwrap();
    }
    for (person, tag) in [(1, 100), (2, 101), (3, 100), (3, 102)] {
        db.query(&format!(
            "MATCH (p:Person {{id: {person}}}), (t:Tag {{id: {tag}}}) CREATE (p)-[:HAS_INTEREST]->(t)"
        ))
        .unwrap();
    }
    for (post, creator, date) in [(10, 2, 1000), (11, 2, 2000), (12, 3, 3000), (13, 4, 4000)] {
        db.query(&format!(
            "CREATE (:Post {{id: {post}, creationDate: {date}}})"
        ))
        .unwrap();
        db.query(&format!(
            "MATCH (m:Post {{id: {post}}}), (p:Person {{id: {creator}}}) CREATE (m)-[:HAS_CREATOR]->(p)"
        ))
        .unwrap();
    }
    for (post, tag) in [(10, 100), (10, 101), (11, 102), (13, 100)] {
        db.query(&format!(
            "MATCH (m:Post {{id: {post}}}), (t:Tag {{id: {tag}}}) CREATE (m)-[:HAS_TAG]->(t)"
        ))
        .unwrap();
    }
    for (comment, creator, parent) in [(20, 1, 10), (21, 3, 10), (22, 2, 20)] {
        db.query(&format!(
            "CREATE (:Comment {{id: {comment}, creationDate: {}}})",
            5000 + comment
        ))
        .unwrap();
        db.query(&format!(
            "MATCH (c:Comment {{id: {comment}}}), (p:Person {{id: {creator}}}) CREATE (c)-[:HAS_CREATOR]->(p)"
        ))
        .unwrap();
        db.query(&format!(
            "MATCH (c:Comment {{id: {comment}}}), (m {{id: {parent}}}) CREATE (c)-[:REPLY_OF]->(m)"
        ))
        .unwrap();
    }
    for (person, message) in [(2, 13), (3, 10), (5, 10), (4, 12)] {
        db.query(&format!(
            "MATCH (p:Person {{id: {person}}}), (m {{id: {message}}}) CREATE (p)-[:LIKES]->(m)"
        ))
        .unwrap();
    }
    db
}

fn rows(db: &mut Database, query: &str) -> Vec<Vec<Value>> {
    let output = db
        .query(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    (0..output.rows.len())
        .map(|row| output.rows[row].values().cloned().collect())
        .collect()
}

fn error(db: &mut Database, query: &str) -> String {
    db.query(query)
        .err()
        .unwrap_or_else(|| panic!("{query} should fail"))
        .to_string()
}

fn ints(values: &[i64]) -> Value {
    Value::List(values.iter().copied().map(Value::Int).collect())
}

fn sorted(value: &Value) -> Value {
    let Value::List(values) = value else {
        return value.clone();
    };
    let mut values = values.clone();
    values.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
    Value::List(values)
}

/// Asserts both queries return the same rows, comparing list cells as
/// multisets: a pattern comprehension's element order is unspecified.
fn assert_same_rows(db: &mut Database, actual: &str, expected: &str) {
    let normalize = |rows: Vec<Vec<Value>>| {
        rows.into_iter()
            .map(|row| row.iter().map(sorted).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    let actual_rows = normalize(rows(db, actual));
    assert!(!actual_rows.is_empty(), "{actual}");
    assert_eq!(
        actual_rows,
        normalize(rows(db, expected)),
        "{actual}\nvs\n{expected}"
    );
}

#[test]
fn ic7_pattern_predicate_in_return_matches_exists_subquery() {
    let mut db = graph();
    // IC7: the likers of a person's messages, and whether each is new.
    let ic7 = "MATCH (person:Person {id: 2})<-[:HAS_CREATOR]-(message)<-[:LIKES]-(liker:Person) \
               RETURN liker.id AS liker, message.id AS message, \
               not((liker)-[:KNOWS]-(person)) AS isNew ORDER BY liker";
    assert_eq!(
        rows(&mut db, ic7),
        [
            vec![Value::Int(3), Value::Int(10), Value::Bool(false)],
            vec![Value::Int(5), Value::Int(10), Value::Bool(true)],
        ]
    );
    for (predicate, expected) in [
        (
            "(liker)-[:KNOWS]-(person)",
            "EXISTS { MATCH (liker)-[:KNOWS]-(person) }",
        ),
        (
            "NOT (liker)-[:KNOWS]-(person)",
            "NOT EXISTS { MATCH (liker)-[:KNOWS]-(person) }",
        ),
    ] {
        let query = |predicate: &str| {
            format!(
                "MATCH (person:Person {{id: 2}})<-[:HAS_CREATOR]-(message)<-[:LIKES]-(liker:Person) \
                 WHERE {predicate} RETURN liker.id AS liker ORDER BY liker"
            )
        };
        assert_same_rows(&mut db, &query(predicate), &query(expected));
    }
    // The same predicate projected from WITH, combined with other booleans.
    assert_eq!(
        rows(
            &mut db,
            "MATCH (a:Person {id: 1}), (b:Person) WITH b, NOT ((a)-[:KNOWS]-(b)) AS stranger \
             RETURN b.id AS id, not(stranger) AS knows, \
             not(not(stranger) OR b.id <= 2) AS far ORDER BY id"
        ),
        [1, 2, 3, 4, 5].map(|id| vec![
            Value::Int(id),
            Value::Bool(matches!(id, 2 | 4)),
            Value::Bool(!matches!(id, 2 | 4) && id > 2),
        ])
    );
}

#[test]
fn ic10_pattern_predicate_correlates_with_a_list_comprehension_element() {
    let mut db = graph();
    // IC10: per friend, the posts and the posts sharing a tag the person
    // is interested in.
    let ic10 = "MATCH (person:Person {id: 1}), (friend:Person) \
                OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post) \
                WITH friend, collect(post) AS posts, person \
                WITH friend, size(posts) AS postCount, \
                     size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]) AS commonPostCount \
                RETURN friend.id AS id, postCount, commonPostCount ORDER BY id";
    assert_eq!(
        rows(&mut db, ic10),
        [(1, 0, 0), (2, 2, 1), (3, 1, 0), (4, 1, 1), (5, 0, 0)].map(|(id, posts, common)| vec![
            Value::Int(id),
            Value::Int(posts),
            Value::Int(common)
        ])
    );
    assert_same_rows(
        &mut db,
        "MATCH (person:Person {id: 1}), (friend:Person) \
         OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post) \
         WITH friend, collect(post) AS posts, person \
         RETURN friend.id AS id, [p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person) | p.id] AS common \
         ORDER BY id",
        "MATCH (person:Person {id: 1}), (friend:Person) \
         OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person) \
         RETURN friend.id AS id, collect(DISTINCT post.id) AS common ORDER BY id",
    );
    // IC10's friend filter: a bound target is part of the pattern.
    assert_same_rows(
        &mut db,
        "MATCH (person:Person {id: 1})-[:KNOWS*2..2]-(friend:Person) \
         WHERE NOT friend = person AND NOT (friend)-[:KNOWS]-(person) \
         RETURN DISTINCT friend.id AS id ORDER BY id",
        "MATCH (person:Person {id: 1})-[:KNOWS*2..2]-(friend:Person) \
         WHERE NOT friend = person AND NOT EXISTS { MATCH (friend)-[:KNOWS]-(person) } \
         RETURN DISTINCT friend.id AS id ORDER BY id",
    );
}

#[test]
fn ic14_pattern_comprehension_inside_reduce_over_path_relationships() {
    let mut db = graph();
    // IC14: weight each KNOWS hop of the shortest path by replies between
    // its endpoints, in either direction.
    let weights = rows(
        &mut db,
        "MATCH path = shortestPath((p1:Person {id: 1})-[:KNOWS*..4]-(p2:Person {id: 5})) \
         WITH path, relationships(path) AS rels \
         RETURN [n IN nodes(path) | n.id] AS ids, \
         [r IN rels | reduce(w = 0.0, v IN [ \
             (a:Person)<-[:HAS_CREATOR]-(:Post)<-[:REPLY_OF]-(:Comment)-[:HAS_CREATOR]->(b:Person) \
             WHERE (a.id = startNode(r).id AND b.id = endNode(r).id) \
                OR (a.id = endNode(r).id AND b.id = startNode(r).id) | 1.0] | w + v)] AS weights",
    );
    assert_eq!(
        weights,
        [vec![
            ints(&[1, 2, 3, 5]),
            Value::List(vec![
                Value::Float(1.0),
                Value::Float(1.0),
                Value::Float(0.0)
            ]),
        ]]
    );
    // Each hop's weight equals a MATCH count over its stored endpoints.
    for (a, b, expected) in [(1, 2, 1), (2, 3, 1), (3, 5, 0)] {
        let count = rows(
            &mut db,
            &format!(
                "MATCH (a:Person)<-[:HAS_CREATOR]-(:Post)<-[:REPLY_OF]-(:Comment)-[:HAS_CREATOR]->(b:Person) \
                 WHERE (a.id = {a} AND b.id = {b}) OR (a.id = {b} AND b.id = {a}) RETURN count(*) AS n"
            ),
        );
        assert_eq!(count, [vec![Value::Int(expected)]], "{a}-{b}");
    }
}

#[test]
fn relationship_values_resolve_their_stored_endpoints() {
    let mut db = graph();
    let path = "MATCH p = shortestPath((a:Person {id: 1})-[:KNOWS*..4]-(b:Person {id: 5}))";
    assert_eq!(
        rows(
            &mut db,
            &format!(
                "{path} RETURN [r IN relationships(p) | startNode(r).id] AS s, \
                 [r IN relationships(p) | endNode(r).firstName] AS e"
            )
        ),
        [vec![
            ints(&[1, 2, 3]),
            Value::List(
                ["p2", "p3", "p5"]
                    .map(|name| Value::String(name.into()))
                    .to_vec()
            ),
        ]]
    );
    // A relationship carried through WITH is a value, too; its endpoints are
    // the stored ones whichever direction the pattern matched.
    assert_same_rows(
        &mut db,
        "MATCH (a:Person {id: 2})-[r:KNOWS]-(b) WITH r \
         RETURN startNode(r).id AS s, endNode(r).id AS e ORDER BY s, e",
        "MATCH (s:Person)-[:KNOWS]->(e:Person) WHERE s.id = 2 OR e.id = 2 \
         RETURN s.id AS s, e.id AS e ORDER BY s, e",
    );
    assert_eq!(
        rows(&mut db, "UNWIND [null] AS r RETURN startNode(r) AS s"),
        [vec![Value::Null]]
    );
    assert!(error(&mut db, "UNWIND [1] AS r RETURN startNode(r) AS s")
        .contains("startNode() requires a relationship"),);
}

#[test]
fn pattern_comprehensions_match_optional_match_and_collect() {
    let mut db = graph();
    assert_same_rows(
        &mut db,
        "MATCH (a:Person) RETURN a.id AS id, [(a)-[:KNOWS]-(b) | b.id] AS friends ORDER BY id",
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]-(b) \
         RETURN a.id AS id, collect(b.id) AS friends ORDER BY id",
    );
    // A predicate over introduced and correlated variables, new
    // relationship variables and labels on introduced nodes.
    assert_same_rows(
        &mut db,
        "MATCH (a:Person) RETURN a.id AS id, \
         [(a)<-[c:HAS_CREATOR]-(m:Post) WHERE m.creationDate > 1500 | [type(c), m.id]] AS posts \
         ORDER BY id",
        "MATCH (a:Person) OPTIONAL MATCH (a)<-[c:HAS_CREATOR]-(m:Post) WHERE m.creationDate > 1500 \
         RETURN a.id AS id, collect(CASE WHEN m IS NULL THEN null ELSE [type(c), m.id] END) AS posts \
         ORDER BY id",
    );
    // Relationship uniqueness applies within the pattern, as in MATCH.
    assert_same_rows(
        &mut db,
        "MATCH (a:Person {id: 1}) RETURN [(a)-[:KNOWS]-(b)-[:KNOWS]-(c) | c.id] AS reach",
        "MATCH (a:Person {id: 1})-[:KNOWS]-(b)-[:KNOWS]-(c) RETURN collect(c.id) AS reach",
    );
    // A bounded variable-length pattern runs as in MATCH.
    assert_same_rows(
        &mut db,
        "MATCH (a:Person {id: 1}) RETURN [(a)-[:KNOWS*1..2]-(b) | b.id] AS reach",
        "MATCH (a:Person {id: 1})-[:KNOWS*1..2]-(b) RETURN collect(b.id) AS reach",
    );
    // A pattern starting at an unbound node scans from its first bound node.
    assert_same_rows(
        &mut db,
        "MATCH (t:Tag {id: 100}) RETURN [(p:Person)-[:HAS_INTEREST]->(t) | p.id] AS fans",
        "MATCH (p:Person)-[:HAS_INTEREST]->(t:Tag {id: 100}) RETURN collect(p.id) AS fans",
    );
}

#[test]
fn pattern_expressions_nest_and_compose_with_list_functions() {
    let mut db = graph();
    assert_eq!(
        rows(
            &mut db,
            "MATCH (a:Person {id: 1}) RETURN size([(a)-[:KNOWS]-(b) | b]) AS degree, \
             head([(a)-[:KNOWS]-(b) WHERE b.id > 3 | b.id]) AS first, \
             reduce(s = 0, id IN [(a)-[:KNOWS]-(b) | b.id] | s + id) AS total, \
             [id IN [(a)-[:KNOWS]-(b) | b.id] WHERE id > 2 | id * 10] AS scaled, \
             [(a)-[:KNOWS]-(b) WHERE b.id = 2 | [(b)-[:KNOWS]-(c) WHERE c <> a | c.id]] AS nested"
        ),
        [vec![
            Value::Int(2),
            Value::Int(4),
            Value::Int(6),
            ints(&[40]),
            Value::List(vec![ints(&[3])]),
        ]]
    );
    // A pattern correlated with an enclosing comprehension's element and
    // with an outer column read inside its WHERE.
    assert_eq!(
        rows(
            &mut db,
            "MATCH (a:Person) WITH collect(a) AS people, 2 AS k \
             RETURN [p IN people WHERE size([(p)-[:KNOWS]-(q) WHERE q.id > k | q]) > 0 | p.id] AS ids"
        ),
        [vec![ints(&[1, 2, 3, 5])]]
    );
}

#[test]
fn pattern_expressions_in_with_where_and_order_by() {
    let mut db = graph();
    // WITH projects it; WITH ... WHERE and MATCH ... WHERE filter on it.
    assert_eq!(
        rows(
            &mut db,
            "MATCH (a:Person) WITH a, [(a)-[:KNOWS]-(b) | b.id] AS friends WHERE size(friends) > 1 \
             RETURN a.id AS id ORDER BY id"
        ),
        [1, 2, 3].map(|id| vec![Value::Int(id)])
    );
    assert_same_rows(
        &mut db,
        "MATCH (a:Person) WHERE size([(a)-[:KNOWS]-(b) WHERE b.id > a.id | b]) > 0 \
         RETURN a.id AS id ORDER BY id",
        "MATCH (a:Person)-[:KNOWS]-(b) WHERE b.id > a.id RETURN DISTINCT a.id AS id ORDER BY id",
    );
    assert_same_rows(
        &mut db,
        "MATCH (a:Person) WHERE size([(a)-[:KNOWS]-() | 1]) = 1 RETURN a.id AS id ORDER BY id",
        "MATCH (a:Person)-[:KNOWS]-() WITH a, count(*) AS n WHERE n = 1 RETURN a.id AS id ORDER BY id",
    );
    assert_same_rows(
        &mut db,
        "MATCH (a:Person) WHERE (a)-[:KNOWS]->()-[:KNOWS]->() RETURN a.id AS id ORDER BY id",
        "MATCH (a:Person)-[:KNOWS]->()-[:KNOWS]->() RETURN DISTINCT a.id AS id ORDER BY id",
    );
    // ORDER BY an alias, an expression over it and a direct pattern
    // expression, with and without LIMIT.
    let expected = [(1, 2), (2, 2), (3, 2), (4, 1), (5, 1)]
        .map(|(id, degree)| vec![Value::Int(id), Value::Int(degree)]);
    for query in [
        "MATCH (a:Person) RETURN a.id AS id, size([(a)-[:KNOWS]-(b) | b]) AS degree \
         ORDER BY degree DESC, id",
        "MATCH (a:Person) RETURN a.id AS id, size([(a)-[:KNOWS]-(b) | b]) AS degree \
         ORDER BY size([(a)-[:KNOWS]-(b) | b]) DESC, id",
        "MATCH (a:Person) WITH a RETURN a.id AS id, size([(a)-[:KNOWS]-(b) | b]) AS degree \
         ORDER BY size([(a)-[:KNOWS]-(b) | b.id]) DESC, id",
    ] {
        assert_eq!(rows(&mut db, query), expected, "{query}");
        assert_eq!(
            rows(&mut db, &format!("{query} LIMIT 2")),
            expected[..2],
            "{query}"
        );
    }
    // Aggregation groups by a pattern expression and aggregates over one.
    assert_eq!(
        rows(
            &mut db,
            "MATCH (a:Person) RETURN size([(a)-[:KNOWS]-(b) | b]) AS degree, count(*) AS n, \
             sum(size([(a)<-[:HAS_CREATOR]-(m) | m])) AS messages ORDER BY degree"
        ),
        [
            vec![Value::Int(1), Value::Int(2), Value::Int(1)],
            vec![Value::Int(2), Value::Int(3), Value::Int(6)],
        ]
    );
}

#[test]
fn null_bound_variables_match_nothing() {
    let mut db = graph();
    // Person 1 likes nothing: the comprehension is empty, not NULL, and the
    // predicate is false, as an EXISTS subquery over a NULL node.
    let normalized = rows(
        &mut db,
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:LIKES]->(m:Post) \
         RETURN a.id AS id, [(m)-[:HAS_TAG]->(t) | t.id] AS tags, \
         NOT ((m)-[:HAS_TAG]->()) AS untagged ORDER BY id",
    )
    .into_iter()
    .map(|row| row.iter().map(sorted).collect::<Vec<_>>())
    .collect::<Vec<_>>();
    assert_eq!(
        normalized,
        [
            vec![Value::Int(1), ints(&[]), Value::Bool(true)],
            vec![Value::Int(2), ints(&[100]), Value::Bool(false)],
            vec![
                Value::Int(3),
                sorted(&ints(&[100, 101])),
                Value::Bool(false)
            ],
            vec![Value::Int(4), ints(&[]), Value::Bool(true)],
            vec![
                Value::Int(5),
                sorted(&ints(&[100, 101])),
                Value::Bool(false)
            ],
        ]
    );
    // Both spellings of `NOT pattern` over the NULL `m` keep person 1, as
    // `NOT EXISTS { MATCH ... }` does.
    for query in [
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:LIKES]->(m:Post) WITH a, m \
         WHERE NOT (m)-[:HAS_TAG]->() RETURN a.id AS id ORDER BY id",
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:LIKES]->(m:Post) WITH a, m \
         WHERE NOT (m)-[:HAS_TAG]->(:Tag) RETURN a.id AS id ORDER BY id",
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:LIKES]->(m:Post) WITH a, m \
         WHERE size([(m)-[:HAS_TAG]->(t) | t]) = 0 RETURN a.id AS id ORDER BY id",
    ] {
        assert_eq!(
            rows(&mut db, query),
            [vec![Value::Int(1)], vec![Value::Int(4)]],
            "{query}"
        );
    }
}

#[test]
fn list_comprehension_where_filters_and_projects() {
    let mut db = graph();
    assert_eq!(
        rows(
            &mut db,
            "UNWIND [1] AS one RETURN [x IN [1, null, 3, 4] WHERE x > 1] AS kept, \
             [x IN [1, 2, 3] WHERE x <> 2 | x * one * 10] AS mapped, \
             [x IN [] WHERE x > 0] AS empty"
        ),
        [vec![ints(&[3, 4]), ints(&[10, 30]), ints(&[])]]
    );
}

#[test]
fn parameters_rebind_pattern_properties_in_cached_plans() {
    let mut db = graph();
    let query =
        "MATCH (a:Person {id: $id}) RETURN [(a)-[:KNOWS]-(b:Person {id: $other}) | b.id] AS ids";
    for (id, other, expected) in [
        (1, 2, vec![2]),
        (1, 4, vec![4]),
        (2, 4, vec![]),
        (2, 3, vec![3]),
    ] {
        let output = db
            .query_with_params(
                query,
                &BTreeMap::from([
                    ("id".to_string(), Value::Int(id)),
                    ("other".to_string(), Value::Int(other)),
                ]),
            )
            .unwrap();
        assert_eq!(
            output.rows[0].value(0),
            Some(&ints(&expected)),
            "{id} {other}"
        );
    }
}

#[test]
fn produced_lists_are_charged_to_the_query_memory_budget() {
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory: crate::executor::ExecutionMemoryConfig {
            query_memory_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            batch_payload_bytes: NonZeroUsize::new(64 * 1024).unwrap(),
            ..crate::executor::ExecutionMemoryConfig::default()
        },
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Hub {id: 1})").unwrap();
    db.query_with_params(
        "UNWIND $rows AS row CREATE (:Leaf {id: row.id})",
        &BTreeMap::from([(
            "rows".to_string(),
            Value::List(
                (1..=400)
                    .map(|id| Value::Map(BTreeMap::from([("id".to_string(), Value::Int(id))])))
                    .collect(),
            ),
        )]),
    )
    .unwrap();
    db.query("MATCH (h:Hub), (l:Leaf) CREATE (h)-[:R]->(l)")
        .unwrap();
    let small = rows(
        &mut db,
        "MATCH (h:Hub) RETURN size([(h)-[:R]->(l) | l.id]) AS n",
    );
    assert_eq!(small, [vec![Value::Int(400)]]);
    let error = error(
        &mut db,
        "MATCH (h:Hub) RETURN size([(h)-[:R]->(l) | [x IN range(1, 100) | [l.id, x]]]) AS n",
    );
    assert!(error.contains("query_memory_bytes"), "{error}");
}

#[test]
fn unsupported_pattern_expressions_fail_clearly() {
    let mut db = graph();
    for (query, message) in [
        // openCypher rejects a bare pattern as a projected value.
        ("MATCH (a:Person) RETURN (a)-[:KNOWS]-() AS x", "expected"),
        (
            "MATCH (a:Person) RETURN [(a)-[:KNOWS]-(b)] AS x",
            "a pattern is not a list element",
        ),
        (
            "MATCH (a:Person) RETURN [p = (a)-[:KNOWS]-(b) | p] AS x",
            "expected",
        ),
        (
            "MATCH (a:Person) RETURN [(a)-[:KNOWS*]-(b) | b] AS x",
            "unbounded variable-length relationships are not supported in pattern comprehensions",
        ),
        (
            "MATCH (a:Person)-[r:KNOWS]-() RETURN [(a)-[:KNOWS]-(r) | 1] AS x",
            "pattern variable 'r' must be bound to a node",
        ),
    ] {
        let error = error(&mut db, query);
        assert!(error.contains(message), "{query}: {error}");
    }
}

#[test]
fn prepared_source_reads_refuse_pattern_expressions() {
    // Their plan-derived read dependencies cannot see the subquery's reads.
    let db = graph();
    let limits = crate::PreparedReadLimits {
        dependency_entries: NonZeroUsize::new(32).unwrap(),
        dependency_bytes: NonZeroUsize::new(4096).unwrap(),
        statement_rows: NonZeroUsize::new(32).unwrap(),
        statement_payload_bytes: NonZeroUsize::new(4096).unwrap(),
    };
    for query in [
        "MATCH (a:Person) RETURN [(a)-[:KNOWS]-(b) | b.id] AS ids",
        "MATCH (a:Person) WHERE size([(a)-[:KNOWS]-(b) | b]) > 1 RETURN a.id AS id",
        "MATCH (a:Person) RETURN a.id AS id ORDER BY size([(a)-[:KNOWS]-(b) | b])",
    ] {
        let mut read = db.begin_prepared_read(limits).unwrap();
        let error = read
            .query_with_params(query, &BTreeMap::new())
            .err()
            .unwrap_or_else(|| panic!("{query} should refuse"))
            .to_string();
        assert!(
            error.contains("dependency collector") || error.contains("adjacency predicate"),
            "{query}: {error}"
        );
    }
}

#[test]
fn mutations_filter_on_pattern_predicates() {
    let mut db = graph();
    db.query("MATCH (a:Person) WHERE (a)-[:KNOWS]->(:Person {id: 3}) SET a.knowsThree = true")
        .unwrap();
    db.query("MATCH (a:Person) WHERE NOT (a)-[:KNOWS]->()-[:KNOWS]->() SET a.short = true")
        .unwrap();
    assert_eq!(
        rows(
            &mut db,
            "MATCH (a:Person) RETURN a.id AS id, a.knowsThree AS three, a.short AS short ORDER BY id"
        ),
        [
            vec![Value::Int(1), Value::Null, Value::Null],
            vec![Value::Int(2), Value::Bool(true), Value::Null],
            vec![Value::Int(3), Value::Null, Value::Bool(true)],
            vec![Value::Int(4), Value::Null, Value::Bool(true)],
            vec![Value::Int(5), Value::Null, Value::Bool(true)],
        ]
    );
}

#[test]
fn order_by_reads_the_projected_alias() {
    let mut db = graph();
    // `a` in ORDER BY is the alias for `b`: sort by `b`'s KNOWS out-degree.
    assert_eq!(
        rows(
            &mut db,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.id AS id, b AS a \
             ORDER BY size([(a)-[:KNOWS]->(x) | x]), id"
        )
        .into_iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>(),
        [4, 5, 2, 3].map(Value::Int)
    );
}

#[test]
fn one_hop_where_predicates_read_node_values_carried_by_with() {
    let mut db = graph();
    let nodes = rows(&mut db, "MATCH (n:Person) RETURN n ORDER BY n.id")
        .into_iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    let parameters = BTreeMap::from([("nodes".to_string(), Value::List(nodes))]);
    for query in [
        "UNWIND $nodes AS x WITH x WHERE (x)-[:KNOWS]->() RETURN x.id AS id ORDER BY id",
        "UNWIND $nodes AS x WITH x WHERE (x)-[:KNOWS]->(:Person) RETURN x.id AS id ORDER BY id",
        "UNWIND $nodes AS x WITH x WHERE size([(x)-[:KNOWS]->(y) | y]) > 0 RETURN x.id AS id ORDER BY id",
    ] {
        let output = db.query_with_params(query, &parameters).unwrap();
        let ids = output
            .rows
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>();
        assert_eq!(ids, [1, 2, 3].map(Value::Int), "{query}");
    }
}

#[test]
fn row_column_case_forms_read_inside_pattern_bodies() {
    let mut db = graph();
    assert_eq!(
        rows(
            &mut db,
            "MATCH (a:Person {id: 1}) WITH a, {title: ''} AS m \
             RETURN [(a)-[:KNOWS]-(b) | CASE WHEN m.title IS NULL OR m.title = '' THEN 'none' ELSE m.title END] AS x"
        ),
        [vec![Value::List(vec![Value::String("none".into()); 2])]]
    );
}

#[test]
fn relationship_values_must_name_stored_relationships() {
    let mut db = graph();
    let path = rows(
        &mut db,
        "MATCH p = shortestPath((a:Person {id: 1})-[:KNOWS*..4]-(b:Person {id: 3})) \
         RETURN relationships(p) AS rels, id(a) AS one, id(b) AS three",
    );
    let [rels, one, three] = [path[0][0].clone(), path[0][1].clone(), path[0][2].clone()];
    // A map with a relationship's keys but no stored relationship reads nothing.
    let forged = Value::Map(BTreeMap::from([
        ("_id".to_string(), Value::Int(999)),
        ("source_id".to_string(), three.clone()),
        ("target_id".to_string(), one),
        ("type".to_string(), Value::String("KNOWS".into())),
    ]));
    for value in [forged, Value::Map(BTreeMap::new())] {
        let error = db
            .query_with_params(
                "UNWIND [$r] AS r RETURN startNode(r) AS s",
                &BTreeMap::from([("r".to_string(), value)]),
            )
            .unwrap_err()
            .to_string();
        assert!(error.contains("names no stored relationship"), "{error}");
    }
    // A real relationship value passed back in still resolves.
    let parameters = BTreeMap::from([("rels".to_string(), rels)]);
    let output = db
        .query_with_params(
            "UNWIND [1] AS one RETURN [r IN $rels | startNode(r).id] AS s",
            &parameters,
        )
        .unwrap();
    assert_eq!(output.rows[0].value(0), Some(&ints(&[1, 2])));
    // After a DETACH DELETE the stale value no longer names a relationship,
    // as MATCH refuses a bound node that has disappeared.
    db.query("MATCH (n:Person {id: 2}) DETACH DELETE n")
        .unwrap();
    let error = db
        .query_with_params(
            "UNWIND [1] AS one RETURN [r IN $rels | startNode(r).id] AS s",
            &parameters,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("names no stored relationship"), "{error}");
    let _ = three;
}

#[test]
fn scanning_patterns_are_charged_per_visited_candidate() {
    let mut db = Database::new();
    db.query_with_params(
        "UNWIND $rows AS row CREATE (:Leaf {id: row.id})",
        &BTreeMap::from([(
            "rows".to_string(),
            Value::List(
                (1..=500)
                    .map(|id| Value::Map(BTreeMap::from([("id".to_string(), Value::Int(id))])))
                    .collect(),
            ),
        )]),
    )
    .unwrap();
    // No leaf has a relationship, so nothing matches, but every evaluation
    // scans all leaves.
    assert_eq!(
        rows(
            &mut db,
            "UNWIND [1] AS one RETURN size([(x:Leaf)-[:R]->() | x]) AS n"
        ),
        [vec![Value::Int(0)]]
    );
    let error = error(
        &mut db,
        "UNWIND [1] AS one RETURN [i IN range(1, 20000) | size([(x:Leaf)-[:R]->() | x])] AS n",
    );
    assert!(error.contains("work limit"), "{error}");
}

#[cfg(feature = "acl")]
mod access_control {
    use super::*;
    use crate::{QueryAccessControlContext, RuntimeCapabilities, RuntimeCapability};

    /// `(1)-[:KNOWS]->(2 hidden)-[:KNOWS]->(3)` and `(1)-[:KNOWS]->(4)`, with
    /// `space_id` 'public' except on node 2.
    fn graph() -> Database {
        let mut db = Database::new_with_config(DatabaseConfig {
            runtime_capabilities: RuntimeCapabilities::default()
                .with(RuntimeCapability::AccessControl, true),
            ..DatabaseConfig::default()
        });
        for (id, space) in [(1, "public"), (2, "hidden"), (3, "public"), (4, "public")] {
            db.query(&format!(
                "CREATE (:Person {{id: {id}, ssn: 'ssn{id}', space_id: '{space}'}})"
            ))
            .unwrap();
        }
        for (a, b) in [(1, 2), (2, 3), (1, 4)] {
            db.query(&format!(
                "MATCH (a:Person {{id: {a}}}), (b:Person {{id: {b}}}) CREATE (a)-[:KNOWS]->(b)"
            ))
            .unwrap();
        }
        db
    }

    fn scoped(
        db: &mut Database,
        query: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<Vec<Vec<Value>>> {
        let output = db.query_with_params_access_control(
            query,
            parameters,
            QueryAccessControlContext::visibility_scope(7, "space_id", "public"),
        )?;
        Ok((0..output.rows.len())
            .map(|row| output.rows[row].values().cloned().collect())
            .collect())
    }

    #[test]
    fn pattern_expressions_see_only_visible_nodes() {
        let mut db = graph();
        let none = BTreeMap::new();
        // A comprehension never projects a hidden node's property.
        assert_eq!(
            scoped(
                &mut db,
                "MATCH (a:Person {id: 1}) RETURN [(a)-[:KNOWS]->(b) | b.ssn] AS ssns",
                &none
            )
            .unwrap()
            .into_iter()
            .map(|row| sorted(&row[0]))
            .collect::<Vec<_>>(),
            [Value::List(vec![Value::String("ssn4".into())])]
        );
        // A predicate cannot probe a hidden node or its properties, in
        // WHERE, in a projection or nested in another pattern.
        for query in [
            "MATCH (a:Person) WHERE (a)-[:KNOWS]->(:Person {ssn: 'ssn2'}) RETURN a.id AS id",
            "MATCH (a:Person) WHERE (a)-[:KNOWS]->()-[:KNOWS]->(:Person {id: 3}) RETURN a.id AS id",
            "MATCH (a:Person) WITH a WHERE size([(a)-[:KNOWS]->(b) WHERE b.ssn = 'ssn2' | b]) > 0 RETURN a.id AS id",
            "MATCH (a:Person) WITH a, [(a)-[:KNOWS]->(b) | [(b)-[:KNOWS]->(c) | c.id]] AS reach \
             WHERE size(reach) > 0 AND size(reach[0]) > 0 RETURN a.id AS id",
        ] {
            assert_eq!(scoped(&mut db, query, &none).unwrap(), Vec::<Vec<Value>>::new(), "{query}");
        }
        assert_eq!(
            scoped(
                &mut db,
                "MATCH (a:Person) RETURN a.id AS id, NOT ((a)-[:KNOWS]->(:Person {ssn: 'ssn2'})) AS clean ORDER BY id",
                &none
            )
            .unwrap(),
            [1, 3, 4].map(|id| vec![Value::Int(id), Value::Bool(true)])
        );
        // The same queries without access control do see node 2.
        assert_eq!(
            rows(
                &mut db,
                "MATCH (a:Person) WHERE (a)-[:KNOWS]->(:Person {ssn: 'ssn2'}) RETURN a.id AS id"
            ),
            [vec![Value::Int(1)]]
        );
    }

    #[test]
    fn relationship_endpoints_hide_invisible_nodes() {
        let mut db = graph();
        let none = BTreeMap::new();
        // The path's middle node is hidden: its endpoint reads are NULL.
        assert_eq!(
            scoped(
                &mut db,
                "MATCH p = shortestPath((a:Person {id: 1})-[:KNOWS*..3]->(c:Person {id: 3})) \
                 RETURN [r IN relationships(p) | startNode(r).ssn] AS starts, \
                 [r IN relationships(p) | endNode(r).ssn] AS ends",
                &none
            )
            .unwrap(),
            [vec![
                Value::List(vec![Value::String("ssn1".into()), Value::Null]),
                Value::List(vec![Value::Null, Value::String("ssn3".into())]),
            ]]
        );
        // A relationship value obtained without access control does not
        // reach the hidden node either, nor does a forged map.
        let rels = rows(
            &mut db,
            "MATCH p = shortestPath((a:Person {id: 1})-[:KNOWS*..3]->(c:Person {id: 3})) \
             RETURN relationships(p) AS rels",
        )[0][0]
            .clone();
        let Value::List(values) = &rels else {
            panic!("{rels:?}")
        };
        let parameters = BTreeMap::from([("r".to_string(), values[1].clone())]);
        assert_eq!(
            scoped(
                &mut db,
                "UNWIND [$r] AS r RETURN startNode(r) AS s",
                &parameters
            )
            .unwrap(),
            [vec![Value::Null]]
        );
        let Value::Map(real) = &values[1] else {
            panic!("{:?}", values[1])
        };
        // Guessed ids cannot probe for relationships: under the policy a
        // value naming no stored relationship reads as NULL, exactly like the
        // real relationship above whose endpoint is hidden. Without the
        // policy it fails.
        let mut forged = real.clone();
        forged.insert("_id".to_string(), Value::Int(9999));
        let mut reversed = real.clone();
        let source = reversed.remove("source_id").unwrap();
        let target = reversed.insert("target_id".to_string(), source).unwrap();
        reversed.insert("source_id".to_string(), target);
        for value in [Value::Map(forged), Value::Map(reversed)] {
            let parameters = BTreeMap::from([("r".to_string(), value)]);
            for query in [
                "UNWIND [$r] AS r RETURN startNode(r) AS s",
                "UNWIND [$r] AS r RETURN endNode(r) AS s",
            ] {
                assert_eq!(
                    scoped(&mut db, query, &parameters).unwrap(),
                    [vec![Value::Null]],
                    "{query}"
                );
                let error = db
                    .query_with_params(query, &parameters)
                    .unwrap_err()
                    .to_string();
                assert!(error.contains("names no stored relationship"), "{error}");
            }
        }
        // Inside a pattern body, endpoints of relationships it binds come
        // from the visible match itself.
        assert_eq!(
            scoped(
                &mut db,
                "MATCH (a:Person {id: 1}) RETURN [(a)-[r:KNOWS]->(b) | endNode(r).ssn] AS ends",
                &none
            )
            .unwrap(),
            [vec![Value::List(vec![Value::String("ssn4".into())])]]
        );
    }
}
