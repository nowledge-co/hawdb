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

//! `floor`/`ceil`/`round`/`abs`/`sign`/`sqrt`, `reduce(...)` and
//! `startNode(r)`/`endNode(r)` end to end (#1487), including the LDBC SNB
//! IC7 and IC14 fragments that previously failed to parse.

use super::*;

fn floats(values: &[f64]) -> Value {
    Value::List(values.iter().copied().map(Value::Float).collect())
}

fn ints(values: &[i64]) -> Value {
    Value::List(values.iter().copied().map(Value::Int).collect())
}

fn single_row(db: &mut Database, query: &str) -> crate::QueryRow {
    let output = db
        .query(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    assert_eq!(output.rows.len(), 1, "{query}");
    output.rows[0].clone()
}

fn error(db: &mut Database, query: &str) -> String {
    db.query(query)
        .err()
        .unwrap_or_else(|| panic!("{query} should fail"))
        .to_string()
}

/// `(:Person {id: 1})-[:KNOWS {w: 1.5}]->(:Person {id: 2})-[:KNOWS {w: 2.25}]->(:Person {id: 3})`,
/// plus a message by person 1 liked by persons 2 and 3.
fn graph() -> Database {
    let mut db = Database::new();
    for (id, score) in [(1, "-1.5"), (2, "2.5"), (3, "-7")] {
        db.query(&format!("CREATE (:Person {{id: {id}, score: {score}}})"))
            .unwrap();
    }
    for (from, to, weight) in [(1, 2, "1.5"), (2, 3, "2.25")] {
        db.query(&format!(
            "MATCH (a:Person {{id: {from}}}), (b:Person {{id: {to}}}) CREATE (a)-[:KNOWS {{w: {weight}}}]->(b)"
        ))
        .unwrap();
    }
    db.query("CREATE (:Message {id: 10, creationDate: 1000})")
        .unwrap();
    db.query("MATCH (p:Person {id: 1}), (m:Message {id: 10}) CREATE (m)-[:HAS_CREATOR]->(p)")
        .unwrap();
    for (liker, time) in [(2, 1_000 + 90 * 60_000), (3, 1_000 + 30_500)] {
        db.query(&format!(
            "MATCH (l:Person {{id: {liker}}}), (m:Message {{id: 10}}) CREATE (l)-[:LIKES {{creationDate: {time}}}]->(m)"
        ))
        .unwrap();
    }
    db
}

#[test]
fn numeric_functions_follow_opencypher_types_and_null() {
    let mut db = graph();
    let row = single_row(
        &mut db,
        "UNWIND [1] AS row RETURN floor(1.5) AS f, floor(-1.5) AS nf, ceil(-1.5) AS c, ceil(1) AS ci, \
         round(2.5) AS r, round(-1.5) AS nr, round(-1.6) AS r2, abs(-3) AS a, \
         abs(-2.5) AS af, sign(-4) AS s, sqrt(12.96) AS q, floor(null) AS n, \
         abs(null) AS an",
    );
    assert_eq!(row["f"], Value::Float(1.0));
    assert_eq!(row["nf"], Value::Float(-2.0));
    assert_eq!(row["c"], Value::Float(-1.0));
    assert_eq!(row["ci"], Value::Float(1.0));
    assert_eq!(row["r"], Value::Float(3.0));
    // Neo4j: ties round toward positive infinity for one-argument round().
    assert_eq!(row["nr"], Value::Float(-1.0));
    assert_eq!(row["r2"], Value::Float(-2.0));
    assert_eq!(row["a"], Value::Int(3));
    assert_eq!(row["af"], Value::Float(2.5));
    assert_eq!(row["s"], Value::Int(-1));
    assert_eq!(row["q"], Value::Float(3.6));
    assert_eq!(row["n"], Value::Null);
    assert_eq!(row["an"], Value::Null);

    let output = db
        .query(
            "MATCH (p:Person) RETURN p.id AS id, abs(p.score) AS a, round(p.score) AS r, \
             floor(p.score) + 1 AS f, p.missing IS NULL AS missing, abs(p.missing) AS n \
             ORDER BY id",
        )
        .unwrap();
    let column =
        |name: &str| Value::List(output.rows.iter().map(|row| row[name].clone()).collect());
    assert_eq!(
        column("a"),
        Value::List(vec![Value::Float(1.5), Value::Float(2.5), Value::Int(7),])
    );
    assert_eq!(column("r"), floats(&[-1.0, 3.0, -7.0]));
    assert_eq!(column("f"), floats(&[-1.0, 3.0, -6.0]));
    assert_eq!(column("n"), Value::List(vec![Value::Null; 3]));

    let output = db
        .query("MATCH (p:Person) WHERE abs(p.score) > 2 RETURN p.id AS id ORDER BY ceil(p.score)")
        .unwrap();
    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(3), Value::Int(2)]
    );
    assert!(error(&mut db, "UNWIND [1] AS row RETURN floor('x') AS f")
        .contains("floor() requires an INTEGER or FLOAT argument, got STRING"));
    assert!(error(&mut db, "UNWIND [1] AS row RETURN round(1, 2) AS r")
        .contains("round() expects 1 argument(s)"));
}

#[test]
fn reduce_folds_left_to_right_with_scoped_variables() {
    let mut db = graph();
    let row = single_row(
        &mut db,
        "UNWIND [1] AS row RETURN reduce(total = 0, x IN [1, 2, 3] | total + x) AS sum, \
         reduce(s = '', x IN ['ab', 'abc', 'a'] | CASE WHEN size(x) > size(s) THEN x ELSE s END) AS text, \
         reduce(total = 0, x IN null | total + x) AS null_list, \
         reduce(total = 5, x IN [] | total + x) AS empty, \
         reduce(acc = [], x IN [1, 2] | list_append(acc, x * 10)) AS list, \
         reduce(outer = 0, xs IN [[1, 2], [3]] | outer + reduce(inner = 0, x IN xs | inner + x)) AS nested",
    );
    assert_eq!(row["sum"], Value::Int(6));
    assert_eq!(row["text"], Value::String("abc".into()));
    assert_eq!(row["null_list"], Value::Null);
    assert_eq!(row["empty"], Value::Int(5));
    assert_eq!(row["list"], ints(&[10, 20]));
    assert_eq!(row["nested"], Value::Int(6));

    // The initial value and the list read the enclosing row.
    let row = single_row(
        &mut db,
        "MATCH (p:Person {id: 2}) RETURN reduce(total = p.id, x IN [p.id, 1] | total * x) AS v",
    );
    assert_eq!(row["v"], Value::Int(4));

    // Over collect() and over the nodes of a path.
    let row = single_row(
        &mut db,
        "MATCH (p:Person) WITH collect(p.id) AS ids RETURN reduce(s = 0, id IN ids | s + id) AS s",
    );
    assert_eq!(row["s"], Value::Int(6));
    let row = single_row(
        &mut db,
        "MATCH path = shortestPath((a:Person {id: 1})-[:KNOWS*..3]-(b:Person {id: 3})) \
         RETURN reduce(s = 0, n IN nodes(path) | s + n.id) AS s, \
         reduce(w = 0.0, r IN relationships(path) | w + r.w) AS w",
    );
    assert_eq!(row["s"], Value::Int(6));
    assert_eq!(row["w"], Value::Float(3.75));

    let row = single_row(
        &mut db,
        "MATCH (p:Person) WHERE reduce(s = 0, x IN [p.id, p.id] | s + x) = 4 RETURN p.id AS id",
    );
    assert_eq!(row["id"], Value::Int(2));

    assert!(error(
        &mut db,
        "MATCH (p:Person) RETURN reduce(p = 0, x IN [1] | p + x) AS v"
    )
    .contains("reduce() variable 'p' shadows a variable that is already in scope"));
    assert!(error(
        &mut db,
        "UNWIND [1] AS row RETURN reduce(x = 0, x IN [1] | x) AS v"
    )
    .contains("reduce() accumulator and list variable must differ"));
    assert!(error(
        &mut db,
        "UNWIND [1] AS row RETURN reduce(s = 0, x IN 3 | s + x) AS v"
    )
    .contains("reduce() requires a list"));
    assert!(error(&mut db, "UNWIND [1] AS row RETURN reduce(s) AS v").contains("reduce() requires"));
}

#[test]
fn start_and_end_node_return_the_stored_direction() {
    let mut db = graph();
    let row = single_row(
        &mut db,
        "MATCH (a:Person {id: 1})-[r:KNOWS]->(b) RETURN startNode(r).id AS s, endNode(r).id AS e",
    );
    assert_eq!(
        (row["s"].clone(), row["e"].clone()),
        (Value::Int(1), Value::Int(2))
    );
    let row = single_row(
        &mut db,
        "MATCH (a:Person {id: 2})<-[r:KNOWS]-(b) RETURN startNode(r).id AS s, endNode(r).id AS e",
    );
    assert_eq!(
        (row["s"].clone(), row["e"].clone()),
        (Value::Int(1), Value::Int(2))
    );

    // An undirected match binds each relationship from both ends; the
    // endpoints keep the stored direction.
    let output = db
        .query(
            "MATCH (a:Person)-[r:KNOWS]-(b:Person) \
             RETURN a.id AS a, startNode(r).id AS s, endNode(r).id AS e ORDER BY a, s",
        )
        .unwrap();
    let rows = output
        .rows
        .iter()
        .map(|row| (row["a"].clone(), row["s"].clone(), row["e"].clone()))
        .collect::<Vec<_>>();
    let triple = |a, s, e| (Value::Int(a), Value::Int(s), Value::Int(e));
    assert_eq!(
        rows,
        vec![
            triple(1, 1, 2),
            triple(2, 1, 2),
            triple(2, 2, 3),
            triple(3, 2, 3),
        ]
    );

    // Whole node values, anonymous endpoints and predicates.
    let row = single_row(
        &mut db,
        "MATCH ()-[r:KNOWS]->() WHERE startNode(r).id = 2 RETURN endNode(r) AS node",
    );
    let Value::Map(node) = &row["node"] else {
        panic!("expected a node value, got {:?}", row["node"]);
    };
    assert_eq!(node["id"], Value::Int(3));
    assert_eq!(
        node["labels"],
        Value::List(vec![Value::String("Person".into())])
    );

    // NULL from an unmatched OPTIONAL MATCH.
    let row = single_row(
        &mut db,
        "MATCH (p:Person {id: 3}) OPTIONAL MATCH (p)-[r:KNOWS]->(q) \
         RETURN startNode(r) AS s, endNode(r).id AS e",
    );
    assert_eq!(
        (row["s"].clone(), row["e"].clone()),
        (Value::Null, Value::Null)
    );

    assert!(
        error(&mut db, "MATCH (p:Person {id: 1}) RETURN startNode(p) AS s")
            .contains("startNode(p) requires 'p' to be bound by a single-hop relationship pattern")
    );
    // A relationship value reads its stored endpoints from the graph (#1488).
    let row = single_row(
        &mut db,
        "MATCH p = shortestPath((a:Person {id: 3})-[:KNOWS*..3]-(b:Person {id: 1})) \
         RETURN [r IN relationships(p) | startNode(r).id] AS s, \
         [r IN relationships(p) | endNode(r).id] AS e",
    );
    assert_eq!(
        (row["s"].clone(), row["e"].clone()),
        (ints(&[2, 1]), ints(&[3, 2]))
    );
}

#[test]
fn endpoints_ignore_other_variables_that_bind_the_same_node() {
    // `(p1)-[:R]->(p2)`, `(p1)-[:KNOWS]->(p2)` and `(p1)-[:ALSO]->(p2)`. Each
    // cyclic query runs as one GraphMatchExec in which `a` also binds p1 but
    // reads only `name`, so its record lacks `id` and `extra`; the endpoint
    // must still be p1's complete node, read from the step's own variable.
    let mut db = Database::new();
    for id in [1, 2] {
        db.query(&format!(
            "CREATE (:P {{id: {id}, name: 'n{id}', extra: 'e{id}'}})"
        ))
        .unwrap();
    }
    for rel_type in ["R", "KNOWS", "ALSO"] {
        db.query(&format!(
            "MATCH (a:P {{id: 1}}), (b:P {{id: 2}}) CREATE (a)-[:{rel_type}]->(b)"
        ))
        .unwrap();
    }
    let complete = |value: &Value, id: i64| {
        let Value::Map(node) = value else {
            panic!("expected a node value, got {value:?}");
        };
        assert_eq!(node["id"], Value::Int(id), "{node:?}");
        assert_eq!(node["name"], Value::String(format!("n{id}")), "{node:?}");
        assert_eq!(node["extra"], Value::String(format!("e{id}")), "{node:?}");
    };
    for query in [
        // Outgoing: `x` is the stored source.
        "MATCH (x)-[r:R]->(y)<-[:KNOWS]-(a), (a)-[:ALSO]->(y) \
         RETURN a.name AS name, startNode(r) AS s, endNode(r) AS e",
        // Incoming: `n` is the stored source.
        "MATCH (a)-[:KNOWS]->(m)<-[r:R]-(n), (n)-[:ALSO]->(m) \
         RETURN a.name AS name, startNode(r) AS s, endNode(r) AS e",
        // Undirected, reached from the target.
        "MATCH (a)-[:KNOWS]->(m)-[r:R]-(n), (n)-[:ALSO]->(m) \
         RETURN a.name AS name, startNode(r) AS s, endNode(r) AS e",
    ] {
        let row = single_row(&mut db, query);
        assert_eq!(row["name"], Value::String("n1".into()), "{query}");
        complete(&row["s"], 1);
        complete(&row["e"], 2);
    }
}

#[test]
fn ldbc_ic7_shaped_minutes_latency_parses_and_runs() {
    let mut db = graph();
    let output = db
        .query(
            "MATCH (person:Person {id: 1})<-[:HAS_CREATOR]-(message:Message)<-[like:LIKES]-(liker:Person) \
             WITH liker, message, like.creationDate AS likeTime, person \
             ORDER BY likeTime DESC, toInteger(message.id) ASC \
             WITH liker, head(collect({msg: message, likeTime: likeTime})) AS latestLike, person \
             RETURN liker.id AS personId, \
               toInteger(floor(toFloat(latestLike.likeTime - latestLike.msg.creationDate)/1000.0)/60.0) AS minutesLatency \
             ORDER BY minutesLatency, personId",
        )
        .unwrap();
    let rows = output
        .rows
        .iter()
        .map(|row| (row["personId"].clone(), row["minutesLatency"].clone()))
        .collect::<Vec<_>>();
    // 30.5 s floors to 30 s, which is 0 whole minutes; 90 minutes stays 90.
    assert_eq!(
        rows,
        vec![
            (Value::Int(3), Value::Int(0)),
            (Value::Int(2), Value::Int(90))
        ]
    );
}

#[test]
fn ldbc_ic14_shaped_path_weights_parse_and_run() {
    let mut db = graph();
    let row = single_row(
        &mut db,
        "MATCH path = shortestPath((person1:Person {id: 1})-[:KNOWS*..3]-(person2:Person {id: 3})) \
         WITH path, relationships(path) AS rels_in_path \
         RETURN [n IN nodes(path) | n.id] AS personIdsInPath, \
           [r IN rels_in_path | reduce(w = 0.0, v IN [r.w, 0.5] | w + v)] AS weights, \
           reduce(weight = 0.0, r IN rels_in_path | weight + r.w) AS pathWeight",
    );
    assert_eq!(row["personIdsInPath"], ints(&[1, 2, 3]));
    assert_eq!(row["weights"], floats(&[2.0, 2.75]));
    assert_eq!(row["pathWeight"], Value::Float(3.75));
    // IC14's endpoint test, on a relationship bound by MATCH.
    let output = db
        .query(
            "MATCH (a:Person)-[r:KNOWS]-(b:Person) \
             WHERE (a.id = startNode(r).id AND b.id = endNode(r).id AND a.id = 2) \
               OR (a.id = endNode(r).id AND b.id = startNode(r).id AND a.id = 2) \
             RETURN b.id AS id ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(1), Value::Int(3)]
    );
}
