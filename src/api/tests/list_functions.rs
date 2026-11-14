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

fn ints(values: &[i64]) -> Value {
    Value::List(values.iter().copied().map(Value::Int).collect())
}

fn strings(values: &[&str]) -> Value {
    Value::List(
        values
            .iter()
            .map(|value| Value::String(value.to_string()))
            .collect(),
    )
}

fn sorted(value: &Value) -> Value {
    let Value::List(values) = value else {
        panic!("expected list, got {value:?}");
    };
    let mut values = values.clone();
    values.sort();
    Value::List(values)
}

fn items() -> Database {
    let mut db = Database::new();
    db.query("CREATE (:Item {id: 1, name: 'abc', tags: ['a', 'b', 'c'], pick: 'b'})")
        .unwrap();
    db.query("CREATE (:Item {id: 2, name: 'xy', tags: ['x', null], pick: 'y'})")
        .unwrap();
    db.query("CREATE (:Item {id: 3, tags: [], pick: 'z'})")
        .unwrap();
    db
}

fn single_row(db: &mut Database, query: &str) -> crate::QueryRow {
    let output = db.query(query).unwrap();
    assert_eq!(output.rows.len(), 1, "{query}");
    output.rows[0].clone()
}

fn ids(output: &QueryOutput) -> Vec<Value> {
    output.rows.iter().map(|row| row["id"].clone()).collect()
}

#[test]
fn list_functions_cover_lists_strings_and_nulls() {
    let mut db = items();
    let row = single_row(
        &mut db,
        "MATCH (n:Item {id: 1}) RETURN size(n.tags) AS size, size(n.name) AS chars, \
         head(n.tags) AS head, last(n.tags) AS last, tail(n.tags) AS tail, \
         reverse(n.tags) AS reversed, reverse(n.name) AS backwards, \
         size(n.missing) AS missing_size, head(n.missing) AS missing_head, \
         tail(n.missing) AS missing_tail, coalesce(head(n.missing), 'none') AS fallback",
    );
    assert_eq!(row["size"], Value::Int(3));
    assert_eq!(row["chars"], Value::Int(3));
    assert_eq!(row["head"], Value::String("a".into()));
    assert_eq!(row["last"], Value::String("c".into()));
    assert_eq!(row["tail"], strings(&["b", "c"]));
    assert_eq!(row["reversed"], strings(&["c", "b", "a"]));
    assert_eq!(row["backwards"], Value::String("cba".into()));
    for name in ["missing_size", "missing_head", "missing_tail"] {
        assert_eq!(row[name], Value::Null, "{name}");
    }
    assert_eq!(row["fallback"], Value::String("none".into()));

    let row = single_row(
        &mut db,
        "MATCH (n:Item {id: 3}) RETURN size(n.tags) AS size, head(n.tags) AS head, \
         last(n.tags) AS last, tail(n.tags) AS tail, reverse(n.tags) AS reversed",
    );
    assert_eq!(row["size"], Value::Int(0));
    assert_eq!(row["head"], Value::Null);
    assert_eq!(row["last"], Value::Null);
    assert_eq!(row["tail"], ints(&[]));
    assert_eq!(row["reversed"], ints(&[]));

    let error = db
        .query("MATCH (n:Item {id: 1}) RETURN head(n.name) AS bad")
        .unwrap_err()
        .to_string();
    assert!(error.contains("head() requires a list"), "{error}");
}

#[test]
fn range_covers_steps_empty_results_and_errors() {
    let mut db = items();
    let row = single_row(
        &mut db,
        "MATCH (n:Item {id: 1}) RETURN range(0, 3) AS up, range(3, 0, -1) AS down, \
         range(0, 10, 4) AS stepped, range(5, 1) AS empty, range(1, 5, -1) AS empty_down, \
         range(2, 2) AS single, size(range(1, 1000)) AS large, range(n.missing, 3) AS missing",
    );
    assert_eq!(row["up"], ints(&[0, 1, 2, 3]));
    assert_eq!(row["down"], ints(&[3, 2, 1, 0]));
    assert_eq!(row["stepped"], ints(&[0, 4, 8]));
    assert_eq!(row["empty"], ints(&[]));
    assert_eq!(row["empty_down"], ints(&[]));
    assert_eq!(row["single"], ints(&[2]));
    assert_eq!(row["large"], Value::Int(1000));
    assert_eq!(row["missing"], Value::Null);

    for (query, message) in [
        (
            "MATCH (n:Item {id: 1}) RETURN range(0, 3, 0) AS bad",
            "step must not be zero",
        ),
        (
            "MATCH (n:Item {id: 1}) RETURN range(0, 'x') AS bad",
            "range() requires integer arguments",
        ),
        (
            "MATCH (n:Item {id: 1}) RETURN range(0, 9223372036854775807) AS bad",
            "exceeding the limit",
        ),
    ] {
        let error = db.query(query).unwrap_err().to_string();
        assert!(error.contains(message), "{query}: {error}");
    }
}

#[test]
fn list_comprehensions_filter_map_and_nest() {
    let mut db = items();
    let row = single_row(
        &mut db,
        "MATCH (n:Item {id: 1}) RETURN [t IN n.tags] AS copy, \
         [t IN n.tags WHERE t <> 'b'] AS filtered, [t IN n.tags | size(t)] AS mapped, \
         [x IN range(1, 6) WHERE x > 2 AND x IN [3, 5, 6] | n.id] AS outer_reference, \
         [x IN range(1, 3) | [y IN range(1, x) WHERE y <> 2]] AS nested, \
         [t IN n.missing | t] AS missing, size([x IN range(1, 10) WHERE x > 7]) AS counted, \
         [x IN range(1, 5)][1..3] AS sliced",
    );
    assert_eq!(row["copy"], strings(&["a", "b", "c"]));
    assert_eq!(row["filtered"], strings(&["a", "c"]));
    assert_eq!(row["mapped"], ints(&[1, 1, 1]));
    assert_eq!(row["outer_reference"], ints(&[1, 1, 1]));
    assert_eq!(
        row["nested"],
        Value::List(vec![ints(&[1]), ints(&[1]), ints(&[1, 3])])
    );
    assert_eq!(row["missing"], Value::Null);
    assert_eq!(row["counted"], Value::Int(3));
    assert_eq!(row["sliced"], ints(&[2, 3]));

    // NULL predicate results drop the element.
    let row = single_row(
        &mut db,
        "MATCH (n:Item {id: 2}) RETURN [t IN n.tags WHERE t = 'x'] AS kept, \
         [t IN n.tags WHERE t IS NULL] AS nulls",
    );
    assert_eq!(row["kept"], strings(&["x"]));
    assert_eq!(row["nulls"], Value::List(vec![Value::Null]));

    let error = db
        .query("MATCH (n:Item) RETURN [n IN n.tags | n] AS bad")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("list comprehension variable 'n' shadows"),
        "{error}"
    );
}

#[test]
fn list_functions_apply_to_collected_values() {
    let mut db = items();
    let row = single_row(
        &mut db,
        "MATCH (n:Item) WITH collect(n.id) AS ids, collect(n) AS nodes \
         RETURN size(ids) AS total, [i IN ids WHERE i > 1] AS big, \
         [p IN nodes WHERE p.name IS NOT NULL | p.name] AS names, size(tail(ids)) AS rest",
    );
    assert_eq!(row["total"], Value::Int(3));
    assert_eq!(sorted(&row["big"]), ints(&[2, 3]));
    assert_eq!(sorted(&row["names"]), strings(&["abc", "xy"]));
    assert_eq!(row["rest"], Value::Int(2));

    let row = single_row(
        &mut db,
        "MATCH (n:Item) RETURN size(collect(n.id)) AS total, size(collect(n.id)) + 1 AS bumped, \
         size(collect(DISTINCT n.missing)) AS no_values",
    );
    assert_eq!(row["total"], Value::Int(3));
    assert_eq!(row["bumped"], Value::Int(4));
    assert_eq!(row["no_values"], Value::Int(0));
    let row = single_row(
        &mut db,
        "MATCH (n:Item) WITH size(collect(n.id)) AS total, collect(n.pick) AS picks \
         WHERE total > 2 RETURN total, size(picks) AS picks",
    );
    assert_eq!(row["total"], Value::Int(3));
    assert_eq!(row["picks"], Value::Int(3));

    let output = db
        .query(
            "MATCH (n:Item) WITH n.pick AS pick, collect(n.id) AS ids \
             RETURN pick, size(ids) AS count, head(ids) AS first ORDER BY pick",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.rows[0]["first"], Value::Int(1));
    assert_eq!(output.rows[2]["count"], Value::Int(1));
}

#[test]
fn indexing_and_slicing_follow_cypher_bounds() {
    let mut db = items();
    let row = single_row(
        &mut db,
        "MATCH (n:Item {id: 1}) RETURN n.tags[0] AS first, n.tags[-1] AS last, \
         n.tags[-3] AS front, n.tags[3] AS past_end, n.tags[-4] AS before_start, \
         n.missing[0] AS missing, n.tags[1..] AS rest, n.tags[..-1] AS init, \
         n.tags[1..2] AS middle, n.tags[2..1] AS crossed, n.tags[-10..10] AS clamped, \
         range(10, 20)[size(n.tags)] AS computed",
    );
    assert_eq!(row["first"], Value::String("a".into()));
    assert_eq!(row["last"], Value::String("c".into()));
    assert_eq!(row["front"], Value::String("a".into()));
    assert_eq!(row["past_end"], Value::Null);
    assert_eq!(row["before_start"], Value::Null);
    assert_eq!(row["missing"], Value::Null);
    assert_eq!(row["rest"], strings(&["b", "c"]));
    assert_eq!(row["init"], strings(&["a", "b"]));
    assert_eq!(row["middle"], strings(&["b"]));
    assert_eq!(row["crossed"], ints(&[]));
    assert_eq!(row["clamped"], strings(&["a", "b", "c"]));
    assert_eq!(row["computed"], Value::Int(13));
}

#[test]
fn in_predicates_use_three_valued_logic() {
    let mut db = items();
    let query = |db: &mut Database, query: &str| db.query(query).unwrap();
    // Literal lists keep the existing value-list predicate.
    let output = query(
        &mut db,
        "MATCH (n:Item) WHERE n.id IN [1, null] RETURN n.id AS id",
    );
    assert_eq!(ids(&output), [Value::Int(1)]);
    let output = query(
        &mut db,
        "MATCH (n:Item) WHERE NOT n.id IN [1, null] RETURN n.id AS id",
    );
    assert!(output.rows.is_empty());
    // A list column uses the generic operator: item 2 compares with a NULL tag.
    let output = query(
        &mut db,
        "MATCH (n:Item) WITH n, n.tags AS tags WHERE n.pick IN tags RETURN n.id AS id",
    );
    assert_eq!(ids(&output), [Value::Int(1)]);
    let output = query(
        &mut db,
        "MATCH (n:Item) WITH n, n.tags AS tags WHERE NOT n.pick IN tags RETURN n.id AS id",
    );
    assert_eq!(ids(&output), [Value::Int(3)]);
    let output = query(
        &mut db,
        "MATCH (n:Item) RETURN n.id AS id, CASE WHEN n.pick IN n.tags THEN 'yes' \
         WHEN NOT (n.pick IN n.tags) THEN 'no' ELSE 'unknown' END AS member ORDER BY id",
    );
    let members: Vec<_> = output
        .rows
        .iter()
        .map(|row| row["member"].clone())
        .collect();
    assert_eq!(Value::List(members), strings(&["yes", "unknown", "no"]));
    // WHERE and ORDER BY accept list functions and indexes.
    let output = query(
        &mut db,
        "MATCH (n:Item) WHERE size(n.tags) > 0 AND n.tags[0] <> 'q' \
         RETURN n.id AS id ORDER BY size(n.tags) DESC",
    );
    assert_eq!(ids(&output), [Value::Int(1), Value::Int(2)]);
}

#[test]
fn list_expression_parameters_are_cache_keyed_and_rebound() {
    let mut db = items();
    // Parameters inside list expressions are detected by the plan cache and
    // keyed exactly: a different value never reuses a stale plan.
    let query = "MATCH (n:Item) WHERE n.id = 1 \
                 RETURN [x IN range(1, $end) WHERE x > $min | x] AS xs, n.tags[$index] AS tag, \
                 CASE WHEN $needle IN n.tags THEN 1 ELSE 0 END AS found";
    let parameters = |end: i64, min: i64, index: i64, needle: &str| {
        BTreeMap::from([
            ("end".to_string(), Value::Int(end)),
            ("min".to_string(), Value::Int(min)),
            ("index".to_string(), Value::Int(index)),
            ("needle".to_string(), Value::String(needle.to_string())),
        ])
    };
    let run = |db: &mut Database, parameters: &BTreeMap<String, Value>| {
        let output = db.query_with_params(query, parameters).unwrap();
        assert_eq!(output.rows.len(), 1);
        output.rows[0].clone()
    };
    let first = parameters(4, 1, 0, "a");
    let row = run(&mut db, &first);
    assert_eq!(row["xs"], ints(&[2, 3, 4]));
    assert_eq!(row["tag"], Value::String("a".into()));
    assert_eq!(row["found"], Value::Int(1));

    let row = run(&mut db, &parameters(6, 4, -1, "q"));
    assert_eq!(row["xs"], ints(&[5, 6]));
    assert_eq!(row["tag"], Value::String("c".into()));
    assert_eq!(row["found"], Value::Int(0));

    let row = run(&mut db, &parameters(2, 0, 7, "c"));
    assert_eq!(row["xs"], ints(&[1, 2]));
    assert_eq!(row["tag"], Value::Null);
    assert_eq!(row["found"], Value::Int(1));

    let before = db.plan_cache_stats().unwrap();
    let row = run(&mut db, &first);
    assert_eq!(row["xs"], ints(&[2, 3, 4]));
    assert_eq!(row["found"], Value::Int(1));
    let after = db.plan_cache_stats().unwrap();
    assert_eq!(after.hits, before.hits + 1, "{before:?} -> {after:?}");
    assert_eq!(
        db.explain_query_with_params(query, &first)
            .unwrap()
            .plan_cache_lookup,
        PlanCacheLookup::Hit
    );

    // A predicate slot rebinds across values while list expressions are
    // evaluated per row from the cached template.
    let query = "MATCH (n:Item) WHERE n.id = $id \
                 RETURN [t IN n.tags WHERE t <> 'b'] AS kept, size(n.tags) AS size, n.tags[-1] AS last";
    let id = |id: i64| BTreeMap::from([("id".to_string(), Value::Int(id))]);
    let row = db.query_with_params(query, &id(1)).unwrap().rows[0].clone();
    assert_eq!(row["kept"], strings(&["a", "c"]));
    let before = db.plan_cache_stats().unwrap();
    let output = db.query_with_params(query, &id(3)).unwrap();
    assert_eq!(db.plan_cache_stats().unwrap().hits, before.hits + 1);
    assert_eq!(output.rows[0]["kept"], ints(&[]));
    assert_eq!(output.rows[0]["size"], Value::Int(0));
    assert_eq!(output.rows[0]["last"], Value::Null);
    let output = db.query_with_params(query, &id(2)).unwrap();
    // `null <> 'b'` is unknown, so the NULL tag is filtered out.
    assert_eq!(output.rows[0]["kept"], strings(&["x"]));
    assert_eq!(output.rows[0]["last"], Value::Null);

    // A rebound step of zero still fails at execution time.
    let step_query = "MATCH (n:Item) WHERE n.id = 1 RETURN range(1, 2, $step) AS r";
    let step = |step: i64| BTreeMap::from([("step".to_string(), Value::Int(step))]);
    assert_eq!(
        db.query_with_params(step_query, &step(1)).unwrap().rows[0]["r"],
        ints(&[1, 2])
    );
    let error = db
        .query_with_params(step_query, &step(0))
        .unwrap_err()
        .to_string();
    assert!(error.contains("step must not be zero"), "{error}");
}

#[test]
fn documented_ldbc_style_example_runs() {
    let mut db = Database::new();
    for (id, name, age) in [(1, "a", 20), (2, "b", 40), (3, "c", 50)] {
        db.query(&format!(
            "CREATE (:Person {{id: {id}, name: '{name}', age: {age}}})"
        ))
        .unwrap();
    }
    for target in [2, 3] {
        db.query(&format!(
            "MATCH (a:Person {{id: 1}}), (b:Person {{id: {target}}}) CREATE (a)-[:KNOWS]->(b)"
        ))
        .unwrap();
    }
    let output = db
        .query_with_params(
            "MATCH (p:Person)-[:KNOWS]->(f:Person) WITH p, collect(f) AS friends \
             WHERE size(friends) > $min RETURN p.id AS id, size(friends) AS total, \
             head([x IN friends WHERE x.age > 30 | x.name]) AS older, \
             [x IN friends | x.name][-1] AS last_name ORDER BY total DESC",
            &BTreeMap::from([("min".to_string(), Value::Int(1))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0]["id"], Value::Int(1));
    assert_eq!(output.rows[0]["total"], Value::Int(2));
    assert!(matches!(&output.rows[0]["older"], Value::String(name) if name == "b" || name == "c"));
    assert!(
        matches!(&output.rows[0]["last_name"], Value::String(name) if name == "b" || name == "c")
    );
}

#[test]
fn nested_list_expressions_fail_closed_at_the_work_limit() {
    let mut db = items();
    // The inner comprehension retains nothing, so only the work limit stops
    // 4,000 x 4,000 iterations.
    let error = db
        .query(
            "MATCH (n:Item {id: 1}) RETURN [x IN range(1, 4000) | [y IN range(1, 4000) WHERE false]] AS grid",
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("list expressions exceed the work limit"),
        "{error}"
    );
    // The limit is per evaluated row, not per query.
    let output = db
        .query("MATCH (n:Item) RETURN size([x IN range(1, 1000) WHERE x > 500]) AS upper")
        .unwrap();
    assert!(!output.rows.is_empty());
    assert!(output
        .rows
        .iter()
        .all(|row| row.get("upper") == Some(&Value::Int(500))));
}

#[test]
fn list_expressions_compose_with_expression_aggregates() {
    let mut db = items();
    // sizes: 3, 2, 0; non-null tags: 3, 1, 0.
    let row = single_row(
        &mut db,
        "MATCH (n:Item) RETURN sum(size(n.tags)) AS total, \
         sum(size([t IN n.tags WHERE t IS NOT NULL])) AS present, \
         max(size(n.tags)) AS widest, collect(DISTINCT head(n.tags)) AS heads",
    );
    assert_eq!(row.get("total"), Some(&Value::Int(5)));
    assert_eq!(row.get("present"), Some(&Value::Int(4)));
    assert_eq!(row.get("widest"), Some(&Value::Int(3)));
    assert_eq!(sorted(row.get("heads").unwrap()), strings(&["a", "x"]));
}

#[test]
fn list_expressions_without_a_memory_account_fail_closed() {
    let mut db = items();
    // Aggregate arguments evaluate without a query memory account: 100,000
    // nested 50-element lists stay under the work limit but exceed the
    // unaccounted-bytes limit.
    let error = db
        .query("MATCH (n:Item) RETURN sum(size([x IN range(1, 100000) | range(1, 50)])) AS s")
        .unwrap_err();
    assert!(
        error.to_string().contains("without a query memory account"),
        "{error}"
    );
    // Small aggregate arguments still work.
    let row = single_row(
        &mut db,
        "MATCH (n:Item {id: 1}) RETURN sum(size([x IN range(1, 10) | range(1, 3)])) AS s",
    );
    assert_eq!(row.get("s"), Some(&Value::Int(10)));
}

#[test]
fn work_inside_comprehension_bodies_is_charged() {
    let mut db = items();
    let ids = Value::List((0..100_000).map(Value::Int).collect());
    let error = db
        .query_with_params(
            "MATCH (n:Item {id: 1}) RETURN size([x IN range(1, 1000000) WHERE x IN $ids]) AS hits",
            &BTreeMap::from([("ids".to_string(), ids)]),
        )
        .unwrap_err();
    assert!(error.to_string().contains("work limit"), "{error}");
    let big = Value::String("x".repeat(10_000));
    let error = db
        .query_with_params(
            "MATCH (n:Item {id: 1}) RETURN size([x IN range(1, 100000) WHERE size(reverse($big)) > 0]) AS n",
            &BTreeMap::from([("big".to_string(), big)]),
        )
        .unwrap_err();
    assert!(error.to_string().contains("work limit"), "{error}");
    let error = db
        .query_with_params(
            "MATCH (n:Item {id: 1}) RETURN size([x IN range(1, 100000) WHERE size(tail($ids)) > 0]) AS n",
            &BTreeMap::from([(
                "ids".to_string(),
                Value::List((0..1_000).map(Value::Int).collect()),
            )]),
        )
        .unwrap_err();
    assert!(error.to_string().contains("work limit"), "{error}");
}
