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

fn people() -> Database {
    let mut db = Database::new();
    for statement in [
        "CREATE (:Person {name: 'alice', lastName: 'Smith', age: 34, city: 'Berlin', tags: ['a', 'b'], rank: 2})",
        "CREATE (:Person {name: 'bob', lastName: 'Jones', age: 27, city: 'amsterdam', tags: ['x', 'y', 'z']})",
        "CREATE (:Person {name: 'carol', lastName: 'Smith', age: 41, city: 'Copenhagen', tags: [], rank: 1})",
        "CREATE (:Person {name: 'dave', lastName: 'Brown', age: 19, city: 'berlin', tags: ['q'], rank: 3})",
        "CREATE (:Person {name: 'erin', lastName: 'Jones', age: 27})",
    ] {
        db.query(statement).unwrap();
    }
    db
}

fn column(output: &QueryOutput, name: &str) -> Vec<Value> {
    output.rows.iter().map(|row| row[name].clone()).collect()
}

fn strings(values: &[&str]) -> Vec<Value> {
    values
        .iter()
        .map(|value| Value::String(value.to_string()))
        .collect()
}

fn names(db: &mut Database, query: &str) -> Vec<Value> {
    let output = db.query(query).unwrap();
    column(&output, "name")
}

fn selected_plan(db: &Database, query: &str) -> String {
    db.explain_query(query).unwrap().trace.selected_plan
}

#[test]
fn orders_by_function_calls_over_unreturned_variables() {
    let mut db = people();
    // `size` of a missing list is NULL, which sorts like a NULL property key.
    assert_eq!(
        names(
            &mut db,
            "MATCH (n:Person) RETURN n.name AS name ORDER BY size(n.tags) DESC, name",
        ),
        strings(&["bob", "alice", "dave", "carol", "erin"])
    );
    let output = db
        .query("MATCH (n:Person) RETURN n.name ORDER BY lower(n.city), n.age DESC")
        .unwrap();
    assert_eq!(output.rows[0].keys().collect::<Vec<_>>(), ["n.name"]);
    assert_eq!(
        column(&output, "n.name"),
        strings(&["erin", "bob", "alice", "dave", "carol"])
    );
}

#[test]
fn orders_by_any_scalar_function_call() {
    let mut db = people();
    for (kind, target) in [("LIKES", "bob"), ("KNOWS", "carol"), ("ADMIRES", "dave")] {
        db.query(&format!(
            "MATCH (a:Person {{name: 'alice'}}), (b:Person {{name: '{target}'}}) \
             CREATE (a)-[:{kind}]->(b)"
        ))
        .unwrap();
    }
    assert_eq!(
        names(
            &mut db,
            "MATCH (:Person {name: 'alice'})-[r]->(b:Person) RETURN b.name AS name \
             ORDER BY type(r) DESC",
        ),
        strings(&["bob", "carol", "dave"])
    );
    assert_eq!(
        names(
            &mut db,
            "MATCH (:Person {name: 'alice'})-[r]->(b:Person) \
             WITH b, r ORDER BY type(r) LIMIT 2 RETURN b.name AS name",
        ),
        strings(&["dave", "carol"])
    );
}

#[test]
fn orders_by_mixed_direction_expression_and_property_keys() {
    let mut db = people();
    assert_eq!(
        names(
            &mut db,
            "MATCH (n:Person) RETURN n.name AS name \
             ORDER BY n.lastName DESC, size(n.tags) ASC, n.age DESC",
        ),
        strings(&["carol", "alice", "erin", "bob", "dave"])
    );
}

#[test]
fn orders_by_expressions_over_projected_aliases() {
    let mut db = people();
    assert_eq!(
        names(
            &mut db,
            "MATCH (n:Person) WHERE n.tags IS NOT NULL RETURN n.name AS name, n.tags AS tags \
             ORDER BY size(tags) DESC, head(tags)",
        ),
        strings(&["bob", "alice", "dave", "carol"])
    );
    // An alias shadows the pre-projection variable of the same name.
    assert_eq!(
        names(
            &mut db,
            "MATCH (n:Person) WITH n.name AS name, n.age AS n \
             RETURN name ORDER BY n % 10 DESC, name",
        ),
        strings(&["dave", "bob", "erin", "alice", "carol"])
    );
}

#[test]
fn orders_by_case_and_arithmetic_keys() {
    let mut db = people();
    assert_eq!(
        names(
            &mut db,
            "MATCH (n:Person) RETURN n.name AS name \
             ORDER BY CASE WHEN n.age < 30 THEN 0 ELSE 1 END, -n.age, name",
        ),
        strings(&["bob", "erin", "dave", "carol", "alice"])
    );
    assert_eq!(
        names(
            &mut db,
            "MATCH (n:Person) RETURN n.name AS name ORDER BY n.age % 10, (n.age + 1) * 2 DESC, name",
        ),
        strings(&["carol", "alice", "bob", "erin", "dave"])
    );
}

#[test]
fn expression_keys_order_nulls_like_property_keys() {
    let mut db = people();
    for direction in ["ASC", "DESC"] {
        let by_property = names(
            &mut db,
            &format!("MATCH (n:Person) RETURN n.name AS name ORDER BY n.rank {direction}, name"),
        );
        let by_expression = names(
            &mut db,
            &format!(
                "MATCH (n:Person) RETURN n.name AS name ORDER BY n.rank * 1 {direction}, name"
            ),
        );
        assert_eq!(by_expression, by_property, "{direction}");
        let by_function = names(
            &mut db,
            &format!(
                "MATCH (n:Person) RETURN n.name AS name \
                 ORDER BY coalesce(n.rank, null) {direction}, name"
            ),
        );
        assert_eq!(by_function, by_property, "{direction}");
    }
}

#[test]
fn parameterized_order_keys_rebind_through_the_plan_cache() {
    let mut db = people();
    let parameter = |name: &str, value: Value| BTreeMap::from([(name.to_string(), value)]);
    let run = |db: &mut Database, query: &str, parameters: &BTreeMap<String, Value>| {
        column(&db.query_with_params(query, parameters).unwrap(), "name")
    };

    // A simple CASE with literal branches is a logical template slot: one
    // proven template rebinds to each value, and the bound plan is cached.
    let query = "MATCH (n:Person) RETURN n.name AS name \
                 ORDER BY CASE n.lastName WHEN $first THEN 0 ELSE 1 END, name";
    let first = |name: &str| parameter("first", Value::String(name.to_string()));
    for (value, expected) in [
        ("Jones", ["bob", "erin", "alice", "carol", "dave"]),
        ("Smith", ["alice", "carol", "bob", "dave", "erin"]),
        ("Brown", ["dave", "alice", "bob", "carol", "erin"]),
    ] {
        assert_eq!(run(&mut db, query, &first(value)), strings(&expected));
    }
    let explain = db
        .explain_query_with_params(query, &first("Nobody"))
        .unwrap();
    assert_eq!(explain.plan_cache_lookup, PlanCacheLookup::Hit);
    assert!(
        explain
            .trace
            .decisions
            .iter()
            .any(|decision| decision == "parameter binding cache hit: proven logical template"),
        "{:?}",
        explain.trace.decisions
    );

    // Other parameterized expressions key the cached plan by value, like the
    // same expression in a projection, so a new value never reuses a plan
    // bound to an old one.
    let query = "MATCH (n:Person) RETURN n.name AS name ORDER BY coalesce(n.rank, $missing), name";
    let missing = |value: i64| parameter("missing", Value::Int(value));
    assert_eq!(
        run(&mut db, query, &missing(0)),
        strings(&["bob", "erin", "carol", "alice", "dave"])
    );
    assert_eq!(
        run(&mut db, query, &missing(9)),
        strings(&["carol", "alice", "dave", "bob", "erin"])
    );
    assert_eq!(
        run(&mut db, query, &missing(2)),
        strings(&["carol", "alice", "bob", "erin", "dave"])
    );
    let before = db.plan_cache_stats().unwrap();
    assert_eq!(
        run(&mut db, query, &missing(9)),
        strings(&["carol", "alice", "dave", "bob", "erin"])
    );
    assert_eq!(db.plan_cache_stats().unwrap().hits, before.hits + 1);
    assert_eq!(
        db.explain_query_with_params(query, &missing(0))
            .unwrap()
            .plan_cache_lookup,
        PlanCacheLookup::Hit
    );
}

#[test]
fn with_order_by_expression_limit_feeds_a_later_match() {
    let mut db = people();
    for statement in [
        "CREATE (:City {name: 'Lyon'})",
        "CREATE (:City {name: 'Oslo'})",
        "CREATE (:Person {name: 'root', lastName: 'Root', age: 50})",
    ] {
        db.query(statement).unwrap();
    }
    for (friend, hops) in [
        ("alice", 2),
        ("bob", 2),
        ("bob", 1),
        ("carol", 1),
        ("dave", 2),
        ("erin", 1),
        ("erin", 2),
    ] {
        db.query(&format!(
            "MATCH (p:Person {{name: 'root'}}), (f:Person {{name: '{friend}'}}) \
             CREATE (p)-[:KNOWS {{hops: {hops}}}]->(f)"
        ))
        .unwrap();
    }
    for (friend, city) in [
        ("alice", "Oslo"),
        ("bob", "Lyon"),
        ("carol", "Oslo"),
        ("dave", "Lyon"),
        ("erin", "Lyon"),
    ] {
        db.query(&format!(
            "MATCH (f:Person {{name: '{friend}'}}), (c:City {{name: '{city}'}}) \
             CREATE (f)-[:IS_LOCATED_IN]->(c)"
        ))
        .unwrap();
    }
    // IC1 shape: aggregate, order by alias, property and function keys, limit,
    // then continue matching from the surviving rows.
    let output = db
        .query(
            "MATCH (p:Person {name: 'root'})-[k:KNOWS]->(friend:Person) \
             WITH min(k.hops) AS distance, friend \
             ORDER BY distance ASC, friend.lastName ASC, size(friend.name) ASC \
             LIMIT 3 \
             MATCH (friend)-[:IS_LOCATED_IN]->(city:City) \
             RETURN friend.name AS name, distance, city.name AS city \
             ORDER BY distance ASC, lower(city) DESC, size(name) ASC",
        )
        .unwrap();
    assert_eq!(
        output.rows[0].keys().collect::<Vec<_>>(),
        ["name", "distance", "city"]
    );
    assert_eq!(column(&output, "name"), strings(&["carol", "bob", "erin"]));
    assert_eq!(
        column(&output, "distance"),
        [Value::Int(1), Value::Int(1), Value::Int(1)]
    );
    assert_eq!(column(&output, "city"), strings(&["Oslo", "Lyon", "Lyon"]));
}

#[test]
fn distinct_order_keys_must_use_projected_columns() {
    let mut db = people();
    let output = db
        .query(
            "MATCH (n:Person) RETURN DISTINCT n.lastName AS last \
             ORDER BY CASE last WHEN 'Jones' THEN 0 ELSE 1 END, size(last) DESC, last",
        )
        .unwrap();
    assert_eq!(
        column(&output, "last"),
        strings(&["Jones", "Brown", "Smith"])
    );
    let error = db
        .query("MATCH (n:Person) RETURN DISTINCT n.lastName AS last ORDER BY size(n.tags)")
        .unwrap_err()
        .to_string();
    assert!(error.contains("'n'"), "{error}");
}

#[test]
fn aggregate_order_keys_resolve_to_projected_aggregates() {
    let mut db = people();
    for query in [
        "MATCH (n:Person) RETURN n.lastName AS last, sum(n.age) AS total \
         ORDER BY sum(n.age) DESC",
        "MATCH (n:Person) RETURN n.lastName AS last, sum(n.age) \
         ORDER BY sum(n.age) DESC",
        "MATCH (n:Person) RETURN n.lastName AS last, sum(n.age) AS total \
         ORDER BY total DESC",
        "MATCH (n:Person) RETURN sum(n.age) AS total, n.lastName AS last \
         ORDER BY sum(n.age) DESC",
    ] {
        let output = db.query(query).unwrap();
        assert_eq!(
            column(&output, "last"),
            strings(&["Smith", "Jones", "Brown"]),
            "{query}"
        );
    }
    let output = db
        .query(
            "MATCH (n:Person) RETURN n.lastName AS last, collect(n.name) AS names \
             ORDER BY size(names) DESC, lower(last) DESC",
        )
        .unwrap();
    assert_eq!(
        column(&output, "last"),
        strings(&["Smith", "Jones", "Brown"])
    );
    let output = db
        .query(
            "MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people \
             ORDER BY count(*) % 2, last DESC",
        )
        .unwrap_err()
        .to_string();
    assert!(output.contains("must also be projected"), "{output}");
    for query in [
        "MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people ORDER BY max(n.age)",
        "MATCH (n:Person) RETURN n.name AS name ORDER BY count(*)",
    ] {
        let error = db.query(query).unwrap_err().to_string();
        assert!(error.contains("must also be projected"), "{query}: {error}");
    }
}

#[test]
fn aggregating_projections_reject_expression_keys_over_unprojected_variables() {
    let mut db = people();
    // Only projected names are in scope after an aggregation. Grouping by the
    // key instead would split `people` per age, so these must fail.
    for query in [
        "MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people ORDER BY n.age * 2 DESC",
        "MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people ORDER BY size(n.tags)",
        "MATCH (n:Person) WITH n.lastName AS last, count(*) AS people \
         ORDER BY -n.age RETURN last, people",
    ] {
        let error = db.query(query).unwrap_err();
        assert!(
            matches!(error, hawdb_core::HawDBError::Semantic(_)),
            "{query}: {error:?}"
        );
    }
    // Keys built only from grouped values are fine: a grouping key itself,
    // or a property of a grouped variable.
    let output = db
        .query(
            "MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people \
             ORDER BY CASE WHEN n.lastName = 'Brown' THEN 0 ELSE 1 END, last",
        )
        .unwrap();
    assert_eq!(
        column(&output, "last"),
        strings(&["Brown", "Jones", "Smith"])
    );
    let output = db
        .query(
            "MATCH (n:Person) WITH n, count(*) AS people \
             ORDER BY n.age * -1, n.name RETURN n.name AS name, people",
        )
        .unwrap();
    assert_eq!(
        column(&output, "name"),
        strings(&["carol", "alice", "bob", "erin", "dave"])
    );
    // A key equal to a projected grouping expression, or built from one, is
    // also grouped.
    let output = db
        .query(
            "MATCH (n:Person) RETURN n.age * 2 AS doubled, count(*) AS people \
             ORDER BY n.age * 2 DESC",
        )
        .unwrap();
    assert_eq!(
        column(&output, "doubled"),
        vec![
            Value::Int(82),
            Value::Int(68),
            Value::Int(54),
            Value::Int(38)
        ]
    );
    assert_eq!(column(&output, "people")[2], Value::Int(2));
    let output = db
        .query(
            "MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people \
             ORDER BY lower(n.lastName) DESC",
        )
        .unwrap();
    assert_eq!(
        column(&output, "last"),
        strings(&["Smith", "Jones", "Brown"])
    );
    // Keys over projected names still work, and the groups are intact.
    let output = db
        .query(
            "MATCH (n:Person) RETURN n.lastName AS last, count(*) AS people \
             ORDER BY people * -1, lower(last)",
        )
        .unwrap();
    assert_eq!(
        column(&output, "last"),
        strings(&["Jones", "Smith", "Brown"])
    );
    assert_eq!(
        column(&output, "people"),
        vec![Value::Int(2), Value::Int(2), Value::Int(1)]
    );
}

#[test]
fn limited_order_keys_keep_top_n_plans() {
    let mut db = people();
    let property = "MATCH (n:Person) RETURN n.name AS name ORDER BY n.age DESC, n.name LIMIT 2";
    let plan = selected_plan(&db, property);
    assert!(plan.contains("TopNExec"), "{plan}");
    assert!(!plan.contains("SortExec"), "{plan}");
    assert!(
        !plan.contains("\0order") && !plan.contains("\\0order"),
        "{plan}"
    );
    assert_eq!(names(&mut db, property), strings(&["carol", "alice"]));

    let expression =
        "MATCH (n:Person) RETURN n.name AS name ORDER BY size(n.tags) DESC, n.age % 10 LIMIT 2";
    let plan = selected_plan(&db, expression);
    assert!(plan.contains("TopNExec"), "{plan}");
    assert!(!plan.contains("SortExec"), "{plan}");
    assert_eq!(names(&mut db, expression), strings(&["bob", "alice"]));
}
