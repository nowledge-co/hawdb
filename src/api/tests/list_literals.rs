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

//! List literals whose elements are row-bound expressions.

use super::*;

fn string(value: &str) -> Value {
    Value::String(value.to_string())
}

fn strings(values: &[&str]) -> Vec<Value> {
    values.iter().map(|value| string(value)).collect()
}

fn column(output: &QueryOutput, name: &str) -> Vec<Value> {
    output.rows.iter().map(|row| row[name].clone()).collect()
}

fn run(db: &mut Database, statements: &[&str]) {
    for statement in statements {
        db.query(statement).unwrap();
    }
}

/// Three countries with one city each, plus two cities sharing every
/// property so that only node identity tells them apart.
fn places() -> Database {
    let mut db = Database::new();
    run(
        &mut db,
        &[
            "CREATE (:Place {type: 'country', name: 'X'})",
            "CREATE (:Place {type: 'country', name: 'Y'})",
            "CREATE (:Place {type: 'country', name: 'Z'})",
        ],
    );
    for (city, country) in [("cx", "X"), ("cy", "Y"), ("cz", "Z")] {
        db.query(&format!("CREATE (:Place {{type: 'city', name: '{city}'}})"))
            .unwrap();
        db.query(&format!(
            "MATCH (city:Place {{name: '{city}'}}), (country:Place {{name: '{country}'}}) \
             CREATE (city)-[:IS_PART_OF]->(country)"
        ))
        .unwrap();
    }
    for _ in 0..2 {
        db.query(
            "CREATE (:Place {type: 'city', name: 'twin', rank: 1})-[:IS_PART_OF]->(:Region {name: 'r'})",
        )
        .unwrap();
    }
    db
}

fn country_parameters(x: &str, y: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("countryXName".to_string(), string(x)),
        ("countryYName".to_string(), string(y)),
    ])
}

#[test]
fn ic3_country_in_list_of_bound_nodes_filters_by_identity() {
    let mut db = places();
    // The IC3 shape: the list holds node variables bound in the same row.
    let query = "MATCH (countryX:Place {type: 'country', name: $countryXName}), \
                 (countryY:Place {type: 'country', name: $countryYName}) \
                 WITH countryX, countryY LIMIT 1 \
                 MATCH (city:Place {type: 'city'})-[:IS_PART_OF]->(country:Place {type: 'country'}) \
                 WHERE country IN [countryX, countryY] \
                 WITH countryX, countryY, collect(city.name) AS cities \
                 RETURN cities";
    let output = db
        .query_with_params(query, &country_parameters("X", "Y"))
        .unwrap();
    let Value::List(mut cities) = output.rows[0]["cities"].clone() else {
        panic!("expected a list, got {:?}", output.rows);
    };
    cities.sort();
    assert_eq!(cities, strings(&["cx", "cy"]));

    // Other parameters select other countries, and so other cities.
    let output = db
        .query_with_params(query, &country_parameters("Y", "Y"))
        .unwrap();
    assert_eq!(output.rows[0]["cities"], Value::List(strings(&["cy"])));

    // NOT IN and IN combined with other conjuncts.
    let output = db
        .query_with_params(
            "MATCH (countryX:Place {type: 'country', name: $countryXName}), \
             (countryY:Place {type: 'country', name: $countryYName}) \
             MATCH (city:Place {type: 'city'})-[:IS_PART_OF]->(country:Place) \
             WHERE NOT country IN [countryX, countryY] AND city.name <> 'twin' \
             RETURN city.name AS name ORDER BY name",
            &country_parameters("X", "Y"),
        )
        .unwrap();
    assert_eq!(column(&output, "name"), strings(&["cz"]));
    let output = db
        .query_with_params(
            "MATCH (countryX:Place {type: 'country', name: $countryXName}), \
             (countryY:Place {type: 'country', name: $countryYName}) \
             MATCH (city:Place {type: 'city'})-[:IS_PART_OF]->(country:Place) \
             WHERE country IN [countryX, countryY] AND city.name <> 'cy' \
             RETURN city.name AS name ORDER BY name",
            &country_parameters("X", "Y"),
        )
        .unwrap();
    assert_eq!(column(&output, "name"), strings(&["cx"]));

    // Equal properties and labels are not identity: each twin is only in
    // its own singleton list, and relationships compare the same way. The
    // relationships are matched separately so uniqueness does not apply.
    let output = db
        .query(
            "MATCH (a:Place {name: 'twin'})-[r:IS_PART_OF]->(c:Region) \
             MATCH (b:Place {name: 'twin'})-[s:IS_PART_OF]->(d:Region) \
             WHERE a IN [b] AND r IN [s, s] RETURN count(*) AS pairs",
        )
        .unwrap();
    assert_eq!(output.rows[0]["pairs"], Value::Int(2));
    let output = db
        .query(
            "MATCH (a:Place {name: 'twin'}), (b:Place {name: 'twin'}) \
             WHERE NOT a IN [b] RETURN count(*) AS pairs",
        )
        .unwrap();
    assert_eq!(output.rows[0]["pairs"], Value::Int(2));
}

#[test]
fn mixed_list_elements_use_three_valued_in() {
    let mut db = Database::new();
    for x in [1, 2, 3, 4] {
        db.query(&format!("CREATE (:N {{x: {x}}})")).unwrap();
    }
    db.query("CREATE (:M {x: 2})").unwrap();
    let parameters = |p: i64| BTreeMap::from([("p".to_string(), Value::Int(p))]);
    let query = "MATCH (n:N), (m:M) WHERE n.x IN [m.x, $p, 4] RETURN n.x AS x ORDER BY x";
    let output = db.query_with_params(query, &parameters(1)).unwrap();
    assert_eq!(
        column(&output, "x"),
        [Value::Int(1), Value::Int(2), Value::Int(4)]
    );
    let output = db.query_with_params(query, &parameters(3)).unwrap();
    assert_eq!(
        column(&output, "x"),
        [Value::Int(2), Value::Int(3), Value::Int(4)]
    );
    let before = db.plan_cache_stats().unwrap();
    let output = db.query_with_params(query, &parameters(1)).unwrap();
    assert_eq!(
        column(&output, "x"),
        [Value::Int(1), Value::Int(2), Value::Int(4)]
    );
    assert_eq!(db.plan_cache_stats().unwrap().hits, before.hits + 1);

    // A NULL element makes a miss unknown, so neither IN nor NOT IN keeps it.
    let output = db
        .query("MATCH (n:N), (m:M) WHERE n.x IN [m.missing, 1] RETURN n.x AS x")
        .unwrap();
    assert_eq!(column(&output, "x"), [Value::Int(1)]);
    let output = db
        .query("MATCH (n:N), (m:M) WHERE NOT n.x IN [m.missing, 1] RETURN n.x AS x")
        .unwrap();
    assert!(output.rows.is_empty(), "{:?}", output.rows);
    let output = db
        .query("MATCH (n:N), (m:M) WHERE NOT n.x IN [m.x, 1] RETURN n.x AS x ORDER BY x")
        .unwrap();
    assert_eq!(column(&output, "x"), [Value::Int(3), Value::Int(4)]);
    let output = db
        .query(
            "MATCH (n:N), (m:M) RETURN n.x AS x, \
             CASE WHEN n.x IN [m.missing, 1] THEN 'yes' WHEN NOT (n.x IN [m.missing, 1]) \
             THEN 'no' ELSE 'unknown' END AS member ORDER BY x",
        )
        .unwrap();
    assert_eq!(
        column(&output, "member"),
        strings(&["yes", "unknown", "unknown", "unknown"])
    );
    // id() and parameters on the left use the same operator.
    let output = db
        .query_with_params(
            "MATCH (n:N), (m:M) WHERE n.x = 2 AND id(n) IN [id(m), id(n)] AND $p IN [m.x, 9] \
             RETURN n.x AS x",
            &parameters(2),
        )
        .unwrap();
    assert_eq!(column(&output, "x"), [Value::Int(2)]);
}

#[test]
fn list_literal_projections_keep_optional_match_nulls_and_nesting() {
    let mut db = Database::new();
    run(
        &mut db,
        &[
            "CREATE (:Person {name: 'a'})-[:STUDY_AT {classYear: 2001}]->(:Uni {name: 'u1'})",
            "CREATE (:Person {name: 'b'})",
        ],
    );
    let output = db
        .query(
            "MATCH (p:Person) OPTIONAL MATCH (p)-[s:STUDY_AT]->(u:Uni) \
             RETURN p.name AS name, [p.name, s.classYear, u.name] AS l, \
             [[p.name, 1], [], [u.name]] AS nested, [] AS empty, [1, [2, 3]] AS constant, \
             size([p, u]) AS size, \
             CASE WHEN [p.name, u][1] IS NULL THEN true ELSE false END AS missing, \
             CASE WHEN u.name IS NULL THEN null ELSE [u.name, s.classYear] END AS c \
             ORDER BY name",
        )
        .unwrap();
    let row = &output.rows[0];
    assert_eq!(
        row["l"],
        Value::List(vec![string("a"), Value::Int(2001), string("u1")])
    );
    assert_eq!(
        row["nested"],
        Value::List(vec![
            Value::List(vec![string("a"), Value::Int(1)]),
            Value::List(Vec::new()),
            Value::List(vec![string("u1")]),
        ])
    );
    assert_eq!(row["empty"], Value::List(Vec::new()));
    assert_eq!(
        row["constant"],
        Value::List(vec![
            Value::Int(1),
            Value::List(vec![Value::Int(2), Value::Int(3)])
        ])
    );
    assert_eq!(row["size"], Value::Int(2));
    assert_eq!(row["missing"], Value::Bool(false));
    assert_eq!(row["c"], Value::List(vec![string("u1"), Value::Int(2001)]));

    let row = &output.rows[1];
    assert_eq!(
        row["l"],
        Value::List(vec![string("b"), Value::Null, Value::Null])
    );
    assert_eq!(
        row["nested"],
        Value::List(vec![
            Value::List(vec![string("b"), Value::Int(1)]),
            Value::List(Vec::new()),
            Value::List(vec![Value::Null]),
        ])
    );
    assert_eq!(row["size"], Value::Int(2));
    assert_eq!(row["missing"], Value::Bool(true));
    assert_eq!(row["c"], Value::Null);

    // A node element is the same value as projecting the node itself.
    let nodes = db
        .query("MATCH (p:Person {name: 'a'}) RETURN [p][0] AS first, p AS node")
        .unwrap();
    assert_eq!(nodes.rows[0]["first"], nodes.rows[0]["node"]);
}

#[test]
fn ic1_collect_case_list_literals_group_per_friend() {
    let mut db = Database::new();
    run(
        &mut db,
        &[
            "CREATE (:Person {id: 1, lastName: 'A'})",
            "CREATE (:Person {id: 2, lastName: 'B'})",
            "CREATE (:Organisation {type: 'university', name: 'u1'})-[:IS_LOCATED_IN]->\
             (:Place {type: 'city', name: 'c1'})",
            "CREATE (:Organisation {type: 'university', name: 'u2'})-[:IS_LOCATED_IN]->\
             (:Place {type: 'city', name: 'c2'})",
            "MATCH (f:Person {id: 1}), (u:Organisation {name: 'u1'}) \
             CREATE (f)-[:STUDY_AT {classYear: 2001}]->(u)",
            "MATCH (f:Person {id: 1}), (u:Organisation {name: 'u2'}) \
             CREATE (f)-[:STUDY_AT {classYear: 2005}]->(u)",
        ],
    );
    let output = db
        .query(
            "MATCH (friend:Person) \
             OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:Organisation {type: 'university'})\
             -[:IS_LOCATED_IN]->(uniCity:Place {type: 'city'}) \
             WITH friend, collect(\
                 CASE \
                     WHEN uni.name IS NULL THEN null \
                     ELSE [uni.name, studyAt.classYear, uniCity.name] \
                 END) AS unis \
             RETURN friend.id AS id, unis ORDER BY id",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    let Value::List(mut unis) = output.rows[0]["unis"].clone() else {
        panic!("expected a list, got {:?}", output.rows[0]);
    };
    unis.sort();
    assert_eq!(
        unis,
        [
            Value::List(vec![string("u1"), Value::Int(2001), string("c1")]),
            Value::List(vec![string("u2"), Value::Int(2005), string("c2")]),
        ]
    );
    // collect() skips the NULL produced for a friend without a university.
    assert_eq!(output.rows[1]["unis"], Value::List(Vec::new()));

    let output = db
        .query(
            "MATCH (friend:Person) OPTIONAL MATCH (friend)-[s:STUDY_AT]->(uni:Organisation) \
             RETURN friend.id AS id, collect([uni.name, s.classYear]) AS pairs ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        output.rows[1]["pairs"],
        Value::List(vec![Value::List(vec![Value::Null, Value::Null])])
    );
}

#[test]
fn constant_in_lists_keep_index_multi_seek() {
    let mut db = Database::new();
    for id in 0..32 {
        db.query(&format!("CREATE (:Memory {{id: 'm{id}'}})"))
            .unwrap();
    }
    db.query("CREATE (:Source {id: 'm3'})").unwrap();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.id IN ['m1', 'm2'] RETURN m.id AS id")
        .unwrap();
    assert!(
        explain
            .physical_plan
            .explain(0)
            .contains("IndexNodeMultiSeek"),
        "{}",
        explain.physical_plan.explain(0)
    );
    // A row-bound element is evaluated per row instead.
    let output = db
        .query(
            "MATCH (s:Source), (m:Memory) WHERE m.id IN [s.id, 'm1'] \
             RETURN m.id AS id ORDER BY id",
        )
        .unwrap();
    assert_eq!(column(&output, "id"), strings(&["m1", "m3"]));
}
