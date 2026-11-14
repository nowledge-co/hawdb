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

//! Planning numeric functions, `reduce(...)` and `startNode`/`endNode` (#1487).

use super::*;
use hawdb_core::{NumericFunction, RelationshipEndpoint};

fn returns(query: &str, scope: &[&str], columns: &[&str]) -> Result<PlannedReturns> {
    let pipeline = hawdb_cypher::parse_pipeline(query).unwrap();
    let hawdb_cypher::ClauseKind::Return(projection) = &pipeline.clauses.last().unwrap().kind
    else {
        panic!("expected RETURN");
    };
    plan_return_items_with_columns(
        &scope.iter().map(|name| name.to_string()).collect(),
        &columns.iter().map(|name| name.to_string()).collect(),
        &projection.items,
        &BTreeMap::from([("start".to_string(), Value::Int(10))]),
    )
}

fn projections(query: &str, scope: &[&str], columns: &[&str]) -> Vec<Projection> {
    match returns(query, scope, columns).unwrap() {
        PlannedReturns::Projections(projections) => projections,
        _ => panic!("expected plain projections"),
    }
}

fn error(query: &str, scope: &[&str], columns: &[&str]) -> String {
    match returns(query, scope, columns) {
        Ok(_) => panic!("expected planning to fail: {query}"),
        Err(error) => error.to_string(),
    }
}

fn local(name: &str) -> ProjectionExpression {
    ProjectionExpression::LocalVariable {
        name: name.to_string(),
        property: None,
    }
}

#[test]
fn numeric_functions_plan_their_argument_and_default_name() {
    let items = projections("MATCH (n) RETURN floor(n.x), abs(n.y) AS a", &["n"], &[]);
    assert_eq!(items[0].name, "floor");
    assert_eq!(
        items[0].expression,
        ProjectionExpression::NumericFunction {
            function: NumericFunction::Floor,
            argument: Box::new(ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "x".to_string(),
            }),
        }
    );
    assert_eq!(items[1].name, "a");
}

#[test]
fn reduce_binds_accumulator_and_element_as_locals() {
    let items = projections(
        "MATCH (n) RETURN reduce(total = $start, x IN xs | total + x * n.k) AS v",
        &["n"],
        &["xs"],
    );
    let ProjectionExpression::Reduce(reduce) = &items[0].expression else {
        panic!("expected reduce, got {:?}", items[0].expression);
    };
    assert_eq!(
        (reduce.accumulator.as_str(), reduce.variable.as_str()),
        ("total", "x")
    );
    assert_eq!(
        reduce.initial,
        ProjectionExpression::Literal(Value::Int(10))
    );
    assert_eq!(reduce.list, ProjectionExpression::Column("xs".to_string()));
    let ProjectionExpression::Binary { left, right, .. } = &reduce.expression else {
        panic!("expected addition");
    };
    assert_eq!(left.as_ref(), &local("total"));
    let ProjectionExpression::Binary { left, right: k, .. } = right.as_ref() else {
        panic!("expected multiplication");
    };
    assert_eq!(left.as_ref(), &local("x"));
    assert!(
        matches!(k.as_ref(), ProjectionExpression::Property { variable, .. } if variable == "n")
    );

    // An enclosing comprehension element is visible to the initial value,
    // the list and the body, and is bound by the comprehension.
    let items = projections(
        "MATCH (n) RETURN [r IN rs | reduce(w = r, v IN [r, 1] | w + v + r)] AS v",
        &["n"],
        &["rs"],
    );
    let ProjectionExpression::ListComprehension(comprehension) = &items[0].expression else {
        panic!("expected comprehension");
    };
    let Some(ProjectionExpression::Reduce(reduce)) = &comprehension.projection else {
        panic!("expected reduce");
    };
    assert_eq!(reduce.initial, local("r"));
    assert_eq!(
        reduce.list,
        ProjectionExpression::List(vec![
            local("r"),
            ProjectionExpression::Literal(Value::Int(1)),
        ])
    );
}

#[test]
fn reduce_rejects_shadowing_and_out_of_scope_locals() {
    assert!(error(
        "MATCH (n) RETURN reduce(n = 0, x IN [1] | n + x) AS v",
        &["n"],
        &[]
    )
    .contains("reduce() variable 'n' shadows a variable that is already in scope"));
    assert!(error(
        "MATCH (n) RETURN reduce(s = 0, xs IN [1] | s) AS v",
        &["n"],
        &["xs"]
    )
    .contains("reduce() variable 'xs' shadows"));
    assert!(error(
        "MATCH (n) RETURN reduce(x = 0, x IN [1] | x) AS v",
        &["n"],
        &[]
    )
    .contains("reduce() accumulator and list variable must differ"));
    // The fold's own names are not visible to its initial value or list.
    assert!(error(
        "MATCH (n) RETURN reduce(s = s, x IN [1] | s) AS v",
        &["n"],
        &[]
    )
    .contains("unknown"));
    assert!(error(
        "MATCH (n) RETURN reduce(s = 0, x IN [x] | s) AS v",
        &["n"],
        &[]
    )
    .contains("unknown"));
}

#[test]
fn relationship_endpoints_plan_over_variables_and_values() {
    let items = projections(
        "MATCH (a)-[r]->(b) RETURN startNode(r), endNode(r).id AS e",
        &["a", "r", "b"],
        &[],
    );
    assert_eq!(items[0].name, "startNode(r)");
    assert_eq!(
        items[0].expression,
        ProjectionExpression::RelationshipEndpoint {
            endpoint: RelationshipEndpoint::Start,
            variable: "r".to_string(),
            endpoints: None,
        }
    );
    assert!(matches!(
        &items[1].expression,
        ProjectionExpression::PropertyAccess { expression, key } if key == "id"
            && matches!(expression.as_ref(), ProjectionExpression::RelationshipEndpoint {
                endpoint: RelationshipEndpoint::End, ..
            })
    ));
    assert!(
        error("MATCH (a) RETURN startNode(q) AS s", &["a"], &[]).contains("unknown variable 'q'")
    );
    // A relationship value, such as a list element, reads its endpoint from
    // the graph by the id it records (#1488).
    let items = projections(
        "MATCH (a) RETURN [r IN rs | endNode(r)] AS s",
        &["a"],
        &["rs"],
    );
    let ProjectionExpression::ListComprehension(comprehension) = &items[0].expression else {
        panic!("expected a comprehension: {items:?}");
    };
    assert_eq!(
        comprehension.projection,
        Some(ProjectionExpression::RelationshipValueEndpoint {
            endpoint: RelationshipEndpoint::End,
            relationship: Box::new(local("r")),
            visibility: None,
        })
    );
}

#[test]
fn relationship_endpoints_resolve_to_the_binding_step_nodes() {
    let plan = |query: &str| {
        crate::plan_pipeline_query(query, &BTreeMap::new()).map(|plan| format!("{plan:?}"))
    };
    for (query, expected) in [
        // `a` binds the same node as `x` in a cycle, but only the step's own
        // endpoint variables are candidates.
        (
            "MATCH (x)-[r:R]->(y)<-[:KNOWS]-(a) RETURN a.name AS n, startNode(r) AS s",
            r#"RelationshipEndpoint { endpoint: Start, variable: "r", endpoints: Some(["x", "y"]) }"#,
        ),
        (
            "MATCH (y)<-[r:R]-(x) WHERE endNode(r).id = 1 RETURN y",
            r#"RelationshipEndpoint { endpoint: End, variable: "r", endpoints: Some(["y", "x"]) }"#,
        ),
        (
            "MATCH (p) OPTIONAL MATCH (p)-[r:R]->(q) RETURN endNode(r) AS e",
            r#"RelationshipEndpoint { endpoint: End, variable: "r", endpoints: Some(["p", "q"]) }"#,
        ),
    ] {
        let plan = plan(query).unwrap();
        assert!(plan.contains(expected), "{query}: {plan}");
        assert!(!plan.contains("endpoints: None"), "{query}: {plan}");
    }
    // After WITH, ORDER BY reads the projected relationship value, whose
    // endpoint is read from the graph (#1488: the projected scope wins).
    let sorted =
        plan("MATCH (a)-[r:R]-(b) WITH a, r, b ORDER BY startNode(r).id RETURN a").unwrap();
    assert!(
        sorted
            .contains(r#"RelationshipValueEndpoint { endpoint: Start, relationship: Column("r")"#),
        "{sorted}"
    );
    // A variable-length relationship variable is rejected before resolution.
    let error = plan("MATCH (a)-[r:R*1..2]->(b) RETURN startNode(r) AS s")
        .unwrap_err()
        .to_string();
    assert!(error.contains("one-hop"), "{error}");
}
