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
use hawdb_core::ListFunction;

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
        &BTreeMap::from([
            ("limit".to_string(), Value::Int(2)),
            ("values".to_string(), Value::List(vec![Value::Int(7)])),
        ]),
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

fn local(name: &str, property: Option<&str>) -> ProjectionExpression {
    ProjectionExpression::LocalVariable {
        name: name.to_string(),
        property: property.map(str::to_string),
    }
}

#[test]
fn comprehension_binds_its_element_as_a_local_and_keeps_outer_references() {
    let items = projections(
        "MATCH (n:Item) RETURN [x IN tags WHERE x.score > n.min | x.name] AS names",
        &["n"],
        &["tags"],
    );
    let ProjectionExpression::ListComprehension(comprehension) = &items[0].expression else {
        panic!("expected comprehension");
    };
    assert_eq!(comprehension.variable, "x");
    assert_eq!(
        comprehension.list,
        ProjectionExpression::Column("tags".to_string())
    );
    assert_eq!(comprehension.projection, Some(local("x", Some("name"))));
    let Some(ProjectionExpression::Binary { left, right, .. }) = &comprehension.predicate else {
        panic!("expected comparison");
    };
    assert_eq!(left.as_ref(), &local("x", Some("score")));
    assert_eq!(
        right.as_ref(),
        &ProjectionExpression::Property {
            variable: "n".to_string(),
            property: "min".to_string(),
        }
    );
    assert_eq!(items[0].name, "names");

    // Nested comprehensions see both elements; the parameter becomes a literal.
    let items = projections(
        "MATCH (n:Item) RETURN [x IN n.groups | [y IN x WHERE y IN $values | x]] AS nested",
        &["n"],
        &[],
    );
    let ProjectionExpression::ListComprehension(outer) = &items[0].expression else {
        panic!("expected comprehension");
    };
    let Some(ProjectionExpression::ListComprehension(inner)) = &outer.projection else {
        panic!("expected nested comprehension");
    };
    assert_eq!(inner.list, local("x", None));
    assert_eq!(inner.projection, Some(local("x", None)));
    assert_eq!(
        inner.predicate,
        Some(ProjectionExpression::Binary {
            left: Box::new(local("y", None)),
            op: ScalarBinaryOp::In,
            right: Box::new(ProjectionExpression::Literal(Value::List(vec![
                Value::Int(7)
            ]))),
        })
    );
}

#[test]
fn comprehension_variables_reject_shadowing_and_do_not_leak() {
    for (query, scope, columns) in [
        (
            "MATCH (n) RETURN [n IN [1] | n] AS bad",
            &["n"][..],
            &[][..],
        ),
        ("MATCH (n) RETURN [c IN [1] | c] AS bad", &["n"], &["c"]),
        (
            "MATCH (n) RETURN [x IN [[1]] | [x IN x | x]] AS bad",
            &["n"],
            &[],
        ),
    ] {
        let error = error(query, scope, columns);
        assert!(
            error.contains("shadows a variable that is already in scope"),
            "{query}: {error}"
        );
    }
    let error = error(
        "MATCH (n) RETURN [x IN [1] | x] AS ok, x AS leaked",
        &["n"],
        &[],
    );
    assert!(error.contains("unknown variable 'x'"), "{error}");
    // Graph-only functions cannot treat a list element as a graph variable.
    assert!(returns("MATCH (n) RETURN [x IN [1] | id(x)] AS bad", &["n"], &[]).is_err());
}

#[test]
fn list_functions_index_and_slice_plan_to_projection_nodes() {
    let items = projections(
        "MATCH (n) RETURN size(n.tags) AS size, range(0, $limit) AS r, n.tags[-1] AS last, n.tags[1..] AS rest",
        &["n"],
        &[],
    );
    assert_eq!(
        items[0].expression,
        ProjectionExpression::ListFunction {
            function: ListFunction::Size,
            arguments: vec![ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "tags".to_string(),
            }],
        }
    );
    assert_eq!(
        items[1].expression,
        ProjectionExpression::ListFunction {
            function: ListFunction::Range,
            arguments: vec![
                ProjectionExpression::Literal(Value::Int(0)),
                ProjectionExpression::Literal(Value::Int(2)),
            ],
        }
    );
    assert!(matches!(
        &items[2].expression,
        ProjectionExpression::ListIndex { index, .. }
            if index.as_ref() == &ProjectionExpression::Literal(Value::Int(-1))
    ));
    assert!(matches!(
        &items[3].expression,
        ProjectionExpression::ListSlice {
            start: Some(_),
            end: None,
            ..
        }
    ));
    let unaliased = projections("MATCH (n) RETURN head(n.tags), [x IN n.tags]", &["n"], &[]);
    assert_eq!(unaliased[0].name, "head");
    assert_eq!(unaliased[1].name, "list_comprehension");
}

#[test]
fn list_functions_over_aggregates_project_after_aggregation() {
    let Ok(PlannedReturns::AggregateProjection {
        group_keys,
        items,
        projections,
    }) = returns(
        "MATCH (n:Item) RETURN n.kind AS kind, size(collect(n.id)) AS total, head(collect(n.id)) AS first",
        &["n"],
        &[],
    )
    else {
        panic!("expected aggregate projection");
    };
    assert_eq!(group_keys.len(), 1);
    assert_eq!(items.len(), 2);
    assert_eq!(
        projections[1].expression,
        ProjectionExpression::ListFunction {
            function: ListFunction::Size,
            arguments: vec![ProjectionExpression::Column(items[0].name.clone())],
        }
    );
    assert_eq!(projections[1].name, "total");
    assert!(matches!(
        &projections[2].expression,
        ProjectionExpression::ListFunction {
            function: ListFunction::Head,
            ..
        }
    ));
}

#[test]
fn where_and_order_by_plan_list_expressions_through_full_pipelines() {
    let plan = plan_pipeline_query(
        "MATCH (n:Item) WITH n, n.tags AS tags WHERE size(tags) > 1 AND n.id IN tags \
         RETURN n.id AS id, [t IN tags WHERE t <> 'x' | t] AS kept ORDER BY tags[0] DESC",
        &BTreeMap::new(),
    )
    .unwrap();
    let rendered = format!("{plan:?}");
    assert!(rendered.contains("ListFunction"), "{rendered}");
    assert!(rendered.contains("op: In"), "{rendered}");
    assert!(rendered.contains("LocalVariable"), "{rendered}");
    assert!(rendered.contains("ListIndex"), "{rendered}");
}

#[test]
fn fingerprints_distinguish_list_expression_kinds() {
    let list = || Box::new(ProjectionExpression::Column("xs".to_string()));
    let literal = |value| Box::new(ProjectionExpression::Literal(Value::Int(value)));
    let mut expressions = ListFunction::ALL
        .into_iter()
        .map(|function| ProjectionExpression::ListFunction {
            function,
            arguments: vec![*list()],
        })
        .collect::<Vec<_>>();
    expressions.extend([
        ProjectionExpression::ListIndex {
            list: list(),
            index: literal(0),
        },
        ProjectionExpression::ListSlice {
            list: list(),
            start: Some(literal(0)),
            end: None,
        },
        ProjectionExpression::ListSlice {
            list: list(),
            start: None,
            end: Some(literal(0)),
        },
        ProjectionExpression::Binary {
            left: literal(0),
            op: ScalarBinaryOp::In,
            right: list(),
        },
        ProjectionExpression::Binary {
            left: literal(0),
            op: ScalarBinaryOp::ListContains,
            right: list(),
        },
        local("x", None),
        local("x", Some("p")),
        ProjectionExpression::Column("x".to_string()),
    ]);
    for (predicate, projection) in [
        (None, None),
        (Some(local("x", None)), None),
        (None, Some(local("x", None))),
    ] {
        expressions.push(ProjectionExpression::ListComprehension(Box::new(
            ListComprehensionProjection {
                variable: "x".to_string(),
                list: *list(),
                predicate,
                projection,
            },
        )));
    }
    let rendered = expressions
        .iter()
        .map(|expression| {
            let mut output = String::new();
            crate::write_projection_expression(&mut output, expression);
            output
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(rendered.len(), expressions.len(), "{rendered:?}");
}
