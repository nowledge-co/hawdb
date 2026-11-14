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

use crate::{
    parse, AggregateExpression, AggregateFunctionName, ClauseKind, PropertyPredicate,
    QueryPipeline, ReturnExpressionKind, ScalarBinaryOp, ScalarExpression, ScalarExpressionKind,
    Statement, ValueExpressionKind,
};
use hawdb_core::Value;

fn pipeline(query: &str) -> QueryPipeline {
    let Statement::Pipeline(parsed) = parse(query).unwrap() else {
        panic!("expected a pipeline");
    };
    *parsed
}

fn match_predicates(query: &QueryPipeline) -> Vec<PropertyPredicate> {
    let ClauseKind::Match { predicate, .. } = &query.clauses[0].kind else {
        panic!("expected MATCH");
    };
    match predicate.as_ref().unwrap().kind.clone() {
        PropertyPredicate::And(predicates) => predicates,
        predicate => vec![predicate],
    }
}

/// The list operand of a generic `element IN list` WHERE predicate.
fn generic_in_list(predicate: &PropertyPredicate) -> &ScalarExpression {
    let PropertyPredicate::ExpressionEq { expression, .. } = predicate else {
        panic!("expected generic IN predicate, got {predicate:?}");
    };
    let ScalarExpressionKind::Binary {
        op: ScalarBinaryOp::In,
        right,
        ..
    } = &expression.kind
    else {
        panic!("expected IN, got {expression:?}");
    };
    right
}

fn list_elements(expression: &ScalarExpression) -> &[ScalarExpression] {
    let ScalarExpressionKind::List(elements) = &expression.kind else {
        panic!("expected row-bound list literal, got {expression:?}");
    };
    elements
}

#[test]
fn row_bound_list_literals_use_the_generic_in_operator() {
    let query = pipeline(
        "MATCH (n:Item), (a:Item), (b:Item) WHERE n IN [a, b] AND NOT n IN [a] \
         AND n.x IN [a.x, $p, 3, null] AND id(n) IN [id(a), 1] AND $p IN [a.x] RETURN n",
    );
    let predicates = match_predicates(&query);
    assert_eq!(predicates.len(), 5);

    let elements = list_elements(generic_in_list(&predicates[0]));
    assert!(matches!(
        elements,
        [left, right] if left.kind == ScalarExpressionKind::Variable("a".into())
            && right.kind == ScalarExpressionKind::Variable("b".into())
    ));
    let PropertyPredicate::Not(negated) = &predicates[1] else {
        panic!("expected NOT, got {:?}", predicates[1]);
    };
    assert_eq!(list_elements(generic_in_list(negated)).len(), 1);

    let elements = list_elements(generic_in_list(&predicates[2]));
    assert!(matches!(
        &elements[0].kind,
        ScalarExpressionKind::Property { variable, property } if variable == "a" && property == "x"
    ));
    let values: Vec<_> = elements[1..]
        .iter()
        .map(|element| match &element.kind {
            ScalarExpressionKind::Value(value) => value.kind.clone(),
            other => panic!("expected value element, got {other:?}"),
        })
        .collect();
    assert_eq!(
        values,
        [
            ValueExpressionKind::Parameter("p".into()),
            ValueExpressionKind::Literal(Value::Int(3)),
            ValueExpressionKind::Literal(Value::Null),
        ]
    );
    let elements = list_elements(generic_in_list(&predicates[3]));
    assert_eq!(elements[0].kind, ScalarExpressionKind::Id("a".into()));
    assert_eq!(list_elements(generic_in_list(&predicates[4])).len(), 1);
}

#[test]
fn constant_list_literals_keep_value_list_predicates() {
    let query = pipeline(
        "MATCH (n:Item) WHERE n.id IN [1, 2, 3] AND id(n) IN [1, 2] AND n.name IN [] \
         AND n.tags IN [[1, 2], [$p]] AND n IN $ids RETURN n",
    );
    let predicates = match_predicates(&query);
    assert!(
        matches!(&predicates[0], PropertyPredicate::In { values, .. }
        if matches!(&values.kind, ValueExpressionKind::List(values) if values.len() == 3))
    );
    assert!(matches!(&predicates[1], PropertyPredicate::IdIn { .. }));
    assert!(
        matches!(&predicates[2], PropertyPredicate::In { values, .. }
        if values.kind == ValueExpressionKind::List(Vec::new()))
    );
    assert!(matches!(&predicates[3], PropertyPredicate::In { .. }));
    assert!(matches!(
        &predicates[4],
        PropertyPredicate::ExpressionIn { values, .. }
            if values.kind == ValueExpressionKind::Parameter("ids".into())
    ));
}

#[test]
fn list_literals_of_expressions_parse_in_projections_case_and_aggregates() {
    let query = pipeline(
        "MATCH (n:Item) OPTIONAL MATCH (n)-[r:R]->(m:Item) \
         RETURN [n.name, r.year, m.name] AS l, [[n.name], [1, 2], []] AS nested, \
         [n.name = m.name, m IS NULL] AS flags, [1, 'a', null] AS constant, \
         CASE WHEN m.name IS NULL THEN null ELSE [m.name, r.year] END AS c, \
         CASE WHEN m.name IS NOT NULL THEN m.name ELSE [n.name] END AS fallback, \
         collect(CASE WHEN m.name IS NULL THEN null ELSE [m.name, r.year] END) AS cs, \
         collect([n.name, m.name]) AS pairs, size([n, m]) AS size",
    );
    let ClauseKind::Return(projection) = &query.clauses.last().unwrap().kind else {
        panic!("expected RETURN");
    };
    let item = |index: usize| &projection.items[index].expression.kind;
    let scalar = |index: usize| match item(index) {
        ReturnExpressionKind::Value(value) => value.clone(),
        other => panic!("expected scalar item {index}, got {other:?}"),
    };
    assert_eq!(list_elements(&scalar(0)).len(), 3);

    let nested = scalar(1);
    let nested = list_elements(&nested);
    assert_eq!(list_elements(&nested[0]).len(), 1);
    assert!(matches!(&nested[1].kind, ScalarExpressionKind::Value(value)
            if matches!(value.kind, ValueExpressionKind::List(ref values) if values.len() == 2)));
    assert!(matches!(&nested[2].kind, ScalarExpressionKind::Value(value)
            if value.kind == ValueExpressionKind::List(Vec::new())));

    let flags = scalar(2);
    let flags = list_elements(&flags);
    assert!(matches!(
        flags[0].kind,
        ScalarExpressionKind::Binary {
            op: ScalarBinaryOp::Eq,
            ..
        }
    ));
    assert!(matches!(
        flags[1].kind,
        ScalarExpressionKind::IsNull { negated: false, .. }
    ));
    assert!(matches!(&scalar(3).kind, ScalarExpressionKind::Value(value)
        if matches!(value.kind, ValueExpressionKind::List(_))));

    // Row-bound CASE results use the generic CASE, never a constant-only
    // specialized form.
    for index in [4, 5] {
        let ScalarExpressionKind::Case { otherwise, .. } = &scalar(index).kind else {
            panic!("expected generic CASE, got {:?}", scalar(index));
        };
        list_elements(otherwise.as_deref().unwrap());
    }
    for index in [6, 7] {
        let ReturnExpressionKind::Aggregate(AggregateExpression::Expression {
            function: AggregateFunctionName::Collect,
            argument,
            ..
        }) = item(index)
        else {
            panic!("expected collect expression, got {:?}", item(index));
        };
        let ReturnExpressionKind::Value(argument) = &argument.kind else {
            panic!("expected scalar collect argument");
        };
        match &argument.kind {
            ScalarExpressionKind::Case { otherwise, .. } => {
                list_elements(otherwise.as_deref().unwrap());
            }
            _ => {
                list_elements(argument);
            }
        }
    }
    let ScalarExpressionKind::ListFunction { arguments, .. } = &scalar(8).kind else {
        panic!("expected size()");
    };
    assert_eq!(list_elements(&arguments[0]).len(), 2);
}

#[test]
fn malformed_list_literals_are_rejected() {
    for query in [
        "MATCH (n:Item) RETURN [n.name AS l",
        "MATCH (n:Item) WHERE n IN [n, RETURN n",
    ] {
        assert!(parse(query).is_err(), "{query}");
    }
}
