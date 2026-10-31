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
    parse, AstNode, ClauseKind, ComparisonOp, PropertyPredicate, ReturnExpressionKind,
    ScalarBinaryOp, ScalarExpression, ScalarExpressionKind, Statement, ValueExpression,
    ValueExpressionKind,
};
use hawdb_core::{HawDBError, Value};

fn where_predicate(query: &str) -> PropertyPredicate {
    let Statement::Pipeline(parsed) = parse(query).unwrap() else {
        panic!("expected a pipeline: {query}");
    };
    parsed
        .clauses
        .iter()
        .find_map(|clause| match &clause.kind {
            ClauseKind::Match { predicate, .. } => predicate.clone(),
            ClauseKind::With(projection) => projection.predicate.clone(),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected a WHERE predicate: {query}"))
        .kind
}

fn returned_scalar(query: &str) -> ScalarExpression {
    let Statement::Pipeline(parsed) = parse(query).unwrap() else {
        panic!("expected a pipeline: {query}");
    };
    let ClauseKind::Return(projection) = &parsed.clauses.last().unwrap().kind else {
        panic!("expected a concluding RETURN clause");
    };
    match &projection.items[0].expression.kind {
        ReturnExpressionKind::Value(value) => value.clone(),
        other => panic!("expected scalar return item, got {other:?}"),
    }
}

fn value(kind: ValueExpressionKind) -> ValueExpression {
    AstNode::synthetic(kind)
}

fn param(name: &str) -> ValueExpression {
    value(ValueExpressionKind::Parameter(name.to_string()))
}

fn int(value_: i64) -> ValueExpression {
    value(ValueExpressionKind::Literal(Value::Int(value_)))
}

fn compare(
    variable: &str,
    property: &str,
    op: ComparisonOp,
    value: ValueExpression,
) -> PropertyPredicate {
    PropertyPredicate::Compare {
        variable: variable.to_string(),
        property: property.to_string(),
        op,
        value,
    }
}

fn scalar(kind: ScalarExpressionKind) -> ScalarExpression {
    AstNode::synthetic(kind)
}

fn property(variable: &str, property: &str) -> ScalarExpression {
    scalar(ScalarExpressionKind::Property {
        variable: variable.to_string(),
        property: property.to_string(),
    })
}

fn scalar_value(value: ValueExpression) -> ScalarExpression {
    scalar(ScalarExpressionKind::Value(value))
}

fn binary(left: ScalarExpression, op: ScalarBinaryOp, right: ScalarExpression) -> ScalarExpression {
    scalar(ScalarExpressionKind::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

fn case_when(condition: ScalarExpression) -> ScalarExpression {
    scalar(ScalarExpressionKind::Case {
        operand: None,
        branches: vec![(condition, scalar_value(int(1)))],
        otherwise: Some(Box::new(scalar_value(int(0)))),
    })
}

#[test]
fn where_value_on_left_mirrors_onto_property_predicates() {
    for (predicate, expected) in [
        (
            "$startDate > p.creationDate",
            compare("p", "creationDate", ComparisonOp::Lt, param("startDate")),
        ),
        (
            "$startDate >= p.creationDate",
            compare("p", "creationDate", ComparisonOp::Lte, param("startDate")),
        ),
        ("5 < p.x", compare("p", "x", ComparisonOp::Gt, int(5))),
        ("-5 <= p.x", compare("p", "x", ComparisonOp::Gte, int(-5))),
        (
            "'a' = p.name",
            PropertyPredicate::Eq {
                variable: "p".to_string(),
                property: "name".to_string(),
                value: value(ValueExpressionKind::Literal(Value::String("a".to_string()))),
            },
        ),
        (
            "$name <> p.name",
            PropertyPredicate::NotEq {
                variable: "p".to_string(),
                property: "name".to_string(),
                value: param("name"),
            },
        ),
        // Single comparisons keep their established forms.
        ("p.x < 5", compare("p", "x", ComparisonOp::Lt, int(5))),
        (
            "$flag = true",
            PropertyPredicate::ParameterEq {
                left: "flag".to_string(),
                right: value(ValueExpressionKind::Literal(Value::Bool(true))),
            },
        ),
        (
            "1 < $limit",
            PropertyPredicate::ExpressionCompare {
                expression: scalar_value(int(1)),
                op: ComparisonOp::Lt,
                value: scalar_value(param("limit")),
            },
        ),
    ] {
        let query = format!("MATCH (p:Post) WHERE {predicate} RETURN p");
        assert_eq!(where_predicate(&query), expected, "{query}");
    }
}

#[test]
fn where_comparison_chains_lower_to_conjunctions() {
    for (predicate, expected) in [
        (
            "$endDate > m.creationDate >= $startDate",
            vec![
                compare("m", "creationDate", ComparisonOp::Lt, param("endDate")),
                compare("m", "creationDate", ComparisonOp::Gte, param("startDate")),
            ],
        ),
        (
            "1 <= m.x < 10",
            vec![
                compare("m", "x", ComparisonOp::Gte, int(1)),
                compare("m", "x", ComparisonOp::Lt, int(10)),
            ],
        ),
        (
            "m.x > 1 > m.y",
            vec![
                compare("m", "x", ComparisonOp::Gt, int(1)),
                compare("m", "y", ComparisonOp::Lt, int(1)),
            ],
        ),
        (
            "0 < m.x <= m.y < $max",
            vec![
                compare("m", "x", ComparisonOp::Gt, int(0)),
                PropertyPredicate::ExpressionCompare {
                    expression: property("m", "x"),
                    op: ComparisonOp::Lte,
                    value: property("m", "y"),
                },
                compare("m", "y", ComparisonOp::Lt, param("max")),
            ],
        ),
        (
            "m.x = m.y = 3",
            vec![
                PropertyPredicate::ExpressionEq {
                    expression: property("m", "x"),
                    value: property("m", "y"),
                },
                PropertyPredicate::Eq {
                    variable: "m".to_string(),
                    property: "y".to_string(),
                    value: int(3),
                },
            ],
        ),
        (
            "0 < m.x <> 5",
            vec![
                compare("m", "x", ComparisonOp::Gt, int(0)),
                PropertyPredicate::NotEq {
                    variable: "m".to_string(),
                    property: "x".to_string(),
                    value: int(5),
                },
            ],
        ),
        (
            "coalesce(m.x, 0) < m.y < 5",
            vec![
                PropertyPredicate::ExpressionCompare {
                    expression: scalar(ScalarExpressionKind::Coalesce(vec![
                        property("m", "x"),
                        scalar_value(int(0)),
                    ])),
                    op: ComparisonOp::Lt,
                    value: property("m", "y"),
                },
                compare("m", "y", ComparisonOp::Lt, int(5)),
            ],
        ),
    ] {
        let query = format!("MATCH (m:Message) WHERE {predicate} RETURN m");
        assert_eq!(
            where_predicate(&query),
            PropertyPredicate::And(expected),
            "{query}"
        );
    }
}

#[test]
fn where_comparison_chains_bind_tighter_than_boolean_operators() {
    let range = |low: i64, high: i64| {
        PropertyPredicate::And(vec![
            compare("n", "x", ComparisonOp::Gt, int(low)),
            compare("n", "x", ComparisonOp::Lt, int(high)),
        ])
    };
    assert_eq!(
        where_predicate("MATCH (n:Item) WHERE NOT 1 < n.x < 3 OR n.y = 1 AND 0 < n.x < 9 RETURN n"),
        PropertyPredicate::Or(vec![
            PropertyPredicate::Not(Box::new(range(1, 3))),
            PropertyPredicate::And(vec![
                PropertyPredicate::Eq {
                    variable: "n".to_string(),
                    property: "y".to_string(),
                    value: int(1),
                },
                range(0, 9),
            ]),
        ])
    );
    assert_eq!(
        where_predicate("MATCH (n:Item) WITH n WHERE (0 < n.x < 9) AND n.y IS NULL RETURN n"),
        PropertyPredicate::And(vec![
            range(0, 9),
            PropertyPredicate::IsNull {
                variable: "n".to_string(),
                property: "y".to_string(),
            },
        ])
    );
    assert_eq!(
        where_predicate("MATCH (n:Item) WHERE n.x > null >= 1 RETURN n"),
        PropertyPredicate::And(vec![
            compare(
                "n",
                "x",
                ComparisonOp::Gt,
                value(ValueExpressionKind::Literal(Value::Null))
            ),
            PropertyPredicate::ExpressionCompare {
                expression: scalar_value(value(ValueExpressionKind::Literal(Value::Null))),
                op: ComparisonOp::Gte,
                value: scalar_value(int(1)),
            },
        ])
    );
}

#[test]
fn case_conditions_accept_value_on_left_and_comparison_chains() {
    let creation = || property("p", "creationDate");
    assert_eq!(
        returned_scalar(
            "MATCH (p:Post) RETURN CASE WHEN $startDate > p.creationDate THEN 1 ELSE 0 END AS v"
        ),
        case_when(binary(
            scalar_value(param("startDate")),
            ScalarBinaryOp::Gt,
            creation()
        ))
    );
    assert_eq!(
        returned_scalar(
            "MATCH (p:Post) RETURN CASE WHEN $endDate > p.creationDate >= $startDate THEN 1 ELSE 0 END AS v"
        ),
        case_when(binary(
            binary(scalar_value(param("endDate")), ScalarBinaryOp::Gt, creation()),
            ScalarBinaryOp::And,
            binary(creation(), ScalarBinaryOp::Gte, scalar_value(param("startDate"))),
        ))
    );
    assert_eq!(
        returned_scalar(
            "MATCH (p:Post) RETURN CASE WHEN 1 < p.x = p.y <> 5 AND NOT p.z IS NULL THEN 1 ELSE 0 END AS v"
        ),
        case_when(binary(
            binary(
                binary(
                    binary(scalar_value(int(1)), ScalarBinaryOp::Lt, property("p", "x")),
                    ScalarBinaryOp::And,
                    binary(property("p", "x"), ScalarBinaryOp::Eq, property("p", "y")),
                ),
                ScalarBinaryOp::And,
                binary(property("p", "y"), ScalarBinaryOp::NotEq, scalar_value(int(5))),
            ),
            ScalarBinaryOp::And,
            scalar(ScalarExpressionKind::Not(Box::new(scalar(
                ScalarExpressionKind::IsNull {
                    expression: Box::new(property("p", "z")),
                    negated: false,
                }
            )))),
        ))
    );
}

#[test]
fn case_comparison_chain_links_keep_operand_source_spans() {
    let query = "MATCH (p:Post) RETURN CASE WHEN $a > p.x >= $b THEN 1 END AS v";
    let ScalarExpressionKind::Case { branches, .. } = returned_scalar(query).kind else {
        panic!("expected CASE");
    };
    let condition = &branches[0].0;
    let ScalarExpressionKind::Binary { left, right, .. } = &condition.kind else {
        panic!("expected chained AND");
    };
    let text = |node: &ScalarExpression| node.span.and_then(|span| span.text(query));
    assert_eq!(text(condition), Some("$a > p.x >= $b"));
    assert_eq!(text(left), Some("$a > p.x"));
    assert_eq!(text(right), Some("p.x >= $b"));
}

#[test]
fn incomplete_comparisons_remain_syntax_errors() {
    for query in [
        "MATCH (n:Item) WHERE 1 < RETURN n",
        "MATCH (n:Item) WHERE n.x < < 3 RETURN n",
        "MATCH (n:Item) WHERE n.x < 1 <= RETURN n",
        "MATCH (n:Item) WHERE 1 < n.x < RETURN n",
        "MATCH (n:Item) WHERE $a RETURN n",
        "MATCH (n:Item) WHERE 5 RETURN n",
        "MATCH (n:Item) WHERE $a > n.x >= RETURN n",
        "MATCH (n:Item) RETURN CASE WHEN 1 < n.x < THEN 1 END AS v",
        "MATCH (n:Item) RETURN CASE WHEN $a > THEN 1 END AS v",
    ] {
        assert!(
            matches!(parse(query), Err(HawDBError::Parse(_))),
            "{query}: {:?}",
            parse(query)
        );
    }
}
