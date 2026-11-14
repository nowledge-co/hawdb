// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! General expressions on the right-hand side of SET (#1791).

use crate::{
    parse, ClauseKind, ScalarBinaryOp, ScalarExpressionKind, SetProperty, SetValueExpression,
    Statement,
};

fn sets(query: &str) -> Vec<SetProperty> {
    let statement = parse(query).unwrap_or_else(|error| panic!("{query}: {error}"));
    let Statement::Pipeline(pipeline) = statement else {
        panic!("expected a pipeline: {query}");
    };
    pipeline
        .clauses
        .iter()
        .filter_map(|clause| match &clause.kind {
            ClauseKind::Set(sets) => Some(sets.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

fn expression(set: &SetProperty) -> &ScalarExpressionKind {
    match &set.value {
        SetValueExpression::Expression(expression) => &expression.kind,
        other => panic!("expected a general SET expression, got {other:?}"),
    }
}

#[test]
fn general_set_values_parse_as_scalar_expressions() {
    let parsed = sets(
        "MATCH (e:E {id: $id}) SET e.x = CASE WHEN e.x IS NULL OR e.x = '' THEN $p ELSE e.x END, \
         e.score = e.score * 0.5 + $d, e.name = e.name + ' ' + $s, e.flag = e.a > $b, \
         e.negated = NOT e.flag, e.empty = e.name IS NULL, e.member = e.id IN $ids",
    );
    assert_eq!(parsed.len(), 7);
    // The CASE may lower to a specialized form; it is an expression either way.
    expression(&parsed[0]);
    assert!(matches!(
        expression(&parsed[1]),
        ScalarExpressionKind::Binary {
            op: ScalarBinaryOp::Add,
            ..
        }
    ));
    let ScalarExpressionKind::Binary { left, op, .. } = expression(&parsed[2]) else {
        panic!("expected a concatenation");
    };
    assert_eq!(*op, ScalarBinaryOp::Add);
    assert!(matches!(
        left.kind,
        ScalarExpressionKind::Binary {
            op: ScalarBinaryOp::Add,
            ..
        }
    ));
    assert!(matches!(
        expression(&parsed[3]),
        ScalarExpressionKind::Binary {
            op: ScalarBinaryOp::Gt,
            ..
        }
    ));
    assert!(matches!(
        expression(&parsed[4]),
        ScalarExpressionKind::Not(_)
    ));
    assert!(matches!(
        expression(&parsed[5]),
        ScalarExpressionKind::IsNull { negated: false, .. }
    ));
    assert!(matches!(
        expression(&parsed[6]),
        ScalarExpressionKind::Binary {
            op: ScalarBinaryOp::In,
            ..
        }
    ));
}

#[test]
fn relationship_set_reads_its_endpoints() {
    let parsed = sets(
        "MATCH (a:E)-[r:R]->(b:E) WHERE (a.id IN $ids OR b.id IN $ids) AND r.space = $from \
         SET r.space = CASE WHEN a.space <> $from THEN a.space ELSE b.space END",
    );
    assert!(matches!(
        expression(&parsed[0]),
        ScalarExpressionKind::Case { .. }
    ));
}

/// Fixed forms keep their historical AST, so their fast paths and frozen
/// corpora are unchanged; only a value that continues past them is general.
#[test]
fn fixed_set_forms_keep_their_ast() {
    let parsed = sets(
        "MATCH (n:N) SET n.a = $p, n.b = 'x', n.c = n.c + 1, n.d = coalesce(n.d, 0), \
         n.e = n.f, n.g = CASE WHEN n.g > 0 THEN n.g - 1 ELSE 0 END, \
         n.h = CASE WHEN $h IS NULL THEN n.h ELSE $h END RETURN n",
    );
    assert!(matches!(parsed[0].value, SetValueExpression::Value(_)));
    assert!(matches!(parsed[1].value, SetValueExpression::Value(_)));
    assert!(matches!(
        parsed[2].value,
        SetValueExpression::PropertyAdd { .. }
    ));
    assert!(matches!(
        parsed[3].value,
        SetValueExpression::CoalesceProperty { .. }
    ));
    assert!(matches!(
        parsed[4].value,
        SetValueExpression::Property { .. }
    ));
    assert!(matches!(
        parsed[5].value,
        SetValueExpression::DecrementFloorZero { .. }
    ));
    assert!(matches!(
        parsed[6].value,
        SetValueExpression::PreserveNewerExisting { .. }
    ));

    // A fixed form followed by an operator continues as a general expression.
    let parsed = sets("MATCH (n:N) SET n.c = n.c + 1 + 2, n.d = $p * 2, n.e = n.f IS NULL");
    assert!(matches!(
        expression(&parsed[0]),
        ScalarExpressionKind::Binary { .. }
    ));
    assert!(matches!(
        expression(&parsed[1]),
        ScalarExpressionKind::Binary { .. }
    ));
    assert!(matches!(
        expression(&parsed[2]),
        ScalarExpressionKind::IsNull { .. }
    ));
}

#[test]
fn merge_set_items_parse_general_expressions_too() {
    let statement = parse("MERGE (n:N {id: $id}) ON MATCH SET n.count = n.count * 2 + $d").unwrap();
    let Statement::MergeNode(node) = statement else {
        panic!("expected a node MERGE");
    };
    assert!(matches!(
        node.on_match_sets[0].value,
        SetValueExpression::Expression(_)
    ));
}
