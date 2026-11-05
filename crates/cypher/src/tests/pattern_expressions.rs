// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Pattern predicates, pattern comprehensions and `NOT` operands (#1488).

use crate::{
    parse, parse_pipeline, ClauseKind, PropertyPredicate, RelationshipDirection,
    ReturnExpressionKind, ScalarExpression, ScalarExpressionKind, Statement,
};

fn returned_scalars(query: &str) -> Vec<ScalarExpression> {
    let Statement::Pipeline(parsed) = parse(query).unwrap() else {
        panic!("expected a pipeline");
    };
    let pipeline = parse_pipeline(query).unwrap();
    assert_eq!(
        format!("{:?}", parsed.clauses),
        format!("{:?}", pipeline.clauses)
    );
    let ClauseKind::Return(projection) = &parsed.clauses.last().unwrap().kind else {
        panic!("expected a concluding RETURN clause");
    };
    projection
        .items
        .iter()
        .map(|item| match &item.expression.kind {
            ReturnExpressionKind::Value(value) => value.clone(),
            other => panic!("expected scalar return item, got {other:?}"),
        })
        .collect()
}

fn match_predicate(query: &str) -> PropertyPredicate {
    let Statement::Pipeline(parsed) = parse(query).unwrap() else {
        panic!("expected a pipeline");
    };
    let ClauseKind::Match {
        predicate: Some(predicate),
        ..
    } = &parsed.clauses[0].kind
    else {
        panic!("expected MATCH ... WHERE");
    };
    predicate.kind.clone()
}

#[test]
fn pattern_comprehensions_parse_pattern_where_and_projection() {
    let items = returned_scalars(
        "MATCH (a) RETURN [(a)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(b:Person) WHERE b.id > 1 | b.name] AS x, \
         [(a)-[r:R*1..2]-(c {id: 3}) | c] AS y",
    );
    let ScalarExpressionKind::PatternComprehension {
        pattern,
        predicate,
        projection,
    } = &items[0].kind
    else {
        panic!("expected a pattern comprehension: {:?}", items[0]);
    };
    assert_eq!(pattern.first.variable, "a");
    assert_eq!(pattern.steps.len(), 2);
    assert_eq!(pattern.steps[0].relationship.rel_type, "HAS_TAG");
    assert_eq!(
        pattern.steps[1].relationship.direction,
        RelationshipDirection::Incoming
    );
    assert!(pattern.steps[0].target.anonymous);
    assert_eq!(pattern.steps[1].target.label, "Person");
    assert!(predicate.is_some());
    assert!(matches!(
        projection.kind,
        ScalarExpressionKind::Property { ref variable, ref property }
            if variable == "b" && property == "name"
    ));
    let ScalarExpressionKind::PatternComprehension {
        pattern, predicate, ..
    } = &items[1].kind
    else {
        panic!("expected a pattern comprehension: {:?}", items[1]);
    };
    assert!(predicate.is_none());
    let relationship = &pattern.steps[0].relationship;
    assert_eq!(relationship.variable.as_deref(), Some("r"));
    assert_eq!((relationship.min_hops, relationship.max_hops), (1, 2));
    assert!(pattern.steps[0].target.properties.contains_key("id"));
}

#[test]
fn pattern_predicates_are_boolean_operands_in_scalar_expressions() {
    let items = returned_scalars(
        "MATCH (p), (q) RETURN NOT ((p)-[:KNOWS]-(q)) AS a, not((p)-[:KNOWS]-(q)) AS b, \
         not(p.x = 1) AS c, size([x IN p.list WHERE (x)-[:R]->(q)]) AS d, \
         CASE WHEN (p)-[:R]->() AND p.y THEN 1 ELSE 0 END AS e",
    );
    for item in &items[..2] {
        let ScalarExpressionKind::Not(inner) = &item.kind else {
            panic!("expected NOT: {item:?}");
        };
        assert!(
            matches!(&inner.kind, ScalarExpressionKind::PatternPredicate(pattern)
                if pattern.first.variable == "p" && pattern.steps[0].target.variable == "q"),
            "{inner:?}"
        );
    }
    // `not(...)` negates the whole parenthesized condition.
    let ScalarExpressionKind::Not(inner) = &items[2].kind else {
        panic!("expected NOT: {:?}", items[2]);
    };
    assert!(matches!(inner.kind, ScalarExpressionKind::Binary { .. }));
    let ScalarExpressionKind::ListFunction { arguments, .. } = &items[3].kind else {
        panic!("expected size(): {:?}", items[3]);
    };
    let ScalarExpressionKind::ListComprehension {
        predicate: Some(predicate),
        ..
    } = &arguments[0].kind
    else {
        panic!("expected a filtered comprehension: {:?}", arguments[0]);
    };
    assert!(matches!(
        predicate.kind,
        ScalarExpressionKind::PatternPredicate(_)
    ));
    assert!(matches!(items[4].kind, ScalarExpressionKind::Case { .. }));
    // A bare `NOT` is a boolean connective, never an arithmetic or
    // comparison operand.
    for query in [
        "MATCH (p) RETURN NOT p.x AS y",
        "MATCH (p) RETURN 1 + NOT p.x AS y",
        "MATCH (p) RETURN p.a = NOT p.b AND p.c AS y",
        "MATCH (p) WITH p WHERE p.a = NOT p.b AND p.c RETURN p",
    ] {
        assert!(parse(query).is_err(), "{query}");
    }
}

#[test]
fn where_pattern_predicates_keep_the_one_hop_form_only_for_anonymous_targets() {
    assert!(matches!(
        match_predicate("MATCH (a) WHERE (a)-[:R]->(:Tag) RETURN a"),
        PropertyPredicate::RelationshipExists { ref variable, ref target_label, .. }
            if variable == "a" && target_label == "Tag"
    ));
    // A named target, a labeled source, more hops, a disjunction or a
    // relationship property are part of the pattern.
    for query in [
        "MATCH (a), (b) WHERE (a)-[:R]->(b) RETURN a",
        "MATCH (a) WHERE (a:Item)-[:R]->() RETURN a",
        "MATCH (a) WHERE (a)-[:R]->()-[:S]->() RETURN a",
        "MATCH (a) WHERE (a)-[:R|S]->() RETURN a",
        "MATCH (a) WHERE (a)-[:R {w: 1}]->() RETURN a",
        "MATCH (a) WHERE (a)-[:R*1..2]->() RETURN a",
    ] {
        assert!(
            matches!(
                match_predicate(query),
                PropertyPredicate::ExpressionEq { ref expression, .. }
                    if matches!(expression.kind, ScalarExpressionKind::PatternPredicate(_))
            ),
            "{query}"
        );
    }
    assert!(matches!(
        match_predicate("MATCH (a), (b) WHERE NOT (a)-[:R]-(b) RETURN a"),
        PropertyPredicate::Not(_)
    ));
}

#[test]
fn bare_patterns_and_incomplete_comprehensions_are_rejected() {
    for query in [
        // openCypher: a pattern is not a projected value.
        "MATCH (n) RETURN (n)-[:R]->() AS x",
        "MATCH (n) WITH (n)-[:R]->() AS x RETURN x",
        // A pattern comprehension requires its projection.
        "MATCH (n) RETURN [(n)-[:R]->(m)] AS x",
        "MATCH (n) RETURN size((n)-[:R]->()) AS x",
        "MATCH (n) RETURN [(n)-[:R]->(m) WHERE m.x | ] AS x",
        "MATCH (n) RETURN [(n)-[:R]->(m) WHERE | m] AS x",
    ] {
        assert!(parse(query).is_err(), "{query}");
    }
    // A parenthesized element is still an expression.
    let items = returned_scalars("MATCH (n) RETURN [(n.x), n.y] AS x");
    assert!(matches!(items[0].kind, ScalarExpressionKind::List(_)));
    let error = parse("MATCH (n) RETURN [(n)-[:R]->(m), 1] AS x")
        .unwrap_err()
        .to_string();
    assert!(error.contains("a pattern is not a list element"), "{error}");
}
