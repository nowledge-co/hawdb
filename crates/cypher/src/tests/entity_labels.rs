// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Kùzu `label(x)` and openCypher `labels(n)` in every expression position
//! (#1678).

use crate::{parse, parse_pipeline, ScalarExpressionKind, Statement};
use hawdb_core::LabelFunction;

/// The statement and pipeline entries agree; returns the clause Debug text.
fn clauses(query: &str) -> String {
    let Statement::Pipeline(parsed) =
        parse(query).unwrap_or_else(|error| panic!("{query}: {error}"))
    else {
        panic!("expected a pipeline: {query}");
    };
    let pipeline = parse_pipeline(query).unwrap();
    let text = format!("{:?}", parsed.clauses);
    assert_eq!(text, format!("{:?}", pipeline.clauses), "{query}");
    text
}

fn label(function: LabelFunction, variable: &str) -> String {
    format!(
        "{:?}",
        ScalarExpressionKind::Label {
            function,
            variable: variable.to_owned(),
        }
    )
}

#[test]
fn label_and_labels_parse_in_every_expression_position() {
    let b = label(LabelFunction::Label, "b");
    let r = label(LabelFunction::Label, "r");
    let ls = label(LabelFunction::Labels, "b");
    for (query, expected) in [
        ("MATCH (b) RETURN label(b) AS l", vec![&b]),
        ("MATCH (b) RETURN LABEL(b), Labels(b)", vec![&b, &ls]),
        (
            "MATCH (a)-[r]->(b) RETURN label(r) AS t, label(b) AS l",
            vec![&r, &b],
        ),
        (
            "MATCH (b) WHERE label(b) IN $labels RETURN b.id AS id",
            vec![&b],
        ),
        (
            "MATCH (b) WHERE label(b) IN ['Entity', 'Memory'] RETURN b.id",
            vec![&b],
        ),
        (
            "MATCH (a)-[r]->(b) WHERE label(r) IN $types RETURN b.id",
            vec![&r],
        ),
        (
            "MATCH (b) WHERE label(b) <> 'Entity' OR label(b) = $l RETURN 1",
            vec![&b],
        ),
        ("MATCH (b) WHERE NOT label(b) IN $labels RETURN 1", vec![&b]),
        ("MATCH (b) WHERE label(b) CONTAINS 'E' RETURN 1", vec![&b]),
        ("MATCH (b) WHERE label(b) IS NOT NULL RETURN 1", vec![&b]),
        ("MATCH (b) WHERE 'Entity' IN labels(b) RETURN 1", vec![&ls]),
        ("MATCH (b) WHERE size(labels(b)) = 1 RETURN 1", vec![&ls]),
        (
            "MATCH (b) WITH b, label(b) AS l WHERE l IN $labels RETURN b.id",
            vec![&b],
        ),
        (
            "MATCH (b) WITH b WHERE label(b) IN $labels RETURN b.id",
            vec![&b],
        ),
        (
            "MATCH (b) RETURN b.id ORDER BY label(b) DESC, b.id",
            vec![&b],
        ),
        (
            "MATCH (b) WITH b ORDER BY label(b) LIMIT 1 RETURN b.id",
            vec![&b],
        ),
        (
            "MATCH (b) RETURN lower(label(b)) + ':' + b.id AS k",
            vec![&b],
        ),
        (
            "MATCH (b) RETURN CASE WHEN label(b) = 'E' THEN 1 ELSE 0 END AS k",
            vec![&b],
        ),
        ("MATCH (b) RETURN label(b) AS l, count(*) AS n", vec![&b]),
        ("MATCH (b) RETURN [x IN [b] | label(b)] AS ls", vec![&b]),
        (
            "MATCH (b) OPTIONAL MATCH (b)-[r]->(c) RETURN label(c), label(r)",
            vec![&r],
        ),
    ] {
        let text = clauses(query);
        for expected in expected {
            assert!(text.contains(expected.as_str()), "{query}: {text}");
        }
    }
}

#[test]
fn type_stays_a_relationship_type_and_now_parses_in_where() {
    let text = clauses("MATCH (a)-[r]->(b) WHERE type(r) IN $types RETURN type(r) AS t");
    assert_eq!(
        text.matches(&format!(
            "{:?}",
            ScalarExpressionKind::RelationshipType("r".into())
        ))
        .count(),
        2,
        "{text}"
    );
    assert!(!text.contains("Label {"), "{text}");
}

#[test]
fn label_in_a_parameter_list_is_an_expression_in_predicate() {
    let text = clauses("MATCH (b) WHERE b.id IN $ids AND label(b) IN $labels RETURN b.id");
    assert!(
        text.contains(&format!(
            "ExpressionIn {{ expression: AstNode {{ kind: {}",
            label(LabelFunction::Label, "b")
        )),
        "{text}"
    );
}

#[test]
fn label_takes_exactly_one_variable() {
    for query in [
        "MATCH (b) RETURN label(b.id) AS l",
        "MATCH (b) RETURN label() AS l",
        "MATCH (b) RETURN label(b, b) AS l",
        "MATCH (b) WHERE labels(b.id) IN $l RETURN 1",
    ] {
        assert!(parse(query).is_err(), "{query}");
    }
    // `label`, `labels` and `type` stay ordinary identifiers.
    clauses("MATCH (label)-[type]->(labels) RETURN label.id, type.w, labels.x");
    clauses("MATCH (label) WHERE label.id = 1 RETURN label");
}
