// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Planning Kùzu `label(x)` and openCypher `labels(n)` (#1678).

use super::*;
use hawdb_core::LabelFunction;

fn plan(query: &str, parameters: &[(&str, Value)]) -> Result<LogicalPlan> {
    let statement = hawdb_cypher::parse(query).unwrap_or_else(|error| panic!("{query}: {error}"));
    crate::plan_with_params(
        &statement,
        &parameters
            .iter()
            .map(|(name, value)| ((*name).to_string(), value.clone()))
            .collect(),
    )
}

fn plan_text(query: &str, parameters: &[(&str, Value)]) -> String {
    format!(
        "{:?}",
        plan(query, parameters).unwrap_or_else(|error| panic!("{query}: {error}"))
    )
}

fn label(function: LabelFunction, variable: &str) -> String {
    format!(
        "{:?}",
        ProjectionExpression::Label {
            function,
            variable: variable.to_string(),
        }
    )
}

fn labels_parameter() -> (&'static str, Value) {
    ("labels", Value::List(vec![Value::String("Entity".into())]))
}

#[test]
fn label_plans_a_label_expression_with_its_default_name() {
    let items = match plan(
        "MATCH (a)-[r]->(b) RETURN label(b), labels(b), label(r), type(r)",
        &[],
    )
    .unwrap()
    {
        LogicalPlan::Project { items, .. } => items,
        other => panic!("expected a projection: {other:?}"),
    };
    let names = items
        .iter()
        .map(|item| item.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["label(b)", "labels(b)", "label(r)", "label(r)"]);
    assert_eq!(
        format!("{:?}", items[0].expression),
        label(LabelFunction::Label, "b")
    );
    assert_eq!(
        format!("{:?}", items[1].expression),
        label(LabelFunction::Labels, "b")
    );
    assert_eq!(
        format!("{:?}", items[2].expression),
        label(LabelFunction::Label, "r")
    );
    // `type(r)` keeps its relationship-only expression.
    assert_eq!(
        items[3].expression,
        ProjectionExpression::RelationshipType {
            variable: "r".into()
        }
    );
}

#[test]
fn label_in_a_parameter_list_plans_an_expression_in_filter() {
    for query in [
        "MATCH (b) WHERE label(b) IN $labels RETURN b.id AS id",
        "MATCH (b) WHERE b.id IN $ids AND label(b) IN $labels RETURN b.id AS id",
        "MATCH (b) WITH b WHERE label(b) IN $labels RETURN b.id AS id",
    ] {
        let text = plan_text(
            query,
            &[
                labels_parameter(),
                ("ids", Value::List(vec![Value::String("e1".into())])),
            ],
        );
        assert!(
            text.contains(&format!(
                "ExpressionIn {{ expression: {}, values: [String(\"Entity\")] }}",
                label(LabelFunction::Label, "b")
            )),
            "{query}: {text}"
        );
    }
    // A literal list binds the same way.
    let text = plan_text(
        "MATCH (b) WHERE label(b) IN ['Entity'] RETURN b.id AS id",
        &[],
    );
    assert!(text.contains("ExpressionIn"), "{text}");
}

#[test]
fn label_reads_its_variable_through_with_and_order_by() {
    let text = plan_text(
        "MATCH (b) WITH b, label(b) AS l WHERE l IN $labels RETURN b.id AS id ORDER BY label(b)",
        &[labels_parameter()],
    );
    assert!(
        text.matches(&label(LabelFunction::Label, "b")).count() >= 2,
        "{text}"
    );
}

#[test]
fn label_requires_a_variable_in_scope() {
    for query in [
        "MATCH (b) RETURN label(x) AS l",
        "MATCH (b) WHERE label(x) IN $labels RETURN b.id",
        "MATCH (b) WITH b.id AS id RETURN labels(b) AS l",
    ] {
        let error = plan(query, &[labels_parameter()]).unwrap_err().to_string();
        assert!(error.contains("unknown variable"), "{query}: {error}");
    }
}

#[test]
fn label_fingerprints_differ_from_type() {
    let render = |expression: &ProjectionExpression| {
        let mut output = String::new();
        crate::write_projection_expression(&mut output, expression);
        output
    };
    let label_r = ProjectionExpression::Label {
        function: LabelFunction::Label,
        variable: "r".into(),
    };
    let labels_r = ProjectionExpression::Label {
        function: LabelFunction::Labels,
        variable: "r".into(),
    };
    let type_r = ProjectionExpression::RelationshipType {
        variable: "r".into(),
    };
    assert_eq!(render(&label_r), "entity_label(1:r)");
    assert_eq!(render(&labels_r), "entity_labels(1:r)");
    assert_ne!(render(&label_r), render(&type_r));
}
