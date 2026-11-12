// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Kùzu `label(x)` and openCypher `labels(n)` evaluation (#1678).

use super::*;
use crate::{QueryMemoryClass, QueryMemoryLedger};
use hawdb_core::{
    ids::{NodeId, RelId},
    LabelFunction,
};
use std::num::NonZeroUsize;

fn call(function: LabelFunction, variable: &str) -> ProjectionExpression {
    ProjectionExpression::Label {
        function,
        variable: variable.to_string(),
    }
}

fn strings(values: &[&str]) -> Value {
    Value::List(
        values
            .iter()
            .map(|value| Value::String((*value).into()))
            .collect(),
    )
}

/// `one:Person`, `none` without a label, `two:Person:Entity`, `r:KNOWS` and a
/// NULL `missing` (an unmatched OPTIONAL MATCH variable).
fn fixture() -> (Catalog, Binding) {
    let mut catalog = Catalog::default();
    let person = catalog.get_or_create_label("Person");
    let entity = catalog.get_or_create_label("Entity");
    let knows = catalog.get_or_create_rel_type("KNOWS");
    let node = |id, labels: &[hawdb_core::LabelId]| NodeRecord {
        id: NodeId(id),
        labels: labels.iter().copied().collect(),
        properties: BTreeMap::new(),
    };
    let mut binding = Binding::values(BTreeMap::from([("missing".to_string(), Value::Null)]));
    binding.nodes.insert("one".to_string(), node(1, &[person]));
    binding.nodes.insert("none".to_string(), node(2, &[]));
    binding
        .nodes
        .insert("two".to_string(), node(3, &[entity, person]));
    binding.relationships.insert(
        "r".to_string(),
        RelRecord {
            id: RelId(4),
            source: NodeId(1),
            target: NodeId(3),
            rel_type: knows,
            properties: BTreeMap::new(),
        },
    );
    (catalog, binding)
}

fn evaluate(expression: &ProjectionExpression) -> Result<Value> {
    let (catalog, binding) = fixture();
    evaluate_projection_expression(expression, &catalog, &binding)
}

#[test]
fn label_reads_a_single_node_label_or_the_relationship_type() {
    assert_eq!(
        evaluate(&call(LabelFunction::Label, "one")).unwrap(),
        Value::String("Person".into())
    );
    assert_eq!(
        evaluate(&call(LabelFunction::Label, "r")).unwrap(),
        Value::String("KNOWS".into())
    );
    // `label(r)` agrees with `type(r)`.
    assert_eq!(
        evaluate(&ProjectionExpression::RelationshipType {
            variable: "r".into()
        })
        .unwrap(),
        evaluate(&call(LabelFunction::Label, "r")).unwrap()
    );
    // A node without a label has no label; NULL in gives NULL out.
    for variable in ["none", "missing"] {
        assert_eq!(
            evaluate(&call(LabelFunction::Label, variable)).unwrap(),
            Value::Null
        );
    }
    assert_eq!(
        evaluate(&call(LabelFunction::Labels, "missing")).unwrap(),
        Value::Null
    );
}

#[test]
fn labels_lists_every_node_label_in_label_id_order() {
    assert_eq!(
        evaluate(&call(LabelFunction::Labels, "one")).unwrap(),
        strings(&["Person"])
    );
    assert_eq!(
        evaluate(&call(LabelFunction::Labels, "two")).unwrap(),
        strings(&["Person", "Entity"])
    );
    assert_eq!(
        evaluate(&call(LabelFunction::Labels, "none")).unwrap(),
        strings(&[])
    );
}

#[test]
fn ambiguous_and_mistyped_calls_are_errors() {
    let error = evaluate(&call(LabelFunction::Label, "two"))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("label(two) requires a node with one label, but the node has 2"),
        "{error}"
    );
    assert!(error.contains("use labels(two)"), "{error}");
    let error = evaluate(&call(LabelFunction::Labels, "r"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("labels(r) requires a node"), "{error}");
    let error = evaluate(&call(LabelFunction::Label, "unbound"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("missing variable 'unbound'"), "{error}");
}

#[test]
fn label_values_are_charged_to_the_memory_account() {
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(1 << 20).unwrap());
    let account = ledger.account(
        QueryMemoryClass::PipelineBatch,
        "label test",
        NonZeroUsize::new(64 * 1024).unwrap(),
    );
    let (catalog, binding) = fixture();
    let available = account.available_bytes();
    for (expression, expected) in [
        (call(LabelFunction::Label, "one"), "Person".len()),
        (call(LabelFunction::Label, "r"), "KNOWS".len()),
        (
            call(LabelFunction::Labels, "two"),
            "Person".len() + "Entity".len(),
        ),
    ] {
        let value =
            evaluate_projection_borrowed(&expression, &catalog, &binding, Some(&account)).unwrap();
        assert!(
            available - account.available_bytes() >= expected,
            "{expression:?}"
        );
        drop(value);
        assert_eq!(account.available_bytes(), available);
    }
}

/// A label id the catalog cannot name is a storage inconsistency: `label(n)`
/// and `labels(n)` refuse it rather than skipping it and returning the
/// remaining label.
#[test]
fn a_label_id_missing_from_the_catalog_is_refused_not_skipped() {
    let (catalog, mut binding) = fixture();
    let person = catalog.label_id("Person").unwrap();
    let unknown = hawdb_core::LabelId(u32::MAX);
    assert!(catalog.label_name(unknown).is_none());
    binding.nodes.insert(
        "stale".to_string(),
        NodeRecord {
            id: NodeId(9),
            labels: [unknown, person].into_iter().collect(),
            properties: BTreeMap::new(),
        },
    );
    for function in [LabelFunction::Label, LabelFunction::Labels] {
        let error = evaluate_projection_expression(&call(function, "stale"), &catalog, &binding)
            .unwrap_err();
        assert!(
            matches!(error, HawDBError::StorageIntegrity(_)),
            "{function:?}: {error:?}"
        );
    }
}
