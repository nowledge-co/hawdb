// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_plan_cypher::{
    MatchSetAssignment, MatchSetBinding, Projection, ProjectionExpression, ScalarBinaryOp, SetValue,
};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

fn node(id: u64, properties: &[(&str, Value)]) -> NodeRecord {
    NodeRecord {
        id: NodeId(id),
        labels: BTreeSet::new(),
        properties: properties
            .iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect(),
    }
}

fn property(variable: &str, property: &str) -> ProjectionExpression {
    ProjectionExpression::Property {
        variable: variable.into(),
        property: property.into(),
    }
}

fn plus(left: ProjectionExpression, right: ProjectionExpression) -> ProjectionExpression {
    ProjectionExpression::Binary {
        left: Box::new(left),
        op: ScalarBinaryOp::Add,
        right: Box::new(right),
    }
}

fn binding(variable: &str, entity: Option<GraphEntityKind>) -> MatchSetBinding {
    MatchSetBinding {
        variable: variable.into(),
        column: format!("\0set.{variable}"),
        entity,
    }
}

fn assignment(variable: &str, property: &str, value: MatchSetValue) -> MatchSetAssignment {
    MatchSetAssignment {
        variable: variable.into(),
        property: property.into(),
        value,
    }
}

fn row(entries: &[(&str, Value)]) -> Row {
    entries
        .iter()
        .map(|(variable, value)| (format!("\0set.{variable}"), value.clone()))
        .collect()
}

fn plan(
    bindings: Vec<MatchSetBinding>,
    assignments: Vec<MatchSetAssignment>,
    returns: Option<Vec<Projection>>,
) -> MatchSet<PhysicalPlan> {
    MatchSet {
        input: PhysicalPlan::EmptyExec,
        bindings,
        assignments,
        returns,
    }
}

fn resolve(
    plan: &MatchSet<PhysicalPlan>,
    input: Vec<Row>,
    nodes: &[NodeRecord],
    limits: MutationLimits,
) -> Result<MatchSetOutcome> {
    resolve_match_set(
        plan,
        input,
        &Catalog::default(),
        &mut |entity| {
            Ok(match entity {
                MutationEntity::Node(id) => nodes
                    .iter()
                    .find(|node| node.id == id)
                    .cloned()
                    .map(MatchSetRecord::Node),
                MutationEntity::Relationship(_) => None,
            })
        },
        limits,
        OutputLimits::default(),
        None,
    )
}

fn updates(outcome: &MatchSetOutcome) -> &[EntityPropertyUpdate] {
    match &outcome.mutation {
        Some(GraphMutation::SetEntityProperties(updates)) => updates,
        other => panic!("expected entity updates, got {other:?}"),
    }
}

/// Like Kùzu, one SET item is evaluated for every row before it is applied:
/// two rows of the same node both read its value before the item.
#[test]
fn every_row_of_an_item_reads_the_state_before_that_item() {
    let plan = plan(
        vec![
            binding("a", Some(GraphEntityKind::Node)),
            binding("b", Some(GraphEntityKind::Node)),
        ],
        vec![
            assignment(
                "a",
                "n",
                MatchSetValue::Expression(plus(
                    property("a", "n"),
                    ProjectionExpression::Literal(Value::Int(1)),
                )),
            ),
            // A later item reads the earlier item's result.
            assignment("a", "copy", MatchSetValue::Expression(property("a", "n"))),
        ],
        Some(vec![Projection {
            expression: property("a", "copy"),
            name: "copy".into(),
        }]),
    );
    let nodes = [node(1, &[("n", Value::Int(100))]), node(2, &[])];
    let input = vec![
        row(&[("a", Value::Int(1)), ("b", Value::Int(1))]),
        row(&[("a", Value::Int(1)), ("b", Value::Int(2))]),
    ];
    let outcome = resolve(&plan, input, &nodes, MutationLimits::default()).unwrap();
    assert_eq!(
        updates(&outcome),
        [EntityPropertyUpdate {
            entity: MutationEntity::Node(NodeId(1)),
            assignments: vec![
                ("n".into(), Value::Int(101)),
                ("copy".into(), Value::Int(101)),
            ],
        }]
    );
    assert_eq!(outcome.rows.len(), 2);
    assert!(outcome
        .rows
        .iter()
        .all(|row| row.get("copy") == Some(&Value::Int(101))));
}

#[test]
fn null_targets_are_skipped_and_scalar_columns_are_read() {
    let plan = plan(
        vec![
            binding("n", Some(GraphEntityKind::Node)),
            binding("x", None),
        ],
        vec![
            assignment(
                "n",
                "value",
                MatchSetValue::Expression(ProjectionExpression::Column("x".into())),
            ),
            assignment(
                "n",
                "count",
                MatchSetValue::Local(SetValue::AddInt {
                    property: "count".into(),
                    amount: 2,
                }),
            ),
        ],
        Some(vec![Projection {
            expression: property("n", "value"),
            name: "value".into(),
        }]),
    );
    let nodes = [node(7, &[])];
    let input = vec![
        row(&[("n", Value::Int(7)), ("x", Value::String("seven".into()))]),
        row(&[("n", Value::Null), ("x", Value::String("none".into()))]),
    ];
    let outcome = resolve(&plan, input, &nodes, MutationLimits::default()).unwrap();
    assert_eq!(
        updates(&outcome),
        [EntityPropertyUpdate {
            entity: MutationEntity::Node(NodeId(7)),
            assignments: vec![
                ("value".into(), Value::String("seven".into())),
                ("count".into(), Value::Int(2)),
            ],
        }]
    );
    assert_eq!(
        outcome.rows,
        vec![
            BTreeMap::from([("value".into(), Value::String("seven".into()))]),
            BTreeMap::from([("value".into(), Value::Null)]),
        ]
    );
}

#[test]
fn no_matched_row_writes_nothing() {
    let plan = plan(
        vec![binding("n", Some(GraphEntityKind::Node))],
        vec![assignment(
            "n",
            "value",
            MatchSetValue::Local(SetValue::Value(Value::Int(1))),
        )],
        None,
    );
    let outcome = resolve(&plan, Vec::new(), &[], MutationLimits::default()).unwrap();
    assert!(outcome.mutation.is_none());
    assert!(outcome.rows.is_empty());
}

#[test]
fn refusals_leave_no_mutation() {
    let set = |value| {
        plan(
            vec![binding("n", Some(GraphEntityKind::Node))],
            vec![assignment("n", "value", value)],
            None,
        )
    };
    let nodes = [
        node(1, &[("name", Value::String("a".into()))]),
        node(2, &[]),
    ];
    let input = || vec![row(&[("n", Value::Int(1))]), row(&[("n", Value::Int(2))])];

    // A type mismatch in any row fails the whole statement.
    let mismatch = set(MatchSetValue::Expression(plus(
        property("n", "name"),
        ProjectionExpression::Literal(Value::Int(1)),
    )));
    let error = resolve(&mismatch, input(), &nodes, MutationLimits::default()).unwrap_err();
    assert!(error.to_string().contains("numeric operands"), "{error}");

    // The row budget is the affected-row limit.
    let constant = set(MatchSetValue::Local(SetValue::Value(Value::Int(1))));
    let limits = MutationLimits {
        max_affected_rows: NonZeroUsize::new(1).unwrap(),
        ..MutationLimits::default()
    };
    let error = resolve(&constant, input(), &nodes, limits).unwrap_err();
    assert!(error.to_string().contains("max_mutation_affected_rows 1"));

    // A matched id the store cannot read is an error, not a skipped row.
    let error = resolve(&constant, input(), &nodes[..1], MutationLimits::default()).unwrap_err();
    assert!(error.to_string().contains("matched node 2 is missing"));
}
