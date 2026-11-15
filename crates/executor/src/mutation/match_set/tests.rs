// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_plan_cypher::{
    MatchSetAssignment, MatchSetBinding, Projection, ProjectionExpression, ScalarBinaryOp,
    SetValue, SortItem,
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

fn returns(items: Vec<Projection>) -> MatchSetReturn {
    MatchSetReturn {
        items,
        distinct: false,
        order_by: Vec::new(),
        offset: 0,
        limit: None,
    }
}

fn plan(
    bindings: Vec<MatchSetBinding>,
    assignments: Vec<MatchSetAssignment>,
    returns: Option<MatchSetReturn>,
) -> MatchSet<PhysicalPlan> {
    MatchSet {
        input: PhysicalPlan::EmptyExec,
        bindings,
        assignments,
        relationship: None,
        returns,
    }
}

fn resolve(
    plan: &MatchSet<PhysicalPlan>,
    input: Vec<Row>,
    nodes: &[NodeRecord],
    limits: MutationLimits,
) -> Result<MatchSetOutcome> {
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(64 << 20).unwrap());
    resolve_with(plan, input, nodes, limits, &ledger)
}

fn resolve_with(
    plan: &MatchSet<PhysicalPlan>,
    input: Vec<Row>,
    nodes: &[NodeRecord],
    limits: MutationLimits,
    ledger: &QueryMemoryLedger,
) -> Result<MatchSetOutcome> {
    let mut charge = match_set_account(ledger).reserve(0)?;
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
        &mut charge,
        limits,
        OutputLimits::default(),
        None,
    )
}

fn updates(outcome: &MatchSetOutcome) -> &[EntityPropertyUpdate] {
    match outcome.mutations.as_slice() {
        [GraphMutation::SetEntityProperties(updates)] => updates,
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
        Some(returns(vec![Projection {
            expression: property("a", "copy"),
            name: "copy".into(),
        }])),
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
        Some(returns(vec![Projection {
            expression: property("n", "value"),
            name: "value".into(),
        }])),
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
    assert!(outcome.mutations.is_empty());
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

/// ORDER BY, SKIP, LIMIT and DISTINCT select returned rows only; the SET
/// already applied to every row.
#[test]
fn return_modifiers_select_rows_after_every_row_was_updated() {
    let nodes = (1..=4)
        .map(|id| node(id, &[("rank", Value::Int(id as i64 % 2))]))
        .collect::<Vec<_>>();
    let input = || {
        (1..=4)
            .map(|id| row(&[("n", Value::Int(id))]))
            .collect::<Vec<_>>()
    };
    let set = |returns| {
        plan(
            vec![binding("n", Some(GraphEntityKind::Node))],
            vec![assignment(
                "n",
                "seen",
                MatchSetValue::Local(SetValue::Value(Value::Bool(true))),
            )],
            Some(returns),
        )
    };
    let id = Projection {
        expression: ProjectionExpression::Id {
            variable: "n".into(),
        },
        name: "id".into(),
    };
    let ordered = MatchSetReturn {
        // Descending by a property that is not returned, then by the column.
        order_by: vec![
            SortItem {
                key: SortKey::Property {
                    variable: "n".into(),
                    property: "rank".into(),
                },
                direction: SortDirection::Desc,
            },
            SortItem {
                key: SortKey::Column("id".into()),
                direction: SortDirection::Asc,
            },
        ],
        offset: 1,
        limit: Some(2),
        ..returns(vec![id.clone()])
    };
    let outcome = resolve(&set(ordered), input(), &nodes, MutationLimits::default()).unwrap();
    assert_eq!(updates(&outcome).len(), 4);
    assert_eq!(
        outcome.rows,
        vec![
            BTreeMap::from([("id".into(), Value::Int(3))]),
            BTreeMap::from([("id".into(), Value::Int(2))]),
        ]
    );

    let distinct = MatchSetReturn {
        distinct: true,
        ..returns(vec![Projection {
            expression: property("n", "rank"),
            name: "rank".into(),
        }])
    };
    let outcome = resolve(&set(distinct), input(), &nodes, MutationLimits::default()).unwrap();
    assert_eq!(updates(&outcome).len(), 4);
    assert_eq!(
        outcome.rows,
        vec![
            BTreeMap::from([("rank".into(), Value::Int(1))]),
            BTreeMap::from([("rank".into(), Value::Int(0))]),
        ]
    );

    // LIMIT 0 returns nothing and still writes every row.
    let none = MatchSetReturn {
        limit: Some(0),
        ..returns(vec![id])
    };
    let outcome = resolve(&set(none), input(), &nodes, MutationLimits::default()).unwrap();
    assert_eq!(updates(&outcome).len(), 4);
    assert!(outcome.rows.is_empty());
}

/// Entity copies and computed values are charged to the statement's query
/// memory: a multi-row SET of large strings is refused with a typed budget
/// error, and the charge is released when the statement ends.
#[test]
fn entity_copies_and_computed_values_are_charged_to_query_memory() {
    let text = "x".repeat(64 * 1024);
    let nodes = (1..=8)
        .map(|id| node(id, &[("text", Value::String(text.clone()))]))
        .collect::<Vec<_>>();
    let input = || {
        (1..=8)
            .map(|id| row(&[("n", Value::Int(id))]))
            .collect::<Vec<_>>()
    };
    let doubled = plan(
        vec![binding("n", Some(GraphEntityKind::Node))],
        vec![assignment(
            "n",
            "text",
            MatchSetValue::Expression(plus(property("n", "text"), property("n", "text"))),
        )],
        None,
    );
    // Eight 64 KiB copies fit; doubling every string does not.
    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(1 << 20).unwrap());
    let error = resolve_with(
        &doubled,
        input(),
        &nodes,
        MutationLimits::default(),
        &ledger,
    )
    .unwrap_err();
    assert!(
        matches!(error, HawDBError::ResourceBudgetExceeded(_)),
        "{error:?}"
    );
    assert_eq!(ledger.snapshot().used_bytes, 0);

    let ledger = QueryMemoryLedger::new(NonZeroUsize::new(4 << 20).unwrap());
    let outcome = resolve_with(
        &doubled,
        input(),
        &nodes,
        MutationLimits::default(),
        &ledger,
    )
    .unwrap();
    assert_eq!(updates(&outcome).len(), 8);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(ledger.snapshot().peak_bytes > 8 * 3 * 64 * 1024);
}
