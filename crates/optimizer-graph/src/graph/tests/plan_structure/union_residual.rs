// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn catalog(space_index: bool) -> OptimizerCatalog {
    let mut indexes = vec![
        ("Thread".into(), "id".into()),
        ("Thread".into(), "thread_id".into()),
    ];
    let mut distinct = vec![
        (("Thread".into(), "id".into()), 10_000),
        (("Thread".into(), "thread_id".into()), 10_000),
    ];
    if space_index {
        indexes.push(("Thread".into(), "space_id".into()));
        distinct.push((("Thread".into(), "space_id".into()), 10_000));
    }
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(indexes, [], [], []),
        OptimizerCatalogStatistics::new([("Thread".into(), 10_000)], [], [], [], [], distinct, []),
    )
}

fn key(property: &str) -> Predicate {
    Predicate::PropertyEq {
        variable: "t".into(),
        property: property.into(),
        value: Value::String("needle".into()),
    }
}

fn scope() -> Predicate {
    Predicate::ExpressionEq {
        expression: ProjectionExpression::Coalesce(vec![
            ProjectionExpression::Property {
                variable: "t".into(),
                property: "space_id".into(),
            },
            ProjectionExpression::Literal(Value::String("default".into())),
        ]),
        value: ProjectionExpression::Literal(Value::String("default".into())),
    }
}

fn logical(predicate: Predicate) -> LogicalPlan {
    LogicalPlan::Filter {
        predicate,
        input: Box::new(LogicalPlan::NodeScan {
            variable: "t".into(),
            label: "Thread".into(),
        }),
    }
}

#[test]
fn residual_union_is_costed_and_keeps_the_complete_where() {
    let predicate = Predicate::And(vec![
        Predicate::Or(vec![key("id"), key("thread_id")]),
        scope(),
    ]);
    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical(predicate.clone()), &catalog(false));
    assert!(
        matches!(plan, PhysicalPlan::FilterExec { predicate: residual, input }
        if residual == predicate && matches!(input.as_ref(), PhysicalPlan::IndexNodeUnionSeek { branches, .. } if branches.len() == 2))
    );
    assert!(trace
        .rule_events
        .iter()
        .any(|event| event.rule() == "implementation:node_conjunction_index_union_seek"));
}

#[test]
fn residual_union_competes_with_a_cheaper_conjunctive_seek() {
    let predicate = Predicate::And(vec![
        Predicate::Or(vec![key("id"), key("thread_id")]),
        Predicate::PropertyEq {
            variable: "t".into(),
            property: "space_id".into(),
            value: Value::String("default".into()),
        },
    ]);
    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical(predicate), &catalog(true));
    assert!(matches!(plan, PhysicalPlan::FilterExec { input, .. }
        if matches!(input.as_ref(), PhysicalPlan::IndexNodeSeek { property, .. } if property == "space_id")));
    assert!(trace
        .decisions
        .iter()
        .any(|decision| decision.contains("alternatives_considered=2")));
}

#[test]
fn residual_union_keeps_unsafe_and_unindexed_arms_on_the_existing_path() {
    let predicate = Predicate::And(vec![
        Predicate::Or(vec![key("id"), key("thread_id")]),
        scope(),
    ]);
    let mut missing_index = catalog(false);
    missing_index
        .equality_property_indexes
        .remove(&("Thread".into(), "thread_id".into()));
    let (plan, _) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical(predicate.clone()), &missing_index);
    assert!(!plan.explain(0).contains("IndexNodeUnionSeek"));
    let fallible = Predicate::ExpressionEq {
        expression: ProjectionExpression::ListFunction {
            function: hawdb_core::ListFunction::Head,
            arguments: vec![ProjectionExpression::Literal(Value::Int(1))],
        },
        value: ProjectionExpression::Literal(Value::Int(1)),
    };
    for parts in [
        vec![fallible.clone(), predicate.clone()],
        vec![predicate.clone(), fallible],
    ] {
        let (plan, _) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical(Predicate::And(parts)), &catalog(false));
        assert!(!plan.explain(0).contains("IndexNodeUnionSeek"));
    }
}

#[test]
fn residual_union_visits_only_mandatory_or_conjuncts() {
    let nested = Predicate::And(vec![
        scope(),
        Predicate::And(vec![
            Predicate::Or(vec![key("id"), key("thread_id")]),
            Predicate::ConstantBool(true),
        ]),
    ]);
    let (plan, _) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical(nested), &catalog(false));
    assert!(plan.explain(0).contains("IndexNodeUnionSeek"));
    for predicate in [
        Predicate::And(vec![
            scope(),
            Predicate::Or(vec![
                key("id"),
                Predicate::PropertyIsNull {
                    variable: "t".into(),
                    property: "thread_id".into(),
                },
            ]),
        ]),
        Predicate::And(vec![
            scope(),
            Predicate::Not(Box::new(Predicate::Or(vec![key("id"), key("thread_id")]))),
        ]),
    ] {
        let (plan, _) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical(predicate), &catalog(false));
        assert!(!plan.explain(0).contains("IndexNodeUnionSeek"));
    }
}
