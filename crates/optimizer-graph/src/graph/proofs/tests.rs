// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::RuleOutcome;
use hawdb_core::{Catalog, GraphStatistics, PropertyType, TableKind};
use hawdb_plan_cypher::{Aggregation, Predicate, ScalarBinaryOp, SortDirection};

fn catalog(nullable: bool, value_type: PropertyType) -> OptimizerCatalog {
    let mut schema = Catalog::default();
    for label in ["Item", "Other"] {
        let id = schema.get_or_create_label(label);
        let table = schema.get_or_create_table(TableKind::Node, label);
        schema.get_or_create_property(table, "external", value_type, nullable);
        for property in ["a", "b", "space"] {
            schema.get_or_create_property(table, property, PropertyType::String, true);
        }
        schema.get_or_create_unique_constraint(id, "external");
    }
    let mut catalog =
        crate::graph::optimizer_catalog_from_graph_statistics(&schema, &GraphStatistics::default());
    for label in ["Item", "Other"] {
        catalog.label_counts.insert(label.into(), 100);
    }
    catalog
}
fn scan(variable: &str, label: &str) -> LogicalPlan {
    LogicalPlan::NodeScan {
        variable: variable.into(),
        label: label.into(),
    }
}
fn property(variable: &str, property: &str) -> ProjectionExpression {
    ProjectionExpression::Property {
        variable: variable.into(),
        property: property.into(),
    }
}
fn projection(name: &str, expression: ProjectionExpression) -> Projection {
    Projection {
        name: name.into(),
        expression,
    }
}
fn project(items: Vec<Projection>, input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Project {
        items,
        input: Box::new(input),
    }
}
fn distinct(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Distinct {
        input: Box::new(input),
    }
}
fn col(name: &str) -> ProjectionExpression {
    ProjectionExpression::Column(name.into())
}
fn count(name: &str) -> Aggregation {
    Aggregation {
        name: name.into(),
        function: AggregateFunction::Count,
        target: AggregateTarget::All,
        distinct: false,
    }
}
fn run(
    plan: LogicalPlan,
    catalog: &OptimizerCatalog,
    disabled: Option<OptionalOptimizerRule>,
) -> (LogicalPlan, Vec<RuleEvent>) {
    let mut events = Vec::new();
    let options = disabled.map_or_else(OptimizerQualificationOptions::default, |rule| {
        OptimizerQualificationOptions::default().without_rule(rule)
    });
    let plan = rewrite(plan, catalog, options, &mut events);
    (plan, events)
}
fn hit(events: &[RuleEvent], rule: OptionalOptimizerRule) -> bool {
    events
        .iter()
        .any(|event| event.rule() == rule.as_str() && event.outcome() == RuleOutcome::Applied)
}

#[test]
fn nullable_schema_unique_needs_nonnull_and_statistics_never_prove_a_key() {
    let catalog = catalog(true, PropertyType::String);
    let source = scan("n", "Item");
    let items = vec![projection("id", property("n", "external"))];
    let (plan, events) = run(
        distinct(project(items.clone(), source.clone())),
        &catalog,
        None,
    );
    assert!(matches!(plan, LogicalPlan::Distinct { .. }));
    assert!(!hit(&events, OptionalOptimizerRule::RedundantDistinct));
    let filtered = LogicalPlan::Filter {
        predicate: Predicate::PropertyIsNotNull {
            variable: "n".into(),
            property: "external".into(),
        },
        input: Box::new(source),
    };
    let (plan, events) = run(distinct(project(items.clone(), filtered)), &catalog, None);
    assert!(matches!(plan, LogicalPlan::Project { .. }));
    assert!(hit(&events, OptionalOptimizerRule::RedundantDistinct));
    let mut statistics_only = OptimizerCatalog::default();
    statistics_only.label_counts.insert("Item".into(), 100);
    statistics_only
        .property_distinct_counts
        .insert(("Item".into(), "external".into()), 100);
    assert!(matches!(
        run(
            distinct(project(items, scan("n", "Item"))),
            &statistics_only,
            None
        )
        .0,
        LogicalPlan::Distinct { .. }
    ));
}

#[test]
fn joins_require_both_entity_keys_and_never_promote_one_fanout_binding_to_a_row_key() {
    let catalog = catalog(false, PropertyType::String);
    let join = LogicalPlan::NodeCartesianProduct {
        left: Box::new(scan("a", "Item")),
        right: Box::new(scan("b", "Other")),
    };
    let left = projection("left", property("a", "external"));
    let right = projection("right", property("b", "external"));
    assert!(matches!(
        run(
            distinct(project(vec![left.clone()], join.clone())),
            &catalog,
            None
        )
        .0,
        LogicalPlan::Distinct { .. }
    ));
    let (plan, events) = run(distinct(project(vec![left, right], join)), &catalog, None);
    assert!(matches!(plan, LogicalPlan::Project { .. }));
    assert!(hit(&events, OptionalOptimizerRule::RedundantDistinct));
    let left = project(
        vec![projection("same", property("a", "external"))],
        scan("a", "Item"),
    );
    let right = project(
        vec![projection("same", property("b", "external"))],
        scan("b", "Other"),
    );
    let collision = LogicalPlan::NodeCartesianProduct {
        left: Box::new(left),
        right: Box::new(right),
    };
    assert!(matches!(
        run(distinct(collision), &catalog, None).0,
        LogicalPlan::Distinct { .. }
    ));
}

#[test]
fn constants_and_equivalences_remove_only_proved_ordering_keys_and_obey_disabling() {
    let catalog = catalog(false, PropertyType::String);
    let source = LogicalPlan::Filter {
        predicate: Predicate::And(vec![
            Predicate::PropertyEq {
                variable: "n".into(),
                property: "space".into(),
                value: Value::String("s".into()),
            },
            Predicate::ExpressionEq {
                expression: property("n", "a"),
                value: property("n", "b"),
            },
        ]),
        input: Box::new(scan("n", "Item")),
    };
    let input = project(
        vec![
            projection("space", property("n", "space")),
            projection("a", property("n", "a")),
            projection("b", property("n", "b")),
            projection(
                "id",
                ProjectionExpression::Id {
                    variable: "n".into(),
                },
            ),
        ],
        source,
    );
    let items = [
        ("space", SortDirection::Asc),
        ("a", SortDirection::Asc),
        ("b", SortDirection::Desc),
        ("id", SortDirection::Asc),
    ]
    .into_iter()
    .map(|(name, direction)| SortItem {
        key: SortKey::Column(name.into()),
        direction,
    })
    .collect();
    let original = LogicalPlan::Sort {
        items,
        input: Box::new(input),
    };
    let (plan, events) = run(original.clone(), &catalog, None);
    let LogicalPlan::Sort { items, .. } = plan else {
        panic!("nonconstant ordering must remain")
    };
    assert_eq!(
        items.iter().map(|item| &item.key).collect::<Vec<_>>(),
        vec![&SortKey::Column("a".into()), &SortKey::Column("id".into())]
    );
    assert!(hit(&events, OptionalOptimizerRule::RedundantOrdering));
    let (disabled, events) = run(
        original.clone(),
        &catalog,
        Some(OptionalOptimizerRule::RedundantOrdering),
    );
    assert_eq!(disabled, original);
    assert!(!hit(&events, OptionalOptimizerRule::RedundantOrdering));
    assert!(events.iter().any(
        |event| event.outcome() == RuleOutcome::Skipped && event.detail().contains("disabled")
    ));
}

#[test]
fn unsupported_domains_and_late_errors_do_not_acquire_a_rewrite_proof() {
    for value_type in [PropertyType::Any, PropertyType::Float] {
        let catalog = catalog(false, value_type);
        let input = LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: "n".into(),
                property: "external".into(),
                value: Value::Float(0.0),
            },
            input: Box::new(scan("n", "Item")),
        };
        let plan = LogicalPlan::Sort {
            items: vec![SortItem {
                key: SortKey::Property {
                    variable: "n".into(),
                    property: "external".into(),
                },
                direction: SortDirection::Asc,
            }],
            input: Box::new(input),
        };
        assert_eq!(run(plan.clone(), &catalog, None).0, plan);
    }
    let catalog = catalog(false, PropertyType::String);
    let items = vec![
        projection(
            "id",
            ProjectionExpression::Id {
                variable: "n".into(),
            },
        ),
        projection(
            "error",
            ProjectionExpression::Binary {
                left: Box::new(ProjectionExpression::Literal(Value::Int(1))),
                op: ScalarBinaryOp::Divide,
                right: Box::new(ProjectionExpression::Literal(Value::Int(0))),
            },
        ),
    ];
    let original = distinct(project(items, scan("n", "Item")));
    let (plan, events) = run(original.clone(), &catalog, None);
    assert_eq!(plan, original);
    assert!(!hit(&events, OptionalOptimizerRule::RedundantDistinct));
}

#[test]
fn singleton_group_rewrite_requires_values_only_scope_and_preserves_empty_group_semantics() {
    let catalog = catalog(false, PropertyType::String);
    let inner = LogicalPlan::Aggregate {
        group_keys: vec![projection("g", property("n", "a"))],
        items: vec![count("total")],
        input: Box::new(scan("n", "Item")),
    };
    let outer = LogicalPlan::Aggregate {
        group_keys: vec![projection("g", col("g"))],
        items: vec![count("one")],
        input: Box::new(inner.clone()),
    };
    let (plan, events) = run(outer, &catalog, None);
    let LogicalPlan::Project { items, input } = plan else {
        panic!("proved one-row groups should project")
    };
    assert_eq!(
        items[1].expression,
        ProjectionExpression::Literal(Value::Int(1))
    );
    assert!(matches!(*input, LogicalPlan::Aggregate { .. }));
    assert!(hit(&events, OptionalOptimizerRule::SingletonGroup));
    let ungrouped = LogicalPlan::Aggregate {
        group_keys: vec![],
        items: vec![count("all")],
        input: Box::new(inner),
    };
    assert_eq!(run(ungrouped.clone(), &catalog, None).0, ungrouped);
    let graph_scope = LogicalPlan::Aggregate {
        group_keys: vec![projection("id", property("n", "external"))],
        items: vec![count("one")],
        input: Box::new(scan("n", "Item")),
    };
    assert_eq!(
        run(graph_scope.clone(), &catalog, None).0,
        graph_scope,
        "Project would preserve graph bindings that Aggregate clears"
    );
}

#[test]
fn duplicate_aliases_and_proof_depth_limits_fall_back_without_partial_keys() {
    let catalog = catalog(false, PropertyType::String);
    let original = distinct(project(
        vec![
            projection("same", property("n", "external")),
            projection("same", property("n", "a")),
        ],
        scan("n", "Item"),
    ));
    assert_eq!(run(original.clone(), &catalog, None).0, original);
    let mut deep = scan("n", "Item");
    for _ in 0..140 {
        deep = project(vec![], deep);
    }
    let (plan, events) = run(deep.clone(), &catalog, None);
    assert_eq!(plan, deep);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.rule() == "transformation:proof_budget")
            .count(),
        1
    );
}
