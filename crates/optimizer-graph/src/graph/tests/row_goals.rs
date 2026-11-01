// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::super::{costing, OptimizerCatalog};
use hawdb_core::Value;
use hawdb_plan_cypher::{PhysicalPlan, Predicate, SortDirection, SortItem, SortKey};

fn catalog() -> OptimizerCatalog {
    let mut catalog = OptimizerCatalog::default();
    catalog.label_counts.insert("Memory".into(), 100_000);
    catalog
        .property_distinct_counts
        .insert(("Memory".into(), "space".into()), 100);
    catalog
}

fn scan() -> PhysicalPlan {
    PhysicalPlan::SeqNodeScan {
        variable: "n".into(),
        label: "Memory".into(),
    }
}

fn filtered() -> PhysicalPlan {
    PhysicalPlan::FilterExec {
        predicate: Predicate::PropertyEq {
            variable: "n".into(),
            property: "space".into(),
            value: Value::String("default".into()),
        },
        input: Box::new(scan()),
    }
}

fn limit(input: PhysicalPlan, offset: usize, limit: Option<usize>) -> PhysicalPlan {
    PhysicalPlan::LimitExec {
        offset,
        limit,
        input: Box::new(input),
    }
}

fn sort_keys() -> Vec<SortItem> {
    vec![SortItem {
        key: SortKey::Property {
            variable: "n".into(),
            property: "id".into(),
        },
        direction: SortDirection::Asc,
    }]
}

#[test]
fn one_percent_filter_goal_preserves_selectivity_and_scan_startup() {
    let catalog = catalog();
    let cost =
        costing::estimate_physical_plan_cost_breakdown(&limit(filtered(), 0, Some(20)), &catalog);
    assert_eq!(cost.estimated_rows, 20);
    assert_eq!(cost.sequential_io, 2004, "20 / 1% inputs plus startup");
    assert_eq!(cost.cpu, 2020, "filter visits plus returned LIMIT rows");
    let offset =
        costing::estimate_physical_plan_cost_breakdown(&limit(filtered(), 10, Some(20)), &catalog);
    assert_eq!(offset.estimated_rows, 20);
    assert_eq!(offset.sequential_io, 3004);
    assert_eq!(offset.cpu, 3020);
    assert_eq!(
        costing::estimate_physical_plan_cost_breakdown(&filtered(), &catalog).estimated_rows,
        1000
    );
}

#[test]
fn blocking_operators_keep_complete_input_work_under_a_parent_goal() {
    let catalog = catalog();
    for input in [
        PhysicalPlan::SortExec {
            items: sort_keys(),
            input: Box::new(filtered()),
        },
        PhysicalPlan::TopNExec {
            items: sort_keys(),
            offset: 0,
            limit: 100,
            input: Box::new(filtered()),
        },
        PhysicalPlan::DistinctExec {
            input: Box::new(filtered()),
        },
    ] {
        let full = costing::estimate_physical_plan_cost_breakdown(&input, &catalog);
        let bounded =
            costing::estimate_physical_plan_cost_breakdown(&limit(input, 0, Some(20)), &catalog);
        assert_eq!(bounded.estimated_rows, 20);
        assert_eq!(bounded.sequential_io, full.sequential_io);
        assert!(bounded.cpu >= full.cpu);
    }
}

#[test]
fn nested_offsets_zero_unbounded_and_saturated_goals_are_distinct() {
    let mut catalog = catalog();
    catalog.label_counts.insert("Memory".into(), 1000);
    let nested = limit(limit(scan(), 10, Some(100)), 5, Some(20));
    let cost = costing::estimate_physical_plan_cost_breakdown(&nested, &catalog);
    assert_eq!(
        (cost.estimated_rows, cost.cpu, cost.sequential_io),
        (20, 45, 39)
    );
    for offset in [0, 100, usize::MAX] {
        let cost = costing::estimate_physical_plan_cost_breakdown(
            &limit(scan(), offset, Some(0)),
            &catalog,
        );
        assert_eq!(cost.sequential_io, 0, "LIMIT 0 must not consume OFFSET");
    }
    for count in [None, Some(usize::MAX)] {
        let cost =
            costing::estimate_physical_plan_cost_breakdown(&limit(scan(), 5, count), &catalog);
        assert_eq!(cost.sequential_io, 1004);
    }
    let exhausted = costing::estimate_physical_plan_cost_breakdown(
        &limit(scan(), usize::MAX, Some(20)),
        &catalog,
    );
    assert_eq!(exhausted.sequential_io, 1004);
}

#[test]
fn row_goals_keep_one_cost_evaluation_per_operator() {
    let catalog = catalog();
    let mut plan = scan();
    for _ in 0..32 {
        plan = PhysicalPlan::ProjectExec {
            items: Vec::new(),
            input: Box::new(plan),
        };
    }
    let plan = limit(plan, 0, Some(20));
    costing::take_cost_evaluations();
    let cost = costing::estimate_physical_plan_cost_breakdown(&plan, &catalog);
    assert_eq!(costing::take_cost_evaluations(), 34);
    assert_eq!(cost.sequential_io, 24);
}

#[test]
fn streaming_probes_and_expand_fanout_scale_consumed_work() {
    use hawdb_cypher::RelationshipDirection;
    let mut catalog = catalog();
    catalog.path_counts.insert(
        ("Memory".into(), "LINKS".into(), "Memory".into()),
        1_000_000,
    );
    let expand = PhysicalPlan::AdjacencyExpandExec {
        source_variable: "n".into(),
        source_label: "Memory".into(),
        rel_variable: None,
        rel_type: "LINKS".into(),
        rel_properties: Default::default(),
        direction: RelationshipDirection::Outgoing,
        target_variable: "m".into(),
        target_label: "Memory".into(),
        min_hops: 1,
        max_hops: 1,
        optional: false,
        graph_budget: None,
        input: Box::new(scan()),
    };
    let cost =
        costing::estimate_physical_plan_cost_breakdown(&limit(expand, 0, Some(20)), &catalog);
    assert_eq!(
        cost.sequential_io, 6,
        "two input seeds at estimated fanout ten, plus startup"
    );
    assert_eq!(cost.random_io, 22, "two probes and twenty outputs");
    let lookup = PhysicalPlan::NodeColumnLookupExec {
        variable: "m".into(),
        label: "Memory".into(),
        property: "id".into(),
        column: "key".into(),
        optional: false,
        node_visibility_predicate: None,
        input: Box::new(filtered()),
    };
    let cost =
        costing::estimate_physical_plan_cost_breakdown(&limit(lookup, 0, Some(20)), &catalog);
    assert_eq!(cost.sequential_io, 2004);
    assert_eq!(
        cost.random_io, 2_000_000,
        "preserve conservative per-probe label lookup cost"
    );
}

#[test]
fn retrieval_and_unknown_statistics_never_turn_output_goals_into_scan_caps() {
    let catalog = catalog();
    let seed = PhysicalPlan::GraphSeedScan {
        query_parameter: "q".into(),
        label: "Memory".into(),
        variable: "n".into(),
        score_column: "score".into(),
        top_k: 100,
        node_visibility_predicate: None,
    };
    let full = costing::estimate_physical_plan_cost_breakdown(&seed, &catalog);
    let bounded =
        costing::estimate_physical_plan_cost_breakdown(&limit(seed, 0, Some(20)), &catalog);
    assert_eq!(bounded.sequential_io, full.sequential_io);
    assert!(bounded.cpu >= full.cpu);
    let mut absent = catalog.clone();
    absent.property_distinct_counts.clear();
    let full = costing::estimate_physical_plan_cost_breakdown(&filtered(), &absent);
    let wanted = 20.min(full.estimated_rows);
    let bounded =
        costing::estimate_physical_plan_cost_breakdown(&limit(filtered(), 0, Some(20)), &absent);
    assert_eq!(bounded.estimated_rows, wanted);
    assert!(bounded.sequential_io >= wanted + 4);
    assert!(bounded.sequential_io <= full.sequential_io);
}

#[test]
fn bounded_ordered_choice_requires_a_nonrejecting_residual_or_zero_goal() {
    use super::super::{
        CascadesOptimizer, OptimizerCatalogIndexes, OptimizerCatalogStatistics, OptimizerConfig,
    };
    use hawdb_core::PropertyType;
    use hawdb_plan_cypher::{LogicalPlan, Projection, ProjectionExpression};
    let properties = vec!["space".into(), "rank".into(), "id".into()];
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [("Memory".into(), "kind".into())],
            [("Memory".into(), properties)],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Memory".into(), 1_000_000)],
            [],
            [],
            [],
            [],
            [
                (("Memory".into(), "space".into()), 1),
                (("Memory".into(), "kind".into()), 100),
            ],
            [],
        ),
    )
    .with_ordered_composite_indexes([(
        "Memory".into(),
        vec![
            ("space".into(), PropertyType::String),
            ("rank".into(), PropertyType::Int),
            ("id".into(), PropertyType::String),
        ],
    )]);
    let logical = LogicalPlan::Sort {
        items: ["rank", "id"]
            .map(|name| SortItem {
                key: SortKey::Column(name.into()),
                direction: SortDirection::Asc,
            })
            .to_vec(),
        input: Box::new(LogicalPlan::Project {
            items: ["rank", "id"]
                .map(|name| Projection {
                    name: name.into(),
                    expression: ProjectionExpression::Property {
                        variable: "n".into(),
                        property: name.into(),
                    },
                })
                .to_vec(),
            input: Box::new(LogicalPlan::Filter {
                predicate: Predicate::And(
                    [("space", "default"), ("kind", "selected")]
                        .map(|(property, value)| Predicate::PropertyEq {
                            variable: "n".into(),
                            property: property.into(),
                            value: Value::String(value.into()),
                        })
                        .to_vec(),
                ),
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "n".into(),
                    label: "Memory".into(),
                }),
            }),
        }),
    };
    for max_groups in [1, 1000] {
        let optimizer = CascadesOptimizer::new(OptimizerConfig { max_groups });
        let (full, _) = optimizer.optimize_with_catalog(&logical, &catalog);
        assert!(
            !full.explain(0).contains("CompositeOrdered"),
            "{}",
            full.explain(0)
        );
        let bounded = LogicalPlan::Limit {
            offset: 0,
            limit: Some(20),
            input: Box::new(logical.clone()),
        };
        let mut small = catalog.clone();
        small.label_counts.insert("Memory".into(), 100_000);
        let (small_plan, _) = optimizer.optimize_with_catalog(&bounded, &small);
        assert!(
            !small_plan.explain(0).contains("CompositeOrdered"),
            "{}",
            small_plan.explain(0)
        );
        let (plan, trace) = optimizer.optimize_with_catalog(&bounded, &catalog);
        let explain = plan.explain(0);
        assert!(
            !explain.contains("CompositeOrdered"),
            "{explain}; {:?}",
            trace.decisions
        );
        assert!(trace
            .decisions
            .iter()
            .any(|decision| decision.contains("unproven residual distribution row_goal=Some(20)")));
        let zero = LogicalPlan::Limit {
            offset: usize::MAX,
            limit: Some(0),
            input: Box::new(logical.clone()),
        };
        let (plan, trace) = optimizer.optimize_with_catalog(&zero, &catalog);
        assert!(matches!(plan, PhysicalPlan::EmptyExec));
        assert_eq!(trace.selected_plan_cost_breakdown.cost, 0);

        // A prefix-only predicate cannot reject ordered access output, so a
        // nonzero goal is safe without assuming any distribution of matches.
        let mut safe = logical.clone();
        let LogicalPlan::Sort { input, .. } = &mut safe else {
            unreachable!()
        };
        let LogicalPlan::Project { input, .. } = input.as_mut() else {
            unreachable!()
        };
        let LogicalPlan::Filter {
            predicate: Predicate::And(parts),
            ..
        } = input.as_mut()
        else {
            unreachable!()
        };
        parts.truncate(1);
        let (unbounded, unbounded_trace) = optimizer.optimize_with_catalog(&safe, &catalog);
        assert!(unbounded.explain(0).contains("CompositeOrdered"));
        assert_eq!(unbounded_trace.selected_plan_cost.estimated_rows, 1_000_000);
        assert_eq!(unbounded_trace.selected_plan_cost.cost, 4_000_002);
        assert!(unbounded_trace
            .decisions
            .iter()
            .any(|decision| decision.contains("row_goal=None")));
        let bounded = LogicalPlan::Limit {
            offset: 0,
            limit: Some(20),
            input: Box::new(safe),
        };
        let (plan, trace) = optimizer.optimize_with_catalog(&bounded, &catalog);
        assert!(plan.explain(0).contains("CompositeOrdered"));
        assert!(trace
            .decisions
            .iter()
            .any(|decision| decision.contains("row_goal=Some(20)")));
    }
}

#[test]
fn joint_filter_statistics_feed_row_goals_and_keep_selected_provenance() {
    use crate::graph::{
        OptimizerCatalogIndexes, OptimizerCatalogStatistics, OptimizerIndexStatistics,
    };
    let catalog = |joint: bool| {
        let index = ("Memory".into(), vec!["a".into(), "b".into()]);
        OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [index.clone()], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".into(), 10_000)],
                [],
                [],
                [],
                [],
                [
                    (("Memory".into(), "a".into()), 100),
                    (("Memory".into(), "b".into()), 100),
                ],
                [],
            )
            .with_composite_index_statistics(joint.then_some((
                index,
                OptimizerIndexStatistics {
                    index_size: 10_000,
                    distinct_count: 100,
                },
            ))),
        )
    };
    let filtered = PhysicalPlan::FilterExec {
        predicate: Predicate::And(
            ["a", "b"]
                .map(|property| Predicate::PropertyEq {
                    variable: "n".into(),
                    property: property.into(),
                    value: Value::Int(1),
                })
                .to_vec(),
        ),
        input: Box::new(scan()),
    };
    let joint = catalog(true);
    assert_eq!(
        costing::estimate_physical_plan_cost_breakdown(&filtered, &joint).estimated_rows,
        100
    );
    let limited = PhysicalPlan::LimitExec {
        offset: 0,
        limit: Some(2),
        input: Box::new(filtered),
    };
    let (trace, diagnostics) = super::super::selected_trace::selected_plan_trace_with_diagnostics(
        &limited,
        &joint,
        &crate::OptimizerContext::default(),
    );
    assert_eq!(trace.cost.estimated_rows, 2);
    assert_eq!(
        diagnostics
            .iter()
            .filter(|line| line.starts_with("cardinality statistics: "))
            .count(),
        1
    );
    let independent = costing::estimate_physical_plan_cost_breakdown(&limited, &catalog(false));
    assert!(
        trace.cost.cost < independent.cost / 10,
        "joint selectivity must reach consumed-work costing rather than leaving the full scan cost"
    );
}
