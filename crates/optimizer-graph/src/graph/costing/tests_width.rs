// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    CascadesOptimizer, LogicalPlanRoot, OptimizerContext, OptimizerSearchDirective, ResourceHints,
};
use hawdb_core::{Catalog, PropertyType, TableKind, Value};
use hawdb_plan_cypher::{
    LogicalPlan, NodeProjectionAccess, Projection, ProjectionExpression, SortDirection, SortItem,
    SortKey,
};

fn catalog(wide: bool) -> OptimizerCatalog {
    let mut schema = Catalog::default();
    let table = schema.get_or_create_table(TableKind::Node, "Item");
    schema.get_or_create_property(table, "id", PropertyType::Int, false);
    schema.get_or_create_property(
        table,
        "body",
        if wide {
            PropertyType::Text
        } else {
            PropertyType::Int
        },
        true,
    );
    let mut catalog = crate::graph::optimizer_catalog_from_graph_statistics(
        &schema,
        &hawdb_core::GraphStatistics::default(),
    );
    catalog.label_counts.insert("Item".into(), 10_000);
    catalog
}

fn projections() -> Vec<Projection> {
    ["id", "body"]
        .into_iter()
        .map(|name| Projection {
            name: name.into(),
            expression: ProjectionExpression::Property {
                variable: "n".into(),
                property: name.into(),
            },
        })
        .collect()
}

fn scan() -> PhysicalPlan {
    PhysicalPlan::SeqNodeScan {
        variable: "n".into(),
        label: "Item".into(),
    }
}

fn fused() -> PhysicalPlan {
    PhysicalPlan::NodeProjectionScanExec {
        variable: "n".into(),
        label: "Item".into(),
        access: NodeProjectionAccess::LabelScan,
        required_properties: vec!["body".into(), "id".into()],
        predicate: None,
        items: projections(),
    }
}

fn order() -> Vec<SortItem> {
    vec![SortItem {
        key: SortKey::Column("id".into()),
        direction: SortDirection::Asc,
    }]
}

#[test]
fn width_cost_preserves_graph_ownership_and_prices_only_required_decode_fields() {
    let wide = catalog(true);
    let narrow = catalog(false);
    let scan_plan = scan();
    let full = estimate_costed_plan(&scan_plan, &wide);
    let fused_plan = fused();
    let values = estimate_costed_plan(&fused_plan, &wide);
    assert!(full.shape.retained_bytes() > values.shape.retained_bytes() - 128);
    let projected_plan = PhysicalPlan::ProjectExec {
        items: projections(),
        input: Box::new(scan()),
    };
    let projected = estimate_costed_plan(&projected_plan, &wide);
    assert_eq!(projected.shape.graph_bytes, full.shape.graph_bytes);
    assert!(projected.shape.retained_bytes() > values.shape.retained_bytes());
    assert!(
        estimate_physical_plan_cost(&scan(), &wide).cost
            > estimate_physical_plan_cost(&scan(), &narrow).cost
    );
    let mut id_only = fused();
    if let PhysicalPlan::NodeProjectionScanExec {
        required_properties,
        items,
        ..
    } = &mut id_only
    {
        required_properties.retain(|name| name == "id");
        items.retain(|item| item.name == "id");
    }
    assert_eq!(
        estimate_physical_plan_cost(&id_only, &wide),
        estimate_physical_plan_cost(&id_only, &narrow)
    );
    assert!(values.width_report.unwrap().proxy_fields > 0);
}

#[test]
fn sort_width_charges_payload_retention_and_key_comparisons_separately() {
    let wide = catalog(true);
    let narrow = catalog(false);
    let plan = PhysicalPlan::SortExec {
        items: order(),
        input: Box::new(fused()),
    };
    let narrow_cost = estimate_costed_plan(&plan, &narrow);
    let wide_cost = estimate_costed_plan(&plan, &wide);
    let small = narrow_cost.width_report.unwrap();
    let big = wide_cost.width_report.unwrap();
    assert_eq!(
        small.comparison_work, big.comparison_work,
        "unused body is not compared or copied on every header move"
    );
    assert!(big.retained_bytes > small.retained_bytes);
    let body_sort = PhysicalPlan::SortExec {
        items: vec![SortItem {
            key: SortKey::Column("body".into()),
            direction: SortDirection::Asc,
        }],
        input: Box::new(fused()),
    };
    assert!(
        estimate_costed_plan(&body_sort, &wide)
            .width_report
            .unwrap()
            .comparison_work
            > big.comparison_work
    );
    assert_eq!(wide_cost.cost.output_rows, narrow_cost.cost.output_rows);
    assert_eq!(
        wide_cost.cost.estimated_rows,
        narrow_cost.cost.estimated_rows
    );
}

#[test]
fn memory_hint_changes_actual_bounded_choice_in_both_lowerings_and_selected_cost() {
    let logical = LogicalPlan::Limit {
        offset: 0,
        limit: Some(2000),
        input: Box::new(LogicalPlan::Sort {
            items: order(),
            input: Box::new(LogicalPlan::Project {
                items: projections(),
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "n".into(),
                    label: "Item".into(),
                }),
            }),
        }),
    };
    for directive in [
        OptimizerSearchDirective::Auto,
        OptimizerSearchDirective::Memo,
        OptimizerSearchDirective::DirectFallback,
    ] {
        for wide in [false, true] {
            for memory in [
                1024 * 1024,
                20 * 1024 * 1024,
                64 * 1024 * 1024,
                1024 * 1024 * 1024,
            ] {
                let context = OptimizerContext::default().with_resource_hints(ResourceHints {
                    max_memory_bytes: Some(memory),
                    ..ResourceHints::default()
                });
                let catalog = catalog(wide);
                let (plan, mut trace) = CascadesOptimizer::with_context(context.clone())
                    .optimize_root_with_catalog_and_directive(
                        &LogicalPlanRoot::new(logical.clone()),
                        &catalog,
                        directive,
                    )
                    .unwrap()
                    .into_parts();
                let top_n = wide
                    && memory == 20 * 1024 * 1024
                    && !cfg!(all(target_arch = "wasm32", target_os = "unknown"));
                assert_eq!(
                    plan.explain(0).contains("TopNExec"),
                    top_n,
                    "{} {:?}",
                    plan.explain(0),
                    trace.decisions
                );
                assert_eq!(
                    trace.selected_plan_cost,
                    estimate_physical_plan_cost_with_context(&plan, &catalog, &context)
                );
                let prior = trace
                    .decisions
                    .iter()
                    .filter(|entry| entry.starts_with("physical width cost: "))
                    .count();
                assert!(prior > 0);
                CascadesOptimizer::with_context(context)
                    .refresh_trace_for_physical_plan(&mut trace, &plan, &catalog);
                assert_eq!(
                    trace
                        .decisions
                        .iter()
                        .filter(|entry| entry.starts_with("physical width cost: "))
                        .count(),
                    prior
                );
            }
        }
    }
}

#[test]
fn estimated_input_within_window_prefers_bounded_retention_in_both_lowerings() {
    for directive in [
        OptimizerSearchDirective::Memo,
        OptimizerSearchDirective::DirectFallback,
    ] {
        for rows in [1, 2, 25] {
            for offset in [0, 3] {
                let mut catalog = catalog(true);
                catalog.label_counts.insert("Item".into(), rows);
                let sort = PhysicalPlan::SortExec {
                    items: order(),
                    input: Box::new(fused()),
                };
                let top_n = PhysicalPlan::TopNExec {
                    items: order(),
                    offset,
                    limit: 25,
                    input: Box::new(fused()),
                };
                let sort_cost = estimate_costed_plan(&sort, &catalog);
                let top_n_cost = estimate_costed_plan(&top_n, &catalog);
                assert_eq!(top_n_cost.cost.cpu, sort_cost.cost.cpu);
                assert_eq!(
                    top_n_cost.width_report.unwrap().retained_bytes,
                    sort_cost.width_report.unwrap().retained_bytes,
                );
                let logical = LogicalPlan::Limit {
                    offset,
                    limit: Some(25),
                    input: Box::new(LogicalPlan::Sort {
                        items: order(),
                        input: Box::new(LogicalPlan::Project {
                            items: projections(),
                            input: Box::new(LogicalPlan::NodeScan {
                                variable: "n".into(),
                                label: "Item".into(),
                            }),
                        }),
                    }),
                };
                let (plan, trace) = CascadesOptimizer::default()
                    .optimize_root_with_catalog_and_directive(
                        &LogicalPlanRoot::new(logical),
                        &catalog,
                        directive,
                    )
                    .unwrap()
                    .into_parts();
                assert!(plan.explain(0).contains("TopNExec"), "{}", plan.explain(0));
                assert_eq!(
                    trace.selected_plan_cost,
                    estimate_physical_plan_cost(&plan, &catalog)
                );
            }
        }
    }
}

#[test]
fn alias_scope_keeps_input_width_before_replacing_output_columns() {
    let catalog = catalog(true);
    let renamed = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            name: "body".into(),
            expression: ProjectionExpression::Column("id".into()),
        }],
        input: Box::new(fused()),
    };
    let plan = PhysicalPlan::SortExec {
        items: vec![SortItem {
            key: SortKey::Column("body".into()),
            direction: SortDirection::Asc,
        }],
        input: Box::new(renamed),
    };
    let costed = estimate_costed_plan(&plan, &catalog);
    let base = PhysicalPlan::SortExec {
        items: order(),
        input: Box::new(fused()),
    };
    assert_eq!(
        costed.width_report.unwrap().comparison_work,
        estimate_costed_plan(&base, &catalog)
            .width_report
            .unwrap()
            .comparison_work
    );
    let literal = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            name: "body".into(),
            expression: ProjectionExpression::Literal(Value::String("x".repeat(700))),
        }],
        input: Box::new(PhysicalPlan::EmptyExec),
    };
    assert!(
        estimate_costed_plan(&literal, &catalog)
            .shape
            .retained_bytes()
            >= 700
    );
}

#[test]
fn width_cost_bounds_metadata_and_saturates_without_turning_rows_into_bytes() {
    let mut catalog = catalog(true);
    catalog.label_counts.insert("Item".into(), u64::MAX);
    let plan = PhysicalPlan::SortExec {
        items: order(),
        input: Box::new(fused()),
    };
    let context = OptimizerContext::default().with_resource_hints(ResourceHints {
        max_memory_bytes: Some(1),
        ..ResourceHints::default()
    });
    let cost = estimate_physical_plan_cost_with_context(&plan, &catalog, &context);
    assert_eq!(cost.estimated_rows, u64::MAX);
    assert_eq!(cost.cost, u64::MAX);
    let projections = (0..1000)
        .map(|id| Projection {
            name: format!("column{id}"),
            expression: ProjectionExpression::Literal(Value::Int(id)),
        })
        .collect();
    let wide_plan = PhysicalPlan::ProjectExec {
        items: projections,
        input: Box::new(PhysicalPlan::EmptyExec),
    };
    let wide = estimate_costed_plan(&wide_plan, &catalog);
    assert_eq!(wide.shape.column_count(), 64);
    assert!(wide.shape.retained_bytes() > 1000 * std::mem::size_of::<Value>() as u64);
    assert!(
        wide.shape.uncertain_fields() > 0,
        "overflow uses a positive opaque proxy"
    );
}

#[test]
fn required_graph_projection_reports_opaque_width_sources() {
    let mut plan = fused();
    if let PhysicalPlan::NodeProjectionScanExec { items, .. } = &mut plan {
        items.clear();
    }
    let costed = estimate_costed_plan(&plan, &OptimizerCatalog::default());
    assert_eq!(costed.width_report.unwrap().proxy_fields, 2);
    assert!(costed.shape.graph_bytes > 0);
    assert_eq!(costed.shape.column_count(), 0);
}

#[test]
fn hash_join_retention_and_spill_follow_the_right_build_side() {
    let mut schema = Catalog::default();
    for (label, body_type) in [("Wide", PropertyType::Text), ("Narrow", PropertyType::Int)] {
        let table = schema.get_or_create_table(TableKind::Node, label);
        schema.get_or_create_property(table, "id", PropertyType::Int, false);
        schema.get_or_create_property(table, "body", body_type, false);
    }
    let mut catalog = crate::graph::optimizer_catalog_from_graph_statistics(
        &schema,
        &hawdb_core::GraphStatistics::default(),
    );
    catalog.label_counts.insert("Wide".into(), 10_000);
    catalog.label_counts.insert("Narrow".into(), 3);
    for (left_label, right_label, spills) in [("Wide", "Narrow", false), ("Narrow", "Wide", true)] {
        let plan = PhysicalPlan::HashJoinExec {
            left_key: hawdb_plan_cypher::HashJoinKey {
                variable: "l".into(),
                property: "id".into(),
            },
            right_key: hawdb_plan_cypher::HashJoinKey {
                variable: "r".into(),
                property: "id".into(),
            },
            left: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "l".into(),
                label: left_label.into(),
            }),
            right: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "r".into(),
                label: right_label.into(),
            }),
        };
        let PhysicalPlan::HashJoinExec { right, .. } = &plan else {
            unreachable!()
        };
        let build = estimate_costed_plan(right, &catalog);
        let expected_retention = build.cost.estimated_rows * (build.shape.retained_bytes() + 64);
        let inputs = estimate_input_costs(&plan, |input| estimate_costed_plan(input, &catalog));
        let costed =
            estimate_operator_cost_internal(&plan, &catalog, inputs, None, Some(32 * 1024));
        let report = costed.width_report.unwrap();
        assert_eq!(report.retained_bytes, expected_retention);
        assert_eq!(
            report.spill_work > 0,
            spills && !cfg!(target_arch = "wasm32")
        );
        assert_eq!(costed.cost.estimated_rows, 30_000);
    }
}

#[test]
fn traversal_opaque_width_remains_visible_in_sort_reports() {
    let catalog = catalog(false);
    for hops in 1..=3 {
        let mut input = scan();
        for hop in 0..hops {
            input = PhysicalPlan::AdjacencyExpandExec {
                source_variable: if hop == 0 {
                    "n".into()
                } else {
                    format!("n{hop}")
                },
                source_label: "Item".into(),
                rel_variable: None,
                rel_type: "RELATED".into(),
                rel_properties: Default::default(),
                direction: hawdb_core::RelationshipDirection::Outgoing,
                target_variable: format!("n{}", hop + 1),
                target_label: "Item".into(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                graph_budget: None,
                input: Box::new(input),
            };
        }
        let plan = PhysicalPlan::SortExec {
            items: order(),
            input: Box::new(input),
        };
        let costed = estimate_costed_plan(&plan, &catalog);
        assert_eq!(costed.width_report.unwrap().proxy_fields, hops);
    }
}

#[test]
fn joint_group_cardinality_and_input_alias_width_share_the_cost_fold() {
    use crate::graph::{
        OptimizerCatalogIndexes, OptimizerCatalogStatistics, OptimizerIndexStatistics,
    };
    let mut schema = Catalog::default();
    let table = schema.get_or_create_table(TableKind::Node, "Item");
    for (name, kind) in [
        ("a", PropertyType::Int),
        ("b", PropertyType::Int),
        ("body", PropertyType::Text),
    ] {
        schema.get_or_create_property(table, name, kind, false);
    }
    for (joint, expected_rows, spills) in [(true, 20, false), (false, 400, true)] {
        let index = ("Item".into(), vec!["a".into(), "b".into()]);
        let mut catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [index.clone()], [], []),
            OptimizerCatalogStatistics::new(
                [("Item".into(), 400)],
                [],
                [],
                [],
                [],
                [
                    (("Item".into(), "a".into()), 20),
                    (("Item".into(), "b".into()), 20),
                ],
                [],
            )
            .with_composite_index_statistics(joint.then_some((
                index,
                OptimizerIndexStatistics {
                    index_size: 400,
                    distinct_count: 20,
                },
            ))),
        );
        catalog.source_schema = Some(schema.clone());
        let aggregate = PhysicalPlan::AggregateExec {
            group_keys: [("a", "body"), ("b", "group")]
                .map(|(property, name)| Projection {
                    name: name.into(),
                    expression: ProjectionExpression::Property {
                        variable: "n".into(),
                        property: property.into(),
                    },
                })
                .to_vec(),
            items: vec![hawdb_plan_cypher::Aggregation {
                function: hawdb_plan_cypher::AggregateFunction::Count,
                target: hawdb_plan_cypher::AggregateTarget::Variable("n".into()),
                distinct: false,
                name: "total".into(),
            }],
            input: Box::new(scan()),
        };
        let plan = PhysicalPlan::SortExec {
            items: vec![SortItem {
                key: SortKey::Column("body".into()),
                direction: SortDirection::Asc,
            }],
            input: Box::new(aggregate),
        };
        let inputs = estimate_input_costs(&plan, |input| estimate_costed_plan(input, &catalog));
        let costed =
            estimate_operator_cost_internal(&plan, &catalog, inputs, None, Some(32 * 1024));
        let report = costed.width_report.unwrap();
        assert_eq!(costed.cost.estimated_rows, expected_rows);
        assert_eq!(
            report.proxy_fields, 1,
            "only the accumulator is opaque; the body alias refers to the incoming INT, not TEXT"
        );
        assert_eq!(
            report.spill_work > 0,
            spills && !cfg!(target_arch = "wasm32")
        );
    }
}

#[test]
fn row_goals_retain_blocking_width_work_and_scale_streaming_decode() {
    let context = OptimizerContext::default().with_resource_hints(ResourceHints {
        max_memory_bytes: Some(1024 * 1024),
        ..ResourceHints::default()
    });
    let wide = catalog(true);
    let sorted = PhysicalPlan::SortExec {
        items: order(),
        input: Box::new(fused()),
    };
    let bounded = PhysicalPlan::LimitExec {
        offset: 0,
        limit: Some(7),
        input: Box::new(sorted.clone()),
    };
    let full = estimate_costed_plan_with_context(&sorted, &wide, &context);
    let page = estimate_costed_plan_with_context(&bounded, &wide, &context);
    assert_eq!(
        full.width_report.unwrap().spill_work > 0,
        !cfg!(all(target_arch = "wasm32", target_os = "unknown"))
    );
    assert_eq!(page.cost.estimated_rows, 7);
    assert_eq!(page.cost.sequential_io, full.cost.sequential_io);
    assert!(page.cost.cpu >= full.cost.cpu);

    let scan = fused();
    let bounded = PhysicalPlan::LimitExec {
        offset: 0,
        limit: Some(7),
        input: Box::new(scan.clone()),
    };
    let full = estimate_costed_plan_with_context(&scan, &wide, &context);
    let page = estimate_costed_plan_with_context(&bounded, &wide, &context);
    let narrow = catalog(false);
    let small = estimate_costed_plan_with_context(&bounded, &narrow, &context);
    assert!(page.cost.cpu < full.cost.cpu / 100);
    assert!(page.cost.cpu > small.cost.cpu);
    assert!(page.cost.sequential_io >= NODE_FULL_SCAN_STARTUP_COST);
}

#[test]
fn bounded_ordered_wide_payload_competitors_keep_the_memory_context() {
    let mut schema = Catalog::default();
    let table = schema.get_or_create_table(TableKind::Node, "Item");
    for (name, kind) in [
        ("kind", PropertyType::String),
        ("rank", PropertyType::Int),
        ("id", PropertyType::String),
        ("body", PropertyType::Text),
    ] {
        schema.get_or_create_property(table, name, kind, false);
    }
    let mut catalog = crate::graph::optimizer_catalog_from_graph_statistics(
        &schema,
        &hawdb_core::GraphStatistics::default(),
    );
    catalog.composite_property_indexes.insert((
        "Item".into(),
        vec!["kind".into(), "rank".into(), "id".into()],
    ));
    let mut catalog = catalog.with_ordered_composite_indexes([(
        "Item".into(),
        vec![
            ("kind".into(), PropertyType::String),
            ("rank".into(), PropertyType::Int),
            ("id".into(), PropertyType::String),
        ],
    )]);
    catalog.label_counts.insert("Item".into(), 10_000);
    catalog
        .property_distinct_counts
        .insert(("Item".into(), "kind".into()), 1);
    let logical = LogicalPlan::Limit {
        offset: 0,
        limit: Some(2000),
        input: Box::new(LogicalPlan::Sort {
            items: ["rank", "id"]
                .into_iter()
                .map(|name| SortItem {
                    key: SortKey::Column(name.into()),
                    direction: SortDirection::Asc,
                })
                .collect(),
            input: Box::new(LogicalPlan::Project {
                items: ["rank", "id", "body"]
                    .into_iter()
                    .map(|name| Projection {
                        name: name.into(),
                        expression: ProjectionExpression::Property {
                            variable: "n".into(),
                            property: name.into(),
                        },
                    })
                    .collect(),
                input: Box::new(LogicalPlan::Filter {
                    predicate: hawdb_plan_cypher::Predicate::PropertyEq {
                        variable: "n".into(),
                        property: "kind".into(),
                        value: Value::String("note".into()),
                    },
                    input: Box::new(LogicalPlan::NodeScan {
                        variable: "n".into(),
                        label: "Item".into(),
                    }),
                }),
            }),
        }),
    };
    for directive in [
        OptimizerSearchDirective::Memo,
        OptimizerSearchDirective::DirectFallback,
    ] {
        let mut competitor_costs = Vec::new();
        for memory in [20 * 1024 * 1024, 1024 * 1024 * 1024] {
            let context = OptimizerContext::default().with_resource_hints(ResourceHints {
                max_memory_bytes: Some(memory),
                ..ResourceHints::default()
            });
            let (plan, trace) = CascadesOptimizer::with_context(context.clone())
                .optimize_root_with_catalog_and_directive(
                    &LogicalPlanRoot::new(logical.clone()),
                    &catalog,
                    directive,
                )
                .unwrap()
                .into_parts();
            assert!(
                plan.explain(0).contains("IndexNodeCompositeOrderedSeek"),
                "{}",
                plan.explain(0)
            );
            let decision = trace
                .decisions
                .iter()
                .find(|entry| entry.starts_with("choose composite ordered seek:"))
                .unwrap();
            assert!(decision.contains("row_goal=Some(2000)"), "{decision}");
            competitor_costs.push(
                decision
                    .split("unordered_cost=")
                    .nth(1)
                    .unwrap()
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap(),
            );
            assert_eq!(
                trace.selected_plan_cost,
                estimate_physical_plan_cost_with_context(&plan, &catalog, &context)
            );
            assert!(trace
                .decisions
                .iter()
                .any(|entry| entry.starts_with("physical width cost:")
                    && entry.contains(&format!("memory_hint=Some({memory})"))));
        }
        if cfg!(all(target_arch = "wasm32", target_os = "unknown")) {
            assert_eq!(competitor_costs[0], competitor_costs[1]);
        } else {
            assert_ne!(
                competitor_costs[0], competitor_costs[1],
                "the bounded competitor must use the caller's spill hint"
            );
        }
    }
}
