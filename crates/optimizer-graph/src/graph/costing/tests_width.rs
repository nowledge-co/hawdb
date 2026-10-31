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
            for memory in [1024 * 1024, 20 * 1024 * 1024, 1024 * 1024 * 1024] {
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
