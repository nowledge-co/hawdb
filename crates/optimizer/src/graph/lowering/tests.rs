use super::*;
use skein_plan::{Aggregation, SortDirection};

fn scan(variable: &str) -> LogicalPlan {
    LogicalPlan::NodeScan {
        variable: variable.to_string(),
        label: "Memory".to_string(),
    }
}

fn sort(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Sort {
        items: vec![SortItem {
            key: SortKey::Property {
                variable: "m".to_string(),
                property: "id".to_string(),
            },
            direction: SortDirection::Asc,
        }],
        input: Box::new(input),
    }
}

#[test]
fn bounded_sort_budget_counts_the_groups_that_are_actually_allocated() {
    let logical = LogicalPlan::Limit {
        offset: 1,
        limit: Some(3),
        input: Box::new(sort(scan("m"))),
    };
    let optimizer = CascadesOptimizer::new(OptimizerConfig { max_groups: 2 });
    let root = optimizer
        .optimize_root_with_catalog_and_directive(
            &LogicalPlanRoot::new(logical),
            &OptimizerCatalog::default(),
            OptimizerSearchDirective::Memo,
        )
        .expect("bounded sorting allocates a limit group and a scan group");
    assert_eq!(root.trace().groups, 2);
    assert!(root.trace().warnings.is_empty());
}

#[test]
fn seeded_child_resolution_preserves_full_plans_costs_and_rule_events() {
    let context = OptimizerContext::default();
    for indexed in [false, true] {
        let catalog = if indexed {
            OptimizerCatalog::optimistic()
        } else {
            OptimizerCatalog::default()
        };
        for seed in 0..256 {
            let logical = generated_plan(seed, 3);
            let mut memo = GraphMemo::default();
            let root = insert_logical_group(&mut memo, &logical);
            assert_eq!(logical_group_count(&logical), memo.group_count());
            let mut memo_decisions = Vec::new();
            let mut memo_events = Vec::new();
            let memo_plan = best_physical(
                &memo,
                root,
                &catalog,
                &context,
                &mut memo_decisions,
                &mut memo_events,
            );
            let mut direct_decisions = Vec::new();
            let mut direct_events = Vec::new();
            let direct_plan = lower_logical(
                &logical,
                LoweringChildren::Direct,
                &catalog,
                &context,
                &mut direct_decisions,
                &mut direct_events,
            );
            assert_eq!(memo_plan, direct_plan, "seed={seed} indexed={indexed}");
            assert_eq!(memo_decisions, direct_decisions, "seed={seed}");
            assert_eq!(memo_events, direct_events, "seed={seed}");
            assert_eq!(
                selected_plan_trace(&memo_plan, &catalog, &context),
                selected_plan_trace(&direct_plan, &catalog, &context),
                "seed={seed}"
            );
        }
    }
}

fn generated_plan(seed: u64, depth: usize) -> LogicalPlan {
    if depth == 0 {
        return scan("m");
    }
    let next = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    let input = Box::new(generated_plan(next >> 16, depth - 1));
    match seed % 13 {
        0 => LogicalPlan::Limit {
            offset: (seed % 3) as usize,
            limit: Some(0),
            input,
        },
        1 => LogicalPlan::Limit {
            offset: (seed % 3) as usize,
            limit: None,
            input,
        },
        2 => LogicalPlan::Limit {
            offset: (seed % 3) as usize,
            limit: Some((seed % 5 + 1) as usize),
            input: Box::new(sort(*input)),
        },
        3 => sort(*input),
        4 => LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "id".to_string(),
                value: Value::Int((seed % 7) as i64),
            },
            input,
        },
        5 => LogicalPlan::Project {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "id".to_string(),
                },
                name: "id".to_string(),
            }],
            input,
        },
        6 => LogicalPlan::Aggregate {
            group_keys: Vec::new(),
            items: vec![Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::All,
                distinct: false,
                name: "n".to_string(),
            }],
            input,
        },
        7 => LogicalPlan::Distinct { input },
        8 => LogicalPlan::NodeCartesianProduct {
            left: input,
            right: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "e".to_string(),
                    property: "id".to_string(),
                    value: Value::Int((seed % 11) as i64),
                },
                input: Box::new(scan("e")),
            }),
        },
        9 => LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: Some("r".to_string()),
            rel_type: "LINKS".to_string(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "e".to_string(),
            target_label: "Memory".to_string(),
            min_hops: 1,
            max_hops: (seed % 3 + 1) as usize,
            optional: seed.is_multiple_of(2),
            input,
        },
        10 => LogicalPlan::OptionalDegree {
            source_variable: "m".to_string(),
            rel_type: "LINKS".to_string(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Outgoing,
            target_label: "Memory".to_string(),
            target_properties: BTreeMap::new(),
            alias: "degree".to_string(),
            input,
        },
        11 => LogicalPlan::NodeColumnLookup {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            property: "id".to_string(),
            column: "id".to_string(),
            optional: seed.is_multiple_of(2),
            input,
        },
        _ => LogicalPlan::Filter {
            predicate: Predicate::BoundRelationshipExists {
                source_variable: "m".to_string(),
                rel_type: "LINKS".to_string(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "e".to_string(),
            },
            input,
        },
    }
}
