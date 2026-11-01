// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::graph::{OptimizerCatalogIndexes, OptimizerCatalogStatistics, OptimizerIndexStatistics};
use hawdb_plan_cypher::PhysicalPlan;
use std::collections::BTreeSet;

fn catalog(population: u64, indexed: u64, joint_ndv: u64) -> OptimizerCatalog {
    let key = ("Item".to_owned(), vec!["a".to_owned(), "b".to_owned()]);
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [key.clone()], [], []),
        OptimizerCatalogStatistics::new(
            [("Item".into(), population)],
            [],
            [],
            [],
            [],
            [
                (("Item".into(), "a".into()), 100),
                (("Item".into(), "b".into()), 100),
            ],
            [],
        )
        .with_composite_index_statistics([(
            key,
            OptimizerIndexStatistics {
                index_size: indexed,
                distinct_count: joint_ndv,
            },
        )]),
    )
}

fn scan() -> PhysicalPlan {
    PhysicalPlan::SeqNodeScan {
        variable: "n".into(),
        label: "Item".into(),
    }
}

fn conditions() -> Vec<Predicate> {
    ["b", "a"]
        .map(|property| Predicate::PropertyEq {
            variable: "n".into(),
            property: property.into(),
            value: Value::Int(1),
        })
        .to_vec()
}

fn groups() -> Vec<Projection> {
    ["b", "a"]
        .map(|property| Projection {
            name: property.into(),
            expression: ProjectionExpression::Property {
                variable: "n".into(),
                property: property.into(),
            },
        })
        .to_vec()
}

#[test]
fn correlated_and_independent_estimates_match_complete_reference_populations() {
    let scan = scan();
    let bindings = PlanBindings::for_plan(&scan);
    for correlated in [false, true] {
        let population = (0..10_000)
            .map(|i| (i % 100, if correlated { i % 100 } else { i / 100 }))
            .collect::<Vec<_>>();
        let distinct = population.iter().copied().collect::<BTreeSet<_>>().len() as u64;
        let matches = population
            .iter()
            .filter(|(a, b)| *a == 1 && *b == 1)
            .count() as u64;
        let catalog = catalog(10_000, 10_000, distinct);
        assert_eq!(
            filter_rows(&conditions(), &bindings, 10_000, &catalog),
            Some(matches)
        );
        assert_eq!(
            group_rows(&groups(), &bindings, 10_000, &catalog),
            Some(distinct)
        );
        let mut duplicate = conditions();
        duplicate.push(duplicate[0].clone());
        assert_eq!(
            filter_rows(&duplicate, &bindings, 10_000, &catalog),
            Some(matches)
        );
    }
}

#[test]
fn conditional_ndv_reuses_only_the_same_fixed_access_equality() {
    let catalog = catalog(10_000, 10_000, 100);
    let seek = PhysicalPlan::IndexNodeSeek {
        variable: "n".into(),
        label: "Item".into(),
        property: "a".into(),
        value: Value::Int(1),
    };
    let bindings = PlanBindings::for_plan(&seek);
    assert_eq!(
        filter_rows(&conditions(), &bindings, 100, &catalog),
        Some(100)
    );
    let mut different = conditions();
    different[1] = Predicate::PropertyEq {
        variable: "n".into(),
        property: "a".into(),
        value: Value::Int(2),
    };
    assert_eq!(filter_rows(&different, &bindings, 100, &catalog), Some(1));
    let multi = PhysicalPlan::IndexNodeMultiSeek {
        variable: "n".into(),
        label: "Item".into(),
        property: "a".into(),
        values: vec![Value::Int(1), Value::Int(2)],
    };
    assert_eq!(
        filter_rows(
            &conditions(),
            &PlanBindings::for_plan(&multi),
            200,
            &catalog
        ),
        None
    );
}

#[test]
fn sparse_samples_retain_null_bearing_group_bounds_and_reject_unsafe_conditioning() {
    let mut catalog = catalog(10_000, 9_000, 100);
    // The 1,000 rows missing a can have b values outside the complete-key
    // population. That larger marginal domain must not erase the joint model.
    catalog
        .property_distinct_counts
        .insert(("Item".into(), "b".into()), 1100);
    let scan = scan();
    let bindings = PlanBindings::for_plan(&scan);
    assert_eq!(
        filter_rows(&conditions(), &bindings, 10_000, &catalog),
        Some(90)
    );
    assert_eq!(
        group_rows(&groups(), &bindings, 10_000, &catalog),
        Some(1100)
    );
    assert_eq!(group_rows(&groups(), &bindings, 7, &catalog), Some(7));
    let seek = PhysicalPlan::IndexNodeSeek {
        variable: "n".into(),
        label: "Item".into(),
        property: "a".into(),
        value: Value::Int(1),
    };
    assert_eq!(
        filter_rows(&conditions(), &PlanBindings::for_plan(&seek), 100, &catalog),
        None
    );
    let mut null = conditions();
    null[0] = Predicate::PropertyEq {
        variable: "n".into(),
        property: "b".into(),
        value: Value::Null,
    };
    assert_eq!(filter_rows(&null, &bindings, 10_000, &catalog), None);
}

#[test]
fn conditional_estimates_preserve_duplicate_bindings_from_a_product() {
    let mut catalog = catalog(10_000, 10_000, 100);
    catalog.label_counts.insert("Other".into(), 100);
    let product = PhysicalPlan::NodeCartesianProductExec {
        left: Box::new(PhysicalPlan::IndexNodeSeek {
            variable: "n".into(),
            label: "Item".into(),
            property: "a".into(),
            value: Value::Int(1),
        }),
        right: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "other".into(),
            label: "Other".into(),
        }),
    };
    let input = crate::graph::costing::estimate_physical_plan_cost_breakdown(&product, &catalog)
        .estimated_rows;
    assert_eq!(input, 10_000);
    assert_eq!(
        filter_rows(
            &conditions(),
            &PlanBindings::for_plan(&product),
            input,
            &catalog
        ),
        Some(10_000)
    );
}

fn join_catalog(ndv: u64, complete: bool) -> OptimizerCatalog {
    let indexes = ["Left", "Right"].map(|label| (label.into(), vec!["x".into(), "y".into()]));
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], indexes.clone(), [], []),
        OptimizerCatalogStatistics::new(
            [("Left".into(), 1000), ("Right".into(), 1000)],
            [],
            [],
            [],
            [],
            ["Left", "Right"].into_iter().flat_map(|label| {
                ["x", "y"].map(move |property| ((label.into(), property.into()), 10))
            }),
            [],
        )
        .with_composite_index_statistics(indexes.map(|index| {
            (
                index,
                OptimizerIndexStatistics {
                    index_size: if complete { 1000 } else { 900 },
                    distinct_count: ndv,
                },
            )
        })),
    )
}

fn join_plan() -> PhysicalPlan {
    PhysicalPlan::HashJoinExec {
        left_key: HashJoinKey {
            variable: "l".into(),
            property: "x".into(),
        },
        right_key: HashJoinKey {
            variable: "r".into(),
            property: "x".into(),
        },
        left: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "l".into(),
            label: "Left".into(),
        }),
        right: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "r".into(),
            label: "Right".into(),
        }),
    }
}

fn join_predicate() -> Predicate {
    Predicate::And(
        ["x", "y"]
            .map(|property| Predicate::ExpressionEq {
                expression: ProjectionExpression::Property {
                    variable: "l".into(),
                    property: property.into(),
                },
                value: ProjectionExpression::Property {
                    variable: "r".into(),
                    property: property.into(),
                },
            })
            .to_vec(),
    )
}

#[test]
fn joint_join_estimates_match_frequency_reference_without_underpricing_hash_work() {
    for correlated in [false, true] {
        let values = (0..1000).map(|i| (i % 10, if correlated { i % 10 } else { (i / 10) % 10 }));
        let mut first = BTreeMap::<_, u64>::new();
        let mut tuples = BTreeMap::<_, u64>::new();
        for pair in values {
            *first.entry(pair.0).or_default() += 1;
            *tuples.entry(pair).or_default() += 1;
        }
        let hash_matches = first.values().map(|count| count * count).sum::<u64>();
        let joint_matches = tuples.values().map(|count| count * count).sum::<u64>();
        let catalog = join_catalog(tuples.len() as u64, true);
        let plan = join_plan();
        let bindings = PlanBindings::for_plan(&plan);
        let cost = crate::graph::costing::estimate_physical_plan_cost_breakdown(&plan, &catalog);
        assert_eq!(cost.estimated_rows, hash_matches);
        assert_eq!(
            join_filter_rows(&join_predicate(), &plan, hash_matches, &catalog, &bindings),
            Some(joint_matches)
        );
        let filtered = PhysicalPlan::FilterExec {
            predicate: join_predicate(),
            input: Box::new(plan),
        };
        let cost =
            crate::graph::costing::estimate_physical_plan_cost_breakdown(&filtered, &catalog);
        assert_eq!(cost.estimated_rows, joint_matches);
        assert!(
            cost.cpu >= hash_matches * 2,
            "hash and residual must visit the one-key intermediate"
        );
    }
}

#[test]
fn unknown_hash_key_ndv_preserves_the_default_residual_selectivity() {
    for known_label in [None, Some("Left"), Some("Right")] {
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::default(),
            OptimizerCatalogStatistics::new(
                [("Left".into(), 1000), ("Right".into(), 1000)],
                [],
                [],
                [],
                [],
                known_label
                    .into_iter()
                    .map(|label| ((label.into(), "x".into()), 10)),
                [],
            ),
        );
        let plan = join_plan();
        let bindings = PlanBindings::for_plan(&plan);
        let hash_cost =
            crate::graph::costing::estimate_physical_plan_cost_breakdown(&plan, &catalog);
        assert_eq!(hash_cost.estimated_rows, 1_000_000);
        let Predicate::And(parts) = join_predicate() else {
            unreachable!()
        };
        for predicate in [parts[0].clone(), join_predicate()] {
            assert_eq!(
                join_filter_rows(
                    &predicate,
                    &plan,
                    hash_cost.estimated_rows,
                    &catalog,
                    &bindings
                ),
                None,
                "an unestimated hash key must not suppress the default filter fallback"
            );
            let filtered = PhysicalPlan::FilterExec {
                predicate,
                input: Box::new(plan.clone()),
            };
            let cost =
                crate::graph::costing::estimate_physical_plan_cost_breakdown(&filtered, &catalog);
            assert_eq!(cost.estimated_rows, 500_000);
        }
    }
}

#[test]
fn sparse_joint_join_domains_fall_back_and_the_enforced_key_is_not_counted_twice() {
    let catalog = join_catalog(100, false);
    let plan = join_plan();
    let bindings = PlanBindings::for_plan(&plan);
    assert_eq!(
        join_filter_rows(&join_predicate(), &plan, 100_000, &catalog, &bindings),
        Some(100_000),
        "an incomplete joint domain must not invent an independent residual discount"
    );
    let Predicate::And(parts) = join_predicate() else {
        unreachable!()
    };
    assert_eq!(
        join_filter_rows(&parts[0], &plan, 100_000, &catalog, &bindings),
        Some(100_000)
    );
}

#[test]
fn grouping_resolves_aliases_in_the_input_scope_and_publishes_only_output_columns() {
    let indexes = [vec!["a".into(), "b".into()], vec!["b".into(), "c".into()]];
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [],
            indexes.clone().map(|keys| ("Item".into(), keys)),
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Item".into(), 10_000)],
            [],
            [],
            [],
            [],
            [
                (("Item".into(), "a".into()), 100),
                (("Item".into(), "b".into()), 100),
                (("Item".into(), "c".into()), 20),
            ],
            [],
        )
        .with_composite_index_statistics(indexes.into_iter().zip([100, 2000]).map(
            |(keys, ndv)| {
                (
                    ("Item".into(), keys),
                    OptimizerIndexStatistics {
                        index_size: 10_000,
                        distinct_count: ndv,
                    },
                )
            },
        )),
    );
    let projected = PhysicalPlan::ProjectExec {
        items: [("u", "a"), ("v", "b"), ("w", "c")]
            .map(|(name, property)| Projection {
                name: name.into(),
                expression: ProjectionExpression::Property {
                    variable: "n".into(),
                    property: property.into(),
                },
            })
            .to_vec(),
        input: Box::new(scan()),
    };
    let aggregate = PhysicalPlan::AggregateExec {
        group_keys: [("u", "v"), ("v", "w")]
            .map(|(output, input)| Projection {
                name: output.into(),
                expression: ProjectionExpression::Column(input.into()),
            })
            .to_vec(),
        items: vec![],
        input: Box::new(projected),
    };
    let cost = crate::graph::costing::estimate_physical_plan_cost_breakdown(&aggregate, &catalog);
    assert_eq!(
        cost.estimated_rows, 2000,
        "group on incoming b,c before output aliases shadow u,v"
    );
    let bindings = PlanBindings::for_plan(&aggregate);
    assert_eq!(
        bindings.property_source(&ProjectionExpression::Column("u".into())),
        Some(("n", "b"))
    );
    assert_eq!(
        bindings.property_source(&ProjectionExpression::Column("v".into())),
        Some(("n", "c"))
    );
    assert_eq!(
        bindings.property_source(&ProjectionExpression::Column("w".into())),
        None
    );
}

#[test]
fn computed_or_ambiguous_aliases_and_optional_rebindings_do_not_gain_joint_proofs() {
    let projected = PhysicalPlan::ProjectExec {
        items: vec![
            Projection {
                name: "x".into(),
                expression: ProjectionExpression::Property {
                    variable: "n".into(),
                    property: "a".into(),
                },
            },
            Projection {
                name: "x".into(),
                expression: ProjectionExpression::Literal(Value::Int(1)),
            },
        ],
        input: Box::new(scan()),
    };
    assert_eq!(
        PlanBindings::for_plan(&projected)
            .property_source(&ProjectionExpression::Column("x".into())),
        None
    );
    let optional = PhysicalPlan::NodeColumnLookupExec {
        variable: "n".into(),
        label: "Item".into(),
        property: "a".into(),
        column: "target".into(),
        optional: true,
        node_visibility_predicate: None,
        input: Box::new(PhysicalPlan::ProjectExec {
            items: vec![Projection {
                name: "target".into(),
                expression: ProjectionExpression::Property {
                    variable: "n".into(),
                    property: "a".into(),
                },
            }],
            input: Box::new(scan()),
        }),
    };
    assert_eq!(
        group_rows(
            &groups(),
            &PlanBindings::for_plan(&optional),
            10_000,
            &catalog(10_000, 10_000, 100)
        ),
        None
    );
}

#[test]
fn statistics_provenance_uses_the_cost_fold_and_refresh_replaces_old_sources() {
    use crate::{CascadesOptimizer, OptimizerConfig};
    use hawdb_plan_cypher::LogicalPlan;
    let catalog = catalog(10_000, 10_000, 100);
    let optimizer = CascadesOptimizer::new(OptimizerConfig { max_groups: 1000 });
    let (_, mut trace) = optimizer.optimize_with_catalog(
        &LogicalPlan::NodeScan {
            variable: "n".into(),
            label: "Item".into(),
        },
        &catalog,
    );
    let plan = PhysicalPlan::FilterExec {
        predicate: Predicate::And(conditions()),
        input: Box::new(scan()),
    };
    crate::graph::costing::take_cost_evaluations();
    crate::graph::cardinality::take_metadata_visits();
    optimizer.refresh_trace_for_physical_plan(&mut trace, &plan, &catalog);
    assert_eq!(crate::graph::costing::take_cost_evaluations(), 2);
    assert_eq!(crate::graph::cardinality::take_metadata_visits(), 2);
    assert_eq!(trace.selected_plan_cost.estimated_rows, 100);
    let entries = trace
        .decisions
        .iter()
        .filter(|line| line.starts_with("cardinality statistics: "))
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 1);
    assert!(entries[0].contains("operator_id=0"));
    assert!(entries[0].ends_with("joint_ndv=100"));
    let changed = super::tests::catalog(10_000, 10_000, 10_000);
    optimizer.refresh_trace_for_physical_plan(&mut trace, &plan, &changed);
    let entries = trace
        .decisions
        .iter()
        .filter(|line| line.starts_with("cardinality statistics: "))
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 1);
    assert!(entries[0].ends_with("joint_ndv=10000"));
    assert_eq!(trace.selected_plan_cost.estimated_rows, 1);
}

#[test]
fn inconsistent_or_unavailable_joint_statistics_fall_back() {
    let scan = scan();
    let bindings = PlanBindings::for_plan(&scan);
    for (indexed, ndv) in [(0, 0), (10_001, 100), (9_000, 9_001), (10_000, 99)] {
        let catalog = catalog(10_000, indexed, ndv);
        assert_eq!(
            filter_rows(&conditions(), &bindings, 10_000, &catalog),
            None,
            "indexed={indexed} ndv={ndv}"
        );
    }
    assert_eq!(
        filter_rows(
            &conditions(),
            &bindings,
            10_000,
            &OptimizerCatalog::default()
        ),
        None
    );
}
