use super::super::{
    costing::estimate_physical_plan_cost_breakdown, CascadesOptimizer, OptimizerCatalog,
    OptimizerCatalogIndexes, OptimizerCatalogStatistics,
};
use crate::OptimizerConfig;
use skein_plan::{
    LogicalPlan, NodeProjectionAccess, PhysicalPlan, Predicate, Projection, ProjectionExpression,
};
use std::collections::BTreeMap;

fn catalog(label_count: u64) -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], [("Memory".into(), "title".into())]),
        OptimizerCatalogStatistics {
            label_counts: BTreeMap::from([("Memory".into(), label_count)]),
            ..OptimizerCatalogStatistics::default()
        },
    )
}

fn projection() -> Projection {
    Projection {
        expression: ProjectionExpression::Property {
            variable: "m".into(),
            property: "title".into(),
        },
        name: "title".into(),
    }
}

fn contains(query: &str) -> Predicate {
    Predicate::PropertyContains {
        variable: "m".into(),
        property: "title".into(),
        value: query.into(),
    }
}

fn expected_candidate_rows(label_count: u64) -> u64 {
    u128::from(label_count).div_ceil(4).max(1) as u64
}

fn assert_full_text_access_equivalence(label_count: u64, query: &str) {
    let catalog = catalog(label_count);
    // Do not call the production estimator: check the fixed fallback in a wider type.
    let candidate_rows = expected_candidate_rows(label_count);
    let expected_io = candidate_rows.saturating_add(3);
    let other_property = Predicate::PropertyContains {
        variable: "m".into(),
        property: "body".into(),
        value: query.into(),
    };
    for predicate in [
        None,
        Some(contains(query)),
        Some(other_property.clone()),
        Some(Predicate::And(vec![contains(query), other_property])),
    ] {
        for projected in [false, true] {
            let items = if projected {
                vec![projection()]
            } else {
                Vec::new()
            };
            let fused = PhysicalPlan::NodeProjectionScanExec {
                variable: "m".into(),
                label: "Memory".into(),
                access: NodeProjectionAccess::FullText {
                    property: "title".into(),
                    query: query.into(),
                },
                required_properties: vec!["body".into(), "title".into()],
                predicate: predicate.clone(),
                items: items.clone(),
            };
            let mut separate = PhysicalPlan::IndexNodeTextSeek {
                variable: "m".into(),
                label: "Memory".into(),
                property: "title".into(),
                query: query.into(),
            };
            if let Some(predicate) = predicate.clone() {
                separate = PhysicalPlan::FilterExec {
                    predicate,
                    input: Box::new(separate),
                };
            }
            if projected {
                separate = PhysicalPlan::ProjectExec {
                    items,
                    input: Box::new(separate),
                };
            }
            let fused_cost = estimate_physical_plan_cost_breakdown(&fused, &catalog);
            let separate_cost = estimate_physical_plan_cost_breakdown(&separate, &catalog);
            assert_eq!(separate_cost.random_io, expected_io);
            assert_eq!(
                fused_cost.random_io, expected_io,
                "label_count={label_count} predicate={predicate:?} projected={projected}"
            );
            assert_eq!(fused_cost.estimated_rows, separate_cost.estimated_rows);
            assert_eq!(fused_cost.sequential_io, 0);
            if predicate.is_none() || predicate == Some(contains(query)) {
                assert_eq!(fused_cost.estimated_rows, candidate_rows);
            }
        }
    }
}

#[test]
fn full_text_costing_preserves_candidates_across_projection_boundaries() {
    for count in [
        0,
        1,
        3,
        4,
        5,
        8,
        9,
        10,
        11,
        99,
        100,
        101,
        u64::MAX - 3,
        u64::MAX,
    ] {
        assert_full_text_access_equivalence(count, "graph");
    }
}

#[test]
fn full_text_costing_matches_rule_decisions_in_memo_and_fallback() {
    let logical = LogicalPlan::Project {
        items: vec![projection()],
        input: Box::new(LogicalPlan::Filter {
            predicate: contains("graph"),
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".into(),
                label: "Memory".into(),
            }),
        }),
    };
    for count in [0, 1, 8, 9, 99, 100, 101, u64::MAX] {
        let catalog = catalog(count);
        let candidate_rows = expected_candidate_rows(count);
        let seek_io = candidate_rows.saturating_add(3);
        let mut reference = None;
        for max_groups in [0, 16] {
            let (physical, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups })
                .optimize_with_catalog(&logical, &catalog);
            let PhysicalPlan::NodeProjectionScanExec {
                access, predicate, ..
            } = &physical
            else {
                panic!("expected fused projection: {physical:?}");
            };
            assert_eq!(predicate.as_ref(), Some(&contains("graph")));
            let cost = trace.selected_plan_cost_breakdown;
            if count > 8 && u128::from(candidate_rows) * 3 + 6 <= u128::from(count) + 4 {
                assert!(matches!(access, NodeProjectionAccess::FullText { .. }));
                assert_eq!(cost.random_io, seek_io);
                assert_eq!(cost.estimated_rows, candidate_rows);
                assert!(
                    trace.decisions.iter().any(|decision| {
                        decision.contains("choose IndexNodeTextSeek for Memory.title:")
                            && decision.contains(&format!("estimated_rows={candidate_rows}"))
                            && decision.contains(&format!(
                                "seek_cost={}",
                                candidate_rows.saturating_add(seek_io.saturating_mul(2))
                            ))
                    }),
                    "count={count} max_groups={max_groups} decisions={:?}",
                    trace.decisions
                );
            } else {
                assert!(matches!(access, NodeProjectionAccess::LabelScan));
                assert_eq!(cost.random_io, 0);
                assert!(trace.decisions.iter().any(|decision| {
                    decision.starts_with("choose SeqNodeScan for Memory.title")
                        && decision.contains(&format!("estimated_rows={candidate_rows}"))
                }));
            }
            if let Some((reference_plan, reference_cost)) = &reference {
                assert_eq!(&physical, reference_plan);
                assert_eq!(&cost, reference_cost);
            } else {
                reference = Some((physical, cost));
            }
        }
    }
}

#[test]
#[ignore = "local-only deterministic full-text cost campaign"]
fn full_text_cost_differential_campaign() {
    let mut cases = 0;
    for seed in [216_u64, 7, 0x5eed] {
        let mut state = seed;
        for case in 0..1024 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let count = if case % 2 == 0 { state } else { state % 1024 };
            let query = match case % 4 {
                0 => "graph",
                1 => "",
                2 => "e\u{301}",
                _ => "\u{1f4da}",
            };
            assert_full_text_access_equivalence(count, query);
            cases += 1;
        }
    }
    println!(
        "Full-text cost differential: {cases} catalogs, {} plan pairs",
        cases * 8
    );
}
