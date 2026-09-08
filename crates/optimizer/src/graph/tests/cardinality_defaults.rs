use super::super::{
    cardinality::{self, estimate_full_text_rows, PlanBindings},
    OptimizerCatalog, OptimizerIndexStatistics,
};
use crate::{
    estimate_relational_join_cost, PlanCostBreakdown, RelationalJoinCardinality,
    RelationalJoinRightInput, RelationalJoinSelectivity,
};
use skein_core::Value;
use skein_cypher::RelationshipDirection;
use skein_plan::{ComparisonOp, PhysicalPlan, Predicate, Projection, ProjectionExpression};
use std::collections::BTreeMap;

fn estimate_filter_rows(
    predicate: &Predicate,
    input: &PhysicalPlan,
    rows: u64,
    catalog: &OptimizerCatalog,
) -> u64 {
    cardinality::estimate_filter_rows(
        predicate,
        input,
        rows,
        catalog,
        &PlanBindings::for_plan(input),
    )
}

fn estimate_aggregate_rows(
    keys: &[Projection],
    input: &PhysicalPlan,
    rows: u64,
    catalog: &OptimizerCatalog,
) -> u64 {
    cardinality::estimate_aggregate_rows(keys, &PlanBindings::for_plan(input), rows, catalog)
}

const COUNTS: &[u64] = &[
    0,
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    61,
    121,
    u64::MAX - 1,
    u64::MAX,
];

// Independent original-formula oracle: never import the production defaults.
fn quotient(rows: u64, divisor: u64) -> u64 {
    u128::from(rows).div_ceil(u128::from(divisor)).max(1) as u64
}

fn saturated_product(left: u64, right: u64) -> u64 {
    (u128::from(left) * u128::from(right)).min(u128::from(u64::MAX)) as u64
}

fn catalog(population: u64, distinct: Option<u64>) -> OptimizerCatalog {
    let mut catalog = OptimizerCatalog::default();
    catalog.label_counts.insert("Memory".into(), population);
    catalog.rel_type_counts.insert("RELATED".into(), population);
    if let Some(distinct) = distinct {
        catalog
            .property_distinct_counts
            .insert(("Memory".into(), "key".into()), distinct);
        catalog
            .rel_property_distinct_counts
            .insert(("RELATED".into(), "key".into()), distinct);
    }
    catalog
}

fn node() -> PhysicalPlan {
    PhysicalPlan::SeqNodeScan {
        variable: "n".into(),
        label: "Memory".into(),
    }
}

fn relationship() -> PhysicalPlan {
    PhysicalPlan::AdjacencyExpandExec {
        source_variable: "n".into(),
        source_label: "Memory".into(),
        rel_variable: Some("r".into()),
        rel_type: "RELATED".into(),
        rel_properties: BTreeMap::new(),
        direction: RelationshipDirection::Outgoing,
        target_variable: "m".into(),
        target_label: "Memory".into(),
        min_hops: 1,
        max_hops: 1,
        optional: false,
        graph_budget: None,
        input: Box::new(node()),
    }
}

fn group_key(variable: &str, property: &str) -> Projection {
    Projection {
        expression: ProjectionExpression::Property {
            variable: variable.into(),
            property: property.into(),
        },
        name: property.into(),
    }
}

fn assert_predicate_defaults(rows: u64, population: u64, distinct: Option<u64>) {
    let catalog = catalog(population, distinct);
    let ndv = distinct.unwrap_or(population.max(1)).max(1);
    for (input, variable) in [(node(), "n"), (relationship(), "r")] {
        for (predicate, cap) in [
            (
                Predicate::PropertyContains {
                    variable: variable.into(),
                    property: "key".into(),
                    value: "x".into(),
                },
                4,
            ),
            (
                Predicate::PropertyStartsWith {
                    variable: variable.into(),
                    property: "key".into(),
                    value: "x".into(),
                },
                8,
            ),
            (
                Predicate::PropertyEndsWith {
                    variable: variable.into(),
                    property: "key".into(),
                    value: "x".into(),
                },
                6,
            ),
            (
                Predicate::PropertyIsNull {
                    variable: variable.into(),
                    property: "key".into(),
                },
                10,
            ),
        ] {
            assert_eq!(
                estimate_filter_rows(&predicate, &input, rows, &catalog),
                quotient(rows, ndv.min(cap)),
                "rows={rows} population={population} distinct={distinct:?} predicate={predicate:?}"
            );
        }
        let not_null = Predicate::PropertyIsNotNull {
            variable: variable.into(),
            property: "key".into(),
        };
        assert_eq!(
            estimate_filter_rows(&not_null, &input, rows, &catalog),
            rows.saturating_sub(quotient(rows, ndv.min(10))).max(1)
        );
        let unknown = Predicate::PropertyEq {
            variable: "unbound".into(),
            property: "key".into(),
            value: Value::Int(1),
        };
        assert_eq!(
            estimate_filter_rows(&unknown, &input, rows, &catalog),
            quotient(rows, 2)
        );
        assert_eq!(
            estimate_filter_rows(&Predicate::ConstantBool(true), &input, rows, &catalog),
            rows.max(1)
        );
        assert_eq!(
            estimate_filter_rows(&Predicate::ConstantBool(false), &input, rows, &catalog),
            1
        );

        let known = group_key(variable, "key");
        assert_eq!(
            estimate_aggregate_rows(std::slice::from_ref(&known), &input, rows, &catalog),
            distinct.map_or_else(|| quotient(rows, 4), |ndv| rows.min(ndv.max(1)).max(1))
        );
        let literal = Projection {
            expression: ProjectionExpression::Literal(Value::Int(1)),
            name: "literal".into(),
        };
        for keys in [
            vec![group_key(variable, "missing")],
            vec![literal.clone()],
            vec![known.clone(), literal],
            vec![known, group_key("unbound", "key")],
        ] {
            assert_eq!(
                estimate_aggregate_rows(&keys, &input, rows, &catalog),
                quotient(rows, 4)
            );
        }
        assert_eq!(estimate_aggregate_rows(&[], &input, rows, &catalog), 1);
    }
    assert_eq!(estimate_full_text_rows(rows), quotient(rows, 4));
}

#[test]
fn predicate_defaults_preserve_boundaries_and_node_relationship_parity() {
    for &rows in COUNTS {
        for distinct in [
            None,
            Some(0),
            Some(1),
            Some(3),
            Some(7),
            Some(10),
            Some(11),
            Some(u64::MAX),
        ] {
            assert_predicate_defaults(rows, 121, distinct);
            assert_predicate_defaults(rows, rows, distinct);
        }
    }
}

#[test]
fn trusted_index_statistics_override_fallbacks_without_changing_relationship_statistics() {
    let mut catalog = catalog(121, Some(3));
    catalog.property_index_statistics.insert(
        ("Memory".into(), "key".into()),
        OptimizerIndexStatistics {
            index_size: 91,
            distinct_count: 11,
        },
    );
    let contains = |variable: &str| Predicate::PropertyContains {
        variable: variable.into(),
        property: "key".into(),
        value: "x".into(),
    };
    assert_eq!(
        estimate_filter_rows(&contains("n"), &node(), 121, &catalog),
        31
    );
    assert_eq!(
        estimate_filter_rows(&contains("r"), &relationship(), 121, &catalog),
        41
    );
    assert_eq!(
        estimate_aggregate_rows(&[group_key("n", "key")], &node(), 121, &catalog),
        11
    );
    assert_eq!(
        estimate_aggregate_rows(&[group_key("r", "key")], &relationship(), 121, &catalog),
        3
    );
    assert_eq!(catalog.estimate_property_index_eq_rows("Memory", "key"), 9);

    let access = PhysicalPlan::IndexNodeTextSeek {
        variable: "n".into(),
        label: "Memory".into(),
        property: "key".into(),
        query: "x".into(),
    };
    assert_eq!(
        estimate_filter_rows(&contains("n"), &access, 31, &catalog),
        31
    );
    let unrelated = Predicate::PropertyContains {
        variable: "n".into(),
        property: "other".into(),
        value: "x".into(),
    };
    assert_eq!(estimate_filter_rows(&unrelated, &access, 31, &catalog), 8);
}

#[test]
fn aggregate_known_ndv_products_keep_saturation_and_input_caps() {
    for &rows in COUNTS {
        let catalog = catalog(u64::MAX, Some(u64::MAX));
        for (input, variable) in [(node(), "n"), (relationship(), "r")] {
            assert_eq!(
                estimate_aggregate_rows(
                    &[group_key(variable, "key"), group_key(variable, "key")],
                    &input,
                    rows,
                    &catalog
                ),
                rows.max(1)
            );
        }
    }
}

fn assert_range_defaults(rows: u64, population: u64, histogram: Option<&[i64]>, sampled: bool) {
    let mut catalog = catalog(population, Some(17));
    if let Some(histogram) = histogram {
        let values = histogram
            .iter()
            .copied()
            .map(Value::Int)
            .collect::<Vec<_>>();
        catalog
            .property_histograms
            .insert(("Memory".into(), "key".into()), values.clone());
        catalog
            .rel_property_histograms
            .insert(("RELATED".into(), "key".into()), values);
    }
    catalog
        .sampled_property_histograms
        .insert(("Memory".into(), "key".into()), sampled);
    catalog
        .sampled_rel_property_histograms
        .insert(("RELATED".into(), "key".into()), sampled);
    let expected = |input_rows, matches| match histogram.filter(|values| !values.is_empty()) {
        None => quotient(input_rows, 2),
        Some(values) => {
            let numerator = matches + u64::from(sampled);
            let denominator = values.len() as u64 + if sampled { 2 } else { 0 };
            quotient(saturated_product(input_rows, numerator), denominator)
        }
    };
    for op in [
        ComparisonOp::Lt,
        ComparisonOp::Lte,
        ComparisonOp::Gt,
        ComparisonOp::Gte,
    ] {
        let matches = histogram
            .unwrap_or_default()
            .iter()
            .filter(|&&value| match op {
                ComparisonOp::Lt => value < 3,
                ComparisonOp::Lte => value <= 3,
                ComparisonOp::Gt => value > 3,
                ComparisonOp::Gte => value >= 3,
            })
            .count() as u64;
        assert_eq!(
            catalog.estimate_property_range_rows("Memory", "key", op, &Value::Int(3), rows),
            expected(rows, matches)
        );
        assert_eq!(
            catalog.estimate_rel_property_range_rows("RELATED", "key", op, &Value::Int(3), rows),
            expected(rows, matches)
        );
        assert_eq!(
            catalog.estimate_range_rows("Memory", "key", op, &Value::Int(3)),
            expected(population.max(1), matches)
        );
        let bound = (
            Value::Int(3),
            matches!(op, ComparisonOp::Lte | ComparisonOp::Gte),
        );
        let (lower, upper) = match op {
            ComparisonOp::Lt | ComparisonOp::Lte => (None, Some(&bound)),
            ComparisonOp::Gt | ComparisonOp::Gte => (Some(&bound), None),
        };
        assert_eq!(
            catalog.estimate_range_bounds_rows("Memory", "key", lower, upper),
            expected(population.max(1), matches)
        );
        for (input, variable) in [(node(), "n"), (relationship(), "r")] {
            let predicate = Predicate::PropertyCompare {
                variable: variable.into(),
                property: "key".into(),
                op,
                value: Value::Int(3),
            };
            assert_eq!(
                estimate_filter_rows(&predicate, &input, rows, &catalog),
                expected(rows, matches)
            );
        }
    }
    let matches = histogram
        .unwrap_or_default()
        .iter()
        .filter(|&&value| (2..6).contains(&value))
        .count() as u64;
    assert_eq!(
        catalog.estimate_range_bounds_rows(
            "Memory",
            "key",
            Some(&(Value::Int(2), true)),
            Some(&(Value::Int(6), false))
        ),
        expected(population.max(1), matches)
    );
}

#[test]
fn missing_and_empty_histograms_keep_the_range_fallback() {
    for &rows in COUNTS {
        for sampled in [false, true] {
            assert_range_defaults(rows, rows, None, sampled);
            assert_range_defaults(rows, 121, Some(&[]), sampled);
        }
    }
}

#[test]
fn complete_histograms_remain_exact_and_sampled_histograms_keep_their_prior() {
    for &rows in COUNTS {
        for sampled in [false, true] {
            for histogram in [&[3][..], &[1, 2], &[4, 5], &[0, 2, 3, 3, 4, 6, 8]] {
                assert_range_defaults(rows, 121, Some(histogram), sampled);
            }
        }
    }
}

fn assert_join_defaults(
    left_rows: u64,
    right_rows: u64,
    left_ndv: Option<u64>,
    right_ndv: Option<u64>,
) {
    let left = PlanCostBreakdown::new(left_rows, 3, 5, 7, 11);
    let right = PlanCostBreakdown::new(right_rows, 13, 17, 19, 23);
    let pairs = saturated_product(left_rows.max(1), right_rows.max(1));
    let known_divisor = [
        left_ndv.map(|ndv| ndv.max(1).min(left_rows.max(1))),
        right_ndv.map(|ndv| ndv.max(1).min(right_rows.max(1))),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(10);
    for (selectivity, divisor) in [
        (RelationalJoinSelectivity::Unknown, 10),
        (RelationalJoinSelectivity::equi_join(None, None), 10),
        (
            RelationalJoinSelectivity::equi_join(left_ndv, right_ndv),
            known_divisor,
        ),
    ] {
        for right_input in [
            RelationalJoinRightInput::Materialized,
            RelationalJoinRightInput::Hash,
            RelationalJoinRightInput::Merge,
            RelationalJoinRightInput::Probe,
        ] {
            let rows = if right_input == RelationalJoinRightInput::Probe {
                pairs
            } else {
                quotient(pairs, divisor)
            };
            for (cardinality, expected) in [
                (RelationalJoinCardinality::Inner, rows),
                (
                    RelationalJoinCardinality::PreserveLeft,
                    rows.max(left_rows.max(1)),
                ),
            ] {
                assert_eq!(
                    estimate_relational_join_cost(
                        left,
                        right,
                        cardinality,
                        right_input,
                        selectivity
                    )
                    .estimated_rows,
                    expected
                );
            }
        }
    }
}

#[test]
fn joins_keep_fallbacks_known_ndv_caps_and_probe_fanout_distinct() {
    for &left in COUNTS {
        for &right in COUNTS {
            for (left_ndv, right_ndv) in [
                (None, None),
                (Some(0), None),
                (None, Some(3)),
                (Some(7), Some(u64::MAX)),
            ] {
                assert_join_defaults(left, right, left_ndv, right_ndv);
            }
        }
    }
}

#[test]
#[ignore = "local-only deterministic cardinality fallback campaign"]
fn cardinality_defaults_differential_campaign() {
    let mut cases = 0;
    for seed in [216_u64, 7, 0x5eed] {
        let mut state = seed;
        for case in 0..1024 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let rows = if case % 2 == 0 { state } else { state % 257 };
            let population = state.rotate_left(19) % 1024;
            let distinct = match case % 5 {
                0 => None,
                1 => Some(0),
                2 => Some(u64::MAX),
                _ => Some(state % 17),
            };
            assert_predicate_defaults(rows, population, distinct);
            let histogram = (0..case % 17)
                .map(|index| (state.rotate_left(index) % 9) as i64)
                .collect::<Vec<_>>();
            assert_range_defaults(rows, population, Some(&histogram), case % 2 == 0);
            assert_join_defaults(rows, state.rotate_right(7), distinct, Some(population));
            cases += 1;
        }
    }
    println!("Cardinality defaults differential: {cases} cases, node/relationship predicates, ranges, aggregates and joins");
}
