// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::graph::{OptimizerCatalogIndexes, OptimizerCatalogStatistics};
use hawdb_core::CompressedHistogram;

fn node(variable: &str, label: &str) -> GraphMatchNode {
    GraphMatchNode {
        variable: variable.into(),
        label: label.into(),
        properties: BTreeMap::new(),
    }
}

fn program() -> GraphMatchProgram {
    GraphMatchProgram {
        imports: Vec::new(),
        introduced: vec!["a".into(), "b".into()],
        projected_nodes: BTreeMap::new(),
        projected_relationships: BTreeMap::new(),
        steps: vec![
            GraphMatchStep::Node(node("a", "A")),
            GraphMatchStep::Expand {
                source: "a".into(),
                relationship: None,
                rel_type: "R".into(),
                rel_types: Vec::new(),
                properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                min_hops: 1,
                max_hops: 1,
                target: node("b", "B"),
            },
        ],
        predicate: None,
        predicate_steps: Vec::new(),
        edge_predicates: Vec::new(),
        optional: false,
        hash_join: None,
        id_set_conjuncts: Vec::new(),
        seeded_union: Vec::new(),
    }
}

fn catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("A".into(), "key".into())], [], [], []),
        OptimizerCatalogStatistics {
            label_counts: BTreeMap::from([("A".into(), 100), ("B".into(), 1_000)]),
            path_counts: BTreeMap::from([(("A".into(), "R".into(), "B".into()), 500)]),
            property_distinct_counts: BTreeMap::from([(("A".into(), "key".into()), 100)]),
            ..OptimizerCatalogStatistics::default()
        },
    )
}

fn cost(program: &GraphMatchProgram, catalog: &OptimizerCatalog) -> PlanCostBreakdown {
    estimate(
        program,
        catalog,
        &PlanBindings::default(),
        PlanCostBreakdown::new(1, 0, 0, 0, 0),
    )
}

#[test]
fn scan_seek_and_expansion_share_frequency_and_direction_statistics() {
    let mut program = program();
    let mut catalog = catalog();
    assert_eq!(cost(&program, &catalog).estimated_rows, 500);
    let GraphMatchStep::Node(seed) = &mut program.steps[0] else {
        unreachable!()
    };
    seed.properties.insert("key".into(), Value::Int(1));
    let selective = cost(&program, &catalog);
    assert_eq!((selective.estimated_rows, selective.random_io), (5, 9));

    let histogram = CompressedHistogram::from_sample(
        100,
        100,
        0,
        0,
        std::iter::repeat_n(Value::Int(1), 80)
            .chain((2..=21).map(Value::Int))
            .collect(),
    )
    .unwrap();
    std::sync::Arc::make_mut(&mut catalog.property_distributions)
        .insert(("A".into(), "key".into()), histogram);
    let frequent = cost(&program, &catalog);
    assert_eq!(frequent.estimated_rows, 400);
    assert!(frequent.random_io > selective.random_io);

    let GraphMatchStep::Expand { direction, .. } = &mut program.steps[1] else {
        unreachable!()
    };
    *direction = RelationshipDirection::Incoming;
    // No B -> A paths in the published statistics: a planning floor, not an
    // empty-result rewrite or a full Cartesian product.
    assert_eq!(cost(&program, &catalog).estimated_rows, 1);
}

#[test]
fn seeded_union_costs_actual_branches_and_charges_input_once() {
    let mut branch = program();
    let GraphMatchStep::Node(seed) = &mut branch.steps[0] else {
        unreachable!()
    };
    seed.properties.insert("key".into(), Value::Int(1));
    let mut union = program();
    union.seeded_union = vec![branch.clone(), branch];
    let input = PlanCostBreakdown::new(3, 7, 11, 13, 17);
    let cost = estimate(&union, &catalog(), &PlanBindings::default(), input);
    assert_eq!((cost.estimated_rows, cost.random_io), (30, 11 + 2 * 3 * 9));
    assert_eq!(
        (cost.cpu, cost.sequential_io, cost.output_rows),
        (7, 13, 17)
    );
}

#[test]
fn bound_endpoint_filters_candidates_and_optional_preserves_input_floor() {
    let mut program = program();
    // Returning to a is an identity check, not another fresh label scan.
    let GraphMatchStep::Expand { target, .. } = &mut program.steps[1] else {
        unreachable!()
    };
    *target = node("a", "A");
    let mut catalog = catalog();
    catalog
        .path_counts
        .insert(("A".into(), "R".into(), "A".into()), 50);
    let required = cost(&program, &catalog);
    assert_eq!(required.estimated_rows, 1);
    // The 500 A -> B candidates are visited before the bound A filter too.
    assert_eq!(required.random_io, 751);
    program.optional = true;
    let optional = estimate(
        &program,
        &catalog,
        &PlanBindings::default(),
        PlanCostBreakdown::new(10, 0, 0, 0, 0),
    );
    assert_eq!(optional.estimated_rows, 10);
}

#[test]
fn unbounded_and_disjunctive_paths_keep_a_bounded_fallback_after_the_seek() {
    let mut program = program();
    let GraphMatchStep::Node(seed) = &mut program.steps[0] else {
        unreachable!()
    };
    seed.properties.insert("key".into(), Value::Int(1));
    let GraphMatchStep::Expand {
        max_hops,
        rel_types,
        ..
    } = &mut program.steps[1]
    else {
        unreachable!()
    };
    *max_hops = usize::MAX;
    *rel_types = vec!["R".into(), "S".into()];
    assert_eq!(cost(&program, &catalog()).estimated_rows, 1_000);
    assert_eq!(cost(&program, &catalog()).random_io, 1_003);
}

#[test]
fn imported_unlabelled_nodes_retain_the_input_label_for_costing() {
    use hawdb_plan_cypher::{GraphBindingImport, GraphEntityKind};
    let mut program = program();
    program.imports.push(GraphBindingImport {
        variable: "a".into(),
        column: "a".into(),
        kind: GraphEntityKind::Node,
    });
    program.introduced.retain(|name| name != "a");
    let GraphMatchStep::Node(node) = &mut program.steps[0] else {
        unreachable!()
    };
    node.label.clear();
    let plan = PhysicalPlan::GraphMatchExec {
        program,
        input: Some(Box::new(PhysicalPlan::SeqNodeScan {
            variable: "a".into(),
            label: "A".into(),
        })),
    };
    let estimate = super::super::estimate_physical_plan_cost_breakdown(&plan, &catalog());
    assert_eq!(estimate.estimated_rows, 500);
}

#[test]
fn relationship_frequency_filters_matches_without_discounting_candidate_reads() {
    let mut program = program();
    let GraphMatchStep::Expand { properties, .. } = &mut program.steps[1] else {
        unreachable!()
    };
    properties.insert("flag".into(), Value::Int(1));
    let mut catalog = catalog();
    catalog.rel_type_counts.insert("R".into(), 500);
    let histogram = CompressedHistogram::from_sample(
        500,
        500,
        0,
        0,
        std::iter::repeat_n(Value::Int(1), 10)
            .chain(std::iter::repeat_n(Value::Int(0), 490))
            .collect(),
    )
    .unwrap();
    std::sync::Arc::make_mut(&mut catalog.rel_property_distributions)
        .insert(("R".into(), "flag".into()), histogram);
    let estimate = cost(&program, &catalog);
    assert_eq!(estimate.estimated_rows, 10);
    assert_eq!(estimate.random_io, 701);
}

#[test]
fn large_intermediates_saturate_without_turning_into_empty_estimates() {
    let mut program = program();
    program.steps.push(GraphMatchStep::Node(node("c", "A")));
    program.introduced.push("c".into());
    let mut catalog = catalog();
    catalog.label_counts.insert("A".into(), u64::MAX);
    catalog.label_counts.insert("B".into(), u64::MAX);
    catalog
        .path_counts
        .insert(("A".into(), "R".into(), "B".into()), u64::MAX);
    let estimate = cost(&program, &catalog);
    assert_eq!(estimate.estimated_rows, u64::MAX);
    assert_eq!(estimate.random_io, u64::MAX);
    assert_eq!(estimate.cost, u64::MAX);
}

#[test]
fn complete_absent_hop_charges_dense_and_sparse_dead_end_prefixes() {
    for (a_count, b_count, first_hops) in [(100, 1_000, 5_000), (1_000, 1_000, 10)] {
        for direction in [
            RelationshipDirection::Outgoing,
            RelationshipDirection::Incoming,
        ] {
            let mut program = program();
            let mut catalog = catalog();
            catalog.label_counts.insert("A".into(), a_count);
            catalog.label_counts.insert("B".into(), b_count);
            catalog.rel_type_counts.insert("R".into(), first_hops);
            catalog
                .rel_type_source_counts
                .insert("R".into(), a_count.min(first_hops));
            catalog
                .path_counts
                .insert(("A".into(), "R".into(), "B".into()), first_hops);
            catalog
                .bounded_path_counts
                .insert(("A".into(), "R".into(), "B".into(), 1), first_hops);
            catalog.bounded_path_complete_through_hop = Some(3);
            let incoming = direction == RelationshipDirection::Incoming;
            let (source_label, target_label, population) = if incoming {
                ("B", "A", b_count)
            } else {
                ("A", "B", a_count)
            };
            program.steps[0] = GraphMatchStep::Node(node("a", source_label));
            let GraphMatchStep::Expand {
                target,
                direction: current_direction,
                min_hops,
                max_hops,
                ..
            } = &mut program.steps[1]
            else {
                unreachable!()
            };
            *target = node("b", target_label);
            *current_direction = direction;
            *min_hops = 2;
            *max_hops = 2;
            // All R edges run A -> B, so both traversal orientations have
            // first-hop prefixes but no two-hop completion.
            assert_eq!(catalog.bounded_path_count("A", "R", "B", 2), Some(0));
            let complete = cost(&program, &catalog);
            assert_eq!(complete.estimated_rows, 1);
            assert!(
                complete.random_io > 2 * population + first_hops,
                "{complete:?}"
            );
            let expand = catalog.estimate_expand_rows("A", "R", &BTreeMap::new(), "B", 2, 2);
            assert!(expand.estimated_rows > 0);
            assert!(expand.hop_estimates[1].rows >= first_hops);
            assert!(!expand.hop_estimates[1].exact);
        }
    }
}

#[test]
fn failed_prefixes_include_intermediate_labels_outside_the_endpoint_pair() {
    let mut program = program();
    let GraphMatchStep::Expand {
        min_hops, max_hops, ..
    } = &mut program.steps[1]
    else {
        unreachable!()
    };
    *min_hops = 2;
    *max_hops = 2;
    let mut catalog = catalog();
    catalog.rel_type_counts.insert("R".into(), 10_000);
    catalog.label_counts.insert("X".into(), 9_999);
    catalog
        .path_counts
        .insert(("A".into(), "R".into(), "B".into()), 1);
    catalog
        .path_counts
        .insert(("A".into(), "R".into(), "X".into()), 9_999);
    catalog
        .bounded_path_counts
        .insert(("A".into(), "R".into(), "B".into(), 1), 1);
    catalog
        .bounded_path_counts
        .insert(("A".into(), "R".into(), "X".into(), 1), 9_999);
    catalog.bounded_path_complete_through_hop = Some(3);
    // Every first hop dead-ends. The single A -> B edge does not describe
    // the 9,999 additional A -> X prefixes inspected by the same traversal.
    let estimate = cost(&program, &catalog);
    assert_eq!(estimate.estimated_rows, 1);
    assert!(estimate.random_io > 10_000 + 2 * 100, "{estimate:?}");
}

#[test]
fn positive_completions_charge_failed_prefixes_in_both_directions() {
    for reverse in [false, true] {
        let mut program = program();
        let GraphMatchStep::Expand {
            min_hops,
            max_hops,
            direction,
            ..
        } = &mut program.steps[1]
        else {
            unreachable!()
        };
        *min_hops = 2;
        *max_hops = 2;
        *direction = if reverse {
            RelationshipDirection::Incoming
        } else {
            RelationshipDirection::Outgoing
        };
        let mut catalog = catalog();
        catalog.label_counts.insert("A".into(), 1);
        catalog.label_counts.insert("B".into(), 10_001);
        catalog.label_counts.insert("X".into(), 1);
        catalog.rel_type_counts.insert("R".into(), 10_002);
        catalog.path_counts.clear();
        for (source, target, count) in [("A", "B", 10_000), ("A", "X", 1), ("X", "B", 1)] {
            let (source, target) = if reverse {
                (target, source)
            } else {
                (source, target)
            };
            catalog
                .path_counts
                .insert((source.into(), "R".into(), target.into()), count);
            catalog
                .bounded_path_counts
                .insert((source.into(), "R".into(), target.into(), 1), count);
        }
        let (source, target) = if reverse { ("B", "A") } else { ("A", "B") };
        catalog
            .bounded_path_counts
            .insert((source.into(), "R".into(), target.into(), 2), 1);
        catalog.bounded_path_complete_through_hop = Some(3);
        let estimate = cost(&program, &catalog);
        assert_eq!(estimate.estimated_rows, 1);
        // Ten thousand first-hop dead ends still require candidate visits and
        // adjacency probes, even with one successful two-hop completion.
        assert!(
            estimate.random_io > 2 * 10_001,
            "reverse={reverse}: {estimate:?}"
        );
    }
}

#[test]
fn positive_completions_charge_deeper_prefixes_with_other_labels() {
    for reverse in [false, true] {
        let mut program = program();
        let GraphMatchStep::Expand {
            min_hops,
            max_hops,
            direction,
            ..
        } = &mut program.steps[1]
        else {
            unreachable!()
        };
        *min_hops = 3;
        *max_hops = 3;
        *direction = if reverse {
            RelationshipDirection::Incoming
        } else {
            RelationshipDirection::Outgoing
        };
        let mut catalog = catalog();
        catalog.label_counts.insert("A".into(), 1);
        catalog.label_counts.insert("B".into(), 10_001);
        catalog.label_counts.insert("X".into(), 10_000);
        catalog.label_counts.insert("Y".into(), 1);
        catalog.label_counts.insert("Z".into(), 1);
        catalog.rel_type_counts.insert("R".into(), 20_003);
        catalog.path_counts.clear();
        for (source, target, count) in [
            ("A", "B", 10_000),
            ("A", "Y", 1),
            ("Y", "X", 10_000),
            ("Y", "Z", 1),
            ("Z", "B", 1),
        ] {
            let (source, target) = if reverse {
                (target, source)
            } else {
                (source, target)
            };
            catalog
                .path_counts
                .insert((source.into(), "R".into(), target.into()), count);
            catalog
                .bounded_path_counts
                .insert((source.into(), "R".into(), target.into(), 1), count);
        }
        for (target, hop, count) in [("X", 2, 10_000), ("Z", 2, 1), ("B", 3, 1)] {
            let (source, target) = if reverse {
                (target, "A")
            } else {
                ("A", target)
            };
            catalog
                .bounded_path_counts
                .insert((source.into(), "R".into(), target.into(), hop), count);
        }
        catalog.bounded_path_complete_through_hop = Some(3);
        let estimate = cost(&program, &catalog);
        assert_eq!(estimate.estimated_rows, 1);
        // Both shorter depths visit 10,001 prefixes, including X/Y/Z nodes
        // absent from the requested A/B endpoint pair.
        assert!(
            estimate.random_io > 4 * 10_001,
            "reverse={reverse}: {estimate:?}"
        );
    }
}
