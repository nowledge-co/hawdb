// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::fixtures::*;
use super::{oracle, PlanBindings, MERGED_ENTRIES};
use crate::graph::{cardinality, costing, selected_trace, OptimizerCatalog};
use crate::OptimizerContext;
use hawdb_plan::{visit_plan_with_ids, PhysicalPlan};

fn catalog(rows: u64, path_mode: usize) -> OptimizerCatalog {
    let mut catalog = OptimizerCatalog::default();
    for (label, count) in [
        ("Left", rows),
        ("Right", rows.saturating_mul(3)),
        ("Lookup", 71),
    ] {
        catalog.label_counts.insert(label.into(), count);
        for (property, ndv) in [("key", 7), ("score", 11), ("x", 13), ("y", 17)] {
            catalog
                .property_distinct_counts
                .insert((label.into(), property.into()), ndv);
        }
    }
    catalog
        .rel_type_counts
        .insert("LINK".into(), rows.saturating_mul(5));
    if path_mode >= 1 {
        let key = ("Left".into(), "LINK".into(), "Right".into());
        catalog
            .path_source_distinct_counts
            .insert(key.clone(), rows);
        catalog
            .path_target_distinct_counts
            .insert(key, rows.saturating_mul(2));
    }
    if path_mode >= 2 {
        for hop in 0..=3 {
            let key = ("Left".into(), "LINK".into(), "Right".into(), hop);
            catalog
                .bounded_path_source_distinct_counts
                .insert(key.clone(), rows);
            catalog
                .bounded_path_target_distinct_counts
                .insert(key, rows.saturating_add(1));
        }
    }
    catalog
}

fn assert_matches_oracle(plan: &PhysicalPlan, catalog: &OptimizerCatalog) {
    let bindings = PlanBindings::for_plan(plan);
    for variable in VARIABLES {
        assert_eq!(
            bindings.node_label(variable),
            oracle::physical_plan_node_label(plan, variable)
        );
        assert_eq!(
            bindings.relationship_type(variable),
            oracle::physical_plan_relationship_type(plan, variable)
        );
        assert_eq!(
            bindings.node_distinct_count(variable, catalog),
            oracle::physical_plan_node_variable_distinct_count(plan, variable, catalog)
        );
        for property in PROPERTIES {
            assert_eq!(
                bindings.covers_property(variable, property),
                oracle::physical_plan_access_path_covers_property(plan, variable, property)
            );
        }
    }
}

#[test]
fn bindings_preserve_every_access_and_unary_rule() {
    for kind in 0..ACCESS_KINDS {
        let leaf = access(kind, "n", "Left");
        for rows in [0, 1, 17, u64::MAX] {
            let catalog = catalog(rows, 2);
            assert_matches_oracle(&leaf, &catalog);
            for unary in 0..UNARY_KINDS {
                assert_tree_matches_oracle(&wrap(unary, leaf.clone()), &catalog);
            }
        }
    }
}

#[test]
fn bindings_keep_lookup_and_anonymous_relationship_barriers() {
    let catalog = catalog(17, 0);
    let expanded = expand(access(1, "n", "Left"), true, false);
    let lookup = wrap(7, expanded.clone());
    assert_tree_matches_oracle(&lookup, &catalog);
    let bindings = PlanBindings::for_plan(&lookup);
    assert_eq!(bindings.node_label("left"), Some("Lookup"));
    assert_eq!(bindings.node_label("n"), None);
    assert_eq!(bindings.relationship_type("r"), None);
    assert_eq!(bindings.node_distinct_count("n", &catalog), Some(17));
    assert_eq!(bindings.node_distinct_count("left", &catalog), None);
    assert!(bindings.covers_property("n", "key"));

    let anonymous = expand(expanded.clone(), false, false);
    assert_tree_matches_oracle(&anonymous, &catalog);
    assert_eq!(
        PlanBindings::for_plan(&anonymous).relationship_type("r"),
        None
    );
    let mut named = expand(expanded, true, false);
    if let PhysicalPlan::AdjacencyExpandExec {
        rel_variable,
        rel_type,
        ..
    } = &mut named
    {
        *rel_variable = Some("s".into());
        *rel_type = "OTHER".into();
    }
    assert_tree_matches_oracle(&named, &catalog);
    let bindings = PlanBindings::for_plan(&named);
    assert_eq!(bindings.relationship_type("r"), Some("LINK"));
    assert_eq!(bindings.relationship_type("s"), Some("OTHER"));
}

#[test]
fn bindings_preserve_binary_priority_and_union_coverage() {
    let catalog = catalog(17, 0);
    for large_on_left in [true, false] {
        let small = expand(access(1, "n", "Right"), true, false);
        let mut large = expand(access(3, "n", "Left"), true, false);
        if let PhysicalPlan::AdjacencyExpandExec {
            source_label,
            rel_type,
            ..
        } = &mut large
        {
            *source_label = "Lookup".into();
            *rel_type = "OTHER".into();
        }
        let large = product(large, access(1, "right", "Right"));
        let plan = if large_on_left {
            product(large, small)
        } else {
            product(small, large)
        };
        assert_tree_matches_oracle(&wrap(6, plan.clone()), &catalog);
        let bindings = PlanBindings::for_plan(&plan);
        assert_eq!(
            bindings.node_label("n"),
            Some(if large_on_left { "Lookup" } else { "Left" })
        );
        assert_eq!(
            bindings.node_distinct_count("n", &catalog),
            Some(if large_on_left { 71 } else { 17 })
        );
        assert_eq!(
            bindings.relationship_type("r"),
            Some(if large_on_left { "OTHER" } else { "LINK" })
        );
        assert!(bindings.covers_property("n", "key"));
        assert!(bindings.covers_property("n", "y"));
    }
}

#[test]
fn bindings_resolve_path_statistics_lazily_and_keep_source_priority() {
    for same_endpoint in [false, true] {
        for min in 0..=3 {
            for max in 0..=3 {
                let mut plan = expand(access(1, "left", "Lookup"), true, same_endpoint);
                if let PhysicalPlan::AdjacencyExpandExec {
                    min_hops, max_hops, ..
                } = &mut plan
                {
                    *min_hops = min;
                    *max_hops = max;
                }
                // The same summary must consult each supplied catalog; it must
                // not eagerly freeze a path statistic during derivation.
                let bindings = PlanBindings::for_plan(&plan);
                for rows in [0, 1, 17, u64::MAX] {
                    for mode in 0..3 {
                        let catalog = catalog(rows, mode);
                        assert_tree_matches_oracle(&wrap(6, plan.clone()), &catalog);
                        for variable in VARIABLES {
                            assert_eq!(
                                bindings.node_distinct_count(variable, &catalog),
                                oracle::physical_plan_node_variable_distinct_count(
                                    &plan, variable, &catalog
                                )
                            );
                        }
                    }
                }
                assert_eq!(bindings.node_label("n"), Some("Left"));
            }
        }
    }
}

fn wide_plan(start: usize, width: usize, shape: usize) -> PhysicalPlan {
    if width == 1 {
        return access(1, &format!("v{start}"), "Left");
    }
    let left = match shape {
        0 => width - 1,
        1 => 1,
        _ => width / 2,
    };
    product(
        wide_plan(start, left, shape),
        wide_plan(start + left, width - left, shape),
    )
}

#[test]
fn bindings_move_only_small_binary_maps_and_reuse_unary_maps() {
    for shape in 0..3 {
        let width = 64;
        let plan = wide_plan(0, width, shape);
        cardinality::take_metadata_visits();
        MERGED_ENTRIES.with(|count| count.set(0));
        let bindings = PlanBindings::for_plan(&plan);
        assert_eq!(cardinality::take_metadata_visits(), 2 * width - 1);
        let moved = MERGED_ENTRIES.with(|count| count.replace(0));
        let bound = if shape == 2 {
            3 * width * 6 / 2
        } else {
            3 * (width - 1)
        };
        assert!(
            moved <= bound,
            "shape={shape}, moved={moved}, bound={bound}"
        );
        for index in 0..width {
            let variable = format!("v{index}");
            assert_eq!(bindings.node_label(&variable), Some("Left"));
            assert!(bindings.covers_property(&variable, "key"));
        }
        let wrapper = wrap(0, PhysicalPlan::EmptyExec);
        let address = std::ptr::from_ref(bindings.node_populations.get("v0").unwrap());
        let forwarded = PlanBindings::for_operator(&wrapper, [Some(bindings), None]);
        assert_eq!(
            address,
            std::ptr::from_ref(forwarded.node_populations.get("v0").unwrap())
        );
        assert_eq!(MERGED_ENTRIES.with(|count| count.replace(0)), 0);

        // Also check the real costing entrypoint rather than only the test fold.
        cardinality::take_metadata_visits();
        costing::estimate_physical_plan_cost_breakdown(&plan, &catalog(17, 0));
        assert_eq!(cardinality::take_metadata_visits(), 2 * width - 1);
        assert_eq!(MERGED_ENTRIES.with(|count| count.replace(0)), moved);
    }
}

fn assert_tree_matches_oracle(plan: &PhysicalPlan, catalog: &OptimizerCatalog) -> usize {
    let derive = |plan| oracle::bindings(plan, catalog);
    let expected_cost = costing::estimate_cost_with_test_bindings(plan, catalog, &derive);
    let trace = selected_trace::selected_plan_trace(plan, catalog, &OptimizerContext::default());
    assert_eq!(trace.cost_breakdown, expected_cost);
    assert_eq!(
        costing::estimate_physical_plan_cost_breakdown(plan, catalog),
        expected_cost
    );
    let mut nodes = 0;
    visit_plan_with_ids(plan, &mut |operator_id, operator| {
        assert_matches_oracle(operator, catalog);
        let expected = costing::estimate_cost_with_test_bindings(operator, catalog, &|plan| {
            oracle::bindings(plan, catalog)
        });
        let actual = &trace.cardinality_estimates[nodes];
        assert_eq!(actual.operator_id, operator_id);
        assert_eq!(actual.operator, operator.kind());
        assert_eq!(actual.estimated_rows, expected.estimated_rows);
        nodes += 1;
    });
    assert_eq!(trace.cardinality_estimates.len(), nodes);
    nodes
}

#[test]
#[ignore = "manual deterministic cardinality metadata differential campaign"]
fn cardinality_metadata_differential_campaign() {
    let mut operators = 0;
    let mut cases = 0;
    for seed in [216, 0x5eed, 0xcafe] {
        let mut random = Random(seed);
        for case in 0..256 {
            let plan = random.plan(8, &mut 48);
            let rows = [0, 1, 17, u64::MAX][case % 4];
            for mode in 0..3 {
                operators += assert_tree_matches_oracle(&plan, &catalog(rows, mode));
                cases += 1;
            }
        }
    }
    println!("Cardinality metadata campaign: {cases} catalog/plan cases, {operators} operators, {} metadata comparisons", operators * VARIABLES.len() * (3 + PROPERTIES.len()));
    assert_eq!(cases, 2304);
    assert!(operators > 10_000);
}
