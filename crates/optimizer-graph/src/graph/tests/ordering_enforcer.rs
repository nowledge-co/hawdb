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

use super::super::{properties::satisfies_ordering, *};
use hawdb_core::Value;
use hawdb_plan_cypher::ComparisonOp;
use hawdb_plan_cypher::{
    LogicalPlan, Predicate, Projection, ProjectionExpression, SortDirection, SortItem, SortKey,
};

fn catalog(ordered: bool) -> OptimizerCatalog {
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [("Item".to_string(), "rank".to_string())], []),
        OptimizerCatalogStatistics::new(
            [("Item".to_string(), 1000)],
            [],
            [],
            [],
            [],
            [],
            [(
                ("Item".to_string(), "rank".to_string()),
                (-990..10).map(Value::Int).collect(),
            )],
        ),
    );
    if ordered {
        catalog.with_ordered_range_indexes([("Item".to_string(), "rank".to_string())])
    } else {
        catalog
    }
}

fn key(variable: &str, property: &str) -> SortItem {
    SortItem {
        key: SortKey::Property {
            variable: variable.to_string(),
            property: property.to_string(),
        },
        direction: SortDirection::Asc,
    }
}

fn range_plan() -> PhysicalPlan {
    PhysicalPlan::IndexNodeRangeSeek {
        variable: "n".to_string(),
        label: "Item".to_string(),
        property: "rank".to_string(),
        lower: Some((Value::Int(10), false)),
        upper: None,
    }
}

#[test]
fn ordering_enforcer_requires_capability_in_normal_and_direct_lowering() {
    for max_groups in [1, 1000] {
        for ordered in [false, true] {
            for bounded in [false, true] {
                let mut logical = LogicalPlan::Sort {
                    items: vec![key("n", "rank")],
                    input: Box::new(LogicalPlan::Filter {
                        predicate: Predicate::PropertyCompare {
                            variable: "n".to_string(),
                            property: "rank".to_string(),
                            op: ComparisonOp::Gt,
                            value: Value::Int(10),
                        },
                        input: Box::new(LogicalPlan::NodeScan {
                            variable: "n".to_string(),
                            label: "Item".to_string(),
                        }),
                    }),
                };
                if bounded {
                    logical = LogicalPlan::Limit {
                        offset: 2,
                        limit: Some(3),
                        input: Box::new(logical),
                    };
                }
                let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups })
                    .optimize_with_catalog(&logical, &catalog(ordered));
                let explain = plan.explain(0);
                assert!(explain.contains("IndexNodeRangeSeek"), "{explain}");
                assert_eq!(
                    !explain.contains("SortExec") && !explain.contains("TopNExec"),
                    ordered,
                    "{explain}"
                );
                if ordered {
                    assert_eq!(trace.selected_plan_properties.ordering, ["n.rank asc"]);
                    assert!(trace
                        .decisions
                        .iter()
                        .any(|decision| decision.contains("satisfies required ordering")));
                }
            }
        }
    }
}

#[test]
fn ordering_enforcer_checks_typed_keys_directions_and_projection_overwrites() {
    let catalog = catalog(true);
    let range = range_plan();
    assert!(satisfies_ordering(&range, &[key("n", "rank")], &catalog));
    for wrong in [
        key("other", "rank"),
        key("n", "other"),
        SortItem {
            direction: SortDirection::Desc,
            ..key("n", "rank")
        },
    ] {
        assert!(!satisfies_ordering(&range, &[wrong], &catalog));
    }
    assert!(!satisfies_ordering(
        &range,
        &[key("n", "rank"), key("n", "other")],
        &catalog
    ));
    let mut project = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            name: "score".to_string(),
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "rank".to_string(),
            },
        }],
        input: Box::new(range),
    };
    let alias = SortItem {
        key: SortKey::Column("score".to_string()),
        direction: SortDirection::Asc,
    };
    assert!(satisfies_ordering(
        &project,
        std::slice::from_ref(&alias),
        &catalog
    ));
    let PhysicalPlan::ProjectExec { items, .. } = &mut project else {
        unreachable!()
    };
    items.push(Projection {
        name: "score".to_string(),
        expression: ProjectionExpression::Literal(Value::Int(0)),
    });
    // Execution keeps the first alias and suffixes later duplicates. Neither
    // occurrence may be assumed to describe the unsuffixed scalar column.
    items.reverse();
    assert!(!satisfies_ordering(&project, &[alias], &catalog));
    assert!(satisfies_ordering(&project, &[key("n", "rank")], &catalog));

    let catalog = catalog.with_ordered_range_indexes([("Item".to_string(), "rank".to_string())]);
    let range = PhysicalPlan::IndexNodeRangeSeek {
        variable: "a.b".to_string(),
        label: "Item".to_string(),
        property: "rank".to_string(),
        lower: Some((Value::Int(10), false)),
        upper: None,
    };
    // These have identical display strings but different identifier boundaries.
    assert!(!satisfies_ordering(&range, &[key("a", "b.rank")], &catalog));
    let product = PhysicalPlan::NodeCartesianProductExec {
        left: Box::new(range_plan()),
        right: Box::new(range_plan()),
    };
    assert!(!satisfies_ordering(&product, &[key("n", "rank")], &catalog));
    let lookup = PhysicalPlan::NodeColumnLookupExec {
        variable: "n".to_string(),
        label: "Item".to_string(),
        property: "id".to_string(),
        column: "other".to_string(),
        optional: false,
        node_visibility_predicate: None,
        input: Box::new(range_plan()),
    };
    assert!(!satisfies_ordering(&lookup, &[key("n", "rank")], &catalog));
}

#[test]
fn ordering_enforcer_fused_projection_does_not_advertise_native_bindings() {
    let catalog = catalog(true);
    let plan = PhysicalPlan::NodeProjectionScanExec {
        variable: "n".to_string(),
        label: "Item".to_string(),
        access: hawdb_plan_cypher::NodeProjectionAccess::PropertyRange {
            property: "rank".to_string(),
            lower: Some((Value::Int(0), false)),
            upper: None,
        },
        required_properties: vec!["rank".to_string()],
        predicate: None,
        items: vec![Projection {
            name: "score".to_string(),
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "rank".to_string(),
            },
        }],
    };
    assert!(!satisfies_ordering(&plan, &[key("n", "rank")], &catalog));
    assert!(satisfies_ordering(
        &plan,
        &[SortItem {
            key: SortKey::Column("score".to_string()),
            direction: SortDirection::Asc
        }],
        &catalog
    ));
    assert_eq!(
        super::super::properties::selected_plan_properties(&plan, &catalog).ordering,
        ["score asc"]
    );
}
