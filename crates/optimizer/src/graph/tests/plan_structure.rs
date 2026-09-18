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

use super::super::{
    CascadesOptimizer, LogicalPlanRoot, OptimizerCatalog, OptimizerCatalogIndexes,
    OptimizerCatalogStatistics, PlanPhaseKind,
};
use crate::{
    Distribution, MemoryBudgetClass, OptimizerConfig, OptimizerSearchDirective, PhysicalPlanClass,
    PhysicalPlanKind, ScanPruningSupport, VectorPrecision,
};
use hawdb_core::Value;
use hawdb_plan::{
    AggregateFunction, AggregateTarget, Aggregation, LogicalPlan, NodeProjectionAccess,
    PhysicalOperatorDomain, PhysicalPlan, PhysicalPlanChildren, PhysicalPlanDomainRef, Predicate,
    Projection, ProjectionExpression, SortDirection, SortItem, SortKey,
};
use std::collections::BTreeMap;

#[test]
fn physical_plan_metadata_describes_kind_class_and_children() {
    let plan = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "title".to_string(),
            },
            name: "title".to_string(),
        }],
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "id".to_string(),
                value: Value::Int(1),
            },
            input: Box::new(PhysicalPlan::IndexNodeSeek {
                variable: "m".to_string(),
                label: "Memory".to_string(),
                property: "id".to_string(),
                value: Value::Int(1),
            }),
        }),
    };

    assert_eq!(plan.kind(), PhysicalPlanKind::ProjectExec);
    assert_eq!(plan.kind().as_str(), "ProjectExec");
    assert_eq!(plan.class(), PhysicalPlanClass::Relational);
    assert_eq!(plan.domain(), PhysicalOperatorDomain::Relational);
    assert_eq!(plan.class().as_str(), "relational");
    assert_eq!(plan.children().len(), 1);

    let PhysicalPlanChildren::Unary(filter) = plan.children() else {
        panic!("project should expose a unary child");
    };
    assert_eq!(filter.kind(), PhysicalPlanKind::FilterExec);
    assert_eq!(filter.class(), PhysicalPlanClass::Relational);

    let PhysicalPlanChildren::Unary(seek) = filter.children() else {
        panic!("filter should expose a unary child");
    };
    assert_eq!(seek.kind(), PhysicalPlanKind::IndexNodeSeek);
    assert_eq!(seek.class(), PhysicalPlanClass::Access);
    assert_eq!(seek.domain(), PhysicalOperatorDomain::Access);
    let PhysicalPlanDomainRef::Access(access) = seek.as_domain() else {
        panic!("seek should expose the access domain wrapper");
    };
    assert!(std::ptr::eq(access.plan(), seek));
    assert!(seek.children().is_empty());
}

#[test]
fn physical_plan_metadata_describes_binary_children() {
    let plan = PhysicalPlan::NodeCartesianProductExec {
        left: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
        right: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "s".to_string(),
            label: "Source".to_string(),
        }),
    };

    assert_eq!(plan.kind(), PhysicalPlanKind::NodeCartesianProductExec);
    assert_eq!(plan.class(), PhysicalPlanClass::Relational);

    let PhysicalPlanChildren::Binary(left, right) = plan.children() else {
        panic!("cartesian product should expose binary children");
    };
    assert_eq!(left.kind(), PhysicalPlanKind::SeqNodeScan);
    assert_eq!(right.kind(), PhysicalPlanKind::SeqNodeScan);
    assert_eq!(left.class(), PhysicalPlanClass::Access);
    assert_eq!(right.class(), PhysicalPlanClass::Access);
}

#[test]
fn source_predicate_is_indexed_before_graph_expansion() {
    let source_predicate = Predicate::PropertyEq {
        variable: "m".to_string(),
        property: "space_id".to_string(),
        value: Value::String("space:1".to_string()),
    };
    let target_predicate = Predicate::PropertyEq {
        variable: "e".to_string(),
        property: "kind".to_string(),
        value: Value::String("person".to_string()),
    };
    let logical = LogicalPlan::Filter {
        predicate: Predicate::And(vec![source_predicate, target_predicate.clone()]),
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: Some("r".to_string()),
            rel_type: "MENTIONS".to_string(),
            rel_properties: Default::default(),
            direction: hawdb_cypher::RelationshipDirection::Outgoing,
            target_variable: "e".to_string(),
            target_label: "Entity".to_string(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "space_id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 10_000)],
            [("MENTIONS".to_string(), 25_000)],
            [],
            [],
            [],
            [(("Memory".to_string(), "space_id".to_string()), 100)],
            [],
        ),
    );

    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(matches!(
        plan,
        PhysicalPlan::FilterExec { predicate, input }
            if predicate == target_predicate
                && matches!(
                    input.as_ref(),
                    PhysicalPlan::AdjacencyExpandExec { input, .. }
                        if matches!(
                            input.as_ref(),
                            PhysicalPlan::IndexNodeSeek { property, .. }
                                if property == "space_id"
                        )
                )
    ));
    assert!(trace
        .rule_events
        .iter()
        .any(|event| { event.rule() == "transformation:push_source_filter_below_expand" }));
}

#[test]
fn bound_relationship_exists_lowers_to_an_observable_adjacency_operator() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::BoundRelationshipExists {
            source_variable: "c".to_string(),
            rel_type: "SYNTHESIZED_FROM".to_string(),
            direction: hawdb_cypher::RelationshipDirection::Outgoing,
            target_variable: "s".to_string(),
        },
        input: Box::new(LogicalPlan::Expand {
            source_variable: "c".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: None,
            rel_type: "CRYSTALLIZED_FROM".to_string(),
            rel_properties: Default::default(),
            direction: hawdb_cypher::RelationshipDirection::Outgoing,
            target_variable: "s".to_string(),
            target_label: "Memory".to_string(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: Box::new(LogicalPlan::NodeScan {
                variable: "c".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };

    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &OptimizerCatalog::default());

    assert!(matches!(
        plan,
        PhysicalPlan::AdjacencyExistsExec {
            source_variable,
            rel_type,
            direction: hawdb_cypher::RelationshipDirection::Outgoing,
            target_variable,
            input,
        } if source_variable == "c"
            && rel_type == "SYNTHESIZED_FROM"
            && target_variable == "s"
            && matches!(input.as_ref(), PhysicalPlan::AdjacencyExpandExec { .. })
    ));
    assert_eq!(
        trace
            .selected_plan_operator_counts
            .get("AdjacencyExistsExec"),
        Some(&1)
    );
    assert!(trace
        .decisions
        .iter()
        .any(|decision| decision.contains("AdjacencyExistsExec")));
}

#[test]
fn exact_or_lookup_lowers_to_one_index_multiseek() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::Or(vec![
            Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "stable_id".to_string(),
                value: Value::String("memory:1".to_string()),
            },
            Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "stable_id".to_string(),
                value: Value::String("memory:2".to_string()),
            },
        ]),
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [("Memory".to_string(), "stable_id".to_string())],
            [],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 10_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "stable_id".to_string()), 10_000)],
            [],
        ),
    );

    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(matches!(
        plan,
        PhysicalPlan::IndexNodeMultiSeek {
            property,
            values,
            ..
        } if property == "stable_id"
            && values
                == vec![
                    Value::String("memory:1".to_string()),
                    Value::String("memory:2".to_string()),
                ]
    ));
    assert!(trace
        .rule_events
        .iter()
        .any(|event| { event.rule() == "transformation:simplify_filter_predicate" }));
}

#[test]
fn different_exact_properties_lower_to_one_bounded_union_seek() {
    let predicate = Predicate::Or(vec![
        Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "id".to_string(),
            value: Value::String("memory:1".to_string()),
        },
        Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "external_id".to_string(),
            value: Value::String("memory:1".to_string()),
        },
    ]);
    let logical = LogicalPlan::Filter {
        predicate: predicate.clone(),
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [
                ("Memory".to_string(), "id".to_string()),
                ("Memory".to_string(), "external_id".to_string()),
            ],
            [],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 10_000)],
            [],
            [],
            [],
            [],
            [
                (("Memory".to_string(), "id".to_string()), 10_000),
                (("Memory".to_string(), "external_id".to_string()), 10_000),
            ],
            [],
        ),
    );

    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(matches!(
        plan,
        PhysicalPlan::FilterExec {
            predicate: residual,
            input,
        } if residual == predicate
            && matches!(
                input.as_ref(),
                PhysicalPlan::IndexNodeUnionSeek { branches, .. }
                    if branches.len() == 2
                        && branches[0].property == "id"
                        && branches[1].property == "external_id"
            )
    ));
    assert_eq!(trace.selected_plan_cost.estimated_rows, 2);
    assert!(trace
        .rule_events
        .iter()
        .any(|event| event.rule() == "implementation:node_exact_index_union_seek"));
}

#[test]
fn unfiltered_node_count_uses_exact_count_store() {
    let logical = LogicalPlan::Aggregate {
        group_keys: Vec::new(),
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::All,
            distinct: false,
            name: "memory_count".to_string(),
        }],
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };

    let (plan, trace) =
        CascadesOptimizer::new(OptimizerConfig { max_groups: 16 }).optimize_with_trace(&logical);

    assert_eq!(
        plan,
        PhysicalPlan::NodeCountExec {
            label: "Memory".to_string(),
            output: "memory_count".to_string(),
        }
    );
    assert_eq!(trace.selected_plan_cost.estimated_rows, 1);
    assert_eq!(
        trace.selected_plan_operator_counts.get("NodeCountExec"),
        Some(&1)
    );
    assert!(!trace
        .selected_plan_operator_counts
        .contains_key("SeqNodeScan"));
    assert!(trace
        .rule_events
        .iter()
        .any(|event| { event.rule() == "implementation:node_count_fast_path" }));

    let direct = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_root_with_catalog_and_directive(
            &LogicalPlanRoot::new(logical),
            &OptimizerCatalog::default(),
            OptimizerSearchDirective::DirectFallback,
        )
        .expect("direct fallback must preserve the exact count fast path");
    assert_eq!(direct.plan(), &plan);
}

#[test]
fn unfiltered_relationship_count_uses_exact_count_store() {
    let logical = LogicalPlan::Aggregate {
        group_keys: Vec::new(),
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Variable("r".to_string()),
            distinct: false,
            name: "mention_count".to_string(),
        }],
        input: Box::new(LogicalPlan::Expand {
            source_variable: "source".to_string(),
            source_label: String::new(),
            rel_variable: Some("r".to_string()),
            rel_type: "MENTIONS".to_string(),
            rel_properties: BTreeMap::new(),
            direction: hawdb_cypher::RelationshipDirection::Outgoing,
            target_variable: "target".to_string(),
            target_label: String::new(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: Box::new(LogicalPlan::NodeScan {
                variable: "source".to_string(),
                label: String::new(),
            }),
        }),
    };

    let (plan, trace) =
        CascadesOptimizer::new(OptimizerConfig { max_groups: 16 }).optimize_with_trace(&logical);

    assert_eq!(
        plan,
        PhysicalPlan::RelationshipCountExec {
            rel_type: "MENTIONS".to_string(),
            output: "mention_count".to_string(),
        }
    );
    assert_eq!(trace.selected_plan_cost.estimated_rows, 1);
    assert_eq!(
        trace
            .selected_plan_operator_counts
            .get("RelationshipCountExec"),
        Some(&1)
    );
    assert!(!trace
        .selected_plan_operator_counts
        .contains_key("AdjacencyExpandExec"));
    assert!(trace
        .rule_events
        .iter()
        .any(|event| event.rule() == "implementation:relationship_count_fast_path"));
}

#[test]
fn filtered_node_aggregate_decodes_only_predicate_and_argument_properties() {
    let predicate = Predicate::PropertyEq {
        variable: "m".to_string(),
        property: "kind".to_string(),
        value: Value::String("note".to_string()),
    };
    let logical = LogicalPlan::Aggregate {
        group_keys: Vec::new(),
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Property {
                variable: "m".to_string(),
                property: "updated_at".to_string(),
            },
            distinct: false,
            name: "updated_count".to_string(),
        }],
        input: Box::new(LogicalPlan::Filter {
            predicate: predicate.clone(),
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "kind".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 10_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "kind".to_string()), 8)],
            [],
        ),
    );

    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(matches!(
        plan,
        PhysicalPlan::AggregateExec { input, .. }
            if matches!(
                input.as_ref(),
                PhysicalPlan::NodeProjectionScanExec {
                    access: NodeProjectionAccess::PropertyValues { property, .. },
                    required_properties,
                    predicate: None,
                    items,
                    ..
                } if property == "kind"
                    && required_properties
                        == &vec!["kind".to_string(), "updated_at".to_string()]
                    && items.is_empty()
            )
    ));
    assert!(trace.decisions.iter().any(|decision| {
        decision.contains("selected implementation:node_aggregate_required_scan")
            && decision.contains("decode 2 required properties")
    }));
}

#[test]
fn physical_cardinality_estimates_never_reach_zero() {
    use super::super::costing::{
        estimate_physical_plan_cost, estimate_physical_plan_cost_breakdown,
    };

    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::default(),
        OptimizerCatalogStatistics {
            label_counts: BTreeMap::from([("Memory".to_string(), 0)]),
            ..OptimizerCatalogStatistics::default()
        },
    );
    let scan = PhysicalPlan::SeqNodeScan {
        variable: "m".to_string(),
        label: "Memory".to_string(),
    };
    let plans = [
        scan.clone(),
        PhysicalPlan::FilterExec {
            predicate: Predicate::ConstantBool(false),
            input: Box::new(scan.clone()),
        },
        PhysicalPlan::TopNExec {
            items: vec![SortItem {
                key: SortKey::Property {
                    variable: "m".to_string(),
                    property: "id".to_string(),
                },
                direction: SortDirection::Asc,
            }],
            offset: 1,
            limit: 0,
            input: Box::new(scan),
        },
    ];

    for plan in plans {
        assert_eq!(
            estimate_physical_plan_cost(&plan, &catalog).estimated_rows,
            1
        );
        assert_eq!(
            estimate_physical_plan_cost_breakdown(&plan, &catalog).estimated_rows,
            1
        );
    }
}

#[test]
fn scalar_node_projection_decodes_only_required_properties() {
    let logical = LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "title".to_string(),
            },
            name: "title".to_string(),
        }],
        input: Box::new(LogicalPlan::Filter {
            predicate: Predicate::PropertyCompare {
                variable: "m".to_string(),
                property: "rank".to_string(),
                op: hawdb_plan::ComparisonOp::Gte,
                value: Value::Int(10),
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };

    let (plan, trace) =
        CascadesOptimizer::new(OptimizerConfig { max_groups: 16 }).optimize_with_trace(&logical);

    assert!(matches!(
        plan,
        PhysicalPlan::NodeProjectionScanExec {
            access: NodeProjectionAccess::LabelScan,
            ref required_properties,
            ..
        } if required_properties == &["rank".to_string(), "title".to_string()]
    ));
    assert!(trace.selected_plan_cost.estimated_rows >= 1);
    assert!(!trace
        .selected_plan_operator_counts
        .contains_key("SeqNodeScan"));
    assert!(trace
        .rule_events
        .iter()
        .any(|event| event.rule() == "implementation:node_projection_scan"));
    assert!(trace
        .stage_events
        .iter()
        .any(|event| event.name() == "plan_finalization"));
}

#[test]
fn multiseek_projection_preserves_index_access_and_required_properties() {
    let logical = LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "title".to_string(),
            },
            name: "title".to_string(),
        }],
        input: Box::new(LogicalPlan::Filter {
            predicate: Predicate::Or(vec![
                Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "stable_id".to_string(),
                    value: Value::String("memory:1".to_string()),
                },
                Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "stable_id".to_string(),
                    value: Value::String("memory:2".to_string()),
                },
            ]),
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [("Memory".to_string(), "stable_id".to_string())],
            [],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 10_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "stable_id".to_string()), 10_000)],
            [],
        ),
    );

    let (plan, _) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(matches!(
        plan,
        PhysicalPlan::NodeProjectionScanExec {
            access: NodeProjectionAccess::PropertyValues { property, values },
            required_properties,
            predicate: None,
            ..
        } if property == "stable_id"
            && values
                == vec![
                    Value::String("memory:1".to_string()),
                    Value::String("memory:2".to_string()),
                ]
            && required_properties == vec!["stable_id".to_string(), "title".to_string()]
    ));
}

#[test]
fn composite_projection_preserves_index_access_and_residual_validation() {
    let predicate = Predicate::And(vec![
        Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "space_id".to_string(),
            value: Value::String("space:1".to_string()),
        },
        Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "kind".to_string(),
            value: Value::String("note".to_string()),
        },
    ]);
    let logical = LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "title".to_string(),
            },
            name: "title".to_string(),
        }],
        input: Box::new(LogicalPlan::Filter {
            predicate: predicate.clone(),
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [],
            [(
                "Memory".to_string(),
                vec!["space_id".to_string(), "kind".to_string()],
            )],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 10_000)],
            [],
            [],
            [],
            [],
            [
                (("Memory".to_string(), "space_id".to_string()), 100),
                (("Memory".to_string(), "kind".to_string()), 10),
            ],
            [],
        ),
    );

    let (plan, _) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(matches!(
        plan,
        PhysicalPlan::NodeProjectionScanExec {
            access: NodeProjectionAccess::CompositeEquality { predicates },
            required_properties,
            predicate: Some(residual),
            ..
        } if predicates
            == vec![
                ("space_id".to_string(), Value::String("space:1".to_string())),
                ("kind".to_string(), Value::String("note".to_string())),
            ]
            && required_properties
                == vec!["kind".to_string(), "space_id".to_string(), "title".to_string()]
            && residual == predicate
    ));
}

#[test]
fn composite_range_projection_uses_a_contiguous_equality_prefix() {
    let predicate = Predicate::And(vec![
        Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "space_id".to_string(),
            value: Value::String("space:1".to_string()),
        },
        Predicate::PropertyCompare {
            variable: "m".to_string(),
            property: "created_at".to_string(),
            op: hawdb_plan::ComparisonOp::Gte,
            value: Value::Int(100),
        },
        Predicate::PropertyCompare {
            variable: "m".to_string(),
            property: "created_at".to_string(),
            op: hawdb_plan::ComparisonOp::Lt,
            value: Value::Int(200),
        },
    ]);
    let logical = LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "title".to_string(),
            },
            name: "title".to_string(),
        }],
        input: Box::new(LogicalPlan::Filter {
            predicate: predicate.clone(),
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let index_properties = vec![
        "space_id".to_string(),
        "created_at".to_string(),
        "stable_id".to_string(),
    ];
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [],
            [("Memory".to_string(), index_properties.clone())],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 100_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "space_id".to_string()), 1_000)],
            [],
        ),
    );

    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(matches!(
        plan,
        PhysicalPlan::NodeProjectionScanExec {
            access: NodeProjectionAccess::CompositeRange { seek },
            required_properties,
            predicate: Some(residual),
            ..
        } if seek.index_properties == index_properties
            && seek.equality_prefix
                == vec![(
                    "space_id".to_string(),
                    Value::String("space:1".to_string()),
                )]
            && seek.range_property == "created_at"
            && seek.lower == Some((Value::Int(100), true))
            && seek.upper == Some((Value::Int(200), false))
            && required_properties
                == vec![
                    "created_at".to_string(),
                    "space_id".to_string(),
                    "title".to_string(),
                ]
            && residual == predicate
    ));
    assert!(trace
        .rule_events
        .iter()
        .any(|event| event.rule() == "implementation:node_composite_range_seek"));
}

#[test]
fn composite_range_projection_rejects_a_gap_in_the_index_prefix() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::And(vec![
            Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "space_id".to_string(),
                value: Value::String("space:1".to_string()),
            },
            Predicate::PropertyCompare {
                variable: "m".to_string(),
                property: "created_at".to_string(),
                op: hawdb_plan::ComparisonOp::Gte,
                value: Value::Int(100),
            },
        ]),
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [],
            [(
                "Memory".to_string(),
                vec![
                    "space_id".to_string(),
                    "kind".to_string(),
                    "created_at".to_string(),
                ],
            )],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 100_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "space_id".to_string()), 1_000)],
            [],
        ),
    );

    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(!matches!(
        plan,
        PhysicalPlan::IndexNodeCompositeRangeSeek { .. }
            | PhysicalPlan::NodeProjectionScanExec {
                access: NodeProjectionAccess::CompositeRange { .. },
                ..
            }
    ));
    assert!(!trace
        .rule_events
        .iter()
        .any(|event| event.rule() == "implementation:node_composite_range_seek"));
}

#[test]
fn optimizer_trace_reports_physical_plan_operator_and_class_counts() {
    let logical = LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "title".to_string(),
            },
            name: "title".to_string(),
        }],
        input: Box::new(LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "id".to_string(),
                value: Value::Int(1),
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 100)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "id".to_string()), 100)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);
    assert_eq!(
        trace
            .selected_plan_operator_counts
            .get("NodeProjectionScanExec"),
        Some(&1)
    );
    assert_eq!(trace.selected_plan_operator_counts.get("ProjectExec"), None);
    assert_eq!(
        trace.selected_plan_operator_counts.get("IndexNodeSeek"),
        None
    );
    assert_eq!(trace.selected_plan_operator_counts.get("FilterExec"), None);
    assert_eq!(trace.selected_plan_class_counts.get("access"), Some(&1));
    assert_eq!(
        trace.selected_plan_properties.scan_pruning,
        ScanPruningSupport::Index
    );
    assert_eq!(
        trace.selected_plan_properties.covering_fields,
        ["Memory.id".to_string(), "Memory.title".to_string()]
    );
    assert_eq!(
        trace.selected_plan_cost_breakdown.as_plan_cost(),
        trace.selected_plan_cost
    );
    assert_eq!(trace.selected_plan_cost_breakdown.cpu, 2);
    assert_eq!(trace.selected_plan_cost_breakdown.random_io, 2);
    assert_eq!(trace.selected_plan_cost_breakdown.sequential_io, 0);

    let (_, fallback_trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 1 })
        .optimize_with_catalog(&logical, &catalog);
    assert_eq!(
        fallback_trace
            .selected_plan_operator_counts
            .get("NodeProjectionScanExec"),
        Some(&1)
    );
    assert_eq!(
        fallback_trace
            .selected_plan_operator_counts
            .get("ProjectExec"),
        None
    );
    assert_eq!(
        fallback_trace
            .selected_plan_operator_counts
            .get("IndexNodeSeek"),
        None
    );
    assert_eq!(
        fallback_trace.selected_plan_class_counts.get("access"),
        Some(&1)
    );
    assert_eq!(
        fallback_trace.selected_plan_cost_breakdown.as_plan_cost(),
        fallback_trace.selected_plan_cost
    );
}

#[test]
fn optimizer_roots_preserve_logical_and_physical_phase_boundaries() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "id".to_string(),
            value: Value::Int(1),
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let logical_root = LogicalPlanRoot::new(logical);
    assert_eq!(logical_root.phase(), PlanPhaseKind::Logical);

    let lowering_ready_root = logical_root.clone().into_lowering_ready();
    assert_eq!(lowering_ready_root.phase(), PlanPhaseKind::LoweringReady);

    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 100)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "id".to_string()), 100)],
            [],
        ),
    );
    let physical_root = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_lowering_ready_root_with_catalog(&lowering_ready_root, &catalog);

    assert_eq!(physical_root.phase(), PlanPhaseKind::Physical);
    assert_eq!(physical_root.plan().kind(), PhysicalPlanKind::IndexNodeSeek);
    assert!(physical_root
        .trace()
        .stage_events
        .iter()
        .any(|event| event.name() == "access_path_selection"));
    assert_eq!(
        physical_root
            .trace()
            .stage_events
            .iter()
            .filter(|event| event.name() == "logical_rewrite")
            .count(),
        1
    );
}

#[test]
fn source_filter_selects_an_optimizer_visible_segment_scan() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyEq {
            variable: "s".to_string(),
            property: "space_id".to_string(),
            value: Value::String("alpha".to_string()),
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "s".to_string(),
            label: "Source".to_string(),
        }),
    };
    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &OptimizerCatalog::default());
    assert_eq!(
        trace.selected_plan_properties.scan_pruning,
        ScanPruningSupport::Segment
    );
    let PhysicalPlan::FilterExec { input, .. } = plan else {
        panic!("expected residual filter over source segment scan");
    };
    assert!(matches!(*input, PhysicalPlan::SourceSegmentScan { .. }));
}

#[test]
fn source_index_seek_remains_preferred_over_segment_scan() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyEq {
            variable: "s".to_string(),
            property: "id".to_string(),
            value: Value::String("source-a".to_string()),
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "s".to_string(),
            label: "Source".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Source".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Source".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Source".to_string(), "id".to_string()), 1_000)],
            [],
        ),
    );
    let plan = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog)
        .0;
    assert!(matches!(plan, PhysicalPlan::IndexNodeSeek { .. }));
}

#[test]
fn selected_plan_properties_report_distribution_and_sort_ordering() {
    let logical = LogicalPlan::Limit {
        offset: 0,
        limit: Some(10),
        input: Box::new(LogicalPlan::Project {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "title".to_string(),
                },
                name: "title".to_string(),
            }],
            input: Box::new(LogicalPlan::Sort {
                items: vec![SortItem {
                    key: SortKey::Column("title".to_string()),
                    direction: SortDirection::Asc,
                }],
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        }),
    };

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &OptimizerCatalog::default());

    assert_eq!(
        trace.selected_plan_properties.distribution,
        Distribution::Single
    );
    assert_eq!(
        trace.selected_plan_properties.ordering,
        vec!["title asc".to_string()]
    );
    assert_eq!(
        trace.selected_plan_properties.scan_pruning,
        ScanPruningSupport::Label
    );
    assert_eq!(
        trace.selected_plan_properties.vector_precision,
        VectorPrecision::NotVector
    );
    assert_eq!(
        trace.selected_plan_properties.memory_budget,
        MemoryBudgetClass::Blocking
    );
}

#[test]
fn sort_with_bounded_limit_lowers_to_top_n() {
    let logical = LogicalPlan::Limit {
        offset: 5,
        limit: Some(10),
        input: Box::new(LogicalPlan::Sort {
            items: vec![SortItem {
                key: SortKey::Property {
                    variable: "m".to_string(),
                    property: "score".to_string(),
                },
                direction: SortDirection::Desc,
            }],
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };

    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::default(),
        OptimizerCatalogStatistics::new([("Memory".to_string(), 1_000)], [], [], [], [], [], []),
    );
    let (plan, trace) = CascadesOptimizer::default().optimize_with_catalog(&logical, &catalog);

    assert_eq!(plan.kind(), PhysicalPlanKind::TopNExec);
    assert_eq!(trace.selected_plan_operator_counts["TopNExec"], 1);
    assert!(!trace.selected_plan_operator_counts.contains_key("SortExec"));
    assert!(!trace
        .selected_plan_operator_counts
        .contains_key("LimitExec"));
    assert_eq!(
        trace.selected_plan_properties.ordering,
        vec!["m.score desc".to_string()]
    );
    assert!(trace
        .decisions
        .iter()
        .any(|decision| decision.starts_with("choose TopN for bounded sort: offset=5 limit=10")));

    let (fallback_plan, fallback_trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 1 })
        .optimize_with_catalog(&logical, &catalog);
    assert_eq!(fallback_plan.kind(), PhysicalPlanKind::TopNExec);
    assert!(fallback_trace
        .decisions
        .iter()
        .any(|decision| decision.starts_with("choose TopN for bounded sort: offset=5 limit=10")));
}

#[test]
fn bounded_sort_keeps_full_sort_when_limit_covers_the_input() {
    let logical = LogicalPlan::Limit {
        offset: 0,
        limit: Some(20),
        input: Box::new(LogicalPlan::Sort {
            items: vec![SortItem {
                key: SortKey::Property {
                    variable: "m".to_string(),
                    property: "score".to_string(),
                },
                direction: SortDirection::Desc,
            }],
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::default(),
        OptimizerCatalogStatistics::new([("Memory".to_string(), 10)], [], [], [], [], [], []),
    );

    let (plan, trace) = CascadesOptimizer::default().optimize_with_catalog(&logical, &catalog);

    assert_eq!(plan.kind(), PhysicalPlanKind::LimitExec);
    assert_eq!(trace.selected_plan_operator_counts["SortExec"], 1);
    assert_eq!(trace.selected_plan_operator_counts["LimitExec"], 1);
    assert!(!trace.selected_plan_operator_counts.contains_key("TopNExec"));
    assert!(trace.decisions.iter().any(|decision| {
        decision.starts_with("keep Sort + Limit for bounded sort: offset=0 limit=20")
    }));

    let (fallback_plan, fallback_trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 1 })
        .optimize_with_catalog(&logical, &catalog);
    assert_eq!(fallback_plan.kind(), PhysicalPlanKind::LimitExec);
    assert!(fallback_trace.decisions.iter().any(|decision| {
        decision.starts_with("keep Sort + Limit for bounded sort: offset=0 limit=20")
    }));
}

#[test]
fn conjunction_access_path_compares_equality_and_range_candidates_by_total_cost() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::And(vec![
            Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "kind".to_string(),
                value: Value::String("note".to_string()),
            },
            Predicate::PropertyCompare {
                variable: "m".to_string(),
                property: "created_at".to_string(),
                op: hawdb_plan::ComparisonOp::Gte,
                value: Value::Int(99),
            },
        ]),
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [("Memory".to_string(), "kind".to_string())],
            [],
            [("Memory".to_string(), "created_at".to_string())],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 100_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "kind".to_string()), 4)],
            [(
                ("Memory".to_string(), "created_at".to_string()),
                (0..100).map(Value::Int).collect::<Vec<_>>(),
            )],
        ),
    );

    let (plan, trace) = CascadesOptimizer::default().optimize_with_catalog(&logical, &catalog);

    assert!(plan.instance_fingerprint().contains("IndexNodeRangeSeek"));
    assert!(trace
        .rule_events
        .iter()
        .any(|event| { event.rule() == "implementation:node_range_index_seek" }));
    assert!(trace
        .decisions
        .iter()
        .any(|decision| { decision.contains("alternatives_considered=2") }));
    let access_path_stage = trace
        .stage_events
        .iter()
        .find(|event| event.name() == "access_path_selection")
        .expect("conjunction access path selection should emit a stage trace");
    assert_eq!(access_path_stage.stats().input_count, 1);
    assert_eq!(access_path_stage.stats().output_count, 2);
    assert_eq!(access_path_stage.stats().applied_rules, 2);
    assert_eq!(access_path_stage.stats().skipped_rules, 2);
}
