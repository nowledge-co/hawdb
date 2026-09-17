use super::super::{
    CascadesOptimizer, OptimizerCatalog, OptimizerCatalogIndexes, OptimizerCatalogStatistics,
};
use crate::{
    LogicalPlanRoot, OptimizerConfig, OptimizerSearchDirective, OptimizerSearchDirectiveError,
    PlanCost, SearchMode,
};
use skein_core::Value;
use skein_cypher::RelationshipDirection;
use skein_plan::{LogicalPlan, Predicate, Projection, ProjectionExpression};
use std::collections::BTreeMap;

#[test]
fn optimizer_budget_changes_lowering_without_degrading_the_plan() {
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

    let budgeted = CascadesOptimizer::new(OptimizerConfig { max_groups: 2 });
    let (budgeted_plan, budgeted_trace) = budgeted.optimize_with_catalog(&logical, &catalog);
    assert_eq!(budgeted_trace.groups, 4);
    assert_eq!(budgeted_trace.search_mode, SearchMode::DirectFallback);
    assert!(budgeted_trace.warnings.is_empty());
    assert!(budgeted_trace.decisions.iter().any(|decision| {
        decision.contains("required_groups=4 max_groups=2")
            && decision.contains("same physical alternatives")
    }));
    assert!(budgeted_trace
        .selected_plan
        .contains("NodeProjectionScanExec"));
    assert!(budgeted_trace.selected_plan.contains("PropertyValues"));
    assert!(budgeted_trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeSeek")));

    let full = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 });
    let (full_plan, full_trace) = full.optimize_with_catalog(&logical, &catalog);
    assert!(full_trace.warnings.is_empty());
    assert_eq!(budgeted_plan, full_plan);
    assert_eq!(
        budgeted_trace.selected_plan_cost,
        full_trace.selected_plan_cost
    );
    assert_eq!(
        budgeted_trace.selected_plan_fingerprint,
        full_trace.selected_plan_fingerprint
    );
}

#[test]
fn explicit_search_directives_preserve_the_optimizer_budget_contract() {
    let logical_root = LogicalPlanRoot::new(LogicalPlan::NodeScan {
        variable: "m".to_string(),
        label: "Memory".to_string(),
    });
    let optimizer = CascadesOptimizer::new(OptimizerConfig { max_groups: 0 });
    let catalog = OptimizerCatalog::default();

    assert_eq!(
        optimizer.optimize_root_with_catalog_and_directive(
            &logical_root,
            &catalog,
            OptimizerSearchDirective::Memo,
        ),
        Err(OptimizerSearchDirectiveError::MemoGroupBudgetExceeded {
            required_groups: 1,
            max_groups: 0,
        })
    );

    let fallback = optimizer
        .optimize_root_with_catalog_and_directive(
            &logical_root,
            &catalog,
            OptimizerSearchDirective::DirectFallback,
        )
        .unwrap();
    assert_eq!(fallback.trace().search_mode, SearchMode::DirectFallback);
    assert!(fallback.trace().warnings.is_empty());
    assert!(fallback
        .trace()
        .decisions
        .iter()
        .any(|decision| decision.contains("explicit optimizer search directive")));
}

#[test]
fn cartesian_product_trace_reports_input_rows_and_costs() {
    let logical = LogicalPlan::NodeCartesianProduct {
        left: Box::new(LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "id".to_string(),
                value: Value::String("memory-42".to_string()),
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
        right: Box::new(LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: "s".to_string(),
                property: "id".to_string(),
                value: Value::String("source-42".to_string()),
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "s".to_string(),
                label: "Source".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [
                ("Memory".to_string(), "id".to_string()),
                ("Source".to_string(), "id".to_string()),
            ],
            [],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 10_000),
                ("Source".to_string(), 1_000),
            ],
            [],
            [],
            [],
            [],
            [
                (("Memory".to_string(), "id".to_string()), 10_000),
                (("Source".to_string(), "id".to_string()), 1_000),
            ],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(trace.decisions.iter().any(|decision| {
        decision.starts_with("keep NodeCartesianProduct single-row input order: inputs=2")
    }));
    assert!(trace.decisions.iter().any(|decision| {
            decision == "estimate NodeCartesianProduct: left_rows=1 right_rows=1 output_rows=1 left_cost=5 right_cost=5 cost=11"
        }));
    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 11,
        }
    );
}

#[test]
fn cartesian_product_orders_single_row_inputs_by_cost() {
    let logical = LogicalPlan::NodeCartesianProduct {
        left: Box::new(LogicalPlan::Filter {
            predicate: Predicate::And(vec![
                Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "id".to_string(),
                    value: Value::String("memory-42".to_string()),
                },
                Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "kind".to_string(),
                    value: Value::String("note".to_string()),
                },
            ]),
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        }),
        right: Box::new(LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: "s".to_string(),
                property: "id".to_string(),
                value: Value::String("source-42".to_string()),
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "s".to_string(),
                label: "Source".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [("Source".to_string(), "id".to_string())],
            [(
                "Memory".to_string(),
                vec!["id".to_string(), "kind".to_string()],
            )],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 10_000),
                ("Source".to_string(), 1_000),
            ],
            [],
            [],
            [],
            [],
            [
                (("Memory".to_string(), "id".to_string()), 10_000),
                (("Memory".to_string(), "kind".to_string()), 2),
                (("Source".to_string(), "id".to_string()), 1_000),
            ],
            [],
        ),
    );

    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(trace.decisions.iter().any(|decision| {
        decision.starts_with("order NodeCartesianProduct single-row inputs: inputs=2")
    }));
    assert!(trace.decisions.iter().any(|decision| {
            decision == "estimate NodeCartesianProduct: left_rows=1 right_rows=1 output_rows=1 left_cost=5 right_cost=8 cost=14"
        }));
    assert!(plan
        .instance_fingerprint()
        .contains("NodeCartesianProductExec(IndexNodeSeek(1:s:6:Source"));
    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 14,
        }
    );
}

#[test]
fn cartesian_product_orders_nested_single_row_inputs() {
    let logical = LogicalPlan::NodeCartesianProduct {
        left: Box::new(LogicalPlan::NodeCartesianProduct {
            left: Box::new(LogicalPlan::Filter {
                predicate: Predicate::And(vec![
                    Predicate::PropertyEq {
                        variable: "m".to_string(),
                        property: "id".to_string(),
                        value: Value::String("memory-42".to_string()),
                    },
                    Predicate::PropertyEq {
                        variable: "m".to_string(),
                        property: "kind".to_string(),
                        value: Value::String("note".to_string()),
                    },
                ]),
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
            right: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "s".to_string(),
                    property: "id".to_string(),
                    value: Value::String("source-42".to_string()),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "s".to_string(),
                    label: "Source".to_string(),
                }),
            }),
        }),
        right: Box::new(LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: "e".to_string(),
                property: "id".to_string(),
                value: Value::String("entity-42".to_string()),
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "e".to_string(),
                label: "Entity".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [
                ("Entity".to_string(), "id".to_string()),
                ("Source".to_string(), "id".to_string()),
            ],
            [(
                "Memory".to_string(),
                vec!["id".to_string(), "kind".to_string()],
            )],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [
                ("Entity".to_string(), 50_000),
                ("Memory".to_string(), 10_000),
                ("Source".to_string(), 1_000),
            ],
            [],
            [],
            [],
            [],
            [
                (("Entity".to_string(), "id".to_string()), 50_000),
                (("Memory".to_string(), "id".to_string()), 10_000),
                (("Memory".to_string(), "kind".to_string()), 2),
                (("Source".to_string(), "id".to_string()), 1_000),
            ],
            [],
        ),
    );

    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 32 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(trace.decisions.iter().any(|decision| {
        decision.starts_with("order NodeCartesianProduct single-row inputs: inputs=3")
    }));
    assert!(trace.decisions.iter().any(|decision| {
            decision == "estimate NodeCartesianProduct: left_rows=1 right_rows=1 output_rows=1 left_cost=5 right_cost=14 cost=20"
        }));
    let fingerprint = plan.instance_fingerprint();
    assert!(fingerprint.contains("NodeCartesianProductExec(IndexNodeSeek(1:e:6:Entity"));
    assert!(fingerprint.contains("IndexNodeSeek(1:s:6:Source"));
    assert!(fingerprint.contains("FilterExec(And(PropertyEq(1:m.2:id=string:9:memory-42)"));
    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 20,
        }
    );
}

#[test]
fn cartesian_product_keeps_nested_multi_row_inputs() {
    let logical = LogicalPlan::NodeCartesianProduct {
        left: Box::new(LogicalPlan::NodeCartesianProduct {
            left: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
            right: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "s".to_string(),
                    property: "id".to_string(),
                    value: Value::String("source-42".to_string()),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "s".to_string(),
                    label: "Source".to_string(),
                }),
            }),
        }),
        right: Box::new(LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: "e".to_string(),
                property: "id".to_string(),
                value: Value::String("entity-42".to_string()),
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "e".to_string(),
                label: "Entity".to_string(),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [
                ("Entity".to_string(), "id".to_string()),
                ("Source".to_string(), "id".to_string()),
            ],
            [],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [
                ("Entity".to_string(), 50_000),
                ("Memory".to_string(), 10),
                ("Source".to_string(), 1_000),
            ],
            [],
            [],
            [],
            [],
            [
                (("Entity".to_string(), "id".to_string()), 50_000),
                (("Source".to_string(), "id".to_string()), 1_000),
            ],
            [],
        ),
    );

    let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 32 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(trace.decisions.iter().any(|decision| {
            decision == "keep NodeCartesianProduct input order: left_rows=10 right_rows=1 reason=non_single_row_input"
        }));
    assert!(plan.instance_fingerprint().starts_with(
        "NodeCartesianProductExec(NodeCartesianProductExec(SeqNodeScan(1:m:6:Memory)"
    ));
    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 10,
            cost: 44,
        }
    );
}

#[test]
fn post_product_node_property_filter_uses_node_statistics() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "kind".to_string(),
            value: Value::String("note".to_string()),
        },
        input: Box::new(LogicalPlan::NodeCartesianProduct {
            left: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
            right: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "s".to_string(),
                    property: "id".to_string(),
                    value: Value::String("source-42".to_string()),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "s".to_string(),
                    label: "Source".to_string(),
                }),
            }),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Source".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000), ("Source".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [
                (("Memory".to_string(), "kind".to_string()), 10),
                (("Source".to_string(), "id".to_string()), 1_000),
            ],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 100,
            cost: 3_009,
        }
    );
    assert!(trace
        .decisions
        .iter()
        .any(|decision| decision == "selected physical plan cost: estimated_rows=100 cost=3009"));
}

#[test]
fn residual_node_property_in_uses_distinct_value_count() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyIn {
            variable: "m".to_string(),
            property: "id".to_string(),
            values: vec![Value::Int(1), Value::Int(2), Value::Int(2), Value::Int(3)],
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
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
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 30,
            cost: 2_004,
        }
    );
}

#[test]
fn empty_in_list_uses_empty_exec_with_cardinality_lower_bound() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyIn {
            variable: "m".to_string(),
            property: "id".to_string(),
            values: Vec::new(),
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "id".to_string()), 100)],
            [],
        ),
    );

    let optimizer = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 });
    let root = LogicalPlanRoot::new(logical);
    let mut selected_plans = Vec::new();
    for directive in [
        OptimizerSearchDirective::Memo,
        OptimizerSearchDirective::DirectFallback,
    ] {
        let output = optimizer
            .optimize_root_with_catalog_and_directive(&root, &catalog, directive)
            .expect("empty predicate should fit either optimizer path");
        let (plan, trace) = output.into_parts();
        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 1,
                cost: 0,
            }
        );
        assert_eq!(trace.selected_plan, "EmptyExec");
        assert_eq!(
            trace.selected_plan_operator_counts.get("EmptyExec"),
            Some(&1)
        );
        assert!(!trace
            .selected_plan_operator_counts
            .contains_key("SeqNodeScan"));
        assert!(trace.rule_events.iter().any(|event| {
            event.rule() == "transformation:replace_false_filter_with_empty_limit"
        }));
        assert_eq!(trace.stage_events[0].name(), "logical_rewrite");
        selected_plans.push(plan);
    }
    assert_eq!(selected_plans[0], selected_plans[1]);
}

#[test]
fn residual_node_string_predicate_uses_distinct_count_cap() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyContains {
            variable: "m".to_string(),
            property: "body".to_string(),
            value: "graph".to_string(),
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "body".to_string()), 100)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 250,
            cost: 2_004,
        }
    );
}

#[test]
fn residual_node_or_filter_uses_branch_selectivity() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::Or(vec![
            Predicate::ConstantBool(false),
            Predicate::PropertyEq {
                variable: "t".to_string(),
                property: "source".to_string(),
                value: Value::String("slack".to_string()),
            },
        ]),
        input: Box::new(LogicalPlan::NodeScan {
            variable: "t".to_string(),
            label: "Thread".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Thread".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Thread".to_string(), "source".to_string()), 10)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 100,
            cost: 2_004,
        }
    );
}

#[test]
fn residual_node_null_predicate_uses_conservative_selectivity() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyIsNotNull {
            variable: "c".to_string(),
            property: "ai_summary".to_string(),
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "c".to_string(),
            label: "Community".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Community".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Community".to_string(), "ai_summary".to_string()), 100)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 900,
            cost: 2_004,
        }
    );
}

#[test]
fn residual_node_not_eq_filter_uses_distinct_counts() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyNotEq {
            variable: "m".to_string(),
            property: "space_id".to_string(),
            value: Value::String("default".to_string()),
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "m".to_string(),
            label: "Memory".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "space_id".to_string()), 10)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 900,
            cost: 2_004,
        }
    );
}

#[test]
fn residual_relationship_id_in_uses_literal_list_width() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::IdIn {
            variable: "r".to_string(),
            values: vec![Value::Int(1), Value::Int(2), Value::Int(2)],
        },
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: Some("r".to_string()),
            rel_type: "MENTIONS".to_string(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Outgoing,
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
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000), ("Entity".to_string(), 1_000)],
            [("MENTIONS".to_string(), 4_000)],
            [("MENTIONS".to_string(), 1_000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                4_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                4_000,
            )],
            [],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 2,
            cost: 15_004,
        }
    );
}

#[test]
fn residual_relationship_property_in_uses_relationship_distinct_counts() {
    let logical = LogicalPlan::Filter {
        predicate: Predicate::PropertyIn {
            variable: "r".to_string(),
            property: "kind".to_string(),
            values: vec![
                Value::String("mentioned".to_string()),
                Value::String("quoted".to_string()),
                Value::String("quoted".to_string()),
                Value::String("linked".to_string()),
            ],
        },
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: Some("r".to_string()),
            rel_type: "MENTIONS".to_string(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Outgoing,
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
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000), ("Entity".to_string(), 1_000)],
            [("MENTIONS".to_string(), 4_000)],
            [("MENTIONS".to_string(), 1_000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                4_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                4_000,
            )],
            [],
            [],
        )
        .with_relationship_property_distinct_counts([(
            ("MENTIONS".to_string(), "kind".to_string()),
            10,
        )]),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1_200,
            cost: 15_004,
        }
    );
}
