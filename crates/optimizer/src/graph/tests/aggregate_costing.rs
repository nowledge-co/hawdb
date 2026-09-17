use super::super::{
    CascadesOptimizer, OptimizerCatalog, OptimizerCatalogIndexes, OptimizerCatalogStatistics,
};
use crate::{OptimizerConfig, PlanCost};
use skein_cypher::RelationshipDirection;
use skein_plan::{
    AggregateFunction, AggregateTarget, Aggregation, LogicalPlan, Projection, ProjectionExpression,
};
use std::collections::BTreeMap;

#[test]
fn aggregate_group_keys_use_node_property_distinct_counts() {
    let logical = LogicalPlan::Aggregate {
        group_keys: vec![
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "e".to_string(),
                    property: "community_id".to_string(),
                },
                name: "community_id".to_string(),
            },
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "l".to_string(),
                    property: "name".to_string(),
                },
                name: "label_name".to_string(),
            },
        ],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Variable("m".to_string()),
            distinct: true,
            name: "memory_count".to_string(),
        }],
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: None,
            rel_type: "HAS_LABEL".to_string(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "l".to_string(),
            target_label: "Label".to_string(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: Box::new(LogicalPlan::Expand {
                source_variable: "m".to_string(),
                source_label: "Memory".to_string(),
                rel_variable: None,
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
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 1_000),
                ("Entity".to_string(), 500),
                ("Label".to_string(), 100),
            ],
            [
                ("MENTIONS".to_string(), 2_000),
                ("HAS_LABEL".to_string(), 1_000),
            ],
            [
                ("MENTIONS".to_string(), 1_000),
                ("HAS_LABEL".to_string(), 1_000),
            ],
            [
                (
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                    ),
                    1_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "HAS_LABEL".to_string(),
                        "Label".to_string(),
                    ),
                    1_000,
                ),
            ],
            [
                (
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                        1,
                    ),
                    1_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "HAS_LABEL".to_string(),
                        "Label".to_string(),
                        1,
                    ),
                    1_000,
                ),
            ],
            [
                (("Entity".to_string(), "community_id".to_string()), 4),
                (("Label".to_string(), "name".to_string()), 5),
            ],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 32 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 20,
            cost: 11_004,
        }
    );
}

#[test]
fn aggregate_group_keys_use_relationship_property_distinct_counts() {
    let logical = LogicalPlan::Aggregate {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "r".to_string(),
                property: "weight".to_string(),
            },
            name: "weight".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Variable("e".to_string()),
            distinct: true,
            name: "entity_count".to_string(),
        }],
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
            [("MENTIONS".to_string(), 1_000)],
            [("MENTIONS".to_string(), 1_000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                1_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                1_000,
            )],
            [],
            [],
        )
        .with_relationship_property_distinct_counts([(
            ("MENTIONS".to_string(), "weight".to_string()),
            10,
        )]),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 10,
            cost: 7_004,
        }
    );
}

#[test]
fn aggregate_distinct_property_targets_add_bounded_work_cost() {
    let logical = LogicalPlan::Aggregate {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "unit_type".to_string(),
            },
            name: "unit_type".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Property {
                variable: "m".to_string(),
                property: "community_id".to_string(),
            },
            distinct: true,
            name: "community_span".to_string(),
        }],
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
            [
                (("Memory".to_string(), "unit_type".to_string()), 5),
                (("Memory".to_string(), "community_id".to_string()), 10),
            ],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 8 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 5,
            cost: 2_014,
        }
    );
}

#[test]
fn aggregate_distinct_variable_targets_add_bounded_work_cost() {
    let logical = LogicalPlan::Aggregate {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "unit_type".to_string(),
            },
            name: "unit_type".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Variable("m".to_string()),
            distinct: true,
            name: "memory_count".to_string(),
        }],
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
            [(("Memory".to_string(), "unit_type".to_string()), 5)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 8 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 5,
            cost: 3_004,
        }
    );
}

#[test]
fn aggregate_distinct_variable_targets_use_path_target_coverage() {
    let logical = LogicalPlan::Aggregate {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "unit_type".to_string(),
            },
            name: "unit_type".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Variable("e".to_string()),
            distinct: true,
            name: "entity_count".to_string(),
        }],
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: None,
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
            [("Memory".to_string(), 1_000), ("Entity".to_string(), 5_000)],
            [("MENTIONS".to_string(), 1_000)],
            [("MENTIONS".to_string(), 1_000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                1_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                1_000,
            )],
            [(("Memory".to_string(), "unit_type".to_string()), 5)],
            [],
        )
        .with_path_target_distinct_counts([(
            (
                "Memory".to_string(),
                "MENTIONS".to_string(),
                "Entity".to_string(),
            ),
            12,
        )]),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 5,
            cost: 6_016,
        }
    );
}

#[test]
fn aggregate_distinct_variable_targets_use_bounded_path_target_coverage() {
    let logical = LogicalPlan::Aggregate {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "unit_type".to_string(),
            },
            name: "unit_type".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Variable("e".to_string()),
            distinct: true,
            name: "two_hop_entities".to_string(),
        }],
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: None,
            rel_type: "RELATES_TO".to_string(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "e".to_string(),
            target_label: "Entity".to_string(),
            min_hops: 2,
            max_hops: 2,
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
            [("Memory".to_string(), 1_000), ("Entity".to_string(), 5_000)],
            [("RELATES_TO".to_string(), 10_000)],
            [("RELATES_TO".to_string(), 1_000)],
            [(
                (
                    "Memory".to_string(),
                    "RELATES_TO".to_string(),
                    "Entity".to_string(),
                ),
                2_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "RELATES_TO".to_string(),
                    "Entity".to_string(),
                    2,
                ),
                1_500,
            )],
            [(("Memory".to_string(), "unit_type".to_string()), 5)],
            [],
        )
        .with_bounded_path_target_distinct_counts([(
            (
                "Memory".to_string(),
                "RELATES_TO".to_string(),
                "Entity".to_string(),
                2,
            ),
            40,
        )]),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 5,
            cost: 7_544,
        }
    );
}
