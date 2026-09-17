use super::super::{
    CascadesOptimizer, OptimizerCatalog, OptimizerCatalogIndexes, OptimizerCatalogStatistics,
};
use crate::{OptimizerConfig, PlanCost, RuleOutcome};
use skein_core::Value;
use skein_cypher::RelationshipDirection;
use skein_plan::{LogicalPlan, Predicate, Projection, ProjectionExpression, RelationshipCountLeg};
use std::collections::BTreeMap;

#[test]
fn optional_relationship_count_sum_uses_seed_and_relationship_statistics() {
    let logical = LogicalPlan::OptionalRelationshipCountSum {
        variable: "t".to_string(),
        label: "Thread".to_string(),
        properties: BTreeMap::from([("id".to_string(), Value::String("thread-42".to_string()))]),
        legs: vec![RelationshipCountLeg {
            rel_type: "CONTAINS".to_string(),
            direction: RelationshipDirection::Outgoing,
            distinct: false,
            filter: None,
        }],
        output: "message_count".to_string(),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Thread".to_string(), 1_000),
                ("Message".to_string(), 50_000),
            ],
            [("CONTAINS".to_string(), 5_000)],
            [("CONTAINS".to_string(), 1_000)],
            [],
            [],
            [(("Thread".to_string(), "id".to_string()), 1_000)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 11,
        }
    );
    assert!(trace.decisions.iter().any(|decision| {
            decision.contains(
                "estimate OptionalRelationshipCountSum for Thread: seed_rows=1 leg_rows=[CONTAINS:out:5] estimated_rows=1 cost=11",
            )
        }));
}

#[test]
fn incoming_optional_relationship_count_sum_uses_target_statistics() {
    let logical = LogicalPlan::OptionalRelationshipCountSum {
        variable: "e".to_string(),
        label: "Entity".to_string(),
        properties: BTreeMap::from([("id".to_string(), Value::String("entity-42".to_string()))]),
        legs: vec![RelationshipCountLeg {
            rel_type: "MENTIONS".to_string(),
            direction: RelationshipDirection::Incoming,
            distinct: false,
            filter: None,
        }],
        output: "mention_count".to_string(),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 10_000),
                ("Entity".to_string(), 1_000),
            ],
            [("MENTIONS".to_string(), 5_000)],
            [("MENTIONS".to_string(), 5_000)],
            [],
            [],
            [(("Entity".to_string(), "id".to_string()), 1_000)],
            [],
        )
        .with_relationship_type_target_counts([("MENTIONS".to_string(), 500)]),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 16,
        }
    );
    assert!(trace.decisions.iter().any(|decision| {
            decision.contains(
                "estimate OptionalRelationshipCountSum for Entity: seed_rows=1 leg_rows=[MENTIONS:in:10] estimated_rows=1 cost=16",
            )
        }));
}

#[test]
fn incoming_optional_degree_cost_uses_target_statistics() {
    let logical = LogicalPlan::OptionalDegree {
        source_variable: "e".to_string(),
        rel_type: "MENTIONS".to_string(),
        rel_properties: BTreeMap::new(),
        direction: RelationshipDirection::Incoming,
        target_label: "Memory".to_string(),
        target_properties: BTreeMap::new(),
        alias: "mention_count".to_string(),
        input: Box::new(LogicalPlan::NodeScan {
            variable: "e".to_string(),
            label: "Entity".to_string(),
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 10_000),
                ("Entity".to_string(), 1_000),
            ],
            [("MENTIONS".to_string(), 5_000)],
            [("MENTIONS".to_string(), 5_000)],
            [],
            [],
            [],
            [],
        )
        .with_relationship_type_target_counts([("MENTIONS".to_string(), 500)]),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1_000,
            cost: 23_004,
        }
    );
}

#[test]
fn expand_trace_marks_fallback_hop_estimates() {
    let logical = LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "e".to_string(),
                property: "name".to_string(),
            },
            name: "name".to_string(),
        }],
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: None,
            rel_type: "LINKS".to_string(),
            rel_properties: Default::default(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "e".to_string(),
            target_label: "Entity".to_string(),
            min_hops: 1,
            max_hops: 3,
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
            [("Memory".to_string(), 10), ("Entity".to_string(), 100)],
            [("LINKS".to_string(), 20)],
            [("LINKS".to_string(), 10)],
            [(
                (
                    "Memory".to_string(),
                    "LINKS".to_string(),
                    "Entity".to_string(),
                ),
                4,
            )],
            [],
            [],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(trace.decisions.iter().any(|decision| {
        decision.contains("estimate AdjacencyExpand")
            && decision.contains("hop_rows=[1:fallback:4,2:fallback:8,3:fallback:16]")
            && decision.contains("estimated_rows=28")
    }));
    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 28,
            cost: 118,
        }
    );
    assert!(trace
        .decisions
        .iter()
        .any(|decision| decision == "selected physical plan cost: estimated_rows=28 cost=118"));
}

#[test]
fn expand_cost_scales_with_selective_input_rows() {
    let logical = LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "e".to_string(),
                property: "name".to_string(),
            },
            name: "name".to_string(),
        }],
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: None,
            rel_type: "LINKS".to_string(),
            rel_properties: Default::default(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "e".to_string(),
            target_label: "Entity".to_string(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "id".to_string(),
                    value: Value::Int(7),
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
            [("Memory".to_string(), 1000), ("Entity".to_string(), 1000)],
            [("LINKS".to_string(), 1000)],
            [("LINKS".to_string(), 1000)],
            [(
                (
                    "Memory".to_string(),
                    "LINKS".to_string(),
                    "Entity".to_string(),
                ),
                1000,
            )],
            [],
            [(("Memory".to_string(), "id".to_string()), 1000)],
            [],
        ),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 10,
        }
    );
    assert!(trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeSeek for Memory.id")));
    assert!(trace.decisions.iter().any(|decision| {
        decision.starts_with("apply implementation:node_equality_index_seek:")
    }));
    assert!(trace.rule_events.iter().any(|event| {
        event.rule() == "implementation:node_equality_index_seek"
            && event.outcome() == RuleOutcome::Applied
    }));
    assert!(trace.stage_events.iter().any(|event| {
        event.name() == "access_path_selection" && event.stats().applied_rules == 1
    }));
    assert!(trace
        .stage_events
        .iter()
        .any(|event| { event.name() == "physical_search" && event.stats().applied_rules >= 1 }));
    assert!(trace
        .decisions
        .iter()
        .any(|decision| decision == "selected physical plan cost: estimated_rows=1 cost=10"));
}

#[test]
fn expand_cost_uses_relationship_property_distinct_counts() {
    let logical = LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "r".to_string(),
                property: "weight".to_string(),
            },
            name: "weight".to_string(),
        }],
        input: Box::new(LogicalPlan::Expand {
            source_variable: "m".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: Some("r".to_string()),
            rel_type: "MENTIONS".to_string(),
            rel_properties: BTreeMap::from([("weight".to_string(), Value::Int(4))]),
            direction: RelationshipDirection::Outgoing,
            target_variable: "e".to_string(),
            target_label: "Entity".to_string(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: Box::new(LogicalPlan::Filter {
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
        }),
    };
    let catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1000), ("Entity".to_string(), 1000)],
            [("MENTIONS".to_string(), 1000)],
            [("MENTIONS".to_string(), 1000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                1000,
            )],
            [],
            [(("Memory".to_string(), "id".to_string()), 1000)],
            [],
        )
        .with_relationship_property_distinct_counts([(
            ("MENTIONS".to_string(), "weight".to_string()),
            10,
        )]),
    );

    let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &catalog);

    assert!(trace.decisions.iter().any(|decision| {
        decision.contains("estimate AdjacencyExpand")
            && decision.contains("rel_property_distinct_product=10")
            && decision.contains("estimated_rows=100")
    }));
    assert_eq!(
        trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 10,
        }
    );
}
