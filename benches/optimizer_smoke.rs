use serde_json::json;
use skein::optimizer::{
    CascadesOptimizer, OptimizerCatalog, OptimizerCatalogIndexes, OptimizerCatalogStatistics,
    OptimizerConfig, PlanCost,
};
use skein::planner::{
    AggregateFunction, AggregateTarget, Aggregation, ComparisonOp, LogicalPlan, PhysicalPlan,
    Predicate, Projection, ProjectionExpression, RelationshipCountLeg, SortDirection, SortItem,
    SortKey,
};
use skein::RelationshipDirection;
use skein::Value;
use std::collections::BTreeMap;
use std::time::Instant;

const ITERATIONS: usize = 2_000;

fn main() {
    let cases = optimizer_smoke_cases();
    let optimizer = CascadesOptimizer::new(OptimizerConfig { max_groups: 128 });

    let expected = cases
        .iter()
        .map(|case| {
            let (plan, trace) = optimizer.optimize_with_catalog(&case.logical, &case.catalog);
            assert_trace(case, &plan, &trace);
            trace.selected_plan_fingerprint
        })
        .collect::<Vec<_>>();
    let summary_reports = cases
        .iter()
        .zip(expected.iter())
        .map(|(case, expected_fingerprint)| {
            let (plan, trace) = optimizer.optimize_with_catalog(&case.logical, &case.catalog);
            assert_trace(case, &plan, &trace);
            assert_eq!(&trace.selected_plan_fingerprint, expected_fingerprint);
            OptimizerSmokeCaseReport::new(case, &trace)
        })
        .collect::<Vec<_>>();

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        for (case, expected_fingerprint) in cases.iter().zip(expected.iter()) {
            let (plan, trace) = optimizer.optimize_with_catalog(&case.logical, &case.catalog);
            assert_trace(case, &plan, &trace);
            assert_eq!(&trace.selected_plan_fingerprint, expected_fingerprint);
        }
    }
    let elapsed = start.elapsed();
    println!(
        "optimizer_smoke cases={} iterations={ITERATIONS} elapsed_ms={} fingerprints={}",
        cases.len(),
        elapsed.as_millis(),
        expected.join("|")
    );
    println!(
        "optimizer_smoke_summaries {}",
        summary_reports
            .iter()
            .map(OptimizerSmokeCaseReport::text)
            .collect::<Vec<_>>()
            .join(" | ")
    );
    println!(
        "optimizer_smoke_summaries_json {}",
        serde_json::Value::Array(
            summary_reports
                .iter()
                .map(OptimizerSmokeCaseReport::json)
                .collect::<Vec<_>>()
        )
    );

    let budgeted = CascadesOptimizer::new(OptimizerConfig { max_groups: 2 });
    let (_, budgeted_trace) = budgeted.optimize_with_catalog(&cases[0].logical, &cases[0].catalog);
    assert!(budgeted_trace.warnings.is_empty());
    assert!(budgeted_trace.groups > 2);
    assert_eq!(budgeted_trace.selected_plan_cost, cases[0].expected_cost);
}

struct OptimizerSmokeCaseReport<'a> {
    case: &'a str,
    groups: usize,
    rows: u64,
    cost: u64,
    operator_counts: BTreeMap<String, usize>,
    class_counts: BTreeMap<String, usize>,
    fingerprint: String,
}

impl<'a> OptimizerSmokeCaseReport<'a> {
    fn new(case: &'a OptimizerSmokeCase, trace: &skein::optimizer::OptimizerTrace) -> Self {
        Self {
            case: case.name,
            groups: trace.groups,
            rows: trace.selected_plan_cost.estimated_rows,
            cost: trace.selected_plan_cost.cost,
            operator_counts: trace.selected_plan_operator_counts.clone(),
            class_counts: trace.selected_plan_class_counts.clone(),
            fingerprint: stable_fingerprint_summary(&trace.selected_plan_fingerprint),
        }
    }

    fn text(&self) -> String {
        format!(
            "case={} groups={} rows={} cost={} operators={} classes={} fingerprint={}",
            self.case,
            self.groups,
            self.rows,
            self.cost,
            stable_counts(&self.operator_counts),
            stable_counts(&self.class_counts),
            self.fingerprint
        )
    }

    fn json(&self) -> serde_json::Value {
        json!({
            "case": self.case,
            "groups": self.groups,
            "rows": self.rows,
            "cost": self.cost,
            "operator_counts": self.operator_counts,
            "class_counts": self.class_counts,
            "fingerprint": self.fingerprint,
        })
    }
}

fn stable_counts(counts: &BTreeMap<String, usize>) -> String {
    counts
        .iter()
        .map(|(name, count)| format!("{name}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn stable_fingerprint_summary(fingerprint: &str) -> String {
    const LIMIT: usize = 96;
    if fingerprint.len() <= LIMIT {
        fingerprint.to_string()
    } else {
        format!("{}...", &fingerprint[..LIMIT])
    }
}

struct OptimizerSmokeCase {
    name: &'static str,
    logical: LogicalPlan,
    catalog: OptimizerCatalog,
    expected_cost: PlanCost,
    instance_fingerprint_contains: &'static str,
    decision_contains: &'static [&'static str],
}

fn assert_trace(
    case: &OptimizerSmokeCase,
    plan: &PhysicalPlan,
    trace: &skein::optimizer::OptimizerTrace,
) {
    assert!(
        trace.warnings.is_empty(),
        "{} unexpectedly warned: {:?}",
        case.name,
        trace.warnings
    );
    assert_eq!(
        trace.selected_plan_cost, case.expected_cost,
        "{} selected cost changed",
        case.name
    );
    assert_eq!(
        trace.selected_plan_fingerprint,
        plan.fingerprint(),
        "{} trace fingerprint diverged from the physical plan shape",
        case.name
    );
    let instance_fingerprint = plan.instance_fingerprint();
    assert!(
        instance_fingerprint.contains(case.instance_fingerprint_contains),
        "{} instance fingerprint did not contain {}: {}",
        case.name,
        case.instance_fingerprint_contains,
        instance_fingerprint
    );
    for expected in case.decision_contains {
        assert!(
            trace
                .decisions
                .iter()
                .any(|decision| decision.contains(expected)),
            "{} missing decision containing {}: {:?}",
            case.name,
            expected,
            trace.decisions
        );
    }
}

fn optimizer_smoke_cases() -> Vec<OptimizerSmokeCase> {
    vec![
        OptimizerSmokeCase {
            name: "range_expand",
            logical: range_expand_plan(),
            catalog: range_expand_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 13,
                cost: 428,
            },
            instance_fingerprint_contains: "IndexNodeRangeSeek",
            decision_contains: &[
                "choose IndexNodeRangeSeek",
                "estimate AdjacencyExpand",
                "estimated_rows=125",
                "selected physical plan cost: estimated_rows=13 cost=428",
            ],
        },
        OptimizerSmokeCase {
            name: "composite_seek",
            logical: composite_seek_plan(),
            catalog: composite_seek_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 5,
            },
            instance_fingerprint_contains: "access=composite_equality",
            decision_contains: &[
                "choose IndexNodeCompositeSeek",
                "selected physical plan cost: estimated_rows=1 cost=5",
            ],
        },
        OptimizerSmokeCase {
            name: "text_seek",
            logical: text_seek_plan(),
            catalog: text_seek_catalog(),
            expected_cost: PlanCost {
                // Projection preserves all 1,000 / 4 full-text candidates.
                // Seek I/O is 3 + 2 * 250; fused filter/projection adds 250 CPU.
                estimated_rows: 250,
                cost: 753,
            },
            instance_fingerprint_contains: "access=full_text",
            decision_contains: &[
                "choose IndexNodeTextSeek",
                "selected physical plan cost: estimated_rows=250 cost=753",
            ],
        },
        OptimizerSmokeCase {
            name: "residual_node_string_contains",
            logical: residual_node_string_contains_plan(),
            catalog: residual_node_string_contains_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 250,
                cost: 1254,
            },
            instance_fingerprint_contains: "PropertyContains",
            decision_contains: &["selected physical plan cost: estimated_rows=250 cost=1254"],
        },
        OptimizerSmokeCase {
            name: "low_selectivity_scan",
            logical: low_selectivity_scan_plan(),
            catalog: low_selectivity_scan_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1000,
                cost: 2004,
            },
            instance_fingerprint_contains: "access=label_scan",
            decision_contains: &[
                "choose SeqNodeScan",
                "selected physical plan cost: estimated_rows=1000 cost=2004",
            ],
        },
        OptimizerSmokeCase {
            name: "memory_seed_entity_mentions",
            logical: memory_seed_entity_mentions_plan(),
            catalog: memory_seed_entity_mentions_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 18,
            },
            instance_fingerprint_contains: "IndexNodeSeek",
            decision_contains: &[
                "choose IndexNodeSeek for Memory.id",
                "estimate AdjacencyExpand",
                "choose TopN for bounded sort",
                "selected physical plan cost: estimated_rows=1 cost=18",
            ],
        },
        OptimizerSmokeCase {
            name: "relationship_property_expand",
            logical: relationship_property_expand_plan(),
            catalog: relationship_property_expand_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 6,
            },
            instance_fingerprint_contains: "AdjacencyExpandExec",
            decision_contains: &[
                "choose IndexNodeSeek for Memory.id",
                "rel_property_distinct_product=10",
                "selected physical plan cost: estimated_rows=1 cost=6",
            ],
        },
        OptimizerSmokeCase {
            name: "relationship_status_range_filter",
            logical: relationship_status_range_filter_plan(),
            catalog: relationship_status_range_filter_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 9,
            },
            instance_fingerprint_contains: "FilterExec",
            decision_contains: &[
                "choose IndexNodeSeek for Memory.id",
                "rel_property_distinct_product=2",
                "selected physical plan cost: estimated_rows=1 cost=9",
            ],
        },
        OptimizerSmokeCase {
            name: "relationship_property_in_filter",
            logical: relationship_property_in_filter_plan(),
            catalog: relationship_property_in_filter_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1_200,
                cost: 10_004,
            },
            instance_fingerprint_contains: "PropertyIn",
            decision_contains: &[
                "estimate AdjacencyExpand for Memory-[:MENTIONS*1..1]->Entity",
                "selected physical plan cost: estimated_rows=1200 cost=10004",
            ],
        },
        OptimizerSmokeCase {
            name: "relationship_id_in_filter",
            logical: relationship_id_in_filter_plan(),
            catalog: relationship_id_in_filter_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 2,
                cost: 10_004,
            },
            instance_fingerprint_contains: "IdIn",
            decision_contains: &[
                "estimate AdjacencyExpand for Memory-[:MENTIONS*1..1]->Entity",
                "selected physical plan cost: estimated_rows=2 cost=10004",
            ],
        },
        OptimizerSmokeCase {
            name: "source_memory_label_cross_pattern",
            logical: source_memory_label_cross_pattern_plan(),
            catalog: source_memory_label_cross_pattern_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 10,
                cost: 1884,
            },
            instance_fingerprint_contains: "AdjacencyExpandExec",
            decision_contains: &[
                "choose IndexNodeSeek for Source.id",
                "estimate AdjacencyExpand for Source-[:SOURCED_FROM*1..1]->Memory",
                "estimate AdjacencyExpand for Memory-[:HAS_LABEL*1..1]->Label",
                "keep Sort + Limit for bounded sort",
                "selected physical plan cost: estimated_rows=10 cost=1884",
            ],
        },
        OptimizerSmokeCase {
            name: "source_memory_entity_label_workload",
            logical: source_memory_entity_label_workload_plan(),
            catalog: source_memory_entity_label_workload_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 25,
                cost: 3088,
            },
            instance_fingerprint_contains: "TopNExec",
            decision_contains: &[
                "choose IndexNodeSeek for Source.id",
                "estimate AdjacencyExpand for Source-[:SOURCED_FROM*1..1]->Memory",
                "estimate AdjacencyExpand for Memory-[:MENTIONS*1..1]->Entity",
                "estimate AdjacencyExpand for Memory-[:HAS_LABEL*1..1]->Label",
                "choose TopN for bounded sort",
                "selected physical plan cost: estimated_rows=25 cost=3088",
            ],
        },
        OptimizerSmokeCase {
            name: "source_entity_community_export_workload",
            logical: source_entity_community_export_workload_plan(),
            catalog: source_entity_community_export_workload_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 10,
                cost: 986,
            },
            instance_fingerprint_contains: "TopNExec",
            decision_contains: &[
                "choose IndexNodeSeek for Source.id",
                "estimate AdjacencyExpand for Source-[:SOURCED_FROM*1..1]->Memory",
                "estimate AdjacencyExpand for Memory-[:MENTIONS*1..1]->Entity",
                "choose TopN for bounded sort",
                "selected physical plan cost: estimated_rows=10 cost=986",
            ],
        },
        OptimizerSmokeCase {
            name: "community_synthesized_source_coverage",
            logical: community_synthesized_source_coverage_plan(),
            catalog: community_synthesized_source_coverage_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 24,
            },
            instance_fingerprint_contains: "AggregateExec",
            decision_contains: &[
                "choose IndexNodeSeek for Community.id",
                "estimate AdjacencyExpand for Community-[:SYNTHESIZED_FROM*1..1]->Source",
                "selected physical plan cost: estimated_rows=1 cost=24",
            ],
        },
        OptimizerSmokeCase {
            name: "source_coverage_aggregate_filter",
            logical: source_coverage_aggregate_filter_plan(),
            catalog: community_synthesized_source_coverage_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 32,
            },
            instance_fingerprint_contains: "ExpressionEq(column(7:covered)=int:3)",
            decision_contains: &[
                "choose IndexNodeSeek for Community.id",
                "estimate AdjacencyExpand for Community-[:SYNTHESIZED_FROM*1..1]->Source",
                "selected physical plan cost: estimated_rows=1 cost=32",
            ],
        },
        OptimizerSmokeCase {
            name: "feed_synthesized_source_collect",
            logical: feed_synthesized_source_collect_plan(),
            catalog: feed_synthesized_source_collect_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 11,
            },
            instance_fingerprint_contains: "collect(distinct 1:s.2:id)",
            decision_contains: &[
                "choose IndexNodeMultiSeek for Memory.id",
                "estimate AdjacencyExpand for Memory-[:SYNTHESIZED_FROM*1..1]->Memory",
                "selected physical plan cost: estimated_rows=1 cost=11",
            ],
        },
        OptimizerSmokeCase {
            name: "skill_thread_source_provenance_workload",
            logical: skill_thread_source_provenance_workload_plan(),
            catalog: skill_thread_source_provenance_workload_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 2,
                cost: 42,
            },
            instance_fingerprint_contains: "COMPACTS_TO",
            decision_contains: &[
                "choose IndexNodeSeek for Skill.id",
                "estimate AdjacencyExpand for Skill-[:SYNTHESIZED_FROM*1..1]->Memory",
                "estimate AdjacencyExpand for Memory-[:COMPACTS_TO*1..1]->Thread",
                "choose TopN for bounded sort",
                "selected physical plan cost: estimated_rows=2 cost=42",
            ],
        },
        OptimizerSmokeCase {
            name: "community_member_memory_evidence_workload",
            logical: community_member_memory_evidence_workload_plan(),
            catalog: community_member_memory_evidence_workload_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 11,
            },
            instance_fingerprint_contains: "BELONGS_TO",
            decision_contains: &[
                "choose IndexNodeSeek for Community.id",
                "estimate AdjacencyExpand for Community-[:BELONGS_TO*1..1]->Entity",
                "estimate AdjacencyExpand for Entity-[:MENTIONS*1..1]->Memory",
                "choose TopN for bounded sort",
                "selected physical plan cost: estimated_rows=1 cost=11",
            ],
        },
        OptimizerSmokeCase {
            name: "entity_bridge_span_aggregate",
            logical: entity_bridge_span_aggregate_plan(),
            catalog: entity_bridge_span_aggregate_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 10,
                cost: 25_834,
            },
            instance_fingerprint_contains: "count(distinct 2:e2.12:community_id)",
            decision_contains: &[
                "estimate AdjacencyExpand for Entity-[:RELATES_TO*1..1]->Entity",
                "choose TopN for bounded sort",
                "selected physical plan cost: estimated_rows=10 cost=25834",
            ],
        },
        OptimizerSmokeCase {
            name: "bounded_path_distinct_target_aggregate",
            logical: bounded_path_distinct_target_aggregate_plan(),
            catalog: bounded_path_distinct_target_aggregate_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 5,
                cost: 5_044,
            },
            instance_fingerprint_contains: "count(distinct 1:e)",
            decision_contains: &[
                "estimate AdjacencyExpand for Memory-[:RELATES_TO*2..2]->Entity",
                "selected physical plan cost: estimated_rows=5 cost=5044",
            ],
        },
        OptimizerSmokeCase {
            name: "thread_cleanup_optional_count",
            logical: thread_cleanup_optional_count_plan(),
            catalog: thread_cleanup_optional_count_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 11,
            },
            instance_fingerprint_contains: "OptionalRelationshipCountSumExec",
            decision_contains: &[
                "estimate OptionalRelationshipCountSum for Thread: seed_rows=1 leg_rows=[CONTAINS:out:5] estimated_rows=1 cost=11",
                "selected physical plan cost: estimated_rows=1 cost=11",
            ],
        },
        OptimizerSmokeCase {
            name: "entity_incoming_mention_optional_count",
            logical: entity_incoming_mention_optional_count_plan(),
            catalog: entity_incoming_mention_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 16,
            },
            instance_fingerprint_contains: "OptionalRelationshipCountSumExec",
            decision_contains: &[
                "estimate OptionalRelationshipCountSum for Entity: seed_rows=1 leg_rows=[MENTIONS:in:10] estimated_rows=1 cost=16",
                "selected physical plan cost: estimated_rows=1 cost=16",
            ],
        },
        OptimizerSmokeCase {
            name: "entity_incoming_mention_optional_degree",
            logical: entity_incoming_mention_optional_degree_plan(),
            catalog: entity_incoming_mention_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1_000,
                cost: 12_004,
            },
            instance_fingerprint_contains: "OptionalDegreeExec",
            decision_contains: &["selected physical plan cost: estimated_rows=1000 cost=12004"],
        },
        OptimizerSmokeCase {
            name: "endpoint_existence_cartesian_product",
            logical: endpoint_existence_cartesian_product_plan(),
            catalog: endpoint_existence_cartesian_product_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 8,
            },
            instance_fingerprint_contains: "NodeCartesianProductExec",
            decision_contains: &[
                "choose IndexNodeSeek for Memory.id",
                "choose IndexNodeSeek for Source.id",
                "estimate NodeCartesianProduct: left_rows=1 right_rows=1 output_rows=1 left_cost=3 right_cost=3 cost=7",
                "selected physical plan cost: estimated_rows=1 cost=8",
            ],
        },
        OptimizerSmokeCase {
            name: "nested_endpoint_existence_cartesian_product",
            logical: nested_endpoint_existence_cartesian_product_plan(),
            catalog: nested_endpoint_existence_cartesian_product_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 1,
                cost: 14,
            },
            instance_fingerprint_contains: "NodeCartesianProductExec(IndexNodeSeek(1:e:6:Entity",
            decision_contains: &[
                "order NodeCartesianProduct single-row inputs: inputs=3",
                "estimate NodeCartesianProduct: left_rows=1 right_rows=1 output_rows=1 left_cost=3 right_cost=9 cost=13",
                "selected physical plan cost: estimated_rows=1 cost=14",
            ],
        },
        OptimizerSmokeCase {
            name: "post_product_node_property_filter",
            logical: post_product_node_property_filter_plan(),
            catalog: post_product_node_property_filter_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 100,
                cost: 3007,
            },
            instance_fingerprint_contains: "FilterExec",
            decision_contains: &[
                "choose IndexNodeSeek for Source.id",
                "keep NodeCartesianProduct input order: left_rows=1000 right_rows=1 reason=non_single_row_input",
                "selected physical plan cost: estimated_rows=100 cost=3007",
            ],
        },
        OptimizerSmokeCase {
            name: "residual_node_property_in",
            logical: residual_node_property_in_plan(),
            catalog: residual_node_property_in_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 30,
                cost: 2004,
            },
            instance_fingerprint_contains: "PropertyIn",
            decision_contains: &["selected physical plan cost: estimated_rows=30 cost=2004"],
        },
        OptimizerSmokeCase {
            name: "thread_optional_source_filter",
            logical: thread_optional_source_filter_plan(),
            catalog: thread_optional_source_filter_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 100,
                cost: 2004,
            },
            instance_fingerprint_contains: "PropertyEq",
            decision_contains: &["selected physical plan cost: estimated_rows=100 cost=2004"],
        },
        OptimizerSmokeCase {
            name: "community_summary_presence_filter",
            logical: community_summary_presence_filter_plan(),
            catalog: community_summary_presence_filter_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 900,
                cost: 2004,
            },
            instance_fingerprint_contains: "PropertyIsNotNull",
            decision_contains: &["selected physical plan cost: estimated_rows=900 cost=2004"],
        },
        OptimizerSmokeCase {
            name: "normalized_space_exclusion_filter",
            logical: normalized_space_exclusion_filter_plan(),
            catalog: normalized_space_exclusion_filter_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 900,
                cost: 2004,
            },
            instance_fingerprint_contains: "PropertyNotEq",
            decision_contains: &["selected physical plan cost: estimated_rows=900 cost=2004"],
        },
        OptimizerSmokeCase {
            name: "thread_candidate_normalized_space_multi_seek",
            logical: thread_candidate_normalized_space_multi_seek_plan(),
            catalog: thread_candidate_normalized_space_multi_seek_catalog(),
            expected_cost: PlanCost {
                estimated_rows: 2,
                cost: 12,
            },
            instance_fingerprint_contains: "IndexNodeMultiSeek",
            decision_contains: &[
                "choose IndexNodeMultiSeek for Thread.thread_id",
                "selected physical plan cost: estimated_rows=2 cost=12",
            ],
        },
    ]
}

fn range_expand_plan() -> LogicalPlan {
    LogicalPlan::Project {
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
            min_hops: 2,
            max_hops: 2,
            optional: false,
            input: Box::new(LogicalPlan::Filter {
                predicate: Predicate::And(vec![
                    Predicate::PropertyCompare {
                        variable: "m".to_string(),
                        property: "created_at".to_string(),
                        op: ComparisonOp::Gte,
                        value: Value::Int(10),
                    },
                    Predicate::PropertyCompare {
                        variable: "m".to_string(),
                        property: "created_at".to_string(),
                        op: ComparisonOp::Lt,
                        value: Value::Int(20),
                    },
                ]),
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        }),
    }
}

fn range_expand_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [],
            [],
            [("Memory".to_string(), "created_at".to_string())],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000), ("Entity".to_string(), 2_000)],
            [("LINKS".to_string(), 500)],
            [("LINKS".to_string(), 250)],
            [(
                (
                    "Memory".to_string(),
                    "LINKS".to_string(),
                    "Entity".to_string(),
                ),
                25,
            )],
            [(
                (
                    "Memory".to_string(),
                    "LINKS".to_string(),
                    "Entity".to_string(),
                    2,
                ),
                125,
            )],
            [],
            [(
                ("Memory".to_string(), "created_at".to_string()),
                (0..100).map(Value::Int).collect::<Vec<_>>(),
            )],
        ),
    )
}

fn composite_seek_plan() -> LogicalPlan {
    project_memory_title(LogicalPlan::Filter {
        predicate: Predicate::And(vec![
            Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "id".to_string(),
                value: Value::Int(42),
            },
            Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "kind".to_string(),
                value: Value::String("note".to_string()),
            },
        ]),
        input: Box::new(memory_scan()),
    })
}

fn composite_seek_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [],
            [(
                "Memory".to_string(),
                vec!["id".to_string(), "kind".to_string()],
            )],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [
                (("Memory".to_string(), "id".to_string()), 1_000),
                (("Memory".to_string(), "kind".to_string()), 10),
            ],
            [],
        ),
    )
}

fn text_seek_plan() -> LogicalPlan {
    project_memory_title(LogicalPlan::Filter {
        predicate: Predicate::PropertyContains {
            variable: "m".to_string(),
            property: "body".to_string(),
            value: "graph".to_string(),
        },
        input: Box::new(memory_scan()),
    })
}

fn text_seek_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], [("Memory".to_string(), "body".to_string())]),
        OptimizerCatalogStatistics::new([("Memory".to_string(), 1_000)], [], [], [], [], [], []),
    )
}

fn residual_node_string_contains_plan() -> LogicalPlan {
    project_memory_title(LogicalPlan::Filter {
        predicate: Predicate::PropertyContains {
            variable: "m".to_string(),
            property: "body".to_string(),
            value: "graph".to_string(),
        },
        input: Box::new(memory_scan()),
    })
}

fn residual_node_string_contains_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
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
    )
}

fn low_selectivity_scan_plan() -> LogicalPlan {
    project_memory_title(LogicalPlan::Filter {
        predicate: Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "kind".to_string(),
            value: Value::String("note".to_string()),
        },
        input: Box::new(memory_scan()),
    })
}

fn low_selectivity_scan_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "kind".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 1_000)],
            [],
            [],
            [],
            [],
            [(("Memory".to_string(), "kind".to_string()), 1)],
            [],
        ),
    )
}

fn memory_seed_entity_mentions_plan() -> LogicalPlan {
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(10),
        input: Box::new(LogicalPlan::Sort {
            items: vec![SortItem {
                key: SortKey::Column("mention_count".to_string()),
                direction: SortDirection::Desc,
            }],
            input: Box::new(LogicalPlan::Aggregate {
                group_keys: vec![
                    Projection {
                        expression: ProjectionExpression::Property {
                            variable: "e".to_string(),
                            property: "id".to_string(),
                        },
                        name: "entity_id".to_string(),
                    },
                    Projection {
                        expression: ProjectionExpression::Property {
                            variable: "e".to_string(),
                            property: "name".to_string(),
                        },
                        name: "entity_name".to_string(),
                    },
                ],
                items: vec![Aggregation {
                    function: AggregateFunction::Count,
                    target: AggregateTarget::Variable("m".to_string()),
                    distinct: true,
                    name: "mention_count".to_string(),
                }],
                input: Box::new(LogicalPlan::Expand {
                    source_variable: "m".to_string(),
                    source_label: "Memory".to_string(),
                    rel_variable: None,
                    rel_type: "MENTIONS".to_string(),
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
                            value: Value::String("memory-42".to_string()),
                        },
                        input: Box::new(memory_scan()),
                    }),
                }),
            }),
        }),
    }
}

fn memory_seed_entity_mentions_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 10_000),
                ("Entity".to_string(), 50_000),
            ],
            [("MENTIONS".to_string(), 120_000)],
            [("MENTIONS".to_string(), 40_000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                40_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                40_000,
            )],
            [(("Memory".to_string(), "id".to_string()), 10_000)],
            [],
        ),
    )
}

fn relationship_property_expand_plan() -> LogicalPlan {
    LogicalPlan::Project {
        items: vec![
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "e".to_string(),
                    property: "name".to_string(),
                },
                name: "entity".to_string(),
            },
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "r".to_string(),
                    property: "weight".to_string(),
                },
                name: "weight".to_string(),
            },
        ],
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
                input: Box::new(memory_scan()),
            }),
        }),
    }
}

fn relationship_property_expand_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 10_000),
                ("Entity".to_string(), 50_000),
            ],
            [("MENTIONS".to_string(), 120_000)],
            [("MENTIONS".to_string(), 40_000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                40_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                40_000,
            )],
            [(("Memory".to_string(), "id".to_string()), 10_000)],
            [],
        )
        .with_relationship_property_distinct_counts([(
            ("MENTIONS".to_string(), "weight".to_string()),
            10,
        )]),
    )
}

fn relationship_status_range_filter_plan() -> LogicalPlan {
    LogicalPlan::Project {
        items: vec![
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "e".to_string(),
                    property: "name".to_string(),
                },
                name: "entity".to_string(),
            },
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "r".to_string(),
                    property: "created_at".to_string(),
                },
                name: "created_at".to_string(),
            },
        ],
        input: Box::new(LogicalPlan::Filter {
            predicate: Predicate::PropertyCompare {
                variable: "r".to_string(),
                property: "created_at".to_string(),
                op: ComparisonOp::Gt,
                value: Value::Int(80),
            },
            input: Box::new(LogicalPlan::Expand {
                source_variable: "m".to_string(),
                source_label: "Memory".to_string(),
                rel_variable: Some("r".to_string()),
                rel_type: "MENTIONS".to_string(),
                rel_properties: BTreeMap::from([(
                    "status".to_string(),
                    Value::String("active".to_string()),
                )]),
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
                    input: Box::new(memory_scan()),
                }),
            }),
        }),
    }
}

fn relationship_status_range_filter_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Memory".to_string(), 10_000),
                ("Entity".to_string(), 50_000),
            ],
            [("MENTIONS".to_string(), 120_000)],
            [("MENTIONS".to_string(), 40_000)],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                40_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                40_000,
            )],
            [(("Memory".to_string(), "id".to_string()), 10_000)],
            [],
        )
        .with_relationship_property_distinct_counts([(
            ("MENTIONS".to_string(), "status".to_string()),
            2,
        )])
        .with_relationship_property_histograms([(
            ("MENTIONS".to_string(), "created_at".to_string()),
            (0..10).map(|bucket| Value::Int(bucket * 10)).collect(),
        )]),
    )
}

fn relationship_property_in_filter_plan() -> LogicalPlan {
    LogicalPlan::Filter {
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
            input: Box::new(memory_scan()),
        }),
    }
}

fn relationship_property_in_filter_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
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
    )
}

fn relationship_id_in_filter_plan() -> LogicalPlan {
    LogicalPlan::Filter {
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
            input: Box::new(memory_scan()),
        }),
    }
}

fn relationship_id_in_filter_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
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
    )
}

fn source_memory_label_cross_pattern_plan() -> LogicalPlan {
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(20),
        input: Box::new(LogicalPlan::Sort {
            items: vec![SortItem {
                key: SortKey::Column("memory_count".to_string()),
                direction: SortDirection::Desc,
            }],
            input: Box::new(LogicalPlan::Aggregate {
                group_keys: vec![Projection {
                    expression: ProjectionExpression::Property {
                        variable: "l".to_string(),
                        property: "name".to_string(),
                    },
                    name: "label_name".to_string(),
                }],
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
                    rel_properties: Default::default(),
                    direction: RelationshipDirection::Outgoing,
                    target_variable: "l".to_string(),
                    target_label: "Label".to_string(),
                    min_hops: 1,
                    max_hops: 1,
                    optional: false,
                    input: Box::new(LogicalPlan::Expand {
                        source_variable: "s".to_string(),
                        source_label: "Source".to_string(),
                        rel_variable: None,
                        rel_type: "SOURCED_FROM".to_string(),
                        rel_properties: Default::default(),
                        direction: RelationshipDirection::Incoming,
                        target_variable: "m".to_string(),
                        target_label: "Memory".to_string(),
                        min_hops: 1,
                        max_hops: 1,
                        optional: false,
                        input: Box::new(LogicalPlan::Filter {
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
                }),
            }),
        }),
    }
}

fn source_memory_label_cross_pattern_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Source".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Source".to_string(), 1_000),
                ("Memory".to_string(), 100_000),
                ("Label".to_string(), 2_000),
            ],
            [
                ("SOURCED_FROM".to_string(), 250_000),
                ("HAS_LABEL".to_string(), 180_000),
            ],
            [
                ("SOURCED_FROM".to_string(), 50_000),
                ("HAS_LABEL".to_string(), 80_000),
            ],
            [
                (
                    (
                        "Source".to_string(),
                        "SOURCED_FROM".to_string(),
                        "Memory".to_string(),
                    ),
                    250_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "HAS_LABEL".to_string(),
                        "Label".to_string(),
                    ),
                    180_000,
                ),
            ],
            [
                (
                    (
                        "Source".to_string(),
                        "SOURCED_FROM".to_string(),
                        "Memory".to_string(),
                        1,
                    ),
                    250_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "HAS_LABEL".to_string(),
                        "Label".to_string(),
                        1,
                    ),
                    180_000,
                ),
            ],
            [
                (("Source".to_string(), "id".to_string()), 1_000),
                (("Label".to_string(), "name".to_string()), 10),
            ],
            [],
        ),
    )
}

fn source_memory_entity_label_workload_plan() -> LogicalPlan {
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(25),
        input: Box::new(LogicalPlan::Sort {
            items: vec![SortItem {
                key: SortKey::Column("memory_count".to_string()),
                direction: SortDirection::Desc,
            }],
            input: Box::new(LogicalPlan::Aggregate {
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
                    rel_properties: Default::default(),
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
                        rel_properties: Default::default(),
                        direction: RelationshipDirection::Outgoing,
                        target_variable: "e".to_string(),
                        target_label: "Entity".to_string(),
                        min_hops: 1,
                        max_hops: 1,
                        optional: false,
                        input: Box::new(LogicalPlan::Expand {
                            source_variable: "s".to_string(),
                            source_label: "Source".to_string(),
                            rel_variable: None,
                            rel_type: "SOURCED_FROM".to_string(),
                            rel_properties: Default::default(),
                            direction: RelationshipDirection::Incoming,
                            target_variable: "m".to_string(),
                            target_label: "Memory".to_string(),
                            min_hops: 1,
                            max_hops: 1,
                            optional: false,
                            input: Box::new(LogicalPlan::Filter {
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
                    }),
                }),
            }),
        }),
    }
}

fn source_memory_entity_label_workload_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Source".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Source".to_string(), 1_000),
                ("Memory".to_string(), 100_000),
                ("Entity".to_string(), 50_000),
                ("Label".to_string(), 2_000),
            ],
            [
                ("SOURCED_FROM".to_string(), 250_000),
                ("MENTIONS".to_string(), 300_000),
                ("HAS_LABEL".to_string(), 180_000),
            ],
            [
                ("SOURCED_FROM".to_string(), 50_000),
                ("MENTIONS".to_string(), 90_000),
                ("HAS_LABEL".to_string(), 80_000),
            ],
            [
                (
                    (
                        "Source".to_string(),
                        "SOURCED_FROM".to_string(),
                        "Memory".to_string(),
                    ),
                    120_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                    ),
                    300_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "HAS_LABEL".to_string(),
                        "Label".to_string(),
                    ),
                    180_000,
                ),
            ],
            [
                (
                    (
                        "Source".to_string(),
                        "SOURCED_FROM".to_string(),
                        "Memory".to_string(),
                        1,
                    ),
                    120_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                        1,
                    ),
                    300_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "HAS_LABEL".to_string(),
                        "Label".to_string(),
                        1,
                    ),
                    180_000,
                ),
            ],
            [
                (("Source".to_string(), "id".to_string()), 1_000),
                (("Entity".to_string(), "community_id".to_string()), 8),
                (("Label".to_string(), "name".to_string()), 10),
            ],
            [],
        ),
    )
}

fn source_entity_community_export_workload_plan() -> LogicalPlan {
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(10),
        input: Box::new(LogicalPlan::Sort {
            items: vec![
                SortItem {
                    key: SortKey::Column("memory_count".to_string()),
                    direction: SortDirection::Desc,
                },
                SortItem {
                    key: SortKey::Column("entity_count".to_string()),
                    direction: SortDirection::Desc,
                },
            ],
            input: Box::new(LogicalPlan::Aggregate {
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
                            variable: "e".to_string(),
                            property: "entity_type".to_string(),
                        },
                        name: "entity_type".to_string(),
                    },
                ],
                items: vec![
                    Aggregation {
                        function: AggregateFunction::Count,
                        target: AggregateTarget::Variable("m".to_string()),
                        distinct: true,
                        name: "memory_count".to_string(),
                    },
                    Aggregation {
                        function: AggregateFunction::Count,
                        target: AggregateTarget::Variable("e".to_string()),
                        distinct: true,
                        name: "entity_count".to_string(),
                    },
                ],
                input: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::PropertyIsNotNull {
                        variable: "e".to_string(),
                        property: "community_id".to_string(),
                    },
                    input: Box::new(LogicalPlan::Expand {
                        source_variable: "m".to_string(),
                        source_label: "Memory".to_string(),
                        rel_variable: None,
                        rel_type: "MENTIONS".to_string(),
                        rel_properties: Default::default(),
                        direction: RelationshipDirection::Outgoing,
                        target_variable: "e".to_string(),
                        target_label: "Entity".to_string(),
                        min_hops: 1,
                        max_hops: 1,
                        optional: false,
                        input: Box::new(LogicalPlan::Filter {
                            predicate: Predicate::PropertyIn {
                                variable: "m".to_string(),
                                property: "unit_type".to_string(),
                                values: vec![
                                    Value::String("fact".to_string()),
                                    Value::String("episode".to_string()),
                                ],
                            },
                            input: Box::new(LogicalPlan::Expand {
                                source_variable: "s".to_string(),
                                source_label: "Source".to_string(),
                                rel_variable: None,
                                rel_type: "SOURCED_FROM".to_string(),
                                rel_properties: Default::default(),
                                direction: RelationshipDirection::Incoming,
                                target_variable: "m".to_string(),
                                target_label: "Memory".to_string(),
                                min_hops: 1,
                                max_hops: 1,
                                optional: false,
                                input: Box::new(LogicalPlan::Filter {
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
                        }),
                    }),
                }),
            }),
        }),
    }
}

fn source_entity_community_export_workload_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Source".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Source".to_string(), 1_000),
                ("Memory".to_string(), 100_000),
                ("Entity".to_string(), 50_000),
            ],
            [
                ("SOURCED_FROM".to_string(), 250_000),
                ("MENTIONS".to_string(), 300_000),
            ],
            [
                ("SOURCED_FROM".to_string(), 50_000),
                ("MENTIONS".to_string(), 90_000),
            ],
            [
                (
                    (
                        "Source".to_string(),
                        "SOURCED_FROM".to_string(),
                        "Memory".to_string(),
                    ),
                    120_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                    ),
                    300_000,
                ),
            ],
            [
                (
                    (
                        "Source".to_string(),
                        "SOURCED_FROM".to_string(),
                        "Memory".to_string(),
                        1,
                    ),
                    120_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                        1,
                    ),
                    300_000,
                ),
            ],
            [
                (("Source".to_string(), "id".to_string()), 1_000),
                (("Memory".to_string(), "unit_type".to_string()), 6),
                (("Entity".to_string(), "community_id".to_string()), 16),
                (("Entity".to_string(), "entity_type".to_string()), 12),
            ],
            [],
        ),
    )
}

fn community_synthesized_source_coverage_plan() -> LogicalPlan {
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(1),
        input: Box::new(LogicalPlan::Aggregate {
            group_keys: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "c".to_string(),
                    property: "id".to_string(),
                },
                name: "cid".to_string(),
            }],
            items: vec![Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::Variable("s".to_string()),
                distinct: true,
                name: "covered".to_string(),
            }],
            input: Box::new(LogicalPlan::Expand {
                source_variable: "c".to_string(),
                source_label: "Community".to_string(),
                rel_variable: None,
                rel_type: "SYNTHESIZED_FROM".to_string(),
                rel_properties: Default::default(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "s".to_string(),
                target_label: "Source".to_string(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                input: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::PropertyEq {
                        variable: "c".to_string(),
                        property: "id".to_string(),
                        value: Value::String("community-42".to_string()),
                    },
                    input: Box::new(LogicalPlan::NodeScan {
                        variable: "c".to_string(),
                        label: "Community".to_string(),
                    }),
                }),
            }),
        }),
    }
}

fn source_coverage_aggregate_filter_plan() -> LogicalPlan {
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(1),
        input: Box::new(LogicalPlan::Filter {
            predicate: Predicate::ExpressionEq {
                expression: ProjectionExpression::Column("covered".to_string()),
                value: ProjectionExpression::Literal(Value::Int(3)),
            },
            input: Box::new(LogicalPlan::Aggregate {
                group_keys: vec![Projection {
                    expression: ProjectionExpression::Property {
                        variable: "c".to_string(),
                        property: "id".to_string(),
                    },
                    name: "cid".to_string(),
                }],
                items: vec![Aggregation {
                    function: AggregateFunction::Count,
                    target: AggregateTarget::Variable("s".to_string()),
                    distinct: true,
                    name: "covered".to_string(),
                }],
                input: Box::new(LogicalPlan::Expand {
                    source_variable: "c".to_string(),
                    source_label: "Community".to_string(),
                    rel_variable: None,
                    rel_type: "SYNTHESIZED_FROM".to_string(),
                    rel_properties: Default::default(),
                    direction: RelationshipDirection::Outgoing,
                    target_variable: "s".to_string(),
                    target_label: "Source".to_string(),
                    min_hops: 1,
                    max_hops: 1,
                    optional: false,
                    input: Box::new(LogicalPlan::Filter {
                        predicate: Predicate::PropertyEq {
                            variable: "c".to_string(),
                            property: "id".to_string(),
                            value: Value::String("community-42".to_string()),
                        },
                        input: Box::new(LogicalPlan::NodeScan {
                            variable: "c".to_string(),
                            label: "Community".to_string(),
                        }),
                    }),
                }),
            }),
        }),
    }
}

fn community_synthesized_source_coverage_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Community".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Community".to_string(), 5_000),
                ("Source".to_string(), 20_000),
            ],
            [("SYNTHESIZED_FROM".to_string(), 40_000)],
            [("SYNTHESIZED_FROM".to_string(), 12_000)],
            [(
                (
                    "Community".to_string(),
                    "SYNTHESIZED_FROM".to_string(),
                    "Source".to_string(),
                ),
                40_000,
            )],
            [(
                (
                    "Community".to_string(),
                    "SYNTHESIZED_FROM".to_string(),
                    "Source".to_string(),
                    1,
                ),
                40_000,
            )],
            [(("Community".to_string(), "id".to_string()), 5_000)],
            [],
        )
        .with_path_target_distinct_counts([(
            (
                "Community".to_string(),
                "SYNTHESIZED_FROM".to_string(),
                "Source".to_string(),
            ),
            3,
        )]),
    )
}

fn feed_synthesized_source_collect_plan() -> LogicalPlan {
    LogicalPlan::Aggregate {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "c".to_string(),
                property: "id".to_string(),
            },
            name: "c.id".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Collect,
            target: AggregateTarget::Property {
                variable: "s".to_string(),
                property: "id".to_string(),
            },
            distinct: true,
            name: "source_ids".to_string(),
        }],
        input: Box::new(LogicalPlan::Expand {
            source_variable: "c".to_string(),
            source_label: "Memory".to_string(),
            rel_variable: None,
            rel_type: "SYNTHESIZED_FROM".to_string(),
            rel_properties: Default::default(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "s".to_string(),
            target_label: "Memory".to_string(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            input: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyIn {
                    variable: "c".to_string(),
                    property: "id".to_string(),
                    values: vec![
                        Value::String("crystal-1".to_string()),
                        Value::String("crystal-2".to_string()),
                    ],
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "c".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        }),
    }
}

fn feed_synthesized_source_collect_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Memory".to_string(), 50_000)],
            [("SYNTHESIZED_FROM".to_string(), 12_000)],
            [("SYNTHESIZED_FROM".to_string(), 4_000)],
            [(
                (
                    "Memory".to_string(),
                    "SYNTHESIZED_FROM".to_string(),
                    "Memory".to_string(),
                ),
                12_000,
            )],
            [(
                (
                    "Memory".to_string(),
                    "SYNTHESIZED_FROM".to_string(),
                    "Memory".to_string(),
                    1,
                ),
                12_000,
            )],
            [(("Memory".to_string(), "id".to_string()), 50_000)],
            [],
        )
        .with_path_target_distinct_counts([(
            (
                "Memory".to_string(),
                "SYNTHESIZED_FROM".to_string(),
                "Memory".to_string(),
            ),
            6_000,
        )]),
    )
}

fn skill_thread_source_provenance_workload_plan() -> LogicalPlan {
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(20),
        input: Box::new(LogicalPlan::Sort {
            items: vec![SortItem {
                key: SortKey::Column("thread_title".to_string()),
                direction: SortDirection::Asc,
            }],
            input: Box::new(LogicalPlan::Project {
                items: vec![
                    Projection {
                        expression: ProjectionExpression::Property {
                            variable: "t".to_string(),
                            property: "title".to_string(),
                        },
                        name: "thread_title".to_string(),
                    },
                    Projection {
                        expression: ProjectionExpression::Property {
                            variable: "m".to_string(),
                            property: "id".to_string(),
                        },
                        name: "memory_id".to_string(),
                    },
                ],
                input: Box::new(LogicalPlan::Expand {
                    source_variable: "m".to_string(),
                    source_label: "Memory".to_string(),
                    rel_variable: None,
                    rel_type: "COMPACTS_TO".to_string(),
                    rel_properties: BTreeMap::new(),
                    direction: RelationshipDirection::Incoming,
                    target_variable: "t".to_string(),
                    target_label: "Thread".to_string(),
                    min_hops: 1,
                    max_hops: 1,
                    optional: false,
                    input: Box::new(LogicalPlan::Expand {
                        source_variable: "sk".to_string(),
                        source_label: "Skill".to_string(),
                        rel_variable: None,
                        rel_type: "SYNTHESIZED_FROM".to_string(),
                        rel_properties: BTreeMap::new(),
                        direction: RelationshipDirection::Outgoing,
                        target_variable: "m".to_string(),
                        target_label: "Memory".to_string(),
                        min_hops: 1,
                        max_hops: 1,
                        optional: false,
                        input: Box::new(LogicalPlan::Filter {
                            predicate: Predicate::PropertyEq {
                                variable: "sk".to_string(),
                                property: "id".to_string(),
                                value: Value::String("skill-42".to_string()),
                            },
                            input: Box::new(LogicalPlan::NodeScan {
                                variable: "sk".to_string(),
                                label: "Skill".to_string(),
                            }),
                        }),
                    }),
                }),
            }),
        }),
    }
}

fn skill_thread_source_provenance_workload_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Skill".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Skill".to_string(), 2_000),
                ("Memory".to_string(), 100_000),
                ("Thread".to_string(), 10_000),
            ],
            [
                ("SYNTHESIZED_FROM".to_string(), 30_000),
                ("COMPACTS_TO".to_string(), 15_000),
            ],
            [
                ("SYNTHESIZED_FROM".to_string(), 10_000),
                ("COMPACTS_TO".to_string(), 10_000),
            ],
            [
                (
                    (
                        "Skill".to_string(),
                        "SYNTHESIZED_FROM".to_string(),
                        "Memory".to_string(),
                    ),
                    30_000,
                ),
                (
                    (
                        "Thread".to_string(),
                        "COMPACTS_TO".to_string(),
                        "Memory".to_string(),
                    ),
                    15_000,
                ),
            ],
            [
                (
                    (
                        "Skill".to_string(),
                        "SYNTHESIZED_FROM".to_string(),
                        "Memory".to_string(),
                        1,
                    ),
                    30_000,
                ),
                (
                    (
                        "Thread".to_string(),
                        "COMPACTS_TO".to_string(),
                        "Memory".to_string(),
                        1,
                    ),
                    15_000,
                ),
            ],
            [
                (("Skill".to_string(), "id".to_string()), 2_000),
                (("Thread".to_string(), "title".to_string()), 8_000),
            ],
            [],
        ),
    )
}

fn community_member_memory_evidence_workload_plan() -> LogicalPlan {
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(25),
        input: Box::new(LogicalPlan::Sort {
            items: vec![SortItem {
                key: SortKey::Column("memory_count".to_string()),
                direction: SortDirection::Desc,
            }],
            input: Box::new(LogicalPlan::Aggregate {
                group_keys: vec![Projection {
                    expression: ProjectionExpression::Property {
                        variable: "e".to_string(),
                        property: "entity_type".to_string(),
                    },
                    name: "entity_type".to_string(),
                }],
                items: vec![Aggregation {
                    function: AggregateFunction::Count,
                    target: AggregateTarget::Variable("m".to_string()),
                    distinct: true,
                    name: "memory_count".to_string(),
                }],
                input: Box::new(LogicalPlan::Expand {
                    source_variable: "e".to_string(),
                    source_label: "Entity".to_string(),
                    rel_variable: None,
                    rel_type: "MENTIONS".to_string(),
                    rel_properties: BTreeMap::new(),
                    direction: RelationshipDirection::Incoming,
                    target_variable: "m".to_string(),
                    target_label: "Memory".to_string(),
                    min_hops: 1,
                    max_hops: 1,
                    optional: false,
                    input: Box::new(LogicalPlan::Expand {
                        source_variable: "c".to_string(),
                        source_label: "Community".to_string(),
                        rel_variable: None,
                        rel_type: "BELONGS_TO".to_string(),
                        rel_properties: BTreeMap::new(),
                        direction: RelationshipDirection::Incoming,
                        target_variable: "e".to_string(),
                        target_label: "Entity".to_string(),
                        min_hops: 1,
                        max_hops: 1,
                        optional: false,
                        input: Box::new(LogicalPlan::Filter {
                            predicate: Predicate::PropertyEq {
                                variable: "c".to_string(),
                                property: "id".to_string(),
                                value: Value::String("community-42".to_string()),
                            },
                            input: Box::new(LogicalPlan::NodeScan {
                                variable: "c".to_string(),
                                label: "Community".to_string(),
                            }),
                        }),
                    }),
                }),
            }),
        }),
    }
}

fn community_member_memory_evidence_workload_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Community".to_string(), "id".to_string())], [], [], []),
        OptimizerCatalogStatistics::new(
            [
                ("Community".to_string(), 5_000),
                ("Entity".to_string(), 50_000),
                ("Memory".to_string(), 100_000),
            ],
            [
                ("BELONGS_TO".to_string(), 40_000),
                ("MENTIONS".to_string(), 300_000),
            ],
            [
                ("BELONGS_TO".to_string(), 5_000),
                ("MENTIONS".to_string(), 90_000),
            ],
            [
                (
                    (
                        "Entity".to_string(),
                        "BELONGS_TO".to_string(),
                        "Community".to_string(),
                    ),
                    40_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                    ),
                    300_000,
                ),
            ],
            [
                (
                    (
                        "Entity".to_string(),
                        "BELONGS_TO".to_string(),
                        "Community".to_string(),
                        1,
                    ),
                    40_000,
                ),
                (
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                        1,
                    ),
                    300_000,
                ),
            ],
            [
                (("Community".to_string(), "id".to_string()), 5_000),
                (("Entity".to_string(), "entity_type".to_string()), 12),
            ],
            [],
        ),
    )
}

fn entity_bridge_span_aggregate_plan() -> LogicalPlan {
    LogicalPlan::Limit {
        offset: 0,
        limit: Some(10),
        input: Box::new(LogicalPlan::Sort {
            items: vec![SortItem {
                key: SortKey::Column("community_span".to_string()),
                direction: SortDirection::Desc,
            }],
            input: Box::new(LogicalPlan::Aggregate {
                group_keys: vec![Projection {
                    expression: ProjectionExpression::Property {
                        variable: "e1".to_string(),
                        property: "id".to_string(),
                    },
                    name: "entity_id".to_string(),
                }],
                items: vec![
                    Aggregation {
                        function: AggregateFunction::Count,
                        target: AggregateTarget::Property {
                            variable: "e2".to_string(),
                            property: "community_id".to_string(),
                        },
                        distinct: true,
                        name: "community_span".to_string(),
                    },
                    Aggregation {
                        function: AggregateFunction::Count,
                        target: AggregateTarget::All,
                        distinct: false,
                        name: "bridge_strength".to_string(),
                    },
                ],
                input: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::PropertyIn {
                        variable: "e1".to_string(),
                        property: "community_id".to_string(),
                        values: vec![Value::Int(10), Value::Int(20), Value::Int(30)],
                    },
                    input: Box::new(LogicalPlan::Expand {
                        source_variable: "e1".to_string(),
                        source_label: "Entity".to_string(),
                        rel_variable: None,
                        rel_type: "RELATES_TO".to_string(),
                        rel_properties: BTreeMap::new(),
                        direction: RelationshipDirection::Undirected,
                        target_variable: "e2".to_string(),
                        target_label: "Entity".to_string(),
                        min_hops: 1,
                        max_hops: 1,
                        optional: false,
                        input: Box::new(entity_scan("e1")),
                    }),
                }),
            }),
        }),
    }
}

fn entity_bridge_span_aggregate_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([], [], [], []),
        OptimizerCatalogStatistics::new(
            [("Entity".to_string(), 10_000)],
            [("RELATES_TO".to_string(), 60_000)],
            [("RELATES_TO".to_string(), 10_000)],
            [(
                (
                    "Entity".to_string(),
                    "RELATES_TO".to_string(),
                    "Entity".to_string(),
                ),
                60_000,
            )],
            [(
                (
                    "Entity".to_string(),
                    "RELATES_TO".to_string(),
                    "Entity".to_string(),
                    1,
                ),
                60_000,
            )],
            [
                (("Entity".to_string(), "id".to_string()), 10_000),
                (("Entity".to_string(), "community_id".to_string()), 100),
            ],
            [],
        ),
    )
}

fn bounded_path_distinct_target_aggregate_plan() -> LogicalPlan {
    LogicalPlan::Aggregate {
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
            input: Box::new(memory_scan()),
        }),
    }
}

fn bounded_path_distinct_target_aggregate_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
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
    )
}

fn thread_cleanup_optional_count_plan() -> LogicalPlan {
    LogicalPlan::OptionalRelationshipCountSum {
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
    }
}

fn thread_cleanup_optional_count_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
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
    )
}

fn entity_incoming_mention_optional_count_plan() -> LogicalPlan {
    LogicalPlan::OptionalRelationshipCountSum {
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
    }
}

fn entity_incoming_mention_optional_degree_plan() -> LogicalPlan {
    LogicalPlan::OptionalDegree {
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
    }
}

fn entity_incoming_mention_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
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
    )
}

fn endpoint_existence_cartesian_product_plan() -> LogicalPlan {
    LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "id".to_string(),
            },
            name: "memory_id".to_string(),
        }],
        input: Box::new(LogicalPlan::NodeCartesianProduct {
            left: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "id".to_string(),
                    value: Value::String("memory-42".to_string()),
                },
                input: Box::new(memory_scan()),
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
    }
}

fn endpoint_existence_cartesian_product_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
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
    )
}

fn nested_endpoint_existence_cartesian_product_plan() -> LogicalPlan {
    LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "id".to_string(),
            },
            name: "memory_id".to_string(),
        }],
        input: Box::new(LogicalPlan::NodeCartesianProduct {
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
                    input: Box::new(memory_scan()),
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
        }),
    }
}

fn nested_endpoint_existence_cartesian_product_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
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
    )
}

fn post_product_node_property_filter_plan() -> LogicalPlan {
    LogicalPlan::Filter {
        predicate: Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "kind".to_string(),
            value: Value::String("note".to_string()),
        },
        input: Box::new(LogicalPlan::NodeCartesianProduct {
            left: Box::new(memory_scan()),
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
    }
}

fn post_product_node_property_filter_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
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
    )
}

fn residual_node_property_in_plan() -> LogicalPlan {
    LogicalPlan::Filter {
        predicate: Predicate::PropertyIn {
            variable: "m".to_string(),
            property: "id".to_string(),
            values: vec![Value::Int(1), Value::Int(2), Value::Int(2), Value::Int(3)],
        },
        input: Box::new(memory_scan()),
    }
}

fn residual_node_property_in_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
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
    )
}

fn thread_optional_source_filter_plan() -> LogicalPlan {
    LogicalPlan::Filter {
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
    }
}

fn thread_optional_source_filter_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
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
    )
}

fn community_summary_presence_filter_plan() -> LogicalPlan {
    LogicalPlan::Filter {
        predicate: Predicate::PropertyIsNotNull {
            variable: "c".to_string(),
            property: "ai_summary".to_string(),
        },
        input: Box::new(LogicalPlan::NodeScan {
            variable: "c".to_string(),
            label: "Community".to_string(),
        }),
    }
}

fn community_summary_presence_filter_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
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
    )
}

fn normalized_space_exclusion_filter_plan() -> LogicalPlan {
    LogicalPlan::Filter {
        predicate: Predicate::PropertyNotEq {
            variable: "m".to_string(),
            property: "space_id".to_string(),
            value: Value::String("default".to_string()),
        },
        input: Box::new(memory_scan()),
    }
}

fn normalized_space_exclusion_filter_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
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
    )
}

fn thread_candidate_normalized_space_multi_seek_plan() -> LogicalPlan {
    LogicalPlan::Filter {
        predicate: Predicate::And(vec![
            Predicate::PropertyIn {
                variable: "t".to_string(),
                property: "thread_id".to_string(),
                values: vec![
                    Value::String("thread-1".to_string()),
                    Value::String("thread-2".to_string()),
                    Value::String("thread-3".to_string()),
                ],
            },
            Predicate::ExpressionEq {
                expression: ProjectionExpression::DefaultIfNullOrEq {
                    variable: "t".to_string(),
                    property: "space_id".to_string(),
                    empty: Value::String(String::new()),
                    default: Value::String("default".to_string()),
                },
                value: ProjectionExpression::Literal(Value::String("default".to_string())),
            },
        ]),
        input: Box::new(LogicalPlan::NodeScan {
            variable: "t".to_string(),
            label: "Thread".to_string(),
        }),
    }
}

fn thread_candidate_normalized_space_multi_seek_catalog() -> OptimizerCatalog {
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [("Thread".to_string(), "thread_id".to_string())],
            [],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [("Thread".to_string(), 50_000)],
            [],
            [],
            [],
            [],
            [(("Thread".to_string(), "thread_id".to_string()), 50_000)],
            [],
        ),
    )
}

fn project_memory_title(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Project {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "title".to_string(),
            },
            name: "title".to_string(),
        }],
        input: Box::new(input),
    }
}

fn memory_scan() -> LogicalPlan {
    LogicalPlan::NodeScan {
        variable: "m".to_string(),
        label: "Memory".to_string(),
    }
}

fn entity_scan(variable: &str) -> LogicalPlan {
    LogicalPlan::NodeScan {
        variable: variable.to_string(),
        label: "Entity".to_string(),
    }
}
