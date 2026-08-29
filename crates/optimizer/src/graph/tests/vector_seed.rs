use super::super::{CascadesOptimizer, OptimizerCatalog};
use crate::{OptimizerConfig, OptimizerContext, OptimizerSearchDirective, ResourceHints};
use skein_core::Value;
use skein_cypher::RelationshipDirection;
use skein_plan::{
    LogicalPlan, LogicalPlanRoot, PhysicalPlan, Predicate, VectorExecutionResourceProfile,
    VectorPhysicalPlan,
};

fn vector_seed_filter(property: &str) -> LogicalPlan {
    LogicalPlan::Filter {
        predicate: Predicate::PropertyEq {
            variable: "m".to_string(),
            property: property.to_string(),
            value: Value::String("selected".to_string()),
        },
        input: Box::new(LogicalPlan::NodeColumnLookup {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            property: "id".to_string(),
            column: "external_id".to_string(),
            optional: false,
            input: Box::new(LogicalPlan::VectorSeed {
                embedding_parameter: "embedding".to_string(),
                embedding_dimension: 2,
                top_k: 8,
                output_external_id: true,
            }),
        }),
    }
}

fn vector_seed_scan(plan: &PhysicalPlan) -> &PhysicalPlan {
    match plan {
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::NodeColumnLookupExec { input, .. } => vector_seed_scan(input),
        PhysicalPlan::VectorSeedScan { .. } => plan,
        other => panic!("expected vector seed plan, got {}", other.kind().as_str()),
    }
}

fn vector_filter_fields(plan: &VectorPhysicalPlan) -> &[String] {
    match plan {
        VectorPhysicalPlan::Filter { fields } => fields,
        VectorPhysicalPlan::VectorCandidateScan { input, .. }
        | VectorPhysicalPlan::ResidualFilter { input, .. }
        | VectorPhysicalPlan::RawVectorRerank { input, .. }
        | VectorPhysicalPlan::TopK { input, .. } => vector_filter_fields(input),
    }
}

#[test]
fn descriptor_safe_filter_is_pushed_into_vector_seed_scan() {
    let (physical, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(
            &vector_seed_filter("space_id"),
            &OptimizerCatalog::default(),
        );

    let PhysicalPlan::VectorSeedScan {
        metadata_filters,
        vector_plan,
        ..
    } = vector_seed_scan(&physical)
    else {
        unreachable!();
    };
    assert_eq!(
        metadata_filters.get("space_id"),
        Some(&"selected".to_string())
    );
    assert_eq!(vector_filter_fields(vector_plan), &["space_id".to_string()]);
    assert!(trace.decisions.iter().any(|decision| {
        decision.contains("push descriptor-safe vector seed filter m.space_id")
    }));
}

#[test]
fn unsupported_filter_remains_graph_side_only() {
    let physical = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize(&vector_seed_filter("custom_property"));

    assert!(matches!(physical, PhysicalPlan::FilterExec { .. }));
    let PhysicalPlan::VectorSeedScan {
        metadata_filters,
        vector_plan,
        ..
    } = vector_seed_scan(&physical)
    else {
        unreachable!();
    };
    assert!(metadata_filters.is_empty());
    assert!(vector_filter_fields(vector_plan).is_empty());
}

#[test]
fn vector_seed_preserves_optimizer_resource_hints_across_lowering_modes() {
    let optimizer = CascadesOptimizer::with_context(
        OptimizerContext::from_config(OptimizerConfig { max_groups: 16 }).with_resource_hints(
            ResourceHints {
                priority: 200,
                max_memory_bytes: Some(8 * 1024 * 1024),
                max_parallelism: 3,
            },
        ),
    );
    let logical = LogicalPlanRoot::new(LogicalPlan::VectorSeed {
        embedding_parameter: "embedding".to_string(),
        embedding_dimension: 2,
        top_k: 8,
        output_external_id: true,
    });
    let expected = VectorExecutionResourceProfile {
        priority: 200,
        max_parallelism: 3,
        max_working_memory_bytes: Some(8 * 1024 * 1024),
    };

    for directive in [
        OptimizerSearchDirective::Memo,
        OptimizerSearchDirective::DirectFallback,
    ] {
        let optimized = optimizer
            .optimize_root_with_catalog_and_directive(
                &logical,
                &OptimizerCatalog::default(),
                directive,
            )
            .unwrap();
        let PhysicalPlan::VectorSeedScan {
            resource_profile, ..
        } = optimized.plan()
        else {
            panic!("expected vector seed scan");
        };
        assert_eq!(*resource_profile, expected);
    }
}

#[test]
fn vector_seeded_expand_receives_bounded_graph_budget() {
    let logical = LogicalPlan::Expand {
        source_variable: "m".to_string(),
        source_label: "Memory".to_string(),
        rel_variable: None,
        rel_type: "MENTIONS".to_string(),
        rel_properties: Default::default(),
        direction: RelationshipDirection::Outgoing,
        target_variable: "e".to_string(),
        target_label: "Entity".to_string(),
        min_hops: 1,
        max_hops: 2,
        optional: false,
        input: Box::new(LogicalPlan::NodeColumnLookup {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            property: "id".to_string(),
            column: "external_id".to_string(),
            optional: false,
            input: Box::new(LogicalPlan::VectorSeed {
                embedding_parameter: "embedding".to_string(),
                embedding_dimension: 2,
                top_k: 8,
                output_external_id: true,
            }),
        }),
    };

    let (physical, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
        .optimize_with_catalog(&logical, &OptimizerCatalog::default());

    let PhysicalPlan::AdjacencyExpandExec {
        graph_budget: Some(graph_budget),
        ..
    } = physical
    else {
        panic!("expected bounded adjacency expansion");
    };
    assert_eq!(graph_budget.candidate_limit, 512);
    assert_eq!(graph_budget.payload_byte_limit, 4 * 1024 * 1024);
    assert!(trace
        .decisions
        .iter()
        .any(|decision| decision.contains("bound vector-seeded graph expansion")));
}
