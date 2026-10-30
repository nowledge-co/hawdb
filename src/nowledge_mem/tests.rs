// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::legacy_nowledge_mem_read_error;
use super::{
    nowledge_mem_bounded_read_evidence_json_with_route_readiness,
    nowledge_mem_fast_path_classification, nowledge_mem_graph_config,
    nowledge_mem_graph_config_with_search_mode,
    nowledge_mem_source_mutation_dual_write_evidence_all_ready,
    nowledge_mem_source_mutation_dual_write_readiness, NowledgeMemCutoverControls,
    NowledgeMemEmbeddedStore, NowledgeMemEmbeddedStoreHandle, NowledgeMemGraph,
    NowledgeMemGraphMode, NowledgeMemOpenDiagnosticOptions, NowledgeMemOpenOptions,
    NowledgeMemOutOfCoreSearchProjection, NowledgeMemQualifiedOutOfCoreSearchOptions,
    NowledgeMemQueryExecutionPath, NowledgeMemQueryReportOptions, NowledgeMemReadOptions,
    NowledgeMemReadinessAreaSummary, NowledgeMemReadinessDashboard, NowledgeMemReadinessOptions,
    NowledgeMemRetrievalProjectionAdvisor, NowledgeMemRouteReadinessSummary,
    NowledgeMemSearchCandidateReadinessOptions, NowledgeMemSearchCandidateRequest,
    NowledgeMemSearchCandidateShadowAccumulator, NowledgeMemSearchProjection,
    NowledgeMemServingEntrypoint, NowledgeMemServingPathReadiness,
    NowledgeMemSourceMutationDualWriteEvidence, NowledgeMemStorageLifecycleActionKind,
    NowledgeMemStorageLifecycleDecision, NowledgeMemStorageRecoveryReport, NowledgeMemWorkControl,
    NowledgeQueryRuntimePreflightProbe, NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
    NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL, NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL,
    NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL, NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL,
    NOWLEDGE_MEM_PRODUCTION_STATUS_PROTOCOL, NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL,
    NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL, NOWLEDGE_MEM_READ_REPORT_PROTOCOL,
    NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL, NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL,
    NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE,
    NOWLEDGE_MEM_STORAGE_LIFECYCLE_DECISION_PROTOCOL, NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL,
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
    REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES, REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
};
use crate::mem_integration_readiness::nowledge_mem_final_cutover_preflight;
use crate::route_ownership::{
    nowledge_mem_route_ownership_all_hawdb, nowledge_mem_route_ownership_all_legacy,
    nowledge_mem_route_ownership_readiness, NowledgeMemRouteOwnershipPolicy,
};
use crate::search::{
    CompressedVectorSearchMode, SearchFusionWeights, SearchLexicalFeasibilityCoverage,
    SearchLexicalFeasibilityMetrics, SearchLexicalProductionQualificationReport,
    SearchOutOfCoreConfig, SearchTopKScoreParity,
};
use crate::search_route_ownership::{
    nowledge_mem_active_search_route_ownership_all_hawdb,
    nowledge_mem_active_search_route_ownership_readiness,
    nowledge_mem_active_search_route_read_evidence_all_hawdb_ready,
    nowledge_mem_active_search_route_readiness, nowledge_mem_search_route_ownership_all_hawdb,
    nowledge_mem_search_route_ownership_readiness,
    NowledgeMemActiveSearchRouteOwnershipReadinessReport,
    NowledgeMemActiveSearchRouteReadinessReport, NowledgeMemSearchRouteOwnershipPolicy,
    NowledgeMemSearchRouteOwnershipReadinessReport,
};
use crate::workload_fixtures::{
    nowledge_graph_route_workload_fixture_report, NowledgeGraphRouteWorkloadFixtureOptions,
};
use crate::AdaptiveVectorBackendPolicy;
use crate::ReadBudgetResource;
use crate::Value;
use crate::{
    BackgroundMaintenanceKind, BackgroundMaintenanceOptions, BackgroundWorkHint, Database,
    DatabaseConfig, GraphRagQueryBinding, GraphRagQueryDraft, GraphRagQueryPattern,
    GraphRagQueryPredicate, GraphRagQueryPredicateOperator, GraphRagQueryProjection,
    GraphRagSchemaContextOptions, HawDBError, HawDBLightningInitialImportCheckpoint,
    HawDBLightningInitialImportCutoverCatchUpReport, HawDBLightningInitialImportDocumentIdentity,
    HawDBLightningInitialImportReadinessInputs, KnowledgeCandidateScoringPolicy,
    KnowledgeRetrievalRequest, LocalQosPolicy, LocalQosState, NowledgeGraphStatement,
    ProductionEvidenceBinding, ProductionQualificationIdentity, RecoveryMode,
    SearchEmbeddingManifest, SearchIndex, SearchMode, SearchProjectionDelta,
    SearchProjectionFreshness, SearchProjectionKind, SearchProjectionProbeOptions,
    SearchProjectionRelationalDelta, SearchProjectionRow, StorageOpenTimings,
    StorageRecoveryReport, StorageResidencyMode, StorageResourceProfileLimits,
    VectorRecallValidationOptions, VectorRecallValidationReport, WorkClass,
    PRODUCTION_QUALIFICATION_POLICY_VERSION, VECTOR_RECALL_VALIDATION_PROTOCOL,
};
use std::any::TypeId;
use std::collections::BTreeMap;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

fn production_identity(canonical_graph_commit_epoch: u64) -> ProductionQualificationIdentity {
    ProductionQualificationIdentity {
        source_revision: "test-revision".to_string(),
        rust_toolchain: "test-toolchain".to_string(),
        target_os: std::env::consts::OS.to_string(),
        target_arch: std::env::consts::ARCH.to_string(),
        enabled_features: vec!["vector-search".to_string()],
        durable_format_version: 1,
        schema_version: 1,
        configuration_digest: "test-configuration".to_string(),
        deployment_profile: "test".to_string(),
        dataset_fingerprint: "test-dataset".to_string(),
        canonical_graph_commit_epoch,
        policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
    }
}

fn ready_vector_recall_report() -> VectorRecallValidationReport {
    VectorRecallValidationReport {
        protocol: VECTOR_RECALL_VALIDATION_PROTOCOL.to_string(),
        ready: true,
        approximate_backend: "hawdb_rabitq_candidate_projection".to_string(),
        sample_candidate_count: 2,
        requested_sample_count: 2,
        executed_sample_count: 2,
        top_k: 1,
        candidate_limit: 1,
        minimum_recall_per_million: 950_000,
        exact_hit_count: 2,
        candidate_hit_count: 2,
        candidate_overlap_count: 2,
        candidate_recall_at_k_per_million: 1_000_000,
        approximate_hit_count: 2,
        overlap_count: 2,
        recall_at_k_per_million: 1_000_000,
        overlap_at_k_per_million: 1_000_000,
        fallback_count: 0,
        index_coverage_incomplete_count: 0,
        average_filter_selectivity_per_million: 1_000_000,
        max_filter_selectivity_per_million: 1_000_000,
        blocker_codes: Vec::new(),
    }
}

#[test]
fn graph_config_tracks_shadow_vs_cutover_mode() {
    assert!(nowledge_mem_graph_config(NowledgeMemGraphMode::ShadowReadOnly).read_only);
    assert!(!nowledge_mem_graph_config(NowledgeMemGraphMode::WritableCutover).read_only);
    assert_eq!(
        nowledge_mem_graph_config(NowledgeMemGraphMode::ShadowReadOnly)
            .compressed_vector_search_mode,
        CompressedVectorSearchMode::Disabled
    );
    assert_eq!(
        nowledge_mem_graph_config_with_search_mode(
            NowledgeMemGraphMode::ShadowReadOnly,
            CompressedVectorSearchMode::Required,
        )
        .compressed_vector_search_mode,
        CompressedVectorSearchMode::Required
    );
}

#[test]
fn serving_path_readiness_reexports_the_readiness_owner_contract() {
    assert_eq!(
        TypeId::of::<NowledgeMemServingPathReadiness>(),
        TypeId::of::<hawdb_readiness::nowledge_mem_serving_path::NowledgeMemServingPathReadiness>()
    );
    assert_eq!(
        TypeId::of::<NowledgeMemServingEntrypoint>(),
        TypeId::of::<hawdb_readiness::nowledge_mem_serving_path::NowledgeMemServingEntrypoint>()
    );
}

#[test]
fn graph_facade_executes_cypher_through_library_api() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);

    graph
        .query("CREATE (:Memory {id: 'mem-1', title: 'Library seam'})")
        .unwrap();
    let output = graph
        .query("MATCH (m:Memory {id: 'mem-1'}) RETURN m.title AS title")
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(graph.mode(), NowledgeMemGraphMode::WritableCutover);
}

#[test]
fn embedded_handle_generates_and_executes_bounded_schema_guided_graph_rag() {
    let mut graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    graph
        .query(
            "CREATE (:Memory {id: 'memory-1'})\
                 -[:MENTIONS]->(:Entity {id: 'entity-1', name: 'HawDB'})",
        )
        .unwrap();
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None)).unwrap();
    let context = handle
        .graph_rag_schema_context(GraphRagSchemaContextOptions::default())
        .unwrap();
    let generated = context
        .generate_query(&GraphRagQueryDraft {
            schema_fingerprint: context.fingerprint,
            pattern: GraphRagQueryPattern::Route {
                source_label: "Memory".to_string(),
                relationship_type: "MENTIONS".to_string(),
                target_label: "Entity".to_string(),
            },
            predicates: vec![GraphRagQueryPredicate {
                binding: GraphRagQueryBinding::Source,
                property: "id".to_string(),
                operator: GraphRagQueryPredicateOperator::Eq,
                parameter: Some("memory_id".to_string()),
            }],
            projections: vec![GraphRagQueryProjection {
                binding: GraphRagQueryBinding::Target,
                property: "name".to_string(),
                alias: "entity_name".to_string(),
            }],
            limit: 2,
        })
        .unwrap();
    let output = handle
        .read_generated_graph_rag(
            &generated,
            &BTreeMap::from([(
                "memory_id".to_string(),
                Value::String("memory-1".to_string()),
            )]),
            &NowledgeMemReadOptions {
                max_rows: Some(2),
                max_estimated_payload_bytes: Some(256),
            },
        )
        .unwrap();

    assert_eq!(
        output.output.rows[0].get("entity_name"),
        Some(&Value::String("HawDB".to_string()))
    );
    assert_eq!(output.report.max_rows, Some(2));
    assert!(output.report.row_limit_enforced_before_output);

    handle
        .query_with_report("CREATE (:Source {id: 'source-1'})")
        .unwrap();
    let error = handle
        .read_generated_graph_rag(
            &generated,
            &BTreeMap::from([(
                "memory_id".to_string(),
                Value::String("memory-1".to_string()),
            )]),
            &NowledgeMemReadOptions::default(),
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("GraphRAG schema context is stale"));
}

#[test]
fn fast_path_classification_uses_ast_shape_not_query_text() {
    let compact =
        crate::cypher::parse("MATCH (m:Memory {id: $id}) RETURN m.title AS title").unwrap();
    let spaced = crate::cypher::parse(
        "  match   ( m : Memory   { id : $id } )   return   m.title   as   title  ",
    )
    .unwrap();
    let ordered = crate::cypher::parse(
        "MATCH (m:Memory {id: $id}) RETURN m.title AS title ORDER BY title LIMIT 1",
    )
    .unwrap();

    let compact_classification = nowledge_mem_fast_path_classification(&compact);
    let spaced_classification = nowledge_mem_fast_path_classification(&spaced);
    let ordered_classification = nowledge_mem_fast_path_classification(&ordered);

    assert_eq!(
        compact_classification.execution_path,
        NowledgeMemQueryExecutionPath::FastPath
    );
    assert_eq!(
        compact_classification.fast_path_reason,
        Some("simple_node_lookup")
    );
    assert_eq!(compact_classification, spaced_classification);
    assert_eq!(
        ordered_classification.execution_path,
        NowledgeMemQueryExecutionPath::OptimizedPath
    );
    assert_eq!(ordered_classification.fast_path_reason, None);
}

#[test]
fn graph_query_with_report_marks_simple_lookup_fast_path() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'mem-fast', title: 'Fast path'})")
        .unwrap();

    let query = graph
        .query_with_report("MATCH (m:Memory {id: 'mem-fast'}) RETURN m.title AS title")
        .unwrap();

    assert_eq!(query.output.rows.len(), 1);
    assert_eq!(query.report.protocol, NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL);
    assert_eq!(query.report.statement_kind, "match_return");
    assert_eq!(
        query.report.execution_path,
        NowledgeMemQueryExecutionPath::FastPath
    );
    assert_eq!(
        query.report.fast_path_reason.as_deref(),
        Some("simple_node_lookup")
    );
    assert_eq!(query.report.optimizer_decision_count, 0);
    assert!(!query.report.physical_plan_captured);
    assert!(query.report.physical_operator_counts.is_empty());
    assert!(!query.report.slow_log_candidate);
    assert_eq!(query.report.json()["execution_path"], "fast_path");
    assert_eq!(query.report.json()["statement_kind"], "match_return");
    assert_eq!(query.report.json()["fast_path_selected"], true);
    assert_eq!(query.report.json()["physical_plan_captured"], false);
    assert_eq!(query.report.optimizer_rule_event_count, 0);
    assert_eq!(query.report.json()["optimizer_rule_event_count"], 0);
    assert_eq!(query.report.json()["plan_cache"]["cacheable"], true);
    assert_eq!(query.report.output_row_shape.row_count, 1);
    assert_eq!(query.report.output_row_shape.column_count, 1);
    assert_eq!(query.report.output_row_shape.columns, vec!["title"]);
    assert_eq!(query.report.json()["output_row_shape"]["row_count"], 1);
    assert_eq!(
        query.report.json()["output_row_shape"]["columns"],
        serde_json::json!(["title"])
    );
    assert!(
        query
            .report
            .api_behavior
            .include_metadata_false_strips_metadata
    );
    assert!(query.report.api_behavior.ordering_contract_recorded);
    assert!(query.report.api_behavior.pagination_contract_recorded);
    assert!(query.report.api_behavior.error_class_stable);
    assert!(!query.report.api_behavior.statement_has_ordering);
    assert!(!query.report.api_behavior.statement_has_pagination);
    assert_eq!(
        query.report.json()["api_behavior"]["error_class_stable"],
        true
    );
}

#[test]
fn graph_query_with_report_keeps_ordered_scan_on_optimized_path() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'mem-slow-1', title: 'B'})")
        .unwrap();
    graph
        .query("CREATE (:Memory {id: 'mem-slow-2', title: 'A'})")
        .unwrap();

    let query = graph
        .query_with_report("MATCH (m:Memory) RETURN m.title AS title ORDER BY title LIMIT 1")
        .unwrap();

    assert_eq!(query.output.rows.len(), 1);
    assert_eq!(
        query.report.execution_path,
        NowledgeMemQueryExecutionPath::OptimizedPath
    );
    assert_eq!(query.report.statement_kind, "match_return");
    assert_eq!(query.report.fast_path_reason, None);
    assert_eq!(query.report.optimizer_decision_count, 0);
    assert_eq!(query.report.optimizer_rule_event_count, 0);
    assert!(!query.report.physical_plan_captured);
    assert!(query.report.physical_operator_counts.is_empty());
    assert!(query.report.plan_cache_cacheable);
    assert!(query.report.plan_cache_miss);
    assert!(!query.report.plan_cache_hit);
    assert!(!query.report.plan_cache_bypassed);
    assert_eq!(query.report.json()["execution_path"], "optimized_path");
    assert_eq!(query.report.json()["fast_path_selected"], false);
    assert_eq!(query.report.json()["plan_cache"]["lookup"], "miss");
    assert_eq!(query.report.json()["plan_cache"]["cacheable"], true);
    assert_eq!(query.report.json()["plan_cache"]["miss"], true);
    assert!(query.report.api_behavior.statement_has_ordering);
    assert!(query.report.api_behavior.statement_has_pagination);
    assert_eq!(
        query.report.json()["api_behavior"]["statement_has_ordering"],
        true
    );
    assert_eq!(
        query.report.json()["api_behavior"]["statement_has_pagination"],
        true
    );
}

#[test]
fn graph_query_with_report_can_capture_physical_plan_on_demand() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'mem-plan-1', title: 'B'})")
        .unwrap();
    graph
        .query("CREATE (:Memory {id: 'mem-plan-2', title: 'A'})")
        .unwrap();

    let query = graph
        .query_with_params_with_report_options(
            "MATCH (m:Memory) RETURN m.title AS title ORDER BY title LIMIT 1",
            &BTreeMap::new(),
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: Some(0),
            },
        )
        .unwrap();

    assert_eq!(query.output.rows.len(), 1);
    assert!(query.report.physical_plan_captured);
    assert_eq!(query.report.statement_kind, "match_return");
    assert!(query.report.optimizer_decision_count > 0);
    assert!(query.report.optimizer_rule_event_count > 0);
    assert!(query
        .report
        .physical_operator_counts
        .contains_key("TopNExec"));
    assert_eq!(query.report.plan_cache_lookup.as_deref(), Some("miss"));
    assert_eq!(query.report.plan_cache_bypass_reason, None);
    assert!(query.report.slow_log_candidate);
    assert_eq!(query.report.json()["physical_plan_captured"], true);
    assert!(query.report.json()["optimizer_rule_event_count"]
        .as_u64()
        .is_some_and(|count| count > 0));
    assert_eq!(query.report.json()["plan_cache_lookup"], "miss");
    assert_eq!(query.report.json()["slow_log_candidate"], true);
    assert_eq!(
        query.report.json()["physical_operator_counts"]["TopNExec"],
        1
    );
}

#[test]
fn graph_query_with_report_captures_plan_without_extra_cache_lookup() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'mem-report-cache-1', title: 'B'})")
        .unwrap();
    graph
        .query("CREATE (:Memory {id: 'mem-report-cache-2', title: 'A'})")
        .unwrap();
    let before = graph.database().plan_cache_stats().unwrap();

    let query = graph
        .query_with_params_with_report_options(
            "MATCH (m:Memory) RETURN m.title AS title ORDER BY title LIMIT 1",
            &BTreeMap::new(),
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
        )
        .unwrap();
    let after = graph.database().plan_cache_stats().unwrap();

    assert_eq!(query.output.rows.len(), 1);
    assert!(query.report.physical_plan_captured);
    assert_eq!(query.report.plan_cache_lookup.as_deref(), Some("miss"));
    assert_eq!(query.report.plan_cache_bypass_reason, None);
    assert!(query.report.plan_cache_cacheable);
    assert!(query.report.plan_cache_miss);
    assert!(!query.report.plan_cache_bypassed);
    assert_eq!(after.entries, before.entries + 1);
    assert_eq!(after.misses, before.misses + 1);
    assert_eq!(after.hits, before.hits);
}

#[test]
fn graph_query_with_report_exposes_storage_scan_pruning() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(kind)").unwrap();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'mem-prune-1', kind: 'note', title: 'Keep'})")
        .unwrap();
    graph
        .query("CREATE (:Memory {id: 'mem-prune-2', kind: 'note', title: 'Also keep'})")
        .unwrap();

    let query = graph
        .query_with_report("MATCH (m:Memory) WHERE m.kind = 'note' RETURN m.title AS title")
        .unwrap();

    assert_eq!(query.output.rows.len(), 2);
    assert_eq!(query.report.scan_pruning_reports.len(), 1);
    let scan = &query.report.scan_pruning_reports[0];
    assert!(scan.pruned);
    assert_eq!(scan.candidate_count_before_filter, 2);
    assert_eq!(scan.output_count, 2);
    assert_eq!(query.report.json()["scan_pruning_report_count"], 1);
    assert_eq!(
        query.report.json()["scan_pruning_reports"][0]["strategy"]["kind"],
        "property_eq"
    );
    assert_eq!(
        query.report.json()["scan_pruning_reports"][0]["strategy"]["property"],
        "kind"
    );
}

#[test]
fn graph_query_report_exposes_normalized_default_scan_pruning() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Thread(space_id)").unwrap();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Thread {thread_id: 'thread-missing', title: 'Missing space'})")
        .unwrap();
    graph
        .query("CREATE (:Thread {thread_id: 'thread-empty', space_id: '', title: 'Empty space'})")
        .unwrap();
    graph
            .query("CREATE (:Thread {thread_id: 'thread-default', space_id: 'default', title: 'Default space'})")
            .unwrap();
    graph
        .query("CREATE (:Thread {thread_id: 'thread-team', space_id: 'team', title: 'Team space'})")
        .unwrap();

    let source_params = BTreeMap::from([(
        "source_space_id".to_string(),
        Value::String("default".to_string()),
    )]);
    let source_query = graph
            .query_with_params_with_report(
                "MATCH (t:Thread) WHERE CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $source_space_id RETURN t.thread_id AS thread_id",
                &source_params,
            )
            .unwrap();

    assert_eq!(source_query.output.rows.len(), 3);
    assert_eq!(source_query.report.scan_pruning_reports.len(), 1);
    let source_scan = &source_query.report.scan_pruning_reports[0];
    assert!(source_scan.pruned);
    assert_eq!(source_scan.candidate_count_before_pruning, 4);
    assert_eq!(source_scan.candidate_count_before_filter, 3);
    assert_eq!(source_scan.pruned_candidate_count, 1);
    assert_eq!(
        source_query.report.json()["scan_pruning_reports"][0]["strategy"]["kind"],
        "property_default_if_null_eq"
    );
    assert_eq!(
        source_query.report.json()["scan_pruning_reports"][0]["strategy"]["property"],
        "space_id"
    );

    let target_params = BTreeMap::from([(
        "target_space_id".to_string(),
        Value::String("default".to_string()),
    )]);
    let target_query = graph
            .query_with_params_with_report(
                "MATCH (t:Thread) WHERE CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END <> $target_space_id RETURN t.thread_id AS thread_id",
                &target_params,
            )
            .unwrap();

    assert_eq!(target_query.output.rows.len(), 1);
    assert_eq!(target_query.report.scan_pruning_reports.len(), 1);
    let target_scan = &target_query.report.scan_pruning_reports[0];
    assert!(target_scan.pruned);
    assert_eq!(target_scan.candidate_count_before_pruning, 4);
    assert_eq!(target_scan.candidate_count_before_filter, 1);
    assert_eq!(target_scan.pruned_candidate_count, 3);
    assert_eq!(
        target_query.report.json()["scan_pruning_reports"][0]["strategy"]["kind"],
        "property_default_if_null_not_eq"
    );
    assert_eq!(
        target_query.report.json()["scan_pruning_reports"][0]["strategy"]["property"],
        "space_id"
    );
}

#[test]
fn graph_query_with_report_keeps_system_statement_out_of_plan_cache() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);

    let query = graph
        .query_with_report("SET system.work_priority = 'background'")
        .unwrap();

    assert_eq!(query.output.rows.len(), 1);
    assert_eq!(query.report.statement_kind, "set_system_variable");
    assert_eq!(
        query.report.execution_path,
        NowledgeMemQueryExecutionPath::OptimizedPath
    );
    assert_eq!(query.report.plan_cache_lookup, None);
    assert_eq!(query.report.plan_cache_bypass_reason, None);
    assert!(!query.report.plan_cache_cacheable);
    assert!(!query.report.plan_cache_hit);
    assert!(!query.report.plan_cache_miss);
    assert!(!query.report.plan_cache_bypassed);
    assert_eq!(
        query.report.json()["plan_cache"]["lookup"],
        serde_json::Value::Null
    );
    assert_eq!(query.report.json()["plan_cache"]["cacheable"], false);
}

#[test]
fn embedded_store_exposes_redacted_typed_slow_query_report() {
    let db = Database::new_with_config(DatabaseConfig {
        slow_query_log_threshold_micros: 0,
        slow_query_log_capacity: 4,
        ..DatabaseConfig::default()
    });
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    let mut store = NowledgeMemEmbeddedStore::new(graph, None);

    store
        .query_with_report("CREATE (:Memory {id: 'slow-secret-id', title: 'Slow Secret'})")
        .unwrap();
    store
        .query_with_report("MATCH (m:Memory {id: 'slow-secret-id'}) RETURN m.title AS title")
        .unwrap();

    let report = store.slow_query_report();
    let json = report.json();
    let encoded = json.to_string();

    assert_eq!(report.protocol, NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL);
    assert_eq!(report.mode, NowledgeMemGraphMode::ShadowReadOnly);
    assert!(report.present);
    assert!(report.ready);
    assert_eq!(report.capacity, 4);
    assert_eq!(report.threshold_micros, 0);
    assert_eq!(report.record_count, 2);
    assert_eq!(report.latest_sequence, Some(2));
    assert_eq!(report.records.len(), 2);
    assert!(report.records.iter().all(|record| record.success));
    assert!(report
        .records
        .iter()
        .all(|record| record.slow_log_candidate));
    assert_eq!(json["protocol"], NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL);
    assert_eq!(json["record_count"], 2);
    assert_eq!(json["redaction"]["query_text_copied"], false);
    assert_eq!(json["redaction"]["parameters_copied"], false);
    assert!(json["records"][0].get("query_digest").is_some());
    assert!(!encoded.contains("Slow Secret"));
    assert!(!encoded.contains("slow-secret-id"));
    assert!(!encoded.contains("MATCH (m:Memory"));
}

#[test]
fn embedded_store_handle_supports_shared_read_and_observability_access() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        slow_query_log_threshold_micros: 0,
        slow_query_log_capacity: 8,
        ..DatabaseConfig::default()
    });
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    let mut store = NowledgeMemEmbeddedStore::new(graph, None);
    store
        .query_with_report("CREATE (:Memory {id: 'shared-1', title: 'Shared State'})")
        .unwrap();
    let handle = NowledgeMemEmbeddedStoreHandle::new(store).unwrap();

    let reader = {
        let handle = handle.clone();
        thread::spawn(move || {
            for _ in 0..4 {
                let query = handle
                    .read_query(
                        "MATCH (m:Memory {id: 'shared-1'}) RETURN m.title AS title",
                        &NowledgeMemReadOptions::default(),
                    )
                    .unwrap();
                assert_eq!(query.output.rows.len(), 1);
                assert!(query.report.row_limit_enforced_before_output);
            }
        })
    };
    let observer = {
        let handle = handle.clone();
        thread::spawn(move || {
            for _ in 0..4 {
                let slow_query = handle.slow_query_report().unwrap();
                assert!(slow_query.ready);
                assert!(slow_query.record_count <= slow_query.capacity);

                let dashboard = handle
                    .readiness_dashboard(&NowledgeMemReadinessOptions::default())
                    .unwrap();
                assert!(readiness_dashboard_area(&dashboard, "slow_query").ready);
                assert!(dashboard.slow_query_record_count <= slow_query.capacity);
            }
        })
    };

    reader.join().unwrap();
    observer.join().unwrap();

    let final_slow_query = handle.slow_query_report().unwrap();
    assert!(final_slow_query.ready);
    assert_eq!(final_slow_query.latest_sequence, Some(1));
    assert_eq!(final_slow_query.record_count, 1);
}

#[test]
fn write_guard_panic_poisons_reads_without_discarding_last_good_snapshot() {
    let db = Database::new_with_config(DatabaseConfig::default());
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    let mut store = NowledgeMemEmbeddedStore::new(graph, None);
    store
        .query_with_report("CREATE (:Memory {id: 'panic-guard', title: 'Before'})")
        .unwrap();
    let handle = NowledgeMemEmbeddedStoreHandle::new(store).unwrap();

    // Baseline: the canonical read path works before anything goes wrong.
    handle
        .with_read_transaction(4096, |_transaction| Ok(()))
        .unwrap();

    let panicking_handle = handle.clone();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _guard = panicking_handle.write_store().unwrap();
        panic!("simulated write failure inside the guard");
    }));
    assert!(panicked.is_err());

    // The last known-good snapshot must survive a panicking writer: it is
    // not replaced with None, only future reads are poisoned.
    assert!(handle.published_read.lock().unwrap().is_some());
    assert!(handle
        .with_read_transaction(4096, |_transaction| Ok(()))
        .is_err());
}

#[test]
fn embedded_store_exposes_typed_operations_readiness_report() {
    let db = Database::new_with_config(DatabaseConfig {
        slow_query_log_threshold_micros: 0,
        slow_query_log_capacity: 4,
        ..DatabaseConfig::default()
    });
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    let mut store = NowledgeMemEmbeddedStore::new(graph, None);
    store
        .query_with_report("CREATE (:Memory {id: 'ops-1', title: 'Operations'})")
        .unwrap();

    let report = store
        .operations_readiness(&NowledgeMemReadinessOptions::default())
        .unwrap();
    let json = report.json();
    let encoded = json.to_string();

    assert_eq!(report.protocol, NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL);
    assert!(report.present);
    assert!(!report.ready);
    assert!(report.graph_open);
    assert!(!report.graph_read_only);
    assert!(!report.search_projection_open);
    assert_eq!(report.graph_commit_epoch, 1);
    assert_eq!(report.projection_commit_lag, 1);
    assert!(!report.projection_stale);
    assert_eq!(
        report.storage_lifecycle_action,
        NowledgeMemStorageLifecycleActionKind::OpenReadOnlyInspect
    );
    assert!(!report.storage_lifecycle_ready);
    assert!(!report.storage_recovery_ready);
    assert!(report.slow_query_ready);
    assert!(report.background_maintenance_ready);
    assert!(report
        .blocker_codes
        .contains(&"storage_recovery_not_ready".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"storage_lifecycle.storage_not_durable".to_string()));
    assert_eq!(json["protocol"], NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL);
    assert_eq!(
        json["storage_lifecycle"]["action"],
        "open_read_only_inspect"
    );
    assert_eq!(json["readiness"]["storage_lifecycle_ready"], false);
    assert_eq!(
        json["storage_lifecycle_decision"]["protocol"],
        NOWLEDGE_MEM_STORAGE_LIFECYCLE_DECISION_PROTOCOL
    );
    assert_eq!(json["redaction"]["query_text_copied"], false);
    assert!(!encoded.contains("Operations"));
    assert!(!encoded.contains("ops-1"));
}

#[test]
fn embedded_store_operations_readiness_reports_projection_staleness() {
    let root = unique_nowledge_mem_test_dir("operations_readiness_projection_stale");
    let search_path = root.join("search");
    let mut graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'ops-stale', title: 'Projection stale'})")
        .unwrap();
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

    let stale = store
        .operations_readiness(&NowledgeMemReadinessOptions::default())
        .unwrap();
    assert!(stale.search_projection_open);
    assert_eq!(stale.projection_commit_lag, 1);
    assert!(stale.projection_stale);
    assert!(stale
        .blocker_codes
        .contains(&"search_projection_stale".to_string()));

    store.catch_up_search_projection(16, 1).unwrap();
    let caught_up = store
        .operations_readiness(&NowledgeMemReadinessOptions::default())
        .unwrap();
    assert!(caught_up.search_projection_open);
    assert_eq!(caught_up.projection_commit_lag, 0);
    assert!(!caught_up.projection_stale);
    assert!(!caught_up
        .blocker_codes
        .contains(&"search_projection_stale".to_string()));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_handle_supports_operations_readiness_with_shared_reads() {
    let db = Database::new_with_config(DatabaseConfig {
        slow_query_log_threshold_micros: 0,
        slow_query_log_capacity: 8,
        ..DatabaseConfig::default()
    });
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    let mut store = NowledgeMemEmbeddedStore::new(graph, None);
    store
        .query_with_report("CREATE (:Memory {id: 'ops-shared', title: 'Shared Operations'})")
        .unwrap();
    let handle = NowledgeMemEmbeddedStoreHandle::new(store);

    let reader = {
        let handle = handle.clone().unwrap();
        thread::spawn(move || {
            let read = handle
                .read_query(
                    "MATCH (m:Memory {id: 'ops-shared'}) RETURN m.title AS title",
                    &NowledgeMemReadOptions::default(),
                )
                .unwrap();
            assert_eq!(read.output.rows.len(), 1);
        })
    };
    let observer = {
        let handle = handle.clone().unwrap();
        thread::spawn(move || {
            let report = handle
                .operations_readiness(&NowledgeMemReadinessOptions::default())
                .unwrap();
            assert!(report.slow_query_ready);
            assert!(report.background_maintenance_ready);
        })
    };

    reader.join().unwrap();
    observer.join().unwrap();
}

#[test]
fn embedded_store_handle_allows_overlapping_read_guards() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None));
    let (acquired_tx, acquired_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let first = {
        let handle = handle.clone().unwrap();
        let acquired_tx = acquired_tx.clone();
        thread::spawn(move || {
            let _guard = handle.read_store().unwrap();
            acquired_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
    };
    acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let second = {
        let handle = handle.clone().unwrap();
        thread::spawn(move || {
            let _guard = handle.read_store().unwrap();
            acquired_tx.send(()).unwrap();
        })
    };
    acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    release_tx.send(()).unwrap();

    first.join().unwrap();
    second.join().unwrap();
}

#[test]
fn embedded_store_exposes_typed_query_runtime_preflight() {
    let db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        ..DatabaseConfig::default()
    });
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    let mut store = NowledgeMemEmbeddedStore::new(graph, None);
    store
        .query_with_report("CREATE INDEX ON :Memory(id)")
        .unwrap();
    store
        .query_with_report("CREATE (:Memory {id: 'preflight-1', title: 'Preflight'})")
        .unwrap();
    for id in 0..8 {
        store
            .query_with_report(&format!(
                "CREATE (:Memory {{id: 'preflight-filler-{id}', title: 'Filler {id}'}})"
            ))
            .unwrap();
    }
    let probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .map(|route| {
            NowledgeQueryRuntimePreflightProbe::new(
                format!("probe:{route}"),
                "MATCH (m:Memory {id: 'preflight-1'}) RETURN m.title AS title",
            )
            .with_route(*route)
            .with_query_family(super::nowledge_mem_required_query_families_for_route(route)[0])
            .require_scan_pruning(1)
            .require_pruned()
            .with_max_output_rows(1)
        })
        .collect::<Vec<_>>();

    let report = store.query_runtime_preflight(&probes);
    let json = report.json();

    assert_eq!(report.protocol, NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL);
    assert!(report.ready);
    assert!(report.database_opened);
    assert!(report.redaction.ready());
    assert!(!report.redaction.rows_copied);
    assert!(!report.redaction.parameters_copied);
    assert!(!report.redaction.local_paths_copied);
    assert!(!report.redaction.raw_errors_copied);
    assert_eq!(
        report.probe_count,
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
    );
    assert_eq!(report.passed_probe_count, report.probe_count);
    assert_eq!(report.failed_probe_count, 0);
    assert!(report.required_routes_covered);
    assert!(report.route_coverage_ready);
    assert!(report.blocker_codes.is_empty());
    assert!(report.probes.iter().all(|probe| probe.ready));
    assert!(report
        .probes
        .iter()
        .all(|probe| probe.selected_plan_fingerprint.is_some()));
    assert!(report
        .probes
        .iter()
        .all(|probe| !probe.scan_pruning_reports.is_empty()));
    assert_eq!(json["protocol"], NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL);
    assert_eq!(json["ready"], true);
    assert_eq!(json["redaction"]["ready"], true);
    assert_eq!(json["redaction"]["rows_copied"], false);
    assert_eq!(json["redaction"]["parameters_copied"], false);
    assert_eq!(json["redaction"]["local_paths_copied"], false);
    assert_eq!(json["redaction"]["raw_errors_copied"], false);
    assert_eq!(json["probe_count"], probes.len());
    assert_eq!(json["probes"][0]["output_row_count"], 1);
    assert_eq!(
        json["probes"][0]["execution_profile"]["scan_pruning_report_count"],
        1
    );
    assert!(json["probes"][0]["selected_plan_fingerprint"]
        .as_str()
        .is_some_and(|fingerprint| fingerprint.contains("IndexNodeSeek")));
}

#[test]
fn embedded_store_query_runtime_preflight_redacts_failed_probe_values() {
    let db = Database::new();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    let mut store = NowledgeMemEmbeddedStore::new(graph, None);
    let probe = NowledgeQueryRuntimePreflightProbe::new(
        "probe:/graph/node-details/{node_id}",
        "MATCH (m:Memory {id: 'secret-preflight-id'}) RETURN missing(",
    )
    .with_route("/graph/node-details/{node_id}")
    .with_query_family("memory_lookup");

    let report = store.query_runtime_preflight(&[probe]);
    let json = report.json();
    let encoded = json.to_string();

    assert!(!report.ready);
    assert!(report.redaction.ready());
    assert_eq!(report.failed_probe_count, 1);
    assert_eq!(
        report.probes[0].blocker_codes,
        vec!["query_runtime_failed".to_string()]
    );
    assert_eq!(json["probes"][0]["success"], false);
    assert_eq!(json["probes"][0]["error_class"], "parse");
    assert_eq!(json["redaction"]["ready"], true);
    assert_eq!(json["redaction"]["parameters_copied"], false);
    assert_eq!(json["redaction"]["raw_errors_copied"], false);
    assert!(!encoded.contains("secret-preflight-id"));
    assert!(!encoded.contains("RETURN missing"));
}

#[test]
fn graph_read_query_reports_bounded_payload() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    graph
        .database_mut()
        .query("CREATE (:Memory {id: 'mem-read', title: 'Bounded read'})")
        .unwrap();

    let read = graph
        .read_query_with_options(
            "MATCH (m:Memory {id: 'mem-read'}) RETURN m.title AS title",
            &NowledgeMemReadOptions {
                max_rows: Some(4),
                max_estimated_payload_bytes: Some(512),
            },
        )
        .unwrap();

    assert_eq!(read.output.rows.len(), 1);
    assert_eq!(read.report.protocol, NOWLEDGE_MEM_READ_REPORT_PROTOCOL);
    assert_eq!(read.report.mode, NowledgeMemGraphMode::ShadowReadOnly);
    assert_eq!(read.report.row_count, 1);
    assert_eq!(read.report.max_rows, Some(4));
    assert_eq!(read.report.execution_row_cap, Some(5));
    assert!(read.report.estimated_payload_bytes <= 512);
    assert!(!read.report.row_budget_exceeded);
    assert!(!read.report.payload_budget_exceeded);
    assert!(read.report.row_limit_enforced_before_output);
    assert!(read.report.operator_row_cap_enabled);
    assert_eq!(read.report.blocking_operator_count, 0);
    assert!(read.report.blocking_operator_kinds.is_empty());
    assert!(read.report.intermediate_rows >= read.report.row_count);
    assert!(read.report.intermediate_payload_bytes >= read.report.output_payload_bytes);
    assert!(read.report.output_payload_bytes > 0);
    assert!(read.report.steady_resident_bytes.is_some());
    assert!(read.report.peak_resident_bytes.is_some());
    assert!(read.report.total_page_faults.is_some());
    assert_eq!(read.report.minor_page_faults.is_some(), cfg!(unix));
    assert_eq!(read.report.major_page_faults.is_some(), cfg!(unix));
    assert!(!read.report.streaming);
    assert_eq!(read.report.json()["execution_row_cap"], 5);
    assert_eq!(read.report.json()["row_limit_enforced_before_output"], true);
    assert_eq!(read.report.json()["operator_row_cap_enabled"], true);
    assert_eq!(read.report.json()["blocking_operator_count"], 0);
    assert!(read.report.json()["intermediate_rows"].as_u64().unwrap() >= 1);
    assert!(read.report.json()["output_payload_bytes"].as_u64().unwrap() > 0);
    assert!(
        read.report.json()["steady_resident_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(read.report.json()["peak_resident_bytes"].as_u64().unwrap() > 0);
    assert_eq!(read.report.json()["streaming"], false);
    assert_eq!(
        read.report.bounded_read_evidence_json()["protocol"],
        NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL
    );
    assert_eq!(
        read.report.bounded_read_evidence_json()["mode"],
        "shadow_read_only"
    );
    assert_eq!(read.report.bounded_read_evidence_json()["ready"], false);
    assert_eq!(
        read.report.bounded_read_evidence_json()["blocker_codes"],
        serde_json::json!(["missing_covered_routes", "graph_route_readiness_missing"])
    );
    let covered_routes = full_bounded_read_routes();
    let route_readiness = ready_route_readiness_summary();
    let evidence = nowledge_mem_bounded_read_evidence_json_with_route_readiness(
        &read.report,
        &covered_routes,
        Some(&route_readiness),
    );
    assert_eq!(evidence["ready"], true);
    assert_eq!(
        evidence["covered_routes"],
        serde_json::json!(covered_routes)
    );
    assert_eq!(evidence["missing_covered_routes"], serde_json::json!([]));
}

#[test]
fn graph_queries_hold_and_release_runtime_governor_permits() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);

    graph
        .query("CREATE (:Memory {id: 'admitted-mem'})")
        .unwrap();
    let read = graph
        .read_query_with_options(
            "MATCH (m:Memory {id: 'admitted-mem'}) RETURN m.id AS id",
            &NowledgeMemReadOptions {
                max_rows: Some(1),
                max_estimated_payload_bytes: Some(1024),
            },
        )
        .unwrap();

    assert_eq!(read.output.rows.len(), 1);
    let snapshot = graph.runtime_governor_snapshot();
    // Each statement admits preparation and execution separately.
    assert_eq!(snapshot.admissions, 4);
    assert_eq!(snapshot.completions, 4);
    assert_eq!(snapshot.active_foreground_tasks, 0);
    assert_eq!(snapshot.active_cpu_slots, 0);
    assert_eq!(snapshot.admitted_memory_bytes, 0);
}

#[test]
fn graph_query_rejects_before_mutation_when_runtime_memory_is_unavailable() {
    let governor = hawdb_qos::RuntimeGovernor::detect(
        hawdb_qos::RuntimeGovernorConfig {
            memory_budget_bytes: Some(1),
            ..hawdb_qos::RuntimeGovernorConfig::default()
        },
        hawdb_qos::IoConcurrencyBudget::new(1, 1),
    );
    let mut graph = NowledgeMemGraph::from_database_with_runtime_governor(
        Database::new(),
        NowledgeMemGraphMode::WritableCutover,
        governor,
    );

    let error = graph
        .query("CREATE (:Memory {id: 'rejected-mem'})")
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("runtime admission memory_saturated"));
    assert!(graph
        .database_mut()
        .query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap()
        .rows
        .is_empty());
    let snapshot = graph.runtime_governor_snapshot();
    assert_eq!(snapshot.admissions, 0);
    assert_eq!(snapshot.completions, 0);
    assert_eq!(snapshot.admission_rejections, 1);
}

#[test]
fn embedded_handle_transaction_rejects_before_mutation_without_runtime_memory() {
    let governor = hawdb_qos::RuntimeGovernor::detect(
        hawdb_qos::RuntimeGovernorConfig {
            memory_budget_bytes: Some(1),
            ..hawdb_qos::RuntimeGovernorConfig::default()
        },
        hawdb_qos::IoConcurrencyBudget::new(2, 1),
    );
    let graph = NowledgeMemGraph::from_database_with_runtime_governor(
        Database::new(),
        NowledgeMemGraphMode::WritableCutover,
        governor,
    );
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None)).unwrap();

    let error = handle
        .transaction(&[NowledgeGraphStatement {
            cypher: "CREATE (:Memory {id: 'transaction-rejected'})".to_string(),
            parameters: BTreeMap::new(),
        }])
        .unwrap_err();

    assert!(error.to_string().contains("runtime admission"));
    assert_eq!(handle.runtime_governor_snapshot().unwrap().admissions, 0);
    let mut store = handle.write_store().unwrap();
    let output = store
        .graph_mut()
        .database_mut()
        .query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn embedded_handle_query_transaction_commits_graph_and_relational_state_once() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None)).unwrap();

    handle
        .with_transaction(|transaction| {
            transaction.query("CREATE (:Memory {id: 'memory-1'})")?;
            let graph_rows = transaction
                .query("MATCH (m:Memory {id: 'memory-1'}) RETURN m.id AS id")?
                .rows;
            assert_eq!(graph_rows.len(), 1);
            transaction.query_sql(
                "CREATE TABLE anchors (\
                       anchor_id TEXT PRIMARY KEY,\
                       memory_id TEXT NOT NULL\
                     )",
            )?;
            transaction.query_sql_with_params(
                "INSERT INTO anchors (anchor_id, memory_id) VALUES ($1, $2)",
                &[
                    Value::String("anchor-1".to_string()),
                    Value::String("memory-1".to_string()),
                ],
            )?;
            Ok(())
        })
        .unwrap();

    assert_eq!(handle.runtime_status().unwrap().graph_commit_epoch, 1);
    let mut store = handle.write_store().unwrap();
    assert_eq!(
        store
            .graph_mut()
            .database_mut()
            .query("MATCH (m:Memory) RETURN m.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        store
            .graph_mut()
            .database_mut()
            .query_sql("SELECT anchor_id, memory_id FROM anchors")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn embedded_handle_query_transaction_rolls_back_callback_errors() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None)).unwrap();

    let error = handle
        .with_transaction(|transaction| {
            transaction.query("CREATE (:Memory {id: 'rolled-back'})")?;
            Err::<(), _>(crate::HawDBError::Execution(
                "injected callback failure".to_string(),
            ))
        })
        .unwrap_err();

    assert!(error.to_string().contains("injected callback failure"));
    assert_eq!(handle.runtime_status().unwrap().graph_commit_epoch, 0);
    let mut store = handle.write_store().unwrap();
    assert!(store
        .graph_mut()
        .database_mut()
        .query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn embedded_handle_checkpoints_through_admitted_maintenance() {
    let root = unique_nowledge_mem_test_dir("embedded_handle_checkpoint");
    let graph_path = root.join("graph");
    let options =
        NowledgeMemOpenOptions::graph_only(&graph_path, NowledgeMemGraphMode::WritableCutover);
    let (handle, _) = NowledgeMemEmbeddedStoreHandle::open_with_options(options).unwrap();

    handle
        .query_with_report("CREATE (:Memory {id: 'checkpointed'})")
        .unwrap();
    let before = handle.runtime_governor_snapshot().unwrap();
    handle.checkpoint().unwrap();
    let after = handle.runtime_governor_snapshot().unwrap();

    assert_eq!(after.admissions, before.admissions + 1);
    assert_eq!(after.completions, before.completions + 1);
    assert_eq!(after.active_background_tasks, 0);
    assert_eq!(after.active_blocking_tasks, 0);
    assert_eq!(after.active_cpu_slots, 0);
    assert_eq!(after.active_background_io_slots, 0);
    assert_eq!(after.admitted_memory_bytes, 0);
    drop(handle);

    let options =
        NowledgeMemOpenOptions::graph_only(&graph_path, NowledgeMemGraphMode::ShadowReadOnly);
    let (handle, _) = NowledgeMemEmbeddedStoreHandle::open_with_options(options).unwrap();
    let output = handle
        .query_with_report("MATCH (m:Memory {id: 'checkpointed'}) RETURN m.id AS id")
        .unwrap();
    assert_eq!(output.output.rows.len(), 1);
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_handle_search_holds_governed_io_and_result_budget() {
    let governor = hawdb_qos::RuntimeGovernor::detect(
        hawdb_qos::RuntimeGovernorConfig {
            memory_budget_bytes: Some(256 * 1024 * 1024),
            result_budget_bytes: 64 * 1024 * 1024,
            ..hawdb_qos::RuntimeGovernorConfig::default()
        },
        hawdb_qos::IoConcurrencyBudget::new(1, 1),
    );
    let graph = NowledgeMemGraph::from_database_with_runtime_governor(
        Database::new(),
        NowledgeMemGraphMode::WritableCutover,
        governor,
    );
    let mut projection = NowledgeMemSearchProjection::from_index(SearchIndex::default());
    projection.set_range_read_config(crate::SearchRangeReadConfig::new(
        std::num::NonZeroUsize::MIN,
        std::num::NonZeroU64::new(1024).unwrap(),
    ));
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, Some(projection)))
            .unwrap();

    let output = handle
        .search_candidates_with_report(&NowledgeMemSearchCandidateRequest::text("empty", 4))
        .unwrap();

    assert!(output.result.hits.is_empty());
    let snapshot = handle.runtime_governor_snapshot().unwrap();
    assert_eq!(snapshot.admissions, 1);
    assert_eq!(snapshot.completions, 1);
    assert_eq!(snapshot.active_foreground_io_slots, 0);
    assert_eq!(snapshot.admitted_memory_bytes, 0);
}

#[test]
fn embedded_handle_charges_out_of_core_buffers_to_runtime_admission() {
    let root = unique_nowledge_mem_test_dir("out_of_core_runtime_admission");
    let search_path = root.join("search");
    {
        let mut index = SearchIndex::open(&search_path).unwrap();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "admission".to_string(),
                title: "Out of core admission".to_string(),
                body: "Bounded buffers must be admitted".to_string(),
                embedding: None,
                source_id: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    let governor = hawdb_qos::RuntimeGovernor::detect(
        hawdb_qos::RuntimeGovernorConfig {
            memory_budget_bytes: Some(256 * 1024 * 1024),
            result_budget_bytes: 64 * 1024 * 1024,
            ..hawdb_qos::RuntimeGovernorConfig::default()
        },
        hawdb_qos::IoConcurrencyBudget::new(1, 1),
    );
    let graph = NowledgeMemGraph::from_database_with_runtime_governor(
        Database::new(),
        NowledgeMemGraphMode::ShadowReadOnly,
        governor,
    );
    let projection = NowledgeMemOutOfCoreSearchProjection::open(&search_path).unwrap();
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new_with_out_of_core_search(
            graph,
            projection,
            NowledgeMemRetrievalProjectionAdvisor::default(),
        ))
        .unwrap();

    let error = handle
        .search_candidates(&NowledgeMemSearchCandidateRequest::text("admission", 1))
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("runtime admission memory_saturated"));
    let snapshot = handle.runtime_governor_snapshot().unwrap();
    assert_eq!(snapshot.admissions, 0);
    assert_eq!(snapshot.admission_rejections, 1);
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_clamps_search_io_depth_to_runtime_governor() {
    let governor = hawdb_qos::RuntimeGovernor::detect(
        hawdb_qos::RuntimeGovernorConfig {
            memory_budget_bytes: Some(256 * 1024 * 1024),
            result_budget_bytes: 64 * 1024 * 1024,
            ..hawdb_qos::RuntimeGovernorConfig::default()
        },
        hawdb_qos::IoConcurrencyBudget::new(2, 1),
    );
    let graph = NowledgeMemGraph::from_database_with_runtime_governor(
        Database::new(),
        NowledgeMemGraphMode::WritableCutover,
        governor,
    );
    let mut projection = NowledgeMemSearchProjection::from_index(SearchIndex::default());
    projection.set_range_read_config(crate::SearchRangeReadConfig::new(
        std::num::NonZeroUsize::new(4).unwrap(),
        std::num::NonZeroU64::new(1024).unwrap(),
    ));

    let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

    assert_eq!(
        store
            .search_projection()
            .unwrap()
            .index()
            .range_read_config()
            .io_depth
            .get(),
        2
    );
}

#[test]
fn embedded_handle_read_transaction_runs_against_one_admitted_snapshot() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None)).unwrap();
    handle
        .query_with_report("CREATE (:Memory {id: 'memory-1'})")
        .unwrap();

    let (epoch, rows, memory_budget) = handle
        .with_read_transaction(64 * 1024, |transaction| {
            let epoch = transaction.commit_epoch();
            let output = transaction.query_with_params_bounded_profile(
                "MATCH (m:Memory) RETURN m.id AS id LIMIT 2",
                &BTreeMap::new(),
                Some(2),
            )?;
            let memory_budget = output
                .execution_profile
                .pipeline_memory_report
                .query_memory_budget_bytes;
            Ok((epoch, output.output.rows, memory_budget))
        })
        .unwrap();

    assert_eq!(epoch, 1);
    assert_eq!(rows.len(), 1);
    assert_eq!(memory_budget, 256 * 1024 * 1024);
    assert_eq!(
        rows[0].get("id"),
        Some(&Value::String("memory-1".to_string()))
    );
    let snapshot = handle.runtime_governor_snapshot().unwrap();
    assert_eq!(snapshot.active_foreground_tasks, 0);
    assert_eq!(snapshot.admitted_memory_bytes, 0);
}

#[test]
fn graph_streaming_query_reports_pre_execution_cancellation() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'cancelled-mem'})").unwrap();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    cancellation.cancel();
    let context = hawdb_core::RuntimeTaskContext::without_deadline(cancellation);

    let error = graph
        .read_query_with_params_streaming_context(
            "MATCH (m:Memory) RETURN m.id AS id",
            &BTreeMap::new(),
            &NowledgeMemReadOptions::default(),
            &context,
            |_| Ok(()),
        )
        .unwrap_err();

    assert!(error.to_string().contains("runtime task cancelled"));
    let snapshot = graph.runtime_governor_snapshot();
    assert_eq!(snapshot.admissions, 0);
    assert_eq!(snapshot.cancellations, 1);
}

#[test]
fn graph_read_query_streaming_collect_enforces_payload_before_host_ownership() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    graph
        .database_mut()
        .query("CREATE (:Memory {id: 'mem-stream', title: 'streamed payload'})")
        .unwrap();
    let query = "MATCH (m:Memory {id: 'mem-stream'}) RETURN m.title AS title";

    let read = graph
        .read_query_with_params_streaming_collect(
            query,
            &BTreeMap::new(),
            &NowledgeMemReadOptions {
                max_rows: Some(1),
                max_estimated_payload_bytes: Some(1024),
            },
        )
        .unwrap();
    assert!(read.report.streaming);
    assert_eq!(read.report.row_count, 1);
    assert_eq!(
        read.output.rows[0].get("title"),
        Some(&Value::String("streamed payload".to_string()))
    );

    let error = graph
        .read_query_with_params_streaming_collect(
            query,
            &BTreeMap::new(),
            &NowledgeMemReadOptions {
                max_rows: Some(1),
                max_estimated_payload_bytes: Some(1),
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("max_payload_bytes 1"));
}

#[test]
fn graph_read_query_rejects_payload_budget_excess() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    graph
        .database_mut()
        .query("CREATE (:Memory {id: 'mem-large', title: 'Large read payload'})")
        .unwrap();

    let error = graph
        .read_query_with_options(
            "MATCH (m:Memory {id: 'mem-large'}) RETURN m.title AS title",
            &NowledgeMemReadOptions {
                max_rows: Some(4),
                max_estimated_payload_bytes: Some(4),
            },
        )
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("exceeding max_estimated_payload_bytes 4"));
    assert!(matches!(error, HawDBError::ReadBudgetExceeded(cause)
            if cause.resource == ReadBudgetResource::PayloadBytes && cause.limit == 4));
}

#[test]
fn legacy_read_error_context_preserves_typed_budget_and_unknown_errors() {
    let options = NowledgeMemReadOptions {
        max_rows: Some(1),
        max_estimated_payload_bytes: Some(4),
    };
    let typed = legacy_nowledge_mem_read_error(
        HawDBError::read_budget_exceeded(
            ReadBudgetResource::PayloadBytes,
            4,
            "diagnostic without budget keywords",
        ),
        &options,
    );
    assert!(matches!(&typed, HawDBError::ReadBudgetExceeded(cause)
            if cause.resource == ReadBudgetResource::PayloadBytes && cause.limit == 4));
    assert!(typed.to_string().contains("max_estimated_payload_bytes 4"));
    let unknown = HawDBError::Execution("unrelated max_payload_bytes 4 diagnostic".into());
    assert_eq!(
        legacy_nowledge_mem_read_error(unknown.clone(), &options),
        unknown
    );
    let rows = HawDBError::read_budget_exceeded(ReadBudgetResource::Rows, 1, "rows");
    assert_eq!(legacy_nowledge_mem_read_error(rows.clone(), &options), rows);
}

#[test]
fn read_transaction_rejects_rows_above_configured_limit() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(1),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'mem-limit-1', title: 'Limit one'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'mem-limit-2', title: 'Limit two'})")
        .unwrap();

    let error = db
        .begin_read_transaction()
        .unwrap()
        .query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap_err();

    assert!(error.to_string().contains("more than 1 rows"));
}

#[test]
fn graph_read_query_rejects_rows_before_returning_oversized_output() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    graph
        .database_mut()
        .query("CREATE (:Memory {id: 'mem-read-limit-1', title: 'Limit one'})")
        .unwrap();
    graph
        .database_mut()
        .query("CREATE (:Memory {id: 'mem-read-limit-2', title: 'Limit two'})")
        .unwrap();

    let error = graph
        .read_query_with_options(
            "MATCH (m:Memory) RETURN m.id AS id",
            &NowledgeMemReadOptions {
                max_rows: Some(1),
                max_estimated_payload_bytes: Some(4096),
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("more than 1 rows"));
}

#[test]
fn graph_read_query_allows_cypher_limit_within_row_budget() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    graph
        .database_mut()
        .query("CREATE (:Memory {id: 'mem-read-limit-pass-1', title: 'Limit one'})")
        .unwrap();
    graph
        .database_mut()
        .query("CREATE (:Memory {id: 'mem-read-limit-pass-2', title: 'Limit two'})")
        .unwrap();

    let read = graph
        .read_query_with_options(
            "MATCH (m:Memory) RETURN m.id AS id LIMIT 1",
            &NowledgeMemReadOptions {
                max_rows: Some(1),
                max_estimated_payload_bytes: Some(4096),
            },
        )
        .unwrap();

    assert_eq!(read.output.rows.len(), 1);
    assert_eq!(read.report.row_count, 1);
    assert!(!read.report.row_budget_exceeded);
}

#[test]
fn graph_read_query_reports_blocking_operators() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    graph
        .database_mut()
        .query("CREATE (:Memory {id: 'mem-sort-profile-1', title: 'B'})")
        .unwrap();
    graph
        .database_mut()
        .query("CREATE (:Memory {id: 'mem-sort-profile-2', title: 'A'})")
        .unwrap();

    let read = graph
        .read_query_with_options(
            "MATCH (m:Memory) RETURN m.title AS title ORDER BY title LIMIT 1",
            &NowledgeMemReadOptions {
                max_rows: Some(4),
                max_estimated_payload_bytes: Some(4096),
            },
        )
        .unwrap();

    assert_eq!(read.output.rows.len(), 1);
    assert_eq!(read.report.blocking_operator_kinds, vec!["TopNExec"]);
    assert_eq!(read.report.blocking_operator_count, 1);
    assert_eq!(read.report.json()["blocking_operator_kinds"][0], "TopNExec");
}

#[test]
fn embedded_store_read_query_does_not_require_search_projection() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'mem-store-read', title: 'Store read'})")
        .unwrap();
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let read = store
        .read_query_with_options(
            "MATCH (m:Memory {id: 'mem-store-read'}) RETURN m.title AS title",
            &NowledgeMemReadOptions::default(),
        )
        .unwrap();

    assert_eq!(read.output.rows.len(), 1);
    assert_eq!(read.report.row_count, 1);
    assert!(!read.report.row_budget_exceeded);
    assert!(!read.report.payload_budget_exceeded);
}

#[test]
fn embedded_store_library_readiness_fails_closed_without_required_evidence() {
    let db = Database::new();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let readiness = store
        .library_readiness_json(&NowledgeMemReadinessOptions::default())
        .unwrap();

    assert_eq!(
        readiness["protocol"],
        NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL
    );
    assert_eq!(readiness["present"], true);
    assert_eq!(readiness["ready"], false);
    assert_eq!(readiness["mode"], "shadow_read_only");
    assert_eq!(readiness["production_path"]["ready"], true);
    assert_eq!(readiness["production_path"]["in_process"], true);
    assert_eq!(readiness["production_path"]["cli_required"], false);
    assert_eq!(
        readiness["production_path"]["env_control_plane_required"],
        false
    );
    assert_eq!(
        readiness["production_path"]["spawned_helper_required"],
        false
    );
    assert_eq!(readiness["bounded_read_evidence"]["present"], false);
    assert_eq!(
        readiness["bounded_read_evidence"]["blocker_codes"],
        serde_json::json!(["bounded_read_probe_missing"])
    );
    assert_eq!(
        readiness["search_projection_evidence"]["blocker_codes"],
        serde_json::json!(["search_projection_not_configured"])
    );
    assert_eq!(
        readiness["search_projection_shadow_evidence"]["blocker_codes"],
        serde_json::json!(["primary_search_projection_probe_missing"])
    );
    assert_eq!(
        readiness["search_candidate_shadow_evidence"]["blocker_codes"],
        serde_json::json!(["search_candidate_shadow_evidence_missing"])
    );
    assert_eq!(
        readiness["query_family_evidence"]["blocker_codes"],
        serde_json::json!(["query_family_evidence_missing"])
    );
    assert!(readiness["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "bounded_read_evidence_not_ready"));
    assert_eq!(readiness["readiness_by_area"]["graph"]["ready"], true);
    assert_eq!(readiness["readiness_by_area"]["query"]["ready"], false);
    assert_eq!(
        readiness["readiness_by_area"]["query"]["blocker_codes"],
        serde_json::json!(["bounded_read_probe_missing"])
    );
    assert_eq!(readiness["readiness_by_area"]["storage"]["ready"], false);
    assert_eq!(readiness["readiness_by_area"]["background"]["ready"], false);
    assert_eq!(readiness["production_resource_profile"]["present"], false);
    assert!(readiness["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "production_resource_profile_not_ready"));
    assert_eq!(
        readiness["readiness_by_area"]["background"]["blocker_codes"],
        serde_json::json!(["no_candidates", "no_ranked_work"])
    );
    assert_eq!(
        readiness["readiness_by_area"]["query_family"]["ready"],
        false
    );
    assert_eq!(
        readiness["readiness_by_area"]["query_family"]["blocker_codes"],
        serde_json::json!(["query_family_evidence_missing"])
    );
    assert_eq!(
        readiness["graph_route_readiness"]["blocker_codes"],
        serde_json::json!(["graph_route_readiness_missing"])
    );
    assert_eq!(
        readiness["readiness_by_area"]["graph_route"]["ready"],
        false
    );
    assert_eq!(
        readiness["readiness_by_area"]["graph_route"]["blocker_codes"],
        serde_json::json!(["graph_route_readiness_missing"])
    );
    assert_eq!(
        readiness["search_route_ownership"]["blocker_codes"],
        serde_json::json!(["search_route_ownership_missing"])
    );
    assert_eq!(
        readiness["active_search_route_ownership"]["blocker_codes"],
        serde_json::json!(["active_search_route_ownership_missing"])
    );
    assert_eq!(
        readiness["readiness_by_area"]["search_route_ownership"]["ready"],
        false
    );
    assert_eq!(
        readiness["readiness_by_area"]["search_route_ownership"]["blocker_codes"],
        serde_json::json!([
            "search_route_ownership_missing",
            "active_search_route_ownership_missing",
            "active_search_route_readiness_missing"
        ])
    );
    assert_eq!(
        readiness["readiness_by_area"]["search_projection"]["ready"],
        false
    );
    assert_eq!(
        readiness["readiness_by_area"]["search_projection_shadow"]["ready"],
        false
    );
    assert_eq!(
        readiness["readiness_by_area"]["search_candidate_shadow"]["ready"],
        false
    );
    assert_eq!(
        readiness["readiness_by_area"]["search_candidate_shadow"]["blocker_codes"],
        serde_json::json!(["search_candidate_shadow_evidence_missing"])
    );
    assert_eq!(
        readiness["workload_fixture_evidence"]["blocker_codes"],
        serde_json::json!(["workload_fixture_evidence_missing"])
    );
    assert_eq!(
        readiness["readiness_by_area"]["workload_fixture"]["ready"],
        false
    );
    assert_eq!(
        readiness["readiness_by_area"]["workload_fixture"]["blocker_codes"],
        serde_json::json!(["workload_fixture_evidence_missing"])
    );
    assert_eq!(readiness["ready_area_count"], 1);
    assert_eq!(readiness["blocked_area_count"], 10);
    assert!(!readiness.to_string().contains("redacted"));
}

#[test]
fn embedded_store_library_readiness_consumes_typed_production_resource_profile() {
    let path = unique_nowledge_mem_test_dir("production_resource_profile");
    let cache_capacity = 1024;
    let config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        segment_cache_capacity_bytes: cache_capacity,
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config.clone()).unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    for id in 0..32 {
        transaction
            .query_with_params(
                "CREATE (:Memory {id: $id, body: $body})",
                &BTreeMap::from([
                    ("id".to_string(), Value::Int(id)),
                    ("body".to_string(), Value::String("x".repeat(1024))),
                ]),
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open_with_config(&path, config).unwrap();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    let store = NowledgeMemEmbeddedStore::new(graph, None);
    let statement = NowledgeGraphStatement {
        cypher: "MATCH (m:Memory) RETURN m.id AS memory_id".to_string(),
        parameters: BTreeMap::new(),
    };
    let identity = crate::ProductionQualificationIdentity {
        source_revision: "test-revision".to_string(),
        rust_toolchain: "test-toolchain".to_string(),
        target_os: std::env::consts::OS.to_string(),
        target_arch: std::env::consts::ARCH.to_string(),
        enabled_features: Vec::new(),
        durable_format_version: 1,
        schema_version: 1,
        configuration_digest: "test-config".to_string(),
        deployment_profile: "test-production-replica".to_string(),
        dataset_fingerprint: "test-dataset".to_string(),
        canonical_graph_commit_epoch: store.graph.database().commit_epoch().unwrap(),
        policy_version: crate::PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    let profile = store
        .production_resource_profile(
            &statement,
            StorageResourceProfileLimits {
                min_canonical_artifact_bytes: 4096,
                max_steady_resident_bytes: u64::MAX,
                max_peak_resident_bytes: u64::MAX,
                max_total_page_faults: Some(u64::MAX),
                max_minor_page_faults: cfg!(unix).then_some(u64::MAX),
                max_major_page_faults: cfg!(unix).then_some(u64::MAX),
                max_intermediate_rows: 1024,
                max_intermediate_payload_bytes: 1024 * 1024,
                max_output_rows: 64,
                max_output_payload_bytes: 1024 * 1024,
                require_fully_streamed: true,
            },
            crate::ProductionEvidenceBinding {
                identity: identity.clone(),
                generated_at_unix_seconds: 1,
            },
            identity,
        )
        .unwrap();

    assert!(
        profile.resource_ready,
        "unexpected blockers: {:?}",
        profile.blocker_codes
    );
    assert!(profile.production_ready());
    assert!(profile.after.canonical_artifact_bytes > cache_capacity);
    let readiness = store
        .library_readiness(&NowledgeMemReadinessOptions {
            production_resource_profile: Some(profile),
            ..NowledgeMemReadinessOptions::default()
        })
        .unwrap();

    assert_eq!(readiness.production_resource_profile["ready"], true);
    assert!(
        readiness.readiness_by_area.storage.ready,
        "unexpected storage blockers: {:?}",
        readiness.readiness_by_area.storage.blocker_codes
    );
    assert!(!readiness
        .blocker_codes
        .iter()
        .any(|code| code == "production_resource_profile_not_ready"));

    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn embedded_store_exposes_typed_library_readiness_report() {
    let db = Database::new();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let report = store
        .library_readiness(&NowledgeMemReadinessOptions::default())
        .unwrap();
    let json = report.json();

    assert_eq!(report.protocol, NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL);
    assert!(report.present);
    assert!(!report.ready);
    assert_eq!(report.mode, NowledgeMemGraphMode::ShadowReadOnly);
    assert!(report.redaction.ready());
    assert!(!report.redaction.query_text_copied);
    assert!(!report.redaction.parameters_copied);
    assert!(!report.redaction.local_paths_copied);
    assert!(report.graph_open);
    assert!(!report.graph_read_only);
    let areas = report.areas();
    assert_eq!(areas.len(), 11);
    assert_eq!(report.ready_area_count, 1);
    assert_eq!(report.blocked_area_count, 10);
    assert!(report.readiness_by_area.graph.ready);
    assert!(!report.readiness_by_area.query.ready);
    assert_eq!(
        report.readiness_by_area.query.blocker_codes,
        vec!["bounded_read_probe_missing".to_string()]
    );
    assert!(!report.readiness_by_area.graph_route.ready);
    assert_eq!(
        report.readiness_by_area.graph_route.blocker_codes,
        vec!["graph_route_readiness_missing".to_string()]
    );
    assert!(!report.readiness_by_area.search_route_ownership.ready);
    assert_eq!(
        report
            .readiness_by_area
            .search_route_ownership
            .blocker_codes,
        vec![
            "search_route_ownership_missing".to_string(),
            "active_search_route_ownership_missing".to_string(),
            "active_search_route_readiness_missing".to_string()
        ]
    );
    assert!(!report.readiness_by_area.workload_fixture.ready);
    assert_eq!(
        report.readiness_by_area.workload_fixture.blocker_codes,
        vec!["workload_fixture_evidence_missing".to_string()]
    );
    let graph_area = areas
        .iter()
        .find(|area| area.name == "graph")
        .expect("graph readiness area");
    let query_area = areas
        .iter()
        .find(|area| area.name == "query")
        .expect("query readiness area");
    assert!(graph_area.ready);
    assert!(!query_area.ready);
    assert_eq!(
        query_area.blocker_codes,
        vec!["bounded_read_probe_missing".to_string()]
    );
    assert!(report
        .blocker_codes
        .iter()
        .any(|code| code == "bounded_read_evidence_not_ready"));
    assert!(report
        .blocker_codes
        .iter()
        .any(|code| code == "graph_route_readiness_not_ready"));
    assert!(report
        .blocker_codes
        .iter()
        .any(|code| code == "search_route_ownership_not_ready"));
    assert!(report
        .blocker_codes
        .iter()
        .any(|code| code == "active_search_route_ownership_not_ready"));
    assert!(report
        .blocker_codes
        .iter()
        .any(|code| code == "active_search_route_readiness_not_ready"));
    assert_eq!(json["ready"], false);
    assert_eq!(json["redaction"]["ready"], true);
    assert_eq!(json["redaction"]["query_text_copied"], false);
    assert_eq!(json["redaction"]["parameters_copied"], false);
    assert_eq!(json["redaction"]["local_paths_copied"], false);
    assert_eq!(
        json["blocker_codes"],
        serde_json::json!(report.blocker_codes)
    );
    assert_eq!(json["areas"].as_array().unwrap().len(), areas.len());
    assert_eq!(
        json["areas"][0],
        serde_json::json!({
            "name": "graph",
            "ready": true,
            "blocker_codes": [],
        })
    );
    assert_eq!(json["graph"]["read_only"], false);
    assert_eq!(
        json["bounded_read_evidence"]["blocker_codes"],
        serde_json::json!(["bounded_read_probe_missing"])
    );
    assert_eq!(
        json["search_route_ownership"]["blocker_codes"],
        serde_json::json!(["search_route_ownership_missing"])
    );
    assert_eq!(
        json["active_search_route_ownership"]["blocker_codes"],
        serde_json::json!(["active_search_route_ownership_missing"])
    );
    assert_eq!(
        json["active_search_route_readiness"]["blocker_codes"],
        serde_json::json!(["active_search_route_readiness_missing"])
    );
}

#[test]
fn embedded_store_library_readiness_requires_active_search_route_read_evidence() {
    let db = Database::new();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let readiness = store
        .library_readiness_json(&NowledgeMemReadinessOptions {
            search_route_ownership: Some(ready_search_route_ownership()),
            active_search_route_ownership: Some(ready_active_search_route_ownership()),
            ..NowledgeMemReadinessOptions::default()
        })
        .unwrap();

    assert_eq!(
        readiness["search_route_ownership"]["ready"],
        serde_json::json!(true)
    );
    assert_eq!(
        readiness["active_search_route_ownership"]["ready"],
        serde_json::json!(true)
    );
    assert_eq!(
        readiness["active_search_route_readiness"]["blocker_codes"],
        serde_json::json!(["active_search_route_readiness_missing"])
    );
    assert_eq!(
        readiness["readiness_by_area"]["search_route_ownership"]["ready"],
        serde_json::json!(false)
    );
    assert_eq!(
        readiness["readiness_by_area"]["search_route_ownership"]["blocker_codes"],
        serde_json::json!(["active_search_route_readiness_missing"])
    );
    assert!(readiness["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "active_search_route_readiness_not_ready"));
}

#[test]
fn embedded_store_library_readiness_accepts_typed_workload_fixture_evidence() {
    let db = Database::new();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);
    let workload_fixture =
        nowledge_graph_route_workload_fixture_report(NowledgeGraphRouteWorkloadFixtureOptions {
            capture_physical_plan: true,
            include_bounded_expansion_probes: true,
            ..NowledgeGraphRouteWorkloadFixtureOptions::default()
        })
        .unwrap();

    let report = store
        .library_readiness(&NowledgeMemReadinessOptions {
            workload_fixture_evidence: Some(workload_fixture.clone()),
            ..NowledgeMemReadinessOptions::default()
        })
        .unwrap();
    let json = report.json();

    assert!(workload_fixture.ready);
    assert!(report.readiness_by_area.workload_fixture.ready);
    assert_eq!(
        report.readiness_by_area.workload_fixture.blocker_codes,
        Vec::<String>::new()
    );
    assert!(report
        .blocker_codes
        .iter()
        .all(|code| code != "workload_fixture_evidence_not_ready"));
    assert_eq!(json["workload_fixture_evidence"]["ready"], true);
    assert_eq!(json["readiness_by_area"]["workload_fixture"]["ready"], true);
    assert_eq!(
        json["workload_fixture_evidence"]["failed_query_count"],
        serde_json::json!(0)
    );
    assert_eq!(
        json["workload_fixture_evidence"]["failed_bounded_expansion_probe_count"],
        serde_json::json!(0)
    );
    assert_eq!(
        json["workload_fixture_evidence"]["failed_search_metadata_probe_count"],
        serde_json::json!(0)
    );
    assert_eq!(
        json["workload_fixture_evidence"]["failed_graph_rag_probe_count"],
        serde_json::json!(0)
    );
    assert_eq!(
        json["workload_fixture_evidence"]["failed_source_projection_probe_count"],
        serde_json::json!(0)
    );
    assert_eq!(
        json["workload_fixture_evidence"]["graph_rag_reports"][0]["ready"],
        true
    );
    assert_eq!(
        json["workload_fixture_evidence"]["graph_rag_reports"][0]["parameter_requirement_count"],
        serde_json::json!(1)
    );
    assert_eq!(
        json["workload_fixture_evidence"]["source_projection_reports"][0]["ready"],
        true
    );
    assert_eq!(
        json["workload_fixture_evidence"]["source_projection_reports"][0]
            ["too_small_batch_failed_closed"],
        true
    );
    assert_eq!(
        json["workload_fixture_evidence"]["source_projection_reports"][0]
            ["indexed_source_document_ready"],
        true
    );
}

#[test]
fn library_readiness_rejects_workload_fixture_without_graph_rag_probe() {
    let workload_fixture = nowledge_graph_route_workload_fixture_report(
        NowledgeGraphRouteWorkloadFixtureOptions::default(),
    )
    .unwrap();
    let mut evidence = workload_fixture.json();
    evidence
        .as_object_mut()
        .unwrap()
        .remove("graph_rag_probe_count");
    evidence
        .as_object_mut()
        .unwrap()
        .remove("failed_graph_rag_probe_count");
    evidence
        .as_object_mut()
        .unwrap()
        .remove("graph_rag_reports");

    let blockers =
        hawdb_readiness::library_readiness::workload_fixture_readiness_blocker_codes(&evidence);

    assert!(blockers.contains(&"workload_fixture_graph_rag_not_ready".to_string()));
    assert!(blockers.contains(&"workload_fixture_graph_rag_probe_missing".to_string()));
}

#[test]
fn library_readiness_rejects_workload_fixture_without_source_projection_probe() {
    let workload_fixture = nowledge_graph_route_workload_fixture_report(
        NowledgeGraphRouteWorkloadFixtureOptions::default(),
    )
    .unwrap();
    let mut evidence = workload_fixture.json();
    evidence
        .as_object_mut()
        .unwrap()
        .remove("source_projection_probe_count");
    evidence
        .as_object_mut()
        .unwrap()
        .remove("failed_source_projection_probe_count");
    evidence
        .as_object_mut()
        .unwrap()
        .remove("source_projection_reports");

    let blockers =
        hawdb_readiness::library_readiness::workload_fixture_readiness_blocker_codes(&evidence);

    assert!(blockers.contains(&"workload_fixture_source_projection_not_ready".to_string()));
    assert!(blockers.contains(&"workload_fixture_source_projection_probe_missing".to_string()));
}

#[test]
fn embedded_store_library_readiness_recomputes_search_candidate_shadow_evidence() {
    let db = Database::new();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let readiness = store
        .library_readiness_json(&NowledgeMemReadinessOptions {
            search_candidate_shadow_evidence: Some(serde_json::json!({
                "ready": true,
                "blocker_codes": []
            })),
            ..NowledgeMemReadinessOptions::default()
        })
        .unwrap();

    assert_eq!(
        readiness["readiness_by_area"]["search_candidate_shadow"]["ready"],
        false
    );
    let blocker_codes = readiness["readiness_by_area"]["search_candidate_shadow"]["blocker_codes"]
        .as_array()
        .unwrap();
    assert!(blocker_codes
        .iter()
        .any(|code| code == "search_candidate_shadow_protocol_mismatch"));
    assert!(blocker_codes
        .iter()
        .any(|code| code == "search_candidate_counts_not_ready"));
    assert!(readiness["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "search_candidate_shadow_evidence_not_ready"));
}

#[test]
fn embedded_store_library_readiness_recomputes_search_projection_evidence() {
    let db = Database::new();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let readiness = store
        .library_readiness_json(&NowledgeMemReadinessOptions {
            search_projection_evidence: Some(serde_json::json!({
                "ready": true,
                "blocker_codes": []
            })),
            ..NowledgeMemReadinessOptions::default()
        })
        .unwrap();

    assert_eq!(
        readiness["readiness_by_area"]["search_projection"]["ready"],
        false
    );
    let blocker_codes = readiness["readiness_by_area"]["search_projection"]["blocker_codes"]
        .as_array()
        .unwrap();
    assert!(blocker_codes
        .iter()
        .any(|code| code == "search_projection_protocol_mismatch"));
    assert!(blocker_codes
        .iter()
        .any(|code| code == "search_projection_tables_not_ready"));
    assert!(blocker_codes
        .iter()
        .any(|code| code == "search_projection_document_identity_not_ready"));
    assert!(readiness["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "search_projection_evidence_not_ready"));
}

#[test]
fn embedded_store_library_readiness_recomputes_search_projection_shadow_evidence() {
    let db = Database::new();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let readiness = store
        .library_readiness_json(&NowledgeMemReadinessOptions {
            search_projection_shadow_evidence: Some(serde_json::json!({
                "ready": true,
                "blocker_codes": []
            })),
            ..NowledgeMemReadinessOptions::default()
        })
        .unwrap();

    assert_eq!(
        readiness["readiness_by_area"]["search_projection_shadow"]["ready"],
        false
    );
    let blocker_codes = readiness["readiness_by_area"]["search_projection_shadow"]["blocker_codes"]
        .as_array()
        .unwrap();
    assert!(blocker_codes
        .iter()
        .any(|code| code == "search_projection_shadow_protocol_mismatch"));
    assert!(blocker_codes
        .iter()
        .any(|code| code == "search_projection_shadow_document_identity_not_ready"));
    assert!(blocker_codes.iter().any(|code| {
        code == hawdb_readiness::library_readiness::SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY
    }));
    assert!(readiness["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "search_projection_shadow_evidence_not_ready"));
}

#[test]
fn embedded_store_exposes_compact_readiness_dashboard() {
    let db = Database::new_with_config(DatabaseConfig {
        slow_query_log_threshold_micros: 0,
        slow_query_log_capacity: 4,
        ..DatabaseConfig::default()
    });
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    let mut store = NowledgeMemEmbeddedStore::new(graph, None);
    store
        .query_with_report("CREATE (:Memory {id: 'dashboard-secret', title: 'Dashboard Secret'})")
        .unwrap();

    let dashboard = store
        .readiness_dashboard(&NowledgeMemReadinessOptions::default())
        .unwrap();
    let json = dashboard.json();
    let encoded = json.to_string();

    assert_eq!(
        dashboard.protocol,
        NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL
    );
    assert_eq!(dashboard.mode, NowledgeMemGraphMode::ShadowReadOnly);
    assert!(!dashboard.ready);
    assert_eq!(dashboard.area_count, 12);
    assert_eq!(dashboard.ready_area_count, 3);
    assert_eq!(dashboard.blocked_area_count, 9);
    assert_eq!(
        dashboard.storage_lifecycle_action,
        NowledgeMemStorageLifecycleActionKind::OpenReadOnlyInspect
    );
    assert!(!dashboard.storage_lifecycle_ready);
    assert!(dashboard.slow_query_ready);
    assert_eq!(dashboard.slow_query_record_count, 1);
    assert!(readiness_dashboard_area(&dashboard, "graph").ready);
    assert!(readiness_dashboard_area(&dashboard, "background").ready);
    assert_eq!(
        readiness_dashboard_area(&dashboard, "query").blocker_codes,
        vec!["bounded_read_probe_missing".to_string()]
    );
    assert_eq!(
        readiness_dashboard_area(&dashboard, "query_family").blocker_codes,
        vec!["query_family_evidence_missing".to_string()]
    );
    assert_eq!(
        readiness_dashboard_area(&dashboard, "graph_route").blocker_codes,
        vec!["graph_route_readiness_missing".to_string()]
    );
    assert_eq!(
        readiness_dashboard_area(&dashboard, "storage").blocker_codes,
        vec![
            "storage_recovery_not_ready".to_string(),
            "production_resource_profile_has_blockers".to_string(),
            "production_resource_profile_identity_invalid".to_string(),
            "production_resource_profile_intermediate_payload_bytes_invalid".to_string(),
            "production_resource_profile_intermediate_rows_invalid".to_string(),
            "production_resource_profile_metric_capabilities_invalid".to_string(),
            "production_resource_profile_missing".to_string(),
            "production_resource_profile_not_ready".to_string(),
            "production_resource_profile_output_payload_bytes_invalid".to_string(),
            "production_resource_profile_output_rows_invalid".to_string(),
            "production_resource_profile_peak_resident_bytes_invalid".to_string(),
            "production_resource_profile_resident_growth_missing".to_string(),
            "production_resource_profile_resource_not_ready".to_string(),
            "production_resource_profile_steady_resident_bytes_invalid".to_string(),
            "production_resource_profile_storage_budget_invalid".to_string(),
            "production_resource_profile_streaming_invalid".to_string(),
            "production_resource_profile_total_page_faults_invalid".to_string()
        ]
    );
    assert_eq!(
        readiness_dashboard_area(&dashboard, "search_projection").blocker_codes,
        vec!["search_projection_not_configured".to_string()]
    );
    assert_eq!(
        readiness_dashboard_area(&dashboard, "search_projection_shadow").blocker_codes,
        vec!["primary_search_projection_probe_missing".to_string()]
    );
    assert_eq!(
        readiness_dashboard_area(&dashboard, "search_candidate_shadow").blocker_codes,
        vec!["search_candidate_shadow_evidence_missing".to_string()]
    );
    assert_eq!(
        readiness_dashboard_area(&dashboard, "workload_fixture").blocker_codes,
        vec!["workload_fixture_evidence_missing".to_string()]
    );
    assert!(readiness_dashboard_area(&dashboard, "slow_query").ready);
    assert_eq!(json["protocol"], NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL);
    assert_eq!(
        json["storage_lifecycle"]["action"],
        "open_read_only_inspect"
    );
    assert_eq!(json["storage_lifecycle"]["ready"], false);
    assert_eq!(json["redaction"]["query_text_copied"], false);
    assert_eq!(json["redaction"]["parameters_copied"], false);
    assert_eq!(json["redaction"]["local_paths_copied"], false);
    assert!(!encoded.contains("Dashboard Secret"));
    assert!(!encoded.contains("dashboard-secret"));
}

#[test]
fn storage_recovery_report_exposes_typed_readiness_summary() {
    let report = NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
        open_timings: Default::default(),
        durable: true,
        recovery_mode: RecoveryMode::Strict,
        max_wal_replay_entries: Some(16),
        max_wal_replay_bytes: Some(4096),
        max_wal_record_bytes: Some(1024),
        checkpoint_epoch: Some(3),
        checkpoint_commit_epoch: Some(11),
        wal_present: true,
        wal_replay_start_lsn: Some(4),
        next_lsn_after_replay: Some(7),
        replayed_wal_entries: 3,
        torn_tail_ignored: false,
        torn_tail_reason: None,
        recovered_commit_epoch: 14,
        ..StorageRecoveryReport::default()
    });
    let json = report.json();

    assert_eq!(report.protocol, "hawdb-storage-recovery-report");
    assert!(report.present);
    assert!(report.ready);
    assert!(report.durable_recovery_observed);
    assert!(report.checkpoint_boundary_present);
    assert!(report.wal_replay_bounded);
    assert!(report.replay_boundary_consistent);
    assert!(report.torn_tail_clean);
    assert!(report.blocker_codes.is_empty());
    assert_eq!(json["ready"], true);
    assert_eq!(json["readiness"]["wal_replay_bounded"], true);
    assert_eq!(json["readiness"]["replay_boundary_consistent"], true);
    assert_eq!(json["max_wal_replay_entries"], 16);
}

#[test]
fn storage_recovery_report_rejects_inconsistent_open_timings() {
    let report = NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
        open_timings: StorageOpenTimings {
            durable_manifest_open_micros: 2,
            total_open_micros: 1,
            ..StorageOpenTimings::default()
        },
        durable: true,
        recovery_mode: RecoveryMode::Strict,
        max_wal_replay_entries: Some(16),
        max_wal_replay_bytes: Some(4096),
        max_wal_record_bytes: Some(1024),
        checkpoint_epoch: Some(3),
        checkpoint_commit_epoch: Some(11),
        wal_present: true,
        wal_replay_start_lsn: Some(4),
        next_lsn_after_replay: Some(7),
        replayed_wal_entries: 3,
        torn_tail_ignored: false,
        torn_tail_reason: None,
        recovered_commit_epoch: 14,
        ..StorageRecoveryReport::default()
    });

    assert!(!report.open_timing_consistent);
    assert!(!report.ready);
    assert_eq!(
        report.blocker_codes,
        vec!["storage_open_timing_inconsistent".to_string()]
    );
    assert_eq!(report.json()["readiness"]["open_timing_consistent"], false);
}

#[test]
fn storage_lifecycle_decision_reports_ready_for_clean_recovery() {
    let recovery = NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
        open_timings: Default::default(),
        durable: true,
        recovery_mode: RecoveryMode::Strict,
        max_wal_replay_entries: Some(16),
        max_wal_replay_bytes: Some(4096),
        max_wal_record_bytes: Some(1024),
        checkpoint_epoch: Some(3),
        checkpoint_commit_epoch: Some(11),
        wal_present: true,
        wal_replay_start_lsn: Some(4),
        next_lsn_after_replay: Some(7),
        replayed_wal_entries: 3,
        torn_tail_ignored: false,
        torn_tail_reason: None,
        recovered_commit_epoch: 14,
        ..StorageRecoveryReport::default()
    });

    let decision = NowledgeMemStorageLifecycleDecision::from_storage_recovery(recovery);
    let json = decision.json();

    assert_eq!(
        decision.protocol,
        NOWLEDGE_MEM_STORAGE_LIFECYCLE_DECISION_PROTOCOL
    );
    assert_eq!(
        decision.action,
        NowledgeMemStorageLifecycleActionKind::Ready
    );
    assert!(decision.ready_for_mem_lifecycle);
    assert!(decision.storage_recovery_ready);
    assert!(!decision.checkpoint_required);
    assert!(!decision.repair_required);
    assert!(!decision.quarantine_required);
    assert!(!decision.read_only_inspection_required);
    assert!(decision.blocker_codes.is_empty());
    assert_eq!(json["action"], "ready");
    assert_eq!(json["ready_for_mem_lifecycle"], true);
}

#[test]
fn storage_lifecycle_decision_recommends_wal_tail_repair() {
    let recovery = NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
        open_timings: Default::default(),
        durable: true,
        recovery_mode: RecoveryMode::DoctorRepairTornTail,
        max_wal_replay_entries: Some(16),
        max_wal_replay_bytes: Some(4096),
        max_wal_record_bytes: Some(1024),
        checkpoint_epoch: Some(3),
        checkpoint_commit_epoch: Some(11),
        wal_present: true,
        wal_replay_start_lsn: Some(4),
        next_lsn_after_replay: Some(7),
        replayed_wal_entries: 3,
        torn_tail_ignored: true,
        torn_tail_reason: Some("partial wal entry".to_string()),
        recovered_commit_epoch: 14,
        ..StorageRecoveryReport::default()
    });

    let decision = NowledgeMemStorageLifecycleDecision::from_storage_recovery(recovery);

    assert_eq!(
        decision.action,
        NowledgeMemStorageLifecycleActionKind::RepairWalTail
    );
    assert!(!decision.ready_for_mem_lifecycle);
    assert!(decision.repair_required);
    assert_eq!(
        decision.blocker_codes,
        vec![
            "torn_tail_observed".to_string(),
            "wal_tail_repair_required".to_string()
        ]
    );
    assert_eq!(decision.json()["action"], "repair_wal_tail");
}

#[test]
fn storage_lifecycle_decision_fails_closed_for_in_memory_storage() {
    let db = Database::new();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let decision = store.storage_lifecycle_decision().unwrap();
    let json = store.storage_lifecycle_decision_json().unwrap();

    assert_eq!(
        decision.action,
        NowledgeMemStorageLifecycleActionKind::OpenReadOnlyInspect
    );
    assert!(!decision.ready_for_mem_lifecycle);
    assert!(decision.read_only_inspection_required);
    assert!(decision
        .blocker_codes
        .contains(&"durable_recovery_not_observed".to_string()));
    assert!(decision
        .blocker_codes
        .contains(&"storage_not_durable".to_string()));
    assert_eq!(json["action"], "open_read_only_inspect");
}

#[test]
fn storage_lifecycle_decision_recommends_checkpoint_for_missing_boundary() {
    let recovery = NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
        open_timings: Default::default(),
        durable: true,
        recovery_mode: RecoveryMode::Strict,
        max_wal_replay_entries: Some(16),
        max_wal_replay_bytes: Some(4096),
        max_wal_record_bytes: Some(1024),
        checkpoint_epoch: None,
        checkpoint_commit_epoch: None,
        wal_present: true,
        wal_replay_start_lsn: Some(4),
        next_lsn_after_replay: Some(4),
        replayed_wal_entries: 0,
        torn_tail_ignored: false,
        torn_tail_reason: None,
        recovered_commit_epoch: 4,
        ..StorageRecoveryReport::default()
    });

    let decision = NowledgeMemStorageLifecycleDecision::from_storage_recovery(recovery);

    assert_eq!(
        decision.action,
        NowledgeMemStorageLifecycleActionKind::RunCheckpoint
    );
    assert!(!decision.ready_for_mem_lifecycle);
    assert!(decision.checkpoint_required);
    assert_eq!(
        decision.blocker_codes,
        vec![
            "checkpoint_boundary_missing".to_string(),
            "replay_boundary_inconsistent".to_string(),
            "checkpoint_required".to_string()
        ]
    );
}

#[test]
fn storage_recovery_report_recomputes_typed_readiness_from_raw_fields() {
    let report = NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
        open_timings: Default::default(),
        durable: true,
        recovery_mode: RecoveryMode::Strict,
        max_wal_replay_entries: Some(2),
        max_wal_replay_bytes: Some(4096),
        max_wal_record_bytes: Some(1024),
        checkpoint_epoch: Some(3),
        checkpoint_commit_epoch: None,
        wal_present: true,
        wal_replay_start_lsn: Some(4),
        next_lsn_after_replay: Some(8),
        replayed_wal_entries: 3,
        torn_tail_ignored: false,
        torn_tail_reason: Some("partial wal entry".to_string()),
        recovered_commit_epoch: 13,
        ..StorageRecoveryReport::default()
    });
    let json = report.json();

    assert!(report.durable_recovery_observed);
    assert!(!report.ready);
    assert!(!report.checkpoint_boundary_present);
    assert!(!report.wal_replay_bounded);
    assert!(!report.replay_boundary_consistent);
    assert!(!report.torn_tail_clean);
    assert_eq!(
        report.blocker_codes,
        vec![
            "checkpoint_boundary_missing".to_string(),
            "wal_replay_unbounded".to_string(),
            "replay_boundary_inconsistent".to_string(),
            "torn_tail_observed".to_string()
        ]
    );
    assert_eq!(json["readiness"]["checkpoint_boundary_present"], false);
    assert_eq!(json["readiness"]["wal_replay_bounded"], false);
    assert_eq!(json["readiness"]["replay_boundary_consistent"], false);
    assert_eq!(json["readiness"]["torn_tail_clean"], false);
}

#[test]
fn storage_recovery_report_rejects_inconsistent_replay_boundary() {
    let report = NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
        open_timings: Default::default(),
        durable: true,
        recovery_mode: RecoveryMode::Strict,
        max_wal_replay_entries: Some(16),
        max_wal_replay_bytes: Some(4096),
        max_wal_record_bytes: Some(1024),
        checkpoint_epoch: Some(3),
        checkpoint_commit_epoch: Some(11),
        wal_present: true,
        wal_replay_start_lsn: Some(4),
        next_lsn_after_replay: Some(7),
        replayed_wal_entries: 3,
        torn_tail_ignored: false,
        torn_tail_reason: None,
        recovered_commit_epoch: 13,
        ..StorageRecoveryReport::default()
    });
    let json = report.json();

    assert!(report.durable_recovery_observed);
    assert!(report.checkpoint_boundary_present);
    assert!(report.wal_replay_bounded);
    assert!(!report.replay_boundary_consistent);
    assert!(report.torn_tail_clean);
    assert!(!report.ready);
    assert_eq!(
        report.blocker_codes,
        vec!["replay_boundary_inconsistent".to_string()]
    );
    assert_eq!(json["readiness"]["replay_boundary_consistent"], false);
}

#[test]
fn embedded_store_exposes_storage_recovery_report_through_library_api() {
    let db = Database::new();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let report = store.storage_recovery_report().unwrap();
    let json = store.storage_recovery_report_json().unwrap();

    assert_eq!(report.protocol, "hawdb-storage-recovery-report");
    assert!(report.present);
    assert!(!report.ready);
    assert_eq!(
        report.blocker_codes,
        vec![
            "durable_recovery_not_observed".to_string(),
            "checkpoint_boundary_missing".to_string(),
            "wal_replay_unbounded".to_string(),
            "replay_boundary_inconsistent".to_string()
        ]
    );
    assert_eq!(json["ready"], false);
    assert_eq!(
        json["blocker_codes"],
        serde_json::json!(report.blocker_codes)
    );
}

#[test]
fn embedded_store_library_readiness_runs_bounded_probe() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    graph
        .database_mut()
        .query("CREATE (:Memory {id: 'mem-readiness', title: 'Readiness'})")
        .unwrap();
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let readiness = store
        .library_readiness_json(&NowledgeMemReadinessOptions {
            bounded_read_probe: Some(NowledgeGraphStatement {
                cypher: "MATCH (m:Memory {id: 'mem-readiness'}) RETURN m.title AS title"
                    .to_string(),
                parameters: BTreeMap::new(),
            }),
            covered_routes: full_bounded_read_routes(),
            graph_route_readiness: Some(ready_route_readiness_summary()),
            search_route_ownership: Some(ready_search_route_ownership()),
            active_search_route_ownership: Some(ready_active_search_route_ownership()),
            active_search_route_readiness: Some(ready_active_search_route_readiness()),
            replacement_readiness_by_query_family: Some(ready_query_family_replacement()),
            ..NowledgeMemReadinessOptions::default()
        })
        .unwrap();

    assert_eq!(readiness["bounded_read_evidence"]["present"], true);
    assert_eq!(readiness["bounded_read_evidence"]["ready"], true);
    assert_eq!(
        readiness["bounded_read_evidence"]["mode"],
        "shadow_read_only"
    );
    assert_eq!(readiness["bounded_read_evidence"]["execution_row_cap"], 513);
    assert!(
        readiness["bounded_read_evidence"]["estimated_payload_bytes"]
            .as_u64()
            .is_some()
    );
    assert_eq!(
        readiness["bounded_read_evidence"]["max_estimated_payload_bytes"],
        serde_json::json!(4 * 1024 * 1024)
    );
    assert_eq!(
        readiness["bounded_read_evidence"]["payload_budget_exceeded"],
        false
    );
    assert_eq!(
        readiness["bounded_read_evidence"]["missing_covered_routes"],
        serde_json::json!([])
    );
    assert_eq!(
        readiness["background_maintenance"]["total_candidates"]
            .as_u64()
            .unwrap_or_default(),
        readiness["background_maintenance"]["ranked"]
            .as_array()
            .unwrap()
            .len() as u64
    );
    assert_eq!(readiness["readiness_by_area"]["query"]["ready"], true);
    assert_eq!(readiness["readiness_by_area"]["background"]["ready"], true);
    assert_eq!(
        readiness["readiness_by_area"]["background"]["blocker_codes"],
        serde_json::json!([])
    );
    assert_eq!(
        readiness["readiness_by_area"]["query_family"]["ready"],
        true
    );
    assert_eq!(readiness["readiness_by_area"]["graph_route"]["ready"], true);
    assert_eq!(
        readiness["readiness_by_area"]["search_route_ownership"]["ready"],
        true
    );
    assert_eq!(
        readiness["search_route_ownership"]["lancedb_route_count"],
        0
    );
    assert_eq!(
        readiness["active_search_route_ownership"]["lancedb_route_count"],
        0
    );
    assert_eq!(
        readiness["active_search_route_readiness"]["lancedb_handle_required_route_count"],
        0
    );
    assert_eq!(
        readiness["graph_route_readiness"]["primary_ready_route_count"],
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
    );
    assert_eq!(
        readiness["query_family_evidence"]["missing_required_query_families"],
        serde_json::json!([])
    );
    assert_eq!(
        readiness["query_family_evidence"]["min_replacement_readiness_per_million"],
        1_000_000
    );
}

#[test]
fn embedded_store_library_readiness_feeds_final_preflight_without_cli() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    graph
        .database_mut()
        .query("CREATE (:Memory {id: 'mem-preflight', title: 'Preflight'})")
        .unwrap();
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let library = store
        .library_readiness(&NowledgeMemReadinessOptions {
            bounded_read_probe: Some(NowledgeGraphStatement {
                cypher: "MATCH (m:Memory {id: 'mem-preflight'}) RETURN m.title AS title"
                    .to_string(),
                parameters: BTreeMap::new(),
            }),
            covered_routes: full_bounded_read_routes(),
            graph_route_readiness: Some(ready_route_readiness_summary()),
            search_route_ownership: Some(ready_search_route_ownership()),
            active_search_route_ownership: Some(ready_active_search_route_ownership()),
            active_search_route_readiness: Some(ready_active_search_route_readiness()),
            replacement_readiness_by_query_family: Some(ready_query_family_replacement()),
            ..NowledgeMemReadinessOptions::default()
        })
        .unwrap();
    let mut library_json = library.json();
    library_json["open_report"] = serde_json::json!({
        "protocol": NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL,
        "mode": "shadow_read_only",
        "graph_configured": true,
        "search_projection_configured": false,
        "compressed_vector_search_mode": "disabled",
        "graph_opened": true,
        "search_projection_opened": false,
    });

    let bundle = serde_json::json!({
        "library_readiness": library_json,
    });
    let preflight = nowledge_mem_final_cutover_preflight(&bundle);

    assert!(library.readiness_by_area.query.ready);
    assert!(library.readiness_by_area.graph_route.ready);
    assert!(library.readiness_by_area.search_route_ownership.ready);
    assert!(library.readiness_by_area.query_family.ready);
    assert!(library.readiness_by_area.background.ready);
    assert!(!library.readiness_by_area.storage.ready);
    assert!(!library.readiness_by_area.search_projection.ready);
    assert!(!preflight.production_cutover_ready);
    assert!(!preflight.library_only_ready);
    assert!(preflight
        .failed_checks
        .iter()
        .any(|check| check == "library_readiness"));
    assert!(preflight
        .failed_evidence_fields
        .iter()
        .any(|field| field == "library_readiness.open_report.search_projection_opened"));
    assert!(preflight
        .next_action_names
        .iter()
        .any(|action| action == "attach_library_readiness_evidence"));
}

#[test]
fn embedded_store_library_readiness_recomputes_bounded_read_payload_budget() {
    let db = Database::new();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let readiness = store
        .library_readiness_json(&NowledgeMemReadinessOptions {
            bounded_read_evidence: Some(serde_json::json!({
                "protocol": NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
                "present": true,
                "ready": true,
                "mode": "shadow_read_only",
                "max_rows": 512,
                "execution_row_cap": 513,
                "row_limit_enforced_before_output": true,
                "operator_row_cap_enabled": true,
                "streaming": false,
                "blocking_operator_count": 0,
                "row_budget_exceeded": false,
                "payload_budget_exceeded": false,
                "missing_covered_routes": [],
                "route_primary_ready": true,
                "route_query_plan_evidence_ready": true,
                "route_query_profile_evidence_ready": true,
                "route_query_api_behavior_evidence_ready": true,
                "relationship_property_pruning_required_count": 0,
                "relationship_property_pruning_report_count": 0,
                "route_relationship_property_pruning_evidence_ready": true,
                "blocker_codes": [],
            })),
            ..NowledgeMemReadinessOptions::default()
        })
        .unwrap();

    assert_eq!(readiness["bounded_read_evidence"]["ready"], true);
    assert_eq!(readiness["readiness_by_area"]["query"]["ready"], false);
    assert!(readiness["blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "bounded_read_evidence_not_ready"));
    let query_blockers = readiness["readiness_by_area"]["query"]["blocker_codes"]
        .as_array()
        .unwrap();
    assert!(query_blockers
        .iter()
        .any(|code| code == "bounded_read_estimated_payload_bytes_missing"));
    assert!(query_blockers
        .iter()
        .any(|code| code == "bounded_read_max_estimated_payload_bytes_missing"));
}

#[test]
fn embedded_store_exposes_search_projection_probe() {
    let index = SearchIndex::default();
    let projection = NowledgeMemSearchProjection::from_index(index);
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

    let probe = store
        .search_projection()
        .unwrap()
        .probe_json(SearchProjectionProbeOptions::default());

    assert_eq!(probe["protocol"], "hawdb-nowledge-search-projection-probe");
}

#[test]
fn embedded_store_handle_exposes_sampled_vector_recall_validation() {
    let index = persisted_nowledge_projection_evidence_index("handle_recall_validation", false);
    let projection = NowledgeMemSearchProjection::from_index(index);
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, Some(projection)))
            .unwrap();

    let report = handle
        .validate_sampled_vector_recall(VectorRecallValidationOptions {
            max_samples: 2,
            top_k: 1,
            candidate_limit: 1,
            minimum_recall_per_million: 0,
            metadata_filters: BTreeMap::new(),
        })
        .unwrap();

    assert!(report.ready, "{:?}", report.blocker_codes);
    assert_eq!(report.protocol, VECTOR_RECALL_VALIDATION_PROTOCOL);
    assert_eq!(report.executed_sample_count, 2);
    assert_eq!(
        report.approximate_backend,
        "hawdb_diskann_candidate_projection"
    );
    assert!(report.validates_default_approximate_backend());
}

#[test]
fn embedded_store_recall_validation_requires_search_projection() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let error = store
        .validate_sampled_vector_recall(VectorRecallValidationOptions::default())
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "storage error: nowledge mem search projection is not configured"
    );
}

#[test]
fn embedded_store_exposes_search_projection_replacement_evidence() {
    let index = persisted_nowledge_projection_evidence_index("replacement_evidence", true);
    let projection = NowledgeMemSearchProjection::from_index(index);
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

    let evidence = store
        .search_projection_evidence_json(SearchProjectionProbeOptions {
            active_embedding_model: Some("bge-m3".to_string()),
            active_embedding_dimension: Some(8),
        })
        .unwrap();

    assert_eq!(
        evidence["protocol"],
        "hawdb-nowledge-search-projection-evidence"
    );
    assert_eq!(evidence["ready"], true, "evidence={evidence:#}");
    assert_eq!(evidence["covered_table_count"], 6);
    assert_eq!(evidence["required_table_count"], 6);
    assert_eq!(evidence["source_chunk_ready"], true);
    assert_eq!(evidence["incremental_update_ready"], true);
    assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
}

#[test]
fn embedded_store_exposes_typed_search_projection_replacement_evidence() {
    let index = persisted_nowledge_projection_evidence_index("typed_replacement_evidence", true);
    let projection = NowledgeMemSearchProjection::from_index(index);
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

    let report = store
        .search_projection_evidence_report(SearchProjectionProbeOptions {
            active_embedding_model: Some("bge-m3".to_string()),
            active_embedding_dimension: Some(8),
        })
        .unwrap();

    assert_eq!(report.protocol, "hawdb-nowledge-search-projection-evidence");
    assert!(report.ready);
    assert!(report.compressed_vector_projection_ready);
    assert!(report.derived_projection);
    assert!(report.all_tables_covered);
    assert_eq!(report.covered_table_count, 6);
    assert_eq!(report.required_table_count, 6);
    assert!(report.source_chunk_ready);
    assert!(report.incremental_update_ready);
    assert_eq!(report.json()["covered_table_count"], 6);
}

#[test]
fn embedded_store_exposes_search_projection_shadow_evidence() {
    let index = persisted_nowledge_projection_evidence_index("shadow_evidence", true);
    let projection = NowledgeMemSearchProjection::from_index(index);
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let probe_options = SearchProjectionProbeOptions {
        active_embedding_model: Some("bge-m3".to_string()),
        active_embedding_dimension: Some(8),
    };
    let primary_probe = store
        .search_projection_probe_json(probe_options.clone())
        .unwrap();

    let evidence = store
        .search_projection_shadow_evidence_json(&primary_probe, probe_options)
        .unwrap();

    assert_eq!(
        evidence["protocol"],
        "hawdb-nowledge-search-projection-shadow-evidence"
    );
    assert_eq!(evidence["ready"], true, "evidence={evidence:#}");
    assert_eq!(evidence["primary_ready"], true);
    assert_eq!(evidence["shadow_ready"], true);
    assert_eq!(evidence["document_count_parity"], true);
    assert_eq!(evidence["table_parity"]["ready"], true);
    assert_eq!(evidence["embedding_identity_parity"], true);
    assert_eq!(evidence["incremental_watermark_parity"], true);
    assert_eq!(
        evidence["pushdown_evidence"]["shadow_segment_document_pruning_ready"],
        true
    );
    assert_eq!(
        evidence["pushdown_evidence"]["shadow_segment_pruning_candidate_document_count"],
        130
    );
    assert_eq!(
        evidence["pushdown_evidence"]["shadow_segment_pruned_document_count"],
        2
    );
    assert_eq!(
        evidence["pushdown_evidence"]["shadow_segment_scanned_document_count"],
        128
    );
    assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
}

#[test]
fn embedded_store_search_projection_evidence_requires_projection() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let error = store
        .search_projection_evidence_json(SearchProjectionProbeOptions::default())
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "storage error: nowledge mem search projection is not configured"
    );
}

#[test]
fn embedded_store_search_projection_shadow_evidence_requires_projection() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let error = store
        .search_projection_shadow_evidence_json(
            &serde_json::json!({ "engine": "lancedb" }),
            SearchProjectionProbeOptions::default(),
        )
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "storage error: nowledge mem search projection is not configured"
    );
}

#[test]
fn open_options_report_is_sanitized() {
    let options = NowledgeMemOpenOptions::with_search_projection(
        "redacted_graph_path",
        "redacted_search_path",
        NowledgeMemGraphMode::ShadowReadOnly,
    );

    let report = options.sanitized_report().json();

    assert_eq!(report["protocol"], NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL);
    assert_eq!(report["mode"], "shadow_read_only");
    assert_eq!(report["graph_configured"], true);
    assert_eq!(report["search_projection_configured"], true);
    assert_eq!(report["compressed_vector_search_mode"], "disabled");
    assert!(report.get("graph_path").is_none());
    assert!(report.get("search_projection_path").is_none());
    assert!(!report.to_string().contains("redacted_graph_path"));
    assert!(!report.to_string().contains("redacted_search_path"));
}

#[test]
fn open_options_diagnostics_include_local_paths_only_with_debug_flag() {
    let options = NowledgeMemOpenOptions::with_search_projection(
        "debug_graph_path",
        "debug_search_path",
        NowledgeMemGraphMode::ShadowReadOnly,
    );

    let redacted = options.diagnostic_report_json(NowledgeMemOpenDiagnosticOptions::default());
    assert_eq!(redacted["debug_local_paths_included"], false);
    assert_eq!(redacted["local_paths_redacted"], true);
    assert!(redacted.get("graph_path").is_none());
    assert!(redacted.get("search_projection_path").is_none());
    assert!(!redacted.to_string().contains("debug_graph_path"));
    assert!(!redacted.to_string().contains("debug_search_path"));

    let debug = options.diagnostic_report_json(NowledgeMemOpenDiagnosticOptions {
        include_local_paths: true,
    });
    assert_eq!(debug["debug_local_paths_included"], true);
    assert_eq!(debug["local_paths_redacted"], false);
    assert_eq!(debug["graph_path"], "debug_graph_path");
    assert_eq!(debug["search_projection_path"], "debug_search_path");
}

#[test]
fn open_options_report_gates_advanced_compressed_vector_search_mode() {
    let options = NowledgeMemOpenOptions::with_search_projection(
        "redacted_graph_path",
        "redacted_search_path",
        NowledgeMemGraphMode::ShadowReadOnly,
    )
    .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred);

    let report = options.sanitized_report().json();

    assert_eq!(report["compressed_vector_search_mode"], "disabled");
    assert_eq!(
        report["requested_compressed_vector_search_mode"],
        "preferred"
    );
    assert_eq!(report["retrieval_projection_advisor"]["ready"], false);
    assert_eq!(
        report["retrieval_projection_advisor_blocker_codes"],
        serde_json::json!([
            "retrieval_projection_recall_evidence_missing",
            "retrieval_projection_parity_evidence_missing",
            "retrieval_projection_segment_not_advised"
        ])
    );
    assert!(!report.to_string().contains("redacted_graph_path"));
    assert!(!report.to_string().contains("redacted_search_path"));
}

#[test]
fn open_options_report_allows_compressed_vector_search_with_advisor_evidence() {
    let recall_report = ready_vector_recall_report();
    let options = NowledgeMemOpenOptions::with_search_projection(
        "redacted_graph_path",
        "redacted_search_path",
        NowledgeMemGraphMode::ShadowReadOnly,
    )
    .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred)
    .with_retrieval_projection_advisor(
        NowledgeMemRetrievalProjectionAdvisor::cold_local_with_recall_parity(&recall_report),
    );

    let report = options.sanitized_report().json();

    assert_eq!(report["compressed_vector_search_mode"], "preferred");
    assert_eq!(
        report["requested_compressed_vector_search_mode"],
        "preferred"
    );
    assert_eq!(
        report["adaptive_vector_backend_policy"]["flat_scan_memory_budget_bytes"],
        16 * 1024 * 1024
    );
    assert_eq!(report["retrieval_projection_advisor"]["ready"], true);
    assert_eq!(
        report["retrieval_projection_advisor_blocker_codes"],
        serde_json::json!([])
    );
}

#[test]
fn open_options_report_rejects_inconsistent_recall_evidence() {
    let mut recall_report = ready_vector_recall_report();
    recall_report.recall_at_k_per_million = 0;
    let options = NowledgeMemOpenOptions::with_search_projection(
        "redacted_graph_path",
        "redacted_search_path",
        NowledgeMemGraphMode::ShadowReadOnly,
    )
    .with_compressed_vector_search_mode(CompressedVectorSearchMode::Required)
    .with_retrieval_projection_advisor(
        NowledgeMemRetrievalProjectionAdvisor::cold_local_with_recall_parity(&recall_report),
    );

    let report = options.sanitized_report().json();

    assert_eq!(report["compressed_vector_search_mode"], "disabled");
    assert_eq!(
        report["retrieval_projection_advisor_blocker_codes"],
        serde_json::json!(["retrieval_projection_recall_evidence_not_ready"])
    );
}

#[test]
fn qualified_out_of_core_open_recomputes_production_evidence() {
    let root = unique_nowledge_mem_test_dir("qualified_out_of_core_open");
    let graph_path = root.join("graph");
    let search_path = root.join("search");
    let graph_commit_epoch = {
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'qualified-open'})").unwrap();
        db.commit_epoch().unwrap()
    };
    {
        let mut index = SearchIndex::open(&search_path).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "test-embedding".to_string(),
                version: Some("v1".to_string()),
                dimension: 2,
            })
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "qualified-open".to_string(),
                    title: "Qualified open".to_string(),
                    body: "Production evidence is recomputed".to_string(),
                    embedding: Some(vec![1.0, 0.0]),
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: Vec::new(),
                max_operations: None,
                source_graph_commit_epoch: Some(graph_commit_epoch),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    let projection = NowledgeMemOutOfCoreSearchProjection::open(&search_path).unwrap();
    let projection_identity = projection.reader().production_qualification_identity();
    drop(projection);
    let expected_identity = production_identity(graph_commit_epoch);
    let qualification = SearchLexicalProductionQualificationReport::evaluate_for_production(
        projection_identity,
        ProductionEvidenceBinding {
            identity: expected_identity.clone(),
            generated_at_unix_seconds: 1,
        },
        expected_identity.clone(),
        SearchTopKScoreParity {
            text: true,
            vector: true,
            hybrid: true,
        },
        SearchLexicalFeasibilityCoverage::default(),
        SearchLexicalFeasibilityMetrics::default(),
    );
    let out_of_core_config = SearchOutOfCoreConfig {
        spill_directory: root.join("spill"),
        ..SearchOutOfCoreConfig::default()
    };
    let options = NowledgeMemOpenOptions::with_qualified_out_of_core_search_projection(
        &graph_path,
        &search_path,
        NowledgeMemGraphMode::ShadowReadOnly,
        NowledgeMemQualifiedOutOfCoreSearchOptions {
            config: out_of_core_config,
            qualification,
            expected_identity,
        },
    );
    let report = options.sanitized_report().json();
    assert_eq!(report["search_projection_role"], "qualified_out_of_core");
    assert_eq!(report["search_production_qualification_bound"], false);

    let error = NowledgeMemEmbeddedStore::open_with_options(options).unwrap_err();
    assert!(error.to_string().contains("dataset_too_small"));
    assert!(error.to_string().contains("workload_coverage_incomplete"));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_opens_from_options_with_sanitized_report() {
    let root = unique_nowledge_mem_test_dir("open_options");
    let graph_path = root.join("graph");
    let search_path = root.join("search");
    let options = NowledgeMemOpenOptions::with_search_projection(
        graph_path,
        search_path,
        NowledgeMemGraphMode::WritableCutover,
    );

    let (mut store, report) = NowledgeMemEmbeddedStore::open_with_options(options).unwrap();
    store
        .graph_mut()
        .query("CREATE (:Memory {id: 'mem-open', title: 'Open options'})")
        .unwrap();

    assert_eq!(report.protocol, NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL);
    assert_eq!(report.mode, NowledgeMemGraphMode::WritableCutover);
    assert_eq!(
        report.compressed_vector_search_mode,
        CompressedVectorSearchMode::Disabled
    );
    assert!(report.graph_opened);
    assert!(report.search_projection_opened);
    assert!(store.search_projection().is_some());
    assert_eq!(
        store
            .graph_mut()
            .query("MATCH (m:Memory {id: 'mem-open'}) RETURN m.title AS title")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn embedded_store_applies_incremental_graph_search_projection_delta() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    graph
            .query("CREATE (:Memory {id: 'new', title: 'Incremental facade', content: 'Graph changes feed search projection'})")
            .unwrap();
    let projection = NowledgeMemSearchProjection::from_index(SearchIndex::in_memory());
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

    let request = store
        .build_search_projection_graph_delta_request_from_freshness(Some(4))
        .unwrap()
        .expect("expected graph delta request");
    let plan = store
        .search_projection_graph_delta_background_work_plan(&request, BackgroundWorkHint::default())
        .expect("expected background work plan")
        .unwrap();

    let report = store.apply_search_projection_graph_delta(request).unwrap();

    assert_eq!(plan.request.class, WorkClass::Projection);
    assert_eq!(report.upserted_documents, 1);
    assert_eq!(
        store
            .search_projection()
            .unwrap()
            .index()
            .document("memory:new")
            .unwrap()
            .title,
        "Incremental facade"
    );
}

#[test]
fn initial_import_projection_uses_local_cursor_and_preserves_legacy_provenance() {
    let root = unique_nowledge_mem_test_dir("initial_import_projection_local_cursor");
    let graph_path = root.join("graph");
    let search_path = root.join("search");
    let options = NowledgeMemOpenOptions::with_search_projection(
        graph_path,
        search_path,
        NowledgeMemGraphMode::WritableCutover,
    );
    let (mut store, _) = NowledgeMemEmbeddedStore::open_with_options(options).unwrap();
    store
        .graph_mut()
        .query("CREATE (:Memory {id: 'm1', title: 'Imported'})")
        .unwrap();
    let local_graph_commit_epoch = store.graph().database().commit_epoch().unwrap();

    let report = store
        .apply_initial_import_projection_delta_and_checkpoint(
            SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "m1".to_string(),
                    title: "Imported".to_string(),
                    body: "legacy import".to_string(),
                    embedding: None,
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: Vec::new(),
                max_operations: Some(1),
                source_graph_commit_epoch: Some(77),
            },
            77,
        )
        .unwrap();

    assert_eq!(
        report.source_graph_commit_epoch_after,
        Some(local_graph_commit_epoch)
    );
    let freshness = store
        .search_projection()
        .unwrap()
        .index()
        .projection_freshness();
    assert_eq!(freshness.import_source_graph_commit_epoch, Some(77));
    assert_eq!(
        freshness.source_graph_commit_epoch,
        Some(local_graph_commit_epoch)
    );
    assert_eq!(
        freshness.durable_source_graph_commit_epoch,
        Some(local_graph_commit_epoch)
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_handle_1043_refuses_a_prefix_and_retries_complete_projection() {
    let root = unique_nowledge_mem_test_dir("embedded_projection_catch_up");
    let search_path = root.join("search");
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'm1', title: 'First'})")
        .unwrap();
    graph
        .query("CREATE (:Memory {id: 'm2', title: 'Second'})")
        .unwrap();
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, Some(projection)))
            .unwrap();

    let error = handle.catch_up_search_projection(1, 4).unwrap_err();
    assert!(matches!(
        error,
        HawDBError::Execution(message)
            if message.contains("differs from source snapshot epoch")
    ));
    {
        let store = handle.read_store().unwrap();
        let index = store.search_projection().unwrap().index();
        assert_eq!(index.document_count(), 0);
        let freshness = index.projection_freshness();
        assert_eq!(freshness.source_graph_commit_epoch, None);
        assert_eq!(freshness.durable_source_graph_commit_epoch, None);
    }
    assert!(!handle.storage_handle_poisoned().unwrap());

    let report = handle.catch_up_search_projection(2, 1).unwrap();

    assert!(report.complete);
    assert_eq!(report.end_applied_epoch, report.end_durable_epoch);
    drop(handle);
    let reopened = SearchIndex::open(&search_path).unwrap();
    assert!(reopened.document("memory:m1").is_some());
    assert!(reopened.document("memory:m2").is_some());
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_handle_runs_unified_relational_projection_catch_up() {
    let root = unique_nowledge_mem_test_dir("embedded_relational_projection_catch_up");
    let search_path = root.join("search");
    let mut db = Database::new();
    db.query_sql("CREATE TABLE thread_messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    db.query_sql(
        "INSERT INTO thread_messages (id, body) VALUES (1, 'Embedded relational document')",
    )
    .unwrap();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, Some(projection)))
            .unwrap();

    let report = handle
        .catch_up_search_projection_with_relational(1, 1, |snapshot, batch| {
            assert_eq!(batch.relational_primary_key_changes().len(), 1);
            let output = snapshot.query_sql_with_params(
                "SELECT body FROM thread_messages WHERE id = $1",
                &[Value::Int(1)],
            )?;
            let body = match output.rows.as_slice() {
                [row] => match row.get("body") {
                    Some(Value::String(body)) => body.clone(),
                    value => {
                        return Err(HawDBError::Execution(format!(
                            "thread_messages body expected STRING, got {value:?}"
                        )));
                    }
                },
                rows => {
                    return Err(HawDBError::Execution(format!(
                        "thread_messages hydration returned {} rows",
                        rows.len()
                    )));
                }
            };
            Ok(SearchProjectionRelationalDelta {
                delta: SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Message,
                        external_id: "1".to_string(),
                        title: String::new(),
                        body,
                        embedding: None,
                        source_id: None,
                        metadata: BTreeMap::new(),
                    }],
                    ..SearchProjectionDelta::default()
                },
                processed_primary_key_count: 1,
            })
        })
        .unwrap();

    assert!(report.complete);
    assert_eq!(report.applied_operation_count, 1);
    assert_eq!(report.end_applied_epoch, report.end_durable_epoch);
    drop(handle);
    let reopened = SearchIndex::open(&search_path).unwrap();
    assert_eq!(
        reopened
            .document("message:1")
            .map(|document| document.content.as_str()),
        Some("Embedded relational document")
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_handle_runs_unified_graph_batch_hydrator() {
    let root = unique_nowledge_mem_test_dir("embedded_graph_batch_projection_catch_up");
    let search_path = root.join("search");
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread-a', title: 'Updated title'})")
        .unwrap();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, Some(projection)))
            .unwrap();
    handle.catch_up_search_projection(1, 1).unwrap();
    handle
        .with_transaction(|transaction| {
            transaction.query("MATCH (t:Thread {id: 'thread-a'}) SET t.title = 'Current title'")?;
            Ok(())
        })
        .unwrap();
    let mut observed_graph_only_batch = false;

    let report = handle
        .catch_up_search_projection_with_batch_hydrator(1, 2, 1, |_snapshot, batch| {
            observed_graph_only_batch =
                !batch.has_relational_changes() && batch.graph_delta().upsert_node_ids.len() == 1;
            Ok(SearchProjectionRelationalDelta {
                delta: SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Message,
                        external_id: "derived".to_string(),
                        title: String::new(),
                        body: "Graph-dependent document".to_string(),
                        embedding: None,
                        source_id: None,
                        metadata: BTreeMap::new(),
                    }],
                    ..SearchProjectionDelta::default()
                },
                processed_primary_key_count: 0,
            })
        })
        .unwrap();

    assert!(observed_graph_only_batch);
    assert!(report.complete);
    drop(handle);
    let reopened = SearchIndex::open(&search_path).unwrap();
    assert!(reopened.document("thread:thread-a").is_none());
    assert!(reopened.document("message:derived").is_some());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_handle_exposes_live_graph_and_projection_watermarks() {
    let root = unique_nowledge_mem_test_dir("embedded_runtime_watermarks");
    let search_path = root.join("search");
    let mut graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'm1', title: 'Runtime status'})")
        .unwrap();
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, Some(projection)))
            .unwrap();

    let status_before = handle.runtime_status().unwrap();
    assert_eq!(status_before.graph_commit_epoch, 1);
    assert_eq!(status_before.changefeed.retained_mutation_count, 1);
    assert_eq!(
        status_before
            .projection_freshness
            .as_ref()
            .unwrap()
            .durable_source_graph_commit_epoch,
        None
    );
    assert_eq!(status_before.projection_commit_lag(), 1);
    assert!(status_before.projection_stale());

    handle.catch_up_search_projection(16, 1).unwrap();

    let status_after = handle.runtime_status().unwrap();
    let after = status_after.projection_freshness.as_ref().unwrap();
    assert_eq!(after.source_graph_commit_epoch, Some(1));
    assert_eq!(after.durable_source_graph_commit_epoch, Some(1));
    assert!(!after.has_uncheckpointed_changes);
    assert_eq!(status_after.projection_commit_lag(), 0);
    assert!(!status_after.projection_stale());
    assert_eq!(
        status_after.json()["projection"]["durable_source_graph_commit_epoch"],
        1
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn production_status_does_not_claim_graph_cutover_without_route_ownership() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let status = store.production_status(None).unwrap();
    let json = status.json();

    assert_eq!(status.protocol, NOWLEDGE_MEM_PRODUCTION_STATUS_PROTOCOL);
    assert!(status.graph_open);
    assert!(!status.graph_read_only);
    assert!(!status.graph_route_ownership_present);
    assert!(!status.graph_hawdb_cutover_effective);
    assert!(!status.search_projection_open);
    assert!(!status.search_hawdb_cutover_effective);
    assert!(status
        .blocker_codes
        .contains(&"graph_route_ownership_missing".to_string()));
    assert!(status
        .blocker_codes
        .contains(&"search_projection_not_open".to_string()));
    assert_eq!(json["graph"]["hawdb_cutover_effective"], false);
    assert_eq!(json["search"]["hawdb_cutover_effective"], false);
    assert_eq!(json["redaction"]["local_paths_copied"], false);
}

#[test]
fn production_status_reports_partial_graph_ownership_without_cutover_claim() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let store = NowledgeMemEmbeddedStore::new(graph, None);
    let route_ownership = nowledge_mem_route_ownership_readiness(
        &nowledge_mem_route_ownership_all_legacy(),
        Some(&ready_route_readiness_summary()),
        NowledgeMemRouteOwnershipPolicy::migration(),
    );

    let status = store.production_status(Some(&route_ownership)).unwrap();

    assert!(status.graph_route_ownership_present);
    assert!(status.graph_route_ownership_ready);
    assert_eq!(status.graph_hawdb_route_count, 0);
    assert_eq!(
        status.graph_legacy_route_count,
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
    );
    assert!(!status.graph_hawdb_cutover_effective);
    assert!(status
        .blocker_codes
        .contains(&"graph_legacy_routes_remaining".to_string()));
}

#[test]
fn production_status_separates_graph_cutover_from_search_freshness() {
    let root = unique_nowledge_mem_test_dir("production_status_split_cutover");
    let graph_path = root.join("graph");
    let search_path = root.join("search");
    let mut graph =
        NowledgeMemGraph::open(&graph_path, NowledgeMemGraphMode::WritableCutover).unwrap();
    graph
        .query("CREATE (:Memory {id: 'status-m1', title: 'Production status'})")
        .unwrap();
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let route_ownership = nowledge_mem_route_ownership_readiness(
        &nowledge_mem_route_ownership_all_hawdb(),
        Some(&ready_route_readiness_summary()),
        NowledgeMemRouteOwnershipPolicy::production_cutover(),
    );

    let stale = store.production_status(Some(&route_ownership)).unwrap();
    assert!(stale.graph_hawdb_cutover_effective);
    assert!(stale.search_projection_open);
    assert_eq!(stale.search_projection_commit_lag, 2);
    assert!(stale.search_projection_stale);
    assert!(!stale.search_hawdb_cutover_effective);
    assert!(stale
        .blocker_codes
        .contains(&"search_projection_stale".to_string()));

    store.catch_up_search_projection(16, 1).unwrap();
    let ready = store.production_status(Some(&route_ownership)).unwrap();
    assert!(ready.graph_hawdb_cutover_effective);
    assert!(ready.search_projection_open);
    assert_eq!(ready.search_projection_commit_lag, 0);
    assert!(!ready.search_projection_stale);
    assert!(ready.search_hawdb_cutover_effective);
    assert!(!ready
        .blocker_codes
        .contains(&"search_projection_stale".to_string()));
    assert_eq!(
        ready.json()["route_ownership"]["production_cutover_ready"],
        true
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn cutover_controls_keep_legacy_reads_ready_without_hawdb_ownership() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let report = store
        .cutover_controls_report(NowledgeMemCutoverControls::legacy(), None)
        .unwrap();
    let json = report.json();

    assert_eq!(report.protocol, NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL);
    assert!(report.ready);
    assert!(!report.graph_read_selected_hawdb);
    assert!(report.graph_read_effective);
    assert!(!report.search_read_selected_hawdb);
    assert!(report.search_read_effective);
    assert!(!report.dual_writes_enabled);
    assert_eq!(json["controls"]["graph_reads"], "legacy");
    assert_eq!(json["controls"]["search_reads"], "legacy");
    assert_eq!(json["redaction"]["local_paths_copied"], false);
}

#[test]
fn source_mutation_owner_preserves_public_type_identity_and_json() {
    use hawdb_readiness::source_mutation as owner;

    let evidence: Vec<NowledgeMemSourceMutationDualWriteEvidence> =
        owner::nowledge_mem_source_mutation_dual_write_evidence_all_ready();
    let same_evidence: &[owner::NowledgeMemSourceMutationDualWriteEvidence] = &evidence;
    let report: crate::NowledgeMemSourceMutationDualWriteReadinessReport =
        owner::nowledge_mem_source_mutation_dual_write_readiness(same_evidence);
    let requirements: Vec<crate::NowledgeMemSourceMutationFamilyRequirement> =
        owner::nowledge_mem_source_mutation_family_requirements();
    assert_eq!(requirements, report.requirements);
    assert_eq!(
        evidence,
        nowledge_mem_source_mutation_dual_write_evidence_all_ready()
    );
    assert_eq!(
        report,
        nowledge_mem_source_mutation_dual_write_readiness(&evidence)
    );
    assert_eq!(
        report,
        crate::nowledge_mem_source_mutation_dual_write_readiness(&evidence)
    );
    assert_eq!(report.json()["ready"], true);
    assert_eq!(
        REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES,
        owner::REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES
    );
    assert_eq!(
        NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
        "hawdb-nowledge-mem-source-mutation-dual-write-readiness-v1"
    );
    assert_eq!(
        crate::NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
        owner::NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL
    );
}

#[test]
fn source_mutation_owner_report_reaches_replacement_summary() {
    let mut evidence = nowledge_mem_source_mutation_dual_write_evidence_all_ready();
    for expected_ready in [true, false] {
        if !expected_ready {
            evidence
                .iter_mut()
                .find(|item| item.family == NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE)
                .unwrap()
                .independent_watermarks_recorded = false;
        }
        let report = nowledge_mem_source_mutation_dual_write_readiness(&evidence);
        for bundle in [
            serde_json::json!({"source_mutation_dual_write_readiness": report.json()}),
            serde_json::json!({"cutover_evidence": {"source_mutation_dual_write_readiness": report.json()}}),
        ] {
            let summary = crate::replacement_summary::nowledge_replacement_summary_json(&bundle);
            let summary = &summary["source_mutation_dual_write_readiness"];
            assert_eq!(summary["present"], true);
            assert_eq!(summary["ready"], expected_ready);
            assert_eq!(
                summary["blocker_codes"],
                serde_json::json!(report.blocker_codes)
            );
            assert_eq!(
                summary["required_family_count"],
                serde_json::json!(REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len())
            );
        }
    }
}

#[test]
fn cutover_controls_fail_closed_when_hawdb_reads_are_not_effective() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let report = store
        .cutover_controls_report(NowledgeMemCutoverControls::hawdb_reads(), None)
        .unwrap();

    assert!(!report.ready);
    assert!(report.graph_read_selected_hawdb);
    assert!(!report.graph_read_effective);
    assert!(report.search_read_selected_hawdb);
    assert!(!report.search_read_effective);
    assert!(report.projection_catch_up_enabled);
    assert!(report
        .blocker_codes
        .contains(&"graph_read_selected_hawdb_but_not_effective".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"search_read_selected_hawdb_but_not_effective".to_string()));
    assert_eq!(
        report.json()["production_status"]["graph"]["hawdb_cutover_effective"],
        false
    );
}

#[test]
fn cutover_controls_require_dual_writes_for_initial_import() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let store = NowledgeMemEmbeddedStore::new(graph, None);
    let controls = NowledgeMemCutoverControls {
        initial_import: NowledgeMemWorkControl::Enabled,
        ..NowledgeMemCutoverControls::legacy()
    };

    let report = store.cutover_controls_report(controls, None).unwrap();

    assert!(!report.ready);
    assert!(report.initial_import_enabled);
    assert!(!report.dual_writes_enabled);
    assert!(!report.initial_import_inactive_for_cutover);
    assert!(report
        .blocker_codes
        .contains(&"initial_import_enabled_without_dual_writes".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_active_blocks_read_cutover".to_string()));
}

fn ready_initial_import_cutover_catch_up_report() -> HawDBLightningInitialImportCutoverCatchUpReport
{
    HawDBLightningInitialImportCutoverCatchUpReport {
        ready: true,
        session_ready_for_cutover: true,
        durable_state_present: true,
        live_projection_present: true,
        import_graph_commit_epoch: Some(42),
        import_durable_search_projection_commit_epoch: Some(42),
        live_graph_commit_epoch: 42,
        live_search_projection_commit_epoch: Some(42),
        live_durable_search_projection_commit_epoch: Some(42),
        graph_watermark_caught_up: true,
        search_projection_watermark_caught_up: true,
        live_projection_checkpointed: true,
        live_projection_healthy: true,
        cutover_watermark: Some(42),
        blocker_codes: Vec::new(),
    }
}

fn ready_initial_import_recovery_report(
) -> crate::HawDBLightningInitialImportRecoveryReadinessReport {
    let mut source = Database::new();
    source
            .query("CREATE (:Memory {id: 'import-root'})-[:LINKS {id: 'import-rel'}]->(:Entity {id: 'import-entity'})")
            .unwrap();
    let export = source.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        protocol_version: 1,
        import_id: "import-controls".to_string(),
        task_id: "import-controls-task".to_string(),
        fencing_token: "import-controls-fence".to_string(),
        object_digest: "import-controls-digest".to_string(),
        schema_checksum: export.manifest.schema_checksum,
        graph_stream_checksum: export.manifest.graph_stream_checksum,
        graph_stream_byte_len: export.manifest.graph_stream_byte_len,
        relational_stream_checksum: export.manifest.relational_stream_checksum,
        relational_stream_byte_len: export.manifest.relational_stream_byte_len,
        manifest_database_commit_epoch: export.manifest.database_commit_epoch,
        manifest_graph_commit_epoch: export.manifest.graph_commit_epoch,
        applied_graph_commit_epoch: export.manifest.graph_commit_epoch,
        applied_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        durable_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        completed_batches: 1,
        total_batches: 1,
        document_identity_count: 6,
    };
    let identities = [
        SearchProjectionKind::Memory,
        SearchProjectionKind::Message,
        SearchProjectionKind::Entity,
        SearchProjectionKind::Source,
        SearchProjectionKind::SourceChunk,
        SearchProjectionKind::Community,
    ]
    .into_iter()
    .map(|kind| HawDBLightningInitialImportDocumentIdentity {
        kind,
        document_id: format!("{}:import", kind.as_str()),
    })
    .collect::<Vec<_>>();
    let durable_state = crate::hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &identities,
    )
    .state
    .expect("expected persistable durable state");
    let durable_state_payload =
        crate::hawdb_lightning_initial_import_encode_durable_state(&durable_state).unwrap();
    let projection_rows = identities
        .iter()
        .map(|identity| SearchProjectionRow {
            kind: identity.kind,
            external_id: identity.document_id.clone(),
            title: "initial import".to_string(),
            body: "initial import".to_string(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        })
        .collect::<Vec<_>>();
    let projection_delta = SearchProjectionDelta {
        upserts: projection_rows,
        deletes: Vec::new(),
        max_operations: Some(6),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };
    let freshness = SearchProjectionFreshness {
        document_count: 6,
        import_source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
        durable_source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
        has_uncheckpointed_changes: false,
        full_reindex_needed: false,
        full_reindex_reasons: Vec::new(),
        metadata_repair_needed: false,
        metadata_repair_reasons: Vec::new(),
        embedding_model: None,
        embedding_version: None,
        embedding_dimension: None,
    };

    let projection_batches = [projection_delta];
    let recovery = source
        .hawdb_lightning_initial_import_recovery_readiness(
            HawDBLightningInitialImportReadinessInputs {
                encoded_graph_stream: &export.graph_stream.encoded,
                encoded_relational_stream: &export.relational_stream.encoded,
                manifest: &export.manifest,
                projection_batches: &projection_batches,
                target_projection_freshness: Some(&freshness),
                live_projection_freshness: Some(&freshness),
            },
            Some(&durable_state_payload),
        )
        .unwrap();
    assert!(recovery.ready);
    recovery
}

#[test]
fn cutover_controls_accept_active_initial_import_with_cutover_catch_up_proof() {
    let root = unique_nowledge_mem_test_dir("cutover_controls_initial_import_catch_up");
    let graph_path = root.join("graph");
    let search_path = root.join("search");
    let mut graph =
        NowledgeMemGraph::open(&graph_path, NowledgeMemGraphMode::WritableCutover).unwrap();
    graph
        .query("CREATE (:Memory {id: 'controls-import-m1', title: 'Import cutover'})")
        .unwrap();
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    store.catch_up_search_projection(16, 1).unwrap();
    let route_ownership = nowledge_mem_route_ownership_readiness(
        &nowledge_mem_route_ownership_all_hawdb(),
        Some(&ready_route_readiness_summary()),
        NowledgeMemRouteOwnershipPolicy::production_cutover(),
    );
    let controls = NowledgeMemCutoverControls {
        dual_writes: NowledgeMemWorkControl::Enabled,
        initial_import: NowledgeMemWorkControl::Enabled,
        projection_catch_up: NowledgeMemWorkControl::Enabled,
        graph_reads: super::NowledgeMemReadControl::HawDB,
        search_reads: super::NowledgeMemReadControl::HawDB,
    };
    let catch_up = ready_initial_import_cutover_catch_up_report();

    let report = store
        .cutover_controls_report_with_initial_import_cutover_catch_up(
            controls,
            Some(&route_ownership),
            Some(&catch_up),
        )
        .unwrap();

    assert!(report.ready);
    assert!(report.initial_import_enabled);
    assert!(!report.initial_import_inactive_for_cutover);
    assert!(report.initial_import_cutover_catch_up_ready);
    assert!(report.initial_import_safe_for_read_cutover);
    assert!(report.blocker_codes.is_empty());
    assert_eq!(
        report.json()["work"]["initial_import_safe_for_read_cutover"],
        true
    );
}

#[test]
fn cutover_controls_require_recovery_proof_for_active_initial_import() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let store = NowledgeMemEmbeddedStore::new(graph, None);
    let controls = NowledgeMemCutoverControls {
        dual_writes: NowledgeMemWorkControl::Enabled,
        initial_import: NowledgeMemWorkControl::Enabled,
        ..NowledgeMemCutoverControls::legacy()
    };

    let report = store
        .cutover_controls_report_with_initial_import_recovery(controls, None, None)
        .unwrap();

    assert!(!report.ready);
    assert!(report
        .blocker_codes
        .contains(&"initial_import_recovery_not_ready".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_active_blocks_read_cutover".to_string()));
}

#[test]
fn cutover_controls_accept_active_initial_import_with_recovery_proof() {
    let root = unique_nowledge_mem_test_dir("cutover_controls_initial_import_recovery");
    let graph_path = root.join("graph");
    let search_path = root.join("search");
    let mut graph =
        NowledgeMemGraph::open(&graph_path, NowledgeMemGraphMode::WritableCutover).unwrap();
    graph
        .query("CREATE (:Memory {id: 'controls-recovery-m1', title: 'Import recovery'})")
        .unwrap();
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    store.catch_up_search_projection(16, 1).unwrap();
    let route_ownership = nowledge_mem_route_ownership_readiness(
        &nowledge_mem_route_ownership_all_hawdb(),
        Some(&ready_route_readiness_summary()),
        NowledgeMemRouteOwnershipPolicy::production_cutover(),
    );
    let controls = NowledgeMemCutoverControls {
        dual_writes: NowledgeMemWorkControl::Enabled,
        initial_import: NowledgeMemWorkControl::Enabled,
        projection_catch_up: NowledgeMemWorkControl::Enabled,
        graph_reads: super::NowledgeMemReadControl::HawDB,
        search_reads: super::NowledgeMemReadControl::HawDB,
    };
    let recovery = ready_initial_import_recovery_report();

    let report = store
        .cutover_controls_report_with_initial_import_recovery(
            controls,
            Some(&route_ownership),
            Some(&recovery),
        )
        .unwrap();

    assert!(report.ready);
    assert!(report.initial_import_cutover_catch_up_ready);
    assert!(report.initial_import_safe_for_read_cutover);
    assert!(report.blocker_codes.is_empty());
}

#[test]
fn cutover_controls_block_read_cutover_while_initial_import_is_active() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let store = NowledgeMemEmbeddedStore::new(graph, None);
    let controls = NowledgeMemCutoverControls {
        dual_writes: NowledgeMemWorkControl::Enabled,
        initial_import: NowledgeMemWorkControl::Enabled,
        ..NowledgeMemCutoverControls::legacy()
    };

    let report = store.cutover_controls_report(controls, None).unwrap();

    assert!(!report.ready);
    assert!(report.dual_writes_enabled);
    assert!(report.initial_import_enabled);
    assert!(!report.initial_import_inactive_for_cutover);
    assert_eq!(
        report.json()["work"]["initial_import_inactive_for_cutover"],
        false
    );
    assert_eq!(
        report.blocker_codes,
        vec!["initial_import_active_blocks_read_cutover".to_string()]
    );
}

#[test]
fn cutover_controls_accept_independent_hawdb_reads_after_status_is_ready() {
    let root = unique_nowledge_mem_test_dir("cutover_controls_ready_reads");
    let graph_path = root.join("graph");
    let search_path = root.join("search");
    let mut graph =
        NowledgeMemGraph::open(&graph_path, NowledgeMemGraphMode::WritableCutover).unwrap();
    graph
        .query("CREATE (:Memory {id: 'controls-m1', title: 'Cutover controls'})")
        .unwrap();
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    store.catch_up_search_projection(16, 1).unwrap();
    let route_ownership = nowledge_mem_route_ownership_readiness(
        &nowledge_mem_route_ownership_all_hawdb(),
        Some(&ready_route_readiness_summary()),
        NowledgeMemRouteOwnershipPolicy::production_cutover(),
    );

    let report = store
        .cutover_controls_report(
            NowledgeMemCutoverControls::hawdb_reads(),
            Some(&route_ownership),
        )
        .unwrap();

    assert!(report.ready);
    assert!(report.graph_read_selected_hawdb);
    assert!(report.graph_read_effective);
    assert!(report.search_read_selected_hawdb);
    assert!(report.search_read_effective);
    assert!(report.dual_writes_enabled);
    assert!(report.projection_catch_up_enabled);
    assert!(report.blocker_codes.is_empty());
    assert_eq!(
        report.json()["production_status"]["search"]["hawdb_cutover_effective"],
        true
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_catches_up_delete_after_graph_checkpoint_and_restart() {
    let root = unique_nowledge_mem_test_dir("embedded_projection_checkpoint_restart");
    let graph_path = root.join("graph");
    let search_path = root.join("search");
    {
        let graph =
            NowledgeMemGraph::open(&graph_path, NowledgeMemGraphMode::WritableCutover).unwrap();
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        store
            .graph_mut()
            .query("CREATE (:Memory {id: 'm1', title: 'Removed after checkpoint'})")
            .unwrap();
        let initial_report = store.catch_up_search_projection(16, 1).unwrap();
        assert!(initial_report.complete);
        assert!(store
            .search_projection()
            .unwrap()
            .index()
            .document("memory:m1")
            .is_some());

        store
            .graph_mut()
            .query("MATCH (m:Memory {id: 'm1'}) DETACH DELETE m")
            .unwrap();
        store.graph_mut().database_mut().checkpoint().unwrap();
    }

    {
        let graph =
            NowledgeMemGraph::open(&graph_path, NowledgeMemGraphMode::WritableCutover).unwrap();
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let report = store.catch_up_search_projection(16, 1).unwrap();

        assert!(report.complete);
        assert_eq!(report.start_durable_epoch, Some(2));
        assert_eq!(report.end_durable_epoch, Some(3));
        assert!(store
            .search_projection()
            .unwrap()
            .index()
            .document("memory:m1")
            .is_none());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_does_not_split_graph_commit_across_projection_batches() {
    let root = unique_nowledge_mem_test_dir("embedded_projection_commit_boundary");
    let search_path = root.join("search");
    let mut db = Database::new();
    {
        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .query("CREATE (:Memory {id: 'm1', title: 'First'})")
            .unwrap();
        transaction
            .query("CREATE (:Memory {id: 'm2', title: 'Second'})")
            .unwrap();
        transaction.commit().unwrap();
    }
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

    let error = store.catch_up_search_projection(1, 1).unwrap_err();
    assert!(error.to_string().contains(
            "search projection change at commit epoch 1 requires 2 operations, exceeding configured per-batch limit 1"
        ));
    let projection = store.search_projection().unwrap();
    assert_eq!(
        projection
            .index()
            .projection_freshness()
            .durable_source_graph_commit_epoch,
        None
    );
    assert!(projection.index().document("memory:m1").is_none());
    assert!(projection.index().document("memory:m2").is_none());

    let report = store.catch_up_search_projection(2, 1).unwrap();
    assert!(report.complete);
    assert_eq!(report.end_durable_epoch, Some(1));
    assert!(store
        .search_projection()
        .unwrap()
        .index()
        .document("memory:m1")
        .is_some());
    assert!(store
        .search_projection()
        .unwrap()
        .index()
        .document("memory:m2")
        .is_some());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_handle_reuses_open_store_for_knowledge_retrieval() {
    let root = unique_nowledge_mem_test_dir("embedded_handle_retrieval");
    let search_path = root.join("search");
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    graph
        .query(
            "CREATE (:Memory {id: 'm1', title: 'Reusable handle', content: 'embedded retrieval'})",
        )
        .unwrap();
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, Some(projection)))
            .unwrap();
    handle.catch_up_search_projection(16, 1).unwrap();

    let output = handle
        .retrieve_knowledge(&KnowledgeRetrievalRequest {
            query_text: "reusable handle".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            offset: 0,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 4,
            graph_context_limit: 4,
            graph_context_max_hops: 1,
        })
        .unwrap();

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("m1"));
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_background_delta_uses_scheduler_qos() {
    let db = Database::new_with_config(DatabaseConfig {
        local_qos_policy: LocalQosPolicy {
            max_total_background_operations: Some(0),
            ..LocalQosPolicy::default()
        },
        ..DatabaseConfig::default()
    });
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'new', title: 'Scheduled facade'})")
        .unwrap();
    let projection = NowledgeMemSearchProjection::from_index(SearchIndex::in_memory());
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let request = store
        .build_search_projection_graph_delta_request_from_freshness(Some(4))
        .unwrap()
        .expect("expected graph delta request");
    let error = store
        .apply_scheduled_background_search_projection_graph_delta(request)
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("background search projection graph delta"));
    assert!(store
        .search_projection()
        .unwrap()
        .index()
        .document("memory:new")
        .is_none());
}

#[test]
fn embedded_store_scheduled_catch_up_reports_qos_deferral_without_applying() {
    let root = unique_nowledge_mem_test_dir("embedded_scheduled_catch_up_deferred");
    let search_path = root.join("search");
    let database = Database::new_with_config(DatabaseConfig {
        local_qos_policy: LocalQosPolicy {
            max_total_background_operations: Some(0),
            ..LocalQosPolicy::default()
        },
        ..DatabaseConfig::default()
    });
    let mut graph =
        NowledgeMemGraph::from_database(database, NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'new', title: 'Scheduled facade'})")
        .unwrap();
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let report = store.catch_up_search_projection_scheduled(4, 1).unwrap();

    assert_eq!(
        report.stop_reason,
        crate::SearchProjectionCatchUpStopReason::Deferred(
            crate::QosAdmissionCode::TotalBackgroundLimitExceeded
        )
    );
    assert_eq!(report.catch_up.applied_batch_count, 0);
    assert!(!report.catch_up.complete);
    let projection = store.search_projection().unwrap();
    assert_eq!(
        projection
            .index()
            .projection_freshness()
            .durable_source_graph_commit_epoch,
        None
    );
    assert!(projection.index().document("memory:new").is_none());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_handle_scheduled_catch_up_checkpoints_before_returning() {
    let root = unique_nowledge_mem_test_dir("embedded_handle_scheduled_catch_up");
    let search_path = root.join("search");
    let database = Database::new();
    let scheduler = database.local_qos_scheduler();
    let mut graph =
        NowledgeMemGraph::from_database(database, NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'new', title: 'Scheduled facade'})")
        .unwrap();
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, Some(projection)))
            .unwrap();
    let report = handle.catch_up_search_projection_scheduled(4, 1).unwrap();

    assert_eq!(
        report.stop_reason,
        crate::SearchProjectionCatchUpStopReason::CaughtUp
    );
    assert!(report.catch_up.complete);
    assert_eq!(report.catch_up.applied_batch_count, 1);
    assert_eq!(
        report.catch_up.end_applied_epoch,
        report.catch_up.end_durable_epoch
    );
    assert_eq!(scheduler.state().running_background_operations, 0);
    drop(handle);

    let reopened = SearchIndex::open(&search_path).unwrap();
    assert!(reopened.document("memory:new").is_some());
    assert_eq!(
        reopened
            .projection_freshness()
            .durable_source_graph_commit_epoch,
        report.catch_up.end_durable_epoch
    );
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_scheduled_catch_up_rejects_partial_source_interval_and_retries() {
    let root = unique_nowledge_mem_test_dir("embedded_scheduled_catch_up_budget");
    let search_path = root.join("search");
    let database = Database::new();
    let scheduler = database.local_qos_scheduler();
    let mut graph =
        NowledgeMemGraph::from_database(database, NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'first', title: 'First'})")
        .unwrap();
    graph
        .query("CREATE (:Memory {id: 'second', title: 'Second'})")
        .unwrap();
    let projection =
        NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let error = store
        .catch_up_search_projection_scheduled(1, 1)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("complete-through epoch 1 differs from source snapshot epoch 2"),
        "{error}"
    );
    let projection = store.search_projection().unwrap();
    assert!(projection.index().document("memory:first").is_none());
    assert!(projection.index().document("memory:second").is_none());
    let freshness = projection.index().projection_freshness();
    assert_eq!(freshness.source_graph_commit_epoch, None);
    assert_eq!(freshness.durable_source_graph_commit_epoch, None);
    assert!(!freshness.has_uncheckpointed_changes);
    assert_eq!(scheduler.state().running_background_operations, 0);

    let report = store.catch_up_search_projection_scheduled(2, 1).unwrap();
    assert_eq!(
        report.stop_reason,
        crate::SearchProjectionCatchUpStopReason::CaughtUp
    );
    assert!(report.catch_up.complete);
    assert_eq!(report.catch_up.applied_batch_count, 1);
    assert_eq!(report.catch_up.applied_operation_count, 2);
    assert_eq!(report.catch_up.end_durable_epoch, Some(2));
    assert_eq!(scheduler.state().running_background_operations, 0);
    drop(store);

    let reopened = SearchIndex::open(&search_path).unwrap();
    assert!(reopened.document("memory:first").is_some());
    assert!(reopened.document("memory:second").is_some());
    assert_eq!(
        reopened
            .projection_freshness()
            .durable_source_graph_commit_epoch,
        Some(2)
    );
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_retrieves_knowledge_through_search_projection() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    graph
            .query("CREATE (:Memory {id: 'mem-search', title: 'Facade retrieval', content: 'HawDB replaces LanceDB retrieval'})")
            .unwrap();
    graph
        .query("CREATE (:Entity {id: 'entity-hawdb', name: 'HawDB'})")
        .unwrap();
    graph
            .query("MATCH (m:Memory {id: 'mem-search'}), (e:Entity {id: 'entity-hawdb'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
    let projection = NowledgeMemSearchProjection::from_index(SearchIndex::in_memory());
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let delta = store
        .build_search_projection_graph_delta_request_from_freshness(Some(8))
        .unwrap()
        .expect("expected search projection delta");
    store.apply_search_projection_graph_delta(delta).unwrap();

    let retrieval = store
        .retrieve_knowledge_with_report(&KnowledgeRetrievalRequest {
            query_text: "facade retrieval".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            offset: 0,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 4,
            graph_context_limit: 4,
            graph_context_max_hops: 1,
        })
        .unwrap();
    let output = retrieval.output;
    let report = retrieval.report;

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.search.hits[0].id, "memory:mem-search");
    assert_eq!(output.diagnostics.projection_commit_lag, 0);
    assert!(!output.evidence.is_empty());
    assert_eq!(report.protocol, NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL);
    assert_eq!(report.mode, NowledgeMemGraphMode::WritableCutover);
    assert_eq!(
        report.compressed_vector_search_mode,
        CompressedVectorSearchMode::Disabled
    );
    assert_eq!(report.search_total_hits, 1);
    assert!(report.candidate_count >= 1);
    assert!(report.evidence_count >= 1);
    assert_eq!(report.text_backend, Some("bm25_text".to_string()));
    assert_eq!(
        report.json()["protocol"],
        NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL
    );
}

#[test]
fn embedded_handle_serves_search_and_graph_context_without_document_residency() {
    for compressed in [false, true] {
        assert_out_of_core_serving_backend(compressed);
    }
}

fn assert_out_of_core_serving_backend(compressed: bool) {
    let root = unique_nowledge_mem_test_dir("embedded_out_of_core_serving");
    let search_path = root.join("search");
    let mut graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    graph
            .query("CREATE (:Memory {id: 'mem-ooc', title: 'Bounded serving', content: 'HawDB out of core retrieval'})")
            .unwrap();
    graph
        .query("CREATE (:Entity {id: 'entity-ooc', name: 'OutOfCore'})")
        .unwrap();
    graph
            .query("MATCH (m:Memory {id: 'mem-ooc'}), (e:Entity {id: 'entity-ooc'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
    let graph_commit_epoch = graph.database().commit_epoch().unwrap();
    {
        let mut index = SearchIndex::open(&search_path).unwrap();
        #[cfg(feature = "vector-search")]
        if compressed {
            index.set_rabitq_projection_build_options(Default::default());
        }
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "test-embedding".to_string(),
                version: Some("v1".to_string()),
                dimension: 2,
            })
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "mem-ooc".to_string(),
                    title: "Bounded serving".to_string(),
                    body: "HawDB out of core retrieval".to_string(),
                    embedding: Some(vec![1.0, 0.0]),
                    source_id: Some("source-ooc".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                }],
                deletes: Vec::new(),
                max_operations: None,
                source_graph_commit_epoch: Some(graph_commit_epoch),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    let projection = NowledgeMemOutOfCoreSearchProjection::open(&search_path).unwrap();
    assert_eq!(projection.reader().resident_document_count(), 0);
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new_with_out_of_core_search(
            graph,
            projection,
            NowledgeMemRetrievalProjectionAdvisor::default(),
        ))
        .unwrap();

    let candidates = handle
        .search_candidates_with_report(&NowledgeMemSearchCandidateRequest::text(
            "bounded serving",
            1,
        ))
        .unwrap();
    assert_eq!(candidates.result.hits[0].id, "memory:mem-ooc");
    assert!(candidates.out_of_core_metrics.is_some());

    let hydration = handle
        .search_projection_documents_with_report(&["memory:mem-ooc".to_string()], 1)
        .unwrap();
    assert_eq!(hydration.documents[0].id, "memory:mem-ooc");
    assert_eq!(
        hydration
            .out_of_core_metrics
            .as_ref()
            .unwrap()
            .hydrated_documents,
        1
    );

    let retrieval = handle
        .retrieve_knowledge_with_report(&KnowledgeRetrievalRequest {
            query_text: "bounded serving".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 1,
            offset: 0,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: Some(4),
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 4,
            graph_context_limit: 4,
            graph_context_max_hops: 1,
        })
        .unwrap();
    assert_eq!(retrieval.output.search.hits[0].id, "memory:mem-ooc");
    assert!(!retrieval.output.graph_context_paths.is_empty());
    assert!(retrieval.out_of_core_search_metrics.is_some());
    assert_eq!(
        retrieval
            .output
            .projection_freshness
            .source_graph_commit_epoch,
        Some(graph_commit_epoch)
    );

    let vector = handle
        .query_with_params_with_report(
            "CALL vector_search($embedding, topK := 1) RETURN id, score",
            &BTreeMap::from([(
                "embedding".to_string(),
                Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
            )]),
        )
        .unwrap();
    assert_eq!(vector.output.rows.len(), 1);
    assert_eq!(
        vector.output.rows[0].get("id"),
        Some(&Value::String("memory:mem-ooc".to_string()))
    );

    let report = &vector.report.vector_execution_reports[0];
    // Disabled compression retains the raw scan for explicit RaBitQ files.
    assert_eq!(
        report.backend,
        if compressed {
            hawdb_executor::VectorExecutionBackend::ScalarFlat
        } else {
            hawdb_executor::VectorExecutionBackend::AnnProjection
        }
    );
    assert_eq!(
        report.candidate_score_source,
        if compressed {
            hawdb_executor::VectorScoreSource::RawVector
        } else {
            hawdb_executor::VectorScoreSource::AnnApproximate
        }
    );
    assert_eq!(
        report.final_score_source,
        hawdb_executor::VectorScoreSource::RawVector
    );

    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn search_projection_candidate_api_reports_metadata_pushdown() {
    let root = unique_nowledge_mem_test_dir("search_candidate_api_pushdown");
    {
        let mut index = SearchIndex::open(&root).unwrap();
        for (external_id, lifecycle_state) in [
            ("deleted", "deleted"),
            ("forgotten", "forgotten"),
            ("active", "active"),
        ] {
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: external_id.to_string(),
                    title: format!("{external_id} candidate"),
                    body: "metadata filtered candidate read".to_string(),
                    embedding: None,
                    source_id: Some("source-1".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), lifecycle_state.to_string()),
                    ]),
                })
                .unwrap();
        }
        upsert_excluded_search_projection_rows(&mut index, "excluded", 126, None);
        index.checkpoint().unwrap();
    }
    let projection = NowledgeMemSearchProjection::open(&root).unwrap();
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let request = NowledgeMemSearchCandidateRequest::text("candidate read", 10)
        .with_metadata_filters(BTreeMap::from([(
            "lifecycle_state__not_in".to_string(),
            r#"["deleted","forgotten"]"#.to_string(),
        )]));

    let output = store.search_candidates(&request).unwrap();

    assert_eq!(output.result.total_hits, 1);
    assert_eq!(output.result.hits[0].id, "memory:active");
    assert_eq!(
        output.report.protocol,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL
    );
    assert_eq!(output.report.metadata_filter_count, 1);
    assert_eq!(output.report.pushed_predicate_count, 1);
    assert_eq!(output.report.residual_predicate_count, 0);
    assert_eq!(output.report.segment_count, 2);
    assert_eq!(output.report.pruned_segment_count, 1);
    assert_eq!(output.report.scanned_segment_count, 1);
    assert!(output.report.persisted_segment_descriptor_used);
    assert_eq!(output.report.physical_range_read_count, 1);
    assert!(output.report.physical_bytes_read > 0);
    assert_eq!(output.report.filtered_out_count, 128);
    assert_eq!(
        output.report.json()["candidate_set"]["metadata_predicate_pushdown"]["field_summaries"][0]
            ["field"],
        "lifecycle_state"
    );

    let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
    accumulator.record_search_candidate_output(["memory:active"], &output);
    let evidence = accumulator.json();

    assert_eq!(evidence["ready"], true);
    assert_eq!(
        evidence["evidence_source"],
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE
    );
    assert_eq!(evidence["request_count"], 1);
    assert_eq!(evidence["candidate_primary_engine"], "hawdb");
    assert_eq!(evidence["candidate_identity"]["ready"], true);
    assert_eq!(evidence["filter_pushdown"]["ready"], true);
    assert_eq!(
        evidence["filter_pushdown"]["field_capabilities_ready"],
        true
    );
    assert_eq!(
        evidence["filter_pushdown"]["field_summary_count"],
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len()
    );
    assert_eq!(
        evidence["filter_pushdown"]["missing_required_fields"],
        serde_json::json!([])
    );
    assert!(evidence["filter_pushdown"]["field_summaries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|summary| summary["field"] == "lifecycle_state"
            && summary["source"] == "persisted_segment_descriptor_contract"));
    assert!(evidence["filter_pushdown"]["field_summaries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|summary| summary["field"] == "confidence"
            && summary["numeric_range_summary_used"] == true));

    let direct_evidence = store
        .search_candidate_shadow_evidence_json(&request, ["memory:active"])
        .unwrap();
    assert_eq!(direct_evidence["ready"], true);
    assert_eq!(
        direct_evidence["evidence_source"],
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE
    );
    assert_eq!(direct_evidence["candidate_identity"]["ready"], true);
    assert_eq!(direct_evidence["filter_pushdown"]["ready"], true);
    assert_eq!(
        direct_evidence["filter_pushdown"]["field_capabilities_ready"],
        true
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_handle_hydrates_only_explicitly_bounded_search_documents() {
    let root = unique_nowledge_mem_test_dir("bounded_search_document_hydration");
    let mut index = SearchIndex::open(&root).unwrap();
    index
        .upsert_projection_row(SearchProjectionRow {
            kind: SearchProjectionKind::SourceChunk,
            external_id: "source-1-chunk-0".to_string(),
            title: "Introduction".to_string(),
            body: "bounded projection payload".to_string(),
            embedding: None,
            source_id: Some("source-1".to_string()),
            metadata: BTreeMap::from([("chunk_index".to_string(), "0".to_string())]),
        })
        .unwrap();
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
        graph,
        Some(NowledgeMemSearchProjection::from_index(index)),
    ))
    .unwrap();

    let documents = handle
        .search_projection_documents(&["source_chunk:source-1-chunk-0".to_string()], 1)
        .unwrap();

    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0].content, "bounded projection payload");
    assert_eq!(documents[0].metadata["chunk_index"], "0");
    assert!(handle
        .search_projection_documents(
            &[
                "source_chunk:source-1-chunk-0".to_string(),
                "source_chunk:source-1-chunk-1".to_string(),
            ],
            1,
        )
        .is_err());
    let snapshot = handle.runtime_governor_snapshot().unwrap();
    assert_eq!(snapshot.admissions, 1);
    assert_eq!(snapshot.completions, 1);
}

#[test]
fn embedded_handle_search_hydration_rejects_payload_over_budget() {
    for out_of_core in [false, true] {
        let root = unique_nowledge_mem_test_dir("search_hydration_budget");
        let graph = NowledgeMemGraph::from_database(
            Database::new_with_config(DatabaseConfig {
                max_read_result_payload_bytes: Some(64),
                ..DatabaseConfig::default()
            }),
            NowledgeMemGraphMode::ShadowReadOnly,
        );
        let mut index = if out_of_core {
            SearchIndex::open(&root).unwrap()
        } else {
            SearchIndex::in_memory()
        };
        index
            .upsert(crate::SearchDocument {
                id: "source_chunk:large".to_string(),
                title: "Large".to_string(),
                content: "x".repeat(256),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        let store = if out_of_core {
            index.checkpoint().unwrap();
            drop(index);
            NowledgeMemEmbeddedStore::new_with_out_of_core_search(
                graph,
                NowledgeMemOutOfCoreSearchProjection::open(&root).unwrap(),
                NowledgeMemRetrievalProjectionAdvisor::default(),
            )
        } else {
            NowledgeMemEmbeddedStore::new(
                graph,
                Some(NowledgeMemSearchProjection::from_index(index)),
            )
        };
        let handle = NowledgeMemEmbeddedStoreHandle::new(store).unwrap();
        let ids = ["source_chunk:large".to_string()];
        let error = handle.search_projection_documents(&ids, 1).unwrap_err();

        assert!(error.to_string().contains("payload bytes"));
        assert!(
            matches!(&error, HawDBError::ReadBudgetExceeded(cause)
                    if cause.resource == ReadBudgetResource::PayloadBytes && cause.limit == 64),
            "hydration lost its result budget cause (out_of_core={out_of_core}): {error}"
        );
        let request_error = handle.search_projection_documents(&ids, 0).unwrap_err();
        assert!(matches!(request_error, HawDBError::Execution(_)));
        let snapshot = handle.runtime_governor_snapshot().unwrap();
        assert_eq!(snapshot.admissions, 1);
        assert_eq!(snapshot.completions, 1);
        drop(handle);
        if out_of_core {
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}

#[test]
fn embedded_store_handle_checkpoints_external_projection_delta_before_returning() {
    let root = unique_nowledge_mem_test_dir("external_projection_delta_checkpoint");
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let projection = NowledgeMemSearchProjection::open(&root).unwrap();
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, Some(projection)))
            .unwrap();

    let report = handle
        .apply_search_projection_delta_and_checkpoint(SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "memory-1".to_string(),
                title: "Imported memory".to_string(),
                body: "Preserved legacy projection payload".to_string(),
                embedding: Some(vec![0.25, 0.75]),
                source_id: Some("source-1".to_string()),
                metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
            }],
            deletes: Vec::new(),
            max_operations: Some(1),
            source_graph_commit_epoch: Some(7),
        })
        .unwrap();
    assert_eq!(report.upserted_documents, 1);
    drop(handle);

    let reopened = SearchIndex::open(&root).unwrap();
    let freshness = reopened.projection_freshness();
    assert_eq!(freshness.document_count, 1);
    assert_eq!(freshness.source_graph_commit_epoch, Some(7));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_query_runtime_executes_vector_seed_with_observability() {
    let mut index = SearchIndex::in_memory();
    for (external_id, embedding) in [("nearest", vec![1.0, 0.0]), ("farther", vec![0.0, 1.0])] {
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: external_id.to_string(),
                title: external_id.to_string(),
                body: String::new(),
                embedding: Some(embedding),
                source_id: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
    }
    let projection = NowledgeMemSearchProjection::from_index(index);
    let graph = NowledgeMemGraph::from_database(
        Database::new_with_config(DatabaseConfig {
            slow_query_log_threshold_micros: 0,
            ..DatabaseConfig::default()
        }),
        NowledgeMemGraphMode::WritableCutover,
    );
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let parameters = BTreeMap::from([(
        "embedding".to_string(),
        Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
    )]);

    let output = store
        .query_with_params_with_report_options(
            "CALL vector_search($embedding, topK := 1) RETURN id, score",
            &parameters,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: Some(0),
            },
        )
        .unwrap();

    assert_eq!(
        output.output.rows[0].get("id"),
        Some(&Value::String("memory:nearest".to_string()))
    );
    assert_eq!(output.report.statement_kind, "vector_search");
    assert_eq!(
        output.report.physical_operator_counts.get("VectorSeedScan"),
        Some(&1)
    );
    assert_eq!(output.report.plan_cache_lookup.as_deref(), Some("bypass"));
    assert_eq!(
        output.report.vector_execution_reports[0].scalar_filtered_count,
        0
    );
    assert_eq!(output.report.vector_execution_reports.len(), 1);
    assert_eq!(
        output.report.vector_execution_reports[0].backend,
        hawdb_executor::VectorExecutionBackend::ScalarFlat
    );
    assert_eq!(
        output.report.vector_execution_reports[0].compression_mode,
        hawdb_executor::VectorCompressionMode::Disabled
    );
    assert_eq!(
        output.report.vector_execution_reports[0].candidate_source,
        hawdb_plan_cypher::VectorCandidateSource::Scalar
    );
    assert_eq!(
        output.report.vector_execution_reports[0].backend_selection_reason,
        Some(hawdb_plan_cypher::VectorBackendSelectionReason::CompressionDisabled)
    );
    assert_eq!(
        output.report.vector_execution_reports[0].estimated_raw_vector_bytes,
        Some(16)
    );
    assert_eq!(
        output.report.vector_execution_reports[0].filter_selectivity_per_million,
        Some(0)
    );
    assert_eq!(
        output.report.vector_execution_reports[0].final_score_source,
        hawdb_executor::VectorScoreSource::RawVector
    );
    assert_eq!(
        output.report.vector_execution_reports[0].index_covered_document_count,
        Some(2)
    );
    assert_eq!(
        output.report.vector_execution_reports[0].index_candidate_document_count,
        Some(2)
    );
    assert_eq!(
        output.report.vector_execution_reports[0].index_coverage_complete,
        Some(true)
    );
    assert!(output.report.vector_execution_reports[0]
        .fallback_reason_codes
        .is_empty());
    assert!(output.report.vector_execution_reports[0].raw_vector_bytes_read > 0);

    let explain = store
        .query_with_params_with_report(
            "EXPLAIN ANALYZE CALL vector_search($embedding, topK := 1) RETURN id, score",
            &parameters,
        )
        .unwrap();
    let Value::List(vector_reports) = explain.output.rows[0]
        .get("vector_execution_reports")
        .expect("explain analyze vector reports")
    else {
        panic!("expected vector execution report list");
    };
    assert_eq!(vector_reports.len(), 1);
    let Some(Value::String(plan)) = explain.output.rows[0].get("plan") else {
        panic!("expected explain plan");
    };
    assert!(plan.contains("VectorSeedScan embedding=$embedding"));
    assert!(plan.contains("priority=128 max_parallelism=16"));
    assert!(plan.contains("max_working_memory_bytes=Some(67108864)"));
    assert!(plan.contains("Filter->VectorCandidateScan->RawVectorRerank->TopK"));
    assert!(!plan.contains("[1"));

    let slow_log = store.graph().database().slow_query_log_jsonl().unwrap();
    let slow_event: serde_json::Value =
        serde_json::from_str(slow_log.lines().next().unwrap()).unwrap();
    assert_eq!(slow_event["vector_execution_report_count"], 1);
    assert_eq!(
        slow_event["vector_execution_reports"][0]["backend"],
        "scalar_flat"
    );
    assert_eq!(
        slow_event["vector_execution_reports"][0]["compression_mode"],
        "disabled"
    );
    assert_eq!(
        slow_event["vector_execution_reports"][0]["backend_selection_reason"],
        "compression_disabled"
    );
    assert_eq!(
        slow_event["vector_execution_reports"][0]["estimated_raw_vector_bytes"],
        16
    );
    assert_eq!(
        slow_event["vector_execution_reports"][0]["index_coverage_complete"],
        true
    );
    assert!(slow_event.get("query_text").is_none());
    assert!(!slow_log.contains("1.0"));
}

#[test]
fn embedded_query_runtime_feeds_vector_candidates_into_graph_match() {
    let mut index = SearchIndex::in_memory();
    for (external_id, embedding) in [("nearest", vec![1.0, 0.0]), ("farther", vec![0.8, 0.2])] {
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: external_id.to_string(),
                title: external_id.to_string(),
                body: String::new(),
                embedding: Some(embedding),
                source_id: None,
                metadata: BTreeMap::from([(
                    "space_id".to_string(),
                    if external_id == "nearest" {
                        "selected"
                    } else {
                        "other"
                    }
                    .to_string(),
                )]),
            })
            .unwrap();
    }
    let projection = NowledgeMemSearchProjection::from_index(index);
    let mut graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'nearest', space_id: 'selected'})")
        .unwrap();
    graph
        .query("CREATE (:Memory {id: 'farther', space_id: 'other'})")
        .unwrap();
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let parameters = BTreeMap::from([
        (
            "embedding".to_string(),
            Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
        ),
        (
            "space_id".to_string(),
            Value::String("selected".to_string()),
        ),
    ]);
    let query = "CALL vector_search($embedding, topK := 2) YIELD id, score \
                     MATCH (m:Memory) WHERE m.space_id = $space_id \
                     RETURN m.id AS memory_id, score";
    let logical =
        crate::planner::plan_with_params(&crate::cypher::parse(query).unwrap(), &parameters)
            .unwrap();
    let plan = crate::optimizer::CascadesOptimizer::new(Default::default())
        .optimize(&logical)
        .explain(0);
    let vector_seed_line = plan
        .lines()
        .find(|line| line.contains("VectorSeedScan"))
        .expect("vector seed plan");
    assert!(vector_seed_line.contains("metadata_filter_fields=[\"space_id\"]"));
    assert!(!vector_seed_line.contains("selected"));

    let output = store
        .query_with_params_with_report_options(
            query,
            &parameters,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 1);
    assert_eq!(
        output.output.rows[0].get("memory_id"),
        Some(&Value::String("nearest".to_string()))
    );
    assert!(matches!(
        output.output.rows[0].get("score"),
        Some(Value::Float(score)) if *score > 0.0
    ));
    assert!(!output.output.rows[0].contains_key("external_id"));
    assert_eq!(output.report.statement_kind, "vector_graph_search");
    assert_eq!(output.report.plan_cache_lookup.as_deref(), Some("bypass"));
    assert_eq!(
        output.report.physical_operator_counts.get("VectorSeedScan"),
        Some(&1)
    );
    assert_eq!(
        output
            .report
            .physical_operator_counts
            .get("NodeColumnLookupExec"),
        Some(&1)
    );
    assert!(output.report.physical_plan_captured);
    assert_eq!(
        output.report.vector_execution_reports[0].scalar_filtered_count,
        1
    );
}

#[test]
fn embedded_query_runtime_expands_from_vector_seed_candidates() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert_projection_row(SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "nearest".to_string(),
            title: "nearest".to_string(),
            body: String::new(),
            embedding: Some(vec![1.0, 0.0]),
            source_id: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    let projection = NowledgeMemSearchProjection::from_index(index);
    let mut graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    graph.query("CREATE (:Memory {id: 'nearest'})").unwrap();
    graph.query("CREATE (:Entity {id: 'entity-rust'})").unwrap();
    graph
        .query(
            "MATCH (m:Memory {id: 'nearest'}), (e:Entity {id: 'entity-rust'}) \
                 CREATE (m)-[:MENTIONS]->(e)",
        )
        .unwrap();
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let parameters = BTreeMap::from([(
        "embedding".to_string(),
        Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
    )]);

    let output = store
        .query_with_params_with_report_options(
            "CALL vector_search($embedding, topK := 1) YIELD id, score \
                 MATCH (m:Memory)-[:MENTIONS]->(e:Entity) \
                 RETURN m.id AS memory_id, e.id AS entity_id, score",
            &parameters,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
        )
        .unwrap();

    assert_eq!(output.output.rows.len(), 1);
    assert_eq!(
        output.output.rows[0].get("memory_id"),
        Some(&Value::String("nearest".to_string()))
    );
    assert_eq!(
        output.output.rows[0].get("entity_id"),
        Some(&Value::String("entity-rust".to_string()))
    );
    assert!(matches!(
        output.output.rows[0].get("score"),
        Some(Value::Float(score)) if *score > 0.0
    ));
    assert_eq!(
        output
            .report
            .physical_operator_counts
            .get("AdjacencyExpandExec"),
        Some(&1)
    );
    assert_eq!(output.report.graph_expansion_reports.len(), 1);
    let graph_report = &output.report.graph_expansion_reports[0];
    assert_eq!(graph_report.seed_count, 1);
    assert_eq!(graph_report.expanded_node_count, 1);
    assert_eq!(graph_report.expanded_edge_count, 1);
    assert_eq!(graph_report.relation_types, vec!["MENTIONS".to_string()]);
    assert_eq!(graph_report.min_hops, 1);
    assert_eq!(graph_report.max_hops, 1);
    assert_eq!(graph_report.reranked_seed_count, 1);
    assert_eq!(graph_report.returned_count, 1);
    assert!(graph_report.payload_bytes_used > 0);
    assert!(!graph_report.truncated());

    let explain = store
        .query_with_params_with_report(
            "EXPLAIN ANALYZE CALL vector_search($embedding, topK := 1) YIELD id, score \
                 MATCH (m:Memory)-[:MENTIONS]->(e:Entity) \
                 RETURN m.id AS memory_id, e.id AS entity_id, score",
            &parameters,
        )
        .unwrap();
    let Value::List(graph_reports) = explain.output.rows[0]
        .get("graph_expansion_reports")
        .expect("explain analyze graph reports")
    else {
        panic!("expected graph expansion report list");
    };
    assert_eq!(graph_reports.len(), 1);
}

#[test]
fn embedded_query_runtime_admits_vector_seed_graph_fanout() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert_projection_row(SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "fanout-seed".to_string(),
            title: "fanout seed".to_string(),
            body: String::new(),
            embedding: Some(vec![1.0, 0.0]),
            source_id: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    let projection = NowledgeMemSearchProjection::from_index(index);
    let mut graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    graph.query("CREATE (:Memory {id: 'fanout-seed'})").unwrap();
    for index in 0..65 {
        graph
            .query(&format!("CREATE (:Entity {{id: 'entity-{index}'}})"))
            .unwrap();
        graph
            .query(&format!(
                "MATCH (m:Memory {{id: 'fanout-seed'}}), \
                     (e:Entity {{id: 'entity-{index}'}}) \
                     CREATE (m)-[:MENTIONS]->(e)"
            ))
            .unwrap();
    }
    let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let parameters = BTreeMap::from([(
        "embedding".to_string(),
        Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
    )]);

    let output = store
        .query_with_params_with_report(
            "CALL vector_search($embedding, topK := 1) YIELD id, score \
                 MATCH (m:Memory)-[:MENTIONS]->(e:Entity) \
                 RETURN e.id AS entity_id",
            &parameters,
        )
        .unwrap();
    assert_eq!(output.output.rows.len(), 65);
    let identities = output
        .output
        .rows
        .iter()
        .map(|row| match &row["entity_id"] {
            Value::String(id) => id.clone(),
            other => panic!("unexpected entity identity: {other:?}"),
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        identities,
        (0..65).map(|index| format!("entity-{index}")).collect()
    );
    let report = &output.report.graph_expansion_reports[0];
    assert_eq!(report.returned_count, 65);
    assert_eq!(report.expanded_node_count, 65);
    assert_eq!(report.expanded_edge_count, 65);
    assert_eq!(report.candidate_limit, usize::MAX);
    assert_eq!(report.payload_byte_limit, usize::MAX);
    assert!(!report.truncated());
}

#[test]
fn embedded_query_runtime_rejects_vector_seed_expansion_over_two_hops() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let mut store = NowledgeMemEmbeddedStore::new(
        graph,
        Some(NowledgeMemSearchProjection::from_index(
            SearchIndex::in_memory(),
        )),
    );
    let parameters = BTreeMap::from([(
        "embedding".to_string(),
        Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
    )]);

    let error = store
        .query_with_params_with_report(
            "CALL vector_search($embedding, topK := 1) YIELD id, score \
                 MATCH (m:Memory)-[:MENTIONS*1..3]->(e:Entity) \
                 RETURN e.id AS entity_id",
            &parameters,
        )
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("vector-seeded graph expansion supports at most 2 hops"));
}

#[test]
fn graph_only_query_runtime_rejects_vector_seed_capability() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let mut store = NowledgeMemEmbeddedStore::new(graph, None);
    let parameters = BTreeMap::from([(
        "embedding".to_string(),
        Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
    )]);

    let error = store
        .query_with_params_with_report(
            "CALL vector_search($embedding, topK := 1) RETURN id, score",
            &parameters,
        )
        .unwrap_err();

    assert!(error.to_string().contains("search projection"));
}

#[test]
fn embedded_store_reopens_search_projection_with_descriptor_pruning() {
    let root = unique_nowledge_mem_test_dir("search_candidate_library_reopen_pruning");
    let graph_path = root.join("graph");
    let search_path = root.join("search");
    {
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'active', title: 'Active candidate', space_id: 'default'})")
            .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let mut index = SearchIndex::open(&search_path).unwrap();
        for (external_id, lifecycle_state, importance) in [
            ("deleted", "deleted", "0.95"),
            ("forgotten", "forgotten", "0.90"),
            ("active", "active", "0.80"),
        ] {
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: external_id.to_string(),
                    title: format!("{external_id} candidate"),
                    body: "checkpointed candidate read".to_string(),
                    embedding: None,
                    source_id: Some("source-1".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("unit_type".to_string(), "memory".to_string()),
                        ("lifecycle_state".to_string(), lifecycle_state.to_string()),
                        ("importance".to_string(), importance.to_string()),
                    ]),
                })
                .unwrap();
        }
        upsert_excluded_search_projection_rows(&mut index, "excluded", 126, None);
        index.checkpoint().unwrap();
    }

    let options = NowledgeMemOpenOptions::with_search_projection(
        graph_path,
        search_path,
        NowledgeMemGraphMode::ShadowReadOnly,
    );
    let (store, open_report) = NowledgeMemEmbeddedStore::open_with_options(options).unwrap();
    let request = NowledgeMemSearchCandidateRequest::text("candidate read", 10)
        .with_metadata_filters(BTreeMap::from([
            (
                "lifecycle_state__not_in".to_string(),
                r#"["deleted","forgotten"]"#.to_string(),
            ),
            ("importance__gte".to_string(), "0.8".to_string()),
        ]));

    let output = store.search_candidates(&request).unwrap();

    assert!(open_report.graph_opened);
    assert!(open_report.search_projection_opened);
    assert_eq!(output.result.total_hits, 1);
    assert_eq!(output.result.hits[0].id, "memory:active");
    assert_eq!(output.report.metadata_filter_count, 2);
    assert_eq!(output.report.pushed_predicate_count, 2);
    assert_eq!(output.report.residual_predicate_count, 0);
    assert!(output.report.persisted_segment_descriptor_used);
    assert_eq!(output.report.segment_count, 2);
    assert_eq!(output.report.pruned_segment_count, 1);
    assert_eq!(output.report.scanned_segment_count, 1);
    assert_eq!(output.report.physical_range_read_count, 1);
    assert!(output.report.physical_bytes_read > 0);
    assert_eq!(output.report.filtered_out_count, 128);
    assert!(output
        .report
        .candidate_set
        .metadata_predicate_pushdown
        .field_summaries
        .iter()
        .any(|summary| summary.field == "lifecycle_state" && summary.value_summary_used));
    assert!(output
        .report
        .candidate_set
        .metadata_predicate_pushdown
        .field_summaries
        .iter()
        .any(|summary| summary.field == "importance" && summary.numeric_range_summary_used));

    let direct_evidence = store
        .search_candidate_shadow_evidence_json(&request, ["memory:active"])
        .unwrap();
    assert_eq!(direct_evidence["ready"], true);
    assert_eq!(
        direct_evidence["filter_pushdown"]["field_capabilities_ready"],
        true
    );
    assert_eq!(
        direct_evidence["filter_pushdown"]["missing_numeric_range_fields"],
        serde_json::json!([])
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn search_projection_candidate_api_reports_vector_generation_inputs() {
    let root = unique_nowledge_mem_test_dir("search_candidate_api_vector");
    {
        let mut index = SearchIndex::open(&root).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 2,
            })
            .unwrap();
        for (external_id, lifecycle_state, embedding) in [
            ("aaa-deleted", "deleted", vec![1.0, 0.0]),
            ("aab-forgotten", "forgotten", vec![1.0, 0.0]),
            ("zza-active", "active", vec![1.0, 0.0]),
            ("zzb-other", "active", vec![0.6, 0.8]),
        ] {
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: external_id.to_string(),
                    title: format!("{external_id} vector candidate"),
                    body: "vector candidate read".to_string(),
                    embedding: Some(embedding),
                    source_id: Some("source-vector".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), lifecycle_state.to_string()),
                    ]),
                })
                .unwrap();
        }
        upsert_excluded_search_projection_rows(
            &mut index,
            "aac-excluded",
            126,
            Some(vec![1.0, 0.0]),
        );
        index.checkpoint().unwrap();
    }
    let projection = NowledgeMemSearchProjection::open(&root).unwrap();
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let request = NowledgeMemSearchCandidateRequest::vector(vec![1.0, 0.0], 10)
        .with_offset(1)
        .with_rank_window(Some(2))
        .with_metadata_filters(BTreeMap::from([(
            "lifecycle_state__not_in".to_string(),
            r#"["deleted","forgotten"]"#.to_string(),
        )]));

    let output = store.search_candidates(&request).unwrap();

    assert_eq!(output.result.total_hits, 2);
    assert_eq!(output.result.offset, 1);
    assert_eq!(output.result.hits.len(), 1);
    assert_eq!(output.result.hits[0].id, "memory:zzb-other");
    assert_eq!(output.result.hits[0].vector_rank, Some(2));
    assert_eq!(output.report.mode, SearchMode::Vector);
    assert_eq!(output.report.query_embedding_dimension, Some(2));
    assert_eq!(output.report.offset, 1);
    assert_eq!(output.report.rank_window, Some(2));
    assert_eq!(
        output.report.retriever_backends.get("vector"),
        Some(&"scalar_vector_scan".to_string())
    );
    assert_eq!(output.report.retriever_available.get("vector"), Some(&true));
    assert_eq!(output.report.retriever_available.get("text"), Some(&false));
    assert_eq!(
        output.report.retriever_candidate_counts.get("vector"),
        Some(&2)
    );
    assert_eq!(
        output
            .report
            .retriever_candidate_score_sources
            .get("vector"),
        Some(&"raw_vector".to_string())
    );
    assert_eq!(
        output.report.retriever_final_score_sources.get("vector"),
        Some(&"raw_vector".to_string())
    );
    assert_eq!(output.report.pushed_predicate_count, 1);
    assert_eq!(output.report.pruned_segment_count, 1);
    assert_eq!(output.report.filtered_out_count, 128);
    assert!(output.report.persisted_segment_descriptor_used);
    assert_eq!(output.report.json()["query_embedding_dimension"], 2);
    assert_eq!(output.report.json()["offset"], 1);
    assert_eq!(
        output.report.json()["retriever_candidate_counts"]["vector"],
        2
    );
    assert_eq!(
        output.report.json()["retriever_final_score_sources"]["vector"],
        "raw_vector"
    );
    assert!(!output.report.json().to_string().contains("[1.0,0.0]"));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn search_candidate_shadow_bridge_aggregates_text_and_vector_leg_evidence() {
    let root = unique_nowledge_mem_test_dir("search_candidate_bridge_retrievers");
    {
        let mut index = SearchIndex::open(&root).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 2,
            })
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "mem-leg".to_string(),
                    title: "Retriever leg candidate".to_string(),
                    body: "retriever leg candidate read".to_string(),
                    embedding: Some(vec![1.0, 0.0]),
                    source_id: Some("source-leg".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                }],
                deletes: Vec::new(),
                max_operations: None,
                source_graph_commit_epoch: Some(31),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    let projection = NowledgeMemSearchProjection::open(&root).unwrap();
    let text_request = NowledgeMemSearchCandidateRequest::text("retriever leg", 10)
        .with_metadata_filters(BTreeMap::from([(
            "lifecycle_state__not_in".to_string(),
            r#"["deleted","forgotten"]"#.to_string(),
        )]));
    let vector_request = NowledgeMemSearchCandidateRequest::vector(vec![1.0, 0.0], 10)
        .with_metadata_filters(BTreeMap::from([(
            "lifecycle_state__not_in".to_string(),
            r#"["deleted","forgotten"]"#.to_string(),
        )]));
    let text_output = projection
        .search_candidates_with_report(&text_request)
        .unwrap();
    let vector_output = projection
        .search_candidates_with_report(&vector_request)
        .unwrap();
    let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();

    accumulator.record_search_candidate_output(["memory:mem-leg"], &text_output);
    accumulator.record_search_candidate_output(["memory:mem-leg"], &vector_output);
    let evidence = accumulator.json();

    assert_eq!(evidence["ready"], true);
    assert_eq!(evidence["request_count"], 2);
    assert_eq!(evidence["text_retriever_ready"], true);
    assert_eq!(evidence["vector_retriever_ready"], true);
    assert_eq!(evidence["fts_top_k_overlap_ready"], true);
    assert_eq!(evidence["vector_top_k_overlap_ready"], true);
    assert_eq!(evidence["top_k_overlap_observed"]["fts"], true);
    assert_eq!(evidence["top_k_overlap_observed"]["vector"], true);
    assert_eq!(
        evidence["candidate_readiness"]["source_chunk_identity_ready"],
        false
    );
    assert_eq!(evidence["candidate_readiness"]["fail_soft_observed"], false);
    assert_eq!(
        evidence["candidate_readiness"]["projection_marker_status_visible"],
        true
    );
    assert_eq!(
        evidence["candidate_readiness"]["projection_watermark_ready"],
        true
    );
    assert_eq!(
        evidence["candidate_readiness"]["embedding_identity_ready"],
        true
    );
    assert_eq!(evidence["retriever_leg_candidate_counts"]["text"], 1);
    assert_eq!(evidence["retriever_leg_candidate_counts"]["vector"], 1);
    assert_eq!(
        evidence["filter_pushdown"]["field_capabilities_ready"],
        true
    );
    assert_eq!(evidence["blocker_codes"], serde_json::json!([]));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn search_projection_candidate_api_preserves_source_chunk_identity() {
    let root = unique_nowledge_mem_test_dir("search_candidate_api_source_chunk");
    {
        let mut index = SearchIndex::open(&root).unwrap();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::SourceChunk,
                external_id: "chunk-1".to_string(),
                title: "Source chunk candidate".to_string(),
                body: "source chunk identity candidate read".to_string(),
                embedding: None,
                source_id: Some("source-1".to_string()),
                metadata: BTreeMap::from([
                    ("space_id".to_string(), "default".to_string()),
                    ("lifecycle_state".to_string(), "active".to_string()),
                ]),
            })
            .unwrap();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "memory-1".to_string(),
                title: "Memory candidate".to_string(),
                body: "source chunk identity candidate read".to_string(),
                embedding: None,
                source_id: Some("source-1".to_string()),
                metadata: BTreeMap::from([
                    ("space_id".to_string(), "default".to_string()),
                    ("lifecycle_state".to_string(), "active".to_string()),
                ]),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    let projection = NowledgeMemSearchProjection::open(&root).unwrap();
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let request =
        NowledgeMemSearchCandidateRequest::text("source chunk identity", 10).with_metadata_filters(
            BTreeMap::from([("kind__in".to_string(), r#"["source_chunk"]"#.to_string())]),
        );

    let output = store.search_candidates(&request).unwrap();

    assert_eq!(output.result.total_hits, 1);
    let hit = &output.result.hits[0];
    assert_eq!(hit.id, "source_chunk:chunk-1");
    assert_eq!(hit.kind.as_deref(), Some("source_chunk"));
    assert_eq!(hit.external_id.as_deref(), Some("chunk-1"));
    assert_eq!(hit.source_id.as_deref(), Some("source-1"));
    assert_eq!(
        output.report.returned_kind_counts.get("source_chunk"),
        Some(&1)
    );
    assert_eq!(output.report.returned_missing_external_id_count, 0);
    assert_eq!(output.report.returned_missing_source_id_count, 0);
    assert_eq!(output.report.metadata_filter_count, 1);
    assert_eq!(output.report.pushed_predicate_count, 1);
    assert_eq!(
        output.report.json()["returned_kind_counts"]["source_chunk"],
        1
    );
    assert!(!output
        .report
        .json()
        .to_string()
        .contains("source chunk identity candidate read"));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn search_candidate_readiness_reports_lancedb_replacement_ready_shape() {
    let root = unique_nowledge_mem_test_dir("search_candidate_readiness_ready");
    {
        let mut index = SearchIndex::open(&root).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 2,
            })
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::SourceChunk,
                    external_id: "chunk-ready".to_string(),
                    title: "Ready source chunk".to_string(),
                    body: "ready candidate replacement body".to_string(),
                    embedding: Some(vec![1.0, 0.0]),
                    source_id: Some("source-ready".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                }],
                deletes: Vec::new(),
                max_operations: None,
                source_graph_commit_epoch: Some(19),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    let projection = NowledgeMemSearchProjection::open(&root).unwrap();
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, Some(projection)))
            .unwrap();
    let request =
        NowledgeMemSearchCandidateRequest::text("ready source chunk", 10).with_metadata_filters(
            BTreeMap::from([("kind__in".to_string(), r#"["source_chunk"]"#.to_string())]),
        );
    let options = NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read()
        .with_text_retriever(true)
        .with_source_chunk_identity(true)
        .with_embedding_identity("bge-m3", 2);

    let readiness = handle
        .search_candidate_readiness(&request, &options)
        .unwrap();

    assert!(readiness.ready);
    assert!(readiness.present);
    assert_eq!(
        readiness.protocol,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL
    );
    assert!(readiness.metadata_pushdown_ready);
    assert!(readiness.segment_descriptor_ready);
    assert!(readiness.text_retriever_ready);
    assert!(readiness.source_chunk_identity_ready);
    assert!(readiness.projection_marker_status_visible);
    assert!(readiness.projection_watermark_ready);
    assert!(readiness.embedding_identity_ready);
    assert!(readiness.blocker_codes.is_empty());
    assert_eq!(
        readiness
            .candidate_report
            .projection_source_graph_commit_epoch,
        Some(19)
    );
    assert_eq!(
        readiness
            .candidate_report
            .projection_embedding_model
            .as_deref(),
        Some("bge-m3")
    );
    assert_eq!(
        readiness.candidate_report.projection_embedding_dimension,
        Some(2)
    );
    assert_eq!(
        readiness
            .candidate_report
            .returned_kind_counts
            .get("source_chunk"),
        Some(&1)
    );
    assert_eq!(readiness.json()["projection_watermark_ready"], true);
    assert_eq!(readiness.json()["embedding_identity_ready"], true);
    assert!(!readiness
        .json()
        .to_string()
        .contains("ready candidate replacement body"));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn search_candidate_readiness_blocks_missing_source_chunk_identity() {
    let root = unique_nowledge_mem_test_dir("search_candidate_readiness_blocked");
    {
        let mut index = SearchIndex::open(&root).unwrap();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "memory-only".to_string(),
                title: "Memory only candidate".to_string(),
                body: "memory only candidate replacement body".to_string(),
                embedding: None,
                source_id: Some("source-memory".to_string()),
                metadata: BTreeMap::from([
                    ("space_id".to_string(), "default".to_string()),
                    ("lifecycle_state".to_string(), "active".to_string()),
                ]),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    let projection = NowledgeMemSearchProjection::open(&root).unwrap();
    let request = NowledgeMemSearchCandidateRequest::text("memory only candidate", 10);
    let options =
        NowledgeMemSearchCandidateReadinessOptions::default().with_source_chunk_identity(true);

    let readiness = projection
        .search_candidate_readiness(&request, &options)
        .unwrap();

    assert!(!readiness.ready);
    assert_eq!(readiness.candidate_report.returned_hit_count, 1);
    assert!(!readiness.source_chunk_identity_ready);
    assert!(readiness
        .blocker_codes
        .iter()
        .any(|code| code == "search_candidate_source_chunk_identity_missing"));
    assert!(!readiness
        .json()
        .to_string()
        .contains("memory only candidate replacement body"));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn search_candidate_readiness_blocks_missing_projection_watermark() {
    let root = unique_nowledge_mem_test_dir("search_candidate_readiness_watermark");
    {
        let mut index = SearchIndex::open(&root).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 2,
            })
            .unwrap();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::SourceChunk,
                external_id: "chunk-no-watermark".to_string(),
                title: "No watermark source chunk".to_string(),
                body: "candidate watermark body".to_string(),
                embedding: Some(vec![1.0, 0.0]),
                source_id: Some("source-no-watermark".to_string()),
                metadata: BTreeMap::from([
                    ("space_id".to_string(), "default".to_string()),
                    ("lifecycle_state".to_string(), "active".to_string()),
                ]),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    let projection = NowledgeMemSearchProjection::open(&root).unwrap();
    let request = NowledgeMemSearchCandidateRequest::text("no watermark source chunk", 10)
        .with_metadata_filters(BTreeMap::from([(
            "kind__in".to_string(),
            r#"["source_chunk"]"#.to_string(),
        )]));
    let options = NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read()
        .with_text_retriever(true)
        .with_source_chunk_identity(true)
        .with_embedding_identity("bge-m3", 2);

    let readiness = projection
        .search_candidate_readiness(&request, &options)
        .unwrap();

    assert!(!readiness.ready);
    assert!(readiness.text_retriever_ready);
    assert!(readiness.source_chunk_identity_ready);
    assert!(!readiness.projection_watermark_ready);
    assert!(readiness.embedding_identity_ready);
    assert!(readiness
        .blocker_codes
        .iter()
        .any(|code| code == "search_candidate_projection_watermark_missing"));
    assert!(!readiness
        .json()
        .to_string()
        .contains("candidate watermark body"));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn search_candidate_readiness_lancedb_default_requires_embedding_identity() {
    let root = unique_nowledge_mem_test_dir("search_candidate_readiness_manifest_required");
    {
        let mut index = SearchIndex::open(&root).unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::SourceChunk,
                    external_id: "chunk-no-manifest".to_string(),
                    title: "No manifest source chunk".to_string(),
                    body: "candidate manifest body".to_string(),
                    embedding: Some(vec![1.0, 0.0]),
                    source_id: Some("source-no-manifest".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                }],
                deletes: Vec::new(),
                max_operations: None,
                source_graph_commit_epoch: Some(29),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    let projection = NowledgeMemSearchProjection::open(&root).unwrap();
    let request = NowledgeMemSearchCandidateRequest::text("no manifest source chunk", 10)
        .with_metadata_filters(BTreeMap::from([(
            "kind__in".to_string(),
            r#"["source_chunk"]"#.to_string(),
        )]));
    let options = NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read()
        .with_text_retriever(true)
        .with_source_chunk_identity(true);

    let readiness = projection
        .search_candidate_readiness(&request, &options)
        .unwrap();

    assert!(!readiness.ready);
    assert!(readiness.projection_watermark_ready);
    assert!(!readiness.embedding_identity_ready);
    assert_eq!(readiness.candidate_report.projection_embedding_model, None);
    assert_eq!(
        readiness.candidate_report.projection_embedding_dimension,
        Some(2)
    );
    assert!(readiness
        .blocker_codes
        .iter()
        .any(|code| code == "search_candidate_embedding_identity_not_ready"));
    assert!(!readiness
        .json()
        .to_string()
        .contains("candidate manifest body"));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn search_candidate_readiness_blocks_embedding_identity_mismatch() {
    let root = unique_nowledge_mem_test_dir("search_candidate_readiness_embedding");
    {
        let mut index = SearchIndex::open(&root).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 2,
            })
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::SourceChunk,
                    external_id: "chunk-embedding".to_string(),
                    title: "Embedding identity source chunk".to_string(),
                    body: "candidate embedding body".to_string(),
                    embedding: Some(vec![1.0, 0.0]),
                    source_id: Some("source-embedding".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                }],
                deletes: Vec::new(),
                max_operations: None,
                source_graph_commit_epoch: Some(23),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    let projection = NowledgeMemSearchProjection::open(&root).unwrap();
    let request = NowledgeMemSearchCandidateRequest::text("embedding identity source chunk", 10)
        .with_metadata_filters(BTreeMap::from([(
            "kind__in".to_string(),
            r#"["source_chunk"]"#.to_string(),
        )]));
    let options = NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read()
        .with_text_retriever(true)
        .with_source_chunk_identity(true)
        .with_embedding_identity("bge-m3", 3);

    let readiness = projection
        .search_candidate_readiness(&request, &options)
        .unwrap();

    assert!(!readiness.ready);
    assert!(readiness.projection_watermark_ready);
    assert!(!readiness.embedding_identity_ready);
    assert_eq!(
        readiness.candidate_report.projection_embedding_dimension,
        Some(2)
    );
    assert!(readiness
        .blocker_codes
        .iter()
        .any(|code| code == "search_candidate_embedding_identity_not_ready"));
    assert!(!readiness
        .json()
        .to_string()
        .contains("candidate embedding body"));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn search_projection_candidate_api_reports_fail_soft_vector_fallback() {
    let root = unique_nowledge_mem_test_dir("search_candidate_api_fail_soft");
    {
        let mut index = SearchIndex::open(&root).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 2,
            })
            .unwrap();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "mem-fallback".to_string(),
                title: "Fallback candidate".to_string(),
                body: "hybrid fallback candidate read".to_string(),
                embedding: Some(vec![1.0, 0.0]),
                source_id: Some("source-fallback".to_string()),
                metadata: BTreeMap::from([
                    ("space_id".to_string(), "default".to_string()),
                    ("lifecycle_state".to_string(), "active".to_string()),
                ]),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    let projection = NowledgeMemSearchProjection::open(&root).unwrap();
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let request =
        NowledgeMemSearchCandidateRequest::hybrid("hybrid fallback", vec![1.0, 0.0, 0.0], 10);

    let output = store.search_candidates(&request).unwrap();

    assert_eq!(output.result.total_hits, 1);
    assert_eq!(output.result.hits[0].id, "memory:mem-fallback");
    assert_eq!(output.report.mode, SearchMode::Hybrid);
    assert_eq!(output.report.query_embedding_dimension, Some(3));
    assert_eq!(
        output.report.retriever_available.get("vector"),
        Some(&false)
    );
    assert_eq!(output.report.retriever_available.get("text"), Some(&true));
    assert_eq!(
        output.report.retriever_candidate_counts.get("text"),
        Some(&1)
    );
    assert!(output
        .report
        .fallback_reason_codes
        .iter()
        .any(|code| code == "vector_dimension_mismatch"));
    assert!(output.report.empty_reason_codes.is_empty());
    assert_eq!(
        output.report.json()["fallback_reason_codes"],
        serde_json::json!(["vector_dimension_mismatch"])
    );
    assert!(!output
        .report
        .json()
        .to_string()
        .contains("hybrid fallback candidate read"));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn search_projection_candidate_api_gates_compressed_vector_preference() {
    let mut index = SearchIndex::in_memory();
    index
        .apply_embedding_manifest(SearchEmbeddingManifest {
            model: "bge-m3".to_string(),
            version: None,
            dimension: 2,
        })
        .unwrap();
    index
        .upsert_projection_row(SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "mem-advisor".to_string(),
            title: "Advisor gated candidate".to_string(),
            body: "advisor gated compressed candidate".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            source_id: Some("source-advisor".to_string()),
            metadata: BTreeMap::new(),
        })
        .unwrap();
    let projection = NowledgeMemSearchProjection::from_index(index);
    let request = NowledgeMemSearchCandidateRequest::vector(vec![1.0, 0.0], 10)
        .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred);

    let output = projection.search_candidates_with_report(&request).unwrap();

    assert_eq!(
        output.report.compressed_vector_search_mode,
        CompressedVectorSearchMode::Disabled
    );
    assert_eq!(
        output.report.requested_compressed_vector_search_mode,
        CompressedVectorSearchMode::Preferred
    );
    assert_eq!(
        output
            .report
            .retriever_backend_selection_reasons
            .get("vector")
            .map(String::as_str),
        Some("compression_disabled")
    );
    assert_eq!(
        output.report.retriever_backends.get("vector"),
        Some(&"scalar_vector_scan".to_string())
    );
    assert_eq!(
        output.report.retrieval_projection_advisor_blocker_codes,
        vec![
            "retrieval_projection_recall_evidence_missing".to_string(),
            "retrieval_projection_parity_evidence_missing".to_string(),
            "retrieval_projection_segment_not_advised".to_string()
        ]
    );
    assert_eq!(
        output.report.json()["retrieval_projection_advisor"]["ready"],
        false
    );
    assert!(!output
        .report
        .json()
        .to_string()
        .contains("advisor gated compressed candidate"));
}

#[test]
fn search_projection_candidate_api_reports_repair_markers_without_hits() {
    let root = unique_nowledge_mem_test_dir("search_candidate_api_repair_markers");
    {
        let mut index = SearchIndex::open(&root).unwrap();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "mem-marker".to_string(),
                title: "Marker candidate".to_string(),
                body: "repair marker candidate read".to_string(),
                embedding: None,
                source_id: Some("source-marker".to_string()),
                metadata: BTreeMap::from([
                    ("space_id".to_string(), "default".to_string()),
                    ("lifecycle_state".to_string(), "active".to_string()),
                ]),
            })
            .unwrap();
        index.mark_full_reindex_needed("stale projection").unwrap();
        index
            .mark_metadata_repair_needed("missing metadata")
            .unwrap();
        index.checkpoint().unwrap();
    }
    let projection = NowledgeMemSearchProjection::open(&root).unwrap();
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
    let request = NowledgeMemSearchCandidateRequest::text("not present", 10);

    let output = store.search_candidates(&request).unwrap();

    assert_eq!(output.result.total_hits, 0);
    assert!(output.report.projection_full_reindex_needed);
    assert!(output.report.projection_metadata_repair_needed);
    assert!(output
        .report
        .empty_reason_codes
        .iter()
        .any(|code| code == "retriever_no_hits"));
    assert_eq!(output.report.json()["projection_full_reindex_needed"], true);
    assert_eq!(
        output.report.json()["projection_metadata_repair_needed"],
        true
    );
    assert!(!output
        .report
        .json()
        .to_string()
        .contains("stale projection"));
    assert!(!output
        .report
        .json()
        .to_string()
        .contains("missing metadata"));
    assert!(!output
        .report
        .json()
        .to_string()
        .contains("repair marker candidate read"));

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_open_options_can_prefer_compressed_vector_search() {
    let root = unique_nowledge_mem_test_dir("compressed_vector_open_options");
    let graph_path = root.join("graph");
    let search_path = root.join("search");
    {
        let mut db = Database::open(&graph_path).unwrap();
        db.query(
                "CREATE (:Memory {id: 'mem-vector', title: 'Vector facade', content: 'Compressed vector retrieval'})",
            )
            .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let mut index = SearchIndex::open(&search_path).unwrap();
        #[cfg(feature = "vector-search")]
        index.set_rabitq_projection_build_options(Default::default());
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 8,
            })
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: vec![
                    SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "mem-vector".to_string(),
                        title: "Vector facade".to_string(),
                        body: "Compressed vector retrieval".to_string(),
                        embedding: Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                        source_id: None,
                        metadata: BTreeMap::new(),
                    },
                    SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "mem-vector-neighbor".to_string(),
                        title: "Vector neighbor".to_string(),
                        body: "Recall validation neighbor".to_string(),
                        embedding: Some(vec![0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                        source_id: None,
                        metadata: BTreeMap::new(),
                    },
                ],
                deletes: Vec::new(),
                max_operations: None,
                source_graph_commit_epoch: Some(1),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    let recall_report = SearchIndex::open(&search_path)
        .unwrap()
        .validate_sampled_compressed_vector_recall(VectorRecallValidationOptions {
            max_samples: 2,
            top_k: 1,
            candidate_limit: 1,
            minimum_recall_per_million: 1_000_000,
            metadata_filters: BTreeMap::new(),
        });
    assert!(recall_report.ready, "{:?}", recall_report.blocker_codes);
    let options = NowledgeMemOpenOptions::with_search_projection(
        graph_path,
        search_path,
        NowledgeMemGraphMode::ShadowReadOnly,
    )
    .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred)
    .with_adaptive_vector_backend_policy(AdaptiveVectorBackendPolicy {
        flat_scan_max_documents: 0,
        high_filter_selectivity_per_million: u32::MAX,
        flat_scan_memory_budget_bytes: 0,
    })
    .with_retrieval_projection_advisor(
        NowledgeMemRetrievalProjectionAdvisor::cold_local_with_recall_parity(&recall_report),
    );

    let (store, report) = NowledgeMemEmbeddedStore::open_with_options(options).unwrap();
    let retrieval = store
        .retrieve_knowledge_with_report(&KnowledgeRetrievalRequest {
            query_text: String::new(),
            query_embedding: Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            mode: SearchMode::Vector,
            limit: 10,
            offset: 0,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit: 0,
            graph_context_max_hops: 0,
        })
        .unwrap();
    let output = retrieval.output;

    assert_eq!(
        report.compressed_vector_search_mode,
        CompressedVectorSearchMode::Preferred
    );
    assert_eq!(
        report.requested_compressed_vector_search_mode,
        CompressedVectorSearchMode::Preferred
    );
    assert!(report.retrieval_projection_advisor.ready());
    assert_eq!(output.search.hits[0].id, "memory:mem-vector");
    assert_eq!(
        output.search.retrievers[0].backend,
        "hawdb_rabitq_candidate_projection"
    );
    assert_eq!(
        retrieval.report.compressed_vector_search_mode,
        CompressedVectorSearchMode::Preferred
    );
    assert_eq!(
        retrieval.report.vector_backend,
        Some("hawdb_rabitq_candidate_projection".to_string())
    );
    assert_eq!(
        retrieval.report.json()["vector_backend"],
        "hawdb_rabitq_candidate_projection"
    );

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn embedded_store_retrieval_requires_search_projection() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let error = store
        .retrieve_knowledge(&KnowledgeRetrievalRequest {
            query_text: "missing projection".to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 10,
            offset: 0,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 4,
            graph_context_limit: 4,
            graph_context_max_hops: 1,
        })
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "storage error: nowledge mem search projection is not configured"
    );
}

#[test]
fn embedded_store_reports_background_maintenance_summary() {
    let db = Database::new();
    let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    graph
        .query("CREATE (:Memory {id: 'mem-maintenance', title: 'Maintenance summary'})")
        .unwrap();
    let projection = NowledgeMemSearchProjection::from_index(SearchIndex::in_memory());
    let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

    let summary = store
        .background_maintenance_summary(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            BackgroundMaintenanceOptions {
                include_schema_maintenance: false,
                include_property_index_projection: false,
                include_search_projection_rebuild: false,
                include_search_projection_metadata_repair: false,
                include_hawdb_lightning_bootstrap_export: false,
                include_external_content_artifact_jobs: false,
                ..BackgroundMaintenanceOptions::default()
            },
        )
        .unwrap();

    assert_eq!(summary.total_candidates, 1);
    assert_eq!(summary.admitted_count, 1);
    assert_eq!(
        summary.top_admitted_kind,
        Some(BackgroundMaintenanceKind::SearchProjectionGraphDelta)
    );
    assert_eq!(summary.executable_search_projection_graph_delta_count, 1);
    assert_eq!(summary.admitted_search_projection_graph_delta_count, 1);
    let item = &summary.ranked[0];
    assert_eq!(
        summary.max_search_projection_graph_delta_complete_through_graph_commit_epoch,
        item.search_projection_graph_delta_complete_through_graph_commit_epoch
    );
    assert!(summary
        .max_search_projection_graph_delta_complete_through_graph_commit_epoch
        .is_some());
    assert_eq!(item.name, "search_projection_graph_delta");
    assert_eq!(item.admission_name, "admit");
    assert_eq!(
        item.search_projection_graph_delta_upsert_node_count,
        Some(1)
    );
    assert_eq!(
        item.search_projection_graph_delta_delete_document_count,
        Some(0)
    );

    let report = store
        .background_maintenance_report(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            BackgroundMaintenanceOptions {
                include_schema_maintenance: false,
                include_property_index_projection: false,
                include_search_projection_rebuild: false,
                include_search_projection_metadata_repair: false,
                include_hawdb_lightning_bootstrap_export: false,
                include_external_content_artifact_jobs: false,
                ..BackgroundMaintenanceOptions::default()
            },
        )
        .unwrap();
    let json = report.json();

    assert_eq!(report.protocol, "hawdb-background-maintenance-report");
    assert!(report.present);
    assert!(report.ready);
    assert_eq!(report.total_candidates, 1);
    assert_eq!(report.ranked_count, 1);
    assert_eq!(report.foreground_ranked_count, 0);
    assert_eq!(report.unknown_admission_count, 0);
    assert_eq!(report.executable_search_projection_graph_delta_count, 1);
    assert_eq!(report.admitted_search_projection_graph_delta_count, 1);
    assert_eq!(report.slow_query_ready, Some(true));
    assert_eq!(report.slow_query_record_count, Some(0));
    assert_eq!(report.slow_query_capacity, Some(256));
    assert_eq!(report.slow_query_redaction_ready, Some(true));
    assert_eq!(report.memory_pressure_ready, Some(true));
    assert!(report.memory_budget_bytes.is_some());
    assert!(report.estimated_memory_bytes.is_some());
    assert!(report.blocker_codes.is_empty());
    assert_eq!(json["protocol"], "hawdb-background-maintenance-report");
    assert_eq!(json["memory_pressure"]["ready"], true);
    assert!(json["memory_pressure"]["budget_bytes"].as_u64().is_some());
    assert!(json["memory_pressure"]["estimated_bytes"]
        .as_u64()
        .is_some());
    assert_eq!(json["slow_query"]["ready"], true);
    assert_eq!(json["slow_query"]["record_count"], 0);
    assert_eq!(json["slow_query"]["capacity"], 256);
    assert_eq!(json["ranked"].as_array().unwrap().len(), 1);
}

#[test]
fn embedded_store_background_maintenance_report_fails_closed_without_work() {
    let db = Database::new();
    let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
    let store = NowledgeMemEmbeddedStore::new(graph, None);

    let report = store
        .background_maintenance_report(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            BackgroundMaintenanceOptions {
                include_schema_maintenance: false,
                include_property_index_projection: false,
                include_search_projection_graph_delta_freshness: false,
                include_search_projection_rebuild: false,
                include_search_projection_metadata_repair: false,
                include_hawdb_lightning_bootstrap_export: false,
                include_external_content_artifact_jobs: false,
                ..BackgroundMaintenanceOptions::default()
            },
        )
        .unwrap();

    assert!(!report.ready);
    assert_eq!(report.total_candidates, 0);
    assert_eq!(report.ranked_count, 0);
    assert_eq!(
        report.blocker_codes,
        vec!["no_candidates".to_string(), "no_ranked_work".to_string()]
    );
}

fn nowledge_projection_evidence_rows() -> Vec<SearchProjectionRow> {
    let mut rows = vec![
        nowledge_projection_evidence_row(SearchProjectionKind::Memory, "mem_1", true),
        nowledge_projection_evidence_row(SearchProjectionKind::Message, "msg_1", false),
        nowledge_projection_evidence_row(SearchProjectionKind::Community, "community_1", true),
        nowledge_projection_evidence_row(SearchProjectionKind::Entity, "entity_1", true),
        nowledge_projection_evidence_row(SearchProjectionKind::Source, "source_1", true),
        nowledge_projection_evidence_row(SearchProjectionKind::SourceChunk, "chunk_1", true),
    ];
    rows.extend((0..124).map(|ordinal| {
        nowledge_projection_evidence_row(
            SearchProjectionKind::Memory,
            &format!("mem_filler_{ordinal:03}"),
            true,
        )
    }));
    rows
}

fn upsert_excluded_search_projection_rows(
    index: &mut SearchIndex,
    external_id_prefix: &str,
    count: usize,
    embedding: Option<Vec<f32>>,
) {
    for ordinal in 0..count {
        let external_id = format!("{external_id_prefix}-{ordinal:03}");
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: external_id.clone(),
                title: format!("{external_id} candidate"),
                body: "metadata filtered candidate read".to_string(),
                embedding: embedding.clone(),
                source_id: Some("source-1".to_string()),
                metadata: BTreeMap::from([
                    ("space_id".to_string(), "default".to_string()),
                    ("unit_type".to_string(), "memory".to_string()),
                    ("lifecycle_state".to_string(), "deleted".to_string()),
                    ("importance".to_string(), "0.95".to_string()),
                ]),
            })
            .unwrap();
    }
}

fn incremental_maintenance_fixture(
    name: &str,
    maintenance: super::NowledgeMemIncrementalSearchMaintenanceOptions,
) -> (
    std::path::PathBuf,
    NowledgeMemEmbeddedStoreHandle,
    super::NowledgeMemOpenOptions,
) {
    let root = unique_nowledge_mem_test_dir(name);
    let search_path = root.join("search");
    let mut index = SearchIndex::open(&search_path).unwrap();
    for id in ["a", "z"] {
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: id.into(),
                title: id.into(),
                body: format!("bootstrap {id}"),
                embedding: None,
                source_id: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
    }
    index.checkpoint().unwrap();
    drop(index);
    let options = super::NowledgeMemOpenOptions::with_search_projection(
        root.join("graph"),
        search_path,
        NowledgeMemGraphMode::WritableCutover,
    )
    .with_incremental_search_maintenance(maintenance);
    let (handle, report) =
        NowledgeMemEmbeddedStoreHandle::open_with_options(options.clone()).unwrap();
    assert_eq!(
        report.search_projection_role,
        Some(super::NowledgeMemSearchProjectionRole::IncrementalMaintenance)
    );
    assert!(!report.search_production_qualification_bound);
    (root, handle, options)
}

fn check_catch_up_binding(
    snapshot: &mut crate::DatabaseReadTransaction,
    expected: &str,
) -> crate::Result<()> {
    let rows = snapshot
        .query(
            "MATCH (binding:CatchUpBinding {id: 'target'}) \
                 RETURN binding.incarnation AS incarnation LIMIT 2",
        )?
        .rows;
    if rows.len() == 1 && rows[0].get("incarnation") == Some(&Value::String(expected.to_string())) {
        Ok(())
    } else {
        Err(HawDBError::Execution("target binding changed".into()))
    }
}

#[test]
fn guarded_incremental_catch_up_rejects_stale_binding_and_reopens_after_retry() {
    let (root, handle, options) =
        incremental_maintenance_fixture("guarded_incremental_catch_up", Default::default());
    handle
        .with_transaction(|transaction| {
            transaction.query("CREATE (:CatchUpBinding {id: 'target', incarnation: 'old'})")?;
            transaction.query("CREATE (:Memory {id: 'm', title: 'Guarded'})")?;
            Ok(())
        })
        .unwrap();
    handle
        .query_with_report(
            "MATCH (binding:CatchUpBinding {id: 'target'}) \
                 SET binding.incarnation = 'current'",
        )
        .unwrap();
    let before = handle
        .runtime_status()
        .unwrap()
        .projection_freshness
        .unwrap();
    let manifest = root.join("search/search_projection.out_of_core.manifest.hawdb");
    let before_manifest = std::fs::read(&manifest).unwrap();
    let mut hydrated = false;
    let error = handle
        .catch_up_search_projection_with_guard_and_batch_hydrator(
            16,
            16,
            4,
            |snapshot| check_catch_up_binding(snapshot, "old"),
            |_, _| {
                hydrated = true;
                Ok(SearchProjectionRelationalDelta::default())
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("target binding changed"));
    assert!(!hydrated);
    assert_eq!(std::fs::read(&manifest).unwrap(), before_manifest);
    assert!(handle
        .search_projection_documents(&["memory:m".into()], 1)
        .is_err());
    let after = handle
        .runtime_status()
        .unwrap()
        .projection_freshness
        .unwrap();
    assert_eq!(
        after.source_graph_commit_epoch,
        before.source_graph_commit_epoch
    );
    assert_eq!(
        after.durable_source_graph_commit_epoch,
        before.durable_source_graph_commit_epoch
    );

    let report = handle
        .catch_up_search_projection_with_guard_and_batch_hydrator(
            16,
            16,
            4,
            |snapshot| check_catch_up_binding(snapshot, "current"),
            |_, _| Ok(SearchProjectionRelationalDelta::default()),
        )
        .unwrap();
    assert!(report.complete);
    assert_eq!(report.end_applied_epoch, report.end_durable_epoch);
    drop(handle);

    let (reopened, _) = NowledgeMemEmbeddedStoreHandle::open_with_options(options).unwrap();
    assert_eq!(
        reopened
            .search_projection_documents(&["memory:m".into()], 1)
            .unwrap()[0]
            .title,
        "Guarded"
    );
    let retry = reopened
        .catch_up_search_projection_with_guard_and_batch_hydrator(
            16,
            16,
            4,
            |snapshot| check_catch_up_binding(snapshot, "current"),
            |_, _| Ok(SearchProjectionRelationalDelta::default()),
        )
        .unwrap();
    assert!(retry.complete);
    assert_eq!(retry.end_durable_epoch, report.end_durable_epoch);
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn guarded_incremental_catch_up_holds_write_owner_until_publication() {
    let (root, handle, _options) =
        incremental_maintenance_fixture("guarded_incremental_lock", Default::default());
    handle
        .with_transaction(|transaction| {
            transaction.query("CREATE (:CatchUpBinding {id: 'target', incarnation: 'current'})")?;
            transaction.query("CREATE (:Memory {id: 'm', title: 'Guarded'})")?;
            Ok(())
        })
        .unwrap();
    let (guard_entered_tx, guard_entered_rx) = mpsc::channel();
    let (mutation_started_tx, mutation_started_rx) = mpsc::channel();
    let writer_handle = handle.clone();
    let writer = thread::spawn(move || {
        guard_entered_rx.recv().unwrap();
        mutation_started_tx.send(()).unwrap();
        writer_handle
            .query_with_report(
                "MATCH (binding:CatchUpBinding {id: 'target'}) \
                     SET binding.incarnation = 'next'",
            )
            .unwrap();
    });

    let report = handle
        .catch_up_search_projection_with_guard_and_batch_hydrator(
            16,
            16,
            4,
            |snapshot| {
                check_catch_up_binding(snapshot, "current")?;
                guard_entered_tx.send(()).unwrap();
                mutation_started_rx.recv().unwrap();
                Ok(())
            },
            |snapshot, _| {
                check_catch_up_binding(snapshot, "current")?;
                assert!(handle.inner.try_write().is_err());
                Ok(SearchProjectionRelationalDelta::default())
            },
        )
        .unwrap();
    assert!(report.complete);
    writer.join().unwrap();
    handle
        .with_read_transaction(4096, |snapshot| check_catch_up_binding(snapshot, "next"))
        .unwrap();
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn incremental_maintenance_preserves_bootstrap_artifacts_and_reopens_deltas() {
    let (root, handle, options) =
        incremental_maintenance_fixture("incremental_maintenance_reopen", Default::default());
    let search_path = root.join("search");
    let pinned = crate::SearchOutOfCoreReader::open(&search_path).unwrap();
    let old_generation = pinned.generation();
    handle
        .query_with_report("CREATE (:Memory {id: 'm', title: 'Inserted'})")
        .unwrap();
    let catch_up = handle
        .catch_up_search_projection_with_batch_hydrator(4, 4, 1, |_, _| {
            Ok(SearchProjectionRelationalDelta::default())
        })
        .unwrap();
    assert!(catch_up.complete);
    assert_eq!(catch_up.end_applied_epoch, catch_up.end_durable_epoch);
    let update = handle
        .apply_search_projection_delta_and_checkpoint(SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "m".into(),
                title: "Inserted".into(),
                body: "durable external embedding".into(),
                embedding: Some(vec![0.25, 0.75]),
                source_id: None,
                metadata: BTreeMap::new(),
            }],
            max_operations: Some(1),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(update.action, "incremental_mutation_publish");
    let resident_error = SearchIndex::open(&search_path).unwrap_err();
    assert!(resident_error
        .to_string()
        .contains("resident search snapshot"));
    assert_eq!(pinned.generation(), old_generation);
    assert_eq!(pinned.document_count(), 2);
    assert!(pinned.hydrate_documents(&["memory:m".into()]).is_err());
    let snapshot = handle.runtime_governor_snapshot().unwrap();
    assert_eq!(snapshot.admissions, snapshot.completions);
    drop(handle);
    let (reopened, _) = NowledgeMemEmbeddedStoreHandle::open_with_options(options).unwrap();
    let rows = reopened
        .search_projection_documents(&["memory:m".into()], 1)
        .unwrap();
    assert_eq!(rows[0].content, "durable external embedding");
    assert_eq!(rows[0].embedding, Some(vec![0.25, 0.75]));
    assert_eq!(
        reopened
            .runtime_status()
            .unwrap()
            .projection_freshness
            .unwrap()
            .durable_source_graph_commit_epoch,
        catch_up.end_durable_epoch
    );
    drop(reopened);
    drop(pinned);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "background-maintenance")]
fn incremental_maintenance_compaction_honors_qos_and_cancellation_before_retry() {
    use crate::search::SearchOutOfCoreSegmentCompactionStopReason;

    let (root, handle, options) =
        incremental_maintenance_fixture("incremental_maintenance_compaction", Default::default());
    let search_path = root.join("search");
    handle
        .query_with_report("CREATE (:Memory {id: 'm', title: 'Inserted'})")
        .unwrap();
    let catch_up = handle
        .catch_up_search_projection_with_batch_hydrator(4, 4, 1, |_, _| {
            Ok(SearchProjectionRelationalDelta::default())
        })
        .unwrap();
    let pinned = crate::SearchOutOfCoreReader::open(&search_path).unwrap();
    let manifest = search_path.join("search_projection.out_of_core.manifest.hawdb");
    let before = std::fs::read(&manifest).unwrap();
    let deferred = handle
        .compact_search_projection_segments_with_context(
            Default::default(),
            hawdb_qos::BackgroundWorkHint {
                tenant_budget_remaining_operations: Some(1),
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap();
    assert!(matches!(
        deferred.stop_reason(),
        SearchOutOfCoreSegmentCompactionStopReason::Deferred(_)
    ));
    assert_eq!(std::fs::read(&manifest).unwrap(), before);
    let cancellation = crate::RuntimeCancellationToken::new();
    cancellation.cancel();
    let error = handle
        .compact_search_projection_segments_with_context(
            Default::default(),
            Default::default(),
            crate::RuntimeTaskContext::without_deadline(cancellation),
        )
        .unwrap_err();
    assert!(error.to_string().contains("cancel"), "{error}");
    assert_eq!(std::fs::read(&manifest).unwrap(), before);
    let completed = handle
        .compact_search_projection_segments_with_context(
            Default::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
    assert_eq!(
        completed.stop_reason(),
        SearchOutOfCoreSegmentCompactionStopReason::Completed
    );
    assert_eq!(completed.compaction().unwrap().source_segment_count(), 2);
    let documents = handle
        .search_projection_documents(
            &["memory:a".into(), "memory:m".into(), "memory:z".into()],
            3,
        )
        .unwrap();
    assert_eq!(
        pinned
            .hydrate_documents(&["memory:m".into()])
            .unwrap()
            .documents[0]
            .title,
        "Inserted"
    );
    assert_eq!(pinned.document_count(), 3);
    assert_eq!(
        handle
            .runtime_status()
            .unwrap()
            .projection_freshness
            .unwrap()
            .durable_source_graph_commit_epoch,
        catch_up.end_durable_epoch
    );
    let governor = handle.runtime_governor_snapshot().unwrap();
    assert_eq!(governor.admissions, governor.completions);
    drop(handle);
    let (reopened, _) = NowledgeMemEmbeddedStoreHandle::open_with_options(options).unwrap();
    assert_eq!(
        reopened
            .search_projection_documents(
                &["memory:a".into(), "memory:m".into(), "memory:z".into()],
                3
            )
            .unwrap(),
        documents
    );
    drop(reopened);
    drop(pinned);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn incremental_refresh_retries_after_descriptor_pressure_without_reopen() {
    let root = unique_nowledge_mem_test_dir("incremental_refresh_fd_retry");
    let search_path = root.join("search");
    let files =
        hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire_with_capacity_limit(
            &search_path,
            64,
            true,
        )
        .unwrap();
    let mut index = SearchIndex::open(&search_path).unwrap();
    index
        .upsert_projection_row(SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "a".into(),
            title: "original".into(),
            body: "original".into(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    index.checkpoint().unwrap();
    drop(index);
    let options = super::NowledgeMemOpenOptions::with_search_projection(
        root.join("graph"),
        search_path,
        NowledgeMemGraphMode::WritableCutover,
    )
    .with_incremental_search_maintenance(Default::default());
    let (handle, _) = NowledgeMemEmbeddedStoreHandle::open_with_options(options).unwrap();
    let pinned = handle
        .lock_published_read()
        .unwrap()
        .as_ref()
        .unwrap()
        .out_of_core_projection
        .as_ref()
        .unwrap()
        .read_snapshot();
    let (held_tx, held_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let pressured_files = files.clone();
    let pressure = thread::spawn(move || {
        // Reservations are borrowed by nested IO on the reserving thread.
        // Hold pressure elsewhere so refresh cannot consume these slots.
        let metrics = pressured_files.metrics();
        let _reservation = pressured_files
            .reserve(metrics.limit - metrics.open - metrics.reserved)
            .unwrap();
        held_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    held_rx.recv().unwrap();
    let result = handle.catch_up_search_projection_with_batch_hydrator(4, 4, 1, |_, _| {
        Ok(SearchProjectionRelationalDelta::default())
    });
    release_tx.send(()).unwrap();
    pressure.join().unwrap();
    let error = result.unwrap_err();
    assert!(
        matches!(
            error,
            HawDBError::FileDescriptors(crate::FileDescriptorError::BudgetExceeded { .. })
        ),
        "{error}"
    );
    assert_eq!(
        pinned
            .hydrate_documents(&["memory:a".into()])
            .unwrap()
            .documents[0]
            .title,
        "original"
    );
    drop(pinned);
    handle
        .query_with_report("CREATE (:Memory {id: 'm', title: 'Recovered'})")
        .unwrap();
    handle
        .catch_up_search_projection_with_batch_hydrator(4, 4, 1, |_, _| {
            Ok(SearchProjectionRelationalDelta::default())
        })
        .unwrap();
    let output = handle
        .search_projection_documents(&["memory:m".into()], 1)
        .unwrap();
    assert_eq!(output[0].title, "Recovered");
    drop(handle);
    drop(files);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn incremental_maintenance_budget_failure_does_not_advance_the_watermark() {
    let maintenance = super::NowledgeMemIncrementalSearchMaintenanceOptions {
        working_memory_bytes: std::num::NonZeroU64::new(1).unwrap(),
        ..Default::default()
    };
    let (root, handle, options) =
        incremental_maintenance_fixture("incremental_maintenance_budget", maintenance);
    let manifest = root.join("search/search_projection.out_of_core.manifest.hawdb");
    let before = std::fs::read(&manifest).unwrap();
    handle
        .query_with_report("CREATE (:Memory {id: 'm', title: 'Rejected'})")
        .unwrap();
    let error = handle
        .catch_up_search_projection_with_batch_hydrator(4, 4, 1, |_, _| {
            Ok(SearchProjectionRelationalDelta::default())
        })
        .unwrap_err();
    assert!(error.to_string().contains("memory"), "{error}");
    assert_eq!(std::fs::read(&manifest).unwrap(), before);
    let freshness = handle
        .runtime_status()
        .unwrap()
        .projection_freshness
        .unwrap();
    assert_eq!(freshness.source_graph_commit_epoch, None);
    assert_eq!(freshness.durable_source_graph_commit_epoch, None);
    drop(handle);
    let (reopened, _) = NowledgeMemEmbeddedStoreHandle::open_with_options(options).unwrap();
    assert!(reopened
        .search_projection_documents(&["memory:m".into()], 1)
        .is_err());
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn incremental_maintenance_rejects_missing_bootstrap_without_recreating_it() {
    let root = unique_nowledge_mem_test_dir("incremental_missing_bootstrap");
    let options = super::NowledgeMemOpenOptions::with_search_projection(
        root.join("graph"),
        root.join("search"),
        NowledgeMemGraphMode::WritableCutover,
    )
    .with_incremental_search_maintenance(Default::default());
    assert!(NowledgeMemEmbeddedStoreHandle::open_with_options(options).is_err());
    assert!(!root
        .join("search/search_projection.out_of_core.manifest.hawdb")
        .exists());
    std::fs::remove_dir_all(root).unwrap();
}

fn persisted_nowledge_projection_evidence_index(name: &str, _compressed: bool) -> SearchIndex {
    let path = unique_nowledge_mem_test_dir(name);
    let mut index = SearchIndex::open(&path).unwrap();
    #[cfg(feature = "vector-search")]
    if _compressed {
        index.set_rabitq_projection_build_options(Default::default());
    }
    index
        .apply_embedding_manifest(SearchEmbeddingManifest {
            model: "bge-m3".to_string(),
            version: None,
            dimension: 8,
        })
        .unwrap();
    index
        .apply_projection_delta(SearchProjectionDelta {
            upserts: nowledge_projection_evidence_rows(),
            deletes: Vec::new(),
            max_operations: None,
            source_graph_commit_epoch: Some(17),
        })
        .unwrap();
    index.checkpoint().unwrap();
    SearchIndex::open(path).unwrap()
}

fn nowledge_projection_evidence_row(
    kind: SearchProjectionKind,
    external_id: &str,
    include_embedding: bool,
) -> SearchProjectionRow {
    SearchProjectionRow {
        kind,
        external_id: external_id.to_string(),
        title: format!("{external_id} title"),
        body: format!("{external_id} body"),
        embedding: include_embedding.then_some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
        source_id: Some("source_1".to_string()),
        metadata: BTreeMap::from([
            ("space_id".to_string(), "default".to_string()),
            ("unit_type".to_string(), "fact".to_string()),
            ("lifecycle_state".to_string(), "active".to_string()),
            ("importance".to_string(), "0.8".to_string()),
            ("confidence".to_string(), "0.9".to_string()),
            ("created_at".to_string(), "11".to_string()),
            ("updated_at".to_string(), "12".to_string()),
            ("event_start".to_string(), "10".to_string()),
            ("event_end".to_string(), "20".to_string()),
            ("is_latest".to_string(), "true".to_string()),
        ]),
    }
}

fn full_bounded_read_routes() -> Vec<String> {
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .map(|route| (*route).to_string())
        .collect()
}

fn ready_route_readiness_summary() -> NowledgeMemRouteReadinessSummary {
    NowledgeMemRouteReadinessSummary {
        route_primary_ready: true,
        primary_ready_routes: full_bounded_read_routes(),
        route_query_plan_evidence_ready: true,
        route_query_profile_evidence_ready: true,
        route_query_api_behavior_evidence_ready: true,
        relationship_property_pruning_required_count: 0,
        relationship_property_pruning_report_count: 0,
        route_relationship_property_pruning_evidence_ready: true,
    }
}

fn ready_search_route_ownership() -> NowledgeMemSearchRouteOwnershipReadinessReport {
    nowledge_mem_search_route_ownership_readiness(
        &nowledge_mem_search_route_ownership_all_hawdb(),
        NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
    )
}

fn ready_active_search_route_ownership() -> NowledgeMemActiveSearchRouteOwnershipReadinessReport {
    nowledge_mem_active_search_route_ownership_readiness(
        &nowledge_mem_active_search_route_ownership_all_hawdb(),
        NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
    )
}

fn ready_active_search_route_readiness() -> NowledgeMemActiveSearchRouteReadinessReport {
    nowledge_mem_active_search_route_readiness(
        &nowledge_mem_active_search_route_read_evidence_all_hawdb_ready(),
        NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
    )
}

fn ready_query_family_replacement() -> serde_json::Value {
    serde_json::json!(REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES
        .iter()
        .map(|family| serde_json::json!({
            "query_family": family,
            "required_checks": 1,
            "covered_checks": 1,
            "shadow_matched_checks": 1,
            "replacement_readiness_per_million": 1_000_000,
        }))
        .collect::<Vec<_>>())
}

fn readiness_dashboard_area<'a>(
    dashboard: &'a NowledgeMemReadinessDashboard,
    name: &str,
) -> &'a NowledgeMemReadinessAreaSummary {
    dashboard
        .areas
        .iter()
        .find(|area| area.name == name)
        .expect("readiness dashboard area")
}

fn unique_nowledge_mem_test_dir(name: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "hawdb_nowledge_mem_{name}_{}_{}",
        std::process::id(),
        nanos
    ))
}

#[test]
fn complete_text_native_projection_queries_preserve_scores_and_candidate_coverage() {
    fn populate(index: &mut SearchIndex) {
        for (id, title, space) in [("a", "graph graph", "team"), ("b", "graph", "other")] {
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: id.into(),
                    title: title.into(),
                    body: "storage graph".into(),
                    embedding: None,
                    source_id: None,
                    metadata: BTreeMap::from([("space_id".into(), space.into())]),
                })
                .unwrap();
        }
    }
    let path = unique_nowledge_mem_test_dir("complete-text-native");
    let mut raw = SearchIndex::in_memory();
    populate(&mut raw);
    let mut persisted = SearchIndex::open(&path).unwrap();
    populate(&mut persisted);
    persisted.checkpoint().unwrap();
    drop(persisted);
    let raw = NowledgeMemSearchProjection::from_index(raw);
    let resident = NowledgeMemSearchProjection::open(&path).unwrap();
    let disk = NowledgeMemOutOfCoreSearchProjection::open(&path).unwrap();
    let mut database = Database::new();
    database
        .query("CREATE (:Memory {id: 'a', space_id: 'team'})")
        .unwrap();
    database
        .query("CREATE (:Memory {id: 'b', space_id: 'other'})")
        .unwrap();
    let query = "CALL text_search($text, complete := true, maxCandidates := $cap) YIELD id AS document_id, score MATCH (seed:Memory) WHERE seed.space_id = 'team' RETURN seed.id AS id, score LIMIT 1";
    let mut successful_scores = Vec::new();
    for (projection, out_of_core_projection) in [
        (Some(&raw), None),
        (Some(&resident), None),
        (None, Some(&disk)),
    ] {
        let mut external = super::SearchProjectionExternalReadOperator {
            projection: projection.map(super::ResidentProjectionRef::Live),
            out_of_core_projection,
            vector_seed_execution_count: 0,
        };
        for cap in [1, 2, 1_000_000] {
            let parameters = BTreeMap::from([
                ("text".into(), Value::String("graph".into())),
                ("cap".into(), Value::Int(cap)),
            ]);
            let mut rows = Vec::new();
            let result = database
                .begin_read_transaction()
                .unwrap()
                .query_request_streaming_with_external(
                    crate::QueryRequest::new(query).with_params(&parameters),
                    &mut external,
                    |row| {
                        rows.push(row);
                        Ok(())
                    },
                );
            if cap == 1 {
                assert!(
                    matches!(result, Err(HawDBError::ResourceBudgetExceeded(cause)) if cause.budget_option == "text_search.maxCandidates")
                );
                assert!(rows.is_empty());
            } else {
                let report = result.unwrap();
                let text = &report.execution_profile.text_seed_execution_reports;
                assert_eq!(text.len(), 1);
                assert_eq!(text[0].status(), "complete");
                assert_eq!(text[0].candidate_count, 2);
                let source = text[0].source.unwrap();
                assert_eq!(source.corpus_documents, Some(2));
                assert_eq!(source.matching_documents, Some(2));
                if out_of_core_projection.is_some() {
                    assert!(source.generation.is_some());
                    assert!(source.postings_visited.unwrap() > 0);
                    assert!(source.bytes_read.unwrap() > 0);
                } else {
                    assert_eq!(source.generation, None);
                    assert_eq!(source.bytes_read, None);
                }

                assert_eq!(
                    report
                        .execution_profile
                        .pipeline_memory_report
                        .query_memory_completion_bytes,
                    0
                );
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0]["id"], Value::String("a".into()));
                let Value::Float(score) = rows[0]["score"] else {
                    panic!("missing BM25 score");
                };
                successful_scores.push(score);
            }
        }
    }
    assert!(successful_scores
        .iter()
        .all(|score| (*score - successful_scores[0]).abs() < 1e-12));
    drop(raw);
    drop(resident);
    drop(disk);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn grouped_evidence_complete_text_preserves_graph_eligibility_and_retained_reads() {
    let mut reference_rows: Option<Vec<crate::Row>> = None;
    for backend in ["raw", "resident", "disk"] {
        let path = unique_nowledge_mem_test_dir(&format!("grouped-complete-{backend}"));
        let mut database = Database::new();
        database
            .query("CREATE (:Scope {id: 'allowed', visible: true})")
            .unwrap();
        database
            .query("CREATE (:Scope {id: 'hidden', visible: false})")
            .unwrap();
        let mut index = if backend == "resident" {
            SearchIndex::open(&path).unwrap()
        } else {
            SearchIndex::in_memory()
        };
        for (id, group, visible, repeats) in [
            ("a1", "A", true, 5),
            ("a2", "A", true, 5),
            ("a3", "A", true, 5),
            ("a4", "A", true, 5),
            ("a5", "A", true, 5),
            ("b1", "B", true, 1),
            ("c1", "C", true, 1),
            ("d1", "D", false, 30),
        ] {
            let parameters = BTreeMap::from([
                ("id".into(), Value::String(id.into())),
                ("g".into(), Value::String(group.into())),
                (
                    "scope".into(),
                    Value::String(if visible { "allowed" } else { "hidden" }.into()),
                ),
            ]);
            database
                .query_with_params("CREATE (:Memory {id: $id, group_id: $g})", &parameters)
                .unwrap();
            database.query_with_params("MATCH (m:Memory {id: $id}), (s:Scope {id: $scope}) CREATE (m)-[:VISIBLE_IN]->(s)", &parameters).unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: id.into(),
                    title: "graph ".repeat(repeats),
                    body: "graph".into(),
                    embedding: None,
                    source_id: None,
                    metadata: BTreeMap::new(),
                })
                .unwrap();
        }
        let (projection, out_of_core_projection) = match backend {
            "raw" => (Some(NowledgeMemSearchProjection::from_index(index)), None),
            "resident" => {
                index.checkpoint().unwrap();
                drop(index);
                (
                    Some(NowledgeMemSearchProjection::open(&path).unwrap()),
                    None,
                )
            }
            "disk" => {
                let mut writer =
                    crate::SearchOutOfCoreGenerationWriter::create(&path, Default::default())
                        .unwrap();
                for id in [
                    "memory:a1",
                    "memory:a2",
                    "memory:a3",
                    "memory:a4",
                    "memory:a5",
                    "memory:b1",
                    "memory:c1",
                    "memory:d1",
                ] {
                    writer.push(index.document(id).unwrap().clone()).unwrap();
                }
                writer
                    .push(
                        SearchProjectionRow {
                            kind: SearchProjectionKind::Memory,
                            external_id: "zombie".into(),
                            title: "graph".into(),
                            body: "graph".into(),
                            embedding: None,
                            source_id: None,
                            metadata: BTreeMap::new(),
                        }
                        .into_document(),
                    )
                    .unwrap();
                writer.finish().unwrap();
                let mut pinned = crate::SearchOutOfCoreReader::open(&path).unwrap();
                crate::SearchOutOfCoreGenerationWriter::prepare_delta(
                    &pinned,
                    SearchProjectionDelta {
                        upserts: vec![SearchProjectionRow {
                            kind: SearchProjectionKind::Memory,
                            external_id: "a1".into(),
                            title: "graph ".repeat(5),
                            body: "graph".into(),
                            embedding: None,
                            source_id: None,
                            metadata: BTreeMap::new(),
                        }],
                        deletes: vec!["memory:zombie".into()],
                        ..Default::default()
                    },
                    Default::default(),
                )
                .unwrap()
                .finish()
                .unwrap();
                let refresh = pinned.refresh().unwrap();
                assert!(refresh.generation_changed);
                assert_eq!(refresh.opened_content_segments, 1);
                assert_eq!(refresh.reused_content_segments, 1);
                assert!(refresh.validated_retractions >= 2);
                drop(pinned);
                (
                    None,
                    Some(NowledgeMemOutOfCoreSearchProjection::open(&path).unwrap()),
                )
            }
            _ => unreachable!(),
        };
        let mut external = super::SearchProjectionExternalReadOperator {
            projection: projection.as_ref().map(super::ResidentProjectionRef::Live),
            out_of_core_projection: out_of_core_projection.as_ref(),
            vector_seed_execution_count: 0,
        };
        let query = "CALL text_search($text, complete := true, maxCandidates := $cap) YIELD id AS document_id, score MATCH (seed:Memory) MATCH (seed)-[:VISIBLE_IN]->(scope:Scope) WHERE scope.visible = true WITH seed.group_id AS g, MAX(score) AS best, collect_top(document_id, score, $m) AS evidence RETURN g, best, evidence ORDER BY best DESC, g ASC LIMIT $k";
        let parameters = BTreeMap::from([
            ("text".into(), Value::String("graph".into())),
            ("cap".into(), Value::Int(8)),
            ("m".into(), Value::Int(2)),
            ("k".into(), Value::Int(3)),
        ]);
        let mut old = database.begin_read_transaction().unwrap();
        let mut rows = Vec::new();
        let report = old
            .query_request_streaming_with_external(
                crate::QueryRequest::new(query).with_params(&parameters),
                &mut external,
                |row| {
                    rows.push(row);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(
            rows.iter().map(|row| row["g"].clone()).collect::<Vec<_>>(),
            [
                Value::String("A".into()),
                Value::String("B".into()),
                Value::String("C".into())
            ]
        );
        assert_eq!(
            rows[0]["evidence"],
            Value::List(vec![
                Value::String("memory:a1".into()),
                Value::String("memory:a2".into())
            ])
        );
        assert_eq!(
            report
                .execution_profile
                .pipeline_memory_report
                .query_memory_completion_bytes,
            0
        );
        if let Some(reference) = &reference_rows {
            assert_eq!(rows.len(), reference.len());
            for (actual, expected) in rows.iter().zip(reference) {
                assert_eq!(actual["g"], expected["g"]);
                assert_eq!(actual["evidence"], expected["evidence"]);
                let (Value::Float(actual), Value::Float(expected)) =
                    (&actual["best"], &expected["best"])
                else {
                    panic!("missing Float score")
                };
                assert!(
                    (actual - expected).abs() < 1e-12,
                    "{backend}: {actual} != {expected}"
                );
            }
        } else {
            reference_rows = Some(rows.clone());
        }
        let text_reports = &report.execution_profile.text_seed_execution_reports;
        assert_eq!(text_reports.len(), 1);
        let text = &text_reports[0];
        assert_eq!(text.status(), "complete");
        assert!(text.operator_id.is_some());
        assert_eq!(text.candidate_count, 8);
        assert_eq!(text.candidate_limit, 8);
        let source = text.source.unwrap();
        assert_eq!(source.corpus_documents, Some(8));
        assert_eq!(source.matching_documents, Some(8));
        if backend == "disk" {
            assert!(source.generation.is_some());
            assert!(source.postings_visited.unwrap() > 0);
            assert!(source.bytes_read.unwrap() > 0);
        } else {
            assert_eq!(source.generation, None);
        }
        let grouped = &report.execution_profile.grouped_evidence_reports;
        assert_eq!(grouped.len(), 1);
        assert!(grouped[0].operator_id.is_some());
        assert!(grouped[0].executed && grouped[0].input_exhausted);
        assert_eq!(grouped[0].input_rows, 7);
        assert_eq!(grouped[0].groups_considered, 3);
        assert_eq!(grouped[0].retained_evidence, 4);
        assert_eq!(grouped[0].output_groups, 3);
        assert!(grouped[0].peak_state_bytes > 0);
        assert_eq!(grouped[0].spilled_bytes, 0);
        assert!(!report.execution_profile.graph_expansion_reports.is_empty());
        assert!(report
            .execution_profile
            .graph_expansion_reports
            .iter()
            .all(|report| !report.truncated()));
        let window = query.replace("complete := true, maxCandidates := $cap", "topK := 3");
        let mut window_groups = Vec::new();
        old.query_request_streaming_with_external(
            crate::QueryRequest::new(&window).with_params(&parameters),
            &mut external,
            |row| {
                window_groups.push(row["g"].clone());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            window_groups,
            [Value::String("A".into())],
            "fixture must distinguish a fixed document prefix from complete grouping"
        );
        let mut narrow = parameters.clone();
        narrow.insert("cap".into(), Value::Int(7));
        assert!(matches!(
            old.query_request_streaming_with_external(
                crate::QueryRequest::new(query).with_params(&narrow),
                &mut external,
                |_| panic!("incomplete domain cannot be delivered")
            ),
            Err(HawDBError::ResourceBudgetExceeded(_))
        ));
        database
            .query("MATCH (m:Memory {id: 'b1'}) DETACH DELETE m")
            .unwrap();
        let mut retained = Vec::new();
        old.query_request_streaming_with_external(
            crate::QueryRequest::new(query).with_params(&parameters),
            &mut external,
            |row| {
                retained.push(row);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(retained, rows);
        let mut current = Vec::new();
        database
            .begin_read_transaction()
            .unwrap()
            .query_request_streaming_with_external(
                crate::QueryRequest::new(query).with_params(&parameters),
                &mut external,
                |row| {
                    current.push(row["g"].clone());
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(
            current,
            [Value::String("A".into()), Value::String("C".into())]
        );
        drop(old);
        drop(projection);
        drop(out_of_core_projection);
        if backend != "raw" {
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}

mod catch_up_admission_tests;
