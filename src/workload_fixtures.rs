use crate::{
    nowledge_mem_graph_augmentation_state_route_query,
    nowledge_mem_graph_community_members_route_query,
    nowledge_mem_graph_community_recent_memories_route_query,
    nowledge_mem_graph_community_subgraph_route_query, nowledge_mem_graph_node_details_route_query,
    nowledge_mem_graph_orphans_route_query, nowledge_mem_graph_overview_route_query,
    nowledge_mem_graph_pagerank_plan_route_query, nowledge_mem_graph_sample_route_query, Database,
    KnowledgeCandidateScoringPolicy, KnowledgeFanoutReasonCode, KnowledgeRetrievalRequest,
    NowledgeMemGraph, NowledgeMemGraphMode, NowledgeMemQueryExecutionPath,
    NowledgeMemQueryReportOptions, NowledgeMemReadOptions, Result, RouteQuery, SearchFusionWeights,
    SearchIndex, SearchMode, SearchPredicatePushdownReport, SearchQueryOptions,
    SearchRebuildOptions, Value, DENSE_ADJACENCY_DEGREE_THRESHOLD,
};
use skein_core::{
    GraphRagQueryBinding, GraphRagQueryDraft, GraphRagQueryPattern, GraphRagQueryPredicate,
    GraphRagQueryPredicateOperator, GraphRagQueryProjection, GraphRagSchemaContextOptions,
};
use std::collections::BTreeMap;

pub use skein_evidence::replacement_contract::NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadFixtureOptions {
    pub limit: usize,
    pub max_entities: usize,
    pub max_edges: usize,
    pub changed_since_epoch_nanos: Option<i64>,
    pub capture_physical_plan: bool,
    pub include_bounded_expansion_probes: bool,
}

impl Default for NowledgeGraphRouteWorkloadFixtureOptions {
    fn default() -> Self {
        Self {
            limit: 8,
            max_entities: 8,
            max_edges: 16,
            changed_since_epoch_nanos: Some(100),
            capture_physical_plan: true,
            include_bounded_expansion_probes: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadFixtureReport {
    pub protocol: &'static str,
    pub ready: bool,
    pub route_count: usize,
    pub query_count: usize,
    pub failed_query_count: usize,
    pub total_rows: usize,
    pub total_elapsed_micros: u128,
    pub bounded_expansion_probe_count: usize,
    pub failed_bounded_expansion_probe_count: usize,
    pub bounded_expansion_reports: Vec<NowledgeGraphRouteWorkloadBoundedExpansionReport>,
    pub search_metadata_probe_count: usize,
    pub failed_search_metadata_probe_count: usize,
    pub search_metadata_reports: Vec<NowledgeSearchMetadataWorkloadReport>,
    pub graph_rag_probe_count: usize,
    pub failed_graph_rag_probe_count: usize,
    pub graph_rag_reports: Vec<NowledgeGraphRagWorkloadReport>,
    pub source_projection_probe_count: usize,
    pub failed_source_projection_probe_count: usize,
    pub source_projection_reports: Vec<NowledgeSourceProjectionWorkloadReport>,
    pub routes: Vec<NowledgeGraphRouteWorkloadRouteReport>,
}

impl NowledgeGraphRouteWorkloadFixtureReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "route_count": self.route_count,
            "query_count": self.query_count,
            "failed_query_count": self.failed_query_count,
            "total_rows": self.total_rows,
            "total_elapsed_micros": self.total_elapsed_micros,
            "bounded_expansion_probe_count": self.bounded_expansion_probe_count,
            "failed_bounded_expansion_probe_count": self.failed_bounded_expansion_probe_count,
            "bounded_expansion_reports": self.bounded_expansion_reports.iter().map(NowledgeGraphRouteWorkloadBoundedExpansionReport::json).collect::<Vec<_>>(),
            "search_metadata_probe_count": self.search_metadata_probe_count,
            "failed_search_metadata_probe_count": self.failed_search_metadata_probe_count,
            "search_metadata_reports": self.search_metadata_reports.iter().map(NowledgeSearchMetadataWorkloadReport::json).collect::<Vec<_>>(),
            "graph_rag_probe_count": self.graph_rag_probe_count,
            "failed_graph_rag_probe_count": self.failed_graph_rag_probe_count,
            "graph_rag_reports": self.graph_rag_reports.iter().map(NowledgeGraphRagWorkloadReport::json).collect::<Vec<_>>(),
            "source_projection_probe_count": self.source_projection_probe_count,
            "failed_source_projection_probe_count": self.failed_source_projection_probe_count,
            "source_projection_reports": self.source_projection_reports.iter().map(NowledgeSourceProjectionWorkloadReport::json).collect::<Vec<_>>(),
            "routes": self.routes.iter().map(NowledgeGraphRouteWorkloadRouteReport::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeSourceProjectionWorkloadReport {
    pub name: String,
    pub ready: bool,
    pub source_graph_commit_epoch: Option<u64>,
    pub complete_through_graph_commit_epoch: Option<u64>,
    pub too_small_batch_failed_closed: bool,
    pub operation_count: usize,
    pub upserted_documents: usize,
    pub deleted_documents: usize,
    pub source_document_count: usize,
    pub indexed_source_document_ready: bool,
    pub error_class: Option<String>,
}

impl NowledgeSourceProjectionWorkloadReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "ready": self.ready,
            "source_graph_commit_epoch": self.source_graph_commit_epoch,
            "complete_through_graph_commit_epoch": self.complete_through_graph_commit_epoch,
            "too_small_batch_failed_closed": self.too_small_batch_failed_closed,
            "operation_count": self.operation_count,
            "upserted_documents": self.upserted_documents,
            "deleted_documents": self.deleted_documents,
            "source_document_count": self.source_document_count,
            "indexed_source_document_ready": self.indexed_source_document_ready,
            "error_class": self.error_class,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphRagWorkloadReport {
    pub name: String,
    pub ready: bool,
    pub schema_protocol: Option<String>,
    pub context_epoch: Option<u64>,
    pub schema_fingerprint: Option<u64>,
    pub label_count: usize,
    pub relationship_type_count: usize,
    pub property_count: usize,
    pub route_count: usize,
    pub common_path_count: usize,
    pub parameter_requirement_count: usize,
    pub row_count: usize,
    pub max_rows: Option<usize>,
    pub execution_row_cap: Option<usize>,
    pub estimated_payload_bytes: usize,
    pub row_budget_exceeded: bool,
    pub payload_budget_exceeded: bool,
    pub blocking_operator_count: usize,
    pub streaming: bool,
    pub error_class: Option<String>,
}

impl NowledgeGraphRagWorkloadReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "ready": self.ready,
            "schema_protocol": self.schema_protocol,
            "context_epoch": self.context_epoch,
            "schema_fingerprint": self.schema_fingerprint,
            "label_count": self.label_count,
            "relationship_type_count": self.relationship_type_count,
            "property_count": self.property_count,
            "route_count": self.route_count,
            "common_path_count": self.common_path_count,
            "parameter_requirement_count": self.parameter_requirement_count,
            "row_count": self.row_count,
            "max_rows": self.max_rows,
            "execution_row_cap": self.execution_row_cap,
            "estimated_payload_bytes": self.estimated_payload_bytes,
            "row_budget_exceeded": self.row_budget_exceeded,
            "payload_budget_exceeded": self.payload_budget_exceeded,
            "blocking_operator_count": self.blocking_operator_count,
            "streaming": self.streaming,
            "error_class": self.error_class,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeSearchMetadataWorkloadReport {
    pub name: String,
    pub ready: bool,
    pub total_hits: usize,
    pub document_count: usize,
    pub filtered_document_count: usize,
    pub metadata_filters: BTreeMap<String, String>,
    pub input_predicate_count: usize,
    pub pushed_predicate_count: usize,
    pub residual_predicate_count: usize,
    pub pruned_segment_count: usize,
    pub scanned_segment_count: usize,
    pub field_summary_count: usize,
    pub fields: Vec<String>,
    pub error_class: Option<String>,
}

impl NowledgeSearchMetadataWorkloadReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "ready": self.ready,
            "total_hits": self.total_hits,
            "document_count": self.document_count,
            "filtered_document_count": self.filtered_document_count,
            "metadata_filters": self.metadata_filters,
            "input_predicate_count": self.input_predicate_count,
            "pushed_predicate_count": self.pushed_predicate_count,
            "residual_predicate_count": self.residual_predicate_count,
            "pruned_segment_count": self.pruned_segment_count,
            "scanned_segment_count": self.scanned_segment_count,
            "field_summary_count": self.field_summary_count,
            "fields": self.fields,
            "error_class": self.error_class,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadBoundedExpansionReport {
    pub name: String,
    pub ready: bool,
    pub graph_context_limit: usize,
    pub graph_context_max_hops: usize,
    pub path_count: usize,
    pub node_count: usize,
    pub relationship_count: usize,
    pub truncated: bool,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub error_class: Option<String>,
}

impl NowledgeGraphRouteWorkloadBoundedExpansionReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "ready": self.ready,
            "graph_context_limit": self.graph_context_limit,
            "graph_context_max_hops": self.graph_context_max_hops,
            "path_count": self.path_count,
            "node_count": self.node_count,
            "relationship_count": self.relationship_count,
            "truncated": self.truncated,
            "fanout_reason_codes": self.fanout_reason_codes.iter().map(|code| code.as_str()).collect::<Vec<_>>(),
            "error_class": self.error_class,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadRouteReport {
    pub route: String,
    pub ready: bool,
    pub query_count: usize,
    pub failed_query_count: usize,
    pub total_rows: usize,
    pub total_elapsed_micros: u128,
    pub queries: Vec<NowledgeGraphRouteWorkloadQueryReport>,
}

impl NowledgeGraphRouteWorkloadRouteReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "route": self.route,
            "ready": self.ready,
            "query_count": self.query_count,
            "failed_query_count": self.failed_query_count,
            "total_rows": self.total_rows,
            "total_elapsed_micros": self.total_elapsed_micros,
            "queries": self.queries.iter().map(NowledgeGraphRouteWorkloadQueryReport::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadQueryReport {
    pub name: String,
    pub query_family: Option<String>,
    pub ready: bool,
    pub row_count: usize,
    pub elapsed_micros: u128,
    pub execution_path: Option<NowledgeMemQueryExecutionPath>,
    pub physical_plan_captured: bool,
    pub scan_pruning_report_count: usize,
    pub physical_operator_counts: BTreeMap<String, usize>,
    pub error_class: Option<String>,
}

impl NowledgeGraphRouteWorkloadQueryReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "query_family": self.query_family,
            "ready": self.ready,
            "row_count": self.row_count,
            "elapsed_micros": self.elapsed_micros,
            "execution_path": self.execution_path.map(NowledgeMemQueryExecutionPath::as_str),
            "physical_plan_captured": self.physical_plan_captured,
            "scan_pruning_report_count": self.scan_pruning_report_count,
            "physical_operator_counts": self.physical_operator_counts,
            "error_class": self.error_class,
        })
    }
}

pub fn nowledge_graph_route_workload_fixture_report(
    options: NowledgeGraphRouteWorkloadFixtureOptions,
) -> Result<NowledgeGraphRouteWorkloadFixtureReport> {
    let mut graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    seed_graph_route_workload_fixture(&mut graph)?;
    let route_queries = nowledge_graph_route_workload_fixture_queries(options)?;
    run_graph_route_workload_fixture(&mut graph, &route_queries, options)
}

pub fn nowledge_graph_route_workload_fixture_queries(
    options: NowledgeGraphRouteWorkloadFixtureOptions,
) -> Result<Vec<RouteQuery>> {
    Ok(vec![
        nowledge_mem_graph_overview_route_query(options.limit)?,
        nowledge_mem_graph_sample_route_query(options.limit)?,
        nowledge_mem_graph_node_details_route_query(0)?,
        nowledge_mem_graph_community_members_route_query(42, options.limit)?,
        nowledge_mem_graph_community_recent_memories_route_query(3676, options.limit)?,
        nowledge_mem_graph_community_subgraph_route_query(
            3505,
            options.max_entities,
            ["community-subgraph-alpha", "community-subgraph-beta"],
            options.max_edges,
        )?,
        nowledge_mem_graph_augmentation_state_route_query(),
        nowledge_mem_graph_pagerank_plan_route_query(options.changed_since_epoch_nanos),
        nowledge_mem_graph_orphans_route_query(options.limit)?,
    ])
}

fn run_graph_route_workload_fixture(
    graph: &mut NowledgeMemGraph,
    route_queries: &[RouteQuery],
    options: NowledgeGraphRouteWorkloadFixtureOptions,
) -> Result<NowledgeGraphRouteWorkloadFixtureReport> {
    let query_options = NowledgeMemQueryReportOptions {
        capture_physical_plan: options.capture_physical_plan,
        slow_log_threshold_micros: None,
    };
    let routes = route_queries
        .iter()
        .map(|route| run_graph_route_workload_route(graph, route, query_options))
        .collect::<Vec<_>>();
    let bounded_expansion_reports = if options.include_bounded_expansion_probes {
        run_bounded_expansion_workload_probes(graph)
    } else {
        Vec::new()
    };
    let search_metadata_reports = run_search_metadata_workload_probes(graph);
    let graph_rag_reports = run_graph_rag_workload_probes(graph);
    let source_projection_reports = run_source_projection_workload_probes(graph);
    let query_count = routes.iter().map(|route| route.query_count).sum();
    let failed_query_count = routes.iter().map(|route| route.failed_query_count).sum();
    let total_rows = routes.iter().map(|route| route.total_rows).sum();
    let total_elapsed_micros = routes.iter().map(|route| route.total_elapsed_micros).sum();
    let failed_bounded_expansion_probe_count = bounded_expansion_reports
        .iter()
        .filter(|report| !report.ready)
        .count();
    let failed_search_metadata_probe_count = search_metadata_reports
        .iter()
        .filter(|report| !report.ready)
        .count();
    let failed_graph_rag_probe_count = graph_rag_reports
        .iter()
        .filter(|report| !report.ready)
        .count();
    let failed_source_projection_probe_count = source_projection_reports
        .iter()
        .filter(|report| !report.ready)
        .count();
    Ok(NowledgeGraphRouteWorkloadFixtureReport {
        protocol: NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
        ready: !routes.is_empty()
            && failed_query_count == 0
            && routes.iter().all(|route| route.ready)
            && failed_bounded_expansion_probe_count == 0
            && failed_search_metadata_probe_count == 0
            && failed_graph_rag_probe_count == 0
            && failed_source_projection_probe_count == 0,
        route_count: routes.len(),
        query_count,
        failed_query_count,
        total_rows,
        total_elapsed_micros,
        bounded_expansion_probe_count: bounded_expansion_reports.len(),
        failed_bounded_expansion_probe_count,
        bounded_expansion_reports,
        search_metadata_probe_count: search_metadata_reports.len(),
        failed_search_metadata_probe_count,
        search_metadata_reports,
        graph_rag_probe_count: graph_rag_reports.len(),
        failed_graph_rag_probe_count,
        graph_rag_reports,
        source_projection_probe_count: source_projection_reports.len(),
        failed_source_projection_probe_count,
        source_projection_reports,
        routes,
    })
}

fn run_graph_route_workload_route(
    graph: &mut NowledgeMemGraph,
    route: &RouteQuery,
    options: NowledgeMemQueryReportOptions,
) -> NowledgeGraphRouteWorkloadRouteReport {
    let queries = route
        .queries
        .iter()
        .map(|query| {
            match graph.query_with_params_with_report_options(
                &query.cypher,
                &query.parameters,
                options,
            ) {
                Ok(output) => NowledgeGraphRouteWorkloadQueryReport {
                    name: query.name.clone(),
                    query_family: query.query_family.clone(),
                    ready: true,
                    row_count: output.output.rows.len(),
                    elapsed_micros: output.report.elapsed_micros,
                    execution_path: Some(output.report.execution_path),
                    physical_plan_captured: output.report.physical_plan_captured,
                    scan_pruning_report_count: output.report.scan_pruning_reports.len(),
                    physical_operator_counts: output.report.physical_operator_counts,
                    error_class: None,
                },
                Err(error) => NowledgeGraphRouteWorkloadQueryReport {
                    name: query.name.clone(),
                    query_family: query.query_family.clone(),
                    ready: false,
                    row_count: 0,
                    elapsed_micros: 0,
                    execution_path: None,
                    physical_plan_captured: false,
                    scan_pruning_report_count: 0,
                    physical_operator_counts: BTreeMap::new(),
                    error_class: Some(error_class(&error)),
                },
            }
        })
        .collect::<Vec<_>>();
    let failed_query_count = queries.iter().filter(|query| !query.ready).count();
    let total_rows = queries.iter().map(|query| query.row_count).sum();
    let total_elapsed_micros = queries.iter().map(|query| query.elapsed_micros).sum();
    NowledgeGraphRouteWorkloadRouteReport {
        route: route.route.clone(),
        ready: !queries.is_empty() && failed_query_count == 0,
        query_count: queries.len(),
        failed_query_count,
        total_rows,
        total_elapsed_micros,
        queries,
    }
}

fn seed_graph_route_workload_fixture(graph: &mut NowledgeMemGraph) -> Result<()> {
    for statement in GRAPH_ROUTE_WORKLOAD_FIXTURE_STATEMENTS {
        graph.query(statement)?;
    }
    seed_bounded_expansion_workload_fixture(graph)?;
    seed_search_metadata_workload_fixture(graph)?;
    Ok(())
}

fn run_bounded_expansion_workload_probes(
    graph: &mut NowledgeMemGraph,
) -> Vec<NowledgeGraphRouteWorkloadBoundedExpansionReport> {
    let mut search_index = SearchIndex::in_memory();
    if let Err(error) = graph
        .database_mut()
        .rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
    {
        return vec![bounded_expansion_error_report(
            "search-projection-rebuild",
            0,
            0,
            error_class(&error),
        )];
    }
    vec![
        run_bounded_expansion_probe(
            graph.database(),
            &search_index,
            "two-hop-context",
            "workload root traversal",
            4,
            2,
        ),
        run_bounded_expansion_probe(
            graph.database(),
            &search_index,
            "dense-context",
            "workload dense retrieval",
            DENSE_ADJACENCY_DEGREE_THRESHOLD,
            1,
        ),
    ]
}

fn run_search_metadata_workload_probes(
    graph: &mut NowledgeMemGraph,
) -> Vec<NowledgeSearchMetadataWorkloadReport> {
    let mut search_index = SearchIndex::in_memory();
    if let Err(error) = graph
        .database_mut()
        .rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
    {
        return vec![search_metadata_error_report(
            "search-projection-rebuild",
            BTreeMap::new(),
            error_class(&error),
        )];
    }
    [
        (
            "unit-type-lifecycle",
            BTreeMap::from([
                (
                    "unit_type__in".to_string(),
                    r#"["fact","learning"]"#.to_string(),
                ),
                (
                    "lifecycle_state__not_in".to_string(),
                    r#"["deleted","forgotten"]"#.to_string(),
                ),
            ]),
        ),
        (
            "importance-confidence-created-at",
            BTreeMap::from([
                ("importance__gte".to_string(), "0.7".to_string()),
                ("confidence__gte".to_string(), "0.8".to_string()),
                ("created_at__gte".to_string(), "100".to_string()),
            ]),
        ),
        (
            "source-space",
            BTreeMap::from([
                ("source_id".to_string(), "workload-source-1".to_string()),
                ("space_id".to_string(), "default".to_string()),
            ]),
        ),
    ]
    .into_iter()
    .map(|(name, filters)| run_search_metadata_workload_probe(&search_index, name, filters))
    .collect()
}

fn run_search_metadata_workload_probe(
    search_index: &SearchIndex,
    name: &str,
    metadata_filters: BTreeMap<String, String>,
) -> NowledgeSearchMetadataWorkloadReport {
    let result = search_index.search_with_options(
        "workload metadata retrieval",
        None,
        SearchMode::Text,
        SearchQueryOptions {
            limit: 10,
            offset: 0,
            rank_window: None,
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: metadata_filters.clone(),
            policy_epoch: None,
        },
    );
    let pushdown = &result.candidate_set.metadata_predicate_pushdown;
    let fields = search_metadata_pushdown_fields(pushdown);
    NowledgeSearchMetadataWorkloadReport {
        name: name.to_string(),
        ready: result.total_hits > 0
            && result.filtered_document_count < result.document_count
            && pushdown.input_predicate_count > 0
            && pushdown.input_predicate_count == pushdown.pushed_predicate_count
            && pushdown.residual_predicate_count == 0
            && pushdown.parse_error.is_none()
            && !pushdown.field_summaries.is_empty(),
        total_hits: result.total_hits,
        document_count: result.document_count,
        filtered_document_count: result.filtered_document_count,
        metadata_filters,
        input_predicate_count: pushdown.input_predicate_count,
        pushed_predicate_count: pushdown.pushed_predicate_count,
        residual_predicate_count: pushdown.residual_predicate_count,
        pruned_segment_count: pushdown.pruned_segment_count,
        scanned_segment_count: pushdown.scanned_segment_count,
        field_summary_count: pushdown.field_summaries.len(),
        fields,
        error_class: None,
    }
}

fn search_metadata_error_report(
    name: &str,
    metadata_filters: BTreeMap<String, String>,
    error_class: String,
) -> NowledgeSearchMetadataWorkloadReport {
    NowledgeSearchMetadataWorkloadReport {
        name: name.to_string(),
        ready: false,
        total_hits: 0,
        document_count: 0,
        filtered_document_count: 0,
        metadata_filters,
        input_predicate_count: 0,
        pushed_predicate_count: 0,
        residual_predicate_count: 0,
        pruned_segment_count: 0,
        scanned_segment_count: 0,
        field_summary_count: 0,
        fields: Vec::new(),
        error_class: Some(error_class),
    }
}

fn search_metadata_pushdown_fields(pushdown: &SearchPredicatePushdownReport) -> Vec<String> {
    pushdown
        .field_summaries
        .iter()
        .map(|summary| summary.field.clone())
        .collect()
}

fn run_source_projection_workload_probes(
    graph: &mut NowledgeMemGraph,
) -> Vec<NowledgeSourceProjectionWorkloadReport> {
    vec![run_source_projection_workload_probe(
        graph,
        "source-ingest-composite-changefeed",
    )]
}

fn run_source_projection_workload_probe(
    graph: &mut NowledgeMemGraph,
    name: &str,
) -> NowledgeSourceProjectionWorkloadReport {
    let db = graph.database_mut();
    let source_graph_commit_epoch_before = db.commit_epoch();
    let mut transaction = db.begin_transaction();
    for statement in [
        "CREATE (:Source {id: 'workload-source-v1', original_name: 'workload.md', lifecycle_state: 'parsed', space_id: 'default', version: 1})",
        "CREATE (:Source {id: 'workload-source-v2', original_name: 'workload.md', lifecycle_state: 'indexed', space_id: 'default', version: 2})",
        "MATCH (newer:Source {id: 'workload-source-v2'}), (older:Source {id: 'workload-source-v1'})
         CREATE (newer)-[:REVISED_AS {revision_type: 'content_refresh', detected_by: 'workload_fixture'}]->(older)",
    ] {
        if let Err(error) = transaction.query(statement) {
            return source_projection_error_report(name, &error_class(&error));
        }
    }
    if let Err(error) = transaction.commit() {
        return source_projection_error_report(name, &error_class(&error));
    }
    let source_graph_commit_epoch = db.commit_epoch();
    let too_small_batch_failed_closed = db
        .build_search_projection_graph_delta_request_after(
            source_graph_commit_epoch_before,
            Some(1),
        )
        .is_err();
    let request = match db.build_search_projection_graph_delta_request_after(
        source_graph_commit_epoch_before,
        Some(2),
    ) {
        Ok(Some(request)) => request,
        Ok(None) => return source_projection_error_report(name, "missing_delta_request"),
        Err(error) => return source_projection_error_report(name, &error_class(&error)),
    };
    let complete_through_graph_commit_epoch = request.complete_through_graph_commit_epoch;
    let mut search_index = SearchIndex::in_memory();
    let delta_report = match db.apply_search_projection_graph_delta(&mut search_index, request) {
        Ok(report) => report,
        Err(error) => return source_projection_error_report(name, &error_class(&error)),
    };
    let source_document_count = ["source:workload-source-v1", "source:workload-source-v2"]
        .into_iter()
        .filter(|document_id| search_index.document(document_id).is_some())
        .count();
    let indexed_source_document_ready = search_index
        .document("source:workload-source-v2")
        .and_then(|document| document.metadata.get("lifecycle_state"))
        .is_some_and(|state| state == "indexed");
    let ready = too_small_batch_failed_closed
        && delta_report.operation_count == 2
        && delta_report.upserted_documents == 2
        && delta_report.deleted_documents == 0
        && complete_through_graph_commit_epoch == Some(source_graph_commit_epoch)
        && delta_report.source_graph_commit_epoch_after == Some(source_graph_commit_epoch)
        && source_document_count == 2
        && indexed_source_document_ready;
    NowledgeSourceProjectionWorkloadReport {
        name: name.to_string(),
        ready,
        source_graph_commit_epoch: Some(source_graph_commit_epoch),
        complete_through_graph_commit_epoch,
        too_small_batch_failed_closed,
        operation_count: delta_report.operation_count,
        upserted_documents: delta_report.upserted_documents,
        deleted_documents: delta_report.deleted_documents,
        source_document_count,
        indexed_source_document_ready,
        error_class: None,
    }
}

fn source_projection_error_report(
    name: &str,
    error_class: &str,
) -> NowledgeSourceProjectionWorkloadReport {
    NowledgeSourceProjectionWorkloadReport {
        name: name.to_string(),
        ready: false,
        source_graph_commit_epoch: None,
        complete_through_graph_commit_epoch: None,
        too_small_batch_failed_closed: false,
        operation_count: 0,
        upserted_documents: 0,
        deleted_documents: 0,
        source_document_count: 0,
        indexed_source_document_ready: false,
        error_class: Some(error_class.to_string()),
    }
}

fn run_graph_rag_workload_probes(graph: &NowledgeMemGraph) -> Vec<NowledgeGraphRagWorkloadReport> {
    vec![run_graph_rag_workload_probe(graph, "memory-to-entity")]
}

fn run_graph_rag_workload_probe(
    graph: &NowledgeMemGraph,
    name: &str,
) -> NowledgeGraphRagWorkloadReport {
    let context = graph.graph_rag_schema_context(GraphRagSchemaContextOptions::default());
    let draft = GraphRagQueryDraft {
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
            property: "id".to_string(),
            alias: "entity_id".to_string(),
        }],
        limit: 4,
    };
    let generated = match context.generate_query(&draft) {
        Ok(query) => query,
        Err(error) => {
            let _ = error;
            return graph_rag_error_report(name, &context, "generation");
        }
    };
    let parameters = BTreeMap::from([(
        "memory_id".to_string(),
        Value::String("community-recent-new".to_string()),
    )]);
    match graph.read_generated_graph_rag(
        &generated,
        &parameters,
        &NowledgeMemReadOptions {
            max_rows: Some(4),
            max_estimated_payload_bytes: Some(4096),
        },
    ) {
        Ok(output) => NowledgeGraphRagWorkloadReport {
            name: name.to_string(),
            ready: output.report.row_count > 0
                && output.report.row_count <= 4
                && !output.report.row_budget_exceeded
                && !output.report.payload_budget_exceeded
                && output.report.blocking_operator_count == 0
                && !output.report.streaming
                && generated.parameter_requirements().len() == 1,
            schema_protocol: Some(context.protocol.to_string()),
            context_epoch: Some(context.computed_at_commit_epoch),
            schema_fingerprint: Some(context.fingerprint),
            label_count: context.labels.len(),
            relationship_type_count: context.relationship_types.len(),
            property_count: context.properties.len(),
            route_count: context.routes.len(),
            common_path_count: context.common_paths.len(),
            parameter_requirement_count: generated.parameter_requirements().len(),
            row_count: output.report.row_count,
            max_rows: output.report.max_rows,
            execution_row_cap: output.report.execution_row_cap,
            estimated_payload_bytes: output.report.estimated_payload_bytes,
            row_budget_exceeded: output.report.row_budget_exceeded,
            payload_budget_exceeded: output.report.payload_budget_exceeded,
            blocking_operator_count: output.report.blocking_operator_count,
            streaming: output.report.streaming,
            error_class: None,
        },
        Err(error) => graph_rag_error_report(name, &context, &error_class(&error)),
    }
}

fn graph_rag_error_report(
    name: &str,
    context: &skein_core::GraphRagSchemaContext,
    error_class: &str,
) -> NowledgeGraphRagWorkloadReport {
    NowledgeGraphRagWorkloadReport {
        name: name.to_string(),
        ready: false,
        schema_protocol: Some(context.protocol.to_string()),
        context_epoch: Some(context.computed_at_commit_epoch),
        schema_fingerprint: Some(context.fingerprint),
        label_count: context.labels.len(),
        relationship_type_count: context.relationship_types.len(),
        property_count: context.properties.len(),
        route_count: context.routes.len(),
        common_path_count: context.common_paths.len(),
        parameter_requirement_count: 0,
        row_count: 0,
        max_rows: None,
        execution_row_cap: None,
        estimated_payload_bytes: 0,
        row_budget_exceeded: false,
        payload_budget_exceeded: false,
        blocking_operator_count: 0,
        streaming: false,
        error_class: Some(error_class.to_string()),
    }
}

fn run_bounded_expansion_probe(
    db: &Database,
    search_index: &SearchIndex,
    name: &str,
    query_text: &str,
    graph_context_limit: usize,
    graph_context_max_hops: usize,
) -> NowledgeGraphRouteWorkloadBoundedExpansionReport {
    let output = db.retrieve_knowledge(
        search_index,
        &KnowledgeRetrievalRequest {
            query_text: query_text.to_string(),
            query_embedding: None,
            mode: SearchMode::Text,
            limit: 1,
            offset: 0,
            rank_window: None,
            search_fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            candidate_limit: None,
            candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
            graph_seed_limit: 0,
            graph_context_limit,
            graph_context_max_hops,
        },
    );
    NowledgeGraphRouteWorkloadBoundedExpansionReport {
        name: name.to_string(),
        ready: output.search.total_hits > 0
            && output.diagnostics.graph_context_path_count > 0
            && output.diagnostics.graph_context_path_count <= graph_context_limit,
        graph_context_limit,
        graph_context_max_hops,
        path_count: output.diagnostics.graph_context_path_count,
        node_count: output.diagnostics.graph_context_node_count,
        relationship_count: output.diagnostics.graph_context_relationship_count,
        truncated: output.diagnostics.graph_context_truncated,
        fanout_reason_codes: output.diagnostics.fanout_reason_codes,
        error_class: None,
    }
}

fn bounded_expansion_error_report(
    name: &str,
    graph_context_limit: usize,
    graph_context_max_hops: usize,
    error_class: String,
) -> NowledgeGraphRouteWorkloadBoundedExpansionReport {
    NowledgeGraphRouteWorkloadBoundedExpansionReport {
        name: name.to_string(),
        ready: false,
        graph_context_limit,
        graph_context_max_hops,
        path_count: 0,
        node_count: 0,
        relationship_count: 0,
        truncated: false,
        fanout_reason_codes: Vec::new(),
        error_class: Some(error_class),
    }
}

fn seed_bounded_expansion_workload_fixture(graph: &mut NowledgeMemGraph) -> Result<()> {
    graph.query("CREATE (:Memory {id: 'workload-root', title: 'Workload root traversal', content: 'workload root traversal'})-[:LINKS]->(:Entity {id: 'workload-mid', name: 'Workload Mid'})")?;
    graph.query("CREATE (:Entity {id: 'workload-leaf', name: 'Workload Leaf'})")?;
    graph.query("MATCH (e:Entity {id: 'workload-mid'}), (leaf:Entity {id: 'workload-leaf'}) CREATE (e)-[:LINKS {weight: 2}]->(leaf)")?;
    graph.query("CREATE (:Memory {id: 'workload-dense-root', title: 'Workload dense retrieval', content: 'workload dense retrieval'})")?;
    for index in 0..DENSE_ADJACENCY_DEGREE_THRESHOLD {
        graph.query(&format!(
            "CREATE (:Entity {{id: 'workload-dense-entity-{index}', name: 'Dense Entity {index}'}})"
        ))?;
        graph.query(&format!(
            "MATCH (m:Memory {{id: 'workload-dense-root'}}), (e:Entity {{id: 'workload-dense-entity-{index}'}}) CREATE (m)-[:MENTIONS]->(e)"
        ))?;
    }
    Ok(())
}

fn seed_search_metadata_workload_fixture(graph: &mut NowledgeMemGraph) -> Result<()> {
    for statement in SEARCH_METADATA_WORKLOAD_FIXTURE_STATEMENTS {
        graph.query(statement)?;
    }
    Ok(())
}

fn error_class(error: &crate::SkeinError) -> String {
    match error {
        crate::SkeinError::Parse(_) => "parse",
        crate::SkeinError::Semantic(_) => "semantic",
        crate::SkeinError::Execution(_) => "execution",
        crate::SkeinError::Storage(_)
        | crate::SkeinError::StorageIntegrity(_)
        | crate::SkeinError::AppendSequenceExhausted { .. } => "storage",
        crate::SkeinError::CapabilityUnavailable { .. } => "capability_unavailable",
    }
    .to_string()
}

const GRAPH_ROUTE_WORKLOAD_FIXTURE_STATEMENTS: &[&str] = &[
    "CREATE (:GraphMeta {meta_id: 'main', community_detection_applied: true, pagerank_applied: true, community_algorithm: 'louvain', community_resolution: 1.0, community_count: 3, pagerank_algorithm: 'pagerank', pagerank_damping: 0.85, pagerank_iterations: 20, last_augmentation_at: 1000, schema_version: 2, community_detection_computed_at: 900, pagerank_computed_at: 950})",
    "CREATE (:Memory {id: 'overview-memory-1', title: 'Overview One', content: 'body one', pagerank_score: 3.0, importance: 0.1, community_id: 7001, space_id: 'default', created_at: 101, updated_at: 201, source: 'overview', event_start: 301, event_end: 401})",
    "CREATE (:Memory {id: 'overview-memory-2', content: 'Fallback body', importance: 2.0, community_id: 7002, space_id: 'default', created_at: 102, updated_at: 202, source: 'overview', event_start: 302, event_end: 402})",
    "CREATE (:Memory {id: 'sample-memory-a', title: 'Sample A', content: 'sample body A', pagerank_score: 1.0, importance: 0.1, community_id: 8001, space_id: 'default', created_at: 101, updated_at: 201, source: 'sample'})",
    "CREATE (:Memory {id: 'sample-memory-b', content: 'Sample body B', importance: 2.0, community_id: 8002, space_id: 'default', created_at: 102, updated_at: 202, source: 'sample'})",
    "CREATE (:Memory {id: 'community-memory-high', title: 'Community High', content: 'high body', pagerank_score: 3.0, importance: 0.1, community_id: 42, space_id: 'default', created_at: 101, updated_at: 201, source: 'community'})",
    "CREATE (:Memory {id: 'community-memory-low', title: 'Community Low', content: 'low body', importance: 1.0, community_id: 42, space_id: 'default', created_at: 102, updated_at: 202, source: 'community'})",
    "CREATE (:Memory {id: 'community-recent-old', title: 'Recent Old', content: 'old body', importance: 0.4, created_at: 10, updated_at: 20, is_crystal: false})",
    "CREATE (:Memory {id: 'community-recent-new', title: 'Recent New', content: 'new body', importance: 0.9, created_at: 30, updated_at: 40, is_crystal: false})",
    "CREATE (:Entity {id: 'community-recent-entity-a', community_id: 3676})",
    "CREATE (:Entity {id: 'community-recent-entity-b', community_id: 3676})",
    "MATCH (m:Memory {id: 'community-recent-old'}), (e:Entity {id: 'community-recent-entity-a'}) CREATE (m)-[:MENTIONS]->(e)",
    "MATCH (m:Memory {id: 'community-recent-new'}), (e:Entity {id: 'community-recent-entity-a'}) CREATE (m)-[:MENTIONS]->(e)",
    "MATCH (m:Memory {id: 'community-recent-new'}), (e:Entity {id: 'community-recent-entity-b'}) CREATE (m)-[:MENTIONS]->(e)",
    "CREATE (:Entity {id: 'community-subgraph-alpha', name: 'Alpha Entity', entity_type: 'concept', community_id: 3505, confidence: 0.9})",
    "CREATE (:Entity {id: 'community-subgraph-beta', name: 'Beta Entity', entity_type: 'concept', community_id: 3505, confidence: 0.7})",
    "CREATE (:Entity {id: 'community-subgraph-outside', name: 'Outside Entity', entity_type: 'concept', community_id: 9999, confidence: 1.0})",
    "CREATE (:Memory {id: 'community-subgraph-memory-a', created_at: 10, updated_at: 20})",
    "CREATE (:Memory {id: 'community-subgraph-memory-b', created_at: 120, updated_at: 130})",
    "MATCH (m:Memory {id: 'community-subgraph-memory-a'}), (e:Entity {id: 'community-subgraph-alpha'}) CREATE (m)-[:MENTIONS]->(e)",
    "MATCH (m:Memory {id: 'community-subgraph-memory-b'}), (e:Entity {id: 'community-subgraph-alpha'}) CREATE (m)-[:MENTIONS]->(e)",
    "MATCH (m:Memory {id: 'community-subgraph-memory-a'}), (e:Entity {id: 'community-subgraph-beta'}) CREATE (m)-[:MENTIONS]->(e)",
    "MATCH (a:Entity {id: 'community-subgraph-alpha'}), (b:Entity {id: 'community-subgraph-beta'}) CREATE (a)-[:RELATES_TO {confidence: 0.77, relation_type: 'related', created_at: 170}]->(b)",
    "CREATE (:Memory {id: 'pagerank-plan-m1', created_at: 10, updated_at: 20})",
    "CREATE (:Memory {id: 'pagerank-plan-m2', created_at: 120, updated_at: 130})",
    "CREATE (:Entity {id: 'pagerank-plan-e1', name: 'Entity One', created_at: 15, updated_at: 25})",
    "CREATE (:Entity {id: 'pagerank-plan-e2', name: 'Entity Two', created_at: 140, updated_at: 150})",
    "MATCH (m:Memory {id: 'pagerank-plan-m1'}), (e:Entity {id: 'pagerank-plan-e1'}) CREATE (m)-[:MENTIONS {created_at: 30}]->(e)",
    "MATCH (m:Memory {id: 'pagerank-plan-m2'}), (e:Entity {id: 'pagerank-plan-e2'}) CREATE (m)-[:MENTIONS {created_at: 160}]->(e)",
    "MATCH (a:Entity {id: 'pagerank-plan-e1'}), (b:Entity {id: 'pagerank-plan-e2'}) CREATE (a)-[:RELATES_TO {created_at: 170}]->(b)",
    "MATCH (a:Memory {id: 'pagerank-plan-m1'}), (b:Memory {id: 'pagerank-plan-m2'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'active', created_at: 180}]->(b)",
    "MATCH (a:Memory {id: 'pagerank-plan-m2'}), (b:Memory {id: 'pagerank-plan-m1'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'inactive', created_at: 190}]->(b)",
    "CREATE (:Entity {id: 'orphan-entity', name: 'Orphan Entity', entity_type: 'concept', description: 'orphan'})",
    "CREATE (:Entity {id: 'mentioned-entity', name: 'Mentioned Entity', entity_type: 'concept'})",
    "CREATE (:Memory {id: 'orphan-blocking-memory', title: 'Blocking Memory'})",
    "MATCH (m:Memory {id: 'orphan-blocking-memory'}), (e:Entity {id: 'mentioned-entity'}) CREATE (m)-[:MENTIONS]->(e)",
];

const SEARCH_METADATA_WORKLOAD_FIXTURE_STATEMENTS: &[&str] = &[
    "CREATE (:Memory {id: 'metadata-fact-active', title: 'Workload metadata retrieval fact', content: 'workload metadata retrieval', unit_type: 'fact', lifecycle_state: 'active', importance: 0.9, confidence: 0.95, created_at: 120, updated_at: 160, source_id: 'workload-source-1', space_id: 'default'})",
    "CREATE (:Memory {id: 'metadata-learning-active', title: 'Workload metadata retrieval learning', content: 'workload metadata retrieval', unit_type: 'learning', lifecycle_state: 'active', importance: 0.8, confidence: 0.85, created_at: 140, updated_at: 180, source_id: 'workload-source-1', space_id: 'default'})",
    "CREATE (:Memory {id: 'metadata-task-active', title: 'Workload metadata retrieval task', content: 'workload metadata retrieval', unit_type: 'task', lifecycle_state: 'active', importance: 0.6, confidence: 0.7, created_at: 90, updated_at: 100, source_id: 'workload-source-2', space_id: 'default'})",
    "CREATE (:Memory {id: 'metadata-fact-deleted', title: 'Workload metadata retrieval deleted', content: 'workload metadata retrieval', unit_type: 'fact', lifecycle_state: 'deleted', importance: 0.95, confidence: 0.99, created_at: 150, updated_at: 190, source_id: 'workload-source-1', space_id: 'archive'})",
];

#[cfg(test)]
mod tests {
    use super::{
        nowledge_graph_route_workload_fixture_queries,
        nowledge_graph_route_workload_fixture_report, NowledgeGraphRouteWorkloadFixtureOptions,
        NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
    };
    use crate::{KnowledgeFanoutReasonCode, DENSE_ADJACENCY_DEGREE_THRESHOLD};

    #[test]
    fn graph_route_workload_fixture_runs_real_route_queries() {
        let report = nowledge_graph_route_workload_fixture_report(
            NowledgeGraphRouteWorkloadFixtureOptions::default(),
        )
        .unwrap();

        assert_eq!(
            report.protocol,
            NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL
        );
        assert!(report.ready);
        assert_eq!(report.route_count, 9);
        assert_eq!(report.failed_query_count, 0);
        assert_eq!(report.bounded_expansion_probe_count, 2);
        assert_eq!(report.failed_bounded_expansion_probe_count, 0);
        assert_eq!(report.search_metadata_probe_count, 3);
        assert_eq!(report.failed_search_metadata_probe_count, 0);
        assert_eq!(report.graph_rag_probe_count, 1);
        assert_eq!(report.failed_graph_rag_probe_count, 0);
        assert_eq!(report.source_projection_probe_count, 1);
        assert_eq!(report.failed_source_projection_probe_count, 0);
        assert!(report.query_count >= report.route_count);
        assert!(report.total_rows > 0);
        assert!(report.routes.iter().all(|route| route.ready));
        assert!(report
            .routes
            .iter()
            .flat_map(|route| route.queries.iter())
            .all(|query| query.physical_plan_captured));
        assert!(report
            .routes
            .iter()
            .flat_map(|route| route.queries.iter())
            .any(|query| query.scan_pruning_report_count > 0));
        let two_hop = report
            .bounded_expansion_reports
            .iter()
            .find(|report| report.name == "two-hop-context")
            .unwrap();
        assert_eq!(two_hop.path_count, 2);
        assert!(!two_hop.truncated);
        let dense = report
            .bounded_expansion_reports
            .iter()
            .find(|report| report.name == "dense-context")
            .unwrap();
        assert_eq!(dense.path_count, DENSE_ADJACENCY_DEGREE_THRESHOLD);
        assert!(dense
            .fanout_reason_codes
            .contains(&KnowledgeFanoutReasonCode::DenseAdjacency));
        assert!(report
            .search_metadata_reports
            .iter()
            .all(|report| report.ready));
        let typed_filter = report
            .search_metadata_reports
            .iter()
            .find(|report| report.name == "unit-type-lifecycle")
            .unwrap();
        assert_eq!(typed_filter.total_hits, 2);
        assert_eq!(typed_filter.input_predicate_count, 2);
        assert_eq!(typed_filter.pushed_predicate_count, 2);
        assert_eq!(typed_filter.residual_predicate_count, 0);
        assert!(typed_filter.fields.contains(&"unit_type".to_string()));
        assert!(typed_filter.fields.contains(&"lifecycle_state".to_string()));
        let range_filter = report
            .search_metadata_reports
            .iter()
            .find(|report| report.name == "importance-confidence-created-at")
            .unwrap();
        assert!(range_filter.fields.contains(&"importance".to_string()));
        assert!(range_filter.fields.contains(&"confidence".to_string()));
        assert!(range_filter.fields.contains(&"created_at".to_string()));
        let graph_rag = report
            .graph_rag_reports
            .iter()
            .find(|report| report.name == "memory-to-entity")
            .unwrap();
        assert!(graph_rag.ready);
        assert!(graph_rag.label_count > 0);
        assert!(graph_rag.relationship_type_count > 0);
        assert!(graph_rag.route_count > 0);
        assert_eq!(graph_rag.parameter_requirement_count, 1);
        assert!(graph_rag.row_count > 0);
        assert_eq!(graph_rag.blocking_operator_count, 0);
        assert!(!graph_rag.streaming);
        assert_eq!(graph_rag.error_class, None);
        let source_projection = report
            .source_projection_reports
            .iter()
            .find(|report| report.name == "source-ingest-composite-changefeed")
            .unwrap();
        assert!(source_projection.ready);
        assert_eq!(source_projection.operation_count, 2);
        assert_eq!(source_projection.upserted_documents, 2);
        assert_eq!(source_projection.deleted_documents, 0);
        assert_eq!(source_projection.source_document_count, 2);
        assert!(source_projection.too_small_batch_failed_closed);
        assert!(source_projection.indexed_source_document_ready);
    }

    #[test]
    fn graph_route_workload_fixture_query_catalog_is_stable() {
        let queries = nowledge_graph_route_workload_fixture_queries(
            NowledgeGraphRouteWorkloadFixtureOptions::default(),
        )
        .unwrap();
        let routes = queries
            .iter()
            .map(|query| query.route.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            routes,
            vec![
                "/graph/overview",
                "/graph/sample",
                "/graph/node-details/{node_id}",
                "/graph/community-members/{community_id}",
                "/library/community/{community_id}/recent-memories",
                "/library/community/{community_id}/subgraph",
                "/graph/augmentation/state",
                "/graph/augmentation/pagerank/plan",
                "/graph/orphans",
            ]
        );
    }
}
