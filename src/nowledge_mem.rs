use crate::search::{
    CompressedVectorSearchMode, SearchPredicateFieldPruningReport, SearchPredicatePushdownReport,
};
use crate::search_projection_evidence::{
    nowledge_search_projection_evidence_json, nowledge_search_projection_shadow_evidence_json,
};
use crate::store::{ScanPruningReport, ScanPruningStrategy};
use crate::{
    cypher, BackgroundMaintenanceOptions, BackgroundMaintenanceSummary, BackgroundWorkHint,
    BackgroundWorkPlan, Database, DatabaseConfig, KnowledgeRetrievalOutput,
    KnowledgeRetrievalRequest, LocalQosPolicy, LocalQosScheduler, LocalQosState, QueryOutput,
    ReadExecutionProfile, Result, SearchIndex, SearchProjectionDeltaReport,
    SearchProjectionFreshness, SearchProjectionGraphDeltaRequest, SearchProjectionProbeOptions,
    SkeinError, Value,
};
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemGraphMode {
    ShadowReadOnly,
    WritableCutover,
}

pub fn nowledge_mem_graph_config(mode: NowledgeMemGraphMode) -> DatabaseConfig {
    nowledge_mem_graph_config_with_search_mode(mode, CompressedVectorSearchMode::Disabled)
}

pub fn nowledge_mem_graph_config_with_search_mode(
    mode: NowledgeMemGraphMode,
    compressed_vector_search_mode: CompressedVectorSearchMode,
) -> DatabaseConfig {
    DatabaseConfig {
        read_only: matches!(mode, NowledgeMemGraphMode::ShadowReadOnly),
        compressed_vector_search_mode,
        ..DatabaseConfig::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemOpenOptions {
    pub graph_path: PathBuf,
    pub search_projection_path: Option<PathBuf>,
    pub mode: NowledgeMemGraphMode,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
}

impl NowledgeMemOpenOptions {
    pub fn graph_only(graph_path: impl Into<PathBuf>, mode: NowledgeMemGraphMode) -> Self {
        Self {
            graph_path: graph_path.into(),
            search_projection_path: None,
            mode,
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
        }
    }

    pub fn with_search_projection(
        graph_path: impl Into<PathBuf>,
        search_projection_path: impl Into<PathBuf>,
        mode: NowledgeMemGraphMode,
    ) -> Self {
        Self {
            graph_path: graph_path.into(),
            search_projection_path: Some(search_projection_path.into()),
            mode,
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
        }
    }

    pub fn with_compressed_vector_search_mode(mut self, mode: CompressedVectorSearchMode) -> Self {
        self.compressed_vector_search_mode = mode;
        self
    }

    pub fn sanitized_report(&self) -> NowledgeMemOpenReport {
        NowledgeMemOpenReport {
            protocol: NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL.to_string(),
            mode: self.mode,
            graph_configured: true,
            search_projection_configured: self.search_projection_path.is_some(),
            compressed_vector_search_mode: self.compressed_vector_search_mode,
            graph_opened: false,
            search_projection_opened: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemOpenReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub graph_configured: bool,
    pub search_projection_configured: bool,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub graph_opened: bool,
    pub search_projection_opened: bool,
}

impl NowledgeMemOpenReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "graph_configured": self.graph_configured,
            "search_projection_configured": self.search_projection_configured,
            "compressed_vector_search_mode": self.compressed_vector_search_mode.as_str(),
            "graph_opened": self.graph_opened,
            "search_projection_opened": self.search_projection_opened,
        })
    }
}

pub const NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL: &str = "skein-nowledge-mem-open-report";
pub const NOWLEDGE_MEM_READ_REPORT_PROTOCOL: &str = "skein-nowledge-mem-read-report";
pub const NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL: &str = "skein-nowledge-mem-query-report-v1";
pub const NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL: &str = "skein-nowledge-mem-retrieval-report";
pub const NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-mem-bounded-read-evidence-v1";
pub const DEFAULT_NOWLEDGE_MEM_READ_MAX_ROWS: usize = 512;
pub const DEFAULT_NOWLEDGE_MEM_READ_MAX_ESTIMATED_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_NOWLEDGE_MEM_SLOW_QUERY_THRESHOLD_MS: u64 = 1_000;
pub const DEFAULT_NOWLEDGE_MEM_SLOW_QUERY_RING_CAPACITY: usize = 64;

impl NowledgeMemGraphMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ShadowReadOnly => "shadow_read_only",
            Self::WritableCutover => "writable_cutover",
        }
    }
}

#[derive(Debug)]
pub struct NowledgeMemGraph {
    db: Database,
    mode: NowledgeMemGraphMode,
    slow_queries: VecDeque<NowledgeMemQueryReport>,
    slow_query_ring_capacity: usize,
    slow_query_dropped_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadOptions {
    pub max_rows: Option<usize>,
    pub max_estimated_payload_bytes: Option<usize>,
}

impl Default for NowledgeMemReadOptions {
    fn default() -> Self {
        Self {
            max_rows: Some(DEFAULT_NOWLEDGE_MEM_READ_MAX_ROWS),
            max_estimated_payload_bytes: Some(
                DEFAULT_NOWLEDGE_MEM_READ_MAX_ESTIMATED_PAYLOAD_BYTES,
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub row_count: usize,
    pub max_rows: Option<usize>,
    pub execution_row_cap: Option<usize>,
    pub estimated_payload_bytes: usize,
    pub max_estimated_payload_bytes: Option<usize>,
    pub row_budget_exceeded: bool,
    pub payload_budget_exceeded: bool,
    pub row_limit_enforced_before_output: bool,
    pub operator_row_cap_enabled: bool,
    pub blocking_operator_count: usize,
    pub blocking_operator_kinds: Vec<String>,
    pub streaming: bool,
}

impl NowledgeMemReadReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "row_count": self.row_count,
            "max_rows": self.max_rows,
            "execution_row_cap": self.execution_row_cap,
            "estimated_payload_bytes": self.estimated_payload_bytes,
            "max_estimated_payload_bytes": self.max_estimated_payload_bytes,
            "row_budget_exceeded": self.row_budget_exceeded,
            "payload_budget_exceeded": self.payload_budget_exceeded,
            "row_limit_enforced_before_output": self.row_limit_enforced_before_output,
            "operator_row_cap_enabled": self.operator_row_cap_enabled,
            "blocking_operator_count": self.blocking_operator_count,
            "blocking_operator_kinds": self.blocking_operator_kinds,
            "streaming": self.streaming,
        })
    }

    pub fn bounded_read_evidence_json(&self) -> serde_json::Value {
        nowledge_mem_bounded_read_evidence_json(self)
    }
}

pub fn nowledge_mem_bounded_read_evidence_json(
    report: &NowledgeMemReadReport,
) -> serde_json::Value {
    let blocker_codes = nowledge_mem_bounded_read_blocker_codes(report);
    let ready = blocker_codes.is_empty();

    serde_json::json!({
        "protocol": NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
        "present": true,
        "ready": ready,
        "mode": report.mode.as_str(),
        "max_rows": report.max_rows,
        "execution_row_cap": report.execution_row_cap,
        "row_limit_enforced_before_output": report.row_limit_enforced_before_output,
        "operator_row_cap_enabled": report.operator_row_cap_enabled,
        "streaming": report.streaming,
        "blocking_operator_count": report.blocking_operator_count,
        "blocking_operator_kinds": report.blocking_operator_kinds,
        "row_budget_exceeded": report.row_budget_exceeded,
        "payload_budget_exceeded": report.payload_budget_exceeded,
        "blocker_codes": blocker_codes,
    })
}

fn nowledge_mem_bounded_read_blocker_codes(report: &NowledgeMemReadReport) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    let expected_row_cap = match report.max_rows {
        Some(0) => {
            blockers.push("invalid_max_rows");
            None
        }
        Some(max_rows) => max_rows.checked_add(1),
        None => {
            blockers.push("missing_max_rows");
            None
        }
    };
    if report.mode != NowledgeMemGraphMode::ShadowReadOnly {
        blockers.push("not_shadow_read_only");
    }

    match (report.execution_row_cap, expected_row_cap) {
        (Some(execution_row_cap), Some(expected_row_cap))
            if execution_row_cap == expected_row_cap => {}
        (Some(_), _) => blockers.push("execution_row_cap_mismatch"),
        (None, _) => blockers.push("missing_execution_row_cap"),
    }
    if !report.row_limit_enforced_before_output {
        blockers.push("row_limit_not_enforced_before_output");
    }
    if !report.operator_row_cap_enabled {
        blockers.push("operator_row_cap_disabled");
    }
    if report.row_budget_exceeded {
        blockers.push("row_budget_exceeded");
    }
    if report.payload_budget_exceeded {
        blockers.push("payload_budget_exceeded");
    }
    blockers
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadOutput {
    pub output: QueryOutput,
    pub report: NowledgeMemReadReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryReportOptions {
    pub capture_physical_plan: bool,
    pub slow_query_threshold_ms: Option<u64>,
    pub record_slow_query: bool,
}

impl Default for NowledgeMemQueryReportOptions {
    fn default() -> Self {
        Self {
            capture_physical_plan: false,
            slow_query_threshold_ms: Some(DEFAULT_NOWLEDGE_MEM_SLOW_QUERY_THRESHOLD_MS),
            record_slow_query: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub statement_class: String,
    pub query_shape: String,
    pub fast_path_candidate: bool,
    pub capture_physical_plan: bool,
    pub physical_plan: Option<String>,
    pub selected_plan_fingerprint: Option<String>,
    pub elapsed_micros: u128,
    pub slow_query_threshold_ms: Option<u64>,
    pub slow_query: bool,
    pub row_count: usize,
    pub scan_pruning_reports: Vec<ScanPruningReport>,
}

impl NowledgeMemQueryReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "statement_class": self.statement_class,
            "query_shape": self.query_shape,
            "fast_path_candidate": self.fast_path_candidate,
            "capture_physical_plan": self.capture_physical_plan,
            "physical_plan": self.physical_plan,
            "selected_plan_fingerprint": self.selected_plan_fingerprint,
            "elapsed_micros": self.elapsed_micros,
            "slow_query_threshold_ms": self.slow_query_threshold_ms,
            "slow_query": self.slow_query,
            "row_count": self.row_count,
            "scan_pruning_report_count": self.scan_pruning_reports.len(),
            "scan_pruning_reports": self.scan_pruning_reports.iter().map(scan_pruning_report_json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryOutput {
    pub output: QueryOutput,
    pub report: NowledgeMemQueryReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSlowQueryRingReport {
    pub capacity: usize,
    pub len: usize,
    pub dropped_count: u64,
    pub reports: Vec<NowledgeMemQueryReport>,
}

impl NowledgeMemSlowQueryRingReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "capacity": self.capacity,
            "len": self.len,
            "dropped_count": self.dropped_count,
            "reports": self.reports.iter().map(NowledgeMemQueryReport::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemRetrievalReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub graph_commit_epoch: u64,
    pub projection_source_graph_commit_epoch: Option<u64>,
    pub projection_commit_lag: u64,
    pub projection_stale: bool,
    pub search_document_count: usize,
    pub search_filtered_document_count: usize,
    pub search_total_hits: usize,
    pub search_candidate_filtered_out_count: usize,
    pub search_metadata_filters: BTreeMap<String, String>,
    pub search_metadata_predicate_pushdown: SearchPredicatePushdownReport,
    pub candidate_count: usize,
    pub candidate_total_count: usize,
    pub evidence_count: usize,
    pub graph_seed_count: usize,
    pub graph_context_path_count: usize,
    pub search_backend: Option<String>,
    pub vector_backend: Option<String>,
    pub text_backend: Option<String>,
    pub search_fallback_reason_codes: Vec<String>,
    pub retriever_fallback_reason_codes: Vec<String>,
    pub knowledge_fallback_reason_codes: Vec<String>,
    pub truncation_reason_codes: Vec<String>,
    pub warning_count: usize,
    pub warnings: Vec<String>,
}

impl NowledgeMemRetrievalReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "compressed_vector_search_mode": self.compressed_vector_search_mode.as_str(),
            "graph_commit_epoch": self.graph_commit_epoch,
            "projection_source_graph_commit_epoch": self.projection_source_graph_commit_epoch,
            "projection_commit_lag": self.projection_commit_lag,
            "projection_stale": self.projection_stale,
            "search_document_count": self.search_document_count,
            "search_filtered_document_count": self.search_filtered_document_count,
            "search_total_hits": self.search_total_hits,
            "search_candidate_filtered_out_count": self.search_candidate_filtered_out_count,
            "search_metadata_filters": self.search_metadata_filters,
            "search_metadata_predicate_pushdown": search_predicate_pushdown_report_json(&self.search_metadata_predicate_pushdown),
            "candidate_count": self.candidate_count,
            "candidate_total_count": self.candidate_total_count,
            "evidence_count": self.evidence_count,
            "graph_seed_count": self.graph_seed_count,
            "graph_context_path_count": self.graph_context_path_count,
            "search_backend": self.search_backend,
            "vector_backend": self.vector_backend,
            "text_backend": self.text_backend,
            "search_fallback_reason_codes": self.search_fallback_reason_codes,
            "retriever_fallback_reason_codes": self.retriever_fallback_reason_codes,
            "knowledge_fallback_reason_codes": self.knowledge_fallback_reason_codes,
            "truncation_reason_codes": self.truncation_reason_codes,
            "warning_count": self.warning_count,
            "warnings": self.warnings,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemRetrievalOutput {
    pub output: KnowledgeRetrievalOutput,
    pub report: NowledgeMemRetrievalReport,
}

impl NowledgeMemGraph {
    pub fn open(path: impl AsRef<Path>, mode: NowledgeMemGraphMode) -> Result<Self> {
        let db = Database::open_with_config(path, nowledge_mem_graph_config(mode))?;
        Ok(Self::from_database(db, mode))
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self> {
        let mode = if config.read_only {
            NowledgeMemGraphMode::ShadowReadOnly
        } else {
            NowledgeMemGraphMode::WritableCutover
        };
        let db = Database::open_with_config(path, config)?;
        Ok(Self::from_database(db, mode))
    }

    pub fn from_database(db: Database, mode: NowledgeMemGraphMode) -> Self {
        Self {
            db,
            mode,
            slow_queries: VecDeque::new(),
            slow_query_ring_capacity: DEFAULT_NOWLEDGE_MEM_SLOW_QUERY_RING_CAPACITY,
            slow_query_dropped_count: 0,
        }
    }

    pub fn mode(&self) -> NowledgeMemGraphMode {
        self.mode
    }

    pub fn database(&self) -> &Database {
        &self.db
    }

    pub fn database_mut(&mut self) -> &mut Database {
        &mut self.db
    }

    pub fn into_database(self) -> Database {
        self.db
    }

    pub fn slow_query_ring_capacity(&self) -> usize {
        self.slow_query_ring_capacity
    }

    pub fn set_slow_query_ring_capacity(&mut self, capacity: usize) {
        self.slow_query_ring_capacity = capacity;
        self.trim_slow_query_ring();
    }

    pub fn slow_query_ring_report(&self) -> NowledgeMemSlowQueryRingReport {
        NowledgeMemSlowQueryRingReport {
            capacity: self.slow_query_ring_capacity,
            len: self.slow_queries.len(),
            dropped_count: self.slow_query_dropped_count,
            reports: self.slow_queries.iter().cloned().collect(),
        }
    }

    pub fn clear_slow_query_ring(&mut self) {
        self.slow_queries.clear();
        self.slow_query_dropped_count = 0;
    }

    pub fn query(&mut self, cypher: &str) -> Result<QueryOutput> {
        self.db.query(cypher)
    }

    pub fn query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.db.query_with_params(cypher, parameters)
    }

    pub fn query_with_report(&mut self, cypher: &str) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_report(
            cypher,
            &BTreeMap::new(),
            &NowledgeMemQueryReportOptions::default(),
        )
    }

    pub fn query_with_options_report(
        &mut self,
        cypher: &str,
        options: &NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_report(cypher, &BTreeMap::new(), options)
    }

    pub fn query_with_params_report(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        let statement = cypher::parse(cypher)?;
        let classification = classify_nowledge_mem_query(statement_body(&statement));
        let explain = if options.capture_physical_plan {
            Some(self.db.explain_query_with_params(cypher, parameters)?)
        } else {
            None
        };
        let start = Instant::now();
        let profiled = self.db.query_with_params_profile(cypher, parameters)?;
        let elapsed_micros = start.elapsed().as_micros();
        let scan_pruning_reports = profiled.execution_profile.scan_pruning_reports.clone();
        let output = profiled.output;
        let slow_query = options
            .slow_query_threshold_ms
            .is_some_and(|threshold| elapsed_micros >= u128::from(threshold) * 1_000);
        let report = NowledgeMemQueryReport {
            protocol: NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL.to_string(),
            mode: self.mode,
            statement_class: classification.statement_class.to_string(),
            query_shape: classification.query_shape.to_string(),
            fast_path_candidate: classification.fast_path_candidate,
            capture_physical_plan: options.capture_physical_plan,
            physical_plan: explain
                .as_ref()
                .map(|explain| explain.physical_plan.explain(0)),
            selected_plan_fingerprint: explain
                .as_ref()
                .map(|explain| explain.trace.selected_plan_fingerprint.clone()),
            elapsed_micros,
            slow_query_threshold_ms: options.slow_query_threshold_ms,
            slow_query,
            row_count: output.rows.len(),
            scan_pruning_reports,
        };
        if options.record_slow_query && report.slow_query {
            self.push_slow_query_report(report.clone());
        }
        Ok(NowledgeMemQueryOutput { output, report })
    }

    pub fn read_query(&mut self, cypher: &str) -> Result<NowledgeMemReadOutput> {
        self.read_query_with_params(cypher, &BTreeMap::new(), &NowledgeMemReadOptions::default())
    }

    pub fn read_query_with_options(
        &mut self,
        cypher: &str,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.read_query_with_params(cypher, &BTreeMap::new(), options)
    }

    pub fn read_query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        let bounded = self
            .db
            .begin_read_transaction()
            .query_with_params_bounded_profile(cypher, parameters, options.max_rows)?;
        let report = nowledge_mem_read_report(
            self.mode,
            &bounded.output,
            options,
            &bounded.execution_profile,
        );
        if report.row_budget_exceeded {
            return Err(SkeinError::Execution(format!(
                "nowledge mem read query returned {} rows, exceeding max_rows {}",
                report.row_count,
                report.max_rows.unwrap_or_default()
            )));
        }
        if report.payload_budget_exceeded {
            return Err(SkeinError::Execution(format!(
                "nowledge mem read query estimated {} payload bytes, exceeding max_estimated_payload_bytes {}",
                report.estimated_payload_bytes,
                report.max_estimated_payload_bytes.unwrap_or_default()
            )));
        }
        Ok(NowledgeMemReadOutput {
            output: bounded.output,
            report,
        })
    }
}

impl NowledgeMemGraph {
    fn push_slow_query_report(&mut self, report: NowledgeMemQueryReport) {
        if self.slow_query_ring_capacity == 0 {
            self.slow_query_dropped_count = self.slow_query_dropped_count.saturating_add(1);
            return;
        }
        while self.slow_queries.len() >= self.slow_query_ring_capacity {
            self.slow_queries.pop_front();
            self.slow_query_dropped_count = self.slow_query_dropped_count.saturating_add(1);
        }
        self.slow_queries.push_back(report);
    }

    fn trim_slow_query_ring(&mut self) {
        while self.slow_queries.len() > self.slow_query_ring_capacity {
            self.slow_queries.pop_front();
            self.slow_query_dropped_count = self.slow_query_dropped_count.saturating_add(1);
        }
    }
}

#[derive(Debug)]
pub struct NowledgeMemSearchProjection {
    index: SearchIndex,
}

impl NowledgeMemSearchProjection {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            index: SearchIndex::open(path)?,
        })
    }

    pub fn from_index(index: SearchIndex) -> Self {
        Self { index }
    }

    pub fn index(&self) -> &SearchIndex {
        &self.index
    }

    pub fn index_mut(&mut self) -> &mut SearchIndex {
        &mut self.index
    }

    pub fn into_index(self) -> SearchIndex {
        self.index
    }

    pub fn probe_json(&self, options: SearchProjectionProbeOptions) -> serde_json::Value {
        self.index.nowledge_search_projection_probe_json(options)
    }

    pub fn evidence_json(&self, options: SearchProjectionProbeOptions) -> serde_json::Value {
        nowledge_search_projection_evidence_json(&self.probe_json(options))
    }

    pub fn shadow_evidence_json(
        &self,
        primary_probe: &serde_json::Value,
        options: SearchProjectionProbeOptions,
    ) -> serde_json::Value {
        nowledge_search_projection_shadow_evidence_json(primary_probe, &self.probe_json(options))
    }

    pub fn freshness(&self) -> SearchProjectionFreshness {
        self.index.projection_freshness()
    }
}

#[derive(Debug)]
pub struct NowledgeMemEmbeddedStore {
    graph: NowledgeMemGraph,
    search_projection: Option<NowledgeMemSearchProjection>,
}

impl NowledgeMemEmbeddedStore {
    pub fn new(
        graph: NowledgeMemGraph,
        search_projection: Option<NowledgeMemSearchProjection>,
    ) -> Self {
        Self {
            graph,
            search_projection,
        }
    }

    pub fn open_with_options(
        options: NowledgeMemOpenOptions,
    ) -> Result<(Self, NowledgeMemOpenReport)> {
        let mut report = options.sanitized_report();
        let graph = NowledgeMemGraph::open_with_config(
            &options.graph_path,
            nowledge_mem_graph_config_with_search_mode(
                options.mode,
                options.compressed_vector_search_mode,
            ),
        )?;
        report.graph_opened = true;
        let search_projection = match options.search_projection_path.as_ref() {
            Some(path) => {
                let projection = NowledgeMemSearchProjection::open(path)?;
                report.search_projection_opened = true;
                Some(projection)
            }
            None => None,
        };
        Ok((Self::new(graph, search_projection), report))
    }

    pub fn graph(&self) -> &NowledgeMemGraph {
        &self.graph
    }

    pub fn graph_mut(&mut self) -> &mut NowledgeMemGraph {
        &mut self.graph
    }

    pub fn slow_query_ring_report(&self) -> NowledgeMemSlowQueryRingReport {
        self.graph.slow_query_ring_report()
    }

    pub fn clear_slow_query_ring(&mut self) {
        self.graph.clear_slow_query_ring();
    }

    pub fn set_slow_query_ring_capacity(&mut self, capacity: usize) {
        self.graph.set_slow_query_ring_capacity(capacity);
    }

    pub fn search_projection(&self) -> Option<&NowledgeMemSearchProjection> {
        self.search_projection.as_ref()
    }

    pub fn search_projection_mut(&mut self) -> Option<&mut NowledgeMemSearchProjection> {
        self.search_projection.as_mut()
    }

    pub fn build_search_projection_graph_delta_request_from_freshness(
        &self,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionGraphDeltaRequest>> {
        let search_projection = self.require_search_projection()?;
        self.graph
            .database()
            .build_search_projection_graph_delta_request_from_freshness(
                search_projection.index(),
                max_operations,
            )
    }

    pub fn search_projection_graph_delta_background_work_plan(
        &self,
        request: &SearchProjectionGraphDeltaRequest,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let search_projection = self.search_projection.as_ref()?;
        self.graph
            .database()
            .search_projection_graph_delta_freshness_background_work_plan(
                search_projection.index(),
                request,
                hint,
            )
    }

    pub fn apply_search_projection_graph_delta(
        &mut self,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        let Self {
            graph,
            search_projection,
        } = self;
        let search_projection = require_search_projection_mut(search_projection)?;
        graph
            .database()
            .apply_search_projection_graph_delta(search_projection.index_mut(), request)
    }

    pub fn apply_scheduled_background_search_projection_graph_delta(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        let Self {
            graph,
            search_projection,
        } = self;
        let search_projection = require_search_projection_mut(search_projection)?;
        graph
            .database()
            .apply_scheduled_background_search_projection_graph_delta(
                search_projection.index_mut(),
                scheduler,
                request,
            )
    }

    pub fn search_projection_probe_json(
        &self,
        options: SearchProjectionProbeOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.require_search_projection()?.probe_json(options))
    }

    pub fn search_projection_evidence_json(
        &self,
        options: SearchProjectionProbeOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.require_search_projection()?.evidence_json(options))
    }

    pub fn search_projection_shadow_evidence_json(
        &self,
        primary_probe: &serde_json::Value,
        options: SearchProjectionProbeOptions,
    ) -> Result<serde_json::Value> {
        Ok(self
            .require_search_projection()?
            .shadow_evidence_json(primary_probe, options))
    }

    pub fn retrieve_knowledge(
        &self,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        Ok(self.retrieve_knowledge_with_report(request)?.output)
    }

    pub fn retrieve_knowledge_with_report(
        &self,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<NowledgeMemRetrievalOutput> {
        let search_projection = self.require_search_projection()?;
        let output = self
            .graph
            .database()
            .retrieve_knowledge(search_projection.index(), request);
        let report = nowledge_mem_retrieval_report(
            self.graph.mode(),
            self.graph.database().config().compressed_vector_search_mode,
            &output,
        );
        Ok(NowledgeMemRetrievalOutput { output, report })
    }

    pub fn read_query(&mut self, cypher: &str) -> Result<NowledgeMemReadOutput> {
        self.graph.read_query(cypher)
    }

    pub fn read_query_with_options(
        &mut self,
        cypher: &str,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.graph.read_query_with_options(cypher, options)
    }

    pub fn read_query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.graph
            .read_query_with_params(cypher, parameters, options)
    }

    pub fn query_with_report(&mut self, cypher: &str) -> Result<NowledgeMemQueryOutput> {
        self.graph.query_with_report(cypher)
    }

    pub fn query_with_options_report(
        &mut self,
        cypher: &str,
        options: &NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.graph.query_with_options_report(cypher, options)
    }

    pub fn query_with_params_report(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.graph
            .query_with_params_report(cypher, parameters, options)
    }

    pub fn background_maintenance_summary(
        &self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> BackgroundMaintenanceSummary {
        self.graph.database().background_maintenance_summary(
            self.search_projection
                .as_ref()
                .map(NowledgeMemSearchProjection::index),
            policy,
            state,
            options,
        )
    }

    fn require_search_projection(&self) -> Result<&NowledgeMemSearchProjection> {
        self.search_projection
            .as_ref()
            .ok_or_else(missing_search_projection_error)
    }
}

fn missing_search_projection_error() -> SkeinError {
    SkeinError::Storage("nowledge mem search projection is not configured".to_string())
}

fn require_search_projection_mut(
    search_projection: &mut Option<NowledgeMemSearchProjection>,
) -> Result<&mut NowledgeMemSearchProjection> {
    search_projection
        .as_mut()
        .ok_or_else(missing_search_projection_error)
}

fn scan_pruning_report_json(report: &ScanPruningReport) -> serde_json::Value {
    serde_json::json!({
        "label_id": report.label_id.map(|label_id| label_id.0),
        "strategy": scan_pruning_strategy_json(&report.strategy),
        "pruned": report.pruned,
        "exact_empty": report.exact_empty,
        "candidate_count_before_filter": report.candidate_count_before_filter,
        "output_count": report.output_count,
        "filtered_out_count": report.filtered_out_count,
    })
}

fn scan_pruning_strategy_json(strategy: &ScanPruningStrategy) -> serde_json::Value {
    match strategy {
        ScanPruningStrategy::FullLabelScan => serde_json::json!({"kind": "full_label_scan"}),
        ScanPruningStrategy::Empty => serde_json::json!({"kind": "empty"}),
        ScanPruningStrategy::IdEq => serde_json::json!({"kind": "id_eq"}),
        ScanPruningStrategy::IdIn => serde_json::json!({"kind": "id_in"}),
        ScanPruningStrategy::IdRange => serde_json::json!({"kind": "id_range"}),
        ScanPruningStrategy::PropertyEq { property } => {
            serde_json::json!({"kind": "property_eq", "property": property})
        }
        ScanPruningStrategy::PropertyNotEq { property } => {
            serde_json::json!({"kind": "property_not_eq", "property": property})
        }
        ScanPruningStrategy::PropertyIn { property } => {
            serde_json::json!({"kind": "property_in", "property": property})
        }
        ScanPruningStrategy::PropertyRange { property } => {
            serde_json::json!({"kind": "property_range", "property": property})
        }
        ScanPruningStrategy::OrUnion => serde_json::json!({"kind": "or_union"}),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NowledgeMemQueryClassification {
    statement_class: &'static str,
    query_shape: &'static str,
    fast_path_candidate: bool,
}

fn classify_nowledge_mem_query(statement: &cypher::Statement) -> NowledgeMemQueryClassification {
    use cypher::Statement;
    match statement {
        Statement::MatchReturn(query) if is_simple_node_lookup(query) => {
            NowledgeMemQueryClassification {
                statement_class: "read",
                query_shape: "simple_node_lookup",
                fast_path_candidate: true,
            }
        }
        Statement::MatchReturn(query)
            if query.expand.is_some()
                && query.post_match_expand.is_none()
                && query.optional_expand.is_none()
                && query.optional_with.is_none()
                && query.collect_with.is_none()
                && query.distinct_with.is_none()
                && query.with_projection.is_none()
                && query.aggregate_with.is_none()
                && query.aggregate_with_filter.is_none()
                && query.post_with_match.is_none() =>
        {
            NowledgeMemQueryClassification {
                statement_class: "read",
                query_shape: "simple_one_hop_expand",
                fast_path_candidate: true,
            }
        }
        Statement::MatchReturn(_)
        | Statement::ShortestPathReturn(_)
        | Statement::MatchNodesReturn(_)
        | Statement::MatchOptionalRelationshipCountSum(_)
        | Statement::MatchThreadRepairStats(_)
        | Statement::GraphAlgorithm(_) => NowledgeMemQueryClassification {
            statement_class: "read",
            query_shape: "general_read",
            fast_path_candidate: false,
        },
        Statement::CreateNode(_)
        | Statement::CreateRelationship(_)
        | Statement::MergeNode(_)
        | Statement::MergeRelationship(_)
        | Statement::MatchSet(_)
        | Statement::MatchSetReturn(_)
        | Statement::MatchDelete(_)
        | Statement::MatchCreateRelationship(_)
        | Statement::MatchMergeRelationship(_)
        | Statement::MatchExpandMergeRelationship(_)
        | Statement::MatchExpandMatchMergeRelationship(_) => NowledgeMemQueryClassification {
            statement_class: "write",
            query_shape: "mutation",
            fast_path_candidate: false,
        },
        Statement::SetSystemVariable(_) => NowledgeMemQueryClassification {
            statement_class: "session",
            query_shape: "set_system_variable",
            fast_path_candidate: false,
        },
        Statement::BeginTransaction | Statement::Commit | Statement::Rollback => {
            NowledgeMemQueryClassification {
                statement_class: "transaction",
                query_shape: "transaction_control",
                fast_path_candidate: false,
            }
        }
        Statement::Checkpoint => NowledgeMemQueryClassification {
            statement_class: "maintenance",
            query_shape: "checkpoint",
            fast_path_candidate: false,
        },
        Statement::CypherQuery(query) => classify_nowledge_mem_query(&query.statement),
        _ => NowledgeMemQueryClassification {
            statement_class: "schema",
            query_shape: "schema_or_catalog",
            fast_path_candidate: false,
        },
    }
}

fn is_simple_node_lookup(query: &cypher::MatchReturn) -> bool {
    query.expand.is_none()
        && query.post_match_expand.is_none()
        && query.optional_expand.is_none()
        && query.optional_with.is_none()
        && query.collect_with.is_none()
        && query.distinct_with.is_none()
        && query.with_projection.is_none()
        && query.with_order_by.is_empty()
        && query.with_offset.is_none()
        && query.with_limit.is_none()
        && query.aggregate_with.is_none()
        && query.aggregate_with_filter.is_none()
        && query.post_with_match.is_none()
        && query.predicate.is_none()
        && !query.distinct
        && query.order_by.is_empty()
        && query.offset.is_none()
        && query.limit.is_none()
        && !query.properties.is_empty()
}

fn statement_body(statement: &cypher::Statement) -> &cypher::Statement {
    match statement {
        cypher::Statement::CypherQuery(query) => &query.statement,
        _ => statement,
    }
}

fn nowledge_mem_retrieval_report(
    mode: NowledgeMemGraphMode,
    compressed_vector_search_mode: CompressedVectorSearchMode,
    output: &KnowledgeRetrievalOutput,
) -> NowledgeMemRetrievalReport {
    let vector_backend = output
        .search
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "vector")
        .map(|retriever| retriever.backend.clone());
    let text_backend = output
        .search
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .map(|retriever| retriever.backend.clone());
    let search_backend = vector_backend
        .clone()
        .or_else(|| text_backend.clone())
        .or_else(|| {
            output
                .search
                .retrievers
                .first()
                .map(|retriever| retriever.backend.clone())
        });
    let knowledge_fallback_reason_codes = output
        .diagnostics
        .graph_context_fallback_reason_codes
        .iter()
        .map(|code| code.as_str().to_string())
        .collect::<Vec<_>>();
    let retriever_fallback_reason_codes = output
        .retrievers
        .iter()
        .flat_map(|retriever| retriever.fallback_reason_codes.iter())
        .map(|code| code.as_str().to_string())
        .collect::<Vec<_>>();
    let truncation_reason_codes = output
        .diagnostics
        .search_truncation_reason_codes
        .iter()
        .map(|code| code.as_str().to_string())
        .chain(
            output
                .diagnostics
                .graph_seed_truncation_reason_codes
                .iter()
                .map(|code| code.as_str().to_string()),
        )
        .chain(
            output
                .diagnostics
                .graph_context_truncation_reason_codes
                .iter()
                .map(|code| code.as_str().to_string()),
        )
        .chain(
            output
                .diagnostics
                .candidate_truncation_reason_codes
                .iter()
                .map(|code| code.as_str().to_string()),
        )
        .collect::<Vec<_>>();
    NowledgeMemRetrievalReport {
        protocol: NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL.to_string(),
        mode,
        compressed_vector_search_mode,
        graph_commit_epoch: output.graph_commit_epoch,
        projection_source_graph_commit_epoch: output.projection_freshness.source_graph_commit_epoch,
        projection_commit_lag: output.diagnostics.projection_commit_lag,
        projection_stale: output.diagnostics.projection_stale,
        search_document_count: output.search.document_count,
        search_filtered_document_count: output.search.filtered_document_count,
        search_total_hits: output.search.total_hits,
        search_candidate_filtered_out_count: output.diagnostics.search_candidate_filtered_out_count,
        search_metadata_filters: output
            .diagnostics
            .search_candidate_set
            .metadata_filters
            .clone(),
        search_metadata_predicate_pushdown: output
            .diagnostics
            .search_candidate_set
            .metadata_predicate_pushdown
            .clone(),
        candidate_count: output.candidates.len(),
        candidate_total_count: output.diagnostics.candidate_total_count,
        evidence_count: output.evidence.len(),
        graph_seed_count: output.graph_seeds.len(),
        graph_context_path_count: output.graph_context_paths.len(),
        search_backend,
        vector_backend,
        text_backend,
        search_fallback_reason_codes: output
            .diagnostics
            .search_fallback_reason_codes
            .iter()
            .map(|code| code.as_str().to_string())
            .collect(),
        retriever_fallback_reason_codes,
        knowledge_fallback_reason_codes,
        truncation_reason_codes,
        warning_count: output.diagnostics.warnings.len(),
        warnings: output.diagnostics.warnings.clone(),
    }
}

fn search_predicate_pushdown_report_json(
    report: &SearchPredicatePushdownReport,
) -> serde_json::Value {
    serde_json::json!({
        "input_predicate_count": report.input_predicate_count,
        "pushed_predicate_count": report.pushed_predicate_count,
        "residual_predicate_count": report.residual_predicate_count,
        "unsatisfiable": report.unsatisfiable,
        "parse_error": report.parse_error,
        "segment_count": report.segment_count,
        "pruned_segment_count": report.pruned_segment_count,
        "scanned_segment_count": report.scanned_segment_count,
        "persisted_segment_descriptor_used": report.persisted_segment_descriptor_used,
        "field_summaries": report.field_summaries.iter().map(search_predicate_field_pruning_report_json).collect::<Vec<_>>(),
    })
}

fn search_predicate_field_pruning_report_json(
    report: &SearchPredicateFieldPruningReport,
) -> serde_json::Value {
    serde_json::json!({
        "field": &report.field,
        "operation_kinds": &report.operation_kinds,
        "segment_count": report.segment_count,
        "pruned_segment_count": report.pruned_segment_count,
        "scanned_segment_count": report.scanned_segment_count,
        "numeric_range_summary_used": report.numeric_range_summary_used,
        "value_summary_used": report.value_summary_used,
    })
}

fn nowledge_mem_read_report(
    mode: NowledgeMemGraphMode,
    output: &QueryOutput,
    options: &NowledgeMemReadOptions,
    execution_profile: &ReadExecutionProfile,
) -> NowledgeMemReadReport {
    let estimated_payload_bytes = estimate_query_output_payload_bytes(output);
    NowledgeMemReadReport {
        protocol: NOWLEDGE_MEM_READ_REPORT_PROTOCOL.to_string(),
        mode,
        row_count: output.rows.len(),
        max_rows: options.max_rows,
        execution_row_cap: execution_profile.detection_row_cap,
        estimated_payload_bytes,
        max_estimated_payload_bytes: options.max_estimated_payload_bytes,
        row_budget_exceeded: options
            .max_rows
            .is_some_and(|max_rows| output.rows.len() > max_rows),
        payload_budget_exceeded: options
            .max_estimated_payload_bytes
            .is_some_and(|max_bytes| estimated_payload_bytes > max_bytes),
        row_limit_enforced_before_output: execution_profile.row_limit_enforced_before_output,
        operator_row_cap_enabled: execution_profile.operator_row_cap_enabled,
        blocking_operator_count: execution_profile.blocking_operator_count(),
        blocking_operator_kinds: execution_profile.blocking_operator_kinds.clone(),
        streaming: false,
    }
}

fn estimate_query_output_payload_bytes(output: &QueryOutput) -> usize {
    output
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|(key, value)| key.len() + estimate_value_payload_bytes(value))
                .sum::<usize>()
        })
        .sum()
}

fn estimate_value_payload_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) => std::mem::size_of::<i64>(),
        Value::String(value) => value.len(),
        Value::List(values) => values.iter().map(estimate_value_payload_bytes).sum(),
        Value::Map(values) => values
            .iter()
            .map(|(key, value)| key.len() + estimate_value_payload_bytes(value))
            .sum(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_mem_bounded_read_evidence_json, nowledge_mem_graph_config,
        nowledge_mem_graph_config_with_search_mode, NowledgeMemEmbeddedStore, NowledgeMemGraph,
        NowledgeMemGraphMode, NowledgeMemOpenOptions, NowledgeMemQueryReportOptions,
        NowledgeMemReadOptions, NowledgeMemReadReport, NowledgeMemSearchProjection,
        NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL, NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL,
        NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL, NOWLEDGE_MEM_READ_REPORT_PROTOCOL,
        NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL,
    };
    use crate::nowledge_contract::{
        SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL,
        SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL,
    };
    use crate::search::CompressedVectorSearchMode;
    use crate::search::SearchFusionWeights;
    use crate::{
        BackgroundMaintenanceKind, BackgroundMaintenanceOptions, BackgroundWorkHint, Database,
        DatabaseConfig, KnowledgeCandidateScoringPolicy, KnowledgeRetrievalRequest, LocalQosPolicy,
        LocalQosScheduler, LocalQosState, SearchEmbeddingManifest, SearchIndex, SearchMode,
        SearchProjectionDelta, SearchProjectionKind, SearchProjectionProbeOptions,
        SearchProjectionRow, WorkClass,
    };
    use std::collections::BTreeMap;

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
    fn graph_query_with_report_classifies_fast_path_without_explain_by_default() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-1', title: 'Query report'})")
            .unwrap();

        let output = graph
            .query_with_report("MATCH (m:Memory {id: 'mem-1'}) RETURN m.title AS title")
            .unwrap();

        assert_eq!(output.output.rows.len(), 1);
        assert_eq!(output.report.protocol, NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL);
        assert_eq!(output.report.mode, NowledgeMemGraphMode::WritableCutover);
        assert_eq!(output.report.statement_class, "read");
        assert_eq!(output.report.query_shape, "simple_node_lookup");
        assert!(output.report.fast_path_candidate);
        assert!(!output.report.capture_physical_plan);
        assert!(output.report.physical_plan.is_none());
        assert!(output.report.selected_plan_fingerprint.is_none());
        assert_eq!(output.report.row_count, 1);
        assert_eq!(output.report.slow_query_threshold_ms, Some(1_000));
        assert!(!output.report.slow_query);
        assert_eq!(
            output.report.json()["protocol"],
            NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL
        );
        assert_eq!(output.report.json()["query_shape"], "simple_node_lookup");
    }

    #[test]
    fn graph_query_with_report_can_capture_physical_plan_on_request() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-1', title: 'Explain report'})")
            .unwrap();

        let output = graph
            .query_with_options_report(
                "MATCH (m:Memory {id: 'mem-1'}) RETURN m.title AS title",
                &NowledgeMemQueryReportOptions {
                    capture_physical_plan: true,
                    slow_query_threshold_ms: None,
                    record_slow_query: true,
                },
            )
            .unwrap();

        assert!(output.report.capture_physical_plan);
        assert!(output
            .report
            .physical_plan
            .as_deref()
            .unwrap()
            .contains("ProjectExec"));
        assert!(output
            .report
            .selected_plan_fingerprint
            .as_deref()
            .unwrap()
            .contains("Memory"));
        assert_eq!(output.report.slow_query_threshold_ms, None);
        assert!(!output.report.slow_query);
    }

    #[test]
    fn graph_query_with_report_exposes_storage_scan_pruning() {
        let db = Database::new();
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
    fn graph_query_with_report_marks_slow_query_from_threshold() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);

        let output = graph
            .query_with_options_report(
                "MATCH (m:Memory) RETURN m.id AS id",
                &NowledgeMemQueryReportOptions {
                    capture_physical_plan: false,
                    slow_query_threshold_ms: Some(0),
                    record_slow_query: true,
                },
            )
            .unwrap();

        assert!(output.report.slow_query);
        assert_eq!(output.report.query_shape, "general_read");
        assert_eq!(output.report.statement_class, "read");
    }

    #[test]
    fn graph_slow_query_ring_records_successful_slow_reports_without_query_text() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);

        graph
            .query_with_options_report(
                "MATCH (m:Memory {id: 'secret-id'}) RETURN m.id AS id",
                &NowledgeMemQueryReportOptions {
                    capture_physical_plan: false,
                    slow_query_threshold_ms: Some(0),
                    record_slow_query: true,
                },
            )
            .unwrap();

        let ring = graph.slow_query_ring_report();
        assert_eq!(ring.capacity, 64);
        assert_eq!(ring.len, 1);
        assert_eq!(ring.dropped_count, 0);
        assert_eq!(ring.reports[0].query_shape, "simple_node_lookup");
        assert!(ring.reports[0].slow_query);
        let json = ring.json().to_string();
        assert!(!json.contains("secret-id"));
        assert!(!json.contains("MATCH"));
    }

    #[test]
    fn graph_slow_query_ring_preserves_scan_pruning_report_without_query_text() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'secret-prune-id', kind: 'note', title: 'Slow'})")
            .unwrap();

        graph
            .query_with_options_report(
                "MATCH (m:Memory) WHERE m.kind = 'note' RETURN m.title AS title",
                &NowledgeMemQueryReportOptions {
                    capture_physical_plan: false,
                    slow_query_threshold_ms: Some(0),
                    record_slow_query: true,
                },
            )
            .unwrap();

        let ring = graph.slow_query_ring_report();
        assert_eq!(ring.len, 1);
        assert_eq!(ring.reports[0].scan_pruning_reports.len(), 1);
        assert!(ring.reports[0].scan_pruning_reports[0].pruned);
        let json = ring.json().to_string();
        assert!(json.contains("property_eq"));
        assert!(json.contains("kind"));
        assert!(!json.contains("secret-prune-id"));
        assert!(!json.contains("MATCH"));
    }

    #[test]
    fn graph_slow_query_ring_enforces_bounded_capacity() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph.set_slow_query_ring_capacity(1);

        for id in ["first", "second"] {
            graph
                .query_with_options_report(
                    &format!("MATCH (m:Memory {{id: '{id}'}}) RETURN m.id AS id"),
                    &NowledgeMemQueryReportOptions {
                        capture_physical_plan: false,
                        slow_query_threshold_ms: Some(0),
                        record_slow_query: true,
                    },
                )
                .unwrap();
        }

        let ring = graph.slow_query_ring_report();
        assert_eq!(ring.capacity, 1);
        assert_eq!(ring.len, 1);
        assert_eq!(ring.dropped_count, 1);

        graph.set_slow_query_ring_capacity(0);
        assert_eq!(graph.slow_query_ring_report().len, 0);
        graph
            .query_with_options_report(
                "MATCH (m:Memory {id: 'third'}) RETURN m.id AS id",
                &NowledgeMemQueryReportOptions {
                    capture_physical_plan: false,
                    slow_query_threshold_ms: Some(0),
                    record_slow_query: true,
                },
            )
            .unwrap();
        let ring = graph.slow_query_ring_report();
        assert_eq!(ring.capacity, 0);
        assert_eq!(ring.len, 0);
        assert_eq!(ring.dropped_count, 3);
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
                    max_estimated_payload_bytes: Some(128),
                },
            )
            .unwrap();

        assert_eq!(read.output.rows.len(), 1);
        assert_eq!(read.report.protocol, NOWLEDGE_MEM_READ_REPORT_PROTOCOL);
        assert_eq!(read.report.mode, NowledgeMemGraphMode::ShadowReadOnly);
        assert_eq!(read.report.row_count, 1);
        assert_eq!(read.report.max_rows, Some(4));
        assert_eq!(read.report.execution_row_cap, Some(5));
        assert!(read.report.estimated_payload_bytes <= 128);
        assert!(!read.report.row_budget_exceeded);
        assert!(!read.report.payload_budget_exceeded);
        assert!(read.report.row_limit_enforced_before_output);
        assert!(read.report.operator_row_cap_enabled);
        assert_eq!(read.report.blocking_operator_count, 0);
        assert!(read.report.blocking_operator_kinds.is_empty());
        assert!(!read.report.streaming);
        assert_eq!(read.report.json()["execution_row_cap"], 5);
        assert_eq!(read.report.json()["row_limit_enforced_before_output"], true);
        assert_eq!(read.report.json()["operator_row_cap_enabled"], true);
        assert_eq!(read.report.json()["blocking_operator_count"], 0);
        assert_eq!(read.report.json()["streaming"], false);
        assert_eq!(
            read.report.bounded_read_evidence_json()["protocol"],
            NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL
        );
        assert_eq!(
            read.report.bounded_read_evidence_json()["mode"],
            "shadow_read_only"
        );
        assert_eq!(read.report.bounded_read_evidence_json()["ready"], true);
    }

    #[test]
    fn bounded_read_evidence_fails_closed_for_missing_row_cap() {
        let report = NowledgeMemReadReport {
            protocol: NOWLEDGE_MEM_READ_REPORT_PROTOCOL.to_string(),
            mode: NowledgeMemGraphMode::ShadowReadOnly,
            row_count: 2,
            max_rows: Some(512),
            execution_row_cap: None,
            estimated_payload_bytes: 128,
            max_estimated_payload_bytes: Some(4 * 1024 * 1024),
            row_budget_exceeded: false,
            payload_budget_exceeded: false,
            row_limit_enforced_before_output: false,
            operator_row_cap_enabled: false,
            blocking_operator_count: 1,
            blocking_operator_kinds: vec!["Sort".to_string()],
            streaming: false,
        };

        let evidence = nowledge_mem_bounded_read_evidence_json(&report);

        assert_eq!(
            evidence["protocol"],
            NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL
        );
        assert_eq!(evidence["present"], true);
        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["max_rows"], 512);
        assert_eq!(evidence["execution_row_cap"], serde_json::Value::Null);
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!([
                "missing_execution_row_cap",
                "row_limit_not_enforced_before_output",
                "operator_row_cap_disabled"
            ])
        );
    }

    #[test]
    fn bounded_read_evidence_requires_shadow_read_only_mode() {
        let report = NowledgeMemReadReport {
            protocol: NOWLEDGE_MEM_READ_REPORT_PROTOCOL.to_string(),
            mode: NowledgeMemGraphMode::WritableCutover,
            row_count: 2,
            max_rows: Some(512),
            execution_row_cap: Some(513),
            estimated_payload_bytes: 128,
            max_estimated_payload_bytes: Some(4 * 1024 * 1024),
            row_budget_exceeded: false,
            payload_budget_exceeded: false,
            row_limit_enforced_before_output: true,
            operator_row_cap_enabled: true,
            blocking_operator_count: 0,
            blocking_operator_kinds: Vec::new(),
            streaming: false,
        };

        let evidence = nowledge_mem_bounded_read_evidence_json(&report);

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["mode"], "writable_cutover");
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!(["not_shadow_read_only"])
        );
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
        assert_eq!(read.report.blocking_operator_kinds, vec!["SortExec"]);
        assert_eq!(read.report.blocking_operator_count, 1);
        assert_eq!(read.report.json()["blocking_operator_kinds"][0], "SortExec");
    }

    #[test]
    fn embedded_store_read_query_does_not_require_search_projection() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-store-read', title: 'Store read'})")
            .unwrap();
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);

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

        assert_eq!(probe["protocol"], "skein-nowledge-search-projection-probe");
    }

    #[test]
    fn embedded_store_exposes_search_projection_replacement_evidence() {
        let mut index = SearchIndex::in_memory();
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
            SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL
        );
        #[cfg(feature = "turbovec")]
        assert_eq!(evidence["ready"], true);
        #[cfg(not(feature = "turbovec"))]
        {
            assert_eq!(evidence["ready"], false);
            assert_eq!(
                evidence["compressed_vector_projection_ready"],
                serde_json::json!(false)
            );
            assert!(evidence["blocker_codes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|code| code == "compressed_vector_projection_not_ready"));
        }
        assert_eq!(evidence["covered_table_count"], 6);
        assert_eq!(evidence["required_table_count"], 6);
        assert_eq!(evidence["source_chunk_ready"], true);
        assert_eq!(evidence["incremental_update_ready"], true);
        #[cfg(feature = "turbovec")]
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn embedded_store_exposes_search_projection_shadow_evidence() {
        let mut index = SearchIndex::in_memory();
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
            SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL
        );
        #[cfg(feature = "turbovec")]
        {
            assert_eq!(evidence["ready"], true);
            assert_eq!(evidence["primary_ready"], true);
            assert_eq!(evidence["shadow_ready"], true);
        }
        #[cfg(not(feature = "turbovec"))]
        {
            assert_eq!(evidence["ready"], false);
            assert_eq!(evidence["primary_ready"], false);
            assert_eq!(evidence["shadow_ready"], false);
            assert!(!evidence["blocker_codes"].as_array().unwrap().is_empty());
        }
        assert_eq!(evidence["document_count_parity"], true);
        assert_eq!(evidence["table_parity"]["ready"], true);
        assert_eq!(evidence["embedding_identity_parity"], true);
        assert_eq!(evidence["incremental_watermark_parity"], true);
        #[cfg(feature = "turbovec")]
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
    fn open_options_report_exposes_advanced_compressed_vector_search_mode() {
        let options = NowledgeMemOpenOptions::with_search_projection(
            "redacted_graph_path",
            "redacted_search_path",
            NowledgeMemGraphMode::ShadowReadOnly,
        )
        .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred);

        let report = options.sanitized_report().json();

        assert_eq!(report["compressed_vector_search_mode"], "preferred");
        assert!(!report.to_string().contains("redacted_graph_path"));
        assert!(!report.to_string().contains("redacted_search_path"));
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
            .search_projection_graph_delta_background_work_plan(
                &request,
                BackgroundWorkHint::default(),
            )
            .expect("expected background work plan");

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
    fn embedded_store_background_delta_uses_scheduler_qos() {
        let db = Database::new();
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
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(0),
            ..LocalQosPolicy::default()
        });

        let error = store
            .apply_scheduled_background_search_projection_graph_delta(&mut scheduler, request)
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
    fn embedded_store_retrieves_knowledge_through_search_projection() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-search', title: 'Facade retrieval', content: 'Skein replaces LanceDB retrieval'})")
            .unwrap();
        graph
            .query("CREATE (:Entity {id: 'entity-skein', name: 'Skein'})")
            .unwrap();
        graph
            .query("MATCH (m:Memory {id: 'mem-search'}), (e:Entity {id: 'entity-skein'}) CREATE (m)-[:MENTIONS]->(e)")
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
    fn embedded_store_retrieval_report_exposes_search_filter_pushdown() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-fact', title: 'Fact retrieval', content: 'filter me', unit_type: 'fact'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'mem-task', title: 'Task retrieval', content: 'filter me', unit_type: 'task'})")
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
                query_text: "filter".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("unit_type".to_string(), "fact".to_string())]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 0,
                graph_context_limit: 0,
                graph_context_max_hops: 0,
            })
            .unwrap();
        let report = retrieval.report;
        let report_json = report.json();

        assert_eq!(report.search_metadata_filters["unit_type"], "fact");
        assert_eq!(report.search_filtered_document_count, 1);
        assert_eq!(report.search_candidate_filtered_out_count, 1);
        assert_eq!(
            report
                .search_metadata_predicate_pushdown
                .input_predicate_count,
            1
        );
        assert_eq!(
            report
                .search_metadata_predicate_pushdown
                .pushed_predicate_count,
            1
        );
        assert_eq!(report_json["search_metadata_filters"]["unit_type"], "fact");
        assert_eq!(
            report_json["search_metadata_predicate_pushdown"]["pushed_predicate_count"],
            1
        );
        assert_eq!(
            report_json["search_metadata_predicate_pushdown"]["field_summaries"][0]["field"],
            "unit_type"
        );
        assert_eq!(
            report_json["search_metadata_predicate_pushdown"]["field_summaries"][0]
                ["operation_kinds"],
            serde_json::json!(["eq"])
        );
        assert_eq!(
            report_json["search_metadata_predicate_pushdown"]["field_summaries"][0]
                ["value_summary_used"],
            true
        );
        assert_eq!(report_json["search_candidate_filtered_out_count"], 1);
    }

    #[test]
    #[cfg(feature = "turbovec")]
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
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 8,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "mem-vector".to_string(),
                        title: "Vector facade".to_string(),
                        body: "Compressed vector retrieval".to_string(),
                        embedding: Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                        source_id: None,
                        metadata: BTreeMap::new(),
                    }],
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(1),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let options = NowledgeMemOpenOptions::with_search_projection(
            graph_path,
            search_path,
            NowledgeMemGraphMode::ShadowReadOnly,
        )
        .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred);

        let (store, report) = NowledgeMemEmbeddedStore::open_with_options(options).unwrap();
        let retrieval = store
            .retrieve_knowledge_with_report(&KnowledgeRetrievalRequest {
                query_text: String::new(),
                query_embedding: Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                mode: SearchMode::Vector,
                limit: 10,
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
        assert_eq!(output.search.hits[0].id, "memory:mem-vector");
        assert_eq!(output.search.retrievers[0].backend, "turbovec_projection");
        assert_eq!(
            retrieval.report.compressed_vector_search_mode,
            CompressedVectorSearchMode::Preferred
        );
        assert_eq!(
            retrieval.report.vector_backend,
            Some("turbovec_projection".to_string())
        );
        assert_eq!(
            retrieval.report.json()["vector_backend"],
            "turbovec_projection"
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

        let summary = store.background_maintenance_summary(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            BackgroundMaintenanceOptions {
                include_schema_maintenance: false,
                include_property_index_projection: false,
                include_search_projection_rebuild: false,
                include_search_projection_metadata_repair: false,
                include_graph_lightning_bootstrap_export: false,
                include_external_content_artifact_jobs: false,
                ..BackgroundMaintenanceOptions::default()
            },
        );

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
    }

    fn nowledge_projection_evidence_rows() -> Vec<SearchProjectionRow> {
        vec![
            nowledge_projection_evidence_row(SearchProjectionKind::Memory, "mem_1", true),
            nowledge_projection_evidence_row(SearchProjectionKind::Message, "msg_1", false),
            nowledge_projection_evidence_row(SearchProjectionKind::Community, "community_1", true),
            nowledge_projection_evidence_row(SearchProjectionKind::Entity, "entity_1", true),
            nowledge_projection_evidence_row(SearchProjectionKind::Source, "source_1", true),
            nowledge_projection_evidence_row(SearchProjectionKind::SourceChunk, "chunk_1", true),
        ]
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
            metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
        }
    }

    fn unique_nowledge_mem_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_nowledge_mem_{name}_{}_{}",
            std::process::id(),
            nanos
        ))
    }
}
