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

//! Virtual `system`, `information_schema`, and `pg_catalog` query execution.
//!
//! This crate owns the bounded, storage-backed system-catalog query engine.
//! The embedded facade supplies a snapshot of its live state and retains all
//! database lifecycle and transaction coordination.

use hawdb_core::{
    Catalog, ConstraintKind, ConstraintSubject, GraphStatistics, HawDBError, IndexKind,
    IndexStatisticsSample, LabelId, PropertyType, RelTypeId, Result, RuntimeCapabilities,
    SchemaObjectState, TableKind, Value,
};
use hawdb_executor::{binding::map_payload_bytes, QueryOutput, Row, VectorExecutionReport};
use hawdb_plan_cache::PlanCacheStats;
use hawdb_query::QueryIdentity;
use hawdb_sql::{Expr, ExprKind};
use hawdb_sql::{
    SelectProjection, SelectStatement, SqlBound, SqlColumnRef, SqlComparisonOp, SqlOrderDirection,
    SqlOrderItem, SqlPredicate, SqlStatement, SqlValue,
};
use hawdb_storage::{
    AppendOrderMode, AppendState, AppendStorageResidencyReport, ProjectedGraphStatus,
    RelationalColumnDefault, RelationalIndexMode, RelationalScalarType, RelationalState,
    RelationalTableSchema, RelationalValue, SearchProjectionChangefeedStatus, StorageResidencyMode,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

#[doc(hidden)]
pub const DEFAULT_SLOW_QUERY_LOG_CAPACITY: usize = 256;
#[doc(hidden)]
pub const DEFAULT_SLOW_QUERY_LOG_THRESHOLD_MICROS: u128 = 300_000;
#[doc(hidden)]
pub const DEFAULT_STATEMENT_SUMMARY_CAPACITY: usize = 256;
pub const SLOW_QUERY_LOG_EVENT_PROTOCOL: &str = "hawdb-slow-query-log-event-v1";
const MAX_SLOW_QUERY_TEXT_BYTES: usize = 4096;
const MAX_STATEMENT_TEXT_BYTES: usize = 4096;
const MAX_STATEMENT_ERROR_BYTES: usize = 1024;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SlowQueryLogExportOptions {
    pub include_query_text: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlowQueryLogRecordSummary {
    pub sequence: u64,
    pub query_language: String,
    pub statement_kind: String,
    pub query_digest: String,
    pub started_unix_micros: i64,
    pub elapsed_micros: i64,
    pub row_count: i64,
    pub success: bool,
    pub slow_log_candidate: bool,
    pub access_control_policy_epoch: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct SlowQueryRecord {
    pub sequence: u64,
    pub query_language: String,
    pub statement_kind: String,
    pub query_digest: String,
    pub query_text_hash: String,
    pub query_text: String,
    pub started_unix_micros: i64,
    pub elapsed_micros: i64,
    pub row_count: i64,
    pub success: bool,
    pub error: Option<String>,
    pub slow_log_candidate: bool,
    pub access_control_policy_epoch: Option<u64>,
    pub vector_execution_reports: Vec<VectorExecutionReport>,
}

#[doc(hidden)]
pub struct SlowQueryCompletion<'a> {
    pub query_language: &'a str,
    pub statement_kind: &'a str,
    pub query_text: &'a str,
    pub query_identity: &'a QueryIdentity,
    pub elapsed_micros: u128,
    pub row_count: usize,
    pub success: bool,
    pub error: Option<String>,
    pub slow_log_candidate: bool,
    pub access_control_policy_epoch: Option<u64>,
    pub vector_execution_reports: Vec<VectorExecutionReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct SlowQueryLog {
    capacity: usize,
    next_sequence: u64,
    records: VecDeque<SlowQueryRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct StatementExecution {
    pub query_language: String,
    pub query_text: String,
    pub statement_kind: String,
    pub query_digest: String,
    pub query_text_hash: String,
    pub elapsed_micros: i64,
    pub row_count: i64,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct StatementSummaryRecord {
    pub digest: String,
    pub query_language: String,
    pub query_text: String,
    pub sample_query_text_hash: String,
    pub statement_kind: String,
    pub execution_count: i64,
    pub success_count: i64,
    pub error_count: i64,
    pub total_elapsed_micros: i64,
    pub max_elapsed_micros: i64,
    pub total_row_count: i64,
    pub last_seen_unix_micros: i64,
    pub last_elapsed_micros: i64,
    pub last_row_count: i64,
    pub last_success: bool,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct StatementSummary {
    capacity: usize,
    records: BTreeMap<String, StatementSummaryRecord>,
    insertion_order: VecDeque<String>,
}

/// Supplies the storage state needed by bounded virtual catalog queries.
///
/// The embedded facade owns the concrete store and adapts it at this boundary.
#[doc(hidden)]
pub trait SystemSqlStore {
    fn commit_epoch(&self) -> u64;
    fn append_storage_residency_report(&self) -> AppendStorageResidencyReport;
    fn statistics(&self, catalog: &Catalog) -> GraphStatistics;
    fn projected_graph_statuses(&self) -> Vec<ProjectedGraphStatus>;
    fn search_projection_changefeed_status(&self) -> SearchProjectionChangefeedStatus;
}

#[doc(hidden)]
pub struct SystemSqlContext<'a, Store: SystemSqlStore> {
    pub catalog: &'a Catalog,
    pub store: &'a Store,
    pub relational_state: &'a RelationalState,
    pub append_state: &'a AppendState,
    pub runtime: SystemRuntimeSnapshot,
    pub plan_cache_stats: &'a PlanCacheStats,
    pub slow_queries: &'a [SlowQueryRecord],
    pub statement_summaries: &'a [StatementSummaryRecord],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct SystemRuntimeSnapshot {
    read_only: bool,
    max_read_result_rows: Option<usize>,
    max_read_result_payload_bytes: Option<usize>,
    storage_residency_mode: StorageResidencyMode,
    relational_index_mode: RelationalIndexMode,
    runtime_capabilities: RuntimeCapabilities,
}

impl SystemRuntimeSnapshot {
    pub fn new(
        read_only: bool,
        max_read_result_rows: Option<usize>,
        max_read_result_payload_bytes: Option<usize>,
        storage_residency_mode: StorageResidencyMode,
        relational_index_mode: RelationalIndexMode,
        runtime_capabilities: RuntimeCapabilities,
    ) -> Self {
        Self {
            read_only,
            max_read_result_rows,
            max_read_result_payload_bytes,
            storage_residency_mode,
            relational_index_mode,
            runtime_capabilities,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SqlLogicalPlan {
    SystemTableScan(SystemTableScan),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SqlPhysicalPlan {
    SystemTableScanExec(SystemTableScan),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SystemTableScan {
    table: SystemTable,
    projection: Vec<SelectProjection>,
    predicate: Option<SqlPredicate>,
    order_by: Vec<SqlOrderItem>,
    offset: Option<u64>,
    limit: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SystemTable {
    Tables,
    Properties,
    Indexes,
    Constraints,
    AppendTables,
    AppendStorage,
    RuntimeStatus,
    RuntimeCapabilities,
    GraphStatistics,
    ProjectedGraphs,
    SearchProjectionChangefeed,
    PlanCache,
    SlowQueries,
    StatementSummary,
    InformationSchemaTables,
    InformationSchemaColumns,
    PgTables,
    PgIndexes,
}

#[doc(hidden)]
pub fn is_virtual_catalog_select(select: &SelectStatement) -> bool {
    matches!(
        (select.from.schema.as_deref(), select.from.name.as_str()),
        (Some("system" | "information_schema" | "pg_catalog"), _)
            | (None, "pg_tables" | "pg_indexes")
    )
}

impl SlowQueryRecord {
    pub fn completed(completion: SlowQueryCompletion<'_>) -> Self {
        Self {
            sequence: 0,
            query_language: completion.query_language.to_string(),
            statement_kind: completion.statement_kind.to_string(),
            query_digest: completion.query_identity.query_digest().to_string(),
            query_text_hash: completion.query_identity.query_text_hash().to_string(),
            query_text: truncate_utf8(completion.query_text, MAX_SLOW_QUERY_TEXT_BYTES),
            started_unix_micros: unix_now_micros(),
            elapsed_micros: saturating_i64_from_u128(completion.elapsed_micros),
            row_count: i64::try_from(completion.row_count).unwrap_or(i64::MAX),
            success: completion.success,
            error: completion.error,
            slow_log_candidate: completion.slow_log_candidate,
            access_control_policy_epoch: completion.access_control_policy_epoch,
            vector_execution_reports: completion.vector_execution_reports,
        }
    }
}

impl SlowQueryLog {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            next_sequence: 1,
            records: VecDeque::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, mut record: SlowQueryRecord) {
        if self.capacity == 0 {
            return;
        }
        record.sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        while self.records.len() >= self.capacity {
            self.records.pop_front();
        }
        self.records.push_back(record);
    }

    pub fn snapshot(&self) -> Vec<SlowQueryRecord> {
        self.records.iter().cloned().collect()
    }
}

pub fn slow_query_log_jsonl(
    records: &[SlowQueryRecord],
    include_query_text: bool,
) -> Result<String> {
    let mut jsonl = String::new();
    for record in records {
        let line = serde_json::to_string(&slow_query_record_json(record, include_query_text))
            .map_err(|error| {
                HawDBError::Execution(format!("slow query log JSON error: {error}"))
            })?;
        jsonl.push_str(&line);
        jsonl.push('\n');
    }
    Ok(jsonl)
}

pub fn slow_query_record_summary(record: &SlowQueryRecord) -> SlowQueryLogRecordSummary {
    SlowQueryLogRecordSummary {
        sequence: record.sequence,
        query_language: record.query_language.clone(),
        statement_kind: record.statement_kind.clone(),
        query_digest: record.query_digest.clone(),
        started_unix_micros: record.started_unix_micros,
        elapsed_micros: record.elapsed_micros,
        row_count: record.row_count,
        success: record.success,
        slow_log_candidate: record.slow_log_candidate,
        access_control_policy_epoch: record.access_control_policy_epoch,
    }
}

fn slow_query_record_json(record: &SlowQueryRecord, include_query_text: bool) -> serde_json::Value {
    let mut object = serde_json::json!({
        "protocol": SLOW_QUERY_LOG_EVENT_PROTOCOL,
        "protocol_version": 1,
        "sequence": record.sequence,
        "query_language": record.query_language,
        "statement_kind": record.statement_kind,
        "query_digest": record.query_digest,
        "started_unix_micros": record.started_unix_micros,
        "elapsed_micros": record.elapsed_micros,
        "row_count": record.row_count,
        "success": record.success,
        "slow_log_candidate": record.slow_log_candidate,
        "access_control_policy_epoch": record.access_control_policy_epoch,
        "redaction": {
            "query_text_copied": include_query_text,
            "parameters_copied": false,
            "access_control_policy_inputs_copied": false
        }
    });
    if include_query_text {
        object["query_text"] = serde_json::Value::String(record.query_text.clone());
    }
    if let Some(error) = &record.error {
        object["error"] =
            serde_json::Value::String(truncate_utf8(error, MAX_STATEMENT_ERROR_BYTES));
    }
    object["vector_execution_report_count"] =
        serde_json::json!(record.vector_execution_reports.len());
    object["vector_execution_reports"] = serde_json::Value::Array(
        record
            .vector_execution_reports
            .iter()
            .map(vector_execution_report_json)
            .collect(),
    );
    object
}

fn vector_execution_report_json(report: &VectorExecutionReport) -> serde_json::Value {
    serde_json::json!({
        "backend": report.backend.as_str(),
        "compression_mode": report.compression_mode.as_str(),
        "candidate_source": report.candidate_source.as_str(),
        "backend_selection_reason": report.backend_selection_reason.map(|reason| reason.as_str()),
        "estimated_raw_vector_bytes": report.estimated_raw_vector_bytes,
        "filter_selectivity_per_million": report.filter_selectivity_per_million,
        "candidate_score_source": report.candidate_score_source.as_str(),
        "final_score_source": report.final_score_source.as_str(),
        "generated_candidate_count": report.generated_candidate_count,
        "descriptor_pruned_count": report.descriptor_pruned_count,
        "scalar_filtered_count": report.scalar_filtered_count,
        "residual_filtered_count": report.residual_filtered_count,
        "candidate_scan_rounds": report.candidate_scan_rounds,
        "reranked_candidate_count": report.reranked_candidate_count,
        "returned_count": report.returned_count,
        "raw_vector_bytes_read": report.raw_vector_bytes_read,
        "candidate_scan": report.candidate_scan_metrics.as_ref().map(|metrics| serde_json::json!({
            "kernel": metrics.kernel,
            "worker_count": metrics.worker_count,
            "segment_count": metrics.segment_count,
            "scanned_segment_count": metrics.scanned_segment_count,
            "scored_document_count": metrics.scored_document_count,
            "filtered_document_count": metrics.filtered_document_count,
            "scanned_block_count": metrics.scanned_block_count,
            "skipped_block_count": metrics.skipped_block_count,
            "payload_bytes_read": metrics.payload_bytes_read,
            "admitted_working_bytes": metrics.admitted_working_bytes,
        })),
        "index_covered_document_count": report.index_covered_document_count,
        "index_candidate_document_count": report.index_candidate_document_count,
        "index_coverage_complete": report.index_coverage_complete,
        "fallback_reason_codes": report.fallback_reason_codes.iter().map(|code| code.as_str()).collect::<Vec<_>>(),
    })
}

impl StatementExecution {
    pub fn completed(
        query_language: &str,
        query_text: &str,
        statement_kind: &str,
        query_identity: &QueryIdentity,
        elapsed_micros: u128,
        row_count: usize,
    ) -> Self {
        Self {
            query_language: query_language.to_string(),
            query_text: truncate_utf8(query_text, MAX_STATEMENT_TEXT_BYTES),
            statement_kind: statement_kind.to_string(),
            query_digest: query_identity.query_digest().to_string(),
            query_text_hash: query_identity.query_text_hash().to_string(),
            elapsed_micros: saturating_i64_from_u128(elapsed_micros),
            row_count: i64::try_from(row_count).unwrap_or(i64::MAX),
            success: true,
            error: None,
        }
    }

    pub fn failed(
        query_language: &str,
        query_text: &str,
        statement_kind: &str,
        query_identity: &QueryIdentity,
        elapsed_micros: u128,
        error: String,
    ) -> Self {
        Self {
            query_language: query_language.to_string(),
            query_text: truncate_utf8(query_text, MAX_STATEMENT_TEXT_BYTES),
            statement_kind: statement_kind.to_string(),
            query_digest: query_identity.query_digest().to_string(),
            query_text_hash: query_identity.query_text_hash().to_string(),
            elapsed_micros: saturating_i64_from_u128(elapsed_micros),
            row_count: 0,
            success: false,
            error: Some(truncate_utf8(&error, MAX_STATEMENT_ERROR_BYTES)),
        }
    }
}

impl StatementSummary {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            records: BTreeMap::new(),
            insertion_order: VecDeque::with_capacity(capacity),
        }
    }

    pub fn record(&mut self, execution: StatementExecution) {
        if self.capacity == 0 {
            return;
        }

        let digest = execution.query_digest.clone();
        if let Some(record) = self.records.get_mut(&digest) {
            record.apply(execution);
            return;
        }

        while self.records.len() >= self.capacity {
            let Some(evicted) = self.insertion_order.pop_front() else {
                break;
            };
            self.records.remove(&evicted);
        }

        self.insertion_order.push_back(digest.clone());
        self.records.insert(
            digest.clone(),
            StatementSummaryRecord::from_execution(digest, execution),
        );
    }

    pub fn snapshot(&self) -> Vec<StatementSummaryRecord> {
        self.insertion_order
            .iter()
            .filter_map(|digest| self.records.get(digest).cloned())
            .collect()
    }
}

impl StatementSummaryRecord {
    fn from_execution(digest: String, execution: StatementExecution) -> Self {
        let now = unix_now_micros();
        let success_count = i64::from(execution.success);
        let error_count = i64::from(!execution.success);
        Self {
            digest,
            query_language: execution.query_language,
            query_text: execution.query_text,
            sample_query_text_hash: execution.query_text_hash,
            statement_kind: execution.statement_kind,
            execution_count: 1,
            success_count,
            error_count,
            total_elapsed_micros: execution.elapsed_micros,
            max_elapsed_micros: execution.elapsed_micros,
            total_row_count: execution.row_count,
            last_seen_unix_micros: now,
            last_elapsed_micros: execution.elapsed_micros,
            last_row_count: execution.row_count,
            last_success: execution.success,
            last_error: execution.error,
        }
    }

    fn apply(&mut self, execution: StatementExecution) {
        self.execution_count = self.execution_count.saturating_add(1);
        if execution.success {
            self.success_count = self.success_count.saturating_add(1);
        } else {
            self.error_count = self.error_count.saturating_add(1);
        }
        self.total_elapsed_micros = self
            .total_elapsed_micros
            .saturating_add(execution.elapsed_micros);
        self.max_elapsed_micros = self.max_elapsed_micros.max(execution.elapsed_micros);
        self.total_row_count = self.total_row_count.saturating_add(execution.row_count);
        self.last_seen_unix_micros = unix_now_micros();
        self.last_elapsed_micros = execution.elapsed_micros;
        self.last_row_count = execution.row_count;
        self.last_success = execution.success;
        self.last_error = execution.error;
    }
}

#[cfg(test)]
pub fn query_sql<Store: SystemSqlStore>(
    sql_text: &str,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    context: &SystemSqlContext<'_, Store>,
) -> Result<QueryOutput> {
    query_sql_with_params(sql_text, &[], max_rows, max_payload_bytes, context)
}

pub fn query_sql_with_params<Store: SystemSqlStore>(
    sql_text: &str,
    parameters: &[Value],
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    context: &SystemSqlContext<'_, Store>,
) -> Result<QueryOutput> {
    let logical = plan_sql(sql_text, parameters)?;
    let physical = optimize_sql(logical);
    let rows = execute_sql(physical, context, max_rows)?;
    let payload_bytes = rows.iter().fold(0usize, |total, row| {
        total.saturating_add(map_payload_bytes(row))
    });
    if max_payload_bytes.is_some_and(|limit| payload_bytes > limit) {
        return Err(HawDBError::Execution(format!(
            "SQL query payload uses {payload_bytes} bytes, exceeding max_read_result_payload_bytes {}",
            max_payload_bytes.unwrap_or_default()
        )));
    }
    Ok(QueryOutput { rows: rows.into() })
}

fn plan_sql(sql_text: &str, parameters: &[Value]) -> Result<SqlLogicalPlan> {
    let prepared = hawdb_sql::prepare_postgres_sql(sql_text)?;
    if prepared.parameters.len() != parameters.len() {
        return Err(HawDBError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.parameters.len(),
            parameters.len()
        )));
    }
    let SqlStatement::Select(select) = prepared.statement else {
        return Err(HawDBError::Semantic(
            "system SQL only supports SELECT statements".to_string(),
        ));
    };
    validate_system_select_shape(&select)?;
    let table = system_table(&select)?;
    validate_projection(table, &select.projection)?;
    validate_predicate_columns(table, select.selection.as_ref())?;
    validate_order_columns(table, &select.order_by)?;
    Ok(SqlLogicalPlan::SystemTableScan(SystemTableScan {
        table,
        projection: select.projection,
        predicate: select
            .selection
            .map(|predicate| bind_predicate(predicate, parameters))
            .transpose()?,
        order_by: select.order_by,
        offset: select
            .offset
            .map(|bound| bind_bound(bound, parameters, "OFFSET"))
            .transpose()?,
        limit: select
            .limit
            .map(|bound| bind_bound(bound, parameters, "LIMIT"))
            .transpose()?,
    }))
}

fn validate_system_select_shape(select: &SelectStatement) -> Result<()> {
    if select.having.is_some() {
        return Err(HawDBError::Semantic(
            "system SQL does not support HAVING".into(),
        ));
    }
    if select.distinct
        || select.from_alias.is_some()
        || !select.joins.is_empty()
        || !select.group_by.is_empty()
        || select.lock_strength.is_some()
    {
        return Err(HawDBError::Semantic(
            "system SQL does not support DISTINCT, table aliases, joins, GROUP BY, or locking clauses"
                .to_string(),
        ));
    }
    if select
        .order_by
        .iter()
        .any(|item| item.nulls != hawdb_sql::SqlNullOrder::DialectDefault)
    {
        return Err(HawDBError::Semantic(
            "system SQL does not support explicit NULLS FIRST/LAST".to_string(),
        ));
    }
    Ok(())
}

fn bind_predicate(mut predicate: SqlPredicate, parameters: &[Value]) -> Result<SqlPredicate> {
    predicate.try_visit_mut(&mut |expression| {
        if let ExprKind::Value(value) = &mut expression.kind {
            let unbound = std::mem::replace(value, SqlValue::Literal(Value::Null));
            *value = SqlValue::Literal(bind_value(unbound, parameters)?);
        }
        if let ExprKind::Like {
            pattern,
            escape,
            case_insensitive,
            ..
        } = &expression.kind
            && let SqlValue::Literal(Value::String(pattern)) = pattern.require_value()?
        {
            hawdb_sql::sql_like_matches("", pattern, *escape, *case_insensitive)?;
        }
        Ok::<_, HawDBError>(())
    })?;
    Ok(predicate)
}

fn bind_value(value: SqlValue, parameters: &[Value]) -> Result<Value> {
    match value {
        SqlValue::Literal(value) => Ok(value),
        SqlValue::Parameter(position) => parameters.get(position - 1).cloned().ok_or_else(|| {
            HawDBError::Semantic(format!("missing PostgreSQL parameter ${position}"))
        }),
    }
}

fn bind_bound(bound: SqlBound, parameters: &[Value], name: &str) -> Result<u64> {
    match bound {
        SqlBound::Literal(value) => Ok(value),
        SqlBound::Parameter(position) => match parameters.get(position - 1) {
            Some(Value::Int(value)) if *value >= 0 => Ok(*value as u64),
            Some(_) => Err(HawDBError::Semantic(format!(
                "PostgreSQL {name} parameter ${position} must be a non-negative integer"
            ))),
            None => Err(HawDBError::Semantic(format!(
                "missing PostgreSQL parameter ${position}"
            ))),
        },
    }
}

fn optimize_sql(logical: SqlLogicalPlan) -> SqlPhysicalPlan {
    match logical {
        SqlLogicalPlan::SystemTableScan(scan) => SqlPhysicalPlan::SystemTableScanExec(scan),
    }
}

fn execute_sql<Store: SystemSqlStore>(
    physical: SqlPhysicalPlan,
    context: &SystemSqlContext<'_, Store>,
    max_rows: Option<usize>,
) -> Result<Vec<Row>> {
    match physical {
        SqlPhysicalPlan::SystemTableScanExec(scan) => {
            execute_system_table_scan(scan, context, max_rows)
        }
    }
}

fn execute_system_table_scan<Store: SystemSqlStore>(
    scan: SystemTableScan,
    context: &SystemSqlContext<'_, Store>,
    max_rows: Option<usize>,
) -> Result<Vec<Row>> {
    let mut rows = match scan.table {
        SystemTable::Tables => table_rows(context.catalog),
        SystemTable::Properties => property_rows(context.catalog),
        SystemTable::Indexes => index_rows(context.catalog),
        SystemTable::Constraints => constraint_rows(context.catalog),
        SystemTable::AppendTables => append_table_rows(context.append_state),
        SystemTable::AppendStorage => append_storage_rows(context.store, context.append_state),
        SystemTable::RuntimeStatus => runtime_status_rows(context),
        SystemTable::RuntimeCapabilities => runtime_capability_rows(context.runtime),
        SystemTable::GraphStatistics => {
            graph_statistics_rows(context.catalog, &context.store.statistics(context.catalog))
        }
        SystemTable::ProjectedGraphs => {
            projected_graph_rows(context.store.projected_graph_statuses())
        }
        SystemTable::SearchProjectionChangefeed => {
            search_projection_changefeed_rows(context.store.search_projection_changefeed_status())
        }
        SystemTable::PlanCache => plan_cache_rows(context.plan_cache_stats),
        SystemTable::SlowQueries => slow_query_rows(context.slow_queries),
        SystemTable::StatementSummary => statement_summary_rows(context.statement_summaries),
        SystemTable::InformationSchemaTables => {
            information_schema_table_rows(context.relational_state)
        }
        SystemTable::InformationSchemaColumns => {
            information_schema_column_rows(context.relational_state)
        }
        SystemTable::PgTables => pg_table_rows(context.relational_state),
        SystemTable::PgIndexes => pg_index_rows(context.relational_state),
    };

    if let Some(predicate) = &scan.predicate {
        rows.retain(|row| predicate_matches(predicate, row));
    }

    if !scan.order_by.is_empty() {
        rows.sort_by(|left, right| compare_ordered_rows(left, right, &scan.order_by));
    }

    let offset = scan.offset.unwrap_or(0);
    let limit = effective_limit(scan.limit, max_rows)?;
    rows = rows
        .into_iter()
        .skip(usize::try_from(offset).unwrap_or(usize::MAX))
        .take(limit.unwrap_or(usize::MAX))
        .collect();

    if let Some(max_rows) = max_rows
        && rows.len() > max_rows
    {
        return Err(HawDBError::Execution(format!(
                "SQL query returned more than {max_rows} rows, exceeding max_read_result_rows {max_rows}"
            )));
    }

    project_rows(rows, &scan.projection)
}

fn effective_limit(query_limit: Option<u64>, max_rows: Option<usize>) -> Result<Option<usize>> {
    let query_limit = query_limit
        .map(|limit| {
            usize::try_from(limit)
                .map_err(|_| HawDBError::Semantic("SQL LIMIT is too large".to_string()))
        })
        .transpose()?;
    Ok(match (query_limit, max_rows) {
        (Some(query_limit), Some(max_rows)) => Some(query_limit.min(max_rows.saturating_add(1))),
        (Some(query_limit), None) => Some(query_limit),
        (None, Some(max_rows)) => Some(max_rows.saturating_add(1)),
        (None, None) => None,
    })
}

fn project_rows(rows: Vec<Row>, projection: &[SelectProjection]) -> Result<Vec<Row>> {
    if projection
        .iter()
        .any(|projection| matches!(projection, SelectProjection::Wildcard))
    {
        return Ok(rows);
    }
    rows.into_iter()
        .map(|row| {
            projection
                .iter()
                .map(|projection| {
                    let SelectProjection::Expression {
                        expression:
                            Expr {
                                kind: ExprKind::Column(name),
                                ..
                            },
                        alias,
                        ..
                    } = projection
                    else {
                        unreachable!("wildcard handled above");
                    };
                    let value = row.get(&name.name).cloned().unwrap_or(Value::Null);
                    Ok((alias.clone().unwrap_or_else(|| name.name.clone()), value))
                })
                .collect()
        })
        .collect()
}

fn table_rows(catalog: &Catalog) -> Vec<Row> {
    catalog
        .table_descriptors()
        .map(|table| {
            BTreeMap::from([
                ("table_id".to_string(), u32_value(table.id.0)),
                ("table_name".to_string(), Value::String(table.name.clone())),
                (
                    "table_kind".to_string(),
                    Value::String(table_kind_name(table.kind).to_string()),
                ),
                (
                    "state".to_string(),
                    Value::String(schema_object_state_name(table.state).to_string()),
                ),
            ])
        })
        .collect()
}

fn information_schema_table_rows(state: &RelationalState) -> Vec<Row> {
    state
        .table_schemas()
        .map(|schema| {
            BTreeMap::from([
                (
                    "table_catalog".to_string(),
                    Value::String("hawdb".to_string()),
                ),
                (
                    "table_schema".to_string(),
                    Value::String("public".to_string()),
                ),
                ("table_name".to_string(), Value::String(schema.name.clone())),
                (
                    "table_type".to_string(),
                    Value::String("BASE TABLE".to_string()),
                ),
                ("self_referencing_column_name".to_string(), Value::Null),
                ("reference_generation".to_string(), Value::Null),
                ("user_defined_type_catalog".to_string(), Value::Null),
                ("user_defined_type_schema".to_string(), Value::Null),
                ("user_defined_type_name".to_string(), Value::Null),
                (
                    "is_insertable_into".to_string(),
                    Value::String("YES".to_string()),
                ),
                ("is_typed".to_string(), Value::String("NO".to_string())),
                ("commit_action".to_string(), Value::Null),
            ])
        })
        .collect()
}

fn information_schema_column_rows(state: &RelationalState) -> Vec<Row> {
    state
        .table_schemas()
        .flat_map(|schema| {
            schema
                .columns
                .iter()
                .enumerate()
                .map(move |(position, column)| {
                    let type_info = information_schema_type(column.scalar_type);
                    let ordinal_position = position.saturating_add(1);
                    BTreeMap::from([
                        (
                            "table_catalog".to_string(),
                            Value::String("hawdb".to_string()),
                        ),
                        (
                            "table_schema".to_string(),
                            Value::String("public".to_string()),
                        ),
                        ("table_name".to_string(), Value::String(schema.name.clone())),
                        (
                            "column_name".to_string(),
                            Value::String(column.name.clone()),
                        ),
                        (
                            "ordinal_position".to_string(),
                            usize_value(ordinal_position),
                        ),
                        (
                            "column_default".to_string(),
                            relational_default_value(column.default.as_ref()),
                        ),
                        (
                            "is_nullable".to_string(),
                            Value::String(if column.nullable { "YES" } else { "NO" }.to_string()),
                        ),
                        (
                            "data_type".to_string(),
                            Value::String(type_info.data_type.to_string()),
                        ),
                        (
                            "character_maximum_length".to_string(),
                            option_i64_value(type_info.character_maximum_length),
                        ),
                        (
                            "character_octet_length".to_string(),
                            option_i64_value(type_info.character_octet_length),
                        ),
                        (
                            "numeric_precision".to_string(),
                            option_i64_value(type_info.numeric_precision),
                        ),
                        (
                            "numeric_precision_radix".to_string(),
                            option_i64_value(type_info.numeric_precision_radix),
                        ),
                        (
                            "numeric_scale".to_string(),
                            option_i64_value(type_info.numeric_scale),
                        ),
                        ("datetime_precision".to_string(), Value::Null),
                        ("interval_type".to_string(), Value::Null),
                        ("interval_precision".to_string(), Value::Null),
                        ("character_set_catalog".to_string(), Value::Null),
                        ("character_set_schema".to_string(), Value::Null),
                        ("character_set_name".to_string(), Value::Null),
                        ("collation_catalog".to_string(), Value::Null),
                        ("collation_schema".to_string(), Value::Null),
                        ("collation_name".to_string(), Value::Null),
                        ("domain_catalog".to_string(), Value::Null),
                        ("domain_schema".to_string(), Value::Null),
                        ("domain_name".to_string(), Value::Null),
                        (
                            "udt_catalog".to_string(),
                            Value::String("hawdb".to_string()),
                        ),
                        (
                            "udt_schema".to_string(),
                            Value::String("pg_catalog".to_string()),
                        ),
                        (
                            "udt_name".to_string(),
                            Value::String(type_info.udt_name.to_string()),
                        ),
                        ("scope_catalog".to_string(), Value::Null),
                        ("scope_schema".to_string(), Value::Null),
                        ("scope_name".to_string(), Value::Null),
                        ("maximum_cardinality".to_string(), Value::Null),
                        (
                            "dtd_identifier".to_string(),
                            Value::String(ordinal_position.to_string()),
                        ),
                        (
                            "is_self_referencing".to_string(),
                            Value::String("NO".to_string()),
                        ),
                        ("is_identity".to_string(), Value::String("NO".to_string())),
                        ("identity_generation".to_string(), Value::Null),
                        ("identity_start".to_string(), Value::Null),
                        ("identity_increment".to_string(), Value::Null),
                        ("identity_maximum".to_string(), Value::Null),
                        ("identity_minimum".to_string(), Value::Null),
                        ("identity_cycle".to_string(), Value::Null),
                        (
                            "is_generated".to_string(),
                            Value::String("NEVER".to_string()),
                        ),
                        ("generation_expression".to_string(), Value::Null),
                        ("is_updatable".to_string(), Value::String("YES".to_string())),
                    ])
                })
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct InformationSchemaType {
    data_type: &'static str,
    udt_name: &'static str,
    character_maximum_length: Option<i64>,
    character_octet_length: Option<i64>,
    numeric_precision: Option<i64>,
    numeric_precision_radix: Option<i64>,
    numeric_scale: Option<i64>,
}

fn information_schema_type(scalar_type: RelationalScalarType) -> InformationSchemaType {
    match scalar_type {
        RelationalScalarType::Boolean => InformationSchemaType {
            data_type: "boolean",
            udt_name: "bool",
            character_maximum_length: None,
            character_octet_length: None,
            numeric_precision: None,
            numeric_precision_radix: None,
            numeric_scale: None,
        },
        RelationalScalarType::BigInt => InformationSchemaType {
            data_type: "bigint",
            udt_name: "int8",
            character_maximum_length: None,
            character_octet_length: None,
            numeric_precision: Some(64),
            numeric_precision_radix: Some(2),
            numeric_scale: Some(0),
        },
        RelationalScalarType::DoublePrecision => InformationSchemaType {
            data_type: "double precision",
            udt_name: "float8",
            character_maximum_length: None,
            character_octet_length: None,
            numeric_precision: Some(53),
            numeric_precision_radix: Some(2),
            numeric_scale: None,
        },
        RelationalScalarType::Text => InformationSchemaType {
            data_type: "text",
            udt_name: "text",
            character_maximum_length: None,
            character_octet_length: None,
            numeric_precision: None,
            numeric_precision_radix: None,
            numeric_scale: None,
        },
        RelationalScalarType::Bytea => InformationSchemaType {
            data_type: "bytea",
            udt_name: "bytea",
            character_maximum_length: None,
            character_octet_length: None,
            numeric_precision: None,
            numeric_precision_radix: None,
            numeric_scale: None,
        },
        RelationalScalarType::Uuid => InformationSchemaType {
            data_type: "uuid",
            udt_name: "uuid",
            character_maximum_length: None,
            character_octet_length: None,
            numeric_precision: None,
            numeric_precision_radix: None,
            numeric_scale: None,
        },
    }
}

fn relational_default_value(default: Option<&RelationalColumnDefault>) -> Value {
    match default {
        None | Some(RelationalColumnDefault::Literal(RelationalValue::Overflow(_))) => Value::Null,
        Some(RelationalColumnDefault::Literal(RelationalValue::Null)) => {
            Value::String("NULL".to_string())
        }
        Some(RelationalColumnDefault::Literal(RelationalValue::Boolean(value))) => {
            Value::String(value.to_string())
        }
        Some(RelationalColumnDefault::Literal(RelationalValue::BigInt(value))) => {
            Value::String(value.to_string())
        }
        Some(RelationalColumnDefault::Literal(RelationalValue::DoublePrecision(value))) => {
            Value::String(value.to_string())
        }
        Some(RelationalColumnDefault::Literal(RelationalValue::Text(value))) => {
            Value::String(format!("'{}'::text", value.replace('\'', "''")))
        }
        Some(RelationalColumnDefault::Literal(RelationalValue::Bytea(value))) => {
            Value::String(format!("'\\x{}'::bytea", encode_hex(value)))
        }
        Some(RelationalColumnDefault::Literal(RelationalValue::Uuid(value))) => {
            Value::String(format!("'{value}'::uuid"))
        }
        Some(RelationalColumnDefault::UuidV7) => Value::String("uuidv7()".to_string()),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

fn pg_table_rows(state: &RelationalState) -> Vec<Row> {
    state
        .table_schemas()
        .map(|schema| {
            BTreeMap::from([
                (
                    "schemaname".to_string(),
                    Value::String("public".to_string()),
                ),
                ("tablename".to_string(), Value::String(schema.name.clone())),
                ("tableowner".to_string(), Value::String("hawdb".to_string())),
                ("tablespace".to_string(), Value::Null),
                (
                    "hasindexes".to_string(),
                    Value::Bool(
                        !schema.primary_key.is_empty()
                            || !schema.unique_constraints.is_empty()
                            || !schema.indexes.is_empty(),
                    ),
                ),
                ("hasrules".to_string(), Value::Bool(false)),
                ("hastriggers".to_string(), Value::Bool(false)),
                ("rowsecurity".to_string(), Value::Bool(false)),
            ])
        })
        .collect()
}

fn pg_index_rows(state: &RelationalState) -> Vec<Row> {
    state
        .table_schemas()
        .flat_map(relational_schema_index_rows)
        .collect()
}

fn relational_schema_index_rows(schema: &RelationalTableSchema) -> Vec<Row> {
    let mut rows = Vec::with_capacity(
        1usize
            .saturating_add(schema.unique_constraints.len())
            .saturating_add(schema.indexes.len()),
    );
    if !schema.primary_key.is_empty() {
        let name = format!("{}_pkey", schema.name);
        rows.push(pg_index_row(schema, &name, &schema.primary_key, true));
    }
    rows.extend(schema.unique_constraints.iter().map(|columns| {
        let name = format!("{}_{}_key", schema.name, columns.join("_"));
        pg_index_row(schema, &name, columns, true)
    }));
    rows.extend(
        schema
            .indexes
            .iter()
            .map(|index| pg_index_row(schema, &index.name, &index.columns, index.unique)),
    );
    rows
}

fn pg_index_row(
    schema: &RelationalTableSchema,
    index_name: &str,
    columns: &[String],
    unique: bool,
) -> Row {
    let unique = if unique { "UNIQUE " } else { "" };
    let columns = columns
        .iter()
        .map(|column| quote_postgres_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let index_definition = format!(
        "CREATE {unique}INDEX {} ON public.{} USING btree ({columns})",
        quote_postgres_identifier(index_name),
        quote_postgres_identifier(&schema.name),
    );
    BTreeMap::from([
        (
            "schemaname".to_string(),
            Value::String("public".to_string()),
        ),
        ("tablename".to_string(), Value::String(schema.name.clone())),
        (
            "indexname".to_string(),
            Value::String(index_name.to_string()),
        ),
        ("tablespace".to_string(), Value::Null),
        ("indexdef".to_string(), Value::String(index_definition)),
    ])
}

fn quote_postgres_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn property_rows(catalog: &Catalog) -> Vec<Row> {
    catalog
        .property_descriptors()
        .map(|property| {
            let table = catalog.table_descriptor(property.table_id);
            BTreeMap::from([
                ("property_id".to_string(), u32_value(property.id.0)),
                ("table_id".to_string(), u32_value(property.table_id.0)),
                (
                    "table_name".to_string(),
                    table
                        .map(|table| Value::String(table.name.clone()))
                        .unwrap_or(Value::Null),
                ),
                (
                    "table_kind".to_string(),
                    table
                        .map(|table| Value::String(table_kind_name(table.kind).to_string()))
                        .unwrap_or(Value::Null),
                ),
                (
                    "property_name".to_string(),
                    Value::String(property.name.clone()),
                ),
                (
                    "value_type".to_string(),
                    Value::String(property_type_name(property.value_type).to_string()),
                ),
                ("nullable".to_string(), Value::Bool(property.nullable)),
                (
                    "state".to_string(),
                    Value::String(schema_object_state_name(property.state).to_string()),
                ),
            ])
        })
        .collect()
}

fn index_rows(catalog: &Catalog) -> Vec<Row> {
    let mut rows = catalog
        .property_indexes()
        .map(|index| {
            BTreeMap::from([
                ("index_id".to_string(), u32_value(index.id.0)),
                (
                    "subject_kind".to_string(),
                    Value::String("node".to_string()),
                ),
                ("subject_id".to_string(), u32_value(index.label_id.0)),
                (
                    "subject_name".to_string(),
                    optional_string_value(catalog.label_name(index.label_id)),
                ),
                (
                    "index_kind".to_string(),
                    Value::String(index_kind_name(index.kind).to_string()),
                ),
                (
                    "property_names".to_string(),
                    Value::List(vec![Value::String(index.property.clone())]),
                ),
            ])
        })
        .chain(catalog.composite_property_indexes().map(|index| {
            BTreeMap::from([
                ("index_id".to_string(), u32_value(index.id.0)),
                (
                    "subject_kind".to_string(),
                    Value::String("node".to_string()),
                ),
                ("subject_id".to_string(), u32_value(index.label_id.0)),
                (
                    "subject_name".to_string(),
                    optional_string_value(catalog.label_name(index.label_id)),
                ),
                (
                    "index_kind".to_string(),
                    Value::String("composite_equality".to_string()),
                ),
                (
                    "property_names".to_string(),
                    Value::List(
                        index
                            .properties
                            .iter()
                            .cloned()
                            .map(Value::String)
                            .collect(),
                    ),
                ),
            ])
        }))
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.get("index_id").cloned());
    rows
}

fn constraint_rows(catalog: &Catalog) -> Vec<Row> {
    let constraints = catalog
        .unique_constraints()
        .chain(catalog.node_property_exists_constraints())
        .chain(catalog.relationship_property_exists_constraints())
        .chain(catalog.relationship_unique_constraints());
    let mut rows = constraints
        .map(|constraint| {
            let (subject_kind, subject_id, subject_name) = match constraint.subject {
                ConstraintSubject::Node(label_id) => (
                    "node",
                    label_id.0,
                    optional_string_value(catalog.label_name(label_id)),
                ),
                ConstraintSubject::Relationship(rel_type_id) => (
                    "relationship",
                    rel_type_id.0,
                    optional_string_value(catalog.rel_type_name(rel_type_id)),
                ),
            };
            BTreeMap::from([
                ("constraint_id".to_string(), u32_value(constraint.id.0)),
                (
                    "subject_kind".to_string(),
                    Value::String(subject_kind.to_string()),
                ),
                ("subject_id".to_string(), u32_value(subject_id)),
                ("subject_name".to_string(), subject_name),
                (
                    "property_name".to_string(),
                    Value::String(constraint.property.clone()),
                ),
                (
                    "constraint_kind".to_string(),
                    Value::String(constraint_kind_name(constraint.kind).to_string()),
                ),
            ])
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.get("constraint_id").cloned());
    rows
}

fn runtime_status_rows<Store: SystemSqlStore>(context: &SystemSqlContext<'_, Store>) -> Vec<Row> {
    vec![BTreeMap::from([
        (
            "commit_epoch".to_string(),
            u64_value(context.store.commit_epoch()),
        ),
        (
            "read_only".to_string(),
            Value::Bool(context.runtime.read_only),
        ),
        (
            "max_read_result_rows".to_string(),
            option_usize_value(context.runtime.max_read_result_rows),
        ),
        (
            "max_read_result_payload_bytes".to_string(),
            option_usize_value(context.runtime.max_read_result_payload_bytes),
        ),
        (
            "storage_residency_mode".to_string(),
            Value::String(
                storage_residency_mode_name(context.runtime.storage_residency_mode).to_string(),
            ),
        ),
        (
            "relational_index_mode".to_string(),
            Value::String(
                relational_index_mode_name(context.runtime.relational_index_mode).to_string(),
            ),
        ),
    ])]
}

fn append_table_rows(state: &AppendState) -> Vec<Row> {
    state
        .schemas()
        .values()
        .map(|schema| {
            BTreeMap::from([
                ("table_name".to_string(), Value::String(schema.name.clone())),
                (
                    "storage_mode".to_string(),
                    Value::String("strict_append".to_string()),
                ),
                (
                    "partition_key".to_string(),
                    Value::List(
                        schema
                            .partition_key
                            .iter()
                            .cloned()
                            .map(Value::String)
                            .collect(),
                    ),
                ),
                (
                    "order_key".to_string(),
                    Value::List(
                        schema
                            .order_key
                            .iter()
                            .cloned()
                            .map(Value::String)
                            .collect(),
                    ),
                ),
                (
                    "order_mode".to_string(),
                    Value::String(
                        match schema.order_mode {
                            AppendOrderMode::CallerProvided => "caller_provided",
                            AppendOrderMode::CommitSequence => "commit_sequence",
                        }
                        .to_string(),
                    ),
                ),
                (
                    "generated_order_watermark".to_string(),
                    state
                        .generated_order_watermark(&schema.name)
                        .map_or(Value::Null, Value::Int),
                ),
                (
                    "column_count".to_string(),
                    Value::Int(i64::try_from(schema.columns.len()).unwrap_or(i64::MAX)),
                ),
            ])
        })
        .collect()
}

fn append_storage_rows(store: &impl SystemSqlStore, state: &AppendState) -> Vec<Row> {
    let mut report = store.append_storage_residency_report();
    report.live_rows = state.live_rows();
    report.live_payload_bytes = state.live_payload_bytes();
    vec![BTreeMap::from([
        (
            "canonical_segment_count".to_string(),
            Value::Int(i64::try_from(report.canonical_segment_count).unwrap_or(i64::MAX)),
        ),
        (
            "canonical_segment_bytes".to_string(),
            Value::Int(i64::try_from(report.canonical_segment_bytes).unwrap_or(i64::MAX)),
        ),
        (
            "resident_segment_payload_bytes".to_string(),
            Value::Int(i64::try_from(report.resident_segment_payload_bytes).unwrap_or(i64::MAX)),
        ),
        (
            "resident_descriptor_count".to_string(),
            Value::Int(i64::try_from(report.resident_descriptor_count).unwrap_or(i64::MAX)),
        ),
        (
            "live_rows".to_string(),
            Value::Int(i64::try_from(report.live_rows).unwrap_or(i64::MAX)),
        ),
        (
            "live_payload_bytes".to_string(),
            Value::Int(i64::try_from(report.live_payload_bytes).unwrap_or(i64::MAX)),
        ),
    ])]
}

fn runtime_capability_rows(runtime: SystemRuntimeSnapshot) -> Vec<Row> {
    [
        (
            "access_control",
            runtime.runtime_capabilities.access_control,
        ),
        (
            "full_text_search",
            runtime.runtime_capabilities.full_text_search,
        ),
        ("vector_search", runtime.runtime_capabilities.vector_search),
        (
            "graph_analytics",
            runtime.runtime_capabilities.graph_analytics,
        ),
        (
            "background_maintenance",
            runtime.runtime_capabilities.background_maintenance,
        ),
    ]
    .into_iter()
    .map(|(capability, enabled)| {
        BTreeMap::from([
            (
                "capability".to_string(),
                Value::String(capability.to_string()),
            ),
            ("enabled".to_string(), Value::Bool(enabled)),
        ])
    })
    .collect()
}

fn graph_statistics_rows(catalog: &Catalog, statistics: &GraphStatistics) -> Vec<Row> {
    let mut rows = Vec::with_capacity(
        2usize
            .saturating_add(statistics.label_counts.len())
            .saturating_add(statistics.rel_type_counts.len())
            .saturating_add(statistics.rel_type_source_counts.len())
            .saturating_add(statistics.rel_type_target_counts.len())
            .saturating_add(statistics.path_counts.len())
            .saturating_add(statistics.path_source_distinct_counts.len())
            .saturating_add(statistics.path_target_distinct_counts.len())
            .saturating_add(statistics.bounded_path_counts.len())
            .saturating_add(statistics.bounded_path_source_distinct_counts.len())
            .saturating_add(statistics.bounded_path_target_distinct_counts.len())
            .saturating_add(statistics.index_samples.len())
            .saturating_add(statistics.property_distinct_counts.len())
            .saturating_add(statistics.rel_property_distinct_counts.len())
            .saturating_add(statistics.property_histograms.len())
            .saturating_add(statistics.rel_property_histograms.len()),
    );
    rows.push(graph_statistic_count_row(
        statistics,
        "node_count",
        statistics.node_count,
    ));
    rows.push(graph_statistic_count_row(
        statistics,
        "relationship_count",
        statistics.relationship_count,
    ));

    for (label_id, count) in &statistics.label_counts {
        let mut row = graph_statistic_count_row(statistics, "label_count", *count);
        row.insert(
            "label_name".to_string(),
            optional_string_value(catalog.label_name(*label_id)),
        );
        rows.push(row);
    }
    for (rel_type_id, count) in &statistics.rel_type_counts {
        let mut row = graph_statistic_count_row(statistics, "relationship_type_count", *count);
        row.insert(
            "relationship_type_name".to_string(),
            optional_string_value(catalog.rel_type_name(*rel_type_id)),
        );
        rows.push(row);
    }
    for (kind, counts) in [
        (
            "relationship_source_count",
            &statistics.rel_type_source_counts,
        ),
        (
            "relationship_target_count",
            &statistics.rel_type_target_counts,
        ),
    ] {
        for (rel_type_id, count) in counts {
            let mut row = graph_statistic_count_row(statistics, kind, *count);
            row.insert(
                "relationship_type_name".to_string(),
                optional_string_value(catalog.rel_type_name(*rel_type_id)),
            );
            rows.push(row);
        }
    }
    for index in catalog.property_indexes() {
        let Some(sample) = statistics.index_samples.get(&index.id) else {
            continue;
        };
        let mut row = graph_statistic_base_row(statistics, "index_sample");
        row.insert("index_id".to_string(), u32_value(index.id.0));
        row.insert(
            "index_kind".to_string(),
            Value::String(index_kind_name(index.kind).to_string()),
        );
        row.insert(
            "label_name".to_string(),
            optional_string_value(catalog.label_name(index.label_id)),
        );
        row.insert(
            "property_name".to_string(),
            Value::String(index.property.clone()),
        );
        populate_index_sample(&mut row, *sample);
        rows.push(row);
    }
    for index in catalog.composite_property_indexes() {
        let Some(sample) = statistics.index_samples.get(&index.id) else {
            continue;
        };
        let mut row = graph_statistic_base_row(statistics, "index_sample");
        row.insert("index_id".to_string(), u32_value(index.id.0));
        row.insert(
            "index_kind".to_string(),
            Value::String("composite".to_string()),
        );
        row.insert(
            "label_name".to_string(),
            optional_string_value(catalog.label_name(index.label_id)),
        );
        row.insert(
            "index_properties".to_string(),
            Value::List(
                index
                    .properties
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
        populate_index_sample(&mut row, *sample);
        rows.push(row);
    }
    for (kind, counts) in [
        ("path_count", &statistics.path_counts),
        (
            "path_source_distinct_count",
            &statistics.path_source_distinct_counts,
        ),
        (
            "path_target_distinct_count",
            &statistics.path_target_distinct_counts,
        ),
    ] {
        for ((source_label_id, rel_type_id, target_label_id), count) in counts {
            let mut row = graph_statistic_count_row(statistics, kind, *count);
            populate_path_statistic_names(
                &mut row,
                catalog,
                *source_label_id,
                *rel_type_id,
                *target_label_id,
            );
            rows.push(row);
        }
    }
    for (kind, counts) in [
        ("bounded_path_count", &statistics.bounded_path_counts),
        (
            "bounded_path_source_distinct_count",
            &statistics.bounded_path_source_distinct_counts,
        ),
        (
            "bounded_path_target_distinct_count",
            &statistics.bounded_path_target_distinct_counts,
        ),
    ] {
        for ((source_label_id, rel_type_id, target_label_id, depth), count) in counts {
            let mut row = graph_statistic_count_row(statistics, kind, *count);
            populate_path_statistic_names(
                &mut row,
                catalog,
                *source_label_id,
                *rel_type_id,
                *target_label_id,
            );
            row.insert("depth".to_string(), usize_value(*depth));
            rows.push(row);
        }
    }
    for ((label_id, property_name), count) in &statistics.property_distinct_counts {
        let mut row = graph_statistic_count_row(statistics, "node_property_distinct_count", *count);
        row.insert(
            "label_name".to_string(),
            optional_string_value(catalog.label_name(*label_id)),
        );
        row.insert(
            "property_name".to_string(),
            Value::String(property_name.clone()),
        );
        rows.push(row);
    }
    for ((rel_type_id, property_name), count) in &statistics.rel_property_distinct_counts {
        let mut row =
            graph_statistic_count_row(statistics, "relationship_property_distinct_count", *count);
        row.insert(
            "relationship_type_name".to_string(),
            optional_string_value(catalog.rel_type_name(*rel_type_id)),
        );
        row.insert(
            "property_name".to_string(),
            Value::String(property_name.clone()),
        );
        rows.push(row);
    }
    for ((label_id, property_name), histogram) in &statistics.property_histograms {
        let mut row = graph_statistic_base_row(statistics, "node_property_histogram");
        row.insert(
            "label_name".to_string(),
            optional_string_value(catalog.label_name(*label_id)),
        );
        row.insert(
            "property_name".to_string(),
            Value::String(property_name.clone()),
        );
        row.insert("histogram".to_string(), Value::List(histogram.clone()));
        row.insert(
            "sampled".to_string(),
            Value::Bool(
                statistics
                    .sampled_property_histograms
                    .get(&(*label_id, property_name.clone()))
                    .copied()
                    .unwrap_or(false),
            ),
        );
        rows.push(row);
    }
    for ((rel_type_id, property_name), histogram) in &statistics.rel_property_histograms {
        let mut row = graph_statistic_base_row(statistics, "relationship_property_histogram");
        row.insert(
            "relationship_type_name".to_string(),
            optional_string_value(catalog.rel_type_name(*rel_type_id)),
        );
        row.insert(
            "property_name".to_string(),
            Value::String(property_name.clone()),
        );
        row.insert("histogram".to_string(), Value::List(histogram.clone()));
        row.insert(
            "sampled".to_string(),
            Value::Bool(
                statistics
                    .sampled_rel_property_histograms
                    .get(&(*rel_type_id, property_name.clone()))
                    .copied()
                    .unwrap_or(false),
            ),
        );
        rows.push(row);
    }
    rows
}

fn graph_statistic_count_row(statistics: &GraphStatistics, kind: &str, count: u64) -> Row {
    let mut row = graph_statistic_base_row(statistics, kind);
    row.insert("count".to_string(), u64_value(count));
    row
}

fn populate_index_sample(row: &mut Row, sample: IndexStatisticsSample) {
    row.insert("index_size".to_string(), u64_value(sample.index_size));
    row.insert("unique_values".to_string(), u64_value(sample.unique_values));
    row.insert("sample_size".to_string(), u64_value(sample.sample_size));
    row.insert(
        "updates_since_sample".to_string(),
        u64_value(sample.updates_since_sample),
    );
    row.insert("stale".to_string(), Value::Bool(sample.is_stale()));
}

fn graph_statistic_base_row(statistics: &GraphStatistics, kind: &str) -> Row {
    BTreeMap::from([
        (
            "statistic_kind".to_string(),
            Value::String(kind.to_string()),
        ),
        (
            "computed_at_commit_epoch".to_string(),
            u64_value(statistics.computed_at_commit_epoch),
        ),
        (
            "advanced_statistics_complete".to_string(),
            Value::Bool(statistics.advanced_statistics_complete),
        ),
        (
            "histogram_sample_limit".to_string(),
            usize_value(statistics.histogram_sample_limit),
        ),
        ("label_name".to_string(), Value::Null),
        ("relationship_type_name".to_string(), Value::Null),
        ("source_label_name".to_string(), Value::Null),
        ("target_label_name".to_string(), Value::Null),
        ("depth".to_string(), Value::Null),
        ("property_name".to_string(), Value::Null),
        ("index_id".to_string(), Value::Null),
        ("index_kind".to_string(), Value::Null),
        ("index_properties".to_string(), Value::Null),
        ("index_size".to_string(), Value::Null),
        ("unique_values".to_string(), Value::Null),
        ("sample_size".to_string(), Value::Null),
        ("updates_since_sample".to_string(), Value::Null),
        ("stale".to_string(), Value::Null),
        ("count".to_string(), Value::Null),
        ("histogram".to_string(), Value::Null),
        ("sampled".to_string(), Value::Null),
    ])
}

fn populate_path_statistic_names(
    row: &mut Row,
    catalog: &Catalog,
    source_label_id: LabelId,
    rel_type_id: RelTypeId,
    target_label_id: LabelId,
) {
    row.insert(
        "source_label_name".to_string(),
        optional_string_value(catalog.label_name(source_label_id)),
    );
    row.insert(
        "relationship_type_name".to_string(),
        optional_string_value(catalog.rel_type_name(rel_type_id)),
    );
    row.insert(
        "target_label_name".to_string(),
        optional_string_value(catalog.label_name(target_label_id)),
    );
}

fn projected_graph_rows(statuses: Vec<ProjectedGraphStatus>) -> Vec<Row> {
    statuses
        .into_iter()
        .map(|status| {
            BTreeMap::from([
                ("name".to_string(), Value::String(status.name)),
                (
                    "node_labels".to_string(),
                    Value::List(status.node_labels.into_iter().map(Value::String).collect()),
                ),
                (
                    "relationship_types".to_string(),
                    Value::List(status.rel_types.into_iter().map(Value::String).collect()),
                ),
                (
                    "projection_epoch".to_string(),
                    option_u64_value(status.projection_epoch),
                ),
                (
                    "commit_epoch".to_string(),
                    option_u64_value(status.commit_epoch),
                ),
                (
                    "node_count".to_string(),
                    option_usize_value(status.node_count),
                ),
                (
                    "edge_count".to_string(),
                    option_usize_value(status.edge_count),
                ),
                ("reusable".to_string(), Value::Bool(status.reusable)),
            ])
        })
        .collect()
}

fn search_projection_changefeed_rows(status: SearchProjectionChangefeedStatus) -> Vec<Row> {
    vec![BTreeMap::from([
        (
            "graph_commit_epoch".to_string(),
            u64_value(status.graph_commit_epoch),
        ),
        (
            "resume_floor_commit_epoch".to_string(),
            u64_value(status.resume_floor_commit_epoch),
        ),
        (
            "oldest_retained_mutation_id".to_string(),
            option_u64_value(
                status
                    .oldest_retained_mutation_id
                    .map(|id| id.commit_epoch()),
            ),
        ),
        (
            "newest_retained_mutation_id".to_string(),
            option_u64_value(
                status
                    .newest_retained_mutation_id
                    .map(|id| id.commit_epoch()),
            ),
        ),
        (
            "retained_mutation_count".to_string(),
            usize_value(status.retained_mutation_count),
        ),
        (
            "restart_recoverable".to_string(),
            Value::Bool(status.restart_recoverable),
        ),
    ])]
}

fn optional_string_value(value: Option<&str>) -> Value {
    value
        .map(|value| Value::String(value.to_string()))
        .unwrap_or(Value::Null)
}

const fn table_kind_name(kind: TableKind) -> &'static str {
    match kind {
        TableKind::Node => "node",
        TableKind::Relationship => "relationship",
    }
}

const fn schema_object_state_name(state: SchemaObjectState) -> &'static str {
    match state {
        SchemaObjectState::DeleteOnly => "delete_only",
        SchemaObjectState::WriteOnly => "write_only",
        SchemaObjectState::Backfill => "backfill",
        SchemaObjectState::Validating => "validating",
        SchemaObjectState::Public => "public",
        SchemaObjectState::Gc => "gc",
    }
}

const fn property_type_name(value_type: PropertyType) -> &'static str {
    match value_type {
        PropertyType::Any => "any",
        PropertyType::Bool => "bool",
        PropertyType::Int => "int",
        PropertyType::Float => "float",
        PropertyType::String => "string",
        PropertyType::Text => "text",
        PropertyType::List => "list",
    }
}

const fn index_kind_name(kind: IndexKind) -> &'static str {
    match kind {
        IndexKind::Equality => "equality",
        IndexKind::Range => "range",
        IndexKind::FullText => "full_text",
    }
}

const fn constraint_kind_name(kind: ConstraintKind) -> &'static str {
    match kind {
        ConstraintKind::NodePropertyUnique => "node_property_unique",
        ConstraintKind::NodePropertyExists => "node_property_exists",
        ConstraintKind::RelationshipPropertyUnique => "relationship_property_unique",
        ConstraintKind::RelationshipPropertyExists => "relationship_property_exists",
    }
}

fn plan_cache_rows(stats: &PlanCacheStats) -> Vec<Row> {
    [
        ("max_entries", option_usize_value(stats.max_entries)),
        ("entries", usize_value(stats.entries)),
        ("hits", u64_value(stats.hits)),
        ("misses", u64_value(stats.misses)),
        ("admissions", u64_value(stats.admissions)),
        ("disabled_misses", u64_value(stats.disabled_misses)),
        ("bypasses", u64_value(stats.bypasses)),
        ("evictions", u64_value(stats.evictions)),
        (
            "memory_pressure_events",
            u64_value(stats.memory_pressure_events),
        ),
    ]
    .into_iter()
    .map(|(metric, value)| {
        BTreeMap::from([
            ("metric".to_string(), Value::String(metric.to_string())),
            ("value".to_string(), value),
        ])
    })
    .collect()
}

fn slow_query_rows(records: &[SlowQueryRecord]) -> Vec<Row> {
    records
        .iter()
        .map(|record| {
            BTreeMap::from([
                ("sequence".to_string(), u64_value(record.sequence)),
                (
                    "query_language".to_string(),
                    Value::String(record.query_language.clone()),
                ),
                (
                    "statement_kind".to_string(),
                    Value::String(record.statement_kind.clone()),
                ),
                (
                    "query_digest".to_string(),
                    Value::String(record.query_digest.clone()),
                ),
                (
                    "query_text_hash".to_string(),
                    Value::String(record.query_text_hash.clone()),
                ),
                (
                    "query_text".to_string(),
                    Value::String(record.query_text.clone()),
                ),
                (
                    "started_unix_micros".to_string(),
                    Value::Int(record.started_unix_micros),
                ),
                (
                    "elapsed_micros".to_string(),
                    Value::Int(record.elapsed_micros),
                ),
                ("row_count".to_string(), Value::Int(record.row_count)),
                ("success".to_string(), Value::Bool(record.success)),
                (
                    "error".to_string(),
                    record
                        .error
                        .as_ref()
                        .map(|error| Value::String(error.clone()))
                        .unwrap_or(Value::Null),
                ),
                (
                    "slow_log_candidate".to_string(),
                    Value::Bool(record.slow_log_candidate),
                ),
                (
                    "access_control_policy_epoch".to_string(),
                    record
                        .access_control_policy_epoch
                        .map(u64_value)
                        .unwrap_or(Value::Null),
                ),
            ])
        })
        .collect()
}

fn statement_summary_rows(records: &[StatementSummaryRecord]) -> Vec<Row> {
    records
        .iter()
        .map(|record| {
            BTreeMap::from([
                ("digest".to_string(), Value::String(record.digest.clone())),
                (
                    "query_language".to_string(),
                    Value::String(record.query_language.clone()),
                ),
                (
                    "query_text".to_string(),
                    Value::String(record.query_text.clone()),
                ),
                (
                    "sample_query_text_hash".to_string(),
                    Value::String(record.sample_query_text_hash.clone()),
                ),
                (
                    "statement_kind".to_string(),
                    Value::String(record.statement_kind.clone()),
                ),
                (
                    "execution_count".to_string(),
                    Value::Int(record.execution_count),
                ),
                (
                    "success_count".to_string(),
                    Value::Int(record.success_count),
                ),
                ("error_count".to_string(), Value::Int(record.error_count)),
                (
                    "total_elapsed_micros".to_string(),
                    Value::Int(record.total_elapsed_micros),
                ),
                (
                    "max_elapsed_micros".to_string(),
                    Value::Int(record.max_elapsed_micros),
                ),
                (
                    "avg_elapsed_micros".to_string(),
                    Value::Int(avg_i64(record.total_elapsed_micros, record.execution_count)),
                ),
                (
                    "total_row_count".to_string(),
                    Value::Int(record.total_row_count),
                ),
                (
                    "last_seen_unix_micros".to_string(),
                    Value::Int(record.last_seen_unix_micros),
                ),
                (
                    "last_elapsed_micros".to_string(),
                    Value::Int(record.last_elapsed_micros),
                ),
                (
                    "last_row_count".to_string(),
                    Value::Int(record.last_row_count),
                ),
                ("last_success".to_string(), Value::Bool(record.last_success)),
                (
                    "last_error".to_string(),
                    record
                        .last_error
                        .as_ref()
                        .map(|error| Value::String(error.clone()))
                        .unwrap_or(Value::Null),
                ),
            ])
        })
        .collect()
}

fn predicate_matches(predicate: &SqlPredicate, row: &Row) -> bool {
    match &predicate.kind {
        ExprKind::And(left, right) => predicate_matches(left, row) && predicate_matches(right, row),
        ExprKind::Or(left, right) => predicate_matches(left, row) || predicate_matches(right, row),
        ExprKind::Not(inner) => !predicate_matches(inner, row),
        ExprKind::Compare { left, op, right } => system_expression_value(left, row)
            .zip(system_expression_value(right, row))
            .is_some_and(|(left, right)| compare_values(left, *op, right)),
        ExprKind::InList {
            left,
            values,
            negated,
        } => {
            let matched = system_expression_value(left, row).is_some_and(|left| {
                values.iter().any(|value| {
                    system_expression_value(value, row).is_some_and(|value| left == value)
                })
            });
            matched != *negated
        }
        ExprKind::Like {
            left,
            pattern,
            case_insensitive,
            negated,
            escape,
        } => {
            let Some((Value::String(value), Value::String(pattern))) =
                system_expression_value(left, row).zip(system_expression_value(pattern, row))
            else {
                return false;
            };
            hawdb_sql::sql_like_matches(value, pattern, *escape, *case_insensitive)
                .is_ok_and(|matched| matched != *negated)
        }
        ExprKind::IsNull {
            expression,
            negated,
        } => {
            let matched = system_expression_value(expression, row)
                .is_none_or(|value| matches!(value, Value::Null));
            matched != *negated
        }
        _ => false,
    }
}

fn system_expression_value<'a>(expression: &'a Expr, row: &'a Row) -> Option<&'a Value> {
    match &expression.kind {
        ExprKind::Column(column) => row.get(&column.name),
        ExprKind::Value(SqlValue::Literal(value)) => Some(value),
        _ => None,
    }
}

fn compare_values(left: &Value, op: SqlComparisonOp, right: &Value) -> bool {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return false;
    }
    match op {
        SqlComparisonOp::Eq => left == right,
        SqlComparisonOp::NotEq => left != right,
        SqlComparisonOp::Lt => left < right,
        SqlComparisonOp::Lte => left <= right,
        SqlComparisonOp::Gt => left > right,
        SqlComparisonOp::Gte => left >= right,
    }
}

fn compare_ordered_rows(left: &Row, right: &Row, order_by: &[SqlOrderItem]) -> Ordering {
    for item in order_by {
        let column = item
            .expression
            .as_column()
            .expect("system ORDER BY columns were validated");
        let ordering = left.get(&column.name).cmp(&right.get(&column.name));
        let ordering = match item.direction {
            SqlOrderDirection::Asc => ordering,
            SqlOrderDirection::Desc => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn system_table(select: &SelectStatement) -> Result<SystemTable> {
    match (select.from.schema.as_deref(), select.from.name.as_str()) {
        (Some("system"), "tables") => Ok(SystemTable::Tables),
        (Some("system"), "properties") => Ok(SystemTable::Properties),
        (Some("system"), "indexes") => Ok(SystemTable::Indexes),
        (Some("system"), "constraints") => Ok(SystemTable::Constraints),
        (Some("system"), "append_tables") => Ok(SystemTable::AppendTables),
        (Some("system"), "append_storage") => Ok(SystemTable::AppendStorage),
        (Some("system"), "runtime_status") => Ok(SystemTable::RuntimeStatus),
        (Some("system"), "runtime_capabilities") => Ok(SystemTable::RuntimeCapabilities),
        (Some("system"), "graph_statistics") => Ok(SystemTable::GraphStatistics),
        (Some("system"), "projected_graphs") => Ok(SystemTable::ProjectedGraphs),
        (Some("system"), "search_projection_changefeed") => {
            Ok(SystemTable::SearchProjectionChangefeed)
        }
        (Some("system"), "plan_cache") => Ok(SystemTable::PlanCache),
        (Some("system"), "slow_queries") => Ok(SystemTable::SlowQueries),
        (Some("system"), "statement_summary") => Ok(SystemTable::StatementSummary),
        (Some("information_schema"), "tables") => Ok(SystemTable::InformationSchemaTables),
        (Some("information_schema"), "columns") => Ok(SystemTable::InformationSchemaColumns),
        (Some("pg_catalog"), "pg_tables") => Ok(SystemTable::PgTables),
        (Some("pg_catalog"), "pg_indexes") => Ok(SystemTable::PgIndexes),
        (None, "pg_tables") => Ok(SystemTable::PgTables),
        (None, "pg_indexes") => Ok(SystemTable::PgIndexes),
        _ => Err(HawDBError::Semantic(format!(
            "unknown SQL virtual catalog table {}",
            format_table_name(select)
        ))),
    }
}

fn validate_projection(table: SystemTable, projection: &[SelectProjection]) -> Result<()> {
    for projection in projection {
        match projection {
            SelectProjection::Wildcard => {}
            SelectProjection::Expression {
                expression:
                    Expr {
                        kind: ExprKind::Column(name),
                        ..
                    },
                ..
            } => validate_column(table, name)?,
            SelectProjection::Expression { .. } => {
                return Err(HawDBError::Semantic(
                    "system SQL aggregate expressions are not supported".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_predicate_columns(table: SystemTable, predicate: Option<&SqlPredicate>) -> Result<()> {
    let mut result = Ok(());
    if let Some(predicate) = predicate {
        predicate.visit(&mut |expression| {
            if result.is_ok()
                && let Some(column) = expression.as_column()
            {
                result = validate_column(table, column);
            }
        });
    }
    result
}

fn validate_order_columns(table: SystemTable, order_by: &[SqlOrderItem]) -> Result<()> {
    for item in order_by {
        validate_column(table, item.expression.require_column()?)?;
    }
    Ok(())
}

fn validate_column(table: SystemTable, column: &SqlColumnRef) -> Result<()> {
    if let Some(qualifier) = &column.qualifier {
        let table_name = match table {
            SystemTable::Tables => "tables",
            SystemTable::Properties => "properties",
            SystemTable::Indexes => "indexes",
            SystemTable::Constraints => "constraints",
            SystemTable::AppendTables => "append_tables",
            SystemTable::AppendStorage => "append_storage",
            SystemTable::RuntimeStatus => "runtime_status",
            SystemTable::RuntimeCapabilities => "runtime_capabilities",
            SystemTable::GraphStatistics => "graph_statistics",
            SystemTable::ProjectedGraphs => "projected_graphs",
            SystemTable::SearchProjectionChangefeed => "search_projection_changefeed",
            SystemTable::PlanCache => "plan_cache",
            SystemTable::SlowQueries => "slow_queries",
            SystemTable::StatementSummary => "statement_summary",
            SystemTable::InformationSchemaTables => "tables",
            SystemTable::InformationSchemaColumns => "columns",
            SystemTable::PgTables => "pg_tables",
            SystemTable::PgIndexes => "pg_indexes",
        };
        if qualifier != table_name {
            return Err(HawDBError::Semantic(format!(
                "unknown SQL column qualifier {qualifier}"
            )));
        }
    }
    if table_columns(table).contains(&column.name.as_str()) {
        Ok(())
    } else {
        Err(HawDBError::Semantic(format!(
            "unknown SQL column {}",
            column.name
        )))
    }
}

fn table_columns(table: SystemTable) -> &'static [&'static str] {
    match table {
        SystemTable::Tables => &["table_id", "table_name", "table_kind", "state"],
        SystemTable::Properties => &[
            "property_id",
            "table_id",
            "table_name",
            "table_kind",
            "property_name",
            "value_type",
            "nullable",
            "state",
        ],
        SystemTable::Indexes => &[
            "index_id",
            "subject_kind",
            "subject_id",
            "subject_name",
            "index_kind",
            "property_names",
        ],
        SystemTable::Constraints => &[
            "constraint_id",
            "subject_kind",
            "subject_id",
            "subject_name",
            "property_name",
            "constraint_kind",
        ],
        SystemTable::AppendTables => &[
            "table_name",
            "storage_mode",
            "partition_key",
            "order_key",
            "order_mode",
            "generated_order_watermark",
            "column_count",
        ],
        SystemTable::AppendStorage => &[
            "canonical_segment_count",
            "canonical_segment_bytes",
            "resident_segment_payload_bytes",
            "resident_descriptor_count",
            "live_rows",
            "live_payload_bytes",
        ],
        SystemTable::RuntimeStatus => &[
            "commit_epoch",
            "read_only",
            "max_read_result_rows",
            "max_read_result_payload_bytes",
            "storage_residency_mode",
            "relational_index_mode",
        ],
        SystemTable::RuntimeCapabilities => &["capability", "enabled"],
        SystemTable::GraphStatistics => &[
            "statistic_kind",
            "computed_at_commit_epoch",
            "advanced_statistics_complete",
            "histogram_sample_limit",
            "label_name",
            "relationship_type_name",
            "source_label_name",
            "target_label_name",
            "depth",
            "property_name",
            "index_id",
            "index_kind",
            "index_properties",
            "index_size",
            "unique_values",
            "sample_size",
            "updates_since_sample",
            "stale",
            "count",
            "histogram",
            "sampled",
        ],
        SystemTable::ProjectedGraphs => &[
            "name",
            "node_labels",
            "relationship_types",
            "projection_epoch",
            "commit_epoch",
            "node_count",
            "edge_count",
            "reusable",
        ],
        SystemTable::SearchProjectionChangefeed => &[
            "graph_commit_epoch",
            "resume_floor_commit_epoch",
            "oldest_retained_mutation_id",
            "newest_retained_mutation_id",
            "retained_mutation_count",
            "restart_recoverable",
        ],
        SystemTable::PlanCache => &["metric", "value"],
        SystemTable::SlowQueries => &[
            "sequence",
            "query_language",
            "statement_kind",
            "query_digest",
            "query_text_hash",
            "query_text",
            "started_unix_micros",
            "elapsed_micros",
            "row_count",
            "success",
            "error",
            "slow_log_candidate",
            "access_control_policy_epoch",
        ],
        SystemTable::StatementSummary => &[
            "digest",
            "query_language",
            "query_text",
            "sample_query_text_hash",
            "statement_kind",
            "execution_count",
            "success_count",
            "error_count",
            "total_elapsed_micros",
            "max_elapsed_micros",
            "avg_elapsed_micros",
            "total_row_count",
            "last_seen_unix_micros",
            "last_elapsed_micros",
            "last_row_count",
            "last_success",
            "last_error",
        ],
        SystemTable::InformationSchemaTables => &[
            "table_catalog",
            "table_schema",
            "table_name",
            "table_type",
            "self_referencing_column_name",
            "reference_generation",
            "user_defined_type_catalog",
            "user_defined_type_schema",
            "user_defined_type_name",
            "is_insertable_into",
            "is_typed",
            "commit_action",
        ],
        SystemTable::InformationSchemaColumns => &[
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position",
            "column_default",
            "is_nullable",
            "data_type",
            "character_maximum_length",
            "character_octet_length",
            "numeric_precision",
            "numeric_precision_radix",
            "numeric_scale",
            "datetime_precision",
            "interval_type",
            "interval_precision",
            "character_set_catalog",
            "character_set_schema",
            "character_set_name",
            "collation_catalog",
            "collation_schema",
            "collation_name",
            "domain_catalog",
            "domain_schema",
            "domain_name",
            "udt_catalog",
            "udt_schema",
            "udt_name",
            "scope_catalog",
            "scope_schema",
            "scope_name",
            "maximum_cardinality",
            "dtd_identifier",
            "is_self_referencing",
            "is_identity",
            "identity_generation",
            "identity_start",
            "identity_increment",
            "identity_maximum",
            "identity_minimum",
            "identity_cycle",
            "is_generated",
            "generation_expression",
            "is_updatable",
        ],
        SystemTable::PgTables => &[
            "schemaname",
            "tablename",
            "tableowner",
            "tablespace",
            "hasindexes",
            "hasrules",
            "hastriggers",
            "rowsecurity",
        ],
        SystemTable::PgIndexes => &[
            "schemaname",
            "tablename",
            "indexname",
            "tablespace",
            "indexdef",
        ],
    }
}

fn format_table_name(select: &SelectStatement) -> String {
    match &select.from.schema {
        Some(schema) => format!("{schema}.{}", select.from.name),
        None => select.from.name.clone(),
    }
}

fn option_usize_value(value: Option<usize>) -> Value {
    value.map(usize_value).unwrap_or(Value::Null)
}

fn option_i64_value(value: Option<i64>) -> Value {
    value.map(Value::Int).unwrap_or(Value::Null)
}

fn option_u64_value(value: Option<u64>) -> Value {
    value.map(u64_value).unwrap_or(Value::Null)
}

fn usize_value(value: usize) -> Value {
    Value::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

fn u32_value(value: u32) -> Value {
    Value::Int(i64::from(value))
}

fn u64_value(value: u64) -> Value {
    Value::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

const fn storage_residency_mode_name(mode: StorageResidencyMode) -> &'static str {
    match mode {
        StorageResidencyMode::Auto => "auto",
        StorageResidencyMode::Materialized => "materialized",
        StorageResidencyMode::OutOfCore => "out_of_core",
    }
}

const fn relational_index_mode_name(mode: RelationalIndexMode) -> &'static str {
    match mode {
        RelationalIndexMode::Materialized => "materialized",
        RelationalIndexMode::Shadow => "shadow",
        RelationalIndexMode::DemandPaged => "demand_paged",
        RelationalIndexMode::Authoritative => "authoritative",
    }
}

fn avg_i64(total: i64, count: i64) -> i64 {
    if count <= 0 {
        0
    } else {
        total / count
    }
}

fn saturating_i64_from_u128(value: u128) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn unix_now_micros() -> i64 {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros())
        .unwrap_or(0);
    saturating_i64_from_u128(micros)
}

fn truncate_utf8(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input.to_string();
    }
    let mut end = max_bytes;
    while !input.is_char_boundary(end) {
        end -= 1;
    }
    input[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct TestStore;

    impl SystemSqlStore for TestStore {
        fn commit_epoch(&self) -> u64 {
            0
        }

        fn append_storage_residency_report(&self) -> AppendStorageResidencyReport {
            AppendStorageResidencyReport::default()
        }

        fn statistics(&self, _catalog: &Catalog) -> GraphStatistics {
            GraphStatistics::default()
        }

        fn projected_graph_statuses(&self) -> Vec<ProjectedGraphStatus> {
            Vec::new()
        }

        fn search_projection_changefeed_status(&self) -> SearchProjectionChangefeedStatus {
            SearchProjectionChangefeedStatus {
                graph_commit_epoch: 0,
                resume_floor_commit_epoch: 0,
                oldest_retained_mutation_id: None,
                newest_retained_mutation_id: None,
                first_rebuild_required_mutation_id: None,
                retained_mutation_count: 0,
                retained_bytes: 0,
                max_retained_bytes: None,
                restart_recoverable: true,
            }
        }
    }

    fn default_runtime() -> SystemRuntimeSnapshot {
        SystemRuntimeSnapshot::new(
            false,
            Some(512),
            Some(4 * 1024 * 1024),
            StorageResidencyMode::Auto,
            RelationalIndexMode::default(),
            RuntimeCapabilities::default(),
        )
    }

    #[test]
    fn predicate_binding_preserves_source_spans_and_system_null_behavior() {
        let SqlStatement::Select(select) = hawdb_sql::prepare_postgres_sql(
            "SELECT value FROM system.plan_cache WHERE \
             (value IN ($1, $2) OR NOT value = $3) AND \
             (metric LIKE $4 OR value IS NULL)",
        )
        .unwrap()
        .statement
        else {
            panic!("expected SELECT");
        };
        let predicate = select.selection.unwrap();
        let mut before = Vec::new();
        predicate.visit(&mut |node| before.push(node.span));
        let bound = bind_predicate(
            predicate.clone(),
            &[
                Value::Null,
                Value::Int(5),
                Value::Null,
                Value::String("hit%".to_owned()),
            ],
        )
        .unwrap();
        let mut after = Vec::new();
        let mut parameters = Vec::new();
        bound.visit(&mut |node| {
            after.push(node.span);
            if let ExprKind::Value(SqlValue::Parameter(position)) = &node.kind {
                parameters.push(*position);
            }
        });
        assert_eq!(after, before);
        assert!(parameters.is_empty());
        assert!(predicate_matches(
            &bound,
            &BTreeMap::from([
                ("value".to_owned(), Value::Null),
                ("metric".to_owned(), Value::String("misses".to_owned())),
            ])
        ));
        assert!(bind_predicate(
            predicate,
            &[
                Value::Null,
                Value::Int(5),
                Value::Null,
                Value::String("dangling\\".to_owned()),
            ]
        )
        .unwrap_err()
        .to_string()
        .contains("ends with its escape"));
    }

    #[test]
    fn query_plan_cache_virtual_table_with_predicate_and_projection() {
        let catalog = Catalog::default();
        let store = TestStore;
        let relational_state = RelationalState::default();
        let append_state = AppendState::default();
        let stats = PlanCacheStats {
            max_entries: Some(128),
            entries: 3,
            hits: 5,
            misses: 7,
            admissions: 4,
            disabled_misses: 0,
            bypasses: 2,
            evictions: 1,
            memory_pressure_events: 1,
        };

        let output = query_sql(
            "SELECT value FROM system.plan_cache WHERE metric = 'hits'",
            None,
            None,
            &SystemSqlContext {
                catalog: &catalog,
                store: &store,
                relational_state: &relational_state,
                append_state: &append_state,
                runtime: default_runtime(),
                plan_cache_stats: &stats,
                slow_queries: &[],
                statement_summaries: &[],
            },
        )
        .expect("system plan cache query");

        assert_eq!(
            output.rows,
            vec![BTreeMap::from([("value".to_string(), Value::Int(5))])]
        );
    }

    #[test]
    fn query_system_table_with_postgres_parameters() {
        let catalog = Catalog::default();
        let store = TestStore;
        let relational_state = RelationalState::default();
        let append_state = AppendState::default();
        let stats = PlanCacheStats {
            max_entries: Some(128),
            entries: 3,
            hits: 5,
            misses: 7,
            admissions: 4,
            disabled_misses: 0,
            bypasses: 2,
            evictions: 1,
            memory_pressure_events: 1,
        };
        let context = SystemSqlContext {
            catalog: &catalog,
            store: &store,
            relational_state: &relational_state,
            append_state: &append_state,
            runtime: default_runtime(),
            plan_cache_stats: &stats,
            slow_queries: &[],
            statement_summaries: &[],
        };

        let output = query_sql_with_params(
            "SELECT metric, value FROM system.plan_cache \
             WHERE metric IN ($1, $2) ORDER BY metric ASC LIMIT $3 OFFSET $4",
            &[
                Value::String("hits".to_string()),
                Value::String("misses".to_string()),
                Value::Int(1),
                Value::Int(1),
            ],
            None,
            None,
            &context,
        )
        .expect("parameterized system table query");

        assert_eq!(
            output.rows,
            vec![BTreeMap::from([
                ("metric".to_string(), Value::String("misses".to_string())),
                ("value".to_string(), Value::Int(7)),
            ])]
        );
    }

    #[test]
    fn query_system_table_rejects_parameter_contract_mismatch() {
        let catalog = Catalog::default();
        let store = TestStore;
        let relational_state = RelationalState::default();
        let append_state = AppendState::default();
        let stats = PlanCacheStats {
            max_entries: None,
            entries: 0,
            hits: 0,
            misses: 0,
            admissions: 0,
            disabled_misses: 0,
            bypasses: 0,
            evictions: 0,
            memory_pressure_events: 0,
        };
        let context = SystemSqlContext {
            catalog: &catalog,
            store: &store,
            relational_state: &relational_state,
            append_state: &append_state,
            runtime: default_runtime(),
            plan_cache_stats: &stats,
            slow_queries: &[],
            statement_summaries: &[],
        };

        let missing = query_sql_with_params(
            "SELECT * FROM system.plan_cache WHERE metric = $1",
            &[],
            None,
            None,
            &context,
        )
        .expect_err("missing parameter must fail");
        assert!(missing.to_string().contains("requires 1 parameters"));

        let invalid_bound = query_sql_with_params(
            "SELECT * FROM system.plan_cache LIMIT $1",
            &[Value::String("one".to_string())],
            None,
            None,
            &context,
        )
        .expect_err("non-integer LIMIT must fail");
        assert!(invalid_bound
            .to_string()
            .contains("must be a non-negative integer"));
    }

    #[test]
    fn query_slow_queries_pushes_filter_order_and_limit_into_scan() {
        let catalog = Catalog::default();
        let store = TestStore;
        let relational_state = RelationalState::default();
        let append_state = AppendState::default();
        let stats = PlanCacheStats {
            max_entries: None,
            entries: 0,
            hits: 0,
            misses: 0,
            admissions: 0,
            disabled_misses: 0,
            bypasses: 0,
            evictions: 0,
            memory_pressure_events: 0,
        };
        let records = vec![
            SlowQueryRecord {
                sequence: 1,
                query_language: "cypher".to_string(),
                statement_kind: "match_return".to_string(),
                query_digest: "q1:first".to_string(),
                query_text_hash: "t1:first".to_string(),
                query_text: "MATCH (m:Memory) RETURN m".to_string(),
                started_unix_micros: 10,
                elapsed_micros: 200,
                row_count: 1,
                success: true,
                error: None,
                slow_log_candidate: false,
                access_control_policy_epoch: None,
                vector_execution_reports: Vec::new(),
            },
            SlowQueryRecord {
                sequence: 2,
                query_language: "cypher".to_string(),
                statement_kind: "match_return".to_string(),
                query_digest: "q1:second".to_string(),
                query_text_hash: "t1:second".to_string(),
                query_text: "MATCH (m:Memory) RETURN m ORDER BY m.id".to_string(),
                started_unix_micros: 20,
                elapsed_micros: 500,
                row_count: 2,
                success: true,
                error: None,
                slow_log_candidate: true,
                access_control_policy_epoch: None,
                vector_execution_reports: Vec::new(),
            },
            SlowQueryRecord {
                sequence: 3,
                query_language: "cypher".to_string(),
                statement_kind: "match_return".to_string(),
                query_digest: "q1:third".to_string(),
                query_text_hash: "t1:third".to_string(),
                query_text: "MATCH (m:Memory {id: 'x'}) RETURN m".to_string(),
                started_unix_micros: 30,
                elapsed_micros: 300,
                row_count: 1,
                success: true,
                error: None,
                slow_log_candidate: true,
                access_control_policy_epoch: None,
                vector_execution_reports: Vec::new(),
            },
        ];

        let output = query_sql(
            "SELECT sequence, elapsed_micros FROM system.slow_queries \
             WHERE slow_log_candidate = true \
             ORDER BY elapsed_micros DESC LIMIT 1",
            None,
            None,
            &SystemSqlContext {
                catalog: &catalog,
                store: &store,
                relational_state: &relational_state,
                append_state: &append_state,
                runtime: default_runtime(),
                plan_cache_stats: &stats,
                slow_queries: &records,
                statement_summaries: &[],
            },
        )
        .expect("system slow query scan");

        assert_eq!(
            output.rows,
            vec![BTreeMap::from([
                ("elapsed_micros".to_string(), Value::Int(500)),
                ("sequence".to_string(), Value::Int(2)),
            ])]
        );
    }

    #[test]
    fn query_catalog_virtual_tables_expose_schema_without_typed_getters() {
        let mut catalog = Catalog::default();
        let store = TestStore;
        let relational_state = RelationalState::default();
        let append_state = AppendState::default();
        let memory_label = catalog.get_or_create_label("Memory");
        let memory_table = catalog.get_or_create_table(TableKind::Node, "Memory");
        catalog.get_or_create_property(memory_table, "id", PropertyType::String, false);
        catalog.get_or_create_property_index_with_kind(memory_label, "id", IndexKind::Equality);
        catalog.get_or_create_unique_constraint(memory_label, "id");
        let stats = PlanCacheStats {
            max_entries: None,
            entries: 0,
            hits: 0,
            misses: 0,
            admissions: 0,
            disabled_misses: 0,
            bypasses: 0,
            evictions: 0,
            memory_pressure_events: 0,
        };
        let context = SystemSqlContext {
            catalog: &catalog,
            store: &store,
            relational_state: &relational_state,
            append_state: &append_state,
            runtime: default_runtime(),
            plan_cache_stats: &stats,
            slow_queries: &[],
            statement_summaries: &[],
        };

        let tables = query_sql(
            "SELECT table_id, table_name, table_kind, state FROM system.tables",
            None,
            None,
            &context,
        )
        .expect("system tables query");
        assert_eq!(
            tables.rows,
            vec![BTreeMap::from([
                ("state".to_string(), Value::String("public".to_string())),
                ("table_id".to_string(), Value::Int(0)),
                ("table_kind".to_string(), Value::String("node".to_string()),),
                (
                    "table_name".to_string(),
                    Value::String("Memory".to_string()),
                ),
            ])]
        );

        let properties = query_sql(
            "SELECT table_name, property_name, value_type, nullable \
             FROM system.properties WHERE table_name = 'Memory'",
            None,
            None,
            &context,
        )
        .expect("system properties query");
        assert_eq!(
            properties.rows,
            vec![BTreeMap::from([
                ("nullable".to_string(), Value::Bool(false)),
                ("property_name".to_string(), Value::String("id".to_string()),),
                (
                    "table_name".to_string(),
                    Value::String("Memory".to_string()),
                ),
                (
                    "value_type".to_string(),
                    Value::String("string".to_string()),
                ),
            ])]
        );

        let indexes = query_sql(
            "SELECT subject_name, index_kind, property_names FROM system.indexes",
            None,
            None,
            &context,
        )
        .expect("system indexes query");
        assert_eq!(
            indexes.rows,
            vec![BTreeMap::from([
                (
                    "index_kind".to_string(),
                    Value::String("equality".to_string()),
                ),
                (
                    "property_names".to_string(),
                    Value::List(vec![Value::String("id".to_string())]),
                ),
                (
                    "subject_name".to_string(),
                    Value::String("Memory".to_string()),
                ),
            ])]
        );

        let constraints = query_sql(
            "SELECT subject_name, property_name, constraint_kind FROM system.constraints",
            None,
            None,
            &context,
        )
        .expect("system constraints query");
        assert_eq!(
            constraints.rows,
            vec![BTreeMap::from([
                (
                    "constraint_kind".to_string(),
                    Value::String("node_property_unique".to_string()),
                ),
                ("property_name".to_string(), Value::String("id".to_string()),),
                (
                    "subject_name".to_string(),
                    Value::String("Memory".to_string()),
                ),
            ])]
        );
    }
}

#[cfg(all(test, feature = "loom-tests"))]
pub(crate) mod loom_tests {
    use super::{SlowQueryCompletion, SlowQueryLog, SlowQueryRecord};
    use hawdb_query::QueryIdentity;
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    #[test]
    fn slow_query_ring_preserves_bounds_under_modeled_concurrent_access() {
        loom::model(|| {
            let log = Arc::new(Mutex::new(SlowQueryLog::new(2)));

            let first_writer = spawn_slow_query_writer(Arc::clone(&log), "first");
            let second_writer = spawn_slow_query_writer(Arc::clone(&log), "second");
            let snapshotter = {
                let log = Arc::clone(&log);
                thread::spawn(move || {
                    let snapshot = log.lock().unwrap().snapshot();
                    assert_snapshot_invariants(&snapshot, 2);
                })
            };

            first_writer.join().unwrap();
            second_writer.join().unwrap();
            snapshotter.join().unwrap();

            let snapshot = log.lock().unwrap().snapshot();
            assert_snapshot_invariants(&snapshot, 2);
            assert_eq!(snapshot.len(), 2);
            assert_eq!(snapshot.last().map(|record| record.sequence), Some(2));
        });
    }

    fn spawn_slow_query_writer(
        log: Arc<Mutex<SlowQueryLog>>,
        query_text: &'static str,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let query_identity = QueryIdentity::new("cypher", query_text);
            log.lock()
                .unwrap()
                .push(SlowQueryRecord::completed(SlowQueryCompletion {
                    query_language: "cypher",
                    statement_kind: "match_return",
                    query_text,
                    query_identity: &query_identity,
                    elapsed_micros: 1,
                    row_count: 1,
                    success: true,
                    error: None,
                    slow_log_candidate: true,
                    access_control_policy_epoch: None,
                    vector_execution_reports: Vec::new(),
                }));
        })
    }

    fn assert_snapshot_invariants(snapshot: &[SlowQueryRecord], capacity: usize) {
        assert!(snapshot.len() <= capacity);
        for pair in snapshot.windows(2) {
            assert!(pair[0].sequence < pair[1].sequence);
        }
    }
}
