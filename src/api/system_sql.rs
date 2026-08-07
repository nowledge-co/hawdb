use super::{PlanCacheStats, QueryOutput};
use crate::error::{Result, SkeinError};
use crate::executor::Row;
use crate::schema::{
    Catalog, ConstraintKind, ConstraintSubject, IndexKind, PropertyType, SchemaObjectState,
    TableKind,
};
use crate::sql::{
    SelectProjection, SelectStatement, SqlBound, SqlColumnRef, SqlComparisonOp, SqlOrderDirection,
    SqlPredicate, SqlStatement, SqlValue,
};
use crate::value::Value;
use skein_query::QueryIdentity;
use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const DEFAULT_SLOW_QUERY_LOG_CAPACITY: usize = 256;
pub(crate) const DEFAULT_SLOW_QUERY_LOG_THRESHOLD_MICROS: u128 = 300_000;
pub(crate) const DEFAULT_STATEMENT_SUMMARY_CAPACITY: usize = 256;
const MAX_SLOW_QUERY_TEXT_BYTES: usize = 4096;
const MAX_STATEMENT_TEXT_BYTES: usize = 4096;
const MAX_STATEMENT_ERROR_BYTES: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlowQueryRecord {
    pub(crate) sequence: u64,
    pub(crate) query_language: String,
    pub(crate) statement_kind: String,
    pub(crate) query_digest: String,
    pub(crate) query_text_hash: String,
    pub(crate) query_text: String,
    pub(crate) started_unix_micros: i64,
    pub(crate) elapsed_micros: i64,
    pub(crate) row_count: i64,
    pub(crate) success: bool,
    pub(crate) error: Option<String>,
    pub(crate) slow_log_candidate: bool,
    pub(crate) access_control_policy_epoch: Option<u64>,
    pub(crate) vector_execution_reports: Vec<skein_executor::VectorExecutionReport>,
}

pub(crate) struct SlowQueryCompletion<'a> {
    pub(crate) query_language: &'a str,
    pub(crate) statement_kind: &'a str,
    pub(crate) query_text: &'a str,
    pub(crate) query_identity: &'a QueryIdentity,
    pub(crate) elapsed_micros: u128,
    pub(crate) row_count: usize,
    pub(crate) success: bool,
    pub(crate) error: Option<String>,
    pub(crate) slow_log_candidate: bool,
    pub(crate) access_control_policy_epoch: Option<u64>,
    pub(crate) vector_execution_reports: Vec<skein_executor::VectorExecutionReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlowQueryLog {
    capacity: usize,
    next_sequence: u64,
    records: VecDeque<SlowQueryRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatementExecution {
    pub(crate) query_language: String,
    pub(crate) query_text: String,
    pub(crate) statement_kind: String,
    pub(crate) query_digest: String,
    pub(crate) query_text_hash: String,
    pub(crate) elapsed_micros: i64,
    pub(crate) row_count: i64,
    pub(crate) success: bool,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatementSummaryRecord {
    pub(crate) digest: String,
    pub(crate) query_language: String,
    pub(crate) query_text: String,
    pub(crate) sample_query_text_hash: String,
    pub(crate) statement_kind: String,
    pub(crate) execution_count: i64,
    pub(crate) success_count: i64,
    pub(crate) error_count: i64,
    pub(crate) total_elapsed_micros: i64,
    pub(crate) max_elapsed_micros: i64,
    pub(crate) total_row_count: i64,
    pub(crate) last_seen_unix_micros: i64,
    pub(crate) last_elapsed_micros: i64,
    pub(crate) last_row_count: i64,
    pub(crate) last_success: bool,
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatementSummary {
    capacity: usize,
    records: BTreeMap<String, StatementSummaryRecord>,
    insertion_order: VecDeque<String>,
}

pub(crate) struct SystemSqlContext<'a> {
    pub(crate) catalog: &'a Catalog,
    pub(crate) plan_cache_stats: &'a PlanCacheStats,
    pub(crate) slow_queries: &'a [SlowQueryRecord],
    pub(crate) statement_summaries: &'a [StatementSummaryRecord],
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
    order_by: Vec<crate::sql::SqlOrderItem>,
    offset: Option<u64>,
    limit: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SystemTable {
    Tables,
    Properties,
    Indexes,
    Constraints,
    PlanCache,
    SlowQueries,
    StatementSummary,
}

impl SlowQueryRecord {
    pub(crate) fn completed(completion: SlowQueryCompletion<'_>) -> Self {
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
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            next_sequence: 1,
            records: VecDeque::with_capacity(capacity),
        }
    }

    pub(crate) fn push(&mut self, mut record: SlowQueryRecord) {
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

    pub(crate) fn snapshot(&self) -> Vec<SlowQueryRecord> {
        self.records.iter().cloned().collect()
    }
}

pub(crate) fn slow_query_log_jsonl(
    records: &[SlowQueryRecord],
    include_query_text: bool,
) -> Result<String> {
    let mut jsonl = String::new();
    for record in records {
        let line = serde_json::to_string(&slow_query_record_json(record, include_query_text))
            .map_err(|error| {
                SkeinError::Execution(format!("slow query log JSON error: {error}"))
            })?;
        jsonl.push_str(&line);
        jsonl.push('\n');
    }
    Ok(jsonl)
}

pub(crate) fn slow_query_record_summary(
    record: &SlowQueryRecord,
) -> super::SlowQueryLogRecordSummary {
    super::SlowQueryLogRecordSummary {
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
        "protocol": super::SLOW_QUERY_LOG_EVENT_PROTOCOL,
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

fn vector_execution_report_json(
    report: &skein_executor::VectorExecutionReport,
) -> serde_json::Value {
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
    pub(crate) fn completed(
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

    pub(crate) fn failed(
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
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            records: BTreeMap::new(),
            insertion_order: VecDeque::with_capacity(capacity),
        }
    }

    pub(crate) fn record(&mut self, execution: StatementExecution) {
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

    pub(crate) fn snapshot(&self) -> Vec<StatementSummaryRecord> {
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
pub(crate) fn query_sql(
    sql_text: &str,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    context: &SystemSqlContext<'_>,
) -> Result<QueryOutput> {
    query_sql_with_params(sql_text, &[], max_rows, max_payload_bytes, context)
}

pub(crate) fn query_sql_with_params(
    sql_text: &str,
    parameters: &[Value],
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    context: &SystemSqlContext<'_>,
) -> Result<QueryOutput> {
    let logical = plan_sql(sql_text, parameters)?;
    let physical = optimize_sql(logical);
    let rows = execute_sql(physical, context, max_rows)?;
    let payload_bytes = rows.iter().fold(0usize, |total, row| {
        total.saturating_add(crate::executor::map_payload_bytes(row))
    });
    if max_payload_bytes.is_some_and(|limit| payload_bytes > limit) {
        return Err(SkeinError::Execution(format!(
            "SQL query payload uses {payload_bytes} bytes, exceeding max_read_result_payload_bytes {}",
            max_payload_bytes.unwrap_or_default()
        )));
    }
    Ok(QueryOutput { rows })
}

fn plan_sql(sql_text: &str, parameters: &[Value]) -> Result<SqlLogicalPlan> {
    let prepared = skein_sql::prepare_postgres_sql(sql_text)?;
    if prepared.parameters.len() != parameters.len() {
        return Err(SkeinError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.parameters.len(),
            parameters.len()
        )));
    }
    let SqlStatement::Select(select) = prepared.statement else {
        return Err(SkeinError::Semantic(
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
    if select.distinct
        || select.from_alias.is_some()
        || !select.joins.is_empty()
        || !select.group_by.is_empty()
    {
        return Err(SkeinError::Semantic(
            "system SQL does not support DISTINCT, table aliases, joins, or GROUP BY".to_string(),
        ));
    }
    if select
        .order_by
        .iter()
        .any(|item| item.nulls != crate::sql::SqlNullOrder::DialectDefault)
    {
        return Err(SkeinError::Semantic(
            "system SQL does not support explicit NULLS FIRST/LAST".to_string(),
        ));
    }
    Ok(())
}

fn bind_predicate(predicate: SqlPredicate, parameters: &[Value]) -> Result<SqlPredicate> {
    Ok(match predicate {
        SqlPredicate::And(left, right) => SqlPredicate::And(
            Box::new(bind_predicate(*left, parameters)?),
            Box::new(bind_predicate(*right, parameters)?),
        ),
        SqlPredicate::Or(left, right) => SqlPredicate::Or(
            Box::new(bind_predicate(*left, parameters)?),
            Box::new(bind_predicate(*right, parameters)?),
        ),
        SqlPredicate::Not(inner) => {
            SqlPredicate::Not(Box::new(bind_predicate(*inner, parameters)?))
        }
        SqlPredicate::Compare { left, op, right } => SqlPredicate::Compare {
            left,
            op,
            right: SqlValue::Literal(bind_value(right, parameters)?),
        },
        SqlPredicate::CompareColumns { left, op, right } => {
            SqlPredicate::CompareColumns { left, op, right }
        }
        SqlPredicate::InList {
            left,
            values,
            negated,
        } => SqlPredicate::InList {
            left,
            values: values
                .into_iter()
                .map(|value| bind_value(value, parameters).map(SqlValue::Literal))
                .collect::<Result<Vec<_>>>()?,
            negated,
        },
        SqlPredicate::IsNull { column, negated } => SqlPredicate::IsNull { column, negated },
    })
}

fn bind_value(value: SqlValue, parameters: &[Value]) -> Result<Value> {
    match value {
        SqlValue::Literal(value) => Ok(value),
        SqlValue::Parameter(position) => parameters.get(position - 1).cloned().ok_or_else(|| {
            SkeinError::Semantic(format!("missing PostgreSQL parameter ${position}"))
        }),
    }
}

fn bind_bound(bound: SqlBound, parameters: &[Value], name: &str) -> Result<u64> {
    match bound {
        SqlBound::Literal(value) => Ok(value),
        SqlBound::Parameter(position) => match parameters.get(position - 1) {
            Some(Value::Int(value)) if *value >= 0 => Ok(*value as u64),
            Some(_) => Err(SkeinError::Semantic(format!(
                "PostgreSQL {name} parameter ${position} must be a non-negative integer"
            ))),
            None => Err(SkeinError::Semantic(format!(
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

fn execute_sql(
    physical: SqlPhysicalPlan,
    context: &SystemSqlContext<'_>,
    max_rows: Option<usize>,
) -> Result<Vec<Row>> {
    match physical {
        SqlPhysicalPlan::SystemTableScanExec(scan) => {
            execute_system_table_scan(scan, context, max_rows)
        }
    }
}

fn execute_system_table_scan(
    scan: SystemTableScan,
    context: &SystemSqlContext<'_>,
    max_rows: Option<usize>,
) -> Result<Vec<Row>> {
    let mut rows = match scan.table {
        SystemTable::Tables => table_rows(context.catalog),
        SystemTable::Properties => property_rows(context.catalog),
        SystemTable::Indexes => index_rows(context.catalog),
        SystemTable::Constraints => constraint_rows(context.catalog),
        SystemTable::PlanCache => plan_cache_rows(context.plan_cache_stats),
        SystemTable::SlowQueries => slow_query_rows(context.slow_queries),
        SystemTable::StatementSummary => statement_summary_rows(context.statement_summaries),
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
        return Err(SkeinError::Execution(format!(
                "SQL query returned more than {max_rows} rows, exceeding max_read_result_rows {max_rows}"
            )));
    }

    project_rows(rows, &scan.projection)
}

fn effective_limit(query_limit: Option<u64>, max_rows: Option<usize>) -> Result<Option<usize>> {
    let query_limit = query_limit
        .map(|limit| {
            usize::try_from(limit)
                .map_err(|_| SkeinError::Semantic("SQL LIMIT is too large".to_string()))
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
                    let SelectProjection::Column { name, alias } = projection else {
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
    match predicate {
        SqlPredicate::And(left, right) => {
            predicate_matches(left, row) && predicate_matches(right, row)
        }
        SqlPredicate::Or(left, right) => {
            predicate_matches(left, row) || predicate_matches(right, row)
        }
        SqlPredicate::Not(inner) => !predicate_matches(inner, row),
        SqlPredicate::Compare { left, op, right } => {
            row.get(&left.name).is_some_and(|left_value| match right {
                SqlValue::Literal(right) => compare_values(left_value, *op, right),
                SqlValue::Parameter(_) => false,
            })
        }
        SqlPredicate::CompareColumns { left, op, right } => row
            .get(&left.name)
            .zip(row.get(&right.name))
            .is_some_and(|(left_value, right_value)| compare_values(left_value, *op, right_value)),
        SqlPredicate::InList {
            left,
            values,
            negated,
        } => {
            let matched = row.get(&left.name).is_some_and(|left_value| {
                values.iter().any(|value| match value {
                    SqlValue::Literal(value) => left_value == value,
                    SqlValue::Parameter(_) => false,
                })
            });
            if *negated {
                !matched
            } else {
                matched
            }
        }
        SqlPredicate::IsNull { column, negated } => {
            let matched = row
                .get(&column.name)
                .is_none_or(|value| matches!(value, Value::Null));
            if *negated {
                !matched
            } else {
                matched
            }
        }
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

fn compare_ordered_rows(
    left: &Row,
    right: &Row,
    order_by: &[crate::sql::SqlOrderItem],
) -> Ordering {
    for item in order_by {
        let ordering = left
            .get(&item.column.name)
            .cmp(&right.get(&item.column.name));
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
        (Some("system"), "plan_cache") => Ok(SystemTable::PlanCache),
        (Some("system"), "slow_queries") => Ok(SystemTable::SlowQueries),
        (Some("system"), "statement_summary") => Ok(SystemTable::StatementSummary),
        _ => Err(SkeinError::Semantic(format!(
            "unknown SQL system table {}",
            format_table_name(select)
        ))),
    }
}

fn validate_projection(table: SystemTable, projection: &[SelectProjection]) -> Result<()> {
    for projection in projection {
        match projection {
            SelectProjection::Wildcard => {}
            SelectProjection::Column { name, .. } => validate_column(table, name)?,
            SelectProjection::Expression { .. } => {
                return Err(SkeinError::Semantic(
                    "system SQL aggregate expressions are not supported".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_predicate_columns(table: SystemTable, predicate: Option<&SqlPredicate>) -> Result<()> {
    let Some(predicate) = predicate else {
        return Ok(());
    };
    match predicate {
        SqlPredicate::And(left, right) | SqlPredicate::Or(left, right) => {
            validate_predicate_columns(table, Some(left))?;
            validate_predicate_columns(table, Some(right))
        }
        SqlPredicate::Not(inner) => validate_predicate_columns(table, Some(inner)),
        SqlPredicate::Compare { left, .. }
        | SqlPredicate::InList { left, .. }
        | SqlPredicate::IsNull { column: left, .. } => validate_column(table, left),
        SqlPredicate::CompareColumns { left, right, .. } => {
            validate_column(table, left)?;
            validate_column(table, right)
        }
    }
}

fn validate_order_columns(table: SystemTable, order_by: &[crate::sql::SqlOrderItem]) -> Result<()> {
    for item in order_by {
        validate_column(table, &item.column)?;
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
            SystemTable::PlanCache => "plan_cache",
            SystemTable::SlowQueries => "slow_queries",
            SystemTable::StatementSummary => "statement_summary",
        };
        if qualifier != table_name {
            return Err(SkeinError::Semantic(format!(
                "unknown SQL column qualifier {qualifier}"
            )));
        }
    }
    if table_columns(table).contains(&column.name.as_str()) {
        Ok(())
    } else {
        Err(SkeinError::Semantic(format!(
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

fn usize_value(value: usize) -> Value {
    Value::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

fn u32_value(value: u32) -> Value {
    Value::Int(i64::from(value))
}

fn u64_value(value: u64) -> Value {
    Value::Int(i64::try_from(value).unwrap_or(i64::MAX))
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

    #[test]
    fn query_plan_cache_virtual_table_with_predicate_and_projection() {
        let catalog = Catalog::default();
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
        let memory_label = catalog.get_or_create_label("Memory");
        let memory_table = catalog.get_or_create_table(TableKind::Node, "Memory");
        catalog.get_or_create_property(
            memory_table,
            "id",
            crate::schema::PropertyType::String,
            false,
        );
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
    use loom::sync::{Arc, Mutex};
    use loom::thread;
    use skein_query::QueryIdentity;

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
