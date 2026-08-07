use super::{
    system_sql, Database, QueryOutput, SlowQueryLogExportOptions, SlowQueryLogRecordSummary,
    StatementExecutionContext,
};
use crate::error::{Result, SkeinError};
use crate::relational_sql::{
    compile_relational_statement_sql, execute_relational_query_sql_with_runtime,
};
use crate::telemetry::QueryTelemetry;
use crate::value::Value;
use std::io::Write;
use std::path::Path;

impl Database {
    pub(super) fn record_statement_execution(
        &self,
        query_language: &str,
        query_text: &str,
        statement_kind: &str,
        started: std::time::Instant,
        result: std::result::Result<&QueryOutput, &SkeinError>,
        context: StatementExecutionContext<'_>,
    ) {
        let elapsed_micros = started.elapsed().as_micros();
        let query_identity = skein_query::QueryIdentity::new(query_language, query_text);
        let execution = match result {
            Ok(output) => system_sql::StatementExecution::completed(
                query_language,
                query_text,
                statement_kind,
                &query_identity,
                elapsed_micros,
                output.rows.len(),
            ),
            Err(error) => system_sql::StatementExecution::failed(
                query_language,
                query_text,
                statement_kind,
                &query_identity,
                elapsed_micros,
                error.to_string(),
            ),
        };
        if let Some(telemetry) = &self.telemetry {
            let pipeline = context
                .execution_profile
                .map(|profile| &profile.pipeline_memory_report);
            telemetry.record_query(QueryTelemetry {
                query_language,
                query_digest: query_identity.query_digest(),
                statement_kind,
                success: result.is_ok(),
                elapsed_micros: elapsed_micros.min(u64::MAX as u128) as u64,
                parse_nanos: context.parse_nanos,
                row_count: result.map_or(0, |output| output.rows.len()),
                intermediate_rows: pipeline.map_or(0, |report| report.intermediate_rows),
                intermediate_payload_bytes: pipeline
                    .map_or(0, |report| report.intermediate_payload_bytes),
                output_payload_bytes: pipeline.map_or(0, |report| report.output_payload_bytes),
                steady_resident_bytes: pipeline.and_then(|report| report.steady_resident_bytes),
                peak_resident_bytes: pipeline.and_then(|report| report.peak_resident_bytes),
                total_page_faults: pipeline.and_then(|report| report.total_page_faults),
                minor_page_faults: pipeline.and_then(|report| report.minor_page_faults),
                major_page_faults: pipeline.and_then(|report| report.major_page_faults),
            });
        }
        self.statement_summary.borrow_mut().record(execution);

        let Ok(output) = result else {
            return;
        };
        let slow_log_candidate = elapsed_micros >= self.config.slow_query_log_threshold_micros;
        if !slow_log_candidate {
            return;
        }
        self.slow_query_log
            .borrow_mut()
            .push(system_sql::SlowQueryRecord::completed(
                system_sql::SlowQueryCompletion {
                    query_language,
                    statement_kind,
                    query_text,
                    query_identity: &query_identity,
                    elapsed_micros,
                    row_count: output.rows.len(),
                    success: true,
                    error: None,
                    slow_log_candidate,
                    access_control_policy_epoch: context
                        .access_control
                        .map(super::QueryAccessControlContext::policy_epoch),
                    vector_execution_reports: context
                        .execution_profile
                        .map(|profile| profile.vector_execution_reports.clone())
                        .unwrap_or_default(),
                },
            ));
    }

    pub fn query_sql(&mut self, sql_text: &str) -> Result<QueryOutput> {
        self.query_sql_bounded(sql_text, self.config.max_read_result_rows)
    }

    pub fn query_sql_with_params(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        self.query_sql_with_params_bounded(sql_text, parameters, self.config.max_read_result_rows)
    }

    pub fn query_sql_bounded(
        &mut self,
        sql_text: &str,
        max_rows: Option<usize>,
    ) -> Result<QueryOutput> {
        self.query_sql_with_params_bounded(sql_text, &[], max_rows)
    }

    pub fn query_sql_with_params_bounded(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
        max_rows: Option<usize>,
    ) -> Result<QueryOutput> {
        self.store.ensure_usable()?;
        let max_rows = super::restrictive_query_limit(self.config.max_read_result_rows, max_rows);
        let prepared = skein_sql::prepare_postgres_sql(sql_text)?;
        if matches!(
            &prepared.statement,
            crate::sql::SqlStatement::Select(select)
                if select.from.schema.as_deref() == Some("system")
        ) {
            let plan_cache_stats = self.plan_cache.borrow().stats();
            let slow_queries = self.slow_query_log.borrow().snapshot();
            let statement_summaries = self.statement_summary.borrow().snapshot();
            return system_sql::query_sql_with_params(
                sql_text,
                parameters,
                max_rows,
                self.config.max_read_result_payload_bytes,
                &system_sql::SystemSqlContext {
                    catalog: &self.catalog,
                    plan_cache_stats: &plan_cache_stats,
                    slow_queries: &slow_queries,
                    statement_summaries: &statement_summaries,
                },
            );
        }

        if matches!(
            prepared.statement,
            crate::sql::SqlStatement::Select(_) | crate::sql::SqlStatement::Explain(_)
        ) {
            let output = execute_relational_query_sql_with_runtime(
                sql_text,
                parameters,
                self.store.relational_state(),
                super::relational_query_limits(&self.config, max_rows),
                &self.config.execution_memory,
                None,
            )?;
            return Ok(QueryOutput { rows: output.rows });
        }

        self.ensure_writable()?;
        let transaction =
            compile_relational_statement_sql(sql_text, parameters, self.store.relational_state())?;
        let summary = self
            .store
            .commit_relational_transaction(&mut self.catalog, transaction)?;
        Ok(QueryOutput { rows: summary.rows })
    }

    pub fn slow_query_log_jsonl(&self) -> Result<String> {
        self.slow_query_log_jsonl_with_options(&SlowQueryLogExportOptions::default())
    }

    pub fn slow_query_log_jsonl_with_options(
        &self,
        options: &SlowQueryLogExportOptions,
    ) -> Result<String> {
        system_sql::slow_query_log_jsonl(
            &self.slow_query_log.borrow().snapshot(),
            options.include_query_text,
        )
    }

    pub fn slow_query_log_snapshot(&self) -> Vec<SlowQueryLogRecordSummary> {
        self.slow_query_log
            .borrow()
            .snapshot()
            .iter()
            .map(system_sql::slow_query_record_summary)
            .collect()
    }

    pub fn write_slow_query_log_jsonl(&self, path: impl AsRef<Path>) -> Result<()> {
        self.write_slow_query_log_jsonl_with_options(path, &SlowQueryLogExportOptions::default())
    }

    pub fn write_slow_query_log_jsonl_with_options(
        &self,
        path: impl AsRef<Path>,
        options: &SlowQueryLogExportOptions,
    ) -> Result<()> {
        let jsonl = self.slow_query_log_jsonl_with_options(options)?;
        let mut file = std::fs::File::create(path)?;
        file.write_all(jsonl.as_bytes())?;
        Ok(())
    }
}
