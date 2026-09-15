use super::{
    system_sql, Database, QueryOutput, QueryStreamOptions, SharedState, SlowQueryLogExportOptions,
    SlowQueryLogRecordSummary, StatementExecutionContext,
};
use crate::error::{Result, SkeinError};
use crate::executor;
use crate::relational_sql::{
    compile_append_explain_sql, compile_append_select_sql, compile_append_statement_sql,
    compile_relational_statement_sql_with_result, format_append_explain, project_append_rows,
};
use crate::sql::SqlStatement;
use crate::telemetry::{QueryTelemetry, TelemetrySink};
use crate::value::Value;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

pub(super) fn sql_statement_kind(statement: &SqlStatement) -> &'static str {
    match statement {
        SqlStatement::Select(_) => "select",
        SqlStatement::Insert(_) => "insert",
        SqlStatement::Update(_) => "update",
        SqlStatement::Delete(_) => "delete",
        SqlStatement::CreateTable(_) => "create_table",
        SqlStatement::CreateIndex(_) => "create_index",
        SqlStatement::AlterTableAddColumn(_) => "alter_table_add_column",
        SqlStatement::Explain(_) => "explain",
    }
}

// A snapshot read can finish recording after releasing the commit sequencer.
// Only the existing bounded observation containers and configured sink are shared.
pub(super) struct StatementRecorder {
    slow_query_log: Arc<SharedState<system_sql::SlowQueryLog>>,
    statement_summary: Arc<SharedState<system_sql::StatementSummary>>,
    slow_query_log_threshold_micros: u128,
    telemetry: Option<Arc<dyn TelemetrySink>>,
}

// Ordinary Database calls borrow their recording target without cloning handles.
struct StatementRecordingTarget<'a> {
    slow_query_log: &'a SharedState<system_sql::SlowQueryLog>,
    statement_summary: &'a SharedState<system_sql::StatementSummary>,
    slow_query_log_threshold_micros: u128,
    telemetry: Option<&'a dyn TelemetrySink>,
}

impl StatementRecordingTarget<'_> {
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
        if let Some(telemetry) = self.telemetry {
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
        let slow_log_candidate = elapsed_micros >= self.slow_query_log_threshold_micros;
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
}

impl StatementRecorder {
    pub(super) fn record_statement_execution(
        &self,
        query_language: &str,
        query_text: &str,
        statement_kind: &str,
        started: std::time::Instant,
        result: std::result::Result<&QueryOutput, &SkeinError>,
        context: StatementExecutionContext<'_>,
    ) {
        StatementRecordingTarget {
            slow_query_log: &self.slow_query_log,
            statement_summary: &self.statement_summary,
            slow_query_log_threshold_micros: self.slow_query_log_threshold_micros,
            telemetry: self.telemetry.as_deref(),
        }
        .record_statement_execution(
            query_language,
            query_text,
            statement_kind,
            started,
            result,
            context,
        );
    }
}

impl Database {
    pub(super) fn statement_recorder(&self) -> StatementRecorder {
        StatementRecorder {
            slow_query_log: Arc::clone(&self.slow_query_log),
            statement_summary: Arc::clone(&self.statement_summary),
            slow_query_log_threshold_micros: self.config.slow_query_log_threshold_micros,
            telemetry: self.telemetry.clone(),
        }
    }

    pub(super) fn record_statement_execution(
        &self,
        query_language: &str,
        query_text: &str,
        statement_kind: &str,
        started: std::time::Instant,
        result: std::result::Result<&QueryOutput, &SkeinError>,
        context: StatementExecutionContext<'_>,
    ) {
        StatementRecordingTarget {
            slow_query_log: &self.slow_query_log,
            statement_summary: &self.statement_summary,
            slow_query_log_threshold_micros: self.config.slow_query_log_threshold_micros,
            telemetry: self.telemetry.as_deref(),
        }
        .record_statement_execution(
            query_language,
            query_text,
            statement_kind,
            started,
            result,
            context,
        );
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
        self.query_sql_with_params_options(
            sql_text,
            parameters,
            QueryStreamOptions {
                max_rows,
                max_payload_bytes: self.config.max_read_result_payload_bytes,
            },
        )
    }

    /// Executes PostgreSQL-dialect SQL with per-statement row and payload
    /// admission. Configured database limits remain hard upper bounds.
    pub fn query_sql_with_params_options(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
        options: QueryStreamOptions,
    ) -> Result<QueryOutput> {
        self.store.ensure_usable()?;
        let started = std::time::Instant::now();
        let max_rows =
            super::restrictive_query_limit(self.config.max_read_result_rows, options.max_rows);
        let max_payload_bytes = super::restrictive_query_limit(
            self.config.max_read_result_payload_bytes,
            options.max_payload_bytes,
        );
        let prepared = self.relational_plan_template_cache.prepare(sql_text)?;
        self.query_sql_with_prepared_params_inner(
            sql_text,
            parameters,
            prepared,
            max_rows,
            max_payload_bytes,
            started,
        )
    }

    pub(super) fn query_sql_with_prepared_params(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
        prepared: crate::relational_sql::PreparedRelationalSql,
    ) -> Result<QueryOutput> {
        self.store.ensure_usable()?;
        let started = std::time::Instant::now();
        self.query_sql_with_prepared_params_inner(
            sql_text,
            parameters,
            prepared,
            self.config.max_read_result_rows,
            self.config.max_read_result_payload_bytes,
            started,
        )
    }

    fn query_sql_with_prepared_params_inner(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
        prepared: crate::relational_sql::PreparedRelationalSql,
        max_rows: Option<usize>,
        max_payload_bytes: Option<usize>,
        started: std::time::Instant,
    ) -> Result<QueryOutput> {
        let statement_kind = sql_statement_kind(prepared.statement());
        let query_result = (|| {
            super::reject_locking_select_without_manager(prepared.statement(), false)?;
            if skein_relational::system_schema::statement_writes_system_schema_registry(
                prepared.statement(),
            ) {
                return Err(SkeinError::Semantic(
                    "skein_schema_migrations is read-only outside system schema upgrade"
                        .to_string(),
                ));
            }
            if matches!(
                prepared.statement(),
                crate::sql::SqlStatement::Select(select)
                    if system_sql::is_virtual_catalog_select(select)
            ) {
                let plan_cache_stats = self.plan_cache.borrow().stats();
                let slow_queries = self.slow_query_log.borrow().snapshot();
                let statement_summaries = self.statement_summary.borrow().snapshot();
                return system_sql::query_sql_with_params(
                    sql_text,
                    parameters,
                    max_rows,
                    max_payload_bytes,
                    &system_sql::SystemSqlContext {
                        catalog: &self.catalog,
                        store: &self.store,
                        relational_state: self.store.relational_state(),
                        append_state: self.store.append_state(),
                        runtime: system_sql::SystemRuntimeSnapshot::from_config(&self.config),
                        plan_cache_stats: &plan_cache_stats,
                        slow_queries: &slow_queries,
                        statement_summaries: &statement_summaries,
                    },
                );
            }

            if let Some(plan) = compile_append_select_sql(
                sql_text,
                parameters,
                self.store.append_state(),
                max_rows.unwrap_or(usize::MAX),
            )? {
                let output = self.store.read_append_partition_bounded(
                    &plan.table,
                    &plan.partition,
                    plan.after.as_ref(),
                    plan.max_rows,
                    max_payload_bytes.unwrap_or(usize::MAX),
                )?;
                return Ok(QueryOutput {
                    rows: project_append_rows(&plan, &output.rows)?.into(),
                });
            }
            if let Some(plan) = compile_append_explain_sql(
                sql_text,
                parameters,
                self.store.append_state(),
                max_rows.unwrap_or(usize::MAX),
            )? {
                let report = if plan.analyze {
                    Some(
                        self.store
                            .read_append_partition_bounded(
                                &plan.select.table,
                                &plan.select.partition,
                                plan.select.after.as_ref(),
                                plan.select.max_rows,
                                max_payload_bytes.unwrap_or(usize::MAX),
                            )?
                            .report,
                    )
                } else {
                    None
                };
                return Ok(QueryOutput {
                    rows: format_append_explain(&plan, report.as_ref()).into(),
                });
            }

            if matches!(
                prepared.statement(),
                crate::sql::SqlStatement::Select(_) | crate::sql::SqlStatement::Explain(_)
            ) {
                let query_result =
                    crate::relational_sql::execute_prepared_relational_query_with_resources(
                        prepared,
                        parameters,
                        self.store.relational_state(),
                        crate::relational_sql::RelationalQueryReadModes::new(
                            super::relational_index_read_mode(&self.config, &self.store),
                            crate::relational_sql::RelationalRowReadMode::Store(&self.store),
                        ),
                        super::relational_query_resource_context(
                            &self.config,
                            max_rows,
                            max_payload_bytes,
                            None,
                        ),
                    );
                self.store.poison_on_storage_error(&query_result);
                let output = query_result?;
                return Ok(QueryOutput { rows: output.rows });
            }

            self.ensure_writable()?;
            if let Some(transaction) =
                compile_append_statement_sql(sql_text, parameters, self.store.append_state())?
            {
                if let crate::sql::SqlStatement::CreateTable(create) = prepared.statement()
                    && self
                        .store
                        .relational_state()
                        .table_schema(&create.table.name)
                        .is_some()
                {
                    return Err(SkeinError::Semantic(format!(
                        "table {} already exists as a RowPage table",
                        create.table.name
                    )));
                }
                let summary = self.store.commit_kernel_write_batch(
                    &mut self.catalog,
                    crate::store::KernelWriteBatch {
                        append: transaction,
                        ..crate::store::KernelWriteBatch::default()
                    },
                    self.config.mutation_limits,
                )?;
                return Ok(QueryOutput {
                    rows: summary.rows.into(),
                });
            }
            if let crate::sql::SqlStatement::CreateTable(create) = prepared.statement()
                && self.store.append_table_schema(&create.table.name).is_some()
            {
                return Err(SkeinError::Semantic(format!(
                    "table {} already exists as a strict append table",
                    create.table.name
                )));
            }
            let compiled = compile_relational_statement_sql_with_result(
                sql_text,
                parameters,
                self.store.relational_state(),
            )?;
            let summary = self
                .store
                .commit_relational_transaction(&mut self.catalog, compiled.transaction)?;
            self.complete_required_relational_row_checkpoint("SQL commit")?;
            if summary.relational_mutation_outcomes.len() > 1 {
                return Err(SkeinError::StorageIntegrity(
                    "one SQL statement produced multiple relational mutation outcomes".to_string(),
                ));
            }
            let mutation = summary
                .relational_mutation_outcomes
                .first()
                .map(|outcome| {
                    super::project_relational_mutation_outcome(
                        outcome,
                        compiled.returning.as_ref(),
                        self.store.relational_state(),
                        self.config.mutation_limits,
                        false,
                    )
                })
                .transpose()?;
            let mut rows: executor::QueryRows = summary.rows.into();
            if rows.is_empty()
                && let Some(mutation) = mutation
            {
                rows = mutation.rows;
            }
            Ok(QueryOutput { rows })
        })();
        self.record_statement_execution(
            "sql",
            sql_text,
            statement_kind,
            started,
            query_result.as_ref(),
            StatementExecutionContext::default(),
        );
        query_result
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
