// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Pinned query execution and result admission.

use super::*;

pub(super) fn profiled_relational_sql_output(
    output: crate::relational_sql::RelationalQueryOutput,
) -> ProfiledRelationalSqlQueryOutput {
    let crate::relational_sql::RelationalQueryOutput {
        rows,
        stage_timings,
        join_planning,
        operator_cardinality_profiles,
        intermediate_rows,
        hydration,
        index_execution_evidence,
        row_execution_evidence,
        blocking_operator_memory_reports,
        ..
    } = output;
    let profile = RelationalSqlReadProfile {
        stage_timings,
        join_planning,
        operator_cardinality_profiles,
        intermediate_rows,
        hydrated_rows: hydration.hydrated_rows,
        hydrated_compressed_bytes: hydration.compressed_bytes,
        hydrated_decompressed_bytes: hydration.decompressed_bytes,
        index_reads: index_execution_evidence
            .into_iter()
            .map(|evidence| {
                let runtime_path = evidence.runtime_path().to_string();
                RelationalSqlIndexReadProfile {
                    table: evidence.table,
                    index: evidence.index,
                    lookups: evidence.lookups,
                    metadata_count_lookups: evidence.metadata_count_lookups,
                    runtime_path,
                    logical_pages: evidence.logical_pages,
                    logical_bytes: evidence.logical_bytes,
                    physical_pages: evidence.file_pages,
                    physical_bytes: evidence.file_bytes,
                    cache_hits: evidence.cache_hits,
                    cache_misses: evidence.cache_misses,
                    cache_admission_rejections: evidence.cache_admission_rejections,
                    rows_visited: evidence.rows_visited,
                }
            })
            .collect(),
        row_read: RelationalSqlRowReadProfile {
            runtime_path: row_execution_evidence.runtime_path.to_string(),
            projection_generation: row_execution_evidence.projection_generation,
            projection_source_watermark: row_execution_evidence.projection_source_watermark,
            projection_version: row_execution_evidence.projection_version,
            projection_publication_commit_epoch: row_execution_evidence
                .projection_publication_commit_epoch,
            base_generation: row_execution_evidence.base_generation,
            delta_generation: row_execution_evidence.delta_generation,
            base_commit_epoch: row_execution_evidence.base_commit_epoch,
            visible_commit_epoch: row_execution_evidence.visible_commit_epoch,
            root_set_digest: row_execution_evidence.root_set_digest,
            descriptor_reads: row_execution_evidence.descriptor_reads,
            logical_pages: row_execution_evidence.logical_pages,
            logical_bytes: row_execution_evidence.logical_bytes,
            physical_pages: row_execution_evidence.file_pages,
            physical_bytes: row_execution_evidence.file_bytes,
            cache_hits: row_execution_evidence.cache_hits,
            cache_misses: row_execution_evidence.cache_misses,
            cache_admission_rejections: row_execution_evidence.cache_admission_rejections,
            rows_visited: row_execution_evidence.rows_visited,
            overlay_entries: row_execution_evidence.overlay_entries,
            overlay_resident_bytes: row_execution_evidence.overlay_resident_bytes,
        },
        blocking_operator_memory_reports,
    };
    ProfiledRelationalSqlQueryOutput {
        output: QueryOutput { rows },
        profile,
    }
}

pub(super) struct DatabaseReadSqlOptions<'a> {
    pub(super) max_rows: Option<usize>,
    pub(super) max_payload_bytes: Option<usize>,
    pub(super) task_context: &'a hawdb_core::RuntimeTaskContext,
    pub(super) join_planning: RelationalJoinPlanningDirective,
}

impl<S: crate::executor::ExecutionStore> DatabaseReadTransaction<S> {
    pub(super) fn read_result_row_limit(&self) -> Option<usize> {
        restrictive_query_limit(
            self.config.read_result_row_limit(),
            self.session_config.row_limit(),
        )
    }

    pub(crate) fn apply_session_config(&mut self, config: SessionConfig) {
        self.session_config = config;
    }

    pub fn commit_epoch(&self) -> u64 {
        self.published_read_view.visible_commit_epoch()
    }

    pub const fn published_read_view(&self) -> PublishedReadView {
        self.published_read_view
    }

    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.query_with_params_bounded(cypher_text, parameters, self.read_result_row_limit())
    }

    pub fn query_with_params_context(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        task_context: &hawdb_core::RuntimeTaskContext,
    ) -> Result<QueryOutput> {
        Ok(self
            .query_with_params_bounded_profile_internal(
                cypher_text,
                parameters,
                self.read_result_row_limit(),
                None,
                Some(task_context),
            )?
            .output)
    }

    pub(crate) fn query_prepared_with_params_profiled_context(
        &mut self,
        prepared: PreparedRuntimeQuery,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        task_context: &hawdb_core::RuntimeTaskContext,
    ) -> Result<BoundedReadQueryOutput> {
        self.store.ensure_usable()?;
        let (cypher_text, prepared) = prepared.into_execution(&self.catalog, &self.store);
        self.query_with_params_bounded_profile_prepared_output_internal(
            &cypher_text,
            prepared,
            parameters,
            options,
            None,
            Some(task_context),
        )
    }

    pub(crate) fn query_prepared_request(
        &mut self,
        prepared: PreparedRuntimeQuery,
        request: QueryRequest<'_>,
    ) -> Result<BoundedReadQueryOutput> {
        let task =
            query_request::bind_request_context(self.task_context.as_ref(), request.task_context)?;
        let request = QueryRequest {
            task_context: task.as_ref(),
            ..request
        };
        let (cypher_text, prepared) = prepared.into_execution(&self.catalog, &self.store);
        self.query_prepared_request_profiled(
            &cypher_text,
            prepared,
            request.parameters.unwrap_or(&BTreeMap::new()),
            query_runtime::QueryExecutionOptions::for_request(request, None),
            &mut executor::NoExternalReadOperator,
        )
    }

    pub(crate) fn query_prepared_request_traced(
        &mut self,
        prepared: PreparedRuntimeQuery,
        request: QueryRequest<'_>,
        capture_trace: bool,
        external: &mut dyn executor::ExternalReadOperator,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        let task =
            query_request::bind_request_context(self.task_context.as_ref(), request.task_context)?;
        let request = QueryRequest {
            task_context: task.as_ref(),
            ..request
        };
        let mut options = query_runtime::QueryExecutionOptions::for_request(request, None);
        options.capture_trace = capture_trace;
        let (cypher_text, prepared) = prepared.into_execution(&self.catalog, &self.store);
        self.query_prepared_request_traced_internal(
            &cypher_text,
            prepared,
            request.parameters.unwrap_or(&BTreeMap::new()),
            options,
            external,
        )
    }

    pub fn query_with_params_access_control(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        access_control: QueryAccessControlContext,
    ) -> Result<QueryOutput> {
        self.query_with_params_bounded_profile_access_control(
            cypher_text,
            parameters,
            self.read_result_row_limit(),
            access_control,
        )
        .map(|output| output.output)
    }

    pub fn query_with_params_bounded(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
    ) -> Result<QueryOutput> {
        Ok(self
            .query_with_params_bounded_profile(cypher_text, parameters, max_rows)?
            .output)
    }

    pub fn query_with_params_bounded_profile(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
    ) -> Result<BoundedReadQueryOutput> {
        let task_context = self.task_context.clone();
        self.query_with_params_bounded_profile_internal(
            cypher_text,
            parameters,
            max_rows,
            None,
            task_context.as_ref(),
        )
    }

    pub fn query_streaming(
        &mut self,
        cypher_text: &str,
        options: QueryStreamOptions,
        consumer: impl FnMut(Row) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.query_with_params_streaming(cypher_text, &BTreeMap::new(), options, consumer)
    }

    /// Streams rows from a read plan through a budgeted consumer boundary.
    ///
    /// When either effective output limit is present, the executor invokes the
    /// consumer only after the complete result passes both limit checks. With
    /// both limits disabled, rows cross the consumer boundary as they are
    /// produced and a later non-budget execution error can still be returned.
    pub fn query_with_params_streaming(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        mut consumer: impl FnMut(Row) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.store.ensure_usable()?;
        let task_context = self.task_context.clone();
        self.query_with_params_streaming_prepared_external_internal(
            cypher_text,
            query_runtime::parse_runtime_execution(cypher_text)?,
            parameters,
            options,
            ReadStreamingExecutionContext {
                task_context: task_context.as_ref(),
                external: None,
                delivery: executor::StreamDelivery::Validated,
                scoring: None,
                access_control: None,
            },
            executor::QueryStreamConsumer::Owned(&mut consumer),
        )
    }

    /// Streams borrowed row views to a synchronous host consumer.
    ///
    /// The view cannot outlive the consumer call. Hosts can inspect, serialize,
    /// or load values without an additional clone; hosts that retain a row
    /// must call [`RowRef::to_owned_row`]. Bounded delivery validates the whole
    /// result in compact, schema-aware storage before invoking the consumer.
    /// The scalar producer still materializes a row before handing it to the
    /// accumulator; native producer delivery is qualified separately.
    pub fn query_with_params_streaming_ref(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        mut consumer: impl for<'row> FnMut(RowRef<'row>) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.store.ensure_usable()?;
        let task_context = self.task_context.clone();
        self.query_with_params_streaming_prepared_external_internal(
            cypher_text,
            query_runtime::parse_runtime_execution(cypher_text)?,
            parameters,
            options,
            ReadStreamingExecutionContext {
                task_context: task_context.as_ref(),
                external: None,
                delivery: executor::StreamDelivery::Validated,
                scoring: None,
                access_control: None,
            },
            executor::QueryStreamConsumer::Borrowed(&mut consumer),
        )
    }

    pub fn query_with_params_streaming_context(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        task_context: &hawdb_core::RuntimeTaskContext,
        mut consumer: impl FnMut(Row) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.store.ensure_usable()?;
        self.query_with_params_streaming_prepared_external_internal(
            cypher_text,
            query_runtime::parse_runtime_execution(cypher_text)?,
            parameters,
            options,
            ReadStreamingExecutionContext {
                task_context: Some(task_context),
                external: None,
                delivery: executor::StreamDelivery::Validated,
                scoring: None,
                access_control: None,
            },
            executor::QueryStreamConsumer::Owned(&mut consumer),
        )
    }

    #[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
    pub(crate) fn query_prepared_with_params_streaming_context(
        &mut self,
        prepared: PreparedRuntimeQuery,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        delivery: executor::StreamDelivery,
        task_context: &hawdb_core::RuntimeTaskContext,
        consumer: impl FnMut(Row) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.query_prepared_request_streaming(
            prepared,
            QueryRequest::new("")
                .with_params(parameters)
                .with_task_context(task_context)
                .with_output_limits(options),
            delivery,
            consumer,
        )
    }

    pub(crate) fn query_prepared_request_streaming(
        &mut self,
        prepared: PreparedRuntimeQuery,
        request: QueryRequest<'_>,
        delivery: executor::StreamDelivery,
        mut consumer: impl FnMut(Row) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        let task =
            query_request::bind_request_context(self.task_context.as_ref(), request.task_context)?;
        let request = QueryRequest {
            task_context: task.as_ref(),
            ..request
        };
        let execution = query_runtime::QueryExecutionOptions::for_request(request, None);
        let (cypher_text, prepared) = prepared.into_execution(&self.catalog, &self.store);
        self.query_with_params_streaming_prepared_external_internal(
            &cypher_text,
            prepared,
            request.parameters.unwrap_or(&BTreeMap::new()),
            execution.output_limits,
            ReadStreamingExecutionContext {
                task_context: execution.task_context,
                external: None,
                delivery,
                scoring: execution.scoring,
                access_control: execution.access_control,
            },
            executor::QueryStreamConsumer::Owned(&mut consumer),
        )
    }

    pub(super) fn query_with_params_streaming_prepared_external_internal(
        &mut self,
        cypher_text: &str,
        mut prepared: query_runtime::PreparedRuntimeExecution,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        context: ReadStreamingExecutionContext<'_>,
        consumer: executor::QueryStreamConsumer<'_>,
    ) -> Result<QueryStreamReport> {
        let _planning_memory = prepared.planning_memory.take();
        self.store.ensure_usable()?;
        let max_rows = restrictive_query_limit(self.read_result_row_limit(), options.max_rows);
        let max_payload_bytes = restrictive_query_limit(
            self.config.max_read_result_payload_bytes,
            options.max_payload_bytes,
        );
        let prepared_optimized =
            prepared.take_optimized(context.access_control, context.scoring)?;
        let query_runtime::PreparedRuntimeExecution { statement, .. } = prepared;
        let body = statement_body(&statement);
        if matches!(statement, cypher::Statement::Explain(_)) {
            return Err(HawDBError::Execution(
                "streaming query does not support EXPLAIN".to_string(),
            ));
        }
        if matches!(body, cypher::Statement::Checkpoint) {
            return Err(HawDBError::Execution(
                "CHECKPOINT is not allowed inside a read transaction".to_string(),
            ));
        }
        if matches!(body, cypher::Statement::SetSystemVariable(_)) {
            return Err(HawDBError::Execution(
                "SET system variable is not allowed inside a read transaction".to_string(),
            ));
        }
        query_work_request_for_statement(&self.system_variables, &statement)?;
        let optimized = match prepared_optimized {
            Some(optimized) => optimized,
            _ => self.optimized_query_plan_for_request(
                cypher_text,
                &statement,
                parameters,
                context.access_control,
                PlanTraceMode::Template,
                context.scoring,
            )?,
        };
        if executor::is_mutation_plan(&optimized.physical_plan)? {
            return Err(HawDBError::Execution(
                "read transaction query must not be a mutation".to_string(),
            ));
        }
        let streamed = executor::execute_with_request_stream(
            executor::ExecutionRequest::new(
                &optimized.physical_plan,
                parameters,
                &self.config.execution_memory,
            )
            .with_output_columns(optimized.output_columns.as_deref())
            .with_output_limits(max_rows, max_payload_bytes)
            .with_optional_task_context(context.task_context)
            .with_optional_host_scorer(context.scoring.and_then(BoundScoringRequest::host_scorer)),
            executor::ExecutionResources::new(
                &mut self.catalog,
                &mut self.store,
                context
                    .external
                    .unwrap_or(&mut executor::NoExternalReadOperator),
            ),
            context.delivery,
            consumer,
        );
        self.store.poison_on_storage_error(&streamed);
        let streamed = streamed?;
        let pipeline = &streamed.profile.pipeline_memory_report;
        Ok(QueryStreamReport {
            fully_streamed: streamed.fully_streamed,
            output_rows: pipeline.output_rows,
            output_payload_bytes: pipeline.output_payload_bytes,
            execution_profile: streamed.profile,
        })
    }

    pub fn query_with_params_bounded_profile_access_control(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
        access_control: QueryAccessControlContext,
    ) -> Result<BoundedReadQueryOutput> {
        let task_context = self.task_context.clone();
        self.query_with_params_bounded_profile_internal(
            cypher_text,
            parameters,
            max_rows,
            Some(access_control),
            task_context.as_ref(),
        )
    }

    pub(super) fn query_with_params_bounded_profile_internal(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
        access_control: Option<QueryAccessControlContext>,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<BoundedReadQueryOutput> {
        self.store.ensure_usable()?;
        self.query_with_params_bounded_profile_prepared_internal(
            cypher_text,
            query_runtime::parse_runtime_execution(cypher_text)?,
            parameters,
            max_rows,
            access_control,
            task_context,
        )
    }

    pub(super) fn query_with_params_bounded_profile_prepared_internal(
        &mut self,
        cypher_text: &str,
        prepared: query_runtime::PreparedRuntimeExecution,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
        access_control: Option<QueryAccessControlContext>,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<BoundedReadQueryOutput> {
        self.query_with_params_bounded_profile_prepared_output_internal(
            cypher_text,
            prepared,
            parameters,
            QueryStreamOptions {
                max_rows,
                max_payload_bytes: None,
            },
            access_control,
            task_context,
        )
    }

    pub(super) fn query_with_params_bounded_profile_prepared_output_internal(
        &mut self,
        cypher_text: &str,
        prepared: query_runtime::PreparedRuntimeExecution,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        access_control: Option<QueryAccessControlContext>,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<BoundedReadQueryOutput> {
        let mut execution_options = query_runtime::QueryExecutionOptions::for_bounded_read(
            options.max_rows,
            access_control.as_ref(),
            task_context,
        );
        execution_options.output_limits = options;
        self.query_prepared_request_profiled(
            cypher_text,
            prepared,
            parameters,
            execution_options,
            &mut executor::NoExternalReadOperator,
        )
    }

    pub(super) fn query_prepared_request_profiled(
        &mut self,
        cypher_text: &str,
        prepared: query_runtime::PreparedRuntimeExecution,
        parameters: &BTreeMap<String, Value>,
        options: query_runtime::QueryExecutionOptions<'_>,
        external: &mut dyn executor::ExternalReadOperator,
    ) -> Result<BoundedReadQueryOutput> {
        let (output, trace) = self.query_prepared_request_traced_internal(
            cypher_text,
            prepared,
            parameters,
            options,
            external,
        )?;
        Ok(BoundedReadQueryOutput {
            output,
            execution_profile: trace
                .execution_profile
                .expect("read execution always supplies a profile"),
        })
    }

    pub(super) fn query_prepared_request_traced_internal(
        &mut self,
        cypher_text: &str,
        mut prepared: query_runtime::PreparedRuntimeExecution,
        parameters: &BTreeMap<String, Value>,
        options: query_runtime::QueryExecutionOptions<'_>,
        external: &mut dyn executor::ExternalReadOperator,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        self.store.ensure_usable()?;
        query_runtime::query_runtime_checkpoint(options.task_context)?;
        if let cypher::Statement::Explain(explain) = &prepared.statement {
            let _planning_memory = prepared.planning_memory.take();
            let result =
                self.execute_explain_request(cypher_text, explain, parameters, options, external)?;
            let mut trace = QueryExecutionTrace::uncached(prepared.statement);
            trace.execution_profile = Some(result.execution_profile);
            return Ok((result.output, trace));
        }
        let prepared = self.prepare_read_execution(cypher_text, prepared, parameters, options)?;
        let row_limit = self.read_result_row_limit();
        let result = prepared.execute(
            parameters,
            options,
            &self.config,
            row_limit,
            executor::ExecutionResources::new(&mut self.catalog, &mut self.store, external),
        );
        self.store.poison_on_storage_error(&result);
        result
    }

    pub(super) fn execute_explain_request(
        &mut self,
        cypher_text: &str,
        explain: &cypher::Explain,
        parameters: &BTreeMap<String, Value>,
        options: query_runtime::QueryExecutionOptions<'_>,
        external: &mut dyn executor::ExternalReadOperator,
    ) -> Result<BoundedReadQueryOutput> {
        let task_context = options.task_context;
        query_runtime::query_runtime_checkpoint(task_context)?;
        let work_request =
            query_work_request_for_statement(&self.system_variables, &explain.statement)?;
        let optimized = self.optimized_query_plan_for_request(
            cypher_text,
            &explain.statement,
            parameters,
            options.access_control,
            PlanTraceMode::Bound,
            options.scoring,
        )?;
        let inner_statement_kind = statement_kind(statement_body(&explain.statement));
        if explain.analyze {
            if executor::requires_write_access(&optimized.physical_plan) {
                return Err(HawDBError::Execution(
                    "EXPLAIN ANALYZE only supports read queries".to_string(),
                ));
            }
            let profiled = executor::execute_with_request(
                executor::ExecutionRequest::new(
                    &optimized.physical_plan,
                    parameters,
                    &self.config.execution_memory,
                )
                .with_output_columns(optimized.output_columns.as_deref())
                .with_output_limits(
                    restrictive_query_limit(
                        self.read_result_row_limit(),
                        options.output_limits.max_rows,
                    ),
                    restrictive_query_limit(
                        self.config.max_read_result_payload_bytes,
                        options.output_limits.max_payload_bytes,
                    ),
                )
                .with_optional_task_context(task_context)
                .with_optional_host_scorer(
                    options.scoring.and_then(BoundScoringRequest::host_scorer),
                ),
                executor::ExecutionResources::new(&mut self.catalog, &mut self.store, external),
            );
            self.store.poison_on_storage_error(&profiled);
            let profiled = profiled?;
            query_runtime::query_runtime_checkpoint(task_context)?;
            let row_count = profiled.rows.len();
            let row = explain_analyze_output_row(
                &optimized,
                work_request,
                inner_statement_kind,
                row_count,
                &profiled.profile,
            );
            drop(profiled.rows);
            return Ok(BoundedReadQueryOutput {
                output: explain::admit_explain_output(row, true, &self.config, options)?,
                execution_profile: profiled.profile,
            });
        }
        Ok(BoundedReadQueryOutput {
            output: explain::admit_explain_output(
                explain_output_row(&optimized, work_request, inner_statement_kind),
                false,
                &self.config,
                options,
            )?,
            execution_profile: empty_read_execution_profile(),
        })
    }
}

impl<S> DatabaseReadTransaction<S>
where
    S: crate::executor::ExecutionStore + hawdb_system_sql::SystemSqlStore,
{
    pub fn query_sql(&self, sql_text: &str) -> Result<QueryOutput> {
        self.query_sql_bounded(sql_text, self.read_result_row_limit())
    }

    pub fn query_sql_with_params(
        &self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        self.query_sql_with_params_bounded(sql_text, parameters, self.read_result_row_limit())
    }

    pub fn query_sql_bounded(
        &self,
        sql_text: &str,
        max_rows: Option<usize>,
    ) -> Result<QueryOutput> {
        self.query_sql_with_params_bounded(sql_text, &[], max_rows)
    }

    pub fn query_sql_with_params_bounded(
        &self,
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
    /// admission against this pinned read transaction. Configured database
    /// limits remain hard upper bounds.
    pub fn query_sql_with_params_options(
        &self,
        sql_text: &str,
        parameters: &[Value],
        options: QueryStreamOptions,
    ) -> Result<QueryOutput> {
        self.query_sql_with_params_options_with_join_planning(
            sql_text,
            parameters,
            options,
            RelationalJoinPlanningDirective::Auto,
        )
    }

    /// Executes one bounded relational query under an explicit join planning
    /// directive. `SyntaxOrder` is intended for diagnostics and differential
    /// verification against the default cost-based plan.
    pub fn query_sql_with_params_options_with_join_planning(
        &self,
        sql_text: &str,
        parameters: &[Value],
        options: QueryStreamOptions,
        join_planning: RelationalJoinPlanningDirective,
    ) -> Result<QueryOutput> {
        let default_context = hawdb_core::RuntimeTaskContext::default();
        let task_context = self.task_context.as_ref().unwrap_or(&default_context);
        self.query_sql_with_params_options_context_and_join_planning(
            sql_text,
            parameters,
            options,
            task_context,
            join_planning,
        )
    }

    /// Executes one bounded relational `SELECT` against this pinned snapshot
    /// and returns result rows together with operator cardinality and storage
    /// accounting from that exact execution. Virtual system-catalog queries
    /// and `EXPLAIN` are kept on their dedicated output paths.
    pub fn query_sql_with_params_options_profiled(
        &self,
        sql_text: &str,
        parameters: &[Value],
        options: QueryStreamOptions,
    ) -> Result<ProfiledRelationalSqlQueryOutput> {
        self.query_sql_with_params_options_profiled_with_join_planning(
            sql_text,
            parameters,
            options,
            RelationalJoinPlanningDirective::Auto,
        )
    }

    /// Executes and profiles one relational `SELECT` under an explicit join
    /// planning directive. `SyntaxOrder` is intended for diagnostics and
    /// differential verification against the default cost-based plan.
    pub fn query_sql_with_params_options_profiled_with_join_planning(
        &self,
        sql_text: &str,
        parameters: &[Value],
        options: QueryStreamOptions,
        join_planning: RelationalJoinPlanningDirective,
    ) -> Result<ProfiledRelationalSqlQueryOutput> {
        let default_context = hawdb_core::RuntimeTaskContext::default();
        let task_context = self.task_context.as_ref().unwrap_or(&default_context);
        self.query_sql_with_params_options_profiled_context_and_join_planning(
            sql_text,
            parameters,
            options,
            task_context,
            join_planning,
        )
    }

    /// Profiled relational `SELECT` with host cancellation and deadline
    /// propagation. The returned rows and profile always belong to one
    /// execution of the same pinned read view.
    pub fn query_sql_with_params_options_profiled_context(
        &self,
        sql_text: &str,
        parameters: &[Value],
        options: QueryStreamOptions,
        task_context: &hawdb_core::RuntimeTaskContext,
    ) -> Result<ProfiledRelationalSqlQueryOutput> {
        self.query_sql_with_params_options_profiled_context_and_join_planning(
            sql_text,
            parameters,
            options,
            task_context,
            RelationalJoinPlanningDirective::Auto,
        )
    }

    pub(super) fn query_sql_with_params_options_profiled_context_and_join_planning(
        &self,
        sql_text: &str,
        parameters: &[Value],
        options: QueryStreamOptions,
        task_context: &hawdb_core::RuntimeTaskContext,
        join_planning: RelationalJoinPlanningDirective,
    ) -> Result<ProfiledRelationalSqlQueryOutput> {
        self.store.ensure_usable()?;
        query_runtime::query_runtime_checkpoint(Some(task_context))?;
        let max_rows = restrictive_query_limit(self.read_result_row_limit(), options.max_rows);
        let max_payload_bytes = restrictive_query_limit(
            self.config.max_read_result_payload_bytes,
            options.max_payload_bytes,
        );
        let prepared = self.relational_plan_template_cache.prepare(sql_text)?;
        reject_locking_select_without_manager(prepared.statement(), false)?;
        let crate::sql::SqlStatement::Select(select) = prepared.statement() else {
            return Err(HawDBError::Semantic(
                "profiled relational SQL requires SELECT".to_string(),
            ));
        };
        if system_sql::is_virtual_catalog_select(select) {
            return Err(HawDBError::Semantic(
                "profiled relational SQL does not support virtual system catalogs".to_string(),
            ));
        }
        self.execute_profiled_relational_sql(
            prepared,
            parameters,
            max_rows,
            max_payload_bytes,
            task_context,
            join_planning,
        )
    }

    /// Executes bounded PostgreSQL-dialect SQL against this pinned read
    /// transaction while propagating host cancellation and deadlines through
    /// planning, index traversal, row hydration, and result construction.
    pub fn query_sql_with_params_options_context(
        &self,
        sql_text: &str,
        parameters: &[Value],
        options: QueryStreamOptions,
        task_context: &hawdb_core::RuntimeTaskContext,
    ) -> Result<QueryOutput> {
        self.query_sql_with_params_options_context_and_join_planning(
            sql_text,
            parameters,
            options,
            task_context,
            RelationalJoinPlanningDirective::Auto,
        )
    }

    pub(super) fn query_sql_with_params_options_context_and_join_planning(
        &self,
        sql_text: &str,
        parameters: &[Value],
        options: QueryStreamOptions,
        task_context: &hawdb_core::RuntimeTaskContext,
        join_planning: RelationalJoinPlanningDirective,
    ) -> Result<QueryOutput> {
        self.store.ensure_usable()?;
        query_runtime::query_runtime_checkpoint(Some(task_context))?;
        let max_rows = restrictive_query_limit(self.read_result_row_limit(), options.max_rows);
        let max_payload_bytes = restrictive_query_limit(
            self.config.max_read_result_payload_bytes,
            options.max_payload_bytes,
        );
        let prepared = self.relational_plan_template_cache.prepare(sql_text)?;
        self.query_sql_with_prepared_params_context_and_join_planning(
            sql_text,
            parameters,
            prepared,
            DatabaseReadSqlOptions {
                max_rows,
                max_payload_bytes,
                task_context,
                join_planning,
            },
        )
    }

    pub(super) fn query_sql_with_prepared_params(
        &self,
        sql_text: &str,
        parameters: &[Value],
        prepared: crate::relational_sql::PreparedRelationalSql,
    ) -> Result<QueryOutput> {
        self.query_sql_with_prepared_request(
            SqlQueryRequest::new(sql_text).with_params(parameters),
            prepared,
        )
    }

    pub(crate) fn query_sql_with_prepared_request(
        &self,
        request: SqlQueryRequest<'_>,
        prepared: crate::relational_sql::PreparedRelationalSql,
    ) -> Result<QueryOutput> {
        self.store.ensure_usable()?;
        let task =
            query_request::bind_request_context(self.task_context.as_ref(), request.task_context)?;
        let default_context = hawdb_core::RuntimeTaskContext::default();
        let task_context = task.as_ref().unwrap_or(&default_context);
        query_runtime::query_runtime_checkpoint(Some(task_context))?;
        let request = SqlQueryRequest {
            task_context: Some(task_context),
            ..request
        };
        let limits = request.effective_output_limits(&self.config);
        self.query_sql_with_prepared_params_context_and_join_planning(
            request.sql,
            request.parameters,
            prepared,
            DatabaseReadSqlOptions {
                max_rows: limits.max_rows,
                max_payload_bytes: limits.max_payload_bytes,
                task_context,
                join_planning: request.join_planning,
            },
        )
    }

    pub(super) fn query_sql_with_prepared_params_context_and_join_planning(
        &self,
        sql_text: &str,
        parameters: &[Value],
        prepared: crate::relational_sql::PreparedRelationalSql,
        options: DatabaseReadSqlOptions<'_>,
    ) -> Result<QueryOutput> {
        let DatabaseReadSqlOptions {
            max_rows,
            max_payload_bytes,
            task_context,
            join_planning,
        } = options;
        reject_locking_select_without_manager(prepared.statement(), false)?;
        if let crate::sql::SqlStatement::Branch(statement) = prepared.statement() {
            return observability::execute_branch_sql_at_path(
                self.branch_catalog_path.as_deref(),
                self.current_branch.as_ref(),
                "read transaction",
                statement,
                parameters,
                max_rows,
                max_payload_bytes,
            );
        }
        if matches!(
            prepared.statement(),
            crate::sql::SqlStatement::Select(select)
                if system_sql::is_virtual_catalog_select(select)
        ) {
            let output = system_sql::query_sql_with_params(
                sql_text,
                parameters,
                max_rows,
                max_payload_bytes,
                &system_sql::SystemSqlContext {
                    catalog: &self.catalog,
                    store: &self.store,
                    relational_state: self.store.relational_state(),
                    append_state: self.store.append_state(),
                    runtime: system_runtime_snapshot(&self.config),
                    plan_cache_stats: &self.plan_cache.borrow().stats(),
                    slow_queries: &self.slow_query_snapshot,
                    statement_summaries: &self.statement_summary_snapshot,
                },
            )?;
            query_runtime::query_runtime_checkpoint(Some(task_context))?;
            return Ok(output);
        }

        if let Some(plan) = crate::relational_sql::compile_prepared_append_select_sql(
            &prepared.template,
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
            query_runtime::query_runtime_checkpoint(Some(task_context))?;
            return Ok(QueryOutput {
                rows: crate::relational_sql::project_append_rows(&plan, &output.rows)?,
            });
        }
        if let Some(plan) = crate::relational_sql::compile_prepared_append_explain_sql(
            &prepared.template,
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
            query_runtime::query_runtime_checkpoint(Some(task_context))?;
            return observability::admit_append_explain(
                crate::relational_sql::format_append_explain(&plan, report.as_ref()),
                plan.analyze,
                &self.config,
                QueryStreamOptions {
                    max_rows,
                    max_payload_bytes,
                },
                Some(task_context),
            );
        }

        self.execute_profiled_relational_sql(
            prepared,
            parameters,
            max_rows,
            max_payload_bytes,
            task_context,
            join_planning,
        )
        .map(|profiled| profiled.output)
    }

    pub(super) fn execute_profiled_relational_sql(
        &self,
        prepared: crate::relational_sql::PreparedRelationalSql,
        parameters: &[Value],
        max_rows: Option<usize>,
        max_payload_bytes: Option<usize>,
        task_context: &hawdb_core::RuntimeTaskContext,
        join_planning: RelationalJoinPlanningDirective,
    ) -> Result<ProfiledRelationalSqlQueryOutput> {
        let row_read_mode = match &self.projection_relational {
            Some(projection) => {
                hawdb_relational::row_runtime::RelationalRowReadMode::<S>::ProjectionGeneration {
                    store: &self.store,
                    reader: &projection.reader,
                    tables: projection.binding.tables(),
                }
            }
            None => hawdb_relational::row_runtime::RelationalRowReadMode::<S>::Store(&self.store),
        };
        let query_result = crate::relational_sql::execute_prepared_relational_query_with_resources(
            prepared,
            parameters,
            self.store.relational_state(),
            hawdb_relational::query::RelationalQueryReadModes::<S>::new(
                relational_index_read_mode(&self.config, &self.store),
                row_read_mode,
            ),
            relational_query_resource_context(
                &self.config,
                max_rows,
                max_payload_bytes,
                Some(task_context),
            )
            .with_join_planning(join_planning),
        );
        self.store.poison_on_storage_error(&query_result);
        let output = query_result?;
        query_runtime::query_runtime_checkpoint(Some(task_context))?;
        Ok(profiled_relational_sql_output(output))
    }
}

impl<S: crate::executor::ExecutionStore> DatabaseReadTransaction<S> {
    pub fn explain_query(&self, cypher_text: &str) -> Result<ExplainOutput> {
        self.explain_query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn explain_query_with_params(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<ExplainOutput> {
        self.store.ensure_usable()?;
        let statement = cypher::parse(cypher_text)?;
        let work_request = query_work_request_for_statement(&self.system_variables, &statement)?;
        let optimized = self.optimized_explain_query_plan(cypher_text, &statement, parameters)?;
        Ok(ExplainOutput {
            physical_plan: optimized.physical_plan,
            trace: optimized.trace,
            work_request,
            plan_cache_lookup: optimized.plan_cache_lookup,
            statement_kind: statement_kind(statement_body(&statement)),
        })
    }

    pub fn plan_cache_stats(&self) -> PlanCacheStats {
        self.plan_cache.borrow().stats()
    }

    pub fn relational_plan_template_cache_stats(&self) -> PlanCacheStats {
        self.relational_plan_template_cache.stats()
    }

    pub(super) fn optimized_query_plan_with_access_control(
        &self,
        cypher_text: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
        access_control: Option<&QueryAccessControlContext>,
    ) -> Result<OptimizedQueryPlan> {
        self.optimized_query_plan_with_access_control_and_trace_mode(
            cypher_text,
            statement,
            parameters,
            access_control,
            PlanTraceMode::Template,
        )
    }

    pub(super) fn optimized_explain_query_plan(
        &self,
        cypher_text: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<OptimizedQueryPlan> {
        self.optimized_explain_query_plan_with_access_control(
            cypher_text,
            statement,
            parameters,
            None,
        )
    }

    pub(super) fn optimized_explain_query_plan_with_access_control(
        &self,
        cypher_text: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
        access_control: Option<&QueryAccessControlContext>,
    ) -> Result<OptimizedQueryPlan> {
        self.optimized_query_plan_with_access_control_and_trace_mode(
            cypher_text,
            statement,
            parameters,
            access_control,
            PlanTraceMode::Bound,
        )
    }

    pub(super) fn optimized_query_plan_with_access_control_and_trace_mode(
        &self,
        cypher_text: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
        access_control: Option<&QueryAccessControlContext>,
        trace_mode: PlanTraceMode,
    ) -> Result<OptimizedQueryPlan> {
        self.optimized_query_plan_for_request(
            cypher_text,
            statement,
            parameters,
            access_control,
            trace_mode,
            None,
        )
    }

    pub(super) fn optimized_query_plan_for_request(
        &self,
        cypher_text: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
        access_control: Option<&QueryAccessControlContext>,
        trace_mode: PlanTraceMode,
        scoring: Option<BoundScoringRequest<'_>>,
    ) -> Result<OptimizedQueryPlan> {
        let optimizer_search =
            query_statement_variables_for_statement(&self.system_variables, statement)?
                .optimizer_search;
        let cache_mode = if optimizer_search != OptimizerSearchDirective::Auto {
            PlanCacheMode::Bypass(PlanCacheBypassReason::OptimizerDirective)
        } else if statement_uses_plan_cache(statement) {
            PlanCacheMode::Use
        } else {
            PlanCacheMode::Bypass(PlanCacheBypassReason::StatementNotCacheable)
        };
        optimized_query_plan_for(
            cypher_text,
            statement,
            parameters,
            cache_mode,
            trace_mode,
            PlanCacheContext {
                catalog: &self.catalog,
                store: &self.store,
                optimizer: &self.optimizer,
                config: &self.config,
                cache: &self.plan_cache,
                planning_cache: &self.optimizer_planning_cache,
                access_control,
                optimizer_search,
                scoring,
            },
        )
    }
}

impl<S> DatabaseReadTransaction<S>
where
    S: crate::executor::ExecutionStore + hawdb_analytics::ProjectionSource,
{
    pub fn project_graph(&self, rel_type: Option<&str>) -> ProjectedGraph {
        match rel_type {
            Some(name) => self
                .catalog
                .rel_type_id(name)
                .map(|rel_type_id| ProjectedGraph::from_store(&self.store, Some(rel_type_id)))
                .unwrap_or_else(|| ProjectedGraph::from_store_without_edges(&self.store)),
            None => ProjectedGraph::from_store(&self.store, None),
        }
    }
}

impl<S: crate::executor::ExecutionStore> DatabaseReadTransaction<S> {
    pub fn export_canonical_graph_snapshot(&self) -> CanonicalGraphSnapshotExport {
        export_canonical_graph_snapshot_for(&self.catalog, &self.store)
    }

    pub fn try_export_canonical_graph_snapshot(&self) -> Result<CanonicalGraphSnapshotExport> {
        canonical_snapshot::try_export_canonical_graph_snapshot_for(&self.catalog, &self.store)
    }
}

impl<S> DatabaseReadTransaction<S>
where
    S: crate::executor::ExecutionStore + hawdb_search::SearchProjectionSource,
{
    pub fn rebuild_search_projection(
        &self,
        search_index: &mut SearchIndex,
        options: SearchRebuildOptions,
    ) -> Result<SearchRebuildSummary> {
        search_index.rebuild_from_graph(&self.catalog, &self.store, options)
    }

    pub fn repair_search_projection_metadata(
        &self,
        search_index: &mut SearchIndex,
        options: MetadataRepairOptions,
    ) -> Result<MetadataRepairSummary> {
        search_index.repair_metadata_from_graph(&self.catalog, &self.store, options)
    }
}

impl<S: crate::executor::ExecutionStore> DatabaseReadTransaction<S> {
    /// Retrieves from resident search payloads, propagating search and graph/pipeline errors.
    pub fn retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        KnowledgeRetrievalGraphContext {
            catalog: &self.catalog,
            store: &self.store,
            compressed_vector_search_mode: self.config.compressed_vector_search_mode,
            adaptive_vector_backend_policy: self.config.adaptive_vector_backend_policy,
            query_memory_budget: self.config.execution_memory.query_memory_bytes,
            result_payload_budget: self
                .config
                .max_read_result_payload_bytes
                .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES),
        }
        .retrieve_knowledge(search_index, request)
    }

    pub fn try_retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        KnowledgeRetrievalGraphContext {
            catalog: &self.catalog,
            store: &self.store,
            compressed_vector_search_mode: self.config.compressed_vector_search_mode,
            adaptive_vector_backend_policy: self.config.adaptive_vector_backend_policy,
            query_memory_budget: self.config.execution_memory.query_memory_bytes,
            result_payload_budget: self
                .config
                .max_read_result_payload_bytes
                .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES),
        }
        .try_retrieve_knowledge(search_index, request)
    }

    pub(crate) fn try_retrieve_knowledge_from_search(
        &self,
        search: SearchResultSet,
        projection_freshness: SearchProjectionFreshness,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        if let Some(context) = &self.task_context {
            context
                .checkpoint()
                .map_err(|reason| HawDBError::Execution(format!("runtime task {reason}")))?;
        }
        let configured_memory = self.config.execution_memory.query_memory_bytes.get();
        let configured_result = self
            .config
            .max_read_result_payload_bytes
            .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES);
        let (memory, result) = self
            .task_context
            .as_ref()
            .and_then(hawdb_core::RuntimeTaskContext::memory_reservation)
            .map_or((configured_memory, configured_result), |reservation| {
                (
                    configured_memory
                        .min(usize::try_from(reservation.memory_bytes()).unwrap_or(usize::MAX)),
                    configured_result
                        .min(usize::try_from(reservation.result_bytes()).unwrap_or(usize::MAX)),
                )
            });
        let query_memory_budget = NonZeroUsize::new(memory).ok_or_else(|| {
            HawDBError::Execution("knowledge retrieval requires admitted working memory".into())
        })?;
        KnowledgeRetrievalGraphContext {
            catalog: &self.catalog,
            store: &self.store,
            compressed_vector_search_mode: self.config.compressed_vector_search_mode,
            adaptive_vector_backend_policy: self.config.adaptive_vector_backend_policy,
            query_memory_budget,
            result_payload_budget: result,
        }
        .retrieve_knowledge_from_search(search, projection_freshness, request)
    }

    #[cfg(test)]
    pub(crate) fn statistics(&self) -> GraphStatistics {
        <S as hawdb_storage::graph_engine::GraphReadEngine>::statistics(&self.store, &self.catalog)
    }

    #[cfg(test)]
    pub(crate) fn basic_statistics(&self) -> crate::schema::BasicGraphStatistics {
        self.store.basic_statistics()
    }
}
