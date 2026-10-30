// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Planning and accounting are identical for private read workspaces and
/// borrowed publications. Only executor resource ownership differs.
pub(super) struct PreparedReadExecution {
    pub(super) statement: cypher::Statement,
    pub(super) optimized: OptimizedQueryPlan,
    pub(super) _planning_memory: Option<hawdb_qos::RuntimeRetainedMemory>,
}

impl PreparedReadExecution {
    pub(super) fn execute<S: executor::ExecutionStore>(
        self,
        parameters: &BTreeMap<String, Value>,
        options: query_runtime::QueryExecutionOptions<'_>,
        config: &DatabaseConfig,
        row_limit: Option<usize>,
        resources: executor::ExecutionResources<'_, S>,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        let Self {
            statement,
            optimized,
            _planning_memory,
        } = self;
        let profiled = executor::execute_with_request(
            executor::ExecutionRequest::new(
                &optimized.physical_plan,
                parameters,
                &config.execution_memory,
            )
            .with_output_columns(optimized.output_columns.as_deref())
            .with_output_limits(
                restrictive_query_limit(row_limit, options.output_limits.max_rows),
                restrictive_query_limit(
                    config.max_read_result_payload_bytes,
                    options.output_limits.max_payload_bytes,
                ),
            )
            .with_optional_task_context(options.task_context)
            .with_optional_host_scorer(options.scoring.and_then(BoundScoringRequest::host_scorer)),
            resources,
        )?;
        query_runtime::query_runtime_checkpoint(options.task_context)?;
        Ok((
            QueryOutput {
                rows: profiled.rows,
            },
            QueryExecutionTrace {
                statement,
                optimizer_trace: options.capture_trace.then_some(optimized.trace),
                plan_cache_lookup: Some(optimized.plan_cache_lookup),
                execution_profile: Some(profiled.profile),
            },
        ))
    }
}

impl<S: executor::ExecutionStore> DatabaseReadTransaction<S> {
    pub(super) fn prepare_read_execution(
        &self,
        cypher_text: &str,
        mut prepared: query_runtime::PreparedRuntimeExecution,
        parameters: &BTreeMap<String, Value>,
        options: query_runtime::QueryExecutionOptions<'_>,
    ) -> Result<PreparedReadExecution> {
        self.store.ensure_usable()?;
        query_runtime::query_runtime_checkpoint(options.task_context)?;
        let _planning_memory = prepared.planning_memory.take();
        let prepared_optimized =
            prepared.take_optimized(options.access_control, options.scoring)?;
        let query_runtime::PreparedRuntimeExecution { statement, .. } = prepared;
        let body = statement_body(&statement);
        if matches!(body, cypher::Statement::Checkpoint) {
            reject_transaction_control_parameters("CHECKPOINT", parameters)?;
            return Err(HawDBError::Execution(
                "CHECKPOINT is not allowed inside a read transaction".into(),
            ));
        }
        if matches!(body, cypher::Statement::SetSystemVariable(_)) {
            reject_system_variable_parameters(parameters)?;
            return Err(HawDBError::Execution(
                "SET system variable is not allowed inside a read transaction".into(),
            ));
        }
        query_work_request_for_statement(&self.system_variables, &statement)?;
        let optimized = match prepared_optimized {
            Some(optimized) => optimized,
            None => self.optimized_query_plan_for_request(
                cypher_text,
                &statement,
                parameters,
                options.access_control,
                PlanTraceMode::Template,
                options.scoring,
            )?,
        };
        if executor::is_mutation_plan(&optimized.physical_plan)? {
            return Err(HawDBError::Execution(
                "read transaction query must not be a mutation".into(),
            ));
        }
        Ok(PreparedReadExecution {
            statement,
            optimized,
            _planning_memory,
        })
    }
}

impl DatabaseReadSnapshot {
    pub(super) fn query_prepared_request_borrowed(
        &self,
        prepared: PreparedRuntimeQuery,
        request: QueryRequest<'_>,
        capture_trace: bool,
        external: &mut dyn executor::ExternalReadOperator,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        let source = &self.0;
        let task = query_request::bind_request_context(
            source.task_context.as_ref(),
            request.task_context,
        )?;
        let request = QueryRequest {
            task_context: task.as_ref(),
            ..request
        };
        let mut options = query_runtime::QueryExecutionOptions::for_request(request, None);
        options.capture_trace = capture_trace;
        let (cypher, prepared) = prepared.into_execution(&source.catalog, &source.store);
        let empty_parameters = BTreeMap::new();
        let parameters = request.parameters.unwrap_or(&empty_parameters);
        let prepared = source.prepare_read_execution(&cypher, prepared, parameters, options)?;
        let result = prepared.execute(
            parameters,
            options,
            &source.config,
            source.read_result_row_limit(),
            executor::ExecutionResources::read_only(&source.catalog, &source.store, external),
        );
        source.store.poison_on_storage_error(&result);
        result
    }
}
