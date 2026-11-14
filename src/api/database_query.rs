// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Database query implementation behind the embedded facade.

use super::*;

impl Database {
    #[cfg(test)]
    pub(super) fn query_read_only_with_params_bounded(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
    ) -> Result<QueryOutput> {
        let started = hawdb_core::time::Instant::now();
        let statement = cypher::parse(cypher_text)?;
        let body = statement_body(&statement);
        let query_result = (|| {
            if matches!(body, cypher::Statement::Checkpoint) {
                reject_transaction_control_parameters("CHECKPOINT", parameters)?;
                return Err(HawDBError::Execution(
                    "CHECKPOINT is not allowed inside a read-only query runtime".to_string(),
                ));
            }
            if matches!(body, cypher::Statement::SetSystemVariable(_)) {
                reject_system_variable_parameters(parameters)?;
                return Err(HawDBError::Execution(
                    "SET system variable is not allowed inside a read-only query runtime"
                        .to_string(),
                ));
            }
            query_work_request_for_statement(&self.system_variables, &statement)?;
            let optimized = self.optimized_query_plan_for_request(
                cypher_text,
                &statement,
                parameters,
                None,
                PlanTraceMode::Template,
                None,
            )?;
            if executor::is_mutation_plan(&optimized.physical_plan)? {
                return Err(HawDBError::Execution(
                    "read-only query runtime must not execute a mutation".to_string(),
                ));
            }
            let mut catalog = self.runtime.get()?.catalog.clone();
            let mut store = self.runtime.get()?.store.snapshot();
            let parameters = BTreeMap::new();
            let memory = executor::ExecutionMemoryConfig::default();
            let mut external = executor::NoExternalReadOperator;
            let profiled = executor::execute_with_request(
                executor::ExecutionRequest::new(&optimized.physical_plan, &parameters, &memory)
                    .with_statement_clock(optimized.statement_clock)
                    .with_csv_import(self.config.csv_import_policy().as_ref())
                    .with_output_limits(max_rows, None),
                executor::ExecutionResources::new(&mut catalog, &mut store, &mut external),
            )?;
            Ok(QueryOutput {
                rows: profiled.rows,
            })
        })();
        self.runtime
            .get()?
            .store
            .poison_on_storage_error(&query_result);
        self.record_statement_execution(
            "cypher",
            cypher_text,
            statement_kind(body),
            started,
            query_result.as_ref(),
            StatementExecutionContext::default(),
        );
        query_result
    }

    pub fn explain_query(&self, cypher_text: &str) -> Result<ExplainOutput> {
        self.explain_query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn explain_query_with_params(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<ExplainOutput> {
        self.explain_query_with_params_access_control_internal(cypher_text, parameters, None)
    }

    pub fn explain_query_with_params_access_control(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        access_control: QueryAccessControlContext,
    ) -> Result<ExplainOutput> {
        self.explain_query_with_params_access_control_internal(
            cypher_text,
            parameters,
            Some(access_control),
        )
    }

    pub(super) fn explain_query_with_params_access_control_internal(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        access_control: Option<QueryAccessControlContext>,
    ) -> Result<ExplainOutput> {
        self.runtime.get()?.store.ensure_usable()?;
        let statement = cypher::parse(cypher_text)?;
        let work_request = query_work_request_for_statement(&self.system_variables, &statement)?;
        let optimized = self.optimized_explain_query_plan_with_access_control(
            cypher_text,
            &statement,
            parameters,
            access_control.as_ref(),
        )?;
        Ok(ExplainOutput {
            physical_plan: optimized.physical_plan,
            trace: optimized.trace,
            work_request,
            plan_cache_lookup: optimized.plan_cache_lookup,
            statement_kind: statement_kind(statement_body(&statement)),
        })
    }

    pub fn explain_analyze_query(&mut self, cypher_text: &str) -> Result<ExplainAnalyzeOutput> {
        self.explain_analyze_query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn explain_analyze_query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<ExplainAnalyzeOutput> {
        self.explain_analyze_query_with_params_access_control_internal(
            cypher_text,
            parameters,
            None,
        )
    }

    pub fn explain_analyze_query_with_params_access_control(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        access_control: QueryAccessControlContext,
    ) -> Result<ExplainAnalyzeOutput> {
        self.explain_analyze_query_with_params_access_control_internal(
            cypher_text,
            parameters,
            Some(access_control),
        )
    }

    pub(super) fn explain_analyze_query_with_params_access_control_internal(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        access_control: Option<QueryAccessControlContext>,
    ) -> Result<ExplainAnalyzeOutput> {
        self.runtime.get()?.store.ensure_usable()?;
        let statement = cypher::parse(cypher_text)?;
        let work_request = query_work_request_for_statement(&self.system_variables, &statement)?;
        let optimized = self.optimized_explain_query_plan_with_access_control(
            cypher_text,
            &statement,
            parameters,
            access_control.as_ref(),
        )?;
        if executor::requires_write_access(&optimized.physical_plan) {
            return Err(HawDBError::Execution(
                "EXPLAIN ANALYZE only supports read queries".to_string(),
            ));
        }
        let mut external = executor::NoExternalReadOperator;
        let profiled = {
            self.runtime.get_read()?.execute(
                executor::ExecutionRequest::new(
                    &optimized.physical_plan,
                    parameters,
                    &self.config.execution_memory,
                )
                .with_statement_clock(optimized.statement_clock)
                .with_csv_import(self.config.csv_import_policy().as_ref())
                .with_output_limits(
                    self.config.read_result_row_limit(),
                    self.config.max_read_result_payload_bytes,
                ),
                &mut external,
            )
        };
        self.runtime.get()?.store.poison_on_storage_error(&profiled);
        let profiled = profiled?;
        Ok(ExplainAnalyzeOutput {
            output: QueryOutput {
                rows: profiled.rows,
            },
            execution_profile: profiled.profile,
            physical_plan: optimized.physical_plan,
            trace: optimized.trace,
            work_request,
            plan_cache_lookup: optimized.plan_cache_lookup,
            statement_kind: statement_kind(statement_body(&statement)),
        })
    }

    pub fn plan_cache_stats(&self) -> Result<PlanCacheStats> {
        Ok(self.runtime.get()?.plan_cache.borrow().stats())
    }

    pub fn relational_plan_template_cache_stats(&self) -> Result<PlanCacheStats> {
        Ok(self.runtime.get()?.relational_plan_template_cache.stats())
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
                catalog: &self.runtime.get()?.catalog,
                store: &self.runtime.get()?.store,
                optimizer: &self.runtime.get()?.optimizer,
                config: &self.config,
                cache: &self.runtime.get()?.plan_cache,
                planning_cache: &self.runtime.get()?.optimizer_planning_cache,
                access_control,
                optimizer_search,
                scoring,
            },
        )
    }
}
