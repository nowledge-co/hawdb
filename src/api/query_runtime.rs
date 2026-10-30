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

use super::*;
use hawdb_executor::runtime_admission::{RuntimeAdmissionPlan, CONTROL_STATEMENT_MEMORY_BYTES};

#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
pub(crate) struct PreparedRuntimeQuery {
    branch_id: Option<hawdb_core::Uuid>,
    cypher_text: String,
    statement: cypher::Statement,
    optimized: Option<OptimizedQueryPlan>,
    optimizer_environment: Option<OptimizerEnvironmentKey>,
    admission: RuntimeAdmissionPlan,
    parse_metrics: hawdb_cypher::ParseMetrics,
    config: DatabaseConfig,
    system_variables: QuerySystemVariables,
    planning_memory: Option<hawdb_qos::RuntimeRetainedMemory>,
    access_control: Option<QueryAccessControlContext>,
    scoring_key: Option<super::query_request::ScoringPlanCacheKey>,
}

#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
pub(crate) struct RuntimePlanningSnapshot {
    branch_id: Option<hawdb_core::Uuid>,
    catalog: Catalog,
    store: GraphStore,
    optimizer: CascadesOptimizer,
    config: DatabaseConfig,
    system_variables: QuerySystemVariables,
    plan_cache: Arc<SharedState<PlanCache>>,
    planning_cache: Arc<SharedState<OptimizerPlanningCache>>,
    _pin: ReaderPin,
}

struct RuntimePlanningContext<'a> {
    branch_id: Option<hawdb_core::Uuid>,
    catalog: &'a Catalog,
    store: &'a GraphStore,
    optimizer: &'a CascadesOptimizer,
    config: &'a DatabaseConfig,
    system_variables: &'a QuerySystemVariables,
}

#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
impl RuntimePlanningSnapshot {
    pub(crate) fn accepts_prepared(&self, prepared: &PreparedRuntimeQuery) -> bool {
        prepared.is_compatible(
            self.branch_id,
            &self.config,
            &self.system_variables,
            &self.catalog,
            &self.store,
        )
    }

    #[cfg(test)]
    pub(crate) fn prepare(
        &self,
        cypher: String,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<PreparedRuntimeQuery> {
        self.prepare_request(QueryRequest::new(&cypher).with_params(parameters))
    }

    pub(crate) fn prepare_request(
        &self,
        request: QueryRequest<'_>,
    ) -> Result<PreparedRuntimeQuery> {
        RuntimePlanningContext {
            branch_id: self.branch_id,
            catalog: &self.catalog,
            store: &self.store,
            optimizer: &self.optimizer,
            config: &self.config,
            system_variables: &self.system_variables,
        }
        .prepare(
            request.cypher.to_owned(),
            request.parameters.unwrap_or(&BTreeMap::new()),
            &self.plan_cache,
            &self.planning_cache,
            QueryExecutionOptions::for_request(request, None),
        )
    }
}

pub(super) struct PreparedRuntimeExecution {
    pub(super) statement: cypher::Statement,
    pub(super) optimized: Option<OptimizedQueryPlan>,
    pub(super) parse_metrics: hawdb_cypher::ParseMetrics,
    pub(super) statement_started: Option<hawdb_core::time::Instant>,
    pub(super) planning_memory: Option<hawdb_qos::RuntimeRetainedMemory>,
    access_control: Option<QueryAccessControlContext>,
    scoring_key: Option<super::query_request::ScoringPlanCacheKey>,
}

impl PreparedRuntimeExecution {
    pub(super) fn take_optimized(
        &mut self,
        access: Option<&QueryAccessControlContext>,
        scoring: Option<BoundScoringRequest<'_>>,
    ) -> Result<Option<OptimizedQueryPlan>> {
        if self.access_control.as_ref() != access
            || self.scoring_key != scoring.map(BoundScoringRequest::cache_key)
        {
            return Ok(None);
        }
        let Some(mut optimized) = self.optimized.take() else {
            return Ok(None);
        };
        if let Some(scoring) = scoring {
            scoring.rebind(&mut optimized.physical_plan)?;
        }
        Ok(Some(optimized))
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct QueryExecutionOptions<'a> {
    pub(super) capture_trace: bool,
    pub(super) access_control: Option<&'a QueryAccessControlContext>,
    pub(super) task_context: Option<&'a hawdb_core::RuntimeTaskContext>,
    pub(super) scoring: Option<BoundScoringRequest<'a>>,
    pub(super) output_limits: QueryStreamOptions,
}

impl<'a> QueryExecutionOptions<'a> {
    pub(super) fn for_request(
        request: QueryRequest<'a>,
        fallback_context: Option<&'a hawdb_core::RuntimeTaskContext>,
    ) -> Self {
        Self {
            capture_trace: false,
            access_control: request.access_control,
            task_context: request.task_context.or(fallback_context),
            scoring: request
                .scoring
                .map(super::query_request::ScoringAttachment::bind),
            output_limits: request.output_limits,
        }
    }

    pub(super) fn for_bounded_read(
        max_rows: Option<usize>,
        access_control: Option<&'a QueryAccessControlContext>,
        task_context: Option<&'a hawdb_core::RuntimeTaskContext>,
    ) -> Self {
        Self {
            capture_trace: false,
            access_control,
            task_context,
            scoring: None,
            output_limits: QueryStreamOptions {
                max_rows,
                max_payload_bytes: None,
            },
        }
    }
}

pub(super) fn parse_runtime_execution(cypher_text: &str) -> Result<PreparedRuntimeExecution> {
    let statement_started = hawdb_core::time::Instant::now();
    let parsed = hawdb_cypher::parse_profiled(cypher_text);
    #[cfg(feature = "qualification")]
    super::query_preparation_metrics::record_parse(parsed.metrics.elapsed_nanos);
    Ok(PreparedRuntimeExecution {
        statement: parsed.result?,
        optimized: None,
        parse_metrics: parsed.metrics,
        statement_started: Some(statement_started),
        planning_memory: None,
        access_control: None,
        scoring_key: None,
    })
}

pub(crate) fn runtime_result_memory_budget(config: &DatabaseConfig, governor_budget: u64) -> u64 {
    // Per-statement logical payload limits cannot fund compact schema/value
    // containers. Admission keeps the host ceiling; output checks stay separate.
    config
        .max_read_result_payload_bytes
        .map(|bytes| u64::try_from(bytes).unwrap_or(u64::MAX))
        .map_or(governor_budget, |bytes| bytes.min(governor_budget))
}

#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
impl PreparedRuntimeQuery {
    pub(super) fn is_compatible(
        &self,
        branch_id: Option<hawdb_core::Uuid>,
        config: &DatabaseConfig,
        system_variables: &QuerySystemVariables,
        catalog: &Catalog,
        store: &GraphStore,
    ) -> bool {
        self.branch_id == branch_id
            && self.config == *config
            && self.system_variables == *system_variables
            && self
                .optimizer_environment
                .as_ref()
                .is_none_or(|environment| environment.is_execution_compatible(catalog, store))
    }

    pub(crate) fn with_planning_memory(mut self, memory: hawdb_qos::RuntimeRetainedMemory) -> Self {
        self.planning_memory = Some(memory);
        self
    }

    pub(crate) fn planning_memory_bytes(&self) -> u64 {
        self.planning_memory
            .as_ref()
            .map_or(0, |memory| memory.bytes())
    }

    pub(crate) fn admission(&self) -> &RuntimeAdmissionPlan {
        &self.admission
    }

    pub(super) fn statement_kind(&self) -> &'static str {
        super::statement_kind(&self.statement)
    }

    pub(super) fn parse_nanos(&self) -> u64 {
        self.parse_metrics.elapsed_nanos
    }

    pub(super) fn take_query_identity(
        &mut self,
        cypher_text: &str,
    ) -> Option<hawdb_query::QueryIdentity> {
        // An incompatible request must not inherit the prepared text's hash,
        // even when execution is refused before the ordinary request guards.
        if self.cypher_text != cypher_text {
            return None;
        }
        self.optimized.as_mut()?.query_identity.take()
    }

    pub(crate) fn uses_external_seed(&self) -> bool {
        let mut required = false;
        if let Some(optimized) = &self.optimized {
            hawdb_plan_cypher::visit_plan(&optimized.physical_plan, &mut |plan| {
                required |= matches!(
                    plan,
                    hawdb_plan_cypher::PhysicalPlan::TextSeedScan { .. }
                        | hawdb_plan_cypher::PhysicalPlan::VectorSeedScan { .. }
                );
            });
        }
        required
    }

    pub(crate) fn uses_read_snapshot(&self) -> bool {
        !self.admission.is_mutation
            && !self
                .optimized
                .as_ref()
                .is_some_and(|optimized| executor::requires_write_access(&optimized.physical_plan))
    }

    pub(crate) fn uses_borrowed_read(&self) -> bool {
        self.uses_read_snapshot()
            && !matches!(self.statement, cypher::Statement::Explain(_))
            && self
                .physical_plan()
                .is_some_and(|plan| hawdb_executor::batch::BatchPlanRef::try_new(plan).is_some())
    }

    pub(crate) fn uses_transaction_kernel(&self) -> bool {
        !matches!(
            statement_body(&self.statement),
            cypher::Statement::Explain(_)
        ) && self.optimized.as_ref().is_some_and(|optimized| {
            matches!(
                optimized.physical_plan.class(),
                hawdb_plan_cypher::PhysicalPlanClass::Schema
                    | hawdb_plan_cypher::PhysicalPlanClass::Mutation
            )
        })
    }

    pub(super) fn physical_plan(&self) -> Option<&PhysicalPlan> {
        self.optimized
            .as_ref()
            .map(|optimized| &optimized.physical_plan)
    }

    pub(super) fn matches_request(&self, request: QueryRequest<'_>) -> bool {
        self.cypher_text == request.cypher
            && self.access_control.as_ref() == request.access_control
            && self.scoring_key == request.scoring.map(|scoring| scoring.bind().cache_key())
    }

    pub(super) fn into_execution<R: hawdb_storage::graph_engine::GraphReadEngine>(
        self,
        catalog: &Catalog,
        store: &R,
    ) -> (String, PreparedRuntimeExecution) {
        let environment_matches = self
            .optimizer_environment
            .as_ref()
            .is_some_and(|prepared| prepared.is_execution_compatible(catalog, store));
        (
            self.cypher_text,
            PreparedRuntimeExecution {
                statement: self.statement,
                optimized: environment_matches.then_some(self.optimized).flatten(),
                parse_metrics: self.parse_metrics,
                statement_started: None,
                planning_memory: self.planning_memory,
                access_control: self.access_control,
                scoring_key: self.scoring_key,
            },
        )
    }
}

impl Database {
    fn validate_prepared_branch(&self, prepared: &PreparedRuntimeQuery) -> Result<()> {
        if prepared.branch_id != self.current_branch()?.map(|current| current.info.id) {
            return Err(HawDBError::Semantic(
                "prepared statement belongs to a different branch".into(),
            ));
        }
        Ok(())
    }

    fn validate_prepared_request(
        &self,
        prepared: &PreparedRuntimeQuery,
        request: QueryRequest<'_>,
    ) -> Result<()> {
        self.validate_prepared_branch(prepared)?;
        if let Some(access) = request.access_control {
            self.config
                .runtime_capabilities
                .require(hawdb_core::RuntimeCapability::AccessControl)?;
            access.validate()?;
        }
        if !prepared.matches_request(request) || !self.accepts_prepared_query(prepared)? {
            return Err(HawDBError::Execution(
                "prepared execution context changed; prepare and admit again".into(),
            ));
        }
        Ok(())
    }

    #[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
    pub(crate) fn runtime_planning_snapshot(&self) -> Result<RuntimePlanningSnapshot> {
        let runtime = self.runtime.get()?;
        Ok({
            let (_, pin) = Self::pin_runtime_read_view(&runtime);
            RuntimePlanningSnapshot {
                branch_id: self
                    .current_branch_from_runtime(&runtime)
                    .map(|current| current.info.id),
                catalog: runtime.catalog.clone(),
                store: runtime.store.snapshot_for_read(),
                optimizer: runtime.optimizer.clone(),
                config: self.config.clone(),
                system_variables: self.system_variables.clone(),
                // Templates and their generation counter must have the same ownership.
                plan_cache: Arc::clone(&runtime.plan_cache),
                planning_cache: Arc::clone(&runtime.optimizer_planning_cache),
                _pin: pin,
            }
        })
    }

    fn runtime_planning_context<'a>(
        &'a self,
        runtime: &'a super::runtime_cell::AdmittedBranchRuntime,
    ) -> Result<RuntimePlanningContext<'a>> {
        Ok({
            RuntimePlanningContext {
                branch_id: self
                    .current_branch_from_runtime(runtime)
                    .map(|current| current.info.id),
                catalog: &runtime.catalog,
                store: &runtime.store,
                optimizer: &runtime.optimizer,
                config: &self.config,
                system_variables: &self.system_variables,
            }
        })
    }

    pub fn query_work_request(&self) -> WorkRequest {
        self.system_variables.query_work_request()
    }

    pub fn query_work_request_for(&self, cypher_text: &str) -> Result<WorkRequest> {
        let statement = cypher::parse(cypher_text)?;
        query_work_request_for_statement(&self.system_variables, &statement)
    }

    #[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
    pub(crate) fn runtime_admission_plan(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<RuntimeAdmissionPlan> {
        self.prepare_runtime_query(cypher_text.to_string(), parameters)
            .map(|prepared| prepared.admission)
    }

    pub(crate) fn prepare_runtime_query(
        &self,
        cypher_text: String,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<PreparedRuntimeQuery> {
        let runtime = self.runtime.get()?;
        self.runtime_planning_context(&runtime)?.prepare(
            cypher_text,
            parameters,
            &self.runtime.get()?.plan_cache,
            &self.runtime.get()?.optimizer_planning_cache,
            QueryExecutionOptions::default(),
        )
    }
    pub(crate) fn prepare_runtime_request(
        &self,
        request: QueryRequest<'_>,
    ) -> Result<PreparedRuntimeQuery> {
        let runtime = self.runtime.get()?;
        self.runtime_planning_context(&runtime)?.prepare(
            request.cypher.to_string(),
            request.parameters.unwrap_or(&BTreeMap::new()),
            &runtime.plan_cache,
            &runtime.optimizer_planning_cache,
            QueryExecutionOptions::for_request(request, None),
        )
    }

    pub(crate) fn accepts_prepared_query(&self, prepared: &PreparedRuntimeQuery) -> Result<bool> {
        let runtime = self.runtime.get()?;
        Ok(prepared.is_compatible(
            self.current_branch_from_runtime(&runtime)
                .map(|current| current.info.id),
            &self.config,
            &self.system_variables,
            &runtime.catalog,
            &runtime.store,
        ))
    }
}

impl DatabaseReadSnapshot {
    pub(super) fn accepts_prepared(&self, prepared: &PreparedRuntimeQuery) -> bool {
        let source = &self.0;
        prepared.is_compatible(
            source
                .current_branch
                .as_ref()
                .map(|current| current.info.id),
            &source.config,
            &source.system_variables,
            &source.catalog,
            &source.store,
        )
    }

    pub(super) fn prepare_runtime_request(
        &self,
        request: QueryRequest<'_>,
        plan_cache: &SharedState<PlanCache>,
    ) -> Result<PreparedRuntimeQuery> {
        let source = &self.0;
        RuntimePlanningContext {
            branch_id: source
                .current_branch
                .as_ref()
                .map(|current| current.info.id),
            catalog: &source.catalog,
            store: &source.store,
            optimizer: &source.optimizer,
            config: &source.config,
            system_variables: &source.system_variables,
        }
        .prepare(
            request.cypher.to_string(),
            request.parameters.unwrap_or(&BTreeMap::new()),
            plan_cache,
            &source.optimizer_planning_cache,
            QueryExecutionOptions::for_request(request, None),
        )
    }
}

impl RuntimePlanningContext<'_> {
    fn prepare(
        &self,
        cypher_text: String,
        parameters: &BTreeMap<String, Value>,
        plan_cache: &SharedState<PlanCache>,
        planning_cache: &SharedState<OptimizerPlanningCache>,
        options: QueryExecutionOptions<'_>,
    ) -> Result<PreparedRuntimeQuery> {
        self.store.ensure_usable()?;
        let parsed = hawdb_cypher::parse_profiled(&cypher_text);
        #[cfg(feature = "qualification")]
        super::query_preparation_metrics::record_parse(parsed.metrics.elapsed_nanos);
        let parse_metrics = parsed.metrics;
        let statement = parsed.result?;
        let work_request = query_work_request_for_statement(self.system_variables, &statement)?;
        let body = statement_body(&statement);
        let mut streaming_eligible = !matches!(body, cypher::Statement::Explain(_));
        let mut prepared_optimized = None;
        let mut optimizer_environment = None;
        let (
            is_mutation,
            estimated_memory_bytes,
            required_io_slots,
            parallel_execution_eligible,
            max_parallelism,
        ) = match body {
            cypher::Statement::Explain(explain) if !explain.analyze => {
                (false, CONTROL_STATEMENT_MEMORY_BYTES, 0, false, 1)
            }
            cypher::Statement::SetSystemVariable(_)
            | cypher::Statement::Checkpoint
            | cypher::Statement::BeginTransaction
            | cypher::Statement::Commit
            | cypher::Statement::Rollback => (true, CONTROL_STATEMENT_MEMORY_BYTES, 0, false, 1),
            _ => {
                let admission_statement = match body {
                    cypher::Statement::Explain(explain) => &explain.statement,
                    _ => &statement,
                };
                let optimized = self.optimized_query_plan_for_runtime_admission(
                    &cypher_text,
                    admission_statement,
                    parameters,
                    plan_cache,
                    planning_cache,
                    options,
                )?;
                let is_mutation = executor::is_mutation_plan(&optimized.physical_plan)?;
                streaming_eligible &= !executor::requires_write_access(&optimized.physical_plan);
                let estimated_memory_bytes = if is_mutation {
                    executor::estimated_mutation_memory_bytes(
                        self.config.mutation_limits,
                        self.config.max_wal_record_bytes,
                    )
                } else {
                    executor::estimated_execution_memory(
                        &optimized.physical_plan,
                        &self.config.execution_memory,
                    )
                    .total_bytes
                };
                let mut required_io_slots = 0;
                hawdb_plan_cypher::visit_plan(&optimized.physical_plan, &mut |node| {
                    if node.kind() == hawdb_plan_cypher::PhysicalPlanKind::SourceSegmentScan {
                        required_io_slots =
                            required_io_slots.max(crate::executor::SOURCE_SEGMENT_SCAN_IO_DEPTH);
                    }
                });
                let parallel_morsel_eligible = !is_mutation
                    && executor::supports_default_morsel_parallelism(
                        &optimized.physical_plan,
                        self.catalog,
                    );
                let morsel_parallelism = if parallel_morsel_eligible {
                    executor::default_morsel_parallelism(
                        &optimized.physical_plan,
                        self.catalog,
                        self.store,
                        &self.config.execution_memory,
                    )
                } else {
                    1
                };
                let external_read_parallelism = if is_mutation {
                    1
                } else {
                    executor::max_external_read_parallelism(&optimized.physical_plan)
                };
                let parallel_execution_eligible =
                    parallel_morsel_eligible || external_read_parallelism > 1;
                let max_parallelism = morsel_parallelism.max(external_read_parallelism);
                optimizer_environment = Some(optimized.optimizer_environment.clone());
                prepared_optimized = Some(optimized);
                (
                    is_mutation,
                    estimated_memory_bytes,
                    required_io_slots,
                    parallel_execution_eligible,
                    max_parallelism,
                )
            }
        };
        Ok(PreparedRuntimeQuery {
            branch_id: self.branch_id,
            cypher_text,
            statement,
            optimized: prepared_optimized,
            optimizer_environment,
            admission: RuntimeAdmissionPlan {
                work_request,
                is_mutation,
                estimated_memory_bytes,
                streaming_eligible,
                required_io_slots,
                parallel_execution_eligible,
                max_parallelism,
            },
            parse_metrics,
            config: self.config.clone(),
            system_variables: self.system_variables.clone(),
            planning_memory: None,
            access_control: options.access_control.cloned(),
            scoring_key: options.scoring.map(BoundScoringRequest::cache_key),
        })
    }

    fn optimized_query_plan_for_runtime_admission(
        &self,
        cypher_text: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
        plan_cache: &SharedState<PlanCache>,
        planning_cache: &SharedState<OptimizerPlanningCache>,
        options: QueryExecutionOptions<'_>,
    ) -> Result<OptimizedQueryPlan> {
        let optimizer_search =
            query_statement_variables_for_statement(self.system_variables, statement)?
                .optimizer_search;
        let cache_mode = if optimizer_search != OptimizerSearchDirective::Auto {
            PlanCacheMode::Bypass(plan_cache::PlanCacheBypassReason::OptimizerDirective)
        } else if statement_uses_plan_cache(statement) {
            PlanCacheMode::Use
        } else {
            PlanCacheMode::Bypass(plan_cache::PlanCacheBypassReason::StatementNotCacheable)
        };
        optimized_query_plan_for(
            cypher_text,
            statement,
            parameters,
            cache_mode,
            PlanTraceMode::Template,
            PlanCacheContext {
                catalog: self.catalog,
                store: self.store,
                optimizer: self.optimizer,
                config: self.config,
                cache: plan_cache,
                planning_cache,
                access_control: options.access_control,
                optimizer_search,
                scoring: options.scoring,
            },
        )
    }
}

impl Database {
    /// Executes an ordinary Cypher request with optional typed engine scoring.
    pub fn query_request(&mut self, request: QueryRequest<'_>) -> Result<QueryOutput> {
        let options = QueryExecutionOptions::for_request(request, None);
        let empty = BTreeMap::new();
        self.query_with_params_trace_and_external_prepared(
            request.cypher,
            parse_runtime_execution(request.cypher)?,
            request.parameters.unwrap_or(&empty),
            &mut executor::NoExternalReadOperator,
            options,
        )
        .map(|(output, _)| output)
    }

    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.query_with_params_trace_internal(cypher_text, parameters, false, None, None)
            .map(|(output, _)| output)
    }

    pub(crate) fn query_prepared_request(
        &mut self,
        prepared: PreparedRuntimeQuery,
        request: QueryRequest<'_>,
    ) -> Result<QueryOutput> {
        self.validate_prepared_request(&prepared, request)?;
        let runtime = self.runtime.get()?;
        let (cypher_text, prepared) = prepared.into_execution(&runtime.catalog, &runtime.store);
        drop(runtime);
        let mut external = executor::NoExternalReadOperator;
        self.query_with_params_trace_and_external_prepared(
            &cypher_text,
            prepared,
            request.parameters.unwrap_or(&BTreeMap::new()),
            &mut external,
            QueryExecutionOptions::for_request(request, None),
        )
        .map(|(output, _)| output)
    }

    pub fn query_with_context(
        &mut self,
        cypher_text: &str,
        task_context: &hawdb_core::RuntimeTaskContext,
    ) -> Result<QueryOutput> {
        self.query_with_params_context(cypher_text, &BTreeMap::new(), task_context)
    }

    pub fn query_with_params_context(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        task_context: &hawdb_core::RuntimeTaskContext,
    ) -> Result<QueryOutput> {
        self.query_with_params_trace_internal(
            cypher_text,
            parameters,
            false,
            None,
            Some(task_context),
        )
        .map(|(output, _)| output)
    }

    pub(crate) fn query_prepared_request_with_trace_and_external(
        &mut self,
        prepared: PreparedRuntimeQuery,
        request: QueryRequest<'_>,
        capture_trace: bool,
        external: &mut dyn executor::ExternalReadOperator,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        self.validate_prepared_request(&prepared, request)?;
        let runtime = self.runtime.get()?;
        let (cypher_text, prepared) = prepared.into_execution(&runtime.catalog, &runtime.store);
        drop(runtime);
        let mut options = QueryExecutionOptions::for_request(request, None);
        options.capture_trace = capture_trace;
        self.query_with_params_trace_and_external_prepared(
            &cypher_text,
            prepared,
            request.parameters.unwrap_or(&BTreeMap::new()),
            external,
            options,
        )
    }

    pub fn query_with_params_access_control(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        access_control: QueryAccessControlContext,
    ) -> Result<QueryOutput> {
        self.query_with_params_trace_internal(
            cypher_text,
            parameters,
            false,
            Some(access_control),
            None,
        )
        .map(|(output, _)| output)
    }

    fn query_with_params_trace_internal(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        capture_trace: bool,
        access_control: Option<QueryAccessControlContext>,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        let mut external = executor::NoExternalReadOperator;
        self.query_with_params_trace_and_external_with_context(
            cypher_text,
            parameters,
            capture_trace,
            &mut external,
            access_control,
            task_context,
        )
    }

    pub(crate) fn query_with_params_trace_and_external_with_context(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        capture_trace: bool,
        external: &mut dyn executor::ExternalReadOperator,
        access_control: Option<QueryAccessControlContext>,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        self.runtime.get()?.store.ensure_usable()?;
        self.query_with_params_trace_and_external_prepared(
            cypher_text,
            parse_runtime_execution(cypher_text)?,
            parameters,
            external,
            QueryExecutionOptions {
                capture_trace,
                access_control: access_control.as_ref(),
                task_context,
                scoring: None,
                output_limits: QueryStreamOptions::default(),
            },
        )
    }

    fn query_with_params_trace_and_external_prepared(
        &mut self,
        cypher_text: &str,
        mut prepared: PreparedRuntimeExecution,
        parameters: &BTreeMap<String, Value>,
        external: &mut dyn executor::ExternalReadOperator,
        options: QueryExecutionOptions<'_>,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        let _planning_memory = prepared.planning_memory.take();
        let prepared_optimized =
            prepared.take_optimized(options.access_control, options.scoring)?;
        let QueryExecutionOptions {
            capture_trace,
            access_control,
            task_context,
            scoring,
            output_limits,
        } = options;
        let execution_started = hawdb_core::time::Instant::now();
        self.runtime.get()?.store.ensure_usable()?;
        query_runtime_checkpoint(task_context)?;
        let PreparedRuntimeExecution {
            statement,
            parse_metrics,
            statement_started,
            ..
        } = prepared;
        let started = statement_started.unwrap_or(execution_started);
        let body = statement_body(&statement);
        let statement_kind_name = statement_kind(&statement);
        if let cypher::Statement::Explain(explain) = &statement {
            let query_result = self
                .execute_explain_statement(cypher_text, explain, parameters, external, options)
                .map(|output| (output, QueryExecutionTrace::uncached(statement)));
            self.runtime
                .get()?
                .store
                .poison_on_storage_error(&query_result);
            let statement_result = match &query_result {
                Ok((output, _)) => Ok(output),
                Err(error) => Err(error),
            };
            self.record_statement_execution(
                "cypher",
                cypher_text,
                statement_kind_name,
                started,
                statement_result,
                StatementExecutionContext {
                    access_control,
                    parse_nanos: parse_metrics.elapsed_nanos,
                    ..StatementExecutionContext::default()
                },
            );
            return query_result;
        }
        if scoring.is_some()
            && matches!(
                body,
                cypher::Statement::SetSystemVariable(_) | cypher::Statement::Checkpoint
            )
        {
            return Err(HawDBError::Semantic("scoring requires a read query".into()));
        }
        if let cypher::Statement::SetSystemVariable(set) = body {
            reject_system_variable_parameters(parameters)?;
            return apply_set_system_variable(&mut self.system_variables, set)
                .map(|output| (output, QueryExecutionTrace::uncached(statement.clone())));
        }
        if matches!(body, cypher::Statement::Checkpoint) {
            if !parameters.is_empty() {
                return Err(HawDBError::Semantic(
                    "CHECKPOINT does not accept parameters".to_string(),
                ));
            }
            self.checkpoint()?;
            return Ok((
                QueryOutput {
                    rows: Vec::new().into(),
                },
                QueryExecutionTrace::uncached(statement),
            ));
        }
        let mut query_identity = None;
        let query_result = (|| {
            query_runtime_checkpoint(task_context)?;
            query_work_request_for_statement(&self.system_variables, &statement)?;
            let mut optimized = match prepared_optimized {
                Some(optimized) => optimized,
                _ => self.optimized_query_plan_for_request(
                    cypher_text,
                    &statement,
                    parameters,
                    access_control,
                    PlanTraceMode::Template,
                    scoring,
                )?,
            };
            query_identity = optimized.query_identity.take();
            let is_mutation = executor::is_mutation_plan(&optimized.physical_plan)?;
            if is_mutation {
                self.ensure_writable()?;
            }
            let (rows, execution_profile) = if is_mutation {
                query_runtime_checkpoint(task_context)?;
                let mut mutation_limits = self.config.mutation_limits;
                // Graph mutation payloads use MutationLimits, checked by the
                // executor preflight and storage tracker before publication.
                if !matches!(
                    optimized.physical_plan,
                    PhysicalPlan::SetNodePropertiesReturn { .. }
                ) && let Some(limit) = output_limits.max_rows.and_then(NonZeroUsize::new)
                {
                    mutation_limits.max_result_rows = mutation_limits.max_result_rows.min(limit);
                }
                (
                    optimized.collect_named_rows({
                        let mut branch_runtime_access = self.runtime.get_mut()?;
                        let branch_runtime = &mut *branch_runtime_access;
                        executor::execute_mutation_with_store_output_limits(
                            &optimized.physical_plan,
                            &mut branch_runtime.catalog,
                            &mut branch_runtime.store,
                            mutation_limits,
                            hawdb_executor::result_delivery::OutputLimits {
                                max_rows: restrictive_query_limit(
                                    self.config.read_result_row_limit(),
                                    output_limits.max_rows,
                                ),
                                max_payload_bytes: restrictive_query_limit(
                                    self.config.max_read_result_payload_bytes,
                                    output_limits.max_payload_bytes,
                                ),
                            },
                            task_context,
                        )
                    }?)?,
                    None,
                )
            } else {
                let request = executor::ExecutionRequest::new(
                    &optimized.physical_plan,
                    parameters,
                    &self.config.execution_memory,
                )
                .with_output_columns(optimized.output_columns.as_deref())
                .with_output_limits(
                    restrictive_query_limit(
                        self.config.read_result_row_limit(),
                        output_limits.max_rows,
                    ),
                    restrictive_query_limit(
                        self.config.max_read_result_payload_bytes,
                        output_limits.max_payload_bytes,
                    ),
                )
                .with_optional_task_context(task_context)
                .with_optional_host_scorer(scoring.and_then(BoundScoringRequest::host_scorer));
                let profiled = if executor::requires_write_access(&optimized.physical_plan) {
                    // Projection registration is a procedure result, but its
                    // WAL append must use the same guard as other writes.
                    let mut access = self.runtime.get_mut()?;
                    let branch_runtime = &mut *access;
                    executor::execute_with_request(
                        request,
                        executor::ExecutionResources::new(
                            &mut branch_runtime.catalog,
                            &mut branch_runtime.store,
                            external,
                        ),
                    )?
                } else {
                    self.runtime.get_read()?.execute(request, external)?
                };
                (profiled.rows, Some(profiled.profile))
            };
            if !is_mutation {
                query_runtime_checkpoint(task_context)?;
            }
            Ok((
                QueryOutput { rows },
                QueryExecutionTrace {
                    statement,
                    optimizer_trace: capture_trace.then_some(optimized.trace),
                    plan_cache_lookup: Some(optimized.plan_cache_lookup),
                    execution_profile,
                },
            ))
        })();
        self.runtime
            .get()?
            .store
            .poison_on_storage_error(&query_result);
        let statement_result = match &query_result {
            Ok((output, _)) => Ok(output),
            Err(error) => Err(error),
        };
        let execution_profile = query_result
            .as_ref()
            .ok()
            .and_then(|(_, trace)| trace.execution_profile.as_ref());
        self.record_statement_execution(
            "cypher",
            cypher_text,
            statement_kind_name,
            started,
            statement_result,
            StatementExecutionContext {
                query_identity: query_identity.as_ref(),
                execution_profile,
                retained_profile: None,
                access_control,
                parse_nanos: parse_metrics.elapsed_nanos,
            },
        );
        query_result
    }

    fn execute_explain_statement(
        &mut self,
        cypher_text: &str,
        explain: &cypher::Explain,
        parameters: &BTreeMap<String, Value>,
        external: &mut dyn executor::ExternalReadOperator,
        options: QueryExecutionOptions<'_>,
    ) -> Result<QueryOutput> {
        let task_context = options.task_context;
        query_runtime_checkpoint(task_context)?;
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
            let profiled = self.runtime.get_read()?.execute(
                executor::ExecutionRequest::new(
                    &optimized.physical_plan,
                    parameters,
                    &self.config.execution_memory,
                )
                .with_output_columns(optimized.output_columns.as_deref())
                .with_output_limits(
                    restrictive_query_limit(
                        self.config.read_result_row_limit(),
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
                external,
            )?;
            let row = explain_analyze_output_row(
                &optimized,
                work_request,
                inner_statement_kind,
                profiled.rows.len(),
                &profiled.profile,
            );
            drop(profiled.rows);
            return super::explain::admit_explain_output(row, true, &self.config, options);
        }
        super::explain::admit_explain_output(
            explain_output_row(&optimized, work_request, inner_statement_kind),
            false,
            &self.config,
            options,
        )
    }
}

pub(super) fn query_runtime_checkpoint(
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<()> {
    match task_context {
        Some(task_context) => task_context
            .checkpoint()
            .map_err(|reason| HawDBError::Execution(format!("runtime task stopped: {reason}"))),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_explain_uses_a_read_snapshot_without_executing_writes() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'existing'})").unwrap();
        let before = db.graph_rag_schema_context(Default::default()).unwrap();
        let publication = crate::api::PublishedConcurrentRead::capture(&db).unwrap();
        for statement in [
            "CREATE (:Memory {id: 'planned'})",
            "CREATE NODE TABLE Planned",
        ] {
            let query = format!("EXPLAIN {statement}");
            let request = QueryRequest::new(&query);
            let prepared = publication.prepare(request).unwrap();
            assert!(prepared.uses_read_snapshot());
            assert!(!prepared.admission().is_mutation);
            assert_eq!(prepared.admission().required_io_slots, 0);
            let mut read = publication
                .begin_read_transaction_with_context(&Default::default())
                .unwrap();
            let output = read
                .query_prepared_request(prepared, request)
                .unwrap()
                .output;
            assert_eq!(output.rows.len(), 1);
            assert!(matches!(&output.rows[0]["plan"], Value::String(plan) if !plan.is_empty()));
            assert!(executor::requires_write_access(
                &read.explain_query(statement).unwrap().physical_plan
            ));
            let analyze = format!("EXPLAIN ANALYZE {statement}");
            assert!(!publication
                .prepare(QueryRequest::new(&analyze))
                .unwrap()
                .uses_read_snapshot());
            assert!(read
                .query(&analyze)
                .unwrap_err()
                .to_string()
                .contains("EXPLAIN ANALYZE only supports read queries"));
            for limits in [
                QueryStreamOptions {
                    max_rows: Some(0),
                    max_payload_bytes: None,
                },
                QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(0),
                },
            ] {
                assert!(read
                    .query_request(request.with_output_limits(limits))
                    .is_err());
            }
            assert_eq!(read.graph_rag_schema_context(Default::default()), before);
        }
        assert_eq!(
            db.graph_rag_schema_context(Default::default()).unwrap(),
            before
        );
        assert_eq!(
            db.query("MATCH (m:Memory) RETURN m.id AS id")
                .unwrap()
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn snapshot_explain_preserves_captured_query_settings_and_statement_overrides() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'existing'})").unwrap();
        db.query("SET system.work_priority = 'background'").unwrap();
        db.query("SET system.work_class = 'import'").unwrap();
        db.query("SET system.estimated_operations = 8").unwrap();
        let direct = db.begin_read_transaction().unwrap();
        let publication = crate::api::PublishedConcurrentRead::capture(&db).unwrap();
        db.query("SET system.work_priority = 'foreground'").unwrap();
        db.query("SET system.work_class = 'query'").unwrap();
        db.query("SET system.estimated_operations = 1").unwrap();
        let fork = publication
            .begin_read_transaction_with_context(&Default::default())
            .unwrap();
        for mut read in [direct, fork] {
            let query = "MATCH (m:Memory) RETURN m.id AS id";
            let expected = WorkRequest::background(WorkClass::Import, 8);
            assert_eq!(read.explain_query(query).unwrap().work_request, expected);
            let output = read.query(&format!("EXPLAIN {query}")).unwrap();
            let Value::Map(work) = &output.rows[0]["work_request"] else {
                panic!("missing work request");
            };
            assert_eq!(work["priority"], Value::String("background".into()));
            assert_eq!(work["class"], Value::String("import".into()));
            assert_eq!(work["estimated_operations"], Value::Int(8));
            let hinted = read
                .explain_query(
                    "CYPHER system.work_priority = 'foreground' system.estimated_operations = 3 \
                 system.optimizer_search = 'direct_fallback' MATCH (m:Memory) RETURN m.id AS id",
                )
                .unwrap();
            assert_eq!(
                hinted.work_request,
                WorkRequest::foreground(WorkClass::Import, 3)
            );
            assert_eq!(hinted.trace.search_mode.as_str(), "direct_fallback");
            assert_eq!(read.explain_query(query).unwrap().work_request, expected);
        }
        assert_eq!(
            db.begin_read_transaction()
                .unwrap()
                .explain_query("MATCH (m:Memory) RETURN m.id AS id")
                .unwrap()
                .work_request,
            WorkRequest::foreground(WorkClass::Query, 1)
        );
    }

    #[test]
    fn planning_cache_invalidation_does_not_reuse_an_in_flight_generation() {
        let db = Database::new();
        let planning = db.runtime_planning_snapshot().unwrap();
        let parameters = BTreeMap::new();
        let before = planning
            .prepare(
                "MATCH (m:Memory) RETURN m.id AS id".to_string(),
                &parameters,
            )
            .unwrap();
        db.runtime
            .get()
            .unwrap()
            .optimizer_planning_cache
            .borrow_mut()
            .invalidate();
        let after = planning
            .prepare(
                "MATCH (m:Memory) RETURN m.id AS id".to_string(),
                &parameters,
            )
            .unwrap();
        assert_ne!(before.optimizer_environment, after.optimizer_environment);
        assert_eq!(
            after.optimized.unwrap().plan_cache_lookup,
            PlanCacheLookup::Miss
        );
    }

    #[test]
    fn mutation_preparation_reuses_data_changes_but_checks_configuration() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'initial'})").unwrap();
        let query = "CREATE (:Memory {id: 'new'})";
        let old = db.runtime_planning_snapshot().unwrap();
        let prepared = old.prepare(query.to_string(), &BTreeMap::new()).unwrap();
        assert!(db.accepts_prepared_query(&prepared).unwrap());
        db.query("CREATE (:Memory {id: 'other'})").unwrap();
        assert!(db.accepts_prepared_query(&prepared).unwrap());
        db.config.max_read_result_rows = Some(1);
        assert!(!db.accepts_prepared_query(&prepared).unwrap());
        let current = db.runtime_planning_snapshot().unwrap();
        let prepared = current
            .prepare(query.to_string(), &BTreeMap::new())
            .unwrap();
        assert!(db.accepts_prepared_query(&prepared).unwrap());
        db.system_variables.estimated_operations += 1;
        assert!(!db.accepts_prepared_query(&prepared).unwrap());
    }

    #[test]
    fn prepared_execution_rejects_changed_configuration_and_request() {
        for changed_config in [false, true] {
            let mut db = Database::new();
            let text = "CREATE NODE TABLE Prepared";
            let prepared = db.prepare_runtime_request(QueryRequest::new(text)).unwrap();
            if changed_config {
                db.config.max_read_result_rows = Some(1);
            }
            let request = QueryRequest::new(if changed_config {
                text
            } else {
                "CREATE NODE TABLE Other"
            });
            let epoch = db.commit_epoch().unwrap();
            assert!(db
                .query_prepared_request(prepared, request)
                .unwrap_err()
                .to_string()
                .contains("prepare and admit again"));
            assert_eq!(db.commit_epoch().unwrap(), epoch);
        }
    }

    #[test]
    fn prepared_query_identity_rejects_mismatched_transfer_and_records_current_text() {
        let db = Database::new_with_config(DatabaseConfig {
            slow_query_log_threshold_micros: 0,
            slow_query_log_capacity: 8,
            statement_summary_capacity: 8,
            ..DatabaseConfig::default()
        });
        let original = "MATCH (n:Memory) RETURN 1 AS value";
        let current = " match (n:Memory) return 2 as value; ";
        let original_identity = hawdb_query::QueryIdentity::new("cypher", original);
        let current_identity = hawdb_query::QueryIdentity::new("cypher", current);
        assert_eq!(
            original_identity.query_digest(),
            current_identity.query_digest()
        );
        assert_ne!(
            original_identity.query_text_hash(),
            current_identity.query_text_hash()
        );
        let mut prepared = db
            .prepare_runtime_request(QueryRequest::new(original))
            .unwrap();
        let error = db
            .validate_prepared_request(&prepared, QueryRequest::new(current))
            .unwrap_err();
        let identity = prepared.take_query_identity(current);
        assert!(identity.is_none());
        db.record_statement_execution(
            "cypher",
            current,
            prepared.statement_kind(),
            hawdb_core::time::Instant::now(),
            Err(&error),
            StatementExecutionContext {
                query_identity: identity.as_ref(),
                ..Default::default()
            },
        );
        let summary = db.statement_summary.borrow().snapshot();
        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0].digest, current_identity.query_digest());
        assert_eq!(
            summary[0].sample_query_text_hash,
            current_identity.query_text_hash()
        );
        assert_eq!(summary[0].error_count, 1);
        assert_eq!(summary[0].success_count, 0);
        assert!(db.slow_query_log_snapshot().is_empty());
        assert_eq!(
            prepared.take_query_identity(original),
            Some(original_identity)
        );
        assert!(prepared.take_query_identity(original).is_none());
    }

    #[test]
    fn mutation_preparation_rebinds_after_checkpoint_and_reopen() {
        use hawdb_storage::config::StorageResidencyMode;

        for mode in [
            StorageResidencyMode::Materialized,
            StorageResidencyMode::OutOfCore,
        ] {
            let path = std::env::temp_dir().join(format!(
                "hawdb-planning-durable-{}-{mode:?}",
                hawdb_core::generate_uuidv7().unwrap()
            ));
            let config = DatabaseConfig {
                storage_residency_mode: mode,
                ..Default::default()
            };
            let mut db = Database::open_with_config(&path, config.clone()).unwrap();
            db.query("CREATE (:Memory {id: 'existing'})").unwrap();
            for id in ["checkpointed", "reopened"] {
                let planning = db.runtime_planning_snapshot().unwrap();
                let request_text = "CREATE (:Memory {id: $id})";
                let parameters = BTreeMap::from([("id".into(), Value::String(id.into()))]);
                let prepared = planning.prepare(request_text.into(), &parameters).unwrap();
                drop(planning);
                db.checkpoint().unwrap();
                // Physical generation replacement alone must not reject compatible
                // syntax. The fresh kernel workspace owns the actual write source.
                assert!(db.accepts_prepared_query(&prepared).unwrap(), "{mode:?}");
                db.query_prepared_request(
                    prepared,
                    QueryRequest::new(request_text).with_params(&parameters),
                )
                .unwrap();
                drop(db);
                db = Database::open_with_config(&path, config.clone()).unwrap();
            }
            assert_eq!(
                db.query("MATCH (m:Memory) RETURN m.id AS id")
                    .unwrap()
                    .rows
                    .len(),
                3
            );
            drop(db);
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn planning_snapshot_reuses_the_database_template_cache() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'existing'})").unwrap();
        let query = "MATCH (m:Memory) WHERE m.id = $id RETURN m.id AS id";
        let parameters =
            BTreeMap::from([("id".to_string(), Value::String("existing".to_string()))]);
        let first = db
            .runtime_planning_snapshot()
            .unwrap()
            .prepare(query.to_string(), &parameters)
            .unwrap();
        assert_eq!(
            first.optimized.as_ref().unwrap().plan_cache_lookup,
            PlanCacheLookup::Miss
        );
        let second = db
            .runtime_planning_snapshot()
            .unwrap()
            .prepare(query.to_string(), &parameters)
            .unwrap();
        assert_eq!(
            second.optimized.as_ref().unwrap().plan_cache_lookup,
            PlanCacheLookup::Hit
        );
        let direct = db
            .prepare_runtime_query(query.to_string(), &parameters)
            .unwrap();
        assert_eq!(
            direct.optimized.as_ref().unwrap().plan_cache_lookup,
            PlanCacheLookup::Hit
        );
        assert_eq!(first.admission, second.admission);
        assert_eq!(second.admission, direct.admission);
    }

    #[test]
    fn planning_snapshot_does_not_require_the_database_mutex() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'existing'})").unwrap();
        let db = Mutex::new(db);
        let planning = db.lock().unwrap().runtime_planning_snapshot().unwrap();
        let guard = db.lock().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let prepared =
                planning.prepare_request(QueryRequest::new("MATCH (m:Memory) RETURN m.id AS id"));
            sender
                .send(prepared.map(|prepared| prepared.admission.is_mutation))
                .unwrap();
        });
        // Keep the writer mutex held until the independent planner reports completion.
        let result = receiver.recv_timeout(std::time::Duration::from_secs(5));
        drop(guard);
        worker.join().unwrap();
        assert!(!result.unwrap().unwrap());
    }

    #[test]
    fn planning_snapshots_keep_schema_keys_separate_and_release_reader_pins() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'existing'})").unwrap();
        let old = db.runtime_planning_snapshot().unwrap();
        assert_eq!(
            db.runtime
                .get()
                .unwrap()
                .reader_pins
                .lock()
                .unwrap()
                .active_views
                .len(),
            1
        );
        db.query("CREATE (:NewLabel {id: 'new'})").unwrap();
        let new = db.runtime_planning_snapshot().unwrap();
        assert_eq!(
            db.runtime
                .get()
                .unwrap()
                .reader_pins
                .lock()
                .unwrap()
                .active_views
                .len(),
            2
        );
        assert!(old.catalog.label_id("NewLabel").is_none());
        assert!(new.catalog.label_id("NewLabel").is_some());
        let query = "MATCH (m:Memory) RETURN m.id AS id";
        let parameters = BTreeMap::new();
        let old_prepared = old.prepare(query.to_string(), &parameters).unwrap();
        let new_prepared = new.prepare(query.to_string(), &parameters).unwrap();
        assert_ne!(
            old_prepared.optimizer_environment,
            new_prepared.optimizer_environment
        );
        assert_eq!(
            old_prepared.optimized.as_ref().unwrap().plan_cache_lookup,
            PlanCacheLookup::Miss
        );
        assert_eq!(
            new_prepared.optimized.as_ref().unwrap().plan_cache_lookup,
            PlanCacheLookup::Miss
        );
        // An older snapshot may populate the shared cache after a newer one.
        let old_again = old.prepare(query.to_string(), &parameters).unwrap();
        let old_warm = old.prepare(query.to_string(), &parameters).unwrap();
        let new_again = new.prepare(query.to_string(), &parameters).unwrap();
        let new_warm = new.prepare(query.to_string(), &parameters).unwrap();
        // Republishing different statistics advances the shared generation; it
        // must not alias a generation independently reused by another snapshot.
        assert_ne!(
            old_again.optimizer_environment,
            old_prepared.optimizer_environment
        );
        assert_ne!(
            new_again.optimizer_environment,
            new_prepared.optimizer_environment
        );
        assert_eq!(
            old_again.optimizer_environment,
            old_warm.optimizer_environment
        );
        assert_eq!(
            new_again.optimizer_environment,
            new_warm.optimizer_environment
        );
        assert_eq!(
            old_warm.optimized.as_ref().unwrap().plan_cache_lookup,
            PlanCacheLookup::Hit
        );
        assert_eq!(
            new_warm.optimized.as_ref().unwrap().plan_cache_lookup,
            PlanCacheLookup::Hit
        );
        drop(old);
        assert_eq!(
            db.runtime
                .get()
                .unwrap()
                .reader_pins
                .lock()
                .unwrap()
                .active_views
                .len(),
            1
        );
        drop(new);
        assert!(db
            .runtime
            .get()
            .unwrap()
            .reader_pins
            .lock()
            .unwrap()
            .active_views
            .is_empty());
    }

    #[test]
    fn prepared_runtime_query_reuses_plan_across_data_changes_with_compatible_schema() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'existing'})").unwrap();
        let query = "MATCH (m:Memory) WHERE m.id = $id RETURN m.id AS id";
        let parameters =
            BTreeMap::from([("id".to_string(), Value::String("existing".to_string()))]);

        let (_, reusable) = db
            .prepare_runtime_query(query.to_string(), &parameters)
            .unwrap()
            .into_execution(
                &db.runtime.get().unwrap().catalog,
                &db.runtime.get().unwrap().store,
            );
        assert!(reusable.optimized.is_some());

        let reusable_after_data_change = db
            .prepare_runtime_query(query.to_string(), &parameters)
            .unwrap();
        db.query("CREATE (:Memory {id: 'newer'})").unwrap();
        let (_, reusable_after_data_change) = reusable_after_data_change.into_execution(
            &db.runtime.get().unwrap().catalog,
            &db.runtime.get().unwrap().store,
        );
        assert!(reusable_after_data_change.optimized.is_some());
    }

    #[test]
    fn preparation_reuse_revalidates_execution_context_without_retaining_a_reader_pin() {
        let mut db = Database::new();
        db.query("CREATE (:Sample {id: 'first'})").unwrap();
        let query = "MATCH (n:Sample) RETURN n.id AS id ORDER BY id";
        let parameters = BTreeMap::new();
        let prepared = {
            let planning = db.runtime_planning_snapshot().unwrap();
            planning
                .prepare_request(QueryRequest::new(query).with_params(&parameters))
                .unwrap()
        };
        assert!(db
            .runtime
            .get()
            .unwrap()
            .reader_pins
            .lock()
            .unwrap()
            .active_views
            .is_empty());
        db.query("CREATE (:Sample {id: 'second'})").unwrap();
        let mut current = db.runtime_planning_snapshot().unwrap();
        assert!(current.accepts_prepared(&prepared));
        current.config.max_read_result_rows = Some(1);
        assert!(!current.accepts_prepared(&prepared));
        current.config = prepared.config.clone();
        current.system_variables.estimated_operations += 1;
        assert!(!current.accepts_prepared(&prepared));
        current.system_variables = prepared.system_variables.clone();
        current.branch_id = Some(hawdb_core::Uuid::nil());
        assert!(!current.accepts_prepared(&prepared));
        drop(current);
        let output = db
            .begin_read_transaction()
            .unwrap()
            .query_prepared_request(prepared, QueryRequest::new(query).with_params(&parameters))
            .unwrap()
            .output;
        assert_eq!(output.rows.len(), 2);

        let prepared = db
            .prepare_runtime_query(query.to_string(), &parameters)
            .unwrap();
        db.query("CREATE (:OtherLabel {id: 'schema-change'})")
            .unwrap();
        assert!(!db
            .runtime_planning_snapshot()
            .unwrap()
            .accepts_prepared(&prepared));
    }

    #[test]
    fn explain_analyze_reserves_the_inner_plan_memory() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'explain-admission'})")
            .unwrap();
        let query = "MATCH (m:Memory) RETURN m.id AS id";
        let parameters = BTreeMap::new();

        let query_admission = db
            .prepare_runtime_query(query.to_string(), &parameters)
            .unwrap()
            .admission;
        let analyze_admission = db
            .prepare_runtime_query(format!("EXPLAIN ANALYZE {query}"), &parameters)
            .unwrap()
            .admission;
        let explain_admission = db
            .prepare_runtime_query(format!("EXPLAIN {query}"), &parameters)
            .unwrap()
            .admission;

        assert_eq!(
            analyze_admission.estimated_memory_bytes,
            query_admission.estimated_memory_bytes
        );
        assert_eq!(
            explain_admission.estimated_memory_bytes,
            CONTROL_STATEMENT_MEMORY_BYTES
        );
    }

    #[test]
    fn prepared_runtime_query_executes_against_a_read_snapshot() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'prepared'})").unwrap();
        let query = "MATCH (m:Memory) WHERE m.id = $id RETURN m.id AS id";
        let parameters =
            BTreeMap::from([("id".to_string(), Value::String("prepared".to_string()))]);
        let prepared = db
            .prepare_runtime_query(query.to_string(), &parameters)
            .unwrap();
        let mut read = db.begin_read_transaction().unwrap();

        let output = read
            .query_prepared_request(prepared, QueryRequest::new(query).with_params(&parameters))
            .unwrap()
            .output;

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("id"),
            Some(&Value::String("prepared".to_string()))
        );
    }
}
