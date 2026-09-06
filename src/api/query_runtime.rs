use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
pub(crate) struct RuntimeAdmissionPlan {
    pub work_request: WorkRequest,
    pub is_mutation: bool,
    pub estimated_memory_bytes: u64,
    pub streaming_eligible: bool,
    pub required_io_slots: usize,
    pub parallel_execution_eligible: bool,
    pub max_parallelism: usize,
}

#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
pub(crate) struct PreparedRuntimeQuery {
    cypher_text: String,
    statement: cypher::Statement,
    optimized: Option<OptimizedQueryPlan>,
    optimizer_environment: Option<OptimizerEnvironmentKey>,
    admission: RuntimeAdmissionPlan,
    parse_metrics: skein_cypher::ParseMetrics,
}

#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
pub(crate) struct RuntimePlanningSnapshot {
    catalog: Catalog,
    store: GraphStore,
    published_read_view: PublishedReadView,
    optimizer: CascadesOptimizer,
    config: DatabaseConfig,
    system_variables: QuerySystemVariables,
    plan_cache: Arc<SharedState<PlanCache>>,
    planning_cache: Arc<SharedState<OptimizerPlanningCache>>,
    _pin: ReaderPin,
}

struct RuntimePlanningContext<'a> {
    catalog: &'a Catalog,
    store: &'a GraphStore,
    optimizer: &'a CascadesOptimizer,
    config: &'a DatabaseConfig,
    system_variables: &'a QuerySystemVariables,
}

#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
impl RuntimePlanningSnapshot {
    pub(crate) fn is_current_for(
        &self,
        database: &Database,
        prepared: &PreparedRuntimeQuery,
    ) -> bool {
        self.published_read_view == database.store.published_read_view()
            && self.config == database.config
            && self.system_variables == database.system_variables
            && prepared
                .optimizer_environment
                .as_ref()
                .is_none_or(|environment| environment.is_execution_compatible(&database.catalog))
    }

    pub(crate) fn prepare(
        &self,
        cypher_text: String,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<PreparedRuntimeQuery> {
        RuntimePlanningContext {
            catalog: &self.catalog,
            store: &self.store,
            optimizer: &self.optimizer,
            config: &self.config,
            system_variables: &self.system_variables,
        }
        .prepare(
            cypher_text,
            parameters,
            &self.plan_cache,
            &self.planning_cache,
        )
    }
}

pub(super) struct PreparedRuntimeExecution {
    pub(super) statement: cypher::Statement,
    pub(super) optimized: Option<OptimizedQueryPlan>,
    pub(super) parse_metrics: skein_cypher::ParseMetrics,
    pub(super) statement_started: Option<std::time::Instant>,
}

struct QueryExecutionOptions<'a> {
    capture_trace: bool,
    access_control: Option<QueryAccessControlContext>,
    task_context: Option<&'a skein_core::RuntimeTaskContext>,
}

pub(super) fn parse_runtime_execution(cypher_text: &str) -> Result<PreparedRuntimeExecution> {
    let statement_started = std::time::Instant::now();
    let parsed = skein_cypher::parse_profiled(cypher_text);
    Ok(PreparedRuntimeExecution {
        statement: parsed.result?,
        optimized: None,
        parse_metrics: parsed.metrics,
        statement_started: Some(statement_started),
    })
}

#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
impl PreparedRuntimeQuery {
    pub(crate) fn admission(&self) -> &RuntimeAdmissionPlan {
        &self.admission
    }

    pub(super) fn statement_kind(&self) -> &'static str {
        super::statement_kind(&self.statement)
    }

    pub(super) fn parse_nanos(&self) -> u64 {
        self.parse_metrics.elapsed_nanos
    }

    pub(super) fn uses_read_snapshot(&self) -> bool {
        if self.admission.is_mutation {
            return false;
        }
        // A plain EXPLAIN may contain a mutation plan without executing it. Snapshot
        // execution rejects that plan as a write, so keep this metadata-only path on
        // the existing exclusive executor until it has a dedicated read API.
        !matches!(
            super::statement_body(&self.statement),
            cypher::Statement::Explain(explain) if !explain.analyze
        )
    }

    pub(super) fn into_execution(
        self,
        catalog: &Catalog,
        _store: &GraphStore,
    ) -> (String, PreparedRuntimeExecution) {
        let environment_matches = self
            .optimizer_environment
            .as_ref()
            .is_some_and(|prepared| prepared.is_execution_compatible(catalog));
        (
            self.cypher_text,
            PreparedRuntimeExecution {
                statement: self.statement,
                optimized: environment_matches.then_some(self.optimized).flatten(),
                parse_metrics: self.parse_metrics,
                statement_started: None,
            },
        )
    }
}

impl RuntimeAdmissionPlan {
    #[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
    pub(crate) fn runtime_work_request(
        &self,
        result_budget_bytes: u64,
        limits: skein_qos::RuntimeGovernorLimits,
    ) -> skein_qos::RuntimeWorkRequest {
        self.runtime_work_request_with_capacity(
            result_budget_bytes,
            limits,
            limits.effective_cpu_slots.get(),
            limits.memory_budget_bytes,
        )
    }

    pub(crate) fn runtime_work_request_for_snapshot(
        &self,
        result_budget_bytes: u64,
        snapshot: skein_qos::RuntimeGovernorSnapshot,
    ) -> skein_qos::RuntimeWorkRequest {
        self.runtime_work_request_with_capacity(
            result_budget_bytes,
            snapshot.limits,
            snapshot
                .limits
                .effective_cpu_slots
                .get()
                .saturating_sub(snapshot.active_cpu_slots)
                .max(1),
            snapshot
                .limits
                .memory_budget_bytes
                .saturating_sub(snapshot.admitted_memory_bytes),
        )
    }

    fn runtime_work_request_with_capacity(
        &self,
        result_budget_bytes: u64,
        limits: skein_qos::RuntimeGovernorLimits,
        available_cpu_slots: usize,
        available_memory_bytes: u64,
    ) -> skein_qos::RuntimeWorkRequest {
        let priority = match self.work_request.priority {
            WorkPriority::Foreground => skein_qos::RuntimeWorkPriority::Foreground,
            WorkPriority::Background => skein_qos::RuntimeWorkPriority::Background,
        };
        if self.is_mutation {
            return skein_qos::RuntimeWorkRequest::mutation(priority, self.estimated_memory_bytes);
        }
        let kind = match self.work_request.class {
            WorkClass::Query | WorkClass::Mutation | WorkClass::Analytics => {
                skein_qos::RuntimeWorkKind::Query
            }
            WorkClass::Projection | WorkClass::Import => skein_qos::RuntimeWorkKind::Maintenance,
            WorkClass::Shadow => skein_qos::RuntimeWorkKind::Control,
        };
        let cpu_slots = self.admitted_cpu_slots(
            result_budget_bytes,
            limits,
            available_cpu_slots,
            available_memory_bytes,
        );
        skein_qos::RuntimeWorkRequest::query(
            priority,
            self.estimated_memory_bytes
                .saturating_mul(u64::try_from(cpu_slots).unwrap_or(u64::MAX)),
            result_budget_bytes,
        )
        .with_cpu_slots(cpu_slots)
        .with_kind(kind)
        .with_io_wave_slots(self.required_io_slots)
    }

    fn admitted_cpu_slots(
        &self,
        result_budget_bytes: u64,
        limits: skein_qos::RuntimeGovernorLimits,
        available_cpu_slots: usize,
        available_memory_bytes: u64,
    ) -> usize {
        if !self.parallel_execution_eligible {
            return 1;
        }
        let cpu_slots =
            crate::executor::default_morsel_cpu_ceiling(limits.effective_cpu_slots.get())
                .min(self.max_parallelism)
                .min(available_cpu_slots);
        if self.estimated_memory_bytes == 0 {
            return cpu_slots.max(1);
        }
        let memory_slots = available_memory_bytes
            .saturating_sub(result_budget_bytes)
            .checked_div(self.estimated_memory_bytes)
            .and_then(|slots| usize::try_from(slots).ok())
            .unwrap_or_default();
        cpu_slots.min(memory_slots.max(1)).max(1)
    }
}

#[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
const CONTROL_STATEMENT_MEMORY_BYTES: u64 = 1024 * 1024;

/// A cheap pre-parse reservation, not a measurement of allocator usage. The
/// source length is available without parsing or walking caller parameters.
pub(crate) fn runtime_planning_request(
    source_bytes: usize,
    priority: skein_qos::RuntimeWorkPriority,
) -> skein_qos::RuntimeWorkRequest {
    skein_qos::RuntimeWorkRequest::new(priority, skein_qos::RuntimeWorkKind::Control)
        .with_cpu_slots(1)
        .with_memory_bytes(
            CONTROL_STATEMENT_MEMORY_BYTES
                .saturating_add(u64::try_from(source_bytes).unwrap_or(u64::MAX)),
        )
}

impl Database {
    #[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
    pub(crate) fn runtime_planning_snapshot(&self) -> RuntimePlanningSnapshot {
        let (published_read_view, pin) = self.pin_read_view();
        RuntimePlanningSnapshot {
            catalog: self.catalog.clone(),
            store: self.store.snapshot(),
            // Store snapshots omit the writable durable handle and its checkpoint
            // metadata. Freshness must compare the original pinned publication.
            published_read_view,
            optimizer: self.optimizer.clone(),
            config: self.config.clone(),
            system_variables: self.system_variables.clone(),
            // Templates and their generation counter must have the same ownership.
            plan_cache: Arc::clone(&self.plan_cache),
            planning_cache: Arc::clone(&self.optimizer_planning_cache),
            _pin: pin,
        }
    }

    fn runtime_planning_context(&self) -> RuntimePlanningContext<'_> {
        RuntimePlanningContext {
            catalog: &self.catalog,
            store: &self.store,
            optimizer: &self.optimizer,
            config: &self.config,
            system_variables: &self.system_variables,
        }
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
        let plan_cache = SharedState::new(PlanCache::new(self.config.max_plan_cache_entries));
        let planning_cache = SharedState::new(self.optimizer_planning_cache.borrow().clone());
        self.runtime_planning_context()
            .prepare(
                cypher_text.to_string(),
                parameters,
                &plan_cache,
                &planning_cache,
            )
            .map(|prepared| prepared.admission)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn prepare_runtime_query(
        &self,
        cypher_text: String,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<PreparedRuntimeQuery> {
        self.runtime_planning_context().prepare(
            cypher_text,
            parameters,
            &self.plan_cache,
            &self.optimizer_planning_cache,
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
    ) -> Result<PreparedRuntimeQuery> {
        self.store.ensure_usable()?;
        let parsed = skein_cypher::parse_profiled(&cypher_text);
        let parse_metrics = parsed.metrics;
        let statement = parsed.result?;
        let work_request = query_work_request_for_statement(self.system_variables, &statement)?;
        let body = statement_body(&statement);
        let streaming_eligible = !matches!(body, cypher::Statement::Explain(_));
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
                )?;
                let is_mutation = executor::is_mutation_plan(&optimized.physical_plan)?;
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
                skein_plan::visit_plan(&optimized.physical_plan, &mut |node| {
                    if node.kind() == skein_plan::PhysicalPlanKind::SourceSegmentScan {
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
        })
    }

    fn optimized_query_plan_for_runtime_admission(
        &self,
        cypher_text: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
        plan_cache: &SharedState<PlanCache>,
        planning_cache: &SharedState<OptimizerPlanningCache>,
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
                access_control: None,
                optimizer_search,
            },
        )
    }
}

impl Database {
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

    pub(super) fn query_prepared_with_params(
        &mut self,
        prepared: PreparedRuntimeQuery,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        let (cypher_text, prepared) = prepared.into_execution(&self.catalog, &self.store);
        let mut external = executor::NoExternalReadOperator;
        self.query_with_params_trace_and_external_prepared(
            &cypher_text,
            prepared,
            parameters,
            &mut external,
            QueryExecutionOptions {
                capture_trace: false,
                access_control: None,
                task_context: None,
            },
        )
        .map(|(output, _)| output)
    }

    pub fn query_with_context(
        &mut self,
        cypher_text: &str,
        task_context: &skein_core::RuntimeTaskContext,
    ) -> Result<QueryOutput> {
        self.query_with_params_context(cypher_text, &BTreeMap::new(), task_context)
    }

    pub fn query_with_params_context(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        task_context: &skein_core::RuntimeTaskContext,
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

    #[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
    pub(crate) fn query_prepared_with_params_context(
        &mut self,
        prepared: PreparedRuntimeQuery,
        parameters: &BTreeMap<String, Value>,
        task_context: &skein_core::RuntimeTaskContext,
    ) -> Result<QueryOutput> {
        let (cypher_text, prepared) = prepared.into_execution(&self.catalog, &self.store);
        let mut external = executor::NoExternalReadOperator;
        self.query_with_params_trace_and_external_prepared(
            &cypher_text,
            prepared,
            parameters,
            &mut external,
            QueryExecutionOptions {
                capture_trace: false,
                access_control: None,
                task_context: Some(task_context),
            },
        )
        .map(|(output, _)| output)
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
        task_context: Option<&skein_core::RuntimeTaskContext>,
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
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        self.store.ensure_usable()?;
        self.query_with_params_trace_and_external_prepared(
            cypher_text,
            parse_runtime_execution(cypher_text)?,
            parameters,
            external,
            QueryExecutionOptions {
                capture_trace,
                access_control,
                task_context,
            },
        )
    }

    fn query_with_params_trace_and_external_prepared(
        &mut self,
        cypher_text: &str,
        prepared: PreparedRuntimeExecution,
        parameters: &BTreeMap<String, Value>,
        external: &mut dyn executor::ExternalReadOperator,
        options: QueryExecutionOptions<'_>,
    ) -> Result<(QueryOutput, QueryExecutionTrace)> {
        let QueryExecutionOptions {
            capture_trace,
            access_control,
            task_context,
        } = options;
        let execution_started = std::time::Instant::now();
        self.store.ensure_usable()?;
        query_runtime_checkpoint(task_context)?;
        let PreparedRuntimeExecution {
            statement,
            optimized: prepared_optimized,
            parse_metrics,
            statement_started,
        } = prepared;
        let started = statement_started.unwrap_or(execution_started);
        let body = statement_body(&statement);
        let statement_kind_name = statement_kind(&statement);
        if let cypher::Statement::Explain(explain) = &statement {
            let query_result = self
                .execute_explain_statement(
                    cypher_text,
                    explain,
                    parameters,
                    external,
                    access_control.as_ref(),
                    task_context,
                )
                .map(|output| (output, QueryExecutionTrace::uncached(statement)));
            self.store.poison_on_storage_error(&query_result);
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
                    access_control: access_control.as_ref(),
                    parse_nanos: parse_metrics.elapsed_nanos,
                    ..StatementExecutionContext::default()
                },
            );
            return query_result;
        }
        if let cypher::Statement::SetSystemVariable(set) = body {
            reject_system_variable_parameters(parameters)?;
            return self
                .system_variables
                .apply_set_system_variable(set)
                .map(|output| (output, QueryExecutionTrace::uncached(statement.clone())));
        }
        if matches!(body, cypher::Statement::Checkpoint) {
            if !parameters.is_empty() {
                return Err(SkeinError::Semantic(
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
        let query_result = (|| {
            query_runtime_checkpoint(task_context)?;
            query_work_request_for_statement(&self.system_variables, &statement)?;
            let optimized = match prepared_optimized {
                Some(optimized) if access_control.is_none() => optimized,
                _ => self.optimized_query_plan_with_access_control(
                    cypher_text,
                    &statement,
                    parameters,
                    access_control.as_ref(),
                )?,
            };
            let is_mutation = executor::is_mutation_plan(&optimized.physical_plan)?;
            if is_mutation {
                self.ensure_writable()?;
            }
            let (rows, execution_profile) = if is_mutation {
                query_runtime_checkpoint(task_context)?;
                (
                    executor::execute_mutation_with_limits(
                        &optimized.physical_plan,
                        &mut self.catalog,
                        &mut self.store,
                        self.config.mutation_limits,
                        task_context,
                    )?
                    .into(),
                    None,
                )
            } else {
                let profiled = match task_context {
                    Some(task_context) => {
                        executor::execute_with_output_limits_profile_and_external_and_context_and_memory(
                            &optimized.physical_plan,
                            &mut self.catalog,
                            &mut self.store,
                            parameters,
                            external,
                            self.config.max_read_result_rows,
                            self.config.max_read_result_payload_bytes,
                            task_context,
                            &self.config.execution_memory,
                        )
                    }
                    None => executor::execute_with_output_limits_profile_and_external_and_memory(
                        &optimized.physical_plan,
                        &mut self.catalog,
                        &mut self.store,
                        parameters,
                        external,
                        self.config.max_read_result_rows,
                        self.config.max_read_result_payload_bytes,
                        &self.config.execution_memory,
                    ),
                }?;
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
        self.store.poison_on_storage_error(&query_result);
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
                execution_profile,
                access_control: access_control.as_ref(),
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
        access_control: Option<&QueryAccessControlContext>,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<QueryOutput> {
        query_runtime_checkpoint(task_context)?;
        let work_request =
            query_work_request_for_statement(&self.system_variables, &explain.statement)?;
        let optimized = self.optimized_explain_query_plan_with_access_control(
            cypher_text,
            &explain.statement,
            parameters,
            access_control,
        )?;
        let inner_statement_kind = statement_kind(statement_body(&explain.statement));
        if explain.analyze {
            if executor::is_mutation_plan(&optimized.physical_plan)? {
                return Err(SkeinError::Execution(
                    "EXPLAIN ANALYZE only supports read queries".to_string(),
                ));
            }
            let profiled = match task_context {
                Some(task_context) => {
                    executor::execute_with_output_limits_profile_and_external_and_context_and_memory(
                        &optimized.physical_plan,
                        &mut self.catalog,
                        &mut self.store,
                        parameters,
                        external,
                        self.config.max_read_result_rows,
                        self.config.max_read_result_payload_bytes,
                        task_context,
                        &self.config.execution_memory,
                    )
                }
                None => executor::execute_with_output_limits_profile_and_external_and_memory(
                    &optimized.physical_plan,
                    &mut self.catalog,
                    &mut self.store,
                    parameters,
                    external,
                    self.config.max_read_result_rows,
                    self.config.max_read_result_payload_bytes,
                    &self.config.execution_memory,
                ),
            }?;
            return Ok(QueryOutput {
                rows: vec![explain_analyze_output_row(
                    &optimized,
                    work_request,
                    inner_statement_kind,
                    profiled.rows.len(),
                    &profiled.profile,
                )]
                .into(),
            });
        }
        Ok(QueryOutput {
            rows: vec![explain_output_row(
                &optimized,
                work_request,
                inner_statement_kind,
            )]
            .into(),
        })
    }
}

pub(super) fn query_runtime_checkpoint(
    task_context: Option<&skein_core::RuntimeTaskContext>,
) -> Result<()> {
    match task_context {
        Some(task_context) => task_context
            .checkpoint()
            .map_err(|reason| SkeinError::Execution(format!("runtime task stopped: {reason}"))),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{default_morsel_cpu_ceiling, MAX_MORSEL_PARALLELISM};
    use std::num::NonZeroUsize;

    fn limits(cpu_slots: usize, memory_budget_bytes: u64) -> skein_qos::RuntimeGovernorLimits {
        let cpu_slots = NonZeroUsize::new(cpu_slots).expect("test CPU slots are non-zero");
        skein_qos::RuntimeGovernorLimits {
            configured_cpu_slots: cpu_slots,
            effective_cpu_slots: cpu_slots,
            foreground_task_limit: cpu_slots,
            background_task_limit: cpu_slots,
            blocking_task_limit: cpu_slots,
            foreground_io_depth: NonZeroUsize::MIN,
            background_io_depth: NonZeroUsize::MIN,
            memory_capacity_bytes: memory_budget_bytes,
            memory_budget_bytes,
            result_budget_bytes: memory_budget_bytes,
        }
    }

    fn admission(parallel_execution_eligible: bool) -> RuntimeAdmissionPlan {
        RuntimeAdmissionPlan {
            work_request: WorkRequest::foreground(WorkClass::Query, 1),
            is_mutation: false,
            estimated_memory_bytes: 1024,
            streaming_eligible: true,
            required_io_slots: 0,
            parallel_execution_eligible,
            max_parallelism: if parallel_execution_eligible {
                MAX_MORSEL_PARALLELISM
            } else {
                1
            },
        }
    }

    #[test]
    fn default_morsel_request_uses_governed_cpu_and_memory_slots() {
        let request = admission(true).runtime_work_request(1024, limits(8, 64 * 1024));

        assert_eq!(request.cpu_slots, 4);
        assert_eq!(request.memory_bytes, 4 * 1024);
    }

    #[test]
    fn planning_cache_invalidation_does_not_reuse_an_in_flight_generation() {
        let db = Database::new();
        let planning = db.runtime_planning_snapshot();
        let parameters = BTreeMap::new();
        let before = planning
            .prepare(
                "MATCH (m:Memory) RETURN m.id AS id".to_string(),
                &parameters,
            )
            .unwrap();
        db.optimizer_planning_cache.borrow_mut().invalidate();
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
    fn mutation_planning_freshness_checks_data_schema_and_configuration() {
        let mut db = Database::new();
        let query = "CREATE (:Memory {id: 'new'})";
        let parameters = BTreeMap::new();
        let old = db.runtime_planning_snapshot();
        let prepared = old.prepare(query.to_string(), &parameters).unwrap();
        assert!(old.is_current_for(&db, &prepared));
        db.query("CREATE (:Memory {id: 'other'})").unwrap();
        assert!(!old.is_current_for(&db, &prepared));
        let current = db.runtime_planning_snapshot();
        let prepared = current.prepare(query.to_string(), &parameters).unwrap();
        assert!(current.is_current_for(&db, &prepared));
        db.config.max_read_result_rows = Some(1);
        assert!(!current.is_current_for(&db, &prepared));
        let current = db.runtime_planning_snapshot();
        assert!(current.is_current_for(&db, &prepared));
        db.system_variables.estimated_operations += 1;
        assert!(!current.is_current_for(&db, &prepared));
    }

    #[test]
    fn durable_mutation_planning_preserves_checkpoint_identity() {
        use skein_storage::StorageResidencyMode;

        for mode in [
            StorageResidencyMode::Materialized,
            StorageResidencyMode::OutOfCore,
        ] {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "skein-planning-durable-{}-{nonce}-{mode:?}",
                std::process::id()
            ));
            let config = DatabaseConfig {
                storage_residency_mode: mode,
                ..DatabaseConfig::default()
            };
            let parameters = BTreeMap::new();
            let query = "CREATE (:Memory {id: 'planned'})";
            let mut db = Database::open_with_config(&path, config.clone()).unwrap();
            db.query("CREATE (:Memory {id: 'existing'})").unwrap();
            let before = db.runtime_planning_snapshot();
            let prepared = before.prepare(query.to_string(), &parameters).unwrap();
            assert!(before.is_current_for(&db, &prepared));
            let epoch = db.store.commit_epoch();
            db.checkpoint().unwrap();
            assert_eq!(db.store.commit_epoch(), epoch);
            assert!(!before.is_current_for(&db, &prepared));
            drop(before);

            let checkpointed = db.runtime_planning_snapshot();
            let prepared = checkpointed
                .prepare(query.to_string(), &parameters)
                .unwrap();
            assert!(checkpointed.is_current_for(&db, &prepared), "{mode:?}");
            drop(checkpointed);
            drop(db);

            let mut db = Database::open_with_config(&path, config).unwrap();
            let reopened = db.runtime_planning_snapshot();
            let prepared = reopened.prepare(query.to_string(), &parameters).unwrap();
            assert!(reopened.is_current_for(&db, &prepared), "{mode:?}");
            db.query("CREATE (:Memory {id: 'concurrent'})").unwrap();
            assert!(!reopened.is_current_for(&db, &prepared));
            drop(reopened);
            drop(db);
            std::fs::remove_dir_all(&path).unwrap();
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
            .prepare(query.to_string(), &parameters)
            .unwrap();
        assert_eq!(
            first.optimized.as_ref().unwrap().plan_cache_lookup,
            PlanCacheLookup::Miss
        );
        let second = db
            .runtime_planning_snapshot()
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
        let planning = db.lock().unwrap().runtime_planning_snapshot();
        let guard = db.lock().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let prepared = planning.prepare(
                "MATCH (m:Memory) RETURN m.id AS id".to_string(),
                &BTreeMap::new(),
            );
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
        let old = db.runtime_planning_snapshot();
        assert_eq!(db.reader_pins.lock().unwrap().active_views.len(), 1);
        db.query("CREATE (:NewLabel {id: 'new'})").unwrap();
        let new = db.runtime_planning_snapshot();
        assert_eq!(db.reader_pins.lock().unwrap().active_views.len(), 2);
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
        assert_eq!(db.reader_pins.lock().unwrap().active_views.len(), 1);
        drop(new);
        assert!(db.reader_pins.lock().unwrap().active_views.is_empty());
    }

    #[test]
    fn source_segment_io_uses_wave_scoped_runtime_slots() {
        let mut admission = admission(false);
        admission.required_io_slots = 2;

        let request = admission.runtime_work_request(1024, limits(8, 64 * 1024));

        assert_eq!(request.io_slots, 2);
        assert_eq!(
            request.io_reservation_scope,
            skein_qos::RuntimeIoReservationScope::Wave
        );
    }

    #[test]
    fn default_morsel_cpu_ceiling_scales_with_effective_cpu_capacity() {
        assert_eq!(default_morsel_cpu_ceiling(1), 1);
        assert_eq!(default_morsel_cpu_ceiling(2), 2);
        assert_eq!(default_morsel_cpu_ceiling(4), 4);
        assert_eq!(default_morsel_cpu_ceiling(8), 4);
        assert_eq!(default_morsel_cpu_ceiling(16), 4);
        assert_eq!(default_morsel_cpu_ceiling(17), 5);
        assert_eq!(default_morsel_cpu_ceiling(32), 8);
        assert_eq!(default_morsel_cpu_ceiling(64), 16);
        assert_eq!(default_morsel_cpu_ceiling(128), 16);

        let request = admission(true).runtime_work_request(1024, limits(32, 1024 * 1024));
        assert_eq!(request.cpu_slots, 8);
        assert_eq!(request.memory_bytes, 8 * 1024);

        let request = admission(true).runtime_work_request(1024, limits(64, 1024 * 1024));
        assert_eq!(request.cpu_slots, 16);
        assert_eq!(request.memory_bytes, 16 * 1024);
    }

    #[test]
    fn default_morsel_request_falls_back_for_ineligible_or_tight_memory_work() {
        let serial = admission(false).runtime_work_request(1024, limits(8, 64 * 1024));
        let memory_limited = admission(true).runtime_work_request(2048, limits(8, 3072));
        let load_limited = admission(true).runtime_work_request_with_capacity(
            1024,
            limits(8, 64 * 1024),
            2,
            64 * 1024,
        );

        assert_eq!(serial.cpu_slots, 1);
        assert_eq!(memory_limited.cpu_slots, 1);
        assert_eq!(load_limited.cpu_slots, 2);
        assert_eq!(load_limited.memory_bytes, 2 * 1024);
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
            .into_execution(&db.catalog, &db.store);
        assert!(reusable.optimized.is_some());

        let reusable_after_data_change = db
            .prepare_runtime_query(query.to_string(), &parameters)
            .unwrap();
        db.query("CREATE (:Memory {id: 'newer'})").unwrap();
        let (_, reusable_after_data_change) =
            reusable_after_data_change.into_execution(&db.catalog, &db.store);
        assert!(reusable_after_data_change.optimized.is_some());
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
        let mut read = db.begin_read_transaction();

        let output = read
            .query_prepared_with_params_context(
                prepared,
                &parameters,
                &skein_core::RuntimeTaskContext::default(),
            )
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("id"),
            Some(&Value::String("prepared".to_string()))
        );
    }
}
