// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Transaction execution, commit and SQL session control.

use super::*;

impl DatabaseTransactionRuntime {
    pub(super) fn from_database(db: &Database) -> Result<Self> {
        Self::from_database_with_system_variables(
            db,
            db.system_variables.clone(),
            SessionConfig::default(),
        )
    }

    pub(super) fn from_database_with_system_variables(
        db: &Database,
        system_variables: QuerySystemVariables,
        session_config: SessionConfig,
    ) -> Result<Self> {
        let runtime = db.runtime.get()?;
        Ok(Self {
            instance_identity: db.instance_identity()?,
            session_config,
            optimizer: runtime.optimizer.clone(),
            plan_cache: SharedState::new(PlanCache::new(db.config.max_plan_cache_entries)),
            relational_plan_template_cache: Arc::clone(&runtime.relational_plan_template_cache),
            optimizer_planning_cache: SharedState::new(
                runtime.optimizer_planning_cache.borrow().clone(),
            ),
            config: db.config.clone(),
            system_variables,
            branch_catalog_path: db.branch_catalog_path_from_runtime(&runtime).ok(),
            current_branch: db.current_branch_from_runtime(&runtime),
        })
    }

    pub(super) fn read_result_row_limit(&self) -> Option<usize> {
        restrictive_query_limit(
            self.config.read_result_row_limit(),
            self.session_config.row_limit(),
        )
    }

    pub(super) fn validate_result_rows(&self, rows: usize) -> Result<()> {
        if let Some(limit) = self.read_result_row_limit()
            && rows > limit
        {
            return Err(HawDBError::read_budget_exceeded(
                crate::ReadBudgetResource::Rows,
                limit,
                "session result exceeds the effective host/session row ceiling",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_result_payload(
        &self,
        bytes: usize,
        ceiling: Option<usize>,
    ) -> Result<()> {
        if let Some(limit) = ceiling
            && bytes > limit
        {
            return Err(HawDBError::read_budget_exceeded(
                crate::ReadBudgetResource::PayloadBytes,
                limit,
                "SQL mutation result exceeds the effective host/admitted payload ceiling",
            ));
        }
        Ok(())
    }

    pub(super) fn ensure_writable(&self, store: &GraphStore) -> Result<()> {
        store.ensure_usable()?;
        if self.config.read_only {
            return Err(HawDBError::Execution(
                "database is opened in read-only mode".to_string(),
            ));
        }
        Ok(())
    }
}

impl DatabaseTransactionState {
    pub(super) fn from_database(db: &Database) -> Result<Self> {
        let runtime = db.runtime.get()?;
        let (_, pin) = Database::pin_runtime_read_view(&runtime);
        Ok(Self {
            snapshot_pin: Some(pin),
            read_dependencies: None,
            graph_transaction: Some(runtime.store.begin_mutation_transaction(&runtime.catalog)),
            relational_transaction: hawdb_storage::relational::RelationalTransaction::default(),
            relational_state: runtime.store.relational_state().clone(),
            append_transaction: hawdb_storage::append_table::AppendTransaction::default(),
            append_state: runtime.store.append_state().clone(),
            pending_generated_append_tables: BTreeSet::new(),
            relational_returning: Vec::new(),
            relational_index: runtime
                .store
                .begin_authoritative_relational_transaction_index()
                .map_err(|error| error.to_string()),
            relational_rows: runtime
                .store
                .begin_authoritative_relational_transaction_rows()
                .map_err(|error| error.to_string()),
        })
    }

    pub(crate) fn rollback(&mut self) {
        self.read_dependencies.take();
        self.graph_transaction.take();
        self.relational_transaction.writes.clear();
        self.append_transaction.writes.clear();
        self.pending_generated_append_tables.clear();
        self.relational_returning.clear();
        self.relational_index = Ok(None);
        self.relational_rows = Ok(None);
        self.snapshot_pin.take();
    }

    pub(crate) fn restore_graph_statement(&mut self, savepoint: GraphMutationSavepoint) {
        self.graph_transaction
            .as_mut()
            .expect("database transaction must own a graph workspace")
            .restore(savepoint);
    }

    pub(super) fn take_for_commit(&mut self) -> Self {
        Self {
            read_dependencies: self.read_dependencies.take(),
            snapshot_pin: self.snapshot_pin.take(),
            graph_transaction: self.graph_transaction.take(),
            relational_transaction: std::mem::take(&mut self.relational_transaction),
            relational_state: std::mem::take(&mut self.relational_state),
            append_transaction: std::mem::take(&mut self.append_transaction),
            append_state: std::mem::take(&mut self.append_state),
            pending_generated_append_tables: std::mem::take(
                &mut self.pending_generated_append_tables,
            ),
            relational_returning: std::mem::take(&mut self.relational_returning),
            relational_index: std::mem::replace(&mut self.relational_index, Ok(None)),
            relational_rows: std::mem::replace(&mut self.relational_rows, Ok(None)),
        }
    }

    pub(super) fn authoritative_relational_index(
        &self,
    ) -> Result<&crate::store::RelationalTransactionIndexView> {
        match &self.relational_index {
            Ok(Some(index)) => Ok(index),
            Ok(None) => Err(HawDBError::StorageIntegrity(
                "authoritative transaction index view is unavailable".to_string(),
            )),
            Err(error) => Err(HawDBError::StorageIntegrity(format!(
                "authoritative transaction index view could not be pinned: {error}"
            ))),
        }
    }

    pub(super) fn stage_sparse_authoritative_relational_statement(
        &self,
        transaction: hawdb_storage::relational::RelationalTransaction,
    ) -> Result<Option<SparseRelationalStatementStage>> {
        let rows = match &self.relational_rows {
            Ok(Some(rows)) => rows,
            Ok(None) => return Ok(None),
            Err(error) => {
                return Err(HawDBError::StorageIntegrity(format!(
                    "authoritative transaction row view could not be pinned: {error}"
                )));
            }
        };
        let index = match &self.relational_index {
            Ok(Some(index)) => index,
            Ok(None) => {
                return Err(HawDBError::StorageIntegrity(
                    "authoritative transaction index view is unavailable".to_string(),
                ));
            }
            Err(error) => {
                return Err(HawDBError::StorageIntegrity(format!(
                    "authoritative transaction index view could not be pinned: {error}"
                )));
            }
        };
        let store = self
            .graph_transaction
            .as_ref()
            .expect("database transaction must own a graph workspace")
            .store();
        let (next, index_capture, row_capture, mutation_outcomes) = store
            .stage_sparse_relational_transaction_statement(
                &self.relational_state,
                rows,
                transaction,
                index,
            )
            .map_err(map_transaction_relational_error)?;
        Ok(Some(SparseRelationalStatementStage {
            state: next,
            mutation_outcomes,
            index_capture,
            row_capture,
        }))
    }
}

pub(super) struct GraphTransactionStatementOptions<'a> {
    pub(super) execution: query_runtime::QueryExecutionOptions<'a>,
    pub(super) read_dependencies:
        Option<&'a mut transaction_read_dependencies::TransactionReadDependencies>,
}

pub(super) fn prepare_graph_transaction_plan(
    runtime: &DatabaseTransactionRuntime,
    transaction: &GraphMutationTransaction,
    system_variables: &QuerySystemVariables,
    cypher_text: &str,
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
    execution: query_runtime::QueryExecutionOptions<'_>,
) -> Result<OptimizedQueryPlan> {
    let task_context = execution.task_context;
    query_runtime::query_runtime_checkpoint(task_context)?;
    query_work_request_for_statement(system_variables, statement)?;
    let optimizer_search =
        query_statement_variables_for_statement(system_variables, statement)?.optimizer_search;
    optimized_query_plan_for(
        cypher_text,
        statement,
        parameters,
        PlanCacheMode::Bypass(PlanCacheBypassReason::MutationPlanning),
        PlanTraceMode::Template,
        PlanCacheContext {
            catalog: transaction.catalog(),
            store: transaction.store(),
            optimizer: &runtime.optimizer,
            config: &runtime.config,
            cache: &runtime.plan_cache,
            planning_cache: &runtime.optimizer_planning_cache,
            access_control: execution.access_control,
            optimizer_search,
            scoring: execution.scoring,
        },
    )
}

pub(super) fn execute_graph_transaction_statement(
    runtime: &DatabaseTransactionRuntime,
    transaction: &mut GraphMutationTransaction,
    system_variables: &QuerySystemVariables,
    cypher_text: &str,
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
    options: GraphTransactionStatementOptions<'_>,
) -> Result<GraphTransactionStatementOutcome> {
    let optimized = prepare_graph_transaction_plan(
        runtime,
        transaction,
        system_variables,
        cypher_text,
        statement,
        parameters,
        options.execution,
    )?;
    execute_graph_transaction_plan(
        runtime,
        transaction,
        statement,
        &optimized,
        parameters,
        options,
    )
}

pub(super) fn execute_graph_transaction_plan(
    runtime: &DatabaseTransactionRuntime,
    transaction: &mut GraphMutationTransaction,
    statement: &cypher::Statement,
    optimized: &OptimizedQueryPlan,
    parameters: &BTreeMap<String, Value>,
    options: GraphTransactionStatementOptions<'_>,
) -> Result<GraphTransactionStatementOutcome> {
    let GraphTransactionStatementOptions {
        execution,
        read_dependencies,
    } = options;
    let task_context = execution.task_context;
    query_runtime::query_runtime_checkpoint(task_context)?;

    if let Some(dependencies) = read_dependencies {
        dependencies.collect_cypher(
            statement,
            &optimized.physical_plan,
            transaction.catalog(),
            transaction.store(),
        )?;
    }
    if executor::is_mutation_plan(&optimized.physical_plan)? {
        runtime.ensure_writable(transaction.store())?;
        let is_mutation_return = matches!(
            optimized.physical_plan,
            PhysicalPlan::SetNodePropertiesReturn { .. }
        );
        let mut mutation_limits = match &optimized.physical_plan {
            PhysicalPlan::SetNodePropertiesReturn {
                returns: crate::planner::SetNodePropertiesReturnMode::Count { .. },
                ..
            } => hawdb_storage::mutation::MutationLimits {
                max_result_rows: runtime.config.mutation_limits.max_affected_rows,
                max_result_payload_bytes: std::num::NonZeroUsize::new(usize::MAX)
                    .expect("usize::MAX is non-zero"),
                ..runtime.config.mutation_limits
            },
            _ => runtime.config.mutation_limits,
        };
        if !is_mutation_return
            && let Some(limit) = execution.output_limits.max_rows.and_then(NonZeroUsize::new)
        {
            mutation_limits.max_result_rows = mutation_limits.max_result_rows.min(limit);
        }
        let statement_savepoint = transaction.savepoint();
        let execution = (|| {
            let summary = match &optimized.physical_plan {
                PhysicalPlan::UnwindMutation {
                    rows,
                    variable,
                    operation,
                } => transaction.stage_mutations_with_limits(
                    executor::materialize_unwind_mutations(
                        rows,
                        variable,
                        operation,
                        mutation_limits,
                        task_context,
                    )?,
                    mutation_limits,
                )?,
                _ => {
                    let command = if is_mutation_return {
                        hawdb_executor::mutation::materialize_node_mutation(
                            &optimized.physical_plan,
                            transaction.catalog(),
                            transaction.store(),
                            mutation_limits,
                            task_context,
                        )?
                    } else {
                        match executor::mutation_command(&optimized.physical_plan) {
                            Ok(command) => command,
                            Err(error) => {
                                match hawdb_executor::mutation::materialize_node_mutation(
                                    &optimized.physical_plan,
                                    transaction.catalog(),
                                    transaction.store(),
                                    mutation_limits,
                                    task_context,
                                )? {
                                    Some(command) => Some(command),
                                    None => return Err(error),
                                }
                            }
                        }
                    };
                    let mutation = command.ok_or_else(|| {
                        HawDBError::Execution(
                            "transaction mutation plan cannot be represented as a staged mutation"
                                .to_string(),
                        )
                    })?;
                    if is_mutation_return {
                        transaction.stage_mutation_without_commit_rows(mutation, mutation_limits)?
                    } else {
                        transaction.stage_mutation_with_limits(mutation, mutation_limits)?
                    }
                }
            };
            let returned_rows = executor::project_staged_mutation_return_rows(
                &optimized.physical_plan,
                transaction.catalog(),
                transaction.store(),
                &summary.rows,
                runtime.config.mutation_limits,
            )?;
            let rows = returned_rows.unwrap_or_default();
            let output = QueryOutput {
                rows: optimized.collect_named_rows(rows)?,
            };
            hawdb_executor::result_delivery::OutputLimits {
                max_rows: restrictive_query_limit(
                    runtime.read_result_row_limit(),
                    execution.output_limits.max_rows,
                ),
                max_payload_bytes: restrictive_query_limit(
                    runtime.config.max_read_result_payload_bytes,
                    execution.output_limits.max_payload_bytes,
                ),
            }
            .validate(
                output.rows.len(),
                if output.rows.is_empty() {
                    0
                } else {
                    output.rows.payload_bytes()
                },
            )?;
            Ok((
                output,
                transaction.lock_footprint_since(&statement_savepoint)?,
            ))
        })();
        return match execution {
            Ok((output, lock_footprint)) => Ok(GraphTransactionStatementOutcome {
                output,
                lock_footprint,
                savepoint: Some(statement_savepoint),
            }),
            Err(error) => {
                transaction.restore(statement_savepoint);
                Err(error)
            }
        };
    }

    let query_result = {
        let (catalog, store) = transaction.catalog_and_store_mut();
        let mut external = executor::NoExternalReadOperator;
        let profiled = executor::execute_with_request(
            executor::ExecutionRequest::new(
                &optimized.physical_plan,
                parameters,
                &runtime.config.execution_memory,
            )
            .with_output_columns(optimized.output_columns.as_deref())
            .with_output_limits(
                restrictive_query_limit(
                    runtime.read_result_row_limit(),
                    execution.output_limits.max_rows,
                ),
                restrictive_query_limit(
                    runtime.config.max_read_result_payload_bytes,
                    execution.output_limits.max_payload_bytes,
                ),
            )
            .with_optional_task_context(task_context)
            .with_optional_host_scorer(
                execution
                    .scoring
                    .and_then(query_request::BoundScoringRequest::host_scorer),
            ),
            executor::ExecutionResources::new(catalog, store, &mut external),
        );
        profiled.map(|profiled| QueryOutput {
            rows: profiled.rows,
        })
    };
    transaction.store().poison_on_storage_error(&query_result);
    query_result.map(|output| GraphTransactionStatementOutcome {
        output,
        savepoint: None,
        lock_footprint: GraphMutationLockFootprint::default(),
    })
}

/// One statement-local preparation. Its borrowed request keeps parameters,
/// authorization, scoring and the combined task context identical during replay.
/// The transaction's admission remains held while locks are acquired.
pub(super) struct PreparedGraphTransactionRequest<'a> {
    pub(super) request: QueryRequest<'a>,
    pub(super) statement: cypher::Statement,
    pub(super) optimized: OptimizedQueryPlan,
    // Outlives the AST/plan, including autocommit lock waiting and staging.
    pub(super) _planning_memory: Option<hawdb_qos::RuntimeRetainedMemory>,
}

impl<'a> PreparedGraphTransactionRequest<'a> {
    pub(super) fn prepare(
        runtime: &DatabaseTransactionRuntime,
        state: &mut DatabaseTransactionState,
        request: QueryRequest<'a>,
    ) -> Result<Self> {
        query_runtime::query_runtime_checkpoint(request.task_context)?;
        let empty = BTreeMap::new();
        let parameters = request.parameters.unwrap_or(&empty);
        if let Some(dependencies) = &mut state.read_dependencies {
            dependencies.admit_cypher(request.cypher, parameters)?;
        }
        let statement = cypher::parse(request.cypher)?;
        let body = statement_body(&statement);
        if matches!(body, cypher::Statement::SetSystemVariable(_)) {
            reject_system_variable_parameters(parameters)?;
            return Err(HawDBError::Execution(
                "SET system variable is not allowed inside a transaction".to_string(),
            ));
        }
        if matches!(body, cypher::Statement::Explain(_)) {
            return Err(HawDBError::Execution(
                "EXPLAIN is not allowed inside a transaction".to_string(),
            ));
        }
        let transaction = state
            .graph_transaction
            .as_ref()
            .expect("database transaction must own a graph workspace");
        let optimized = prepare_graph_transaction_plan(
            runtime,
            transaction,
            &runtime.system_variables,
            request.cypher,
            &statement,
            parameters,
            query_runtime::QueryExecutionOptions::for_request(request, None),
        )?;
        Ok(Self {
            request,
            statement,
            optimized,
            _planning_memory: None,
        })
    }

    pub(super) fn from_admission(
        runtime: &DatabaseTransactionRuntime,
        state: &DatabaseTransactionState,
        prepared: PreparedRuntimeQuery,
        request: QueryRequest<'a>,
    ) -> Result<Self> {
        let transaction = state.graph_transaction.as_ref().expect("graph workspace");
        if !prepared.matches_request(request)
            || !prepared.is_compatible(
                runtime.current_branch.as_ref().map(|branch| branch.info.id),
                &runtime.config,
                &runtime.system_variables,
                transaction.catalog(),
                transaction.store(),
            )
        {
            return Err(HawDBError::Execution(
                "prepared mutation changed before execution; prepare and admit again".into(),
            ));
        }
        let (_, mut prepared) = prepared.into_execution(transaction.catalog(), transaction.store());
        let optimized = prepared
            .take_optimized(
                request.access_control,
                request.scoring.map(query_request::ScoringAttachment::bind),
            )?
            .ok_or_else(|| {
                HawDBError::Execution("admitted mutation has no compatible plan".into())
            })?;
        Ok(Self {
            request,
            statement: prepared.statement,
            optimized,
            _planning_memory: prepared.planning_memory,
        })
    }

    pub(super) fn returns_statement_rows(&self) -> bool {
        matches!(
            self.optimized.physical_plan,
            PhysicalPlan::SetNodePropertiesReturn { .. }
        )
    }

    pub(super) fn into_trace(self, capture_trace: bool) -> QueryExecutionTrace {
        QueryExecutionTrace {
            statement: self.statement,
            optimizer_trace: capture_trace.then_some(self.optimized.trace),
            plan_cache_lookup: Some(self.optimized.plan_cache_lookup),
            execution_profile: None,
        }
    }

    pub(super) fn execute(
        &self,
        runtime: &DatabaseTransactionRuntime,
        state: &mut DatabaseTransactionState,
    ) -> Result<GraphTransactionStatementOutcome> {
        let transaction = state
            .graph_transaction
            .as_mut()
            .expect("database transaction must own a graph workspace");
        execute_graph_transaction_plan(
            runtime,
            transaction,
            &self.statement,
            &self.optimized,
            self.request.parameters.unwrap_or(&BTreeMap::new()),
            GraphTransactionStatementOptions {
                execution: query_runtime::QueryExecutionOptions::for_request(self.request, None),
                read_dependencies: state.read_dependencies.as_mut(),
            },
        )
    }
}

pub(crate) fn execute_concurrent_graph_transaction_request(
    runtime: &DatabaseTransactionRuntime,
    state: &mut DatabaseTransactionState,
    request: QueryRequest<'_>,
    parent_context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<GraphTransactionStatementOutcome> {
    let task = query_request::bind_request_context(parent_context, request.task_context)?;
    let request = QueryRequest {
        task_context: task.as_ref(),
        ..request
    };
    PreparedGraphTransactionRequest::prepare(runtime, state, request)?.execute(runtime, state)
}

pub(crate) fn execute_database_transaction_sql_request(
    runtime: &DatabaseTransactionRuntime,
    state: &mut DatabaseTransactionState,
    request: SqlQueryRequest<'_>,
    parent_context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<SqlStatementResult> {
    let result = (|| {
        let task = query_request::bind_request_context(parent_context, request.task_context)?;
        query_runtime::query_runtime_checkpoint(task.as_ref())?;
        if let Some(dependencies) = &mut state.read_dependencies {
            dependencies.admit_sql(request.sql, request.parameters)?;
        }
        let prepared = runtime
            .relational_plan_template_cache
            .prepare(request.sql)?;
        execute_database_transaction_prepared_sql(
            runtime,
            state,
            request.sql,
            prepared,
            request.parameters,
            DatabaseTransactionSqlOptions {
                allow_system_schema_registry_write: false,
                allow_locking_select: false,
                task_context: task.as_ref(),
                output_limits: request.output_limits,
                join_planning: request.join_planning,
            },
        )
    })();
    if result.is_err() {
        state.fail_read_dependencies();
    }
    result
}

pub(super) fn execute_database_transaction_sql(
    runtime: &DatabaseTransactionRuntime,
    state: &mut DatabaseTransactionState,
    sql_text: &str,
    parameters: &[Value],
    allow_system_schema_registry_write: bool,
    allow_locking_select: bool,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<SqlStatementResult> {
    if let Some(dependencies) = &mut state.read_dependencies {
        query_runtime::query_runtime_checkpoint(task_context)?;
        dependencies.admit_sql(sql_text, parameters)?;
    }
    let prepared = runtime.relational_plan_template_cache.prepare(sql_text)?;
    execute_database_transaction_prepared_sql(
        runtime,
        state,
        sql_text,
        prepared,
        parameters,
        DatabaseTransactionSqlOptions {
            allow_system_schema_registry_write,
            allow_locking_select,
            task_context,
            output_limits: QueryStreamOptions::default(),
            join_planning: RelationalJoinPlanningDirective::Auto,
        },
    )
}

pub(super) struct DatabaseTransactionSqlOptions<'a> {
    pub(super) allow_system_schema_registry_write: bool,
    pub(super) allow_locking_select: bool,
    pub(super) task_context: Option<&'a hawdb_core::RuntimeTaskContext>,
    pub(super) output_limits: QueryStreamOptions,
    pub(super) join_planning: RelationalJoinPlanningDirective,
}

pub(super) fn execute_database_transaction_prepared_sql(
    runtime: &DatabaseTransactionRuntime,
    state: &mut DatabaseTransactionState,
    sql_text: &str,
    prepared: crate::relational_sql::PreparedRelationalSql,
    parameters: &[Value],
    options: DatabaseTransactionSqlOptions<'_>,
) -> Result<SqlStatementResult> {
    query_runtime::query_runtime_checkpoint(options.task_context)?;
    let max_rows = restrictive_query_limit(
        runtime.read_result_row_limit(),
        options.output_limits.max_rows,
    );
    let admitted_result_bytes = options
        .task_context
        .and_then(hawdb_core::RuntimeTaskContext::memory_reservation)
        .map(|reservation| usize::try_from(reservation.result_bytes()).unwrap_or(usize::MAX));
    let max_read_result_payload_bytes = match (
        restrictive_query_limit(
            runtime.config.max_read_result_payload_bytes,
            options.output_limits.max_payload_bytes,
        ),
        admitted_result_bytes,
    ) {
        (Some(configured), Some(admitted)) => Some(configured.min(admitted)),
        (configured, admitted) => configured.or(admitted),
    };
    if let Some(dependencies) = &mut state.read_dependencies {
        query_runtime::query_runtime_checkpoint(options.task_context)?;
        dependencies.collect_sql(
            sql_text,
            prepared.statement(),
            parameters,
            &state.relational_state,
        )?;
    }
    reject_locking_select_without_manager(prepared.statement(), options.allow_locking_select)?;
    if let crate::sql::SqlStatement::Branch(statement) = prepared.statement() {
        return observability::execute_branch_sql_at_path(
            runtime.branch_catalog_path.as_deref(),
            runtime.current_branch.as_ref(),
            "explicit transaction",
            statement,
            parameters,
            max_rows,
            max_read_result_payload_bytes,
        )
        .map(sql_query_result);
    }
    if !options.allow_system_schema_registry_write
        && hawdb_relational::system_schema::statement_writes_system_schema_registry(
            prepared.statement(),
        )
    {
        return Err(HawDBError::Semantic(
            "hawdb_schema_migrations is read-only outside system schema upgrade".to_string(),
        ));
    }
    if matches!(
        prepared.statement(),
        crate::sql::SqlStatement::Select(select)
            if system_sql::is_virtual_catalog_select(select)
    ) {
        let graph_transaction = state
            .graph_transaction
            .as_ref()
            .expect("database transaction must own a graph workspace");
        let plan_cache_stats = runtime.plan_cache.borrow().stats();
        return system_sql::query_sql_with_params(
            sql_text,
            parameters,
            max_rows,
            max_read_result_payload_bytes,
            &system_sql::SystemSqlContext {
                catalog: graph_transaction.catalog(),
                store: graph_transaction.store(),
                relational_state: &state.relational_state,
                append_state: &state.append_state,
                runtime: system_runtime_snapshot(&runtime.config),
                plan_cache_stats: &plan_cache_stats,
                slow_queries: &[],
                statement_summaries: &[],
            },
        )
        .map(sql_query_result);
    }
    if let crate::sql::SqlStatement::CreateMaterializedView(create) = prepared.statement()
        && state.append_state.schema(&create.name.name).is_some()
    {
        return Err(HawDBError::Semantic(format!(
            "relation {} already exists as a strict append table",
            create.name.name
        )));
    }
    let pending_generated_read_table = match prepared.statement() {
        crate::sql::SqlStatement::Select(select) => {
            select.from.as_ref().map(|from| from.name.as_str())
        }
        crate::sql::SqlStatement::Explain(explain) => match explain.statement.as_ref() {
            crate::sql::SqlStatement::Select(select) => {
                select.from.as_ref().map(|from| from.name.as_str())
            }
            _ => None,
        },
        _ => None,
    };
    if pending_generated_read_table
        .is_some_and(|table| state.pending_generated_append_tables.contains(table))
    {
        return Err(HawDBError::Execution(format!(
            "strict append table {} has uncommitted generated-order rows; reads are unavailable until commit",
            pending_generated_read_table.expect("pending generated table was checked")
        )));
    }
    if let Some(plan) = crate::relational_sql::compile_prepared_append_select_sql(
        &prepared.template,
        parameters,
        &state.append_state,
        max_rows.unwrap_or(usize::MAX),
    )? {
        let store = state
            .graph_transaction
            .as_ref()
            .expect("database transaction must own a graph workspace")
            .store();
        let output = store.read_append_partition_from_state_bounded(
            &state.append_state,
            &plan.table,
            &plan.partition,
            plan.after.as_ref(),
            plan.max_rows,
            max_read_result_payload_bytes.unwrap_or(usize::MAX),
        )?;
        return Ok(sql_query_result(QueryOutput {
            rows: crate::relational_sql::project_append_rows(&plan, &output.rows)?,
        }));
    }
    if let Some(plan) = crate::relational_sql::compile_prepared_append_explain_sql(
        &prepared.template,
        parameters,
        &state.append_state,
        max_rows.unwrap_or(usize::MAX),
    )? {
        let report = if plan.analyze {
            let store = state
                .graph_transaction
                .as_ref()
                .expect("database transaction must own a graph workspace")
                .store();
            Some(
                store
                    .read_append_partition_from_state_bounded(
                        &state.append_state,
                        &plan.select.table,
                        &plan.select.partition,
                        plan.select.after.as_ref(),
                        plan.select.max_rows,
                        max_read_result_payload_bytes.unwrap_or(usize::MAX),
                    )?
                    .report,
            )
        } else {
            None
        };
        return observability::admit_append_explain(
            crate::relational_sql::format_append_explain(&plan, report.as_ref()),
            plan.analyze,
            &runtime.config,
            QueryStreamOptions {
                max_rows,
                max_payload_bytes: max_read_result_payload_bytes,
            },
            options.task_context,
        )
        .map(sql_query_result);
    }
    if matches!(
        prepared.statement(),
        crate::sql::SqlStatement::Select(_) | crate::sql::SqlStatement::Explain(_)
    ) {
        let index_read_mode = if runtime
            .config
            .relational_index_mode
            .requires_authoritative_indexes()
        {
            crate::relational_sql::RelationalIndexReadMode::AuthoritativeTransaction(
                state.authoritative_relational_index()?,
            )
        } else if runtime
            .config
            .relational_index_mode
            .serves_demand_paged_reads()
        {
            crate::relational_sql::RelationalIndexReadMode::TransactionWorkspace
        } else {
            crate::relational_sql::RelationalIndexReadMode::Materialized
        };
        let row_read_mode = match &state.relational_rows {
            Ok(Some(rows)) => crate::relational_sql::RelationalRowReadMode::Transaction {
                store: state
                    .graph_transaction
                    .as_ref()
                    .expect("database transaction must own a graph workspace")
                    .store(),
                rows,
            },
            Ok(None) => crate::relational_sql::RelationalRowReadMode::CanonicalMemory,
            Err(error) => {
                return Err(HawDBError::StorageIntegrity(format!(
                    "authoritative transaction row view could not be pinned: {error}"
                )));
            }
        };
        let output = crate::relational_sql::execute_prepared_relational_query_with_resources(
            prepared,
            parameters,
            &state.relational_state,
            crate::relational_sql::RelationalQueryReadModes::new(index_read_mode, row_read_mode),
            relational_query_resource_context(
                &runtime.config,
                max_rows,
                max_read_result_payload_bytes,
                options.task_context,
            )
            .with_join_planning(options.join_planning),
        )?;
        return Ok(sql_query_result(QueryOutput { rows: output.rows }));
    }

    runtime.ensure_writable(
        state
            .graph_transaction
            .as_ref()
            .expect("database transaction must own a graph workspace")
            .store(),
    )?;
    if let Some(transaction) = crate::relational_sql::compile_prepared_append_statement_sql(
        &prepared.template,
        parameters,
        &state.append_state,
    )? {
        if let crate::sql::SqlStatement::CreateTable(create) = prepared.statement()
            && state
                .relational_state
                .table_schema(&create.table.name)
                .is_some()
        {
            return Err(HawDBError::Semantic(format!(
                "table {} already exists as a RowPage table",
                create.table.name
            )));
        }
        state.append_state = state
            .append_state
            .stage_provisional_transaction(
                &transaction,
                hawdb_storage::append_table::AppendMutationLimits::default(),
            )
            .map_err(map_transaction_append_error)?;
        state
            .pending_generated_append_tables
            .extend(transaction.writes.iter().filter_map(|write| match write {
                hawdb_storage::append_table::AppendWrite::AppendGenerated { table, rows }
                    if !rows.is_empty() =>
                {
                    Some(table.clone())
                }
                _ => None,
            }));
        state.append_transaction.writes.extend(transaction.writes);
        return Ok(sql_query_result(QueryOutput {
            rows: Vec::new().into(),
        }));
    }
    if let crate::sql::SqlStatement::CreateTable(create) = prepared.statement()
        && state.append_state.schema(&create.table.name).is_some()
    {
        return Err(HawDBError::Semantic(format!(
            "table {} already exists as a strict append table",
            create.table.name
        )));
    }
    let compiled = crate::relational_sql::compile_prepared_relational_statement_with_result(
        &prepared.template,
        parameters,
        &state.relational_state,
    )?;
    let transaction = compiled.transaction;
    let mut pending_index_capture = None;
    let mut pending_row_capture = None;
    let (next_relational_state, mutation_outcomes) = if runtime
        .config
        .relational_index_mode
        .requires_authoritative_indexes()
    {
        if let Some(staged) =
            state.stage_sparse_authoritative_relational_statement(transaction.clone())?
        {
            pending_index_capture = Some(staged.index_capture);
            pending_row_capture = Some(staged.row_capture);
            (staged.state, staged.mutation_outcomes)
        } else {
            let index = match &mut state.relational_index {
                Ok(Some(index)) => index,
                Ok(None) => {
                    return Err(HawDBError::StorageIntegrity(
                        "authoritative transaction index view is unavailable".to_string(),
                    ));
                }
                Err(error) => {
                    return Err(HawDBError::StorageIntegrity(format!(
                        "authoritative transaction index view could not be pinned: {error}"
                    )));
                }
            };
            let (next, capture, outcomes) = state
                .relational_state
                .stage_transaction_with_authoritative_index_and_outcomes(
                    transaction.clone(),
                    hawdb_storage::relational::RelationalMutationLimits::default(),
                    hawdb_storage::relational::RelationalOverflowConfig::default(),
                    index.capture_limits(),
                    index,
                )
                .map_err(map_transaction_relational_error)?;
            pending_index_capture = Some(capture);
            (next, outcomes)
        }
    } else {
        state
            .relational_state
            .stage_transaction_with_outcomes(
                transaction.clone(),
                hawdb_storage::relational::RelationalMutationLimits::default(),
                hawdb_storage::relational::RelationalOverflowConfig::default(),
            )
            .map_err(map_transaction_relational_error)?
    };
    if mutation_outcomes.len() > 1 {
        return Err(HawDBError::StorageIntegrity(
            "one SQL statement produced multiple relational mutation outcomes".to_string(),
        ));
    }
    let mutation = mutation_outcomes
        .first()
        .map(|outcome| {
            project_relational_mutation_outcome(
                outcome,
                compiled.returning.as_ref(),
                &next_relational_state,
                runtime.config.mutation_limits,
                true,
            )
        })
        .transpose()?;
    if let Some(mutation) = &mutation {
        runtime.validate_result_rows(mutation.rows.len())?;
        if let Some(limit) = max_rows
            && mutation.rows.len() > limit
        {
            return Err(HawDBError::read_budget_exceeded(
                crate::ReadBudgetResource::Rows,
                limit,
                "SQL mutation result exceeds the request row ceiling",
            ));
        }
        runtime.validate_result_payload(
            mutation.rows.payload_bytes(),
            max_read_result_payload_bytes,
        )?;
    }
    let next_relational_rows = if let Some(row_capture) = pending_row_capture {
        let rows = match &state.relational_rows {
            Ok(Some(rows)) => rows,
            Ok(None) => {
                return Err(HawDBError::StorageIntegrity(
                    "relational row transaction view is unavailable".to_string(),
                ));
            }
            Err(error) => {
                return Err(HawDBError::StorageIntegrity(format!(
                    "relational transaction row view could not be pinned: {error}"
                )));
            }
        };
        Some(
            rows.stage_advance(row_capture)
                .map_err(map_transaction_relational_error)?,
        )
    } else {
        None
    };
    if let Some(index_capture) = pending_index_capture {
        state
            .relational_index
            .as_mut()
            .map_err(|error| {
                HawDBError::StorageIntegrity(format!(
                    "authoritative transaction index view could not be pinned: {error}"
                ))
            })?
            .as_mut()
            .ok_or_else(|| {
                HawDBError::StorageIntegrity(
                    "authoritative transaction index view is unavailable".to_string(),
                )
            })?
            .append(index_capture)
            .map_err(map_transaction_relational_error)?;
    }
    if let Some(next_relational_rows) = next_relational_rows {
        state.relational_rows = Ok(Some(next_relational_rows));
    }
    state.relational_state = next_relational_state;
    state
        .relational_transaction
        .writes
        .extend(transaction.writes);
    state
        .relational_returning
        .extend(mutation_outcomes.iter().map(|_| compiled.returning.clone()));
    let rows = mutation
        .as_ref()
        .map(|mutation| mutation.rows.clone())
        .unwrap_or_default();
    Ok(SqlStatementResult {
        output: QueryOutput { rows },
        mutation,
    })
}

pub(super) fn sql_query_result(output: QueryOutput) -> SqlStatementResult {
    SqlStatementResult {
        output,
        mutation: None,
    }
}

pub(super) fn project_relational_mutation_outcome(
    outcome: &hawdb_storage::relational::RelationalMutationOutcome,
    returning: Option<&crate::relational_sql::RelationalReturningProjection>,
    state: &hawdb_storage::relational::RelationalState,
    limits: hawdb_storage::mutation::MutationLimits,
    provisional: bool,
) -> Result<RelationalMutationResult> {
    if outcome.affected_rows > limits.max_affected_rows.get() {
        return Err(HawDBError::Execution(format!(
            "relational mutation affects {} rows, exceeding max_affected_rows {}",
            outcome.affected_rows, limits.max_affected_rows
        )));
    }
    let rows = if let Some(returning) = returning {
        if returning.table != outcome.table {
            return Err(HawDBError::StorageIntegrity(format!(
                "relational mutation outcome for table {} was paired with RETURNING for table {}",
                outcome.table, returning.table
            )));
        }
        if outcome.rows.len() > limits.max_result_rows.get() {
            return Err(HawDBError::Execution(format!(
                "relational mutation returns {} rows, exceeding max_result_rows {}",
                outcome.rows.len(),
                limits.max_result_rows
            )));
        }
        let schema = state.table_schema(&outcome.table).ok_or_else(|| {
            HawDBError::StorageIntegrity(format!(
                "relational mutation outcome references unknown table {}",
                outcome.table
            ))
        })?;
        let positions = returning
            .columns
            .iter()
            .map(|column| {
                schema.column_position(column).ok_or_else(|| {
                    HawDBError::StorageIntegrity(format!(
                        "relational mutation outcome references unknown column {column}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let query_schema = executor::QuerySchema::try_new(returning.columns.clone())?;
        let mut builder = executor::QueryRowsBuilder::with_schema(query_schema, outcome.rows.len());
        for row in &outcome.rows {
            builder.push_values(
                positions
                    .iter()
                    .map(|position| relational_value_to_query_value(&row.values()[*position]))
                    .collect::<Result<Vec<_>>>()?,
            )?;
        }
        let rows = builder.finish();
        if rows.payload_bytes() > limits.max_result_payload_bytes.get() {
            return Err(HawDBError::Execution(format!(
                "relational mutation result contains {} payload bytes, exceeding max_result_payload_bytes {}",
                rows.payload_bytes(), limits.max_result_payload_bytes
            )));
        }
        rows
    } else {
        executor::QueryRows::empty()
    };
    Ok(RelationalMutationResult {
        affected_rows: outcome.affected_rows,
        conflict_rows: outcome.conflict_rows,
        rows,
        provisional,
    })
}

pub(super) fn relational_value_to_query_value(
    value: &hawdb_storage::relational::RelationalValue,
) -> Result<Value> {
    match value {
        hawdb_storage::relational::RelationalValue::Null => Ok(Value::Null),
        hawdb_storage::relational::RelationalValue::Boolean(value) => Ok(Value::Bool(*value)),
        hawdb_storage::relational::RelationalValue::BigInt(value) => Ok(Value::Int(*value)),
        hawdb_storage::relational::RelationalValue::DoublePrecision(value) => {
            Ok(Value::Float(*value))
        }
        hawdb_storage::relational::RelationalValue::Text(value) => Ok(Value::String(value.clone())),
        hawdb_storage::relational::RelationalValue::Bytea(value) => {
            Ok(Value::Binary(value.clone()))
        }
        hawdb_storage::relational::RelationalValue::Uuid(value) => Ok(Value::Uuid(*value)),
        hawdb_storage::relational::RelationalValue::Vector(value) => {
            Ok(Value::Vector(value.clone()))
        }
        hawdb_storage::relational::RelationalValue::Overflow(_) => {
            Err(HawDBError::StorageIntegrity(
                "logical relational mutation outcome contains an overflow reference".to_string(),
            ))
        }
    }
}

pub(super) fn map_transaction_append_error(
    error: hawdb_storage::append_table::AppendTableError,
) -> HawDBError {
    match error {
        hawdb_storage::append_table::AppendTableError::UnsupportedFormat(error) => {
            HawDBError::UnsupportedStorageFormat(error)
        }
        hawdb_storage::append_table::AppendTableError::FileDescriptors(error) => {
            HawDBError::FileDescriptors(error)
        }
        hawdb_storage::append_table::AppendTableError::SequenceExhausted {
            table,
            watermark,
            requested,
        } => HawDBError::AppendSequenceExhausted {
            table,
            watermark,
            requested,
        },
        hawdb_storage::append_table::AppendTableError::Corruption(message)
        | hawdb_storage::append_table::AppendTableError::Durability(message) => {
            HawDBError::StorageIntegrity(message)
        }
        error @ (hawdb_storage::append_table::AppendTableError::Admission(_)
        | hawdb_storage::append_table::AppendTableError::Schema(_)
        | hawdb_storage::append_table::AppendTableError::Constraint(_)) => {
            HawDBError::Execution(error.to_string())
        }
    }
}

pub(super) fn map_transaction_relational_error(
    error: hawdb_storage::relational::RelationalError,
) -> HawDBError {
    match error {
        hawdb_storage::relational::RelationalError::UnsupportedFormat(error) => {
            HawDBError::UnsupportedStorageFormat(error)
        }
        hawdb_storage::relational::RelationalError::FileDescriptors(error) => {
            HawDBError::FileDescriptors(error)
        }
        hawdb_storage::relational::RelationalError::Corruption(message)
        | hawdb_storage::relational::RelationalError::Durability(message) => {
            HawDBError::StorageIntegrity(message)
        }
        error @ (hawdb_storage::relational::RelationalError::Admission(_)
        | hawdb_storage::relational::RelationalError::Schema(_)
        | hawdb_storage::relational::RelationalError::Constraint(_)) => {
            HawDBError::Execution(error.to_string())
        }
    }
}

pub(super) fn reject_locking_select_without_manager(
    statement: &crate::sql::SqlStatement,
    allow_locking_select: bool,
) -> Result<()> {
    let locking_select = match statement {
        crate::sql::SqlStatement::Select(select) => select.lock_strength.is_some(),
        crate::sql::SqlStatement::Explain(explain) => {
            matches!(explain.statement.as_ref(), crate::sql::SqlStatement::Select(select) if select.lock_strength.is_some())
        }
        _ => false,
    };
    if locking_select && !allow_locking_select {
        return Err(HawDBError::Semantic(
            "FOR UPDATE/SHARE requires a pessimistic concurrent transaction".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn commit_database_transaction_state(
    db: &mut Database,
    state: &mut DatabaseTransactionState,
    allow_stale_rebase: bool,
) -> Result<TransactionCommitResult> {
    let result = (|| {
        let prepared = state
            .graph_transaction
            .take()
            .expect("database transaction must own a graph workspace")
            .prepare_commit(
                db.config.mutation_limits,
                state.relational_transaction.writes.is_empty()
                    && state.append_transaction.writes.is_empty(),
            )?;
        commit_database_transaction_state_inner(db, state, prepared, allow_stale_rebase)
    })();
    db.poison_on_storage_error(&result);
    result
}

pub(super) fn commit_prepared_database_transaction_state(
    db: &mut Database,
    state: &mut DatabaseTransactionState,
    prepared: crate::store::PreparedGraphMutationTransaction,
    allow_stale_rebase: bool,
) -> Result<TransactionCommitResult> {
    let result = commit_database_transaction_state_inner(db, state, prepared, allow_stale_rebase);
    db.poison_on_storage_error(&result);
    result
}

pub(super) fn commit_database_transaction_state_inner(
    db: &mut Database,
    state: &mut DatabaseTransactionState,
    graph_transaction: crate::store::PreparedGraphMutationTransaction,
    allow_stale_rebase: bool,
) -> Result<TransactionCommitResult> {
    db.ensure_writable()?;
    if let Some(dependencies) = &state.read_dependencies {
        dependencies.validate(db)?;
    }
    let returning = std::mem::take(&mut state.relational_returning);
    let relational_transaction = std::mem::take(&mut state.relational_transaction);
    let append_transaction = std::mem::take(&mut state.append_transaction);
    let mut branch_runtime_access = db.runtime.get_mut()?;
    let branch_runtime = &mut *branch_runtime_access;
    let summary = branch_runtime.store.commit_prepared_graph_transaction(
        &mut branch_runtime.catalog,
        graph_transaction,
        relational_transaction,
        append_transaction,
        db.config.mutation_limits,
        allow_stale_rebase,
    )?;
    drop(branch_runtime_access);
    db.complete_required_relational_row_checkpoint("transaction commit")?;
    if returning.len() != summary.relational_mutation_outcomes.len() {
        return Err(HawDBError::StorageIntegrity(format!(
            "transaction recorded {} relational result projections but committed {} outcomes",
            returning.len(),
            summary.relational_mutation_outcomes.len()
        )));
    }
    let branch_runtime = db.runtime.get()?;
    let mutations = summary
        .relational_mutation_outcomes
        .iter()
        .zip(&returning)
        .map(|(outcome, returning)| {
            project_relational_mutation_outcome(
                outcome,
                returning.as_ref(),
                branch_runtime.store.relational_state(),
                db.config.mutation_limits,
                false,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let mut rows: executor::QueryRows = summary.rows.into();
    if rows.is_empty() && mutations.len() == 1 {
        rows = mutations[0].rows.clone();
    }
    Ok(TransactionCommitResult {
        output: QueryOutput { rows },
        mutations,
        append_mutations: summary.append_mutation_outcomes,
    })
}

impl DatabaseTransaction<'_> {
    pub fn read_dependency_usage(&self) -> Option<TransactionReadDependencyUsage> {
        self.state.read_dependency_usage()
    }

    pub(crate) fn apply_session_config(&mut self, config: SessionConfig) {
        self.runtime.session_config = config;
    }

    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.query_request(QueryRequest::new(cypher_text).with_params(parameters))
    }

    pub fn query_request(&mut self, request: QueryRequest<'_>) -> Result<QueryOutput> {
        let result = execute_concurrent_graph_transaction_request(
            &self.runtime,
            &mut self.state,
            request,
            self.task_context.as_ref(),
        )
        .map(|outcome| outcome.output);
        if result.is_err() {
            self.state.fail_read_dependencies();
        }
        result
    }

    pub fn query_sql(&mut self, sql_text: &str) -> Result<QueryOutput> {
        self.query_sql_with_params(sql_text, &[])
    }

    pub fn query_sql_with_params(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        self.query_sql_with_result_and_params(sql_text, parameters)
            .map(|result| result.output)
    }

    /// Executes SQL and returns both PostgreSQL rows and a provisional mutation outcome.
    pub fn query_sql_with_result(&mut self, sql_text: &str) -> Result<SqlStatementResult> {
        self.query_sql_with_result_and_params(sql_text, &[])
    }

    /// Parameterized form of [`Self::query_sql_with_result`].
    pub fn query_sql_with_result_and_params(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<SqlStatementResult> {
        self.query_sql_request_with_result(SqlQueryRequest::new(sql_text).with_params(parameters))
    }

    pub fn query_sql_request(&mut self, request: SqlQueryRequest<'_>) -> Result<QueryOutput> {
        self.query_sql_request_with_result(request)
            .map(|result| result.output)
    }

    pub fn query_sql_request_with_result(
        &mut self,
        request: SqlQueryRequest<'_>,
    ) -> Result<SqlStatementResult> {
        execute_database_transaction_sql_request(
            &self.runtime,
            &mut self.state,
            request,
            self.task_context.as_ref(),
        )
    }

    pub(super) fn query_system_schema_sql(&mut self, sql_text: &str) -> Result<QueryOutput> {
        self.query_system_schema_sql_with_params(sql_text, &[])
    }

    pub(super) fn query_system_schema_sql_with_params(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        execute_database_transaction_sql(
            &self.runtime,
            &mut self.state,
            sql_text,
            parameters,
            true,
            false,
            self.task_context.as_ref(),
        )
        .map(|result| result.output)
    }

    pub fn commit(self) -> Result<QueryOutput> {
        self.commit_with_result().map(|result| result.output)
    }

    /// Commits and returns outcomes recomputed against the durable commit state.
    pub fn commit_with_result(mut self) -> Result<TransactionCommitResult> {
        if let Some(dependencies) = &self.state.read_dependencies {
            dependencies.ensure_complete()?;
            query_runtime::query_runtime_checkpoint(self.task_context.as_ref())?;
        }
        commit_database_transaction_state(self.db, &mut self.state, false)
    }

    pub fn rollback(mut self) {
        self.state.rollback();
    }
}

impl DatabaseSession<'_> {
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Changes this session only. Active transactions retain the settings
    /// captured at BEGIN; the next transaction inherits the new settings.
    pub fn set_max_result_rows(&mut self, max_result_rows: u64) {
        self.config.max_result_rows = max_result_rows;
    }

    pub(super) fn row_limit(&self) -> Option<usize> {
        restrictive_query_limit(
            self.db.config.read_result_row_limit(),
            self.config.row_limit(),
        )
    }

    pub fn query_sql(&mut self, sql_text: &str) -> Result<QueryOutput> {
        self.query_sql_with_params(sql_text, &[])
    }

    pub fn query_sql_with_params(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        if self.graph_transaction.is_none() {
            let row_limit = self.row_limit();
            return self.db.query_sql_with_params_options_and_system_variables(
                sql_text,
                parameters,
                QueryStreamOptions {
                    max_rows: row_limit,
                    max_payload_bytes: self.db.config.max_read_result_payload_bytes,
                },
                self.system_variables.clone(),
            );
        }
        let prepared = self
            .db
            .runtime
            .get()?
            .relational_plan_template_cache
            .prepare(sql_text)?;
        if let crate::sql::SqlStatement::Branch(statement) = prepared.statement() {
            let runtime = self
                .transaction_runtime
                .as_ref()
                .expect("active session transaction runtime");
            return observability::execute_branch_sql_at_path(
                runtime.branch_catalog_path.as_deref(),
                runtime.current_branch.as_ref(),
                "explicit transaction",
                statement,
                parameters,
                runtime.read_result_row_limit(),
                runtime.config.max_read_result_payload_bytes,
            );
        }
        Err(HawDBError::Execution(
            "SQL data statements are unavailable in an explicit Cypher session transaction".into(),
        ))
    }

    /// Opens a retained numeric cursor using this session's row ceiling.
    /// Arrow streams derived from it inherit the same terminal-error contract.
    pub fn query_with_params_retained(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        mut options: RetainedQueryOptions,
    ) -> std::result::Result<RetainedQueryCursor, RetainedQueryError> {
        if self.graph_transaction.is_some() {
            return Err(RetainedQueryError::UnsupportedPlan);
        }
        options.max_result_rows =
            restrictive_query_limit(self.row_limit(), options.max_result_rows);
        self.db
            .query_with_params_retained(cypher, parameters, options)
    }

    pub fn system_variables(&self) -> &QuerySystemVariables {
        &self.system_variables
    }

    pub fn query_work_request(&self) -> WorkRequest {
        self.system_variables.query_work_request()
    }

    pub fn query_work_request_for(&self, cypher_text: &str) -> Result<WorkRequest> {
        let statement = cypher::parse(cypher_text)?;
        query_work_request_for_statement(&self.system_variables, &statement)
    }

    pub fn explain_query(&self, cypher_text: &str) -> Result<ExplainOutput> {
        self.explain_query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn explain_query_with_params(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<ExplainOutput> {
        if self.graph_transaction.is_some() {
            return Err(HawDBError::Execution(
                "EXPLAIN is not allowed inside an active transaction".to_string(),
            ));
        }
        let statement = cypher::parse(cypher_text)?;
        let work_request = query_work_request_for_statement(&self.system_variables, &statement)?;
        let optimized =
            self.db
                .optimized_explain_query_plan(cypher_text, &statement, parameters)?;
        Ok(ExplainOutput {
            physical_plan: optimized.physical_plan,
            trace: optimized.trace,
            work_request,
            plan_cache_lookup: optimized.plan_cache_lookup,
            statement_kind: statement_kind(statement_body(&statement)),
        })
    }

    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        let statement = cypher::parse(cypher_text)?;
        let body = statement_body(&statement);
        match body {
            cypher::Statement::BeginTransaction => {
                reject_transaction_control_parameters("BEGIN TRANSACTION", parameters)?;
                if self.graph_transaction.is_some() {
                    return Err(HawDBError::Execution(
                        "transaction is already active".to_string(),
                    ));
                }
                self.db.ensure_writable()?;
                self.transaction_runtime = Some(
                    DatabaseTransactionRuntime::from_database_with_system_variables(
                        self.db,
                        self.system_variables.clone(),
                        self.config,
                    )?,
                );
                let branch_runtime = self.db.runtime.get()?;
                self.graph_transaction = Some(
                    branch_runtime
                        .store
                        .begin_mutation_transaction(&branch_runtime.catalog),
                );
                Ok(QueryOutput {
                    rows: Vec::new().into(),
                })
            }
            cypher::Statement::Commit => {
                reject_transaction_control_parameters("COMMIT", parameters)?;
                let Some(transaction) = self.graph_transaction.take() else {
                    return Err(HawDBError::Execution(
                        "COMMIT requires an active transaction".to_string(),
                    ));
                };
                let runtime = self.transaction_runtime.take().ok_or_else(|| {
                    HawDBError::StorageIntegrity("active session transaction has no runtime".into())
                })?;
                let mut mutation_limits = self.db.config.mutation_limits;
                // Graph summaries keep the MutationLimits payload contract;
                // storage's MutationCommitLimitTracker checks it before WAL publication.
                if let Some(limit) = runtime.read_result_row_limit().and_then(NonZeroUsize::new) {
                    mutation_limits.max_result_rows = mutation_limits.max_result_rows.min(limit);
                }
                self.db.ensure_writable()?;
                let mut branch_runtime_access = self.db.runtime.get_mut()?;
                let branch_runtime = &mut *branch_runtime_access;
                let summary = branch_runtime
                    .store
                    .commit_mutation_transaction_and_relational(
                        &mut branch_runtime.catalog,
                        transaction,
                        hawdb_storage::relational::RelationalTransaction::default(),
                        mutation_limits,
                    )?;
                Ok(QueryOutput {
                    rows: summary.rows.into(),
                })
            }
            cypher::Statement::Rollback => {
                reject_transaction_control_parameters("ROLLBACK", parameters)?;
                if self.graph_transaction.take().is_none() {
                    return Err(HawDBError::Execution(
                        "ROLLBACK requires an active transaction".to_string(),
                    ));
                }
                self.transaction_runtime.take();
                Ok(QueryOutput {
                    rows: Vec::new().into(),
                })
            }
            cypher::Statement::Checkpoint if self.graph_transaction.is_some() => {
                Err(HawDBError::Execution(
                    "CHECKPOINT is not allowed inside an active transaction".to_string(),
                ))
            }
            cypher::Statement::SetSystemVariable(_) if self.graph_transaction.is_some() => {
                Err(HawDBError::Execution(
                    "SET system variable is not allowed inside an active transaction".to_string(),
                ))
            }
            cypher::Statement::SetSystemVariable(set) => {
                reject_system_variable_parameters(parameters)?;
                apply_set_system_variable(&mut self.system_variables, set)
            }
            cypher::Statement::Explain(_) if self.graph_transaction.is_some() => {
                Err(HawDBError::Execution(
                    "EXPLAIN is not allowed inside an active transaction".to_string(),
                ))
            }
            cypher::Statement::Explain(explain) => {
                self.execute_explain_statement(cypher_text, explain, parameters)
            }
            statement if self.graph_transaction.is_some() => {
                let transaction = self
                    .graph_transaction
                    .as_mut()
                    .expect("checked active transaction");
                let runtime = self
                    .transaction_runtime
                    .as_ref()
                    .expect("active session transaction must own a query runtime");
                execute_graph_transaction_statement(
                    runtime,
                    transaction,
                    &self.system_variables,
                    cypher_text,
                    statement,
                    parameters,
                    GraphTransactionStatementOptions {
                        execution: query_runtime::QueryExecutionOptions::for_request(
                            QueryRequest::new(cypher_text).with_params(parameters),
                            None,
                        ),
                        read_dependencies: None,
                    },
                )
                .map(|outcome| outcome.output)
            }
            _ => {
                query_work_request_for_statement(&self.system_variables, &statement)?;
                self.db.query_request(
                    QueryRequest::new(cypher_text)
                        .with_params(parameters)
                        .with_output_limits(QueryStreamOptions {
                            max_rows: self.row_limit(),
                            max_payload_bytes: None,
                        }),
                )
            }
        }
    }

    pub(super) fn execute_explain_statement(
        &mut self,
        cypher_text: &str,
        explain: &cypher::Explain,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        let work_request =
            query_work_request_for_statement(&self.system_variables, &explain.statement)?;
        let optimized =
            self.db
                .optimized_explain_query_plan(cypher_text, &explain.statement, parameters)?;
        let options =
            query_runtime::QueryExecutionOptions::for_bounded_read(self.row_limit(), None, None);
        let inner_statement_kind = statement_kind(statement_body(&explain.statement));
        if explain.analyze {
            if executor::is_mutation_plan(&optimized.physical_plan)? {
                return Err(HawDBError::Execution(
                    "EXPLAIN ANALYZE only supports read queries".to_string(),
                ));
            }
            let row_limit = self.row_limit();
            let mut external = executor::NoExternalReadOperator;
            let mut branch_runtime_access = self.db.runtime.get_mut()?;
            let branch_runtime = &mut *branch_runtime_access;
            let profiled = executor::execute_with_request(
                executor::ExecutionRequest::new(
                    &optimized.physical_plan,
                    parameters,
                    &self.db.config.execution_memory,
                )
                .with_output_columns(optimized.output_columns.as_deref())
                .with_output_limits(row_limit, self.db.config.max_read_result_payload_bytes),
                executor::ExecutionResources::new(
                    &mut branch_runtime.catalog,
                    &mut branch_runtime.store,
                    &mut external,
                ),
            );
            drop(branch_runtime_access);
            self.db.poison_on_storage_error(&profiled);
            let profiled = profiled?;
            let row = explain_analyze_output_row(
                &optimized,
                work_request,
                inner_statement_kind,
                profiled.rows.len(),
                &profiled.profile,
            );
            drop(profiled.rows);
            return explain::admit_explain_output(row, true, &self.db.config, options);
        }
        explain::admit_explain_output(
            explain_output_row(&optimized, work_request, inner_statement_kind),
            false,
            &self.db.config,
            options,
        )
    }
}

pub(super) fn reject_transaction_control_parameters(
    statement: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    if parameters.is_empty() {
        Ok(())
    } else {
        Err(HawDBError::Semantic(format!(
            "{statement} does not accept parameters"
        )))
    }
}
