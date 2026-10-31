// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Typed local qualification of the ordinary embedded read runtime. Planning
//! failures are outer errors; execution errors retain the actual chosen trace.

use super::*;
use crate::optimizer::{
    verify_optimizer_rule_assertions, OptimizerQualificationOptions, OptimizerRuleAssertion,
    OptimizerRuleAssertionError,
};

#[derive(Debug)]
pub struct OptimizerQualificationOutput {
    pub snapshot_epoch: u64,
    pub optimizer_trace: OptimizerTrace,
    pub result: Result<BoundedReadQueryOutput>,
}
impl OptimizerQualificationOutput {
    pub fn verify_rules(
        &self,
        assertions: &[OptimizerRuleAssertion],
    ) -> std::result::Result<(), OptimizerRuleAssertionError> {
        verify_optimizer_rule_assertions(&self.optimizer_trace.rule_events, assertions)
    }
}

impl<S: executor::ExecutionStore> DatabaseReadTransaction<S> {
    /// Prepares and executes one read with explicit rule/ranking controls on
    /// this pinned snapshot. The same parser, planner and admitted executor
    /// serve ordinary reads. Qualified plans bypass physical cache admission;
    /// no shared optimizer state, quota, capability or legality check changes.
    pub fn qualify_optimizer_query(
        &mut self,
        request: QueryRequest<'_>,
        qualification: OptimizerQualificationOptions,
        search: OptimizerSearchDirective,
    ) -> Result<OptimizerQualificationOutput> {
        self.store.ensure_usable()?;
        let task =
            query_request::bind_request_context(self.task_context.as_ref(), request.task_context)?;
        let request = QueryRequest {
            task_context: task.as_ref(),
            ..request
        };
        query_runtime::query_runtime_checkpoint(request.task_context)?;
        let prepared = query_runtime::parse_runtime_execution(request.cypher)?;
        let statement = prepared.statement;
        if matches!(statement, cypher::Statement::Explain(_))
            || matches!(
                statement_body(&statement),
                cypher::Statement::Checkpoint | cypher::Statement::SetSystemVariable(_)
            )
        {
            return Err(HawDBError::Execution(
                "optimizer qualification requires an ordinary read statement".into(),
            ));
        }
        query_work_request_for_statement(&self.system_variables, &statement)?;
        let hinted = query_statement_variables_for_statement(&self.system_variables, &statement)?
            .optimizer_search;
        let search = if search == OptimizerSearchDirective::Auto {
            hinted
        } else if hinted == OptimizerSearchDirective::Auto || search == hinted {
            search
        } else {
            return Err(HawDBError::Execution(
                "conflicting typed and Cypher optimizer search directives".into(),
            ));
        };
        let optimizer = CascadesOptimizer::with_context(
            self.optimizer
                .context()
                .clone()
                .with_qualification_options(qualification),
        );
        let empty = BTreeMap::new();
        let parameters = request.parameters.unwrap_or(&empty);
        let options = query_runtime::QueryExecutionOptions::for_request(request, None);
        let optimized = plan_cache::optimized_query_plan_for(
            request.cypher,
            &statement,
            parameters,
            PlanCacheMode::Bypass(PlanCacheBypassReason::OptimizerDirective),
            PlanTraceMode::Bound,
            plan_cache::PlanCacheContext {
                catalog: &self.catalog,
                store: &self.store,
                optimizer: &optimizer,
                config: &self.config,
                cache: &self.plan_cache,
                planning_cache: &self.optimizer_planning_cache,
                access_control: options.access_control,
                optimizer_search: search,
                scoring: options.scoring,
            },
        )?;
        if executor::is_mutation_plan(&optimized.physical_plan)?
            || executor::requires_write_access(&optimized.physical_plan)
        {
            return Err(HawDBError::Execution(
                "optimizer qualification cannot execute a mutation or write procedure".into(),
            ));
        }
        let optimizer_trace = optimized.trace.clone();
        let snapshot_epoch = self.commit_epoch();
        let row_limit = self.read_result_row_limit();
        let mut external = executor::NoExternalReadOperator;
        let result = read_execution::PreparedReadExecution {
            statement,
            optimized,
            _planning_memory: prepared.planning_memory,
        }
        .execute(
            parameters,
            options,
            &self.config,
            row_limit,
            executor::ExecutionResources::new(&mut self.catalog, &mut self.store, &mut external),
        )
        .and_then(|(output, trace)| {
            let execution_profile = trace.execution_profile.ok_or_else(|| {
                HawDBError::Execution(
                    "qualified read completed without an execution profile".into(),
                )
            })?;
            Ok(BoundedReadQueryOutput {
                output,
                execution_profile,
            })
        });
        self.store.poison_on_storage_error(&result);
        Ok(OptimizerQualificationOutput {
            snapshot_epoch,
            optimizer_trace,
            result,
        })
    }
}
