// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Typed local qualification of the ordinary embedded read runtime. Planning
//! failures are outer errors; execution errors retain the actual chosen trace.

use super::*;
use crate::optimizer::{
    verify_optimizer_rule_assertions, OptimizerBoundedOrderAlternative,
    OptimizerQualificationOptions, OptimizerRuleAssertion, OptimizerRuleAssertionError,
    RuleOutcome, BOUNDED_ORDER_QUALIFICATION_RULE, DEFAULT_PLAN_COST_POLICY_ID,
    DEFAULT_PLAN_COST_WEIGHTS,
};

/// Protocol-wide upper bound for one frozen query's candidate inventory.
pub const OPTIMIZER_QUALIFICATION_CANDIDATE_LIMIT: usize = 8;

/// Equal deadline and work admission applied independently to each candidate.
///
/// A [`QueryRequest`] task context remains the enclosing campaign admission:
/// its deadline and work budget are shared across all candidate attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptimizerQualificationAttemptLimits {
    timeout: std::time::Duration,
    work_units: usize,
}

impl OptimizerQualificationAttemptLimits {
    pub fn new(timeout: std::time::Duration, work_units: usize) -> Result<Self> {
        if timeout.is_zero() {
            return Err(HawDBError::Execution(
                "optimizer qualification attempt timeout must be nonzero".into(),
            ));
        }
        if std::time::Instant::now().checked_add(timeout).is_none() {
            return Err(HawDBError::Execution(
                "optimizer qualification attempt timeout exceeds the supported monotonic deadline range"
                    .into(),
            ));
        }
        if work_units == 0 {
            return Err(HawDBError::Execution(
                "optimizer qualification attempt work limit must be nonzero".into(),
            ));
        }
        Ok(Self {
            timeout,
            work_units,
        })
    }

    pub const fn timeout(self) -> std::time::Duration {
        self.timeout
    }

    pub const fn work_units(self) -> usize {
        self.work_units
    }

    fn task_context(self) -> Result<hawdb_core::RuntimeTaskContext> {
        let deadline = std::time::Instant::now()
            .checked_add(self.timeout)
            .ok_or_else(|| {
                HawDBError::Execution(
                    "optimizer qualification attempt timeout exceeds the supported monotonic deadline range"
                        .into(),
                )
            })?;
        Ok(hawdb_core::RuntimeTaskContext::new(
            hawdb_core::RuntimeCancellationToken::new(),
            Some(deadline),
        )
        .with_work_limit(self.work_units))
    }
}

/// How one candidate was selected for an offline qualification attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerQualificationCandidateKind {
    Ordinary,
    BoundedOrder(OptimizerBoundedOrderAlternative),
}

impl OptimizerQualificationCandidateKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::BoundedOrder(alternative) => alternative.as_str(),
        }
    }
}

/// One named candidate requested by an offline qualification campaign.
///
/// A replay can bind the request to the complete physical identity observed by
/// an earlier campaign. A changed identity is refused instead of silently
/// measuring a different plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptimizerQualificationCandidateRequest<'a> {
    pub kind: OptimizerQualificationCandidateKind,
    pub expected_plan_instance_fingerprint: Option<&'a str>,
}

impl<'a> OptimizerQualificationCandidateRequest<'a> {
    pub const fn new(kind: OptimizerQualificationCandidateKind) -> Self {
        Self {
            kind,
            expected_plan_instance_fingerprint: None,
        }
    }

    pub const fn with_expected_plan_instance_fingerprint(mut self, fingerprint: &'a str) -> Self {
        self.expected_plan_instance_fingerprint = Some(fingerprint);
        self
    }
}

/// Evidence from an offline optimizer qualification read.
#[derive(Debug)]
pub struct OptimizerQualificationOutput {
    pub snapshot_epoch: u64,
    pub optimizer_trace: OptimizerTrace,
    /// Complete physical identity, including bound access/index details omitted
    /// from the ordinary shape fingerprint.
    pub selected_plan_instance_fingerprint: String,
    pub cost_policy_id: &'static str,
    pub cost_weights: crate::optimizer::PlanCostWeights,
    /// Work charged while preparing and executing this qualified read.
    pub work_units: usize,
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

/// One executed member of a bounded candidate inventory.
#[derive(Debug)]
pub struct OptimizerQualificationCandidateOutput {
    pub kind: OptimizerQualificationCandidateKind,
    /// Earlier candidate with the same complete physical identity, when any.
    pub same_plan_as: Option<usize>,
    pub output: OptimizerQualificationOutput,
}

/// Phase-one candidate evidence for one bounded-order query on one snapshot.
#[derive(Debug)]
pub struct OptimizerQualificationCandidateSetOutput {
    pub snapshot_epoch: u64,
    pub candidate_limit: usize,
    pub distinct_candidate_count: usize,
    /// Aggregate result-materialization ceiling for the complete candidate set.
    pub result_memory_limit: usize,
    /// Exact result-materialization bytes retained by all successful candidates.
    pub retained_result_bytes: usize,
    pub candidates: Vec<OptimizerQualificationCandidateOutput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptimizerQualificationResultError {
    CandidateFailed {
        index: usize,
        kind: OptimizerQualificationCandidateKind,
        error: String,
    },
    ResultMismatch {
        ordinary_index: usize,
        candidate_index: usize,
        kind: OptimizerQualificationCandidateKind,
    },
}

impl std::fmt::Display for OptimizerQualificationResultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CandidateFailed { index, kind, error } => write!(
                f,
                "optimizer qualification candidate {index} ({}) failed: {error}",
                kind.as_str()
            ),
            Self::ResultMismatch {
                ordinary_index,
                candidate_index,
                kind,
            } => write!(
                f,
                "optimizer qualification candidate {candidate_index} ({}) differs from ordinary candidate {ordinary_index}",
                kind.as_str()
            ),
        }
    }
}

impl std::error::Error for OptimizerQualificationResultError {}

impl OptimizerQualificationCandidateSetOutput {
    /// Compares complete owned outputs, including schema, order, duplicates and
    /// NULLs. Execution failures remain evidence and are never treated as equal.
    pub fn verify_complete_results(
        &self,
    ) -> std::result::Result<(), OptimizerQualificationResultError> {
        let ordinary_index = self
            .candidates
            .iter()
            .position(|candidate| candidate.kind == OptimizerQualificationCandidateKind::Ordinary)
            .expect("bounded candidate set always retains its ordinary control");
        let ordinary = self.candidates[ordinary_index]
            .output
            .result
            .as_ref()
            .map_err(|error| OptimizerQualificationResultError::CandidateFailed {
                index: ordinary_index,
                kind: OptimizerQualificationCandidateKind::Ordinary,
                error: error.to_string(),
            })?;
        for (index, candidate) in self.candidates.iter().enumerate() {
            let output = candidate.output.result.as_ref().map_err(|error| {
                OptimizerQualificationResultError::CandidateFailed {
                    index,
                    kind: candidate.kind,
                    error: error.to_string(),
                }
            })?;
            if output.output != ordinary.output {
                return Err(OptimizerQualificationResultError::ResultMismatch {
                    ordinary_index,
                    candidate_index: index,
                    kind: candidate.kind,
                });
            }
        }
        Ok(())
    }
}

impl<S: executor::ExecutionStore> DatabaseReadTransaction<S> {
    /// Prepares and executes one read with explicit rule/ranking controls on
    /// this pinned snapshot. The same parser, planner and admitted executor
    /// serve ordinary reads. Qualified plans bypass physical cache admission;
    /// no shared optimizer state, quota, capability or legality check changes.
    ///
    /// This qualification-only entry point is available in default builds for
    /// offline library diagnostics. Serving reads should use the ordinary query
    /// and streaming APIs rather than rule-disabling or ranking controls.
    pub fn qualify_optimizer_query(
        &mut self,
        request: QueryRequest<'_>,
        qualification: OptimizerQualificationOptions,
        search: OptimizerSearchDirective,
    ) -> Result<OptimizerQualificationOutput> {
        if qualification.cost_perturbation().is_some()
            && qualification.bounded_order_alternative().is_some()
        {
            return Err(HawDBError::Execution(
                "optimizer qualification cannot combine cost perturbation with an explicit bounded-order alternative"
                    .into(),
            ));
        }
        self.store.ensure_usable()?;
        let task =
            query_request::bind_request_context(self.task_context.as_ref(), request.task_context)?;
        let work_units_before = task
            .as_ref()
            .map_or(0, hawdb_core::RuntimeTaskContext::work_units);
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
        if let Some(alternative) = qualification.bounded_order_alternative() {
            let applications = optimized
                .trace
                .rule_events
                .iter()
                .filter(|event| {
                    event.rule() == BOUNDED_ORDER_QUALIFICATION_RULE
                        && event.outcome() == RuleOutcome::Applied
                })
                .count();
            if applications != 1 {
                return Err(HawDBError::Execution(format!(
                    "optimizer qualification requested bounded-order alternative {} but found {applications} legal selection sites; exactly one is required",
                    alternative.as_str()
                )));
            }
        }
        if executor::is_mutation_plan(&optimized.physical_plan)?
            || executor::requires_write_access(&optimized.physical_plan)
        {
            return Err(HawDBError::Execution(
                "optimizer qualification cannot execute a mutation or write procedure".into(),
            ));
        }
        let optimizer_trace = optimized.trace.clone();
        let selected_plan_instance_fingerprint = optimized.physical_plan.instance_fingerprint();
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
        let work_units = task
            .as_ref()
            .map_or(0, hawdb_core::RuntimeTaskContext::work_units)
            .saturating_sub(work_units_before);
        Ok(OptimizerQualificationOutput {
            snapshot_epoch,
            optimizer_trace,
            selected_plan_instance_fingerprint,
            cost_policy_id: DEFAULT_PLAN_COST_POLICY_ID,
            cost_weights: DEFAULT_PLAN_COST_WEIGHTS,
            work_units,
            result,
        })
    }

    /// Executes the ordinary, `TopN`, and `Sort + Limit` candidates for one
    /// bounded-order query on this pinned snapshot. All candidates are produced
    /// by the ordinary parser, lowerer, legality checks and admitted executor.
    pub fn qualify_optimizer_bounded_order_candidates(
        &mut self,
        request: QueryRequest<'_>,
        attempt_limits: OptimizerQualificationAttemptLimits,
        search: OptimizerSearchDirective,
    ) -> Result<OptimizerQualificationCandidateSetOutput> {
        let candidates = [
            OptimizerQualificationCandidateRequest::new(
                OptimizerQualificationCandidateKind::Ordinary,
            ),
            OptimizerQualificationCandidateRequest::new(
                OptimizerQualificationCandidateKind::BoundedOrder(
                    OptimizerBoundedOrderAlternative::TopN,
                ),
            ),
            OptimizerQualificationCandidateRequest::new(
                OptimizerQualificationCandidateKind::BoundedOrder(
                    OptimizerBoundedOrderAlternative::SortThenLimit,
                ),
            ),
        ];
        self.qualify_optimizer_bounded_order_candidate_requests(
            request,
            &candidates,
            attempt_limits,
            search,
        )
    }

    /// Executes a bounded, caller-ordered inventory of named legal candidates.
    /// Requests are validated before planning; replay identities are checked
    /// before any candidate can be accepted as matching prior evidence.
    /// All attempts share one logical statement clock, preserving an existing
    /// caller pin. Replaying clock-dependent queries requires the same caller
    /// pin; an unpinned later campaign samples a new instant and may be stale.
    pub fn qualify_optimizer_bounded_order_candidate_requests(
        &mut self,
        request: QueryRequest<'_>,
        requests: &[OptimizerQualificationCandidateRequest<'_>],
        attempt_limits: OptimizerQualificationAttemptLimits,
        search: OptimizerSearchDirective,
    ) -> Result<OptimizerQualificationCandidateSetOutput> {
        hawdb_core::temporal::with_stable_statement_clock(|| {
            self.qualify_optimizer_candidate_requests(request, requests, attempt_limits, search)
        })
    }

    fn qualify_optimizer_candidate_requests(
        &mut self,
        request: QueryRequest<'_>,
        requests: &[OptimizerQualificationCandidateRequest<'_>],
        attempt_limits: OptimizerQualificationAttemptLimits,
        search: OptimizerSearchDirective,
    ) -> Result<OptimizerQualificationCandidateSetOutput> {
        if requests.len() > OPTIMIZER_QUALIFICATION_CANDIDATE_LIMIT {
            return Err(HawDBError::Execution(format!(
                "optimizer qualification requested {} candidates; limit is {}",
                requests.len(),
                OPTIMIZER_QUALIFICATION_CANDIDATE_LIMIT
            )));
        }
        if !requests
            .iter()
            .any(|request| request.kind == OptimizerQualificationCandidateKind::Ordinary)
        {
            return Err(HawDBError::Execution(
                "optimizer qualification candidate inventory is missing the ordinary plan".into(),
            ));
        }
        for (index, request) in requests.iter().enumerate() {
            if requests[..index]
                .iter()
                .any(|earlier| earlier.kind == request.kind)
            {
                return Err(HawDBError::Execution(format!(
                    "optimizer qualification candidate inventory contains duplicate kind {}",
                    request.kind.as_str()
                )));
            }
        }

        let campaign_task =
            query_request::bind_request_context(self.task_context.as_ref(), request.task_context)?;
        let result_memory_limit = executor::enforced_result_memory_budget(
            &self.config.execution_memory,
            campaign_task.as_ref(),
        )?
        .get();
        let working_memory_limit = executor::enforced_query_memory_budget(
            &self.config.execution_memory,
            campaign_task.as_ref(),
        )?
        .get();
        let working_memory_limit = u64::try_from(working_memory_limit).map_err(|_| {
            HawDBError::Execution(
                "optimizer qualification campaign working-memory limit does not fit u64".into(),
            )
        })?;

        let mut candidates = Vec::with_capacity(requests.len());
        let mut retained_result_bytes = 0usize;
        for candidate_request in requests {
            let kind = candidate_request.kind;
            let qualification = match kind {
                OptimizerQualificationCandidateKind::Ordinary => {
                    OptimizerQualificationOptions::default()
                }
                OptimizerQualificationCandidateKind::BoundedOrder(alternative) => {
                    OptimizerQualificationOptions::default()
                        .with_bounded_order_alternative(alternative)
                }
            };
            let remaining_result_bytes = result_memory_limit
                .checked_sub(retained_result_bytes)
                .filter(|remaining| *remaining != 0)
                .ok_or_else(|| {
                    HawDBError::Execution(format!(
                        "optimizer qualification campaign exhausted its {result_memory_limit}-byte result-memory budget after retaining {retained_result_bytes} bytes"
                    ))
                })?;
            let remaining_result_bytes = u64::try_from(remaining_result_bytes).map_err(|_| {
                HawDBError::Execution(
                    "optimizer qualification campaign result-memory limit does not fit u64".into(),
                )
            })?;
            let attempt_task = attempt_limits.task_context()?;
            let attempt_task = request
                .task_context
                .map_or_else(
                    || Ok(attempt_task.clone()),
                    |campaign_task| campaign_task.constrained_by(&attempt_task),
                )?
                .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(
                    working_memory_limit,
                    remaining_result_bytes,
                ));
            let attempt_request = QueryRequest {
                task_context: Some(&attempt_task),
                ..request
            };
            let output = self.qualify_optimizer_query(attempt_request, qualification, search)?;
            if let Some(expected) = candidate_request.expected_plan_instance_fingerprint
                && output.selected_plan_instance_fingerprint != expected
            {
                return Err(HawDBError::Execution(format!(
                    "optimizer qualification candidate {} is stale: expected physical fingerprint {expected:?}, observed {:?}",
                    kind.as_str(),
                    output.selected_plan_instance_fingerprint
                )));
            }
            let same_plan_as =
                candidates
                    .iter()
                    .position(|candidate: &OptimizerQualificationCandidateOutput| {
                        candidate.output.selected_plan_instance_fingerprint
                            == output.selected_plan_instance_fingerprint
                    });
            let candidate_result_bytes = output
                .result
                .as_ref()
                .map_or(0, |result| result.output.rows.retained_memory_bytes());
            retained_result_bytes = retained_result_bytes
                .checked_add(candidate_result_bytes)
                .filter(|retained| *retained <= result_memory_limit)
                .ok_or_else(|| {
                    HawDBError::Execution(format!(
                        "optimizer qualification candidate {} exceeded the campaign's {result_memory_limit}-byte result-memory budget",
                        kind.as_str()
                    ))
                })?;
            candidates.push(OptimizerQualificationCandidateOutput {
                kind,
                same_plan_as,
                output,
            });
        }

        let snapshot_epoch = candidates[0].output.snapshot_epoch;
        if candidates
            .iter()
            .any(|candidate| candidate.output.snapshot_epoch != snapshot_epoch)
        {
            return Err(HawDBError::Execution(
                "optimizer qualification candidates did not retain one snapshot epoch".into(),
            ));
        }
        let distinct_candidate_count = candidates
            .iter()
            .map(|candidate| &candidate.output.selected_plan_instance_fingerprint)
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        if distinct_candidate_count < 2 {
            return Err(HawDBError::Execution(
                "optimizer qualification candidate inventory contains fewer than two distinct physical plans"
                    .into(),
            ));
        }
        Ok(OptimizerQualificationCandidateSetOutput {
            snapshot_epoch,
            candidate_limit: OPTIMIZER_QUALIFICATION_CANDIDATE_LIMIT,
            distinct_candidate_count,
            result_memory_limit,
            retained_result_bytes,
            candidates,
        })
    }
}
