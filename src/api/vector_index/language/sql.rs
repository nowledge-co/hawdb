// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_relational::query::vector_index::{
    collect_vector_eligible_domain, execute_vector_index_select, VectorIndexCandidates,
    VectorIndexInput, VectorIndexSqlExecution,
};
use hawdb_sql::{SqlStatement, SqlValue};

pub struct VectorIndexSqlRequest<'a> {
    pub sql: &'a str,
    pub parameters: &'a [hawdb_core::Value],
    pub output_limits: crate::QueryStreamOptions,
    pub task: RuntimeTaskContext,
}

#[derive(Debug)]
pub struct VectorIndexSqlReport {
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    pub row_execution: hawdb_relational::row_runtime::RelationalRowExecutionEvidence,
    pub eligibility_row_execution:
        Option<hawdb_relational::row_runtime::RelationalRowExecutionEvidence>,
    index: Arc<VectorIndexRetainedOutput>,
    _metadata_memory: RuntimeRetainedMemory,
}

impl VectorIndexSqlReport {
    pub fn index(&self) -> &VectorIndexQueryReport {
        self.index.report()
    }

    /// Logical index reads and spill counters from the retained execution.
    pub fn index_metrics(&self) -> &crate::SearchOutOfCoreMetrics {
        self.index.metrics()
    }

    /// Runtime memory still owned by the index output, excluding query rows.
    pub fn index_retained_capacity_bytes(&self) -> u64 {
        self.index.retained_capacity_bytes()
    }
}

impl VectorIndexReadTransaction {
    pub fn query_sql_streaming(
        &self,
        permit: &RuntimePermit,
        budget: VectorIndexLanguageBudget,
        request: VectorIndexSqlRequest<'_>,
        mut consumer: impl FnMut(crate::executor::Row) -> Result<()>,
    ) -> std::result::Result<VectorIndexSqlReport, VectorIndexReadError> {
        self.require_query_permit(permit)?;
        let (index_task, query_task) = budget.split(permit, request.task)?;
        crate::api::query_runtime::query_runtime_checkpoint(Some(&query_task))?;
        let VectorIndexTarget::Relational { table, .. } = self.definition.target() else {
            return Err(
                invalid("SQL vector index search requires a relational column index").into(),
            );
        };
        let metadata_memory = permit
            .reserve_retained_memory(4096)
            .map_err(VectorIndexReadError::Admission)?;
        let prepared = self
            .source
            .relational_plan_template_cache
            .prepare(request.sql)?;
        if prepared.template.parameters.len() != request.parameters.len() {
            return Err(invalid(
                "SQL vector index parameter count differs from the prepared statement",
            )
            .into());
        }
        let SqlStatement::Select(select) = prepared.statement() else {
            return Err(invalid("vector index SQL streaming requires SELECT").into());
        };
        let search = select
            .vector_index
            .as_ref()
            .ok_or_else(|| invalid("SQL source must be vector_index_search"))?;
        let hawdb_core::Value::String(index) = bind(&search.index, request.parameters)? else {
            return Err(invalid("vector index name must be a string").into());
        };
        if index != self.definition.name() {
            return Err(invalid("query index differs from the bound index definition").into());
        }
        if !matches!(&search.embedding, SqlValue::Parameter(_)) {
            return Err(invalid("SQL vector query requires a typed vector parameter").into());
        }
        let hawdb_core::Value::Vector(embedding) = bind(&search.embedding, request.parameters)?
        else {
            return Err(invalid("SQL vector query requires a typed vector parameter").into());
        };
        let positive = |value: &SqlValue| -> Result<usize> {
            let hawdb_core::Value::Int(value) = bind(value, request.parameters)? else {
                return Err(invalid("vector index limits must be positive integers"));
            };
            usize::try_from(*value)
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| invalid("vector index limits must be positive integers"))
        };
        let top_k = positive(&search.top_k)?;
        let candidate_limit = search
            .candidates
            .as_ref()
            .map(positive)
            .transpose()?
            .unwrap_or(top_k);
        if top_k > candidate_limit {
            return Err(invalid("vector top_k exceeds candidates").into());
        }
        let canonical_memory = hawdb_executor::QueryMemoryLedger::for_query(
            &self.source.config.execution_memory,
            Some(&query_task),
        )?;
        let eligible = if search.mode.requires_eligible_domain() {
            let value = search
                .max_work
                .as_ref()
                .ok_or_else(|| invalid("eligible vector search requires max_work"))?;
            let hawdb_core::Value::Int(max_work) = bind(value, request.parameters)? else {
                return Err(invalid("max_work must be a nonnegative integer").into());
            };
            let max_work = usize::try_from(*max_work)
                .map_err(|_| invalid("max_work must be a nonnegative integer"))?;
            Some(collect_vector_eligible_domain(
                select,
                request.parameters,
                &self.definition,
                self.pin.vector_index_read_identity(),
                embedding.as_ref(),
                max_work,
                VectorIndexSqlExecution {
                    memory_ledger: &canonical_memory,
                    state: self.source.store.relational_state(),
                    row_mode: hawdb_relational::row_runtime::RelationalRowReadMode::Store(
                        &self.source.store,
                    ),
                    resources: crate::api::relational_query_resource_context(
                        &self.source.config,
                        request.output_limits.max_rows,
                        request.output_limits.max_payload_bytes,
                        Some(&query_task),
                    ),
                },
            )?)
        } else {
            None
        };
        let mut provider = IndexProvider {
            pin: &self.pin,
            definition: &self.definition,
            permit,
            task: index_task,
            output: None,
            error: None,
        };
        let output = provider.execute_kernel(VectorIndexExecutionRequest {
            index,
            embedding: embedding.as_ref(),
            candidate_limit,
            top_k,
            mode: search.mode,
            eligible: eligible.as_ref().map(|(domain, _)| domain),
            resources: hawdb_executor::ExternalReadResourceContract {
                priority: 0,
                max_parallelism: NonZeroUsize::MIN,
                max_working_memory_bytes: budget.index_working_bytes,
                result: hawdb_executor::ExternalReadResultBudget {
                    max_rows: top_k,
                    max_memory_bytes: budget.index_result_bytes,
                },
                task_context: Some(&query_task),
            },
        });
        if let Some(error) = provider.error {
            return Err(VectorIndexReadError::Index(error));
        }
        let index = output?;
        // Both phases share one canonical ledger. Release the domain before
        // hydration so its retained identities no longer consume the grant.
        let eligibility_row_execution = eligible.map(|(domain, evidence)| {
            drop(domain);
            evidence
        });
        let candidates = SqlCandidates(&index);
        let rows = execute_vector_index_select(
            select,
            request.parameters,
            VectorIndexInput {
                table,
                candidates: &candidates,
            },
            VectorIndexSqlExecution {
                memory_ledger: &canonical_memory,
                state: self.source.store.relational_state(),
                row_mode: hawdb_relational::row_runtime::RelationalRowReadMode::Store(
                    &self.source.store,
                ),
                resources: crate::api::relational_query_resource_context(
                    &self.source.config,
                    request.output_limits.max_rows,
                    request.output_limits.max_payload_bytes,
                    Some(&query_task),
                ),
            },
        )?;
        let report = VectorIndexSqlReport {
            output_rows: rows.row_count(),
            output_payload_bytes: rows.output_payload_bytes,
            row_execution: rows.row_execution.clone(),
            eligibility_row_execution,
            index,
            _metadata_memory: metadata_memory,
        };
        rows.deliver(|row| {
            crate::api::query_runtime::query_runtime_checkpoint(Some(&query_task))?;
            consumer(row)?;
            crate::api::query_runtime::query_runtime_checkpoint(Some(&query_task))
        })?;
        Ok(report)
    }
}

fn bind<'a>(
    value: &'a SqlValue,
    parameters: &'a [hawdb_core::Value],
) -> Result<&'a hawdb_core::Value> {
    match value {
        SqlValue::Literal(value) => Ok(value),
        SqlValue::Expression(_) => Err(invalid(
            "SQL vector index arguments require literal or parameter values",
        )),
        SqlValue::Parameter(position) => position
            .checked_sub(1)
            .and_then(|position| parameters.get(position))
            .ok_or_else(|| invalid("missing SQL vector index parameter")),
    }
}

struct SqlCandidates<'a>(&'a VectorIndexRetainedOutput);
impl VectorIndexCandidates for SqlCandidates<'_> {
    fn row_count(&self) -> usize {
        self.0.hits().len()
    }
    fn row(&self, ordinal: usize) -> Result<(&str, f64)> {
        let hit = self
            .0
            .hits()
            .get(ordinal)
            .ok_or_else(|| invalid("vector candidate ordinal is out of range"))?;
        Ok((&hit.id, hit.distance))
    }
}
