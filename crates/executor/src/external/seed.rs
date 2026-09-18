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

//! Internal vector seed execution over the host-provided external read contract.

use super::{
    ExternalReadOperator, ExternalReadResourceContract, ExternalReadResultBudget,
    VectorSeedExecutionOutput, VectorSeedExecutionRequest,
};
use crate::binding::Binding;
use crate::kernel::collect_bounded_operator_bindings_with_account;
use crate::memory::external_read_memory_budget;
use crate::observer::QueryExecutionObserver;
use crate::pipeline::{emit_owned_binding_batches, BatchControl, BindingBatch};
use crate::{ExecutionLimit, ExecutionMemoryConfig, QueryMemoryClass, QueryMemoryLedger};
use hawdb_core::{HawDBError, Result, RuntimeTaskContext, Value};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

/// Borrows query-owned resources; admission and concrete host execution stay outside this module.
#[derive(Clone, Copy)]
pub struct VectorSeedContext<'a> {
    pub parameters: &'a BTreeMap<String, Value>,
    pub external: &'a dyn BatchExternalRead,
    pub memory: &'a ExecutionMemoryConfig,
    pub memory_ledger: &'a QueryMemoryLedger,
    pub task_context: Option<&'a RuntimeTaskContext>,
    pub observer: &'a QueryExecutionObserver,
}

pub trait BatchExternalRead {
    fn execute_vector_seed(
        &self,
        request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput>;
}

pub struct BatchExternalReadAdapter<'a> {
    external: RefCell<&'a mut dyn ExternalReadOperator>,
}

impl<'a> BatchExternalReadAdapter<'a> {
    pub fn new(external: &'a mut dyn ExternalReadOperator) -> Self {
        Self {
            external: RefCell::new(external),
        }
    }
}

impl BatchExternalRead for BatchExternalReadAdapter<'_> {
    fn execute_vector_seed(
        &self,
        request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        self.external.borrow_mut().execute_vector_seed(request)
    }
}

fn vector_embedding_parameter(
    parameters: &BTreeMap<String, Value>,
    name: &str,
    vector_plan: &hawdb_plan::VectorPhysicalPlan,
) -> Result<Vec<f32>> {
    let Some(Value::List(values)) = parameters.get(name) else {
        return Err(HawDBError::Semantic(format!(
            "vector search parameter '${name}' must be a numeric list"
        )));
    };
    let embedding = values
        .iter()
        .map(|value| match value {
            Value::Float(value) if value.is_finite() => {
                let value = *value as f32;
                if value.is_finite() {
                    Ok(value)
                } else {
                    Err(HawDBError::Semantic(format!(
                        "vector search parameter '${name}' exceeds f32 range"
                    )))
                }
            }
            Value::Int(value) => Ok(*value as f32),
            _ => Err(HawDBError::Semantic(format!(
                "vector search parameter '${name}' must contain finite numbers"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    let expected_dimension = vector_plan_embedding_dimension(vector_plan);
    if embedding.len() != expected_dimension {
        return Err(HawDBError::Semantic(format!(
            "vector search parameter '${name}' dimension changed after planning"
        )));
    }
    Ok(embedding)
}

fn vector_plan_embedding_dimension(plan: &hawdb_plan::VectorPhysicalPlan) -> usize {
    match plan {
        hawdb_plan::VectorPhysicalPlan::VectorCandidateScan {
            embedding_dimension,
            ..
        }
        | hawdb_plan::VectorPhysicalPlan::RawVectorRerank {
            embedding_dimension,
            ..
        } => *embedding_dimension,
        hawdb_plan::VectorPhysicalPlan::ResidualFilter { input, .. }
        | hawdb_plan::VectorPhysicalPlan::TopK { input, .. } => {
            vector_plan_embedding_dimension(input)
        }
        hawdb_plan::VectorPhysicalPlan::Filter { .. } => 0,
    }
}

fn vector_plan_top_k(plan: &hawdb_plan::VectorPhysicalPlan) -> Option<usize> {
    match plan {
        hawdb_plan::VectorPhysicalPlan::TopK { limit, .. } => Some(*limit),
        hawdb_plan::VectorPhysicalPlan::VectorCandidateScan { input, .. }
        | hawdb_plan::VectorPhysicalPlan::RawVectorRerank { input, .. }
        | hawdb_plan::VectorPhysicalPlan::ResidualFilter { input, .. } => vector_plan_top_k(input),
        hawdb_plan::VectorPhysicalPlan::Filter { .. } => None,
    }
}

pub struct VectorSeedScanSpec<'a> {
    pub embedding_parameter: &'a str,
    pub output_external_id: &'a bool,
    pub metadata_filters: &'a BTreeMap<String, String>,
    pub resource_profile: &'a hawdb_plan::VectorExecutionResourceProfile,
    pub vector_plan: &'a hawdb_plan::VectorPhysicalPlan,
}

impl VectorSeedScanSpec<'_> {
    pub fn stream(
        self,
        context: VectorSeedContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Self {
            embedding_parameter,
            output_external_id,
            metadata_filters,
            resource_profile,
            vector_plan,
        } = self;
        let max_rows = vector_plan_top_k(vector_plan)
            .ok_or_else(|| {
                HawDBError::Execution("vector seed physical plan is missing TopK".to_string())
            })?
            .min(execution_limit.output_rows.unwrap_or(usize::MAX));
        if max_rows == 0 {
            return emit_owned_binding_batches(Vec::new(), context.memory.batch_rows.get(), emit);
        }
        let embedding =
            vector_embedding_parameter(context.parameters, embedding_parameter, vector_plan)?;
        let external_memory = external_read_memory_budget(*resource_profile, context.memory);
        let admitted_parallelism = context
            .task_context
            .map_or(1, |task_context| task_context.admitted_parallelism().get());
        let resources = ExternalReadResourceContract {
            priority: resource_profile.priority,
            max_parallelism: NonZeroUsize::new(
                resource_profile
                    .max_parallelism
                    .max(1)
                    .min(admitted_parallelism),
            )
            .expect("resolved external read parallelism is non-zero"),
            max_working_memory_bytes: external_memory.max_working_bytes,
            result: ExternalReadResultBudget {
                max_rows,
                max_memory_bytes: external_memory.max_result_bytes,
            },
            task_context: context.task_context,
        };
        let external_account = context.memory_ledger.account(
            QueryMemoryClass::ExternalRead,
            "VectorSeedScan external read",
            NonZeroUsize::new(resources.reserved_memory_bytes())
                .expect("external read reservation is non-zero"),
        );
        let _external_lease = external_account.reserve(resources.reserved_memory_bytes())?;
        resources.checkpoint()?;
        let output = context
            .external
            .execute_vector_seed(VectorSeedExecutionRequest {
                embedding: &embedding,
                metadata_filters,
                vector_plan,
                resources,
            })?;
        resources.checkpoint()?;
        output.validate_result_budget(resources.result)?;
        context.observer.record_vector_execution(output.report);
        let mut bindings = collect_bounded_operator_bindings_with_account(
            "VectorSeedScan",
            output.rows.into_iter().map(|row| {
                let mut values = BTreeMap::from([
                    ("id".to_string(), Value::String(row.id)),
                    ("score".to_string(), Value::Float(row.score)),
                ]);
                if *output_external_id && let Some(external_id) = row.external_id {
                    values.insert("external_id".to_string(), Value::String(external_id));
                }
                Binding {
                    values,
                    nodes: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                }
            }),
            context.memory.blocking_operator_bytes,
            context.memory_ledger.account(
                QueryMemoryClass::BlockingState,
                "VectorSeedScan",
                context.memory.blocking_operator_bytes,
            ),
        )?;
        bindings.truncate(execution_limit.output_rows.unwrap_or(usize::MAX));
        emit_owned_binding_batches(bindings, context.memory.batch_rows.get(), emit)
    }
}

#[cfg(test)]
mod tests;
