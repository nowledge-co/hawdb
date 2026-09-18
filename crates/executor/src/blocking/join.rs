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

//! Bounded graph equi-join over owned bindings and the shared spill pool.

use super::*;
use hawdb_plan::HashJoinKey;
use std::hash::BuildHasher;

const OPERATOR: &str = "HashJoinExec";

fn join_key<'a>(binding: &'a Binding, key: &HashJoinKey) -> Option<&'a Value> {
    binding_property(binding, &key.variable, &key.property).filter(|value| **value != Value::Null)
}

struct GraphHashJoinAdapter<'a, 'output> {
    keys: (&'a HashJoinKey, &'a HashJoinKey),
    hash_state: RandomState,
    output: &'output mut CartesianOutput,
    emit: &'output mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
}

impl GraphHashJoinAdapter<'_, '_> {
    fn record(&self, binding: Binding, key: &HashJoinKey) -> Option<AdmittedHashJoinRecord> {
        let hash = self.hash_state.hash_one(join_key(&binding, key)?);
        Some(AdmittedHashJoinRecord { hash, binding })
    }
}

impl AdmittedHashJoinAdapter for GraphHashJoinAdapter<'_, '_> {
    fn validate_spill_record(
        &mut self,
        side: AdmittedHashJoinSide,
        hash: u64,
        binding: &Binding,
    ) -> Result<()> {
        let key = match side {
            AdmittedHashJoinSide::Build => self.keys.1,
            AdmittedHashJoinSide::Probe => self.keys.0,
        };
        if join_key(binding, key).map(|value| self.hash_state.hash_one(value)) != Some(hash) {
            return Err(HawDBError::StorageIntegrity(
                "HashJoinExec spill key does not match its recorded hash".into(),
            ));
        }
        Ok(())
    }

    fn visit_candidate(
        &mut self,
        probe: &Binding,
        build: &Binding,
    ) -> Result<AdmittedHashJoinCandidate> {
        if join_key(probe, self.keys.0) != join_key(build, self.keys.1) {
            return Ok(AdmittedHashJoinCandidate::rejected());
        }
        let control = self.output.push(probe, build, self.emit)?;
        Ok(AdmittedHashJoinCandidate::matched(match control {
            BatchControl::Continue => AdmittedHashJoinControl::Continue,
            BatchControl::Stop => AdmittedHashJoinControl::Stop,
        }))
    }
}

pub fn stream_hash_join_batches(
    plan: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    execute_hash_join(plan, source, context, execution_limit, emit).map(|(control, _)| control)
}

fn execute_hash_join(
    plan: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<(BatchControl, AdmittedHashJoinWork)> {
    let PhysicalPlan::HashJoinExec {
        left_key,
        right_key,
        left,
        right,
    } = plan
    else {
        return Err(HawDBError::Execution(
            "expected HashJoinExec plan".to_string(),
        ));
    };
    if execution_limit.is_reached(0) {
        return Ok((BatchControl::Stop, AdmittedHashJoinWork::default()));
    }
    let mut join = AdmittedHashJoin::new(
        OPERATOR,
        context.memory,
        context.memory_ledger,
        context.task_context,
    );
    let output_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "HashJoinExec output",
        context.memory.batch_payload_bytes,
    );
    let mut output = CartesianOutput::new(
        "HashJoinExec output",
        context.memory.batch_rows.get(),
        context.memory.batch_payload_bytes,
        output_account,
        execution_limit,
    );
    let mut adapter = GraphHashJoinAdapter {
        keys: (left_key, right_key),
        hash_state: RandomState::new(),
        output: &mut output,
        emit,
    };
    source.execute(right, ExecutionLimit::unlimited(), &mut |batch| {
        for binding in batch {
            if let Some(record) = adapter.record(binding, right_key) {
                join.push_build(record)?;
            }
        }
        Ok(BatchControl::Continue)
    })?;
    join.finish_build()?;
    let source_control = source.execute(left, ExecutionLimit::unlimited(), &mut |batch| {
        for binding in batch {
            let Some(record) = adapter.record(binding, left_key) else {
                continue;
            };
            if join.push_probe(record, &mut adapter)? == AdmittedHashJoinControl::Stop {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    })?;
    let control = if source_control == BatchControl::Stop {
        BatchControl::Stop
    } else {
        match join.finish(&mut adapter)? {
            AdmittedHashJoinControl::Continue => BatchControl::Continue,
            AdmittedHashJoinControl::Stop => BatchControl::Stop,
        }
    };
    context
        .observer
        .record_blocking_memory_report(join.report());
    let control = if output.finish(emit)? == BatchControl::Stop {
        BatchControl::Stop
    } else {
        control
    };
    Ok((control, join.work()))
}

#[cfg(test)]
mod tests;
