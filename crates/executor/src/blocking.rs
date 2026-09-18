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

//! Memory-bounded blocking operators and spill-backed execution.

use crate::binding::{binding_memory_bytes, value_memory_bytes, Binding, TopNBinding};
use crate::expression::{
    binding_has_variable, binding_identity_key, binding_property, binding_value, group_key_value,
    insert_projected_value, sort_value,
};
use crate::kernel::{ensure_operator_item_fits, OperatorMemoryTracker, SpillBudgetTracker};
use crate::observer::ExecutionObserver;
use crate::pipeline::{
    emit_binding_iterator, runtime_checkpoint, AccountedBindingBatch, BatchControl, BindingBatch,
};
use crate::spill;
use crate::{
    BlockingOperatorMemoryReport, ExecutionLimit, ExecutionMemoryConfig, QueryMemoryAccount,
    QueryMemoryClass, QueryMemoryLedger,
};
use hawdb_core::{Catalog, HawDBError, Result, RuntimeTaskContext, Value};
use hawdb_plan::{
    AggregateFunction, AggregateTarget, Aggregation, PhysicalPlan, Projection, SortDirection,
    SortItem,
};
use std::cmp::Ordering;
use std::collections::{hash_map::RandomState, BTreeMap, BinaryHeap, HashMap, HashSet};
use std::num::NonZeroUsize;

pub use crate::pipeline::{BatchExecutionContext as BlockingExecutionContext, BindingBatchSource};

pub fn in_memory_report(
    operator: &'static str,
    tracker: &OperatorMemoryTracker,
    peak_tracked_bytes: usize,
    input_rows: usize,
    memory: &ExecutionMemoryConfig,
) -> BlockingOperatorMemoryReport {
    BlockingOperatorMemoryReport {
        operator: operator.to_string(),
        budget_bytes: tracker.budget_bytes,
        peak_tracked_bytes,
        input_rows,
        candidate_rows: 0,
        replay_rows: 0,
        repartitions: 0,
        max_spill_bytes: memory.max_spill_bytes.get(),
        max_spill_runs: memory.max_spill_runs.get(),
        spilled_bytes: 0,
        spill_run_count: 0,
        spilled_rows: 0,
    }
}

pub fn spill_backed_report(
    operator: &'static str,
    tracker: &OperatorMemoryTracker,
    peak_tracked_bytes: usize,
    input_rows: usize,
    spill_budget: &SpillBudgetTracker,
    spilled_rows: usize,
) -> BlockingOperatorMemoryReport {
    BlockingOperatorMemoryReport {
        operator: operator.to_string(),
        budget_bytes: tracker.budget_bytes,
        peak_tracked_bytes,
        input_rows,
        candidate_rows: 0,
        replay_rows: 0,
        repartitions: 0,
        max_spill_bytes: spill_budget.max_bytes,
        max_spill_runs: spill_budget.max_runs,
        spilled_bytes: spill_budget.used_bytes,
        spill_run_count: spill_budget.run_count,
        spilled_rows,
    }
}

pub fn spill_binding_run(
    operator: &str,
    bindings: &mut Vec<Binding>,
    spill_budget: &mut SpillBudgetTracker,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<spill::SpillRun> {
    runtime_checkpoint(task_context)?;
    let (run, mut writer) = spill_budget.create_run(operator)?;
    for binding in bindings.drain(..) {
        runtime_checkpoint(task_context)?;
        writer.write(0, &binding, spill_budget)?;
    }
    writer.finish()?;
    Ok(run)
}

pub fn stream_cartesian_product_batches(
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    context: BlockingExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let blocking_account = context.operator_account("NodeCartesianProductExec");
    let mut tracker = OperatorMemoryTracker::with_account(
        context.memory.blocking_operator_bytes,
        blocking_account.clone(),
    );
    let mut spill_budget = SpillBudgetTracker::with_ledger(
        "NodeCartesianProductExec",
        context.memory,
        context.memory_ledger,
    );
    let mut right_bindings = Vec::new();
    let mut runs = Vec::new();
    let mut right_ordinal = 0u64;
    source.execute(right, ExecutionLimit::unlimited(), &mut |batch| {
        for binding in batch {
            let bytes = binding_memory_bytes(&binding);
            ensure_operator_item_fits("NodeCartesianProductExec", bytes, &tracker)?;
            if tracker.would_exceed(bytes) {
                runs.push(spill_binding_run(
                    "cartesian",
                    &mut right_bindings,
                    &mut spill_budget,
                    context.task_context,
                )?);
                tracker.reset();
            }
            tracker.try_charge(bytes)?;
            right_bindings.push(binding);
            right_ordinal = right_ordinal.saturating_add(1);
        }
        Ok(BatchControl::Continue)
    })?;
    if !runs.is_empty() && !right_bindings.is_empty() {
        runs.push(spill_binding_run(
            "cartesian",
            &mut right_bindings,
            &mut spill_budget,
            context.task_context,
        )?);
        tracker.reset();
    }
    context
        .observer
        .record_blocking_memory_report(spill_backed_report(
            "NodeCartesianProductExec",
            &tracker,
            tracker.peak_bytes,
            right_ordinal as usize,
            &spill_budget,
            if runs.is_empty() {
                0
            } else {
                right_ordinal as usize
            },
        ));
    if right_bindings.is_empty() && runs.is_empty() {
        return Ok(BatchControl::Continue);
    }

    let output_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "NodeCartesianProductExec output",
        context.memory.batch_payload_bytes,
    );
    let mut output = CartesianOutput::new(
        "NodeCartesianProductExec output",
        context.memory.batch_rows.get(),
        context.memory.batch_payload_bytes,
        output_account,
        execution_limit,
    );
    let mut replay_tracker = OperatorMemoryTracker::with_account(
        context.memory.blocking_operator_bytes,
        blocking_account,
    );
    let control = source.execute(left, ExecutionLimit::unlimited(), &mut |batch| {
        for left_binding in batch {
            if runs.is_empty() {
                for right_binding in &right_bindings {
                    if output.push(&left_binding, right_binding, emit)? == BatchControl::Stop {
                        return Ok(BatchControl::Stop);
                    }
                }
            } else {
                for run in &runs {
                    runtime_checkpoint(context.task_context)?;
                    let mut reader = run.reader()?;
                    while let Some(record) = reader.read_binding_record(
                        context.memory.blocking_operator_bytes.get(),
                        &spill_budget,
                    )? {
                        runtime_checkpoint(context.task_context)?;
                        let right_binding = record.try_map(
                            "NodeCartesianProductExec replay",
                            context.memory.blocking_operator_bytes.get(),
                            &mut replay_tracker,
                            |_, binding| Ok(binding),
                            binding_memory_bytes,
                        )?;
                        let right_bytes = binding_memory_bytes(&right_binding);
                        let control = output.push(&left_binding, &right_binding, emit);
                        replay_tracker.release(right_bytes);
                        if control? == BatchControl::Stop {
                            return Ok(BatchControl::Stop);
                        }
                    }
                }
                if output.is_complete() {
                    return Ok(BatchControl::Stop);
                }
            }
        }
        Ok(BatchControl::Continue)
    })?;
    if output.finish(emit)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(control)
}

struct CartesianOutput {
    operator: &'static str,
    batch_rows: usize,
    execution_limit: ExecutionLimit,
    batch: BindingBatch,
    emitted: usize,
    tracker: OperatorMemoryTracker,
}

impl CartesianOutput {
    fn new(
        operator: &'static str,
        batch_rows: usize,
        memory_budget: NonZeroUsize,
        account: QueryMemoryAccount,
        execution_limit: ExecutionLimit,
    ) -> Self {
        Self {
            operator,
            batch_rows,
            execution_limit,
            batch: Vec::with_capacity(batch_rows),
            emitted: 0,
            tracker: OperatorMemoryTracker::with_account(memory_budget, account),
        }
    }

    fn push(
        &mut self,
        left: &Binding,
        right: &Binding,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let reserved_bytes = binding_memory_bytes(left).saturating_add(binding_memory_bytes(right));
        ensure_operator_item_fits(self.operator, reserved_bytes, &self.tracker)?;
        if self.tracker.would_exceed(reserved_bytes)
            && !self.batch.is_empty()
            && self.emit(emit)? == BatchControl::Stop
        {
            return Ok(BatchControl::Stop);
        }
        self.tracker.try_charge(reserved_bytes)?;
        let binding = merge_cartesian_bindings(left, right);
        let actual_bytes = binding_memory_bytes(&binding);
        self.tracker
            .release(reserved_bytes.saturating_sub(actual_bytes));
        self.batch.push(binding);
        self.emitted = self.emitted.saturating_add(1);
        if self.batch.len() == self.batch_rows && self.emit(emit)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        Ok(if self.is_complete() {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        })
    }

    fn finish(
        &mut self,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        if self.batch.is_empty() {
            Ok(BatchControl::Continue)
        } else {
            self.emit(emit)
        }
    }

    fn emit(
        &mut self,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let outgoing = std::mem::replace(&mut self.batch, Vec::with_capacity(self.batch_rows));
        self.tracker.reset();
        emit(outgoing)
    }

    fn is_complete(&self) -> bool {
        self.execution_limit.is_reached(self.emitted)
    }
}

fn merge_cartesian_bindings(left: &Binding, right: &Binding) -> Binding {
    let mut values = left.values.clone();
    values.extend(right.values.clone());
    let mut nodes = left.nodes.clone();
    nodes.extend(right.nodes.clone());
    let mut relationships = left.relationships.clone();
    relationships.extend(right.relationships.clone());
    Binding {
        values,
        nodes,
        relationships,
    }
}

mod admitted_hash_join;
mod aggregate;
mod distinct;
mod hash_key;
#[cfg(test)]
mod hash_oracle;
mod join;
mod sort;

use hash_key::{hash_entry_overhead, hash_set_capacity_bytes, HashGroups, HashedKey};

pub use admitted_hash_join::{
    AdmittedHashJoin, AdmittedHashJoinAdapter, AdmittedHashJoinCandidate, AdmittedHashJoinControl,
    AdmittedHashJoinRecord, AdmittedHashJoinSide, AdmittedHashJoinWork,
};
pub use aggregate::*;
pub use distinct::*;
pub use join::stream_hash_join_batches;
pub use sort::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observer::NoopExecutionObserver;
    use std::cell::RefCell;

    #[derive(Default)]
    struct RecordingExecutionObserver {
        reports: RefCell<Vec<BlockingOperatorMemoryReport>>,
    }

    impl ExecutionObserver for RecordingExecutionObserver {
        fn record_blocking_memory_report(&self, report: BlockingOperatorMemoryReport) {
            self.reports.borrow_mut().push(report);
        }
    }

    struct FixedBatchSource {
        batches: Vec<BindingBatch>,
    }

    impl BindingBatchSource for FixedBatchSource {
        fn execute(
            &mut self,
            _input: &PhysicalPlan,
            _execution_limit: ExecutionLimit,
            emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
        ) -> Result<BatchControl> {
            for batch in std::mem::take(&mut self.batches) {
                if emit(batch)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
            }
            Ok(BatchControl::Continue)
        }
    }

    fn value_binding(value: i64) -> Binding {
        Binding {
            values: BTreeMap::from([("value".to_string(), Value::Int(value))]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        }
    }

    #[test]
    fn report_factory_snapshots_tracker_and_spill_state() {
        let memory = ExecutionMemoryConfig::default();
        let mut tracker = OperatorMemoryTracker::new(memory.blocking_operator_bytes);
        tracker.try_charge(64).unwrap();
        let mut spill = SpillBudgetTracker::new("SortExec", &memory);
        spill.used_bytes = 128;
        spill.run_count = 1;

        let report = spill_backed_report("SortExec", &tracker, 96, 10, &spill, 8);

        assert_eq!(report.operator, "SortExec");
        assert_eq!(report.peak_tracked_bytes, 96);
        assert_eq!(report.input_rows, 10);
        assert_eq!(report.spilled_bytes, 128);
        assert_eq!(report.spill_run_count, 1);
        assert_eq!(report.spilled_rows, 8);
    }

    #[test]
    fn distinct_operator_consumes_storage_neutral_batches_in_input_order() {
        let input = PhysicalPlan::SeqNodeScan {
            variable: "node".to_string(),
            label: String::new(),
        };
        let mut source = FixedBatchSource {
            batches: vec![
                vec![value_binding(1), value_binding(1)],
                vec![value_binding(2)],
            ],
        };
        let catalog = Catalog::default();
        let memory = ExecutionMemoryConfig::default();
        let memory_ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let observer = NoopExecutionObserver;
        let mut output = Vec::new();

        stream_distinct_batches(
            &input,
            &mut source,
            BlockingExecutionContext {
                catalog: &catalog,
                memory: &memory,
                memory_ledger: &memory_ledger,
                task_context: None,
                observer: &observer,
            },
            ExecutionLimit::unlimited(),
            &mut |batch| {
                output.extend(batch);
                Ok(BatchControl::Continue)
            },
        )
        .expect("distinct execution");

        assert_eq!(output, vec![value_binding(1), value_binding(2)]);
    }

    #[test]
    fn distinct_operator_keeps_equal_values_from_different_schemas() {
        let input = PhysicalPlan::SeqNodeScan {
            variable: "node".to_string(),
            label: String::new(),
        };
        let left = Binding::scalar("left", Value::Int(1));
        let right = Binding::scalar("right", Value::Int(1));
        let left_null = Binding::scalar("left", Value::Null);
        let mut source = FixedBatchSource {
            batches: vec![
                vec![left.clone(), right.clone(), left_null.clone()],
                vec![left.clone(), left_null.clone()],
            ],
        };
        let catalog = Catalog::default();
        let memory = ExecutionMemoryConfig::default();
        let memory_ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let observer = NoopExecutionObserver;
        let mut output = Vec::new();

        stream_distinct_batches(
            &input,
            &mut source,
            BlockingExecutionContext {
                catalog: &catalog,
                memory: &memory,
                memory_ledger: &memory_ledger,
                task_context: None,
                observer: &observer,
            },
            ExecutionLimit::unlimited(),
            &mut |batch| {
                output.extend(batch);
                Ok(BatchControl::Continue)
            },
        )
        .expect("distinct execution");

        assert_eq!(output, vec![left, right, left_null]);
    }

    #[test]
    fn distinct_operator_preserves_mixed_schemas_and_null_across_spills() {
        let input = PhysicalPlan::SeqNodeScan {
            variable: "node".to_string(),
            label: String::new(),
        };
        let mut expected = Vec::new();
        for name in ["left", "right"] {
            for index in 0..3 {
                expected.push(Binding::scalar(
                    name,
                    Value::String(format!("{name}-{index}-{}", "x".repeat(96))),
                ));
            }
            expected.push(Binding::scalar(name, Value::Null));
        }
        let mut source = FixedBatchSource {
            batches: vec![expected.clone(), expected.clone()],
        };
        let catalog = Catalog::default();
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(2048).unwrap(),
            ..ExecutionMemoryConfig::default()
        };
        let memory_ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let observer = RecordingExecutionObserver::default();
        let mut output = Vec::new();

        stream_distinct_batches(
            &input,
            &mut source,
            BlockingExecutionContext {
                catalog: &catalog,
                memory: &memory,
                memory_ledger: &memory_ledger,
                task_context: None,
                observer: &observer,
            },
            ExecutionLimit::unlimited(),
            &mut |batch| {
                output.extend(batch);
                Ok(BatchControl::Continue)
            },
        )
        .expect("distinct spill execution");

        assert_eq!(output.len(), expected.len());
        for binding in expected {
            assert_eq!(output.iter().filter(|row| **row == binding).count(), 1);
        }
        let reports = observer.reports.borrow();
        let report = reports
            .iter()
            .find(|report| report.operator == "DistinctExec")
            .expect("distinct memory report");
        assert!(report.spill_run_count > 0);
        assert!(report.peak_tracked_bytes <= report.budget_bytes);
    }

    #[test]
    fn top_n_operator_applies_offset_limit_and_parent_cap() {
        let input = PhysicalPlan::SeqNodeScan {
            variable: "node".to_string(),
            label: String::new(),
        };
        let mut source = FixedBatchSource {
            batches: vec![vec![
                value_binding(5),
                value_binding(1),
                value_binding(3),
                value_binding(2),
                value_binding(4),
            ]],
        };
        let catalog = Catalog::default();
        let memory = ExecutionMemoryConfig::default();
        let memory_ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let observer = NoopExecutionObserver;
        let mut output = Vec::new();

        stream_top_n_batches(
            &input,
            &[SortItem {
                key: hawdb_plan::SortKey::Column("value".to_string()),
                direction: SortDirection::Asc,
            }],
            1,
            3,
            &mut source,
            BlockingExecutionContext {
                catalog: &catalog,
                memory: &memory,
                memory_ledger: &memory_ledger,
                task_context: None,
                observer: &observer,
            },
            ExecutionLimit {
                output_rows: Some(2),
            },
            &mut |batch| {
                output.extend(batch);
                Ok(BatchControl::Continue)
            },
        )
        .expect("top-n execution");

        assert_eq!(output, vec![value_binding(2), value_binding(3)]);
    }

    #[test]
    fn cartesian_output_is_root_admitted_before_binding_clones() {
        let root_budget = NonZeroUsize::new(300).unwrap();
        let ledger = QueryMemoryLedger::new(root_budget);
        let mut retained = OperatorMemoryTracker::with_account(
            root_budget,
            ledger.account(QueryMemoryClass::BlockingState, "retained", root_budget),
        );
        retained.try_charge(200).unwrap();
        let mut output = CartesianOutput::new(
            "NodeCartesianProductExec output",
            8,
            root_budget,
            ledger.account(
                QueryMemoryClass::PipelineBatch,
                "cartesian output",
                root_budget,
            ),
            ExecutionLimit::unlimited(),
        );
        let mut emitted = false;

        let error = output
            .push(&value_binding(1), &value_binding(2), &mut |_| {
                emitted = true;
                Ok(BatchControl::Continue)
            })
            .unwrap_err();

        assert!(error.to_string().contains("query_memory_bytes 300"));
        assert!(!emitted);
        assert!(output.batch.is_empty());
        assert_eq!(output.tracker.used_bytes, 0);
        assert_eq!(ledger.snapshot().used_bytes, 200);
        drop(output);
        drop(retained);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn cartesian_output_flushes_before_exceeding_batch_payload_budget() {
        let left = value_binding(1);
        let right = value_binding(2);
        let reserved_bytes =
            binding_memory_bytes(&left).saturating_add(binding_memory_bytes(&right));
        let actual_bytes = binding_memory_bytes(&merge_cartesian_bindings(&left, &right));
        let budget = NonZeroUsize::new(
            actual_bytes
                .saturating_add(reserved_bytes)
                .saturating_sub(1),
        )
        .unwrap();
        assert!(reserved_bytes <= budget.get());

        let ledger = QueryMemoryLedger::new(budget);
        let mut output = CartesianOutput::new(
            "NodeCartesianProductExec output",
            8,
            budget,
            ledger.account(QueryMemoryClass::PipelineBatch, "cartesian output", budget),
            ExecutionLimit::unlimited(),
        );
        let mut emitted = Vec::new();
        {
            let mut emit = |batch| {
                emitted.push(batch);
                Ok(BatchControl::Continue)
            };
            assert_eq!(
                output.push(&left, &right, &mut emit).unwrap(),
                BatchControl::Continue
            );
        }
        assert!(emitted.is_empty());
        {
            let mut emit = |batch| {
                emitted.push(batch);
                Ok(BatchControl::Continue)
            };
            assert_eq!(
                output.push(&left, &right, &mut emit).unwrap(),
                BatchControl::Continue
            );
        }
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].len(), 1);
        {
            let mut emit = |batch| {
                emitted.push(batch);
                Ok(BatchControl::Continue)
            };
            assert_eq!(output.finish(&mut emit).unwrap(), BatchControl::Continue);
        }
        assert_eq!(emitted.len(), 2);
        assert_eq!(emitted[1].len(), 1);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
