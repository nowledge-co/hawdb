//! Memory-bounded blocking operators and spill-backed execution.

use crate::binding::{binding_memory_bytes, value_memory_bytes, Binding, TopNBinding};
use crate::expression::{
    binding_has_countable_variable, binding_identity_key, binding_property, binding_value,
    group_key_value, insert_projected_value, sort_value,
};
use crate::kernel::{ensure_operator_item_fits, OperatorMemoryTracker, SpillBudgetTracker};
use crate::observer::ExecutionObserver;
use crate::pipeline::{emit_binding_iterator, runtime_checkpoint, BatchControl, BindingBatch};
use crate::spill;
use crate::{BlockingOperatorMemoryReport, ExecutionLimit, ExecutionMemoryConfig};
use skein_core::{Catalog, Result, RuntimeTaskContext, SkeinError, Value};
use skein_plan::{
    AggregateFunction, AggregateTarget, Aggregation, PhysicalPlan, Projection, SortDirection,
    SortItem,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::num::NonZeroUsize;

pub trait BindingBatchSource {
    fn execute(
        &mut self,
        input: &PhysicalPlan,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl>;
}

pub struct BlockingExecutionContext<'a> {
    pub catalog: &'a Catalog,
    pub memory: &'a ExecutionMemoryConfig,
    pub task_context: Option<&'a RuntimeTaskContext>,
    pub observer: &'a dyn ExecutionObserver,
}

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

mod aggregate;
mod distinct;
mod sort;

pub use aggregate::*;
pub use distinct::*;
pub use sort::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observer::NoopExecutionObserver;

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
        tracker.charge(64);
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
        let observer = NoopExecutionObserver;
        let mut output = Vec::new();

        stream_distinct_batches(
            &input,
            &mut source,
            BlockingExecutionContext {
                catalog: &catalog,
                memory: &memory,
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
        let observer = NoopExecutionObserver;
        let mut output = Vec::new();

        stream_top_n_batches(
            &input,
            &[SortItem {
                key: skein_plan::SortKey::Column("value".to_string()),
                direction: SortDirection::Asc,
            }],
            1,
            3,
            &mut source,
            BlockingExecutionContext {
                catalog: &catalog,
                memory: &memory,
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
}
