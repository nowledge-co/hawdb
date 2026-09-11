use super::*;
use crate::observer::NoopExecutionObserver;
use crate::pipeline::runtime_checkpoint;
use crate::{ExecutionMemoryConfig, QueryMemoryLedger};
use skein_core::{
    Catalog, LabelId, RelTypeId, RuntimeCancellationToken, RuntimeTaskContext, SkeinError, Value,
};
use skein_plan::ProjectionExpression;
use skein_storage::{NodeId, NodeRecord, RelId, RelRecord};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

struct Source<'a> {
    rows: Vec<Binding>,
    batch_rows: usize,
    requested: Vec<ExecutionLimit>,
    calls: usize,
    fail_at: Option<usize>,
    task: Option<&'a RuntimeTaskContext>,
}

impl Source<'_> {
    fn new(rows: Vec<Binding>, batch_rows: usize) -> Self {
        Self {
            rows,
            batch_rows,
            requested: Vec::new(),
            calls: 0,
            fail_at: None,
            task: None,
        }
    }
}

impl BindingBatchSource for Source<'_> {
    fn execute(
        &mut self,
        input: &PhysicalPlan,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        assert!(matches!(input, PhysicalPlan::EmptyExec));
        self.requested.push(execution_limit);
        let end = self
            .rows
            .len()
            .min(execution_limit.output_rows.unwrap_or(usize::MAX));
        runtime_checkpoint(self.task)?;
        for batch in self.rows[..end].chunks(self.batch_rows) {
            runtime_checkpoint(self.task)?;
            if self.fail_at == Some(self.calls) {
                return Err(SkeinError::Execution("source failure".into()));
            }
            self.calls += 1;
            if emit(batch.to_vec())? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    }
}

fn with_context<T>(
    batch_rows: usize,
    budget: usize,
    run: impl FnOnce(BatchExecutionContext<'_>) -> T,
) -> T {
    let catalog = Catalog::default();
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(batch_rows).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(4096).unwrap(),
        query_memory_bytes: NonZeroUsize::new(budget).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let output = run(BatchExecutionContext {
        catalog: &catalog,
        memory: &memory,
        memory_ledger: &ledger,
        task_context: None,
        observer: &NoopExecutionObserver,
    });
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0);
    assert!(snapshot.peak_bytes <= budget);
    output
}

fn rows(count: usize, seed: usize) -> Vec<Binding> {
    (0..count)
        .map(|index| {
            let value = if (index + seed).is_multiple_of(5) {
                Value::Null
            } else {
                Value::Int(((index * 7 + seed) % 13) as i64 - 6)
            };
            Binding {
                values: BTreeMap::from([
                    ("value".into(), value),
                    ("unused".into(), Value::Int(index as i64)),
                ]),
                nodes: BTreeMap::from([(
                    "n".into(),
                    NodeRecord {
                        id: NodeId(index as u64),
                        labels: BTreeSet::from([LabelId(3)]),
                        properties: BTreeMap::new(),
                    },
                )]),
                relationships: BTreeMap::from([(
                    "r".into(),
                    RelRecord {
                        id: RelId(index as u64),
                        source: NodeId(index as u64),
                        target: NodeId(index as u64 + 1),
                        rel_type: RelTypeId(4),
                        properties: BTreeMap::new(),
                    },
                )]),
            }
        })
        .collect()
}

fn items(column: &str) -> Vec<Projection> {
    vec![Projection {
        name: "alias".into(),
        expression: ProjectionExpression::Column(column.into()),
    }]
}

#[derive(Clone, Copy, Debug)]
enum Kernel {
    Filter,
    Projection,
    Limit { offset: usize, limit: Option<usize> },
}

impl Kernel {
    fn execute(
        self,
        source: &mut dyn BindingBatchSource,
        context: BatchExecutionContext<'_>,
        cap: Option<usize>,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let input = &PhysicalPlan::EmptyExec;
        let cap = ExecutionLimit { output_rows: cap };
        match self {
            Self::Filter => stream_filter_batches(
                input,
                source,
                context,
                cap,
                &mut |row| Ok(matches!(row.values["value"], Value::Int(value) if value >= 0)),
                emit,
            ),
            Self::Projection => {
                stream_projection_batches(&items("value"), input, source, context, cap, emit)
            }
            Self::Limit { offset, limit } => {
                stream_limit_batches(offset, limit, input, source, context, cap, emit)
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Exit {
    Complete,
    Stop,
    Error,
}

fn check_case(seed: usize, input_batch: usize, output_batch: usize, kernel: Kernel, exit: Exit) {
    let original = rows(seed % 25, seed);
    let cap = [None, Some(0), Some(1), Some(7), Some(usize::MAX)][seed % 5];
    let (expected, upstream_cap): (Vec<_>, _) = match kernel {
        Kernel::Filter => (
            original
                .iter()
                .filter(|row| matches!(row.values["value"], Value::Int(value) if value >= 0))
                .take(cap.unwrap_or(usize::MAX))
                .cloned()
                .collect(),
            None,
        ),
        Kernel::Projection => (
            original
                .iter()
                .take(cap.unwrap_or(usize::MAX))
                .map(|row| Binding {
                    values: BTreeMap::from([("alias".into(), row.values["value"].clone())]),
                    nodes: row.nodes.clone(),
                    relationships: row.relationships.clone(),
                })
                .collect(),
            cap,
        ),
        Kernel::Limit { offset, limit } => {
            let total =
                offset as u128 + limit.unwrap_or(usize::MAX).min(cap.unwrap_or(usize::MAX)) as u128;
            (
                original
                    .iter()
                    .skip(offset)
                    .take(limit.unwrap_or(usize::MAX))
                    .take(cap.unwrap_or(usize::MAX))
                    .cloned()
                    .collect(),
                Some(total.min(usize::MAX as u128) as usize),
            )
        }
    };
    let mut source = Source::new(original, input_batch);
    let mut actual = Vec::new();
    let mut calls = 0;
    let result = with_context(output_batch, 8192, |context| {
        kernel.execute(&mut source, context, cap, &mut |batch| {
            assert!(!batch.is_empty());
            assert!(batch.len() <= output_batch);
            assert_eq!(
                context.memory_ledger.snapshot().used_bytes,
                0,
                "transfer must release its lease"
            );
            calls += 1;
            actual.extend(batch);
            match exit {
                Exit::Complete => Ok(BatchControl::Continue),
                Exit::Stop => Ok(BatchControl::Stop),
                Exit::Error => Err(SkeinError::Execution("consumer failure".into())),
            }
        })
    });
    assert_eq!(
        source.requested,
        vec![ExecutionLimit {
            output_rows: upstream_cap
        }]
    );
    match exit {
        Exit::Complete => {
            result.unwrap();
            assert_eq!(actual, expected, "seed {seed}, kernel {kernel:?}");
        }
        Exit::Stop | Exit::Error => {
            assert!(calls <= 1, "consumer called after {exit:?}");
            assert_eq!(actual, expected[..actual.len()]);
            if calls == 1 {
                match exit {
                    Exit::Stop => assert_eq!(result.unwrap(), BatchControl::Stop),
                    Exit::Error => {
                        assert!(result.unwrap_err().to_string().contains("consumer failure"))
                    }
                    Exit::Complete => unreachable!(),
                }
            } else {
                result.unwrap();
                assert!(expected.is_empty());
            }
        }
    }
}

fn campaign(seeds: usize) {
    for seed in 0..seeds {
        for input_batch in [1, 3, 16] {
            for output_batch in [1, 4, 9] {
                for kernel in [
                    Kernel::Filter,
                    Kernel::Projection,
                    Kernel::Limit {
                        offset: [0, 1, 7, usize::MAX][seed % 4],
                        limit: [None, Some(0), Some(2), Some(usize::MAX)][seed / 4 % 4],
                    },
                ] {
                    for exit in [Exit::Complete, Exit::Stop, Exit::Error] {
                        check_case(seed, input_batch, output_batch, kernel, exit);
                    }
                }
            }
        }
    }
}

#[test]
fn transform_kernels_match_vector_oracles() {
    campaign(16);
}

#[test]
#[ignore = "deterministic local streaming transform campaign"]
fn transform_differential_campaign() {
    campaign(256);
    eprintln!("streaming transform campaign: 256 seeds, 20736 operator/batch/exit cases");
}

#[test]
fn source_errors_cancellation_and_admission_fail_closed() {
    for kernel in [
        Kernel::Filter,
        Kernel::Projection,
        Kernel::Limit {
            offset: 0,
            limit: None,
        },
    ] {
        let mut source = Source::new(rows(12, 1), 3);
        source.fail_at = Some(1);
        let mut output = Vec::new();
        let error = with_context(2, 8192, |context| {
            kernel.execute(&mut source, context, None, &mut |batch| {
                output.extend(batch);
                Ok(BatchControl::Continue)
            })
        })
        .unwrap_err();
        assert!(error.to_string().contains("source failure"));
        assert_eq!(source.calls, 1);
        assert!(!output.is_empty());

        let cancellation = RuntimeCancellationToken::new();
        assert!(cancellation.cancel());
        let task = RuntimeTaskContext::without_deadline(cancellation);
        let mut source = Source::new(rows(3, 1), 1);
        source.task = Some(&task);
        let error = with_context(2, 8192, |context| {
            kernel.execute(&mut source, context, None, &mut |_| {
                panic!("cancelled output")
            })
        })
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("runtime task stopped: cancelled"));
        assert_eq!(source.calls, 0);

        let mut source = Source::new(rows(3, 1), 3);
        let error = with_context(2, 1, |context| {
            kernel.execute(&mut source, context, None, &mut |_| {
                panic!("over-budget output")
            })
        })
        .unwrap_err();
        assert!(error.to_string().contains("memory"), "{error}");
    }
}

#[test]
fn predicate_and_projection_failures_release_partial_batches() {
    let mut source = Source::new(rows(3, 1), 3);
    let mut evaluated = 0;
    let error = with_context(3, 8192, |context| {
        stream_filter_batches(
            &PhysicalPlan::EmptyExec,
            &mut source,
            context,
            ExecutionLimit::unlimited(),
            &mut |_| {
                evaluated += 1;
                if evaluated == 2 {
                    Err(SkeinError::Execution("predicate failure".into()))
                } else {
                    Ok(true)
                }
            },
            &mut |_| panic!("partial predicate output"),
        )
    })
    .unwrap_err();
    assert!(error.to_string().contains("predicate failure"));
    assert_eq!(evaluated, 2);

    let mut source = Source::new(rows(3, 1), 3);
    source.rows[1].values.remove("value");
    let error = with_context(3, 8192, |context| {
        stream_projection_batches(
            &items("value"),
            &PhysicalPlan::EmptyExec,
            &mut source,
            context,
            ExecutionLimit::unlimited(),
            &mut |_| panic!("partial projection output"),
        )
    })
    .unwrap_err();
    assert!(error.to_string().contains("missing column 'value'"));
}

#[test]
fn pipeline_and_blocking_paths_share_context_and_source_identity() {
    with_context(2, 8192, |context| {
        let old: crate::blocking::BlockingExecutionContext<'_> = context;
        let new: BatchExecutionContext<'_> = old;
        assert!(std::ptr::eq(new.memory_ledger, context.memory_ledger));
        let mut source = Source::new(Vec::new(), 1);
        let old: &mut dyn crate::blocking::BindingBatchSource = &mut source;
        let new: &mut dyn BindingBatchSource = old;
        assert_eq!(
            new.execute(
                &PhysicalPlan::EmptyExec,
                ExecutionLimit::unlimited(),
                &mut |_| panic!("empty output")
            )
            .unwrap(),
            BatchControl::Continue
        );
    });
}
