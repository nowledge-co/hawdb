//! Seeded differential checks against independent ordered-container oracles.

use super::*;
use skein_core::RuntimeCancellationToken;
use skein_plan::ProjectionExpression;
use skein_storage::{NodeId, NodeRecord};
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

#[derive(Default)]
struct Reports(RefCell<Vec<BlockingOperatorMemoryReport>>);

impl ExecutionObserver for Reports {
    fn record_blocking_memory_report(&self, report: BlockingOperatorMemoryReport) {
        self.0.borrow_mut().push(report);
    }
}

struct Source {
    rows: Vec<Binding>,
    batch_rows: usize,
    cancel_after_input: Option<RuntimeCancellationToken>,
}

impl BindingBatchSource for Source {
    fn execute(
        &mut self,
        _: &PhysicalPlan,
        _: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        for batch in self.rows.chunks(self.batch_rows) {
            if emit(batch.to_vec())? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
        }
        if let Some(token) = &self.cancel_after_input {
            token.cancel();
        }
        Ok(BatchControl::Continue)
    }
}

struct Fixture {
    memory: ExecutionMemoryConfig,
    ledger: QueryMemoryLedger,
    reports: Reports,
    catalog: Catalog,
}

impl Fixture {
    fn new(budget: usize) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let memory = ExecutionMemoryConfig {
            blocking_operator_bytes: NonZeroUsize::new(budget).unwrap(),
            batch_rows: NonZeroUsize::new(7).unwrap(),
            min_spill_free_bytes: NonZeroU64::MIN,
            spill_directory: std::env::temp_dir().join(format!(
                "skein-hash-oracle-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, AtomicOrdering::Relaxed)
            )),
            ..ExecutionMemoryConfig::default()
        };
        Self {
            ledger: QueryMemoryLedger::new(memory.query_memory_bytes),
            memory,
            reports: Reports::default(),
            catalog: Catalog::default(),
        }
    }

    fn context(&self) -> BlockingExecutionContext<'_> {
        BlockingExecutionContext {
            catalog: &self.catalog,
            memory: &self.memory,
            memory_ledger: &self.ledger,
            task_context: None,
            observer: &self.reports,
        }
    }

    fn assert_released(&self) {
        let ledger = self.ledger.snapshot();
        assert_eq!(ledger.used_bytes, 0);
        assert!(ledger.peak_bytes <= ledger.budget_bytes);
        let spill = self.memory.spill_pool_snapshot().unwrap();
        assert_eq!(spill.active_runs, 0);
        assert_eq!(spill.active_bytes, 0);
        assert_eq!(spill.pending_write_bytes, 0);
    }

    fn assert_report(&self, spilled: bool) {
        let reports = self.reports.0.borrow();
        let report = reports.last().expect("operator memory report");
        assert_eq!(report.spill_run_count > 0, spilled);
        assert!(report.peak_tracked_bytes <= report.budget_bytes);
        self.assert_released();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // A nonempty directory indicates leaked runs and must not be hidden by
        // recursive cleanup. The pool can remain uninitialized on early errors.
        if self.memory.spill_directory.exists() {
            std::fs::remove_dir(&self.memory.spill_directory).unwrap();
        }
    }
}

fn input() -> PhysicalPlan {
    PhysicalPlan::SeqNodeScan {
        variable: "n".into(),
        label: String::new(),
    }
}

fn values() -> Vec<Value> {
    vec![
        Value::Null,
        Value::Bool(false),
        Value::Bool(true),
        Value::Int(-1),
        Value::Int(0),
        Value::Int(1),
        Value::Float(-0.0),
        Value::Float(0.0),
        Value::Float(1.0),
        Value::Float(f64::NEG_INFINITY),
        Value::Float(f64::INFINITY),
        Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
        Value::Float(f64::from_bits(0x7ff8_0000_0000_0002)),
        Value::String(String::new()),
        Value::String("shared-prefix-shared-prefix-value".into()),
        Value::Binary(vec![0, 1, 255]),
        Value::Uuid("00112233-4455-6677-8899-aabbccddeeff".parse().unwrap()),
        Value::List(vec![Value::Null, Value::Int(1)]),
        Value::Map(BTreeMap::from([(
            "nested".into(),
            Value::List(vec![Value::Float(-0.0)]),
        )])),
    ]
}

fn next(state: &mut u64) -> usize {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
    (*state >> 32) as usize
}

fn rows(seed: u64) -> Vec<Binding> {
    let domain = values();
    let mut state = seed;
    (0..152)
        .map(|index| {
            let properties = BTreeMap::from([
                ("group".into(), domain[index % domain.len()].clone()),
                ("partition".into(), Value::Int((index % 2) as i64)),
                (
                    "value".into(),
                    domain[next(&mut state) % domain.len()].clone(),
                ),
            ]);
            Binding {
                values: BTreeMap::new(),
                nodes: BTreeMap::from([(
                    "n".into(),
                    NodeRecord {
                        id: NodeId(index as u64),
                        labels: BTreeSet::new(),
                        properties,
                    },
                )]),
                relationships: BTreeMap::new(),
            }
        })
        .collect()
}

fn group_keys() -> Vec<Projection> {
    ["group", "partition"]
        .into_iter()
        .map(|name| Projection {
            expression: ProjectionExpression::Property {
                variable: "n".into(),
                property: name.into(),
            },
            name: name.into(),
        })
        .collect()
}

fn aggregations(buffered: bool) -> Vec<Aggregation> {
    let functions = if buffered {
        vec![
            (AggregateFunction::Count, true, "unique_count"),
            (AggregateFunction::Collect, false, "collected"),
            (AggregateFunction::Collect, true, "unique_values"),
        ]
    } else {
        vec![
            (AggregateFunction::Count, false, "count"),
            (AggregateFunction::Min, false, "min"),
            (AggregateFunction::Max, false, "max"),
            (AggregateFunction::Avg, false, "avg"),
        ]
    };
    functions
        .into_iter()
        .map(|(function, distinct, name)| Aggregation {
            function,
            target: AggregateTarget::Property {
                variable: "n".into(),
                property: "value".into(),
            },
            distinct,
            name: name.into(),
        })
        .collect()
}

fn aggregate_oracle(rows: &[Binding], buffered: bool) -> Vec<Binding> {
    let mut groups = BTreeMap::<Vec<Value>, Vec<Value>>::new();
    for row in rows {
        let row = &row.nodes["n"].properties;
        let values = groups
            .entry(vec![row["group"].clone(), row["partition"].clone()])
            .or_default();
        if row["value"] != Value::Null {
            values.push(row["value"].clone());
        }
    }
    groups
        .into_iter()
        .map(|(key, values)| {
            let mut output = BTreeMap::from([
                ("group".into(), key[0].clone()),
                ("partition".into(), key[1].clone()),
            ]);
            if buffered {
                let distinct = values.iter().cloned().collect::<BTreeSet<_>>();
                output.insert("unique_count".into(), Value::Int(distinct.len() as i64));
                output.insert("collected".into(), Value::List(values));
                output.insert(
                    "unique_values".into(),
                    Value::List(distinct.into_iter().collect()),
                );
            } else {
                let numbers = values
                    .iter()
                    .filter_map(|value| match value {
                        Value::Int(value) => Some(*value as f64),
                        Value::Float(value) if value.is_finite() => Some(*value),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                output.insert("count".into(), Value::Int(values.len() as i64));
                output.insert(
                    "min".into(),
                    values.iter().min().cloned().unwrap_or(Value::Null),
                );
                output.insert(
                    "max".into(),
                    values.iter().max().cloned().unwrap_or(Value::Null),
                );
                output.insert(
                    "avg".into(),
                    if numbers.is_empty() {
                        Value::Null
                    } else {
                        Value::Float(
                            numbers.iter().fold(0.0, |sum, value| sum + value)
                                / numbers.len() as f64,
                        )
                    },
                );
            }
            Binding::values(output)
        })
        .collect()
}

#[test]
fn partial_spill_workload_keeps_the_existing_run_budget() {
    const ROWS: usize = 4096;
    const GROUPS: usize = 256;
    for distinct in [false, true] {
        let mut fixture = Fixture::new(16 * 1024);
        fixture.memory.max_spill_runs = NonZeroUsize::new(128).unwrap();
        fixture.memory.max_total_spill_runs = NonZeroUsize::new(256).unwrap();
        fixture.memory.max_spill_bytes = NonZeroU64::new(64 * 1024 * 1024).unwrap();
        fixture.memory.max_total_spill_bytes = NonZeroU64::new(128 * 1024 * 1024).unwrap();
        let mut source = Source {
            rows: (0..ROWS)
                .map(|index| Binding {
                    nodes: BTreeMap::from([(
                        "n".into(),
                        NodeRecord {
                            id: NodeId(index as u64),
                            labels: BTreeSet::new(),
                            properties: BTreeMap::from([
                                ("group".into(), Value::Int((index % GROUPS) as i64)),
                                ("value".into(), Value::Int(index as i64)),
                            ]),
                        },
                    )]),
                    values: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })
                .collect(),
            batch_rows: 1024,
            cancel_after_input: None,
        };
        let mut output = Vec::new();
        stream_aggregate_batches(
            &input(),
            &group_keys()[..1],
            &[Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::Property {
                    variable: "n".into(),
                    property: "value".into(),
                },
                distinct,
                name: "count".into(),
            }],
            &mut source,
            fixture.context(),
            ExecutionLimit::unlimited(),
            &mut |batch| {
                output.extend(batch);
                Ok(BatchControl::Continue)
            },
        )
        .unwrap_or_else(|error| panic!("distinct={distinct}: {error}"));
        assert_eq!(output.len(), GROUPS);
        for (group, row) in output.iter().enumerate() {
            assert_eq!(row.values["group"], Value::Int(group as i64));
            assert_eq!(row.values["count"], Value::Int((ROWS / GROUPS) as i64));
        }
        fixture.assert_report(true);
        assert!(fixture.reports.0.borrow()[0].spill_run_count <= 128);
    }
}

#[test]
fn seeded_hash_aggregation_matches_ordered_oracle_with_and_without_spill() {
    for seed in 0..16 {
        let rows = rows(seed);
        for buffered in [false, true] {
            let expected = aggregate_oracle(&rows, buffered);
            for budget in [8 * 1024, 1024 * 1024] {
                let fixture = Fixture::new(budget);
                let mut source = Source {
                    rows: rows.clone(),
                    batch_rows: seed as usize % 17 + 1,
                    cancel_after_input: None,
                };
                let mut output = Vec::new();
                stream_aggregate_batches(
                    &input(),
                    &group_keys(),
                    &aggregations(buffered),
                    &mut source,
                    fixture.context(),
                    ExecutionLimit::unlimited(),
                    &mut |batch| {
                        output.extend(batch);
                        Ok(BatchControl::Continue)
                    },
                )
                .unwrap_or_else(|error| {
                    panic!("seed={seed}, buffered={buffered}, budget={budget}: {error}")
                });
                assert_eq!(
                    output, expected,
                    "seed={seed}, buffered={buffered}, budget={budget}"
                );
                fixture.assert_report(budget == 8 * 1024);
            }
        }
    }
}

#[test]
fn seeded_hash_distinct_preserves_schema_and_order_across_spills() {
    for seed in 0..16 {
        let mut state = seed;
        let mut rows = rows(seed);
        for row in &mut rows {
            *row = Binding::values(std::mem::take(
                &mut row.nodes.get_mut("n").unwrap().properties,
            ));
            if next(&mut state).is_multiple_of(3) {
                let value = row.values.remove("value").unwrap();
                row.values.insert("other".into(), value);
            }
        }
        rows.extend(rows.clone());
        let mut seen = BTreeSet::new();
        let first_seen = rows
            .iter()
            .filter(|row| seen.insert(row.values.clone()))
            .cloned()
            .collect::<Vec<_>>();
        let mut schemas = Vec::new();
        for row in &rows {
            let names = row.values.keys().cloned().collect::<Vec<_>>();
            if !schemas.contains(&names) {
                schemas.push(names);
            }
        }
        let mut spill_order = first_seen.clone();
        spill_order.sort_by_key(|row| {
            (
                schemas
                    .iter()
                    .position(|schema| schema.iter().eq(row.values.keys()))
                    .unwrap(),
                row.values.values().cloned().collect::<Vec<_>>(),
            )
        });
        for budget in [8 * 1024, 1024 * 1024] {
            let fixture = Fixture::new(budget);
            let mut source = Source {
                rows: rows.clone(),
                batch_rows: 13,
                cancel_after_input: None,
            };
            let mut output = Vec::new();
            stream_distinct_batches(
                &input(),
                &mut source,
                fixture.context(),
                ExecutionLimit::unlimited(),
                &mut |batch| {
                    output.extend(batch);
                    Ok(BatchControl::Continue)
                },
            )
            .unwrap_or_else(|error| panic!("seed={seed}, budget={budget}: {error}"));
            assert_eq!(
                output,
                if budget == 8 * 1024 {
                    &spill_order
                } else {
                    &first_seen
                }
                .clone(),
                "seed={seed}, budget={budget}"
            );
            fixture.assert_report(budget == 8 * 1024);
        }
    }
}

#[test]
fn hash_operators_release_memory_and_runs_on_stop_error_and_cancellation() {
    for kind in 0..3 {
        for budget in [8 * 1024, 1024 * 1024] {
            for exit in 0..3 {
                let fixture = Fixture::new(budget);
                let token = RuntimeCancellationToken::new();
                let task = RuntimeTaskContext::without_deadline(token.clone());
                let rows = if kind == 2 {
                    rows(7)
                        .into_iter()
                        .map(|mut row| Binding::values(row.nodes.remove("n").unwrap().properties))
                        .collect()
                } else {
                    rows(7)
                };
                let mut source = Source {
                    rows,
                    batch_rows: 11,
                    cancel_after_input: (exit == 2).then_some(token),
                };
                let mut context = fixture.context();
                context.task_context = Some(&task);
                let mut emit = |_| {
                    if exit == 1 {
                        Err(SkeinError::Execution("oracle consumer failure".into()))
                    } else {
                        Ok(BatchControl::Stop)
                    }
                };
                let result = if kind == 2 {
                    stream_distinct_batches(
                        &input(),
                        &mut source,
                        context,
                        ExecutionLimit::unlimited(),
                        &mut emit,
                    )
                } else {
                    stream_aggregate_batches(
                        &input(),
                        &group_keys(),
                        &aggregations(kind == 1),
                        &mut source,
                        context,
                        ExecutionLimit::unlimited(),
                        &mut emit,
                    )
                };
                if exit == 0 {
                    assert_eq!(result.unwrap(), BatchControl::Stop);
                } else if exit == 1 {
                    assert!(result
                        .unwrap_err()
                        .to_string()
                        .contains("oracle consumer failure"));
                } else {
                    assert!(result.is_err(), "cancelled kind={kind}, budget={budget}");
                }
                fixture.assert_released();
            }
        }
    }
}

#[test]
fn hash_operators_fail_closed_on_oversized_state_and_release_admission() {
    for kind in 0..3 {
        let fixture = Fixture::new(8 * 1024);
        let value = Value::String("oversized".repeat(2048));
        let row = if kind == 2 {
            Binding::scalar("value", value)
        } else {
            let mut row = rows(0).remove(0);
            row.nodes
                .get_mut("n")
                .unwrap()
                .properties
                .insert("value".into(), value);
            row
        };
        let mut source = Source {
            rows: vec![row],
            batch_rows: 1,
            cancel_after_input: None,
        };
        let mut emitted = false;
        let mut emit = |_| {
            emitted = true;
            Ok(BatchControl::Continue)
        };
        let result = if kind == 2 {
            stream_distinct_batches(
                &input(),
                &mut source,
                fixture.context(),
                ExecutionLimit::unlimited(),
                &mut emit,
            )
        } else {
            stream_aggregate_batches(
                &input(),
                &group_keys(),
                &aggregations(kind == 1),
                &mut source,
                fixture.context(),
                ExecutionLimit::unlimited(),
                &mut emit,
            )
        };
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("blocking_operator_bytes") || error.contains("bounded merge allowance"),
            "{error}"
        );
        assert!(!emitted);
        fixture.assert_released();
    }
}
