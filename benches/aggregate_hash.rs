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

//! Run the identical benchmark on the base and candidate revisions.
//! Input construction is excluded; ordered output and admission are checked.

use hawdb::optimizer::PhysicalPlan;
use hawdb::planner::{
    AggregateFunction, AggregateTarget, Aggregation, Projection, ProjectionExpression,
};
use hawdb_core::{Catalog, Result, Value};
use hawdb_executor::binding::Binding;
use hawdb_executor::blocking::{
    stream_aggregate_batches, BindingBatchSource, BlockingExecutionContext,
};
use hawdb_executor::observer::ExecutionObserver;
use hawdb_executor::pipeline::{BatchControl, BindingBatch};
use hawdb_executor::{
    BlockingOperatorMemoryReport, ExecutionLimit, ExecutionMemoryConfig, QueryMemoryLedger,
};
use hawdb_storage::{NodeId, NodeRecord};
use serde_json::json;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::time::Instant;

const ROWS: usize = 262_144;
const GROUPS: usize = 32_768;
const SAMPLES: usize = 7;

struct Source(Vec<BindingBatch>);

impl BindingBatchSource for Source {
    fn execute(
        &mut self,
        _: &PhysicalPlan,
        _: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        for batch in std::mem::take(&mut self.0) {
            if emit(batch)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    }
}

#[derive(Default)]
struct Report(RefCell<Option<BlockingOperatorMemoryReport>>);

impl ExecutionObserver for Report {
    fn record_blocking_memory_report(&self, report: BlockingOperatorMemoryReport) {
        *self.0.borrow_mut() = Some(report);
    }
}

fn key(group: usize, composite: bool) -> Value {
    if composite {
        Value::List(vec![
            Value::String("common-prefix-for-high-cardinality-grouping".repeat(2)),
            Value::Int(group as i64),
        ])
    } else {
        Value::Int(group as i64)
    }
}

fn source(composite: bool) -> Source {
    let mut batches = Vec::new();
    for start in (0..ROWS).step_by(512) {
        batches.push(
            (start..start + 512)
                .map(|row| Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(
                        "n".into(),
                        NodeRecord {
                            id: NodeId(row as u64),
                            labels: BTreeSet::new(),
                            properties: BTreeMap::from([(
                                "group".into(),
                                key(row.wrapping_mul(104_729) % GROUPS, composite),
                            )]),
                        },
                    )]),
                    relationships: BTreeMap::new(),
                })
                .collect(),
        );
    }
    Source(batches)
}

fn main() {
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(128 * 1024 * 1024).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let catalog = Catalog::default();
    let input = PhysicalPlan::SeqNodeScan {
        variable: "n".into(),
        label: String::new(),
    };
    let groups = [Projection {
        expression: ProjectionExpression::Property {
            variable: "n".into(),
            property: "group".into(),
        },
        name: "group".into(),
    }];
    let aggregates = [Aggregation {
        function: AggregateFunction::Count,
        target: AggregateTarget::All,
        distinct: false,
        name: "count".into(),
    }];

    for composite in [false, true] {
        let expected = (0..GROUPS)
            .map(|group| {
                Binding::values(BTreeMap::from([
                    ("group".into(), key(group, composite)),
                    ("count".into(), Value::Int((ROWS / GROUPS) as i64)),
                ]))
            })
            .collect::<Vec<_>>();
        let mut samples = Vec::new();
        let mut peak_bytes = 0;
        for sample in 0..=SAMPLES {
            let mut source = source(composite);
            let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
            let report = Report::default();
            let mut output = Vec::with_capacity(GROUPS);
            let started = Instant::now();
            stream_aggregate_batches(
                &input,
                &groups,
                &aggregates,
                &mut source,
                BlockingExecutionContext {
                    catalog: &catalog,
                    memory: &memory,
                    memory_ledger: &ledger,
                    task_context: None,
                    observer: &report,
                },
                ExecutionLimit::unlimited(),
                &mut |batch| {
                    output.extend(batch);
                    Ok(BatchControl::Continue)
                },
            )
            .expect("high-cardinality grouping");
            let elapsed = started.elapsed().as_nanos();
            assert_eq!(black_box(&output), &expected);
            let report = report.0.into_inner().expect("memory report");
            assert_eq!(report.spill_run_count, 0);
            assert!(report.peak_tracked_bytes <= report.budget_bytes);
            assert_eq!(ledger.snapshot().used_bytes, 0);
            peak_bytes = peak_bytes.max(report.peak_tracked_bytes);
            if sample > 0 {
                samples.push(elapsed);
            }
        }
        samples.sort_unstable();
        println!(
            "aggregate_hash {}",
            json!({
                "workload": if composite { "nested_shared_prefix" } else { "integer" },
                "input_rows": ROWS,
                "group_count": GROUPS,
                "samples": SAMPLES,
                "median_nanoseconds": samples[SAMPLES / 2],
                "min_nanoseconds": samples[0],
                "max_nanoseconds": samples[SAMPLES - 1],
                "peak_tracked_bytes": peak_bytes,
                "spill_run_count": 0,
                "ordered_output_verified": true,
            })
        );
    }
}
