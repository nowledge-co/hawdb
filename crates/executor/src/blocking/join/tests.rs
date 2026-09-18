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

use super::*;
use crate::blocking::hash_oracle::{values, Fixture};
use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
use hawdb_storage::{NodeId, NodeRecord};
use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};

struct Source {
    left: Vec<Binding>,
    right: Vec<Binding>,
    batch_rows: usize,
    cancel_build: Option<RuntimeCancellationToken>,
}

impl BindingBatchSource for Source {
    fn execute(
        &mut self,
        input: &PhysicalPlan,
        limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        assert_eq!(limit, ExecutionLimit::unlimited());
        let PhysicalPlan::SeqNodeScan { variable, .. } = input else {
            panic!("expected scan")
        };
        let rows = if variable == "a" {
            &self.left
        } else {
            &self.right
        };
        for batch in rows.chunks(self.batch_rows) {
            if emit(batch.to_vec())? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
        }
        if variable == "b"
            && let Some(token) = &self.cancel_build
        {
            token.cancel();
        }
        Ok(BatchControl::Continue)
    }
}

fn plan() -> PhysicalPlan {
    PhysicalPlan::HashJoinExec {
        left_key: HashJoinKey {
            variable: "a".into(),
            property: "key".into(),
        },
        right_key: HashJoinKey {
            variable: "b".into(),
            property: "key".into(),
        },
        left: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "a".into(),
            label: "Left".into(),
        }),
        right: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "b".into(),
            label: "Right".into(),
        }),
    }
}

fn row(variable: &str, id: usize, key: Option<Value>, padding: usize) -> Binding {
    let mut properties = BTreeMap::from([("payload".into(), Value::String("x".repeat(padding)))]);
    if let Some(key) = key {
        properties.insert("key".into(), key);
    }
    Binding {
        values: BTreeMap::new(),
        nodes: BTreeMap::from([(
            variable.into(),
            NodeRecord {
                id: NodeId(id as u64),
                labels: BTreeSet::new(),
                properties,
            },
        )]),
        relationships: BTreeMap::new(),
    }
}

fn key_values() -> Vec<Value> {
    let mut domain = values();
    domain.push(Value::String("unicode-\u{03bb}-\u{1f980}".into()));
    domain
}

fn source(seed: usize, count: usize, padding: usize) -> Source {
    let domain = key_values();
    let rows = |variable: &str, offset: usize| {
        (0..count)
            .map(|i| {
                let key =
                    (i % 23 != 0).then(|| domain[(i * 7 + seed + offset) % domain.len()].clone());
                row(variable, i, key, padding)
            })
            .collect()
    };
    Source {
        left: rows("a", 3),
        right: rows("b", 0),
        batch_rows: seed % 17 + 1,
        cancel_build: None,
    }
}

fn oracle(source: &Source) -> Vec<(u64, u64)> {
    let mut expected = Vec::new();
    for left in &source.left {
        for right in &source.right {
            let a = &left.nodes["a"];
            let b = &right.nodes["b"];
            match (a.properties.get("key"), b.properties.get("key")) {
                (Some(a_key), Some(b_key))
                    if a_key != &Value::Null && b_key != &Value::Null && a_key == b_key =>
                {
                    expected.push((a.id.0, b.id.0));
                }
                _ => {}
            }
        }
    }
    expected.sort_unstable();
    expected
}

fn run(fixture: &Fixture, source: &mut Source) -> (Vec<(u64, u64)>, AdmittedHashJoinWork) {
    let mut output = Vec::new();
    let (_, work) = execute_hash_join(
        &plan(),
        source,
        fixture.context(),
        ExecutionLimit::unlimited(),
        &mut |batch| {
            output.extend(
                batch
                    .iter()
                    .map(|binding| (binding.nodes["a"].id.0, binding.nodes["b"].id.0)),
            );
            Ok(BatchControl::Continue)
        },
    )
    .unwrap();
    output.sort_unstable();
    (output, work)
}

#[test]
fn graph_hash_join_matches_product_oracle_in_memory_and_spill() {
    for seed in 0..16 {
        for budget in [128 * 1024, 8 * 1024 * 1024] {
            let fixture = Fixture::new(budget);
            let mut source = source(seed, 192, 256);
            let expected = oracle(&source);
            let (actual, work) = run(&fixture, &mut source);
            assert_eq!(actual, expected, "seed={seed} budget={budget}");
            assert_eq!(
                work.candidate_rows,
                expected.len(),
                "only equal hashes become candidates"
            );
            fixture.assert_report(budget == 128 * 1024);
        }
    }
}

#[test]
fn graph_hash_join_repartitions_distinct_keys_without_product_work() {
    let fixture = Fixture::new(128 * 1024);
    let mut source = Source {
        left: (0..512)
            .map(|i| row("a", i, Some(Value::Int(i as i64)), 256))
            .collect(),
        right: (0..512)
            .map(|i| row("b", i, Some(Value::Int(i as i64)), 256))
            .collect(),
        batch_rows: 13,
        cancel_build: None,
    };
    let (actual, work) = run(&fixture, &mut source);
    assert_eq!(actual, (0..512).map(|i| (i, i)).collect::<Vec<_>>());
    assert_eq!(work.candidate_rows, 512);
    assert!(work.repartitions > 0);
    assert!(work.replay_rows < 512 * 64, "{work:?}");
    fixture.assert_report(true);
}

#[test]
fn graph_hash_join_hot_key_replays_probe_per_build_chunk() {
    let fixture = Fixture::new(128 * 1024);
    let mut source = Source {
        left: (0..7)
            .map(|i| row("a", i, Some(Value::Int(1)), 128))
            .collect(),
        right: (0..512)
            .map(|i| row("b", i, Some(Value::Int(1)), 128))
            .collect(),
        batch_rows: 11,
        cancel_build: None,
    };
    let expected = oracle(&source);
    let (actual, work) = run(&fixture, &mut source);
    assert_eq!(actual, expected);
    assert_eq!(work.repartitions, 0);
    assert!(work.replay_rows < 1024, "{work:?}");
    fixture.assert_report(true);
}

#[test]
fn graph_hash_join_limit_stop_error_cancel_and_panic_release_ownership() {
    for budget in [128 * 1024, 8 * 1024 * 1024] {
        for mode in 0..6 {
            let fixture = Fixture::new(budget);
            let mut source = source(3, 192, 256);
            let token = RuntimeCancellationToken::new();
            let task = RuntimeTaskContext::without_deadline(token.clone());
            let mut context = fixture.context();
            context.task_context = Some(&task);
            if mode == 4 {
                source.cancel_build = Some(token.clone());
            }
            let mut emitted = 0;
            let result = catch_unwind(AssertUnwindSafe(|| {
                execute_hash_join(
                    &plan(),
                    &mut source,
                    context,
                    if mode == 0 {
                        ExecutionLimit {
                            output_rows: Some(3),
                        }
                    } else {
                        ExecutionLimit::unlimited()
                    },
                    &mut |batch| {
                        emitted += batch.len();
                        match mode {
                            1 => Ok(BatchControl::Stop),
                            2 => Err(HawDBError::Execution("consumer error".into())),
                            3 => {
                                token.cancel();
                                Ok(BatchControl::Continue)
                            }
                            5 => panic!("consumer panic"),
                            _ => Ok(BatchControl::Continue),
                        }
                    },
                )
            }));
            match mode {
                0 => {
                    assert!(result.unwrap().is_ok());
                    assert_eq!(emitted, 3);
                }
                1 => {
                    assert!(result.unwrap().is_ok());
                    assert_eq!(emitted, 7);
                }
                2..=4 => assert!(result.unwrap().is_err()),
                5 => assert!(result.is_err()),
                _ => unreachable!(),
            }
            fixture.assert_released();
        }
    }
}

#[test]
fn graph_hash_join_empty_inputs_and_zero_limit_emit_nothing() {
    for side in 0..3 {
        let fixture = Fixture::new(128 * 1024);
        let mut source = source(0, 8, 0);
        if side == 0 {
            source.left.clear();
        }
        if side == 1 {
            source.right.clear();
        }
        let result = execute_hash_join(
            &plan(),
            &mut source,
            fixture.context(),
            ExecutionLimit {
                output_rows: (side == 2).then_some(0),
            },
            &mut |_| panic!("empty join emitted output"),
        );
        assert!(result.is_ok());
        fixture.assert_released();
    }
}

#[test]
fn graph_hash_join_budget_failures_release_all_runs_and_reservations() {
    for mode in 0..4 {
        let fixture = Fixture::new(128 * 1024);
        let mut memory = fixture.context().memory.clone();
        match mode {
            0 => memory.max_spill_bytes = std::num::NonZeroU64::MIN,
            1 => memory.max_spill_runs = NonZeroUsize::MIN,
            2 => memory.query_memory_bytes = NonZeroUsize::new(4096).unwrap(),
            3 => memory.batch_payload_bytes = NonZeroUsize::MIN,
            _ => unreachable!(),
        }
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let mut context = fixture.context();
        context.memory = &memory;
        context.memory_ledger = &ledger;
        let mut source = source(7, 192, 256);
        let result = execute_hash_join(
            &plan(),
            &mut source,
            context,
            ExecutionLimit::unlimited(),
            &mut |_| panic!("budget failure must precede output"),
        );
        assert!(result.is_err(), "mode={mode}");
        assert_eq!(ledger.snapshot().used_bytes, 0);
        fixture.assert_released();
    }
}

#[test]
#[ignore = "local seeded graph hash-join differential campaign"]
fn graph_hash_join_differential_campaign() {
    for seed in 0..1024 {
        let fixture = Fixture::new(128 * 1024);
        let count = 32 + seed % 257;
        let mut source = source(seed, count, (seed % 4) * 97);
        source.left.truncate((seed * 17) % (count + 1));
        if seed % 3 == 0 {
            source.right.reverse();
        }
        if seed % 7 == 0 {
            source.right.clear();
        }
        let expected = oracle(&source);
        let (actual, _) = run(&fixture, &mut source);
        assert_eq!(actual, expected, "seed={seed}");
        fixture.assert_released();
    }
}
