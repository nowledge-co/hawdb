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

use super::super::projection::{DistinctBatchSource, RelationalBlockingObserver};
use super::*;
use hawdb_executor::binding::binding_memory_bytes;
use std::path::PathBuf;

struct Directory(PathBuf);

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn memory(spilled: bool) -> (hawdb_executor::ExecutionMemoryConfig, Directory) {
    let directory = Directory(constrained_hash_join_memory().spill_directory);
    std::fs::create_dir_all(&directory.0).unwrap();
    let memory = hawdb_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(if spilled { 1024 } else { 16384 }).unwrap(),
        query_memory_bytes: NonZeroUsize::new(65536).unwrap(),
        batch_rows: NonZeroUsize::new(8).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(512).unwrap(),
        min_spill_free_bytes: NonZeroU64::MIN,
        spill_directory: directory.0.clone(),
        ..Default::default()
    };
    (memory, directory)
}

struct Rows(Vec<ExecutorBinding>);

impl BindingBatchSource<()> for Rows {
    fn execute(
        &mut self,
        _: &(),
        _: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        emit(std::mem::take(&mut self.0))
    }
}

#[test]
fn resident_distinct_batch_source_obeys_the_kernel_payload_budget() {
    assert_distinct_source_payload_is_bounded(false);
}

#[test]
fn spilled_distinct_batch_source_obeys_the_kernel_payload_budget() {
    assert_distinct_source_payload_is_bounded(true);
}

fn assert_distinct_source_payload_is_bounded(spilled: bool) {
    let (mut memory, _directory) = memory(spilled);
    memory.batch_payload_bytes = NonZeroUsize::new(200).unwrap();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let catalog = Catalog::default();
    let observer = RelationalBlockingObserver::default();
    let expected = (0..4)
        .map(|value| ExecutorBinding::scalar("value", Value::Int(value)))
        .collect::<Vec<_>>();
    let mut input = Rows([expected.clone(), expected.clone()].concat());
    let mut source = DistinctBatchSource {
        input: &mut input,
        input_plan: &(),
        catalog: &catalog,
        memory: &memory,
        memory_ledger: &ledger,
        task_context: None,
        observer: &observer,
    };
    let mut actual = Vec::new();
    source
        .execute(&(), ExecutionLimit::unlimited(), &mut |batch| {
            let bytes = batch.iter().map(binding_memory_bytes).sum::<usize>();
            assert!(
                bytes <= memory.batch_payload_bytes.get(),
                "SQL distinct batch uses {bytes} bytes"
            );
            actual.extend(batch);
            Ok(BatchControl::Continue)
        })
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(observer.reports.borrow()[0].spill_run_count > 0, spilled);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

fn state() -> RelationalState {
    let mut state = RelationalState::default();
    let ddl = compile_relational_statement_sql(
        "CREATE TABLE distinct_records (id BIGINT PRIMARY KEY, n BIGINT)",
        &[],
        &state,
    )
    .unwrap();
    state = state
        .stage_transaction(ddl, Default::default(), Default::default())
        .unwrap();
    for (id, value) in [
        Some(2),
        Some(1),
        Some(2),
        Some(4),
        Some(3),
        Some(1),
        Some(0),
        Some(4),
        None,
        None,
    ]
    .into_iter()
    .enumerate()
    {
        let insert = compile_relational_statement_sql(
            "INSERT INTO distinct_records (id, n) VALUES ($1, $2)",
            &[Value::Int(id as i64), value.map_or(Value::Null, Value::Int)],
            &state,
        )
        .unwrap();
        state = state
            .stage_transaction(insert, Default::default(), Default::default())
            .unwrap();
    }
    state
}

fn read_modes() -> RelationalQueryReadModes<'static> {
    RelationalQueryReadModes::new(
        RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
        RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
    )
}

#[test]
fn sql_distinct_preserves_results_limits_and_order_through_spill() {
    let state = state();
    for suffix in [
        "",
        " ORDER BY n DESC",
        " ORDER BY n DESC LIMIT 2 OFFSET 1",
        " LIMIT 0",
    ] {
        let sql = format!("SELECT DISTINCT n FROM distinct_records{suffix}");
        let (reference_memory, _reference_directory) = memory(false);
        let reference = execute_relational_query_sql_with_runtime(
            &sql,
            &[],
            &state,
            read_modes(),
            batched_index_join_limits(),
            &reference_memory,
            None,
        )
        .unwrap();
        let mut expected = reference
            .rows
            .iter()
            .map(|row| row["n"].clone())
            .collect::<Vec<_>>();
        if suffix.is_empty() {
            expected.sort();
            assert_eq!(
                expected,
                [vec![Value::Null], (0..5).map(Value::Int).collect()].concat()
            );
        }
        for spilled in [false, true] {
            let (memory, directory) = memory(spilled);
            let output = execute_relational_query_sql_with_runtime(
                &sql,
                &[],
                &state,
                read_modes(),
                batched_index_join_limits(),
                &memory,
                None,
            )
            .unwrap_or_else(|error| panic!("{sql}, spilled={spilled}: {error}"));
            let mut actual = output
                .rows
                .iter()
                .map(|row| row["n"].clone())
                .collect::<Vec<_>>();
            if suffix.is_empty() {
                actual.sort();
            }
            assert_eq!(actual, expected, "{sql}, spilled={spilled}");
            if suffix != " LIMIT 0" {
                let report = output
                    .blocking_operator_memory_reports
                    .iter()
                    .find(|report| report.operator == "DistinctExec")
                    .unwrap();
                assert_eq!(report.spill_run_count > 0, spilled);
            }
            assert!(std::fs::read_dir(&directory.0).unwrap().all(|entry| {
                !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".spill")
            }));
        }
    }
}
