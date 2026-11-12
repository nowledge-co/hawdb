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
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct ProjectionAllocator;

thread_local! {
    static PAYLOAD_THRESHOLD: Cell<usize> = const { Cell::new(0) };
    static PAYLOAD_BYTES: Cell<usize> = const { Cell::new(0) };
}

fn record_allocation(bytes: usize) {
    let _ = PAYLOAD_THRESHOLD.try_with(|threshold| {
        if threshold.get() != 0 && bytes >= threshold.get() {
            let _ = PAYLOAD_BYTES.try_with(|total| total.set(total.get().saturating_add(bytes)));
        }
    });
}

// SAFETY: All allocation and deallocation use the same unmodified System layout.
// The thread-local counters allocate no memory and cannot affect another test.
unsafe impl GlobalAlloc for ProjectionAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Forward the allocator's original layout unchanged.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout originate from this System allocator.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Forward the allocator's original layout unchanged.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: Forward the original pointer, layout and requested size.
        let pointer = unsafe { System.realloc(pointer, layout, size) };
        if !pointer.is_null() {
            record_allocation(size);
        }
        pointer
    }
}

#[global_allocator]
static ALLOCATOR: ProjectionAllocator = ProjectionAllocator;

struct PayloadProbe;

impl PayloadProbe {
    fn start(threshold: usize) -> Self {
        PAYLOAD_BYTES.set(0);
        PAYLOAD_THRESHOLD.set(threshold);
        Self
    }
}

impl Drop for PayloadProbe {
    fn drop(&mut self) {
        PAYLOAD_THRESHOLD.set(0);
    }
}

fn state() -> RelationalState {
    let mut state = RelationalState::default();
    for (sql, parameters) in [
        (
            "CREATE TABLE docs (id BIGINT PRIMARY KEY, body TEXT)",
            vec![],
        ),
        (
            "INSERT INTO docs (id, body) VALUES ($1, $2)",
            vec![Value::Int(1), Value::String("x".repeat(256 * 1024))],
        ),
    ] {
        let transaction = compile_relational_statement_sql(sql, &parameters, &state).unwrap();
        state = state
            .stage_transaction(transaction, Default::default(), Default::default())
            .unwrap();
    }
    state
}

#[test]
fn blocking_projection_refuses_before_repeated_payload_copies() {
    let state = state();
    let projection = (0..100)
        .map(|i| format!("body AS body_{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let order = (0..100)
        .map(|i| format!("body_{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let memory = hawdb_executor::ExecutionMemoryConfig {
        batch_payload_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        ..Default::default()
    };
    for distinct in ["", "DISTINCT "] {
        let control = format!("SELECT {distinct}body AS body_0 FROM docs ORDER BY body_0");
        let probe = PayloadProbe::start(256 * 1024);
        let control = execute_relational_query_sql_with_runtime(
            &control,
            &[],
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
                RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
            ),
            RelationalQueryLimits {
                max_output_payload_bytes: 64 * 1024 * 1024,
                ..batched_index_join_limits()
            },
            &memory,
            None,
        );
        drop(probe);
        let source_copies = PAYLOAD_BYTES.get();
        assert_eq!(control.unwrap().rows.len(), 1);
        let sql = format!("SELECT {distinct}{projection} FROM docs ORDER BY {order}");
        let probe = PayloadProbe::start(256 * 1024);
        let result = execute_relational_query_sql_with_runtime(
            &sql,
            &[],
            &state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
                RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
            ),
            RelationalQueryLimits {
                max_output_payload_bytes: 64 * 1024 * 1024,
                ..batched_index_join_limits()
            },
            &memory,
            None,
        );
        drop(probe);
        let copied = PAYLOAD_BYTES.get();
        assert!(result.is_err(), "wide projection must refuse");
        assert!(
            copied <= source_copies + 256 * 1024,
            "projection copied {copied} payload bytes before refusal"
        );
    }
}

fn run(
    sql: &str,
    state: &RelationalState,
    memory: &hawdb_executor::ExecutionMemoryConfig,
    ledger: &QueryMemoryLedger,
    snapshot: Option<hawdb_storage::relational::RelationalRowPageSnapshotReader>,
) -> Result<RelationalQueryOutput> {
    let SqlStatement::Select(select) = hawdb_sql::prepare_postgres_sql(sql)?.statement else {
        panic!("expected SELECT");
    };
    let limits = RelationalQueryLimits {
        max_output_payload_bytes: 64 * 1024 * 1024,
        ..batched_index_join_limits()
    };
    let read_modes = RelationalQueryReadModes::new(
        RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
        RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
    );
    let prepared = prepare_relational_select(
        select,
        &[],
        state,
        read_modes,
        limits,
        RelationalJoinPlanningContext::new(
            Default::default(),
            RelationalJoinPlanningDirective::SyntaxOrder,
        ),
        Default::default(),
    )?;
    execute_select(
        &prepared,
        &[],
        AdmittedRelationalExecution {
            state,
            index_read_mode: read_modes.index,
            row_read_mode: read_modes.row,
            limits,
            execution_memory: memory,
            memory_ledger: ledger.clone(),
            task_context: None,
            planning_snapshot: snapshot.map(|reader| RelationalQueryPlanningSnapshot {
                reader: Some(reader),
                cost_contexts: Default::default(),
                index_context: crate::index_runtime::RelationalIndexRuntimeContext::new(
                    limits.index_read,
                    Default::default(),
                ),
            }),
        },
    )
}

#[test]
fn projection_refusal_refunds_and_retries_with_persistent_canonical_rows() {
    let fixture = crate::row_runtime::tests::fixtures::Fixture::with_state(state());
    let memory = hawdb_executor::ExecutionMemoryConfig {
        batch_payload_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        ..Default::default()
    };
    let projection = (0..100)
        .map(|i| format!("COALESCE(body, body) AS body_{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    for persistent in [false, true] {
        let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
        let sql = format!("SELECT {projection} FROM docs ORDER BY body_0");
        let result = run(
            &sql,
            &fixture.state,
            &memory,
            &ledger,
            persistent.then(|| fixture.snapshot()),
        );
        assert!(result.is_err(), "wide expression must refuse");
        assert_eq!(ledger.snapshot().used_bytes, 0);
        for suffix in ["LIMIT 0", "WHERE id = -1"] {
            let sql = format!("SELECT {projection} FROM docs {suffix}");
            assert!(run(
                &sql,
                &fixture.state,
                &memory,
                &ledger,
                persistent.then(|| fixture.snapshot())
            )
            .unwrap()
            .rows
            .is_empty());
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
        let output = run(
            "SELECT body AS body_0 FROM docs ORDER BY body_0",
            &fixture.state,
            &memory,
            &ledger,
            persistent.then(|| fixture.snapshot()),
        )
        .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("body_0"),
            Some(&Value::String("x".repeat(256 * 1024)))
        );
        if persistent {
            assert_eq!(output.row_execution_evidence.runtime_path, "snapshot_rows");
        }
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
    fixture.remove();
}

#[test]
fn distinct_order_keys_are_admitted_before_repeated_copies() {
    let state = state();
    let memory = hawdb_executor::ExecutionMemoryConfig {
        batch_payload_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        ..Default::default()
    };
    let order = std::iter::repeat_n("body_0", 100)
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT DISTINCT body AS body_0 FROM docs ORDER BY {order}");
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let probe = PayloadProbe::start(256 * 1024);
    let result = run(&sql, &state, &memory, &ledger, None);
    drop(probe);
    assert!(result.is_err());
    assert!(
        PAYLOAD_BYTES.get() < 8 * 256 * 1024,
        "DISTINCT ordering copied {} payload bytes",
        PAYLOAD_BYTES.get()
    );
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn ordinary_projection_preserves_nulls_duplicates_and_exact_payload_limits() {
    let mut state = RelationalState::default();
    for sql in [
        "CREATE TABLE docs (id BIGINT PRIMARY KEY, body TEXT)",
        "INSERT INTO docs (id, body) VALUES (1, 'a'), (2, 'a'), (3, NULL)",
    ] {
        let transaction = compile_relational_statement_sql(sql, &[], &state).unwrap();
        state = state
            .stage_transaction(transaction, Default::default(), Default::default())
            .unwrap();
    }
    let memory = hawdb_executor::ExecutionMemoryConfig::default();
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let output = run(
        "SELECT COALESCE(body, 'z') AS value FROM docs ORDER BY value",
        &state,
        &memory,
        &ledger,
        None,
    )
    .unwrap();
    assert_eq!(
        output.rows,
        vec![
            Row::from([("value".into(), Value::String("a".into()))]),
            Row::from([("value".into(), Value::String("a".into()))]),
            Row::from([("value".into(), Value::String("z".into()))])
        ]
    );
    let result = execute_relational_query_sql_with_runtime(
        "SELECT body AS b FROM docs WHERE id = 1",
        &[],
        &state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
            RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
        ),
        RelationalQueryLimits {
            max_output_payload_bytes: 2,
            ..batched_index_join_limits()
        },
        &memory,
        None,
    )
    .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}
