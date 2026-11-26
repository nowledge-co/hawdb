// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! An otherwise idle store admits a small typed read or transaction on a busy
//! host whose dynamic memory budget is below one request's fixed reservation
//! (#2003), while concurrent work, Background work and executor memory limits
//! keep their behavior.

use super::*;
use crate::search::{SearchProjectionKind, SearchProjectionRow};
use crate::{Database, SearchIndex};
use hawdb_qos::{RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot};
use std::num::NonZeroUsize;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;
/// The result limit Nowledge Mem configures.
const RESULT_LIMIT: usize = 64 * 1024 * 1024;

/// A pinned 8 GiB host with `available` free, sized like
/// `default_embedded_runtime_governor`: capacity is 2 GiB and the dynamic
/// budget is a quarter of `available` while nothing is admitted.
fn governor(available: u64) -> RuntimeGovernor {
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            result_budget_bytes: RESULT_LIMIT as u64,
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(8 * GIB), Some(available), None, None, None),
        ),
        IoConcurrencyBudget::new(4, 1),
    );
    governor.pin_resources();
    governor
}

fn config() -> DatabaseConfig {
    DatabaseConfig {
        max_read_result_rows: Some(1024),
        max_read_result_payload_bytes: Some(RESULT_LIMIT),
        ..DatabaseConfig::default()
    }
}

fn handle(governor: &RuntimeGovernor, config: DatabaseConfig) -> EmbeddedStoreHandle {
    let mut database = Database::new_with_config(config);
    database
        .query("CREATE (:Memory {id: 'seed', title: 'Seed memory'})")
        .unwrap();
    let graph = EmbeddedGraph::from_database_with_runtime_governor(
        database,
        GraphMode::WritableCutover,
        governor.clone(),
    );
    let mut index = SearchIndex::in_memory();
    index
        .upsert_projection_row(SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "seed".into(),
            title: "Seed memory".into(),
            body: "seed".into(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    let projection = SearchProjection::from_index(index);
    EmbeddedStoreHandle::new(EmbeddedStore::new(graph, Some(projection))).unwrap()
}

fn small_budget() -> EmbeddedReadSnapshotBudget {
    EmbeddedReadSnapshotBudget {
        max_rows: 4,
        max_payload_bytes: 4096,
    }
}

fn count_memories(handle: &EmbeddedStoreHandle, context: &RuntimeTaskContext) -> Result<usize> {
    handle.with_bounded_read_snapshot_context(small_budget(), context, |snapshot| {
        let output = snapshot.query_cypher(
            "MATCH (m:Memory) RETURN count(m) AS memories",
            &BTreeMap::new(),
            1,
        )?;
        match output.rows.first().and_then(|row| row.value(0)) {
            Some(Value::Int(count)) => Ok(usize::try_from(*count).unwrap()),
            other => panic!("unexpected count row {other:?}"),
        }
    })
}

fn assert_retryable_memory_saturated(error: HawDBError) {
    let HawDBError::RuntimeAdmissionRefused(refusal) = error else {
        panic!("expected a runtime admission refusal, got {error:?}");
    };
    assert_eq!(refusal.code, "memory_saturated", "{refusal:?}");
    assert!(refusal.retryable, "{refusal:?}");
    assert_eq!(refusal.waited, None, "{refusal:?}");
}

fn assert_idle(governor: &RuntimeGovernor) {
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.active_foreground_tasks, 0);
    assert_eq!(snapshot.active_background_tasks, 0);
    assert_eq!(snapshot.admitted_memory_bytes, 0);
    assert_eq!(snapshot.admissions, snapshot.completions);
}

#[test]
fn idle_store_admits_a_small_typed_read_below_the_dynamic_budget() {
    let governor = governor(GIB);
    let handle = handle(&governor, config());
    let limits = governor.snapshot().limits;
    assert_eq!(limits.memory_budget_bytes, 256 * MIB);
    assert_eq!(limits.memory_capacity_bytes, 2 * GIB);

    let count = handle
        .with_bounded_read_snapshot(small_budget(), |snapshot| {
            let admitted = governor.snapshot();
            assert_eq!(admitted.active_foreground_tasks, 1);
            // The typed read still reserves the configured working memory
            // and result ceiling, above the dynamic budget (#2003).
            assert_eq!(
                admitted.admitted_memory_bytes,
                config().execution_memory.query_memory_bytes.get() as u64 + RESULT_LIMIT as u64
            );
            assert!(admitted.admitted_memory_bytes > limits.memory_budget_bytes);
            let output = snapshot.query_cypher(
                "MATCH (m:Memory) RETURN count(m) AS memories",
                &BTreeMap::new(),
                1,
            )?;
            Ok(output.rows.len())
        })
        .unwrap();
    assert_eq!(count, 1);
    // The graph-only snapshot and the plain read transaction share sizing.
    handle
        .with_bounded_graph_read_snapshot(small_budget(), |_| Ok(()))
        .unwrap();
    handle.with_read_transaction(4096, |_| Ok(())).unwrap();
    // A planned query admits its retained planning memory, then execution.
    let output = handle
        .query_with_params_with_report_options_context(
            "MATCH (m:Memory) RETURN m.id AS id",
            &BTreeMap::new(),
            EmbeddedQueryReportOptions::default(),
            &RuntimeTaskContext::default(),
        )
        .unwrap();
    assert_eq!(output.output.rows.len(), 1);
    assert_idle(&governor);
}

#[test]
fn idle_store_admits_a_small_typed_transaction_below_the_dynamic_budget() {
    let governor = governor(GIB);
    let handle = handle(&governor, config());
    handle
        .with_transaction(|transaction| {
            assert_eq!(governor.snapshot().active_foreground_tasks, 1);
            transaction.query("CREATE (:Memory {id: 'second', title: 'Second memory'})")?;
            Ok(())
        })
        .unwrap();
    assert_idle(&governor);
    assert_eq!(
        count_memories(&handle, &RuntimeTaskContext::default()).unwrap(),
        2
    );
    assert_idle(&governor);
}

#[test]
fn idle_store_admits_one_foreground_request_and_refuses_a_concurrent_one() {
    let governor = governor(GIB);
    let handle = handle(&governor, config());
    handle
        .with_bounded_read_snapshot(small_budget(), |_| {
            // The first read took the floor; a second concurrent request,
            // read or transaction, is refused retryably as before.
            assert_retryable_memory_saturated(
                count_memories(&handle, &RuntimeTaskContext::default()).unwrap_err(),
            );
            assert_retryable_memory_saturated(handle.with_transaction(|_| Ok(())).unwrap_err());
            Ok(())
        })
        .unwrap();
    assert_idle(&governor);
    // Retrying unchanged once the first finishes succeeds.
    assert_eq!(
        count_memories(&handle, &RuntimeTaskContext::default()).unwrap(),
        1
    );
}

#[test]
fn idle_store_keeps_background_reads_within_the_dynamic_budget() {
    let governor = governor(GIB);
    let handle = handle(&governor, config());
    let background =
        RuntimeTaskContext::default().with_admission_priority(RuntimeWorkPriority::Background);
    assert_retryable_memory_saturated(count_memories(&handle, &background).unwrap_err());
    assert_idle(&governor);
    // With ample availability the same Background read is admitted.
    let ample = self::governor(8 * GIB);
    let handle = self::handle(&ample, config());
    assert_eq!(count_memories(&handle, &background).unwrap(), 1);
}

#[test]
fn floor_admitted_query_keeps_the_executor_memory_limit() {
    const QUERY_MEMORY: usize = 16 * 1024;
    let config = DatabaseConfig {
        execution_memory: crate::executor::ExecutionMemoryConfig {
            query_memory_bytes: NonZeroUsize::new(QUERY_MEMORY).unwrap(),
            ..Default::default()
        },
        ..config()
    };
    let sort = |handle: &EmbeddedStoreHandle| {
        handle
            .with_bounded_read_snapshot(
                EmbeddedReadSnapshotBudget {
                    max_rows: 1,
                    max_payload_bytes: 4096,
                },
                |snapshot| {
                    snapshot.query_cypher(
                        "UNWIND range(1, 5000) AS x WITH x ORDER BY x DESC RETURN sum(x) AS total",
                        &BTreeMap::new(),
                        1,
                    )
                },
            )
            .unwrap_err()
    };
    // 128 MiB free: the 16 KiB + 64 MiB request exceeds the 32 MiB dynamic
    // budget, so only the progress floor admits it.
    let constrained = governor(128 * MIB);
    assert_eq!(constrained.snapshot().limits.memory_budget_bytes, 32 * MIB);
    let floor_error = sort(&handle(&constrained, config.clone()));
    let HawDBError::ResourceBudgetExceeded(cause) = &floor_error else {
        panic!("expected the executor memory refusal, got {floor_error:?}");
    };
    assert_eq!(cause.resource, crate::ResourceBudgetKind::MemoryBytes);
    assert!(cause.budget <= QUERY_MEMORY as u64, "{cause:?}");
    assert_idle(&constrained);

    // Within the dynamic budget the same query fails exactly the same way.
    let ample = governor(8 * GIB);
    let ample_error = sort(&handle(&ample, config));
    assert_eq!(format!("{floor_error:?}"), format!("{ample_error:?}"));
}
