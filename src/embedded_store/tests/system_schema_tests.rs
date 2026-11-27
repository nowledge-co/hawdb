// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Applying a system schema registry to an open store through the shared
//! handle: maintenance admission first, then the exclusive write access, one
//! all-or-nothing batch, the post-schema checkpoint on out-of-core stores
//! (#2030) and a typed refusal on read-only handles.

use super::*;
use crate::store::StorageResidencyMode;
use crate::{
    EmbeddedReadSnapshotBudget, SystemSchemaApplyReport, SystemSchemaMigration,
    SystemSchemaRegistry,
};
use hawdb_core::{RuntimeCancellationReason, RuntimeCancellationToken, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimePermit, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const OWNER: &str = "nowledge.mem";
const NODES: i64 = 64;
/// Bound for anything that should happen promptly.
const PROMPT: Duration = Duration::from_secs(20);

struct Root(std::path::PathBuf);

impl Root {
    fn new(name: &str) -> Self {
        let path = unique_embedded_store_test_dir(name);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn graph(&self) -> std::path::PathBuf {
        self.0.join("graph")
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn registry() -> SystemSchemaRegistry {
    SystemSchemaRegistry::new(
        OWNER,
        [
            SystemSchemaMigration::new(
                1,
                "create_memory_audit",
                ["CREATE TABLE memory_audit (id TEXT PRIMARY KEY)"],
            ),
            SystemSchemaMigration::new(2, "index_memory_keys", Vec::<String>::new())
                .with_cypher(["CREATE INDEX ON :Memory(k)"]),
        ],
    )
}

fn config(residency: StorageResidencyMode) -> DatabaseConfig {
    let mut config = DatabaseConfig {
        storage_residency_mode: residency,
        // Only an explicit checkpoint may build the index projection.
        automatic_checkpoint_max_age: Duration::from_secs(3600),
        max_read_result_rows: Some(1024),
        max_read_result_payload_bytes: Some(1 << 20),
        ..DatabaseConfig::default()
    };
    config.runtime_capabilities.background_maintenance = false;
    config
}

fn options(
    root: &Root,
    mode: GraphMode,
    residency: StorageResidencyMode,
) -> EmbeddedStoreOpenOptions {
    let mut options = EmbeddedStoreOpenOptions::graph_only(root.graph(), mode);
    options.database_config = Some(config(residency));
    options
}

fn open(root: &Root, mode: GraphMode, residency: StorageResidencyMode) -> EmbeddedStoreHandle {
    EmbeddedStoreHandle::open_with_options(options(root, mode, residency))
        .unwrap()
        .0
}

fn open_with_governor(root: &Root, governor: &RuntimeGovernor) -> EmbeddedStoreHandle {
    EmbeddedStoreHandle::open_with_options_and_runtime_governor(
        options(
            root,
            GraphMode::WritableCutover,
            StorageResidencyMode::OutOfCore,
        ),
        governor.clone(),
    )
    .unwrap()
    .0
}

fn create_memories(handle: &EmbeddedStoreHandle) {
    let rows = (0..NODES)
        .map(|id| {
            Value::Map(BTreeMap::from([
                ("id".to_string(), Value::Int(id)),
                ("k".to_string(), Value::String(format!("k{}", id % 8))),
            ]))
        })
        .collect();
    handle
        .query_with_params_with_report(
            "UNWIND $rows AS r CREATE (:Memory {id: r.id, k: r.k})",
            &BTreeMap::from([("rows".to_string(), Value::List(rows))]),
        )
        .unwrap();
}

/// A checkpointed base of `NODES` `:Memory` nodes, closed again.
fn seeded(name: &str) -> Root {
    let root = Root::new(name);
    let handle = open(
        &root,
        GraphMode::WritableCutover,
        StorageResidencyMode::Materialized,
    );
    create_memories(&handle);
    handle.checkpoint().unwrap();
    root
}

/// A one-CPU host: every maintenance admission needs the single CPU slot.
fn one_cpu_governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(2 << 30),
            result_budget_bytes: 128 << 20,
            background_task_limit: NonZeroUsize::new(1),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(1).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(8 << 30), Some(6 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

fn hold_cpu(governor: &RuntimeGovernor) -> RuntimePermit {
    governor
        .try_admit(RuntimeWorkRequest::foreground_query(0, 0).with_blocking(false))
        .unwrap()
}

fn apply(
    handle: &EmbeddedStoreHandle,
    context: &RuntimeTaskContext,
) -> crate::Result<SystemSchemaApplyReport> {
    handle.apply_system_schema_registry_context(&registry(), context)
}

fn applied(report: &SystemSchemaApplyReport) -> &[u64] {
    &report.registries[0].applied_versions
}

/// The installed version of `OWNER`.
fn installed(handle: &EmbeddedStoreHandle) -> u64 {
    handle
        .write_store()
        .unwrap()
        .graph_mut()
        .database_mut()
        .system_schema_status(&[registry()])
        .unwrap()
        .into_iter()
        .find(|status| status.owner == OWNER)
        .unwrap()
        .installed_version
}

fn uses_key_index(handle: &EmbeddedStoreHandle) -> bool {
    let plan = handle
        .query_with_report("EXPLAIN MATCH (m:Memory {k: 'k3'}) RETURN m.id AS id")
        .unwrap()
        .output;
    !format!("{:?}", plan.rows).contains("SeqNodeScan")
}

fn count_k3(handle: &EmbeddedStoreHandle) -> crate::Result<i64> {
    handle.with_bounded_read_snapshot(budget(), |snapshot| {
        let output = snapshot.query_cypher(
            "MATCH (m:Memory {k: 'k3'}) RETURN count(m) AS n",
            &BTreeMap::new(),
            1,
        )?;
        match output.rows.first().and_then(|row| row.value(0)) {
            Some(Value::Int(count)) => Ok(*count),
            other => panic!("unexpected count row {other:?}"),
        }
    })
}

fn budget() -> EmbeddedReadSnapshotBudget {
    EmbeddedReadSnapshotBudget {
        max_rows: 4,
        max_payload_bytes: 4096,
    }
}

fn audit_table_exists(handle: &EmbeddedStoreHandle) -> bool {
    handle
        .with_bounded_read_snapshot(budget(), |snapshot| {
            snapshot.query_sql("SELECT COUNT(*) AS n FROM memory_audit", &[], 1)
        })
        .is_ok()
}

fn unserved(handle: &EmbeddedStoreHandle) -> Vec<crate::UnservedGraphPropertyIndex> {
    handle
        .storage_residency_report()
        .unwrap()
        .unserved_graph_property_indexes
}

#[test]
fn out_of_core_apply_builds_the_index_projection_and_reapply_is_a_no_op() {
    let root = seeded("system_schema_handle_out_of_core");
    let handle = open(
        &root,
        GraphMode::WritableCutover,
        StorageResidencyMode::OutOfCore,
    );
    assert!(handle.storage_residency_report().unwrap().out_of_core);
    assert!(!uses_key_index(&handle));

    let report = apply(&handle, &RuntimeTaskContext::default()).unwrap();
    assert_eq!(applied(&report), [1, 2]);
    assert_eq!(report.registries[0].previous_version, 0);
    assert_eq!(report.registries[0].current_version, 2);
    assert!(report.applied_any());
    assert!(report.checkpointed, "{report:?}");
    // The persistent projection is published: seeks serve the new index.
    assert_eq!(unserved(&handle), Vec::new());
    assert!(uses_key_index(&handle));
    assert!(audit_table_exists(&handle));
    assert_eq!(count_k3(&handle).unwrap(), NODES / 8);

    let epoch = handle
        .read_store()
        .unwrap()
        .graph
        .database()
        .commit_epoch()
        .unwrap();
    let again = apply(&handle, &RuntimeTaskContext::default()).unwrap();
    assert!(applied(&again).is_empty());
    assert!(!again.applied_any());
    assert!(!again.checkpointed);
    assert_eq!(again.registries[0].previous_version, 2);
    assert_eq!(
        handle
            .read_store()
            .unwrap()
            .graph
            .database()
            .commit_epoch()
            .unwrap(),
        epoch
    );
    drop(handle);

    // Durable across reopen.
    let handle = open(
        &root,
        GraphMode::WritableCutover,
        StorageResidencyMode::OutOfCore,
    );
    assert_eq!(installed(&handle), 2);
    assert!(uses_key_index(&handle));
    assert_eq!(unserved(&handle), Vec::new());
}

/// Resident stores serve an index from memory: no post-schema checkpoint.
#[test]
fn resident_and_in_memory_apply_needs_no_checkpoint() {
    let root = seeded("system_schema_handle_resident");
    let durable = open(
        &root,
        GraphMode::WritableCutover,
        StorageResidencyMode::Materialized,
    );
    let in_memory = EmbeddedStoreHandle::new(EmbeddedStore::new(
        EmbeddedGraph::from_database(
            Database::new_with_config(config(StorageResidencyMode::Materialized)),
            GraphMode::WritableCutover,
        ),
        None,
    ))
    .unwrap();
    create_memories(&in_memory);
    for handle in [&durable, &in_memory] {
        assert!(!handle.storage_residency_report().unwrap().out_of_core);
        let report = apply(handle, &RuntimeTaskContext::default()).unwrap();
        assert_eq!(applied(&report), [1, 2]);
        assert!(!report.checkpointed, "{report:?}");
        assert!(uses_key_index(handle));
        assert_eq!(unserved(handle), Vec::new());
        assert!(applied(&apply(handle, &RuntimeTaskContext::default()).unwrap()).is_empty());
    }
}

#[test]
fn read_only_handle_refuses_a_pending_migration_and_writes_nothing() {
    let root = seeded("system_schema_handle_read_only");
    let version_one = SystemSchemaRegistry::new(OWNER, [registry().migrations()[0].clone()]);
    {
        let handle = open(
            &root,
            GraphMode::WritableCutover,
            StorageResidencyMode::OutOfCore,
        );
        handle
            .apply_system_schema_registry_context(&version_one, &RuntimeTaskContext::default())
            .unwrap();
    }
    {
        let handle = open(
            &root,
            GraphMode::ShadowReadOnly,
            StorageResidencyMode::OutOfCore,
        );
        let epoch = handle
            .read_store()
            .unwrap()
            .graph
            .database()
            .commit_epoch()
            .unwrap();
        // Nothing pending: the report, with nothing applied.
        let report = handle
            .apply_system_schema_registry_context(&version_one, &RuntimeTaskContext::default())
            .unwrap();
        assert!(applied(&report).is_empty());
        assert_eq!(report.registries[0].current_version, 1);
        assert!(!report.checkpointed);
        // Migration 2 is pending: refused before anything is written.
        let error = apply(&handle, &RuntimeTaskContext::default()).unwrap_err();
        assert!(
            matches!(
                error,
                HawDBError::ReadOnly {
                    operation: "apply_system_schema_registry"
                }
            ),
            "{error:?}"
        );
        assert_eq!(
            handle
                .read_store()
                .unwrap()
                .graph
                .database()
                .commit_epoch()
                .unwrap(),
            epoch
        );
        assert!(!uses_key_index(&handle));
        // The handle stays usable.
        assert_eq!(count_k3(&handle).unwrap(), NODES / 8);
    }
    let handle = open(
        &root,
        GraphMode::WritableCutover,
        StorageResidencyMode::OutOfCore,
    );
    assert_eq!(installed(&handle), 1, "migration 2 is still pending");
    assert!(!uses_key_index(&handle));
    assert_eq!(
        applied(&apply(&handle, &RuntimeTaskContext::default()).unwrap()),
        [2]
    );
}

#[test]
fn refused_admission_applies_nothing_and_never_waits_unboundedly() {
    let root = seeded("system_schema_handle_refused");
    let governor = one_cpu_governor();
    let handle = open_with_governor(&root, &governor);
    let held = hold_cpu(&governor);

    // The default context refuses at once.
    let error = apply(&handle, &RuntimeTaskContext::default()).unwrap_err();
    let HawDBError::RuntimeAdmissionRefused(refusal) = error else {
        panic!("expected an admission refusal, got {error:?}");
    };
    assert!(refusal.retryable);
    assert_eq!(refusal.waited, None);

    // A bounded wait ends with the typed refusal and the time queued.
    let started = Instant::now();
    let error = apply(
        &handle,
        &RuntimeTaskContext::default().with_admission_wait(Duration::from_millis(30)),
    )
    .unwrap_err();
    let HawDBError::RuntimeAdmissionRefused(refusal) = error else {
        panic!("expected an admission refusal, got {error:?}");
    };
    assert!(refusal
        .waited
        .is_some_and(|waited| waited >= Duration::from_millis(30)));
    assert!(started.elapsed() < PROMPT);

    drop(held);
    assert_eq!(installed(&handle), 0);
    assert!(!audit_table_exists(&handle));
    let report = apply(&handle, &RuntimeTaskContext::default()).unwrap();
    assert_eq!(applied(&report), [1, 2]);
    assert!(report.checkpointed);
}

#[test]
fn cancelled_or_expired_context_applies_nothing() {
    let root = seeded("system_schema_handle_stopped");
    let handle = open(
        &root,
        GraphMode::WritableCutover,
        StorageResidencyMode::OutOfCore,
    );
    let token = RuntimeCancellationToken::new();
    token.cancel();
    assert!(matches!(
        apply(&handle, &RuntimeTaskContext::without_deadline(token)),
        Err(HawDBError::RuntimeTaskStopped(
            RuntimeCancellationReason::Cancelled
        ))
    ));
    assert!(matches!(
        apply(&handle, &RuntimeTaskContext::with_timeout(Duration::ZERO)),
        Err(HawDBError::RuntimeTaskStopped(
            RuntimeCancellationReason::DeadlineExceeded
        ))
    ));
    // A deadline that passes while queued for admission stops the wait.
    let governor = one_cpu_governor();
    drop(handle);
    let handle = open_with_governor(&root, &governor);
    let held = hold_cpu(&governor);
    assert!(matches!(
        apply(
            &handle,
            &RuntimeTaskContext::with_timeout(Duration::from_millis(30))
                .with_admission_wait(Duration::MAX),
        ),
        Err(HawDBError::RuntimeTaskStopped(
            RuntimeCancellationReason::DeadlineExceeded
        ))
    ));
    drop(held);
    assert_eq!(installed(&handle), 0);
    assert!(!audit_table_exists(&handle));
    assert!(!uses_key_index(&handle));
}

/// Admission comes first: a call queued for admission holds neither the
/// store guard nor the exclusive database lease, so a writer still commits.
#[test]
fn queued_apply_does_not_block_writers() {
    let root = seeded("system_schema_handle_queued");
    let governor = one_cpu_governor();
    let handle = open_with_governor(&root, &governor);
    let held = hold_cpu(&governor);
    let applier = thread::spawn({
        let handle = handle.clone();
        move || {
            apply(
                &handle,
                &RuntimeTaskContext::default().with_admission_wait(PROMPT),
            )
        }
    });
    let started = Instant::now();
    while governor.snapshot().queued_admission_waiters < 1 {
        assert!(!applier.is_finished(), "{:?}", applier.join().unwrap());
        assert!(started.elapsed() < PROMPT, "apply never queued");
        thread::yield_now();
    }
    let (sender, receiver) = mpsc::channel();
    let writer = thread::spawn({
        let handle = handle.clone();
        move || {
            // The same exclusive write access the apply takes after admission.
            let mut store = handle.write_store().unwrap();
            store
                .graph_mut()
                .database_mut()
                .query("CREATE (:Memory {id: 1000, k: 'k3'})")
                .unwrap();
            drop(store);
            sender.send(()).unwrap();
        }
    });
    receiver
        .recv_timeout(PROMPT)
        .expect("a queued apply blocked the write access");
    writer.join().unwrap();
    assert!(!applier.is_finished());
    drop(held);
    let report = applier.join().unwrap().unwrap();
    assert_eq!(applied(&report), [1, 2]);
    assert!(report.admission_wait > Duration::ZERO, "{report:?}");
    assert!(report.write_lock_held > Duration::ZERO, "{report:?}");
    // The write that ran while the apply queued was indexed too.
    assert_eq!(count_k3(&handle).unwrap(), NODES / 8 + 1);
}

/// Two maintenance threads apply the same registry: exactly one applies it,
/// the other skips it or gets the typed busy refusal, never a lock timeout,
/// and the index is built once.
#[test]
fn concurrent_applies_build_the_index_once() {
    let root = seeded("system_schema_handle_concurrent_apply");
    let handle = open(
        &root,
        GraphMode::WritableCutover,
        StorageResidencyMode::OutOfCore,
    );
    let barrier = Arc::new(Barrier::new(2));
    let results = (0..2)
        .map(|_| {
            let handle = handle.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                apply(
                    &handle,
                    &RuntimeTaskContext::default().with_admission_wait(PROMPT),
                )
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    let mut applying = 0;
    for result in &results {
        match result {
            Ok(report) if applied(report) == [1, 2] => applying += 1,
            Ok(report) => {
                assert!(applied(report).is_empty(), "{report:?}");
                assert!(!report.checkpointed, "{report:?}");
                assert_eq!(report.registries[0].previous_version, 2);
            }
            Err(HawDBError::BranchBusy {
                resource: "system schema apply in progress",
            }) => {}
            Err(error) => panic!("unexpected error {error:?}"),
        }
    }
    assert_eq!(applying, 1, "{results:?}");
    assert_eq!(installed(&handle), 2);
    assert!(uses_key_index(&handle));
    assert_eq!(unserved(&handle), Vec::new());
    assert_eq!(count_k3(&handle).unwrap(), NODES / 8);
}

/// A call made while another apply is in progress is refused at once with the
/// typed busy error: before admission (it never queues), holding no lock and
/// applying nothing. The guard is released when the first call returns.
#[test]
fn apply_in_progress_refuses_a_second_call_immediately() {
    let root = seeded("system_schema_handle_busy");
    let governor = one_cpu_governor();
    let handle = open_with_governor(&root, &governor);
    let held = hold_cpu(&governor);
    let first = thread::spawn({
        let handle = handle.clone();
        move || {
            apply(
                &handle,
                &RuntimeTaskContext::default().with_admission_wait(PROMPT),
            )
        }
    });
    let started = Instant::now();
    while governor.snapshot().queued_admission_waiters < 1 {
        assert!(!first.is_finished(), "{:?}", first.join().unwrap());
        assert!(started.elapsed() < PROMPT, "apply never queued");
        thread::yield_now();
    }
    // Another clone, a long admission wait: still refused at once.
    let second = handle.clone();
    let refused_at = Instant::now();
    let error = apply(
        &second,
        &RuntimeTaskContext::default().with_admission_wait(PROMPT),
    )
    .unwrap_err();
    assert!(
        matches!(
            error,
            HawDBError::BranchBusy {
                resource: "system schema apply in progress"
            }
        ),
        "{error:?}"
    );
    assert!(refused_at.elapsed() < PROMPT);
    assert_eq!(governor.snapshot().queued_admission_waiters, 1);
    assert_eq!(installed(&handle), 0);
    drop(held);
    assert_eq!(applied(&first.join().unwrap().unwrap()), [1, 2]);
    // Released: a retry finds the migrations applied and skips them.
    let retry = apply(&second, &RuntimeTaskContext::default()).unwrap();
    assert!(applied(&retry).is_empty());
    // Released on an error path after it was taken (a refused admission).
    let held = hold_cpu(&governor);
    assert!(matches!(
        apply(&handle, &RuntimeTaskContext::default()),
        Err(HawDBError::RuntimeAdmissionRefused(_))
    ));
    drop(held);
    assert!(applied(&apply(&second, &RuntimeTaskContext::default()).unwrap()).is_empty());
}

/// Readers on other threads keep succeeding while the registry applies and
/// see either the state before or after it, never a partial one.
#[test]
fn concurrent_readers_see_before_or_after_state() {
    let root = seeded("system_schema_handle_concurrent_reads");
    let handle = open(
        &root,
        GraphMode::WritableCutover,
        StorageResidencyMode::OutOfCore,
    );
    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicUsize::new(0));
    let readers = (0..2)
        .map(|_| {
            let handle = handle.clone();
            let stop = Arc::clone(&stop);
            let reads = Arc::clone(&reads);
            thread::spawn(move || {
                while !stop.load(Ordering::Acquire) || reads.load(Ordering::Acquire) < 4 {
                    match count_k3(&handle) {
                        Ok(count) => assert_eq!(count, NODES / 8),
                        // A read racing the apply may be refused admission.
                        Err(HawDBError::RuntimeAdmissionRefused(_)) => continue,
                        Err(error) => panic!("read failed: {error:?}"),
                    }
                    reads.fetch_add(1, Ordering::AcqRel);
                }
            })
        })
        .collect::<Vec<_>>();
    while reads.load(Ordering::Acquire) == 0 {
        thread::yield_now();
    }
    let report = apply(
        &handle,
        &RuntimeTaskContext::default().with_admission_wait(PROMPT),
    )
    .unwrap();
    stop.store(true, Ordering::Release);
    for reader in readers {
        reader.join().unwrap();
    }
    assert_eq!(applied(&report), [1, 2]);
    assert!(report.checkpointed);
    assert!(uses_key_index(&handle));
}

/// A migration step that fails at execution rolls the whole batch back: no
/// DDL, no migration record, the handle stays usable, and a corrected
/// registry later applies every migration from scratch.
#[test]
fn failing_migration_rolls_back_the_whole_batch() {
    let root = seeded("system_schema_handle_failing");
    let handle = open(
        &root,
        GraphMode::WritableCutover,
        StorageResidencyMode::OutOfCore,
    );
    let first = SystemSchemaMigration::new(
        1,
        "index_memory_keys",
        ["CREATE TABLE memory_audit (id TEXT PRIMARY KEY)"],
    )
    .with_cypher(["CREATE INDEX ON :Memory(k)"]);
    // Valid on its own, but the table already exists inside the batch.
    let failing = SystemSchemaRegistry::new(
        OWNER,
        [
            first.clone(),
            SystemSchemaMigration::new(
                2,
                "duplicate_audit",
                ["CREATE TABLE memory_audit (id TEXT PRIMARY KEY)"],
            ),
        ],
    );
    let epoch = handle
        .read_store()
        .unwrap()
        .graph
        .database()
        .commit_epoch()
        .unwrap();
    assert!(handle
        .apply_system_schema_registry_context(&failing, &RuntimeTaskContext::default())
        .is_err());
    assert_eq!(
        handle
            .read_store()
            .unwrap()
            .graph
            .database()
            .commit_epoch()
            .unwrap(),
        epoch
    );
    assert_eq!(installed(&handle), 0);
    assert!(!audit_table_exists(&handle));
    assert!(!uses_key_index(&handle));
    // Still usable for reads and writes.
    handle
        .query_with_report("CREATE (:Memory {id: 1000, k: 'k3'})")
        .unwrap();
    assert_eq!(count_k3(&handle).unwrap(), NODES / 8 + 1);

    let fixed = SystemSchemaRegistry::new(
        OWNER,
        [
            first,
            SystemSchemaMigration::new(
                2,
                "create_memory_notes",
                ["CREATE TABLE memory_notes (id TEXT PRIMARY KEY)"],
            ),
        ],
    );
    let report = handle
        .apply_system_schema_registry_context(&fixed, &RuntimeTaskContext::default())
        .unwrap();
    assert_eq!(applied(&report), [1, 2]);
    assert!(report.checkpointed);
    assert!(audit_table_exists(&handle));
    assert!(uses_key_index(&handle));
    assert_eq!(unserved(&handle), Vec::new());
    assert_eq!(count_k3(&handle).unwrap(), NODES / 8 + 1);
}
