// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Automatic checkpoints keep running while synchronous callers wait in the
//! runtime admission queue (#1947 review). The checkpoint worker admits with
//! immediate Background requests; a queued waiter must only hold them back
//! when they compete for a resource the waiter itself is short of.

use super::*;
use crate::DatabaseConfig;
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkPriority, RuntimeWorkRequest,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BOUND: Duration = Duration::from_secs(30);

struct Directory(std::path::PathBuf);

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn directory(name: &str) -> Directory {
    let path = std::env::temp_dir().join(format!(
        "hawdb-1947-{name}-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    Directory(path)
}

/// Four CPUs, but a single Foreground I/O slot that queued callers contend for.
fn governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(1 << 30),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(8 << 30), Some(6 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(1, 1),
    )
}

fn io_request() -> RuntimeWorkRequest {
    RuntimeWorkRequest::foreground_query(0, 0)
        .with_blocking(false)
        .with_io_slots(1)
}

fn database(directory: &Directory, governor: &RuntimeGovernor) -> Database {
    let mut db = Database::open_with_config(
        directory.0.join("database"),
        DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..Default::default()
        },
    )
    .unwrap();
    db.set_runtime_governor(governor.clone());
    db.query_sql("CREATE TABLE progress (id BIGINT PRIMARY KEY)")
        .unwrap();
    db
}

fn completed_checkpoints(db: &Database) -> u64 {
    db.automatic_checkpoint_report()
        .unwrap()
        .map_or(0, |report| report.completed_checkpoints)
}

/// Writes until `checkpoints` more automatic checkpoints have completed.
fn write_through_checkpoints(db: &mut Database, checkpoints: u64) -> i64 {
    let target = completed_checkpoints(db) + checkpoints;
    let started = Instant::now();
    let mut id = 0;
    while completed_checkpoints(db) < target {
        db.query_sql(&format!("INSERT INTO progress (id) VALUES ({id})"))
            .unwrap();
        id += 1;
        assert!(
            started.elapsed() < BOUND,
            "automatic checkpoint stalled behind queued waiters: {:?}",
            db.automatic_checkpoint_report().unwrap()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    id
}

#[test]
fn automatic_checkpoint_runs_while_a_waiter_is_queued() {
    let directory = directory("queued");
    let governor = governor();
    let mut db = database(&directory, &governor);
    // B: a caller queued for the only Foreground I/O slot, held by A.
    let holder = governor.try_admit(io_request()).unwrap();
    let waiter = governor.admission_waiter(RuntimeWorkPriority::Foreground);
    assert!(governor.try_admit_waiter(&waiter, io_request()).is_err());
    assert_eq!(governor.snapshot().queued_admission_waiters, 1);
    // The checkpoint needs none of that I/O slot, so it must make progress.
    let written = write_through_checkpoints(&mut db, 2);
    assert!(written > 0);
    drop(holder);
    drop(governor.try_admit_waiter(&waiter, io_request()).unwrap());
    drop(waiter);
    assert_eq!(governor.snapshot().queued_admission_waiters, 0);
}

#[test]
fn automatic_checkpoint_runs_beside_callers_that_keep_requeuing() {
    let directory = directory("requeue");
    let governor = governor();
    let mut db = database(&directory, &governor);
    let stop = Arc::new(AtomicBool::new(false));
    let callers = (0..3)
        .map(|_| {
            let governor = governor.clone();
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let context = RuntimeTaskContext::default().with_admission_wait(BOUND);
                let mut admitted = 0usize;
                while !stop.load(Ordering::Acquire) {
                    let permit = governor
                        .admit_waiting(io_request(), &context, None)
                        .unwrap();
                    admitted += 1;
                    std::thread::yield_now();
                    drop(permit);
                }
                admitted
            })
        })
        .collect::<Vec<_>>();
    write_through_checkpoints(&mut db, 3);
    stop.store(true, Ordering::Release);
    for caller in callers {
        assert!(caller.join().unwrap() > 0);
    }
    assert_eq!(governor.snapshot().queued_admission_waiters, 0);
}
