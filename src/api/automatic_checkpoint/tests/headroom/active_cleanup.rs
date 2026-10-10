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
use crate::{QueryRows, Value};

fn wait(control: &Control, predicate: impl Fn(&State) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let state = control.lock().unwrap();
        if predicate(&state) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "owner stalled at {:?}: {:?}",
            state.phase,
            state.report
        );
        drop(state);
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn assert_rows(rows: &QueryRows, count: i64) {
    assert_eq!(rows.len(), count as usize);
    for (id, row) in rows.iter().enumerate() {
        assert_eq!(row["id"], Value::Int(id as i64));
        assert_eq!(row["body"], Value::String("b".repeat(512)));
    }
}

#[test]
fn foreground_denial_wakeup_does_not_bypass_an_admitted_retirement() {
    let fixture = Fixture::new();
    let (mut db, suspension, mut next) = seeded(&fixture);
    let initial = next;
    let mut old = db.begin_read_transaction().unwrap();
    let control = db.runtime.checkpoint_control_for_test();
    let governor = retirement_governor();
    db.set_runtime_governor(governor.clone());
    let healthy = governor.snapshot().resources;
    let (cleanup_probe, cleanup_started, resume_cleanup) = pause();
    control.lock().unwrap().retirement_probe = Some(cleanup_probe);
    drop(suspension);
    wait(&control, |state| state.phase == Phase::Handoff);
    let mut critical = healthy;
    critical.memory.pressure = hawdb_qos::RuntimeMemoryPressure::Critical;
    governor.update_resources(critical);
    drop(db.runtime.get_mut().unwrap());
    wait(&control, |state| {
        state.retired.is_some() && state.report.deferred_attempts > 0
    });
    while db
        .runtime
        .peek()
        .unwrap()
        .store
        .checkpoint_debt_snapshot()
        .unwrap()
        .delta_bytes
        * 1_000_000
        < 16 * 1024 * u64::from(hawdb_storage::pressure::STORAGE_PRESSURE_SOFT_RATIO_PER_MILLION)
    {
        create(&mut db, next).unwrap();
        next += 1;
        assert!(next < 320);
    }
    let base = fixture.0.join("checkpoint.1.hawdb");
    let bytes = std::fs::read(&base).unwrap();
    let epoch_before = control.lock().unwrap().debt.unwrap().commit_epoch;
    let (waiting, blocked) = mpsc::channel();
    let (frontend_probe, frontend_woken, resume_frontend) = pause();
    {
        let mut state = control.lock().unwrap();
        state.frontend_wait_probe = Some(waiting);
        state.frontend_resume_probe = Some(frontend_probe);
    }
    let (completed, result) = mpsc::channel();
    let producer = std::thread::spawn(move || {
        let created = create(&mut db, next);
        completed.send((db, created)).unwrap();
    });
    assert_eq!(
        blocked.recv_timeout(Duration::from_secs(15)).unwrap(),
        Phase::Retiring
    );
    frontend_woken
        .recv_timeout(Duration::from_secs(15))
        .unwrap();
    // The caller has already observed a denial, but has not reevaluated
    // its wait. Let that same real retirement acquire execution and I/O.
    governor.update_resources(healthy);
    control.changed.notify_all();
    cleanup_started
        .recv_timeout(Duration::from_secs(15))
        .unwrap();
    assert_eq!(governor.snapshot().active_cpu_slots, 1);
    assert_eq!(governor.snapshot().active_background_tasks, 1);
    assert_eq!(governor.snapshot().active_background_io_slots, 1);
    assert_eq!(std::fs::read(&base).unwrap(), bytes);
    resume_frontend.send(()).unwrap();
    let reblocked = blocked.recv_timeout(Duration::from_secs(3));
    let epoch_while_cleanup_is_active = control.lock().unwrap().debt.unwrap().commit_epoch;
    // Complete the actual paused cleanup before checking a failed
    // observation, so both real threads are joined in either outcome.
    let finishing = control.suspend().unwrap();
    resume_cleanup.send(()).unwrap();
    let (mut db, created) = result.recv_timeout(Duration::from_secs(15)).unwrap();
    producer.join().unwrap();
    created.unwrap();
    wait(&control, |state| {
        state.phase == Phase::Idle && state.retired.is_none()
    });
    assert_rows(
        &old.query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
            .unwrap()
            .rows,
        initial,
    );
    db.checkpoint().unwrap();
    assert_rows(
        &db.query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
            .unwrap()
            .rows,
        next + 1,
    );
    drop(old);
    drop(finishing);
    drop(db);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    let mut reopened = Database::open(&fixture.0).unwrap();
    assert_rows(
        &reopened
            .query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
            .unwrap()
            .rows,
        next + 1,
    );
    assert_eq!(
        epoch_while_cleanup_is_active, epoch_before,
        "a denial wakeup must not admit another commit during active cleanup"
    );
    assert_eq!(
        reblocked.unwrap(),
        Phase::Retiring,
        "a denial wakeup must not release pressure while cleanup is admitted"
    );
}
