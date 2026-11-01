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

#[test]
fn critical_preparation_wait_returns_busy_without_resetting_its_deadline() {
    assert_critical_wait_returns_busy(Phase::Preparing);
}

#[test]
fn critical_discard_wait_returns_busy_without_resetting_its_deadline() {
    assert_critical_wait_returns_busy(Phase::Discarding);
}

#[test]
fn critical_release_wait_returns_busy_without_resetting_its_deadline() {
    assert_critical_wait_returns_busy(Phase::Releasing);
}

fn capacity_boundary(limit: u64) -> u64 {
    let defer = (u128::from(limit)
        * u128::from(hawdb_storage::pressure::STORAGE_PRESSURE_DEFER_RATIO_PER_MILLION)
        / 1_000_000) as u64;
    defer.saturating_sub(limit / 10)
}

fn assert_critical_wait_returns_busy(phase: Phase) {
    let fixture = Fixture::new();
    let (db, _suspension, _) = seeded(&fixture);
    let control = Arc::new(Control::default());
    {
        let mut state = control.lock().unwrap();
        state.phase = phase;
        let mut debt = db
            .runtime
            .peek()
            .unwrap()
            .store
            .checkpoint_debt_snapshot()
            .unwrap();
        debt.delta_bytes = capacity_boundary(debt.max_delta_bytes.unwrap());
        state.debt = Some(debt);
        assert!(preparation_needs_capacity(&state));
    }
    let now = Instant::now();
    control.set_frontend_time(now);
    let (waiting, entered) = mpsc::channel();
    control.lock().unwrap().frontend_wait_probe = Some(waiting);
    let frontend = control.clone();
    let (finished, completion) = mpsc::channel();
    let caller = std::thread::spawn(move || {
        let result = frontend.lock_frontend().map(|guard| guard.phase);
        finished.send(result).unwrap();
    });
    assert_eq!(
        entered.recv_timeout(Duration::from_secs(15)).unwrap(),
        phase
    );
    let before = now + PREPARATION_CAPACITY_WAIT_LIMIT - Duration::from_nanos(1);
    for _ in 0..4 {
        control.set_frontend_time(before);
        assert_eq!(
            entered.recv_timeout(Duration::from_secs(15)).unwrap(),
            phase
        );
        assert!(matches!(
            completion.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
    }
    control.set_frontend_time(now + PREPARATION_CAPACITY_WAIT_LIMIT);
    let result = completion.recv_timeout(Duration::from_secs(15)).unwrap();
    caller.join().unwrap();
    assert!(matches!(
        result,
        Err(HawDBError::BranchBusy {
            resource: "automatic checkpoint capacity"
        })
    ));
    assert_eq!(control.lock().unwrap().phase, phase);
    control.ensure_healthy().unwrap();
    control.lock().unwrap().phase = Phase::Idle;
    control.notify_all();
    assert_eq!(control.lock_frontend().unwrap().phase, Phase::Idle);
}

#[test]
fn parked_preparation_rejects_no_data_and_a_busy_writer_can_retry_after_checkpoint() {
    let fixture = Fixture::new();
    let (mut db, suspension, mut next) = seeded(&fixture);
    while !preparation_needs_capacity(&db.runtime.checkpoint_control_for_test().lock().unwrap()) {
        create(&mut db, next).unwrap();
        next += 1;
    }
    let control = db.runtime.checkpoint_control_for_test();
    let (sealed, observed) = mpsc::channel();
    let (resume, continuation) = mpsc::channel();
    control.lock().unwrap().prefix_seal_probe = Some(Arc::new(PrefixSealProbe {
        sealed,
        resume: Mutex::new(continuation),
    }));
    drop(suspension);
    observed.recv_timeout(Duration::from_secs(15)).unwrap();
    let before = db
        .runtime
        .peek()
        .unwrap()
        .store
        .checkpoint_source_identity();
    let result = create(&mut db, next);
    let after = db
        .runtime
        .peek()
        .unwrap()
        .store
        .checkpoint_source_identity();
    // Always release the test-only owner pause before checking the outcome.
    control.lock().unwrap().prefix_seal_probe = None;
    resume.send(()).unwrap();
    assert!(matches!(
        result,
        Err(HawDBError::BranchBusy {
            resource: "automatic checkpoint capacity"
        })
    ));
    assert_eq!(after, before);
    db.checkpoint().unwrap();
    create(&mut db, next).unwrap();
    let expected = db
        .query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(expected.len(), usize::try_from(next + 1).unwrap());
    for (id, row) in expected.iter().enumerate() {
        assert_eq!(
            row.get("id"),
            Some(&crate::Value::Int(i64::try_from(id).unwrap()))
        );
        assert_eq!(
            row.get("body"),
            Some(&crate::Value::String("b".repeat(512)))
        );
    }
    drop(db);
    let mut reopened = Database::open(&fixture.0).unwrap();
    assert_eq!(
        reopened
            .query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
            .unwrap()
            .rows,
        expected
    );
}

fn cleanup_control(phase: Phase, delta_bytes: u64) -> Arc<Control> {
    let control = Arc::new(Control::default());
    {
        let mut state = control.lock().unwrap();
        state.phase = phase;
        state.debt = Some(CheckpointDebtSnapshot {
            commit_epoch: 1,
            checkpoint_commit_epoch: 0,
            wal_generation: 1,
            wal_bytes: 0,
            wal_age_millis: 0,
            max_wal_bytes: None,
            max_wal_record_bytes: None,
            delta_bytes,
            max_delta_bytes: Some(16 * 1024),
            read_only: false,
            wal_sync_group_active: false,
            relational_live: Default::default(),
        });
    }
    control
}

#[test]
fn releasing_old_owners_preserves_available_write_capacity() {
    assert_cleanup_preserves_available_write_capacity(Phase::Releasing);
}

#[test]
fn discarding_preparation_preserves_available_write_capacity() {
    assert_cleanup_preserves_available_write_capacity(Phase::Discarding);
}

fn assert_cleanup_preserves_available_write_capacity(phase: Phase) {
    let boundary = capacity_boundary(16 * 1024);
    assert!(boundary > 0);
    let control = cleanup_control(phase, boundary - 1);
    assert!(!preparation_needs_capacity(&control.lock().unwrap()));
    let state = control.lock_frontend().unwrap();
    assert_eq!(state.phase, phase);
}

#[test]
fn releasing_old_owners_fences_capacity_growth_until_cleanup_finishes() {
    assert_cleanup_fences_capacity_growth(Phase::Releasing);
}

#[test]
fn discarding_preparation_fences_capacity_growth_until_cleanup_finishes() {
    assert_cleanup_fences_capacity_growth(Phase::Discarding);
}

fn assert_cleanup_fences_capacity_growth(phase: Phase) {
    let control = cleanup_control(phase, capacity_boundary(16 * 1024));
    assert!(preparation_needs_capacity(&control.lock().unwrap()));
    let (waiting, observed) = mpsc::channel();
    control.lock().unwrap().frontend_wait_probe = Some(waiting);
    let (finished, completion) = mpsc::channel();
    let writer_control = control.clone();
    let writer = std::thread::spawn(move || {
        let phase = writer_control.lock_frontend().unwrap().phase;
        finished.send(phase).unwrap();
    });
    let observed_phase = observed.recv_timeout(Duration::from_secs(1));
    let early = completion.try_recv();
    {
        let mut state = control.lock().unwrap();
        state.phase = Phase::Idle;
        control.notify_all();
    }
    writer.join().unwrap();
    assert_eq!(observed_phase.unwrap(), phase);
    assert!(matches!(early, Err(mpsc::TryRecvError::Empty)));
    assert_eq!(completion.recv().unwrap(), Phase::Idle);
}

#[test]
fn only_completed_checkpoint_or_builder_progress_renews_capacity_deadline() {
    for checkpoint_progress in [false, true] {
        let control = cleanup_control(Phase::Preparing, capacity_boundary(16 * 1024));
        let now = Instant::now();
        control.set_frontend_time(now);
        let (waiting, entered) = mpsc::channel();
        control.lock().unwrap().frontend_wait_probe = Some(waiting);
        let frontend = control.clone();
        let (finished, completion) = mpsc::channel();
        let caller = std::thread::spawn(move || {
            finished
                .send(frontend.lock_frontend().map(|state| state.phase))
                .unwrap();
        });
        assert_eq!(
            entered.recv_timeout(Duration::from_secs(15)).unwrap(),
            Phase::Preparing
        );
        let progress_at = now + PREPARATION_CAPACITY_WAIT_LIMIT - Duration::from_nanos(1);
        control.set_frontend_time(progress_at);
        assert_eq!(
            entered.recv_timeout(Duration::from_secs(15)).unwrap(),
            Phase::Preparing
        );
        {
            let mut state = control.lock().unwrap();
            if checkpoint_progress {
                state.report.completed_checkpoints += 1;
            } else {
                control.work_progress.fetch_add(1, Ordering::Relaxed);
            }
            control.notify_all();
        }
        assert_eq!(
            entered.recv_timeout(Duration::from_secs(15)).unwrap(),
            Phase::Preparing
        );
        // The original deadline has elapsed, but real progress established a
        // fresh bounded wait. Notifications alone are covered above.
        control.set_frontend_time(now + PREPARATION_CAPACITY_WAIT_LIMIT);
        assert_eq!(
            entered.recv_timeout(Duration::from_secs(15)).unwrap(),
            Phase::Preparing
        );
        assert!(matches!(
            completion.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        control.set_frontend_time(progress_at + PREPARATION_CAPACITY_WAIT_LIMIT);
        let result = completion.recv_timeout(Duration::from_secs(15)).unwrap();
        caller.join().unwrap();
        assert!(matches!(
            result,
            Err(HawDBError::BranchBusy {
                resource: "automatic checkpoint capacity"
            })
        ));
    }
}
