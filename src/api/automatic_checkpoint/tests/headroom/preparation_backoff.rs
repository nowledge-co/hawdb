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
fn pressured_writer_finishes_while_a_real_prepared_prefix_remains_paused() {
    let fixture = Fixture::new();
    let (mut db, suspension, next) = seeded(&fixture);
    let old = db.runtime.get().unwrap().store.snapshot_for_read();
    let control = db.runtime.checkpoint_control_for_test();
    let (sealed, observed) = mpsc::channel();
    let (resume, continuation) = mpsc::channel();
    control.lock().unwrap().prefix_seal_probe = Some(Arc::new(PrefixSealProbe {
        sealed,
        resume: Mutex::new(continuation),
    }));
    drop(suspension);
    observed.recv_timeout(Duration::from_secs(15)).unwrap();
    {
        let state = control.lock().unwrap();
        assert_eq!(state.phase, Phase::Preparing);
        assert!(needs_headroom(&state));
    }
    let (finished, completion) = mpsc::channel();
    let producer = std::thread::spawn(move || {
        let result = create(&mut db, next);
        finished.send((db, result)).unwrap();
    });
    // The owner stays paused for this complete observation. Waiting for its
    // entire preparation phase would fail even though the next write fits.
    let early = completion.recv_timeout(Duration::from_secs(1));
    control.lock().unwrap().prefix_seal_probe = None;
    resume.send(()).unwrap();
    producer.join().unwrap();
    let (mut db, result) = match early {
        Ok(value) => value,
        Err(error) => {
            drop(completion.recv_timeout(Duration::from_secs(15)).unwrap());
            panic!("writer waited for the complete Preparing phase: {error}");
        }
    };
    result.unwrap();
    assert!(
        db.automatic_checkpoint_report()
            .unwrap()
            .unwrap()
            .preparation_backoff_expirations
            > 0
    );
    assert_eq!(old.node_count_for_label(None), next as usize);
    drop(old);
    for id in next + 1..320 {
        create(&mut db, id).unwrap();
    }
    assert_values_and_reopen(db, &fixture);
}

#[test]
fn preparing_backoff_uses_one_deadline_across_repeated_notifications() {
    let fixture = Fixture::new();
    let (db, _suspension, _) = seeded(&fixture);
    let control = Arc::new(Control::default());
    {
        let mut state = control.lock().unwrap();
        state.phase = Phase::Preparing;
        state.debt = db.runtime.peek().unwrap().store.checkpoint_debt_snapshot();
        assert!(needs_headroom(&state));
    }
    let notifying = control.clone();
    let notifier = std::thread::spawn(move || {
        let end = Instant::now() + Duration::from_millis(500);
        while Instant::now() < end {
            notifying.changed.notify_all();
            std::thread::sleep(Duration::from_millis(1));
        }
        notifying.lock().unwrap().phase = Phase::Idle;
        notifying.changed.notify_all();
    });
    let started = Instant::now();
    drop(control.lock_frontend().unwrap());
    let elapsed = started.elapsed();
    notifier.join().unwrap();
    assert_eq!(
        control
            .lock()
            .unwrap()
            .report
            .preparation_backoff_expirations,
        1
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "notifications restarted the pressure backoff: {elapsed:?}"
    );
}

#[test]
fn preparation_deadline_cannot_bypass_capacity_or_publication_fences() {
    let fixture = Fixture::new();
    let (db, _suspension, _) = seeded(&fixture);
    let control = Arc::new(Control::default());
    let (waiting, entered) = mpsc::channel();
    {
        let mut state = control.lock().unwrap();
        state.phase = Phase::Preparing;
        let mut debt = db
            .runtime
            .peek()
            .unwrap()
            .store
            .checkpoint_debt_snapshot()
            .unwrap();
        debt.delta_bytes = debt.max_delta_bytes.unwrap() * 85 / 100;
        state.debt = Some(debt);
        state.frontend_wait_probe = Some(waiting);
        assert!(preparation_needs_capacity(&state));
    }
    let publication = control.request_publication();
    let frontend = control.clone();
    let (finished, completion) = mpsc::channel();
    let caller = std::thread::spawn(move || {
        drop(frontend.lock_frontend().unwrap());
        finished.send(()).unwrap();
    });
    assert_eq!(
        entered.recv_timeout(Duration::from_secs(1)).unwrap(),
        Phase::Preparing
    );
    let before_publication = completion.recv_timeout(Duration::from_millis(40));
    drop(publication);
    assert_eq!(
        entered.recv_timeout(Duration::from_secs(1)).unwrap(),
        Phase::Preparing
    );
    let before_capacity = completion.recv_timeout(Duration::from_millis(40));
    control.lock().unwrap().phase = Phase::Finalizing;
    control.changed.notify_all();
    assert_eq!(
        entered.recv_timeout(Duration::from_secs(1)).unwrap(),
        Phase::Finalizing
    );
    let before_finalizing = completion.recv_timeout(Duration::from_millis(40));
    control.lock().unwrap().phase = Phase::Idle;
    control.changed.notify_all();
    completion.recv_timeout(Duration::from_secs(1)).unwrap();
    caller.join().unwrap();
    for early in [before_publication, before_capacity, before_finalizing] {
        assert!(
            matches!(early, Err(mpsc::RecvTimeoutError::Timeout)),
            "preparation deadline bypassed a required fence: {early:?}"
        );
    }
    assert_eq!(
        control
            .lock()
            .unwrap()
            .report
            .preparation_backoff_expirations,
        0
    );
}

#[test]
fn cleanup_capacity_waits_before_admitting_another_writer() {
    let fixture = Fixture::new();
    let (db, _suspension, _) = seeded(&fixture);
    let debt = db
        .runtime
        .peek()
        .unwrap()
        .store
        .checkpoint_debt_snapshot()
        .unwrap();
    for phase in [Phase::Discarding, Phase::Releasing] {
        let control = Arc::new(Control::default());
        let (observation, observed) = mpsc::channel();
        {
            let mut state = control.lock().unwrap();
            state.phase = phase;
            let mut debt = debt;
            debt.delta_bytes = debt.max_delta_bytes.unwrap() * 85 / 100;
            state.debt = Some(debt);
            state.frontend_wait_probe = Some(observation.clone());
        }
        let caller_control = control.clone();
        let (finished, completion) = mpsc::channel();
        let caller = std::thread::spawn(move || {
            let result = caller_control.lock_frontend().map(drop);
            // Report a completed admission on the same observation channel.
            // A missing capacity fence therefore fails without depending on a
            // sleep or on whether the operating system scheduled this thread.
            observation.send(Phase::Idle).unwrap();
            finished.send(result).unwrap();
        });
        let first = observed.recv_timeout(Duration::from_secs(15));
        let early = completion.try_recv();
        {
            let mut state = control.lock().unwrap();
            state.phase = Phase::Idle;
        }
        control.changed.notify_all();
        let result = match early {
            Ok(result) => result,
            Err(mpsc::TryRecvError::Empty) => {
                completion.recv_timeout(Duration::from_secs(15)).unwrap()
            }
            Err(error) => panic!("writer disconnected: {error}"),
        };
        caller.join().unwrap();
        assert_eq!(
            first.unwrap(),
            phase,
            "capacity admission bypassed {phase:?}"
        );
        result.unwrap();
    }
}

#[test]
fn cleanup_with_capacity_still_admits_a_writer_before_resource_release() {
    let fixture = Fixture::new();
    let (db, _suspension, _) = seeded(&fixture);
    let debt = db
        .runtime
        .peek()
        .unwrap()
        .store
        .checkpoint_debt_snapshot()
        .unwrap();
    for phase in [Phase::Discarding, Phase::Releasing] {
        let control = Arc::new(Control::default());
        {
            let mut state = control.lock().unwrap();
            state.phase = phase;
            let mut debt = debt;
            debt.delta_bytes = debt.max_delta_bytes.unwrap() * 75 / 100;
            state.debt = Some(debt);
        }
        let caller_control = control.clone();
        let (finished, completion) = mpsc::channel();
        let caller = std::thread::spawn(move || {
            finished
                .send(caller_control.lock_frontend().map(drop))
                .unwrap();
        });
        let result = completion.recv_timeout(Duration::from_secs(1));
        let still_cleaning = control.lock().unwrap().phase;
        // Release even a faulty over-broad fence before checking the result.
        control.lock().unwrap().phase = Phase::Idle;
        control.changed.notify_all();
        caller.join().unwrap();
        result.unwrap().unwrap();
        assert_eq!(still_cleaning, phase);
    }
}
