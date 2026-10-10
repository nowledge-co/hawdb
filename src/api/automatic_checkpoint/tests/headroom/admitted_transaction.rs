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
use crate::{ConcurrentTransactionOptions, QueryRows, Value};
use std::collections::BTreeMap;

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
fn concurrent_admitted_commit_releases_pressure_wait_after_retirement_denial() {
    let fixture = Fixture::new();
    let (mut db, suspension, mut next) = seeded(&fixture);
    let initial = next;
    let mut old = db.begin_read_transaction().unwrap();
    let control = db.runtime.checkpoint_control_for_test();
    let governor = retirement_governor();
    db.set_runtime_governor(governor.clone());
    let (probe, retirement_started, resume) = pause();
    control.lock().unwrap().retirement_probe = Some(probe);
    drop(suspension);
    wait(&control, |state| state.phase == Phase::Handoff);
    let foreground = governor
        .try_admit(
            RuntimeWorkRequest::mutation(
                hawdb_qos::RuntimeWorkPriority::Foreground,
                16 * 1024 * 1024,
            )
            .with_result_bytes(1024 * 1024),
        )
        .unwrap();
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
    let (waiting, blocked) = mpsc::channel();
    control.lock().unwrap().frontend_wait_probe = Some(waiting);
    let (completed, result) = mpsc::channel();
    let (begun, started) = mpsc::channel();
    let producer = std::thread::spawn(move || {
        let concurrent = db.into_concurrent();
        let committed = (|| -> Result<()> {
            let mut transaction = concurrent.begin_admitted_transaction(
                ConcurrentTransactionOptions::optimistic(),
                foreground,
                RuntimeTaskContext::default(),
            )?;
            begun.send(()).unwrap();
            transaction.query_with_params(
                "CREATE (:Memory {id: $id, body: $body})",
                &BTreeMap::from([
                    ("id".into(), Value::Int(next)),
                    ("body".into(), Value::String("b".repeat(512))),
                ]),
            )?;
            transaction.commit()?;
            Ok(())
        })();
        completed.send((concurrent, committed)).unwrap();
    });
    assert_eq!(
        blocked.recv_timeout(Duration::from_secs(15)).unwrap(),
        Phase::Retiring
    );
    let response = result.recv_timeout(Duration::from_secs(15));
    let progressed = response.is_ok();
    let began_before_rescue = started.try_recv().is_ok();
    let rescue = if !progressed {
        control.lock().unwrap().retirement_probe.take();
        Some(control.suspend().unwrap())
    } else {
        None
    };
    let (db, committed) =
        response.unwrap_or_else(|_| result.recv_timeout(Duration::from_secs(15)).unwrap());
    producer.join().unwrap();
    committed.unwrap();
    assert_eq!(std::fs::read(&base).unwrap(), bytes);
    assert_rows(
        &old.query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
            .unwrap()
            .rows,
        initial,
    );
    let finishing = if progressed {
        retirement_started
            .recv_timeout(Duration::from_secs(15))
            .unwrap();
        assert_eq!(governor.snapshot().active_foreground_tasks, 0);
        assert_eq!(governor.snapshot().active_cpu_slots, 1);
        assert_eq!(governor.snapshot().active_background_tasks, 1);
        assert_eq!(governor.snapshot().active_background_io_slots, 1);
        let finishing = control.suspend().unwrap();
        resume.send(()).unwrap();
        Some(finishing)
    } else {
        rescue
    };
    wait(&control, |state| {
        state.phase == Phase::Idle && state.retired.is_none()
    });
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
    assert!(
        began_before_rescue,
        "the original pressure wait must occur after actual transaction admission"
    );
    assert!(
        progressed,
        "admitted commit waited for retirement while retaining its only CPU"
    );
}
