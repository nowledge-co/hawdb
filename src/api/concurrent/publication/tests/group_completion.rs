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
use crate::DatabaseConfig;

fn verify_group_rows(snapshot: &mut DatabaseReadTransaction, ids: &[i64]) {
    let graph = snapshot
        .query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
        .unwrap();
    let relational = snapshot
        .query_sql("SELECT id, body FROM records ORDER BY id")
        .unwrap();
    for output in [graph, relational] {
        assert_eq!(output.rows.len(), ids.len());
        for (row, id) in output.rows.iter().zip(ids) {
            assert_eq!(row["id"], Value::Int(*id));
            assert_eq!(row["body"], Value::String("group-body".into()));
        }
    }
}

#[test]
fn completed_group_wakes_automatic_checkpoint_without_another_frontend_access() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-group-idle-checkpoint-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut database = Database::open_with_config(
        &path,
        DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let governor = hawdb_qos::RuntimeGovernor::new(
        hawdb_qos::RuntimeGovernorConfig {
            cpu_slot_limit: Some(NonZeroUsize::MIN),
            background_task_limit: Some(NonZeroUsize::MIN),
            memory_budget_bytes: Some(512 * 1024 * 1024),
            ..hawdb_qos::RuntimeGovernorConfig::shared_host()
        },
        hawdb_qos::RuntimeResourceSnapshot::from_parts(
            hawdb_qos::RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            hawdb_qos::RuntimeMemorySnapshot::from_limits(
                Some(1 << 30),
                Some(1 << 30),
                None,
                None,
                None,
            ),
        ),
        hawdb_qos::IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    database.set_runtime_governor(governor.clone());
    let suspension = database.runtime.suspend_automatic_checkpoint().unwrap();
    database
        .query("CREATE (:Memory {id: 1, body: 'group-body'})")
        .unwrap();
    database
        .query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();
    database
        .query_sql("INSERT INTO records (id, body) VALUES (1, 'group-body')")
        .unwrap();
    database.checkpoint().unwrap();
    let mut old = database.begin_read_transaction().unwrap();
    let before = old.commit_epoch();
    let control = database.runtime.checkpoint_control_for_test();
    let group = WalGroupCommitConfig::benchmark_candidate(
        NonZeroUsize::MIN,
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::ZERO,
    )
    .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(database, group);
    let (staged, stages) = mpsc::channel();
    let (release, releases) = mpsc::channel();
    let writer_db = db.clone();
    let writer = std::thread::spawn(move || {
        writer_db.inner.commits.execute_grouped(move |database| {
            let mut tx = database.begin_transaction()?;
            tx.query("CREATE (:Memory {id: 2, body: 'group-body'})")?;
            tx.query_sql("INSERT INTO records (id, body) VALUES (2, 'group-body')")?;
            let result = tx.commit()?;
            assert!(database.runtime.get()?.store.wal_sync_group_active());
            staged.send(()).unwrap();
            releases.recv_timeout(Duration::from_secs(15)).unwrap();
            Ok(result)
        })
    });
    let staged = stages.recv_timeout(Duration::from_secs(15));
    drop(suspension);
    // The data is due by age while the group is still unacknowledged. Observe
    // only the owner control, so no frontend guard can finish its sync group.
    std::thread::sleep(Duration::from_millis(50));
    let premature = control.report().unwrap().unwrap().completed_checkpoints;
    release.send(()).unwrap();
    let committed = writer.join().unwrap();
    staged.unwrap();
    assert_eq!(premature, 0);
    committed.unwrap();

    // No Database or ConcurrentDatabase access is permitted in this wait.
    // Group completion itself must submit and wake the idle maintenance owner.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let report = control.report().unwrap().unwrap();
        if report.completed_checkpoints == 1 {
            assert!(report.waiting_for_handoff);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "idle group checkpoint stalled: {report:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    verify_group_rows(&mut old, &[1]);
    let mut current = db.begin_read_transaction().unwrap();
    assert_eq!(current.commit_epoch(), before + 1);
    verify_group_rows(&mut current, &[1, 2]);
    drop(current);
    drop(old);
    db.checkpoint().unwrap();
    drop(db);
    drop(control);
    let resources = governor.snapshot();
    assert_eq!(resources.active_cpu_slots, 0);
    assert_eq!(resources.active_background_tasks, 0);
    assert_eq!(resources.active_background_io_slots, 0);
    assert_eq!(resources.admitted_memory_bytes, 0);
    let reopened = Database::open(&path).unwrap();
    verify_group_rows(&mut reopened.begin_read_transaction().unwrap(), &[1, 2]);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}
