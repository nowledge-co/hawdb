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
use crate::{DatabaseConfig, StorageResidencyMode};
use std::io::Write;
use std::path::Path;
use std::time::Instant;

const CHILD: &str = "api::tests::concurrent_transactions::crash_recovery::concurrent_crash_child";
const PATH_ENV: &str = "HAWDB_TEST_CONCURRENT_CRASH_PATH";
const MODE_ENV: &str = "HAWDB_TEST_CONCURRENT_CRASH_MODE";
const OVERLAP_ENV: &str = "HAWDB_TEST_CONCURRENT_CRASH_OVERLAP";
const GRAPH: &str = "MATCH (m:CrashCounter) RETURN m.id AS id, m.value AS value ORDER BY id";

fn config(mode: StorageResidencyMode) -> DatabaseConfig {
    DatabaseConfig {
        storage_residency_mode: mode,
        ..DatabaseConfig::default()
    }
}

fn append_schema(table: &str) -> String {
    format!("CREATE TABLE {table} (stream_id TEXT NOT NULL, sequence BIGINT NOT NULL, payload TEXT NOT NULL) WITH (storage_mode = 'strict_append', partition_key = 'stream_id', order_key = 'sequence', generated_order = 'commit_sequence')")
}

fn stage_writer(
    db: &ConcurrentDatabase,
    writer: i64,
    overlap: bool,
) -> crate::ConcurrentDatabaseTransaction {
    let mut tx = db
        .begin_transaction(ConcurrentTransactionOptions::optimistic())
        .unwrap();
    let key = if overlap { 1 } else { writer };
    tx.query_with_params(
        "MATCH (m:CrashCounter) WHERE m.id = $id SET m.value = $value",
        &BTreeMap::from([
            ("id".into(), Value::Int(key)),
            ("value".into(), Value::Int(writer)),
        ]),
    )
    .unwrap();
    tx.query_sql_with_params(
        "INSERT INTO records (id, payload) VALUES ($1, $2)",
        &[
            Value::Int(writer),
            Value::String(format!("writer-{writer}")),
        ],
    )
    .unwrap();
    tx.query_sql_with_params(
        &format!("INSERT INTO events_{writer} (stream_id, payload) VALUES ($1, $2)"),
        &[
            Value::String("writer".into()),
            Value::String(format!("writer-{writer}")),
        ],
    )
    .unwrap();
    tx
}

fn wait_submitted(db: &ConcurrentDatabase, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while db.wal_group_commit_snapshot().unwrap().submitted_commits < expected {
        assert!(
            Instant::now() < deadline,
            "writer failed to reach commit queue"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn concurrent_crash_child() {
    let Some(path) = std::env::var_os(PATH_ENV) else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let mode = match std::env::var(MODE_ENV).unwrap().as_str() {
        "materialized" => StorageResidencyMode::Materialized,
        "out_of_core" => StorageResidencyMode::OutOfCore,
        other => panic!("unknown crash residency {other}"),
    };
    let overlap = std::env::var(OVERLAP_ENV).unwrap() == "true";
    let database = Database::open_with_config(&path, config(mode)).unwrap();
    let group = WalGroupCommitConfig::benchmark_candidate(
        NonZeroUsize::new(2).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::from_millis(5),
    )
    .unwrap();
    let db = ConcurrentDatabase::new_with_wal_group_commit(database, group);
    let mut old = db.begin_read_transaction().unwrap();
    let before = old.query(GRAPH).unwrap().rows;
    let first = stage_writer(&db, 1, overlap);
    let second = stage_writer(&db, 2, overlap);
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    db.set_group_commit_enqueue_gate(Arc::clone(&gate)).unwrap();
    // Both workspaces already share the same base. Queue order is deliberate,
    // so the recovery oracle checks an exact prefix, not any subset of writers.
    let first = std::thread::spawn(move || first.commit());
    wait_submitted(&db, 1);
    let second = std::thread::spawn(move || second.commit());
    wait_submitted(&db, 2);
    release_autocommit_reads(&gate);
    first.join().unwrap().unwrap();
    let second = second.join().unwrap();
    if overlap {
        assert!(second.unwrap_err().is_retryable_transaction_conflict());
    } else {
        second.unwrap();
    }
    let count = if overlap { 1 } else { 2 };
    let metrics = db.wal_group_commit_snapshot().unwrap();
    assert_eq!(metrics.submitted_commits, 2);
    assert_eq!(metrics.completed_commits, count);
    assert_eq!(metrics.shared_sync_count, 1);
    assert_eq!(metrics.grouped_wal_entries, count);
    assert_eq!(old.query(GRAPH).unwrap().rows, before);
    assert_eq!(
        old.query_sql("SELECT id FROM records ORDER BY id")
            .unwrap()
            .rows,
        vec![BTreeMap::from([("id".into(), Value::Int(0))])]
    );
    // Persist evidence only after actual result delivery. Checkpoint crashes
    // must retain these acknowledgements as well as the earlier baseline WAL.
    let mut evidence = std::fs::File::create(path.join("acknowledged.txt")).unwrap();
    writeln!(evidence, "{count}").unwrap();
    evidence.sync_all().unwrap();
    db.checkpoint().unwrap();
    panic!("configured process crash point was not reached");
}

fn fixture(path: &Path, mode: StorageResidencyMode) -> u64 {
    let mut db = Database::open_with_config(path, config(mode)).unwrap();
    for id in 0..3 {
        db.query(&format!("CREATE (:CrashCounter {{id: {id}, value: 0}})"))
            .unwrap();
    }
    db.query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, payload TEXT)")
        .unwrap();
    for table in ["events_1", "events_2"] {
        db.query_sql(&append_schema(table)).unwrap();
    }
    db.checkpoint().unwrap();
    // Acknowledged mixed-domain data remains in WAL before the child starts.
    let mut tx = db.begin_transaction();
    tx.query("MATCH (m:CrashCounter) WHERE m.id = 0 SET m.value = 77")
        .unwrap();
    tx.query_sql("INSERT INTO records (id, payload) VALUES (0, 'baseline')")
        .unwrap();
    for table in ["events_1", "events_2"] {
        tx.query_sql(&format!(
            "INSERT INTO {table} (stream_id, payload) VALUES ('baseline', 'baseline')"
        ))
        .unwrap();
    }
    tx.commit().unwrap();
    db.commit_epoch()
}

fn recovered_prefix(db: &mut Database, baseline: u64, maximum: u64) -> u64 {
    let count = db.commit_epoch().checked_sub(baseline).unwrap();
    assert!(
        count <= maximum,
        "replayed an unappended or rejected transaction"
    );
    let expected = (0..3)
        .map(|id| {
            BTreeMap::from([
                ("id".into(), Value::Int(id)),
                (
                    "value".into(),
                    Value::Int(if id == 0 {
                        77
                    } else if id as u64 <= count {
                        id
                    } else {
                        0
                    }),
                ),
            ])
        })
        .collect::<Vec<_>>();
    assert_eq!(db.query(GRAPH).unwrap().rows, expected);
    let expected = (0..=count)
        .map(|id| {
            BTreeMap::from([
                ("id".into(), Value::Int(id as i64)),
                (
                    "payload".into(),
                    Value::String(if id == 0 {
                        "baseline".into()
                    } else {
                        format!("writer-{id}")
                    }),
                ),
            ])
        })
        .collect::<Vec<_>>();
    assert_eq!(
        db.query_sql("SELECT id, payload FROM records ORDER BY id")
            .unwrap()
            .rows,
        expected
    );
    for writer in 1..=2 {
        let query = format!("SELECT sequence, payload FROM events_{writer} WHERE stream_id = $1 ORDER BY sequence LIMIT 10");
        let baseline = db
            .query_sql_with_params(&query, &[Value::String("baseline".into())])
            .unwrap()
            .rows;
        assert_eq!(
            baseline,
            vec![BTreeMap::from([
                ("sequence".into(), Value::Int(1)),
                ("payload".into(), Value::String("baseline".into())),
            ])]
        );
        let rows = db
            .query_sql_with_params(&query, &[Value::String("writer".into())])
            .unwrap()
            .rows;
        let expected = if writer <= count {
            vec![BTreeMap::from([
                ("sequence".into(), Value::Int(2)),
                ("payload".into(), Value::String(format!("writer-{writer}"))),
            ])]
        } else {
            vec![]
        };
        assert_eq!(rows, expected);
    }
    assert_eq!(
        db.storage_recovery_report().recovered_commit_epoch,
        baseline + count
    );
    count
}

#[test]
fn subprocess_concurrent_crash_recovers_mixed_transactions_as_serial_prefixes() {
    // Bounds concern complete records after process exit, not power-loss media.
    let stages = [
        ("before_wal_append", 0, 0, false),
        ("after_wal_append", 0, 1, false),
        ("after_first_group_task", 0, 1, false),
        ("before_group_sync", 0, 2, false),
        ("after_wal_sync", 2, 2, false),
        ("during_checkpoint_publication", 2, 2, true),
        ("after_manifest_publication", 2, 2, true),
    ];
    for (mode_name, mode) in [
        ("materialized", StorageResidencyMode::Materialized),
        ("out_of_core", StorageResidencyMode::OutOfCore),
    ] {
        for overlap in [false, true] {
            for (stage, minimum, maximum, acknowledged) in stages {
                let path = super::super::unique_test_dir(&format!(
                    "concurrent_crash_{mode_name}_{overlap}_{stage}"
                ));
                let baseline = fixture(&path, mode);
                let group_point = matches!(stage, "after_first_group_task" | "before_group_sync");
                let log = std::fs::File::create(path.join("child.log")).unwrap();
                let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", CHILD, "--nocapture"])
                    .env(PATH_ENV, &path)
                    .env(MODE_ENV, mode_name)
                    .env(OVERLAP_ENV, overlap.to_string())
                    .env(
                        "HAWDB_TEST_PROCESS_CRASH_POINT",
                        if group_point { "" } else { stage },
                    )
                    .env(
                        "HAWDB_TEST_GROUP_COMMIT_CRASH_POINT",
                        if group_point { stage } else { "" },
                    )
                    .stdout(log.try_clone().unwrap())
                    .stderr(log)
                    .spawn()
                    .unwrap();
                let deadline = Instant::now() + Duration::from_secs(60);
                let status = loop {
                    if let Some(status) = child.try_wait().unwrap() {
                        break status;
                    }
                    if Instant::now() >= deadline {
                        child.kill().unwrap();
                        child.wait().unwrap();
                        panic!("crash child did not terminate: {}", path.display());
                    }
                    std::thread::sleep(Duration::from_millis(10));
                };
                assert_eq!(
                    status.code(),
                    Some(86),
                    "{mode_name}/{overlap}/{stage}: {}",
                    std::fs::read_to_string(path.join("child.log")).unwrap()
                );
                let accepted = if overlap { 1 } else { 2 };
                let mut db = Database::open_with_config(&path, config(mode)).unwrap();
                let count = recovered_prefix(&mut db, baseline, maximum.min(accepted));
                assert!(
                    count >= minimum.min(accepted),
                    "lost durable prefix after {stage}"
                );
                let evidence = path.join("acknowledged.txt");
                if acknowledged {
                    assert_eq!(
                        std::fs::read_to_string(evidence).unwrap(),
                        format!("{accepted}\n")
                    );
                } else {
                    assert!(
                        !evidence.exists(),
                        "crash point ran after unexpected acknowledgement"
                    );
                }
                // Restart discards old process-local stamps but the next pair
                // must still establish first-committer-wins and durable data.
                let db = db.into_concurrent();
                let mut first = db
                    .begin_transaction(ConcurrentTransactionOptions::optimistic())
                    .unwrap();
                let mut stale = db
                    .begin_transaction(ConcurrentTransactionOptions::optimistic())
                    .unwrap();
                for tx in [&mut first, &mut stale] {
                    tx.query("MATCH (m:CrashCounter) WHERE m.id = 0 SET m.value = 99")
                        .unwrap();
                }
                first.commit().unwrap();
                let wal_path = super::super::active_wal_path(&path);
                let wal = std::fs::read(&wal_path).unwrap();
                assert!(stale
                    .commit()
                    .unwrap_err()
                    .is_retryable_transaction_conflict());
                assert_eq!(std::fs::read(wal_path).unwrap(), wal);
                drop(db);
                let mut reopened = Database::open_with_config(&path, config(mode)).unwrap();
                assert_eq!(reopened.commit_epoch(), baseline + count + 1);
                assert_eq!(
                    reopened
                        .query("MATCH (m:CrashCounter) WHERE m.id = 0 RETURN m.value AS value")
                        .unwrap()
                        .rows,
                    vec![BTreeMap::from([("value".into(), Value::Int(99))])]
                );
                drop(reopened);
                std::fs::remove_dir_all(path).unwrap();
            }
        }
    }
}
