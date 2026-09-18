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
use crate::store::{set_wal_append_failpoint, WalAppendFailure};
use crate::HawDBError;
use std::fs;
use std::path::Path;

fn automatic_config() -> DatabaseConfig {
    DatabaseConfig {
        recovery_mode: RecoveryMode::AutoRepairTornTail,
        ..storage_crash_test_config()
    }
}

fn seed_database(path: &Path) {
    let mut db = Database::open_with_config(path, storage_crash_test_config()).unwrap();
    db.query("CREATE (:Memory {id: 'baseline'})").unwrap();
    db.checkpoint().unwrap();
    db.query("CREATE (:Memory {id: 'acknowledged'})").unwrap();
}

fn append_torn_tail(path: &Path) -> Vec<u8> {
    fs::OpenOptions::new()
        .append(true)
        .open(active_wal_path(path))
        .unwrap()
        .write_all(b"partial")
        .unwrap();
    fs::read(active_wal_path(path)).unwrap()
}

fn assert_repair_evidence(path: &Path, original: &[u8]) {
    let doctor = path.join("doctor");
    let records = fs::read_dir(&doctor)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.to_string_lossy().ends_with(".repair.applied.json"))
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 1);
    assert!(!fs::read_dir(&doctor).unwrap().any(|entry| {
        entry
            .unwrap()
            .path()
            .to_string_lossy()
            .ends_with(".repair.pending.json")
    }));
    let audit: serde_json::Value = serde_json::from_slice(&fs::read(&records[0]).unwrap()).unwrap();
    assert_eq!(audit["protocol"], "hawdb-wal-doctor-repair-v1");
    assert_eq!(audit["state"], "applied");
    let quarantine = doctor
        .join("quarantine")
        .join(audit["quarantine_file"].as_str().unwrap());
    assert_eq!(fs::read(quarantine).unwrap(), original);
}

#[test]
fn partial_write_rolls_back_and_next_commit_succeeds() {
    for baseline in [false, true] {
        for transaction in [false, true] {
            let path = unique_test_dir("wal_append_rollback");
            if baseline {
                seed_database(&path);
            }
            let mut db = Database::open(&path).unwrap();
            let wal = active_wal_path(&path);
            let before = fs::read(&wal).unwrap_or_default();
            let epoch = db.commit_epoch();
            set_wal_append_failpoint(WalAppendFailure::PartialWrite);
            let query =
                "CREATE (:Memory {id: 'failed-a'})-[:RELATED_TO]->(:Memory {id: 'failed-b'})";
            let error = if transaction {
                let mut tx = db.begin_transaction();
                tx.query(query).unwrap();
                tx.commit().unwrap_err()
            } else {
                db.query(query).unwrap_err()
            };
            assert!(error.to_string().contains("rolled back"), "{error}");
            assert_eq!(fs::read(&wal).unwrap_or_default(), before);
            assert!(!db.storage_handle_poisoned());
            assert_eq!(db.commit_epoch(), epoch);
            db.query("CREATE (:Memory {id: 'retry'})").unwrap();
            drop(db);
            let mut db = Database::open(&path).unwrap();
            assert_eq!(
                count_query(
                    &mut db,
                    "MATCH (m:Memory {id: 'retry'}) RETURN count(m) AS count"
                ),
                1
            );
            assert_eq!(
                count_query(
                    &mut db,
                    "MATCH (m:Memory {id: 'failed-a'}) RETURN count(m) AS count"
                ),
                0
            );
            assert_eq!(
                count_query(&mut db, "MATCH (m:Memory) RETURN count(m) AS count"),
                if baseline { 3 } else { 1 }
            );
            drop(db);
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn first_failed_append_can_reopen_without_repair() {
    let path = unique_test_dir("first_wal_write_failure");
    let mut db = Database::open(&path).unwrap();
    set_wal_append_failpoint(WalAppendFailure::PartialWrite);
    assert!(db.query("CREATE (:Memory {id: 'failed'})").is_err());
    drop(db);
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'retry'})").unwrap();
    drop(db);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn partial_write_keeps_earlier_group_entries_flushable() {
    let path = unique_test_dir("wal_group_write_rollback");
    let mut db = Database::open(&path).unwrap();
    assert!(db.begin_wal_sync_group().unwrap());
    db.query("CREATE (:Memory {id: 'group-prefix'})").unwrap();
    let progress = db.wal_sync_group_progress();
    let prefix = fs::read(active_wal_path(&path)).unwrap();
    set_wal_append_failpoint(WalAppendFailure::PartialWrite);
    assert!(db.query("CREATE (:Memory {id: 'failed'})").is_err());
    assert!(!db.storage_handle_poisoned());
    assert_eq!(db.wal_sync_group_progress(), progress);
    assert_eq!(fs::read(active_wal_path(&path)).unwrap(), prefix);
    let flush = db.finish_wal_sync_group().unwrap();
    assert_eq!(flush.entry_count, progress.entry_count);
    assert!(flush.fsync_performed);
    drop(db);
    let mut db = Database::open(&path).unwrap();
    assert_eq!(
        count_query(&mut db, "MATCH (m:Memory) RETURN count(m) AS count"),
        1
    );
    db.query("CREATE (:Memory {id: 'after-flush'})").unwrap();
    drop(db);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn rollback_and_sync_failures_poison_the_handle() {
    for failure in [WalAppendFailure::Rollback, WalAppendFailure::Sync] {
        let path = unique_test_dir("wal_append_uncertain");
        seed_database(&path);
        let mut db = Database::open(&path).unwrap();
        let wal = active_wal_path(&path);
        let before = fs::metadata(&wal).unwrap().len();
        set_wal_append_failpoint(failure);
        let error = db.query("CREATE (:Memory {id: 'uncertain'})").unwrap_err();
        assert!(matches!(error, HawDBError::StorageIntegrity(_)), "{error}");
        assert!(db.storage_handle_poisoned());
        let damaged = fs::read(&wal).unwrap();
        assert!(damaged.len() as u64 > before);
        assert!(db.query("CREATE (:Memory {id: 'blocked'})").is_err());
        assert_eq!(fs::read(&wal).unwrap(), damaged);
        drop(db);
        let mut db = Database::open_with_config(&path, automatic_config()).unwrap();
        assert_eq!(
            db.storage_recovery_report().torn_tail_repaired,
            failure == WalAppendFailure::Rollback
        );
        assert_eq!(
            count_query(
                &mut db,
                "MATCH (m:Memory {id: 'acknowledged'}) RETURN count(m) AS count"
            ),
            1
        );
        assert_eq!(
            count_query(
                &mut db,
                "MATCH (m:Memory {id: 'uncertain'}) RETURN count(m) AS count"
            ),
            u64::from(failure == WalAppendFailure::Sync)
        );
        drop(db);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn automatic_repair_preserves_quarantine_and_acknowledged_commits() {
    let path = unique_test_dir("automatic_wal_tail");
    seed_database(&path);
    let original = append_torn_tail(&path);
    assert!(Database::open(&path).is_err());
    let mut db = Database::open_with_config(&path, automatic_config()).unwrap();
    let report = db.storage_recovery_report();
    assert_eq!(report.recovery_mode, RecoveryMode::AutoRepairTornTail);
    assert!(report.torn_tail_repaired);
    assert!(!report.torn_tail_ignored);
    assert_eq!(report.discarded_wal_tail_bytes, 7);
    assert!(report.torn_tail_reason.is_some());
    assert_repair_evidence(&path, &original);
    assert_eq!(
        count_query(&mut db, "MATCH (m:Memory) RETURN count(m) AS count"),
        2
    );
    db.query("CREATE (:Memory {id: 'after-repair'})").unwrap();
    drop(db);
    let mut db = Database::open(&path).unwrap();
    assert!(!db.storage_recovery_report().torn_tail_repaired);
    assert_eq!(
        count_query(&mut db, "MATCH (m:Memory) RETURN count(m) AS count"),
        3
    );
    drop(db);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn automatic_repair_rejects_read_only_and_insufficient_evidence_budget() {
    let path = unique_test_dir("automatic_wal_tail_boundaries");
    seed_database(&path);
    let original = append_torn_tail(&path);
    for config in [
        DatabaseConfig {
            read_only: true,
            ..automatic_config()
        },
        DatabaseConfig {
            max_wal_quarantine_bytes: 1,
            ..automatic_config()
        },
        DatabaseConfig {
            max_wal_replay_entries: Some(0),
            ..automatic_config()
        },
    ] {
        assert!(Database::open_with_config(&path, config).is_err());
        assert_eq!(fs::read(active_wal_path(&path)).unwrap(), original);
        assert!(!path.join("doctor").exists());
    }
    drop(Database::open_with_config(&path, automatic_config()).unwrap());
    assert_repair_evidence(&path, &original);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn automatic_repair_rejects_complete_corruption() {
    for orphan in [false, true] {
        let path = unique_test_dir("automatic_wal_corruption");
        seed_database(&path);
        let wal = active_wal_path(&path);
        let mut bytes = fs::read(&wal).unwrap();
        if orphan {
            let generation = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
            let mut hasher = hawdb_integrity::Crc32cHasher::new();
            hasher.update(&[3]);
            hasher.update(&generation.to_le_bytes());
            let crc = hasher
                .finish_u32()
                .rotate_right(15)
                .wrapping_add(0xa282_ead8);
            bytes.extend_from_slice(&crc.to_le_bytes());
            bytes.extend_from_slice(&0u16.to_le_bytes());
            bytes.push(3);
            bytes.extend_from_slice(&generation.to_le_bytes());
        } else {
            *bytes.last_mut().unwrap() ^= 0xff;
        }
        fs::write(&wal, &bytes).unwrap();
        let error = Database::open_with_config(&path, automatic_config()).unwrap_err();
        assert!(error.to_string().contains("corrupt"), "{error}");
        assert_eq!(fs::read(&wal).unwrap(), bytes);
        assert!(!path.join("doctor").exists());
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn automatic_repair_budget_preserves_prior_audit_evidence() {
    let path = unique_test_dir("automatic_wal_cumulative_budget");
    seed_database(&path);
    let first = append_torn_tail(&path);
    let mut db = Database::open_with_config(&path, automatic_config()).unwrap();
    db.query("CREATE (:Memory {id: 'later'})").unwrap();
    drop(db);
    let second = append_torn_tail(&path);
    let error = Database::open_with_config(
        &path,
        DatabaseConfig {
            max_wal_quarantine_bytes: (first.len() + second.len() - 1) as u64,
            ..automatic_config()
        },
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("quarantine byte limit"),
        "{error}"
    );
    assert_eq!(fs::read(active_wal_path(&path)).unwrap(), second);
    assert_repair_evidence(&path, &first);
    drop(Database::open_with_config(&path, automatic_config()).unwrap());
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn automatic_repair_rejects_nonzero_block_trailers() {
    use hawdb_storage::wal::binary::encode_binary_wal_record;
    use hawdb_storage::wal::frame::{
        encode_binary_wal_header, frame_binary_wal_record, WAL_BLOCK_BYTES,
        WAL_FRAGMENT_HEADER_BYTES,
    };
    use hawdb_storage::wal::{WalEntry, WalOp};

    let path = unique_test_dir("automatic_wal_trailer_corruption");
    seed_database(&path);
    let wal = active_wal_path(&path);
    let bytes = fs::read(&wal).unwrap();
    let generation = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let lsn = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let encode = |padding: usize| {
        encode_binary_wal_record(
            &WalEntry {
                lsn,
                op: WalOp::CreateNode {
                    id: hawdb_storage::NodeId(100),
                    label: "Memory".to_string(),
                    properties: BTreeMap::from([(
                        "payload".to_string(),
                        Value::String("x".repeat(padding)),
                    )]),
                },
            },
            2,
        )
        .unwrap()
    };
    let target_len = WAL_BLOCK_BYTES - WAL_FRAGMENT_HEADER_BYTES - 7;
    let payload = encode(target_len - (encode(16 * 1024).len() - 16 * 1024));
    assert_eq!(payload.len(), target_len);
    let mut corrupt = encode_binary_wal_header(generation, lsn);
    corrupt.extend(frame_binary_wal_record(generation, &payload, 0));
    corrupt.extend([0, 0, 0xff, 0, 0, 0, 0]);
    fs::write(&wal, &corrupt).unwrap();
    let error = Database::open_with_config(&path, automatic_config()).unwrap_err();
    assert!(error.to_string().contains("trailer"), "{error}");
    assert_eq!(fs::read(&wal).unwrap(), corrupt);
    assert!(!path.join("doctor").exists());
    fs::remove_dir_all(path).unwrap();
}

const CRASH_CHILD: &str = "api::tests::storage_recovery::wal_tail::crash_child";

#[test]
fn crash_child() {
    let Some(path) = std::env::var_os("HAWDB_TEST_AUTO_REPAIR_PATH") else {
        return;
    };
    let mut db = Database::open_with_config(path, automatic_config()).unwrap();
    db.query("CREATE (:Memory {id: 'unacknowledged-a'})-[:RELATED_TO]->(:Memory {id: 'unacknowledged-b'})").unwrap();
    panic!("crash failpoint did not terminate the child");
}

#[test]
fn subprocess_partial_append_and_interrupted_automatic_repair_recover() {
    for repair_stage in [
        None,
        Some("after_wal_repair_prepare"),
        Some("after_wal_repair_truncate"),
    ] {
        let path = unique_test_dir("automatic_wal_crash");
        seed_database(&path);
        let child = |stage: &str| {
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", CRASH_CHILD, "--nocapture"])
                .env("HAWDB_TEST_AUTO_REPAIR_PATH", &path)
                .env("HAWDB_TEST_PROCESS_CRASH_POINT", stage)
                .status()
                .unwrap()
        };
        assert_eq!(child("during_wal_append").code(), Some(86));
        let original = fs::read(active_wal_path(&path)).unwrap();
        if let Some(stage) = repair_stage {
            assert_eq!(child(stage).code(), Some(86));
            assert!(Database::open(&path)
                .unwrap_err()
                .to_string()
                .contains("interrupted WAL doctor"));
        }
        let mut db = Database::open_with_config(&path, automatic_config()).unwrap();
        assert!(db.storage_recovery_report().torn_tail_repaired);
        assert_eq!(
            count_query(&mut db, "MATCH (m:Memory) RETURN count(m) AS count"),
            2
        );
        assert_eq!(
            count_query(
                &mut db,
                "MATCH ()-[r:RELATED_TO]->() RETURN count(r) AS count"
            ),
            0
        );
        assert_repair_evidence(&path, &original);
        db.query("CREATE (:Memory {id: 'after-recovery'})").unwrap();
        drop(db);
        drop(Database::open(&path).unwrap());
        fs::remove_dir_all(path).unwrap();
    }
}
