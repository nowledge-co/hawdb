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
#[cfg(feature = "test-support")]
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

fn doctor_directory(path: &Path) -> std::path::PathBuf {
    active_storage_root(path).parent().unwrap().join("doctor")
}

fn admission_error(path: &Path, config: DatabaseConfig) -> HawDBError {
    let database = Database::open_with_config(path, config).unwrap();
    let error = database.commit_epoch().unwrap_err();
    assert_eq!(
        database
            .file_descriptor_metrics()
            .unwrap()
            .admitted_runtimes,
        0
    );
    error
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
    let doctor = doctor_directory(path);
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

fn child_wal_fixture(
    name: &str,
) -> (
    std::path::PathBuf,
    std::path::PathBuf,
    std::path::PathBuf,
    hawdb_storage::file_descriptors::FileOpenContext,
) {
    let path = unique_test_dir(name);
    let mut db = Database::open_with_config(&path, storage_crash_test_config()).unwrap();
    db.query("CREATE (:Memory {id: 'baseline'})").unwrap();
    let main = db.current_branch().unwrap().unwrap().info.id;
    let created = db
        .query_sql_with_params(
            "CREATE BRANCH NAME $1 FROM ID $2 AT REVISION $3 REQUEST KEY $4",
            &[
                Value::String("repair-child".into()),
                Value::Uuid(main),
                Value::Int(i64::try_from(db.commit_epoch().unwrap()).unwrap()),
                Value::String("repair-child-request".into()),
            ],
        )
        .unwrap();
    let Value::Uuid(child) = created.rows[0]["branch_id"] else {
        panic!("created branch UUID")
    };
    db.query_sql("USE BRANCH NAME 'repair-child'").unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .query_sql("CREATE TABLE repaired_rows (id BIGINT PRIMARY KEY, value TEXT)")
        .unwrap();
    transaction
        .query_sql("INSERT INTO repaired_rows (id, value) VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    transaction
        .query("CREATE (:Memory {id: 'child-a'})")
        .unwrap();
    transaction
        .query("CREATE (:Memory {id: 'child-b'})")
        .unwrap();
    transaction.commit().unwrap();
    db.query_sql("USE BRANCH main").unwrap();
    db.query("CREATE (:Memory {id: 'main-only'})").unwrap();
    // Keep only the registered domain, allowing the database's immutable
    // handle cache to close. Windows reopen then needs no alias discovery
    // across unrelated domains.
    let context = db.runtime.file_descriptor_context().unwrap();
    let limit = db.config().max_open_files;
    drop(db);
    let files =
        hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire_existing(&path, limit)
            .unwrap();
    assert_eq!(files.metrics().open, 0);
    assert_eq!(files.metrics().ownership_locks, 0);
    assert_eq!(files.metrics().admitted_runtimes, 0);
    drop(files);
    (
        path.clone(),
        path.join("branches").join(child.to_string()),
        path.join("branches").join(main.to_string()),
        context,
    )
}

fn child_wal_path(directory: &Path) -> std::path::PathBuf {
    let head =
        hawdb_storage::branch_head::read_branch_head(&directory.join("branch.head")).unwrap();
    directory.join(hawdb_storage::artifact_files::wal_generation_file(
        head.active_wal.generation,
    ))
}

fn bind_child_wal_prefix(directory: &Path, prefix: &[u8]) {
    let head_path = directory.join("branch.head");
    let mut head = hawdb_storage::branch_head::read_branch_head(&head_path).unwrap();
    let wal_path = child_wal_path(directory);
    let original = fs::read(&wal_path).unwrap();
    fs::write(&wal_path, prefix).unwrap();
    head.active_wal = hawdb_storage::branch_head::active_wal_identity_from_file(
        &wal_path,
        head.active_wal.generation,
        head.active_wal.replay_start_lsn,
        u64::MAX,
    )
    .unwrap();
    fs::write(&wal_path, original).unwrap();
    fs::write(head_path, head.encode().unwrap()).unwrap();
}

#[test]
fn branch_wal_repair_preserves_published_prefix_and_complete_schema_data_transactions() {
    use crate::{DatabaseDoctor, WalDoctorOptions};
    // Cover both a retained prefix beyond the selector's original header and
    // equality at the exact published-prefix boundary.
    for exact_published_prefix in [false, true] {
        let (path, child, main, _context) = child_wal_fixture("child_wal_repair");
        let wal_path = child_wal_path(&child);
        let valid = fs::read(&wal_path).unwrap();
        if exact_published_prefix {
            bind_child_wal_prefix(&child, &valid);
        }
        let head_before = fs::read(child.join("branch.head")).unwrap();
        let catalog_before = fs::read(path.join("branches/catalog.hawdb")).unwrap();
        let main_head_before = fs::read(main.join("branch.head")).unwrap();
        let main_wal_before = fs::read(child_wal_path(&main)).unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap()
            .write_all(b"partial")
            .unwrap();
        let damaged = fs::read(&wal_path).unwrap();
        let plan =
            DatabaseDoctor::plan_wal_tail_repair(&child, WalDoctorOptions::default()).unwrap();
        assert_eq!(plan.retained_wal_len, u64::try_from(valid.len()).unwrap());
        assert_eq!(plan.discarded_wal_tail_bytes, 7);
        assert_eq!(
            plan.manifest_sha256,
            hawdb_integrity::integrity_digest(&head_before)
                .sha256
                .to_string()
        );
        DatabaseDoctor::apply_wal_tail_repair(
            &child,
            &plan,
            plan.acknowledge_potential_data_loss(),
            WalDoctorOptions::default(),
        )
        .unwrap();
        assert_eq!(fs::read(&wal_path).unwrap(), valid);
        assert_eq!(fs::read(child.join("branch.head")).unwrap(), head_before);
        assert_eq!(
            fs::read(path.join("branches/catalog.hawdb")).unwrap(),
            catalog_before
        );
        assert_eq!(
            fs::read(main.join("branch.head")).unwrap(),
            main_head_before
        );
        assert_eq!(fs::read(child_wal_path(&main)).unwrap(), main_wal_before);
        let audit = fs::read_dir(child.join("doctor"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.to_string_lossy().ends_with(".repair.applied.json"))
            .unwrap();
        let record: serde_json::Value = serde_json::from_slice(&fs::read(audit).unwrap()).unwrap();
        assert_eq!(
            fs::read(
                child
                    .join("doctor/quarantine")
                    .join(record["quarantine_file"].as_str().unwrap())
            )
            .unwrap(),
            damaged
        );
        let mut db = Database::open(&path).unwrap();
        assert_eq!(
            count_query(&mut db, "MATCH (m:Memory) RETURN count(m) AS count"),
            2
        );
        db.query_sql("USE BRANCH NAME 'repair-child'").unwrap();
        assert_eq!(
            count_query(&mut db, "MATCH (m:Memory) RETURN count(m) AS count"),
            3
        );
        assert_eq!(
            db.query_sql("SELECT id, value FROM repaired_rows ORDER BY id")
                .unwrap()
                .rows,
            vec![
                BTreeMap::from([
                    ("id".into(), Value::Int(1)),
                    ("value".into(), Value::String("a".into()))
                ]),
                BTreeMap::from([
                    ("id".into(), Value::Int(2)),
                    ("value".into(), Value::String("b".into()))
                ]),
            ]
        );
        drop(db);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn branch_wal_repair_rejects_a_torn_record_inside_the_published_prefix() {
    use crate::{DatabaseDoctor, WalDoctorOptions};
    let (path, child, _, _context) = child_wal_fixture("published_torn_record");
    let wal_path = child_wal_path(&child);
    let valid = fs::read(&wal_path).unwrap();
    fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap()
        .write_all(b"partial")
        .unwrap();
    let damaged = fs::read(&wal_path).unwrap();
    // A checksum-valid selector covers one byte of a structurally torn frame.
    // Truncating to the last complete record would cross its published fence.
    bind_child_wal_prefix(&child, &damaged[..valid.len() + 1]);
    let head_before = fs::read(child.join("branch.head")).unwrap();
    let catalog_before = fs::read(path.join("branches/catalog.hawdb")).unwrap();
    let error =
        DatabaseDoctor::plan_wal_tail_repair(&child, WalDoctorOptions::default()).unwrap_err();
    assert!(matches!(error, HawDBError::StorageIntegrity(_)), "{error}");
    assert!(
        error
            .to_string()
            .contains("discard a published branch prefix"),
        "{error}"
    );
    assert_eq!(fs::read(wal_path).unwrap(), damaged);
    assert_eq!(fs::read(child.join("branch.head")).unwrap(), head_before);
    assert_eq!(
        fs::read(path.join("branches/catalog.hawdb")).unwrap(),
        catalog_before
    );
    assert!(!child.join("doctor").exists());
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn branch_wal_repair_rejects_a_missing_selector_without_modifying_evidence() {
    use crate::{DatabaseDoctor, WalDoctorOptions};
    let (path, child, _, _context) = child_wal_fixture("child_wal_repair_selector");
    let wal_path = child_wal_path(&child);
    fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap()
        .write_all(b"partial")
        .unwrap();
    let damaged = fs::read(&wal_path).unwrap();
    let catalog_before = fs::read(path.join("branches/catalog.hawdb")).unwrap();
    let head_path = child.join("branch.head");
    fs::remove_file(&head_path).unwrap();
    let error =
        DatabaseDoctor::plan_wal_tail_repair(&child, WalDoctorOptions::default()).unwrap_err();
    assert!(matches!(error, HawDBError::StorageIntegrity(_)), "{error}");
    assert!(
        error.to_string().contains("branch head is missing"),
        "{error}"
    );
    assert!(!head_path.exists());
    assert_eq!(fs::read(wal_path).unwrap(), damaged);
    assert_eq!(
        fs::read(path.join("branches/catalog.hawdb")).unwrap(),
        catalog_before
    );
    assert!(!child.join("doctor").exists());
    fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "test-support")]
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
            let epoch = db.commit_epoch().unwrap();
            set_wal_append_failpoint(WalAppendFailure::PartialWrite);
            let query =
                "CREATE (:Memory {id: 'failed-a'})-[:RELATED_TO]->(:Memory {id: 'failed-b'})";
            let error = if transaction {
                let mut tx = db.begin_transaction().unwrap();
                tx.query(query).unwrap();
                tx.commit().unwrap_err()
            } else {
                db.query(query).unwrap_err()
            };
            assert!(error.to_string().contains("rolled back"), "{error}");
            assert_eq!(fs::read(&wal).unwrap_or_default(), before);
            assert!(!db.storage_handle_poisoned().unwrap());
            assert_eq!(db.commit_epoch().unwrap(), epoch);
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

#[cfg(feature = "test-support")]
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

#[cfg(feature = "test-support")]
#[test]
fn partial_write_keeps_earlier_group_entries_flushable() {
    let path = unique_test_dir("wal_group_write_rollback");
    let mut db = Database::open(&path).unwrap();
    assert!(db.begin_wal_sync_group().unwrap());
    db.query("CREATE (:Memory {id: 'group-prefix'})").unwrap();
    let progress = db.runtime.get().unwrap().store.wal_sync_group_progress();
    let prefix = fs::read(active_wal_path(&path)).unwrap();
    set_wal_append_failpoint(WalAppendFailure::PartialWrite);
    assert!(db.query("CREATE (:Memory {id: 'failed'})").is_err());
    assert!(!db.storage_handle_poisoned().unwrap());
    assert_eq!(
        db.runtime.get().unwrap().store.wal_sync_group_progress(),
        progress
    );
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

#[cfg(feature = "test-support")]
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
        assert!(db.storage_handle_poisoned().unwrap());
        let damaged = fs::read(&wal).unwrap();
        assert!(damaged.len() as u64 > before);
        assert!(db.query("CREATE (:Memory {id: 'blocked'})").is_err());
        assert_eq!(fs::read(&wal).unwrap(), damaged);
        drop(db);
        let mut db = Database::open_with_config(&path, automatic_config()).unwrap();
        assert_eq!(
            db.storage_recovery_report().unwrap().torn_tail_repaired,
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
    assert!(admission_error(&path, storage_crash_test_config())
        .to_string()
        .contains("torn tail"));
    let mut db = Database::open_with_config(&path, automatic_config()).unwrap();
    let report = db.storage_recovery_report().unwrap();
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
    assert!(!db.storage_recovery_report().unwrap().torn_tail_repaired);
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
        admission_error(&path, config);
        assert_eq!(fs::read(active_wal_path(&path)).unwrap(), original);
        assert!(!doctor_directory(&path).exists());
    }
    let repaired = Database::open_with_config(&path, automatic_config()).unwrap();
    assert!(
        repaired
            .storage_recovery_report()
            .unwrap()
            .torn_tail_repaired
    );
    drop(repaired);
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
        let error = admission_error(&path, automatic_config());
        assert!(error.to_string().contains("corrupt"), "{error}");
        assert_eq!(fs::read(&wal).unwrap(), bytes);
        assert!(!doctor_directory(&path).exists());
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
    let error = admission_error(
        &path,
        DatabaseConfig {
            max_wal_quarantine_bytes: (first.len() + second.len() - 1) as u64,
            ..automatic_config()
        },
    );
    assert!(
        error.to_string().contains("quarantine byte limit"),
        "{error}"
    );
    assert_eq!(fs::read(active_wal_path(&path)).unwrap(), second);
    assert_repair_evidence(&path, &first);
    let repaired = Database::open_with_config(&path, automatic_config()).unwrap();
    assert!(
        repaired
            .storage_recovery_report()
            .unwrap()
            .torn_tail_repaired
    );
    drop(repaired);
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
    let error = admission_error(&path, automatic_config());
    assert!(error.to_string().contains("trailer"), "{error}");
    assert_eq!(fs::read(&wal).unwrap(), corrupt);
    assert!(!doctor_directory(&path).exists());
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
            assert!(admission_error(&path, storage_crash_test_config())
                .to_string()
                .contains("interrupted WAL doctor"));
        }
        let mut db = Database::open_with_config(&path, automatic_config()).unwrap();
        assert!(db.storage_recovery_report().unwrap().torn_tail_repaired);
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
