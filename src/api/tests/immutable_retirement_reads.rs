// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Reads beside a checkpoint whose immutable-object retirement is paused
//! (#1959): they wait at most for that retirement, succeed, and never poison
//! the handle.

use super::*;
use hawdb_storage::RetirementPause;
use std::sync::mpsc;
use std::time::Duration;

const LIMIT: Duration = Duration::from_secs(30);

fn out_of_core(path: &std::path::Path) -> Database {
    Database::open_with_config(
        path,
        DatabaseConfig {
            storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
            automatic_checkpoint_max_age: Duration::from_secs(3600),
            ..DatabaseConfig::default()
        },
    )
    .unwrap()
}

/// A database with two superseded checkpoint generations and rows in all.
fn populated(path: &std::path::Path) -> Database {
    let mut db = out_of_core(path);
    db.query_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    for round in 0..4 {
        write_round(&mut db, round);
        db.checkpoint().unwrap();
    }
    db
}

fn write_round(db: &mut Database, round: i64) {
    for id in round * 8..(round + 1) * 8 {
        db.query_sql(&format!("INSERT INTO items VALUES ({id}, 'body-{id}')"))
            .unwrap();
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, body: '{}'}})",
            "b".repeat(512)
        ))
        .unwrap();
    }
}

pub(in crate::api) fn project(
    db: &Database,
) -> hawdb_storage::file_descriptors::ProjectFileDescriptors {
    hawdb_storage::file_descriptors::ProjectFileDescriptors::from_open_context(
        &db.runtime.file_descriptor_context().unwrap(),
    )
}

/// Drops every idle cached handle of the project.
pub(in crate::api) fn evict(project: &hawdb_storage::file_descriptors::ProjectFileDescriptors) {
    let mut held = Vec::new();
    while project.metrics().cached_handles > 0 {
        held.push(project.reserve(1).unwrap());
    }
}

/// Pauses the next manual checkpoint's retirement and runs `read` meanwhile.
/// Returns the read's result, whether it finished before the retirement
/// resumed, and the database.
pub(in crate::api) fn read_while_retiring<R: Send + 'static>(
    mut db: Database,
    read: impl FnOnce() -> R + Send + 'static,
) -> (R, bool, Database) {
    let (pause, arm) = RetirementPause::new();
    let checkpoint = std::thread::spawn(move || {
        arm.arm();
        db.checkpoint().unwrap();
        db
    });
    pause
        .wait_paused(LIMIT)
        .expect("the checkpoint retires an object");
    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || sender.send(read()).unwrap());
    let early = receiver.recv_timeout(Duration::from_secs(2)).ok();
    drop(pause);
    let finished_early = early.is_some();
    let result = early.unwrap_or_else(|| receiver.recv_timeout(LIMIT).unwrap());
    reader.join().unwrap();
    (result, finished_early, checkpoint.join().unwrap())
}

fn assert_healthy(mut db: Database) {
    assert!(!db.storage_handle_poisoned().unwrap());
    let count = db.query_sql("SELECT count(*) AS n FROM items").unwrap();
    assert_eq!(count.rows[0]["n"], Value::Int(40));
    assert_eq!(
        db.query("MATCH (n:Memory) RETURN n.id AS id")
            .unwrap()
            .rows
            .len(),
        40
    );
}

fn check(name: &str, read: fn(&mut DatabaseReadTransaction) -> Result<usize>, warm: bool) {
    let path = unique_test_dir(name);
    let mut db = populated(&path);
    let mut reader = db.begin_read_transaction().unwrap();
    write_round(&mut db, 4);
    let handles = project(&db);
    if warm {
        assert_eq!(read(&mut reader).unwrap(), 32);
    }
    let (result, finished_early, db) = read_while_retiring(db, move || {
        if !warm {
            evict(&handles);
        }
        let result = read(&mut reader);
        (result, reader)
    });
    let (result, mut reader) = result;
    assert_eq!(result.unwrap(), 32);
    // A warm read takes no lock retirement holds; a cold open waits for it.
    assert_eq!(finished_early, warm);
    // The sweep is over and nothing is cached: the pinned snapshot's objects
    // were not in it and every one of them still opens.
    evict(&project(&db));
    assert_eq!(read(&mut reader).unwrap(), 32);
    drop(reader);
    assert_healthy(db);
    std::fs::remove_dir_all(&path).unwrap();
}

fn rows(reader: &mut DatabaseReadTransaction) -> Result<usize> {
    reader
        .query_sql("SELECT id, body FROM items ORDER BY id")
        .map(|output| output.rows.len())
}

fn nodes(reader: &mut DatabaseReadTransaction) -> Result<usize> {
    reader
        .query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
        .map(|output| output.rows.len())
}

#[test]
fn warm_row_page_read_finishes_while_retirement_is_paused() {
    check("retirement_rows_warm", rows, true);
}

#[test]
fn cold_row_page_read_waits_for_retirement_without_poisoning() {
    check("retirement_rows_cold", rows, false);
}

#[test]
fn cold_out_of_core_graph_read_waits_for_retirement_without_poisoning() {
    check("retirement_graph_cold", nodes, false);
}

/// Real damage met while waiting for a retirement is still an integrity error.
#[test]
fn damaged_row_page_root_beside_retirement_stays_an_integrity_error() {
    let path = unique_test_dir("retirement_rows_damaged");
    let mut db = populated(&path);
    let mut reader = db.begin_read_transaction().unwrap();
    write_round(&mut db, 4);
    let (handles, root) = (project(&db), super::active_storage_root(&path));
    let (error, _, _db) = read_while_retiring(db, move || {
        let mut damaged = 0;
        for entry in std::fs::read_dir(root).unwrap() {
            let artifact = entry.unwrap().path();
            let name = artifact.file_name().unwrap().to_string_lossy().into_owned();
            if name.starts_with("relational-row-root") && name.ends_with(".descriptors.hawdb") {
                let length = std::fs::metadata(&artifact).unwrap().len();
                super::flip_test_file_byte(&artifact, length / 2, None);
                damaged += 1;
            }
        }
        assert!(damaged > 0);
        evict(&handles);
        rows(&mut reader).unwrap_err()
    });
    assert!(
        matches!(&error, HawDBError::StorageIntegrity(message)
            if !message.contains("retirement")),
        "{error:?}"
    );
    std::fs::remove_dir_all(&path).unwrap();
}
