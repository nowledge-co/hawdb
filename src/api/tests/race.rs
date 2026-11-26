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

//! Threaded race coverage for concurrent commits, pinned reads and checkpoints.

use crate::{ConcurrentDatabase, Database, QueryOutput, Value};
use std::sync::{Arc, Barrier};

const WRITERS: usize = 4;
const WRITES_PER_WRITER: usize = 16;
const READERS: usize = 3;
const READS_PER_READER: usize = 20;
const CHECKPOINTS: usize = 3;
const RETRIES: usize = 64;

fn total(output: &QueryOutput) -> i64 {
    match output.rows.first().and_then(|row| row.get("total")) {
        Some(Value::Int(value)) => *value,
        other => panic!("expected an integer count, got {other:?}"),
    }
}

fn count(db: &ConcurrentDatabase) -> i64 {
    for attempt in 0..RETRIES {
        match db.query("MATCH (r:Race) RETURN count(r) AS total") {
            Ok(output) => return total(&output),
            Err(error) if error.is_retryable_transaction_conflict() && attempt + 1 < RETRIES => {
                std::thread::yield_now();
            }
            Err(error) => panic!("latest count failed: {error}"),
        }
    }
    unreachable!("the last retry either returns or panics")
}

fn create_node(db: &ConcurrentDatabase, id: usize, writer: usize) {
    let query = format!("CREATE (:Race {{id: {id}, writer: {writer}}})");
    for attempt in 0..RETRIES {
        match db.query(&query) {
            Ok(_) => return,
            Err(error) if error.is_retryable_transaction_conflict() && attempt + 1 < RETRIES => {
                std::thread::yield_now();
            }
            Err(error) => panic!("writer {writer} id {id} failed: {error}"),
        }
    }
}

fn checkpoint(db: &ConcurrentDatabase) {
    for attempt in 0..RETRIES {
        match db.checkpoint() {
            Ok(()) => return,
            Err(error) if attempt + 1 < RETRIES && retryable_checkpoint(&error) => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) => panic!("checkpoint failed: {error}"),
        }
    }
}

fn retryable_checkpoint(error: &crate::HawDBError) -> bool {
    error.is_retryable_transaction_conflict()
        || error
            .to_string()
            .contains("checkpoint source changed before publication")
}

#[test]
fn concurrent_commits_reads_and_checkpoints_hold_snapshots() {
    let path = super::unique_test_dir("race_concurrent_snapshots");
    let mut database = Database::open(&path).unwrap();
    database.query("CREATE (:Race {id: 0})").unwrap();
    database.checkpoint().unwrap();
    let db = database.into_concurrent();
    let start = Arc::new(Barrier::new(1 + WRITERS + READERS + 1));
    let mut handles = Vec::new();

    for writer in 0..WRITERS {
        let db = db.clone();
        let start = Arc::clone(&start);
        handles.push(std::thread::spawn(move || {
            start.wait();
            for step in 0..WRITES_PER_WRITER {
                create_node(&db, writer * WRITES_PER_WRITER + step + 1, writer);
            }
        }));
    }

    for _ in 0..READERS {
        let db = db.clone();
        let start = Arc::clone(&start);
        handles.push(std::thread::spawn(move || {
            start.wait();
            for _ in 0..READS_PER_READER {
                let mut snapshot = db.begin_read_transaction().unwrap();
                let first = total(
                    &snapshot
                        .query("MATCH (r:Race) RETURN count(r) AS total")
                        .unwrap(),
                );
                let again = total(
                    &snapshot
                        .query("MATCH (r:Race) RETURN count(r) AS total")
                        .unwrap(),
                );
                assert_eq!(first, again, "a pinned snapshot changed between reads");
                let latest = count(&db);
                assert!(
                    latest >= first,
                    "the latest view {latest} fell behind pinned snapshot {first}"
                );
                assert!(latest <= 1 + (WRITERS * WRITES_PER_WRITER) as i64);
            }
        }));
    }

    {
        let db = db.clone();
        let start = Arc::clone(&start);
        handles.push(std::thread::spawn(move || {
            start.wait();
            for _ in 0..CHECKPOINTS {
                checkpoint(&db);
                std::thread::yield_now();
            }
        }));
    }

    start.wait();
    for handle in handles {
        handle.join().unwrap();
    }
    checkpoint(&db);
    let final_count = count(&db);
    assert_eq!(final_count, 1 + (WRITERS * WRITES_PER_WRITER) as i64);
    drop(db);

    let mut reopened = Database::open(&path).unwrap();
    let reopened_count = total(
        &reopened
            .query("MATCH (r:Race) RETURN count(r) AS total")
            .unwrap(),
    );
    assert_eq!(reopened_count, final_count);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}
