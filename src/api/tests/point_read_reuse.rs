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

//! Projected point reads of one query execution reuse the canonical segment
//! they selected (#1482): reading every message of a creator seeks the
//! canonical descriptor tree once per segment, not once per message. Results
//! equal the resident store's, also after checkpoints publish new generations
//! and for a read snapshot pinned before them.

use super::*;
use hawdb_storage::canonical::point_descriptor_seeks_for_test;
use hawdb_storage::store::record_reads_for_test;

const MESSAGES: i64 = 300;
const START: &str = "MATCH (p:Person {id: 1}) WITH p WITH p ";
const QUERY: &str = "MATCH (p)<-[:HAS_CREATOR]-(m:Message) RETURN m.id AS id, m.date AS date";

fn populate(db: &mut Database) {
    db.query("CREATE (:Person {id: 1, name: 'p1'})").unwrap();
    for id in 1..=MESSAGES {
        // A long unrequested string per message, with multi-byte characters.
        db.query(&format!(
            "CREATE (:Message {{id: {id}, date: {}, content: 'é {id} {}'}})",
            id * 10,
            "x".repeat(200)
        ))
        .unwrap();
    }
    db.query("MATCH (p:Person {id: 1}), (m:Message) CREATE (m)-[:HAS_CREATOR]->(p)")
        .unwrap();
}

fn rows(output: &QueryOutput) -> Vec<(Value, Value)> {
    let mut rows = output
        .rows
        .iter()
        .map(|row| (row["id"].clone(), row["date"].clone()))
        .collect::<Vec<_>>();
    rows.sort_by_key(|(id, _)| match id {
        Value::Int(id) => *id,
        other => panic!("unexpected id {other:?}"),
    });
    rows
}

fn expected(date: impl Fn(i64) -> i64) -> Vec<(Value, Value)> {
    (1..=MESSAGES)
        .map(|id| (Value::Int(id), Value::Int(date(id))))
        .collect()
}

fn open_out_of_core(path: &std::path::Path) -> Database {
    Database::open_with_config(
        path,
        DatabaseConfig {
            storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
            ..DatabaseConfig::default()
        },
    )
    .unwrap()
}

/// Rows, projected node reads and descriptor seeks of one execution.
fn measure(run: impl FnOnce() -> QueryOutput) -> (Vec<(Value, Value)>, u64, u64) {
    let reads = record_reads_for_test();
    let seeks = point_descriptor_seeks_for_test();
    let output = run();
    (
        rows(&output),
        record_reads_for_test().since(reads).projected_nodes,
        point_descriptor_seeks_for_test() - seeks,
    )
}

#[test]
fn projected_reads_of_one_query_seek_each_segment_once() {
    let query = format!("{START}{QUERY}");
    let mut resident = Database::new();
    populate(&mut resident);
    let (resident_rows, _, _) = measure(|| resident.query(&query).unwrap());
    assert_eq!(resident_rows, expected(|id| id * 10));

    let path = unique_test_dir("point_read_reuse");
    {
        let mut db = Database::open(&path).unwrap();
        populate(&mut db);
        db.checkpoint().unwrap();
    }
    let mut db = open_out_of_core(&path);
    let (actual, projected, seeks) = measure(|| db.query(&query).unwrap());
    assert_eq!(actual, resident_rows);
    assert!(projected >= MESSAGES as u64, "{projected} projected reads");
    // All messages share one or a few canonical segments. Before #1482 every
    // projected read seeked the descriptor tree again (`seeks >= MESSAGES`).
    assert!(seeks <= 8, "{seeks} descriptor seeks for {projected} reads");

    // A snapshot pinned before an update and checkpoint keeps its generation.
    let mut snapshot = db.begin_read_transaction().unwrap();
    db.query("MATCH (m:Message) WHERE m.id <= 50 SET m.date = 7")
        .unwrap();
    db.checkpoint().unwrap();
    let updated = expected(|id| if id <= 50 { 7 } else { id * 10 });
    let (current, _, seeks) = measure(|| db.query(&query).unwrap());
    assert_eq!(current, updated);
    assert!(seeks <= 8, "{seeks} descriptor seeks after checkpoint");
    let (pinned, _, _) = measure(|| snapshot.query(&query).unwrap());
    assert_eq!(pinned, resident_rows);
    drop(snapshot);
    drop(db);

    // Reopened from the new generation, out of core.
    let mut db = open_out_of_core(&path);
    let (reopened, projected, seeks) = measure(|| db.query(&query).unwrap());
    assert_eq!(reopened, updated);
    assert!(projected >= MESSAGES as u64, "{projected} projected reads");
    assert!(seeks <= 8, "{seeks} descriptor seeks after reopen");
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}
