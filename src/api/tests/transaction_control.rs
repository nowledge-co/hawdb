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
fn transaction_rollback_discards_buffered_mutations() {
    let mut db = Database::new();
    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        let staged = tx
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            staged.rows[0].get("title"),
            Some(&Value::String("Graph foundations".to_string()))
        );
        tx.rollback();
    }

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn transaction_commit_applies_buffered_mutations() {
    let mut db = Database::new();
    let output = {
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        tx.query(
            "CREATE (:Memory {id: 2, title: 'Runtime strategy'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
        )
        .unwrap();
        tx.commit().unwrap()
    };

    assert_eq!(output.rows.len(), 2);
    let output = db
        .query(
            "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE e.id = 10 RETURN m.title AS memory, e.name AS entity",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("memory"),
        Some(&Value::String("Runtime strategy".to_string()))
    );
}

#[test]
fn transaction_commit_replays_as_one_wal_batch() {
    let path = unique_test_dir("transaction_batch_wal");
    {
        let mut db = Database::open(&path).unwrap();
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
            .unwrap();
        tx.query(
            "CREATE (:Memory {id: 2, title: 'Runtime strategy'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
        )
        .unwrap();
        tx.commit().unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.lines().count(), 2);
    assert!(wal.contains("\tbatch\t"));
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.title AS memory, e.name AS entity",
            )
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("entity"),
            Some(&Value::String("Rust".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_reads_own_writes_and_commits_exact_staged_operations() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, state: 'old'})").unwrap();

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 2, state: 'created'})")
            .unwrap();
        let created = tx
            .query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.state AS state")
            .unwrap();
        assert_eq!(
            created.rows[0].get("state"),
            Some(&Value::String("created".to_string()))
        );

        tx.query("MATCH (m:Memory) WHERE m.id = 1 SET m.state = 'staged'")
            .unwrap();
        tx.query("MATCH (m:Memory) WHERE m.state = 'staged' SET m.marker = 'matched-staged-state'")
            .unwrap();

        let returned = tx
            .query(
                "MATCH (m:Memory) WHERE m.id = 2 SET m.state = 'returned' RETURN m.id AS id, m.state AS state",
            )
            .unwrap();
        assert_eq!(returned.rows.len(), 1);
        assert_eq!(returned.rows[0].get("id"), Some(&Value::Int(2)));
        assert_eq!(
            returned.rows[0].get("state"),
            Some(&Value::String("returned".to_string()))
        );

        let staged = tx
            .query(
                "MATCH (m:Memory) WHERE m.marker = 'matched-staged-state' RETURN m.id AS id, m.state AS state",
            )
            .unwrap();
        assert_eq!(staged.rows.len(), 1);
        assert_eq!(staged.rows[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(
            staged.rows[0].get("state"),
            Some(&Value::String("staged".to_string()))
        );

        tx.commit().unwrap();
    }

    let committed = db
        .query(
            "MATCH (m:Memory) WHERE m.marker = 'matched-staged-state' RETURN m.id AS id, m.state AS state",
        )
        .unwrap();
    assert_eq!(committed.rows.len(), 1);
    assert_eq!(committed.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(
        committed.rows[0].get("state"),
        Some(&Value::String("staged".to_string()))
    );
    let returned = db
        .query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.state AS state")
        .unwrap();
    assert_eq!(
        returned.rows[0].get("state"),
        Some(&Value::String("returned".to_string()))
    );
}

#[test]
fn transaction_commit_updates_property_index() {
    let mut db = Database::new();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 42, title: 'Indexed memory'})")
            .unwrap();
        for id in 100..116 {
            tx.query(&format!(
                "CREATE (:Memory {{id: {id}, title: 'Indexed extra {id}'}})"
            ))
            .unwrap();
        }
        tx.commit().unwrap();
    }

    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.id = 42 RETURN m.title AS title")
        .unwrap();
    assert!(explain.trace.selected_plan.contains("IndexNodeSeek"));
    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 42 RETURN m.title AS title")
        .unwrap();
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Indexed memory".to_string()))
    );
}
