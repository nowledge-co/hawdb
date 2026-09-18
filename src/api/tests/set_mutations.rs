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
fn property_increment_set_updates_integer_properties() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 's1', memory_count: 0})")
        .unwrap();
    db.query("CREATE (:Source {id: 's2'})").unwrap();
    db.query("CREATE (:Source {id: 'bad', memory_count: 'zero'})")
        .unwrap();

    let output = db
        .query("MATCH (s:Source {id: 's1'}) SET s.memory_count = s.memory_count + 1")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    let output = db
        .query("MATCH (s:Source {id: 's1'}) RETURN s.memory_count AS count")
        .unwrap();
    assert_eq!(output.rows[0].get("count"), Some(&Value::Int(1)));

    db.query("MATCH (s:Source {id: 's2'}) SET s.memory_count = s.memory_count + 1")
        .unwrap();
    let output = db
        .query("MATCH (s:Source {id: 's2'}) RETURN s.memory_count AS count")
        .unwrap();
    assert_eq!(output.rows[0].get("count"), Some(&Value::Int(1)));

    let error = db
        .query("MATCH (s:Source {id: 'bad'}) SET s.memory_count = s.memory_count + 1")
        .unwrap_err();
    assert!(error.to_string().contains("requires an integer or null"));

    db.query(
        "MATCH (s:Source {id: 's1'})
             SET s.memory_count = CASE WHEN s.memory_count > 0 THEN s.memory_count - 1 ELSE 0 END",
    )
    .unwrap();
    let output = db
        .query("MATCH (s:Source {id: 's1'}) RETURN s.memory_count AS count")
        .unwrap();
    assert_eq!(output.rows[0].get("count"), Some(&Value::Int(0)));

    db.query(
        "MATCH (s:Source {id: 's1'})
             SET s.memory_count = CASE WHEN s.memory_count > 0 THEN s.memory_count - 1 ELSE 0 END",
    )
    .unwrap();
    let output = db
        .query("MATCH (s:Source {id: 's1'}) RETURN s.memory_count AS count")
        .unwrap();
    assert_eq!(output.rows[0].get("count"), Some(&Value::Int(0)));

    db.query("CREATE (:Memory {id: 'm1'})").unwrap();
    db.query(
        "MATCH (m:Memory) WHERE m.id = 'm1'
             SET m.access_count = COALESCE(m.access_count, 0) + 1,
                 m.last_accessed_at = 42",
    )
    .unwrap();
    let output = db
        .query(
            "MATCH (m:Memory {id: 'm1'})
                 RETURN m.access_count AS count, m.last_accessed_at AS last_accessed_at",
        )
        .unwrap();
    assert_eq!(output.rows[0].get("count"), Some(&Value::Int(1)));
    assert_eq!(
        output.rows[0].get("last_accessed_at"),
        Some(&Value::Int(42))
    );

    db.query("CREATE (:AugmentationJob {job_id: 'job-1', created_at: CURRENT_TIMESTAMP()})")
        .unwrap();
    let output = db
        .query(
            "MATCH (j:AugmentationJob {job_id: 'job-1'})
                 RETURN j.created_at AS created_at",
        )
        .unwrap();
    let Some(Value::Int(created_at)) = output.rows[0].get("created_at") else {
        panic!("expected integer timestamp");
    };
    assert!(*created_at > 0);

    db.query(
        "MATCH (j:AugmentationJob {job_id: 'job-1'})
             SET j.started_at = CURRENT_TIMESTAMP()",
    )
    .unwrap();
    let output = db
        .query(
            "MATCH (j:AugmentationJob {job_id: 'job-1'})
                 RETURN j.started_at AS started_at",
        )
        .unwrap();
    let Some(Value::Int(started_at)) = output.rows[0].get("started_at") else {
        panic!("expected integer timestamp");
    };
    assert!(*started_at >= *created_at);
}

#[test]
fn timestamp_function_binds_iso_strings_and_epoch_numbers() {
    let mut db = Database::new();
    db.query_with_params(
            "CREATE (:Memory {id: 'm1', created_at: timestamp($created_at), updated_at: timestamp($updated_at)})",
            &BTreeMap::from([
                (
                    "created_at".to_string(),
                    Value::String("1970-01-01T00:00:02Z".to_string()),
                ),
                ("updated_at".to_string(), Value::Int(3)),
            ]),
        )
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.created_at > timestamp($cutoff) RETURN count(m) AS total",
            &BTreeMap::from([(
                "cutoff".to_string(),
                Value::String("1970-01-01T00:00:01".to_string()),
            )]),
        )
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(1)));

    let output = db
            .query_with_params(
                "MATCH (m:Memory) WHERE m.created_at >= CAST($cutoff AS TIMESTAMP) RETURN count(m) AS total",
                &BTreeMap::from([(
                    "cutoff".to_string(),
                    Value::String("1970-01-01T00:00:02".to_string()),
                )]),
            )
            .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(1)));

    let output = db
            .query("MATCH (m:Memory {id: 'm1'}) RETURN m.created_at AS created_at, m.updated_at AS updated_at")
            .unwrap();
    assert_eq!(
        output.rows[0].get("created_at"),
        Some(&Value::Int(2_000_000_000))
    );
    assert_eq!(
        output.rows[0].get("updated_at"),
        Some(&Value::Int(3_000_000_000))
    );
}

#[test]
fn set_uses_or_predicate_filter() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note', title: 'One'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'task', title: 'Two'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'task', title: 'Three'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.kind = 'note' OR m.id = 2 SET m.flag = 'selected'")
        .unwrap();
    assert_eq!(output.rows.len(), 2);

    let output = db
        .query("MATCH (m:Memory) WHERE m.flag = 'selected' RETURN m.id AS id ORDER BY id ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(2)));
}

#[test]
fn set_persists_and_replays_from_wal() {
    let path = unique_test_dir("set_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Old'})").unwrap();
        db.query_with_params(
            "MATCH (m:Memory) WHERE m.id = $id SET m.title = $title",
            &BTreeMap::from([
                ("id".to_string(), Value::Int(1)),
                ("title".to_string(), Value::String("New".to_string())),
            ]),
        )
        .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory) WHERE m.title = 'New' RETURN m.id AS id")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_set_commits_and_rolls_back() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Old'})").unwrap();
    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory) WHERE m.id = 1 SET m.title = 'Ignored'")
            .unwrap();
        tx.rollback();
    }
    let output = db
        .query("MATCH (m:Memory) WHERE m.title = 'Old' RETURN m.id AS id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);

    {
        let mut tx = db.begin_transaction();
        tx.query("MATCH (m:Memory) WHERE m.id = 1 SET m.title = 'Committed'")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    let output = db
        .query("MATCH (m:Memory) WHERE m.title = 'Committed' RETURN m.id AS id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
}

#[test]
fn set_updates_node_property_and_property_index() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Old'})").unwrap();

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 SET m.title = 'New'")
        .unwrap();
    assert_eq!(output.rows.len(), 1);

    let output = db
        .query("MATCH (m:Memory) WHERE m.title = 'New' RETURN m.id AS id")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    let output = db
        .query("MATCH (m:Memory) WHERE m.title = 'Old' RETURN m.id AS id")
        .unwrap();
    assert!(output.rows.is_empty());
}
