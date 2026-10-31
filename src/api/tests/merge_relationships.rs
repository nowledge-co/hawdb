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
use crate::{ConcurrentDatabase, ConcurrentTransactionOptions, QueryRequest};

const EVIDENCE_MERGE: &str = "MATCH (s:Skill {id: $id}) UNWIND $ids AS memory_id MATCH (m:Memory {id: memory_id}) MERGE (s)-[r:SYNTHESIZED_FROM]->(m) ON CREATE SET r.weight = 1.0, r.occasion_key = $occasion_key, r.created_at = $now RETURN m.id";

fn evidence_parameters(ids: &[&str]) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("id".into(), Value::String("skill".into())),
        (
            "ids".into(),
            Value::List(ids.iter().map(|id| Value::String((*id).into())).collect()),
        ),
        ("occasion_key".into(), Value::String("first".into())),
        ("now".into(), Value::Int(0)),
    ])
}

#[test]
fn matched_unwind_relationship_merge_returns_exact_members() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill'})").unwrap();
    for id in ["a", "b"] {
        db.query(&format!("CREATE (:Memory {{id: '{id}'}})"))
            .unwrap();
    }
    let parameters = evidence_parameters(&["a", "missing", "b", "a"]);
    let first = db.query_with_params(EVIDENCE_MERGE, &parameters).unwrap();
    assert_eq!(first.rows.schema().columns(), &["m.id"]);
    assert_eq!(first.rows.len(), 3);
    let mut members = first
        .rows
        .iter()
        .map(|row| row["m.id"].clone())
        .collect::<Vec<_>>();
    members.sort_by_key(|value| format!("{value:?}"));
    assert_eq!(
        members,
        vec![
            Value::String("a".into()),
            Value::String("a".into()),
            Value::String("b".into())
        ]
    );
    let mut repeated = parameters;
    repeated.insert("occasion_key".into(), Value::String("second".into()));
    assert_eq!(
        db.query_with_params(EVIDENCE_MERGE, &repeated)
            .unwrap()
            .rows
            .len(),
        3
    );
    let edges = db.query("MATCH (:Skill)-[r:SYNTHESIZED_FROM]->(:Memory) RETURN r.occasion_key AS occasion, r.weight AS weight").unwrap();
    assert_eq!(edges.rows.len(), 2);
    for row in &edges.rows {
        assert_eq!(row.get("occasion"), Some(&Value::String("first".into())));
        assert_eq!(row.get("weight"), Some(&Value::Float(1.0)));
    }
    let empty = db
        .query_with_params(EVIDENCE_MERGE, &evidence_parameters(&[]))
        .unwrap();
    assert!(empty.rows.is_empty());
    assert_eq!(empty.rows.schema().columns(), &["m.id"]);
}

#[test]
fn matched_relationship_merge_returns_without_unwind() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill'})").unwrap();
    db.query("CREATE (:Memory {id: 'a'})").unwrap();
    let output = db.query("MATCH (s:Skill {id: 'skill'}), (m:Memory {id: 'a'}) MERGE (s)-[r:SYNTHESIZED_FROM]->(m) ON CREATE SET r.weight = 1.0 RETURN m.id AS member, s.id AS skill").unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("member"),
        Some(&Value::String("a".into()))
    );
    assert_eq!(output.rows.schema().columns(), &["member", "skill"]);
}

#[test]
fn matched_unwind_relationship_merge_rejects_null_match_properties_atomically() {
    let mut db = evidence_fixture();
    let parameters = BTreeMap::from([(
        "rows".into(),
        Value::List(vec![
            Value::Map(BTreeMap::from([
                ("id".into(), Value::String("a".into())),
                ("key".into(), Value::Int(1)),
            ])),
            Value::Map(BTreeMap::from([("id".into(), Value::String("b".into()))])),
        ]),
    )]);
    let query = "MATCH (s:Skill {id: 'skill'}) UNWIND $rows AS row MATCH (m:Memory {id: row.id}) MERGE (s)-[:SYNTHESIZED_FROM {key: row.key}]->(m) RETURN m.id";
    let error = db.query_with_params(query, &parameters).unwrap_err();
    assert!(
        matches!(error, HawDBError::Semantic(message) if message.contains("NULL") && message.contains("key"))
    );
    assert_eq!(
        db.query(EVIDENCE_COUNT).unwrap().rows[0]["total"],
        Value::Int(0)
    );
}

fn evidence_fixture() -> Database {
    let mut database = Database::new();
    for query in [
        "CREATE (:Skill {id: 'skill'})",
        "CREATE (:Memory {id: 'a'})",
        "CREATE (:Memory {id: 'b'})",
    ] {
        database.query(query).unwrap();
    }
    database
}

const EVIDENCE_COUNT: &str =
    "MATCH (:Skill)-[r:SYNTHESIZED_FROM]->(:Memory) RETURN count(r) AS total";

fn assert_evidence_output_refusal(
    mut execute: impl for<'a> FnMut(QueryRequest<'a>) -> crate::Result<QueryOutput>,
) {
    execute(QueryRequest::new("CREATE (:RetainedStatement {id: 1})")).unwrap();
    let parameters = evidence_parameters(&["a", "b"]);
    for limits in [
        QueryStreamOptions {
            max_rows: Some(0),
            max_payload_bytes: None,
        },
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: None,
        },
        QueryStreamOptions {
            max_rows: None,
            max_payload_bytes: Some(0),
        },
        QueryStreamOptions {
            max_rows: None,
            max_payload_bytes: Some(1),
        },
    ] {
        let error = execute(
            QueryRequest::new(EVIDENCE_MERGE)
                .with_params(&parameters)
                .with_output_limits(limits),
        )
        .unwrap_err();
        assert!(
            matches!(error, HawDBError::ReadBudgetExceeded(_)),
            "{error:?}"
        );
        assert_eq!(
            execute(QueryRequest::new(EVIDENCE_COUNT)).unwrap().rows[0].get("total"),
            Some(&Value::Int(0))
        );
        assert_eq!(
            execute(QueryRequest::new("MATCH (n:RetainedStatement) RETURN n.id"))
                .unwrap()
                .rows
                .len(),
            1
        );
    }
    assert_eq!(
        execute(QueryRequest::new(EVIDENCE_MERGE).with_params(&parameters))
            .unwrap()
            .rows
            .len(),
        2
    );
}

#[test]
fn matched_unwind_relationship_merge_refusal_preserves_every_transaction_surface() {
    let mut database = evidence_fixture();
    assert_evidence_output_refusal(|request| database.query_request(request));
    let mut database = evidence_fixture();
    let mut transaction = database.begin_transaction().unwrap();
    assert_evidence_output_refusal(|request| transaction.query_request(request));
    transaction.rollback();
    assert_eq!(
        database.query(EVIDENCE_COUNT).unwrap().rows[0].get("total"),
        Some(&Value::Int(0))
    );
    let database = ConcurrentDatabase::new(evidence_fixture());
    assert_evidence_output_refusal(|request| database.query_request(request));
    for options in [
        ConcurrentTransactionOptions::optimistic(),
        ConcurrentTransactionOptions::default(),
    ] {
        let database = ConcurrentDatabase::new(evidence_fixture());
        let mut transaction = database.begin_transaction(options).unwrap();
        assert_evidence_output_refusal(|request| transaction.query_request(request));
        transaction.commit().unwrap();
        assert_eq!(
            database.query(EVIDENCE_COUNT).unwrap().rows[0].get("total"),
            Some(&Value::Int(2))
        );
    }
}

#[test]
fn matched_unwind_relationship_merge_where_rows_nulls_and_missing_source() {
    let mut database = evidence_fixture();
    let query = EVIDENCE_MERGE.replace(
        "MATCH (m:Memory {id: memory_id})",
        "MATCH (m:Memory) WHERE m.id = memory_id",
    );
    assert_eq!(
        database
            .query_with_params(&query, &evidence_parameters(&["a", "b"]))
            .unwrap()
            .rows
            .len(),
        2
    );
    let mut missing = evidence_parameters(&["a"]);
    missing.insert("id".into(), Value::String("missing".into()));
    assert!(database
        .query_with_params(EVIDENCE_MERGE, &missing)
        .unwrap()
        .rows
        .is_empty());
    database.query("CREATE (:Memory {id: null})").unwrap();
    let mut null = evidence_parameters(&[]);
    null.insert("ids".into(), Value::List(vec![Value::Null]));
    assert!(database
        .query_with_params(EVIDENCE_MERGE, &null)
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn matched_unwind_relationship_merge_preflights_the_entire_input_batch() {
    let mut database = evidence_fixture();
    let query = EVIDENCE_MERGE
        .replace("memory_id", "row.id")
        .replace("AS row.id", "AS row");
    let mut parameters = evidence_parameters(&[]);
    parameters.insert(
        "ids".into(),
        Value::List(vec![
            Value::Map(BTreeMap::from([("id".into(), Value::String("a".into()))])),
            Value::Int(7),
        ]),
    );
    assert!(database.query_with_params(&query, &parameters).is_err());
    assert_eq!(
        database.query(EVIDENCE_COUNT).unwrap().rows[0].get("total"),
        Some(&Value::Int(0))
    );
}

#[test]
fn matched_unwind_relationship_merge_storage_limits_and_constraints_are_atomic() {
    let mut config = DatabaseConfig::default();
    config.mutation_limits.max_operations = std::num::NonZeroUsize::new(1).unwrap();
    let mut database = Database::new_with_config(config);
    for query in [
        "CREATE (:Skill {id: 'skill'})",
        "CREATE (:Memory {id: 'a'})",
        "CREATE (:Memory {id: 'b'})",
    ] {
        database.query(query).unwrap();
    }
    let error = database
        .query_with_params(EVIDENCE_MERGE, &evidence_parameters(&["a", "b"]))
        .unwrap_err();
    assert!(
        error.to_string().contains("max_mutation_operations"),
        "{error:?}"
    );
    assert_eq!(
        database.query(EVIDENCE_COUNT).unwrap().rows[0].get("total"),
        Some(&Value::Int(0))
    );

    let mut database = evidence_fixture();
    database
        .query("CREATE CONSTRAINT ON -[:SYNTHESIZED_FROM(weight)]-> ASSERT UNIQUE")
        .unwrap();
    let result = database.query_with_params(EVIDENCE_MERGE, &evidence_parameters(&["a", "b"]));
    assert!(result.is_err());
    assert_eq!(
        database.query(EVIDENCE_COUNT).unwrap().rows[0].get("total"),
        Some(&Value::Int(0))
    );
}

#[test]
fn matched_unwind_relationship_merge_embedded_count_guard_rolls_back_and_reopens() {
    use crate::{NowledgeMemEmbeddedStoreHandle, NowledgeMemGraphMode, NowledgeMemOpenOptions};
    let path = unique_test_dir("matched_unwind_merge");
    let options = NowledgeMemOpenOptions::graph_only(&path, NowledgeMemGraphMode::WritableCutover);
    {
        let (handle, _) =
            NowledgeMemEmbeddedStoreHandle::open_with_options(options.clone()).unwrap();
        handle
            .with_transaction(|transaction| {
                transaction.query("CREATE (:Memory {id: 'a'})")?;
                Ok(())
            })
            .unwrap();
        let refused = handle.with_transaction(|transaction| {
            transaction.query("CREATE (:Skill {id: 'skill'})")?;
            let output = transaction
                .query_with_params(EVIDENCE_MERGE, &evidence_parameters(&["a", "missing"]))?;
            assert_eq!(output.rows.len(), 1);
            Err::<(), _>(HawDBError::Execution("exact member count refused".into()))
        });
        assert!(refused.is_err());
        assert!(handle
            .query_with_report("MATCH (s:Skill {id: 'skill'}) RETURN s.id")
            .unwrap()
            .output
            .rows
            .is_empty());
        assert_eq!(
            handle
                .query_with_report(EVIDENCE_COUNT)
                .unwrap()
                .output
                .rows[0]
                .get("total"),
            Some(&Value::Int(0))
        );
        handle
            .with_transaction(|transaction| {
                transaction.query("CREATE (:Skill {id: 'skill'})")?;
                assert_eq!(
                    transaction
                        .query_with_params(EVIDENCE_MERGE, &evidence_parameters(&["a"]))?
                        .rows
                        .len(),
                    1
                );
                Ok(())
            })
            .unwrap();
    }
    {
        let (handle, _) = NowledgeMemEmbeddedStoreHandle::open_with_options(options).unwrap();
        assert_eq!(
            handle
                .query_with_report(EVIDENCE_COUNT)
                .unwrap()
                .output
                .rows[0]
                .get("total"),
            Some(&Value::Int(1))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn merge_relationship_is_idempotent() {
    let mut db = Database::new();
    let first = db
            .query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 3}]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
    let second = db
            .query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS {weight: 3}]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();

    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(first.rows[0].get("rel_id"), second.rows[0].get("rel_id"));

    let output = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE e.id = 10 RETURN m.title AS memory, e.name AS entity",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
}

#[test]
fn merge_relationship_reuses_existing_endpoint_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Existing memory'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 10, name: 'Existing entity'})")
        .unwrap();

    let output = db
            .query(
                "MERGE (:Memory {id: 1, title: 'Existing memory'})-[:MENTIONS]->(:Entity {id: 10, name: 'Existing entity'})",
            )
            .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    let memories = db.query("MATCH (m:Memory) RETURN m.id AS id").unwrap();
    assert_eq!(memories.rows.len(), 1);
    let entities = db.query("MATCH (e:Entity) RETURN e.id AS id").unwrap();
    assert_eq!(entities.rows.len(), 1);
    let rels = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
        .unwrap();
    assert_eq!(rels.rows.len(), 1);
}

#[test]
fn create_relationship_between_matched_nodes_covers_source_provenance_write() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Memory'})")
        .unwrap();
    db.query("CREATE (:Source {id: 's1', memory_count: 0})")
        .unwrap();

    let output = db
            .query_with_params(
                "MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) CREATE (m)-[:SOURCED_FROM {chunk_index: $chunk_index}]->(s)",
                &BTreeMap::from([
                    ("memory_id".to_string(), Value::String("m1".to_string())),
                    ("source_id".to_string(), Value::String("s1".to_string())),
                    ("chunk_index".to_string(), Value::Int(7)),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);

    let rels = db
            .query(
                "MATCH (m:Memory {id: 'm1'})-[r:SOURCED_FROM]->(s:Source {id: 's1'}) RETURN count(r) AS total, min(r.chunk_index) AS first_chunk",
            )
            .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(1)));
    assert_eq!(rels.rows[0].get("first_chunk"), Some(&Value::Int(7)));
}

#[test]
fn create_relationship_between_where_matched_nodes_covers_evolves_write() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'older', title: 'Older'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'newer', title: 'Newer'})")
        .unwrap();

    let output = db
            .query_with_params(
                "MATCH (a:Memory), (b:Memory) WHERE a.id = $older_id AND b.id = $newer_id CREATE (a)-[:EVOLVES {content_relation: $relation}]->(b)",
                &BTreeMap::from([
                    ("older_id".to_string(), Value::String("older".to_string())),
                    ("newer_id".to_string(), Value::String("newer".to_string())),
                    ("relation".to_string(), Value::String("replaces".to_string())),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);

    let rels = db
            .query(
                "MATCH (a:Memory {id: 'older'})-[r:EVOLVES]->(b:Memory {id: 'newer'}) RETURN count(r) AS total, min(r.content_relation) AS relation",
            )
            .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(1)));
    assert_eq!(
        rels.rows[0].get("relation"),
        Some(&Value::String("replaces".to_string()))
    );

    let missing = db
            .query_with_params(
                "MATCH (a:Memory), (b:Memory) WHERE a.id = $older_id AND b.id = $newer_id CREATE (a)-[:EVOLVES]->(b)",
                &BTreeMap::from([
                    ("older_id".to_string(), Value::String("older".to_string())),
                    ("newer_id".to_string(), Value::String("missing".to_string())),
                ]),
            )
            .unwrap();
    assert!(missing.rows.is_empty());
    let rels = db
        .query("MATCH (:Memory)-[r:EVOLVES]->(:Memory) RETURN count(r) AS total")
        .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(1)));
}

#[test]
fn merge_relationship_between_matched_nodes_on_create_set_writes_only_when_created() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Memory'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'l1', name: 'Important'})")
        .unwrap();

    let first = db
            .query_with_params(
                "MATCH (m:Memory {id: $memory_id}), (l:Label {id: $label_id}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = $assigned_by, r.properties = '{}'",
                &BTreeMap::from([
                    ("memory_id".to_string(), Value::String("m1".to_string())),
                    ("label_id".to_string(), Value::String("l1".to_string())),
                    ("assigned_by".to_string(), Value::String("system".to_string())),
                ]),
            )
            .unwrap();
    let second = db
            .query_with_params(
                "MATCH (m:Memory {id: $memory_id}), (l:Label {id: $label_id}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = 'overwritten'",
                &BTreeMap::from([
                    ("memory_id".to_string(), Value::String("m1".to_string())),
                    ("label_id".to_string(), Value::String("l1".to_string())),
                ]),
            )
            .unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(first.rows[0].get("rel_id"), second.rows[0].get("rel_id"));

    let rels = db
            .query(
                "MATCH (m:Memory {id: 'm1'})-[r:HAS_LABEL]->(l:Label {id: 'l1'}) RETURN count(r) AS total, min(r.assigned_by) AS assigned_by, min(r.properties) AS properties",
            )
            .unwrap();
    assert_eq!(rels.rows[0].get("total"), Some(&Value::Int(1)));
    assert_eq!(
        rels.rows[0].get("assigned_by"),
        Some(&Value::String("system".to_string()))
    );
    assert_eq!(
        rels.rows[0].get("properties"),
        Some(&Value::String("{}".to_string()))
    );
}

#[test]
fn two_node_match_return_covers_source_provenance_existence_check() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Memory'})")
        .unwrap();
    db.query("CREATE (:Source {id: 's1', memory_count: 0})")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) RETURN count(m)",
            &BTreeMap::from([
                ("memory_id".to_string(), Value::String("m1".to_string())),
                ("source_id".to_string(), Value::String("s1".to_string())),
            ]),
        )
        .unwrap();
    assert_eq!(output.rows[0].get("count(m)"), Some(&Value::Int(1)));

    let missing = db
        .query_with_params(
            "MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) RETURN count(m)",
            &BTreeMap::from([
                ("memory_id".to_string(), Value::String("m1".to_string())),
                (
                    "source_id".to_string(),
                    Value::String("missing".to_string()),
                ),
            ]),
        )
        .unwrap();
    assert_eq!(missing.rows[0].get("count(m)"), Some(&Value::Int(0)));
}

#[test]
fn consecutive_two_node_match_return_covers_entity_endpoint_check() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'source', name: 'Source'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'target', name: 'Target'})")
        .unwrap();

    let output = db
            .query_with_params(
                "MATCH (source:Entity {id: $source_entity_id}) MATCH (target:Entity {id: $target_entity_id}) RETURN source.id, target.id",
                &BTreeMap::from([
                    (
                        "source_entity_id".to_string(),
                        Value::String("source".to_string()),
                    ),
                    (
                        "target_entity_id".to_string(),
                        Value::String("target".to_string()),
                    ),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("source.id"),
        Some(&Value::String("source".to_string()))
    );
    assert_eq!(
        output.rows[0].get("target.id"),
        Some(&Value::String("target".to_string()))
    );

    let missing = db
            .query_with_params(
                "MATCH (source:Entity {id: $source_entity_id}) MATCH (target:Entity {id: $target_entity_id}) RETURN source.id, target.id",
                &BTreeMap::from([
                    (
                        "source_entity_id".to_string(),
                        Value::String("source".to_string()),
                    ),
                    (
                        "target_entity_id".to_string(),
                        Value::String("missing".to_string()),
                    ),
                ]),
            )
            .unwrap();
    assert!(missing.rows.is_empty());
}

#[test]
fn matched_relationship_create_uses_one_wal_batch() {
    let path = unique_test_dir("matched_relationship_create_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'm1'})").unwrap();
        db.query("CREATE (:Source {id: 's1'})").unwrap();
        db.query("MATCH (m:Memory {id: 'm1'}), (s:Source {id: 's1'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 0}]->(s)")
                .unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    // Engine bootstrap is already checkpointed; count the private user WAL.
    assert_eq!(wal.lines().count(), 3);
    assert_eq!(wal.matches("create_node").count(), 2);
    assert_eq!(wal.matches("create_rel").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (:Memory)-[r:SOURCED_FROM]->(:Source) RETURN count(r) AS total")
            .unwrap();
        assert_eq!(output.rows[0].get("total"), Some(&Value::Int(1)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn merge_relationship_existing_pattern_does_not_write_wal() {
    let path = unique_test_dir("merge_relationship_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
        db.query(
                "MERGE (:Memory {id: 1, title: 'Graph foundations'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
            )
            .unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    // Engine bootstrap is already checkpointed; count the private user WAL.
    assert_eq!(wal.lines().count(), 1);
    assert_eq!(wal.matches("create_node").count(), 2);
    assert_eq!(wal.matches("create_rel").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN e.name AS entity")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn matched_unwind_relationship_merge_preserves_large_fanout_and_pending_subsets() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill'})").unwrap();
    let parameters = BTreeMap::from([(
        "rows".into(),
        Value::List(
            (0..4_096)
                .map(|id| Value::Map(BTreeMap::from([("id".into(), Value::Int(id))])))
                .collect(),
        ),
    )]);
    db.query_with_params(
        "UNWIND $rows AS row CREATE (:Memory {id: row.id, cohort: 1})",
        &parameters,
    )
    .unwrap();
    let rows = db.query("MATCH (s:Skill {id: 'skill'}) UNWIND [1, 2, 1] AS key MATCH (m:Memory {cohort: 1}) MERGE (s)-[:SYNTHESIZED_FROM {key: key}]->(m) RETURN m.id").unwrap();
    assert_eq!(rows.rows.len(), 12_288);
    assert_eq!(
        db.query(EVIDENCE_COUNT).unwrap().rows[0]["total"],
        Value::Int(8_192)
    );
}
