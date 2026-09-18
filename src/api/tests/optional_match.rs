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
fn optional_match_count_after_node_match_covers_thread_message_count() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 't1'})-[:CONTAINS]->(:Message {id: 'msg1'})")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (t:Thread {id: $thread_uuid}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN COUNT(m)",
            &BTreeMap::from([(
                "thread_uuid".to_string(),
                Value::String("t1".to_string()),
            )]),
        )
        .unwrap();
    assert_eq!(output.rows[0].get("count(m)"), Some(&Value::Int(1)));

    let missing = db
        .query_with_params(
            "MATCH (t:Thread {id: $thread_uuid}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN COUNT(m)",
            &BTreeMap::from([(
                "thread_uuid".to_string(),
                Value::String("missing".to_string()),
            )]),
        )
        .unwrap();
    assert_eq!(missing.rows[0].get("count(m)"), Some(&Value::Int(0)));
}

#[test]
fn thread_repair_summary_counts_identity_refs_across_threads() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'thread-a', thread_id: 'logical-a', message_count: 2})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread-b', thread_id: 'logical-b', space_id: 'archive'})")
        .unwrap();
    db.query("CREATE (:ThreadIdentity {id: 'identity-a-1', thread_node_id: 'thread-a'})")
        .unwrap();
    db.query("CREATE (:ThreadIdentity {id: 'identity-a-2', thread_node_id: 'thread-a'})")
        .unwrap();
    db.query("CREATE (:Message {id: 'message-a'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory-a'})").unwrap();
    db.query("MATCH (t:Thread {id: 'thread-a'}), (m:Message {id: 'message-a'}) CREATE (t)-[:CONTAINS]->(m)")
        .unwrap();
    db.query("MATCH (t:Thread {id: 'thread-a'}), (m:Memory {id: 'memory-a'}) CREATE (t)-[:COMPACTS_TO]->(m)")
        .unwrap();

    let output = db
        .query(
            "MATCH (t:Thread) OPTIONAL MATCH (ti:ThreadIdentity) WHERE ti.thread_node_id = t.id WITH t, COUNT(ti) AS identity_refs OPTIONAL MATCH (t)-[:CONTAINS]->(msg:Message) WITH t, identity_refs, COUNT(msg) AS legacy_messages OPTIONAL MATCH (t)-[:COMPACTS_TO]->(m:Memory) RETURN t.id, t.thread_id, CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END, COALESCE(t.message_count, 0), identity_refs, legacy_messages, COUNT(m) ORDER BY t.id ASC",
        )
        .unwrap();

    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("t.id"),
        Some(&Value::String("thread-a".into()))
    );
    assert_eq!(output.rows[0].get("identity_refs"), Some(&Value::Int(2)));
    assert_eq!(output.rows[0].get("legacy_messages"), Some(&Value::Int(1)));
    assert_eq!(output.rows[0].get("COUNT(m)"), Some(&Value::Int(1)));
    assert_eq!(
        output.rows[0].get(
            "CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END"
        ),
        Some(&Value::String("default".into()))
    );
    assert_eq!(
        output.rows[1].get("t.id"),
        Some(&Value::String("thread-b".into()))
    );
    assert_eq!(output.rows[1].get("identity_refs"), Some(&Value::Int(0)));
    assert_eq!(output.rows[1].get("legacy_messages"), Some(&Value::Int(0)));
    assert_eq!(output.rows[1].get("COUNT(m)"), Some(&Value::Int(0)));
}

#[test]
fn thread_repair_summary_does_not_admit_unprojected_node_payloads() {
    let execution_memory = crate::executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(2048).unwrap(),
        ..crate::executor::ExecutionMemoryConfig::default()
    };
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory,
        ..DatabaseConfig::default()
    });
    let unused_payload = Value::String("x".repeat(16 * 1024));
    db.query_with_params(
        "CREATE (:Thread {id: 'thread', thread_id: 'logical', unused: $payload})",
        &BTreeMap::from([("payload".to_string(), unused_payload.clone())]),
    )
    .unwrap();
    db.query_with_params(
        "CREATE (:ThreadIdentity {id: 'identity', thread_node_id: 'thread', unused: $payload})",
        &BTreeMap::from([("payload".to_string(), unused_payload)]),
    )
    .unwrap();

    let output = db
        .query(
            "MATCH (t:Thread) OPTIONAL MATCH (ti:ThreadIdentity) WHERE ti.thread_node_id = t.id WITH t, COUNT(ti) AS identity_refs OPTIONAL MATCH (t)-[:CONTAINS]->(msg:Message) WITH t, identity_refs, COUNT(msg) AS legacy_messages OPTIONAL MATCH (t)-[:COMPACTS_TO]->(m:Memory) RETURN t.id, t.thread_id, CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END, COALESCE(t.message_count, 0), identity_refs, legacy_messages, COUNT(m) ORDER BY t.id ASC",
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("identity_refs"), Some(&Value::Int(1)));
}

#[test]
fn optional_match_count_after_relationship_match_covers_legacy_tail_refs() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 't1'})-[:CONTAINS]->(:Message {id: 'msg1', order_index: 1})")
        .unwrap();
    db.query("CREATE (:Message {id: 'msg2', order_index: 2})")
        .unwrap();
    db.query("MATCH (t:Thread {id: 't1'}), (m:Message {id: 'msg2'}) CREATE (t)-[:CONTAINS]->(m)")
        .unwrap();
    db.query("CREATE (:Memory {id: 'm1'})").unwrap();
    db.query("MATCH (mem:Memory {id: 'm1'}), (msg:Message {id: 'msg1'}) CREATE (mem)-[:EXTRACTED_FROM]->(msg)")
            .unwrap();

    let output = db
            .query_with_params(
                "MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) WHERE m.order_index >= $start_index OPTIONAL MATCH (:Memory)-[r:EXTRACTED_FROM]->(m) RETURN COUNT(r)",
                &BTreeMap::from([
                    ("thread_uuid".to_string(), Value::String("t1".to_string())),
                    ("start_index".to_string(), Value::Int(0)),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows[0].get("count(r)"), Some(&Value::Int(1)));

    let missing = db
            .query_with_params(
                "MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) WHERE m.order_index >= $start_index OPTIONAL MATCH (:Memory)-[r:EXTRACTED_FROM]->(m) RETURN COUNT(r)",
                &BTreeMap::from([
                    ("thread_uuid".to_string(), Value::String("t1".to_string())),
                    ("start_index".to_string(), Value::Int(2)),
                ]),
            )
            .unwrap();
    assert_eq!(missing.rows[0].get("count(r)"), Some(&Value::Int(0)));
}

#[test]
fn optional_match_with_degree_projection_covers_top_entities() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'e1', name: 'Alpha'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e2', name: 'Beta'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e3', name: 'Gamma'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e4', name: 'Isolated'})")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'e1'}), (b:Entity {id: 'e2'}) CREATE (a)-[:RELATES_TO]->(b)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'e2'}), (b:Entity {id: 'e3'}) CREATE (a)-[:RELATES_TO]->(b)")
        .unwrap();

    let output = db
        .query(
            "MATCH (e:Entity)
                 OPTIONAL MATCH (e)-[r]-()
                 WITH e, COUNT(r) as degree
                 RETURN e.id, e.name, degree
                 ORDER BY degree DESC
                 LIMIT 10",
        )
        .unwrap();

    assert_eq!(output.rows.len(), 4);
    assert_eq!(
        output.rows[0].get("e.id"),
        Some(&Value::String("e2".into()))
    );
    assert_eq!(output.rows[0].get("degree"), Some(&Value::Int(2)));
    let isolated = output
        .rows
        .iter()
        .find(|row| row.get("e.id") == Some(&Value::String("e4".into())))
        .expect("isolated entity row");
    assert_eq!(
        isolated.get("e.name"),
        Some(&Value::String("Isolated".into()))
    );
    assert_eq!(isolated.get("degree"), Some(&Value::Int(0)));
}

#[test]
fn optional_match_with_target_count_projection_covers_label_usage() {
    let mut db = Database::new();
    db.query("CREATE (:Label {id: 'l1', name: 'Important', canonical_name: 'important'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'l2', name: 'Unused', canonical_name: 'unused'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'm1'})").unwrap();
    db.query("CREATE (:Entity {id: 'e1'})").unwrap();
    db.query("MATCH (m:Memory {id: 'm1'}), (l:Label {id: 'l1'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();
    db.query("MATCH (e:Entity {id: 'e1'}), (l:Label {id: 'l1'}) CREATE (e)-[:HAS_LABEL]->(l)")
        .unwrap();

    let output = db
        .query(
            "MATCH (l:Label)
                 OPTIONAL MATCH (l)<-[:HAS_LABEL]-(n)
                 WITH l, COUNT(n) as usage_count
                 RETURN l.id, l.name, usage_count
                 ORDER BY usage_count DESC, l.id ASC",
        )
        .unwrap();

    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("l.id"),
        Some(&Value::String("l1".into()))
    );
    assert_eq!(output.rows[0].get("usage_count"), Some(&Value::Int(2)));
    assert_eq!(
        output.rows[1].get("l.id"),
        Some(&Value::String("l2".into()))
    );
    assert_eq!(output.rows[1].get("usage_count"), Some(&Value::Int(0)));
}
