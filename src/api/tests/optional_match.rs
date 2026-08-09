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

#[test]
fn optional_match_projects_source_with_distinct_target_and_relationship_counts() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'source', name: 'Source'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'target', name: 'Target'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'isolated', name: 'Isolated'})")
        .unwrap();
    db.query(
        "MATCH (source:Entity {id: 'source'}), (target:Entity {id: 'target'}) \
         CREATE (source)-[:FIRST]->(target)",
    )
    .unwrap();
    db.query(
        "MATCH (source:Entity {id: 'source'}), (target:Entity {id: 'target'}) \
         CREATE (source)-[:SECOND]->(target)",
    )
    .unwrap();

    let output = db
        .query(
            "MATCH (n:Entity {id: 'source'}) OPTIONAL MATCH (n)-[r]-(neighbor) \
             RETURN n AS entity, COUNT(DISTINCT neighbor) AS neighbor_count, \
                    COUNT(neighbor) AS neighbor_occurrence_count, \
                    COUNT(r) AS relationship_count",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert!(matches!(output.rows[0].get("entity"), Some(Value::Map(_))));
    assert_eq!(output.rows[0].get("neighbor_count"), Some(&Value::Int(1)));
    assert_eq!(
        output.rows[0].get("neighbor_occurrence_count"),
        Some(&Value::Int(2))
    );
    assert_eq!(
        output.rows[0].get("relationship_count"),
        Some(&Value::Int(2))
    );

    let isolated = db
        .query(
            "MATCH (n:Entity {id: 'isolated'}) OPTIONAL MATCH (n)-[r]-(neighbor) \
             RETURN n AS entity, COUNT(DISTINCT neighbor) AS neighbor_count, \
                    COUNT(neighbor) AS neighbor_occurrence_count, \
                    COUNT(r) AS relationship_count",
        )
        .unwrap();
    assert_eq!(isolated.rows.len(), 1);
    assert_eq!(isolated.rows[0].get("neighbor_count"), Some(&Value::Int(0)));
    assert_eq!(
        isolated.rows[0].get("neighbor_occurrence_count"),
        Some(&Value::Int(0))
    );
    assert_eq!(
        isolated.rows[0].get("relationship_count"),
        Some(&Value::Int(0))
    );

    let optional_row = db
        .query(
            "MATCH (n:Entity {id: 'isolated'}) OPTIONAL MATCH (n)-[r]-(neighbor) \
             RETURN neighbor, id(neighbor) AS neighbor_id",
        )
        .unwrap();
    assert_eq!(optional_row.rows[0].get("neighbor"), Some(&Value::Null));
    assert_eq!(optional_row.rows[0].get("neighbor_id"), Some(&Value::Null));
}
