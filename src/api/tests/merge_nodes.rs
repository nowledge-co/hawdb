use super::*;

#[test]
fn merge_node_is_idempotent() {
    let mut db = Database::new();
    let first = db
        .query("MERGE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    let second = db
        .query("MERGE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();

    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(first.rows[0].get("node_id"), second.rows[0].get("node_id"));

    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
}

#[test]
fn merge_node_on_create_set_writes_only_when_created() {
    let mut db = Database::new();
    let first = db
            .query(
                "MERGE (m:SchemaMigrationLog {id: 'migration-1'}) ON CREATE SET m.applied_at = CURRENT_TIMESTAMP(), m.note = 'created'",
            )
            .unwrap();
    let second = db
        .query(
            "MERGE (m:SchemaMigrationLog {id: 'migration-1'}) ON CREATE SET m.note = 'overwritten'",
        )
        .unwrap();

    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(first.rows[0].get("node_id"), second.rows[0].get("node_id"));

    let output = db
            .query(
                "MATCH (m:SchemaMigrationLog {id: 'migration-1'}) RETURN m.note AS note, m.applied_at AS applied_at",
            )
            .unwrap();
    assert_eq!(
        output.rows[0].get("note"),
        Some(&Value::String("created".to_string()))
    );
    assert!(matches!(
        output.rows[0].get("applied_at"),
        Some(Value::Int(value)) if *value > 0
    ));
}

#[test]
fn merge_node_on_match_set_updates_only_when_matched() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Label {id: 'label-1', name: 'Important', canonical_name: null, updated_at: 1})",
    )
    .unwrap();

    let matched = db
            .query_with_params(
                "MERGE (l:Label {id: $label_id}) ON CREATE SET l.name = $label_name, l.canonical_name = $canonical, l.updated_at = 10 ON MATCH SET l.updated_at = $now, l.canonical_name = COALESCE(l.canonical_name, $canonical)",
                &BTreeMap::from([
                    ("label_id".to_string(), Value::String("label-1".to_string())),
                    ("label_name".to_string(), Value::String("Ignored".to_string())),
                    (
                        "canonical".to_string(),
                        Value::String("important".to_string()),
                    ),
                    ("now".to_string(), Value::Int(42)),
                ]),
            )
            .unwrap();
    assert_eq!(matched.rows[0].get("created"), Some(&Value::Bool(false)));

    let created = db
            .query_with_params(
                "MERGE (l:Label {id: $label_id}) ON CREATE SET l.name = $label_name, l.canonical_name = $canonical, l.updated_at = 10 ON MATCH SET l.updated_at = $now, l.canonical_name = COALESCE(l.canonical_name, $canonical)",
                &BTreeMap::from([
                    ("label_id".to_string(), Value::String("label-2".to_string())),
                    ("label_name".to_string(), Value::String("New".to_string())),
                    ("canonical".to_string(), Value::String("new".to_string())),
                    ("now".to_string(), Value::Int(99)),
                ]),
            )
            .unwrap();
    assert_eq!(created.rows[0].get("created"), Some(&Value::Bool(true)));

    let output = db
            .query(
                "MATCH (l:Label) RETURN l.id AS id, l.name AS name, l.canonical_name AS canonical, l.updated_at AS updated ORDER BY l.id ASC",
            )
            .unwrap();
    assert_eq!(
        output.rows[0].get("canonical"),
        Some(&Value::String("important".to_string()))
    );
    assert_eq!(output.rows[0].get("updated"), Some(&Value::Int(42)));
    assert_eq!(
        output.rows[1].get("canonical"),
        Some(&Value::String("new".to_string()))
    );
    assert_eq!(output.rows[1].get("updated"), Some(&Value::Int(10)));
}

#[test]
fn merge_node_post_set_updates_created_and_matched_nodes() {
    let path = unique_test_dir("merge_node_post_set");
    {
        let mut db = Database::open(&path).unwrap();
        let first = db
                .query(
                    "MERGE (m:GraphMeta {meta_id: 'main'}) SET m.pagerank_applied = true, m.pagerank_algorithm = 'pagerank', m.pagerank_iterations = 20, m.updated_at = CURRENT_TIMESTAMP()",
                )
                .unwrap();
        let second = db
                .query(
                    "MERGE (m:GraphMeta {meta_id: 'main'}) SET m.pagerank_applied = false, m.pagerank_computed_at = null, m.updated_at = CURRENT_TIMESTAMP()",
                )
                .unwrap();

        assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
        assert_eq!(first.rows[0].get("node_id"), second.rows[0].get("node_id"));

        let output = db
                .query(
                    "MATCH (m:GraphMeta {meta_id: 'main'}) RETURN m.pagerank_applied AS applied, m.pagerank_algorithm AS algorithm, m.pagerank_iterations AS iterations, m.pagerank_computed_at AS computed_at, count(m.updated_at) AS updated",
                )
                .unwrap();
        assert_eq!(output.rows[0].get("applied"), Some(&Value::Bool(false)));
        assert_eq!(
            output.rows[0].get("algorithm"),
            Some(&Value::String("pagerank".to_string()))
        );
        assert_eq!(output.rows[0].get("iterations"), Some(&Value::Int(20)));
        assert_eq!(output.rows[0].get("computed_at"), Some(&Value::Null));
        assert_eq!(output.rows[0].get("updated"), Some(&Value::Int(1)));
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.matches("create_node").count(), 1);
    assert_eq!(wal.matches("set_node_property").count(), 3);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn parameterized_merge_binds_before_storage_access() {
    let mut db = Database::new();
    db.query_with_params(
        "MERGE (:Memory {id: $id, title: $title})",
        &BTreeMap::from([
            ("id".to_string(), Value::Int(7)),
            (
                "title".to_string(),
                Value::String("Parameterized merge".to_string()),
            ),
        ]),
    )
    .unwrap();
    let error = db.query("MERGE (:Memory {id: $missing})").unwrap_err();
    assert!(error.to_string().contains("missing parameter '$missing'"));

    let output = db
        .query("MATCH (m:Memory) RETURN m.title AS title")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
}
