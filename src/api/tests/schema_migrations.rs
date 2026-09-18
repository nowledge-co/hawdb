use super::*;

const SCHEMA_MIGRATION_COUNT_QUERY: &str = "MATCH (m:SchemaMigrationLog) RETURN count(m) AS total";
const SCHEMA_MIGRATION_ALL_QUERY: &str = "MATCH (m:SchemaMigrationLog) \
     RETURN m.id AS migration_id, id(m) AS node_id, m.applied_at AS applied_at \
     ORDER BY migration_id ASC";
const SCHEMA_MIGRATION_PAGE_QUERY: &str = "MATCH (m:SchemaMigrationLog) \
     RETURN m.id AS migration_id, id(m) AS node_id, m.applied_at AS applied_at \
     ORDER BY migration_id ASC LIMIT $limit";

#[test]
fn applies_schema_migration_log_batch_idempotently() {
    let mut db = Database::new();
    db.query("CREATE (:SchemaMigrationLog {id: 'existing', applied_at: 10})")
        .unwrap();

    let output = db
        .apply_knowledge_schema_migrations_batch(&KnowledgeSchemaMigrationApplyBatchRequest {
            migrations: vec![
                KnowledgeSchemaMigrationApply {
                    migration_id: "existing".to_string(),
                    applied_at: Value::Int(100),
                },
                KnowledgeSchemaMigrationApply {
                    migration_id: "new_1".to_string(),
                    applied_at: Value::Int(101),
                },
                KnowledgeSchemaMigrationApply {
                    migration_id: "new_1".to_string(),
                    applied_at: Value::Int(102),
                },
                KnowledgeSchemaMigrationApply {
                    migration_id: "new_2".to_string(),
                    applied_at: Value::Int(103),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.created_count, 2);
    assert_eq!(output.already_applied_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert!(output.rows[0].already_applied);
    assert!(output.rows[1].created);
    assert!(output.rows[1].node_id.is_some());
    assert!(output.rows[2].duplicate);
    assert!(output.rows[3].created);

    let rows = db
        .query("MATCH (m:SchemaMigrationLog) RETURN m.id AS id, m.applied_at AS applied_at ORDER BY id ASC")
        .unwrap();
    assert_eq!(rows.rows.len(), 3);
    assert_eq!(
        rows.rows[0].get("id"),
        Some(&Value::String("existing".to_string()))
    );
    assert_eq!(rows.rows[0].get("applied_at"), Some(&Value::Int(10)));
    assert_eq!(
        rows.rows[1].get("id"),
        Some(&Value::String("new_1".to_string()))
    );
    assert_eq!(rows.rows[1].get("applied_at"), Some(&Value::Int(101)));
    assert_eq!(
        rows.rows[2].get("id"),
        Some(&Value::String("new_2".to_string()))
    );
    assert_eq!(rows.rows[2].get("applied_at"), Some(&Value::Int(103)));

    let mut read = db.begin_read_transaction();
    let count = read
        .query_with_params_bounded(SCHEMA_MIGRATION_COUNT_QUERY, &BTreeMap::new(), Some(1))
        .unwrap();
    assert_eq!(count.rows[0].get("total"), Some(&Value::Int(3)));
    let applied = read
        .query_with_params_bounded(SCHEMA_MIGRATION_ALL_QUERY, &BTreeMap::new(), Some(3))
        .unwrap();
    assert_eq!(read.commit_epoch(), db.store.commit_epoch());
    assert_eq!(applied.rows.len(), 3);
    assert_eq!(
        applied
            .rows
            .iter()
            .map(|row| row.get("migration_id").unwrap())
            .collect::<Vec<_>>(),
        vec![
            &Value::String("existing".to_string()),
            &Value::String("new_1".to_string()),
            &Value::String("new_2".to_string())
        ]
    );
    assert_eq!(applied.rows[0].get("applied_at"), Some(&Value::Int(10)));
    assert_eq!(applied.rows[1].get("applied_at"), Some(&Value::Int(101)));
    assert_eq!(applied.rows[2].get("applied_at"), Some(&Value::Int(103)));

    let limited = read
        .query_with_params_bounded(
            SCHEMA_MIGRATION_PAGE_QUERY,
            &BTreeMap::from([("limit".to_string(), Value::Int(2))]),
            Some(2),
        )
        .unwrap();
    assert_eq!(limited.rows.len(), 2);
    assert_eq!(
        limited
            .rows
            .iter()
            .map(|row| row.get("migration_id").unwrap())
            .collect::<Vec<_>>(),
        vec![
            &Value::String("existing".to_string()),
            &Value::String("new_1".to_string())
        ]
    );
}

#[test]
fn schema_migration_apply_rejects_empty_id_before_wal() {
    let mut db = Database::new();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .apply_knowledge_schema_migrations_batch(&KnowledgeSchemaMigrationApplyBatchRequest {
            migrations: vec![KnowledgeSchemaMigrationApply {
                migration_id: String::new(),
                applied_at: Value::Int(100),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty migration id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_schema_migration_apply_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_schema_migration_apply_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        let batch_count_before_update = read_test_wal(&path)
            .ok()
            .map_or(0, |wal| wal.matches("\tbatch\t").count());
        db.apply_knowledge_schema_migrations_batch(&KnowledgeSchemaMigrationApplyBatchRequest {
            migrations: vec![
                KnowledgeSchemaMigrationApply {
                    migration_id: "migration_1".to_string(),
                    applied_at: Value::Int(100),
                },
                KnowledgeSchemaMigrationApply {
                    migration_id: "migration_2".to_string(),
                    applied_at: Value::Int(200),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_node"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query("MATCH (m:SchemaMigrationLog) RETURN m.id AS id, m.applied_at AS applied_at ORDER BY id ASC")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0].get("id"),
            Some(&Value::String("migration_1".to_string()))
        );
        assert_eq!(rows.rows[0].get("applied_at"), Some(&Value::Int(100)));
        assert_eq!(
            rows.rows[1].get("id"),
            Some(&Value::String("migration_2".to_string()))
        );
        assert_eq!(rows.rows[1].get("applied_at"), Some(&Value::Int(200)));
    }
    std::fs::remove_dir_all(path).unwrap();
}
