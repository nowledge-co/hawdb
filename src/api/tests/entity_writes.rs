// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

const MEMORY_CREATE: &str = "MERGE (m:Memory {id: $id}) ON CREATE SET m.title = $title";
const MEMORY_CREATE_WITH_SOURCE: &str = "MERGE (m:Memory {id: $id}) \
    ON CREATE SET m.title = $title, m.source_id = $source_id";
const MEMORY_UPSERT: &str = "MERGE (m:Memory {id: $id}) \
    ON CREATE SET m.title = $title ON MATCH SET m.updated_at = $updated_at";
const MEMORY_TITLE_UPSERT: &str = "MERGE (m:Memory {id: $id}) \
    ON CREATE SET m.title = $title ON MATCH SET m.title = $title";
const MEMORY_READ: &str = "MATCH (m:Memory {id: $id}) RETURN id(m) AS node_id, \
    m.id AS id, m.title AS title, m.source_id AS source_id, m.updated_at AS updated_at";
const MEMORY_PAGE: &str = "MATCH (m:Memory) RETURN m.id AS id, m.title AS title ORDER BY id";
const ENTITY_CREATE: &str = "MERGE (e:Entity {id: $id}) ON CREATE SET e.name = $name";
const ENTITY_READ: &str =
    "MATCH (e:Entity {id: $id}) RETURN id(e) AS node_id, e.id AS id, e.name AS name";

fn identity_parameters(id: &str, supplied_id: Option<&str>) -> Result<BTreeMap<String, Value>> {
    if id.is_empty() {
        return Err(HawDBError::Semantic(
            "non-empty external id required".into(),
        ));
    }
    if supplied_id.is_some_and(|supplied| supplied != id) {
        return Err(HawDBError::Semantic(
            "id property does not match external id".into(),
        ));
    }
    Ok(BTreeMap::from([("id".into(), Value::String(id.into()))]))
}

fn memory_parameters(id: &str, title: &str) -> BTreeMap<String, Value> {
    let mut parameters = identity_parameters(id, None).unwrap();
    parameters.insert("title".into(), Value::String(title.into()));
    parameters
}

fn entity_parameters() -> BTreeMap<String, Value> {
    let mut parameters = identity_parameters("entity_1", None).unwrap();
    parameters.insert("name".into(), Value::String("HawDB".into()));
    parameters
}

#[test]
fn creates_knowledge_entity_through_bounded_query() {
    let mut db = Database::new();
    let mut parameters = memory_parameters("memory_1", "First");
    parameters.insert("source_id".into(), Value::String("thread_1".into()));
    let before = db.commit_epoch().unwrap();
    let output = db
        .query_request(bounded_batch_query(MEMORY_CREATE_WITH_SOURCE, &parameters))
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0]["created"], Value::Bool(true));
    assert_eq!(output.rows[0]["node_id"], Value::Int(0));
    assert_eq!(before, 0);
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    let row = db
        .query_request(bounded_batch_query(MEMORY_READ, &parameters))
        .unwrap();
    assert_eq!(row.rows.len(), 1);
    assert_eq!(row.rows[0]["node_id"], Value::Int(0));
    for key in ["id", "title", "source_id"] {
        assert_eq!(row.rows[0][key], parameters[key]);
    }
}

#[test]
fn create_knowledge_entity_reports_existing_identity_without_writing() {
    let path = unique_test_dir("entity_create_existing_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
            .unwrap();
        let epoch = db.commit_epoch().unwrap();
        let wal = read_test_wal(&path).unwrap();
        let parameters = memory_parameters("memory_1", "New");
        let output = db
            .query_request(bounded_batch_query(MEMORY_CREATE, &parameters))
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["created"], Value::Bool(false));
        assert_eq!(output.rows[0]["node_id"], Value::Int(0));
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        assert_eq!(read_test_wal(&path).unwrap(), wal);
        let rows = db
            .query_request(bounded_batch_query(MEMORY_READ, &parameters))
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0]["title"], Value::String("Old".into()));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn creates_knowledge_entity_batch_through_bounded_queries() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'existing', title: 'Existing'})")
        .unwrap();
    let epoch = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    for (query, parameters) in [
        (MEMORY_CREATE, memory_parameters("memory_1", "First")),
        (MEMORY_CREATE, memory_parameters("existing", "Ignored")),
        (ENTITY_CREATE, entity_parameters()),
    ] {
        assert!(transaction
            .query_request(bounded_batch_query(query, &parameters))
            .unwrap()
            .rows
            .is_empty());
    }
    let committed = transaction.commit().unwrap();
    assert_eq!(committed.rows.len(), 3);
    for (row, created) in committed.rows.iter().zip([true, false, true]) {
        assert_eq!(row["created"], Value::Bool(created));
        assert!(matches!(row["node_id"], Value::Int(_)));
    }
    assert_eq!(committed.rows[1]["node_id"], Value::Int(0));
    assert_eq!(epoch, 1);
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let memories = db
        .query_request(bounded_batch_query(MEMORY_PAGE, &BTreeMap::new()))
        .unwrap();
    assert_eq!(memories.rows.len(), 2);
    assert_eq!(memories.rows[0]["id"], Value::String("existing".into()));
    assert_eq!(memories.rows[0]["title"], Value::String("Existing".into()));
    assert_eq!(memories.rows[1]["id"], Value::String("memory_1".into()));
    assert_eq!(memories.rows[1]["title"], Value::String("First".into()));
    let entity = db
        .query_request(bounded_batch_query(ENTITY_READ, &entity_parameters()))
        .unwrap();
    assert_eq!(entity.rows.len(), 1);
    assert_eq!(entity.rows[0]["name"], Value::String("HawDB".into()));
}

#[test]
fn create_knowledge_entity_batch_deduplicates_pending_identity() {
    let mut db = Database::new();
    let mut transaction = db.begin_transaction().unwrap();
    for title in ["First", "Duplicate"] {
        transaction
            .query_request(bounded_batch_query(
                MEMORY_CREATE,
                &memory_parameters("memory_1", title),
            ))
            .unwrap();
    }
    let committed = transaction.commit().unwrap();
    assert_eq!(committed.rows.len(), 2);
    assert_eq!(committed.rows[0]["created"], Value::Bool(true));
    assert_eq!(committed.rows[1]["created"], Value::Bool(false));
    assert_eq!(committed.rows[0]["node_id"], committed.rows[1]["node_id"]);
    assert_eq!(db.commit_epoch().unwrap(), 1);
    let rows = db
        .query_request(bounded_batch_query(MEMORY_PAGE, &BTreeMap::new()))
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0]["id"], Value::String("memory_1".into()));
    assert_eq!(rows.rows[0]["title"], Value::String("First".into()));
}

#[test]
fn create_knowledge_entity_rejects_invalid_identifiers_and_id_mismatch() {
    let path = unique_test_dir("entity_create_invalid_query");
    {
        let mut db = Database::open(&path).unwrap();
        let epoch = db.commit_epoch().unwrap();
        let wal = read_test_wal(&path).unwrap();
        let error = db
            .query_request(bounded_batch_query(
                "CREATE (:Memory {id: $id, bad-name: $title})",
                &memory_parameters("memory_1", "First"),
            ))
            .unwrap_err();
        assert!(matches!(error, HawDBError::Parse(_)));
        let error = identity_parameters("memory_1", Some("different")).unwrap_err();
        assert!(error.to_string().contains("does not match external id"));
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        assert_eq!(read_test_wal(&path).unwrap(), wal);
    }
    std::fs::remove_dir_all(path).unwrap();
}

fn assert_read_only_refusal(query: &str) {
    let path = unique_test_dir("entity_write_read_only_query");
    drop(Database::open(&path).unwrap());
    let wal = read_test_wal(&path).unwrap();
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                recovery_mode: crate::RecoveryMode::Strict,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let mut parameters = memory_parameters("memory_1", "First");
        parameters.insert("updated_at".into(), Value::Int(100));
        let error = db
            .query_request(bounded_batch_query(query, &parameters))
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
        assert_eq!(read_test_wal(&path).unwrap(), wal);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_database_rejects_knowledge_entity_create_query() {
    assert_read_only_refusal(MEMORY_CREATE);
}

#[test]
fn knowledge_entity_batch_create_queries_persist_as_one_wal_batch_and_replay() {
    let path = unique_test_dir("entity_create_query_replay");
    {
        let mut db = Database::open(&path).unwrap();
        let before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let mut transaction = db.begin_transaction().unwrap();
        for (query, parameters) in [
            (MEMORY_CREATE, memory_parameters("memory_1", "First")),
            (ENTITY_CREATE, entity_parameters()),
        ] {
            transaction
                .query_request(bounded_batch_query(query, &parameters))
                .unwrap();
        }
        let committed = transaction.commit().unwrap();
        assert_eq!(committed.rows.len(), 2);
        assert!(committed
            .rows
            .iter()
            .all(|row| row["created"] == Value::Bool(true)));
        assert_eq!(
            read_test_wal(&path).unwrap().matches("\tbatch\t").count(),
            before + 1
        );
    }
    assert!(read_test_wal(&path).unwrap().contains("create_node"));
    {
        let mut db = Database::open(&path).unwrap();
        for (query, parameters, key, value) in [
            (
                MEMORY_READ,
                memory_parameters("memory_1", "First"),
                "title",
                "First",
            ),
            (ENTITY_READ, entity_parameters(), "name", "HawDB"),
        ] {
            let rows = db
                .query_request(bounded_batch_query(query, &parameters))
                .unwrap();
            assert_eq!(rows.rows.len(), 1);
            assert_eq!(rows.rows[0]["id"], parameters["id"]);
            assert_eq!(rows.rows[0][key], Value::String(value.into()));
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn upserts_knowledge_entity_through_bounded_query() {
    let mut db = Database::new();
    let mut parameters = memory_parameters("memory_1", "First");
    parameters.insert(
        "updated_at".into(),
        Value::String("ignored-on-create".into()),
    );
    let created = db
        .query_request(bounded_batch_query(MEMORY_UPSERT, &parameters))
        .unwrap();
    assert_eq!(created.rows.len(), 1);
    assert_eq!(created.rows[0]["created"], Value::Bool(true));
    assert_eq!(created.rows[0]["node_id"], Value::Int(0));
    let first = db
        .query_request(bounded_batch_query(MEMORY_READ, &parameters))
        .unwrap();
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0]["title"], Value::String("First".into()));
    assert_eq!(first.rows[0]["updated_at"], Value::Null);
    let mut second = identity_parameters("memory_1", Some("memory_1")).unwrap();
    second.insert("title".into(), Value::String("Should Not Replace".into()));
    second.insert("updated_at".into(), Value::String("2026-07-19".into()));
    let epoch = db.commit_epoch().unwrap();
    let existing = db
        .query_request(bounded_batch_query(MEMORY_UPSERT, &second))
        .unwrap();
    assert_eq!(existing.rows.len(), 1);
    assert_eq!(existing.rows[0]["created"], Value::Bool(false));
    assert_eq!(existing.rows[0]["node_id"], Value::Int(0));
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let row = db
        .query_request(bounded_batch_query(MEMORY_READ, &second))
        .unwrap();
    assert_eq!(row.rows.len(), 1);
    assert_eq!(row.rows[0]["node_id"], Value::Int(0));
    assert_eq!(row.rows[0]["id"], Value::String("memory_1".into()));
    assert_eq!(row.rows[0]["title"], Value::String("First".into()));
    assert_eq!(
        row.rows[0]["updated_at"],
        Value::String("2026-07-19".into())
    );
}

#[test]
fn knowledge_entity_upsert_rejects_id_mismatch_before_writing() {
    let path = unique_test_dir("entity_upsert_identity_query");
    {
        let db = Database::open(&path).unwrap();
        let epoch = db.commit_epoch().unwrap();
        let wal = read_test_wal(&path).unwrap();
        let error = identity_parameters("memory_1", Some("different")).unwrap_err();
        assert!(error.to_string().contains("does not match external id"));
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        assert_eq!(read_test_wal(&path).unwrap(), wal);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn knowledge_entity_update_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {name: 'HawDB', description: 'old'})")
        .unwrap();
    let parameters = identity_parameters("0", None).unwrap();
    let before = db.query_request(bounded_batch_query(
        "MATCH (e:Entity) RETURN id(e) AS node_id, e.id AS id, e.name AS name, e.description AS description",
        &parameters,
    )).unwrap();
    assert_eq!(before.rows.len(), 1);
    assert_eq!(before.rows[0]["node_id"], Value::Int(0));
    assert_eq!(before.rows[0]["id"], Value::Null);
    let epoch = db.commit_epoch().unwrap();
    let mut update = parameters.clone();
    update.insert("description".into(), Value::String("new".into()));
    let output = db
        .query_request(bounded_batch_query(
            "MATCH (e:Entity {id: $id}) SET e.description = $description RETURN id(e) AS node_id",
            &update,
        ))
        .unwrap();
    assert!(output.rows.is_empty());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    let after = db.query_request(bounded_batch_query(
        "MATCH (e:Entity) RETURN id(e) AS node_id, e.id AS id, e.name AS name, e.description AS description", &parameters,
    )).unwrap();
    assert_eq!(after.rows, before.rows);
    assert_eq!(after.rows[0]["description"], Value::String("old".into()));
}

#[test]
fn upserts_knowledge_entity_batch_through_bounded_queries() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'existing', title: 'Old'})")
        .unwrap();
    let epoch = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let mut seen = BTreeSet::new();
    let mut duplicates = 0;
    for (id, title) in [
        ("created", "Created"),
        ("existing", "Updated"),
        ("created", "Duplicate Update"),
    ] {
        if !seen.insert(id) {
            duplicates += 1;
            continue;
        }
        transaction
            .query_request(bounded_batch_query(
                MEMORY_TITLE_UPSERT,
                &memory_parameters(id, title),
            ))
            .unwrap();
    }
    assert_eq!(duplicates, 1);
    let committed = transaction.commit().unwrap();
    assert_eq!(committed.rows.len(), 2);
    assert_eq!(committed.rows[0]["created"], Value::Bool(true));
    assert_eq!(committed.rows[1]["created"], Value::Bool(false));
    assert_eq!(committed.rows[1]["node_id"], Value::Int(0));
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let rows = db
        .query_request(bounded_batch_query(MEMORY_PAGE, &BTreeMap::new()))
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
    assert_eq!(rows.rows[0]["id"], Value::String("created".into()));
    assert_eq!(rows.rows[0]["title"], Value::String("Created".into()));
    assert_eq!(rows.rows[1]["id"], Value::String("existing".into()));
    assert_eq!(rows.rows[1]["title"], Value::String("Updated".into()));
}

#[test]
fn read_only_database_rejects_knowledge_entity_upsert_query() {
    assert_read_only_refusal(MEMORY_UPSERT);
}

#[test]
fn knowledge_entity_batch_upsert_queries_persist_as_one_wal_batch_and_replay() {
    let path = unique_test_dir("entity_upsert_query_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'existing', title: 'Old'})")
            .unwrap();
        let before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let mut transaction = db.begin_transaction().unwrap();
        for (id, title) in [("created", "Created"), ("existing", "Updated")] {
            transaction
                .query_request(bounded_batch_query(
                    MEMORY_TITLE_UPSERT,
                    &memory_parameters(id, title),
                ))
                .unwrap();
        }
        let committed = transaction.commit().unwrap();
        assert_eq!(committed.rows.len(), 2);
        assert_eq!(committed.rows[0]["created"], Value::Bool(true));
        assert_eq!(committed.rows[1]["created"], Value::Bool(false));
        assert_eq!(
            read_test_wal(&path).unwrap().matches("\tbatch\t").count(),
            before + 1
        );
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_node"));
    assert!(wal.contains("set_node_property"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query_request(bounded_batch_query(MEMORY_PAGE, &BTreeMap::new()))
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        for (index, id, title) in [(0, "created", "Created"), (1, "existing", "Updated")] {
            assert_eq!(rows.rows[index]["id"], Value::String(id.into()));
            assert_eq!(rows.rows[index]["title"], Value::String(title.into()));
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}
