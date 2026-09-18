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
fn system_sql_exposes_pinned_catalog_snapshot() {
    let mut db = Database::new();
    db.query("CREATE NODE LABEL Memory").unwrap();
    db.query("CREATE NODE TABLE Memory").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE STRING NOT NULL")
        .unwrap();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
        .unwrap();
    let read_tx = db.begin_read_transaction();

    db.query("CREATE NODE LABEL Source").unwrap();
    db.query("CREATE NODE TABLE Source").unwrap();

    let pinned_tables = read_tx
        .query_sql("SELECT table_name FROM system.tables ORDER BY table_id")
        .unwrap();
    assert_eq!(
        pinned_tables.rows,
        vec![BTreeMap::from([(
            "table_name".to_string(),
            Value::String("Memory".to_string()),
        )])]
    );

    let live_tables = db
        .query_sql("SELECT table_name FROM system.tables ORDER BY table_id")
        .unwrap();
    assert_eq!(live_tables.rows.len(), 2);
    assert_eq!(
        read_tx
            .query_sql("SELECT property_name FROM system.properties")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        read_tx
            .query_sql("SELECT index_kind FROM system.indexes")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        read_tx
            .query_sql("SELECT constraint_kind FROM system.constraints")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn schema_ddl_creates_catalog_tokens_idempotently() {
    let mut db = Database::new();
    let first = db.query("CREATE NODE LABEL Memory").unwrap();
    let second = db.query("CREATE NODE LABEL Memory").unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("label_id"),
        second.rows[0].get("label_id")
    );

    let first = db.query("CREATE RELATIONSHIP TYPE MENTIONS").unwrap();
    let second = db.query("CREATE RELATIONSHIP TYPE MENTIONS").unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("rel_type_id"),
        second.rows[0].get("rel_type_id")
    );

    let first = db.query("CREATE NODE TABLE Memory").unwrap();
    let second = db.query("CREATE NODE TABLE Memory").unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("table_id"),
        second.rows[0].get("table_id")
    );

    let first = db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
    let second = db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("table_id"),
        second.rows[0].get("table_id")
    );
    let tables = db.table_descriptors();
    assert!(tables.iter().any(|table| {
        table.name == "Memory"
            && table.kind == TableKind::Node
            && table.state == SchemaObjectState::Public
    }));
    assert!(tables.iter().any(|table| {
        table.name == "MENTIONS"
            && table.kind == TableKind::Relationship
            && table.state == SchemaObjectState::Public
    }));

    let first = db
        .query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
        .unwrap();
    let second = db
        .query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
        .unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("property_id"),
        second.rows[0].get("property_id")
    );
    db.query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT")
        .unwrap();
    let properties = db.property_descriptors();
    assert!(properties.iter().any(|property| {
        property.name == "id" && property.value_type == PropertyType::Int && !property.nullable
    }));
    assert!(properties.iter().any(|property| {
        property.name == "weight" && property.value_type == PropertyType::Int && property.nullable
    }));

    let first = db.query("CREATE INDEX ON :Memory(id)").unwrap();
    let second = db.query("CREATE INDEX ON :Memory(id)").unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("index_id"),
        second.rows[0].get("index_id")
    );
    assert!(db
        .property_indexes()
        .iter()
        .any(|index| index.property == "id"));

    let first = db
        .query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();
    let second = db
        .query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("index_id"),
        second.rows[0].get("index_id")
    );
    assert!(db
        .composite_property_indexes()
        .iter()
        .any(|index| index.properties == ["kind", "source_id"]));

    let first = db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
    let second = db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("index_id"),
        second.rows[0].get("index_id")
    );
    assert!(db.property_indexes().iter().any(|index| {
        index.property == "title" && index.kind == crate::schema::IndexKind::FullText
    }));

    let first = db
        .query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
        .unwrap();
    let second = db
        .query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
        .unwrap();
    assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
    assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    assert_eq!(
        first.rows[0].get("constraint_id"),
        second.rows[0].get("constraint_id")
    );
    assert!(db
        .unique_constraints()
        .iter()
        .any(|constraint| constraint.property == "id"));
}

#[test]
fn schema_ddl_replays_from_wal_without_data_rows() {
    let path = unique_test_dir("schema_ddl_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE LABEL Memory").unwrap();
        db.query("CREATE RELATIONSHIP TYPE MENTIONS").unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(body) TYPE TEXT")
            .unwrap();
        db.query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT")
            .unwrap();
        db.query("CREATE INDEX ON :Memory(id)").unwrap();
        db.query("CREATE INDEX ON :Memory(kind, source_id)")
            .unwrap();
        db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
        db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
            .unwrap();
        db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT EXISTS")
            .unwrap();
        db.query("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT EXISTS")
            .unwrap();
        db.query("CREATE CONSTRAINT ON -[:MENTIONS(id)]-> ASSERT UNIQUE")
            .unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_node_label"));
    assert!(wal.contains("create_rel_type"));
    assert!(wal.contains("create_node_table"));
    assert!(wal.contains("create_rel_table"));
    assert!(wal.contains("create_property"));
    assert!(wal.contains("create_index"));
    assert!(wal.contains("create_composite_index"));
    assert!(wal.contains("create_fulltext_index"));
    assert!(wal.contains("create_unique_constraint"));
    assert!(wal.contains("create_node_property_exists_constraint"));
    assert!(wal.contains("create_relationship_unique_constraint"));
    assert!(wal.contains("create_relationship_property_exists_constraint"));
    {
        let mut db = Database::open(&path).unwrap();
        let label = db.query("CREATE NODE LABEL Memory").unwrap();
        assert_eq!(label.rows[0].get("created"), Some(&Value::Bool(false)));
        let rel_type = db.query("CREATE RELATIONSHIP TYPE MENTIONS").unwrap();
        assert_eq!(rel_type.rows[0].get("created"), Some(&Value::Bool(false)));
        let table = db.query("CREATE NODE TABLE Memory").unwrap();
        assert_eq!(table.rows[0].get("created"), Some(&Value::Bool(false)));
        let table = db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
        assert_eq!(table.rows[0].get("created"), Some(&Value::Bool(false)));
        let property = db
            .query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        assert_eq!(property.rows[0].get("created"), Some(&Value::Bool(false)));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "body" && property.value_type == PropertyType::Text
        }));
        let property = db
            .query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT")
            .unwrap();
        assert_eq!(property.rows[0].get("created"), Some(&Value::Bool(false)));
        let index = db.query("CREATE INDEX ON :Memory(id)").unwrap();
        assert_eq!(index.rows[0].get("created"), Some(&Value::Bool(false)));
        let index = db
            .query("CREATE INDEX ON :Memory(kind, source_id)")
            .unwrap();
        assert_eq!(index.rows[0].get("created"), Some(&Value::Bool(false)));
        let index = db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
        assert_eq!(index.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON :Memory(id) ASSERT EXISTS")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT EXISTS")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON -[:MENTIONS(id)]-> ASSERT UNIQUE")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_ddl_survives_checkpoint_without_wal() {
    let path = unique_test_dir("schema_ddl_checkpoint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE LABEL Memory").unwrap();
        db.query("CREATE RELATIONSHIP TYPE MENTIONS").unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(body) TYPE TEXT")
            .unwrap();
        db.query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT")
            .unwrap();
        db.query("CREATE INDEX ON :Memory(id)").unwrap();
        db.query("CREATE INDEX ON :Memory(kind, source_id)")
            .unwrap();
        db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
        db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
            .unwrap();
        db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT EXISTS")
            .unwrap();
        db.query("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT EXISTS")
            .unwrap();
        db.query("CREATE CONSTRAINT ON -[:MENTIONS(id)]-> ASSERT UNIQUE")
            .unwrap();
        db.checkpoint().unwrap();
    }
    assert_eq!(read_test_wal(&path).unwrap(), "");
    let checkpoint = read_test_durable_text(&active_checkpoint_path(&path)).unwrap();
    assert!(checkpoint.contains("table"));
    assert!(checkpoint.contains("property\t"));
    assert!(checkpoint.contains("property_index"));
    assert!(checkpoint.contains("composite_property_index"));
    assert!(checkpoint.contains("fulltext"));
    assert!(checkpoint.contains("unique_constraint"));
    assert!(checkpoint.contains("node_property_exists_constraint"));
    assert!(checkpoint.contains("relationship_unique_constraint"));
    assert!(checkpoint.contains("relationship_property_exists_constraint"));
    {
        let mut db = Database::open(&path).unwrap();
        let label = db.query("CREATE NODE LABEL Memory").unwrap();
        assert_eq!(label.rows[0].get("created"), Some(&Value::Bool(false)));
        let rel_type = db.query("CREATE RELATIONSHIP TYPE MENTIONS").unwrap();
        assert_eq!(rel_type.rows[0].get("created"), Some(&Value::Bool(false)));
        let table = db.query("CREATE NODE TABLE Memory").unwrap();
        assert_eq!(table.rows[0].get("created"), Some(&Value::Bool(false)));
        let table = db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
        assert_eq!(table.rows[0].get("created"), Some(&Value::Bool(false)));
        let property = db
            .query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        assert_eq!(property.rows[0].get("created"), Some(&Value::Bool(false)));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "body" && property.value_type == PropertyType::Text
        }));
        let property = db
            .query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT")
            .unwrap();
        assert_eq!(property.rows[0].get("created"), Some(&Value::Bool(false)));
        let index = db.query("CREATE INDEX ON :Memory(id)").unwrap();
        assert_eq!(index.rows[0].get("created"), Some(&Value::Bool(false)));
        let index = db
            .query("CREATE INDEX ON :Memory(kind, source_id)")
            .unwrap();
        assert_eq!(index.rows[0].get("created"), Some(&Value::Bool(false)));
        let index = db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
        assert_eq!(index.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON :Memory(id) ASSERT EXISTS")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT EXISTS")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
        let constraint = db
            .query("CREATE CONSTRAINT ON -[:MENTIONS(id)]-> ASSERT UNIQUE")
            .unwrap();
        assert_eq!(constraint.rows[0].get("created"), Some(&Value::Bool(false)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_state_transitions_are_idempotent_and_persisted() {
    let path = unique_test_dir("schema_state_transition");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();

        let output = db
            .query("ALTER NODE TABLE Memory SET STATE WRITE_ONLY")
            .unwrap();
        assert_eq!(output.rows[0].get("changed"), Some(&Value::Bool(true)));
        let output = db
            .query("ALTER NODE TABLE Memory SET STATE WRITE_ONLY")
            .unwrap();
        assert_eq!(output.rows[0].get("changed"), Some(&Value::Bool(false)));

        let output = db
            .query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        assert_eq!(output.rows[0].get("changed"), Some(&Value::Bool(true)));
    }

    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("alter_table_state"));
    assert!(wal.contains("write_only"));
    assert!(wal.contains("alter_property_state"));
    assert!(wal.contains("backfill"));

    {
        let mut db = Database::open(&path).unwrap();
        assert!(db.table_descriptors().iter().any(|table| {
            table.name == "Memory" && table.state == SchemaObjectState::WriteOnly
        }));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
        db.checkpoint().unwrap();
    }

    assert_eq!(read_test_wal(&path).unwrap(), "");
    let checkpoint = read_test_durable_text(&active_checkpoint_path(&path)).unwrap();
    assert!(checkpoint.contains("write_only"));
    assert!(checkpoint.contains("backfill"));

    {
        let db = Database::open(&path).unwrap();
        assert!(db.table_descriptors().iter().any(|table| {
            table.name == "Memory" && table.state == SchemaObjectState::WriteOnly
        }));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn non_public_property_schema_is_not_validated_until_public() {
    let path = unique_test_dir("schema_state_public_validation");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        db.query("CREATE (:Memory {title: 'Missing id'})").unwrap();

        let error = db
            .query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE PUBLIC")
            .unwrap_err();
        assert!(error.to_string().contains("property schema violation"));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
    }

    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("backfill"));
    assert!(!wal.contains("public"));
    {
        let db = Database::open(&path).unwrap();
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_maintenance_advances_backfill_and_validation_in_batch_wal() {
    let path = unique_test_dir("schema_maintenance_advance");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();

        let output = db.run_schema_maintenance().unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert_eq!(
            output.rows[0].get("from_state"),
            Some(&Value::String("backfill".to_string()))
        );
        assert_eq!(
            output.rows[0].get("to_state"),
            Some(&Value::String("validating".to_string()))
        );

        let output = db.run_schema_maintenance().unwrap();
        assert_eq!(
            output.rows[0].get("to_state"),
            Some(&Value::String("public".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Public
        }));
    }

    let wal = read_test_wal(&path).unwrap();
    assert_eq!(wal.matches("\tbatch\t").count(), 6);
    assert!(wal.contains("alter_property_state,node,4d656d6f7279,6964,validating"));
    assert!(wal.contains("alter_property_state,node,4d656d6f7279,6964,public"));
    {
        let db = Database::open(&path).unwrap();
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Public
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_maintenance_rejects_invalid_validation_before_wal() {
    let path = unique_test_dir("schema_maintenance_rejects_invalid");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("ALTER NODE TABLE Memory SET STATE BACKFILL")
            .unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE (:Memory {title: 'Missing id'})").unwrap();
        db.query("ALTER NODE TABLE Memory SET STATE VALIDATING")
            .unwrap();

        let before = read_test_wal(&path).unwrap();
        let error = db.run_schema_maintenance().unwrap_err();
        assert!(error.to_string().contains("property schema violation"));
        let after = read_test_wal(&path).unwrap();
        assert_eq!(after, before);
        assert!(db.table_descriptors().iter().any(|table| {
            table.name == "Memory" && table.state == SchemaObjectState::Validating
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_maintenance_plan_reports_pending_property_work_without_wal_write() {
    let path = unique_test_dir("schema_maintenance_plan_property");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let before = read_test_wal(&path).unwrap();

        let plan = db.plan_schema_maintenance();

        assert_eq!(plan.rows.len(), 1);
        assert_eq!(
            plan.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert_eq!(
            plan.rows[0].get("from_state"),
            Some(&Value::String("backfill".to_string()))
        );
        assert_eq!(
            plan.rows[0].get("to_state"),
            Some(&Value::String("validating".to_string()))
        );
        assert_eq!(
            plan.rows[0].get("estimated_operations"),
            Some(&Value::Int(2))
        );
        let after = read_test_wal(&path).unwrap();
        assert_eq!(after, before);
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_maintenance_plan_estimates_relationship_table_validation_work() {
    let path = unique_test_dir("schema_maintenance_plan_relationship");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE RELATIONSHIP TABLE MENTIONS").unwrap();
        db.query("CREATE (:Memory {id: 1})-[:MENTIONS {weight: 3}]->(:Entity {id: 2})")
            .unwrap();
        db.query("CREATE (:Memory {id: 3})-[:MENTIONS {weight: 4}]->(:Entity {id: 4})")
            .unwrap();
        db.query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER RELATIONSHIP TABLE MENTIONS SET STATE VALIDATING")
            .unwrap();

        let plan = db.plan_schema_maintenance();

        assert_eq!(plan.rows.len(), 1);
        assert_eq!(
            plan.rows[0].get("object"),
            Some(&Value::String("MENTIONS".to_string()))
        );
        assert_eq!(
            plan.rows[0].get("from_state"),
            Some(&Value::String("validating".to_string()))
        );
        assert_eq!(
            plan.rows[0].get("to_state"),
            Some(&Value::String("public".to_string()))
        );
        assert_eq!(
            plan.rows[0].get("estimated_operations"),
            Some(&Value::Int(2))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_schema_maintenance_skips_work_that_exceeds_budget_without_wal_write() {
    let path = unique_test_dir("bounded_schema_maintenance_budget_skip");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let before = read_test_wal(&path).unwrap();

        let output = db.run_bounded_schema_maintenance(1).unwrap();

        assert!(output.rows.is_empty());
        let after = read_test_wal(&path).unwrap();
        assert_eq!(after, before);
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_schema_maintenance_advances_descriptor_batches_incrementally() {
    let path = unique_test_dir("bounded_schema_maintenance_incremental");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'a'})").unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'b'})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(title) TYPE STRING NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(title) SET STATE BACKFILL")
            .unwrap();

        let first = db.run_bounded_schema_maintenance(2).unwrap();

        assert_eq!(first.rows.len(), 1);
        assert_eq!(
            first.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Validating
        }));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "title" && property.state == SchemaObjectState::Backfill
        }));

        let second = db.run_bounded_schema_maintenance(2).unwrap();

        assert_eq!(second.rows.len(), 1);
        assert_eq!(
            second.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert_eq!(
            second.rows[0].get("to_state"),
            Some(&Value::String("public".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Public
        }));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "title" && property.state == SchemaObjectState::Backfill
        }));

        let third = db.run_bounded_schema_maintenance(2).unwrap();

        assert_eq!(third.rows.len(), 1);
        assert_eq!(
            third.rows[0].get("object"),
            Some(&Value::String("Memory.title".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "title" && property.state == SchemaObjectState::Validating
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_maintenance_background_work_plan_is_absent_without_pending_work() {
    let db = Database::new();

    assert!(db
        .schema_maintenance_background_work_plan(BackgroundWorkHint::default())
        .is_none());
}

#[test]
fn schema_maintenance_background_work_plan_uses_pending_estimate_for_ranking() {
    let path = unique_test_dir("schema_maintenance_background_plan");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();

        let plan = db
            .schema_maintenance_background_work_plan(BackgroundWorkHint {
                active_topic: true,
                query_probability_per_million: 250_000,
                ..BackgroundWorkHint::default()
            })
            .unwrap();

        assert_eq!(plan.request.class, crate::WorkClass::Mutation);
        assert_eq!(plan.request.estimated_operations, 2);

        let ranked =
            LocalQosPolicy::default().rank_background_work(&LocalQosState::default(), &[plan]);

        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].index, 0);
        assert!(ranked[0]
            .decision
            .reasons
            .iter()
            .any(|reason| reason == "active topic"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn background_schema_maintenance_defers_without_mutating_schema() {
    let path = unique_test_dir("background_schema_maintenance_defers");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let before = read_test_wal(&path).unwrap();
        let policy = LocalQosPolicy {
            max_background_operations: Some(0),
            ..LocalQosPolicy::default()
        };

        let error = db
            .run_background_schema_maintenance(&policy, &LocalQosState::default(), 1)
            .unwrap_err();

        assert!(error.to_string().contains("deferred"));
        let after = read_test_wal(&path).unwrap();
        assert_eq!(after, before);
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));

        let output = db.run_schema_maintenance().unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("to_state"),
            Some(&Value::String("validating".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn planned_background_schema_maintenance_uses_pending_work_estimate() {
    let path = unique_test_dir("planned_background_schema_maintenance_estimate");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let before = read_test_wal(&path).unwrap();
        let policy = LocalQosPolicy {
            max_background_operations: Some(1),
            ..LocalQosPolicy::default()
        };

        let error = db
            .run_planned_background_schema_maintenance(&policy, &LocalQosState::default())
            .unwrap_err();

        assert!(error.to_string().contains("estimated operations 2"));
        let after = read_test_wal(&path).unwrap();
        assert_eq!(after, before);
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Backfill
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_background_schema_maintenance_limits_actual_execution() {
    let path = unique_test_dir("bounded_background_schema_maintenance_execution");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'a'})").unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'b'})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(title) TYPE STRING NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(title) SET STATE BACKFILL")
            .unwrap();

        let output = db
            .run_bounded_background_schema_maintenance(
                &LocalQosPolicy::default(),
                &LocalQosState::default(),
                2,
            )
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Validating
        }));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "title" && property.state == SchemaObjectState::Backfill
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_background_schema_maintenance_admits_actual_work_not_caller_cap() {
    let path = unique_test_dir("bounded_background_schema_maintenance_actual_work");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let policy = LocalQosPolicy {
            max_background_operations: Some(2),
            ..LocalQosPolicy::default()
        };

        let output = db
            .run_bounded_background_schema_maintenance(&policy, &LocalQosState::default(), 10)
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Validating
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn scheduled_background_schema_maintenance_tracks_mutation_budget() {
    let path = unique_test_dir("scheduled_schema_maintenance_budget");
    {
        let mut class_limits = [None; crate::WORK_CLASS_COUNT];
        class_limits[crate::WorkClass::Mutation.as_index()] = Some(2);
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                local_qos_policy: LocalQosPolicy {
                    max_background_operations: Some(4),
                    max_total_background_operations: Some(4),
                    max_background_operations_by_class: class_limits,
                    ..LocalQosPolicy::default()
                },
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let scheduler = db.local_qos_scheduler();

        let output = db.run_scheduled_background_schema_maintenance(2).unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("to_state"),
            Some(&Value::String("validating".to_string()))
        );
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [crate::WorkClass::Mutation.as_index()],
            0
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_scheduled_background_schema_maintenance_admits_actual_work_not_caller_cap() {
    let path = unique_test_dir("bounded_scheduled_schema_maintenance_actual_work");
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                local_qos_policy: LocalQosPolicy {
                    max_background_operations: Some(2),
                    max_total_background_operations: Some(2),
                    ..LocalQosPolicy::default()
                },
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let scheduler = db.local_qos_scheduler();

        let output = db
            .run_bounded_scheduled_background_schema_maintenance(10)
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Validating
        }));
        assert_eq!(scheduler.state().running_background_operations, 0);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_scheduled_background_schema_maintenance_limits_execution_and_releases_budget() {
    let path = unique_test_dir("bounded_scheduled_schema_maintenance_budget");
    {
        let mut class_limits = [None; crate::WORK_CLASS_COUNT];
        class_limits[crate::WorkClass::Mutation.as_index()] = Some(2);
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                local_qos_policy: LocalQosPolicy {
                    max_background_operations: Some(4),
                    max_total_background_operations: Some(4),
                    max_background_operations_by_class: class_limits,
                    ..LocalQosPolicy::default()
                },
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'a'})").unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'b'})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(title) TYPE STRING NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(title) SET STATE BACKFILL")
            .unwrap();
        let scheduler = db.local_qos_scheduler();

        let output = db
            .run_bounded_scheduled_background_schema_maintenance(2)
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("object"),
            Some(&Value::String("Memory.id".to_string()))
        );
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "id" && property.state == SchemaObjectState::Validating
        }));
        assert!(db.property_descriptors().iter().any(|property| {
            property.name == "title" && property.state == SchemaObjectState::Backfill
        }));
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [crate::WorkClass::Mutation.as_index()],
            0
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn planned_scheduled_background_schema_maintenance_tracks_estimated_mutation_budget() {
    let path = unique_test_dir("planned_scheduled_schema_maintenance_budget");
    {
        let mut class_limits = [None; crate::WORK_CLASS_COUNT];
        class_limits[crate::WorkClass::Mutation.as_index()] = Some(2);
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                local_qos_policy: LocalQosPolicy {
                    max_background_operations: Some(4),
                    max_total_background_operations: Some(4),
                    max_background_operations_by_class: class_limits,
                    ..LocalQosPolicy::default()
                },
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        let scheduler = db.local_qos_scheduler();

        let output = db
            .run_planned_scheduled_background_schema_maintenance()
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("to_state"),
            Some(&Value::String("validating".to_string()))
        );
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [crate::WorkClass::Mutation.as_index()],
            0
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn scheduled_background_schema_maintenance_releases_budget_on_validation_error() {
    let path = unique_test_dir("scheduled_schema_maintenance_error_releases");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("ALTER NODE TABLE Memory SET STATE BACKFILL")
            .unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE (:Memory {title: 'Missing id'})").unwrap();
        db.query("ALTER NODE TABLE Memory SET STATE VALIDATING")
            .unwrap();
        let scheduler = db.local_qos_scheduler();

        let error = db
            .run_scheduled_background_schema_maintenance(1)
            .unwrap_err();

        assert!(error.to_string().contains("property schema violation"));
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(
            scheduler.state().running_background_operations_by_class
                [crate::WorkClass::Mutation.as_index()],
            0
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_maintenance_gc_removes_descriptors_and_persists() {
    let path = unique_test_dir("schema_maintenance_gc");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE GC")
            .unwrap();

        let output = db.run_schema_maintenance().unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("action"),
            Some(&Value::String("gc".to_string()))
        );
        assert!(db
            .property_descriptors()
            .iter()
            .all(|property| property.name != "id"));
    }

    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("gc_property_descriptor,node,4d656d6f7279,6964"));
    {
        let mut db = Database::open(&path).unwrap();
        assert!(db
            .property_descriptors()
            .iter()
            .all(|property| property.name != "id"));
        db.query("ALTER NODE TABLE Memory SET STATE GC").unwrap();
        let output = db.run_schema_maintenance().unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("object"),
            Some(&Value::String("Memory".to_string()))
        );
    }
    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .table_descriptors()
            .iter()
            .all(|table| table.name != "Memory"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_ddl_transaction_commits_and_rolls_back() {
    let mut db = Database::new();
    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE NODE LABEL RolledBack").unwrap();
        tx.rollback();
    }
    let output = db.query("CREATE NODE LABEL RolledBack").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE NODE TABLE RolledBack").unwrap();
        tx.rollback();
    }
    let output = db.query("CREATE NODE TABLE RolledBack").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE INDEX ON :RolledBack(id)").unwrap();
        tx.rollback();
    }
    let output = db.query("CREATE INDEX ON :RolledBack(id)").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE INDEX ON :RolledBack(kind, source_id)")
            .unwrap();
        tx.rollback();
    }
    let output = db
        .query("CREATE INDEX ON :RolledBack(kind, source_id)")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE FULLTEXT INDEX ON :RolledBack(title)")
            .unwrap();
        tx.rollback();
    }
    let output = db
        .query("CREATE FULLTEXT INDEX ON :RolledBack(title)")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE PROPERTY ON NODE TABLE RolledBack(id) TYPE INT NOT NULL")
            .unwrap();
        tx.rollback();
    }
    let output = db
        .query("CREATE PROPERTY ON NODE TABLE RolledBack(id) TYPE INT NOT NULL")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE CONSTRAINT ON :RolledBack(id) ASSERT UNIQUE")
            .unwrap();
        tx.rollback();
    }
    let output = db
        .query("CREATE CONSTRAINT ON :RolledBack(id) ASSERT UNIQUE")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));

    {
        let mut tx = db.begin_transaction();
        tx.query("CREATE RELATIONSHIP TYPE COMMITTED").unwrap();
        tx.query("CREATE NODE TABLE Committed").unwrap();
        tx.query("CREATE RELATIONSHIP TABLE COMMITTED").unwrap();
        tx.query("CREATE PROPERTY ON NODE TABLE Committed(id) TYPE INT NOT NULL")
            .unwrap();
        tx.query("CREATE INDEX ON :Committed(id)").unwrap();
        tx.query("CREATE INDEX ON :Committed(kind, source_id)")
            .unwrap();
        tx.query("CREATE FULLTEXT INDEX ON :Committed(title)")
            .unwrap();
        tx.query("CREATE CONSTRAINT ON :Committed(id) ASSERT UNIQUE")
            .unwrap();
        let output = tx.commit().unwrap();
        assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[1].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[2].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[3].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[4].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[5].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[6].get("created"), Some(&Value::Bool(true)));
        assert_eq!(output.rows[7].get("created"), Some(&Value::Bool(true)));
    }
    let output = db.query("CREATE RELATIONSHIP TYPE COMMITTED").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db.query("CREATE NODE TABLE Committed").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db.query("CREATE RELATIONSHIP TABLE COMMITTED").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db
        .query("CREATE PROPERTY ON NODE TABLE Committed(id) TYPE INT NOT NULL")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db.query("CREATE INDEX ON :Committed(id)").unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db
        .query("CREATE INDEX ON :Committed(kind, source_id)")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db
        .query("CREATE FULLTEXT INDEX ON :Committed(title)")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
    let output = db
        .query("CREATE CONSTRAINT ON :Committed(id) ASSERT UNIQUE")
        .unwrap();
    assert_eq!(output.rows[0].get("created"), Some(&Value::Bool(false)));
}

#[test]
fn explicit_index_ddl_enables_index_seek_plans() {
    let mut db = Database::new();
    for id in 1..=16 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, title: 'Memory {id}'}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(id)").unwrap();

    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.id AS id")
        .unwrap();

    assert!(explain.physical_plan.explain(0).contains("IndexNodeSeek"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeSeek")));
}

#[test]
fn explicit_index_ddl_enables_index_multi_seek_plans_for_property_in() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'a', title: 'A'})").unwrap();
    db.query("CREATE (:Memory {id: 'b', title: 'B'})").unwrap();
    db.query("CREATE (:Memory {id: 'c', title: 'C'})").unwrap();
    for id in 0..32 {
        db.query(&format!(
            "CREATE (:Memory {{id: 'extra-{id}', title: 'Extra {id}'}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(id)").unwrap();

    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.id IN ['a', 'b', 'a'] RETURN m.id AS id")
        .unwrap();
    assert!(explain
        .physical_plan
        .explain(0)
        .contains("IndexNodeMultiSeek"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeMultiSeek")));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.starts_with("apply implementation:node_in_index_multi_seek:")
    }));

    let output = db
        .query("MATCH (m:Memory) WHERE m.id IN ['a', 'b', 'a'] RETURN m.id AS id ORDER BY id ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("a".to_string()))
    );
    assert_eq!(
        output.rows[1].get("id"),
        Some(&Value::String("b".to_string()))
    );
}

#[test]
fn exact_property_disjunction_uses_bounded_union_seek_and_deduplicates_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'needle', external_id: 'first', title: 'By id'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'second', external_id: 'needle', title: 'By external id'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'needle', external_id: 'needle', title: 'Matches both'})")
        .unwrap();
    for id in 0..32 {
        db.query(&format!(
            "CREATE (:Memory {{id: 'filler-{id}', external_id: 'external-{id}', title: 'Filler {id}'}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    db.query("CREATE INDEX ON :Memory(external_id)").unwrap();

    let cypher = "MATCH (m:Memory) WHERE m.id = $identity OR m.external_id = $identity RETURN m.title AS title ORDER BY title ASC";
    let parameters =
        BTreeMap::from([("identity".to_string(), Value::String("needle".to_string()))]);
    let explain = db.explain_query_with_params(cypher, &parameters).unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("IndexNodeUnionSeek"));
    assert!(physical_plan.contains("NodeProjectionScanExec"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeUnionSeek")));
    assert!(explain.trace.selected_plan.contains("IndexNodeUnionSeek"));

    let output = db.query_with_params(cypher, &parameters).unwrap();
    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row.get("title").cloned().unwrap())
            .collect::<Vec<_>>(),
        vec![
            Value::String("By external id".to_string()),
            Value::String("By id".to_string()),
            Value::String("Matches both".to_string()),
        ]
    );
    let rebound = db
        .query_with_params(
            cypher,
            &BTreeMap::from([("identity".to_string(), Value::String("first".to_string()))]),
        )
        .unwrap();
    assert_eq!(rebound.rows.len(), 1);
    assert_eq!(
        rebound.rows[0].get("title"),
        Some(&Value::String("By id".to_string()))
    );
}

#[test]
fn exact_property_disjunction_requires_every_union_branch_to_be_indexed() {
    let mut db = Database::new();
    for id in 0..32 {
        db.query(&format!(
            "CREATE (:Memory {{id: 'id-{id}', external_id: 'external-{id}'}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(id)").unwrap();

    let explain = db
        .explain_query(
            "MATCH (m:Memory) WHERE m.id = 'id-1' OR m.external_id = 'id-1' RETURN m.id AS id",
        )
        .unwrap();
    assert!(!explain
        .physical_plan
        .explain(0)
        .contains("IndexNodeUnionSeek"));
}

#[test]
fn exact_property_union_declines_more_than_sixty_four_lookup_values() {
    let mut db = Database::new();
    for id in 0..96 {
        db.query(&format!(
            "CREATE (:Memory {{id: 'id-{id}', external_id: 'external-{id}'}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    db.query("CREATE INDEX ON :Memory(external_id)").unwrap();
    let id_values = (0..33)
        .map(|id| format!("'id-{id}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let external_values = (0..33)
        .map(|id| format!("'external-{id}'"))
        .collect::<Vec<_>>()
        .join(", ");

    let explain = db
        .explain_query(&format!(
            "MATCH (m:Memory) WHERE m.id IN [{id_values}] OR m.external_id IN [{external_values}] RETURN m.id AS id"
        ))
        .unwrap();
    assert!(!explain
        .physical_plan
        .explain(0)
        .contains("IndexNodeUnionSeek"));
}

#[test]
fn exact_property_union_deduplication_is_memory_admitted() {
    let execution_memory = crate::executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(64).unwrap(),
        ..crate::executor::ExecutionMemoryConfig::default()
    };
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'needle', external_id: 'first', title: 'One'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'second', external_id: 'needle', title: 'Two'})")
        .unwrap();
    for id in 0..32 {
        db.query(&format!(
            "CREATE (:Memory {{id: 'filler-{id}', external_id: 'external-{id}', title: 'Filler {id}'}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    db.query("CREATE INDEX ON :Memory(external_id)").unwrap();

    let error = db
        .query(
            "MATCH (m:Memory) WHERE m.id = 'needle' OR m.external_id = 'needle' RETURN m.title AS title",
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("operator state would use 128 bytes"));
}

#[test]
fn indexed_property_in_parameter_list_keeps_residual_filters() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'a', title: 'A', lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'b', title: 'B', lifecycle_state: 'archived'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'c', title: 'C', lifecycle_state: 'active'})")
        .unwrap();
    for id in 0..32 {
        db.query(&format!(
            "CREATE (:Memory {{id: 'extra-{id}', title: 'Extra {id}', lifecycle_state: 'active'}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(id)").unwrap();

    let parameters = BTreeMap::from([(
        "ids".to_string(),
        Value::List(vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::String("a".to_string()),
        ]),
    )]);
    let cypher =
        "MATCH (m:Memory) WHERE m.id IN $ids AND m.lifecycle_state = 'active' RETURN m.id AS id";

    let explain = db.explain_query_with_params(cypher, &parameters).unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("IndexNodeMultiSeek"));
    assert!(physical_plan.contains("NodeProjectionScanExec"));
    assert!(physical_plan.contains("predicate=Some"));
    assert!(!physical_plan.contains("FilterExec"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeMultiSeek")));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.starts_with("apply implementation:node_conjunction_index_seek:")
    }));

    let output = db
        .query_with_params(
            "MATCH (m:Memory) WHERE m.id IN $ids AND m.lifecycle_state = 'active' RETURN m.id AS id ORDER BY id ASC",
            &parameters,
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("id"),
        Some(&Value::String("a".to_string()))
    );
}

#[test]
fn composite_index_ddl_enables_composite_index_seek_plans() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'One'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Two'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'task', source_id: 'a', title: 'Three'})")
        .unwrap();
    for id in 4..=16 {
        db.query(&format!(
            "CREATE (:Memory {{kind: 'archive', source_id: 'filler-{id}', title: 'Filler {id}'}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();

    let explain = db
        .explain_query(
            "MATCH (m:Memory) WHERE m.kind = 'note' AND m.source_id = 'a' RETURN m.title AS title",
        )
        .unwrap();

    assert!(explain
        .physical_plan
        .explain(0)
        .contains("IndexNodeCompositeSeek"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeCompositeSeek")));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.starts_with("apply implementation:node_composite_index_seek:")
    }));

    let output = db
        .query(
            "MATCH (m:Memory) WHERE m.kind = 'note' AND m.source_id = 'a' RETURN m.title AS title",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("One".to_string()))
    );

    db.query("MATCH (m:Memory) WHERE m.title = 'Two' SET m.source_id = 'a'")
        .unwrap();
    let output = db
            .query(
                "MATCH (m:Memory) WHERE m.kind = 'note' AND m.source_id = 'a' RETURN m.title AS title ORDER BY title ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("One".to_string()))
    );
    assert_eq!(
        output.rows[1].get("title"),
        Some(&Value::String("Two".to_string()))
    );
}

#[test]
fn composite_index_uses_leading_equalities_and_next_column_range() {
    let mut db = Database::new();
    for created_at in 0..64 {
        let kind = if created_at < 48 { "note" } else { "archive" };
        db.query(&format!(
            "CREATE (:Memory {{id: 'memory-{created_at}', kind: '{kind}', created_at: {created_at}, payload: '{}'}})",
            "x".repeat(1024)
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(kind, created_at)")
        .unwrap();
    let cypher = "MATCH (m:Memory) WHERE m.kind = 'note' AND m.created_at >= $lower AND m.created_at <= $upper RETURN m.created_at AS created_at ORDER BY created_at ASC";
    let parameters = BTreeMap::from([
        ("lower".to_string(), Value::Int(10)),
        ("upper".to_string(), Value::Int(20)),
    ]);

    let explain = db.explain_query_with_params(cypher, &parameters).unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("IndexNodeCompositeRangeSeek"));
    assert!(physical_plan.contains("NodeProjectionScanExec"));
    assert!(!physical_plan.contains("payload"));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.contains("choose IndexNodeCompositeRangeSeek")
            && decision.contains("equality_prefix_len=1")
    }));

    let output = db.query_with_params(cypher, &parameters).unwrap();
    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row.get("created_at").cloned().unwrap())
            .collect::<Vec<_>>(),
        (10..=20).map(Value::Int).collect::<Vec<_>>()
    );

    let rebound = db
        .query_with_params(
            cypher,
            &BTreeMap::from([
                ("lower".to_string(), Value::Int(30)),
                ("upper".to_string(), Value::Int(31)),
            ]),
        )
        .unwrap();
    assert_eq!(
        rebound
            .rows
            .iter()
            .map(|row| row.get("created_at").cloned().unwrap())
            .collect::<Vec<_>>(),
        vec![Value::Int(30), Value::Int(31)]
    );
}

#[test]
fn composite_range_requires_a_leading_equality_prefix() {
    let mut db = Database::new();
    for created_at in 0..32 {
        db.query(&format!(
            "CREATE (:Memory {{kind: 'note', created_at: {created_at}}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(kind, created_at)")
        .unwrap();

    let explain = db
        .explain_query(
            "MATCH (m:Memory) WHERE m.created_at >= 10 RETURN m.created_at AS created_at",
        )
        .unwrap();
    assert!(!explain
        .physical_plan
        .explain(0)
        .contains("IndexNodeCompositeRangeSeek"));
}

#[test]
fn full_text_index_ddl_enables_text_seek_plans() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Vector search'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Graph query planning'})")
        .unwrap();
    for id in 4..=16 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, title: 'Vector filler {id}'}})"
        ))
        .unwrap();
    }

    let scan = db
        .explain_query("MATCH (m:Memory) WHERE m.title CONTAINS 'raph' RETURN m.id AS id")
        .unwrap();
    assert!(scan
        .physical_plan
        .explain(0)
        .contains("NodeProjectionScanExec"));
    assert!(!scan.physical_plan.explain(0).contains("IndexNodeTextSeek"));

    db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.title CONTAINS 'raph' RETURN m.id AS id")
        .unwrap();
    assert!(explain
        .physical_plan
        .explain(0)
        .contains("IndexNodeTextSeek"));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeTextSeek")));
    assert!(explain
        .trace
        .decisions
        .iter()
        .any(|decision| { decision.starts_with("apply implementation:node_text_index_seek:") }));

    let output = db
        .query("MATCH (m:Memory) WHERE m.title CONTAINS 'Graph' RETURN m.id AS id ORDER BY id ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(3)));

    db.query("MATCH (m:Memory) WHERE m.id = 2 SET m.title = 'Graph search'")
        .unwrap();
    let output = db
        .query("MATCH (m:Memory) WHERE m.title CONTAINS 'Graph' RETURN m.id AS id ORDER BY id ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(2)));
    assert_eq!(output.rows[2].get("id"), Some(&Value::Int(3)));
}

#[test]
fn bounded_property_index_projection_rebuild_skips_descriptors_over_budget_without_wal_write() {
    let path = unique_test_dir("bounded_index_projection_rebuild_skip");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'Graph foundations'})")
            .unwrap();
        db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Vector search'})")
            .unwrap();
        db.query("CREATE (:Memory {kind: 'task', source_id: 'a', title: 'Graph query planning'})")
            .unwrap();
        db.query("CREATE INDEX ON :Memory(kind, source_id)")
            .unwrap();
        db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
        let before = read_test_wal(&path).unwrap();

        let output = db.rebuild_bounded_property_index_projections(2);

        assert!(output.rows.is_empty());
        let after = read_test_wal(&path).unwrap();
        assert_eq!(after, before);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_property_index_projection_rebuild_reports_descriptor_batches() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Vector search'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'task', source_id: 'a', title: 'Graph query planning'})")
        .unwrap();
    db.query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();
    db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();

    let first = db.rebuild_bounded_property_index_projections(3);

    assert_eq!(first.rows.len(), 1);
    assert_eq!(
        first.rows[0].get("index_kind"),
        Some(&Value::String("composite".to_string()))
    );
    assert_eq!(
        first.rows[0].get("label"),
        Some(&Value::String("Memory".to_string()))
    );
    assert_eq!(
        first.rows[0].get("properties"),
        Some(&Value::List(vec![
            Value::String("kind".to_string()),
            Value::String("source_id".to_string())
        ]))
    );
    assert_eq!(
        first.rows[0].get("estimated_operations"),
        Some(&Value::Int(3))
    );
    assert_eq!(first.rows[0].get("indexed_entries"), Some(&Value::Int(3)));

    let second = db.rebuild_bounded_property_index_projections(6);

    assert_eq!(second.rows.len(), 2);
    assert_eq!(
        second.rows[1].get("index_kind"),
        Some(&Value::String("full_text".to_string()))
    );
    assert_eq!(
        second.rows[1].get("properties"),
        Some(&Value::List(vec![Value::String("title".to_string())]))
    );
    assert_eq!(
        second.rows[1].get("estimated_operations"),
        Some(&Value::Int(3))
    );
    assert!(matches!(
        second.rows[1].get("indexed_entries"),
        Some(Value::Int(value)) if *value > 0
    ));
}

#[test]
fn property_index_projection_background_work_plan_is_absent_without_descriptors() {
    let db = Database::new();

    assert!(db
        .property_index_projection_background_work_plan(BackgroundWorkHint::default())
        .is_none());
}

#[test]
fn property_index_projection_background_work_plan_uses_projection_lane() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Vector search'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'task', source_id: 'a', title: 'Graph query planning'})")
        .unwrap();
    db.query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();
    db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();

    let plan = db
        .property_index_projection_background_work_plan(BackgroundWorkHint {
            active_topic: true,
            query_probability_per_million: 100_000,
            ..BackgroundWorkHint::default()
        })
        .unwrap();

    assert_eq!(plan.request.class, crate::WorkClass::Projection);
    assert_eq!(plan.request.estimated_operations, 6);
    let ranked = LocalQosPolicy::default().rank_background_work(&LocalQosState::default(), &[plan]);
    assert_eq!(ranked.len(), 1);
    assert_eq!(ranked[0].index, 0);
    assert!(ranked[0]
        .decision
        .reasons
        .iter()
        .any(|reason| reason == "active topic"));
}

#[test]
fn bounded_background_property_index_projection_rebuild_defers_without_rebuilding() {
    let path = unique_test_dir("bounded_background_index_projection_defer");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'Graph foundations'})")
            .unwrap();
        db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Vector search'})")
            .unwrap();
        db.query("CREATE INDEX ON :Memory(kind, source_id)")
            .unwrap();
        let before = read_test_wal(&path).unwrap();
        let policy = LocalQosPolicy {
            max_background_operations: Some(1),
            ..LocalQosPolicy::default()
        };

        let error = db
            .rebuild_bounded_background_property_index_projections(
                &policy,
                &LocalQosState::default(),
                2,
            )
            .unwrap_err();

        assert!(error.to_string().contains("deferred"));
        let after = read_test_wal(&path).unwrap();
        assert_eq!(after, before);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_background_property_index_projection_rebuild_admits_actual_batch_estimate() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Vector search'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'task', source_id: 'a', title: 'Graph query planning'})")
        .unwrap();
    db.query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();
    db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
    let policy = LocalQosPolicy {
        max_background_operations: Some(3),
        ..LocalQosPolicy::default()
    };

    let output = db
        .rebuild_bounded_background_property_index_projections(
            &policy,
            &LocalQosState::default(),
            4,
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("index_kind"),
        Some(&Value::String("composite".to_string()))
    );
    assert_eq!(
        output.rows[0].get("estimated_operations"),
        Some(&Value::Int(3))
    );
}

#[test]
fn bounded_scheduled_background_property_index_projection_rebuild_releases_budget() {
    let mut class_limits = [None; crate::WORK_CLASS_COUNT];
    class_limits[crate::WorkClass::Projection.as_index()] = Some(3);
    let mut db = Database::new_with_config(DatabaseConfig {
        local_qos_policy: LocalQosPolicy {
            max_background_operations: Some(4),
            max_total_background_operations: Some(4),
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        },
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'Graph foundations'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Vector search'})")
        .unwrap();
    db.query("CREATE (:Memory {kind: 'task', source_id: 'a', title: 'Graph query planning'})")
        .unwrap();
    db.query("CREATE INDEX ON :Memory(kind, source_id)")
        .unwrap();
    db.query("CREATE FULLTEXT INDEX ON :Memory(title)").unwrap();
    let scheduler = db.local_qos_scheduler();

    let output = db
        .rebuild_bounded_scheduled_background_property_index_projections(3)
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("index_kind"),
        Some(&Value::String("composite".to_string()))
    );
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Projection.as_index()],
        0
    );
}

#[test]
fn range_predicates_filter_and_use_range_index() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, created_at: 10, title: 'Old'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, created_at: 20, title: 'Current'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, created_at: 30, title: 'Future'})")
        .unwrap();
    for id in 4..=16 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, created_at: -{id}, title: 'Filler {id}'}})"
        ))
        .unwrap();
    }

    let scan = db
        .explain_query("MATCH (m:Memory) WHERE m.created_at >= 20 RETURN m.id AS id")
        .unwrap();
    assert!(scan
        .physical_plan
        .explain(0)
        .contains("NodeProjectionScanExec"));
    assert!(!scan.physical_plan.explain(0).contains("IndexNodeRangeSeek"));

    db.query("CREATE RANGE INDEX ON :Memory(created_at)")
        .unwrap();
    let indexed = db
        .explain_query("MATCH (m:Memory) WHERE m.created_at >= 20 RETURN m.id AS id")
        .unwrap();
    assert!(indexed
        .physical_plan
        .explain(0)
        .contains("IndexNodeRangeSeek"));
    assert!(indexed
        .trace
        .decisions
        .iter()
        .any(|decision| decision.contains("choose IndexNodeRangeSeek")));
    assert!(indexed
        .trace
        .decisions
        .iter()
        .any(|decision| { decision.starts_with("apply implementation:node_range_index_seek:") }));

    let output = db
        .query("MATCH (m:Memory) WHERE m.created_at >= 20 RETURN m.id AS id ORDER BY id ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(3)));
}

#[test]
fn and_range_predicates_use_bounded_range_index_with_residual_filter() {
    let mut db = Database::new();
    for (id, created_at) in [(1, 5), (2, 10), (3, 15), (4, 20), (5, 25)] {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, created_at: {created_at}}})"
        ))
        .unwrap();
    }
    for id in 6..=16 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}, created_at: {}}})",
            100 + id
        ))
        .unwrap();
    }
    db.query("CREATE RANGE INDEX ON :Memory(created_at)")
        .unwrap();

    let explain = db
        .explain_query(
            "MATCH (m:Memory) WHERE m.created_at >= 10 AND m.created_at < 20 RETURN m.id AS id",
        )
        .unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("NodeProjectionScanExec"));
    assert!(physical_plan.contains("predicate=Some"));
    assert!(!physical_plan.contains("FilterExec"));
    assert!(physical_plan.contains("IndexNodeRangeSeek"));
    assert!(physical_plan.contains("lower: Some((Int(10), true))"));
    assert!(physical_plan.contains("upper: Some((Int(20), false))"));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.contains("choose IndexNodeRangeSeek") && decision.contains("in conjunction")
    }));

    let output = db
            .query(
                "MATCH (m:Memory) WHERE m.created_at >= 10 AND m.created_at < 20 RETURN m.id AS id ORDER BY id ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].get("id"), Some(&Value::Int(2)));
    assert_eq!(output.rows[1].get("id"), Some(&Value::Int(3)));
}

#[test]
fn range_selectivity_statistics_drive_range_index_costing() {
    let mut db = Database::new();
    for id in 0..100 {
        db.query(&format!("CREATE (:Memory {{id: {id}, created_at: {id}}})"))
            .unwrap();
    }
    db.query("CREATE RANGE INDEX ON :Memory(created_at)")
        .unwrap();

    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.created_at > 98 RETURN m.id AS id")
        .unwrap();
    assert!(explain
        .physical_plan
        .explain(0)
        .contains("IndexNodeRangeSeek"));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.contains("choose IndexNodeRangeSeek") && decision.contains("estimated_rows=1")
    }));
}

#[test]
fn relationship_property_statistics_drive_expand_costing() {
    let mut db = Database::new();
    for id in 0..10 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}}})-[:MENTIONS {{weight: {id}}}]->(:Entity {{id: {}}})",
            id + 100
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Memory(id)").unwrap();

    let explain = db
            .explain_query(
                "MATCH (m:Memory {id: 1})-[r:MENTIONS]->(e:Entity) WHERE r.weight = 1 RETURN r.weight AS weight",
            )
            .unwrap();
    let physical_plan = explain.physical_plan.explain(0);

    assert!(physical_plan.contains("AdjacencyExpandExec"));
    assert!(physical_plan.contains(r#"properties={"weight": Int(1)}"#));
    assert!(explain.trace.decisions.iter().any(|decision| {
        decision.contains("estimate AdjacencyExpand")
            && decision.contains("rel_property_distinct_product=10")
            && decision.contains("estimated_rows=1")
    }));
    assert_eq!(
        explain.trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 10,
        }
    );
}

#[test]
fn relationship_property_histograms_drive_filter_range_costing() {
    let mut db = Database::new();
    for id in 0..10 {
        db.query(&format!(
            "CREATE (:Memory {{id: {id}}})-[:MENTIONS {{created_at: {id}}}]->(:Entity {{id: {}}})",
            id + 100
        ))
        .unwrap();
    }

    let explain = db
            .explain_query(
                "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE r.created_at > 8 RETURN r.created_at AS created_at",
            )
            .unwrap();
    let physical_plan = explain.physical_plan.explain(0);

    assert!(physical_plan.contains("FilterExec"));
    assert!(physical_plan.contains("PropertyCompare"));
    assert_eq!(
        explain.trace.selected_plan_cost,
        PlanCost {
            estimated_rows: 1,
            cost: 65,
        }
    );
}

#[test]
fn property_histograms_are_bounded_deterministic_samples() {
    let mut db = Database::new();
    for id in 0..200 {
        db.query(&format!("CREATE (:Memory {{score: {id}}})"))
            .unwrap();
    }

    let statistics = db.statistics();
    assert_eq!(statistics.computed_at_commit_epoch, 200);
    assert_eq!(statistics.histogram_sample_limit, 512);
    let ((_, property), distinct_count) = statistics
        .property_distinct_counts
        .iter()
        .find(|((_, property), _)| property == "score")
        .unwrap();
    assert_eq!(property, "score");
    assert_eq!(*distinct_count, 200);

    let histogram = statistics
        .property_histograms
        .iter()
        .find_map(|((_, property), values)| (property == "score").then_some(values))
        .unwrap();
    assert_eq!(histogram.len(), 128);
    assert_eq!(histogram.first(), Some(&Value::Int(0)));
    assert_eq!(histogram.last(), Some(&Value::Int(199)));
    let sampled = statistics
        .sampled_property_histograms
        .iter()
        .find_map(|((_, property), sampled)| (property == "score").then_some(sampled))
        .unwrap();
    assert!(*sampled);

    let explain = db
        .explain_query("MATCH (m:Memory) WHERE m.score < 10 RETURN m.score AS score")
        .unwrap();
    let scan_estimate = explain
        .trace
        .selected_plan_cardinality_estimates
        .iter()
        .find(|estimate| estimate.operator == hawdb_plan::PhysicalPlanKind::NodeProjectionScanExec)
        .unwrap();
    assert_eq!(scan_estimate.estimated_rows, 13);

    db.query("CREATE (:Memory {exact_score: 1})").unwrap();
    db.query("CREATE (:Memory {exact_score: 2})").unwrap();
    let statistics = db.statistics();
    let sampled = statistics
        .sampled_property_histograms
        .iter()
        .find_map(|((_, property), sampled)| (property == "exact_score").then_some(sampled))
        .unwrap();
    assert!(!sampled);
}

#[test]
fn optimizer_statistics_exclude_text_large_and_mixed_property_groups() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Metric").unwrap();
    db.query("CREATE RELATIONSHIP TABLE MEASURES").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Metric(text_value) TYPE TEXT")
        .unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Metric(varchar_value) TYPE VARCHAR")
        .unwrap();
    db.query("CREATE PROPERTY ON RELATIONSHIP TABLE MEASURES(text_value) TYPE TEXT")
        .unwrap();
    db.query(
        "CREATE PROPERTY ON RELATIONSHIP TABLE MEASURES(varchar_value) TYPE CHARACTER VARYING",
    )
    .unwrap();
    db.query_with_params(
        "CREATE (:Metric {score: 1, text_value: $text, varchar_value: 'short', list_value: $list, map_value: $map, mixed_value: 1})",
        &BTreeMap::from([
            ("text".to_string(), Value::String("large text".to_string())),
            (
                "list".to_string(),
                Value::List(vec![Value::Int(1), Value::Int(2)]),
            ),
            (
                "map".to_string(),
                Value::Map(BTreeMap::from([("key".to_string(), Value::Int(1))])),
            ),
        ]),
    )
    .unwrap();
    db.query_with_params(
        "CREATE (:Metric {score: 2, varchar_value: 'other', mixed_value: $mixed})",
        &BTreeMap::from([(
            "mixed".to_string(),
            Value::Map(BTreeMap::from([("nested".to_string(), Value::Int(1))])),
        )]),
    )
    .unwrap();
    db.query(
        "CREATE (:Source)-[:MEASURES {score: 3, text_value: 'payload', varchar_value: 'kind', list_value: [1, 2]}]->(:Target)",
    )
    .unwrap();

    let statistics = db.statistics();
    let node_properties = statistics
        .property_distinct_counts
        .keys()
        .map(|(_, property)| property.as_str())
        .collect::<BTreeSet<_>>();
    let relationship_properties = statistics
        .rel_property_distinct_counts
        .keys()
        .map(|(_, property)| property.as_str())
        .collect::<BTreeSet<_>>();

    assert!(node_properties.contains("score"));
    assert!(node_properties.contains("varchar_value"));
    assert!(!node_properties.contains("text_value"));
    assert!(!node_properties.contains("list_value"));
    assert!(!node_properties.contains("map_value"));
    assert!(!node_properties.contains("mixed_value"));
    assert_eq!(
        relationship_properties,
        BTreeSet::from(["score", "varchar_value"])
    );
    assert!(statistics
        .property_histograms
        .values()
        .flatten()
        .all(|value| matches!(
            value,
            Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::String(_)
        )));
    assert!(statistics
        .rel_property_histograms
        .values()
        .flatten()
        .all(|value| matches!(
            value,
            Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::String(_)
        )));
}

#[test]
fn range_index_descriptor_persists_through_wal_and_checkpoint() {
    let path = unique_test_dir("range_index_descriptor");
    {
        let mut db = Database::open(&path).unwrap();
        let first = db
            .query("CREATE RANGE INDEX ON :Memory(created_at)")
            .unwrap();
        let second = db
            .query("CREATE RANGE INDEX ON :Memory(created_at)")
            .unwrap();
        assert_eq!(first.rows[0].get("created"), Some(&Value::Bool(true)));
        assert_eq!(second.rows[0].get("created"), Some(&Value::Bool(false)));
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_range_index"));

    {
        let mut db = Database::open(&path).unwrap();
        assert!(db
            .property_indexes()
            .iter()
            .any(|index| { index.property == "created_at" && index.kind == IndexKind::Range }));
        db.checkpoint().unwrap();
    }
    assert_eq!(read_test_wal(&path).unwrap(), "");
    let checkpoint = read_test_durable_text(&active_checkpoint_path(&path)).unwrap();
    assert!(checkpoint.contains("property_index"));
    assert!(checkpoint.contains("range"));

    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .property_indexes()
            .iter()
            .any(|index| { index.property == "created_at" && index.kind == IndexKind::Range }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn unique_constraint_rejects_duplicate_create_and_set_before_wal() {
    let path = unique_test_dir("unique_constraint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
        db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
            .unwrap();

        let error = db
            .query("CREATE (:Memory {id: 1, title: 'Duplicate'})")
            .unwrap_err();
        assert!(error.to_string().contains("unique constraint violation"));

        let error = db
            .query("MATCH (m:Memory) WHERE m.id = 2 SET m.id = 1")
            .unwrap_err();
        assert!(error.to_string().contains("unique constraint violation"));

        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.title AS title")
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Two".to_string()))
        );
    }
    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .unique_constraints()
            .iter()
            .any(|constraint| constraint.property == "id"));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn unique_constraint_rejects_existing_duplicate_data() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 1, title: 'Duplicate'})")
        .unwrap();

    let error = db
        .query("CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE")
        .unwrap_err();
    assert!(error.to_string().contains("unique constraint violation"));
    assert!(db.unique_constraints().is_empty());
}

#[test]
fn node_property_exists_constraint_rejects_missing_and_null_writes_before_wal() {
    let path = unique_test_dir("property_exists_constraint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
        db.query("CREATE CONSTRAINT ON :Memory(id) ASSERT EXISTS")
            .unwrap();

        let error = db.query("CREATE (:Memory {title: 'Missing'})").unwrap_err();
        assert!(error
            .to_string()
            .contains("node property exists constraint violation"));

        let error = db
            .query("MATCH (m:Memory) WHERE m.id = 1 SET m.id = null")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("node property exists constraint violation"));

        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("One".to_string()))
        );
    }
    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .node_property_exists_constraints()
            .iter()
            .any(|constraint| {
                constraint.property == "id" && constraint.kind == ConstraintKind::NodePropertyExists
            }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn node_property_exists_constraint_rejects_existing_bad_data() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {title: 'Missing'})").unwrap();

    let error = db
        .query("CREATE CONSTRAINT ON :Memory(id) ASSERT NOT NULL")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("node property exists constraint violation"));
    assert!(db.node_property_exists_constraints().is_empty());
}

#[test]
fn relationship_property_exists_constraint_rejects_missing_and_null_writes_before_wal() {
    let path = unique_test_dir("relationship_property_exists_constraint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1})-[:MENTIONS {weight: 1}]->(:Memory {id: 2})")
            .unwrap();
        db.query("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT EXISTS")
            .unwrap();

        let error = db
            .query("CREATE (:Memory {id: 3})-[:MENTIONS]->(:Memory {id: 4})")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("relationship property exists constraint violation"));

        let error = db
            .query("CREATE (:Memory {id: 5})-[:MENTIONS {weight: null}]->(:Memory {id: 6})")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("relationship property exists constraint violation"));
    }
    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .relationship_property_exists_constraints()
            .iter()
            .any(|constraint| {
                constraint.property == "weight"
                    && constraint.kind == ConstraintKind::RelationshipPropertyExists
                    && matches!(constraint.subject, ConstraintSubject::Relationship(_))
            }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn relationship_property_exists_constraint_rejects_existing_bad_data() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:MENTIONS {weight: 1}]->(:Memory {id: 2})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3})-[:MENTIONS]->(:Memory {id: 4})")
        .unwrap();

    let error = db
        .query("CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT NOT NULL")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("relationship property exists constraint violation"));
    assert!(db.relationship_property_exists_constraints().is_empty());
}

#[test]
fn relationship_unique_constraint_rejects_duplicate_writes_before_wal() {
    let path = unique_test_dir("relationship_unique_constraint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1})-[:MENTIONS {id: 10}]->(:Memory {id: 2})")
            .unwrap();
        db.query("CREATE CONSTRAINT ON -[:MENTIONS(id)]-> ASSERT UNIQUE")
            .unwrap();

        let error = db
            .query("CREATE (:Memory {id: 3})-[:MENTIONS {id: 10}]->(:Memory {id: 4})")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("relationship unique constraint violation"));

        db.query("CREATE (:Memory {id: 5})-[:MENTIONS]->(:Memory {id: 6})")
            .unwrap();
        db.query("CREATE (:Memory {id: 7})-[:MENTIONS {id: null}]->(:Memory {id: 8})")
            .unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .relationship_unique_constraints()
            .iter()
            .any(|constraint| {
                constraint.property == "id"
                    && constraint.kind == ConstraintKind::RelationshipPropertyUnique
                    && matches!(constraint.subject, ConstraintSubject::Relationship(_))
            }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn relationship_unique_constraint_rejects_existing_duplicate_data() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:MENTIONS {id: 10}]->(:Memory {id: 2})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3})-[:MENTIONS {id: 10}]->(:Memory {id: 4})")
        .unwrap();

    let error = db
        .query("CREATE CONSTRAINT ON -[:MENTIONS(id)]-> ASSERT UNIQUE")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("relationship unique constraint violation"));
    assert!(db.relationship_unique_constraints().is_empty());
}

#[test]
fn property_schema_rejects_invalid_writes_before_wal() {
    let path = unique_test_dir("property_schema_write");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(weight) TYPE INT")
            .unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
        let before = read_test_wal(&path).unwrap();

        let error = db
            .query("CREATE (:Memory {id: 'bad', title: 'Bad'})")
            .unwrap_err();
        assert!(error.to_string().contains("property schema violation"));
        let error = db
            .query("MATCH (m:Memory) WHERE m.id = 1 SET m.id = 'bad'")
            .unwrap_err();
        assert!(error.to_string().contains("property schema violation"));
        let error = db
            .query("CREATE (:Memory {id: 2})-[:MENTIONS {weight: 'bad'}]->(:Entity {name: 'Rust'})")
            .unwrap_err();
        assert!(error.to_string().contains("property schema violation"));

        let after = read_test_wal(&path).unwrap();
        assert_eq!(before, after);
        let output = db
            .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
            .unwrap();
        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("One".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn property_schema_rejects_existing_invalid_data() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'bad', title: 'Bad'})")
        .unwrap();

    let error = db
        .query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
        .unwrap_err();
    assert!(error.to_string().contains("property schema violation"));
    assert!(db.property_descriptors().is_empty());
}

#[test]
fn failed_transaction_does_not_publish_property_schema_or_wal() {
    let path = unique_test_dir("property_schema_failed_transaction");
    {
        let mut db = Database::open(&path).unwrap();
        let before = read_test_wal(&path).unwrap_or_default();
        let mut tx = db.begin_transaction();
        tx.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        let error = tx.query("CREATE (:Memory {id: 'bad'})").unwrap_err();
        assert!(error.to_string().contains("property schema violation"));
        tx.rollback();
        assert!(db.property_descriptors().is_empty());
        let after = read_test_wal(&path).unwrap_or_default();
        assert_eq!(before, after);
    }
    std::fs::remove_dir_all(path).unwrap();
}

/// Only declared properties are indexed, so a query filtering on an
/// undeclared one has to reach every matching node through a scan. Before
/// the pruner learned to decline, it would have consulted an index that
/// never received those nodes and returned nothing.
#[test]
fn undeclared_property_filters_still_return_every_match() {
    let mut db = Database::new();
    db.query("CREATE NODE LABEL Memory").unwrap();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    db.query("CREATE (:Memory {id: 1, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'decision'})")
        .unwrap();

    // `kind` has no declared index; the rows must still be found.
    let notes = db
        .query("MATCH (m:Memory) WHERE m.kind = 'note' RETURN m.id AS id ORDER BY id")
        .unwrap();
    assert_eq!(notes.rows.len(), 2);
    assert_eq!(notes.rows[0].get("id"), Some(&Value::Int(1)));
    assert_eq!(notes.rows[1].get("id"), Some(&Value::Int(2)));

    // The negative and null-shaped filters take separate pruning branches,
    // and each one would have read the same empty index.
    let others = db
        .query("MATCH (m:Memory) WHERE m.kind <> 'note' RETURN m.id AS id")
        .unwrap();
    assert_eq!(others.rows.len(), 1);
    let present = db
        .query("MATCH (m:Memory) WHERE m.kind IS NOT NULL RETURN m.id AS id")
        .unwrap();
    assert_eq!(present.rows.len(), 3);

    // The declared property keeps its index path and its result.
    let by_id = db
        .query("MATCH (m:Memory) WHERE m.id = 2 RETURN m.id AS id")
        .unwrap();
    assert_eq!(by_id.rows.len(), 1);
}

/// Declaring an index after the rows exist must backfill them. The pruner
/// treats a declared index as complete, so an unbackfilled one makes the
/// query omit rows rather than run slowly.
#[test]
fn index_declared_after_writes_backfills_existing_nodes() {
    let mut db = Database::new();
    db.query("CREATE NODE LABEL Memory").unwrap();
    db.query("CREATE (:Memory {id: 1, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'note'})").unwrap();

    db.query("CREATE INDEX ON :Memory(kind)").unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'note'})").unwrap();

    let notes = db
        .query("MATCH (m:Memory) WHERE m.kind = 'note' RETURN m.id AS id ORDER BY id")
        .unwrap();
    assert_eq!(
        notes.rows.len(),
        3,
        "nodes written before the index was declared must be backfilled"
    );
}
