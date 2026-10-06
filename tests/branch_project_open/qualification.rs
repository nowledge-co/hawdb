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

use super::Project;
use hawdb::{Database, DurabilityPolicy, Value};
use std::collections::BTreeMap;

fn select(database: &mut Database, name: &str) {
    database
        .query_sql_with_params("USE BRANCH NAME $1", &[Value::String(name.into())])
        .unwrap();
}

fn assert_branch_state(database: &mut Database, name: &str, applied_at: i64) {
    let rows = database
        .query_sql("SELECT id, owner, tag FROM records ORDER BY id")
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
    assert_eq!(rows.rows[0]["owner"], Value::String("seed".into()));
    assert_eq!(rows.rows[0]["tag"], Value::Null);
    assert_eq!(rows.rows[1]["owner"], Value::String(name.into()));
    assert_eq!(rows.rows[1]["tag"], Value::String(format!("tag-{name}")));
    let migrations = database
        .query_with_params(
            "MATCH (m:SchemaMigrationLog) WHERE m.id = $id RETURN m.applied_at AS applied_at",
            &BTreeMap::from([("id".into(), Value::String("branch-local-migration".into()))]),
        )
        .unwrap();
    assert_eq!(migrations.rows.len(), 1);
    assert_eq!(migrations.rows[0]["applied_at"], Value::Int(applied_at));
    let epoch = database.commit_epoch().unwrap();
    for (id, owner, tag, reason) in [
        (2, "seed".to_string(), "unused-tag".to_string(), "unique"),
        (
            3,
            "unused-owner".to_string(),
            format!("tag-{name}"),
            "unique",
        ),
        (
            1,
            "another-owner".to_string(),
            "another-tag".to_string(),
            "primary",
        ),
    ] {
        let error = database
            .query_sql_with_params(
                "INSERT INTO records (id, owner, tag) VALUES ($1, $2, $3)",
                &[Value::Int(id), Value::String(owner), Value::String(tag)],
            )
            .unwrap_err();
        assert!(
            error.to_string().to_ascii_lowercase().contains(reason),
            "{error}"
        );
        assert_eq!(database.commit_epoch().unwrap(), epoch);
    }
    assert!(database.query_sql("SELECT id FROM rolled_back").is_err());
    let missing = database
        .query("MATCH (m:SchemaMigrationLog) WHERE m.id = 'rolled-back' RETURN m.id AS id")
        .unwrap();
    assert!(missing.rows.is_empty());
}

#[test]
fn branch_schema_constraints_indexes_and_migrations_survive_every_reopen_order() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        let project = Project::new();
        let mut database = Database::open_with_durability(&project.0, durability).unwrap();
        database
            .query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, owner TEXT NOT NULL UNIQUE)")
            .unwrap();
        database
            .query_sql("INSERT INTO records (id, owner) VALUES (0, 'seed')")
            .unwrap();
        database
            .query("CREATE CONSTRAINT ON :SchemaMigrationLog(id) ASSERT UNIQUE")
            .unwrap();
        database.checkpoint().unwrap();
        let revision = database.commit_epoch().unwrap();
        for name in ["child", "sibling"] {
            database
                .query_sql_with_params(
                    "CREATE BRANCH NAME $1 FROM main AT REVISION $2 REQUEST KEY $3",
                    &[
                        Value::String(name.into()),
                        Value::Int(i64::try_from(revision).unwrap()),
                        Value::String(format!("create-{name}")),
                    ],
                )
                .unwrap();
        }
        for (name, applied_at) in [("main", 10), ("child", 20), ("sibling", 30)] {
            select(&mut database, name);
            database
                .query_sql("ALTER TABLE records ADD COLUMN tag TEXT")
                .unwrap();
            database
                .query_sql("CREATE UNIQUE INDEX records_tag_unique ON records (tag)")
                .unwrap();
            database
                .query_sql_with_params(
                    "INSERT INTO records (id, owner, tag) VALUES (1, $1, $2)",
                    &[
                        Value::String(name.into()),
                        Value::String(format!("tag-{name}")),
                    ],
                )
                .unwrap();
            let migration_parameters = BTreeMap::from([
                ("id".into(), Value::String("branch-local-migration".into())),
                ("applied_at".into(), Value::Int(applied_at)),
            ]);
            let record_migration =
                "CREATE (:SchemaMigrationLog {id: $id, applied_at: $applied_at})";
            database
                .query_with_params(record_migration, &migration_parameters)
                .unwrap();
            let epoch = database.commit_epoch().unwrap();
            let error = database
                .query_with_params(record_migration, &migration_parameters)
                .unwrap_err();
            assert!(
                error.to_string().to_ascii_lowercase().contains("unique"),
                "{error}"
            );
            assert_eq!(database.commit_epoch().unwrap(), epoch);
            {
                let mut transaction = database.begin_transaction().unwrap();
                transaction
                    .query_sql("CREATE TABLE rolled_back (id BIGINT PRIMARY KEY)")
                    .unwrap();
                transaction
                    .query_sql("INSERT INTO rolled_back (id) VALUES (1)")
                    .unwrap();
                transaction
                    .query("CREATE (:SchemaMigrationLog {id: 'rolled-back', applied_at: -1})")
                    .unwrap();
                transaction.rollback();
            }
            assert_branch_state(&mut database, name, applied_at);
            database.checkpoint().unwrap();
        }
        drop(database);
        for order in [
            ["main", "child", "sibling"],
            ["main", "sibling", "child"],
            ["child", "main", "sibling"],
            ["child", "sibling", "main"],
            ["sibling", "main", "child"],
            ["sibling", "child", "main"],
        ] {
            for name in order {
                let mut database = Database::open_with_durability(&project.0, durability).unwrap();
                select(&mut database, name);
                let applied_at = match name {
                    "main" => 10,
                    "child" => 20,
                    "sibling" => 30,
                    _ => unreachable!(),
                };
                assert_branch_state(&mut database, name, applied_at);
            }
        }
    }
}

#[test]
fn branch_read_snapshot_keeps_schema_and_data_through_atomic_ddl_publication() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        let project = Project::new();
        let mut database = Database::open_with_durability(&project.0, durability).unwrap();
        database
            .query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, body TEXT)")
            .unwrap();
        database
            .query_sql("INSERT INTO records (id, body) VALUES (1, 'old')")
            .unwrap();
        let revision = database.commit_epoch().unwrap();
        database
            .query_sql_with_params(
                "CREATE BRANCH child FROM main AT REVISION $1 REQUEST KEY 'snapshot-child'",
                &[Value::Int(i64::try_from(revision).unwrap())],
            )
            .unwrap();
        select(&mut database, "child");
        let old = database.begin_read_transaction().unwrap();
        let epoch = old.commit_epoch();
        let mut database = std::thread::spawn(move || {
            let mut transaction = database.begin_transaction().unwrap();
            transaction
                .query_sql("ALTER TABLE records ADD COLUMN tag TEXT")
                .unwrap();
            transaction
                .query_sql("INSERT INTO records (id, body, tag) VALUES (2, 'new', 'committed')")
                .unwrap();
            transaction.commit().unwrap();
            database
        })
        .join()
        .unwrap();
        database.checkpoint().unwrap();
        let previous = old
            .query_sql("SELECT id, body FROM records ORDER BY id")
            .unwrap();
        assert_eq!(previous.rows.len(), 1);
        assert_eq!(previous.rows[0]["body"], Value::String("old".into()));
        assert_eq!(old.commit_epoch(), epoch);
        assert!(old.query_sql("SELECT tag FROM records").is_err());
        let current = database
            .query_sql("SELECT id, body, tag FROM records ORDER BY id")
            .unwrap();
        assert_eq!(current.rows.len(), 2);
        assert_eq!(current.rows[0]["tag"], Value::Null);
        assert_eq!(current.rows[1]["tag"], Value::String("committed".into()));
        assert_eq!(database.commit_epoch().unwrap(), epoch + 1);
        drop(old);
        select(&mut database, "main");
        assert!(database.query_sql("SELECT tag FROM records").is_err());
        assert_eq!(
            database
                .query_sql("SELECT id FROM records")
                .unwrap()
                .rows
                .len(),
            1
        );
        drop(database);
        let mut reopened = Database::open_with_durability(&project.0, durability).unwrap();
        select(&mut reopened, "child");
        assert_eq!(
            reopened
                .query_sql("SELECT id, body, tag FROM records ORDER BY id")
                .unwrap(),
            current
        );
    }
}
