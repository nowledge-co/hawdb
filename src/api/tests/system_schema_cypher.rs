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

//! Registry migrations that carry Cypher graph schema DDL next to SQL.

use super::*;
use crate::{SystemSchemaMigration, SystemSchemaRegistry};

const OWNER: &str = "nowledge.graph";

fn index_registry() -> SystemSchemaRegistry {
    SystemSchemaRegistry::new(
        OWNER,
        [
            SystemSchemaMigration::new(
                1,
                "create_graph_audit",
                ["CREATE TABLE graph_audit (id TEXT PRIMARY KEY)"],
            ),
            SystemSchemaMigration::new(2, "index_memory_ids", Vec::<String>::new())
                .with_cypher(["CREATE INDEX ON :Memory(id)"]),
        ],
    )
}

fn owner_versions(db: &mut Database, owner: &str) -> Vec<i64> {
    let mut versions = db
        .query_sql_with_params(
            "SELECT version FROM hawdb_schema_migrations WHERE owner = $1",
            &[Value::String(owner.to_string())],
        )
        .unwrap()
        .rows
        .into_iter()
        .map(|row| match row.get("version") {
            Some(Value::Int(version)) => *version,
            value => panic!("expected a migration version for {owner}, got {value:?}"),
        })
        .collect::<Vec<_>>();
    versions.sort_unstable();
    versions
}

fn lookup_plan(db: &mut Database) -> String {
    let rows = db
        .query("EXPLAIN MATCH (m:Memory {id: 'k1'}) RETURN m.id AS id")
        .unwrap();
    format!("{:?}", rows.rows)
}

fn has_memory_id_index(db: &mut Database) -> bool {
    !lookup_plan(db).contains("SeqNodeScan")
}

#[test]
fn cypher_index_migration_backfills_applies_once_and_is_used() {
    let path = unique_test_dir("system_schema_cypher_index");
    {
        let mut db = Database::open(&path).unwrap();
        for i in 0..32 {
            db.query(&format!("CREATE (:Memory {{id: 'k{i}', v: 0}})"))
                .unwrap();
        }
        assert!(!has_memory_id_index(&mut db));
        let report = db.apply_system_schema_registry(&index_registry()).unwrap();
        assert_eq!(report.applied_versions, vec![1, 2]);
        assert_eq!(owner_versions(&mut db, OWNER), vec![1, 2]);
        // Existing nodes were indexed in the migration's commit.
        assert!(has_memory_id_index(&mut db), "{}", lookup_plan(&mut db));
        let (_, seeks) = mutation_targets(&db);
        db.query("MATCH (m:Memory {id: 'k7'}) SET m.v = 1").unwrap();
        assert_eq!(mutation_targets(&db).1, seeks + 1);
    }
    {
        let mut db = Database::open(&path).unwrap();
        let epoch = db.commit_epoch().unwrap();
        let report = db.apply_system_schema_registry(&index_registry()).unwrap();
        assert!(report.applied_versions.is_empty());
        assert_eq!(report.previous_version, 2);
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        assert!(has_memory_id_index(&mut db), "{}", lookup_plan(&mut db));
        let rows = db
            .query("MATCH (m:Memory {id: 'k7'}) RETURN m.v AS v")
            .unwrap();
        assert_eq!(
            rows.rows.into_rows()[0]
                .values()
                .cloned()
                .collect::<Vec<_>>(),
            vec![Value::Int(1)]
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

/// Index seeks and label scans by mutation target lookups.
fn mutation_targets(db: &Database) -> (u64, u64) {
    let reads = db.storage_residency_report().unwrap().graph_index_reads;
    (
        reads.mutation_target_label_scans,
        reads.mutation_target_index_seeks,
    )
}

#[test]
fn a_failing_cypher_statement_publishes_no_owner() {
    let path = unique_test_dir("system_schema_cypher_atomic");
    let mut db = Database::open(&path).unwrap();
    // Duplicate keys make the unique constraint fail during apply.
    db.query("CREATE (:Thread {id: 'dup'})").unwrap();
    db.query("CREATE (:Thread {id: 'dup'})").unwrap();
    let epoch = db.commit_epoch().unwrap();
    let registries = [
        index_registry(),
        SystemSchemaRegistry::new(
            "nowledge.threads",
            [SystemSchemaMigration::new(
                1,
                "unique_thread_ids",
                ["CREATE TABLE thread_audit (id TEXT PRIMARY KEY)"],
            )
            .with_cypher(["CREATE CONSTRAINT ON :Thread(id) ASSERT UNIQUE"])],
        ),
    ];

    assert!(db.apply_system_schema_registries(&registries).is_err());

    assert_eq!(db.commit_epoch().unwrap(), epoch);
    for owner in [OWNER, "nowledge.threads"] {
        assert!(owner_versions(&mut db, owner).is_empty(), "{owner}");
    }
    assert!(db.query_sql("SELECT * FROM graph_audit").is_err());
    assert!(db.query_sql("SELECT * FROM thread_audit").is_err());
    assert!(!has_memory_id_index(&mut db));
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn registry_cypher_is_limited_to_graph_schema_ddl() {
    let path = unique_test_dir("system_schema_cypher_refused");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'k1', v: 0})").unwrap();
    let epoch = db.commit_epoch().unwrap();
    for statement in [
        "CREATE (:Memory {id: 'k2'})",
        "MERGE (:Memory {id: 'k3'})",
        "MATCH (m:Memory {id: 'k1'}) SET m.v = 1",
        "MATCH (m:Memory) DETACH DELETE m",
        "MATCH (m:Memory) RETURN m.id",
        "CHECKPOINT",
    ] {
        let registry = SystemSchemaRegistry::new(
            OWNER,
            [
                SystemSchemaMigration::new(1, "not_ddl", Vec::<String>::new())
                    .with_cypher([statement]),
            ],
        );
        let error = db.apply_system_schema_registry(&registry).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("must create graph indexes or constraints"),
            "{statement}: {error}"
        );
    }
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert!(owner_versions(&mut db, OWNER).is_empty());
    let rows = db
        .query("MATCH (m:Memory) RETURN m.id AS id, m.v AS v")
        .unwrap();
    assert_eq!(
        rows.rows
            .into_rows()
            .into_iter()
            .map(|row| row.values().cloned().collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec![Value::String("k1".into()), Value::Int(0)]]
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn changing_an_applied_cypher_statement_is_checksum_drift() {
    let path = unique_test_dir("system_schema_cypher_drift");
    {
        let mut db = Database::open(&path).unwrap();
        db.apply_system_schema_registry(&index_registry()).unwrap();
    }
    let drifted = SystemSchemaRegistry::new(
        OWNER,
        [
            index_registry().migrations()[0].clone(),
            SystemSchemaMigration::new(2, "index_memory_ids", Vec::<String>::new())
                .with_cypher(["CREATE INDEX ON :Memory(uuid)"]),
        ],
    );
    let mut db = Database::open(&path).unwrap();
    let error = db.apply_system_schema_registry(&drifted).unwrap_err();
    assert!(error.to_string().contains("checksum or identity drifted"));
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

/// Large Mem databases open out of core: the migration must apply there too
/// and find nodes left in the canonical base.
#[test]
fn cypher_index_migration_applies_to_an_out_of_core_database() {
    let path = unique_test_dir("system_schema_cypher_out_of_core");
    {
        let mut db = Database::open(&path).unwrap();
        for i in 0..32 {
            db.query(&format!("CREATE (:Memory {{id: 'k{i}', v: 0}})"))
                .unwrap();
        }
        db.checkpoint().unwrap();
    }
    let open = || {
        Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: crate::store::StorageResidencyMode::OutOfCore,
                ..DatabaseConfig::default()
            },
        )
        .unwrap()
    };
    {
        let mut db = open();
        assert!(db.storage_residency_report().unwrap().out_of_core);
        let report = db.apply_system_schema_registry(&index_registry()).unwrap();
        assert_eq!(report.applied_versions, vec![1, 2]);
        // Without reopening, the first writes already seek the new index:
        // the migration published its persistent projection.
        let (scans, seeks) = mutation_targets(&db);
        db.query("MATCH (m:Memory {id: 'k7'}) SET m.v = 1").unwrap();
        assert_eq!(mutation_targets(&db), (scans, seeks + 1), "SET");
        // MERGE resolves its node through the read plan, which must not fall
        // back to a mutation label scan either.
        db.query("MERGE (m:Memory {id: 'k8'}) SET m.v = 2").unwrap();
        assert_eq!(mutation_targets(&db).0, scans, "MERGE");
        let rows = db
            .query("MATCH (m:Memory {id: 'k8'}) RETURN count(m) AS n, max(m.v) AS v")
            .unwrap();
        assert_eq!(
            rows.rows.into_rows()[0]
                .values()
                .cloned()
                .collect::<Vec<_>>(),
            vec![Value::Int(1), Value::Int(2)]
        );
    }
    let mut db = open();
    assert!(db
        .apply_system_schema_registry(&index_registry())
        .unwrap()
        .applied_versions
        .is_empty());
    assert!(has_memory_id_index(&mut db), "{}", lookup_plan(&mut db));
    let rows = db
        .query("MATCH (m:Memory {id: 'k7'}) RETURN m.v AS v")
        .unwrap();
    assert_eq!(
        rows.rows.into_rows()[0]
            .values()
            .cloned()
            .collect::<Vec<_>>(),
        vec![Value::Int(1)]
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

fn memory_id_index_kinds(db: &Database) -> Vec<crate::schema::IndexKind> {
    let runtime = db.runtime.get().unwrap();
    let label = runtime.catalog.label_id("Memory").unwrap();
    [
        crate::schema::IndexKind::Equality,
        crate::schema::IndexKind::Range,
    ]
    .into_iter()
    .filter(|kind| {
        runtime
            .catalog
            .property_index_id_with_kind(label, "id", *kind)
            .is_some()
    })
    .collect()
}

/// After #2013 a primary key creates the equality index itself, and hosts or
/// tests may create it ahead of the migration: an identical index is a no-op
/// and the migration is still recorded. An index of another kind on the same
/// property is a different index, so the migration adds its own next to it.
#[test]
fn an_existing_index_is_reused_and_another_kind_is_kept() {
    for existing in [
        "CREATE INDEX ON :Memory(id)",
        "CREATE RANGE INDEX ON :Memory(id)",
    ] {
        let path = unique_test_dir("system_schema_cypher_existing");
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'k1', v: 0})").unwrap();
        db.query(existing).unwrap();
        let before = memory_id_index_kinds(&db);
        let report = db.apply_system_schema_registry(&index_registry()).unwrap();
        assert_eq!(report.applied_versions, vec![1, 2], "{existing}");
        assert_eq!(owner_versions(&mut db, OWNER), vec![1, 2], "{existing}");
        let mut expected = before;
        if !expected.contains(&crate::schema::IndexKind::Equality) {
            expected.insert(0, crate::schema::IndexKind::Equality);
        }
        assert_eq!(memory_id_index_kinds(&db), expected, "{existing}");
        let (_, seeks) = mutation_targets(&db);
        db.query("MATCH (m:Memory {id: 'k1'}) SET m.v = 1").unwrap();
        assert_eq!(mutation_targets(&db).1, seeks + 1, "{existing}");
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}
