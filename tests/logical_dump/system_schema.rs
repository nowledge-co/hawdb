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

//! Logical dumps of databases with host system schema registries (#1975):
//! the migration records and owned tables are restored, and a dump cannot
//! use the registry table to pass anything else.

use std::fs;
use std::path::Path;

use hawdb::{
    verify_logical_dump, Database, HawDBEmbedded, HawDBError, SystemSchemaMigration,
    SystemSchemaRegistry, Value,
};
use serde_json::{json, Value as Json};

use super::logical_dump_support::{golden_host_ledger_files, TempDir};
use super::{assert_not_published, dump_content, manifest, rewrite_schema, rows, small_pages, sql};

const LEDGER: &str = "hawdb_schema_migrations";
const LEDGER_ROWS: &str = "SELECT migration_id, owner, version, name, checksum \
                           FROM hawdb_schema_migrations ORDER BY migration_id";

/// The registries the v0.37.0 golden dump was written with, cut to the first
/// `content` migrations; 3 is pending there.
fn registries(content: usize) -> Vec<SystemSchemaRegistry> {
    let migrations = [
        SystemSchemaMigration::new(
            1,
            "create_documents",
            [
                "CREATE TABLE documents (id TEXT PRIMARY KEY, body TEXT)",
                "CREATE TABLE revisions (document TEXT NOT NULL REFERENCES documents(id) \
                 ON DELETE CASCADE, seq BIGINT NOT NULL, PRIMARY KEY (document, seq))",
            ],
        ),
        SystemSchemaMigration::new(
            2,
            "rank_documents",
            [
                "ALTER TABLE documents ADD COLUMN rank BIGINT",
                "CREATE INDEX documents_rank ON documents (rank) INCLUDE (body)",
            ],
        ),
        SystemSchemaMigration::new(
            3,
            "tag_documents",
            ["ALTER TABLE documents ADD COLUMN tag TEXT DEFAULT 'none'"],
        ),
    ];
    vec![
        SystemSchemaRegistry::new(
            "example.content",
            migrations.into_iter().take(content).collect::<Vec<_>>(),
        ),
        SystemSchemaRegistry::new(
            "example.settings",
            [SystemSchemaMigration::new(
                1,
                "create_settings",
                ["CREATE TABLE settings (k TEXT PRIMARY KEY, v TEXT)"],
            )],
        ),
    ]
}

fn golden(temp: &TempDir) -> std::path::PathBuf {
    let dump = temp.path().join("golden");
    fs::create_dir_all(dump.join("data")).unwrap();
    for (path, bytes) in golden_host_ledger_files() {
        fs::write(dump.join(path), bytes).unwrap();
    }
    dump
}

fn open(path: &Path) -> HawDBEmbedded {
    HawDBEmbedded::open(path).unwrap()
}

fn apply(database: &mut HawDBEmbedded, registries: &[SystemSchemaRegistry]) -> Vec<Vec<u64>> {
    database
        .database_mut()
        .apply_system_schema_registries(registries)
        .unwrap()
        .into_iter()
        .map(|report| report.applied_versions)
        .collect()
}

fn int(value: i64) -> Value {
    Value::Int(value)
}

fn text(value: &str) -> Value {
    Value::String(value.to_string())
}

/// The content, keys, foreign keys and index of the host-owned tables.
fn assert_golden_tables(database: &mut HawDBEmbedded) {
    assert_eq!(
        rows(database, "SELECT id, body, rank FROM documents ORDER BY id"),
        [
            vec![text("d1"), text("第一"), int(2)],
            vec![text("d2"), text("second"), int(1)],
            vec![text("d3"), Value::Null, int(1)],
        ]
    );
    assert_eq!(
        rows(
            database,
            "SELECT document, seq FROM revisions ORDER BY document, seq"
        ),
        [
            vec![text("d1"), int(1)],
            vec![text("d1"), int(2)],
            vec![text("d2"), int(1)],
        ]
    );
    assert_eq!(
        rows(database, "SELECT k, v FROM settings"),
        [vec![text("theme"), text("dark")]]
    );
    let plan = format!(
        "{:?}",
        rows(
            database,
            "EXPLAIN SELECT id, body FROM documents WHERE rank = 1"
        )
    );
    assert!(plan.contains("documents_rank"), "{plan}");
    assert_eq!(
        rows(
            database,
            "SELECT id FROM documents WHERE rank = 1 ORDER BY id"
        ),
        [vec![text("d2")], vec![text("d3")]]
    );
    for refused in [
        "INSERT INTO documents (id) VALUES ('d1')",
        "INSERT INTO revisions (document, seq) VALUES ('absent', 1)",
        // The restored registry table stays closed to SQL.
        "INSERT INTO hawdb_schema_migrations (migration_id, owner, version, name, checksum) \
         VALUES ('x:1', 'x', 1, 'x', 'x')",
        "DELETE FROM hawdb_schema_migrations",
        "UPDATE hawdb_schema_migrations SET checksum = 'x'",
    ] {
        assert!(
            database.database_mut().query_sql(refused).is_err(),
            "{refused}"
        );
    }
}

fn file_entry<'a>(manifest: &'a mut Json, table: &str) -> &'a mut Json {
    manifest["files"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|file| file["table"] == table)
        .unwrap()
}

/// Rewrites one table's only data file and every manifest field that covers
/// it: a dump that is internally consistent, as a forger would leave it.
fn rewrite_rows(dump: &Path, table: &str, edit: impl FnOnce(&mut Vec<Json>)) {
    let mut document = manifest(dump);
    let path = dump.join(file_entry(&mut document, table)["path"].as_str().unwrap());
    let mut records = fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect::<Vec<Json>>();
    let before = records.len() as i64;
    edit(&mut records);
    let bytes = records
        .iter()
        .map(|record| format!("{record}\n"))
        .collect::<String>();
    fs::write(&path, &bytes).unwrap();
    let mut hasher = hawdb_integrity::IntegrityHasher::new();
    hasher.update(bytes.as_bytes());
    let entry = file_entry(&mut document, table);
    entry["records"] = json!(records.len());
    entry["bytes"] = json!(bytes.len());
    entry["sha256"] = json!(hasher.finish().sha256.to_string());
    let total = document["counts"]["table_rows"].as_i64().unwrap();
    document["counts"]["table_rows"] = json!(total + records.len() as i64 - before);
    fs::write(
        dump.join("manifest.json"),
        serde_json::to_vec(&document).unwrap(),
    )
    .unwrap();
}

/// Golden ledger rows, in file order: content 1 and 2, settings 1, engine 1.
fn rewrite_ledger(dump: &Path, edit: impl FnOnce(&mut Vec<Json>)) {
    rewrite_rows(dump, LEDGER, edit);
}

/// A dump v0.37.0 (persistent format 5) wrote. These bytes must never change.
#[test]
fn dump_of_an_older_persistent_format_restores_installed_migrations() {
    let temp = TempDir::new("ledger-golden");
    let dump = golden(&temp);
    let report = verify_logical_dump(&dump).unwrap();
    assert_eq!(report.source_hawdb_version, "0.37.0");
    assert_eq!(report.source_persistent_format_version, 5);
    const { assert!(hawdb::PERSISTENT_FORMAT_VERSION > 5) };

    let destination = temp.path().join("imported");
    let report = Database::import_logical_dump(&dump, &destination).unwrap();
    assert_eq!((report.table_count, report.table_row_count), (4, 11));
    let mut database = open(&destination);
    let ledger = rows(&mut database, LEDGER_ROWS);
    let installed = registries(2);
    let record = |registry: &SystemSchemaRegistry, index: usize| {
        let migration = &registry.migrations()[index];
        vec![
            text(&format!("{}:{}", registry.owner(), migration.version())),
            text(registry.owner()),
            int(migration.version() as i64),
            text(migration.name()),
            text(&migration.checksum(registry.owner())),
        ]
    };
    assert_eq!(
        ledger[..3],
        [
            record(&installed[0], 0),
            record(&installed[0], 1),
            record(&installed[1], 0)
        ]
    );
    assert_eq!(ledger.len(), 4);
    assert_eq!(ledger[3][1], text("hawdb.engine"));
    assert_golden_tables(&mut database);

    // Installed migrations are not run again; the pending one is.
    assert_eq!(apply(&mut database, &installed), vec![Vec::<u64>::new(); 2]);
    assert_eq!(apply(&mut database, &registries(3)), [vec![3], vec![]]);
    drop(database);
    let mut database = open(&destination);
    assert_eq!(
        apply(&mut database, &registries(3)),
        vec![Vec::<u64>::new(); 2]
    );
    assert_eq!(
        rows(&mut database, "SELECT DISTINCT tag FROM documents"),
        [vec![text("none")]]
    );
    assert_golden_tables(&mut database);
}

#[test]
fn same_format_round_trip_restores_installed_and_pending_migrations() {
    let temp = TempDir::new("ledger-round-trip");
    let mut source = open(&temp.path().join("source"));
    // More than nine versions, so record keys do not sort in version order.
    let host = SystemSchemaRegistry::new(
        "example.host",
        (1..=11).map(|version| {
            SystemSchemaMigration::new(
                version,
                format!("step_{version}"),
                [if version == 1 {
                    "CREATE TABLE host (id BIGINT PRIMARY KEY, a TEXT UNIQUE)".to_string()
                } else {
                    format!("ALTER TABLE host ADD COLUMN c{version} BIGINT")
                }],
            )
        }),
    );
    let installed =
        SystemSchemaRegistry::new("example.host", host.migrations()[..10].iter().cloned());
    assert_eq!(
        apply(&mut source, std::slice::from_ref(&installed)),
        [(1..=10).collect::<Vec<_>>()]
    );
    for id in 0..20 {
        sql(
            &mut source,
            &format!("INSERT INTO host (id, a, c10) VALUES ({id}, 'a{id}', {id})"),
        );
    }
    let dump = temp.path().join("dump");
    source
        .export_logical_dump_with_options(&dump, &small_pages())
        .unwrap();
    let ledger = rows(&mut source, LEDGER_ROWS);
    assert_eq!(ledger.len(), 11);

    for (name, supplied) in [
        ("unverified", None),
        ("installed", Some(&installed)),
        ("pending", Some(&host)),
    ] {
        let destination = temp.path().join(name);
        match supplied {
            None => Database::import_logical_dump_with_options(&dump, &destination, &small_pages()),
            Some(registry) => Database::import_logical_dump_with_system_schema(
                &dump,
                &destination,
                &small_pages(),
                std::slice::from_ref(registry),
            ),
        }
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        let mut database = open(&destination);
        // Import runs no migration, not even a pending one it was given.
        assert_eq!(rows(&mut database, LEDGER_ROWS), ledger, "{name}");
        let again = temp.path().join(format!("{name}-again"));
        database
            .export_logical_dump_with_options(&again, &small_pages())
            .unwrap();
        assert_eq!(dump_content(&again), dump_content(&dump), "{name}");
        assert_eq!(
            apply(&mut database, std::slice::from_ref(&host)),
            [vec![11]],
            "{name}"
        );
        assert!(database
            .database_mut()
            .query_sql("INSERT INTO host (id, a) VALUES (99, 'a1')")
            .is_err());
    }
}

/// A record in the right shape is only a claim: the registry decides.
#[test]
fn host_registries_decide_whether_restored_records_are_trusted() {
    let temp = TempDir::new("ledger-registries");
    let dump = golden(&temp);
    let forged = temp.path().join("forged");
    super::copy_dump(&dump, &forged);
    rewrite_ledger(&forged, |records| records[1][4] = json!("0".repeat(64)));
    let invented = temp.path().join("invented");
    super::copy_dump(&dump, &invented);
    rewrite_ledger(&invented, |records| {
        records.push(json!(["evil:1", "evil", 1, "grant", "a".repeat(64)]));
    });
    let destination = temp.path().join("imported");
    let drifted = vec![
        registries(2).remove(0),
        SystemSchemaRegistry::new(
            "example.settings",
            [SystemSchemaMigration::new(
                1,
                "create_settings",
                ["CREATE TABLE settings (k TEXT PRIMARY KEY)"],
            )],
        ),
    ];
    let engine = SystemSchemaRegistry::new(
        "hawdb.engine",
        [SystemSchemaMigration::new(
            1,
            "create_schema_registry",
            ["CREATE TABLE evil (id BIGINT PRIMARY KEY)"],
        )],
    );
    for (dump, supplied, expected) in [
        (&dump, drifted.clone(), "drifted"),
        (&dump, registries(1), "future version 2"),
        (&dump, vec![registries(2).remove(0)], "no registry"),
        (&dump, vec![], "no registry"),
        (&forged, registries(2), "drifted"),
        (&invented, registries(2), "no registry"),
        (&dump, vec![engine], "reserved"),
        (
            &dump,
            [registries(2), registries(2)].concat(),
            "more than once",
        ),
    ] {
        let error = Database::import_logical_dump_with_system_schema(
            dump,
            &destination,
            &small_pages(),
            &supplied,
        )
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{expected}: {error}");
        assert_not_published(&destination);
    }
    // Without a registry the forgery is imported, as it would be found in a
    // database file, and the host's registry refuses it on open.
    Database::import_logical_dump(&forged, &destination).unwrap();
    let mut database = open(&destination);
    let error = database
        .database_mut()
        .apply_system_schema_registries(&registries(3))
        .unwrap_err();
    assert!(error.to_string().contains("drifted"), "{error}");
    assert!(
        database
            .database_mut()
            .query_sql("SELECT tag FROM documents")
            .is_err(),
        "a pending migration ran behind a forged record"
    );
}

#[test]
fn forged_or_corrupt_ledgers_are_refused_before_anything_is_written() {
    let temp = TempDir::new("ledger-forged");
    let dump = golden(&temp);
    type Edit = fn(&mut Vec<Json>);
    let edits: [(&str, Edit); 19] = [
        ("engine record missing", |r| {
            r.pop();
        }),
        ("engine checksum", |r| r[3][4] = json!("0".repeat(64))),
        ("engine name", |r| r[3][3] = json!("other")),
        ("newer engine", |r| {
            r.push(json!([
                "hawdb.engine:2",
                "hawdb.engine",
                2,
                "next",
                "0".repeat(64)
            ]))
        }),
        ("gap", |r| {
            r.remove(0);
        }),
        ("duplicate record", |r| {
            let record = r[1].clone();
            r.push(record);
        }),
        ("duplicate version under another key", |r| {
            let mut record = r[1].clone();
            record[0] = json!("example.content:02");
            r.push(record);
        }),
        ("key does not name the record", |r| {
            r[0][0] = json!("hawdb.engine:1")
        }),
        ("key with a leading zero", |r| {
            r[0][0] = json!("example.content:01")
        }),
        ("version 0", |r| {
            r[2][0] = json!("example.settings:0");
            r[2][2] = json!(0);
        }),
        ("negative version", |r| {
            r[2][0] = json!("example.settings:-1");
            r[2][2] = json!(-1);
        }),
        ("invalid owner", |r| {
            r[2][0] = json!("two words:1");
            r[2][1] = json!("two words");
        }),
        ("empty owner", |r| {
            r[2][0] = json!(":1");
            r[2][1] = json!("");
        }),
        ("empty name", |r| r[2][3] = json!("")),
        ("long name", |r| r[2][3] = json!("n".repeat(129))),
        ("short checksum", |r| r[2][4] = json!("00")),
        ("uppercase checksum", |r| r[2][4] = json!("A".repeat(64))),
        ("null checksum", |r| r[2][4] = Json::Null),
        ("extra value", |r| {
            r[2].as_array_mut().unwrap().push(json!(1))
        }),
    ];
    type SchemaEdit = fn(&mut Json);
    let schema_edits: [(&str, SchemaEdit); 6] = [
        ("extra column", |table| {
            table["columns"]
                .as_array_mut()
                .unwrap()
                .push(json!({"name": "x", "type": "text", "nullable": true}))
        }),
        ("swapped columns", |table| {
            table["columns"].as_array_mut().unwrap().swap(0, 1)
        }),
        ("no unique constraint", |table| table["unique"] = json!([])),
        (
            "extra index",
            |table| {
                table["indexes"] = json!([{"name": "i", "columns": ["owner"], "unique": false}])
            },
        ),
        ("foreign key", |table| {
            table["foreign_keys"] = json!([{
                "columns": ["name"], "references_table": LEDGER,
                "references_columns": ["migration_id"],
                "on_delete": "cascade", "on_update": "cascade"
            }])
        }),
        (
            "search index",
            |table| {
                table["search_indexes"] =
                    json!([{"name": "s", "column": "name", "analyzer": "cjk_ngram"}])
            },
        ),
    ];
    let destination = temp.path().join("imported");
    let check = |name: &str, copy: &Path| {
        for error in [
            verify_logical_dump(copy).unwrap_err(),
            Database::import_logical_dump(copy, &destination).unwrap_err(),
        ] {
            assert!(
                matches!(
                    error,
                    HawDBError::StorageIntegrity(_) | HawDBError::Storage(_)
                ),
                "{name}: {error:?}"
            );
        }
        assert_not_published(&destination);
    };
    for (index, (name, edit)) in edits.into_iter().enumerate() {
        let copy = temp.path().join(format!("rows-{index}"));
        super::copy_dump(&dump, &copy);
        rewrite_ledger(&copy, edit);
        check(name, &copy);
    }
    for (index, (name, edit)) in schema_edits.into_iter().enumerate() {
        let copy = temp.path().join(format!("schema-{index}"));
        super::copy_dump(&dump, &copy);
        rewrite_schema(&copy, |schema| {
            edit(
                schema["tables"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|table| table["name"] == LEDGER)
                    .unwrap(),
            )
        });
        check(name, &copy);
    }

    // Host-owned data that fails after the records were restored into the
    // unpublished target leaves nothing either.
    let copy = temp.path().join("late");
    super::copy_dump(&dump, &copy);
    rewrite_rows(&copy, "settings", |records| {
        let record = records[0].clone();
        records.push(record);
    });
    Database::import_logical_dump(&copy, &destination).unwrap_err();
    assert_not_published(&destination);

    // The destination is still usable: the intact dump imports there.
    Database::import_logical_dump_with_system_schema(
        &dump,
        &destination,
        &small_pages(),
        &registries(3),
    )
    .unwrap();
    let mut database = open(&destination);
    assert_eq!(apply(&mut database, &registries(3)), [vec![3], vec![]]);
    assert_golden_tables(&mut database);
}
