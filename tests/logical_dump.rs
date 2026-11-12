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

//! Logical export/import (docs/LOGICAL_DUMP.md) through the public embedded
//! API: round-trip equality on realistic data, snapshot consistency, refusal
//! of unrepresentable state, failure atomicity of import, the golden format 1
//! dump every later release must keep reading, and the typed refusal of an
//! incompatible storage format.

#[path = "logical_dump/support.rs"]
mod logical_dump_support;

use std::collections::BTreeMap;
use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use hawdb::{
    confirm_logical_dump_publication, verify_logical_dump, Database, HawDBEmbedded, HawDBError,
    LogicalDumpOptions, StorageFormatError, Value, LOGICAL_DUMP_FORMAT_VERSION,
    PERSISTENT_FORMAT_VERSION,
};
use logical_dump_support::{golden_v1_files, TempDir};

fn small_pages() -> LogicalDumpOptions {
    LogicalDumpOptions {
        page_rows: NonZeroUsize::new(7).unwrap(),
        file_records: NonZeroUsize::new(10).unwrap(),
        import_checkpoint_records: NonZeroUsize::new(16).unwrap(),
    }
}

fn sql(database: &mut HawDBEmbedded, text: &str) {
    database
        .database_mut()
        .query_sql(text)
        .unwrap_or_else(|error| panic!("{text}: {error}"));
}

fn sql_params(database: &mut HawDBEmbedded, text: &str, parameters: Vec<Value>) {
    database
        .database_mut()
        .query_sql_with_params(text, &parameters)
        .unwrap_or_else(|error| panic!("{text}: {error}"));
}

fn cypher(database: &mut HawDBEmbedded, text: &str, parameters: BTreeMap<String, Value>) {
    database
        .query_with_params_admitted(text, &parameters)
        .unwrap_or_else(|error| panic!("{text}: {error}"));
}

fn vector(coordinates: &[f32]) -> Value {
    Value::Vector(hawdb::Vector::new(coordinates).unwrap())
}

fn decimal(text: &str) -> Value {
    Value::Decimal(text.parse().unwrap())
}

/// A realistic embedded database: an event log with a thread table, a
/// self-referencing tree, a keyless table with duplicate rows, awkward
/// identifiers, every relational scalar type, and a typed property graph
/// with indexes and constraints.
fn populate(database: &mut HawDBEmbedded) {
    sql(
        database,
        "CREATE TABLE threads (id BIGINT PRIMARY KEY, title TEXT NOT NULL, \
         created TIMESTAMPTZ(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), \
         token UUID DEFAULT uuidv7(), state TEXT DEFAULT 'it''s new', \
         budget NUMERIC(12,2) DEFAULT 1.25, ratio DOUBLE PRECISION DEFAULT 0.5, \
         due TIMESTAMP(3) DEFAULT '2024-01-02T03:04:05.123', UNIQUE (title))",
    );
    sql(
        database,
        "CREATE TABLE events (thread BIGINT NOT NULL REFERENCES threads(id) ON DELETE CASCADE, \
         seq BIGINT NOT NULL, body TEXT, payload BYTEA, amount NUMERIC(38,10), \
         score DOUBLE PRECISION, embedding VECTOR(3), at TIMESTAMP(6), flag BOOLEAN, \
         PRIMARY KEY (thread, seq))",
    );
    sql(database, "CREATE INDEX events_at ON events (at)");
    sql(
        database,
        "CREATE TABLE tree (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES tree(id), label TEXT)",
    );
    sql(database, "CREATE TABLE notes (k TEXT, v BIGINT)");
    sql(
        database,
        "CREATE TABLE \"Odd \"\"Name\"\"\" (\"Key Col\" TEXT PRIMARY KEY, \"值\" TEXT)",
    );
    sql(
        database,
        "CREATE UNIQUE INDEX \"odd unique\" ON \"Odd \"\"Name\"\"\" (\"值\")",
    );

    let titles = [
        "日志 🚀",
        "Ünïcødé",
        "plain",
        "emoji 👩‍💻 and \"quotes\"",
        "tab\tand\nnewline",
    ];
    for (id, title) in titles.iter().enumerate() {
        sql_params(
            database,
            "INSERT INTO threads (id, title, created) VALUES ($1, $2, $3)",
            vec![
                Value::Int(id as i64 + 1),
                Value::String((*title).to_string()),
                Value::String(format!("2024-0{}-02 03:04:05.12{id}+08", id + 1)),
            ],
        );
    }
    let specials = [
        Value::Float(f64::NAN),
        Value::Float(f64::INFINITY),
        Value::Float(f64::NEG_INFINITY),
        Value::Float(-0.0),
        Value::Float(f64::MIN_POSITIVE),
        Value::Null,
    ];
    for thread in 1..=3i64 {
        for seq in [i64::MIN, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, i64::MAX] {
            let index = (seq.rem_euclid(97) + thread) as usize;
            sql_params(
                database,
                "INSERT INTO events (thread, seq, body, payload, amount, score, embedding, at, flag) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                vec![
                    Value::Int(thread),
                    Value::Int(seq),
                    if index.is_multiple_of(5) {
                        Value::Null
                    } else {
                        Value::String(format!("事件 {thread}/{seq} ✓ {}", "x".repeat(index % 13)))
                    },
                    Value::Binary((0..(index % 9) as u8).chain([0, 255, b'\n']).collect()),
                    decimal(&format!("{}.0123456789", (index as i64 - 40) * 1_000_000_007)),
                    specials[index % specials.len()].clone(),
                    vector(&[index as f32 / 3.0, -1.5, f32::MAX]),
                    Value::String(format!("2025-12-31 23:59:{:02}.{:06}", index % 60, index * 7)),
                    if index.is_multiple_of(3) {
                        Value::Null
                    } else {
                        Value::Bool(index.is_multiple_of(2))
                    },
                ],
            );
        }
    }
    // Parents are inserted first, but children precede their parents in
    // primary-key order (1 -> 9 -> 5), which is the order export writes them.
    // 11 references itself.
    for (id, parent) in [
        (5, None),
        (9, Some(5)),
        (1, Some(9)),
        (2, Some(1)),
        (11, Some(11)),
        (12, None),
    ] {
        sql_params(
            database,
            "INSERT INTO tree (id, parent, label) VALUES ($1, $2, $3)",
            vec![
                Value::Int(id),
                parent.map_or(Value::Null, Value::Int),
                Value::String(format!("节点 {id}")),
            ],
        );
    }
    sql(
        database,
        "INSERT INTO tree (id, parent, label) VALUES (20, NULL, 'a'), (21, 20, 'b')",
    );
    sql(
        database,
        "INSERT INTO notes (k, v) VALUES ('dup', 1), ('dup', 1), (NULL, NULL), ('ключ', 9223372036854775807)",
    );
    // Enough keyless rows, duplicates included, for several offset pages.
    for index in 0..17i64 {
        sql_params(
            database,
            "INSERT INTO notes (k, v) VALUES ($1, $2)",
            vec![
                Value::String(format!("n{}", index % 5)),
                Value::Int(index % 3),
            ],
        );
    }
    sql(
        database,
        "INSERT INTO \"Odd \"\"Name\"\"\" (\"Key Col\", \"值\") VALUES ('a b', '一'), ('\"q\"', NULL)",
    );

    cypher(database, "CREATE NODE TABLE Memory", BTreeMap::new());
    cypher(
        database,
        "CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL",
        BTreeMap::new(),
    );
    cypher(
        database,
        "CREATE RELATIONSHIP TABLE MENTIONS",
        BTreeMap::new(),
    );
    for id in 0..23i64 {
        let mut parameters = BTreeMap::new();
        parameters.insert("id".to_string(), Value::Int(id));
        parameters.insert(
            "title".to_string(),
            Value::String(format!("记忆 {id} – ünïcødé")),
        );
        parameters.insert("blob".to_string(), Value::Binary(vec![id as u8, 0, 255]));
        parameters.insert("vec".to_string(), vector(&[id as f32, 0.25]));
        parameters.insert(
            "tags".to_string(),
            Value::List(vec![Value::String("标签".to_string()), Value::Int(id)]),
        );
        parameters.insert(
            "meta".to_string(),
            Value::Map(BTreeMap::from([
                (
                    "$binary".to_string(),
                    Value::String("not binary".to_string()),
                ),
                ("n".to_string(), Value::Int(i64::MIN + id)),
            ])),
        );
        parameters.insert("rank".to_string(), Value::Float(id as f64 / 7.0));
        parameters.insert("dec".to_string(), decimal("-98765432109876543210.0001"));
        cypher(
            database,
            "CREATE (n:Memory {id: $id, title: $title, blob: $blob, vec: $vec, tags: $tags, \
             meta: $meta, rank: $rank, dec: $dec})",
            parameters,
        );
    }
    cypher(
        database,
        "CREATE (p:Person {name: '李雷', id: 1})",
        BTreeMap::new(),
    );
    for source in 0..22i64 {
        let mut parameters = BTreeMap::new();
        parameters.insert("a".to_string(), Value::Int(source));
        parameters.insert("b".to_string(), Value::Int((source * 5 + 1) % 23));
        parameters.insert("w".to_string(), Value::Float(source as f64 * 0.5));
        cypher(
            database,
            "MATCH (a:Memory {id: $a}), (b:Memory {id: $b}) CREATE (a)-[:MENTIONS {weight: $w, note: '提及'}]->(b)",
            parameters,
        );
    }
    for statement in [
        "CREATE INDEX ON :Memory(id)",
        "CREATE FULLTEXT INDEX ON :Memory(title)",
        "CREATE RANGE INDEX ON :Memory(rank)",
        "CREATE INDEX ON :Memory(title, rank)",
        "CREATE CONSTRAINT ON :Memory(id) ASSERT UNIQUE",
        "CREATE CONSTRAINT ON :Memory(title) ASSERT EXISTS",
        "CREATE CONSTRAINT ON -[:MENTIONS(weight)]-> ASSERT EXISTS",
    ] {
        cypher(database, statement, BTreeMap::new());
    }
    // Delete a node and a row so ids and keys have gaps.
    cypher(
        database,
        "MATCH (n:Person {name: '李雷'}) DELETE n",
        BTreeMap::new(),
    );
    cypher(
        database,
        "CREATE (p:Person {name: 'Ada', id: 2})",
        BTreeMap::new(),
    );
    sql(database, "DELETE FROM events WHERE thread = 2 AND seq = 5");
}

/// Data files and schema of a dump, keyed by manifest path. Two dumps of the
/// same logical content are byte-identical here; only `manifest.json`'s
/// provenance (version, epoch) may differ.
fn dump_content(dump: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut content = BTreeMap::new();
    content.insert(
        "schema.json".to_string(),
        fs::read(dump.join("schema.json")).unwrap(),
    );
    for entry in fs::read_dir(dump.join("data")).unwrap() {
        let entry = entry.unwrap();
        content.insert(
            format!("data/{}", entry.file_name().to_string_lossy()),
            fs::read(entry.path()).unwrap(),
        );
    }
    content
}

fn manifest(dump: &Path) -> serde_json::Value {
    serde_json::from_slice(&fs::read(dump.join("manifest.json")).unwrap()).unwrap()
}

fn assert_not_published(destination: &Path) {
    assert!(
        !destination.exists(),
        "{} was published",
        destination.display()
    );
    let parent = destination.parent().unwrap();
    let name = destination
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let leftovers = fs::read_dir(parent)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .filter(|entry| entry.starts_with(&format!(".{name}.hawdb-")))
        .collect::<Vec<_>>();
    assert!(leftovers.is_empty(), "staging left behind: {leftovers:?}");
}

fn rows(database: &mut HawDBEmbedded, text: &str) -> Vec<Vec<Value>> {
    database
        .database_mut()
        .query_sql(text)
        .unwrap()
        .value_rows()
        .map(|row| row.to_vec())
        .collect()
}

/// Value equality where NaN equals NaN and -0.0 differs from 0.0.
fn same(left: &[Vec<Value>], right: &[Vec<Value>]) -> bool {
    fn value(left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Float(left), Value::Float(right)) => left.to_bits() == right.to_bits(),
            (Value::List(left), Value::List(right)) => {
                left.len() == right.len() && left.iter().zip(right).all(|(l, r)| value(l, r))
            }
            _ => left == right,
        }
    }
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(l, r)| l.len() == r.len() && l.iter().zip(r).all(|(a, b)| value(a, b)))
}

const COMPARED_QUERIES: &[&str] = &[
    "SELECT * FROM threads ORDER BY id",
    "SELECT * FROM events ORDER BY thread, seq",
    "SELECT * FROM tree ORDER BY id",
    "SELECT k, v FROM notes ORDER BY k, v",
    "SELECT * FROM \"Odd \"\"Name\"\"\" ORDER BY \"Key Col\"",
    "SELECT owner, version, name, checksum FROM hawdb_schema_migrations ORDER BY migration_id",
];

#[test]
fn round_trip_preserves_relational_and_graph_data_exactly() {
    let temp = TempDir::new("round-trip");
    let source_path = temp.path().join("source");
    let mut source = HawDBEmbedded::open(&source_path).unwrap();
    populate(&mut source);

    let first = temp.path().join("dump-1");
    let report = source
        .export_logical_dump_with_options(&first, &small_pages())
        .unwrap();
    assert_eq!(report.dump_format_version, LOGICAL_DUMP_FORMAT_VERSION);
    assert_eq!(report.persistent_format_version, PERSISTENT_FORMAT_VERSION);
    assert_eq!(report.branch.as_deref(), Some("main"));
    assert_eq!(report.table_count, 6);
    assert_eq!(report.node_count, 24);
    assert_eq!(report.relationship_count, 22);
    // 5 threads + 44 events + 8 tree + 21 notes + 2 odd + 1 registry row.
    assert_eq!(report.table_row_count, 81);
    let files = manifest(&first)["files"].as_array().unwrap().clone();
    assert!(
        files
            .iter()
            .filter(|file| file["table"] == "events")
            .count()
            >= 5,
        "small pages must rotate the events table across files: {files:?}"
    );

    let imported_path = temp.path().join("imported");
    let imported_report =
        Database::import_logical_dump_with_options(&first, &imported_path, &small_pages()).unwrap();
    assert_eq!(imported_report.table_row_count, report.table_row_count);
    assert_eq!(imported_report.node_count, report.node_count);
    assert_eq!(
        imported_report.relationship_count,
        report.relationship_count
    );
    assert_eq!(imported_report.manifest_sha256, report.manifest_sha256);
    assert_eq!(imported_report.source_hawdb_version, report.hawdb_version);

    let mut imported = HawDBEmbedded::open(&imported_path).unwrap();
    for query in COMPARED_QUERIES {
        let before = rows(&mut source, query);
        let after = rows(&mut imported, query);
        assert!(
            same(&before, &after),
            "{query}\nbefore: {before:?}\nafter:  {after:?}"
        );
    }
    assert_eq!(
        rows(&mut imported, "SELECT id, parent FROM tree WHERE id = 1"),
        vec![vec![Value::Int(1), Value::Int(9)]],
    );

    // Constraints and defaults are live in the imported database.
    assert!(imported
        .database_mut()
        .query_sql("INSERT INTO threads (id, title) VALUES (99, 'plain')")
        .is_err());
    sql(
        &mut imported,
        "INSERT INTO threads (id, title) VALUES (100, 'new')",
    );
    let defaults = rows(
        &mut imported,
        "SELECT state, budget, ratio, due FROM threads WHERE id = 100",
    );
    assert_eq!(
        defaults,
        vec![vec![
            Value::String("it's new".to_string()),
            decimal("1.25"),
            Value::Float(0.5),
            rows(&mut source, "SELECT due FROM threads WHERE id = 1")[0][0].clone(),
        ]]
    );
    assert!(imported
        .query_admitted("CREATE (n:Memory {id: 1, title: 'duplicate id'})")
        .is_err());
    let graph = imported
        .query_admitted(
            "MATCH (a:Memory {id: 3})-[r:MENTIONS]->(b:Memory) RETURN b.id, r.weight, r.note",
        )
        .unwrap();
    assert_eq!(
        graph
            .value_rows()
            .map(|row| row.to_vec())
            .collect::<Vec<_>>(),
        vec![vec![
            Value::Int(16),
            Value::Float(1.5),
            Value::String("提及".to_string())
        ]]
    );
    drop(imported);

    // dump(import(dump(db))) == dump(db): schema, every row, node and
    // relationship with its id, labels, types and properties.
    let mut imported = HawDBEmbedded::open(&imported_path).unwrap();
    // Remove the row written above so both snapshots hold the same data.
    sql(&mut imported, "DELETE FROM threads WHERE id = 100");
    let second = temp.path().join("dump-2");
    imported
        .export_logical_dump_with_options(&second, &small_pages())
        .unwrap();
    let first_content = dump_content(&first);
    let second_content = dump_content(&second);
    assert_eq!(
        first_content.keys().collect::<Vec<_>>(),
        second_content.keys().collect::<Vec<_>>()
    );
    for (path, bytes) in &first_content {
        assert!(
            bytes == &second_content[path],
            "{path} differs after a round trip:\n{}\n---\n{}",
            String::from_utf8_lossy(bytes),
            String::from_utf8_lossy(&second_content[path])
        );
    }
    assert_eq!(manifest(&first)["files"], manifest(&second)["files"]);
    assert_eq!(manifest(&first)["counts"], manifest(&second)["counts"]);
}

#[test]
fn export_is_one_consistent_snapshot_while_writers_commit() {
    let temp = TempDir::new("snapshot");
    let mut database = HawDBEmbedded::open(temp.path().join("db")).unwrap();
    sql(
        &mut database,
        "CREATE TABLE log (id BIGINT PRIMARY KEY, body TEXT)",
    );
    for id in 0..40 {
        sql_params(
            &mut database,
            "INSERT INTO log (id, body) VALUES ($1, $2)",
            vec![Value::Int(id), Value::String(format!("before {id}"))],
        );
    }
    cypher(&mut database, "CREATE (n:Item {id: 1})", BTreeMap::new());
    let snapshot = database.begin_read_transaction().unwrap();
    let epoch = snapshot.commit_epoch();

    // Writers commit after the snapshot was pinned and before and during the
    // export's pages: inserts, an update, a delete and graph writes.
    let writer = std::thread::spawn(move || {
        for id in 40..140 {
            sql_params(
                &mut database,
                "INSERT INTO log (id, body) VALUES ($1, $2)",
                vec![Value::Int(id), Value::String(format!("after {id}"))],
            );
        }
        sql(
            &mut database,
            "UPDATE log SET body = 'changed' WHERE id = 3",
        );
        sql(&mut database, "DELETE FROM log WHERE id = 4");
        cypher(&mut database, "CREATE (n:Item {id: 2})", BTreeMap::new());
        database
    });
    let dump = temp.path().join("dump");
    let report = snapshot
        .export_logical_dump(
            &dump,
            &LogicalDumpOptions {
                page_rows: NonZeroUsize::new(3).unwrap(),
                ..LogicalDumpOptions::default()
            },
        )
        .unwrap();
    let mut database = writer.join().unwrap();
    assert_eq!(report.commit_epoch, epoch);
    assert_eq!(
        report.table_row_count,
        40 + 1,
        "40 log rows and the engine registry row"
    );
    assert_eq!(report.node_count, 1);

    let imported = temp.path().join("imported");
    Database::import_logical_dump(&dump, &imported).unwrap();
    let mut imported = HawDBEmbedded::open(&imported).unwrap();
    let expected = (0..40)
        .map(|id| vec![Value::Int(id), Value::String(format!("before {id}"))])
        .collect::<Vec<_>>();
    assert_eq!(
        rows(&mut imported, "SELECT id, body FROM log ORDER BY id"),
        expected
    );
    assert_eq!(
        rows(&mut database, "SELECT COUNT(*) FROM log"),
        vec![vec![Value::Int(139)]]
    );
}

#[test]
fn export_refuses_state_format_1_cannot_represent_and_writes_nothing() {
    let temp = TempDir::new("refuse");
    let cases: &[(&str, &[&str], &str)] = &[
        (
            "append",
            &[
                "CREATE TABLE ev (stream TEXT NOT NULL, seq BIGINT NOT NULL, body TEXT) \
               WITH (storage_mode = 'strict_append', partition_key = 'stream', order_key = 'seq')",
            ],
            "strict append tables",
        ),
        (
            "view",
            &[
                "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT)",
                "CREATE MATERIALIZED VIEW v AS SELECT g, COUNT(*) AS c FROM t GROUP BY g",
            ],
            "materialized views",
        ),
        (
            "default",
            &["CREATE TABLE d (id BIGINT PRIMARY KEY, v VECTOR(2) DEFAULT '[1,2]')"],
            "this column default",
        ),
    ];
    for (name, statements, kind) in cases {
        let mut database = HawDBEmbedded::open(temp.path().join(name)).unwrap();
        let mut accepted = true;
        for statement in *statements {
            accepted &= database.database_mut().query_sql(statement).is_ok();
        }
        if !accepted {
            // This build's SQL surface cannot create the state at all; there
            // is nothing to refuse.
            assert_eq!(*name, "default", "{name} could not be created");
            continue;
        }
        let destination = temp.path().join(format!("{name}-dump"));
        let error = database.export_logical_dump(&destination).unwrap_err();
        assert!(
            matches!(&error, HawDBError::Semantic(message) if message.contains(kind)),
            "{name}: {error:?}"
        );
        assert_not_published(&destination);
    }
    // A transaction open on the handle refuses export.
    let mut database = HawDBEmbedded::open(temp.path().join("transaction")).unwrap();
    database.begin_transaction().unwrap();
    let destination = temp.path().join("transaction-dump");
    assert!(matches!(
        database.export_logical_dump(&destination),
        Err(HawDBError::Semantic(_))
    ));
    assert_not_published(&destination);
    database.rollback_transaction().unwrap();
    // An existing destination is never overwritten.
    fs::create_dir(&destination).unwrap();
    fs::write(destination.join("keep"), b"keep").unwrap();
    assert!(database.export_logical_dump(&destination).is_err());
    assert_eq!(fs::read(destination.join("keep")).unwrap(), b"keep");
}

/// A small dump with rotated table, node and relationship files; DDL is
/// the expensive part of an import, so the damage cases use one table.
fn exported_fixture(temp: &TempDir) -> PathBuf {
    let mut database = HawDBEmbedded::open(temp.path().join("fixture-source")).unwrap();
    sql(
        &mut database,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)",
    );
    for id in 0..25 {
        sql_params(
            &mut database,
            "INSERT INTO t (id, body) VALUES ($1, $2)",
            vec![Value::Int(id), Value::String(format!("行 {id}"))],
        );
    }
    for id in 0..12 {
        cypher(
            &mut database,
            "CREATE (n:Item {id: $id})",
            BTreeMap::from([("id".to_string(), Value::Int(id))]),
        );
    }
    for id in 0..11 {
        cypher(
            &mut database,
            "MATCH (a:Item {id: $a}), (b:Item {id: $b}) CREATE (a)-[:NEXT]->(b)",
            BTreeMap::from([
                ("a".to_string(), Value::Int(id)),
                ("b".to_string(), Value::Int(id + 1)),
            ]),
        );
    }
    let dump = temp.path().join("fixture-dump");
    database
        .export_logical_dump_with_options(&dump, &small_pages())
        .unwrap();
    dump
}

fn copy_dump(from: &Path, to: &Path) {
    fs::create_dir_all(to.join("data")).unwrap();
    fs::copy(from.join("manifest.json"), to.join("manifest.json")).unwrap();
    fs::copy(from.join("schema.json"), to.join("schema.json")).unwrap();
    for entry in fs::read_dir(from.join("data")).unwrap() {
        let entry = entry.unwrap();
        fs::copy(entry.path(), to.join("data").join(entry.file_name())).unwrap();
    }
}

#[test]
fn damaged_or_incomplete_dumps_publish_nothing() {
    let temp = TempDir::new("damaged");
    let dump = exported_fixture(&temp);
    let files = manifest(&dump)["files"].as_array().unwrap().clone();
    let last = files.last().unwrap()["path"].as_str().unwrap().to_string();
    assert!(last.starts_with("data/relationships-"), "{last}");
    let first_table = files[0]["path"].as_str().unwrap().to_string();

    type Damage = Box<dyn Fn(&Path)>;
    let flip_last_file: Damage = Box::new({
        let last = last.clone();
        move |dump: &Path| {
            // The final relationship file fails after every table and node
            // was already written into the unpublished target.
            let path = dump.join(&last);
            let mut bytes = fs::read(&path).unwrap();
            let middle = bytes.len() / 2;
            bytes[middle] ^= 0x20;
            fs::write(path, bytes).unwrap();
        }
    });
    let truncate_table: Damage = Box::new({
        let first_table = first_table.clone();
        move |dump: &Path| {
            let path = dump.join(&first_table);
            let bytes = fs::read(&path).unwrap();
            fs::write(path, &bytes[..bytes.len() - 5]).unwrap();
        }
    });
    let remove_file: Damage =
        Box::new(move |dump: &Path| fs::remove_file(dump.join(&last)).unwrap());
    let missing_manifest: Damage =
        Box::new(|dump: &Path| fs::remove_file(dump.join("manifest.json")).unwrap());
    let damaged_schema: Damage = Box::new(|dump: &Path| {
        let path = dump.join("schema.json");
        let text = fs::read_to_string(&path)
            .unwrap()
            .replacen("bigint", "BIGINT", 1);
        fs::write(path, text).unwrap();
    });
    let escaping_path: Damage = Box::new(|dump: &Path| {
        let path = dump.join("manifest.json");
        let text = fs::read_to_string(&path)
            .unwrap()
            .replacen("data/", "../", 1);
        fs::write(path, text).unwrap();
    });
    let cases: Vec<(&str, Damage)> = vec![
        ("flipped-relationships", flip_last_file),
        ("truncated-table", truncate_table),
        ("missing-file", remove_file),
        ("missing-manifest", missing_manifest),
        ("damaged-schema", damaged_schema),
        ("escaping-path", escaping_path),
    ];
    for (name, damage) in cases {
        let copy = temp.path().join(format!("{name}-dump"));
        copy_dump(&dump, &copy);
        damage(&copy);
        // Verification refuses the same damage without writing anything.
        let error = verify_logical_dump(&copy).unwrap_err();
        assert!(
            matches!(
                error,
                HawDBError::StorageIntegrity(_) | HawDBError::Storage(_)
            ),
            "{name}: verify: {error:?}"
        );
        let destination = temp.path().join(format!("{name}-db"));
        let error = Database::import_logical_dump_with_options(&copy, &destination, &small_pages())
            .unwrap_err();
        assert!(
            matches!(
                error,
                HawDBError::StorageIntegrity(_) | HawDBError::Storage(_)
            ),
            "{name}: {error:?}"
        );
        assert_not_published(&destination);
    }

    // A crash during an earlier import leaves only its staging directory. It
    // is never adopted: the next import builds and publishes its own.
    let destination = temp.path().join("intact-db");
    let stale = temp.path().join(".intact-db.hawdb-import-1-2-3");
    fs::create_dir(&stale).unwrap();
    fs::write(stale.join("partial"), b"interrupted").unwrap();
    Database::import_logical_dump(&dump, &destination).unwrap();
    assert_eq!(fs::read(stale.join("partial")).unwrap(), b"interrupted");
    assert!(!destination.join("partial").exists());
    // An existing destination is refused and left untouched.
    let error = Database::import_logical_dump(&dump, &destination).unwrap_err();
    assert!(matches!(error, HawDBError::Storage(message) if message.contains("already exists")));
    HawDBEmbedded::open(&destination).unwrap();
}

#[test]
fn verify_and_confirm_reconcile_a_published_dump() {
    let temp = TempDir::new("verify");
    let dump = exported_fixture(&temp);
    let before = dump_content(&dump);
    let written = manifest(&dump);
    let report = verify_logical_dump(&dump).unwrap();
    let mut hasher = hawdb_integrity::IntegrityHasher::new();
    hasher.update(&fs::read(dump.join("manifest.json")).unwrap());
    assert_eq!(report.manifest_sha256, hasher.finish().sha256.to_string());
    assert_eq!(report.dump_format_version, LOGICAL_DUMP_FORMAT_VERSION);
    assert_eq!(
        report.source_persistent_format_version,
        PERSISTENT_FORMAT_VERSION
    );
    let counts = &written["counts"];
    assert_eq!(report.table_count, counts["tables"].as_u64().unwrap());
    assert_eq!(
        report.table_row_count,
        counts["table_rows"].as_u64().unwrap()
    );
    assert_eq!(report.node_count, 12);
    assert_eq!(report.relationship_count, 11);
    assert_eq!(
        report.file_count,
        written["files"].as_array().unwrap().len() as u64 + 2
    );
    assert_eq!(
        report.commit_epoch,
        written["source"]["commit_epoch"].as_u64().unwrap()
    );
    // Confirming a publication is idempotent and changes no byte; it refuses
    // a missing destination and one that is not a directory.
    confirm_logical_dump_publication(&dump).unwrap();
    confirm_logical_dump_publication(&dump).unwrap();
    assert_eq!(dump_content(&dump), before);
    assert_eq!(verify_logical_dump(&dump).unwrap(), report);
    assert!(confirm_logical_dump_publication(temp.path().join("absent")).is_err());
    assert!(confirm_logical_dump_publication(dump.join("manifest.json")).is_err());
    // An imported database confirms the same way.
    let imported = temp.path().join("imported");
    Database::import_logical_dump(&dump, &imported).unwrap();
    confirm_logical_dump_publication(&imported).unwrap();
    HawDBEmbedded::open(&imported).unwrap();
}

#[test]
fn newer_dump_format_is_a_typed_refusal_before_any_write() {
    let temp = TempDir::new("newer");
    let dump = temp.path().join("dump");
    let mut database = HawDBEmbedded::open(temp.path().join("db")).unwrap();
    sql(&mut database, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    database.export_logical_dump(&dump).unwrap();
    let path = dump.join("manifest.json");
    let mut document = manifest(&dump);
    document["dump_format_version"] = serde_json::json!(LOGICAL_DUMP_FORMAT_VERSION + 1);
    document["a_future_field"] = serde_json::json!({"ignored": true});
    fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    let destination = temp.path().join("imported");
    assert_eq!(
        verify_logical_dump(&dump).unwrap_err(),
        HawDBError::UnsupportedStorageFormat(StorageFormatError::UnsupportedVersion {
            format: "logical dump",
            found: LOGICAL_DUMP_FORMAT_VERSION + 1,
            supported: LOGICAL_DUMP_FORMAT_VERSION,
        })
    );
    assert_eq!(
        Database::import_logical_dump(&dump, &destination).unwrap_err(),
        HawDBError::UnsupportedStorageFormat(StorageFormatError::UnsupportedVersion {
            format: "logical dump",
            found: LOGICAL_DUMP_FORMAT_VERSION + 1,
            supported: LOGICAL_DUMP_FORMAT_VERSION,
        })
    );
    assert_not_published(&destination);

    // Unknown fields alone are ignored within a version.
    document["dump_format_version"] = serde_json::json!(LOGICAL_DUMP_FORMAT_VERSION);
    fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    Database::import_logical_dump(&dump, &destination).unwrap();
}

/// A dump written by format 1, checked in. Every later release must import it
/// unchanged; this is the forward-compatibility contract of
/// docs/LOGICAL_DUMP.md.
#[test]
fn golden_format_1_dump_stays_importable() {
    let temp = TempDir::new("golden");
    let dump = temp.path().join("golden");
    fs::create_dir_all(dump.join("data")).unwrap();
    for (path, bytes) in golden_v1_files() {
        fs::write(dump.join(path), bytes).unwrap();
    }
    let destination = temp.path().join("imported");
    let report = Database::import_logical_dump(&dump, &destination).unwrap();
    assert_eq!(report.dump_format_version, 1);
    // Four docs rows and the writer's engine registry row.
    assert_eq!(
        (
            report.table_row_count,
            report.node_count,
            report.relationship_count
        ),
        (5, 2, 1)
    );
    let mut database = HawDBEmbedded::open(&destination).unwrap();
    assert_eq!(
        rows(&mut database, "SELECT id, title, body, amount, score, payload, embedding, at, made FROM docs ORDER BY id"),
        logical_dump_support::golden_v1_rows(),
    );
    let graph = database
        .query_admitted(
            "MATCH (a:Doc)-[r:CITES]->(b:Doc) RETURN a.title, b.title, r.weight, a.vec, a.blob",
        )
        .unwrap();
    assert_eq!(
        graph
            .value_rows()
            .map(|row| row.to_vec())
            .collect::<Vec<_>>(),
        vec![vec![
            Value::String("甲".to_string()),
            Value::String("乙".to_string()),
            Value::Float(0.25),
            vector(&[1.0, -2.5]),
            Value::Binary(vec![0, 1, 254, 255]),
        ]]
    );
}

/// Regenerates the golden dump (run with `--ignored` and
/// `HAWDB_GOLDEN_OUT=<new dir>`). Only for adding a new golden version: the
/// format 1 files must never change.
#[test]
#[ignore = "writes the golden fixture on request"]
fn write_golden_format_1_dump() {
    let Some(output) = std::env::var_os("HAWDB_GOLDEN_OUT") else {
        return;
    };
    let temp = TempDir::new("golden-writer");
    let mut database = HawDBEmbedded::open(temp.path().join("db")).unwrap();
    sql(
        &mut database,
        "CREATE TABLE docs (id BIGINT PRIMARY KEY, title TEXT NOT NULL, body TEXT, \
         amount NUMERIC(20,4), score DOUBLE PRECISION, payload BYTEA, embedding VECTOR(2), \
         at TIMESTAMPTZ(6), made TIMESTAMP(3) NOT NULL DEFAULT '2024-01-02T03:04:05.123')",
    );
    for row in logical_dump_support::golden_v1_rows() {
        let explicit_made = row[0] == Value::Int(2);
        let columns = if explicit_made {
            "id, title, body, amount, score, payload, embedding, at, made"
        } else {
            "id, title, body, amount, score, payload, embedding, at"
        };
        let width = if explicit_made { 9 } else { 8 };
        let placeholders = (1..=width)
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>()
            .join(", ");
        sql_params(
            &mut database,
            &format!("INSERT INTO docs ({columns}) VALUES ({placeholders})"),
            row.into_iter().take(width).collect(),
        );
    }
    sql(&mut database, "CREATE INDEX docs_title ON docs (title)");
    cypher(
        &mut database,
        "CREATE (n:Doc {title: '甲', vec: $vec, blob: $blob})",
        BTreeMap::from([
            ("vec".to_string(), vector(&[1.0, -2.5])),
            ("blob".to_string(), Value::Binary(vec![0, 1, 254, 255])),
        ]),
    );
    cypher(
        &mut database,
        "CREATE (n:Doc {title: '乙'})",
        BTreeMap::new(),
    );
    cypher(
        &mut database,
        "MATCH (a:Doc {title: '甲'}), (b:Doc {title: '乙'}) CREATE (a)-[:CITES {weight: 0.25}]->(b)",
        BTreeMap::new(),
    );
    cypher(
        &mut database,
        "CREATE INDEX ON :Doc(title)",
        BTreeMap::new(),
    );
    database.export_logical_dump(PathBuf::from(output)).unwrap();
}

#[test]
fn incompatible_storage_format_open_is_a_typed_refusal_naming_both_versions() {
    let temp = TempDir::new("incompatible");
    let path = temp.path().join("db");
    {
        // A single-branch store as an older release leaves it on disk; the
        // embedded open adopts it into the branch layout after reading it.
        let mut catalog = hawdb::schema::Catalog::default();
        let mut store = hawdb::store::GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("id".to_string(), Value::Int(1))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    let rewritten = logical_dump_support::rewrite_storage_version(
        &path,
        "hawdb-storage-v1",
        "hawdb-storage-v0",
    );
    assert_eq!(
        rewritten,
        1,
        "expected one storage manifest under {}",
        path.display()
    );
    let open = |path: &Path| {
        HawDBEmbedded::open(path).and_then(|mut database| {
            database
                .query_admitted("MATCH (n:Memory) RETURN n.id")
                .map(|output| {
                    output
                        .value_rows()
                        .map(|row| row.to_vec())
                        .collect::<Vec<_>>()
                })
                .map_err(|error| match error {
                    hawdb::EmbeddedQueryError::Database(error) => error,
                    other => HawDBError::Execution(other.to_string()),
                })
        })
    };
    let error = open(&path).unwrap_err();
    assert_eq!(
        error,
        HawDBError::UnsupportedStorageFormat(StorageFormatError::UnsupportedVersion {
            format: "storage manifest",
            found: 0,
            supported: 1,
        }),
        "{error}"
    );
    assert!(error.to_string().contains("version 0") && error.to_string().contains("version 1"));
    // The refusal changed nothing: restoring the version makes it readable.
    logical_dump_support::rewrite_storage_version(&path, "hawdb-storage-v0", "hawdb-storage-v1");
    assert_eq!(open(&path).unwrap(), vec![vec![Value::Int(1)]]);
}

#[test]
fn persistent_format_version_is_exposed() {
    assert_eq!(
        hawdb::persistent_format_version(),
        PERSISTENT_FORMAT_VERSION
    );
    const { assert!(PERSISTENT_FORMAT_VERSION >= 1) };
    const { assert!(LOGICAL_DUMP_FORMAT_VERSION >= 1) };
}
